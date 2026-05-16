# Factor Backtest Page Design

**Date**: 2026-05-13 (revised 2026-05-15)
**Status**: Design finalised, pending implementation
**Decisions**: 见文末「设计决策记录」

## Overview

新增因子回测页面 `/factor-backtest`，支持：
- **单因子回测**：评估任意一个因子在历史上的选股能力
- **因子组合回测**：自由勾选因子 + 自定义权重，跑组合回测
- **多持有期对比**：一次跑出 1/3/5/10/20 交易日的收益曲线横向对比
- **完整指标**：累计收益曲线、胜率、夏普比率、最大回撤、Rank IC、分位数收益

## Module Integration

FactorBacktestEngine 统一替代并整合现有两个模块：

| 旧模块 | 职责 | 整合方式 |
|--------|------|----------|
| FactorMonitor (`src/discovery/factor_monitor.py`) | 每因子 Top-20 选股 → 回填收益 → 均值/胜率 | 被 FactorBacktestEngine 替代。历史 picks JSON 不再维护，数据源统一为 factor_score_snapshots 表 |
| ICTracker (`src/discovery/ic_tracker.py`) | 横截面 Rank IC 计算，存 JSON | IC 计算逻辑整合入 FactorBacktestEngine，直接从 snapshots 表算，不再依赖内存 dict + JSON 存储 |

整合后的 FactorBacktestEngine：

```
FactorBacktestEngine
├── 单因子评估：Rank IC、分位数收益、胜率（原 ICTracker + FactorMonitor 职责）
├── 组合回测：加权合成 + 多持有期 + 资金曲线（本次新增）
└── 数据源统一：factor_score_snapshots 表（已存在，见 src/storage.py:1287）
```

FactorMonitor 的 Markdown 报告生成能力保留，但底层改为从 snapshots 表取数。

## Core Design

### 数据层：复用现有 `factor_score_snapshots` 表

**表已存在**（`src/storage.py:1287`），引擎在每次扫描后自动写入（`engine.py:1005`）。

| 字段 | 类型 | 说明 |
|------|------|------|
| trade_date | TEXT | 交易日 YYYYMMDD |
| ts_code | TEXT | 股票代码 |
| mode | TEXT | intraday / postmarket |
| factor_name | TEXT | 因子名 |
| score | REAL | 原始得分 (0-100) |

当前覆盖：2026-03-24 ~ 至今，35 个交易日，22 个因子（16 postmarket + 6 intraday）。
单次扫描约 `5000 只 × 22 因子 ≈ 11 万行`，SQLite 可承受。

历史数据回补：`scripts/backfill_factor_snapshots.py`（已存在）。

### 计算层：FactorBacktestEngine

- 读快照：按日期范围 + mode 拉取因子分数矩阵
- 加权合成：`composite = Σ(score_i × weight_i) / Σweight_i`
  - **注意**：引擎实际运行时因子分经过「去相关 → 行业中性化 → 横截面标准化 → 加权」四步。回测直接用原始分数加权，结果会与实际策略排名有系统性偏差。前端标注此差异，后续可选支持去相关开关。
- Top-N 选股：按合成分数排序取前 N
- 多持有期收益：持有 1/3/5/10/20 个**交易日**后价格变化
- 指标：累计收益曲线、胜率、夏普、最大回撤、Rank IC、分位数收益

#### 交易日计算

持有期以**交易日**为单位计算（与 DiscoveryBacktest 现有逻辑一致）。例如持有 5 天 = 5 个交易日后的卖出日。

#### 停牌 / 涨跌停处理

| 场景 | 处理 |
|------|------|
| 持有期满日停牌（无卖出价） | 顺延到复牌后第一个有价格的交易日卖出，按实际持有天数调整年化收益 |
| 买入日一字涨停（买不到） | 跳过该股，资金分配给列表中下一只候选（按排名顺延） |
| 买入日一字跌停 | 正常以开盘价买入，计入回测 |
| 买入日正常、卖出日涨跌停 | 涨跌停不存在无价格问题，正常以收盘价卖出 |

### 准确性保证

1. 买入价用次日开盘价（实际可成交），卖出价用持有期满收盘价
2. 因子分数存扫描时刻快照，回测时不做事后修正
3. 价格获取：DB stock_daily 主路径 → Tushare API fallback（复用 DiscoveryBacktest 链）

### 数据覆盖不均匀的日期范围策略

不同因子历史覆盖天数差异巨大（margin ~2426 天 vs profit_forecast ~4 天）：

**后端策略（A）**：取所选因子覆盖的**交集**作为可用日期范围。API 返回实际的 start_date / end_date。

**前端策略（C）**：因子选择面板勾选因子后，实时显示该组合的「可用日期范围」。用户手动输入的日期超出范围时，输入框标红并展示提示文案（如「chip 因子最早可用 2026-01-30」）。

### API 层

**POST /api/v1/discovery/factor-backtest**

同步返回（预计耗时 < 2s，snapshots 表已预计算分数）：

```json
{
  "mode": "postmarket",
  "date_range": {"start": "20260324", "end": "20260515"},
  "factors": [
    {"name": "technical", "weight": 25.0, "available_from": "20241231", "available_to": "20260515"},
    {"name": "chip", "weight": 15.0, "available_from": "20260130", "available_to": "20260515"}
  ],
  "params": {
    "top_n": 5,
    "hold_days": [1, 3, 5, 10, 20],
    "initial_capital": 1000000
  },
  "summary": {
    "cumulative_return": 0.152,
    "annualized_return": 0.23,
    "win_rate": 0.62,
    "max_drawdown": -0.08,
    "sharpe_ratio": 1.82,
    "total_trades": 175,
    "total_periods": 35
  },
  "capital_curves": {
    "1": [{"date": "20260325", "capital": 1005000}],
    "3": [],
    "5": [],
    "10": [],
    "20": []
  },
  "rank_ic": {
    "technical": 0.042,
    "chip": -0.013
  },
  "quantile_returns": {
    "top_10pct": 0.038,
    "top_20pct": 0.025,
    "top_50pct": 0.008
  },
  "trade_records": [
    {
      "trade_date": "20260325",
      "hold_days": 5,
      "stock_code": "600519.SH",
      "stock_name": "贵州茅台",
      "buy_price": 1850.0,
      "sell_date": "20260401",
      "sell_price": 1920.0,
      "return_pct": 0.038,
      "pnl": 10270.0,
      "status": "closed"
    }
  ]
}
```

字段说明：
- `factors[].available_from / available_to`：该因子在 snapshots 表中的可用日期范围，前端用此判断交集
- `status`：`"closed"` 正常完成 / `"extended"` 顺延卖出 / `"canceled"` 一字涨停取消 / `"open"` 期末未平仓
- `rank_ic`：因子得分与持有期内收益的 Spearman 秩相关系数

**GET /api/v1/discovery/factor-snapshot-dates**

返回每个因子各自的可用日期范围 + 因子名称/标签列表：

```json
{
  "factors": [
    {"name": "technical", "label": "技术面", "mode": "postmarket", "available_from": "20241231", "available_to": "20260515", "trading_days": 328},
    {"name": "chip", "label": "筹码分布", "mode": "postmarket", "available_from": "20260130", "available_to": "20260515", "trading_days": 41}
  ],
  "global": {
    "mode": "postmarket",
    "available_from": "20260130",
    "available_to": "20260515"
  }
}
```

`global` 为全量因子交集。

### 前端层

新页面 `/factor-backtest`：

- 左侧面板：
  - mode 切换（intraday / postmarket）
  - 因子勾选列表（复选框 + 权重数字输入），勾选后即时显示「组合可用日期范围」
  - 快速操作：全选 / 全部默认权重 / 全部等权
- 右侧上半：参数配置
  - 持有期多选（1/3/5/10/20 天 Checkbox）
  - Top-N（数字输入）
  - 日期范围（DatePicker，超交集范围标红 + tooltip 提示）
  - 初始资金
  - 无风险利率（默认 2.0%，可修改）
  - 「开始回测」按钮
- 右侧下半：结果展示
  - 摘要指标卡片：累计收益、年化收益、胜率、最大回撤、夏普比率
  - 多持有期收益曲线叠加（Recharts LineChart，5 条线，点击图例可隐藏单条）
  - Rank IC 表格（每因子一行）
  - 分位数收益柱状图
  - 逐日交易明细表（Table 组件，超过 50 行分页）

#### 导航集成

在 `SidebarNav.tsx` NAV_ITEMS 中新增：

```typescript
{ key: 'factor-backtest', label: '因子回测', to: '/factor-backtest', icon: Activity },
```

位置放在「回测」（/backtest）旁边。当前 /backtest 路由已占用 BarChart3 图标，因子回测使用 `Activity`（lucide-react）。

在 `App.tsx` 路由中新增：

```tsx
<Route path="/factor-backtest" element={<FactorBacktestPage />} />
```

## Backtest Parameters

| 参数 | 默认值 | 说明 |
|------|--------|------|
| mode | postmarket | intraday / postmarket |
| factors | 全部勾选 | 选择的因子 + 各自权重 |
| top_n | 5 | 每期选股数量 |
| hold_days | [1, 3, 5, 10, 20] | 多持有期（交易日） |
| start_date | 所选因子可用交集的最早日期 | 回测起始 |
| end_date | 所选因子可用交集的最晚日期 | 回测结束 |
| initial_capital | 1,000,000 | 初始资金 |
| risk_free_rate | 2.0 | 无风险利率（%，用于夏普比率，可修改） |

## Output Metrics

- 累计收益率曲线（多持有期叠加，支持图例切换）
- 胜率（正收益交易占比）
- 平均收益（均值 + 中位数）
- 最大回撤
- 夏普比率（默认无风险利率 2%，可配置）
- Rank IC（因子得分与未来收益的 Spearman 相关性）
- 分位数收益表（Top-10% / 20% / 50% 各层表现）
- 逐日交易明细表（分页）

## Design Decisions (2026-05-15)

### 决策一：模块整合

FactorBacktestEngine 统一替代 FactorMonitor 的绩效追踪，整合 ICTracker 的 IC 计算。三者合并为一个模块，数据源统一用 factor_score_snapshots 表。FactorMonitor 的 Markdown 报告生成保留，底层改为从 snapshots 取数。

### 决策二：交易日规则 + 异常处理

- 持有期以**交易日**为单位计算
- 停牌：顺延到复牌后第一个有价格的交易日卖出
- 一字涨停买入：跳过，资金分配给下一只候选（按排名顺延）
- 一字跌停买入：正常以开盘价买入计入

### 决策三：日期范围取交集 + 前端提示

后端取所选因子覆盖的交集作为可用范围。前端勾选因子后实时显示组合可用日期范围，手动输入超范围标红提示。

### 决策四：引擎与回测一致性策略

盘后扫描引擎改为从 `factor_score_snapshots` 表读取因子得分（与回测同源），跳过实时因子计算。时间线：当日首跑 → 回退实时计算 → 存快照 → 后续运行（含 web 回测）直接读快照。

### 决策五：加权管线简化

引擎和回测统一使用「简单加权 composite = Σ(score_i × weight_i) / Σweight_i」：
- 取消因子去相关（decorrelation）
- 取消行业中性化（industry neutralization）
- 取消横截面标准化（cross-sectional standardization）
- `DISCOVERY_PIPELINE_ENABLED` 仅控制是否启用后段 StockScorer 融合（30/70 混合）
- 盘后引擎 StockScorer 改为调用回测引擎的静态方法 `FactorBacktestEngine._batch_stockscorer_static()`
- 策略文件 `strategies/*.yaml` 中的因子权重唯一真源

### 决策六：盘后/盘中分拆

- **盘后**：全部从快照 + stock_daily 取数（MA/ATR 自算、量比自算、板块用 ths_industry_map）
- **盘中**：维持原有实时计算链路（spot_df、realtime_spot、akshare 价格），未改动

### 决策七：选股与交易分离

回测 `compute()` 在买入日尚未来临时跳过交易（trades=0），但选股结果仍可通过直接调用 `_batch_stockscorer_static()` 验证。前端回测页面展示的 trades 仅包含已发生的交易。

### 决策八：ICTracker 删除

`src/discovery/ic_tracker.py` 已移除。IC 计算完全整合入 FactorBacktestEngine，不再依赖独立模块。

## Dependencies

- 数据源：`factor_score_snapshots` 表（已存在，`src/storage.py`）
- 快照写入：`engine.py:1005`（已存在）
- 历史回补：`scripts/backfill_factor_snapshots.py`（已存在）
- 价格获取：DB stock_daily → Tushare fallback（复用 DiscoveryBacktest 链）
- 前端图表：Recharts LineChart（复用 DiscoveryPage 的 BacktestCard 模式）
- 前端组件：Card、StatCard、Segmented、DatePicker、Table 等 common 组件库
