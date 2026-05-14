# Factor Backtest Page Design

**Date**: 2026-05-13
**Status**: Design approved, implementation pending

## Overview

新增因子回测页面 `/factor-backtest`，支持：
- **单因子回测**：评估任意一个因子在历史上的选股能力
- **因子组合回测**：自由勾选因子 + 自定义权重，跑组合回测
- **多持有期对比**：一次跑出 1/3/5/10/20 天的收益曲线横向对比
- **完整指标**：累计收益曲线、胜率、夏普比率、最大回撤、Rank IC、分位数收益

## Core Design

### 数据层：因子快照表

新增 DB 表 `factor_score_snapshots`，每次扫描后将各因子全市场得分向量存档：

| 字段 | 类型 | 说明 |
|------|------|------|
| trade_date | TEXT | 交易日 YYYYMMDD |
| ts_code | TEXT | 股票代码 |
| mode | TEXT | intraday / postmarket |
| factor_name | TEXT | 因子名 |
| score | REAL | 原始得分 (0-100) |

一次扫描产生约 `5000 只 × 14 因子 ≈ 7 万行`，SQLite 可承受。

### 计算层：FactorBacktestEngine

- 读快照：按日期范围 + mode 拉取因子分数矩阵
- 加权合成：`composite = Σ(score_i × weight_i) / Σweight_i`
- Top-N 选股：按合成分数排序取前 N
- 多持有期收益：持有 1/3/5/10/20 天后价格变化
- 指标：累计收益曲线、胜率、夏普、最大回撤、Rank IC、分位数收益

### 准确性保证

1. 买入价用次日开盘价（实际可成交），卖出价用持有期满收盘价
2. 因子分数存扫描时刻快照，回测时不做事后修正
3. 复用已验证的 DiscoveryBacktest 价格获取逻辑

### API 层

- `POST /api/v1/discovery/factor-backtest` — 提交回测参数，同步返回结果
- `GET /api/v1/discovery/factor-snapshot-dates` — 查询可用快照日期范围

### 前端层

新页面 `/factor-backtest`：
- 左侧：因子选择面板（勾选框 + 权重滑块 + mode 切换）
- 右侧上半：参数配置（持有期多选、Top-N、日期范围）
- 右侧下半：结果展示（累计收益曲线 + 指标卡片 + Rank IC + 分位数收益表）
- 侧边栏新增导航项

## Backtest Parameters

| 参数 | 默认值 | 说明 |
|------|--------|------|
| mode | postmarket | intraday / postmarket |
| factors | 全部勾选 | 选择的因子 + 各自权重 |
| top_n | 5 | 每期选股数量 |
| hold_days | [1, 3, 5, 10, 20] | 多持有期 |
| start_date | 最早可用快照日期 | 回测起始 |
| end_date | 最新可用快照日期 | 回测结束 |
| initial_capital | 1,000,000 | 初始资金 |

## Output Metrics

- 累计收益率曲线（多持有期叠加）
- 胜率（正收益天数占比）
- 平均收益（均值 + 中位数）
- 最大回撤
- 夏普比率（假设无风险利率 2%）
- Rank IC（因子得分与未来收益的 Spearman 相关性）
- 分位数收益表（Top-10% / 20% / 50% 各层表现）
- 逐日交易明细表

## Dependencies

- 复用 FactorMonitor 的因子得分收集逻辑，扩展为全市场存储
- 复用 DiscoveryBacktest 的价格获取逻辑（DB stock_daily → Tushare fallback）
- 复用 DiscoveryPage 的 BacktestCard 图表组件模式（Recharts LineChart）
- 复用 common 组件库（Card、StatCard、Segmented、DatePicker、Table 等）
