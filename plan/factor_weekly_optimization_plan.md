# 因子体系周线级优化方案

> 状态: **方案重新设计，待评审**
> 日期: 2026-05-14
> 目标: 提高系统健壮性，引入多时间尺度交叉验证，修复已发现的缺陷

---

## 一、现状问题总结

### 1.1 关键缺陷：StockScorer 大盘评分失效（BUG）

**严重程度: HIGH**

`engine.py:1003` 检查 `hasattr(self, '_index_ohlcv_cache')` 但该属性**从未被赋值**。导致：
- `StockScorer.preload_index_ohlcv()` 永远不会被调用
- `_calc_market_score()` 始终返回默认值 50.0
- `_judge_sector_regime()` 中依赖大盘数据的判断（crisis、calm）全部失效
- **结论：当前所有股票的大盘维度评分都是 50 分，动态权重中的危机/平静模式从未触发**

**根因**: `_calc_dynamic_weights()` 在约 line 460 拉取了上证指数数据，但结果只用于计算因子动态权重，没有转发给 `self._index_ohlcv_cache`。

### 1.2 因子体系缺少多时间尺度验证

当前所有 20 个因子和 StockScorer 都基于**日线或日内数据**。存在以下问题：

| 问题 | 表现 | 影响 |
|------|------|------|
| 趋势末期追高 | 日线显示多头，周线已是下降趋势 | 盘中因子在周线下跌中给反弹股高分 |
| 信号噪音大 | 日线 MACD/RSI 频繁金叉死叉 | TechnicalFactor 产生大量假信号 |
| 主力一日游 | 单日大单净流入可能对倒 | MoneyFlowFactor 无法区分持续建仓和一日游 |
| 板块轮动过快 | 日线级别板块轮动 2-3 天 | SectorFactor 在快速轮动中失效 |

### 1.3 因子时间尺度混杂

盘后因子中混杂了 4 种时间尺度的信号：
- **日内**: LimitFactor、ConceptHeatFactor
- **日线**: MoneyFlowFactor、TechnicalFactor、ChipFactor
- **准周线**: MarginFactor（5日）、PopularityFactor（5/10日趋势）
- **季度**: InstitutionHoldFactor、ProfitForecastFactor、PerformanceFactor

这些因子的 `weighted_score()` 直接相加，没有时间尺度对齐，导致低频因子的信号被高频因子淹没。

---

## 二、可行性分析

### 2.1 数据层可行性

| 数据源 | 周线接口 | 参数 | 状态 |
|--------|----------|------|------|
| Tushare `stk_weekly_monthly` | 个股周线 | `freq='week'`, `ts_code`, `start_date`, `end_date` | **已验证可用**（18 行/半年） |
| Tushare `index_weekly` | 指数周线 | `ts_code`, `start_date`, `end_date` | **已验证可用**（17 行/半年） |
| Akshare `stock_zh_a_hist` | 原生支持 | `period="weekly"` | 极低：改硬编码字符串 |

**验证结果**（2026-05-14）：
- `stk_weekly_monthly(ts_code='600519.SH', freq='week')` → 18 行，trade_date 为连续周五（20260508, 20260501, ...）
- `index_weekly(ts_code='000001.SH')` → 17 行，trade_date 为连续周五
- `freq='week'`（小写）有效；`freq='W'`/`freq='WEEK'` 无效

**关键决策**: 选择**Tushare 接口**而非日线→周线聚合。原因：
1. Tushare `stk_weekly_monthly`（个股）和 `index_weekly`（指数）已验证可用
2. 周线边界为**周五**（Tushare 已处理），trade_date 即周线收盘日
3. Tushare 周线数据包含已计算好的 `pct_chg`（周涨跌幅），复用现成数据
4. 不需要自己聚合，避免边界处理不一致

### 2.2 存储层可行性

**方案 A：新建 `stock_weekly` 表**
- 优点：查询快，可预计算周线指标
- 缺点：需要数据同步脚本、迁移脚本、增加存储开销
- 适用：如果周线数据需要长期存储和高频访问

**方案 B：内存聚合（推荐）**
- 优点：零存储开销，零迁移成本，实时计算
- 缺点：每次需要重新聚合（但 60 天日线→12 周，计算量可忽略）
- 适用：因子评分场景（每周五盘后跑一次，不需要毫秒级响应）

**结论：选择方案 B**。在因子工具模块中提供 `resample_to_weekly()` 函数，各因子按需调用。

### 2.3 因子改造可行性

根据第一部分的可行性评估，20 个因子中：

| 可行性 | 因子 | 数量 |
|--------|------|------|
| **HIGH** | TechnicalFactor、StockScorer | 2 |
| **MEDIUM** | MaEntryFactor、ChipFactor、MoneyFlowFactor | 3 |
| **LOW** | SectorFactor、MomentumFactor、ReboundFactor、FundamentalFactor、LimitFactor、ConceptHeatFactor、BuybackFactor | 7 |
| **NONE** | InstitutionHoldFactor、ProfitForecastFactor、PerformanceFactor、InsiderBuyFactor、BrokerRecommendFactor、PopularityFactor(已有准周线)、MarginFactor(已有准周线) | 7 |

**优先级排序**：HIGH > MEDIUM > LOW > NONE。NONE 组不需要改造。

### 2.4 性能影响评估

- 日线→周线聚合：60 行 DataFrame 的 groupby 操作，耗时 < 1ms/只
- 全市场 5000 只股票：额外 ~5s 总计算时间
- 盘后扫描模式：当前总耗时 ~3-5 分钟，增加 5s 可忽略
- 盘中扫描模式：不引入周线计算（实时性要求高），零影响

---

## 三、详细实施方案

### Phase 0：修复 StockScorer 大盘评分失效（BUG FIX）

**优先级: P0 — 立即修复**

#### 3.0.1 问题定位

```
engine.py:~460  → _calc_dynamic_weights() 拉取上证指数数据，但只用于因子权重调整
engine.py:1003  → hasattr(self, '_index_ohlcv_cache') 永远为 False
stock_scorer.py → ohlcv is None → return 50.0
```

#### 3.0.2 修复方案

**文件**: `src/discovery/engine.py`

在 `_calc_dynamic_weights()` 中拉取指数数据后，将数据转发给 `_index_ohlcv_cache`：

```python
# engine.py _calc_dynamic_weights() 方法末尾，约 line 460-470
# 新增：将指数数据缓存供 StockScorer 使用
try:
    if df is not None and len(df) >= 20:
        # Tushare index_daily 列顺序: ts_code, trade_date, close, open, high, low, ...
        # StockScorer 期望格式: [open, high, low, close]
        ohlcv_cols = [
            pd.to_numeric(df['open'], errors='coerce').values,
            pd.to_numeric(df['high'], errors='coerce').values,
            pd.to_numeric(df['low'], errors='coerce').values,
            pd.to_numeric(df['close'], errors='coerce').values,
        ]
        if all(len(c) == len(ohlcv_cols[0]) for c in ohlcv_cols):
            self._index_ohlcv_cache = np.column_stack(ohlcv_cols)
except Exception:
    pass
```

**关键点**：`close` 列在 Tushare 返回中是第 3 列（index=2），而 StockScorer 需要 `[open, high, low, close]` 顺序。必须显式按列名重排，不能依赖 `df.values` 的原始列顺序。

**边界条件处理**：
- `df` 为 None 或行数不足 20 → 不设置 cache，scorer 保持默认 50 分（现有降级行为）
- 列名不匹配 → try-except 兜底，scorer 降级
- 数据含 NaN → numpy 自然传播，scorer 内部已有 `if ohlcv is None` 检查

#### 3.0.3 验证方法

```bash
# 1. 单元测试
python -m pytest tests/ -k "scorer" -v

# 2. 手动验证：运行盘后扫描，检查 market_score 是否不再是 50.0
python main.py --discover-only --debug
# 检查日志中 market_score 的分布
```

#### 3.0.4 影响范围

- 修改 1 个文件（engine.py），约 15 行
- 影响：所有使用 StockScorer 的场景（盘中扫描、盘后扫描）
- 风险：低。现有降级逻辑不变，只是把已有数据传递过去

---

### Phase 1：日线→周线聚合基础设施

**优先级: P1 — Phase 2A 的前置依赖**

#### 3.1.1 新增周线数据获取函数

**文件**: `src/discovery/factors/utils.py`（新建工具模块）

```python
def fetch_weekly_data(
    ts_code: str,
    start_date: str = None,
    end_date: str = None,
    days: int = 120
) -> pd.DataFrame:
    """从 Tushare 获取周线数据。

    Args:
        ts_code: 股票代码（如 '000001.SH', '600519.SH'）
        start_date: 起始日期 YYYYMMDD，None 时自动从 end_date往前推 days 天
        end_date: 结束日期 YYYYMMDD，默认为今日
        days: start_date 为 None 时，往前推的天数（默认 120 天 ≈ 半年 ≈ 24 周）

    Returns:
        周线 DataFrame，列: ts_code, trade_date, open, high, low, close, vol, pct_chg
        trade_date 为周线收盘日（周五）
    """
    from datetime import date, timedelta
    from data_provider.tushare_fetcher import TushareFetcher

    if end_date is None:
        end_date = date.today().strftime('%Y%m%d')
    if start_date is None:
        start_date = (date.today() - timedelta(days=days)).strftime('%Y%m%d')

    fetcher = TushareFetcher.get_instance()
    # 个股用 stk_weekly_monthly，指数用 index_weekly
    if ts_code.startswith('000') or ts_code.startswith('399'):
        api_name = 'index_weekly'
    else:
        api_name = 'stk_weekly_monthly'

    kwargs = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    if api_name == 'stk_weekly_monthly':
        kwargs['freq'] = 'week'

    df = fetcher._api.query(api_name, **kwargs)
    if df is not None and not df.empty:
        df = df.sort_values('trade_date').reset_index(drop=True)
    return df if df is not None else pd.DataFrame()
```

**验证结果**（已实测）：
```
stk_weekly_monthly(ts_code='600519.SH', freq='week') → 18 行/半年
index_weekly(ts_code='000001.SH') → 17 行/半年
trade_date = 连续周五（20260508, 20260501, 20260424, ...）
```

#### 3.1.2 新增周线指标计算函数

**文件**: `src/discovery/factors/utils.py`

#### 3.1.2 新增周线指标计算函数

**文件**: `src/discovery/factors/utils.py`

```python
def compute_weekly_indicators(weekly_df: pd.DataFrame) -> pd.DataFrame:
    """在周线数据上计算技术指标（MA, MACD, RSI, BOLL）。

    Args:
        weekly_df: fetch_weekly_data() 或 resample_to_weekly() 的输出

    Returns:
        添加了以下列的 DataFrame:
        - ma5_w, ma10_w, ma20_w: 周线均线
        - macd_dif_w, macd_dea_w, macd_hist_w: 周线 MACD
        - rsi_14_w: 周线 RSI（14 周 ≈ 3.5 个月）
        - boll_upper_w, boll_mid_w, boll_lower_w: 周线 BOLL
    """
    if weekly_df is None or weekly_df.empty:
        return pd.DataFrame()

    df = weekly_df.copy()
    close = df['close']

    # 均线
    df['ma5_w'] = close.rolling(5).mean()
    df['ma10_w'] = close.rolling(10).mean()
    df['ma20_w'] = close.rolling(20).mean()

    # MACD (12, 26, 9) — 周线级别
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df['macd_dif_w'] = ema12 - ema26
    df['macd_dea_w'] = df['macd_dif_w'].ewm(span=9, adjust=False).mean()
    df['macd_hist_w'] = (df['macd_dif_w'] - df['macd_dea_w']) * 2

    # RSI 14 周
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi_14_w'] = 100 - (100 / (1 + rs))

    # BOLL (20 周)
    df['boll_mid_w'] = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    df['boll_upper_w'] = df['boll_mid_w'] + 2 * std20
    df['boll_lower_w'] = df['boll_mid_w'] - 2 * std20

    return df
```

#### 3.1.3 新增周线趋势判断工具

**文件**: `src/discovery/factors/utils.py`

```python
from enum import Enum

class WeeklyTrend(Enum):
    """周线趋势状态。"""
    STRONG_UP = "strong_up"       # 多头排列: MA5 > MA10 > MA20
    WEAK_UP = "weak_up"           # 偏多但未完全排列
    RANGE = "range"               # 震荡
    WEAK_DOWN = "weak_down"       # 偏空
    STRONG_DOWN = "strong_down"   # 空头排列: MA5 < MA10 < MA20


def judge_weekly_trend(weekly_indicators: pd.DataFrame) -> Tuple[WeeklyTrend, float]:
    """判断周线趋势状态和趋势强度。

    Args:
        weekly_indicators: compute_weekly_indicators() 的输出

    Returns:
        (trend_state, trend_strength)
        trend_strength: 0.0-1.0，表示趋势的确定性

    边界条件：
    - 数据不足（< 10 周）→ 返回 (RANGE, 0.0)
    - MA 值含 NaN → 对应排列条件视为 False
    """
    if weekly_indicators is None or len(weekly_indicators) < 10:
        return WeeklyTrend.RANGE, 0.0

    latest = weekly_indicators.iloc[-1]
    ma5 = latest.get('ma5_w')
    ma10 = latest.get('ma10_w')
    ma20 = latest.get('ma20_w')

    if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20):
        return WeeklyTrend.RANGE, 0.0

    # 排列判断
    bull_align = ma5 > ma10 > ma20
    bear_align = ma5 < ma10 < ma20

    # 趋势强度：用 MACD 柱方向 + RSI 位置综合判断
    macd_hist = latest.get('macd_hist_w', 0)
    rsi = latest.get('rsi_14_w', 50)
    if pd.isna(macd_hist):
        macd_hist = 0
    if pd.isna(rsi):
        rsi = 50

    # MACD 柱方向一致性（近3周同向）
    macd_consistent = False
    if len(weekly_indicators) >= 3:
        recent_hist = weekly_indicators['macd_hist_w'].tail(3).dropna()
        if len(recent_hist) == 3:
            macd_consistent = all(recent_hist > 0) or all(recent_hist < 0)

    # 强度计算
    if bull_align:
        strength = 0.6
        if macd_hist > 0:
            strength += 0.2
        if macd_consistent:
            strength += 0.1
        if rsi > 50:
            strength += 0.1
        return WeeklyTrend.STRONG_UP, min(strength, 1.0)
    elif bear_align:
        strength = 0.6
        if macd_hist < 0:
            strength += 0.2
        if macd_consistent:
            strength += 0.1
        if rsi < 50:
            strength += 0.1
        return WeeklyTrend.STRONG_DOWN, min(strength, 1.0)
    elif ma5 > ma10 and ma10 < ma20:
        return WeeklyTrend.WEAK_UP, 0.3
    elif ma5 < ma10 and ma10 > ma20:
        return WeeklyTrend.WEAK_DOWN, 0.3
    else:
        return WeeklyTrend.RANGE, 0.1
```

---

### Phase 2：因子增强 — 双轨并行

分为两条独立路线：
- **Phase 2A**（周线信号）：依赖 Phase 1 基础设施
- **Phase 2B**（日线扩展）：独立实施，不依赖 Phase 1

#### Phase 2A-1：TechnicalFactor 周线增强

**文件**: `src/discovery/factors/technical_factor.py`

**改动**: 在 `_compute_signals()` 中新增 3 个周线子信号

| 新信号 | 满分 | 逻辑 | 边界条件 |
|--------|------|------|----------|
| `weekly_macd_divergence` | +8/-8 | 周线 MACD 底背离/顶背离（20周回看） | 数据不足 → 0；NaN → 跳过 |
| `weekly_ma_align` | +6 | 周线 MA5>MA10>MA20 多头排列 | 任一 MA 为 NaN → 中性 3 分 |
| `weekly_rsi` | +6 | 周线 RSI 在 40-60 健康区间 | 数据不足 → 中性 3 分 |

**总分上限不变**（仍为 100）：从现有信号中各扣一点让出 20 分空间。
- `macd_cross`: 12 → 10（让出 2）
- `rsi`: 12 → 10（让出 2）
- `ma`: 10 → 8（让出 2）
- 新增 3 个信号共 20 分

**数据来源**: 调用 `utils.resample_to_weekly()` + `utils.compute_weekly_indicators()`

**向后兼容**: 新信号权重为 0 时等价于旧版。`describe()` 方法同步新增周线标签。

#### Phase 2B-1：MoneyFlowFactor 准周线增强

**文件**: `src/discovery/factors/money_flow_factor.py`

**改动**: 新增 5 日资金流趋势信号（独立于周线基础设施，不依赖 Phase 1）

| 新信号 | 满分 | 逻辑 |
|--------|------|------|
| `5d_major_trend` | +15 | 5 日主力净流入趋势斜率 > 0 → 累计正分 |
| `5d_elg_consistency` | +10 | 5 日超大单净流入天数 / 5，≥3 天为正 |

**总分调整**: 现有信号满分从 100 降到 75，新增信号占 25 分。

**数据来源**: 复用现有 DB 查询，仅改日期范围为前 5 个交易日

**边界条件**：
- 数据不足 5 天 → 降级为单日评分（现有逻辑）
- 某天数据缺失 → 跳过该天，用剩余天数计算
- 5 日全缺失 → 新信号得 0 分

#### Phase 2B-2：ChipFactor 扩展回看窗口

**文件**: `src/discovery/factors/chip_factor.py`

**改动**: 扩展 `_LOOKBACK_DAYS` 从 5 到 10，新增筹码趋势信号（独立于周线基础设施，不依赖 Phase 1）

| 新信号 | 满分 | 逻辑 |
|--------|------|------|
| `wr_10d_trend` | +10 | winner_rate 10 日线性回归斜率 |
| `cost50_migration` | +5 | cost_50pct 10 日移动方向 |

**总分调整**: 现有信号满分从 100 降到 85，新增信号占 15 分。

**边界条件**：
- 数据不足 10 天 → 回退到 5 天窗口
- 5 天也不足 → 新信号得 0 分

---

### Phase 3：StockScorer 周线融合

**优先级: P1**

#### 3.3.1 market_score 双时间尺度融合

**文件**: `src/services/stock_scorer.py`

**改动**: `score()` 方法中融合日线和周线大盘评分

```python
def _calc_market_score(self) -> float:
    """大盘环境评分：日线 60% + 周线 40%。"""
    daily_score = self._calc_daily_market_score()

    # 周线评分（新增）
    weekly_score = self._calc_weekly_market_score()

    # 周线数据不可用时，降级为纯日线评分
    if weekly_score == 50.0 and getattr(self, '_weekly_index_closes', None) is None:
        return daily_score

    return daily_score * 0.6 + weekly_score * 0.4

def _calc_weekly_market_score(self) -> float:
    """周线级别大盘评分：基于周线 MA5/MA10/MA20 偏离度。"""
    closes = getattr(self, '_weekly_index_closes', None)
    if closes is None or len(closes) < 10:
        return 50.0

    price = float(closes[-1])
    ma5 = float(np.mean(closes[-5:]))
    ma10 = float(np.mean(closes[-10:]))
    ma20 = float(np.mean(closes[-20:])) if len(closes) >= 20 else ma10

    dev5 = (price - ma5) / ma5 if ma5 > 0 else 0
    dev10 = (price - ma10) / ma10 if ma10 > 0 else 0
    dev20 = (price - ma20) / ma20 if ma20 > 0 else 0

    score = 50 + (dev5 * 200 + dev10 * 150 + dev20 * 100) / 3
    return min(max(score, 0), 100)

def preload_weekly_index(self, weekly_closes: np.ndarray) -> None:
    """预加载大盘周线收盘价（用于周线级别 market_score）。"""
    self._weekly_index_closes = weekly_closes
```

**边界条件**：
- 周线数据不可用 → 降级为纯日线评分（向后兼容）
- 周线数据不足 10 周 → 周线评分返回 50.0，不影响融合

#### 3.3.2 regime 判断软过滤

**文件**: `src/services/stock_scorer.py`

**改动**: `_judge_sector_regime()` 接收周线趋势作为输入参数，调整下降趋势判定阈值

```python
def _judge_sector_regime(
    self, sector_pct: float, hist_closes: Optional[np.ndarray],
    breadth: Optional[Dict[str, float]] = None,
    momentum_acc: float = 0.0,
    index_pct: float = 0.0,
    vol_info: Optional[Dict[str, float]] = None,
    weekly_trend: Optional[str] = None  # 新增：周线趋势（来自 WeeklyTrend）
) -> str:
    # ... 现有逻辑全部保留（数据不足判断、ma20/trend_strength 计算、breadth/ad_ratio）...

    # 根据周线趋势软调整判定阈值
    if weekly_trend == "strong_down":
        # 周线空头排列 → 更敏感地进入 bearish/decelerating
        trend_threshold = 0.005  # 原来 0.008，降低门槛 0.003
        decel_threshold = 0.005   # 原来 0.003，下跌减速信号更早触发
    elif weekly_trend == "strong_up":
        # 周线多头排列 → 放宽上升趋势判定，抑制 deceleration
        trend_threshold = 0.008  # 保持原始阈值
        decel_threshold = -0.002 # 原来 -0.003，负值更大（绝对值更小）→ 更难触发减速
    else:
        trend_threshold = 0.008
        decel_threshold = -0.003

    # 1. 弱势判断（三条件取二）... 保持不变 ...

    # 2. 上升趋势：价格 > MA20 + 趋势强
    if hist_closes[-1] > ma20 and trend_strength > trend_threshold:
        if momentum_acc > 0.003:
            stage = self._calc_acceleration_stage(hist_closes, momentum_acc)
            return f"accelerating_{stage}"
        elif momentum_acc < decel_threshold:
            return "decelerating"
        else:
            return "strong_stable_up"

    # 3. 下降趋势：价格 < MA20 + 趋势强
    if hist_closes[-1] < ma20 and trend_strength > trend_threshold:
        if momentum_acc > 0.003:
            return "decelerating"
        else:
            return "bearish"

    # 4. 高波动判断（使用 vol_info）... 保持不变 ...
    if vol_info is None:
        vol_info = self._calc_long_term_vol_percentile(hist_closes)
    if vol_info["vol_regime"] in ("high", "extreme"):
        return "high_volatility"

    # 5. 震荡市 ... 保持不变 ...
```

**关键设计**：
- 不新增 regime 名称，现有 12 种形态体系完整保留
- 只在边界（threshold）处做软调整
- `weekly_trend=None`（无数据）时完全向后兼容

---

## 四、改动文件清单

| 文件 | Phase | 改动类型 | 改动量 |
|------|-------|----------|--------|
| `src/discovery/engine.py` | 0 | BUG 修复（修列顺序） | ~15 行 |
| `src/discovery/factors/utils.py` | 1 | 新建：聚合 + 趋势判断 | ~120 行 |
| `src/discovery/factors/technical_factor.py` | 2A-1 | 新增周线子信号 | ~60 行 |
| `src/discovery/factors/money_flow_factor.py` | 2A-2 | 新增 5 日趋势信号 | ~40 行 |
| `src/discovery/factors/chip_factor.py` | 2A-3 | 扩展回看窗口 | ~30 行 |
| `src/services/stock_scorer.py` | 3 | 周线 market_score + regime 软过滤 | ~50 行 |

**总改动量**: ~315 行（原方案 ~560 行）

**删除的内容**（相比原方案）：
- ~~Phase 4（盘中因子周线过滤器）~~：StockScorer 的 regime 体系已覆盖，叠床架屋
- ~~Phase 5（时间尺度敏感度）~~：与动态权重形成循环依赖，难以解释效果

---

## 五、测试策略

### 5.1 单元测试

```python
# tests/test_weekly.py

class TestResampleWeekly:
    def test_normal_5day_week(self):
        """标准 5 个交易日一周的聚合"""

    def test_short_week_holiday(self):
        """节假日导致的短周（如春节前只有 2 个交易日）"""

    def test_single_day_week(self):
        """极端情况：一周只有 1 个交易日"""

    def test_empty_dataframe(self):
        """空 DataFrame 输入"""

    def test_missing_columns(self):
        """缺少 volume/amount 列"""

    def test_nan_values(self):
        """日线数据中包含 NaN"""

    def test_thu_boundary(self):
        """周线聚合的边界是周五（weekday() == 4）"""

class TestWeeklyTrend:
    def test_strong_up(self):
        """MA5 > MA10 > MA20 判定为 STRONG_UP"""

    def test_strong_down(self):
        """MA5 < MA10 < MA20 判定为 STRONG_DOWN"""

    def test_insufficient_data(self):
        """数据不足 10 周返回 RANGE"""

    def test_all_nan_ma(self):
        """所有 MA 为 NaN"""

class TestWeeklyIndicators:
    def test_macd_calculation(self):
        """周线 MACD 与手动计算一致"""

    def test_rsi_boundaries(self):
        """RSI 在 0-100 范围内"""

    def test_boll_width(self):
        """BOLL 上轨 > 中轨 > 下轨"""

class TestMoneyFlowTrend:
    def test_5d_consecutive_inflow(self):
        """5 日连续净流入"""

    def test_3_missing_days(self):
        """5 日中 3 日数据缺失"""

    def test_zero_volume_days(self):
        """成交量为 0 的交易日"""

### 5.2 集成测试

```bash
# Phase 0 验证：market_score 不再全 50
python main.py --discover-only --debug 2>&1 | grep -E "market_score"

# 检查 market_score 分布
python -c "
import sqlite3
conn = sqlite3.connect('data/stock_analysis.db')
rows = conn.execute('SELECT market_score FROM scan_result_postmarket ORDER BY scan_date DESC LIMIT 100').fetchall()
scores = [r[0] for r in rows if r[0] is not None]
print(f'market_score: min={min(scores):.1f}, max={max(scores):.1f}, mean={sum(scores)/len(scores):.1f}')
print(f'非50分占比: {sum(1 for s in scores if abs(s - 50) > 1) / len(scores) * 100:.1f}%')
"
```

### 5.3 边界条件测试清单

| 场景 | 预期行为 | 测试方法 |
|------|----------|----------|
| 数据库无日线数据 | 所有周线信号返回中性值 | 清空 stock_daily 后运行 |
| 只有 3 周数据 | 周线 MA20 返回 NaN，趋势判定为 RANGE | 截断数据到 15 天 |
| 节假日短周 | 聚合正确（2-3 天也能聚合成 1 周） | 构造春节前后数据 |
| 某只股票数据缺失 | 该股票跳过周线信号，不影响其他股票 | 删除单只股票数据 |
| 盘中扫描模式 | 不执行周线计算（性能要求） | 运行 --scan 模式 |
| 周一开盘（周线未闭合） | 使用上周五闭合的周线数据 | 日期为周一 |

---

## 六、风险评估与回滚

### 6.1 风险矩阵

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 周线聚合逻辑错误 | 中 | 中 | 单元测试覆盖 + 与 Tushare 周线 API 对比验证 |
| market_score 修复后评分大幅变化 | 高 | 低 | 这是预期行为，修复前所有评分都是错误的 |
| 趋势阈值调整过于激进 | 中 | 中 | `weekly_trend=None` 时完全向后兼容，默认不变 |
| 性能回退 | 低 | 低 | 盘中模式不引入周线计算 |

### 6.2 回滚方案

**Phase 0（BUG 修复）**: 不回滚。这是修复，不是功能变更。

**Phase 1-3（功能增强）**:
- 周线信号都有独立的开关（通过 weight=0 禁用）
- `weekly_trend=None` 时 regime 逻辑完全向后兼容
- 如果发现异常，只需将新因子的 weight 设为 0 即可回滚

**快速回滚命令**:
```bash
# 通过环境变量禁用周线增强
export WEEKLY_FACTORS_ENABLED=false
```

在 `engine.py` 中检查此环境变量，为 false 时跳过所有周线相关逻辑。

---

## 七、实施顺序与依赖

```
Phase 0 (BUG 修复) ─── 独立，0.5 天，必须优先
    ↓
Phase 1 (基础设施) ─── 1 天，Phase 2A 的前置依赖
    │
    ├── Phase 2A-1 (TechnicalFactor) ─── 1 天，依赖 Phase 1
    ├── Phase 2A-2 (MoneyFlowFactor) ─── 0.5 天，独立（准周线，不依赖 Phase 1）
    └── Phase 2A-3 (ChipFactor) ─── 0.5 天，独立（扩展日线回看，不依赖 Phase 1）
         ↓
Phase 3 (StockScorer 融合) ─── 0.5 天，依赖 Phase 1
    ↓
测试 & 集成 ─── 1 天

总计：~5 天（原方案 8.5 天）
```

---

## 八、未来扩展（不在本方案范围内）

1. **月线级别数据**: 在周线基础上进一步聚合，用于判断季度级别的趋势
2. **多指数周线判断**: 不仅看上证指数，还看沪深 300、创业板指的周线趋势
3. **板块周线排名**: 计算各板块周线涨跌幅排名，用于 SectorFactor 增强
4. **周线级别 R&D 因子**: 在 FactorCoder 中引导 LLM 生成使用周线数据的因子
5. **周线级别回测**: 在 FactorEvaluator 中支持周线级别的回测窗口