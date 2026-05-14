# StockScorer 多维技术评分设计文档

> 源码：`src/services/stock_scorer.py`
> 测试：`scripts/test_scorer.py`（91 个用例）
> 更新：2026-05-14

---

## 1. 总体架构

StockScorer 对候选股计算 **6 个维度**的 0-100 技术评分，通过**动态权重**加权求和输出 `tech_score`。

核心特点：
- 板块级别动态权重，不依赖大盘统一判定
- 11 种市场形态，每种有独立权重分配
- 所有权重加总恒等于 1.0
- 加速形态区分初期/末期，策略截然不同

计算流程：
```
输入: (stock_code, sector, price, pre_close, tp1, tp2, stop_loss, reasons, ohlcv, volume_ratio)
  │
  ├─ 1. 计算 6 个维度分（各 0-100）
  │     rr_score, market_score, sector_score, volume_score, position_score, formation_score
  │
  ├─ 2. 判定市场形态 → 选择动态权重
  │     _get_dynamic_weights(sector, sector_score)
  │
  └─ 3. 加权求和
        tech_score = Σ(weight_i × score_i)
```

---

## 2. 六个评分维度

### 2.1 RR 赔率评分 (`_calc_rr_score`)

**公式：**
```
RR = (TP1 - price) / (price - stop_loss)
score = min(RR / 2.0, 1.0) × 100
```

| RR 值 | 得分 | 含义 |
|-------|------|------|
| ≤ 0 | 0 | 止损价已破或无盈利空间 |
| 0.5 | 25 | 赔率偏低 |
| 1.0 | 50 | 赔率 1:1 |
| 1.5 | 75 | 赔率良好 |
| ≥ 2.0 | 100 | 赔率优秀（封顶） |

边界条件：
- `price ≤ stop_loss` → 0
- `tp1 ≤ price` → 0
- `RR > 2` → cap at 100

### 2.2 大盘环境评分 (`_calc_market_score`)

**公式：**
```
score = 0.6 × min(price / MA20 × 50, 100) + 0.4 × min(price / MA60 × 50, 100)
```

| 条件 | 得分范围 |
|------|---------|
| 价格 > MA20 且 > MA60 | 50-100 |
| 价格在 MA20 附近 | ~50 |
| 价格 < MA20 且 < MA60 | 0-50 |

数据来源：上证指数 OHLCV（通过 `preload_index_ohlcv` 预加载）

边界条件：
- 无数据 → 50（中间值）
- 最终 clip 到 [0, 100]

### 2.3 板块评分 (`_calc_sector_score`)

**公式（波动率标准化）：**
```
sector_vol = std(diff(hist_closes) / hist_closes)   # 板块 20 日收益率标准差
vol_scale = max(sector_vol × 100, 1.0)              # 转为百分比，最低 1%
abs_score = (stock_pct / vol_scale + 1) / 2 × 100   # 标准化绝对涨幅分
rel_score = (stock_pct - sector_pct) × 10 + 50      # 相对强弱分
score = abs_score × 0.4 + rel_score × 0.6
```

| 场景 | abs_score | rel_score | 综合 |
|------|-----------|-----------|------|
| 个股+10%, 板块0% (vol=2%) | 100 | 100 | 100 |
| 个股0%, 板块0% | 50 | 50 | 50 |
| 个股-10%, 板块+5% | 0 | 0 | 0 |
| 个股+2%, 板块0% (银行 vol=1%) | 100 | 70 | 82 |
| 个股+2%, 板块0% (半导体 vol=4%) | 75 | 70 | 72 |

权重分配：相对强弱占 60%（跑赢板块更重要），绝对涨幅占 40%。
波动率标准化消除了板块间波动率差异：高波动板块（半导体）同样涨幅得分更低，低波动板块（银行）更容易得高分。

### 2.4 量能评分 (`_calc_volume_score`)

**连续函数（替代原 5 档离散打分）：**
```
vol_signal = (volume_ratio - 1.0) × price_pct
score = 65 + 25 × tanh(vol_signal × 0.8)
```

| 场景 | vol_signal | tanh | 得分 |
|------|-----------|------|------|
| 量比2.0 涨2% | 2.0 | 0.96 | 89 |
| 量比2.0 跌2% | -2.0 | -0.96 | 41 |
| 量比0.5 涨2% | -1.0 | -0.76 | 46 |
| 量比1.0 平盘 | 0.0 | 0.0 | 65 |
| 量比3.0 涨5% | 10.0 | ~1.0 | 90 |

边界条件：
- `volume_ratio ≤ 0` 或 `pre_close ≤ 0` → 50

### 2.5 位置评分 (`_calc_position_score`)

**核心指标：ATR 标准化的天花板距离**
```
BOLL上轨 = MA20 + 2σ
BOLL下轨 = MA20 - 2σ
distance = (BOLL上轨 - price) / ATR(14)
```

**基础分：**

| distance | 得分 | 含义 |
|----------|------|------|
| ≥ 2.0 ATR | 85 | 空间充裕 |
| ≥ 1.0 ATR | 75 | 合理空间 |
| ≥ 0 ATR | 60 | 逼近上轨 |
| < 0（突破） | 见下表 | 量能决定真假突破 |

**突破上轨时的量能判断：**

| 量比 | 得分 | 含义 |
|------|------|------|
| > 2.0 | 70 | 放量突破 → 主升浪 |
| > 1.0 | 55 | 普通突破 |
| ≤ 1.0 | 35 | 缩量突破 → 假突破/衰竭 |

**修正因子：**

| 条件 | 调整 | 原因 |
|------|------|------|
| 价格在下轨附近且回到 MA20 上方 | +10 | 下轨反弹信号 |
| 0 ≤ distance < 0.5 ATR | -15 | 贴近上轨，空间极小 |
| price ≥ TP1 | -20 | 已达止盈位，无盈利空间 |

最终 clip 到 [0, 100]。

### 2.6 形态评分 (`_calc_formation_score`)

**关键词匹配规则：**

| 关键词 | 加分 | 含义 |
|--------|------|------|
| "均线多头排列" | +30 | 强趋势确认 |
| "回踩MA5均线" | +20 | 回踩支撑 |
| "BOLL中轨支撑" | +15 | 中轨支撑 |
| "强势" / "量价齐升" / "放量" | +15 | 量能确认 |
| "MACD金叉" | +15 | MACD 金叉信号 |
| "KDJ金叉" | +10 | KDJ 金叉（不与"金叉"双重匹配） |
| "金叉"（其他） | +15 | 其他金叉信号 |
| "均线粘合" | +10 | 均线粘合 |
| "KDJ" + "超卖" | +10 | 超卖反弹 |
| "涨停" | -30 | 已涨停无空间 |

基础分 = 20（无信号 ≠ 极差，避免空理由拖累 composite）。
金叉关键词使用 elif 链避免双重匹配。
最终 clip 到 [0, 100]。

---

## 3. 市场形态判定

### 3.1 判定流程 (`_judge_sector_regime`)

**层级化判定**：先判宏观趋势方向，再在趋势内部判加速子阶段。

```
1. 数据不足（< 20 日） → range_bound
2. 弱势判断（三条件取二，最高优先级）:
   - 板块单日跌幅 > 7%
   - 涨跌比 < 0.5
   - 大盘单日跌幅 > 3%
   满足任意两条 → weak
3. 上升趋势（价格 > MA20 且 trend_strength > 0.008）:
   a. momentum_acc > 0.003 → accelerating_early 或 accelerating_late
   b. momentum_acc < -0.003 → decelerating（上涨减速）
   c. 其他 → strong_stable_up（稳定上升）
4. 下降趋势（价格 < MA20 且 trend_strength > 0.008）:
   a. momentum_acc > 0.003 → decelerating（下跌减速/见底）
   b. 其他 → bearish（持续弱势）
5. 波动率 high/extreme（长期百分位） → high_volatility
6. trend_strength < 0.003 且 20日波动率 < 2% → range_bound
7. 默认 → range_bound
```

**12 种形态**：range_bound, weak, strong_stable_up, accelerating_early, accelerating_late, decelerating, bearish, high_volatility, crisis, calm

### 3.2 辅助指标

**趋势强度 (`_calc_trend_strength`)：**
```
daily_returns = diff(closes[-5:]) / closes[-5:-1]
trend_strength = abs(mean(daily_returns))
```

**动量加速度 (`_calc_momentum_acceleration`)：**
```
returns = diff(closes) / closes[:-1]
recent_momentum = mean(returns[-3:])
prev_momentum = mean(returns[-6:-3])
acceleration = recent_momentum - prev_momentum
```
- 正值 = 趋势加速
- 负值 = 趋势减速

**市场宽度 (`_calc_market_breadth`)：**
```
涨跌比 = 涨家数 / 跌家数（从 stock_daily 最新数据）
新高新低比 = 一年新高数 / 一年新低数
breadth_score = ad_score × 0.6 + hl_score × 0.4
```
其中：
```
ad_score = 50 + 30 × log2(ad_ratio)    # ratio=2→80, ratio=1→50, ratio=0.5→20
hl_score = 50 + 20 × log2(hl_ratio)
```

**波动率聚类 (`_calc_volatility_regime`)：**
```
EWMA 条件方差（λ=0.94）:
  ewma_var[t] = 0.94 × ewma_var[t-1] + 0.06 × return[t]²

当前波动率 = √ewma_var[-1] × √252（年化）
百分位 = 历史 ewma_var 中 ≤ 当前值的比例

regime:
  percentile > 90 → "extreme"
  percentile > 70 → "high"
  percentile < 20 → "low"
  其他 → "normal"
```

**长期波动率百分位 (`_calc_long_term_vol_percentile`)：**

用于 regime 判定中的高波动检测（替代 `_calc_volatility_regime` 的短窗口自引用）。
```
数据源：_get_sector_hist_closes 返回的 40-60 个数据点
EWMA 方差序列：40+ 个数据点
百分位 = 历史 ewma_var 中 ≤ 当前值的比例
```
比 `_calc_volatility_regime`（15 个数据点）更稳定，百分位更有意义。

**RSI (`_calc_rsi` / `_calc_rsi_series`)：**
```
returns = diff(closes) / closes[:-1]
for i in range(period, len(returns)):
    window = returns[i-period:i]
    avg_gain = mean(positive returns in window)
    avg_loss = mean(negative returns in window)
    RS = avg_gain / avg_loss
    RSI[i] = 100 - 100 / (1 + RS)
```
`_calc_rsi` 返回最新值，`_calc_rsi_series` 返回完整时间序列（用于加速阶段判定的 RSI 动量方向）。

### 3.3 加速阶段判定 (`_calc_acceleration_stage`)

当形态为 "accelerating" 时，进一步判定初期 vs 末期。

**三个信号 → late_score 累计：**

| 信号 | +1 | +2 |
|------|----|----|
| MA20 乖离率 | > 5% | > 10% |
| RSI 动量方向 | abs(RSI变化) < 0.5×rsi_std（走平） | RSI 变化 < -1.0×rsi_std（下降） |
| 连续加速天数 | > 3 天 | > 5 天 |

**RSI 动量方向计算（波动率缩放阈值）：**
```
rsi_series = _calc_rsi_series(hist_closes)   # 每日 RSI 值
rsi_recent = mean(rsi_series[-5:])           # 近 5 日均值
rsi_prev   = mean(rsi_series[-10:-5])        # 前 5 日均值
rsi_change = rsi_recent - rsi_prev
rsi_std    = std(rsi_series)                 # RSI 历史标准差
rsi_std    = max(rsi_std, 3.0)               # 最低 3 防止阈值过小
```
- `rsi_change < -1.0 × rsi_std` → RSI 明显下降，动能背离 → +2
- `abs(rsi_change) < 0.5 × rsi_std` → RSI 真正走平，动能衰减 → +1
- `rsi_change ≥ 0.5 × rsi_std` → RSI 仍在上升，动能确认 → +0

波动率缩放效果：低波动板块阈值更敏感（±2），高波动板块过滤噪音（±10）。

**判定规则：**
- `late_score ≥ 3` → **late**（后期，谨慎）
- `late_score < 3` → **early**（初期，追涨）

**设计理由：**
- 旧方案用 RSI 绝对值（> 65 / > 75），但纯上涨序列 RSI 恒为 100，无法区分初期和后期
- 新方案用 RSI 变化方向：RSI 仍在上升 = 动能确认（初期），RSI 走平或下降 = 动能背离（后期）
- 三档阈值用 RSI 历史标准差缩放：`abs(rsi_change) < 0.5σ` 判为走平，比固定 ±3 更适应不同波动率板块
- 匀速上涨（无回调）时 RSI 稳定在高位但变化≈0，会被判为走平（+1），配合乖离率和天数仍可能判为 early

### 3.4 大盘状态判定

**危机 (`_is_crisis`)：**
- 近 5 日累计跌幅 > 10% → True
- 波动率百分位 > 90（extreme） → True

**平稳 (`_is_calm`)：**
- 波动率百分位 < 20（low） → True

---

## 4. 动态权重体系

### 4.1 全部 12 种形态的权重分配

每组权重加总恒等于 1.0（100%）。

| 形态 | rr | market | sector | volume | position | form | 合计 |
|------|-----|--------|--------|--------|----------|------|------|
| **基准** | 30 | 20 | 15 | 15 | 10 | 10 | 100 |
| **稳定上升↑** | 25 | 20 | 25 | 10 | 10 | 10 | 100 |
| **稳定上升↓** | 30 | 20 | 10 | 15 | 15 | 10 | 100 |
| **加速初期** | 20 | 20 | 25 | 15 | 10 | 10 | 100 |
| **加速后期** | 35 | 15 | 10 | 15 | 15 | 10 | 100 |
| **减速** | 35 | 15 | 10 | 15 | 20 | 5 | 100 |
| **弱势** | 35 | 20 | 5 | 15 | 15 | 10 | 100 |
| **bearish** | 35 | 20 | 5 | 15 | 15 | 10 | 100 |
| **高波动** | 40 | 15 | 10 | 15 | 10 | 10 | 100 |
| **危机** | 40 | 25 | 5 | 15 | 15 | 0 | 100 |
| **平稳** | 25 | 10 | 15 | 20 | 20 | 10 | 100 |

注：
- **稳定上升** 使用 lerp 插值（软切换），根据板块涨跌幅在基准和目标之间平滑过渡
- **高波动** 使用 lerp 插值，根据 sector_score 在基准和高波动目标之间过渡
- 其他形态直接返回固定权重

### 4.2 各形态的策略思路

| 形态 | 核心策略 | RR | 板块 | 位置 | 关键变化 |
|------|---------|-----|------|------|---------|
| 基准 | 均衡配置 | 30 | 15 | 10 | 默认 |
| 稳定上升↑ | 跟板块 | 25↓ | 25↑ | 10 | 追板块动量 |
| 稳定上升↓ | 防守 | 30 | 10↓ | 15↑ | 降板块风险 |
| 加速初期 | 追涨 | 20↓ | 25↑ | 10 | 敢追，RR 让给板块 |
| 加速后期 | 防回撤 | 35↑ | 10↓ | 15↑ | 不追，RR+位置兜底 |
| 减速 | 控仓位 | 35↑ | 10↓ | 20↑ | RR+位置保命 |
| 弱势 | 不碰板块 | 35↑ | 5↓ | 15↑ | 板块风险最低 |
| bearish | 持续弱势 | 35↑ | 5↓ | 15↑ | 下降趋势防守 |
| 高波动 | 稳赔率 | 40↑ | 10↓ | 10 | 混乱市看赔率 |
| 危机 | 最保守 | 40↑ | 5↓ | 15↑ | 形态分为 0 |
| 平稳 | 看量和位 | 25↓ | 15 | 20↑ | 低波动看量能位置 |

### 4.3 权重切换机制

**硬切换：** 直接返回目标权重（弱、减速、加速初期/后期、危机、平稳）

**软切换（lerp）：** 强趋势和高波动使用线性插值
```python
# 强趋势：根据板块涨跌幅插值
t = min(abs(sector_pct) / 3.0, 1.0)
weights = lerp(base_weights, target_weights, t)

# 高波动：根据 sector_score 插值
t = min(max((100 - sector_score) / 50.0, 0.0), 1.0)
weights = lerp(base_weights, high_vol_weights, t)
```
lerp 保证权重连续变化，避免硬跳导致的排序不稳定。

---

## 5. 辅助指标详情

### 5.1 ATR(14) 计算
```python
TR = max(high - low, |high - prev_close|, |low - prev_close|)
ATR = mean(TR[-14:])
```
用于 position_score 的天花板距离标准化。

### 5.2 BOLL 通道
```python
MA20 = mean(closes[-20:])
σ = std(closes[-20:])
上轨 = MA20 + 2σ
下轨 = MA20 - 2σ
```

### 5.3 板块历史收盘价
从 `sector_daily` 表读取近 90 日数据，截取最近 60 日收盘价。
用于趋势强度、动量加速度、RSI、波动率百分位等多指标计算。
60 日给 EWMA 百分位约 59 个数据点，百分位排序才有意义（20 日只有 19 个点，粒度太粗）。

---

## 6. 数据流与缓存

```
preload_sector_pct()      → _sector_pct_cache     # 板块当日涨跌幅（每轮更新一次）
preload_index_ohlcv()     → _index_ohlcv          # 大盘 OHLCV（每轮更新一次）
_calc_index_pct()         → 单日涨跌幅（从 _index_ohlcv 实时计算，无额外缓存）
_get_sector_hist_closes() → _sector_hist_cache    # 板块历史收盘价（按板块名缓存，同板块只查一次 DB）
_calc_market_breadth()    → _breadth_cache        # 市场宽度（全轮次只查一次 DB）
```

**缓存策略：**
- `_sector_hist_cache`: `Dict[str, Optional[np.ndarray]]`，按板块名缓存，同板块 300 只股票只查 1 次 DB
- `_breadth_cache`: 单值缓存，整个扫描轮次只执行一次全市场聚合查询
- 缓存生命周期：每个 `StockScorer` 实例（即每个扫描轮次）

---

## 7. 环境变量配置

通过 `DiscoveryConfig` 注入 `StockScorerConfig`：

| 环境变量 | 默认值 | 含义 |
|---------|--------|------|
| `ENABLE_STOCK_SCORER` | False | 是否启用多维技术评分 |
| `DISCOVER_SCORER_WEIGHT_RR` | 0.30 | RR 维度基准权重 |
| `DISCOVER_SCORER_WEIGHT_MARKET` | 0.20 | 大盘维度基准权重 |
| `DISCOVER_SCORER_WEIGHT_SECTOR` | 0.15 | 板块维度基准权重 |
| `DISCOVER_SCORER_WEIGHT_VOLUME` | 0.15 | 量能维度基准权重 |
| `DISCOVER_SCORER_WEIGHT_POSITION` | 0.10 | 位置维度基准权重 |
| `DISCOVER_SCORER_WEIGHT_FORMATION` | 0.10 | 形态维度基准权重 |

---

## 8. 输出格式

```python
TechScoreResult(
    rr_score=80.0,       # 赔率评分
    market_score=60.0,   # 大盘环境
    sector_score=90.0,   # 板块强弱
    volume_score=70.0,   # 量能质量
    position_score=65.0, # 位置评估
    formation_score=55.0,# 形态确认
    composite=72.5       # 加权总分（tech_score）
)
```

`to_dict()` 输出：
```json
{
    "tech_score": 72.5,
    "rr_score": 80.0,
    "market_score": 60.0,
    "sector_score": 90.0,
    "volume_score": 70.0,
    "position_score": 65.0,
    "formation_score": 55.0
}
```

---

## 9. 已知限制

1. **~~RSI 在纯上涨序列中恒为 100~~** — 已通过 RSI 动量方向（变化率）解决，见 3.3 节
2. **~~市场宽度每次查 DB~~** — 已加 `_breadth_cache`，同轮次只查一次
3. **~~板块历史数据无缓存~~** — 已加 `_sector_hist_cache`，按板块名缓存
4. **~~形成评分基础分为 0~~** — 已改为 20，空理由不再拖累 composite
5. **~~RSI 动量阈值为固定值~~** — 已用 RSI 历史标准差缩放，见 3.3 节
6. **~~板块评分无波动率标准化~~** — 已用板块 20 日收益率标准差标准化，见 2.3 节
7. **~~波动率百分位自引用~~** — 已新增 `_calc_long_term_vol_percentile` 使用 40+ 数据点

---

## 10. 测试覆盖

`scripts/test_scorer.py` 覆盖 18 个测试组、91 个用例：

| 测试组 | 用例数 | 覆盖内容 |
|--------|--------|---------|
| RR 赔率评分 | 7 | 正常、边界、异常 |
| 大盘环境评分 | 3 | 无数据、上涨、下跌 |
| 板块评分 | 4 | 高分、低分、持平、波动率标准化 |
| 量能评分 | 6 | 连续函数各场景 |
| 位置评分 | 5 | 中轨、突破、下轨、数据不足 |
| 形态评分 | 7 | 基线20、看涨、看跌、混合、金叉去重、BOLL中轨、成交量显著放大 |
| 市场形态判定 | 10 | 层级化：weak三条件取二(3)、strong_stable_up、accelerating、decelerating、bearish、range_bound、数据不足 |
| 加速阶段判定 | 6 | 初期、末期、边界、RSI 动量上升→early、匀速上涨→early、RSI 走平→late |
| 权重归一化 | 11 | 所有形态加总=1.0（含 bearish） |
| Composite 极端 | 3 | 高分、低分、差距 |
| RSI 计算 | 4 | 上涨、下跌、震荡、数据不足 |
| 动量加速度 | 2 | 加速、减速 |
| 波动率聚类 | 3 | 低波动、高波动、数据不足 |
| 权重插值 | 4 | t=0, t=0.5, t=1, 归一化 |
| 形态优先级 | 4 | 弱势>strong_stable_up>accelerating、bearish |
| 极端输入 | 5 | 边界值处理 |
| 长期波动率百分位 | 4 | 低波动、高波动、数据不足、长窗口稳定性 |
| 层级化 regime | 2 | accelerating_early 可触发、下降趋势 decelerating |
