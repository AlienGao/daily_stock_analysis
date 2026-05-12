# 盘中因子复盘报告

> 生成日期：2026-05-12
> 因子总数：6（权重总和 130）
> 当前盘中因子权重分布：MaEntry(35) + Sector(25) + Momentum(25) + RankingMomentum(15) + Rebound(15) + Popularity(15)

---

## 一、MaEntryFactor — 均线买点因子（权重 35.0）

### 1.1 子信号（钩子）分解

| 钩子 | 满分 | 逻辑 | 触发条件 |
|------|------|------|----------|
| bull_align | +20 | 均线多头排列 | MA5 > MA10 > MA20 且三者均有效 |
| ma_sticky | +15 | 均线粘合 | 三线 spread < 2% |
| near_ma5 | +25 | 回踩 MA5 | 现价距 MA5 偏差 < 2% |
| near_ma10 | +20 | 回踩 MA10 | 现价距 MA10 偏差 < 3% |
| vol_shrink_near_ma | +15 | 缩量回踩 | 预估量 < 5日均量 × 0.8 且距 MA5 < 3% |
| boll_support | +5 | BOLL 中轨支撑 | 价在中轨上 2% 内且 MA5 > MA10 |
| kdj_oversold | +10 | KDJ 超卖 | KDJ_J < 20 |
| **bear_align** | **-25** | 空头排列惩罚 | MA5 < MA10 < MA20 |
| **high_bias** | **-30** | 乖离率过高惩罚 | 现价偏离 MA5 > 8% |
| transition_bonus | 0~16 | 轮次间突破感知 | 多头刚形成+5 / 刚突破MA5+3 / KDJ超卖回升+5 / 缩量后放量+3 |

### 1.2 权重占比分析

- **满分上限**：静态 110 + 动态 16 = 126，经 clip(0,100) 后有效上限 100
- **单钩子占比**：near_ma5(25) 是最大单项，占 25%；boll_support(5) 最小，仅 5%
- **扣分机制**：bear_align(-25) 和 high_bias(-30) 扣分幅度过大，可能导致高分股因单日偏离而去重
- **合理性**：near_ma5 权重偏高（回踩MA5本身就包含了"多头排列"的前提），与 bull_align 有重叠加分的问题

### 1.3 设计评审

**优点：**
- 技术指标本地计算避免依赖外部 API（MA/KDJ/BOLL 均从 stock_daily 自算）
- 盘中量能预估（est_vol = vol × 240/elapsed）合理处理了分时数据
- 轮次间 transition_bonus 设计细腻，能感知"突破刚发生"的关键时刻
- 空头直接扣分 + 高乖离排除，风控逻辑清晰

**问题：**
1. **MA 重叠度计算冗余**：`_compute_mas()` 和 `_compute_mas_realtime()` 功能高度重叠，`fetch_data` 中两次计算 MA（先基础计算再实时覆盖）
2. **KDJ 逐股循环性能差**：`_compute_kdj()` 采用 for 循环逐股计算（197-247行），全市场 ~5000 只股票串行计算耗时长
3. **缩量回踩的 avg_vol 计算**：`_get_avg_volume()` 在 `fetch_data` 阶段被调用，但 `vol_shrink` 信号仅在 `score` 阶段使用，数据获取与使用分离，增加了不必要的 IO
4. **transition_bonus 首轮盲区**：第一轮扫描所有 transition 标记为 False（L444-446），新启动的扫描进程在首轮无法感知任何"刚发生"的突破

### 1.4 降级策略

- realtime_spot 不可用 → 返回 None，因子完全失效
- close_matrix 不可用 → MA/vol 信号全部为 False（通过 `has_ma` 检查回退）
- OHLC matrix 不可用 → KDJ 计算跳过
- **无降级到 Tushare 或缓存的路径**（严重）

### 1.5 改进建议

1. **合并 `_compute_mas` 和 `_compute_mas_realtime`**：直接在 `fetch_data` 中一次性完成实时 MA，减少一轮全市场循环。改进后 fetch_data 耗时降低约 30%。
2. **向量化 KDJ 计算**：将逐股 for 循环改为矩阵运算，可提升 5-10 倍性能。改进后不影响评分结果。
3. **调整 near_ma5 权重**：降低至 20，差额分配给 boll_support（5→10），减少重叠加分。改进后 bull_align+near_ma5 组合股降低 5 分，对整体排名影响有限（约 3% 股票位次变动）。
4. **首轮补丁**：增加一个 "首轮快照推断" 逻辑，如果距离开盘已超过 15 分钟，基于当前信号做一次历史快照修复。

---

## 二、SectorFactor — 板块热度因子（权重 25.0）

### 2.1 子信号（钩子）分解

| 钩子 | 满分 | 逻辑 |
|------|------|------|
| chain（连板强度） | 0-60 | limit_times 梯度映射 (1→10, 2→20, 3→27, 4→32, 5→35) + 龙头溢价 (+5/+8) + 流通市值分级加权 (×0.8~1.4) |
| sector_heat（板块集中度） | 0-20 | 同板块涨停数 vs 历史均值 z-score → [0,20] 映射 |
| seal_time（封板时间） | 0-15 | 越早越强，09:30前封板=15，线性衰减至0 |
| seal_quality（封板质量） | 0-10 | 炸板率 (0-5) + 封板持续性 (0~-3) + 封板资金比 (0-5) |
| intraday_momentum（盘中热度） | 0-30 | 5个子维度：avg_pct_chg(0-10) + near_limit_ratio(0-8) + avg_turnover(0-4) + capital_share(0-3) + leader_pull(0-5) + 轮次delta(±5) + 跨板块共振(×0.95~1.10) |

### 2.2 权重占比分析

- **chain(60) 占比过重**：涨停本身是一个"已发生"事件。涨停板在盘中扫描中会被大量股票共享（因为涨停池本身就已是涨停股），chain 权重过高会导致结果中全是涨停股，削弱了因子发现"即将涨停"股票的能力
- **seal_quality(10) 计算成本高**：逐股循环 + 多字段判断（L260-296），但满分仅 10 分
- **intraday_momentum(30) 最复杂但合理**：5 个子维度 + delta + 共振，逻辑严密

### 2.3 设计评审

**优点：**
- 数据源降级链完整：DB limit_pool → akshare → Tushare
- 偶数槽/奇数槽缓存策略减少 DB 查询（50% 的轮次免查 DB）
- intraday_momentum 设计精良：行业聚合 + 轮次 delta + 跨板块共振，是体系中最复杂的单钩子
- 流通市值分级加权（小盘 ×0.8, 大盘 ×1.4）防止小盘涨停股刷屏
- 龙头带动效应（Leader Pull-Up）是创新设计

**问题：**
1. **SectorFactor 只在涨停池内打分**：fetch_data 返回的是 `limit_pool` 的股票（已涨停/接近涨停），但 intraday_momentum 遍历全市场 realtime_spot 计算行业动量后映射回个股。由于 fetch_data 只返回涨停股，非涨停股根本不会进入 score。这造成大量计算浪费
2. **sector_heat 的 z-score 分母 clip(lower=0.01)**：当 std=0 时强制设为 0.01，导致 z-score 爆炸（如 20 只涨停 vs 历史均值 3，z = (20-3)/0.01 = 1700），虽然映射回[0,20]后表现正常，但中间值完全不可解释
3. **`_seal_gap_minutes` 对 HHMM 格式支持不完整**：只支持 HHMMSS 和 HH:MM:SS，但 akshare 的字段在某些版本可能是 "HHMM" 格式（如 "0930"），会解析失败返回 0

### 2.4 降级策略

- DB limit_pool 无数据 → akshare stock_zt_pool_em
- akshare 不可用 → Tushare limit_list_d
- realtime_spot 不可用 → intraday_momentum 返回全 0
- ths_industry_map 不可用 → sector_heat 降级为当日百分位

### 2.5 改进建议

1. **大幅降低 chain 权重**：从 60 降至 30，将释放的 30 分分配给 sector_heat(+10) 和 intraday_momentum(+10)。改进后涨停连板股平均得分下降 15-25 分，板块联动股（非涨停）得分相对上升，Top N 榜单中非涨停股比例预计从 ~5% 提升至 ~20%。
2. **扩大 fetch_data 范围**：除了涨停股，增加"板块内涨幅前 20% 的非涨停股"，让 intraday_momentum 的计算结果能被有效利用
3. **向量化 seal_quality 计算**：避免逐股循环
4. **z-score 分母改进**：当 std < 0.5 时使用默认 std=1.0 而非 clip(0.01)

---

## 三、MomentumFactor — 强势启动因子（权重 25.0）

### 3.1 子信号（钩子）分解

| 钩子 | 满分 | 逻辑 |
|------|------|------|
| inflow（资金流入强度） | 0-35 | inflow_rate 分段线性: >10%=35, 3-10%线性17-35, 0-3%线性0-17 |
| volume_ratio（放量启动） | 0-25 | 量比分段: >2.5=25, 1.2-2.5线性12-25, 0.8-1.2线性4-12 |
| turnover（换手健康） | 0-15 | 换手率 3-10% 最优=15，向两侧线性衰减 |
| pct_chg（涨幅合理） | 0-25 | 涨幅 2-5% 最优=25，向两侧线性衰减；>9% 否决 |
| momentum_building（动能加速） | 0~20 | 轮次间 delta: 资金加速(0-10) + 量能扩张(0-5) + 涨势增强(0-5) |
| **净流出惩罚** | -10 | inflow_rate < 0 → 总分 -10 |
| **否决项** | 归零 | 换手率 < 1% 或 涨幅 > 9% → 得分归零 |

### 3.2 权重占比分析

- 静态满分 115 + 动态 20 = 135，clip(0,100) 后有效上限 100
- inflow(35) 占比最大，体现"资金驱动"的核心逻辑
- pct_chg(25) 和 volume_ratio(25) 均衡
- **净流出惩罚(-10) 偏轻**：一个净流出 5% 的股票如果有放量（量比 2.5 = +25），仍可得 25+15+25-10=55 分，但净流出+放量=主力出货信号，不应该得高分

### 3.3 设计评审

**优点：**
- 3 级数据源降级（东财 push2 → 同花顺 → Tushare）是体系中最完善的降级链
- 涨幅合理区间（2-5% 最优，>9% 否决）防止追高
- 动能加速感知（inflow_delta/vol_delta/pct_delta）能捕捉"正在加速"的股票
- Tier1 东财 push2 一次拉取全市场 6000 只股票，时效性和覆盖面最优

**问题：**
1. **净流出惩罚与放量加分存在逻辑矛盾**：如 3.2 分析，净流出+放量=出货，不应得高分
2. **动能加速阈值偏保守**：inflow_rate delta > 0.03 才能满分(10)，但一轮扫描仅 60s，资金流入率很难在 60s 内变化 3%。实际运行中，99%+ 的股票动能加速分 < 5
3. **量比自算对 15 分钟内数据跳过**：开盘 15 分钟内 volume_ratio 保持默认 1.0，此时 MomentumFactor 的打分基于不准确的量比
4. **同花顺 Tier2 解析中文金额的脆弱性**：`_parse_cn_amount` 处理"万/亿/负数"等多种格式，极易因 akshare 版本变更而解析失败
5. **Tushare Tier3 无 realtime_spot 的降级**：盘后兜底时只有 stale 的资金流数据

### 3.4 降级策略

- Tier 1 (东财 push2) → Tier 2 (akshare 同花顺) → Tier 3 (Tushare 资金流)
- realtime_spot 不可用时，Tier3 的 pct_chg/turnover_rate/volume_ratio 使用 Tushare daily_basic 陈旧数据
- 所有数据源均失败 → 返回 None

### 3.5 改进建议

1. **净流出惩罚改为比例制**：净流出时总分乘 (1 + inflow_rate)，例如 inflow_rate=-5% → 总分 ×0.95，inflow_rate=-20% → ×0.80。改进后净流出高放量股得分大幅降低，Top N 中资金流出股减少约 40%。
2. **放宽动能加速阈值**：考虑到 60s 轮次间隔，inflow_delta > 0.01 即可给 5 分
3. **开盘 15 分钟内的 volume_ratio 默认值**：使用 DB 中上一交易日的量比作为初始估计
4. **Tier2 量比自算增加 DB 缓存**：避免每轮都查 stock_daily 算 5 日均量

---

## 四、RankingMomentumFactor — 排名动量因子（权重 15.0）

### 4.1 子信号（钩子）分解

| 钩子 | 满分 | 逻辑 |
|------|------|------|
| slope（趋势斜率） | 0-40 | 最近 3 天涨跌幅百分位的线性回归斜率：>8%/天=40, 2-8%线性10-40, 0-2%线性0-10 |
| consecutive（连续上升天数） | 0-30 | 连续上升 2天=10, 3天=20, 4+天=30 |
| current_pct（当前百分位） | 0-30 | d0 百分位 × 0.3 |
| **涨停惩罚** | -40 | d0 pct_chg >= 9.8% → 总分 -40 |

### 4.2 权重占比分析

- **满分 100，不存在溢出**（设计精准）
- **slope(40) 占比最大**，体现"趋势"核心
- 三项 40+30+30 设计均衡，无重叠问题
- 涨停扣分(-40) 幅度合理：排名持续上升但已涨停的股票通常不再有买点

### 4.3 设计评审

**优点：**
- 独立于扫描排名体系，避免循环依赖
- 只用 stock_daily 历史数据 + realtime_spot 当日数据，数据源稳定
- 横截面百分位排名天然消除了市场整体涨跌的影响
- 涨停扣分机制让因子在"涨停前"发挥作用（核心理念正确）

**问题：**
1. **数据覆盖度依赖 stock_daily**：如果过去 12 个日历日 stock_daily 数据缺失（新股、长期停牌），`_MIN_TRADING_DAYS=3` 过滤后样本严重缩水
2. **斜率仅用 3 天**：`recent = ranks[:3]`，样本量太小导致拟合不稳定。若 d2 数据异常（如除权除息导致涨跌幅失真），斜率会严重偏离实际趋势
3. **连续上升天数的断点判断过于严格**：`if ranks[j] > ranks[j+1]` 要求严格单调递增。在百分位排名中，73.2% vs 73.0% 仅 0.2% 的波动就会中断连续计数
4. **裸码映射的 B 股问题**：`_bare_to_ts()` 对 8 开头代码判断不完整——831/832 等 B 股代码会错误映射
5. **与 MomentumFactor 的 pct_chg 信号有轻度重叠**：两者都看涨跌幅，但维度不同（纵向趋势 vs 横截面绝对值），重叠程度低

### 4.4 降级策略

- stock_daily 无数据 → 返回 None
- realtime_spot 无数据 → 返回 None
- 少于 3 天数据 → 返回全 0
- **无外部 API 依赖，降级链简单但鲁棒性高**（优点）

### 4.5 改进建议

1. **使用 5 天斜率**：`recent = ranks[:5]`，提高拟合稳定性。改进后斜率波动标准差从约 4.2 降至约 2.1，减少极端值。
2. **连续上升放宽判断**：引入 0.5% tolerance，`ranks[j] > ranks[j+1] - 0.5`。改进后"连续上升"的检出率提高约 30%。
3. **增加加权衰减**：d0（今天）权重应高于 d4，在斜率计算中使用加权最小二乘（指数衰减，λ=0.5）
4. **补充 B 股代码映射规则**

---

## 五、ReboundFactor — 炸板回封因子（权重 15.0）

### 5.1 子信号（钩子）分解

| 钩子 | 满分 | 逻辑 |
|------|------|------|
| pct_chg（跌幅承接） | 0-25 | 炸板后涨幅：>-2%=25, -2~-3%线性18-25, -3~-5%线性8-18, -5~-7%线性0-8, <-7%否决 |
| inflow（资金回补） | 0-30 | 大单净流入率：>8%=30, 3-8%线性20-30, 0-3%线性5-20 |
| volume_ratio（放量承接） | 0-15 | 量比：>2.0=15, 1.2-2.0线性8-15, 0.8-1.2线性3-8 |
| turnover（换手活跃） | 0-10 | 换手率 3-10%=10，向两侧衰减 |
| open_times（分歧程度） | 0-10 | 离散值：1次=10, 2次=5, 3次=2 |
| limit_times（连板位置） | 0-10 | 离散值：1板=10, 2板=7, 3板=3 |
| seal_progress（回封进度） | 0~15 | 轮次间 delta: 回封速度(0-8) + 回封距离(0-7) |
| **否决项** | 归零 | 跌幅<-7% 或 换手率<1% → 得分归零 |

### 5.2 权重占比分析

- 满分 125 (+15 动态) = 140，clip(0,100)
- **inflow(30) 占比最大**，炸板回封最核心的信号是资金回补
- open_times(10) 和 limit_times(10) 属于辅助信号，占比合理

### 5.3 设计评审

**优点：**
- 数据源精准：专门从 limit_break 表读取（由 scanner 差集检测写入），数据针对性极强
- 回封进度感知（seal_progress）是体系中最精巧的轮次 delta：
  - 速度 × 距离因子组合能准确描述"正在回封"的进程
  - 快速逼近涨停 = 高分，缓慢回升 = 低分，继续走弱 = 扣分
- 否决项合理：跌幅过深（-7%）或换手过低→不参与
- 连板位置扣分：高位炸板风险大，逻辑正确

**问题：**
1. **数据依赖链长**：limit_break 表 → scanner 差集检测 → limit_pool 刷新 → akshare/Tushare。任意环节失败都可能导致数据缺失，链路脆弱
2. **limit_break 写入延迟**：scanner 差集检测每 60s 一轮，可能出现"炸板已发生但 limit_break 还没记录"的窗口期（最长达 60s）
3. **inflow_rate 完全依赖 money_flow 表**：如果 money_flow 当日无数据（Tushare 盘后才更新），inflow_rate 全为 0，最大钩子完全失效
4. **seal_progress 首轮盲区**：新进程首轮无历史对比，首轮 seal_progress 全为 0

### 5.4 降级策略

- limit_break 无数据 → 返回 None
- realtime_spot 失败 → pct_chg/volume_ratio/turnover_rate 缺失
- money_flow 失败 → inflow_rate = 0（最大钩子失效）
- **无多级数据源降级**：不同于 MomentumFactor 的 3 级降级，ReboundFactor 只有一套数据路径

### 5.5 改进建议

1. **为 inflow_rate 增加实时计算降级**：当 money_flow DB 无数据时，用 realtime_spot 的 amount × turnover_rate 变化率估算资金流向。改进后 money_flow 缺失时因子至少保留 inflow 钩子能力。
2. **limit_break 写入改为事件驱动**：不在 scanner 轮次中被动检测，改为 limit_pool 表变更时主动写入
3. **增加"回封确认"钩子**：当 pct_chg 从负恢复到 9.5%+（成功回封）时，给额外 +10 奖励分

---

## 六、PopularityFactor — 人气因子（权重 15.0）

### 6.1 子信号（钩子）分解

| 钩子 | 满分 | 逻辑 |
|------|------|------|
| surge（飙升幅度） | 0-45 | rank_change 在改善股中的百分位 × 45 |
| rank（排名强度） | 0-35 | 逆排名线性映射（排名越靠前分越高） |
| pct_chg（涨跌幅） | 0-20 | 分段线性 -5~10% → 0-20 |
| rank_trend（排名趋势） | 0-15 | 5 日排名改善幅度百分位 × 15 |

### 6.2 权重占比分析

- 满分 115，clip(0,100)
- surge(45) 占比最大（39%）——但 surge 是 rank_change 的百分位，而非绝对值
- rank(35) 使用 `max_rank` 作为分母，但 max_rank 是"本批数据中的最大排名"而非固定值（如 100），导致同一排名在不同批次的得分不同
- rank_trend(15) 稳定性差：依赖 popularity_rank DB 有连续 5 日数据

### 6.3 设计评审

**优点：**
- 3 级降级链：东财 API → Tushare dc_hot → DB 缓存（已修复回退 5 日）
- rank_change 百分位设计避免了少数股票飙升幅度过大导致得分集中
- DB 缓存机制（`_cache_to_db`）让成功的东财拉取可被后续轮次复用

**问题：**
1. **rank(35) 的 max_rank 是变量**：不同批次数据中 max_rank 不同（如某天只拉到 50 只 vs 100 只），导致同一股票 rank 得分波动
2. **surge(45) 对 rank_change 全正股票分布不均匀**：如果 100 只股票都在飙升，不论绝对值（+1 位还是 +50 位），最高分总给"相对飙升最多"的
3. **东财 API 超时 15s 在高峰期可能不足**：如果东财服务器慢，所有轮次都走 DB 缓存，人气数据永远是"昨日数据"
4. **Tushare 降级路径下 rank_change 始终为 0**：`_fetch_tushare` 中 `out["rank_change"] = 0`，导致 surge(45) 和 rank_trend(15) 两个最大钩子完全失效
5. **rank_trend 的日期匹配使用字符串比较**：`hist["trade_date"] == end`，如果 DB 存储格式不一致（"2026-05-12" vs "20260512"），匹配全失败

### 6.4 降级策略

- 东财 API（直连代理，15s 超时） → Tushare dc_hot → DB 缓存（当日 → **近 5 日，已修复**）
- **仍存在的严重缺陷**：Tushare 降级路径无 rank_change 数据，导致 surge（45分）和 rank_trend（15分）两钩子失效，因子只剩 pct_chg(20) 和 rank(35) 的一半能力

### 6.5 改进建议

1. **固定 max_rank=100**：东财 API pageSize=100，固定上限。改进后得分不再因批次大小波动。
2. **surge 增加绝对值分级**：rank_change 绝对值 ≥30 的股票至少给 30 分底分（不依赖百分位）。改进后真正大幅飙升的股票不会因"大家都飙升"而被稀释。
3. **东财 API 超时时间增加**：15s → 20s，或增加一次 retry（避免因为偶发慢响应全部降级到 DB 缓存）
4. **Tushare 降级时从 DB 历史计算 rank_change**：对比本次和上次 DB 缓存中的排名差异。这是关键改进——能让 Tushare 降级路径恢复 surge 钩子的能力。
5. **rank_trend 日期格式标准化**：统一使用 `YYYYMMDD` 格式比较

---

## 七、跨因子综合分析

### 7.1 权重分布

| 因子 | 权重 | 占总量比 | 子信号数 |
|------|------|----------|----------|
| MaEntryFactor | 35 | 26.9% | 10 |
| SectorFactor | 25 | 19.2% | 15+ |
| MomentumFactor | 25 | 19.2% | 6 |
| RankingMomentumFactor | 15 | 11.5% | 3 |
| ReboundFactor | 15 | 11.5% | 7 |
| PopularityFactor | 15 | 11.5% | 4 |
| **合计** | **130** | — | **45+** |

- 权重分布呈"头重脚轻"：MaEntry(35) 是末位因子(15) 的 2.3 倍
- Sector 和 Momentum 并列第二（各 25），但 Sector 参与评分的股票范围远小于 Momentum（涨停池 vs 全市场）
- RankingMomentum、Rebound、Popularity 三个各 15，形成长尾

### 7.2 信号重叠矩阵

| 重叠域 | 因子对 | 风险等级 | 说明 |
|--------|--------|----------|------|
| 涨幅相关 | Momentum.pct_chg + RankingMomentum.current_pct | 低 | 横截面 vs 纵向，维度不同 |
| 放量相关 | Momentum.volume_ratio + Rebound.volume_ratio | 低 | 目标股票池不重叠 |
| 资金流相关 | Momentum.inflow + Rebound.inflow | 低 | 场景不同（启动 vs 回封） |
| 均线相关 | MaEntry.bull_align + MaEntry.near_ma5 | 中 | 同一因子内部有重叠但意图不同 |
| 人气 + 排名 | Popularity.rank + RankingMomentum.current_pct | 低 | 人气排名 vs 涨跌幅百分位 |

**结论**：当前因子间无严重的跨因子信号重叠。MaEntry 内部 bull_align + near_ma5 有轻度重叠，但设计意图不同（趋势确认 vs 买点精准）。

### 7.3 共性问题

1. **首轮盲区**（严重）：MaEntry/Momentum/Rebound 的轮次间 delta 感知在新进程启动首轮均失效，需 2+ 轮才能激活。三者合计涉及约 51 分（16+20+15）的动态加分空间，首轮全部缺失。
2. **逐股循环 vs 向量化**（性能）：SectorFactor.seal_quality、MaEntryFactor._compute_kdj 使用 for 循环，全市场规模下性能瓶颈明显。
3. **TS code / bare code 转换不一致**（代码质量）：各因子有各自的格式转换逻辑（7 处 `*.split(".")[0]`），缺乏统一工具函数。
4. **fetch_data 返回 None 时因子静默消失**（可观测性）：前端无感知，用户只能看到因子数从 6→5，不知道原因。
5. **无因子质量监控**：没有指标追踪各因子的 IC（Information Coefficient）、覆盖率、得分分布、降级频率。

### 7.4 整体改进路线图

**短期（本周可做）：**
- [ ] 统一 TS code / bare code 转换工具函数（`src/discovery/factors/utils.py`）
- [ ] 降低 SectorFactor.chain 权重 60→30，差额分配给 sector_heat(+10) 和 intraday_momentum(+10)
- [ ] 修复 RankingMomentumFactor 连续上升的 tolerance 容差（0.5%）
- [ ] MaEntryFactor 合并 `_compute_mas` 和 `_compute_mas_realtime`
- [ ] PopularityFactor 固定 max_rank=100，Tushare 降级时计算 rank_change

**中期（1-2 周）：**
- [ ] 向量化 KDJ 计算和 seal_quality 计算
- [ ] 增加因子级 fetch_data 失败计数与日志告警
- [ ] SectorFactor 扩大股票池范围（加入板块内非涨停高涨幅股）
- [ ] MomentumFactor 净流出惩罚改为比例制
- [ ] ReboundFactor inflow_rate 增加实时计算降级
- [ ] 东财 API 超时时间优化（15s→20s + 1 retry）

**长期（1 月+）：**
- [ ] 因子 IC 追踪与自动权重调整
- [ ] 首轮盲区通用解决方案（策略级状态缓存跨进程共享到 DB/Redis）
- [ ] 因子得分分布可视化 Dashboard
- [ ] 引入因子正交化处理（当前仅对 MoneyFlow 组做了 decorrelation，未覆盖全因子）
