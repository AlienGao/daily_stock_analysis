# 连板预期因子 (Limit Continuation Factor) 设计方案

> 日期: 2026-05-15
> 状态: 待审核

## 1. 背景与问题

大唐发电 601991 近期走势：5/6 首板 → 5/7 二板 → 5/8 三板 → 5/11 四板 → 5/12 五板 → 5/13 六板，
连续涨停，但盘中得分始终 24~27，排名 1700+。

**根因分析**：

| 现有因子 | 为何失效 |
|---------|---------|
| momentum (权重18) | 涨幅 >7% 时分数骤降（设计为"不追高"） |
| ma_entry (权重25) | 涨停一字板无均线突破事件 |
| limit (权重15) | 仅盘后可用，评价"今天封板好不好"，不预判明天 |
| rebound (权重20) | 仅检测炸板回封，涨停未开板时不触发 |
| popularity | 公用事业股无散户热度 |

**缺失能力**：没有因子回答"昨天涨停了，今天大概率还能涨停吗"这个问题。

## 2. 现有数据基础

| 数据表 | 行数 | 关键字段 | 更新频率 |
|--------|------|---------|---------|
| `limit_pool` | 2638 | limit_times, open_times, first_seal_time, seal_amount, float_market_cap, break_count | 盘后(tushare) + 盘中(akshare) |
| `limit_up_history` | 632 | code, trade_date, open_times, limit_times, sector | 盘中实时(akshare) |
| `limit_break` | 183 | code, trade_date, status, limit_times | 盘中实时 |
| `realtime_spot` | - | price, pct_chg, volume, turnover_rate, volume_ratio | 盘中30秒 |

**全部所需数据已在库中，无需新增数据源。**

## 3. 与现有因子的关系

```
limit_factor (盘后)     →  "今天封板质量如何"     → 回顾型
rebound_factor (盘中)   →  "炸板后能回封吗"       → 盘中事件型
limit_continuation (新) →  "明天/今天能续板吗"    → 预测型 ← 新增
```

三者使用同一批数据表（limit_pool / limit_up_history），但回答不同问题，不存在信号重叠。

## 4. 因子定义

### 文件
`src/discovery/factors/limit_continuation_factor.py`

### 属性
```python
class LimitContinuationFactor(BaseFactor):
    name = "limit_continuation"
    available_intraday = True       # 盘中可用（前日连板数据 + 当日实时行情确认）
    available_postmarket = True     # 盘后可用（当日连板数据预判次日）
    weight = 12.0
```

### 数据获取 (fetch_data)

**盘后模式**：
- 读 `limit_pool` 当日数据，筛选 `limit_type='U'`（涨停股）
- 读 `limit_pool` 前一交易日数据，用于计算连板加速度

**盘中模式**：
- 读 `limit_pool` 前一交易日数据（昨日涨停名单）
- 读 `realtime_spot` 当日实时行情（确认是否延续）
- 读 `limit_up_history` 当日数据（盘中已封板的票）

### 五个子信号

#### 4.1 连板动能 (chain_momentum) — 满分 30

核心逻辑：连板天数越多 + 板越干净 → 续板概率越高。但超高连板（7+）风险加大。

```
limit_times 打分（基础分）:
  1板 → 10    // 首板续板概率最低
  2板 → 18    // 二板确认强势
  3板 → 24    // 三板成妖
  4板 → 28
  5板 → 30    // 满分区
  6板 → 29    // 开始衰减（高位接力风险）
  7板 → 27
  8+板 → 25
```

来源: limit_pool.limit_times

#### 4.2 封板质量 (seal_strength) — 满分 25

首封时间越早 + 开板次数越少 → 抛压越小 → 续板概率越高。

```
首封时间打分 (0-15):
  < 09:30 (集合竞价一字) → 15
  09:30 ~ 09:45          → 12
  09:45 ~ 10:30          → 8
  10:30 ~ 13:00          → 4
  > 13:00                → 1

开板次数打分 (0-10):
  0次 (一字/T字未开) → 10
  1次               → 7
  2次               → 4
  3次               → 2
  4+次              → 0
```

来源: limit_pool.first_seal_time, limit_pool.open_times

#### 4.3 封单强度 (seal_ratio) — 满分 20

封单金额 / 流通市值 = 封单比，衡量买盘意愿的压倒性。

```
seal_ratio = seal_amount / float_market_cap

打分:
  > 10%   → 20  // 巨量封单
  5%~10%  → 线性 15→20
  2%~5%   → 线性 8→15
  1%~2%   → 线性 3→8
  < 1%    → 线性 0→3
```

来源: limit_pool.seal_amount, limit_pool.float_market_cap

#### 4.4 板块共振 (sector_sync) — 满分 15

同板块同日涨停票数量越多 → 板块效应越强 → 龙头续板概率越高。

```
sector_limit_count = 同 sector 在 limit_pool 中 limit_type='U' 的数量

打分:
  1只（独苗）   → 2
  2只           → 6
  3只           → 10
  4只           → 13
  5+只          → 15

龙头加分：若该股在板块内 limit_times 最大，额外 +3（上限仍 15）
```

来源: limit_pool.sector + 横截面聚合

#### 4.5 盘中确认 (intraday_confirmation) — 满分 10（仅盘中模式）

用当日实时行情确认昨日涨停股是否延续强势。盘后模式此项为 0。

```
打分规则（针对昨日涨停股）:
  今日已封涨停 (在 limit_up_history 中) → 10
  今日涨幅 > 5% 且量比 > 1.5           → 7
  今日涨幅 > 3%                         → 4
  今日涨幅 > 0%                         → 1
  今日下跌                              → 0
```

来源: realtime_spot.pct_chg / volume_ratio + limit_up_history 当日

### 最终得分

```
盘后: total = chain_momentum + seal_strength + seal_ratio + sector_sync     (0~90, 再 * 100/90 归一化到 0~100)
盘中: total = chain_momentum + seal_strength + seal_ratio + sector_sync + intraday_confirmation  (0~100)
```

## 5. 候选股筛选逻辑

因子只对**昨日/今日涨停的股票**打分，非涨停股得 0 分。
这意味着这个因子天然只在涨停行情活跃时产生影响，不会干扰正常市场的评分结构。

## 6. 预期效果

以 601991 大唐发电 5/12（五板当日，次日预判六板）为例：
- chain_momentum: 30（5 连板，满分区）
- seal_strength: ~12（首封 09:34 早盘 + 开板 3 次）
- seal_ratio: ~10-15（封单比待验证）
- sector_sync: ~6-10（电力板块其他涨停票数）

**预估总分 ~65 → 归一化后 ~72 → 加权贡献 8.6 分**

对比当前盘后得分 37 → 加入后约 46，排名从 831 显著提升。

## 7. 实现步骤

| # | 内容 | 文件 | 依赖 |
|---|------|------|------|
| 1 | 新建因子文件 | `src/discovery/factors/limit_continuation_factor.py` | - |
| 2 | 实现 `fetch_data`: 盘后读当日 limit_pool，盘中读前日 limit_pool + 当日 realtime_spot + limit_up_history | 同上 | storage.py 现有方法 |
| 3 | 实现 `score`: 5 个子信号打分 + 归一化 | 同上 | - |
| 4 | 实现 `describe`: 生成推荐理由文本 | 同上 | - |
| 5 | 注册因子 | `src/discovery/factors/__init__.py` | step 1 |
| 6 | 验证 `get_limit_pool` 支持按日期查询前日数据 | `src/storage.py` | 检查是否需要新增方法 |
| 7 | py_compile + 现有测试回归 | - | step 1-6 |
| 8 | 更新 CHANGELOG | `docs/CHANGELOG.md` | - |

## 8. 需要确认的事项

1. **权重 12.0 是否合适？** 和 limit_factor(15) + rebound_factor(20) 合计 47，涨停相关因子占比偏高，是否需要同步下调其他两个？
2. **盘中模式是否需要？** 盘中 limit_up_history 数据来自 akshare 实时抓取（scanner.py:319），依赖 scanner 运行中。如果只做盘后版，实现更简单。
3. **历史数据量**：limit_pool 仅 2638 行（约 10 个交易日），续板率统计样本偏少。是否需要先积累更多历史数据再调参？

## 9. 风险

- **追高风险**：连板股高位接力失败时回撤剧烈（天地板 -20%），需配合止损系统和仓位控制。
- **数据延迟**：limit_pool 的盘后数据依赖 Tushare 更新时间，若延迟可能影响次日早盘决策。
- **样本偏差**：当前 limit_pool 历史仅覆盖约 10 个交易日，子信号权重分配基于经验而非统计回测，后续需用更多数据验证。
