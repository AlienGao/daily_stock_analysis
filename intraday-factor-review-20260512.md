# 盘中因子复盘报告

日期：2026-05-12

---

## 一、盘中因子总览

| 因子 | 权重 | 最高子信号 | 数据来源 |
|------|------|-----------|---------|
| MaEntryFactor | 35 | 多头排列(20) + 接近MA5(25) + KDJ超卖(10) | DB 本地 |
| MomentumFactor | 25 | 资金流入(35) + 涨幅(25) + 放量(25) | 东财 push2 / akshare / Tushare |
| SectorFactor | 25 | 连板强度(60) + 盘中行业热度(30) | DB + akshare / Tushare |
| PopularityFactor | 15 | 飙升幅度(45) + 排名强度(35) | 东财 API / Tushare dc_hot / DB |
| RankingMomentum | 15 | 排名斜率(40) + 连续改善(30) | DB 本地 |
| ReboundFactor | 15 | 资金回补(30) + 跌幅承接(25) | DB 本地 |

总权重 130，引擎用百分位标准化后归一化（`pct.rank(pct=True) * 100 * weight/total_weight`）。
MaEntryFactor 贡献约 27%，Popularity/RankingMomentum/Rebound 各约 11.5%。

---

## 二、重叠计算

| 重复项 | 出现次数 | 所在文件 |
|--------|---------|---------|
| `_get_avg_volume()` | 2 次 | ma_entry_factor.py, momentum_factor.py |
| `_trading_minutes_elapsed()` | 2 次 | ma_entry_factor.py, momentum_factor.py |
| `_bare_to_ts_code()` / 代码归一化 | 6 次 | limit_factor, momentum_factor, money_flow_factor, ranking_momentum_factor, sector_factor, rebound_factor |
| `inflow_rate` 计算公式 | 3 次 | momentum_factor, rebound_factor, money_flow_factor |
| `_linear_map()` | 4 次 | momentum_factor, popularity_factor, rebound_factor, technical_factor |

**建议**：抽到共享 `factor_utils.py` 模块。

---

## 三、权重评估

- **PopularityFactor 偏轻 (15)**。东财人气排行是 A 股最直接的散户情绪信号，学术上短期 alpha 显著。飙升幅度子信号满分 45 但在降级路径静默清零（见下文 Bug 部分）。
- **RankingMomentumFactor (15) 起步合适**。作为最新加入的因子，15 是合理起步权重，后续可基于 Rank IC 回测调整。
- 引擎层做百分位标准化，绝对权重差异不如数字看起来大。最终贡献取决于得分分布。

---

## 四、Bug 与风险

### 4.1 Volume 单位可能翻倍除（MaEntryFactor）

```
spot:   result["vol"] = result["vol"] / 100.0       # line 59
stock:  df["volume"] = df["volume"] / 100.0          # line 128
```

注释写 `stock_daily.volume 存储为股（手×100），此处除以 100 还原为手`。如果 stock_daily.volume 本身已是手（取决于数据导入逻辑），相当于除了两次，`vol_shrink_near_ma` 信号会系统性地算错。**需要确认数据导入端的单位。**

### 4.2 PopularityFactor 降级路径静默丢分

东财 API 不可用时降级到 Tushare `dc_hot` 或 DB 缓存，`rank_change` 被设为 0。飙升幅度子信号（满分 45）对降级路径的所有股票返回 0，无任何日志警告。榜单可能在东财故障时突然变样。

### 4.3 SectorFactor 全市场聚合性能

`_compute_intraday_momentum()` 在 `score()` 内读取全部 5000+ 只股票的 `realtime_spot` 计算行业热度。扫描只处理 ~100 只涨停池股票，却读了全市场数据。30 秒扫描间隔下值得关注。

### 4.4 技术指标计算不一致

MaEntryFactor 本地计算 MA/KDJ/BOLL（从 OHLCV），盘后的 TechnicalFactor 用 Tushare `stk_factor` 表。同一只股票同一天，盘中和盘后的 KDJ 可能因计算参数或复权方式不同而有差异。

---

## 五、设计评价

### 做得好的

- 因子体系分层清晰（fetch → score → describe），新增因子成本低
- 引擎层有行业中性化 + 因子去相关（`_decorrelate_scores`），不是简单加权
- 多数据源 fallback 体系完整
- MaEntryFactor 过渡信号设计精巧（多头排列刚形成 +5、KDJ 刚恢复 +5 等）

### 可改进的

- `fetch_data` 职责过重：MaEntryFactor 在数据获取阶段做了大量计算（MA/KDJ/BOLL/volume），模糊了获取与计算的边界
- 阈值硬编码（30B/100B/500B 市值档位、penalty 系数 0.6/0.8）缺少可配置入口
- akshare 中文列名 substring 匹配（`"代码" in str(c)`）脆弱，依赖非稳定 API 的列名格式
- SectorFactor 的 intraday_momentum 子信号（6 个指标 + delta 追踪 + 共振系数）本身就是一个完整因子，嵌在 SectorFactor 里难以独立调参

---

## 六、值得加入的因子

| 因子 | 理由 | 数据可得性 |
|------|------|-----------|
| **缺口因子** (Gap) | 跳空高开/低开 + 放量是强日内信号 | realtime_spot + OHLCV，已有 |
| **异常放量因子** | 当前量比只是相对均值，缺少相对分布的异常检测（如 volume > 2σ） | volume 数据已有 |
| **波动率扩张因子** | ATR 百分位已在 StopLossCalculator 中使用，但因子体系没有用到 | OHLCV，已有 |
| **MoneyFlowFactor 盘中化** | 代码逻辑与时间无关，换成东财 push2 数据源即可 | 改数据源即可 |
| **TechnicalFactor 盘中化** | MaEntryFactor 已在算 MA/KDJ/BOLL，扩展到 MACD/RSI 即可 | 本地计算 |

---

## 七、总评

| 维度 | 评级 | 说明 |
|------|------|------|
| 因子覆盖 | B+ | 6 个因子覆盖面合理，但缺口/异常量/波动率是明显空白 |
| 计算重叠 | C+ | 共享工具函数缺失，5 处重复代码 |
| 权重设计 | B | 整体合理，Popularity 偏轻，新因子起步权重合适 |
| Bug 风险 | B | 无阻断性 Bug，Volume 单位需确认，降级静默丢分待修 |
| 可扩展性 | A- | 新增因子成本低，引擎层设计好 |
| 性能 | B | SectorFactor 全市场聚合是热点，30s 间隔下需关注 |
