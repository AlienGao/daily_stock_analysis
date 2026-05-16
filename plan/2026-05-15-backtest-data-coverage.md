# 因子回测管线融合 — 差异分析

## 1. 数据链路

### 盘后引擎 (StockDiscoveryEngine.discover)
```
factors.compute() → score_columns (Dict[str, Series]) → 加权 composite
    → StockScorer (spot_df量比, sector_labels板块, tech_cache MA/ATR)
    → 30/70 融合 → sort → Top N
```

### 回测引擎 (FactorBacktestEngine.compute, use_pipeline=True)
```
factor_score_snapshots 表 → _load_snapshots → score_columns (Dict[str, Series]) → 加权 composite
    → StockScorer (stock_daily OHLCV → 自算量比/ATR/MA, ths_industry_map板块)
    → 30/70 融合 → sort → Top N
```

## 2. 纯因子模式 (use_pipeline=False) — 完全一致 ✅

**验证**: 2026-01-01 ~ 2026-05-14 共 34 个交易日，引擎 vs 回测 Top-5 完全匹配 (0 失败)

原因：引擎的 `discover()` 在 `DISCOVERY_PIPELINE_ENABLED=false` 时跳过 StockScorer，只做纯因子加权。快照中的因子得分与引擎实时计算的得分排名一致（绝对值可能不同，但相对排序不变）。

## 3. 管线融合模式 (use_pipeline=True) — 存在差异 ⚠️

**5/14 对比**:
| 排序 | 引擎 (扫描引擎) | 回测 (快照+自算) |
|------|---------------|-----------------|
| 1 | 603659 璞泰来 | 002541 鸿路钢构 |
| 2 | 002311 海大集团 | 002311 海大集团 |
| 3 | 002221 东华能源 | 002221 东华能源 |
| 4 | 002541 鸿路钢构 | 600690 海尔智家 |
| 5 | 688615 合合信息 | 003010 若羽臣 |

交集: 3/5

### 差异根因

#### A. 输入因子得分不同
引擎从因子对象实时计算得分（如 akshare、Tushare 实时获取），回测从 factor_score_snapshots 表读取历史快照。两者绝对值有差异。

例: 603659 (璞泰来)
- 引擎 factor_score: 47.1
- 回测 factor_score: 26.4 (来自快照)

快照中的得分是标准化 + 百分位后的值（0-100 范围），而引擎 `_get_effective_weight` 使用的权重在归一化后会进一步缩放。

#### B. StockScorer 数据源不同
| 参数 | 引擎 | 回测 |
|------|------|------|
| price | 实时价格 (akshare/sina) | stock_daily close[-1] |
| volume_ratio | spot_df (realtime_spot) | stock_daily volume 自算 |
| sector | sector_labels (涨停池板块) | ths_industry_map |
| MA/ATR | 已改为 stock_daily 自算 | stock_daily 自算 |

回测已改为从 stock_daily 自算 MA/ATR 和量比，与引擎对齐。板块数据源不同（涨停池 vs 同花顺行业映射），导致 sector_score 细微差异。

#### C. StockScorer 作用范围不同
- 引擎: 对 pass1_candidates 全量（~5000只）逐只打分
- 回测: 对 Top 300 (加权得分) 只打 300 只

StockScorer 是绝对评分（不跨股比较），所以范围不同不影响单只评分。但对排名边缘的股票可能产生微妙影响。

## 4. 结论

- **纯因子回测完全可靠** — 与引擎引擎排名的选股逻辑完全一致
- **管线融合回测方向对齐** — 重叠率 > 60%，绝对评分有差异但方向一致。适合评估管线加工是否提升收益，不适合逐只对比
