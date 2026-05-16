# 盘中/盘后扫描流程详解

> 最后更新: 2026-05-16，基于 engine.py、scanner.py、pipeline.py 当前代码。

## 一、整体架构

```
                    ┌──────────────────────────┐
                    │      main.py / server.py   │
                    │   --schedule / --serve     │
                    └──────────┬───────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                                 ▼
    ┌─────────────────┐              ┌─────────────────────┐
    │  IntradayScanner │              │ StockAnalysisPipeline│
    │  (守护进程,阻塞)  │              │   _run_impl()       │
    │  9:29→15:01循环  │              │   auto_discover     │
    └────────┬────────┘              └──────────┬──────────┘
             │                                   │
             │  engine.discover("intraday")       │  ensure_postmarket_scan()
             │                                   │
             ▼                                   ▼
    ┌──────────────────────────────────────────────────────────────┐
    │               StockDiscoveryEngine.discover(mode)             │
    │                                                              │
    │  Phase 1: fetch_data → Phase 2: 快照加载                      │
    │  Phase 3: score → Phase 4: 加权组合                           │
    │  Phase 4.5: 全量 describe → Phase 4.9a: 保存快照               │
    │  Phase 5: StockScorer 两阶段 (Pass1全量 + Pass2 TopN)          │
    └──────────────────────────────────────────────────────────────┘
```

## 二、盘中扫描（IntradayScanner）

**入口**: `scanner.py:66 start()` → 阻塞守护进程，永久运行

### 时序控制

```
非交易日 → 休眠到次日 8:00
交易日 9:25 → 进入盘中循环
  每 scan_interval_seconds 秒一轮
  11:30-13:00 → 午休暂停
  15:01 → 收盘后退出循环
15:30 → 等待 Tushare 日线数据更新
      → _ensure_daily_kline_complete()
      → 休眠到次日 8:00
```

### 每轮执行 (scanner.py:144-206)

```
1. _refresh_realtime_spot()
   ├─ RealtimeSpotProvider.fetch()
   ├─ 多源优先级: 腾讯30s → sina兜底 → 东财补充换手率/量比
   ├─ 30s slot 去重（同 slot 内复用缓存）
   └─ upsert_realtime_spot() 落库

2. _refresh_limit_pool()
   ├─ 每 60s 刷新（偶 30s slot）
   ├─ 3-tier fallback: akshare → realtime_spot DB → Tushare
   ├─ 板块分类（同花顺行业优先，申万填充）
   ├─ 炸板检测 (_detect_limit_breaks)
   └─ upsert 落库

3. engine.discover(mode="intraday")
   └─ 详见第四节「核心发现引擎」

4. _annotate_changes()
   └─ 对比上一轮结果，标注排名变化 (↑/↓/new)

5. _write_output()
   ├─ 写 /tmp/discovery_top10.json (实时输出)
   └─ 写 discovery_reports/intraday_*.json / *.md

6. _notify_new_stocks()
   └─ 推送新上榜股票 (企业微信/webhook)

7. _save_full_scan_to_db()
   └─ 全量写入 scan_result_intraday 表
```

## 三、盘后扫描（ensure_postmarket_scan）

**入口**: `pipeline.py:2136` → `auto_discover` 触发

### 时序

由 `StockAnalysisPipeline._run_impl()` 中的 `auto_discover` 标志控制，每日运行一次（通常在 15:30+ 数据刷新后）。

### 完整流程 (scanner.py:1815-1941)

```
ensure_postmarket_scan(tushare_fetcher, akshare_fetcher)
│
├─ Step 1: DB 缓存检查
│   has_postmarket_scan_today(today) → 已有记录？
│   ├─ 是 → load_factor_signals_for_date(today)
│   │       → 返回缓存 (零 API 调用)
│   └─ 否 → 继续完整扫描
│
├─ Step 2: 18 个数据源刷新 (fail-open，失败不阻断)
│   ├─ ths_industry_map      (Tushare)
│   ├─ ths_concept_map       (Tushare)
│   ├─ sector_daily          (DB/缓存)
│   ├─ stock_daily           (Tushare: 日K线)
│   ├─ limit_pool            (Tushare: 涨停池)
│   ├─ money_flow            (Tushare: 资金流向)
│   ├─ daily_basic           (Tushare: 每日指标)
│   ├─ margin_detail         (Tushare: 融资融券)
│   ├─ cyq_perf              (Tushare: 筹码集中度)
│   ├─ insider_buy           (DB/缓存)
│   ├─ institution_hold      (DB/缓存)
│   ├─ repurchase            (Tushare: 回购)
│   ├─ profit_forecast       (Akshare: 盈利预测)
│   ├─ performance_report    (Akshare: 业绩报表)
│   ├─ hm_detail             (Tushare: 游资明细)
│   ├─ popularity            (Tushare: 人气排名)
│   └─ tech_indicator        (Tushare: stk_factor_pro, 技术指标)
│
├─ Step 3: 盘后炸板重校正
│   └─ 用 Tushare 全量数据纠正盘中基于 AkShare 的误判
│
├─ Step 4: 游资质量更新 (hm_detail 有新数据才重算)
│
├─ Step 5: engine.discover(mode="postmarket")
│   └─ 详见第四节「核心发现引擎」
│
└─ Step 6: 结果落库
    ├─ get_last_full_scan_records() → 全量扫描记录
    ├─ 合并 tech_score / composite_score
    ├─ save_scan_results_postmarket(records, today)
    └─ 构建 factor_signals_cache (供 downstream 消费)
```

## 四、核心发现引擎 discover() 详细阶段

### Phase 1: 拉取因子数据 (engine.py:748-811)

```
策略:
├─ 盘中 (intraday): 始终实时拉取，不做缓存
│   每个因子 fetch_data() → DB 优先 → API fallback
│
└─ 盘后 (postmarket):
    ├─ 优先复用 session 缓存 (_factor_data_cache)
    │   条件: cache 非空且 trade_date 匹配
    │   排除 available_intraday 因子 (含实时依赖)
    │
    ├─ 未命中: 逐因子拉取
    │   fetch_data() → DB 优先 → API fallback
    │
    └─ 更新 session 缓存 (排除实时因子)
```

### Phase 2: 快照优先加载（仅盘后）(engine.py:817-837)

```
仅 mode == "postmarket":
├─ _load_factor_scores_from_snapshots(trade_date, factor_names)
│   └─ 查询 factor_score_snapshots 表当日数据
│
├─ 命中 → raw_scores = snapshot_scores
│         score_columns = snapshot_scores
│         Phase 3 将被整体跳过 ⚠️ 关键决策点
│
└─ 未命中 → score_columns 为空，走正常 Phase 3

收集 all_codes:
├─ 有快照 → 从 score_columns 的 index 收集
└─ 无快照 → 从 factor_data 的 index 收集
```

### Phase 3: 逐因子打分 (engine.py:855-888)

```
条件: score_columns 为空时才执行 (有快照则整体跳过)

for each factor:
├─ factor.score(factor_data[name])
│   └─ 返回 pd.Series (0-100), index=ts_code
│
├─ 归一化: 代码→裸6位码, 去重(mean)
├─ 存入 raw_scores[factor.name] 和 score_columns[factor.name]
└─ 日志: "[Discovery] {factor}: scored {n} stocks, max={x}"
```

### Phase 3.5: 纯因子加权组合 (engine.py:894-904)

```
combined = DataFrame(score_columns).fillna(0)
for name in score_columns:
    w = effective_weight(name, mode)
    combined[name] *= w / total_weight
combined["_total"] = combined.sum(axis=1)
combined.sort_values("_total", ascending=False)
```

`_total` = 各因子按权重加权求和，即纯因子综合分（不含 StockScorer 技术分）。

### Phase 4.5: 全量推荐理由 (engine.py:906-923)

```
for each factor (全量, ~5500 stocks):
├─ factor.describe(factor_data, raw_scores)
│   └─ TechnicalFactor: 调用 _compute_signals + _detect_divergence
│      需查询 stock_tech_indicator 90 天历史
│
└─ 汇总到 all_reasons[ts_code] = [理由列表]

注意: describe() 对全市场运行，但只有 Top N 在 Pass 2 中使用。
```

### Phase 4.9: 输出暂存 (engine.py:986-994)

供外部 Scanner / Pipeline 通过 `get_last_full_scan_records()` 落库。

### Phase 4.9a: 保存因子得分快照 (engine.py:995-1000)

```
save_factor_score_snapshots(raw_scores, trade_date, mode)
```

- 写入 `factor_score_snapshots` 表
- 按 (trade_date, ts_code, mode, factor_name) 唯一约束
- 同 mode + trade_date 新扫描覆盖旧数据
- 写入成功后，同天下次 discover 可在 Phase 2 命中快照

### Phase 5: 两阶段构建 DiscoveryResult (engine.py:1031-1243+)

```
┌─────────────────────────────────────────────────────────────────┐
│ Pass 1 (轻量): 遍历全市场 ~5500 只                              │
│ ├─ 过滤: ST 股 + 白名单/金股范围                                 │
│ └─ 产出: pass1_candidates 列表                                  │
├─────────────────────────────────────────────────────────────────┤
│ StockScorer 评分 (两路径互斥)                                    │
│                                                                 │
│ 路径A (仅盘后): _batch_stockscorer_static(Top300)               │
│   └─ 与回测引擎对齐，仅 Top300 获 tech_score，其余为 0           │
│                                                                 │
│ 路径B (盘中): StockScorer(全量Pass1)                             │
│   ├─ 自算 MA20/MA60/ATR (对齐回测引擎)                           │
│   ├─ 精确止盈止损 compute_from_arrays()                          │
│   └─ 全量获得 tech_score                                         │
│                                                                 │
│ 互斥: 路径B 仅当 tech_scores_map 为空时执行                       │
├─────────────────────────────────────────────────────────────────┤
│ 综合分融合                                                       │
│   composite = alpha × raw + (1-alpha) × tech                    │
│   默认 alpha = 0.3  →  0.3×因子分 + 0.7×技术分                   │
│                                                                 │
│ 排序 → Top N (auto_discover_count / scan_top_n)                 │
├─────────────────────────────────────────────────────────────────┤
│ Pass 2 (重量): 仅 Top N                                          │
│ ├─ 精确止盈止损 (复用或 fallback 自算)                            │
│ ├─ 超买/低盈亏比过滤 (仅全市场)                                   │
│ └─ 构建 DiscoveryResult                                          │
└─────────────────────────────────────────────────────────────────┘
```

## 五、关键数据表

| 表名 | 写入时机 | 内容 |
|------|---------|------|
| `factor_score_snapshots` | Phase 4.9a (每轮扫描) | 各因子原始得分, 按(trade_date,ts_code,mode,factor)唯一 |
| `scan_result_intraday` | 盘中每轮 `_save_full_scan_to_db()` | 盘中全量扫描记录 |
| `scan_result_postmarket` | 盘后 `save_scan_results_postmarket()` | 盘后全量扫描记录 |
| `stock_tech_indicator` | Tushare stk_factor_pro 缓存 | MACD/RSI/KDJ/BOLL/CCI/ATR/MA |
| `realtime_spot` | 盘中每 30s | 实时价格/涨跌幅/换手率/量比 |

## 六、回测管线

```
FactorBacktestEngine.compute()
│
├─ _load_snapshots() → 从 factor_score_snapshots 读历史得分
│
├─ use_pipeline = False (默认):
│   _compute_composite(scores, weights) → 纯因子加权
│
└─ use_pipeline = True (与盘后扫描一致):
    ├─ _compute_composite → Top300 pool
    ├─ _batch_stockscorer_static(pool) → StockScorer技术分
    └─ blended = 0.3 × composite + 0.7 × tech_score
```
