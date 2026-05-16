# FactorMonitor → FactorBacktestEngine 合并方案

## 现状

两套因子质量评估系统并存：

| | FactorMonitor | FactorBacktestEngine |
|---|---|---|
| 文件 | `src/discovery/factor_monitor.py` | `src/discovery/factor_backtest_engine.py` |
| 触发 | 每日自动（Phase 5.6） | 手动（Web UI 按钮） |
| 核心指标 | forward return（实际涨跌幅） | Rank IC（Spearman 秩相关） |
| 存储 | `discovery_reports/factor_monitor/picks/*.json` + `performance.json` | score snapshots（DB） |
| 报告 | Markdown（`format_report()`） | 前端图表（资金曲线 + IC 表格） |
| 额外功能 | 因子变化检测 + 历史回放 | 多持有期、管线模式、分位数收益 |
| 下游消费 | Phase 5.7 FactorTuner 读取 `performance.json` | 无 |

## 合并方案

### 1. FactorBacktestEngine 新增 `quick_monitor()` 方法

```python
def quick_monitor(self, mode: str = "postmarket", window: int = 20,
                  hold_days: List[int] = None) -> Dict:
    """快速因子监控：加载最近 N 天快照，计算每个因子的 Rank IC。

    替代 FactorMonitor 的日常监控角色。无需记录 picks 和回填，
    直接从已有 factor_score_snapshots 表读取并计算 IC。

    Returns:
        {
            "factors": {
                "technical": {"ic_1d": 0.052, "ic_5d": 0.038},
                ...
            },
            "summary": "## 因子监控报告\n...",
            "trade_dates": ["20260501", ...],
            "generated_at": "2026-05-16T18:00:00",
        }
    """
```

**关键设计**：
- 输入：从 `factor_score_snapshots` 表读取最近 N 天快照
- 计算：复用已有 `_calc_rank_ic(scores_by_date, hd, trading_days, mode)`
- 输出：每个因子的多持有期 IC + Markdown 报告
- 性能：N=20 天 × 15 因子，预计 <5 秒

### 2. 替换 engine.py Phase 5.6

**删除**（engine.py:1520-1534）：
```python
monitor = FactorMonitor(top_n=20, eval_days=5)
monitor.detect_factor_changes(current_factors, mode)
monitor.replay_history(self, mode, days=5)
monitor.record_picks(raw_scores, trade_date, mode=mode)
monitor.backfill(trade_date)
```

**替换为**：
```python
backtest_engine = FactorBacktestEngine(self.tushare_fetcher)
report = backtest_engine.quick_monitor(mode=mode, window=20)
if report:
    logger.info("[Monitor] %s", report["summary"])
```

**不再需要的功能**：
- `record_picks()` → 不再存每日 picks JSON
- `backfill()` → IC 计算不需要等 N 天后
- `detect_factor_changes()` + `replay_history()` → 因子变化后 IC 自然反映

### 3. 更新 Phase 5.7 FactorTuner

FactorTuner 当前读取 `performance.json`。建议直接删除 FactorTuner —— IC 比 forward return 更可靠，用户通过 Web UI 看 IC 手动调权重。

### 4. 删除文件

| 文件 | 原因 |
|------|------|
| `src/discovery/factor_monitor.py` | 功能被 `quick_monitor()` 替代 |
| `src/discovery/factor_tuner.py` | 自动调优改为手动 |
| `discovery_reports/factor_monitor/` | picks JSON + performance.json 不再需要 |

### 5. 改动清单

| 文件 | 改动 | 行数 |
|------|------|------|
| `src/discovery/factor_backtest_engine.py` | 新增 `quick_monitor()` | +60 |
| `src/discovery/engine.py` | Phase 5.6 替换 + Phase 5.7 删除 | -30/+10 |
| `src/discovery/factor_monitor.py` | 删除 | -529 |
| `src/discovery/factor_tuner.py` | 删除 | -150 |

## 不改的

- 不做因子变化检测和自动回放 — IC 是统计指标，新因子 N 天后自然有数据
- 不保留 forward return — Rank IC 更严谨（消除市场方向偏差）
- Web UI 不做独立监控面板 — 回测页已有 IC 展示

## 收益

1. 代码减少 ~700 行
2. 不再产 picks JSON 和 performance.json
3. 监控和回测一致使用 Rank IC，消除指标不一致
4. IC 计算无需等 N 天回填，当天即可评估
5. 一套 IC 计算逻辑，不再重复实现
