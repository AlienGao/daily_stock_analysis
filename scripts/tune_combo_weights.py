#!/usr/bin/env python3
"""对 combos/ 下多因子组合微调权重：回撤约束 + min(两区间 5日收益) 选优。

默认：
  - 区间: 20250101, 20260101（结束日最新）
  - 权重倍数: 0.75, 1.0, 1.25（钳位 5~35）
  - 回撤 slack: +3pp（相对基准 worst 区间回撤）
  - 目标: min(ret5) across periods

用法:
  python scripts/tune_combo_weights.py
  python scripts/tune_combo_weights.py --dry-run
"""
from __future__ import annotations

import argparse
import copy
import csv
import json
import logging
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from itertools import product
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_provider.tushare_fetcher import TushareFetcher
from scripts.search_factor_combos import (
    FLM,
    HOLD_DAYS,
    INITIAL_CAPITAL,
    PRIMARY_HOLD,
    RISK_FREE_RATE,
    TOP_N,
    combo_label,
    period_stats_for_hold,
)
from src.discovery.factor_backtest_engine import FactorBacktestEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("combo_tune")

COMBOS_DIR = Path(__file__).resolve().parent.parent / "reports_simple_backtest" / "combos"
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "reports_simple_backtest" / "combo_tune"
WEIGHT_MIN = 5.0
WEIGHT_MAX = 35.0


def parse_fw_from_preset_name(preset_name: str) -> dict[str, float]:
    result: dict[str, float] = {}
    parts = preset_name.split("_")
    i = 2
    while i < len(parts):
        token = parts[i]
        if token.startswith("w") and token[1:].replace(".", "", 1).isdigit():
            w = float(token[1:])
            j = i + 1
            fp: list[str] = []
            while j < len(parts):
                nxt = parts[j]
                if nxt.startswith("w") and len(nxt) > 1 and nxt[1:].replace(".", "", 1).isdigit():
                    break
                fp.append(nxt)
                j += 1
            fn = "_".join(fp)
            if fn:
                result[fn] = w
            i = j
        else:
            i += 1
    return result


def load_combos_from_dir(combos_dir: Path) -> list[dict]:
    presets: list[dict] = []
    for f in sorted(combos_dir.glob("backtest_postmarket_*.md")):
        name = f.stem
        fw = parse_fw_from_preset_name(name)
        if len(fw) < 2:
            continue
        factors = tuple(fw.keys())
        presets.append({
            "name": name,
            "factor_weights": fw,
            "factors": factors,
            "label": combo_label(factors),
        })
    return presets


def build_weight_grid(baseline: dict[str, float], multipliers: list[float]) -> list[dict[str, float]]:
    factors = sorted(baseline.keys())
    grids: list[dict[str, float]] = []
    seen: set[tuple] = set()
    for ms in product(multipliers, repeat=len(factors)):
        fw: dict[str, float] = {}
        for f, m in zip(factors, ms):
            w = max(WEIGHT_MIN, min(WEIGHT_MAX, round(baseline[f] * m, 1)))
            fw[f] = w
        key = tuple(fw[f] for f in factors)
        if key in seen:
            continue
        seen.add(key)
        grids.append(fw)
    return grids


def patch_engine_cache(engine: FactorBacktestEngine) -> None:
    _orig_load = engine._load_snapshots
    _orig_dates = engine._get_available_dates
    _orig_ranges = engine.get_snapshot_date_ranges

    def _cached_load(factor_names, mode, dates, progress_cb=None):
        key = (tuple(sorted(factor_names)), mode, tuple(dates))
        cache = engine.__dict__.setdefault("_snap_cache", {})
        if key not in cache:
            cache[key] = _orig_load(factor_names, mode, dates, progress_cb=progress_cb)
        return copy.deepcopy(cache[key])

    def _cached_dates(factor_names, mode):
        key = (tuple(sorted(factor_names)), mode)
        cache = engine.__dict__.setdefault("_dates_cache", {})
        if key not in cache:
            cache[key] = _orig_dates(factor_names, mode)
        return cache[key]

    def _cached_ranges(mode):
        cache = engine.__dict__.setdefault("_ranges_cache", {})
        if mode not in cache:
            cache[mode] = _orig_ranges(mode)
        return cache[mode]

    engine._load_snapshots = _cached_load
    engine._get_available_dates = _cached_dates
    engine.get_snapshot_date_ranges = _cached_ranges


def run_backtest(engine, factor_weights, start_date, end_date):
    result = engine.compute(
        mode="postmarket",
        factor_weights=factor_weights,
        start_date=start_date,
        end_date=end_date,
        top_n=TOP_N,
        hold_days=HOLD_DAYS,
        initial_capital=INITIAL_CAPITAL,
        risk_free_rate=RISK_FREE_RATE,
    )
    return asdict(result) if result else None


def eval_period(result_dict):
    stats = period_stats_for_hold(result_dict, PRIMARY_HOLD)
    if not stats:
        return None
    return {
        "ret5": stats["total_return"],
        "mdd5": stats["max_drawdown"],
        "sharpe5": stats["sharpe"],
    }


def eval_multi_period(engine, factor_weights, periods, end_date):
    out = {}
    for p in periods:
        rd = run_backtest(engine, factor_weights, p, end_date)
        out[p] = eval_period(rd) if rd else None
    return out


def aggregate_metrics(period_metrics):
    if any(period_metrics[p] is None for p in period_metrics):
        return None
    ret5s = [period_metrics[p]["ret5"] for p in period_metrics]
    mdds = [period_metrics[p]["mdd5"] for p in period_metrics]
    sharpes = [period_metrics[p]["sharpe5"] for p in period_metrics]
    return {
        "min_ret5": min(ret5s),
        "avg_ret5": sum(ret5s) / len(ret5s),
        "worst_mdd5": max(mdds),
        "avg_sharpe5": sum(sharpes) / len(sharpes),
        "by_period": period_metrics,
    }


def select_best(trials, baseline_agg, mdd_slack):
    cap = baseline_agg["worst_mdd5"] + mdd_slack
    feasible = [t for t in trials if t["agg"] and t["agg"]["worst_mdd5"] <= cap + 1e-9]
    if not feasible:
        return None
    feasible.sort(
        key=lambda t: (t["agg"]["min_ret5"], t["agg"]["avg_sharpe5"], t["agg"]["avg_ret5"]),
        reverse=True,
    )
    return feasible[0]


def format_weight_line(fw: dict[str, float]) -> str:
    parts = [f"{FLM.get(k, k)} {fw[k]:g}" for k in sorted(fw)]
    return ", ".join(parts)


def format_weight_delta(baseline: dict[str, float], best: dict[str, float]) -> str:
    parts = []
    for k in sorted(baseline):
        b, o = baseline[k], best[k]
        label = FLM.get(k, k)
        if abs(b - o) < 1e-9:
            parts.append(f"{label} {b:g}")
        else:
            parts.append(f"{label} {b:g}→{o:g}")
    return ", ".join(parts)


def save_outputs(run_dir, summary_rows, all_trials, periods, mdd_slack):
    run_dir.mkdir(parents=True, exist_ok=True)
    period_cols = []
    for p in periods:
        period_cols.extend([f"ret5_{p}", f"mdd5_{p}", f"sharpe5_{p}"])
    csv_fields = [
        "combo_name", "combo_label", "is_baseline", "weights_json",
        "min_ret5", "avg_ret5", "worst_mdd5", "avg_sharpe5", *period_cols,
    ]
    with (run_dir / "results_all.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=csv_fields)
        w.writeheader()
        for t in all_trials:
            row = {
                "combo_name": t["combo_name"],
                "combo_label": t["combo_label"],
                "is_baseline": t.get("is_baseline", False),
                "weights_json": json.dumps(t["factor_weights"], ensure_ascii=False),
                "min_ret5": t["agg"]["min_ret5"] if t["agg"] else "",
                "avg_ret5": t["agg"]["avg_ret5"] if t["agg"] else "",
                "worst_mdd5": t["agg"]["worst_mdd5"] if t["agg"] else "",
                "avg_sharpe5": t["agg"]["avg_sharpe5"] if t["agg"] else "",
            }
            for p in periods:
                pm = t["agg"]["by_period"][p] if t["agg"] else None
                row[f"ret5_{p}"] = pm["ret5"] if pm else ""
                row[f"mdd5_{p}"] = pm["mdd5"] if pm else ""
                row[f"sharpe5_{p}"] = pm["sharpe5"] if pm else ""
            w.writerow(row)

    lines = [
        "# 组合权重微调 — 总结",
        "",
        f"- **区间**: {', '.join(periods)}（结束: 最新）",
        f"- **回撤 slack**: +{mdd_slack * 100:.0f}pp",
        f"- **目标函数**: min(两区间 5日总收益)",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 基准 vs 最优",
        "",
        "| 组合 | 基准 min(ret5) | 基准回撤 | 最优 min(ret5) | 最优回撤 | Δmin(ret5) | 可行/总数 |",
        "|------|----------------|----------|----------------|----------|------------|-----------|",
    ]
    for r in summary_rows:
        if r.get("best_weights"):
            delta = (r["best_min_ret5"] - r["baseline_min_ret5"]) * 100
            lines.append(
                f"| {r['label']} | {r['baseline_min_ret5'] * 100:+.2f}% "
                f"| {r['baseline_worst_mdd'] * 100:.2f}% "
                f"| {r['best_min_ret5'] * 100:+.2f}% | {r['best_worst_mdd'] * 100:.2f}% "
                f"| {delta:+.2f}pp | {r['feasible_count']}/{r['trial_count']} |"
            )
        else:
            lines.append(
                f"| {r['label']} | {r['baseline_min_ret5'] * 100:+.2f}% "
                f"| {r['baseline_worst_mdd'] * 100:.2f}% | — | — | — "
                f"| {r['feasible_count']}/{r['trial_count']} |"
            )

    lines.extend(["", "## 最优权重", ""])
    for r in summary_rows:
        lines.append(f"### {r['label']}")
        lines.append("")
        lines.append(f"- **基准**: `{json.dumps(r['baseline_weights'], ensure_ascii=False)}`")
        lines.append(f"  - {format_weight_line(r['baseline_weights'])}")
        if r.get("best_weights"):
            lines.append(f"- **最优**: `{json.dumps(r['best_weights'], ensure_ascii=False)}`")
            lines.append(f"  - {format_weight_delta(r['baseline_weights'], r['best_weights'])}")
        else:
            lines.append("- **最优**: 与基准相同（网格内无更优可行解）")
        lines.append("")

    best_json = []
    for r in summary_rows:
        entry = {
            "label": r["label"],
            "baseline_weights": r["baseline_weights"],
            "best_weights": r.get("best_weights") or r["baseline_weights"],
        }
        if r.get("best_weights"):
            entry["best_min_ret5"] = r["best_min_ret5"]
            entry["best_worst_mdd"] = r["best_worst_mdd"]
        best_json.append(entry)
    (run_dir / "best_weights.json").write_text(
        json.dumps(best_json, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines.append("")
    (run_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def tune_one_combo(engine, preset, periods, end_date, multipliers, mdd_slack):
    baseline_fw = preset["factor_weights"]
    trials = []
    for fw in build_weight_grid(baseline_fw, multipliers):
        is_baseline = all(fw[k] == baseline_fw[k] for k in baseline_fw)
        pm = eval_multi_period(engine, fw, periods, end_date)
        agg = aggregate_metrics(pm)
        trials.append({
            "combo_name": preset["name"],
            "combo_label": preset["label"],
            "factor_weights": fw,
            "is_baseline": is_baseline,
            "agg": agg,
        })

    baseline_agg = next(t["agg"] for t in trials if t.get("is_baseline") and t["agg"])
    cap = baseline_agg["worst_mdd5"] + mdd_slack
    feasible_count = sum(1 for t in trials if t["agg"] and t["agg"]["worst_mdd5"] <= cap + 1e-9)
    best = select_best(trials, baseline_agg, mdd_slack)

    summary = {
        "label": preset["label"],
        "baseline_weights": baseline_fw,
        "baseline_min_ret5": baseline_agg["min_ret5"],
        "baseline_worst_mdd": baseline_agg["worst_mdd5"],
        "baseline_by_period": {p: baseline_agg["by_period"][p] for p in periods},
        "trial_count": len(trials),
        "feasible_count": feasible_count,
    }
    if best and not best.get("is_baseline"):
        summary["best_weights"] = best["factor_weights"]
        summary["best_min_ret5"] = best["agg"]["min_ret5"]
        summary["best_worst_mdd"] = best["agg"]["worst_mdd5"]
        summary["best_by_period"] = {p: best["agg"]["by_period"][p] for p in periods}
    return trials, summary


def parse_args():
    p = argparse.ArgumentParser(description="combos/ 权重微调（min ret5 + 回撤 slack）")
    p.add_argument("--periods", default="20250101,20260101")
    p.add_argument("--end-date", default=None)
    p.add_argument("--multipliers", default="0.75,1.0,1.25")
    p.add_argument("--mdd-slack", type=float, default=0.03)
    p.add_argument("--combos-dir", default=str(COMBOS_DIR))
    p.add_argument("--combo", default=None)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    periods = [x.strip() for x in args.periods.split(",") if x.strip()]
    multipliers = [float(x.strip()) for x in args.multipliers.split(",") if x.strip()]
    presets = load_combos_from_dir(Path(args.combos_dir))
    if args.combo:
        want = set(x.strip() for x in args.combo.split(",") if x.strip())
        presets = [p for p in presets if set(p["factors"]) == want]
    if not presets:
        logger.error("未找到组合")
        return 1
    if args.dry_run:
        n = len(build_weight_grid(presets[0]["factor_weights"], multipliers))
        logger.info("%d combos × %d trials × %d periods = %d runs",
                    len(presets), n, len(periods), len(presets) * n * len(periods))
        return 0

    run_dir = OUTPUT_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    engine = FactorBacktestEngine(TushareFetcher.get_instance())
    patch_engine_cache(engine)
    all_trials, summary_rows = [], []

    for idx, preset in enumerate(presets, 1):
        logger.info("[%d/%d] %s", idx, len(presets), preset["label"])
        t0 = time.time()
        trials, summary = tune_one_combo(
            engine, preset, periods, args.end_date, multipliers, args.mdd_slack,
        )
        all_trials.extend(trials)
        summary_rows.append(summary)
        if summary.get("best_weights"):
            logger.info("  best min(ret5)=%+.2f%% vs baseline %+.2f%% weights=%s",
                        summary["best_min_ret5"] * 100, summary["baseline_min_ret5"] * 100,
                        summary["best_weights"])
        else:
            logger.info("  no improvement (baseline min_ret5=%+.2f%%)",
                        summary["baseline_min_ret5"] * 100)
        logger.info("  %.0fs feasible %d/%d", time.time() - t0, summary["feasible_count"], summary["trial_count"])
        engine._price_cache.clear()
        FactorBacktestEngine._spot_df_cache = ()

    save_outputs(run_dir, summary_rows, all_trials, periods, args.mdd_slack)
    logger.info("done: %s", run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
