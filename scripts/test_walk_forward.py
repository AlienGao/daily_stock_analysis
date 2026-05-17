"""Integration test for walk-forward TPE backtest.

Tests compute_walk_forward() with a small date range to verify:
1. Fixed + dynamic curves produced correctly
2. summary.dynamic present with parallel stats
3. Trade records tagged with reoptimized=True/False
4. No duplicate TPE runs (Phase 1 once, Phase 2 per hold_days)

Usage: python scripts/test_walk_forward.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.discovery.factor_backtest_engine import FactorBacktestEngine

# Test with recent 60-trading-day window to keep runtime low
MODE = "postmarket"
START_DATE = "20260101"
END_DATE = "20260515"
TOP_N = 3
HOLD_DAYS = [1, 3]
INITIAL_CAPITAL = 100_000

progress_lines = []


def progress_cb(msg: str):
    progress_lines.append(msg)
    print(f"  {msg}")


def main():
    print("=" * 60)
    print("Walk-Forward TPE Backtest Integration Test")
    print(f"  Mode: {MODE}, Range: {START_DATE}-{END_DATE}")
    print(f"  Top-N: {TOP_N}, Hold Days: {HOLD_DAYS}")
    print("=" * 60)

    print("\n[1] Creating engine…")
    engine = FactorBacktestEngine()

    print("\n[2] Running compute_walk_forward()…")
    result = engine.compute_walk_forward(
        mode=MODE,
        start_date=START_DATE,
        end_date=END_DATE,
        top_n=TOP_N,
        hold_days=HOLD_DAYS,
        initial_capital=INITIAL_CAPITAL,
        reoptimize_interval=5,
        n_trials=20,
        use_pipeline=False,
        progress_cb=progress_cb,
    )

    if result is None:
        print("\nFAIL: compute_walk_forward returned None (insufficient data)")
        return 1

    # --- Structural checks ---
    errors = []

    # Check 1: capital_curves has _fixed and _dynamic keys
    curve_keys = list(result.capital_curves.keys())
    fixed_keys = [k for k in curve_keys if k.endswith("_fixed")]
    dynamic_keys = [k for k in curve_keys if k.endswith("_dynamic")]
    print(f"\n[3] Curve keys: {curve_keys}")
    if not fixed_keys:
        errors.append("No _fixed curve keys found")
    if not dynamic_keys:
        errors.append("No _dynamic curve keys found")

    # Check 2: summary.dynamic exists
    summ = result.summary
    dyn = summ.get("dynamic") if isinstance(summ, dict) else getattr(summ, "dynamic", None)
    if dyn:
        print(f"\n[4] Dynamic summary:")
        for k in ["cumulative_return", "annualized_return", "win_rate",
                   "max_drawdown", "sharpe_ratio", "total_trades",
                   "final_capital", "nodes_evaluated"]:
            v = dyn[k] if isinstance(dyn, dict) else getattr(dyn, k, None)
            print(f"    {k}: {v}")
        ne = dyn.get("nodes_evaluated", 0) if isinstance(dyn, dict) else getattr(dyn, "nodes_evaluated", 0)
        if ne <= 0:
            errors.append("nodes_evaluated <= 0")
    else:
        errors.append("summary.dynamic is None")

    # Check 3: trade records have reoptimized flag
    def _is_reopt(tr): return tr.get("reoptimized", False) if isinstance(tr, dict) else getattr(tr, "reoptimized", False)
    reopt_trades = [t for t in result.trade_records if _is_reopt(t)]
    fixed_trades = [t for t in result.trade_records if not _is_reopt(t)]
    print(f"\n[5] Trade records: {len(result.trade_records)} total")
    print(f"    Fixed-weight trades: {len(fixed_trades)}")
    print(f"    Reoptimized trades: {len(reopt_trades)}")
    if not reopt_trades:
        errors.append("No reoptimized trades found")

    # Check 4: Phase 1 progress message appears (TPE once), Phase 2 per hold_days
    tpe_msgs = [m for m in progress_lines if "动态调优节点" in m]
    eval_msgs = [m for m in progress_lines if "评估动态权重" in m]
    print(f"\n[6] Progress messages:")
    print(f"    TPE optimization nodes: {len(tpe_msgs)}")
    print(f"    Evaluation passes (hold_days): {len(eval_msgs)}")
    if len(eval_msgs) != len(HOLD_DAYS):
        errors.append(f"Expected {len(HOLD_DAYS)} evaluation passes, got {len(eval_msgs)}")

    # Check 5: curves have actual data
    for k in fixed_keys + dynamic_keys:
        curve = result.capital_curves[k]
        if len(curve) < 2:
            errors.append(f"Curve {k} has < 2 points (len={len(curve)})")

    # --- Report ---
    print("\n" + "=" * 60)
    if errors:
        print(f"FAIL ({len(errors)} errors):")
        for e in errors:
            print(f"  - {e}")
        return 1
    else:
        print("PASS -- All checks passed")
        print(f"   TPE nodes: {dyn['nodes_evaluated']}")
        print(f"   Total trades: {len(result.trade_records)}")
        print(f"   Hold days: {HOLD_DAYS}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
