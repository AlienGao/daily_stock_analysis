"""Test whether per-node TPE dynamic rebalancing improves alpha vs fixed weights.

Key design:
- Each node uses a 60-day rolling window TPE, persisted to optuna_cache/factor_opt.db
- Study name: {mode}_w{window}_{end_date} (e.g. postmarket_w60_20260401)
- First run for a node is slow (TPE), subsequent runs hit DB cache instantly
- No look-ahead bias: each node's TPE only uses data up to the node's end date

Usage: python scripts/test_dynamic_alpha.py [--intervals 5,10,20] [--mode postmarket]
"""

import os
import sys
import time
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.discovery.factor_backtest_engine import FactorBacktestEngine, _optuna_study_has_trials

MODE = os.environ.get("TEST_MODE", "postmarket")
START = os.environ.get("TEST_START", "20250901")
END = os.environ.get("TEST_END", "20260515")
TOP_N = 5
HOLD_DAYS = [3, 5]
INITIAL_CAPITAL = 1_000_000
INTERVALS = [5, 10, 20]
N_TRIALS = 15

RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"


def fmt_pct(v: float) -> str:
    return f"{v * 100:+.2f}%"


def fmt_num(v: float) -> str:
    return f"{v:,.2f}"


def run_test(engine, interval: int) -> dict:
    """Run walk-forward and return comparison metrics."""
    print(f"\n{CYAN}{'='*70}{RESET}")
    print(f"{BOLD}reoptimize_interval = {interval} 交易日{RESET}")
    print(f"{CYAN}{'='*70}{RESET}")

    # Count cached vs new nodes
    snap_dates = engine._get_available_dates(
        list(engine._get_default_weights(MODE).keys()), MODE)
    sd = START if START >= snap_dates[0] else snap_dates[0]
    ed = END if END <= snap_dates[-1] else snap_dates[-1]
    snap_filtered = [d for d in snap_dates if sd <= d <= ed]
    window_size = 60
    node_end_indices = list(range(window_size - 1, len(snap_filtered), interval))

    cached = 0
    need_tpe = 0
    for node_idx in node_end_indices:
        opt_start = max(0, node_idx - window_size + 1)
        opt_window_dates = snap_filtered[opt_start:node_idx + 1]
        if len(opt_window_dates) < 20:
            continue
        node_study = f"{MODE}_w{window_size}_{opt_window_dates[-1]}"
        if _optuna_study_has_trials(node_study):
            cached += 1
        else:
            need_tpe += 1

    print(f"节点: {cached} 缓存命中, {need_tpe} 需跑 TPE (共 {cached + need_tpe} 个)")

    t0 = time.time()
    result = engine.compute_walk_forward(
        mode=MODE, start_date=START, end_date=END,
        top_n=TOP_N, hold_days=HOLD_DAYS, initial_capital=INITIAL_CAPITAL,
        reoptimize_interval=interval, n_trials=N_TRIALS, use_pipeline=False,
    )
    elapsed = time.time() - t0

    if result is None:
        print(f"{RED}  FAIL: compute_walk_forward returned None{RESET}")
        return None

    summ = result.summary
    dyn = summ.get("dynamic") if isinstance(summ, dict) else getattr(summ, "dynamic", None)

    fixed_m = {
        "cumulative_return": summ.get("cumulative_return"),
        "annualized_return": summ.get("annualized_return"),
        "sharpe_ratio": summ.get("sharpe_ratio"),
        "max_drawdown": summ.get("max_drawdown"),
        "win_rate": summ.get("win_rate"),
        "final_capital": summ.get("final_capital"),
        "total_trades": summ.get("total_trades"),
    }
    dyn_m = {
        "cumulative_return": dyn.get("cumulative_return") if dyn else None,
        "annualized_return": dyn.get("annualized_return") if dyn else None,
        "sharpe_ratio": dyn.get("sharpe_ratio") if dyn else None,
        "max_drawdown": dyn.get("max_drawdown") if dyn else None,
        "win_rate": dyn.get("win_rate") if dyn else None,
        "final_capital": dyn.get("final_capital") if dyn else None,
        "total_trades": dyn.get("total_trades") if dyn else None,
    }

    print(f"\n{BOLD}{'指标':<18} {'固定权重':>12} {'动态调优':>12} {'差值':>12}{RESET}")
    print("-" * 54)
    rows = [
        ("累计收益率", "cumulative_return"),
        ("年化收益率", "annualized_return"),
        ("夏普比率", "sharpe_ratio"),
        ("最大回撤", "max_drawdown"),
        ("胜率", "win_rate"),
    ]
    for label, key in rows:
        fv = fixed_m.get(key)
        dv = dyn_m.get(key)
        if fv is not None and dv is not None:
            delta = dv - fv if key != "max_drawdown" else fv - dv
            color = GREEN if delta > 0 else RED
            print(f"{label:<16} {fmt_pct(fv):>12} {fmt_pct(dv):>12} "
                  f"{color}{fmt_pct(delta):>12}{RESET}")

    print(f"{'最终资金':<16} {fmt_num(fixed_m['final_capital']):>12} "
          f"{fmt_num(dyn_m['final_capital']):>12}")
    print(f"{'交易次数':<16} {fixed_m['total_trades']:>12} {dyn_m['total_trades']:>12}")
    print(f"\n耗时: {elapsed:.1f}s")

    delta = {}
    for k in ["cumulative_return", "annualized_return", "sharpe_ratio"]:
        if fixed_m.get(k) is not None and dyn_m.get(k) is not None:
            delta[k] = dyn_m[k] - fixed_m[k]
    for k in ["max_drawdown"]:
        if fixed_m.get(k) is not None and dyn_m.get(k) is not None:
            delta[k] = fixed_m[k] - dyn_m[k]
    if fixed_m.get("win_rate") is not None and dyn_m.get("win_rate") is not None:
        delta["win_rate"] = dyn_m["win_rate"] - fixed_m["win_rate"]

    score = sum(1 for v in delta.values() if v is not None and v > 0)
    total = sum(1 for v in delta.values() if v is not None)
    verdict = (f"{GREEN}动态 > 固定 ({score}/{total}){RESET}" if score >= total / 2
               else f"{RED}固定 > 动态 ({total - score}/{total}){RESET}")
    print(f"\n{BOLD}综合: {verdict}{RESET}")

    return {"interval": interval, "fixed": fixed_m, "dynamic": dyn_m,
            "delta": delta, "elapsed": elapsed, "cached": cached, "tpe_runs": need_tpe}


def main():
    print(f"{BOLD}动态调优 Alpha 测试{RESET}")
    print(f"  模式: {MODE}  日期: {START}~{END}")
    print(f"  Top-N: {TOP_N}  Hold: {HOLD_DAYS}")
    print(f"  Trials/节点: {N_TRIALS}  区间: {INTERVALS}")

    engine = FactorBacktestEngine()
    results = []
    for interval in INTERVALS:
        r = run_test(engine, interval)
        if r:
            results.append(r)

    print(f"\n\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}总结{RESET}")
    print(f"{'='*70}")
    hdr = f"{'间隔':>6} {'累计收益':>12} {'年化收益':>12} {'夏普':>10} {'回撤改善':>10} {'胜率':>10} {'耗时':>8}"
    print(hdr)
    print("-" * 68)
    for r in results:
        d = r["delta"]
        print(f"{r['interval']:>5}日 "
              f"{fmt_pct(d.get('cumulative_return', 0) or 0):>12} "
              f"{fmt_pct(d.get('annualized_return', 0) or 0):>12} "
              f"{(d.get('sharpe_ratio') or 0):>+10.4f} "
              f"{fmt_pct(d.get('max_drawdown', 0) or 0):>10} "
              f"{fmt_pct(d.get('win_rate', 0) or 0):>10} "
              f"{r['elapsed']:>7.1f}s")

    best = max(results, key=lambda r: r["delta"].get("sharpe_ratio", -999) or -999)
    print(f"\n{BOLD}最佳间隔: {best['interval']} 日 "
          f"(动态夏普 {best['dynamic']['sharpe_ratio']:.4f} vs "
          f"固定 {best['fixed']['sharpe_ratio']:.4f}){RESET}")


if __name__ == "__main__":
    main()
