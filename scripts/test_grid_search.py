"""Grid search for postmarket factor backtest — find best parameter combination.

Tests all combos: hold [1,3,5] × pipeline [on,off] × reopt [5,10,20,null] × top_n [1..5]
Optimized: monkey-patches _load_snapshots / _get_available_dates with in-memory caches
so 40 compute_walk_forward calls share snapshot data.

Usage: python scripts/test_grid_search.py
"""

import os, sys, time, json, itertools, copy
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.discovery.factor_backtest_engine import FactorBacktestEngine

MODE = os.environ.get("TEST_MODE", "postmarket")
START = os.environ.get("TEST_START", "20250901")
END = os.environ.get("TEST_END", date.today().strftime("%Y%m%d"))
INITIAL_CAPITAL = 1_000_000

RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"

HOLD_DAYS = [1, 3, 5]
USE_PIPELINE = [False, True]
REOPT_INTERVALS = [5, 10, 20, None]
TOP_N_VALUES = [1, 2, 3, 4, 5]


def fmt_pct(v):
    return f"{v * 100:+.2f}%"


def fmt_num(v):
    return f"{v:,.2f}"


def _patch_engine(engine):
    """Monkey-patch expensive DB methods with in-memory caches + pickle persistence."""
    import pickle as _pickle
    _orig_load = engine._load_snapshots
    _orig_dates = engine._get_available_dates
    _orig_prices = engine._prefetch_prices
    _orig_ranges = engine.get_snapshot_date_ranges

    def _cached_load(factor_names, mode, dates, progress_cb=None):
        key = (tuple(sorted(factor_names)), mode, tuple(dates))
        c = engine.__dict__.setdefault("_snap_cache", {})
        if key not in c:
            c[key] = _orig_load(factor_names, mode, dates, progress_cb=progress_cb)
        return copy.deepcopy(c[key])

    def _cached_dates(factor_names, mode):
        key = (tuple(sorted(factor_names)), mode)
        c = engine.__dict__.setdefault("_dates_cache", {})
        if key not in c:
            c[key] = _orig_dates(factor_names, mode)
        return c[key]

    def _cached_ranges(mode):
        key = mode
        c = engine.__dict__.setdefault("_ranges_cache", {})
        if key not in c:
            c[key] = _orig_ranges(mode)
        return c[key]

    def _skip_prices(codes, tds):
        _orig_prices(codes, tds)

    engine._load_snapshots = _cached_load
    engine._get_available_dates = _cached_dates
    engine._prefetch_prices = _skip_prices
    engine.get_snapshot_date_ranges = _cached_ranges


def run_grid():
    engine = FactorBacktestEngine()
    _patch_engine(engine)

    fw = engine._get_default_weights(MODE)
    snap_dates = engine._get_available_dates(list(fw.keys()), MODE)
    sd = max(START, snap_dates[0]) if snap_dates else START
    ed = min(END, snap_dates[-1]) if snap_dates else END
    sn_filtered = [d for d in snap_dates if sd <= d <= ed]
    total_combos = len(HOLD_DAYS) * len(USE_PIPELINE) * len(REOPT_INTERVALS) * len(TOP_N_VALUES)
    total_runs = len(USE_PIPELINE) * len(REOPT_INTERVALS) * len(TOP_N_VALUES)

    print(f"{BOLD}Grid Search: Postmarket Factor Backtest{RESET}")
    print(f"  模式: {MODE}  日期: {sd}~{ed}  快照数: {len(sn_filtered)}")
    print(f"  数据点: {total_combos} (来自 {total_runs} 次回测)")
    print(f"  优化: snapshot + dates 内存缓存, 价格预取一次")
    sys.stdout.flush()

    # Pre-warm: first call populates snapshot + price caches (慢, 仅一次)
    print(f"\n{CYAN}预热中（首次加载快照 + 预取价格，后续复用）…{RESET}")
    sys.stdout.flush()
    t_pre = time.time()
    _ = engine.compute_walk_forward(
        mode=MODE, start_date=START, end_date=END,
        top_n=5, hold_days=HOLD_DAYS, initial_capital=INITIAL_CAPITAL,
        use_pipeline=False, reoptimize_interval=5,
    )
    print(f"  预热完成 ({time.time() - t_pre:.1f}s)")
    sys.stdout.flush()

    results = []
    run_idx = 0
    grid_start = time.time()

    for use_pl, reopt, top_n in itertools.product(USE_PIPELINE, REOPT_INTERVALS, TOP_N_VALUES):
        run_idx += 1
        reopt_label = "固定" if reopt is None else f"{reopt}日调优"
        pl_label = "管线" if use_pl else "纯因子"

        print(f"\n{CYAN}[{run_idx}/{total_runs}]{RESET} {pl_label} | {reopt_label} | "
              f"Top-{top_n} | 持有 {HOLD_DAYS}日")
        sys.stdout.flush()

        t0 = time.time()
        try:
            result = engine.compute_walk_forward(
                mode=MODE, start_date=START, end_date=END,
                top_n=top_n, hold_days=HOLD_DAYS,
                initial_capital=INITIAL_CAPITAL,
                use_pipeline=use_pl,
                reoptimize_interval=reopt if reopt is not None else 9999,
            )
        except Exception as e:
            print(f"  {RED}FAIL: {e}{RESET}")
            sys.stdout.flush()
            continue
        elapsed = time.time() - t0

        if result is None:
            print(f"  {RED}→ None{RESET}")
            sys.stdout.flush()
            continue

        summ = result.summary
        curves = getattr(result, "capital_curves", {})
        if not curves:
            print(f"  {RED}→ 无 capital_curves{RESET}")
            sys.stdout.flush()
            continue

        # Best hold day by fixed capital curve (final capital)
        best_hd = max(
            [(hd, curves[f"{hd}_fixed"][-1]["capital"]) for hd in HOLD_DAYS
             if f"{hd}_fixed" in curves and curves[f"{hd}_fixed"]],
            key=lambda x: x[1], default=(None, 0))
        if best_hd[0] is not None:
            ret = (best_hd[1] - INITIAL_CAPITAL) / INITIAL_CAPITAL
            print(f"  {GREEN}最佳持有{best_hd[0]}日: {fmt_pct(ret)} "
                  f"(夏普 {summ.get('sharpe_ratio', 0):.4f}) "
                  f"{elapsed:.1f}s{RESET}")
            sys.stdout.flush()

        for hd in HOLD_DAYS:
            fkey = f"{hd}_fixed"
            dkey = f"{hd}_dynamic"
            curve = curves.get(fkey, [])
            dcurve = curves.get(dkey, [])
            if not curve:
                continue
            final_cap = curve[-1]["capital"]
            total_ret = (final_cap - INITIAL_CAPITAL) / INITIAL_CAPITAL

            results.append({
                "pipeline": use_pl,
                "reoptimize": reopt,
                "top_n": top_n,
                "hold_days": hd,
                "final_capital": round(final_cap, 2),
                "total_return": round(total_ret, 4),
                "annualized_return": summ.get("annualized_return"),
                "sharpe_ratio": summ.get("sharpe_ratio"),
                "max_drawdown": summ.get("max_drawdown"),
                "win_rate": summ.get("win_rate"),
                "total_trades": summ.get("total_trades"),
                "dyn_final_capital": round(dcurve[-1]["capital"], 2) if dcurve else None,
                "dyn_total_return": round((dcurve[-1]["capital"] - INITIAL_CAPITAL) / INITIAL_CAPITAL, 4) if dcurve else None,
                "dyn_sharpe": summ.get("dynamic", {}).get("sharpe_ratio") if isinstance(summ.get("dynamic"), dict) else None,
                "elapsed": round(elapsed, 1),
            })

    print(f"\n{BOLD}Grid 计算耗时: {time.time() - grid_start:.0f}s{RESET}")
    sys.stdout.flush()
    return results


def print_top(results, metric="total_return", top_k=15):
    s = sorted(results, key=lambda r: r.get(metric, -999) or -999, reverse=True)
    print(f"\n\n{BOLD}{'='*100}{RESET}")
    print(f"{BOLD}  Top {top_k} by {metric}{RESET}")
    print(f"{'='*100}")
    hdr = (f"{'#':>3} {'管线':>4} {'调优':>8} {'TopN':>5} {'持有':>5} "
           f"{'总收益':>10} {'年化':>10} {'夏普':>8} {'回撤':>10} {'胜率':>8} {'交易':>6} {'最终资金':>14}")
    print(hdr)
    print("-" * 100)
    for i, r in enumerate(s[:top_k], 1):
        pl = "Y" if r["pipeline"] else "N"
        reopt = "固定" if r["reoptimize"] is None else f"{r['reoptimize']}日"
        print(f"{i:>3} {pl:>4} {reopt:>8} {r['top_n']:>5} {r['hold_days']:>4}日 "
              f"{fmt_pct(r['total_return']):>10} "
              f"{fmt_pct(r['annualized_return'] or 0):>10} "
              f"{(r['sharpe_ratio'] or 0):>8.4f} "
              f"{fmt_pct(r['max_drawdown'] or 0):>10} "
              f"{fmt_pct(r['win_rate'] or 0):>8} "
              f"{r['total_trades'] or 0:>6} "
              f"{fmt_num(r['final_capital']):>14}")
    sys.stdout.flush()


def print_best_by_category(results):
    print(f"\n\n{BOLD}{'='*100}{RESET}")
    print(f"{BOLD}  各分类最优 (按总收益){RESET}")
    print(f"{'='*100}")
    for hd in HOLD_DAYS:
        print(f"\n{CYAN}── 持有 {hd} 日 ──{RESET}")
        sub = [r for r in results if r["hold_days"] == hd]
        for use_pl in [False, True]:
            pl_sub = [r for r in sub if r["pipeline"] == use_pl]
            if not pl_sub:
                continue
            best = max(pl_sub, key=lambda r: r["total_return"])
            reopt = "固定" if best["reoptimize"] is None else f"{best['reoptimize']}日"
            pl = "管线" if use_pl else "纯因子"
            print(f"  {pl}: Top-{best['top_n']} | {reopt} | "
                  f"收益 {fmt_pct(best['total_return'])} | "
                  f"夏普 {(best['sharpe_ratio'] or 0):.4f} | "
                  f"回撤 {fmt_pct(best['max_drawdown'] or 0)} | "
                  f"交易 {best['total_trades']}")
    sys.stdout.flush()


def main():
    t0 = time.time()
    results = run_grid()

    if not results:
        print(f"{RED}No results!{RESET}")
        return

    out_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..",
        "reports_discovery", f"grid_search_{date.today().strftime('%Y%m%d')}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")

    print_top(results, "total_return", top_k=15)
    print_top(results, "sharpe_ratio", top_k=15)
    print_best_by_category(results)

    # Top 2
    print(f"\n\n{BOLD}{'='*100}{RESET}")
    print(f"{BOLD}  Top 2 (按总收益){RESET}")
    print(f"{'='*100}")
    s = sorted(results, key=lambda r: r["total_return"], reverse=True)
    for i, r in enumerate(s[:2], 1):
        pl = "管线" if r["pipeline"] else "纯因子"
        reopt = "固定权重" if r["reoptimize"] is None else f"{r['reoptimize']}日动态调优"
        label = "#1 首选" if i == 1 else "#2 备选"
        print(f"\n{label}: {pl} | {reopt} | Top-{r['top_n']} | 持有{r['hold_days']}日")
        print(f"  总收益: {fmt_pct(r['total_return'])}  |  夏普: {(r['sharpe_ratio'] or 0):.4f}  |  "
              f"回撤: {fmt_pct(r['max_drawdown'] or 0)}  |  胜率: {fmt_pct(r['win_rate'] or 0)}  |  "
              f"交易: {r['total_trades']}  |  最终: {fmt_num(r['final_capital'])}")

    print(f"\n{BOLD}总耗时: {time.time() - t0:.0f}s ({(time.time() - t0)/60:.1f}min){RESET}")


if __name__ == "__main__":
    main()
