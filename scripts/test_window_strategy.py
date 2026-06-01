"""对比两种 TPE 窗口策略的回测收益：随机窗口 vs 最近 60 日。

策略 A（当前）: 每个 trial 从窗口池随机抽 5 个子窗口
策略 B（最近）: 每个 trial 仅用窗口池中最新的 1 个子窗口

只测管线 ON + 5日重调优（TPE 调用最密集），覆盖 top_n 1-5 + 持有 1/3/5。

Usage: python scripts/test_window_strategy.py
"""

import os, sys, time, json
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
YELLOW = "\033[33m"

HOLD_DAYS = [1, 3, 5]
TOP_N_VALUES = [1, 2, 3, 4, 5]
STRATEGIES = [
    ("random", "随机 5 窗口"),
    ("recent", "最近 1 窗口"),
]


def fmt_pct(v):
    return f"{v * 100:+.2f}%"


def _patch_engine(engine):
    """Monkey-patch DB 方法做内存缓存。"""
    import copy
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

    def _prefetch(codes, tds):
        _orig_prices(codes, tds)

    engine._load_snapshots = _cached_load
    engine._get_available_dates = _cached_dates
    engine._prefetch_prices = _prefetch
    engine.get_snapshot_date_ranges = _cached_ranges


def _patch_optimizer(strategy: str):
    """替换 FactorOptimizer._tpe_search 的窗口选取逻辑。"""
    from src.discovery import factor_optimizer as fopt
    import random as _random
    import optuna
    from src.discovery.factor_optimizer import _TPE_STORAGE, _WEIGHT_MIN, _WEIGHT_MAX

    def _patched_tpe(self, candidates, current_weights, mode, window,
                     n_trials, normalize=False, preloaded=None,
                     use_persistent_storage=True, study_name=None):
        candidate_names = list(candidates.keys())
        all_factor_names = list(current_weights.keys())
        study_name = study_name or f"{mode}_w{window}"

        # 对比测试强制纯内存，不污染 optuna_cache
        study = optuna.create_study(
            direction="maximize",
            storage=None,
            study_name=study_name + f"_{strategy}",
            load_if_exists=False,
            sampler=optuna.samplers.TPESampler(seed=42),
        )

        if preloaded:
            all_snap_dates = preloaded["snap_dates"]
            all_scores = preloaded["scores"]
            full_tdays = preloaded["trading_days"]
            _window_pool = preloaded["window_pool"]
        else:
            all_snap_dates = self._get_recent_snap_dates(all_factor_names, mode, 9999)
            all_scores = self._engine._load_snapshots(all_factor_names, mode, all_snap_dates)
            all_codes = set()
            for ss in all_scores.values():
                for s in ss.values():
                    if hasattr(s, 'index'):
                        all_codes.update(s.index.tolist())
            full_tdays = self._engine._get_trading_days(all_snap_dates)
            self._engine._prefetch_prices(list(all_codes), full_tdays)
            _window_pool = []
            w = window
            for i in range(w - 1, len(all_snap_dates), 5):
                seg = all_snap_dates[max(0, i - w + 1): i + 1]
                if len(seg) >= 30:
                    _window_pool.append(seg)
            if not _window_pool and len(all_snap_dates) >= 20:
                _window_pool = [all_snap_dates]

        all_results = []
        best = None

        def objective(trial):
            nonlocal best
            full_weights = dict(current_weights)
            for fn in candidate_names:
                full_weights[fn] = trial.suggest_int(fn, _WEIGHT_MIN, _WEIGHT_MAX)

            excesses = []
            # ── 窗口选取策略（唯一差异）──
            if strategy == "recent":
                chosen = [_window_pool[-1]]
            else:
                chosen = _random.sample(_window_pool, min(5, len(_window_pool)))

            for w_dates in chosen:
                w_tdays = self._engine._get_trading_days(w_dates)
                ar, mdd, _, _, base_ar = self._evaluate_combo(
                    full_weights, all_scores, w_dates, w_tdays, mode)
                if ar is not None and base_ar is not None and mdd is not None:
                    excesses.append(ar - base_ar)

            return sum(excesses) / len(excesses) if excesses else -999.0

        try:
            study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
        except Exception:
            pass

        completed = [t for t in study.trials
                      if t.state == optuna.trial.TrialState.COMPLETE]
        if not completed:
            return [], None, 0

        completed.sort(key=lambda t: t.value, reverse=True)
        bt = completed[0]
        best_weights = {fn: bt.params[fn] for fn in candidate_names if fn in bt.params}

        # 用最新窗口评估最优权重，补齐 _build_report 需要的字段
        latest = all_snap_dates[-window:] if len(all_snap_dates) >= window else all_snap_dates
        latest_tdays = self._engine._get_trading_days(latest)
        l_ar, l_mdd, l_sh, l_wr, _ = self._evaluate_combo(
            best_weights, all_scores, latest, latest_tdays, mode)

        best = {
            "weights": best_weights,
            "annual_return": l_ar if l_ar is not None else 0.0,
            "max_drawdown": l_mdd if l_mdd is not None else 0.0,
            "sharpe": l_sh if l_sh is not None else 0.0,
            "win_rate": l_wr if l_wr is not None else 0.0,
            "fallback_triggered": (l_mdd or 0) > 0.25,
        }
        return all_results, best, len(completed)

    fopt.FactorOptimizer._tpe_search = _patched_tpe


def run_comparison():
    engine = FactorBacktestEngine()
    _patch_engine(engine)

    # —— 诊断：逐步定位因子加载问题 ——
    print(f"{BOLD}诊断中…{RESET}")
    sys.stdout.flush()

    # 1. 直接调 get_factor_weights（绕过 engine 的 try/except）
    try:
        from src.discovery.engine import get_factor_weights
        # 清除 lru_cache 避免旧结果干扰
        get_factor_weights.cache_clear()
        raw_fw = get_factor_weights(MODE)
        print(f"  get_factor_weights({MODE}) → {len(raw_fw)} 因子: {list(raw_fw.keys())[:5]}...")
    except Exception as e:
        import traceback
        print(f"  {RED}get_factor_weights 异常: {e}{RESET}")
        traceback.print_exc()
        raw_fw = {}
    sys.stdout.flush()

    # 2. 直接查 DB（绕过 engine 所有方法）
    from src.storage import DatabaseManager, FactorScoreSnapshot
    from sqlalchemy import distinct as sa_distinct, func
    db = DatabaseManager()
    try:
        with db.get_session() as s:
            total = s.query(func.count()).select_from(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.mode == MODE).scalar()
            factors = s.query(sa_distinct(FactorScoreSnapshot.factor_name)).filter(
                FactorScoreSnapshot.mode == MODE).all()
            print(f"  DB FactorScoreSnapshot: {total} 行, {len(factors)} 因子 (mode={MODE})")
            if factors:
                print(f"    因子列表: {[r[0] for r in factors]}")
    except Exception as e:
        print(f"  {RED}DB 查询失败: {e}{RESET}")
        import traceback
        traceback.print_exc()
    sys.stdout.flush()

    # 3. 检查 engine._get_default_weights（带异常捕获）
    fw = engine._get_default_weights(MODE)
    available = engine._list_factors_with_data(MODE)
    fw = {k: v for k, v in fw.items() if k in available}
    print(f"  过滤后因子: {len(fw)} (config={len(engine._get_default_weights(MODE))}, db_available={len(available)})")
    sys.stdout.flush()

    if not fw:
        print(f"\n  {RED}因子为空，无法继续。请确认:{RESET}")
        print(f"    1. 是否从项目根目录运行: cd /path/to/daily_stock_analysis && python scripts/test_window_strategy.py")
        print(f"    2. .env 中是否配置了因子权重")
        print(f"    3. DB 中是否有 mode={MODE} 的快照数据")
        print(f"    4. 对比: python scripts/test_grid_search.py 是否正常")
        sys.stdout.flush()
        return []
    snap_dates = engine._get_available_dates(list(fw.keys()), MODE)
    sd = max(START, snap_dates[0]) if snap_dates else START
    ed = min(END, snap_dates[-1]) if snap_dates else END
    sn_filtered = [d for d in snap_dates if sd <= d <= ed]

    total = len(TOP_N_VALUES) * len(STRATEGIES)

    print(f"{BOLD}TPE 窗口策略对比: 随机 5 窗口 vs 最近 1 窗口{RESET}")
    print(f"  模式: {MODE}  日期: {sd}~{ed}  快照数: {len(sn_filtered)}")
    print(f"  配置: 管线 ON + 5日调优 | top_n 1-5 x 持有 1/3/5 x 2 策略")
    sys.stdout.flush()

    # 预热（策略 A：随机窗口，同时填充 snapshot + price 缓存）
    print(f"\n{CYAN}预热中…{RESET}")
    sys.stdout.flush()
    t_pre = time.time()
    _patch_optimizer("random")
    _ = engine.compute_walk_forward(
        mode=MODE, start_date=START, end_date=END,
        top_n=5, hold_days=HOLD_DAYS, initial_capital=INITIAL_CAPITAL,
        use_pipeline=True, reoptimize_interval=5,
    )
    print(f"  预热完成 ({time.time() - t_pre:.1f}s)")
    sys.stdout.flush()

    all_results = []
    run_idx = 0
    grid_start = time.time()

    for strat_key, strat_label in STRATEGIES:
        _patch_optimizer(strat_key)
        print(f"\n{YELLOW}{'='*60}{RESET}")
        print(f"{YELLOW}  策略: {strat_label}{RESET}")
        print(f"{YELLOW}{'='*60}{RESET}")
        sys.stdout.flush()

        for top_n in TOP_N_VALUES:
            run_idx += 1
            print(f"\n{CYAN}[{run_idx}/{total}]{RESET} {strat_label} | "
                  f"Top-{top_n} | 管线 ON | 5日调优 | 持有 {HOLD_DAYS}日")
            sys.stdout.flush()

            t0 = time.time()
            try:
                result = engine.compute_walk_forward(
                    mode=MODE, start_date=START, end_date=END,
                    top_n=top_n, hold_days=HOLD_DAYS,
                    initial_capital=INITIAL_CAPITAL,
                    use_pipeline=True,
                    reoptimize_interval=5,
                )
            except Exception as e:
                print(f"  {RED}FAIL: {e}{RESET}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
                continue
            elapsed = time.time() - t0

            if result is None:
                print(f"  {RED}-> None{RESET}")
                sys.stdout.flush()
                continue

            summ = result.summary
            curves = getattr(result, "capital_curves", {})
            if not curves:
                print(f"  {RED}-> 无 capital_curves{RESET}")
                sys.stdout.flush()
                continue

            best_hd = max(
                [(hd, curves[f"{hd}_fixed"][-1]["capital"]) for hd in HOLD_DAYS
                 if f"{hd}_fixed" in curves and curves[f"{hd}_fixed"]],
                key=lambda x: x[1], default=(None, 0))
            if best_hd[0] is not None:
                ret = (best_hd[1] - INITIAL_CAPITAL) / INITIAL_CAPITAL
                dyn = summ.get("dynamic") if isinstance(summ, dict) else None
                dyn_data = dyn if isinstance(dyn, dict) else {}
                dyn_sharpe = dyn_data.get("sharpe_ratio", 0)
                print(f"  {GREEN}最佳持有{best_hd[0]}日: {fmt_pct(ret)} "
                      f"(夏普 {summ.get('sharpe_ratio', 0):.4f}) "
                      f"动态夏普 {dyn_sharpe:.4f} "
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
                dyn = summ.get("dynamic") if isinstance(summ, dict) else None
                dyn_data = dyn if isinstance(dyn, dict) else {}

                all_results.append({
                    "strategy": strat_key,
                    "strategy_label": strat_label,
                    "top_n": top_n,
                    "hold_days": hd,
                    "final_capital": round(final_cap, 2),
                    "total_return": round(total_ret, 4),
                    "sharpe_ratio": summ.get("sharpe_ratio"),
                    "max_drawdown": summ.get("max_drawdown"),
                    "win_rate": summ.get("win_rate"),
                    "total_trades": summ.get("total_trades"),
                    "dyn_final_capital": round(dcurve[-1]["capital"], 2) if dcurve else None,
                    "dyn_total_return": round((dcurve[-1]["capital"] - INITIAL_CAPITAL) / INITIAL_CAPITAL, 4) if dcurve else None,
                    "dyn_sharpe": dyn_data.get("sharpe_ratio"),
                    "elapsed": round(elapsed, 1),
                })

    print(f"\n{BOLD}对比耗时: {time.time() - grid_start:.0f}s{RESET}")
    sys.stdout.flush()
    return all_results


def print_comparison(results):
    print(f"\n\n{BOLD}{'='*100}{RESET}")
    print(f"{BOLD}  策略对比 (按固定权重总收益){RESET}")
    print(f"{'='*100}")
    hdr = (f"{'策略':>12} {'TopN':>5} {'持有':>5} "
           f"{'固定收益':>10} {'固定夏普':>9} "
           f"{'动态收益':>10} {'动态夏普':>9} {'回撤':>8}")
    print(hdr)
    print("-" * 100)

    for strat_key, strat_label in STRATEGIES:
        for top_n in TOP_N_VALUES:
            sub = [r for r in results
                   if r["strategy"] == strat_key and r["top_n"] == top_n]
            if not sub:
                continue
            best = max(sub, key=lambda r: r["total_return"])
            print(f"{strat_label:>12} {top_n:>5} {best['hold_days']:>4}日 "
                  f"{fmt_pct(best['total_return']):>10} "
                  f"{(best['sharpe_ratio'] or 0):>9.4f} "
                  f"{fmt_pct(best['dyn_total_return'] or 0):>10} "
                  f"{(best['dyn_sharpe'] or 0):>9.4f} "
                  f"{fmt_pct(best['max_drawdown'] or 0):>8}")
    sys.stdout.flush()

    print(f"\n{BOLD}-- 各策略全局最佳 --{RESET}")
    best_by_strat = {}
    for strat_key, strat_label in STRATEGIES:
        sub = [r for r in results if r["strategy"] == strat_key]
        if not sub:
            continue
        best = max(sub, key=lambda r: r["total_return"])
        best_by_strat[strat_key] = best
        print(f"  {strat_label}: Top-{best['top_n']} | 持有{best['hold_days']}日 | "
              f"固定收益 {fmt_pct(best['total_return'])} | "
              f"固定夏普 {(best['sharpe_ratio'] or 0):.4f} | "
              f"动态收益 {fmt_pct(best['dyn_total_return'] or 0)} | "
              f"动态夏普 {(best['dyn_sharpe'] or 0):.4f}")

    # ── 结论 ──
    print(f"\n\n{BOLD}{'='*100}{RESET}")
    print(f"{BOLD}  结 论{RESET}")
    print(f"{'='*100}")
    ra = best_by_strat.get("random")
    rc = best_by_strat.get("recent")
    if ra and rc:
        diff_ret = ra["total_return"] - rc["total_return"]
        diff_sharpe = (ra["sharpe_ratio"] or 0) - (rc["sharpe_ratio"] or 0)
        winner = "随机 5 窗口" if diff_ret > 0 else ("最近 1 窗口" if diff_ret < 0 else "平手")
        print(f"  收益差: {fmt_pct(diff_ret)} (随机 - 最近)")
        print(f"  夏普差: {diff_sharpe:+.4f}")
        print(f"  胜出: {GREEN}{winner}{RESET}" if diff_ret != 0 else f"  胜出: 平手")
        if diff_ret > 0:
            print(f"\n  随机窗口通过多窗口平均来抵抗过拟合，在样本外表现更好。")
        elif diff_ret < 0:
            print(f"\n  最近窗口对当前行情更敏感，在趋势延续的市场中表现更好。")
    else:
        print(f"  {RED}数据不足，无法比较{RESET}")
    sys.stdout.flush()


def main():
    t0 = time.time()
    results = run_comparison()

    if not results:
        print(f"{RED}No results!{RESET}")
        return

    out_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..",
        "reports_discovery", f"window_strategy_{date.today().strftime('%Y%m%d')}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存: {out_path}")

    print_comparison(results)

    print(f"\n{BOLD}总耗时: {time.time() - t0:.0f}s ({(time.time() - t0)/60:.1f}min){RESET}")


if __name__ == "__main__":
    main()
