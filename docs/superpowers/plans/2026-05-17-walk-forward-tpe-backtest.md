# Walk-Forward TPE 动态权重回测 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a toggle to the factor backtest page that enables walk-forward TPE dynamic weight re-optimization every 5 trading days, producing dual capital curves (fixed vs dynamic) for comparison.

**Architecture:** New `compute_walk_forward()` method on `FactorBacktestEngine` preloads all data once and iterates through snap dates. Each advance step runs an independent TPE study (pure memory, `storage=None`) on the preceding 60-day window, then evaluates the next 5 trading days with the optimized weights. The existing `compute()` fixed-weight path is untouched.

**Tech Stack:** Python/FastAPI backend, React/TypeScript frontend, Optuna TPE, Recharts

---

## File Structure

| File | Role |
|------|------|
| `src/discovery/factor_backtest_engine.py` | New `compute_walk_forward()` method |
| `src/discovery/factor_optimizer.py` | Add `preloaded` + `use_persistent_storage` params to `optimize()` and `_tpe_search()` |
| `api/v1/endpoints/discovery.py` | Add `reoptimize_interval` to `FactorBacktestRequest`, route to walk-forward |
| `apps/dsa-web/src/api/discovery.ts` | Add `reoptimize_interval` to request type, dynamic curve keys to response |
| `apps/dsa-web/src/pages/FactorBacktestPage.tsx` | Toggle switch, dual-line chart overlay, summary comparison table |

---

### Task 1: Add `preloaded` and `use_persistent_storage` params to FactorOptimizer

**Files:**
- Modify: `src/discovery/factor_optimizer.py:63-133` (optimize), `src/discovery/factor_optimizer.py:329-475` (_tpe_search)

- [ ] **Step 1: Add params to `optimize()` signature**

In `src/discovery/factor_optimizer.py`, change the `optimize()` method signature (line 63-65):

```python
def optimize(self, mode: str = "postmarket", window: int = 60,
             normalize: bool = False, n_trials: int = 100,
             auto_apply: bool = True,
             preloaded: Optional[Dict] = None,
             use_persistent_storage: bool = True) -> Optional[Dict]:
```

Pass them through to `_tpe_search()` on line 91-92:

```python
all_results, best, total_trials = self._tpe_search(
    candidates, current_weights, mode, window, n_trials, normalize,
    preloaded=preloaded, use_persistent_storage=use_persistent_storage)
```

- [ ] **Step 2: Add params to `_tpe_search()` and conditionally skip preload + use memory storage**

Change `_tpe_search()` signature (line 329-333):

```python
def _tpe_search(self, candidates: Dict[str, Dict],
                current_weights: Dict[str, float],
                mode: str, window: int,
                n_trials: int,
                normalize: bool = False,
                preloaded: Optional[Dict] = None,
                use_persistent_storage: bool = True) -> Tuple[List[Dict], Optional[Dict], int]:
```

Replace the preload block (lines 355-386). When `preloaded` is provided, use it; otherwise keep existing behavior:

```python
if preloaded:
    all_snap_dates = preloaded["snap_dates"]
    all_scores = preloaded["scores"]
    full_tdays = preloaded["trading_days"]
    _window_pool = preloaded["window_pool"]
    # price cache already populated by caller
else:
    # existing preload block (unchanged)
    self._notify("preload", message="加载历史快照日期…")
    all_snap_dates = self._get_recent_snap_dates(all_factor_names, mode, 9999)
    ...
    self._notify("preload", message="构建窗口池…")
    _window_pool: List[List[str]] = []
    ...
```

Replace study creation (line 347-353) to respect `use_persistent_storage`:

```python
if use_persistent_storage:
    _TPE_STORAGE.parent.mkdir(parents=True, exist_ok=True)
    storage_url = f"sqlite:///{_TPE_STORAGE}"
else:
    storage_url = None  # pure memory, no pollution

study = optuna.create_study(
    direction="maximize",
    storage=storage_url,
    study_name=study_name,
    load_if_exists=use_persistent_storage,
    sampler=optuna.samplers.TPESampler(seed=42),
)
```

- [ ] **Step 3: Verify compilation**

```bash
python -m py_compile src/discovery/factor_optimizer.py
```
Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add src/discovery/factor_optimizer.py
git commit -m "feat: add preloaded and use_persistent_storage params to FactorOptimizer for walk-forward TPE"
```

---

### Task 2: Add `compute_walk_forward()` to FactorBacktestEngine

**Files:**
- Modify: `src/discovery/factor_backtest_engine.py` (after `compute()`, around line 302)

- [ ] **Step 1: Add the `compute_walk_forward()` method**

Insert after the `compute()` method's return statement (after line 302). The method preloads all data once, runs fixed-weight baseline across all snap dates, then walks forward re-optimizing every `interval` trading days with independent TPE nodes.

The method signature and full implementation:

```python
def compute_walk_forward(
    self,
    mode="postmarket",
    factor_weights=None,
    start_date=None,
    end_date=None,
    top_n=5,
    hold_days=None,
    initial_capital=_DEFAULT_INITIAL_CAPITAL,
    risk_free_rate=_DEFAULT_RISK_FREE_RATE,
    use_pipeline=False,
    reoptimize_interval=5,
    progress_cb=None,
):
    """Walk-forward TPE 动态权重回测。

    每个 reoptimize_interval 个交易日，使用之前 60 日窗口运行独立 TPE
    优化权重，然后用优化后的权重评估接下来 interval 日的交易。
    返回双线资金曲线：固定权重（基线）+ 动态调优。
    """
    if hold_days is None:
        hold_days = [1, 3, 5, 10, 20]
    if factor_weights is None:
        factor_weights = self._get_default_weights(mode)
    if not factor_weights:
        return None

    snap_dates = self._get_available_dates(list(factor_weights.keys()), mode)
    if not snap_dates:
        available_factors = self._list_factors_with_data(mode)
        if not available_factors:
            return None
        factor_weights = {k: v for k, v in factor_weights.items() if k in available_factors}
        snap_dates = self._get_available_dates(list(factor_weights.keys()), mode)
    if not snap_dates:
        return None
    if start_date and start_date > snap_dates[-1]:
        return None
    if end_date and end_date < snap_dates[0]:
        return None

    sd = start_date if start_date and start_date >= snap_dates[0] else snap_dates[0]
    ed = end_date if end_date and end_date <= snap_dates[-1] else snap_dates[-1]
    snap_filtered = [d for d in snap_dates if sd <= d <= ed]
    if len(snap_filtered) < 1:
        return None

    trading_days = self._get_trading_days(snap_filtered)
    if len(trading_days) < 2:
        return None

    all_codes_set = set()
    scores_by_date = self._load_snapshots(
        list(factor_weights.keys()), mode, snap_filtered, progress_cb=progress_cb)
    raw_scores_by_date = {k: dict(v) for k, v in scores_by_date.items()}
    if use_pipeline:
        if progress_cb:
            progress_cb("管线融合计算中…")
        pool_n = 300
        for sdate in scores_by_date:
            sc = scores_by_date[sdate]
            if not sc:
                continue
            sc = self._decorrelate_scores(sc)
            sc = self._neutralize_scores(sc)
            multipliers = self._get_market_multipliers(sdate)
            adjusted_weights = self._apply_dynamic_weights(factor_weights, multipliers)
            comp = self._compute_composite(sc, adjusted_weights)
            if comp.empty:
                continue
            pool = comp.nlargest(pool_n).index.tolist()
            tech = self._batch_stockscorer(pool, sdate, trading_days, comp)
            blended = pd.Series(0.0, index=comp.index)
            for c in comp.index:
                blended[c] = 0.3 * comp.get(c, 0) + 0.7 * tech.get(c, 50.0)
            scores_by_date[sdate] = {'_pipeline': blended.dropna()}
    for ss in scores_by_date.values():
        for s in ss.values():
            all_codes_set.update(s.index.tolist() if hasattr(s, 'index') else s)
    self._prefetch_prices(list(all_codes_set), trading_days)
    self._prefetch_stock_names(list(all_codes_set))
    if progress_cb:
        progress_cb("预计算收益率矩阵…")
    fw_returns = self._precompute_forward_returns(
        all_codes_set, trading_days, snap_filtered, hold_days, mode)

    today_str = date.today().strftime("%Y%m%d")

    # ── 1. Fixed-weight baseline ──
    if progress_cb:
        progress_cb("固定权重回测中…")
    fixed_curves: Dict[str, List[Dict]] = {str(h): [] for h in hold_days}
    fixed_trades: Dict[int, List[FactorBacktestTrade]] = {h: [] for h in hold_days}
    cached_composites: Dict[str, pd.Series] = {}

    for hd in hold_days:
        cap = initial_capital
        curve = [{"date": snap_filtered[0], "capital": cap}]
        for snap_date in snap_filtered:
            if snap_date not in trading_days:
                continue
            ti = trading_days.index(snap_date)
            is_intra = mode == "intraday"
            buy_idx = ti if is_intra else ti + 1
            sell_idx = (ti + hd) if is_intra else (ti + 1 + hd)
            if buy_idx >= len(trading_days) or sell_idx >= len(trading_days):
                continue
            buy_date = trading_days[buy_idx]
            sell_date = trading_days[sell_idx]
            buy_field = "close" if is_intra else "open"
            sell_field = "close" if is_intra else "open"

            scores = scores_by_date.get(snap_date, {})
            if not scores:
                continue
            if use_pipeline:
                composite = scores.get("_pipeline", pd.Series())
            else:
                multipliers = self._get_market_multipliers(snap_date)
                adjusted = self._apply_dynamic_weights(factor_weights, multipliers)
                composite = self._compute_composite(scores, adjusted)
                cached_composites[snap_date] = composite
            if composite.empty:
                continue

            if buy_date > today_str:
                ranked = composite.nlargest(top_n)
                for code, _sc in ranked.items():
                    fixed_trades[hd].append(FactorBacktestTrade(
                        trade_date=snap_date, hold_days=hd, stock_code=code,
                        stock_name=self._stock_names.get(code, code),
                        buy_price=0, sell_date=sell_date, sell_price=0,
                        return_pct=0, pnl=0, allocated=0, status="pending"))
                continue

            ranked = composite.nlargest(top_n * 5)
            bought = []
            skipped = []
            for code, _sc in ranked.items():
                if len(bought) >= top_n:
                    break
                name = self._stock_names.get(code, code)
                if self._is_limit_up(code, buy_date):
                    skipped.append(code)
                    continue
                bp = self._get_price(code, buy_date, buy_field)
                sp = self._get_price(code, sell_date, sell_field)
                status = "closed"
                ext_date = sell_date
                if sp is None:
                    ext = self._find_next_td(sell_date, trading_days)
                    if ext:
                        ext_sp = self._get_price(code, ext, sell_field)
                        if ext_sp is not None:
                            sp = ext_sp
                            ext_date = ext
                            status = "extended"
                if bp and sp and bp > 0:
                    bought.append((code, name, bp, sp, ext_date, status))
                elif bp is None:
                    skipped.append(code)
                else:
                    bought.append((code, name, bp, 0, ext_date, "open"))
            n_bought = len(bought)
            if n_bought == 0:
                continue
            alloc = cap / n_bought / hd
            day_pnl = 0.0
            for code, name, bp, sp, sd_ext, status in bought:
                if bp and sp and bp > 0:
                    ret = (sp - bp) / bp
                    pnl = alloc * ret
                    day_pnl += pnl
                else:
                    ret = 0.0
                    pnl = 0.0
                fixed_trades[hd].append(FactorBacktestTrade(
                    trade_date=snap_date, hold_days=hd, stock_code=code, stock_name=name,
                    buy_price=round(bp, 2) if bp else 0, sell_date=sd_ext,
                    sell_price=round(sp, 2) if sp else 0, return_pct=round(ret, 6),
                    pnl=round(pnl, 2), allocated=round(alloc, 2), status=status))
            for code in skipped:
                fixed_trades[hd].append(FactorBacktestTrade(
                    trade_date=snap_date, hold_days=hd, stock_code=code,
                    stock_name=self._stock_names.get(code, code),
                    buy_price=0, sell_date=sell_date, sell_price=0,
                    return_pct=0, pnl=0, allocated=0, status="canceled"))
            cap += day_pnl
            if day_pnl != 0:
                curve.append({"date": snap_date, "capital": round(cap, 2)})
        fixed_curves[str(hd)] = curve

    # ── 2. Dynamic walk-forward TPE ──
    if progress_cb:
        progress_cb("动态调优回测中…")
    dynamic_curves: Dict[str, List[Dict]] = {str(h): [] for h in hold_days}
    dynamic_trades: Dict[int, List[FactorBacktestTrade]] = {h: [] for h in hold_days}

    from src.discovery.factor_optimizer import FactorOptimizer

    window_size = 60
    node_end_indices = list(range(window_size - 1, len(snap_filtered), reoptimize_interval))
    nodes_evaluated = 0

    for hd in hold_days:
        cap = initial_capital
        curve = [{"date": snap_filtered[0], "capital": cap}]

        for node_idx in node_end_indices:
            opt_start = max(0, node_idx - window_size + 1)
            opt_window_dates = snap_filtered[opt_start:node_idx + 1]
            if len(opt_window_dates) < 20:
                continue

            eval_start = node_idx + 1
            eval_end = min(node_idx + reoptimize_interval, len(snap_filtered))
            eval_dates = snap_filtered[eval_start:eval_end]
            if not eval_dates:
                continue

            if progress_cb:
                nodes_evaluated += 1
                progress_cb(
                    f"动态调优节点 {nodes_evaluated} "
                    f"({opt_window_dates[0]}..{opt_window_dates[-1]}"
                    f" → {eval_dates[0]}..{eval_dates[-1]})…")

            # Build preloaded data for this node's optimization window
            opt_tdays = self._get_trading_days(opt_window_dates)
            opt_scores = {d: scores_by_date[d] for d in opt_window_dates
                         if d in scores_by_date}

            w_pool = []
            for i in range(window_size - 1, len(opt_window_dates), 5):
                seg = opt_window_dates[max(0, i - window_size + 1):i + 1]
                if len(seg) >= 20:
                    w_pool.append(seg)
            if not w_pool and len(opt_window_dates) >= 20:
                w_pool = [opt_window_dates]

            preloaded = {
                "snap_dates": opt_window_dates,
                "scores": opt_scores,
                "trading_days": opt_tdays,
                "window_pool": w_pool,
            }

            optimizer = FactorOptimizer(tushare_fetcher=self._fetcher)
            optimizer._engine._price_cache = self._price_cache
            optimizer._engine._stock_names = self._stock_names

            opt_result = optimizer.optimize(
                mode=mode, window=window_size, normalize=False,
                n_trials=min(30, max(10, len(opt_window_dates) // 2)),
                auto_apply=False,
                preloaded=preloaded,
                use_persistent_storage=False,
            )

            if opt_result and opt_result.get("recommendation"):
                dyn_weights = {**factor_weights}
                for fn, w in opt_result["recommendation"].items():
                    if fn in dyn_weights:
                        dyn_weights[fn] = w
            else:
                dyn_weights = dict(factor_weights)

            # Evaluate this node's eval_dates with dyn_weights
            for snap_date in eval_dates:
                if snap_date not in trading_days:
                    continue
                ti = trading_days.index(snap_date)
                is_intra = mode == "intraday"
                buy_idx = ti if is_intra else ti + 1
                sell_idx = (ti + hd) if is_intra else (ti + 1 + hd)
                if buy_idx >= len(trading_days) or sell_idx >= len(trading_days):
                    continue
                buy_date = trading_days[buy_idx]
                sell_date = trading_days[sell_idx]
                buy_field = "close" if is_intra else "open"
                sell_field = "close" if is_intra else "open"

                scores = scores_by_date.get(snap_date, {})
                if not scores:
                    continue
                if use_pipeline:
                    composite = scores.get("_pipeline", pd.Series())
                else:
                    multipliers = self._get_market_multipliers(snap_date)
                    adjusted = self._apply_dynamic_weights(dyn_weights, multipliers)
                    composite = self._compute_composite(scores, adjusted)
                if composite.empty:
                    continue

                if buy_date > today_str:
                    ranked = composite.nlargest(top_n)
                    for code, _sc in ranked.items():
                        dynamic_trades[hd].append(FactorBacktestTrade(
                            trade_date=snap_date, hold_days=hd, stock_code=code,
                            stock_name=self._stock_names.get(code, code),
                            buy_price=0, sell_date=sell_date, sell_price=0,
                            return_pct=0, pnl=0, allocated=0, status="pending"))
                    continue

                ranked = composite.nlargest(top_n * 5)
                bought = []
                skipped = []
                for code, _sc in ranked.items():
                    if len(bought) >= top_n:
                        break
                    name = self._stock_names.get(code, code)
                    if self._is_limit_up(code, buy_date):
                        skipped.append(code)
                        continue
                    bp = self._get_price(code, buy_date, buy_field)
                    sp = self._get_price(code, sell_date, sell_field)
                    status = "closed"
                    ext_date = sell_date
                    if sp is None:
                        ext = self._find_next_td(sell_date, trading_days)
                        if ext:
                            ext_sp = self._get_price(code, ext, sell_field)
                            if ext_sp is not None:
                                sp = ext_sp
                                ext_date = ext
                                status = "extended"
                    if bp and sp and bp > 0:
                        bought.append((code, name, bp, sp, ext_date, status))
                    elif bp is None:
                        skipped.append(code)
                    else:
                        bought.append((code, name, bp, 0, ext_date, "open"))
                n_bought = len(bought)
                if n_bought == 0:
                    continue
                alloc = cap / n_bought / hd
                day_pnl = 0.0
                for code, name, bp, sp, sd_ext, status in bought:
                    if bp and sp and bp > 0:
                        ret = (sp - bp) / bp
                        pnl = alloc * ret
                        day_pnl += pnl
                    else:
                        ret = 0.0
                        pnl = 0.0
                    dynamic_trades[hd].append(FactorBacktestTrade(
                        trade_date=snap_date, hold_days=hd, stock_code=code,
                        stock_name=name, buy_price=round(bp, 2) if bp else 0,
                        sell_date=sd_ext, sell_price=round(sp, 2) if sp else 0,
                        return_pct=round(ret, 6), pnl=round(pnl, 2),
                        allocated=round(alloc, 2), status=status))
                for code in skipped:
                    dynamic_trades[hd].append(FactorBacktestTrade(
                        trade_date=snap_date, hold_days=hd, stock_code=code,
                        stock_name=self._stock_names.get(code, code),
                        buy_price=0, sell_date=sell_date, sell_price=0,
                        return_pct=0, pnl=0, allocated=0, status="canceled"))
                cap += day_pnl
                if day_pnl != 0:
                    curve.append({"date": snap_date, "capital": round(cap, 2)})
        dynamic_curves[str(hd)] = curve

    # ── 3. Build result with dual curves ──
    capital_curves: Dict[str, List[Dict]] = {}
    for hd in hold_days:
        capital_curves[f"{hd}_fixed"] = fixed_curves[str(hd)]
        capital_curves[f"{hd}_dynamic"] = dynamic_curves[str(hd)]

    # Summary for both paths
    phd = min(hold_days)
    fcurve = fixed_curves[str(phd)]
    dcurve = dynamic_curves[str(phd)]
    fc_fixed = fcurve[-1]["capital"] if fcurve else initial_capital
    fc_dynamic = dcurve[-1]["capital"] if dcurve else initial_capital
    cr_fixed = (fc_fixed - initial_capital) / initial_capital
    cr_dynamic = (fc_dynamic - initial_capital) / initial_capital

    td = (datetime.strptime(ed, "%Y%m%d") - datetime.strptime(sd, "%Y%m%d")).days
    ar_fixed = (math.exp(math.log1p(cr_fixed) * 365 / max(td, 1)) - 1) if cr_fixed > -1 else cr_fixed
    ar_dynamic = (math.exp(math.log1p(cr_dynamic) * 365 / max(td, 1)) - 1) if cr_dynamic > -1 else cr_dynamic

    mdd_fixed, mf_start, mf_end = self._calc_mdd(fcurve, initial_capital)
    mdd_dynamic, md_start, md_end = self._calc_mdd(dcurve, initial_capital)

    drs_fixed = []
    pc = initial_capital
    for pt in fcurve[1:]:
        drs_fixed.append((pt["capital"] - pc) / pc)
        pc = pt["capital"]
    sh_fixed = self._calc_sharpe(drs_fixed, risk_free_rate) if drs_fixed else 0

    drs_dynamic = []
    pc = initial_capital
    for pt in dcurve[1:]:
        drs_dynamic.append((pt["capital"] - pc) / pc)
        pc = pt["capital"]
    sh_dynamic = self._calc_sharpe(drs_dynamic, risk_free_rate) if drs_dynamic else 0

    ft_closed = [t for t in fixed_trades[phd] if t.status in ("closed", "extended")]
    dt_closed = [t for t in dynamic_trades[phd] if t.status in ("closed", "extended")]
    wr_fixed = sum(1 for t in ft_closed if t.return_pct > 0) / len(ft_closed) if ft_closed else 0
    wr_dynamic = sum(1 for t in dt_closed if t.return_pct > 0) / len(dt_closed) if dt_closed else 0

    self._price_cache.clear()

    if progress_cb:
        progress_cb("计算 IC / 分位数中…")
    ric: Dict[str, Dict[str, float]] = {}
    for hd in hold_days:
        ric[str(hd)] = self._calc_rank_ic(raw_scores_by_date, hd, trading_days, mode, fw_returns)
    qr = {}
    for hd in hold_days:
        qr[str(hd)] = self._calc_quantile(scores_by_date, factor_weights, hd, trading_days, mode,
                                          fw_returns, cached_composites)

    finfo = []
    for fn, fw in factor_weights.items():
        fd = self._get_factor_date_range(fn, mode)
        finfo.append({"name": fn, "weight": fw,
                      "available_from": fd[0] if fd else "",
                      "available_to": fd[1] if fd else ""})

    tds = []
    for hd in hold_days:
        for t in fixed_trades[hd]:
            tds.append({"trade_date": t.trade_date, "hold_days": t.hold_days,
                       "stock_code": t.stock_code, "stock_name": t.stock_name,
                       "buy_price": t.buy_price, "sell_date": t.sell_date,
                       "sell_price": t.sell_price, "return_pct": t.return_pct,
                       "pnl": t.pnl, "allocated": t.allocated, "status": t.status,
                       "reoptimized": False})
        for t in dynamic_trades[hd]:
            tds.append({"trade_date": t.trade_date, "hold_days": t.hold_days,
                       "stock_code": t.stock_code, "stock_name": t.stock_name,
                       "buy_price": t.buy_price, "sell_date": t.sell_date,
                       "sell_price": t.sell_price, "return_pct": t.return_pct,
                       "pnl": t.pnl, "allocated": t.allocated, "status": t.status,
                       "reoptimized": True})

    return FactorBacktestResult(
        mode=mode, date_range={"start": sd, "end": ed}, factors=finfo,
        params={"top_n": top_n, "hold_days": hold_days,
                "initial_capital": initial_capital, "risk_free_rate": risk_free_rate,
                "use_pipeline": use_pipeline,
                "reoptimize_interval": reoptimize_interval},
        summary={"cumulative_return": round(cr_fixed, 4),
                 "annualized_return": round(ar_fixed, 4),
                 "win_rate": round(wr_fixed, 4),
                 "max_drawdown": round(mdd_fixed, 4),
                 "max_drawdown_start": mf_start,
                 "max_drawdown_end": mf_end,
                 "sharpe_ratio": round(sh_fixed, 4),
                 "total_trades": len(ft_closed),
                 "total_periods": len(snap_filtered),
                 "final_capital": round(fc_fixed, 2),
                 "dynamic": {
                     "cumulative_return": round(cr_dynamic, 4),
                     "annualized_return": round(ar_dynamic, 4),
                     "win_rate": round(wr_dynamic, 4),
                     "max_drawdown": round(mdd_dynamic, 4),
                     "max_drawdown_start": md_start,
                     "max_drawdown_end": md_end,
                     "sharpe_ratio": round(sh_dynamic, 4),
                     "total_trades": len(dt_closed),
                     "final_capital": round(fc_dynamic, 2),
                     "nodes_evaluated": nodes_evaluated,
                 }},
        capital_curves=capital_curves,
        rank_ic=ric,
        quantile_returns=qr,
        trade_records=tds,
    )
```

- [ ] **Step 2: Verify compilation**

```bash
python -m py_compile src/discovery/factor_backtest_engine.py
```
Expected: no output (success).

- [ ] **Step 3: Import smoke test**

```bash
python -c "from src.discovery.factor_backtest_engine import FactorBacktestEngine; print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add src/discovery/factor_backtest_engine.py
git commit -m "feat: add compute_walk_forward() for dynamic weight TPE backtest"
```

---

### Task 3: Update API endpoint to support reoptimize_interval

**Files:**
- Modify: `api/v1/endpoints/discovery.py:1939-2034`

- [ ] **Step 1: Add `reoptimize_interval` to request model**

```python
class FactorBacktestRequest(BaseModel):
    mode: str = "postmarket"
    factor_weights: Dict[str, float] = {}
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    top_n: int = 5
    hold_days: List[int] = [1, 3, 5, 10, 20]
    initial_capital: float = 1_000_000.0
    risk_free_rate: float = 0.02
    use_pipeline: bool = False
    reoptimize_interval: Optional[int] = None  # None=固定权重, 5=每5日TPE调优
```

- [ ] **Step 2: Route to `compute_walk_forward` when `reoptimize_interval` is set**

In the `_run()` inner function of `factor_backtest()`, replace the `engine.compute(...)` call:

```python
if req.reoptimize_interval and req.reoptimize_interval > 0:
    result = engine.compute_walk_forward(
        mode=req.mode,
        factor_weights=fw,
        start_date=req.start_date,
        end_date=req.end_date,
        top_n=req.top_n,
        hold_days=req.hold_days,
        initial_capital=req.initial_capital,
        risk_free_rate=req.risk_free_rate,
        use_pipeline=req.use_pipeline,
        reoptimize_interval=req.reoptimize_interval,
        progress_cb=_progress,
    )
else:
    result = engine.compute(
        mode=req.mode,
        factor_weights=fw,
        start_date=req.start_date,
        end_date=req.end_date,
        top_n=req.top_n,
        hold_days=req.hold_days,
        initial_capital=req.initial_capital,
        risk_free_rate=req.risk_free_rate,
        use_pipeline=req.use_pipeline,
        progress_cb=_progress,
    )
```

- [ ] **Step 3: Verify compilation**

```bash
python -m py_compile api/v1/endpoints/discovery.py
```
Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add api/v1/endpoints/discovery.py
git commit -m "feat: add reoptimize_interval to factor backtest API for walk-forward TPE"
```

---

### Task 4: Update frontend API types

**Files:**
- Modify: `apps/dsa-web/src/api/discovery.ts:201-267`

- [ ] **Step 1: Add `reoptimize_interval` to request type**

```typescript
export type FactorBacktestRequest = {
  mode: 'intraday' | 'postmarket';
  factor_weights?: Record<string, number>;
  start_date?: string;
  end_date?: string;
  top_n?: number;
  hold_days?: number[];
  initial_capital?: number;
  risk_free_rate?: number;
  use_pipeline?: boolean;
  reoptimize_interval?: number | null;
};
```

- [ ] **Step 2: Add `dynamic` sub-object to summary type and `reoptimize_interval` to params**

```typescript
export type FactorBacktestResultResponse = {
  mode: string;
  date_range: { start: string; end: string };
  factors: FactorBacktestFactorInfo[];
  params: {
    top_n: number;
    hold_days: number[];
    initial_capital: number;
    risk_free_rate: number;
    use_pipeline: boolean;
    reoptimize_interval?: number | null;
  };
  summary: {
    cumulative_return: number;
    annualized_return: number;
    win_rate: number;
    max_drawdown: number;
    max_drawdown_start?: string;
    max_drawdown_end?: string;
    sharpe_ratio: number;
    total_trades: number;
    total_periods: number;
    final_capital: number;
    dynamic?: {
      cumulative_return: number;
      annualized_return: number;
      win_rate: number;
      max_drawdown: number;
      max_drawdown_start?: string;
      max_drawdown_end?: string;
      sharpe_ratio: number;
      total_trades: number;
      final_capital: number;
      nodes_evaluated: number;
    };
  };
  capital_curves: Record<string, FactorBacktestCapitalPoint[]>;
  rank_ic: Record<string, Record<string, number>>;
  quantile_returns: Record<string, {
    top_10pct: number;
    top_20pct: number;
    top_50pct: number;
  }>;
  trade_records: FactorBacktestTrade[];
};
```

- [ ] **Step 3: Verify TypeScript compilation**

```bash
cd apps/dsa-web && npx tsc --noEmit
```
Expected: no new errors from this file.

- [ ] **Step 4: Commit**

```bash
git add apps/dsa-web/src/api/discovery.ts
git commit -m "feat: add walk-forward types to frontend API layer"
```

---

### Task 5: Add toggle switch and dual curve display to FactorBacktestPage

**Files:**
- Modify: `apps/dsa-web/src/pages/FactorBacktestPage.tsx`

- [ ] **Step 1: Add state for toggle and update handleRun**

Add Switch import at line 8:

```typescript
import { DatePicker, Segmented, Table, InputNumber, Checkbox, Switch } from 'antd';
```

Add state variable after existing param states (after line 68 `const [usePipeline, setUsePipeline] = useState(true);`):

```typescript
const [reoptimize, setReoptimize] = useState(false);
```

In `handleRun`, pass `reoptimize_interval` to the API call. Update the `runFactorBacktest` call arguments (around line 288-298):

```typescript
const { task_id } = await discoveryApi.runFactorBacktest({
  mode,
  factor_weights: fw,
  start_date: startDate,
  end_date: endDate,
  top_n: topN,
  hold_days: holdDays,
  initial_capital: initialCapital,
  risk_free_rate: riskFreeRate / 100,
  use_pipeline: usePipeline,
  reoptimize_interval: reoptimize ? 5 : null,
});
```

- [ ] **Step 2: Add toggle switch in parameters section**

In the JSX, add after the pipeline checkbox row. Find the `usePipeline` checkbox JSX and add after it:

```tsx
<div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 12 }}>
  <span style={{ fontSize: 13, color: '#64748b' }}>动态调优 (Walk-Forward TPE)</span>
  <Switch
    checked={reoptimize}
    onChange={setReoptimize}
    size="small"
  />
  {reoptimize && (
    <span style={{ fontSize: 11, color: '#f59e0b' }}>
      每 5 个交易日 TPE 调优权重（内存 study，不污染生产数据）
    </span>
  )}
</div>
```

- [ ] **Step 3: Update chart rendering for dual curves with dashed/solid distinction**

When `reoptimize` was used, curve keys are `"1_fixed"`, `"1_dynamic"`, `"5_fixed"`, `"5_dynamic"` instead of `"1"`, `"5"`.

In the chart JSX, find the `<LineChart>` section and update the Line rendering. The key change: fixed curves use dashed lines, dynamic curves use solid lines:

```tsx
{Object.keys(result.capital_curves)
  .filter(k => selectedCurves[k])
  .map((k, i) => {
    const isDynamic = k.endsWith('_dynamic');
    const isFixed = k.endsWith('_fixed');
    let label: string;
    if (isDynamic) {
      label = k.replace('_dynamic', '日 动态');
    } else if (isFixed) {
      label = k.replace('_fixed', '日 固定');
    } else {
      label = `${k}日`;
    }
    return (
      <Line
        key={k}
        type="monotone"
        dataKey={`h${k}`}
        name={label}
        stroke={CAPITAL_COLORS[i % CAPITAL_COLORS.length]}
        strokeWidth={2}
        strokeDasharray={isFixed ? '5 5' : undefined}
        dot={false}
        connectNulls
      />
    );
  })}
```

- [ ] **Step 4: Add dual-column summary comparison table when dynamic mode active**

After the existing summary `StatCard` section, add a comparison table when `result.summary.dynamic` is present:

```tsx
{result.summary.dynamic && (
  <Card title="固定权重 vs 动态调优 对比">
    <Table
      pagination={false}
      size="small"
      dataSource={[
        { metric: '年化收益', fixed: pct(result.summary.annualized_return), dynamic: pct(result.summary.dynamic.annualized_return) },
        { metric: '累计收益', fixed: pct(result.summary.cumulative_return), dynamic: pct(result.summary.dynamic.cumulative_return) },
        { metric: '最大回撤', fixed: pct(result.summary.max_drawdown), dynamic: pct(result.summary.dynamic.max_drawdown) },
        { metric: '夏普比率', fixed: result.summary.sharpe_ratio.toFixed(2), dynamic: result.summary.dynamic.sharpe_ratio.toFixed(2) },
        { metric: '胜率', fixed: pct(result.summary.win_rate), dynamic: pct(result.summary.dynamic.win_rate) },
        { metric: '最终资金', fixed: fmtMoney(result.summary.final_capital), dynamic: fmtMoney(result.summary.dynamic.final_capital) },
        { metric: '已平仓交易', fixed: result.summary.total_trades, dynamic: result.summary.dynamic.total_trades },
        { metric: 'TPE 节点数', fixed: '-', dynamic: result.summary.dynamic.nodes_evaluated },
      ]}
      columns={[
        { title: '指标', dataIndex: 'metric', key: 'metric' },
        { title: '固定权重', dataIndex: 'fixed', key: 'fixed' },
        { title: '动态调优', dataIndex: 'dynamic', key: 'dynamic' },
      ]}
    />
  </Card>
)}
```

- [ ] **Step 5: Verify TypeScript build**

```bash
cd apps/dsa-web && npm run build
```
Expected: build succeeds.

- [ ] **Step 6: Commit**

```bash
git add apps/dsa-web/src/pages/FactorBacktestPage.tsx
git commit -m "feat: add walk-forward TPE toggle and dual curve comparison to backtest page"
```

---

### Task 6: Integration smoke test

- [ ] **Step 1: Verify all Python files compile**

```bash
python -m py_compile src/discovery/factor_backtest_engine.py src/discovery/factor_optimizer.py api/v1/endpoints/discovery.py
```
Expected: no output.

- [ ] **Step 2: Verify import chain**

```bash
python -c "
from src.discovery.factor_backtest_engine import FactorBacktestEngine
from src.discovery.factor_optimizer import FactorOptimizer
print('All imports OK')
"
```
Expected: `All imports OK`

- [ ] **Step 3: Verify frontend build**

```bash
cd apps/dsa-web && npm run build
```
Expected: build succeeds.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: integration smoke test pass for walk-forward TPE feature"
```

---

## Verification Checklist

After all tasks complete:

1. Start the dev server: `uvicorn server:app --reload`
2. Open the factor backtest page in browser
3. Verify the "动态调优" toggle appears and is off by default
4. Run a normal backtest (toggle off) — confirm results match existing behavior
5. Enable toggle, run backtest — confirm:
   - Progress messages show dynamic TPE node evaluation
   - Response includes both `"_fixed"` and `"_dynamic"` curve keys
   - Chart shows dashed fixed lines + solid dynamic lines
   - Summary comparison table appears below
6. Verify `optuna_cache/factor_opt.db` has NO new trials from walk-forward run
7. Verify FactorTuningPage still works normally with its persistent study
