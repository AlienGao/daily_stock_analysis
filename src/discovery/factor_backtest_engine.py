# -*- coding: utf-8 -*-
"""因子回测引擎 (Factor Backtest Engine).

从 factor_score_snapshots 表读取历史因子得分，执行单因子评估和多因子组合回测。

统一替代：
- FactorMonitor (因子绩效追踪)
- ICTracker (IC 计算)
"""

import logging
import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)

_OPTUNA_DB = Path(__file__).resolve().parent.parent.parent / "optuna_cache" / "factor_opt.db"


def _optuna_study_has_trials(study_name: str) -> bool:
    """检查 Optuna study 是否已有完成的 trial。"""
    try:
        conn = sqlite3.connect(str(_OPTUNA_DB))
        cur = conn.cursor()
        cur.execute("SELECT study_id FROM studies WHERE study_name = ?", (study_name,))
        row = cur.fetchone()
        if not row:
            conn.close()
            return False
        study_id = row[0]
        cur.execute(
            "SELECT COUNT(*) FROM trials WHERE study_id = ? AND state = 'COMPLETE'",
            (study_id,))
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def _load_optuna_best_weights(study_name: str, fallback_weights: Dict[str, float]) -> Dict[str, float]:
    """从 Optuna 缓存加载指定 study 的最优因子权重。"""
    try:
        conn = sqlite3.connect(str(_OPTUNA_DB))
        cur = conn.cursor()
        cur.execute("SELECT study_id FROM studies WHERE study_name = ?", (study_name,))
        row = cur.fetchone()
        if not row:
            logger.info("Optuna study '%s' 未找到，使用默认权重", study_name)
            return dict(fallback_weights)
        study_id = row[0]
        cur.execute("""
            SELECT t.trial_id FROM trials t
            JOIN trial_values tv ON t.trial_id = tv.trial_id
            WHERE t.study_id = ? ORDER BY tv.value DESC LIMIT 1
        """, (study_id,))
        row = cur.fetchone()
        if not row:
            logger.info("study '%s' 无已完成 trial，使用默认权重", study_name)
            return dict(fallback_weights)
        trial_id = row[0]
        cur.execute("SELECT param_name, param_value FROM trial_params WHERE trial_id = ?", (trial_id,))
        opt_weights = {name: value for name, value in cur.fetchall()}
        conn.close()
        result = {**fallback_weights}
        for fn, w in opt_weights.items():
            if fn in result:
                result[fn] = w
        logger.info("从 Optuna 缓存加载最优权重 study='%s': %s", study_name, opt_weights)
        return result
    except Exception:
        logger.exception("加载 Optuna 缓存权重失败，使用默认权重")
        return dict(fallback_weights)

_DEFAULT_INITIAL_CAPITAL = 1_000_000.0
_DEFAULT_RISK_FREE_RATE = 0.02


@dataclass
class FactorBacktestTrade:
    trade_date: str
    hold_days: int
    stock_code: str
    stock_name: str
    buy_price: float
    sell_date: str
    sell_price: float
    return_pct: float
    pnl: float
    allocated: float
    status: str
    reoptimized: bool = False


@dataclass
class FactorBacktestResult:
    mode: str
    date_range: Dict[str, str]
    factors: List[Dict]
    params: Dict
    summary: Dict
    capital_curves: Dict[str, List[Dict]]
    rank_ic: Dict[str, Dict[str, float]]
    quantile_returns: Dict[str, Dict[str, float]]
    trade_records: List[Dict]
    benchmark_curve: List[Dict] = None  # 上证指数归一化曲线

    def __post_init__(self):
        if self.benchmark_curve is None:
            self.benchmark_curve = []


class FactorBacktestEngine:

    def __init__(self, tushare_fetcher=None):
        self._fetcher = tushare_fetcher
        self._price_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        self._sector_cache: Dict[str, str] = {}
        self._stock_names: Dict[str, str] = {}
        self._market_mult_cache: Dict[str, Dict[str, float]] = {}

    def compute(
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
        score_blend_alpha=0.3,
        progress_cb: Optional[Callable[[str], None]] = None,
    ):
        if hold_days is None:
            hold_days = [1, 3, 5, 10, 20]
        if factor_weights is None:
            factor_weights = self._get_default_weights(mode)
        if not factor_weights:
            return None

        snap_dates = self._get_available_dates(list(factor_weights.keys()), mode)
        if not snap_dates:
            # 有因子缺数据时，放宽要求：用有数据的因子即可
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

        scores_by_date = self._load_snapshots(list(factor_weights.keys()), mode, snap_filtered,
                                             progress_cb=progress_cb)
        self._prefetch_market_multipliers(snap_filtered)
        # 保存原始单因子分用于 IC 计算（管线模式会替换为 _pipeline 综合分）
        raw_scores_by_date = {k: dict(v) for k, v in scores_by_date.items()}
        if use_pipeline:
            if progress_cb:
                progress_cb("管线融合计算中…")
            # StockScorer 融合：去相关 → 行业中性化 → 加权 composite → Top300 → StockScorer → 30/70 blend
            pool_n = 300
            for sdate in scores_by_date:
                sc = scores_by_date[sdate]
                if not sc: continue
                sc = self._decorrelate_scores(sc)
                sc = self._neutralize_scores(sc)
                multipliers = self._get_market_multipliers(sdate)
                adjusted_weights = self._apply_dynamic_weights(factor_weights, multipliers)
                comp = self._compute_composite(sc, adjusted_weights)
                if comp.empty: continue
                pool = comp.nlargest(pool_n).index.tolist()
                tech = self._batch_stockscorer(pool, sdate, trading_days, comp)
                blended = pd.Series(0.0, index=comp.index)
                for c in comp.index:
                    blended[c] = score_blend_alpha * comp.get(c, 0) + (1 - score_blend_alpha) * tech.get(c, 50.0)
                scores_by_date[sdate] = {'_pipeline': blended.dropna()}
        all_codes = set()
        for ss in scores_by_date.values():
            for s in ss.values():
                all_codes.update(s.index.tolist() if hasattr(s, 'index') else s)
        self._prefetch_prices(list(all_codes), trading_days)
        self._prefetch_stock_names(list(all_codes))
        if progress_cb:
            progress_cb("预计算收益率矩阵…")
        fw_returns = self._precompute_forward_returns(all_codes, trading_days, snap_filtered, hold_days, mode)

        # 预计算所有 snap_date 的综合得分（所有 hold_days 共用，避免重复计算）
        cached_composites: Dict[str, pd.Series] = {}
        for sdate in snap_filtered:
            ss = scores_by_date.get(sdate, {})
            if not ss:
                continue
            if use_pipeline:
                cached_composites[sdate] = ss.get("_pipeline", pd.Series())
            else:
                multipliers = self._get_market_multipliers(sdate)
                adjusted = self._apply_dynamic_weights(factor_weights, multipliers)
                cached_composites[sdate] = self._compute_composite(ss, adjusted)

        today_str = date.today().strftime("%Y%m%d")
        capital_curves = {str(h): [] for h in hold_days}
        all_trades = {h: [] for h in hold_days}

        for hd_i, hd in enumerate(hold_days):
            if progress_cb:
                progress_cb(f"回测计算中 (持有期 {hd_i + 1}/{len(hold_days)})…")
            cap = initial_capital
            curve = [{"date": snap_filtered[0], "capital": cap}]

            for snap_date in snap_filtered:
                if snap_date not in trading_days:
                    continue
                ti = trading_days.index(snap_date)
                # 盘后: T→T+1开盘买→T+1+N开盘卖 | 盘中: T→T日收盘买→T+N收盘卖
                is_intra = mode == "intraday"
                buy_idx = ti if is_intra else ti + 1
                sell_idx = (ti + hd) if is_intra else (ti + 1 + hd)
                if buy_idx >= len(trading_days) or sell_idx >= len(trading_days):
                    continue
                buy_date = trading_days[buy_idx]
                sell_date = trading_days[sell_idx]
                buy_field = "close" if is_intra else "open"
                sell_field = "close" if is_intra else "open"

                composite = cached_composites.get(snap_date, pd.Series())
                if composite.empty:
                    continue

                if buy_date > today_str:
                    # 买入日未到：返回选股结果但不执行交易
                    ranked = composite.nlargest(top_n)
                    for code, _sc in ranked.items():
                        all_trades[hd].append(FactorBacktestTrade(
                            trade_date=snap_date, hold_days=hd, stock_code=code,
                            stock_name=self._stock_names.get(code, code),
                            buy_price=0, sell_date=sell_date, sell_price=0,
                            return_pct=0, pnl=0, allocated=0, status="pending"))
                    continue
                # 两轮：先试买（取消的跳过顺延），后均分资金
                ranked = composite.nlargest(top_n * 5)
                bought = []
                skipped = []
                for code, _sc in ranked.items():
                    if len(bought) >= top_n: break
                    name = self._stock_names.get(code, code)
                    if self._is_limit_up(code, buy_date, buy_field):
                        skipped.append(code); continue
                    bp = self._get_price(code, buy_date, buy_field)
                    sp, ext_date, status = self._resolve_sell_price(
                        code, sell_date, sell_field, trading_days)
                    if bp and sp and bp > 0 and status not in ("locked", "open"):
                        bought.append((code, name, bp, sp, ext_date, status))
                    elif bp is None:
                        skipped.append(code)
                    else:
                        bought.append((code, name, bp, sp if sp else 0, ext_date, status))
                n_bought = len(bought)
                if n_bought == 0: continue
                alloc = cap / n_bought / hd
                day_pnl = 0.0
                for code, name, bp, sp, sd, status in bought:
                    if bp and sp and bp > 0:
                        ret = (sp - bp) / bp
                        pnl = alloc * ret; day_pnl += pnl
                    else:
                        ret = 0.0; pnl = 0.0
                    all_trades[hd].append(FactorBacktestTrade(
                        trade_date=snap_date, hold_days=hd, stock_code=code, stock_name=name,
                        buy_price=round(bp, 2) if bp else 0, sell_date=sd,
                        sell_price=round(sp, 2) if sp else 0, return_pct=round(ret, 6),
                        pnl=round(pnl, 2), allocated=round(alloc, 2), status=status))
                for code in skipped:
                    all_trades[hd].append(FactorBacktestTrade(
                        trade_date=snap_date, hold_days=hd, stock_code=code,
                        stock_name=self._stock_names.get(code, code),
                        buy_price=0, sell_date=sell_date, sell_price=0,
                        return_pct=0, pnl=0, allocated=0, status="canceled"))
                
                    

                cap += day_pnl
                # 仅在有实际盈亏时记录曲线点（跳过纯未平仓日期）
                if day_pnl != 0:
                    curve.append({"date": snap_date, "capital": round(cap, 2)})
            capital_curves[str(hd)] = curve

        phd = min(hold_days)  # 优先用最短持有期
        ptrades = all_trades[phd]
        closed = [t for t in ptrades if t.status in ("closed", "extended")]
        # 摘要只用已平仓交易
        pcurve = capital_curves[str(phd)]
        fc = pcurve[-1]["capital"] if pcurve else initial_capital
        cr = (fc - initial_capital) / initial_capital
        wins = sum(1 for t in closed if t.return_pct > 0)
        wr = wins / len(closed) if closed else 0
        mdd, mdd_start, mdd_end = self._calc_mdd(pcurve, initial_capital)
        if cr > -1:
            log_ar = math.log1p(cr) * 252 / max(len(snap_filtered), 1)
            ar = math.exp(log_ar) - 1 if log_ar < 700 else None
        else:
            ar = cr
        drs = []
        pc = initial_capital
        for pt in pcurve[1:]:
            drs.append((pt["capital"] - pc) / pc)
            pc = pt["capital"]
        sh = self._calc_sharpe(drs, risk_free_rate) if drs else 0
        # 主循环已结束，价格缓存不再需要（IC/quantile 用预计算的 fw_returns）
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
            for t in all_trades[hd]:
                tds.append({"trade_date": t.trade_date, "hold_days": t.hold_days,
                           "stock_code": t.stock_code, "stock_name": t.stock_name,
                           "buy_price": t.buy_price, "sell_date": t.sell_date,
                           "sell_price": t.sell_price, "return_pct": t.return_pct,
                           "pnl": t.pnl, "allocated": t.allocated, "status": t.status})

        benchmark_curve = self._build_benchmark_curve(trading_days, initial_capital)

        return FactorBacktestResult(
            mode=mode, date_range={"start": sd, "end": ed}, factors=finfo,
            params={"top_n": top_n, "hold_days": hold_days,
                    "initial_capital": initial_capital, "risk_free_rate": risk_free_rate,
                    "use_pipeline": use_pipeline},
            summary={"cumulative_return": round(cr, 4),
                     "annualized_return": round(ar, 4) if ar is not None else None,
                     "win_rate": round(wr, 4), "max_drawdown": round(mdd, 4),
                     "max_drawdown_start": mdd_start, "max_drawdown_end": mdd_end,
                     "sharpe_ratio": round(sh, 4), "total_trades": len(closed),
                     "total_periods": len(snap_filtered), "final_capital": round(fc, 2)},
            capital_curves=capital_curves, rank_ic=ric, quantile_returns=qr,
            trade_records=tds, benchmark_curve=benchmark_curve)

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
        score_blend_alpha=0.3,
        reoptimize_interval=10,
        n_trials=None,
        progress_cb=None,
    ):
        """Walk-forward TPE 动态权重回测。

        每 reoptimize_interval 个交易日（默认10），使用之前 60 日窗口运行独立 TPE
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
        self._prefetch_market_multipliers(snap_filtered)
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
                    blended[c] = score_blend_alpha * comp.get(c, 0) + (1 - score_blend_alpha) * tech.get(c, 50.0)
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
        cached_composites: Dict[str, pd.Series] = {}
        for sdate in snap_filtered:
            ss = scores_by_date.get(sdate, {})
            if not ss:
                continue
            if use_pipeline:
                cached_composites[sdate] = ss.get("_pipeline", pd.Series())
            else:
                multipliers = self._get_market_multipliers(sdate)
                adjusted = self._apply_dynamic_weights(factor_weights, multipliers)
                cached_composites[sdate] = self._compute_composite(ss, adjusted)

        fixed_curves: Dict[str, List[Dict]] = {str(h): [] for h in hold_days}
        fixed_trades: Dict[int, List[FactorBacktestTrade]] = {h: [] for h in hold_days}

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

                composite = cached_composites.get(snap_date, pd.Series())
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
                    if self._is_limit_up(code, buy_date, buy_field):
                        skipped.append(code)
                        continue
                    bp = self._get_price(code, buy_date, buy_field)
                    sp, ext_date, status = self._resolve_sell_price(
                        code, sell_date, sell_field, trading_days)
                    if bp and sp and bp > 0 and status not in ("locked", "open"):
                        bought.append((code, name, bp, sp, ext_date, status))
                    elif bp is None:
                        skipped.append(code)
                    else:
                        bought.append((code, name, bp, sp if sp else 0, ext_date, status))
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

        # ── 2. Dynamic walk-forward (按窗口 TPE + DB 持久化缓存) ──
        if progress_cb:
            progress_cb("动态调优回测中…")

        from src.discovery.factor_optimizer import FactorOptimizer

        window_size = 60
        node_end_indices = list(range(window_size - 1, len(snap_filtered), reoptimize_interval))
        nodes_evaluated = 0
        node_weights: Dict[int, Dict[str, float]] = {}

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

            nodes_evaluated += 1
            window_end = opt_window_dates[-1]
            node_study = f"{mode}_w{window_size}_{window_end}"

            # 先查 DB 缓存：同一窗口跑过 TPE 就直接读
            if _optuna_study_has_trials(node_study):
                if progress_cb:
                    progress_cb(
                        f"动态调优节点 {nodes_evaluated}/{len(node_end_indices)} "
                        f"({opt_window_dates[0]}..{window_end}"
                        f" → {eval_dates[0]}..{eval_dates[-1]}) [缓存命中]")
                node_weights[node_idx] = _load_optuna_best_weights(node_study, factor_weights)
                continue

            if progress_cb:
                progress_cb(
                    f"动态调优节点 {nodes_evaluated}/{len(node_end_indices)} "
                    f"({opt_window_dates[0]}..{window_end}"
                    f" → {eval_dates[0]}..{eval_dates[-1]}) [TPE 中…]")

            # 构建此节点的窗口数据和窗口池
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
                n_trials=n_trials if n_trials is not None else min(30, max(10, len(opt_window_dates) // 2)),
                auto_apply=False,
                preloaded=preloaded,
                use_persistent_storage=True,
                study_name=node_study,
                skip_report=True,
            )

            if opt_result and opt_result.get("recommendation"):
                dyn_weights = {**factor_weights}
                for fn, w in opt_result["recommendation"].items():
                    if fn in dyn_weights:
                        dyn_weights[fn] = w
            else:
                dyn_weights = dict(factor_weights)

            node_weights[node_idx] = dyn_weights

        # Phase 2: Precompute dynamic composites per node (shared across hold_days)
        node_composites: Dict[int, Dict[str, pd.Series]] = {}
        for node_idx in node_end_indices:
            dyn_weights = node_weights.get(node_idx)
            if dyn_weights is None:
                continue
            eval_start = node_idx + 1
            eval_end = min(node_idx + reoptimize_interval, len(snap_filtered))
            eval_dates = snap_filtered[eval_start:eval_end]
            comps = {}
            for sdate in eval_dates:
                ss = scores_by_date.get(sdate, {})
                if not ss:
                    continue
                if use_pipeline:
                    comps[sdate] = ss.get("_pipeline", pd.Series())
                else:
                    multipliers = self._get_market_multipliers(sdate)
                    adjusted = self._apply_dynamic_weights(dyn_weights, multipliers)
                    comps[sdate] = self._compute_composite(ss, adjusted)
            node_composites[node_idx] = comps

        # Phase 3: Evaluate dynamic weights for each hold_days
        dynamic_curves: Dict[str, List[Dict]] = {str(h): [] for h in hold_days}
        dynamic_trades: Dict[int, List[FactorBacktestTrade]] = {h: [] for h in hold_days}

        for hd in hold_days:
            if progress_cb:
                progress_cb(f"评估动态权重 · {hd}日持有期…")
            cap = initial_capital
            curve = [{"date": snap_filtered[0], "capital": cap}]

            for node_idx in node_end_indices:
                comps = node_composites.get(node_idx, {})
                if not comps:
                    continue

                eval_start = node_idx + 1
                eval_end = min(node_idx + reoptimize_interval, len(snap_filtered))
                eval_dates = snap_filtered[eval_start:eval_end]

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

                    composite = comps.get(snap_date, pd.Series())
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
                        if self._is_limit_up(code, buy_date, buy_field):
                            skipped.append(code)
                            continue
                        bp = self._get_price(code, buy_date, buy_field)
                        sp, ext_date, status = self._resolve_sell_price(
                            code, sell_date, sell_field, trading_days)
                        if bp and sp and bp > 0 and status not in ("locked", "open"):
                            bought.append((code, name, bp, sp, ext_date, status))
                        elif bp is None:
                            skipped.append(code)
                        else:
                            bought.append((code, name, bp, sp if sp else 0, ext_date, status))
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
                            allocated=round(alloc, 2), status=status,
                            reoptimized=True))
                    for code in skipped:
                        dynamic_trades[hd].append(FactorBacktestTrade(
                            trade_date=snap_date, hold_days=hd, stock_code=code,
                            stock_name=self._stock_names.get(code, code),
                            buy_price=0, sell_date=sell_date, sell_price=0,
                            return_pct=0, pnl=0, allocated=0, status="canceled",
                            reoptimized=True))
                    cap += day_pnl
                    if day_pnl != 0:
                        curve.append({"date": snap_date, "capital": round(cap, 2)})
            dynamic_curves[str(hd)] = curve

        # ── 3. Build result with dual curves ──
        capital_curves: Dict[str, List[Dict]] = {}
        for hd in hold_days:
            capital_curves[f"{hd}_fixed"] = fixed_curves[str(hd)]
            capital_curves[f"{hd}_dynamic"] = dynamic_curves[str(hd)]

        phd = min(hold_days)
        fcurve = fixed_curves[str(phd)]
        dcurve = dynamic_curves[str(phd)]
        fc_fixed = fcurve[-1]["capital"] if fcurve else initial_capital
        fc_dynamic = dcurve[-1]["capital"] if dcurve else initial_capital
        cr_fixed = (fc_fixed - initial_capital) / initial_capital
        cr_dynamic = (fc_dynamic - initial_capital) / initial_capital

        if cr_fixed > -1:
            ar_fixed = math.exp(math.log1p(cr_fixed) * 252 / max(len(snap_filtered), 1)) - 1
        else:
            ar_fixed = cr_fixed
        if cr_dynamic > -1:
            ar_dynamic = math.exp(math.log1p(cr_dynamic) * 252 / max(len(snap_filtered), 1)) - 1
        else:
            ar_dynamic = cr_dynamic

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

        benchmark_curve = self._build_benchmark_curve(trading_days, initial_capital)

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
            capital_curves=capital_curves, rank_ic=ric, quantile_returns=qr,
            trade_records=tds, benchmark_curve=benchmark_curve)

    def _run_full_pipeline(self, scores_by_date, weights, trading_days, pool_n, score_blend_alpha=0.3):
        """管线：纯因子加权 + StockScorer 融合（与盘后扫描完全一致）。"""
        result = {}
        for snap_date, raw_scores in scores_by_date.items():
            if not raw_scores:
                continue
            composite = self._compute_composite(raw_scores, weights)
            if composite.empty:
                continue
            pool = composite.nlargest(pool_n).index.tolist()
            tech = self._batch_stockscorer(pool, snap_date, trading_days, composite)
            final = pd.Series(0.0, index=composite.index)
            for c in composite.index:
                final[c] = score_blend_alpha * composite.get(c, 0) + (1 - score_blend_alpha) * tech.get(c, 50.0)
            result[snap_date] = {'_pipeline': final.dropna()}
        return result

    def _decorrelate_scores(self, score_columns):
        """去相关：资金流/动量/技术三组因子降冗余。"""
        if len(score_columns) < 2:
            return score_columns
        try:
            df_scores = pd.DataFrame(score_columns)
            corr_matrix = df_scores.corr()

            groups = [
                ["money_flow", "hot_money"],
                ["momentum", "ranking_momentum"],
                ["technical", "chip"],
            ]
            for group in groups:
                existing = [f for f in group if f in corr_matrix.columns]
                if len(existing) < 2:
                    continue
                sub = df_scores[existing]
                pc = sub.mean(axis=1)
                for f in existing:
                    orig = df_scores[f]
                    corr_with_mean = corr_matrix.loc[f, existing].mean()
                    score_columns[f] = (orig - pc * corr_with_mean).clip(0, 100).fillna(0)

            return score_columns
        except Exception:
            return score_columns

    def _neutralize_scores(self, scores):
        """行业中性化：行业内 Z-score 归一化到 0-100。"""
        try:
            from src.storage import DatabaseManager
            imap = DatabaseManager().get_ths_industry_map()
            if not imap:
                return scores
            result = {}
            for name, s in scores.items():
                n = pd.Series(50.0, index=s.index)
                sectors = [imap.get(c, "unknown") for c in s.index]
                by_sec = {}
                for i, sec in enumerate(sectors):
                    by_sec.setdefault(sec, []).append(i)
                for sec, idxs in by_sec.items():
                    g = s.iloc[idxs]
                    if g.std() > 1e-6:
                        z = (g - g.mean()) / g.std()
                        n.iloc[idxs] = ((z + 2) / 4 * 100).clip(0, 100)
                result[name] = n
            return result
        except Exception:
            return scores

    @staticmethod
    def _batch_stockscorer_static(codes, snap_date, trading_days, composite=None):
        """对候选池批量 StockScorer（静态版本，可供引擎直接调用）。"""
        try:
            from src.services.stock_scorer import StockScorer, StockScorerConfig
            from src.services.stop_loss_calculator import compute_from_arrays
            scorer = StockScorer(StockScorerConfig())
            # 指数 OHLCV
            idx_df = FactorBacktestEngine._prefetch_index_ohlcv(snap_date)
            if idx_df is not None:
                scorer.preload_index_ohlcv(idx_df)
            # 板块涨跌幅
            sector_pct = FactorBacktestEngine._compute_sector_pct(snap_date)
            if sector_pct:
                scorer.preload_sector_pct(sector_pct)
            # 板块名
            sector_map = FactorBacktestEngine._prefetch_sectors()
            # OHLCV
            full = FactorBacktestEngine._prefetch_full_ohlcv(codes, snap_date)
            tech = {}
            for code in codes:
                try:
                    r = full.get(code)
                    if r is None or len(r["close"]) < 20:
                        tech[code] = 50.0; continue
                    h, l, c, v = (np.array(r[k]) for k in ["high","low","close","volume"])
                    price = float(c[-1])
                    tr = np.maximum(h[1:]-l[1:], np.abs(h[1:]-c[:-1]))
                    tr = np.maximum(tr, np.abs(l[1:]-c[:-1]))
                    atr_v = float(np.mean(tr[-14:])) if len(tr) >= 14 else 0.01
                    ma20_v = float(np.mean(c[-20:])) if len(c) >= 20 else price
                    ma60_v = float(np.mean(c[-60:])) if len(c) >= 60 else price
                    sl = compute_from_arrays(h, l, c, code=code, ma20=ma20_v, ma60=ma60_v, atr=atr_v,
                                             factor_score=float(composite.get(code, 50)) if composite is not None else 50)
                    vols = v[-6:] if len(v) >= 6 else v
                    mv = float(np.mean(vols[:-1])) if len(vols) > 1 else float(vols[-1])
                    vr = float(vols[-1]/mv) if mv > 0 else 1.0
                    pre_close = float(c[-2]) if len(c) > 1 else price
                    res = scorer.score(
                        stock_code=code, sector=sector_map.get(code, ""), price=price, pre_close=pre_close,
                        tp1=sl.take_profit_1 or price*1.1, tp2=sl.take_profit_2 or price*1.2,
                        stop_loss=sl.stop_loss or price*0.93, reasons=[],
                        ohlcv=(h, l, c), volume_ratio=vr)
                    tech[code] = res.composite
                except Exception:
                    tech[code] = 50.0
            return tech
        except Exception:
            return {c: 50.0 for c in codes}

    def _batch_stockscorer(self, codes, snap_date, trading_days, composite=None):
        """委托到静态版本。"""
        return FactorBacktestEngine._batch_stockscorer_static(codes, snap_date, trading_days, composite)

    _sector_cache_static: Dict[str, str] = {}

    @staticmethod
    def _prefetch_sectors():
        """缓存 code → sector 映射（静态）。"""
        if not FactorBacktestEngine._sector_cache_static:
            try:
                from src.storage import DatabaseManager
                FactorBacktestEngine._sector_cache_static = DatabaseManager().get_ths_industry_map() or {}
            except Exception:
                return {}
        return FactorBacktestEngine._sector_cache_static

    @staticmethod
    def _compute_sector_pct(snap_date):
        """从 stock_daily 计算当日各板块平均涨跌幅。"""
        try:
            from src.storage import DatabaseManager, StockDaily
            from datetime import timedelta
            db = DatabaseManager()
            imap = db.get_ths_industry_map()
            if not imap:
                return {}
            end_d = datetime.strptime(snap_date, "%Y%m%d").date()
            s = db.get_session()
            try:
                rows = s.query(StockDaily.code, StockDaily.pct_chg).filter(
                    StockDaily.date == end_d, StockDaily.pct_chg.isnot(None)).all()
                by_sec = {}
                for code, pct in rows:
                    sec = imap.get(code)
                    if sec:
                        by_sec.setdefault(sec, []).append(float(pct or 0))
                return {sec: sum(vals)/len(vals) for sec, vals in by_sec.items() if vals}
            finally:
                s.close()
        except Exception:
            return {}

    @staticmethod
    def _prefetch_full_ohlcv(codes, snap_date):
        """从 stock_daily 取完整 OHLCV + volume: {code: {high,low,close,volume,open: list}}."""
        try:
            from src.storage import DatabaseManager, StockDaily
            from datetime import timedelta
            db = DatabaseManager()
            end = datetime.strptime(snap_date, "%Y%m%d")
            start = end - timedelta(days=90)
            s = db.get_session()
            try:
                rows = s.query(StockDaily).filter(
                    StockDaily.code.in_(list(codes)),
                    StockDaily.date >= start.date(), StockDaily.date <= end.date(),
                ).order_by(StockDaily.date).all()
                by_code = {}
                for r in rows:
                    by_code.setdefault(r.code, []).append(r)
                result = {}
                for code, vals in by_code.items():
                    result[code] = {
                        "high": [float(v.high or 0) for v in vals],
                        "low": [float(v.low or 0) for v in vals],
                        "close": [float(v.close or 0) for v in vals],
                        "volume": [float(v.volume or 0) for v in vals],
                        "open": [float(v.open or 0) for v in vals],
                    }
                return result
            finally:
                s.close()
        except Exception:
            return {}

    @staticmethod
    def _prefetch_index_ohlcv(snap_date):
        """获取上证指数 OHLCV: np.array shape (N, 4) [open, high, low, close]."""
        try:
            from src.storage import DatabaseManager, StockDaily
            from datetime import timedelta
            db = DatabaseManager()
            end = datetime.strptime(snap_date, "%Y%m%d")
            start = end - timedelta(days=90)
            s = db.get_session()
            try:
                rows = s.query(StockDaily).filter(
                    StockDaily.code == "000001",
                    StockDaily.date >= start.date(), StockDaily.date <= end.date(),
                ).order_by(StockDaily.date).all()
                if not rows:
                    return None
                data = [(float(r.open or 0), float(r.high or 0), float(r.low or 0), float(r.close or 0)) for r in rows]
                return np.array(data)
            finally:
                s.close()
        except Exception:
            return None

    @staticmethod
    def _build_benchmark_curve(trading_days, initial_capital):
        """构建上证指数归一化曲线（按初始资金等比例缩放）。"""
        try:
            from datetime import datetime
            from src.storage import DatabaseManager, StockDaily
            db = DatabaseManager()
            td_min = datetime.strptime(min(trading_days), "%Y%m%d").date()
            td_max = datetime.strptime(max(trading_days), "%Y%m%d").date()
            s = db.get_session()
            try:
                rows = s.query(StockDaily).filter(
                    StockDaily.code == "000001.SH",
                    StockDaily.date >= td_min,
                    StockDaily.date <= td_max,
                ).order_by(StockDaily.date).all()
            finally:
                s.close()
            if not rows or len(rows) < 2:
                return []
            close_map = {}
            for r in rows:
                ds = r.date.strftime("%Y%m%d")
                close_map[ds] = float(r.close or 0)
            curve = []
            first_close = None
            for td in trading_days:
                if td not in close_map:
                    continue
                if first_close is None:
                    first_close = close_map[td]
                if first_close and first_close > 0:
                    capital = initial_capital * (close_map[td] / first_close)
                else:
                    capital = float(initial_capital)
                curve.append({"date": td, "capital": round(capital, 2)})
            return curve
        except Exception:
            return []

    def _prefetch_ohlcv(self, codes, snap_date):
        """从 stock_daily 预取 OHLCV 数据: {code: (highs, lows, closes)}。"""
        try:
            from src.storage import DatabaseManager, StockDaily
            from datetime import timedelta
            db = DatabaseManager()
            end = datetime.strptime(snap_date, "%Y%m%d")
            start = end - timedelta(days=90)
            s = db.get_session()
            try:
                rows = s.query(StockDaily).filter(
                    StockDaily.code.in_(list(codes)),
                    StockDaily.date >= start.date(),
                    StockDaily.date <= end.date(),
                ).order_by(StockDaily.date).all()
                by_code = {}
                for r in rows:
                    by_code.setdefault(r.code, []).append((float(r.high or 0), float(r.low or 0), float(r.close or 0)))
                result = {}
                for code, vals in by_code.items():
                    vals.sort()
                    result[code] = (
                        np.array([v[0] for v in vals]),
                        np.array([v[1] for v in vals]),
                        np.array([v[2] for v in vals]),
                    )
                return result
            finally:
                s.close()
        except Exception:
            return {}

    def _get_pipeline_dates(self, mode):
        from src.storage import DatabaseManager
        db = DatabaseManager()
        try:
            if mode == "intraday":
                from src.storage import ScanResultIntraday as M
            else:
                from src.storage import ScanResultPostmarket as M
        except ImportError:
            return []
        with db.get_session() as s:
            from sqlalchemy import distinct as dd
            rows = s.query(dd(M.scan_date)).filter(M.scan_date.isnot(None)).order_by(M.scan_date).all()
            return [r[0] for r in rows]

    def _load_pipeline_scores(self, mode, dates):
        from src.storage import DatabaseManager
        db = DatabaseManager()
        result = {}
        try:
            if mode == "intraday":
                from src.storage import ScanResultIntraday as M
            else:
                from src.storage import ScanResultPostmarket as M
        except ImportError:
            return result
        with db.get_session() as s:
            rows = s.query(M).filter(M.scan_date.in_(dates)).all()
        for r in rows:
            result.setdefault(r.scan_date, {}).setdefault("_pipeline", {})[r.stock_code] = r.total_score or 0
        for dd in result:
            result[dd]["_pipeline"] = pd.Series(result[dd]["_pipeline"])
        return result

    def get_snapshot_date_ranges(self, mode):
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        FLM = {"money_flow": "资金流向", "margin": "融资融券", "chip": "筹码分布",
               "technical": "技术形态", "limit": "涨跌停", "fundamental": "基本面",
               "institution_hold": "机构持股", "profit_forecast": "盈利预测",
               "buyback": "回购", "insider_buy": "高管增持",
               "broker_recommend": "券商推荐", "popularity": "人气", "hot_money": "游资",
               "performance": "业绩", "momentum": "动量", "rebound": "反弹",
               "sector": "板块", "ma_entry": "均线",
               "ranking_momentum": "排名动量", "concept_heat": "概念热度"}
        dw = self._get_default_weights(mode)
        fi, af, at, trading_dates = [], None, None, []
        with db.get_session() as sess:
            from sqlalchemy import func, distinct as sa_distinct
            rows = (sess.query(FactorScoreSnapshot.factor_name,
                    func.min(FactorScoreSnapshot.trade_date),
                    func.max(FactorScoreSnapshot.trade_date),
                    func.count(sa_distinct(FactorScoreSnapshot.trade_date)))
                    .filter(FactorScoreSnapshot.mode == mode)
                    .group_by(FactorScoreSnapshot.factor_name).all())
            for fn, md, xd, cnt in rows:
                fi.append({"name": fn, "label": FLM.get(fn, fn), "mode": mode,
                           "available_from": md, "available_to": xd, "trading_days": cnt,
                           "default_weight": dw.get(fn, 0)})
                if af is None or md > af:
                    af = md
                if at is None or xd < at:
                    at = xd
            # 一次查询获取全部唯一交易日（与 GROUP BY 共用同一 session）
            dt_rows = sess.query(
                sa_distinct(FactorScoreSnapshot.trade_date)
            ).filter(
                FactorScoreSnapshot.mode == mode
            ).order_by(FactorScoreSnapshot.trade_date).all()
            trading_dates = [r[0] for r in dt_rows]
        return fi, {"mode": mode, "available_from": af or "", "available_to": at or "",
                     "trading_dates": trading_dates}

    def _list_factors_with_data(self, mode):
        """列出指定模式下有快照数据的因子名。"""
        try:
            from src.storage import DatabaseManager, FactorScoreSnapshot
            from sqlalchemy import distinct as sa_distinct
            db = DatabaseManager()
            with db.get_session() as s:
                rows = s.query(sa_distinct(FactorScoreSnapshot.factor_name)).filter(
                    FactorScoreSnapshot.mode == mode).all()
                return [r[0] for r in rows]
        except Exception:
            return []

    def _get_default_weights(self, mode):
        try:
            from src.discovery.engine import get_factor_weights
            return get_factor_weights(mode)
        except Exception:
            pass
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        with db.get_session() as sess:
            from sqlalchemy import distinct as dd
            rows = sess.query(dd(FactorScoreSnapshot.factor_name)).filter(
                FactorScoreSnapshot.mode == mode).all()
            names = [r[0] for r in rows]
        return {n: 10.0 for n in names} if names else {}

    def _get_available_dates(self, factor_names, mode):
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        with db.get_session() as sess:
            from sqlalchemy import func, distinct as dd
            rows = (sess.query(FactorScoreSnapshot.trade_date)
                    .filter(FactorScoreSnapshot.mode == mode,
                            FactorScoreSnapshot.factor_name.in_(list(factor_names)))
                    .group_by(FactorScoreSnapshot.trade_date)
                    .having(func.count(dd(FactorScoreSnapshot.factor_name)) >= len(list(factor_names)))
                    .order_by(FactorScoreSnapshot.trade_date).all())
            return [r[0] for r in rows]

    def _get_factor_date_range(self, fn, mode):
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        with db.get_session() as sess:
            from sqlalchemy import func
            r = sess.query(func.min(FactorScoreSnapshot.trade_date),
                           func.max(FactorScoreSnapshot.trade_date)).filter(
                FactorScoreSnapshot.factor_name == fn, FactorScoreSnapshot.mode == mode).first()
            if r and r[0]:
                return (r[0], r[1])
        return ("", "")

    def _load_snapshots(self, factor_names, mode, dates, progress_cb=None):
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        fns = list(factor_names)
        # 单次批量查询，而非逐日期循环（728 日期 → 1 次查询）
        if progress_cb:
            progress_cb(f"加载因子数据中 ({len(dates)} 日期)...")
        with db.get_session() as sess:
            rows = sess.query(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.mode == mode,
                FactorScoreSnapshot.factor_name.in_(fns),
                FactorScoreSnapshot.trade_date.in_(dates),
            ).all()
        result = {}
        for r in rows:
            code = r.ts_code or ""
            if "." in code:
                code = code.split(".")[0]
            result.setdefault(r.trade_date, {}).setdefault(r.factor_name, {})[code] = r.score
        for dd in result:
            for fn in result[dd]:
                result[dd][fn] = pd.Series(result[dd][fn])
        return result

    @staticmethod
    def _compute_composite(scores, weights):
        tw = 0.0
        comp = pd.Series(0.0, index=pd.Index([]))
        for fn, w in weights.items():
            s = scores.get(fn)
            if s is None or s.empty:
                continue
            sc = s.dropna()
            if sc.empty:
                continue
            tw += w
            comp = comp.add(sc * w, fill_value=0.0)
        if tw > 0:
            comp = comp / tw
        return comp.sort_values(ascending=False)

    def _get_trading_days(self, snap_dates):
        md = snap_dates[0]
        xd = (datetime.strptime(snap_dates[-1], "%Y%m%d") + timedelta(days=60)).strftime("%Y%m%d")
        if self._fetcher:
            try:
                df = self._fetcher._call_api_with_rate_limit(
                    "trade_cal", exchange="SSE", start_date=md, end_date=xd, is_open="1")
                if df is not None and not df.empty:
                    return sorted(df["cal_date"].tolist())
            except Exception:
                pass
        try:
            import exchange_calendars as xc
            cal = xc.get_calendar("XSHG")
            ss = cal.sessions_in_range(
                datetime.strptime(md, "%Y%m%d").strftime("%Y-%m-%d"),
                datetime.strptime(xd, "%Y%m%d").strftime("%Y-%m-%d"))
            return [s.strftime("%Y%m%d") for s in ss]
        except Exception:
            pass
        ds = []
        d = datetime.strptime(md, "%Y%m%d")
        e = datetime.strptime(xd, "%Y%m%d")
        while d <= e:
            if d.weekday() < 5:
                ds.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        return ds

    def _find_next_td(self, ds, tds):
        for td in tds:
            if td > ds:
                return td
        return None

    def _resolve_sell_price(self, code, sell_date, sell_field, trading_days, max_ext=5):
        """解析卖出价：若卖出日在跌停板或缺失价格，顺延至下一个可交易日。

        Returns (sell_price, final_sell_date, status)。
        status: "closed" | "extended" | "locked"（跌停无法卖出）| "open"（无价格）
        """
        sp = self._get_price(code, sell_date, sell_field)
        ext_date = sell_date
        status = "closed"

        for _ in range(max_ext):
            if sp is not None and not self._is_limit_down(code, ext_date, sell_field):
                return sp, ext_date, status
            ext = self._find_next_td(ext_date, trading_days)
            if not ext:
                break
            ext_sp = self._get_price(code, ext, sell_field)
            ext_date = ext
            if ext_sp is not None:
                sp = ext_sp
                status = "extended"

        if sp is not None and self._is_limit_down(code, ext_date, sell_field):
            status = "locked"
        elif sp is None:
            status = "open"
        return sp, ext_date, status

    def _prefetch_market_multipliers(self, snap_dates):
        """一次 DB 查询批量预计算所有 snap_date 的市场动态权重乘数。"""
        if not snap_dates or self._market_mult_cache:
            return
        try:
            from src.storage import DatabaseManager, StockDaily
            db = DatabaseManager()
            min_sd = datetime.strptime(min(snap_dates), "%Y%m%d").date() - timedelta(days=25)
            max_sd = datetime.strptime(max(snap_dates), "%Y%m%d").date()
            with db.get_session() as sess:
                rows = (sess.query(StockDaily.pct_chg, StockDaily.date)
                        .filter(StockDaily.code == "000001.SH",
                                StockDaily.date >= min_sd,
                                StockDaily.date <= max_sd)
                        .order_by(StockDaily.date.asc()).all())
            # build ordered list of (date_str, pct_chg) for fast slicing
            daily_returns = [(r.date.strftime("%Y%m%d") if isinstance(r.date, date) else str(r.date)[:8],
                              float(r.pct_chg) if r.pct_chg is not None else 0.0)
                             for r in rows]
            for sd in snap_dates:
                # collect preceding ≤20 returns
                preceding = [pct for d, pct in daily_returns if d < sd]
                returns = pd.Series(preceding[-20:]).dropna()
                if len(returns) < 5:
                    self._market_mult_cache[sd] = {}
                    continue
                vol = returns.std()
                ts = abs(returns.mean() / (vol + 1e-9))
                if ts > 0.8:
                    self._market_mult_cache[sd] = {"momentum": 1.3, "rebound": 0.7, "technical": 1.1}
                elif vol > 1.5:
                    self._market_mult_cache[sd] = {"rebound": 1.4, "performance": 1.2,
                                                   "profit_forecast": 1.1, "momentum": 0.6}
                else:
                    self._market_mult_cache[sd] = {}
        except Exception:
            pass

    def _get_market_multipliers(self, snap_date: str) -> Dict[str, float]:
        """返回 snap_date 的市场动态权重乘数，优先使用批量预计算结果。"""
        if self._market_mult_cache:
            return self._market_mult_cache.get(snap_date, {})
        # fallback: per-date DB query（批量预取未调用时）
        try:
            from src.storage import DatabaseManager, StockDaily
            db = DatabaseManager()
            sd = datetime.strptime(snap_date, "%Y%m%d").date()
            with db.get_session() as sess:
                rows = (sess.query(StockDaily.pct_chg)
                        .filter(StockDaily.code == "000001.SH",
                                StockDaily.date < sd)
                        .order_by(StockDaily.date.desc())
                        .limit(20).all())
            returns = pd.Series([float(r[0]) for r in rows if r[0] is not None]).dropna()
            if len(returns) < 5:
                return {}
            volatility = returns.std()
            trend_strength = abs(returns.mean() / (returns.std() + 1e-9))
            if trend_strength > 0.8:
                return {"momentum": 1.3, "rebound": 0.7, "technical": 1.1}
            elif volatility > 1.5:
                return {"rebound": 1.4, "performance": 1.2, "profit_forecast": 1.1, "momentum": 0.6}
            return {}
        except Exception:
            return {}

    def _apply_dynamic_weights(self, base_weights: Dict[str, float],
                                multipliers: Dict[str, float]) -> Dict[str, float]:
        """将市场状态乘数应用到基础权重。"""
        if not multipliers:
            return base_weights
        return {k: v * multipliers.get(k, 1.0) for k, v in base_weights.items()}

    def _prefetch_prices(self, codes, tds):
        if not codes or not tds:
            return
        dbcs = set()
        try:
            from src.storage import DatabaseManager, StockDaily
            db = DatabaseManager()
            do = [datetime.strptime(d, "%Y%m%d").date() for d in tds]
            # Batch codes to keep IN clause small
            codes_list = list(codes)
            for batch_start in range(0, len(codes_list), 500):
                batch = codes_list[batch_start:batch_start + 500]
                s = db.get_session()
                try:
                    rows = s.query(StockDaily).filter(
                        StockDaily.code.in_(batch), StockDaily.date.in_(do)).all()
                    for r in rows:
                        ds = r.date.strftime("%Y%m%d") if isinstance(r.date, date) else str(r.date)[:8]
                        self._price_cache.setdefault(ds, {})[r.code] = {
                            "open": float(r.open) if r.open else None,
                            "high": float(r.high) if r.high else None,
                            "low": float(r.low) if r.low else None,
                            "close": float(r.close) if r.close else None,
                            "pct_chg": float(r.pct_chg) if r.pct_chg is not None else None}
                    dbcs.update(r.code for r in rows)
                finally:
                    s.close()
        except Exception as e:
            logger.debug("[FactorBacktest] DB price: %s", e)
        miss = [c for c in codes if c not in dbcs]
        if not miss or self._fetcher is None:
            return
        try:
            tcs = []
            for c in miss:
                if c.isdigit() and len(c) == 6:
                    tcs.append(f"{c}.SH")
                    tcs.append(f"{c}.SZ")
                else:
                    tcs.append(c)
            for td in tds:
                ex = set((self._price_cache.get(td) or {}).keys())
                if set(miss).issubset(ex):
                    continue
                try:
                    df = self._fetcher._call_api_with_rate_limit(
                        "daily", ts_code=",".join(tcs[:200]), trade_date=td)
                    if df is not None and not df.empty:
                        for _, r in df.iterrows():
                            ts = str(r.get("ts_code", ""))
                            c = ts.split(".")[0] if "." in ts else ts
                            self._price_cache.setdefault(td, {})[c] = {
                                "open": float(r["open"]) if pd.notna(r.get("open")) else None,
                                "high": float(r["high"]) if pd.notna(r.get("high")) else None,
                                "low": float(r["low"]) if pd.notna(r.get("low")) else None,
                                "close": float(r["close"]) if pd.notna(r.get("close")) else None,
                                "pct_chg": float(r["pct_chg"]) if pd.notna(r.get("pct_chg")) else None}
                except Exception:
                    pass
        except Exception as e:
            logger.debug("[FactorBacktest] Tushare price: %s", e)

        # 实时行情补充：realtime_spot DB + Sina API 补齐今日盘中价格
        today_str = date.today().strftime("%Y%m%d")
        if today_str in tds:
            self._prefetch_realtime_spot(codes, today_str)
            self._prefetch_sina_realtime(codes, today_str)

    def _prefetch_realtime_spot(self, codes, date_str):
        """从 realtime_spot DB 补齐当日盘中 OHLC。"""
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            spot_df = db.get_realtime_spot()
            if spot_df is None or spot_df.empty:
                return

            from datetime import datetime as _dt, timezone, timedelta
            now_cn = _dt.now(timezone(timedelta(hours=8)))
            is_market_open = (
                now_cn.weekday() < 5
                and now_cn.time() >= _dt.strptime("09:25", "%H:%M").time()
            )

            spot_index = spot_df.index.astype(str).str.strip().str.zfill(6)
            for code in codes:
                bare = str(code).strip().zfill(6)
                matches = spot_df[spot_index == bare]
                if matches.empty:
                    continue
                row = matches.iloc[0]
                cache_entry = self._price_cache.setdefault(date_str, {}).setdefault(code, {})

                price = row.get("price")
                if pd.notna(price) and float(price) > 0:
                    cache_entry["close"] = float(price)

                if not is_market_open:
                    continue
                if pd.notna(row.get("open_price")):
                    cache_entry.setdefault("open", float(row["open_price"]))
                if pd.notna(row.get("high")):
                    cache_entry.setdefault("high", float(row["high"]))
                if pd.notna(row.get("low")):
                    cache_entry.setdefault("low", float(row["low"]))
        except Exception as e:
            logger.debug("[FactorBacktest] realtime_spot 取价失败: %s", e)

    def _prefetch_sina_realtime(self, codes, date_str):
        """通过 Sina 实时行情补齐当日 OHLC（盘中可用）。"""
        sina_codes = []
        for c in codes:
            if not c.isdigit() or len(c) != 6:
                continue
            existing = (self._price_cache.get(date_str, {}).get(c) or {})
            if existing.get("open") and existing.get("high") and existing.get("low") and existing.get("close"):
                continue
            prefix = "sh" if c.startswith(("6", "68")) else "sz" if c.startswith(("0", "3")) else "bj"
            sina_codes.append(f"{prefix}{c}")

        if not sina_codes:
            return

        try:
            url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
            headers = {"Referer": "http://finance.sina.com.cn"}
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "gbk"
            body = resp.text
        except Exception as e:
            logger.debug("[FactorBacktest] Sina 实时行情请求失败: %s", e)
            return

        from datetime import datetime as _dt, timezone, timedelta as td
        now_cn = _dt.now(timezone(timedelta(hours=8)))
        is_market_open = (
            now_cn.weekday() < 5
            and now_cn.time() >= _dt.strptime("09:25", "%H:%M").time()
        )

        for sc in sina_codes:
            try:
                pattern = re.compile(rf"var hq_str_{sc}=\"([^\"]*)\"")
                m = pattern.search(body)
                if not m:
                    continue
                fields = m.group(1).split(",")
                if len(fields) < 6:
                    continue
                code = sc[2:]
                open_p = float(fields[1]) if fields[1] and fields[1] != "0.000" else None
                high_p = float(fields[4]) if fields[4] and fields[4] != "0.000" else None
                low_p = float(fields[5]) if fields[5] and fields[5] != "0.000" else None
                close_p = float(fields[3]) if fields[3] and fields[3] != "0.000" else None

                if open_p is None and high_p is None and low_p is None and close_p is None:
                    continue

                cache_entry = self._price_cache.setdefault(date_str, {}).setdefault(code, {})
                if is_market_open:
                    if open_p is not None:
                        cache_entry["open"] = open_p
                    if high_p is not None:
                        cache_entry["high"] = high_p
                    if low_p is not None:
                        cache_entry["low"] = low_p
                if close_p is not None:
                    cache_entry["close"] = close_p
            except Exception:
                pass

    def _prefetch_stock_names(self, codes):
        if not codes:
            return
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            s = db.get_session()
            try:
                from sqlalchemy import text, bindparam
                # scan_result_postmarket（覆盖最全）
                rows = s.execute(
                    text("SELECT DISTINCT stock_code, stock_name FROM scan_result_postmarket WHERE stock_code IN :codes").bindparams(
                        bindparam("codes", expanding=True)),
                    {"codes": list(codes)},
                ).fetchall()
                for stock_code, name in rows:
                    if name:
                        self._stock_names[stock_code] = name
                # 补齐：limit_pool
                missing = [c for c in codes if c not in self._stock_names]
                if missing:
                    ts_codes = [f"{c}.SZ" for c in missing] + [f"{c}.SH" for c in missing] + [f"{c}.BJ" for c in missing]
                    rows2 = s.execute(
                        text("SELECT DISTINCT ts_code, name FROM limit_pool WHERE ts_code IN :codes").bindparams(
                            bindparam("codes", expanding=True)),
                        {"codes": ts_codes},
                    ).fetchall()
                    for ts_code, name in rows2:
                        if name:
                            code = ts_code.split(".")[0] if "." in ts_code else ts_code
                            if code not in self._stock_names:
                                self._stock_names[code] = name
            finally:
                s.close()
        except Exception:
            pass

    def _precompute_forward_returns(self, all_codes, trading_days, snap_dates, hold_days, mode):
        """预计算所有 (code, buy_date, sell_date) 的 forward return，供 IC & quantile 复用。"""
        is_intra = mode == "intraday"
        bf = "close" if is_intra else "open"
        sf = "close" if is_intra else "open"
        fw = {}  # (buy_date, sell_date) -> {code: return}
        seen = set()
        for hd in hold_days:
            for sd in snap_dates:
                if sd not in trading_days:
                    continue
                ti = trading_days.index(sd)
                buy_idx = ti if is_intra else ti + 1
                sell_idx = (ti + hd) if is_intra else (ti + 1 + hd)
                if buy_idx >= len(trading_days) or sell_idx >= len(trading_days):
                    continue
                bd = trading_days[buy_idx]
                ed = trading_days[sell_idx]
                pair = (bd, ed)
                if pair in seen:
                    continue
                seen.add(pair)
                fr = {}
                for c in all_codes:
                    bp = self._get_price(c, bd, bf)
                    sp = self._get_price(c, ed, sf)
                    if bp and sp and bp > 0:
                        fr[c] = (sp - bp) / bp
                fw[pair] = fr
        return fw

    def _get_price(self, code, ds, field):
        v = (self._price_cache.get(ds, {}).get(code) or {}).get(field)
        if v is not None:
            return float(v)

        # Fallback：当天非 close 字段（open/high/low）未命中缓存时，
        # 优先用当天 close（实时价）作为近似值，避免盘中静默丢弃未平仓交易。
        today_str = date.today().strftime("%Y%m%d")
        if ds == today_str and field != "close":
            close_val = (self._price_cache.get(ds, {}).get(code) or {}).get("close")
            if close_val is not None:
                return float(close_val)

        # 当天 close 仍未命中 → 搜索缓存中最近交易日的 close（仅当天，未来日期无数据不降级）
        if ds == today_str:
            for d in sorted(self._price_cache.keys(), reverse=True):
                cv = (self._price_cache.get(d, {}).get(code) or {}).get("close")
                if cv is not None:
                    return float(cv)

        return None

    @staticmethod
    def _get_limit_pct(code: str) -> float:
        """返回涨跌停幅度：创业板30xxxx/301xxx 和科创板688xxx 为20%，其余为10%。"""
        if code.startswith(("300", "301", "688")):
            return 0.20
        return 0.10

    def _is_limit_up(self, code, ds, field="close"):
        """检查交易日 ds 是否涨停（无法以 field 价买入）。

        field="open"（盘后开盘买入）：仅检查一字涨停。
        field="close"（盘中收盘买入）：检查一字涨停 + pct_chg 阈值。"""
        o = self._get_price(code, ds, "open")
        h = self._get_price(code, ds, "high")
        l = self._get_price(code, ds, "low")
        if o and h and l and o > 0:
            if abs(h - l) < 0.001 and abs(o - h) < 0.001:
                pct = self._get_price(code, ds, "pct_chg")
                if pct is None or pct >= 0:
                    return True  # 一字涨停 或 无法分辨（保守）
                return False  # 一字跌停 → 可以买入
        if field == "close":
            pct = self._get_price(code, ds, "pct_chg")
            if pct is not None:
                limit_pct = self._get_limit_pct(code)
                if pct >= (limit_pct * 100 - 1.0):
                    return True
        return False

    def _is_limit_down(self, code, ds, field="close"):
        """检查交易日 ds 是否跌停（无法以 field 价卖出）。

        field="open"（盘后开盘卖出）：仅检查一字跌停。
        field="close"（盘中收盘卖出）：检查一字跌停 + pct_chg 阈值。"""
        o = self._get_price(code, ds, "open")
        h = self._get_price(code, ds, "high")
        l = self._get_price(code, ds, "low")
        if o and h and l and o > 0:
            if abs(h - l) < 0.001 and abs(o - h) < 0.001:
                pct = self._get_price(code, ds, "pct_chg")
                if pct is None or pct <= 0:
                    return True  # 一字跌停 或 无法分辨（保守）
                return False  # 一字涨停 → 可以卖出
        if field == "close":
            pct = self._get_price(code, ds, "pct_chg")
            if pct is not None:
                limit_pct = self._get_limit_pct(code)
                if pct <= -(limit_pct * 100 - 1.0):
                    return True
        return False

    def _calc_mdd(self, curve, ic):
        mdd = 0.0
        mdd_start = ""
        mdd_end = ""
        peak = ic
        peak_date = curve[0].get("date", "") if curve else ""
        dd_start = ""
        for pt in curve[1:]:
            cap = pt.get("capital", peak)
            pt_date = pt.get("date", "")
            if cap > peak:
                peak = cap
                peak_date = pt_date
            dd = (peak - cap) / peak if peak > 0 else 0.0
            if dd > 0 and dd_start == "":
                dd_start = pt_date
            if dd > mdd:
                mdd = dd
                mdd_start = dd_start or peak_date
                mdd_end = pt_date
            if dd == 0:
                dd_start = ""
        return mdd, mdd_start, mdd_end

    def _calc_sharpe(self, drs, rfr):
        if not drs:
            return 0.0
        a = np.array(drs)
        m = np.mean(a)
        s = np.std(a, ddof=1)
        if s == 0:
            return 0.0
        drf = (1 + rfr) ** (1 / 252) - 1
        return (m - drf) / s * np.sqrt(252)

    def _calc_rank_ic(self, sbd, hd, tds, mode, fw_returns):
        """向量化 Rank IC：每日期一次 pd.DataFrame.corr 替代逐因子 scipy.spearmanr。"""
        is_intra = mode == "intraday"
        res = {}
        for sd, ss in sbd.items():
            if sd not in tds:
                continue
            ti = tds.index(sd)
            bd_idx = ti if is_intra else ti + 1
            sd_idx = (ti + hd) if is_intra else (ti + 1 + hd)
            if sd_idx >= len(tds):
                continue
            bd = tds[bd_idx]
            ed = tds[sd_idx]
            fr = fw_returns.get((bd, ed), {})
            if len(fr) < 30:
                continue
            # 构建 DataFrame：每因子一列 + _fr 列，一次 corr 算出所有因子 IC
            data = {fn: fs for fn, fs in ss.items()}
            data['_fr'] = pd.Series(fr)
            df = pd.DataFrame(data)
            corr = df.corr(method='spearman', min_periods=30)
            ic_row = corr['_fr']
            for fn in ss:
                ic = ic_row.get(fn)
                if ic is not None and not np.isnan(ic):
                    res.setdefault(fn, []).append(ic)
        return {n: round(float(np.mean(v)), 4) for n, v in res.items() if v}

    def _calc_quantile(self, sbd, wts, hd, tds, mode, fw_returns, cached_composites=None):
        is_intra = mode == "intraday"
        ar, tr10, tr20 = [], [], []
        for sd, ss in sbd.items():
            if sd not in tds:
                continue
            ti = tds.index(sd)
            bd_idx = ti if is_intra else ti + 1
            sd_idx = (ti + hd) if is_intra else (ti + 1 + hd)
            if sd_idx >= len(tds):
                continue
            bd = tds[bd_idx]
            ed = tds[sd_idx]
            fr = fw_returns.get((bd, ed), {})
            if len(fr) < 10:
                continue
            if "_pipeline" in ss:
                comp = ss["_pipeline"]
            elif cached_composites and sd in cached_composites:
                comp = cached_composites[sd]
            else:
                comp = self._compute_composite(ss, wts)
            if comp.empty:
                continue
            rs = [fr[c] for c in comp.sort_values(ascending=False).index if c in fr]
            if not rs:
                continue
            t10n = max(1, len(rs) // 10)
            t20n = max(1, len(rs) // 5)
            ar.append(np.mean(rs))
            tr10.append(np.mean(rs[:t10n]))
            tr20.append(np.mean(rs[:t20n]))
        return {"top_10pct": round(float(np.mean(tr10)), 4) if tr10 else 0,
                "top_20pct": round(float(np.mean(tr20)), 4) if tr20 else 0,
                "top_50pct": round(float(np.mean(ar)), 4) if ar else 0}

    # ── quick_monitor ──

    def quick_monitor(self, mode: str = "postmarket", window: int = 20,
                      hold_days: Optional[List[int]] = None) -> Optional[Dict]:
        """快速因子监控：加载最近 N 天快照，计算每个因子的 Rank IC。

        替代 FactorMonitor 的日常监控角色。直接从 factor_score_snapshots 表读取并计算 IC，
        无需记录 picks 和回填。

        Returns:
            {
                "factors": {"technical": {"ic_1": 0.052, "ic_5": 0.038}, ...},
                "summary": "## 因子监控报告\\n...",
                "trade_dates": ["20260501", ...],
                "generated_at": "2026-05-16T18:00:00",
            }
        """
        if hold_days is None:
            hold_days = [1, 5]

        factor_names = self._list_factors_with_data(mode)
        if not factor_names:
            logger.warning("[quick_monitor] %s 模式无因子快照数据", mode)
            return None

        # 取最近 N 天快照日期（从全部有数据的因子取并集）
        from src.storage import DatabaseManager, FactorScoreSnapshot
        from sqlalchemy import func, distinct as sa_distinct
        db = DatabaseManager()
        with db.get_session() as sess:
            rows = (sess.query(FactorScoreSnapshot.trade_date)
                    .filter(FactorScoreSnapshot.mode == mode,
                            FactorScoreSnapshot.factor_name.in_(factor_names))
                    .group_by(FactorScoreSnapshot.trade_date)
                    .order_by(FactorScoreSnapshot.trade_date.desc())
                    .limit(window).all())
            snap_dates = sorted([r[0] for r in rows])
        if len(snap_dates) < 3:
            logger.warning("[quick_monitor] %s 快照日期不足 (%d < 3)", mode, len(snap_dates))
            return None

        scores_by_date = self._load_snapshots(factor_names, mode, snap_dates)
        if not scores_by_date:
            return None

        trading_days = self._get_trading_days(snap_dates)
        if len(trading_days) < 2:
            return None

        # 预取价格用于 IC 计算
        all_codes = set()
        for ss in scores_by_date.values():
            for s in ss.values():
                if hasattr(s, 'index'):
                    all_codes.update(s.index.tolist())
        self._prefetch_prices(list(all_codes), trading_days)

        # 计算每个持有期的 IC
        ic_result: Dict[str, Dict[str, float]] = {}
        for hd in hold_days:
            ic_result[str(hd)] = self._calc_rank_ic(scores_by_date, hd, trading_days, mode)

        # 构建按因子汇总的 IC 表
        factors: Dict[str, Dict[str, float]] = {}
        for fn in factor_names:
            entry = {}
            for hd_str, hd_ic in ic_result.items():
                if fn in hd_ic:
                    entry[f"ic_{hd_str}"] = hd_ic[fn]
            if entry:
                factors[fn] = entry

        # 生成 Markdown 摘要
        lines = [f"## 因子监控 · {mode}",
                 f"**快照窗口**: {snap_dates[0]} ~ {snap_dates[-1]} ({len(snap_dates)} 天)",
                 f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                 "",
                 "| 因子 | " + " | ".join(f"IC({h}d)" for h in hold_days) + " |",
                 "|------|" + "|".join("------|" for _ in hold_days)]
        for fn in factor_names:
            if fn in factors:
                vals = [f"{factors[fn].get(f'ic_{h}', 0):.4f}" for h in hold_days]
                lines.append(f"| {fn} | " + " | ".join(vals) + " |")
        # 负 IC 标记
        negs = []
        for hd_str, hd_ic in ic_result.items():
            for fn, ic in hd_ic.items():
                if ic < -0.01:
                    negs.append(f"{fn}(IC{hd_str}d={ic:.4f})")
        if negs:
            lines.append(f"\n⚠️ 负 IC: {', '.join(negs)}")

        summary = "\n".join(lines)
        logger.info("[quick_monitor] %s 模式: %d 因子, %d 快照日期",
                     mode, len(factors), len(snap_dates))

        return {
            "factors": factors,
            "summary": summary,
            "trade_dates": snap_dates,
            "generated_at": datetime.now().isoformat(),
        }
