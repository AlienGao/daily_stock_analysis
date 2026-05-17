#!/usr/bin/env python3
"""对比发现引擎与回测引擎的信号计算管线。

用法:
    python scripts/compare_engine_pipelines.py --days 30
    python scripts/compare_engine_pipelines.py --days 30 --mode intraday
    python scripts/compare_engine_pipelines.py --days 30 --mode postmarket
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("compare_pipelines")

import numpy as np
import pandas as pd


# ── helpers ──────────────────────────────────────────────────────────────────


def _spearman_corr(a: pd.Series, b: pd.Series) -> float:
    common = a.index.intersection(b.index)
    if len(common) < 10:
        return float("nan")
    return float(a[common].corr(b[common], method="spearman"))


def _pearson_corr(a: pd.Series, b: pd.Series) -> float:
    common = a.index.intersection(b.index)
    if len(common) < 10:
        return float("nan")
    return float(a[common].corr(b[common], method="pearson"))


def _top_n_overlap(a: pd.Series, b: pd.Series, n: int) -> float:
    if len(a) < n or len(b) < n:
        return float("nan")
    sa = set(a.nlargest(n).index)
    sb = set(b.nlargest(n).index)
    return len(sa & sb) / n


# ── main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="对比发现引擎与回测引擎管线")
    parser.add_argument("--days", type=int, default=30, help="回溯交易日数")
    parser.add_argument("--mode", choices=["intraday", "postmarket", "both"],
                        default="both", help="对比模式")
    args = parser.parse_args()

    # ── 1. 获取交易日列表 ──────────────────────────────────────────────
    from src.storage import DatabaseManager, FactorScoreSnapshot
    from sqlalchemy import func, distinct as sa_distinct

    db = DatabaseManager()

    def get_trading_days(mode: str, limit: int):
        with db.get_session() as sess:
            rows = (
                sess.query(FactorScoreSnapshot.trade_date)
                .filter(FactorScoreSnapshot.mode == mode)
                .group_by(FactorScoreSnapshot.trade_date)
                .having(func.count(sa_distinct(FactorScoreSnapshot.factor_name)) >= 3)
                .order_by(FactorScoreSnapshot.trade_date.desc())
                .limit(limit)
                .all()
            )
        return [r[0] for r in reversed(rows)]

    # ── 2. 加载因子得分 ──────────────────────────────────────────────
    def load_scores(mode: str, dates: list):
        from src.discovery.factor_backtest_engine import FactorBacktestEngine
        engine = FactorBacktestEngine()
        fws = engine._get_default_weights(mode)
        return engine._load_snapshots(list(fws.keys()), mode, dates)

    # ── 3. 管线方法 ──────────────────────────────────────────────────
    from src.discovery.factor_backtest_engine import FactorBacktestEngine as BTE

    bt = BTE()

    def _default_factors_map():
        from src.discovery.engine import _default_factors
        return {f.name: f.__class__ for f in _default_factors()}

    def bte_dynamic_multipliers(snap_date: str):
        return bt._get_market_multipliers(snap_date)

    _dsc_engine_cache = {}

    def _get_dsc_engine(mode: str):
        if mode not in _dsc_engine_cache:
            from src.discovery.engine import StockDiscoveryEngine
            from src.discovery.config import DiscoveryConfig
            engine = StockDiscoveryEngine(DiscoveryConfig())
            for name, factor_cls in _default_factors_map().items():
                f = factor_cls()
                if f.is_available(mode):
                    engine.register_factor(f)
            _dsc_engine_cache[mode] = engine
        return _dsc_engine_cache[mode]

    def dsc_dynamic_multipliers(mode: str):
        try:
            engine = _get_dsc_engine(mode)
            return engine._calc_dynamic_weights(mode)
        except Exception as e:
            logger.warning("发现引擎动态权重计算失败: %s", e)
            return {}

    def get_discovery_weights(mode: str):
        engine = _get_dsc_engine(mode)
        fws = bt._get_default_weights(mode)
        return {n: engine._get_effective_weight(n, mode) for n in fws}

    # ── 4. 逐日对比 ──────────────────────────────────────────────────
    modes_to_test = ["intraday", "postmarket"] if args.mode == "both" else [args.mode]

    for mode in modes_to_test:
        print(f"\n{'='*70}")
        print(f"  {mode.upper()} 模式 — 发现引擎 vs 回测引擎")
        print(f"{'='*70}")

        dates = get_trading_days(mode, args.days)
        if len(dates) < 5:
            print(f"  ⚠ 仅有 {len(dates)} 个交易日，跳过")
            continue
        print(f"  交易日: {len(dates)} ({dates[0]} ~ {dates[-1]})")

        scores_by_date = load_scores(mode, dates)
        available_dates = sorted(scores_by_date.keys())
        print(f"  有数据: {len(available_dates)} 天")

        dsc_base_weights = get_discovery_weights(mode)
        bte_base_weights = bt._get_default_weights(mode)

        # 基础权重差异
        print(f"\n  因子数: {len(dsc_base_weights)}")
        common_factors = sorted(set(dsc_base_weights) & set(bte_base_weights))
        weight_diffs = []
        for fn in common_factors:
            dw = dsc_base_weights.get(fn, 0)
            bw = bte_base_weights.get(fn, 0)
            if abs(dw - bw) > 0.01:
                weight_diffs.append((fn, dw, bw))
        if weight_diffs:
            print(f"  基础权重差异 ({len(weight_diffs)} 项):")
            for fn, dw, bw in weight_diffs[:10]:
                print(f"    {fn:25s}  发现={dw:6.1f}  回测={bw:6.1f}")
        else:
            print(f"  基础权重: 一致 ✅")

        # 对比两种管线模式
        for pipeline in [True, False]:
            label = "管线模式" if pipeline else "纯因子模式"
            print(f"\n  --- {label} ---")

            metrics = {
                "spearman": [], "pearson": [],
                "top10": [], "top20": [], "top50": [], "top300": [],
                "score_diff_pct": [],
                "dyn_weight_match": [],
            }

            for sdate in available_dates:
                sc = scores_by_date[sdate]
                if len(sc) < 3:
                    continue

                dsc_scores = {k: v.copy() for k, v in sc.items()}
                bte_scores = {k: v.copy() for k, v in sc.items()}

                if pipeline:
                    dsc_scores = bt._decorrelate_scores(dsc_scores)
                    bte_scores = bt._decorrelate_scores(bte_scores)
                    dsc_scores = bt._neutralize_scores(dsc_scores)
                    bte_scores = bt._neutralize_scores(bte_scores)

                # 动态权重
                dsc_mult = dsc_dynamic_multipliers(mode)
                bte_mult = bte_dynamic_multipliers(sdate)

                dsc_eff = dict(dsc_base_weights)
                bte_eff = dict(bte_base_weights)
                if dsc_mult:
                    for k, v in dsc_mult.items():
                        if k in dsc_eff:
                            dsc_eff[k] = round(dsc_eff[k] * v, 1)
                if bte_mult:
                    for k, v in bte_mult.items():
                        if k in bte_eff:
                            bte_eff[k] = round(bte_eff[k] * v, 1)

                common_f = set(dsc_eff.keys()) & set(bte_eff.keys())
                if common_f:
                    match = all(
                        abs(dsc_eff.get(f, 0) - bte_eff.get(f, 0)) < 0.01
                        for f in common_f
                    )
                    metrics["dyn_weight_match"].append(1.0 if match else 0.0)

                dsc_comp = BTE._compute_composite(dsc_scores, dsc_eff)
                bte_comp = BTE._compute_composite(bte_scores, bte_eff)

                if dsc_comp.empty or bte_comp.empty:
                    continue

                sp = _spearman_corr(dsc_comp, bte_comp)
                pr = _pearson_corr(dsc_comp, bte_comp)
                metrics["spearman"].append(sp)
                metrics["pearson"].append(pr)

                for n in [10, 20, 50, 300]:
                    metrics[f"top{n}"].append(_top_n_overlap(dsc_comp, bte_comp, n))

                common = dsc_comp.index.intersection(bte_comp.index)
                if len(common) > 0:
                    diff = (dsc_comp[common] - bte_comp[common]).abs().mean()
                    mean_score = bte_comp[common].abs().mean()
                    metrics["score_diff_pct"].append(
                        float(diff / mean_score * 100) if mean_score > 0 else 0.0
                    )

            def _mean(vals):
                vals = [v for v in vals if not (isinstance(v, float) and np.isnan(v))]
                return float(np.mean(vals)) if vals else float("nan")

            n_days = len(metrics["spearman"])
            print(f"  样本天数: {n_days}")
            sp_m = _mean(metrics["spearman"])
            pr_m = _mean(metrics["pearson"])
            t10 = _mean(metrics["top10"])
            t20 = _mean(metrics["top20"])
            t50 = _mean(metrics["top50"])
            t300 = _mean(metrics["top300"])
            diff_pct = _mean(metrics["score_diff_pct"])
            dyn_m = _mean(metrics["dyn_weight_match"])

            print(f"  Spearman 秩相关:       {sp_m:.4f}")
            print(f"  Pearson 线性相关:      {pr_m:.4f}")
            print(f"  Top-10 交集:           {t10*100:.1f}%")
            print(f"  Top-20 交集:           {t20*100:.1f}%")
            print(f"  Top-50 交集:           {t50*100:.1f}%")
            print(f"  Top-300 交集:          {t300*100:.1f}%")
            print(f"  综合分平均相对差异:     {diff_pct:.2f}%")
            print(f"  动态权重一致率:         {dyn_m*100:.1f}%" if not np.isnan(dyn_m)
                  else "  动态权重一致率:         N/A")

            if not np.isnan(sp_m) and sp_m > 0.95 and t50 > 0.8:
                print(f"  ✅ 高度一致 (Spearman > 0.95, Top-50 > 80%)")
            elif not np.isnan(sp_m) and sp_m > 0.85:
                print(f"  ⚠ 中度差异 (Spearman {sp_m:.3f})，可能由权重差异导致")
            else:
                print(f"  ❌ 显著差异，需排查")

    print(f"\n{'='*70}")
    print("  对比完成")
    print(f"{'='*70}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
