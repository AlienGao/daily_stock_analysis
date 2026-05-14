#!/usr/bin/env python3
"""对比三种 tech_score 计算方式对排序的影响。

1. ATR 估算止盈止损 + 无 reason
2. 精确止盈止损 + 无 reason
3. 精确止盈止损 + 轻量 formation reason（从 tech_cache 推导）

直接从数据库加载数据，不走 discover() 管线，避免网络依赖。
用法: python scripts/compare_scoring_methods.py
"""

import sys
import time
import sqlite3
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.services.stock_scorer import StockScorer, StockScorerConfig
from src.services.stop_loss_calculator import compute_from_arrays


DB_PATH = "data/stock_analysis.db"


def load_data():
    """从数据库加载 tech_indicators 和 stock_daily 数据。"""
    conn = sqlite3.connect(DB_PATH)

    # 最新 tech_indicators 日期
    max_date = conn.execute("SELECT MAX(date) FROM stock_tech_indicator").fetchone()[0]
    print(f"  tech_indicators 日期: {max_date}")

    # 加载 tech_indicators
    cols = "code, close_qfq, ma5, ma10, ma20, ma60, atr, rsi_12, macd, boll_upper, boll_mid, boll_lower, vol"
    rows = conn.execute(
        f"SELECT {cols} FROM stock_tech_indicator WHERE date = ?", (max_date,)
    ).fetchall()
    tech_cache = {}
    for r in rows:
        code = r[0]
        tech_cache[code] = {
            "close_qfq": r[1], "ma5": r[2], "ma10": r[3], "ma20": r[4], "ma60": r[5],
            "atr": r[6], "rsi_12": r[7], "macd": r[8],
            "boll_upper": r[9], "boll_mid": r[10], "boll_lower": r[11], "vol": r[12],
        }
    print(f"  tech_cache: {len(tech_cache)} 只")

    # 加载 stock_daily (180 天 OHLCV)
    from datetime import datetime, timedelta
    dt = datetime.strptime(max_date, "%Y-%m-%d").date()
    start = dt - timedelta(days=180)

    codes = list(tech_cache.keys())

    ohlcv_map = {}
    placeholders = ",".join("?" * len(codes))
    rows = conn.execute(
        f"SELECT code, date, high, low, close, volume FROM stock_daily "
        f"WHERE code IN ({placeholders}) AND date >= ? ORDER BY code, date",
        codes + [str(start)],
    ).fetchall()

    from collections import defaultdict
    by_code = defaultdict(list)
    for code, date, high, low, close, vol in rows:
        by_code[code].append(type("OHLCV", (), {"high": high, "low": low, "close": close, "vol": vol}))

    for code in codes:
        if code in by_code and len(by_code[code]) >= 20:
            ohlcv_map[code] = by_code[code]

    print(f"  ohlcv_map: {len(ohlcv_map)} 只 (>=20 天数据)")

    # 因子分模拟
    factor_scores = {}
    for code, tc in tech_cache.items():
        rsi = tc.get("rsi_12") or 50
        bu, bl = tc.get("boll_upper"), tc.get("boll_lower")
        price = tc.get("close_qfq") or 0
        if bu and bl and bu > bl and price > 0:
            boll_pos = (price - bl) / (bu - bl)
            pos_score = 100 - abs(boll_pos - 0.5) * 200
        else:
            pos_score = 50
        if 40 <= rsi <= 60:
            rsi_score = 60 + abs(rsi - 50)
        elif rsi < 40:
            rsi_score = 40 + rsi * 0.5
        else:
            rsi_score = max(20, 100 - (rsi - 60) * 1.5)
        factor_scores[code] = 0.5 * pos_score + 0.5 * rsi_score

    conn.close()
    return tech_cache, ohlcv_map, factor_scores, max_date


def generate_lite_reasons(tc, price, vol_ratio):
    """从 tech_cache 推导轻量 formation reason（零额外查询）。"""
    reasons = []
    ma5, ma10, ma20_v = tc.get("ma5"), tc.get("ma10"), tc.get("ma20")
    if ma5 and ma10 and ma20_v and ma5 > ma10 > ma20_v:
        reasons.append("均线多头排列")
    if tc.get("macd", 0) > 0:
        reasons.append("MACD金叉")
    rsi = tc.get("rsi_12")
    if rsi is not None and rsi < 45:
        reasons.append("RSI低位回升")
    bu, bm, bl = tc.get("boll_upper"), tc.get("boll_mid"), tc.get("boll_lower")
    if bm and price > bm:
        reasons.append("站上BOLL中轨")
    if vol_ratio > 1.2:
        reasons.append("成交量放大")
    return reasons


def main():
    print("=" * 70)
    print("tech_score 排序对比: ATR估算 / 精确无reason / 精确+轻量reason")
    print("=" * 70)

    print("\n[1/4] 加载数据...")
    t0 = time.time()
    tech_cache, ohlcv_map, factor_scores, trade_date = load_data()
    print(f"  耗时 {time.time()-t0:.1f}s")

    print("\n[2/4] 初始化 StockScorer...")
    scorer = StockScorer(StockScorerConfig())
    print("  完成")

    common_codes = set(ohlcv_map.keys()) & set(tech_cache.keys())
    print(f"\n[3/4] 计算 tech_score ({len(common_codes)} 只)...")

    # --- 方法 1: ATR 估算止盈止损 + 无 reason ---
    t0 = time.time()
    scores_atr = {}
    for code in common_codes:
        ohlcv_rows = ohlcv_map[code]
        highs = np.array([d.high for d in ohlcv_rows], dtype=float)
        lows = np.array([d.low for d in ohlcv_rows], dtype=float)
        closes = np.array([d.close for d in ohlcv_rows], dtype=float)
        price = float(closes[-1])
        pre_close = float(closes[-2]) if len(closes) > 1 else price

        atr_val = tech_cache[code].get("atr")
        if atr_val and atr_val > 0:
            est_stop = price - 2 * atr_val
            est_tp1 = price + 4 * atr_val
        else:
            est_stop = price * 0.92
            est_tp1 = price * 1.12

        try:
            tech = scorer.score(
                stock_code=code, sector="", price=price, pre_close=pre_close,
                tp1=est_tp1, tp2=est_tp1 * 1.05, stop_loss=est_stop,
                reasons=[], ohlcv=(highs, lows, closes), volume_ratio=1.0,
            )
            scores_atr[code] = tech.composite
        except Exception:
            pass
    t_atr = time.time() - t0
    print(f"  方法1 ATR估算+无reason: {len(scores_atr)} 只, {t_atr:.1f}s")

    # --- 方法 2: 精确止盈止损 + 无 reason ---
    t0 = time.time()
    scores_exact_no_reason = {}
    stop_tp_cache = {}
    for code in common_codes:
        ohlcv_rows = ohlcv_map[code]
        highs = np.array([d.high for d in ohlcv_rows], dtype=float)
        lows = np.array([d.low for d in ohlcv_rows], dtype=float)
        closes = np.array([d.close for d in ohlcv_rows], dtype=float)
        price = float(closes[-1])
        pre_close = float(closes[-2]) if len(closes) > 1 else price

        sl = compute_from_arrays(
            highs, lows, closes, code=code,
            ma20=tech_cache[code].get("ma20"),
            ma60=tech_cache[code].get("ma60"),
            atr=tech_cache[code].get("atr"),
            factor_score=factor_scores.get(code, 50.0),
        )
        est_stop = sl.stop_loss or 0
        est_tp1 = sl.take_profit_1 or 0
        est_tp2 = sl.take_profit_2 or 0
        stop_tp_cache[code] = (est_stop, est_tp1, est_tp2)

        try:
            tech = scorer.score(
                stock_code=code, sector="", price=price, pre_close=pre_close,
                tp1=est_tp1, tp2=est_tp2, stop_loss=est_stop,
                reasons=[], ohlcv=(highs, lows, closes), volume_ratio=1.0,
            )
            scores_exact_no_reason[code] = tech.composite
        except Exception:
            pass
    t_exact = time.time() - t0
    print(f"  方法2 精确+无reason: {len(scores_exact_no_reason)} 只, {t_exact:.1f}s")

    # --- 方法 3: 精确止盈止损 + 轻量 formation reason ---
    t0 = time.time()
    scores_exact_lite_reason = {}
    for code in common_codes:
        ohlcv_rows = ohlcv_map[code]
        highs = np.array([d.high for d in ohlcv_rows], dtype=float)
        lows = np.array([d.low for d in ohlcv_rows], dtype=float)
        closes = np.array([d.close for d in ohlcv_rows], dtype=float)
        price = float(closes[-1])
        pre_close = float(closes[-2]) if len(closes) > 1 else price

        est_stop, est_tp1, est_tp2 = stop_tp_cache.get(code, (0, 0, 0))
        if est_stop == 0:
            continue

        # 成交量比率
        vol_ratio = 1.0
        tc = tech_cache.get(code, {})
        avg_vol = tc.get("vol")
        if avg_vol and avg_vol > 0 and len(ohlcv_rows) > 1:
            last_vol = ohlcv_rows[-1].vol
            if last_vol and last_vol > 0:
                vol_ratio = last_vol / avg_vol

        lite_reasons = generate_lite_reasons(tc, price, vol_ratio)

        try:
            tech = scorer.score(
                stock_code=code, sector="", price=price, pre_close=pre_close,
                tp1=est_tp1, tp2=est_tp2, stop_loss=est_stop,
                reasons=lite_reasons, ohlcv=(highs, lows, closes), volume_ratio=vol_ratio,
            )
            scores_exact_lite_reason[code] = tech.composite
        except Exception:
            pass
    t_lite = time.time() - t0
    print(f"  方法3 精确+轻量reason: {len(scores_exact_lite_reason)} 只, {t_lite:.1f}s")

    # --- 方法 4: 精确止盈止损 + 真实 reason（基准）---
    # 从 discovery 历史获取真实 reason，模拟完整管线
    t0 = time.time()
    scores_exact_full_reason = {}
    for code in common_codes:
        ohlcv_rows = ohlcv_map[code]
        highs = np.array([d.high for d in ohlcv_rows], dtype=float)
        lows = np.array([d.low for d in ohlcv_rows], dtype=float)
        closes = np.array([d.close for d in ohlcv_rows], dtype=float)
        price = float(closes[-1])
        pre_close = float(closes[-2]) if len(closes) > 1 else price

        est_stop, est_tp1, est_tp2 = stop_tp_cache.get(code, (0, 0, 0))
        if est_stop == 0:
            continue

        # 模拟完整 reason（5 个维度全部生成）
        tc = tech_cache.get(code, {})
        vol_ratio = 1.0
        avg_vol = tc.get("vol")
        if avg_vol and avg_vol > 0 and len(ohlcv_rows) > 1:
            last_vol = ohlcv_rows[-1].vol
            if last_vol and last_vol > 0:
                vol_ratio = last_vol / avg_vol

        full_reasons = []
        ma5, ma10, ma20_v = tc.get("ma5"), tc.get("ma10"), tc.get("ma20")
        if ma5 and ma10 and ma20_v:
            if ma5 > ma10 > ma20_v:
                full_reasons.append("均线多头排列")
            elif ma5 < ma10 < ma20_v:
                full_reasons.append("均线空头排列")
        if tc.get("macd", 0) > 0:
            full_reasons.append("MACD金叉")
        rsi = tc.get("rsi_12")
        if rsi is not None:
            if rsi < 30:
                full_reasons.append("RSI超卖")
            elif rsi < 45:
                full_reasons.append("RSI低位回升")
            elif rsi > 70:
                full_reasons.append("RSI超买")
        bu, bm, bl = tc.get("boll_upper"), tc.get("boll_mid"), tc.get("boll_lower")
        if bm and price > bm:
            full_reasons.append("站上BOLL中轨")
        if bu and price > bu:
            full_reasons.append("突破BOLL上轨")
        if bl and price < bl:
            full_reasons.append("跌破BOLL下轨")
        if vol_ratio > 1.5:
            full_reasons.append("成交量显著放大")
        elif vol_ratio > 1.2:
            full_reasons.append("成交量放大")
        elif vol_ratio < 0.5:
            full_reasons.append("成交量萎缩")
        # RR reason
        if est_stop > 0 and est_tp1 > 0:
            risk = price - est_stop
            reward = est_tp1 - price
            if risk > 0:
                rr = reward / risk
                if rr >= 3:
                    full_reasons.append("盈亏比优秀")
                elif rr >= 2:
                    full_reasons.append("盈亏比良好")

        try:
            tech = scorer.score(
                stock_code=code, sector="", price=price, pre_close=pre_close,
                tp1=est_tp1, tp2=est_tp2, stop_loss=est_stop,
                reasons=full_reasons, ohlcv=(highs, lows, closes), volume_ratio=vol_ratio,
            )
            scores_exact_full_reason[code] = tech.composite
        except Exception:
            pass
    t_full = time.time() - t0
    print(f"  方法4 精确+完整reason: {len(scores_exact_full_reason)} 只, {t_full:.1f}s")

    # --- 对比 ---
    print(f"\n[4/4] 对比分析...")
    alpha = 0.3

    def rank_by(scores):
        ranked = []
        for code in set(scores.keys()) & set(factor_scores.keys()):
            factor = factor_scores.get(code, 50.0)
            composite = alpha * factor + (1 - alpha) * scores[code]
            ranked.append((composite, code))
        ranked.sort(key=lambda x: x[0], reverse=True)
        return ranked

    ranked_atr = rank_by(scores_atr)
    ranked_exact = rank_by(scores_exact_no_reason)
    ranked_lite = rank_by(scores_exact_lite_reason)
    ranked_full = rank_by(scores_exact_full_reason)

    def overlap(r1, r2, n=300):
        s1 = {x[1] for x in r1[:n]}
        s2 = {x[1] for x in r2[:n]}
        return len(s1 & s2)

    print(f"\n  Top 300 重叠度（以完整reason为基准）:")
    o300_atr = overlap(ranked_atr, ranked_full)
    o300_noreason = overlap(ranked_exact, ranked_full)
    o300_lite = overlap(ranked_lite, ranked_full)
    print(f"    ATR估算  vs 完整reason:   {o300_atr}/300 ({o300_atr/300*100:.1f}%)")
    print(f"    无reason vs 完整reason:   {o300_noreason}/300 ({o300_noreason/300*100:.1f}%)")
    print(f"    轻量reason vs 完整reason: {o300_lite}/300 ({o300_lite/300*100:.1f}%)")

    for top_n in [100, 300, 500]:
        o_atr = overlap(ranked_atr, ranked_full, top_n)
        o_nr = overlap(ranked_exact, ranked_full, top_n)
        o_lite = overlap(ranked_lite, ranked_full, top_n)
        print(f"\n  Top {top_n} (vs 完整reason):")
        print(f"    ATR估算:   {o_atr}/{top_n} ({o_atr/top_n*100:.1f}%)")
        print(f"    无reason:  {o_nr}/{top_n} ({o_nr/top_n*100:.1f}%)")
        print(f"    轻量reason: {o_lite}/{top_n} ({o_lite/top_n*100:.1f}%)")

    # tech_score 相关性
    common4 = set(scores_atr.keys()) & set(scores_exact_no_reason.keys()) & set(scores_exact_lite_reason.keys()) & set(scores_exact_full_reason.keys())
    vals_atr = [scores_atr[c] for c in common4]
    vals_nr = [scores_exact_no_reason[c] for c in common4]
    vals_lite = [scores_exact_lite_reason[c] for c in common4]
    vals_full = [scores_exact_full_reason[c] for c in common4]

    corr_a_f = np.corrcoef(vals_atr, vals_full)[0, 1]
    corr_nr_f = np.corrcoef(vals_nr, vals_full)[0, 1]
    corr_l_f = np.corrcoef(vals_lite, vals_full)[0, 1]

    print(f"\n  tech_score 相关系数（vs 完整reason）:")
    print(f"    ATR估算:    {corr_a_f:.4f}")
    print(f"    无reason:   {corr_nr_f:.4f}")
    print(f"    轻量reason: {corr_l_f:.4f}")

    # 各方法 vs 完整reason 的差异
    for label, scores_comp in [("无reason", scores_exact_no_reason), ("轻量reason", scores_exact_lite_reason)]:
        diffs = [abs(scores_comp[c] - scores_exact_full_reason[c]) for c in common4 if c in scores_comp and c in scores_exact_full_reason]
        if not diffs:
            continue
        print(f"\n  {label} vs 完整reason 差异 ({len(diffs)} 只):")
        print(f"    平均绝对差异: {np.mean(diffs):.2f}")
        print(f"    中位差异: {np.median(diffs):.2f}")
        print(f"    最大差异: {np.max(diffs):.2f}")
        print(f"    差异>5分: {sum(1 for d in diffs if d > 5)} 只 ({sum(1 for d in diffs if d > 5)/len(diffs)*100:.1f}%)")
        print(f"    差异>10分: {sum(1 for d in diffs if d > 10)} 只 ({sum(1 for d in diffs if d > 10)/len(diffs)*100:.1f}%)")

    # 性能对比
    print(f"\n{'='*70}")
    print(f"性能对比:")
    print(f"  方法1 ATR估算+无reason:    {t_atr:.1f}s")
    print(f"  方法2 精确+无reason:        {t_exact:.1f}s")
    print(f"  方法3 精确+轻量reason:      {t_lite:.1f}s")
    print(f"  方法4 精确+完整reason:      {t_full:.1f}s")

    # 结论
    print(f"\n{'='*70}")
    print(f"结论 (以完整reason为基准 Top 300):")
    print(f"  无reason:   {o300_noreason}/300 ({o300_noreason/300*100:.1f}%)")
    print(f"  轻量reason: {o300_lite}/300 ({o300_lite/300*100:.1f}%)")
    improvement = o300_lite - o300_noreason
    print(f"  轻量reason 比 无reason 提升: {improvement:+d} 只 ({improvement/300*100:+.1f}%)")
    if o300_lite >= 285:
        print("  轻量reason 接近完整reason，可安全使用")
    elif o300_lite >= 270:
        print("  轻量reason 有改善，建议使用")
    else:
        print("  轻量reason 改善有限，需进一步调优")


if __name__ == "__main__":
    main()
