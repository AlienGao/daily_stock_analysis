#!/usr/bin/env python3
"""基准测试：模拟完整 Phase 2-5 管线（跳过 Phase 1 网络 I/O）。

从 DB 加载全市场股票 + tech_cache + OHLCV，模拟因子打分，
运行全量 tech_score + 综合分排序 + Pass 2 + Phase 4.7。
用法: python scripts/benchmark_full_pipeline.py
"""

import sys
import time
import sqlite3
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


DB_PATH = "data/stock_analysis.db"


def load_all_stocks():
    """从 DB 加载全市场股票列表。"""
    conn = sqlite3.connect(DB_PATH)
    # 最新 tech_indicators 日期
    max_date = conn.execute("SELECT MAX(date) FROM stock_tech_indicator").fetchone()[0]

    # 全部有 tech_indicators 的股票
    rows = conn.execute(
        "SELECT DISTINCT code FROM stock_tech_indicator WHERE date = ?", (max_date,)
    ).fetchall()
    codes = [r[0] for r in rows]
    conn.close()
    return codes, max_date


def main():
    print("=" * 60)
    print("完整管线基准测试（跳过 Phase 1 网络 I/O）")
    print("=" * 60)

    # --- 加载数据 ---
    print("\n[1] 加载全市场股票列表...")
    t0 = time.time()
    all_codes, trade_date = load_all_stocks()
    print(f"  {len(all_codes)} 只股票, 日期 {trade_date}, 耗时 {time.time()-t0:.1f}s")

    # 模拟 Phase 2-4: 因子打分（用随机分代替，只测 Phase 5 性能）
    print("\n[2] 模拟因子打分（Phase 2-4）...")
    t0 = time.time()
    import random
    random.seed(42)
    factor_scores = {code: random.uniform(20, 80) for code in all_codes}
    print(f"  {len(factor_scores)} 只, 耗时 {time.time()-t0:.2f}s (模拟)")

    # 构造 candidate_codes（ts_code 格式）
    candidate_codes = all_codes  # DB 里已经是裸代码

    # --- 运行 engine ---
    print("\n[3] 初始化 DiscoveryEngine...")
    t0 = time.time()

    # 直接用 StockScorer + compute_from_arrays，不走完整 discover()
    # 因为 discover() 需要 tushare_fetcher 等依赖
    from src.services.stock_scorer import StockScorer, StockScorerConfig
    from src.services.stop_loss_calculator import compute_from_arrays
    from src.storage import DatabaseManager

    scorer = StockScorer(StockScorerConfig())
    db = DatabaseManager()

    # 预加载数据
    trade_date_str = trade_date
    if len(trade_date_str) == 8:
        trade_date_str = f"{trade_date_str[:4]}-{trade_date_str[4:6]}-{trade_date_str[6:]}"

    print(f"  初始化耗时 {time.time()-t0:.1f}s")

    # Phase 4.9b: 批量 tech_cache
    print("\n[4] 批量加载 tech_cache (Phase 4.9b)...")
    t0 = time.time()
    tech_cache = db.get_tech_indicators_batch(all_codes, trade_date_str)
    print(f"  {len(tech_cache)} 只, 耗时 {time.time()-t0:.1f}s")

    # Phase 4.9c: 批量 OHLCV
    print("\n[5] 批量加载 OHLCV (Phase 4.9c)...")
    t0 = time.time()
    from datetime import datetime, timedelta
    td_obj = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
    ohlcv_start = td_obj - timedelta(days=180)
    ohlcv_map = db.get_data_range_batch(all_codes, ohlcv_start, td_obj)
    print(f"  {len(ohlcv_map)} 只, 耗时 {time.time()-t0:.1f}s")

    # Phase 4.9: 实时价格（从 RealtimeSpot 表）
    print("\n[6] 加载实时价格 (Phase 4.9)...")
    t0 = time.time()
    spot_df = db.get_current_prices(all_codes)
    live_prices = {}
    if not spot_df.empty:
        for code in all_codes:
            try:
                val = spot_df.at[code, "price"]
                if val is not None and not (isinstance(val, float) and np.isnan(val)):
                    live_prices[code] = float(val)
            except (KeyError, ValueError, TypeError):
                pass
    print(f"  {len(live_prices)} 只有实时价格, 耗时 {time.time()-t0:.1f}s")

    # 预加载板块涨跌幅
    try:
        ths_map = db.get_ths_industry_map()
        if ths_map and spot_df is not None and not spot_df.empty:
            spot_c = spot_df.copy()
            spot_c["sector_name"] = spot_c.index.map(ths_map)
            sector_pct = spot_c.groupby("sector_name")["pct_chg"].mean().dropna()
            scorer.preload_sector_pct(sector_pct.to_dict())
    except Exception:
        pass

    # ============================================================
    # Phase 5: 全量 tech_score + 综合分排序 + Pass 2
    # ============================================================
    print("\n[7] Phase 5: 全量 tech_score 计算...")
    t_phase5_start = time.time()

    alpha = 0.3
    tech_scores_map = {}
    stop_tp_map = {}
    failed = 0

    for code in all_codes:
        try:
            ohlcv_rows = ohlcv_map.get(code, [])
            if not ohlcv_rows:
                failed += 1
                continue
            highs = np.array([d.high for d in ohlcv_rows], dtype=float)
            lows = np.array([d.low for d in ohlcv_rows], dtype=float)
            closes = np.array([d.close for d in ohlcv_rows], dtype=float)

            # 盘中追加实时价格
            rt_p = live_prices.get(code)
            if rt_p and rt_p > 0:
                highs = np.append(highs, rt_p)
                lows = np.append(lows, rt_p)
                closes = np.append(closes, rt_p)

            # 精确止盈止损
            sl = compute_from_arrays(
                highs, lows, closes, code=code,
                ma20=tech_cache.get(code, {}).get("ma20"),
                ma60=tech_cache.get(code, {}).get("ma60"),
                atr=tech_cache.get(code, {}).get("atr"),
                factor_score=factor_scores.get(code, 50.0),
            )
            est_stop = sl.stop_loss or 0
            est_tp1 = sl.take_profit_1 or 0
            est_tp2 = sl.take_profit_2 or 0
            stop_tp_map[code] = (sl.buy_low, sl.buy_high, sl.stop_loss, sl.take_profit_1, sl.take_profit_2)

            price = live_prices.get(code) or float(closes[-1])
            pre_close = float(closes[-2]) if len(closes) > 1 else price

            # 量比
            vol_ratio = 1.0
            if spot_df is not None and "volume_ratio" in spot_df.columns:
                try:
                    vr = spot_df.at[code, "volume_ratio"]
                    if vr is not None and float(vr) > 0:
                        vol_ratio = float(vr)
                except (KeyError, ValueError, TypeError):
                    pass

            # 轻量 formation reason
            tc = tech_cache.get(code, {})
            lite_reasons = []
            ma5, ma10, ma20_v = tc.get("ma5"), tc.get("ma10"), tc.get("ma20")
            if ma5 and ma10 and ma20_v and ma5 > ma10 > ma20_v:
                lite_reasons.append("均线多头排列")
            if tc.get("macd", 0) > 0:
                lite_reasons.append("MACD金叉")
            rsi = tc.get("rsi_12")
            if rsi is not None and rsi < 45:
                lite_reasons.append("RSI低位回升")
            bm = tc.get("boll_mid")
            if bm and price > bm:
                lite_reasons.append("站上BOLL中轨")
            if vol_ratio > 1.2:
                lite_reasons.append("成交量放大")

            tech = scorer.score(
                stock_code=code, sector="", price=price, pre_close=pre_close,
                tp1=est_tp1, tp2=est_tp2, stop_loss=est_stop,
                reasons=lite_reasons, ohlcv=(highs, lows, closes), volume_ratio=vol_ratio,
            )
            tech_scores_map[code] = tech.composite
        except Exception:
            failed += 1

    t_tech = time.time() - t_phase5_start
    print(f"  评分完成: {len(tech_scores_map)} 只, 失败 {failed} 只, 耗时 {t_tech:.1f}s")

    # 综合分排序
    print("\n[8] 综合分排序 + 取 top_n...")
    t0 = time.time()
    scored = []
    for code in all_codes:
        tech = tech_scores_map.get(code, 0.0)
        factor = factor_scores.get(code, 50.0)
        composite = alpha * factor + (1 - alpha) * tech
        scored.append((composite, code, factor, tech))
    scored.sort(key=lambda x: x[0], reverse=True)
    top_n = scored[:300]
    print(f"  排序耗时 {time.time()-t0:.2f}s")
    print(f"  Top 5: {', '.join(f'{c}({s:.1f})' for s, c, _, _ in top_n[:5])}")

    # Pass 2: 构建结果
    print("\n[9] Pass 2: 构建 top_n 结果...")
    t0 = time.time()
    results = []
    for composite, code, factor, tech in top_n:
        cached = stop_tp_map.get(code)
        if cached:
            buy_low, buy_high, stop, tp1, tp2 = cached
        else:
            buy_low = buy_high = stop = tp1 = tp2 = None

        # 过滤超买/低盈亏比
        price = live_prices.get(code) or 0
        if price and tp1 and price >= tp1:
            continue
        if price and tp1 and stop:
            if price <= stop:
                continue
            pnl_ratio = (tp1 - price) / (price - stop)
            if pnl_ratio <= 0:
                continue

        results.append({
            "code": code, "composite": composite,
            "factor": factor, "tech": tech,
            "stop": stop, "tp1": tp1,
        })
    print(f"  Pass 2 耗时 {time.time()-t0:.2f}s, 结果 {len(results)} 只")

    # Phase 4.7: 用完整 reason 重算 tech_score
    print("\n[10] Phase 4.7: 精确重算 top_n tech_score...")
    t0 = time.time()
    for r in results:
        code = r["code"]
        ohlcv_rows = ohlcv_map.get(code, [])
        if not ohlcv_rows:
            continue
        highs = np.array([d.high for d in ohlcv_rows], dtype=float)
        lows = np.array([d.low for d in ohlcv_rows], dtype=float)
        closes = np.array([d.close for d in ohlcv_rows], dtype=float)
        price = live_prices.get(code) or float(closes[-1])
        pre_close = float(closes[-2]) if len(closes) > 1 else price

        vol_ratio = 1.0
        if spot_df is not None and "volume_ratio" in spot_df.columns:
            try:
                vr = spot_df.at[code, "volume_ratio"]
                if vr is not None and float(vr) > 0:
                    vol_ratio = float(vr)
            except (KeyError, ValueError, TypeError):
                pass

        # 完整 reason
        tc = tech_cache.get(code, {})
        full_reasons = []
        ma5, ma10, ma20_v = tc.get("ma5"), tc.get("ma10"), tc.get("ma20")
        if ma5 and ma10 and ma20_v and ma5 > ma10 > ma20_v:
            full_reasons.append("均线多头排列")
        if tc.get("macd", 0) > 0:
            full_reasons.append("MACD金叉")
        rsi = tc.get("rsi_12")
        if rsi is not None and rsi < 45:
            full_reasons.append("RSI低位回升")
        bm = tc.get("boll_mid")
        if bm and price > bm:
            full_reasons.append("站上BOLL中轨")
        if vol_ratio > 1.2:
            full_reasons.append("成交量放大")

        try:
            tech = scorer.score(
                stock_code=code, sector="", price=price, pre_close=pre_close,
                tp1=r["tp1"] or 0, tp2=(r["tp1"] or 0) * 1.05, stop_loss=r["stop"] or 0,
                reasons=full_reasons, ohlcv=(highs, lows, closes), volume_ratio=vol_ratio,
            )
            r["tech_final"] = tech.composite
        except Exception:
            pass
    print(f"  Phase 4.7 耗时 {time.time()-t0:.2f}s")

    # 汇总
    total = time.time() - t_phase5_start
    print(f"\n{'='*60}")
    print(f"汇总（跳过 Phase 1 网络 I/O）:")
    print(f"  Phase 4.9b tech_cache:  预加载")
    print(f"  Phase 4.9c OHLCV:       预加载")
    print(f"  Phase 5 全量 tech_score: {t_tech:.1f}s ({len(tech_scores_map)} 只)")
    print(f"  综合分排序 + Pass 2:     <1s")
    print(f"  Phase 4.7 精确重算:      <1s")
    print(f"  Phase 5 总耗时:          {total:.1f}s")
    print(f"\n结论: 全市场 {len(all_codes)} 只，Phase 5（核心管线）耗时 {total:.1f}s")


if __name__ == "__main__":
    main()
