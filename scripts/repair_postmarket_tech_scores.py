#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""修复昨日盘后发现的 tech_score。

从 DB 拉取 OHLCV 数据，在线跑 StockScorer 补算 tech_score，
更新 postmarket_{date}_topn.json 文件。
"""

import sys
import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def repair_postmarket_tech_scores(date_str: str = "20260513") -> None:
    reports_dir = Path(__file__).resolve().parent.parent / "reports_discovery"
    json_file = reports_dir / f"postmarket_{date_str}_topn.json"

    if not json_file.exists():
        logger.error("文件不存在: %s", json_file)
        return

    with open(json_file, encoding="utf-8") as f:
        raw_items = json.load(f)

    if not raw_items:
        logger.warning("空文件: %s", json_file)
        return

    # 检查是否已有完整技术评分（总分 + 六维明细）
    needs_repair = any(
        (d.get("tech_score") or 0) <= 0
        or (d.get("tech_score", 0) > 0 and (d.get("rr_score") or 0) <= 0)
        for d in raw_items
    )
    if not needs_repair:
        logger.info("技术评分已完整，无需修复")
        return

    from src.services.stock_scorer import StockScorer, StockScorerConfig
    from src.storage import DatabaseManager

    db = DatabaseManager()
    stock_codes = [d["stock_code"] for d in raw_items]

    # 拉取 180 日 OHLCV（与 engine.py 一致）
    ohlcv_map: Dict[str, List] = {}
    try:
        td_obj = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        ohlcv_start = td_obj - timedelta(days=180)
        ohlcv_map = db.get_data_range_batch(stock_codes, ohlcv_start, td_obj)
    except Exception as e:
        logger.warning("OHLCV 批量拉取失败: %s", e)

    # 预加载板块涨跌幅（用于 sector_score）
    spot_df = None
    try:
        spot_df = db.get_realtime_spot()
        if spot_df is not None and not spot_df.empty:
            ths_map = db.get_ths_industry_map()
            if ths_map:
                spot_c = spot_df.copy()
                spot_c["sector_name"] = spot_c.index.map(ths_map)
                sector_pct = spot_c.groupby("sector_name")["pct_chg"].mean().dropna()
    except Exception as e:
        logger.warning("板块涨跌幅预加载失败: %s", e)
        sector_pct_dict: Dict[str, float] = {}
    else:
        sector_pct_dict = sector_pct.to_dict()

    config = StockScorerConfig()
    scorer = StockScorer(config)
    if sector_pct_dict:
        scorer.preload_sector_pct(sector_pct_dict)

    updated = 0
    for item in raw_items:
        code = item["stock_code"]
        price = item.get("price_at_discovery") or item.get("buy_price_low") or item.get("buy_price_high") or 0.0
        tp1 = item.get("take_profit_1") or 0.0
        tp2 = item.get("take_profit_2") or 0.0
        stop_loss = item.get("stop_loss") or 0.0
        sector = item.get("sector") or ""
        reasons = item.get("reasons") or []

        ohlcv_rows = ohlcv_map.get(code, [])
        if not ohlcv_rows:
            logger.debug("无 OHLCV 数据: %s", code)
            continue

        highs = np.array([d.high for d in ohlcv_rows], dtype=float)
        lows = np.array([d.low for d in ohlcv_rows], dtype=float)
        closes = np.array([d.close for d in ohlcv_rows], dtype=float)

        pre_close = float(closes[-2]) if len(closes) > 1 else float(closes[-1]) if closes.size > 0 else 0.0

        # 量比
        vol_ratio = 1.0
        if len(ohlcv_rows) >= 6 and hasattr(ohlcv_rows[-1], "vol"):
            vols = np.array([d.vol for d in ohlcv_rows[-6:]], dtype=float)
            mean_vol = np.mean(vols[:-1])
            if mean_vol > 0:
                vol_ratio = float(vols[-1] / mean_vol)

        try:
            tech = scorer.score(
                stock_code=code,
                sector=sector,
                price=float(price),
                pre_close=pre_close,
                tp1=float(tp1),
                tp2=float(tp2),
                stop_loss=float(stop_loss),
                reasons=reasons,
                ohlcv=(highs, lows, closes),
                volume_ratio=vol_ratio,
            )
            item["tech_score"] = round(tech.composite, 2)
            item["rr_score"] = round(tech.rr_score, 2)
            item["market_score"] = round(tech.market_score, 2)
            item["sector_score"] = round(tech.sector_score, 2)
            item["volume_score"] = round(tech.volume_score, 2)
            item["position_score"] = round(tech.position_score, 2)
            item["formation_score"] = round(tech.formation_score, 2)
            updated += 1
            logger.info(
                "%s %s: tech_score=%.1f rr=%.1f market=%.1f sector=%.1f vol=%.1f pos=%.1f form=%.1f",
                code, item.get("stock_name", ""),
                tech.composite, tech.rr_score, tech.market_score,
                tech.sector_score, tech.volume_score,
                tech.position_score, tech.formation_score,
            )
        except Exception as e:
            logger.warning("StockScorer 失败 %s: %s", code, e)

    json_file.write_text(json.dumps(raw_items, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("修复完成: %d/%d 只股票更新了 tech_score", updated, len(raw_items))


if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y%m%d")
    repair_postmarket_tech_scores(date_str)