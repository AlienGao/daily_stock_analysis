#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回填港股通成份股新浪日 K 线到 hk_stock_daily。"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import akshare as ak
from data_provider.akshare_fetcher import AkshareFetcher
from src.services.hk_stock_service import HkStockService
from src.storage import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill HK stock daily klines via Sina")
    parser.add_argument("--start-date", default="20260101", help="起始日 YYYYMMDD")
    parser.add_argument("--end-date", help="截止日 YYYYMMDD")
    parser.add_argument("--codes", help="逗号分隔的港股代码，默认全部成份股")
    parser.add_argument("--sleep", type=float, default=0.5, help="每次请求间隔秒数")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="跳过已有数据")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    service = HkStockService()
    db = DatabaseManager()

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        trade_date = db.get_latest_hk_ggt_trade_date()
        if not trade_date:
            logger.error("No HK component trade date found")
            return 1
        codes = db.list_hk_ggt_codes_for_date(trade_date)
        if not codes:
            logger.error("No HK component codes found for %s", trade_date)
            return 1

    if args.skip_existing:
        existing = set()
        for code in codes:
            norm = str(code).lower().replace("hk", "").zfill(5)
            latest = db.get_latest_hk_stock_daily_trade_date(norm)
            if latest and latest >= args.start_date.replace("-", "")[:8]:
                existing.add(norm)
        before = len(codes)
        codes = [c for c in codes if str(c).lower().replace("hk", "").zfill(5) not in existing]
        logger.info("Skip existing: %d/%d codes remain (%d already have data)",
                    len(codes), before, len(existing))

    if not codes:
        logger.info("All codes already backfilled")
        return 0

    fetcher = AkshareFetcher()
    total_saved = 0
    failed: list[str] = []

    for idx, raw_code in enumerate(codes, start=1):
        norm = str(raw_code).lower().replace("hk", "").zfill(5)
        try:
            fetcher._set_random_user_agent()
            fetcher._enforce_rate_limit()
            df = ak.stock_hk_daily(symbol=norm, adjust="")
        except Exception as exc:
            logger.warning("[%d/%d] %s fetch error: %s", idx, len(codes), norm, exc)
            failed.append(norm)
            continue

        if df is None or df.empty:
            logger.warning("[%d/%d] %s empty", idx, len(codes), norm)
            failed.append(norm)
            if args.sleep > 0:
                time.sleep(args.sleep)
            continue

        rows = []
        start_date = args.start_date.replace("-", "")[:8]
        end_date = args.end_date.replace("-", "")[:8] if args.end_date else "20991231"
        for _, row in df.iterrows():
            raw_date = row.get("date")
            if hasattr(raw_date, "strftime"):
                trade_date = raw_date.strftime("%Y%m%d")
            else:
                trade_date = str(raw_date).replace("-", "")[:8]
            if not trade_date or trade_date < start_date or trade_date > end_date:
                continue
            close_val = _safe_float(row.get("close"))
            if close_val is None:
                continue
            open_val = _safe_float(row.get("open"))
            high_val = _safe_float(row.get("high"))
            low_val = _safe_float(row.get("low"))
            volume_val = _safe_float(row.get("vol", row.get("volume")))
            rows.append({
                "hk_code": norm,
                "trade_date": trade_date,
                "open": open_val if open_val and open_val > 0 else None,
                "high": high_val,
                "low": low_val,
                "close": close_val,
                "volume": volume_val,
            })

        if rows:
            saved = db.upsert_hk_stock_daily_bars(rows)
            total_saved += saved
            logger.info("[%d/%d] %s saved=%d bars (%s ~ %s)",
                        idx, len(codes), norm, saved, rows[0]["trade_date"], rows[-1]["trade_date"])
        else:
            logger.warning("[%d/%d] %s no rows in date range", idx, len(codes), norm)

        if args.sleep > 0 and idx < len(codes):
            time.sleep(args.sleep)

    logger.info("Backfill done: %d codes, %d bars saved, %d failed",
                len(codes), total_saved, len(failed))
    if failed:
        logger.info("Failed sample: %s", ",".join(failed[:20]))
    return 0


def _safe_float(v):
    if v is None:
        return None
    try:
        fv = float(v)
        return fv if fv == fv and abs(fv) < 1e12 else None
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())
