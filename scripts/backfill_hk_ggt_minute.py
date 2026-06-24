#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回填港股通成份股 AkShare 1 分钟历史到 hk_ggt_minute_bar。"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_provider.akshare_fetcher import AkshareFetcher
from src.config import get_config
from src.services.hk_ggt_monitor_service import HkGgtMonitorService
from src.storage import DatabaseManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill HK GGT minute bars via AkShare")
    parser.add_argument("--trade-date", help="Only backfill codes from this component snapshot YYYYMMDD")
    parser.add_argument("--codes", help="Comma-separated HK codes (5-digit), default all components")
    parser.add_argument("--start-date", help="Minute start date YYYYMMDD")
    parser.add_argument("--sleep", type=float, help="Sleep seconds between codes")
    parser.add_argument("--force-refresh-components", action="store_true")
    parser.add_argument("--max-retries", type=int, default=3, help="AkShare fetch retries per code")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="Skip codes already in hk_ggt_minute_bar (akshare)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    config = get_config()
    start_date = (args.start_date or "20260622").replace("-", "")[:8]
    sleep_sec = args.sleep if args.sleep is not None else float(0.3 or 0.3)

    service = HkGgtMonitorService()
    trade_date = args.trade_date or service.resolve_trade_date()
    if args.force_refresh_components:
        service.refresh_components(trade_date, force=True)

    db = DatabaseManager()
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        codes = db.list_hk_ggt_codes_for_date(trade_date)
        if not codes:
            refreshed = service.refresh_components(trade_date, force=True)
            trade_date = refreshed.get("trade_date", trade_date)
            codes = db.list_hk_ggt_codes_for_date(trade_date)

    if not codes:
        logger.error("No HK GGT component codes found for trade_date=%s", trade_date)
        return 1


    fetcher = AkshareFetcher()
    total_saved = 0
    failed: list[str] = []
    for idx, code in enumerate(codes, start=1):
        rows: list = []
        for attempt in range(1, max(1, int(args.max_retries)) + 1):
            try:
                rows = fetcher.fetch_hk_ggt_minute_bars(code, start_date=start_date)
            except Exception as exc:
                logger.warning("[%d/%d] %s fetch error attempt=%d: %s", idx, len(codes), code, attempt, exc)
                rows = []
            if rows:
                break
            if attempt < args.max_retries:
                time.sleep(min(10.0, sleep_sec + attempt * 2))
        saved = len(rows)
        total_saved += saved
        if not rows:
            failed.append(code)
        logger.info("[%d/%d] %s rows=%d saved=%d", idx, len(codes), code, len(rows), saved)
        if sleep_sec > 0 and idx < len(codes):
            time.sleep(sleep_sec)

    logger.info(
        "Backfill done codes=%d total_saved=%d start_date=%s failed=%d",
        len(codes), total_saved, start_date, len(failed),
    )
    if failed:
        logger.info("Failed codes sample: %s", ",".join(failed[:20]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
