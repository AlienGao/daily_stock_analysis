#!/usr/bin/env python3
"""回填 repurchase 历史回购数据。

从 Tushare repurchase API 按月拉取近 10 年数据并写入本地 DB。
API 单次上限 ~2000 条，按月拆分确保不超限。

用法:
    python scripts/backfill_repurchase.py              # 回填全部 10 年
    python scripts/backfill_repurchase.py --years 3    # 仅近 3 年
    python scripts/backfill_repurchase.py --dry-run    # 预览待回填月份
"""

import argparse
import logging
import sys
import time
from datetime import date as _date
from datetime import timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_repurchase")


def get_month_ranges(years: int = 10) -> list:
    """返回 (start_date, end_date) 的月度区间列表，从最早到最近。"""
    end = _date.today()
    start = end.replace(year=end.year - years)
    ranges = []
    cursor = start
    while cursor < end:
        month_end = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = month_end - timedelta(days=1)
        if month_end > end:
            month_end = end
        ranges.append((
            cursor.strftime("%Y%m%d"),
            month_end.strftime("%Y%m%d"),
        ))
        cursor = month_end + timedelta(days=1)
    return ranges


def main():
    parser = argparse.ArgumentParser(description="回填 repurchase 历史回购数据")
    parser.add_argument("--years", type=int, default=10, help="回填年数 (默认 10)")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager

    db = DatabaseManager()
    tf = TushareFetcher.get_instance()
    if not tf.is_available:
        logger.error("Tushare 不可用，请检查 Token")
        sys.exit(1)

    month_ranges = get_month_ranges(years=args.years)
    logger.info(
        f"{'[DRY-RUN] ' if args.dry_run else ''}"
        f"回填范围: {month_ranges[0][0]} ~ {month_ranges[-1][1]} "
        f"({len(month_ranges)} 个月)"
    )

    if args.dry_run:
        return

    total = 0
    for i, (start_date, end_date) in enumerate(month_ranges):
        label = f"{start_date[:6]}"
        logger.info(f"[{i+1}/{len(month_ranges)}] {label} ({start_date}~{end_date}) ...")
        try:
            df = tf.get_repurchase(start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                logger.info(f"  {label}: 无数据")
                continue
            saved = db.upsert_repurchase(df, source="tushare_backfill")
            total += saved
            logger.info(f"  {label}: API 返回 {len(df)} 条, 新增 {saved} 条")
        except Exception as e:
            logger.warning(f"  {label}: 失败 - {e}")

        if i < len(month_ranges) - 1:
            time.sleep(0.3)

    logger.info(f"回填完成: 共 {total} 条新增")


if __name__ == "__main__":
    main()
