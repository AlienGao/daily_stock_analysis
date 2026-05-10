#!/usr/bin/env python3
"""回填 performance_report 历史业绩报表数据。

从 akshare stock_yjbb_em API 按季度拉取全市场 A 股业绩数据并写入本地 DB。
支持断点续跑：已缓存的季度自动跳过。

用法:
    python scripts/backfill_performance_report.py              # 回填近 4 个季度
    python scripts/backfill_performance_report.py --quarters 8 # 回填近 8 个季度
    python scripts/backfill_performance_report.py --dry-run    # 预览待回填季度
"""

import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_perf")


def _quarter_end_dates(ref_date_str: str, n: int = 4):
    """返回 ref_date 之前最近 n 个季度末日期 (YYYYMMDD)。"""
    d = date(int(ref_date_str[:4]), int(ref_date_str[4:6]), int(ref_date_str[6:8]))
    quarters = []
    cursor = d.replace(day=1)
    while len(quarters) < n:
        m = cursor.month
        if m <= 3:
            q_end = date(cursor.year, 3, 31)
        elif m <= 6:
            q_end = date(cursor.year, 6, 30)
        elif m <= 9:
            q_end = date(cursor.year, 9, 30)
        else:
            q_end = date(cursor.year, 12, 31)
        if q_end <= d and q_end.strftime("%Y%m%d") not in quarters:
            quarters.append(q_end.strftime("%Y%m%d"))
        if m == 1:
            cursor = date(cursor.year - 1, 12, 1)
        else:
            cursor = date(cursor.year, m - 1, 1)
    return quarters


def main():
    parser = argparse.ArgumentParser(description="回填 performance_report 历史数据")
    parser.add_argument("--quarters", type=int, default=4, help="回填最近 N 个季度")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    args = parser.parse_args()

    from data_provider.akshare_fetcher import AkshareFetcher
    from src.storage import DatabaseManager

    db = DatabaseManager()
    af = AkshareFetcher()

    today = date.today().strftime("%Y%m%d")
    periods = _quarter_end_dates(today, args.quarters)

    todo = []
    for p in periods:
        existing = db.get_performance_report(p)
        if not existing.empty:
            logger.info("  %s: 已有 %d 条，跳过", p, len(existing))
        else:
            todo.append(p)

    if not todo:
        logger.info("所有季度已缓存，无需回填")
        return

    logger.info(
        "%s待回填 %d 个季度: %s",
        "[DRY-RUN] " if args.dry_run else "",
        len(todo),
        " → ".join(todo),
    )

    if args.dry_run:
        return

    success = 0
    for i, period in enumerate(todo):
        logger.info("[%d/%d] 回填 %s ...", i + 1, len(todo), period)
        try:
            df = af.get_performance_report_quarter(period)
            if df is None or df.empty:
                logger.warning("  %s: akshare 无数据", period)
                continue
            saved = db.upsert_performance_report(df, period, source="akshare_backfill")
            logger.info("  %s: %d 条", period, saved)
            success += 1
        except Exception as e:
            logger.warning("  %s: 失败 - %s", period, e)

        if i < len(todo) - 1:
            time.sleep(1.0)

    logger.info("回填完成: %d/%d 个季度成功", success, len(todo))


if __name__ == "__main__":
    main()
