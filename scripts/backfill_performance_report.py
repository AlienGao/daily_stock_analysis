#!/usr/bin/env python3
"""回填 performance_report 历史业绩报表数据。

从 akshare stock_yjbb_em 逐季度拉取全市场 EPS/ROE/毛利率等并写入 DB。
支持断点续跑：已缓存的季度自动跳过。

用法:
    python scripts/backfill_performance_report.py              # 回填全部缺失季度
    python scripts/backfill_performance_report.py --years 5    # 仅回填最近 5 年
    python scripts/backfill_performance_report.py --dry-run    # 预览待回填季度
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_performance_report")


def _generate_quarters(start_year: int, end_year: int) -> list:
    """生成 start_year 到 end_year 的所有季度末日期 (YYYYMMDD)。"""
    quarters = []
    for y in range(start_year, end_year + 1):
        for m, d in [(3, 31), (6, 30), (9, 30), (12, 31)]:
            quarters.append(f"{y}{m:02d}{d:02d}")
    return quarters


def main():
    parser = argparse.ArgumentParser(description="回填 performance_report 历史数据")
    parser.add_argument("--years", type=int, default=0, help="仅回填最近 N 年（默认：全部 10 年）")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    args = parser.parse_args()

    from data_provider.akshare_fetcher import AkshareFetcher
    from src.storage import DatabaseManager
    from sqlalchemy import text

    db = DatabaseManager()
    af = AkshareFetcher()

    # 读取已缓存季度
    with db.get_session() as s:
        cached = {
            row[0] for row in
            s.execute(text("SELECT DISTINCT report_period FROM performance_report")).fetchall()
        }
    logger.info("已缓存 %d 个季度", len(cached))

    # 生成待回填季度列表
    from datetime import date as _date
    today = _date.today()
    end_year = today.year
    if today.month < 4:
        end_year -= 1

    start_year = end_year - (args.years - 1) if args.years > 0 else 2016
    all_quarters = _generate_quarters(start_year, end_year)

    # 只保留未缓存的 + 未超过当前日期的
    missing = []
    for q in all_quarters:
        if q not in cached and int(q) <= int(today.strftime("%Y%m%d")):
            missing.append(q)

    if not missing:
        logger.info("无缺失季度，无需回填")
        return

    logger.info(
        "%s待回填 %d 个季度: %s ~ %s",
        "[DRY-RUN] " if args.dry_run else "",
        len(missing), missing[0], missing[-1],
    )

    if args.dry_run:
        return

    success = 0
    for i, period in enumerate(missing):
        logger.info("[%d/%d] 回填 %s ...", i + 1, len(missing), period)
        try:
            df = af.get_performance_report(date=period)
            if df is None or df.empty:
                logger.warning("  %s: akshare 无数据", period)
                continue
            saved = db.upsert_performance_report(df, period, source="akshare_backfill")
            logger.info("  %s: %d 条", period, saved)
            success += 1
        except Exception as e:
            logger.warning("  %s: 失败 - %s", period, e)

        if i < len(missing) - 1:
            time.sleep(0.3)

    logger.info("回填完成: %d/%d 个季度成功", success, len(missing))


if __name__ == "__main__":
    main()
