#!/usr/bin/env python3
"""回填 broker_recommend_monthly 历史券商金股推荐数据。

从 Tushare broker_recommend API 逐月拉取券商月度金股并写入本地 DB。
支持断点续跑：已缓存的月份自动跳过。

用法:
    python scripts/backfill_broker_recommend_monthly.py              # 回填全部缺失月份
    python scripts/backfill_broker_recommend_monthly.py --months N   # 仅最近 N 个月
    python scripts/backfill_broker_recommend_monthly.py --dry-run    # 预览待回填月份
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
logger = logging.getLogger("backfill_br")


def get_missing_months(db, limit_months: int = 0) -> list:
    """返回 stock_daily 中有数据但 broker_recommend_monthly 中缺失的月份列表。"""
    from sqlalchemy import text

    with db.get_session() as s:
        cached = sorted({
            row[0] for row in
            s.execute(text("SELECT DISTINCT month FROM broker_recommend_monthly")).fetchall()
        })
        all_months = sorted({
            row[0][:7].replace("-", "")
            for row in s.execute(text("SELECT DISTINCT date FROM stock_daily ORDER BY date DESC")).fetchall()
            if row[0] and "-" in str(row[0])
        })

    missing = [m for m in all_months if m not in cached]
    if limit_months > 0:
        missing = missing[-limit_months:]

    return missing


def main():
    parser = argparse.ArgumentParser(description="回填 broker_recommend_monthly 历史数据")
    parser.add_argument("--months", type=int, default=0, help="仅回填最近 N 个月")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    parser.add_argument("--delay", type=float, default=0.5, help="API 调用间隔秒数")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager

    db = DatabaseManager()
    tf = TushareFetcher.get_instance()
    if not tf.is_available:
        logger.error("Tushare 不可用，请检查 Token")
        sys.exit(1)

    missing = get_missing_months(db, limit_months=args.months)
    if not missing:
        logger.info("无缺失月份，无需回填")
        return

    logger.info(
        "%s待回填 %d 个月份: %s ~ %s",
        "[DRY-RUN] " if args.dry_run else "",
        len(missing), missing[0], missing[-1],
    )

    if args.dry_run:
        return

    success = 0
    t0 = time.time()
    for i, month in enumerate(missing):
        try:
            df = tf.get_broker_recommend(month)
            if df is None or df.empty:
                logger.warning("  [%d/%d] %s: 无数据", i + 1, len(missing), month)
                continue

            saved = db.save_broker_recommend_monthly(month, df.reset_index())
            elapsed = time.time() - t0
            eta = (elapsed / (i + 1)) * (len(missing) - i - 1) if i > 0 else 0
            logger.info(
                "[%d/%d] %s: %d 条 | ETA %.0fs",
                i + 1, len(missing), month, saved, eta,
            )
            success += 1
        except Exception as e:
            logger.warning("[%d/%d] %s: 失败 - %s", i + 1, len(missing), month, e)

        if i < len(missing) - 1 and args.delay > 0:
            time.sleep(args.delay)

    elapsed = time.time() - t0
    logger.info("回填完成: %d/%d 个月份成功, 耗时 %.0fs (%.1f 分钟)", success, len(missing), elapsed, elapsed / 60)


if __name__ == "__main__":
    main()
