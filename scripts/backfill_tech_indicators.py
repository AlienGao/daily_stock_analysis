#!/usr/bin/env python3
"""回填 stock_tech_indicator 历史技术指标缓存。

从 Tushare stk_factor API 逐日拉取全市场技术指标并写入本地 DB。
支持断点续跑：已缓存的日期自动跳过。

用法:
    python scripts/backfill_tech_indicators.py           # 回填全部缺失日期
    python scripts/backfill_tech_indicators.py --days 60  # 仅最近 60 个交易日
    python scripts/backfill_tech_indicators.py --dry-run  # 预览待回填日期
"""

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_tech")


def get_missing_dates(db, limit_days: int = 0) -> list:
    """返回 stock_daily 中有数据但 stock_tech_indicator 中缺失的日期列表。"""
    from datetime import date as _date
    from sqlalchemy import text

    def _to_date(val) -> _date:
        return val if isinstance(val, _date) else _date.fromisoformat(str(val))

    with db.get_session() as s:
        cached = {
            _to_date(row[0]) for row in
            s.execute(text("SELECT DISTINCT date FROM stock_tech_indicator")).fetchall()
        }
        all_dates = [
            _to_date(row[0]) for row in
            s.execute(
                text("SELECT DISTINCT date FROM stock_daily ORDER BY date DESC")
            ).fetchall()
        ]

    missing = []
    for d_obj in all_dates:
        if d_obj not in cached:
            missing.append(d_obj)
    if limit_days > 0:
        missing = missing[:limit_days]

    return missing


def main():
    parser = argparse.ArgumentParser(description="回填 Tushare 技术指标历史缓存")
    parser.add_argument(
        "--days", type=int, default=0,
        help="仅回填最近 N 个交易日（0=全部）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅预览待回填日期，不实际拉取"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="每次 API 调用间隔秒数（默认 1.0，避免触发限流）"
    )
    args = parser.parse_args()

    from src.storage import DatabaseManager
    db = DatabaseManager.get_instance()

    missing = get_missing_dates(db, limit_days=args.days)
    if not missing:
        logger.info("缓存已完整，无需回填")
        return 0

    logger.info("待回填日期: %d 个 (%s ~ %s)",
                 len(missing),
                 max(missing).isoformat() if missing else "N/A",
                 min(missing).isoformat() if missing else "N/A")

    if args.dry_run:
        for d in sorted(missing, reverse=True)[:20]:
            logger.info("  %s", d.isoformat())
        if len(missing) > 20:
            logger.info("  ... 共 %d 个", len(missing))
        return 0

    from data_provider.tushare_fetcher import TushareFetcher
    tf = TushareFetcher.get_instance()
    if not tf.is_available():
        logger.error("Tushare 不可用，请检查 TUSHARE_TOKEN 配置")
        return 1

    success = 0
    fail = 0
    skipped = 0
    t0 = time.time()

    for i, trade_date_obj in enumerate(sorted(missing, reverse=True), 1):
        trade_date_str = trade_date_obj.strftime("%Y%m%d")
        try:
            # 二次确认：避免同一次运行中重复写入
            with db.get_session() as s:
                from sqlalchemy import text
                exists = s.execute(
                    text("SELECT 1 FROM stock_tech_indicator WHERE date = :d LIMIT 1"),
                    {"d": trade_date_obj}
                ).scalar()
            if exists:
                skipped += 1
                continue

            df = tf.get_bulk_stk_factor(trade_date_str)
            if df is not None and not df.empty:
                success += 1
                elapsed = time.time() - t0
                eta = (elapsed / i) * (len(missing) - i) if i > 0 else 0
                logger.info(
                    "[%d/%d] %s 写入 %d 条 | 成功=%d 失败=%d | ETA %.0fs",
                    i, len(missing), trade_date_str, len(df),
                    success, fail, eta,
                )
            else:
                fail += 1
                logger.warning("[%d/%d] %s 返回空（可能非交易日或无数据）", i, len(missing), trade_date_str)
        except Exception as e:
            fail += 1
            logger.warning("[%d/%d] %s 失败: %s", i, len(missing), trade_date_str, e)

        if args.delay > 0:
            time.sleep(args.delay)

    elapsed = time.time() - t0
    logger.info(
        "回填完成: 成功=%d 失败=%d 跳过=%d 耗时 %.0fs (%.1f 分钟)",
        success, fail, skipped, elapsed, elapsed / 60,
    )
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
