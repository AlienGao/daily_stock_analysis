#!/usr/bin/env python3
"""回补 limit_up_history 历史涨停数据。

从 Tushare limit_list_d API 逐日拉取涨停列表并写入 limit_up_history 表。
已存在的 (code, trade_date) 自动跳过。

用法:
    python scripts/backfill_limit_up_history.py                    # 默认 2024-01-01 至今
    python scripts/backfill_limit_up_history.py --start 20250101   # 指定起始
    python scripts/backfill_limit_up_history.py --start 20240101 --end 20260515
    python scripts/backfill_limit_up_history.py --dry-run          # 预览待回补日期
    python scripts/backfill_limit_up_history.py --sleep 0.5        # 自定义 API 间隔
"""

import argparse
import logging
import sys
import time
from datetime import date as _date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_limit_up")


def _to_date(val) -> _date:
    if isinstance(val, _date):
        return val
    s = str(val).replace("-", "").strip()[:8]
    return _date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def get_missing_dates(db, start: str, end: str) -> list:
    """返回 stock_daily 中有数据但 limit_up_history 中缺失的交易日。"""
    from sqlalchemy import text

    with db.get_session() as s:
        cached = {
            str(row[0]).replace("-", "")[:8]
            for row in s.execute(
                text("SELECT DISTINCT trade_date FROM limit_up_history")
            ).fetchall()
        }
        all_dates = [
            row[0]
            for row in s.execute(
                text(
                    "SELECT DISTINCT date FROM stock_daily "
                    "WHERE date >= :start AND date <= :end "
                    "ORDER BY date"
                ),
                {"start": start, "end": end},
            ).fetchall()
        ]

    missing = []
    for d in all_dates:
        d_str = str(d).replace("-", "").strip()[:8]
        if d_str not in cached:
            missing.append(d_str)
    return missing


def main():
    parser = argparse.ArgumentParser(description="回补 limit_up_history 历史涨停数据")
    parser.add_argument("--start", default="20240101", help="起始日期 YYYYMMDD (默认 20240101)")
    parser.add_argument("--end", default=None, help="结束日期 YYYYMMDD (默认今天)")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    parser.add_argument("--sleep", type=float, default=0.3, help="API 调用间隔秒数 (默认 0.3)")
    parser.add_argument("--max-requests", type=int, default=0, help="最大请求数 (0=无限)")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager

    db = DatabaseManager()
    tf = TushareFetcher.get_instance()
    if not tf.is_available:
        logger.error("Tushare 不可用，请检查 Token")
        sys.exit(1)

    start_fmt = f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:]}"
    if args.end:
        end_fmt = f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:]}"
    else:
        end_fmt = _date.today().strftime("%Y-%m-%d")

    missing = get_missing_dates(db, start_fmt, end_fmt)
    if not missing:
        logger.info("无缺失日期，无需回补")
        return

    if args.max_requests > 0:
        missing = missing[:args.max_requests]

    logger.info(
        "%s待回补 %d 个交易日: %s ~ %s",
        "[DRY-RUN] " if args.dry_run else "",
        len(missing), missing[0], missing[-1],
    )

    if args.dry_run:
        return

    success = 0
    total_saved = 0
    for i, trade_date in enumerate(missing):
        logger.info("[%d/%d] 回补 %s ...", i + 1, len(missing), trade_date)
        try:
            df_api = tf.get_limit_list(trade_date=trade_date, limit_type="U")
            if df_api is None or df_api.empty:
                logger.warning("  %s: Tushare 无数据", trade_date)
                continue

            df_api = df_api.reset_index()

            df_insert = pd.DataFrame()
            df_insert["code"] = df_api["ts_code"].astype(str)
            df_insert["name"] = ""
            df_insert["trade_date"] = trade_date
            df_insert["open_times"] = pd.to_numeric(
                df_api.get("open_times", 0), errors="coerce"
            ).fillna(0).astype(int)
            df_insert["limit_times"] = pd.to_numeric(
                df_api.get("limit_times", 0), errors="coerce"
            ).fillna(0).astype(int)
            df_insert["sector"] = ""

            saved = db.insert_limit_up_history_bulk(df_insert, source="tushare_backfill")
            total_saved += saved
            logger.info("  %s: %d 条", trade_date, saved)
            success += 1
        except Exception as e:
            logger.warning("  %s: 失败 - %s", trade_date, e)

        if i < len(missing) - 1:
            time.sleep(args.sleep)

    logger.info("回补完成: %d/%d 个日期成功, 共 %d 条", success, len(missing), total_saved)


if __name__ == "__main__":
    main()
