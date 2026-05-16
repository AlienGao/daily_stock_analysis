#!/usr/bin/env python3
"""回填 limit_pool 历史涨跌停数据。

从 Tushare limit_list_d API 逐日拉取全市场涨跌停记录并写入本地 DB。
支持断点续跑：已缓存的日期自动跳过。

用法:
    python scripts/backfill_limit_pool.py              # 回填全部缺失日期
    python scripts/backfill_limit_pool.py --days 30    # 仅最近 30 个交易日
    python scripts/backfill_limit_pool.py --dry-run    # 预览待回填日期
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_lp")


def get_missing_dates(db, limit_days: int = 0) -> list:
    """返回 stock_daily 中有数据但 limit_pool 中缺失的日期列表。"""
    from datetime import date as _date
    from sqlalchemy import text

    def _to_date(val) -> _date:
        return val if isinstance(val, _date) else _date.fromisoformat(str(val))

    with db.get_session() as s:
        cached = {
            _to_date(row[0]) for row in
            s.execute(text("SELECT DISTINCT trade_date FROM limit_pool")).fetchall()
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
    parser = argparse.ArgumentParser(description="回填 limit_pool 历史数据")
    parser.add_argument("--days", type=int, default=0, help="仅回填最近 N 个交易日")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    parser.add_argument("--delay", type=float, default=0.3, help="API 调用间隔秒数")
    parser.add_argument("--force", action="store_true", help="强制重新拉取已有日期的数据")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager

    db = DatabaseManager()
    tf = TushareFetcher.get_instance()
    if not tf.is_available:
        logger.error("Tushare 不可用，请检查 Token")
        sys.exit(1)

    missing = get_missing_dates(db, limit_days=args.days)
    if not missing:
        logger.info("无缺失日期，无需回填")
        return

    logger.info(
        "%s待回填 %d 个日期: %s ~ %s",
        "[DRY-RUN] " if args.dry_run else "",
        len(missing), missing[-1], missing[0],
    )

    if args.dry_run:
        return

    success = 0
    t0 = time.time()
    for i, d_obj in enumerate(missing):
        trade_date = d_obj.strftime("%Y%m%d")
        try:
            df = tf.get_limit_list(trade_date=trade_date)
            if df is None or df.empty:
                logger.warning("  [%d/%d] %s: 无数据", i + 1, len(missing), trade_date)
                continue

            df = df.reset_index()
            out = pd.DataFrame()
            out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out["trade_date"] = trade_date
            raw_limit_type = df.get("limit_type", "")
            fallback_limit = df.get("limit", "")
            out_limit_type = raw_limit_type.where(
                raw_limit_type.notna() & (raw_limit_type != ""),
                fallback_limit,
            ).fillna("")
            out["limit_type"] = out_limit_type
            out["pct_chg"] = pd.to_numeric(df.get("pct_chg", 0), errors="coerce")
            out["limit_times"] = pd.to_numeric(df.get("limit_times", 0), errors="coerce").fillna(0).astype(int)
            out["open_times"] = pd.to_numeric(df.get("open_times", 0), errors="coerce").fillna(0).astype(int)
            out["up_stat"] = df.get("up_stat", "")
            out["limit_stats"] = out_limit_type

            try:
                ths_map = db.get_ths_industry_map()
                if ths_map:
                    out["sector"] = out["code"].map(ths_map).fillna("")
            except Exception:
                out["sector"] = ""

            saved = db.upsert_limit_pool(out, source="tushare_backfill", slot=0)
            elapsed = time.time() - t0
            eta = (elapsed / (i + 1)) * (len(missing) - i - 1) if i > 0 else 0
            logger.info(
                "[%d/%d] %s: %d 条 | ETA %.0fs",
                i + 1, len(missing), trade_date, saved, eta,
            )
            success += 1
        except Exception as e:
            logger.warning("[%d/%d] %s: 失败 - %s", i + 1, len(missing), trade_date, e)

        if i < len(missing) - 1 and args.delay > 0:
            time.sleep(args.delay)

    elapsed = time.time() - t0
    logger.info("回填完成: %d/%d 个日期成功, 耗时 %.0fs (%.1f 分钟)", success, len(missing), elapsed, elapsed / 60)


if __name__ == "__main__":
    main()
