#!/usr/bin/env python3
"""回填 popularity_rank 历史人气排行数据。

从 Tushare dc_hot API 逐日拉取全市场 A 股人气数据并写入本地 DB。
支持断点续跑：已缓存的日期自动跳过。

用法:
    python scripts/backfill_popularity_rank.py              # 回填全部缺失日期
    python scripts/backfill_popularity_rank.py --days 30    # 仅最近 30 个交易日
    python scripts/backfill_popularity_rank.py --dry-run    # 预览待回填日期
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
logger = logging.getLogger("backfill_pop")


def get_missing_dates(db, limit_days: int = 0) -> list:
    """返回 stock_daily 中有数据但 popularity_rank 中缺失的日期列表。"""
    from datetime import date as _date
    from sqlalchemy import text

    def _to_date(val) -> _date:
        return val if isinstance(val, _date) else _date.fromisoformat(str(val))

    with db.get_session() as s:
        cached = {
            _to_date(row[0]) for row in
            s.execute(text("SELECT DISTINCT trade_date FROM popularity_rank")).fetchall()
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
    parser = argparse.ArgumentParser(description="回填 popularity_rank 历史数据")
    parser.add_argument("--days", type=int, default=0, help="仅回填最近 N 个交易日")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
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
        f"{'[DRY-RUN] ' if args.dry_run else ''}"
        f"待回填 {len(missing)} 个日期: "
        f"{missing[0]} ~ {missing[-1]}"
    )

    if args.dry_run:
        return

    success = 0
    for i, d_obj in enumerate(missing):
        trade_date = d_obj.strftime("%Y%m%d")
        logger.info(f"[{i+1}/{len(missing)}] 回填 {trade_date} ...")
        try:
            df = tf.get_dc_hot(trade_date)
            if df is None or df.empty:
                logger.warning(f"  {trade_date}: Tushare dc_hot 无数据")
                continue
            out = pd.DataFrame()
            out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out["name"] = df.get("name", "")
            out["trade_date"] = df.get("trade_date", trade_date)
            out["rank"] = pd.to_numeric(df.get("rank", 0), errors="coerce").fillna(0).astype(int)
            out["pct_change"] = pd.to_numeric(df.get("pct_change", 0), errors="coerce")
            out["hot"] = pd.to_numeric(df.get("hot", 0), errors="coerce") if "hot" in df.columns else None
            out["concept"] = df.get("concept", "") if "concept" in df.columns else ""

            saved = db.upsert_popularity_rank(out, source="tushare_backfill")
            logger.info(f"  {trade_date}: {saved} 条")
            success += 1
        except Exception as e:
            logger.warning(f"  {trade_date}: 失败 - {e}")

        if i < len(missing) - 1:
            time.sleep(0.3)

    logger.info(f"回填完成: {success}/{len(missing)} 个日期成功")


if __name__ == "__main__":
    main()
