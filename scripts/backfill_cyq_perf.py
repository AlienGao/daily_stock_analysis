#!/usr/bin/env python3
"""回填 cyq_perf 历史筹码胜率数据。

从 Tushare cyq_perf API 逐日拉取全市场数据并写入本地 DB。
支持断点续跑：已全量覆盖的日期自动跳过（阈值: >= 4000 条）。

用法:
    python scripts/backfill_cyq_perf.py              # 回填全部缺失日期
    python scripts/backfill_cyq_perf.py --days 30    # 仅最近 30 个交易日
    python scripts/backfill_cyq_perf.py --dry-run    # 预览待回填日期
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
logger = logging.getLogger("backfill_cyq")

FULL_MARKET_THRESHOLD = 4000  # 当日 >= 此数视为已全量覆盖


def get_missing_dates(db, limit_days: int = 0) -> list:
    """返回 stock_daily 中有数据但 cyq_perf 中不完整的日期列表。"""
    from datetime import date as _date
    from sqlalchemy import text

    def _to_date(val) -> _date:
        return val if isinstance(val, _date) else _date.fromisoformat(str(val))

    with db.get_session() as s:
        date_counts = {
            _to_date(row[0]): row[1]
            for row in s.execute(
                text("SELECT trade_date, COUNT(*) FROM broker_enrichment_cyq_perf GROUP BY trade_date")
            ).fetchall()
        }
        all_dates = [
            _to_date(row[0]) for row in
            s.execute(
                text("SELECT DISTINCT date FROM stock_daily ORDER BY date DESC")
            ).fetchall()
        ]

    missing = []
    for d_obj in all_dates:
        count = date_counts.get(d_obj, 0)
        if count < FULL_MARKET_THRESHOLD:
            missing.append((d_obj, count))
    if limit_days > 0:
        missing = missing[:limit_days]

    return missing


def main():
    parser = argparse.ArgumentParser(description="回填 cyq_perf 历史数据")
    parser.add_argument("--days", type=int, default=0, help="仅回填最近 N 个交易日")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    parser.add_argument("--force", action="store_true", help="强制覆盖（即使已有 >= 4000 条）")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager

    db = DatabaseManager()
    tf = TushareFetcher.get_instance()
    if not tf.is_available:
        logger.error("Tushare 不可用，请检查 Token")
        sys.exit(1)

    if args.force:
        from datetime import date as _date
        from sqlalchemy import text
        with db.get_session() as s:
            all_dates = [
                _date.fromisoformat(str(row[0])) for row in
                s.execute(
                    text("SELECT DISTINCT date FROM stock_daily ORDER BY date DESC")
                ).fetchall()
            ]
        missing = [(d, 0) for d in all_dates[:args.days]] if args.days > 0 else [(d, 0) for d in all_dates]
    else:
        missing = get_missing_dates(db, limit_days=args.days)

    if not missing:
        logger.info("无缺失日期，无需回填")
        return

    logger.info(
        f"{'[DRY-RUN] ' if args.dry_run else ''}"
        f"待回填 {len(missing)} 个日期: "
        f"{missing[0][0]} ~ {missing[-1][0]}"
    )
    for d_obj, cnt in missing[:5]:
        logger.info(f"  {d_obj}: 当前 {cnt} 条")
    if len(missing) > 5:
        logger.info(f"  ... 共 {len(missing)} 个日期")

    if args.dry_run:
        return

    success = 0
    for i, (d_obj, _) in enumerate(missing):
        trade_date = d_obj.strftime("%Y%m%d")
        logger.info(f"[{i+1}/{len(missing)}] 回填 {trade_date} ...")
        try:
            df = tf.get_bulk_cyq_perf(trade_date)
            if df is None or df.empty:
                logger.warning(f"  {trade_date}: Tushare 无数据")
                continue
            df = df.reset_index()
            df["trade_date"] = df.get("trade_date", trade_date)
            numeric_cols = [
                "winner_rate", "cost_5pct", "cost_15pct", "cost_50pct",
                "cost_85pct", "cost_95pct", "weight_avg", "his_low", "his_high",
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            saved = db.upsert_cyq_perf(df, source="tushare_backfill")
            logger.info(f"  {trade_date}: {saved} 条")
            success += 1
        except Exception as e:
            logger.warning(f"  {trade_date}: 失败 - {e}")

        if i < len(missing) - 1:
            time.sleep(0.3)

    logger.info(f"回填完成: {success}/{len(missing)} 个日期成功")


if __name__ == "__main__":
    main()
