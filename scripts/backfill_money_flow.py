#!/usr/bin/env python3
"""回填 money_flow 历史资金流向数据。

从 Tushare moneyflow API 逐日拉取全市场资金流数据并写入本地 DB。
支持断点续跑：已缓存的日期自动跳过。

用法:
    python scripts/backfill_money_flow.py              # 回填全部缺失日期
    python scripts/backfill_money_flow.py --days 30    # 仅最近 30 个交易日
    python scripts/backfill_money_flow.py --dry-run    # 预览待回填日期
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
logger = logging.getLogger("backfill_mf")


def get_missing_dates(db, limit_days: int = 0) -> list:
    """返回 stock_daily 中有数据但 money_flow 中缺失的日期列表。"""
    from datetime import date as _date
    from sqlalchemy import text

    def _to_date(val) -> _date:
        return val if isinstance(val, _date) else _date.fromisoformat(str(val))

    with db.get_session() as s:
        cached = {
            _to_date(row[0]) for row in
            s.execute(text("SELECT DISTINCT trade_date FROM money_flow")).fetchall()
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
    parser = argparse.ArgumentParser(description="回填 money_flow 历史数据")
    parser.add_argument("--days", type=int, default=0, help="仅回填最近 N 个交易日")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不实际写入")
    parser.add_argument("--delay", type=float, default=0.3, help="API 调用间隔秒数")
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
            df = tf.get_bulk_money_flow(trade_date=trade_date)
            if df is None or df.empty:
                logger.warning("  [%d/%d] %s: 无数据", i + 1, len(missing), trade_date)
                continue

            df = df.reset_index()
            out = pd.DataFrame()
            out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out["name"] = df.get("name", pd.Series("", index=df.index)).values if "name" in df.columns else ""
            out["trade_date"] = df.get("trade_date", trade_date)
            for c in ("buy_elg_amount", "sell_elg_amount", "buy_lg_amount",
                       "sell_lg_amount", "buy_md_amount", "sell_md_amount",
                       "buy_sm_amount", "sell_sm_amount", "net_mf_amount"):
                if c in df.columns:
                    out[c] = pd.to_numeric(df[c], errors="coerce")

            out = out[out["trade_date"].notna() & (out["trade_date"].astype(str).str.match(r"^\d{8}$"))]
            if out.empty:
                continue

            saved = db.upsert_money_flow(out, source="tushare_backfill")
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
