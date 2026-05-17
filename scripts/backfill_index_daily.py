#!/usr/bin/env python3
"""回填上证指数 (000001.SH) 日线数据到 stock_daily 表。

用途：为回测引擎的动态权重提供历史大盘数据。
数据来源：Tushare index_daily API → 标准化 → stock_daily 表 UPSERT。
UPSERT 按 (code, date) 匹配，已存在记录覆盖更新，不删不改其他数据。

用法:
    python scripts/backfill_index_daily.py --test       # 测试 ~90 天
    python scripts/backfill_index_daily.py               # 全量 2010-01-01 至今
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("backfill_index")

INDEX_CODE = "000001.SH"


def fetch_index_daily(fetcher, ts_code: str, start_date: str, end_date: str):
    """通过 Tushare index_daily 拉取指数日线，返回标准化 DataFrame。"""
    ts_start = start_date.replace("-", "")
    ts_end = end_date.replace("-", "")

    fetcher._check_rate_limit()
    df = fetcher._api.index_daily(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
    if df is None or df.empty:
        return None

    import pandas as pd
    result = pd.DataFrame()
    result["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    result["open"] = pd.to_numeric(df["open"], errors="coerce")
    result["high"] = pd.to_numeric(df["high"], errors="coerce")
    result["low"] = pd.to_numeric(df["low"], errors="coerce")
    result["close"] = pd.to_numeric(df["close"], errors="coerce")
    result["volume"] = pd.to_numeric(df["vol"], errors="coerce")
    result["amount"] = pd.to_numeric(df["amount"], errors="coerce") * 1000  # 千元→元
    result["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
    result = result.sort_values("date").reset_index(drop=True)
    return result


def main():
    parser = argparse.ArgumentParser(description="回填上证指数日线数据")
    parser.add_argument("--start", default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD（默认: 今天）")
    parser.add_argument("--test", action="store_true", help="测试模式：仅回填最近 ~90 天")
    parser.add_argument("--dry-run", action="store_true", help="仅预览不写入")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager

    end_date = args.end or date.today().strftime("%Y-%m-%d")
    if args.test:
        start_date = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")
        logger.info("测试模式: %s ~ %s", start_date, end_date)
    elif args.start:
        start_date = args.start
    else:
        start_date = "2010-01-01"

    logger.info("范围: %s ~ %s", start_date, end_date)

    # ── 拉取 ──
    logger.info("初始化 TushareFetcher…")
    fetcher = TushareFetcher.get_instance()
    if fetcher._api is None:
        logger.error("Tushare API 未初始化，请检查 TUSHARE_TOKEN")
        return 1

    logger.info("拉取 %s (index_daily)…", INDEX_CODE)
    df = fetch_index_daily(fetcher, INDEX_CODE, start_date, end_date)
    if df is None or df.empty:
        logger.error("未获取到任何数据")
        return 1

    logger.info("获取 %d 行 (%s ~ %s)", len(df),
                df["date"].min().strftime("%Y-%m-%d"),
                df["date"].max().strftime("%Y-%m-%d"))

    # 预览
    logger.info("预览前 3 行:")
    for _, row in df.head(3).iterrows():
        logger.info("  %s | close=%.2f | pct_chg=%.4f",
                    row["date"].strftime("%Y-%m-%d"), row["close"], row["pct_chg"])

    if args.dry_run:
        logger.info("dry-run，不执行写入")
        return 0

    # ── 入库（UPSERT by code+date）──
    db = DatabaseManager()
    added = db.save_daily_data(df, code=INDEX_CODE, data_source="TushareFetcher-index")
    logger.info("入库: 新增 %d 行", added)

    # ── 验证 ──
    with db.get_session() as sess:
        from src.storage import StockDaily
        from sqlalchemy import func
        cnt = sess.query(func.count()).filter(StockDaily.code == INDEX_CODE).scalar()
        mind = sess.query(func.min(StockDaily.date)).filter(StockDaily.code == INDEX_CODE).scalar()
        maxd = sess.query(func.max(StockDaily.date)).filter(StockDaily.code == INDEX_CODE).scalar()
        logger.info("stock_daily 中 %s: %d 行 (%s ~ %s)", INDEX_CODE, cnt, mind, maxd)

        rows = (sess.query(StockDaily)
                .filter(StockDaily.code == INDEX_CODE)
                .order_by(StockDaily.date.desc())
                .limit(3).all())
        for r in rows:
            logger.info("  %s | close=%.2f | pct_chg=%.4f", r.date, r.close or 0, r.pct_chg or 0)

    return 0


if __name__ == "__main__":
    sys.exit(main())
