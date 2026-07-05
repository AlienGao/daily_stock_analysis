#!/usr/bin/env python3
"""回填全球主要指数日线数据到 global_index_daily 表。

数据来源：Tushare index_global API → 标准化 → global_index_daily 表 UPSERT。

用法:
    python scripts/backfill_global_index.py --test       # 测试 ~90 天
    python scripts/backfill_global_index.py               # 全量 2026-01-01 至今
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
logger = logging.getLogger("backfill_global_index")

# ── Tushare index_global 可用的主要指数 ts_code ──
# 参考 https://tushare.pro/document/2?doc_id=211
GLOBAL_INDEX_CODES = [
    ("XIN9", "富时中国A50指数"),
    ("HSI", "恒生指数"),
    ("HKTECH", "恒生科技指数"),
    ("HKAH", "恒生AH股H指数"),
    ("DJI", "道琼斯工业指数"),
    ("SPX", "标普500指数"),
    ("IXIC", "纳斯达克指数"),
    ("FTSE", "富时100指数"),
    ("FCHI", "法国CAC40指数"),
    ("GDAXI", "德国DAX指数"),
    ("N225", "日经225指数"),
    ("KS11", "韩国综合指数"),
    ("AS51", "澳大利亚标普200指数"),
    ("SENSEX", "印度孟买SENSEX指数"),
    ("IBOVESPA", "巴西IBOVESPA指数"),
    ("TWII", "台湾加权指数"),
    ("SPTSX", "加拿大S&P/TSX指数"),
    ("CSX5P", "STOXX欧洲50指数"),
    ("RUT", "罗素2000指数"),
]


def fetch_global_index(fetcher, ts_code: str, start_date: str, end_date: str):
    """通过 Tushare index_global 拉取全球指数日线。"""
    ts_start = start_date.replace("-", "")
    ts_end = end_date.replace("-", "")

    fetcher._check_rate_limit()
    df = fetcher._api.index_global(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
    if df is None or df.empty:
        return None

    import pandas as pd

    result = pd.DataFrame()
    result["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    result["open"] = pd.to_numeric(df["open"], errors="coerce")
    result["high"] = pd.to_numeric(df["high"], errors="coerce")
    result["low"] = pd.to_numeric(df["low"], errors="coerce")
    result["close"] = pd.to_numeric(df["close"], errors="coerce")
    result["pre_close"] = pd.to_numeric(df.get("pre_close", pd.Series([None] * len(df))), errors="coerce")
    result["pct_chg"] = pd.to_numeric(df.get("pct_chg", pd.Series([None] * len(df))), errors="coerce")
    result["change"] = pd.to_numeric(df.get("change", pd.Series([None] * len(df))), errors="coerce")
    result["swing"] = pd.to_numeric(df.get("swing", pd.Series([None] * len(df))), errors="coerce")
    result["vol"] = pd.to_numeric(df.get("vol", pd.Series([None] * len(df))), errors="coerce")
    result["amount"] = pd.to_numeric(df.get("amount", pd.Series([None] * len(df))), errors="coerce")
    result = result.sort_values("trade_date").reset_index(drop=True)
    return result


def upsert_global_index(db, ts_code: str, name: str, df) -> int:
    """UPSERT 到 global_index_daily 表。"""
    from src.storage import GlobalIndexDaily
    from sqlalchemy import and_

    if df is None or df.empty:
        return 0

    saved = 0
    with db.get_session() as session:
        for _, row in df.iterrows():
            row_date = row["trade_date"]
            if hasattr(row_date, "strftime"):
                date_str = row_date.strftime("%Y%m%d")
            elif hasattr(row_date, "to_pydatetime"):
                date_str = row_date.to_pydatetime().strftime("%Y%m%d")
            else:
                date_str = str(row_date)

            existing = session.query(GlobalIndexDaily).filter(
                and_(
                    GlobalIndexDaily.ts_code == ts_code,
                    GlobalIndexDaily.trade_date == date_str,
                )
            ).first()

            def _v(val):
                if val is None:
                    return None
                try:
                    f = float(val)
                    return None if (f != f or f in (float("inf"), float("-inf"))) else f
                except (TypeError, ValueError):
                    return None

            if existing:
                existing.open = _v(row.get("open"))
                existing.high = _v(row.get("high"))
                existing.low = _v(row.get("low"))
                existing.close = _v(row.get("close"))
                existing.pre_close = _v(row.get("pre_close"))
                existing.pct_chg = _v(row.get("pct_chg"))
                existing.change = _v(row.get("change"))
                existing.swing = _v(row.get("swing"))
                existing.vol = _v(row.get("vol"))
                existing.amount = _v(row.get("amount"))
                existing.name = name
                existing.updated_at = datetime.now()
            else:
                session.add(GlobalIndexDaily(
                    ts_code=ts_code,
                    trade_date=date_str,
                    name=name,
                    open=_v(row.get("open")),
                    high=_v(row.get("high")),
                    low=_v(row.get("low")),
                    close=_v(row.get("close")),
                    pre_close=_v(row.get("pre_close")),
                    pct_chg=_v(row.get("pct_chg")),
                    change=_v(row.get("change")),
                    swing=_v(row.get("swing")),
                    vol=_v(row.get("vol")),
                    amount=_v(row.get("amount")),
                ))
            saved += 1

        session.commit()
        return saved


def main():
    parser = argparse.ArgumentParser(description="回填全球指数日线数据")
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
        start_date = "2026-01-01"

    logger.info("范围: %s ~ %s", start_date, end_date)

    fetcher = TushareFetcher.get_instance()
    if fetcher._api is None:
        logger.error("Tushare API 未初始化，请检查 TUSHARE_TOKEN")
        return 1

    db = DatabaseManager()
    total_saved = 0
    total_failed = 0

    for idx, (ts_code, name) in enumerate(GLOBAL_INDEX_CODES):
        label = f"[{idx + 1}/{len(GLOBAL_INDEX_CODES)}] {ts_code} {name}"
        logger.info("%s 拉取 index_global…", label)

        df = fetch_global_index(fetcher, ts_code, start_date, end_date)
        if df is None or df.empty:
            logger.warning("%s 无数据，跳过", label)
            total_failed += 1
            continue

        if args.dry_run:
            logger.info("  dry-run: 获取 %d 行 (%s ~ %s)", len(df),
                        df["trade_date"].iloc[0].strftime("%Y-%m-%d") if hasattr(df["trade_date"].iloc[0], "strftime") else str(df["trade_date"].iloc[0])[:10],
                        df["trade_date"].iloc[-1].strftime("%Y-%m-%d") if hasattr(df["trade_date"].iloc[-1], "strftime") else str(df["trade_date"].iloc[-1])[:10])
            total_saved += len(df)
            continue

        saved = upsert_global_index(db, ts_code, name, df)
        logger.info("  → 入库 %d 行", saved)
        total_saved += saved

    logger.info("===== 完成 =====")
    logger.info("处理 %d / %d 个指数，入库 %d 行，失败 %d 个",
                len(GLOBAL_INDEX_CODES) - total_failed, len(GLOBAL_INDEX_CODES),
                total_saved, total_failed)

    if not args.dry_run:
        with db.get_session() as sess:
            from src.storage import GlobalIndexDaily
            from sqlalchemy import func
            cnt = sess.query(func.count()).select_from(GlobalIndexDaily).scalar()
            code_cnt = sess.query(func.count(GlobalIndexDaily.ts_code.distinct())).scalar()
            mind = sess.query(func.min(GlobalIndexDaily.trade_date)).scalar()
            maxd = sess.query(func.max(GlobalIndexDaily.trade_date)).scalar()
            logger.info("global_index_daily 表: %d 行, %d 个指数 (%s ~ %s)", cnt, code_cnt, mind, maxd)

    return 0


if __name__ == "__main__":
    sys.exit(main())
