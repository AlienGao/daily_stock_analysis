#!/usr/bin/env python3
"""回填 A 股指数基本信息、日线、周线数据。

数据来源：
  - index_basic  → index_basic 表（指数列表）
  - index_daily  → index_daily 表（日线）
  - index_weekly → index_weekly 表（周线）

用法:
    python scripts/backfill_index_a_daily.py --test       # 测试 ~90 天
    python scripts/backfill_index_a_daily.py               # 全量 2026-01-01 至今
    python scripts/backfill_index_a_daily.py --indices-only  # 仅刷新指数列表
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
logger = logging.getLogger("backfill_index_a")

# ── 核心 A 股指数列表（优先级高于 index_basic 拉取） ──
# 覆盖宽基、风格、策略主要指数
MAJOR_INDICES = [
    ("000001.SH", "上证指数"),
    ("000016.SH", "上证50"),
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("000852.SH", "中证1000"),
    ("000688.SH", "科创50"),
    ("000015.SH", "上证红利"),
    ("932056.SH", "科创100"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("399005.SZ", "中小板指"),
    ("399300.SZ", "沪深300"),
    ("399673.SZ", "创业板50"),
    ("399303.SZ", "国证2000"),
]

# 可选带有成分股的指数（已不再使用，成分股通过 Tushare 实时获取）
DEDICATED_INDEX_CODES: set = set()


INDEX_DAILY_START = "2026-01-01"


def fetch_index_list(fetcher) -> list[tuple[str, str]]:
    """通过 Tushare index_basic 获取 A 股指数列表。"""
    results = []
    for market in ("SSE", "SZSE", "CSI"):
        try:
            fetcher._check_rate_limit()
            df = fetcher._api.index_basic(market=market)
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    ts_code = str(row.get("ts_code", "")).strip()
                    name = str(row.get("name", "")).strip()
                    if ts_code:
                        results.append((ts_code, name))
        except Exception as exc:
            logger.warning("index_basic(%s) failed: %s", market, exc)
    logger.info("index_basic 返回 %d 个指数", len(results))
    return results


def fetch_index_daily(fetcher, ts_code: str, start_date: str, end_date: str):
    """通过 Tushare index_daily 拉取指数日线。"""
    ts_start = start_date.replace("-", "")
    ts_end = end_date.replace("-", "")
    fetcher._check_rate_limit()
    df = fetcher._api.index_daily(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
    if df is None or df.empty:
        return None
    import pandas as pd
    result = pd.DataFrame()
    result["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    result["open"] = pd.to_numeric(df["open"], errors="coerce")
    result["high"] = pd.to_numeric(df["high"], errors="coerce")
    result["low"] = pd.to_numeric(df["low"], errors="coerce")
    result["close"] = pd.to_numeric(df["close"], errors="coerce")
    result["pre_close"] = pd.to_numeric(df.get("pre_close", pd.Series([None]*len(df))), errors="coerce")
    result["pct_chg"] = pd.to_numeric(df.get("pct_chg", pd.Series([None]*len(df))), errors="coerce")
    result["vol"] = pd.to_numeric(df.get("vol", pd.Series([None]*len(df))), errors="coerce")
    result["amount"] = pd.to_numeric(df.get("amount", pd.Series([None]*len(df))), errors="coerce")
    result = result.sort_values("trade_date").reset_index(drop=True)
    return result


def fetch_index_weekly(fetcher, ts_code: str, start_date: str, end_date: str):
    """通过 Tushare index_weekly 拉取指数周线。"""
    ts_start = start_date.replace("-", "")
    ts_end = end_date.replace("-", "")
    fetcher._check_rate_limit()
    df = fetcher._api.index_weekly(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
    if df is None or df.empty:
        return None
    import pandas as pd
    result = pd.DataFrame()
    result["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    result["open"] = pd.to_numeric(df["open"], errors="coerce")
    result["high"] = pd.to_numeric(df["high"], errors="coerce")
    result["low"] = pd.to_numeric(df["low"], errors="coerce")
    result["close"] = pd.to_numeric(df["close"], errors="coerce")
    result["pre_close"] = pd.to_numeric(df.get("pre_close", pd.Series([None]*len(df))), errors="coerce")
    result["pct_chg"] = pd.to_numeric(df.get("pct_chg", pd.Series([None]*len(df))), errors="coerce")
    result["vol"] = pd.to_numeric(df.get("vol", pd.Series([None]*len(df))), errors="coerce")
    result["amount"] = pd.to_numeric(df.get("amount", pd.Series([None]*len(df))), errors="coerce")
    result = result.sort_values("trade_date").reset_index(drop=True)
    return result


def _backfill_index_weight(fetcher, db, ts_code: str):
    """拉取指数成分股并落库，补全股票名称。
    注意：前端已改为通过 Tushare 实时获取，此函数不再使用。
    保留以兼容旧调用路径。"""
    pass


def _load_stock_name_map(db) -> dict:
    """从 stock_daily + daily_basic 构建代码→名称映射。"""
    name_map = {}
    try:
        from src.data.stock_index_loader import get_stock_name_index_map
        name_map.update(get_stock_name_index_map())
    except Exception:
        pass
    return name_map


def _v(val):
    if val is None: return None
    try:
        f = float(val)
        return None if (f != f or f in (float("inf"), float("-inf"))) else f
    except (TypeError, ValueError): return None


def _save_index_basic(db, index_list: list[tuple[str, str]]):
    """写入 index_basic 表。"""
    from src.storage import IndexBasic
    saved = 0
    with db.get_session() as session:
        for ts_code, name in index_list:
            existing = session.query(IndexBasic).filter(IndexBasic.ts_code == ts_code).first()
            if existing:
                existing.name = name
            else:
                session.add(IndexBasic(ts_code=ts_code, name=name))
                saved += 1
        session.commit()
    return saved


def _upsert_index_data(db, ts_code: str, df, model_cls, date_col: str, ts_code_col: str = "ts_code"):
    """通用 UPSERT 到 A 股指数表。"""
    if df is None or df.empty:
        return 0
    saved = 0
    from sqlalchemy import and_
    with db.get_session() as session:
        for _, row in df.iterrows():
            row_date = row[date_col]
            if hasattr(row_date, "strftime"):
                date_str = row_date.strftime("%Y%m%d")
            elif hasattr(row_date, "to_pydatetime"):
                date_str = row_date.to_pydatetime().strftime("%Y%m%d")
            else:
                date_str = str(row_date)

            existing = session.query(model_cls).filter(
                and_(
                    getattr(model_cls, ts_code_col) == ts_code,
                    getattr(model_cls, "trade_date") == date_str,
                )
            ).first()

            vals = {
                "open": _v(row.get("open")),
                "high": _v(row.get("high")),
                "low": _v(row.get("low")),
                "close": _v(row.get("close")),
                "pre_close": _v(row.get("pre_close")),
                "pct_chg": _v(row.get("pct_chg")),
                "vol": _v(row.get("vol")),
                "amount": _v(row.get("amount")),
            }

            if existing:
                for k, v in vals.items():
                    setattr(existing, k, v)
                existing.updated_at = datetime.now()
            else:
                vals[ts_code_col] = ts_code
                vals["trade_date"] = date_str
                session.add(model_cls(**vals))
            saved += 1
        session.commit()
    return saved


def main():
    parser = argparse.ArgumentParser(description="回填 A 股指数日线/周线数据")
    parser.add_argument("--start", default=None, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD（默认: 今天）")
    parser.add_argument("--test", action="store_true", help="测试模式：仅回填最近 ~90 天")
    parser.add_argument("--dry-run", action="store_true", help="仅预览不写入")
    parser.add_argument("--indices-only", action="store_true", help="仅刷新指数列表")
    args = parser.parse_args()

    from data_provider.tushare_fetcher import TushareFetcher
    from src.storage import DatabaseManager, IndexDaily, IndexWeekly, IndexBasic

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
        logger.error("Tushare API 未初始化")
        return 1

    db = DatabaseManager()

    # ── 指数列表 ──
    logger.info("获取指数列表…")
    try:
        all_indices = fetch_index_list(fetcher)
    except Exception as exc:
        logger.warning("index_basic 拉取失败: %s，使用预定义列表", exc)
        all_indices = []
    if not all_indices:
        all_indices = MAJOR_INDICES
        logger.info("使用预定义 %d 个指数", len(all_indices))
    # 合并预定义列表（确保主要指数都在）
    defined_ts = set(ts for ts, _ in MAJOR_INDICES)
    existing_ts = set(ts for ts, _ in all_indices)
    for ts, name in MAJOR_INDICES:
        if ts not in existing_ts:
            all_indices.append((ts, name))
    logger.info("合计 %d 个指数", len(all_indices))

    # 只保留中证/上交所/深交所/申万指数
    def _get_market(ts_code: str, db) -> str:
        if ts_code.endswith(".SH"): return "SSE"
        if ts_code.endswith(".SZ"): return "SZSE"
        if ts_code.endswith(".SI"): return "SW"
        try:
            with db.get_session() as sess:
                row = sess.query(IndexBasic.market).filter(IndexBasic.ts_code == ts_code).scalar()
                if row: return str(row)
        except Exception:
            pass
        return ""
    ALLOWED_MARKETS = {"CSI", "SSE", "SZSE", "SW"}
    filtered_idx = [(t, n) for t, n in all_indices if _get_market(t, db) in ALLOWED_MARKETS]
    skip_count = len(all_indices) - len(filtered_idx)
    if skip_count:
        logger.info("过滤掉 %d 个非目标市场指数", skip_count)
    all_indices = filtered_idx
    logger.info("过滤后 %d 个指数", len(all_indices))

    if not args.dry_run:
        new_basic = _save_index_basic(db, all_indices)
        if new_basic > 0:
            logger.info("index_basic 新增 %d 个", new_basic)
        else:
            logger.info("index_basic 无需更新")

    if args.indices_only:
        logger.info("--indices-only，跳过行情回填")
        return 0

    # ── 行情回填 ──
    total_daily = 0
    total_weekly = 0
    total_failed = 0
    total_con = 0

    for idx, (ts_code, name) in enumerate(all_indices):
        label = f"[{idx + 1}/{len(all_indices)}] {ts_code} {name}"

        # 日线
        logger.info("%s 拉取 index_daily…", label)
        df = fetch_index_daily(fetcher, ts_code, start_date, end_date)
        if df is None or df.empty:
            logger.warning("%s 日线无数据", label)
        else:
            if args.dry_run:
                logger.info("  dry-run: 日线 %d 行", len(df))
                total_daily += len(df)
            else:
                saved = _upsert_index_data(db, ts_code, df, IndexDaily, "trade_date")
                if saved:
                    logger.info("  → 日线入库 %d 行", saved)
                    total_daily += saved

        # 周线
        logger.info("%s 拉取 index_weekly…", label)
        wdf = fetch_index_weekly(fetcher, ts_code, start_date, end_date)
        if wdf is None or wdf.empty:
            logger.warning("%s 周线无数据", label)
        else:
            if args.dry_run:
                logger.info("  dry-run: 周线 %d 行", len(wdf))
                total_weekly += len(wdf)
            else:
                saved_w = _upsert_index_data(db, ts_code, wdf, IndexWeekly, "trade_date")
                if saved_w:
                    logger.info("  → 周线入库 %d 行", saved_w)
                    total_weekly += saved_w

    logger.info("===== 完成 =====")
    logger.info("日线 %d 行, 周线 %d 行", total_daily, total_weekly)

    if not args.dry_run:
        with db.get_session() as sess:
            from sqlalchemy import func
            daily_cnt = sess.query(func.count()).select_from(IndexDaily).scalar()
            daily_code = sess.query(func.count(IndexDaily.ts_code.distinct())).scalar()
            weekly_cnt = sess.query(func.count()).select_from(IndexWeekly).scalar()
            weekly_code = sess.query(func.count(IndexWeekly.ts_code.distinct())).scalar()
            basic_cnt = sess.query(func.count()).select_from(IndexBasic).scalar()
            logger.info("index_basic=%d, index_daily=%d行/%d个, index_weekly=%d行/%d个",
                        basic_cnt, daily_cnt, daily_code, weekly_cnt, weekly_code)

    return 0


if __name__ == "__main__":
    sys.exit(main())
