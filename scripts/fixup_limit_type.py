#!/usr/bin/env python3
"""修复 limit_pool 中 limit_type 为空的历史数据，用 Tushare limit 字段兜底。"""
import logging
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("fixup_limit")

from data_provider.tushare_fetcher import TushareFetcher
from src.storage import DatabaseManager
from sqlalchemy import text

db = DatabaseManager()
tf = TushareFetcher.get_instance()

with db.get_session() as s:
    rows = s.execute(text(
        """SELECT DISTINCT trade_date FROM limit_pool
           WHERE (limit_type IS NULL OR limit_type = '')
           ORDER BY trade_date"""
    )).fetchall()
bad_dates = [r[0] for r in rows]

if not bad_dates:
    logger.info("无需修复")
    sys.exit(0)

logger.info("待修复 %d 个日期: %s ~ %s", len(bad_dates), bad_dates[0], bad_dates[-1])

fixed = 0
for i, trade_date in enumerate(bad_dates):
    try:
        df = tf.get_limit_list(trade_date=trade_date)
        if df is None or df.empty:
            logger.warning("[%d/%d] %s: 无数据", i + 1, len(bad_dates), trade_date)
            continue

        df = df.reset_index()

        # 安全获取列（列可能不存在，返回字符串默认值）
        raw_lt = df["limit_type"] if "limit_type" in df.columns else pd.Series("", index=df.index)
        fallback = df["limit"] if "limit" in df.columns else pd.Series("", index=df.index)
        new_lt = raw_lt.where(raw_lt.notna() & (raw_lt != ""), fallback).fillna("")

        # 组装 DataFrame 调用 upsert
        out = pd.DataFrame()
        out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        out["trade_date"] = trade_date
        out["limit_type"] = new_lt.values
        out["pct_chg"] = pd.to_numeric(df.get("pct_chg", 0), errors="coerce")
        out["limit_times"] = pd.to_numeric(df.get("limit_times", 0), errors="coerce").fillna(0).astype(int)
        out["open_times"] = pd.to_numeric(df.get("open_times", 0), errors="coerce").fillna(0).astype(int)
        out["up_stat"] = df.get("up_stat", "")
        out["limit_stats"] = new_lt.values

        try:
            ths_map = db.get_ths_industry_map()
            if ths_map:
                out["sector"] = out["code"].map(ths_map).fillna("")
        except Exception:
            out["sector"] = ""

        updated = db.upsert_limit_pool(out, source="tushare_backfill", slot=0)

        logger.info("[%d/%d] %s: %d 条更新", i + 1, len(bad_dates), trade_date, updated)
        fixed += 1
    except Exception as e:
        logger.warning("[%d/%d] %s: %s", i + 1, len(bad_dates), trade_date, e)

    if i < len(bad_dates) - 1:
        time.sleep(0.3)

logger.info("修复完成: %d/%d 个日期", fixed, len(bad_dates))
