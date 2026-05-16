#!/usr/bin/env python3
"""重算 limit_factor 的历史评分，更新 factor_score_snapshots。
修复 limit_pool 的 limit_type 之后运行，覆盖之前全 0 的记录。"""
import logging
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("rerun_limit")

from src.storage import DatabaseManager
from src.discovery.factors.limit_factor import LimitFactor
from sqlalchemy import text

db = DatabaseManager()
factor = LimitFactor()

with db.get_session() as s:
    rows = s.execute(text(
        "SELECT DISTINCT trade_date FROM factor_score_snapshots WHERE trade_date >= '20230517' ORDER BY trade_date"
    )).fetchall()
dates = [r[0] for r in rows]
logger.info("待重算 %d 个日期: %s ~ %s", len(dates), dates[0], dates[-1])

saved_total = 0
for i, td in enumerate(dates):
    try:
        df = factor.fetch_data(td)
        if df is None or df.empty:
            logger.warning("[%d/%d] %s: limit_pool 无数据", i + 1, len(dates), td)
            continue

        scores = factor.score(df, trade_date=td)
        if scores is None or scores.empty:
            logger.warning("[%d/%d] %s: score 为空", i + 1, len(dates), td)
            continue

        saved = db.save_factor_score_snapshots({"limit": scores}, td, "postmarket")
        saved_total += saved
        nonzero = (scores > 0).sum()

        if (i + 1) % 50 == 0 or nonzero == 0:
            logger.info("[%d/%d] %s: %d 条, %d 非零", i + 1, len(dates), td, saved, nonzero)
    except Exception as e:
        logger.warning("[%d/%d] %s: %s", i + 1, len(dates), td, e)

logger.info("重算完成: %d 个日期, 共写入 %d 条", len(dates), saved_total)
