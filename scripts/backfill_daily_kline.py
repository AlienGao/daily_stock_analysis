#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""10 年日K线数据回填脚本.

使用 Tushare daily() API 逐只股票拉取完整历史日线（一次调用覆盖全时间段），
标准化后写入 stock_daily 表。支持断点续跑（已覆盖的股票自动跳过）。

用法:
    python scripts/backfill_daily_kline.py                     # 回填 2016 ~ 今
    python scripts/backfill_daily_kline.py --year-start 2016 --year-end 2020
    python scripts/backfill_daily_kline.py --dry-run           # 仅预览
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime as dt
from pathlib import Path
from typing import List, Set

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backfill_daily_kline")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _bare_to_ts_code(code: str) -> str:
    """裸代码 → ts_code (e.g. 600519 → 600519.SH)."""
    c = str(code).strip().zfill(6)
    if c.startswith(("60", "68")):
        return f"{c}.SH"
    elif c.startswith(("00", "30")):
        return f"{c}.SZ"
    elif c.startswith(("4", "8", "92")):
        return f"{c}.BJ"
    return c


def normalize_tushare_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化 Tushare daily() 返回数据.

    - vol: 手 → 股 (×100)
    - amount: 千元 → 元 (×1000)
    """
    df = df.copy()
    if "vol" in df.columns:
        df["vol"] = pd.to_numeric(df["vol"], errors="coerce") * 100
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce") * 1000
    if "pct_chg" in df.columns:
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def get_already_covered_codes(db, start_date: str) -> Set[str]:
    """返回 stock_daily 中已有 <= start_date 数据的股票代码集合。"""
    from sqlalchemy import text
    with db.get_session() as s:
        rows = s.execute(
            text(
                "SELECT code, MIN(date) as earliest FROM stock_daily "
                "GROUP BY code HAVING MIN(date) <= :start"
            ),
            {"start": start_date},
        ).fetchall()
        return {r[0] for r in rows}


# ------------------------------------------------------------------
# Core logic
# ------------------------------------------------------------------

def backfill(
    year_start: int = 2016,
    year_end: int = 0,
    dry_run: bool = False,
    batch_commit: int = 200,
) -> int:
    """回填全市场日K线数据.

    Args:
        year_start: 起始年份（默认 2016）
        year_end: 截止年份（0 = 当前年份）
        dry_run: True = 仅预览不拉取
        batch_commit: 每 N 只股票批量提交一次 DB 写入

    Returns:
        0 = 成功, 1 = 失败
    """
    load_dotenv()
    if not os.getenv("TUSHARE_TOKEN"):
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        return 1

    from src.storage import DatabaseManager
    from data_provider.tushare_fetcher import TushareFetcher

    tf = TushareFetcher.get_instance()
    if not tf.is_available():
        logger.error("Tushare 不可用")
        return 1

    # ── 代码列表 ──
    stock_df = tf.get_stock_list()
    if stock_df is None or stock_df.empty:
        logger.error("无股票列表")
        return 1

    codes = stock_df["code"].dropna().astype(str).tolist()
    logger.info("股票列表: %d 只", len(codes))

    # ── 日期范围 ──
    now_year = year_end if year_end > 0 else dt.now().year
    start_date = f"{year_start}0101"
    end_date = f"{now_year}1231"
    logger.info("回填范围: %s ~ %s (%d 年)", start_date, end_date, now_year - year_start + 1)

    # ── 断点续跑 ──
    db = DatabaseManager()
    covered = get_already_covered_codes(db, start_date)
    todo_codes = [c for c in codes if c not in covered]
    logger.info(
        "已覆盖: %d 只, 待拉取: %d 只", len(covered), len(todo_codes)
    )

    if dry_run:
        logger.info("[DRY-RUN] 将拉取 %d 只股票，跳过实际操作", len(todo_codes))
        if todo_codes:
            logger.info("  示例: %s ...", ", ".join(todo_codes[:10]))
        return 0

    if not todo_codes:
        logger.info("全部已覆盖，无需拉取")
        return 0

    # ── 逐只拉取 ──
    success, fail, skipped = 0, 0, 0
    pending_dfs: List[pd.DataFrame] = []
    minute_start = time.time()
    call_count = 0
    t0 = time.time()

    for i, code in enumerate(todo_codes):
        ts_code = _bare_to_ts_code(code)
        try:
            df = tf._api.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
            )
            call_count += 1

            if call_count >= 480:
                elapsed = time.time() - minute_start
                if elapsed < 60:
                    sleep_sec = 60 - elapsed + 1
                    logger.debug("Rate limit: sleeping %.1fs", sleep_sec)
                    time.sleep(sleep_sec)
                minute_start = time.time()
                call_count = 0

            if df is not None and not df.empty:
                df_norm = normalize_tushare_daily(df)
                if not df_norm.empty:
                    pending_dfs.append(df_norm)
                    success += 1
                else:
                    skipped += 1
            else:
                skipped += 1
        except Exception as e:
            fail += 1
            if fail <= 5:
                logger.warning("%s 失败: %s", ts_code, e)

        # 批量写入
        if len(pending_dfs) >= batch_commit:
            _flush_pending(db, pending_dfs)
            pending_dfs.clear()
            elapsed = time.time() - t0
            done = success + fail + skipped
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(todo_codes) - done) / rate if rate > 0 else 0
            logger.info(
                "[%d/%d] (%.0f%%) 成功=%d 失败=%d 跳过=%d | %.1f 只/秒 ETA %.0fs",
                done, len(todo_codes), done / len(todo_codes) * 100,
                success, fail, skipped, rate, eta,
            )

    # 最后一批写入
    if pending_dfs:
        _flush_pending(db, pending_dfs)

    elapsed = time.time() - t0
    total = success + fail + skipped
    logger.info(
        "完成: %d 只/成功 %d/失败 %d/跳过 %d 耗时 %.0fs (%.1f 分钟)",
        total, success, fail, skipped, elapsed, elapsed / 60,
    )
    return 0 if fail == 0 else 1


def _flush_pending(db, dfs: List[pd.DataFrame]) -> None:
    """批量写入待处理 DataFrame。"""
    if not dfs:
        return
    merged = pd.concat(dfs, ignore_index=True)
    try:
        saved = db.save_daily_batch(merged, data_source="tushare_backfill")
        logger.debug("写入 %d 行", saved)
    except Exception as e:
        logger.error("批量写入失败: %s", e)


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="10 年日K线数据回填")
    parser.add_argument(
        "--year-start", type=int, default=2016,
        help="起始年份（默认 2016）",
    )
    parser.add_argument(
        "--year-end", type=int, default=0,
        help="截止年份（默认当前年份）",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅预览不拉取",
    )
    parser.add_argument(
        "--batch-commit", type=int, default=200,
        help="每 N 只股票批量写入一次 DB（默认 200）",
    )
    args = parser.parse_args()
    return backfill(
        year_start=args.year_start,
        year_end=args.year_end,
        dry_run=args.dry_run,
        batch_commit=args.batch_commit,
    )


if __name__ == "__main__":
    sys.exit(main())
