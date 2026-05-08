#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""全市场日K线数据同步脚本.

拉取近 ~60 个交易日的全 A 股日线数据并写入 stock_daily 表。
直接调用 Tushare API，不依赖 sync_all_daily。

用法:
    python scripts/sync_daily_kline.py                # 默认今天
    python scripts/sync_daily_kline.py --date 20260508
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime as dt, timedelta
from pathlib import Path
from typing import List

import pandas as pd

# 确保项目根目录在 sys.path 中
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sync_daily_kline")


def normalize_tushare_daily(df: pd.DataFrame) -> pd.DataFrame:
    """标准化 Tushare daily() 返回的数据.

    - 成交量 vol: 手 → 股 (×100)
    - 成交额 amount: 千元 → 元 (×1000)
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


def _bare_to_ts_code(code: str) -> str:
    """裸代码 → ts_code (e.g. 600519 → 600519.SH)."""
    c = str(code).strip().zfill(6)
    if c.startswith(("60", "68")):
        return f"{c}.SH"
    elif c.startswith(("00", "30")):
        return f"{c}.SZ"
    elif c.startswith(("4", "8", "92")):
        return f"{c}.BJ"
    else:
        return c


def sync_all_daily(fetcher, trade_date: str, lookback_calendar_days: int = 75) -> List[pd.DataFrame]:
    """全市场日线同步 — 独立实现（绕过 TushareFetcher 的结构问题）。"""
    stock_df = fetcher.get_stock_list()
    if stock_df is None or stock_df.empty:
        logger.warning("无股票列表")
        return []

    code_col = next((c for c in ["ts_code", "code"] if c in stock_df.columns), None)
    if not code_col:
        return []

    codes = stock_df[code_col].dropna().astype(str).tolist()
    if not codes:
        return []

    end_dt = dt.strptime(trade_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=lookback_calendar_days)
    ts_start = start_dt.strftime("%Y%m%d")
    ts_end = end_dt.strftime("%Y%m%d")

    logger.info("开始同步 %d 只股票, 日期范围 %s ~ %s", len(codes), ts_start, ts_end)

    results: List[pd.DataFrame] = []
    errors = 0
    minute_start = time.time()
    call_count = 0

    for i, raw_code in enumerate(codes):
        ts_code = _bare_to_ts_code(raw_code)
        try:
            df = fetcher._api.daily(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            call_count += 1

            # Rate limit: ~500 calls/min
            if call_count >= 480:
                elapsed = time.time() - minute_start
                if elapsed < 60:
                    sleep_sec = 60 - elapsed + 1
                    logger.debug("Rate limit: sleeping %.1fs", sleep_sec)
                    time.sleep(sleep_sec)
                minute_start = time.time()
                call_count = 0

            if df is not None and not df.empty:
                results.append(df)
        except Exception as e:
            errors += 1
            if errors <= 5:
                logger.warning("%s 失败: %s", ts_code, e)

        if (i + 1) % 500 == 0:
            logger.info("进度 %d/%d (%.0f%%), errors=%d",
                        i + 1, len(codes), (i + 1) / len(codes) * 100, errors)

    logger.info("完成: %d 只成功, %d 失败", len(results), errors)
    return results


def main():
    parser = argparse.ArgumentParser(description="全市场日K线同步")
    parser.add_argument("--date", default=None, help="目标交易日期 (YYYYMMDD)，默认今天")
    parser.add_argument("--lookback", type=int, default=75, help="向前推的自然日数 (默认 75)")
    args = parser.parse_args()

    load_dotenv()
    if not os.getenv("TUSHARE_TOKEN"):
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        return 1

    trade_date = args.date or time.strftime("%Y%m%d")
    logger.info("目标日期: %s, 回溯自然日: %d", trade_date, args.lookback)

    # ── 拉取 ──
    from data_provider.tushare_fetcher import TushareFetcher
    tf = TushareFetcher.get_instance()
    raw_dfs = sync_all_daily(tf, trade_date=trade_date, lookback_calendar_days=args.lookback)

    if not raw_dfs:
        logger.warning("无数据返回")
        return 1

    total_rows = sum(len(d) for d in raw_dfs)
    logger.info("拉取完成: %d 只股票, %d 行原始数据", len(raw_dfs), total_rows)

    # ── 合并 & 标准化 ──
    normalized = []
    for df in raw_dfs:
        df_norm = normalize_tushare_daily(df)
        if not df_norm.empty:
            normalized.append(df_norm)

    if not normalized:
        logger.warning("标准化后无有效数据")
        return 1

    merged = pd.concat(normalized, ignore_index=True)
    logger.info("合并后: %d 行", len(merged))

    # ── 写入 ──
    from src.storage import DatabaseManager
    db = DatabaseManager()
    try:
        saved = db.save_daily_batch(merged, data_source="tushare_sync")
        logger.info("写入完成: %d 行", saved)
    except Exception as e:
        logger.error("写入失败: %s", e)
        return 1

    # ── 统计 ──
    unique_codes = merged["ts_code"].nunique() if "ts_code" in merged.columns else 0
    date_min = merged["trade_date"].min() if "trade_date" in merged.columns else "?"
    date_max = merged["trade_date"].max() if "trade_date" in merged.columns else "?"
    logger.info("统计: %d 只股票, 日期范围 %s ~ %s", unique_codes, date_min, date_max)
    return 0


if __name__ == "__main__":
    sys.exit(main())
