# -*- coding: utf-8 -*-
"""补齐机构调研缺失日期数据。

用法:
    python scripts/fetch_missing_institution_survey.py
    python scripts/fetch_missing_institution_survey.py --start 20260501 --end 20260531
    python scripts/fetch_missing_institution_survey.py --dry-run
"""

import argparse
import sys
from pathlib import Path

# 确保项目根目录在 sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import exchange_calendars as xcals
import pandas as pd

from data_provider.tushare_fetcher import TushareFetcher
from src.storage import DatabaseManager


def get_existing_dates(db: DatabaseManager, start: str, end: str) -> set:
    """查询已有数据的日期集合。"""
    from sqlalchemy import text

    with db.get_session() as session:
        r = session.execute(
            text(
                "SELECT DISTINCT surv_date FROM institution_survey "
                "WHERE surv_date >= :s AND surv_date <= :e"
            ),
            {"s": start, "e": end},
        )
        return {row[0] for row in r.fetchall()}


def get_trading_days(start: str, end: str) -> list:
    """获取区间内的 A 股交易日列表。"""
    cal = xcals.get_calendar("XSHG")
    dates = pd.date_range(start, end)
    return [d.strftime("%Y%m%d") for d in dates if cal.is_session(d)]


def fetch_and_save(fetcher: TushareFetcher, db: DatabaseManager, date: str) -> int:
    """拉取单日机构调研数据并存入 DB。"""
    df = fetcher.get_stk_surv(start_date=date, end_date=date)
    if df is None or df.empty:
        return 0

    # 标准化列名（Tushare 返回的列名可能带 _date 后缀）
    rename_map = {}
    for col in df.columns:
        if col == "ann_date":
            rename_map[col] = "surv_date"
    if rename_map:
        df = df.rename(columns=rename_map)

    # 确保 surv_date 列存在
    if "surv_date" not in df.columns:
        # 尝试从 index 或其他列推断
        if "trade_date" in df.columns:
            df["surv_date"] = df["trade_date"]
        else:
            df["surv_date"] = date

    # 标准化日期格式
    df["surv_date"] = df["surv_date"].astype(str).str.replace("-", "").str[:8]

    # 填充缺失列
    for col in ["rece_org", "org_type", "rece_mode", "weight", "fund_visitors", "rece_place", "comp_rece"]:
        if col not in df.columns:
            df[col] = ""

    return db.save_institution_survey(df, clear_date=date)


def main():
    parser = argparse.ArgumentParser(description="补齐机构调研缺失日期数据")
    parser.add_argument("--start", default="20260501", help="起始日期 (YYYYMMDD)")
    parser.add_argument("--end", default="20260531", help="截止日期 (YYYYMMDD)")
    parser.add_argument("--dry-run", action="store_true", help="只显示缺失日期，不实际拉取")
    args = parser.parse_args()

    db = DatabaseManager.get_instance()
    existing = get_existing_dates(db, args.start, args.end)
    trading_days = get_trading_days(args.start, args.end)
    missing = [d for d in trading_days if d not in existing]

    print(f"区间: {args.start} ~ {args.end}")
    print(f"交易日: {len(trading_days)} 天")
    print(f"已有数据: {len(existing)} 天")
    print(f"缺失: {len(missing)} 天")

    if not missing:
        print("无缺失日期，退出。")
        return

    for d in missing:
        print(f"  - {d}")

    if args.dry_run:
        print("\n[dry-run] 未实际拉取。")
        return

    fetcher = TushareFetcher.get_instance()
    total = 0
    for i, date in enumerate(missing, 1):
        print(f"\n[{i}/{len(missing)}] 拉取 {date} ...", end=" ")
        try:
            count = fetch_and_save(fetcher, db, date)
            print(f"{count} 条")
            total += count
        except Exception as e:
            print(f"失败: {e}")

    print(f"\n完成，共拉取 {total} 条记录。")


if __name__ == "__main__":
    main()
