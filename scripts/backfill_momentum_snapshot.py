#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""回填 momentum_snapshot 表：Tushare 资金流历史数据，最多 10 年。

用法:
    python scripts/backfill_momentum_snapshot.py           # 回填全部缺失日期
    python scripts/backfill_momentum_snapshot.py --dry-run  # 仅统计，不实际写入
    python scripts/backfill_momentum_snapshot.py --cleanup  # 仅清理 10 年前数据
"""

import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_provider.tushare_fetcher import TushareFetcher
from src.storage import DatabaseManager
from src.discovery.money_flow_source import _fetch_tier3_tushare, _cache_to_db
from sqlalchemy import text


def get_existing_dates(db: DatabaseManager) -> set:
    """获取 momentum_snapshot 中已有的 trade_date 集合。"""
    with db.get_session() as s:
        rows = s.execute(text("SELECT DISTINCT trade_date FROM momentum_snapshot")).fetchall()
    return {r[0] for r in rows}


def get_missing_dates(fetcher: TushareFetcher, existing: set) -> list:
    """获取最近 10 年交易日中缺失的日期列表。"""
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=3660)).strftime("%Y%m%d")
    trade_dates = fetcher._get_trade_dates(end_date=end, start_date=start)
    missing = sorted([d for d in trade_dates if d not in existing], reverse=False)
    return missing


def delete_old_records(db: DatabaseManager) -> int:
    """删除 10 年前的 momentum_snapshot 记录。"""
    cutoff = (datetime.now() - timedelta(days=3660)).strftime("%Y%m%d")
    with db.get_session() as s:
        result = s.execute(
            text("DELETE FROM momentum_snapshot WHERE trade_date < :cutoff"),
            {"cutoff": cutoff},
        )
        s.commit()
    return result.rowcount


def main():
    dry_run = "--dry-run" in sys.argv
    cleanup_only = "--cleanup" in sys.argv

    print("=" * 60)
    print("momentum_snapshot 历史回填")
    mode = "dry-run (仅统计)" if dry_run else "cleanup-only" if cleanup_only else "正常写入"
    print(f"模式: {mode}")
    print("=" * 60)

    db = DatabaseManager()
    fetcher = TushareFetcher()

    # Pro 积分可支持更高频率，默认 2000 次/分钟（安全余量）
    fetcher.rate_limit_per_minute = int(
        os.getenv("TUSHARE_RATE_LIMIT", "2000")
    )

    if fetcher._api is None:
        print("[ERROR] Tushare API 未初始化，请检查 TUSHARE_TOKEN 环境变量")
        sys.exit(1)

    if not dry_run:
        deleted = delete_old_records(db)
        print(f"\n[清理] 删除 10 年前记录: {deleted} 条")

    if cleanup_only:
        print("cleanup 完成")
        return

    existing = get_existing_dates(db)
    print(f"[现有] momentum_snapshot 已覆盖 {len(existing)} 个交易日")

    missing = get_missing_dates(fetcher, existing)
    if not missing:
        print("[完成] 没有缺失的交易日，无需回填")
        return

    print(f"[缺失] {len(missing)} 个交易日待回填")
    if missing:
        print(f"  日期范围: {missing[0]} ~ {missing[-1]}")

    if dry_run:
        return

    success = 0
    fail = 0
    empty = 0
    t0 = time.time()

    for i, trade_date in enumerate(missing):
        elapsed = time.time() - t0
        eta = (elapsed / max(i, 1)) * (len(missing) - i) if i > 0 else 0

        try:
            df = _fetch_tier3_tushare(trade_date, fetcher)
            if df is not None and not df.empty:
                _cache_to_db(df, trade_date, source="tushare")
                print(f"  [{i+1}/{len(missing)}] {trade_date}  OK  {len(df)}只  "
                      f"| ETA {eta/60:.0f}min")
                success += 1
            else:
                print(f"  [{i+1}/{len(missing)}] {trade_date}  EMPTY")
                empty += 1
        except Exception as e:
            print(f"  [{i+1}/{len(missing)}] {trade_date}  FAIL: {e}")
            fail += 1

    if not dry_run:
        deleted = delete_old_records(db)
        if deleted:
            print(f"\n[最终清理] 删除 10 年前记录: {deleted} 条")

    total_elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"回填完成: 成功 {success}, 空数据 {empty}, 失败 {fail}")
    print(f"耗时: {total_elapsed/60:.1f} 分钟")
    print(f"现有覆盖: {len(get_existing_dates(db))} 个交易日")


if __name__ == "__main__":
    main()
