#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""清理 2023-01-01 之前的数据。

先做边界验证（保证不误删/不漏删），确认后逐表删除，最后 VACUUM。
"""

import sqlite3
import sys

DB = "/Users/justingao/Documents/daily_stock_analysis/data/stock_analysis.db"
ISO_CUTOFF = "2023-01-01"
YMD_CUTOFF = "20230101"

# (table, date_col, cutoff_value, format_label)
TABLES = [
    ("stock_daily", "date", ISO_CUTOFF, "ISO"),
    ("stock_tech_indicator", "date", ISO_CUTOFF, "ISO"),
    ("sector_daily", "trade_date", ISO_CUTOFF, "ISO"),
    ("daily_basic", "trade_date", YMD_CUTOFF, "YMD"),
    ("hm_detail", "trade_date", YMD_CUTOFF, "YMD"),
    ("limit_pool", "trade_date", YMD_CUTOFF, "YMD"),
    ("margin_detail", "trade_date", YMD_CUTOFF, "YMD"),
    ("momentum_snapshot", "trade_date", YMD_CUTOFF, "YMD"),
    ("money_flow", "trade_date", YMD_CUTOFF, "YMD"),
]


def verify(conn):
    """边界验证：确保 cutoff 前后各有一条数据正确归类。"""
    print("=" * 60)
    print("Step 1: 边界验证")
    print("=" * 60)
    all_ok = True
    for tbl, col, cutoff, _ in TABLES:
        after = conn.execute(
            f"SELECT COUNT(*) FROM '{tbl}' WHERE {col} >= ?", (cutoff,)
        ).fetchone()[0]
        before = conn.execute(
            f"SELECT COUNT(*) FROM '{tbl}' WHERE {col} < ?", (cutoff,)
        ).fetchone()[0]
        total = conn.execute(f"SELECT COUNT(*) FROM '{tbl}'").fetchone()[0]

        max_del = conn.execute(
            f"SELECT MAX({col}) FROM '{tbl}' WHERE {col} < ?", (cutoff,)
        ).fetchone()[0]
        min_keep = conn.execute(
            f"SELECT MIN({col}) FROM '{tbl}' WHERE {col} >= ?", (cutoff,)
        ).fetchone()[0]

        ok = (before + after == total) and max_del is not None and min_keep is not None
        status = "OK" if ok else "CHECK"
        if not ok:
            all_ok = False

        print(f"  {tbl:30s} total={total:>10,}  del={before:>10,}  keep={after:>10,}  "
              f"max_del={str(max_del):>12s}  min_keep={str(min_keep):>12s}  [{status}]")

    if all_ok:
        print("\n边界验证全部通过。")
    else:
        print("\n*** 边界验证有异常，请检查！***")
    return all_ok


def delete_table(conn, tbl, col, cutoff):
    """Delete pre-cutoff rows from one table, return count."""
    before = conn.execute(
        f"SELECT COUNT(*) FROM '{tbl}' WHERE {col} < ?", (cutoff,)
    ).fetchone()[0]
    if before == 0:
        return 0

    batch = 500000
    while True:
        conn.execute(f"DELETE FROM '{tbl}' WHERE {col} < ?", (cutoff,))
        conn.commit()
        remaining = conn.execute(
            f"SELECT COUNT(*) FROM '{tbl}' WHERE {col} < ?", (cutoff,)
        ).fetchone()[0]
        if remaining == 0:
            break
        print(f"    ... remaining {remaining:,}")

    return before


def main():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Step 1: Verify
    if not verify(conn):
        print("取消操作。")
        conn.close()
        sys.exit(1)

    # Step 2: Confirm
    print("\n" + "=" * 60)
    print("Step 2: 确认删除")
    print("=" * 60)
    total_to_delete = 0
    for tbl, col, cutoff, _ in TABLES:
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM '{tbl}' WHERE {col} < ?", (cutoff,)
        ).fetchone()[0]
        total_to_delete += cnt
        if cnt > 0:
            print(f"  {tbl:30s} 删除 {cnt:>10,} 行")

    print(f"\n  合计删除: {total_to_delete:,} 行")
    resp = input("\n确认执行? (输入 yes 继续): ")
    if resp.strip().lower() != "yes":
        print("取消。")
        conn.close()
        sys.exit(0)

    # Step 3: Delete
    print("\n" + "=" * 60)
    print("Step 3: 逐表删除")
    print("=" * 60)
    grand_total = 0
    for tbl, col, cutoff, _ in TABLES:
        print(f"\n  {tbl} ...")
        deleted = delete_table(conn, tbl, col, cutoff)
        grand_total += deleted
        print(f"    完成: {deleted:,} 行")

    print(f"\n  总计删除: {grand_total:,} 行")

    # Step 4: VACUUM
    print("\n" + "=" * 60)
    print("Step 4: VACUUM（回收磁盘空间，可能需要几分钟）")
    print("=" * 60)
    conn.execute("VACUUM")
    print("  VACUUM 完成")

    # Final check
    print("\n" + "=" * 60)
    print("最终验证")
    print("=" * 60)
    for tbl, col, cutoff, _ in TABLES:
        total = conn.execute(f"SELECT COUNT(*) FROM '{tbl}'").fetchone()[0]
        dmin = conn.execute(f"SELECT MIN({col}) FROM '{tbl}'").fetchone()[0]
        dmax = conn.execute(f"SELECT MAX({col}) FROM '{tbl}'").fetchone()[0]
        pre = conn.execute(
            f"SELECT COUNT(*) FROM '{tbl}' WHERE {col} < ?", (cutoff,)
        ).fetchone()[0]
        print(f"  {tbl:30s} {total:>10,} rows  "
              f"[{str(dmin):>12s} ~ {str(dmax):>12s}]  pre2023={pre}")

    conn.close()
    print("\n完成。")


if __name__ == "__main__":
    main()
