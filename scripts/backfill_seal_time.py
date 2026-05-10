"""回补 limit_pool 表中缺失的 first_seal_time / last_seal_time。

数据来源 akshare stock_zt_pool_em，逐日拉取并 UPDATE 已有记录。
"""

import sys
import time
import traceback
from datetime import date

import pandas as pd

sys.path.insert(0, ".")

from src.storage import DatabaseManager, LimitPool
from sqlalchemy import and_, select, update


def backfill_seal_time(dry_run: bool = False) -> dict:
    """回补所有日期缺失的封板时间。

    Returns:
        {"total_missing": N, "fixed": N, "failed_dates": [...], "skipped_dates": N}
    """
    db = DatabaseManager()

    # 1) 查所有缺失的日期
    with db.get_session() as session:
        from sqlalchemy import text
        rows = session.execute(
            text(
                "SELECT DISTINCT trade_date FROM limit_pool "
                "WHERE first_seal_time IS NULL OR first_seal_time = '' "
                "ORDER BY trade_date DESC"
            )
        ).fetchall()
    dates = [r[0] for r in rows]
    print(f"发现 {len(dates)} 个日期缺少封板时间: {dates}")

    total_fixed = 0
    total_missing = 0
    failed_dates = []
    skipped_dates = 0

    for i, trade_date in enumerate(dates):
        print(f"\n[{i + 1}/{len(dates)}] {trade_date} ...", end=" ", flush=True)

        try:
            import akshare as ak
            df = ak.stock_zt_pool_em(date=trade_date)
        except Exception as e:
            print(f"akshare 请求失败: {e}")
            failed_dates.append(trade_date)
            continue

        if df is None or df.empty:
            print("akshare 无数据")
            skipped_dates += 1
            continue

        if "首次封板时间" not in df.columns or "最后封板时间" not in df.columns:
            print("akshare 返回值无封板时间列")
            skipped_dates += 1
            continue

        # 建立 代码 -> (first_seal, last_seal) 映射
        code_to_seal = {}
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip().zfill(6)
            if not code or len(code) != 6:
                continue
            fst = str(row.get("首次封板时间", "")).strip()
            lst = str(row.get("最后封板时间", "")).strip()
            if fst:
                code_to_seal[code] = (fst, lst if lst else fst)

        if not code_to_seal:
            print("无有效封板时间")
            skipped_dates += 1
            continue

        # 查询该日期 DB 中缺失 seal_time 的 code 列表
        with db.get_session() as session:
            db_codes = session.execute(
                select(LimitPool.code).where(
                    and_(
                        LimitPool.trade_date == trade_date,
                        LimitPool.code.in_(list(code_to_seal.keys())),
                    )
                )
            ).fetchall()
        db_codes_set = {r[0] for r in db_codes}

        updated = 0
        for code, (fst, lst) in code_to_seal.items():
            if code not in db_codes_set:
                continue
            if dry_run:
                updated += 1
                continue
            with db.get_session() as session:
                session.execute(
                    update(LimitPool)
                    .where(
                        and_(
                            LimitPool.code == code,
                            LimitPool.trade_date == trade_date,
                        )
                    )
                    .values(first_seal_time=fst, last_seal_time=lst)
                )
                session.commit()
            updated += 1

        total_fixed += updated
        total_missing += len(db_codes_set) - updated
        print(f"更新 {updated} 条", end="")

        # akshare 限速
        if i < len(dates) - 1:
            time.sleep(0.5)

    print(f"\n\n完成: 修复 {total_fixed} 条, 失败日期 {len(failed_dates)}")
    if failed_dates:
        print(f"失败日期: {failed_dates}")
    return {
        "total_missing": total_fixed + total_missing,
        "fixed": total_fixed,
        "failed_dates": failed_dates,
        "skipped_dates": skipped_dates,
    }


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    if dry:
        print(">>> DRY RUN MODE <<<")
    result = backfill_seal_time(dry_run=dry)
    print(f"Result: {result}")
