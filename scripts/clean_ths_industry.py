#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""替换历史数据中的申万行业为同花顺行业。

读取 ths_industry_map，批量更新相关表的 sector/industry 字段。
仅在同花顺映射存在时替换，无映射的保留原值不污染。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from src.storage import DatabaseManager

_TABLES = [
    ("scan_result_intraday", "stock_code", "sector"),
    ("scan_result_postmarket", "stock_code", "sector"),
    ("limit_pool", "code", "sector"),
    ("limit_up_history", "code", "sector"),
    ("limit_break", "code", "sector"),
    ("performance_report", "code", "industry"),
]


def main() -> int:
    db = DatabaseManager()
    ths_map = db.get_ths_industry_map()
    if not ths_map:
        print("[error] ths_industry_map 为空，请先运行 build_ths_industry_map.py")
        return 1
    print(f"同花顺映射: {len(ths_map)} 条")

    total_updated = 0
    for table, code_col, sector_col in _TABLES:
        try:
            with db.get_session() as session:
                cnt_before = session.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE {sector_col} IS NOT NULL AND {sector_col} != ''")
                ).scalar()
                if not cnt_before:
                    print(f"  {table}: 0 条需更新，跳过")
                    continue

                _BATCH = 500
                updated = 0
                offset = 0
                while True:
                    rows = session.execute(
                        text(f"SELECT id, {code_col}, {sector_col} FROM {table} "
                             f"WHERE {sector_col} IS NOT NULL AND {sector_col} != '' "
                             f"LIMIT {_BATCH} OFFSET {offset}")
                    ).fetchall()
                    if not rows:
                        break
                    for row in rows:
                        rid = row[0]
                        code = str(row[1]).strip().zfill(6)
                        old_sector = str(row[2]).strip()
                        new_sector = ths_map.get(code)
                        # 仅在有同花顺映射且不同时替换，无映射保留原值
                        if new_sector and new_sector != old_sector:
                            session.execute(
                                text(f"UPDATE {table} SET {sector_col} = :sec WHERE id = :rid"),
                                {"sec": new_sector, "rid": rid},
                            )
                            updated += 1
                    session.commit()
                    offset += _BATCH

                total_updated += updated
                skipped = cnt_before - updated
                print(f"  {table}: {cnt_before} 条中 {updated} 替换, {skipped} 保持原值")
        except Exception as e:
            print(f"  {table}: error - {e}")

    print(f"\n总计替换 {total_updated} 条")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[中断]")
        sys.exit(1)
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
