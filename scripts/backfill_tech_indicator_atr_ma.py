# -*- coding: utf-8 -*-
"""回填 stock_tech_indicator 表中缺失的 atr/ma5/ma10/ma20/ma60 字段。

从 Tushare stk_factor_pro 批量拉取前复权数据，无需本地计算。
用法: python scripts/backfill_tech_indicator_atr_ma.py [--dry-run] [--limit N]
"""

import argparse
import logging
import os
from datetime import datetime as _dt

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# stk_factor_pro → DB 字段映射
FIELD_MAP = {
    "atr_qfq": "atr",
    "ma_qfq_5": "ma5",
    "ma_qfq_10": "ma10",
    "ma_qfq_20": "ma20",
    "ma_qfq_60": "ma60",
}


def backfill(dry_run: bool = False, limit: int = 0):
    import tushare as ts
    from sqlalchemy import func, text

    from src.storage import DatabaseManager, StockTechIndicator

    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        print("错误: 未设置 TUSHARE_TOKEN")
        return
    pro = ts.pro_api(token)

    db = DatabaseManager.get_instance()

    # 1. 获取需要回填的日期列表
    with db.get_session() as s:
        dates = (
            s.query(StockTechIndicator.date)
            .filter(StockTechIndicator.atr.is_(None))
            .distinct()
            .order_by(StockTechIndicator.date.desc())
            .all()
        )
        dates = [d[0] for d in dates]
        print(f"需要回填的日期数: {len(dates)}")

        if limit > 0:
            dates = dates[:limit]
            print(f"限制为: {len(dates)} 个日期")

    total_updated = 0
    total_failed = 0
    total_missing = 0

    for i, td in enumerate(dates):
        trade_date_str = td.strftime("%Y%m%d")
        print(f"[{i + 1}/{len(dates)}] {trade_date_str} ...", end=" ", flush=True)

        # 2. 从 Tushare 拉取当日全量前复权数据
        try:
            fields = "ts_code," + ",".join(FIELD_MAP.keys())
            df = pro.stk_factor_pro(trade_date=trade_date_str, fields=fields)
        except Exception as e:
            print(f"Tushare 拉取失败: {e}")
            continue

        if df is None or df.empty:
            print("无数据")
            continue

        # 3. 查找该日期需要回填的 code 列表
        with db.get_session() as s:
            need_codes = set(
                row[0]
                for row in s.query(StockTechIndicator.code)
                .filter(
                    StockTechIndicator.date == td,
                    StockTechIndicator.atr.is_(None),
                )
                .all()
            )

        date_updated = 0
        date_missing = 0

        # 4. 逐行匹配更新
        for _, row in df.iterrows():
            ts_code = str(row["ts_code"])
            bare_code = ts_code.split(".")[0].zfill(6)
            if bare_code not in need_codes:
                continue

            values = {}
            for tushare_field, db_field in FIELD_MAP.items():
                v = row.get(tushare_field)
                if v is not None and not (isinstance(v, float) and v != v):
                    values[db_field] = float(v)

            if not values:
                date_missing += 1
                continue

            if dry_run:
                date_updated += 1
                if date_updated <= 3:
                    print(f"\n  [DRY] {bare_code}: {values}")
                continue

            try:
                set_clause = ", ".join(f"{k}=:{k}" for k in values)
                params = {**values, "code": bare_code, "date": td}
                with db.session_scope() as s:
                    s.execute(
                        text(
                            f"UPDATE stock_tech_indicator "
                            f"SET {set_clause} "
                            f"WHERE code=:code AND date=:date"
                        ),
                        params,
                    )
                date_updated += 1
            except Exception as e:
                logger.warning(f"更新失败 {bare_code} {td}: {e}")

        total_updated += date_updated
        total_missing += date_missing
        print(f"ok={date_updated}" + (f" missing={date_missing}" if date_missing else ""))

    print(f"\n完成: updated={total_updated} missing={total_missing}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(description="回填 stock_tech_indicator ATR/MA (Tushare stk_factor_pro)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="限制日期数 (0=全部)")
    args = ap.parse_args()
    backfill(dry_run=args.dry_run, limit=args.limit)
