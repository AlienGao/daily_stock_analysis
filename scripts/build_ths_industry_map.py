#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""构建全市场股票 → 同花顺行业映射，写入 DB。

数据来源:
  1. akshare stock_board_industry_name_ths → 90 个同花顺行业代码
  2. Tushare ths_member → 每个行业的成分股列表

用法:
    python scripts/build_ths_industry_map.py
    python scripts/build_ths_industry_map.py --test  # 只跑前 3 个行业
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

from data_provider.tushare_fetcher import TushareFetcher
from src.storage import DatabaseManager

_PAUSE_SEC = 0.8


def _get_industry_list() -> pd.DataFrame:
    """获取 90 个同花顺行业代码与名称。"""
    import akshare as ak

    df = ak.stock_board_industry_name_ths()
    df["ths_code"] = df["code"].astype(str).str.strip()
    df["industry_name"] = df["name"].astype(str).str.strip()
    return df[["ths_code", "industry_name"]]


def _fetch_members(tf, ths_code: str) -> list:
    """获取某同花顺行业的所有成分股 ts_code。"""
    ts_code_full = f"{ths_code}.TI"
    try:
        raw = tf._api.ths_member(ts_code=ts_code_full, fields="ts_code,con_code")
        if raw is not None and not raw.empty and "con_code" in raw.columns:
            codes = raw["con_code"].astype(str).str.strip()
            return [c.split(".")[0].zfill(6) for c in codes if "." in c]
    except Exception as e:
        print(f"  [warn] {ths_code} ({ts_code_full}) 查询失败: {e}")
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="构建同花顺行业映射")
    parser.add_argument("--test", action="store_true", help="只跑前 3 个行业")
    args = parser.parse_args()

    # [1/4] 获取行业列表
    print("[1/4] 获取同花顺行业列表...")
    industry_df = _get_industry_list()
    print(f"  共 {len(industry_df)} 个行业")

    if args.test:
        industry_df = industry_df.head(3)
        print(f"  [test] 只处理 {len(industry_df)} 个")

    # 确保 DB 表存在
    db = DatabaseManager()

    # [2/4] 逐行业查询成分股
    print("[2/4] 逐行业查询成分股...")
    tf = TushareFetcher.get_instance()
    if tf._api is None:
        print("[error] Tushare API 不可用，请检查 TUSHARE_TOKEN")
        return 1

    code_to_industry: dict = {}
    total = len(industry_df)
    for i, (_, row) in enumerate(industry_df.iterrows()):
        ths_code = row["ths_code"]
        name = row["industry_name"]
        codes = _fetch_members(tf, ths_code)
        for c in codes:
            if c not in code_to_industry:
                code_to_industry[c] = name
        pct = (i + 1) / total * 100
        print(f"  [{i+1}/{total}] {name}({ths_code}): {len(codes)} stocks ({pct:.0f}%)")
        time.sleep(_PAUSE_SEC)

    print(f"  映射总数: {len(code_to_industry)} 只股票")

    # [3/4] 构建 DataFrame
    print("[3/4] 写入 DB...")
    out = pd.DataFrame([
        {"stock_code": k, "industry_name": v}
        for k, v in code_to_industry.items()
    ])

    # [4/4] 入库
    saved = db.upsert_ths_industry_map(out, source="tushare")
    print(f"[4/4] 入库完成: {saved} 条")

    # 验证
    verify = db.get_ths_industry_map()
    print(f"  验证: DB 中 {len(verify)} 条")
    if verify:
        sample = list(verify.items())[:5]
        for code, ind in sample:
            print(f"    {code} → {ind}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[中断] 用户取消")
        sys.exit(1)
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
