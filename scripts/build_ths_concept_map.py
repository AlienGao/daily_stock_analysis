#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""构建全市场股票 → 同花顺概念映射，写入 DB。

数据来源:
  1. Tushare ths_index → ~412 个同花顺概念板块 (type=N)
  2. Tushare ths_member → 每个概念的成分股列表

用法:
    python scripts/build_ths_concept_map.py
    python scripts/build_ths_concept_map.py --test  # 只跑前 3 个概念
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


def _get_concept_list(tf: TushareFetcher) -> pd.DataFrame:
    """获取 ~412 个同花顺概念板块 (ths_index type=N)。"""
    raw = tf._api.ths_index()
    concepts = raw[raw["type"] == "N"].copy()
    concepts["concept_code"] = concepts["ts_code"].astype(str).str.strip()
    concepts["concept_name"] = concepts["name"].astype(str).str.strip()
    return concepts[["concept_code", "concept_name"]]


def _fetch_members(tf: TushareFetcher, concept_code: str) -> list:
    """获取某同花顺概念板块的所有成分股 stock_code。"""
    try:
        raw = tf._api.ths_member(ts_code=concept_code, fields="ts_code,con_code")
        if raw is not None and not raw.empty and "con_code" in raw.columns:
            codes = raw["con_code"].astype(str).str.strip()
            return [c.split(".")[0].zfill(6) for c in codes if "." in c]
    except Exception as e:
        print(f"  [warn] {concept_code} 查询失败: {e}")
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="构建同花顺概念映射")
    parser.add_argument("--test", action="store_true", help="只跑前 3 个概念")
    args = parser.parse_args()

    tf = TushareFetcher.get_instance()
    if tf._api is None:
        print("[error] Tushare API 不可用，请检查 TUSHARE_TOKEN")
        return 1

    # [1/4] 获取概念板块列表
    print("[1/4] 获取同花顺概念板块列表...")
    concept_df = _get_concept_list(tf)
    print(f"  共 {len(concept_df)} 个概念板块")

    if args.test:
        concept_df = concept_df.head(3)
        print(f"  [test] 只处理 {len(concept_df)} 个")

    # [2/4] 逐概念查询成分股
    print("[2/4] 逐概念查询成分股...")
    code_to_concepts: dict = {}
    total = len(concept_df)
    for i, (_, row) in enumerate(concept_df.iterrows()):
        code = row["concept_code"]
        name = row["concept_name"]
        members = _fetch_members(tf, code)
        for c in members:
            code_to_concepts.setdefault(c, []).append(name)
        pct = (i + 1) / total * 100
        print(f"  [{i+1}/{total}] {name}({code}): {len(members)} stocks ({pct:.0f}%)")
        time.sleep(_PAUSE_SEC)

    total_mappings = sum(len(v) for v in code_to_concepts.values())
    print(f"  映射总数: {total_mappings} 条, {len(code_to_concepts)} 只股票")

    # [3/4] 构建 DataFrame
    print("[3/4] 构建 DataFrame...")
    rows = []
    for stock_code, concepts in code_to_concepts.items():
        for cn in concepts:
            rows.append({"stock_code": stock_code, "concept_name": cn})
    out = pd.DataFrame(rows)

    # [4/4] 入库
    db = DatabaseManager()
    print("[4/4] 写入 DB...")
    saved = db.upsert_ths_concept_map(out, source="tushare")
    print(f"  入库完成: {saved} 条")

    # 验证
    verify = db.get_ths_concept_map()
    print(f"  验证: DB 中 {len(verify)} 只股票")
    if verify:
        sample = list(verify.items())[:5]
        for code, concepts in sample:
            print(f"    {code} -> {', '.join(concepts[:3])}{'...' if len(concepts) > 3 else ''}")

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
