#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate DISCOVERY_STOCK_WHITELIST for computing power hardware sectors.

Queries akshare concept boards for:
  光模块, 算力, 存储芯片, CPO, PCB, 液冷, 算力租赁
Outputs a comma-separated stock code list for DISCOVERY_STOCK_WHITELIST.

Usage:
    python scripts/generate_discovery_whitelist.py
"""

from __future__ import annotations

import sys
from typing import Dict, Set

CONCEPT_KEYWORDS = [
    "光模块",
    "算力",
    "存储芯片",
    "CPO",
    "PCB",
    "液冷",
    "算力租赁",
]


def get_concept_codes(concept_name: str) -> Set[str]:
    """Fetch all stock codes under a concept board via akshare."""
    try:
        import akshare as ak
    except ImportError:
        print("[ERROR] akshare not installed. Run: pip install akshare", file=sys.stderr)
        return set()

    # Try exact match first
    try:
        df = ak.stock_board_concept_cons_em(symbol=concept_name)
        if df is not None and not df.empty:
            codes = set()
            col = "代码" if "代码" in df.columns else df.columns[0]
            for _, row in df.iterrows():
                c = str(row[col]).strip()
                if c and len(c) == 6 and c.isdigit():
                    codes.add(c)
            return codes
    except Exception:
        pass

    # Fallback: search concept name list for partial matches
    try:
        board_df = ak.stock_board_concept_name_em()
        if board_df is not None and not board_df.empty:
            name_col = "板块名称" if "板块名称" in board_df.columns else board_df.columns[1]
            matches = board_df[board_df[name_col].str.contains(concept_name, na=False)]
            if not matches.empty:
                codes = set()
                for _, row in matches.iterrows():
                    board_name = row[name_col]
                    try:
                        cons_df = ak.stock_board_concept_cons_em(symbol=board_name)
                        if cons_df is not None and not cons_df.empty:
                            col = "代码" if "代码" in cons_df.columns else cons_df.columns[0]
                            for _, r in cons_df.iterrows():
                                c = str(r[col]).strip()
                                if c and len(c) == 6 and c.isdigit():
                                    codes.add(c)
                    except Exception:
                        pass
                return codes
    except Exception:
        pass

    return set()


def main() -> None:
    all_codes: Dict[str, Set[str]] = {}
    combined: Set[str] = set()

    for concept in CONCEPT_KEYWORDS:
        codes = get_concept_codes(concept)
        all_codes[concept] = codes
        combined.update(codes)
        print(f"  {concept}: {len(codes)} stocks")

    print(f"\n=== Total unique stocks: {len(combined)} ===")
    print()
    print("# Add this line to your .env:")
    code_list = ",".join(sorted(combined))
    print(f'DISCOVERY_STOCK_WHITELIST={code_list}')

    print()
    print("# Breakdown:")
    for concept, codes in all_codes.items():
        if codes:
            print(f"#   {concept}: {len(codes)} stocks")


if __name__ == "__main__":
    main()
