# -*- coding: utf-8 -*-
"""Map LLM operation_advice text to BUY/HOLD/LOOK/SELL (must stay in sync with main.py)."""

from typing import Dict, Optional, Set

# 与 main.run_full_analysis 中 category_stocks 的 rating_map 一致
RATING_MAP: Dict[str, str] = {
    "强烈买入": "BUY",
    "买入": "BUY",
    "加仓": "BUY",
    "建仓": "BUY",
    "增持": "BUY",
    "持有": "HOLD",
    "谨慎买入": "HOLD",
    "持有观望": "HOLD",
    "观望": "LOOK",
    "等待": "LOOK",
    "watch": "LOOK",
    "减持": "SELL",
    "减仓": "SELL",
    "清仓": "SELL",
    "卖出": "SELL",
    "强烈卖出": "SELL",
}


def operation_advice_to_category(operation_advice: str, unmapped: Optional[Set[str]] = None) -> str:
    """Return one of BUY/HOLD/LOOK/SELL. Unknown non-empty advice → LOOK + optional unmapped collect."""
    advice = (operation_advice or "").strip()
    if advice in RATING_MAP:
        return RATING_MAP[advice]
    if advice and unmapped is not None:
        unmapped.add(advice)
    return "LOOK"
