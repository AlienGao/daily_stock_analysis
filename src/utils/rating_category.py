# -*- coding: utf-8 -*-
"""Map operation_advice text (emoji only) to BUY/HOLD/LOOK/SELL."""

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

# 优先按 emoji 分类（适配报告摘要和富文本建议）
EMOJI_CATEGORY_MAP: Dict[str, str] = {
    "🟢": "BUY",
    "🟡": "HOLD",
    "⚪": "LOOK",
    "🟠": "SELL",
    "🔴": "SELL",
}


def operation_advice_to_category(operation_advice: str, unmapped: Optional[Set[str]] = None) -> str:
    """Return one of BUY/HOLD/LOOK/SELL.

    Matching priority:
    1) Emoji marker: 🟢/🟡/⚪/🟠/🔴
    2) Text match: RATING_MAP key in advice text
    3) Fallback to LOOK (optionally collecting unmapped raw text)
    """
    advice = (operation_advice or "").strip()
    for emoji, category in EMOJI_CATEGORY_MAP.items():
        if emoji in advice:
            return category
    for text, category in RATING_MAP.items():
        if text in advice:
            return category
    if advice and unmapped is not None:
        unmapped.add(advice)
    return "LOOK"
