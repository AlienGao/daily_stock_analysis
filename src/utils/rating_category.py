# -*- coding: utf-8 -*-
"""Map operation_advice text to BUY/HOLD/LOOK/SELL.

Canonical tag mapping (strong_buy/buy/hold/watch/reduce/sell → BUY/HOLD/LOOK/SELL)
is unified with report_language.py's get_signal_level, so report display and .env
categorization always agree without maintaining two separate text-match maps.
"""

from typing import Optional, Set

# 优先按 emoji 分类（报告摘要已包含 emoji，最快路径）
EMOJI_CATEGORY_MAP = {
    "🟢": "BUY",
    "💚": "BUY",
    "🟡": "HOLD",
    "⚪": "LOOK",
    "🟠": "SELL",
    "🔴": "SELL",
}

# canonical tag → category（与 report_language.py get_signal_level 的输出对齐）
TAG_CATEGORY_MAP = {
    "strong_buy": "BUY",
    "buy": "BUY",
    "hold": "HOLD",
    "watch": "LOOK",
    "reduce": "SELL",
    "sell": "SELL",
    "strong_sell": "SELL",
}


def operation_advice_to_category(operation_advice: str, unmapped: Optional[Set[str]] = None) -> str:
    """Return one of BUY/HOLD/LOOK/SELL.

    Priority:
    1) Emoji marker (fast path for already-formatted text)
    2) Canonical tag via get_signal_level (same canonical map as report display)
    3) Fallback to LOOK
    """
    advice = (operation_advice or "").strip()
    if not advice:
        return "LOOK"

    # 1) Emoji match
    for emoji, category in EMOJI_CATEGORY_MAP.items():
        if emoji in advice:
            return category

    # 2) Canonical tag via get_signal_level — unified with report display mapping
    try:
        from src.report_language import get_signal_level
        _, _, tag = get_signal_level(advice, None, None)
        if tag in TAG_CATEGORY_MAP:
            return TAG_CATEGORY_MAP[tag]
    except Exception:
        pass

    # 3) Fallback
    if advice and unmapped is not None:
        unmapped.add(advice)
    return "LOOK"
