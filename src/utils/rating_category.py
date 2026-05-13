# -*- coding: utf-8 -*-
"""Map operation_advice text to BUY/HOLD/LOOK/SELL.

Canonical tag mapping (strong_buy/buy/hold/watch/reduce/sell → BUY/HOLD/LOOK/SELL)
is unified with report_language.py's get_signal_level, so report display and .env
categorization always agree without maintaining two separate text-match maps.
"""

from typing import Dict, List, Optional, Set

# tag → category（单向，无重复数据源）
_TAG_CATEGORY_MAP = {
    "strong_buy": "BUY",
    "buy": "BUY",
    "hold": "HOLD",
    "watch": "LOOK",
    "reduce": "SELL",
    "sell": "SELL",
    "strong_sell": "SELL",
}


def _build_operation_advice_by_category() -> Dict[str, List[str]]:
    """Derive category → [advice] reverse map from report_language canonical map."""
    from src.report_language import _OPERATION_ADVICE_CANONICAL_MAP

    result: Dict[str, List[str]] = {
        "BUY": [],
        "HOLD": [],
        "LOOK": [],
        "SELL": [],
    }
    for advice, tag in _OPERATION_ADVICE_CANONICAL_MAP.items():
        category = _TAG_CATEGORY_MAP.get(tag)
        if category:
            result[category].append(advice)
    return result


OPERATION_ADVICE_BY_CATEGORY: Dict[str, List[str]] = _build_operation_advice_by_category()

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

    Primary path: emoji prepended by localize_operation_advice (set by
    get_signal_level, never by LLM).  Fallback: canonical tag lookup via
    get_signal_level for texts from code paths that bypass localisation.
    """
    advice = (operation_advice or "").strip()
    if not advice:
        return "LOOK"

    for emoji, category in EMOJI_CATEGORY_MAP.items():
        if emoji in advice:
            return category

    # Fallback: some pipeline paths set operation_advice without going through
    # localize_operation_advice, so the text has no emoji.  Use get_signal_level
    # to derive the canonical tag deterministically.
    try:
        from src.report_language import get_signal_level

        _, _, tag = get_signal_level(advice, None, None)
        if tag in TAG_CATEGORY_MAP:
            return TAG_CATEGORY_MAP[tag]
    except Exception:
        pass

    if unmapped is not None:
        unmapped.add(advice)
    return "LOOK"
