# -*- coding: utf-8 -*-
"""报告内五档评级的变化方向、排序与行首表情（与 report 正文中的 买入/持有/… 一致）。"""

from __future__ import annotations

from typing import Dict, List, Tuple

# 看多程度：数值越大越偏向买入 / 越看涨
BULLISHNESS: Dict[str, int] = {
    "买入": 5,
    "持有": 4,
    "观望": 3,
    "减持": 2,
    "卖出": 1,
}

RATING_UP_EMOJI = "✅"
RATING_DOWN_EMOJI = "❌"
# 特判：买入 → 持有，不视为与「一般下降」同档
BUY_TO_HOLD_EMOJI = "🟡"
SAME_EMOJI = "➡️"


def rating_bullishness(rating: str) -> int:
    return BULLISHNESS.get(rating, 0)


def rating_change_kind(old_rating: str, new_rating: str) -> str:
    """返回 up | down | buy_to_hold | same。"""
    if old_rating == "买入" and new_rating == "持有":
        return "buy_to_hold"
    o = rating_bullishness(old_rating)
    n = rating_bullishness(new_rating)
    if n > o:
        return "up"
    if n < o:
        return "down"
    return "same"


def rating_change_emoji(old_rating: str, new_rating: str) -> str:
    kind = rating_change_kind(old_rating, new_rating)
    if kind == "up":
        return RATING_UP_EMOJI
    if kind == "down":
        return RATING_DOWN_EMOJI
    if kind == "buy_to_hold":
        return BUY_TO_HOLD_EMOJI
    return SAME_EMOJI


def _change_group_order(kind: str) -> int:
    """排序：升高 → 买入转持有 → 一般下降。"""
    if kind == "up":
        return 0
    if kind == "buy_to_hold":
        return 1
    if kind == "down":
        return 2
    return 3


def sort_rating_changes(
    items: List[Tuple[str, Tuple[str, str, str]]],
) -> List[Tuple[str, Tuple[str, str, str]]]:
    """对 changes.items() 列表排序：先分组，组内新评级看涨程度降序，同分按代码。"""

    def key(it: Tuple[str, Tuple[str, str, str]]) -> Tuple[int, int, str]:
        stock, (_name, old_r, new_r) = it
        kind = rating_change_kind(old_r, new_r)
        grp = _change_group_order(kind)
        new_pri = rating_bullishness(new_r)
        return (grp, -new_pri, stock)

    return sorted(items, key=key)
