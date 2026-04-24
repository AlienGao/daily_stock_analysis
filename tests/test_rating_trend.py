# -*- coding: utf-8 -*-
import pytest

from src.utils.rating_trend import (
    BUY_TO_HOLD_EMOJI,
    RATING_DOWN_EMOJI,
    RATING_UP_EMOJI,
    SAME_EMOJI,
    rating_bullishness,
    rating_change_emoji,
    rating_change_kind,
    sort_rating_changes,
)


@pytest.mark.parametrize(
    "old_r,new_r,kind,emoji",
    [
        ("观望", "持有", "up", RATING_UP_EMOJI),
        ("持有", "买入", "up", RATING_UP_EMOJI),
        ("买入", "持有", "buy_to_hold", BUY_TO_HOLD_EMOJI),
        ("买入", "观望", "down", RATING_DOWN_EMOJI),
        ("持有", "观望", "down", RATING_DOWN_EMOJI),
        ("买入", "买入", "same", SAME_EMOJI),
    ],
)
def test_kind_and_emoji(old_r: str, new_r: str, kind: str, emoji: str) -> None:
    assert rating_change_kind(old_r, new_r) == kind
    assert rating_change_emoji(old_r, new_r) == emoji


def test_sort_order_groups() -> None:
    items = [
        ("A", ("x", "持有", "买入")),
        ("B", ("x", "买入", "持有")),
        ("C", ("x", "持有", "观望")),
    ]
    out = sort_rating_changes(items)
    codes = [s for s, _ in out]
    # up (A) first, buy_to_hold (B) second, down (C) third
    assert codes == ["A", "B", "C"]


def test_bullishness_unknown_defaults_zero() -> None:
    assert rating_bullishness("未知") == 0
    # unknown vs known: 0 vs 3 -> up from look perspective if old unknown?
    # kind: n=3,o=0 -> up
    assert rating_change_kind("未知", "观望") == "up"
    assert rating_change_kind("观望", "未知") == "down"
