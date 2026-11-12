# -*- coding: utf-8 -*-
"""连续涨跌区间统计。

港股页与后复权新高页的「最近最大回撤 / 最近最大涨幅」共用同一口径：
在日线收盘价序列上取最近一段至少连续 2 个交易日收跌（或收涨）的区间。
"""
from __future__ import annotations

import math
from typing import List, Optional, Tuple


def _valid_dated_closes(dated_closes: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    return [
        (trade_date, float(close))
        for trade_date, close in dated_closes
        if close is not None and math.isfinite(close) and close > 0
    ]


def latest_consecutive_drawdown(
    dated_closes: List[Tuple[str, float]],
) -> Optional[Tuple[float, int, str, str]]:
    """返回最近一段至少连续 2 个交易日收跌的跌幅、天数和起止日期。"""
    valid = _valid_dated_closes(dated_closes)
    end_idx = len(valid) - 1
    while end_idx > 0:
        while end_idx > 0 and valid[end_idx][1] >= valid[end_idx - 1][1]:
            end_idx -= 1
        if end_idx <= 0:
            return None

        start_idx = end_idx
        while start_idx > 0 and valid[start_idx][1] < valid[start_idx - 1][1]:
            start_idx -= 1
        decline_days = end_idx - start_idx
        if decline_days >= 2:
            start_date, start_close = valid[start_idx]
            end_date, end_close = valid[end_idx]
            drawdown_pct = round((end_close - start_close) / start_close * 100, 2)
            return drawdown_pct, decline_days, start_date, end_date
        end_idx = start_idx - 1
    return None


def latest_consecutive_gain(
    dated_closes: List[Tuple[str, float]],
) -> Optional[Tuple[float, int, str, str]]:
    """返回最近一段至少连续 2 个交易日收涨的涨幅、天数和起止日期。"""
    valid = _valid_dated_closes(dated_closes)
    end_idx = len(valid) - 1
    while end_idx > 0:
        while end_idx > 0 and valid[end_idx][1] <= valid[end_idx - 1][1]:
            end_idx -= 1
        if end_idx <= 0:
            return None

        start_idx = end_idx
        while start_idx > 0 and valid[start_idx][1] > valid[start_idx - 1][1]:
            start_idx -= 1
        rise_days = end_idx - start_idx
        if rise_days >= 2:
            start_date, start_close = valid[start_idx]
            end_date, end_close = valid[end_idx]
            gain_pct = round((end_close - start_close) / start_close * 100, 2)
            return gain_pct, rise_days, start_date, end_date
        end_idx = start_idx - 1
    return None
