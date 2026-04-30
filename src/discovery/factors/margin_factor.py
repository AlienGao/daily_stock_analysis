# -*- coding: utf-8 -*-
"""融资融券因子 (Margin Trading Factor).

盘后因子：杠杆资金趋势分析（T+1 数据）。
数据来源: Tushare margin_detail (59)
注意: 交易所次日 ~9:00 发布，20:00 只能拿到 T-1 数据。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class MarginFactor(BaseFactor):
    """融资融券因子。

    分析融资买入趋势和融券做空信号。
    T+1 延迟不影响趋势判断：杠杆资金看连续多日变化。
    """

    name = "margin"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        # T+1 数据，get_bulk_margin_detail 内部已取前一交易日
        return tushare_fetcher.get_bulk_margin_detail(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        rzmre = df.get("rzmre", pd.Series(0, index=df.index))  # 融资买入额
        rzye = df.get("rzye", pd.Series(0, index=df.index))    # 融资余额
        rqye = df.get("rqye", pd.Series(0, index=df.index))    # 融券余额
        rqyl = df.get("rqyl", pd.Series(0, index=df.index))    # 融券余量

        # 融资买入活跃: rzmre > 0（有融资买入即活跃）(+10)
        scores.loc[rzmre > 0] += 10.0

        # 融资余额显著: rzye > 1亿 (+10)
        scores.loc[rzye > 1e8] += 10.0

        # 融资买入占余额比: rzmre / rzye > 5% (+20)
        # (融资买入活跃度)
        margin_ratio = (rzmre / rzye.replace(0, 1)) * 100
        scores.loc[margin_ratio > 5] += 20.0

        # 融券余额上升: 空头信号 (-15)
        # 有融券余额表示市场有空头仓位
        has_short = rqye > 0
        high_short = rqye > rzye * 0.1  # 融券 > 融资的10%
        scores.loc[has_short] = scores - 5.0
        scores.loc[high_short] = scores - 15.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        rzmre = df.get("rzmre", pd.Series(0, index=df.index))
        rzye = df.get("rzye", pd.Series(0, index=df.index))
        rqye = df.get("rqye", pd.Series(0, index=df.index))

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            _rzmre = rzmre.get(ts_code, 0)
            _rzye = rzye.get(ts_code, 0)
            _rqye = rqye.get(ts_code, 0)

            if _rzmre > 0:
                r.append("融资买入活跃")
            if _rzye > 1e8:
                r.append(f"融资余额{_rzye/1e8:.1f}亿")
            if _rzye > 0 and _rzmre / max(_rzye, 1) * 100 > 5:
                r.append("融资买入占比>5%")
            if _rqye > _rzye * 0.1:
                r.append("融券占比偏高")
            elif _rqye > 0:
                r.append("有融券头寸")
            if r:
                reasons[ts_code] = r
        return reasons
