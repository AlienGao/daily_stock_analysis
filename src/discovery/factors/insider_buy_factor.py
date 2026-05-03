# -*- coding: utf-8 -*-
"""险资举牌因子 (Insider Buy Factor).

盘后因子：基于同花顺险资举牌数据，识别被大资金举牌的股票。
数据来源: akshare stock_rank_xzjp_ths()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class InsiderBuyFactor(BaseFactor):
    """险资举牌因子。

    基于险资举牌数据，识别被大资金明确看好的股票。
    关键信号：举牌增持比例高 + 举牌后持股比例高 = 强认可信号。
    """

    name = "insider_buy"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_insider_buy()

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 增持数量占总股本比例
        col_add_ratio = next(
            (c for c in df.columns if "增持数量占总股本比例" in c),
            None,
        )
        # 变动后持股比例
        col_hold_ratio = next(
            (c for c in df.columns if "变动后持股比例" in c),
            None,
        )
        # 交易均价
        col_price = next(
            (c for c in df.columns if c == "交易均价"),
            None,
        )

        if col_add_ratio:
            add_ratio = pd.to_numeric(df[col_add_ratio], errors="coerce").fillna(0)
            scores.loc[add_ratio > 5] += 50.0  # 举牌比例 > 5%
            scores.loc[(add_ratio > 1) & (add_ratio <= 5)] += 35.0
            scores.loc[(add_ratio > 0) & (add_ratio <= 1)] += 20.0

        if col_hold_ratio:
            hold_ratio = pd.to_numeric(df[col_hold_ratio], errors="coerce").fillna(0)
            scores.loc[hold_ratio > 10] += 25.0
            scores.loc[(hold_ratio > 5) & (hold_ratio <= 10)] += 15.0

        if col_price:
            price = pd.to_numeric(df[col_price], errors="coerce").fillna(0)
            # 有交易均价说明是近期举牌，加分
            scores.loc[price > 0] += 25.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        col_add_ratio = next(
            (c for c in df.columns if "增持数量占总股本比例" in c),
            None,
        )
        col_hold_ratio = next(
            (c for c in df.columns if "变动后持股比例" in c),
            None,
        )

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if col_add_ratio:
                v = df[col_add_ratio].get(ts_code, 0)
                if v > 0:
                    r.append(f"举牌增持{v:.2f}%")
            if col_hold_ratio:
                v = df[col_hold_ratio].get(ts_code, 0)
                if v > 0:
                    r.append(f"持股比例{v:.1f}%")
            if r:
                reasons[ts_code] = r
        return reasons