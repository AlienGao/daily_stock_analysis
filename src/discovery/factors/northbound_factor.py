# -*- coding: utf-8 -*-
"""北向资金因子 (Northbound Factor).

盘后因子：基于沪深港通持股数据，识别北向资金增持的股票。
数据来源: akshare stock_hsgt_hold_stock_em()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class NorthboundFactor(BaseFactor):
    """北向资金因子。

    基于沪深港通持股排行，识别北向资金增持幅度大的股票。
    关键信号：增持占流通股比高 + 持续增持 = 外资看好。
    """

    name = "northbound"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_northbound_holds()

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 增持占流通股比列名
        col_ratio = next(
            (c for c in df.columns if "增持" in c and "流通股" in c),
            None,
        )
        # 持股占流通股比
        col_hold_ratio = next(
            (c for c in df.columns if "持股" in c and "流通股" in c),
            None,
        )
        # 增持市值
        col_amount = next(
            (c for c in df.columns if "增持" in c and "市值" in c),
            None,
        )

        if col_ratio:
            ratio = pd.to_numeric(df[col_ratio], errors="coerce").fillna(0)
            # 增持占流通股比 > 0.5% : +50分, 0.1-0.5%: +30分, >0: +15分
            scores.loc[ratio > 0.5] += 50.0
            scores.loc[(ratio > 0.1) & (ratio <= 0.5)] += 30.0
            scores.loc[(ratio > 0) & (ratio <= 0.1)] += 15.0

        if col_hold_ratio:
            hold = pd.to_numeric(df[col_hold_ratio], errors="coerce").fillna(0)
            # 持股占流通股 > 5%: +25分
            scores.loc[hold > 5] += 25.0

        if col_amount:
            amount = pd.to_numeric(df[col_amount], errors="coerce").fillna(0)
            # 增持市值 > 1亿: +25分, > 1000万: +15分
            scores.loc[amount > 1e8] += 25.0
            scores.loc[(amount > 1e7) & (amount <= 1e8)] += 15.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        col_ratio = next(
            (c for c in df.columns if "增持" in c and "流通股" in c),
            None,
        )
        col_hold_ratio = next(
            (c for c in df.columns if "持股" in c and "流通股" in c),
            None,
        )

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if col_ratio:
                v = df[col_ratio].get(ts_code, 0)
                if v > 0:
                    r.append(f"北向增持占流通股{v:.2f}%")
            if col_hold_ratio:
                v = df[col_hold_ratio].get(ts_code, 0)
                if v > 5:
                    r.append(f"北向持股占流通股{v:.1f}%")
            if r:
                reasons[ts_code] = r
        return reasons