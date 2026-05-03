# -*- coding: utf-8 -*-
"""机构持仓因子 (Institution Hold Factor).

盘后因子：基于新浪财经机构持股数据，识别机构增仓的股票。
数据来源: akshare stock_institute_hold()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class InstitutionHoldFactor(BaseFactor):
    """机构持仓因子。

    基于机构持股变化，识别机构增配的股票。
    关键信号：机构数增加 + 持股比例增幅为正 = 机构建仓。
    """

    name = "institution_hold"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_institution_holds()

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        col_count = next((c for c in df.columns if c == "机构数"), None)
        col_count_chg = next((c for c in df.columns if "机构数变化" in c), None)
        col_ratio = next((c for c in df.columns if c == "持股比例"), None)
        col_ratio_chg = next((c for c in df.columns if "持股比例增幅" in c), None)

        if col_count_chg:
            chg = pd.to_numeric(df[col_count_chg], errors="coerce").fillna(0)
            scores.loc[chg > 0] += 30.0  # 机构数增加

        if col_ratio_chg:
            ratio_chg = pd.to_numeric(df[col_ratio_chg], errors="coerce").fillna(0)
            scores.loc[ratio_chg > 1] += 35.0  # 持股比例增幅 > 1%
            scores.loc[(ratio_chg > 0) & (ratio_chg <= 1)] += 20.0

        if col_ratio:
            ratio = pd.to_numeric(df[col_ratio], errors="coerce").fillna(0)
            scores.loc[ratio > 10] += 20.0  # 持股比例 > 10%
            scores.loc[(ratio > 5) & (ratio <= 10)] += 10.0

        if col_count:
            count = pd.to_numeric(df[col_count], errors="coerce").fillna(0)
            scores.loc[count > 50] += 15.0  # 机构数 > 50

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        col_count_chg = next((c for c in df.columns if "机构数变化" in c), None)
        col_ratio_chg = next((c for c in df.columns if "持股比例增幅" in c), None)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if col_count_chg:
                v = df[col_count_chg].get(ts_code, 0)
                if v > 0:
                    r.append(f"机构数增加{v:.0f}家")
            if col_ratio_chg:
                v = df[col_ratio_chg].get(ts_code, 0)
                if v > 0:
                    r.append(f"机构持股增幅{v:.2f}%")
            if r:
                reasons[ts_code] = r
        return reasons