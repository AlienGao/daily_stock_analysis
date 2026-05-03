# -*- coding: utf-8 -*-
"""业绩因子 (Performance Factor).

盘后因子：基于东财业绩报表数据，识别业绩增长强劲的股票。
数据来源: akshare stock_yjbb_em()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class PerformanceFactor(BaseFactor):
    """业绩因子。

    基于业绩报表的净利润增长、ROE、毛利率，识别业绩成长股。
    关键信号：净利润增长 + ROE 高 + 毛利率稳定 = 优质成长。
    """

    name = "performance"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_performance_report()

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 净利润同比增长
        col_net_chg = next((c for c in df.columns if "净利润" in c and "增长" in c), None)
        # 营业收入增长
        col_rev_chg = next((c for c in df.columns if "营业收入" in c and "增长" in c), None)
        # 净资产收益率
        col_roe = next((c for c in df.columns if "净资产收益率" in c), None)
        # 销售毛利率
        col_gp = next((c for c in df.columns if "销售毛利率" in c), None)

        if col_net_chg:
            net_chg = pd.to_numeric(df[col_net_chg], errors="coerce").fillna(0)
            # 净利润增长 > 50%: +40分, 20-50%: +30分, 10-20%: +20分
            scores.loc[net_chg > 50] += 40.0
            scores.loc[(net_chg > 20) & (net_chg <= 50)] += 30.0
            scores.loc[(net_chg > 10) & (net_chg <= 20)] += 20.0
            scores.loc[(net_chg > 0) & (net_chg <= 10)] += 10.0

        if col_roe:
            roe = pd.to_numeric(df[col_roe], errors="coerce").fillna(0)
            # ROE > 15%: +30分, 10-15%: +20分, 5-10%: +10分
            scores.loc[roe > 15] += 30.0
            scores.loc[(roe > 10) & (roe <= 15)] += 20.0
            scores.loc[(roe > 5) & (roe <= 10)] += 10.0

        if col_gp:
            gp = pd.to_numeric(df[col_gp], errors="coerce").fillna(0)
            # 毛利率 > 30%: +15分, 15-30%: +10分
            scores.loc[gp > 30] += 15.0
            scores.loc[(gp > 15) & (gp <= 30)] += 10.0

        if col_rev_chg:
            rev_chg = pd.to_numeric(df[col_rev_chg], errors="coerce").fillna(0)
            scores.loc[rev_chg > 20] += 15.0
            scores.loc[(rev_chg > 10) & (rev_chg <= 20)] += 10.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        col_net_chg = next((c for c in df.columns if "净利润" in c and "增长" in c), None)
        col_roe = next((c for c in df.columns if "净资产收益率" in c), None)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if col_net_chg:
                v = df[col_net_chg].get(ts_code, 0)
                if v > 0:
                    r.append(f"净利润增长{v:.1f}%")
            if col_roe:
                v = df[col_roe].get(ts_code, 0)
                if v > 10:
                    r.append(f"ROE{v:.1f}%")
            if r:
                reasons[ts_code] = r
        return reasons