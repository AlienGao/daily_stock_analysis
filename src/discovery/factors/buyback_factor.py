# -*- coding: utf-8 -*-
"""回购因子 (Buyback Factor).

盘后因子：基于东财股票回购数据，识别公司回购自家股票的股票。
数据来源: akshare stock_repurchase_em()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class BuybackFactor(BaseFactor):
    """回购因子。

    基于股票回购数据，识别公司用自有资金回购股份的行为。
    关键信号：回购进行中 + 占比高 = 公司认为股价被低估。
    """

    name = "buyback"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_buyback_data()

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 已回购数量
        col_bought = next((c for c in df.columns if "已回购股份数量" in c), None)
        # 已回购金额
        col_amount = next((c for c in df.columns if "已回购金额" in c), None)
        # 计划回购上限（占总股本比）
        col_plan_ratio = next((c for c in df.columns if "总股本比例" in c and "上限" in c), None)
        # 实施进度
        col_progress = next((c for c in df.columns if c == "实施进度"), None)

        if col_progress:
            # 实施中/完成 -> 加分
            progress = df[col_progress].astype(str)
            scores.loc[progress.str.contains("实施中", na=False)] += 30.0
            scores.loc[progress.str.contains("完成", na=False)] += 20.0

        if col_plan_ratio:
            plan_ratio = pd.to_numeric(df[col_plan_ratio], errors="coerce").fillna(0)
            scores.loc[plan_ratio > 3] += 35.0
            scores.loc[(plan_ratio > 1) & (plan_ratio <= 3)] += 20.0

        if col_amount:
            amount = pd.to_numeric(df[col_amount], errors="coerce").fillna(0)
            # 已回购金额 > 1亿: +35分, > 1000万: +20分
            scores.loc[amount > 1e8] += 35.0
            scores.loc[(amount > 1e7) & (amount <= 1e8)] += 20.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        col_progress = next((c for c in df.columns if c == "实施进度"), None)
        col_amount = next((c for c in df.columns if "已回购金额" in c), None)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if col_progress:
                v = str(df[col_progress].get(ts_code, ""))
                if v:
                    r.append(f"回购{v}")
            if col_amount:
                v = df[col_amount].get(ts_code, 0)
                if v > 0:
                    r.append(f"已回购{v/1e7:.1f}万元")
            if r:
                reasons[ts_code] = r
        return reasons