# -*- coding: utf-8 -*-
"""资金流向因子 (Money Flow Factor).

盘后因子：全市场资金流向分析，识别主力建仓和散户接盘。
数据来源: Tushare moneyflow (170)
兜底: efinance 资金流
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class MoneyFlowFactor(BaseFactor):
    """资金流向因子。

    基于特大单/大单/中单/小单买卖数据，判断资金结构与方向。
    关键信号：特大单净流入 + 大单净流入 = 主力建仓。
    危险信号：特大单流出 + 小单流入 = 散户接盘。
    """

    name = "money_flow"
    available_intraday = False
    available_postmarket = True
    weight = 25.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_bulk_money_flow(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
        sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))
        buy_sm = df.get("buy_sm_amount", pd.Series(0, index=df.index))
        sell_sm = df.get("sell_sm_amount", pd.Series(0, index=df.index))
        net_mf = df.get("net_mf_amount", pd.Series(0, index=df.index))

        elg_net = buy_elg - sell_elg
        lg_net = buy_lg - sell_lg
        sm_net = buy_sm - sell_sm
        major_net = elg_net + lg_net
        total_trade = buy_elg + sell_elg + buy_lg + sell_lg + buy_sm + sell_sm

        # 特大单向流入 (+25)
        scores.loc[elg_net > 0] += 25.0

        # 主力净流入率 > 10% (+25)
        mf_ratio = (major_net / total_trade.replace(0, 1)) * 100
        scores.loc[mf_ratio > 10] += 25.0

        # 大单净流入 > 0 (+15)
        scores.loc[lg_net > 0] += 15.0

        # 散户接盘预警: 特大单流出 + 小单流入 (-20)
        retail_trap = (elg_net < 0) & (sm_net > 0)
        scores.loc[retail_trap] = (scores - 20).clip(0, 100)

        # 全市场净流入基础分
        scores.loc[net_mf > 0] += 10.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
        sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))
        buy_sm = df.get("buy_sm_amount", pd.Series(0, index=df.index))
        sell_sm = df.get("sell_sm_amount", pd.Series(0, index=df.index))

        elg_net = buy_elg - sell_elg
        lg_net = buy_lg - sell_lg
        sm_net = buy_sm - sell_sm
        major_net = elg_net + lg_net
        total_trade = buy_elg + sell_elg + buy_lg + sell_lg + buy_sm + sell_sm
        mf_ratio = (major_net / total_trade.replace(0, 1)) * 100

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if elg_net.get(ts_code, 0) > 0:
                r.append("特大单净流入")
            if mf_ratio.get(ts_code, 0) > 10:
                r.append("主力净流入率>10%")
            if lg_net.get(ts_code, 0) > 0:
                r.append("大单净流入")
            if sm_net.get(ts_code, 0) > 0 and elg_net.get(ts_code, 0) < 0:
                r.append("散户接盘预警")
            if r:
                reasons[ts_code] = r
        return reasons
