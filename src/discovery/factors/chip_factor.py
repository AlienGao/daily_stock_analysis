# -*- coding: utf-8 -*-
"""筹码因子 (Chip Structure / Winner Rate Factor).

盘后因子：基于筹码分布和胜率数据分析获利盘压力和反弹潜力。
数据来源: Tushare cyq_perf (293)
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class ChipFactor(BaseFactor):
    """筹码胜率因子。

    核心指标: winner_rate（获利比例）。
    - 胜率适中 (30%-70%): 获利盘不大，抛压适中
    - 胜率极低 (<15%): 深度套牢，反弹潜力大
    - 胜率极高 (>85%): 获利盘巨大，抛压风险
    """

    name = "chip"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_bulk_cyq_perf(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        winner_rate = df.get("winner_rate", pd.Series(50.0, index=df.index))
        cost_5 = df.get("cost_5pct", pd.Series(0, index=df.index))
        cost_95 = df.get("cost_95pct", pd.Series(0, index=df.index))
        weight_avg = df.get("weight_avg", pd.Series(1.0, index=df.index))

        # 胜率适中 30%-70%: 获利适中，抛压不大 (+10)
        scores.loc[(winner_rate >= 30) & (winner_rate <= 70)] += 10.0

        # 胜率极低 < 15%: 深度套牢，反弹潜力 (+15)
        scores.loc[winner_rate < 15] += 15.0

        # 胜率极高 > 85%: 获利盘巨大，抛压风险 (-10)
        scores.loc[winner_rate > 85] = (scores - 10).clip(0, 100)

        # 成本集中: (cost_95 - cost_5) / weight_avg < 0.2 (+10)
        cost_range = (cost_95 - cost_5).abs()
        concentration = cost_range / weight_avg.replace(0, 1)
        scores.loc[concentration < 0.2] += 10.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        winner_rate = df.get("winner_rate", pd.Series(50.0, index=df.index))
        cost_5 = df.get("cost_5pct", pd.Series(0, index=df.index))
        cost_95 = df.get("cost_95pct", pd.Series(0, index=df.index))
        weight_avg = df.get("weight_avg", pd.Series(1.0, index=df.index))

        cost_range = (cost_95 - cost_5).abs()
        concentration = cost_range / weight_avg.replace(0, 1)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            wr = winner_rate.get(ts_code, 50)
            if wr < 15:
                r.append(f"深度套牢(获利{wr:.0f}%)，反弹潜力大")
            elif 30 <= wr <= 70:
                r.append(f"获利适中({wr:.0f}%)，抛压不大")
            elif wr > 85:
                r.append(f"获利盘过大({wr:.0f}%)，注意抛压")

            if concentration.get(ts_code, 1.0) < 0.2:
                r.append("筹码高度集中")
            if r:
                reasons[ts_code] = r
        return reasons
