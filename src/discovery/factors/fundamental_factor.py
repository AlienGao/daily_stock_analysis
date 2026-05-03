# -*- coding: utf-8 -*-
"""基本面因子 (Fundamental Factor).

盘后因子：基于 PE/PB/换手率/市值等估值指标，识别低估值高性价比股票。
数据来源: Tushare daily_basic
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class FundamentalFactor(BaseFactor):
    """基本面因子。

    基于估值指标和规模指标，判断股票的基本面吸引力。
    关键信号：低 PE + 低 PB + 高换手率 = 价值+流动性兼备。
    """

    name = "fundamental"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_daily_basic_all(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        pe = df.get("pe", pd.Series(0.0, index=df.index))
        pb = df.get("pb", pd.Series(0.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0.0, index=df.index))
        total_mv = df.get("total_mv", pd.Series(0.0, index=df.index))

        # 低 PE 得分 (PE 0-15: +30分, 15-30: +15分, >50: 0分)
        scores.loc[pe <= 0] = 0.0
        valid_pe = pe > 0
        scores.loc[valid_pe & (pe <= 15)] += 30.0
        scores.loc[valid_pe & (pe > 15) & (pe <= 30)] += 15.0
        scores.loc[valid_pe & (pe > 30) & (pe <= 50)] += 5.0

        # 低 PB 得分 (PB < 1.5: +25分, 1.5-3: +15分, >5: 0分)
        valid_pb = pb > 0
        scores.loc[valid_pb & (pb < 1.5)] += 25.0
        scores.loc[valid_pb & (pb >= 1.5) & (pb < 3.0)] += 15.0
        scores.loc[valid_pb & (pb >= 3.0) & (pb <= 5.0)] += 5.0

        # 高换手率得分 (换手率 > 3%: +20分, 1-3%: +10分)
        scores.loc[turnover_rate > 3] += 20.0
        scores.loc[(turnover_rate > 1) & (turnover_rate <= 3)] += 10.0

        # 中小市值偏好 (+10分)：市值 10-200 亿
        # total_mv 单位是元，转为亿：除以 1e8
        mv_b = total_mv / 1e8
        scores.loc[(mv_b >= 10) & (mv_b <= 200)] += 10.0

        # 负面：PE 为负（亏损）扣分
        scores.loc[pe < 0] = (scores.loc[pe < 0] - 15).clip(0, 100)

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        pe = df.get("pe", pd.Series(0.0, index=df.index))
        pb = df.get("pb", pd.Series(0.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0.0, index=df.index))
        total_mv = df.get("total_mv", pd.Series(0.0, index=df.index))

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            pe_v = pe.get(ts_code, 0)
            pb_v = pb.get(ts_code, 0)
            tr_v = turnover_rate.get(ts_code, 0)
            mv_b = total_mv.get(ts_code, 0) / 1e8

            if pe_v > 0 and pe_v <= 15:
                r.append(f"低PE({pe_v:.1f})")
            elif pe_v > 0 and pe_v <= 30:
                r.append(f"适中PE({pe_v:.1f})")

            if pb_v > 0 and pb_v < 1.5:
                r.append(f"低PB({pb_v:.2f})")
            elif pb_v > 0 and pb_v < 3:
                r.append(f"适中PB({pb_v:.2f})")

            if tr_v > 3:
                r.append(f"高换手率({tr_v:.1f}%)")
            elif tr_v > 1:
                r.append(f"适中换手率({tr_v:.1f}%)")

            if mv_b >= 10 and mv_b <= 200:
                r.append(f"中小市值({mv_b:.0f}亿)")

            if pe_v < 0:
                r.append("亏损")

            if r:
                reasons[ts_code] = r
        return reasons