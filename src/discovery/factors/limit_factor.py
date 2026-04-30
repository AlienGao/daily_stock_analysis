# -*- coding: utf-8 -*-
"""涨跌停因子 (Limit Factor).

盘后因子：基于涨跌停数据识别强势股和风险股。
数据来源: Tushare limit_list_d (298)
盘后可用（权重低于盘中版）。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class LimitFactor(BaseFactor):
    """涨跌停因子（盘后版）。

    涨停: 强度信号（但需 LLM 判断次日溢价）
    连板: 龙头识别
    炸板过多: 分歧风险
    """

    name = "limit"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_limit_list(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        limit_type = df.get("limit", pd.Series("", index=df.index))
        limit_times = df.get("limit_times", pd.Series(0, index=df.index))
        open_times = df.get("open_times", pd.Series(0, index=df.index))

        # 涨停 (+20)
        is_up = limit_type == "U"
        scores.loc[is_up] += 20.0

        # 连板 ≥ 2 (+15)
        scores.loc[(limit_times >= 2) & is_up] += 15.0

        # 连板 ≥ 3: 额外龙头分 (+10)
        scores.loc[(limit_times >= 3) & is_up] += 10.0

        # 炸板次数 > 3: 分歧过大 (-20)
        scores.loc[open_times > 3] = (scores - 20).clip(0, 100)

        # 跌停: 排除
        is_down = limit_type == "D"
        scores.loc[is_down] = 0.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        limit_type = df.get("limit", pd.Series("", index=df.index))
        limit_times = df.get("limit_times", pd.Series(0, index=df.index))
        open_times = df.get("open_times", pd.Series(0, index=df.index))

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if limit_type.get(ts_code, "") == "U":
                lt = limit_times.get(ts_code, 0)
                if lt >= 3:
                    r.append(f"连板龙头({int(lt)}连板)")
                elif lt >= 2:
                    r.append(f"连板({int(lt)}连板)")
                else:
                    r.append("涨停")
            if open_times.get(ts_code, 0) > 3:
                r.append("炸板过多，分歧风险")
            if r:
                reasons[ts_code] = r
        return reasons
