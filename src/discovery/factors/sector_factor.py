# -*- coding: utf-8 -*-
"""板块热度因子 (Sector Heat Factor).

基于涨跌停列表识别今日主线板块，输出板块热度评分作为选股范围权重。
数据来源: Tushare limit_list_d (298)
盘中可用，盘后不可用（盘后有独立的涨跌停因子）。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class SectorFactor(BaseFactor):
    """板块热度因子。

    不直接推荐涨停股，而是识别「今日主线板块」作为选股方向。
    输出板块得分字典（通过 context 传递给其他因子）。
    """

    name = "sector"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        df = tushare_fetcher.get_limit_list(trade_date, limit_type="U")
        if df is not None and not df.empty:
            df = df.copy()
            # 连板信息：limit_times 表示连续涨停次数
            if "limit_times" in df.columns:
                df["is_leader"] = df["limit_times"] >= 3
                df["is_2board"] = df["limit_times"] == 2
                df["is_first"] = df["limit_times"] == 1
        return df

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """返回全市场股票的板块热度加权分。

        由于没有直接的 ts_code → sector 映射，这里基于涨停股
        形成一个简化的热度信号传给其他因子。

        实际板块识别逻辑（涨停集中度等）会在 build_sector_scores 中完成，
        score() 返回一个占位 Series 让引擎可以合并。
        """
        result = pd.Series(0.0, index=df.index, name=self.name)
        # 涨停股本身获得基础分（涨停板上限计数）
        if "limit_times" in df.columns:
            result += df["limit_times"].fillna(0).clip(0, 5) * 5.0
        return result.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons
        limit_times = df.get("limit_times", pd.Series(0, index=df.index))
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            lt = int(limit_times.get(ts_code, 0))
            if lt >= 3:
                r.append(f"板块龙头({lt}连板)")
            elif lt >= 2:
                r.append(f"板块连板({lt}连板)")
            elif lt == 1:
                r.append("板块首板")
            if r:
                reasons[ts_code] = r
        return reasons
