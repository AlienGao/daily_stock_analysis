# -*- coding: utf-8 -*-
"""游资因子 (Hot Money Factor).

盘后因子：基于 Tushare 游资每日交易明细，识别游资关注的股票。
数据来源: Tushare hm_detail (doc_id=312)

游资质量加权：通过 HmTracker 历史回测统计各游资 T+1 胜率，
将「游资家数」升级为「质量加权共识」，区分游资优劣。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor
from src.discovery.hm_tracker import HmTracker

logger = logging.getLogger(__name__)


class HotMoneyFactor(BaseFactor):
    """游资因子。

    基于 Tushare 游资每日明细（hm_detail），聚合后百分位评分。
    关键信号：净买入额、游资质量加权共识、买入强度。
    """

    name = "hot_money"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """从 TushareFetcher 获取游资明细。"""
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_bulk_hm_detail(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """按 ts_code 聚合明细，百分位评分。

        三个子信号：
        - 游资净买入额 (40%) — 净买入越多越好
        - 游资质量加权 (30%) — 历史胜率高的游资买入权重更高
        - 买入强度 (30%) — 纯买入 vs 有买有卖

        净卖出惩罚：total_net < 0 → ×0.5
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        per_stock = df.groupby("ts_code").agg(
            total_net=("net_amount", "sum"),
            hm_count=("hm_name", "nunique"),
            total_buy=("buy_amount", "sum"),
            total_sell=("sell_amount", "sum"),
            hm_names=("hm_name", lambda x: "|".join(sorted(set(x)))),
        )

        total_volume = per_stock["total_buy"] + per_stock["total_sell"]

        def _pct(s: pd.Series) -> pd.Series:
            return s.rank(pct=True, na_option="bottom") * 100

        # 游资质量加权：用历史胜率替换原始家数
        quality_map = HmTracker.load_quality()
        quality_scores = []
        for names in per_stock["hm_names"]:
            score = sum(quality_map.get(n, 0.5) for n in names.split("|"))
            quality_scores.append(score)
        quality_pct = _pct(pd.Series(quality_scores, index=per_stock.index))

        scores = (
            _pct(per_stock["total_net"]) * 0.40
            + quality_pct * 0.30
            + _pct(per_stock["total_buy"] / total_volume.replace(0, float("nan"))) * 0.30
        )

        penalty = np.where(per_stock["total_net"] < 0, 0.5, 1.0)
        scores = scores * penalty

        self._last_hm_agg = per_stock
        return pd.Series(scores, index=per_stock.index, name=self.name).clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        per_stock = getattr(self, "_last_hm_agg", None)
        if per_stock is None:
            return reasons

        for i in range(len(scores)):
            if scores.iloc[i] <= 0:
                continue
            ts_code = scores.index[i]
            r = []
            net_val = per_stock["total_net"].iloc[i]
            count_val = int(per_stock["hm_count"].iloc[i])
            names_val = per_stock["hm_names"].iloc[i]

            if net_val > 1e8:
                r.append(f"游资净买入{net_val/1e8:.1f}亿")
            elif net_val > 1e4:
                r.append(f"游资净买入{net_val/1e4:.0f}万")
            elif net_val < -1e4:
                r.append(f"游资净卖出{abs(net_val)/1e4:.0f}万")

            if count_val > 1:
                r.append(f"{count_val}家游资({names_val})")

            if r:
                reasons[ts_code] = r
        return reasons
