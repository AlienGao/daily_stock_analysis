# -*- coding: utf-8 -*-
"""炸板回封因子 (Limit-up Rebound Factor).

涨停打开后跌幅收窄、有大单回补，短线经典买点。
数据来源: Tushare limit_list_d (298), limit_type='Z'
盘中可用。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class ReboundFactor(BaseFactor):
    """炸板回封机会因子。

    检测涨停打开（炸板）后跌幅收窄 + 大单回补的短线买点。
    """

    name = "rebound"
    available_intraday = True
    available_postmarket = False
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None

        # 获取炸板股票 (limit_type='Z')
        df = tushare_fetcher.get_limit_list(trade_date, limit_type="Z")
        if df is not None and not df.empty:
            df = df.copy()
            # 合并资金流数据判断大单回补
            mf = tushare_fetcher.get_bulk_money_flow(trade_date)
            if mf is not None and not mf.empty:
                for col in ["buy_elg_amount", "sell_elg_amount",
                            "buy_lg_amount", "sell_lg_amount"]:
                    if col in mf.columns:
                        df[col] = mf[col]
        return df

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        open_times = df.get("open_times", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))

        # 炸板后横盘: 跌幅 < 3% (+20)
        # pct_chg for limit_type='Z' may still be positive (回封) or negative (开板跌)
        scores.loc[pct_chg > -3] += 20.0

        # 大单回补: 炸板后大单/特大单恢复净流入 (+25)
        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
        sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))
        major_net = (buy_elg - sell_elg) + (buy_lg - sell_lg)
        scores.loc[major_net > 0] += 25.0

        # 封板时间早 (< 10:30): 从 up_stat 字段推断
        up_stat = df.get("up_stat", pd.Series("", index=df.index))
        # up_stat 格式如 "1/1" 表示当天封板状态
        # 简化处理：有 up_stat 记录的加分
        has_stat = up_stat.notna() & (up_stat != "")
        scores.loc[has_stat] += 15.0

        # 换手充分: open_times 表示打开次数，炸板通常换手增加 (+10)
        scores.loc[open_times >= 1] += 10.0

        # 排除: 炸板后跌幅 > 5% (多头溃败)
        scores.loc[pct_chg < -5] = 0.0
        # 排除: 炸板次数 > 3 (分歧过大)
        scores.loc[open_times > 3] = 0.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons
        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
        sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))
        major_net = (buy_elg - sell_elg) + (buy_lg - sell_lg)
        open_times = df.get("open_times", pd.Series(0, index=df.index))
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            _pct = pct_chg.get(ts_code, 0)
            if _pct > -3:
                r.append(f"炸板后横盘(跌幅{_pct:.1f}%)")
            if major_net.get(ts_code, 0) > 0:
                r.append("大单回补")
            if int(open_times.get(ts_code, 0)) >= 1:
                r.append("换手充分")
            if r:
                reasons[ts_code] = r
        return reasons
