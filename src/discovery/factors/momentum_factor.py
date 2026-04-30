# -*- coding: utf-8 -*-
"""强势启动因子 (Momentum / Breakout Factor).

在均线买点基础上叠加强势信号：资金流入、放量启动。
数据来源: Tushare moneyflow (170) + daily_basic
盘中可用。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class MomentumFactor(BaseFactor):
    """强势启动因子。

    检测资金流入、量比放大、换手健康、涨幅温和的启动信号。
    排除主力净流出、换手过低、涨幅接近涨停。
    """

    name = "momentum"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None

        mf = tushare_fetcher.get_bulk_money_flow(trade_date)
        db = tushare_fetcher.get_daily_basic_all(trade_date)
        ll = tushare_fetcher.get_limit_list(trade_date)

        if mf is None:
            return None

        result = mf.copy()

        # Merge daily_basic (turnover_rate, volume_ratio)
        if db is not None and not db.empty:
            for col in ["turnover_rate", "volume_ratio"]:
                if col in db.columns:
                    result[col] = db[col]

        # Merge limit data (pct_chg for the day)
        if ll is not None and not ll.empty and "pct_chg" in ll.columns:
            result["pct_chg"] = ll["pct_chg"]

        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
        sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))
        net_mf = df.get("net_mf_amount", pd.Series(0, index=df.index))

        major_net = (buy_elg - sell_elg) + (buy_lg - sell_lg)
        total_amount = buy_elg + sell_elg + buy_lg + sell_lg
        # Avoid division by zero
        total_amount_safe = total_amount.replace(0, 1)

        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))

        # 主力净流入 > 0 (+20)
        scores.loc[major_net > 0] += 20.0

        # 主力净流入率 > 10% (+25)
        inflow_rate = major_net / total_amount_safe
        scores.loc[inflow_rate > 0.10] += 25.0

        # 大单净流入 > 0 (+15)
        lg_net = buy_lg - sell_lg
        scores.loc[lg_net > 0] += 15.0

        # 量比 > 2: 放量启动 (+15)
        scores.loc[volume_ratio > 2] += 15.0
        # 量比 1.2-2: 温和放量 (+10)
        scores.loc[(volume_ratio >= 1.2) & (volume_ratio <= 2)] += 10.0

        # 换手率 3%-15%: 活跃但不失控 (+10)
        scores.loc[(turnover_rate >= 3) & (turnover_rate <= 15)] += 10.0

        # 涨幅 0%-3%: 刚启动 (+10)
        scores.loc[(pct_chg >= 0) & (pct_chg < 3)] += 10.0
        # 涨幅 3%-7%: 趋势确立 (+5)
        scores.loc[(pct_chg >= 3) & (pct_chg <= 7)] += 5.0

        # 排除: 主力净流出
        scores.loc[major_net < 0] = 0.0
        # 排除: 换手率 < 1% (无人关注)
        scores.loc[turnover_rate < 1] = 0.0
        # 涨幅 > 9%: 接近涨停 (-10)
        scores.loc[pct_chg > 9] = (scores - 10).clip(0, 100)

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons
        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
        sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))
        major_net = (buy_elg - sell_elg) + (buy_lg - sell_lg)
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if major_net.get(ts_code, 0) > 0:
                r.append("主力资金净流入")
            _vr = volume_ratio.get(ts_code, 1)
            if _vr > 2:
                r.append(f"放量启动(量比{_vr:.1f})")
            elif _vr >= 1.2:
                r.append(f"温和放量(量比{_vr:.1f})")
            _tr = turnover_rate.get(ts_code, 0)
            if 3 <= _tr <= 15:
                r.append(f"换手活跃({_tr:.1f}%)")
            _pct = pct_chg.get(ts_code, 0)
            if 0 <= _pct < 3:
                r.append(f"温和启动(涨幅{_pct:.1f}%)")
            elif 3 <= _pct <= 7:
                r.append(f"趋势确立(涨幅{_pct:.1f}%)")
            if r:
                reasons[ts_code] = r
        return reasons
