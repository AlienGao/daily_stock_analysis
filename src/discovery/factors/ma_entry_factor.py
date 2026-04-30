# -*- coding: utf-8 -*-
"""均线买点因子 (MA Entry Factor).

核心盘中因子：在热门板块内找「均线附近、赔率好」的股票。
数据来源: Tushare stk_factor (328) + daily_basic (换手率/量比)
盘中可用，盘后不可用（盘后有技术面因子替代）。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class MaEntryFactor(BaseFactor):
    """均线买点因子。

    寻找均线多头排列、回踩均线附近、缩量企稳的买点信号。
    核心原则：不追高（乖离率>8%排除），不碰空头排列。
    """

    name = "ma_entry"
    available_intraday = True
    available_postmarket = False
    weight = 35.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None

        # 获取技术面因子 (含 close, kdj, boll, cci)
        tf = tushare_fetcher.get_bulk_stk_factor(trade_date)
        # 获取每日指标 (含 turnover_rate, volume_ratio)
        db = tushare_fetcher.get_daily_basic_all(trade_date)

        if tf is None:
            return None

        result = tf.copy()
        if db is not None and not db.empty:
            for col in ["turnover_rate", "volume_ratio"]:
                if col in db.columns:
                    result[col] = db[col]
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        price = df.get("close", pd.Series(1.0, index=df.index))
        kdj_k = df.get("kdj_k", pd.Series(50.0, index=df.index))
        volume = df.get("vol", pd.Series(0, index=df.index))
        boll_mid = df.get("boll_mid", pd.Series(price, index=df.index))
        turnover = df.get("turnover_rate", pd.Series(0, index=df.index))

        # --- 均线位置信号 (需要从 pro_bar 获取 MA 值) ---
        # MA values come from daily bar data via context
        bar_df = context.get("bar_data")
        if bar_df is not None and not bar_df.empty and "close" in bar_df.columns:
            ma5 = bar_df.get("ma5", pd.Series(price, index=df.index))
            ma10 = bar_df.get("ma10", pd.Series(price, index=df.index))
            ma20 = bar_df.get("ma20", pd.Series(price, index=df.index))

            # 多头排列: MA5 > MA10 > MA20 (+20)
            bull_align = (ma5 > ma10) & (ma10 > ma20)
            scores.loc[bull_align] += 20.0

            # 均线粘合: spread < 3% (+15)
            ma_max = pd.concat([ma5, ma10, ma20], axis=1).max(axis=1)
            ma_min = pd.concat([ma5, ma10, ma20], axis=1).min(axis=1)
            spread = (ma_max - ma_min) / ma_min.replace(0, 1)
            scores.loc[spread < 0.03] += 15.0

            # 回踩 MA5: 现价距 MA5 < 2% (+25)
            bias_5 = (price - ma5).abs() / ma5.replace(0, 1)
            scores.loc[bias_5 < 0.02] += 25.0

            # 回踩 MA10: 现价距 MA10 < 3% (+20)
            bias_10 = (price - ma10).abs() / ma10.replace(0, 1)
            scores.loc[bias_10 < 0.03] += 20.0

            # 空头排列排除: MA5 < MA10 < MA20 → 0 分（不扣分，但之前加分无效）
            bear_align = (ma5 < ma10) & (ma10 < ma20)
            scores.loc[bear_align] = 0.0

            # 乖离率 > 8% 排除
            bias = (price - ma5) / ma5.replace(0, 1)
            scores.loc[bias > 0.08] = 0.0

        # --- KDJ 低位超卖 (+10) ---
        scores.loc[kdj_k < 30] += 10.0

        # --- 缩量回踩: 均线回踩 + 缩量 (+15) ---
        if bar_df is not None and "ma5" in (bar_df.columns if bar_df is not None else []):
            vol_prev = volume.shift(1).fillna(volume)
            vol_shrink = volume < vol_prev * 0.8
            near_ma = ((price - ma5).abs() / ma5.replace(0, 1)) < 0.03 if "ma5" in (bar_df.columns if bar_df is not None else []) else pd.Series(False, index=df.index)
            scores.loc[vol_shrink & near_ma] += 15.0

        # --- BOLL 中轨支撑 (+5) ---
        above_mid = price > boll_mid
        near_mid = (price - boll_mid).abs() / boll_mid.replace(0, 1) < 0.02
        scores.loc[above_mid & near_mid] += 5.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons
        price = df.get("close", pd.Series(1.0, index=df.index))
        kdj_k = df.get("kdj_k", pd.Series(50.0, index=df.index))
        boll_mid = df.get("boll_mid", pd.Series(price, index=df.index))
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            bar_df = context.get("bar_data")
            if bar_df is not None and not bar_df.empty:
                ma5 = bar_df.get("ma5", pd.Series(index=df.index))
                ma10 = bar_df.get("ma10", pd.Series(index=df.index))
                ma20 = bar_df.get("ma20", pd.Series(index=df.index))
                _ma5 = ma5.get(ts_code, 0)
                _ma10 = ma10.get(ts_code, 0)
                _ma20 = ma20.get(ts_code, 0)
                if _ma5 > _ma10 > _ma20 > 0:
                    r.append("均线多头排列")
                _price = price.get(ts_code, 0)
                if _ma5 > 0 and abs(_price - _ma5) / _ma5 < 0.02:
                    r.append("回踩MA5均线")
                elif _ma10 > 0 and abs(_price - _ma10) / _ma10 < 0.03:
                    r.append("回踩MA10均线")
            _kdj = kdj_k.get(ts_code, 50)
            if _kdj < 30:
                r.append(f"KDJ超卖({_kdj:.0f})")
            _bm = boll_mid.get(ts_code, 0)
            _p = price.get(ts_code, 0)
            if _bm > 0 and 0 <= (_p - _bm) / _bm < 0.02:
                r.append("BOLL中轨支撑")
            if r:
                reasons[ts_code] = r
        return reasons
