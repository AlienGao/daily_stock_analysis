# -*- coding: utf-8 -*-
"""技术面因子 (Technical Factor).

盘后因子：基于 stk_factor 全套预计算指标评分。
数据来源: Tushare stk_factor (328)
替代本地 StockTrendAnalyzer 的 rolling/ewm 手工计算。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class TechnicalFactor(BaseFactor):
    """技术面因子。

    stk_factor 提供 MACD/RSI/KDJ/BOLL/CCI 全套预计算指标（前复权）。
    MA 均线由本地 StockTrendAnalyzer 计算补充。
    """

    name = "technical"
    available_intraday = False
    available_postmarket = True
    weight = 25.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_bulk_stk_factor(trade_date)

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        macd_dif = df.get("macd_dif", pd.Series(0, index=df.index))
        macd_dea = df.get("macd_dea", pd.Series(0, index=df.index))
        macd = df.get("macd", pd.Series(0, index=df.index))
        rsi_12 = df.get("rsi_12", pd.Series(50, index=df.index))
        kdj_k = df.get("kdj_k", pd.Series(50, index=df.index))
        boll_upper = df.get("boll_upper", pd.Series(0, index=df.index))
        boll_mid = df.get("boll_mid", pd.Series(0, index=df.index))
        boll_lower = df.get("boll_lower", pd.Series(0, index=df.index))
        close = df.get("close", pd.Series(0, index=df.index))
        vol = df.get("vol", pd.Series(0, index=df.index))
        cci = df.get("cci", pd.Series(0, index=df.index))

        # MACD 金叉: dif 上穿 dea (+15)
        golden_cross = macd_dif > macd_dea
        scores.loc[golden_cross] += 15.0

        # MACD 柱 > 0 (+10)
        scores.loc[macd > 0] += 10.0

        # RSI 健康: 40 < RSI12 < 70 (+10)
        scores.loc[(rsi_12 > 40) & (rsi_12 < 70)] += 10.0

        # KDJ 低位超卖: KDJ_K < 30 (+10)
        scores.loc[kdj_k < 30] += 10.0

        # BOLL 中轨支撑: close 在 mid 上方 2% 内 (+5)
        boll_pct = (close - boll_mid) / boll_mid.replace(0, 1)
        scores.loc[(boll_pct >= 0) & (boll_pct < 0.02)] += 5.0

        # BOLL 收窄: (upper - lower) / mid < 0.2 (+5)
        boll_width = (boll_upper - boll_lower) / boll_mid.replace(0, 1)
        scores.loc[boll_width < 0.2] += 5.0

        # 放量: vol 环比增长 (因无法轻松计算环比，用 vol > 0 占位) (+5)
        scores.loc[vol > 0] += 5.0

        # CCI 超卖: cci < -100 (+5)
        scores.loc[cci < -100] += 5.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        macd_dif = df.get("macd_dif", pd.Series(0, index=df.index))
        macd_dea = df.get("macd_dea", pd.Series(0, index=df.index))
        macd = df.get("macd", pd.Series(0, index=df.index))
        rsi_12 = df.get("rsi_12", pd.Series(50, index=df.index))
        kdj_k = df.get("kdj_k", pd.Series(50, index=df.index))
        boll_upper = df.get("boll_upper", pd.Series(0, index=df.index))
        boll_mid = df.get("boll_mid", pd.Series(0, index=df.index))
        boll_lower = df.get("boll_lower", pd.Series(0, index=df.index))
        close = df.get("close", pd.Series(0, index=df.index))
        cci = df.get("cci", pd.Series(0, index=df.index))

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if macd_dif.get(ts_code, 0) > macd_dea.get(ts_code, 0):
                r.append("MACD金叉")
            if macd.get(ts_code, 0) > 0:
                r.append("MACD红柱")
            _rsi = rsi_12.get(ts_code, 50)
            if 40 < _rsi < 70:
                r.append(f"RSI健康({_rsi:.0f})")
            _kdj = kdj_k.get(ts_code, 50)
            if _kdj < 30:
                r.append(f"KDJ超卖({_kdj:.0f})")
            _boll_mid = boll_mid.get(ts_code, 1)
            _close = close.get(ts_code, 0)
            if _boll_mid > 0 and _close > 0:
                boll_pct = (_close - _boll_mid) / _boll_mid
                if 0 <= boll_pct < 0.02:
                    r.append("BOLL中轨支撑")
            _upper = boll_upper.get(ts_code, 0)
            _lower = boll_lower.get(ts_code, 0)
            if _boll_mid > 0 and (_upper - _lower) / _boll_mid < 0.2:
                r.append("BOLL收窄")
            _cci = cci.get(ts_code, 0)
            if _cci < -100:
                r.append(f"CCI超卖({_cci:.0f})")
            if r:
                reasons[ts_code] = r
        return reasons
