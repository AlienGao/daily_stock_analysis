# -*- coding: utf-8 -*-
"""技术面因子 (Technical Factor).

盘后因子：基于 stk_factor 全套预计算指标评分。
数据来源: Tushare stk_factor (328)
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _pct_rank(series: pd.Series) -> pd.Series:
    """返回 0-1 的百分位排名，处理全 NaN/全零边界。"""
    ranked = series.rank(pct=True)
    ranked = ranked.fillna(0.0)
    return ranked


def _linear_map(series: pd.Series, x0: float, y0: float,
                x1: float, y1: float, clip_low: float = 0.0,
                clip_high: float = 1e9) -> pd.Series:
    """两点线性映射，超出范围 clip。"""
    slope = (y1 - y0) / (x1 - x0) if x1 != x0 else 0.0
    result = y0 + slope * (series - x0)
    return result.clip(clip_low, clip_high)


class TechnicalFactor(BaseFactor):
    """技术面因子。

    stk_factor 提供 MACD/RSI/KDJ/BOLL/CCI 全套预计算指标（前复权）。
    """

    name = "technical"
    available_intraday = False
    available_postmarket = True
    weight = 25.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_bulk_stk_factor(trade_date)

    # ------------------------------------------------------------------
    # 共享信号提取（score 和 describe 共用）
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取并归一化 8 个子信号到各自满分区间。

        返回 dict: key=信号名, value=同 index 的 0~max_points Series。
        """
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        close = df.get("close", zeros)
        macd_dif = df.get("macd_dif", zeros)
        macd_dea = df.get("macd_dea", zeros)
        macd_hist = df.get("macd", zeros)
        rsi = df.get("rsi_12", pd.Series(50, index=idx))
        kdj_k = df.get("kdj_k", pd.Series(50, index=idx))
        kdj_d = df.get("kdj_d", pd.Series(50, index=idx))
        boll_u = df.get("boll_upper", zeros)
        boll_m = df.get("boll_mid", pd.Series(1.0, index=idx))
        boll_l = df.get("boll_lower", zeros)
        vol = df.get("vol", zeros)
        cci = df.get("cci", zeros)

        signals: Dict[str, pd.Series] = {}

        # 1. MACD 金叉强度 (0-12)
        dif_dea_gap = (macd_dif - macd_dea) / macd_dea.abs().replace(0, 1.0)
        golden = dif_dea_gap > 0
        s_macd_cross = zeros.copy()
        if golden.any():
            s_macd_cross[golden] = _pct_rank(dif_dea_gap[golden]) * 12.0
        signals["macd_cross"] = s_macd_cross

        # 2. MACD 动能柱 (0-8)
        hist_pos = macd_hist > 0
        s_macd_hist = zeros.copy()
        if hist_pos.any():
            s_macd_hist[hist_pos] = _pct_rank(macd_hist[hist_pos]) * 8.0
        signals["macd_hist"] = s_macd_hist

        # 3. RSI 健康度 (0-12)：梯形，25-40 爬坡/40-55 满/55-75 衰减
        s_rsi = zeros.copy()
        s_rsi = s_rsi.mask((rsi >= 25) & (rsi < 40),
                           _linear_map(rsi, 25, 4, 40, 12))
        s_rsi = s_rsi.mask((rsi >= 40) & (rsi < 55), 12.0)
        s_rsi = s_rsi.mask((rsi >= 55) & (rsi < 75),
                           _linear_map(rsi, 55, 12, 75, 0))
        signals["rsi"] = s_rsi

        # 4. KDJ 超卖+金叉 (0-15)
        s_kdj_os = _linear_map(kdj_k, 20, 10, 45, 0).clip(0, 10)
        kdj_cross = ((kdj_k > kdj_d) & (kdj_k < 50)).astype(float) * 5.0
        signals["kdj"] = s_kdj_os + kdj_cross

        # 5. BOLL 收窄 (0-10)
        boll_width = (boll_u - boll_l) / boll_m.abs().replace(0, 1.0)
        signals["boll_squeeze"] = _linear_map(boll_width, 0.3, 0, 0, 10).clip(0, 10)

        # 6. BOLL 下轨支撑 (0-10)
        boll_range = (boll_u - boll_l).abs().replace(0, 1.0)
        boll_pos = (close - boll_l) / boll_range
        signals["boll_support"] = _linear_map(boll_pos, 1, 0, 0, 10).clip(0, 10)

        # 7. 成交量活跃度 (0-10)：横截面百分位
        s_vol = zeros.copy()
        vol_pos = vol > 0
        if vol_pos.any():
            s_vol[vol_pos] = _pct_rank(vol[vol_pos]) * 10.0
        signals["volume"] = s_vol

        # 8. 放量+BOLL 下轨共振加成 (0-6)
        # 两者同时强势时几何加成，单边弱则压回
        s_vol_norm = _pct_rank(vol[vol_pos]) if vol_pos.any() else zeros
        boll_sup_norm = (_linear_map(boll_pos, 1, 0, 0, 1).clip(0, 1)
                         if vol_pos.any() else zeros)
        signals["vol_boll_bonus"] = (s_vol_norm * boll_sup_norm * 6.0).clip(0, 6)

        # 9. CCI 超卖 (0-15)
        signals["cci"] = _linear_map(cci.clip(upper=-100), -200, 15, -100, 0).clip(0, 15)

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        total = sum(signals.values()).clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        if df.empty:
            return {}

        signals = self._compute_signals(df)

        signal_meta = [
            ("macd_cross", "MACD金叉"),
            ("macd_hist", "MACD红柱"),
            ("rsi", "RSI健康"),
            ("kdj", "KDJ超卖"),
            ("boll_squeeze", "BOLL收窄"),
            ("boll_support", "BOLL下轨支撑"),
            ("volume", "放量"),
            ("vol_boll_bonus", "放量反弹"),
            ("cci", "CCI超卖"),
        ]

        max_map = {
            "macd_cross": 12, "macd_hist": 8, "rsi": 12, "kdj": 15,
            "boll_squeeze": 10, "boll_support": 10, "volume": 10,
            "vol_boll_bonus": 6, "cci": 15,
        }
        threshold = self._LABEL_THRESHOLD_RATIO

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            labels = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_map[key] * threshold:
                    continue
                if key == "rsi":
                    rsi_v = df.get("rsi_12", pd.Series(50, index=df.index)).get(ts_code, 50)
                    labels.append(f"{label}({rsi_v:.0f})")
                elif key == "kdj":
                    k_v = df.get("kdj_k", pd.Series(50, index=df.index)).get(ts_code, 50)
                    labels.append(f"{label}(K={k_v:.0f})")
                elif key == "cci":
                    cci_v = df.get("cci", pd.Series(0, index=df.index)).get(ts_code, 0)
                    labels.append(f"{label}({cci_v:.0f})")
                else:
                    labels.append(label)
            if labels:
                reasons[ts_code] = labels

        return reasons
