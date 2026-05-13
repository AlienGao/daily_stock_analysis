# -*- coding: utf-8 -*-
"""技术面因子 (Technical Factor).

盘后因子：基于 stk_factor 全套预计算指标评分。
数据来源: Tushare stk_factor (328)
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor, ts_code_to_bare

logger = logging.getLogger(__name__)


def _pct_rank(series: pd.Series) -> pd.Series:
    """返回 0-1 的百分位排名，处理全 NaN/全零边界。"""
    ranked = series.rank(pct=True)
    ranked = ranked.fillna(0.5)
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
    weight = 15.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def __init__(self):
        super().__init__()
        self._hist_data: Dict[str, pd.DataFrame] = {}

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        # 1. 优先读 DB 缓存
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df = db.get_tech_indicators_all(trade_date)
            if df is not None and not df.empty:
                logger.info("[TechnicalFactor] DB 命中: %d 条", len(df))
            else:
                df = None
        except Exception as e:
            logger.debug("[TechnicalFactor] DB 读取失败: %s", e)
            df = None

        # 2. DB 无数据，fallback Tushare
        if df is None:
            tushare_fetcher = kwargs.get("tushare_fetcher")
            if tushare_fetcher is None:
                return None
            df = tushare_fetcher.get_bulk_stk_factor(trade_date)
            if df is None or df.empty:
                return None

        # 3. 拉历史 close + macd_dif（60 日，用于背离检测）
        self._hist_data.clear()
        try:
            from datetime import datetime as _dt, timedelta as _td
            from src.storage import DatabaseManager as _DB
            from sqlalchemy import text as _text

            target_dt = _dt.strptime(trade_date, "%Y%m%d").date()
            hist_start = (target_dt - _td(days=90)).strftime("%Y-%m-%d")
            hist_end = target_dt.strftime("%Y-%m-%d")

            with _DB().get_session() as sess:
                rows = sess.execute(
                    _text(
                        "SELECT code, date, close_qfq, macd_dif FROM stock_tech_indicator "
                        "WHERE date >= :start AND date <= :end ORDER BY code, date"
                    ),
                    {"start": hist_start, "end": hist_end},
                ).fetchall()
            if rows:
                hist_df = pd.DataFrame(
                    rows, columns=["code", "date", "close", "dif"]
                )
                hist_df["code"] = hist_df["code"].astype(str).str.zfill(6)
                for code, grp in hist_df.groupby("code"):
                    self._hist_data[code] = grp.set_index("date").sort_index()
                logger.info(
                    "[TechnicalFactor] 历史数据: %d 只, %d 条",
                    len(self._hist_data), len(rows),
                )
        except Exception as e:
            logger.debug("[TechnicalFactor] 历史数据查询失败: %s", e)

        return df

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

        # 追踪各维度原始数据是否缺失（用于中性化）
        close_ok = close.notna() & (close > 0)
        macd_ok = macd_dif.notna() & macd_dea.notna()
        macd_hist_ok = macd_hist.notna()
        rsi_ok = rsi.notna()
        kdj_ok = kdj_k.notna() & kdj_d.notna()
        ma5 = df.get("ma5", pd.Series(np.nan, index=idx))
        ma10 = df.get("ma10", pd.Series(np.nan, index=idx))
        ma20 = df.get("ma20", pd.Series(np.nan, index=idx))
        ma_ok = ma5.notna() & ma10.notna() & ma20.notna()
        boll_ok = boll_u.notna() & boll_m.notna() & boll_l.notna()
        vol_ok = vol.notna() & (vol > 0)
        cci_ok = cci.notna()

        # 1. MACD 金叉强度 (0-12)
        dif_dea_gap = (macd_dif - macd_dea) / macd_dea.abs().replace(0, 1.0)
        golden = dif_dea_gap > 0
        s_macd_cross = zeros.copy()
        if golden.any():
            s_macd_cross[golden] = _pct_rank(dif_dea_gap[golden]) * 12.0
        s_macd_cross[~macd_ok] = 6.0
        signals["macd_cross"] = s_macd_cross

        # 2. MACD 动能柱 (0-8)
        hist_pos = macd_hist > 0
        s_macd_hist = zeros.copy()
        if hist_pos.any():
            s_macd_hist[hist_pos] = _pct_rank(macd_hist[hist_pos]) * 8.0
        s_macd_hist[~macd_hist_ok] = 4.0
        signals["macd_hist"] = s_macd_hist

        # 2b. MACD 背离 (底背离 +8 / 顶背离 -8)
        s_div_bull, s_div_bear = self._detect_divergence(idx, close, macd_dif, macd_dea)
        s_div_bull[~(close_ok & macd_ok)] = 4.0
        s_div_bear[~(close_ok & macd_ok)] = -4.0
        signals["macd_divergence_bull"] = s_div_bull
        signals["macd_divergence_bear"] = s_div_bear

        # 3. RSI 健康度 (0-12)：以 50 为中心的尖峰，越极端分越低
        rsi_dev = (rsi - 50).abs()
        s_rsi = (12.0 - rsi_dev / 25.0 * 12.0).clip(0, 12)
        s_rsi[~rsi_ok] = 6.0
        signals["rsi"] = s_rsi

        # 4. KDJ 超卖+金叉 (0-10)
        s_kdj_os = _linear_map(kdj_k, 20, 7, 45, 0).clip(0, 7)
        kdj_gap = kdj_k - kdj_d
        cross = (kdj_gap > 0) & (kdj_k < 50)
        s_kdj_cross = zeros.copy()
        if cross.any():
            s_kdj_cross[cross] = _pct_rank(kdj_gap[cross]) * 3.0
        s_kdj = s_kdj_os + s_kdj_cross
        s_kdj[~kdj_ok] = 5.0
        signals["kdj"] = s_kdj

        # 5. 均线多头排列 (0-10)
        s_ma = zeros.copy()
        full_bull = ma5 > ma10
        partial_bull = ma10 > ma20
        s_ma[full_bull & partial_bull] = 10.0
        s_ma[full_bull & ~partial_bull] = 5.0
        s_ma[~ma_ok] = 5.0
        signals["ma"] = s_ma

        # 6. BOLL 收窄 (0-10)：全市场百分位，带宽越窄分越高
        boll_width = (boll_u - boll_l) / boll_m.abs().replace(0, 1.0)
        s_boll_sqz = _pct_rank(-boll_width) * 10.0
        s_boll_sqz[~boll_ok] = 5.0
        signals["boll_squeeze"] = s_boll_sqz

        # 7. BOLL 下轨支撑 (0-10)：价在下轨内方有效，破位得 0
        boll_range = (boll_u - boll_l).abs().replace(0, 1.0)
        boll_pos = ((close - boll_l) / boll_range).clip(0, 1)
        s_boll_sup = _linear_map(boll_pos, 1, 0, 0, 10).clip(0, 10)
        s_boll_sup[~(close_ok & boll_ok)] = 5.0
        signals["boll_support"] = s_boll_sup

        # 8. 成交量活跃度 (0-10)：横截面百分位
        s_vol = zeros.copy()
        vol_pos = vol > 0
        if vol_pos.any():
            s_vol[vol_pos] = _pct_rank(vol[vol_pos]) * 10.0
        s_vol[~vol_ok] = 5.0
        signals["volume"] = s_vol

        # 9. 放量+BOLL 下轨共振加成 (0-6)
        s_vol_norm = _pct_rank(vol[vol_pos]).reindex(idx, fill_value=0) if vol_pos.any() else zeros
        boll_sup_norm = (_linear_map(boll_pos, 1, 0, 0, 1).clip(0, 1)
                         if vol_pos.any() else zeros)
        s_bonus = (s_vol_norm * boll_sup_norm * 6.0).fillna(0).clip(0, 6)
        s_bonus[~(vol_ok & close_ok & boll_ok)] = 3.0
        signals["vol_boll_bonus"] = s_bonus

        # 10. CCI 超卖 (0-10)
        s_cci = _linear_map(cci.clip(upper=-100), -200, 10, -100, 0).clip(0, 10)
        s_cci[~cci_ok] = 5.0
        signals["cci"] = s_cci

        return signals

    def _detect_divergence(self, idx: pd.Index, close: pd.Series,
                           macd_dif: pd.Series, macd_dea: pd.Series) -> tuple:
        """MACD 底背离 & 顶背离检测。

        底背离：价接近/跌破 20 日前低，DIF 高于前低
        - 背离 + 金叉确认（DIF>DEA）→ +8
        - 仅背离无确认 → +4
        顶背离：价接近/突破 20 日前高，DIF 低于前高
        - 背离 + 死叉确认（DIF<DEA）→ -8
        - 仅背离无确认 → -4
        """
        s_bull = pd.Series(0.0, index=idx)
        s_bear = pd.Series(0.0, index=idx)

        if not self._hist_data:
            return s_bull, s_bear

        for ts_code in idx:
            bare = ts_code_to_bare(str(ts_code))
            hist = self._hist_data.get(bare)
            if hist is None or len(hist) < 20:
                continue

            today_close = close.get(ts_code, np.nan)
            today_dif = macd_dif.get(ts_code, np.nan)
            today_dea = macd_dea.get(ts_code, np.nan)
            if pd.isna(today_close) or pd.isna(today_dif) or today_close <= 0:
                continue

            recent = hist.tail(20)
            if recent.empty:
                continue

            low_dt = recent["close"].idxmin()
            high_dt = recent["close"].idxmax()
            low_c = recent.loc[low_dt, "close"]
            low_d = recent.loc[low_dt, "dif"]
            high_c = recent.loc[high_dt, "close"]
            high_d = recent.loc[high_dt, "dif"]

            if pd.isna(low_d) or pd.isna(high_d):
                continue

            # 底背离
            if today_close <= low_c * 1.02 and today_dif > low_d:
                is_golden = not pd.isna(today_dea) and today_dif > today_dea
                s_bull.loc[ts_code] = 8.0 if is_golden else 4.0

            # 顶背离
            if today_close >= high_c * 0.98 and today_dif < high_d:
                is_dead = not pd.isna(today_dea) and today_dif < today_dea
                s_bear.loc[ts_code] = -8.0 if is_dead else -4.0

        return s_bull, s_bear

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
            ("macd_divergence_bull", "MACD底背离"),
            ("macd_divergence_bear", "MACD顶背离"),
            ("rsi", "RSI健康"),
            ("kdj", "KDJ超卖"),
            ("ma", "均线多头"),
            ("boll_squeeze", "BOLL收窄"),
            ("boll_support", "BOLL下轨支撑"),
            ("volume", "放量"),
            ("vol_boll_bonus", "放量反弹"),
            ("cci", "CCI超卖"),
        ]

        max_map = {
            "macd_cross": 12, "macd_hist": 8,
            "macd_divergence_bull": 8, "macd_divergence_bear": 8,
            "rsi": 12, "kdj": 10, "ma": 10,
            "boll_squeeze": 10, "boll_support": 10, "volume": 10,
            "vol_boll_bonus": 6, "cci": 10,
        }
        threshold = self._LABEL_THRESHOLD_RATIO

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            labels = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if abs(val) < max_map[key] * threshold:
                    continue
                if key == "macd_divergence_bull":
                    labels.append("MACD底背离，看涨反转")
                elif key == "macd_divergence_bear":
                    labels.append("MACD顶背离，注意回调")
                elif key == "rsi":
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
