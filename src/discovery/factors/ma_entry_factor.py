# -*- coding: utf-8 -*-
"""均线买点因子 (MA Entry Factor).

核心盘中因子：在热门板块内找「均线附近、赔率好」的股票。
数据来源: stock_daily 历史日线 + realtime_spot 实时行情（本地计算 MA/KDJ/BOLL）
盘中可用，盘后不可用（盘后有技术面因子替代）。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class MaEntryFactor(BaseFactor):
    """均线买点因子。

    寻找均线多头排列、回踩均线附近、缩量企稳的买点信号。
    核心原则：不追高（乖离率>8%排除），不碰空头排列。
    MA5/MA10/MA20 从 stock_daily 历史日线收盘价实时计算。
    """

    name = "ma_entry"
    available_intraday = True
    available_postmarket = False
    weight = 35.0
    _LABEL_THRESHOLD = 5.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        from src.storage import DatabaseManager

        db_mgr = DatabaseManager()

        # ── 实时行情 ──
        spot = db_mgr.get_realtime_spot()
        if spot is None or spot.empty:
            logger.warning("[MaEntryFactor] 实时行情不可用，跳过")
            return None

        spot_cols = ["price", "volume"]
        if not all(c in spot.columns for c in spot_cols):
            logger.warning("[MaEntryFactor] 实时行情缺少 price/volume 列")
            return None

        result = spot[spot_cols].copy()
        result.columns = ["close", "vol"]
        result["vol"] = result["vol"] / 100.0  # 手

        # ── 盘中量能预估 ──
        elapsed = self._trading_minutes_elapsed()
        if elapsed >= 15:
            result["est_vol"] = result["vol"] * (240.0 / elapsed)
        else:
            result["est_vol"] = result["vol"]

        # ── MA + 近 5 日均量（stock_daily） ──
        close_matrix = db_mgr.get_recent_close_matrix(trade_date, 60)
        if close_matrix is not None and not close_matrix.empty:
            mas = self._compute_mas(close_matrix)
            result = result.merge(mas, left_index=True, right_index=True, how="left")

            avg_vol = self._get_avg_volume(db_mgr, trade_date)
            if avg_vol is not None and not avg_vol.empty:
                result["avg_vol"] = avg_vol
        else:
            logger.warning("[MaEntryFactor] close_matrix 不可用，跳过 MA/均量计算")

        # ── 本地 KDJ + BOLL（stock_daily + 实时行情覆盖） ──
        ohlc_matrix = db_mgr.get_recent_ohlc_matrix(trade_date, 30)
        if ohlc_matrix is not None and not ohlc_matrix.empty:
            kdj_df = self._compute_kdj(ohlc_matrix, spot)
            if kdj_df is not None and not kdj_df.empty:
                result = result.merge(kdj_df, left_index=True, right_index=True, how="left")
                logger.debug("[MaEntryFactor] KDJ 本地实时计算完成")

        if close_matrix is not None and not close_matrix.empty:
            boll_mid = self._compute_boll_mid(close_matrix, spot)
            if boll_mid is not None and not boll_mid.empty:
                result["boll_mid"] = boll_mid
                logger.debug("[MaEntryFactor] BOLL 中轨本地实时计算完成")

            mas_rt = self._compute_mas_realtime(close_matrix, spot)
            if mas_rt is not None and not mas_rt.empty:
                for col in ["ma5", "ma10", "ma20"]:
                    if col in mas_rt.columns:
                        m = mas_rt[col].notna()
                        result.loc[m, col] = mas_rt.loc[m, col]
                logger.debug("[MaEntryFactor] MA 实时计算完成")

        mask = result["close"].notna() & (result["close"] > 0)
        return result.loc[mask] if mask.any() else result

    @staticmethod
    def _get_avg_volume(db_mgr, trade_date: str, window: int = 5) -> "pd.Series":
        """从 stock_daily 获取每个 stock 在 trade_date 前 window 个自然日内各交易日的平均成交量。

        返回 Series: index=code, values=avg_volume（手）。
        stock_daily.volume 存储为股（手×100），此处除以 100 还原为手。
        """
        from datetime import datetime as dt, timedelta
        from sqlalchemy import text
        target = dt.strptime(trade_date, "%Y%m%d").date()
        cutoff = target - timedelta(days=window + 3)  # 留 buffer 覆盖非交易日
        with db_mgr.get_session() as s:
            rows = s.execute(
                text(
                    "SELECT code, volume FROM stock_daily "
                    "WHERE date >= :cutoff AND date < :target AND volume > 0 "
                    "ORDER BY code, date DESC"
                ),
                {"target": target, "cutoff": cutoff},
            ).fetchall()
            if not rows:
                return pd.Series(dtype=float)
            df = pd.DataFrame(rows, columns=["code", "volume"])
            df["volume"] = df["volume"] / 100.0
            # 每只股票取最近 window 个交易日的均值
            avg = df.groupby("code")["volume"].apply(
                lambda x: x.head(window).mean() if len(x) > 0 else x.mean()
            )
            return avg

    @staticmethod
    def _trading_minutes_elapsed() -> int:
        """A 股当日已过交易分钟数，排除午休（11:30-13:00）。

        9:30 前返回 0，15:00 后返回 240。
        """
        from datetime import datetime as dt
        from zoneinfo import ZoneInfo
        now = dt.now(ZoneInfo("Asia/Shanghai"))
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        morning_close = now.replace(hour=11, minute=30, second=0, microsecond=0)
        afternoon_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)

        if now < market_open:
            return 0
        if now > market_close:
            return 240
        if now <= morning_close:
            return int((now - market_open).total_seconds() / 60)
        return 120 + int((now - afternoon_open).total_seconds() / 60)

    @staticmethod
    def _compute_mas(close_matrix: pd.DataFrame) -> pd.DataFrame:
        """从收盘价矩阵计算最新 MA5/MA10/MA20。

        close_matrix: index=code, columns=date, values=close
        对每个 stock 取最后 N 个有效收盘价的均值。
        """
        mat_t = close_matrix.T
        ma5 = mat_t.rolling(window=5, min_periods=1).mean().iloc[-1]
        ma10 = mat_t.rolling(window=10, min_periods=1).mean().iloc[-1]
        ma20 = mat_t.rolling(window=20, min_periods=1).mean().iloc[-1]
        ma5.name, ma10.name, ma20.name = "ma5", "ma10", "ma20"
        return pd.concat([ma5, ma10, ma20], axis=1)

    @staticmethod
    def _compute_kdj(ohlc_matrix: pd.DataFrame, spot: pd.DataFrame, period: int = 9) -> pd.DataFrame:
        """用历史 OHLC + 当日实时 high/low/price 本地计算 KDJ。

        ohlc_matrix: MultiIndex (field, date), index=code (bare)
        spot: realtime_spot, index=code (bare), 需含 price/high/low 列
        返回 DataFrame: kdj_k, kdj_d, kdj_j, index=code
        """
        if ohlc_matrix is None or ohlc_matrix.empty:
            return pd.DataFrame()

        # 归一化 spot.index 为裸码，兼容 ts_code / bare code 两种格式
        spot = spot.copy()
        spot.index = spot.index.astype(str).str.replace(
            r"\.(SH|SZ|BJ)$", "", regex=True
        ).str.zfill(6)
        spot = spot[~spot.index.duplicated(keep="first")]

        high_df = ohlc_matrix.xs("high", axis=1, level=0)
        low_df = ohlc_matrix.xs("low", axis=1, level=0)
        close_df = ohlc_matrix.xs("close", axis=1, level=0)

        results = []
        for code in ohlc_matrix.index:
            try:
                h_hist = high_df.loc[code].dropna().values
                l_hist = low_df.loc[code].dropna().values
                c_hist = close_df.loc[code].dropna().values
            except (KeyError, ValueError):
                results.append({"kdj_k": np.nan, "kdj_d": np.nan, "kdj_j": np.nan})
                continue

            if len(c_hist) < 2:
                results.append({"kdj_k": np.nan, "kdj_d": np.nan, "kdj_j": np.nan})
                continue

            # 当日实时数据
            if code not in spot.index:
                results.append({"kdj_k": np.nan, "kdj_d": np.nan, "kdj_j": np.nan})
                continue
            rt_c = float(spot.at[code, "price"]) if "price" in spot.columns else np.nan
            rt_h = float(spot.at[code, "high"]) if "high" in spot.columns and pd.notna(spot.at[code, "high"]) else rt_c
            rt_l = float(spot.at[code, "low"]) if "low" in spot.columns and pd.notna(spot.at[code, "low"]) else rt_c

            if pd.isna(rt_c) or rt_c <= 0:
                results.append({"kdj_k": np.nan, "kdj_d": np.nan, "kdj_j": np.nan})
                continue

            # 拼接历史 + 当日
            h_all = np.append(h_hist, rt_h if rt_h > 0 else rt_c)
            l_all = np.append(l_hist, rt_l if rt_l > 0 else rt_c)
            c_all = np.append(c_hist, rt_c)
            n = len(c_all)

            # 滚动 H9 / L9
            _h = pd.Series(h_all)
            _l = pd.Series(l_all)
            h9 = _h.rolling(window=period, min_periods=1).max().values
            l9 = _l.rolling(window=period, min_periods=1).min().values

            denom = h9 - l9
            rsv = np.zeros(n)
            valid = denom > 0
            rsv[valid] = (c_all[valid] - l9[valid]) / denom[valid] * 100

            # 迭代 K / D（从 K=50, D=50 收敛）
            k_val, d_val = 50.0, 50.0
            for i in range(n):
                k_val = 2.0 / 3.0 * k_val + 1.0 / 3.0 * rsv[i]
                d_val = 2.0 / 3.0 * d_val + 1.0 / 3.0 * k_val
            j_val = 3.0 * k_val - 2.0 * d_val

            results.append({
                "kdj_k": round(float(k_val), 2),
                "kdj_d": round(float(d_val), 2),
                "kdj_j": round(float(j_val), 2),
            })

        return pd.DataFrame(results, index=ohlc_matrix.index)

    @staticmethod
    def _compute_boll_mid(close_matrix: pd.DataFrame, spot: pd.DataFrame, period: int = 20) -> "pd.Series":
        """用历史收盘价 + 当日实时价格计算 BOLL 中轨（MA20）。

        close_matrix: index=code, columns=date, values=close
        spot: realtime_spot, index=code (bare)
        返回 Series: boll_mid, index=code
        """
        # 归一化 spot.index 为裸码，兼容 ts_code / bare code
        spot = spot.copy()
        spot.index = spot.index.astype(str).str.replace(
            r"\.(SH|SZ|BJ)$", "", regex=True
        ).str.zfill(6)
        spot = spot[~spot.index.duplicated(keep="first")]

        results = {}
        for code in close_matrix.index:
            row = close_matrix.loc[code].dropna()
            if len(row) == 0:
                continue
            hist = row.values[-(period - 1):]

            if code in spot.index and "price" in spot.columns:
                rt_c = float(spot.at[code, "price"])
            else:
                rt_c = np.nan

            if pd.notna(rt_c) and rt_c > 0:
                results[code] = float(np.mean(np.append(hist, rt_c)))
            elif len(hist) > 0:
                results[code] = float(np.mean(hist))

        return pd.Series(results, name="boll_mid")

    @staticmethod
    def _compute_mas_realtime(close_matrix: pd.DataFrame, spot: pd.DataFrame) -> pd.DataFrame:
        """用历史收盘价 + 当日实时价格计算 MA5/MA10/MA20。

        close_matrix: index=code, columns=date, values=close
        spot: realtime_spot, index=code (bare)
        返回 DataFrame: ma5, ma10, ma20, index=code
        """
        # 归一化 spot.index 为裸码，兼容 ts_code / bare code
        spot = spot.copy()
        spot.index = spot.index.astype(str).str.replace(
            r"\.(SH|SZ|BJ)$", "", regex=True
        ).str.zfill(6)
        spot = spot[~spot.index.duplicated(keep="first")]

        results = {}
        for code in close_matrix.index:
            row = close_matrix.loc[code].dropna()
            if len(row) == 0:
                continue
            closes = row.values

            if code in spot.index and "price" in spot.columns:
                rt_c = float(spot.at[code, "price"])
            else:
                rt_c = np.nan

            mas = {}
            for period, name in [(5, "ma5"), (10, "ma10"), (20, "ma20")]:
                hist = closes[-(period - 1):] if len(closes) >= period - 1 else closes
                if pd.notna(rt_c) and rt_c > 0:
                    all_closes = np.append(hist, rt_c)
                    mas[name] = float(np.mean(all_closes[-period:]))
                elif len(hist) > 0:
                    mas[name] = float(np.mean(hist[-period:]))
                else:
                    mas[name] = np.nan
            results[code] = mas

        return pd.DataFrame.from_dict(results, orient="index")

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取均线买点信号，返回信号名 → boolean Series 的映射。"""
        idx = df.index
        false_s = pd.Series(False, index=idx)
        signals: Dict[str, pd.Series] = {}

        price = df.get("close", pd.Series(1.0, index=idx))
        boll_mid = df.get("boll_mid", pd.Series(np.nan, index=idx))
        kdj_j = df.get("kdj_j", pd.Series(50.0, index=idx))
        today_vol = df.get("est_vol", df.get("vol", pd.Series(0, index=idx)))
        avg_vol = df.get("avg_vol", pd.Series(0, index=idx))

        signals["kdj_j"] = kdj_j
        signals["kdj_oversold"] = kdj_j < 20

        has_ma = "ma5" in df.columns
        if not has_ma:
            for k in ("ma_valid", "bull_align", "ma_sticky", "near_ma5",
                       "near_ma10", "bear_align", "high_bias",
                       "vol_shrink_near_ma", "boll_support"):
                signals[k] = false_s
            return signals

        ma5 = df["ma5"]
        ma10 = df["ma10"]
        ma20 = df["ma20"]
        ma_valid = ma5.notna() & ma10.notna() & ma20.notna()
        signals["ma_valid"] = ma_valid

        # 多头排列
        bull_align = ma_valid & (ma5 > ma10) & (ma10 > ma20)
        signals["bull_align"] = bull_align

        # 空头排列
        bear_align = ma_valid & (ma5 < ma10) & (ma10 < ma20)
        signals["bear_align"] = bear_align

        # 乖离率 > 8%
        bias = (price - ma5) / ma5.replace(0, 1)
        signals["high_bias"] = ma_valid & (bias > 0.08)

        # 均线粘合: spread < 2%
        ma_max = pd.concat([ma5, ma10, ma20], axis=1).max(axis=1)
        ma_min = pd.concat([ma5, ma10, ma20], axis=1).min(axis=1)
        mid = (ma_max + ma_min) / 2
        spread = (ma_max - ma_min) / mid.replace(0, 1)
        signals["ma_sticky"] = ma_valid & (spread < 0.02)

        # 回踩 MA5: 现价距 MA5 < 2%
        bias_5 = (price - ma5).abs() / ma5.replace(0, 1)
        signals["near_ma5"] = ma_valid & (bias_5 < 0.02)

        # 回踩 MA10: 现价距 MA10 < 3%
        bias_10 = (price - ma10).abs() / ma10.replace(0, 1)
        signals["near_ma10"] = ma_valid & (bias_10 < 0.03)

        # 缩量回踩: 预估全天量 < 5日均量 × 0.8，且距 MA5 < 3%
        has_avg = avg_vol > 0
        vol_shrink = has_avg & (today_vol < avg_vol * 0.8)
        near_ma = ((price - ma5).abs() / ma5.replace(0, 1)) < 0.03
        signals["vol_shrink_near_ma"] = ma_valid & vol_shrink & near_ma

        # BOLL 中轨支撑: 价在中轨上方 2% 内，且 MA5 > MA10
        above_mid = price > boll_mid
        near_mid = (price - boll_mid).abs() / boll_mid.replace(0, 1) < 0.02
        mini_bull = ma_valid & (ma5 > ma10)
        signals["boll_support"] = mini_bull & above_mid & near_mid

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        scores = pd.Series(0.0, index=df.index, name=self.name)

        # 正向信号
        scores.loc[signals["bull_align"]] += 20.0
        scores.loc[signals["ma_sticky"]] += 15.0
        scores.loc[signals["near_ma5"]] += 25.0
        scores.loc[signals["near_ma10"]] += 20.0
        scores.loc[signals["vol_shrink_near_ma"]] += 15.0
        scores.loc[signals["boll_support"]] += 5.0
        scores.loc[signals["kdj_oversold"]] += 10.0

        # 排除条件：减分惩罚而非归零，避免掩藏「均线粘合」等有效子信号
        scores.loc[signals["bear_align"]] -= 25.0
        scores.loc[signals["high_bias"]] -= 30.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue
            r = []

            if signals["bull_align"].get(ts_code, False):
                r.append("均线多头排列")
            if signals["ma_sticky"].get(ts_code, False):
                r.append("均线粘合")
            if signals["near_ma5"].get(ts_code, False):
                r.append("回踩MA5均线")
            elif signals["near_ma10"].get(ts_code, False):
                r.append("回踩MA10均线")
            if signals["boll_support"].get(ts_code, False):
                r.append("BOLL中轨支撑")
            if signals["vol_shrink_near_ma"].get(ts_code, False):
                r.append("缩量回踩")
            j_val = signals["kdj_j"].get(ts_code, 50)
            if signals["kdj_oversold"].get(ts_code, False):
                r.append(f"KDJ超卖(J{j_val:.0f})")

            if r:
                reasons[ts_code] = r
        return reasons
