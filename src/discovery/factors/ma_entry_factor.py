# -*- coding: utf-8 -*-
"""均线买点因子 (MA Entry Factor).

核心盘中因子：在热门板块内找「均线附近、赔率好」的股票。
轮次间 delta 感知：多头排列刚形成/刚突破MA5/KDJ超卖回升/缩量后放量 → 额外加分。
数据来源: stock_daily 历史日线 + realtime_spot 实时行情（本地计算 MA/KDJ/BOLL）
盘中可用，盘后不可用（盘后有技术面因子替代）。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor, ts_code_to_bare

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
    weight = 25.0
    _LABEL_THRESHOLD = 5.0

    def __init__(self):
        super().__init__()
        self._prev_ma_states: Dict[str, Dict[str, object]] = {}  # {bare_code: {bull_align, near_ma5, vol_shrink, kdj_j}}
        self._ma_trade_date: Optional[str] = None
        self._cached_transitions: Dict[str, Dict[str, bool]] = {}  # {bare: {bull_new, ma5_new, kdj_recovery, vol_breakout}}

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

        # ── 近 5 日均量（stock_daily） ──
        # 使用 raw close 价格，与 realtime_spot 的 raw price 保持一致，
        # 避免后复权历史 + raw 实时混合导致 MA/BOLL 在除权日附近失真。
        close_matrix = db_mgr.get_recent_close_matrix(trade_date, 60)
        if close_matrix is not None and not close_matrix.empty:
            avg_vol = self._get_avg_volume(db_mgr, trade_date)
            if avg_vol is not None and not avg_vol.empty:
                result["avg_vol"] = avg_vol
        else:
            logger.warning("[MaEntryFactor] close_matrix 不可用，跳过均量计算")

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
    def _compute_kdj(ohlc_matrix: pd.DataFrame, spot: pd.DataFrame, period: int = 9) -> pd.DataFrame:
        """用历史 OHLC + 当日实时 high/low/price 本地计算 KDJ。

        ohlc_matrix: MultiIndex (field, date), index=code (bare)
        spot: realtime_spot, index=code (bare), 需含 price/high/low 列
        返回 DataFrame: kdj_k, kdj_d, kdj_j, index=code
        """
        if ohlc_matrix is None or ohlc_matrix.empty:
            return pd.DataFrame()

        codes = ohlc_matrix.index.tolist()
        n_stocks = len(codes)
        if n_stocks == 0:
            return pd.DataFrame()

        # 归一化 spot.index 为裸码
        spot = spot.copy()
        spot.index = spot.index.astype(str).str.replace(
            r"\.(SH|SZ|BJ)$", "", regex=True
        ).str.zfill(6)
        spot = spot[~spot.index.duplicated(keep="first")]

        high_df = ohlc_matrix.xs("high", axis=1, level=0)
        low_df = ohlc_matrix.xs("low", axis=1, level=0)
        close_df = ohlc_matrix.xs("close", axis=1, level=0)

        all_dates = high_df.columns.sort_values()
        n_hist = len(all_dates)

        # 向量化：reindex 对齐所有 stock × date，缺失补 NaN → (n_hist, n_stocks) 数组
        high_arr = high_df.reindex(index=codes, columns=all_dates).to_numpy(dtype=float).T
        low_arr = low_df.reindex(index=codes, columns=all_dates).to_numpy(dtype=float).T
        close_arr = close_df.reindex(index=codes, columns=all_dates).to_numpy(dtype=float).T

        # 向量化：realtime 数据对齐（无 spot 的 stock 自动填 NaN）
        spot_align = spot.reindex(codes)
        rt_c = spot_align["price"].to_numpy(dtype=float, na_value=np.nan) if "price" in spot_align else np.full(n_stocks, np.nan)
        rt_h_raw = spot_align["high"].to_numpy(dtype=float, na_value=np.nan) if "high" in spot_align else rt_c.copy()
        rt_l_raw = spot_align["low"].to_numpy(dtype=float, na_value=np.nan) if "low" in spot_align else rt_c.copy()
        rt_h = np.where(np.isnan(rt_h_raw), rt_c, rt_h_raw)
        rt_l = np.where(np.isnan(rt_l_raw), rt_c, rt_l_raw)

        # 堆叠：历史行在前（旧→新），realtime 追加在最后一行 → (n_hist+1, n_stocks)
        h_all = np.vstack([high_arr, rt_h.reshape(1, -1)])
        l_all = np.vstack([low_arr, rt_l.reshape(1, -1)])
        c_all = np.vstack([close_arr, rt_c.reshape(1, -1)])

        # 滚动 H9/L9（向量化，min_periods=1 允许窗口内仅有部分数据）
        h9_all = pd.DataFrame(h_all).rolling(window=period, min_periods=1).max().to_numpy(dtype=float)
        l9_all = pd.DataFrame(l_all).rolling(window=period, min_periods=1).min().to_numpy(dtype=float)

        # RSV = (C - L9) / (H9 - L9) * 100
        denom = h9_all - l9_all
        rsv_mat = np.zeros_like(h9_all)
        valid = denom > 0
        rsv_mat[valid] = (c_all[valid] - l9_all[valid]) / denom[valid] * 100.0

        # 每只股票的首个有效行（历史数据不足的 stock 跳过前置 NaN 期，防止 K/D 衰减）
        valid_mask = ~np.isnan(c_all)
        first_valid = np.argmax(valid_mask, axis=0)
        all_nan = ~np.any(valid_mask, axis=0)
        first_valid[all_nan] = n_hist + 1  # 无任何有效数据的 stock 永不激活

        # K/D 迭代：仅在首条有效数据之后更新，保持 50 初始化
        k = np.full(n_stocks, 50.0)
        d = np.full(n_stocks, 50.0)
        n_total = n_hist + 1
        for t in range(n_total):
            active = t >= first_valid
            if not active.any():
                continue
            k_new = 2.0 / 3.0 * k + 1.0 / 3.0 * rsv_mat[t]
            d_new = 2.0 / 3.0 * d + 1.0 / 3.0 * k_new
            k = np.where(active, k_new, k)
            d = np.where(active, d_new, d)
        j = 3.0 * k - 2.0 * d

        # NaN 掩码：无实时价格或价格无效或无任何历史数据 → 全部 NaN
        no_data = np.isnan(rt_c) | (rt_c <= 0) | np.all(np.isnan(close_arr), axis=0)
        k[no_data] = np.nan
        d[no_data] = np.nan
        j[no_data] = np.nan

        return pd.DataFrame(
            {"kdj_k": np.round(k, 2), "kdj_d": np.round(d, 2), "kdj_j": np.round(j, 2)},
            index=codes,
        )

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
    # 轮次间 delta 感知（均线突破/金叉刚发生 → 额外加分）
    # ------------------------------------------------------------------

    def _compute_transition_bonus(self, df: pd.DataFrame,
                                   signals: Dict[str, pd.Series],
                                   trade_date: str) -> pd.Series:
        """对比上一轮信号状态，捕获「刚发生」的技术突破。

        Returns:
            per-stock transition bonus Series (0 ~ +16).
        """
        idx = df.index
        bonus = pd.Series(0.0, index=idx)

        # 跨日重置
        if self._ma_trade_date != trade_date:
            self._prev_ma_states.clear()
            self._ma_trade_date = trade_date

        prev_states = self._prev_ma_states
        new_states: Dict[str, Dict[str, object]] = {}
        cached_t: Dict[str, Dict[str, bool]] = {}

        for ts_code in idx:
            bare = ts_code_to_bare(str(ts_code))

            cur_bull = bool(signals["bull_align"].get(ts_code, False))
            cur_near_ma5 = bool(signals["near_ma5"].get(ts_code, False))
            cur_vol_shrink = bool(signals["vol_shrink_near_ma"].get(ts_code, False))
            cur_kdj_j = float(signals["kdj_j"].get(ts_code, 50))

            new_states[bare] = {
                "bull_align": cur_bull,
                "near_ma5": cur_near_ma5,
                "vol_shrink": cur_vol_shrink,
                "kdj_j": cur_kdj_j,
            }

            prev = prev_states.get(bare)
            t_flags: Dict[str, bool] = {
                "bull_new": False, "ma5_new": False,
                "kdj_recovery": False, "vol_breakout": False,
            }

            if prev is None:
                cached_t[bare] = t_flags
                continue  # 首轮不调整

            # 1. 多头排列刚形成: +5
            if cur_bull and not bool(prev.get("bull_align", False)):
                bonus.loc[ts_code] += 5.0
                t_flags["bull_new"] = True

            # 2. 刚突破/回踩 MA5: +3
            if cur_near_ma5 and not bool(prev.get("near_ma5", False)):
                bonus.loc[ts_code] += 3.0
                t_flags["ma5_new"] = True

            # 3. KDJ 超卖回升: +5 (J 从 <20 回升到 >=20 且继续上升)
            prev_j = float(prev.get("kdj_j", 50))
            if prev_j < 20 and cur_kdj_j >= 20 and cur_kdj_j > prev_j:
                bonus.loc[ts_code] += 5.0
                t_flags["kdj_recovery"] = True

            # 4. 缩量后放量: +3 (vol_shrink 从 True → False，资金开始活跃)
            if not cur_vol_shrink and bool(prev.get("vol_shrink", False)):
                bonus.loc[ts_code] += 3.0
                t_flags["vol_breakout"] = True

            cached_t[bare] = t_flags

        # 更新快照
        self._prev_ma_states = new_states
        self._cached_transitions = cached_t
        return bonus

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        trade_date = context.get("trade_date", "")
        signals = self._compute_signals(df)
        scores = pd.Series(0.0, index=df.index, name=self.name)

        # 正向信号
        scores.loc[signals["bull_align"]] += 20.0
        scores.loc[signals["ma_sticky"]] += 15.0
        scores.loc[signals["near_ma5"]] += 20.0
        scores.loc[signals["near_ma10"]] += 20.0
        scores.loc[signals["vol_shrink_near_ma"]] += 15.0
        scores.loc[signals["boll_support"]] += 10.0
        scores.loc[signals["kdj_oversold"]] += 10.0

        # 排除条件：减分惩罚而非归零，避免掩藏「均线粘合」等有效子信号
        scores.loc[signals["bear_align"]] -= 25.0
        scores.loc[signals["high_bias"]] -= 30.0

        # 轮次间突破溢价（刚形成多头排列/刚突破MA5/KDJ回升/缩量后放量）
        transition_bonus = self._compute_transition_bonus(df, signals, trade_date)
        scores = scores + transition_bonus

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        trade_date = context.get("trade_date", "")
        signals = self._compute_signals(df)
        transitions = getattr(self, "_cached_transitions", {}) or {}

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue
            bare = ts_code_to_bare(str(ts_code))
            t = transitions.get(bare, {})
            r = []

            # 均线信号 + 突破标记
            if signals["bull_align"].get(ts_code, False):
                r.append("多头排列(新形成)" if t.get("bull_new") else "均线多头排列")
            if signals["ma_sticky"].get(ts_code, False):
                r.append("均线粘合")
            if signals["near_ma5"].get(ts_code, False):
                r.append("刚突破MA5" if t.get("ma5_new") else "回踩MA5均线")
            elif signals["near_ma10"].get(ts_code, False):
                r.append("回踩MA10均线")
            if signals["boll_support"].get(ts_code, False):
                r.append("BOLL中轨支撑")
            if signals["vol_shrink_near_ma"].get(ts_code, False):
                r.append("缩量回踩")
            elif t.get("vol_breakout"):
                r.append("缩量后放量启动")
            j_val = signals["kdj_j"].get(ts_code, 50)
            if signals["kdj_oversold"].get(ts_code, False):
                r.append(f"KDJ超卖(J{j_val:.0f})")
            elif t.get("kdj_recovery"):
                r.append(f"KDJ超卖回升(J{j_val:.0f})")

            if r:
                reasons[ts_code] = r
        return reasons
