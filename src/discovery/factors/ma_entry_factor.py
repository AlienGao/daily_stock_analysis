# -*- coding: utf-8 -*-
"""均线买点因子 (MA Entry Factor).

核心盘中因子：在热门板块内找「均线附近、赔率好」的股票。
数据来源: Tushare stk_factor + daily_basic + stock_daily 历史日线 (MA计算)
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

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None

        tf = tushare_fetcher.get_bulk_stk_factor(trade_date)
        day_basic = tushare_fetcher.get_daily_basic_all(trade_date)

        if tf is None:
            return None

        result = tf.copy()
        if day_basic is not None and not day_basic.empty:
            for col in ["turnover_rate", "volume_ratio"]:
                if col in day_basic.columns:
                    result[col] = day_basic[col]

        # 辅助列：裸代码，用于与 DB 表 (index=code) 对齐
        result["_code"] = [str(x).split(".")[0].zfill(6) for x in result.index]

        # 统一 DB 连接
        db_mgr = None
        spot, close_matrix = None, None
        try:
            from src.storage import DatabaseManager
            db_mgr = DatabaseManager()
        except Exception:
            pass

        # ── 实时行情：价格/成交量替换 + high/low（KDJ用） ──
        if db_mgr:
            try:
                spot = db_mgr.get_realtime_spot()
            except Exception as e:
                logger.warning("[MaEntryFactor] 获取实时行情失败: %s", e)

        if spot is not None and not spot.empty:
            spot_cols = [c for c in ["price", "volume"] if c in spot.columns]
            if spot_cols:
                spot_rt = spot[spot_cols].rename(
                    columns={"price": "rt_price", "volume": "rt_volume"}
                )
                result = result.merge(spot_rt, left_on="_code", right_index=True, how="left")
                rt_mask = result["rt_price"].notna()
                result.loc[rt_mask, "close"] = result.loc[rt_mask, "rt_price"]
                result.loc[rt_mask, "vol"] = result.loc[rt_mask, "rt_volume"] / 100.0
                result = result.drop(columns=["rt_price", "rt_volume"])
                logger.debug("[MaEntryFactor] 已替换 %d 只股票的实时价格/成交量", rt_mask.sum())

                # 盘中量能预估：按已过交易时间比例推估全天量
                elapsed = self._trading_minutes_elapsed()
                if elapsed >= 15:
                    result["est_vol"] = result["vol"] * (240.0 / elapsed)
                else:
                    result["est_vol"] = result["vol"]

        # ── MA + 近 5 日均量（stock_daily） ──
        if db_mgr:
            try:
                close_matrix = db_mgr.get_recent_close_matrix(trade_date, 60)
                if close_matrix is not None and not close_matrix.empty:
                    mas = self._compute_mas(close_matrix)
                    result = result.merge(mas, left_on="_code", right_index=True, how="left")

                    avg_vol = self._get_avg_volume(db_mgr, trade_date)
                    if avg_vol is not None and not avg_vol.empty:
                        result = result.merge(
                            avg_vol.rename("avg_vol"), left_on="_code", right_index=True, how="left",
                        )
            except Exception as e:
                logger.warning("[MaEntryFactor] 计算MA失败: %s", e)

        # ── 本地 KDJ + BOLL（替换 stk_factor 的盘后值） ──
        if db_mgr and spot is not None and not spot.empty:
            try:
                ohlc_matrix = db_mgr.get_recent_ohlc_matrix(trade_date, 30)
                if ohlc_matrix is not None and not ohlc_matrix.empty:
                    kdj_df = self._compute_kdj(ohlc_matrix, spot)
                    if kdj_df is not None and not kdj_df.empty:
                        kdj_renamed = kdj_df.rename(columns=lambda c: f"_local_{c}")
                        result = result.merge(kdj_renamed, left_on="_code", right_index=True, how="left")
                        for col in ["kdj_k", "kdj_d", "kdj_j"]:
                            local_col = f"_local_{col}"
                            if local_col in result.columns:
                                m = result[local_col].notna()
                                result.loc[m, col] = result.loc[m, local_col]
                                result = result.drop(columns=[local_col])
                        logger.debug("[MaEntryFactor] KDJ本地实时计算完成")

                if close_matrix is not None and not close_matrix.empty:
                    boll_mid = self._compute_boll_mid(close_matrix, spot)
                    if boll_mid is not None and not boll_mid.empty:
                        result = result.merge(
                            boll_mid.rename("_local_bmid"), left_on="_code", right_index=True, how="left",
                        )
                        m = result["_local_bmid"].notna()
                        result.loc[m, "boll_mid"] = result.loc[m, "_local_bmid"]
                        result = result.drop(columns=["_local_bmid"])
                        logger.debug("[MaEntryFactor] BOLL中轨本地实时计算完成")

                    # MA5/10/20 纳入当日实时收盘价
                    mas_rt = self._compute_mas_realtime(close_matrix, spot)
                    if mas_rt is not None and not mas_rt.empty:
                        mas_rt_renamed = mas_rt.rename(columns=lambda c: f"_rt_{c}")
                        result = result.merge(mas_rt_renamed, left_on="_code", right_index=True, how="left")
                        for col in ["ma5", "ma10", "ma20"]:
                            rt_col = f"_rt_{col}"
                            if rt_col in result.columns:
                                m_ma = result[rt_col].notna()
                                result.loc[m_ma, col] = result.loc[m_ma, rt_col]
                                result = result.drop(columns=[rt_col])
                        logger.debug("[MaEntryFactor] MA实时计算完成")
            except Exception as e:
                logger.warning("[MaEntryFactor] 本地KDJ/BOLL计算失败: %s", e)

        result = result.drop(columns=["_code"])
        return result

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

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        price = df.get("close", pd.Series(1.0, index=df.index))
        boll_mid = df.get("boll_mid", pd.Series(price, index=df.index))

        has_ma = "ma5" in df.columns

        # --- 均线多头排列 (+20) ---
        if has_ma:
            ma5 = df["ma5"]
            ma10 = df["ma10"]
            ma20 = df["ma20"]
            ma_valid = ma5.notna() & ma10.notna() & ma20.notna()

            # 多头排列
            bull_align = ma_valid & (ma5 > ma10) & (ma10 > ma20)
            scores.loc[bull_align] += 20.0

            # 均线粘合: spread < 2% (+15)
            ma_max = pd.concat([ma5, ma10, ma20], axis=1).max(axis=1)
            ma_min = pd.concat([ma5, ma10, ma20], axis=1).min(axis=1)
            mid = (ma_max + ma_min) / 2
            spread = (ma_max - ma_min) / mid.replace(0, 1)
            scores.loc[ma_valid & (spread < 0.02)] += 15.0

            # 回踩 MA5: 现价距 MA5 < 2% (+25)
            bias_5 = (price - ma5).abs() / ma5.replace(0, 1)
            scores.loc[ma_valid & (bias_5 < 0.02)] += 25.0

            # 回踩 MA10: 现价距 MA10 < 3% (+20)
            bias_10 = (price - ma10).abs() / ma10.replace(0, 1)
            scores.loc[ma_valid & (bias_10 < 0.03)] += 20.0

            # 空头排列排除
            bear_align = ma_valid & (ma5 < ma10) & (ma10 < ma20)
            scores.loc[bear_align] = 0.0

            # 乖离率 > 8% 排除
            bias = (price - ma5) / ma5.replace(0, 1)
            scores.loc[ma_valid & (bias > 0.08)] = 0.0

            # 缩量回踩 (+15): 预估全天量 < 5 日均量 × 0.8
            today_vol = df.get("est_vol", df.get("vol", pd.Series(0, index=df.index)))
            avg_vol = df.get("avg_vol", pd.Series(0, index=df.index))
            has_avg = avg_vol > 0
            vol_shrink = has_avg & (today_vol < avg_vol * 0.8)
            near_ma = ((price - ma5).abs() / ma5.replace(0, 1)) < 0.03
            scores.loc[ma_valid & vol_shrink & near_ma] += 15.0

            # --- BOLL 中轨支撑 (+5): 价在中轨上方 2% 内，且 MA5 > MA10 ---
            above_mid = price > boll_mid
            near_mid = (price - boll_mid).abs() / boll_mid.replace(0, 1) < 0.02
            mini_bull = ma_valid & (ma5 > ma10)
            scores.loc[mini_bull & above_mid & near_mid] += 5.0

        # --- KDJ J 线超卖 (+10): J < 20 ---
        kdj_j = df.get("kdj_j", pd.Series(50.0, index=df.index))
        scores.loc[kdj_j < 20] += 10.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons
        price = df.get("close", pd.Series(1.0, index=df.index))
        kdj_j = df.get("kdj_j", pd.Series(50.0, index=df.index))
        boll_mid = df.get("boll_mid", pd.Series(price, index=df.index))
        has_ma = "ma5" in df.columns

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []

            if has_ma and ts_code in df.index:
                _ma5 = df.loc[ts_code, "ma5"]
                _ma10 = df.loc[ts_code, "ma10"]
                _ma20 = df.loc[ts_code, "ma20"]
                if pd.notna(_ma5) and pd.notna(_ma10) and pd.notna(_ma20):
                    if _ma5 > _ma10 > _ma20:
                        r.append("均线多头排列")
                    _p = price.get(ts_code, 0)
                    if _ma5 > 0 and abs(_p - _ma5) / _ma5 < 0.02:
                        r.append("回踩MA5均线")
                    elif _ma10 > 0 and abs(_p - _ma10) / _ma10 < 0.03:
                        r.append("回踩MA10均线")

                    # BOLL 中轨支撑 + MA5 > MA10
                    _bm = boll_mid.get(ts_code, 0)
                    if _ma5 > _ma10 and 0 <= (_p - _bm) / _bm < 0.02:
                        r.append("BOLL中轨支撑")

            # 缩量回踩信号
            if has_ma and ts_code in df.index and "avg_vol" in df.columns:
                _avg = df.loc[ts_code, "avg_vol"]
                _vol = df.loc[ts_code, "vol"]
                if pd.notna(_avg) and _avg > 0 and _vol < _avg * 0.8:
                    r.append("缩量回踩")

            _kdj_j = kdj_j.get(ts_code, 50)
            if _kdj_j < 20:
                r.append(f"KDJ超卖(J{_kdj_j:.0f})")
            if r:
                reasons[ts_code] = r
        return reasons
