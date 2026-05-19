# -*- coding: utf-8 -*-
"""GTJA191 Alpha 114 因子 (GTJA Alpha 114 Factor).

盘后因子：RANK(DELAY(hl_ratio, 2)) * RANK(RANK(VOLUME)) / (hl_ratio / (VWAP - CLOSE))
其中 hl_ratio = (HIGH - LOW) / (SUM(CLOSE, 5) / 5)
数据来源: stock_daily (high, low, close, volume, amount × 7 交易日)。

评分逻辑：
- hl_ratio：5 日均价的相对振幅；
- 分子：延迟 2 日的 hl_ratio 排名 × 双次成交量排名 → 捕捉滞后振幅+高换手；
- 分母：当日 hl_ratio / VWAP 偏离 → 振幅被 VWAP 偏离消化则分母变小、总分为高；
- 综合：高振幅滞后 + 高成交量 + 当前 VWAP 偏离消化 → 高分（潜在反转/突破信号）。
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_TOTAL_DAYS = 7
_MIN_DAYS = 5
_DELAY = 2
_CLOSE_AVG_WINDOW = 5


class Gtja114Factor(BaseFactor):
    """GTJA191 Alpha 114 因子。

    RANK(DELAY(hl_ratio, 2)) * RANK(RANK(VOLUME)) / (hl_ratio / (VWAP - CLOSE))
    捕捉延迟振幅 + 成交量放大 + VWAP 偏离消化的复合反转信号。
    """

    name = "gtja114"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取近 7 个交易日每只股票的高、低、收、量、额。

        Returns:
            DataFrame index=ts_code, columns=high_d0..high_d6, low_d0.., close_d0.., vol_d0.., amt_d0..
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        target_dt = datetime.strptime(trade_date, "%Y%m%d").date()

        with db.get_session() as s:
            trading_dates = [
                row[0] for row in s.execute(
                    text(
                        "SELECT DISTINCT date FROM stock_daily "
                        "WHERE date <= :target ORDER BY date DESC LIMIT :limit"
                    ),
                    {"target": target_dt, "limit": _TOTAL_DAYS},
                ).fetchall()
            ]

            if len(trading_dates) < _MIN_DAYS:
                logger.warning("[Gtja114] 交易日数据不足 (target=%s, got=%d)",
                               target_dt, len(trading_dates))
                return None

            placeholders = ",".join(f":d{i}" for i in range(len(trading_dates)))
            params = {f"d{i}": d for i, d in enumerate(trading_dates)}
            rows = s.execute(
                text(
                    f"SELECT code, date, high, low, close, volume, amount FROM stock_daily "
                    f"WHERE date IN ({placeholders}) ORDER BY code, date DESC"
                ),
                params,
            ).fetchall()

        if not rows:
            logger.warning("[Gtja114] stock_daily 无数据 (%s)", trade_date)
            return None

        df = pd.DataFrame(rows, columns=["code", "date", "high", "low", "close", "volume", "amount"])
        df = df.drop_duplicates(subset=["code", "date"], keep="last")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)

        dates_sorted = sorted(df["date"].unique(), reverse=True)

        for col in ["high", "low", "close", "volume", "amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        records = {}
        for _, row in df.iterrows():
            code = row["code"]
            row_date = row["date"]
            if isinstance(row_date, pd.Timestamp):
                row_date = row_date.date()
            try:
                day_idx = dates_sorted.index(row_date)
            except ValueError:
                continue
            if day_idx < _TOTAL_DAYS:
                rec = records.setdefault(code, {})
                for col in ["high", "low", "close", "volume", "amount"]:
                    val = row[col]
                    rec[f"{col}_d{day_idx}"] = float(val) if pd.notna(val) else np.nan

        if not records:
            return None

        result = pd.DataFrame.from_dict(records, orient="index")
        result.index.name = "ts_code"

        required_cols = ["high_d0", "low_d0", "close_d0", "volume_d0", "amount_d0"]
        result = result.dropna(subset=required_cols)

        codes = result.index.astype(str).str.zfill(6)
        pre2 = codes.str[:2]
        suffix = pre2.map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        result.index = codes + suffix

        logger.info(
            "[Gtja114] 数据组装完成: %d 个交易日, %d 只股票",
            len(dates_sorted), len(result),
        )
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算 GTJA114 因子评分。

        1. hl_ratio = (high - low) / (avg_close_5d)
        2. numerator = pct_rank(DELAY(hl_ratio, 2)) * pct_rank(pct_rank(volume))
        3. denominator = hl_ratio / (vwap - close)
        4. raw = numerator / denominator
        5. score = pct_rank(raw)
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index

        # --- 5日均价 ---
        close_cols = sorted(
            [c for c in df.columns if c.startswith("close_d")],
            key=lambda x: int(x.split("_d")[1]),
        )
        if len(close_cols) < _CLOSE_AVG_WINDOW:
            return pd.Series(50.0, index=idx, name=self.name)

        closes = df[[c for c in close_cols if int(c.split("_d")[1]) < _CLOSE_AVG_WINDOW]].astype(float)
        avg_close_5d = closes.mean(axis=1)

        # --- hl_ratio per day ---
        hl_ratios = {}
        for day_idx in range(_TOTAL_DAYS):
            h_col = f"high_d{day_idx}"
            l_col = f"low_d{day_idx}"
            if h_col in df.columns and l_col in df.columns:
                hl_ratios[day_idx] = (df[h_col] - df[l_col]) / avg_close_5d.replace(0, np.nan)

        if not hl_ratios:
            return pd.Series(50.0, index=idx, name=self.name)

        # --- DELAY(hl_ratio, 2): 取滞后 2 日的 hl_ratio ---
        if _DELAY in hl_ratios:
            delayed_hl = hl_ratios[_DELAY]
        elif max(hl_ratios.keys()) >= _DELAY:
            delayed_hl = hl_ratios[max(k for k in hl_ratios.keys())]
        else:
            delayed_hl = hl_ratios[min(hl_ratios.keys())]

        # --- RANK(DELAY(hl_ratio, 2)) ---
        rank_delayed = self._pct_rank(delayed_hl, idx)

        # --- RANK(RANK(VOLUME)): 双次排名 ---
        vol_d0 = df.get("volume_d0", pd.Series(np.nan, index=idx))
        rank_vol_once = self._pct_rank(vol_d0, idx)
        rank_vol_double = self._pct_rank(rank_vol_once, idx)

        # --- hl_ratio today ---
        hl_today = hl_ratios.get(0, pd.Series(np.nan, index=idx))

        # --- VWAP - CLOSE ---
        amt_d0 = df.get("amount_d0", pd.Series(np.nan, index=idx))
        vwap = amt_d0 / vol_d0.replace(0, np.nan)
        close_d0 = df.get("close_d0", pd.Series(np.nan, index=idx))
        vwap_dev = (vwap - close_d0).abs()

        # --- denominator: hl_ratio / (VWAP - CLOSE) ---
        denominator = hl_today / vwap_dev.replace(0, np.nan)

        # --- numerator / denominator ---
        numerator = rank_delayed * rank_vol_double
        raw = numerator / denominator.abs().replace(0, np.nan)

        scores = self._pct_rank(raw, idx)
        scores.name = self.name
        return scores

    @staticmethod
    def _pct_rank(series: pd.Series, index: pd.Index) -> pd.Series:
        """百分位排名 (0-100)，缺失值补 50。"""
        valid = series.dropna()
        if len(valid) < 2:
            return pd.Series(50.0, index=index)
        ranks = valid.rank(pct=True) * 100
        return ranks.reindex(index).fillna(50.0)
