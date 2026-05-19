# -*- coding: utf-8 -*-
"""Alpha101 VWAP 动量反转因子 (VWAP Momentum Reversal Factor).

盘后因子：RANK(MAX(DELTA(VWAP,3), 5)) * -1
数据来源: stock_daily (amount, volume × 8 交易日)。

评分逻辑：
- DELTA(VWAP, 3)：3 日 VWAP 变化量（VWAP_t - VWAP_{t-3}）
- MAX(DELTA, 5)：过去 5 日 DELTA 的滚动最大值（捕捉最强 VWAP 动量）
- RANK * -1：排名取反 → VWAP 持续下跌（动量最弱）→ 高分（反转买点）
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_MAX_WINDOW = 5
_DELTA_OFFSET = 3
_TOTAL_DAYS = _MAX_WINDOW + _DELTA_OFFSET
_MIN_DAYS = 5


class VwapReversalFactor(BaseFactor):
    """VWAP 动量反转因子。

    RANK(MAX(DELTA(VWAP,3), 5)) * -1
    捕捉 VWAP 持续下跌后的均值回归机会。
    """

    name = "vwap_reversal"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取近 8 个交易日每只股票的 amount、volume，计算每日 VWAP。

        Returns:
            DataFrame index=ts_code, columns=[vwap_d0, ..., vwap_d7]
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
                logger.warning("[VwapReversal] 交易日数据不足 (target=%s, got=%d)",
                               target_dt, len(trading_dates))
                return None

            placeholders = ",".join(f":d{i}" for i in range(len(trading_dates)))
            params = {f"d{i}": d for i, d in enumerate(trading_dates)}
            rows = s.execute(
                text(
                    f"SELECT code, date, amount, volume FROM stock_daily "
                    f"WHERE date IN ({placeholders}) ORDER BY code, date DESC"
                ),
                params,
            ).fetchall()

        if not rows:
            logger.warning("[VwapReversal] stock_daily 无数据 (%s)", trade_date)
            return None

        df = pd.DataFrame(rows, columns=["code", "date", "amount", "volume"])
        df = df.drop_duplicates(subset=["code", "date"], keep="last")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)

        dates_sorted = sorted(df["date"].unique(), reverse=True)

        amount_s = pd.to_numeric(df["amount"], errors="coerce")
        volume_s = pd.to_numeric(df["volume"], errors="coerce")
        df["vwap"] = amount_s / volume_s.replace(0, np.nan)

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
                records.setdefault(code, {})[f"vwap_d{day_idx}"] = (
                    float(row["vwap"]) if pd.notna(row["vwap"]) else np.nan
                )

        if not records:
            return None

        result = pd.DataFrame.from_dict(records, orient="index")
        result.index.name = "ts_code"

        vwap_cols = [c for c in result.columns if c.startswith("vwap_d")]
        result = result.dropna(thresh=_MIN_DAYS, subset=vwap_cols)

        codes = result.index.astype(str).str.zfill(6)
        pre2 = codes.str[:2]
        suffix = pre2.map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        result.index = codes + suffix

        logger.info(
            "[VwapReversal] 数据组装完成: %d 个交易日, %d 只股票",
            len(dates_sorted), len(result),
        )
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算 VWAP 动量反转因子评分。

        1. DELTA(VWAP, 3) = vwap_d{i} - vwap_d{i+3}, for i=0..4
        2. MAX across 5 DELTA values
        3. score = 100 - pct_rank(max_delta)
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index

        vwap_cols = sorted(
            [c for c in df.columns if c.startswith("vwap_d")],
            key=lambda x: int(x.split("_d")[1]),
        )

        if len(vwap_cols) < _MIN_DAYS:
            return pd.Series(50.0, index=idx, name=self.name)

        vwaps = df[vwap_cols].astype(float)

        deltas = []
        for i in range(min(_MAX_WINDOW, len(vwap_cols) - _DELTA_OFFSET)):
            col_t = f"vwap_d{i}"
            col_t3 = f"vwap_d{i + _DELTA_OFFSET}"
            if col_t in vwaps.columns and col_t3 in vwaps.columns:
                deltas.append(vwaps[col_t] - vwaps[col_t3])

        if not deltas:
            return pd.Series(50.0, index=idx, name=self.name)

        delta_df = pd.concat(deltas, axis=1)
        max_delta = delta_df.max(axis=1)

        rank = self._pct_rank(max_delta, idx)
        scores = 100 - rank
        scores = scores.fillna(50.0)
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
