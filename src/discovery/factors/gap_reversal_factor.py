# -*- coding: utf-8 -*-
"""Alpha101 跳空反转因子 (Gap Reversal Factor).

盘后因子：-1 * RANK(STD(|close-open|,10) + (close-open) + CORR(close,open,10))
数据来源: stock_daily (open, close × 10 交易日)。

评分逻辑：
- 三项复合信号做横截面排名后取反：
  STD(|close-open|,10)：跳空振幅波动率（高 = 走势紊乱）
  (close-open)：当日阴阳方向（阳线正值、阴线负值）
  CORR(close,open,10)：收盘与开盘的同步趋势（高 = 量价同向）
- 低原始值 → 高分（跳空稳定 + 阴线 + 开盘收盘背离 → 超卖反转买点）
- 高原始值 → 低分（跳空紊乱 + 阳线 + 量价同向 → 过热）
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor, apply_hfq_to_prices

logger = logging.getLogger(__name__)

_LOOKBACK_TRADING_DAYS = 10
_MIN_TRADING_DAYS = 5


class GapReversalFactor(BaseFactor):
    """跳空反转因子。

    -1 * RANK(STD(|close-open|, 10) + (close-open) + CORR(close, open, 10))
    捕捉跳空振幅波动、当日阴阳方向、量价同步性三重信号的反转机会。
    """

    name = "gap_reversal"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取近 10 个交易日每只股票的 open 和 close。

        Returns:
            DataFrame index=ts_code, columns=[open_d0, close_d0, ..., open_d9, close_d9]
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
                    {"target": target_dt, "limit": _LOOKBACK_TRADING_DAYS},
                ).fetchall()
            ]

            if len(trading_dates) < _MIN_TRADING_DAYS:
                logger.warning("[GapReversal] 交易日数据不足 (target=%s, got=%d)",
                               target_dt, len(trading_dates))
                return None

            placeholders = ",".join(f":d{i}" for i in range(len(trading_dates)))
            params = {f"d{i}": d for i, d in enumerate(trading_dates)}
            rows = s.execute(
                text(
                    f"SELECT code, date, open, close FROM stock_daily "
                    f"WHERE date IN ({placeholders}) ORDER BY code, date DESC"
                ),
                params,
            ).fetchall()

        if not rows:
            logger.warning("[GapReversal] stock_daily 无数据 (%s)", trade_date)
            return None

        df = pd.DataFrame(rows, columns=["code", "date", "open", "close"])
        df = df.drop_duplicates(subset=["code", "date"], keep="last")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)
        apply_hfq_to_prices(db, df)

        dates_sorted = sorted(df["date"].unique(), reverse=True)

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
            records.setdefault(code, {})[f"open_d{day_idx}"] = float(row["open"])
            records[code][f"close_d{day_idx}"] = float(row["close"])

        if not records:
            return None

        result = pd.DataFrame.from_dict(records, orient="index")
        result.index.name = "ts_code"

        close_cols = [c for c in result.columns if c.startswith("close_d")]
        open_cols = [c for c in result.columns if c.startswith("open_d")]
        result = result.dropna(thresh=_MIN_TRADING_DAYS, subset=open_cols)
        result = result.dropna(thresh=_MIN_TRADING_DAYS, subset=close_cols)

        codes = result.index.astype(str).str.zfill(6)
        pre2 = codes.str[:2]
        suffix = pre2.map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        result.index = codes + suffix

        logger.info(
            "[GapReversal] 数据组装完成: %d 个交易日, %d 只股票",
            len(dates_sorted), len(result),
        )
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算跳空反转因子评分。

        1. 逐日 |close - open|
        2. STD(|close-open|) 横向（每只股票 10 日标准差）
        3. 当日 close - open
        4. CORR(close, open) 横向（每只股票 10 日相关系数）
        5. raw = STD + (close-open) + CORR
        6. score = 100 - pct_rank(raw) → -1 * RANK
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index

        open_cols = sorted(
            [c for c in df.columns if c.startswith("open_d")],
            key=lambda x: int(x.split("_d")[1]),
        )
        close_cols = sorted(
            [c for c in df.columns if c.startswith("close_d")],
            key=lambda x: int(x.split("_d")[1]),
        )

        if not open_cols or not close_cols:
            return pd.Series(0.0, index=idx, name=self.name)

        opens = df[open_cols].astype(float)
        closes = df[close_cols].astype(float)

        # 1-2. STD(|close - open|) per stock across 10 days
        abs_diff = (closes - opens).abs()
        std_abs_diff = abs_diff.std(axis=1).fillna(0)

        # 3. Current day (close_d0 - open_d0)
        co_diff = closes.iloc[:, 0] - opens.iloc[:, 0]
        co_diff = co_diff.fillna(0)

        # 4. CORR(close, open) per stock across 10 days (vectorized)
        # demean
        o_mean = opens.mean(axis=1)
        c_mean = closes.mean(axis=1)
        o_demean = opens.sub(o_mean, axis=0)
        c_demean = closes.sub(c_mean, axis=0)
        # covariance and std
        cov = (o_demean * c_demean).mean(axis=1)
        o_std = opens.std(axis=1)
        c_std = closes.std(axis=1)
        denom = o_std * c_std
        corr = (cov / denom.replace(0, np.nan)).fillna(0).clip(-1, 1)

        # 5. Composite raw signal
        raw = std_abs_diff + co_diff + corr

        # 6. -1 * RANK: invert cross-sectional percentile
        rank = self._pct_rank(raw, idx)
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
