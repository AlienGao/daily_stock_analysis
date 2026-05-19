# -*- coding: utf-8 -*-
"""Alpha101-042 因子 (Quantile Mean-Reversion Factor).

盘后因子：quantile_{0.2}(close, 5) / close — 5 日收盘价 20% 分位数与当日收盘价的比值。
数据来源: stock_daily 历史日线收盘价。

评分逻辑：
- ratio = quantile_0.2(close_d0..close_d4) / close_d0
- ratio → 1.0：当前价接近 5 日低点区间（均值回归买点），高分
- ratio → 0.85-：当前价远离 5 日低点（短期冲高），低分
- 线性映射 (0.80, 1.0) → (0, 100)，超出区间 clipping
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_LOOKBACK_TRADING_DAYS = 5
_RATIO_MIN = 0.80   # ratio <= 0.80 → score 0
_RATIO_MAX = 1.00   # ratio >= 1.00 → score 100


class Alpha042Factor(BaseFactor):
    """Alpha101-042 因子。

    基于 5 日收盘价 20% 分位数与当日收盘价的比值，捕捉短期均值回归机会。
    ratio > 0.95 说明当前价已接近 5 日低点区间，向上回归概率较大。
    """

    name = "alpha042"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取近 5 个交易日每只股票的收盘价。

        Returns:
            DataFrame index=ts_code, columns=[close_d0, ..., close_d4]
            其中 d0 = trade_date（最近交易日），d4 = 最早交易日。
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

            if len(trading_dates) < 2:
                logger.warning("[Alpha042] stock_daily 交易日数据不足 (target=%s)", target_dt)
                return None

            placeholders = ",".join(f":d{i}" for i in range(len(trading_dates)))
            params = {f"d{i}": d for i, d in enumerate(trading_dates)}
            rows = s.execute(
                text(
                    f"SELECT code, date, close FROM stock_daily "
                    f"WHERE date IN ({placeholders}) ORDER BY code, date DESC"
                ),
                params,
            ).fetchall()

        if not rows:
            logger.warning("[Alpha042] stock_daily 无收盘数据 (%s)", trade_date)
            return None

        df = pd.DataFrame(rows, columns=["code", "date", "close"])
        df = df.drop_duplicates(subset=["code", "date"], keep="last")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)

        dates_sorted = sorted(df["date"].unique(), reverse=True)

        # Pivot: one column per trading day
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
            records.setdefault(code, {})[f"close_d{day_idx}"] = float(row["close"])

        if not records:
            return None

        result = pd.DataFrame.from_dict(records, orient="index")
        result.index.name = "ts_code"

        # Filter: require at least 3 of 5 days
        close_cols = [c for c in result.columns if c.startswith("close_d")]
        result = result.dropna(thresh=max(3, len(close_cols)), subset=close_cols)

        # Convert bare codes to ts_code format
        codes = result.index.astype(str).str.zfill(6)
        pre2 = codes.str[:2]
        suffix = pre2.map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        result.index = codes + suffix

        logger.info(
            "[Alpha042] 数据组装完成: %d 个交易日, %d 只股票",
            len(dates_sorted), len(result),
        )
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算 Alpha042 因子评分。

        ratio = quantile_0.2(close_d0..close_d4) / close_d0
        线性映射 (0.80, 1.00) → (0, 100)，clipping 边界。
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        close_cols = sorted(
            [c for c in df.columns if c.startswith("close_d")],
            key=lambda x: int(x.split("_d")[1]),
        )

        if not close_cols:
            return pd.Series(0.0, index=df.index, name=self.name)

        closes = df[close_cols].astype(float)
        quantile_02 = closes.quantile(0.20, axis=1)
        close_d0 = closes[close_cols[0]]

        # Avoid division by zero
        close_d0_safe = close_d0.replace(0, np.nan)
        ratio = quantile_02 / close_d0_safe

        ratio_clean = ratio.fillna(1.0).clip(0.7, 1.05)

        scores = ((ratio_clean - _RATIO_MIN) / (_RATIO_MAX - _RATIO_MIN) * 100).clip(0, 100)
        scores = scores.fillna(50.0)
        scores.name = self.name
        return scores
