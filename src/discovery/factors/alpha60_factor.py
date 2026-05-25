# -*- coding: utf-8 -*-
"""Alpha101-060 因子 (Close Location Value Factor).

盘后因子：(HIGH - CLOSE) / (HIGH - LOW)
数据来源: stock_daily (high, low, close × 1 交易日)。

评分逻辑：
- raw = (high - close) / (high - low) — 收盘在日内的相对位置
  → +1: 收盘在最低价（日内最弱）
  →  0: 收盘在最高价（日内最强）
- 均值回归视角：收盘在最低价 → 高分（反弹概率大）
- 横截面百分位排名映射到 0-100
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_EPS = 1e-8


class Alpha60Factor(BaseFactor):
    """Alpha101-060 因子。

    (HIGH - CLOSE) / (HIGH - LOW)
    捕捉收盘价在日内高低区间中的相对位置，用于均值回归信号。
    """

    name = "alpha60"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取当日每只股票的 HLC。

        Returns:
            DataFrame index=ts_code, columns=[high, low, close]
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        target_dt = datetime.strptime(trade_date, "%Y%m%d").date()

        with db.get_session() as s:
            # 跳过股票数太少的不完整日期（如盘中未收盘）
            actual_dt = target_dt
            for _ in range(5):
                cnt = s.execute(
                    text("SELECT COUNT(*) FROM stock_daily WHERE date = :d"),
                    {"d": actual_dt},
                ).scalar()
                if cnt >= 500:
                    break
                prev = s.execute(
                    text("SELECT MAX(date) FROM stock_daily WHERE date < :d"),
                    {"d": actual_dt},
                ).scalar()
                if prev is None:
                    break
                actual_dt = prev

            rows = s.execute(
                text(
                    "SELECT code, high, low, close FROM stock_daily "
                    "WHERE date = :target"
                ),
                {"target": actual_dt},
            ).fetchall()

        if not rows:
            logger.warning("[Alpha60] stock_daily 无数据 (%s)", trade_date)
            return None

        df = pd.DataFrame(rows, columns=["code", "high", "low", "close"])
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)

        from src.discovery.factors.base import apply_hfq_to_prices
        apply_hfq_to_prices(db, df)

        for col in ["high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.drop_duplicates(subset=["code"], keep="last")
        df = df.set_index("code")
        df.index.name = "ts_code"

        df = df.dropna(subset=["high", "low", "close"])

        codes = df.index.astype(str).str.zfill(6)
        suffix = codes.str[:2].map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        df.index = codes + suffix

        logger.info("[Alpha60] 数据组装完成: %d 只股票", len(df))
        return df

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算 Alpha60 因子评分。

        raw = (high - close) / (high - low)
        score = pct_rank(raw)
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index
        h = df["high"].astype(float)
        l = df["low"].astype(float)
        c = df["close"].astype(float)

        hl_range = (h - l).replace(0, np.nan)
        raw = (h - c) / hl_range

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
