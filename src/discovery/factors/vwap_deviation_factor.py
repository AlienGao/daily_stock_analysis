# -*- coding: utf-8 -*-
"""Alpha101-006 VWAP 偏离因子 (VWAP Deviation Factor).

盘后因子：rank(vwap - close) / rank(vwap + close) — VWAP 与收盘价偏离的横截面排名比。
数据来源: stock_daily (amount, volume, close)，VWAP 以 amount/volume 代理。

评分逻辑：
- VWAP = amount / volume（日内成交均价）
- diff_rank = pct_rank(vwap - close)，sum_rank = pct_rank(vwap + close)
- ratio = diff_rank / sum_rank → 再 pct_rank 得最终 0-100 分
- ratio 高 → close 远低于 VWAP（尾盘杀跌超卖），高分（均值回归买点）
- ratio 低 → close 远高于 VWAP（尾盘拉高超买），低分
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor, apply_hfq_to_prices

logger = logging.getLogger(__name__)


class VwapDeviationFactor(BaseFactor):
    """VWAP 偏离因子。

    rank(vwap - close) / rank(vwap + close)，横截面排名比。
    捕捉收盘价相对于日内均价 VWAP 的偏离程度。
    """

    name = "vwap_deviation"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取当日全市场 close、amount、volume。

        Returns:
            DataFrame index=ts_code, columns=[close, amount, volume]
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        target_dt = datetime.strptime(trade_date, "%Y%m%d").date()

        with db.get_session() as s:
            rows = s.execute(
                text(
                    "SELECT code, close, amount, volume FROM stock_daily "
                    "WHERE date = :target"
                ),
                {"target": target_dt},
            ).fetchall()

        if not rows:
            logger.warning("[VwapDeviation] stock_daily 无数据 (date=%s)", target_dt)
            return None

        df = pd.DataFrame(rows, columns=["code", "close", "amount", "volume"])
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)
        df["date"] = target_dt
        apply_hfq_to_prices(db, df)
        df = df.drop(columns=["date"])
        df = df.set_index("code")
        df.index.name = "ts_code"

        df = df[df["volume"].fillna(0) > 0].copy()

        codes = df.index.astype(str).str.zfill(6)
        pre2 = codes.str[:2]
        suffix = pre2.map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        df.index = codes + suffix

        logger.info("[VwapDeviation] 数据获取完成: %d 只股票", len(df))
        return df

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算 VWAP 偏离因子评分。

        1. VWAP = amount / volume
        2. diff_rank = pct_rank(vwap - close)
        3. sum_rank = pct_rank(vwap + close)
        4. ratio = diff_rank / sum_rank
        5. 最终分 = pct_rank(ratio) → 0-100
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index

        close = pd.to_numeric(df["close"], errors="coerce")
        amount = pd.to_numeric(df["amount"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")

        vwap = amount / volume.replace(0, np.nan)

        valid = vwap.notna() & close.notna() & (close > 0)
        if valid.sum() < 10:
            logger.warning("[VwapDeviation] 有效数据不足 (%d 只)", valid.sum())
            return pd.Series(50.0, index=idx, name=self.name)

        diff = vwap - close
        total = vwap + close

        diff_rank = self._pct_rank(diff, idx)
        sum_rank = self._pct_rank(total, idx)

        sum_rank_safe = sum_rank.replace(0, 1.0)
        ratio = diff_rank / sum_rank_safe

        scores = self._pct_rank(ratio, idx)
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
