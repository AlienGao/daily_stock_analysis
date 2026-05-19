# -*- coding: utf-8 -*-
"""Alpha101 流动性超卖反转因子 (Liquid Oversold Reversal Factor).

盘后因子：rank((-1*ret)*mean(v,20)*vwap*(high-close))
数据来源: stock_daily (pct_chg, volume, amount, high, close)。

评分逻辑：
- (-1*ret)：收益取反，下跌股获正值（反转倾向）
- mean(v,20)：20 日均量，高流动性放大信号
- vwap：amount/volume 日内均价，价格中枢权重
- (high-close)：收盘距日内高点幅度，尾盘杀跌越大信号越强
- 四项乘积做横截面 rank → 0-100 分
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_VOL_LOOKBACK = 20
_MIN_TRADING_DAYS = 5


class LiquidOversoldFactor(BaseFactor):
    """流动性超卖反转因子。

    rank((-1*ret)*mean(v,20)*vwap*(high-close))
    高流动性股票在下跌日尾盘大幅回落时的反转信号。
    """

    name = "liquid_oversold"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取当日 OHLCV + 近 20 日成交量。

        Returns:
            DataFrame index=ts_code, columns=[pct_chg, high, close, amount, volume, avg_vol_20]
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
                    {"target": target_dt, "limit": _VOL_LOOKBACK},
                ).fetchall()
            ]

            if len(trading_dates) < _MIN_TRADING_DAYS:
                logger.warning("[LiquidOversold] 交易日数据不足 (target=%s)", target_dt)
                return None

            today_rows = s.execute(
                text(
                    "SELECT code, pct_chg, high, close, amount, volume "
                    "FROM stock_daily WHERE date = :target"
                ),
                {"target": trading_dates[0]},
            ).fetchall()

            if not today_rows:
                logger.warning("[LiquidOversold] 当日无数据 (date=%s)", trading_dates[0])
                return None

            today = pd.DataFrame(
                today_rows,
                columns=["code", "pct_chg", "high", "close", "amount", "volume"],
            )
            today["code"] = today["code"].astype(str).str.strip().str.zfill(6)
            today = today.set_index("code")

            placeholders = ",".join(f":d{i}" for i in range(len(trading_dates)))
            params = {f"d{i}": d for i, d in enumerate(trading_dates)}
            vol_rows = s.execute(
                text(
                    f"SELECT code, volume FROM stock_daily "
                    f"WHERE date IN ({placeholders})"
                ),
                params,
            ).fetchall()

        vol_df = pd.DataFrame(vol_rows, columns=["code", "volume"])
        vol_df["code"] = vol_df["code"].astype(str).str.strip().str.zfill(6)
        avg_vol = vol_df.groupby("code")["volume"].mean()
        avg_vol.name = "avg_vol_20"

        result = today.join(avg_vol, how="inner")
        result.index.name = "ts_code"

        result = result[result["volume"].fillna(0) > 0].copy()

        codes = result.index.astype(str).str.zfill(6)
        pre2 = codes.str[:2]
        suffix = pre2.map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        result.index = codes + suffix

        logger.info("[LiquidOversold] 数据组装完成: %d 只股票", len(result))
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算流动性超卖反转因子评分。

        1. ret = pct_chg
        2. neg_ret = -ret
        3. avg_vol_20 = mean(volume, 20)
        4. vwap = amount / volume
        5. high_close_gap = high - close
        6. raw = neg_ret * avg_vol_20 * vwap * high_close_gap
        7. score = pct_rank(raw) → 0-100
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index

        pct_chg = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0)
        high = pd.to_numeric(df["high"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        amount = pd.to_numeric(df["amount"], errors="coerce")
        volume = pd.to_numeric(df["volume"], errors="coerce")
        avg_vol_20 = pd.to_numeric(df["avg_vol_20"], errors="coerce").fillna(0)

        neg_ret = -pct_chg
        vwap = amount / volume.replace(0, np.nan)
        high_close_gap = (high - close).fillna(0).clip(lower=0)

        raw = neg_ret * avg_vol_20 * vwap.fillna(0) * high_close_gap

        scores = self._pct_rank(raw, idx)
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
