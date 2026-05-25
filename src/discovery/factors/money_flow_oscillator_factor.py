# -*- coding: utf-8 -*-
"""资金流振荡因子 (Money Flow Oscillator Factor).

盘后因子：SMA(V * ((C-L)-(H-C)) / (H-L), 11, 2) - SMA(V * ((C-L)-(H-C)) / (H-L), 4, 2)
数据来源: stock_daily (high, low, close, volume × 15 交易日)。

评分逻辑：
- CLV (Close Location Value) = ((C-L) - (H-C)) / (H-L)
  → +1 收盘在最高价，-1 收盘在最低价
- MFI_raw = V * CLV — 成交量加权的收盘位置
- oscillator = SMA(MFI_raw, 11) - SMA(MFI_raw, 4)
  → 正值：短期资金流入强于中期（加速流入）→ 高分
  → 负值：短期资金流入弱于中期（资金衰减）→ 低分
"""

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_LOOKBACK_DAYS = 15
_MIN_DAYS = 11


class MoneyFlowOscillatorFactor(BaseFactor):
    """资金流振荡因子。

    SMA(V*CLV, 11, 2) - SMA(V*CLV, 4, 2)
    捕捉短期 vs 中期资金流入强度的差异。
    """

    name = "money_flow_osc"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取近 15 个交易日每只股票的高、低、收、量。

        Returns:
            DataFrame index=ts_code, columns=high_d0..high_d14, low_d0.., close_d0.., vol_d0..
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        target_dt = datetime.strptime(trade_date, "%Y%m%d").date()

        with db.get_session() as s:
            # 多取几天以跳过不完整的最新交易日
            all_dates = [
                row[0] for row in s.execute(
                    text(
                        "SELECT DISTINCT date FROM stock_daily "
                        "WHERE date <= :target ORDER BY date DESC LIMIT :limit"
                    ),
                    {"target": target_dt, "limit": _LOOKBACK_DAYS + 5},
                ).fetchall()
            ]

            # 跳过股票数太少的不完整日期（如盘中未收盘）
            trading_dates = []
            for d in all_dates:
                cnt = s.execute(
                    text("SELECT COUNT(*) FROM stock_daily WHERE date = :d"),
                    {"d": d},
                ).scalar()
                if cnt >= 500:
                    trading_dates.append(d)
                if len(trading_dates) >= _LOOKBACK_DAYS:
                    break

            if len(trading_dates) < _MIN_DAYS:
                logger.warning("[MF-Osc] 交易日数据不足 (target=%s, got=%d)",
                               target_dt, len(trading_dates))
                return None

            placeholders = ",".join(f":d{i}" for i in range(len(trading_dates)))
            params = {f"d{i}": d for i, d in enumerate(trading_dates)}
            rows = s.execute(
                text(
                    f"SELECT code, date, high, low, close, volume FROM stock_daily "
                    f"WHERE date IN ({placeholders}) ORDER BY code, date DESC"
                ),
                params,
            ).fetchall()

        if not rows:
            logger.warning("[MF-Osc] stock_daily 无数据 (%s)", trade_date)
            return None

        df = pd.DataFrame(rows, columns=["code", "date", "high", "low", "close", "volume"])
        df = df.drop_duplicates(subset=["code", "date"], keep="last")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(6)

        from src.discovery.factors.base import apply_hfq_to_prices
        apply_hfq_to_prices(db, df)

        dates_sorted = sorted(df["date"].unique(), reverse=True)

        for col in ["high", "low", "close", "volume"]:
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
            if day_idx < _LOOKBACK_DAYS:
                rec = records.setdefault(code, {})
                for col in ["high", "low", "close", "volume"]:
                    val = row[col]
                    rec[f"{col}_d{day_idx}"] = float(val) if pd.notna(val) else np.nan

        if not records:
            return None

        result = pd.DataFrame.from_dict(records, orient="index")
        result.index.name = "ts_code"

        required_cols = ["high_d0", "low_d0", "close_d0", "volume_d0"]
        result = result.dropna(subset=required_cols)

        codes = result.index.astype(str).str.zfill(6)
        suffix = codes.str[:2].map({
            "60": ".SH", "68": ".SH",
            "00": ".SZ", "30": ".SZ",
            "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
        }).fillna("")
        result.index = codes + suffix

        logger.info("[MF-Osc] 数据组装完成: %d 个交易日, %d 只股票",
                    len(dates_sorted), len(result))
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """计算资金流振荡因子评分。

        1. CLV = ((C-L) - (H-C)) / (H-L)
        2. mfi_raw = V * CLV
        3. osc = SMA(mfi_raw, 11, 2) - SMA(mfi_raw, 4, 2)
        4. score = pct_rank(osc)
        """
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index

        # 逐日计算 mfi_raw = V * CLV
        mfi_series = []
        for day_idx in range(_LOOKBACK_DAYS):
            h = df.get(f"high_d{day_idx}")
            l = df.get(f"low_d{day_idx}")
            c = df.get(f"close_d{day_idx}")
            v = df.get(f"volume_d{day_idx}")
            if h is None or l is None or c is None or v is None:
                break
            h = h.astype(float)
            l = l.astype(float)
            c = c.astype(float)
            v = v.astype(float)
            hl_range = (h - l).replace(0, np.nan)
            clv = ((c - l) - (h - c)) / hl_range
            mfi_series.append(v * clv)

        if len(mfi_series) < _MIN_DAYS:
            return pd.Series(50.0, index=idx, name=self.name)

        # 转成 DataFrame: 每列一天，d0 是最近日
        # SMA 需要时间序列从旧到新，所以 reverse
        mfi_df = pd.DataFrame(
            {f"d{i}": s for i, s in enumerate(mfi_series)},
            index=idx,
        )
        # d0=最近, d14=最老 → 翻转为时间正序
        mfi_df = mfi_df[[f"d{i}" for i in range(len(mfi_series) - 1, -1, -1)]]
        mfi_df.columns = range(len(mfi_df.columns))

        # SMA(span, weight) = ewm(span=span, adjust=False)
        sma11 = mfi_df.ewm(span=11, adjust=False).mean()
        sma4 = mfi_df.ewm(span=4, adjust=False).mean()

        # 取最后一列（最近日）的差值
        osc = sma11.iloc[:, -1] - sma4.iloc[:, -1]

        scores = self._pct_rank(osc, idx)
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
