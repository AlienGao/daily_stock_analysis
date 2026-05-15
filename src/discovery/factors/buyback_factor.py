# -*- coding: utf-8 -*-
"""回购因子 (Buyback Factor).

盘后因子：基于 Tushare repurchase API 数据，识别公司回购自家股票的股票。
数据来源: Tushare repurchase (doc_id 124) → DB 缓存。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _pct_rank(series: pd.Series) -> pd.Series:
    """返回 0-1 的百分位排名，处理全 NaN 边界。"""
    ranked = series.rank(pct=True)
    return ranked.fillna(0.0)


class BuybackFactor(BaseFactor):
    """回购因子。

    基于上市公司回购公告数据，百分位归一化打分：
    - 回购金额百分位 (0-25)
    - 回购数量百分位 (0-15)
    - 进度分 (0-30)：实施>股东大会通过>预案>完成>提议
    - 价格区间 (0-30)：当前价 vs 回购上限的上行空间

    fetch_data 优先读 DB 缓存，无缓存时降级为 Tushare 实时请求。
    """

    name = "buyback"
    available_intraday = False
    available_postmarket = True
    weight = 5.0

    _LABEL_THRESHOLD = 0.6
    _PROC_SCORE = {"实施": 30, "股东大会通过": 20, "预案": 15, "完成": 10, "提议": 5}

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 DB 近期回购数据，降级为 Tushare 实时请求。"""
        self._trade_date = trade_date
        # 计算 180 天前的日期作为过滤起点
        from datetime import datetime as _dt, timedelta
        try:
            cutoff = (_dt.strptime(trade_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
        except (ValueError, TypeError):
            cutoff = (_dt.now() - timedelta(days=180)).strftime("%Y%m%d")

        # 1. 尝试 DB
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df_db = db.get_repurchase_recent(ann_date_from=cutoff)
            if not df_db.empty:
                return df_db
        except Exception:
            pass

        # 2. 降级：Tushare 实时请求，同样限定 180 天内公告
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_repurchase(start_date=cutoff)

    # ------------------------------------------------------------------
    # 股价获取
    # ------------------------------------------------------------------

    def _get_current_prices(self, index: pd.Index, trade_date: str) -> pd.Series:
        """批量获取最新收盘价。"""
        from datetime import datetime
        from src.storage import DatabaseManager, StockDaily
        from sqlalchemy import select

        bare_codes = list(set(str(c)[:6] for c in index))
        try:
            db = DatabaseManager()
            td = datetime.strptime(trade_date[:8], "%Y%m%d").date()

            with db.get_session() as session:
                rows = session.execute(
                    select(StockDaily.code, StockDaily.close)
                    .where(StockDaily.code.in_(bare_codes), StockDaily.date <= td)
                    .order_by(StockDaily.code, StockDaily.date.desc())
                ).all()

            price_map = {}
            for code, close in rows:
                if code not in price_map:
                    price_map[code] = close

            return pd.Series(
                [price_map.get(str(c)[:6], float("nan")) for c in index],
                index=index,
            )
        except Exception as e:
            logger.warning("[BuybackFactor] 获取股价失败: %s", e)
            return pd.Series(index=index, dtype=float)

    # ------------------------------------------------------------------
    # 信号提取
    # ------------------------------------------------------------------

    def _compute_signals(
        self, df: pd.DataFrame, current_prices: pd.Series = None,
    ) -> Dict[str, pd.Series]:
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        amount = pd.to_numeric(
            df.get("amount", zeros), errors="coerce"
        ).fillna(0)
        vol = pd.to_numeric(
            df.get("vol", zeros), errors="coerce"
        ).fillna(0)
        high_limit = pd.to_numeric(
            df.get("high_limit", zeros), errors="coerce"
        ).fillna(0)
        proc = df.get("proc", pd.Series("", index=idx)).fillna("").astype(str)

        signals: Dict[str, pd.Series] = {}

        # 1. 回购金额百分位 (0-25)
        valid_a = amount > 0
        s_amount = zeros.copy()
        if valid_a.any():
            s_amount[valid_a] = _pct_rank(amount[valid_a]) * 25.0
        signals["amount"] = s_amount

        # 2. 回购数量百分位 (0-15)
        valid_v = vol > 0
        s_vol = zeros.copy()
        if valid_v.any():
            s_vol[valid_v] = _pct_rank(vol[valid_v]) * 15.0
        signals["vol"] = s_vol

        # 3. 进度分 (0-30)：按阶段递进，越靠后越确定
        s_proc = zeros.copy()
        for stage, score in self._PROC_SCORE.items():
            s_proc[proc.str.contains(stage, na=False)] = score
        signals["proc"] = s_proc

        # 4. 价格区间信号 (0-30)：当前价低于回购上限的上行空间
        s_price = zeros.copy()
        if current_prices is not None and current_prices.notna().any():
            active = proc.str.contains("实施|股东大会通过|预案", na=False)
            valid_p = (
                active & (high_limit > 0) & current_prices.notna() & (current_prices > 0)
            )
            if valid_p.any():
                upside = (high_limit - current_prices) / current_prices
                pos = valid_p & (upside > 0)
                if pos.any():
                    s_price[pos] = _pct_rank(upside[pos]) * 30.0
        signals["price_range"] = s_price

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        trade_date = getattr(self, "_trade_date", "")
        prices = self._get_current_prices(df.index, trade_date) if trade_date else None
        signals = self._compute_signals(df, current_prices=prices)
        total = sum(signals.values()).clip(0, 100)
        total.name = self.name
        # 同股票可能有多条回购记录（不同阶段），取最高分
        total = total.groupby(total.index).max()
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series,
                 **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        trade_date = getattr(self, "_trade_date", "")
        prices = self._get_current_prices(df.index, trade_date) if trade_date else None
        signals = self._compute_signals(df, current_prices=prices)
        thresholds = {
            "amount": 25.0 * self._LABEL_THRESHOLD,
            "vol": 15.0 * self._LABEL_THRESHOLD,
            "proc": 30.0 * self._LABEL_THRESHOLD,
            "price_range": 30.0 * self._LABEL_THRESHOLD,
        }

        amount = pd.to_numeric(
            df.get("amount", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)
        vol = pd.to_numeric(
            df.get("vol", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)
        proc = df.get("proc", pd.Series("", index=df.index)).fillna("").astype(str)
        high_limit = pd.to_numeric(
            df.get("high_limit", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)

        for i in range(len(scores)):
            if scores.iat[i] <= 0:
                continue
            ts_code = str(scores.index[i])
            labels = []

            if signals["proc"].iat[i] >= thresholds["proc"]:
                p = str(proc.iat[i])
                if p and p != "nan":
                    labels.append(f"回购{p}")

            if signals["amount"].iat[i] >= thresholds["amount"]:
                amt = amount.iat[i]
                if amt > 0:
                    if amt >= 1e4:
                        labels.append(f"回购{amt/1e4:.1f}亿元")
                    else:
                        labels.append(f"回购{amt:.0f}万元")

            if signals["vol"].iat[i] >= thresholds["vol"]:
                v = vol.iat[i]
                if v > 0:
                    labels.append(f"回购{v:.0f}万股")

            if signals["price_range"].iat[i] >= thresholds["price_range"]:
                hl = high_limit.iat[i]
                px = prices.iat[i] if prices is not None else float("nan")
                if hl > 0 and not pd.isna(px) and px > 0:
                    pct = (hl - px) / px * 100
                    labels.append(f"回购上限+{pct:.0f}%")

            if labels:
                reasons[ts_code] = labels

        return reasons
