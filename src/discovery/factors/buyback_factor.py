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
    - 回购金额百分位 (0-40)
    - 回购数量百分位 (0-30)
    - 进度加分 (0-30)：实施中/完成

    fetch_data 优先读 DB 缓存，无缓存时降级为 Tushare 实时请求。
    """

    name = "buyback"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    _LABEL_THRESHOLD = 0.6

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 DB 近期回购数据，降级为 Tushare 实时请求。"""
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
    # 信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 3 个子信号，各自用百分位归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        amount = pd.to_numeric(
            df.get("amount", zeros), errors="coerce"
        ).fillna(0)
        vol = pd.to_numeric(
            df.get("vol", zeros), errors="coerce"
        ).fillna(0)
        proc = df.get("proc", pd.Series("", index=idx)).fillna("").astype(str)

        signals: Dict[str, pd.Series] = {}

        # 1. 回购金额百分位 (0-40)
        valid_a = amount > 0
        s_amount = zeros.copy()
        if valid_a.any():
            s_amount[valid_a] = _pct_rank(amount[valid_a]) * 40.0
        signals["amount"] = s_amount

        # 2. 回购数量百分位 (0-30)
        valid_v = vol > 0
        s_vol = zeros.copy()
        if valid_v.any():
            s_vol[valid_v] = _pct_rank(vol[valid_v]) * 30.0
        signals["vol"] = s_vol

        # 3. 进度固定加分 (0-30)
        s_proc = zeros.copy()
        s_proc[proc.str.contains("实施", na=False)] = 30.0
        # "完成" 仅在无"实施"时才加分，避免覆盖"实施完成"的 30 分
        s_proc[(proc.str.contains("完成", na=False))
               & ~(proc.str.contains("实施", na=False))] = 15.0
        signals["proc"] = s_proc

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        total = sum(signals.values()).clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series,
                 **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        thresholds = {
            "amount": 40.0 * self._LABEL_THRESHOLD,
            "vol": 30.0 * self._LABEL_THRESHOLD,
            "proc": 30.0 * self._LABEL_THRESHOLD,
        }

        amount = pd.to_numeric(
            df.get("amount", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)
        vol = pd.to_numeric(
            df.get("vol", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)
        proc = df.get("proc", pd.Series("", index=df.index)).fillna("").astype(str)

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
                    # amount 已归一化为万元
                    if amt >= 1e4:
                        labels.append(f"回购{amt/1e4:.1f}亿元")
                    else:
                        labels.append(f"回购{amt:.0f}万元")

            if signals["vol"].iat[i] >= thresholds["vol"]:
                v = vol.iat[i]
                if v > 0:
                    # vol 已归一化为万股
                    labels.append(f"回购{v:.0f}万股")

            if labels:
                reasons[ts_code] = labels

        return reasons
