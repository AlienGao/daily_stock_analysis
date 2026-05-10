# -*- coding: utf-8 -*-
"""涨跌停因子 (Limit Factor).

盘后因子：基于涨跌停数据识别强势股。
3 个子信号：
- 封板质量 (0-35)：开板次数梯度，一字板满分
- 连板强度 (0-35)：连续涨停天数，递增递减
- 涨幅强度 (0-30)：pct_chg 百分位排名

数据来源: Tushare limit_list_d + 本地 limit_pool
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class LimitFactor(BaseFactor):
    """涨跌停因子（盘后版）。

    涨停质量 + 连板强度 + 涨幅强度，跌停和炸板自动低分/归零。
    """

    name = "limit"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 limit_pool DB，降级到 Tushare API。"""
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df = db.get_limit_pool(trade_date=trade_date)
            if df is not None and not df.empty:
                df = df.reset_index().copy()
                df.index = [self._bare_to_ts_code(c) for c in df["code"]]
                # DB 列名与 Tushare 不同：limit_type 为空，U/D/Z 在 limit_stats
                if "limit" not in df.columns:
                    if "limit_stats" in df.columns:
                        df["limit"] = df["limit_stats"]
                    elif "limit_type" in df.columns:
                        df["limit"] = df["limit_type"]
                return df
        except Exception as e:
            logger.debug("[LimitFactor] limit_pool 查询失败，回退 Tushare: %s", e)

        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_limit_list(trade_date)

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 3 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        limit_type = df.get("limit", pd.Series("", index=idx))
        open_times = df.get("open_times", pd.Series(0, index=idx))
        limit_times = df.get("limit_times", pd.Series(0, index=idx))
        pct_chg = df.get("pct_chg", zeros)

        is_up = limit_type == "U"
        is_down = limit_type == "D"
        is_break = limit_type == "Z"

        signals: Dict[str, pd.Series] = {}

        # --- 1. 封板质量 (0-35)：open_times 梯度 ---
        s_seal = zeros.copy()
        up_idx = is_up[is_up].index
        ot_up = open_times.reindex(up_idx).fillna(0).astype(int)
        s_seal.loc[up_idx] = (
            ot_up.map(lambda n: {0: 35, 1: 28, 2: 20, 3: 12, 4: 6}.get(n, 0))
            .clip(0, 35)
        )
        # 炸板：按开板次数给低分
        br_idx = is_break[is_break].index
        ot_br = open_times.reindex(br_idx).fillna(0).astype(int)
        s_seal.loc[br_idx] = (
            ot_br.map(lambda n: {0: 8, 1: 5, 2: 3}.get(n, 0)).clip(0, 35)
        )
        signals["seal"] = s_seal

        # --- 2. 连板强度 (0-35)：limit_times 递增递减 ---
        def _map_chain(n: int) -> float:
            if n <= 0:
                return 0.0
            if n == 1:
                return 15.0
            if n == 2:
                return 23.0
            if n == 3:
                return 29.0
            if n == 4:
                return 33.0
            return 35.0

        s_chain = zeros.copy()
        s_chain.loc[is_up] = limit_times[is_up].apply(_map_chain).clip(0, 35)
        # 炸板保留部分连板分
        s_chain.loc[is_break] = (
            limit_times[is_break].apply(lambda n: _map_chain(n) * 0.4).clip(0, 35)
        )
        signals["chain"] = s_chain

        # --- 3. 涨幅强度 (0-30)：pct_chg 在涨停股中的百分位 ---
        s_pct = zeros.copy()
        up_pct = pct_chg[is_up]
        if len(up_pct) > 0:
            pct_rank = up_pct.rank(pct=True)
            s_pct.loc[is_up] = (pct_rank * 30).clip(0, 30)
        # 炸板：按实际涨幅给一半权重
        br_pct = pct_chg[is_break]
        if len(br_pct) > 0:
            br_rank = br_pct.rank(pct=True)
            s_pct.loc[is_break] = (br_rank * 15).clip(0, 30)
        signals["pct_chg"] = s_pct

        # 跌停全部归零
        for key in signals:
            signals[key].loc[is_down] = 0.0

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        limit_type = df.get("limit", pd.Series("", index=df.index))
        is_down = limit_type == "D"

        signals = self._compute_signals(df)
        total = sum(signals.values())

        total.loc[is_down] = 0.0
        total = total.clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        limit_type = df.get("limit", pd.Series("", index=df.index))
        open_times = df.get("open_times", pd.Series(0, index=df.index))
        limit_times = df.get("limit_times", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0.0, index=df.index))
        up_stat = df.get("up_stat", pd.Series("", index=df.index))

        signals = self._compute_signals(df)

        signal_meta = [
            ("seal", "封板质量", 35),
            ("chain", "连板强度", 35),
            ("pct_chg", "涨幅强度", 30),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            score_val = scores[ts_code]
            if score_val <= 0:
                continue

            lt_type = limit_type.get(ts_code, "")
            labels: List[str] = []

            for key, label, max_val in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_val * threshold:
                    continue
                if key == "seal":
                    ot = int(open_times.get(ts_code, 0))
                    if ot == 0 and lt_type == "U":
                        labels.append("一字封板")
                    elif ot == 1:
                        labels.append("短暂开板后回封")
                    elif ot <= 3:
                        labels.append(f"开板{ot}次回封")
                    else:
                        labels.append("反复开板")
                elif key == "chain":
                    lt = int(limit_times.get(ts_code, 0))
                    if lt >= 4:
                        labels.append(f"连板龙头({lt}连板)")
                    elif lt >= 2:
                        labels.append(f"{lt}连板")
                    else:
                        labels.append("首板涨停")
                elif key == "pct_chg":
                    pct = pct_chg.get(ts_code, 0)
                    labels.append(f"涨幅{pct:.1f}%")

            if lt_type == "Z":
                us = str(up_stat.get(ts_code, ""))
                if us:
                    labels.insert(0, f"炸板({us})")
                else:
                    labels.insert(0, "炸板")

            if labels:
                reasons[ts_code] = labels

        return reasons

    @staticmethod
    def _bare_to_ts_code(code: str) -> str:
        """裸代码 → ts_code 格式 (e.g. '600519' → '600519.SH')。"""
        c = str(code).strip().zfill(6)
        if c.startswith(("60", "68")):
            return f"{c}.SH"
        elif c.startswith(("00", "30")):
            return f"{c}.SZ"
        elif c.startswith(("4", "8", "92")):
            return f"{c}.BJ"
        return c
