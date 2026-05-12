# -*- coding: utf-8 -*-
"""机构持仓因子 (Institution Hold Factor).

盘后因子：基于新浪财经机构持股数据，识别机构增仓的股票。
数据来源: akshare stock_institute_hold() → DB 缓存（季度更新）。
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


class InstitutionHoldFactor(BaseFactor):
    """机构持仓因子。

    基于机构持股季度数据，百分位归一化打分：
    - 机构数量百分位 (0-25)
    - 机构数变化百分位 (0-30)
    - 持股比例百分位 (0-20)
    - 持股比例增幅百分位 (0-25)

    fetch_data 优先读 DB 缓存，无缓存时降级为 akshare 实时请求。
    """

    name = "institution_hold"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    _LABEL_THRESHOLD = 0.6

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 DB 最新季度数据，降级为 akshare 实时请求。"""
        self._trade_date = trade_date
        # 1. 尝试 DB
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df_db = db.get_latest_institution_hold()
            if not df_db.empty:
                return df_db
        except Exception:
            pass

        # 2. 降级：akshare 实时请求
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_institution_holds()

    # ------------------------------------------------------------------
    # 信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 4 个子信号，各自用百分位归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        inst_count = pd.to_numeric(
            df.get("inst_count", zeros), errors="coerce"
        ).fillna(0)
        inst_count_chg = pd.to_numeric(
            df.get("inst_count_change", zeros), errors="coerce"
        ).fillna(0)
        hold_ratio = pd.to_numeric(
            df.get("hold_ratio", zeros), errors="coerce"
        ).fillna(0)
        hold_ratio_chg = pd.to_numeric(
            df.get("hold_ratio_change", zeros), errors="coerce"
        ).fillna(0)

        signals: Dict[str, pd.Series] = {}

        # 1. 机构数量百分位 (0-20)
        valid = inst_count > 0
        s_count = zeros.copy()
        if valid.any():
            s_count[valid] = _pct_rank(inst_count[valid]) * 20.0
        signals["inst_count"] = s_count

        # 2. 机构数变化百分位 (0-25)：仅正变化参与排名
        s_chg = zeros.copy()
        pos_chg = inst_count_chg > 0
        if pos_chg.any():
            s_chg[pos_chg] = _pct_rank(inst_count_chg[pos_chg]) * 25.0
        signals["inst_count_change"] = s_chg

        # 3. 持股比例百分位 (0-18)
        valid_r = hold_ratio > 0
        s_ratio = zeros.copy()
        if valid_r.any():
            s_ratio[valid_r] = _pct_rank(hold_ratio[valid_r]) * 18.0
        signals["hold_ratio"] = s_ratio

        # 4. 持股比例增幅百分位 (0-22)：仅正增幅参与排名
        s_rchg = zeros.copy()
        pos_rchg = hold_ratio_chg > 0
        if pos_rchg.any():
            s_rchg[pos_rchg] = _pct_rank(hold_ratio_chg[pos_rchg]) * 22.0
        signals["hold_ratio_change"] = s_rchg

        # 5. 集中度 (0-15)：持股比例/机构数，high = 少数机构重仓
        s_conc = zeros.copy()
        conc_valid = (inst_count > 0) & (hold_ratio > 0)
        if conc_valid.any():
            avg_per_inst = hold_ratio[conc_valid] / inst_count[conc_valid]
            s_conc[conc_valid] = _pct_rank(avg_per_inst) * 15.0
        signals["concentration"] = s_conc

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        total = sum(signals.values()).clip(0, 100)

        # 季度时效性衰减：上季度×0.7，前季度×0.4，更早归零
        total = self._apply_quarter_decay(total, df)

        total.name = self.name
        return total

    def _apply_quarter_decay(
        self, total: pd.Series, df: pd.DataFrame,
    ) -> pd.Series:
        """按报告期距今实际天数衰减，>90 天开始降权。"""
        trade_date = getattr(self, "_trade_date", "")
        if not trade_date or "quarter" not in df.columns:
            return total

        try:
            import re
            td = pd.to_datetime(str(trade_date)[:8], format="%Y%m%d")

            _Q_END = {"1": "-03-31", "2": "-06-30", "3": "-09-30", "4": "-12-31"}

            def _report_end(q_str):
                m = re.match(r"(\d{4})Q(\d)", str(q_str))
                if m:
                    return pd.Timestamp(m.group(1) + _Q_END.get(m.group(2), "-12-31"))
                return td

            report_ends = df["quarter"].apply(_report_end)
            days_lag = (td - report_ends).dt.days.clip(lower=0)

            decay = pd.Series(1.0, index=total.index)
            decay[days_lag > 90] = 0.7
            decay[days_lag > 180] = 0.4
            decay[days_lag > 270] = 0.0

            decayed = (days_lag > 0).sum()
            if decayed:
                logger.debug("[InstitutionHold] 时效衰减: %d 只, max_lag=%d天",
                             int(decayed), days_lag.max())
            return total * decay
        except Exception as e:
            logger.debug("[InstitutionHold] 时效衰减计算失败: %s", e)
            return total

    def describe(self, df: pd.DataFrame, scores: pd.Series,
                 **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        thresholds = {
            "inst_count": 20.0 * self._LABEL_THRESHOLD,
            "inst_count_change": 25.0 * self._LABEL_THRESHOLD,
            "hold_ratio": 18.0 * self._LABEL_THRESHOLD,
            "hold_ratio_change": 22.0 * self._LABEL_THRESHOLD,
            "concentration": 15.0 * self._LABEL_THRESHOLD,
        }

        inst_count = pd.to_numeric(
            df.get("inst_count", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)
        inst_count_chg = pd.to_numeric(
            df.get("inst_count_change", pd.Series(0, index=df.index)),
            errors="coerce",
        ).fillna(0)
        hold_ratio = pd.to_numeric(
            df.get("hold_ratio", pd.Series(0, index=df.index)), errors="coerce"
        ).fillna(0)
        hold_ratio_chg = pd.to_numeric(
            df.get("hold_ratio_change", pd.Series(0, index=df.index)),
            errors="coerce",
        ).fillna(0)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            labels = []

            if signals["inst_count"].get(ts_code, 0) >= thresholds["inst_count"]:
                cnt = int(inst_count.get(ts_code, 0))
                labels.append(f"{cnt}家机构持仓")

            if signals["inst_count_change"].get(ts_code, 0) >= thresholds["inst_count_change"]:
                chg = int(inst_count_chg.get(ts_code, 0))
                labels.append(f"机构数+{chg}")

            if signals["hold_ratio"].get(ts_code, 0) >= thresholds["hold_ratio"]:
                ratio = hold_ratio.get(ts_code, 0)
                labels.append(f"持股{ratio:.1f}%")

            if signals["hold_ratio_change"].get(ts_code, 0) >= thresholds["hold_ratio_change"]:
                rchg = hold_ratio_chg.get(ts_code, 0)
                labels.append(f"持股比例+{rchg:.2f}%")

            if signals["concentration"].get(ts_code, 0) >= thresholds["concentration"]:
                cnt = int(inst_count.get(ts_code, 0))
                ratio = hold_ratio.get(ts_code, 0)
                labels.append(f"持仓集中({cnt}家持{ratio:.1f}%)")

            if labels:
                reasons[ts_code] = labels

        return reasons
