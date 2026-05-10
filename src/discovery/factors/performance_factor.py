# -*- coding: utf-8 -*-
"""业绩因子 (Performance Factor).

盘后因子：基于东财业绩报表数据，识别业绩增长强劲的股票。
数据来源: akshare stock_yjbb_em() → performance_report 表。

评分逻辑：
- 4 个核心子信号（横截面百分位排名）：净利润增长、营收增长、ROE、毛利率
- 2 个趋势子信号（多季度加速/减速）：净利润趋势、营收趋势
- 所有子信号通过 _compute_signals() 统一计算，score() 和 describe() 共享
"""

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)

_LOOKBACK_QUARTERS = 4
_LABEL_THRESHOLD = 5.0


def _pct_rank(series: pd.Series, index) -> pd.Series:
    """全市场百分位排名 (0-100)，缺失值补 50（中位数）。"""
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(50.0, index=index)
    ranks = valid.rank(pct=True) * 100
    return ranks.reindex(index).fillna(50.0)


def _quarter_end_dates(ref_date: str, n: int = 4) -> List[str]:
    """返回 ref_date 之前最近 n 个季度末日期 (YYYYMMDD)。"""
    d = date(int(ref_date[:4]), int(ref_date[4:6]), int(ref_date[6:8]))

    # Find most recent quarter-end <= ref_date
    candidates = [
        date(d.year, 3, 31),
        date(d.year, 6, 30),
        date(d.year, 9, 30),
        date(d.year, 12, 31),
        date(d.year - 1, 12, 31),
    ]
    q_end = max(qe for qe in candidates if qe <= d)

    quarters = [q_end.strftime("%Y%m%d")]
    cursor_year, cursor_month = q_end.year, q_end.month
    for _ in range(n - 1):
        cursor_month -= 3
        if cursor_month <= 0:
            cursor_month += 12
            cursor_year -= 1
        if cursor_month == 3:
            q = date(cursor_year, 3, 31)
        elif cursor_month == 6:
            q = date(cursor_year, 6, 30)
        elif cursor_month == 9:
            q = date(cursor_year, 9, 30)
        else:
            q = date(cursor_year, 12, 31)
        quarters.append(q.strftime("%Y%m%d"))
    return quarters


class PerformanceFactor(BaseFactor):
    """业绩因子。

    基于业绩报表的净利润增长、ROE、毛利率，识别业绩成长股。
    使用横截面百分位排名 + 多季度趋势分析。
    """

    name = "performance"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """从 DB 读取最近 N 个季度业绩数据，pivot 为 wide format。

        若 DB 无数据，fallback 到 akshare API 并持久化。
        """
        from src.storage import DatabaseManager

        periods = _quarter_end_dates(trade_date, _LOOKBACK_QUARTERS)
        if not periods:
            return None

        db = DatabaseManager()

        # Try DB first — fetch all quarters
        dfs = []
        all_codes = set()
        for period in periods:
            df_p = db.get_performance_report(period)
            if not df_p.empty:
                df_p = df_p.copy()
                df_p["report_period"] = period
                dfs.append(df_p)
                all_codes.update(df_p.index)

        # Fallback: akshare API for missing quarters
        if len(dfs) < len(periods):
            akshare_fetcher = kwargs.get("akshare_fetcher")
            if akshare_fetcher is None:
                from data_provider.akshare_fetcher import AkshareFetcher
                akshare_fetcher = AkshareFetcher()

            existing_periods = {df["report_period"].iloc[0] for df in dfs}
            for period in periods:
                if period in existing_periods:
                    continue
                raw = akshare_fetcher.get_performance_report_quarter(period)
                if raw is not None and not raw.empty:
                    db.upsert_performance_report(raw, period, source="akshare")
                    raw = raw.copy()
                    raw["report_period"] = period
                    dfs.append(raw)
                    all_codes.update(raw.index)

        if not dfs:
            return None

        # Merge all quarters into wide format
        merged = None
        for df_p in dfs:
            period = df_p["report_period"].iloc[0]
            cols = ["report_period", "name", "net_profit_yoy", "revenue_yoy",
                    "roe", "gross_margin", "industry"]
            subset = df_p[[c for c in cols if c in df_p.columns]].copy()
            subset = subset.rename(columns={
                "net_profit_yoy": f"net_profit_yoy_{period}",
                "revenue_yoy": f"revenue_yoy_{period}",
                "roe": f"roe_{period}",
                "gross_margin": f"gross_margin_{period}",
            })
            if merged is None:
                merged = subset
            else:
                subset = subset.drop(columns=["name", "industry", "report_period"], errors="ignore")
                merged = merged.join(subset, how="outer")

        if merged is None or merged.empty:
            return None

        # Sort periods and rename to d0 (latest), d1, d2, d3
        sorted_periods = sorted(periods, reverse=True)
        for i, period in enumerate(sorted_periods):
            for metric in ["net_profit_yoy", "revenue_yoy", "roe", "gross_margin"]:
                old_col = f"{metric}_{period}"
                new_col = f"d{i}_{metric}"
                if old_col in merged.columns:
                    merged = merged.rename(columns={old_col: new_col})

        keep_cols = ["name", "industry"] + [
            f"d{i}_{m}" for i in range(len(sorted_periods))
            for m in ["net_profit_yoy", "revenue_yoy", "roe", "gross_margin"]
        ]
        merged = merged[[c for c in keep_cols if c in merged.columns]]

        return merged

    # ------------------------------------------------------------------
    # Signal computation
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """计算所有子信号的横截面百分位排名。

        每个指标独立退化：缺失 d1 列时 df.get() 返回 NaN，
        _pct_rank 将全 NaN 填充为 50.0（中性），趋势信号自动中性化。
        """
        idx = df.index
        signals: Dict[str, pd.Series] = {}

        net_profit_yoy = df.get("d0_net_profit_yoy", pd.Series(index=idx))
        revenue_yoy = df.get("d0_revenue_yoy", pd.Series(index=idx))
        roe = df.get("d0_roe", pd.Series(index=idx))
        gross_margin = df.get("d0_gross_margin", pd.Series(index=idx))

        signals["net_profit_yoy_pct"] = _pct_rank(pd.to_numeric(net_profit_yoy, errors="coerce"), idx)
        signals["revenue_yoy_pct"] = _pct_rank(pd.to_numeric(revenue_yoy, errors="coerce"), idx)
        signals["roe_pct"] = _pct_rank(pd.to_numeric(roe, errors="coerce"), idx)
        signals["gross_margin_pct"] = _pct_rank(pd.to_numeric(gross_margin, errors="coerce"), idx)

        net_d0 = pd.to_numeric(df.get("d0_net_profit_yoy", pd.Series(index=idx)), errors="coerce")
        net_d1 = pd.to_numeric(df.get("d1_net_profit_yoy", pd.Series(index=idx)), errors="coerce")
        rev_d0 = pd.to_numeric(df.get("d0_revenue_yoy", pd.Series(index=idx)), errors="coerce")
        rev_d1 = pd.to_numeric(df.get("d1_revenue_yoy", pd.Series(index=idx)), errors="coerce")

        net_trend = (net_d0 - net_d1).fillna(0)
        rev_trend = (rev_d0 - rev_d1).fillna(0)

        signals["net_profit_trend"] = net_trend
        signals["net_profit_trend_pct"] = _pct_rank(net_trend, idx)
        signals["revenue_trend"] = rev_trend
        signals["revenue_trend_pct"] = _pct_rank(rev_trend, idx)

        return signals

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        signals = self._compute_signals(df)
        idx = df.index

        # Net profit YoY growth: max +37
        npp = signals.get("net_profit_yoy_pct", pd.Series(50.0, index=idx))
        scores.loc[npp >= 80] += 37.0
        scores.loc[(npp >= 60) & (npp < 80)] += 23.0

        # Revenue YoY growth: max +20
        rvp = signals.get("revenue_yoy_pct", pd.Series(50.0, index=idx))
        scores.loc[rvp >= 80] += 20.0
        scores.loc[(rvp >= 60) & (rvp < 80)] += 10.0

        # ROE: max +20
        roe_p = signals.get("roe_pct", pd.Series(50.0, index=idx))
        scores.loc[roe_p >= 80] += 20.0
        scores.loc[(roe_p >= 60) & (roe_p < 80)] += 10.0

        # Gross margin: max +15
        gm_p = signals.get("gross_margin_pct", pd.Series(50.0, index=idx))
        scores.loc[gm_p >= 80] += 15.0
        scores.loc[(gm_p >= 60) & (gm_p < 80)] += 7.0

        # Net profit accelerating: +5 / decelerating: -5
        nt_p = signals.get("net_profit_trend_pct", pd.Series(50.0, index=idx))
        scores.loc[nt_p >= 80] += 5.0
        scores.loc[nt_p < 20] -= 5.0

        # Revenue accelerating: +3
        rt_p = signals.get("revenue_trend_pct", pd.Series(50.0, index=idx))
        scores.loc[rt_p >= 80] += 3.0

        return scores.clip(0, 100)

    # ------------------------------------------------------------------
    # Describe
    # ------------------------------------------------------------------

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        idx = df.index

        npp = signals.get("net_profit_yoy_pct", pd.Series(50.0, index=idx))
        rvp = signals.get("revenue_yoy_pct", pd.Series(50.0, index=idx))
        roe_p = signals.get("roe_pct", pd.Series(50.0, index=idx))
        gm_p = signals.get("gross_margin_pct", pd.Series(50.0, index=idx))
        nt_p = signals.get("net_profit_trend_pct", pd.Series(50.0, index=idx))
        nt_raw = signals.get("net_profit_trend", pd.Series(0.0, index=idx))

        net_yoy_raw = pd.to_numeric(df.get("d0_net_profit_yoy", pd.Series(index=idx)), errors="coerce")
        roe_raw = pd.to_numeric(df.get("d0_roe", pd.Series(index=idx)), errors="coerce")

        for ts_code in scores.index:
            if scores[ts_code] < _LABEL_THRESHOLD:
                continue
            r = []

            np_val = net_yoy_raw.get(ts_code, np.nan)
            np_rank = npp.get(ts_code, 50.0)
            if pd.notna(np_val):
                if np_rank > 80:
                    r.append(f"净利润增长{np_val:.1f}%，全市场前{100-np_rank:.0f}%")
                elif np_rank > 60:
                    r.append(f"净利润增长{np_val:.1f}%，优于{np_rank:.0f}%股票")

            roe_val = roe_raw.get(ts_code, np.nan)
            roe_rank = roe_p.get(ts_code, 50.0)
            if pd.notna(roe_val) and roe_rank > 60:
                r.append(f"ROE{roe_val:.1f}%，优于{roe_rank:.0f}%股票")

            rev_rank = rvp.get(ts_code, 50.0)
            if rev_rank > 80:
                r.append(f"营收增长强劲（前{100-rev_rank:.0f}%）")

            gm_rank = gm_p.get(ts_code, 50.0)
            if gm_rank > 80:
                r.append(f"毛利率领先（前{100-gm_rank:.0f}%）")

            nt_rank = nt_p.get(ts_code, 50.0)
            nt_val = nt_raw.get(ts_code, 0)
            if nt_rank > 80 and pd.notna(nt_val) and nt_val > 0:
                r.append(f"净利润连续加速增长（+{nt_val:.1f}pp）")
            elif nt_rank < 20 and pd.notna(nt_val) and nt_val < 0:
                r.append(f"净利润增速放缓（{nt_val:.1f}pp）")

            if r:
                reasons[ts_code] = r

        return reasons
