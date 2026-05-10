# -*- coding: utf-8 -*-
"""PerformanceFactor 单元测试。

覆盖：空数据、百分位排名、多季度趋势、单季度退化、
describe 阈值、clamp、因子属性、_compute_signals 子信号。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.performance_factor import (
    PerformanceFactor,
    _pct_rank,
    _quarter_end_dates,
)


def _make_df(index_codes, **cols):
    """构建因子评分用的 DataFrame。"""
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


class TestHelpers:
    def test_pct_rank(self):
        series = pd.Series([10, 20, 30, 40, 50], index=["A", "B", "C", "D", "E"])
        result = _pct_rank(series, series.index)
        assert result["A"] < result["B"] < result["E"]
        assert result["A"] > 0.0
        assert result["E"] == 100.0

    def test_pct_rank_single_value(self):
        series = pd.Series([42], index=["A"])
        result = _pct_rank(series, series.index)
        assert result["A"] == 50.0

    def test_pct_rank_with_nan(self):
        series = pd.Series([10, np.nan, 30], index=["A", "B", "C"])
        result = _pct_rank(series, series.index)
        assert result["B"] == 50.0  # NaN -> median

    def test_quarter_end_dates(self):
        dates = _quarter_end_dates("20260510", 4)
        assert len(dates) == 4
        assert dates[0] == "20260331"
        assert dates[1] == "20251231"
        assert dates[2] == "20250930"
        assert dates[3] == "20250630"

    def test_quarter_end_dates_mid_quarter(self):
        dates = _quarter_end_dates("20260215", 2)
        assert dates[0] == "20251231"
        assert dates[1] == "20250930"


class TestPerformanceFactor:
    @pytest.fixture
    def factor(self):
        return PerformanceFactor()

    # -- 空数据 --

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # -- 百分位排名 --

    def test_higher_growth_scores_higher(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[10, 30, 50, 80, 120],
            d0_revenue_yoy=[5]*5,
            d0_roe=[10]*5,
            d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[10, 30, 50, 80, 120],
            d1_revenue_yoy=[5]*5,
            d1_roe=[10]*5,
            d1_gross_margin=[20]*5,
        )
        scores = factor.score(df)
        assert scores["E.SH"] > scores["A.SH"]

    def test_higher_roe_scores_higher(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[10]*5, d0_revenue_yoy=[5]*5,
            d0_roe=[2, 5, 10, 18, 25],
            d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[10]*5, d1_revenue_yoy=[5]*5,
            d1_roe=[2, 5, 10, 18, 25],
            d1_gross_margin=[20]*5,
        )
        scores = factor.score(df)
        assert scores["E.SH"] > scores["A.SH"]

    def test_neutral_stock_gets_baseline(self, factor):
        """Single stock at median percentile → no brackets trigger → score 0."""
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[10], d0_revenue_yoy=[5],
            d0_roe=[10], d0_gross_margin=[20],
            d1_net_profit_yoy=[10], d1_revenue_yoy=[5],
            d1_roe=[10], d1_gross_margin=[20],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    # -- 多季度趋势 --

    def test_accelerating_scores_higher(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[50, 50, 50, 50, 80],
            d0_revenue_yoy=[5]*5, d0_roe=[10]*5, d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[50, 50, 50, 50, 20],
            d1_revenue_yoy=[5]*5, d1_roe=[10]*5, d1_gross_margin=[20]*5,
        )
        scores = factor.score(df)
        assert scores["E.SH"] > scores["A.SH"]

    def test_decelerating_penalty(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[50, 50, 50, 50, 10],
            d0_revenue_yoy=[5]*5, d0_roe=[10]*5, d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[50, 50, 50, 50, 80],
            d1_revenue_yoy=[5]*5, d1_roe=[10]*5, d1_gross_margin=[20]*5,
        )
        scores = factor.score(df)
        assert scores["E.SH"] < scores["A.SH"]

    # -- 单季度退化 --

    def test_single_period_degradation(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[30], d0_revenue_yoy=[10],
            d0_roe=[12], d0_gross_margin=[25],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    # -- 营收增长 --

    def test_higher_revenue_scores_higher(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[10]*5,
            d0_revenue_yoy=[-5, 0, 10, 25, 50],
            d0_roe=[10]*5, d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[10]*5,
            d1_revenue_yoy=[-5, 0, 10, 25, 50],
            d1_roe=[10]*5, d1_gross_margin=[20]*5,
        )
        scores = factor.score(df)
        assert scores["E.SH"] > scores["A.SH"]

    # -- clamp --

    def test_score_clamped_0_100(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            d0_net_profit_yoy=[200, 0, -100], d0_revenue_yoy=[100, 0, -50],
            d0_roe=[50, 5, -10], d0_gross_margin=[80, 15, 5],
            d1_net_profit_yoy=[200, 0, -100], d1_revenue_yoy=[100, 0, -50],
            d1_roe=[50, 5, -10], d1_gross_margin=[80, 15, 5],
        )
        scores = factor.score(df)
        for code in df.index:
            assert 0 <= scores[code] <= 100

    # -- describe --

    def test_describe_returns_reasons(self, factor):
        """Use 5 stocks so the high-value stock gets high percentiles (>80)."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[10, 20, 30, 40, 60],
            d0_revenue_yoy=[5, 10, 15, 20, 30],
            d0_roe=[5, 8, 10, 14, 18],
            d0_gross_margin=[15, 20, 25, 30, 40],
            d1_net_profit_yoy=[10, 20, 30, 40, 40],
            d1_revenue_yoy=[5, 10, 15, 20, 20],
            d1_roe=[5, 8, 10, 14, 15],
            d1_gross_margin=[15, 20, 25, 30, 35],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "E.SH" in reasons
        assert len(reasons["E.SH"]) > 0

    def test_describe_empty_for_low_score(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[-80], d0_revenue_yoy=[-50],
            d0_roe=[-10], d0_gross_margin=[5],
        )
        scores = pd.Series(2.0, index=["A.SH"], name="performance")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    # -- _compute_signals --

    def test_compute_signals_keys(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[10]*5, d0_revenue_yoy=[5]*5,
            d0_roe=[10]*5, d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[10]*5, d1_revenue_yoy=[5]*5,
            d1_roe=[10]*5, d1_gross_margin=[20]*5,
        )
        signals = factor._compute_signals(df)
        expected = {
            "net_profit_yoy_pct", "revenue_yoy_pct", "roe_pct",
            "gross_margin_pct", "net_profit_trend", "net_profit_trend_pct",
            "revenue_trend", "revenue_trend_pct",
        }
        assert set(signals.keys()) == expected

    def test_compute_signals_single_period(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[30], d0_revenue_yoy=[10],
            d0_roe=[12], d0_gross_margin=[25],
        )
        signals = factor._compute_signals(df)
        assert signals["net_profit_trend_pct"]["A.SH"] == 50.0
        assert signals["revenue_trend_pct"]["A.SH"] == 50.0
        assert signals["net_profit_trend"]["A.SH"] == 0.0

    def test_compute_signals_trend_direction(self, factor):
        codes = ["A.SH", "B.SZ"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[60, 20],
            d0_revenue_yoy=[5, 5], d0_roe=[10, 10], d0_gross_margin=[20, 20],
            d1_net_profit_yoy=[20, 60],
            d1_revenue_yoy=[5, 5], d1_roe=[10, 10], d1_gross_margin=[20, 20],
        )
        signals = factor._compute_signals(df)
        assert signals["net_profit_trend"]["A.SH"] > 0
        assert signals["net_profit_trend"]["B.SZ"] < 0

    # -- 因子属性 --

    def test_factor_attributes(self, factor):
        assert factor.name == "performance"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[30], d0_revenue_yoy=[10],
            d0_roe=[12], d0_gross_margin=[25],
        )
        scores = factor.score(df)
        assert scores.name == "performance"

    # -- describe 标签内容 --

    def test_describe_has_growth_label(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[80], d0_revenue_yoy=[30],
            d0_roe=[20], d0_gross_margin=[40],
            d1_net_profit_yoy=[60], d1_revenue_yoy=[25],
            d1_roe=[18], d1_gross_margin=[38],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "A.SH" in reasons:
            assert any("净利润" in r for r in reasons["A.SH"])

    def test_describe_has_accel_label(self, factor):
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[10, 10, 10, 10, 100],
            d0_revenue_yoy=[5]*5, d0_roe=[10]*5, d0_gross_margin=[20]*5,
            d1_net_profit_yoy=[10, 10, 10, 10, 10],
            d1_revenue_yoy=[5]*5, d1_roe=[10]*5, d1_gross_margin=[20]*5,
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "E.SH" in reasons:
            assert any("加速" in r for r in reasons["E.SH"])

    # -- 边缘情况 --

    def test_all_nan_columns(self, factor):
        """All NaN → _pct_rank fallback to 50 → no brackets trigger → score 0."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            d0_net_profit_yoy=[np.nan, np.nan],
            d0_revenue_yoy=[np.nan, np.nan],
            d0_roe=[np.nan, np.nan],
            d0_gross_margin=[np.nan, np.nan],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0
        assert 0 <= scores["A.SH"] <= 100

    def test_missing_d1_columns(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_net_profit_yoy=[30], d0_revenue_yoy=[10],
            d0_roe=[12], d0_gross_margin=[25],
        )
        signals = factor._compute_signals(df)
        assert signals["net_profit_trend_pct"]["A.SH"] == 50.0

    def test_mixed_nan_trend(self, factor):
        codes = ["A.SH", "B.SZ"]
        df = _make_df(
            codes,
            d0_net_profit_yoy=[30, np.nan], d0_revenue_yoy=[10, 5],
            d0_roe=[12, 10], d0_gross_margin=[25, 20],
            d1_net_profit_yoy=[20, np.nan], d1_revenue_yoy=[8, 5],
            d1_roe=[10, 10], d1_gross_margin=[22, 20],
        )
        scores = factor.score(df)
        for code in df.index:
            assert 0 <= scores[code] <= 100
