# -*- coding: utf-8 -*-
"""MarginFactor 单元测试。

覆盖：空数据、多日趋势、市值归一化、空头信号、describe 对齐、clamp。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.margin_factor import MarginFactor
from src.discovery.factors.base import safe_pct_change, safe_ratio, pct_rank


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
    def test_safe_pct_change_positive(self):
        last = pd.Series([200, 100], index=["A", "B"])
        first = pd.Series([100, 50], index=["A", "B"])
        result = safe_pct_change(last, first)
        assert result["A"] == 100.0
        assert result["B"] == 100.0

    def test_safe_pct_change_negative(self):
        last = pd.Series([50, 80], index=["A", "B"])
        first = pd.Series([100, 100], index=["A", "B"])
        result = safe_pct_change(last, first)
        assert result["A"] == -50.0
        assert result["B"] == -20.0

    def test_safe_pct_change_zero_first(self):
        last = pd.Series([100], index=["A"])
        first = pd.Series([0], index=["A"])
        result = safe_pct_change(last, first)
        assert result["A"] == 0.0

    def test_safe_ratio(self):
        val = pd.Series([100, 200, 50], index=["A", "B", "C"])
        mv = pd.Series([1000, 1000, 0], index=["A", "B", "C"])
        result = safe_ratio(val, mv)
        assert result["A"] == 0.1
        assert result["B"] == 0.2
        assert np.isnan(result["C"])

    def test_pct_rank(self):
        series = pd.Series([10, 20, 30, 40, 50], index=["A", "B", "C", "D", "E"])
        result = pct_rank(series, series.index)
        # rank(pct=True) uses average method: [20, 40, 50, 80, 100]
        assert result["A"] < result["B"] < result["E"]
        assert result["A"] > 0.0
        assert result["E"] == 100.0

    def test_pct_rank_single_value(self):
        series = pd.Series([42], index=["A"])
        result = pct_rank(series, series.index)
        assert result["A"] == 50.0


class TestMarginFactor:
    @pytest.fixture
    def factor(self):
        return MarginFactor()

    # -- 空数据 --

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # -- 多日趋势：正向信号 --

    def test_rzmre_growth_gradient(self, factor):
        """融资买入额 5 日增长 20% → +4 (梯度: growth/100*20)"""
        df = _make_df(
            ["A.SH", "B.SZ"],
            d0_rzmre=[100, 200], d1_rzmre=[120, 150],
            d0_rzye=[500, 800], d1_rzye=[500, 800],
            d0_rzche=[0, 0], d1_rzche=[0, 0],
            d0_rqye=[0, 0], d1_rqye=[0, 0],
            d0_rqmcl=[0, 0], d1_rqmcl=[0, 0],
            total_mv=[1e9, 1e9],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == pytest.approx(8.0)  # 4(growth) + 4(ratio_trend)

    def test_rzche_decline_gradient(self, factor):
        """融资偿还额下降 20% → +3 (梯度: -growth/100*15). d1 is latest, d0 is earliest."""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[100], d1_rzmre=[100],
            d0_rzye=[500], d1_rzye=[500],
            d0_rzche=[100], d1_rzche=[80],  # 100->80 decline
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == pytest.approx(3.0)

    def test_margin_ratio_trend_up_adds_15(self, factor):
        """融资买入占比趋势上升 → +15"""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[50], d1_rzmre=[100],
            d0_rzye=[500], d1_rzye=[500],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert scores["A.SH"] >= 30.0

    # -- 市值归一化 --

    def test_rzye_ratio_high_pct_adds_score(self, factor):
        """融资余额占市值比超 70 分位 → +20"""
        df = _make_df(
            ["A.SH", "B.SZ", "C.BJ"],
            d0_rzmre=[100, 100, 100], d1_rzmre=[100, 100, 100],
            d0_rzye=[500, 500, 500], d1_rzye=[500, 500, 500],
            d0_rzche=[0, 0, 0], d1_rzche=[0, 0, 0],
            d0_rqye=[0, 0, 0], d1_rqye=[0, 0, 0],
            d0_rqmcl=[0, 0, 0], d1_rqmcl=[0, 0, 0],
            total_mv=[1e9, 1e10, 1e11],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["C.BJ"]

    # -- 负向信号 --

    def test_rqmcl_penalty(self, factor):
        """有融券卖出（未下降） → -10"""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[0], d1_rzmre=[0],
            d0_rzye=[0], d1_rzye=[0],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[50], d1_rqmcl=[50],  # 持平，不触发 short_covering
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert scores["A.SH"] <= 0

    def test_no_penalty_without_rqmcl(self, factor):
        """无融券卖出 → 不扣分"""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[100], d1_rzmre=[100],
            d0_rzye=[500], d1_rzye=[500],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert scores["A.SH"] >= 0

    def test_short_covering_adds_score(self, factor):
        """融券卖出大幅下降 → 空头平仓加分."""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[200], d1_rzmre=[200],
            d0_rzye=[500], d1_rzye=[500],
            d0_rzche=[100], d1_rzche=[0],   # 偿还下降 +15
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[200], d1_rqmcl=[20],  # -90% rqmcl → short_covering +9
            total_mv=[1e9],
        )
        scores = factor.score(df)
        # repay_decline +15 + short_covering +9 - short_selling -10 = 14
        assert scores["A.SH"] == pytest.approx(14.0, abs=1.0)

    def test_short_covering_no_trigger_below_threshold(self, factor):
        """融券卖出下降 < 20% → 不触发 short_covering."""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[0], d1_rzmre=[0],
            d0_rzye=[0], d1_rzye=[0],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[100], d1_rqmcl=[90],  # -10% only
            total_mv=[1e9],
        )
        scores = factor.score(df)
        # Only short_selling penalty (-10), no covering bonus
        assert scores["A.SH"] <= 0

    # -- 边界 --

    def test_score_clamped_0_100(self, factor):
        """分数限幅 0-100"""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[1e9], d1_rzmre=[1e5],
            d0_rzye=[1e8], d1_rzye=[1e7],
            d0_rzche=[1e5], d1_rzche=[1e9],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    def test_score_zero_for_inactive(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[0], d1_rzmre=[0],
            d0_rzye=[0], d1_rzye=[0],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    # -- describe 对齐 --

    def test_describe_returns_reasons(self, factor):
        """有正向信号时应生成 reasons."""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[100], d1_rzmre=[200],  # 买入额增长
            d0_rzye=[1000], d1_rzye=[1000],
            d0_rzche=[100], d1_rzche=[50],    # 偿还额下降
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e8],  # 小市值 → 高市值比
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert len(reasons["A.SH"]) > 0

    def test_describe_empty_for_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[0], d1_rzmre=[0],
            d0_rzye=[0], d1_rzye=[0],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = pd.Series(0.0, index=["A.SH"], name="margin")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    # -- 单日退化 --

    def test_single_day_degradation(self, factor):
        """仅 1 日数据时退化为单日评分。"""
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[100], d0_rzye=[500],
            d0_rzche=[0], d0_rqye=[0], d0_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    # -- 因子属性 --

    def test_factor_attributes(self, factor):
        assert factor.name == "margin"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 20.0

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_rzmre=[100], d1_rzmre=[100],
            d0_rzye=[500], d1_rzye=[500],
            d0_rzche=[0], d1_rzche=[0],
            d0_rqye=[0], d1_rqye=[0],
            d0_rqmcl=[0], d1_rqmcl=[0],
            total_mv=[1e9],
        )
        scores = factor.score(df)
        assert scores.name == "margin"
