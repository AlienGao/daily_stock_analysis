# -*- coding: utf-8 -*-
"""ChipFactor 单元测试。

覆盖：空数据、梯度评分、多日趋势、筹码集中、describe 对齐、clamp。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.chip_factor import (
    ChipFactor,
    _safe_pct_change,
    _pct_rank,
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
    def test_safe_pct_change_positive(self):
        last = pd.Series([200, 100], index=["A", "B"])
        first = pd.Series([100, 50], index=["A", "B"])
        result = _safe_pct_change(last, first)
        assert result["A"] == 100.0
        assert result["B"] == 100.0

    def test_safe_pct_change_negative(self):
        last = pd.Series([50, 80], index=["A", "B"])
        first = pd.Series([100, 100], index=["A", "B"])
        result = _safe_pct_change(last, first)
        assert result["A"] == -50.0
        assert result["B"] == -20.0

    def test_safe_pct_change_zero_first(self):
        last = pd.Series([100], index=["A"])
        first = pd.Series([0], index=["A"])
        result = _safe_pct_change(last, first)
        assert result["A"] == 0.0

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


class TestChipFactor:
    @pytest.fixture
    def factor(self):
        return ChipFactor()

    # -- 空数据 --

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # -- 梯度评分 --

    def test_wr_50_gives_max_moderate(self, factor):
        """wr=50% 获最大值 (15 分)."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            d0_winner_rate=[50, 50], d1_winner_rate=[50, 50],
            d0_cost_5pct=[10, 20], d1_cost_5pct=[10, 20],
            d0_cost_95pct=[30, 40], d1_cost_95pct=[30, 40],
            d0_weight_avg=[20, 30], d1_weight_avg=[20, 30],
        )
        scores = factor.score(df)
        assert scores["A.SH"] >= 15.0

    def test_wr_0_gives_max_deep(self, factor):
        """wr=0% (全部套牢) 获深套满分 15."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[0], d1_winner_rate=[0],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[30], d1_cost_95pct=[30],
            d0_weight_avg=[20], d1_weight_avg=[20],
        )
        scores = factor.score(df)
        assert scores["A.SH"] >= 15.0

    def test_wr_100_gives_max_pressure(self, factor):
        """wr=100% (全部获利) 扣满 15 分."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[100], d1_winner_rate=[100],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[30], d1_cost_95pct=[30],
            d0_weight_avg=[20], d1_weight_avg=[20],
        )
        scores = factor.score(df)
        assert scores["A.SH"] <= 0

    # -- 多日趋势 --

    def test_wr_decline_washout_signal(self, factor):
        """wr 快速下降 >10% → 洗盘信号 +15."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[80], d1_winner_rate=[50],  # d1 是最新日，从 80 → 50
            d0_cost_5pct=[10], d1_cost_5pct=[20],
            d0_cost_95pct=[30], d1_cost_95pct=[40],
            d0_weight_avg=[20], d1_weight_avg=[30],
        )
        scores = factor.score(df)
        assert scores["A.SH"] >= 15.0

    def test_wr_surge_risk_penalty(self, factor):
        """wr 快速上升 >10% → 追高风险 -10."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[30], d1_winner_rate=[60],  # d1 最新：30 → 60
            d0_cost_5pct=[10], d1_cost_5pct=[20],
            d0_cost_95pct=[30], d1_cost_95pct=[40],
            d0_weight_avg=[20], d1_weight_avg=[30],
        )
        scores = factor.score(df)
        # wr_change = (60-30)/30*100 = 100% > 10% → -10
        # wr_moderate: |60-50|=10 → 15-10/50*15 = 12
        assert scores["A.SH"] < 12.0

    # -- 筹码集中 --

    def test_concentration_top_pct(self, factor):
        """筹码最集中的股票排名最高."""
        df = _make_df(
            ["A.SH", "B.SZ", "C.BJ"],
            d0_winner_rate=[50, 50, 50], d1_winner_rate=[50, 50, 50],
            d0_cost_5pct=[9.5, 5, 1], d1_cost_5pct=[9.5, 5, 1],
            d0_cost_95pct=[10.5, 15, 19], d1_cost_95pct=[10.5, 15, 19],
            d0_weight_avg=[10, 10, 10], d1_weight_avg=[10, 10, 10],
        )
        signals = factor._compute_signals(df)
        conc = signals["conc_pct"]
        # A: (10.5-9.5)/10 = 0.1 → most concentrated → highest pct
        # B: (15-5)/10 = 1.0
        # C: (19-1)/10 = 1.8 → least concentrated → lowest pct
        assert conc["A.SH"] > conc["B.SZ"] > conc["C.BJ"]

    # -- 距历史低点信号 --

    def test_close_to_low_rebound(self, factor):
        """close 距 his_low 10% 内加 10 分."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            d0_winner_rate=[50, 50], d1_winner_rate=[50, 50],
            d0_cost_5pct=[10, 10], d1_cost_5pct=[10, 10],
            d0_cost_95pct=[30, 30], d1_cost_95pct=[30, 30],
            d0_weight_avg=[20, 20], d1_weight_avg=[20, 20],
            his_low=[10.0, np.nan], close=[10.5, 11.0],
        )
        scores = factor.score(df)
        # A: dist_to_low=(10.5-10)/10*100=5% < 10% → +10
        # B: his_low=NaN → no bonus
        assert scores["A.SH"] > scores["B.SZ"]

    # -- 距历史高点信号 --

    def test_close_to_high_risk(self, factor):
        """close 距 his_high 5% 内扣 10 分."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            d0_winner_rate=[50, 50], d1_winner_rate=[50, 50],
            d0_cost_5pct=[10, 10], d1_cost_5pct=[10, 10],
            d0_cost_95pct=[30, 30], d1_cost_95pct=[30, 30],
            d0_weight_avg=[20, 20], d1_weight_avg=[20, 20],
            his_high=[20.0, np.nan], close=[19.5, 19.5],
        )
        scores = factor.score(df)
        # A: dist_to_high=(20-19.5)/20*100=2.5% < 5% → -10
        assert scores["A.SH"] < scores["B.SZ"]

    # -- 成本中轴趋势 --

    def test_cost50_uptrend_signal(self, factor):
        """成本中轴上移加 5-10 分."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[50], d1_winner_rate=[50],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[30], d1_cost_95pct=[30],
            d0_weight_avg=[20], d1_weight_avg=[20],
            d0_cost_50pct=[10], d1_cost_50pct=[15],  # 50% 增长
        )
        scores = factor.score(df)
        # cost50_trend = (15-10)/10*100 = 50% > 10 → +5+5=10 bonus
        assert scores["A.SH"] >= 20.0  # 15(moderate) + 10(trend) = 25

    def test_cost50_downtrend_penalty(self, factor):
        """成本中轴下移 >10% 扣 5 分."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[50], d1_winner_rate=[50],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[30], d1_cost_95pct=[30],
            d0_weight_avg=[20], d1_weight_avg=[20],
            d0_cost_50pct=[20], d1_cost_50pct=[10],  # -50% 下降
        )
        scores = factor.score(df)
        # cost50_trend = (10-20)/20*100 = -50% < -10 → -5
        assert scores["A.SH"] <= 10.0  # 15(moderate) - 5(penalty) = 10

    # -- 筹码不对称性 --

    def test_chip_skew_bullish(self, factor):
        """上方筹码松散 (skew > 2) 加 5 分."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[50], d1_winner_rate=[50],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[40], d1_cost_95pct=[40],
            d0_weight_avg=[20], d1_weight_avg=[20],
            d0_cost_15pct=[12], d1_cost_15pct=[12],
            d0_cost_50pct=[15], d1_cost_50pct=[15],
            d0_cost_85pct=[25], d1_cost_85pct=[25],
        )
        scores = factor.score(df)
        # upper=(25-15)=10, lower=(15-12)=3, skew=10/3=3.33 > 2 → +5
        assert scores["A.SH"] >= 20.0  # 15(moderate) + 5(skew) = 20

    def test_chip_skew_bearish(self, factor):
        """下方筹码松散 (skew < 0.5) 扣 5 分."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[50], d1_winner_rate=[50],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[40], d1_cost_95pct=[40],
            d0_weight_avg=[20], d1_weight_avg=[20],
            d0_cost_15pct=[10], d1_cost_15pct=[10],
            d0_cost_50pct=[15], d1_cost_50pct=[15],
            d0_cost_85pct=[16], d1_cost_85pct=[16],
        )
        scores = factor.score(df)
        # upper=(16-15)=1, lower=(15-10)=5, skew=0.2 < 0.5 → -5
        # 15(moderate) - 5(skew) = 10
        assert scores["A.SH"] <= 10.0

    # -- clamp --

    def test_score_clamped_0_100(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[0], d1_winner_rate=[100],
            d0_cost_5pct=[1], d1_cost_5pct=[1],
            d0_cost_95pct=[1.1], d1_cost_95pct=[1.1],
            d0_weight_avg=[1], d1_weight_avg=[1],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    # -- describe 对齐 --

    def test_describe_returns_reasons(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[80], d1_winner_rate=[50],  # 洗盘
            d0_cost_5pct=[9.5], d1_cost_5pct=[9.5],
            d0_cost_95pct=[10.5], d1_cost_95pct=[10.5],
            d0_weight_avg=[10], d1_weight_avg=[10],
            his_low=[8.0], close=[9.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert len(reasons["A.SH"]) > 0

    def test_describe_empty_for_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[100], d1_winner_rate=[100],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[30], d1_cost_95pct=[30],
            d0_weight_avg=[20], d1_weight_avg=[20],
        )
        scores = pd.Series(0.0, index=["A.SH"], name="chip")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    # -- 因子属性 --

    def test_factor_attributes(self, factor):
        assert factor.name == "chip"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[50], d1_winner_rate=[50],
            d0_cost_5pct=[10], d1_cost_5pct=[10],
            d0_cost_95pct=[30], d1_cost_95pct=[30],
            d0_weight_avg=[20], d1_weight_avg=[20],
        )
        scores = factor.score(df)
        assert scores.name == "chip"

    # -- 单日退化 --

    def test_single_day_degradation(self, factor):
        """仅 1 日数据时退化为单日评分."""
        df = _make_df(
            ["A.SH"],
            d0_winner_rate=[30], d0_cost_5pct=[10],
            d0_cost_95pct=[30], d0_weight_avg=[20],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100
