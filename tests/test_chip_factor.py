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
        """wr 快速下降且成本稳定 → 洗盘信号加分."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50, 50, 50, 50, 80],
            d1_winner_rate=[50, 50, 50, 50, 20],  # E: 80→20, wr_change=-75%
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_50pct=[15, 14, 13, 12, 15],
            d1_cost_50pct=[15, 14, 13, 12, 15],  # E: cost50 stable (trend=0 >= 0)
        )
        scores = factor.score(df)
        # E: wr_change=-75% → wr_change_pct ≈ 20 (rank 1/5=20)
        # cost50_trend=0 >= 0 → confirmation passes
        # wr_change_pct > 60 AND cost50_trend >= 0 → +5 washout bonus
        # E out-scores neutral stocks
        assert scores["E.SH"] > scores["A.SH"]

    def test_wr_surge_risk_penalty(self, factor):
        """wr 快速上升被扣分（追高风险）."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50, 50, 50, 50, 30],
            d1_winner_rate=[50, 50, 50, 50, 70],  # E: 30→70, wr_change=+133%
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_50pct=[15]*5, d1_cost_50pct=[15]*5,
        )
        scores = factor.score(df)
        # E: wr_change=+133% → wr_change_pct=20 (rank 1/5), cost50_pct=60
        # wr_change_pct >= 20 & < 40 → -5 penalty
        # wr_moderate: |70-50|=20 → 15-20/50*15=9
        # E total ≈ 9 - 5 + 5(cost50) = 9
        # Others: wr_moderate=15 + 5(cost50) = 20
        assert scores["E.SH"] < scores["A.SH"]

    def test_wr_decline_outflow_penalty(self, factor):
        """wr 下降 + 成本中轴也降 → 量价齐跌真出逃，扣分."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50, 50, 50, 50, 80],
            d1_winner_rate=[50, 50, 50, 50, 20],  # E: 80→20, wr_change=-75%
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_50pct=[15, 14, 13, 12, 15],
            d1_cost_50pct=[15, 14, 13, 12, 10],  # E: cost50 declining (-33%)
        )
        scores = factor.score(df)
        # E: wr_change=-75%, cost50_trend=-33% < 0 → is_outflow → -10
        # A-D: neutral wr and cost50
        # E scores lowest due to outflow penalty
        assert scores["E.SH"] < scores["A.SH"]

    def test_wr_change_vol_normalized(self, factor):
        """wr 波动大的股票，相同 wr_change 归一化后影响更小."""
        codes = ["A.SH", "B.SZ"]
        df = _make_df(
            codes,
            d0_winner_rate=[50, 30], d1_winner_rate=[50, 70],
            # A: wr_change=0%, wr_vol=0→clipped to 1 → wr_change_norm=0
            # B: wr_change=133%, wr_vol=std([30,70])=28.3 → wr_change_norm=4.7
            d0_cost_5pct=[10, 10], d1_cost_5pct=[10, 10],
            d0_cost_95pct=[30, 30], d1_cost_95pct=[30, 30],
            d0_weight_avg=[20, 20], d1_weight_avg=[20, 20],
        )
        signals = factor._compute_signals(df)
        # B's wr_change_norm=4.7 >> A's 0, so -wr_change_norm ranking: B top pct
        assert signals["wr_change_pct"]["B.SZ"] < signals["wr_change_pct"]["A.SH"]

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
        """close 距 his_low 越近（经波动率归一化），得分越高."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50]*5, d1_winner_rate=[50]*5,
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            his_low=[10]*5,
            close=[10.2, 11.0, 12.0, 13.0, 14.0],  # A closest to his_low (2%)
            avg_range=[0.03]*5,  # 日均振幅 3%
        )
        scores = factor.score(df)
        # A: dist_to_low=2%, norm=2/3=0.67 → dist_low_pct=100 (>80 → +15)
        # E: dist_to_low=40%, norm=40/3=13.3 → dist_low_pct=20 → no bonus
        assert scores["A.SH"] > scores["E.SH"]

    # -- 距历史高点信号 --

    def test_close_to_high_risk(self, factor):
        """close 距 his_high 越近（经波动率归一化），得分越低."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50]*5, d1_winner_rate=[50]*5,
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            his_high=[20]*5,
            close=[19.0, 18.0, 17.0, 16.0, 15.0],  # A closest to his_high (5%)
            avg_range=[0.03]*5,  # 日均振幅 3%
        )
        scores = factor.score(df)
        # A: dist_to_high=5%, norm=5/3=1.67 → dist_high_pct=20 → <40 → -7 penalty
        # E: dist_to_high=25%, norm=25/3=8.33 → dist_high_pct=100 → no penalty
        assert scores["A.SH"] < scores["E.SH"]

    # -- 成本中轴趋势 --

    def test_cost50_uptrend_signal(self, factor):
        """成本中轴强势上移获加分."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50]*5, d1_winner_rate=[50]*5,
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_50pct=[10, 10, 10, 10, 10],
            d1_cost_50pct=[10, 11, 12, 15, 20],  # E: +100% trend
        )
        scores = factor.score(df)
        # E: cost50_trend=100% → cost50_pct=100 (>80 → +10)
        # A: cost50_trend=0% → cost50_pct=20 → no bonus
        # E total = 15 + 10 = 25, A total = 15
        assert scores["E.SH"] > scores["A.SH"]

    def test_cost50_downtrend_no_bonus(self, factor):
        """成本中轴下移不获加分（百分位低无奖励）."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50]*5, d1_winner_rate=[50]*5,
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_50pct=[10, 10, 10, 10, 20],
            d1_cost_50pct=[10, 10, 10, 10, 10],  # E: -50% downtrend
        )
        scores = factor.score(df)
        # E: cost50_trend=-50% → cost50_pct=20 → no bonus
        # A: cost50_trend=0% (10→10) → cost50_pct=100 (>80 → +10)
        # A out-scores E because A gets uptrend bonus, E doesn't
        assert scores["A.SH"] > scores["E.SH"]

    # -- 筹码不对称性 --

    def test_chip_skew_bullish(self, factor):
        """上方筹码松散 (skew 高) 获加分."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50]*5, d1_winner_rate=[50]*5,
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_15pct=[19, 18, 15, 12, 10], d1_cost_15pct=[19, 18, 15, 12, 10],
            d0_cost_50pct=[20]*5, d1_cost_50pct=[20]*5,
            d0_cost_85pct=[30, 27, 25, 23, 21], d1_cost_85pct=[30, 27, 25, 23, 21],
        )
        scores = factor.score(df)
        # A: upper=10, lower=1 → skew=10 → skew_pct=100 (>80 → +5)
        # E: upper=1, lower=10 → skew=0.1 → skew_pct=20 (<20 → -5)
        assert scores["A.SH"] > scores["E.SH"]

    def test_chip_skew_bearish(self, factor):
        """下方筹码松散 (skew 低) 得分低于上方松散股票."""
        codes = ["A.SH", "B.SZ", "C.SH", "D.SZ", "E.SH"]
        df = _make_df(
            codes,
            d0_winner_rate=[50]*5, d1_winner_rate=[50]*5,
            d0_cost_5pct=[10]*5, d1_cost_5pct=[10]*5,
            d0_cost_95pct=[30]*5, d1_cost_95pct=[30]*5,
            d0_weight_avg=[20]*5, d1_weight_avg=[20]*5,
            d0_cost_15pct=[19, 18, 15, 12, 10], d1_cost_15pct=[19, 18, 15, 12, 10],
            d0_cost_50pct=[20]*5, d1_cost_50pct=[20]*5,
            d0_cost_85pct=[30, 27, 25, 23, 21], d1_cost_85pct=[30, 27, 25, 23, 21],
        )
        scores = factor.score(df)
        # A: upper=10, lower=1 → skew=10 → skew_pct=100 (>80 → +5)
        # E: upper=1, lower=10 → skew=0.1 → skew_pct=20 → neutral (no penalty/bonus)
        # A gets +5 skew bonus → A > E
        assert scores["A.SH"] > scores["E.SH"]

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
            avg_range=[0.03],
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
