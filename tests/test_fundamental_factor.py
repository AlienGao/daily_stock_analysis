# -*- coding: utf-8 -*-
"""FundamentalFactor 单元测试。

覆盖：辅助函数边界、_compute_signals 5 个子信号、score clamp、
describe 标签阈值、空数据。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.fundamental_factor import (
    FundamentalFactor,
    _linear_map,
    _pct_rank,
    _industry_pct_rank,
)


def _make_df(index_codes, **cols):
    """构建因子评分用的 DataFrame，index 为 ts_code。"""
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


# ------------------------------------------------------------------
# 辅助函数
# ------------------------------------------------------------------

class TestHelpers:
    def test_pct_rank_basic(self):
        s = pd.Series([10, 20, 30, 40, 50])
        result = _pct_rank(s)
        assert result.iloc[0] < result.iloc[-1]
        assert 0.0 < result.iloc[0] < 1.0
        assert result.iloc[-1] == 1.0

    def test_pct_rank_single_value(self):
        s = pd.Series([42])
        result = _pct_rank(s)
        assert result.iloc[0] == 1.0

    def test_pct_rank_all_nan(self):
        s = pd.Series([np.nan, np.nan])
        result = _pct_rank(s)
        assert result.iloc[0] == 0.0
        assert result.iloc[1] == 0.0

    def test_pct_rank_all_same(self):
        s = pd.Series([5, 5, 5])
        result = _pct_rank(s)
        # 3 tied values → avg rank=2 → pct=2/3
        assert result.iloc[0] == pytest.approx(2 / 3)

    def test_linear_map_basic(self):
        s = pd.Series([1, 2, 3, 4, 5])
        result = _linear_map(s, 1, 0, 5, 20)
        assert result.iloc[0] == 0.0
        assert result.iloc[-1] == 20.0
        assert result.iloc[2] == 10.0

    def test_linear_map_clip_low(self):
        s = pd.Series([0, 1, 2])
        result = _linear_map(s, 1, 5, 3, 15, clip_low=0.0)
        assert result.iloc[0] == 0.0

    def test_linear_map_zero_range(self):
        s = pd.Series([1, 2, 3])
        result = _linear_map(s, 5, 10, 5, 10)
        assert (result == 10.0).all()

    def test_industry_pct_rank_large_groups(self):
        vals = pd.Series([10, 20, 30, 40, 50, 60], index=list("ABCDEF"))
        inds = pd.Series(["X", "X", "X", "Y", "Y", "Y"], index=list("ABCDEF"))
        result = _industry_pct_rank(vals, inds, 30.0)
        assert result["C"] == 30.0
        assert result["F"] == 30.0
        assert result["A"] > 0.0

    def test_industry_pct_rank_small_group_fallback(self):
        vals = pd.Series([10, 20, 30, 40], index=list("ABCD"))
        inds = pd.Series(["X", "X", "Y", "Z"], index=list("ABCD"))
        result = _industry_pct_rank(vals, inds, 20.0)
        assert result["C"] > 0.0
        assert result["D"] == 20.0


# ------------------------------------------------------------------
# FundamentalFactor
# ------------------------------------------------------------------

class TestFundamentalFactor:
    @pytest.fixture
    def factor(self):
        return FundamentalFactor()

    # -- 空数据 --

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # -- PE 子信号 --

    def test_pe_signal_low_pe_scores_high(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ", "C.BJ"],
            pe=[5, 15, 50],
            pb=[2, 2, 2],
            turnover_rate=[1, 1, 1],
            volume_ratio=[1, 1, 1],
            total_mv=[5e8, 5e8, 5e8],
            industry=["银行", "银行", "银行"],
        )
        signals = factor._compute_signals(df)
        assert signals["pe"]["A.SH"] > signals["pe"]["C.BJ"]

    def test_pe_negative_ignored(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            pe=[-5, 10],
            pb=[2, 2],
            turnover_rate=[1, 1],
            volume_ratio=[1, 1],
            total_mv=[5e8, 5e8],
            industry=["银行", "银行"],
        )
        signals = factor._compute_signals(df)
        assert signals["pe"]["A.SH"] == 0.0
        assert signals["pe"]["B.SZ"] > 0.0

    def test_pe_all_negative(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            pe=[-5, -10],
            pb=[2, 2],
            turnover_rate=[1, 1],
            volume_ratio=[1, 1],
            total_mv=[5e8, 5e8],
        )
        signals = factor._compute_signals(df)
        assert (signals["pe"] == 0.0).all()

    # -- PB 子信号 --

    def test_pb_signal_low_pb_scores_high(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            pe=[15, 15],
            pb=[1, 5],
            turnover_rate=[1, 1],
            volume_ratio=[1, 1],
            total_mv=[5e8, 5e8],
            industry=["银行", "银行"],
        )
        signals = factor._compute_signals(df)
        assert signals["pb"]["A.SH"] > signals["pb"]["B.SZ"]

    def test_pb_negative_ignored(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            pe=[15, 15],
            pb=[-1, 3],
            turnover_rate=[1, 1],
            volume_ratio=[1, 1],
            total_mv=[5e8, 5e8],
        )
        signals = factor._compute_signals(df)
        assert signals["pb"]["A.SH"] == 0.0
        assert signals["pb"]["B.SZ"] > 0.0

    # -- 换手率子信号 --

    def test_turnover_high_scores_full(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[6], volume_ratio=[1],
            total_mv=[5e8],
        )
        signals = factor._compute_signals(df)
        assert signals["turnover"]["A.SH"] == 20.0

    def test_turnover_low_scores_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[0.3], volume_ratio=[1],
            total_mv=[5e8],
        )
        signals = factor._compute_signals(df)
        assert signals["turnover"]["A.SH"] == 0.0

    def test_turnover_mid_range_monotonic(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            pe=[15, 15], pb=[2, 2],
            turnover_rate=[1.5, 3.5],
            volume_ratio=[1, 1],
            total_mv=[5e8, 5e8],
        )
        signals = factor._compute_signals(df)
        assert signals["turnover"]["B.SZ"] > signals["turnover"]["A.SH"]

    # -- 量比子信号 --

    def test_volume_ratio_high_scores_full(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[1], volume_ratio=[2.5],
            total_mv=[5e8],
        )
        signals = factor._compute_signals(df)
        assert signals["volume_ratio"]["A.SH"] == 15.0

    def test_volume_ratio_low_scores_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[1], volume_ratio=[0.5],
            total_mv=[5e8],
        )
        signals = factor._compute_signals(df)
        assert signals["volume_ratio"]["A.SH"] == 0.0

    # -- 市值子信号 --

    def test_market_cap_mid_range_scores_full(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            pe=[15, 15], pb=[2, 2], turnover_rate=[1, 1],
            volume_ratio=[1, 1],
            total_mv=[5e9, 3e10],
        )
        signals = factor._compute_signals(df)
        assert signals["market_cap"]["A.SH"] == 15.0

    def test_market_cap_large_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[1], volume_ratio=[1],
            total_mv=[1e12],
        )
        signals = factor._compute_signals(df)
        assert signals["market_cap"]["A.SH"] == 0.0

    # -- 总分 --

    def test_score_clamped_0_100(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[5], pb=[0.8], turnover_rate=[6], volume_ratio=[2.5],
            total_mv=[5e9],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    def test_score_all_zeros_for_useless_stock(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[-1], pb=[-1], turnover_rate=[0.1], volume_ratio=[0.3],
            total_mv=[2e12],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[2], volume_ratio=[1.2],
            total_mv=[5e9],
        )
        scores = factor.score(df)
        assert scores.name == "fundamental"

    def test_score_multiple_stocks_ordering(self, factor):
        df = _make_df(
            ["good.SH", "bad.SZ"],
            pe=[5, 200],
            pb=[1, 10],
            turnover_rate=[4, 0.2],
            volume_ratio=[2, 0.5],
            total_mv=[5e9, 5e11],
        )
        scores = factor.score(df)
        assert scores["good.SH"] > scores["bad.SZ"]

    # -- describe --

    def test_describe_returns_labels_for_high_score(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[5], pb=[1], turnover_rate=[5], volume_ratio=[2],
            total_mv=[2e9],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert len(reasons["A.SH"]) > 0

    def test_describe_empty_for_zero_score(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[-1], pb=[-1], turnover_rate=[0.1], volume_ratio=[0.3],
            total_mv=[2e12],
        )
        scores = pd.Series([0.0], index=["A.SH"], name="fundamental")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    def test_describe_no_labels_below_threshold(self, factor):
        """多只股票中 PE/PB 最高者 → 行业内百分位最低 → 不触发标签."""
        df = _make_df(
            ["bad.SH", "ok1.SZ", "ok2.BJ"],
            pe=[500, 15, 20],
            pb=[8, 2, 3],
            turnover_rate=[0.6, 2, 3],
            volume_ratio=[0.9, 1.2, 1.5],
            total_mv=[3e10, 5e9, 8e9],
            industry=["银行", "银行", "银行"],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert scores["bad.SH"] < scores["ok1.SZ"]
        assert "bad.SH" not in reasons

    # -- 因子属性 --

    def test_factor_attributes(self, factor):
        assert factor.name == "fundamental"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 20.0

    # -- 默认行业 --

    def test_default_industry_when_missing(self, factor):
        df = _make_df(
            ["A.SH"],
            pe=[15], pb=[2], turnover_rate=[2], volume_ratio=[1.2],
            total_mv=[5e9],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100
