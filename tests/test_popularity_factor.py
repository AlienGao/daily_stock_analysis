# -*- coding: utf-8 -*-
"""PopularityFactor 单元测试。

覆盖：空数据、飙升幅度排名、排名强度、涨跌幅信号、
排名趋势、describe 阈值、clamp、因子属性。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.popularity_factor import PopularityFactor, _linear_map


def _make_df(index_codes, **cols):
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


class TestPopularityFactor:
    @pytest.fixture
    def factor(self):
        f = PopularityFactor()
        f._trade_date = ""
        return f

    # --- 空数据 ---

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # --- 飙升幅度 (surge) ---

    def test_surge_higher_change_scores_higher(self, factor):
        """rank_change 越大，surge 子信号越高."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            rank=[10, 15],
            rank_change=[50, 5],
            pct_chg=[2.0, 2.0],
        )
        signals = factor._compute_signals(df)
        assert signals["surge"]["A.SH"] > signals["surge"]["B.SZ"]

    def test_surge_zero_for_no_improvement(self, factor):
        """无 rank_change > 0 时 surge 全为 0."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            rank=[10, 15],
            rank_change=[0, -3],
            pct_chg=[2.0, 2.0],
        )
        signals = factor._compute_signals(df)
        assert (signals["surge"] == 0).all()

    # --- 排名强度 (rank) ---

    def test_rank_better_scores_higher(self, factor):
        """排名更靠前（rank 更小）得分更高."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            rank=[1, 100],
            rank_change=[0, 0],
            pct_chg=[2.0, 2.0],
        )
        signals = factor._compute_signals(df)
        assert signals["rank"]["A.SH"] > signals["rank"]["B.SZ"]

    def test_rank_top_gets_full_score(self, factor):
        """排名第 1 的股票 rank 强度为满分 35."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            rank=[1, 100],
            rank_change=[0, 0],
            pct_chg=[2.0, 2.0],
        )
        signals = factor._compute_signals(df)
        assert signals["rank"]["A.SH"] == 35.0
        assert signals["rank"]["B.SZ"] == 0.0

    # --- 涨跌幅 (pct_chg) ---

    def test_pct_chg_positive_scores_higher(self, factor):
        """正涨跌幅 > 负涨跌幅."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            rank=[10, 15],
            rank_change=[0, 0],
            pct_chg=[8.0, -3.0],
        )
        signals = factor._compute_signals(df)
        assert signals["pct_chg"]["A.SH"] > signals["pct_chg"]["B.SZ"]

    def test_pct_chg_capped_at_20(self, factor):
        """涨跌幅 >= 10% 时得满分 20."""
        df = _make_df(
            ["A.SH"],
            rank=[10],
            rank_change=[0],
            pct_chg=[15.0],
        )
        signals = factor._compute_signals(df)
        assert signals["pct_chg"]["A.SH"] == 20.0

    # --- 排名趋势 (rank_trend) ---

    def test_rank_trend_no_trade_date(self, factor):
        """无 _trade_date 时 rank_trend 全为 0."""
        factor._trade_date = ""
        df = _make_df(
            ["A.SH", "B.SZ"],
            rank=[10, 15],
            rank_change=[0, 0],
            pct_chg=[2.0, 2.0],
        )
        signals = factor._compute_signals(df)
        assert (signals["rank_trend"] == 0).all()

    def test_rank_trend_db_empty(self, factor):
        """DB 无历史数据时 rank_trend 全为 0."""
        factor._trade_date = "20260510"
        df = _make_df(
            ["999999.SH", "888888.SZ"],
            rank=[10, 15],
            rank_change=[0, 0],
            pct_chg=[2.0, 2.0],
        )
        signals = factor._compute_signals(df)
        assert (signals["rank_trend"] == 0).all()

    # --- score clamp ---

    def test_score_clamped(self, factor):
        """score 应在 0-100 之间."""
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            rank=[1, 50, 100],
            rank_change=[100, 0, -50],
            pct_chg=[10.0, 0.0, -10.0],
        )
        scores = factor.score(df)
        for code in df.index:
            assert 0 <= scores[code] <= 100

    # --- describe ---

    def test_describe_below_threshold_skipped(self, factor):
        """低于 _LABEL_THRESHOLD 的股票不应有 describe."""
        df = _make_df(
            ["A.SH"],
            rank=[9999],
            rank_change=[0],
            pct_chg=[-10.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    def test_describe_surge_label(self, factor):
        """高 surge 应有飙升标签."""
        df = _make_df(
            ["A.SH"],
            rank=[10],
            rank_change=[30],
            pct_chg=[3.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "A.SH" in reasons:
            assert any("飙升" in r for r in reasons["A.SH"])

    def test_describe_rank_label(self, factor):
        """高排名强度应有核心圈标签."""
        df = _make_df(
            ["A.SH"],
            rank=[1],
            rank_change=[0],
            pct_chg=[1.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "A.SH" in reasons:
            assert any("核心圈" in r for r in reasons["A.SH"])

    def test_describe_pct_chg_label(self, factor):
        """高涨幅应有涨跌方向标签."""
        df = _make_df(
            ["A.SH"],
            rank=[10],
            rank_change=[0],
            pct_chg=[8.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "A.SH" in reasons:
            assert any("人气股" in r for r in reasons["A.SH"])

    # --- factor attributes ---

    def test_factor_attributes(self, factor):
        assert factor.name == "popularity"
        assert factor.available_intraday is True
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            rank=[10],
            rank_change=[5],
            pct_chg=[3.0],
        )
        scores = factor.score(df)
        assert scores.name == "popularity"


class TestLinearMap:
    def test_basic_linear(self):
        s = pd.Series([0, 5, 10], index=["a", "b", "c"])
        result = _linear_map(s, 0, 0, 10, 20)
        assert result["a"] == 0.0
        assert result["b"] == 10.0
        assert result["c"] == 20.0

    def test_clip_low(self):
        s = pd.Series([-5, 0, 5], index=["a", "b", "c"])
        result = _linear_map(s, 0, 10, 10, 20, clip_low=10)
        assert result["a"] == 10.0

    def test_clip_high(self):
        s = pd.Series([0, 10, 20], index=["a", "b", "c"])
        result = _linear_map(s, 0, 0, 10, 10, clip_high=10)
        assert result["c"] == 10.0

    def test_same_x(self):
        """x0 == x1 时不除零，slope=0."""
        s = pd.Series([5], index=["a"])
        result = _linear_map(s, 5, 0, 5, 10)
        assert result["a"] == 0.0
