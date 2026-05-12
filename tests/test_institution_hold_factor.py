# -*- coding: utf-8 -*-
"""InstitutionHoldFactor 单元测试。

覆盖：百分位辅助函数、_compute_signals 4 个子信号、score clamp、
describe 标签阈值、空数据。
"""

import pandas as pd
import pytest

from src.discovery.factors.institution_hold_factor import (
    InstitutionHoldFactor,
    _pct_rank,
)


def _make_df(index_codes, **cols):
    """构建因子评分用的 DataFrame，index 为 code。"""
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, tuple)):
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
        assert result.iloc[-1] == 1.0

    def test_pct_rank_all_nan(self):
        s = pd.Series([float("nan"), float("nan")])
        result = _pct_rank(s)
        assert (result == 0.0).all()

    def test_pct_rank_single_value(self):
        s = pd.Series([42])
        result = _pct_rank(s)
        assert result.iloc[0] == 1.0


# ------------------------------------------------------------------
# InstitutionHoldFactor
# ------------------------------------------------------------------

class TestInstitutionHoldFactor:
    @pytest.fixture
    def factor(self):
        return InstitutionHoldFactor()

    # -- 空数据 --

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # -- 机构数量信号 --

    def test_inst_count_signal_higher_for_more_institutions(self, factor):
        df = _make_df(
            ["A", "B", "C"],
            inst_count=[1, 5, 10],
            inst_count_change=[0, 0, 0],
            hold_ratio=[2, 2, 2],
            hold_ratio_change=[0, 0, 0],
        )
        signals = factor._compute_signals(df)
        assert signals["inst_count"]["A"] < signals["inst_count"]["C"]
        assert signals["inst_count"]["C"] == pytest.approx(20.0)

    def test_inst_count_all_zero(self, factor):
        df = _make_df(
            ["A", "B"],
            inst_count=[0, 0],
            inst_count_change=[0, 0],
            hold_ratio=[0, 0],
            hold_ratio_change=[0, 0],
        )
        signals = factor._compute_signals(df)
        assert (signals["inst_count"] == 0.0).all()

    # -- 机构数变化信号 --

    def test_inst_count_change_positive_scores_high(self, factor):
        df = _make_df(
            ["A", "B", "C"],
            inst_count=[5, 5, 5],
            inst_count_change=[-2, 0, 5],
            hold_ratio=[2, 2, 2],
            hold_ratio_change=[0, 0, 0],
        )
        signals = factor._compute_signals(df)
        assert signals["inst_count_change"]["C"] > signals["inst_count_change"]["B"]
        assert signals["inst_count_change"]["C"] == pytest.approx(25.0)

    def test_inst_count_change_negative_and_zero_get_zero(self, factor):
        df = _make_df(
            ["A", "B", "C"],
            inst_count=[5, 5, 5],
            inst_count_change=[-5, 0, 3],
            hold_ratio=[2, 2, 2],
            hold_ratio_change=[0, 0, 0],
        )
        signals = factor._compute_signals(df)
        assert signals["inst_count_change"]["A"] == 0.0
        assert signals["inst_count_change"]["B"] == 0.0
        assert signals["inst_count_change"]["C"] == 25.0

    # -- 持股比例信号 --

    def test_hold_ratio_signal_higher_for_larger_ratio(self, factor):
        df = _make_df(
            ["A", "B", "C"],
            inst_count=[5, 5, 5],
            inst_count_change=[0, 0, 0],
            hold_ratio=[1, 5, 15],
            hold_ratio_change=[0, 0, 0],
        )
        signals = factor._compute_signals(df)
        assert signals["hold_ratio"]["A"] < signals["hold_ratio"]["C"]
        assert signals["hold_ratio"]["C"] == pytest.approx(18.0)

    def test_hold_ratio_all_zero(self, factor):
        df = _make_df(
            ["A", "B"],
            inst_count=[5, 5],
            inst_count_change=[0, 0],
            hold_ratio=[0, 0],
            hold_ratio_change=[0, 0],
        )
        signals = factor._compute_signals(df)
        assert (signals["hold_ratio"] == 0.0).all()

    # -- 持股比例增幅信号 --

    def test_hold_ratio_change_positive_scores_high(self, factor):
        df = _make_df(
            ["A", "B", "C"],
            inst_count=[5, 5, 5],
            inst_count_change=[0, 0, 0],
            hold_ratio=[5, 5, 5],
            hold_ratio_change=[-1, 0, 3],
        )
        signals = factor._compute_signals(df)
        assert signals["hold_ratio_change"]["C"] > signals["hold_ratio_change"]["B"]
        assert signals["hold_ratio_change"]["C"] == pytest.approx(22.0)

    # -- 总分 --

    def test_score_all_zeros(self, factor):
        df = _make_df(
            ["A"],
            inst_count=[0], inst_count_change=[0],
            hold_ratio=[0], hold_ratio_change=[0],
        )
        scores = factor.score(df)
        assert scores["A"] == 0.0

    def test_score_clamped_0_100(self, factor):
        df = _make_df(
            ["A", "B"],
            inst_count=[50, 0],
            inst_count_change=[20, -10],
            hold_ratio=[30, 0],
            hold_ratio_change=[5, -5],
        )
        scores = factor.score(df)
        assert 0 <= scores["A"] <= 100
        assert 0 <= scores["B"] <= 100

    def test_score_ordering(self, factor):
        df = _make_df(
            ["strong.SH", "weak.SZ"],
            inst_count=[20, 1],
            inst_count_change=[5, -3],
            hold_ratio=[15, 0.5],
            hold_ratio_change=[3, -2],
        )
        scores = factor.score(df)
        assert scores["strong.SH"] > scores["weak.SZ"]

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A"],
            inst_count=[5], inst_count_change=[0],
            hold_ratio=[3], hold_ratio_change=[0],
        )
        scores = factor.score(df)
        assert scores.name == "institution_hold"

    # -- describe --

    def test_describe_returns_labels_for_strong_signals(self, factor):
        df = _make_df(
            ["A"],
            inst_count=[10],
            inst_count_change=[3],
            hold_ratio=[8],
            hold_ratio_change=[2],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A" in reasons
        assert len(reasons["A"]) > 0

    def test_describe_empty_for_zero_score(self, factor):
        df = _make_df(
            ["A"],
            inst_count=[0], inst_count_change=[0],
            hold_ratio=[0], hold_ratio_change=[0],
        )
        scores = pd.Series([0.0], index=["A"], name="institution_hold")
        reasons = factor.describe(df, scores)
        assert "A" not in reasons

    def test_describe_no_labels_for_weak_signals(self, factor):
        df = _make_df(
            ["A", "B"],
            inst_count=[0, 0], inst_count_change=[0, 0],
            hold_ratio=[0, 0], hold_ratio_change=[0, 0],
        )
        scores = pd.Series([5.0, 5.0], index=["A", "B"], name="institution_hold")
        reasons = factor.describe(df, scores)
        assert "A" not in reasons
        assert "B" not in reasons

    def test_describe_includes_change_direction(self, factor):
        df = _make_df(
            ["A"],
            inst_count=[10],
            inst_count_change=[5],
            hold_ratio=[8],
            hold_ratio_change=[3],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        labels = reasons.get("A", [])
        assert any("+" in l for l in labels)

    # -- 因子属性 --

    def test_factor_attributes(self, factor):
        assert factor.name == "institution_hold"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    # -- 降级：akshare 原始中文列名 --

    def test_compute_signals_with_chinese_columns(self, factor):
        df_raw = _make_df(
            ["A", "B"],
            机构数=[5, 3],
            机构数变化=[1, 0],
            持股比例=[5, 2],
            持股比例增幅=[0.5, 0],
        )
        signals = factor._compute_signals(df_raw)
        assert signals["inst_count"]["A"] == 0.0
