# -*- coding: utf-8 -*-
"""BuybackFactor 单元测试。

覆盖：百分位辅助函数、_compute_signals 3 个子信号、score clamp、
describe 标签阈值、空数据。
"""

import pandas as pd
import pytest

from src.discovery.factors.buyback_factor import (
    BuybackFactor,
    _pct_rank,
)


def _make_df(index_codes, **cols):
    """构建因子评分用的 DataFrame，index 为 ts_code。"""
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
# BuybackFactor
# ------------------------------------------------------------------

class TestBuybackFactor:
    @pytest.fixture
    def factor(self):
        return BuybackFactor()

    # -- 空数据 --

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # -- 金额信号 --

    def test_amount_signal_higher_for_larger_amount(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            amount=[100, 500, 1000],
            vol=[0, 0, 0],
            proc=["", "", ""],
        )
        signals = factor._compute_signals(df)
        assert signals["amount"]["A.SH"] < signals["amount"]["C.SH"]
        assert signals["amount"]["C.SH"] == pytest.approx(40.0)

    def test_amount_all_zero(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            amount=[0, 0],
            vol=[0, 0],
            proc=["", ""],
        )
        signals = factor._compute_signals(df)
        assert (signals["amount"] == 0.0).all()

    # -- 数量信号 --

    def test_vol_signal_higher_for_larger_volume(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            amount=[0, 0, 0],
            vol=[10, 50, 100],
            proc=["", "", ""],
        )
        signals = factor._compute_signals(df)
        assert signals["vol"]["A.SH"] < signals["vol"]["C.SH"]
        assert signals["vol"]["C.SH"] == pytest.approx(30.0)

    def test_vol_all_zero(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            amount=[0, 0],
            vol=[0, 0],
            proc=["", ""],
        )
        signals = factor._compute_signals(df)
        assert (signals["vol"] == 0.0).all()

    # -- 进度信号 --

    def test_proc_implementing_gets_30(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[0],
            vol=[0],
            proc=["实施中"],
        )
        signals = factor._compute_signals(df)
        assert signals["proc"]["A.SH"] == 30.0

    def test_proc_completed_gets_15(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[0],
            vol=[0],
            proc=["完成"],
        )
        signals = factor._compute_signals(df)
        assert signals["proc"]["A.SH"] == 15.0

    def test_proc_empty_gets_0(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[0],
            vol=[0],
            proc=[""],
        )
        signals = factor._compute_signals(df)
        assert signals["proc"]["A.SH"] == 0.0

    # -- 总分 --

    def test_score_all_zeros(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[0], vol=[0], proc=[""],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_score_max_is_100(self, factor):
        """三个子信号满分 40+30+30=100，不超上限。"""
        df = _make_df(
            ["A.SH"],
            amount=[1e6],
            vol=[1e6],
            proc=["实施中"],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 100.0

    def test_score_clamped_0_100(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            amount=[1e6, 0],
            vol=[1e6, 0],
            proc=["实施中", ""],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100
        assert 0 <= scores["B.SZ"] <= 100

    def test_score_ordering(self, factor):
        df = _make_df(
            ["strong.SH", "weak.SZ"],
            amount=[5000, 10],
            vol=[500, 5],
            proc=["实施中", ""],
        )
        scores = factor.score(df)
        assert scores["strong.SH"] > scores["weak.SZ"]

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[100], vol=[50], proc=["实施中"],
        )
        scores = factor.score(df)
        assert scores.name == "buyback"

    # -- describe --

    def test_describe_returns_labels_for_strong_signals(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[50000],
            vol=[1000],
            proc=["实施中"],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert len(reasons["A.SH"]) > 0

    def test_describe_empty_for_zero_score(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[0], vol=[0], proc=[""],
        )
        scores = pd.Series([0.0], index=["A.SH"], name="buyback")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    def test_describe_no_labels_for_weak_signals(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            amount=[0, 0], vol=[0, 0], proc=["", ""],
        )
        scores = pd.Series([5.0, 5.0], index=["A.SH", "B.SZ"], name="buyback")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons
        assert "B.SZ" not in reasons

    def test_describe_includes_proc_status(self, factor):
        df = _make_df(
            ["A.SH"],
            amount=[50000],
            vol=[1000],
            proc=["实施中"],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        labels = reasons.get("A.SH", [])
        assert any("实施中" in l for l in labels)

    # -- 因子属性 --

    def test_factor_attributes(self, factor):
        assert factor.name == "buyback"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 10.0
