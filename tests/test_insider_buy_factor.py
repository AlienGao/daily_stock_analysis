# -*- coding: utf-8 -*-
"""InsiderBuyFactor 单元测试。

覆盖：空数据、梯度评分、时效性、describe 对齐、clamp。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.insider_buy_factor import InsiderBuyFactor


def _make_df(index_codes, **cols):
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


class TestInsiderBuyFactor:
    @pytest.fixture
    def factor(self):
        return InsiderBuyFactor()

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    def test_add_ratio_gradient(self, factor):
        """增持比例线性梯度：5% → 50 分，2.5% → 25 分."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            add_ratio=[5.0, 2.5],
        )
        scores = factor.score(df)
        assert scores["A.SH"] >= scores["B.SZ"]
        assert scores["A.SH"] >= 50.0
        assert 20 < scores["B.SZ"] < 30

    def test_add_ratio_capped(self, factor):
        """增持比例 >5% 不额外加分."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            add_ratio=[20.0, 5.0],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == pytest.approx(scores["B.SZ"], rel=0.05)

    def test_hold_ratio_gradient(self, factor):
        """持股比例线性梯度：10% → 25 分."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            add_ratio=[0, 0],
            hold_ratio=[10.0, 5.0],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    def test_recent_announcement_bonus(self, factor):
        """近期公告得分高于远期."""
        from datetime import datetime, timedelta
        recent = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        old = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        df = _make_df(
            ["A.SH", "B.SZ"],
            add_ratio=[2.0, 2.0],
            announce_date=[recent, old],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    def test_very_old_no_recency(self, factor):
        """超过 90 天无时效加分."""
        from datetime import datetime, timedelta
        old = (datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d")
        df = _make_df(
            ["A.SH"],
            add_ratio=[2.0],
            announce_date=[old],
        )
        signals = factor._compute_signals(df)
        assert signals["recency"].iloc[0] == 0.0

    def test_has_price_bonus(self, factor):
        """有交易均价加分."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            add_ratio=[2.0, 2.0],
            avg_price=[10.0, 0.0],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    def test_score_clamped(self, factor):
        df = _make_df(
            ["A.SH"],
            add_ratio=[5.0], hold_ratio=[10.0], avg_price=[10.0],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    def test_describe_returns_reasons(self, factor):
        from datetime import datetime, timedelta
        recent = datetime.now().strftime("%Y-%m-%d")
        df = _make_df(
            ["A.SH"],
            add_ratio=[4.5], hold_ratio=[9.0],
            avg_price=[10.0], announce_date=[recent],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert len(reasons["A.SH"]) > 0

    def test_describe_empty_for_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            add_ratio=[0], hold_ratio=[0],
        )
        scores = pd.Series(0.0, index=["A.SH"], name="insider_buy")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    def test_factor_attributes(self, factor):
        assert factor.name == "insider_buy"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    def test_score_series_name(self, factor):
        df = _make_df(["A.SH"], add_ratio=[2.0])
        scores = factor.score(df)
        assert scores.name == "insider_buy"
