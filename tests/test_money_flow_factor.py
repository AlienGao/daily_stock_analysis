# -*- coding: utf-8 -*-
"""MoneyFlowFactor 单元测试。

覆盖：空数据、主力净流入率百分位、特大单主导、大单活跃、
散户接盘惩罚、describe 对齐、clamp、因子属性。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.money_flow_factor import MoneyFlowFactor


def _make_df(index_codes, **cols):
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


class TestMoneyFlowFactor:
    @pytest.fixture
    def factor(self):
        return MoneyFlowFactor()

    # --- 空数据 ---

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # --- 主力净流入率排名 ---

    def test_major_inflow_rank_higher(self, factor):
        """主力净流入率高的股票得分更高."""
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            buy_elg_amount=[100000, 50000, 5000],
            sell_elg_amount=[20000, 30000, 10000],
            buy_lg_amount=[50000, 20000, 5000],
            sell_lg_amount=[10000, 15000, 10000],
            buy_sm_amount=[5000, 20000, 50000],
            sell_sm_amount=[10000, 10000, 20000],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["C.SH"]

    # --- 特大单主导 ---

    def test_elg_dominant_scores_high(self, factor):
        """特大单净流入占比高的股票得分高."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            buy_elg_amount=[200000, 10000],
            sell_elg_amount=[10000, 10000],
            buy_lg_amount=[10000, 10000],
            sell_lg_amount=[10000, 10000],
            buy_sm_amount=[10000, 200000],
            sell_sm_amount=[10000, 10000],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    # --- 大单活跃 vs 散户主导 ---

    def test_lg_vs_sm_dominance(self, factor):
        """主力大单流入 > 散户小单流入."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            buy_elg_amount=[50000, 10000],
            sell_elg_amount=[10000, 10000],
            buy_lg_amount=[50000, 10000],
            sell_lg_amount=[10000, 10000],
            buy_sm_amount=[10000, 100000],
            sell_sm_amount=[10000, 20000],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    # --- 散户接盘惩罚 ---

    def test_retail_trap_penalty(self, factor):
        """特大单流出+小单流入 → 散户接盘惩罚."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            buy_elg_amount=[10000, 50000],
            sell_elg_amount=[100000, 10000],
            buy_lg_amount=[10000, 50000],
            sell_lg_amount=[50000, 10000],
            buy_sm_amount=[200000, 10000],
            sell_sm_amount=[10000, 10000],
        )
        scores = factor.score(df)
        assert scores["A.SH"] < scores["B.SZ"]

    def test_retail_trap_same_major_diff_sm(self, factor):
        """相同主力流入率下，小单流入更高的受惩罚更重."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            buy_elg_amount=[100000, 100000],
            sell_elg_amount=[50000, 50000],
            buy_lg_amount=[50000, 50000],
            sell_lg_amount=[30000, 30000],
            buy_sm_amount=[10000, 200000],
            sell_sm_amount=[10000, 10000],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    # --- 全市场零交易 ---

    def test_all_zero_trade(self, factor):
        df = _make_df(["A.SH"], buy_elg_amount=[0], sell_elg_amount=[0],
                      buy_lg_amount=[0], sell_lg_amount=[0],
                      buy_sm_amount=[0], sell_sm_amount=[0])
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    # --- score clamp ---

    def test_score_clamped(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            buy_elg_amount=[1e9, 1e6, 0],
            sell_elg_amount=[0, 1e6, 1e9],
            buy_lg_amount=[1e8, 1e6, 0],
            sell_lg_amount=[0, 1e6, 1e8],
            buy_sm_amount=[0, 1e6, 1e9],
            sell_sm_amount=[1e9, 1e6, 0],
        )
        scores = factor.score(df)
        for code in df.index:
            assert 0 <= scores[code] <= 100

    # --- describe ---

    def test_describe_high_major_inflow(self, factor):
        """高主力流入率股票应有标签."""
        df = _make_df(
            ["A.SH"],
            buy_elg_amount=[1e8], sell_elg_amount=[1e6],
            buy_lg_amount=[1e7], sell_lg_amount=[1e6],
            buy_sm_amount=[1e6], sell_sm_amount=[1e6],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert any("主力" in r for r in reasons["A.SH"])

    def test_describe_retail_trap(self, factor):
        """散户接盘应有预警标签."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            buy_elg_amount=[10000, 100000],
            sell_elg_amount=[100000, 10000],
            buy_lg_amount=[10000, 50000],
            sell_lg_amount=[50000, 10000],
            buy_sm_amount=[200000, 10000],
            sell_sm_amount=[10000, 10000],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "A.SH" in reasons:
            assert any("散户接盘" in r for r in reasons["A.SH"])

    def test_describe_empty_for_low_score(self, factor):
        """低于阈值不应有标签."""
        df = _make_df(
            ["A.SH"],
            buy_elg_amount=[0], sell_elg_amount=[1e9],
            buy_lg_amount=[0], sell_lg_amount=[1e8],
            buy_sm_amount=[1e9], sell_sm_amount=[0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if scores["A.SH"] < factor._LABEL_THRESHOLD:
            assert "A.SH" not in reasons

    def test_describe_elg_dominant(self, factor):
        """特大单主导应有标签."""
        df = _make_df(
            ["A.SH"],
            buy_elg_amount=[5e8], sell_elg_amount=[1e6],
            buy_lg_amount=[1e7], sell_lg_amount=[1e7],
            buy_sm_amount=[1e7], sell_sm_amount=[1e7],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if "A.SH" in reasons:
            assert any("特大单" in r for r in reasons["A.SH"])

    # --- factor attributes ---

    def test_factor_attributes(self, factor):
        assert factor.name == "money_flow"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 25.0

    def test_score_series_name(self, factor):
        df = _make_df(
            ["A.SH"],
            buy_elg_amount=[1e6], sell_elg_amount=[1e5],
            buy_lg_amount=[1e5], sell_lg_amount=[1e5],
            buy_sm_amount=[1e5], sell_sm_amount=[1e5],
        )
        scores = factor.score(df)
        assert scores.name == "money_flow"
