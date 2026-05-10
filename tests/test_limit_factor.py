# -*- coding: utf-8 -*-
"""LimitFactor 单元测试。

覆盖：空数据、一字板满分、跌停归零、炸板低于涨停、封板质量梯度、
连板强度梯度、pct_chg 百分位排名、describe 对齐、clamp、_bare_to_ts_code。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.limit_factor import LimitFactor


def _make_df(index_codes, **cols):
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


class TestLimitFactor:
    @pytest.fixture
    def factor(self):
        return LimitFactor()

    # --- 空数据 ---

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # --- 一字板满分 ---

    def test_yizi_ban_max_score(self, factor):
        """一字板（limit=U, open_times=0, limit_times>=5）：seal 35 + chain 35 + pct 高分."""
        df = _make_df(
            ["A.SH", "B.SH"],
            limit=["U", "U"],
            open_times=[0, 0],
            limit_times=[5, 5],
            pct_chg=[10.0, 9.98],
        )
        scores = factor.score(df)
        # 一字板封板质量=35, 连板=35, pct rank 在两人中前50%得~15, 合计~85
        assert scores["A.SH"] >= 80
        assert scores["B.SH"] >= 80

    # --- 跌停归零 ---

    def test_downt_limit_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["D"],
            open_times=[0],
            limit_times=[1],
            pct_chg=[-10.0],
        )
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_downt_limit_describe_empty(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["D"],
            open_times=[0],
            limit_times=[1],
            pct_chg=[-10.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    # --- 炸板低于涨停 ---

    def test_break_lower_than_up(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ"],
            limit=["U", "Z"],
            open_times=[0, 0],
            limit_times=[1, 1],
            pct_chg=[10.0, 5.0],
        )
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SZ"]

    # --- 封板质量梯度 (open_times) ---

    def test_seal_open_times_0(self, factor):
        """一字板 open_times=0 → seal=35."""
        df = _make_df(["A.SH"], limit=["U"], open_times=[0])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 35.0

    def test_seal_open_times_1(self, factor):
        df = _make_df(["A.SH"], limit=["U"], open_times=[1])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 28.0

    def test_seal_open_times_2(self, factor):
        df = _make_df(["A.SH"], limit=["U"], open_times=[2])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 20.0

    def test_seal_open_times_3(self, factor):
        df = _make_df(["A.SH"], limit=["U"], open_times=[3])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 12.0

    def test_seal_open_times_4(self, factor):
        df = _make_df(["A.SH"], limit=["U"], open_times=[4])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 6.0

    def test_seal_open_times_5_plus(self, factor):
        """open_times >= 5 → seal=0."""
        df = _make_df(["A.SH"], limit=["U"], open_times=[7])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 0.0

    def test_seal_break_open_0(self, factor):
        """炸板 open_times=0 → seal=8."""
        df = _make_df(["A.SH"], limit=["Z"], open_times=[0])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 8.0

    def test_seal_break_open_3_plus(self, factor):
        """炸板 open_times>=3 → seal=0."""
        df = _make_df(["A.SH"], limit=["Z"], open_times=[3])
        signals = factor._compute_signals(df)
        assert signals["seal"]["A.SH"] == 0.0

    # --- 连板强度 (limit_times) ---

    def test_chain_limit_1(self, factor):
        df = _make_df(["A.SH"], limit=["U"], limit_times=[1])
        signals = factor._compute_signals(df)
        assert signals["chain"]["A.SH"] == 15.0

    def test_chain_limit_2(self, factor):
        df = _make_df(["A.SH"], limit=["U"], limit_times=[2])
        signals = factor._compute_signals(df)
        assert signals["chain"]["A.SH"] == 23.0

    def test_chain_limit_3(self, factor):
        df = _make_df(["A.SH"], limit=["U"], limit_times=[3])
        signals = factor._compute_signals(df)
        assert signals["chain"]["A.SH"] == 29.0

    def test_chain_limit_4(self, factor):
        df = _make_df(["A.SH"], limit=["U"], limit_times=[4])
        signals = factor._compute_signals(df)
        assert signals["chain"]["A.SH"] == 33.0

    def test_chain_limit_5_plus(self, factor):
        df = _make_df(["A.SH"], limit=["U"], limit_times=[6])
        signals = factor._compute_signals(df)
        assert signals["chain"]["A.SH"] == 35.0

    def test_chain_break_half(self, factor):
        """炸板连板分 = 涨停连板分 * 0.4."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            limit=["U", "Z"],
            limit_times=[2, 2],
        )
        signals = factor._compute_signals(df)
        assert signals["chain"]["A.SH"] == 23.0
        assert signals["chain"]["B.SZ"] == pytest.approx(23.0 * 0.4, abs=0.1)

    # --- pct_chg 百分位排名 ---

    def test_pct_chg_rank_top(self, factor):
        """涨幅最高的涨停股 pct 得分最高."""
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            limit=["U", "U", "U"],
            pct_chg=[10.0, 9.95, 9.90],
        )
        signals = factor._compute_signals(df)
        assert signals["pct_chg"]["A.SH"] > signals["pct_chg"]["B.SZ"]
        assert signals["pct_chg"]["B.SZ"] > signals["pct_chg"]["C.SH"]

    def test_pct_chg_break_half_weight(self, factor):
        """炸板涨幅分权重减半."""
        df = _make_df(
            ["A.SH", "B.SZ"],
            limit=["U", "Z"],
            pct_chg=[8.0, 8.0],
        )
        signals = factor._compute_signals(df)
        assert signals["pct_chg"]["A.SH"] > signals["pct_chg"]["B.SZ"]

    # --- score clamp ---

    def test_score_clamped(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["U"],
            open_times=[0],
            limit_times=[5],
            pct_chg=[10.0],
        )
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    def test_multiple_stocks_in_range(self, factor):
        df = _make_df(
            ["A.SH", "B.SZ", "C.SH"],
            limit=["U", "U", "U"],
            open_times=[0, 2, 5],
            limit_times=[3, 1, 1],
            pct_chg=[10.0, 5.0, 3.0],
        )
        scores = factor.score(df)
        for code in df.index:
            assert 0 <= scores[code] <= 100
        assert scores["A.SH"] > scores["B.SZ"]
        assert scores["A.SH"] > scores["C.SH"]

    # --- describe ---

    def test_describe_yizi(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["U"],
            open_times=[0],
            limit_times=[1],
            pct_chg=[10.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert any("一字封板" in r for r in reasons["A.SH"])

    def test_describe_lianban(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["U"],
            open_times=[2],
            limit_times=[4],
            pct_chg=[10.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert any("连板龙头" in r for r in reasons["A.SH"])

    def test_describe_break(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["Z"],
            open_times=[1],
            limit_times=[2],
            pct_chg=[5.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert any("炸板" in r for r in reasons["A.SH"])

    def test_describe_empty_for_zero(self, factor):
        df = _make_df(
            ["A.SH"],
            limit=["U"],
            open_times=[10],
            limit_times=[0],
            pct_chg=[0.5],
        )
        scores = pd.Series(0.0, index=["A.SH"], name="limit")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    # --- factor attributes ---

    def test_factor_attributes(self, factor):
        assert factor.name == "limit"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    def test_score_series_name(self, factor):
        df = _make_df(["A.SH"], limit=["U"], pct_chg=[10.0])
        scores = factor.score(df)
        assert scores.name == "limit"


# ------------------------------------------------------------------
# _bare_to_ts_code 参数化
# ------------------------------------------------------------------

@pytest.mark.parametrize(
    "code,expected",
    [
        ("600519", "600519.SH"),   # 上海主板
        ("688001", "688001.SH"),   # 科创板
        ("000001", "000001.SZ"),   # 深圳主板
        ("002415", "002415.SZ"),   # 中小板
        ("300750", "300750.SZ"),   # 创业板
        ("430047", "430047.BJ"),   # 北交所 4 开头
        ("830799", "830799.BJ"),   # 北交所 8 开头
        ("920123", "920123.BJ"),   # 北交所 92 开头
        ("unknown", "unknown"),    # 未知格式原样返回
    ],
)
def test_bare_to_ts_code(code, expected):
    assert LimitFactor._bare_to_ts_code(code) == expected
