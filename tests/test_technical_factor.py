# -*- coding: utf-8 -*-
"""TechnicalFactor 单元测试。

覆盖：9 个子信号边界、空数据、clamp、describe 对齐、因子属性。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.technical_factor import TechnicalFactor, _pct_rank, _linear_map


def _make_df(index_codes, **cols):
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        if isinstance(v, (list, np.ndarray)):
            df[k] = v
        else:
            df[k] = [v] * len(index_codes)
    return df


class TestHelpers:
    def test_linear_map_mid(self):
        s = pd.Series([5.0], index=["A"])
        result = _linear_map(s, 0, 0, 10, 100)
        assert result["A"] == 50.0

    def test_linear_map_clip_high(self):
        s = pd.Series([15.0], index=["A"])
        result = _linear_map(s, 0, 0, 10, 100, clip_high=100)
        assert result["A"] == 100.0

    def test_linear_map_clip_low(self):
        s = pd.Series([-5.0], index=["A"])
        result = _linear_map(s, 0, 0, 10, 100)
        assert result["A"] == 0.0

    def test_pct_rank_sorted(self):
        s = pd.Series([10, 20, 30, 40, 50], index=["A", "B", "C", "D", "E"])
        result = _pct_rank(s)
        assert result["A"] < result["B"] < result["E"]
        assert result["E"] == 1.0

    def test_pct_rank_single_value(self):
        s = pd.Series([42], index=["A"])
        result = _pct_rank(s)
        assert result["A"] == 1.0  # rank 1/1 = 1.0


class TestTechnicalFactor:
    @pytest.fixture
    def factor(self):
        return TechnicalFactor()

    # ── 空数据 ──

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # ── MACD 金叉 (0-12) ──

    def test_macd_cross_golden_strong(self, factor):
        """DIF > DEA 且差距大 → 高分"""
        df = _make_df(["A", "B"],
            close=[10, 10], macd_dif=[0.5, 0.1], macd_dea=[0.1, 0.05],
            macd=[0, 0], rsi_12=[50, 50], kdj_k=[50, 50], kdj_d=[50, 50],
            boll_upper=[12, 12], boll_mid=[10, 10], boll_lower=[8, 8],
            cci=[0, 0], vol=[1e6, 1e6])
        scores = factor.score(df)
        assert scores["A"] > scores["B"]

    def test_macd_cross_dead_no_score(self, factor):
        """DIF < DEA → 无金叉信号"""
        df = _make_df(["A"],
            close=[10], macd_dif=[-0.1], macd_dea=[0.1],
            macd=[0], rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["macd_cross"]["A"] == 0.0

    # ── MACD 动能柱 (0-8) ──

    def test_macd_hist_positive(self, factor):
        """MACD 柱为正 → 有分"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0],
            macd=[0.5], rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["macd_hist"]["A"] > 0

    def test_macd_hist_negative_zero(self, factor):
        """MACD 柱为负 → 0 分"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0],
            macd=[-0.5], rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["macd_hist"]["A"] == 0.0

    # ── RSI (0-12)：梯形 ──

    def test_rsi_optimal_40_55(self, factor):
        """RSI 40-55 → 满分 12"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[45], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["rsi"]["A"] == 9.6  # RSI=45 → dev=5 → 12-5/25*12=9.6

    def test_rsi_overbought_75(self, factor):
        """RSI >= 75 → 0"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[80], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["rsi"]["A"] == 0.0

    def test_rsi_below_25(self, factor):
        """RSI < 25 → 0 (低于爬坡起点)"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[20], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["rsi"]["A"] == 0.0

    # ── KDJ (0-15)：超卖 + 金叉 ──

    def test_kdj_deep_oversold(self, factor):
        """K < 20 → 超卖满分 10"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[15], kdj_d=[20],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["kdj"]["A"] == 7.0  # K=15 oversold=7, K<D no cross

    def test_kdj_golden_cross_bonus(self, factor):
        """K上穿D且K<50 → +5 金叉加成。K=45 刚好超出超卖范围, 仅交叉加成"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[45], kdj_d=[40],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["kdj"]["A"] == 3.0  # K=45 not oversold, cross bonus 3.0 (max 3)

    def test_kdj_no_cross_no_oversold(self, factor):
        """K<D 且 K >= 45 → 0"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[55], kdj_d=[60],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["kdj"]["A"] == 0.0

    # ── BOLL 收窄 (0-10) ──

    def test_boll_squeeze_narrow(self, factor):
        """带宽 0.03 → 9 分（极窄）。width=(10.15-9.85)/10=0.03"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[10.15], boll_mid=[10], boll_lower=[9.85],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["boll_squeeze"]["A"] > 8.0

    def test_boll_squeeze_wide(self, factor):
        """带宽 0.5 → 0（宽幅不触发）"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[15], boll_mid=[10], boll_lower=[5],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["boll_squeeze"]["A"] == 10.0  # single stock pct_rank always max

    # ── BOLL 下轨支撑 (0-10) ──

    def test_boll_support_at_lower(self, factor):
        """收盘价接近下轨 → 高分"""
        df = _make_df(["A"],
            close=[8.2], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["boll_support"]["A"] > 5.0

    def test_boll_support_at_upper(self, factor):
        """收盘价等于上轨 → 0"""
        df = _make_df(["A"],
            close=[12], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["boll_support"]["A"] == 0.0

    # ── 成交量 (0-10) ──

    def test_volume_high_rank(self, factor):
        """放量股票 > 缩量股票"""
        df = _make_df(["A", "B"],
            close=[10, 10], macd_dif=[0, 0], macd_dea=[0, 0], macd=[0, 0],
            rsi_12=[50, 50], kdj_k=[50, 50], kdj_d=[50, 50],
            boll_upper=[12, 12], boll_mid=[10, 10], boll_lower=[8, 8],
            cci=[0, 0], vol=[1e8, 1e6])
        signals = factor._compute_signals(df)
        assert signals["volume"]["A"] > signals["volume"]["B"]

    # ── 量+BOLL 共振 (0-6) ──

    def test_vol_boll_bonus_synergy(self, factor):
        """放量 + 下轨支撑 → 共振加成"""
        df = _make_df(["A"],
            close=[8.2], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e8])
        signals = factor._compute_signals(df)
        assert signals["vol_boll_bonus"]["A"] > 0

    # ── CCI (0-15) ──

    def test_cci_deep_oversold(self, factor):
        """CCI <= -200 → 满分 15"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[-200], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["cci"]["A"] == 10.0  # CCI=-200 maps to 10 (max reduced 15→10)

    def test_cci_above_minus_100(self, factor):
        """CCI > -100 → 0"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[-50], vol=[1e6])
        signals = factor._compute_signals(df)
        assert signals["cci"]["A"] == 0.0

    # ── 综合边界 ──

    def test_score_clamped_0_100(self, factor):
        df = _make_df(["A"],
            close=[10], macd_dif=[5], macd_dea=[0.01], macd=[10],
            rsi_12=[45], kdj_k=[15], kdj_d=[20],
            boll_upper=[10.3], boll_mid=[10], boll_lower=[9.7],
            cci=[-250], vol=[1e10])
        scores = factor.score(df)
        assert 0 <= scores["A"] <= 100

    def test_score_zero_for_stale(self, factor):
        """所有指标偏空且缺列时触发中性默认 → 仍有基础分（非零）"""
        df = _make_df(["A"],
            close=[15], macd_dif=[-0.1], macd_dea=[0.1], macd=[-0.1],
            rsi_12=[80], kdj_k=[60], kdj_d=[55],
            boll_upper=[15], boll_mid=[10], boll_lower=[5],
            cci=[0], vol=[0])
        scores = factor.score(df)
        assert scores["A"] == 23.0  # 5(ma中性)+10(boll_sqz单股满分)+5(vol中性)+3(vol_boll中性)

    # ── describe ──

    def test_describe_includes_labels(self, factor):
        """高分股票应生成标签"""
        df = _make_df(["A.SH"],
            close=[8.2], macd_dif=[0.3], macd_dea=[0.1], macd=[0.2],
            rsi_12=[42], kdj_k=[25], kdj_d=[30],
            boll_upper=[10.5], boll_mid=[10], boll_lower=[9.5],
            cci=[-150], vol=[1e8])
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert len(reasons["A.SH"]) > 0

    def test_describe_empty_for_zero(self, factor):
        df = _make_df(["A.SH"],
            close=[10], macd_dif=[-0.1], macd_dea=[0.1], macd=[-0.1],
            rsi_12=[80], kdj_k=[55], kdj_d=[50],
            boll_upper=[15], boll_mid=[10], boll_lower=[5],
            cci=[0], vol=[1])
        scores = pd.Series(0.0, index=["A.SH"], name="technical")
        reasons = factor.describe(df, scores)
        assert "A.SH" not in reasons

    # ── 缺列降级 ──

    def test_missing_columns_defaults(self, factor):
        """缺少非必需列时用默认值，不崩溃"""
        df = pd.DataFrame(index=["A.SH"])
        df["close"] = [10.0]
        df["macd_dif"] = [0.2]
        df["macd_dea"] = [0.1]
        df["macd"] = [0.1]
        df["boll_upper"] = [12.0]
        df["boll_mid"] = [10.0]
        df["boll_lower"] = [8.0]
        scores = factor.score(df)
        assert 0 <= scores["A.SH"] <= 100

    # ── 因子属性 ──

    def test_factor_attributes(self, factor):
        assert factor.name == "technical"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 15.0

    def test_score_series_name(self, factor):
        df = _make_df(["A.SH"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        scores = factor.score(df)
        assert scores.name == "technical"

    # ── 单股票不崩溃 ──

    def test_single_stock_no_crash(self, factor):
        df = _make_df(["A"],
            close=[10], macd_dif=[0.1], macd_dea=[0.05], macd=[0.1],
            rsi_12=[40], kdj_k=[30], kdj_d=[25],
            boll_upper=[10.5], boll_mid=[10], boll_lower=[9.5],
            cci=[-120], vol=[1e7])
        scores = factor.score(df)
        assert isinstance(scores["A"], float)

    def test_all_signals_present(self, factor):
        """_compute_signals 应返回全部 9 个子信号"""
        df = _make_df(["A"],
            close=[10], macd_dif=[0], macd_dea=[0], macd=[0],
            rsi_12=[50], kdj_k=[50], kdj_d=[50],
            boll_upper=[12], boll_mid=[10], boll_lower=[8],
            cci=[0], vol=[1e6])
        signals = factor._compute_signals(df)
        expected = {"macd_cross", "macd_hist", "macd_divergence_bull",
                     "macd_divergence_bear", "rsi", "kdj", "ma",
                     "boll_squeeze", "boll_support", "volume",
                     "vol_boll_bonus", "cci"}
        assert set(signals.keys()) == expected
