# -*- coding: utf-8 -*-
"""StopLossCalculator Layer 1 单元测试（纯 numpy，零 DB 依赖）。"""

import numpy as np
import pytest

from src.services.stop_loss_calculator import (
    _compute_true_range,
    _compute_atr,
    _compute_atr_series,
    _compute_atr_percentile,
    _compute_ma,
    _compute_max_drawdown,
    _select_stop_method,
    _build_tight_stop,
    _build_take_profits,
    compute_from_arrays,
)


# ---- helpers ----

def _make_ohlcv(prices: list, spread_pct: float = 0.02) -> tuple:
    """Generate high/low/close from a list of close prices."""
    closes = np.array(prices, dtype=float)
    highs = closes * (1 + spread_pct / 2)
    lows = closes * (1 - spread_pct / 2)
    return highs, lows, closes


# ===================================================================
# True Range
# ===================================================================

def test_true_range_constant():
    highs, lows, closes = _make_ohlcv([10.0] * 20, spread_pct=0.02)
    tr = _compute_true_range(highs, lows, closes)
    assert len(tr) == 20
    assert np.allclose(tr[1:], highs[1:] - lows[1:])


def test_true_range_gap_up():
    highs = np.array([10.5, 12.0])
    lows = np.array([9.5, 11.0])
    closes = np.array([10.0, 11.5])
    tr = _compute_true_range(highs, lows, closes)
    assert tr[0] == pytest.approx(1.0)
    assert tr[1] == pytest.approx(2.0)


def test_true_range_gap_down():
    highs = np.array([10.5, 9.0])
    lows = np.array([9.5, 8.0])
    closes = np.array([10.0, 8.5])
    tr = _compute_true_range(highs, lows, closes)
    assert tr[1] == pytest.approx(2.0)


# ===================================================================
# ATR
# ===================================================================

def test_atr_insufficient_data():
    highs, lows, closes = _make_ohlcv([10.0] * 10)
    assert _compute_atr(highs, lows, closes, period=20) is None


def test_atr_constant_prices():
    highs, lows, closes = _make_ohlcv([100.0] * 25, spread_pct=0.02)
    atr = _compute_atr(highs, lows, closes)
    expected = 100.0 * 0.02
    assert atr == pytest.approx(expected, rel=0.01)


def test_atr_series_length():
    highs, lows, closes = _make_ohlcv([100.0] * 30)
    s = _compute_atr_series(highs, lows, closes)
    assert len(s) == 30
    assert np.isnan(s[:19]).all()
    assert not np.isnan(s[19:]).all()


# ===================================================================
# ATR 百分位
# ===================================================================

def test_atr_percentile_insufficient():
    highs, lows, closes = _make_ohlcv([100.0] * 10)
    assert _compute_atr_percentile(highs, lows, closes) is None


def test_atr_percentile_range():
    np.random.seed(42)
    closes = 100 + np.cumsum(np.random.randn(100) * 2)
    highs = closes + np.abs(np.random.randn(100))
    lows = closes - np.abs(np.random.randn(100))
    p = _compute_atr_percentile(highs, lows, closes)
    assert p is not None
    assert 0 <= p <= 100


# ===================================================================
# MA
# ===================================================================

def test_ma_insufficient():
    closes = np.array([100.0] * 10)
    assert _compute_ma(closes, 20) is None


def test_ma_computation():
    closes = np.arange(1, 31, dtype=float)
    ma20 = _compute_ma(closes, 20)
    assert ma20 == pytest.approx(20.5)  # mean of 11..30


# ===================================================================
# 最大回撤
# ===================================================================

def test_max_drawdown_zero():
    closes = np.arange(100, 121, dtype=float)
    dd = _compute_max_drawdown(closes)
    assert dd == 0.0


def test_max_drawdown_with_drop():
    closes = np.array([100, 110, 120, 100, 90, 95], dtype=float)
    dd = _compute_max_drawdown(closes)
    assert dd == pytest.approx(-25.0, rel=0.1)


# ===================================================================
# 止损方法决策树
# ===================================================================

def test_select_stop_no_atr():
    method, reason, sl = _select_stop_method(
        current_price=100, atr_14=None, atr_percentile=None,
        ma20=None, ma60=None, swing_low_20=95, swing_high_20=105,
        boll_lower=None, factor_score=25.0,
    )
    # 无 ATR 无 Bollinger → close 百分比降级 (factor_score>=25: 0.94)
    assert method == "close_pct"
    assert sl == pytest.approx(100 * 0.94)


def test_select_stop_high_vol():
    method, reason, sl = _select_stop_method(
        current_price=100, atr_14=5.0, atr_percentile=80,
        ma20=98, ma60=95, swing_low_20=95, swing_high_20=105,
    )
    assert method == "volatility_band"
    assert sl == pytest.approx(100 - 3.0 * 5.0)


def test_select_stop_low_vol_uptrend():
    method, reason, sl = _select_stop_method(
        current_price=105, atr_14=2.0, atr_percentile=20,
        ma20=100, ma60=95, swing_low_20=98, swing_high_20=108,
    )
    assert method == "atr_trailing"
    assert sl == pytest.approx(108 - 2.0 * 2.0)


def test_select_stop_ma_proximity():
    method, reason, sl = _select_stop_method(
        current_price=101, atr_14=3.0, atr_percentile=50,
        ma20=100, ma60=95, swing_low_20=95, swing_high_20=110,
    )
    assert method == "ma_support"
    assert sl == pytest.approx(100 * 0.99)


def test_select_stop_default():
    method, reason, sl = _select_stop_method(
        current_price=105, atr_14=3.0, atr_percentile=50,
        ma20=100, ma60=95, swing_low_20=95, swing_high_20=110,
    )
    assert method == "swing_low"
    assert sl == pytest.approx(95 * 0.99)


# ===================================================================
# 止损/止盈构建
# ===================================================================

def test_tight_stop():
    assert _build_tight_stop(110, 4.0) == pytest.approx(110 - 1.5 * 4.0)
    assert _build_tight_stop(110, 0) is None
    assert _build_tight_stop(110, None) is None


def test_take_profits_with_atr():
    tp1, tp2 = _build_take_profits(100, 110, atr_14=4.0)
    assert tp1 == pytest.approx(max(100 * 1.03, 100 + 1.5 * 4))
    assert tp2 == pytest.approx(max(100 * 1.07, 110 + 2 * 4))


def test_take_profits_no_atr():
    tp1, tp2 = _build_take_profits(100, 110, atr_14=None)
    assert tp1 == pytest.approx(100 * 1.05)
    assert tp2 == pytest.approx(100 * 1.10)


# ===================================================================
# compute_from_arrays 集成测试
# ===================================================================

def test_compute_from_arrays_valid():
    highs, lows, closes = _make_ohlcv([100.0] * 80, spread_pct=0.02)
    result = compute_from_arrays(highs, lows, closes, code="000001")
    assert result.valid
    assert result.code == "000001"
    assert result.current_price == pytest.approx(100.0)
    assert result.atr_14 is not None
    assert result.atr_percentile is not None
    assert result.swing_low_20 is not None
    assert result.ma20 is not None
    assert result.ma60 is not None
    assert result.stop_loss is not None
    assert result.stop_method is not None


def test_compute_from_arrays_insufficient_data():
    highs, lows, closes = _make_ohlcv([100.0] * 10)
    result = compute_from_arrays(highs, lows, closes)
    assert not result.valid
    assert "数据不足" in result.error_msg


def test_compute_from_arrays_with_precomputed():
    highs, lows, closes = _make_ohlcv([100.0] * 80)
    result = compute_from_arrays(
        highs, lows, closes, ma20=99.5, ma60=98.0, atr=2.5,
    )
    assert result.ma20 == pytest.approx(99.5)
    assert result.ma60 == pytest.approx(98.0)
    assert result.atr_14 == pytest.approx(2.5)


def test_compute_to_dict():
    highs, lows, closes = _make_ohlcv([100.0] * 80)
    result = compute_from_arrays(highs, lows, closes, code="000001")
    d = result.to_dict()
    assert d["code"] == "000001"
    assert d["valid"] is True
    assert d["atr_14"] is not None
