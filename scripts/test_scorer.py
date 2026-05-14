# -*- coding: utf-8 -*-
"""StockScorer 评分逻辑边界测试。

覆盖：
1. 各维度评分边界值
2. 市场形态判定逻辑
3. 加速初期/末期判定
4. 所有形态权重归一化
5. 最终 composite 分极端场景
"""

import sys
import os
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.stock_scorer import (
    StockScorer, StockScorerConfig, TechScoreResult,
    _BASE_WEIGHTS, _HIGH_VOL_WEIGHTS, _STRONG_TREND_UP_WEIGHTS,
    _STRONG_TREND_DOWN_WEIGHTS, _CALM_WEIGHTS, _CRISIS_WEIGHTS,
    _BEARISH_WEIGHTS,
)

PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} — {detail}")


# ===================================================================
# 辅助：构造有波动的价格序列
# ===================================================================

def make_trending_data(start, end, n=30, noise=0.0):
    """构造有趋势的价格序列，带可选噪声。"""
    rng = np.random.RandomState(42)
    base = np.linspace(start, end, n)
    if noise > 0:
        base += rng.normal(0, noise, n)
    return base.astype(float)


def make_ohlcv_from_closes(closes):
    """从 closes 构造标准 OHLCV 格式。"""
    highs = closes * 1.01
    lows = closes * 0.99
    return highs, lows, closes


# ===================================================================
# 1. 各维度评分边界
# ===================================================================

def test_rr_score():
    print("\n[1] RR 赔率评分")
    scorer = StockScorer.__new__(StockScorer)

    check("RR=2 → 100", scorer._calc_rr_score(10, 14, 8) == 100.0,
          f"got {scorer._calc_rr_score(10, 14, 8)}")
    check("RR=1 → 50", scorer._calc_rr_score(10, 12, 8) == 50.0,
          f"got {scorer._calc_rr_score(10, 12, 8)}")
    check("TP1=price → 10 (精度保护)", scorer._calc_rr_score(10, 10, 8) == 10.0)
    check("price=stop_loss → 0", scorer._calc_rr_score(8, 12, 8) == 0.0)
    check("price<stop_loss → 0", scorer._calc_rr_score(7, 12, 8) == 0.0)
    check("RR=3 → 100 (capped)", scorer._calc_rr_score(10, 16, 8) == 100.0,
          f"got {scorer._calc_rr_score(10, 16, 8)}")
    check("TP1明显<price → 0", scorer._calc_rr_score(10, 8, 5) == 0.0)


def test_market_score():
    print("\n[2] 大盘环境评分")
    scorer = StockScorer.__new__(StockScorer)

    scorer._index_ohlcv = None
    check("无数据 → 50", scorer._calc_market_score() == 50.0)

    closes = np.array([100] * 20 + [110], dtype=float)
    ohlcv = np.column_stack([closes, closes, closes, closes])
    scorer._index_ohlcv = ohlcv
    score = scorer._calc_market_score()
    check("价格>MA20 → >50", score > 50, f"got {score}")

    closes = np.array([100] * 20 + [90], dtype=float)
    ohlcv = np.column_stack([closes, closes, closes, closes])
    scorer._index_ohlcv = ohlcv
    score = scorer._calc_market_score()
    check("价格<MA20 → <50", score < 50, f"got {score}")


def test_sector_score():
    print("\n[3] 板块评分")
    scorer = StockScorer.__new__(StockScorer)
    scorer._sector_pct_cache = {}
    # 提供板块历史数据用于波动率标准化（默认 vol=2%）
    scorer._sector_hist_cache = {}

    score = scorer._calc_sector_score("000001", "银行", 11.0, 10.0)
    check("个股+10% 板块0% → 高分", score > 80, f"got {score}")

    scorer._sector_pct_cache = {"银行": 5.0}
    score = scorer._calc_sector_score("000001", "银行", 9.0, 10.0)
    check("个股-10% 板块+5% → 低分", score < 20, f"got {score}")

    scorer._sector_pct_cache = {"银行": 0.0}
    score = scorer._calc_sector_score("000001", "银行", 10.0, 10.0)
    check("持平 → ~50", 45 <= score <= 55, f"got {score}")

    # 波动率标准化：高波动板块（半导体 vol≈4%）同样涨幅得分更低
    # 用小涨幅（2%）测试，5% 涨幅在两个板块都会封顶
    scorer._sector_pct_cache = {}
    high_vol_hist = 100 + np.cumsum(np.random.RandomState(42).normal(0, 4, 30))
    scorer._sector_hist_cache = {"半导体": high_vol_hist}
    score_high_vol = scorer._calc_sector_score("000001", "半导体", 10.2, 10.0)
    scorer._sector_hist_cache = {}  # 默认 vol=2%
    score_low_vol = scorer._calc_sector_score("000001", "银行", 10.2, 10.0)
    check(f"高波动板块同样涨幅得分更低 (半导体={score_high_vol:.0f} 银行={score_low_vol:.0f})",
          score_high_vol < score_low_vol, f"半导体={score_high_vol}, 银行={score_low_vol}")


def test_volume_score():
    print("\n[4] 量能评分（连续函数）")
    scorer = StockScorer.__new__(StockScorer)

    # tanh 连续函数：score = 65 + 25 * tanh(vol_signal * 0.8)
    # vol_signal = (volume_ratio - 1.0) * price_pct
    import math
    # 放量上涨：vol_signal = (1.5-1)*10 = 5.0 → tanh(4)≈1.0 → 90
    score = scorer._calc_volume_score(11, 10, 1.5)
    check(f"放量上涨 → ~90 (got {score:.0f})", abs(score - 90) < 2, f"got {score}")

    # 放量下跌：vol_signal = (1.5-1)*(-10) = -5.0 → tanh(-4)≈-1.0 → 40
    score = scorer._calc_volume_score(9, 10, 1.5)
    check(f"放量下跌 → ~40 (got {score:.0f})", abs(score - 40) < 2, f"got {score}")

    # 缩量上涨：vol_signal = (0.5-1)*10 = -5.0 → tanh(-4)≈-1.0 → 40
    score = scorer._calc_volume_score(11, 10, 0.5)
    check(f"缩量上涨 → ~40 (got {score:.0f})", abs(score - 40) < 5, f"got {score}")

    # 缩量平盘：vol_signal = (0.5-1)*0 = 0 → tanh(0)=0 → 65
    score = scorer._calc_volume_score(10, 10, 0.5)
    check(f"缩量平盘 → 65 (got {score:.0f})", abs(score - 65) < 1, f"got {score}")

    # 温和放量平盘：vol_signal = (1.0-1)*0 = 0 → 65（中性）
    score = scorer._calc_volume_score(10, 10, 1.0)
    check(f"量比1.0平盘 → 65 (got {score:.0f})", abs(score - 65) < 1, f"got {score}")

    # 量比=0 → 50（边界保护）
    check("量比=0 → 50", scorer._calc_volume_score(10, 10, 0) == 50.0)


def test_position_score():
    print("\n[5] 位置评分")
    scorer = StockScorer.__new__(StockScorer)

    # 有波动的价格序列（BOLL 上下轨有间距）
    rng = np.random.RandomState(42)
    closes = 100 + np.cumsum(rng.normal(0, 1, 20))
    highs = closes * 1.01
    lows = closes * 0.99
    ma20 = np.mean(closes)
    price_near_mid = float(closes[-1])  # 大约在 BOLL 中轨附近

    score = scorer._calc_position_score(price_near_mid, highs, lows, closes, 110, 1.0)
    check(f"中轨附近 → 中等分 (got {score:.0f})", 50 <= score <= 90, f"got {score}")

    # 价格突破上轨 + 缩量 → 假突破
    std20 = np.std(closes)
    boll_upper = ma20 + 2 * std20
    score_above = scorer._calc_position_score(float(boll_upper) + 2, highs, lows, closes, 110, 0.5)
    check(f"突破上轨+缩量 → 低分 (got {score_above:.0f})", score_above < 55, f"got {score_above}")

    # 价格突破上轨 + 放量 → 真突破
    score_vol = scorer._calc_position_score(float(boll_upper) + 2, highs, lows, closes, 110, 2.5)
    check(f"突破上轨+放量 → 较高分 (got {score_vol:.0f})", score_vol >= 55, f"got {score_vol}")

    # 远低于下轨 → 应该有反弹加分
    boll_lower = ma20 - 2 * std20
    score_below = scorer._calc_position_score(float(boll_lower) - 1, highs, lows, closes, 110, 1.0)
    check(f"低于下轨 (got {score_below:.0f})", score_below >= 40, f"got {score_below}")

    # 数据不足
    short = np.array([100.0] * 5)
    score = scorer._calc_position_score(100, short, short, short, 110)
    check("数据不足 → 50", score == 50.0)


def test_formation_score():
    print("\n[6] 形态评分")
    scorer = StockScorer.__new__(StockScorer)

    # 基线分是 20（无信号时偏低），靠关键词加分
    score_empty = scorer._calc_formation_score([])
    check(f"空理由 → 20 (got {score_empty})", score_empty == 20.0, f"got {score_empty}")

    # "均线多头排列" +30（"放量"不再匹配，需"成交量显著放大"等）
    score_bull = scorer._calc_formation_score(["放量突破", "均线多头排列"])
    check(f"看涨理由 → 50 (got {score_bull:.0f})", score_bull == 50.0, f"got {score_bull}")

    # 无匹配关键词 → 20（基线）
    score_bear = scorer._calc_formation_score(["缩量阴线", "均线空头"])
    check(f"无匹配词 → 20 (got {score_bear:.0f})", score_bear == 20.0, f"got {score_bear}")

    # 混合信号："放量"不匹配, 涨停 -30 → 20-30=-10→0
    score_mix = scorer._calc_formation_score(["放量突破", "涨停板"])
    check(f"放量+涨停 → 0 (got {score_mix:.0f})", score_mix == 0.0, f"got {score_mix}")

    # 金叉去重：MACD金叉 不应同时匹配"金叉"
    score_macd = scorer._calc_formation_score(["MACD金叉"])
    check(f"MACD金叉 → 20+15=35 (got {score_macd:.0f})", score_macd == 35.0, f"got {score_macd}")

    # 新关键词匹配
    score_vol = scorer._calc_formation_score(["成交量显著放大"])
    check(f"成交量显著放大 → 20+20=40 (got {score_vol:.0f})", score_vol == 40.0, f"got {score_vol}")

    score_boll = scorer._calc_formation_score(["站上BOLL中轨"])
    check(f"站上BOLL中轨 → 20+15=35 (got {score_boll:.0f})", score_boll == 35.0, f"got {score_boll}")


# ===================================================================
# 2. 市场形态判定
# ===================================================================

def test_regime_detection():
    print("\n[7] 市场形态判定（层级化）")
    scorer = StockScorer.__new__(StockScorer)
    scorer._sector_pct_cache = {}

    breadth_good = {"advance_decline_ratio": 1.5, "breadth_score": 70.0}
    breadth_weak = {"advance_decline_ratio": 0.5, "breadth_score": 30.0}

    # 强上升：指数增长，趋势强度 > 0.012
    strong_up = 100 * np.exp(np.linspace(0, 0.25, 30))

    # 弱势：三条件取二（板块<-7 + 涨跌比<0.5 + 大盘<-3%）
    # 2/3 满足：板块-8% + 大盘-4% → weak
    regime = scorer._judge_sector_regime(-8.0, strong_up, breadth_weak, 0.0, index_pct=-4.0)
    check(f"弱势2/3条件 → weak (got {regime})", regime == "weak", f"got {regime}")

    # 只满足1个条件：板块-8% 但涨跌比1.0 且大盘0% → 不触发 weak
    regime = scorer._judge_sector_regime(-8.0, strong_up, breadth_good, 0.0, index_pct=0.0)
    check(f"弱势仅1/3条件 → 非weak (got {regime})", regime != "weak", f"got {regime}")

    # 边界：ad_ratio=0.5 不满足 <0.5，只有板块触发 → 1/3 → 非weak
    breadth_boundary = {"advance_decline_ratio": 0.5, "breadth_score": 30.0}
    regime = scorer._judge_sector_regime(-8.0, strong_up, breadth_boundary, 0.0, index_pct=0.0)
    check(f"ad_ratio=0.5边界 → 非weak (got {regime})", regime != "weak", f"got {regime}")

    # 层级化：上升趋势默认 → strong_stable_up（旧 strong_trend）
    regime = scorer._judge_sector_regime(5.0, strong_up, breadth_good, 0.001)
    check(f"上升趋势默认 → strong_stable_up (got {regime})",
          regime == "strong_stable_up", f"got {regime}")

    # 层级化：上升趋势 + 正动量加速度 → accelerating
    breadth_narrow = {"advance_decline_ratio": 0.9, "breadth_score": 40.0}
    t = np.linspace(0, 1, 30)
    accel_data = (100 + 12 * t**3).astype(float)
    regime = scorer._judge_sector_regime(5.0, accel_data, breadth_narrow, 0.008)
    check(f"上升趋势+正动量 → accelerating_* (got {regime})",
          regime.startswith("accelerating"), f"got {regime}")

    # 层级化：上升趋势 + 负动量加速度 → decelerating
    regime = scorer._judge_sector_regime(5.0, strong_up, breadth_good, -0.008)
    check(f"上升趋势+负动量 → decelerating (got {regime})",
          regime == "decelerating", f"got {regime}")

    # 层级化：下降趋势 + 价格<MA20 + 趋势强 → bearish
    strong_down = 100 * np.exp(np.linspace(0, -0.25, 30))
    regime = scorer._judge_sector_regime(-2.0, strong_down, breadth_good, -0.001)
    check(f"下降趋势 → bearish (got {regime})", regime == "bearish", f"got {regime}")

    # 层级化：下降趋势 + 正动量（下跌减速/见底） → decelerating
    regime = scorer._judge_sector_regime(-2.0, strong_down, breadth_good, 0.008)
    check(f"下降趋势+正动量 → decelerating (got {regime})",
          regime == "decelerating", f"got {regime}")

    # 震荡
    rng = np.random.RandomState(42)
    range_bd = 100 + rng.normal(0, 0.3, 30)
    regime = scorer._judge_sector_regime(0.5, range_bd, breadth_good, 0.001)
    check(f"震荡条件 → range_bound (got {regime})", regime == "range_bound", f"got {regime}")

    # 数据不足
    short = np.array([100.0] * 5)
    regime = scorer._judge_sector_regime(0.0, short, None, 0.0)
    check("数据不足 → range_bound", regime == "range_bound", f"got {regime}")


# ===================================================================
# 3. 加速初期/末期判定
# ===================================================================

def test_acceleration_stage():
    print("\n[8] 加速初期/末期判定")
    scorer = StockScorer.__new__(StockScorer)

    # 初期：低乖离 + 低 RSI + 短天数
    early = 100 * np.exp(np.linspace(0, 0.03, 30))
    stage = scorer._calc_acceleration_stage(early, 0.008)
    check(f"低乖离+低RSI+短天数 → early (got {stage})", stage == "early", f"got {stage}")

    # 末期：高乖离 + 高 RSI + 长天数（后段快速拉升）
    late = np.concatenate([
        np.linspace(100, 102, 10),
        np.linspace(102, 135, 20),
    ]).astype(float)
    stage = scorer._calc_acceleration_stage(late, 0.008)
    check(f"高乖离+高RSI+长天数 → late (got {stage})", stage == "late", f"got {stage}")

    # 边界：中等乖离
    mid = np.concatenate([
        np.linspace(100, 103, 15),
        np.linspace(103, 112, 15),
    ]).astype(float)
    stage = scorer._calc_acceleration_stage(mid, 0.008)
    check(f"边界情况 → early 或 late (got {stage})", stage in ("early", "late"), f"got {stage}")

    # RSI 动量方向：RSI 仍在上升 → 初期（即使价格已涨不少）
    # 构造：先震荡（RSI ~50），再连涨（RSI 从 50 升到 80+）
    mixed = np.concatenate([
        100 + np.array([0, -1, 1, -0.5, 0.5, -1, 1, 0, -0.5, 0.5] * 2, dtype=float),  # 20天震荡
        np.linspace(101, 110, 10),  # 10天连涨
    ]).astype(float)
    stage = scorer._calc_acceleration_stage(mixed, 0.008)
    check(f"RSI仍在上升 → early (got {stage})", stage == "early", f"got {stage}")

    # 纯匀速上涨：RSI=100恒定 + 收益率递减（无加速）→ early（正确：恒速上涨不是后期）
    pure_up = np.linspace(100, 115, 30).astype(float)
    stage = scorer._calc_acceleration_stage(pure_up, 0.008)
    check(f"匀速上涨无加速 → early (got {stage})", stage == "early", f"got {stage}")

    # 加速上涨后 RSI 背离：前半段慢涨建立 RSI 基准，后半段加速但 RSI 已到顶
    # 先慢涨（RSI 从 50 升到 70），再快涨（RSI 恒定 100，动量方向=走平）
    accel_then_flat = np.concatenate([
        np.linspace(100, 105, 20),  # 慢涨，RSI 上升
        np.linspace(105, 125, 10),  # 快涨，RSI 已到 100 走平
    ]).astype(float)
    stage = scorer._calc_acceleration_stage(accel_then_flat, 0.008)
    check(f"加速后RSI走平 → late (got {stage})", stage == "late", f"got {stage}")


# ===================================================================
# 4. 权重归一化验证
# ===================================================================

def test_weight_sums():
    print("\n[9] 权重归一化（所有形态加总=1.0）")

    preset_weights = {
        "base": _BASE_WEIGHTS,
        "high_vol": _HIGH_VOL_WEIGHTS,
        "strong_trend_up": _STRONG_TREND_UP_WEIGHTS,
        "strong_trend_down": _STRONG_TREND_DOWN_WEIGHTS,
        "calm": _CALM_WEIGHTS,
        "crisis": _CRISIS_WEIGHTS,
        "bearish": _BEARISH_WEIGHTS,
    }

    for name, w in preset_weights.items():
        total = sum(w.values())
        check(f"{name} 加总={total:.2f}", abs(total - 1.0) < 0.001, f"sum={total}")

    # 动态生成的权重（从 _get_dynamic_weights 的各分支）
    dynamic = {
        "accelerating_early": {"rr_score": 0.20, "market_score": 0.20, "sector_score": 0.25,
                               "volume_score": 0.15, "position_score": 0.10, "formation_score": 0.10},
        "accelerating_late":  {"rr_score": 0.35, "market_score": 0.15, "sector_score": 0.10,
                               "volume_score": 0.15, "position_score": 0.15, "formation_score": 0.10},
        "decelerating":       {"rr_score": 0.35, "market_score": 0.15, "sector_score": 0.10,
                               "volume_score": 0.15, "position_score": 0.20, "formation_score": 0.05},
        "weak":               {"rr_score": 0.35, "market_score": 0.20, "sector_score": 0.05,
                               "volume_score": 0.15, "position_score": 0.15, "formation_score": 0.10},
    }

    for name, w in dynamic.items():
        total = sum(w.values())
        check(f"{name} 加总={total:.2f}", abs(total - 1.0) < 0.001, f"sum={total}")


# ===================================================================
# 5. 最终 composite 分极端场景
# ===================================================================

def test_composite_extremes():
    print("\n[10] Composite 分极端场景")
    scorer = StockScorer.__new__(StockScorer)
    scorer._sector_pct_cache = {}
    scorer._index_ohlcv = None

    # 构造有波动的数据
    rng = np.random.RandomState(42)
    closes = 100 + np.cumsum(rng.normal(0, 0.5, 20))
    highs = closes * 1.02
    lows = closes * 0.98

    # 全维度高分
    rr = scorer._calc_rr_score(10, 20, 9)       # RR=11 → 100
    vol = scorer._calc_volume_score(11, 10, 2)   # 放量上涨 → 90
    form = scorer._calc_formation_score(["放量突破", "均线多头", "MACD金叉"])
    pos = scorer._calc_position_score(float(closes[-1]) - 5, highs, lows, closes, 110, 1.0)

    w = _BASE_WEIGHTS
    composite = (w["rr_score"] * rr + w["market_score"] * 50 + w["sector_score"] * 80 +
                 w["volume_score"] * vol + w["position_score"] * pos + w["formation_score"] * form)
    check(f"高分场景 composite={composite:.1f}", composite > 70, f"too low: {composite}")

    # 全维度低分
    rr_low = scorer._calc_rr_score(10, 9, 11)     # tp1<price → 0
    vol_low = scorer._calc_volume_score(9, 10, 2)  # 放量下跌 → ~41
    form_low = scorer._calc_formation_score(["均线空头", "MACD死叉"])  # 基线30
    pos_low = scorer._calc_position_score(float(closes[-1]) + 5, highs, lows, closes, 110, 0.3)

    composite_low = (w["rr_score"] * rr_low + w["market_score"] * 30 + w["sector_score"] * 10 +
                     w["volume_score"] * vol_low + w["position_score"] * pos_low + w["formation_score"] * form_low)
    check(f"低分场景 composite={composite_low:.1f}", composite_low < 40, f"too high: {composite_low}")

    diff = composite - composite_low
    check(f"高低分差={diff:.1f} > 30", diff > 30, f"差距不够: {diff}")


# ===================================================================
# 6. RSI 计算
# ===================================================================

def test_rsi():
    print("\n[11] RSI 计算")
    scorer = StockScorer.__new__(StockScorer)

    up = np.linspace(100, 130, 20).astype(float)
    rsi = scorer._calc_rsi(up)
    check(f"连续上涨 RSI={rsi:.0f} → >80", rsi > 80, f"got {rsi}")

    down = np.linspace(130, 100, 20).astype(float)
    rsi = scorer._calc_rsi(down)
    check(f"连续下跌 RSI={rsi:.0f} → <20", rsi < 20, f"got {rsi}")

    flat = np.array([100 + (i % 2) * 2 for i in range(20)], dtype=float)
    rsi = scorer._calc_rsi(flat)
    check(f"震荡 RSI={rsi:.0f} → 40-60", 35 <= rsi <= 65, f"got {rsi}")

    rsi = scorer._calc_rsi(np.array([100.0] * 5))
    check("数据不足 → 50", rsi == 50.0)


# ===================================================================
# 7. 动量加速度
# ===================================================================

def test_momentum_acceleration():
    print("\n[12] 动量加速度")
    scorer = StockScorer.__new__(StockScorer)

    # 加速上升：后 3 天涨幅 > 前 3 天
    accel = np.array([100, 101, 102, 104, 107, 111, 116, 122], dtype=float)
    acc = scorer._calc_momentum_acceleration(accel)
    check(f"加速上升 acc={acc:.4f} → >0", acc > 0, f"got {acc}")

    # 减速上升：后 3 天涨幅 < 前 3 天
    decel = np.array([100, 104, 107, 109, 110, 110.5, 110.8, 111], dtype=float)
    acc = scorer._calc_momentum_acceleration(decel)
    check(f"减速上升 acc={acc:.4f} → <0", acc < 0, f"got {acc}")

    acc = scorer._calc_momentum_acceleration(np.array([100.0] * 3))
    check("数据不足 → 0", acc == 0.0)


# ===================================================================
# 8. 波动率聚类
# ===================================================================

def test_volatility_regime():
    print("\n[13] 波动率聚类")
    scorer = StockScorer.__new__(StockScorer)

    low_vol = np.linspace(100, 101, 30).astype(float)
    result = scorer._calc_volatility_regime(low_vol)
    check(f"低波动 regime={result['vol_regime']}", result["vol_regime"] in ("low", "normal"),
          f"got {result}")

    rng = np.random.RandomState(42)
    high_vol = 100 + np.cumsum(rng.normal(0, 3, 30))
    result = scorer._calc_volatility_regime(high_vol)
    check(f"高波动 regime={result['vol_regime']}", result["current_vol"] > 0,
          f"got {result}")

    result = scorer._calc_volatility_regime(np.array([100.0] * 5))
    check("数据不足 → normal", result["vol_regime"] == "normal")


# ===================================================================
# 9. lerp 权重插值
# ===================================================================

def test_lerp_weights():
    print("\n[14] 权重线性插值")
    scorer = StockScorer.__new__(StockScorer)

    w1 = {"a": 0.5, "b": 0.5}
    w2 = {"a": 0.0, "b": 1.0}

    result = scorer._lerp_weights(w1, w2, 0.0)
    check("t=0 → w1", abs(result["a"] - 0.5) < 0.001)

    result = scorer._lerp_weights(w1, w2, 1.0)
    check("t=1 → w2", abs(result["a"] - 0.0) < 0.001)

    result = scorer._lerp_weights(w1, w2, 0.5)
    check("t=0.5 → 中间", abs(result["a"] - 0.25) < 0.001)

    total = sum(result.values())
    check(f"插值后归一化 sum={total:.3f}", abs(total - 1.0) < 0.001)


# ===================================================================
# 10. 形态判定条件优先级
# ===================================================================

def test_regime_priority():
    print("\n[15] 形态判定优先级（层级化）")
    scorer = StockScorer.__new__(StockScorer)
    scorer._sector_pct_cache = {}

    breadth_good = {"advance_decline_ratio": 1.5, "breadth_score": 70.0}

    # 弱势优先于一切（三条件取二：板块-8% + 大盘-4% → 直接返回 weak）
    strong_up = 100 * np.exp(np.linspace(0, 0.3, 30))
    regime = scorer._judge_sector_regime(-8.0, strong_up, {"advance_decline_ratio": 0.8, "breadth_score": 50.0}, 0.0, index_pct=-4.0)
    check(f"弱势优先于一切 (got {regime})", regime == "weak", f"got {regime}")

    # 层级化：上升趋势默认 → strong_stable_up（不再是 strong_trend）
    regime = scorer._judge_sector_regime(5.0, strong_up, breadth_good, 0.001)
    check(f"上升趋势默认 → strong_stable_up (got {regime})",
          regime == "strong_stable_up", f"got {regime}")

    # 层级化：上升趋势 + 加速动量 → accelerating_*（现在能触发了！）
    t = np.linspace(0, 1, 30)
    accel_data = (100 + 12 * t**3).astype(float)
    regime = scorer._judge_sector_regime(5.0, accel_data, {"advance_decline_ratio": 0.9, "breadth_score": 40.0}, 0.008)
    check(f"上升趋势+加速动量 → accelerating_* (got {regime})",
          regime.startswith("accelerating"), f"got {regime}")

    # 层级化：下降趋势 → bearish
    strong_down = 100 * np.exp(np.linspace(0, -0.3, 30))
    regime = scorer._judge_sector_regime(-2.0, strong_down, breadth_good, -0.001)
    check(f"下降趋势 → bearish (got {regime})", regime == "bearish", f"got {regime}")


# ===================================================================
# 11. 边界值：极端输入
# ===================================================================

def test_extreme_inputs():
    print("\n[16] 极端输入")
    scorer = StockScorer.__new__(StockScorer)

    # RR: 价格=止损价 → 0
    check("price=stop → 0", scorer._calc_rr_score(10, 15, 10) == 0.0)

    # Volume: 负量比
    check("负量比 → 50", scorer._calc_volume_score(10, 10, -1) == 50.0)

    # RSI: 单点数据
    rsi = scorer._calc_rsi(np.array([100.0]))
    check("单点RSI → 50", rsi == 50.0)

    # 动量: 空数组
    acc = scorer._calc_momentum_acceleration(np.array([]))
    check("空数组动量 → 0", acc == 0.0)

    # 波动率: 空数组
    result = scorer._calc_volatility_regime(np.array([]))
    check("空数组波动率 → normal", result["vol_regime"] == "normal")


def test_long_term_vol_percentile():
    print("\n[17] 长期波动率百分位")
    scorer = StockScorer.__new__(StockScorer)

    # 低波动序列：百分位应该低
    low_vol = np.linspace(100, 101, 50).astype(float)
    result = scorer._calc_long_term_vol_percentile(low_vol)
    check(f"低波动 → regime={result['vol_regime']}", result["vol_regime"] in ("low", "normal"),
          f"got {result}")

    # 高波动序列：百分位应该高
    rng = np.random.RandomState(42)
    high_vol = 100 + np.cumsum(rng.normal(0, 3, 50))
    result = scorer._calc_long_term_vol_percentile(high_vol)
    check(f"高波动 → current_vol>0", result["current_vol"] > 0, f"got {result}")

    # 数据不足 → 默认值
    result = scorer._calc_long_term_vol_percentile(np.array([100.0] * 5))
    check("数据不足 → normal", result["vol_regime"] == "normal")

    # 40+ 数据点比 15 数据点百分位更稳定
    # 先构造有波动的序列，最后突增
    base = 100 + np.cumsum(np.random.RandomState(99).normal(0, 0.5, 45))
    spike = np.append(base, [base[-1] * 1.1, base[-1] * 1.2])
    result_long = scorer._calc_long_term_vol_percentile(spike)
    result_short = scorer._calc_volatility_regime(spike)
    check(f"长期百分位 (vol={result_long['current_vol']:.1f}%, p={result_long['vol_percentile']:.0f})",
          result_long["current_vol"] > 0, f"got {result_long}")


def test_hierarchical_regime_accelerating():
    print("\n[18] 层级化 regime — accelerating_early 现在可触发")
    scorer = StockScorer.__new__(StockScorer)
    scorer._sector_pct_cache = {}

    # 构造上升趋势 + 有回调的数据（避免 RSI=100 恒定）
    # 先震荡建立 RSI 基准，再连涨制造加速
    mixed = np.concatenate([
        100 + np.array([0, -1, 1, -0.5, 0.5, -1, 1, 0, -0.5, 0.5] * 2, dtype=float),
        np.linspace(101, 115, 10),
    ]).astype(float)

    # 正动量加速度 → 在上升趋势内应判为 accelerating
    acc = scorer._calc_momentum_acceleration(mixed)
    regime = scorer._judge_sector_regime(3.0, mixed, {"advance_decline_ratio": 1.5, "breadth_score": 70.0}, max(acc, 0.005))
    check(f"上升趋势+有回调+正动量 → accelerating_* (got {regime})",
          regime.startswith("accelerating"), f"got {regime}")

    # 下降趋势 + 正动量 → decelerating（下跌减速/见底）
    # 需要足够强的下跌趋势（trend_strength > 0.008）
    down_data = 100 * np.exp(np.linspace(0, -0.3, 30))
    regime = scorer._judge_sector_regime(-3.0, down_data, {"advance_decline_ratio": 0.8, "breadth_score": 40.0}, 0.005)
    check(f"下降趋势+正动量 → decelerating (got {regime})",
          regime == "decelerating", f"got {regime}")


# ===================================================================
# Main
# ===================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("StockScorer 评分逻辑边界测试")
    print("=" * 60)

    test_rr_score()
    test_market_score()
    test_sector_score()
    test_volume_score()
    test_position_score()
    test_formation_score()
    test_regime_detection()
    test_acceleration_stage()
    test_weight_sums()
    test_composite_extremes()
    test_rsi()
    test_momentum_acceleration()
    test_volatility_regime()
    test_lerp_weights()
    test_regime_priority()
    test_extreme_inputs()
    test_long_term_vol_percentile()
    test_hierarchical_regime_accelerating()

    print("\n" + "=" * 60)
    print(f"结果: {PASS} 通过, {FAIL} 失败")
    print("=" * 60)

    sys.exit(1 if FAIL > 0 else 0)
