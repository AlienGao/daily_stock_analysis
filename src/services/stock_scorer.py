# -*- coding: utf-8 -*-
"""多维技术评分器（StockScorer）。

对候选股计算 6 个维度的 0-100 技术评分，最终输出加权综合分 tech_score，
用于盘中扫描结果排序。板块级别动态权重，不依赖大盘统一判定。

评分维度（基准权重）：
  1. RR分（赔率）       权重 30%  — RR = (TP1 - price) / (price - stop_loss)
  2. 大盘分            权重 20%  — 上证指数 MA20/MA60 偏离度
  3. 板块分            权重 15%  — 个股在板块内的相对强弱（波动率标准化）
  4. 量能分            权重 15%  — 量比 × 价格方向连续函数
  5. 位置分            权重 10%  — BOLL 通道位置、ATR 标准化天花板距离
  6. 形态分            权重 10%  — 复用 reasons 关键词匹配

动态权重（12 种市场形态）：
  - 上升趋势内加速（momentum_acc > 0.003）：初期追涨 vs 末期防守，权重截然不同
  - 板块弱势（三条件取二：板块跌>7%、涨跌比<0.5、大盘跌>3%）：降板块权重
  - 大盘危机（近5日跌幅 > 10% 或波动率极端）：统一切换到 Crisis 权重
  - 权重切换使用线性插值（软切换），避免硬跳
"""

from __future__ import annotations

import logging
import math
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from src.storage import DatabaseManager

logger = logging.getLogger(__name__)


# ===================================================================
# StockScorer 配置
# ===================================================================


@dataclass
class StockScorerConfig:
    """StockScorer 维度权重配置（可通过环境变量或 DiscoveryConfig 注入）。"""
    # 六个维度的默认权重（归一化，相加≈1.0）
    weight_rr: float = 0.30
    weight_market: float = 0.20
    weight_sector: float = 0.15
    weight_volume: float = 0.15
    weight_position: float = 0.10
    weight_formation: float = 0.10

    def to_weights_dict(self) -> Dict[str, float]:
        return {
            "rr_score": self.weight_rr,
            "market_score": self.weight_market,
            "sector_score": self.weight_sector,
            "volume_score": self.weight_volume,
            "position_score": self.weight_position,
            "formation_score": self.weight_formation,
        }


# ===================================================================
# 权重预设
# ===================================================================

_BASE_WEIGHTS = {
    "rr_score": 0.30,
    "market_score": 0.20,
    "sector_score": 0.15,
    "volume_score": 0.15,
    "position_score": 0.10,
    "formation_score": 0.10,
}

_HIGH_VOL_WEIGHTS = {
    "rr_score": 0.40,
    "market_score": 0.15,
    "sector_score": 0.10,
    "volume_score": 0.15,
    "position_score": 0.10,
    "formation_score": 0.10,
}

_STRONG_TREND_UP_WEIGHTS = {
    "rr_score": 0.25,
    "market_score": 0.20,
    "sector_score": 0.25,
    "volume_score": 0.10,
    "position_score": 0.10,
    "formation_score": 0.10,
}

_STRONG_TREND_DOWN_WEIGHTS = {
    "rr_score": 0.30,
    "market_score": 0.20,
    "sector_score": 0.10,
    "volume_score": 0.15,
    "position_score": 0.15,
    "formation_score": 0.10,
}

_CALM_WEIGHTS = {
    "rr_score": 0.30,
    "market_score": 0.10,
    "sector_score": 0.15,
    "volume_score": 0.20,
    "position_score": 0.15,
    "formation_score": 0.10,
}

_CRISIS_WEIGHTS = {
    "rr_score": 0.40,
    "market_score": 0.25,
    "sector_score": 0.05,
    "volume_score": 0.15,
    "position_score": 0.15,
    "formation_score": 0.00,
}

_BEARISH_WEIGHTS = {
    "rr_score": 0.35,
    "market_score": 0.25,
    "sector_score": 0.05,
    "volume_score": 0.15,
    "position_score": 0.15,
    "formation_score": 0.05,
}

# ===================================================================
# 结果类型
# ===================================================================


@dataclass
class TechScoreResult:
    """6 维度评分 + 加权总分。"""
    rr_score: float       # 赔率评分 0-100
    market_score: float   # 大盘环境评分 0-100
    sector_score: float   # 板块相对强弱评分 0-100
    volume_score: float   # 成交量质量评分 0-100
    position_score: float # 相对位置评分 0-100
    formation_score: float  # 形态确认度评分 0-100
    composite: float      # 加权总分 0-100
    weights: Dict[str, float] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.weights is None:
            self.weights = {}

    def to_dict(self) -> Dict[str, float]:
        return {
            "tech_score": self.composite,
            "rr_score": self.rr_score,
            "market_score": self.market_score,
            "sector_score": self.sector_score,
            "volume_score": self.volume_score,
            "position_score": self.position_score,
            "formation_score": self.formation_score,
        }


# ===================================================================
# StockScorer
# ===================================================================


class StockScorer:
    """多维技术评分器。"""

    def __init__(self, config: StockScorerConfig | None = None):
        self._db = DatabaseManager.get_instance()
        # 板块当日涨跌幅缓存（每个扫描轮次更新一次）
        self._sector_pct_cache: Dict[str, float] = {}
        # 大盘 OHLCV 缓存（由 preload_index_ohlcv 填充）
        self._index_ohlcv: Optional[np.ndarray] = None
        # 板块历史收盘价缓存（避免重复查 DB）
        self._sector_hist_cache: Dict[str, Optional[np.ndarray]] = {}
        # 市场宽度缓存（同轮次只算一次）
        self._breadth_cache: Optional[Dict[str, float]] = None
        # 基础权重（来自 config 或使用 class-level 默认）
        self._base_weights: Dict[str, float] = (
            config.to_weights_dict() if config else _BASE_WEIGHTS.copy()
        )

    # =================================================================
    # 公共接口
    # =================================================================

    def score(
        self,
        stock_code: str,
        sector: str,
        price: float,
        pre_close: float,
        tp1: float,
        tp2: float,
        stop_loss: float,
        reasons: List[str],
        ohlcv: Tuple[np.ndarray, np.ndarray, np.ndarray],
        volume_ratio: float,
    ) -> TechScoreResult:
        """
        计算单只股票的多维技术评分。

        Args:
            stock_code:       股票代码（无后缀，如 "002340"）
            sector:           所属板块（同花顺行业名）
            price:            当前/发现价格
            pre_close:        昨收价
            tp1/tp2/stop_loss: 止盈止损价位
            reasons:          推荐理由列表
            ohlcv:            (highs, lows, closes) numpy 数组
            volume_ratio:     量比（今日成交量 / 5日均量）
        """
        highs, lows, closes = ohlcv

        # 1. 各维度评分
        atr = self._calc_atr(highs, lows, closes)
        rr = self._calc_rr_score(price, tp1, stop_loss, atr)
        market = self._calc_market_score()
        sector_s = self._calc_sector_score(stock_code, sector, price, pre_close)
        vol = self._calc_volume_score(price, pre_close, volume_ratio)
        pos = self._calc_position_score(price, highs, lows, closes, tp1, volume_ratio)
        form = self._calc_formation_score(reasons)

        # 2. 动态权重
        weights = self._get_dynamic_weights(sector, sector_s)

        # 3. 加权求和
        total = (
            weights["rr_score"] * rr
            + weights["market_score"] * market
            + weights["sector_score"] * sector_s
            + weights["volume_score"] * vol
            + weights["position_score"] * pos
            + weights["formation_score"] * form
        )

        return TechScoreResult(
            rr_score=rr,
            market_score=market,
            sector_score=sector_s,
            volume_score=vol,
            position_score=pos,
            formation_score=form,
            composite=round(total, 2),
            weights=weights,
        )

    def preload_sector_pct(self, sector_pct_map: Dict[str, float]) -> None:
        """预加载板块当日涨跌幅，避免多次网络请求。"""
        self._sector_pct_cache = sector_pct_map

    def preload_index_ohlcv(self, index_ohlcv: np.ndarray) -> None:
        """预加载大盘 OHLCV 数组，格式 (N, 4+): [open, high, low, close, ...]。

        Tushare index_daily 返回列: ts_code, trade_date, close, open, high, low, ...
        传入前需确保列顺序对齐到 OHLC 格式。
        """
        self._index_ohlcv = index_ohlcv

    # =================================================================
    # 维度评分计算
    # =================================================================

    def _calc_rr_score(self, price: float, tp1: float, stop_loss: float, atr: float = 0.0) -> float:
        """赔率评分：RR = (TP1 - price) / max(price - stop_loss, 0.5 × ATR)，RR=2.0 时满分。

        分母至少取 0.5 ATR，避免止损过近导致 RR 虚高。
        - price <= stop_loss：已跌破止损，0 分
        - tp1 略低于 price（<0.5%）：数据精度问题，给 10 分而非 0
        - tp1 明显低于 price（>=0.5%）：无盈利空间，0 分
        """
        if price <= stop_loss:
            return 0.0
        if tp1 <= price:
            gap_pct = (price - tp1) / price
            return 10.0 if gap_pct < 0.005 else 0.0
        risk_dist = max(price - stop_loss, 0.5 * atr) if atr > 0 else (price - stop_loss)
        rr = (tp1 - price) / risk_dist
        return min(rr / 4.0, 1.0) * 100

    @staticmethod
    def _calc_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
        """计算 ATR(14)，返回 0 表示数据不足。"""
        if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
            return 0.0
        h = highs[-(period + 1):]
        l = lows[-(period + 1):]
        c = closes[-(period + 1):]
        tr = np.maximum(h - l, np.maximum(
            np.abs(h - np.roll(c, 1)), np.abs(l - np.roll(c, 1))))
        tr[0] = float(h[0] - l[0])
        return float(np.mean(tr[-period:]))

    def _calc_market_score(self) -> float:
        """大盘环境评分：基于上证指数 MA20/MA60 偏离度，输出 0-100。

        偏离度 = (price - MA) / MA，正常范围 ±10%。
        映射：偏离 -10% → 10 分，0% → 50 分，+10% → 90 分。
        """
        ohlcv = getattr(self, "_index_ohlcv", None)
        if ohlcv is None or len(ohlcv) < 20:
            return 50.0  # 无数据返回中间值
        closes = ohlcv[:, 3] if ohlcv.ndim == 2 else ohlcv
        if isinstance(closes, np.ndarray) and closes.ndim == 1:
            price = float(closes[-1])
            ma20 = float(np.mean(closes[-20:]))
            ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else ma20

            # 偏离度 → 评分：50 为中性，每 1% 偏离对应 4 分
            dev20 = (price - ma20) / ma20 if ma20 > 0 else 0.0
            dev60 = (price - ma60) / ma60 if ma60 > 0 else 0.0
            score20 = 50 + dev20 * 400  # +5% → 70, -5% → 30
            score60 = 50 + dev60 * 400
            score = 0.6 * score20 + 0.4 * score60
            return min(max(score, 0), 100)
        return 50.0

    def _calc_sector_vol(self, sector: str) -> float:
        """板块 20 日收益率标准差（日度），用于标准化板块评分。"""
        hist = self._get_sector_hist_closes(sector)
        if hist is None or len(hist) < 5:
            return 0.02  # 默认 2% 日波动率
        returns = np.diff(hist) / hist[:-1]
        return float(np.std(returns)) if len(returns) > 1 else 0.02

    def _calc_sector_score(self, stock_code: str, sector: str, price: float, pre_close: float) -> float:
        """板块评分：标准化绝对涨幅（40%）+ 相对强弱（60%）。

        绝对分用板块波动率标准化，消除板块间波动率差异。
        相对分：跑赢板块5%→100 跑输5%→0 持平→50
        """
        sector_pct = self._sector_pct_cache.get(sector, 0.0)
        stock_pct = 0.0
        if pre_close > 0 and price > 0:
            stock_pct = (price - pre_close) / pre_close * 100

        # 标准化绝对分：用板块波动率缩放
        sector_vol = self._calc_sector_vol(sector) * 100  # 转为百分比
        vol_scale = max(sector_vol, 1.0)  # 最低 1% 防除零
        abs_score = min(max((stock_pct / vol_scale + 1) / 2 * 100, 0), 100)

        # 相对强弱分：跑输5%→0, 持平→50, 跑赢5%→100
        relative = stock_pct - sector_pct
        rel_score = min(max(relative * 10 + 50, 0), 100)

        return abs_score * 0.4 + rel_score * 0.6

    def _calc_volume_score(
        self, price: float, pre_close: float, volume_ratio: float
    ) -> float:
        """成交量质量评分：量比 × 价格方向的连续函数。"""
        if pre_close <= 0 or volume_ratio <= 0:
            return 50.0

        price_pct = (price - pre_close) / pre_close * 100

        # 量价信号：量比偏离 × 价格方向
        vol_signal = (volume_ratio - 1.0) * price_pct

        # 连续映射：tanh 压缩到 [-1, 1]，再映射到 [40, 90]
        # k=0.8 控制敏感度，vol_signal≈3 时饱和
        score = 65.0 + 25.0 * math.tanh(vol_signal * 0.8)

        return min(max(score, 0), 100)

    def _calc_position_score(
        self,
        price: float,
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        tp1: float,
        vol_ratio: float = 1.0,
    ) -> float:
        """相对位置评分：ATR 标准化的天花板距离 + 放量确认。

        核心思想：
        - 天花板距离 = (BOLL上轨 - price) / ATR，用该股自身波动率做尺度
        - 突破上轨本身不一定是坏事，放量突破往往是主升浪
        - 缩量突破上轨 = 假突破/衰竭，重扣分
        """
        if len(closes) < 20:
            return 50.0

        ma20 = float(np.mean(closes[-20:]))
        std20 = float(np.std(closes[-20:]))
        boll_upper = ma20 + 2 * std20
        boll_lower = ma20 - 2 * std20

        if boll_upper <= boll_lower:
            return 50.0

        # ATR(14)
        if len(highs) >= 15 and len(lows) >= 15 and len(closes) >= 15:
            h = highs[-15:]
            l = lows[-15:]
            c = closes[-15:]
            tr = np.maximum(h - l, np.maximum(
                np.abs(h - np.roll(c, 1)), np.abs(l - np.roll(c, 1))))
            tr[0] = float(h[0] - l[0])
            atr = float(np.mean(tr[-14:])) if len(tr) >= 14 else std20
        else:
            atr = std20

        if atr <= 0:
            return 50.0

        # 天花板距离（ATR 标准化）
        distance = (boll_upper - price) / atr

        if distance >= 2.0:
            base = 85          # 空间充裕
        elif distance >= 1.0:
            base = 75          # 合理空间
        elif distance >= 0:
            base = 60          # 逼近上轨
        else:
            # 突破上轨 → 量能决定真假突破
            if vol_ratio > 2.0:
                base = 70      # 放量突破 → 真突破，主升浪
            elif vol_ratio > 1.0:
                base = 55      # 普通突破
            else:
                base = 35      # 缩量突破 → 假突破/衰竭

        # 下轨反弹加分
        if price < boll_lower * 1.02 and price > ma20:
            base = min(base + 10, 100)

        # 天花板逼近惩罚：distance < 0.5 ATR → 几乎贴在BOLL上轨
        if 0 <= distance < 0.5:
            base -= 10

        # 超买扣分：价格 >= TP1 且无放量确认时扣分
        # 放量突破 TP1 可能是主升浪，不扣
        if price >= tp1 and vol_ratio <= 1.5:
            base -= 15

        return min(max(base, 0), 100)

    def _calc_formation_score(self, reasons: List[str]) -> float:
        """形态确认度评分：复用 reasons 关键词匹配。

        关键词与 engine.py Phase 5 的 lite_reasons 生成逻辑耦合，
        修改时需同步。
        """
        score = 20.0  # 基线分：无形态信号时偏低，有信号时快速拉升
        for r in reasons:
            # 趋势类（互斥，取最强）
            if "均线多头排列" in r:
                score += 30
            elif "均线粘合" in r:
                score += 10
            # 均线回踩
            if "回踩MA5均线" in r:
                score += 20
            # 支撑/压力
            if "站上BOLL中轨" in r:
                score += 15
            elif "BOLL中轨支撑" in r:
                score += 15
            # 量价（互斥）
            if "成交量显著放大" in r:
                score += 20
            elif "成交量放大" in r:
                score += 12
            elif "量价齐升" in r:
                score += 15
            # 动量（互斥）
            if "MACD金叉" in r:
                score += 15
            elif "KDJ金叉" in r:
                score += 10
            elif "金叉" in r:
                score += 15
            # RSI
            if "RSI低位回升" in r:
                score += 10
            elif "KDJ" in r and "超卖" in r:
                score += 10
            # 负面信号
            if "涨停" in r:
                score -= 30  # 已涨停无空间
        return min(max(score, 0), 100)

    # =================================================================
    # 动态权重
    # =================================================================

    def _get_dynamic_weights(self, sector: str, sector_score: float) -> Dict[str, float]:
        """根据板块状态返回动态权重。"""
        # 大盘危机检测
        if self._is_crisis():
            logger.info("[StockScorer] 大盘危机模式")
            return _CRISIS_WEIGHTS.copy()

        # 板块状态判定
        sector_pct = self._sector_pct_cache.get(sector, 0.0)
        hist_closes = self._get_sector_hist_closes(sector)

        breadth = self._calc_market_breadth()
        momentum_acc = self._calc_momentum_acceleration(hist_closes)
        vol_info = self._calc_long_term_vol_percentile(hist_closes)
        index_pct = self._calc_index_pct()

        regime = self._judge_sector_regime(sector_pct, hist_closes, breadth, momentum_acc, index_pct, vol_info)

        logger.debug(
            "[StockScorer] 板块=%s 形态=%s 涨跌比=%.2f 宽度分=%.0f 动量加速度=%.4f 波动率=%.1f%%(%.0f%%)",
            sector, regime, breadth["advance_decline_ratio"],
            breadth["breadth_score"], momentum_acc,
            vol_info["current_vol"] * 100, vol_info["vol_percentile"]
        )

        if regime == "strong_stable_up":
            t = min(abs(sector_pct) / 3.0, 1.0)
            target = (
                _STRONG_TREND_UP_WEIGHTS
                if sector_pct > 0
                else _STRONG_TREND_DOWN_WEIGHTS
            )
            return self._lerp_weights(self._base_weights, target, t)
        elif regime == "accelerating_early":
            # 加速初期：追涨，RR 让给 sector
            return {"rr_score": 0.20, "market_score": 0.20, "sector_score": 0.25,
                    "volume_score": 0.15, "position_score": 0.10, "formation_score": 0.10}
        elif regime == "accelerating_late":
            # 加速后期：防回撤，RR 拉满 + sector 砍掉
            return {"rr_score": 0.35, "market_score": 0.15, "sector_score": 0.10,
                    "volume_score": 0.15, "position_score": 0.15, "formation_score": 0.10}
        elif regime == "decelerating":
            # 趋势减速：RR+position 保命，form 噪音大降权
            return {"rr_score": 0.35, "market_score": 0.15, "sector_score": 0.10,
                    "volume_score": 0.15, "position_score": 0.20, "formation_score": 0.05}
        elif regime == "weak":
            # 弱势：不碰板块，RR+position 兜底
            return {"rr_score": 0.35, "market_score": 0.20, "sector_score": 0.05,
                    "volume_score": 0.15, "position_score": 0.15, "formation_score": 0.10}
        elif regime == "bearish":
            # 持续弱势：防守为主
            return _BEARISH_WEIGHTS.copy()
        elif regime == "high_volatility":
            t = min(max((100 - sector_score) / 50.0, 0.0), 1.0)
            return self._lerp_weights(self._base_weights, _HIGH_VOL_WEIGHTS, t)

        # 平稳市：大盘波动低，降低大盘权重，增配位置和量能
        if self._is_calm():
            return _CALM_WEIGHTS.copy()

        return self._base_weights.copy()

    def _calc_index_pct(self) -> float:
        """大盘单日涨跌幅（%），从 _index_ohlcv 最新两根 K 线计算。"""
        ohlcv = getattr(self, "_index_ohlcv", None)
        if ohlcv is None or len(ohlcv) < 2:
            return 0.0
        closes = ohlcv[:, 3] if ohlcv.ndim == 2 else ohlcv
        if len(closes) < 2 or closes[-2] <= 0:
            return 0.0
        return (closes[-1] / closes[-2] - 1) * 100

    def _is_crisis(self) -> bool:
        """大盘危机判断：近5日跌幅 > 10% 或波动率极端。"""
        ohlcv = getattr(self, "_index_ohlcv", None)
        if ohlcv is None or len(ohlcv) < 10:
            return False

        closes = ohlcv[:, 3] if ohlcv.ndim == 2 else ohlcv
        if len(closes) < 5:
            return False

        recent = closes[-5:]
        cumulative_return = (recent[-1] / recent[0]) - 1
        if cumulative_return < -0.10:
            return True

        # 使用波动率聚类替代单点 ATR
        vol_info = self._calc_volatility_regime(closes)
        if vol_info["vol_regime"] == "extreme":
            return True

        return False

    def _is_calm(self) -> bool:
        """平稳市判断：波动率处于低位。"""
        ohlcv = getattr(self, "_index_ohlcv", None)
        if ohlcv is None or len(ohlcv) < 30:
            return False

        closes = ohlcv[:, 3] if ohlcv.ndim == 2 else ohlcv
        vol_info = self._calc_volatility_regime(closes)
        return vol_info["vol_regime"] == "low"

    def _judge_sector_regime(
        self, sector_pct: float, hist_closes: Optional[np.ndarray],
        breadth: Optional[Dict[str, float]] = None,
        momentum_acc: float = 0.0,
        index_pct: float = 0.0,
        vol_info: Optional[Dict[str, float]] = None
    ) -> str:
        """板块市场状态：层级化判定。

        先判宏观趋势方向，再在趋势内部判加速子阶段。
        - 上升趋势 → strong_stable_up / accelerating_early / accelerating_late / decelerating
        - 下降趋势 → bearish / decelerating
        - 其他 → weak / high_volatility / range_bound
        """
        if hist_closes is None or len(hist_closes) < 20:
            return "range_bound"

        ma20 = np.mean(hist_closes[-20:])
        trend_strength = self._calc_trend_strength(hist_closes)

        # 市场宽度信号
        breadth_score = breadth.get("breadth_score", 50.0) if breadth else 50.0
        ad_ratio = breadth.get("advance_decline_ratio", 1.0) if breadth else 1.0

        # 1. 弱势判断：三条件取二（板块深度下跌 + 市场宽度崩塌 + 大盘同步下跌）
        sector_weak = sector_pct < -7
        breadth_weak = ad_ratio < 0.5
        market_weak = index_pct < -3
        if (sector_weak + breadth_weak + market_weak) >= 2:
            return "weak"

        # 2. 上升趋势：价格 > MA20 + 趋势强
        if hist_closes[-1] > ma20 and trend_strength > 0.008:
            # 在上升趋势内部判定加速子阶段
            if momentum_acc > 0.003:
                stage = self._calc_acceleration_stage(hist_closes, momentum_acc)
                return f"accelerating_{stage}"
            elif momentum_acc < -0.003:
                return "decelerating"  # 上涨减速
            else:
                return "strong_stable_up"  # 稳定上升

        # 3. 下降趋势：价格 < MA20 + 趋势强
        if hist_closes[-1] < ma20 and trend_strength > 0.008:
            if momentum_acc > 0.003:
                return "decelerating"  # 下跌减速（可能见底）
            else:
                return "bearish"  # 持续弱势

        # 4. 高波动判断（使用长期波动率百分位）
        if vol_info is None:
            vol_info = self._calc_long_term_vol_percentile(hist_closes)
        if vol_info["vol_regime"] in ("high", "extreme"):
            return "high_volatility"

        # 5. 震荡市
        if trend_strength < 0.003 and np.std(hist_closes[-20:]) / ma20 < 0.02:
            return "range_bound"

        return "range_bound"

    def _calc_trend_strength(self, hist_closes: Optional[np.ndarray]) -> float:
        """趋势强度：近5日日均收益率绝对值。"""
        if hist_closes is None or len(hist_closes) < 6:
            return 0.0
        daily_returns = np.diff(hist_closes[-5:]) / hist_closes[-5:-1]
        return abs(np.mean(daily_returns))

    def _get_sector_hist_closes(self, sector: str) -> Optional[np.ndarray]:
        """从 DB 读取板块近 60 日收盘价（带缓存，供趋势/波动率/RSI 等多指标使用）。"""
        if sector in self._sector_hist_cache:
            return self._sector_hist_cache[sector]
        try:
            from datetime import date, timedelta
            from src.storage import SectorDaily

            with self._db.get_session() as session:
                end_date = date.today()
                start_date = end_date - timedelta(days=90)

                rows = (
                    session.query(SectorDaily)
                    .filter(
                        SectorDaily.sector_name == sector,
                        SectorDaily.trade_date >= start_date,
                        SectorDaily.trade_date <= end_date,
                    )
                    .order_by(SectorDaily.trade_date)
                    .all()
                )

                if not rows:
                    self._sector_hist_cache[sector] = None
                    return None
                closes = np.array([float(r.close) for r in rows], dtype=float)
                result = closes[-60:] if len(closes) >= 60 else closes
                self._sector_hist_cache[sector] = result
                return result
        except Exception:
            self._sector_hist_cache[sector] = None
            return None

    def _compute_atr(self, ohlcv: np.ndarray) -> float:
        """计算 ATR(14)。ohlcv: shape (N, 4) or (N, 3)，4列=[open,high,low,close]."""
        if ohlcv is None or len(ohlcv) < 2:
            return 0.0
        if ohlcv.ndim == 2 and ohlcv.shape[1] >= 4:
            highs, lows, closes = ohlcv[:, 1], ohlcv[:, 2], ohlcv[:, 3]
        else:
            return 0.0
        trs = np.array(
            [
                max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
                for i in range(1, len(closes))
            ]
        )
        return float(np.mean(trs[-14:])) if len(trs) >= 14 else float(np.mean(trs))

    # =================================================================
    # 市场宽度指标
    # =================================================================

    def _calc_market_breadth(self) -> Dict[str, float]:
        """计算市场宽度指标：涨跌比、新高新低比（带缓存，同轮次只查一次 DB）。"""
        if self._breadth_cache is not None:
            return self._breadth_cache

        result = {
            "advance_decline_ratio": 1.0,
            "new_high_low_ratio": 1.0,
            "breadth_score": 50.0,
        }

        try:
            from sqlalchemy import text
            from datetime import date, timedelta

            with self._db.get_session() as session:
                # 涨跌比：从 stock_daily 最新数据
                today = date.today()
                row = session.execute(
                    text("""
                        SELECT
                            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as advance,
                            SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) as decline
                        FROM stock_daily
                        WHERE date = (SELECT MAX(date) FROM stock_daily)
                    """)
                ).fetchone()

                if row and row[0] and row[1] and row[1] > 0:
                    advance, decline = row[0], row[1]
                    result["advance_decline_ratio"] = advance / decline

                # 新高新低比：近一年新高/新低
                one_year_ago = today - timedelta(days=252)
                row = session.execute(
                    text("""
                        SELECT
                            SUM(CASE WHEN latest.close >= yearly.high THEN 1 ELSE 0 END) as new_highs,
                            SUM(CASE WHEN latest.close <= yearly.low THEN 1 ELSE 0 END) as new_lows
                        FROM (
                            SELECT code, close FROM stock_daily
                            WHERE date = (SELECT MAX(date) FROM stock_daily)
                        ) latest
                        JOIN (
                            SELECT code, MAX(high) as high, MIN(low) as low
                            FROM stock_daily WHERE date >= :start_date
                            GROUP BY code
                        ) yearly ON latest.code = yearly.code
                    """),
                    {"start_date": one_year_ago}
                ).fetchone()

                if row and row[0] is not None and row[1] is not None:
                    new_highs, new_lows = row[0], row[1]
                    if new_lows > 0:
                        result["new_high_low_ratio"] = new_highs / new_lows
                    elif new_highs > 0:
                        result["new_high_low_ratio"] = 10.0  # 全是新高

            # 综合宽度分：涨跌比权重 60%，新高新低比权重 40%
            ad_ratio = result["advance_decline_ratio"]
            hl_ratio = result["new_high_low_ratio"]

            # 涨跌比分：ratio=2 → 80分，ratio=1 → 50分，ratio=0.5 → 20分
            ad_score = min(100, max(0, 50 + 30 * np.log2(max(ad_ratio, 0.01))))
            # 新高新低比分：类似映射
            hl_score = min(100, max(0, 50 + 20 * np.log2(max(hl_ratio, 0.01))))

            result["breadth_score"] = ad_score * 0.6 + hl_score * 0.4

        except Exception as e:
            logger.debug("[StockScorer] 市场宽度计算失败: %s", e)

        self._breadth_cache = result
        return result

    def _calc_rsi(self, hist_closes: Optional[np.ndarray], period: int = 14) -> float:
        """计算 RSI 指标（最新值）。"""
        rsi_series = self._calc_rsi_series(hist_closes, period)
        return float(rsi_series[-1]) if len(rsi_series) > 0 else 50.0

    def _calc_rsi_series(self, hist_closes: Optional[np.ndarray], period: int = 14) -> np.ndarray:
        """计算 RSI 时间序列（每天一个值）。"""
        if hist_closes is None or len(hist_closes) < period + 1:
            return np.array([50.0])
        returns = np.diff(hist_closes) / hist_closes[:-1]
        rsi_values = []
        for i in range(period, len(returns)):
            window = returns[i - period:i]
            gains = np.where(window > 0, window, 0.0)
            losses = np.where(window < 0, -window, 0.0)
            avg_gain = np.mean(gains)
            avg_loss = np.mean(losses)
            rs = avg_gain / max(avg_loss, 1e-10)
            rsi_values.append(100.0 - 100.0 / (1.0 + rs))
        return np.array(rsi_values) if rsi_values else np.array([50.0])

    def _calc_acceleration_stage(
        self, hist_closes: Optional[np.ndarray], momentum_acc: float
    ) -> str:
        """判断加速阶段：early（初期追涨）vs late（后期谨慎）。

        综合三个信号：
        1. MA20 乖离率 — 价格离均线越远越可能是后期
        2. RSI 动量方向 — RSI 下降=动能背离(后期)，RSI 上升=动能确认(初期)
        3. 连续加速天数 — 持续越久越接近尾声

        Returns:
            "early" 或 "late"
        """
        if hist_closes is None or len(hist_closes) < 20:
            return "early"

        # 信号1: MA20 乖离率
        ma20 = np.mean(hist_closes[-20:])
        deviation = (hist_closes[-1] - ma20) / ma20 if ma20 > 0 else 0.0

        # 信号2: RSI 动量方向（近5日均值 vs 前5日均值），阈值用 RSI 标准差缩放
        rsi_series = self._calc_rsi_series(hist_closes)
        if len(rsi_series) >= 10:
            rsi_recent = float(np.mean(rsi_series[-5:]))
            rsi_prev = float(np.mean(rsi_series[-10:-5]))
            rsi_change = rsi_recent - rsi_prev
            rsi_std = float(np.std(rsi_series)) if len(rsi_series) > 1 else 5.0
            rsi_std = max(rsi_std, 3.0)  # 最低 3 防止阈值过小
        else:
            rsi_change = 0.0
            rsi_std = 5.0
        rsi_current = float(rsi_series[-1]) if len(rsi_series) > 0 else 50.0

        # 信号3: 连续加速天数
        returns = np.diff(hist_closes) / hist_closes[:-1]
        consecutive_acc_days = 0
        if len(returns) >= 6:
            for i in range(len(returns) - 3, 0, -1):
                recent = np.mean(returns[i:i + 3])
                prev = np.mean(returns[max(0, i - 3):i])
                if recent > prev and recent > 0:
                    consecutive_acc_days += 1
                else:
                    break

        # 综合评分
        late_score = 0

        # 乖离率：越远越可能是后期
        if deviation > 0.10:
            late_score += 2
        elif deviation > 0.05:
            late_score += 1

        # RSI 动量方向：下降=背离(后期) 走平=衰减 上升=确认(初期)
        # 三档：<-1σ 下降(+2), -0.5σ~+0.5σ 走平(+1), >+0.5σ 上升(+0)
        if rsi_change < -1.0 * rsi_std:
            late_score += 2  # RSI 明显下降，动能背离
        elif abs(rsi_change) < 0.5 * rsi_std:
            late_score += 1  # RSI 真正走平，动能衰减

        # 连续加速天数
        if consecutive_acc_days > 5:
            late_score += 2
        elif consecutive_acc_days > 3:
            late_score += 1

        logger.debug(
            "[StockScorer] 加速阶段判定: 乖离率=%.1f%% RSI=%.0f(变化%+.1f) 连续加速=%d天 late_score=%d",
            deviation * 100, rsi_current, rsi_change, consecutive_acc_days, late_score
        )

        return "late" if late_score >= 3 else "early"

    def _calc_momentum_acceleration(self, hist_closes: Optional[np.ndarray]) -> float:
        """计算动量加速度（二阶导数）。

        正值 = 趋势加速，负值 = 趋势减速。

        Returns:
            加速度值（日收益率的变化率）
        """
        if hist_closes is None or len(hist_closes) < 8:
            return 0.0

        # 计算日收益率
        returns = np.diff(hist_closes) / hist_closes[:-1]

        if len(returns) < 4:
            return 0.0

        # 近期动量（近3日均值）vs 前期动量（前3日均值）
        recent_momentum = np.mean(returns[-3:])
        prev_momentum = np.mean(returns[-6:-3])

        # 加速度 = 近期动量 - 前期动量
        acceleration = recent_momentum - prev_momentum

        return float(acceleration)

    def _calc_volatility_regime(self, hist_closes: Optional[np.ndarray]) -> Dict[str, float]:
        """波动率聚类分析（简化版 GARCH）。

        使用 EWMA(lambda=0.94) 估计条件方差，比单点 ATR 更稳定。
        最少需要 15 个收益率点（即 16 个价格点）。

        Returns:
            {
                "current_vol": 当前波动率（年化）,
                "vol_percentile": 波动率百分位 (0-100),
                "vol_regime": 波动率状态 ("low"/"normal"/"high"/"extreme")
            }
        """
        return self._compute_vol_percentile(hist_closes, min_returns=15, min_closes=16)

    def _calc_long_term_vol_percentile(self, hist_closes: Optional[np.ndarray]) -> Dict[str, float]:
        """用更长窗口计算波动率百分位（比 _calc_volatility_regime 更稳定）。

        需要 20+ 收益率点（即 21+ 价格点），用于 regime 判定中的高波动检测。
        """
        return self._compute_vol_percentile(hist_closes, min_returns=20, min_closes=21)

    def _compute_vol_percentile(
        self, hist_closes: Optional[np.ndarray],
        min_returns: int = 15, min_closes: int = 16,
    ) -> Dict[str, float]:
        """EWMA 波动率百分位计算（统一实现）。"""
        result = {"current_vol": 0.0, "vol_percentile": 50.0, "vol_regime": "normal"}

        if hist_closes is None or len(hist_closes) < min_closes:
            return result

        returns = np.diff(hist_closes) / hist_closes[:-1]
        if len(returns) < min_returns:
            return result

        lambda_ = 0.94
        ewma_var = returns[0] ** 2
        ewma_vars = [ewma_var]
        for r in returns[1:]:
            ewma_var = lambda_ * ewma_var + (1 - lambda_) * r ** 2
            ewma_vars.append(ewma_var)

        current_vol = float(np.sqrt(ewma_vars[-1]) * np.sqrt(252))
        result["current_vol"] = current_vol

        all_vols = np.array([np.sqrt(v) * np.sqrt(252) for v in ewma_vars])
        all_vols.sort()
        idx = int(np.searchsorted(all_vols, current_vol, side="right"))
        percentile = (idx / len(all_vols)) * 100
        result["vol_percentile"] = percentile

        if percentile > 90:
            result["vol_regime"] = "extreme"
        elif percentile > 70:
            result["vol_regime"] = "high"
        elif percentile < 20:
            result["vol_regime"] = "low"

        return result

    @staticmethod
    def _lerp_weights(w1: Dict[str, float], w2: Dict[str, float], t: float) -> Dict[str, float]:
        """线性插值：t=0 返回 w1，t=1 返回 w2。"""
        t = max(0.0, min(1.0, t))
        return {k: w1[k] * (1 - t) + w2[k] * t for k in w1}