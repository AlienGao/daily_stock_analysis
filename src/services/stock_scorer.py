# -*- coding: utf-8 -*-
"""多维技术评分器（StockScorer）。

对候选股计算 6 个维度的 0-100 技术评分，最终输出加权综合分 tech_score，
用于盘中扫描结果排序。板块级别动态权重，不依赖大盘统一判定。

评分维度：
  1. RR分（赔率）       权重 25%  — RR = (TP1 - price) / (price - stop_loss)
  2. 大盘分            权重 20%  — 上证指数 MA20/MA60 偏离度
  3. 板块分            权重 15%  — 个股在板块内的相对强弱
  4. 量能分            权重 15%  — 量比、价升量增 vs 缩量上涨
  5. 位置分            权重 15%  — BOLL 通道位置、乖离度、超买扣分
  6. 形态分            权重 10%  — 复用 reasons 关键词匹配

动态权重：
  - 板块强趋势（板块 MA20 向上 + 趋势强度 > 0.8）：提高板块权重 25%
  - 板块弱势（近5日涨幅 < -5%）：降低板块权重，提高 RR 权重
  - 大盘危机（近5日跌幅 > 10% 或 ATR 百分位 > 95%）：统一切换到 Crisis 权重
  - 权重切换使用线性插值（软切换），避免硬跳
"""

from __future__ import annotations

import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from src.storage import DatabaseManager

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

    def __init__(self):
        self._db = DatabaseManager.get_instance()
        # 板块当日涨跌幅缓存（每个扫描轮次更新一次）
        self._sector_pct_cache: Dict[str, float] = {}
        # 大盘 OHLCV 缓存
        self._index_ohlcv_cache: Optional[np.ndarray] = None

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
        rr = self._calc_rr_score(price, tp1, stop_loss)
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

    def _calc_rr_score(self, price: float, tp1: float, stop_loss: float) -> float:
        """赔率评分：RR = (TP1 - price) / (price - stop_loss)，RR=2.0 时满分。"""
        if price <= stop_loss or tp1 <= price:
            return 0.0
        rr = (tp1 - price) / (price - stop_loss)
        return min(rr / 2.0, 1.0) * 100

    def _calc_market_score(self) -> float:
        """大盘环境评分：基于上证指数 MA20/MA60 偏离度，输出 0-100。"""
        ohlcv = getattr(self, "_index_ohlcv", None)
        if ohlcv is None or len(ohlcv) < 20:
            return 50.0  # 无数据返回中间值
        closes = ohlcv[:, 3] if ohlcv.ndim == 2 else ohlcv
        if isinstance(closes, np.ndarray) and closes.ndim == 1:
            price = float(closes[-1])
            ma20 = float(np.mean(closes[-20:]))
            ma60 = float(np.mean(closes[-60:])) if len(closes) >= 60 else ma20
            score = 0.6 * min(price / ma20 * 50, 100) + 0.4 * min(price / ma60 * 50, 100)
            return min(max(score, 0), 100)
        return 50.0

    def _calc_sector_score(self, stock_code: str, sector: str, price: float, pre_close: float) -> float:
        """板块评分：绝对涨幅（40%）+ 相对强弱（60%）。

        绝对分：涨10%→100 跌10%→0 平盘→50
        相对分：跑赢板块5%→100 跑输5%→0 持平→50
        """
        sector_pct = self._sector_pct_cache.get(sector, 0.0)
        stock_pct = 0.0
        if pre_close > 0 and price > 0:
            stock_pct = (price - pre_close) / pre_close * 100

        # 绝对涨幅分：-10%→0, 0%→50, +10%→100
        abs_score = min(max((stock_pct + 10) / 20 * 100, 0), 100)

        # 相对强弱分：跑输5%→0, 持平→50, 跑赢5%→100
        relative = stock_pct - sector_pct
        rel_score = min(max(relative * 10 + 50, 0), 100)

        return abs_score * 0.4 + rel_score * 0.6

    def _calc_volume_score(
        self, price: float, pre_close: float, volume_ratio: float
    ) -> float:
        """成交量质量评分：量比 + 价量配合方向。"""
        if pre_close <= 0 or volume_ratio <= 0:
            return 50.0

        price_pct = (price - pre_close) / pre_close * 100

        if volume_ratio > 1.2 and price_pct > 0.5:
            # 价升量增 → 最强
            return 90.0
        elif volume_ratio > 1.2 and price_pct < -0.5:
            # 放量下跌 → 偏弱
            return 40.0
        elif volume_ratio < 0.8 and price_pct > 0.5:
            # 缩量上涨 → 警惕
            return 45.0
        elif volume_ratio < 0.8:
            # 缩量整理 → 正常
            return 65.0
        else:
            # 温和放量
            return 70.0

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
            base -= 15

        # 超买软扣分（价格 >= TP1 → 已经没有盈利空间）
        if price >= tp1:
            base -= 20

        return min(max(base, 0), 100)

    def _calc_formation_score(self, reasons: List[str]) -> float:
        """形态确认度评分：复用 reasons 关键词匹配。"""
        score = 0.0
        for r in reasons:
            if "均线多头排列" in r:
                score += 30
            if "回踩MA5均线" in r:
                score += 20
            if "BOLL中轨支撑" in r:
                score += 15
            if "强势" in r or "量价齐升" in r or "放量" in r:
                score += 15
            if "涨停" in r:
                score -= 30  # 已涨停无空间
            if "KDJ" in r and "超卖" in r:
                score += 10
            if "MACD金叉" in r or "金叉" in r:
                score += 15
            if "KDJ金叉" in r:
                score += 10
            if "均线粘合" in r:
                score += 10
        return min(max(score, 0), 100)

    # =================================================================
    # 动态权重
    # =================================================================

    def _get_dynamic_weights(self, sector: str, sector_score: float) -> Dict[str, float]:
        """根据板块状态返回动态权重。"""
        # 大盘危机检测
        if self._is_crisis():
            return _CRISIS_WEIGHTS.copy()

        # 板块状态判定
        sector_pct = self._sector_pct_cache.get(sector, 0.0)
        hist_closes = self._get_sector_hist_closes(sector)

        regime = self._judge_sector_regime(sector_pct, hist_closes)
        trend_strength = self._calc_trend_strength(hist_closes)

        if regime == "strong_trend":
            sector_trending_up = sector_pct > 0
            t = min(abs(sector_pct) / 3.0, 1.0)
            target = (
                _STRONG_TREND_UP_WEIGHTS
                if sector_trending_up
                else _STRONG_TREND_DOWN_WEIGHTS
            )
            return self._lerp_weights(_BASE_WEIGHTS, target, t)
        elif regime == "weak":
            return {**_BASE_WEIGHTS, "sector_score": 0.05, "rr_score": 0.30}
        elif regime == "high_volatility":
            t = min(max((100 - sector_score) / 50.0, 0.0), 1.0)
            return self._lerp_weights(_BASE_WEIGHTS, _HIGH_VOL_WEIGHTS, t)

        # 平稳市：大盘波动低，降低大盘权重，增配位置和量能
        if self._is_calm():
            return _CALM_WEIGHTS.copy()

        return _BASE_WEIGHTS.copy()

    def _is_crisis(self) -> bool:
        """大盘危机判断：近5日跌幅 > 10% 或 ATR 百分位 > 95%。"""
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

        # ATR 百分位
        if len(ohlcv) >= 30:
            atr_now = self._compute_atr(ohlcv[-15:])
            all_atrs = [
                self._compute_atr(ohlcv[i - 14 : i + 1])
                for i in range(14, len(ohlcv))
            ]
            if all_atrs and max(all_atrs) > 0:
                atr_pct = (sum(1 for x in all_atrs if x <= atr_now) / len(all_atrs)) * 100
                if atr_pct > 95:
                    return True
        return False

    def _is_calm(self) -> bool:
        """平稳市判断：大盘 ATR 百分位 < 30%。"""
        ohlcv = getattr(self, "_index_ohlcv", None)
        if ohlcv is None or len(ohlcv) < 30:
            return False
        atr_now = self._compute_atr(ohlcv[-15:])
        all_atrs = [
            self._compute_atr(ohlcv[i - 14 : i + 1])
            for i in range(14, len(ohlcv))
        ]
        if all_atrs and max(all_atrs) > 0:
            pct = (sum(1 for x in all_atrs if x <= atr_now) / len(all_atrs)) * 100
            return pct < 30
        return False

    def _judge_sector_regime(
        self, sector_pct: float, hist_closes: Optional[np.ndarray]
    ) -> str:
        """板块市场状态：强趋势 / 震荡 / 弱势。"""
        if hist_closes is None or len(hist_closes) < 20:
            return "range_bound"

        ma20 = np.mean(hist_closes[-20:])
        trend_strength = self._calc_trend_strength(hist_closes)

        if hist_closes[-1] > ma20 and trend_strength > 0.008:
            return "strong_trend"
        elif sector_pct < -5:
            return "weak"
        elif trend_strength < 0.003 and np.std(hist_closes[-20:]) / ma20 < 0.02:
            return "range_bound"
        return "range_bound"

    def _calc_trend_strength(self, hist_closes: Optional[np.ndarray]) -> float:
        """趋势强度：近5日日均收益率绝对值。"""
        if hist_closes is None or len(hist_closes) < 6:
            return 0.0
        daily_returns = np.diff(hist_closes[-5:]) / hist_closes[-6:-1]
        return abs(np.mean(daily_returns))

    def _get_sector_hist_closes(self, sector: str) -> Optional[np.ndarray]:
        """从 DB 读取板块近 20 日收盘价。"""
        try:
            from datetime import date, timedelta
            from src.storage import SectorDaily

            session = self._db.get_session()
            end_date = date.today()
            start_date = end_date - timedelta(days=40)

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
                return None
            closes = np.array([float(r.close) for r in rows], dtype=float)
            return closes[-20:] if len(closes) >= 20 else closes
        except Exception:
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

    @staticmethod
    def _lerp_weights(w1: Dict[str, float], w2: Dict[str, float], t: float) -> Dict[str, float]:
        """线性插值：t=0 返回 w1，t=1 返回 w2。"""
        t = max(0.0, min(1.0, t))
        return {k: w1[k] * (1 - t) + w2[k] * t for k in w1}