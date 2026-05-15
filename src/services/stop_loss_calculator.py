# -*- coding: utf-8 -*-
"""个股止盈止损计算基础设施。

两层架构：
  Layer 1 — 纯计算（仅依赖 numpy，项目无关，可独立复用）
  Layer 2 — 数据适配（DatabaseManager 胶水，项目相关）

使用示例:
  # 独立使用（无 DB 依赖）
  from src.services.stop_loss_calculator import compute_from_arrays
  result = compute_from_arrays(highs, lows, closes)

  # 项目内使用
  calc = StopLossCalculator()
  result = calc.compute("600519", date.today())
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 可调参数
# ---------------------------------------------------------------------------
ATR_PERIOD = 20
ATR_PERCENTILE_LOOKBACK = 60
ATR_STOP_MULTIPLIER = 2.0          # 正常波动下 ATR 止损倍数
ATR_STOP_MULTIPLIER_WIDE = 3.0     # 高波动下 ATR 宽止损倍数
ATR_STOP_MULTIPLIER_TIGHT = 1.5    # 紧止损倍数
SWING_LOOKBACK = 20
MAX_DRAWDOWN_LOOKBACK = 20
MA60_PERIOD = 60
DATA_LOOKBACK_DAYS = 180           # fetch 回看窗口

# 高/低波动阈值 (ATR 百分位)
HIGH_VOL_THRESHOLD = 70
LOW_VOL_THRESHOLD = 30

# MA20 贴近判定阈值
MA_PROXIMITY_PCT = 3.0  # close 在 MA20 +/-3% 内视为贴近


# ---------------------------------------------------------------------------
# 输出结构
# ---------------------------------------------------------------------------

@dataclass
class StopLossResult:
    """止盈止损计算结果。"""

    code: str
    trade_date: date
    current_price: float

    # 核心指标
    atr_14: Optional[float] = None
    atr_percentile: Optional[float] = None
    boll_lower: Optional[float] = None
    swing_low_20: Optional[float] = None
    swing_high_20: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    max_drawdown_20: Optional[float] = None

    # 买入区间
    buy_low: Optional[float] = None
    buy_high: Optional[float] = None

    # 止损
    stop_loss: Optional[float] = None
    stop_loss_tight: Optional[float] = None
    stop_loss_wide: Optional[float] = None

    # 止盈
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None

    # 风控
    risk_reward_ratio: Optional[float] = None

    # 元数据
    stop_method: Optional[str] = None
    reasoning: Optional[str] = None
    valid: bool = True
    error_msg: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "trade_date": self.trade_date.isoformat() if self.trade_date else None,
            "current_price": self.current_price,
            "atr_14": self.atr_14,
            "atr_percentile": self.atr_percentile,
            "boll_lower": self.boll_lower,
            "swing_low_20": self.swing_low_20,
            "swing_high_20": self.swing_high_20,
            "ma20": self.ma20,
            "ma60": self.ma60,
            "max_drawdown_20": self.max_drawdown_20,
            "buy_low": self.buy_low,
            "buy_high": self.buy_high,
            "stop_loss": self.stop_loss,
            "stop_loss_tight": self.stop_loss_tight,
            "stop_loss_wide": self.stop_loss_wide,
            "take_profit_1": self.take_profit_1,
            "take_profit_2": self.take_profit_2,
            "risk_reward_ratio": self.risk_reward_ratio,
            "stop_method": self.stop_method,
            "reasoning": self.reasoning,
            "valid": self.valid,
            "error_msg": self.error_msg,
        }


# ===================================================================
# Layer 1 — 纯计算（项目无关，仅依赖 numpy）
# ===================================================================

def compute_from_arrays(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    code: str = "",
    trade_date: Optional[date] = None,
    ma20: Optional[float] = None,
    ma60: Optional[float] = None,
    atr: Optional[float] = None,
    factor_score: float = 25.0,
) -> StopLossResult:
    """从 OHLCV 数组计算止盈止损（纯计算，零项目依赖）。

    Args:
        highs: 最高价序列 (任意长度，索引 -1 = 最新)
        lows: 最低价序列
        closes: 收盘价序列
        code: 股票代码（可选，用于结果标识）
        trade_date: 交易日期（可选）
        ma20: 预计算的 MA20（可选，传入则跳过本地计算）
        ma60: 预计算的 MA60（可选）
        atr: 预计算的 ATR(14)（可选）
        factor_score: 因子综合评分 0-100，高分→更宽止损/买入区间

    Returns:
        StopLossResult
    """
    n = len(closes)
    if n < SWING_LOOKBACK:
        return StopLossResult(
            code=code,
            trade_date=trade_date or date.today(),
            current_price=float(closes[-1]) if n > 0 else 0.0,
            valid=False,
            error_msg=f"数据不足: {n} 条 < {SWING_LOOKBACK} 条最低要求",
        )

    # 过滤极端异常值（脏数据如 high=51 实际应为 ~11）
    highs, lows, closes = _filter_outliers(highs, lows, closes)
    n = len(closes)
    if n < SWING_LOOKBACK:
        return StopLossResult(
            code=code,
            trade_date=trade_date or date.today(),
            current_price=float(closes[-1]) if n > 0 else 0.0,
            valid=False,
            error_msg=f"数据不足（过滤异常值后）: {n} 条 < {SWING_LOOKBACK} 条",
        )

    current_price = float(closes[-1])

    # --- 核心指标 ---
    if atr is None:
        atr = _compute_atr(highs, lows, closes, ATR_PERIOD)
    atr_14 = float(atr) if atr is not None and not np.isnan(atr) else None

    atr_percentile = _compute_atr_percentile(highs, lows, closes)

    swing_low_20 = float(np.min(lows[-SWING_LOOKBACK:]))
    swing_high_20 = float(np.max(highs[-SWING_LOOKBACK:]))

    if ma20 is None:
        ma20 = _compute_ma(closes, 20)
    ma20_f = float(ma20) if ma20 is not None and not np.isnan(ma20) else None

    if ma60 is None:
        ma60 = _compute_ma(closes, MA60_PERIOD) if n >= MA60_PERIOD else None
    ma60_f = float(ma60) if ma60 is not None and not np.isnan(ma60) else None

    max_dd = _compute_max_drawdown(closes, MAX_DRAWDOWN_LOOKBACK)

    # --- Bollinger 下轨（自算，盘中使用实时数据） ---
    if n >= 20 and ma20_f is not None:
        boll_lower = round(ma20_f - 2 * float(np.std(closes[-20:])), 3)
    else:
        boll_lower = None

    # --- 止损方法选择 ---
    stop_method, reasoning, stop_loss = _select_stop_method(
        current_price=current_price,
        atr_14=atr_14,
        atr_percentile=atr_percentile,
        ma20=ma20_f,
        ma60=ma60_f,
        swing_low_20=swing_low_20,
        swing_high_20=swing_high_20,
        boll_lower=boll_lower,
        factor_score=factor_score,
    )

    # --- 复合止损 ---
    stop_loss_tight = _build_tight_stop(swing_high_20, atr_14) if atr_14 else None

    if atr_14 and atr_14 > 0:
        stop_loss_wide = round(float(max(
            current_price - ATR_STOP_MULTIPLIER_WIDE * atr_14,
            ma60_f or swing_low_20,
            swing_low_20,
        )), 2)
    elif ma60_f:
        stop_loss_wide = round(float(max(ma60_f, swing_low_20)), 2)
    else:
        stop_loss_wide = round(swing_low_20, 2)

    # --- 买入区间 ---
    buy_low, buy_high = _compute_buy_range(current_price, boll_lower, ma20_f, factor_score)

    # --- 止盈 ---
    take_profit_1, take_profit_2 = _build_take_profits(
        current_price, swing_high_20, atr_14
    )

    # --- 风控 ---
    rr = None
    if stop_loss and current_price > stop_loss > 0:
        risk = current_price - stop_loss
        reward = (take_profit_1 - current_price) if take_profit_1 else 0
        if risk > 0:
            rr = round(reward / risk, 2)

    return StopLossResult(
        code=code,
        trade_date=trade_date or date.today(),
        current_price=current_price,
        atr_14=atr_14,
        atr_percentile=atr_percentile,
        boll_lower=boll_lower,
        swing_low_20=swing_low_20,
        swing_high_20=swing_high_20,
        ma20=ma20_f,
        ma60=ma60_f,
        max_drawdown_20=max_dd,
        buy_low=buy_low,
        buy_high=buy_high,
        stop_loss=stop_loss,
        stop_loss_tight=stop_loss_tight,
        stop_loss_wide=stop_loss_wide,
        take_profit_1=take_profit_1,
        take_profit_2=take_profit_2,
        risk_reward_ratio=rr,
        stop_method=stop_method,
        reasoning=reasoning,
        valid=True,
    )


# ---- 数据清洗 ----

def _filter_outliers(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """过滤极端异常 OHLCV 条（收盘偏离中位数 3x 以上视为脏数据）。"""
    if len(closes) < 5:
        return highs, lows, closes
    med = float(np.median(closes))
    if med <= 0:
        return highs, lows, closes
    mask = (closes >= med * 0.33) & (closes <= med * 3.0)
    if mask.all():
        return highs, lows, closes
    dropped = (~mask).sum()
    logger.debug(f"_filter_outliers dropped {dropped} bars (median={med:.2f})")
    return highs[mask].copy(), lows[mask].copy(), closes[mask].copy()


# ---- 指标计算 ----

def _compute_true_range(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray
) -> np.ndarray:
    """逐日 True Range。"""
    prev_close = np.roll(closes, 1)
    prev_close[0] = closes[0]
    return np.maximum(
        highs - lows,
        np.maximum(
            np.abs(highs - prev_close),
            np.abs(lows - prev_close),
        ),
    )


def _compute_atr(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 20
) -> Optional[float]:
    """Wilder 平滑法 ATR。返回最新值，数据不足返回 None。"""
    n = len(closes)
    if n < period + 1:
        return None
    tr = _compute_true_range(highs, lows, closes)
    atr_val = float(np.mean(tr[:period]))
    for i in range(period, n):
        atr_val = (atr_val * (period - 1) + float(tr[i])) / period
    return atr_val if atr_val > 0 else 0.0


def _compute_atr_series(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 20
) -> np.ndarray:
    """返回完整 ATR(14) 序列（用于百分位计算）。"""
    n = len(closes)
    tr = _compute_true_range(highs, lows, closes)
    atr_series = np.full(n, np.nan)
    if n < period:
        return atr_series
    atr_series[period - 1] = float(np.mean(tr[:period]))
    for i in range(period, n):
        atr_series[i] = (atr_series[i - 1] * (period - 1) + float(tr[i])) / period
    return atr_series


def _compute_atr_percentile(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    lookback: int = ATR_PERCENTILE_LOOKBACK,
) -> Optional[float]:
    """当前 ATR(14) 在近 lookback 日 ATR 序列中的百分位 (0-100)。"""
    atr_series = _compute_atr_series(highs, lows, closes)
    valid = atr_series[~np.isnan(atr_series)]
    if len(valid) < 2:
        return None
    window = valid[-lookback:] if len(valid) >= lookback else valid
    current = valid[-1]
    if current <= 0:
        return None
    rank = np.sum(window <= current)
    return round(float(rank) / len(window) * 100, 1)


def _compute_ma(closes: np.ndarray, period: int) -> Optional[float]:
    """简单移动平均。"""
    n = len(closes)
    if n < period:
        return None
    return float(np.mean(closes[-period:]))


def _compute_max_drawdown(
    closes: np.ndarray, lookback: int = MAX_DRAWDOWN_LOOKBACK
) -> Optional[float]:
    """近 lookback 日最大回撤 (百分比，负值)。"""
    n = len(closes)
    if n < lookback:
        lookback = n
    window = closes[-lookback:]
    rolling_max = np.maximum.accumulate(window)
    dd = (window - rolling_max) / rolling_max * 100
    return round(float(np.min(dd)), 2)


# ---- 止损方法决策树 ----

def _compute_buy_range(
    current_price: float,
    boll_lower: Optional[float],
    ma20: Optional[float],
    factor_score: float = 25.0,
) -> Tuple[Optional[float], Optional[float]]:
    """计算买入区间。

    buy_low  — Bollinger 下轨锚定，因子分越高越宽松
    buy_high — MA20 锚定，因子分越高可略微超越 MA20，但不追高
    """
    if current_price <= 0:
        return None, None

    # 因子分分档 multiplier
    if factor_score >= 35:
        bl_mult, bh_mult = 0.97, 1.01
    elif factor_score >= 25:
        bl_mult, bh_mult = 0.98, 1.00
    else:
        bl_mult, bh_mult = 0.99, 0.99

    # buy_low: Bollinger 下轨锚定，不低于当前价 95%
    if boll_lower is not None and boll_lower > 0:
        anchor_low = max(boll_lower, current_price * 0.95)
        buy_low = round(anchor_low * bl_mult, 1)
    elif current_price > 0:
        if factor_score >= 35:
            buy_low = round(current_price * 0.95, 1)
        elif factor_score >= 25:
            buy_low = round(current_price * 0.97, 1)
        else:
            buy_low = round(current_price * 0.98, 1)
    else:
        return None, None

    # buy_high: MA20 * bh_mult，但不超当前价 +2%
    if ma20 is not None and ma20 > 0:
        buy_high = round(min(ma20 * bh_mult, current_price * 1.02), 1)
    else:
        buy_high = round(current_price * 1.02, 1)

    if buy_low >= buy_high:
        buy_low = round(buy_high * 0.98, 2)
        if buy_low >= buy_high:
            buy_low = buy_high - 0.01

    return buy_low, buy_high


def _select_stop_method(
    current_price: float,
    atr_14: Optional[float],
    atr_percentile: Optional[float],
    ma20: Optional[float],
    ma60: Optional[float],
    swing_low_20: float,
    swing_high_20: float,
    boll_lower: Optional[float] = None,
    factor_score: float = 25.0,
) -> Tuple[Optional[str], Optional[str], Optional[float]]:
    """止损方法决策树。返回 (method, reasoning, stop_loss_price)。"""

    if atr_14 is None or atr_14 <= 0:
        # 无 ATR 降级：优先 Bollinger 下轨 + 因子分分档（与内联一致）
        if boll_lower is not None and boll_lower > 0:
            if factor_score >= 35:
                sl = round(boll_lower * 0.94, 2)
            elif factor_score >= 25:
                sl = round(boll_lower * 0.95, 2)
            else:
                sl = round(boll_lower * 0.96, 2)
            return ("bollinger", f"ATR 不可用，Bollinger 下轨止损 (fs={factor_score:.0f})", sl)
        if current_price > 0:
            if factor_score >= 35:
                sl = round(current_price * 0.92, 2)
            elif factor_score >= 25:
                sl = round(current_price * 0.94, 2)
            else:
                sl = round(current_price * 0.95, 2)
            return ("close_pct", f"ATR 不可用，收盘价百分比止损 (fs={factor_score:.0f})", sl)
        sl = round(swing_low_20 * 0.99, 2)
        return ("swing_low", "ATR 不可用，回退 20 日低点止损", sl)

    # 1) 高波动
    if atr_percentile is not None and atr_percentile >= HIGH_VOL_THRESHOLD:
        sl = round(current_price - ATR_STOP_MULTIPLIER_WIDE * atr_14, 2)
        return (
            "volatility_band",
            f"高波动 (ATR 百分位 {atr_percentile})，{ATR_STOP_MULTIPLIER_WIDE}x ATR 宽止损",
            sl,
        )

    # 2) 低波动 + 价格在 MA20 上方
    if (
        atr_percentile is not None
        and atr_percentile < LOW_VOL_THRESHOLD
        and ma20 is not None
        and current_price > ma20
    ):
        sl = round(swing_high_20 - ATR_STOP_MULTIPLIER * atr_14, 2)
        return (
            "atr_trailing",
            f"低波上升 (ATR 百分位 {atr_percentile})，{ATR_STOP_MULTIPLIER}x ATR 追踪止损",
            sl,
        )

    # 3) 贴近 MA20
    if ma20 is not None and ma20 > 0:
        proximity = abs(current_price - ma20) / ma20 * 100
        if proximity <= MA_PROXIMITY_PCT:
            sl = round(ma20 * 0.99, 2)
            return (
                "ma_support",
                f"贴近 MA20 ({proximity:.1f}%)，MA20 支撑止损",
                sl,
            )

    # 4) 默认
    sl = round(swing_low_20 * 0.99, 2)
    return ("swing_low", "默认 20 日低点止损", sl)


# ---- 止损/止盈价格构建 ----

def _build_tight_stop(
    swing_high_20: float, atr_14: Optional[float]
) -> Optional[float]:
    """紧止损：近期高点 - 1.5x ATR。"""
    if atr_14 is None or atr_14 <= 0:
        return None
    return round(swing_high_20 - ATR_STOP_MULTIPLIER_TIGHT * atr_14, 2)


def _build_take_profits(
    current_price: float,
    swing_high_20: float,
    atr_14: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """构建两级止盈价格。"""
    if atr_14 and atr_14 > 0:
        tp1 = round(max(current_price * 1.03, current_price + 1.5 * atr_14), 2)
        tp2 = round(max(current_price * 1.07, swing_high_20 + 2 * atr_14), 2)
    else:
        tp1 = round(current_price * 1.05, 2)
        tp2 = round(current_price * 1.10, 2)
    return tp1, tp2


# ===================================================================
# Layer 2 — 数据适配（项目相关）
# ===================================================================

class StopLossCalculator:
    """止盈止损计算器（项目内使用，通过 DB 获取数据）。

    数据获取策略：
      - trade_date < today（盘后/历史）→ StockTechIndicator Tushare 前复权 AT/MA，
        OHLCV 用于 swing/ATR 百分位等本地衍生指标
      - trade_date == today（盘中）→ StockDaily 历史 OHLCV + RealtimeSpot 实时行情
    """

    def compute(self, code: str, trade_date: Optional[date] = None,
                factor_score: float = 25.0) -> StopLossResult:
        if trade_date is None:
            trade_date = date.today()

        today = date.today()
        is_today = trade_date == today

        try:
            ohlcv = self._fetch_ohlcv(code, trade_date)
        except Exception as e:
            logger.warning(f"[StopLossCalculator] 获取 OHLCV 失败 {code}: {e}")
            return StopLossResult(
                code=code, trade_date=trade_date, current_price=0.0,
                valid=False, error_msg=f"获取数据失败: {e}",
            )

        if not ohlcv:
            return StopLossResult(
                code=code, trade_date=trade_date, current_price=0.0,
                valid=False, error_msg="无 OHLCV 数据",
            )

        # --- 构建 OHLCV 数组 ---
        if is_today:
            highs, lows, closes, current_price = self._build_intraday_arrays(ohlcv, code)
        else:
            highs = np.array([d.high for d in ohlcv], dtype=float)
            lows = np.array([d.low for d in ohlcv], dtype=float)
            closes = np.array([d.close for d in ohlcv], dtype=float)
            current_price = float(closes[-1])

        # --- 预计算指标 ---
        atr = None
        ma20_db = None
        ma60_db = None

        if not is_today:
            tech = self._fetch_tech_indicator(code, trade_date)
            if tech:
                atr = tech.get("atr")
                ma20_db = tech.get("ma20")
                ma60_db = tech.get("ma60")

        if ma20_db is None and ohlcv:
            ma20_db = ohlcv[-1].ma20

        return compute_from_arrays(
            highs=highs,
            lows=lows,
            closes=closes,
            code=code,
            trade_date=trade_date,
            ma20=ma20_db,
            ma60=ma60_db,
            atr=atr,
            factor_score=factor_score,
        )

    @staticmethod
    def _build_intraday_arrays(ohlcv: List, code: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray, float]:
        """组合 StockDaily 历史数据 + RealtimeSpot 当天实时行情。"""
        highs = np.array([d.high for d in ohlcv], dtype=float)
        lows = np.array([d.low for d in ohlcv], dtype=float)
        closes = np.array([d.close for d in ohlcv], dtype=float)

        spot = StopLossCalculator._fetch_realtime_spot(code)
        if spot is None or not spot.price or spot.price <= 0:
            return highs, lows, closes, float(closes[-1])

        current_price = float(spot.price)
        today_high = float(spot.high) if spot.high and spot.high > 0 else current_price
        today_low = float(spot.low) if spot.low and spot.low > 0 else current_price

        # 检查最后一条 OHLCV 是否为当天（可能已有部分盘中数据）
        today_str = date.today().isoformat()
        if ohlcv and str(getattr(ohlcv[-1], 'date', '')) == today_str:
            highs[-1] = max(highs[-1], today_high)
            lows[-1] = min(lows[-1], today_low)
            closes[-1] = current_price
        else:
            highs = np.append(highs, today_high)
            lows = np.append(lows, today_low)
            closes = np.append(closes, current_price)

        return highs, lows, closes, current_price

    @staticmethod
    def _fetch_realtime_spot(code: str):
        from src.storage import DatabaseManager, RealtimeSpot

        db = DatabaseManager.get_instance()
        with db.get_session() as s:
            return s.query(RealtimeSpot).filter(RealtimeSpot.code == code).first()

    @staticmethod
    def _fetch_ohlcv(code: str, target_date: date) -> List:
        from src.storage import DatabaseManager

        db = DatabaseManager.get_instance()
        start = target_date - timedelta(days=DATA_LOOKBACK_DAYS)
        return db.get_data_range(code, start, target_date)

    @staticmethod
    def _fetch_tech_indicator(code: str, target_date: date):
        from src.storage import DatabaseManager

        db = DatabaseManager.get_instance()
        return db.get_tech_indicator(code, target_date)


def quick_stop_loss(code: str, trade_date: Optional[date] = None) -> StopLossResult:
    """快速计算止损位（一行调用）。"""
    return StopLossCalculator().compute(code, trade_date)
