# -*- coding: utf-8 -*-
"""强势启动因子 (Momentum / Breakout Factor).

在均线买点基础上叠加强势信号：资金流入、放量启动。
盘中 3 级数据源降级：东财 push2（实时全粒度）→ 同花顺（实时粗粒度）→ Tushare 资金流 + realtime_spot 实时指标（盘后兜底）。
轮次间动能加速感知：检测资金加速流入、量比放大、涨势增强的变化。
盘中可用，盘后不可用（盘后有独立的技术面因子）。
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _linear_map(series: pd.Series, x0: float, y0: float,
                x1: float, y1: float, clip_low: float = 0.0,
                clip_high: float = 1e9) -> pd.Series:
    """两点线性映射，超出范围 clip。"""
    slope = (y1 - y0) / (x1 - x0) if x1 != x0 else 0.0
    return (y0 + slope * (series - x0)).clip(clip_low, clip_high)


class MomentumFactor(BaseFactor):
    """强势启动因子。

    检测资金流入、量比放大、换手健康、涨幅温和的启动信号。
    排除主力净流出、换手过低、涨幅接近涨停。
    """

    name = "momentum"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def __init__(self):
        super().__init__()
        self._prev_momentum: Dict[str, Dict[str, float]] = {}  # {bare: {inflow_rate, volume_ratio, pct_chg}}
        self._momentum_trade_date: Optional[str] = None
        self._cached_mbuilding: Dict[str, Dict[str, float]] = {}  # {bare: {inflow_delta, vol_delta, pct_delta, score}}

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """委托共享模块进行 3 级降级拉取：东财 push2 → 同花顺 → Tushare。"""
        from src.discovery.money_flow_source import fetch_intraday_money_flow

        tushare_fetcher = kwargs.get("tushare_fetcher")
        df = fetch_intraday_money_flow(trade_date, tushare_fetcher)
        if df is not None and not df.empty:
            logger.info("[MomentumFactor] 获取 %d 条资金流数据", len(df))
        return df

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 4 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        inflow_rate = df.get("inflow_rate", zeros)
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=idx))
        turnover_rate = df.get("turnover_rate", zeros)
        pct_chg = df.get("pct_chg", zeros)

        signals: Dict[str, pd.Series] = {}

        # 1. 资金流入强度 (0-35)
        s_inflow = zeros.copy()
        s_inflow = s_inflow.mask(inflow_rate > 0.10, 35.0)
        s_inflow = s_inflow.mask((inflow_rate >= 0.03) & (inflow_rate <= 0.10),
                                 _linear_map(inflow_rate, 0.03, 17, 0.10, 35))
        s_inflow = s_inflow.mask((inflow_rate > 0) & (inflow_rate < 0.03),
                                 _linear_map(inflow_rate, 0, 0, 0.03, 17))
        signals["inflow"] = s_inflow

        # 2. 放量启动 (0-25)
        s_vol = zeros.copy()
        s_vol = s_vol.mask(volume_ratio > 2.5, 25.0)
        s_vol = s_vol.mask((volume_ratio >= 1.2) & (volume_ratio <= 2.5),
                           _linear_map(volume_ratio, 1.2, 12, 2.5, 25))
        s_vol = s_vol.mask((volume_ratio >= 0.8) & (volume_ratio < 1.2),
                           _linear_map(volume_ratio, 0.8, 4, 1.2, 12))
        signals["volume_ratio"] = s_vol

        # 3. 换手健康 (0-15)：3-10% 最优，向两侧衰减
        s_tr = zeros.copy()
        s_tr = s_tr.mask((turnover_rate >= 3) & (turnover_rate <= 10), 15.0)
        s_tr = s_tr.mask((turnover_rate > 10) & (turnover_rate <= 15),
                         _linear_map(turnover_rate, 10, 15, 15, 5))
        s_tr = s_tr.mask((turnover_rate >= 1) & (turnover_rate < 3),
                         _linear_map(turnover_rate, 1, 2, 3, 15))
        signals["turnover"] = s_tr

        # 4. 涨幅合理 (0-25)：2-5% 最优，温和启动不追高
        s_pct = zeros.copy()
        s_pct = s_pct.mask((pct_chg >= 2) & (pct_chg <= 5), 25.0)
        s_pct = s_pct.mask((pct_chg >= 0) & (pct_chg < 2),
                           _linear_map(pct_chg, 0, 6, 2, 25))
        s_pct = s_pct.mask((pct_chg > 5) & (pct_chg <= 7),
                           _linear_map(pct_chg, 5, 25, 7, 10))
        s_pct = s_pct.mask((pct_chg > 7) & (pct_chg <= 9),
                           _linear_map(pct_chg, 7, 10, 9, 3))
        signals["pct_chg"] = s_pct

        return signals

    # ------------------------------------------------------------------
    # 动能加速感知钩子（非通用 delta）
    # ------------------------------------------------------------------

    def _compute_momentum_building(self, df: pd.DataFrame,
                                   trade_date: str) -> pd.Series:
        """检测轮次间动能加速：资金流入加速、量比放大、涨势增强。

        不套通用 delta 框架，针对强势启动场景定制：
        - 资金加速 (0-10)：inflow_rate 轮次间变化
        - 量能扩张 (0-5)：volume_ratio 轮次间变化
        - 涨势增强 (0-5)：pct_chg 轮次间变化（温和区间内）

        Returns per-stock momentum_building bonus (0 ~ +20).
        """
        idx = df.index
        bonus = pd.Series(0.0, index=idx)

        # 跨日重置
        if self._momentum_trade_date != trade_date:
            self._prev_momentum.clear()
            self._momentum_trade_date = trade_date

        inflow_rate = df.get("inflow_rate", pd.Series(0.0, index=idx))
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=idx))
        pct_chg = df.get("pct_chg", pd.Series(0.0, index=idx))

        prev_map = self._prev_momentum
        new_map: Dict[str, Dict[str, float]] = {}
        cached: Dict[str, Dict[str, float]] = {}

        for ts_code in idx:
            bare = str(ts_code).split(".")[0] if "." in str(ts_code) else str(ts_code).strip().zfill(6)

            cur_ir = float(inflow_rate.get(ts_code, 0))
            cur_vr = float(volume_ratio.get(ts_code, 1.0))
            cur_pct = float(pct_chg.get(ts_code, 0))

            new_map[bare] = {"inflow_rate": cur_ir, "volume_ratio": cur_vr, "pct_chg": cur_pct}

            prev = prev_map.get(bare)
            if prev is None:
                cached[bare] = {"inflow_delta": 0.0, "vol_delta": 0.0, "pct_delta": 0.0, "score": 0.0}
                continue

            # 1. 资金加速 (0-10)
            ir_delta = cur_ir - prev["inflow_rate"]
            if ir_delta > 0.03:
                ir_score = 10.0
            elif ir_delta > 0.01:
                ir_score = 5.0 + (ir_delta - 0.01) * 250.0  # 5~10
            elif ir_delta > 0:
                ir_score = ir_delta * 500.0  # 0~5
            elif ir_delta > -0.02:
                ir_score = ir_delta * 100.0  # -2~0
            else:
                ir_score = -2.0

            # 2. 量能扩张 (0-5)
            vr_delta = cur_vr - prev["volume_ratio"]
            if vr_delta > 0.5:
                vr_score = 5.0
            elif vr_delta > 0.2:
                vr_score = 2.0 + (vr_delta - 0.2) * 10.0  # 2~5
            elif vr_delta > 0:
                vr_score = vr_delta * 10.0  # 0~2
            elif vr_delta > -0.3:
                vr_score = vr_delta * 3.0   # -1~0
            else:
                vr_score = -1.0

            # 3. 涨势增强 (0-5)：只在温和区间(0-7%)内
            pct_delta = cur_pct - prev["pct_chg"]
            if 0 < cur_pct <= 7:
                if pct_delta > 1.0:
                    pct_score = 5.0
                elif pct_delta > 0.3:
                    pct_score = 2.0 + (pct_delta - 0.3) * 4.0  # 2~5
                elif pct_delta > 0:
                    pct_score = pct_delta * 6.0  # 0~2
                else:
                    pct_score = 0.0
            else:
                pct_score = 0.0  # 不在温和区间不追踪涨势

            composite = ir_score + vr_score + pct_score
            composite = max(-3.0, min(20.0, composite))

            bonus.loc[ts_code] = composite
            cached[bare] = {
                "inflow_delta": round(ir_delta, 3),
                "vol_delta": round(vr_delta, 2),
                "pct_delta": round(pct_delta, 2),
                "score": round(composite, 2),
            }

        self._prev_momentum = new_map
        self._cached_mbuilding = cached
        return bonus

    # ------------------------------------------------------------------
    # Score / Describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        trade_date = context.get("trade_date", "")
        signals = self._compute_signals(df)
        total = sum(signals.values())

        # 动能加速溢价（0-20，资金加速+量能扩张+涨势增强）
        mbuilding = self._compute_momentum_building(df, trade_date)
        total = total + mbuilding

        # 净流出惩罚
        inflow_rate = df.get("inflow_rate", pd.Series(0, index=df.index))
        total.loc[inflow_rate < 0] = (total - 10).clip(0, 100)

        # 否决项
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        total.loc[turnover_rate < 1] = 0.0
        total.loc[pct_chg > 9] = 0.0

        total = total.clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        if df.empty:
            return {}

        signals = self._compute_signals(df)
        mb_cache = getattr(self, "_cached_mbuilding", {}) or {}

        signal_meta = [
            ("inflow", "资金流入"),
            ("volume_ratio", "放量启动"),
            ("turnover", "换手活跃"),
            ("pct_chg", "温和启动"),
        ]
        max_map = {"inflow": 35, "volume_ratio": 25, "turnover": 15, "pct_chg": 25}
        threshold = self._LABEL_THRESHOLD_RATIO

        inflow_raw = df.get("inflow_rate", pd.Series(0, index=df.index))
        vol_r = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        tr_r = df.get("turnover_rate", pd.Series(0, index=df.index))
        pct_r = df.get("pct_chg", pd.Series(0, index=df.index))

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            bare = str(ts_code).split(".")[0] if "." in str(ts_code) else str(ts_code).strip().zfill(6)
            labels = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_map[key] * threshold:
                    continue
                if key == "inflow":
                    ir = inflow_raw.get(ts_code, 0)
                    labels.append(f"{label}(流入率{ir*100:.1f}%)")
                elif key == "volume_ratio":
                    vr = vol_r.get(ts_code, 1)
                    labels.append(f"{label}(量比{vr:.1f})")
                elif key == "turnover":
                    tr = tr_r.get(ts_code, 0)
                    labels.append(f"{label}({tr:.1f}%)")
                elif key == "pct_chg":
                    pct = pct_r.get(ts_code, 0)
                    labels.append(f"{label}(涨幅{pct:.1f}%)")

            # 动能加速标签
            mb = mb_cache.get(bare, {})
            if mb.get("score", 0) >= 5.0:
                parts = []
                if mb.get("inflow_delta", 0) > 0.01:
                    parts.append(f"资金加速↑")
                if mb.get("vol_delta", 0) > 0.2:
                    parts.append(f"量能放大↑")
                if mb.get("pct_delta", 0) > 0.3:
                    parts.append(f"涨势增强↑")
                if parts:
                    labels.append("+".join(parts))
            elif mb.get("score", 0) <= -2.0:
                labels.append("动能衰减↓")

            if labels:
                reasons[ts_code] = labels
        return reasons



