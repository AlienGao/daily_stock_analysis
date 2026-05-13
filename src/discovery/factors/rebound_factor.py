# -*- coding: utf-8 -*-
"""炸板回封因子 (Limit Break Rebound Factor).

盘中实时检测涨停打开（炸板）后的买点机会。
数据来源: limit_pool + limit_up_history（自算差集） + realtime_spot（行情） + money_flow（资金流）。
轮次间回封进度感知：追踪炸板股的涨幅回升速度，捕获「正在回封」的进程。
盘中可用，盘后不可用。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor, ts_code_to_bare, ts_codes_to_bare

logger = logging.getLogger(__name__)


def _to_bare_codes(index: pd.Index) -> pd.Index:
    """将 index 转为 6 位裸码，兼容 '600519' 和 '600519.SH' 两种格式。"""
    return ts_codes_to_bare(index)


def _linear_map(series: pd.Series, x0: float, y0: float,
                x1: float, y1: float, clip_low: float = 0.0,
                clip_high: float = 1e9) -> pd.Series:
    """两点线性映射，超出范围 clip。"""
    slope = (y1 - y0) / (x1 - x0) if x1 != x0 else 0.0
    return (y0 + slope * (series - x0)).clip(clip_low, clip_high)


class ReboundFactor(BaseFactor):
    """炸板回封因子。

    检测涨停打开后跌幅收窄、有大单回补的短线买点。
    自算炸板差集（limit_up_history - limit_pool），不依赖 limit_break 中间表。
    专用回封进度钩子：追踪炸板股涨幅回升速度，捕获「正在回封」进程。
    """

    name = "rebound"
    available_intraday = True
    available_postmarket = False
    weight = 20.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def __init__(self):
        super().__init__()
        self._prev_pct_chg: Dict[str, float] = {}          # {bare_code: pct_chg} 上一轮快照
        self._rebound_trade_date: Optional[str] = None     # 跨日重置
        self._cached_seal: Dict[str, Dict[str, float]] = {}  # {bare: {speed, distance, score}}

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """自算炸板差集：limit_up_history - limit_pool → 炸板候选，合并行情+资金流。

        不再依赖 limit_break 表（scanner 差集检测的中间产物），直接从两张源表读差集，
        砍掉中间环节，消除 scanner._detect_limit_breaks() 失败导致的因子数据缺失。
        """
        from datetime import date
        from src.storage import DatabaseManager

        db = DatabaseManager()
        today = date.today().strftime("%Y%m%d") if not trade_date else trade_date

        pool = db.get_limit_pool(trade_date=today)
        hist = db.get_limit_up_history(trade_date=today)

        if hist is None or hist.empty:
            logger.debug("[ReboundFactor] limit_up_history 无数据")
            return None
        if pool is None or pool.empty:
            logger.debug("[ReboundFactor] limit_pool 无数据")
            return None

        hist_codes = set(hist.index.astype(str).str.strip().str.zfill(6))
        pool_codes = set(pool.index.astype(str).str.strip().str.zfill(6))

        # 差集：曾涨停但当前不在 → 炸板
        broke_codes = hist_codes - pool_codes

        # Z型补充：仍在池中但 limit_type='Z'（Tushare 数据特有列）
        if "limit_type" in pool.columns:
            z_mask = pool["limit_type"] == "Z"
            if z_mask.any():
                z_codes = set(pool.loc[z_mask].index.astype(str).str.strip().str.zfill(6))
                broke_codes |= z_codes

        if not broke_codes:
            logger.debug("[ReboundFactor] 当前无炸板股票")
            return None

        # 从 limit_up_history 带出 metadata（open_times / limit_times / sector / name）
        hist["_bare"] = hist.index.astype(str).str.strip().str.zfill(6)
        result = hist[hist["_bare"].isin(broke_codes)].copy()
        result = result.drop(columns=["_bare"])
        keep_cols = [c for c in ["name", "open_times", "limit_times", "sector"] if c in result.columns]
        result = result[keep_cols]

        logger.info("[ReboundFactor] 自算差集检测到 %d 只炸板股票", len(result))

        # 合并 realtime_spot
        try:
            spot = db.get_realtime_spot()
            if spot is not None and not spot.empty:
                df_bare = _to_bare_codes(result.index)
                spot_bare = _to_bare_codes(spot.index)
                for col in ["pct_chg", "volume_ratio", "turnover_rate", "price"]:
                    if col in spot.columns:
                        s = spot[col].copy()
                        s.index = spot_bare
                        result[col] = df_bare.map(s)
        except Exception as e:
            logger.warning("[ReboundFactor] realtime_spot 合并失败: %s", e)

        # 合并 money_flow
        try:
            from src.discovery.money_flow_source import fetch_intraday_money_flow
            mf = fetch_intraday_money_flow(trade_date, kwargs.get("tushare_fetcher"))
            if mf is not None and not mf.empty:
                df_bare = _to_bare_codes(result.index)
                mf_bare = _to_bare_codes(mf.index)
                inflow = mf.get("inflow_rate", pd.Series(0.0, index=mf.index))
                inflow.index = mf_bare
                result["inflow_rate"] = df_bare.map(inflow).fillna(0)
            else:
                result["inflow_rate"] = 0
        except Exception as e:
            logger.warning("[ReboundFactor] money_flow 获取失败: %s", e)
            result["inflow_rate"] = 0

        return result

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 6 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        pct_chg = df.get("pct_chg", zeros)
        inflow_rate = df.get("inflow_rate", zeros)
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=idx))
        turnover_rate = df.get("turnover_rate", zeros)
        open_times = df.get("open_times", pd.Series(0, index=idx))
        limit_times = df.get("limit_times", pd.Series(0, index=idx))

        signals: Dict[str, pd.Series] = {}

        # 1. 跌幅承接力 (0-25)：浅跌 > 深度回撤
        s_pct = zeros.copy()
        s_pct = s_pct.mask(pct_chg > -2, 25.0)
        s_pct = s_pct.mask((pct_chg >= -3) & (pct_chg <= -2),
                           _linear_map(pct_chg, -3, 18, -2, 25))
        s_pct = s_pct.mask((pct_chg >= -5) & (pct_chg < -3),
                           _linear_map(pct_chg, -5, 8, -3, 18))
        s_pct = s_pct.mask((pct_chg >= -7) & (pct_chg < -5),
                           _linear_map(pct_chg, -7, 0, -5, 8))
        signals["pct_chg"] = s_pct

        # 2. 资金回补 (0-30)：大单净流入比例
        s_ir = zeros.copy()
        s_ir = s_ir.mask(inflow_rate > 0.08, 30.0)
        s_ir = s_ir.mask((inflow_rate >= 0.03) & (inflow_rate <= 0.08),
                         _linear_map(inflow_rate, 0.03, 20, 0.08, 30))
        s_ir = s_ir.mask((inflow_rate > 0) & (inflow_rate < 0.03),
                         _linear_map(inflow_rate, 0, 5, 0.03, 20))

        # 量价确认：涨+放量+净流入 → 真回封，资金分放大 1.5x
        if self._prev_pct_chg:
            bare_from_ts = _to_bare_codes(idx)
            prev_s = pd.Series(self._prev_pct_chg)
            prev_aligned = bare_from_ts.map(prev_s)
            pct_rising = (pct_chg > prev_aligned).fillna(False)
            vol_confirm = volume_ratio > 1.0
            s_ir = s_ir.mask(pct_rising & vol_confirm & (inflow_rate > 0), s_ir * 1.5)

        signals["inflow"] = s_ir

        # 3. 放量承接 (0-15)
        s_vr = zeros.copy()
        s_vr = s_vr.mask(volume_ratio > 2.0, 15.0)
        s_vr = s_vr.mask((volume_ratio >= 1.2) & (volume_ratio <= 2.0),
                         _linear_map(volume_ratio, 1.2, 8, 2.0, 15))
        s_vr = s_vr.mask((volume_ratio >= 0.8) & (volume_ratio < 1.2),
                         _linear_map(volume_ratio, 0.8, 3, 1.2, 8))
        signals["volume_ratio"] = s_vr

        # 4. 换手活跃 (0-10)
        s_tr = zeros.copy()
        s_tr = s_tr.mask((turnover_rate >= 3) & (turnover_rate <= 10), 10.0)
        s_tr = s_tr.mask((turnover_rate > 10) & (turnover_rate <= 15),
                         _linear_map(turnover_rate, 10, 10, 15, 3))
        s_tr = s_tr.mask((turnover_rate >= 1) & (turnover_rate < 3),
                         _linear_map(turnover_rate, 1, 0, 3, 10))
        signals["turnover"] = s_tr

        # 5. 分歧程度 (0-10)：离散值，1 次最优
        s_ot = zeros.copy()
        s_ot = s_ot.mask(open_times == 1, 10.0)
        s_ot = s_ot.mask(open_times == 2, 5.0)
        s_ot = s_ot.mask(open_times == 3, 2.0)
        signals["open_times"] = s_ot

        # 6. 连板位置 (0-10)：递减，高位炸板风险大
        s_lt = zeros.copy()
        s_lt = s_lt.mask(limit_times == 1, 10.0)
        s_lt = s_lt.mask(limit_times == 2, 7.0)
        s_lt = s_lt.mask(limit_times == 3, 3.0)
        signals["limit_times"] = s_lt

        return signals

    # ------------------------------------------------------------------
    # 专用回封进度钩子（非通用 delta）
    # ------------------------------------------------------------------

    def _compute_seal_progress(self, df: pd.DataFrame,
                               trade_date: str) -> pd.Series:
        """追踪炸板股的回封进展：涨幅回升速度 + 距涨停距离。

        不套通用 delta 框架，针对回封场景定制：
        - 回封速度（seal_speed）：pct_chg 轮次间变化，正=在回封
        - 回封距离（seal_distance）：10% - pct_chg，越近越好
        - 综合评分：速度分 × 距离衰减，快速逼近涨停 = 最高分

        Returns per-stock seal_progress bonus (0 ~ +15).
        """
        idx = df.index
        bonus = pd.Series(0.0, index=idx)

        # 跨日重置
        if self._rebound_trade_date != trade_date:
            self._prev_pct_chg.clear()
            self._rebound_trade_date = trade_date

        pct_chg = df.get("pct_chg", pd.Series(0.0, index=idx))
        prev_map = self._prev_pct_chg
        new_map: Dict[str, float] = {}
        cached: Dict[str, Dict[str, float]] = {}

        for ts_code in idx:
            bare = ts_code_to_bare(str(ts_code))
            cur = float(pct_chg.get(ts_code, 0))
            new_map[bare] = cur

            prev_val = prev_map.get(bare)
            if prev_val is None:
                cached[bare] = {"speed": 0.0, "distance": 0.0, "score": 0.0}
                continue

            # 回封速度：涨幅回升幅度
            speed = cur - prev_val  # 正=在回封，负=继续走弱

            # 回封距离：距涨停板剩余空间（0=已封板, 10=刚开盘价）
            distance = max(0.0, 10.0 - cur)

            # 速度分 (0-8)：快速回升 > 缓慢回升 > 走弱
            if speed > 1.5:
                speed_score = 8.0
            elif speed > 0.5:
                speed_score = 5.0 + (speed - 0.5) * 3.0  # 5~8
            elif speed > 0:
                speed_score = speed * 10.0  # 0~5
            elif speed > -1.0:
                speed_score = speed * 3.0   # -3~0（轻微走弱）
            else:
                speed_score = -3.0          # 明显走弱

            # 距离分 (0-7)：越接近涨停越好
            if distance < 1.0:
                dist_score = 7.0
            elif distance < 2.0:
                dist_score = 5.0 + (2.0 - distance) * 2.0  # 5~7
            elif distance < 3.0:
                dist_score = 3.0 + (3.0 - distance) * 2.0  # 3~5
            elif distance < 5.0:
                dist_score = 1.0 + (5.0 - distance) * 1.0  # 1~3
            else:
                dist_score = 0.0  # 距离太远，回封希望渺茫

            # 综合评分 (0-15)：速度 × 距离因子
            composite = speed_score + dist_score
            composite = max(-3.0, min(15.0, composite))

            bonus.loc[ts_code] = composite
            cached[bare] = {
                "speed": round(speed, 2),
                "distance": round(distance, 2),
                "score": round(composite, 2),
            }

        self._prev_pct_chg = new_map
        self._cached_seal = cached
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

        # 回封进度溢价（0-15，轮次间涨幅回升 + 距涨停距离）
        seal_progress = self._compute_seal_progress(df, trade_date)
        total = total + seal_progress

        pct_chg = df.get("pct_chg")
        turnover_rate = df.get("turnover_rate")
        if pct_chg is not None:
            total.loc[pct_chg < -7] = 0.0
        if turnover_rate is not None:
            total.loc[turnover_rate < 1] = 0.0

        total = total.clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        if df.empty:
            return {}

        signals = self._compute_signals(df)
        seal_cache = getattr(self, "_cached_seal", {}) or {}

        signal_meta = [
            ("pct_chg", "跌幅承接"),
            ("inflow", "资金回补"),
            ("volume_ratio", "放量承接"),
            ("turnover", "换手活跃"),
            ("open_times", "分歧"),
            ("limit_times", "连板"),
        ]
        max_map = {
            "pct_chg": 25, "inflow": 30, "volume_ratio": 15,
            "turnover": 10, "open_times": 10, "limit_times": 10,
        }
        threshold = self._LABEL_THRESHOLD_RATIO

        pct_r = df.get("pct_chg", pd.Series(0, index=df.index))
        ir_r = df.get("inflow_rate", pd.Series(0, index=df.index))
        vr_r = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        tr_r = df.get("turnover_rate", pd.Series(0, index=df.index))
        ot_r = df.get("open_times", pd.Series(0, index=df.index))
        lt_r = df.get("limit_times", pd.Series(0, index=df.index))

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            bare = ts_code_to_bare(str(ts_code))
            labels = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_map[key] * threshold:
                    continue
                if key == "pct_chg":
                    pct = pct_r.get(ts_code, 0)
                    labels.append(f"{label}(跌幅{pct:.1f}%)")
                elif key == "inflow":
                    ir = ir_r.get(ts_code, 0)
                    labels.append(f"{label}(流入率{ir*100:.1f}%)")
                elif key == "volume_ratio":
                    vr = vr_r.get(ts_code, 1)
                    labels.append(f"{label}(量比{vr:.1f})")
                elif key == "turnover":
                    tr = tr_r.get(ts_code, 0)
                    labels.append(f"{label}({tr:.1f}%)")
                elif key == "open_times":
                    ot = int(ot_r.get(ts_code, 0))
                    labels.append(f"{label}(开板{ot}次)")
                elif key == "limit_times":
                    lt = int(lt_r.get(ts_code, 0))
                    labels.append(f"{'首板' if lt <= 1 else f'{lt}板'}炸板")

            # 回封进度标签（速度阈值防抖：有实际涨跌变化才打标）
            seal = seal_cache.get(bare, {})
            seal_speed = seal.get("speed", 0.0)
            seal_score = seal.get("score", 0.0)
            if seal_score >= 3.0 and seal_speed > 0.1:
                labels.append(f"回封进行中(↑+{seal_speed:.1f}%)")
            elif seal_speed < -0.5:
                labels.append(f"回封受阻(↓{seal_speed:.1f}%)")

            if labels:
                reasons[ts_code] = labels
        return reasons
