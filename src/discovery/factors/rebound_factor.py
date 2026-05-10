# -*- coding: utf-8 -*-
"""炸板回封因子 (Limit Break Rebound Factor).

盘中实时检测涨停打开（炸板）后的买点机会。
数据来源: limit_break 表（scanner 差集检测） + realtime_spot（行情） + money_flow（资金流）。
盘中可用，盘后不可用。
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


class ReboundFactor(BaseFactor):
    """炸板回封因子。

    检测涨停打开后跌幅收窄、有大单回补的短线买点。
    数据由 scanner._detect_limit_breaks() 实时写入 limit_break 表。
    """

    name = "rebound"
    available_intraday = True
    available_postmarket = False
    weight = 15.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """从 limit_break 读取炸板中股票，合并 realtime_spot + money_flow。"""
        from datetime import date
        from src.storage import DatabaseManager

        db = DatabaseManager()
        today = date.today().strftime("%Y%m%d") if not trade_date else trade_date

        df = db.get_limit_break(trade_date=today, status="broke")
        if df is None or df.empty:
            logger.debug("[ReboundFactor] 当前无炸板股票")
            return None

        logger.info("[ReboundFactor] 当前 %d 只炸板股票", len(df))

        try:
            spot = db.get_realtime_spot()
            if spot is not None and not spot.empty:
                bare_codes = df.index.astype(str).str.strip().str.zfill(6)
                for col in ["pct_chg", "volume_ratio", "turnover_rate", "price"]:
                    if col in spot.columns:
                        df[col] = bare_codes.map(spot[col])
        except Exception as e:
            logger.warning("[ReboundFactor] realtime_spot 合并失败: %s", e)

        try:
            mf = db.get_money_flow(trade_date=today)
            if mf is not None and not mf.empty:
                bare_codes = df.index.astype(str).str.strip().str.zfill(6)
                for col in ["buy_elg_amount", "sell_elg_amount",
                            "buy_lg_amount", "sell_lg_amount"]:
                    if col in mf.columns:
                        df[col] = bare_codes.map(mf[col])
        except Exception as e:
            logger.warning("[ReboundFactor] money_flow 合并失败: %s", e)

        if all(c in df.columns for c in ["buy_elg_amount", "sell_elg_amount",
                                          "buy_lg_amount", "sell_lg_amount"]):
            buy = df["buy_elg_amount"].fillna(0) + df["buy_lg_amount"].fillna(0)
            sell = df["sell_elg_amount"].fillna(0) + df["sell_lg_amount"].fillna(0)
            total = buy + sell
            df["inflow_rate"] = ((buy - sell) / total.replace(0, float("nan"))).fillna(0)
        else:
            df["inflow_rate"] = 0

        return df

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
    # Score / Describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        total = sum(signals.values())

        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        total.loc[pct_chg < -7] = 0.0
        total.loc[turnover_rate < 1] = 0.0

        total = total.clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        if df.empty:
            return {}

        signals = self._compute_signals(df)

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
            if labels:
                reasons[ts_code] = labels
        return reasons
