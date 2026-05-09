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


class ReboundFactor(BaseFactor):
    """炸板回封因子。

    检测涨停打开后跌幅收窄、有大单回补的短线买点。
    数据由 scanner._detect_limit_breaks() 实时写入 limit_break 表。
    """

    name = "rebound"
    available_intraday = True
    available_postmarket = False
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """从 limit_break 读取炸板中股票，合并 realtime_spot + money_flow。"""
        from datetime import date
        from src.storage import DatabaseManager

        db = DatabaseManager()
        today = date.today().strftime("%Y%m%d") if not trade_date else trade_date

        # 1) 读取炸板中股票（status=broke）
        df = db.get_limit_break(trade_date=today, status="broke")
        if df is None or df.empty:
            logger.debug("[ReboundFactor] 当前无炸板股票")
            return None

        logger.info("[ReboundFactor] 当前 %d 只炸板股票", len(df))

        # 2) 合并 realtime_spot：当前行情（pct_chg, volume_ratio, turnover_rate, price）
        try:
            spot = db.get_realtime_spot()
            if spot is not None and not spot.empty:
                bare_codes = df.index.astype(str).str.strip().str.zfill(6)
                for col in ["pct_chg", "volume_ratio", "turnover_rate", "price"]:
                    if col in spot.columns:
                        df[col] = bare_codes.map(spot[col])
        except Exception as e:
            logger.warning("[ReboundFactor] realtime_spot 合并失败: %s", e)

        # 3) 合并 money_flow：大单资金流
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

        # 4) 计算 inflow_rate
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
    # Score / Describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        inflow_rate = df.get("inflow_rate", pd.Series(0, index=df.index))
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        open_times = df.get("open_times", pd.Series(0, index=df.index))
        limit_times = df.get("limit_times", pd.Series(0, index=df.index))

        # ── 跌幅分档（炸板后承接力） ──
        # > -3%：浅跌，承接强 (+25)
        scores.loc[pct_chg > -3] += 25.0
        # -5% ~ -3%：中度回撤 (+15)
        scores.loc[(pct_chg >= -5) & (pct_chg <= -3)] += 15.0
        # -7% ~ -5%：深度回撤 (+5)
        scores.loc[(pct_chg >= -7) & (pct_chg < -5)] += 5.0

        # ── 大单净流入（资金回补） ──
        scores.loc[inflow_rate > 0.05] += 25.0
        scores.loc[(inflow_rate > 0) & (inflow_rate <= 0.05)] += 15.0

        # ── 放量承接 ──
        scores.loc[volume_ratio > 1.5] += 10.0

        # ── 换手活跃 ──
        scores.loc[(turnover_rate >= 3) & (turnover_rate <= 15)] += 10.0

        # ── 炸板次数分档（分歧程度） ──
        scores.loc[open_times == 1] += 10.0
        scores.loc[open_times == 2] += 5.0

        # ── 连板数分档（高位炸板风险高，不加分） ──
        scores.loc[(limit_times >= 1) & (limit_times <= 2)] += 10.0
        scores.loc[(limit_times >= 3) & (limit_times <= 5)] += 5.0

        # ── 否决项 ──
        scores.loc[pct_chg < -7] = 0.0
        scores.loc[turnover_rate < 1] = 0.0
        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        inflow_rate = df.get("inflow_rate", pd.Series(0, index=df.index))
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        limit_times = df.get("limit_times", pd.Series(0, index=df.index))
        open_times = df.get("open_times", pd.Series(0, index=df.index))

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            _pct = pct_chg.get(ts_code, 0)
            if _pct > -3:
                r.append(f"浅跌承接(跌幅{_pct:.1f}%)")
            elif _pct >= -5:
                r.append(f"中度回撤(跌幅{_pct:.1f}%)")
            elif _pct >= -7:
                r.append(f"深度回撤(跌幅{_pct:.1f}%)")

            _ir = inflow_rate.get(ts_code, 0)
            if _ir > 0.05:
                r.append(f"强力回补(流入率{_ir*100:.1f}%)")
            elif _ir > 0:
                r.append(f"资金回补(流入率{_ir*100:.1f}%)")

            _vr = volume_ratio.get(ts_code, 1)
            if _vr > 1.5:
                r.append(f"放量承接(量比{_vr:.1f})")

            _tr = turnover_rate.get(ts_code, 0)
            if 3 <= _tr <= 15:
                r.append(f"换手活跃({_tr:.1f}%)")

            _ot = int(open_times.get(ts_code, 0))
            if _ot == 1:
                r.append(f"轻度分歧(开板{_ot}次)")
            elif _ot == 2:
                r.append(f"中度分歧(开板{_ot}次)")

            _lt = int(limit_times.get(ts_code, 0))
            if _lt == 1:
                r.append(f"首板炸板")
            elif _lt >= 2 and _lt <= 5:
                r.append(f"{_lt}板炸板")

            if r:
                reasons[ts_code] = r
        return reasons
