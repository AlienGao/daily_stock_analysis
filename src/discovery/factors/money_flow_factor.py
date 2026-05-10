# -*- coding: utf-8 -*-
"""资金流向因子 (Money Flow Factor).

盘后因子：全市场资金流向分析，识别主力建仓和散户接盘。
数据来源: DB (money_flow 表) > Tushare moneyflow (170)
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _bare_to_ts_code(codes: pd.Index) -> pd.Index:
    """将 6 位代码转为 ts_code 格式 (000001 → 000001.SZ)。"""
    codes = codes.astype(str).str.zfill(6)
    suffix_map = {
        "6": "SH", "9": "SH",
        "0": "SZ", "1": "SZ", "2": "SZ", "3": "SZ", "5": "SZ",
        "4": "BJ", "8": "BJ",
    }
    suffixes = codes.str[0].map(suffix_map).fillna("SZ")
    return codes + "." + suffixes


class MoneyFlowFactor(BaseFactor):
    """资金流向因子。

    基于特大单/大单/中单/小单买卖数据，判断资金结构与方向。
    采用百分位评分：主力净流入率、特大单净率、大单净率各自在全市场中排名。
    散户接盘（特大单流出+小单流入）施加乘性惩罚。
    """

    name = "money_flow"
    available_intraday = False
    available_postmarket = True
    weight = 25.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 DB money_flow 表，无数据时回退 Tushare API。"""
        # 1) 先查 DB
        try:
            from src.storage import DatabaseManager

            df = DatabaseManager().get_money_flow(trade_date)
            if df is not None and not df.empty:
                df.index = _bare_to_ts_code(df.index)
                return df
        except Exception as e:
            logger.debug("[MoneyFlow] DB 读取失败，回退 API: %s", e)

        # 2) DB 无数据，调 Tushare API
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        df = tushare_fetcher.get_bulk_money_flow(trade_date)
        if df is not None and not df.empty:
            try:
                _persist_to_db(df)
            except Exception as e:
                logger.debug("[MoneyFlow] 落库失败: %s", e)
            return df
        return None

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        buy_elg = pd.to_numeric(df.get("buy_elg_amount", 0), errors="coerce").fillna(0)
        sell_elg = pd.to_numeric(df.get("sell_elg_amount", 0), errors="coerce").fillna(0)
        buy_lg = pd.to_numeric(df.get("buy_lg_amount", 0), errors="coerce").fillna(0)
        sell_lg = pd.to_numeric(df.get("sell_lg_amount", 0), errors="coerce").fillna(0)
        buy_sm = pd.to_numeric(df.get("buy_sm_amount", 0), errors="coerce").fillna(0)
        sell_sm = pd.to_numeric(df.get("sell_sm_amount", 0), errors="coerce").fillna(0)

        elg_net = buy_elg - sell_elg
        lg_net = buy_lg - sell_lg
        sm_net = buy_sm - sell_sm
        total_trade = buy_elg + sell_elg + buy_lg + sell_lg + buy_sm + sell_sm

        def _pct_scores(series: pd.Series) -> pd.Series:
            """将序列转为 0-100 的百分位分数（值越大分越高）。"""
            ranks = series.rank(pct=True, na_option="bottom")
            return (ranks * 100).clip(0, 100)

        # a) 主力净流入率 = (特大单净 + 大单净) / 总成交额 — 权重 40%
        major_rate = (elg_net + lg_net) / total_trade.replace(0, float("nan"))
        # b) 特大单净流入率 — 权重 30%
        elg_rate = elg_net / total_trade.replace(0, float("nan"))
        # c) 大单净流入率 — 权重 20%
        lg_rate = lg_net / total_trade.replace(0, float("nan"))

        scores = (
            _pct_scores(major_rate) * 0.40
            + _pct_scores(elg_rate) * 0.30
            + _pct_scores(lg_rate) * 0.20
        ) / 0.90  # 归一化回 0-100

        # 散户接盘惩罚：特大单流出 + 小单流入占比高 → ×0.4~×1.0
        # 惩罚力度 = 小单净流入在全市场中的百分位 × 0.6
        sm_rate = sm_net / total_trade.replace(0, float("nan"))
        sm_pct = _pct_scores(sm_rate) / 100  # 0-1
        penalty = 1.0 - 0.6 * sm_pct * (elg_net < 0).astype(float)
        scores = scores * penalty

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        buy_elg = pd.to_numeric(df.get("buy_elg_amount", 0), errors="coerce").fillna(0)
        sell_elg = pd.to_numeric(df.get("sell_elg_amount", 0), errors="coerce").fillna(0)
        buy_lg = pd.to_numeric(df.get("buy_lg_amount", 0), errors="coerce").fillna(0)
        sell_lg = pd.to_numeric(df.get("sell_lg_amount", 0), errors="coerce").fillna(0)
        buy_sm = pd.to_numeric(df.get("buy_sm_amount", 0), errors="coerce").fillna(0)
        sell_sm = pd.to_numeric(df.get("sell_sm_amount", 0), errors="coerce").fillna(0)

        elg_net = buy_elg - sell_elg
        lg_net = buy_lg - sell_lg
        sm_net = buy_sm - sell_sm
        total_trade = buy_elg + sell_elg + buy_lg + sell_lg + buy_sm + sell_sm
        major_rate = (elg_net + lg_net) / total_trade.replace(0, float("nan"))

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if elg_net.get(ts_code, 0) > 0:
                r.append("特大单净流入")
            mf = major_rate.get(ts_code, 0)
            if pd.notna(mf) and mf > 0.10:
                r.append(f"主力净流入率{abs(mf)*100:.0f}%")
            if lg_net.get(ts_code, 0) > 0:
                r.append("大单净流入")
            if elg_net.get(ts_code, 0) < 0 and sm_net.get(ts_code, 0) > 0:
                r.append("散户接盘预警")
            if r:
                reasons[ts_code] = r
        return reasons


def _persist_to_db(df: pd.DataFrame) -> None:
    """将 Tushare 返回的 moneyflow DataFrame 写入 DB（API 回退时顺手落库）。"""
    from src.storage import DatabaseManager

    out = pd.DataFrame()
    out["code"] = df.index.astype(str).str.split(".").str[0].str.zfill(6)
    out["trade_date"] = df.get("trade_date", "")
    for c in ("buy_elg_amount", "sell_elg_amount", "buy_lg_amount",
              "sell_lg_amount", "buy_md_amount", "sell_md_amount",
              "buy_sm_amount", "sell_sm_amount", "net_mf_amount"):
        if c in df.columns:
            out[c] = pd.to_numeric(df[c], errors="coerce")

    db = DatabaseManager()
    saved = db.upsert_money_flow(out, source="tushare")
    logger.info("[MoneyFlow] 自动落库 %d 条", saved)

