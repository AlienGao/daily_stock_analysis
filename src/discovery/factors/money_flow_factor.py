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
    pre2 = codes.str[:2]
    suffixes = pd.Series("SZ", index=codes.index)
    suffixes[pre2.isin(["60", "68"])] = "SH"
    suffixes[pre2.isin(["43", "83", "87", "92"])] = "BJ"
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
    _LABEL_THRESHOLD = 25.0
    _STRONG = 0.75   # 百分位阈值：top 25% → 强势标签
    _MODERATE = 0.55  # top 45% → 偏多标签
    _WEAK = 0.25     # bottom 25% → 偏空标签

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

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取资金流向信号，返回信号名 → Series 的映射。
        所有比率信号做全市场百分位归一化（0-100），值越高越优。
        """
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

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

        def _to_pct(series: pd.Series) -> pd.Series:
            ranks = series.rank(pct=True, na_option="bottom")
            return (ranks * 100).clip(0, 100)

        major_rate = (elg_net + lg_net) / total_trade.replace(0, float("nan"))
        elg_rate = elg_net / total_trade.replace(0, float("nan"))
        lg_rate = lg_net / total_trade.replace(0, float("nan"))
        sm_rate = sm_net / total_trade.replace(0, float("nan"))

        return {
            "elg_net": elg_net, "lg_net": lg_net, "sm_net": sm_net,
            "major_rate": major_rate,
            "elg_rate": elg_rate, "lg_rate": lg_rate,
            "major_rate_pct": _to_pct(major_rate),
            "elg_rate_pct": _to_pct(elg_rate),
            "lg_rate_pct": _to_pct(lg_rate),
            "sm_rate_pct": _to_pct(sm_rate),
            "retail_trap": (elg_net < 0) & (sm_net > 0),
        }

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)

        scores = (
            signals["major_rate_pct"] * 0.40
            + signals["elg_rate_pct"] * 0.30
            + signals["lg_rate_pct"] * 0.20
        ) / 0.90

        # 散户接盘惩罚：小单净流入百分位越高、且特大单流出 → 惩罚越重
        sm_pct = signals["sm_rate_pct"] / 100
        penalty = 1.0 - 0.6 * sm_pct * signals["retail_trap"].astype(float)
        scores = scores * penalty

        scores.name = self.name
        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        p100 = 100.0

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue
            r = []

            major_pct = float(signals["major_rate_pct"].get(ts_code, 0))
            elg_pct = float(signals["elg_rate_pct"].get(ts_code, 0))
            lg_pct = float(signals["lg_rate_pct"].get(ts_code, 0))

            # 主力净流：阈值由 _STRONG/_MODERATE/_WEAK 决定，与百分位打分对齐
            if major_pct >= p100 * self._STRONG:
                r.append(f"主力净流入率超{major_pct:.0f}%股票")
            elif major_pct >= p100 * self._MODERATE:
                r.append("主力资金偏多")
            elif major_pct <= p100 * self._WEAK:
                r.append("主力资金偏空")

            # 特大单 vs 大单主导
            if elg_pct >= p100 * self._STRONG:
                r.append("特大单主导")
            elif lg_pct >= p100 * self._STRONG and elg_pct < p100 * self._MODERATE:
                r.append("大单资金活跃")

            if signals["retail_trap"].get(ts_code, False):
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

    # 过滤 trade_date 非法行
    out = out[out["trade_date"].notna() & (out["trade_date"].astype(str).str.match(r"^\d{8}$"))]
    if out.empty:
        return

    db = DatabaseManager()
    saved = db.upsert_money_flow(out, source="tushare")
    logger.info("[MoneyFlow] 自动落库 %d 条", saved)

