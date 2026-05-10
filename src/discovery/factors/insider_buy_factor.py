# -*- coding: utf-8 -*-
"""险资举牌因子 (Insider Buy Factor).

盘后因子：基于同花顺险资举牌数据，识别被大资金举牌的股票。
数据来源: akshare stock_rank_xzjp_ths()，落库 insider_buy 表。

同一股票多次举牌时保留全部事件，聚合后提取峰值增持 + 持续买入信号。
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class InsiderBuyFactor(BaseFactor):
    """险资举牌因子。

    基于举牌增持比例 + 持股比例 + 公告时效性 + 持续买入，梯度评分。
    被大资金举牌 = 明确的机构看多信号。
    """

    name = "insider_buy"
    available_intraday = False
    available_postmarket = True
    weight = 15.0
    _LABEL_THRESHOLD = 5.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取险资举牌数据：优先 DB，fallback 到 akshare 并落库。

        保留全部历史事件（不去重），由 _aggregate() 统一处理。
        Returns:
            DataFrame index=ts_code, 列含 add_ratio/hold_ratio/announce_date/avg_price。
        """
        # 1. 尝试从 DB 读
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df = db.get_insider_buy_recent(months=6)
            if not df.empty:
                logger.info(
                    f"[InsiderBuy] DB 命中: {len(df)} 条举牌事件"
                )
                return df
        except Exception as e:
            logger.debug(f"[InsiderBuy] DB 查询失败: {e}")

        # 2. Fallback 到 akshare
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None

        raw = akshare_fetcher.get_insider_buy()
        if raw is None or raw.empty:
            return None

        # 映射列名
        col_map = {
            "股票简称": "stock_name", "举牌公告日": "announce_date",
            "举牌方": "buyer", "增持数量": "buy_shares",
            "交易均价": "avg_price", "增持数量占总股本比例": "add_ratio",
            "变动后持股总数": "hold_shares", "变动后持股比例": "hold_ratio",
        }
        df = raw.rename(columns={k: v for k, v in col_map.items() if k in raw.columns})
        for c in ["add_ratio", "hold_ratio", "avg_price"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # 确保 ts_code 在列中（用作聚合 key）
        if "ts_code" not in df.columns:
            df = df.reset_index()

        # 落库（用原始列名，upsert 认中文列名）
        try:
            from src.storage import DatabaseManager
            db2 = DatabaseManager()
            db2.upsert_insider_buy(raw, source="akshare")
            logger.info(f"[InsiderBuy] 落库 {len(raw)} 条举牌事件")
        except Exception as e:
            logger.debug(f"[InsiderBuy] 落库失败: {e}")

        return df

    # ------------------------------------------------------------------
    # 聚合：多事件 → 单行信号
    # ------------------------------------------------------------------

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """按 ts_code 聚合多次举牌事件为单行信号。

        返回 DataFrame index=ts_code：
        - add_ratio_peak: 最大单次增持比例
        - add_ratio_cumul: 累计增持比例
        - hold_ratio: 最新变动后持股比例
        - event_count: 举牌次数
        - last_announce_date: 最新公告日
        - has_price: 是否有过交易均价
        """
        if "ts_code" not in df.columns:
            df = df.reset_index()
        # reset_index 后列名可能为 "index"，统一映射为 ts_code
        if "ts_code" not in df.columns and "index" in df.columns:
            df = df.rename(columns={"index": "ts_code"})

        # 按公告日升序排列，确保 last=最新
        if "announce_date" in df.columns:
            df = df.sort_values("announce_date")

        agg: Dict[str, object] = {
            "add_ratio": ("add_ratio", "max"),
            "add_ratio_cumul": ("add_ratio", "sum"),
            "hold_ratio": ("hold_ratio", "last"),
            "event_count": ("add_ratio", "count"),
            "announce_date": ("announce_date", "max"),
            "avg_price": ("avg_price", "max"),
        }
        # 只对存在的列聚合
        available_cols = {k: v for k, v in agg.items() if v[0] in df.columns}
        grouped = df.groupby("ts_code").agg(**{
            k: (v[0], v[1]) for k, v in available_cols.items()
        })

        grouped = grouped.rename(columns={"add_ratio": "add_ratio_peak"})
        return grouped

    # ------------------------------------------------------------------
    # 信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame, trade_date: str = "") -> Dict[str, pd.Series]:
        """计算所有信号，返回信号名 → Series 的映射。"""
        signals: Dict[str, pd.Series] = {}
        if df.empty:
            return signals

        idx = df.index

        add_peak = df.get("add_ratio_peak", df.get("add_ratio", pd.Series(0.0, index=idx)))
        if hasattr(add_peak, 'fillna'):
            add_peak = add_peak.fillna(0).astype(float)
        signals["add_ratio_peak"] = add_peak

        hold_ratio = df.get("hold_ratio", pd.Series(0.0, index=idx))
        if hasattr(hold_ratio, 'fillna'):
            hold_ratio = hold_ratio.fillna(0).astype(float)
        signals["hold_ratio"] = hold_ratio

        event_count = df.get("event_count", pd.Series(1, index=idx))
        if hasattr(event_count, 'fillna'):
            event_count = event_count.fillna(1).astype(int)
        signals["event_count"] = event_count

        add_cumul = df.get("add_ratio_cumul", add_peak)
        if hasattr(add_cumul, 'fillna'):
            add_cumul = add_cumul.fillna(0).astype(float)
        signals["add_ratio_cumul"] = add_cumul

        # 公告时效性：距 trade_date 越近权重越高（0-20 分，90 天内线性衰减）
        recency = pd.Series(0.0, index=idx)
        announce_col = df.get("announce_date")
        td_clean = str(trade_date).replace("-", "")[:8] if trade_date else ""
        if announce_col is not None and len(td_clean) == 8:
            today = datetime.strptime(td_clean, "%Y%m%d")
            for i, ts in enumerate(idx):
                d_str = str(announce_col.iloc[i] if hasattr(announce_col, 'iloc') else announce_col.get(ts, ""))
                try:
                    d_str_clean = d_str.replace("-", "")[:8]
                    d = datetime.strptime(d_str_clean, "%Y%m%d")
                    days = (today - d).days
                    recency.iloc[i] = max(0, 1 - days / 90) * 20.0
                except (ValueError, KeyError):
                    pass
        signals["recency"] = recency

        # 有交易均价说明是近期有实质成交的举牌
        avg_price = df.get("avg_price", pd.Series(0.0, index=idx))
        has_price = pd.Series(0.0, index=idx)
        if hasattr(avg_price, 'fillna'):
            avg_price = avg_price.fillna(0).astype(float)
        has_price[avg_price > 0] = 5.0
        signals["has_price"] = has_price

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        per_stock = self._aggregate(df)
        self._per_stock = per_stock  # 供 describe 复用

        trade_date = context.get("trade_date", "")
        signals = self._compute_signals(per_stock, trade_date=trade_date)
        if not signals:
            return pd.Series(0.0, index=per_stock.index, name=self.name)

        idx = per_stock.index
        scores = pd.Series(0.0, index=idx)

        add_peak = signals.get("add_ratio_peak", pd.Series(0.0, index=idx))
        hold_ratio = signals.get("hold_ratio", pd.Series(0.0, index=idx))
        recency = signals.get("recency", pd.Series(0.0, index=idx))
        has_price = signals.get("has_price", pd.Series(0.0, index=idx))
        event_count = signals.get("event_count", pd.Series(1, index=idx))

        # 增持峰值梯度：0% → 0 分，5%+ → 50 分（线性）
        add_score = (add_peak.clip(0, 5) / 5 * 50).fillna(0)
        scores = scores + add_score

        # 持股比例梯度：0% → 0 分，10%+ → 25 分（线性）
        hold_score = (hold_ratio.clip(0, 10) / 10 * 25).fillna(0)
        scores = scores + hold_score

        # 公告时效性：90 天内线性衰减（0-20）
        scores = scores + recency.fillna(0)

        # 有实质成交（0-5）
        scores = scores + has_price.fillna(0)

        # 持续增持奖励（多次买入是更强的信号）
        scores.loc[event_count >= 2] += 5.0
        scores.loc[event_count >= 4] += 5.0

        scores = scores.clip(0, 100)
        scores.name = self.name
        return scores

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        per_stock = getattr(self, "_per_stock", None)
        if per_stock is None:
            per_stock = self._aggregate(df)

        trade_date = context.get("trade_date", "")
        signals = self._compute_signals(per_stock, trade_date=trade_date)
        if not signals:
            return reasons

        add_peak = signals.get("add_ratio_peak", pd.Series(0.0, index=per_stock.index))
        hold_ratio = signals.get("hold_ratio", pd.Series(0.0, index=per_stock.index))
        recency = signals.get("recency", pd.Series(0.0, index=per_stock.index))
        event_count = signals.get("event_count", pd.Series(1, index=per_stock.index))
        add_cumul = signals.get("add_ratio_cumul", pd.Series(0.0, index=per_stock.index))

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue
            r = []
            ar = float(add_peak.get(ts_code, 0))
            hr = float(hold_ratio.get(ts_code, 0))
            rc = float(recency.get(ts_code, 0))
            ec = int(event_count.get(ts_code, 1))
            ac = float(add_cumul.get(ts_code, 0))

            if ar >= 4:
                r.append(f"大比例举牌增持{ar:.1f}%，强机构认可")
            elif ar >= 1:
                r.append(f"举牌增持{ar:.1f}%，增量资金入场")
            elif ar > 0:
                r.append(f"小额增持{ar:.2f}%")

            if ec >= 4:
                r.append(f"持续增持（{ec}次，累计+{ac:.1f}%）")
            elif ec >= 2:
                r.append(f"多次增持（{ec}次，累计+{ac:.1f}%）")

            if hr >= 8:
                r.append(f"持股比例高({hr:.1f}%)，长期看好")
            elif hr >= 3:
                r.append(f"持股{hr:.1f}%，有配置价值")

            if rc > 15:
                r.append("近期举牌，信号时效性强")
            elif rc > 5:
                r.append("近期有举牌动作")

            if r:
                reasons[ts_code] = r
        return reasons
