# -*- coding: utf-8 -*-
"""险资举牌因子 (Insider Buy Factor).

盘后因子：基于同花顺险资举牌数据，识别被大资金举牌的股票。
数据来源: akshare stock_rank_xzjp_ths()，落库 insider_buy 表。
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

    基于举牌增持比例 + 持股比例 + 公告时效性，梯度评分。
    被大资金举牌 = 明确的机构看多信号。
    """

    name = "insider_buy"
    available_intraday = False
    available_postmarket = True
    weight = 15.0
    _LABEL_THRESHOLD = 5.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取险资举牌数据：优先 DB，fallback 到 akshare 并落库。

        Returns:
            DataFrame index=ts_code, 列含 add_ratio/hold_ratio/announce_date/avg_price。
        """
        self._trade_date = trade_date
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

        # 落库（用原始列名，upsert 认中文列名）
        try:
            from src.storage import DatabaseManager
            db2 = DatabaseManager()
            db2.upsert_insider_buy(raw, source="akshare")
            logger.info(f"[InsiderBuy] 落库 {len(raw)} 条举牌事件")
        except Exception as e:
            logger.debug(f"[InsiderBuy] 落库失败: {e}")

        # 去掉重复股票，保留最新一条
        if "announce_date" in df.columns:
            df = df.sort_values("announce_date", ascending=False)
        df = df[~df.index.duplicated(keep="first")]
        return df

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """计算所有信号，返回信号名 → Series 的映射。"""
        signals: Dict[str, pd.Series] = {}
        if df.empty:
            return signals

        idx = df.index

        add_ratio = df.get("add_ratio", pd.Series(0.0, index=idx))
        if hasattr(add_ratio, 'fillna'):
            add_ratio = add_ratio.fillna(0).astype(float)
        signals["add_ratio"] = add_ratio

        hold_ratio = df.get("hold_ratio", pd.Series(0.0, index=idx))
        if hasattr(hold_ratio, 'fillna'):
            hold_ratio = hold_ratio.fillna(0).astype(float)
        signals["hold_ratio"] = hold_ratio

        # 公告时效性：距 trade_date 越近权重越高（0-25 分，90 天内线性衰减）
        recency = pd.Series(0.0, index=idx)
        announce_col = df.get("announce_date")
        if announce_col is not None:
            td_str = getattr(self, "_trade_date", "")
            td_clean = str(td_str).replace("-", "")[:8] if td_str else ""
            if len(td_clean) == 8:
                today = datetime.strptime(td_clean, "%Y%m%d")
            else:
                today = datetime.now()
            for i, ts in enumerate(idx):
                d_str = str(announce_col.iloc[i] if hasattr(announce_col, 'iloc') else announce_col.get(ts, ""))
                try:
                    d = datetime.strptime(d_str[:10], "%Y-%m-%d")
                    days = (today - d).days
                    recency.iloc[i] = max(0, 1 - days / 90) * 25.0
                except (ValueError, KeyError):
                    pass
        signals["recency"] = recency

        # 有交易均价说明是近期有实质成交的举牌
        avg_price = df.get("avg_price", pd.Series(0.0, index=idx))
        has_price = pd.Series(0.0, index=idx)
        if hasattr(avg_price, 'fillna'):
            avg_price = avg_price.fillna(0).astype(float)
        has_price[avg_price > 0] = 10.0
        signals["has_price"] = has_price

        return signals

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        signals = self._compute_signals(df)
        if not signals:
            return scores

        add_ratio = signals.get("add_ratio", pd.Series(0.0, index=df.index))
        hold_ratio = signals.get("hold_ratio", pd.Series(0.0, index=df.index))
        recency = signals.get("recency", pd.Series(0.0, index=df.index))
        has_price = signals.get("has_price", pd.Series(0.0, index=df.index))

        # 增持比例梯度：0% → 0 分，5%+ → 50 分（线性）
        add_score = (add_ratio.clip(0, 5) / 5 * 50).fillna(0)
        scores = scores + add_score

        # 持股比例梯度：0% → 0 分，10%+ → 25 分（线性）
        hold_score = (hold_ratio.clip(0, 10) / 10 * 25).fillna(0)
        scores = scores + hold_score

        # 公告时效性：90 天内线性衰减，越近越高
        scores = scores + recency.fillna(0)

        # 有实质成交
        scores = scores + has_price.fillna(0)

        scores = scores.clip(0, 100)
        scores.name = self.name
        return scores

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        if not signals:
            return reasons

        add_ratio = signals.get("add_ratio", pd.Series(0.0, index=df.index))
        hold_ratio = signals.get("hold_ratio", pd.Series(0.0, index=df.index))
        recency = signals.get("recency", pd.Series(0.0, index=df.index))

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue
            r = []
            ar = float(add_ratio.get(ts_code, 0))
            hr = float(hold_ratio.get(ts_code, 0))
            rc = float(recency.get(ts_code, 0))

            if ar >= 4:
                r.append(f"大比例举牌增持{ar:.1f}%，强机构认可")
            elif ar >= 1:
                r.append(f"举牌增持{ar:.1f}%，增量资金入场")
            elif ar > 0:
                r.append(f"小额增持{ar:.2f}%")

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
