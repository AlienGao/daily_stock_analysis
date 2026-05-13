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
    weight = 5.0
    _LABEL_THRESHOLD = 5.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取险资举牌数据：优先 DB，fallback 到 akshare 并落库。

        保留全部历史事件（不去重），由 _aggregate() 统一处理。
        两路径统一返回：ts_code 为列、英文列名。
        """
        # 1. 尝试从 DB 读（index=ts_code → reset 为列）
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df = db.get_insider_buy_recent(months=6)
            if not df.empty:
                logger.info("[InsiderBuy] DB 命中: %d 条举牌事件", len(df))
                return df.reset_index()
        except Exception as e:
            logger.debug("[InsiderBuy] DB 查询失败: %s", e)

        # 2. Fallback 到 akshare，标准化为英文列名
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None

        raw = akshare_fetcher.get_insider_buy()
        if raw is None or raw.empty:
            return None

        df = self._normalize_columns(raw)
        if df.empty:
            return None

        try:
            from src.storage import DatabaseManager
            db2 = DatabaseManager()
            db2.upsert_insider_buy(df, source="akshare")
            logger.info("[InsiderBuy] 落库 %d 条举牌事件", len(df))
        except Exception as e:
            logger.debug("[InsiderBuy] 落库失败: %s", e)

        return df

    @staticmethod
    def _normalize_columns(raw: pd.DataFrame) -> pd.DataFrame:
        """中英文双查找标准化列名，与 upsert_insider_buy 保持一致。"""
        def _get(row, cn, en, default=""):
            v = row.get(cn)
            if pd.isna(v) or v == "":
                v = row.get(en, default)
            return v

        if "ts_code" not in raw.columns:
            raw = raw.reset_index()

        records = []
        for _, row in raw.iterrows():
            records.append({
                "ts_code": str(row.get("ts_code", "")).strip(),
                "stock_name": str(_get(row, "股票简称", "stock_name")).strip(),
                "announce_date": str(_get(row, "举牌公告日", "announce_date"))[:10].strip(),
                "buyer": str(_get(row, "举牌方", "buyer")).strip(),
                "buy_shares": pd.to_numeric(_get(row, "增持数量", "buy_shares"), errors="coerce"),
                "avg_price": pd.to_numeric(_get(row, "交易均价", "avg_price"), errors="coerce"),
                "add_ratio": pd.to_numeric(_get(row, "增持数量占总股本比例", "add_ratio"), errors="coerce"),
                "hold_shares": pd.to_numeric(_get(row, "变动后持股总数", "hold_shares"), errors="coerce"),
                "hold_ratio": pd.to_numeric(_get(row, "变动后持股比例", "hold_ratio"), errors="coerce"),
            })
        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # 举牌方类型识别
    # ------------------------------------------------------------------

    # 按优先级从高到低匹配，避免"保险股份有限公司"被"有限/股份"误判为产业资本
    _BUYER_RULES = [
        ("险资/社保", 8, ("保险", "人寿", "养老", "社保")),
        ("金融机构", 6, ("基金", "资管", "信托", "证券")),
        ("产业资本", 3, ("集团", "有限", "股份", "控股")),
        ("私募/PE",   2, ("合伙", "咨询", "私募")),
    ]

    @staticmethod
    def _classify_buyer(buyer_name: str):
        """返回 (类型标签, 加分值)。未识别返回 ("其他", 0)。"""
        if not buyer_name or not isinstance(buyer_name, str):
            return ("其他", 0)
        for label, bonus, keywords in InsiderBuyFactor._BUYER_RULES:
            for kw in keywords:
                if kw in buyer_name:
                    return (label, bonus)
        return ("其他", 0)

    # ------------------------------------------------------------------
    # 聚合：多事件 → 单行信号
    # ------------------------------------------------------------------

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """按 ts_code 聚合多次举牌事件为单行信号。"""
        if "ts_code" not in df.columns:
            df = df.reset_index()
            if "ts_code" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "ts_code"})
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
        if "buyer" in df.columns:
            agg["buyers"] = ("buyer", lambda x: "|".join(sorted(set(x.dropna()))))

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

        # 举牌方类型加分：取所有举牌方中最高分
        buyer_type = pd.Series(0.0, index=idx)
        buyers_col = df.get("buyers")
        if buyers_col is not None:
            for i, ts in enumerate(idx):
                buyers_str = str(buyers_col.iloc[i] if hasattr(buyers_col, 'iloc') else buyers_col.get(ts, ""))
                best = 0
                for name in buyers_str.split("|"):
                    _, bonus = self._classify_buyer(name.strip())
                    if bonus > best:
                        best = bonus
                buyer_type.iloc[i] = float(best)
        signals["buyer_type"] = buyer_type

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

        # 持续增持奖励：2-3次=5分, 4次+=10分
        event_bonus = np.where(event_count >= 4, 10, np.where(event_count >= 2, 5, 0))
        scores = scores + pd.Series(event_bonus, index=scores.index)

        # 举牌方类型加分：险资/社保 +8，金融 +6，产业 +3，私募 +2
        buyer_type = signals.get("buyer_type", pd.Series(0.0, index=idx))
        scores = scores + buyer_type.fillna(0)

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
        buyer_type = signals.get("buyer_type", pd.Series(0.0, index=per_stock.index))
        buyers_col = per_stock.get("buyers")

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

            bt = float(buyer_type.get(ts_code, 0))
            if bt >= 8:
                bs = str(buyers_col.loc[ts_code] if hasattr(buyers_col, 'loc') else buyers_col.get(ts_code, ""))
                r.append(f"险资/社保举牌（{bs}），最强机构信号")
            elif bt >= 6:
                bs = str(buyers_col.loc[ts_code] if hasattr(buyers_col, 'loc') else buyers_col.get(ts_code, ""))
                r.append(f"金融机构举牌（{bs}），专业资金认可")
            elif bt >= 3:
                r.append("产业资本举牌")
            elif bt >= 2:
                r.append("私募/PE举牌")

            if r:
                reasons[ts_code] = r
        return reasons
