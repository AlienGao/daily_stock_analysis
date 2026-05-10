# -*- coding: utf-8 -*-
"""游资因子 (Hot Money Factor).

盘后因子：基于 Tushare 游资每日交易明细，识别游资关注的股票。
数据来源: Tushare hm_detail (doc_id=312)

游资质量加权：通过 HmTracker 历史回测统计各游资 T+1 胜率，
将「游资家数」升级为「质量加权共识」，区分游资优劣。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor
from src.discovery.hm_tracker import HmTracker

logger = logging.getLogger(__name__)


class HotMoneyFactor(BaseFactor):
    """游资因子。

    基于 Tushare 游资每日明细（hm_detail），聚合后百分位评分。
    3 个子信号：净买入额(40%)、平均游资质量(30%)、买入强度(30%)。
    """

    name = "hot_money"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 DB 缓存，降级到 Tushare API。"""
        try:
            from src.storage import DatabaseManager
            df = DatabaseManager().get_hm_detail_by_date(trade_date)
            if df is not None and not df.empty:
                logger.info("[HotMoneyFactor] DB 命中: %d 条", len(df))
                return df
        except Exception as e:
            logger.debug("[HotMoneyFactor] DB 读取失败: %s", e)

        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        return tushare_fetcher.get_bulk_hm_detail(trade_date)

    # ------------------------------------------------------------------
    # 聚合 + 共享信号
    # ------------------------------------------------------------------

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """按 ts_code 聚合游资明细。"""
        return df.groupby("ts_code").agg(
            total_net=("net_amount", "sum"),
            hm_count=("hm_name", "nunique"),
            total_buy=("buy_amount", "sum"),
            total_sell=("sell_amount", "sum"),
            hm_names=("hm_name", lambda x: "|".join(sorted(set(x)))),
        )

    def _compute_signals(self, per_stock: pd.DataFrame) -> Dict[str, pd.Series]:
        """计算 3 个子信号，各自归一化到满分区间。

        - net (0-40)：净买入额百分位
        - quality (0-30)：平均游资质量百分位（按家数均分，区分质量 vs 数量）
        - intensity (0-30)：买入额占总成交比百分位
        """
        idx = per_stock.index
        total_volume = per_stock["total_buy"] + per_stock["total_sell"]

        def _pct(s: pd.Series) -> pd.Series:
            return s.rank(pct=True, na_option="bottom") * 100

        # 1. 净买入 (0-40)
        net_score = pd.Series(_pct(per_stock["total_net"]) * 0.40, index=idx)

        # 2. 游资平均质量 (0-30)
        quality_map = HmTracker.load_quality()
        avg_quality = []
        for names in per_stock["hm_names"]:
            scores_list = [quality_map.get(n, 0.5) for n in names.split("|")]
            avg_quality.append(sum(scores_list) / max(len(scores_list), 1))
        self._avg_quality = pd.Series(avg_quality, index=idx)
        quality_score = pd.Series(_pct(self._avg_quality) * 0.30, index=idx)

        # 3. 买入强度 (0-30)
        buy_ratio = per_stock["total_buy"] / total_volume.replace(0, float("nan"))
        intensity_score = pd.Series(_pct(buy_ratio) * 0.30, index=idx)

        return {
            "net": net_score,
            "quality": quality_score,
            "intensity": intensity_score,
        }

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        per_stock = self._aggregate(df)
        signals = self._compute_signals(per_stock)
        self._per_stock = per_stock  # 供 describe 复用

        total = sum(signals.values())
        penalty = np.where(per_stock["total_net"] < 0, 0.5, 1.0)
        total = total * pd.Series(penalty, index=per_stock.index)

        # 跌停股归零：游资在跌停日的参与不构成正面信号
        total = self._zero_limit_down(df, total)

        total = total.clip(0, 100)
        total.name = self.name
        return total

    def _zero_limit_down(self, df: pd.DataFrame, total: pd.Series) -> pd.Series:
        """通过 limit_pool 查询当日跌停股，将其得分归零。"""
        trade_date = None
        if "trade_date" in df.columns:
            raw = df["trade_date"].iloc[0]
            trade_date = pd.Timestamp(str(raw)).strftime("%Y%m%d")
        if trade_date is None:
            return total
        try:
            from src.storage import DatabaseManager
            lp = DatabaseManager().get_limit_pool(trade_date=trade_date)
            if lp is None or lp.empty:
                return total
            down_bare = set(lp[lp["limit_type"] == "D"].index)
            if not down_bare:
                return total
            # ts_code 前 6 位为裸代码，匹配后归零
            down_mask = total.index.str[:6].isin(down_bare)
            total[down_mask] = 0.0
        except Exception as e:
            logger.debug("[HotMoneyFactor] 跌停过滤失败，继续: %s", e)
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        per_stock = getattr(self, "_per_stock", None)
        if per_stock is None:
            return reasons

        signals = self._compute_signals(per_stock)
        avg_quality = getattr(self, "_avg_quality", None)

        signal_meta = [
            ("net", "游资净买入", 40),
            ("quality", "游资胜率", 30),
            ("intensity", "买入强度", 30),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue

            labels: List[str] = []
            for key, label, max_val in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_val * threshold:
                    continue

                if key == "net":
                    net_v = per_stock["total_net"].get(ts_code, 0)
                    if net_v > 1e8:
                        labels.append(f"{label}{net_v/1e8:.1f}亿")
                    elif net_v > 1e4:
                        labels.append(f"{label}{net_v/1e4:.0f}万")
                    elif net_v < -1e4:
                        labels.append(f"游资净卖出{abs(net_v)/1e4:.0f}万")
                elif key == "quality":
                    if avg_quality is not None and ts_code in avg_quality.index:
                        labels.append(f"高胜率游资({avg_quality[ts_code]:.0%})")
                    else:
                        labels.append("高胜率游资")
                elif key == "intensity":
                    labels.append("强势买入")

            # 多家游资补充信息（仅在其他标签已触发时附加）
            if labels:
                count_v = int(per_stock["hm_count"].get(ts_code, 0))
                if count_v > 1:
                    names_v = per_stock["hm_names"].get(ts_code, "")
                    labels.append(f"{count_v}家游资({names_v})")
                reasons[ts_code] = labels

        return reasons
