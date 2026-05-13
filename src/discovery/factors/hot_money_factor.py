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
    weight = 8.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先读 DB 缓存，降级到 Tushare API。"""
        self._limit_down_set = None  # 每次拉取重置

        df = None
        try:
            from src.storage import DatabaseManager
            df = DatabaseManager().get_hm_detail_by_date(trade_date)
            if df is not None and not df.empty:
                logger.info("[HotMoneyFactor] DB 命中: %d 条", len(df))
        except Exception as e:
            logger.debug("[HotMoneyFactor] DB 读取失败: %s", e)

        if df is None or df.empty:
            tushare_fetcher = kwargs.get("tushare_fetcher")
            if tushare_fetcher is None:
                return None
            df = tushare_fetcher.get_bulk_hm_detail(trade_date)

        if df is not None and not df.empty:
            self._cache_limit_down(trade_date)
            self._prev_hm_pairs = self._load_prev_pairs(trade_date)

        return df

    def _load_prev_pairs(self, trade_date: str) -> set:
        """加载上一交易日 (bare_code, hm_name) 集合，用于连续买入信号。"""
        try:
            from datetime import datetime as _dt, timedelta
            from src.storage import DatabaseManager
            td = _dt.strptime(str(trade_date)[:8], "%Y%m%d")
            prev_date = (td - timedelta(days=1)).strftime("%Y%m%d")
            df_prev = DatabaseManager().get_hm_detail_by_date(prev_date)
            if df_prev is None or df_prev.empty:
                return set()
            pairs = set()
            for idx, row in df_prev.iterrows():
                code = str(idx)[:6]
                hm = str(row.get("hm_name", ""))
                if code and hm:
                    pairs.add((code, hm))
            logger.debug("[HotMoneyFactor] 昨日游资配对: %d 组", len(pairs))
            return pairs
        except Exception as e:
            logger.debug("[HotMoneyFactor] 加载昨日游资数据失败: %s", e)
            return set()

    def _cache_limit_down(self, trade_date: str) -> None:
        """预加载当日跌停股集合，供 _zero_limit_down 复用，避免重复查 DB。"""
        try:
            from src.storage import DatabaseManager
            lp = DatabaseManager().get_limit_pool(trade_date=trade_date)
            if lp is not None and not lp.empty:
                self._limit_down_set = set(lp[lp["limit_type"] == "D"].index)
            else:
                self._limit_down_set = set()
        except Exception as e:
            logger.debug("[HotMoneyFactor] limit_pool 查询失败: %s", e)
            self._limit_down_set = set()

    # ------------------------------------------------------------------
    # 聚合 + 共享信号
    # ------------------------------------------------------------------

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """按 ts_code 聚合游资明细。"""
        return df.groupby(level=0).agg(
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
            scores_list = [quality_map.get(n, 0.25) for n in names.split("|")]
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
        total = self._zero_limit_down(total)

        # 连续买入加成（质量加权）：同一游资连续两天买入 = 看好信号
        total = self._apply_consecutive_bonus(total, per_stock)

        total = total.clip(0, 100)
        total.name = self.name
        return total

    def _zero_limit_down(self, total: pd.Series) -> pd.Series:
        """将当日跌停股得分归零（集合由 fetch_data 预加载）。"""
        down_set = getattr(self, "_limit_down_set", None)
        if not down_set:
            return total
        down_mask = total.index.str[:6].isin(down_set)
        total[down_mask] = 0.0
        return total

    def _apply_consecutive_bonus(
        self, total: pd.Series, per_stock: pd.DataFrame,
    ) -> pd.Series:
        """连续买入加成：游资昨日也在同一股票出现，质量加权。

        - 跳过「单体在多家中偶发重复」(repeat<2 且 total≥3)
        - bonus = min(Σ quality × 0.15, 0.20)
        """
        prev_pairs = getattr(self, "_prev_hm_pairs", None)
        if not prev_pairs:
            return total

        from src.discovery.hm_tracker import HmTracker
        quality_map = HmTracker.load_quality()

        bonus = pd.Series(0.0, index=total.index)
        for ts_code in total.index:
            row = per_stock.loc[ts_code]
            names_str = row.get("hm_names", "")
            if not names_str:
                continue
            names = names_str.split("|")
            total_count = len(names)
            code = str(ts_code)[:6]
            repeat_names = [n for n in names if (code, n) in prev_pairs]
            repeat_count = len(repeat_names)

            if repeat_count == 0:
                continue
            # 单体在多家中偶发重复 → 跳过
            if repeat_count < 2 and total_count >= 3:
                continue

            quality_sum = sum(quality_map.get(n, 0.25) for n in repeat_names)
            bonus[ts_code] = min(quality_sum * 0.15, 0.20)

        if bonus.sum() > 0:
            logger.debug("[HotMoneyFactor] 连续买入加成: %d 只, max %.1f%%",
                         int((bonus > 0).sum()), bonus.max() * 100)

        return total * (1 + bonus)

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
