# -*- coding: utf-8 -*-
"""券商金股因子 (Broker Recommend Factor).

盘后因子：基于 Tushare broker_recommend 数据，统计当月各券商金股推荐数量。
被越多券商覆盖的股票，说明机构关注度越高。

数据来源: Tushare broker_recommend (需要 6000 积分)
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class BrokerRecommendFactor(BaseFactor):
    """券商金股因子。

    逻辑：统计当月（最新月份）各券商对每只股票的金股推荐次数，
    被推荐次数越多，说明机构关注度越高、共识越强。
    """

    name = "broker_recommend"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取当月券商金股推荐数据。

        Args:
            trade_date: 交易日期 YYYYMMDD，用于推断目标月份

        Returns:
            DataFrame(columns=[month, broker, ts_code, name])
        """
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None or tushare_fetcher._api is None:
            return None

        # 推断目标月份（trade_date 的 YYYYMM）
        if len(trade_date) >= 6:
            month = trade_date[:6]
        else:
            from datetime import date

            month = date.today().strftime("%Y%m")

        try:
            df = tushare_fetcher._api.query(
                "broker_recommend",
                month=month,
            )
            return df
        except Exception as e:
            logger.warning(f"[BrokerRecommend] 获取券商金股数据失败: {e}")
            return None

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """按被推荐次数评分。

        逻辑：
        - 被推荐 1 次：30分
        - 被推荐 2 次：50分
        - 被推荐 3 次：70分
        - 被推荐 4 次：85分
        - 被推荐 5 次及以上：95分
        """
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        ts_code_col = next((c for c in df.columns if "ts_code" in c), None)
        broker_col = next((c for c in df.columns if "broker" in c), None)

        if ts_code_col is None or broker_col is None:
            return scores

        # 统计每只股票被多少家不同券商推荐
        broker_count = df.groupby(ts_code_col)[broker_col].nunique()

        def map_score(n):
            if n >= 5:
                return 95.0
            elif n == 4:
                return 85.0
            elif n == 3:
                return 70.0
            elif n == 2:
                return 50.0
            elif n == 1:
                return 30.0
            return 0.0

        broker_scores = broker_count.apply(map_score)

        # 为每行赋值：找到该行股票代码对应的评分
        for idx, row in df.iterrows():
            ts = str(row.get(ts_code_col, ""))
            if ts in broker_scores.index:
                scores.at[idx] = broker_scores[ts]

        return scores

    def describe(
        self, df: pd.DataFrame, scores: pd.Series, **context
    ) -> Dict[str, List[str]]:
        """生成推荐理由。"""
        reasons: Dict[str, List[str]] = {}

        if df.empty:
            return reasons

        ts_code_col = next((c for c in df.columns if "ts_code" in c), None)
        broker_col = next((c for c in df.columns if "broker" in c), None)
        name_col = next((c for c in df.columns if c == "name"), None)

        if ts_code_col is None or broker_col is None:
            return reasons

        # 构建 ts_code -> score 的映射（scores 与 df 行对齐）
        ts_to_score: Dict[str, float] = {}
        for idx, row in df.iterrows():
            ts = str(row.get(ts_code_col, ""))
            if ts and idx < len(scores):
                ts_to_score[ts] = scores.iloc[idx]

        # 统计每只股票的推荐券商列表
        broker_by_stock: Dict[str, List[str]] = {}
        name_by_stock: Dict[str, str] = {}
        for _, row in df.iterrows():
            ts = str(row.get(ts_code_col, ""))
            broker = str(row.get(broker_col, ""))
            name = str(row.get(name_col, "")) if name_col else ""
            if ts and broker:
                broker_by_stock.setdefault(ts, []).append(broker)
                if name:
                    name_by_stock[ts] = name

        for ts, brokers in broker_by_stock.items():
            n = len(set(brokers))  # 去重
            score = ts_to_score.get(ts, 0)
            if n > 0 and score > 0:
                top_brokers = list(set(brokers))[:3]
                reasons[ts] = [
                    f"券商金股({n}家推荐)",
                    f"推荐券商: {', '.join(top_brokers)}",
                ]

        return reasons