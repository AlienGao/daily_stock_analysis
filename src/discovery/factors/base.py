# -*- coding: utf-8 -*-
"""因子抽象基类和发现结果数据类。"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryResult:
    """单只股票的发现结果。

    包含综合评分、各因子得分、买卖点位，用于排序、推送和 Web 展示。
    """

    ts_code: str
    stock_code: str
    stock_name: str
    score: float
    sector: str = ""
    factor_scores: Dict[str, float] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)
    buy_price_low: Optional[float] = None
    buy_price_high: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None
    change_pct: float = 0.0
    discovered_at: str = ""
    price_at_discovery: Optional[float] = None


class BaseFactor(ABC):
    """因子抽象基类。

    每个因子负责一种选股逻辑：从 Tushare 拉取全市场数据，逐股打分，
    返回 0-100 分的 pd.Series（索引为 ts_code）。

    子类需要：
    - 设置类属性 name / available_intraday / available_postmarket / weight
    - 实现 fetch_data() 和 score()
    """

    name: str = ""
    available_intraday: bool = False
    available_postmarket: bool = False
    weight: float = 0.0

    @abstractmethod
    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """拉取全市场原始数据。

        Args:
            trade_date: 交易日期 (YYYYMMDD)
            **kwargs: 额外上下文（如 tushare_fetcher 实例）

        Returns:
            DataFrame (index=ts_code) 或 None
        """

    @abstractmethod
    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """对每只股票打分。

        Args:
            df: fetch_data() 返回的 DataFrame
            **context: 额外打分上下文（如 sector_scores, price_data）

        Returns:
            pd.Series (index=ts_code, values=0-100)
        """

    def is_available(self, mode: str) -> bool:
        """检查因子在给定模式下是否可用。"""
        if mode == "intraday":
            return self.available_intraday
        if mode == "postmarket":
            return self.available_postmarket
        return False

    def weighted_score(self, df: pd.DataFrame, **context) -> pd.Series:
        """返回加权后的分数 (原始分 × 权重/100)。"""
        raw = self.score(df, **context)
        return raw * self.weight / 100.0

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        """对每只股票生成推荐理由描述。

        默认返回空，子类可按需覆写。返回 {ts_code: [理由1, 理由2, ...]}。
        """
        return {}
