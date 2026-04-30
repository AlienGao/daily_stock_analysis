# -*- coding: utf-8 -*-
"""股票自动发现引擎 (Stock Discovery Engine).

基于 Tushare 多因子数据自动发现值得关注的股票，
支持盘中快速扫描 (intraday) 和盘后深度分析 (postmarket) 两种模式。
"""

from src.discovery.config import DiscoveryConfig
from src.discovery.engine import StockDiscoveryEngine
from src.discovery.factors.base import BaseFactor, DiscoveryResult

__all__ = [
    "DiscoveryConfig",
    "StockDiscoveryEngine",
    "BaseFactor",
    "DiscoveryResult",
]
