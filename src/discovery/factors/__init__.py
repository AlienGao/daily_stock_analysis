# -*- coding: utf-8 -*-
"""股票发现因子模块。

每个因子对应一个独立文件，继承 BaseFactor 抽象基类。
盘中因子 (5个): SectorFactor, MaEntryFactor, MomentumFactor, ReboundFactor, PopularityFactor
盘后因子 (14个): MoneyFlowFactor, MarginFactor, ChipFactor, TechnicalFactor, LimitFactor,
                 FundamentalFactor, HotMoneyFactor, InstitutionHoldFactor,
                 ProfitForecastFactor, PerformanceFactor, BuybackFactor, InsiderBuyFactor,
                 PopularityFactor, BrokerRecommendFactor
"""

import logging

from src.discovery.factors.base import BaseFactor, DiscoveryResult

logger = logging.getLogger(__name__)

# 显式导入（保证加载顺序和确定性）
from src.discovery.factors.sector_factor import SectorFactor
from src.discovery.factors.ma_entry_factor import MaEntryFactor
from src.discovery.factors.momentum_factor import MomentumFactor
from src.discovery.factors.ranking_momentum_factor import RankingMomentumFactor
from src.discovery.factors.rebound_factor import ReboundFactor
from src.discovery.factors.money_flow_factor import MoneyFlowFactor
from src.discovery.factors.margin_factor import MarginFactor
from src.discovery.factors.chip_factor import ChipFactor
from src.discovery.factors.technical_factor import TechnicalFactor
from src.discovery.factors.limit_factor import LimitFactor
from src.discovery.factors.fundamental_factor import FundamentalFactor
from src.discovery.factors.popularity_factor import PopularityFactor
from src.discovery.factors.hot_money_factor import HotMoneyFactor
from src.discovery.factors.institution_hold_factor import InstitutionHoldFactor
from src.discovery.factors.profit_forecast_factor import ProfitForecastFactor
from src.discovery.factors.performance_factor import PerformanceFactor
from src.discovery.factors.buyback_factor import BuybackFactor
from src.discovery.factors.insider_buy_factor import InsiderBuyFactor
from src.discovery.factors.broker_recommend_factor import BrokerRecommendFactor
from src.discovery.factors.concept_heat_factor import ConceptHeatFactor

__all__ = [
    "BaseFactor",
    "DiscoveryResult",
    "SectorFactor",
    "MaEntryFactor",
    "MomentumFactor",
    "RankingMomentumFactor",
    "ReboundFactor",
    "MoneyFlowFactor",
    "MarginFactor",
    "ChipFactor",
    "TechnicalFactor",
    "LimitFactor",
    "FundamentalFactor",
    "PopularityFactor",
    "HotMoneyFactor",
    "InstitutionHoldFactor",
    "ProfitForecastFactor",
    "PerformanceFactor",
    "BuybackFactor",
    "InsiderBuyFactor",
    "BrokerRecommendFactor",
    "ConceptHeatFactor",
]
