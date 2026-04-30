# -*- coding: utf-8 -*-
"""股票发现因子模块。

每个因子对应一个独立文件，继承 BaseFactor 抽象基类。
盘中因子 (4个): SectorFactor, MaEntryFactor, MomentumFactor, ReboundFactor
盘后因子 (5个): MoneyFlowFactor, MarginFactor, ChipFactor, TechnicalFactor, LimitFactor
"""

from src.discovery.factors.base import BaseFactor, DiscoveryResult
from src.discovery.factors.sector_factor import SectorFactor
from src.discovery.factors.ma_entry_factor import MaEntryFactor
from src.discovery.factors.momentum_factor import MomentumFactor
from src.discovery.factors.rebound_factor import ReboundFactor
from src.discovery.factors.money_flow_factor import MoneyFlowFactor
from src.discovery.factors.margin_factor import MarginFactor
from src.discovery.factors.chip_factor import ChipFactor
from src.discovery.factors.technical_factor import TechnicalFactor
from src.discovery.factors.limit_factor import LimitFactor

__all__ = [
    "BaseFactor",
    "DiscoveryResult",
    "SectorFactor",
    "MaEntryFactor",
    "MomentumFactor",
    "ReboundFactor",
    "MoneyFlowFactor",
    "MarginFactor",
    "ChipFactor",
    "TechnicalFactor",
    "LimitFactor",
]
