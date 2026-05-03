# -*- coding: utf-8 -*-
"""股票发现因子模块。

每个因子对应一个独立文件，继承 BaseFactor 抽象基类。
盘中因子 (4个): SectorFactor, MaEntryFactor, MomentumFactor, ReboundFactor
盘后因子 (5个): MoneyFlowFactor, MarginFactor, ChipFactor, TechnicalFactor, LimitFactor
R&D 闭环生成因子: rd_gen_*.py（自动发现并注册）
"""

import importlib
import inspect
import logging
from pathlib import Path

from src.discovery.factors.base import BaseFactor, DiscoveryResult

logger = logging.getLogger(__name__)

# 显式导入（保证加载顺序和确定性）
from src.discovery.factors.sector_factor import SectorFactor
from src.discovery.factors.ma_entry_factor import MaEntryFactor
from src.discovery.factors.momentum_factor import MomentumFactor
from src.discovery.factors.rebound_factor import ReboundFactor
from src.discovery.factors.money_flow_factor import MoneyFlowFactor
from src.discovery.factors.margin_factor import MarginFactor
from src.discovery.factors.chip_factor import ChipFactor
from src.discovery.factors.technical_factor import TechnicalFactor
from src.discovery.factors.limit_factor import LimitFactor
from src.discovery.factors.fundamental_factor import FundamentalFactor
from src.discovery.factors.popularity_factor import PopularityFactor
from src.discovery.factors.hot_money_factor import HotMoneyFactor

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
    "FundamentalFactor",
    "PopularityFactor",
    "HotMoneyFactor",
]

# ---------------------------------------------------------------------------
# 自动发现：扫描目录下 rd_gen_*.py 文件，注册 R&D 闭环生成的因子
# ---------------------------------------------------------------------------

_KNOWN_MODULES = {
    "base", "sector_factor", "ma_entry_factor", "momentum_factor",
    "rebound_factor", "money_flow_factor", "margin_factor", "chip_factor",
    "technical_factor", "limit_factor", "fundamental_factor", "popularity_factor",
    "hot_money_factor",
}

_factors_dir = Path(__file__).resolve().parent
for _fp in sorted(_factors_dir.glob("rd_gen_*.py")):
    _mod_name = _fp.stem
    if _mod_name in _KNOWN_MODULES:
        continue
    try:
        _mod = importlib.import_module(f"src.discovery.factors.{_mod_name}")
        for _name, _obj in inspect.getmembers(_mod, inspect.isclass):
            if (
                issubclass(_obj, BaseFactor)
                and _obj is not BaseFactor
                and _obj.__module__ == _mod.__name__
            ):
                globals()[_name] = _obj
                __all__.append(_name)
                logger.info("[Factors] 自动注册 R&D 因子: %s (来自 %s)", _name, _mod_name)
    except Exception as _e:
        logger.warning("[Factors] 加载 R&D 因子 %s 失败: %s", _mod_name, _e)
