# -*- coding: utf-8 -*-
"""
API v1 Endpoints 模块初始化

职责：
1. 声明所有 endpoint 路由模块
"""

from api.v1.endpoints import (
    discovery,
    factor_backtest,
    health,
    analysis,
    history,
    stocks,
    backtest,
    system_config,
    auth,
    agent,
    usage,
    portfolio,
    broker_recommend,
    alerts,
    research,
    decision_signals,
    screening,
    market,
)
__all__ = [
    "discovery",
    "factor_backtest",
    "health",
    "analysis",
    "history",
    "stocks",
    "backtest",
    "system_config",
    "auth",
    "agent",
    "usage",
    "portfolio",
    "broker_recommend",
    "alerts",
    "research",
    "decision_signals",
    "screening",
    "market",
]
