# -*- coding: utf-8 -*-
"""券商金股推荐 API 端点。"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.services.broker_recommend_service import BrokerRecommendService

logger = logging.getLogger(__name__)

router = APIRouter()


class BrokerRecommendItem(BaseModel):
    ts_code: str
    name: str
    broker: str
    broker_count: int


class BrokerRecommendResponse(BaseModel):
    month: str
    total_recommendations: int
    unique_stocks: int
    unique_brokers: int
    items: List[BrokerRecommendItem]


class BrokerFetchResponse(BaseModel):
    month: str
    saved_count: int


class BrokerDailyReturn(BaseModel):
    date: str
    daily_return: Optional[float] = None
    cumulative: Optional[float] = None


class BrokerBacktestItem(BaseModel):
    broker: str
    stock_count: int
    cumulative_return: float
    win_rate: float
    avg_return: float
    daily_returns: List[BrokerDailyReturn]
    stocks: List[Dict[str, str]]


class StockReturnItem(BaseModel):
    ts_code: str
    name: str
    broker_count: int
    broker: str
    daily_returns: List[BrokerDailyReturn]


class BrokerBacktestResponse(BaseModel):
    month: str
    next_month: str
    buy_date: str
    sell_date: str
    total_recommendations: int
    unique_stocks: int
    unique_brokers: int
    brokers: List[BrokerBacktestItem]
    stock_returns: List[StockReturnItem]


@router.get("/months", response_model=List[str])
def get_available_months() -> List[str]:
    """获取有券商金股数据的月份列表。"""
    service = BrokerRecommendService()
    return service.get_available_months()


@router.get("/{month}", response_model=BrokerRecommendResponse)
def get_monthly_recommendations(month: str) -> BrokerRecommendResponse:
    """获取指定月份的券商金股推荐列表。"""
    service = BrokerRecommendService()
    df = service.get_monthly_recommendations(month)

    if df is None or df.empty:
        return BrokerRecommendResponse(
            month=month,
            total_recommendations=0,
            unique_stocks=0,
            unique_brokers=0,
            items=[],
        )

    # 去重后按券商+股票排序
    df_unique = df.drop_duplicates(subset=['broker', 'ts_code'])

    items = [
        BrokerRecommendItem(
            ts_code=str(row.get('ts_code', '')),
            name=str(row.get('name', '')),
            broker=str(row.get('broker', '')),
            broker_count=int(row.get('broker_count', 1)),
        )
        for _, row in df_unique.iterrows()
    ]

    return BrokerRecommendResponse(
        month=month,
        total_recommendations=len(df),
        unique_stocks=df['ts_code'].nunique(),
        unique_brokers=df['broker'].nunique(),
        items=items,
    )


@router.post("/{month}/fetch", response_model=BrokerFetchResponse)
def fetch_month(month: str) -> BrokerFetchResponse:
    """抓取并存储指定月份的券商金股数据。"""
    service = BrokerRecommendService()
    count = service.fetch_and_store_month(month)
    return BrokerFetchResponse(month=month, saved_count=count)


@router.get("/{month}/backtest", response_model=BrokerBacktestResponse)
def get_backtest(
    month: str,
    top_n: int = Query(default=10, ge=1, le=50, description="每个券商最多取几只金股"),
) -> BrokerBacktestResponse:
    """对指定月份金股池按券商分组做回测。"""
    service = BrokerRecommendService()
    result = service.compute_backtest(month, top_n_per_broker=top_n)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    # 转换嵌套结构
    brokers = []
    for b in result.get("brokers", []):
        brokers.append(BrokerBacktestItem(
            broker=b["broker"],
            stock_count=b["stock_count"],
            cumulative_return=b["cumulative_return"],
            win_rate=b["win_rate"],
            avg_return=b["avg_return"],
            daily_returns=[
                BrokerDailyReturn(
                    date=dr["date"],
                    daily_return=dr.get("return"),
                    cumulative=dr.get("cumulative"),
                )
                for dr in b.get("daily_returns", [])
            ],
            stocks=b.get("stocks", []),
        ))

    stock_returns = [
        StockReturnItem(
            ts_code=sr["ts_code"],
            name=sr["name"],
            broker_count=sr["broker_count"],
            broker=sr["broker"],
            daily_returns=[
                BrokerDailyReturn(
                    date=dr["date"],
                    daily_return=dr.get("return"),
                    cumulative=dr.get("cumulative"),
                )
                for dr in sr.get("daily_returns", [])
            ],
        )
        for sr in result.get("stock_returns", [])
    ]

    return BrokerBacktestResponse(
        month=result["month"],
        next_month=result["next_month"],
        buy_date=result["buy_date"],
        sell_date=result["sell_date"],
        total_recommendations=result["total_recommendations"],
        unique_stocks=result["unique_stocks"],
        unique_brokers=result["unique_brokers"],
        brokers=brokers,
        stock_returns=stock_returns,
    )
