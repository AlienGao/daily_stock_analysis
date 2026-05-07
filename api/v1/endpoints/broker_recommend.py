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


class StockEnrichment(BaseModel):
    """单只股票的增强数据。"""
    nineturn: Optional[NineTurnSignal] = None
    forecast: Optional[ForecastSummary] = None
    cyq_perf: Optional[CyqPerfSummary] = None


class EnrichmentResponse(BaseModel):
    """增强数据响应：{ts_code -> StockEnrichment} 字典。"""
    month: str
    query_date: str
    data: Dict[str, StockEnrichment]


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
    price: Optional[float] = None
    daily_return: Optional[float] = None
    cumulative: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None


class NineTurnSignal(BaseModel):
    up_count: Optional[int] = None
    down_count: Optional[int] = None
    nine_up_turn: Optional[int] = None
    nine_down_turn: Optional[int] = None


class ForecastSummary(BaseModel):
    eps: Optional[float] = None
    pe: Optional[float] = None
    roe: Optional[float] = None
    np: Optional[float] = None
    rating: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    imp_dg: Optional[str] = None


class CyqPerfSummary(BaseModel):
    cost_avg: Optional[float] = None
    winner_rate: Optional[float] = None
    concentration: Optional[float] = None
    scr90: Optional[float] = None


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
    end_price: Optional[float] = None
    end_date: Optional[str] = None
    daily_change: Optional[float] = None
    daily_returns: List[BrokerDailyReturn]
    nineturn: Optional[NineTurnSignal] = None
    forecast: Optional[ForecastSummary] = None
    cyq_perf: Optional[CyqPerfSummary] = None


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


class YtdMonthlyReturn(BaseModel):
    """券商在单个月份的回测表现。"""
    month: str
    cumulative_return: float
    stock_count: int
    win_rate: float


class YtdBrokerItem(BaseModel):
    """YTD 单家券商的跨月复合表现。"""
    broker: str
    cumulative_return: float
    active_months: int
    daily_returns: List[BrokerDailyReturn]
    monthly_returns: List[YtdMonthlyReturn]


class YtdBacktestResponse(BaseModel):
    """年初至今回测响应：Top-N 券商跨月复合累计收益曲线。"""
    year: str
    start_date: str
    end_date: str
    total_brokers: int
    brokers: List[YtdBrokerItem]


@router.get("/months", response_model=List[str])
def get_available_months() -> List[str]:
    """获取有券商金股数据的月份列表。"""
    service = BrokerRecommendService()
    return service.get_available_months()


@router.get("/ytd", response_model=YtdBacktestResponse)
def get_ytd_backtest(
    year: Optional[str] = Query(default=None, description="4-digit year, defaults to current year"),
    top_n: int = Query(default=5, ge=1, le=50, description="Number of top brokers"),
) -> YtdBacktestResponse:
    """年初至今累计回测：跨月复合 Top-N 券商组合收益。"""
    from datetime import datetime

    if year is None:
        year = str(datetime.now().year)

    service = BrokerRecommendService()
    result = service.compute_ytd_backtest(year, top_n=top_n)

    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])

    brokers = [
        YtdBrokerItem(
            broker=b["broker"],
            cumulative_return=b["cumulative_return"],
            active_months=b["active_months"],
            daily_returns=[
                BrokerDailyReturn(
                    date=dr["date"],
                    daily_return=dr.get("return"),
                    cumulative=dr.get("cumulative"),
                )
                for dr in b.get("daily_returns", [])
            ],
            monthly_returns=[
                YtdMonthlyReturn(
                    month=mr["month"],
                    cumulative_return=mr["cumulative_return"],
                    stock_count=mr["stock_count"],
                    win_rate=mr["win_rate"],
                )
                for mr in b.get("monthly_returns", [])
            ],
        )
        for b in result.get("brokers", [])
    ]

    return YtdBacktestResponse(
        year=result["year"],
        start_date=result["start_date"],
        end_date=result["end_date"],
        total_brokers=result["total_brokers"],
        brokers=brokers,
    )


# ---- 机构调研 Top 10 ----

class SurveyDetail(BaseModel):
    surv_date: str
    rece_org: str
    org_type: str
    rece_mode: str
    weight: float
    fund_visitors: str
    rece_place: str
    comp_rece: str


class InstitutionSurveyItem(BaseModel):
    ts_code: str
    name: str
    weighted_score: float
    visit_count: int
    last_surv_date: str
    top_orgs: List[str]
    details: List[SurveyDetail]


class InstitutionSurveyResponse(BaseModel):
    date: str
    start_date: str
    end_date: str
    total_stocks: int
    items: List[InstitutionSurveyItem]


@router.get("/institution-survey", response_model=InstitutionSurveyResponse)
def get_institution_survey() -> InstitutionSurveyResponse:
    """近两周机构调研加权 Top 10。"""
    service = BrokerRecommendService()
    result = service.get_institution_survey_top10()

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    items = [
        InstitutionSurveyItem(
            ts_code=item["ts_code"],
            name=item["name"],
            weighted_score=item["weighted_score"],
            visit_count=item["visit_count"],
            last_surv_date=item["last_surv_date"],
            top_orgs=item["top_orgs"],
            details=[SurveyDetail(**d) for d in item["details"]],
        )
        for item in result.get("items", [])
    ]

    return InstitutionSurveyResponse(
        date=result["date"],
        start_date=result["start_date"],
        end_date=result["end_date"],
        total_stocks=result["total_stocks"],
        items=items,
    )


@router.get("/{month}", response_model=BrokerRecommendResponse)
def get_monthly_recommendations(month: str) -> BrokerRecommendResponse:
    """获取指定月份的券商金股推荐列表（不含增强数据，增强数据请用 /{month}/enrichment）。"""
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


@router.get("/{month}/enrichment", response_model=EnrichmentResponse)
def get_monthly_enrichment(month: str) -> EnrichmentResponse:
    """获取指定月份推荐股票的增强数据（九转、盈利预测、筹码胜率）。

    独立端点，带缓存和并行化，与 /{month} 分开以避免超时。
    返回 {ts_code: {nineturn, forecast, cyq_perf}} 字典。
    """
    service = BrokerRecommendService()
    enrichment = service.get_monthly_enrichment(month)
    query_date = service._resolve_enrichment_date(month)

    data: Dict[str, StockEnrichment] = {}
    for ts_code, enrich in enrichment.items():
        data[ts_code] = StockEnrichment(
            nineturn=NineTurnSignal(**enrich["nineturn"]) if enrich.get("nineturn") else None,
            forecast=ForecastSummary(**enrich["forecast"]) if enrich.get("forecast") else None,
            cyq_perf=CyqPerfSummary(**enrich["cyq_perf"]) if enrich.get("cyq_perf") else None,
        )

    return EnrichmentResponse(month=month, query_date=query_date, data=data)


class ConsecutiveStockItem(BaseModel):
    ts_code: str
    name: str
    broker_count_current: int
    broker_count_prev: int
    brokers_current: List[str]
    brokers_prev: List[str]


@router.get("/{month}/consecutive", response_model=List[ConsecutiveStockItem])
def get_consecutive_stocks(month: str) -> List[ConsecutiveStockItem]:
    """获取连续两个月都被券商推荐的金股。"""
    service = BrokerRecommendService()
    data = service.get_consecutive_stocks(month)
    return [ConsecutiveStockItem(**item) for item in data]


@router.post("/{month}/fetch", response_model=BrokerFetchResponse)
def fetch_month(month: str) -> BrokerFetchResponse:
    """抓取并存储指定月份的券商金股数据。

    当前月份：同时清除 enrichment 缓存，强制后续请求刷新价格和筹码胜率。
    """
    from datetime import datetime

    service = BrokerRecommendService()
    count = service.fetch_and_store_month(month)

    # 当前月份：清除 L1/L2 缓存，确保价格和筹码胜率重新拉取
    current_month = datetime.now().strftime("%Y%m")
    if month == current_month:
        service.invalidate_enrichment_cache(month)

    return BrokerFetchResponse(month=month, saved_count=count)


@router.get("/{month}/backtest", response_model=BrokerBacktestResponse)
def get_backtest(
    month: str,
    top_n: int = Query(default=15, ge=1, le=50, description="每个券商最多取几只金股"),
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
                    price=dr.get("price"),
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
            end_price=sr.get("end_price"),
            end_date=sr.get("end_date"),
            daily_change=sr.get("daily_change"),
            daily_returns=[
                BrokerDailyReturn(
                    date=dr["date"],
                    price=dr.get("price"),
                    daily_return=dr.get("return"),
                    cumulative=dr.get("cumulative"),
                    open=dr.get("open"),
                    high=dr.get("high"),
                    low=dr.get("low"),
                )
                for dr in sr.get("daily_returns", [])
            ],
            nineturn=NineTurnSignal(**sr["nineturn"]) if sr.get("nineturn") else None,
            forecast=ForecastSummary(**sr["forecast"]) if sr.get("forecast") else None,
            cyq_perf=CyqPerfSummary(**sr["cyq_perf"]) if sr.get("cyq_perf") else None,
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
