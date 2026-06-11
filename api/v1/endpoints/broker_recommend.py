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
    sector: Optional[str] = None


class EnrichmentResponse(BaseModel):
    """增强数据响应：{ts_code -> StockEnrichment} 字典。"""
    month: str
    query_date: str
    data: Dict[str, StockEnrichment]


class UpToDownDailyStockItem(BaseModel):
    ts_code: str
    name: str
    broker_count: int = 1
    signal_type: str = "up_to_down"
    prev_nineturn_up_count: int = 0
    prev_nineturn_down_count: int = 0
    nineturn_up_count: Optional[int] = None
    nineturn_down_count: Optional[int] = None


class UpToDownDailyDayItem(BaseModel):
    date: str
    stocks: List[UpToDownDailyStockItem]


class UpToDownDailyResponse(BaseModel):
    month: str
    buy_date: str
    sell_date: str
    days: List[UpToDownDailyDayItem]


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
    month_cumulative_return: Optional[float] = None
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



class StockHistoryEntry(BaseModel):
    """单只股票在某推荐月的持仓期走势。"""
    month: str
    brokers: List[str]
    broker_count: int
    buy_date: str
    sell_date: str
    cumulative_return: Optional[float] = None
    daily_returns: List[BrokerDailyReturn]


class StockHistoryResponse(BaseModel):
    ts_code: str
    name: str
    entries: List[StockHistoryEntry]


class HistoricalMonthCountItem(BaseModel):
    ts_code: str
    month_count: int


class HistoricalRecommendStatsItem(BaseModel):
    ts_code: str
    month_count: int = 0
    period_count: int = 0
    win_rate: Optional[float] = None
    max_return: Optional[float] = None
    max_drawdown: Optional[float] = None


class CurrentMonthReturnItem(BaseModel):
    ts_code: str
    cumulative_return: Optional[float] = None
    end_date: Optional[str] = None


class CurrentMonthReturnsResponse(BaseModel):
    month: str
    buy_date: str
    sell_date: str
    items: List[CurrentMonthReturnItem]


class PrevMonthCurrentTopItem(BaseModel):
    ts_code: str
    name: str = ""
    broker_count: int = 1
    cumulative_return: Optional[float] = None
    end_date: Optional[str] = None
    is_current_month_recommend: bool = False


class PrevMonthCurrentTopResponse(BaseModel):
    prev_month: str
    current_month: str
    buy_date: str
    sell_date: str
    items: List[PrevMonthCurrentTopItem]


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


class EqualWeightStrategyDailyReturn(BaseModel):
    """策略日收益。"""
    date: str
    daily_return: Optional[float] = None
    cumulative: Optional[float] = None
    stock_count: int


class EqualWeightStrategyMonthlyReturn(BaseModel):
    """策略月度收益汇总。"""
    month: str
    month_return: float
    cumulative_return: float
    stock_count: int
    stocks: List[Dict[str, Any]] = []


class RankStatItem(BaseModel):
    """顺位收益统计。"""
    rank: int
    avg_return: float
    month_count: int
    win_rate: float


class UpToDownStatItem(BaseModel):
    """升转降分档交易统计。"""
    up_count: int
    trade_count: int
    avg_return: float
    win_rate: float


class EqualWeightStrategyResponse(BaseModel):
    """九转选股等权策略响应。"""
    strategy: str
    top_n: int
    period_start_month: Optional[str] = None
    period_end_month: Optional[str] = None
    start_date: str
    end_date: str
    total_months: int
    cumulative_return: float
    daily_returns: List[EqualWeightStrategyDailyReturn]
    monthly_returns: List[EqualWeightStrategyMonthlyReturn]
    rank_stats: List[RankStatItem] = []
    up_to_down_stats: List[UpToDownStatItem] = []
    multi_curves: Dict[str, List[EqualWeightStrategyDailyReturn]] = {}


@router.get("/top-brokers", response_model=List[str])
def get_top_brokers(
    top_n: int = Query(default=5, ge=1, le=20, description="Number of top brokers"),
) -> List[str]:
    """获取有史以来累计收益排名前 N 的券商名称列表。"""
    service = BrokerRecommendService()
    return service.get_alltime_top_brokers(top_n=top_n)


@router.get("/months", response_model=List[str])
def get_available_months() -> List[str]:
    """获取有券商金股数据的月份列表。"""
    service = BrokerRecommendService()
    return service.get_available_months()


@router.get("/ytd", response_model=YtdBacktestResponse)
def get_ytd_backtest(
    year: Optional[str] = Query(default=None, description="4-digit year; omit for all-time"),
    top_n: int = Query(default=5, ge=1, le=50, description="Number of top brokers"),
) -> YtdBacktestResponse:
    """跨月复合回测：指定 year 为年初至今，省略 year 为有记录以来。"""
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
def get_institution_survey(
    start_date: Optional[str] = Query(None, description="起始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日期 YYYYMMDD"),
) -> InstitutionSurveyResponse:
    """机构调研加权 Top 10（默认最近一个有数据的日期，传日期参数则查历史）。"""
    service = BrokerRecommendService()
    result = service.get_institution_survey_top10(start_date=start_date, end_date=end_date)

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


@router.get("/institution-survey/dates")
def get_institution_survey_dates() -> List[str]:
    """获取所有有机构调研数据的日期列表（降序）。"""
    service = BrokerRecommendService()
    return service.get_institution_survey_dates()




@router.get("/historical-month-counts", response_model=List[HistoricalMonthCountItem])
def get_historical_month_counts(
    codes: str = Query(..., description="逗号分隔的股票代码列表"),
) -> List[HistoricalMonthCountItem]:
    """批量查询股票历史上被推荐的月份数。"""
    ts_codes = [c.strip() for c in codes.split(",") if c.strip()]
    service = BrokerRecommendService()
    counts = service.get_historical_month_counts(ts_codes)
    return [
        HistoricalMonthCountItem(ts_code=tc, month_count=counts.get(tc, 0))
        for tc in ts_codes
    ]






@router.get("/current-month-returns", response_model=CurrentMonthReturnsResponse)
def get_current_month_returns(
    codes: str = Query(..., description="逗号分隔的股票代码列表"),
) -> CurrentMonthReturnsResponse:
    """批量查询股票在当前自然月的累计收益（后复权，供历史月份金股明细对照）。"""
    ts_codes = [c.strip() for c in codes.split(",") if c.strip()]
    service = BrokerRecommendService()
    result = service.get_current_month_stock_returns(ts_codes)
    return CurrentMonthReturnsResponse(
        month=result.get("month", ""),
        buy_date=result.get("buy_date", ""),
        sell_date=result.get("sell_date", ""),
        items=[
            CurrentMonthReturnItem(
                ts_code=item["ts_code"],
                cumulative_return=item.get("cumulative_return"),
                end_date=item.get("end_date"),
            )
            for item in result.get("items", [])
        ],
    )

@router.get("/prev-month-current-top", response_model=PrevMonthCurrentTopResponse)
def get_prev_month_current_top(
    top_n: int = Query(5, ge=1, le=20, description="返回条数，默认 Top5"),
) -> PrevMonthCurrentTopResponse:
    """上月推荐金股在当前自然月的累计收益 Top N（后复权，每日更新）。"""
    service = BrokerRecommendService()
    result = service.get_prev_month_current_returns_top(top_n=top_n)
    return PrevMonthCurrentTopResponse(
        prev_month=result.get("prev_month", ""),
        current_month=result.get("current_month", ""),
        buy_date=result.get("buy_date", ""),
        sell_date=result.get("sell_date", ""),
        items=[
            PrevMonthCurrentTopItem(
                ts_code=item["ts_code"],
                name=item.get("name", ""),
                broker_count=int(item.get("broker_count") or 1),
                cumulative_return=item.get("cumulative_return"),
                end_date=item.get("end_date"),
                is_current_month_recommend=bool(item.get("is_current_month_recommend")),
            )
            for item in result.get("items", [])
        ],
    )


@router.get("/historical-recommend-stats", response_model=List[HistoricalRecommendStatsItem])
def get_historical_recommend_stats(
    codes: str = Query(..., description="逗号分隔的股票代码列表"),
    exclude_after: str | None = Query(None, description="仅统计该月份之前的推荐记录，格式 YYYYMM"),
) -> List[HistoricalRecommendStatsItem]:
    """批量查询股票历史推荐胜率、最高/最低期末收益（与展开历史一致）。"""
    ts_codes = [c.strip() for c in codes.split(",") if c.strip()]
    service = BrokerRecommendService()
    stats = service.get_historical_recommend_stats(ts_codes, exclude_after=exclude_after)
    return [
        HistoricalRecommendStatsItem(
            ts_code=tc,
            month_count=stats.get(tc, {}).get("month_count", 0),
            period_count=stats.get(tc, {}).get("period_count", 0),
            win_rate=stats.get(tc, {}).get("win_rate"),
            max_return=stats.get(tc, {}).get("max_return"),
            max_drawdown=stats.get(tc, {}).get("max_drawdown"),
        )
        for tc in ts_codes
    ]


@router.get("/stock/{ts_code}/history", response_model=StockHistoryResponse)
def get_stock_recommend_history(
    ts_code: str,
    exclude_after: str | None = Query(None, description="仅返回该月份之前的推荐记录，格式 YYYYMM"),
) -> StockHistoryResponse:
    """获取单只股票历次推荐月份及对应持仓期 K 线数据。"""
    service = BrokerRecommendService()
    result = service.get_stock_recommend_history(ts_code, exclude_after=exclude_after)
    entries = [
        StockHistoryEntry(
            month=e["month"],
            brokers=e.get("brokers", []),
            broker_count=e.get("broker_count", 0),
            buy_date=e.get("buy_date", ""),
            sell_date=e.get("sell_date", ""),
            cumulative_return=e.get("cumulative_return"),
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
                for dr in e.get("daily_returns", [])
            ],
        )
        for e in result.get("entries", [])
    ]
    return StockHistoryResponse(
        ts_code=result.get("ts_code", ts_code),
        name=result.get("name", ""),
        entries=entries,
    )


@router.get("/equal-weight-strategy", response_model=EqualWeightStrategyResponse)
def get_equal_weight_strategy(
    top_n: int = Query(default=4, ge=1, le=20, description="兼容参数，不再参与选股"),
    start_month: Optional[str] = Query(
        default=None, pattern=r"^\d{6}$", description="统计起始月 YYYYMM（含）",
    ),
    end_month: Optional[str] = Query(
        default=None, pattern=r"^\d{6}$", description="统计截止月 YYYYMM（含）",
    ),
) -> EqualWeightStrategyResponse:
    """九转上升 + 近3日形态收盘买卖；每笔固定金额买入；总收益=平仓后总资产/初始本金-1；升转降 T+1 仍下降收盘卖。"""
    from fastapi.responses import JSONResponse

    service = BrokerRecommendService()
    result = service.compute_equal_weight_strategy(
        top_n=top_n, start_month=start_month, end_month=end_month,
    )

    if result is None or result.get("status") == "computing":
        return JSONResponse(
            status_code=202, content={"status": "computing"},
        )

    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])

    daily_returns = [
        EqualWeightStrategyDailyReturn(
            date=dr["date"],
            daily_return=dr.get("daily_return"),
            cumulative=dr.get("cumulative"),
            stock_count=dr.get("stock_count", 0),
        )
        for dr in result.get("daily_returns", [])
    ]

    monthly_returns = [
        EqualWeightStrategyMonthlyReturn(
            month=mr["month"],
            month_return=mr["month_return"],
            cumulative_return=mr["cumulative_return"],
            stock_count=mr.get("stock_count", 0),
            stocks=mr.get("stocks", []),
        )
        for mr in result.get("monthly_returns", [])
    ]

    return EqualWeightStrategyResponse(
        strategy=result["strategy"],
        top_n=result["top_n"],
        period_start_month=result.get("period_start_month"),
        period_end_month=result.get("period_end_month"),
        start_date=result["start_date"],
        end_date=result["end_date"],
        total_months=result["total_months"],
        cumulative_return=result["cumulative_return"],
        daily_returns=daily_returns,
        monthly_returns=monthly_returns,
        rank_stats=result.get("rank_stats", []),
        up_to_down_stats=[
            UpToDownStatItem(**item)
            for item in result.get("up_to_down_stats", [])
        ],
        multi_curves={
            k: [EqualWeightStrategyDailyReturn(
                date=d["date"], daily_return=d.get("return"),
                cumulative=d.get("cumulative"), stock_count=0,
            ) for d in v]
            for k, v in result.get("multi_curves", {}).items()
        },
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


@router.get("/{month}/up-to-down-daily", response_model=UpToDownDailyResponse)
def get_monthly_up_to_down_daily(month: str) -> UpToDownDailyResponse:
    """当月金股池每日九转反转个股（升 1..8 转降、降 1..8 升，末交易日忽略）。"""
    service = BrokerRecommendService()
    result = service.get_monthly_up_to_down_daily(month)
    return UpToDownDailyResponse(
        month=result.get("month", month),
        buy_date=result.get("buy_date", ""),
        sell_date=result.get("sell_date", ""),
        days=[
            UpToDownDailyDayItem(
                date=day["date"],
                stocks=[UpToDownDailyStockItem(**s) for s in day.get("stocks", [])],
            )
            for day in result.get("days", [])
        ],
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
            sector=enrich.get("sector"),
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
            month_cumulative_return=sr.get("month_cumulative_return"),
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
