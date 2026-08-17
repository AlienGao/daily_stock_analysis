# -*- coding: utf-8 -*-
"""Market statistics API endpoints."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.market import (
    AIndexBollPickListResponse,
    AIndexConstituentItem,
    AIndexConstituentResponse,
    AIndexKLineItem,
    AIndexKLineResponse,
    AIndexNewHighItem,
    AIndexNewHighListResponse,
    AIndexListResponse,
    EtfBollPickListResponse,
    EtfKLineItem,
    EtfKLineResponse,
    EtfNewHighListResponse,
    GlobalIndexBollPickListResponse,
    GlobalIndexKLineItem,
    GlobalIndexKLineResponse,
    GlobalIndexNewHighListResponse,
    HfqBollPickListResponse,
    HfqKLineItem,
    HfqKLineResponse,
    HfqNewHighListResponse,
    HkGgtComponentListResponse,
    HkGgtMinuteBarListResponse,
    HkGgtPollResponse,
    HkBollPickListResponse,
    HkStockListResponse,
    HkStockKLineResponse,
    HkStockRealtimeResponse,
)
from src.services.hk_ggt_monitor_service import HkGgtMonitorService
from src.services.hk_stock_service import HkStockService
from src.services.hfq_new_high_service import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT,
    DEFAULT_NEAR_PCT,
    DEFAULT_START_DATE,
    HfqNewHighService,
)
from src.services.etf_new_high_service import EtfNewHighService
from src.services.global_index_new_high_service import GlobalIndexNewHighService
from src.services.a_index_new_high_service import AIndexNewHighService, compute_latest_boll, _band_distance_pct

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/hfq-new-highs",
    response_model=HfqNewHighListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="后复权收盘创新高列表",
    description="扫描全 A 股自 start_date 以来后复权收盘价创新高记录，按最近创新高日降序",
)
def list_hfq_new_highs(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
) -> HfqNewHighListResponse:
    try:
        result = HfqNewHighService().scan_new_highs(
            start_date=start_date,
            as_of_date=as_of_date,
            refresh=refresh,
        )
        return HfqNewHighListResponse(**result)
    except Exception as exc:
        logger.error("hfq-new-highs failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"扫描失败: {exc}"},
        ) from exc


@router.get(
    "/hfq-new-highs/boll-picks",
    response_model=HfqBollPickListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="BOLL 中轨/下轨附近且近月创新高推荐",
    description="筛选近 lookback_days 内创新高、后复权现价靠近 BOLL(20,2) 中轨或下轨的个股",
)
def list_hfq_boll_picks(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
    near_pct: float = Query(DEFAULT_NEAR_PCT, ge=0.5, le=20.0, description="靠近轨道阈值（%）"),
    lookback_days: int = Query(DEFAULT_LOOKBACK_DAYS, ge=7, le=90, description="最近创新高回溯自然日"),
    max_drawdown_from_high_pct: float = Query(
        DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT,
        ge=1.0,
        le=50.0,
        description="距最近新高最大回撤（%），超出则排除",
    ),
) -> HfqBollPickListResponse:
    try:
        result = HfqNewHighService().scan_boll_near_picks(
            start_date=start_date,
            as_of_date=as_of_date,
            refresh=refresh,
            near_pct=near_pct,
            lookback_days=lookback_days,
            max_drawdown_from_high_pct=max_drawdown_from_high_pct,
        )
        return HfqBollPickListResponse(**result)
    except Exception as exc:
        logger.error("hfq-new-highs boll-picks failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"扫描失败: {exc}"},
        ) from exc


@router.get(
    "/hfq-new-highs/{stock_code}/klines",
    response_model=HfqKLineResponse,
    responses={500: {"model": ErrorResponse}},
    summary="单股后复权 K 线",
)
def get_hfq_new_high_klines(
    stock_code: str,
    start_date: str = Query(DEFAULT_START_DATE, description="起始日 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日 YYYYMMDD"),
) -> HfqKLineResponse:
    try:
        service = HfqNewHighService()
        rows = service.get_hfq_klines(stock_code, start_date=start_date, end_date=end_date)
        end = end_date or date.today().strftime("%Y%m%d")
        data = [HfqKLineItem(**row) for row in rows]
        bare = str(stock_code).split(".")[0].strip().zfill(6)
        return HfqKLineResponse(
            stock_code=bare,
            start_date=start_date,
            end_date=end,
            data=data,
        )
    except Exception as exc:
        logger.error("hfq klines failed for %s: %s", stock_code, exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"获取 K 线失败: {exc}"},
        ) from exc


@router.get(
    "/etf-new-highs",
    response_model=EtfNewHighListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="ETF 收盘价新高列表",
    description="扫描全 ETF 自 start_date 以来收盘价创新高记录，按最近创新高日降序",
)
def list_etf_new_highs(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
) -> EtfNewHighListResponse:
    try:
        result = EtfNewHighService().scan_new_highs(
            start_date=start_date,
            as_of_date=as_of_date,
            refresh=refresh,
        )
        return EtfNewHighListResponse(**result)
    except Exception as exc:
        logger.error("etf-new-highs failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"扫描失败: {exc}"},
        ) from exc


@router.get(
    "/etf-new-highs/boll-picks",
    response_model=EtfBollPickListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="ETF BOLL 中轨/下轨附近且近月创新高推荐",
    description="筛选近 lookback_days 内创新高、收盘价靠近 BOLL(20,2) 中轨或下轨的 ETF",
)
def list_etf_boll_picks(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
    near_pct: float = Query(DEFAULT_NEAR_PCT, ge=0.5, le=20.0, description="靠近轨道阈值（%）"),
    lookback_days: int = Query(DEFAULT_LOOKBACK_DAYS, ge=7, le=90, description="最近创新高回溯自然日"),
    max_drawdown_from_high_pct: float = Query(
        30.0,
        ge=1.0,
        le=50.0,
        description="距最近新高最大回撤（%），超出则排除",
    ),
) -> EtfBollPickListResponse:
    try:
        result = EtfNewHighService().scan_boll_near_picks(
            start_date=start_date,
            as_of_date=as_of_date,
            refresh=refresh,
            near_pct=near_pct,
            lookback_days=lookback_days,
            max_drawdown_from_high_pct=max_drawdown_from_high_pct,
        )
        return EtfBollPickListResponse(**result)
    except Exception as exc:
        logger.error("etf-new-highs boll-picks failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"扫描失败: {exc}"},
        ) from exc


@router.get(
    "/etf-new-highs/{stock_code}/klines",
    response_model=EtfKLineResponse,
    responses={500: {"model": ErrorResponse}},
    summary="单只 ETF 日 K 线",
)
def get_etf_new_high_klines(
    stock_code: str,
    start_date: str = Query(DEFAULT_START_DATE, description="起始日 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日 YYYYMMDD"),
) -> EtfKLineResponse:
    try:
        service = EtfNewHighService()
        rows = service.get_klines(stock_code, start_date=start_date, end_date=end_date)
        end = end_date or date.today().strftime("%Y%m%d")
        data = [EtfKLineItem(**row) for row in rows]
        bare = str(stock_code).split(".")[0].strip().zfill(6)
        return EtfKLineResponse(
            stock_code=bare,
            start_date=start_date,
            end_date=end,
            data=data,
        )
    except Exception as exc:
        logger.error("etf klines failed for %s: %s", stock_code, exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"获取 K 线失败: {exc}"},
        ) from exc


@router.get(
    "/global-index-new-highs",
    response_model=GlobalIndexNewHighListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="全球主要指数新高列表",
    description="扫描全球主要指数自 start_date 以来收盘价创新高记录",
)
def list_global_index_new_highs(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
) -> GlobalIndexNewHighListResponse:
    try:
        result = GlobalIndexNewHighService().scan_new_highs(
            start_date=start_date,
            as_of_date=as_of_date,
            refresh=refresh,
        )
        return GlobalIndexNewHighListResponse(**result)
    except Exception as exc:
        logger.error("global-index-new-highs failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"扫描失败: {exc}"},
        ) from exc


@router.get(
    "/global-index-new-highs/{ts_code}/klines",
    response_model=GlobalIndexKLineResponse,
    responses={500: {"model": ErrorResponse}},
    summary="单只全球指数日 K 线",
)
def get_global_index_klines(
    ts_code: str,
    start_date: str = Query(DEFAULT_START_DATE, description="起始日 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日 YYYYMMDD"),
) -> GlobalIndexKLineResponse:
    try:
        service = GlobalIndexNewHighService()
        rows = service.get_klines(ts_code, start_date=start_date, end_date=end_date)
        end = end_date or date.today().strftime("%Y%m%d")
        data = [GlobalIndexKLineItem(**row) for row in rows]
        return GlobalIndexKLineResponse(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end,
            data=data,
        )
    except Exception as exc:
        logger.error("global index klines failed for %s: %s", ts_code, exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"获取 K 线失败: {exc}"},
        ) from exc


@router.get(
    "/global-index-new-highs/boll-picks",
    response_model=GlobalIndexBollPickListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="全球主要指数 BOLL 推荐",
)
def list_global_index_boll_picks(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
    near_pct: float = Query(DEFAULT_NEAR_PCT, ge=0.5, le=20.0),
    lookback_days: int = Query(DEFAULT_LOOKBACK_DAYS, ge=7, le=90),
    max_drawdown_from_high_pct: float = Query(30.0, ge=1.0, le=50.0),
) -> GlobalIndexBollPickListResponse:
    try:
        result = GlobalIndexNewHighService().scan_boll_near_picks(
            start_date=start_date, as_of_date=as_of_date, refresh=refresh,
            near_pct=near_pct, lookback_days=lookback_days,
            max_drawdown_from_high_pct=max_drawdown_from_high_pct,
        )
        return GlobalIndexBollPickListResponse(**result)
    except Exception as exc:
        logger.error("global-index-new-highs boll-picks failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": f"扫描失败: {exc}"}) from exc


@router.get(
    "/a-index-list",
    response_model=AIndexListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="A 股指数列表",
)
def list_a_indices() -> AIndexListResponse:
    try:
        items = AIndexNewHighService().list_indices()
        return AIndexListResponse(total=len(items), items=items)
    except Exception as exc:
        logger.error("a-index-list failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.get(
    "/a-index-new-highs",
    response_model=AIndexNewHighListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="A 股指数新高列表（日线/周线）",
)
def list_a_index_new_highs(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
    freq: str = Query("daily", description="频率: daily 或 weekly"),
) -> AIndexNewHighListResponse:
    try:
        result = AIndexNewHighService().scan_new_highs(
            start_date=start_date, as_of_date=as_of_date, refresh=refresh, freq=freq,
        )
        return AIndexNewHighListResponse(**result)
    except Exception as exc:
        logger.error("a-index-new-highs failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": f"扫描失败: {exc}"}) from exc


@router.get(
    "/a-index-new-highs/boll-picks",
    response_model=AIndexBollPickListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="A 股指数 BOLL 推荐（日线/周线）",
)
def list_a_index_boll_picks(
    start_date: str = Query(DEFAULT_START_DATE, description="统计起始日 YYYYMMDD"),
    as_of_date: Optional[str] = Query(None, description="截止日 YYYYMMDD，默认今日"),
    refresh: bool = Query(False, description="强制刷新缓存"),
    near_pct: float = Query(DEFAULT_NEAR_PCT, ge=0.5, le=20.0),
    lookback_days: int = Query(DEFAULT_LOOKBACK_DAYS, ge=7, le=90),
    max_drawdown_from_high_pct: float = Query(30.0, ge=1.0, le=50.0),
    freq: str = Query("daily", description="频率: daily 或 weekly"),
) -> AIndexBollPickListResponse:
    try:
        result = AIndexNewHighService().scan_boll_near_picks(
            start_date=start_date, as_of_date=as_of_date, refresh=refresh,
            near_pct=near_pct, lookback_days=lookback_days,
            max_drawdown_from_high_pct=max_drawdown_from_high_pct, freq=freq,
        )
        return AIndexBollPickListResponse(**result)
    except Exception as exc:
        logger.error("a-index-new-highs boll-picks failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": f"扫描失败: {exc}"}) from exc


@router.get(
    "/a-index-new-highs/{ts_code}/klines",
    response_model=AIndexKLineResponse,
    responses={500: {"model": ErrorResponse}},
    summary="A 股指数日/周 K 线",
)
def get_a_index_klines(
    ts_code: str,
    start_date: str = Query(DEFAULT_START_DATE, description="起始日 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日 YYYYMMDD"),
    freq: str = Query("daily", description="频率: daily 或 weekly"),
) -> AIndexKLineResponse:
    try:
        service = AIndexNewHighService()
        rows = service.get_klines(ts_code, start_date=start_date, end_date=end_date, freq=freq)
        end = end_date or date.today().strftime("%Y%m%d")
        data = [AIndexKLineItem(**row) for row in rows]
        return AIndexKLineResponse(ts_code=ts_code, start_date=start_date, end_date=end, data=data, freq=freq)
    except Exception as exc:
        logger.error("a-index klines failed for %s: %s", ts_code, exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": f"获取 K 线失败: {exc}"}) from exc


@router.get(
    "/a-index-constituents/{index_code}",
    response_model=AIndexConstituentResponse,
    responses={500: {"model": ErrorResponse}},
    summary="A 股指数成分股列表",
)
def list_a_index_constituents(
    index_code: str,
) -> AIndexConstituentResponse:
    try:
        items = AIndexNewHighService().list_constituents(index_code)
        return AIndexConstituentResponse(index_code=index_code, total=len(items), items=items)
    except Exception as exc:
        logger.error("a-index-constituents failed for %s: %s", index_code, exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.post(
    "/a-index-clear-disallowed",
    response_model=dict,
    responses={500: {"model": ErrorResponse}},
    summary="清除非宽基/申万一级行业的存量 A 股指数数据",
)
def clear_disallowed_index_data() -> dict:
    try:
        result = AIndexNewHighService().clear_non_allowed_data()
        return {"deleted": result, "message": "已清除非宽基/申万一级行业指数数据"}
    except Exception as exc:
        logger.error("a-index-clear-disallowed failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.get(
    "/hk-ggt/components",
    response_model=HkGgtComponentListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="港股通成份股快照",
)
def list_hk_ggt_components(
    trade_date: Optional[str] = Query(None, description="交易日 YYYYMMDD，默认最新"),
    refresh: bool = Query(False, description="强制刷新 AkShare 成份"),
) -> HkGgtComponentListResponse:
    try:
        result = HkGgtMonitorService().list_components(trade_date, refresh=refresh)
        return HkGgtComponentListResponse(**result)
    except Exception as exc:
        logger.error("hk-ggt/components failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc



@router.get(
    "/hk-stocks",
    response_model=HkStockListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="港股通成份股列表（含最新价）",
)
def list_hk_stocks(
    refresh: bool = Query(False, description="强制刷新港股通成份快照后再返回列表"),
) -> HkStockListResponse:
    try:
        result = HkStockService().list_components(refresh=refresh)
        return HkStockListResponse(**result)
    except Exception as exc:
        logger.error("hk-stocks failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.get(
    "/hk-stocks/realtime",
    response_model=HkStockRealtimeResponse,
    responses={500: {"model": ErrorResponse}},
    summary="港股通最新分钟价、日内连续最大回撤与分钟涨幅榜",
)
def get_hk_stock_realtime() -> HkStockRealtimeResponse:
    try:
        result = HkGgtMonitorService().get_realtime_snapshot()
        return HkStockRealtimeResponse(**result)
    except Exception as exc:
        logger.error("hk-stocks realtime failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.get(
    "/hk-stocks/{hk_code}/klines",
    response_model=HkStockKLineResponse,
    responses={500: {"model": ErrorResponse}},
    summary="港股通个股日 K 线（BOLL 叠加）",
)
def get_hk_stock_klines(
    hk_code: str,
    start_date: Optional[str] = Query(None, description="起始日 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="截止日 YYYYMMDD"),
) -> HkStockKLineResponse:
    try:
        result = HkStockService().get_klines(hk_code, start_date=start_date, end_date=end_date)
        return HkStockKLineResponse(**result)
    except Exception as exc:
        logger.error("hk-stocks klines failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.get(
    "/hk-stocks/boll-picks",
    response_model=HkBollPickListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="港股通 BOLL 推荐（上轨/中轨/下轨附近 ±1.5%）",
)
def list_hk_boll_picks(
    near_pct: float = Query(1.5, description="距轨道阈值（%）"),
) -> HkBollPickListResponse:
    try:
        result = HkStockService().scan_boll_picks(near_pct=near_pct)
        return HkBollPickListResponse(**result)
    except Exception as exc:
        logger.error("hk-stocks boll-picks failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.get(
    "/hk-ggt/{hk_code}/minutes",
    response_model=HkGgtMinuteBarListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="港股通个股分钟行情",
)
def get_hk_ggt_minutes(
    hk_code: str,
    trade_date: Optional[str] = Query(None, description="交易日 YYYYMMDD，默认今日"),
) -> HkGgtMinuteBarListResponse:
    try:
        result = HkGgtMonitorService().get_minute_bars(hk_code, trade_date)
        return HkGgtMinuteBarListResponse(**result)
    except Exception as exc:
        logger.error("hk-ggt minutes failed for %s: %s", hk_code, exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc


@router.post(
    "/hk-ggt/poll",
    response_model=HkGgtPollResponse,
    responses={500: {"model": ErrorResponse}},
    summary="手动触发一轮腾讯港股实时行情轮询落库",
)
def poll_hk_ggt_rt() -> HkGgtPollResponse:
    try:
        result = HkGgtMonitorService().poll_rt_once()
        return HkGgtPollResponse(**result)
    except Exception as exc:
        logger.error("hk-ggt poll failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "internal_error", "message": str(exc)}) from exc
