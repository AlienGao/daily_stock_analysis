# -*- coding: utf-8 -*-
"""Market statistics API endpoints."""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.market import (
    HfqBollPickListResponse,
    HfqKLineItem,
    HfqKLineResponse,
    HfqNewHighListResponse,
)
from src.services.hfq_new_high_service import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT,
    DEFAULT_NEAR_PCT,
    DEFAULT_START_DATE,
    HfqNewHighService,
)

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
