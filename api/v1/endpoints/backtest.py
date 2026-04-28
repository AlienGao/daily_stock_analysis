# -*- coding: utf-8 -*-
"""Backtest endpoints."""

from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, Literal, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from api.deps import get_database_manager
from api.v1.schemas.backtest import (
    BacktestRunRequest,
    BacktestRunResponse,
    BacktestTaskAcceptedResponse,
    BacktestTaskStatusResponse,
    BacktestResultItem,
    BacktestResultsResponse,
    PerformanceMetrics,
)
from api.v1.schemas.common import ErrorResponse
from src.services.backtest_service import BacktestService
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

router = APIRouter()
_TASK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="backtest_task_")
_TASK_LOCK = threading.RLock()
_TASKS: Dict[str, Dict[str, Any]] = {}
_MAX_TASK_HISTORY = 100


def _task_status_payload(task: Dict[str, Any]) -> BacktestTaskStatusResponse:
    return BacktestTaskStatusResponse(
        task_id=task["task_id"],
        status=task["status"],
        progress=task["progress"],
        message=task.get("message"),
        result=BacktestRunResponse(**task["result"]) if task.get("result") else None,
        error=task.get("error"),
        created_at=task["created_at"],
        started_at=task.get("started_at"),
        completed_at=task.get("completed_at"),
    )


def _trim_task_history_locked() -> None:
    if len(_TASKS) <= _MAX_TASK_HISTORY:
        return
    removable = sorted(
        (
            task
            for task in _TASKS.values()
            if task.get("status") in {"completed", "failed"}
        ),
        key=lambda x: x.get("completed_at") or x.get("created_at") or "",
    )
    for task in removable[: max(0, len(_TASKS) - _MAX_TASK_HISTORY)]:
        _TASKS.pop(task["task_id"], None)


def _run_backtest_task(task_id: str, request_data: Dict[str, Any], db_manager: DatabaseManager) -> None:
    with _TASK_LOCK:
        task = _TASKS.get(task_id)
        if task is None:
            return
        task["status"] = "processing"
        task["progress"] = 15
        task["started_at"] = datetime.now().isoformat()
        task["message"] = "回测任务执行中..."

    try:
        service = BacktestService(db_manager)
        stats = service.run_backtest(
            code=request_data.get("code"),
            force=bool(request_data.get("force", False)),
            eval_window_days=request_data.get("eval_window_days"),
            min_age_days=request_data.get("min_age_days"),
            limit=int(request_data.get("limit") or 200),
            allowed_categories=request_data.get("allowed_categories"),
            sentiment_score_min=request_data.get("sentiment_score_min"),
            sentiment_score_max=request_data.get("sentiment_score_max"),
            trigger_source="manual",
        )
        with _TASK_LOCK:
            task = _TASKS.get(task_id)
            if task is None:
                return
            task["status"] = "completed"
            task["progress"] = 100
            task["completed_at"] = datetime.now().isoformat()
            task["message"] = "回测任务完成"
            task["result"] = stats
            task["error"] = None
            _trim_task_history_locked()
    except Exception as exc:  # pragma: no cover - defensive branch
        logger.error("异步回测任务失败(%s): %s", task_id, exc, exc_info=True)
        with _TASK_LOCK:
            task = _TASKS.get(task_id)
            if task is None:
                return
            task["status"] = "failed"
            task["progress"] = 100
            task["completed_at"] = datetime.now().isoformat()
            task["message"] = "回测任务失败"
            task["error"] = str(exc)
            _trim_task_history_locked()


def _validate_analysis_date_range(
    analysis_date_from: Optional[date],
    analysis_date_to: Optional[date],
) -> None:
    if analysis_date_from and analysis_date_to and analysis_date_from > analysis_date_to:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_params",
                "message": "analysis_date_from cannot be after analysis_date_to",
            },
        )


@router.post(
    "/run",
    response_model=Union[BacktestRunResponse, BacktestTaskAcceptedResponse],
    responses={
        200: {"description": "回测执行完成"},
        202: {"description": "回测任务已接受", "model": BacktestTaskAcceptedResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="触发回测",
    description="对历史分析记录进行回测评估，并写入 backtest_results/backtest_summaries",
)
def run_backtest(
    request: BacktestRunRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> Union[BacktestRunResponse, JSONResponse]:
    if request.async_mode:
        task_id = uuid.uuid4().hex
        with _TASK_LOCK:
            _TASKS[task_id] = {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "回测任务已加入队列",
                "created_at": datetime.now().isoformat(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
            }
        _TASK_EXECUTOR.submit(
            _run_backtest_task,
            task_id,
            request.model_dump(),
            db_manager,
        )
        accepted = BacktestTaskAcceptedResponse(
            task_id=task_id,
            status="pending",
            message="回测任务已提交，正在后台执行",
        )
        return JSONResponse(status_code=202, content=accepted.model_dump())

    try:
        service = BacktestService(db_manager)
        stats = service.run_backtest(
            code=request.code,
            force=request.force,
            eval_window_days=request.eval_window_days,
            min_age_days=request.min_age_days,
            limit=request.limit,
            allowed_categories=request.allowed_categories,
            sentiment_score_min=request.sentiment_score_min,
            sentiment_score_max=request.sentiment_score_max,
            trigger_source="manual",
        )
        return BacktestRunResponse(**stats)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    except Exception as exc:
        logger.error(f"回测执行失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"回测执行失败: {str(exc)}"},
        )


@router.get(
    "/tasks/{task_id}",
    response_model=BacktestTaskStatusResponse,
    responses={
        200: {"description": "回测任务状态"},
        404: {"description": "任务不存在", "model": ErrorResponse},
    },
    summary="查询回测任务状态",
)
def get_backtest_task_status(task_id: str) -> BacktestTaskStatusResponse:
    with _TASK_LOCK:
        task = _TASKS.get(task_id)
        if task is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "回测任务不存在或已过期"},
            )
        return _task_status_payload(task)


@router.get(
    "/results",
    response_model=BacktestResultsResponse,
    responses={
        200: {"description": "回测结果列表"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取回测结果",
    description="分页获取回测结果，支持按股票代码过滤",
)
def get_backtest_results(
    code: Optional[str] = Query(None, description="股票代码筛选"),
    trigger_source: Optional[str] = Query(None, description="触发来源筛选（auto/manual）"),
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="评估窗口过滤"),
    analysis_date_from: Optional[date] = Query(None, description="分析日期起始（含）"),
    analysis_date_to: Optional[date] = Query(None, description="分析日期结束（含）"),
    sort_by: Literal["analysis_date", "actual_return_pct", "sentiment_score"] = Query("analysis_date", description="排序字段"),
    sort_order: Literal["asc", "desc"] = Query("desc", description="排序方向"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(20, ge=1, le=200, description="每页数量"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> BacktestResultsResponse:
    try:
        _validate_analysis_date_range(analysis_date_from, analysis_date_to)
        service = BacktestService(db_manager)
        data = service.get_recent_evaluations(
            code=code,
            trigger_source=trigger_source,
            eval_window_days=eval_window_days,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            page=page,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
        )
        items = [BacktestResultItem(**item) for item in data.get("items", [])]
        return BacktestResultsResponse(
            total=int(data.get("total", 0)),
            page=page,
            limit=limit,
            items=items,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"查询回测结果失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"查询回测结果失败: {str(exc)}"},
        )


@router.get(
    "/performance",
    response_model=PerformanceMetrics,
    responses={
        200: {"description": "整体回测表现"},
        404: {"description": "无回测汇总", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取整体回测表现",
)
def get_overall_performance(
    trigger_source: Optional[str] = Query(None, description="触发来源筛选（auto/manual）"),
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="评估窗口过滤"),
    analysis_date_from: Optional[date] = Query(None, description="分析日期起始（含）"),
    analysis_date_to: Optional[date] = Query(None, description="分析日期结束（含）"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PerformanceMetrics:
    try:
        _validate_analysis_date_range(analysis_date_from, analysis_date_to)
        service = BacktestService(db_manager)
        summary = service.get_summary(
            scope="overall",
            code=None,
            trigger_source=trigger_source,
            eval_window_days=eval_window_days,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
        )
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": "未找到整体回测汇总"},
            )
        return PerformanceMetrics(**summary)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"查询整体表现失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"查询整体表现失败: {str(exc)}"},
        )


@router.get(
    "/performance/{code}",
    response_model=PerformanceMetrics,
    responses={
        200: {"description": "单股回测表现"},
        404: {"description": "无回测汇总", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取单股回测表现",
)
def get_stock_performance(
    code: str,
    trigger_source: Optional[str] = Query(None, description="触发来源筛选（auto/manual）"),
    eval_window_days: Optional[int] = Query(None, ge=1, le=120, description="评估窗口过滤"),
    analysis_date_from: Optional[date] = Query(None, description="分析日期起始（含）"),
    analysis_date_to: Optional[date] = Query(None, description="分析日期结束（含）"),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> PerformanceMetrics:
    try:
        _validate_analysis_date_range(analysis_date_from, analysis_date_to)
        service = BacktestService(db_manager)
        summary = service.get_summary(
            scope="stock",
            code=code,
            trigger_source=trigger_source,
            eval_window_days=eval_window_days,
            analysis_date_from=analysis_date_from,
            analysis_date_to=analysis_date_to,
        )
        if summary is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "message": f"未找到 {code} 的回测汇总"},
            )
        return PerformanceMetrics(**summary)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"查询单股表现失败: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": f"查询单股表现失败: {str(exc)}"},
        )
