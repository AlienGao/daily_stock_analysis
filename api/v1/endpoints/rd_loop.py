# -*- coding: utf-8 -*-
"""R&D 闭环因子审核 API 端点。

提供待审核因子列表查询、批准和拒绝操作。
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class PendingFactor(BaseModel):
    name: str
    path: str = ""
    hypothesis: str = ""
    score: float = 0.0
    cum_return: float = 0.0
    sharpe: float = 0.0
    win_rate: float = 0.0
    code: str = ""


class PendingListResponse(BaseModel):
    total: int
    items: List[PendingFactor]


class ApproveRejectRequest(BaseModel):
    factor_name: str


class ActionResponse(BaseModel):
    ok: bool
    message: str = ""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/pending", response_model=PendingListResponse)
async def list_pending_factors():
    """列出所有待审核的 R&D 因子。"""
    try:
        from src.discovery.rd_loop import RDLoop

        items = RDLoop.list_pending_factors()
        return PendingListResponse(
            total=len(items),
            items=[
                PendingFactor(
                    name=f.get("name", ""),
                    path=f.get("path", ""),
                    hypothesis=f.get("hypothesis", ""),
                    score=f.get("score", 0.0),
                    cum_return=f.get("cum_return", 0.0),
                    sharpe=f.get("sharpe", 0.0),
                    win_rate=f.get("win_rate", 0.0),
                    code=f.get("code", ""),
                )
                for f in items
            ],
        )
    except Exception as e:
        logger.exception("[RDLoopAPI] 列出待审核因子失败: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/approve", response_model=ActionResponse)
async def approve_factor(req: ApproveRejectRequest):
    """批准一个待审核因子，将其从 pending/ 移动到 factors/。"""
    try:
        from src.discovery.rd_loop import RDLoop

        ok = RDLoop.approve_factor(req.factor_name)
        if ok:
            return ActionResponse(ok=True, message=f"因子已批准: {req.factor_name}")
        return ActionResponse(ok=False, message=f"因子不存在: {req.factor_name}")
    except Exception as e:
        logger.exception("[RDLoopAPI] 批准因子失败: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reject", response_model=ActionResponse)
async def reject_factor(req: ApproveRejectRequest):
    """拒绝一个待审核因子，删除 pending 文件。"""
    try:
        from src.discovery.rd_loop import RDLoop

        ok = RDLoop.reject_factor(req.factor_name)
        if ok:
            return ActionResponse(ok=True, message=f"因子已拒绝: {req.factor_name}")
        return ActionResponse(ok=False, message=f"因子不存在: {req.factor_name}")
    except Exception as e:
        logger.exception("[RDLoopAPI] 拒绝因子失败: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
