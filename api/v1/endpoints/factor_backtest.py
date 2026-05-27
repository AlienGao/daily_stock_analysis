# -*- coding: utf-8 -*-
"""独立因子回测 API 端点。

简化版因子回测：统一使用开盘价交易，不分 intraday/postmarket 模式。
"""

import logging
import multiprocessing
import uuid
from datetime import datetime
from queue import Empty as QueueEmpty
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

# 任务状态存储
_tasks: Dict[str, dict] = {}


class BacktestRequest(BaseModel):
    factor_weights: Dict[str, float] = {}
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    top_n: int = 5
    hold_days: List[int] = [1, 3, 5, 10, 20]
    initial_capital: float = 1_000_000.0
    risk_free_rate: float = 0.02


def _run_in_process(queue: multiprocessing.Queue, req_dict: dict):
    """在独立进程中运行回测。"""
    try:
        from data_provider.tushare_fetcher import TushareFetcher
        from src.discovery.factor_backtest_engine import FactorBacktestEngine

        fetcher = TushareFetcher.get_instance()
        engine = FactorBacktestEngine(fetcher)

        fw = None
        if req_dict.get("factor_weights"):
            non_zero = {k: v for k, v in req_dict["factor_weights"].items() if v > 0}
            if non_zero:
                fw = non_zero

        def _progress(msg: str):
            queue.put(("progress", msg))

        result = engine.compute(
            mode="postmarket",  # 固定使用 postmarket（开盘价交易）
            factor_weights=fw,
            start_date=req_dict.get("start_date"),
            end_date=req_dict.get("end_date"),
            top_n=req_dict.get("top_n"),
            hold_days=req_dict.get("hold_days"),
            initial_capital=req_dict.get("initial_capital"),
            risk_free_rate=req_dict.get("risk_free_rate"),
            progress_cb=_progress,
        )

        if result is None:
            queue.put(("failed", "回测数据不足，请检查日期范围或因子选择"))
        else:
            from dataclasses import asdict
            queue.put(("completed", asdict(result)))
    except Exception as e:
        import traceback
        traceback.print_exc()
        queue.put(("failed", str(e)))


def _monitor(task_id: str, queue: multiprocessing.Queue, proc: multiprocessing.Process):
    """监控线程：从 Queue 读取进度/结果。"""
    logger.info("回测监控启动 task_id=%s", task_id)
    try:
        while True:
            try:
                msg = queue.get(timeout=2.0)
            except QueueEmpty:
                if not proc.is_alive():
                    exitcode = proc.exitcode
                    t = _tasks.get(task_id, {})
                    if t.get("status") == "running":
                        t["status"] = "failed"
                        t["error"] = f"回测进程异常退出 (exitcode={exitcode})"
                        t["finished_at"] = datetime.now().isoformat()
                    break
                continue

            msg_type, payload = msg
            if msg_type == "progress":
                t = _tasks.get(task_id)
                if t:
                    t["status_message"] = payload
            elif msg_type == "completed":
                _tasks[task_id] = {
                    "status": "completed",
                    "result": payload,
                    "finished_at": datetime.now().isoformat(),
                }
                break
            elif msg_type == "failed":
                t = _tasks.get(task_id, {})
                t["status"] = "failed"
                t["error"] = payload
                t["finished_at"] = datetime.now().isoformat()
                break
    except Exception as e:
        logger.exception("监控线程异常 task_id=%s", task_id)
        t = _tasks.get(task_id, {})
        if t.get("status") == "running":
            t["status"] = "failed"
            t["error"] = str(e)
            t["finished_at"] = datetime.now().isoformat()


def _cleanup_old():
    """清理超过 60 分钟的旧任务。"""
    now = datetime.now()
    expired = []
    for tid, t in _tasks.items():
        try:
            finished = t.get("finished_at")
            if finished:
                dt = datetime.fromisoformat(finished)
                if (now - dt).total_seconds() > 3600:
                    expired.append(tid)
        except Exception:
            pass
    for tid in expired:
        _tasks.pop(tid, None)


@router.get("/factors", summary="获取可用因子列表")
def list_factors():
    """返回所有可用因子及其默认权重。"""
    try:
        from src.discovery.factors import __all__ as all_factors
        from src.discovery.factors.base import BaseFactor

        factors = []
        for name in all_factors:
            if name in ("BaseFactor", "DiscoveryResult"):
                continue
            try:
                mod = __import__(f"src.discovery.factors", fromlist=[name])
                cls = getattr(mod, name)
                if isinstance(cls, type) and issubclass(cls, BaseFactor) and cls is not BaseFactor:
                    inst = cls()
                    if inst.available_postmarket:
                        factors.append({
                            "name": inst.name,
                            "weight": inst.weight,
                        })
            except Exception:
                pass
        return {"factors": factors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run", summary="提交因子回测任务")
def run_backtest(req: BacktestRequest):
    """提交因子回测任务（异步执行，统一使用开盘价交易）。"""
    _cleanup_old()

    # 检查是否已有任务在运行
    for tid, t in _tasks.items():
        if t.get("status") == "running":
            raise HTTPException(status_code=429, detail="已有回测任务在运行，请等待完成")

    task_id = uuid.uuid4().hex[:8]
    _tasks[task_id] = {
        "status": "running",
        "started_at": datetime.now().isoformat(),
    }

    queue = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_run_in_process,
        args=(queue, req.model_dump()),
        daemon=True,
    )
    proc.start()

    import threading
    t = threading.Thread(target=_monitor, args=(task_id, queue, proc), daemon=True)
    t.start()

    return {"task_id": task_id, "status": "running"}


@router.get("/status", summary="查询回测任务状态")
def task_status(task_id: str = Query(...)):
    """查询回测任务状态。"""
    _cleanup_old()
    t = _tasks.get(task_id)
    if not t:
        raise HTTPException(status_code=404, detail="任务不存在")
    resp = {"task_id": task_id, "status": t["status"]}
    if "status_message" in t:
        resp["status_message"] = t["status_message"]
    if "error" in t:
        resp["error"] = t["error"]
    if "result" in t:
        resp["result"] = t["result"]
    return resp
