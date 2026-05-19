# -*- coding: utf-8 -*-
"""LightGBM 研究 API 端点。

提供异步模型训练、状态轮询、特征重要性、预测结果、回测对比接口。
"""

import multiprocessing
import threading
import uuid
from datetime import datetime, timedelta
from queue import Empty as QueueEmpty
from typing import Dict

from fastapi import APIRouter, HTTPException, Query

from api.v1.schemas.research import (
    LGBTrainRequest,
    LGBTaskStatusResponse,
    LGBFeatureImportanceResponse,
    LGBPredictionsResponse,
    LGBPredictionItem,
    LGBBacktestCompareResponse,
    LGBModelInfo,
    LGBModelListResponse,
    LGBDateRangeResponse,
    LGBStockLookupItem,
    LGBStockLookupResponse,
)
from src.discovery.ml.lgb_trainer import LGBTrainer
from src.storage import DatabaseManager, FactorScoreSnapshot

router = APIRouter()

_lgb_tasks: Dict[str, dict] = {}


def _cleanup_old_lgb_tasks():
    cutoff = datetime.now() - timedelta(minutes=60)
    stale = [
        tid for tid, t in list(_lgb_tasks.items())
        if t.get("status") in ("completed", "failed")
        and datetime.fromisoformat(
            t.get("finished_at", t.get("started_at", "2000-01-01T00:00:00"))
        ) < cutoff
    ]
    for tid in stale:
        del _lgb_tasks[tid]


def _run_train_in_process(queue: multiprocessing.Queue, req_dict: dict):
    try:
        def _progress(msg: str):
            queue.put(("progress", msg))

        trainer = LGBTrainer(
            mode=req_dict["mode"],
            forward_days=req_dict["forward_days"],
            progress_callback=_progress,
        )
        trainer.prepare_data(
            start_date=req_dict.get("start_date"),
            end_date=req_dict.get("end_date"),
        )

        _progress(f"正在训练 (n_estimators={req_dict.get('n_estimators', 200)})...")
        trainer.train(
            n_estimators=req_dict.get("n_estimators", 200),
            num_leaves=req_dict.get("num_leaves", 31),
            learning_rate=req_dict.get("learning_rate", 0.05),
            cv_folds=req_dict.get("cv_folds", 5),
        )

        _progress("正在生成预测...")
        trainer.predict()

        _progress("正在保存模型...")
        model_path = trainer.save()

        importance = trainer.get_feature_importance()
        predictions = trainer.get_latest_predictions()
        metrics = trainer._training_metrics

        queue.put(("completed", {
            "model_path": model_path,
            "feature_importance": importance,
            "predictions": predictions,
            "training_metrics": metrics,
        }))
    except Exception as e:
        import traceback
        traceback.print_exc()
        queue.put(("failed", str(e)))


def _monitor_train_process(
    task_id: str,
    queue: multiprocessing.Queue,
    proc: multiprocessing.Process,
):
    try:
        while True:
            try:
                msg = queue.get(timeout=2.0)
            except QueueEmpty:
                if not proc.is_alive():
                    exitcode = proc.exitcode
                    t = _lgb_tasks.get(task_id, {})
                    if t.get("status") == "running":
                        t["status"] = "failed"
                        t["error"] = f"训练进程异常退出 (exitcode={exitcode})"
                        t["finished_at"] = datetime.now().isoformat()
                    break
                continue

            msg_type, payload = msg
            if msg_type == "progress":
                t = _lgb_tasks.get(task_id)
                if t:
                    t["status_message"] = payload
            elif msg_type == "completed":
                _lgb_tasks[task_id] = {
                    "status": "completed",
                    "result": payload,
                    "finished_at": datetime.now().isoformat(),
                }
                break
            elif msg_type == "failed":
                _lgb_tasks[task_id] = {
                    "status": "failed",
                    "error": str(payload),
                    "finished_at": datetime.now().isoformat(),
                }
                break
    except Exception as e:
        t = _lgb_tasks.get(task_id, {})
        if t.get("status") == "running":
            t["status"] = "failed"
            t["error"] = f"监控异常: {e}"
            t["finished_at"] = datetime.now().isoformat()
    finally:
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()


@router.post(
    "/lgb/train",
    summary="训练 LightGBM 模型（异步）",
)
def lgb_train(req: LGBTrainRequest):
    if req.mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")

    for tid, t in list(_lgb_tasks.items()):
        if t.get("status") == "running":
            raise HTTPException(
                status_code=409,
                detail=f"已有训练任务运行中（task_id={tid}），请等待完成后再试",
            )

    task_id = str(uuid.uuid4())[:8]
    _lgb_tasks[task_id] = {
        "status": "running",
        "mode": req.mode,
        "started_at": datetime.now().isoformat(),
    }

    req_dict = {
        "mode": req.mode,
        "forward_days": req.forward_days,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "n_estimators": req.n_estimators,
        "num_leaves": req.num_leaves,
        "learning_rate": req.learning_rate,
        "cv_folds": req.cv_folds,
    }

    queue: multiprocessing.Queue = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_run_train_in_process,
        args=(queue, req_dict),
        daemon=True,
    )
    proc.start()

    threading.Thread(
        target=_monitor_train_process,
        args=(task_id, queue, proc),
        daemon=True,
    ).start()

    return {"task_id": task_id, "status": "running"}


@router.get(
    "/lgb/status",
    summary="查询 LightGBM 训练任务状态",
)
def lgb_status(task_id: str = Query(..., description="任务 ID")):
    _cleanup_old_lgb_tasks()

    task = _lgb_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")

    resp = {"task_id": task_id, "status": task.get("status", "unknown")}
    if task.get("status_message"):
        resp["status_message"] = task["status_message"]
    if task.get("status") == "failed":
        resp["error"] = task.get("error", "")
    if task.get("status") == "completed":
        resp["result"] = task.get("result")
    return resp


@router.get(
    "/lgb/feature-importance",
    response_model=LGBFeatureImportanceResponse,
    summary="获取最新模型的特征重要性",
)
def lgb_feature_importance(
    model_path: str = Query(None, description="可选：指定模型路径"),
):
    if model_path:
        trainer = LGBTrainer.load(model_path)
    else:
        completed = [
            (tid, t) for tid, t in _lgb_tasks.items()
            if t.get("status") == "completed"
            and t.get("result", {}).get("model_path")
        ]
        if not completed:
            raise HTTPException(
                status_code=404,
                detail="没有已完成的训练任务，请先调用 POST /research/lgb/train",
            )
        _, latest_task = max(
            completed,
            key=lambda x: x[1].get("finished_at", ""),
        )
        model_path = latest_task["result"]["model_path"]
        trainer = LGBTrainer.load(model_path)

    importance = trainer.get_feature_importance()
    return LGBFeatureImportanceResponse(**importance)


@router.get(
    "/lgb/predictions",
    response_model=LGBPredictionsResponse,
    summary="获取最新的 LGB 预测结果",
)
def lgb_predictions(
    model_path: str = Query(None, description="可选：指定模型路径"),
):
    if model_path:
        trainer = LGBTrainer.load(model_path)
    else:
        completed = [
            (tid, t) for tid, t in _lgb_tasks.items()
            if t.get("status") == "completed"
            and t.get("result", {}).get("model_path")
        ]
        if not completed:
            raise HTTPException(
                status_code=404,
                detail="没有已完成的训练任务",
            )
        _, latest_task = max(
            completed,
            key=lambda x: x[1].get("finished_at", ""),
        )
        model_path = latest_task["result"]["model_path"]
        trainer = LGBTrainer.load(model_path)

    predictions = trainer.get_latest_predictions()
    return LGBPredictionsResponse(
        model_date=trainer._latest_date or "",
        forward_days=trainer.forward_days,
        mode=trainer.mode,
        predictions=[LGBPredictionItem(**p) for p in predictions],
    )


@router.get(
    "/lgb/backtest-compare",
    response_model=LGBBacktestCompareResponse,
    summary="LightGBM vs 因子体系回测对比",
)
def lgb_backtest_compare(
    mode: str = Query("postmarket", description="扫描模式"),
    top_n: int = Query(10, ge=2, le=50),
    forward_days: int = Query(5, ge=1, le=60),
    start_date: str = Query(None),
    end_date: str = Query(None),
    model_path: str = Query(None, description="可选：指定模型路径"),
):
    if model_path:
        trainer = LGBTrainer.load(model_path)
    else:
        trainer = LGBTrainer(mode=mode, forward_days=forward_days)
        try:
            trainer.prepare_data(start_date=start_date, end_date=end_date)
            trainer.train()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    result = trainer.backtest_compare(
        top_n=top_n,
        start_date=start_date,
        end_date=end_date,
    )
    return LGBBacktestCompareResponse(**result)


@router.get(
    "/lgb/date-range",
    response_model=LGBDateRangeResponse,
    summary="获取 factor_score_snapshots 中每种模式的可训练日期范围",
)
def lgb_date_range():
    db = DatabaseManager.get_instance()
    result = {}
    with db.get_session() as session:
        for mode in ("intraday", "postmarket"):
            dates = session.query(FactorScoreSnapshot.trade_date).filter(
                FactorScoreSnapshot.mode == mode,
            ).distinct().order_by(FactorScoreSnapshot.trade_date).all()
            if dates:
                result[mode] = {"min": dates[0][0], "max": dates[-1][0]}
            else:
                result[mode] = None
    return LGBDateRangeResponse(**result)


@router.get(
    "/lgb/models",
    response_model=LGBModelListResponse,
    summary="列出所有已保存的模型",
)
def lgb_list_models():
    models = LGBTrainer.list_models()
    return LGBModelListResponse(
        models=[LGBModelInfo(**m) for m in models],
    )


@router.get(
    "/lgb/stock-lookup",
    response_model=LGBStockLookupResponse,
    summary="查询指定股票的 LGB 预测评分与排名",
)
def lgb_stock_lookup(
    stock_code: str = Query(..., description="股票代码，如 600519"),
    model_path: str = Query(None, description="可选：指定模型路径"),
):
    """在最新预测结果中查找指定个股的评分和全市场排名。"""
    import pandas as pd

    if model_path:
        trainer = LGBTrainer.load(model_path)
    else:
        completed = [
            (tid, t) for tid, t in _lgb_tasks.items()
            if t.get("status") == "completed"
            and t.get("result", {}).get("model_path")
        ]
        if not completed:
            raise HTTPException(
                status_code=404,
                detail="没有已完成的训练任务，请先训练模型",
            )
        _, latest_task = max(
            completed,
            key=lambda x: x[1].get("finished_at", ""),
        )
        model_path = latest_task["result"]["model_path"]
        trainer = LGBTrainer.load(model_path)

    df = trainer.predict()
    code = str(stock_code).strip().zfill(6)

    if df is None or df.empty:
        return LGBStockLookupResponse(found=False, message="预测结果为空")

    # 按 stock_code（去后缀）或 ts_code 匹配
    mask = (df["stock_code"] == code) | (df["ts_code"] == code)
    if not mask.any():
        # 尝试带后缀匹配
        for sfx in [".SH", ".SZ", ".BJ"]:
            mask = df["ts_code"] == f"{code}{sfx}"
            if mask.any():
                break

    if not mask.any():
        return LGBStockLookupResponse(
            found=False,
            message=f"未找到 {stock_code} 的预测数据，请确认代码正确且该股票在当次扫描范围内",
        )

    row = df[mask].iloc[0]
    rank = int(df["lgb_score"].rank(ascending=False).loc[row.name])

    return LGBStockLookupResponse(
        found=True,
        item=LGBStockLookupItem(
            stock_code=str(row["stock_code"]),
            ts_code=str(row["ts_code"]),
            rank=rank,
            lgb_score=round(float(row["lgb_score_norm"]), 4),
            raw_score=round(float(row["lgb_score"]), 4),
            total_stocks=len(df),
        ),
    )
