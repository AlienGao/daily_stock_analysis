# -*- coding: utf-8 -*-
"""LightGBM 研究 API 端点。

提供异步模型训练、状态轮询、特征重要性、预测结果、回测对比接口。
"""

import multiprocessing
import os
import sys
import threading
import uuid
from datetime import datetime, timedelta
from queue import Empty as QueueEmpty
from typing import Dict, Optional

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
    LGBBacktestSimResponse,
    LGBBacktestSimMetrics,
    LGBBacktestTradeItem,
    LGBBacktestSimAvailableResponse,
    LGBBruteForceItem,
    LGBBruteForceResult,
    LGBBruteForceTaskStatus,
    LGBDiagnosticsResponse,
    LGBTrainingMetrics,
    LGBTreeDiagnostics,
    LGBPredictionStats,
    LGBCrossModelOverlapResponse,
    LGBCrossModelOverlapStock,
    LGBFinBERTResponse,
    FinBERTNewsItem,
    LGBFactorSubsetResult,
    LGBFactorSubsetTaskStatus,
)
from src.discovery.ml.lgb_trainer import LGBTrainer
from src.storage import DatabaseManager, FactorScoreSnapshot, StockAdjFactor, StockDaily

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


def _apply_factor_subset(trainer, progress=None, fallback_disabled=None):
    """从 factor_subset/ 加载匹配的因子子集；找不到则 fallback 到 LGB_DISABLE_FACTOR。"""
    import glob as _glob
    import json as _json

    subset_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))), "lgb_reports", "factor_subset")

    exec_mode = getattr(trainer, "exec_mode", "open")
    label_mode = getattr(trainer, "label_mode", "fixed")
    if label_mode == "peak_speed":
        period = f"peak{getattr(trainer, 'window_days', 20)}d"
    else:
        period = f"fixed_{getattr(trainer, 'forward_days', 5)}d"

    # 1) 优先从 factor_subset/ 白名单
    if os.path.isdir(subset_dir):
        pattern = os.path.join(subset_dir, f"subset_{exec_mode}_{period}_*.json")
        matches = sorted(_glob.glob(pattern), reverse=True)
        if matches:
            try:
                with open(matches[0], "r", encoding="utf-8") as f:
                    report = _json.load(f)
                final_subset = report.get("final_subset", [])
                if final_subset:
                    available = set(trainer.feature_names)
                    to_use = [f for f in final_subset if f in available]
                    skipped = [f for f in final_subset if f not in available]
                    if to_use:
                        trainer.feature_names = to_use
                        if progress:
                            msg = f"[因子子集] 使用 {matches[0].split('/')[-1]} ({len(to_use)} 因子)"
                            if skipped:
                                msg += f"，跳过: {skipped}"
                            progress(msg)
                        return
            except Exception:
                pass

    # 2) Fallback: 用 LGB_DISABLE_FACTOR 黑名单
    disabled = fallback_disabled or os.environ.get("LGB_DISABLE_FACTOR", "")
    if disabled:
        disabled_set = {f.strip() for f in disabled.split(",") if f.strip()}
        before = len(trainer.feature_names)
        trainer.feature_names = [f for f in trainer.feature_names if f not in disabled_set]
        removed = before - len(trainer.feature_names)
        if progress:
            progress(f"[因子子集] 未找到 {exec_mode}_{period} 报告，使用 LGB_DISABLE_FACTOR 过滤 ({removed} 因子禁用)" if removed else
                     f"[因子子集] 未找到 {exec_mode}_{period} 报告，使用全部 {len(trainer.feature_names)} 因子")
    elif progress:
        progress(f"[因子子集] 未找到 {exec_mode}_{period} 报告，使用全部 {len(trainer.feature_names)} 因子")


def _run_train_in_process(queue: multiprocessing.Queue, req_dict: dict):
    try:
        def _progress(msg: str):
            queue.put(("progress", msg))

        trainer = LGBTrainer(
            mode=req_dict["mode"],
            forward_days=req_dict["forward_days"],
            exec_mode=req_dict.get("exec_mode", "close"),
            label_mode=req_dict.get("label_mode", "fixed"),
            window_days=req_dict.get("window_days", 20),
            peak_min_return=req_dict.get("peak_min_return", 0.01),
            progress_callback=_progress,
        )
        # 先保存 LGB_DISABLE_FACTOR，再清除，避免 prepare_data 过滤因子
        disabled_factors = os.environ.pop("LGB_DISABLE_FACTOR", None)
        trainer.prepare_data(
            start_date=req_dict.get("start_date"),
            end_date=req_dict.get("end_date"),
        )
        _apply_factor_subset(trainer, _progress, fallback_disabled=disabled_factors)

        _progress(f"正在训练 (n_estimators={req_dict.get('n_estimators', 200)})...")
        trainer.train(
            n_estimators=req_dict.get("n_estimators", 200),
            num_leaves=req_dict.get("num_leaves", 31),
            learning_rate=req_dict.get("learning_rate", 0.05),
            cv_folds=req_dict.get("cv_folds", 5),
        )

        _progress("正在生成预测...")
        trainer.predict(target_date=req_dict.get("end_date"))

        _progress("正在保存模型...")
        model_path = trainer.save()

        _progress("正在生成报告...")
        report_path = trainer.save_report(top_n=5)

        importance = trainer.get_feature_importance()
        predictions = trainer.get_latest_predictions()
        metrics = trainer._training_metrics
        tree_diag = trainer.get_tree_diagnostics()
        pred_stats = trainer.get_prediction_stats()

        queue.put(("completed", {
            "model_path": model_path,
            "report_path": report_path,
            "model_date": trainer._latest_date,
            "feature_importance": importance,
            "predictions": predictions,
            "training_metrics": metrics,
            "tree_diagnostics": tree_diag,
            "prediction_stats": pred_stats,
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
        "exec_mode": req.exec_mode,
        "forward_days": req.forward_days,
        "label_mode": req.label_mode,
        "started_at": datetime.now().isoformat(),
    }

    req_dict = {
        "mode": req.mode,
        "forward_days": req.forward_days,
        "exec_mode": req.exec_mode,
        "label_mode": req.label_mode,
        "window_days": req.window_days,
        "peak_min_return": req.peak_min_return,
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


def _load_lgb_trainer(model_path: str | None) -> tuple[str, "LGBTrainer"]:
    """Load an LGBTrainer by path or from the latest completed task.

    Returns (resolved_model_path, trainer).
    Raises HTTPException 404 if model file or task not found.
    """
    if model_path:
        try:
            return model_path, LGBTrainer.load(model_path)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail=f"模型文件不存在: {model_path}",
            )
    completed = [
        (tid, t) for tid, t in _lgb_tasks.items()
        if t.get("status") == "completed"
        and t.get("result", {}).get("model_path")
    ]
    if not completed:
        raise HTTPException(
            status_code=404,
            detail="没有已完成的训练任务，请先训练模型或指定 model_path",
        )
    _, latest_task = max(
        completed,
        key=lambda x: x[1].get("finished_at", ""),
    )
    resolved = latest_task["result"]["model_path"]
    try:
        return resolved, LGBTrainer.load(resolved)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"模型文件不存在: {resolved}",
        )


@router.get(
    "/lgb/status",
    response_model=LGBTaskStatusResponse,
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
        result = task.get("result")
        if result and result.get("predictions"):
            fwd = task.get("forward_days", 3)
            em = task.get("exec_mode", "close")
            result["predictions"] = _enrich_predictions_with_stats(
                result["predictions"], fwd, em
            )
        resp["result"] = result
    return resp


@router.get(
    "/lgb/feature-importance",
    response_model=LGBFeatureImportanceResponse,
    summary="获取最新模型的特征重要性",
)
def lgb_feature_importance(
    model_path: str = Query(None, description="可选：指定模型路径"),
):
    _, trainer = _load_lgb_trainer(model_path)

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
    _, trainer = _load_lgb_trainer(model_path)

    if trainer._X_latest is None:
        trainer.predict()
    predictions = trainer.get_latest_predictions()
    predictions = _enrich_predictions_with_stats(
        predictions, trainer.forward_days, getattr(trainer, "exec_mode", "close")
    )
    return LGBPredictionsResponse(
        model_date=trainer._latest_date or "",
        forward_days=trainer.forward_days,
        mode=trainer.mode,
        predictions=[LGBPredictionItem(**p) for p in predictions],
    )


@router.get(
    "/lgb/finbert",
    response_model=LGBFinBERTResponse,
    summary="单只股票 FinBERT 新闻情感分析",
)
def lgb_finbert(
    stock_code: str = Query(..., description="股票代码"),
    stock_name: str = Query("", description="股票名称（可选，用于搜索回退）"),
):
    result = _fetch_finbert_for_stock(stock_code, stock_name)
    return LGBFinBERTResponse(stock_code=stock_code, **result)


def _fetch_finbert_for_stock(stock_code: str, stock_name: str = "") -> dict:
    """获取单只股票的 FinBERT 新闻情感分析结果。"""
    try:
        from src.services.finbert_sentiment_service import get_finbert_service
        service = get_finbert_service()
        if not service or not service.is_available:
            return {}
    except Exception:
        return {}

    db = DatabaseManager.get_instance()
    texts = []
    items = []

    try:
        news_items = db.get_recent_news(code=stock_code, days=3, limit=5)
        for item in news_items:
            text = (item.title or "")
            if item.snippet:
                text += " " + item.snippet
            if text.strip():
                texts.append(text.strip())
            items.append({
                "title": (item.title or "").strip(),
                "snippet": (item.snippet or "").strip(),
                "source": getattr(item, "source", "") or "",
                "url": getattr(item, "url", "") or "",
                "date": str(getattr(item, "published_date", "") or ""),
            })
    except Exception:
        pass

    if not texts:
        try:
            search_svc = _get_search_service_instance()
            texts, raw_results = _fetch_texts_from_search(stock_code, stock_name, search_svc)
            items = []
            for r in raw_results:
                items.append({
                    "title": (r.title or "").strip(),
                    "snippet": (r.snippet or "").strip(),
                    "source": r.source or "",
                    "url": r.url or "",
                    "date": r.published_date or "",
                })
        except Exception:
            pass

    if not texts:
        return {}

    try:
        res = service.analyze_news_sentiment(texts)
        if res:
            fb_details = res.get("details", []) or []
            for j, item in enumerate(items):
                if j < len(fb_details):
                    item["sentiment_label"] = fb_details[j].get("label", "")
                    item["sentiment_score"] = fb_details[j].get("score")
            return {
                "finbert_label": res.get("overall_label"),
                "finbert_score": res.get("overall_score"),
                "finbert_summary": res.get("summary"),
                "news_items": items if items else None,
            }
    except Exception:
        pass

    return {}


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
    exec_mode: str = Query("close", description="标签模式（仅无 model_path 时生效）"),
):
    if model_path:
        try:
            trainer = LGBTrainer.load(model_path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"模型文件不存在: {model_path}")
    else:
        trainer = LGBTrainer(mode=mode, forward_days=forward_days, exec_mode=exec_mode)
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
    from src.storage import StockDaily
    from sqlalchemy import distinct as _distinct
    db = DatabaseManager.get_instance()
    result = {}
    with db.get_session() as session:
        dates_raw = session.query(_distinct(StockDaily.date)).order_by(
            StockDaily.date
        ).all()
        if dates_raw:
            all_dates = [d[0] for d in dates_raw]
            min_d = min(all_dates)
            max_d = max(all_dates)
            if hasattr(min_d, 'strftime'):
                min_s, max_s = min_d.strftime("%Y%m%d"), max_d.strftime("%Y%m%d")
            else:
                min_s, max_s = str(min_d).replace('-', ''), str(max_d).replace('-', '')
            result["intraday"] = {"min": min_s, "max": max_s}
            result["postmarket"] = {"min": min_s, "max": max_s}
        else:
            result["intraday"] = None
            result["postmarket"] = None
    return LGBDateRangeResponse(**result)


@router.get(
    "/lgb/models",
    response_model=LGBModelListResponse,
    summary="列出所有已保存的模型",
)
def lgb_list_models(
    label_mode: Optional[str] = Query(None, description="按模式过滤: fixed | peak_speed"),
):
    models = LGBTrainer.list_models(label_mode=label_mode)
    return LGBModelListResponse(
        models=[LGBModelInfo(**m) for m in models],
    )


@router.get(
    "/lgb/diagnostics",
    response_model=LGBDiagnosticsResponse,
    summary="获取模型诊断数据",
)
def lgb_diagnostics(
    model_path: str = Query(None, description="模型路径，为空则使用最近训练的模型"),
):
    """返回训练指标、树结构诊断、预测分布统计。"""
    _, trainer = _load_lgb_trainer(model_path)

    tree_diag = trainer.get_tree_diagnostics()
    metrics_raw = trainer._training_metrics

    pred_stats = None
    try:
        trainer.predict()
        pred_stats = trainer.get_prediction_stats()
    except Exception:
        pass

    return LGBDiagnosticsResponse(
        training_metrics=LGBTrainingMetrics(**{
            "cv_rmse_mean": metrics_raw.get("cv_rmse_mean", 0.0),
            "cv_rmse_std": metrics_raw.get("cv_rmse_std", 0.0),
            "n_samples": metrics_raw.get("n_samples", 0),
            "n_features": metrics_raw.get("n_features", 0),
            "cv_scores": metrics_raw.get("cv_scores", []),
            "rank_ic_mean": metrics_raw.get("rank_ic_mean"),
            "rank_ic_std": metrics_raw.get("rank_ic_std"),
            "icir": metrics_raw.get("icir"),
            "oof_corr": metrics_raw.get("oof_corr"),
        }),
        tree_diagnostics=LGBTreeDiagnostics(**tree_diag),
        prediction_stats=LGBPredictionStats(**pred_stats) if pred_stats else None,
    )


@router.get(
    "/lgb/stock-lookup",
    response_model=LGBStockLookupResponse,
    summary="查询指定股票的 LGB 预测评分与排名",
)
def lgb_stock_lookup(
    stock_code: str = Query(..., description="股票代码（如 600519）或字母缩写（如 ZSYH、ZGPA）"),
    model_path: str = Query(None, description="可选：指定模型路径"),
):
    """在最新预测结果中查找指定个股的评分和全市场排名，支持拼音首字母缩写查询。"""
    import pandas as pd
    from pypinyin import lazy_pinyin

    if model_path:
        trainer = LGBTrainer.load(model_path)
    else:
        completed = [
            (tid, t) for tid, t in _lgb_tasks.items()
            if t.get("status") == "completed"
            and t.get("result", {}).get("model_path")
        ]
        if completed:
            _, latest_task = max(
                completed,
                key=lambda x: x[1].get("finished_at", ""),
            )
            model_path = latest_task["result"]["model_path"]
        else:
            # Fallback: load latest model from disk
            project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
            models_dir = _os.path.join(project_root, "src", "data", "lgb_models")
            if _os.path.isdir(models_dir):
                joblib_files = sorted(
                    [f for f in _os.listdir(models_dir) if f.endswith(".joblib")],
                    key=lambda f: _os.path.getmtime(_os.path.join(models_dir, f)),
                    reverse=True,
                )
                if joblib_files:
                    model_path = _os.path.join(models_dir, joblib_files[0])
                else:
                    raise HTTPException(status_code=404, detail="没有已完成的训练任务，请先训练模型")
            else:
                raise HTTPException(status_code=404, detail="没有已完成的训练任务，请先训练模型")
        try:
            trainer = LGBTrainer.load(model_path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"模型文件不存在: {model_path}")

    df = trainer.predict()
    code = str(stock_code).strip().zfill(6)

    if df is None or df.empty:
        return LGBStockLookupResponse(found=False, message="预测结果为空")

    # 按 stock_code 或 ts_code 匹配
    mask = (df["stock_code"] == code) | (df["ts_code"] == code)
    if not mask.any():
        for sfx in [".SH", ".SZ", ".BJ"]:
            mask = df["ts_code"] == f"{code}{sfx}"
            if mask.any():
                break

    # 如果代码匹配失败且输入为纯字母，尝试拼音首字母匹配
    if not mask.any() and stock_code.replace(" ", "").isalpha():
        q = stock_code.strip().upper()
        name_col = df["stock_name"] if "stock_name" in df.columns else None
        if name_col is not None:
            init_mask = pd.Series(False, index=df.index)
            for i, name in enumerate(name_col):
                if not isinstance(name, str):
                    continue
                initials = "".join([s[0].upper() for s in lazy_pinyin(name) if s])
                if q in initials or q in name.upper():
                    init_mask.iloc[i] = True
            if init_mask.any():
                mask = init_mask
            else:
                return LGBStockLookupResponse(
                    found=False,
                    message=f"未找到与「{stock_code}」匹配的股票（已尝试代码匹配和拼音首字母匹配）",
                )
        else:
            return LGBStockLookupResponse(
                found=False,
                message=f"未找到 {stock_code} 的预测数据，请确认代码正确且该股票在当次扫描范围内",
            )

    if not mask.any():
        return LGBStockLookupResponse(
            found=False,
            message=f"未找到 {stock_code} 的预测数据，请确认代码正确且该股票在当次扫描范围内",
        )

    row = df[mask].iloc[0]
    rank = int(df["lgb_score"].rank(ascending=False).loc[row.name])
    stock_name_val = str(row.get("stock_name", "")) if "stock_name" in df.columns else ""

    # FinBERT 新闻情感分析
    finbert_sentiment = None
    try:
        from src.services.finbert_sentiment_service import get_finbert_service
        finbert_svc = get_finbert_service()
        if finbert_svc.is_available:
            news_lines = []
            news_items = []  # 原始新闻条目，供前端展示
            # 优先从 DB 获取最近 7 天的新闻
            try:
                from src.storage import StockDatabase
                db = StockDatabase()
                recent = db.get_recent_news(str(row["stock_code"]), days=7, limit=20)
                for r in recent:
                    text = (r.title or "") + " " + (r.snippet or "")
                    text = text.strip()
                    if len(text) >= 10:
                        news_lines.append(text)
                    news_items.append({
                        "title": (r.title or "").strip(),
                        "snippet": (r.snippet or "").strip(),
                        "source": r.source or "",
                        "url": r.url or "",
                        "date": r.published_date.strftime("%Y-%m-%d") if r.published_date else "",
                    })
            except Exception:
                pass
            # DB 无新闻则回退到搜索引擎
            if not news_lines:
                try:
                    from src.search_service import SearchService
                    from src.config import Config
                    cfg = Config.get_instance()
                    search_svc = SearchService(
                        bocha_keys=cfg.bocha_api_keys,
                        tavily_keys=cfg.tavily_api_keys,
                        anspire_keys=cfg.anspire_api_keys,
                        brave_keys=cfg.brave_api_keys,
                        serpapi_keys=cfg.serpapi_keys,
                        minimax_keys=cfg.minimax_api_keys,
                        bailian_keys=getattr(cfg, 'bailian_api_keys', None),
                        kimi_keys=cfg.kimi_api_keys,
                        searxng_base_urls=cfg.searxng_base_urls,
                        searxng_public_instances_enabled=cfg.searxng_public_instances_enabled,
                        akshare_news_enabled=getattr(cfg, 'enable_akshare_news', True),
                    )
                    if search_svc.is_available:
                        news_resp = search_svc.search_stock_news(
                            stock_code=str(row["stock_code"]),
                            stock_name=stock_name_val,
                            max_results=10,
                        )
                        if news_resp.success and news_resp.results:
                            for r in news_resp.results:
                                title = (r.title or "").strip()
                                snippet = (r.snippet or "").strip()
                                combined = f"{title} {snippet}".strip()
                                if len(combined) >= 10:
                                    news_lines.append(combined)
                                news_items.append({
                                    "title": title,
                                    "snippet": snippet,
                                    "source": r.source or "",
                                    "url": r.url or "",
                                    "date": r.published_date or "",
                                })
                except Exception:
                    pass
            if news_lines:
                fb = finbert_svc.analyze_news_sentiment(news_lines)
                if fb:
                    # 将 FinBERT 情感标签关联回新闻条目
                    fb_details = fb.get("details", []) or []
                    for i, item in enumerate(news_items):
                        if i < len(fb_details):
                            item["sentiment_label"] = fb_details[i].get("label", "")
                            item["sentiment_score"] = fb_details[i].get("score")
                    finbert_sentiment = {
                                "overall_score": fb.get("overall_score"),
                                "overall_label": fb.get("overall_label"),
                                "positive_count": fb.get("positive_count"),
                                "negative_count": fb.get("negative_count"),
                                "neutral_count": fb.get("neutral_count"),
                                "summary": fb.get("summary"),
                                "news_items": news_items,
                            }
    except Exception:
        pass

    matched_df = df[mask]
    ranks = df["lgb_score"].rank(ascending=False)

    first_item = LGBStockLookupItem(
        stock_code=str(row["stock_code"]),
        ts_code=str(row["ts_code"]),
        stock_name=stock_name_val,
        rank=rank,
        lgb_score=round(float(row["lgb_score_norm"]), 4),
        raw_score=round(float(row["lgb_score"]), 4),
        total_stocks=len(df),
        finbert_sentiment=finbert_sentiment,
    )

    # 多结果：拼音匹配返回全部
    if len(matched_df) > 1:
        items = []
        for _, r in matched_df.iterrows():
            items.append(LGBStockLookupItem(
                stock_code=str(r["stock_code"]),
                ts_code=str(r["ts_code"]),
                stock_name=str(r.get("stock_name", "")) if "stock_name" in df.columns else "",
                rank=int(ranks.loc[r.name]),
                lgb_score=round(float(r["lgb_score_norm"]), 4),
                raw_score=round(float(r["lgb_score"]), 4),
                total_stocks=len(df),
            ))
        return LGBStockLookupResponse(found=True, item=first_item, items=items)

    return LGBStockLookupResponse(found=True, item=first_item)


# ── Backtest Simulation ──

import glob as _glob
import json as _json
import os as _os
from collections import defaultdict as _defaultdict
from typing import Optional as _Optional
from datetime import date as _date, datetime as _datetime, timedelta as _timedelta

# Cache for backtest results (in-memory, cleared on restart)
_backtest_cache: Dict[str, dict] = {}


def _get_limit_pct(stock_code: str) -> float:
    """Get the daily limit-up/down percentage for a stock."""
    code = str(stock_code).strip().zfill(6)
    if code.startswith(("688",)):
        return 0.20
    if code.startswith(("300", "301")):
        return 0.20
    if code.startswith(("83", "87", "43")):
        return 0.30
    return 0.10


def _get_lot_info(stock_code: str) -> tuple:
    """获取最小交易股数和递增单位。
    科创板/北证：最低200股，超过200后100股递增。
    其他板块：最低100股，100股递增。
    """
    code = str(stock_code).strip().zfill(6)
    if code.startswith(("688",)):
        return 200, 100
    if code.startswith(("83", "87", "43")):
        return 200, 100
    return 100, 100


def _find_next_trading_day(d: str, trading_days_set: set, trading_days_sorted: list) -> _Optional[str]:
    """Find the next trading day strictly after date d (YYYYMMDD)."""
    d_str = d.replace("-", "")[:8]
    for td in trading_days_sorted:
        if td > d_str:
            return td
    return None


def _find_nth_trading_day(start: str, n: int, trading_days_sorted: list) -> _Optional[str]:
    """Return the nth trading day on or after start (0-indexed).
    Extrapolates if beyond the available data using average trading-day interval."""
    try:
        idx = trading_days_sorted.index(start)
    except ValueError:
        return None
    target_idx = idx + n
    if 0 <= target_idx < len(trading_days_sorted):
        return trading_days_sorted[target_idx]
    if target_idx >= len(trading_days_sorted) and trading_days_sorted:
        # Extrapolate: estimate based on average interval between recent trading days
        from datetime import datetime as _dt, timedelta as _td
        extra = target_idx - len(trading_days_sorted) + 1
        # Average gap between last 20 trading days
        recent = trading_days_sorted[-20:]
        gaps = []
        for i in range(1, len(recent)):
            d1 = _dt.strptime(recent[i-1], "%Y%m%d")
            d2 = _dt.strptime(recent[i], "%Y%m%d")
            gaps.append((d2 - d1).days)
        avg_gap = sum(gaps) / len(gaps) if gaps else 1.4
        last_date = _dt.strptime(trading_days_sorted[-1], "%Y%m%d")
        est_date = last_date + _td(days=round(extra * avg_gap))
        return est_date.strftime("%Y%m%d")
    return None


def _adj_lookup(code: str, date_str: str, adj_by_code: dict) -> float:
    """Get adj_factor for a date, forward-filling from nearest prior date if missing."""
    entries = adj_by_code.get(code, [])
    if not entries:
        return 1.0
    lo, hi = 0, len(entries) - 1
    best = 1.0
    while lo <= hi:
        mid = (lo + hi) // 2
        if entries[mid][0] <= date_str:
            best = entries[mid][1]
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _check_loss_stop(
    code: str, buy_date: str, buy_price: float, sell_date: str,
    price_map: dict, adj_by_code: dict, trading_days_sorted: list,
) -> tuple:
    """Check trading days (including sell_date) for loss stop (亏损厌恶).
    Returns (effective_sell_date, effective_sell_price) — same as input if not stopped.
    """
    for td in trading_days_sorted:
        if td <= buy_date or td > sell_date:
            continue
        entry = price_map.get((code, td))
        if not entry:
            continue
        px = entry["close"] if entry["close"] > 0 else entry["open"]
        if px <= 0:
            continue
        adj_b = _adj_lookup(code, buy_date, adj_by_code)
        adj_t = _adj_lookup(code, td, adj_by_code)
        raw_ret = (px - buy_price) / buy_price if buy_price > 0 else 0.0
        ret = (1.0 + raw_ret) * (adj_t / adj_b) - 1.0 if adj_b > 0 and adj_t > 0 else 0.0
        if ret < -0.10:
            return (td, px)
    return (sell_date, 0.0)  # 0.0 sentinel: caller uses original sell_price


def _check_dead_hold(
    code: str, buy_date: str, buy_price: float, sell_date: str,
    price_map: dict, adj_by_code: dict, trading_days_sorted: list,
    max_hold_days: int = 20,
) -> tuple:
    """Dead-hold (跌了死扛): if losing at sell_date, extend up to max_hold_days trading days.
    Close as soon as breakeven (return >= 0), or force-close after max_hold_days.
    Returns (effective_sell_date, effective_sell_price, was_extended).
    """
    # Find index range: sell_date_idx + 1 to sell_date_idx + max_hold_days
    try:
        sell_idx = trading_days_sorted.index(sell_date)
    except ValueError:
        return (sell_date, 0.0, False)
    adj_b = _adj_lookup(code, buy_date, adj_by_code)

    for offset in range(1, max_hold_days + 1):
        ext_idx = sell_idx + offset
        if ext_idx >= len(trading_days_sorted):
            break
        td = trading_days_sorted[ext_idx]
        entry = price_map.get((code, td))
        if not entry:
            continue
        px = entry["close"] if entry["close"] > 0 else entry["open"]
        if px <= 0:
            continue
        adj_t = _adj_lookup(code, td, adj_by_code)
        raw_ret = (px - buy_price) / buy_price if buy_price > 0 else 0.0
        ret = (1.0 + raw_ret) * (adj_t / adj_b) - 1.0 if adj_b > 0 and adj_t > 0 else 0.0
        if ret >= 0:
            return (td, px, True)
    # Force-close: use last available trading day within extended range
    last_idx = min(sell_idx + max_hold_days, len(trading_days_sorted) - 1)
    if last_idx > sell_idx:
        last_td = trading_days_sorted[last_idx]
        last_entry = price_map.get((code, last_td))
        if last_entry:
            last_px = last_entry["close"] if last_entry["close"] > 0 else last_entry["open"]
            if last_px > 0:
                return (last_td, last_px, True)
    return (sell_date, 0.0, False)


def _compute_sharpe(capital_curve: list) -> float:
    """Compute annualized Sharpe ratio from capital curve daily returns."""
    if not capital_curve or len(capital_curve) < 2:
        return 0.0
    daily_returns = [pt.get("daily_return", 0.0) for pt in capital_curve][1:]
    if not daily_returns:
        return 0.0
    mean_ret = sum(daily_returns) / len(daily_returns)
    variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
    if variance <= 0:
        return 0.0
    return (mean_ret / (variance ** 0.5)) * (252 ** 0.5)


# Cache for prediction historical stats
_pred_stats_cache: Dict[str, dict] = {}


def _get_search_service_instance():
    """Lazy-init a SearchService for fallback news fetching."""
    try:
        from src.search_service import SearchService
        from src.config import Config
        cfg = Config.get_instance()
        return SearchService(
            bocha_keys=cfg.bocha_api_keys,
            tavily_keys=cfg.tavily_api_keys,
            anspire_keys=cfg.anspire_api_keys,
            brave_keys=cfg.brave_api_keys,
            serpapi_keys=cfg.serpapi_keys,
            minimax_keys=cfg.minimax_api_keys,
            bailian_keys=getattr(cfg, 'bailian_api_keys', None),
            kimi_keys=cfg.kimi_api_keys,
            searxng_base_urls=cfg.searxng_base_urls,
            searxng_public_instances_enabled=cfg.searxng_public_instances_enabled,
            akshare_news_enabled=getattr(cfg, 'enable_akshare_news', True),
        )
    except Exception:
        return None


def _fetch_texts_from_search(stock_code: str, stock_name: str, search_svc=None):
    """Fetch news texts via search engine fallback.

    Returns (texts, raw_results) where raw_results are the SearchResult objects
    for building news_items downstream.
    """
    if search_svc is None:
        search_svc = _get_search_service_instance()
    if not search_svc or not search_svc.is_available:
        return [], []
    try:
        resp = search_svc.search_stock_news(
            stock_code=str(stock_code),
            stock_name=stock_name or str(stock_code),
            max_results=5,
        )
        if not resp.success or not resp.results:
            return [], []
        texts = []
        for r in resp.results:
            title = (r.title or "").strip()
            snippet = (r.snippet or "").strip()
            combined = f"{title} {snippet}".strip()
            if combined:
                texts.append(combined)
        return texts, resp.results
    except Exception:
        return [], []


def _enrich_predictions_with_finbert(predictions: list) -> list:
    """Enrich prediction items with FinBERT news sentiment analysis."""
    try:
        from src.services.finbert_sentiment_service import get_finbert_service
        service = get_finbert_service()
        if not service or not service.is_available:
            return predictions
    except Exception:
        return predictions

    db = DatabaseManager.get_instance()
    groups: list = []
    valid_indices: list = []
    # 每只股票对应的原始新闻条目（用于回传前端展示）
    per_stock_items: list = []

    # 预创建搜索引擎实例（DB 无新闻时回退用）
    search_svc = _get_search_service_instance()

    for idx, p in enumerate(predictions):
        code = p.get("stock_code", "")
        if not code:
            continue
        try:
            news_items = db.get_recent_news(code=code, days=3, limit=5)
            texts = []
            items_for_stock = []
            for item in news_items:
                text = (item.title or "")
                if item.snippet:
                    text += " " + item.snippet
                if text.strip():
                    texts.append(text.strip())
                items_for_stock.append({
                    "title": (item.title or "").strip(),
                    "snippet": (item.snippet or "").strip(),
                    "source": getattr(item, "source", "") or "",
                    "url": getattr(item, "url", "") or "",
                    "date": str(getattr(item, "published_date", "") or ""),
                })

            # DB 无新闻时回退到搜索引擎
            if not texts:
                texts, raw_results = _fetch_texts_from_search(
                    code, p.get("stock_name", ""), search_svc
                )
                items_for_stock = []
                for r in raw_results:
                    items_for_stock.append({
                        "title": (r.title or "").strip(),
                        "snippet": (r.snippet or "").strip(),
                        "source": r.source or "",
                        "url": r.url or "",
                        "date": r.published_date or "",
                    })

            groups.append(texts)
            per_stock_items.append(items_for_stock)
            valid_indices.append(idx)
        except Exception:
            continue

    if not groups:
        return predictions

    try:
        results = service.analyze_news_sentiment_batch(groups)
        for i, idx in enumerate(valid_indices):
            res = results[i] if i < len(results) else None
            if res:
                predictions[idx]["finbert_label"] = res.get("overall_label")
                predictions[idx]["finbert_score"] = res.get("overall_score")
                predictions[idx]["finbert_summary"] = res.get("summary")
                # 将情感标签关联回新闻条目
                fb_details = res.get("details", []) or []
                items = per_stock_items[i]
                for j, item in enumerate(items):
                    if j < len(fb_details):
                        item["sentiment_label"] = fb_details[j].get("label", "")
                        item["sentiment_score"] = fb_details[j].get("score")
                if items:
                    predictions[idx]["news_items"] = items
    except Exception:
        # Fail-open: leave predictions unchanged
        pass

    return predictions


def _enrich_predictions_with_stats(
    predictions: list,
    forward_days: int,
    exec_mode: str,
) -> list:
    """Enrich prediction items with historical stats (win_rate, avg_return, etc.).

    predictions: list of dicts with rank, stock_code, ts_code, raw_score, etc.
    Returns the same list with added fields.
    """
    cache_key = f"{exec_mode}_{forward_days}"
    stats = _pred_stats_cache.get(cache_key)

    if stats is None:
        stats = _build_historical_stats(forward_days, exec_mode)
        _pred_stats_cache[cache_key] = stats

    if not stats or not stats.get("rank_stats"):
        return predictions

    rank_stats = stats["rank_stats"]
    stock_hits = stats["stock_hits"]
    all_raw_scores_sorted = stats["all_raw_scores_sorted"]

    for p in predictions:
        rank = p.get("rank", 0)
        code = p.get("stock_code", "")
        raw_score = p.get("raw_score", 0.0)

        rs = rank_stats.get(rank)
        if rs:
            p["win_rate"] = rs["win_rate"]
            p["avg_return"] = rs["avg_return"]
            p["max_return"] = rs["max_return"]
            p["max_loss"] = rs["max_loss"]
            p["profit_loss_ratio"] = rs["profit_loss_ratio"]
        else:
            p["win_rate"] = None
            p["avg_return"] = None
            p["max_return"] = None
            p["max_loss"] = None
            p["profit_loss_ratio"] = None

        p["hit_count"] = stock_hits.get(code, 0) or None

        if all_raw_scores_sorted:
            import bisect
            idx = bisect.bisect_left(all_raw_scores_sorted, raw_score)
            p["score_percentile"] = round(idx / len(all_raw_scores_sorted) * 100, 1)
        else:
            p["score_percentile"] = None

    return predictions


def _build_historical_stats(forward_days: int, exec_mode: str) -> dict:
    """Build historical prediction stats from lgb_reports/ + DB prices."""
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")
    exec_suffix = "open2open" if exec_mode == "open" else "close2close"
    fwd_dir = f"fwd{forward_days}d"
    search_dir = _os.path.join(reports_dir, exec_suffix, fwd_dir)

    if not _os.path.isdir(search_dir):
        return {}

    pattern = _os.path.join(search_dir, "*_pred_*.json")
    json_files = sorted(_glob.glob(pattern))
    if not json_files:
        return {}

    # 1) Parse all historical predictions
    hist_preds: list = []  # [(pred_date, rank, stock_code, raw_score)]
    all_raw_scores: list = []
    stock_hits: Dict[str, int] = _defaultdict(int)

    for fp in json_files:
        try:
            with open(fp, "r", encoding="utf-8") as fh:
                data = _json.load(fh)
        except Exception:
            continue
        pred_date = data.get("pred_date", "")
        if not pred_date:
            continue
        for p in data.get("predictions", []):
            rank = int(p.get("rank", 0))
            code = str(p.get("stock_code", "")).strip().zfill(6)
            raw = float(p.get("raw_score", 0.0))
            hist_preds.append((pred_date, rank, code, raw))
            all_raw_scores.append(raw)
            stock_hits[code] += 1

    if not hist_preds:
        return {}

    # 2) Get trading days
    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        from sqlalchemy import func
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(func.count(StockDaily.code) >= 3000)
            .order_by(StockDaily.date)
            .all()
        )
    trading_days_sorted = [
        d[0].strftime("%Y%m%d") if hasattr(d[0], "strftime") else str(d[0]).replace("-", "")[:8]
        for d in dates_raw
    ]
    trading_days_set = set(trading_days_sorted)

    # 3) Determine buy/sell dates for each prediction
    all_codes: set = set()
    pred_with_dates: list = []  # [(pred_date, rank, code, raw, buy_date, sell_date)]

    for pred_date, rank, code, raw in hist_preds:
        if exec_mode == "close":
            if pred_date not in trading_days_set:
                continue
            buy_date = pred_date
            sell_date = _find_nth_trading_day(pred_date, forward_days, trading_days_sorted)
        else:
            buy_date = _find_next_trading_day(pred_date, trading_days_set, trading_days_sorted)
            if not buy_date:
                continue
            sell_date = _find_nth_trading_day(buy_date, forward_days, trading_days_sorted)

        if not sell_date:
            continue
        pred_with_dates.append((pred_date, rank, code, raw, buy_date, sell_date))
        all_codes.add(code)

    if not pred_with_dates:
        return {"rank_stats": {}, "stock_hits": dict(stock_hits), "all_raw_scores_sorted": sorted(all_raw_scores)}

    # 4) Load prices + adj_factors
    all_date_strs: set = set()
    for _, _, _, _, bd, sd in pred_with_dates:
        all_date_strs.add(bd)
        all_date_strs.add(sd)

    price_map: Dict[tuple, dict] = {}
    with db.get_session() as session:
        rows = session.query(StockDaily).filter(
            StockDaily.code.in_(list(all_codes)),
            StockDaily.date.in_([f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in all_date_strs]),
        ).all()
    for r in rows:
        c = str(r.code).split(".")[0].zfill(6)
        d = r.date.strftime("%Y%m%d") if hasattr(r.date, "strftime") else str(r.date).replace("-", "")[:8]
        price_map[(c, d)] = {
            "open": float(r.open) if r.open else 0.0,
            "close": float(r.close) if r.close else 0.0,
        }

    adj_map: Dict[tuple, float] = {}
    with db.get_session() as session:
        adj_rows = session.query(StockAdjFactor).filter(
            StockAdjFactor.code.in_(list(all_codes)),
            StockAdjFactor.trade_date.in_([f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in all_date_strs]),
        ).all()
    for r in adj_rows:
        d = r.trade_date.strftime("%Y%m%d") if hasattr(r.trade_date, "strftime") else str(r.trade_date).replace("-", "")[:8]
        adj_map[(str(r.code).split(".")[0].zfill(6), d)] = float(r.adj_factor)

    # Forward-fill adj_factor
    _adj_by_code: Dict[str, list] = _defaultdict(list)
    for (code, d), v in adj_map.items():
        _adj_by_code[code].append((d, v))
    for code in _adj_by_code:
        _adj_by_code[code].sort(key=lambda x: x[0])

    def _get_adj_local(code: str, date_str: str) -> float:
        entries = _adj_by_code.get(code, [])
        if not entries:
            return 1.0
        lo, hi = 0, len(entries) - 1
        best = 1.0
        while lo <= hi:
            mid = (lo + hi) // 2
            if entries[mid][0] <= date_str:
                best = entries[mid][1]
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    # 5) Compute actual returns per prediction
    rank_returns: Dict[int, list] = _defaultdict(list)

    for _, rank, code, _, buy_date, sell_date in pred_with_dates:
        buy_entry = price_map.get((code, buy_date))
        sell_entry = price_map.get((code, sell_date))
        if not buy_entry or not sell_entry:
            continue

        if exec_mode == "close":
            buy_px = buy_entry["close"]
            sell_px = sell_entry["close"]
        else:
            buy_px = buy_entry["open"]
            sell_px = sell_entry["open"]

        if buy_px <= 0 or sell_px <= 0:
            continue

        adj_buy = _get_adj_local(code, buy_date)
        adj_sell = _get_adj_local(code, sell_date)
        raw_ret = (sell_px - buy_px) / buy_px
        ret = (1.0 + raw_ret) * (adj_sell / adj_buy) - 1.0 if adj_buy > 0 else raw_ret

        rank_returns[rank].append(ret)

    # 6) Compute per-rank stats
    rank_stats: Dict[int, dict] = {}
    for rank, returns in rank_returns.items():
        if not returns:
            continue
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0

        rank_stats[rank] = {
            "win_rate": round(len(wins) / len(returns), 4),
            "avg_return": round(sum(returns) / len(returns), 4),
            "max_return": round(max(returns), 4),
            "max_loss": round(min(returns), 4),
            "profit_loss_ratio": round(avg_win / avg_loss, 2) if avg_loss > 0 else None,
        }

    return {
        "rank_stats": rank_stats,
        "stock_hits": dict(stock_hits),
        "all_raw_scores_sorted": sorted(all_raw_scores),
    }


def _simulate_backtest(exec_mode: str, forward_days: int, top_n: int, stop_strategy: str = "none") -> dict:
    """Run backtest simulation from cached prediction files.

    stop_strategy: "none" | "loss_aversion" | "dead_hold"
    Returns dict with keys: error, metrics, capital_curve, trades.
    If error is not None, the simulation failed and metrics is None.
    """
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")

    exec_suffix = "open2open" if exec_mode == "open" else "close2close"
    fwd_dir = f"fwd{forward_days}d"
    pattern = f"*_fwd{forward_days}d_*_pred_*.json"
    search_dir = _os.path.join(reports_dir, exec_suffix, fwd_dir)
    json_files = sorted(_glob.glob(_os.path.join(search_dir, pattern)))
    if not json_files:
        json_files = sorted(_glob.glob(_os.path.join(reports_dir, exec_suffix, pattern)))
    if not json_files:
        return {"error": f"No {exec_suffix} prediction files for forward_days={forward_days}", "metrics": None, "capital_curve": [], "trades": []}

    preds_by_date: Dict[str, list] = _defaultdict(list)
    for fp in json_files:
        with open(fp, "r", encoding="utf-8") as fh:
            data = _json.load(fh)
        pd_date = data.get("pred_date", "")
        if not pd_date:
            continue
        for p in data.get("predictions", [])[:5]:  # read all 5 for fallback, limit to top_n in trade loop
            preds_by_date[pd_date].append({
                "stock_code": str(p.get("stock_code", "")).strip().zfill(6),
                "ts_code": str(p.get("ts_code", "")),
                "stock_name": str(p.get("stock_name", "")),
                "rank": int(p.get("rank", 0)),
                "raw_score": float(p.get("raw_score", 0.0)),
            })

    if not preds_by_date:
        return {"error": "No prediction data found", "metrics": None, "capital_curve": [], "trades": []}

    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        from sqlalchemy import func
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(func.count(StockDaily.code) >= 3000)
            .order_by(StockDaily.date)
            .all()
        )
    trading_days_sorted = [
        d[0].strftime("%Y%m%d") if hasattr(d[0], "strftime") else str(d[0]).replace("-", "")[:8]
        for d in dates_raw
    ]
    trading_days_set = set(trading_days_sorted)

    latest_td = trading_days_sorted[-1] if trading_days_sorted else None
    all_codes: set = set()
    date_pairs: list = []
    for pred_date, preds in sorted(preds_by_date.items()):
        if exec_mode == "close":
            if pred_date not in trading_days_set:
                continue
            sell_date = _find_nth_trading_day(pred_date, forward_days, trading_days_sorted)
            # If sell_date is beyond available data, treat as holding
            if sell_date and latest_td and sell_date > latest_td:
                sell_date = None
            date_pairs.append((pred_date, pred_date, sell_date))
        else:
            buy_date = _find_next_trading_day(pred_date, trading_days_set, trading_days_sorted)
            if not buy_date:
                continue
            sell_date = _find_nth_trading_day(buy_date, forward_days, trading_days_sorted)
            if sell_date and latest_td and sell_date > latest_td:
                sell_date = None
            date_pairs.append((pred_date, buy_date, sell_date))
        for p in preds:
            all_codes.add(p["stock_code"])

    if not date_pairs:
        return {"error": "No valid trading dates found", "metrics": None, "capital_curve": [], "trades": []}

    all_date_strs: set = set()
    has_holding = any(sd is None for _, _, sd in date_pairs)
    for _, bd, sd in date_pairs:
        all_date_strs.add(bd)
        if sd is not None:
            all_date_strs.add(sd)
    if has_holding and latest_td:
        all_date_strs.add(latest_td)

    if exec_mode == "open":
        for d in list(all_date_strs):
            for i, td in enumerate(trading_days_sorted):
                if td == d and i > 0:
                    all_date_strs.add(trading_days_sorted[i - 1])
                    break
    else:
        for _, bd, _ in date_pairs:
            for i, td in enumerate(trading_days_sorted):
                if td == bd and i > 0:
                    all_date_strs.add(trading_days_sorted[i - 1])
                    break

    if stop_strategy == "loss_aversion":
        for _, bd, sd in date_pairs:
            if sd is None:
                continue
            for td in trading_days_sorted:
                if bd < td < sd:
                    all_date_strs.add(td)

    if stop_strategy == "dead_hold":
        for _, _bd, sd in date_pairs:
            if sd is None:
                continue
            try:
                sd_idx = trading_days_sorted.index(sd)
            except ValueError:
                continue
            for offset in range(1, 21):
                ext_idx = sd_idx + offset
                if ext_idx < len(trading_days_sorted):
                    all_date_strs.add(trading_days_sorted[ext_idx])

    all_dates_iso = {ds: f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}" for ds in all_date_strs}

    with db.get_session() as session:
        rows = session.query(StockDaily).filter(
            StockDaily.date.in_([v for v in all_dates_iso.values()]),
            StockDaily.code.in_(list(all_codes)),
        ).all()

    price_map: Dict[tuple, dict] = {}
    for r in rows:
        d = r.date.strftime("%Y%m%d") if hasattr(r.date, "strftime") else str(r.date).replace("-", "")[:8]
        price_map[(str(r.code).strip().zfill(6), d)] = {
            "open": float(r.open) if r.open else 0.0,
            "close": float(r.close) if r.close else 0.0,
            "pct_chg": float(r.pct_chg) if r.pct_chg else 0.0,
        }

    adj_map: Dict[tuple, float] = {}
    with db.get_session() as session:
        adj_rows = session.query(StockAdjFactor).filter(
            StockAdjFactor.code.in_(list(all_codes)),
            StockAdjFactor.trade_date.in_(
                [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in all_date_strs]
            ),
        ).all()
    for r in adj_rows:
        d = r.trade_date.strftime("%Y%m%d") if hasattr(r.trade_date, "strftime") else str(r.trade_date).replace("-", "")[:8]
        adj_map[(str(r.code).split(".")[0].zfill(6), d)] = float(r.adj_factor)

    # Build per-code sorted date list for forward-fill lookup
    _adj_by_code: Dict[str, list] = _defaultdict(list)
    for (code, d), v in adj_map.items():
        _adj_by_code[code].append((d, v))
    for code in _adj_by_code:
        _adj_by_code[code].sort(key=lambda x: x[0])

    def _get_adj(code: str, date_str: str) -> float:
        """Get adj_factor for a date, forward-filling from nearest prior date if missing."""
        entries = _adj_by_code.get(code, [])
        if not entries:
            return 1.0
        # Binary search for nearest date <= date_str
        lo, hi = 0, len(entries) - 1
        best = 1.0
        while lo <= hi:
            mid = (lo + hi) // 2
            if entries[mid][0] <= date_str:
                best = entries[mid][1]
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    INITIAL_CAPITAL = 10_000_000.0
    trades: list = []
    capital_curve: list = []

    pairs_by_buy: Dict[str, list] = _defaultdict(list)
    for pred_date, buy_date, sell_date in date_pairs:
        pairs_by_buy[buy_date].append((pred_date, sell_date))

    buy_dates_sorted = sorted(pairs_by_buy.keys())
    cash = INITIAL_CAPITAL
    active_positions: list = []

    for buy_date in buy_dates_sorted:
        still_active: list = []
        for pos in active_positions:
            if pos["sell_date"] <= buy_date:
                proceeds = pos["entry_value"] * (1.0 + pos["ret_pct"])
                cash += proceeds
            else:
                still_active.append(pos)
        active_positions = still_active

        locked_value = sum(p["entry_value"] for p in active_positions)
        portfolio_value = cash + locked_value
        slot_size = portfolio_value / forward_days if forward_days > 0 else portfolio_value

        for pred_date, sell_date in pairs_by_buy[buy_date]:
            preds = preds_by_date.get(pred_date, [])
            slot_trades: list = []
            is_holding = sell_date is None
            eff_sell_date = sell_date if sell_date is not None else latest_td

            held = 0  # count of valid trades for this slot
            for p in preds:
                if held >= top_n:
                    break
                code = p["stock_code"]
                ts_code = p["ts_code"]
                stock_name = p["stock_name"]
                rank = p["rank"]
                raw_score = p.get("raw_score", 0.0)

                if is_holding:
                    buy_entry = price_map.get((code, buy_date))
                    if not buy_entry:
                        continue
                    if exec_mode == "close":
                        if buy_entry["close"] <= 0:
                            continue
                        buy_price = buy_entry["close"]
                    else:
                        if buy_entry["open"] <= 0:
                            continue
                        buy_price = buy_entry["open"]
                    # Holding positions: always use latest close for current valuation
                    latest_entry = price_map.get((code, latest_td))
                    actual_sell_date = latest_td
                    if latest_entry and latest_entry["close"] > 0:
                        sell_price_raw = latest_entry["close"]
                    else:
                        # Walk backwards to find the most recent close for this stock
                        sell_price_raw = 0.0
                        for td in reversed(trading_days_sorted):
                            if td > latest_td:
                                continue
                            entry = price_map.get((code, td))
                            if entry and entry["close"] > 0:
                                sell_price_raw = entry["close"]
                                actual_sell_date = td
                                break
                        if sell_price_raw <= 0:
                            sell_price_raw = buy_price

                    eff_sell_date = actual_sell_date
                    eff_sell_price = sell_price_raw
                    if stop_strategy == "loss_aversion" and latest_td:
                        eff_sd, eff_sp = _check_loss_stop(
                            code, buy_date, buy_price, latest_td,
                            price_map, _adj_by_code, trading_days_sorted,
                        )
                        if eff_sp > 0:
                            eff_sell_date = eff_sd
                            eff_sell_price = eff_sp

                    adj_b = _get_adj(code, buy_date)
                    adj_s = _get_adj(code, eff_sell_date)
                    raw_ret = (eff_sell_price - buy_price) / buy_price if buy_price > 0 else 0.0
                    ret = round((1.0 + raw_ret) * (adj_s / adj_b) - 1.0, 6) if adj_s > 0 else 0.0

                    # 按手数计算持有仓位的股数
                    per_stock_h = slot_size / top_n if top_n > 0 else slot_size
                    min_lot, step = _get_lot_info(code)
                    raw_shares = per_stock_h / buy_price if buy_price > 0 else 0
                    h_shares = 0
                    h_cost = 0.0
                    if raw_shares >= min_lot:
                        h_shares = min_lot + int((raw_shares - min_lot) / step) * step
                        h_cost = h_shares * buy_price

                    if h_shares <= 0:
                        held += 1
                        continue
                    trades.append({
                        "pred_date": pred_date, "stock_code": code, "ts_code": ts_code,
                        "stock_name": stock_name, "rank": rank,
                        "buy_date": buy_date, "buy_price": buy_price,
                        "sell_date": eff_sell_date if eff_sell_date != latest_td else "",
                        "sell_price": eff_sell_price,
                        "return_pct": ret, "skipped": False,
                        "target_return": raw_score,
                        "shares": h_shares, "actual_cost": round(h_cost, 2),
                    })
                    held += 1
                    continue

                if exec_mode == "close":
                    buy_entry = price_map.get((code, buy_date))
                    if not buy_entry or buy_entry["close"] <= 0:
                        continue

                    limit_pct_c = _get_limit_pct(code)
                    at_limit_up = False
                    prev_bd = _find_nth_trading_day(buy_date, -1, trading_days_sorted)
                    if prev_bd:
                        prev_entry = price_map.get((code, prev_bd))
                        if prev_entry and prev_entry["close"] > 0:
                            limit_up_c = prev_entry["close"] * (1.0 + limit_pct_c)
                            if buy_entry["close"] >= limit_up_c * 0.999:
                                at_limit_up = True
                    if not at_limit_up and buy_entry.get("pct_chg", 0) >= limit_pct_c * 99.0:
                        at_limit_up = True
                    if at_limit_up:
                        trades.append({
                            "pred_date": pred_date, "stock_code": code, "ts_code": ts_code,
                            "stock_name": stock_name, "rank": rank,
                            "buy_date": buy_date, "buy_price": buy_entry["close"],
                            "sell_date": "", "sell_price": 0.0, "return_pct": 0.0, "skipped": True,
                        })
                        continue

                    buy_price = buy_entry["close"]
                    actual_sell_date = sell_date
                    sell_price = None
                    postpone_count = 0
                    while postpone_count < 10:
                        sell_entry = price_map.get((code, actual_sell_date))
                        if not sell_entry or sell_entry["close"] <= 0:
                            actual_sell_date = _find_next_trading_day(
                                actual_sell_date, trading_days_set, trading_days_sorted,
                            )
                            if not actual_sell_date:
                                break
                            postpone_count += 1
                            continue

                        limit_pct_c2 = _get_limit_pct(code)
                        at_limit_down = False
                        prev_sd = _find_nth_trading_day(actual_sell_date, -1, trading_days_sorted)
                        if prev_sd:
                            prev_sell_entry = price_map.get((code, prev_sd))
                            if prev_sell_entry and prev_sell_entry["close"] > 0:
                                limit_down_c = prev_sell_entry["close"] * (1.0 - limit_pct_c2)
                                if sell_entry["close"] <= limit_down_c * 1.001:
                                    at_limit_down = True
                        if not at_limit_down and sell_entry.get("pct_chg", 0) <= -limit_pct_c2 * 99.0:
                            at_limit_down = True
                        if at_limit_down:
                            next_sd = _find_next_trading_day(
                                actual_sell_date, trading_days_set, trading_days_sorted,
                            )
                            if not next_sd:
                                sell_price = sell_entry["close"]
                                break
                            actual_sell_date = next_sd
                            postpone_count += 1
                            continue

                        sell_price = sell_entry["close"]
                        break

                    if sell_price is None:
                        continue
                    skipped = False

                    if stop_strategy == "loss_aversion":
                        eff_sd, eff_sp = _check_loss_stop(
                            code, buy_date, buy_price, actual_sell_date,
                            price_map, _adj_by_code, trading_days_sorted,
                        )
                        if eff_sp > 0:
                            actual_sell_date = eff_sd
                            sell_price = eff_sp
                else:
                    buy_entry = price_map.get((code, buy_date))
                    if not buy_entry or buy_entry["open"] <= 0:
                        continue

                    limit_pct = _get_limit_pct(code)
                    at_limit_up = False
                    prev_buy = _find_nth_trading_day(buy_date, -1, trading_days_sorted)
                    if prev_buy:
                        prev_entry = price_map.get((code, prev_buy))
                        if prev_entry and prev_entry["close"] > 0:
                            limit_up = prev_entry["close"] * (1.0 + limit_pct)
                            if buy_entry["open"] >= limit_up * 0.999:
                                at_limit_up = True
                    if not at_limit_up and buy_entry.get("pct_chg", 0) >= limit_pct * 99.0:
                        at_limit_up = True
                    if at_limit_up:
                        trades.append({
                            "pred_date": pred_date, "stock_code": code, "ts_code": ts_code,
                            "stock_name": stock_name, "rank": rank,
                            "buy_date": buy_date, "buy_price": buy_entry["open"],
                            "sell_date": "", "sell_price": 0.0, "return_pct": 0.0, "skipped": True,
                        })
                        continue

                    buy_price = buy_entry["open"]
                    actual_sell_date = sell_date
                    actual_sell_price = None
                    postpone_count = 0
                    while postpone_count < 10:
                        sell_entry = price_map.get((code, actual_sell_date))
                        if not sell_entry or sell_entry["open"] <= 0:
                            actual_sell_date = _find_next_trading_day(
                                actual_sell_date, trading_days_set, trading_days_sorted,
                            )
                            if not actual_sell_date:
                                break
                            postpone_count += 1
                            continue

                        at_limit_down = False
                        prev_sell = _find_nth_trading_day(actual_sell_date, -1, trading_days_sorted)
                        if prev_sell:
                            prev_sell_entry = price_map.get((code, prev_sell))
                            if prev_sell_entry and prev_sell_entry["close"] > 0:
                                limit_down = prev_sell_entry["close"] * (1.0 - limit_pct)
                                if sell_entry["open"] <= limit_down * 1.001:
                                    at_limit_down = True
                        if not at_limit_down and sell_entry.get("pct_chg", 0) <= -limit_pct * 99.0:
                            at_limit_down = True
                        if at_limit_down:
                            next_sd = _find_next_trading_day(
                                actual_sell_date, trading_days_set, trading_days_sorted,
                            )
                            if not next_sd:
                                actual_sell_price = sell_entry["open"]
                                break
                            actual_sell_date = next_sd
                            postpone_count += 1
                        else:
                            actual_sell_price = sell_entry["open"]
                            break

                    if actual_sell_price is None or actual_sell_price <= 0:
                        continue
                    sell_price = actual_sell_price
                    skipped = False

                    if stop_strategy == "loss_aversion":
                        eff_sd, eff_sp = _check_loss_stop(
                            code, buy_date, buy_price, actual_sell_date,
                            price_map, _adj_by_code, trading_days_sorted,
                        )
                        if eff_sp > 0:
                            actual_sell_date = eff_sd
                            sell_price = eff_sp

                adj_b = _get_adj(code, buy_date)
                adj_s = _get_adj(code, actual_sell_date)
                raw_ret = (sell_price - buy_price) / buy_price
                ret = round((1.0 + raw_ret) * (adj_s / adj_b) - 1.0, 6)

                # Dead-hold: if losing at sell_date, extend up to 20 trading days
                was_dead_held = False
                if stop_strategy == "dead_hold" and ret < 0:
                    ext_sd, ext_sp, was_ext = _check_dead_hold(
                        code, buy_date, buy_price, actual_sell_date,
                        price_map, _adj_by_code, trading_days_sorted,
                    )
                    if was_ext:
                        was_dead_held = True
                        actual_sell_date = ext_sd
                        sell_price = ext_sp
                        adj_s2 = _get_adj(code, actual_sell_date)
                        raw_ret2 = (sell_price - buy_price) / buy_price
                        ret = round((1.0 + raw_ret2) * (adj_s2 / adj_b) - 1.0, 6)

                slot_trades.append({
                    "code": code, "ts_code": ts_code, "stock_name": stock_name,
                    "rank": rank, "buy_price": buy_price, "sell_price": sell_price,
                    "ret": ret, "skipped": skipped, "actual_sell_date": actual_sell_date,
                    "raw_score": raw_score,
                })
                held += 1

            if not slot_trades:
                continue

            n_stocks = len(slot_trades)
            per_stock = slot_size / n_stocks if n_stocks > 0 else 0

            # 按手数计算实际可买股数
            for st in slot_trades:
                min_lot, step = _get_lot_info(st["code"])
                raw_shares = per_stock / st["buy_price"] if st["buy_price"] > 0 else 0
                if raw_shares < min_lot:
                    st["shares"] = 0
                    continue
                shares = min_lot + int((raw_shares - min_lot) / step) * step
                st["shares"] = shares
                st["actual_cost"] = shares * st["buy_price"]

            valid_trades = [st for st in slot_trades if st["shares"] > 0]
            if not valid_trades:
                continue

            total_cost = sum(st["actual_cost"] for st in valid_trades)
            if total_cost > cash:
                # 资金不足，按比例缩减
                scale = cash / total_cost
                for st in valid_trades:
                    min_lot, step = _get_lot_info(st["code"])
                    new_shares = min_lot + int((st["shares"] * scale - min_lot) / step) * step
                    if new_shares < min_lot:
                        st["shares"] = 0
                        st["actual_cost"] = 0.0
                    else:
                        st["shares"] = new_shares
                        st["actual_cost"] = new_shares * st["buy_price"]
                valid_trades = [st for st in valid_trades if st["shares"] > 0]
                if not valid_trades:
                    continue
                total_cost = sum(st["actual_cost"] for st in valid_trades)

            if total_cost > cash:
                continue

            cash -= total_cost

            # 按实际持仓金额加权计算组合收益（使用已含复权因子的 ret）
            slot_pnl = sum(st["actual_cost"] * st["ret"] for st in valid_trades)
            slot_ret = slot_pnl / total_cost if total_cost > 0 else 0.0

            for st in valid_trades:
                trades.append({
                    "pred_date": pred_date, "stock_code": st["code"], "ts_code": st["ts_code"],
                    "stock_name": st["stock_name"], "rank": st["rank"],
                    "buy_date": buy_date, "buy_price": st["buy_price"],
                    "sell_date": st["actual_sell_date"], "sell_price": st["sell_price"],
                    "return_pct": st["ret"], "skipped": st["skipped"],
                    "target_return": st.get("raw_score", 0.0),
                    "shares": st["shares"], "actual_cost": round(st["actual_cost"], 2),
                })

            active_positions.append({
                "entry_value": total_cost,
                "sell_date": max(st["actual_sell_date"] for st in valid_trades),
                "ret_pct": slot_ret,
            })

        locked_value = sum(p["entry_value"] for p in active_positions)
        portfolio_value = cash + locked_value
        c_t = portfolio_value / INITIAL_CAPITAL
        if capital_curve:
            c_prev = capital_curve[-1]["capital"]
            daily_ret = c_t / c_prev - 1.0 if c_prev > 0 else 0.0
        else:
            daily_ret = 0.0
        capital_curve.append({
            "date": buy_date,
            "capital": round(c_t, 6),
            "daily_return": round(daily_ret, 6),
        })

    # Settle remaining active positions at market value for final capital
    holding_gain = 0.0
    for pos in active_positions:
        holding_gain += pos["entry_value"] * pos["ret_pct"]
    if active_positions:
        final_cash = cash + sum(p["entry_value"] for p in active_positions) + holding_gain
        final_portfolio = final_cash / INITIAL_CAPITAL
        if capital_curve and latest_td:
            c_prev = capital_curve[-1]["capital"]
            final_daily_ret = final_portfolio / c_prev - 1.0 if c_prev > 0 else 0.0
            capital_curve.append({
                "date": latest_td,
                "capital": round(final_portfolio, 6),
                "daily_return": round(final_daily_ret, 6),
            })

    holding_trades = [t for t in trades if not t["skipped"] and t["sell_date"] == ""]
    completed_trades = [t for t in trades if not t["skipped"] and t["sell_date"] != ""]
    skipped_count = sum(1 for t in trades if t["skipped"])
    win_count = sum(1 for t in completed_trades if t["return_pct"] > 0)
    total_count = len(completed_trades)

    final_capital = capital_curve[-1]["capital"] if capital_curve else 1.0
    win_rate = round(win_count / total_count, 4) if total_count > 0 else 0.0

    peak = 1.0
    max_dd = 0.0
    for pt in capital_curve:
        val = pt["capital"]
        if val > peak:
            peak = val
        dd = (val - peak) / peak
        if dd < max_dd:
            max_dd = dd

    sharpe = _compute_sharpe(capital_curve)

    return {
        "error": None,
        "metrics": {
            "cumulative_return": round(final_capital - 1.0, 4),
            "win_rate": win_rate,
            "max_drawdown": round(max_dd, 4),
            "sharpe_ratio": round(sharpe, 4),
            "total_trades": total_count,
            "skipped_trades": skipped_count,
            "holding_trades": len(holding_trades),
        },
        "capital_curve": capital_curve,
        "trades": trades,
    }


def _simulate_peak_backtest(exec_mode: str, top_n: int, stop_loss_pct: float = -0.10) -> dict:
    """Peak speed 模式回测：从预测文件读取，动态退出策略。

    退出优先级: 止损 → 止盈(预测收益 × 历史胜率) → 到期窗口(±2天) → 强制退出(20天)
    """
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")
    exec_suffix = "open2open" if exec_mode == "open" else "close2close"

    search_dir = _os.path.join(reports_dir, exec_suffix, "peak20d")
    if not _os.path.isdir(search_dir):
        return {"error": f"No peak20d reports for {exec_suffix}", "metrics": None, "capital_curve": [], "trades": []}

    json_files = sorted(_glob.glob(_os.path.join(search_dir, "*.json")))
    if not json_files:
        return {"error": f"No peak prediction files for {exec_suffix}", "metrics": None, "capital_curve": [], "trades": []}

    preds_by_date: Dict[str, list] = _defaultdict(list)
    for fp in json_files:
        with open(fp, "r", encoding="utf-8") as fh:
            data = _json.load(fh)
        pd_date = data.get("pred_date", "")
        if not pd_date:
            continue
        for p in data.get("predictions", [])[:top_n]:
            preds_by_date[pd_date].append({
                "stock_code": str(p.get("stock_code", "")).strip().zfill(6),
                "ts_code": str(p.get("ts_code", "")),
                "stock_name": str(p.get("stock_name", "")),
                "rank": int(p.get("rank", 0)),
                "predicted_days": int(p.get("predicted_days", 10)),
                "raw_score": float(p.get("raw_score", 0.0)),
            })

    if not preds_by_date:
        return {"error": "No peak prediction data found", "metrics": None, "capital_curve": [], "trades": []}

    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        from sqlalchemy import func
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(func.count(StockDaily.code) >= 3000)
            .order_by(StockDaily.date)
            .all()
        )
    trading_days_sorted = [
        d[0].strftime("%Y%m%d") if hasattr(d[0], "strftime") else str(d[0]).replace("-", "")[:8]
        for d in dates_raw
    ]
    trading_days_set = set(trading_days_sorted)
    latest_td = trading_days_sorted[-1] if trading_days_sorted else None

    # Collect all dates needed for price/adj lookup
    all_codes: set = set()
    all_dates: set = set()
    pred_dates_sorted = sorted(preds_by_date.keys())
    for pd_date in pred_dates_sorted:
        if exec_mode == "close":
            if pd_date not in trading_days_set:
                continue
            buy_date = pd_date
        else:
            buy_date = _find_next_trading_day(pd_date, trading_days_set, trading_days_sorted)
            if not buy_date:
                continue
        all_dates.add(buy_date)
        # Add up to 25 trading days after buy_date for potential exits
        for offset in range(26):
            sd = _find_nth_trading_day(buy_date, offset, trading_days_sorted)
            if sd:
                all_dates.add(sd)
        for p in preds_by_date[pd_date]:
            all_codes.add(p["stock_code"])

    # Add dates before each buy_date for open-mode prev-day lookup
    if exec_mode == "open":
        extra_dates = set()
        for d in all_dates:
            for i, td in enumerate(trading_days_sorted):
                if td == d and i > 0:
                    extra_dates.add(trading_days_sorted[i - 1])
                    break
        all_dates.update(extra_dates)

    all_dates_iso = {ds: f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}" for ds in all_dates}

    # Load prices
    with db.get_session() as session:
        rows = session.query(StockDaily).filter(
            StockDaily.date.in_([v for v in all_dates_iso.values()]),
            StockDaily.code.in_(list(all_codes)),
        ).all()

    price_map: Dict[tuple, dict] = {}
    for r in rows:
        d = r.date.strftime("%Y%m%d") if hasattr(r.date, "strftime") else str(r.date).replace("-", "")[:8]
        price_map[(str(r.code).strip().zfill(6), d)] = {
            "open": float(r.open) if r.open else 0.0,
            "close": float(r.close) if r.close else 0.0,
            "pct_chg": float(r.pct_chg) if r.pct_chg else 0.0,
        }

    # Load adj factors
    adj_map: Dict[tuple, float] = {}
    with db.get_session() as session:
        adj_rows = session.query(StockAdjFactor).filter(
            StockAdjFactor.code.in_(list(all_codes)),
            StockAdjFactor.trade_date.in_(
                [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in all_dates]
            ),
        ).all()
    for r in adj_rows:
        d = r.trade_date.strftime("%Y%m%d") if hasattr(r.trade_date, "strftime") else str(r.trade_date).replace("-", "")[:8]
        adj_map[(str(r.code).split(".")[0].zfill(6), d)] = float(r.adj_factor)

    _adj_by_code: Dict[str, list] = _defaultdict(list)
    for (code, d), v in adj_map.items():
        _adj_by_code[code].append((d, v))
    for code in _adj_by_code:
        _adj_by_code[code].sort(key=lambda x: x[0])

    def _get_adj_pk(code: str, date_str: str) -> float:
        entries = _adj_by_code.get(code, [])
        if not entries:
            return 1.0
        lo, hi = 0, len(entries) - 1
        best = 1.0
        while lo <= hi:
            mid = (lo + hi) // 2
            if entries[mid][0] <= date_str:
                best = entries[mid][1]
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    # Build date-to-index map for tracking holding days
    date_to_idx = {td: i for i, td in enumerate(trading_days_sorted)}

    # Build prediction queue: for each buy_date, what are the top-N picks
    buy_date_preds: Dict[str, list] = _defaultdict(list)
    for pd_date in pred_dates_sorted:
        if exec_mode == "close":
            if pd_date not in trading_days_set:
                continue
            buy_date = pd_date
        else:
            buy_date = _find_next_trading_day(pd_date, trading_days_set, trading_days_sorted)
            if not buy_date:
                continue
        for p in preds_by_date[pd_date]:
            p["_pred_date"] = pd_date
            buy_date_preds[buy_date].append(p)

    buy_dates_sorted = sorted(buy_date_preds.keys())
    if not buy_dates_sorted:
        return {"error": "No valid buy dates found", "metrics": None, "capital_curve": [], "trades": []}

    # Build sorted pred_date list for latest-prediction lookup
    _sorted_pred_dates = sorted(preds_by_date.keys())

    # Compute historical win rate per rank from matured predictions
    _rank_wins: Dict[int, int] = _defaultdict(int)
    _rank_total: Dict[int, int] = _defaultdict(int)
    preds_flat = [p for preds in buy_date_preds.values() for p in preds]
    for cand in preds_flat:
        code = cand["stock_code"]
        rank = cand["rank"]
        buy_d = cand["_pred_date"] if exec_mode == "close" else _find_next_trading_day(cand["_pred_date"], trading_days_set, trading_days_sorted)
        if not buy_d:
            continue
        sell_d = _find_nth_trading_day(buy_d, cand.get("predicted_days", 10), trading_days_sorted)
        if not sell_d or sell_d > latest_td:
            continue
        buy_px_entry = price_map.get((code, buy_d))
        if not buy_px_entry:
            continue
        buy_px = buy_px_entry["close"] if exec_mode == "close" else buy_px_entry["open"]
        if not buy_px or buy_px <= 0:
            continue
        buy_idx = trading_days_sorted.index(buy_d)
        sell_idx = trading_days_sorted.index(sell_d)
        peak_px = buy_px
        for di in range(buy_idx + 1, sell_idx + 1):
            px_entry = price_map.get((code, trading_days_sorted[di]))
            if px_entry:
                px = px_entry["close"] if exec_mode == "close" else px_entry["open"]
                if px and px > peak_px:
                    peak_px = px
        actual_ret = (peak_px - buy_px) / buy_px
        _rank_total[rank] += 1
        if actual_ret > 0:
            _rank_wins[rank] += 1
    rank_win_rate: Dict[int, float] = {}
    for r in sorted(_rank_total.keys()):
        rank_win_rate[r] = _rank_wins.get(r, 0) / _rank_total[r] if _rank_total[r] > 0 else 0.5

    # Extract all trading days from first buy_date to latest_td for the daily loop
    try:
        start_idx = trading_days_sorted.index(buy_dates_sorted[0])
    except ValueError:
        start_idx = 0
    try:
        end_idx = trading_days_sorted.index(latest_td) if latest_td else len(trading_days_sorted) - 1
    except ValueError:
        end_idx = len(trading_days_sorted) - 1
    all_trading_days = trading_days_sorted[start_idx:end_idx + 1]

    WINDOW_DAYS = 20

    INITIAL_CAPITAL = 10_000_000.0
    cash = INITIAL_CAPITAL
    positions: list = []  # [{code, ts_code, stock_name, buy_date, buy_price, pred_days, pred_return, entry_idx, alloc, adj_buy, rank}]
    trades: list = []
    capital_curve: list = []
    exit_reasons = {"stop_loss": 0, "take_profit": 0, "arrival": 0, "force_exit": 0}

    for td in all_trading_days:
        is_last_day = td == latest_td
        exited_this_round = False

        # --- 1. Check exits ---
        if positions:
            held_codes = [p["code"] for p in positions]
            to_close = []
            for pos in positions:
                cur_entry = price_map.get((pos["code"], td))
                if not cur_entry:
                    continue
                # open mode: check exit at open price; close mode: check at close price
                if exec_mode == "open":
                    cur_price = cur_entry["open"]
                else:
                    cur_price = cur_entry["close"]
                if cur_price <= 0:
                    continue
                adj_buy = pos.get("adj_buy", 1.0)
                adj_cur = _get_adj_pk(pos["code"], td)
                if adj_buy > 0 and adj_cur > 0:
                    raw_ret = (cur_price - pos["buy_price"]) / pos["buy_price"] if pos["buy_price"] > 0 else 0.0
                    adj_return = (1.0 + raw_ret) * (adj_cur / adj_buy) - 1.0
                else:
                    adj_return = (cur_price - pos["buy_price"]) / pos["buy_price"] if pos["buy_price"] > 0 else 0.0

                held_days = date_to_idx.get(td, 0) - pos["entry_idx"]

                exit_reason = None
                if adj_return <= stop_loss_pct:
                    exit_reason = "stop_loss"
                elif adj_return >= pos["pred_return"] * pos.get("win_rate", 0.5):
                    exit_reason = "take_profit"
                elif abs(held_days - pos["pred_days"]) <= 2 and adj_return > 0:
                    exit_reason = "arrival"
                elif held_days > WINDOW_DAYS:
                    exit_reason = "force_exit"

                if exit_reason:
                    to_close.append((pos, adj_return, exit_reason, held_days, cur_price))

            for pos, ret, reason, held, sell_price in to_close:
                adj_b = pos.get("adj_buy", 1.0)
                adj_s = _get_adj_pk(pos["code"], td)
                adj_ratio = (adj_s / adj_b) if adj_b > 0 else 1.0
                proceeds = pos["shares"] * sell_price * adj_ratio
                cash += proceeds
                trades.append({
                    "pred_date": pos.get("pred_date", ""),
                    "stock_code": pos["code"],
                    "ts_code": pos["ts_code"],
                    "stock_name": pos["stock_name"],
                    "rank": pos["rank"],
                    "buy_date": pos["buy_date"],
                    "buy_price": pos["buy_price"],
                    "sell_date": td,
                    "sell_price": sell_price,
                    "return_pct": round(ret, 6),
                    "skipped": False,
                    "expected_sell_date": pos.get("expected_sell_date", ""),
                    "target_return": round(pos["pred_return"] * pos.get("win_rate", 0.5), 4),
                    "shares": pos["shares"],
                    "actual_cost": round(pos["actual_cost"], 2),
                })
                exit_reasons[reason] += 1
                exited_this_round = True
                positions.remove(pos)

        # --- 2. Refill: buy top_n from previous trading day's prediction ---
        if not is_last_day and (exited_this_round or len(positions) == 0):
            open_slots = top_n
            if open_slots > 0:
                # close: use today's prediction; open: use previous trading day's prediction
                if exec_mode == "close":
                    refill_pred_date = td
                else:
                    refill_pred_date = _find_nth_trading_day(td, -1, trading_days_sorted)
                candidates = preds_by_date.get(refill_pred_date, []) if refill_pred_date else []

                if candidates:
                    alloc_per_slot = cash / max(top_n, 1)
                    for cand in candidates:
                        if open_slots <= 0:
                            break
                        entry = price_map.get((cand["stock_code"], td))
                        if not entry:
                            continue
                        if exec_mode == "close":
                            price = entry["close"]
                        else:
                            price = entry["open"]
                        if price <= 0:
                            continue

                        min_lot, step = _get_lot_info(cand["stock_code"])
                        raw_shares = alloc_per_slot / price
                        if raw_shares < min_lot:
                            continue
                        shares = min_lot + int((raw_shares - min_lot) / step) * step
                        actual_cost = shares * price
                        if actual_cost > cash:
                            continue

                        adj_b = _get_adj_pk(cand["stock_code"], td)
                        entry_idx = date_to_idx.get(td, 0)
                        cash -= actual_cost
                        positions.append({
                            "code": cand["stock_code"],
                            "ts_code": cand["ts_code"],
                            "stock_name": cand["stock_name"],
                            "pred_date": refill_pred_date,
                            "buy_date": td,
                            "buy_price": price,
                            "pred_days": cand["predicted_days"],
                            "pred_return": cand["raw_score"],
                            "win_rate": rank_win_rate.get(cand["rank"], 0.5),
                            "entry_idx": entry_idx,
                            "alloc": actual_cost,
                            "shares": shares,
                            "actual_cost": actual_cost,
                            "adj_buy": adj_b,
                            "rank": cand["rank"],
                            "expected_sell_date": _find_nth_trading_day(td, cand["predicted_days"], trading_days_sorted) or (latest_td or ""),
                        })
                        open_slots -= 1

        # --- 3. Mark-to-market portfolio value ---
        locked_value = 0.0
        for pos in positions:
            cur_entry = price_map.get((pos["code"], td))
            if cur_entry and cur_entry["close"] > 0:
                adj_b = pos.get("adj_buy", 1.0)
                adj_c = _get_adj_pk(pos["code"], td)
                adj_ratio = (adj_c / adj_b) if adj_b > 0 else 1.0
                locked_value += pos["shares"] * cur_entry["close"] * adj_ratio
            else:
                locked_value += pos["actual_cost"]
        portfolio_value = cash + locked_value
        c_t = portfolio_value / INITIAL_CAPITAL
        if capital_curve:
            c_prev = capital_curve[-1]["capital"]
            daily_ret = c_t / c_prev - 1.0 if c_prev > 0 else 0.0
        else:
            daily_ret = 0.0
        capital_curve.append({
            "date": td,
            "capital": round(c_t, 6),
            "daily_return": round(daily_ret, 6),
        })

    # Remaining positions: report as holding (no exit triggered before data end)
    for pos in positions:
        cur_entry = price_map.get((pos["code"], latest_td)) if latest_td else None
        adj_b = pos.get("adj_buy", 1.0)
        adj_s = _get_adj_pk(pos["code"], latest_td) if latest_td else 1.0
        adj_ratio = (adj_s / adj_b) if adj_b > 0 else 1.0
        if cur_entry and cur_entry["close"] > 0:
            sell_price = cur_entry["close"]
            raw_ret = (sell_price - pos["buy_price"]) / pos["buy_price"] if pos["buy_price"] > 0 else 0.0
            ret = (1.0 + raw_ret) * adj_ratio - 1.0
        else:
            sell_price = pos["buy_price"]
            ret = 0.0

        trades.append({
            "pred_date": pos.get("pred_date", ""),
            "stock_code": pos["code"],
            "ts_code": pos["ts_code"],
            "stock_name": pos["stock_name"],
            "rank": pos["rank"],
            "buy_date": pos["buy_date"],
            "buy_price": pos["buy_price"],
            "sell_date": "",
            "sell_price": sell_price,
            "return_pct": round(ret, 6),
            "skipped": False,
            "expected_sell_date": pos.get("expected_sell_date", ""),
            "target_return": round(pos["pred_return"] * pos.get("win_rate", 0.5), 4),
            "shares": pos["shares"],
            "actual_cost": round(pos["actual_cost"], 2),
        })
    positions.clear()

    # Metrics
    completed_trades = [t for t in trades if not t["skipped"]]
    win_count = sum(1 for t in completed_trades if t["return_pct"] > 0)
    total_count = len(completed_trades)
    final_capital = capital_curve[-1]["capital"] if capital_curve else 1.0
    win_rate = round(win_count / total_count, 4) if total_count > 0 else 0.0

    peak = 1.0
    max_dd = 0.0
    for pt in capital_curve:
        val = pt["capital"]
        if val > peak:
            peak = val
        dd = (val - peak) / peak
        if dd < max_dd:
            max_dd = dd

    sharpe = _compute_sharpe(capital_curve)

    return {
        "error": None,
        "metrics": {
            "cumulative_return": round(final_capital - 1.0, 4),
            "win_rate": win_rate,
            "max_drawdown": round(max_dd, 4),
            "sharpe_ratio": round(sharpe, 4),
            "total_trades": total_count,
            "skipped_trades": 0,
            "holding_trades": 0,
        },
        "capital_curve": capital_curve,
        "trades": trades,
    }


@router.get(
    "/lgb/backtest-sim/peak",
    response_model=LGBBacktestSimResponse,
    summary="Peak Speed 模式回测模拟（动态退出策略）",
)
def lgb_backtest_sim_peak(
    top_n: int = Query(5, ge=1, le=20, description="每预测日选取 Top N"),
    exec_mode: str = Query("open", pattern="^(open|close)$", description="执行模式: open=开盘买入→开盘卖出, close=收盘买入→收盘卖出"),
    stop_loss: float = Query(-0.10, description="止损线: -0.10 / -0.15 / -0.20"),
):
    cache_key = f"peak_top{top_n}_{exec_mode}_sl{stop_loss}"
    if cache_key in _backtest_cache:
        return _backtest_cache[cache_key]

    sim_result = _simulate_peak_backtest(exec_mode, top_n, stop_loss_pct=stop_loss)
    if sim_result.get("error"):
        raise HTTPException(status_code=404, detail=sim_result["error"])

    metrics = LGBBacktestSimMetrics(**sim_result["metrics"])
    trades = [LGBBacktestTradeItem(**t) for t in sim_result["trades"]]
    capital_curve = sim_result["capital_curve"]

    result = LGBBacktestSimResponse(
        forward_days=0,
        top_n=top_n,
        exec_mode=exec_mode,
        metrics=metrics,
        capital_curve=capital_curve,
        trades=trades,
    )

    _backtest_cache[cache_key] = result
    return result


@router.get(
    "/lgb/backtest-sim",
    response_model=LGBBacktestSimResponse,
    summary="LGB 预测交易回测模拟（基于预测文件）",
)
def lgb_backtest_sim(
    forward_days: int = Query(..., ge=1, le=60, description="前向天数（1 或 3）"),
    top_n: int = Query(5, ge=1, le=20, description="每预测日选取 Top N"),
    exec_mode: str = Query("open", pattern="^(open|close)$", description="执行模式: open=开盘买入→开盘卖出, close=收盘买入→收盘卖出"),
    stop_strategy: str = Query("none", pattern="^(none|loss_aversion|dead_hold)$", description="止损策略: none=默认, loss_aversion=亏损厌恶, dead_hold=跌了死扛"),
):
    cache_key = f"fwd{forward_days}_top{top_n}_{exec_mode}_st{stop_strategy}"
    if cache_key in _backtest_cache:
        return _backtest_cache[cache_key]

    sim_result = _simulate_backtest(exec_mode, forward_days, top_n, stop_strategy)
    if sim_result.get("error"):
        raise HTTPException(status_code=404, detail=sim_result["error"])

    metrics = LGBBacktestSimMetrics(**sim_result["metrics"])
    trades = [LGBBacktestTradeItem(**t) for t in sim_result["trades"]]
    capital_curve = sim_result["capital_curve"]

    result = LGBBacktestSimResponse(
        forward_days=forward_days,
        top_n=top_n,
        exec_mode=exec_mode,
        metrics=metrics,
        capital_curve=capital_curve,
        trades=trades,
    )

    _backtest_cache[cache_key] = result
    return result


@router.get(
    "/lgb/backtest-sim/available",
    response_model=LGBBacktestSimAvailableResponse,
    summary="可用回测模拟的 forward_days（基于本地 lgb_reports 目录）",
)
def lgb_backtest_sim_available():
    """扫描 lgb_reports/ 返回每个 exec_mode 下实际可用的 forward_days 以及是否有 peak 数据。"""
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")
    exec_dirs = {"open2open": "open", "close2close": "close"}
    result = {"open": [], "close": [], "has_peak": False}
    for dir_name, exec_key in exec_dirs.items():
        base = _os.path.join(reports_dir, dir_name)
        if not _os.path.isdir(base):
            continue
        for entry in sorted(_os.listdir(base)):
            if entry.startswith("fwd") and entry.endswith("d"):
                fwd_dir = _os.path.join(base, entry)
                if _os.path.isdir(fwd_dir):
                    json_count = len(_glob.glob(_os.path.join(fwd_dir, "*.json")))
                    if json_count > 0:
                        try:
                            fwd = int(entry[3:-1])
                            result[exec_key].append(fwd)
                        except ValueError:
                            pass
            elif entry.startswith("peak") and entry.endswith("d"):
                peak_dir = _os.path.join(base, entry)
                if _os.path.isdir(peak_dir) and _glob.glob(_os.path.join(peak_dir, "*.json")):
                    result["has_peak"] = True
        result[exec_key].sort()
    return result


# ── Brute-Force Search ──

_brute_force_tasks: Dict[str, dict] = {}


def _run_brute_force_search(task_id: str):
    """Background: iterate over all available forward_days, call _simulate_backtest, save report."""
    task = _brute_force_tasks.get(task_id)
    if not task:
        return
    task["status"] = "running"
    task["status_message"] = "Starting brute-force search..."

    try:
        stop_strategies = ["none", "loss_aversion", "dead_hold"]
        # ── Discover everything from lgb_reports/ cache files ──
        project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
        reports_root = _os.path.join(project_root, "lgb_reports")

        # 1) exec_modes from directory names (open2open→open, close2close→close)
        exec_mode_map: Dict[str, str] = {}
        all_forward_days: set = set()
        all_peak_windows: set = set()
        max_top_n = 0
        for dir_name in sorted(_os.listdir(reports_root)):
            dir_path = _os.path.join(reports_root, dir_name)
            if not _os.path.isdir(dir_path) or dir_name.startswith("."):
                continue
            if dir_name == "open2open":
                exec_mode_map["open2open"] = "open"
            elif dir_name == "close2close":
                exec_mode_map["close2close"] = "close"
            else:
                continue

            # 2) forward_days from fwd{N}d + peak{N}d subdirectories
            for entry in sorted(_os.listdir(dir_path)):
                entry_dir = _os.path.join(dir_path, entry)
                if not _os.path.isdir(entry_dir):
                    continue

                # Fixed mode: fwd{N}d
                if entry.startswith("fwd") and entry.endswith("d"):
                    try:
                        fwd = int(entry[3:-1])
                    except ValueError:
                        continue
                    all_forward_days.add(fwd)

                # Peak mode: peak{N}d
                if entry.startswith("peak") and entry.endswith("d"):
                    try:
                        wd = int(entry[4:-1])
                    except ValueError:
                        continue
                    all_peak_windows.add(wd)

                # 3) max top_n from prediction JSON files (sample first file per combo)
                json_files = _glob.glob(_os.path.join(entry_dir, "*.json"))
                if json_files:
                    try:
                        with open(json_files[0], "r", encoding="utf-8") as fh:
                            data = _json.load(fh)
                        n_preds = len(data.get("predictions", []))
                        if n_preds > max_top_n:
                            max_top_n = n_preds
                    except Exception:
                        pass

        exec_modes = sorted(set(exec_mode_map.values()))
        forward_days_list = sorted(all_forward_days) or [3, 5, 10]
        peak_windows_list = sorted(all_peak_windows) if all_peak_windows else []
        top_n_list = list(range(1, max(max_top_n, 5) + 1))

        # Fixed combos: stop_strategies × exec_modes × forward_days × top_n
        n_fixed = len(stop_strategies) * len(exec_modes) * len(forward_days_list) * len(top_n_list)
        # Peak combos: exec_modes × stop_loss × peak_windows × top_n
        peak_stop_losses = [-0.10, -0.15, -0.20]
        n_peak = len(exec_modes) * len(peak_stop_losses) * len(peak_windows_list) * len(top_n_list)
        total = n_fixed + n_peak

        all_results: list = []
        progress = 0

        # ── Fixed mode combos ──
        for st in stop_strategies:
            for em in exec_modes:
                for fwd in forward_days_list:
                    for tn in top_n_list:
                        progress += 1
                        task["progress_current"] = progress
                        task["progress_total"] = total
                        task["status_message"] = f"Fixed {st} {em} fwd={fwd} top={tn} ({progress}/{total})"

                        try:
                            sim = _simulate_backtest(em, fwd, tn, stop_strategy=st)
                        except Exception as exc:
                            import traceback
                            sim = {"error": f"Exception: {exc}\n{traceback.format_exc()}"}
                        item = LGBBruteForceItem(
                            exec_mode=em,
                            forward_days=fwd,
                            top_n=tn,
                            stop_strategy=st,
                            label_mode="fixed",
                            window_days=0,
                            cumulative_return=0.0,
                            sharpe_ratio=0.0,
                            win_rate=0.0,
                            max_drawdown=0.0,
                            total_trades=0,
                            skipped_trades=0,
                        )
                        if sim.get("error"):
                            item.error = sim["error"]
                        elif sim.get("metrics"):
                            m = sim["metrics"]
                            item.cumulative_return = m.get("cumulative_return", 0.0)
                            item.sharpe_ratio = m.get("sharpe_ratio", 0.0)
                            item.win_rate = m.get("win_rate", 0.0)
                            item.max_drawdown = m.get("max_drawdown", 0.0)
                            item.total_trades = m.get("total_trades", 0)
                            item.skipped_trades = m.get("skipped_trades", 0)
                        all_results.append(item)

        # ── Peak mode combos ──
        for em in exec_modes:
            for sl in peak_stop_losses:
                for wd in peak_windows_list:
                    for tn in top_n_list:
                        progress += 1
                        task["progress_current"] = progress
                        task["progress_total"] = total
                        sl_pct = f"{int(abs(sl)*100)}%"
                        task["status_message"] = f"Peak {em} window={wd}d top={tn} sl={sl_pct} ({progress}/{total})"

                        try:
                            sim = _simulate_peak_backtest(em, tn, stop_loss_pct=sl)
                        except Exception as exc:
                            import traceback
                            sim = {"error": f"Exception: {exc}\n{traceback.format_exc()}"}
                        item = LGBBruteForceItem(
                            exec_mode=em,
                            forward_days=0,
                            top_n=tn,
                            stop_strategy=f"sl{sl_pct}",
                            label_mode="peak_speed",
                            window_days=wd,
                            cumulative_return=0.0,
                            sharpe_ratio=0.0,
                            win_rate=0.0,
                            max_drawdown=0.0,
                            total_trades=0,
                            skipped_trades=0,
                        )
                        if sim.get("error"):
                            item.error = sim["error"]
                        elif sim.get("metrics"):
                            m = sim["metrics"]
                            item.cumulative_return = m.get("cumulative_return", 0.0)
                            item.sharpe_ratio = m.get("sharpe_ratio", 0.0)
                            item.win_rate = m.get("win_rate", 0.0)
                            item.max_drawdown = m.get("max_drawdown", 0.0)
                            item.total_trades = m.get("total_trades", 0)
                            item.skipped_trades = m.get("skipped_trades", 0)
                        all_results.append(item)

        # Sort
        valid = [r for r in all_results if not r.error]
        by_return = sorted(valid, key=lambda r: r.cumulative_return, reverse=True)
        by_sharpe = sorted(valid, key=lambda r: r.sharpe_ratio, reverse=True)

        result = LGBBruteForceResult(
            best_by_return=by_return[0] if by_return else None,
            best_by_sharpe=by_sharpe[0] if by_sharpe else None,
            top5_by_return=by_return[:5],
            top5_by_sharpe=by_sharpe[:5],
            all_results=all_results,
        )

        # Generate Markdown report
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
        reports_dir = _os.path.join(project_root, "lgb_reports")
        _os.makedirs(reports_dir, exist_ok=True)
        report_path = _os.path.join(reports_dir, f"brute_force_{now_str}.md")

        n_peak_str = f" + {n_peak} peak" if n_peak > 0 else ""
        lines = [
            f"# LGB 全方案搜索报告",
            f"",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**搜索方案数**: {total} ({len(stop_strategies)} stop × {len(exec_modes)} exec × {len(forward_days_list)} fwd × {len(top_n_list)} top_n{n_peak_str})",
            f"**成功方案数**: {len(valid)}",
            f"",
            f"---",
            f"",
            f"## 最佳收益方案 (Top 5)",
            f"",
            f"| # | label | stop | exec | fwd/window | top_n | cum_return | sharpe | win_rate | max_dd | trades |",
            f"|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for i, r in enumerate(by_return[:5], 1):
            label = f"peak{r.window_days}d" if r.label_mode == "peak_speed" else f"fwd{r.forward_days}d"
            fwd_str = str(r.window_days) if r.label_mode == "peak_speed" else str(r.forward_days)
            lines.append(
                f"| {i} | {label} | {r.stop_strategy} | {r.exec_mode} | {fwd_str} | {r.top_n} | "
                f"{r.cumulative_return:.4f} | {r.sharpe_ratio:.4f} | {r.win_rate:.4f} | "
                f"{r.max_drawdown:.4f} | {r.total_trades} |"
            )
        lines += [
            f"",
            f"## 最佳夏普方案 (Top 5)",
            f"",
            f"| # | label | stop | exec | fwd/window | top_n | cum_return | sharpe | win_rate | max_dd | trades |",
            f"|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for i, r in enumerate(by_sharpe[:5], 1):
            label = f"peak{r.window_days}d" if r.label_mode == "peak_speed" else f"fwd{r.forward_days}d"
            fwd_str = str(r.window_days) if r.label_mode == "peak_speed" else str(r.forward_days)
            lines.append(
                f"| {i} | {label} | {r.stop_strategy} | {r.exec_mode} | {fwd_str} | {r.top_n} | "
                f"{r.cumulative_return:.4f} | {r.sharpe_ratio:.4f} | {r.win_rate:.4f} | "
                f"{r.max_drawdown:.4f} | {r.total_trades} |"
            )
        lines += [
            f"",
            f"## 全部方案",
            f"",
            f"| # | label | stop | exec | fwd/window | top_n | cum_return | sharpe | win_rate | max_dd | trades | error |",
            f"|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for i, r in enumerate(all_results, 1):
            label = f"peak{r.window_days}d" if r.label_mode == "peak_speed" else f"fwd{r.forward_days}d"
            fwd_str = str(r.window_days) if r.label_mode == "peak_speed" else str(r.forward_days)
            err = r.error[:30] if r.error else ""
            lines.append(
                f"| {i} | {label} | {r.stop_strategy} | {r.exec_mode} | {fwd_str} | {r.top_n} | "
                f"{r.cumulative_return:.4f} | {r.sharpe_ratio:.4f} | {r.win_rate:.4f} | "
                f"{r.max_drawdown:.4f} | {r.total_trades} | {err} |"
            )

        with open(report_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")

        result.report_path = report_path

        # Also save JSON copy for frontend consumption
        json_path = _os.path.join(reports_dir, f"brute_force_{now_str}.json")
        with open(json_path, "w", encoding="utf-8") as fh:
            _json.dump(result.model_dump(mode="json"), fh, ensure_ascii=False, default=str)

        # Delete old brute force reports (keep only the latest)
        for old_file in _os.listdir(reports_dir):
            if old_file.startswith("brute_force_") and old_file != _os.path.basename(report_path) and old_file != _os.path.basename(json_path):
                if old_file.endswith(".md") or old_file.endswith(".json"):
                    _os.remove(_os.path.join(reports_dir, old_file))

        task["status"] = "completed"
        task["result"] = result
        task["status_message"] = f"Done — {len(valid)}/{total} combos valid, report saved to {report_path}"
        task["finished_at"] = datetime.now().isoformat()
    except Exception as exc:
        import traceback
        task["status"] = "failed"
        task["error"] = f"{exc}\n{traceback.format_exc()}"
        task["status_message"] = f"Failed: {exc}"
        task["finished_at"] = datetime.now().isoformat()


@router.get(
    "/lgb/cross-model-overlap",
    response_model=LGBCrossModelOverlapResponse,
    summary="统计同一 exec_mode 下所有模型的 Top 5 预测重叠情况",
)
def lgb_cross_model_overlap(
    exec_mode: str = Query("all", pattern="^(open|close|all)$", description="执行模式，all=全部"),
    top_n: int = Query(5, ge=1, le=20, description="从每个模型取前 N 只股票"),
):
    """遍历指定 exec_mode 下所有已保存模型，获取各自 Top N 预测，统计股票出现次数。"""
    from collections import Counter as _Counter

    all_models = LGBTrainer.list_models()
    if not all_models:
        raise HTTPException(status_code=404, detail="没有已保存的模型")

    if exec_mode == "all":
        matched = [m for m in all_models if m["name"].endswith(("open2open", "close2close"))]
    else:
        suffix = "open2open" if exec_mode == "open" else "close2close"
        matched = [m for m in all_models if m["name"].endswith(suffix)]
    if not matched:
        raise HTTPException(
            status_code=404,
            detail=f"没有找到 exec_mode={exec_mode} 的模型",
        )

    stock_counter: _Counter = _Counter()
    stock_info: dict = {}  # ts_code -> {stock_code, stock_name, model_names: list}
    models_used = 0

    # Load factor pivot once, share across all models
    import pandas as pd
    first_trainer = LGBTrainer.load(matched[0]["path"])
    first_trainer.predict()
    shared_pivot = first_trainer._X_latest
    # Add stock_code/stock_name columns if missing
    if "stock_code" not in shared_pivot.columns:
        shared_pivot = shared_pivot.copy()
        shared_pivot["stock_code"] = (
            shared_pivot["ts_code"].str.replace(".SH", "").str.replace(".SZ", "")
        )

    for m in matched:
        try:
            trainer = LGBTrainer.load(m["path"])
            pivot_copy = shared_pivot.copy()
            trainer.predict_from_pivot(pivot_copy)
            preds = trainer.get_latest_predictions(top_n=top_n)
            for p in preds:
                code = p.get("ts_code", "")
                stock_counter[code] += 1
                if code not in stock_info:
                    stock_info[code] = {
                        "stock_code": p.get("stock_code", code),
                        "stock_name": p.get("stock_name", ""),
                        "model_names": [],
                    }
                stock_info[code]["model_names"].append(m["name"])
            models_used += 1
        except Exception:
            continue

    stocks = []
    for ts_code, count in stock_counter.most_common():
        info = stock_info.get(ts_code, {})
        stocks.append(LGBCrossModelOverlapStock(
            stock_code=info.get("stock_code", ts_code),
            ts_code=ts_code,
            stock_name=info.get("stock_name", ""),
            count=count,
            model_names=info.get("model_names", []),
        ))

    return LGBCrossModelOverlapResponse(
        exec_mode=exec_mode,
        total_models=models_used,
        stocks=stocks,
    )


@router.post(
    "/lgb/brute-force-search",
    summary="启动全方案搜索（异步）：遍历 90 种参数组合寻找收益/夏普最优方案",
)
def lgb_brute_force_search():
    task_id = str(uuid.uuid4())[:8]
    _brute_force_tasks[task_id] = {
        "status": "pending",
        "progress_current": 0,
        "progress_total": 90,
        "status_message": "Queued...",
        "started_at": datetime.now().isoformat(),
    }
    threading.Thread(
        target=_run_brute_force_search,
        args=(task_id,),
        daemon=True,
    ).start()
    return {"task_id": task_id, "status": "pending"}


@router.get(
    "/lgb/brute-force-search/status",
    response_model=LGBBruteForceTaskStatus,
    summary="查询全方案搜索任务状态",
)
def lgb_brute_force_search_status(
    task_id: str = Query(..., description="任务 ID"),
):
    task = _brute_force_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")
    return LGBBruteForceTaskStatus(
        task_id=task_id,
        status=task.get("status", "unknown"),
        progress_current=task.get("progress_current", 0),
        progress_total=task.get("progress_total", 90),
        status_message=task.get("status_message", ""),
        result=task.get("result"),
        error=task.get("error", ""),
    )


def _parse_old_brute_force_md(md_path: str):
    """Parse legacy brute_force_*.md format (before JSON companion was added)."""
    from api.v1.schemas.research import LGBBruteForceItem

    with open(md_path, "r", encoding="utf-8") as fh:
        text = fh.read()

    def parse_table_section(start_marker: str, stop_marker: str | None = None) -> list[LGBBruteForceItem]:
        """Parse a pipe-separated table from the markdown text."""
        start_idx = text.find(start_marker)
        if start_idx < 0:
            return []
        # Find the table header after the marker
        header_start = text.find("| # |", start_idx)
        if header_start < 0:
            return []
        header_end = text.find("\n", header_start)
        header_line = text[header_start:header_end]
        headers = [h.strip() for h in header_line.strip("|").split("|")]

        body_start = text.find("\n", header_end + 1)
        if stop_marker:
            body_end = text.find(stop_marker, body_start)
        else:
            body_end = len(text)
        if body_end < 0:
            body_end = len(text)

        lines = [l.strip() for l in text[body_start:body_end].split("\n") if l.strip().startswith("|")]
        items: list[LGBBruteForceItem] = []
        for line in lines:
            cols = [c.strip() for c in line.strip("|").split("|")]
            if len(cols) < len(headers):
                continue
            row = dict(zip(headers, cols))
            try:
                forward_days = int(row.get("forward_days", 0))
                items.append(LGBBruteForceItem(
                    exec_mode=row.get("exec_mode", "close"),
                    forward_days=forward_days,
                    top_n=int(row.get("top_n", 1)),
                    stop_strategy=row.get("stop_strategy", "none"),
                    cumulative_return=float(row.get("cumulative_return", 0)),
                    sharpe_ratio=float(row.get("sharpe_ratio", 0)),
                    win_rate=float(row.get("win_rate", 0)),
                    max_drawdown=float(row.get("max_drawdown", 0)),
                    total_trades=int(row.get("total_trades", 0)),
                ))
            except (ValueError, KeyError):
                continue
        return items

    by_return = parse_table_section("## 最佳收益方案", "## 最佳夏普方案")
    by_sharpe = parse_table_section("## 最佳夏普方案", "## 全部方案")
    all_results = parse_table_section("## 全部方案")

    return LGBBruteForceResult(
        best_by_return=by_return[0] if by_return else None,
        best_by_sharpe=by_sharpe[0] if by_sharpe else None,
        top5_by_return=by_return[:5],
        top5_by_sharpe=by_sharpe[:5],
        all_results=all_results,
        report_path=md_path,
    )


@router.get(
    "/lgb/brute-force-reports/latest",
    response_model=LGBBruteForceResult,
    summary="获取最新的全方案搜索报告",
)
def lgb_brute_force_report_latest():
    """扫描 lgb_reports/ 目录，返回最新的 brute_force_*.json 解析结果。
    若无 JSON，则回退解析最新的 .md 文件。"""
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")

    json_files = sorted(
        [f for f in _os.listdir(reports_dir) if f.startswith("brute_force_") and f.endswith(".json")],
        reverse=True,
    )
    if json_files:
        latest = json_files[0]
        json_path = _os.path.join(reports_dir, latest)
        try:
            with open(json_path, "r", encoding="utf-8") as fh:
                data = _json.load(fh)
            return LGBBruteForceResult(**data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"读取JSON报告失败: {str(e)}")

    # Fallback: parse legacy markdown
    md_files = sorted(
        [f for f in _os.listdir(reports_dir) if f.startswith("brute_force_") and f.endswith(".md")],
        reverse=True,
    )
    if not md_files:
        raise HTTPException(status_code=404, detail="尚未生成任何全方案搜索报告")

    md_path = _os.path.join(reports_dir, md_files[0])
    try:
        return _parse_old_brute_force_md(md_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解析MD报告失败: {str(e)}")


_catch_up_tasks: Dict[str, dict] = {}


def _run_catch_up(task_id: str):
    """Run catch-up prediction for all combos."""
    import logging
    import traceback
    from dateutil.relativedelta import relativedelta

    _log = logging.getLogger(__name__)
    task = _catch_up_tasks[task_id]
    task["status"] = "running"

    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_root = _os.path.join(project_root, "lgb_reports")
    models_dir = _os.path.join(project_root, "src", "data", "lgb_models")

    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        from sqlalchemy import func
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(func.count(StockDaily.code) >= 3000)
            .order_by(StockDaily.date.desc())
            .first()
        )
    if not dates_raw:
        task["status"] = "failed"
        task["error"] = "No trading days found"
        return
    latest_td = dates_raw[0].strftime("%Y%m%d") if hasattr(dates_raw[0], "strftime") else str(dates_raw[0]).replace("-", "")[:8]

    # Discover all (exec_mode, label_mode, forward_days/window_days) combos
    combos: list = []
    exec_dirs = {"open2open": "open", "close2close": "close"}
    for dir_name, exec_mode in exec_dirs.items():
        base = _os.path.join(reports_root, dir_name)
        if not _os.path.isdir(base):
            continue
        for entry in sorted(_os.listdir(base)):
            entry_dir = _os.path.join(base, entry)
            if not _os.path.isdir(entry_dir):
                continue

            # Fixed mode: fwd{N}d
            if entry.startswith("fwd") and entry.endswith("d"):
                try:
                    fwd = int(entry[3:-1])
                except ValueError:
                    continue
                combos.append({"exec_mode": exec_mode, "label_mode": "fixed", "forward_days": fwd, "dir_name": dir_name, "subdir": entry})

            # Peak mode: peak{N}d
            if entry.startswith("peak") and entry.endswith("d"):
                try:
                    wd = int(entry[4:-1])
                except ValueError:
                    continue
                combos.append({"exec_mode": exec_mode, "label_mode": "peak_speed", "window_days": wd, "dir_name": dir_name, "subdir": entry})

    total = len(combos)
    task["progress_total"] = total
    results: list = []

    for idx, combo in enumerate(combos):
        exec_mode = combo["exec_mode"]
        label_mode = combo["label_mode"]
        dir_name = combo["dir_name"]
        subdir = combo["subdir"]
        is_peak = label_mode == "peak_speed"
        fwd = combo.get("forward_days", 0)
        wd = combo.get("window_days", 20)

        label_tag = f"peak{wd}d" if is_peak else f"fwd{fwd}d"
        task["progress_current"] = idx + 1
        task["status_message"] = f"{dir_name} {label_tag}..."

        # Find latest prediction date and last training window
        report_dir = _os.path.join(reports_root, dir_name, subdir)
        json_files = sorted(_glob.glob(_os.path.join(report_dir, "*_pred_*.json")))

        latest_pred_date = ""
        if json_files:
            for jf in reversed(json_files):
                try:
                    with open(jf, "r", encoding="utf-8") as fh:
                        data = _json.load(fh)
                    pd = data.get("pred_date", "")
                    if pd > latest_pred_date:
                        latest_pred_date = pd
                except Exception:
                    continue

        if latest_pred_date >= latest_td:
            results.append({
                "exec_mode": exec_mode, "forward_days": fwd, "label_mode": label_mode,
                "window_days": wd, "status": "up_to_date", "latest_pred": latest_pred_date,
            })
            continue

        # Determine prediction range: from day after latest prediction to today
        if latest_pred_date:
            new_pred_s = datetime.strptime(latest_pred_date, "%Y%m%d") + timedelta(days=1)
        else:
            new_pred_s = datetime.strptime(latest_td, "%Y%m%d") - timedelta(days=30)
        new_pred_e_str = latest_td

        # Training window: full calendar months, 12 months ending before first prediction
        from calendar import monthrange
        _te = new_pred_s - relativedelta(months=1)
        _te = _te.replace(day=monthrange(_te.year, _te.month)[1])
        new_train_e = _te
        new_train_s = _te.replace(day=1) - relativedelta(months=11)

        new_train_s_str = new_train_s.strftime("%Y%m%d")
        new_train_e_str = new_train_e.strftime("%Y%m%d")

        # Find existing model for this combo
        exec_tag = "open2open" if exec_mode == "open" else "close2close"
        existing_model = None
        if _os.path.isdir(models_dir):
            import re as _re
            if is_peak:
                _pat = _re.compile(rf"_peak{wd}d_")
            else:
                _pat = _re.compile(rf"_fwd{fwd}d_")
            for mf in sorted(_os.listdir(models_dir), reverse=True):
                if mf.endswith(".joblib") and not mf.endswith("_days.joblib") and _pat.search(mf) and exec_tag in mf:
                    existing_model = _os.path.join(models_dir, mf)
                    break

        try:
            if existing_model:
                task["status_message"] = f"{dir_name} {label_tag}: 加载已有模型预测..."
                trainer = LGBTrainer.load(existing_model)
            else:
                task["status_message"] = f"{dir_name} {label_tag}: 训练 {new_train_s_str}~{new_train_e_str}..."
                if is_peak:
                    trainer = LGBTrainer(mode="postmarket", label_mode="peak_speed", window_days=wd, exec_mode=exec_mode)
                else:
                    trainer = LGBTrainer(mode="postmarket", forward_days=fwd, exec_mode=exec_mode)
                trainer.prepare_data(start_date=new_train_s_str, end_date=new_train_e_str)
                trainer.train()
                trainer.save()

            # Get all trading days from pred_start to latest
            from scripts.rolling_lgb_backtest import get_trading_days
            trading_days = get_trading_days(new_pred_s.strftime("%Y%m%d"), new_pred_e_str)
            ok = 0
            fail = 0
            for td in trading_days:
                try:
                    from scripts.rolling_lgb_backtest import save_daily_report, _ymd
                    trainer.predict(target_date=_ymd(td))
                    save_daily_report(trainer, _ymd(td))
                    ok += 1
                except Exception:
                    fail += 1

            results.append({
                "exec_mode": exec_mode, "forward_days": fwd, "label_mode": label_mode,
                "window_days": wd, "status": "done",
                "train_window": f"{new_train_s_str}~{new_train_e_str}",
                "pred_range": f"{new_pred_s.strftime('%Y%m%d')}~{new_pred_e_str}",
                "ok": ok, "fail": fail,
                "used_existing_model": bool(existing_model),
            })

        except Exception as e:
            _log.error(f"Catch-up failed for {exec_mode} {label_tag}: {e}")
            results.append({
                "exec_mode": exec_mode, "forward_days": fwd, "label_mode": label_mode,
                "window_days": wd, "status": "failed", "error": str(e),
            })

    task["status"] = "completed"
    task["result"] = {"combos": results, "latest_trading_day": latest_td}
    task["finished_at"] = datetime.now().isoformat()


@router.post(
    "/lgb/catch-up",
    summary="补全所有模式的预测到最新日期",
)
def lgb_catch_up():
    """检查每个 (exec_mode, forward_days) 组合是否有未预测日期，自动补全。
    若跨月则重新训练（滑动窗口），并删除旧模型。"""
    for tid, t in list(_catch_up_tasks.items()):
        if t.get("status") == "running":
            raise HTTPException(
                status_code=409,
                detail=f"已有补全任务运行中（task_id={tid}），请等待完成后再试",
            )

    task_id = str(uuid.uuid4())[:8]
    _catch_up_tasks[task_id] = {
        "status": "pending",
        "progress_current": 0,
        "progress_total": 0,
        "status_message": "Queued...",
        "started_at": datetime.now().isoformat(),
    }
    threading.Thread(
        target=_run_catch_up,
        args=(task_id,),
        daemon=True,
    ).start()
    return {"task_id": task_id, "status": "pending"}


@router.get(
    "/lgb/catch-up/status",
    summary="查询补全预测任务状态",
)
def lgb_catch_up_status(task_id: str = Query(..., description="任务 ID")):
    task = _catch_up_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")
    return {
        "task_id": task_id,
        "status": task.get("status", "unknown"),
        "progress_current": task.get("progress_current", 0),
        "progress_total": task.get("progress_total", 0),
        "status_message": task.get("status_message", ""),
        "result": task.get("result"),
        "error": task.get("error", ""),
    }


def _warmup_backtest_cache():
    """Pre-warm backtest-sim cache in background to avoid first-request timeout.
    Discovers available combos dynamically from lgb_reports/ directory."""
    import logging
    _log = logging.getLogger(__name__)
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_root = _os.path.join(project_root, "lgb_reports")

    combos: list = []
    exec_dirs = {"open2open": "open", "close2close": "close"}
    for dir_name, exec_mode in exec_dirs.items():
        base = _os.path.join(reports_root, dir_name)
        if not _os.path.isdir(base):
            continue
        for entry in sorted(_os.listdir(base)):
            if not entry.startswith("fwd") or not entry.endswith("d"):
                continue
            try:
                fwd = int(entry[3:-1])
            except ValueError:
                continue
            fwd_dir = _os.path.join(base, entry)
            if not _os.path.isdir(fwd_dir):
                continue
            # Sample one JSON file to find top_n
            json_files = _glob.glob(_os.path.join(fwd_dir, "*.json"))
            n_preds = 5
            if json_files:
                try:
                    with open(json_files[0], "r", encoding="utf-8") as fh:
                        data = _json.load(fh)
                    n_preds = len(data.get("predictions", []))
                except Exception:
                    pass
            combos.append((fwd, min(n_preds, 5), exec_mode))

    for fwd, tn, em in combos:
        key = f"fwd{fwd}_top{tn}_{em}_stnone"
        if key not in _backtest_cache:
            try:
                lgb_backtest_sim(forward_days=fwd, top_n=tn, exec_mode=em, stop_strategy="none")
                _log.info(f"Backtest cache warmed: {key}")
            except Exception:
                _log.warning(f"Backtest cache warmup failed: {key}", exc_info=True)

    # Also warm peak backtest cache
    for exec_mode in ["open", "close"]:
        peak_key = f"peak_top5_{exec_mode}"
        if peak_key not in _backtest_cache:
            try:
                lgb_backtest_sim_peak(top_n=5, exec_mode=exec_mode, stop_loss=-0.10)
                _log.info(f"Peak backtest cache warmed: {peak_key}")
            except Exception:
                _log.warning(f"Peak backtest cache warmup failed: {peak_key}", exc_info=True)


# Fire-and-forget background warmup after module loads
_t = threading.Thread(target=_warmup_backtest_cache, daemon=True)
_t.start()


# ------------------------------------------------------------------
# 因子子集最优搜索
# ------------------------------------------------------------------

_factor_subset_tasks: Dict[str, dict] = {}


def _run_factor_subset_search(task_id: str, params: dict):
    from src.discovery.ml.factor_subset_search import FactorSubsetSearcher

    task = _factor_subset_tasks.get(task_id)
    if task is None:
        return

    task["status"] = "running"
    task["status_message"] = "正在搜索最优因子组合..."

    def progress(msg: str):
        task["status_message"] = msg

    try:
        searcher = FactorSubsetSearcher(
            mode=params.get("mode", "postmarket"),
            label_mode=params.get("label_mode", "fixed"),
            forward_days=params.get("forward_days", 5),
            window_days=params.get("window_days", 20),
            exec_mode=params.get("exec_mode", "open"),
            start_date=params.get("start_date"),
            end_date=params.get("end_date"),
            cv_folds=params.get("cv_folds", 5),
            tpe_trials=params.get("tpe_trials", 80),
            top_n=5,
            progress_callback=progress,
        )
        # 加载数据一次，测试 top_n 1-5，取最优
        searcher.load_data()
        best_result = None
        best_daily_return = -999.0
        for tn in range(1, 6):
            progress(f"[top_n={tn}] 开始搜索...")
            searcher.top_n = tn
            # 包装 progress_callback，确保所有内部日志都带 top_n 标识
            searcher.cb = lambda msg, _tn=tn: progress(f"[top_n={_tn}] {msg}")
            result = searcher.run_search(save_report=False)
            dr = result.get("final_metrics", {}).get("daily_return_mean", -999.0)
            progress(f"[top_n={tn}] 完成: daily_return={dr:.6f}")
            if dr > best_daily_return:
                best_daily_return = dr
                best_result = result
                searcher.top_n = tn  # 确保 _save_report 使用最优 top_n
                # 增量保存当前最优（中断时也有一份已完成 top_n 中的最优报告）
                searcher._save_report(best_result)

        # 查找报告路径
        report_path = ""
        import glob as _glob
        report_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))), "lgb_reports", "factor_subset")
        if os.path.isdir(report_dir):
            json_files = sorted(_glob.glob(os.path.join(report_dir, "subset_*.json")),
                                reverse=True)
            if json_files:
                report_path = json_files[0]

        fm = best_result.get("final_metrics", {})
        bl = best_result.get("phase1", {}).get("baseline", {})
        task["status"] = "completed"
        task["result"] = {
            "all_factors": best_result.get("all_factors", []),
            "final_subset": best_result.get("final_subset", []),
            "excluded_factors": best_result.get("excluded_factors", []),
            "baseline_daily_return": bl.get("daily_return_mean", 0.0),
            "final_daily_return": fm.get("daily_return_mean", 0.0),
            "delta_daily_return": fm.get("daily_return_mean", 0.0) - bl.get("daily_return_mean", 0.0),
            "baseline_ic": bl.get("rank_ic_mean", 0.0),
            "final_ic": fm.get("rank_ic_mean", 0.0),
            "final_icir": fm.get("icir", 0.0),
            "final_rmse": fm.get("cv_rmse_mean", 0.0),
            "top_n": best_result.get("top_n", 5),
            "elapsed_seconds": best_result.get("elapsed_seconds", 0.0),
            "report_path": report_path,
        }
    except Exception as e:
        task["status"] = "failed"
        task["error"] = str(e)
        import traceback
        traceback.print_exc()


@router.post(
    "/lgb/factor-subset-search",
    summary="启动因子子集最优搜索",
)
def lgb_factor_subset_search(
    label_mode: str = Query("fixed", description="标签模式: fixed / peak_speed"),
    forward_days: int = Query(5, description="前瞻天数 (fixed 模式)"),
    window_days: int = Query(20, description="窗口天数 (peak_speed 模式)"),
    exec_mode: str = Query("open", description="执行模式: open / close"),
    mode: str = Query("postmarket", description="postmarket / intraday"),
    tpe_trials: int = Query(80, description="TPE 精调试验次数"),
    top_n: int = Query(5, description="每日买入 top N 股票"),
):
    task_id = str(uuid.uuid4())[:8]
    _factor_subset_tasks[task_id] = {
        "status": "pending",
        "status_message": "Queued...",
        "started_at": datetime.now().isoformat(),
    }
    params = {
        "label_mode": label_mode,
        "forward_days": forward_days,
        "window_days": window_days,
        "exec_mode": exec_mode,
        "mode": mode,
        "tpe_trials": tpe_trials,
        "top_n": top_n,
    }
    threading.Thread(
        target=_run_factor_subset_search,
        args=(task_id, params),
        daemon=True,
    ).start()
    return {"task_id": task_id, "status": "pending"}


@router.get(
    "/lgb/factor-subset-search/status",
    response_model=LGBFactorSubsetTaskStatus,
    summary="查询因子子集搜索任务状态",
)
def lgb_factor_subset_search_status(
    task_id: str = Query(..., description="任务 ID"),
):
    task = _factor_subset_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")
    return LGBFactorSubsetTaskStatus(
        task_id=task_id,
        status=task.get("status", "unknown"),
        status_message=task.get("status_message", ""),
        result=task.get("result"),
        error=task.get("error", ""),
    )


@router.post(
    "/lgb/factor-subset-apply",
    summary="应用因子子集搜索结果",
)
def lgb_factor_subset_apply():
    """将最近一次搜索结果的排除因子写入 .env（与已有值合并）。"""
    latest = None
    for _tid, t in _factor_subset_tasks.items():
        if t.get("status") == "completed" and t.get("result"):
            if latest is None or t.get("started_at", "") > latest.get("started_at", ""):
                latest = t
    if latest is None:
        raise HTTPException(status_code=404, detail="没有已完成的搜索结果")

    result = latest["result"]
    new_excluded = result.get("excluded_factors", [])
    if not new_excluded:
        return {"applied": False, "message": "无需排除任何因子"}

    # 读取已有的 LGB_DISABLE_FACTOR
    existing_excluded = []
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if stripped.startswith("LGB_DISABLE_FACTOR=") or stripped.startswith("# LGB_DISABLE_FACTOR="):
                    val = stripped.split("=", 1)[1].split("#")[0].strip()
                    if val:
                        existing_excluded = [f.strip() for f in val.split(",") if f.strip()]
                    break

    from src.discovery.ml.factor_subset_search import FactorSubsetSearcher
    searcher = FactorSubsetSearcher()
    env_value = searcher.apply_best_subset(result)
    return {
        "applied": True,
        "existing_excluded": existing_excluded,
        "new_excluded": new_excluded,
        "env_value": env_value,
    }


# ── Factor Subset Batch Search ──

_factor_subset_batch_tasks: Dict[str, dict] = {}


def _run_factor_subset_batch(task_id: str, params: dict):
    from src.discovery.ml.factor_subset_search import FactorSubsetSearcher

    task = _factor_subset_batch_tasks.get(task_id)
    if task is None:
        return

    task["status"] = "running"
    task["status_message"] = "正在初始化批量搜索..."

    def progress(msg: str):
        task["status_message"] = msg

    try:
        configs = params.get("configs", [])
        searcher = FactorSubsetSearcher(
            mode=params.get("mode", "postmarket"),
            exec_mode=params.get("exec_mode", "open"),
            start_date=params.get("start_date"),
            end_date=params.get("end_date"),
            cv_folds=params.get("cv_folds", 5),
            tpe_trials=params.get("tpe_trials", 80),
            progress_callback=progress,
        )
        result = searcher.batch_run(
            configs=configs,
            start_date=params.get("start_date"),
            end_date=params.get("end_date"),
        )
        task["status"] = "completed"
        task["result"] = result
    except Exception as e:
        task["status"] = "failed"
        task["error"] = str(e)
        import traceback
        traceback.print_exc()


@router.post(
    "/lgb/factor-subset-batch",
    summary="启动因子子集批量搜索",
)
def lgb_factor_subset_batch(
    exec_mode: str = Query("open", description="执行模式: open / close"),
    mode: str = Query("postmarket", description="postmarket / intraday"),
    start_date: str = Query("20250101", description="数据起始日期"),
    end_date: str = Query("20260525", description="数据结束日期"),
    tpe_trials: int = Query(80, description="TPE 精调试验次数"),
):
    # 构建全量配置: open/close × fixed(3/5/10/20d) × 2 top_n + peak_speed(20d) × 2 top_n
    configs = []
    for exec in ["open", "close"]:
        for fwd in [3, 5, 10, 20]:
            for tn in [1, 2]:
                configs.append({"exec_mode": exec, "label_mode": "fixed", "forward_days": fwd, "top_n": tn})
        for tn in [1, 2]:
            configs.append({"exec_mode": exec, "label_mode": "peak_speed", "forward_days": 5, "window_days": 20, "top_n": tn})

    task_id = str(uuid.uuid4())[:8]
    _factor_subset_batch_tasks[task_id] = {
        "status": "pending",
        "status_message": "Queued...",
        "started_at": datetime.now().isoformat(),
    }
    params = {
        "configs": configs,
        "exec_mode": exec_mode,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "tpe_trials": tpe_trials,
    }
    threading.Thread(
        target=_run_factor_subset_batch,
        args=(task_id, params),
        daemon=True,
    ).start()
    return {"task_id": task_id, "status": "pending", "total_configs": len(configs)}


@router.get(
    "/lgb/factor-subset-batch/status",
    summary="查询因子子集批量搜索任务状态",
)
def lgb_factor_subset_batch_status(
    task_id: str = Query(..., description="任务 ID"),
):
    task = _factor_subset_batch_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")
    return {
        "task_id": task_id,
        "status": task.get("status", "unknown"),
        "status_message": task.get("status_message", ""),
        "result": task.get("result"),
        "error": task.get("error", ""),
    }


# ── Rolling Backtest ──

_rolling_tasks: Dict[str, dict] = {}


def _run_rolling_backtest(task_id: str):
    """后台运行滚动窗口 LGB 回测脚本。"""
    import subprocess

    task = _rolling_tasks.get(task_id)
    if task is None:
        return

    task["status"] = "running"
    task["status_message"] = "正在启动滚动回测..."

    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))), "scripts", "rolling_lgb_backtest.py")
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))

    try:
        env = os.environ.copy()
        env.pop("LGB_DISABLE_FACTOR", None)  # 不继承黑名单，使用 factor_subset/ 白名单
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=project_root,
            env=env,
        )

        output_lines: list = []
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                output_lines.append(line)
                task["status_message"] = line[-200:]
                if len(output_lines) > 500:
                    output_lines.pop(0)

        proc.wait()
        if proc.returncode == 0:
            task["status"] = "completed"
            task["status_message"] = "滚动回测完成"
            task["result"] = {"output": "\n".join(output_lines[-100:])}
        else:
            task["status"] = "failed"
            task["error"] = f"脚本退出码: {proc.returncode}"
            task["result"] = {"output": "\n".join(output_lines[-100:])}
    except Exception as e:
        task["status"] = "failed"
        task["error"] = str(e)
        import traceback
        traceback.print_exc()


@router.post(
    "/lgb/rolling-backtest",
    summary="启动滚动窗口 LGB 回测",
)
def lgb_rolling_backtest():
    task_id = str(uuid.uuid4())[:8]
    _rolling_tasks[task_id] = {
        "status": "pending",
        "status_message": "Queued...",
        "started_at": datetime.now().isoformat(),
    }
    threading.Thread(
        target=_run_rolling_backtest,
        args=(task_id,),
        daemon=True,
    ).start()
    return {"task_id": task_id, "status": "pending"}


@router.get(
    "/lgb/rolling-backtest/status",
    summary="查询滚动回测任务状态",
)
def lgb_rolling_backtest_status(
    task_id: str = Query(..., description="任务 ID"),
):
    task = _rolling_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")
    return {
        "task_id": task_id,
        "status": task.get("status", "unknown"),
        "status_message": task.get("status_message", ""),
        "result": task.get("result"),
        "error": task.get("error", ""),
    }
