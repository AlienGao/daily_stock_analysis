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
    LGBBacktestSimResponse,
    LGBBacktestSimMetrics,
    LGBBacktestTradeItem,
    LGBBacktestSimAvailableResponse,
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


def _run_train_in_process(queue: multiprocessing.Queue, req_dict: dict):
    try:
        def _progress(msg: str):
            queue.put(("progress", msg))

        trainer = LGBTrainer(
            mode=req_dict["mode"],
            forward_days=req_dict["forward_days"],
            exec_mode=req_dict.get("exec_mode", "close"),
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
        trainer.predict(target_date=req_dict.get("end_date"))

        _progress("正在保存模型...")
        model_path = trainer.save()

        _progress("正在生成报告...")
        report_path = trainer.save_report(top_n=5)

        importance = trainer.get_feature_importance()
        predictions = trainer.get_latest_predictions()
        metrics = trainer._training_metrics

        queue.put(("completed", {
            "model_path": model_path,
            "report_path": report_path,
            "model_date": trainer._latest_date,
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
        "exec_mode": req.exec_mode,
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

    if trainer._X_latest is None:
        trainer.predict()
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
    exec_mode: str = Query("close", description="标签模式（仅无 model_path 时生效）"),
):
    if model_path:
        trainer = LGBTrainer.load(model_path)
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


def _find_next_trading_day(d: str, trading_days_set: set, trading_days_sorted: list) -> _Optional[str]:
    """Find the next trading day strictly after date d (YYYYMMDD)."""
    d_str = d.replace("-", "")[:8]
    for td in trading_days_sorted:
        if td > d_str:
            return td
    return None


def _find_nth_trading_day(start: str, n: int, trading_days_sorted: list) -> _Optional[str]:
    """Return the nth trading day on or after start (0-indexed)."""
    try:
        idx = trading_days_sorted.index(start)
    except ValueError:
        return None
    target_idx = idx + n
    if 0 <= target_idx < len(trading_days_sorted):
        return trading_days_sorted[target_idx]
    return None


@router.get(
    "/lgb/backtest-sim",
    response_model=LGBBacktestSimResponse,
    summary="LGB 预测交易回测模拟（基于预测文件）",
)
def lgb_backtest_sim(
    forward_days: int = Query(..., ge=1, le=60, description="前向天数（1 或 3）"),
    top_n: int = Query(5, ge=1, le=20, description="每预测日选取 Top N"),
    exec_mode: str = Query("open", pattern="^(open|close)$", description="执行模式: open=开盘买入→开盘卖出, close=收盘买入→收盘卖出"),
):
    cache_key = f"fwd{forward_days}_top{top_n}_{exec_mode}"
    if cache_key in _backtest_cache:
        return _backtest_cache[cache_key]

    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")

    # 1. Scan prediction files (filtered by exec_mode suffix)
    exec_suffix = "open2open" if exec_mode == "open" else "close2close"
    fwd_dir = f"fwd{forward_days}d"
    pattern = f"*_fwd{forward_days}d_*_pred_*.json"
    search_dir = _os.path.join(reports_dir, exec_suffix, fwd_dir)
    json_files = sorted(_glob.glob(_os.path.join(search_dir, pattern)))
    # Fallback: also check exec_suffix dir without fwd subdir for legacy files
    if not json_files:
        json_files = sorted(_glob.glob(_os.path.join(reports_dir, exec_suffix, pattern)))
    if not json_files:
        raise HTTPException(status_code=404, detail=f"No {exec_suffix} prediction files found for forward_days={forward_days}")

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
            })

    if not preds_by_date:
        raise HTTPException(status_code=404, detail="No prediction data found")

    # 2. Get real trading days from stock_daily (filter sparse/fake dates)
    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        from sqlalchemy import func
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(func.count(StockDaily.code) >= 100)
            .order_by(StockDaily.date)
            .all()
        )
    trading_days_sorted = [
        d[0].strftime("%Y%m%d") if hasattr(d[0], "strftime") else str(d[0]).replace("-", "")[:8]
        for d in dates_raw
    ]
    trading_days_set = set(trading_days_sorted)

    # 3. Collect all needed stock codes and date pairs
    all_codes: set = set()
    date_pairs: list = []  # (pred_date, buy_date, sell_date) — sell_date None = holding
    for pred_date, preds in sorted(preds_by_date.items()):
        if exec_mode == "close":
            if pred_date not in trading_days_set:
                continue
            sell_date = _find_nth_trading_day(pred_date, forward_days, trading_days_sorted)
            date_pairs.append((pred_date, pred_date, sell_date))
        else:
            buy_date = _find_next_trading_day(pred_date, trading_days_set, trading_days_sorted)
            if not buy_date:
                continue
            sell_date = _find_nth_trading_day(buy_date, forward_days, trading_days_sorted)
            date_pairs.append((pred_date, buy_date, sell_date))
        for p in preds:
            all_codes.add(p["stock_code"])

    if not date_pairs:
        raise HTTPException(status_code=404, detail="No valid trading dates found")

    # 4. Batch fetch all price data in one query
    all_date_strs: set = set()
    latest_td = trading_days_sorted[-1] if trading_days_sorted else None
    has_holding = any(sd is None for _, _, sd in date_pairs)
    for _, bd, sd in date_pairs:
        all_date_strs.add(bd)
        if sd is not None:
            all_date_strs.add(sd)
    if has_holding and latest_td:
        all_date_strs.add(latest_td)

    if exec_mode == "open":
        # Need pre_close (previous trading day close) for limit-up/down checks
        for d in list(all_date_strs):
            prev_td = None
            for i, td in enumerate(trading_days_sorted):
                if td == d and i > 0:
                    prev_td = trading_days_sorted[i - 1]
                    break
            if prev_td:
                all_date_strs.add(prev_td)
    else:
        # Close mode: also need pre_close for limit-up checks on buy dates
        for _, bd, _ in date_pairs:
            prev_td = None
            for i, td in enumerate(trading_days_sorted):
                if td == bd and i > 0:
                    prev_td = trading_days_sorted[i - 1]
                    break
            if prev_td:
                all_date_strs.add(prev_td)

    all_dates_iso = {
        ds: f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"
        for ds in all_date_strs
    }

    with db.get_session() as session:
        rows = session.query(StockDaily).filter(
            StockDaily.date.in_([v for v in all_dates_iso.values()]),
            StockDaily.code.in_(list(all_codes)),
        ).all()

    # Build price lookup: {(code, YYYYMMDD): {open, close, pct_chg}}
    price_map: Dict[tuple, dict] = {}
    for r in rows:
        d = r.date.strftime("%Y%m%d") if hasattr(r.date, "strftime") else str(r.date).replace("-", "")[:8]
        price_map[(str(r.code).strip().zfill(6), d)] = {
            "open": float(r.open) if r.open else 0.0,
            "close": float(r.close) if r.close else 0.0,
            "pct_chg": float(r.pct_chg) if r.pct_chg else 0.0,
        }

    # Build adj_factor lookup: {(code, YYYYMMDD): adj_factor}
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

    # 5. Simulate trades with rolling position sizing
    # initial_capital = 100w, each slot uses 1/forward_days of total portfolio
    INITIAL_CAPITAL = 1_000_000.0
    trades: list = []
    capital_curve: list = []

    # Group date_pairs by buy_date → pred_date list (for sorting by actual calendar)
    pairs_by_buy: Dict[str, list] = _defaultdict(list)
    for pred_date, buy_date, sell_date in date_pairs:
        pairs_by_buy[buy_date].append((pred_date, sell_date))

    buy_dates_sorted = sorted(pairs_by_buy.keys())
    cash = INITIAL_CAPITAL
    active_positions: list = []  # [{entry_value, sell_date, ret_pct}]

    for buy_date in buy_dates_sorted:
        # 1. Close positions maturing on or before this buy_date
        still_active: list = []
        for pos in active_positions:
            if pos["sell_date"] <= buy_date:
                proceeds = pos["entry_value"] * (1.0 + pos["ret_pct"])
                cash += proceeds
            else:
                still_active.append(pos)
        active_positions = still_active

        # 2. Calculate current portfolio value and per-slot allocation
        locked_value = sum(p["entry_value"] for p in active_positions)
        portfolio_value = cash + locked_value
        slot_size = portfolio_value / forward_days if forward_days > 0 else portfolio_value

        # 3. Open new positions for this buy_date
        for pred_date, sell_date in pairs_by_buy[buy_date]:
            preds = preds_by_date.get(pred_date, [])
            slot_trades: list = []
            is_holding = sell_date is None
            eff_sell_date = sell_date if sell_date is not None else latest_td

            for p in preds:
                code = p["stock_code"]
                ts_code = p["ts_code"]
                stock_name = p["stock_name"]
                rank = p["rank"]

                if is_holding:
                    # ── Holding trade (sell date not yet reached) ──
                    buy_entry = price_map.get((code, buy_date))
                    if not buy_entry:
                        continue
                    if exec_mode == "close":
                        if buy_entry["close"] <= 0:
                            continue
                        buy_price = buy_entry["close"]
                        sell_price_raw = price_map.get((code, latest_td), {}).get("close", buy_entry["close"])
                    else:
                        if buy_entry["open"] <= 0:
                            continue
                        buy_price = buy_entry["open"]
                        sell_price_raw = price_map.get((code, latest_td), {}).get("open", buy_entry["open"])
                    adj_b = adj_map.get((code, buy_date), 1.0)
                    adj_s = adj_map.get((code, latest_td), 1.0)
                    raw_ret = (sell_price_raw - buy_price) / buy_price if buy_price > 0 else 0.0
                    ret = round((1.0 + raw_ret) * (adj_s / adj_b) - 1.0, 6) if adj_s > 0 else 0.0
                    trades.append(LGBBacktestTradeItem(
                        pred_date=pred_date, stock_code=code, ts_code=ts_code,
                        stock_name=stock_name, rank=rank,
                        buy_date=buy_date, buy_price=buy_price,
                        sell_date="", sell_price=sell_price_raw,
                        return_pct=ret, skipped=False,
                    ))
                    continue

                if exec_mode == "close":
                    buy_entry = price_map.get((code, buy_date))
                    if not buy_entry or buy_entry["close"] <= 0:
                        continue

                    # Close mode: check if close was at limit-up
                    limit_pct_c = _get_limit_pct(code)
                    at_limit_up = False
                    prev_bd = _find_nth_trading_day(buy_date, -1, trading_days_sorted)
                    if prev_bd:
                        prev_entry = price_map.get((code, prev_bd))
                        if prev_entry and prev_entry["close"] > 0:
                            limit_up_c = prev_entry["close"] * (1.0 + limit_pct_c)
                            if buy_entry["close"] >= limit_up_c * 0.999:
                                at_limit_up = True
                    # Fallback: use pct_chg when pre_close unavailable (e.g. stock resumed trading after gap)
                    if not at_limit_up and buy_entry.get("pct_chg", 0) >= limit_pct_c * 99.0:
                        at_limit_up = True
                    if at_limit_up:
                                trades.append(LGBBacktestTradeItem(
                                    pred_date=pred_date, stock_code=code, ts_code=ts_code,
                                    stock_name=stock_name, rank=rank,
                                    buy_date=buy_date, buy_price=buy_entry["close"],
                                    sell_date="", sell_price=0.0, return_pct=0.0, skipped=True,
                                ))
                                continue

                    buy_price = buy_entry["close"]

                    # Limit-down sell check: postpone if close is at limit-down
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
                        # Fallback: use pct_chg when pre_close unavailable
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
                else:
                    # ── Open-to-open with limit checks ──
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
                    # Fallback: use pct_chg when pre_close unavailable
                    if not at_limit_up and buy_entry.get("pct_chg", 0) >= limit_pct * 99.0:
                        at_limit_up = True
                    if at_limit_up:
                        trades.append(LGBBacktestTradeItem(
                            pred_date=pred_date, stock_code=code, ts_code=ts_code,
                            stock_name=stock_name, rank=rank,
                            buy_date=buy_date, buy_price=buy_entry["open"],
                            sell_date="", sell_price=0.0, return_pct=0.0, skipped=True,
                        ))
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
                        # Fallback: use pct_chg when pre_close unavailable
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

                adj_b = adj_map.get((code, buy_date), 1.0)
                adj_s = adj_map.get((code, actual_sell_date), 1.0)
                raw_ret = (sell_price - buy_price) / buy_price
                ret = round((1.0 + raw_ret) * (adj_s / adj_b) - 1.0, 6)
                slot_trades.append({
                    "code": code, "ts_code": ts_code, "stock_name": stock_name,
                    "rank": rank, "buy_price": buy_price, "sell_price": sell_price,
                    "ret": ret, "skipped": skipped, "actual_sell_date": actual_sell_date,
                })

            if not slot_trades:
                continue

            n_stocks = len(slot_trades)
            per_stock = slot_size / n_stocks if n_stocks > 0 else 0
            actual_cash_used = min(per_stock * n_stocks, cash)
            if actual_cash_used <= 0:
                continue

            cash -= actual_cash_used
            per_stock_actual = actual_cash_used / n_stocks

            slot_ret = 0.0
            for st in slot_trades:
                slot_ret += st["ret"] / n_stocks  # equal-weight within slot
                trades.append(LGBBacktestTradeItem(
                    pred_date=pred_date, stock_code=st["code"], ts_code=st["ts_code"],
                    stock_name=st["stock_name"], rank=st["rank"],
                    buy_date=buy_date, buy_price=st["buy_price"],
                    sell_date=st["actual_sell_date"], sell_price=st["sell_price"],
                    return_pct=st["ret"], skipped=st["skipped"],
                ))

            active_positions.append({
                "entry_value": actual_cash_used,
                "sell_date": max(st["actual_sell_date"] for st in slot_trades),
                "ret_pct": slot_ret,
            })

        # 4. Record capital curve
        locked_value = sum(p["entry_value"] for p in active_positions)
        portfolio_value = cash + locked_value
        capital_curve.append({
            "date": buy_date,
            "capital": round(portfolio_value / INITIAL_CAPITAL, 6),
            "daily_return": round((portfolio_value / INITIAL_CAPITAL - 1.0)
                                  - (capital_curve[-1]["capital"] - 1.0 if capital_curve else 0), 4),
        })

    # 6. Compute metrics on a per-trade basis (holding trades excluded)
    completed_trades = [t for t in trades if not t.skipped and t.sell_date != ""]
    skipped_count = sum(1 for t in trades if t.skipped)
    holding_count = sum(1 for t in trades if not t.skipped and t.sell_date == "")
    win_count = sum(1 for t in completed_trades if t.return_pct > 0)
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

    metrics = LGBBacktestSimMetrics(
        cumulative_return=round(final_capital - 1.0, 4),
        win_rate=win_rate,
        max_drawdown=round(max_dd, 4),
        total_trades=total_count,
        skipped_trades=skipped_count,
    )

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
    """扫描 lgb_reports/ 返回每个 exec_mode 下实际可用的 forward_days。"""
    project_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    reports_dir = _os.path.join(project_root, "lgb_reports")
    result = {"open": [], "close": []}
    for exec_key, dir_name in [("open", "open2open"), ("close", "close2close")]:
        base = _os.path.join(reports_dir, dir_name)
        if not _os.path.isdir(base):
            continue
        for fwd in [3, 5, 10]:
            fwd_dir = _os.path.join(base, f"fwd{fwd}d")
            if _os.path.isdir(fwd_dir):
                json_count = len(_glob.glob(_os.path.join(fwd_dir, "*.json")))
                if json_count > 0:
                    result[exec_key].append(fwd)
    return result


def _warmup_backtest_cache():
    """Pre-warm backtest-sim cache in background to avoid first-request timeout."""
    import logging
    _log = logging.getLogger(__name__)
    common_combos = [
        (3, 5, "open"),
        (3, 5, "close"),
        (5, 5, "open"),
        (5, 5, "close"),
        (10, 5, "open"),
        (10, 5, "close"),
    ]
    for fwd, tn, em in common_combos:
        key = f"fwd{fwd}_top{tn}_{em}"
        if key not in _backtest_cache:
            try:
                # Internal call — duplicate logic but avoids circular imports
                lgb_backtest_sim(forward_days=fwd, top_n=tn, exec_mode=em)
                _log.info(f"Backtest cache warmed: {key}")
            except Exception:
                _log.warning(f"Backtest cache warmup failed: {key}", exc_info=True)


# Fire-and-forget background warmup after module loads
_t = threading.Thread(target=_warmup_backtest_cache, daemon=True)
_t.start()
