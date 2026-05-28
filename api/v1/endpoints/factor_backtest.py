# -*- coding: utf-8 -*-
"""独立因子回测 API 端点。

简化版因子回测：统一使用开盘价交易，不分 intraday/postmarket 模式。
"""

import logging
import multiprocessing
import os
import uuid
from datetime import datetime
from pathlib import Path
from queue import Empty as QueueEmpty
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# api/v1/endpoints → api/v1 → api → project root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_REPORTS_DIR = _PROJECT_ROOT / "reports_simple_backtest"


def _compute_period_stats(curve, trades, init_cap, rfr):
    """从资金曲线和交易记录计算单个持有期的汇总指标。"""
    closed = [t for t in trades if t.get("status") in ("closed", "extended")]
    if not curve or len(curve) < 2:
        return None
    final_cap = curve[-1]["capital"]
    total_ret = (final_cap - init_cap) / init_cap
    n_periods = len(curve) - 1
    ann_ret = (1 + total_ret) ** (252 / max(n_periods, 1)) - 1 if total_ret > -1 else total_ret
    wins = sum(1 for t in closed if t.get("return_pct", 0) > 0)
    win_rate = wins / len(closed) if closed else 0
    peak = init_cap
    mdd = 0.0
    for pt in curve:
        if pt["capital"] > peak:
            peak = pt["capital"]
        dd = (peak - pt["capital"]) / peak if peak > 0 else 0
        if dd > mdd:
            mdd = dd
    dr = []
    for i in range(1, len(curve)):
        dr.append((curve[i]["capital"] - curve[i - 1]["capital"]) / curve[i - 1]["capital"])
    mean_ret = sum(dr) / len(dr) if dr else 0
    std_ret = (sum((r - mean_ret) ** 2 for r in dr) / (len(dr) - 1)) ** 0.5 if len(dr) > 1 else 0
    daily_rf = (1 + rfr) ** (1 / 252) - 1
    sharpe = (mean_ret - daily_rf) / std_ret * (252 ** 0.5) if std_ret > 0 else 0
    return {
        "total_return": total_ret,
        "annual_return": ann_ret,
        "win_rate": win_rate,
        "max_drawdown": mdd,
        "sharpe": sharpe,
        "trade_count": len(closed),
        "open_count": sum(1 for t in trades if t.get("status") == "open"),
        "canceled_count": sum(1 for t in trades if t.get("status") == "canceled"),
    }


def _save_report(result_dict: dict):
    """将回测汇总保存为 Markdown 到 reports_simple_backtest/ 目录。"""
    try:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        mode = result_dict.get("mode", "postmarket")
        factors = result_dict.get("factors", [])
        factor_names = [f.get("name", "") for f in factors]
        factor_str = "_".join(factor_names) if factor_names else "unknown"
        filename = f"backtest_{mode}_{factor_str}.md"
        filepath = _REPORTS_DIR / filename

        params = result_dict.get("params", {})
        hold_days = params.get("hold_days", [])
        init_cap = params.get("initial_capital", 1_000_000)
        rfr = params.get("risk_free_rate", 0.02)
        date_range = result_dict.get("date_range", {})
        curves = result_dict.get("capital_curves", {})
        all_trades = result_dict.get("trade_records", [])
        rank_ic = result_dict.get("rank_ic", {})

        lines = []
        lines.append(f"# 因子回测报告")
        lines.append(f"")
        lines.append(f"- **模式**: {mode}")
        lines.append(f"- **回测区间**: {date_range.get('start', '?')} ~ {date_range.get('end', '?')}")
        lines.append(f"- **初始资金**: {init_cap:,.0f}")
        lines.append(f"- **无风险利率**: {rfr * 100:.1f}%")
        lines.append(f"- **每期选股数**: {params.get('top_n', '-')}")
        lines.append(f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"")
        lines.append(f"## 因子")
        lines.append(f"")
        lines.append(f"| 因子 | 权重 |")
        lines.append(f"|------|------|")
        for f in factors:
            lines.append(f"| {f.get('name', '?')} | {f.get('weight', 0):.1f} |")
        lines.append(f"")

        # 因子标签映射
        FLM = {"money_flow": "资金流向", "margin": "融资融券", "chip": "筹码分布",
               "technical": "技术形态", "limit": "涨跌停", "fundamental": "基本面",
               "institution_hold": "机构持股", "profit_forecast": "盈利预测",
               "buyback": "回购", "insider_buy": "高管增持", "broker_recommend": "券商推荐",
               "popularity": "人气", "hot_money": "游资", "performance": "业绩",
               "momentum": "动量", "rebound": "反弹", "sector": "板块", "ma_entry": "均线",
               "ranking_momentum": "排名动量", "concept_heat": "概念热度",
               "alpha042": "均值回归Alpha042", "vwap_deviation": "VWAP偏离",
               "gap_reversal": "跳空反转", "liquid_oversold": "流动性超卖",
               "vwap_reversal": "VWAP动量反转", "gtja114": "GTJA114",
               "alpha60": "Alpha60收盘位置", "money_flow_osc": "资金流振荡",
               "market_cap": "小市值"}

        lines.append(f"## 各持有期汇总")
        lines.append(f"")
        header = "| 持有期 | 总收益 | 年化收益 | 胜率 | 最大回撤 | Sharpe | 交易次数 | 持仓中 | 跳过 |"
        lines.append(header)
        lines.append("|--------|--------|----------|------|----------|--------|----------|--------|------|")
        for hd in hold_days:
            hd_str = str(hd)
            curve = curves.get(hd_str, [])
            trades_hd = [t for t in all_trades if t.get("hold_days") == hd]
            stats = _compute_period_stats(curve, trades_hd, init_cap, rfr)
            if stats:
                lines.append(
                    f"| {hd}日 | {stats['total_return'] * 100:+.2f}% | {stats['annual_return'] * 100:+.2f}% "
                    f"| {stats['win_rate'] * 100:.1f}% | {stats['max_drawdown'] * 100:.2f}% "
                    f"| {stats['sharpe']:+.2f} | {stats['trade_count']} "
                    f"| {stats['open_count']} | {stats['canceled_count']} |")
            else:
                lines.append(f"| {hd}日 | - | - | - | - | - | - | - | - |")

        lines.append(f"")
        lines.append(f"## Rank IC（因子有效性）")
        lines.append(f"")
        if rank_ic:
            all_factors = sorted(set(fn for hd_ic in rank_ic.values() for fn in hd_ic))
            header_ic = "| 因子 | " + " | ".join(f"{h}日" for h in hold_days) + " |"
            lines.append(header_ic)
            lines.append("|------|" + "|".join("------" for _ in hold_days) + "|")
            for fn in all_factors:
                label = FLM.get(fn, fn)
                vals = []
                for hd in hold_days:
                    ic = rank_ic.get(str(hd), {}).get(fn, 0)
                    vals.append(f"{ic:+.4f}")
                lines.append(f"| {label} | " + " | ".join(vals) + " |")
        else:
            lines.append("无 IC 数据")

        lines.append("")

        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        logger.info("回测报告已保存: %s", filepath)
    except Exception:
        logger.exception("保存回测报告失败")


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
            result_dict = asdict(result)
            _save_report(result_dict)
            queue.put(("completed", result_dict))
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


# ── Batch test: 逐一测试所有因子 ──

def _run_batch_test(queue: multiprocessing.Queue):
    """在独立进程中逐一测试所有因子，生成总结报告。"""
    import numpy as np
    try:
        from data_provider.tushare_fetcher import TushareFetcher
        from src.discovery.factor_backtest_engine import FactorBacktestEngine
        from src.discovery.factors import __all__ as all_factors
        from src.discovery.factors.base import BaseFactor
        from dataclasses import asdict
        from datetime import datetime as dt

        fetcher = TushareFetcher.get_instance()
        engine = FactorBacktestEngine(fetcher)

        # 获取 postmarket 因子
        factors = []
        for name in all_factors:
            if name in ("BaseFactor", "DiscoveryResult"):
                continue
            try:
                mod = __import__("src.discovery.factors", fromlist=[name])
                cls = getattr(mod, name)
                if isinstance(cls, type) and issubclass(cls, BaseFactor) and cls is not BaseFactor:
                    inst = cls()
                    if inst.available_postmarket:
                        factors.append(inst.name)
            except Exception:
                pass
        factors = sorted(factors)

        results = []
        for i, fn in enumerate(factors):
            queue.put(("progress", f"[{i + 1}/{len(factors)}] 测试: {fn}"))
            try:
                r = engine.compute(
                    mode="postmarket",
                    factor_weights={fn: 1.0},
                    top_n=5,
                    hold_days=[1, 3, 5, 10, 20],
                    initial_capital=5_000_000,
                    risk_free_rate=0.02,
                )
            except Exception as e:
                results.append({"name": fn, "error": str(e)})
                continue

            if r is None:
                results.append({"name": fn, "error": "数据不足"})
                continue

            rd = asdict(r)
            _save_report(rd)

            # 提取 5 日持有期指标
            curves = rd.get("capital_curves", {})
            trades = rd.get("trade_records", [])
            ic5 = rd.get("rank_ic", {}).get("5", {}).get(fn, 0)
            stats = _compute_period_stats(
                curves.get("5", []),
                [t for t in trades if t.get("hold_days") == 5],
                5_000_000, 0.02,
            )
            if stats:
                results.append({
                    "name": fn,
                    "total_return": round(stats["total_return"], 4),
                    "annual_return": round(stats["annual_return"], 4),
                    "win_rate": round(stats["win_rate"], 4),
                    "max_drawdown": round(stats["max_drawdown"], 4),
                    "sharpe": round(stats["sharpe"], 4),
                    "trade_count": stats["trade_count"],
                    "ic5": round(ic5, 4),
                    "date_range": rd.get("date_range", {}),
                })
            else:
                results.append({"name": fn, "error": "统计不足"})

        queue.put(("completed", {
            "factors_tested": len(factors),
            "results": sorted(results, key=lambda x: x.get("total_return", -999), reverse=True),
        }))
    except Exception as e:
        import traceback
        traceback.print_exc()
        queue.put(("failed", str(e)))


@router.post("/batch-test", summary="逐一测试所有因子")
def start_batch_test():
    """逐一测试所有 postmarket 因子，生成单因子报告和总结。"""
    _cleanup_old()
    for tid, t in _tasks.items():
        if t.get("status") == "running":
            raise HTTPException(status_code=429, detail="已有任务在运行，请等待完成")

    task_id = uuid.uuid4().hex[:8]
    _tasks[task_id] = {"status": "running", "started_at": datetime.now().isoformat()}

    queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=_run_batch_test, args=(queue,), daemon=True)
    proc.start()

    import threading
    t = threading.Thread(target=_monitor, args=(task_id, queue, proc), daemon=True)
    t.start()

    return {"task_id": task_id, "status": "running"}
