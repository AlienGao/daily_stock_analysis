# -*- coding: utf-8 -*-
"""独立因子回测 API 端点。

简化版因子回测：统一使用开盘价交易，不分 intraday/postmarket 模式。
"""

import logging
import multiprocessing
import os
import re
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


def _update_combo_summary():
    """扫描 combos/ 目录，生成 combo_summary.md。"""
    try:
        combos_dir = _REPORTS_DIR / "combos"
        if not combos_dir.exists():
            return
        combo_files = sorted(combos_dir.glob("backtest_*.md"))
        if not combo_files:
            return

        lines = [
            "# 多因子组合总览", "",
            f"- **组合数量**: {len(combo_files)}",
            f"- **更新时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "",
            "| 组合 | 因子 | 1日 | 3日 | 5日 | 10日 | 20日 |",
            "|------|------|------|------|------|------|------|",
        ]
        rows = []
        for cf in combo_files:
            with open(cf, "r") as fh:
                content = fh.read()
            ft_match = re.search(r'## 因子\n\n\| 因子 \| 权重 \|\n\|------\|------\|\n(.*?)\n\n', content, re.DOTALL)
            factor_labels = []
            if ft_match:
                for row in ft_match.group(1).strip().split("\n"):
                    m = re.match(r'\|\s*(.+?)\s*\|\s*([\d.]+)\s*\|', row)
                    if m:
                        factor_labels.append(f"{m.group(1)}({m.group(2)})")
            returns = {}
            for m in re.finditer(r'\|\s*(\d+)日\s*\|\s*([+-][\d.]+)%', content):
                returns[m.group(1)] = float(m.group(2))
            name = cf.stem.replace("backtest_postmarket_", "")
            ret5 = returns.get("5", -9999)
            rets = [f"{returns.get(str(h), 0):+.2f}%" for h in [1, 3, 5, 10, 20]]
            rows.append((ret5, name, factor_labels, rets))
        rows.sort(key=lambda x: x[0], reverse=True)
        for ret5, name, factor_labels, rets in rows:
            lines.append(f"| {name[:70]} | {', '.join(factor_labels[:4])} | " + " | ".join(rets) + " |")
        lines.append("")
        summary_path = combos_dir / "combo_summary.md"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        logger.info("组合总览已更新: %s", summary_path)
    except Exception:
        logger.exception("更新组合总览失败")


def _save_report(result_dict: dict):
    """将回测汇总保存为 Markdown 到 reports_simple_backtest/ 目录。"""
    try:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        mode = result_dict.get("mode", "postmarket")
        factors = result_dict.get("factors", [])
        factor_parts = [f"w{int(f.get('weight', 0))}_{f.get('name', '')}" for f in factors]
        factor_str = "_".join(factor_parts) if factor_parts else "unknown"
        filename = f"backtest_{mode}_{factor_str}.md"
        is_combo = len(factors) >= 2
        save_dir = _REPORTS_DIR / "combos" if is_combo else _REPORTS_DIR
        save_dir.mkdir(parents=True, exist_ok=True)
        filepath = save_dir / filename

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
            lines.append(f"| {FLM.get(f.get('name', ''), f.get('name', '?'))} | {f.get('weight', 0):.1f} |")
        lines.append(f"")

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
        all_factors = sorted(set(fn for hd_ic in rank_ic.values() for fn in hd_ic)) if rank_ic else []
        if all_factors:
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
        if is_combo:
            _update_combo_summary()
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


# ── Presets: 读取多因子组合快捷配置 ──

@router.get("/presets", summary="获取多因子快捷组合")
def list_presets():
    """扫描 reports_simple_backtest/combos/ 下的多因子组合文件，提取因子权重配置。"""
    presets = []
    combos_dir = _REPORTS_DIR / "combos"
    if combos_dir.exists():
        for f in sorted(combos_dir.glob("backtest_*.md")):
            name = f.stem  # without .md
            # 解析文件名中的 w{weight}_{factor} 对
            parts = name.split("_")
            # 格式: backtest_postmarket_w10_margin_w20_profit_forecast...
            # 跳过 backtest, postmarket 前缀，然后解析 wN_factor 对
            factor_weights = {}
            i = 2  # skip "backtest", "postmarket"
            while i < len(parts):
                if parts[i].startswith("w") and parts[i][1:].isdigit():
                    weight = int(parts[i][1:])
                    # 收集后续非 wN 的部分作为因子名
                    j = i + 1
                    factor_parts = []
                    while j < len(parts) and not (parts[j].startswith("w") and parts[j][1:].isdigit()):
                        factor_parts.append(parts[j])
                        j += 1
                    factor_name = "_".join(factor_parts)
                    if factor_name:
                        factor_weights[factor_name] = weight
                    i = j
                else:
                    i += 1
            if len(factor_weights) >= 2:  # 只算多因子组合
                # 读取 5 日持有期收益用于排序
                ret5 = -9999.0
                try:
                    with open(f, "r") as fh:
                        content = fh.read()
                    m = re.search(r'\|\s*5日\s*\|\s*([+-][\d.]+)%', content)
                    if m:
                        ret5 = float(m.group(1))
                except Exception:
                    pass
                presets.append({"name": name, "factor_weights": factor_weights, "ret5": ret5})
    presets.sort(key=lambda x: x.get("ret5", -9999), reverse=True)
    return {"presets": presets}


@router.post("/cross-validate", summary="多因子组合交叉验证")
def cross_validate():
    """对 presets 中的多因子组合，取最新快照日期，运行各组合选股，找出交叉命中个股。"""
    presets_data = list_presets()
    preset_list = presets_data.get("presets", [])

    # 从 summary_all_factors 取排名第一的单因子加入交叉验证
    top_factor = None
    summary_path = _REPORTS_DIR / "summary_all_factors.md"
    if summary_path.exists():
        with open(summary_path, "r") as fh:
            for line in fh:
                m = re.match(r'\|\s*1\s*\|\s*\S+\s*\|\s*(.+?)\s*\|', line)
                if m:
                    label = m.group(1).strip()
                    for fn, fl in FLM.items():
                        if fl == label:
                            top_factor = fn
                            break
                    break
    if top_factor:
        from src.discovery.factors import __all__ as all_factors
        from src.discovery.factors.base import BaseFactor
        for name in all_factors:
            if name in ("BaseFactor", "DiscoveryResult"):
                continue
            try:
                mod = __import__("src.discovery.factors", fromlist=[name])
                cls = getattr(mod, name)
                if isinstance(cls, type) and issubclass(cls, BaseFactor) and cls is not BaseFactor:
                    inst = cls()
                    if inst.name == top_factor:
                        preset_list.insert(0, {
                            "name": f"top1_{top_factor}",
                            "factor_weights": {top_factor: inst.weight},
                        })
                        break
            except Exception:
                pass

    if not preset_list:
        raise HTTPException(status_code=404, detail="无多因子组合配置")

    try:
        from src.discovery.factor_backtest_engine import FactorBacktestEngine
        engine = FactorBacktestEngine()
        all_factors = list({fn for p in preset_list for fn in p["factor_weights"]})
        snap_dates = engine._get_available_dates(all_factors, "postmarket")
        if not snap_dates:
            raise HTTPException(status_code=404, detail="无可用快照数据")
        latest_date = snap_dates[-1]

        # 加载最新日期的因子得分
        scores_by_date = engine._load_snapshots(all_factors, "postmarket", [latest_date])
        ss = scores_by_date.get(latest_date, {})
        if not ss:
            raise HTTPException(status_code=404, detail=f"快照日期 {latest_date} 无数据")

        # 对每个 preset 计算 top 5
        preset_tops = {}
        for p in preset_list:
            composite = engine._compute_composite(ss, p["factor_weights"])
            if composite.empty:
                continue
            top5 = composite.nlargest(5)
            preset_tops[p["name"]] = [
                {"ts_code": code, "score": round(float(sc), 1)} for code, sc in top5.items()
            ]

        # 交叉命中: 统计每只股票出现在几个 preset 中
        from collections import Counter
        stock_presets: dict = {}
        for preset_name, stocks in preset_tops.items():
            for s in stocks:
                ts = s["ts_code"]
                if ts not in stock_presets:
                    stock_presets[ts] = {"ts_code": ts, "count": 0, "presets": [], "scores": {}}
                stock_presets[ts]["count"] += 1
                stock_presets[ts]["presets"].append(preset_name)
                stock_presets[ts]["scores"][preset_name] = s["score"]

        # 补充股票名称
        engine._prefetch_stock_names([s["ts_code"] for s in stock_presets.values()])

        cross_stocks = sorted(stock_presets.values(), key=lambda x: x["count"], reverse=True)
        for s in cross_stocks:
            s["stock_name"] = engine._stock_names.get(s["ts_code"], s["ts_code"])

        return {
            "latest_date": latest_date,
            "total_presets": len(preset_list),
            "preset_tops": preset_tops,
            "cross_stocks": cross_stocks,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("交叉验证失败")
        raise HTTPException(status_code=500, detail=str(e))


FLM = {
    "money_flow": "资金流向", "margin": "融资融券", "chip": "筹码分布",
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
    "market_cap": "小市值",
}


def _save_summary_md(results: list):
    """生成 summary_all_factors.md。"""
    try:
        filepath = _REPORTS_DIR / "summary_all_factors.md"
        # 读取前一次排名
        prev_rank: dict = {}
        if filepath.exists():
            with open(filepath, "r") as fh:
                for m in re.finditer(r'\|\s*(\d+)\s*\|\s*(.+?)\s*\|', fh.read()):
                    rank = int(m.group(1))
                    label = m.group(2).strip()
                    if label in ("排名", "因子", "------", "变化"):
                        continue
                    fn = None
                    for k, v in FLM.items():
                        if v == label:
                            fn = k
                            break
                    if fn:
                        prev_rank[fn] = rank

        valid = sorted([r for r in results if "total_return" in r], key=lambda x: x["total_return"], reverse=True)
        lines = [
            "# 因子全面测试总结", "",
            f"- **测试因子数**: {len(results)}",
            f"- **有效因子 (累计收益>0)**: {sum(1 for v in valid if v['total_return'] > 0)}",
            f"- **无效因子 (累计收益≤0)**: {sum(1 for v in valid if v['total_return'] <= 0)}",
            f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "",
            "## 综合排名（按 5 日持有期累计收益）", "",
            "| 排名 | 变化 | 因子 | 累计收益 | 年化收益 | 胜率 | 最大回撤 | Sharpe | IC(5日) | 交易数 |",
            "|------|------|------|----------|----------|------|----------|--------|---------|--------|",
        ]
        for rank, v in enumerate(valid, 1):
            prev = prev_rank.get(v["name"])
            if prev is None:
                change = "🆕"
            elif rank < prev:
                change = f"↑{prev - rank}"
            elif rank > prev:
                change = f"↓{rank - prev}"
            else:
                change = "-"
            lines.append(
                f"| {rank} | {change} | {FLM.get(v['name'], v['name'])} "
                f"| {v['total_return'] * 100:+.2f}% | {v['annual_return'] * 100:+.2f}% "
                f"| {v['win_rate'] * 100:.1f}% | {v['max_drawdown'] * 100:.2f}% "
                f"| {v['sharpe']:+.2f} | {v['ic5']:+.4f} | {v['trade_count']} |")
        lines.append("")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        logger.info("总结报告已保存: %s", filepath)
    except Exception:
        logger.exception("保存总结报告失败")


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

        # 获取 postmarket 因子（名称 + 默认权重）
        factor_weights_map: dict = {}
        for name in all_factors:
            if name in ("BaseFactor", "DiscoveryResult"):
                continue
            try:
                mod = __import__("src.discovery.factors", fromlist=[name])
                cls = getattr(mod, name)
                if isinstance(cls, type) and issubclass(cls, BaseFactor) and cls is not BaseFactor:
                    inst = cls()
                    if inst.available_postmarket:
                        factor_weights_map[inst.name] = inst.weight
            except Exception:
                pass
        factors = sorted(factor_weights_map.keys())

        results = []
        for i, fn in enumerate(factors):
            fw = factor_weights_map[fn]
            queue.put(("progress", f"[{i + 1}/{len(factors)}] 测试: {fn}"))
            try:
                r = engine.compute(
                    mode="postmarket",
                    factor_weights={fn: fw},
                    start_date="20250101",
                    top_n=3,
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

        # 生成总结报告
        _save_summary_md(results)

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
