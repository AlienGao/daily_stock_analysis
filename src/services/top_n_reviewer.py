# -*- coding: utf-8 -*-
"""
Top-N sentiment stocks → multi-agent review, merge into batch results, appendix file, optional DB replace.
"""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.analyzer import AnalysisResult
from src.config import Config
from src.core.pipeline import StockAnalysisPipeline
from src.enums import ReportType
from src.utils.rating_category import operation_advice_to_category

logger = logging.getLogger(__name__)


def select_top_n_by_sentiment(results: List[AnalysisResult], n: int) -> List[AnalysisResult]:
    if not results or n <= 0:
        return []
    ranked = sorted(
        results,
        key=lambda r: (r.sentiment_score is not None, r.sentiment_score or 0),
        reverse=True,
    )
    return ranked[: min(n, len(ranked))]


def _build_multi_config(base: Config) -> Config:
    return replace(
        base,
        agent_arch="multi",
        agent_orchestrator_mode=base.top_n_multi_agent_review_orchestrator_mode,
    )


def _tz_cn_now() -> datetime:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz)


def should_run_top_n_review(config: Config, args: Any) -> bool:
    if not getattr(config, "top_n_multi_agent_review_enabled", False):
        return False
    sched = getattr(config, "top_n_multi_agent_review_schedule", "close") or "close"
    now = _tz_cn_now()
    hour = now.hour
    is_close = hour >= 14
    is_open = hour < 13
    if sched == "both":
        return True
    if sched == "close":
        return is_close
    if sched == "open":
        return is_open
    return is_close


def _review_one(
    pipeline: StockAnalysisPipeline,
    single: AnalysisResult,
    report_type: ReportType,
    multi_cfg: Config,
) -> Tuple[str, Optional[AnalysisResult], str]:
    """
    Returns (code, merged_result_or_none, status).
    merged_result is the AnalysisResult to use for that code (multi if category changed o/w None to keep single).
    status: ok_same | ok_replaced | err_multi | err_run
    """
    code = single.code
    qid = getattr(single, "query_id", None) or uuid.uuid4().hex
    try:
        multi = pipeline.analyze_stock(
            code,
            report_type,
            qid,
            agent_exec_config=multi_cfg,
            force_agent=True,
            replace_history=False,
            persist_history=False,
        )
    except Exception as exc:
        logger.exception("[%s] Top-N multi 复核异常", code)
        return (code, None, f"err_run:{exc}")

    if not multi or not getattr(multi, "success", True):
        msg = (multi and multi.error_message) or "multi failed"
        logger.warning("[%s] Top-N multi 未成功: %s", code, msg)
        return (code, None, f"err_multi:{msg}")

    cat_s = operation_advice_to_category(single.operation_advice, None)
    cat_m = operation_advice_to_category(multi.operation_advice, None)
    if cat_s == cat_m:
        logger.info("[%s] Top-N multi 与 single 四类一致，保留 single 结论", code)
        return (code, None, "ok_same")

    try:
        if pipeline.db.delete_analysis_history_by_query_and_code(qid, code):
            logger.info("[%s] Top-N multi 四类不同，已删旧历史 query_id=%s", code, qid)
        initial_context: Dict[str, Any] = {
            "stock_code": code,
            "stock_name": multi.name,
            "report_type": report_type.value,
        }
        pipeline.db.save_analysis_history(
            result=multi,
            query_id=qid,
            report_type=report_type.value,
            news_content=None,
            context_snapshot=initial_context,
            save_snapshot=pipeline.save_context_snapshot,
        )
    except Exception as exc:
        logger.warning("[%s] 写入 multi 分析历史失败: %s", code, exc)

    multi.query_id = qid
    return (code, multi, "ok_replaced")


def merge_results_list(
    base_results: List[AnalysisResult],
    replacement: Dict[str, AnalysisResult],
) -> List[AnalysisResult]:
    out: List[AnalysisResult] = []
    for r in base_results:
        rep = replacement.get(r.code)
        if rep is not None:
            out.append(rep)
        else:
            out.append(r)
    return out


def _write_appendix_markdown(
    config: Config,
    top: List[AnalysisResult],
    replacement: Dict[str, AnalysisResult],
    stats: Dict[str, Any],
) -> str:
    out_dir = getattr(config, "top_n_multi_agent_review_output_dir", "reports_multi_agent") or "reports_multi_agent"
    import os

    os.makedirs(out_dir, exist_ok=True)
    d = _tz_cn_now().strftime("%Y%m%d")
    path = os.path.join(out_dir, f"report_multi_{d}.md")
    lines: List[str] = [
        f"# Top {len(top)} 多 Agent 复核（{d}）",
        "",
        f"- 成功替换（四类不同）: {', '.join(stats.get('replaced', [])) or '无'}",
        f"- 未改（四类一致或失败）: 见下表",
        "",
        "| 代码 | 原评分 | 原建议 | 新评分 | 新建议 | 状态 |",
        "|---|---:|---|---:|---|---|",
    ]
    for s in top:
        r = replacement.get(s.code)
        if r is not None:
            st = "replaced"
            lines.append(
                f"| {s.code} | {s.sentiment_score} | {s.operation_advice} | "
                f"{r.sentiment_score} | {r.operation_advice} | {st} |"
            )
        else:
            st = "keep"
            lines.append(
                f"| {s.code} | {s.sentiment_score} | {s.operation_advice} | — | — | {st} |"
            )
    for code, err in (stats.get("errors") or {}).items():
        lines.append(f"- 错误 {code}: {err}")
    body = "\n".join(lines) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    logger.info("Top-N 多 Agent 附录已写入 %s", path)
    return path


def run_top_n_multi_review(
    base_results: List[AnalysisResult],
    config: Config,
    pipeline: StockAnalysisPipeline,
) -> Tuple[List[AnalysisResult], Dict[str, Any]]:
    n = max(1, int(getattr(config, "top_n_multi_agent_review_count", 10) or 10))
    top = select_top_n_by_sentiment(base_results, n)
    if not top:
        return (base_results, {"skipped": True, "reason": "no_results"})

    report_type = pipeline.get_config_report_type()
    multi_cfg = _build_multi_config(config)
    workers = max(1, int(getattr(config, "top_n_multi_agent_review_concurrency", 3) or 3))

    replacement: Dict[str, AnalysisResult] = {}
    stats: Dict[str, Any] = {
        "replaced": [],
        "unchanged": [],
        "errors": {},
    }

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_review_one, pipeline, s, report_type, multi_cfg) for s in top]
        for fut in as_completed(futs):
            code, multi_res, status = fut.result()
            if status.startswith("err_"):
                stats["errors"][code] = status
                continue
            if status == "ok_replaced" and multi_res is not None:
                replacement[code] = multi_res
                stats["replaced"].append(code)
            else:
                stats["unchanged"].append(code)

    merged = merge_results_list(base_results, replacement)
    try:
        _write_appendix_markdown(config, top, replacement, {**stats, "errors": stats["errors"]})
    except Exception as exc:
        logger.warning("附录 Markdown 写入失败: %s", exc)

    return (merged, {**stats, "merged": True, "n": n})
