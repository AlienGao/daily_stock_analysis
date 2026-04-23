#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在不重跑整批单 Agent 分析的前提下，从数据库恢复某日分析结果，再跑 Top-N 多 Agent 复核。

典型用法（项目根目录）:
  python scripts/run_top_n_replay.py
  python scripts/run_top_n_replay.py --date 2026-04-23
  python scripts/run_top_n_replay.py --query-id <uuid>   # 若你明确知道共享 query_id 时

默认（未带 --query-id）:
  取「该自然日 created_at 范围内」的 AnalysisHistory，按股票代码去重、每代码只保留
  当天**最新**一条。当前 pipeline 批跑为每只股票生成独立 query_id，因此**不能**
  依赖「同一 query_id 一批」来还原整表。
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Any, List, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sqlalchemy import and_, desc, func, select

from src.config import get_config, setup_env
from src.core.pipeline import StockAnalysisPipeline
from src.logging_config import setup_logging
from src.services.history_service import HistoryService
from src.services.top_n_reviewer import run_top_n_multi_review
from src.storage import AnalysisHistory, get_db
from src.utils.data_processing import parse_json_field

logger = logging.getLogger(__name__)


def _parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _query_id_counts_for_date(d: date) -> List[tuple]:
    start = datetime.combine(d, datetime.min.time())
    end = datetime.combine(d + timedelta(days=1), datetime.min.time())
    db = get_db()
    with db.get_session() as session:
        q = (
            select(AnalysisHistory.query_id, func.count().label("cnt"))
            .where(
                and_(
                    AnalysisHistory.created_at >= start,
                    AnalysisHistory.created_at < end,
                    AnalysisHistory.query_id.isnot(None),
                )
            )
            .group_by(AnalysisHistory.query_id)
            .order_by(func.count().desc())
        )
        return list(session.execute(q).all())


def _load_results_for_query(query_id: str, *, limit: int = 2000) -> List[Any]:
    db = get_db()
    with db.get_session() as session:
        q = (
            select(AnalysisHistory)
            .where(AnalysisHistory.query_id == query_id)
            .order_by(AnalysisHistory.code)
        )
        rows = list(session.execute(q).scalars().all())
    if len(rows) >= limit:
        logger.warning("记录数 %d 达到/超过 limit=%d，可能不完整", len(rows), limit)
    return rows


def _load_latest_per_code_for_date(
    d: date,
    *,
    cap: int = 5000,
    query_source: Optional[str] = None,
) -> List[Any]:
    """同一自然日内，每只股票只保留 created_at 最新的一条（批跑为每股独立 query_id 时的基线方式）。"""
    start = datetime.combine(d, datetime.min.time())
    end = datetime.combine(d + timedelta(days=1), datetime.min.time())
    db = get_db()
    conds = [
        AnalysisHistory.created_at >= start,
        AnalysisHistory.created_at < end,
    ]
    if query_source:
        conds.append(AnalysisHistory.query_source == query_source)
    with db.get_session() as session:
        q = (
            select(AnalysisHistory)
            .where(and_(*conds))
            .order_by(desc(AnalysisHistory.created_at))
            .limit(cap)
        )
        rows = list(session.execute(q).scalars().all())
    by_code: dict = {}
    for r in rows:
        c = getattr(r, "code", None) or ""
        if c and c not in by_code:
            by_code[c] = r
    if len(rows) >= cap:
        logger.warning("当日记录达到 cap=%d 条，去重后 %d 只；若数量不对请增大 cap", cap, len(by_code))
    return sorted(by_code.values(), key=lambda x: (x.code or ""))


def _records_to_results(records) -> list:
    hs = HistoryService()
    out = []
    for rec in records:
        raw = parse_json_field(getattr(rec, "raw_result", None))
        if not raw:
            logger.warning("跳过 %s：无 raw_result", rec.code)
            continue
        ar = hs._rebuild_analysis_result(raw, rec)
        if not ar:
            logger.warning("跳过 %s：无法还原 AnalysisResult", rec.code)
            continue
        ar.query_id = getattr(rec, "query_id", None)
        out.append(ar)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="对已有批跑结果重跑 Top-N 多 Agent 复核")
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="自然日 (YYYY-MM-DD)，默认今天",
    )
    p.add_argument("--query-id", type=str, default=None, help="若已知当日批次 query_id，可显式指定")
    p.add_argument(
        "--query-source",
        type=str,
        default="cli",
        metavar="SRC",
        help="与按日去重联用：只取该 query_source，批跑/定时走 main 为 cli。传 all 则不过滤来源",
    )
    p.add_argument(
        "--update-report",
        action="store_true",
        help="复核结束后按合并结果写 reports/report_YYYYMMDD.md（日报名与主流程一致）",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="只解析数据库并打印将参与 Top-N 的代码，不调用 multi",
    )
    p.add_argument("--workers", type=int, default=None, help="Pipeline 并发线程数，默认用配置")
    p.add_argument(
        "--list-batches",
        action="store_true",
        help="仅列出该日各 query_id 的条数后退出（调试用；批跑目前多为每只股票独立 query_id）",
    )
    args = p.parse_args()

    setup_env()
    config = get_config()
    setup_logging(log_prefix="top_n_replay", debug=False, log_dir=config.log_dir)

    d = _parse_date(args.date) if args.date else date.today()
    batch_rows = _query_id_counts_for_date(d)
    if args.list_batches:
        print(f"日期 {d} 各 query_id 条数（降序）:")
        for q, cnt in batch_rows:
            print(f"  {cnt:5d}  {q}")
        if not batch_rows:
            print("  （无带 query_id 的记录；整批可能未写入 query_id，需从主流程另找依据）")
        return 0

    qid = (args.query_id or "").strip() or None
    if qid:
        records = _load_results_for_query(qid)
        if not records:
            logger.error("query_id=%s 无历史记录", qid)
            return 1
        logger.info("使用 --query-id=%s，共 %d 条 (日期 %s)", qid, len(records), d)
    else:
        qsrc = (args.query_source or "cli").strip()
        if qsrc.lower() == "all":
            src = None
        else:
            src = qsrc or None
        records = _load_latest_per_code_for_date(d, query_source=src)
        if not records:
            logger.error("日期 %s 无分析历史记录，确认已跑过批分析且数据已落库", d)
            return 1
        logger.info("按自然日去重: %s 共 %d 只股票（每代码取当日最新一条）", d, len(records))
    base_results = _records_to_results(records)
    if not base_results:
        logger.error("没有可用的 AnalysisResult，退出")
        return 1
    if len(base_results) != len(records):
        logger.warning("有效结果 %d 条，原始记录 %d 条", len(base_results), len(records))

    if not getattr(config, "top_n_multi_agent_review_enabled", False):
        logger.warning("TOP_N_MULTI_AGENT_REVIEW_ENABLED 未开启；脚本仍会根据当前配置跑 multi。请在 .env 中启用或确认模型可用。")

    n = max(1, int(getattr(config, "top_n_multi_agent_review_count", 10) or 10))
    from src.services.top_n_reviewer import select_top_n_by_sentiment

    top = select_top_n_by_sentiment(base_results, n)
    logger.info("Top-N=%d 将复核（按 sentiment 排序）: %s", n, [x.code for x in top])

    if args.dry_run:
        print("[dry-run] 不进行 multi 调用")
        return 0

    new_qid = uuid.uuid4().hex
    pipeline = StockAnalysisPipeline(
        config=config,
        max_workers=args.workers,
        query_id=new_qid,
        query_source="top_n_replay",
    )
    try:
        merged, stats = run_top_n_multi_review(base_results, config, pipeline)
        logger.info("[Top-N multi] 完成: %s", stats)
    except Exception:
        logger.exception("Top-N multi 失败")
        return 1

    if args.update_report and merged:
        rt = pipeline.get_config_report_type()
        try:
            pipeline._save_local_report(merged, rt)
            logger.info("主日报已按合并结果更新")
        except Exception:
            logger.exception("写入主日报失败")

    return 0


if __name__ == "__main__":
    sys.exit(main())
