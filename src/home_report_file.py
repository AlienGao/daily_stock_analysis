# -*- coding: utf-8 -*-
"""
首页 / API 交互式单股分析：将 Markdown 快照写入可配置根目录下按日分子目录、按股票代码覆盖。

路径：{root}/YYYY-MM-DD/{code}.md
"""

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.config import get_config
from src.analyzer import AnalysisResult
from src.notification import NotificationService

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def write_home_interactive_analysis_markdown(result: AnalysisResult) -> None:
    """
    为 query_source 为 api/web 的成功单股分析写入/覆盖当天 Markdown 文件。
    失败不阻断主流程（仅打日志）。
    """
    if result is None or not getattr(result, "success", True):
        return
    code = getattr(result, "code", None) or ""
    if not code.strip():
        return
    try:
        cfg = get_config()
        rel = (getattr(cfg, "home_analysis_reports_dir", None) or "reports_temp").strip()
        d = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        out_dir = (_PROJECT_ROOT / rel / d.isoformat())
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{code}.md"
        text = NotificationService().generate_single_stock_report(result)
        out_path.write_text(text, encoding="utf-8")
        logger.info("已写入首页单股报告 %s", out_path)
    except Exception as exc:
        logger.warning("写入首页单股报告文件失败: %s", exc, exc_info=True)
