# -*- coding: utf-8 -*-
"""股票发现 API 端点。

提供盘中扫描 Top N 榜单和盘后发现结果查询。
"""

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

_SCAN_OUTPUT = "/tmp/discovery_top10.json"


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class FactorScores(BaseModel):
    money_flow: float = 0.0
    margin: float = 0.0
    chip: float = 0.0
    technical: float = 0.0
    limit: float = 0.0


class DiscoveryItem(BaseModel):
    rank: int
    ts_code: str = ""
    stock_code: str
    stock_name: str
    score: float
    sector: str = ""
    factor_scores: dict = {}
    reasons: List[str] = []
    buy_price_low: Optional[float] = None
    buy_price_high: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    take_profit_2: Optional[float] = None
    change: str = ""


class IntradayTopResponse(BaseModel):
    updated: Optional[str] = None
    round: int = 0
    mode: str = "intraday"
    top_n: List[DiscoveryItem] = []
    dropped: List[DiscoveryItem] = []


class PostmarketReportResponse(BaseModel):
    date: str
    report: str
    exists: bool
    top_n: List[DiscoveryItem] = []


# ---------------------------------------------------------------------------
# Intraday Top 10 (from scanner JSON)
# ---------------------------------------------------------------------------

@router.get(
    "/intraday/top10",
    response_model=IntradayTopResponse,
    summary="获取盘中扫描 Top 10",
)
def get_intraday_top10():
    """返回盘中扫描器最新一轮结果（从 /tmp/discovery_top10.json 读取）。"""
    if not os.path.exists(_SCAN_OUTPUT):
        return IntradayTopResponse(mode="intraday")

    try:
        with open(_SCAN_OUTPUT, "r", encoding="utf-8") as f:
            data = json.load(f)

        top_n = []
        for entry in data.get("top_n", []):
            top_n.append(DiscoveryItem(
                rank=entry.get("rank", 0),
                ts_code=entry.get("ts_code", ""),
                stock_code=entry.get("stock_code", ""),
                stock_name=entry.get("stock_name", ""),
                score=entry.get("score", 0),
                sector=entry.get("sector", ""),
                factor_scores=entry.get("factor_scores", {}),
                reasons=entry.get("reasons", []),
                buy_price_low=entry.get("buy_price_low"),
                buy_price_high=entry.get("buy_price_high"),
                stop_loss=entry.get("stop_loss"),
                take_profit_1=entry.get("take_profit_1"),
                take_profit_2=entry.get("take_profit_2"),
                change=entry.get("change", ""),
            ))

        dropped = []
        for entry in data.get("dropped", []):
            dropped.append(DiscoveryItem(
                rank=-1,
                stock_code=entry.get("stock_code", ""),
                stock_name=entry.get("stock_name", ""),
                score=0,
                change="out",
            ))

        return IntradayTopResponse(
            updated=data.get("updated"),
            round=data.get("round", 0),
            mode="intraday",
            top_n=top_n,
            dropped=dropped,
        )
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.warning("解析盘中扫描结果失败: %s", e)
        return IntradayTopResponse(mode="intraday")


# ---------------------------------------------------------------------------
# Post-market report (from reports/
# ---------------------------------------------------------------------------

@router.get(
    "/postmarket/report",
    response_model=PostmarketReportResponse,
    summary="获取盘后发现报告",
)
def get_postmarket_report(
    report_date: Optional[str] = Query(None, description="日期 YYYYMMDD，默认今天"),
):
    """返回盘后发现 Markdown 报告内容。"""
    if report_date is None:
        report_date = date.today().strftime("%Y%m%d")

    reports_dir = Path(__file__).resolve().parent.parent.parent.parent / "reports"
    filepath = reports_dir / f"discovery_{report_date}.md"
    effective_date = report_date

    if not filepath.exists():
        # 尝试前一天
        from datetime import timedelta
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        filepath = reports_dir / f"discovery_{yesterday}.md"
        effective_date = yesterday
        if not filepath.exists():
            return PostmarketReportResponse(date=report_date, report="", exists=False)

    try:
        report = filepath.read_text(encoding="utf-8")

        # 尝试加载结构化 Top N
        top_n: List[DiscoveryItem] = []
        topn_file = reports_dir / f"discovery_{effective_date}_topn.json"
        if topn_file.exists():
            try:
                topn_data = json.loads(topn_file.read_text(encoding="utf-8"))
                for entry in topn_data:
                    top_n.append(DiscoveryItem(
                        rank=entry.get("rank", 0),
                        ts_code=entry.get("ts_code", ""),
                        stock_code=entry.get("stock_code", ""),
                        stock_name=entry.get("stock_name", ""),
                        score=entry.get("score", 0),
                        sector=entry.get("sector", ""),
                        factor_scores=entry.get("factor_scores", {}),
                        reasons=entry.get("reasons", []),
                        buy_price_low=entry.get("buy_price_low"),
                        buy_price_high=entry.get("buy_price_high"),
                        stop_loss=entry.get("stop_loss"),
                        take_profit_1=entry.get("take_profit_1"),
                        take_profit_2=entry.get("take_profit_2"),
                    ))
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.debug("解析盘后 Top N JSON 失败: %s", e)

        return PostmarketReportResponse(
            date=effective_date, report=report, exists=True, top_n=top_n,
        )
    except Exception as e:
        logger.warning("读取盘后报告失败: %s", e)
        return PostmarketReportResponse(date=report_date, report="", exists=False)


# ---------------------------------------------------------------------------
# Run post-market discovery on demand
# ---------------------------------------------------------------------------

@router.post(
    "/postmarket/run",
    summary="手动触发盘后发现",
)
def run_postmarket_discovery():
    """手动触发一次盘后股票发现，返回 Top 10 结果。"""
    try:
        from src.discovery.config import get_discovery_config
        from src.discovery.engine import StockDiscoveryEngine
        from src.discovery.factors import (
            MoneyFlowFactor, MarginFactor, ChipFactor,
            TechnicalFactor, LimitFactor,
        )
        from data_provider.tushare_fetcher import TushareFetcher

        discovery_config = get_discovery_config()
        tushare_fetcher = TushareFetcher()
        if not tushare_fetcher.is_available():
            raise HTTPException(status_code=503, detail="数据源 Tushare 不可用")

        engine = StockDiscoveryEngine(discovery_config, tushare_fetcher)
        engine.register_factors([
            MoneyFlowFactor(),
            MarginFactor(),
            ChipFactor(),
            TechnicalFactor(),
            LimitFactor(),
        ])

        results = engine.discover(mode="postmarket")
        if not results:
            return {"mode": "postmarket", "top_n": [], "message": "未发现符合条件的股票"}

        report = engine.format_report(results, mode="postmarket")

        # 保存报告 + 结构化数据
        reports_dir = Path(__file__).resolve().parent.parent.parent.parent / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        date_str = date.today().strftime('%Y%m%d')
        filename = f"discovery_{date_str}.md"
        (reports_dir / filename).write_text(report, encoding="utf-8")

        # 保存结构化 Top N 供前端卡片渲染
        top_n_data = []
        for i, r in enumerate(results, 1):
            top_n_data.append({
                "rank": i,
                "ts_code": r.ts_code,
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "score": r.score,
                "sector": r.sector,
                "factor_scores": r.factor_scores,
                "reasons": r.reasons,
                "buy_price_low": r.buy_price_low,
                "buy_price_high": r.buy_price_high,
                "stop_loss": r.stop_loss,
                "take_profit_1": r.take_profit_1,
                "take_profit_2": r.take_profit_2,
            })
        json_file = reports_dir / f"discovery_{date_str}_topn.json"
        json_file.write_text(json.dumps(top_n_data, ensure_ascii=False, indent=2), encoding="utf-8")

        # 同步到 .env STOCK_LIST
        try:
            from src.services.system_config_service import SystemConfigService
            codes = [r.stock_code for r in results]
            SystemConfigService().apply_simple_updates([("STOCK_LIST", ",".join(codes))])
        except Exception:
            pass

        items = []
        for i, r in enumerate(results, 1):
            items.append(DiscoveryItem(
                rank=i,
                ts_code=r.ts_code,
                stock_code=r.stock_code,
                stock_name=r.stock_name,
                score=r.score,
                sector=r.sector,
                factor_scores=r.factor_scores,
                reasons=r.reasons,
                buy_price_low=r.buy_price_low,
                buy_price_high=r.buy_price_high,
                stop_loss=r.stop_loss,
                take_profit_1=r.take_profit_1,
                take_profit_2=r.take_profit_2,
            ))

        return {"mode": "postmarket", "top_n": items, "report_preview": report[:500]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("手动盘后发现失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"盘后发现失败: {str(e)}")
