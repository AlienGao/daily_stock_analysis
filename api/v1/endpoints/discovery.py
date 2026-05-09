# -*- coding: utf-8 -*-
"""股票发现 API 端点。

提供盘中扫描 Top N 榜单和盘后发现结果查询。
"""

import json
import logging
import os
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

_SCAN_OUTPUT = "/tmp/discovery_top10.json"


def _get_live_prices(ts_codes: List[str]) -> Dict[str, float]:
    """获取实时价格，从 realtime_spot DB 读取。"""
    try:
        from src.storage import DatabaseManager
        bare_codes = [c.split(".")[0] if "." in c else c for c in ts_codes]
        spot_df = DatabaseManager().get_current_prices(bare_codes)
        if spot_df is not None and not spot_df.empty:
            result: Dict[str, float] = {}
            for ts_code in ts_codes:
                code = ts_code.split(".")[0] if "." in ts_code else ts_code
                try:
                    val = spot_df.at[code, "price"]
                    if pd.notna(val):
                        result[ts_code] = float(val)
                except (KeyError, ValueError, TypeError):
                    pass
            return result
    except Exception:
        logger.warning("[Discovery API] realtime_spot 读取出错", exc_info=True)
    return {}


# ---------------------------------------------------------------------------
# Markdown fallback parser（当 _topn.json 不存在时，直接从 md 解析）
# ---------------------------------------------------------------------------

def _parse_markdown_top_n(md: str) -> list[dict]:
    """从 engine.format_report 输出的 Markdown 中解析 Top N 结构化数据。"""
    import re

    items: list[dict] = []
    # 匹配 "### #排名 代码 名称 — 综合评分 分数"
    title_re = re.compile(
        r'^###\s+#(\d+)\s+([0-9A-Za-z.]+)\s+(.+?)\s+—\s+综合评分\s+([0-9.]+)\s*$',
        re.MULTILINE,
    )
    matches = list(title_re.finditer(md))

    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(md)
        block = md[start:end]

        rank = int(m.group(1))
        stock_code = m.group(2)
        stock_full = m.group(3).strip()
        # 解析名称和行业: "深南电路 · PCB" → stock_name="深南电路", sector="PCB"
        if ' · ' in stock_full:
            stock_name, sector = stock_full.rsplit(' · ', 1)
        else:
            stock_name, sector = stock_full, ''
        score = float(m.group(4))

        # 发现时间和价格: *发现 15:30:00 · ¥51.20*
        discovered_at = ''
        price_at_discovery = None
        dm = re.search(r'\*发现\s+(\d{2}:\d{2}:\d{2})\s+·\s+¥([0-9.]+)\*', block)
        if dm:
            discovered_at = dm.group(1)
            try:
                price_at_discovery = float(dm.group(2))
            except ValueError:
                pass

        # 推荐理由
        reasons = re.findall(r'^- (.+)$', block, re.MULTILINE)

        # 买卖点位表格
        buy_low = buy_high = tp1 = tp2 = sl = None
        tbl_row = re.findall(
            r'^\|\s*([^|\n]+?)\s*\|\s*([^|\n]+?)\s*\|\s*([^|\n]+?)\s*\|\s*([^|\n]+?)\s*\|$',
            block, re.MULTILINE,
        )
        # 取最后一个数据行（跳过表头）
        if len(tbl_row) >= 2:
            cells = [c.strip() for c in tbl_row[-1]]
            if len(cells) >= 4:
                buy_low, buy_high = _parse_price_range(cells[0])
                tp1 = _parse_float(cells[1])
                tp2 = _parse_float(cells[2])
                sl = _parse_float(cells[3])

        # 因子得分
        factor_scores: dict[str, float] = {}
        fm = re.search(r'\*因子得分：([^\n*]+)\*', block)
        if fm:
            for pair in fm.group(1).split('|'):
                parts = [p.strip() for p in pair.split(':')]
                if len(parts) == 2:
                    try:
                        factor_scores[parts[0]] = float(parts[1])
                    except ValueError:
                        pass

        items.append({
            "rank": rank,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "score": score,
            "sector": sector,
            "reasons": reasons,
            "discovered_at": discovered_at,
            "price_at_discovery": price_at_discovery,
            "buy_price_low": buy_low,
            "buy_price_high": buy_high,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "stop_loss": sl,
            "factor_scores": factor_scores,
        })

    return items


def _parse_price_range(value: str):
    """解析"10.50-12.30"或"10.50"格式的买入区间。"""
    nums = __import__('re').findall(r'\d+(?:\.\d+)?', value)
    if not nums:
        return None, None
    if len(nums) == 1:
        n = _parse_float(nums[0])
        return n, n
    return _parse_float(nums[0]), _parse_float(nums[1])


def _parse_float(v: str):
    try:
        return float(v.strip())
    except (ValueError, AttributeError):
        return None


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
    discovered_at: str = ""
    price_at_discovery: Optional[float] = None
    live_price: Optional[float] = None
    factor_weights: dict = {}


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
# Async postmarket task tracker
# ---------------------------------------------------------------------------

_postmarket_tasks: Dict[str, dict] = {}


def _get_latest_completed_task() -> Optional[dict]:
    """获取最近一个已完成且有报告的盘后任务（用于非交易日文件不存在时回退）。"""
    completed = [t for t in _postmarket_tasks.values()
                 if t.get("status") == "completed" and t.get("report")]
    if not completed:
        return None
    return max(completed, key=lambda t: t.get("finished_at", ""))


def _build_discovery_items(raw_items: list, mode: str = "") -> List[DiscoveryItem]:
    """将原始 dict 列表转为 DiscoveryItem 列表。"""
    items: List[DiscoveryItem] = []
    fallback_weights = _get_factor_weights(mode) if mode else {}
    for entry in raw_items:
        items.append(DiscoveryItem(
            rank=entry.get("rank", 0),
            ts_code=entry.get("ts_code", ""),
            stock_code=entry.get("stock_code", ""),
            stock_name=entry.get("stock_name", ""),
            score=entry.get("score", 0),
            sector=entry.get("sector", ""),
            factor_scores=entry.get("factor_scores", {}),
            factor_weights=entry.get("factor_weights") or fallback_weights,
            reasons=entry.get("reasons", []),
            buy_price_low=entry.get("buy_price_low"),
            buy_price_high=entry.get("buy_price_high"),
            stop_loss=entry.get("stop_loss"),
            take_profit_1=entry.get("take_profit_1"),
            take_profit_2=entry.get("take_profit_2"),
            discovered_at=entry.get("discovered_at", ""),
            price_at_discovery=entry.get("price_at_discovery"),
        ))
    return items


class RunStatusResponse(BaseModel):
    task_id: str
    status: str  # "running" | "completed" | "failed"
    error: str = ""
    top_n_count: int = 0


# ---------------------------------------------------------------------------
# Intraday Top 10 (from scanner JSON)
# ---------------------------------------------------------------------------

@router.get(
    "/intraday/top10",
    response_model=IntradayTopResponse,
    summary="获取盘中扫描 Top 10",
)
def get_intraday_top10():
    """返回盘中扫描器最新一轮结果（从 /tmp/discovery_top10.json 读取），并刷新实时价格。"""
    if not os.path.exists(_SCAN_OUTPUT):
        return IntradayTopResponse(mode="intraday")

    try:
        with open(_SCAN_OUTPUT, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 每次请求刷新实时价格，避免展示扫描时刻的陈旧价格
        live_prices: Dict[str, float] = {}
        ts_codes = [e.get("ts_code", "") for e in data.get("top_n", []) if e.get("ts_code")]
        if ts_codes:
            live_prices = _get_live_prices(ts_codes)

        top_n = []
        for entry in data.get("top_n", []):
            ts_code = entry.get("ts_code", "")
            live_price = live_prices.get(ts_code) or entry.get("price_at_discovery")
            tp1 = entry.get("take_profit_1")
            stop = entry.get("stop_loss")

            # 用实时价格重新过滤，避免展示已失效的标的
            if live_price and tp1 and live_price >= tp1:
                continue  # 现价已超过止盈目标
            if live_price and tp1 and stop:
                if live_price <= stop:
                    continue  # 现价已跌破止损线
                pnl = (tp1 - live_price) / (live_price - stop)
                if pnl <= 0:
                    continue  # 盈亏比非正

            top_n.append(DiscoveryItem(
                rank=entry.get("rank", 0),
                ts_code=ts_code,
                stock_code=entry.get("stock_code", ""),
                stock_name=entry.get("stock_name", ""),
                score=entry.get("score", 0),
                sector=entry.get("sector", ""),
                factor_scores=entry.get("factor_scores", {}),
                reasons=entry.get("reasons", []),
                buy_price_low=entry.get("buy_price_low"),
                buy_price_high=entry.get("buy_price_high"),
                stop_loss=stop,
                take_profit_1=tp1,
                take_profit_2=entry.get("take_profit_2"),
                change=entry.get("change", ""),
                discovered_at=entry.get("discovered_at", ""),
                price_at_discovery=entry.get("price_at_discovery"),
                live_price=live_prices.get(ts_code) if live_prices.get(ts_code) != entry.get("price_at_discovery") else None,
                factor_weights=entry.get("factor_weights") or _get_factor_weights("intraday"),
            ))
        # Re-rank after filtering
        for i, item in enumerate(top_n):
            item.rank = i + 1

        dropped = []
        for entry in data.get("dropped", []):
            dropped.append(DiscoveryItem(
                rank=-1,
                stock_code=entry.get("stock_code", ""),
                stock_name=entry.get("stock_name", ""),
                score=0,
                change="out",
                factor_weights={},
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
# Scan mode (per-mode: intraday / postmarket)
# ---------------------------------------------------------------------------

class ScanModeResponse(BaseModel):
    scan_universe: str  # full_market / whitelist / broker_gold
    has_whitelist: bool


@router.get(
    "/scan-mode",
    response_model=ScanModeResponse,
    summary="获取扫描范围（盘中/盘后独立）",
)
def get_scan_mode(mode: str = Query("intraday", description="intraday 或 postmarket")):
    from src.discovery.config import _ensure_active_config
    cfg = _ensure_active_config()
    universe = cfg.intraday_scan_universe if mode == "intraday" else cfg.postmarket_scan_universe
    return ScanModeResponse(
        scan_universe=universe,
        has_whitelist=bool(cfg.discover_whitelist),
    )


@router.post(
    "/scan-mode",
    response_model=ScanModeResponse,
    summary="切换扫描范围（盘中/盘后独立）",
)
def set_scan_mode(
    scan_universe: str = Query(..., description="full_market / whitelist / broker_gold"),
    mode: str = Query("intraday", description="intraday 或 postmarket"),
):
    from src.discovery.config import _ensure_active_config, save_runtime_state
    cfg = _ensure_active_config()
    if mode == "intraday":
        cfg.intraday_scan_universe = scan_universe
    else:
        cfg.postmarket_scan_universe = scan_universe
    save_runtime_state()
    result_universe = cfg.intraday_scan_universe if mode == "intraday" else cfg.postmarket_scan_universe
    return ScanModeResponse(
        scan_universe=result_universe,
        has_whitelist=bool(cfg.discover_whitelist),
    )


# ---------------------------------------------------------------------------
# Whitelist management
# ---------------------------------------------------------------------------

class WhitelistResponse(BaseModel):
    codes: list
    count: int


@router.get(
    "/whitelist",
    response_model=WhitelistResponse,
    summary="获取扫描白名单",
)
def get_whitelist():
    from src.discovery.config import get_effective_whitelist
    codes = get_effective_whitelist()
    return WhitelistResponse(codes=codes, count=len(codes))


class UpdateWhitelistRequest(BaseModel):
    codes: List[str] = []


@router.put(
    "/whitelist",
    response_model=WhitelistResponse,
    summary="更新扫描白名单（立即生效）",
)
def update_whitelist(body: UpdateWhitelistRequest):
    from src.discovery.config import set_whitelist, get_effective_whitelist
    set_whitelist(body.codes)
    updated = get_effective_whitelist()
    return WhitelistResponse(codes=updated, count=len(updated))


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

    from datetime import timedelta
    reports_dir = Path(__file__).resolve().parent.parent.parent.parent / "discovery_reports"
    effective_date = report_date

    # 按优先级查找报告文件：交易日目录 → 前一天 → non_trading/ 目录 → 内存缓存
    def _find_report(candidate_date: str) -> tuple:
        """Search for report md file. Returns (filepath, effective_dir, effective_date) or (None, None, None)."""
        # Check main reports_dir
        fp = reports_dir / f"postmarket_{candidate_date}.md"
        if fp.exists():
            return (fp, reports_dir, candidate_date)
        # Check non_trading subdirectory
        fp = reports_dir / "non_trading" / f"postmarket_{candidate_date}.md"
        if fp.exists():
            return (fp, reports_dir / "non_trading", candidate_date)
        return (None, None, None)

    filepath, found_dir, effective_date = _find_report(report_date)
    if filepath is None:
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        filepath, found_dir, effective_date = _find_report(yesterday)
    if filepath is None:
        # 最后尝试内存中的最近完成任务
        recent = _get_latest_completed_task()
        if recent and recent.get("report"):
            top_n = _build_discovery_items(recent.get("top_n", []), mode="postmarket")
            return PostmarketReportResponse(
                date=recent.get("date_str", report_date),
                report=recent["report"],
                exists=True,
                top_n=top_n,
            )
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    try:
        report = filepath.read_text(encoding="utf-8")

        # 优先加载结构化 Top N JSON，不存在则从 markdown 解析
        top_n: List[DiscoveryItem] = []
        topn_file = found_dir / f"postmarket_{effective_date}_topn.json"
        raw_items: list[dict] = []
        if topn_file.exists():
            try:
                raw_items = json.loads(topn_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.debug("解析盘后 Top N JSON 失败: %s", e)
        if not raw_items:
            raw_items = _parse_markdown_top_n(report)

        for entry in raw_items:
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
                discovered_at=entry.get("discovered_at", ""),
                price_at_discovery=entry.get("price_at_discovery"),
                factor_weights=entry.get("factor_weights") or _get_factor_weights("postmarket"),
            ))

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
    summary="手动触发盘后发现（异步）",
)
def run_postmarket_discovery():
    """启动后台盘后股票发现任务，返回 task_id 用于轮询状态。"""
    import uuid

    task_id = str(uuid.uuid4())[:8]
    _postmarket_tasks[task_id] = {
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "top_n_count": 0,
    }

    def _run():
        try:
            from src.discovery.config import get_active_config, set_active_config, get_discovery_config
            from src.discovery.engine import StockDiscoveryEngine
            from src.discovery.factors import (
                MoneyFlowFactor, MarginFactor, ChipFactor,
                TechnicalFactor, LimitFactor,
                FundamentalFactor, PopularityFactor, HotMoneyFactor,
                NorthboundFactor, InstitutionHoldFactor, ProfitForecastFactor,
                PerformanceFactor, BuybackFactor, InsiderBuyFactor,
                BrokerRecommendFactor,
            )
            from data_provider.tushare_fetcher import TushareFetcher
            from data_provider.akshare_fetcher import AkshareFetcher

            discovery_config = get_active_config() or get_discovery_config()
            set_active_config(discovery_config)
            tushare_fetcher = TushareFetcher.get_instance()
            akshare_fetcher = AkshareFetcher()
            if not tushare_fetcher.is_available():
                _postmarket_tasks[task_id] = {"status": "failed", "error": "数据源 Tushare 不可用"}
                return

            engine = StockDiscoveryEngine(discovery_config, tushare_fetcher, akshare_fetcher)
            engine.register_factors([
                MoneyFlowFactor(),
                MarginFactor(),
                ChipFactor(),
                TechnicalFactor(),
                LimitFactor(),
                FundamentalFactor(),
                PopularityFactor(),
                HotMoneyFactor(),
                NorthboundFactor(),
                InstitutionHoldFactor(),
                ProfitForecastFactor(),
                PerformanceFactor(),
                BuybackFactor(),
                InsiderBuyFactor(),
                BrokerRecommendFactor(),
            ])

            results = engine.discover(mode="postmarket")
            if not results:
                _postmarket_tasks[task_id] = {
                    "status": "completed",
                    "top_n_count": 0,
                    "finished_at": datetime.now().isoformat(),
                }
                return

            report = engine.format_report(results, mode="postmarket")

            # 构建 top_n_data（始终在内存中保存，供无文件时报告端点使用）
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
                    "discovered_at": r.discovered_at,
                    "price_at_discovery": r.price_at_discovery,
                })

            # 保存报告 + 结构化数据到 discovery_reports
            # 交易日 → 直接保存（供回测使用）；非交易日 → non_trading/ 子目录（仅展示，不回测）
            from src.discovery.engine import is_trading_day
            date_str = date.today().strftime('%Y%m%d')
            base_dir = Path(__file__).resolve().parent.parent.parent.parent / "discovery_reports"
            if is_trading_day(engine):
                reports_dir = base_dir
            else:
                reports_dir = base_dir / "non_trading"
            reports_dir.mkdir(parents=True, exist_ok=True)
            filename = f"postmarket_{date_str}.md"
            (reports_dir / filename).write_text(report, encoding="utf-8")
            json_file = reports_dir / f"postmarket_{date_str}_topn.json"
            json_file.write_text(json.dumps(top_n_data, ensure_ascii=False, indent=2), encoding="utf-8")

            # 全市场扫描结果落库（供查分功能使用）
            if engine._last_full_scan_df is not None:
                try:
                    from src.storage import DatabaseManager
                    records = engine.get_last_full_scan_records()
                    if records:
                        DatabaseManager().save_scan_results_postmarket(
                            records, engine._last_scan_trade_date
                        )
                except Exception as e_save:
                    logger.warning("全量扫描结果落库失败: %s", e_save)

            _postmarket_tasks[task_id] = {
                "status": "completed",
                "top_n_count": len(top_n_data),
                "finished_at": datetime.now().isoformat(),
                "report": report,
                "top_n": top_n_data,
                "date_str": date_str,
            }
        except Exception as e:
            logger.error("手动盘后发现失败: %s", e, exc_info=True)
            _postmarket_tasks[task_id] = {"status": "failed", "error": str(e)}

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id, "status": "running"}


@router.get(
    "/postmarket/run/status",
    response_model=RunStatusResponse,
    summary="查询盘后发现任务状态",
)
def get_postmarket_run_status(task_id: str = Query(..., description="任务 ID")):
    """轮询后台盘后发现任务的执行状态。"""
    task = _postmarket_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")
    return RunStatusResponse(
        task_id=task_id,
        status=task.get("status", "unknown"),
        error=task.get("error", ""),
        top_n_count=task.get("top_n_count", 0),
    )


# ---------------------------------------------------------------------------
# Backtest
# ---------------------------------------------------------------------------

class TradeRecordItem(BaseModel):
    stock_code: str
    stock_name: str
    buy_date: str
    buy_price: float
    sell_date: str
    sell_price: float
    return_pct: float
    pnl: float
    allocated_capital: float


class BacktestDailyItem(BaseModel):
    trade_date: str
    avg_return: float
    cumulative_return: float
    capital: float
    win_count: int
    total_count: int


class CapitalCurvePoint(BaseModel):
    date: str
    capital: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None


class BacktestResponse(BaseModel):
    mode: str
    initial_capital: float
    final_capital: float
    cumulative_return: float
    total_pnl: float
    win_rate: float
    total_days: int
    total_trades: int
    daily_results: List[BacktestDailyItem] = []
    trade_records: List[TradeRecordItem] = []
    capital_curve: List[CapitalCurvePoint] = []


@router.get(
    "/backtest",
    response_model=BacktestResponse,
    summary="获取发现引擎回测结果",
)
def get_backtest(
    mode: str = Query("intraday", description="回测模式: intraday | postmarket"),
    days: int = Query(60, description="回看天数（自然日），start_date 未指定时使用"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD"),
):
    """返回盘中或盘后发现策略的回测累计收益、资金曲线、交易记录。"""
    from src.discovery.backtest import DiscoveryBacktest
    from data_provider.tushare_fetcher import TushareFetcher

    if mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 仅支持 intraday 或 postmarket")

    try:
        fetcher = TushareFetcher.get_instance()
    except Exception:
        fetcher = None

    try:
        bt = DiscoveryBacktest(tushare_fetcher=fetcher)
        summary = bt.compute(
            mode=mode,
            lookback_days=days,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as e:
        logger.error("回测计算失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"回测计算失败: {str(e)}")

    if summary is None:
        return BacktestResponse(mode=mode)

    daily = [
        BacktestDailyItem(
            trade_date=dr.trade_date,
            avg_return=round(dr.avg_return, 6),
            cumulative_return=round(dr.cumulative_return, 6),
            capital=dr.capital,
            win_count=dr.win_count,
            total_count=dr.total_count,
        )
        for dr in summary.daily_results
    ]

    trades = [
        TradeRecordItem(
            stock_code=t.stock_code,
            stock_name=t.stock_name,
            buy_date=t.buy_date,
            buy_price=t.buy_price,
            sell_date=t.sell_date,
            sell_price=t.sell_price,
            return_pct=t.return_pct,
            pnl=t.pnl,
            allocated_capital=t.allocated_capital,
        )
        for t in summary.trade_records
    ]

    curve = [
        CapitalCurvePoint(
            date=p["date"],
            capital=p["capital"],
            open=p.get("open"),
            high=p.get("high"),
            low=p.get("low"),
            close=p.get("close"),
        )
        for p in summary.capital_curve
    ]

    return BacktestResponse(
        mode=summary.mode,
        initial_capital=summary.initial_capital,
        final_capital=summary.final_capital,
        cumulative_return=round(summary.cumulative_return, 6),
        total_pnl=summary.total_pnl,
        win_rate=round(summary.win_rate, 4),
        total_days=summary.total_days,
        total_trades=summary.total_trades,
        daily_results=daily,
        trade_records=trades,
        capital_curve=curve,
    )


# ------------------------------------------------------------------
# Stock Score Lookup
# ------------------------------------------------------------------

class StockScoreItem(BaseModel):
    """单次扫描中的个股评分。"""
    scanned_at: str               # "2026-05-09 09:35:00"
    rank: int
    total_score: float
    factor_scores: Dict[str, float]
    factor_weights: Dict[str, float] = {}
    sector: str


class StockScoreEntry(BaseModel):
    """单只股票的盘中最新的评分（含盘中+盘后两条）。"""
    stock_code: str
    stock_name: str
    intraday: Optional[StockScoreItem] = None
    postmarket: Optional[StockScoreItem] = None


class StockScoreResponse(BaseModel):
    """多股评分查询响应。"""
    items: List[StockScoreEntry]


def _format_scanned_at(scan_date: str, scan_time: str) -> str:
    """将 YYYYMMDD + HHMMSS（或 HH:MM:SS）→ ISO 时间字符串。"""
    if not scan_date:
        return ""
    d = f"{scan_date[:4]}-{scan_date[4:6]}-{scan_date[6:8]}"
    if scan_time:
        digits = "".join(c for c in scan_time if c.isdigit())
        if len(digits) >= 6:
            return f"{d} {digits[:2]}:{digits[2:4]}:{digits[4:6]}"
    return d


def _row_to_item(row, factor_weights: Dict[str, float] = None) -> Optional[StockScoreItem]:
    """将 ORM 行转为 StockScoreItem。"""
    if row is None:
        return None
    factor_scores: Dict[str, float] = {}
    try:
        raw = json.loads(row.factor_scores_json or "{}")
        factor_scores = {k: float(v) for k, v in raw.items()}
    except (json.JSONDecodeError, TypeError):
        pass
    return StockScoreItem(
        scanned_at=_format_scanned_at(row.scan_date, row.scan_time),
        rank=row.rank,
        total_score=round(float(row.total_score or 0), 2),
        factor_scores=factor_scores,
        factor_weights=factor_weights or {},
        sector=row.sector or "",
    )


def _get_factor_weights(mode: str) -> Dict[str, float]:
    """获取指定模式下所有活跃因子的权重映射。"""
    from src.discovery.config import get_discovery_config
    from src.discovery.engine import StockDiscoveryEngine
    from src.discovery.factors import (
        SectorFactor, MaEntryFactor, MomentumFactor, ReboundFactor,
        MoneyFlowFactor, MarginFactor, ChipFactor, TechnicalFactor,
        LimitFactor, FundamentalFactor, PopularityFactor, HotMoneyFactor,
        NorthboundFactor, InstitutionHoldFactor, ProfitForecastFactor,
        PerformanceFactor, BuybackFactor, InsiderBuyFactor,
        BrokerRecommendFactor,
    )
    config = get_discovery_config()
    engine = StockDiscoveryEngine(config)
    engine.register_factors([
        SectorFactor(), MaEntryFactor(), MomentumFactor(), ReboundFactor(),
        MoneyFlowFactor(), MarginFactor(), ChipFactor(), TechnicalFactor(),
        LimitFactor(), FundamentalFactor(), PopularityFactor(), HotMoneyFactor(),
        NorthboundFactor(), InstitutionHoldFactor(), ProfitForecastFactor(),
        PerformanceFactor(), BuybackFactor(), InsiderBuyFactor(),
        BrokerRecommendFactor(),
    ])
    return {
        name: factor.weight
        for name, factor in engine._factors.items()
        if factor.is_available(mode)
    }


@router.get(
    "/stock-score",
    response_model=StockScoreResponse,
    summary="查询股票最新评分",
)
def get_stock_score(
    codes: str = Query(..., description="股票代码，逗号分隔，如 600519,000001"),
    mode: str = Query("intraday", description="intraday 或 postmarket"),
):
    """返回每只股票的最新评分（盘中/盘后隔离，含因子明细和时间戳）。"""
    from src.storage import DatabaseManager, ScanResultIntraday, ScanResultPostmarket

    if mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")

    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if not code_list:
        raise HTTPException(status_code=400, detail="请提供至少一个股票代码")

    db = DatabaseManager()
    items: List[StockScoreEntry] = []
    Model = ScanResultIntraday if mode == "intraday" else ScanResultPostmarket
    factor_weights = _get_factor_weights(mode)

    with db.get_session() as session:
        from sqlalchemy import desc

        for code in code_list:
            row = (
                session.query(Model)
                .filter(Model.stock_code == code)
                .order_by(desc(Model.scan_date), desc(Model.scan_time))
                .first()
            )

            stock_name = (row and row.stock_name) or code
            score_item = _row_to_item(row, factor_weights=factor_weights)

            items.append(StockScoreEntry(
                stock_code=code,
                stock_name=stock_name,
                intraday=score_item if mode == "intraday" else None,
                postmarket=score_item if mode == "postmarket" else None,
            ))

    return StockScoreResponse(items=items)
