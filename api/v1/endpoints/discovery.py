# -*- coding: utf-8 -*-
"""股票发现 API 端点。

提供盘中扫描 Top N 榜单和盘后发现结果查询。
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

import fastapi
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

_SCAN_OUTPUT = "/tmp/discovery_top10.json"
_INTRADAY_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "discovery_reports"


def _postmarket_stream_path(date_str: str = "") -> Path:
    """盘后 TopN JSON 路径（每日一个文件）。"""
    if not date_str:
        date_str = date.today().strftime("%Y%m%d")
    return _INTRADAY_REPORTS_DIR / f"postmarket_{date_str}_topn.json"


def _is_trading_hours() -> bool:
    """当前是否在 A 股交易时段（工作日 9:30-15:00）。"""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    minute_of_day = now.hour * 60 + now.minute
    return (9 * 60 + 30) <= minute_of_day <= (15 * 60)


# ---------------------------------------------------------------------------
# Live rescore cache & engine reuse
# ---------------------------------------------------------------------------

_live_rescore_cache: Dict[str, dict] = {}  # {date_str: {"ts": float, "items": list}}
_cached_engine = None
_cached_engine_ts = 0


def _get_or_create_engine():
    """复用 engine 实例（5 分钟内），避免重复注册因子。"""
    global _cached_engine, _cached_engine_ts
    now = time.time()
    if _cached_engine is not None and now - _cached_engine_ts < 300:
        return _cached_engine
    from src.discovery.config import get_active_config, set_active_config, get_discovery_config
    from src.discovery.engine import create_discovery_engine
    from data_provider.tushare_fetcher import TushareFetcher
    from data_provider.akshare_fetcher import AkshareFetcher

    config = get_active_config() or get_discovery_config()
    set_active_config(config)
    tushare_fetcher = TushareFetcher.get_instance()
    akshare_fetcher = AkshareFetcher()
    _cached_engine = create_discovery_engine(config, tushare_fetcher, akshare_fetcher)
    _cached_engine_ts = now
    return _cached_engine


def _get_live_quotes(ts_codes: List[str]) -> "tuple[Dict[str, float], Dict[str, float]]":
    """获取实时价格和涨跌幅。

    交易时段优先从 realtime_spot DB 读取（盘中扫描器每 30s 刷新），
    非交易时段（盘后/非交易日）回退到 Sina 实时行情接口补充。

    Returns: (prices_dict, pct_chg_dict)，key 为裸码（与 DB 一致）。
    """
    try:
        from src.storage import DatabaseManager
        bare_codes = [c.split(".")[0] if "." in c else c for c in ts_codes]
        spot_df = DatabaseManager().get_current_prices(bare_codes)
        if spot_df is not None and not spot_df.empty:
            prices: Dict[str, float] = {}
            pct_chgs: Dict[str, float] = {}
            for code in bare_codes:
                try:
                    p = spot_df.at[code, "price"]
                    if pd.notna(p):
                        prices[code] = float(p)
                    pct = spot_df.at[code, "pct_chg"]
                    if pd.notna(pct):
                        pct_chgs[code] = float(pct)
                except (KeyError, ValueError, TypeError):
                    pass
            if prices:
                return prices, pct_chgs
    except Exception:
        logger.warning("[Discovery API] realtime_spot 读取出错", exc_info=True)

    # 非交易时段：Sina 实时行情兜底
    prices, pct_chgs = _get_live_quotes_sina(bare_codes)
    return prices, pct_chgs


def _get_live_prices(ts_codes: List[str]) -> Dict[str, float]:
    """获取实时价格，从 realtime_spot DB 读取。"""
    prices, _ = _get_live_quotes(ts_codes)
    return prices


def _get_live_quotes_sina(bare_codes: List[str]) -> "tuple[Dict[str, float], Dict[str, float]]":
    """通过 Sina 实时行情接口获取价格和涨跌幅（非交易时段兜底）。

    Returns: (prices_dict, pct_chg_dict)，key 为 6 位裸码。
    """
    if not bare_codes:
        return {}, {}
    try:
        sina_symbols: List[str] = []
        for c in bare_codes:
            if not c.isdigit() or len(c) != 6:
                continue
            prefix = "sh" if c.startswith(("6", "68")) else "sz" if c.startswith(("0", "3")) else "bj"
            sina_symbols.append(f"{prefix}{c}")

        url = f"http://hq.sinajs.cn/list={','.join(sina_symbols)}"
        resp = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=10)
        resp.encoding = "gbk"
    except Exception as e:
        logger.debug("[Discovery API] Sina 实时行情请求失败: %s", e)
        return {}, {}

    prices: Dict[str, float] = {}
    pct_chgs: Dict[str, float] = {}
    for sc in sina_symbols:
        try:
            m = re.search(rf'var hq_str_{sc}="([^"]*)"', resp.text)
            if not m:
                continue
            fields = m.group(1).split(",")
            if len(fields) < 6:
                continue
            code = sc[2:]
            close_p = float(fields[3]) if fields[3] and fields[3] != "0.000" else None
            if close_p is not None:
                prices[code] = close_p
            pre_close = float(fields[2]) if len(fields) > 2 and fields[2] and fields[2] != "0.000" else None
            if close_p is not None and pre_close is not None and pre_close > 0:
                pct_chgs[code] = round((close_p - pre_close) / pre_close * 100, 2)
        except Exception:
            pass
    return prices, pct_chgs


def _get_tech_score_weights() -> Dict[str, float]:
    """从 DiscoveryConfig 读取 StockScorer 各维度权重（归一化为百分比）。

    盘中/盘后共用同一套 StockScorerConfig，返回 {维度名: 百分比}。
    """
    try:
        from src.discovery.config import get_active_config, get_discovery_config
        cfg = get_active_config() or get_discovery_config()
        weights = {
            "rr_score": cfg.scorer_weight_rr,
            "market_score": cfg.scorer_weight_market,
            "sector_score": cfg.scorer_weight_sector,
            "volume_score": cfg.scorer_weight_volume,
            "position_score": cfg.scorer_weight_position,
            "formation_score": cfg.scorer_weight_formation,
        }
        return {k: round(v * 100, 1) for k, v in weights.items()}
    except Exception:
        return {
            "rr_score": 30.0,
            "market_score": 20.0,
            "sector_score": 15.0,
            "volume_score": 15.0,
            "position_score": 10.0,
            "formation_score": 10.0,
        }


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
    pct_chg: Optional[float] = None
    factor_weights: dict = {}
    # StockScorer 多维技术评分
    tech_score: float = 0.0
    rr_score: float = 0.0
    market_score: float = 0.0
    sector_score: float = 0.0
    volume_score: float = 0.0
    position_score: float = 0.0
    formation_score: float = 0.0
    tech_score_weights: dict = {}   # {维度名: 权重}，由 API 动态注入
    composite_score: float = 0.0


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
    live_rescored: bool = False


def _enrich_live_quotes(items: List[DiscoveryItem]) -> None:
    """用 realtime_spot 的实时价格和涨跌幅覆盖列表中的对应字段。

    非交易时段通过 Sina 实时行情兜底，确保盘后页面也能展示实时价格。
    ts_code 为空时用 stock_code 6位裸码兜底。
    """
    ts_codes = [item.ts_code for item in items if item.ts_code]
    bare_codes = [item.stock_code for item in items if not item.ts_code and item.stock_code]
    all_codes = ts_codes + bare_codes
    if not all_codes:
        return
    live_prices, live_pct_chgs = _get_live_quotes(all_codes)
    for item in items:
        code = item.ts_code or item.stock_code
        if not code:
            continue
        lp = live_prices.get(code)
        if lp is not None:
            item.live_price = lp
        pct = live_pct_chgs.get(code)
        if pct is not None:
            item.pct_chg = pct


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
            pct_chg=entry.get("pct_chg"),
            tech_score=entry.get("tech_score", 0.0),
            rr_score=entry.get("rr_score", 0.0),
            market_score=entry.get("market_score", 0.0),
            sector_score=entry.get("sector_score", 0.0),
            volume_score=entry.get("volume_score", 0.0),
            position_score=entry.get("position_score", 0.0),
            formation_score=entry.get("formation_score", 0.0),
            tech_score_weights=_get_tech_score_weights(),
            composite_score=entry.get("composite_score", 0.0),
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
                pct_chg=entry.get("pct_chg"),
                factor_weights=entry.get("factor_weights") or _get_factor_weights("intraday"),
                tech_score=entry.get("tech_score", 0.0),
                rr_score=entry.get("rr_score", 0.0),
                market_score=entry.get("market_score", 0.0),
                sector_score=entry.get("sector_score", 0.0),
                volume_score=entry.get("volume_score", 0.0),
                position_score=entry.get("position_score", 0.0),
                formation_score=entry.get("formation_score", 0.0),
                tech_score_weights=_get_tech_score_weights(),
                composite_score=entry.get("composite_score", 0.0),
            ))
        # 按综合分重新排序，只返回前 5
        top_n.sort(key=lambda x: x.composite_score, reverse=True)
        top_n = top_n[:5]
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
                pct_chg=entry.get("pct_chg"),
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
# Intraday SSE stream — pushes update events when scanner writes new data
# ---------------------------------------------------------------------------

@router.get(
    "/intraday/stream",
    responses={
        200: {"description": "SSE 事件流", "content": {"text/event-stream": {}}},
    },
    summary="盘中扫描实时推送",
    description="通过 Server-Sent Events 推送盘中扫描更新事件，前端收到后拉取最新数据",
)
async def intraday_stream():
    """SSE 端点：监听 /tmp/discovery_top10.json 的 mtime 变化，推送 update 事件。"""

    async def event_generator():
        last_mtime = None
        ticks_since_last_event = 0
        try:
            while True:
                try:
                    await asyncio.sleep(2)
                    ticks_since_last_event += 1

                    try:
                        current_mtime = os.path.getmtime(_SCAN_OUTPUT)
                    except OSError:
                        current_mtime = None

                    if current_mtime is not None and current_mtime != last_mtime:
                        last_mtime = current_mtime
                        ticks_since_last_event = 0
                        yield f"event: update\ndata: {{}}\n\n"
                    elif ticks_since_last_event >= 15:
                        ticks_since_last_event = 0
                        yield f"event: heartbeat\ndata: {{}}\n\n"
                except asyncio.CancelledError:
                    raise
        except asyncio.CancelledError:
            logger.debug("SSE client disconnected from intraday stream")
            raise

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Postmarket SSE stream — 交易时段（9:30-15:00）推送盘后榜单更新
# 非交易时段 SSE keep-alive，不主动推送（盘后数据不会更新）
# ---------------------------------------------------------------------------

@router.get(
    "/postmarket/stream",
    responses={
        200: {"description": "SSE 事件流", "content": {"text/event-stream": {}}},
    },
    summary="盘后扫描实时推送（仅交易时段有效）",
    description="通过 SSE 推送盘后榜单更新事件；非交易时段仅保持连接，不推送数据",
)
async def postmarket_stream():
    """SSE 端点：监听盘后 TopN JSON 的 mtime 变化推送 update 事件。

    推送时机：
    - 交易时段（9:30-15:00）：检查文件 mtime 变化，变化时推送
    - 非交易时段：仅保持连接，每 30s 发 heartbeat
    """

    async def event_generator():
        last_mtime: Optional[float] = None
        ticks_since_last_event = 0
        ticks_since_last_rescore = 0
        try:
            while True:
                await asyncio.sleep(2)
                ticks_since_last_event += 1
                ticks_since_last_rescore += 1

                if _is_trading_hours():
                    today_str = date.today().strftime("%Y%m%d")
                    mpath = _postmarket_stream_path(today_str)
                    try:
                        current_mtime = os.path.getmtime(mpath)
                    except OSError:
                        current_mtime = None

                    if current_mtime is not None and current_mtime != last_mtime:
                        last_mtime = current_mtime
                        ticks_since_last_event = 0
                        yield "event: update\ndata: {}\n\n"

                    # 交易时段每 30s 推送 rescore 事件
                    if ticks_since_last_rescore >= 15:
                        ticks_since_last_rescore = 0
                        yield "event: rescore\ndata: {}\n\n"

                    if ticks_since_last_event >= 15:
                        ticks_since_last_event = 0
                        yield "event: heartbeat\ndata: {}\n\n"
                else:
                    ticks_since_last_rescore = 0
                    if ticks_since_last_event >= 15:
                        ticks_since_last_event = 0
                        yield "event: heartbeat\ndata: {}\n\n"

        except asyncio.CancelledError:
            logger.debug("SSE client disconnected from postmarket stream")
            raise

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
            _enrich_live_quotes(top_n)
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
                pct_chg=entry.get("pct_chg"),
                factor_weights=entry.get("factor_weights") or _get_factor_weights("postmarket"),
                tech_score=entry.get("tech_score", 0.0),
                rr_score=entry.get("rr_score", 0.0),
                market_score=entry.get("market_score", 0.0),
                sector_score=entry.get("sector_score", 0.0),
                volume_score=entry.get("volume_score", 0.0),
                position_score=entry.get("position_score", 0.0),
                formation_score=entry.get("formation_score", 0.0),
                tech_score_weights=_get_tech_score_weights(),
                composite_score=entry.get("composite_score", 0.0),
            ))
        _enrich_live_quotes(top_n)

        return PostmarketReportResponse(
            date=effective_date, report=report, exists=True, top_n=top_n,
        )
    except Exception as e:
        logger.warning("读取盘后报告失败: %s", e)
        return PostmarketReportResponse(date=report_date, report="", exists=False)


# ---------------------------------------------------------------------------
# Postmarket followup — 盘中交易时段对盘后推荐股实时重评
# ---------------------------------------------------------------------------


@router.post(
    "/postmarket/followup",
    response_model=PostmarketReportResponse,
    summary="盘中实时重评盘后推荐股",
    description="交易时段内，对盘后推荐股票用盘中因子重新评分。非交易时段返回 exists=False。",
)
def postmarket_followup(
    report_date: Optional[str] = Query(None, description="盘后报告日期 YYYYMMDD，默认昨天"),
):
    if not _is_trading_hours():
        return PostmarketReportResponse(date=report_date or "", report="", exists=False)

    # 确定盘后报告日期（默认昨天）
    if report_date is None:
        from datetime import timedelta
        report_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    # 缓存命中（内存，交易时段内一直有效；SSE 每 30 秒会触发重新计算覆盖）
    cache = _live_rescore_cache.get(report_date)
    if cache:
        return PostmarketReportResponse(
            date=report_date, report="", exists=True,
            top_n=cache["items"], live_rescored=True,
        )

    # 读取盘后 topn 获取候选股票代码
    topn_file = _INTRADAY_REPORTS_DIR / f"postmarket_{report_date}_topn.json"
    if not topn_file.exists():
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    try:
        raw_items = json.loads(topn_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    if not raw_items:
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    candidate_codes = [e.get("stock_code", "") for e in raw_items if e.get("stock_code")]
    if not candidate_codes:
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    # 用盘中因子重评
    try:
        engine = _get_or_create_engine()
        results = engine.discover(mode="intraday", candidate_codes=candidate_codes)
    except Exception as e:
        logger.warning("[Followup] 盘中重评失败: %s", e, exc_info=True)
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    if not results:
        return PostmarketReportResponse(date=report_date, report="", exists=False)

    # 构建 DiscoveryItem 列表
    items: List[DiscoveryItem] = []
    fallback_weights = _get_factor_weights("intraday")
    for i, r in enumerate(results, 1):
        items.append(DiscoveryItem(
            rank=i,
            ts_code=r.ts_code,
            stock_code=r.stock_code,
            stock_name=r.stock_name,
            score=r.score,
            sector=r.sector,
            factor_scores=r.factor_scores,
            factor_weights=r.factor_weights or fallback_weights,
            reasons=r.reasons,
            buy_price_low=r.buy_price_low,
            buy_price_high=r.buy_price_high,
            stop_loss=r.stop_loss,
            take_profit_1=r.take_profit_1,
            take_profit_2=r.take_profit_2,
            discovered_at=r.discovered_at,
            price_at_discovery=r.price_at_discovery,
            pct_chg=getattr(r, "change_pct", 0.0),
            tech_score=r.tech_score,
            rr_score=r.rr_score,
            market_score=r.market_score,
            sector_score=r.sector_score,
            volume_score=r.volume_score,
            position_score=r.position_score,
            formation_score=r.formation_score,
            tech_score_weights=_get_tech_score_weights(),
            composite_score=r.composite_score,
        ))

    _enrich_live_quotes(items)

    # 更新缓存
    _live_rescore_cache[report_date] = {"ts": time.time(), "items": items}

    return PostmarketReportResponse(
        date=report_date, report="", exists=True,
        top_n=items, live_rescored=True,
    )


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
            from src.discovery.engine import create_discovery_engine
            from data_provider.tushare_fetcher import TushareFetcher
            from data_provider.akshare_fetcher import AkshareFetcher

            discovery_config = get_active_config() or get_discovery_config()
            set_active_config(discovery_config)
            tushare_fetcher = TushareFetcher.get_instance()
            akshare_fetcher = AkshareFetcher()
            if not tushare_fetcher.is_available():
                _postmarket_tasks[task_id] = {"status": "failed", "error": "数据源 Tushare 不可用"}
                return

            # 盘后刷新全部数据源，供各因子 fetch_data 直接命中
            from datetime import date as dt_date
            from src.discovery.scanner import (
                refresh_ths_industry_map_postmarket,
                refresh_sector_daily_postmarket,
                refresh_stock_daily_postmarket,
                refresh_limit_pool_postmarket,
                refresh_money_flow_postmarket,
                refresh_daily_basic_postmarket,
                refresh_margin_detail_postmarket,
                refresh_cyq_perf_postmarket,
                refresh_insider_buy_postmarket,
                refresh_institution_hold_postmarket,
                refresh_repurchase_postmarket,
                refresh_profit_forecast_postmarket,
                refresh_performance_report_postmarket,
                refresh_hm_detail_postmarket,
                refresh_popularity_postmarket,
                refresh_tech_indicator_postmarket,
                IntradayScanner,
            )

            today = (
                tushare_fetcher.get_trade_time(early_time="00:00", late_time="18:00")
                or dt_date.today().strftime("%Y%m%d")
            )
            from src.storage import DatabaseManager as _DB

            refreshers = [
                ("ths_industry_map", lambda: refresh_ths_industry_map_postmarket(tushare_fetcher)),
                ("sector_daily", lambda: refresh_sector_daily_postmarket()),
                ("stock_daily", lambda: refresh_stock_daily_postmarket(tushare_fetcher)),
                ("limit_pool", lambda: refresh_limit_pool_postmarket(tushare_fetcher)),
                ("money_flow", lambda: refresh_money_flow_postmarket(tushare_fetcher)),
                ("daily_basic", lambda: refresh_daily_basic_postmarket(tushare_fetcher)),
                ("margin_detail", lambda: refresh_margin_detail_postmarket(tushare_fetcher)),
                ("cyq_perf", lambda: refresh_cyq_perf_postmarket(tushare_fetcher)),
                ("insider_buy", lambda: refresh_insider_buy_postmarket()),
                ("institution_hold", lambda: refresh_institution_hold_postmarket()),
                ("repurchase", lambda: refresh_repurchase_postmarket(tushare_fetcher)),
                ("profit_forecast", lambda: refresh_profit_forecast_postmarket(today, akshare_fetcher)),
                ("performance_report", lambda: refresh_performance_report_postmarket(akshare_fetcher)),
                ("hm_detail", lambda: refresh_hm_detail_postmarket(tushare_fetcher)),
                ("popularity", lambda: refresh_popularity_postmarket(tushare_fetcher)),
                ("tech_indicator", lambda: refresh_tech_indicator_postmarket(tushare_fetcher)),
            ]
            for name, fn in refreshers:
                try:
                    fn()
                except Exception:
                    logger.warning("[Postmarket] %s 刷新失败，继续", name, exc_info=True)

            # 盘后 Tushare 全量刷新 limit_pool 后，用正确数据重跑炸板检测
            try:
                db = _DB()
                fresh_pool = db.get_limit_pool(trade_date=today)
                if fresh_pool is not None and not fresh_pool.empty:
                    fresh_pool = fresh_pool.reset_index()
                    IntradayScanner._detect_limit_breaks(db, fresh_pool, today, "tushare")
            except Exception:
                logger.warning("[Postmarket] 盘后炸板重检测失败", exc_info=True)

            # 游资质量更新（hm_detail 有新数据才重算）
            try:
                from src.discovery.hm_tracker import HmTracker
                HmTracker(db).refresh_and_update()
            except Exception:
                logger.warning("[Postmarket] hm_quality 更新失败", exc_info=True)

            engine = create_discovery_engine(
                discovery_config, tushare_fetcher, akshare_fetcher,
            )

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
                    "pct_chg": getattr(r, "change_pct", 0.0),
                    "tech_score": getattr(r, "tech_score", 0.0),
                    "rr_score": getattr(r, "rr_score", 0.0),
                    "market_score": getattr(r, "market_score", 0.0),
                    "sector_score": getattr(r, "sector_score", 0.0),
                    "volume_score": getattr(r, "volume_score", 0.0),
                    "position_score": getattr(r, "position_score", 0.0),
                    "formation_score": getattr(r, "formation_score", 0.0),
                })

            # 保存报告 + 结构化数据到 discovery_reports
            # 交易日 → 直接保存（供回测使用）；非交易日 → non_trading/ 子目录（仅展示，不回测）
            from src.discovery.engine import is_trading_day
            date_str = (
                tushare_fetcher.get_trade_time(early_time="00:00", late_time="18:00")
                or date.today().strftime('%Y%m%d')
            )
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
    max_drawdown: float = 0.0
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
        max_drawdown=summary.max_drawdown,
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
    from src.discovery.engine import get_factor_weights
    return get_factor_weights(mode)


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


# ------------------------------------------------------------------
# Factor Top-3  (per-factor intraday / postmarket)
# ------------------------------------------------------------------

_FACTOR_LABEL_MAP = {
    "money_flow": "资金流向", "margin": "融资融券", "chip": "筹码分布",
    "technical": "技术形态", "limit": "涨跌停", "fundamental": "基本面",
    "northbound": "北向资金", "institution_hold": "机构持股",
    "profit_forecast": "盈利预测", "buyback": "回购", "insider_buy": "高管增持",
    "broker_recommend": "券商推荐", "popularity": "人气", "hot_money": "游资",
    "performance": "业绩", "momentum": "动量", "rebound": "反弹",
    "sector": "板块", "ma_entry": "均线",
    "ranking_momentum": "排名动量",
}


def _factor_label(name: str) -> str:
    return _FACTOR_LABEL_MAP.get(name, name)


class FactorTopStock(BaseModel):
    stock_code: str
    stock_name: str
    factor_score: float        # 该因子得分
    total_score: float         # 综合得分
    sector: str = ""


class FactorTopEntry(BaseModel):
    factor_name: str           # e.g. "momentum"
    factor_label: str          # e.g. "动量"
    stocks: List[FactorTopStock]  # Top 3


class FactorTopsResponse(BaseModel):
    mode: str
    scan_date: str
    factors: List[FactorTopEntry]


@router.get(
    "/{mode}/factor-tops",
    response_model=FactorTopsResponse,
    summary="获取各因子 Top 3 股票",
)
def get_factor_tops(mode: str = fastapi.Path(..., description="扫描模式: intraday 或 postmarket")):
    """返回最新一次扫描中各因子评分最高的 3 只股票（基于 DB 全量评分记录）。"""
    if mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")

    from src.storage import DatabaseManager, ScanResultIntraday, ScanResultPostmarket
    from sqlalchemy import desc
    from datetime import date as dt_date

    db = DatabaseManager()
    Model = ScanResultIntraday if mode == "intraday" else ScanResultPostmarket

    with db.get_session() as session:
        latest = (
            session.query(Model.scan_date)
            .order_by(desc(Model.scan_date))
            .limit(1)
            .first()
        )
        if not latest:
            return FactorTopsResponse(mode=mode, scan_date="", factors=[])

        scan_date = latest[0]
        # 仅返回当天扫描结果，避免盘中显示旧交易日的盘后数据
        today_str = dt_date.today().strftime("%Y%m%d")
        if scan_date != today_str:
            return FactorTopsResponse(mode=mode, scan_date=scan_date, factors=[])
        rows = session.query(Model).filter(Model.scan_date == scan_date).all()

    if not rows:
        return FactorTopsResponse(mode=mode, scan_date=scan_date, factors=[])

    factor_scores_map: Dict[str, list] = {}
    for row in rows:
        scores = json.loads(row.factor_scores_json or "{}")
        for fname, fscore in scores.items():
            if fscore <= 0:
                continue
            factor_scores_map.setdefault(fname, []).append({
                "stock_code": row.stock_code,
                "stock_name": row.stock_name,
                "factor_score": round(float(fscore), 2),
                "total_score": round(float(row.total_score or 0), 2),
                "sector": row.sector or "",
            })

    factors: List[FactorTopEntry] = []
    for fname, candidates in factor_scores_map.items():
        candidates.sort(key=lambda x: x["factor_score"], reverse=True)
        top5 = candidates[:5]
        factors.append(FactorTopEntry(
            factor_name=fname,
            factor_label=_factor_label(fname),
            stocks=[FactorTopStock(**s) for s in top5],
        ))

    # 按因子权重降序排列
    weights = _get_factor_weights(mode)
    factors.sort(key=lambda f: weights.get(f.factor_name, 0), reverse=True)

    return FactorTopsResponse(mode=mode, scan_date=scan_date, factors=factors)
