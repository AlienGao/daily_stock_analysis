# -*- coding: utf-8 -*-
"""股票发现 API 端点。

提供盘中扫描 Top N 榜单和盘后发现结果查询。
"""

import asyncio
import json
import logging
import multiprocessing
import os
import re
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from queue import Empty as QueueEmpty
from typing import Dict, List, Optional, Tuple

import numpy as np
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
_SNAPSHOT_CACHE_FILE = _INTRADAY_REPORTS_DIR / ".snapshot_dates_cache.json"


def _load_snapshot_cache() -> Dict[str, tuple]:
    """从文件加载快照日期缓存（跨重启持久化）。"""
    if not _SNAPSHOT_CACHE_FILE.exists():
        return {}
    try:
        raw = json.loads(_SNAPSHOT_CACHE_FILE.read_text(encoding="utf-8"))
        cache: Dict[str, tuple] = {}
        for key, val in raw.items():
            factors = val.get("factors", [])
            global_range = val.get("global", {})
            cache[key] = (factors, global_range)
        return cache
    except Exception:
        return {}


def _save_snapshot_cache(cache: Dict[str, tuple]) -> None:
    """将快照日期缓存写入文件。"""
    try:
        serializable: Dict[str, dict] = {}
        for key, (factors, global_range) in cache.items():
            serializable[key] = {"factors": factors, "global": global_range}
        _SNAPSHOT_CACHE_FILE.write_text(
            json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass


def _postmarket_stream_path(date_str: str = "") -> Path:
    """盘后 TopN JSON 路径（每日一个文件）。"""
    if not date_str:
        date_str = date.today().strftime("%Y%m%d")
    return _INTRADAY_REPORTS_DIR / f"postmarket_{date_str}_topn.json"


def _is_trading_hours() -> bool:
    """当前是否在 A 股交易时段（工作日 9:30-11:30, 13:00-15:00）。"""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    minute_of_day = now.hour * 60 + now.minute
    if minute_of_day < 9 * 60 + 30 or minute_of_day > 15 * 60:
        return False
    return not (11 * 60 + 30 <= minute_of_day < 13 * 60)


# ---------------------------------------------------------------------------
# Engine reuse
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Followup cache — 防止 rescore SSE 高频触发导致 discover() 并发堆积
# ---------------------------------------------------------------------------
_followup_cache: Optional["PostmarketReportResponse"] = None
_followup_cache_ts: float = 0.0
_followup_lock = threading.Lock()
_FOLLOWUP_TTL = 60


def _get_live_quotes(ts_codes: List[str]) -> "tuple[Dict[str, float], Dict[str, float]]":
    """获取实时价格和涨跌幅。

    优先从 realtime_spot DB 读取（盘中扫描器每 30s 刷新），
    若数据过期（slot 落后 >30s）则回退到 Sina 实时行情接口。

    Returns: (prices_dict, pct_chg_dict)，key 为裸码（与 DB 一致）。
    """
    bare_codes = [c.split(".")[0] if "." in c else c for c in ts_codes]
    use_db = False
    try:
        from src.storage import DatabaseManager
        spot_df = DatabaseManager().get_current_prices(bare_codes)
        if spot_df is not None and not spot_df.empty and "slot" in spot_df.columns:
            current_slot = int(time.time() / 30)
            max_slot = int(spot_df["slot"].max())
            if current_slot - max_slot <= 1:
                use_db = True
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

    # DB 数据过期或不可用：Sina 实时行情兜底
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
    recent_count: int = 0            # 近5个交易日出现次数


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


_SELECTION_HISTORY_PATH = _INTRADAY_REPORTS_DIR / "selection_history.json"


def _get_recent_appearance_counts(days: int = 5) -> Dict[str, int]:
    """从 selection_history.json 统计近 N 个交易日的出现次数。

    返回 {bare_code: count}，代码统一归一化为 6 位裸码。
    """
    if not _SELECTION_HISTORY_PATH.exists():
        return {}
    try:
        all_history = json.loads(_SELECTION_HISTORY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

    sorted_dates = sorted(all_history.keys(), reverse=True)[:days]
    counts: Dict[str, int] = {}
    for d in sorted_dates:
        for code in all_history.get(d, []):
            code_str = str(code).strip()
            bare = code_str.split(".")[0].zfill(6) if "." in code_str else code_str.zfill(6)
            counts[bare] = counts.get(bare, 0) + 1
    return counts


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


def _enrich_recent_counts(items: List[DiscoveryItem]) -> None:
    """给每个 item 设置近 N 个交易日的出现次数。"""
    counts = _get_recent_appearance_counts(days=5)
    if not counts:
        return
    for item in items:
        bare = (item.ts_code or item.stock_code).split(".")[0].zfill(6)
        item.recent_count = counts.get(bare, 0)


# ---------------------------------------------------------------------------
# Async postmarket task tracker
# ---------------------------------------------------------------------------

_postmarket_tasks: Dict[str, dict] = {}

_factor_backtest_tasks: Dict[str, dict] = {}
_factor_optimize_tasks: Dict[str, dict] = {}


def _cleanup_old_tasks():
    """清理 60 分钟前完成/失败的回测与优化任务。"""
    cutoff = datetime.now() - timedelta(minutes=60)
    for tasks_dict in (_factor_backtest_tasks, _factor_optimize_tasks):
        stale = [
            tid for tid, t in list(tasks_dict.items())
            if t.get("status") in ("completed", "failed")
            and datetime.fromisoformat(t.get("finished_at", t.get("started_at", "2000-01-01T00:00:00"))) < cutoff
        ]
        for tid in stale:
            del tasks_dict[tid]


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

        _enrich_recent_counts(top_n)
        _enrich_recent_counts(dropped)
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

                    # 交易时段每 15s 推送 rescore 事件
                    if ticks_since_last_rescore >= 7:
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
# Pipeline config (运行时覆盖，持久化到 discovery_runtime.json)
# ---------------------------------------------------------------------------

class PipelineConfigResponse(BaseModel):
    intraday_pipeline_enabled: Optional[bool] = None
    postmarket_pipeline_enabled: Optional[bool] = None
    score_blend_alpha: Optional[float] = None


@router.get(
    "/pipeline-config",
    response_model=PipelineConfigResponse,
    summary="获取管线开关 & 综合分混合比例",
)
def get_pipeline_config():
    from src.discovery.config import _ensure_active_config
    cfg = _ensure_active_config()
    return PipelineConfigResponse(
        intraday_pipeline_enabled=cfg._intraday_pipeline_enabled,
        postmarket_pipeline_enabled=cfg._postmarket_pipeline_enabled,
        score_blend_alpha=cfg._score_blend_alpha,
    )


@router.post(
    "/pipeline-config",
    response_model=PipelineConfigResponse,
    summary="更新管线开关 & 综合分混合比例（立即生效）",
)
def set_pipeline_config(body: PipelineConfigResponse):
    from src.discovery.config import _ensure_active_config, save_runtime_state
    cfg = _ensure_active_config()
    if body.intraday_pipeline_enabled is not None:
        cfg._intraday_pipeline_enabled = body.intraday_pipeline_enabled
    if body.postmarket_pipeline_enabled is not None:
        cfg._postmarket_pipeline_enabled = body.postmarket_pipeline_enabled
    if body.score_blend_alpha is not None:
        cfg._score_blend_alpha = max(0.0, min(1.0, body.score_blend_alpha))
    save_runtime_state()
    return PipelineConfigResponse(
        intraday_pipeline_enabled=cfg._intraday_pipeline_enabled,
        postmarket_pipeline_enabled=cfg._postmarket_pipeline_enabled,
        score_blend_alpha=cfg._score_blend_alpha,
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

def _find_latest_report_date(candidate: str, pattern: str = "postmarket_") -> str:
    """从 candidate 向前查找最近一个存在报告文件的日期（最多 14 天）。"""
    reports_dir = Path(__file__).resolve().parent.parent.parent.parent / "discovery_reports"
    for offset in range(14):
        d = (datetime.strptime(candidate, "%Y%m%d") - timedelta(days=offset)).strftime("%Y%m%d")
        for subdir in (reports_dir, reports_dir / "non_trading"):
            if (subdir / f"{pattern}{d}.md").exists() or (subdir / f"{pattern}{d}_topn.json").exists():
                return d
    return candidate


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
        effective_date = _find_latest_report_date(report_date, "postmarket_")
        filepath, found_dir, _ = _find_report(effective_date)
    if filepath is None:
        # 最后尝试内存中的最近完成任务
        recent = _get_latest_completed_task()
        if recent and recent.get("report"):
            top_n = _build_discovery_items(recent.get("top_n", []), mode="postmarket")
            _enrich_live_quotes(top_n)
            _enrich_recent_counts(top_n)
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
        _enrich_recent_counts(top_n)

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
    global _followup_cache, _followup_cache_ts

    if not _is_trading_hours():
        return PostmarketReportResponse(date=report_date or "", report="", exists=False)

    from src.discovery.engine import is_trading_day
    if not is_trading_day():
        return PostmarketReportResponse(date=report_date or "", report="", exists=False)

    # TTL 内直接返回缓存
    now = time.time()
    if _followup_cache is not None and now - _followup_cache_ts < _FOLLOWUP_TTL:
        return _followup_cache

    # 另一个线程正在跑 discover()，返回上一次缓存
    if not _followup_lock.acquire(blocking=False):
        if _followup_cache is not None:
            return _followup_cache
        return PostmarketReportResponse(date=report_date or "", report="", exists=False)

    try:
        return _postmarket_followup_locked(report_date)
    finally:
        _followup_lock.release()


def _postmarket_followup_locked(report_date: Optional[str]):
    global _followup_cache, _followup_cache_ts

    # 确定盘后报告日期（默认最近一个交易日）
    if report_date is None:
        report_date = _find_latest_report_date(date.today().strftime("%Y%m%d"), "postmarket_")

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
        results = engine.discover(mode="intraday", candidate_codes=candidate_codes, skip_monitor=True)
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
    _enrich_recent_counts(items)

    resp = PostmarketReportResponse(
        date=report_date, report="", exists=True,
        top_n=items, live_rescored=True,
    )
    _followup_cache = resp
    _followup_cache_ts = time.time()
    return resp


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
    is_open: bool = False  # 未到卖出时间，未平仓


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
            is_open=t.is_open,
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
    tech_score: float = 0.0
    composite_score: float = 0.0
    tech_score_breakdown: Optional[Dict[str, float]] = None
    tech_score_weights: Dict[str, float] = {}
    factor_scores: Dict[str, float]
    factor_weights: Dict[str, float] = {}
    sector: str
    # 价格点位（实时计算）
    current_price: Optional[float] = None
    buy_price_low: Optional[float] = None
    buy_price_high: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None


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


def _append_today_ohlcv(ohlcv_rows: list, stock_code: str, td_obj, db: "DatabaseManager") -> None:
    """从 realtime_spot 取当日 OHLC 补齐到 OHLCV 列表末尾。

    stock_daily 只有历史日线，盘中/盘后当日 K 线尚未落库。
    用 realtime_spot 的 open_price/high/low/price 拼成当日 K 线。
    """
    from collections import namedtuple
    try:
        with db.get_session() as s:
            from src.storage import RealtimeSpot
            spot = s.execute(
                s.query(RealtimeSpot).filter(RealtimeSpot.code == stock_code)
            ).scalars().first()
            if spot is None:
                return
            if spot.trade_date and spot.trade_date != td_obj.isoformat():
                return

        if not (spot.open_price and spot.high and spot.low and spot.price):
            return

        ORow = namedtuple("ORow", ["date", "open", "high", "low", "close"])
        ohlcv_rows.append(ORow(
            date=td_obj,
            open=float(spot.open_price),
            high=float(spot.high),
            low=float(spot.low),
            close=float(spot.price),
        ))
    except Exception:
        pass


# 大盘指数 OHLCV 缓存（供 StockScorer 市场环境评分使用）
_index_ohlcv_cache: Optional[np.ndarray] = None
_index_ohlcv_ts: float = 0


def _get_index_ohlcv() -> Optional[np.ndarray]:
    """获取上证指数 OHLCV，5 分钟缓存。失败返回 None。"""
    global _index_ohlcv_cache, _index_ohlcv_ts
    import numpy as np
    from datetime import timedelta
    now = time.time()
    if _index_ohlcv_cache is not None and (now - _index_ohlcv_ts) < 300:
        return _index_ohlcv_cache

    try:
        from data_provider.tushare_fetcher import TushareFetcher
        fetcher = TushareFetcher()
        if not fetcher.is_available():
            return None

        today = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        api = getattr(fetcher, '_api', None)
        if api is None:
            return None

        df = api.index_daily(ts_code='000001.SH', start_date=start, end_date=today)
        if df is None or df.empty:
            return None

        df = df.sort_values('trade_date')
        arr = df[['open', 'high', 'low', 'close']].values.astype(np.float64)

        # 补齐当日指数 K 线（Sina 实时）
        try:
            import requests
            r = requests.get(
                "http://hq.sinajs.cn/list=s_sh000001",
                headers={"Referer": "https://finance.sina.com.cn"},
                timeout=10,
            )
            r.encoding = "gbk"
            content = r.text.split('"')[1] if '"' in r.text else ""
            parts = content.split(",")
            if len(parts) >= 5:
                idx_open = float(parts[1]) if parts[1] else 0
                idx_high = float(parts[4]) if parts[4] else 0
                idx_low = float(parts[5]) if parts[5] else 0
                idx_close = float(parts[3]) if parts[3] else 0
                if idx_close > 0:
                    today_row = np.array([[idx_open, idx_high, idx_low, idx_close]], dtype=np.float64)
                    arr = np.vstack([arr, today_row])
        except Exception:
            pass

        _index_ohlcv_cache = arr
        _index_ohlcv_ts = now
        logger.info("[StockScore] 已加载 %d 条上证指数 OHLCV", len(arr))
        return arr
    except Exception:
        logger.warning("[StockScore] 获取 index OHLCV 失败", exc_info=True)
        return None


def _calc_tech_for_row(row, db: "DatabaseManager") -> Tuple[float, float, Dict, Dict, float, Optional[float], Optional[float], Optional[float], Optional[float]]:  # noqa: E501
    """从 DB 行实时计算 tech_score + 价格点位（price, buy_low, buy_high, stop_loss, tp1）。"""
    from src.services.stock_scorer import StockScorer, StockScorerConfig
    from src.services.stop_loss_calculator import compute_from_arrays
    import numpy as np
    from datetime import date, timedelta

    stock_code = row.stock_code
    trade_date = row.scan_date
    sector = row.sector or ""

    # 格式化日期
    if len(trade_date) == 8:
        trade_date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
    else:
        trade_date_fmt = trade_date

    # tech_indicators
    tech_cache = db.get_tech_indicators_batch([stock_code], trade_date_fmt).get(stock_code, {})

    # 最新价格（用昨收作为 fallback，因为盘后实时价格可能已清）
    price_df = db.get_current_prices([stock_code])
    price = 0.0
    if not price_df.empty and stock_code in price_df.index:
        price = float(price_df.at[stock_code, "price"])
    # fallback: 用 stock_daily 最新收盘价
    if price <= 0:
        from src.storage import StockDaily
        with db.get_session() as s:
            latest = s.execute(
                s.query(StockDaily).filter(StockDaily.code == stock_code)
                .order_by(StockDaily.date.desc())
            ).scalars().first()
            if latest:
                price = float(latest.close)
    if price <= 0:
        return 0.0, 0.0, {}, {}, 0.0, None, None, None, None

    # 量比（从 daily_basic 补）
    vol_ratio = max(tech_cache.get("vol_ratio", 1.0), 1.0)
    if vol_ratio == 1.0:
        from src.storage import DailyBasic
        with db.get_session() as s:
            from sqlalchemy import select
            vr = s.execute(
                select(DailyBasic.volume_ratio).filter(
                    DailyBasic.code == stock_code,
                    DailyBasic.trade_date == trade_date[:8]
                )
            ).scalar_one_or_none()
            if vr is not None:
                vol_ratio = max(float(vr), 1.0)

    # OHLCV 180天
    td_obj = date.today()
    ohlcv_start = td_obj - timedelta(days=200)
    ohlcv_map = db.get_data_range_batch([stock_code], ohlcv_start, td_obj)
    ohlcv_rows = ohlcv_map.get(stock_code, [])

    # 盘中/盘后补上当日 K 线（stock_daily 只有历史日线，当日尚未落库）
    if not ohlcv_rows or (ohlcv_rows[-1].date != td_obj):
        _append_today_ohlcv(ohlcv_rows, stock_code, td_obj, db)

    highs = np.array([d.high for d in ohlcv_rows], dtype=float)
    lows = np.array([d.low for d in ohlcv_rows], dtype=float)
    closes = np.array([d.close for d in ohlcv_rows], dtype=float)
    pre_close = float(closes[-2]) if len(closes) > 1 else price

    # 精确止盈止损
    sl = compute_from_arrays(
        highs, lows, closes, code=stock_code,
        ma20=tech_cache.get("ma20"),
        ma60=tech_cache.get("ma60"),
        atr=tech_cache.get("atr"),
        factor_score=float(row.total_score or 50.0),
    )
    est_stop = sl.stop_loss or 0
    est_tp1 = sl.take_profit_1 or 0
    est_tp2 = sl.take_profit_2 or 0

    # 轻量 formation reason
    lite_reasons = []
    ma5, ma10, ma20_v = tech_cache.get("ma5"), tech_cache.get("ma10"), tech_cache.get("ma20")
    if ma5 and ma10 and ma20_v and ma5 > ma10 > ma20_v:
        lite_reasons.append("均线多头排列")
    if tech_cache.get("macd", 0) > 0:
        lite_reasons.append("MACD金叉")
    rsi = tech_cache.get("rsi_12")
    if rsi is not None and rsi < 45:
        lite_reasons.append("RSI低位回升")
    bm = tech_cache.get("boll_mid")
    if bm and price > bm:
        lite_reasons.append("站上BOLL中轨")
    vol_ratio = max(vol_ratio, 1.0)

    scorer = StockScorer(StockScorerConfig())
    idx_ohlcv = _get_index_ohlcv()
    if idx_ohlcv is not None:
        scorer.preload_index_ohlcv(idx_ohlcv)
    tech = scorer.score(
        stock_code=stock_code, sector=sector, price=price, pre_close=pre_close,
        tp1=est_tp1, tp2=est_tp2, stop_loss=est_stop,
        reasons=lite_reasons, ohlcv=(highs, lows, closes), volume_ratio=vol_ratio,
    )
    alpha = 0.3
    factor_score = float(row.total_score or 0)
    composite = alpha * factor_score + (1 - alpha) * tech.composite
    breakdown = {
        "rr_score": tech.rr_score,
        "market_score": tech.market_score,
        "sector_score": tech.sector_score,
        "volume_score": tech.volume_score,
        "position_score": tech.position_score,
        "formation_score": tech.formation_score,
    }
    return (tech.composite, composite, breakdown, tech.weights,
            price, sl.buy_low, sl.buy_high, est_stop, est_tp1)


def _row_to_item(row, factor_weights: Dict[str, float] = None,
                 db: "DatabaseManager" = None) -> Optional[StockScoreItem]:
    """将 ORM 行转为 StockScoreItem，tech_score 实时计算。"""
    if row is None:
        return None
    factor_scores: Dict[str, float] = {}
    try:
        raw = json.loads(row.factor_scores_json or "{}")
        factor_scores = {k: float(v) for k, v in raw.items()}
    except (json.JSONDecodeError, TypeError):
        pass

    # 实时计算 tech_score（DB 里因子分对应的 tech 分），仅管线开启时执行
    tech_score_val = round(float(getattr(row, 'tech_score', 0) or 0), 2)
    composite_val = round(float(getattr(row, 'composite_score', 0) or 0), 2)
    tech_breakdown = None
    tech_weights: Dict[str, float] = {}
    current_price: Optional[float] = None
    buy_price_low: Optional[float] = None
    buy_price_high: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit_1: Optional[float] = None
    if db is not None:
        try:
            from src.discovery.config import _ensure_active_config
            cfg = _ensure_active_config()
            use_pipeline = cfg.enable_intraday_pipeline if row.scan_mode == 'intraday' else cfg.enable_postmarket_pipeline
            if use_pipeline:
                (_tech, _composite, _breakdown, _weights,
                 _price, _buy_low, _buy_high, _stop_loss, _tp1) = _calc_tech_for_row(row, db)
                current_price = _price if _price > 0 else None
                buy_price_low = _buy_low
                buy_price_high = _buy_high
                stop_loss = _stop_loss if (_stop_loss and _stop_loss > 0) else None
                take_profit_1 = _tp1 if (_tp1 and _tp1 > 0) else None
                if _tech > 0:
                    tech_score_val = round(_tech, 2)
                    factor_score = float(row.total_score or 0)
                    alpha = cfg.effective_score_blend_alpha
                    composite_val = round(alpha * factor_score + (1 - alpha) * _tech, 2)
                    tech_breakdown = _breakdown
                    tech_weights = _weights
        except Exception:
            pass

    return StockScoreItem(
        scanned_at=_format_scanned_at(row.scan_date, row.scan_time),
        rank=row.rank,
        total_score=round(float(getattr(row, 'total_score', 0) or 0), 2),
        tech_score=tech_score_val,
        composite_score=composite_val,
        tech_score_breakdown=tech_breakdown,
        tech_score_weights=tech_weights,
        factor_scores=factor_scores,
        factor_weights=factor_weights or {},
        sector=getattr(row, 'sector', '') or '',
        current_price=current_price,
        buy_price_low=buy_price_low,
        buy_price_high=buy_price_high,
        stop_loss=stop_loss,
        take_profit_1=take_profit_1,
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
            score_item = _row_to_item(row, factor_weights=factor_weights, db=db)

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


# ------------------------------------------------------------------
# Factor Backtest
# ------------------------------------------------------------------


class FactorBacktestRequest(BaseModel):
    mode: str = "postmarket"
    factor_weights: Dict[str, float] = {}
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    top_n: int = 5
    hold_days: List[int] = [1, 3, 5, 10, 20]
    initial_capital: float = 1_000_000.0
    risk_free_rate: float = 0.02
    use_pipeline: bool = False
    score_blend_alpha: float = 0.3
    reoptimize_interval: Optional[int] = None  # None=固定权重, 10=每10日TPE调优


def _run_backtest_in_process(queue: multiprocessing.Queue, req_dict: dict):
    """在独立进程中运行回测（拥有独立 GIL，不阻塞 uvicorn 事件循环）。"""
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

        if req_dict.get("reoptimize_interval") and req_dict["reoptimize_interval"] > 0:
            result = engine.compute_walk_forward(
                mode=req_dict["mode"],
                factor_weights=fw,
                start_date=req_dict.get("start_date"),
                end_date=req_dict.get("end_date"),
                top_n=req_dict.get("top_n"),
                hold_days=req_dict.get("hold_days"),
                initial_capital=req_dict.get("initial_capital"),
                risk_free_rate=req_dict.get("risk_free_rate"),
                use_pipeline=req_dict.get("use_pipeline"),
                score_blend_alpha=req_dict.get("score_blend_alpha"),
                reoptimize_interval=req_dict["reoptimize_interval"],
                progress_cb=_progress,
            )
        else:
            result = engine.compute(
                mode=req_dict["mode"],
                factor_weights=fw,
                start_date=req_dict.get("start_date"),
                end_date=req_dict.get("end_date"),
                top_n=req_dict.get("top_n"),
                hold_days=req_dict.get("hold_days"),
                initial_capital=req_dict.get("initial_capital"),
                risk_free_rate=req_dict.get("risk_free_rate"),
                use_pipeline=req_dict.get("use_pipeline"),
                score_blend_alpha=req_dict.get("score_blend_alpha"),
                progress_cb=_progress,
            )

        if result is None:
            queue.put(("failed", "回测数据不足，请检查日期范围或因子选择"))
        else:
            from dataclasses import asdict
            queue.put(("completed", asdict(result)))
    except Exception as e:
        import traceback
        traceback.print_exc()
        queue.put(("failed", str(e)))


def _monitor_backtest_process(task_id: str, queue: multiprocessing.Queue, proc: multiprocessing.Process):
    """轻量监控线程：从 Queue 读取进度/结果并更新任务字典。queue.get 会释放 GIL。"""
    logger.info("回测监控线程启动 task_id=%s proc_pid=%s", task_id, proc.pid)
    try:
        while True:
            try:
                msg = queue.get(timeout=2.0)
            except QueueEmpty:
                if not proc.is_alive():
                    exitcode = proc.exitcode
                    logger.error("回测进程异常退出 task_id=%s exitcode=%s", task_id, exitcode)
                    t = _factor_backtest_tasks.get(task_id, {})
                    if t.get("status") == "running":
                        t["status"] = "failed"
                        t["error"] = f"回测进程异常退出 (exitcode={exitcode})"
                        t["finished_at"] = datetime.now().isoformat()
                    break
                continue

            msg_type, payload = msg
            if msg_type == "progress":
                t = _factor_backtest_tasks.get(task_id)
                if t:
                    t["status_message"] = payload
            elif msg_type == "completed":
                _factor_backtest_tasks[task_id] = {
                    "status": "completed",
                    "result": payload,
                    "finished_at": datetime.now().isoformat(),
                }
                break
            elif msg_type == "failed":
                t = _factor_backtest_tasks.get(task_id, {})
                t["status"] = "failed"
                t["error"] = payload
                t["finished_at"] = datetime.now().isoformat()
                break
    except Exception as e:
        logger.error("回测监控线程异常: %s", e)
        t = _factor_backtest_tasks.get(task_id, {})
        if t.get("status") == "running":
            t["status"] = "failed"
            t["error"] = f"监控异常: {e}"
            t["finished_at"] = datetime.now().isoformat()
    finally:
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()


@router.post(
    "/factor-backtest",
    summary="因子组合回测（异步）",
)
def factor_backtest(req: FactorBacktestRequest):
    """提交因子回测参数，返回 task_id，通过 GET /factor-backtest/status 轮询结果。

    支持单因子评估和多因子加权组合，多持有期横向对比。
    数据源为 factor_score_snapshots 表。
    """
    import uuid

    if req.mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")

    # 并发控制：同一 mode 只允许一个回测运行
    for tid, t in list(_factor_backtest_tasks.items()):
        if t.get("status") == "running" and t.get("mode") == req.mode:
            raise HTTPException(
                status_code=409,
                detail=f"已有回测任务运行中（task_id={tid}），请等待完成后再试",
            )

    task_id = str(uuid.uuid4())[:8]
    _factor_backtest_tasks[task_id] = {
        "status": "running",
        "mode": req.mode,
        "started_at": datetime.now().isoformat(),
    }

    req_dict = {
        "mode": req.mode,
        "factor_weights": req.factor_weights,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "top_n": req.top_n,
        "hold_days": req.hold_days,
        "initial_capital": req.initial_capital,
        "risk_free_rate": req.risk_free_rate,
        "use_pipeline": req.use_pipeline,
        "score_blend_alpha": req.score_blend_alpha,
        "reoptimize_interval": req.reoptimize_interval,
    }

    queue: multiprocessing.Queue = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_run_backtest_in_process,
        args=(queue, req_dict),
        daemon=True,
    )
    proc.start()
    logger.info("回测进程已启动 task_id=%s proc_pid=%s mode=%s", task_id, proc.pid, req.mode)

    threading.Thread(
        target=_monitor_backtest_process,
        args=(task_id, queue, proc),
        daemon=True,
    ).start()

    return {"task_id": task_id, "status": "running"}


@router.get(
    "/factor-backtest/status",
    summary="查询因子回测任务状态",
)
def factor_backtest_status(task_id: str = Query(..., description="任务 ID")):
    """轮询后台因子回测任务的执行状态。"""
    # 懒清理：删除 60 分钟前完成/失败的任务
    _cleanup_old_tasks()

    task = _factor_backtest_tasks.get(task_id)
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


# 快照日期范围缓存（按 mode + 日期，内存 + 文件持久化，重启不丢）
_snapshot_dates_cache: Dict[str, tuple] = {}
_snapshot_cache_loaded = False


def _ensure_snapshot_cache():
    """惰性加载文件缓存到内存（首次调用时触发）。"""
    global _snapshot_dates_cache, _snapshot_cache_loaded
    if _snapshot_cache_loaded:
        return
    _snapshot_dates_cache = _load_snapshot_cache()
    _snapshot_cache_loaded = True


def _factor_snapshot_response(factors, global_range, mode):
    """构建 snapshot-dates 统一响应（含权重与管线配置）。"""
    from src.discovery.engine import get_factor_weights
    from src.discovery.config import DiscoveryConfig
    weights = get_factor_weights(mode)
    cfg = DiscoveryConfig()
    use_pipeline = cfg.enable_intraday_pipeline if mode == "intraday" else cfg.enable_postmarket_pipeline
    return {
        "factors": factors, "global": global_range,
        "weights": weights, "use_pipeline": use_pipeline,
        "score_blend_alpha": cfg.score_blend_alpha,
    }


@router.get(
    "/factor-snapshot-dates",
    summary="查询因子快照可用日期范围",
)
def factor_snapshot_dates(mode: str = Query("postmarket", description="intraday 或 postmarket")):
    """返回每个因子的可用日期范围及全量交集（按 mode 缓存，文件持久化跨重启有效）。"""
    if mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")

    _ensure_snapshot_cache()

    cache_key = mode
    cached = _snapshot_dates_cache.get(cache_key)
    if cached is not None:
        factors, global_range = cached
        cached_to = global_range.get("available_to", "") if isinstance(global_range, dict) else ""
        try:
            from src.storage import DatabaseManager, FactorScoreSnapshot
            from sqlalchemy import func
            db = DatabaseManager()
            with db.get_session() as sess:
                db_max = sess.query(func.max(FactorScoreSnapshot.trade_date)).filter(
                    FactorScoreSnapshot.mode == mode
                ).scalar()
            # 仅比较最新日期：数据为 append-only，max 不变则缓存有效
            if db_max and cached_to and db_max <= cached_to:
                return _factor_snapshot_response(factors, global_range, mode)
        except Exception:
            import logging
            _log = logging.getLogger(__name__)
            _log.warning("[snapshot-dates] DB 校验失败，降级复用缓存 (mode=%s)", mode, exc_info=True)
            return _factor_snapshot_response(factors, global_range, mode)

    from src.discovery.factor_backtest_engine import FactorBacktestEngine

    engine = FactorBacktestEngine()
    factors, global_range = engine.get_snapshot_date_ranges(mode)

    _snapshot_dates_cache[cache_key] = (factors, global_range)
    _save_snapshot_cache(_snapshot_dates_cache)
    return _factor_snapshot_response(factors, global_range, mode)


@router.get(
    "/factor-weights",
    summary="查询当前因子权重与管线模式（轻量，仅读 .env）",
)
def factor_weights(mode: str = Query("postmarket", description="intraday 或 postmarket")):
    """返回当前 .env 中的因子权重映射及管线开关状态，无 DB 查询，毫秒级响应。"""
    if mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")

    from src.discovery.factor_backtest_engine import FactorBacktestEngine
    engine = FactorBacktestEngine()
    weights = engine._get_default_weights(mode)

    from src.discovery.config import get_active_config, DiscoveryConfig
    cfg = get_active_config() or DiscoveryConfig()
    use_pipeline = cfg.enable_intraday_pipeline if mode == "intraday" else cfg.enable_postmarket_pipeline
    blend_alpha = cfg.effective_score_blend_alpha

    return {
        "mode": mode,
        "weights": weights,
        "use_pipeline": use_pipeline,
        "score_blend_alpha": blend_alpha,
    }


# ------------------------------------------------------------------
# Factor Weight Optimization (TPE)
# ------------------------------------------------------------------


class FactorOptimizeRequest(BaseModel):
    mode: str = "postmarket"  # intraday 或 postmarket
    window: int = 60  # 回测窗口交易日数
    normalize: bool = False  # 归一化（零和重分配）
    n_trials: int = 100  # TPE 试验次数
    auto_apply: bool = True  # 自动写入 .env


@router.post(
    "/factor-backtest/optimize",
    summary="因子权重优化（异步 TPE）",
)
def factor_optimize(req: FactorOptimizeRequest):
    """提交因子权重优化参数，返回 task_id，通过 GET /factor-backtest/optimize/status 轮询结果。

    复用 Optuna TPE + SQLite 持久化，CLI 与 Web 共享同一 study。
    优化通过护栏后自动写入 .env。
    """
    import uuid

    if req.mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")
    if req.window < 20 or req.window > 252:
        raise HTTPException(status_code=400, detail="window 须在 20~252 之间")
    if req.n_trials < 10 or req.n_trials > 500:
        raise HTTPException(status_code=400, detail="n_trials 须在 10~500 之间")

    # 并发控制：同一 mode 只允许一个优化运行
    for tid, t in list(_factor_optimize_tasks.items()):
        if t.get("status") == "running" and t.get("mode") == req.mode:
            raise HTTPException(
                status_code=409,
                detail=f"已有优化任务运行中（task_id={tid}），请等待完成后再试",
            )

    task_id = str(uuid.uuid4())[:8]
    _factor_optimize_tasks[task_id] = {
        "status": "running",
        "mode": req.mode,
        "started_at": datetime.now().isoformat(),
        "phase": "starting",
        "progress": {"trial": 0, "n_trials": req.n_trials, "best_value": None},
    }

    def _progress(info: dict):
        t = _factor_optimize_tasks.get(task_id)
        if t is None:
            return
        phase = info.get("phase", t.get("phase", ""))
        t["phase"] = phase
        t["status_message"] = info.get("message", "")
        if phase == "tpe":
            t["progress"] = {
                "trial": info.get("trial", 0),
                "n_trials": info.get("n_trials", req.n_trials),
                "best_value": info.get("best_value"),
            }
        elif phase == "done" and info.get("result"):
            t["result"] = info["result"]

    def _run():
        try:
            from src.discovery.factor_optimizer import FactorOptimizer

            optimizer = FactorOptimizer(progress_callback=_progress)
            result = optimizer.optimize(
                mode=req.mode,
                window=req.window,
                normalize=req.normalize,
                n_trials=req.n_trials,
                auto_apply=False,  # Web 端始终不自动应用，由前端确认弹窗接管
            )

            t = _factor_optimize_tasks.get(task_id, {})
            if result and result.get("report"):
                t["status"] = "completed"
                t["result"] = result
            else:
                t["status"] = "failed"
                t["error"] = "优化未产生有效推荐（无因子通过筛选或无有效组合）"
                t["result"] = result
            t["finished_at"] = datetime.now().isoformat()
        except Exception as e:
            logger.error("因子权重优化失败: %s", e, exc_info=True)
            t = _factor_optimize_tasks.get(task_id, {})
            t["status"] = "failed"
            t["error"] = str(e)
            t["finished_at"] = datetime.now().isoformat()

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id, "status": "running"}


@router.get(
    "/factor-backtest/optimize/status",
    summary="查询因子权重优化任务状态",
)
def factor_optimize_status(task_id: str = Query(..., description="任务 ID")):
    """轮询后台优化任务的执行状态，包含阶段、进度和结果。"""
    _cleanup_old_tasks()

    task = _factor_optimize_tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务 ID 不存在")

    resp = {
        "task_id": task_id,
        "status": task.get("status", "unknown"),
        "phase": task.get("phase", ""),
        "progress": task.get("progress", {}),
    }
    if task.get("status_message"):
        resp["status_message"] = task["status_message"]
    if task.get("status") == "failed":
        resp["error"] = task.get("error", "")
    if task.get("status") == "completed" and task.get("result"):
        r = task["result"]
        resp["result"] = {
            "report_path": r.get("report_path"),
            "recommendation": r.get("recommendation"),
            "baseline": r.get("baseline"),
            "applied": r.get("applied", False),
        }
    return resp


class FactorApplyRequest(BaseModel):
    mode: str = "postmarket"  # intraday 或 postmarket
    weights: Dict[str, float]  # 因子名 → 新权重
    report_path: Optional[str] = None  # 元数据 JSON 路径，用于标记 applied=true


@router.post(
    "/factor-backtest/optimize/apply",
    summary="应用优化权重到 .env",
)
def factor_optimize_apply(req: FactorApplyRequest):
    """将确认后的优化权重写入 .env（备份旧文件后替换），并标记元数据 JSON 的 applied=true。"""
    if req.mode not in ("intraday", "postmarket"):
        raise HTTPException(status_code=400, detail="mode 须为 intraday 或 postmarket")
    if not req.weights:
        raise HTTPException(status_code=400, detail="weights 不能为空")

    from src.discovery.factor_optimizer import FactorOptimizer
    success = FactorOptimizer.apply_weights(req.weights, mode=req.mode)
    if not success:
        raise HTTPException(status_code=500, detail="写入 .env 失败")

    # 更新元数据 JSON 的 applied 标记
    opt_dir = _INTRADAY_REPORTS_DIR / "factor_optimization"
    meta_updated = False
    logger.info("[apply] report_path=%s mode=%s weights_count=%d",
                req.report_path, req.mode, len(req.weights))

    if req.report_path:
        meta_path = Path(req.report_path).with_suffix(".json")
        logger.info("[apply] trying exact meta_path=%s exists=%s", meta_path, meta_path.exists())
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                meta["applied"] = True
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                meta_updated = True
                logger.info("[apply] updated via exact report_path")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("[apply] exact update failed: %s", e)

    if not meta_updated and opt_dir.exists():
        # 回退1：找到最近一条匹配模式的未应用元数据 JSON
        import glob as _glob
        for mp in sorted(_glob.glob(str(opt_dir / "*.json")), reverse=True):
            if mp.endswith("latest.json"):
                continue
            try:
                meta = json.loads(Path(mp).read_text(encoding="utf-8"))
                if meta.get("mode") == req.mode and not meta.get("applied", False):
                    meta["applied"] = True
                    Path(mp).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                    meta_updated = True
                    logger.info("[apply] updated via fallback: %s", os.path.basename(mp))
                    break
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("[apply] fallback skip %s: %s", os.path.basename(mp), e)
                continue

    if not meta_updated and opt_dir.exists():
        # 回退2：按权重匹配，找到推荐权重与所应用权重一致的元数据
        applied_set = set(f"{k}={v}" for k, v in sorted(req.weights.items()))
        for mp in sorted(_glob.glob(str(opt_dir / "*.json")), reverse=True):
            if mp.endswith("latest.json"):
                continue
            try:
                meta = json.loads(Path(mp).read_text(encoding="utf-8"))
                rec = meta.get("recommendation", {})
                if not rec:
                    continue
                rec_set = set(f"{k}={v}" for k, v in sorted(rec.items()))
                if applied_set == rec_set and not meta.get("applied", False):
                    meta["applied"] = True
                    Path(mp).write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                    meta_updated = True
                    logger.info("[apply] updated via weight-match: %s", os.path.basename(mp))
                    break
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("[apply] weight-match skip %s: %s", os.path.basename(mp), e)
                continue

    if not meta_updated:
        logger.warning("[apply] weights applied to .env but no metadata JSON updated")

    # 同步更新 latest.json
    latest_path = opt_dir / "latest.json"
    if latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
            if latest.get("mode") == req.mode:
                latest["applied"] = True
                latest_path.write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")
                logger.info("[apply] synced latest.json applied=true")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("[apply] latest.json sync failed: %s", e)

    return {"status": "applied", "mode": req.mode, "updated": len(req.weights)}


@router.get(
    "/factor-backtest/optimize/history",
    summary="查询因子权重优化历史",
)
def factor_optimize_history(
    mode: Optional[str] = Query(None, description="筛选模式：intraday 或 postmarket"),
):
    """扫描 discovery_reports/factor_optimization/ 目录下所有元数据 JSON，返回优化历史列表。"""
    import glob

    opt_dir = _INTRADAY_REPORTS_DIR / "factor_optimization"
    if not opt_dir.exists():
        return {"items": []}

    items = []
    for meta_path in sorted(glob.glob(str(opt_dir / "*.json")), reverse=True):
        if meta_path.endswith("_latest.json") or meta_path.endswith("latest.json"):
            continue
        try:
            meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        if mode and meta.get("mode") != mode:
            continue

        recommendation = meta.get("recommendation", {})
        items.append({
            "report_path": meta.get("report_path", ""),
            "timestamp": meta.get("timestamp", ""),
            "mode": meta.get("mode", ""),
            "recommendation": recommendation,
            "changed_count": len(recommendation),
            "baseline": meta.get("baseline", {}),
            "applied": meta.get("applied", False),
        })

    return {"items": items}


@router.get(
    "/factor-backtest/optimize/report",
    summary="查询因子权重优化报告全文",
)
def factor_optimize_report(report_path: str = Query(..., description="报告路径（从 history/status 获取）")):
    """返回 Markdown 格式的优化报告全文。会校验路径必须在 factor_optimization/ 目录下。"""
    opt_dir = (_INTRADAY_REPORTS_DIR / "factor_optimization").resolve()
    rp = Path(report_path).resolve()

    # 路径包含性校验
    try:
        rp.relative_to(opt_dir)
    except ValueError:
        raise HTTPException(status_code=403, detail="报告路径不在允许的目录下")

    if not rp.exists():
        raise HTTPException(status_code=404, detail="报告文件不存在")

    return {"report_path": str(rp), "content": rp.read_text(encoding="utf-8")}
