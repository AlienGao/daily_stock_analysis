# -*- coding: utf-8 -*-
"""港股通成份监控：成份快照刷新、分钟序列查询、盘中 rt_hk_k 轮询落库。

自动过期刷新：list_components 读取最新快照时，如果距今超过 MAX_STALE_TRADING_DAYS 个交易日，自动触发 refresh_components。"""

from __future__ import annotations

import logging
import math
from datetime import datetime, time
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.config import Config, get_config
from src.core.trading_calendar import MarketPhase, get_market_now, infer_market_phase
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

DEFAULT_MINUTE_START_DATE = "20260622"
MAX_STALE_TRADING_DAYS = 5
HK_TIMEZONE = ZoneInfo("Asia/Hong_Kong")
MAX_CONSECUTIVE_BAR_GAP_SECONDS = 120


def _norm_hk_code(code: str) -> str:
    digits = "".join(char for char in str(code or "") if char.isdigit())
    return digits[-5:].zfill(5) if digits else ""


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        import pandas as pd

        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def align_bar_time(now: datetime) -> tuple[str, str]:
    """将行情采集时间对齐到香港时区的分钟边界。"""
    market_now = now.replace(tzinfo=HK_TIMEZONE) if now.tzinfo is None else now.astimezone(HK_TIMEZONE)
    aligned = market_now.replace(second=0, microsecond=0)
    return aligned.strftime("%Y%m%d"), aligned.strftime("%Y-%m-%d %H:%M:%S")


def is_hk_ggt_poll_window(now: Optional[datetime] = None) -> bool:
    """仅在港股交易日的连续交易及收市竞价时段采集行情。"""
    market_now = now or get_market_now("hk")
    phase = infer_market_phase("hk", current_time=market_now)
    if phase in {MarketPhase.INTRADAY, MarketPhase.CLOSING_AUCTION}:
        return True
    if phase != MarketPhase.UNKNOWN:
        return False

    local_now = (
        market_now.replace(tzinfo=HK_TIMEZONE)
        if market_now.tzinfo is None
        else market_now.astimezone(HK_TIMEZONE)
    )
    if local_now.weekday() >= 5:
        return False
    local_time = local_now.time().replace(tzinfo=None)
    return (
        time(9, 30) <= local_time <= time(12, 0)
        or time(13, 0) <= local_time <= time(16, 0)
    )


def _max_consecutive_drawdown(bars: List[Any]) -> Optional[Dict[str, Any]]:
    """计算同一交易日内连续分钟收跌区间的最大累计回撤。"""
    valid: List[tuple[str, datetime, float]] = []
    for bar in bars:
        raw_time = str(getattr(bar, "bar_time", "") or "")[:19]
        close = _safe_float(getattr(bar, "close", None))
        if not raw_time or close is None or close <= 0:
            continue
        try:
            parsed_time = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        valid.append((raw_time, parsed_time, close))
    valid.sort(key=lambda item: item[1])
    if len(valid) < 2:
        return None

    run_start: Optional[int] = None
    best: Optional[Dict[str, Any]] = None
    for index in range(1, len(valid)):
        previous = valid[index - 1]
        current = valid[index]
        gap_seconds = (current[1] - previous[1]).total_seconds()
        is_continuous_drop = (
            0 < gap_seconds <= MAX_CONSECUTIVE_BAR_GAP_SECONDS
            and current[2] < previous[2]
        )
        if not is_continuous_drop:
            run_start = None
            continue
        if run_start is None:
            run_start = index - 1
        start = valid[run_start]
        drawdown_pct = (current[2] - start[2]) / start[2] * 100
        if best is None or drawdown_pct < best["drawdown_pct"]:
            elapsed_minutes = max(1, int(round((current[1] - start[1]).total_seconds() / 60)))
            best = {
                "drawdown_pct": round(drawdown_pct, 2),
                "minutes": elapsed_minutes,
                "start_time": start[0],
                "end_time": current[0],
            }
    return best


class HkGgtMonitorService:
    """港股通成分监控服务。"""

    def __init__(
        self,
        db: Optional[DatabaseManager] = None,
        config: Optional[Config] = None,
    ) -> None:
        self._db = db or DatabaseManager()
        self._config = config or get_config()


    def resolve_trade_date(self, trade_date: Optional[str] = None) -> str:
        if trade_date:
            return str(trade_date).replace("-", "")[:8]
        return get_market_now("hk").strftime("%Y%m%d")

    def refresh_components(
        self,
        trade_date: Optional[str] = None,
        *,
        force: bool = False,
    ) -> Dict[str, Any]:
        trade_date = self.resolve_trade_date(trade_date)
        if not force:
            existing = self._db.list_hk_ggt_components(trade_date)
            if existing:
                return {
                    "trade_date": trade_date,
                    "saved": 0,
                    "total": len(existing),
                    "skipped": True,
                }

        from data_provider.akshare_fetcher import AkshareFetcher

        fetcher = AkshareFetcher()
        rows = fetcher.fetch_hk_ggt_components()
        if not rows:
            logger.warning("[HkGgt] 成份刷新无数据 trade_date=%s", trade_date)
            return {"trade_date": trade_date, "saved": 0, "total": 0, "skipped": False}

        saved = self._db.replace_hk_ggt_components(trade_date, rows)
        return {"trade_date": trade_date, "saved": saved, "total": saved, "skipped": False}

    def list_components(
        self,
        trade_date: Optional[str] = None,
        *,
        refresh: bool = False,
    ) -> Dict[str, Any]:
        latest_db_date = self._db.get_latest_hk_ggt_trade_date()
        trade_date = trade_date or latest_db_date
        if refresh and trade_date:
            self.refresh_components(trade_date, force=True)
        elif not trade_date:
            refreshed = self.refresh_components(force=True)
            trade_date = refreshed.get("trade_date")
        elif not self._db.list_hk_ggt_components(trade_date):
            self.refresh_components(trade_date, force=True)
        elif trade_date == latest_db_date and self._is_stale(trade_date):
            logger.info("[HkGgt] 成份数据过期(%s)，触发自动刷新", trade_date)
            self.refresh_components(force=True)
            trade_date = self._db.get_latest_hk_ggt_trade_date() or trade_date

        if not trade_date:
            return {"trade_date": "", "total": 0, "items": [], "available_dates": []}

        items = [row.to_dict() for row in self._db.list_hk_ggt_components(trade_date)]
        return {
            "trade_date": trade_date,
            "total": len(items),
            "items": items,
            "available_dates": self._db.list_hk_ggt_component_dates(),
        }

    def _is_stale(self, trade_date: str) -> bool:
        """检查最新快照距今是否超过 MAX_STALE_TRADING_DAYS 个自然日。"""
        try:
            dt = datetime.strptime(trade_date, "%Y%m%d").date()
            return (get_market_now("hk").date() - dt).days >= MAX_STALE_TRADING_DAYS
        except Exception as exc:
            logger.warning("[HkGgt] _is_stale 自然日判断失败: %s", exc)
            return False

    def get_minute_bars(
        self,
        hk_code: str,
        trade_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized_code = _norm_hk_code(hk_code)
        resolved_date = self.resolve_trade_date(trade_date)
        rows = self._db.list_hk_ggt_minute_bars(normalized_code, resolved_date)
        return {
            "hk_code": normalized_code,
            "trade_date": resolved_date,
            "total": len(rows),
            "items": [row.to_dict() for row in rows],
        }

    def get_realtime_snapshot(self, trade_date: Optional[str] = None) -> Dict[str, Any]:
        """返回最新分钟价及按日内连续分钟下跌计算的回撤排名。"""
        resolved_date = self.resolve_trade_date(trade_date)
        component_date = self._db.get_latest_hk_ggt_trade_date()
        components = self._db.list_hk_ggt_components(component_date) if component_date else []
        component_by_code = {
            _norm_hk_code(row.hk_code): row.to_dict()
            for row in components
        }
        codes = list(component_by_code)
        grouped_result = self._db.list_hk_ggt_minute_bars_batch(codes, resolved_date)
        grouped = grouped_result if isinstance(grouped_result, dict) else {}

        items: List[Dict[str, Any]] = []
        drawdowns: List[Dict[str, Any]] = []
        updated_at: Optional[str] = None
        for code in codes:
            bars = grouped.get(code) or []
            if not bars:
                continue
            latest = bars[-1]
            latest_price = _safe_float(getattr(latest, "close", None))
            pct_change = _safe_float(getattr(latest, "pct_change", None))
            prev_close = _safe_float(getattr(latest, "prev_close", None))
            if pct_change is None and latest_price is not None and prev_close and prev_close > 0:
                pct_change = round((latest_price - prev_close) / prev_close * 100, 2)
            bar_time = str(getattr(latest, "bar_time", "") or "")[:19]
            if bar_time and (updated_at is None or bar_time > updated_at):
                updated_at = bar_time
            item = {
                "hk_code": code,
                "name": component_by_code[code].get("name"),
                "latest_price": latest_price,
                "pct_change": pct_change,
                "bar_time": bar_time or None,
            }
            drawdown = _max_consecutive_drawdown(bars)
            if drawdown:
                item.update({
                    "intraday_consecutive_drawdown_pct": drawdown["drawdown_pct"],
                    "intraday_consecutive_drawdown_minutes": drawdown["minutes"],
                    "intraday_consecutive_drawdown_start_time": drawdown["start_time"],
                    "intraday_consecutive_drawdown_end_time": drawdown["end_time"],
                })
                drawdowns.append(dict(item))
            items.append(item)

        drawdowns.sort(key=lambda item: item["intraday_consecutive_drawdown_pct"])
        return {
            "trade_date": resolved_date,
            "updated_at": updated_at,
            "market_open": is_hk_ggt_poll_window(),
            "total": len(items),
            "items": items,
            "top_drawdowns": drawdowns[:5],
        }

    def poll_rt_once(self) -> Dict[str, Any]:
        """拉取一轮 Tushare 港股实时快照，并按分钟落库。"""
        if not bool(getattr(self._config, "hk_ggt_rt_poll_enabled", True)):
            return {"polled": False, "reason": "disabled", "saved": 0}

        market_now = get_market_now("hk")
        if not is_hk_ggt_poll_window(market_now):
            return {"polled": False, "reason": "outside_session", "saved": 0}

        component_date = self._db.get_latest_hk_ggt_trade_date()
        if not component_date:
            return {"polled": False, "reason": "no_components", "saved": 0}
        component_codes = {
            _norm_hk_code(code)
            for code in self._db.list_hk_ggt_codes_for_date(component_date)
        }
        if not component_codes:
            return {"polled": False, "reason": "no_components", "saved": 0}

        from data_provider.tushare_fetcher import TushareFetcher

        trade_date, bar_time = align_bar_time(market_now)
        quotes = TushareFetcher.get_instance().fetch_rt_hk_k()
        rows: List[Dict[str, Any]] = []
        for quote in quotes:
            code = _norm_hk_code(quote.get("hk_code"))
            close = _safe_float(quote.get("close"))
            if code not in component_codes or close is None or close <= 0:
                continue
            prev_close = _safe_float(quote.get("pre_close"))
            pct_change = _safe_float(quote.get("pct_change"))
            if pct_change is None and prev_close and prev_close > 0:
                pct_change = round((close - prev_close) / prev_close * 100, 2)
            rows.append({
                "hk_code": code,
                "trade_date": trade_date,
                "bar_time": bar_time,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "prev_close": prev_close,
                "pct_change": pct_change,
                "period": "1",
                "source": "tushare_rt",
            })
        if not rows:
            return {
                "polled": False,
                "reason": "no_data",
                "trade_date": trade_date,
                "bar_time": bar_time,
                "saved": 0,
                "matched": 0,
            }
        saved = self._db.upsert_hk_ggt_minute_bars(rows)
        return {
            "polled": True,
            "trade_date": trade_date,
            "bar_time": bar_time,
            "saved": saved,
            "matched": len(rows),
        }
