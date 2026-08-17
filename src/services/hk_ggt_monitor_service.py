# -*- coding: utf-8 -*-
"""港股通成份监控：成份快照刷新、分钟序列查询、盘中腾讯行情轮询落库。

自动过期刷新：list_components 读取最新快照时，如果距今超过 MAX_STALE_TRADING_DAYS 个交易日，自动触发 refresh_components。"""

from __future__ import annotations

import logging
import math
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

from src.config import Config, get_config
from src.core.trading_calendar import MarketPhase, get_market_now, infer_market_phase
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

DEFAULT_MINUTE_START_DATE = "20260622"
MAX_STALE_TRADING_DAYS = 5
HK_TIMEZONE = ZoneInfo("Asia/Hong_Kong")
MAX_CONSECUTIVE_BAR_GAP_SECONDS = 120
MAX_INTRADAY_RANKING_WINDOW_SECONDS = 30 * 60
TENCENT_HK_QUOTE_ENDPOINT = "https://qt.gtimg.cn/q"
TENCENT_HK_QUOTE_BATCH_SIZE = 500
MINUTE_BOLL_PERIOD = 20
MINUTE_BOLL_MULTIPLIER = 2.0
MINUTE_BOLL_ALERT_NEAR_PCT = 0.5


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


def _fetch_tencent_hk_quotes(codes: List[str]) -> List[Dict[str, Any]]:
    """通过腾讯批量接口获取指定港股的实时行情。"""
    normalized_codes = sorted(set(filter(None, (_norm_hk_code(code) for code in codes))))
    if not normalized_codes:
        return []

    headers = {
        "Referer": "https://finance.qq.com",
        "User-Agent": "Mozilla/5.0",
    }
    quotes: List[Dict[str, Any]] = []
    for offset in range(0, len(normalized_codes), TENCENT_HK_QUOTE_BATCH_SIZE):
        batch = normalized_codes[offset:offset + TENCENT_HK_QUOTE_BATCH_SIZE]
        symbols = ",".join(f"hk{code}" for code in batch)
        try:
            response = requests.get(
                f"{TENCENT_HK_QUOTE_ENDPOINT}={symbols}",
                headers=headers,
                timeout=15,
            )
            response.raise_for_status()
            response.encoding = "gbk"
        except requests.RequestException as exc:
            logger.warning("[HkGgt] 腾讯实时行情批次请求失败 offset=%d: %s", offset, exc)
            continue

        for line in response.text.splitlines():
            if '="' not in line:
                continue
            payload = line.split('="', 1)[1].rsplit('"', 1)[0]
            fields = payload.split("~")
            if len(fields) < 38:
                continue
            code = _norm_hk_code(fields[2])
            close = _safe_float(fields[3])
            if not code or close is None or close <= 0:
                continue
            quotes.append({
                "hk_code": code,
                "close": close,
                "pre_close": _safe_float(fields[4]),
                "pct_change": _safe_float(fields[32]),
            })

    logger.info(
        "[HkGgt] 腾讯实时行情返回 %d/%d 条",
        len(quotes),
        len(normalized_codes),
    )
    return quotes


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


def _latest_ranking_window(
    valid: List[tuple[str, datetime, float]],
    window_end: Optional[datetime] = None,
) -> List[tuple[str, datetime, float]]:
    """只保留相对最新行情时点的最近 30 分钟数据。"""
    if not valid or window_end is None:
        return []
    anchor = window_end
    cutoff = anchor - timedelta(seconds=MAX_INTRADAY_RANKING_WINDOW_SECONDS)
    return [item for item in valid if cutoff <= item[1] <= anchor]


def _max_consecutive_drawdown(
    bars: List[Any],
    *,
    window_end: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """计算最新 30 分钟内连续分钟收跌区间的最大累计回撤。"""
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
    if window_end is not None:
        valid = _latest_ranking_window(valid, window_end)
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
        while (
            run_start < index
            and (current[1] - valid[run_start][1]).total_seconds()
            > MAX_INTRADAY_RANKING_WINDOW_SECONDS
        ):
            run_start += 1
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


def _max_rolling_gain(
    bars: List[Any],
    *,
    window_end: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """计算最新 30 分钟内连续分钟数据的最大区间涨幅。"""
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
    if window_end is not None:
        valid = _latest_ranking_window(valid, window_end)
    if len(valid) < 2:
        return None

    segment_start = 0
    best: Optional[Dict[str, Any]] = None
    for index in range(1, len(valid)):
        previous = valid[index - 1]
        current = valid[index]
        gap_seconds = (current[1] - previous[1]).total_seconds()
        if not 0 < gap_seconds <= MAX_CONSECUTIVE_BAR_GAP_SECONDS:
            segment_start = index
            continue
        while (
            segment_start < index
            and (current[1] - valid[segment_start][1]).total_seconds()
            > MAX_INTRADAY_RANKING_WINDOW_SECONDS
        ):
            segment_start += 1
        for start in valid[segment_start:index]:
            change_pct = (current[2] - start[2]) / start[2] * 100
            if best is None or change_pct > best["change_pct"]:
                best = {
                    "change_pct": round(change_pct, 2),
                    "start_time": start[0],
                    "end_time": current[0],
                }
    return best


def _compute_minute_boll(bars: List[Any]) -> Optional[Dict[str, Any]]:
    """计算最新一分钟的 BOLL(20,2)。"""
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
    if len(valid) < MINUTE_BOLL_PERIOD:
        return None

    window = valid[-MINUTE_BOLL_PERIOD:]
    closes = [item[2] for item in window]
    mid = sum(closes) / MINUTE_BOLL_PERIOD
    variance = sum((close - mid) ** 2 for close in closes) / MINUTE_BOLL_PERIOD
    std = math.sqrt(variance)
    latest = window[-1]
    return {
        "bar_time": latest[0],
        "close": latest[2],
        "mid": mid,
        "lower": mid - MINUTE_BOLL_MULTIPLIER * std,
    }


class HkGgtMonitorService:
    """港股通成分监控服务。"""

    def __init__(
        self,
        db: Optional[DatabaseManager] = None,
        config: Optional[Config] = None,
    ) -> None:
        self._db = db or DatabaseManager()
        self._config = config or get_config()

    def _watchlist_codes(self) -> set[str]:
        configured_hk_codes = getattr(self._config, "hk_list", None) or []
        return {
            normalized_code
            for raw_code in configured_hk_codes
            if (normalized_code := _norm_hk_code(raw_code))
        }

    def _record_minute_boll_alerts(self, trade_date: str, bar_time: str) -> int:
        watchlist_codes = self._watchlist_codes()
        if not watchlist_codes:
            return 0

        grouped = self._db.list_hk_ggt_minute_bars_batch(
            sorted(watchlist_codes),
            trade_date,
        )
        candidates: List[Dict[str, Any]] = []
        for code in sorted(watchlist_codes):
            boll = _compute_minute_boll((grouped or {}).get(code) or [])
            if not boll or boll["bar_time"] != bar_time:
                continue
            close = boll["close"]
            nearby_bands = []
            for band, band_value in (("mid", boll["mid"]), ("lower", boll["lower"])):
                if band_value <= 0:
                    continue
                distance_pct = (close - band_value) / band_value * 100
                if abs(distance_pct) > MINUTE_BOLL_ALERT_NEAR_PCT:
                    continue
                nearby_bands.append((band, band_value, distance_pct))
            if nearby_bands:
                # The middle band wins a tie because it appears first in nearby_bands.
                band, band_value, distance_pct = min(
                    nearby_bands,
                    key=lambda item: abs(item[2]),
                )
                candidates.append({
                    "trade_date": trade_date,
                    "hk_code": code,
                    "bar_time": bar_time,
                    "band": band,
                    "close": round(close, 4),
                    "band_value": round(band_value, 4),
                    "boll_mid": round(boll["mid"], 4),
                    "boll_lower": round(boll["lower"], 4),
                    "distance_pct": round(distance_pct, 2),
                    "source": "tencent_rt",
                })
        return self._db.insert_hk_minute_boll_alerts(candidates)

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

        ranking_end_time: Optional[datetime] = None
        for bars in grouped.values():
            for bar in bars or []:
                raw_time = str(getattr(bar, "bar_time", "") or "")[:19]
                try:
                    parsed_time = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                if ranking_end_time is None or parsed_time > ranking_end_time:
                    ranking_end_time = parsed_time

        items: List[Dict[str, Any]] = []
        drawdowns: List[Dict[str, Any]] = []
        gainers: List[Dict[str, Any]] = []
        ranked_codes = self._watchlist_codes()
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
            drawdown = _max_consecutive_drawdown(bars, window_end=ranking_end_time)
            if drawdown:
                item.update({
                    "intraday_consecutive_drawdown_pct": drawdown["drawdown_pct"],
                    "intraday_consecutive_drawdown_minutes": drawdown["minutes"],
                    "intraday_consecutive_drawdown_start_time": drawdown["start_time"],
                    "intraday_consecutive_drawdown_end_time": drawdown["end_time"],
                })
                if not ranked_codes or code in ranked_codes:
                    drawdowns.append(dict(item))
            minute_change = _max_rolling_gain(bars, window_end=ranking_end_time)
            if minute_change:
                item.update({
                    "minute_change_pct": minute_change["change_pct"],
                    "minute_change_start_time": minute_change["start_time"],
                    "minute_change_end_time": minute_change["end_time"],
                })
                if not ranked_codes or code in ranked_codes:
                    gainers.append(dict(item))
            items.append(item)

        drawdowns.sort(key=lambda item: item["intraday_consecutive_drawdown_pct"])
        gainers.sort(key=lambda item: item["minute_change_pct"], reverse=True)
        today_boll_alerts: List[Dict[str, Any]] = []
        if ranked_codes:
            closest_alert_by_code = {}
            for alert in self._db.list_hk_minute_boll_alerts(
                resolved_date,
                sorted(ranked_codes),
            ):
                existing = closest_alert_by_code.get(alert.hk_code)
                if existing is None or abs(alert.distance_pct) < abs(existing.distance_pct):
                    closest_alert_by_code[alert.hk_code] = alert
            # 先按时间倒序，再用稳定排序把中轨整体提到下轨之前：
            # 结果即「中轨报警优先，同轨内最新在前」。
            band_rank = {"mid": 0, "lower": 1}
            alerts_sorted = sorted(
                closest_alert_by_code.values(),
                key=lambda item: (item.bar_time, item.id),
                reverse=True,
            )
            alerts_sorted.sort(key=lambda item: band_rank.get(item.band, 2))
            for alert in alerts_sorted:
                alert_item = alert.to_dict()
                alert_item["name"] = component_by_code.get(alert.hk_code, {}).get("name")
                alert_item["band_label"] = "中轨" if alert.band == "mid" else "下轨"
                today_boll_alerts.append(alert_item)

        return {
            "trade_date": resolved_date,
            "updated_at": updated_at,
            "market_open": is_hk_ggt_poll_window(),
            "total": len(items),
            "items": items,
            "top_drawdowns": drawdowns[:5],
            "top_gainers": gainers[:5],
            "today_boll_alerts": today_boll_alerts,
        }

    def poll_rt_once(self) -> Dict[str, Any]:
        """拉取一轮腾讯港股实时快照，并按分钟落库。"""
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

        trade_date, bar_time = align_bar_time(market_now)
        quotes = _fetch_tencent_hk_quotes(list(component_codes))
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
                "source": "tencent_rt",
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
        alerts_created = 0
        try:
            alerts_created = self._record_minute_boll_alerts(trade_date, bar_time)
        except Exception as exc:
            logger.warning("[HkGgt] 分钟 BOLL 报警计算失败: %s", exc)
        return {
            "polled": True,
            "trade_date": trade_date,
            "bar_time": bar_time,
            "saved": saved,
            "matched": len(rows),
            "alerts_created": alerts_created,
        }
