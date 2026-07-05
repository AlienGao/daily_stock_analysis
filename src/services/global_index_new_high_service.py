# -*- coding: utf-8 -*-
"""全球主要指数收盘价新高扫描与 K 线服务。

数据来源：global_index_daily 表（Tushare index_global 回填）。
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from src.services.etf_new_high_service import compute_latest_boll

logger = logging.getLogger(__name__)


DEFAULT_START_DATE = "20260101"
BOLL_PERIOD = 20
BOLL_MULT = 2.0
DEFAULT_NEAR_PCT = 2.0
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT = 30.0
NEW_HIGH_MAX_DRAWDOWN_PCT = 30.0
_CACHE_TTL_SEC = 300
_REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports_market"
_memory_cache: Dict[str, Any] = {"key": None, "payload": None, "ts": 0.0}
_boll_picks_cache: Dict[str, Any] = {"key": None, "payload": None, "ts": 0.0}
_scan_inflight_lock = threading.Lock()
_scan_inflight: Dict[tuple, threading.Event] = {}
_SCAN_INFLIGHT_WAIT_SEC = 600.0


def _safe_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if not math.isfinite(out) else out


def _parse_yyyymmdd(value: str) -> date:
    digits = "".join(c for c in str(value) if c.isdigit())[:8]
    return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))


def _fmt_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def compute_latest_boll(
    closes: List[float],
    period: int = BOLL_PERIOD,
    mult: float = BOLL_MULT,
) -> Optional[Tuple[float, float, float, float]]:
    valid = [float(c) for c in closes if c is not None and math.isfinite(c)]
    if len(valid) < period:
        return None
    window = valid[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period
    std = math.sqrt(variance)
    lower = mid - mult * std
    upper = mid + mult * std
    close = valid[-1]
    return close, mid, lower, upper


def _band_distance_pct(close: float, band: float) -> Optional[float]:
    if not math.isfinite(close) or not math.isfinite(band) or band <= 0:
        return None
    return round((close - band) / band * 100, 2)


def _is_near_band(close: float, band: float, near_pct: float) -> bool:
    dist = _band_distance_pct(close, band)
    return dist is not None and abs(dist) <= near_pct


def _drawdown_from_high(close: float, high_close: Optional[float]) -> Optional[float]:
    if high_close is None or not math.isfinite(high_close) or high_close <= 0:
        return None
    if not math.isfinite(close):
        return None
    return round((close / high_close - 1) * 100, 2)


def _mid_slope(closes: List[float], period: int = BOLL_PERIOD, lookback: int = 3) -> Optional[float]:
    valid = [float(c) for c in closes if c is not None and math.isfinite(c)]
    if len(valid) < period + lookback - 1:
        return None
    mids = []
    for i in range(len(valid) - lookback + 1, len(valid) + 1):
        if i < period:
            return None
        window = valid[i - period:i]
        mids.append(sum(window) / period)
    if len(mids) < 2:
        return None
    slope = (mids[-1] - mids[0]) / (len(mids) - 1)
    return round(slope, 4)


class GlobalIndexNewHighService:
    """全球主要指数收盘价新高统计。"""

    def __init__(self) -> None:
        from src.storage import DatabaseManager
        self.db = DatabaseManager.get_instance()

    def scan_new_highs(
        self,
        start_date: str = DEFAULT_START_DATE,
        as_of_date: Optional[str] = None,
        refresh: bool = False,
    ) -> Dict[str, Any]:
        as_of = _parse_yyyymmdd(as_of_date) if as_of_date else date.today()
        as_of_str = _fmt_date(as_of)
        cache_key = ("global_index", start_date, as_of_str)

        if not refresh:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        wait_event: Optional[threading.Event] = None
        is_leader = False
        with _scan_inflight_lock:
            existing = _scan_inflight.get(cache_key)
            if existing is not None:
                wait_event = existing
            else:
                wait_event = threading.Event()
                _scan_inflight[cache_key] = wait_event
                is_leader = True

        if not is_leader:
            wait_event.wait(timeout=_SCAN_INFLIGHT_WAIT_SEC)
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached
            logger.warning("[GlobalIndex] inflight wait ended without cache for %s", cache_key)
            return self.scan_new_highs(start_date=start_date, as_of_date=as_of_str, refresh=refresh)

        try:
            if not refresh:
                cached = self._get_cached(cache_key)
                if cached is not None:
                    return cached
            return self._scan_new_highs_uncached(start_date, as_of, as_of_str, cache_key)
        finally:
            with _scan_inflight_lock:
                current = _scan_inflight.pop(cache_key, None)
            if current is not None:
                current.set()

    def _scan_new_highs_uncached(
        self,
        start_date: str,
        as_of: date,
        as_of_str: str,
        cache_key: tuple,
    ) -> Dict[str, Any]:
        start_dt = _parse_yyyymmdd(start_date)
        preload_dt = start_dt - timedelta(days=400)

        daily_df = self._load_global_daily_bars(preload_dt, as_of)
        if daily_df.empty:
            payload = self._empty_payload(start_date, as_of_str)
            self._set_cache(cache_key, payload)
            return payload

        items: List[Dict[str, Any]] = []

        for ts_code, grp in daily_df.groupby("ts_code"):
            grp = grp.sort_values("date_str")
            rows = list(zip(grp["date_str"].tolist(), grp["close"].tolist()))
            scanned = self._scan_single_code_new_highs(rows, start_date)
            if not scanned:
                continue
            name = grp["name"].iloc[0] if "name" in grp.columns and grp["name"].iloc[0] else ts_code
            drawdown = None
            lh = scanned["latest_new_high_close"]
            cc = scanned["current_close"]
            if lh and cc and lh > 0:
                drawdown = round((cc / lh - 1) * 100, 2)
            if drawdown is not None and drawdown < -NEW_HIGH_MAX_DRAWDOWN_PCT:
                continue
            items.append({
                "ts_code": ts_code,
                "stock_code": ts_code,
                "stock_name": name,
                **scanned,
                "drawdown_from_high_pct": drawdown,
            })

        items.sort(
            key=lambda x: (x["latest_new_high_date"], x["new_high_count"]),
            reverse=True,
        )
        payload = {
            "start_date": start_date,
            "as_of_date": as_of_str,
            "total": len(items),
            "items": items,
        }
        self._set_cache(cache_key, payload)
        self._maybe_persist_disk(as_of_str, payload)
        logger.info("[GlobalIndex] scan done: %d indices, as_of=%s", len(items), as_of_str)
        return payload

    @staticmethod
    def _scan_single_code_new_highs(
        rows: List[Tuple[str, float]],
        start_date: str,
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None
        running_max = float("-inf")
        new_highs: List[Dict[str, Any]] = []
        current_close: Optional[float] = None
        ytd_base: Optional[float] = None

        for ds, hc in rows:
            if hc is None or not math.isfinite(hc):
                continue
            current_close = hc
            if ds < start_date:
                running_max = max(running_max, hc)
                continue
            if ytd_base is None:
                ytd_base = hc
            if hc >= running_max:
                new_highs.append({"date": ds, "close": round(hc, 4)})
                running_max = hc

        if not new_highs:
            return None

        new_highs_desc = sorted(new_highs, key=lambda x: x["date"], reverse=True)
        latest = new_highs_desc[0]
        ytd_return_pct: Optional[float] = None
        if ytd_base is not None and current_close is not None and ytd_base > 0:
            ytd_return_pct = round((current_close / ytd_base - 1) * 100, 2)
        return {
            "new_high_dates": new_highs_desc,
            "new_high_count": len(new_highs),
            "latest_new_high_date": latest["date"],
            "latest_new_high_close": latest["close"],
            "current_close": round(current_close, 4) if current_close is not None else None,
            "ytd_return_pct": ytd_return_pct,
        }

    def _load_global_daily_bars(
        self,
        start_dt: date,
        end_dt: date,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        sql = """
            SELECT ts_code, trade_date, name, close
            FROM global_index_daily
            WHERE trade_date >= :start_date_str AND trade_date <= :end_date_str AND close IS NOT NULL
        """
        params: Dict[str, Any] = {
            "start_date_str": _fmt_date(start_dt),
            "end_date_str": _fmt_date(end_dt),
        }
        if codes:
            placeholders = ", ".join(f":c{i}" for i in range(len(codes)))
            sql += f" AND ts_code IN ({placeholders})"
            for i, c in enumerate(codes):
                params[f"c{i}"] = str(c).strip()

        try:
            with self.db.get_session() as session:
                rows = session.execute(text(sql), params).fetchall()
        except Exception as exc:
            logger.warning("[GlobalIndex] load failed: %s", exc)
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            records.append({
                "ts_code": str(r.ts_code).strip(),
                "date_str": str(r.trade_date).strip(),
                "name": str(r.name or "").strip(),
                "close": _safe_optional_float(r.close),
            })
        return pd.DataFrame(records)

    def _load_global_ohlc_bars(
        self,
        start_dt: date,
        end_dt: date,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        sql = """
            SELECT ts_code, trade_date, name, open, high, low, close, vol
            FROM global_index_daily
            WHERE trade_date >= :start_date_str AND trade_date <= :end_date_str AND close IS NOT NULL
        """
        params: Dict[str, Any] = {
            "start_date_str": _fmt_date(start_dt),
            "end_date_str": _fmt_date(end_dt),
        }
        if codes:
            placeholders = ", ".join(f":c{i}" for i in range(len(codes)))
            sql += f" AND ts_code IN ({placeholders})"
            for i, c in enumerate(codes):
                params[f"c{i}"] = str(c).strip()

        try:
            with self.db.get_session() as session:
                rows = session.execute(text(sql), params).fetchall()
        except Exception as exc:
            logger.warning("[GlobalIndex] load ohlc failed: %s", exc)
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            records.append({
                "ts_code": str(r.ts_code).strip(),
                "date_str": str(r.trade_date).strip(),
                "name": str(r.name or "").strip(),
                "open": _safe_optional_float(r.open),
                "high": _safe_optional_float(r.high),
                "low": _safe_optional_float(r.low),
                "close": _safe_optional_float(r.close),
                "vol": _safe_optional_float(r.vol),
            })
        return pd.DataFrame(records)

    def get_klines(
        self,
        ts_code: str,
        start_date: str = DEFAULT_START_DATE,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        start_dt = _parse_yyyymmdd(start_date)
        end_dt = _parse_yyyymmdd(end_date) if end_date else date.today()
        df = self._load_global_ohlc_bars(start_dt, end_dt, codes=[ts_code])
        if df.empty:
            return []
        df = df[df["date_str"] >= start_date].sort_values("date_str")
        out: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            if row.get("close") is None or not math.isfinite(row["close"]):
                continue
            out.append({
                "date": row["date_str"],
                "open": _safe_optional_float(row.get("open")),
                "high": _safe_optional_float(row.get("high")),
                "low": _safe_optional_float(row.get("low")),
                "close": _safe_optional_float(row["close"]),
                "volume": _safe_optional_float(row.get("vol")),
            })
        return out

    @staticmethod
    def _empty_payload(start_date: str, as_of_str: str) -> Dict[str, Any]:
        return {"start_date": start_date, "as_of_date": as_of_str, "total": 0, "items": []}

    def scan_boll_near_picks(
        self,
        start_date: str = DEFAULT_START_DATE,
        as_of_date: Optional[str] = None,
        refresh: bool = False,
        near_pct: float = DEFAULT_NEAR_PCT,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        max_drawdown_from_high_pct: float = DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT,
    ) -> Dict[str, Any]:
        """近 lookback_days 日创新高且现价靠近 BOLL 的全球指数。"""
        as_of = _parse_yyyymmdd(as_of_date) if as_of_date else date.today()
        as_of_str = _fmt_date(as_of)
        max_dd = round(float(max_drawdown_from_high_pct), 2)
        boll_cache_key = ("gboll", start_date, as_of_str, round(float(near_pct), 2), int(lookback_days), max_dd)

        if not refresh:
            cached = self._get_boll_picks_cached(boll_cache_key)
            if cached is not None: return cached

        new_highs = self.scan_new_highs(start_date=start_date, as_of_date=as_of_str, refresh=refresh)
        cutoff = _fmt_date(as_of - timedelta(days=max(int(lookback_days), 1)))
        candidates = [
            it for it in new_highs.get("items", [])
            if it.get("drawdown_from_high_pct") is not None and it["drawdown_from_high_pct"] >= -max_dd
        ]
        if not candidates:
            return self._empty_boll_picks_payload(start_date, as_of_str, near_pct, lookback_days, cutoff, max_dd)

        codes = [str(it["stock_code"]) for it in candidates]
        by_code = {str(it["stock_code"]): it for it in candidates}
        start_dt = as_of - timedelta(days=90)
        daily_df = self._load_global_daily_bars(start_dt, as_of, codes=codes)

        picks: List[Dict[str, Any]] = []
        for ts_code, grp in daily_df.groupby("ts_code"):
            code = str(ts_code).strip()
            base = by_code.get(code)
            if not base: continue
            grp = grp.sort_values("date_str")
            closes = grp["close"].tolist()
            boll = compute_latest_boll(closes)
            if not boll: continue
            close, mid, lower, upper = boll
            if lower <= 0: continue
            dm = _band_distance_pct(close, mid)
            dl = _band_distance_pct(close, lower)
            if dm is None or dl is None: continue
            du = _band_distance_pct(close, upper)
            nm = dm is not None and abs(dm) <= near_pct
            nl = dl is not None and abs(dl) <= near_pct
            nu = du is not None and du >= -near_pct
            if not nm and not nl and not nu: continue
            zones = []
            if nu: zones.append("upper")
            if nm: zones.append("mid")
            if nl: zones.append("lower")
            picks.append({
                "ts_code": code, "stock_code": code, "stock_name": base.get("stock_name") or code,
                "latest_new_high_date": base["latest_new_high_date"],
                "latest_new_high_close": base.get("latest_new_high_close"),
                "current_close": round(close, 4),
                "drawdown_from_high_pct": base.get("drawdown_from_high_pct"),
                "boll_mid": round(mid, 4), "boll_lower": round(lower, 4), "boll_upper": round(upper, 4),
                "dist_mid_pct": dm, "dist_lower_pct": dl, "dist_upper_pct": du,
                "band_zone": "_".join(zones),
            })

        payload = {
            "start_date": start_date, "as_of_date": as_of_str,
            "lookback_days": int(lookback_days), "near_pct": round(float(near_pct), 2),
            "max_drawdown_from_high_pct": max_dd, "cutoff_date": cutoff,
            "total": len(picks), "items": picks,
        }
        self._set_boll_picks_cache(boll_cache_key, payload)
        return payload

    @staticmethod
    def _empty_boll_picks_payload(
        start_date: str, as_of_str: str, near_pct: float, lookback_days: int, cutoff: str, max_dd: float,
    ) -> Dict[str, Any]:
        return {"start_date": start_date, "as_of_date": as_of_str, "lookback_days": int(lookback_days),
                "near_pct": round(float(near_pct), 2), "max_drawdown_from_high_pct": round(float(max_dd), 2),
                "cutoff_date": cutoff, "total": 0, "items": []}

    @staticmethod
    def _get_cached(cache_key: tuple) -> Optional[Dict[str, Any]]:
        global _memory_cache
        if _memory_cache.get("key") != cache_key:
            return None
        if time.time() - float(_memory_cache.get("ts") or 0) > _CACHE_TTL_SEC:
            return None
        return _memory_cache.get("payload")

    @staticmethod
    def _set_cache(cache_key: tuple, payload: Dict[str, Any]) -> None:
        global _memory_cache
        _memory_cache = {"key": cache_key, "payload": payload, "ts": time.time()}

    @staticmethod
    def _maybe_persist_disk(as_of_str: str, payload: Dict[str, Any]) -> None:
        try:
            _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            path = _REPORTS_DIR / f"global_index_new_highs_{as_of_str}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            logger.debug("[GlobalIndex] disk cache skip: %s", exc)

    @staticmethod
    def _get_boll_picks_cached(cache_key: tuple) -> Optional[Dict[str, Any]]:
        global _boll_picks_cache
        if _boll_picks_cache.get("key") != cache_key: return None
        if time.time() - float(_boll_picks_cache.get("ts") or 0) > _CACHE_TTL_SEC: return None
        return _boll_picks_cache.get("payload")

    @staticmethod
    def _set_boll_picks_cache(cache_key: tuple, payload: Dict[str, Any]) -> None:
        global _boll_picks_cache
        _boll_picks_cache = {"key": cache_key, "payload": payload, "ts": time.time()}
