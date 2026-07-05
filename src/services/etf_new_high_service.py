# -*- coding: utf-8 -*-
"""ETF 后复权收盘价创新高扫描与 K 线服务。

基于 hfq_new_high_service 架构，适配 ETF 数据。
数据来源：etf_daily 表（Tushare fund_daily 回填）。
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
from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)


def _safe_optional_float(value: Any) -> Optional[float]:
    """Coerce to float or None; NaN/inf/empty become None (JSON-safe)."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if not math.isfinite(out) else out


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


def _reset_memory_cache_for_tests() -> None:
    global _memory_cache, _boll_picks_cache, _scan_inflight
    _memory_cache = {"key": None, "payload": None, "ts": 0.0}
    _boll_picks_cache = {"key": None, "payload": None, "ts": 0.0}
    with _scan_inflight_lock:
        _scan_inflight = {}


def _parse_yyyymmdd(value: str) -> date:
    digits = "".join(c for c in str(value) if c.isdigit())[:8]
    return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))


def _fmt_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def _to_date_str(value) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    digits = "".join(c for c in str(value) if c.isdigit())[:8]
    return digits if len(digits) == 8 else ""


def lookup_adj_factor(adj_map: Dict[str, float], date_str: str) -> Optional[float]:
    """从复权因子 map 中查找指定日期的 adj_factor。"""
    if date_str in adj_map:
        f = adj_map[date_str]
        if f is not None and f > 0 and math.isfinite(f):
            return float(f)
    prev_dates = sorted(d for d in adj_map if d <= date_str and adj_map.get(d, 0) > 0)
    if prev_dates:
        f = adj_map[prev_dates[-1]]
        if f > 0 and math.isfinite(f):
            return float(f)
    return None


def scan_single_code_new_highs(
    rows: List[Tuple[str, float]],
    start_date: str,
) -> Optional[Dict[str, Any]]:
    """扫描单只 ETF 2026+ 创新高记录。rows: [(YYYYMMDD, close)] 升序。"""
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


def compute_latest_boll(
    closes: List[float],
    period: int = BOLL_PERIOD,
    mult: float = BOLL_MULT,
) -> Optional[Tuple[float, float, float, float]]:
    """Return (close, mid, lower, upper) for the last bar."""
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


def _within_drawdown_from_high_limit(
    drawdown_pct: Optional[float],
    max_drawdown_pct: float = DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT,
) -> bool:
    """距最近新高回撤不超过 max_drawdown_pct（%）；drawdown 为负表示低于新高。"""
    if drawdown_pct is None:
        return True
    return float(drawdown_pct) >= -float(max_drawdown_pct)


def _drawdown_from_high(close: float, high_close: Optional[float]) -> Optional[float]:
    if high_close is None or not math.isfinite(high_close) or high_close <= 0:
        return None
    if not math.isfinite(close):
        return None
    return round((close / high_close - 1) * 100, 2)


def _mid_slope(closes: List[float], period: int = BOLL_PERIOD, lookback: int = 3) -> Optional[float]:
    """计算 BOLL 中轨在最近 lookback 个有效值上的平均斜率（正值=上移）。"""
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


class EtfNewHighService:
    """全 ETF 收盘价创新高统计。"""

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
        cache_key = ("etf", start_date, as_of_str)

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
            logger.warning("[EtfNewHigh] inflight wait ended without cache for %s", cache_key)
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

        daily_df = self._load_etf_daily_bars(preload_dt, as_of)
        if daily_df.empty:
            payload = self._empty_payload(start_date, as_of_str)
            self._set_cache(cache_key, payload)
            return payload

        adj_by_code = self._load_adj_factors(preload_dt, as_of)
        daily_df = self._attach_hfq_close(daily_df, adj_by_code)
        daily_df = daily_df.dropna(subset=["hfq_close"])

        name_map = self._load_name_map()
        items: List[Dict[str, Any]] = []

        for code, grp in daily_df.groupby("code"):
            grp = grp.sort_values("date_str")
            rows = list(zip(grp["date_str"].tolist(), grp["hfq_close"].tolist()))
            scanned = scan_single_code_new_highs(rows, start_date)
            if not scanned:
                continue
            bare = str(code).strip().zfill(6)
            name = name_map.get(bare) or bare
            drawdown = None
            lh = scanned["latest_new_high_close"]
            cc = scanned["current_close"]
            if lh and cc and lh > 0:
                drawdown = round((cc / lh - 1) * 100, 2)
            # 剔除距新高超过 30% 的个股
            if drawdown is not None and drawdown < -NEW_HIGH_MAX_DRAWDOWN_PCT:
                continue
            items.append({
                "ts_code": bare,
                "stock_code": bare,
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
        logger.info("[EtfNewHigh] scan done: %d ETFs, as_of=%s", len(items), as_of_str)
        return payload

    def scan_boll_near_picks(
        self,
        start_date: str = DEFAULT_START_DATE,
        as_of_date: Optional[str] = None,
        refresh: bool = False,
        near_pct: float = DEFAULT_NEAR_PCT,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        max_drawdown_from_high_pct: float = DEFAULT_MAX_DRAWDOWN_FROM_HIGH_PCT,
    ) -> Dict[str, Any]:
        """近 lookback_days 日创新高且现价靠近 BOLL 中轨/下轨/上轨的 ETF。"""
        as_of = _parse_yyyymmdd(as_of_date) if as_of_date else date.today()
        as_of_str = _fmt_date(as_of)
        max_dd = round(float(max_drawdown_from_high_pct), 2)
        boll_cache_key = ("etf_boll", start_date, as_of_str, round(float(near_pct), 2), int(lookback_days), max_dd)

        if not refresh:
            cached = self._get_boll_picks_cached(boll_cache_key)
            if cached is not None:
                return cached

        new_highs = self.scan_new_highs(
            start_date=start_date,
            as_of_date=as_of_str,
            refresh=refresh,
        )
        cutoff = _fmt_date(as_of - timedelta(days=max(int(lookback_days), 1)))
        candidates = [
            it for it in new_highs.get("items", [])
            if _within_drawdown_from_high_limit(it.get("drawdown_from_high_pct"), max_dd)
        ]
        if not candidates:
            payload = self._empty_boll_picks_payload(
                start_date, as_of_str, near_pct, lookback_days, cutoff, max_dd,
            )
            self._set_boll_picks_cache(boll_cache_key, payload)
            return payload

        codes = [str(it["stock_code"]).strip().zfill(6) for it in candidates]
        by_code = {str(it["stock_code"]).strip().zfill(6): it for it in candidates}
        start_dt = as_of - timedelta(days=90)

        daily_df = self._load_etf_daily_bars(start_dt, as_of, codes=codes)
        adj_by_code = self._load_adj_factors(start_dt, as_of, codes=codes)
        daily_df = self._attach_hfq_close(daily_df, adj_by_code)
        daily_df = daily_df.dropna(subset=["hfq_close"])

        picks: List[Dict[str, Any]] = []
        for code, grp in daily_df.groupby("code"):
            bare = str(code).strip().zfill(6)
            base = by_code.get(bare)
            if not base:
                continue
            grp = grp.sort_values("date_str")
            closes = grp["hfq_close"].tolist()
            boll = compute_latest_boll(closes)
            if not boll:
                continue
            close, mid, lower, upper = boll
            dist_mid = _band_distance_pct(close, mid)
            dist_lower = _band_distance_pct(close, lower)
            if dist_mid is None or dist_lower is None:
                continue
            dist_upper = _band_distance_pct(close, upper)
            near_mid = _is_near_band(close, mid, near_pct)
            near_lower = _is_near_band(close, lower, near_pct)
            near_upper = dist_upper is not None and dist_upper >= -near_pct
            if not near_mid and not near_lower and not near_upper:
                continue
            latest_high = base.get("latest_new_high_close")
            drawdown = _drawdown_from_high(close, latest_high)
            if drawdown is None:
                drawdown = base.get("drawdown_from_high_pct")
            if not _within_drawdown_from_high_limit(drawdown, max_dd):
                continue
            zones = []
            if near_upper:
                zones.append("upper")
            if near_mid:
                zones.append("mid")
            if near_lower:
                zones.append("lower")
            band_zone = "_".join(zones)
            _sl = _mid_slope(closes)
            picks.append({
                "ts_code": bare,
                "stock_code": bare,
                "stock_name": base.get("stock_name") or bare,
                "latest_new_high_date": base["latest_new_high_date"],
                "latest_new_high_close": base.get("latest_new_high_close"),
                "current_close": round(close, 4),
                "drawdown_from_high_pct": drawdown,
                "boll_mid": round(mid, 4),
                "boll_lower": round(lower, 4),
                "boll_upper": round(upper, 4),
                "dist_mid_pct": dist_mid,
                "dist_lower_pct": dist_lower,
                "dist_upper_pct": dist_upper,
                "band_zone": band_zone,
                "mid_slope": _sl,
            })

        picks.sort(
            key=lambda x: (
                0 if (x.get("mid_slope") or 0) > 0 else 1,
                -(x.get("mid_slope") or 0),
            ),
        )
        payload = {
            "start_date": start_date,
            "as_of_date": as_of_str,
            "lookback_days": int(lookback_days),
            "near_pct": round(float(near_pct), 2),
            "max_drawdown_from_high_pct": max_dd,
            "cutoff_date": cutoff,
            "total": len(picks),
            "items": picks,
        }
        self._set_boll_picks_cache(boll_cache_key, payload)
        logger.info("[EtfNewHigh] boll picks done: %d ETFs, as_of=%s", len(picks), as_of_str)
        return payload

    def get_klines(
        self,
        etf_code: str,
        start_date: str = DEFAULT_START_DATE,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        bare = str(etf_code).split(".")[0].strip().zfill(6)
        start_dt = _parse_yyyymmdd(start_date)
        end_dt = _parse_yyyymmdd(end_date) if end_date else date.today()

        df = self._load_etf_daily_bars(start_dt, end_dt, codes=[bare])
        if df.empty:
            return []
        adj_by_code = self._load_adj_factors(start_dt, end_dt, codes=[bare])
        df = self._attach_hfq_ohlc(df, adj_by_code)
        df = df[df["date_str"] >= start_date].sort_values("date_str")

        out: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            if row.get("hfq_close") is None or not math.isfinite(row["hfq_close"]):
                continue
            out.append({
                "date": row["date_str"],
                "open": _safe_optional_float(row.get("hfq_open")),
                "high": _safe_optional_float(row.get("hfq_high")),
                "low": _safe_optional_float(row.get("hfq_low")),
                "close": _safe_optional_float(row["hfq_close"]),
                "volume": _safe_optional_float(row.get("volume")),
            })
        return out

    def _load_etf_daily_bars(
        self,
        start_dt: date,
        end_dt: date,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        sql = """
            SELECT code, date, name, open, high, low, close, volume, amount, pct_chg
            FROM etf_daily
            WHERE date >= :start_dt AND date <= :end_dt AND close IS NOT NULL
        """
        params: Dict[str, Any] = {"start_dt": start_dt, "end_dt": end_dt}
        if codes:
            sql += " AND code IN :codes"
            params["codes"] = [str(c).strip().zfill(6) for c in codes]
        stmt = text(sql)
        if codes:
            stmt = stmt.bindparams(bindparam("codes", expanding=True))
        try:
            with self.db.get_session() as session:
                rows = session.execute(stmt, params).fetchall()
        except Exception as exc:
            logger.warning("[EtfNewHigh] load etf daily failed: %s", exc)
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            d = r.date
            ds = _to_date_str(d)
            records.append({
                "code": str(r.code).strip().zfill(6),
                "date_str": ds,
                "name": str(r.name or "").strip(),
                "open": _safe_optional_float(r.open),
                "high": _safe_optional_float(r.high),
                "low": _safe_optional_float(r.low),
                "close": _safe_optional_float(r.close),
                "volume": _safe_optional_float(r.volume),
                "amount": _safe_optional_float(r.amount),
                "pct_chg": _safe_optional_float(r.pct_chg),
            })
        return pd.DataFrame(records)

    def _load_adj_factors(
        self,
        start_dt: date,
        end_dt: date,
        codes: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, float]]:
        sql = """
            SELECT code, trade_date, adj_factor
            FROM fund_adj_factor
            WHERE trade_date >= :start_dt AND trade_date <= :end_dt
              AND adj_factor IS NOT NULL AND adj_factor > 0
        """
        params: Dict[str, Any] = {"start_dt": start_dt, "end_dt": end_dt}
        stmt = text(sql)
        if codes:
            sql += " AND code IN :codes"
            params["codes"] = [str(c).strip().zfill(6) for c in codes]
            stmt = text(sql).bindparams(bindparam("codes", expanding=True))

        result: Dict[str, Dict[str, float]] = {}
        try:
            with self.db.get_session() as session:
                rows = session.execute(stmt, params).fetchall()
            for r in rows:
                code = str(r.code).strip().zfill(6)
                ds = _to_date_str(r.trade_date)
                result.setdefault(code, {})[ds] = float(r.adj_factor)
        except Exception as exc:
            logger.warning("[EtfNewHigh] load adj failed: %s", exc)
        return result

    @staticmethod
    def _attach_hfq_close(df: pd.DataFrame, adj_by_code: Dict[str, Dict[str, float]]) -> pd.DataFrame:
        hfq: List[Optional[float]] = []
        for _, row in df.iterrows():
            adj = lookup_adj_factor(adj_by_code.get(row["code"], {}), row["date_str"])
            if adj is None or row["close"] is None:
                hfq.append(None)
            else:
                hfq.append(round(row["close"] * adj, 4))
        out = df.copy()
        out["hfq_close"] = hfq
        return out

    @staticmethod
    def _attach_hfq_ohlc(df: pd.DataFrame, adj_by_code: Dict[str, Dict[str, float]]) -> pd.DataFrame:
        hfq_open: List[Optional[float]] = []
        hfq_high: List[Optional[float]] = []
        hfq_low: List[Optional[float]] = []
        hfq_close: List[Optional[float]] = []

        for _, row in df.iterrows():
            adj = lookup_adj_factor(adj_by_code.get(row["code"], {}), row["date_str"])
            if adj is None:
                hfq_open.append(None)
                hfq_high.append(None)
                hfq_low.append(None)
                hfq_close.append(None)
                continue

            def _mul(v: Optional[float]) -> Optional[float]:
                if v is None or not math.isfinite(v):
                    return None
                return round(v * adj, 4)

            hfq_open.append(_mul(row.get("open")))
            hfq_high.append(_mul(row.get("high")))
            hfq_low.append(_mul(row.get("low")))
            hfq_close.append(_mul(row.get("close")))

        out = df.copy()
        out["hfq_open"] = hfq_open
        out["hfq_high"] = hfq_high
        out["hfq_low"] = hfq_low
        out["hfq_close"] = hfq_close
        return out

    @staticmethod
    def _load_name_map() -> Dict[str, str]:
        try:
            from src.data.stock_index_loader import get_stock_name_index_map
            name_map = get_stock_name_index_map()
        except Exception:
            name_map = {}
        # Try to get names from etf_daily table as fallback
        try:
            from src.storage import DatabaseManager, EtfDaily
            from sqlalchemy import distinct

            db = DatabaseManager.get_instance()
            with db.get_session() as session:
                rows = session.query(EtfDaily.code, EtfDaily.name).distinct().all()
                for code, name in rows:
                    if name:
                        name_map[str(code).strip().zfill(6)] = name
        except Exception:
            pass
        return name_map

    @staticmethod
    def _empty_payload(start_date: str, as_of_str: str) -> Dict[str, Any]:
        return {
            "start_date": start_date,
            "as_of_date": as_of_str,
            "total": 0,
            "items": [],
        }

    @staticmethod
    def _empty_boll_picks_payload(
        start_date: str,
        as_of_str: str,
        near_pct: float,
        lookback_days: int,
        cutoff: str,
        max_drawdown_from_high_pct: float,
    ) -> Dict[str, Any]:
        return {
            "start_date": start_date,
            "as_of_date": as_of_str,
            "lookback_days": int(lookback_days),
            "near_pct": round(float(near_pct), 2),
            "max_drawdown_from_high_pct": round(float(max_drawdown_from_high_pct), 2),
            "cutoff_date": cutoff,
            "total": 0,
            "items": [],
        }

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
    def _get_boll_picks_cached(cache_key: tuple) -> Optional[Dict[str, Any]]:
        global _boll_picks_cache
        if _boll_picks_cache.get("key") != cache_key:
            return None
        if time.time() - float(_boll_picks_cache.get("ts") or 0) > _CACHE_TTL_SEC:
            return None
        return _boll_picks_cache.get("payload")

    @staticmethod
    def _set_boll_picks_cache(cache_key: tuple, payload: Dict[str, Any]) -> None:
        global _boll_picks_cache
        _boll_picks_cache = {"key": cache_key, "payload": payload, "ts": time.time()}

    @staticmethod
    def _maybe_persist_disk(as_of_str: str, payload: Dict[str, Any]) -> None:
        try:
            _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            path = _REPORTS_DIR / f"etf_new_highs_{as_of_str}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            logger.debug("[EtfNewHigh] disk cache skip: %s", exc)
