# -*- coding: utf-8 -*-
"""A 股指数新高扫描与 K 线服务。

数据来源：index_daily / index_weekly 表（Tushare index_daily / index_weekly 回填）。
支持日线和周线频率，由调用端指定。
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
    if value is None: return None
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
    if len(valid) < period: return None
    window = valid[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period
    std = math.sqrt(variance)
    lower = mid - mult * std
    upper = mid + mult * std
    close = valid[-1]
    return close, mid, lower, upper


def _band_distance_pct(close: float, band: float) -> Optional[float]:
    if not math.isfinite(close) or not math.isfinite(band) or band <= 0: return None
    return round((close - band) / band * 100, 2)


FOR_TABLE = {"daily": "index_daily", "weekly": "index_weekly"}
FOR_KLINE_TABLE = {"daily": "index_daily", "weekly": "index_weekly"}


class AIndexNewHighService:
    """A 股指数收盘价新高（支持日线/周线）。"""

    def __init__(self) -> None:
        from src.storage import DatabaseManager
        self.db = DatabaseManager.get_instance()

    def list_indices(self) -> List[Dict[str, Any]]:
        """返回所有已录入的 A 股指数列表。"""
        from src.storage import IndexBasic
        with self.db.get_session() as session:
            rows = session.query(IndexBasic).order_by(IndexBasic.ts_code).all()
            return [r.to_dict() for r in rows]

    def scan_new_highs(
        self,
        start_date: str = DEFAULT_START_DATE,
        as_of_date: Optional[str] = None,
        refresh: bool = False,
        freq: str = "daily",
    ) -> Dict[str, Any]:
        as_of = _parse_yyyymmdd(as_of_date) if as_of_date else date.today()
        as_of_str = _fmt_date(as_of)
        cache_key = ("aindex", freq, start_date, as_of_str)

        if not refresh:
            cached = self._get_cached(cache_key)
            if cached is not None: return cached

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
            if cached is not None: return cached
            return self.scan_new_highs(start_date=start_date, as_of_date=as_of_str, refresh=refresh, freq=freq)

        try:
            if not refresh:
                cached = self._get_cached(cache_key)
                if cached is not None: return cached
            return self._scan_new_highs_uncached(start_date, as_of, as_of_str, cache_key, freq)
        finally:
            with _scan_inflight_lock:
                current = _scan_inflight.pop(cache_key, None)
            if current is not None: current.set()

    def _scan_new_highs_uncached(
        self,
        start_date: str,
        as_of: date,
        as_of_str: str,
        cache_key: tuple,
        freq: str,
    ) -> Dict[str, Any]:
        start_dt = _parse_yyyymmdd(start_date)
        preload_dt = start_dt - timedelta(days=400)
        table = FOR_TABLE.get(freq, "index_daily")

        daily_df = self._load_bars(table, preload_dt, as_of)
        if daily_df.empty:
            payload = self._empty_payload(start_date, as_of_str)
            self._set_cache(cache_key, payload)
            return payload

        name_map = self._get_name_map()
        items: List[Dict[str, Any]] = []

        for ts_code, grp in daily_df.groupby("ts_code"):
            grp = grp.sort_values("date_str")
            rows = list(zip(grp["date_str"].tolist(), grp["close"].tolist()))
            scanned = self._scan_single(rows, start_date)
            if not scanned: continue
            name = name_map.get(ts_code) or ts_code
            drawdown = None
            lh, cc = scanned["latest_new_high_close"], scanned["current_close"]
            if lh and cc and lh > 0: drawdown = round((cc / lh - 1) * 100, 2)
            if drawdown is not None and drawdown < -NEW_HIGH_MAX_DRAWDOWN_PCT: continue
            items.append({
                "ts_code": ts_code, "stock_code": ts_code, "stock_name": name,
                **scanned, "drawdown_from_high_pct": drawdown,
            })

        items.sort(key=lambda x: (x["latest_new_high_date"], x["new_high_count"]), reverse=True)
        payload = {"start_date": start_date, "as_of_date": as_of_str, "total": len(items), "items": items}
        self._set_cache(cache_key, payload)
        self._maybe_persist_disk(as_of_str, freq, payload)
        logger.info("[AIndex] scan done: %d (freq=%s), as_of=%s", len(items), freq, as_of_str)
        return payload

    @staticmethod
    def _scan_single(rows: List[Tuple[str, float]], start_date: str) -> Optional[Dict[str, Any]]:
        if not rows: return None
        running_max = float("-inf")
        new_highs: List[Dict[str, Any]] = []
        current_close: Optional[float] = None
        ytd_base: Optional[float] = None
        for ds, hc in rows:
            if hc is None or not math.isfinite(hc): continue
            current_close = hc
            if ds < start_date:
                running_max = max(running_max, hc); continue
            if ytd_base is None: ytd_base = hc
            if hc >= running_max:
                new_highs.append({"date": ds, "close": round(hc, 4)}); running_max = hc
        if not new_highs: return None
        new_highs_desc = sorted(new_highs, key=lambda x: x["date"], reverse=True)
        latest = new_highs_desc[0]
        ytd_return_pct = round((current_close / ytd_base - 1) * 100, 2) if ytd_base and current_close and ytd_base > 0 else None
        return {"new_high_dates": new_highs_desc, "new_high_count": len(new_highs),
                "latest_new_high_date": latest["date"], "latest_new_high_close": latest["close"],
                "current_close": round(current_close, 4) if current_close is not None else None,
                "ytd_return_pct": ytd_return_pct}

    def get_klines(
        self,
        ts_code: str,
        start_date: str = DEFAULT_START_DATE,
        end_date: Optional[str] = None,
        freq: str = "daily",
    ) -> List[Dict[str, Any]]:
        start_dt = _parse_yyyymmdd(start_date)
        end_dt = _parse_yyyymmdd(end_date) if end_date else date.today()
        table = FOR_KLINE_TABLE.get(freq, "index_daily")
        df = self._load_bars(table, start_dt, end_dt, codes=[ts_code])
        if df.empty: return []
        df = df[df["date_str"] >= start_date].sort_values("date_str")
        out = []
        for _, row in df.iterrows():
            if row.get("close") is None or not math.isfinite(row["close"]): continue
            out.append({
                "date": row["date_str"], "open": _safe_optional_float(row.get("open")),
                "high": _safe_optional_float(row.get("high")), "low": _safe_optional_float(row.get("low")),
                "close": _safe_optional_float(row["close"]), "volume": _safe_optional_float(row.get("vol")),
            })
        return out

    def _load_bars(
        self, table: str, start_dt: date, end_dt: date, codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        id_col = {"index_daily": "ts_code", "index_weekly": "ts_code"}.get(table, "ts_code")
        sql = f"""
            SELECT ts_code, trade_date, open, high, low, close, vol
            FROM {table}
            WHERE trade_date >= :start_ds AND trade_date <= :end_ds AND close IS NOT NULL
        """
        params: Dict[str, Any] = {"start_ds": _fmt_date(start_dt), "end_ds": _fmt_date(end_dt)}
        if codes:
            placeholders = ", ".join(f":c{i}" for i in range(len(codes)))
            sql += f" AND ts_code IN ({placeholders})"
            for i, c in enumerate(codes): params[f"c{i}"] = str(c).strip()
        try:
            with self.db.get_session() as session:
                rows = session.execute(text(sql), params).fetchall()
        except Exception as exc:
            logger.warning("[AIndex] load %s failed: %s", table, exc)
            return pd.DataFrame()
        if not rows: return pd.DataFrame()
        records = []
        for r in rows:
            records.append({
                "ts_code": str(r.ts_code).strip(),
                "date_str": str(r.trade_date).strip(),
                "open": _safe_optional_float(r.open),
                "high": _safe_optional_float(r.high),
                "low": _safe_optional_float(r.low),
                "close": _safe_optional_float(r.close),
                "vol": _safe_optional_float(r.vol),
            })
        return pd.DataFrame(records)

    def _get_name_map(self) -> Dict[str, str]:
        from src.storage import IndexBasic
        try:
            with self.db.get_session() as session:
                rows = session.query(IndexBasic.ts_code, IndexBasic.name).all()
                return {r[0]: r[1] or r[0] for r in rows}
        except Exception:
            return {}

    def list_constituents(self, index_code: str) -> List[Dict[str, Any]]:
        """获取指数成分股列表（含名称和权重）。

        实时从 Tushare index_weight 接口获取，不依赖 DB。
        补全股票中文名称。
        """
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            fetcher = TushareFetcher.get_instance()
            if fetcher._api is None:
                logger.warning("[AIndex] Tushare API 未初始化")
                return []

            fetcher._check_rate_limit()
            df = fetcher._api.index_weight(index_code=index_code)
            if df is None or df.empty:
                logger.warning("[AIndex] index_weight(%s) 无数据", index_code)
                return []

            # 获取名称映射
            name_map = {}
            try:
                from src.data.stock_index_loader import get_stock_name_index_map
                name_map.update(get_stock_name_index_map())
            except Exception:
                pass

            # 解析 trade_date
            trade_dates = df["trade_date"].unique() if "trade_date" in df.columns else []
            latest_date = str(sorted(trade_dates)[-1]) if len(trade_dates) > 0 else ""

            items = []
            seen: set = set()
            for _, row in df.iterrows():
                raw_code = str(row.get("con_code", "")).strip()
                con_code = raw_code.split(".")[0].strip().zfill(6)
                if con_code in seen:
                    continue
                seen.add(con_code)
                weight = row.get("weight")
                if weight is not None:
                    try:
                        weight = float(weight)
                        if weight != weight or weight in (float("inf"), float("-inf")):
                            weight = None
                    except (TypeError, ValueError):
                        weight = None
                con_name = name_map.get(con_code, name_map.get(raw_code, ""))
                items.append({
                    "con_code": con_code,
                    "con_name": con_name or None,
                    "weight": weight,
                    "trade_date": latest_date,
                })

            # 按权重降序
            items.sort(key=lambda x: -(x["weight"] or 0))
            return items
        except Exception as exc:
            logger.warning("[AIndex] list_constituents failed for %s: %s", index_code, exc)
            return []

    def clear_non_allowed_data(self) -> Dict[str, int]:
        """删除非允许市场的指数数据（CSI/SSE/SZSE/SW 之外）。

        Returns:
            {table: deleted_count}
        """
        from src.storage import IndexBasic, IndexDaily, IndexWeekly
        ALLOWED_MARKETS = {"CSI", "SSE", "SZSE", "SW"}

        # 先查所有 index_basic 中非允许市场的 ts_code
        # market 字段可能为空字符串或 NULL，这些也属于非允许
        with self.db.get_session() as session:
            from sqlalchemy import or_
            disallowed = session.query(IndexBasic.ts_code).filter(
                or_(
                    IndexBasic.market.is_(None),
                    IndexBasic.market == "",
                    IndexBasic.market.notin_(list(ALLOWED_MARKETS)),
                )
            ).all()
            disallowed_codes = [r[0] for r in disallowed]

        if not disallowed_codes:
            return {"index_basic": 0, "index_daily": 0, "index_weekly": 0}

        import math
        counts = {}
        with self.db.get_session() as session:
            # 删除 index_daily
            for code in disallowed_codes:
                cnt = session.query(IndexDaily).filter(IndexDaily.ts_code == code).delete(synchronize_session=False)
                counts.setdefault("index_daily", 0)
                counts["index_daily"] += cnt
            # 删除 index_weekly
            for code in disallowed_codes:
                cnt = session.query(IndexWeekly).filter(IndexWeekly.ts_code == code).delete(synchronize_session=False)
                counts.setdefault("index_weekly", 0)
                counts["index_weekly"] += cnt
            # 删除 index_basic
            from sqlalchemy import or_
            cnt = session.query(IndexBasic).filter(
                or_(
                    IndexBasic.market.is_(None),
                    IndexBasic.market == "",
                    IndexBasic.market.notin_(list(ALLOWED_MARKETS)),
                )
            ).delete(synchronize_session=False)
            counts["index_basic"] = cnt
            session.commit()

        # 清理孤立的 constituent 记录
        try:
            from src.storage import IndexConstituent
            with self.db.get_session() as session:
                remaining = [r[0] for r in session.query(IndexBasic.ts_code).all()]
                kept_set = set(remaining)
                all_constituent = session.query(IndexConstituent.index_code).distinct().all()
                for (ic,) in all_constituent:
                    if ic not in kept_set:
                        session.query(IndexConstituent).filter(IndexConstituent.index_code == ic).delete(synchronize_session=False)
                session.commit()
        except Exception:
            pass

        return counts

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
        freq: str = "daily",
    ) -> Dict[str, Any]:
        """近 lookback_days 日创新高且现价靠近 BOLL 的 A 股指数。"""
        as_of = _parse_yyyymmdd(as_of_date) if as_of_date else date.today()
        as_of_str = _fmt_date(as_of)
        max_dd = round(float(max_drawdown_from_high_pct), 2)
        bck = ("aboll", freq, start_date, as_of_str, round(float(near_pct), 2), int(lookback_days), max_dd)

        if not refresh:
            cached = self._get_boll_picks_cached(bck)
            if cached is not None: return cached

        new_highs = self.scan_new_highs(start_date=start_date, as_of_date=as_of_str, refresh=refresh, freq=freq)
        cutoff = _fmt_date(as_of - timedelta(days=max(int(lookback_days), 1)))
        candidates = [
            it for it in new_highs.get("items", [])
            if it.get("drawdown_from_high_pct") is not None and it["drawdown_from_high_pct"] >= -max_dd
        ]
        if not candidates:
            return self._empty_boll_picks_payload(start_date, as_of_str, near_pct, lookback_days, cutoff, max_dd)

        table = FOR_TABLE.get(freq, "index_daily")
        codes = [str(it["stock_code"]) for it in candidates]
        by_code = {str(it["stock_code"]): it for it in candidates}
        start_dt = as_of - timedelta(days=90)
        daily_df = self._load_bars(table, start_dt, as_of, codes=codes)

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
        self._set_boll_picks_cache(bck, payload)
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
        if _memory_cache.get("key") != cache_key: return None
        if time.time() - float(_memory_cache.get("ts") or 0) > _CACHE_TTL_SEC: return None
        return _memory_cache.get("payload")

    @staticmethod
    def _set_cache(cache_key: tuple, payload: Dict[str, Any]) -> None:
        global _memory_cache
        _memory_cache = {"key": cache_key, "payload": payload, "ts": time.time()}

    @staticmethod
    def _maybe_persist_disk(as_of_str: str, freq: str, payload: Dict[str, Any]) -> None:
        try:
            _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            path = _REPORTS_DIR / f"aindex_new_highs_{freq}_{as_of_str}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            logger.debug("[AIndex] disk cache skip: %s", exc)

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
