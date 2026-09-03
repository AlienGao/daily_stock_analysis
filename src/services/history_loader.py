"""DB-first K-line history loader for Agent tools.

Provides:
- ContextVar-based frozen target_date propagation across threads
- ``load_history_df``: read from DB first, DataFetcherManager fallback

Fixes #1066 – eliminates 45+ redundant HTTP requests per stock in Agent mode.
"""
from __future__ import annotations

import contextvars
import logging
from datetime import date, datetime, timedelta
from threading import Lock
from typing import Any, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)
_CACHE_MIN_RECORDS = 30

# ---------------------------------------------------------------------------
# Frozen target date (ContextVar) – set once per stock in pipeline, read by
# all agent tool threads via copy_context().run().
# ---------------------------------------------------------------------------
_frozen_target_date: contextvars.ContextVar[Optional[date]] = contextvars.ContextVar(
    "_frozen_target_date", default=None,
)


def set_frozen_target_date(d: date) -> contextvars.Token:
    return _frozen_target_date.set(d)


def get_frozen_target_date() -> Optional[date]:
    return _frozen_target_date.get()


def reset_frozen_target_date(token: contextvars.Token) -> None:
    _frozen_target_date.reset(token)


# ---------------------------------------------------------------------------
# Internal DataFetcherManager singleton (fallback only)
# ---------------------------------------------------------------------------
_fetcher_singleton = None
_fetcher_lock = Lock()


def _get_fetcher_manager():
    global _fetcher_singleton
    if _fetcher_singleton is None:
        with _fetcher_lock:
            if _fetcher_singleton is None:
                from data_provider import DataFetcherManager
                _fetcher_singleton = DataFetcherManager()
    return _fetcher_singleton


# ---------------------------------------------------------------------------
# DB-first history loader
# ---------------------------------------------------------------------------
def _history_code_candidates(stock_code: str) -> Tuple[List[str], str]:
    from data_provider.base import canonical_stock_code, normalize_stock_code
    from src.services.stock_list_parser import ParseStatus, parse_analysis_target

    raw_code = str(stock_code or "").strip()
    target = parse_analysis_target(raw_code)
    if target.asset_type == ParseStatus.INDEX:
        # Explicit index identities keep their canonical bucket (``sh000016``
        # / ``csi930955``) so index bars never land in the colliding stock
        # bucket (Story 1.5).
        return [target.canonical_id], target.canonical_id
    normalized_code = canonical_stock_code(normalize_stock_code(raw_code))
    candidates: List[str] = []
    for candidate in (canonical_stock_code(raw_code), normalized_code):
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates, normalized_code


def _coerce_bar_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return date.min
    if hasattr(value, "date"):
        try:
            coerced = value.date()
            return coerced if isinstance(coerced, date) else date.min
        except Exception:
            return date.min
    return date.min


def _bar_date(bar: Any) -> date:
    row_date = _coerce_bar_date(getattr(bar, "date", None))
    if row_date != date.min:
        return row_date
    if hasattr(bar, "to_dict"):
        try:
            return _coerce_bar_date((bar.to_dict() or {}).get("date"))
        except Exception:
            return date.min
    return date.min


def _select_best_bars(db, stock_code: str, start: date, end: date) -> Tuple[Optional[str], list]:
    candidates, normalized_code = _history_code_candidates(stock_code)
    best_code = None
    best_bars = []
    best_key = None

    for candidate in candidates:
        bars = list(db.get_data_range(candidate, start, end) or [])
        if not bars:
            continue
        latest_date = max(_bar_date(bar) for bar in bars)
        key = (latest_date, len(bars), candidate == normalized_code)
        if best_key is None or key > best_key:
            best_key = key
            best_code = candidate
            best_bars = bars

    return best_code, best_bars


def load_history_df(
    stock_code: str,
    days: int = 60,
    target_date: Optional[date] = None,
    start_date: Optional[date] = None,
) -> Tuple[Optional[pd.DataFrame], str]:
    """Load K-line history, DB first with DataFetcherManager fallback.

    Returns ``(df, source)`` where *source* is ``"db_cache"`` on DB hit or the
    actual provider name on network fallback.  Returns ``(None, "none")`` when
    both paths fail.
    """
    from src.storage import get_db

    # Resolve effective end date
    if target_date is not None:
        end = target_date
    else:
        frozen = get_frozen_target_date()
        end = frozen if frozen else date.today()

    if start_date is not None:
        start = start_date
        span_calendar = max((end - start).days + 1, 1)
        required_records = max(_CACHE_MIN_RECORDS, int(span_calendar * 0.45))
    else:
        # Calendar-day buffer: ~1.8x trading days + margin for long holidays
        start = end - timedelta(days=int(days * 1.8) + 10)
        required_records = max(_CACHE_MIN_RECORDS, min(days, max(int(days * 0.6), 1)))

    start_slack = timedelta(days=7)
    range_query = start_date is not None

    # 显式区间（如金股近 6 月）不走 DB 快捷路径，避免只命中近月残缺缓存
    if not range_query:
        try:
            db = get_db()
            _code, bars = _select_best_bars(db, stock_code, start, end)
            latest_date = max((_bar_date(bar) for bar in bars), default=date.min)
            earliest_date = min((_bar_date(bar) for bar in bars), default=date.max)
            if (
                bars
                and latest_date >= end
                and earliest_date <= start + start_slack
                and len(bars) >= required_records
            ):
                df = pd.DataFrame([b.to_dict() for b in bars])
                logger.debug(
                    "load_history_df(%s): %d bars from DB [%s..%s], requested %d",
                    stock_code, len(df), start, end, days,
                )
                return df, "db_cache"
        except Exception as e:
            logger.debug("load_history_df(%s): DB read failed: %s", stock_code, e)

    fetch_days = days
    if range_query:
        fetch_days = max(days, int(span_calendar * 0.65) + 10)

    try:
        manager = _get_fetcher_manager()
        df, source = manager.get_daily_data(
            stock_code,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            days=fetch_days,
        )
        if df is not None and not df.empty:
            if range_query and "date" in df.columns:
                dt = pd.to_datetime(df["date"], errors="coerce")
                mask = (dt.dt.date >= start) & (dt.dt.date <= end)
                df = df.loc[mask].copy()
            if not df.empty:
                return df, source
    except Exception as e:
        logger.warning("load_history_df(%s): DataFetcherManager failed: %s", stock_code, e)

    return None, "none"
