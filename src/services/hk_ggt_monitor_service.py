# -*- coding: utf-8 -*-
"""港股通成份监控：成份快照刷新、分钟序列查询、盘中 rt_hk_k 轮询落库。

自动过期刷新：list_components 读取最新快照时，如果距今超过 MAX_STALE_TRADING_DAYS 个交易日，自动触发 refresh_components。"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from src.config import Config, get_config
from src.core.trading_calendar import MarketPhase, get_market_now, infer_market_phase
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

DEFAULT_MINUTE_START_DATE = "20260622"
MAX_STALE_TRADING_DAYS = 5


def _norm_hk_code(code: str) -> str:
    return str(code or "").lower().replace("hk", "").zfill(5)


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
        return float(value)
    except (TypeError, ValueError):
        return None


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


