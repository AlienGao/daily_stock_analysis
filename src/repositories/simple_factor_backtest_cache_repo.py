# -*- coding: utf-8 -*-
"""Simple factor backtest cache repository."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import desc, select

from src.storage import DatabaseManager, SimpleFactorBacktestCache, compute_param_fingerprint

logger = logging.getLogger(__name__)


class SimpleFactorBacktestCacheRepo:
    """DB access layer for simple factor backtest cache."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def get_by_fingerprint(self, fingerprint: str) -> Optional[SimpleFactorBacktestCache]:
        """Return cached result or None."""
        with self.db.get_session() as session:
            return session.execute(
                select(SimpleFactorBacktestCache)
                .where(SimpleFactorBacktestCache.param_fingerprint == fingerprint)
                .limit(1)
            ).scalar_one_or_none()

    def upsert(self, fingerprint: str, req_dict: dict, result_dict: dict) -> SimpleFactorBacktestCache:
        """Insert or replace a cached result by fingerprint."""
        result_json = json.dumps(result_dict, ensure_ascii=False)
        now = datetime.now()

        with self.db.get_session() as session:
            existing = session.execute(
                select(SimpleFactorBacktestCache)
                .where(SimpleFactorBacktestCache.param_fingerprint == fingerprint)
                .limit(1)
            ).scalar_one_or_none()

            if existing:
                existing.result_json = result_json
                existing.updated_at = now
                session.commit()
                return existing

            entry = SimpleFactorBacktestCache(
                param_fingerprint=fingerprint,
                factor_weights_json=json.dumps(req_dict.get("factor_weights", {}), ensure_ascii=False),
                start_date=req_dict.get("start_date"),
                end_date=req_dict.get("end_date"),
                top_n=req_dict.get("top_n", 5),
                hold_days_json=json.dumps(sorted(req_dict.get("hold_days", []))),
                initial_capital=req_dict.get("initial_capital", 1_000_000.0),
                risk_free_rate=req_dict.get("risk_free_rate", 0.02),
                result_json=result_json,
                created_at=now,
                updated_at=now,
            )
            session.add(entry)
            session.commit()
            return entry

    def list_recent(self, limit: Optional[int] = None) -> list:
        """Return recent cache entries (without result_json), newest first."""
        with self.db.get_session() as session:
            query = (
                select(
                    SimpleFactorBacktestCache.id,
                    SimpleFactorBacktestCache.param_fingerprint,
                    SimpleFactorBacktestCache.factor_weights_json,
                    SimpleFactorBacktestCache.start_date,
                    SimpleFactorBacktestCache.end_date,
                    SimpleFactorBacktestCache.top_n,
                    SimpleFactorBacktestCache.hold_days_json,
                    SimpleFactorBacktestCache.initial_capital,
                    SimpleFactorBacktestCache.risk_free_rate,
                    SimpleFactorBacktestCache.created_at,
                    SimpleFactorBacktestCache.updated_at,
                )
                .order_by(desc(SimpleFactorBacktestCache.created_at))
            )
            if limit is not None:
                query = query.limit(limit)
            rows = session.execute(query).all()
            return list(rows)

    def get_by_id(self, cache_id: int) -> Optional[SimpleFactorBacktestCache]:
        """Return a single cache entry with full result_json."""
        with self.db.get_session() as session:
            return session.execute(
                select(SimpleFactorBacktestCache)
                .where(SimpleFactorBacktestCache.id == cache_id)
                .limit(1)
            ).scalar_one_or_none()

    def delete_by_id(self, cache_id: int) -> bool:
        """Delete a cache entry. Returns True if deleted."""
        with self.db.get_session() as session:
            entry = session.execute(
                select(SimpleFactorBacktestCache)
                .where(SimpleFactorBacktestCache.id == cache_id)
                .limit(1)
            ).scalar_one_or_none()
            if not entry:
                return False
            session.delete(entry)
            session.commit()
            return True

    def delete_by_factor_weights(self, factor_weights: dict) -> int:
        """Delete all cache entries matching the given factor_weights dict.
        
        Returns the number of rows deleted.
        """
        target_json = json.dumps(factor_weights, ensure_ascii=False)
        with self.db.get_session() as session:
            deleted = (
                session.query(SimpleFactorBacktestCache)
                .filter(SimpleFactorBacktestCache.factor_weights_json == target_json)
                .delete(synchronize_session='fetch')
            )
            session.commit()
            return deleted