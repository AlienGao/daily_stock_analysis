# -*- coding: utf-8 -*-
"""Simple factor backtest cache repository."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import desc, select

from src.storage import DatabaseManager, SimpleFactorBacktestCache, compute_param_fingerprint

logger = logging.getLogger(__name__)


class SimpleFactorBacktestCacheRepo:
    """DB access layer for simple factor backtest cache."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    @staticmethod
    def _factor_weights_json(req_dict: dict) -> str:
        return json.dumps(
            dict(sorted((req_dict.get("factor_weights") or {}).items())),
            ensure_ascii=False,
        )

    @staticmethod
    def _hold_days_json(req_dict: dict) -> str:
        return json.dumps(sorted(req_dict.get("hold_days") or []))

    @staticmethod
    def _combo_signature_from_values(
        factor_weights_json: str,
        top_n: int,
        hold_days_json: str,
        initial_capital: float,
        risk_free_rate: float,
    ) -> tuple:
        try:
            factor_weights = dict(sorted(json.loads(factor_weights_json or "{}").items()))
        except Exception:
            factor_weights = {}
        try:
            hold_days = sorted(json.loads(hold_days_json or "[]"))
        except Exception:
            hold_days = []
        return (
            tuple(factor_weights.items()),
            int(top_n or 5),
            tuple(hold_days),
            float(initial_capital if initial_capital is not None else 1_000_000.0),
            float(risk_free_rate if risk_free_rate is not None else 0.02),
        )

    @classmethod
    def _combo_signature_from_request(cls, req_dict: dict) -> tuple:
        return cls._combo_signature_from_values(
            cls._factor_weights_json(req_dict),
            req_dict.get("top_n", 5),
            cls._hold_days_json(req_dict),
            req_dict.get("initial_capital", 1_000_000.0),
            req_dict.get("risk_free_rate", 0.02),
        )

    @classmethod
    def _combo_signature_from_row(cls, row) -> tuple:
        return cls._combo_signature_from_values(
            row.factor_weights_json,
            row.top_n,
            row.hold_days_json,
            row.initial_capital,
            row.risk_free_rate,
        )

    def get_by_fingerprint(self, fingerprint: str) -> Optional[SimpleFactorBacktestCache]:
        """Return cached result or None."""
        with self.db.get_session() as session:
            return session.execute(
                select(SimpleFactorBacktestCache)
                .where(SimpleFactorBacktestCache.param_fingerprint == fingerprint)
                .limit(1)
            ).scalar_one_or_none()

    def upsert(self, fingerprint: str, req_dict: dict, result_dict: dict) -> SimpleFactorBacktestCache:
        """Insert or replace a cached result.

        The page treats cache entries as "history records". For that history,
        date range changes should update the same combo instead of creating a
        duplicate row, so the replacement key intentionally ignores
        start_date/end_date while still storing the latest selected range.
        """
        result_json = json.dumps(result_dict, ensure_ascii=False)
        now = datetime.now()
        factor_weights_json = self._factor_weights_json(req_dict)
        hold_days_json = self._hold_days_json(req_dict)
        combo_signature = self._combo_signature_from_request(req_dict)

        with self.db.get_session() as session:
            existing = session.execute(
                select(SimpleFactorBacktestCache)
                .where(SimpleFactorBacktestCache.param_fingerprint == fingerprint)
                .limit(1)
            ).scalar_one_or_none()
            same_combo_entries = [
                entry
                for entry in session.execute(
                    select(SimpleFactorBacktestCache)
                    .where(SimpleFactorBacktestCache.top_n == req_dict.get("top_n", 5))
                    .order_by(desc(SimpleFactorBacktestCache.updated_at))
                ).scalars()
                if self._combo_signature_from_row(entry) == combo_signature
            ]

            if existing is None and same_combo_entries:
                existing = same_combo_entries[0]

            if existing:
                for entry in same_combo_entries:
                    if entry.id != existing.id:
                        session.delete(entry)
                existing.param_fingerprint = fingerprint
                existing.factor_weights_json = factor_weights_json
                existing.start_date = req_dict.get("start_date")
                existing.end_date = req_dict.get("end_date")
                existing.top_n = req_dict.get("top_n", 5)
                existing.hold_days_json = hold_days_json
                existing.initial_capital = req_dict.get("initial_capital", 1_000_000.0)
                existing.risk_free_rate = req_dict.get("risk_free_rate", 0.02)
                existing.result_json = result_json
                existing.updated_at = now
                session.commit()
                return existing

            entry = SimpleFactorBacktestCache(
                param_fingerprint=fingerprint,
                factor_weights_json=factor_weights_json,
                start_date=req_dict.get("start_date"),
                end_date=req_dict.get("end_date"),
                top_n=req_dict.get("top_n", 5),
                hold_days_json=hold_days_json,
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
        """Return recent cache entries (without result_json), newest first.

        Existing databases may already contain duplicate rows for the same
        combo with different date ranges. Collapse them for the history list so
        the newest row is the single visible record.
        """
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
                .order_by(desc(SimpleFactorBacktestCache.updated_at))
            )
            rows = session.execute(query).all()
            deduped = []
            seen = set()
            for row in rows:
                signature = self._combo_signature_from_row(row)
                if signature in seen:
                    continue
                seen.add(signature)
                deduped.append(row)
                if limit is not None and len(deduped) >= limit:
                    break
            return deduped

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