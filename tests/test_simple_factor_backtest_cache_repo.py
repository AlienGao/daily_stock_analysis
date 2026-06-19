"""Simple factor backtest cache repository regression tests."""

import json
import os
import tempfile
from datetime import datetime, timedelta

from src.config import Config
from src.repositories.simple_factor_backtest_cache_repo import SimpleFactorBacktestCacheRepo
from src.storage import DatabaseManager, SimpleFactorBacktestCache, compute_param_fingerprint


def _request(start_date: str, end_date: str) -> dict:
    return {
        "factor_weights": {
            "institution_hold": 18.8,
            "performance": 5,
            "buyback": 5,
        },
        "start_date": start_date,
        "end_date": end_date,
        "top_n": 5,
        "hold_days": [5],
        "initial_capital": 1_000_000.0,
        "risk_free_rate": 0.02,
    }


def test_upsert_replaces_same_combo_when_only_date_range_changes() -> None:
    temp_dir = tempfile.TemporaryDirectory()
    try:
        db_path = os.path.join(temp_dir.name, "simple_factor_cache.db")
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")
        repo = SimpleFactorBacktestCacheRepo(db_manager=db)

        old_req = _request("20250101", "20251231")
        new_req = _request("20260101", "20260619")
        repo.upsert(
            compute_param_fingerprint(old_req),
            old_req,
            {"date_range": {"start": "20250101", "end": "20251231"}, "summary": {"ret": 0.1}},
        )
        old_id = repo.list_recent()[0].id

        repo.upsert(
            compute_param_fingerprint(new_req),
            new_req,
            {"date_range": {"start": "20260101", "end": "20260619"}, "summary": {"ret": 0.2}},
        )

        rows = repo.list_recent()

        assert len(rows) == 1
        assert rows[0].id == old_id
        assert rows[0].start_date == "20260101"
        assert rows[0].end_date == "20260619"
        assert repo.get_by_fingerprint(compute_param_fingerprint(old_req)) is None
        assert repo.get_by_fingerprint(compute_param_fingerprint(new_req)) is not None
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir.cleanup()


def test_list_recent_collapses_existing_same_combo_date_range_duplicates() -> None:
    temp_dir = tempfile.TemporaryDirectory()
    try:
        db_path = os.path.join(temp_dir.name, "simple_factor_cache.db")
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")
        repo = SimpleFactorBacktestCacheRepo(db_manager=db)
        old_req = _request("20250101", "20251231")
        new_req = _request("20260101", "20260619")
        old_time = datetime(2026, 6, 18, 10, 0, 0)
        new_time = old_time + timedelta(hours=1)

        with db.get_session() as session:
            session.add_all([
                SimpleFactorBacktestCache(
                    param_fingerprint=compute_param_fingerprint(old_req),
                    factor_weights_json=json.dumps(old_req["factor_weights"], ensure_ascii=False),
                    start_date=old_req["start_date"],
                    end_date=old_req["end_date"],
                    top_n=old_req["top_n"],
                    hold_days_json=json.dumps(old_req["hold_days"]),
                    initial_capital=old_req["initial_capital"],
                    risk_free_rate=old_req["risk_free_rate"],
                    result_json=json.dumps({"summary": {"ret": 0.1}}, ensure_ascii=False),
                    created_at=old_time,
                    updated_at=old_time,
                ),
                SimpleFactorBacktestCache(
                    param_fingerprint=compute_param_fingerprint(new_req),
                    factor_weights_json=json.dumps(new_req["factor_weights"], ensure_ascii=False),
                    start_date=new_req["start_date"],
                    end_date=new_req["end_date"],
                    top_n=new_req["top_n"],
                    hold_days_json=json.dumps(new_req["hold_days"]),
                    initial_capital=new_req["initial_capital"],
                    risk_free_rate=new_req["risk_free_rate"],
                    result_json=json.dumps({"summary": {"ret": 0.2}}, ensure_ascii=False),
                    created_at=new_time,
                    updated_at=new_time,
                ),
            ])
            session.commit()

        rows = repo.list_recent()

        assert len(rows) == 1
        assert rows[0].start_date == "20260101"
        assert rows[0].end_date == "20260619"
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir.cleanup()
