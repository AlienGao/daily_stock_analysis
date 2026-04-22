# -*- coding: utf-8 -*-
"""Smoke tests for the manual "run-full" analysis endpoint."""

from __future__ import annotations

import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

try:
    from fastapi.testclient import TestClient
    from api.app import create_app
    from api.v1.endpoints import analysis as analysis_endpoint
except Exception:  # pragma: no cover - optional deps
    TestClient = None
    create_app = None
    analysis_endpoint = None


class RunFullAnalysisEndpointTestCase(unittest.TestCase):
    def setUp(self) -> None:
        if create_app is None or TestClient is None or analysis_endpoint is None:
            self.skipTest("FastAPI test environment unavailable")

        # Reset module-level state between tests.
        analysis_endpoint._FULL_ANALYSIS_STATE = {
            "status": "idle",
            "started_at": None,
            "completed_at": None,
            "stock_count": 0,
            "message": None,
            "error": None,
        }

        self.app = create_app()
        self.client = TestClient(self.app)

    def test_status_endpoint_reports_idle_initially(self) -> None:
        resp = self.client.get("/api/v1/analysis/run-full/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "idle")
        self.assertIsNone(data["started_at"])
        self.assertIsNone(data["completed_at"])

    def test_trigger_run_full_launches_background_job_and_completes(self) -> None:
        recorded: dict = {}

        def fake_run_full_analysis(config, args, stock_codes=None):  # noqa: ARG001
            recorded["stock_codes"] = stock_codes
            recorded["no_notify"] = args.no_notify
            recorded["no_market_review"] = args.no_market_review
            recorded["force_run"] = args.force_run

        fake_config = SimpleNamespace(
            stock_list=["600519", "000001"],
            refresh_stock_list=lambda: None,
        )

        with patch("main.run_full_analysis", side_effect=fake_run_full_analysis, create=True), \
             patch("src.config.get_config", return_value=fake_config):
            resp = self.client.post(
                "/api/v1/analysis/run-full",
                json={"no_notify": True, "no_market_review": False, "force_run": True},
            )
            self.assertEqual(resp.status_code, 202)
            body = resp.json()
            self.assertEqual(body["status"], "running")
            self.assertIsNotNone(body["started_at"])

            # Wait briefly for the background thread to finish (in-memory stub).
            deadline = time.time() + 3.0
            while time.time() < deadline:
                status = self.client.get("/api/v1/analysis/run-full/status").json()
                if status["status"] in {"completed", "failed"}:
                    break
                time.sleep(0.05)

            final = self.client.get("/api/v1/analysis/run-full/status").json()
            self.assertEqual(final["status"], "completed", f"unexpected state: {final}")
            self.assertEqual(final["stock_count"], 2)
            self.assertIsNotNone(final["completed_at"])

        # Verify args propagated correctly into the pipeline stub.
        self.assertIsNone(recorded.get("stock_codes"))
        self.assertTrue(recorded.get("no_notify"))
        self.assertFalse(recorded.get("no_market_review"))
        self.assertTrue(recorded.get("force_run"))

    def test_trigger_run_full_is_serialized(self) -> None:
        # Simulate a running job already in progress.
        analysis_endpoint._FULL_ANALYSIS_STATE = {
            "status": "running",
            "started_at": "2026-04-22T10:00:00",
            "completed_at": None,
            "stock_count": 10,
            "message": "running",
            "error": None,
        }

        resp = self.client.post("/api/v1/analysis/run-full", json={})
        self.assertEqual(resp.status_code, 409)
        body = resp.json()
        # FastAPI may wrap the detail dict under `detail`, or surface it at top
        # level depending on custom exception handlers — accept either shape.
        detail = body.get("detail") if isinstance(body.get("detail"), dict) else body
        self.assertEqual(detail.get("error"), "busy")


if __name__ == "__main__":
    unittest.main()
