# -*- coding: utf-8 -*-
"""API contract tests for intelligence source endpoints."""

from __future__ import annotations

import os
import requests
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from api.app import create_app
from src.config import Config
from src.storage import DatabaseManager

RSS_FIXTURE = b'<?xml version="1.0" encoding="UTF-8"?>\n<rss version="2.0"><channel><item><title>Market event</title><link>https://news.example.com/market-event</link><description>Evidence summary</description></item></channel></rss>'


class IntelligenceApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self._temp_dir.name, "api_intel.db")
        Config._instance = None
        DatabaseManager.reset_instance()
        self.client = TestClient(create_app(static_dir=Path(self._temp_dir.name)))

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config._instance = None
        os.environ.pop("DATABASE_PATH", None)
        self._temp_dir.cleanup()

    def _mock_response(self):
        response = Mock()
        response.status_code = 200
        response.headers = {}
        response.iter_content.return_value = [RSS_FIXTURE]
        response.url = "https://feeds.example.com/rss.xml"
        response.raise_for_status.return_value = None
        response.close.return_value = None
        return response

    def _public_dns(self):
        return patch(
            "src.services.intelligence_service.socket.getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))],
        )

    def test_create_fetch_and_query_items(self) -> None:
        with self._public_dns():
            create_resp = self.client.post("/api/v1/intelligence/sources", json={"name": "api-feed", "url": "https://feeds.example.com/rss.xml", "source_type": "rss", "scope_type": "market", "market": "cn"})
        self.assertEqual(create_resp.status_code, 200)
        source_id = create_resp.json()["id"]
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=self._mock_response()):
            fetch_resp = self.client.post(f"/api/v1/intelligence/sources/{source_id}/fetch")
        self.assertEqual(fetch_resp.status_code, 200)
        self.assertEqual(fetch_resp.json()["saved_count"], 1)
        list_resp = self.client.get("/api/v1/intelligence/items", params={"scope_type": "market", "market": "cn"})
        self.assertEqual(list_resp.status_code, 200)
        body = list_resp.json()
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["url"], "https://news.example.com/market-event")

    def test_rejects_private_source_url(self) -> None:
        resp = self.client.post("/api/v1/intelligence/sources", json={"name": "bad", "url": "http://localhost/rss.xml", "scope_type": "market"})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["error"], "validation_error")

    def test_fetch_error_does_not_expose_source_url_secret(self) -> None:
        with self._public_dns():
            create_resp = self.client.post(
                "/api/v1/intelligence/sources",
                json={
                    "name": "secret-feed",
                    "url": "https://feeds.example.com/rss.xml?token=secret",
                    "scope_type": "market",
                },
            )
        self.assertEqual(create_resp.status_code, 200)

        response = self._mock_response()
        response.raise_for_status.side_effect = requests.HTTPError(
            "404 Client Error for url: https://feeds.example.com/rss.xml?token=secret"
        )

        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=response), self.assertLogs("api.v1.endpoints.intelligence", level="ERROR") as logs:
            fetch_resp = self.client.post(f"/api/v1/intelligence/sources/{create_resp.json()['id']}/fetch")

        self.assertEqual(fetch_resp.status_code, 500)
        response_blob = str(fetch_resp.json())
        log_blob = "\n".join(logs.output)
        self.assertNotIn("secret", response_blob)
        self.assertNotIn("token=secret", log_blob)
        self.assertEqual(fetch_resp.json()["message"], "Fetch intelligence source failed")


if __name__ == "__main__":
    unittest.main()
