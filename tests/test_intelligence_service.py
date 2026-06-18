# -*- coding: utf-8 -*-
"""Tests for configurable persisted intelligence sources."""

from __future__ import annotations

import os
import socket
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.config import Config
from src.repositories.intelligence_repo import IntelligenceRepository
from src.services.intelligence_service import IntelligenceService, IntelligenceServiceError, _MAX_FEED_BYTES
from src.storage import DatabaseManager, IntelligenceItem, INTELLIGENCE_ITEM_NULL_SCOPE_VALUE

RSS_FIXTURE = b'<?xml version="1.0" encoding="UTF-8"?>\n<rss version="2.0"><channel>\n<item><title>Policy support lifts AI supply chain</title><link>https://news.example.com/a</link><description>Market-level catalyst with evidence link.</description><pubDate>Wed, 17 Jun 2026 08:00:00 GMT</pubDate></item>\n<item><title>Second item</title><link>https://news.example.com/b</link><description>Second summary.</description></item>\n</channel></rss>'


class IntelligenceServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        os.environ["DATABASE_PATH"] = os.path.join(self._temp_dir.name, "intelligence.db")
        os.environ["NEWS_INTEL_RETENTION_DAYS"] = "30"
        os.environ["NEWS_INTEL_MAX_ITEMS_PER_SOURCE"] = "50"
        os.environ["NEWS_INTEL_FETCH_TIMEOUT_SEC"] = "3"
        Config._instance = None
        DatabaseManager.reset_instance()
        self.service = IntelligenceService()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config._instance = None
        for key in ["DATABASE_PATH", "NEWS_INTEL_RETENTION_DAYS", "NEWS_INTEL_MAX_ITEMS_PER_SOURCE", "NEWS_INTEL_FETCH_TIMEOUT_SEC"]:
            os.environ.pop(key, None)
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

    def test_create_fetch_and_deduplicate_rss_source(self) -> None:
        with self._public_dns():
            source = self.service.create_source({
                "name": "market-feed", "url": "https://feeds.example.com/rss.xml",
                "source_type": "rss", "scope_type": "market", "market": "cn",
            })
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=self._mock_response()):
            first = self.service.fetch_source(source["id"])
            second = self.service.fetch_source(source["id"])
        self.assertEqual(first["fetched_count"], 2)
        self.assertEqual(first["saved_count"], 2)
        self.assertEqual(second["saved_count"], 0)
        items = self.service.list_items(scope_type="market", market="cn")
        self.assertEqual(items["total"], 2)
        self.assertEqual(items["items"][0]["scope_type"], "market")
        self.assertTrue(items["items"][0]["url"].startswith("https://news.example.com/"))

    def test_same_url_from_different_source_scope_preserves_both_items(self) -> None:
        with self._public_dns():
            cn_source = self.service.create_source({
                "name": "cn-feed", "url": "https://feeds.example.com/rss.xml",
                "source_type": "rss", "scope_type": "market", "market": "cn",
            })
            us_source = self.service.create_source({
                "name": "us-feed", "url": "https://feeds.example.com/rss.xml",
                "source_type": "rss", "scope_type": "market", "market": "us",
            })
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=self._mock_response()):
            cn_result = self.service.fetch_source(cn_source["id"])
            us_result = self.service.fetch_source(us_source["id"])
        self.assertEqual(cn_result["saved_count"], 2)
        self.assertEqual(us_result["saved_count"], 2)
        cn_items = self.service.list_items(scope_type="market", market="cn")
        us_items = self.service.list_items(scope_type="market", market="us")
        self.assertEqual(cn_items["total"], 2)
        self.assertEqual(us_items["total"], 2)
        self.assertEqual({item["source_name"] for item in cn_items["items"]}, {"cn-feed"})
        self.assertEqual({item["source_name"] for item in us_items["items"]}, {"us-feed"})

    def test_private_network_url_is_rejected(self) -> None:
        with self.assertRaises(IntelligenceServiceError):
            self.service.create_source({"name": "bad", "url": "http://127.0.0.1:8000/rss.xml", "scope_type": "market"})

    def test_dns_name_resolving_private_address_is_rejected(self) -> None:
        private_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 0))]
        with patch("src.services.intelligence_service.socket.getaddrinfo", return_value=private_dns):
            with self.assertRaises(IntelligenceServiceError):
                self.service.create_source({"name": "bad", "url": "https://metadata.example.com/rss.xml", "scope_type": "market"})

    def test_shared_address_space_url_is_rejected(self) -> None:
        shared_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("100.64.0.1", 0))]
        with patch("src.services.intelligence_service.socket.getaddrinfo", return_value=shared_dns):
            with self.assertRaises(IntelligenceServiceError):
                self.service.create_source({"name": "bad", "url": "https://metadata.example.com/rss.xml", "scope_type": "market"})

    def test_generated_no_url_sentinel_is_rejected_for_source_url(self) -> None:
        with self.assertRaisesRegex(IntelligenceServiceError, "absolute http\\(s\\) URL"):
            self.service.create_source({"name": "bad", "url": "no-url:intel:anything", "scope_type": "market"})

    def test_redirect_target_is_validated_before_following(self) -> None:
        with self._public_dns():
            source = self.service.create_source({
                "name": "redirect-feed",
                "url": "https://feeds.example.com/rss.xml",
                "scope_type": "market",
            })
        redirect = Mock()
        redirect.status_code = 302
        redirect.headers = {"Location": "http://127.0.0.1/rss.xml"}
        redirect.url = "https://feeds.example.com/rss.xml"
        redirect.content = b""
        redirect.raise_for_status.return_value = None
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=redirect) as mock_get:
            with self.assertRaises(IntelligenceServiceError):
                self.service.fetch_source(source["id"])
            self.assertEqual(mock_get.call_count, 1)
            self.assertFalse(mock_get.call_args.kwargs["allow_redirects"])

    def test_fetch_requests_disable_environment_proxies(self) -> None:
        with self._public_dns():
            source = self.service.create_source({
                "name": "proxy-safe-feed",
                "url": "https://feeds.example.com/rss.xml",
                "scope_type": "market",
            })
        with patch.dict(os.environ, {"HTTP_PROXY": "http://127.0.0.1:3128", "HTTPS_PROXY": "http://127.0.0.1:3128", "ALL_PROXY": "http://127.0.0.1:3128"}):
            with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=self._mock_response()) as mock_get:
                self.service.fetch_source(source["id"])
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(mock_get.call_args.kwargs["proxies"], {"http": None, "https": None})

    def test_fetch_validates_dns_resolution_used_by_request(self) -> None:
        public_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
        private_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("169.254.169.254", 0))]

        def fake_get(url, **kwargs):
            socket.getaddrinfo("feeds.example.com", 443, type=socket.SOCK_STREAM)
            return self._mock_response()

        with patch("src.services.intelligence_service.socket.getaddrinfo", side_effect=[public_dns, private_dns]):
            with patch("src.services.intelligence_service.requests.get", side_effect=fake_get) as mock_get:
                with self.assertRaises(IntelligenceServiceError):
                    self.service._get_feed_response("https://feeds.example.com/rss.xml")
        self.assertEqual(mock_get.call_count, 1)

    def test_fetch_streams_response_before_enforcing_byte_cap(self) -> None:
        with self._public_dns():
            source = self.service.create_source({
                "name": "large-feed", "url": "https://feeds.example.com/rss.xml",
                "scope_type": "market",
            })

        class LargeResponse:
            status_code = 200
            headers: dict = {}
            url = "https://feeds.example.com/rss.xml"

            @property
            def content(self):
                raise AssertionError("content should not be read")

            def raise_for_status(self):
                return None

            def iter_content(self, chunk_size=1):
                yield b"x" * (_MAX_FEED_BYTES + 1)

            def close(self):
                self.closed = True

        response = LargeResponse()
        response.closed = False
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=response) as mock_get:
            with self.assertRaisesRegex(IntelligenceServiceError, "feed response is too large"):
                self.service.fetch_source(source["id"])
        self.assertTrue(mock_get.call_args.kwargs["stream"])
        self.assertTrue(response.closed)

    def test_fetch_enabled_sources_is_fail_open(self) -> None:
        with self._public_dns():
            self.service.create_source({"name": "good-feed", "url": "https://feeds.example.com/rss.xml", "scope_type": "market"})
            bad = self.service.create_source({"name": "bad-feed", "url": "https://bad.example.com/rss.xml", "scope_type": "market"})

        def fake_get(url, **kwargs):
            if "bad" in url:
                raise RuntimeError("network token=secret should not leak")
            return self._mock_response()
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", side_effect=fake_get):
            result = self.service.fetch_enabled_sources()
        self.assertEqual(result["source_count"], 2)
        self.assertEqual(result["saved_count"], 2)
        failures = [item for item in result["results"] if not item["ok"]]
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["source_id"], bad["id"])
        self.assertIn("token=***", failures[0]["error"])
        self.assertNotIn("secret", failures[0]["error"])

    def test_sanitize_error_redacts_common_query_secret_names(self) -> None:
        sanitized = IntelligenceService._sanitize_error(
            RuntimeError(
                "failed url=https://feed.example/rss?access_token=first&auth-token=second"
                " callback=https://feed.example/rss?api-key=third#secret=fourth"
            )
        )

        self.assertNotIn("first", sanitized)
        self.assertNotIn("second", sanitized)
        self.assertNotIn("third", sanitized)
        self.assertNotIn("fourth", sanitized)
        self.assertIn("access_token=***", sanitized)
        self.assertIn("auth-token=***", sanitized)
        self.assertIn("api-key=***", sanitized)
        self.assertIn("secret=***", sanitized)

    def test_fetch_enabled_sources_iterates_every_enabled_source(self) -> None:
        with self._public_dns():
            for index in range(101):
                self.service.create_source({
                    "name": f"feed-{index}",
                    "url": f"https://feeds{index}.example.com/rss.xml",
                    "scope_type": "market",
                })
        with self._public_dns(), patch("src.services.intelligence_service.requests.get", return_value=self._mock_response()) as mock_get:
            result = self.service.fetch_enabled_sources()
        self.assertEqual(result["source_count"], 101)
        self.assertEqual(len(result["results"]), 101)
        self.assertEqual(mock_get.call_count, 101)

    def test_retention_removes_old_items(self) -> None:
        repo = IntelligenceRepository()
        old_time = datetime.now() - timedelta(days=60)
        repo.upsert_items([{"source_name": "legacy", "source_type": "rss", "title": "old", "summary": "old item", "url": "https://news.example.com/old", "source": "legacy", "published_at": old_time, "fetched_at": old_time, "scope_type": "market", "scope_value": None, "market": "cn"}])
        self.assertEqual(repo.apply_retention(30), 1)
        with DatabaseManager.get_instance().get_session() as session:
            self.assertEqual(session.query(IntelligenceItem).count(), 0)

    def test_market_scope_item_uses_non_null_dedupe_key(self) -> None:
        repo = IntelligenceRepository()
        with self._public_dns():
            source = self.service.create_source({
                "name": "unique-market-feed",
                "url": "https://feeds.example.com/rss.xml",
                "scope_type": "market",
                "market": "cn",
            })
        fields = {
            "source_id": source["id"],
            "source_name": "unique-market-feed",
            "source_type": "rss",
            "title": "same market item",
            "summary": "summary",
            "url": "https://news.example.com/unique",
            "source": "unique-market-feed",
            "published_at": datetime.now(),
            "fetched_at": datetime.now(),
            "scope_type": "market",
            "scope_value": None,
            "market": "cn",
        }

        self.assertEqual(repo.upsert_items([fields]), 1)
        with DatabaseManager.get_instance().get_session() as session:
            stored = session.query(IntelligenceItem).one()
            self.assertEqual(stored.scope_value, INTELLIGENCE_ITEM_NULL_SCOPE_VALUE)
            session.add(IntelligenceItem(**{**fields, "scope_value": INTELLIGENCE_ITEM_NULL_SCOPE_VALUE}))
            with self.assertRaises(IntegrityError):
                session.flush()

    def test_duplicate_insert_race_does_not_rollback_prior_batch_items(self) -> None:
        repo = IntelligenceRepository()
        base_fields = {
            "source_name": "race-feed",
            "source_type": "rss",
            "summary": "summary",
            "source": "race-feed",
            "published_at": datetime.now(),
            "fetched_at": datetime.now(),
            "scope_type": "market",
            "scope_value": None,
            "market": "cn",
        }
        kept = {**base_fields, "title": "kept", "url": "https://news.example.com/kept"}
        race = {**base_fields, "title": "race", "url": "https://news.example.com/race"}
        after = {**base_fields, "title": "after", "url": "https://news.example.com/after"}
        original_flush = Session.flush
        raised = False

        def flush_with_duplicate_race(session, *args, **kwargs):
            nonlocal raised
            if not raised and any(
                isinstance(row, IntelligenceItem) and row.url == race["url"]
                for row in session.new
            ):
                raised = True
                raise IntegrityError("insert intelligence item", {}, RuntimeError("duplicate url"))
            return original_flush(session, *args, **kwargs)

        with patch.object(Session, "flush", flush_with_duplicate_race):
            self.assertEqual(repo.upsert_items([kept, race, after]), 2)

        with DatabaseManager.get_instance().get_session() as session:
            titles = {row.title for row in session.query(IntelligenceItem).all()}
        self.assertEqual(titles, {"kept", "after"})


if __name__ == "__main__":
    unittest.main()
