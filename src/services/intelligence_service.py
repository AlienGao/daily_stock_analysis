# -*- coding: utf-8 -*-
"""Configurable compliant news / intelligence source service."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
import re
import socket
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import requests

from src.config import get_config
from src.repositories.intelligence_repo import IntelligenceRepository
from src.storage import IntelligenceSource, INTELLIGENCE_ITEM_NULL_SCOPE_VALUE

logger = logging.getLogger(__name__)
_ALLOWED_SOURCE_TYPES = {"rss", "atom"}
_ALLOWED_SCOPE_TYPES = {"symbol", "market", "sector"}
_ALLOWED_MARKETS = {"cn", "hk", "us", "global"}
_PRIVATE_HOSTNAMES = {"localhost", "localhost.localdomain"}
_MAX_FEED_BYTES = 2 * 1024 * 1024
_MAX_REDIRECTS = 5
_REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}
_DNS_GUARD_LOCK = threading.RLock()
_DISABLE_REQUEST_PROXIES = {"http": None, "https": None}


class IntelligenceServiceError(ValueError):
    """User-facing validation error for intelligence operations."""


@dataclass(frozen=True)
class FeedEntry:
    title: str
    summary: str
    url: str
    source: str
    published_at: Optional[datetime]
    raw_payload: Dict[str, Any]


class IntelligenceService:
    """Fetch, validate, persist and query configurable intelligence sources."""

    def __init__(self, repository: Optional[IntelligenceRepository] = None):
        self.repo = repository or IntelligenceRepository()
        self.config = get_config()

    def create_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        fields = self._normalize_source_fields(payload)
        self._validate_url(fields["url"])
        return self._source_to_dict(self.repo.create_source(fields))

    def list_sources(self, **filters: Any) -> Dict[str, Any]:
        rows, total = self.repo.list_sources(**filters)
        return {
            "items": [self._source_to_dict(row) for row in rows],
            "total": total,
            "page": max(1, int(filters.get("page") or 1)),
            "page_size": max(1, min(int(filters.get("page_size") or 50), 100)),
        }

    def list_items(self, **filters: Any) -> Dict[str, Any]:
        rows, total = self.repo.list_items(**filters)
        return {
            "items": [self._item_to_dict(row) for row in rows],
            "total": total,
            "page": max(1, int(filters.get("page") or 1)),
            "page_size": max(1, min(int(filters.get("page_size") or 50), 100)),
        }

    def test_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        fields = self._normalize_source_fields(payload)
        entries = self._fetch_feed_entries(fields, limit=min(5, self.config.news_intel_max_items_per_source))
        return {
            "ok": True,
            "source": self._redact_source_fields(fields),
            "fetched_count": len(entries),
            "sample_items": [self._feed_entry_to_dict(entry) for entry in entries[:5]],
        }

    def fetch_source(self, source_id: int, *, dry_run: bool = False) -> Dict[str, Any]:
        source = self.repo.get_source(source_id)
        if source is None:
            raise IntelligenceServiceError(f"Intelligence source not found: {source_id}")
        if not source.enabled:
            raise IntelligenceServiceError(f"Intelligence source is disabled: {source_id}")
        now = datetime.now()
        try:
            entries = self._fetch_feed_entries(self._source_to_fields(source), limit=self.config.news_intel_max_items_per_source)
            item_fields = [self._entry_to_item_fields(entry, source, now) for entry in entries]
            saved = 0 if dry_run else self.repo.upsert_items(item_fields)
            deleted = 0 if dry_run else self.repo.apply_retention(self.config.news_intel_retention_days)
            if not dry_run:
                self.repo.update_source_status(source.id, status="success", error=None, fetched_at=now)
            return {
                "ok": True,
                "source_id": source.id,
                "fetched_count": len(entries),
                "saved_count": saved,
                "retention_deleted": deleted,
                "dry_run": dry_run,
                "sample_items": [self._feed_entry_to_dict(entry) for entry in entries[:5]],
            }
        except Exception as exc:
            error = self._sanitize_error(exc)
            if not dry_run:
                self.repo.update_source_status(source.id, status="failed", error=error)
            logger.warning("Intelligence source fetch failed id=%s name=%s: %s", source.id, source.name, error)
            raise

    def fetch_enabled_sources(self) -> Dict[str, Any]:
        source_ids: List[int] = []
        page = 1
        while True:
            rows, total = self.repo.list_sources(enabled=True, page=page, page_size=100)
            source_ids.extend(row.id for row in rows)
            if not rows or len(source_ids) >= total:
                break
            page += 1

        results = []
        for source_id in source_ids:
            try:
                results.append(self.fetch_source(source_id))
            except Exception as exc:
                results.append({"ok": False, "source_id": source_id, "error": self._sanitize_error(exc)})
        return {
            "ok": True,
            "source_count": len(source_ids),
            "results": results,
            "saved_count": sum(int(item.get("saved_count") or 0) for item in results),
        }

    def _normalize_source_fields(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = str(payload.get("name") or "").strip()
        url = str(payload.get("url") or "").strip()
        source_type = str(payload.get("source_type") or "rss").strip().lower()
        scope_type = str(payload.get("scope_type") or "market").strip().lower()
        scope_value = str(payload.get("scope_value") or "").strip() or None
        market = str(payload.get("market") or "cn").strip().lower()
        enabled = bool(payload.get("enabled", True))
        description = str(payload.get("description") or "").strip() or None
        if not name:
            raise IntelligenceServiceError("source name is required")
        if not url:
            raise IntelligenceServiceError("source url is required")
        if source_type not in _ALLOWED_SOURCE_TYPES:
            raise IntelligenceServiceError(f"unsupported source_type: {source_type}")
        if scope_type not in _ALLOWED_SCOPE_TYPES:
            raise IntelligenceServiceError(f"unsupported scope_type: {scope_type}")
        if scope_type in {"symbol", "sector"} and not scope_value:
            raise IntelligenceServiceError(f"scope_value is required when scope_type={scope_type}")
        if market not in _ALLOWED_MARKETS:
            raise IntelligenceServiceError(f"unsupported market: {market}")
        return {
            "name": name[:100],
            "source_type": source_type,
            "url": url,
            "enabled": enabled,
            "scope_type": scope_type,
            "scope_value": scope_value[:64] if scope_value else None,
            "market": market,
            "description": description,
        }

    def _validate_url(self, raw_url: str) -> None:
        parsed = urlparse(raw_url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            raise IntelligenceServiceError("source url must be an absolute http(s) URL")
        if parsed.username or parsed.password:
            raise IntelligenceServiceError("source url must not contain credentials")
        hostname = (parsed.hostname or "").strip().lower().rstrip(".")
        if not hostname:
            raise IntelligenceServiceError("source url host is required")
        if hostname in _PRIVATE_HOSTNAMES or hostname.endswith(".local"):
            raise IntelligenceServiceError("source url host is not allowed")
        try:
            ip = ipaddress.ip_address(hostname)
        except ValueError:
            self._validate_resolved_hostname(hostname)
            return
        if self._is_blocked_ip(ip):
            raise IntelligenceServiceError("source url must not target private or local network addresses")

    def _fetch_feed_entries(self, fields: Dict[str, Any], *, limit: int) -> List[FeedEntry]:
        self._validate_url(fields["url"])
        response = self._get_feed_response(fields["url"])
        try:
            response.raise_for_status()
            content = self._read_limited_response(response)
        finally:
            self._close_response(response)
        return self._parse_feed(content, source_name=fields["name"], limit=limit)

    def _get_feed_response(self, raw_url: str) -> requests.Response:
        current_url = raw_url
        timeout = max(1, min(float(self.config.news_intel_fetch_timeout_sec), 30.0))
        headers = {"User-Agent": "daily-stock-analysis-intel/1.0"}
        for redirect_index in range(_MAX_REDIRECTS + 1):
            self._validate_url(current_url)
            response = self._get_with_validated_dns(
                current_url,
                timeout=timeout,
                headers=headers,
                allow_redirects=False,
                stream=True,
            )
            status_code = self._response_status_code(response)
            if status_code not in _REDIRECT_STATUS_CODES:
                try:
                    self._validate_url(response.url or current_url)
                except Exception:
                    self._close_response(response)
                    raise
                return response
            if redirect_index >= _MAX_REDIRECTS:
                self._close_response(response)
                raise IntelligenceServiceError("too many redirects while fetching source url")
            location = response.headers.get("Location") if response.headers else None
            self._close_response(response)
            if not location:
                raise IntelligenceServiceError("redirect response is missing Location header")
            current_url = urljoin(current_url, location.strip())
            self._validate_url(current_url)
        raise IntelligenceServiceError("too many redirects while fetching source url")

    def _get_with_validated_dns(self, raw_url: str, **kwargs: Any) -> requests.Response:
        parsed = urlparse(raw_url)
        target_hostname = self._normalize_hostname(parsed.hostname)
        original_getaddrinfo = socket.getaddrinfo

        def guarded_getaddrinfo(host: Any, port: Any, *args: Any, **inner_kwargs: Any) -> Any:
            addrinfos = original_getaddrinfo(host, port, *args, **inner_kwargs)
            if self._normalize_hostname(host) == target_hostname:
                self._validate_addrinfos(addrinfos)
            return addrinfos

        with _DNS_GUARD_LOCK:
            socket.getaddrinfo = guarded_getaddrinfo
            try:
                request_kwargs = dict(kwargs)
                request_kwargs.setdefault("proxies", _DISABLE_REQUEST_PROXIES)
                return requests.get(raw_url, **request_kwargs)
            finally:
                socket.getaddrinfo = original_getaddrinfo

    @staticmethod
    def _read_limited_response(response: requests.Response) -> bytes:
        chunks: List[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if not chunk:
                continue
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            total += len(chunk)
            if total > _MAX_FEED_BYTES:
                raise IntelligenceServiceError("feed response is too large")
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _close_response(response: requests.Response) -> None:
        close = getattr(response, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _normalize_hostname(hostname: Any) -> str:
        if isinstance(hostname, bytes):
            hostname = hostname.decode("ascii", errors="ignore")
        normalized = str(hostname or "").strip().lower().rstrip(".")
        try:
            return normalized.encode("idna").decode("ascii")
        except UnicodeError:
            return normalized

    @staticmethod
    def _response_status_code(response: requests.Response) -> int:
        try:
            return int(getattr(response, "status_code", 0) or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _validate_resolved_hostname(hostname: str) -> None:
        try:
            addrinfos = socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise IntelligenceServiceError("source url host could not be resolved") from exc
        if not addrinfos:
            raise IntelligenceServiceError("source url host could not be resolved")
        IntelligenceService._validate_addrinfos(addrinfos)

    @staticmethod
    def _validate_addrinfos(addrinfos: Any) -> None:
        for addrinfo in addrinfos:
            address = addrinfo[4][0]
            try:
                ip = ipaddress.ip_address(address)
            except ValueError as exc:
                raise IntelligenceServiceError("source url resolved to an invalid address") from exc
            if IntelligenceService._is_blocked_ip(ip):
                raise IntelligenceServiceError("source url must resolve to a public internet address")

    @staticmethod
    def _is_blocked_ip(ip: ipaddress._BaseAddress) -> bool:
        return not ip.is_global

    def _parse_feed(self, content: bytes, *, source_name: str, limit: int) -> List[FeedEntry]:
        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            raise IntelligenceServiceError(f"invalid RSS/Atom feed: {exc}") from exc
        tag = self._strip_ns(root.tag).lower()
        if tag == "rss":
            nodes = root.findall("./channel/item")
            return [entry for entry in (self._parse_rss_item(node, source_name) for node in nodes[:limit]) if entry]
        if tag == "feed":
            nodes = root.findall("./{*}entry") or root.findall("./entry")
            return [entry for entry in (self._parse_atom_entry(node, source_name) for node in nodes[:limit]) if entry]
        raise IntelligenceServiceError("unsupported feed format; expected RSS or Atom")

    def _parse_rss_item(self, node: ET.Element, source_name: str) -> Optional[FeedEntry]:
        return self._build_entry(
            self._text(node, "title"),
            self._text(node, "description") or self._text(node, "summary"),
            self._text(node, "link"),
            source_name,
            self._parse_datetime(self._text(node, "pubDate") or self._text(node, "published")),
        )

    def _parse_atom_entry(self, node: ET.Element, source_name: str) -> Optional[FeedEntry]:
        url = ""
        for link in node.findall("./{*}link") or node.findall("./link"):
            if (link.attrib.get("rel") or "alternate").lower() == "alternate" and link.attrib.get("href"):
                url = link.attrib["href"].strip()
                break
        return self._build_entry(
            self._text(node, "title"),
            self._text(node, "summary") or self._text(node, "content"),
            url,
            source_name,
            self._parse_datetime(self._text(node, "published") or self._text(node, "updated")),
        )

    def _build_entry(self, title: str, summary: str, url: str, source_name: str, published_at: Optional[datetime]) -> Optional[FeedEntry]:
        title = self._clean_text(title)[:300]
        summary = self._clean_text(summary)[:2000]
        url = url.strip()
        if not title and not url:
            return None
        if url:
            self._validate_url(url)
            url_key = url
        else:
            digest = hashlib.sha256(f"{source_name}|{title}|{published_at}".encode("utf-8")).hexdigest()[:24]
            url_key = f"no-url:intel:{digest}"
        return FeedEntry(title or url_key, summary, url_key, source_name, published_at, {"source": source_name})

    def _entry_to_item_fields(self, entry: FeedEntry, source: IntelligenceSource, now: datetime) -> Dict[str, Any]:
        return {
            "source_id": source.id,
            "source_name": source.name,
            "source_type": source.source_type,
            "title": entry.title,
            "summary": entry.summary,
            "url": entry.url,
            "source": entry.source,
            "published_at": entry.published_at,
            "fetched_at": now,
            "scope_type": source.scope_type,
            "scope_value": source.scope_value,
            "market": source.market,
            "raw_payload": json.dumps(entry.raw_payload, ensure_ascii=False),
        }

    @staticmethod
    def _source_to_fields(source: IntelligenceSource) -> Dict[str, Any]:
        return {
            "name": source.name,
            "source_type": source.source_type,
            "url": source.url,
            "enabled": source.enabled,
            "scope_type": source.scope_type,
            "scope_value": source.scope_value,
            "market": source.market,
            "description": source.description,
        }

    @staticmethod
    def _source_to_dict(source: IntelligenceSource) -> Dict[str, Any]:
        return {
            "id": source.id,
            "name": source.name,
            "source_type": source.source_type,
            "url": source.url,
            "enabled": bool(source.enabled),
            "scope_type": source.scope_type,
            "scope_value": source.scope_value,
            "market": source.market,
            "description": source.description,
            "last_status": source.last_status,
            "last_error": source.last_error,
            "last_fetched_at": IntelligenceService._iso(source.last_fetched_at),
            "created_at": IntelligenceService._iso(source.created_at),
            "updated_at": IntelligenceService._iso(source.updated_at),
        }

    @staticmethod
    def _item_to_dict(item: Any) -> Dict[str, Any]:
        return {
            "id": item.id,
            "source_id": item.source_id,
            "source_name": item.source_name,
            "source_type": item.source_type,
            "title": item.title,
            "summary": item.summary,
            "url": item.url,
            "source": item.source,
            "published_at": IntelligenceService._iso(item.published_at),
            "fetched_at": IntelligenceService._iso(item.fetched_at),
            "scope_type": item.scope_type,
            "scope_value": None if (
                item.scope_type == "market" and item.scope_value == INTELLIGENCE_ITEM_NULL_SCOPE_VALUE
            ) else item.scope_value,
            "market": item.market,
        }

    @staticmethod
    def _feed_entry_to_dict(entry: FeedEntry) -> Dict[str, Any]:
        return {
            "title": entry.title,
            "summary": entry.summary,
            "url": entry.url,
            "source": entry.source,
            "published_at": IntelligenceService._iso(entry.published_at),
        }

    @staticmethod
    def _redact_source_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in fields.items() if k not in {"headers", "token", "api_key"}}

    @staticmethod
    def _sanitize_error(exc: Exception) -> str:
        return re.sub(
            r"((?:^|[?&#\s])(?:access[_-]?token|auth[_-]?token|api[_-]?key|apikey|api_key|token|key|secret)=)[^&\s#]+",
            r"\1***",
            str(exc),
            flags=re.I,
        )[:500]

    @staticmethod
    def _strip_ns(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    @classmethod
    def _text(cls, node: ET.Element, name: str) -> str:
        found = node.find(f"./{{*}}{name}")
        if found is None:
            found = node.find(f"./{name}")
        return "" if found is None or found.text is None else found.text.strip()

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value or "")).strip()

    @staticmethod
    def _parse_datetime(value: str) -> Optional[datetime]:
        raw = (value or "").strip()
        if not raw:
            return None
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    @staticmethod
    def _iso(value: Optional[datetime]) -> Optional[str]:
        return value.isoformat() if value else None
