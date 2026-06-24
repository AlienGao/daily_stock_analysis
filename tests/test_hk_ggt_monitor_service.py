# -*- coding: utf-8 -*-
from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from src.core.trading_calendar import MarketPhase
from src.services.hk_ggt_monitor_service import (
    align_bar_time,
    is_hk_ggt_poll_window,
    HkGgtMonitorService,
)


def test_align_bar_time_truncates_seconds():
    now = datetime(2026, 6, 24, 10, 15, 47, tzinfo=ZoneInfo("Asia/Hong_Kong"))
    trade_date, bar_time = align_bar_time(now)
    assert trade_date == "20260624"
    assert bar_time == "2026-06-24 10:15:00"


def test_is_hk_ggt_poll_window_fallback_morning():
    now = datetime(2026, 6, 24, 10, 30, tzinfo=ZoneInfo("Asia/Hong_Kong"))
    with patch("src.services.hk_ggt_monitor_service.infer_market_phase", return_value=MarketPhase.UNKNOWN):
        assert is_hk_ggt_poll_window(now) is True


def test_is_hk_ggt_poll_window_outside_fallback():
    now = datetime(2026, 6, 24, 8, 0, tzinfo=ZoneInfo("Asia/Hong_Kong"))
    with patch("src.services.hk_ggt_monitor_service.infer_market_phase", return_value=MarketPhase.UNKNOWN):
        assert is_hk_ggt_poll_window(now) is False


def test_refresh_components_skips_when_cached():
    db = MagicMock()
    db.list_hk_ggt_components.return_value = [MagicMock()]
    service = HkGgtMonitorService(db=db, config=MagicMock())
    result = service.refresh_components("20260624")
    assert result["skipped"] is True
    assert result["saved"] == 0


def test_poll_rt_once_disabled():
    config = MagicMock()
    config.hk_ggt_rt_poll_enabled = False
    service = HkGgtMonitorService(db=MagicMock(), config=config)
    assert service.poll_rt_once()["reason"] == "disabled"


def test_poll_rt_once_outside_session():
    config = MagicMock()
    config.hk_ggt_rt_poll_enabled = True
    service = HkGgtMonitorService(db=MagicMock(), config=config)
    with patch("src.services.hk_ggt_monitor_service.is_hk_ggt_poll_window", return_value=False):
        assert service.poll_rt_once()["reason"] == "outside_session"
