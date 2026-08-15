# -*- coding: utf-8 -*-
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from src.core.trading_calendar import MarketPhase
from src.services.hk_ggt_monitor_service import (
    align_bar_time,
    is_hk_ggt_poll_window,
    _max_consecutive_drawdown,
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


def _minute(bar_time: str, close: float, pct_change: float | None = None):
    return SimpleNamespace(
        hk_code="00700",
        trade_date="20260814",
        bar_time=bar_time,
        close=close,
        prev_close=100.0,
        pct_change=pct_change,
        to_dict=lambda: {
            "hk_code": "00700",
            "trade_date": "20260814",
            "bar_time": bar_time,
            "close": close,
            "period": "1",
            "source": "tushare_rt",
        },
    )


def test_max_consecutive_drawdown_uses_deepest_run_and_breaks_on_gap():
    result = _max_consecutive_drawdown([
        _minute("2026-08-14 09:30:00", 100.0),
        _minute("2026-08-14 09:31:00", 98.0),
        _minute("2026-08-14 09:32:00", 95.0),
        _minute("2026-08-14 10:10:00", 90.0),
        _minute("2026-08-14 10:11:00", 89.0),
    ])

    assert result == {
        "drawdown_pct": -5.0,
        "minutes": 2,
        "start_time": "2026-08-14 09:30:00",
        "end_time": "2026-08-14 09:32:00",
    }


def test_poll_rt_once_filters_components_and_saves_aligned_snapshot():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260814"
    db.list_hk_ggt_codes_for_date.return_value = ["00700"]
    db.upsert_hk_ggt_minute_bars.return_value = 1
    config = MagicMock(hk_ggt_rt_poll_enabled=True)
    service = HkGgtMonitorService(db=db, config=config)
    now = datetime(2026, 8, 14, 10, 15, 47, tzinfo=ZoneInfo("Asia/Hong_Kong"))

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=now), \
            patch("src.services.hk_ggt_monitor_service.is_hk_ggt_poll_window", return_value=True), \
            patch("data_provider.tushare_fetcher.TushareFetcher.get_instance") as get_fetcher:
        get_fetcher.return_value.fetch_rt_hk_k.return_value = [
            {"hk_code": "00700", "close": 102.0, "pre_close": 100.0},
            {"hk_code": "09988", "close": 80.0, "pre_close": 79.0},
        ]
        result = service.poll_rt_once()

    assert result["polled"] is True
    assert result["saved"] == 1
    saved_rows = db.upsert_hk_ggt_minute_bars.call_args.args[0]
    assert saved_rows[0]["hk_code"] == "00700"
    assert saved_rows[0]["bar_time"] == "2026-08-14 10:15:00"
    assert saved_rows[0]["pct_change"] == 2.0


def test_realtime_snapshot_returns_latest_quotes_and_top_drawdowns():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260814"
    db.list_hk_ggt_components.return_value = [
        SimpleNamespace(hk_code="00700", to_dict=lambda: {"hk_code": "00700", "name": "腾讯控股"}),
        SimpleNamespace(hk_code="09988", to_dict=lambda: {"hk_code": "09988", "name": "阿里巴巴-W"}),
    ]
    db.list_hk_ggt_minute_bars_batch.return_value = {
        "00700": [
            _minute("2026-08-14 10:00:00", 100.0),
            _minute("2026-08-14 10:01:00", 98.0),
            _minute("2026-08-14 10:02:00", 94.0, -6.0),
        ],
        "09988": [
            _minute("2026-08-14 10:00:00", 100.0),
            _minute("2026-08-14 10:01:00", 97.0, -3.0),
        ],
    }

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=datetime(2026, 8, 14, 10, 2)):
        result = HkGgtMonitorService(db=db, config=MagicMock()).get_realtime_snapshot("20260814")

    assert result["updated_at"] == "2026-08-14 10:02:00"
    assert result["items"][0]["latest_price"] == 94.0
    assert [item["hk_code"] for item in result["top_drawdowns"]] == ["00700", "09988"]
    assert result["top_drawdowns"][0]["intraday_consecutive_drawdown_pct"] == -6.0
