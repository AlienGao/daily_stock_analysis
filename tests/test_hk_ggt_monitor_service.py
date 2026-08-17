# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import requests

from src.core.trading_calendar import MarketPhase
from src.services.hk_ggt_monitor_service import (
    align_bar_time,
    is_hk_ggt_poll_window,
    _fetch_tencent_hk_quotes,
    _max_consecutive_drawdown,
    _max_rolling_gain,
    _compute_minute_boll,
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


def test_max_rolling_gain_uses_latest_run_before_gap():
    result = _max_rolling_gain([
        _minute("2026-08-14 11:59:00", 100.0),
        _minute("2026-08-14 12:00:00", 102.0),
        _minute("2026-08-14 13:01:00", 103.0),
    ])

    assert result == {
        "change_pct": 2.0,
        "start_time": "2026-08-14 11:59:00",
        "end_time": "2026-08-14 12:00:00",
    }


def test_compute_minute_boll_uses_latest_twenty_bars():
    bars = [
        _minute(
            (datetime(2026, 8, 14, 10, 0) + timedelta(minutes=index)).strftime("%Y-%m-%d %H:%M:%S"),
            100.0,
        )
        for index in range(20)
    ]
    result = _compute_minute_boll(bars)
    assert result is not None
    assert result["bar_time"] == "2026-08-14 10:19:00"
    assert result["mid"] == 100.0
    assert result["lower"] == 100.0


def test_intraday_rankings_cap_continuous_moves_at_30_minutes():
    start = datetime(2026, 8, 14, 10, 0)
    falling = [
        _minute((start + timedelta(minutes=index)).strftime("%Y-%m-%d %H:%M:%S"), 100 - index)
        for index in range(32)
    ]
    rising = [
        _minute((start + timedelta(minutes=index)).strftime("%Y-%m-%d %H:%M:%S"), 100 + index)
        for index in range(32)
    ]

    drawdown = _max_consecutive_drawdown(falling)
    gain = _max_rolling_gain(rising)

    assert drawdown is not None
    assert drawdown["minutes"] == 30
    assert drawdown["start_time"] == "2026-08-14 10:01:00"
    assert drawdown["end_time"] == "2026-08-14 10:31:00"
    assert gain is not None
    assert gain["start_time"] == "2026-08-14 10:00:00"
    assert gain["end_time"] == "2026-08-14 10:30:00"


def test_intraday_rankings_ignore_historical_results_outside_latest_window():
    bars = [
        _minute("2026-08-14 14:00:00", 100.0),
        _minute("2026-08-14 14:01:00", 90.0),
        _minute("2026-08-14 15:10:00", 100.0),
        _minute("2026-08-14 15:11:00", 98.0),
    ]

    ranking_end = datetime(2026, 8, 14, 15, 11)
    drawdown = _max_consecutive_drawdown(bars, window_end=ranking_end)
    gain = _max_rolling_gain(bars, window_end=ranking_end)

    assert drawdown is not None
    assert drawdown["start_time"] == "2026-08-14 15:10:00"
    assert drawdown["end_time"] == "2026-08-14 15:11:00"
    assert gain is not None
    assert gain["start_time"] == "2026-08-14 15:10:00"
    assert gain["end_time"] == "2026-08-14 15:11:00"
    assert gain["change_pct"] == -2.0


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
            "source": "tencent_rt",
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


def _tencent_quote_line(
    code: str = "00700",
    close: float = 102.0,
    prev_close: float = 100.0,
    pct_change: float = 2.0,
) -> str:
    fields = [""] * 50
    fields[0] = "100"
    fields[1] = "腾讯控股"
    fields[2] = code
    fields[3] = str(close)
    fields[4] = str(prev_close)
    fields[32] = str(pct_change)
    return f'v_hk{code}="{"~".join(fields)}";'


def test_fetch_tencent_hk_quotes_parses_valid_rows_and_skips_invalid():
    response = MagicMock()
    response.text = "\n".join([
        _tencent_quote_line(),
        _tencent_quote_line(code="09988", close=0.0),
        'v_hk00001="broken";',
    ])
    response.raise_for_status.return_value = None

    with patch("src.services.hk_ggt_monitor_service.requests.get", return_value=response) as get:
        quotes = _fetch_tencent_hk_quotes(["00700", "09988", "00700"])

    assert quotes == [{
        "hk_code": "00700",
        "close": 102.0,
        "pre_close": 100.0,
        "pct_change": 2.0,
    }]
    get.assert_called_once()
    assert "hk00700,hk09988" in get.call_args.args[0]


def test_fetch_tencent_hk_quotes_returns_empty_on_request_failure():
    with patch(
        "src.services.hk_ggt_monitor_service.requests.get",
        side_effect=requests.RequestException("unavailable"),
    ):
        assert _fetch_tencent_hk_quotes(["00700"]) == []


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
            patch("src.services.hk_ggt_monitor_service._fetch_tencent_hk_quotes") as fetch_quotes:
        fetch_quotes.return_value = [
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
    assert saved_rows[0]["source"] == "tencent_rt"


def test_poll_rt_once_records_minute_boll_alerts_for_hk_list():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260814"
    db.list_hk_ggt_codes_for_date.return_value = ["00700"]
    db.upsert_hk_ggt_minute_bars.return_value = 1
    db.list_hk_ggt_minute_bars_batch.return_value = {
        "00700": [
            _minute(
                (datetime(2026, 8, 14, 10, 0) + timedelta(minutes=index)).strftime("%Y-%m-%d %H:%M:%S"),
                100.0,
            )
            for index in range(20)
        ],
    }
    db.insert_hk_minute_boll_alerts.return_value = 1
    config = SimpleNamespace(hk_ggt_rt_poll_enabled=True, hk_list=["HK00700"])
    service = HkGgtMonitorService(db=db, config=config)
    now = datetime(2026, 8, 14, 10, 19, 47, tzinfo=ZoneInfo("Asia/Hong_Kong"))

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=now), \
            patch("src.services.hk_ggt_monitor_service.is_hk_ggt_poll_window", return_value=True), \
            patch("src.services.hk_ggt_monitor_service._fetch_tencent_hk_quotes") as fetch_quotes:
        fetch_quotes.return_value = [{"hk_code": "00700", "close": 100.0, "pre_close": 100.0}]
        result = service.poll_rt_once()

    assert result["alerts_created"] == 1
    alert_rows = db.insert_hk_minute_boll_alerts.call_args.args[0]
    assert len(alert_rows) == 1
    assert alert_rows[0]["band"] == "mid"


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
            _minute("2026-08-14 10:03:00", 96.0, -4.0),
        ],
        "09988": [
            _minute("2026-08-14 10:00:00", 100.0),
            _minute("2026-08-14 10:01:00", 97.0, -3.0),
            _minute("2026-08-14 10:02:00", 97.5, -2.5),
        ],
    }

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=datetime(2026, 8, 14, 10, 2)):
        result = HkGgtMonitorService(db=db, config=MagicMock()).get_realtime_snapshot("20260814")

    assert result["updated_at"] == "2026-08-14 10:03:00"
    assert result["items"][0]["latest_price"] == 96.0
    assert [item["hk_code"] for item in result["top_drawdowns"]] == ["00700", "09988"]
    assert result["top_drawdowns"][0]["intraday_consecutive_drawdown_pct"] == -6.0
    assert [item["hk_code"] for item in result["top_gainers"]] == ["00700", "09988"]
    assert result["top_gainers"][0]["minute_change_pct"] == 2.13


def test_realtime_snapshot_keeps_only_closest_boll_alert_per_stock():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260814"
    db.list_hk_ggt_components.return_value = [
        SimpleNamespace(hk_code="00700", to_dict=lambda: {"hk_code": "00700", "name": "腾讯控股"}),
    ]
    db.list_hk_ggt_minute_bars_batch.return_value = {
        "00700": [_minute("2026-08-14 10:19:00", 100.0)],
    }
    mid_alert = SimpleNamespace(
        id=1,
        hk_code="00700",
        bar_time="2026-08-14 10:15:00",
        band="mid",
        distance_pct=0.35,
        to_dict=lambda: {"id": 1, "hk_code": "00700", "band": "mid", "distance_pct": 0.35},
    )
    lower_alert = SimpleNamespace(
        id=2,
        hk_code="00700",
        bar_time="2026-08-14 10:19:00",
        band="lower",
        distance_pct=-0.12,
        to_dict=lambda: {"id": 2, "hk_code": "00700", "band": "lower", "distance_pct": -0.12},
    )
    db.list_hk_minute_boll_alerts.return_value = [lower_alert, mid_alert]

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=datetime(2026, 8, 14, 10, 19)):
        result = HkGgtMonitorService(
            db=db,
            config=SimpleNamespace(hk_list=["HK00700"]),
        ).get_realtime_snapshot("20260814")

    assert result["today_boll_alerts"] == [{
        "id": 2,
        "hk_code": "00700",
        "band": "lower",
        "distance_pct": -0.12,
        "name": "腾讯控股",
        "band_label": "下轨",
    }]


def test_realtime_snapshot_orders_boll_alerts_mid_band_first():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260814"
    db.list_hk_ggt_components.return_value = [
        SimpleNamespace(hk_code="00700", to_dict=lambda: {"hk_code": "00700", "name": "腾讯控股"}),
        SimpleNamespace(hk_code="09988", to_dict=lambda: {"hk_code": "09988", "name": "阿里巴巴-W"}),
        SimpleNamespace(hk_code="01810", to_dict=lambda: {"hk_code": "01810", "name": "小米集团-W"}),
    ]
    db.list_hk_ggt_minute_bars_batch.return_value = {}
    mid_alert = SimpleNamespace(
        id=3,
        hk_code="00700",
        bar_time="2026-08-14 10:15:00",
        band="mid",
        distance_pct=0.3,
        to_dict=lambda: {"id": 3, "hk_code": "00700", "band": "mid", "distance_pct": 0.3},
    )
    lower_new_alert = SimpleNamespace(
        id=4,
        hk_code="09988",
        bar_time="2026-08-14 10:18:00",
        band="lower",
        distance_pct=-0.4,
        to_dict=lambda: {"id": 4, "hk_code": "09988", "band": "lower", "distance_pct": -0.4},
    )
    lower_old_alert = SimpleNamespace(
        id=2,
        hk_code="01810",
        bar_time="2026-08-14 10:10:00",
        band="lower",
        distance_pct=-0.2,
        to_dict=lambda: {"id": 2, "hk_code": "01810", "band": "lower", "distance_pct": -0.2},
    )
    db.list_hk_minute_boll_alerts.return_value = [lower_old_alert, mid_alert, lower_new_alert]

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=datetime(2026, 8, 14, 10, 19)):
        result = HkGgtMonitorService(
            db=db,
            config=SimpleNamespace(hk_list=["HK00700", "HK09988", "HK01810"]),
        ).get_realtime_snapshot("20260814")

    assert [(a["band"], a["hk_code"]) for a in result["today_boll_alerts"]] == [
        ("mid", "00700"),
        ("lower", "09988"),
        ("lower", "01810"),
    ]
    assert [a["band_label"] for a in result["today_boll_alerts"]] == ["中轨", "下轨", "下轨"]


def test_realtime_snapshot_limits_top_drawdowns_to_hk_list():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260814"
    db.list_hk_ggt_components.return_value = [
        SimpleNamespace(hk_code="00700", to_dict=lambda: {"hk_code": "00700", "name": "腾讯控股"}),
        SimpleNamespace(hk_code="09988", to_dict=lambda: {"hk_code": "09988", "name": "阿里巴巴-W"}),
    ]
    db.list_hk_ggt_minute_bars_batch.return_value = {
        "00700": [
            _minute("2026-08-14 10:00:00", 100.0),
            _minute("2026-08-14 10:01:00", 94.0, -6.0),
        ],
        "09988": [
            _minute("2026-08-14 10:00:00", 100.0),
            _minute("2026-08-14 10:01:00", 97.0, -3.0),
        ],
    }

    with patch("src.services.hk_ggt_monitor_service.get_market_now", return_value=datetime(2026, 8, 14, 10, 2)):
        result = HkGgtMonitorService(
            db=db,
            config=SimpleNamespace(hk_list=["HK09988"]),
        ).get_realtime_snapshot("20260814")

    assert [item["hk_code"] for item in result["top_drawdowns"]] == ["09988"]
    assert [item["hk_code"] for item in result["top_gainers"]] == ["09988"]
    assert [item["hk_code"] for item in result["items"]] == ["00700", "09988"]
