# -*- coding: utf-8 -*-
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.app import create_app


def test_list_hk_ggt_components():
    app = create_app()
    client = TestClient(app)
    payload = {
        "trade_date": "20260624",
        "total": 1,
        "items": [{
            "trade_date": "20260624",
            "hk_code": "00700",
            "name": "腾讯控股",
            "latest_price": 400.0,
            "pct_change": 1.2,
        }],
        "available_dates": ["20260624"],
    }
    with patch("api.v1.endpoints.market.HkGgtMonitorService") as mock_cls:
        mock_cls.return_value.list_components.return_value = payload
        resp = client.get("/api/v1/market/hk-ggt/components")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["hk_code"] == "00700"


def test_list_hk_stocks_passes_refresh_flag():
    app = create_app()
    client = TestClient(app)
    payload = {
        "trade_date": "20260709",
        "recent_trade_dates": ["20260709", "20260708", "20260707", "20260706", "20260703"],
        "total": 1,
        "items": [{
            "hk_code": "00700",
            "name": "腾讯控股",
            "latest_price": 510.0,
            "pct_change": 2.0,
            "high_n_price": 600.0,
            "drawdown_pct": -15.0,
            "latest_consecutive_drawdown_pct": -6.5,
            "latest_consecutive_drawdown_days": 3,
            "latest_consecutive_drawdown_start_date": "20260701",
            "latest_consecutive_drawdown_end_date": "20260706",
        }],
    }
    with patch("api.v1.endpoints.market.HkStockService") as mock_cls:
        mock_cls.return_value.list_components.return_value = payload
        resp = client.get("/api/v1/market/hk-stocks", params={"refresh": "true"})

    assert resp.status_code == 200
    mock_cls.return_value.list_components.assert_called_once_with(refresh=True)
    assert resp.json()["items"][0]["hk_code"] == "00700"
    assert resp.json()["items"][0]["high_n_price"] == 600.0
    assert resp.json()["items"][0]["drawdown_pct"] == -15.0
    assert resp.json()["recent_trade_dates"] == ["20260709", "20260708", "20260707", "20260706", "20260703"]
    assert resp.json()["items"][0]["latest_consecutive_drawdown_pct"] == -6.5
    assert resp.json()["items"][0]["latest_consecutive_drawdown_days"] == 3
    assert resp.json()["items"][0]["latest_consecutive_drawdown_start_date"] == "20260701"
    assert resp.json()["items"][0]["latest_consecutive_drawdown_end_date"] == "20260706"


def test_get_hk_ggt_minutes():
    app = create_app()
    client = TestClient(app)
    payload = {
        "hk_code": "00700",
        "trade_date": "20260624",
        "total": 1,
        "items": [{
            "hk_code": "00700",
            "trade_date": "20260624",
            "bar_time": "2026-06-24 10:15:00",
            "close": 401.0,
            "period": "1",
            "source": "tushare_rt",
        }],
    }
    with patch("api.v1.endpoints.market.HkGgtMonitorService") as mock_cls:
        mock_cls.return_value.get_minute_bars.return_value = payload
        resp = client.get("/api/v1/market/hk-ggt/00700/minutes", params={"trade_date": "20260624"})
    assert resp.status_code == 200
    assert resp.json()["items"][0]["source"] == "tushare_rt"


def test_get_hk_stock_realtime_drawdown_ranking():
    app = create_app()
    client = TestClient(app)
    payload = {
        "trade_date": "20260814",
        "updated_at": "2026-08-14 10:15:00",
        "market_open": True,
        "total": 1,
        "items": [{
            "hk_code": "00700",
            "name": "腾讯控股",
            "latest_price": 505.0,
            "pct_change": -1.2,
            "bar_time": "2026-08-14 10:15:00",
            "intraday_consecutive_drawdown_pct": -3.5,
            "intraday_consecutive_drawdown_minutes": 4,
            "intraday_consecutive_drawdown_start_time": "2026-08-14 10:11:00",
            "intraday_consecutive_drawdown_end_time": "2026-08-14 10:15:00",
        }],
        "top_drawdowns": [{
            "hk_code": "00700",
            "name": "腾讯控股",
            "latest_price": 505.0,
            "intraday_consecutive_drawdown_pct": -3.5,
            "intraday_consecutive_drawdown_minutes": 4,
            "intraday_consecutive_drawdown_start_time": "2026-08-14 10:11:00",
            "intraday_consecutive_drawdown_end_time": "2026-08-14 10:15:00",
        }],
    }
    with patch("api.v1.endpoints.market.HkGgtMonitorService") as mock_cls:
        mock_cls.return_value.get_realtime_snapshot.return_value = payload
        resp = client.get("/api/v1/market/hk-stocks/realtime")

    assert resp.status_code == 200
    assert resp.json()["top_drawdowns"][0]["hk_code"] == "00700"
    assert resp.json()["top_drawdowns"][0]["intraday_consecutive_drawdown_pct"] == -3.5
