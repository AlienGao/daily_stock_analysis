# -*- coding: utf-8 -*-
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.services.hk_stock_service import HkStockService


def _component(code: str, name: str):
    return SimpleNamespace(
        hk_code=code,
        to_dict=lambda: {
            "trade_date": "20260630",
            "hk_code": code,
            "name": name,
            "latest_price": 0.0,
            "pct_change": 0.0,
        }
    )


def _bar(trade_date: str, close: float, pct_chg: float | None = None):
    return SimpleNamespace(
        hk_code="",
        trade_date=trade_date,
        close=close,
        pct_chg=pct_chg,
    )


def test_list_components_uses_latest_market_trade_date_pct_change():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260630"
    db.list_hk_ggt_components.return_value = [
        _component("00700", "腾讯控股"),
        _component("09988", "阿里巴巴"),
    ]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {
        "00700": "20260630",
        "09988": "20260629",
    }

    db.list_hk_stock_daily_bars_batch.return_value = {
        "00700": [
            _bar("20260629", 400.0, None),
            _bar("20260630", 410.0, 2.5),
        ],
        "09988": [_bar("20260629", 120.0, -3.0)],
    }

    result = HkStockService(db=db).list_components()

    assert result["trade_date"] == "20260630"
    by_code = {item["hk_code"]: item for item in result["items"]}
    assert by_code["00700"]["latest_price"] == 410.0
    assert by_code["00700"]["pct_change"] == 2.5
    assert by_code["09988"]["latest_price"] is None
    assert by_code["09988"]["pct_change"] is None


def test_list_components_recomputes_pct_change_from_latest_two_closes():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260706"
    db.list_hk_ggt_components.return_value = [
        _component("02650", "挚达科技"),
    ]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {
        "02650": "20260706",
    }
    db.list_hk_stock_daily_bars_batch.return_value = {
        "02650": [
            _bar("20260703", 20.48, None),
            _bar("20260706", 29.90, -4.75),
        ],
    }

    result = HkStockService(db=db).list_components()

    item = result["items"][0]
    assert item["latest_price"] == 29.90
    assert item["pct_change"] == 46.0


def test_list_components_includes_latest_boll_distances():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260706"
    db.list_hk_ggt_components.return_value = [
        _component("00700", "腾讯控股"),
    ]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {
        "00700": "20260706",
    }
    db.list_hk_stock_daily_bars_batch.return_value = {
        "00700": [
            _bar(f"202606{day:02d}", 100.0 + idx)
            for idx, day in enumerate(range(11, 31), start=1)
        ][:-6] + [
            _bar(f"202607{day:02d}", 114.0 + day)
            for day in range(1, 7)
        ],
    }

    result = HkStockService(db=db).list_components()

    item = result["items"][0]
    assert item["boll_mid"] == 110.5
    assert item["boll_upper"] == 122.0326
    assert item["boll_lower"] == 98.9674
    assert item["boll_mid_dist_pct"] == 8.6
    assert item["boll_upper_dist_pct"] == -1.67
    assert item["boll_lower_dist_pct"] == 21.25


def test_list_components_fills_blank_name_from_stock_index():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260710"
    db.list_hk_ggt_components.return_value = [
        _component("00522", ""),
    ]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {}
    db.list_hk_stock_daily_bars_batch.return_value = {}

    with patch("src.services.hk_stock_service.get_index_stock_name", return_value="ASMPT"):
        result = HkStockService(db=db).list_components()

    assert result["items"][0]["name"] == "ASMPT"


def test_list_components_refreshes_ggt_snapshot_before_reading_latest_date():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260709"
    db.list_hk_ggt_components.return_value = [
        _component("00700", "腾讯控股"),
    ]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {
        "00700": "20260709",
    }
    db.list_hk_stock_daily_bars_batch.return_value = {
        "00700": [
            _bar("20260708", 500.0),
            _bar("20260709", 510.0),
        ],
    }

    with patch("data_provider.akshare_fetcher.AkshareFetcher") as fetcher_cls, \
            patch("src.services.hk_ggt_monitor_service.get_market_now") as get_market_now:
        get_market_now.return_value.strftime.return_value = "20260709"
        fetcher_cls.return_value.fetch_hk_ggt_components.return_value = [
            {"hk_code": "00700", "name": "腾讯控股"},
        ]
        result = HkStockService(db=db).list_components(refresh=True)

    db.replace_hk_ggt_components.assert_called_once_with(
        "20260709",
        [{"hk_code": "00700", "name": "腾讯控股"}],
    )
    assert result["trade_date"] == "20260709"
