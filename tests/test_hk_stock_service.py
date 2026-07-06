# -*- coding: utf-8 -*-
from types import SimpleNamespace
from unittest.mock import MagicMock

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
