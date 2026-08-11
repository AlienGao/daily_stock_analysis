# -*- coding: utf-8 -*-
from datetime import datetime
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


def _bar(trade_date: str, close: float, pct_chg: float | None = None, high: float | None = None):
    return SimpleNamespace(
        hk_code="",
        trade_date=trade_date,
        high=close if high is None else high,
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
    db.batch_get_hk_stock_all_time_high.return_value = {
        "00700": 500.0,
        "09988": 180.0,
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
    assert by_code["00700"]["high_n_price"] == 500.0
    assert by_code["00700"]["drawdown_pct"] == -18.0
    assert by_code["09988"]["latest_price"] is None
    assert by_code["09988"]["pct_change"] is None
    assert by_code["09988"]["high_n_price"] is None
    assert by_code["09988"]["drawdown_pct"] is None


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


def test_list_components_includes_latest_drawdown_from_database_all_time_high():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260706"
    db.list_hk_ggt_components.return_value = [_component("00700", "腾讯控股")]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {"00700": "20260706"}
    db.batch_get_hk_stock_all_time_high.return_value = {"00700": 200.0}
    db.list_hk_stock_daily_bars_batch.return_value = {
        "00700": [
            _bar("20260701", 100.0),
            _bar("20260702", 120.0, high=150.0),
            _bar("20260703", 110.0),
            _bar("20260706", 90.0),
        ],
    }

    result = HkStockService(db=db).list_components()

    item = result["items"][0]
    assert item["high_n_price"] == 200.0
    assert item["drawdown_pct"] == -55.0
    assert item["latest_consecutive_drawdown_pct"] == -25.0
    assert item["latest_consecutive_drawdown_days"] == 2
    assert item["latest_consecutive_drawdown_start_date"] == "20260702"
    assert item["latest_consecutive_drawdown_end_date"] == "20260706"
    assert result["recent_trade_dates"] == ["20260706", "20260703", "20260702", "20260701"]


def test_list_components_returns_only_five_latest_trade_dates_for_recent_drawdown_filter():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260707"
    db.list_hk_ggt_components.return_value = [_component("00700", "腾讯控股")]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {"00700": "20260707"}
    db.batch_get_hk_stock_all_time_high.return_value = {"00700": 150.0}
    db.list_hk_stock_daily_bars_batch.return_value = {
        "00700": [
            _bar("20260701", 100.0),
            _bar("20260702", 101.0),
            _bar("20260703", 102.0),
            _bar("20260704", 103.0),
            _bar("20260705", 104.0),
            _bar("20260706", 105.0),
            _bar("20260707", 106.0),
        ],
    }

    result = HkStockService(db=db).list_components()

    assert result["recent_trade_dates"] == [
        "20260707",
        "20260706",
        "20260705",
        "20260704",
        "20260703",
    ]


def test_list_components_skips_single_day_drop_for_latest_consecutive_drawdown():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260707"
    db.list_hk_ggt_components.return_value = [_component("00700", "腾讯控股")]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {"00700": "20260707"}
    db.batch_get_hk_stock_all_time_high.return_value = {"00700": 150.0}
    db.list_hk_stock_daily_bars_batch.return_value = {
        "00700": [
            _bar("20260701", 100.0),
            _bar("20260702", 95.0),
            _bar("20260703", 90.0),
            _bar("20260706", 92.0),
            _bar("20260707", 91.0),
        ],
    }

    item = HkStockService(db=db).list_components()["items"][0]

    assert item["latest_consecutive_drawdown_pct"] == -10.0
    assert item["latest_consecutive_drawdown_days"] == 2
    assert item["latest_consecutive_drawdown_start_date"] == "20260701"
    assert item["latest_consecutive_drawdown_end_date"] == "20260703"


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
        service = HkStockService(db=db)
        with patch.object(service, "backfill_daily", return_value=2) as backfill:
            result = service.list_components(refresh=True)

    db.replace_hk_ggt_components.assert_called_once_with(
        "20260709",
        [{"hk_code": "00700", "name": "腾讯控股"}],
    )
    backfill.assert_called_once()
    assert result["trade_date"] == "20260709"


def test_list_components_refreshes_latest_prices_before_pct_change_sorting():
    db = MagicMock()
    db.get_latest_hk_ggt_trade_date.return_value = "20260730"
    db.list_hk_ggt_components.return_value = [
        _component("00668", "安克创新"),
        _component("01876", "百威亚太"),
    ]
    db.batch_get_latest_hk_stock_daily_trade_date.return_value = {
        "00668": "20260729",
        "01876": "20260729",
    }
    db.list_hk_stock_daily_bars_batch.return_value = {
        "00668": [
            _bar("20260728", 107.30),
            _bar("20260729", 114.90),
        ],
        "01876": [
            _bar("20260728", 6.93),
            _bar("20260729", 7.50),
        ],
    }
    service = HkStockService(db=db)

    stale = service.list_components()
    stale_by_code = {item["hk_code"]: item for item in stale["items"]}
    assert stale_by_code["01876"]["pct_change"] == 8.23
    assert stale_by_code["00668"]["pct_change"] == 7.08

    def sync_latest_daily(**_kwargs):
        db.batch_get_latest_hk_stock_daily_trade_date.return_value = {
            "00668": "20260730",
            "01876": "20260730",
        }
        db.list_hk_stock_daily_bars_batch.return_value = {
            "00668": [
                _bar("20260729", 107.30),
                _bar("20260730", 118.30),
            ],
            "01876": [
                _bar("20260729", 6.93),
                _bar("20260730", 7.05),
            ],
        }
        return 4

    with patch("src.services.hk_ggt_monitor_service.HkGgtMonitorService"), \
            patch("src.services.hk_stock_service.get_market_now", return_value=datetime(2026, 7, 30, 16, 10)), \
            patch.object(service, "backfill_daily", side_effect=sync_latest_daily) as backfill:
        refreshed = service.list_components(refresh=True)

    backfill.assert_called_once_with(
        codes=["00668", "01876"],
        start_date="20260131",
        end_date="20260730",
    )
    refreshed_by_code = {item["hk_code"]: item for item in refreshed["items"]}
    assert refreshed_by_code["00668"]["latest_price"] == 118.30
    assert refreshed_by_code["00668"]["pct_change"] == 10.25
    assert refreshed_by_code["01876"]["latest_price"] == 7.05
    assert refreshed_by_code["01876"]["pct_change"] == 1.73

    sorted_codes = [
        item["hk_code"]
        for item in sorted(
            refreshed["items"],
            key=lambda item: item["pct_change"],
            reverse=True,
        )
    ]
    assert sorted_codes == ["00668", "01876"]
