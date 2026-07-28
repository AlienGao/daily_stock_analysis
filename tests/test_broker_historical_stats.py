# -*- coding: utf-8 -*-
"""金股历史推荐统计单元测试。"""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services.broker_recommend_service import (
    BrokerRecommendService,
    _holding_final_return,
    _holding_max_drawdown,
)


def _daily(cum_series):
    return [
        {"date": f"2025010{i}", "price": 10.0, "cumulative": c}
        for i, c in enumerate(cum_series, start=1)
    ]


class TestHoldingStatsHelpers:
    def test_final_return_from_cumulative(self):
        drs = _daily([0.0, 0.05, 0.12])
        assert _holding_final_return(drs) == pytest.approx(0.12)

    def test_max_drawdown_peak_to_trough(self):
        drs = _daily([0.0, 0.1, 0.05, -0.02])
        assert _holding_max_drawdown(drs) == pytest.approx(-0.12)


class TestGetHistoricalRecommendStats:
    def test_aggregates_adj_period_returns_without_full_resolve(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        svc.db.get_broker_recommend_month_counts.return_value = {"600519.SH": 2}
        svc.db.get_all_broker_backtests.return_value = [
            {
                "month": "202501",
                "buy_date": "20250102",
                "sell_date": "20250127",
                "stock_returns": [{
                    "ts_code": "600519.SH",
                    "daily_returns": [
                        {"date": "20250102", "price": 100.0},
                        {"date": "20250127", "price": 115.0},
                    ],
                }],
            },
            {
                "month": "202502",
                "buy_date": "20250205",
                "sell_date": "20250228",
                "stock_returns": [{
                    "ts_code": "600519.SH",
                    "daily_returns": [
                        {"date": "20250205", "price": 100.0},
                        {"date": "20250228", "price": 77.99},
                    ],
                }],
            },
        ]

        with patch.object(svc, "_load_all_adj_factors", return_value={}), patch.object(
            svc,
            "_resolve_stock_holding_daily_returns",
        ) as resolve:
            out = svc.get_historical_recommend_stats(["600519.SH"])

        assert resolve.call_count == 0
        row = out["600519.SH"]
        assert row["period_count"] == 2
        assert row["win_rate"] == pytest.approx(0.5)
        assert row["max_return"] == pytest.approx(0.15)
        assert row["max_drawdown"] == pytest.approx(-0.2201)


class TestBrokerDailyChange:
    @staticmethod
    def _spot(**overrides):
        import pandas as pd

        row = {
            "code": "600519",
            "price": 105.0,
            "pct_chg": 5.0,
            "pre_close": 100.0,
            "open_price": 101.0,
            "high": 106.0,
            "low": 99.0,
            "trade_date": date.today().isoformat(),
        }
        row.update(overrides)
        return pd.DataFrame([row]).set_index("code")

    def test_uses_current_snapshot_pct_change(self):
        db = MagicMock()
        db.get_current_prices.return_value = self._spot(pct_chg=9.0)

        with patch("src.storage.DatabaseManager", return_value=db):
            _, changes, _, change_dates = BrokerRecommendService()._get_realtime_prices_batch(["600519.SH"])

        assert changes == {"600519.SH": 0.05}
        assert change_dates == {"600519.SH": date.today().strftime("%Y%m%d")}
        db.get_session.assert_not_called()

    def test_computes_current_change_from_pre_close_when_pct_is_missing(self):
        db = MagicMock()
        db.get_current_prices.return_value = self._spot(pct_chg=None)

        with patch("src.storage.DatabaseManager", return_value=db):
            _, changes, _, change_dates = BrokerRecommendService()._get_realtime_prices_batch(["600519.SH"])

        assert changes == {"600519.SH": 0.05}
        assert change_dates == {"600519.SH": date.today().strftime("%Y%m%d")}

    def test_falls_back_to_each_stocks_latest_daily_change(self):
        import pandas as pd

        db = MagicMock()
        db.get_current_prices.return_value = pd.concat([
            self._spot(code="600519", trade_date="2026-07-24", pct_chg=None),
            self._spot(code="000001", trade_date="2026-07-23", pct_chg=None),
        ])
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            ("000001", date(2026, 7, 23), 9.8, -2.0),
            ("000001", date(2026, 7, 22), 10.0, None),
            ("600519", date(2026, 7, 24), 105.0, None),
            ("600519", date(2026, 7, 23), 100.0, 1.0),
        ]
        db.get_session.return_value = session
        session.__enter__.return_value = session

        with patch("src.storage.DatabaseManager", return_value=db):
            _, changes, _, change_dates = BrokerRecommendService()._get_realtime_prices_batch([
                "600519.SH", "000001.SZ",
            ])

        assert changes == {"600519.SH": 0.05, "000001.SZ": -0.02}
        assert change_dates == {"600519.SH": "20260724", "000001.SZ": "20260723"}

    def test_falls_back_to_daily_change_when_spot_is_empty(self):
        import pandas as pd

        db = MagicMock()
        db.get_current_prices.return_value = pd.DataFrame()
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            ("600519", date(2026, 7, 24), 101.0, 1.0),
            ("600519", date(2026, 7, 23), 100.0, None),
        ]
        db.get_session.return_value = session
        session.__enter__.return_value = session

        with patch("src.storage.DatabaseManager", return_value=db):
            prices, changes, _, change_dates = BrokerRecommendService()._get_realtime_prices_batch(["600519.SH"])

        assert prices == {}
        assert changes == {"600519.SH": 0.01}
        assert change_dates == {"600519.SH": "20260724"}


class TestPeriodReturnFromDailyReturns:
    def test_ex_dividend_uses_adj_factor_on_stored_prices(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        daily = [
            {"date": "20250603", "price": 20.0},
            {"date": "20250604", "price": 10.0},
            {"date": "20250627", "price": 10.5},
        ]
        adj_all = {"600188": {"20250603": 1.0, "20250604": 2.0, "20250627": 2.0}}
        ret = svc._period_return_from_daily_returns("600188.SH", daily, adj_all)
        assert ret == pytest.approx(0.05)


class TestSyncDailyReturnsFromOhlc:
    """除权月：展示价跳空，累计收益应走后复权口径。"""

    def test_ex_dividend_does_not_fake_deep_drawdown(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        # 除权前 20，除权后不复权收盘约 10（因子 2），后复权应连续
        raw_ohlc = {
            "20250603": {"open": 19.5, "high": 20.2, "low": 19.0, "close": 20.0},
            "20250604": {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.0},
            "20250627": {"open": 10.2, "high": 10.8, "low": 10.0, "close": 10.5},
        }
        adj_map = {
            "20250603": 1.0,
            "20250604": 2.0,
            "20250627": 2.0,
        }
        daily = [
            {"date": "20250603", "price": 20.0},
            {"date": "20250604", "price": 10.0},
            {"date": "20250627", "price": 10.5},
        ]

        with patch.object(svc, "_prefetch_ohlc", return_value={"600188.SH": raw_ohlc}), patch.object(
            svc, "_load_all_adj_factors", return_value={"600188": adj_map},
        ):
            out = svc._sync_daily_returns_from_ohlc(
                "600188.SH", daily, "20250603", "20250627",
            )

        assert len(out) == 3
        assert out[0]["price"] == pytest.approx(20.0)
        assert out[1]["price"] == pytest.approx(10.0)
        assert out[-1]["cumulative"] == pytest.approx(0.05)
        assert out[1]["cumulative"] > -0.05

class TestMonthCumulativeReturn:
    def test_month_cumulative_matches_adj_cumulative_with_unadj_display_prices(self):
        """end_price 为不复权时，当月累计收益仍应与 daily_returns 后复权累计一致。"""
        svc = BrokerRecommendService()
        sr = {
            "ts_code": "000651.SZ",
            "end_price": 46.42,
            "daily_returns": [
                {"date": "20250506", "price": 45.47, "cumulative": 0.0},
                {"date": "20250530", "price": 46.42, "cumulative": 0.0431},
            ],
        }
        assert svc._month_cumulative_return_from_stock(sr) == 0.0431

class TestCurrentMonthStockReturns:
    def test_get_current_month_stock_returns_structure(self):
        svc = BrokerRecommendService()
        with patch.object(svc, "_get_trading_days", return_value=["20260602", "20260611"]), \
             patch.object(svc, "_effective_month_end", return_value="20260611"), \
             patch.object(svc, "_prefetch_prices", return_value={
                 "000651.SZ": {"20260602": 40.0, "20260611": 42.0},
             }), \
             patch.object(svc, "_load_all_adj_factors", return_value={
                 "000651": {"20260602": 1.0, "20260611": 1.0},
             }), \
             patch.object(svc, "_get_realtime_prices_batch", return_value=({}, {}, {}, {})), \
             patch.object(svc, "_sync_daily_returns_from_ohlc", side_effect=lambda _ts, drs, *_a: drs), \
             patch("src.services.broker_recommend_service.date") as mock_date:
            mock_date.today.return_value.strftime.return_value = "202606"
            out = svc.get_current_month_stock_returns(["000651.SZ"])
        assert out["month"] == "202606"
        assert out["buy_date"] == "20260602"
        assert out["sell_date"] == "20260611"
        assert out["items"][0]["end_date"] == "20260611"
        assert out["items"][0]["cumulative_return"] == pytest.approx(0.05)

    def test_current_month_suspended_stock_without_month_prices_returns_null(self):
        """本月无真实行情的停牌股不展示当月累计收益。"""
        svc = BrokerRecommendService()
        with patch.object(svc, "_get_trading_days", return_value=["20260701", "20260706"]), \
             patch.object(svc, "_effective_month_end", return_value="20260706"), \
             patch.object(svc, "_prefetch_prices", return_value={
                 "000524.SZ": {"20260630": 7.88},
             }), \
             patch.object(svc, "_load_all_adj_factors", return_value={
                 "000524": {"20260630": 1.0},
             }), \
             patch.object(svc, "_get_realtime_prices_batch", return_value=({}, {}, {}, {})), \
             patch.object(svc, "_sync_daily_returns_from_ohlc", side_effect=lambda _ts, drs, *_a: drs), \
             patch("src.services.broker_recommend_service.date") as mock_date:
            mock_date.today.return_value.strftime.return_value = "202607"
            out = svc.get_current_month_stock_returns(["000524.SZ"])

        assert out["items"][0]["ts_code"] == "000524.SZ"
        assert out["items"][0]["cumulative_return"] is None

    def test_current_month_backtest_does_not_use_realtime_prev_close_as_return_basis(self):
        """本月无真实月内行情时，实时价/昨收不得拼成券商月收益。"""
        import pandas as pd

        svc = BrokerRecommendService()
        svc.db = MagicMock()
        svc.db.save_daily_data = MagicMock()
        recs = pd.DataFrame([{
            "ts_code": "000524.SZ",
            "name": "岭南控股",
            "broker": "中银证券",
            "broker_count": 1,
        }])
        with patch.object(svc, "_effective_month_end", return_value="20260706"), \
             patch.object(svc, "_get_trading_days", return_value=["20260701", "20260706"]), \
             patch.object(svc, "get_monthly_recommendations", return_value=recs), \
             patch.object(svc, "_prefetch_prices", return_value={}), \
             patch.object(svc, "_load_all_adj_factors", return_value={
                 "000524": {"20260706": 1.0},
             }), \
             patch.object(svc, "_get_realtime_prices_batch", return_value=(
                 {"000524.SZ": {"20260706": 44.65}},
                 {"000524.SZ": 0.0},
                 {},
                 {"000524.SZ": "2026-07-06"},
             )), \
             patch.object(svc, "_sync_daily_returns_from_ohlc", side_effect=lambda _ts, drs, *_a: drs), \
             patch("src.services.broker_recommend_service.date") as mock_date:
            mock_date.today.return_value.strftime.side_effect = lambda fmt: {
                "%Y%m": "202607",
                "%Y%m%d": "20260706",
            }[fmt]
            out = svc.compute_backtest("202607")

        assert out["brokers"] == []
        assert out["stock_returns"][0]["ts_code"] == "000524.SZ"
        assert out["stock_returns"][0]["month_cumulative_return"] is None

class TestResolveSellDateWithAdj:
    def test_resolve_sell_date_without_exact_adj(self):
        """截止日无当日复权因子时回退至上一交易日。"""
        svc = BrokerRecommendService()
        trading_days = ["20260602", "20260610", "20260611"]
        adj_map = {"20260602": 2.0, "20260610": 2.0}
        assert svc._resolve_sell_date_with_adj(
            "300502.SZ", trading_days, "20260611", adj_map,
        ) == "20260610"

    def test_get_current_month_falls_back_when_no_today_adj(self):
        svc = BrokerRecommendService()
        with patch.object(svc, "_get_trading_days", return_value=["20260602", "20260610", "20260611"]),              patch.object(svc, "_effective_month_end", return_value="20260611"),              patch.object(svc, "_prefetch_prices", return_value={
                 "300502.SZ": {"20260602": 100.0, "20260610": 110.0, "20260611": 80.0},
             }),              patch.object(svc, "_load_all_adj_factors", return_value={
                 "300502": {"20260602": 1.0, "20260610": 1.0},
             }),              patch.object(svc, "_get_realtime_prices_batch", return_value=({}, {}, {}, {})),              patch.object(svc, "_sync_daily_returns_from_ohlc", side_effect=lambda _ts, drs, *_a: drs),              patch("src.services.broker_recommend_service.date") as mock_date:
            mock_date.today.return_value.strftime.return_value = "202606"
            out = svc.get_current_month_stock_returns(["300502.SZ"])
        assert out["items"][0]["end_date"] == "20260610"
        assert out["items"][0]["cumulative_return"] == pytest.approx(0.1)



class TestPrevMonthCurrentTop:
    def test_get_prev_month_current_returns_top_sorts_and_limits(self):
        import pandas as pd
        svc = BrokerRecommendService()
        with patch.object(svc, "get_monthly_recommendations", return_value=pd.DataFrame([
            {"ts_code": "000001.SZ", "name": "平安", "broker_count": 2, "broker": "A"},
            {"ts_code": "000002.SZ", "name": "万科", "broker_count": 1, "broker": "B"},
            {"ts_code": "000003.SZ", "name": "缺收益", "broker_count": 1, "broker": "C"},
        ])), patch.object(svc, "get_current_month_stock_returns", return_value={
            "month": "202606",
            "buy_date": "20260601",
            "sell_date": "20260611",
            "items": [
                {"ts_code": "000001.SZ", "cumulative_return": 0.1, "end_date": "20260611"},
                {"ts_code": "000002.SZ", "cumulative_return": 0.25, "end_date": "20260611"},
                {"ts_code": "000003.SZ", "cumulative_return": None, "end_date": "20260610"},
            ],
        }), patch("src.services.broker_recommend_service.date") as mock_date:
            mock_date.today.return_value.strftime.return_value = "202606"
            out = svc.get_prev_month_current_returns_top(top_n=2)
        assert out["prev_month"] == "202605"
        assert out["current_month"] == "202606"
        assert len(out["items"]) == 2
        assert out["items"][0]["ts_code"] == "000002.SZ"
        assert out["items"][0]["cumulative_return"] == 0.25
        assert out["items"][1]["ts_code"] == "000001.SZ"
        assert out["items"][0].get("name") == "万科"
    def test_resolve_broker_stock_names_falls_back_to_index(self):
        svc = BrokerRecommendService()
        with patch("src.data.stock_index_loader.get_index_stock_name", return_value="贵州茅台"):
            names = svc._resolve_broker_stock_names(["600519.SH"], {"600519.SH": ""})
        assert names["600519.SH"] == "贵州茅台"

class TestYtdBacktestCurrentMonth:
    def test_ytd_includes_live_current_month_when_not_persisted(self):
        svc = BrokerRecommendService()
        cm = datetime.now().strftime("%Y%m")
        stored = [{
            "month": f"{cm[:4]}05",
            "brokers": [{
                "broker": "华泰证券",
                "cumulative_return": 0.05,
                "stock_count": 3,
                "win_rate": 0.6,
                "daily_returns": [{"date": f"{cm[:4]}0530", "cumulative": 0.05}],
            }],
        }]
        live = {
            "month": cm,
            "brokers": [{
                "broker": "华泰证券",
                "cumulative_return": 0.1019,
                "stock_count": 9,
                "win_rate": 0.5556,
                "daily_returns": [{"date": f"{cm}11", "cumulative": 0.1019}],
            }],
        }
        with patch.object(svc.db, "get_all_broker_backtests", return_value=stored):
            with patch.object(svc, "_append_live_current_month_backtest", side_effect=lambda x: x + [live]):
                result = svc.compute_ytd_backtest(year=cm[:4], top_n=5)
        huatai = next(b for b in result["brokers"] if b["broker"] == "华泰证券")
        months = [mr["month"] for mr in huatai["monthly_returns"]]
        assert cm in months
        assert months == sorted(months, reverse=True)
