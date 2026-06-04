# -*- coding: utf-8 -*-
"""金股历史推荐统计单元测试。"""

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
