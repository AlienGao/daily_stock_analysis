# -*- coding: utf-8 -*-
"""金股升转降策略回测单元测试。"""

from unittest.mock import MagicMock, patch

import pytest

from src.services.broker_recommend_service import BrokerRecommendService


def _make_backtest(month: str, stocks, trading_days=None):
    if trading_days is None:
        trading_days = [
            f"{month}02",
            f"{month}03",
            f"{month}06",
            f"{month}28",
        ]
    return {
        "month": month,
        "buy_date": trading_days[0],
        "sell_date": trading_days[-1],
        "stock_returns": [
            {
                "ts_code": ts_code,
                "name": name,
                "daily_returns": [
                    {
                        "date": td,
                        "open": open_px,
                        "price": close_px,
                        "cumulative": cum,
                    }
                    for td, open_px, close_px, cum in bars
                ],
            }
            for ts_code, name, bars in stocks
        ],
    }


def _up_to_down_cache(days: list, ts_code: str = "600519.SH") -> dict:
    """首日上升、次日升转降。"""
    cache = {}
    for i, d in enumerate(days):
        if i == 0:
            cache[d] = {ts_code: {"up_count": 5, "down_count": 0}}
        else:
            cache[d] = {ts_code: {"up_count": 0, "down_count": 1}}
    return cache


class TestEqualWeightStrategy:
    def test_prev_month_str_handles_year_boundary(self):
        assert BrokerRecommendService._prev_month_str("202501") == "202412"
        assert BrokerRecommendService._prev_month_str("202503") == "202502"

    def test_compute_up_to_down_trade_stats_groups_by_prev_up_count(self):
        monthly = [
            {
                "stocks": [
                    {
                        "month_return": 0.1,
                        "buy_reason": {
                            "trigger": "nineturn_up_to_down_buy",
                            "prev_nineturn_up_count": 8,
                        },
                    },
                    {
                        "month_return": -0.05,
                        "buy_reason": {
                            "trigger": "nineturn_up_to_down_buy",
                            "prev_nineturn_up_count": 8,
                        },
                    },
                ],
            },
        ]
        stats = BrokerRecommendService._compute_up_to_down_trade_stats(monthly)
        by_up = {s["up_count"]: s for s in stats}
        assert len(stats) == 8
        assert by_up[8]["trade_count"] == 2
        assert by_up[8]["avg_return"] == pytest.approx(0.025)
        assert by_up[8]["win_rate"] == pytest.approx(0.5)
        assert by_up[1]["trade_count"] == 0

    def test_nineturn_up_to_down_on_day(self):
        days = ["20250102", "20250103", "20250106"]
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 1}},
        }
        assert BrokerRecommendService._nineturn_up_to_down_on_day(
            cache, days, 2, "600519.SH",
        )

    def test_nineturn_up_to_down_ignores_non_allowed_up_counts(self):
        days = ["20250102", "20250103"]
        for prev_up in (9, 10, 11, 15):
            cache = {
                "20250102": {"600519.SH": {"up_count": prev_up, "down_count": 0}},
                "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            }
            assert not BrokerRecommendService._nineturn_up_to_down_on_day(
                cache, days, 1, "600519.SH",
            )

    def test_nineturn_up_to_down_accepts_up1_through_up8(self):
        days = ["20250102", "20250103"]
        for prev_up in (1, 2, 7, 8):
            cache = {
                "20250102": {"600519.SH": {"up_count": prev_up, "down_count": 0}},
                "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            }
            assert BrokerRecommendService._nineturn_up_to_down_on_day(
                cache, days, 1, "600519.SH",
            )

    def test_nineturn_up_to_down_cross_month_first_day(self):
        days = ["20250603", "20250604", "20250605"]
        cache = {
            "20250530": {"600519.SH": {"up_count": 5, "down_count": 0}},
            "20250603": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250604": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250605": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        assert BrokerRecommendService._nineturn_up_to_down_on_day(
            cache, days, 0, "600519.SH", prev_month_last_day="20250530",
        )

    def test_simulate_accepts_cross_month_first_day_signal(self):
        days = ["20250603", "20250604", "20250605", "20250606"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250530": {"600519.SH": {"up_count": 3, "down_count": 0}},
            "20250603": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250604": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250605": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250606": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days,
            stock_books,
            cache,
            nav_start=1.0,
            total_capital=1.0,
            prev_month_last_day="20250530",
        )
        assert result is not None
        assert result["stock_count"] == 1

    def test_simulate_ignores_up9_up_to_down_signal(self):
        days = ["20250102", "20250103", "20250106", "20250107"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 9, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250107": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert result["stock_count"] == 0

    def test_simulate_accepts_up1_up_to_down_signal(self):
        days = ["20250102", "20250103", "20250106", "20250107"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 1, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250107": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert result["stock_count"] == 1

    def test_buy_does_not_inject_cash_after_prior_loss(self):
        days = ["20250102", "20250103", "20250106", "20250107", "20250108", "20250109"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 90.0,
                    "20250108": 100.0,
                    "20250109": 90.0,
                },
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250107": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250108": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250109": {"600519.SH": {"up_count": 0, "down_count": 1}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days[:5], stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert len(result["stocks"]) == 1
        assert result["stocks"][0]["buy_amount"] == pytest.approx(1.0)
        assert result["nav_end"] == pytest.approx(0.9)

    def test_simulate_up_to_down_buys_t1_open_sells_t2_open_on_loss(self):
        days = ["20250102", "20250103", "20250106", "20250107"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250107": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days,
            stock_books,
            cache,
            nav_start=1.0,
            total_capital=1.0,
        )
        assert result is not None
        assert len(result["stocks"]) == 1
        leg = result["stocks"][0]
        assert leg["buy_date"] == "20250106"
        assert leg["sell_date"] == "20250107"
        assert leg["buy_amount"] == pytest.approx(1.0)
        assert leg["buy_reason"]["trigger"] == "nineturn_up_to_down_buy"
        assert leg["sell_reason"]["trigger"] == "nineturn_up_to_down_sell_loss"

    def test_simulate_up_to_down_holds_profit_past_t3_when_close_exceeds_entry(self):
        days = ["20250102", "20250103", "20250106", "20250107", "20250108", "20250109"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 115.0,
                    "20250109": 112.0,
                },
                "adj_close": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 115.0,
                    "20250109": 112.0,
                },
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250107": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250108": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250109": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        leg = result["stocks"][0]
        assert leg["buy_date"] == "20250106"
        assert leg["sell_date"] == "20250109"
        assert leg["sell_reason"]["trigger"] == "month_end"
        assert result["nav_end"] == pytest.approx(1.12)

    def test_simulate_profit_t3_sells_when_close_not_above_entry_close(self):
        days = ["20250102", "20250103", "20250106", "20250107", "20250108"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 108.0,
                },
                "adj_close": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 100.0,
                },
            },
        }
        cache = _up_to_down_cache(days)
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        leg = result["stocks"][0]
        assert leg["sell_date"] == "20250108"
        assert leg["sell_reason"]["trigger"] == "nineturn_up_to_down_sell_profit_t3"
        assert result["nav_end"] == pytest.approx(1.0)

    def test_simulate_profit_trail_sells_on_close_loss(self):
        days = [
            "20250102", "20250103", "20250106", "20250107",
            "20250108", "20250109", "20250110",
        ]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 115.0,
                    "20250109": 105.0,
                    "20250110": 98.0,
                },
                "adj_close": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 115.0,
                    "20250109": 105.0,
                    "20250110": 98.0,
                },
            },
        }
        cache = _up_to_down_cache(days)
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        leg = result["stocks"][0]
        assert leg["sell_date"] == "20250110"
        assert leg["sell_reason"]["trigger"] == "nineturn_up_to_down_sell_profit_trail"
        assert result["nav_end"] == pytest.approx(0.98)

    def test_simulate_month_end_forces_close_on_profit_trail(self):
        """盈利跟踪未触发卖出时，月末最后交易日收盘强制清仓。"""
        month_days = ["20250102", "20250103", "20250106", "20250107"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                },
                "adj_close": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                },
            },
        }
        cache = _up_to_down_cache(month_days)
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            month_days,
            stock_books,
            cache,
            nav_start=1.0,
            total_capital=1.0,
        )
        assert result is not None
        leg = result["stocks"][0]
        assert leg["buy_date"] == "20250106"
        assert leg["sell_date"] == "20250107"
        assert leg["sell_reason"]["trigger"] == "month_end"
        assert leg["sell_reason"]["action"] == "月末强制清仓"
        assert result["nav_end"] == pytest.approx(1.1)

    def test_simulate_month_end_deferred_when_no_close_on_last_day(self):
        month_days = ["20250102", "20250103", "20250106", "20250107"]
        post_days = ["20250108"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 120.0,
                },
                "adj_close": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250108": 120.0,
                },
            },
        }
        cache = _up_to_down_cache(month_days)
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            month_days,
            stock_books,
            cache,
            nav_start=1.0,
            total_capital=1.0,
            post_month_trading_days=post_days,
        )
        assert result is not None
        leg = result["stocks"][0]
        assert leg["sell_date"] == "20250108"
        assert leg["sell_reason"]["trigger"] == "month_end_deferred"
        assert result["nav_end"] == pytest.approx(1.2)

    def test_simulate_splits_capital_across_multiple_signal_stocks(self):
        days = ["20250102", "20250103", "20250106", "20250107"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
            "000001.SZ": {
                "name": "平安",
                "adj_open": {d: 10.0 for d in days},
                "adj_close": {d: 10.0 for d in days},
            },
        }
        cache = {
            "20250102": {
                "600519.SH": {"up_count": 8, "down_count": 0},
                "000001.SZ": {"up_count": 8, "down_count": 0},
            },
            "20250103": {
                "600519.SH": {"up_count": 0, "down_count": 1},
                "000001.SZ": {"up_count": 0, "down_count": 1},
            },
            "20250106": {
                "600519.SH": {"up_count": 0, "down_count": 0},
                "000001.SZ": {"up_count": 0, "down_count": 0},
            },
            "20250107": {
                "600519.SH": {"up_count": 0, "down_count": 0},
                "000001.SZ": {"up_count": 0, "down_count": 0},
            },
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert len(result["stocks"]) == 2
        amounts = sorted(leg["buy_amount"] for leg in result["stocks"])
        assert amounts == [pytest.approx(0.5), pytest.approx(0.5)]

    def test_simulate_no_signal_liquidates_t1_and_skips_until_next_signal(self):
        days = ["20250102", "20250103", "20250106", "20250107", "20250108"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 110.0,
                },
                "adj_close": {
                    "20250102": 100.0,
                    "20250103": 100.0,
                    "20250106": 100.0,
                    "20250107": 110.0,
                    "20250108": 110.0,
                },
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250107": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250108": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert len(result["stocks"]) == 1
        leg = result["stocks"][0]
        assert leg["buy_date"] == "20250106"
        assert leg["sell_date"] == "20250108"
        assert leg["sell_reason"]["trigger"] == "month_end"
        assert result["nav_end"] == pytest.approx(1.1)

    def test_simulate_ignores_up_to_down_on_last_trading_day(self):
        days = ["20250102", "20250103", "20250106"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 1}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert result["stock_count"] == 0

    def test_simulate_skips_signal_when_t2_falls_outside_month(self):
        days = ["20250102", "20250103", "20250106"]
        stock_books = {
            "600519.SH": {
                "name": "茅台",
                "adj_open": {d: 100.0 for d in days},
                "adj_close": {d: 100.0 for d in days},
            },
        }
        cache = {
            "20250102": {"600519.SH": {"up_count": 8, "down_count": 0}},
            "20250103": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250106": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }
        result = BrokerRecommendService._simulate_month_nineturn_rotation(
            days, stock_books, cache, nav_start=1.0, total_capital=1.0,
        )
        assert result is not None
        assert result["stock_count"] == 0

    def test_strategy_impl_uses_fixed_total_capital(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        days = ["20250102", "20250103", "20250106", "20250107"]
        svc.db.get_all_broker_backtests.return_value = [
            _make_backtest(
                "202501",
                [("600519.SH", "茅台", [
                    (d, 100.0, 100.0, 0.0) for d in days
                ])],
                trading_days=days,
            ),
        ]

        def _seed(date_pools, cache):
            for td in date_pools:
                cache.setdefault(td, {})["600519.SH"] = {
                    "up_count": 0,
                    "down_count": 0,
                }
            cache["20250102"]["600519.SH"] = {"up_count": 8, "down_count": 0}
            cache["20250103"]["600519.SH"] = {"up_count": 0, "down_count": 1}

        with patch.object(svc, "get_monthly_recommendations"), patch.object(
            svc, "_get_trading_days", return_value=days,
        ), patch.object(svc, "_prefetch_nineturn_for_dates", side_effect=_seed), patch.object(
            svc, "_load_all_adj_factors", return_value={},
        ):
            result = svc._compute_equal_weight_strategy_impl(top_n=4)

        assert "error" not in result
        assert result["strategy"] == "nineturn_up_to_down_open"
        assert result["total_capital"] == pytest.approx(1.0)
        assert result["total_months"] == 1
        assert len(result["monthly_returns"][0]["stocks"]) == 1

    def test_compute_equal_weight_strategy_returns_computing_on_first_call(self):
        BrokerRecommendService._strategy_cache.clear()
        BrokerRecommendService._strategy_computing = False
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()

        with patch.object(
            svc, "_compute_equal_weight_strategy_impl",
            return_value={"strategy": "nineturn_up_to_down_open"},
        ):
            first = svc.compute_equal_weight_strategy()
            assert first == {"status": "computing"}
            import time as time_mod
            deadline = time_mod.time() + 2.0
            while time_mod.time() < deadline:
                if not BrokerRecommendService._strategy_computing:
                    break
                time_mod.sleep(0.05)
            import json
            cache_key = json.dumps({
                "strategy": "nineturn_up_to_down_open_v49",
                "start_month": "",
                "end_month": "",
            }, sort_keys=True)
            assert BrokerRecommendService._strategy_cache.get(cache_key)
