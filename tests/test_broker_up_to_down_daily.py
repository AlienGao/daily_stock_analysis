# -*- coding: utf-8 -*-
"""金股每日升转降扫描单元测试。"""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.services.broker_recommend_service import BrokerRecommendService


class TestMonthlyUpToDownDaily:
    def test_returns_signals_for_valid_up_to_down(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        days = ["20250603", "20250604", "20250605", "20250606"]
        df = pd.DataFrame([
            {
                "ts_code": "600519.SH",
                "name": "茅台",
                "broker": "中信",
                "broker_count": 3,
            },
        ])
        cache = {
            "20250603": {"600519.SH": {"up_count": 5, "down_count": 0}},
            "20250604": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250605": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250606": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }

        with patch.object(svc, "get_monthly_recommendations", return_value=df), patch.object(
            svc, "_effective_month_end", return_value="20250606",
        ), patch.object(svc, "_get_trading_days", return_value=days), patch.object(
            svc, "_load_nineturn_by_trade_date_cache", return_value=cache,
        ), patch.object(svc, "_prefetch_nineturn_for_dates"):
            result = svc.get_monthly_up_to_down_daily("202506")

        assert result["month"] == "202506"
        assert len(result["days"]) == 1
        assert result["days"][0]["date"] == "20250604"
        stock = result["days"][0]["stocks"][0]
        assert stock["ts_code"] == "600519.SH"
        assert stock["signal_type"] == "up_to_down"
        assert stock["prev_nineturn_up_count"] == 5
        assert stock["broker_count"] == 3

    def test_returns_signals_for_valid_down_to_up(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        days = ["20250603", "20250604", "20250605", "20250606"]
        df = pd.DataFrame([
            {
                "ts_code": "000001.SZ",
                "name": "平安",
                "broker": "中信",
                "broker_count": 2,
            },
        ])
        cache = {
            "20250603": {"000001.SZ": {"up_count": 0, "down_count": 3}},
            "20250604": {"000001.SZ": {"up_count": 1, "down_count": 0}},
            "20250605": {"000001.SZ": {"up_count": 1, "down_count": 0}},
            "20250606": {"000001.SZ": {"up_count": 0, "down_count": 0}},
        }

        with patch.object(svc, "get_monthly_recommendations", return_value=df), patch.object(
            svc, "_effective_month_end", return_value="20250606",
        ), patch.object(svc, "_get_trading_days", return_value=days), patch.object(
            svc, "_load_nineturn_by_trade_date_cache", return_value=cache,
        ), patch.object(svc, "_prefetch_nineturn_for_dates"):
            result = svc.get_monthly_up_to_down_daily("202506")

        assert len(result["days"]) == 1
        stock = result["days"][0]["stocks"][0]
        assert stock["signal_type"] == "down_to_up"
        assert stock["prev_nineturn_down_count"] == 3

    def test_empty_when_no_recommendations(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        with patch.object(svc, "get_monthly_recommendations", return_value=pd.DataFrame()):
            result = svc.get_monthly_up_to_down_daily("202506")
        assert result["days"] == []

    def test_first_trading_day_uses_prev_month_last_day(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        days = ["20250603", "20250604", "20250605", "20250606"]
        df = pd.DataFrame([
            {
                "ts_code": "600519.SH",
                "name": "茅台",
                "broker": "中信",
                "broker_count": 2,
            },
        ])
        cache = {
            "20250530": {"600519.SH": {"up_count": 6, "down_count": 0}},
            "20250603": {"600519.SH": {"up_count": 0, "down_count": 1}},
            "20250604": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250605": {"600519.SH": {"up_count": 0, "down_count": 0}},
            "20250606": {"600519.SH": {"up_count": 0, "down_count": 0}},
        }

        with patch.object(svc, "get_monthly_recommendations", return_value=df), patch.object(
            svc, "_effective_month_end", return_value="20250606",
        ), patch.object(svc, "_get_trading_days", return_value=days), patch.object(
            svc, "_prev_month_last_trading_day", return_value="20250530",
        ), patch.object(
            svc, "_load_nineturn_by_trade_date_cache", return_value=cache,
        ), patch.object(svc, "_prefetch_nineturn_for_dates"):
            result = svc.get_monthly_up_to_down_daily("202506")

        assert len(result["days"]) == 1
        assert result["days"][0]["date"] == "20250603"
        stock = result["days"][0]["stocks"][0]
        assert stock["signal_type"] == "up_to_down"
        assert stock["prev_nineturn_up_count"] == 6

    def test_in_progress_month_includes_latest_trading_day(self):
        """当月数据截止日非月末时，最新交易日不应被当作末交易日忽略。"""
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        days = ["20260601", "20260602", "20260603", "20260604", "20260605"]
        df = pd.DataFrame([
            {
                "ts_code": "300593.SZ",
                "name": "新雷能",
                "broker": "光大证券",
                "broker_count": 1,
            },
        ])
        cache = {
            "20260530": {"300593.SZ": {"up_count": 0, "down_count": 0}},
            "20260604": {"300593.SZ": {"up_count": 1, "down_count": 0}},
            "20260605": {"300593.SZ": {"up_count": 0, "down_count": 1}},
        }

        with patch.object(svc, "get_monthly_recommendations", return_value=df), patch.object(
            svc, "_effective_month_end", return_value="20260605",
        ), patch.object(svc, "_get_trading_days", return_value=days), patch.object(
            svc, "_calendar_month_last_trading_day", return_value="20260630",
        ), patch.object(
            svc, "_prev_month_last_trading_day", return_value="20260530",
        ), patch.object(
            svc, "_load_nineturn_by_trade_date_cache", return_value=cache,
        ), patch.object(svc, "_prefetch_nineturn_for_dates"):
            result = svc.get_monthly_up_to_down_daily("202606")

        hit = next((d for d in result["days"] if d["date"] == "20260605"), None)
        assert hit is not None
        stock = hit["stocks"][0]
        assert stock["ts_code"] == "300593.SZ"
        assert stock["signal_type"] == "up_to_down"
        assert stock["prev_nineturn_up_count"] == 1
