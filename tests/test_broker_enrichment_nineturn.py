# -*- coding: utf-8 -*-
"""券商金股 enrichment 九转缓存逻辑测试。"""

from unittest.mock import MagicMock, patch

import pandas as pd

from src.services.broker_recommend_service import BrokerRecommendService as S


class TestNineturnCacheHelpers:
    def test_empty_nineturn_all_zero(self):
        assert S._is_empty_nineturn({
            "up_count": 0, "down_count": 0,
            "nine_up_turn": 0, "nine_down_turn": 0,
        })

    def test_nonempty_nineturn(self):
        assert not S._is_empty_nineturn({"up_count": 2, "down_count": 0})

    def test_current_month_empty_is_cache_miss(self):
        from datetime import datetime
        month = datetime.now().strftime("%Y%m")
        assert S._nineturn_cache_miss({"up_count": 0, "down_count": 0}, month)

    def test_current_month_with_signal_not_miss(self):
        from datetime import datetime
        month = datetime.now().strftime("%Y%m")
        assert not S._nineturn_cache_miss({"up_count": 1, "down_count": 0}, month)

    def test_history_month_empty_not_miss(self):
        assert not S._nineturn_cache_miss({"up_count": 0, "down_count": 0}, "202401")


class TestEnrichmentEmptyCheck:
    def test_empty_when_nineturn_zero_and_cyq_missing(self):
        enrichment = {
            "600519.SH": {
                "nineturn": {"up_count": 0, "down_count": 0,
                             "nine_up_turn": 0, "nine_down_turn": 0},
                "cyq_perf": {"winner_rate": None},
            },
        }
        assert S._is_enrichment_empty(enrichment)

    def test_not_empty_with_nineturn_signal(self):
        enrichment = {
            "600519.SH": {"nineturn": {"up_count": 3, "down_count": 0}},
        }
        assert not S._is_enrichment_empty(enrichment)

    def test_not_empty_with_winner_rate(self):
        enrichment = {
            "600519.SH": {"cyq_perf": {"winner_rate": 0.62}},
        }
        assert not S._is_enrichment_empty(enrichment)


class TestEnrichmentFallback:
    def _service(self):
        svc = S.__new__(S)
        svc.db = MagicMock()
        return svc

    def test_current_month_falls_back_when_nineturn_not_published(self):
        svc = self._service()
        df = pd.DataFrame([{
            "ts_code": "600519.SH", "name": "茅台", "broker": "中信", "broker_count": 1,
        }])
        fallback_data = {
            "600519.SH": {
                "nineturn": {"up_count": 2, "down_count": 0,
                             "nine_up_turn": 0, "nine_down_turn": 0},
                "cyq_perf": {"winner_rate": 0.5},
                "sector": "白酒",
            },
        }
        with patch.object(svc, "get_monthly_recommendations", return_value=df), \
             patch.object(svc, "_resolve_enrichment_date", return_value="20261118"), \
             patch.object(svc, "_is_current_month", return_value=True), \
             patch.object(svc, "_is_intraday_live_window", return_value=False), \
             patch.object(svc, "_latest_cached_enrichment_date", return_value="20261117"), \
             patch.object(svc, "_has_published_nineturn", return_value=False), \
             patch.object(svc, "_collect_enrichment", return_value=fallback_data) as collect:
            enrichment, used_date = svc.get_monthly_enrichment_with_date("202611")

        assert used_date == "20261117"
        assert enrichment == fallback_data
        collect.assert_called_once_with("202611", ["600519.SH"], "20261117")

    def test_current_month_keeps_query_date_when_nineturn_published(self):
        svc = self._service()
        df = pd.DataFrame([{
            "ts_code": "600519.SH", "name": "茅台", "broker": "中信", "broker_count": 1,
        }])
        today_data = {
            "600519.SH": {
                "nineturn": {"up_count": 0, "down_count": 2},
                "cyq_perf": {"winner_rate": None},
            },
        }
        with patch.object(svc, "get_monthly_recommendations", return_value=df), \
             patch.object(svc, "_resolve_enrichment_date", return_value="20261118"), \
             patch.object(svc, "_is_current_month", return_value=True), \
             patch.object(svc, "_is_intraday_live_window", return_value=False), \
             patch.object(svc, "_latest_cached_enrichment_date", return_value="20261117"), \
             patch.object(svc, "_has_published_nineturn", return_value=True), \
             patch.object(svc, "_collect_enrichment", return_value=today_data):
            enrichment, used_date = svc.get_monthly_enrichment_with_date("202611")

        assert used_date == "20261118"
        assert enrichment == today_data

    def test_latest_cached_date_skips_empty_cache(self):
        svc = self._service()
        empty_cache = {
            "600519.SH": {
                "nineturn": {"up_count": 0, "down_count": 0},
                "cyq_perf": {"winner_rate": None},
            },
        }
        good_cache = {
            "600519.SH": {"cyq_perf": {"winner_rate": 0.4}},
        }
        svc.db.get_enrichment_cache.side_effect = [empty_cache, good_cache]
        with patch.object(
            svc, "_get_trading_days", return_value=["20261117", "20261118", "20261119"],
        ):
            assert svc._latest_cached_enrichment_date(
                "202611", ["600519.SH"], "20261119",
            ) == "20261117"

    def test_latest_cached_date_none_when_all_empty(self):
        svc = self._service()
        svc.db.get_enrichment_cache.return_value = {}
        with patch.object(
            svc, "_get_trading_days", return_value=["20261117", "20261118"],
        ):
            assert svc._latest_cached_enrichment_date(
                "202611", ["600519.SH"], "20261118",
            ) is None
