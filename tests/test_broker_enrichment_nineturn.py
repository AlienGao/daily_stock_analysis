# -*- coding: utf-8 -*-
"""券商金股 enrichment 九转缓存逻辑测试。"""

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
