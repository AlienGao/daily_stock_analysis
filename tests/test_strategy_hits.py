# -*- coding: utf-8 -*-
from src.analyzer import AnalysisResult
from src.utils.strategy_hits import count_matched_skills, matched_skill_ids_preview


def test_count_matched_skills():
    r = AnalysisResult(
        code="1",
        name="a",
        sentiment_score=50,
        trend_prediction="x",
        operation_advice="买入",
        matched_skills=[{"id": "a"}, {"id": "b"}],
    )
    assert count_matched_skills(r) == 2
    assert count_matched_skills(AnalysisResult(
        code="2", name="b", sentiment_score=1, trend_prediction="x", operation_advice="持有"
    )) == 0


def test_matched_skill_ids_preview():
    r = AnalysisResult(
        code="1",
        name="a",
        sentiment_score=50,
        trend_prediction="x",
        operation_advice="买入",
        matched_skills=[{"id": "bull_trend"}, {"name": "x"}],
    )
    assert matched_skill_ids_preview(r, 5) == ["bull_trend", "x"]
