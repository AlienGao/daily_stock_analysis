# -*- coding: utf-8 -*-
import pytest

from src.analyzer import AnalysisResult
from src.utils.rating_category import operation_advice_to_category
from src.services.top_n_reviewer import (
    select_top_n_by_sentiment,
    merge_results_list,
    should_run_top_n_review,
)
from src.config import Config


def test_operation_advice_to_category():
    assert operation_advice_to_category("买入", None) == "BUY"
    assert operation_advice_to_category("持有", None) == "HOLD"
    assert operation_advice_to_category("卖出", None) == "SELL"


def test_select_top_n_by_sentiment():
    r1 = AnalysisResult(
        code="1",
        name="a",
        sentiment_score=10,
        trend_prediction="x",
        operation_advice="买入",
    )
    r2 = AnalysisResult(
        code="2",
        name="b",
        sentiment_score=90,
        trend_prediction="x",
        operation_advice="持有",
    )
    r3 = AnalysisResult(
        code="3",
        name="c",
        sentiment_score=50,
        trend_prediction="x",
        operation_advice="观望",
    )
    out = select_top_n_by_sentiment([r1, r2, r3], 2)
    assert [x.code for x in out] == ["2", "3"]


def test_merge_results_list():
    a = AnalysisResult(
        code="1",
        name="a",
        sentiment_score=1,
        trend_prediction="x",
        operation_advice="买入",
    )
    b = AnalysisResult(
        code="2",
        name="b",
        sentiment_score=2,
        trend_prediction="x",
        operation_advice="持有",
    )
    b2 = AnalysisResult(
        code="2",
        name="b",
        sentiment_score=9,
        trend_prediction="y",
        operation_advice="卖出",
    )
    m = merge_results_list([a, b], {"2": b2})
    assert m[0].code == "1" and m[0].sentiment_score == 1
    assert m[1].code == "2" and m[1].operation_advice == "卖出"


def test_should_run_top_n_off():
    c = object.__new__(Config)
    c.top_n_multi_agent_review_enabled = False
    c.top_n_multi_agent_review_schedule = "both"
    assert should_run_top_n_review(c, None) is False
