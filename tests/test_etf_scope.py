from src.services.etf_scope import (
    get_etf_theme,
    is_pure_etf_name,
    select_representative_etfs,
)


def test_etf_theme_normalizes_related_names() -> None:
    assert get_etf_theme("华夏中证新能源汽车ETF") == "新能源车"
    assert get_etf_theme("万家国证新能源车电池ETF") == "新能源车"
    assert get_etf_theme("永赢中证红利低波动ETF") == "红利低波"
    assert get_etf_theme("泰康中证红利低波动ETF") == "红利低波"
    assert get_etf_theme("鹏华中证软件服务ETF") == "软件"
    assert get_etf_theme("国泰中证全指软件ETF") == "软件"
    assert get_etf_theme("国泰中证军工ETF") == "军工"
    assert get_etf_theme("广发中证军工ETF") == "军工"


def test_non_etf_index_funds_are_excluded() -> None:
    assert is_pure_etf_name("招商中证白酒指数-A") is False
    assert is_pure_etf_name("某某ETF联接A") is False
    assert get_etf_theme("方正富邦中证保险主题指数-A") is None


def test_representative_prefers_liquidity_with_sufficient_history() -> None:
    selected, excluded = select_representative_etfs([
        {"code": "512420", "name": "华安中证工程机械主题ETF", "history_days": 1, "avg_amount": 900000},
        {"code": "515970", "name": "华夏中证工程机械主题ETF", "history_days": 91, "avg_amount": 100000},
        {"code": "516440", "name": "南方中证工程机械主题ETF", "history_days": 90, "avg_amount": 10000},
    ])

    assert selected["工程机械"]["code"] == "515970"
    assert {item["code"] for item in excluded} == {"512420", "516440"}


def test_representative_selection_is_deterministic_without_history() -> None:
    selected, _ = select_representative_etfs([
        {"code": "530050.SH", "name": "东财上证50ETF"},
        {"code": "510050.SH", "name": "华夏上证50ETF"},
    ])

    assert selected["上证50"]["code"] == "510050"
