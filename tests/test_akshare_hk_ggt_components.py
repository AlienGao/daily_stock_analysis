# -*- coding: utf-8 -*-
from unittest.mock import MagicMock, patch

from data_provider.akshare_fetcher import AkshareFetcher


def test_fetch_hk_ggt_components_falls_back_to_eastmoney_http():
    fetcher = AkshareFetcher(sleep_min=0, sleep_max=0)

    def fake_get(_url, params=None, **_kwargs):
        page = int((params or {}).get("pn") or 1)
        response = MagicMock()
        response.raise_for_status.return_value = None
        if page == 1:
            response.json.return_value = {
                "data": {
                    "total": 2,
                    "diff": [{
                        "f1": 1,
                        "f2": 68.85,
                        "f3": 18.81,
                        "f4": 10.9,
                        "f5": 5933460,
                        "f6": 388000000,
                        "f12": "03296",
                        "f14": "华勤技术",
                        "f15": 69.5,
                        "f16": 60.65,
                        "f17": 61.35,
                        "f18": 57.95,
                    }],
                },
            }
        else:
            response.json.return_value = {
                "data": {
                    "total": 2,
                    "diff": [{
                        "f1": 2,
                        "f2": 100.0,
                        "f3": 1.0,
                        "f4": 1.0,
                        "f5": 1000,
                        "f6": 100000,
                        "f12": "00700",
                        "f14": "腾讯控股",
                        "f15": 101.0,
                        "f16": 99.0,
                        "f17": 99.5,
                        "f18": 99.0,
                    }],
                },
            }
        return response

    with patch("akshare.stock_hk_ggt_components_em", side_effect=ConnectionError("https failed")), \
         patch("data_provider.akshare_fetcher.requests.get", side_effect=fake_get):
        rows = fetcher.fetch_hk_ggt_components()

    assert len(rows) == 2
    assert rows[0]["hk_code"] == "03296"
    assert rows[0]["name"] == "华勤技术"
    assert rows[0]["latest_price"] == 68.85


def test_fetch_hk_ggt_components_filters_removed_stocks_by_exchange_list():
    fetcher = AkshareFetcher(sleep_min=0, sleep_max=0)

    ak_df = MagicMock()
    ak_df.empty = False
    ak_df.columns = ["代码", "名称", "最新价"]
    ak_df.iterrows.return_value = iter([
        (0, {"代码": "00700", "名称": "腾讯控股", "最新价": 500.0}),
        (1, {"代码": "02525", "名称": "禾赛-W", "最新价": 200.0}),
    ])

    with patch.object(fetcher, "_fetch_hk_ggt_official_components", return_value=[
        {"hk_code": "00700", "name": "腾讯控股"},
    ]), patch("akshare.stock_hk_ggt_components_em", return_value=ak_df):
        rows = fetcher.fetch_hk_ggt_components()

    assert [row["hk_code"] for row in rows] == ["00700"]


def test_fetch_hk_ggt_components_uses_exchange_list_when_quote_source_empty():
    fetcher = AkshareFetcher(sleep_min=0, sleep_max=0)

    with patch.object(fetcher, "_fetch_hk_ggt_official_components", return_value=[
        {"hk_code": "00700", "name": "腾讯控股"},
    ]), patch("akshare.stock_hk_ggt_components_em", side_effect=ConnectionError("failed")), \
            patch.object(fetcher, "_fetch_hk_ggt_components_em_http_fallback", return_value=None):
        rows = fetcher.fetch_hk_ggt_components()

    assert rows == [{"hk_code": "00700", "name": "腾讯控股"}]


def test_fetch_hk_ggt_exchange_components_falls_back_to_english_name():
    fetcher = AkshareFetcher(sleep_min=0, sleep_max=0)

    payload = (
        'jsonpCallback({"pageHelp":{"data":['
        '{"SECURITY_TYPE":"股票","SECURITY_CODE":"00522","ABBR_CN":"　　　　","ABBR_EN":"ASMPT"}'
        ']}})'
    )
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.text = payload

    with patch("data_provider.akshare_fetcher.requests.get", return_value=response):
        rows = fetcher._fetch_hk_ggt_components_from_sse()

    assert rows == [{"hk_code": "00522", "name": "ASMPT"}]
