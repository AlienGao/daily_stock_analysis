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
