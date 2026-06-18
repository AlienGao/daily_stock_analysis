import json
import math

from src.services.hfq_new_high_service import (
    _band_distance_pct,
    _is_near_band,
    _safe_optional_float,
    compute_latest_boll,
    lookup_adj_factor,
    scan_single_code_new_highs,
)


def test_lookup_adj_factor_forward_fill():
    adj = {"20251230": 2.0, "20260105": 2.1}
    assert lookup_adj_factor(adj, "20260105") == 2.1
    assert lookup_adj_factor(adj, "20260103") == 2.0
    assert lookup_adj_factor(adj, "20251201") is None


def test_scan_single_code_new_highs_multiple_dates_desc():
    rows = [
        ("20251230", 10.0),
        ("20260105", 11.0),
        ("20260106", 10.5),
        ("20260110", 12.0),
        ("20260115", 11.8),
    ]
    result = scan_single_code_new_highs(rows, "20260101")
    assert result is not None
    assert result["new_high_count"] == 2
    assert result["latest_new_high_date"] == "20260110"
    assert result["latest_new_high_close"] == 12.0
    assert result["current_hfq_close"] == 11.8
    assert result["ytd_hfq_return_pct"] == round((11.8 / 11.0 - 1) * 100, 2)
    dates = [x["date"] for x in result["new_high_dates"]]
    assert dates == ["20260110", "20260105"]
    assert all(math.isfinite(x["hfq_close"]) for x in result["new_high_dates"])


def test_scan_single_code_no_new_high_in_period():
    rows = [("20251230", 20.0), ("20260105", 18.0), ("20260106", 17.0)]
    assert scan_single_code_new_highs(rows, "20260101") is None


def test_to_date_str_from_iso_string():
    from src.services.hfq_new_high_service import _to_date_str
    assert _to_date_str("2026-06-19") == "20260619"
    assert _to_date_str("20260619") == "20260619"


def test_safe_optional_float_rejects_nan_for_json():
    assert _safe_optional_float(float("nan")) is None
    assert _safe_optional_float(float("inf")) is None
    assert _safe_optional_float(189.9994) == 189.9994
    row = {
        "date": "20260618",
        "open": 178.2485,
        "high": 198.3777,
        "low": 176.2798,
        "close": 189.9994,
        "volume": _safe_optional_float(float("nan")),
    }
    json.dumps([row])


def test_new_high_list_sort_same_date_by_count_desc():
    items = [
        {"latest_new_high_date": "20260110", "new_high_count": 2},
        {"latest_new_high_date": "20260110", "new_high_count": 5},
        {"latest_new_high_date": "20260115", "new_high_count": 1},
    ]
    items.sort(
        key=lambda x: (x["latest_new_high_date"], x["new_high_count"]),
        reverse=True,
    )
    assert [x["new_high_count"] for x in items] == [1, 5, 2]
    assert items[0]["latest_new_high_date"] == "20260115"


def test_overlay_spot_names_fills_missing_bse_code(monkeypatch):
    from src.services.hfq_new_high_service import HfqNewHighService

    class _FakeSpot:
        columns = ["name"]
        empty = False

        def iterrows(self):
            yield "920083", {"name": "金戈新材"}

    class _FakeDb:
        @staticmethod
        def get_instance():
            return _FakeDb()

        def get_realtime_spot(self):
            return _FakeSpot()

    import src.storage as storage_mod

    monkeypatch.setattr(storage_mod.DatabaseManager, "get_instance", _FakeDb.get_instance)
    merged = HfqNewHighService._overlay_spot_names({})
    assert merged.get("920083") == "金戈新材"
    assert merged.get("920083.BJ") == "金戈新材"


def test_compute_latest_boll_basic():
    closes = [float(i) for i in range(1, 25)]
    out = compute_latest_boll(closes)
    assert out is not None
    close, mid, lower, upper = out
    assert close == 24.0
    assert lower < mid < upper


def test_band_distance_and_near():
    assert _band_distance_pct(10.5, 10.0) == 5.0
    assert _is_near_band(10.4, 10.0, 5.0) is True
    assert _is_near_band(10.6, 10.0, 5.0) is False


def test_within_drawdown_from_high_limit():
    from src.services.hfq_new_high_service import (
        _drawdown_from_high,
        _within_drawdown_from_high_limit,
    )

    assert _within_drawdown_from_high_limit(-15.0, 20.0) is True
    assert _within_drawdown_from_high_limit(-20.0, 20.0) is True
    assert _within_drawdown_from_high_limit(-20.1, 20.0) is False
    assert _within_drawdown_from_high_limit(3.0, 20.0) is True
    assert _within_drawdown_from_high_limit(None, 20.0) is True
    assert _drawdown_from_high(80.0, 100.0) == -20.0
