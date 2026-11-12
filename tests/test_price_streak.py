from src.services.price_streak import latest_consecutive_drawdown, latest_consecutive_gain


def test_latest_consecutive_drawdown_returns_most_recent_two_day_decline():
    rows = [
        ("20260105", 10.0),
        ("20260106", 11.0),
        ("20260107", 11.5),
        ("20260108", 11.0),
        ("20260109", 10.0),
    ]
    assert latest_consecutive_drawdown(rows) == (-13.04, 2, "20260107", "20260109")


def test_latest_consecutive_drawdown_skips_single_day_dip():
    rows = [
        ("20260105", 10.0),
        ("20260106", 9.0),
        ("20260107", 10.0),
        ("20260108", 9.5),
    ]
    assert latest_consecutive_drawdown(rows) is None


def test_latest_consecutive_gain_returns_most_recent_three_day_rise():
    rows = [
        ("20260105", 10.0),
        ("20260106", 9.0),
        ("20260107", 9.5),
        ("20260108", 10.5),
        ("20260109", 11.5),
    ]
    assert latest_consecutive_gain(rows) == (27.78, 3, "20260106", "20260109")


def test_streak_helpers_ignore_invalid_prices():
    rows = [("20260105", 10.0), ("20260106", None), ("20260107", float("nan")), ("20260108", 9.0)]
    assert latest_consecutive_drawdown(rows) is None
    assert latest_consecutive_gain(rows) is None
