import json
import math
from unittest.mock import MagicMock, patch

import pandas as pd

from src.services.stock_service import (
    StockService,
    _append_today_kl,
    _dataframe_to_history_rows,
    _safe_optional_float,
)


def test_safe_optional_float_rejects_nan():
    assert _safe_optional_float(float("nan")) is None
    assert _safe_optional_float(float("inf")) is None


def test_dataframe_to_history_rows_sanitizes_nan_volume():
    df = pd.DataFrame(
        [
            {
                "date": "2026-06-17",
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 105.0,
                "volume": float("nan"),
                "amount": 1_000_000.0,
                "pct_chg": 1.2,
            }
        ]
    )
    rows = _dataframe_to_history_rows(df)
    assert rows[0]["volume"] is None
    json.dumps(rows)


def test_append_today_kl_sanitizes_nan_volume():
    data = [
        {
            "date": "2026-06-16",
            "open": 100.0,
            "high": 110.0,
            "low": 95.0,
            "close": 100.0,
            "volume": 1000.0,
        }
    ]
    spot = MagicMock()
    spot.trade_date = "2026-06-17"
    spot.open_price = 101.0
    spot.high = 111.0
    spot.low = 96.0
    spot.price = 106.0
    spot.volume = float("nan")
    spot.amount = None

    session = MagicMock()
    session.execute.return_value.scalars.return_value.first.return_value = spot
    db = MagicMock()
    db.get_session.return_value.__enter__.return_value = session

    with patch("src.storage.DatabaseManager", return_value=db), patch(
        "src.storage.RealtimeSpot", MagicMock()
    ):
        _append_today_kl(data, "688256")

    assert data[-1]["date"] == "2026-06-17"
    assert data[-1]["volume"] is None
    assert not math.isnan(data[-1]["close"])
    json.dumps(data)
