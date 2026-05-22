# -*- coding: utf-8 -*-
"""Tests for LGBTrainer peak_speed label mode."""
import numpy as np
import pandas as pd
import pytest


def test_compute_peak_labels_basic():
    """Peak return and days_to_peak computed correctly from price series."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=5,
    )
    prices = np.array([102.0, 105.0, 103.0, 101.0, 100.0])
    buy_price = 100.0

    peak_ret, days = trainer._calc_peak_from_prices(prices, buy_price)
    assert abs(peak_ret - 0.05) < 1e-9
    assert days == 2


def test_compute_peak_labels_below_threshold():
    """When peak_return < peak_min_return, label is zeroed."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=5,
        peak_min_return=0.01,
    )
    prices = np.array([100.5, 100.3, 100.2, 100.1, 99.8])
    buy_price = 100.0

    peak_ret, days = trainer._calc_peak_from_prices(prices, buy_price)
    assert peak_ret == 0.0
    assert days == 5


def test_winsorize_labels():
    """Winsorize clips extreme values at quantile boundaries."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=20,
        winsorize_quantile=0.95,
    )
    values = pd.Series([0.02] * 95 + [0.5, 0.6, 0.7, 0.8, 1.0])
    clipped = trainer._winsorize(values, trainer.winsorize_quantile)
    assert clipped.max() <= values.quantile(0.95) + 1e-9
    assert clipped.min() >= values.quantile(0.05) - 1e-9


def test_train_peak_speed_produces_two_models():
    """In peak_speed mode, train() produces both model and days_model."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=5,
    )
    n = 200
    np.random.seed(42)
    feature_names = ["factor_a", "factor_b", "factor_c"]
    trainer.feature_names = feature_names
    idx = pd.MultiIndex.from_arrays([
        [f"2024010{i % 5 + 1}" for i in range(n)],
        [f"00000{i % 10}.SZ" for i in range(n)],
    ], names=["trade_date", "ts_code"])
    trainer._X_train = pd.DataFrame(
        np.random.randn(n, 3), index=idx, columns=feature_names
    )
    trainer._y_train = pd.Series(np.random.rand(n) * 0.1, index=idx)
    trainer._y_days_train = pd.Series(
        np.random.randint(1, 6, n).astype(float), index=idx
    )

    trainer.train(n_estimators=10, cv_folds=2)

    assert trainer.model is not None
    assert trainer.days_model is not None


def test_save_load_peak_speed_model(tmp_path):
    """Save and load preserves both models and metadata."""
    from src.discovery.ml.lgb_trainer import LGBTrainer
    import unittest.mock as mock
    import os

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=20,
        peak_min_return=0.01,
        winsorize_quantile=0.99,
    )
    n = 100
    np.random.seed(42)
    feature_names = ["f1", "f2"]
    trainer.feature_names = feature_names
    idx = pd.MultiIndex.from_arrays([
        [f"2024010{i % 5 + 1}" for i in range(n)],
        [f"00000{i % 10}.SZ" for i in range(n)],
    ])
    trainer._X_train = pd.DataFrame(
        np.random.randn(n, 2), index=idx, columns=feature_names
    )
    trainer._y_train = pd.Series(np.random.rand(n) * 0.1, index=idx)
    trainer._y_days_train = pd.Series(
        np.random.randint(1, 21, n).astype(float), index=idx
    )
    trainer._train_start = "20240101"
    trainer._train_end = "20240105"
    trainer.train(n_estimators=10, cv_folds=1)

    with mock.patch("src.discovery.ml.lgb_trainer.MODEL_DIR", str(tmp_path)):
        path = trainer.save()

    assert os.path.exists(path)
    days_path = path.replace(".joblib", "_days.joblib")
    assert os.path.exists(days_path)

    loaded = LGBTrainer.load(path)
    assert loaded.label_mode == "peak_speed"
    assert loaded.window_days == 20
    assert loaded.model is not None
    assert loaded.days_model is not None


def test_peak_speed_full_flow_synthetic():
    """End-to-end: construct with peak_speed, train, verify dual models."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=10,
        peak_min_return=0.005,
        winsorize_quantile=0.99,
    )
    assert trainer.label_mode == "peak_speed"
    assert trainer.window_days == 10
    assert trainer.days_model is None

    n = 300
    np.random.seed(123)
    features = ["momentum", "volume", "volatility"]
    trainer.feature_names = features
    dates = [f"2024{m:02d}01" for m in range(1, 13) for _ in range(25)][:n]
    codes = [f"{i % 50:06d}.SZ" for i in range(n)]
    idx = pd.MultiIndex.from_arrays([dates, codes])
    trainer._X_train = pd.DataFrame(
        np.random.randn(n, 3), index=idx, columns=features
    )
    trainer._y_train = pd.Series(
        np.abs(np.random.randn(n)) * 0.05, index=idx
    )
    trainer._y_days_train = pd.Series(
        np.random.randint(1, 11, n).astype(float), index=idx
    )
    trainer._train_start = "20240101"
    trainer._train_end = "20241201"

    trainer.train(n_estimators=20, cv_folds=3)
    assert trainer.model is not None
    assert trainer.days_model is not None
    assert trainer._training_metrics["n_samples"] == n


def test_backtest_peak_speed_requires_peak_speed_mode():
    """backtest_peak_speed raises if label_mode is not peak_speed."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(mode="postmarket", label_mode="fixed", forward_days=3)
    with pytest.raises(RuntimeError, match="peak_speed"):
        trainer.backtest_peak_speed()


def test_backtest_peak_speed_requires_days_model():
    """backtest_peak_speed raises if days_model is missing."""
    from src.discovery.ml.lgb_trainer import LGBTrainer
    import lightgbm as lgb

    trainer = LGBTrainer(mode="postmarket", label_mode="peak_speed", window_days=20)
    trainer.model = lgb.LGBMRegressor()  # fake model
    trainer.days_model = None
    with pytest.raises(RuntimeError, match="days_model"):
        trainer.backtest_peak_speed()