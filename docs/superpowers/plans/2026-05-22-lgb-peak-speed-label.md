# LGB Peak Speed Label Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `peak_speed` label mode to LGBTrainer that trains dual models (peak_return + days_to_peak) using a 20-day window, as an alternative to the existing fixed-holding-period mode.

**Architecture:** New `label_mode` parameter gates label computation in LGBTrainer. When `peak_speed`, two independent LGB regressors are trained on the same feature matrix with different targets. Models are saved/loaded as a pair. API and frontend expose the mode selector with mutual exclusivity.

**Tech Stack:** Python (LightGBM, pandas, numpy, scipy), FastAPI, React (Ant Design, TypeScript)

---

### Task 1: Add peak_speed label computation to LGBTrainer

**Files:**
- Modify: `src/discovery/ml/lgb_trainer.py:91-120` (constructor)
- Modify: `src/discovery/ml/lgb_trainer.py:125-223` (prepare_data)
- Add new method: `_compute_peak_speed_labels`
- Test: `tests/test_lgb_peak_speed.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_lgb_peak_speed.py
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
    # Simulate price series: day0=100, day1=102, day2=105, day3=103, day4=101, day5=100
    # Peak = day2: 105/100 - 1 = 0.05, days_to_peak = 2
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
    # Peak is 0.5% which is below 1% threshold
    prices = np.array([100.5, 100.3, 100.2, 100.1, 99.8])
    buy_price = 100.0

    peak_ret, days = trainer._calc_peak_from_prices(prices, buy_price)
    assert peak_ret == 0.0
    assert days == 5  # window_days when below threshold


def test_winsorize_labels():
    """Winsorize clips extreme values at quantile boundaries."""
    from src.discovery.ml.lgb_trainer import LGBTrainer

    trainer = LGBTrainer(
        mode="postmarket",
        label_mode="peak_speed",
        window_days=20,
        winsorize_quantile=0.95,
    )
    # 100 values, one extreme outlier
    values = pd.Series([0.02] * 95 + [0.5, 0.6, 0.7, 0.8, 1.0])
    clipped = trainer._winsorize(values, trainer.winsorize_quantile)
    assert clipped.max() <= values.quantile(0.95) + 1e-9
    assert clipped.min() >= values.quantile(0.05) - 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_lgb_peak_speed.py -v`
Expected: FAIL with TypeError or AttributeError (label_mode param doesn't exist yet)

- [ ] **Step 3: Update LGBTrainer constructor**

Modify `src/discovery/ml/lgb_trainer.py` constructor to accept new parameters:

```python
class LGBTrainer:
    def __init__(self, mode: str = "postmarket", forward_days: int = 3,
                 exec_mode: str = "close", progress_callback=None,
                 label_mode: str = "fixed", window_days: int = 20,
                 peak_min_return: float = 0.01, winsorize_quantile: float = 0.99):
        if mode not in ("intraday", "postmarket"):
            raise ValueError("mode 须为 intraday 或 postmarket")
        if exec_mode not in ("open", "close"):
            raise ValueError("exec_mode 须为 open 或 close")
        if label_mode not in ("fixed", "peak_speed"):
            raise ValueError("label_mode 须为 fixed 或 peak_speed")
        self.mode = mode
        self.forward_days = forward_days
        self.exec_mode = exec_mode
        self.label_mode = label_mode
        self.window_days = window_days
        self.peak_min_return = peak_min_return
        self.winsorize_quantile = winsorize_quantile
        self.progress_callback = progress_callback
        self.model: Optional[LGBMRegressor] = None
        self.days_model: Optional[LGBMRegressor] = None  # auxiliary model for peak_speed
        self.feature_names: List[str] = []
        self._X_train: Optional[pd.DataFrame] = None
        self._y_train: Optional[pd.Series] = None
        self._y_days_train: Optional[pd.Series] = None  # days_to_peak labels
        self._X_latest: Optional[pd.DataFrame] = None
        self._latest_date: Optional[str] = None
        self._latest_codes: List[str] = []
        self._training_metrics: Dict = {}
```

- [ ] **Step 4: Add `_calc_peak_from_prices` helper method**

Add after `_compute_forward_returns` method:

```python
def _calc_peak_from_prices(
    self, forward_prices: np.ndarray, buy_price: float,
) -> Tuple[float, int]:
    """Calculate peak return and days to peak from a price array.

    Args:
        forward_prices: Array of prices for days T+1 to T+W
        buy_price: Price at day T (buy price)

    Returns:
        (peak_return, days_to_peak): If peak_return < peak_min_return,
        returns (0.0, window_days).
    """
    if len(forward_prices) == 0 or buy_price <= 0:
        return 0.0, self.window_days

    returns = forward_prices / buy_price - 1.0
    peak_idx = int(np.argmax(returns))
    peak_return = float(returns[peak_idx])

    if peak_return < self.peak_min_return:
        return 0.0, self.window_days

    days_to_peak = peak_idx + 1
    return peak_return, days_to_peak
```

- [ ] **Step 5: Add `_winsorize` static helper**

```python
@staticmethod
def _winsorize(series: pd.Series, quantile: float) -> pd.Series:
    """Clip series at symmetric quantile boundaries."""
    lower = series.quantile(1.0 - quantile)
    upper = series.quantile(quantile)
    return series.clip(lower=lower, upper=upper)
```

- [ ] **Step 6: Add `_compute_peak_speed_labels` method**

Add after `_compute_forward_returns`:

```python
def _compute_peak_speed_labels(
    self, db: DatabaseManager, X: pd.DataFrame,
    _start: Optional[str], _end: Optional[str],
) -> Tuple[pd.Series, pd.Series]:
    """Compute peak_return and days_to_peak labels for peak_speed mode.

    Returns:
        (peak_returns, days_to_peak): Both indexed by (trade_date, ts_code)
    """
    from src.storage import StockAdjFactor, StockDaily
    from sqlalchemy import func

    trading_dates = sorted(X["trade_date"].unique())
    all_codes = X["ts_code"].unique().tolist()
    bare_codes = [c.split(".")[0] for c in all_codes]

    # Get all trading days for window lookup
    with db.get_session() as session:
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(func.count(StockDaily.code) >= 3000)
            .order_by(StockDaily.date)
            .all()
        )
    trading_days_all = sorted(
        d[0].strftime("%Y%m%d") if hasattr(d[0], "strftime") else str(d[0]).replace("-", "")[:8]
        for d in dates_raw
    )

    # For each trade_date, find the window of W future trading days
    needed_dates: set = set(trading_dates)
    window_dates_by_td: Dict[str, List[str]] = {}

    for td in trading_dates:
        try:
            td_idx = trading_days_all.index(td)
        except ValueError:
            continue
        window = trading_days_all[td_idx + 1: td_idx + 1 + self.window_days]
        if len(window) < 3:
            continue
        window_dates_by_td[td] = window
        needed_dates.update(window)

    # Fetch prices
    price_col = "open" if self.exec_mode == "open" else "close"
    with db.get_session() as session:
        rows = session.query(StockDaily).filter(
            StockDaily.date.in_([
                pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:8]}") for d in needed_dates
            ]),
            StockDaily.code.in_(bare_codes),
        ).all()

    price_map: Dict[Tuple[str, str], float] = {}
    for r in rows:
        code_str = str(r.code).split(".")[0]
        d = r.date.strftime("%Y%m%d")
        p = float(getattr(r, price_col)) if getattr(r, price_col) else 0.0
        price_map[(d, code_str)] = p

    # Load adj factors
    adj_map = _load_adj_factors(db, bare_codes, needed_dates)

    # Compute labels
    peak_results = {}
    days_results = {}

    for td, window in window_dates_by_td.items():
        td_codes = X[X["trade_date"] == td]["ts_code"].tolist()
        for code in td_codes:
            bare = str(code).split(".")[0]
            buy_price = price_map.get((td, bare))
            if not buy_price or buy_price <= 0:
                continue

            adj_buy = adj_map.get((bare, td), 1.0)
            if adj_buy <= 0:
                continue

            # Build adj-price array for window
            forward_prices = []
            for wd in window:
                raw_p = price_map.get((wd, bare))
                if raw_p and raw_p > 0:
                    adj_sell = adj_map.get((bare, wd), 1.0)
                    if adj_sell > 0:
                        forward_prices.append(raw_p * (adj_sell / adj_buy))
                    else:
                        forward_prices.append(raw_p)
                else:
                    forward_prices.append(np.nan)

            fp_array = np.array(forward_prices, dtype=np.float64)
            valid_mask = ~np.isnan(fp_array)
            if valid_mask.sum() < 3:
                continue

            peak_ret, days = self._calc_peak_from_prices(
                fp_array[valid_mask], buy_price
            )
            peak_results[(td, code)] = peak_ret
            days_results[(td, code)] = days

    peak_series = pd.Series(peak_results, name="peak_return")
    days_series = pd.Series(days_results, name="days_to_peak")

    # Winsorize peak returns
    if len(peak_series) > 0:
        peak_series = self._winsorize(peak_series, self.winsorize_quantile)

    return peak_series, days_series
```

- [ ] **Step 7: Update `prepare_data` to branch on label_mode**

Replace the section after `X = X.dropna(...)` (lines ~201-223) with:

```python
        if self.label_mode == "peak_speed":
            if cb:
                cb(f"正在计算 {self.window_days} 日窗口内峰值收益与到达天数...")
            y_peak, y_days = self._compute_peak_speed_labels(db, X, start_date, end_date)

            X = X.set_index(["trade_date", "ts_code"])
            common = X.index.intersection(y_peak.index)
            X = X.loc[common]
            y_peak = y_peak.loc[common]
            y_days = y_days.loc[common]

            mask = np.isfinite(y_peak) & np.isfinite(y_days)
            X = X.loc[mask]
            y_peak = y_peak.loc[mask]
            y_days = y_days.loc[mask]

            if cb:
                cb(f"数据准备完成: {len(X):,} 样本，{len(self.feature_names)} 特征，"
                   f"日期范围 {X.index.get_level_values(0).min()} ~ "
                   f"{X.index.get_level_values(0).max()}")
            self._train_start = start_date or X.index.get_level_values(0).min()
            self._train_end = end_date or X.index.get_level_values(0).max()
            self._X_train = X
            self._y_train = y_peak
            self._y_days_train = y_days
            return X, y_peak
        else:
            if cb:
                cb(f"正在计算未来 {self.forward_days} 日收益...")
            y = self._compute_forward_returns(db, X, start_date, end_date)

            X = X.set_index(["trade_date", "ts_code"])
            common = X.index.intersection(y.index)
            X = X.loc[common]
            y = y.loc[common]

            mask = np.isfinite(y)
            X = X.loc[mask]
            y = y.loc[mask]

            if cb:
                cb(f"数据准备完成: {len(X):,} 样本，{len(self.feature_names)} 特征，"
                   f"日期范围 {X.index.get_level_values(0).min()} ~ "
                   f"{X.index.get_level_values(0).max()}")
            self._train_start = start_date or X.index.get_level_values(0).min()
            self._train_end = end_date or X.index.get_level_values(0).max()
            self._X_train = X
            self._y_train = y
            return X, y
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `python -m pytest tests/test_lgb_peak_speed.py -v`
Expected: All 3 tests PASS

- [ ] **Step 9: Commit**

```bash
git add src/discovery/ml/lgb_trainer.py tests/test_lgb_peak_speed.py
git commit -m "feat: add peak_speed label computation to LGBTrainer"
```

---

### Task 2: Add dual-model training and prediction for peak_speed mode

**Files:**
- Modify: `src/discovery/ml/lgb_trainer.py:359-423` (train method)
- Modify: `src/discovery/ml/lgb_trainer.py:503-590` (predict method)
- Modify: `src/discovery/ml/lgb_trainer.py:636-657` (get_latest_predictions)
- Test: `tests/test_lgb_peak_speed.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgb_peak_speed.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_lgb_peak_speed.py::test_train_peak_speed_produces_two_models -v`
Expected: FAIL (days_model remains None)

- [ ] **Step 3: Update `train()` to train auxiliary model**

After `self.model = model` (line ~420), add:

```python
        # Train auxiliary days_to_peak model for peak_speed mode
        if self.label_mode == "peak_speed" and self._y_days_train is not None:
            y_days = self._y_days_train.values
            days_model = LGBMRegressor(
                n_estimators=n_estimators,
                num_leaves=num_leaves,
                learning_rate=learning_rate,
                verbose=-1,
                random_state=43,
                **kwargs,
            )
            days_model.fit(X, y_days)
            self.days_model = days_model
```

- [ ] **Step 4: Update `predict()` to include days prediction**

After `pivot["lgb_score_norm"] = ...` block, add:

```python
        # Predict days_to_peak for peak_speed mode
        if self.label_mode == "peak_speed" and self.days_model is not None:
            days_pred = self.days_model.predict(X_pred)
            pivot["predicted_days"] = np.clip(
                np.round(days_pred), 1, self.window_days
            ).astype(int)
```

- [ ] **Step 5: Update `get_latest_predictions()` to include predicted_days**

Replace the `results.append({...})` block with:

```python
            entry = {
                "rank": len(results) + 1,
                "ts_code": row["ts_code"],
                "stock_code": row["stock_code"],
                "stock_name": name,
                "lgb_score": round(float(row["lgb_score_norm"]), 2),
                "raw_score": round(float(row["lgb_score"]), 2),
            }
            if "predicted_days" in row.index:
                entry["predicted_days"] = int(row["predicted_days"])
            results.append(entry)
```

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/test_lgb_peak_speed.py -v`
Expected: All 4 tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/discovery/ml/lgb_trainer.py tests/test_lgb_peak_speed.py
git commit -m "feat: dual-model training and prediction for peak_speed mode"
```

---

### Task 3: Update model save/load for dual-model persistence

**Files:**
- Modify: `src/discovery/ml/lgb_trainer.py:905-949` (save/load)
- Test: `tests/test_lgb_peak_speed.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_lgb_peak_speed.py`:

```python
def test_save_load_peak_speed_model(tmp_path):
    """Save and load preserves both models and metadata."""
    from src.discovery.ml.lgb_trainer import LGBTrainer
    import unittest.mock as mock

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

    import os
    assert os.path.exists(path)
    days_path = path.replace(".joblib", "_days.joblib")
    assert os.path.exists(days_path)

    loaded = LGBTrainer.load(path)
    assert loaded.label_mode == "peak_speed"
    assert loaded.window_days == 20
    assert loaded.model is not None
    assert loaded.days_model is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_lgb_peak_speed.py::test_save_load_peak_speed_model -v`
Expected: FAIL

- [ ] **Step 3: Update `save()` method**

Replace the `save()` method body with:

```python
    def save(self, name: Optional[str] = None) -> str:
        """保存模型到 data/lgb_models/ 目录。"""
        if self.model is None:
            raise RuntimeError("没有已训练的模型可保存")

        os.makedirs(MODEL_DIR, exist_ok=True)
        if name is None:
            exec_suffix = "open2open" if self.exec_mode == "open" else "close2close"
            sd = str(self._train_start).replace("-", "")[:8]
            ed = str(self._train_end).replace("-", "")[:8]
            if self.label_mode == "peak_speed":
                name = f"lgb_{self.mode}_peak{self.window_days}d_{sd}_{ed}_{exec_suffix}"
            else:
                name = f"lgb_{self.mode}_fwd{self.forward_days}d_{sd}_{ed}_{exec_suffix}"

        self._cleanup_same_config_models(name)

        model_path = os.path.join(MODEL_DIR, f"{name}.joblib")
        meta = {
            "mode": self.mode,
            "forward_days": self.forward_days,
            "exec_mode": self.exec_mode,
            "label_mode": self.label_mode,
            "window_days": self.window_days,
            "peak_min_return": self.peak_min_return,
            "winsorize_quantile": self.winsorize_quantile,
            "feature_names": self.feature_names,
            "training_metrics": self._training_metrics,
            "train_start": str(self._train_start)[:10] if self._train_start else "",
            "train_end": str(self._train_end)[:10] if self._train_end else "",
            "saved_at": datetime.now().isoformat(),
        }
        joblib.dump({"model": self.model, "meta": meta}, model_path)

        # Save auxiliary days model for peak_speed mode
        if self.label_mode == "peak_speed" and self.days_model is not None:
            days_path = os.path.join(MODEL_DIR, f"{name}_days.joblib")
            joblib.dump({"model": self.days_model, "meta": meta}, days_path)

        return model_path
```

- [ ] **Step 4: Update `load()` classmethod**

```python
    @classmethod
    def load(cls, model_path: str) -> "LGBTrainer":
        """从文件加载模型。"""
        data = joblib.load(model_path)
        meta = data["meta"]
        trainer = cls(
            mode=meta["mode"],
            forward_days=meta["forward_days"],
            exec_mode=meta.get("exec_mode", "close"),
            label_mode=meta.get("label_mode", "fixed"),
            window_days=meta.get("window_days", 20),
            peak_min_return=meta.get("peak_min_return", 0.01),
            winsorize_quantile=meta.get("winsorize_quantile", 0.99),
        )
        trainer.model = data["model"]
        trainer.feature_names = meta["feature_names"]
        trainer._training_metrics = meta.get("training_metrics", {})
        trainer._train_start = meta.get("train_start", "")
        trainer._train_end = meta.get("train_end", "")

        # Load auxiliary days model if peak_speed
        if trainer.label_mode == "peak_speed":
            days_path = model_path.replace(".joblib", "_days.joblib")
            if os.path.exists(days_path):
                days_data = joblib.load(days_path)
                trainer.days_model = days_data["model"]

        return trainer
```

- [ ] **Step 5: Run tests**

Run: `python -m pytest tests/test_lgb_peak_speed.py -v`
Expected: All 5 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/discovery/ml/lgb_trainer.py tests/test_lgb_peak_speed.py
git commit -m "feat: save/load dual models for peak_speed mode"
```

---

### Task 4: Update API schemas and endpoint

**Files:**
- Modify: `api/v1/schemas/research.py:9-19` (LGBTrainRequest)
- Modify: `api/v1/schemas/research.py:34-47` (LGBPredictionItem)
- Modify: `api/v1/endpoints/research.py:63-115` (_run_train_in_process)
- Modify: `api/v1/endpoints/research.py:173-219` (lgb_train)

- [ ] **Step 1: Update LGBTrainRequest schema**

In `api/v1/schemas/research.py`, replace lines 9-19:

```python
class LGBTrainRequest(BaseModel):
    mode: str = Field("postmarket", description="扫描模式: intraday | postmarket")
    forward_days: int = Field(3, ge=1, le=60, description="预测未来 N 日收益（fixed 模式）")
    exec_mode: str = Field("close", description="标签模式: open | close")
    label_mode: str = Field("fixed", description="标签构造: fixed | peak_speed")
    window_days: int = Field(20, ge=5, le=60, description="峰值搜索窗口天数（peak_speed 模式）")
    peak_min_return: float = Field(0.01, ge=0.0, le=0.1, description="最小峰值门槛")
    start_date: Optional[str] = Field(None, description="训练起始日期 YYYYMMDD")
    end_date: Optional[str] = Field(None, description="训练结束日期 YYYYMMDD")
    n_estimators: int = Field(200, ge=10, le=2000)
    num_leaves: int = Field(31, ge=2, le=255)
    learning_rate: float = Field(0.05, ge=0.001, le=1.0)
    cv_folds: int = Field(5, ge=1, le=10)
```

- [ ] **Step 2: Update LGBPredictionItem schema**

Add `predicted_days` field after `raw_score`:

```python
class LGBPredictionItem(BaseModel):
    rank: int
    ts_code: str
    stock_code: str
    stock_name: str = ""
    lgb_score: float
    raw_score: float
    predicted_days: Optional[int] = None
    win_rate: Optional[float] = None
    avg_return: Optional[float] = None
    max_return: Optional[float] = None
    max_loss: Optional[float] = None
    profit_loss_ratio: Optional[float] = None
    hit_count: Optional[int] = None
    score_percentile: Optional[float] = None
```

- [ ] **Step 3: Update `_run_train_in_process`**

In `api/v1/endpoints/research.py`, update the LGBTrainer instantiation:

```python
        trainer = LGBTrainer(
            mode=req_dict["mode"],
            forward_days=req_dict["forward_days"],
            exec_mode=req_dict.get("exec_mode", "close"),
            label_mode=req_dict.get("label_mode", "fixed"),
            window_days=req_dict.get("window_days", 20),
            peak_min_return=req_dict.get("peak_min_return", 0.01),
            progress_callback=_progress,
        )
```

Add `"label_mode": trainer.label_mode` to the completed result dict.

- [ ] **Step 4: Update `lgb_train` endpoint `req_dict`**

```python
    req_dict = {
        "mode": req.mode,
        "forward_days": req.forward_days,
        "exec_mode": req.exec_mode,
        "label_mode": req.label_mode,
        "window_days": req.window_days,
        "peak_min_return": req.peak_min_return,
        "start_date": req.start_date,
        "end_date": req.end_date,
        "n_estimators": req.n_estimators,
        "num_leaves": req.num_leaves,
        "learning_rate": req.learning_rate,
        "cv_folds": req.cv_folds,
    }
```

Update `_lgb_tasks[task_id]` to include `"label_mode": req.label_mode`.

- [ ] **Step 5: Verify compilation**

Run: `python -m py_compile api/v1/schemas/research.py && python -m py_compile api/v1/endpoints/research.py`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add api/v1/schemas/research.py api/v1/endpoints/research.py
git commit -m "feat: add peak_speed params to LGB API schema and endpoint"
```

---

### Task 5: Update frontend types and training form

**Files:**
- Modify: `apps/dsa-web/src/api/research.ts:5-15` (types)
- Modify: `apps/dsa-web/src/pages/LightGBMPage.tsx` (state, form, display)

- [ ] **Step 1: Update TypeScript types**

In `apps/dsa-web/src/api/research.ts`, update `LGBTrainRequest`:

```typescript
export type LGBTrainRequest = {
  mode: 'intraday' | 'postmarket';
  forward_days: number;
  exec_mode?: string;
  label_mode?: 'fixed' | 'peak_speed';
  window_days?: number;
  peak_min_return?: number;
  start_date?: string | null;
  end_date?: string | null;
  n_estimators: number;
  num_leaves: number;
  learning_rate: number;
  cv_folds: number;
};
```

Add `predicted_days` to `LGBPredictionItem`:

```typescript
export type LGBPredictionItem = {
  rank: number;
  ts_code: string;
  stock_code: string;
  stock_name: string;
  lgb_score: number;
  raw_score: number;
  predicted_days?: number | null;
  win_rate: number | null;
  avg_return: number | null;
  max_return: number | null;
  max_loss: number | null;
  profit_loss_ratio: number | null;
  hit_count: number | null;
  score_percentile?: number | null;
};
```

- [ ] **Step 2: Add state variables in LightGBMPage.tsx**

After `const [forwardDays, setForwardDays] = useState(3);` (line 22):

```typescript
  const [labelMode, setLabelMode] = useState<'fixed' | 'peak_speed'>('fixed');
  const [windowDays, setWindowDays] = useState(20);
  const [peakMinReturn, setPeakMinReturn] = useState(0.01);
```

- [ ] **Step 3: Update handleTrain request params**

In the `researchApi.train({...})` call, add new fields:

```typescript
      const { task_id } = await researchApi.train({
        mode: 'postmarket',
        forward_days: forwardDays,
        exec_mode: trainExecMode,
        label_mode: labelMode,
        window_days: windowDays,
        peak_min_return: peakMinReturn,
        start_date: startDate,
        end_date: endDate,
        n_estimators: nEstimators,
        num_leaves: numLeaves,
        learning_rate: learningRate,
        cv_folds: cvFolds,
      });
```

Add `labelMode, windowDays, peakMinReturn` to the `useCallback` dependency array.

- [ ] **Step 4: Add label mode selector card**

Insert after the exec_mode `<Card>` (after line ~488), before the training params card:

```tsx
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">标签模式</div>
              <Segmented
                block
                value={labelMode}
                onChange={(v) => {
                  setLabelMode(v as 'fixed' | 'peak_speed');
                  setSelectedModel(undefined);
                  setPredictions([]);
                  setFeatureImportance([]);
                  setDiagnostics(null);
                }}
                options={[
                  { label: '固定持有期', value: 'fixed' },
                  { label: '峰值速度', value: 'peak_speed' },
                ]}
              />
              <div className="text-[10px] text-tertiary-text">
                {labelMode === 'fixed'
                  ? '预测固定第N天的涨跌幅'
                  : '预测窗口内最大涨幅与到达天数'}
              </div>
            </div>
          </Card>
```

- [ ] **Step 5: Conditionally render forward_days vs window params**

Replace the `预测天数` input section (lines ~494-498) with:

```tsx
              {labelMode === 'fixed' ? (
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">预测天数 <span className="text-tertiary-text">（未来N日后涨跌幅，推荐3）</span></div>
                  <InputNumber size="small" min={1} max={60} value={forwardDays} onChange={(v) => setForwardDays(v ?? 3)} className="w-full" />
                </div>
              ) : (
                <>
                  <div className="space-y-1.5">
                    <div className="text-xs text-secondary-text">观察窗口 <span className="text-tertiary-text">（未来N日内搜索峰值，推荐20）</span></div>
                    <InputNumber size="small" min={5} max={60} value={windowDays} onChange={(v) => setWindowDays(v ?? 20)} className="w-full" />
                  </div>
                  <div className="space-y-1.5">
                    <div className="text-xs text-secondary-text">最小涨幅门槛 <span className="text-tertiary-text">（低于此值视为无效）</span></div>
                    <InputNumber size="small" min={0} max={0.1} step={0.005} value={peakMinReturn} onChange={(v) => setPeakMinReturn(v ?? 0.01)} className="w-full" />
                  </div>
                </>
              )}
```

- [ ] **Step 6: Add predicted_days column to predictions display**

Find the predictions table/list rendering and add a conditional column for `predicted_days`:

```tsx
...(predictions.some(p => p.predicted_days != null) ? [{
  title: '预计见顶',
  dataIndex: 'predicted_days',
  key: 'predicted_days',
  width: 80,
  render: (v: number | null | undefined) => v != null ? `${v}天` : '-',
}] : []),
```

- [ ] **Step 7: Build frontend**

Run: `cd apps/dsa-web && npm run build`
Expected: Build succeeds

- [ ] **Step 8: Commit**

```bash
git add apps/dsa-web/src/api/research.ts apps/dsa-web/src/pages/LightGBMPage.tsx
git commit -m "feat: add peak_speed mode selector to LightGBM frontend"
```

---

### Task 6: Integration test and regression check

**Files:**
- Test: `tests/test_lgb_peak_speed.py` (append)

- [ ] **Step 1: Add full-flow integration test**

```python
def test_peak_speed_full_flow_synthetic():
    """End-to-end: construct with peak_speed → train → verify dual models."""
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
```

- [ ] **Step 2: Run all peak_speed tests**

Run: `python -m pytest tests/test_lgb_peak_speed.py -v`
Expected: All 6 tests PASS

- [ ] **Step 3: Run backend compilation**

Run: `python -m py_compile src/discovery/ml/lgb_trainer.py && python -m py_compile api/v1/schemas/research.py && python -m py_compile api/v1/endpoints/research.py`
Expected: No errors

- [ ] **Step 4: Run existing tests for regressions**

Run: `python -m pytest tests/ -m "not network" --timeout=120 -q`
Expected: No new failures beyond pre-existing ones

- [ ] **Step 5: Commit**

```bash
git add tests/test_lgb_peak_speed.py
git commit -m "test: add integration tests for peak_speed label mode"
```
