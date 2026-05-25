# -*- coding: utf-8 -*-
"""LightGBM 因子收益预测训练器。

从 factor_score_snapshots 表提取特征矩阵，训练 LightGBM 回归模型预测
未来 N 日收益，输出特征重要性、预测结果、回测对比等研究数据。
"""

from __future__ import annotations

import json
import os
import warnings
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from scipy.stats import spearmanr
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error

from data_provider.base import is_st_stock
from src.storage import DatabaseManager

warnings.filterwarnings("ignore", category=UserWarning, module="lightgbm")

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
MODEL_DIR = os.path.join(_PROJECT_ROOT, "data", "lgb_models")
_REPORTS_DIR = os.path.join(_PROJECT_ROOT, "lgb_reports")


def _get_db() -> DatabaseManager:
    return DatabaseManager.get_instance()


def mode_label(mode: str) -> str:
    return "盘后" if mode == "postmarket" else "盘中"


def _load_adj_factors(db, bare_codes, needed_dates):
    """Load adj_factor map for given stocks and dates.

    Returns Dict[(code, date_str), adj_factor] — code is bare (no exchange suffix).
    Missing entries default to 1.0 at call sites.
    """
    from src.storage import StockAdjFactor

    if not bare_codes or not needed_dates:
        return {}

    date_objs = []
    for d in needed_dates:
        try:
            date_objs.append(
                date(int(d[:4]), int(d[4:6]), int(d[6:8]))
                if isinstance(d, str) and len(d) >= 8
                else d
            )
        except (ValueError, IndexError):
            pass

    if not date_objs:
        return {}

    adj_map = {}
    with db.get_session() as session:
        rows = (
            session.query(StockAdjFactor)
            .filter(
                StockAdjFactor.code.in_(bare_codes),
                StockAdjFactor.trade_date.in_(date_objs),
            )
            .all()
        )

    for r in rows:
        code_str = str(r.code).split(".")[0].zfill(6)
        d = (
            r.trade_date.strftime("%Y%m%d")
            if hasattr(r.trade_date, "strftime")
            else str(r.trade_date).replace("-", "")[:8]
        )
        adj_map[(code_str, d)] = float(r.adj_factor)

    return adj_map


class LGBTrainer:
    """LightGBM 因子收益预测器。

    用法:
        trainer = LGBTrainer(mode="postmarket")
        trainer.prepare_data(start_date="2024-01-01", end_date="2025-12-31")
        trainer.train()
        predictions = trainer.predict()
        importance = trainer.get_feature_importance()
    """

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
        self.days_model: Optional[LGBMRegressor] = None
        self.feature_names: List[str] = []
        self._X_train: Optional[pd.DataFrame] = None
        self._y_train: Optional[pd.Series] = None
        self._y_days_train: Optional[pd.Series] = None
        self._X_latest: Optional[pd.DataFrame] = None
        self._latest_date: Optional[str] = None
        self._latest_codes: List[str] = []
        self._training_metrics: Dict = {}

    # ------------------------------------------------------------------
    # Data Preparation
    # ------------------------------------------------------------------

    def prepare_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """从 factor_score_snapshots 构建特征矩阵与目标变量。

        使用 SQL 级 PIVOT（MAX+CASE WHEN）避免将 60M 行加载到 Python 内存。
        特征矩阵: 每行 = (trade_date, ts_code)，每列 = 一个因子 score
        目标变量: N 日后的涨跌幅（从 stock_daily 计算）
        """
        db = _get_db()
        cb = self.progress_callback
        from sqlalchemy import text as _text

        if start_date is not None:
            start_date = start_date.replace("-", "")
        if end_date is not None:
            end_date = end_date.replace("-", "")

        if cb:
            cb(f"正在查询{mode_label(self.mode)}因子快照...")

        with db.get_session() as session:
            factor_rows = session.execute(
                _text(
                    "SELECT DISTINCT factor_name FROM factor_score_snapshots "
                    "WHERE mode = :mode"
                ),
                {"mode": self.mode},
            ).fetchall()
            if not factor_rows:
                raise ValueError(
                    f"factor_score_snapshots 中没有 mode={self.mode} 的数据"
                )
        factor_names = sorted(r[0] for r in factor_rows)
        self.feature_names = factor_names

        if cb:
            cb(f"发现 {len(factor_names)} 个因子，正在 SQL PIVOT 构建特征矩阵...")

        cols = ", ".join(
            f"MAX(CASE WHEN factor_name = :fn{i} THEN score END) AS \"{fn}\""
            for i, fn in enumerate(factor_names)
        )

        sql_where = "WHERE mode = :mode"
        params: Dict = {"mode": self.mode}
        if start_date:
            sql_where += " AND trade_date >= :start_date"
            params["start_date"] = start_date
        if end_date:
            sql_where += " AND trade_date <= :end_date"
            params["end_date"] = end_date
        for i, fn in enumerate(factor_names):
            params[f"fn{i}"] = fn

        pivot_sql = (
            f"SELECT trade_date, ts_code, {cols} "
            f"FROM factor_score_snapshots {sql_where} "
            f"GROUP BY trade_date, ts_code"
        )

        with db.get_session() as session:
            conn = session.connection()
            X = pd.read_sql_query(_text(pivot_sql), conn, params=params)

        if X.empty:
            raise ValueError(
                f"factor_score_snapshots 中没有 mode={self.mode} "
                f"日期范围 [{start_date or 'any'}, {end_date or 'any'}] 的数据"
            )

        if cb:
            cb(f"特征矩阵: {X.shape[0]:,} 行 × {len(self.feature_names)} 因子")

        X = X.dropna(subset=self.feature_names[:min(3, len(self.feature_names))])

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

    def _compute_forward_returns(
        self, db: DatabaseManager, X: pd.DataFrame,
        _start: Optional[str], _end: Optional[str],
    ) -> pd.Series:
        """计算每只股票在每个交易日的 forward N-day 收益（后复权）。

        exec_mode="close": buy at td close, sell at (td+N) close
        exec_mode="open":  buy at next-trading-day open, sell at N days later open

        使用 stock_adj_factor 表后复权，消除除权除息影响：
        adj_return = (1 + unadj_return) × (adj_sell / adj_buy) - 1
        """
        from src.storage import StockAdjFactor, StockDaily
        from sqlalchemy import func

        trading_dates = sorted(X["trade_date"].unique())
        all_codes = X["ts_code"].unique().tolist()
        bare_codes = [c.split(".")[0] for c in all_codes]

        if self.exec_mode == "open":
            # ── Open-to-open ──
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

            needed_dates: set = set(trading_dates)
            buy_dates_by_td: Dict[str, Optional[str]] = {}
            sell_dates_by_td: Dict[str, Optional[str]] = {}
            for td in trading_dates:
                buy_date = None
                for d in trading_days_all:
                    if d > td:
                        buy_date = d
                        break
                buy_dates_by_td[td] = buy_date
                if buy_date:
                    needed_dates.add(buy_date)
                    try:
                        buy_idx = trading_days_all.index(buy_date)
                        sell_idx = buy_idx + self.forward_days
                        if sell_idx < len(trading_days_all):
                            sell_date = trading_days_all[sell_idx]
                            sell_dates_by_td[td] = sell_date
                            needed_dates.add(sell_date)
                        else:
                            sell_dates_by_td[td] = None
                    except ValueError:
                        sell_dates_by_td[td] = None
                else:
                    sell_dates_by_td[td] = None

            # ── Fetch prices & adj_factors ──
            with db.get_session() as session:
                rows = session.query(StockDaily).filter(
                    StockDaily.date.in_([pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:8]}") for d in needed_dates]),
                    StockDaily.code.in_(bare_codes),
                ).all()

            price_map: Dict[Tuple[str, str], Dict[str, float]] = {}
            for r in rows:
                code_str = str(r.code).split(".")[0]
                d = r.date.strftime("%Y%m%d")
                price_map[(d, code_str)] = {
                    "open": float(r.open) if r.open else 0.0,
                    "close": float(r.close) if r.close else 0.0,
                }

            adj_map = _load_adj_factors(db, bare_codes, needed_dates)

            results = {}
            for td in trading_dates:
                buy_date = buy_dates_by_td.get(td)
                sell_date = sell_dates_by_td.get(td)
                if not buy_date or not sell_date:
                    continue
                td_codes = X[X["trade_date"] == td]["ts_code"].tolist()
                for code in td_codes:
                    bare = str(code).split(".")[0]
                    buy_entry = price_map.get((buy_date, bare))
                    sell_entry = price_map.get((sell_date, bare))
                    if buy_entry and sell_entry and buy_entry["open"] > 0 and sell_entry["open"] > 0:
                        unadj_ret = (sell_entry["open"] - buy_entry["open"]) / buy_entry["open"]
                        adj_buy = adj_map.get((bare, buy_date), 1.0)
                        adj_sell = adj_map.get((bare, sell_date), 1.0)
                        if adj_buy > 0 and adj_sell > 0:
                            results[(td, code)] = (1.0 + unadj_ret) * (adj_sell / adj_buy) - 1.0
            return pd.Series(results, name=f"fwd_{self.forward_days}d")

        # ── Close-to-close ──
        with db.get_session() as session:
            rows = session.query(StockDaily).filter(
                StockDaily.date.in_([pd.Timestamp(d) for d in trading_dates]),
                StockDaily.code.in_(bare_codes),
            ).all()

        price_map: Dict[Tuple[str, str], float] = {}
        for r in rows:
            code_str = str(r.code).split(".")[0]
            price_map[(r.date.strftime("%Y%m%d"), code_str)] = float(r.close)

        adj_map = _load_adj_factors(db, bare_codes, set(trading_dates))

        results = {}
        for i, td in enumerate(trading_dates):
            sell_idx = i + self.forward_days
            if sell_idx >= len(trading_dates):
                continue
            sell_date = trading_dates[sell_idx]
            td_codes = X[X["trade_date"] == td]["ts_code"].tolist()
            for code in td_codes:
                bare = str(code).split(".")[0]
                bp = price_map.get((td, bare))
                sp = price_map.get((sell_date, bare))
                if bp and sp and bp > 0:
                    unadj_ret = (sp - bp) / bp
                    adj_buy = adj_map.get((bare, td), 1.0)
                    adj_sell = adj_map.get((bare, sell_date), 1.0)
                    if adj_buy > 0 and adj_sell > 0:
                        results[(td, code)] = (1.0 + unadj_ret) * (adj_sell / adj_buy) - 1.0
        return pd.Series(results, name=f"fwd_{self.forward_days}d")

    # ------------------------------------------------------------------
    # Peak Speed Label Helpers
    # ------------------------------------------------------------------

    def _calc_peak_from_prices(
        self, forward_prices: np.ndarray, buy_price: float,
    ) -> Tuple[float, int]:
        """Calculate peak return and days to peak from a price array."""
        if len(forward_prices) == 0 or buy_price <= 0:
            return 0.0, self.window_days

        returns = forward_prices / buy_price - 1.0
        peak_idx = int(np.argmax(returns))
        peak_return = float(returns[peak_idx])

        if peak_return < self.peak_min_return:
            return 0.0, self.window_days

        days_to_peak = peak_idx + 1
        return peak_return, days_to_peak

    @staticmethod
    def _winsorize(series: pd.Series, quantile: float) -> pd.Series:
        """Clip series at symmetric quantile boundaries."""
        lower = series.quantile(1.0 - quantile)
        upper = series.quantile(quantile)
        return series.clip(lower=lower, upper=upper)

    def _compute_peak_speed_labels(
        self, db: DatabaseManager, X: pd.DataFrame,
        _start: Optional[str], _end: Optional[str],
    ) -> Tuple[pd.Series, pd.Series]:
        """Compute peak_return and days_to_peak labels for peak_speed mode."""
        from src.storage import StockAdjFactor, StockDaily
        from sqlalchemy import func

        trading_dates = sorted(X["trade_date"].unique())
        all_codes = X["ts_code"].unique().tolist()
        bare_codes = [c.split(".")[0] for c in all_codes]

        with db.get_session() as session:
            dates_raw = (
                session.query(StockDaily.date)
                .group_by(StockDaily.date)
                .having(func.count(StockDaily.code) >= 3000)
                .order_by(StockDaily.date)
                .all()
            )
        trading_days_all = sorted(
            d[0].strftime("%Y%m%d") if hasattr(d[0], "strftime")
            else str(d[0]).replace("-", "")[:8]
            for d in dates_raw
        )

        needed_dates: set = set(trading_dates)
        window_dates_by_td: Dict[str, List[str]] = {}

        for td in trading_dates:
            try:
                td_idx = trading_days_all.index(td)
            except ValueError:
                continue
            if self.exec_mode == "open":
                # open 模式: 买入日 = 下一个交易日, peak 搜索从买入日后一天开始
                window = trading_days_all[td_idx + 2: td_idx + 2 + self.window_days]
            else:
                # close 模式: 买入日 = 预测日, peak 搜索从下一个交易日开始
                window = trading_days_all[td_idx + 1: td_idx + 1 + self.window_days]
            if len(window) < 3:
                continue
            window_dates_by_td[td] = window
            needed_dates.update(window)

        price_col = "open" if self.exec_mode == "open" else "close"
        with db.get_session() as session:
            rows = session.query(StockDaily).filter(
                StockDaily.date.in_([
                    pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:8]}")
                    for d in needed_dates
                ]),
                StockDaily.code.in_(bare_codes),
            ).all()

        price_map: Dict[Tuple[str, str], float] = {}
        for r in rows:
            code_str = str(r.code).split(".")[0]
            d = r.date.strftime("%Y%m%d")
            p = float(getattr(r, price_col)) if getattr(r, price_col) else 0.0
            price_map[(d, code_str)] = p

        adj_map = _load_adj_factors(db, bare_codes, needed_dates)

        peak_results = {}
        days_results = {}

        for td, window in window_dates_by_td.items():
            td_codes = X[X["trade_date"] == td]["ts_code"].tolist()
            for code in td_codes:
                bare = str(code).split(".")[0]
                # open 模式: 买入价 = 下一个交易日的 open; close 模式: 买入价 = 预测日的 close
                buy_date = window[0] if self.exec_mode == "open" else td
                buy_price = price_map.get((buy_date, bare))
                if not buy_price or buy_price <= 0:
                    continue

                adj_buy = adj_map.get((bare, buy_date), 1.0)
                if adj_buy <= 0:
                    continue

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

        if len(peak_series) > 0:
            peak_series = self._winsorize(peak_series, self.winsorize_quantile)

        return peak_series, days_series

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(
        self,
        n_estimators: int = 200,
        num_leaves: int = 31,
        learning_rate: float = 0.05,
        cv_folds: int = 5,
        **kwargs,
    ) -> LGBMRegressor:
        """训练 LightGBM 回归模型。"""
        if self._X_train is None or self._y_train is None:
            raise RuntimeError("请先调用 prepare_data() 准备训练数据")

        X = self._X_train[self.feature_names].fillna(0).values
        y = self._y_train.values

        model = LGBMRegressor(
            n_estimators=n_estimators,
            num_leaves=num_leaves,
            learning_rate=learning_rate,
            verbose=-1,
            random_state=42,
            **kwargs,
        )

        if cv_folds > 1 and len(X) > cv_folds * 100:
            tscv = TimeSeriesSplit(n_splits=cv_folds)
            cv_scores = []
            rank_ics = []
            oof_preds = np.full(len(X), np.nan)
            for train_idx, val_idx in tscv.split(X):
                X_tr, X_val = X[train_idx], X[val_idx]
                y_tr, y_val = y[train_idx], y[val_idx]
                model.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], callbacks=[])
                pred = model.predict(X_val)
                cv_scores.append(np.sqrt(mean_squared_error(y_val, pred)))
                oof_preds[val_idx] = pred
                if len(pred) > 5:
                    ic, _ = spearmanr(pred, y_val)
                    if np.isfinite(ic):
                        rank_ics.append(float(ic))

            self._training_metrics["cv_rmse_mean"] = float(np.mean(cv_scores))
            self._training_metrics["cv_rmse_std"] = float(np.std(cv_scores))
            self._training_metrics["cv_scores"] = [round(s, 6) for s in cv_scores]

            if rank_ics:
                ic_mean = float(np.mean(rank_ics))
                ic_std = float(np.std(rank_ics))
                self._training_metrics["rank_ic_mean"] = round(ic_mean, 6)
                self._training_metrics["rank_ic_std"] = round(ic_std, 6)
                self._training_metrics["icir"] = (
                    round(ic_mean / ic_std, 4) if ic_std > 1e-9 else None
                )

            valid_mask = ~np.isnan(oof_preds)
            if valid_mask.sum() > 10:
                oof_corr = float(np.corrcoef(oof_preds[valid_mask], y[valid_mask])[0, 1])
                if np.isfinite(oof_corr):
                    self._training_metrics["oof_corr"] = round(oof_corr, 6)

        model.fit(X, y)
        self.model = model
        self._training_metrics["n_samples"] = int(len(X))
        self._training_metrics["n_features"] = int(len(self.feature_names))

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

        return model

    # ------------------------------------------------------------------
    # Feature Importance
    # ------------------------------------------------------------------

    def get_feature_importance(self) -> Dict[str, Dict[str, float]]:
        """获取特征重要性。"""
        if self.model is None:
            raise RuntimeError("请先调用 train() 训练模型")

        gain = {k: float(v) for k, v in zip(
            self.feature_names,
            self.model.booster_.feature_importance(importance_type="gain"),
        )}
        split = {k: float(v) for k, v in zip(
            self.feature_names,
            self.model.booster_.feature_importance(importance_type="split"),
        )}
        return {
            "gain": dict(sorted(gain.items(), key=lambda x: x[1], reverse=True)),
            "split": dict(sorted(split.items(), key=lambda x: x[1], reverse=True)),
        }

    def get_tree_diagnostics(self) -> Dict[str, float]:
        """从 booster 提取树结构诊断信息。"""
        if self.model is None:
            raise RuntimeError("请先调用 train() 训练模型")

        model_dump = self.model.booster_.dump_model()
        trees = model_dump.get("tree_info", [])
        n_trees = len(trees)
        if n_trees == 0:
            return {"n_trees": 0, "avg_depth": 0, "avg_n_leaves": 0, "total_n_leaves": 0}

        def _tree_depth(node: dict, depth: int = 0) -> int:
            if "leaf_index" in node or "leaf_value" in node:
                return depth
            left = node.get("left_child", {})
            right = node.get("right_child", {})
            return max(_tree_depth(left, depth + 1), _tree_depth(right, depth + 1))

        depths = []
        leaves = []
        for t in trees:
            leaves.append(t.get("num_leaves", 0))
            structure = t.get("tree_structure", {})
            depths.append(_tree_depth(structure))

        return {
            "n_trees": n_trees,
            "avg_depth": round(float(np.mean(depths)), 2),
            "avg_n_leaves": round(float(np.mean(leaves)), 2),
            "total_n_leaves": int(np.sum(leaves)),
        }

    def get_prediction_stats(self) -> Optional[Dict[str, float]]:
        """计算最新预测得分的分布统计。"""
        if self._X_latest is None:
            return None

        scores = self._X_latest["lgb_score"].values
        if len(scores) == 0:
            return None

        from scipy.stats import skew as _skew, kurtosis as _kurtosis
        return {
            "mean": round(float(np.mean(scores)), 6),
            "std": round(float(np.std(scores)), 6),
            "skew": round(float(_skew(scores)), 4),
            "kurtosis": round(float(_kurtosis(scores)), 4),
            "min": round(float(np.min(scores)), 6),
            "max": round(float(np.max(scores)), 6),
            "median": round(float(np.median(scores)), 6),
        }

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """对指定交易日（默认最新）的全市场股票打分。

        优先从 factor_score_snapshots 读取因子分；若当天数据缺失，
        则通过 discovery engine 从实时数据（realtime_spot + stock_daily）在内存中计算因子分。
        """
        if self.model is None:
            raise RuntimeError("请先调用 train() 训练模型")
        if target_date is not None:
            target_date = target_date.replace("-", "")

        db = _get_db()
        from src.storage import FactorScoreSnapshot

        if target_date is None:
            with db.get_session() as session:
                row = session.query(FactorScoreSnapshot.trade_date).filter(
                    FactorScoreSnapshot.mode == self.mode,
                ).order_by(FactorScoreSnapshot.trade_date.desc()).first()
                if not row:
                    target_date = None  # let fallback handle it
                else:
                    target_date = row[0]
            # 如果最新的快照日期早于今天交易日，优先使用今天实时计算
            try:
                from data_provider.tushare_fetcher import TushareFetcher
                fetcher = TushareFetcher()
                today_str = fetcher.get_trade_time(early_time="18:01", late_time="04:59")
                if today_str and target_date and today_str > target_date:
                    target_date = today_str
            except Exception:
                pass
            if target_date is None:
                # No snapshots at all - use today as target for realtime compute
                from datetime import date as _date
                target_date = _date.today().strftime("%Y%m%d")

        self._latest_date = target_date

        with db.get_session() as session:
            rows = session.query(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.mode == self.mode,
                FactorScoreSnapshot.trade_date == target_date,
            ).all()

        if not rows:
            # Fallback: compute factor scores in memory via discovery engine
            rows = self._compute_realtime_scores(db, target_date)

        if not rows:
            raise ValueError(f"无法获取 {target_date} 的因子数据（DB 快照缺失且实时计算失败）")

        records = [
            {"ts_code": r["ts_code"] if isinstance(r, dict) else r.ts_code,
             "factor_name": r["factor_name"] if isinstance(r, dict) else r.factor_name,
             "score": r["score"] if isinstance(r, dict) else r.score}
            for r in rows
        ]
        df = pd.DataFrame(records)
        pivot = df.pivot_table(
            index="ts_code", columns="factor_name",
            values="score", aggfunc="mean",
        )
        pivot.reset_index(inplace=True)

        for f in self.feature_names:
            if f not in pivot.columns:
                pivot[f] = 0.0

        self._latest_codes = pivot["ts_code"].tolist()
        X_pred = pivot[self.feature_names].fillna(0).values
        scores = self.model.predict(X_pred)
        pivot["lgb_score"] = scores

        smin, smax = scores.min(), scores.max()
        if smax > smin:
            pivot["lgb_score_norm"] = (scores - smin) / (smax - smin) * 100
        else:
            pivot["lgb_score_norm"] = 50.0

        # Predict days_to_peak for peak_speed mode
        if self.label_mode == "peak_speed" and self.days_model is not None:
            days_pred = self.days_model.predict(X_pred)
            pivot["predicted_days"] = np.clip(
                np.round(days_pred), 1, self.window_days
            ).astype(int)

        pivot = pivot.sort_values("lgb_score", ascending=False)
        pivot["stock_code"] = (
            pivot["ts_code"].str.replace(".SH", "").str.replace(".SZ", "")
        )

        # 补股票名称
        with db.get_session() as session:
            from src.storage import RealtimeSpot
            codes = pivot["stock_code"].unique().tolist()
            spots = session.query(RealtimeSpot.code, RealtimeSpot.name).filter(
                RealtimeSpot.code.in_(codes),
            ).all()
        name_map = {s.code: s.name for s in spots if s.name}
        pivot["stock_name"] = pivot["stock_code"].map(name_map).fillna("")

        self._X_latest = pivot
        return pivot

    def _compute_realtime_scores(self, db, target_date: str) -> List[Dict]:
        """通过 discovery engine 在内存中计算因子分（不写入 DB）。

        用于 predict() 的兜底路径：当 factor_score_snapshots 没有当天数据时，
        从 realtime_spot + stock_daily + 各因子自身数据源实时计算因子分。
        """
        import logging
        _log = logging.getLogger(__name__)

        try:
            from src.discovery.engine import create_discovery_engine
            engine = create_discovery_engine()
            results = engine.discover(
                mode=self.mode,
                trade_date=target_date,
                skip_persist=True,
            )
            raw_scores = getattr(engine, '_last_raw_scores', {})
            _log.info(
                "Realtime factor scoring: %d factors scored for %s mode on %s (results=%d)",
                len(raw_scores), self.mode, target_date, len(results),
            )
        except Exception as e:
            _log.warning("Realtime factor scoring failed: %s", e)
            return []

        if not raw_scores:
            return []

        # Convert raw_scores {factor_name: Series(index=bare_code, score)} to snapshot rows
        rows: list = []
        for factor_name, series in raw_scores.items():
            if not hasattr(series, 'items'):
                continue
            for ts_code, score in series.items():
                if score is None:
                    continue
                rows.append({
                    "ts_code": str(ts_code),
                    "factor_name": factor_name,
                    "score": float(score),
                })
        return rows

    def get_latest_predictions(self, top_n: int = 5) -> List[Dict]:
        """获取最新预测结果列表，自动过滤 ST 股顺延。"""
        if self._X_latest is None:
            raise RuntimeError("请先调用 predict() 生成预测")

        df = self._X_latest
        results = []
        for _, row in df.iterrows():
            name = str(row.get("stock_name", ""))
            if is_st_stock(name):
                continue
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
            if len(results) >= top_n:
                break
        return results

    # ------------------------------------------------------------------
    # Backtest Comparison
    # ------------------------------------------------------------------

    def backtest_compare(
        self, top_n: int = 10, start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        """LightGBM vs 因子加权体系的回测对比。"""
        db = _get_db()
        from src.discovery.engine import get_factor_weights
        from src.discovery.factor_backtest_engine import FactorBacktestEngine
        from data_provider.tushare_fetcher import TushareFetcher
        from src.storage import FactorScoreSnapshot, StockDaily

        try:
            fetcher = TushareFetcher.get_instance()
            be = FactorBacktestEngine(fetcher)
            weights = get_factor_weights(self.mode)
            fb_result = be.compute(
                mode=self.mode, factor_weights=weights,
                start_date=start_date, end_date=end_date,
                top_n=top_n, hold_days=[self.forward_days],
                initial_capital=1_000_000.0,
                risk_free_rate=0.02, use_pipeline=False,
            )
            factor_summary: Dict = {}
            if fb_result:
                from dataclasses import asdict
                factor_summary = {
                    "cumulative_return": round(float(fb_result.summary.cumulative_return), 4),
                    "win_rate": round(float(fb_result.summary.win_rate), 4),
                    "max_drawdown": round(float(fb_result.summary.max_drawdown), 4),
                    "sharpe_ratio": round(float(fb_result.summary.sharpe_ratio), 4),
                }
        except Exception as e:
            factor_summary = {"error": str(e)}

        # LGB 回测
        with db.get_session() as session:
            dates_query = session.query(FactorScoreSnapshot.trade_date).filter(
                FactorScoreSnapshot.mode == self.mode,
            )
            if start_date:
                dates_query = dates_query.filter(
                    FactorScoreSnapshot.trade_date >= start_date)
            if end_date:
                dates_query = dates_query.filter(
                    FactorScoreSnapshot.trade_date <= end_date)
            all_dates = sorted(set(r[0] for r in dates_query.distinct().all()))

        if len(all_dates) < self.forward_days + 5:
            return {"error": "数据不足", "factor_summary": factor_summary}

        lgb_capital = 1.0
        bench_capital = 1.0
        capital_curve: List[Dict] = []
        lgb_returns: List[float] = []
        win_count = 0
        total_trades = 0

        for i in range(len(all_dates) - self.forward_days):
            td = all_dates[i]
            sell_date = all_dates[i + self.forward_days]

            # LGB 组合收益
            try:
                lgb_ret = self._calc_lgb_return(db, td, sell_date, top_n)
                if lgb_ret is not None:
                    lgb_returns.append(lgb_ret)
                    lgb_capital *= (1 + lgb_ret)
                    if lgb_ret > 0:
                        win_count += 1
                    total_trades += 1
            except Exception:
                pass

            # 基准收益
            bench_ret = self._calc_benchmark_return(db, td, sell_date)
            if bench_ret is not None:
                bench_capital *= (1 + bench_ret)

            capital_curve.append({
                "date": td,
                "lgb": round(lgb_capital, 6),
                "benchmark": round(bench_capital, 6),
            })

        win_rate = win_count / total_trades if total_trades > 0 else 0
        lgb_drawdown = self._calc_max_drawdown([p["lgb"] for p in capital_curve])

        return {
            "lgb_metrics": {
                "cumulative_return": round(lgb_capital - 1, 4),
                "win_rate": round(win_rate, 4),
                "max_drawdown": round(lgb_drawdown, 4),
                "total_trades": total_trades,
            },
            "factor_metrics": factor_summary,
            "capital_curve": capital_curve,
            "comparison": {
                "lgb_return": round(lgb_capital - 1, 4),
                "factor_return": factor_summary.get("cumulative_return", 0),
                "benchmark_return": round(bench_capital - 1, 4),
            },
        }

    def backtest_peak_speed(
        self,
        top_n: int = 5,
        stop_loss: float = -0.05,
        take_profit_ratio: float = 0.8,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        """Peak speed 模式回测：动态退出策略。

        每个持仓根据模型预测的 predicted_return 和 predicted_days 决定退出时机。
        退出优先级: 止损 → 止盈 → 到期窗口 → 强制退出。
        """
        if self.label_mode != "peak_speed":
            raise RuntimeError("backtest_peak_speed 仅适用于 peak_speed 模式")
        if self.model is None or self.days_model is None:
            raise RuntimeError("需要主模型和辅助模型（days_model）")

        db = _get_db()
        from src.storage import FactorScoreSnapshot, StockDaily

        with db.get_session() as session:
            dates_query = session.query(FactorScoreSnapshot.trade_date).filter(
                FactorScoreSnapshot.mode == self.mode,
            )
            if start_date:
                dates_query = dates_query.filter(
                    FactorScoreSnapshot.trade_date >= start_date.replace("-", ""))
            if end_date:
                dates_query = dates_query.filter(
                    FactorScoreSnapshot.trade_date <= end_date.replace("-", ""))
            all_dates = sorted(set(r[0] for r in dates_query.distinct().all()))

        if len(all_dates) < self.window_days + 5:
            return {"error": "数据不足（需要至少 window_days + 5 个交易日的因子快照）"}

        # State
        positions: List[Dict] = []  # {ts_code, bare, buy_date, buy_price, pred_return, pred_days, alloc, entry_idx}
        cash = 1.0
        trades: List[Dict] = []
        capital_curve: List[Dict] = []
        exit_reasons = {"stop_loss": 0, "take_profit": 0, "arrival": 0, "force_exit": 0}

        for date_idx, td in enumerate(all_dates):
            # --- 1. Check exit conditions for open positions ---
            if positions:
                held_codes = [p["bare"] for p in positions]
                with db.get_session() as session:
                    price_rows = session.query(StockDaily).filter(
                        StockDaily.date == pd.Timestamp(td),
                        StockDaily.code.in_(held_codes),
                    ).all()
                price_map = {str(r.code): float(r.close) for r in price_rows}

                adj_map = _load_adj_factors(db, held_codes, {td}) if held_codes else {}

                to_close = []
                for pos in positions:
                    cur_price = price_map.get(pos["bare"])
                    if cur_price is None or cur_price <= 0:
                        continue
                    held_days = date_idx - pos["entry_idx"]

                    adj_buy = pos.get("adj_buy", 1.0)
                    adj_cur = adj_map.get((pos["bare"], td), 1.0)
                    if adj_buy > 0 and adj_cur > 0:
                        raw_ret = (cur_price - pos["buy_price"]) / pos["buy_price"]
                        adj_return = (1.0 + raw_ret) * (adj_cur / adj_buy) - 1.0
                    else:
                        adj_return = (cur_price - pos["buy_price"]) / pos["buy_price"]

                    exit_reason = None
                    if adj_return <= stop_loss:
                        exit_reason = "stop_loss"
                    elif adj_return >= pos["pred_return"] * take_profit_ratio:
                        exit_reason = "take_profit"
                    elif abs(held_days - pos["pred_days"]) <= 2 and adj_return > 0:
                        exit_reason = "arrival"
                    elif held_days > self.window_days:
                        exit_reason = "force_exit"

                    if exit_reason:
                        to_close.append((pos, adj_return, exit_reason, held_days))

                for pos, ret, reason, held in to_close:
                    cash += pos["alloc"] * (1.0 + ret)
                    trades.append({
                        "ts_code": pos["ts_code"],
                        "buy_date": pos["buy_date"],
                        "sell_date": td,
                        "return_pct": round(ret, 6),
                        "holding_days": held,
                        "exit_reason": reason,
                    })
                    exit_reasons[reason] += 1
                    positions.remove(pos)

            # --- 2. Open new positions if slots available ---
            open_slots = top_n - len(positions)
            if open_slots > 0 and cash > 0.01:
                with db.get_session() as session:
                    rows = session.query(FactorScoreSnapshot).filter(
                        FactorScoreSnapshot.mode == self.mode,
                        FactorScoreSnapshot.trade_date == td,
                    ).all()

                if rows:
                    records = [
                        {"ts_code": r.ts_code, "factor_name": r.factor_name, "score": r.score}
                        for r in rows
                    ]
                    df = pd.DataFrame(records)
                    pivot = df.pivot_table(
                        index="ts_code", columns="factor_name",
                        values="score", aggfunc="mean",
                    )
                    pivot.fillna(0, inplace=True)
                    for f in self.feature_names:
                        if f not in pivot.columns:
                            pivot[f] = 0.0

                    X = pivot[self.feature_names].fillna(0).values
                    scores = self.model.predict(X)
                    days_pred = self.days_model.predict(X)
                    pivot["_score"] = scores
                    pivot["_days"] = np.clip(np.round(days_pred), 1, self.window_days).astype(int)

                    held_ts = {p["ts_code"] for p in positions}
                    candidates = pivot[~pivot.index.isin(held_ts)].nlargest(
                        open_slots, "_score"
                    )

                    if len(candidates) > 0:
                        # Get buy prices
                        cand_codes = [c.split(".")[0] for c in candidates.index]
                        with db.get_session() as session:
                            buy_rows = session.query(StockDaily).filter(
                                StockDaily.date == pd.Timestamp(td),
                                StockDaily.code.in_(cand_codes),
                            ).all()
                        buy_prices = {str(r.code): float(r.close) for r in buy_rows}
                        adj_map_buy = _load_adj_factors(db, cand_codes, {td})

                        alloc_per = cash / top_n
                        for ts_code in candidates.index:
                            bare = ts_code.split(".")[0]
                            bp = buy_prices.get(bare)
                            if bp is None or bp <= 0:
                                continue
                            positions.append({
                                "ts_code": ts_code,
                                "bare": bare,
                                "buy_date": td,
                                "buy_price": bp,
                                "pred_return": float(candidates.loc[ts_code, "_score"]),
                                "pred_days": int(candidates.loc[ts_code, "_days"]),
                                "alloc": alloc_per,
                                "adj_buy": adj_map_buy.get((bare, td), 1.0),
                                "entry_idx": date_idx,
                            })
                            cash -= alloc_per

            # --- 3. Record capital curve (mark-to-market) ---
            mtm = cash
            for pos in positions:
                mtm += pos["alloc"]  # simplified: assume flat for open positions intra-day
            capital_curve.append({"date": td, "capital": round(mtm, 6)})

        # Force-close any remaining positions at last available date
        if positions:
            last_td = all_dates[-1]
            held_codes = [p["bare"] for p in positions]
            with db.get_session() as session:
                price_rows = session.query(StockDaily).filter(
                    StockDaily.date == pd.Timestamp(last_td),
                    StockDaily.code.in_(held_codes),
                ).all()
            price_map = {str(r.code): float(r.close) for r in price_rows}
            adj_map = _load_adj_factors(db, held_codes, {last_td})

            for pos in list(positions):
                cur_price = price_map.get(pos["bare"])
                held_days = len(all_dates) - 1 - pos["entry_idx"]
                if cur_price and cur_price > 0:
                    adj_buy = pos.get("adj_buy", 1.0)
                    adj_cur = adj_map.get((pos["bare"], last_td), 1.0)
                    if adj_buy > 0 and adj_cur > 0:
                        raw_ret = (cur_price - pos["buy_price"]) / pos["buy_price"]
                        adj_return = (1.0 + raw_ret) * (adj_cur / adj_buy) - 1.0
                    else:
                        adj_return = (cur_price - pos["buy_price"]) / pos["buy_price"]
                else:
                    adj_return = 0.0
                cash += pos["alloc"] * (1.0 + adj_return)
                trades.append({
                    "ts_code": pos["ts_code"],
                    "buy_date": pos["buy_date"],
                    "sell_date": last_td,
                    "return_pct": round(adj_return, 6),
                    "holding_days": held_days,
                    "exit_reason": "force_exit",
                })
                exit_reasons["force_exit"] += 1

        total_trades = len(trades)
        win_count = sum(1 for t in trades if t["return_pct"] > 0)
        avg_hold = float(np.mean([t["holding_days"] for t in trades])) if trades else 0.0
        capital_values = [c["capital"] for c in capital_curve]
        max_dd = self._calc_max_drawdown(capital_values) if capital_values else 0.0

        return {
            "metrics": {
                "cumulative_return": round(cash - 1.0, 4),
                "win_rate": round(win_count / total_trades, 4) if total_trades > 0 else 0.0,
                "max_drawdown": round(max_dd, 4),
                "total_trades": total_trades,
                "avg_holding_days": round(avg_hold, 1),
                "exit_reasons": exit_reasons,
            },
            "capital_curve": capital_curve,
            "trades": trades,
        }

    def _calc_lgb_return(
        self, db, td: str, sell_date: str, top_n: int,
    ) -> Optional[float]:
        """计算 LGB 模型在单日的 top N 等权组合收益。"""
        from src.storage import FactorScoreSnapshot, StockDaily

        with db.get_session() as session:
            rows = session.query(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.mode == self.mode,
                FactorScoreSnapshot.trade_date == td,
            ).all()
        if not rows:
            return None

        records = [
            {"ts_code": r.ts_code, "factor_name": r.factor_name, "score": r.score}
            for r in rows
        ]
        df = pd.DataFrame(records)
        pivot = df.pivot_table(
            index="ts_code", columns="factor_name",
            values="score", aggfunc="mean",
        )
        pivot.fillna(0, inplace=True)

        for f in self.feature_names:
            if f not in pivot.columns:
                pivot[f] = 0.0

        pivot["composite"] = self.model.predict(
            pivot[self.feature_names].fillna(0).values)
        top = pivot.nlargest(min(top_n, len(pivot)), "composite")

        codes = top.index.tolist()
        bare = [c.split(".")[0] for c in codes]

        with db.get_session() as session:
            prices = session.query(StockDaily).filter(
                StockDaily.date.in_([pd.Timestamp(td), pd.Timestamp(sell_date)]),
                StockDaily.code.in_(bare),
            ).all()

        buy_map: Dict[str, float] = {}
        sell_map: Dict[str, float] = {}
        for r in prices:
            d = r.date.strftime("%Y%m%d")
            if d == td:
                buy_map[str(r.code)] = float(r.close)
            elif d == sell_date:
                sell_map[str(r.code)] = float(r.close)

        adj_map = _load_adj_factors(db, bare, {td, sell_date})

        rets = []
        for c in bare:
            bp = buy_map.get(c)
            sp = sell_map.get(c)
            if bp and sp and bp > 0:
                raw_ret = (sp - bp) / bp
                adj_b = adj_map.get((c, td), 1.0)
                adj_s = adj_map.get((c, sell_date), 1.0)
                if adj_b > 0 and adj_s > 0:
                    rets.append((1.0 + raw_ret) * (adj_s / adj_b) - 1.0)

        return float(np.mean(rets)) if rets else None

    def _calc_benchmark_return(self, db, td: str, sell_date: str) -> Optional[float]:
        """计算等权全市场基准收益"""
        from src.storage import StockDaily

        with db.get_session() as session:
            rows = session.query(StockDaily).filter(
                StockDaily.date.in_([pd.Timestamp(td), pd.Timestamp(sell_date)]),
            ).all()

        buy_map: Dict[str, float] = {}
        sell_map: Dict[str, float] = {}
        all_bare: set = set()
        for r in rows:
            d = r.date.strftime("%Y%m%d")
            c = str(r.code).split(".")[0]
            if d == td:
                buy_map[c] = float(r.close)
            elif d == sell_date:
                sell_map[c] = float(r.close)
            all_bare.add(c)

        common = set(buy_map) & set(sell_map)
        if not common:
            return None

        adj_map = _load_adj_factors(db, sorted(common), {td, sell_date})

        rets = []
        for c in common:
            bp = buy_map.get(c)
            sp = sell_map.get(c)
            if bp and sp and bp > 0:
                raw_ret = (sp - bp) / bp
                adj_b = adj_map.get((c, td), 1.0)
                adj_s = adj_map.get((c, sell_date), 1.0)
                if adj_b > 0 and adj_s > 0:
                    rets.append((1.0 + raw_ret) * (adj_s / adj_b) - 1.0)
        return float(np.mean(rets)) if rets else None

    @staticmethod
    def _calc_max_drawdown(capital_series: List[float]) -> float:
        peak = capital_series[0] if capital_series else 1.0
        max_dd = 0.0
        for v in capital_series:
            if v > peak:
                peak = v
            dd = (peak - v) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)
        return max_dd

    # ------------------------------------------------------------------
    # Model Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _cleanup_same_config_models(new_name: str):
        """Delete old models matching the same mode/fwd/exec_mode."""
        parts = new_name.rsplit("_", 3)
        if len(parts) < 4:
            return
        prefix = parts[0]
        suffix = parts[3]

        if not os.path.isdir(MODEL_DIR):
            return
        import glob as _g
        pattern = f"{prefix}_*_{suffix}.joblib"
        for fp in _g.glob(os.path.join(MODEL_DIR, pattern)):
            try:
                os.remove(fp)
            except OSError:
                pass

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

        # Delete old models with the same (mode, forward_days, exec_mode)
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

        if self.label_mode == "peak_speed" and self.days_model is not None:
            days_path = os.path.join(MODEL_DIR, f"{name}_days.joblib")
            joblib.dump({"model": self.days_model, "meta": meta}, days_path)

        return model_path

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

        if trainer.label_mode == "peak_speed":
            days_path = model_path.replace(".joblib", "_days.joblib")
            if os.path.exists(days_path):
                days_data = joblib.load(days_path)
                trainer.days_model = days_data["model"]

        return trainer

    @staticmethod
    def list_models(label_mode: Optional[str] = None) -> List[Dict]:
        """列出 data/lgb_models/ 下所有已保存的模型。

        Args:
            label_mode: 可选过滤 "fixed" 或 "peak_speed"，只返回对应模式的模型。
        """
        if not os.path.isdir(MODEL_DIR):
            return []
        models = []
        for fname in sorted(os.listdir(MODEL_DIR), reverse=True):
            if fname.endswith(".joblib") and not fname.endswith("_days.joblib"):
                if label_mode == "peak_speed" and "_peak" not in fname:
                    continue
                if label_mode == "fixed" and "_fwd" not in fname:
                    continue
                full = os.path.join(MODEL_DIR, fname)
                size = os.path.getsize(full)
                mtime = datetime.fromtimestamp(os.path.getmtime(full))
                models.append({
                    "name": fname.replace(".joblib", ""),
                    "path": full,
                    "size_kb": round(size / 1024, 1),
                    "saved_at": mtime.isoformat(),
                })
        return models

    def save_report(self, top_n: int = 5) -> str:
        """保存训练报告到 discovery_reports/lgb_report_*.md/json。

        包含模型摘要、特征重要性、Top N 预测。
        """
        if self.model is None:
            raise RuntimeError("请先调用 train() 训练模型")
        if self._X_latest is None:
            self.predict()

        sd = getattr(self, "_train_start", self._latest_date or "unknown")
        ed = getattr(self, "_train_end", self._latest_date or "unknown")
        # normalize to YYYYMMDD without hyphens
        sd = str(sd).replace("-", "")[:8]
        ed = str(ed).replace("-", "")[:8]
        exec_suffix = "open2open" if self.exec_mode == "open" else "close2close"
        fwd_dir = f"fwd{self.forward_days}d"
        base = f"{self.mode}_fwd{self.forward_days}d_{sd}_{ed}_{exec_suffix}"
        report_dir = os.path.join(_REPORTS_DIR, exec_suffix, fwd_dir)
        md_path = os.path.join(report_dir, f"{base}.md")
        json_path = os.path.join(report_dir, f"{base}.json")
        os.makedirs(report_dir, exist_ok=True)

        importance = self.get_feature_importance()
        top_predictions = self.get_latest_predictions(top_n=top_n)
        metrics = self._training_metrics

        # ── Markdown ──
        lines = [
            f"# LGB 训练报告 · {mode_label(self.mode)} · 前向 {self.forward_days} 日",
            "",
            f"**日期范围**: {sd} ~ {ed}",
            f"**生成时间**: {datetime.now().strftime('%Y%m%d %H:%M:%S')}",
            f"**模式**: {mode_label(self.mode)}",
            f"**预测窗口**: {self.forward_days} 日",
            f"**训练样本**: {metrics.get('n_samples', 'N/A'):,}",
            f"**特征数**: {metrics.get('n_features', 'N/A')}",
        ]
        if "cv_rmse_mean" in metrics:
            lines.append(
                f"**CV RMSE**: {metrics['cv_rmse_mean']:.4f} "
                f"± {metrics.get('cv_rmse_std', 0):.4f}"
            )

        lines.extend([
            "",
            "## 特征重要性 (Gain Top 10)",
            "",
            "| 排名 | 因子 | Gain | Split |",
            "|------|------|------|-------|",
        ])
        for i, (name, g) in enumerate(list(importance["gain"].items())[:10]):
            s = importance["split"].get(name, 0)
            lines.append(f"| {i + 1} | {name} | {g:.1f} | {s:.1f} |")

        lines.extend([
            "",
            f"## Top {top_n} 预测",
            "",
            "| 排名 | 代码 | 名称 | LGB 评分 | 原始得分 |",
            "|------|------|------|----------|----------|",
        ])
        for p in top_predictions:
            lines.append(
                f"| {p['rank']} | {p['stock_code']} | {p['stock_name']} "
                f"| {p['lgb_score']:.2f} | {p['raw_score']:.4f} |"
            )

        with open(md_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        # ── JSON ──
        report = {
            "mode": self.mode,
            "forward_days": self.forward_days,
            "generated_at": datetime.now().isoformat(),
            "training_metrics": {k: v for k, v in metrics.items()},
            "feature_importance": {
                "gain": dict(list(importance["gain"].items())[:20]),
                "split": dict(list(importance["split"].items())[:20]),
            },
            "predictions": top_predictions,
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        return md_path
