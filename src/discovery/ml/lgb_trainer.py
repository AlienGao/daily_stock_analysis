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
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error

from src.storage import DatabaseManager

warnings.filterwarnings("ignore", category=UserWarning, module="lightgbm")

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
MODEL_DIR = os.path.join(_PROJECT_ROOT, "data", "lgb_models")


def _get_db() -> DatabaseManager:
    return DatabaseManager.get_instance()


def mode_label(mode: str) -> str:
    return "盘后" if mode == "postmarket" else "盘中"


class LGBTrainer:
    """LightGBM 因子收益预测器。

    用法:
        trainer = LGBTrainer(mode="postmarket")
        trainer.prepare_data(start_date="2024-01-01", end_date="2025-12-31")
        trainer.train()
        predictions = trainer.predict()
        importance = trainer.get_feature_importance()
    """

    def __init__(self, mode: str = "postmarket", forward_days: int = 5,
                 progress_callback=None):
        if mode not in ("intraday", "postmarket"):
            raise ValueError("mode 须为 intraday 或 postmarket")
        self.mode = mode
        self.forward_days = forward_days
        self.progress_callback = progress_callback
        self.model: Optional[LGBMRegressor] = None
        self.feature_names: List[str] = []
        self._X_train: Optional[pd.DataFrame] = None
        self._y_train: Optional[pd.Series] = None
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

        特征矩阵: 每行 = (trade_date, ts_code)，每列 = 一个因子 score
        目标变量: N 日后的涨跌幅（从 stock_daily 计算）
        """
        db = _get_db()
        cb = self.progress_callback

        from src.storage import FactorScoreSnapshot

        if cb:
            cb(f"正在查询{mode_label(self.mode)}因子快照...")
        with db.get_session() as session:
            q = session.query(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.mode == self.mode,
            )
            if start_date:
                q = q.filter(FactorScoreSnapshot.trade_date >= start_date)
            if end_date:
                q = q.filter(FactorScoreSnapshot.trade_date <= end_date)

            rows = q.all()
            if not rows:
                raise ValueError(
                    f"factor_score_snapshots 中没有 mode={self.mode} "
                    f"日期范围 [{start_date or 'any'}, {end_date or 'any'}] 的数据"
                )

            if cb:
                cb(f"已读取 {len(rows):,} 行快照数据，正在构建特征矩阵...")
            records = [
                {"trade_date": r.trade_date, "ts_code": r.ts_code,
                 "factor_name": r.factor_name, "score": r.score}
                for r in rows
            ]

        df = pd.DataFrame(records)
        df["score"] = pd.to_numeric(df["score"], errors="coerce")

        pivot = df.pivot_table(
            index=["trade_date", "ts_code"],
            columns="factor_name",
            values="score",
            aggfunc="mean",
        )
        pivot.reset_index(inplace=True)
        self.feature_names = [c for c in pivot.columns
                              if c not in ("trade_date", "ts_code")]
        if cb:
            cb(f"特征矩阵: {pivot.shape[0]:,} 行 × {len(self.feature_names)} 因子")
        X = pivot.dropna(subset=self.feature_names[:min(3, len(self.feature_names))])

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
               f"日期范围 {X.index.get_level_values(0).min()} ~ {X.index.get_level_values(0).max()}")
        self._X_train = X
        self._y_train = y
        return X, y

    def _compute_forward_returns(
        self, db: DatabaseManager, X: pd.DataFrame,
        _start: Optional[str], _end: Optional[str],
    ) -> pd.Series:
        """计算每只股票在每个交易日的 forward N-day 收益。"""
        from src.storage import StockDaily

        trading_dates = sorted(X["trade_date"].unique())
        all_codes = X["ts_code"].unique().tolist()
        bare_codes = [c.split(".")[0] for c in all_codes]

        with db.get_session() as session:
            rows = session.query(StockDaily).filter(
                StockDaily.date.in_([pd.Timestamp(d) for d in trading_dates]),
                StockDaily.code.in_(bare_codes),
            ).all()

        price_map: Dict[Tuple[str, str], float] = {}
        for r in rows:
            code_str = str(r.code)
            if "." not in code_str:
                suffix = ".SH" if code_str.startswith("6") else ".SZ"
                code_str = f"{code_str}{suffix}"
            price_map[(r.date.strftime("%Y%m%d"), code_str)] = float(r.close)

        results = {}
        for i, td in enumerate(trading_dates):
            sell_idx = i + self.forward_days
            if sell_idx >= len(trading_dates):
                continue
            sell_date = trading_dates[sell_idx]
            td_codes = X[X["trade_date"] == td]["ts_code"].tolist()
            for code in td_codes:
                bp = price_map.get((td, code))
                sp = price_map.get((sell_date, code))
                if bp and sp and bp > 0:
                    results[(td, code)] = (sp - bp) / bp
        return pd.Series(results, name=f"fwd_{self.forward_days}d")

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
            for train_idx, val_idx in tscv.split(X):
                X_tr, X_val = X[train_idx], X[val_idx]
                y_tr, y_val = y[train_idx], y[val_idx]
                model.fit(X_tr, y_tr, eval_set=[(X_val, y_val)], callbacks=[])
                pred = model.predict(X_val)
                cv_scores.append(np.sqrt(mean_squared_error(y_val, pred)))

            self._training_metrics["cv_rmse_mean"] = float(np.mean(cv_scores))
            self._training_metrics["cv_rmse_std"] = float(np.std(cv_scores))

        model.fit(X, y)
        self.model = model
        self._training_metrics["n_samples"] = len(X)
        self._training_metrics["n_features"] = len(self.feature_names)
        return model

    # ------------------------------------------------------------------
    # Feature Importance
    # ------------------------------------------------------------------

    def get_feature_importance(self) -> Dict[str, Dict[str, float]]:
        """获取特征重要性。"""
        if self.model is None:
            raise RuntimeError("请先调用 train() 训练模型")

        gain = dict(zip(
            self.feature_names,
            self.model.booster_.feature_importance(importance_type="gain"),
        ))
        split = dict(zip(
            self.feature_names,
            self.model.booster_.feature_importance(importance_type="split"),
        ))
        return {
            "gain": dict(sorted(gain.items(), key=lambda x: x[1], reverse=True)),
            "split": dict(sorted(split.items(), key=lambda x: x[1], reverse=True)),
        }

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """对指定交易日（默认最新）的全市场股票打分。"""
        if self.model is None:
            raise RuntimeError("请先调用 train() 训练模型")

        db = _get_db()
        from src.storage import FactorScoreSnapshot

        if target_date is None:
            with db.get_session() as session:
                row = session.query(FactorScoreSnapshot.trade_date).filter(
                    FactorScoreSnapshot.mode == self.mode,
                ).order_by(FactorScoreSnapshot.trade_date.desc()).first()
                if not row:
                    raise ValueError("factor_score_snapshots 中没有数据")
                target_date = row[0]

        self._latest_date = target_date

        with db.get_session() as session:
            rows = session.query(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.mode == self.mode,
                FactorScoreSnapshot.trade_date == target_date,
            ).all()

        if not rows:
            raise ValueError(f"没有 {target_date} 的因子快照数据")

        records = [
            {"ts_code": r.ts_code, "factor_name": r.factor_name, "score": r.score}
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

        pivot = pivot.sort_values("lgb_score", ascending=False)
        pivot["stock_code"] = (
            pivot["ts_code"].str.replace(".SH", "").str.replace(".SZ", "")
        )
        self._X_latest = pivot
        return pivot

    def get_latest_predictions(self) -> List[Dict]:
        """获取最新预测结果列表。"""
        if self._X_latest is None:
            raise RuntimeError("请先调用 predict() 生成预测")

        top = self._X_latest.head(50)
        return [
            {
                "rank": i + 1,
                "ts_code": row["ts_code"],
                "stock_code": row["stock_code"],
                "lgb_score": round(float(row["lgb_score_norm"]), 2),
                "raw_score": round(float(row["lgb_score"]), 2),
            }
            for i, (_, row) in enumerate(top.iterrows())
        ]

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

        rets = []
        for c in bare:
            bp = buy_map.get(c)
            sp = sell_map.get(c)
            if bp and sp and bp > 0:
                rets.append((sp - bp) / bp)

        return float(np.mean(rets)) if rets else None

    def _calc_benchmark_return(self, db, td: str, sell_date: str) -> Optional[float]:
        """计算等权全市场基准收益。"""
        from src.storage import StockDaily

        with db.get_session() as session:
            rows = session.query(StockDaily).filter(
                StockDaily.date.in_([pd.Timestamp(td), pd.Timestamp(sell_date)]),
            ).all()

        buy_map: Dict[str, float] = {}
        sell_map: Dict[str, float] = {}
        for r in rows:
            d = r.date.strftime("%Y%m%d")
            if d == td:
                buy_map[str(r.code)] = float(r.close)
            elif d == sell_date:
                sell_map[str(r.code)] = float(r.close)

        common = set(buy_map) & set(sell_map)
        if not common:
            return None
        rets = [
            (sell_map[c] - buy_map[c]) / buy_map[c]
            for c in common if buy_map[c] > 0
        ]
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

    def save(self, name: Optional[str] = None) -> str:
        """保存模型到 data/lgb_models/ 目录。"""
        if self.model is None:
            raise RuntimeError("没有已训练的模型可保存")

        os.makedirs(MODEL_DIR, exist_ok=True)
        if name is None:
            name = (
                f"lgb_{self.mode}_fwd{self.forward_days}d_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

        model_path = os.path.join(MODEL_DIR, f"{name}.joblib")
        meta = {
            "mode": self.mode,
            "forward_days": self.forward_days,
            "feature_names": self.feature_names,
            "training_metrics": self._training_metrics,
            "saved_at": datetime.now().isoformat(),
        }
        joblib.dump({"model": self.model, "meta": meta}, model_path)
        return model_path

    @classmethod
    def load(cls, model_path: str) -> "LGBTrainer":
        """从文件加载模型。"""
        data = joblib.load(model_path)
        meta = data["meta"]
        trainer = cls(mode=meta["mode"], forward_days=meta["forward_days"])
        trainer.model = data["model"]
        trainer.feature_names = meta["feature_names"]
        trainer._training_metrics = meta.get("training_metrics", {})
        return trainer

    @staticmethod
    def list_models() -> List[Dict]:
        """列出 data/lgb_models/ 下所有已保存的模型。"""
        if not os.path.isdir(MODEL_DIR):
            return []
        models = []
        for fname in sorted(os.listdir(MODEL_DIR), reverse=True):
            if fname.endswith(".joblib"):
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
