# -*- coding: utf-8 -*-
"""因子子集最优搜索。

三阶段搜索:
  Phase 1: 全因子基线 + LGB gain 重要性排序
  Phase 2: 贪心前向选择（按重要性顺序尝试添加因子）
  Phase 3: Optuna TPE 精调（可选）

关键优化: 首次 prepare_data() 后缓存完整特征矩阵，后续评估只做列选择。
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from scipy.stats import spearmanr
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error

from src.discovery.ml.lgb_trainer import LGBTrainer

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
_REPORTS_DIR = os.path.join(_PROJECT_ROOT, "reports_lgb", "factor_subset")


class FactorSubsetSearcher:
    """因子子集最优搜索器。"""

    def __init__(
        self,
        mode: str = "postmarket",
        label_mode: str = "fixed",
        forward_days: int = 5,
        window_days: int = 20,
        exec_mode: str = "open",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        n_estimators: int = 200,
        num_leaves: int = 31,
        learning_rate: float = 0.05,
        cv_folds: int = 5,
        tpe_trials: int = 80,
        top_n: int = 5,
        progress_callback: Optional[Callable[[str], None]] = None,
    ):
        self.mode = mode
        self.label_mode = label_mode
        self.forward_days = forward_days
        self.window_days = window_days
        self.exec_mode = exec_mode
        self.start_date = start_date
        self.end_date = end_date
        self.n_estimators = n_estimators
        self.num_leaves = num_leaves
        self.learning_rate = learning_rate
        self.cv_folds = cv_folds
        self.tpe_trials = tpe_trials
        self.top_n = top_n
        self.cb = progress_callback

        self._X_full: Optional[pd.DataFrame] = None
        self._y_full: Optional[pd.Series] = None
        self._dates: Optional[pd.Index] = None  # trade_date per row
        self._all_factors: List[str] = []
        self._importance_ranking: List[str] = []
        self._cached_gain: Dict[str, float] = {}  # batch_run 预计算的 gain

    def _log(self, msg: str):
        if self.cb:
            self.cb(msg)
        else:
            print(f"[FactorSubset] {msg}")

    # ------------------------------------------------------------------
    # Phase 0: 加载完整数据
    # ------------------------------------------------------------------

    def _load_full_data(self):
        """调用 prepare_data() 一次，缓存完整特征矩阵。

        临时清除 LGB_DISABLE_FACTOR 以加载全部因子用于搜索。
        """
        self._log("加载完整因子数据（忽略 LGB_DISABLE_FACTOR，使用全部因子）...")
        kw: Dict[str, Any] = {
            "mode": self.mode,
            "label_mode": self.label_mode,
            "forward_days": self.forward_days,
            "exec_mode": self.exec_mode,
        }
        if self.label_mode == "peak_speed":
            kw["window_days"] = self.window_days

        # 临时清除 LGB_DISABLE_FACTOR，确保搜索覆盖全部因子
        saved_disable = os.environ.pop("LGB_DISABLE_FACTOR", None)
        try:
            trainer = LGBTrainer(**kw)
            X, y = trainer.prepare_data(
                start_date=self.start_date,
                end_date=self.end_date,
            )
        finally:
            if saved_disable is not None:
                os.environ["LGB_DISABLE_FACTOR"] = saved_disable

        self._X_full = X[trainer.feature_names].fillna(0)
        self._y_full = y
        self._dates = X.index.get_level_values("trade_date")
        self._all_factors = list(trainer.feature_names)
        self._log(f"数据加载完成: {len(self._X_full):,} 行, "
                  f"{len(self._all_factors)} 因子（全部因子）")

    # ------------------------------------------------------------------
    # 评估单个因子子集
    # ------------------------------------------------------------------

    def _eval_subset(self, factor_list: List[str]) -> Dict[str, Any]:
        """从缓存数据切片 + LGB CV，返回以日均收益为核心的 metrics。"""
        X = self._X_full[factor_list].values
        y = self._y_full.values
        dates = self._dates.values

        model = LGBMRegressor(
            n_estimators=self.n_estimators,
            num_leaves=self.num_leaves,
            learning_rate=self.learning_rate,
            verbose=-1,
            random_state=42,
        )

        tscv = TimeSeriesSplit(n_splits=self.cv_folds)
        rank_ics: List[float] = []
        cv_scores: List[float] = []
        all_daily_returns: List[float] = []

        for train_idx, val_idx in tscv.split(X):
            X_tr, X_val = X[train_idx], X[val_idx]
            y_tr, y_val = y[train_idx], y[val_idx]
            d_val = dates[val_idx]
            model.fit(X_tr, y_tr)
            pred = model.predict(X_val)
            cv_scores.append(float(np.sqrt(mean_squared_error(y_val, pred))))

            if len(pred) > 5:
                ic, _ = spearmanr(pred, y_val)
                if np.isfinite(ic):
                    rank_ics.append(float(ic))

            # 模拟每日买入 top_n 股票，记录每日收益
            val_df = pd.DataFrame({
                "date": d_val,
                "pred": pred,
                "ret": y_val,
            })
            for _, grp in val_df.groupby("date"):
                if len(grp) >= self.top_n:
                    top = grp.nlargest(self.top_n, "pred")
                    all_daily_returns.append(float(top["ret"].mean()))

        ic_mean = float(np.mean(rank_ics)) if rank_ics else -999.0
        ic_std = float(np.std(rank_ics)) if rank_ics else 999.0

        # 日均收益（不受验证期长度影响）
        daily_ret_mean = float(np.mean(all_daily_returns)) if all_daily_returns else -999.0

        return {
            "daily_return_mean": round(daily_ret_mean, 6),
            "rank_ic_mean": round(ic_mean, 6),
            "rank_ic_std": round(ic_std, 6),
            "icir": round(ic_mean / ic_std, 4) if ic_std > 1e-9 else 0.0,
            "cv_rmse_mean": round(float(np.mean(cv_scores)), 6),
            "n_factors": len(factor_list),
            "n_trading_days": len(all_daily_returns),
            "factors": list(factor_list),
        }

    # ------------------------------------------------------------------
    # Phase 1: 基线 + 重要性排序
    # ------------------------------------------------------------------

    def _rank_by_importance(self) -> Dict[str, Any]:
        """训练全因子模型，提取 gain importance 排序。"""
        self._log("Phase 1: 训练全因子基线模型...")
        baseline = self._eval_subset(self._all_factors)
        self._log(f"  基线: 日均收益={baseline['daily_return_mean']:.4f}, "
                  f"rank_ic={baseline['rank_ic_mean']:.4f}, "
                  f"icir={baseline['icir']:.4f}")

        # 如果已有 importance ranking（batch_run 预计算），跳过模型训练
        if self._importance_ranking:
            self._log("  复用已有因子重要性排名（跳过模型训练）")
            return {
                "baseline": baseline,
                "importance_ranking": self._importance_ranking,
                "gain": {f: round(self._cached_gain.get(f, 0), 1) for f in self._all_factors},
            }

        # 训练完整模型获取 gain importance
        model = LGBMRegressor(
            n_estimators=self.n_estimators,
            num_leaves=self.num_leaves,
            learning_rate=self.learning_rate,
            verbose=-1,
            random_state=42,
        )
        X = self._X_full[self._all_factors].values
        y = self._y_full.values
        model.fit(X, y)
        gain = dict(zip(
            self._all_factors,
            model.booster_.feature_importance(importance_type="gain"),
        ))
        self._importance_ranking = sorted(
            self._all_factors, key=lambda f: gain.get(f, 0), reverse=True
        )
        self._log("  因子重要性排名:")
        for i, f in enumerate(self._importance_ranking, 1):
            self._log(f"    {i:2d}. {f}: {gain.get(f, 0):.1f}")

        return {
            "baseline": baseline,
            "importance_ranking": self._importance_ranking,
            "gain": {f: round(float(gain.get(f, 0)), 1) for f in self._all_factors},
        }

    # ------------------------------------------------------------------
    # Phase 2: 贪心前向选择
    # ------------------------------------------------------------------

    def _greedy_forward(self) -> Dict[str, Any]:
        """按重要性顺序贪心前向选择因子。"""
        self._log("Phase 2: 贪心前向选择...")
        remaining = list(self._importance_ranking)
        selected: List[str] = []
        trace: List[Dict[str, Any]] = []
        best_daily_ret = -999.0
        no_improve_count = 0

        for round_num in range(1, len(self._all_factors) + 1):
            best_factor = None
            best_round_daily_ret = -999.0
            best_round_result = None

            # 按重要性顺序尝试每个剩余因子
            for f in remaining:
                trial = selected + [f]
                result = self._eval_subset(trial)
                ret = result["daily_return_mean"]
                if ret > best_round_daily_ret:
                    best_round_daily_ret = ret
                    best_factor = f
                    best_round_result = result

            if best_factor is None:
                break

            selected.append(best_factor)
            remaining.remove(best_factor)

            improved = best_round_daily_ret > best_daily_ret + 1e-8
            if improved:
                best_daily_ret = best_round_daily_ret
                no_improve_count = 0
            else:
                no_improve_count += 1

            trace.append({
                "round": round_num,
                "added": best_factor,
                "daily_return_mean": best_round_result["daily_return_mean"],
                "rank_ic_mean": best_round_result["rank_ic_mean"],
                "icir": best_round_result["icir"],
                "n_factors": len(selected),
                "improved": improved,
            })
            mark = "+" if improved else "-"
            self._log(f"  Round {round_num:2d}: +{best_factor:20s} "
                      f"日均收益={best_round_result['daily_return_mean']:.4f} "
                      f"IC={best_round_result['rank_ic_mean']:.4f} [{mark}]")

            if no_improve_count >= 3:
                self._log(f"  连续 {no_improve_count} 轮无提升，提前停止")
                break

        # 找到日均收益最高的子集
        best_trace = max(trace, key=lambda t: t["daily_return_mean"])
        best_subset = []
        for t in trace:
            best_subset.append(t["added"])
            if t["round"] == best_trace["round"]:
                break

        return {
            "selected": selected,
            "best_subset": best_subset,
            "best_daily_return": best_trace["daily_return_mean"],
            "trace": trace,
        }

    # ------------------------------------------------------------------
    # Phase 3: Optuna TPE 精调
    # ------------------------------------------------------------------

    def _optuna_tpe_search(self, warm_start: List[str]) -> Dict[str, Any]:
        """用 Optuna TPE 搜索因子子集。"""
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            self._log("Phase 3: optuna 未安装，跳过 TPE 精调")
            return {"skipped": True, "reason": "optuna not installed"}

        self._log(f"Phase 3: Optuna TPE 精调 ({self.tpe_trials} trials)...")
        warm_set = set(warm_start)
        n_all = len(self._all_factors)

        def objective(trial: optuna.Trial) -> float:
            mask = []
            for f in self._all_factors:
                if f in warm_set:
                    # warm start 因子: 高概率选中
                    mask.append(trial.suggest_categorical(f"include_{f}", [1, 0]))
                else:
                    mask.append(trial.suggest_categorical(f"include_{f}", [0, 1]))
            selected = [f for f, m in zip(self._all_factors, mask) if m]
            if not selected:
                return -999.0
            result = self._eval_subset(selected)
            return result["daily_return_mean"]

        study = optuna.create_study(direction="maximize")
        # warm start trial
        study.enqueue_trial({
            f"include_{f}": 1 for f in warm_start
        })
        study.optimize(objective, n_trials=self.tpe_trials, show_progress_bar=False)

        best = study.best_trial
        best_factors = [
            f for f in self._all_factors
            if best.params.get(f"include_{f}") == 1
        ]
        best_result = self._eval_subset(best_factors)

        self._log(f"  TPE 最优: {len(best_factors)} 因子, "
                  f"日均收益={best_result['daily_return_mean']:.4f}, "
                  f"IC={best_result['rank_ic_mean']:.4f}")

        return {
            "best_subset": best_factors,
            "best_result": best_result,
            "n_trials": len(study.trials),
        }

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------

    def load_data(self):
        """加载完整因子数据（公开方法，支持数据复用）。"""
        self._load_full_data()

    def run_search(self, save_report: bool = True) -> Dict[str, Any]:
        """执行三阶段搜索（需先调用 load_data）。"""
        if self._X_full is None:
            raise RuntimeError("请先调用 load_data() 加载数据")

        t0 = time.time()

        # Phase 1: 基线 + 重要性
        phase1 = self._rank_by_importance()

        # Phase 2: 贪心前向选择
        phase2 = self._greedy_forward()

        # Phase 3: TPE 精调
        phase3 = self._optuna_tpe_search(phase2["best_subset"])

        # 确定最终最优子集
        if not phase3.get("skipped") and phase3.get("best_result", {}).get("daily_return_mean", -999) > phase2["best_daily_return"]:
            final_subset = phase3["best_subset"]
            final_metrics = phase3["best_result"]
            self._log(f"最终结果: TPE 精调子集 ({len(final_subset)} 因子)")
        else:
            final_subset = phase2["best_subset"]
            final_metrics = self._eval_subset(final_subset)
            self._log(f"最终结果: 贪心前向子集 ({len(final_subset)} 因子)")

        elapsed = time.time() - t0

        # 与基线对比
        baseline_daily_ret = phase1["baseline"]["daily_return_mean"]
        final_daily_ret = final_metrics["daily_return_mean"]
        delta_daily_ret = final_daily_ret - baseline_daily_ret

        self._log(f"\n{'='*50}")
        self._log(f"搜索完成 ({elapsed:.0f}s)")
        self._log(f"基线 ({len(self._all_factors)}因子): 日均收益={baseline_daily_ret:.4f}")
        self._log(f"最优 ({len(final_subset)}因子): 日均收益={final_daily_ret:.4f} "
                  f"(Δ={delta_daily_ret:+.4f})")
        self._log(f"排除因子: {sorted(set(self._all_factors) - set(final_subset))}")

        result = {
            "all_factors": self._all_factors,
            "phase1": phase1,
            "phase2": phase2,
            "phase3": phase3,
            "final_subset": final_subset,
            "final_metrics": final_metrics,
            "excluded_factors": sorted(set(self._all_factors) - set(final_subset)),
            "elapsed_seconds": round(elapsed, 1),
            "label_mode": self.label_mode,
            "forward_days": self.forward_days,
            "window_days": self.window_days,
            "exec_mode": self.exec_mode,
            "mode": self.mode,
            "top_n": self.top_n,
            "timestamp": datetime.now().isoformat(),
        }

        # 保存报告
        if save_report:
            self._save_report(result)
        return result

    def run(self) -> Dict[str, Any]:
        """加载数据 + 执行三阶段搜索。"""
        self.load_data()
        return self.run_search()

    # ------------------------------------------------------------------
    # 批量搜索
    # ------------------------------------------------------------------

    def batch_run(
        self,
        configs: List[Dict[str, Any]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """批量搜索多个参数组合，按 (exec_mode, label_mode, forward_days) 分组共享数据。

        configs: [{"label_mode": "fixed", "forward_days": 5, "top_n": 1, "exec_mode": "open"}, ...]
        """
        from collections import defaultdict

        # 按 (exec_mode, label_mode, forward_days) 分组
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for cfg in configs:
            key = (cfg.get("exec_mode", self.exec_mode), cfg["label_mode"], cfg.get("forward_days", 5))
            groups[key].append(cfg)

        all_results: List[Dict[str, Any]] = []
        total = len(configs)
        done = 0

        for (exec_mode, label_mode, forward_days), group_configs in groups.items():
            window_days = group_configs[0].get("window_days", 20)
            self._log(f"\n{'='*60}")
            self._log(f"加载数据: exec={exec_mode}, label_mode={label_mode}, forward_days={forward_days}")
            self._log(f"该组有 {len(group_configs)} 个 top_n 配置: "
                      f"{[c['top_n'] for c in group_configs]}")

            # 创建 searcher 并加载数据
            searcher = FactorSubsetSearcher(
                mode=self.mode,
                label_mode=label_mode,
                forward_days=forward_days,
                window_days=window_days,
                exec_mode=exec_mode,
                start_date=start_date,
                end_date=end_date,
                n_estimators=self.n_estimators,
                num_leaves=self.num_leaves,
                learning_rate=self.learning_rate,
                cv_folds=self.cv_folds,
                tpe_trials=self.tpe_trials,
                progress_callback=self.cb,
            )
            searcher.load_data()

            # 预计算 importance ranking（与 top_n 无关，只需算一次）
            self._log(f"预计算因子重要性排名...")
            model = LGBMRegressor(
                n_estimators=searcher.n_estimators,
                num_leaves=searcher.num_leaves,
                learning_rate=searcher.learning_rate,
                verbose=-1,
                random_state=42,
            )
            X_all = searcher._X_full[searcher._all_factors].values
            y_all = searcher._y_full.values
            model.fit(X_all, y_all)
            gain = dict(zip(
                searcher._all_factors,
                model.booster_.feature_importance(importance_type="gain"),
            ))
            searcher._importance_ranking = sorted(
                searcher._all_factors, key=lambda f: gain.get(f, 0), reverse=True
            )
            searcher._cached_gain = gain

            # 对每个 top_n 执行搜索，组内只保留最优
            group_results = []
            for cfg in group_configs:
                done += 1
                top_n = cfg["top_n"]
                self._log(f"\n[{done}/{total}] 搜索: "
                          f"label={label_mode}, fwd={forward_days}, top_n={top_n}")
                searcher.top_n = top_n
                result = searcher.run_search(save_report=False)
                result["_config"] = cfg
                group_results.append(result)

            # 组内按日均收益取最优，只保存一份报告
            best_in_group = max(
                group_results,
                key=lambda r: r.get("final_metrics", {}).get("daily_return_mean", -999),
            )
            searcher.top_n = best_in_group["top_n"]
            searcher._save_report(best_in_group)
            self._log(f"  组内最优: top_n={best_in_group['top_n']}, "
                      f"日均收益={best_in_group['final_metrics']['daily_return_mean']:.4f}")
            all_results.append(best_in_group)

        # 生成汇总报告
        batch_result = self._generate_batch_report(all_results)
        return batch_result

    def _generate_batch_report(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """生成批量搜索汇总报告。"""
        # 按日均收益排序
        ranked = sorted(
            results,
            key=lambda r: r.get("final_metrics", {}).get("daily_return_mean", -999),
            reverse=True,
        )

        summary_items = []
        for i, r in enumerate(ranked, 1):
            fm = r.get("final_metrics", {})
            bl = r.get("phase1", {}).get("baseline", {})
            cfg = r.get("_config", {})
            summary_items.append({
                "rank": i,
                "exec_mode": r.get("exec_mode", ""),
                "label_mode": r.get("label_mode", ""),
                "forward_days": r.get("forward_days", 0),
                "top_n": r.get("top_n", 0),
                "daily_return_mean": fm.get("daily_return_mean", 0),
                "baseline_daily_return": bl.get("daily_return_mean", 0),
                "delta_daily_return": fm.get("daily_return_mean", 0) - bl.get("daily_return_mean", 0),
                "rank_ic_mean": fm.get("rank_ic_mean", 0),
                "icir": fm.get("icir", 0),
                "n_factors": fm.get("n_factors", 0),
                "final_subset": r.get("final_subset", []),
                "excluded_factors": r.get("excluded_factors", []),
                "elapsed_seconds": r.get("elapsed_seconds", 0),
            })

        batch_result = {
            "timestamp": datetime.now().isoformat(),
            "total_configs": len(results),
            "summary": summary_items,
            "best": summary_items[0] if summary_items else None,
        }

        # 保存批量报告
        self._save_batch_report(batch_result)
        return batch_result

    def _save_batch_report(self, batch_result: Dict[str, Any]):
        """保存批量搜索汇总报告。"""
        batch_dir = os.path.join(_REPORTS_DIR, "batch")
        os.makedirs(batch_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        # JSON
        json_path = os.path.join(batch_dir, f"batch_summary_{ts}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(batch_result, f, ensure_ascii=False, indent=2, default=str)

        # Markdown
        md_path = os.path.join(batch_dir, f"batch_summary_{ts}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(self._format_batch_markdown(batch_result))

        self._log(f"\n批量报告已保存: {md_path}")

    def _format_batch_markdown(self, batch_result: Dict[str, Any]) -> str:
        """生成批量搜索 Markdown 报告。"""
        lines = []
        lines.append("# 因子子集批量搜索报告")
        lines.append(f"\n搜索时间: {batch_result['timestamp']}")
        lines.append(f"方案数: {batch_result['total_configs']}")
        lines.append(f"数据范围: 2025-01-01 ~ 2026-05-25\n")

        lines.append("## 汇总排名")
        lines.append("| 排名 | 执行 | 模式 | 持有 | Top | 日均收益 | 基线收益 | 收益变化 | IC | ICIR | 因子 |")
        lines.append("|------|------|------|------|-----|---------|---------|---------|------|------|------|")
        for item in batch_result["summary"]:
            label = "fixed" if item["label_mode"] == "fixed" else "peak"
            fwd = f"{item['forward_days']}d" if item["label_mode"] == "fixed" else "-"
            lines.append(
                f"| {item['rank']} | {item.get('exec_mode', '')} | {label} | {fwd} | {item['top_n']} | "
                f"{item['daily_return_mean']:.4f} | {item['baseline_daily_return']:.4f} | "
                f"{item['delta_daily_return']:+.4f} | "
                f"{item['rank_ic_mean']:.4f} | {item['icir']:.4f} | "
                f"{item['n_factors']} |"
            )

        best = batch_result.get("best")
        if best:
            lines.append(f"\n## 最优方案")
            lines.append(f"- 执行模式: {best.get('exec_mode', '')}")
            lines.append(f"- 标签模式: {best['label_mode']}")
            lines.append(f"- 持有期: {best['forward_days']}d")
            lines.append(f"- Top N: {best['top_n']}")
            lines.append(f"- 日均收益: {best['daily_return_mean']:.4f}")
            lines.append(f"- Rank IC: {best['rank_ic_mean']:.4f}")
            lines.append(f"- ICIR: {best['icir']:.4f}")
            lines.append(f"- 因子数: {best['n_factors']}")
            lines.append(f"\n### 最优因子")
            for f in best["final_subset"]:
                lines.append(f"- {f}")
            lines.append(f"\n### 排除因子")
            for f in best["excluded_factors"]:
                lines.append(f"- {f}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 报告输出
    # ------------------------------------------------------------------

    def _save_report(self, result: Dict[str, Any]):
        """保存 Markdown + JSON 报告，同时删除同类型旧报告。"""
        import glob as _glob
        os.makedirs(_REPORTS_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        mode_tag = (f"{self.label_mode}_{self.forward_days}d"
                    if self.label_mode == "fixed"
                    else f"peak{self.window_days}d")
        label_tag = f"{self.exec_mode}_{mode_tag}"

        # 删除同类型旧报告
        prefix = os.path.join(_REPORTS_DIR, f"subset_{label_tag}_")
        for old in _glob.glob(f"{prefix}*"):
            os.remove(old)

        # JSON
        json_path = os.path.join(_REPORTS_DIR, f"subset_{label_tag}_{ts}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

        # Markdown
        md_path = os.path.join(_REPORTS_DIR, f"subset_{label_tag}_{ts}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(self._format_markdown(result))

        self._log(f"报告已保存: {md_path}")

    def _format_markdown(self, result: Dict[str, Any]) -> str:
        """生成 Markdown 报告。"""
        lines = []
        label_tag = (f"{self.label_mode} fwd{self.forward_days}d"
                     if self.label_mode == "fixed"
                     else f"peak_speed {self.window_days}d")
        lines.append(f"# 因子子集搜索报告 ({label_tag})")
        lines.append(f"\n搜索时间: {result['timestamp']}")
        lines.append(f"耗时: {result['elapsed_seconds']}s\n")

        # 基线
        b = result["phase1"]["baseline"]
        lines.append("## 基线 (全部因子)")
        lines.append(f"- 因子数: {b['n_factors']}")
        lines.append(f"- 日均收益: {b['daily_return_mean']:.4f}")
        lines.append(f"- Rank IC: {b['rank_ic_mean']:.4f} (±{b['rank_ic_std']:.4f})")
        lines.append(f"- ICIR: {b['icir']:.4f}")
        lines.append(f"- CV RMSE: {b['cv_rmse_mean']:.4f}")

        # 重要性排名
        lines.append("\n## 因子重要性排名")
        lines.append("| 排名 | 因子 | Gain |")
        lines.append("|------|------|------|")
        gain = result["phase1"]["gain"]
        for i, f in enumerate(result["phase1"]["importance_ranking"], 1):
            lines.append(f"| {i} | {f} | {gain.get(f, 0):.1f} |")

        # 前向选择 trace
        lines.append("\n## 贪心前向选择")
        lines.append("| 轮次 | 添加因子 | 日均收益 | Rank IC | 改善 |")
        lines.append("|------|----------|----------|---------|------|")
        for t in result["phase2"]["trace"]:
            mark = "Y" if t["improved"] else "N"
            lines.append(f"| {t['round']} | {t['added']} | "
                        f"{t['daily_return_mean']:.4f} | {t['rank_ic_mean']:.4f} | {mark} |")

        # TPE 精调
        p3 = result["phase3"]
        if not p3.get("skipped"):
            lines.append(f"\n## Optuna TPE 精调 ({p3.get('n_trials', 0)} trials)")
            br = p3.get("best_result", {})
            lines.append(f"- 最优因子数: {br.get('n_factors', '?')}")
            lines.append(f"- 日均收益: {br.get('daily_return_mean', 0):.4f}")
            lines.append(f"- Rank IC: {br.get('rank_ic_mean', 0):.4f}")

        # 最终结果
        fm = result["final_metrics"]
        lines.append("\n## 最终结果")
        lines.append(f"- 最优因子数: {len(result['final_subset'])}")
        lines.append(f"- 日均收益: {fm['daily_return_mean']:.4f}")
        lines.append(f"- Rank IC: {fm['rank_ic_mean']:.4f}")
        lines.append(f"- ICIR: {fm['icir']:.4f}")
        lines.append(f"- CV RMSE: {fm['cv_rmse_mean']:.4f}")
        delta = fm["daily_return_mean"] - b["daily_return_mean"]
        lines.append(f"- 相比基线收益变化: {delta:+.4f}")

        lines.append(f"\n### 最优因子 ({len(result['final_subset'])} 个)")
        for f in result["final_subset"]:
            lines.append(f"- {f}")

        lines.append(f"\n### 排除因子 ({len(result['excluded_factors'])} 个)")
        for f in result["excluded_factors"]:
            lines.append(f"- {f}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 应用结果
    # ------------------------------------------------------------------

    def apply_best_subset(self, result: Dict[str, Any]) -> str:
        """将排除因子写入 .env 的 LGB_DISABLE_FACTOR（与已有值合并）。"""
        new_excluded = set(result.get("excluded_factors", []))
        if not new_excluded:
            return ""

        # 读取 .env 中已有的 LGB_DISABLE_FACTOR
        existing_excluded: set = set()
        env_path = os.path.join(_PROJECT_ROOT, ".env")
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped.startswith("LGB_DISABLE_FACTOR=") or stripped.startswith("# LGB_DISABLE_FACTOR="):
                        val = stripped.split("=", 1)[1].split("#")[0].strip()
                        if val:
                            existing_excluded = {f.strip() for f in val.split(",") if f.strip()}
                        break

        # 合并：已有排除 + 新排除
        merged = sorted(existing_excluded | new_excluded)
        env_value = ",".join(merged)

        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            new_lines = []
            found = False
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("LGB_DISABLE_FACTOR=") or stripped.startswith("# LGB_DISABLE_FACTOR="):
                    new_lines.append(f"LGB_DISABLE_FACTOR={env_value}\n")
                    found = True
                else:
                    new_lines.append(line)
            if not found:
                new_lines.append(f"\nLGB_DISABLE_FACTOR={env_value}\n")
            with open(env_path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        else:
            with open(env_path, "w", encoding="utf-8") as f:
                f.write(f"LGB_DISABLE_FACTOR={env_value}\n")

        self._log(f"已写入 .env: LGB_DISABLE_FACTOR={env_value}")
        return env_value
