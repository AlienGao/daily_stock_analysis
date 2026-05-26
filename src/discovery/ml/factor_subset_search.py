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
_REPORTS_DIR = os.path.join(_PROJECT_ROOT, "lgb_reports", "factor_subset")


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
        self.cb = progress_callback

        self._X_full: Optional[pd.DataFrame] = None
        self._y_full: Optional[pd.Series] = None
        self._all_factors: List[str] = []
        self._importance_ranking: List[str] = []

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
        self._all_factors = list(trainer.feature_names)
        self._log(f"数据加载完成: {len(self._X_full):,} 行, "
                  f"{len(self._all_factors)} 因子（全部因子）")

    # ------------------------------------------------------------------
    # 评估单个因子子集
    # ------------------------------------------------------------------

    def _eval_subset(self, factor_list: List[str]) -> Dict[str, Any]:
        """从缓存数据切片 + LGB CV，返回 metrics。"""
        X = self._X_full[factor_list].values
        y = self._y_full.values

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

        for train_idx, val_idx in tscv.split(X):
            X_tr, X_val = X[train_idx], X[val_idx]
            y_tr, y_val = y[train_idx], y[val_idx]
            model.fit(X_tr, y_tr)
            pred = model.predict(X_val)
            cv_scores.append(float(np.sqrt(mean_squared_error(y_val, pred))))
            if len(pred) > 5:
                ic, _ = spearmanr(pred, y_val)
                if np.isfinite(ic):
                    rank_ics.append(float(ic))

        ic_mean = float(np.mean(rank_ics)) if rank_ics else -999.0
        ic_std = float(np.std(rank_ics)) if rank_ics else 999.0
        return {
            "rank_ic_mean": round(ic_mean, 6),
            "rank_ic_std": round(ic_std, 6),
            "icir": round(ic_mean / ic_std, 4) if ic_std > 1e-9 else 0.0,
            "cv_rmse_mean": round(float(np.mean(cv_scores)), 6),
            "n_factors": len(factor_list),
            "factors": list(factor_list),
        }

    # ------------------------------------------------------------------
    # Phase 1: 基线 + 重要性排序
    # ------------------------------------------------------------------

    def _rank_by_importance(self) -> Dict[str, Any]:
        """训练全因子模型，提取 gain importance 排序。"""
        self._log("Phase 1: 训练全因子基线模型...")
        baseline = self._eval_subset(self._all_factors)
        self._log(f"  基线: rank_ic={baseline['rank_ic_mean']:.4f}, "
                  f"icir={baseline['icir']:.4f}, "
                  f"rmse={baseline['cv_rmse_mean']:.4f}")

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
        best_ic = -999.0
        no_improve_count = 0

        for round_num in range(1, len(self._all_factors) + 1):
            best_factor = None
            best_round_ic = -999.0
            best_round_result = None

            # 按重要性顺序尝试每个剩余因子
            for f in remaining:
                trial = selected + [f]
                result = self._eval_subset(trial)
                ic = result["rank_ic_mean"]
                if ic > best_round_ic:
                    best_round_ic = ic
                    best_factor = f
                    best_round_result = result

            if best_factor is None:
                break

            selected.append(best_factor)
            remaining.remove(best_factor)

            improved = best_round_ic > best_ic + 1e-6
            if improved:
                best_ic = best_round_ic
                no_improve_count = 0
            else:
                no_improve_count += 1

            trace.append({
                "round": round_num,
                "added": best_factor,
                "rank_ic_mean": best_round_result["rank_ic_mean"],
                "icir": best_round_result["icir"],
                "cv_rmse_mean": best_round_result["cv_rmse_mean"],
                "n_factors": len(selected),
                "improved": improved,
            })
            mark = "+" if improved else "-"
            self._log(f"  Round {round_num:2d}: +{best_factor:20s} "
                      f"IC={best_round_result['rank_ic_mean']:.4f} "
                      f"ICIR={best_round_result['icir']:.4f} [{mark}]")

            if no_improve_count >= 3:
                self._log(f"  连续 {no_improve_count} 轮无提升，提前停止")
                break

        # 找到 IC 最高的子集
        best_trace = max(trace, key=lambda t: t["rank_ic_mean"])
        best_subset = []
        for t in trace:
            best_subset.append(t["added"])
            if t["round"] == best_trace["round"]:
                break

        return {
            "selected": selected,
            "best_subset": best_subset,
            "best_ic": best_trace["rank_ic_mean"],
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
            # 简洁性惩罚
            penalty = 0.002 * len(selected)
            return result["rank_ic_mean"] - penalty

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
                  f"IC={best_result['rank_ic_mean']:.4f}, "
                  f"ICIR={best_result['icir']:.4f}")

        return {
            "best_subset": best_factors,
            "best_result": best_result,
            "n_trials": len(study.trials),
        }

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """执行三阶段搜索。"""
        t0 = time.time()

        # Phase 0: 加载数据
        self._load_full_data()

        # Phase 1: 基线 + 重要性
        phase1 = self._rank_by_importance()

        # Phase 2: 贪心前向选择
        phase2 = self._greedy_forward()

        # Phase 3: TPE 精调
        phase3 = self._optuna_tpe_search(phase2["best_subset"])

        # 确定最终最优子集
        if not phase3.get("skipped") and phase3.get("best_result", {}).get("rank_ic_mean", -999) > phase2["best_ic"]:
            final_subset = phase3["best_subset"]
            final_metrics = phase3["best_result"]
            self._log(f"最终结果: TPE 精调子集 ({len(final_subset)} 因子)")
        else:
            final_subset = phase2["best_subset"]
            final_metrics = self._eval_subset(final_subset)
            self._log(f"最终结果: 贪心前向子集 ({len(final_subset)} 因子)")

        elapsed = time.time() - t0

        # 与基线对比
        baseline_ic = phase1["baseline"]["rank_ic_mean"]
        final_ic = final_metrics["rank_ic_mean"]
        delta_ic = final_ic - baseline_ic

        self._log(f"\n{'='*50}")
        self._log(f"搜索完成 ({elapsed:.0f}s)")
        self._log(f"基线 (18因子): IC={baseline_ic:.4f}")
        self._log(f"最优 ({len(final_subset)}因子): IC={final_ic:.4f} "
                  f"(Δ={delta_ic:+.4f})")
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
            "timestamp": datetime.now().isoformat(),
        }

        # 保存报告
        self._save_report(result)
        return result

    # ------------------------------------------------------------------
    # 报告输出
    # ------------------------------------------------------------------

    def _save_report(self, result: Dict[str, Any]):
        """保存 Markdown + JSON 报告。"""
        os.makedirs(_REPORTS_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        label_tag = (f"{self.label_mode}_{self.forward_days}d"
                     if self.label_mode == "fixed"
                     else f"peak{self.window_days}d")

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
        lines.append("| 轮次 | 添加因子 | Rank IC | ICIR | 改善 |")
        lines.append("|------|----------|---------|------|------|")
        for t in result["phase2"]["trace"]:
            mark = "Y" if t["improved"] else "N"
            lines.append(f"| {t['round']} | {t['added']} | "
                        f"{t['rank_ic_mean']:.4f} | {t['icir']:.4f} | {mark} |")

        # TPE 精调
        p3 = result["phase3"]
        if not p3.get("skipped"):
            lines.append(f"\n## Optuna TPE 精调 ({p3.get('n_trials', 0)} trials)")
            br = p3.get("best_result", {})
            lines.append(f"- 最优因子数: {br.get('n_factors', '?')}")
            lines.append(f"- Rank IC: {br.get('rank_ic_mean', 0):.4f}")
            lines.append(f"- ICIR: {br.get('icir', 0):.4f}")

        # 最终结果
        fm = result["final_metrics"]
        lines.append("\n## 最终结果")
        lines.append(f"- 最优因子数: {len(result['final_subset'])}")
        lines.append(f"- Rank IC: {fm['rank_ic_mean']:.4f}")
        lines.append(f"- ICIR: {fm['icir']:.4f}")
        lines.append(f"- CV RMSE: {fm['cv_rmse_mean']:.4f}")
        delta = fm["rank_ic_mean"] - b["rank_ic_mean"]
        lines.append(f"- 相比基线 IC 变化: {delta:+.4f}")

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
