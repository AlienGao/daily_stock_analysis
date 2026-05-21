# -*- coding: utf-8 -*-
"""因子权重优化器 (Factor Weight Optimizer).

三步流程 + 四个护栏，输出 Markdown 变更报告。
Step 2 使用 Optuna TPE 采样替代网格搜索，通过 SQLite 持久化实现 CLI/Web 共享。
优化通过护栏后自动写入 .env。
"""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import optuna

from src.discovery.factor_backtest_engine import FactorBacktestEngine

logger = logging.getLogger(__name__)

_REPORT_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports" / "factor_optimization"
_LATEST_JSON = _REPORT_DIR / "latest.json"
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
_TPE_STORAGE = Path(__file__).resolve().parent.parent.parent / "optuna_cache" / "factor_opt.db"

# 权重钳位范围
_WEIGHT_MIN = 5
_WEIGHT_MAX = 35
# 候选因子上限（控制 TPE 搜索空间维度）
_MAX_CANDIDATES = 10
# 回撤约束
_MAX_DRAWDOWN = 0.80
_MAX_DRAWDOWN_FALLBACK = 0.90
# 最小改进阈值（年化收益差 ≥ 1%）
_MIN_IMPROVEMENT = 0.01
# 相关性提示阈值
_CORR_WARN_THRESHOLD = 0.75


class FactorOptimizer:
    """因子权重优化器。"""

    def __init__(self, tushare_fetcher=None, progress_callback: Optional[Callable] = None):
        self._engine = FactorBacktestEngine(tushare_fetcher)
        self._fetcher = tushare_fetcher
        self._cb = progress_callback

    def _notify(self, phase: str, **kwargs):
        """通知进度回调（Web 轮询用）。"""
        if self._cb:
            try:
                self._cb(dict(phase=phase, **kwargs))
            except Exception:
                pass

    # ── public API ──

    def optimize(self, mode: str = "postmarket", window: int = 60,
                 normalize: bool = False, n_trials: int = 100,
                 auto_apply: bool = True,
                 preloaded: Optional[Dict] = None,
                 use_persistent_storage: bool = True,
                 study_name: Optional[str] = None,
                 skip_report: bool = False) -> Optional[Dict]:
        """运行完整优化流程，返回报告 dict。

        Args:
            mode: "intraday" 或 "postmarket"
            window: 回测窗口交易日数
            normalize: True 时最优组合按原总权重归一化（零和重分配）
            n_trials: Optuna TPE 试验次数
            auto_apply: True 时自动将推荐权重写入 .env
            preloaded: 预加载数据（walk-forward 用），含 snap_dates/scores/trading_days/window_pool
            use_persistent_storage: False 时使用纯内存 study（不写 SQLite）
            study_name: 自定义 Optuna study 名（walk-forward 按日期分 study 用）
            skip_report: True 时不写报告文件（walk-forward 用）
        """
        current_weights = self._engine._get_default_weights(mode)
        if not current_weights:
            logger.warning("[FactorOptimizer] %s 模式无可用因子", mode)
            return None

        # Step 1: 单因子筛选（仅排除 IC<0，放宽回撤限制）
        self._notify("screen", message="Step 1: 因子筛选…")
        candidates = self._screen_factors(current_weights, mode, window)
        if not candidates:
            logger.warning("[FactorOptimizer] %s 模式无因子通过 Step 1 筛选", mode)
            result = self._no_result_report(mode, window, current_weights, persist=use_persistent_storage)
            self._notify("done", result=result)
            return result

        # Step 2: Optuna TPE 搜索
        self._notify("tpe", message="Step 2: TPE 搜索…", trial=0, n_trials=n_trials)
        all_results, best, total_trials = self._tpe_search(
            candidates, current_weights, mode, window, n_trials, normalize,
            preloaded=preloaded, use_persistent_storage=use_persistent_storage,
            study_name=study_name)

        if total_trials == 0:
            logger.warning("[FactorOptimizer] TPE 搜索无有效 trial")
            result = self._no_result_report(mode, window, current_weights, persist=use_persistent_storage)
            self._notify("done", result=result)
            return result

        # 确定报告路径
        now = datetime.now()
        report_path = _REPORT_DIR / f"optimize_{now.strftime('%Y%m%d_%H%M')}.md"

        # Step 3: 护栏检查 + 生成报告
        self._notify("guardrails", message="Step 3: 护栏检查…")
        report_md, recommendation = self._build_report(
            mode, window, current_weights, candidates, all_results, best, report_path, total_trials)

        # 保存报告 + 元数据（仅持久化模式，避免回测 walk-forward 污染历史）
        if use_persistent_storage and not skip_report:
            _REPORT_DIR.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report_md, encoding="utf-8")
            self._save_latest(recommendation, current_weights, mode, now.isoformat())
            self._save_metadata(report_path, now, mode, recommendation, current_weights, auto_apply)

        result = {
            "report": report_md,
            "report_path": str(report_path) if use_persistent_storage else "",
            "recommendation": recommendation,
            "baseline": current_weights,
            "applied": False,
        }

        # 自动应用权重到 .env
        if auto_apply and recommendation:
            applied = FactorOptimizer.apply_weights(recommendation, mode)
            result["applied"] = applied
            if applied:
                logger.info("[FactorOptimizer] 权重已自动应用到 .env")
            else:
                logger.warning("[FactorOptimizer] 自动应用权重失败")

        self._notify("done", result=result)
        return result

    @staticmethod
    def apply_weights(source, mode: str = "") -> bool:
        """写入权重到 .env。

        Args:
            source: str (报告路径) 或 Dict[str, float] (因子名→新权重)
            mode: "intraday" 或 "postmarket"。dict 方式必须提供；文件路径时可从报告解析。

        Returns: True 如果成功写入。
        """
        if isinstance(source, dict):
            changes = source
            if not mode:
                logger.error("[FactorOptimizer] dict 方式必须提供 mode 参数")
                return False
        else:
            # 从 Markdown 报告解析权重变更
            rp = Path(source)
            if not rp.exists():
                logger.error("[FactorOptimizer] 报告文件不存在: %s", source)
                return False

            text = rp.read_text(encoding="utf-8")
            # 解析模式
            if not mode:
                m = re.search(r'\*\*模式\*\*:\s*(\w+)', text)
                if m:
                    mode = m.group(1)
                else:
                    mode = "postmarket"  # 回退
            logger.info("[FactorOptimizer] 从报告解析模式: %s", mode)

            changes: Dict[str, float] = {}
            in_table = False
            for line in text.split("\n"):
                line = line.strip()
                if line.startswith("| 因子 |") and "新权重" in line:
                    in_table = True
                    continue
                if in_table and line.startswith("|-"):
                    continue
                if in_table and line.startswith("|") and "|" in line[1:]:
                    parts = [p.strip() for p in line.split("|")[1:-1]]
                    if len(parts) >= 3:
                        fn = parts[0]
                        try:
                            nw = float(parts[2])
                            changes[fn] = nw
                        except ValueError:
                            continue
                elif in_table and not line.startswith("|"):
                    in_table = False

        if not changes:
            logger.error("[FactorOptimizer] 未找到权重变更")
            return False

        # 备份原 .env
        bak = _ENV_PATH.with_suffix(_ENV_PATH.suffix + f".bak.{datetime.now().strftime('%Y%m%d_%H%M')}")
        bak.write_text(_ENV_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        logger.info("[FactorOptimizer] 已备份 .env → %s", bak.name)

        # 替换权重（mode-aware key mapping）
        content = _ENV_PATH.read_text(encoding="utf-8")
        for fn, nw in changes.items():
            key = _weight_env_key(fn, mode)
            if key:
                pattern = re.compile(rf"^{key}=.*$", re.MULTILINE)
                if pattern.search(content):
                    content = pattern.sub(f"{key}={nw}", content)
                else:
                    # key 不存在则追加
                    content += f"\n{key}={nw}\n"
                logger.info("[FactorOptimizer] %s → %s = %s", fn, key, nw)

        _ENV_PATH.write_text(content, encoding="utf-8")
        # 同步更新进程环境变量，使运行中的 server 无需重启即可读取新权重
        for fn, nw in changes.items():
            key = _weight_env_key(fn, mode)
            if key:
                os.environ[key] = str(int(nw))
        # 清除 get_factor_weights 的 LRU 缓存，使下次调用能读到新权重
        from src.discovery.engine import get_factor_weights
        get_factor_weights.cache_clear()
        logger.info("[FactorOptimizer] 已更新 %d 个因子权重到 .env (mode=%s)", len(changes), mode)
        return True

    # ── Step 1: 单因子筛选 ──

    def _screen_factors(self, weights: Dict[str, float], mode: str,
                        window: int) -> Dict[str, Dict]:
        """对每个因子单独回测，筛选通过 IC 约束的候选。

        仅排除 IC(1d) < 0 的因子，回撤过滤交给 TPE 处理。
        """
        factor_names = list(weights.keys())
        snap_dates = self._get_recent_snap_dates(factor_names, mode, window)
        if len(snap_dates) < 10:
            logger.warning("[FactorOptimizer] Step 1: 快照日期不足 (%d < 10)", len(snap_dates))
            return {}

        scores_by_date = self._engine._load_snapshots(factor_names, mode, snap_dates)
        trading_days = self._engine._get_trading_days(snap_dates)
        if len(trading_days) < 2:
            return {}

        all_codes = set()
        for ss in scores_by_date.values():
            for s in ss.values():
                if hasattr(s, 'index'):
                    all_codes.update(s.index.tolist())
        self._engine._prefetch_prices(list(all_codes), trading_days)
        fw_returns = self._engine._precompute_forward_returns(
            list(all_codes), trading_days, snap_dates, [1, 5], mode)

        entries = {}
        for fn in factor_names:
            single = {}
            for sd, ss in scores_by_date.items():
                if fn in ss and not ss[fn].empty:
                    single[sd] = {fn: ss[fn]}
            if len(single) < 10:
                continue

            ic_1 = self._engine._calc_rank_ic(single, 1, trading_days, mode, fw_returns).get(fn)
            ic_5 = self._engine._calc_rank_ic(single, 5, trading_days, mode, fw_returns).get(fn)

            # 仅排除负 IC（方向错误），回撤交给 TPE
            if ic_1 is not None and ic_1 < 0:
                logger.info("[FactorOptimizer] %s IC(1d)=%.4f 为负，排除", fn, ic_1)
                continue

            ar, mdd = self._backtest_single(single, fn, 5, snap_dates, trading_days, mode)
            if ar is None:
                continue

            entries[fn] = {"annual_return": ar, "max_drawdown": mdd,
                           "ic_1": ic_1, "ic_5": ic_5}

        # 按年化收益排序，取 Top N
        ranked = sorted(entries.items(), key=lambda x: x[1]["annual_return"], reverse=True)
        if len(ranked) > _MAX_CANDIDATES:
            ranked = ranked[:_MAX_CANDIDATES]

        logger.info("[FactorOptimizer] Step 1: %d → %d 候选", len(entries), len(ranked))
        return dict(ranked)

    def _backtest_single(self, scores_by_date: Dict, fn: str, hold_days: int,
                         snap_dates: List[str], trading_days: List[str], mode: str):
        """对单个因子做轻量回测，返回 (年化收益, 最大回撤)。"""
        is_intra = mode == "intraday"
        bf = "close" if is_intra else "open"
        sf = "close" if is_intra else "open"

        capital = 1_000_000.0
        peak = capital
        mdd = 0.0
        num_trades = 0

        for i in range(0, len(snap_dates), hold_days):
            snap_date = snap_dates[i]
            if snap_date not in trading_days:
                continue
            ti = trading_days.index(snap_date)
            buy_idx = ti if is_intra else ti + 1
            sell_idx = (ti + hold_days) if is_intra else (ti + 1 + hold_days)
            if buy_idx >= len(trading_days) or sell_idx >= len(trading_days):
                continue
            buy_date = trading_days[buy_idx]
            sell_date = trading_days[sell_idx]

            ss = scores_by_date.get(snap_date, {}).get(fn)
            if ss is None or ss.empty:
                continue
            top5 = ss.nlargest(5)
            returns = []
            for code in top5.index:
                # 涨跌停限制：买入日涨停则跳过
                if self._engine._is_limit_up(code, buy_date, bf):
                    continue
                bp = self._engine._get_price(code, buy_date, bf)
                sp, _, sell_status = self._engine._resolve_sell_price(
                    code, sell_date, sf, trading_days)
                if bp and sp and bp > 0 and sell_status not in ("locked", "open"):
                    returns.append((sp - bp) / bp)
            if not returns:
                continue
            day_ret = np.mean(returns)
            capital *= (1 + day_ret)
            num_trades += 1
            if capital > peak:
                peak = capital
            dd = (peak - capital) / peak
            if dd > mdd:
                mdd = dd

        if num_trades == 0:
            return None, None
        ar = (capital / 1_000_000.0) ** (252 / (num_trades * hold_days)) - 1
        return round(ar, 4), round(mdd, 4)

    def _get_recent_snap_dates(self, factor_names: List[str], mode: str,
                               window: int, end_date: str = None) -> List[str]:
        """获取最近 N 天快照日期。end_date 非空时取该日期及之前最近 window 个。"""
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        with db.get_session() as sess:
            q = (sess.query(FactorScoreSnapshot.trade_date)
                 .filter(FactorScoreSnapshot.mode == mode,
                         FactorScoreSnapshot.factor_name.in_(factor_names)))
            if end_date:
                q = q.filter(FactorScoreSnapshot.trade_date <= end_date)
            rows = (q.group_by(FactorScoreSnapshot.trade_date)
                    .order_by(FactorScoreSnapshot.trade_date.desc())
                    .limit(window).all())
            return sorted([r[0] for r in rows])

    # ── Step 2: Optuna TPE 搜索 ──

    def _tpe_search(self, candidates: Dict[str, Dict],
                    current_weights: Dict[str, float],
                    mode: str, window: int,
                    n_trials: int,
                    normalize: bool = False,
                    preloaded: Optional[Dict] = None,
                    use_persistent_storage: bool = True,
                    study_name: Optional[str] = None) -> Tuple[List[Dict], Optional[Dict], int]:
        """用 Optuna TPE 采样搜索最优权重组合。

        每个 trial 从历史中随机抽 5 个窗口，以平均超额收益（vs 等权基准）
        为 objective，让 TPE 学到跨行情的鲁棒解。
        """
        import random as _random

        candidate_names = list(candidates.keys())
        all_factor_names = list(current_weights.keys())
        study_name = study_name or f"{mode}_w{window}"

        if use_persistent_storage:
            _TPE_STORAGE.parent.mkdir(parents=True, exist_ok=True)
            storage_url = f"sqlite:///{_TPE_STORAGE}"
        else:
            storage_url = None

        study = optuna.create_study(
            direction="maximize",
            storage=storage_url,
            study_name=study_name,
            load_if_exists=use_persistent_storage,
            sampler=optuna.samplers.TPESampler(seed=42),
        )

        if preloaded:
            all_snap_dates = preloaded["snap_dates"]
            all_scores = preloaded["scores"]
            full_tdays = preloaded["trading_days"]
            _window_pool = preloaded["window_pool"]
        else:
            # 预加载历史数据（上限 252 个交易日 ≈ 1 年，避免全表扫描 49M 行）
            self._notify("preload", message="加载历史快照日期…")
            all_snap_dates = self._get_recent_snap_dates(all_factor_names, mode, 252)
            if len(all_snap_dates) < window:
                logger.warning("[FactorOptimizer] 历史数据不足 %d 日（仅 %d），使用全部", window, len(all_snap_dates))

            self._notify("preload", message=f"加载因子得分 ({len(all_snap_dates)} 日)…")
            all_scores = self._engine._load_snapshots(all_factor_names, mode, all_snap_dates)

            all_codes = set()
            for ss in all_scores.values():
                for s in ss.values():
                    if hasattr(s, 'index'):
                        all_codes.update(s.index.tolist())

            self._notify("preload", message=f"预取价格数据 ({len(all_codes)} 只)…")
            full_tdays = self._engine._get_trading_days(all_snap_dates)
            self._engine._prefetch_prices(list(all_codes), full_tdays)

            # 预建窗口池：从全部历史日期切片 60 日窗口
            self._notify("preload", message="构建窗口池…")
            _window_pool: List[List[str]] = []
            w = window
            for i in range(w - 1, len(all_snap_dates), 5):
                seg = all_snap_dates[max(0, i - w + 1): i + 1]
                if len(seg) >= 30:
                    _window_pool.append(seg)
            if not _window_pool and len(all_snap_dates) >= 20:
                _window_pool = [all_snap_dates]

        logger.info("[FactorOptimizer] 窗口池: %d 个候选 (window=%d, dates=%d)",
                    len(_window_pool), window, len(all_snap_dates))

        all_results: List[Dict] = []
        best: Optional[Dict] = None
        best_excess_mdd_cache: Dict[str, float] = {}

        def objective(trial: optuna.trial.Trial) -> float:
            nonlocal best
            full_weights = dict(current_weights)
            for fn in candidate_names:
                full_weights[fn] = trial.suggest_int(fn, _WEIGHT_MIN, _WEIGHT_MAX)

            excesses: List[float] = []
            n_pick = min(5, len(_window_pool))
            chosen = _random.sample(_window_pool, n_pick)
            for w_dates in chosen:
                w_tdays = self._engine._get_trading_days(w_dates)
                ar, mdd, _, _, base_ar = self._evaluate_combo(
                    full_weights, all_scores, w_dates, w_tdays, mode)
                if ar is not None and base_ar is not None and mdd is not None:
                    excesses.append(ar - base_ar)

            if not excesses:
                return -999.0

            excess = float(np.mean(excesses))
            entry = {"weights": dict(full_weights), "excess_return": excess}
            all_results.append(entry)

            if best is None or excess > best.get("excess_return", -999):
                best = entry
                best_excess_mdd_cache.clear()

            return excess

        def _tpe_callback(study_opt: optuna.Study, trial: optuna.trial.FrozenTrial):
            self._notify("tpe", trial=len(study_opt.trials), n_trials=n_trials,
                         best_value=study_opt.best_value if study_opt.best_value > -900 else None)

        logger.info("[FactorOptimizer] Step 2: %d 因子, %d trials (study=%s)",
                    len(candidate_names), n_trials, study_name)

        study.optimize(objective, n_trials=n_trials, callbacks=[_tpe_callback], n_jobs=1)

        total_trials = len(study.trials)

        # TPE 完成后，用最新窗口评估最佳权重，获取度量用于报告护栏
        latest_snap_dates = all_snap_dates[-window:] if len(all_snap_dates) >= window else all_snap_dates
        latest_tdays = self._engine._get_trading_days(latest_snap_dates)
        if best and best.get("weights"):
            l_ar, l_mdd, l_sharpe, l_wr, l_base = self._evaluate_combo(
                best["weights"], all_scores, latest_snap_dates, latest_tdays, mode)
            if l_ar is not None:
                best["annual_return"] = l_ar
                best["max_drawdown"] = l_mdd if l_mdd is not None else 0
                best["sharpe"] = l_sharpe if l_sharpe is not None else 0
                best["win_rate"] = l_wr if l_wr is not None else 0
                best["baseline_return"] = l_base if l_base is not None else 0
                best["excess_return"] = l_ar - (l_base or l_ar)
            else:
                best["annual_return"] = 0.0
                best["max_drawdown"] = 0.0
                best["sharpe"] = 0.0
                best["win_rate"] = 0.0
                best["baseline_return"] = 0.0

        # 兜底：放宽回撤（基于最新窗口的 max_drawdown）
        if best is None:
            logger.warning("[FactorOptimizer] 无有效组合，放宽回撤约束")
            for entry in all_results:
                if not entry.get("weights"):
                    continue
                l_ar, l_mdd, l_sh, l_wr, _ = self._evaluate_combo(
                    entry["weights"], all_scores, latest_snap_dates, latest_tdays, mode)
                if l_ar is not None and l_mdd is not None and l_mdd <= _MAX_DRAWDOWN_FALLBACK:
                    if best is None or l_ar > best.get("annual_return", -999):
                        entry["annual_return"] = l_ar
                        entry["max_drawdown"] = l_mdd
                        entry["sharpe"] = l_sh
                        entry["win_rate"] = l_wr
                        best = entry

        if best:
            best["fallback_triggered"] = best.get("max_drawdown", 1) > _MAX_DRAWDOWN
            if normalize:
                self._normalize_weights(best, current_weights)

        logger.info("[FactorOptimizer] Step 2 完成: %d trials, %d 有效",
                    total_trials, len(all_results))
        return all_results, best, total_trials

    def _normalize_weights(self, best: Dict, current_weights: Dict[str, float]) -> None:
        """将最优组合权重按比例缩放到原总权重，保持零和重分配。"""
        orig_total = sum(current_weights.values())
        best_total = sum(best["weights"].values())
        if best_total <= 0 or abs(best_total - orig_total) < 0.5:
            return

        scale = orig_total / best_total
        normalized = {}
        for k, v in best["weights"].items():
            nv = max(_WEIGHT_MIN, min(_WEIGHT_MAX, round(v * scale)))
            normalized[k] = nv

        diff = orig_total - sum(normalized.values())
        if diff != 0:
            largest = max(normalized, key=lambda k: normalized[k])
            normalized[largest] = max(_WEIGHT_MIN, min(_WEIGHT_MAX,
                                       normalized[largest] + diff))

        logger.info("[FactorOptimizer] 归一化: %d → %d (scale=%.3f)",
                    int(best_total), int(orig_total), scale)
        best["weights"] = normalized
        best["weights_normalized"] = True

    def _eval_weights(self, weights: Dict[str, float], all_scores: Dict,
                      snap_dates: List[str], trading_days: List[str], mode: str):
        """评估一组权重的回测表现，返回 (ar, mdd, sharpe, wr) 或 (None*4)。"""
        is_intra = mode == "intraday"
        bf = "close" if is_intra else "open"
        sf = "close" if is_intra else "open"
        hd = 5

        capital = 1_000_000.0
        peak = capital
        mdd = 0.0
        daily_returns = []
        wins = 0
        total_trades = 0

        for i in range(0, len(snap_dates), hd):
            snap_date = snap_dates[i]
            if snap_date not in trading_days:
                continue
            ti = trading_days.index(snap_date)
            buy_idx = ti if is_intra else ti + 1
            sell_idx = (ti + hd) if is_intra else (ti + 1 + hd)
            if buy_idx >= len(trading_days) or sell_idx >= len(trading_days):
                continue
            buy_date = trading_days[buy_idx]
            sell_date = trading_days[sell_idx]

            ss = all_scores.get(snap_date, {})
            comp = self._engine._compute_composite(ss, weights)
            if comp.empty:
                continue
            top5 = comp.nlargest(5)
            returns = []
            for code in top5.index:
                # 涨跌停限制：买入日涨停则跳过
                if self._engine._is_limit_up(code, buy_date, bf):
                    continue
                bp = self._engine._get_price(code, buy_date, bf)
                sp, _, sell_status = self._engine._resolve_sell_price(
                    code, sell_date, sf, trading_days)
                if bp and sp and bp > 0 and sell_status not in ("locked", "open"):
                    ret = (sp - bp) / bp
                    returns.append(ret)
                    total_trades += 1
                    if ret > 0:
                        wins += 1
            if not returns:
                daily_returns.append(0.0)
                continue
            day_ret = np.mean(returns)
            daily_returns.append(day_ret)
            capital *= (1 + day_ret)
            if capital > peak:
                peak = capital
            dd = (peak - capital) / peak
            if dd > mdd:
                mdd = dd

        if not daily_returns:
            return None, None, None, None

        num_trades = len(daily_returns)
        ar = (capital / 1_000_000.0) ** (252 / (num_trades * hd)) - 1
        drs = np.array(daily_returns)
        sharpe = (np.mean(drs) / np.std(drs, ddof=1) * np.sqrt(252 / hd)) if np.std(drs, ddof=1) > 0 else 0
        wr = wins / total_trades if total_trades > 0 else 0

        return round(ar, 4), round(mdd, 4), round(sharpe, 4), round(wr, 4)

    def _evaluate_combo(self, weights: Dict[str, float], all_scores: Dict,
                        snap_dates: List[str], trading_days: List[str], mode: str):
        """评估权重组合 + 等权基准，返回 (ar, mdd, sharpe, wr, baseline_ar)。"""
        ar, mdd, sharpe, wr = self._eval_weights(weights, all_scores, snap_dates, trading_days, mode)
        if ar is None:
            return None, None, None, None, None

        eq_weights = {fn: 1.0 for fn in weights}
        base_ar, _, _, _ = self._eval_weights(eq_weights, all_scores, snap_dates, trading_days, mode)
        return ar, mdd, sharpe, wr, base_ar if base_ar is not None else ar

    # ── Step 3 + 护栏: 报告生成 ──

    def _build_report(self, mode: str, window: int,
                      current_weights: Dict[str, float],
                      candidates: Dict[str, Dict],
                      all_results: List[Dict],
                      best: Optional[Dict],
                      report_path: Path,
                      total_trials: int = 0) -> Tuple[str, Dict[str, float]]:
        """生成 Markdown 报告 + 推荐权重。"""
        now = datetime.now()
        lines = [
            "# 因子权重优化报告",
            "",
            f"**日期**: {now.strftime('%Y-%m-%d %H:%M')}",
            f"**模式**: {mode}",
            f"**回测窗口**: 最近 {window} 个交易日",
            f"**优化算法**: Optuna TPE",
            "",
        ]

        # ── Step 1 筛选结果 ──
        lines.append("## Step 1 筛选结果")
        lines.append("")
        lines.append("| 因子 | 年化收益 | 最大回撤 | IC(1日) | IC(5日) | 通过 |")
        lines.append("|------|----------|----------|---------|---------|------|")

        for fn in list(current_weights.keys()):
            c = candidates.get(fn)
            if c:
                lines.append(f"| {fn} | {c['annual_return']:+.1%} | {c['max_drawdown']:.1%} | "
                             f"{c.get('ic_1') or 0:.4f} | {c.get('ic_5') or 0:.4f} | ✅ |")
            else:
                lines.append(f"| {fn} | — | — | — | — | ❌ |")

        lines.append(f"\n通过: {len(candidates)} 个")

        # ── Step 2 最优组合 ──
        lines.append("")
        lines.append("## Step 2 最优组合（TPE）")
        lines.append("")

        prev = self._load_latest(mode)
        stability: Dict[str, str] = {}

        if best is None:
            lines.append("⚠️ 无有效组合（回撤约束内无可选方案）")
            return "\n".join(lines), {}
        elif not prev:
            stability = {fn: "✅" for fn in candidates}
        else:
            for fn in candidates:
                old_delta = prev.get("deltas", {}).get(fn, 0)
                cw = current_weights.get(fn, 15)
                bw = best["weights"].get(fn, cw)
                new_delta = bw - cw
                if old_delta != 0 and new_delta != 0 and (old_delta > 0) != (new_delta > 0):
                    stability[fn] = "⚠️ 方向不稳定, 跳过"
                else:
                    stability[fn] = "✅"

        lines.append("| 因子 | 原权重 | 新权重 | 变化 | IC(1日) | IC(5日) | 稳定性 |")
        lines.append("|------|--------|--------|------|---------|---------|--------|")

        recommendation: Dict[str, float] = {}
        for fn in list(current_weights.keys()):
            cw = current_weights.get(fn, 0)
            if fn in candidates:
                raw_nw = best["weights"].get(fn, cw)
                raw_delta = raw_nw - cw
                c = candidates[fn]
                st = stability.get(fn, "✅")
                if "跳过" in st:
                    nw = cw
                    delta = 0
                    lines.append(f"| {fn} | {cw} | {cw} | 0 | "
                                 f"{c.get('ic_1') or 0:.4f} | {c.get('ic_5') or 0:.4f} | {st} (建议{raw_nw}) |")
                else:
                    nw = raw_nw
                    delta = raw_delta
                    recommendation[fn] = nw
                    lines.append(f"| {fn} | {cw} | {nw} | {delta:+.0f} | "
                                 f"{c.get('ic_1') or 0:.4f} | {c.get('ic_5') or 0:.4f} | {st} |")
            else:
                lines.append(f"| {fn} | {cw} | {cw} | 0 | — | — | — (未参与) |")

        for fn, st in stability.items():
            if "跳过" in st:
                prev_delta = prev.get("deltas", {}).get(fn, 0) if prev else "?"
                pd_str = f"{prev_delta:+.0f}" if isinstance(prev_delta, (int, float)) else str(prev_delta)
                lines.append(f"\n> {fn}: {st}（前次{pd_str}）")

        # ── 最优 vs 当前对比 ──
        current_eval = self._evaluate_current(current_weights, all_results, mode, window)
        lines.append("")
        lines.append("## 最优组合 vs 当前组合")
        lines.append("")
        lines.append("| 指标 | 当前组合 | 最优组合 | 变化 |")
        lines.append("|------|----------|----------|------|")

        if current_eval:
            ca = current_eval.get("annual_return", 0)
            ba = best["annual_return"]
            lines.append(f"| 年化收益 | {ca:+.1%} | {ba:+.1%} | **{ba-ca:+.1%}** |")
            cm = current_eval.get("max_drawdown", 0)
            bm = best["max_drawdown"]
            lines.append(f"| 最大回撤 | {cm:.1%} | {bm:.1%} | {bm-cm:+.1%} |")
            cs = current_eval.get("sharpe", 0)
            bs = best.get("sharpe", 0)
            lines.append(f"| 夏普比率 | {cs:.4f} | {bs:.4f} | {bs-cs:+.4f} |")
            cw_val = current_eval.get("win_rate", 0)
            bw_val = best.get("win_rate", 0)
            lines.append(f"| 胜率 | {cw_val:.1%} | {bw_val:.1%} | {bw_val-cw_val:+.1%} |")

        lines.append(f"\n搜索: {len(candidates)} 因子 × TPE {total_trials} trials, 有效 {len(all_results)} 个")

        # ── 护栏 D: 相关性提示 ──
        corr_warnings = self._check_correlations(best["weights"], current_weights, mode, window)
        if corr_warnings:
            lines.append("")
            lines.append("## 相关性提示")
            lines.append("")
            for w in corr_warnings:
                lines.append(f"> {w}")

        # ── 护栏检查 ──
        lines.append("")
        lines.append("## 护栏检查")
        lines.append("")
        lines.append("| 护栏 | 状态 |")
        lines.append("|------|------|")

        imp = best["annual_return"] - (current_eval.get("annual_return", 0) if current_eval else 0)
        if imp < _MIN_IMPROVEMENT:
            lines.append(f"| 最小改进阈值 (≥1%) | ⚠️ 改进不足 (+{imp:.1%})，建议保持当前权重 |")
        else:
            lines.append(f"| 最小改进阈值 (≥1%) | ✅ +{imp:.1%} |")

        unstable = [fn for fn, st in stability.items() if "跳过" in st]
        if unstable:
            lines.append(f"| 稳定性 | ⚠️ {', '.join(unstable)} 跳过 |")
        else:
            lines.append("| 稳定性 | ✅ |")

        if best.get("fallback_triggered"):
            lines.append("| 兜底 (-25%) | ⚠️ 已触发 |")
        else:
            lines.append("| 兜底 (-25%) | 未触发 |")

        # ── 最终权重汇总 ──
        lines.append("")
        lines.append("## 最终权重汇总")
        lines.append("")
        lines.append(f"**模式**: {mode} | **总因子数**: {len(current_weights)} | **修改数**: {len(recommendation)}")
        lines.append("")
        lines.append("| 因子 | 原权重 | 新权重 | 变化 | 说明 |")
        lines.append("|------|--------|--------|------|------|")

        for fn in sorted(current_weights.keys()):
            cw = current_weights[fn]
            nw = recommendation.get(fn, cw)
            chg = nw - cw
            if chg != 0:
                note = "✅ 已调整"
            elif fn in recommendation:
                note = "─ 未变"
            elif fn in candidates:
                st = stability.get(fn, "")
                if "跳过" in st:
                    note = f"⚠️ 跳过（建议{best['weights'].get(fn, cw):.0f}）"
                else:
                    note = "─ 未变"
            else:
                note = "─ 未参与优化"
            lines.append(f"| {fn} | {cw} | {nw} | {chg:+.0f} | {note} |")

        if not recommendation:
            lines.append(f"\n> ⚠️ 护栏检查未通过，建议保持当前权重不变")

        # ── 应用指令 ──
        if recommendation:
            lines.append("")
            lines.append("## 应用指令")
            lines.append("")
            lines.append("```")
            lines.append(f"python main.py --factor-apply {report_path}")
            lines.append("```")

        return "\n".join(lines), recommendation

    def _evaluate_current(self, weights: Dict[str, float], all_results: List[Dict],
                          mode: str, window: int) -> Optional[Dict]:
        """在 all_results 中找当前权重组合的评估结果。"""
        for r in all_results:
            if r["weights"] == weights:
                return r
        snap_dates = self._get_recent_snap_dates(list(weights.keys()), mode, window)
        trading_days = self._engine._get_trading_days(snap_dates)
        all_scores = self._engine._load_snapshots(list(weights.keys()), mode, snap_dates)
        ar, mdd, sharpe, wr, _ = self._evaluate_combo(weights, all_scores, snap_dates, trading_days, mode)
        if ar is not None:
            return {"annual_return": ar, "max_drawdown": mdd, "sharpe": sharpe, "win_rate": wr}
        return None

    def _check_correlations(self, new_weights: Dict[str, float],
                            current_weights: Dict[str, float],
                            mode: str, window: int) -> List[str]:
        """检查同时加权的因子间相关性。"""
        increased = [fn for fn, nw in new_weights.items()
                     if nw > current_weights.get(fn, 0)]
        if len(increased) < 2:
            return []

        snap_dates = self._get_recent_snap_dates(increased, mode, window)
        if len(snap_dates) < 10:
            return []

        scores = self._engine._load_snapshots(increased, mode, snap_dates)
        factor_series = {}
        for fn in increased:
            vals = []
            for sd in snap_dates:
                ss = scores.get(sd, {}).get(fn)
                if ss is not None and not ss.empty:
                    vals.append(ss.mean())
            if len(vals) >= 10:
                factor_series[fn] = pd.Series(vals)

        if len(factor_series) < 2:
            return []

        warnings = []
        fns = list(factor_series.keys())
        for i in range(len(fns)):
            for j in range(i + 1, len(fns)):
                a, b = fns[i], fns[j]
                corr = factor_series[a].corr(factor_series[b])
                if abs(corr) > _CORR_WARN_THRESHOLD:
                    warnings.append(f"⚠️ {a} 与 {b} 相关性 {corr:.2f}，同时加权重可能过拟合同一类信号")

        return warnings

    # ── 无结果报告 ──

    def _no_result_report(self, mode: str, window: int,
                          current_weights: Dict[str, float],
                          persist: bool = True) -> Dict:
        """生成无结果时的报告。"""
        now = datetime.now()
        lines = [
            "# 因子权重优化报告",
            "",
            f"**日期**: {now.strftime('%Y-%m-%d %H:%M')}",
            f"**模式**: {mode}",
            f"**回测窗口**: 最近 {window} 个交易日",
            "",
            "## 结果",
            "",
            "⚠️ 无因子通过 Step 1 筛选或 Step 2 无有效组合。",
            "",
            "建议扩大回测窗口或检查因子数据完整性。",
        ]
        report_md = "\n".join(lines)
        report_path_str = ""
        if persist:
            rp = _REPORT_DIR / f"optimize_{now.strftime('%Y%m%d_%H%M')}.md"
            _REPORT_DIR.mkdir(parents=True, exist_ok=True)
            rp.write_text(report_md, encoding="utf-8")
            report_path_str = str(rp)
        return {"report": report_md, "report_path": report_path_str,
                "recommendation": {}, "baseline": current_weights}

    # ── 稳定性持久化 ──

    def _save_latest(self, recommendation: Dict[str, float],
                     current_weights: Dict[str, float],
                     mode: str, timestamp: str):
        """保存最近一次推荐到 latest.json。"""
        deltas = {}
        for fn, nw in recommendation.items():
            cw = current_weights.get(fn, 0)
            if nw != cw:
                deltas[fn] = nw - cw

        _REPORT_DIR.mkdir(parents=True, exist_ok=True)
        data = {"mode": mode, "timestamp": timestamp, "deltas": deltas,
                "recommendation": recommendation}
        _LATEST_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_latest(self, mode: str) -> Optional[Dict]:
        """加载最近一次推荐。"""
        if not _LATEST_JSON.exists():
            return None
        try:
            data = json.loads(_LATEST_JSON.read_text(encoding="utf-8"))
            if data.get("mode") == mode:
                return data
        except (json.JSONDecodeError, KeyError):
            pass
        return None

    @staticmethod
    def _save_metadata(report_path: Path, now: datetime, mode: str,
                       recommendation: Dict[str, float],
                       baseline: Dict[str, float],
                       applied: bool):
        """保存元数据 JSON，方便 API 读取（无需解析 Markdown）。"""
        meta = {
            "report_path": str(report_path),
            "timestamp": now.isoformat(),
            "mode": mode,
            "recommendation": recommendation,
            "baseline": baseline,
            "applied": applied,
        }
        meta_path = report_path.with_suffix(".json")
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


# ── 工具函数 ──

def _weight_env_key(factor_name: str, mode: str = "postmarket") -> Optional[str]:
    """因子名 → .env key 映射（区分盘中/盘后模式）。

    intraday popularity → DISCOVER_WEIGHT_POPULARITY_INTRADAY
    postmarket popularity → DISCOVER_WEIGHT_POPULARITY_POSTMARKET
    intraday ranking_momentum → DISCOVER_WEIGHT_RANKING_MOMENTUM (无后缀)
    postmarket ranking_momentum → DISCOVER_WEIGHT_RANKING_MOMENTUM_POSTMARKET
    """
    mapping = {
        "sector": "DISCOVER_WEIGHT_SECTOR",
        "ma_entry": "DISCOVER_WEIGHT_MA_ENTRY",
        "momentum": "DISCOVER_WEIGHT_MOMENTUM",
        "rebound": "DISCOVER_WEIGHT_REBOUND",
        "money_flow": "DISCOVER_WEIGHT_MONEYFLOW",
        "margin": "DISCOVER_WEIGHT_MARGIN",
        "chip": "DISCOVER_WEIGHT_CHIP",
        "technical": "DISCOVER_WEIGHT_TECHNICAL",
        "limit": "DISCOVER_WEIGHT_LIMIT_POST",
        "broker_recommend": "DISCOVER_WEIGHT_BROKER_RECOMMEND",
        "buyback": "DISCOVER_WEIGHT_BUYBACK",
        "concept_heat": "DISCOVER_WEIGHT_CONCEPT_HEAT",
        "hot_money": "DISCOVER_WEIGHT_HOT_MONEY",
        "insider_buy": "DISCOVER_WEIGHT_INSIDER_BUY",
        "institution_hold": "DISCOVER_WEIGHT_INSTITUTION_HOLD",
        "fundamental": "DISCOVER_WEIGHT_FUNDAMENTAL",
        "alpha042": "DISCOVER_WEIGHT_ALPHA042",
        "vwap_deviation": "DISCOVER_WEIGHT_VWAP_DEVIATION",
        "gap_reversal": "DISCOVER_WEIGHT_GAP_REVERSAL",
        "liquid_oversold": "DISCOVER_WEIGHT_LIQUID_OVERSOLD",
        "vwap_reversal": "DISCOVER_WEIGHT_VWAP_REVERSAL",
        "gtja114": "DISCOVER_WEIGHT_GTJA114",
    }
    if factor_name == "popularity":
        return f"DISCOVER_WEIGHT_POPULARITY_{'INTRADAY' if mode == 'intraday' else 'POSTMARKET'}"
    if factor_name == "ranking_momentum":
        return f"DISCOVER_WEIGHT_RANKING_MOMENTUM{'_POSTMARKET' if mode == 'postmarket' else ''}"
    key = mapping.get(factor_name)
    if key:
        return key
    return f"DISCOVER_WEIGHT_{factor_name.upper()}"
