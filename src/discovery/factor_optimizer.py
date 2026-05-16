# -*- coding: utf-8 -*-
"""因子权重优化器 (Factor Weight Optimizer).

三步流程 + 四个护栏，输出 Markdown 变更报告。
权重替换由独立指令 --factor-apply 执行。
"""

import json
import logging

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.discovery.factor_backtest_engine import FactorBacktestEngine

logger = logging.getLogger(__name__)

_REPORT_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports" / "factor_optimization"
_LATEST_JSON = _REPORT_DIR / "latest.json"
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"

# 权重钳位范围
_WEIGHT_MIN = 5
_WEIGHT_MAX = 35
# 扰动步长
_PERTURB_STEPS = [-10, -5, 0, 5, 10]
# 候选因子上限（控制网格搜索规模）
_MAX_CANDIDATES = 6
# 回撤约束
_MAX_DRAWDOWN = 0.20
_MAX_DRAWDOWN_FALLBACK = 0.25
# 最小改进阈值（年化收益差 ≥ 1%）
_MIN_IMPROVEMENT = 0.01
# 相关性提示阈值
_CORR_WARN_THRESHOLD = 0.75


class FactorOptimizer:
    """因子权重优化器。"""

    def __init__(self, tushare_fetcher=None):
        self._engine = FactorBacktestEngine(tushare_fetcher)
        self._fetcher = tushare_fetcher

    # ── public API ──

    def optimize(self, mode: str = "postmarket", window: int = 60,
                 normalize: bool = False) -> Optional[Dict]:
        """运行完整优化流程，返回报告 dict。

        Args:
            mode: "intraday" 或 "postmarket"
            window: 回测窗口交易日数
            normalize: True 时最优组合按原总权重归一化（零和重分配）

        Returns:
            {
                "report": "## 因子权重优化报告\\n...",
                "report_path": "discovery_reports/factor_optimization/optimize_20260516_2130.md",
                "recommendation": {factor_name: new_weight, ...},
                "baseline": {factor_name: current_weight, ...},
            }
        """
        current_weights = self._engine._get_default_weights(mode)
        if not current_weights:
            logger.warning("[FactorOptimizer] %s 模式无可用因子", mode)
            return None

        # Step 1: 单因子筛选
        candidates = self._screen_factors(current_weights, mode, window)
        if not candidates:
            logger.warning("[FactorOptimizer] %s 模式无因子通过 Step 1 筛选", mode)
            return self._no_result_report(mode, window, current_weights)

        # Step 2: 权重扰动搜索
        results, best = self._perturbation_search(candidates, current_weights, mode, window, normalize)

        # 确定报告路径（一次 now 避免时间差）
        now = datetime.now()
        report_path = _REPORT_DIR / f"optimize_{now.strftime('%Y%m%d_%H%M')}.md"

        # Step 3: 护栏检查 + 生成报告
        report_md, recommendation = self._build_report(
            mode, window, current_weights, candidates, results, best, report_path)

        # 保存报告 + latest.json
        _REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_md, encoding="utf-8")
        self._save_latest(recommendation, current_weights, mode, now.isoformat())

        return {
            "report": report_md,
            "report_path": str(report_path),
            "recommendation": recommendation,
            "baseline": current_weights,
        }

    @staticmethod
    def apply_weights(report_path: str) -> bool:
        """从报告文件中提取新权重并写入 .env。

        Returns: True 如果成功写入。
        """
        rp = Path(report_path)
        if not rp.exists():
            logger.error("[FactorOptimizer] 报告文件不存在: %s", report_path)
            return False

        text = rp.read_text(encoding="utf-8")
        # 解析权重变更表
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
            logger.error("[FactorOptimizer] 报告中未找到权重变更")
            return False

        # 备份原 .env
        bak = _ENV_PATH.with_suffix(_ENV_PATH.suffix + f".bak.{datetime.now().strftime('%Y%m%d_%H%M')}")
        bak.write_text(_ENV_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        logger.info("[FactorOptimizer] 已备份 .env → %s", bak.name)

        # 替换权重
        content = _ENV_PATH.read_text(encoding="utf-8")
        for fn, nw in changes.items():
            key = _weight_env_key(fn)
            if key:
                pattern = re.compile(rf"^{key}=.*$", re.MULTILINE)
                content = pattern.sub(f"{key}={nw}", content)

        _ENV_PATH.write_text(content, encoding="utf-8")
        logger.info("[FactorOptimizer] 已更新 %d 个因子权重到 .env", len(changes))
        return True

    # ── Step 1: 单因子筛选 ──

    def _screen_factors(self, weights: Dict[str, float], mode: str,
                        window: int) -> Dict[str, Dict]:
        """对每个因子单独回测，筛选通过回撤+IC 约束的候选。

        Returns: {factor_name: {annual_return, max_drawdown, ic_1, ic_5, ...}}
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

        candidates = {}
        for fn in factor_names:
            # 构造单因子 scores（权重 100%）
            single = {}
            for sd, ss in scores_by_date.items():
                if fn in ss and not ss[fn].empty:
                    single[sd] = {fn: ss[fn]}
            if len(single) < 10:
                continue

            # IC（1日/5日）
            ic_1 = self._engine._calc_rank_ic(single, 1, trading_days, mode).get(fn)
            ic_5 = self._engine._calc_rank_ic(single, 5, trading_days, mode).get(fn)

            # 负 IC 排除
            if ic_1 is not None and ic_1 < 0:
                logger.info("[FactorOptimizer] %s IC(1d)=%.4f 为负，排除", fn, ic_1)
                continue

            # 收益 & 回撤
            ar, mdd = self._backtest_single(single, fn, 5, snap_dates, trading_days, mode)
            if ar is None:
                continue

            entry = {"annual_return": ar, "max_drawdown": mdd,
                     "ic_1": ic_1, "ic_5": ic_5}
            if mdd <= _MAX_DRAWDOWN:
                candidates[fn] = entry

        # 按年化收益排序，取 Top N
        ranked = sorted(candidates.items(), key=lambda x: x[1]["annual_return"], reverse=True)
        if len(ranked) > _MAX_CANDIDATES:
            ranked = ranked[:_MAX_CANDIDATES]

        logger.info("[FactorOptimizer] Step 1: %d 通过 → %d 候选", len(candidates), len(ranked))
        return dict(ranked)

    def _backtest_single(self, scores_by_date: Dict, fn: str, hold_days: int,
                         snap_dates: List[str], trading_days: List[str], mode: str):
        """对单个因子做轻量回测，返回 (年化收益, 最大回撤)。

        使用非重叠持有期，每 hold_days 个交易日开仓一次，避免收益重复计算。
        """
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
                bp = self._engine._get_price(code, buy_date, bf)
                sp = self._engine._get_price(code, sell_date, sf)
                if bp and sp and bp > 0:
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

        if capital <= 1_000_000.0 or num_trades == 0:
            return None, None
        ar = (capital / 1_000_000.0) ** (252 / (num_trades * hold_days)) - 1
        return round(ar, 4), round(mdd, 4)

    def _get_recent_snap_dates(self, factor_names: List[str], mode: str,
                               window: int) -> List[str]:
        """获取最近 N 天快照日期。"""
        from src.storage import DatabaseManager, FactorScoreSnapshot
        db = DatabaseManager()
        with db.get_session() as sess:
            from sqlalchemy import func
            rows = (sess.query(FactorScoreSnapshot.trade_date)
                    .filter(FactorScoreSnapshot.mode == mode,
                            FactorScoreSnapshot.factor_name.in_(factor_names))
                    .group_by(FactorScoreSnapshot.trade_date)
                    .order_by(FactorScoreSnapshot.trade_date.desc())
                    .limit(window).all())
            return sorted([r[0] for r in rows])

    # ── Step 2: 权重扰动搜索 ──

    def _perturbation_search(self, candidates: Dict[str, Dict],
                             current_weights: Dict[str, float],
                             mode: str, window: int,
                             normalize: bool = False) -> Tuple[List[Dict], Optional[Dict]]:
        """对候选因子在当前权重附近做全组合搜索。

        Returns: (all_results, best_result)
        """
        factor_names = list(candidates.keys())
        # 构建每个因子的权重选项
        weight_options = {}
        for fn in factor_names:
            cw = current_weights.get(fn, 15)
            opts = set()
            for step in _PERTURB_STEPS:
                v = int(cw + step)
                if _WEIGHT_MIN <= v <= _WEIGHT_MAX:
                    opts.add(v)
            weight_options[fn] = sorted(opts)

        # 全组合
        import itertools
        snap_dates = self._get_recent_snap_dates(
            list(current_weights.keys()), mode, window)
        trading_days = self._engine._get_trading_days(snap_dates)

        best = None
        all_results = []
        total = 1
        for opts in weight_options.values():
            total *= len(opts)

        logger.info("[FactorOptimizer] Step 2: %d 因子, %d 组合", len(factor_names), total)

        # 预加载快照（所有因子）
        all_scores = self._engine._load_snapshots(list(current_weights.keys()), mode, snap_dates)
        all_codes = set()
        for ss in all_scores.values():
            for s in ss.values():
                if hasattr(s, 'index'):
                    all_codes.update(s.index.tolist())
        self._engine._prefetch_prices(list(all_codes), trading_days)

        keys = list(weight_options.keys())
        for combo_idx, combo_vals in enumerate(itertools.product(*weight_options.values())):
            combo = dict(zip(keys, combo_vals))
            # 合并当前权重（候选之外保持原值）
            full_weights = dict(current_weights)
            full_weights.update(combo)

            ar, mdd, sharpe, wr = self._evaluate_combo(
                full_weights, all_scores, snap_dates, trading_days, mode)
            if ar is None:
                continue

            entry = {"weights": full_weights, "annual_return": ar,
                     "max_drawdown": mdd, "sharpe": sharpe, "win_rate": wr}
            all_results.append(entry)

            if mdd <= _MAX_DRAWDOWN:
                if best is None or ar > best["annual_return"]:
                    best = entry

            # 进度日志（每 20%）
            if total > 100 and combo_idx % max(1, total // 5) == 0:
                logger.info("[FactorOptimizer] Step 2: %d/%d", combo_idx + 1, total)

        # 兜底：放宽回撤
        if best is None:
            logger.warning("[FactorOptimizer] -20%% 约束无有效组合，放宽至 -25%%")
            for entry in all_results:
                if entry["max_drawdown"] <= _MAX_DRAWDOWN_FALLBACK:
                    if best is None or entry["annual_return"] > best["annual_return"]:
                        best = entry

        if best:
            best["fallback_triggered"] = best.get("max_drawdown", 1) > _MAX_DRAWDOWN
            if normalize:
                self._normalize_weights(best, current_weights)

        logger.info("[FactorOptimizer] Step 2 完成: %d 有效组合", len(all_results))
        return all_results, best

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

        # 四舍五入后补足差值（从最大权重项吸收）
        diff = orig_total - sum(normalized.values())
        if diff != 0:
            largest = max(normalized, key=lambda k: normalized[k])
            normalized[largest] = max(_WEIGHT_MIN, min(_WEIGHT_MAX,
                                       normalized[largest] + diff))

        logger.info("[FactorOptimizer] 归一化: %d → %d (scale=%.3f)",
                    int(best_total), int(orig_total), scale)
        best["weights"] = normalized
        best["weights_normalized"] = True

    def _evaluate_combo(self, weights: Dict[str, float], all_scores: Dict,
                        snap_dates: List[str], trading_days: List[str], mode: str):
        """评估一组权重的回测表现。

        使用非重叠持有期，每 hold_days 个交易日开仓一次。
        """
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
                bp = self._engine._get_price(code, buy_date, bf)
                sp = self._engine._get_price(code, sell_date, sf)
                if bp and sp and bp > 0:
                    ret = (sp - bp) / bp
                    returns.append(ret)
                    total_trades += 1
                    if ret > 0:
                        wins += 1
            if not returns:
                continue
            day_ret = np.mean(returns)
            daily_returns.append(day_ret)
            capital *= (1 + day_ret)
            if capital > peak:
                peak = capital
            dd = (peak - capital) / peak
            if dd > mdd:
                mdd = dd

        if capital <= 1_000_000.0 or not daily_returns:
            return None, None, None, None

        num_trades = len(daily_returns)
        ar = (capital / 1_000_000.0) ** (252 / (num_trades * hd)) - 1
        drs = np.array(daily_returns)
        sharpe = (np.mean(drs) / np.std(drs, ddof=1) * np.sqrt(252 / hd)) if np.std(drs, ddof=1) > 0 else 0
        wr = wins / total_trades if total_trades > 0 else 0

        return round(ar, 4), round(mdd, 4), round(sharpe, 4), round(wr, 4)

    # ── Step 3 + 护栏: 报告生成 ──

    def _build_report(self, mode: str, window: int,
                      current_weights: Dict[str, float],
                      candidates: Dict[str, Dict],
                      all_results: List[Dict],
                      best: Optional[Dict],
                      report_path: Path) -> Tuple[str, Dict[str, float]]:
        """生成 Markdown 报告 + 推荐权重。"""
        now = datetime.now()
        lines = [
            "# 因子权重优化报告",
            "",
            f"**日期**: {now.strftime('%Y-%m-%d %H:%M')}",
            f"**模式**: {mode}",
            f"**回测窗口**: 最近 {window} 个交易日",
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
        lines.append("## Step 2 最优组合")
        lines.append("")

        # 稳定性检查
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

        # 稳定性注释
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
            cw = current_eval.get("win_rate", 0)
            bw = best.get("win_rate", 0)
            lines.append(f"| 胜率 | {cw:.1%} | {bw:.1%} | {bw-cw:+.1%} |")

        # 搜索统计
        total_combos = len(all_results)
        valid_combos = sum(1 for r in all_results if r["max_drawdown"] <= _MAX_DRAWDOWN)
        lines.append(f"\n搜索: {len(candidates)} 因子 × 5 档 = {total_combos} 组合, 有效 {valid_combos} 个")

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

        # A: 最小改进阈值
        imp = best["annual_return"] - (current_eval.get("annual_return", 0) if current_eval else 0)
        if imp < _MIN_IMPROVEMENT:
            lines.append(f"| 最小改进阈值 (≥1%) | ⚠️ 改进不足 (+{imp:.1%})，建议保持当前权重 |")
        else:
            lines.append(f"| 最小改进阈值 (≥1%) | ✅ +{imp:.1%} |")

        # B: 稳定性
        unstable = [fn for fn, st in stability.items() if "跳过" in st]
        if unstable:
            lines.append(f"| 稳定性 | ⚠️ {', '.join(unstable)} 跳过 |")
        else:
            lines.append("| 稳定性 | ✅ |")

        # 兜底
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
        ar, mdd, sharpe, wr = self._evaluate_combo(weights, all_scores, snap_dates, trading_days, mode)
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
                          current_weights: Dict[str, float]) -> Dict:
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
        rp = _REPORT_DIR / f"optimize_{now.strftime('%Y%m%d_%H%M')}.md"
        _REPORT_DIR.mkdir(parents=True, exist_ok=True)
        rp.write_text(report_md, encoding="utf-8")
        return {"report": report_md, "report_path": str(rp),
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


# ── 工具函数 ──

def _weight_env_key(factor_name: str) -> Optional[str]:
    """因子名 → .env key 映射。"""
    mapping = {
        "sector": "DISCOVER_WEIGHT_SECTOR",
        "ma_entry": "DISCOVER_WEIGHT_MA_ENTRY",
        "momentum": "DISCOVER_WEIGHT_MOMENTUM",
        "rebound": "DISCOVER_WEIGHT_REBOUND",
        "popularity": "DISCOVER_WEIGHT_POPULARITY_POSTMARKET",
        "ranking_momentum": "DISCOVER_WEIGHT_RANKING_MOMENTUM_POSTMARKET",
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
    }
    key = mapping.get(factor_name)
    if key:
        return key
    return f"DISCOVER_WEIGHT_{factor_name.upper()}"
