# -*- coding: utf-8 -*-
"""R&D 闭环编排器 —— 借鉴 RD-Agent 的 Hypothesis → Implement → Test → Iterate 模式。

在现有因子框架和回测引擎上构建轻量级自动化因子发现循环：
1. LLM 提出因子假设（Hypothesis Generation）
2. FactorCoder 将假设转化为代码（Implementation）
3. FactorEvaluator 在历史数据上评估（Testing）
4. 评估反馈驱动下一轮假设改进（Feedback → Iteration）

支持 SOTA 跟踪和因子去重。
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.discovery.factor_coder import FactorCoder
from src.discovery.factor_evaluator import FactorEvaluator, FactorEvalResult

logger = logging.getLogger(__name__)

_STATE_DIR = Path(__file__).resolve().parent.parent.parent / ".claude" / "rd_loop"
_RD_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "rd_loop_reports"
_FACTORS_DIR = Path(__file__).resolve().parent / "factors"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class HypothesisResult:
    """单个假设的完整生命周期。"""

    hypothesis: str
    iteration: int = 0           # 哪一轮产生的
    code: str = ""               # 生成的因子代码
    eval_result: Optional[FactorEvalResult] = None  # 评估结果
    is_sota: bool = False        # 是否进入 SOTA


@dataclass
class RDLoopResult:
    """R&D 闭环的完整输出。"""

    iterations: int = 0
    total_hypotheses: int = 0
    total_evaluated: int = 0
    sota_factors: List[HypothesisResult] = field(default_factory=list)
    all_results: List[HypothesisResult] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    elapsed_seconds: float = 0.0

    def leaderboard_markdown(self) -> str:
        """生成排行榜 Markdown。"""
        if not self.sota_factors:
            return "## R&D 闭环 - 未发现有效因子\n\n无。\n"

        lines = [
            "## R&D 因子发现闭环 - 排行榜",
            "",
            f"运行时间: {self.started_at} → {self.finished_at}",
            f"迭代轮数: {self.iterations} | 总假设: {self.total_hypotheses} | 有效因子: {len(self.sota_factors)}",
            "",
            "| # | 因子名 | 轮次 | 累计收益 | 夏普 | 胜率(1d) | IC | 综合分 | 假设摘要 |",
            "|---|--------|------|----------|------|----------|-----|--------|----------|",
        ]

        for i, h in enumerate(self.sota_factors, 1):
            e = h.eval_result
            if e is None:
                continue
            desc = h.hypothesis[:40] + "..." if len(h.hypothesis) > 40 else h.hypothesis
            lines.append(
                f"| {i} | {e.factor_name} | {h.iteration} | "
                f"{e.cumulative_return:.1f}% | {e.sharpe_ratio:.2f} | "
                f"{e.win_rate_1d:.0f}% | {e.ic_mean:.3f} | "
                f"{e.rank_score:.0f} | {desc} |"
            )

        lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# R&D Loop
# ---------------------------------------------------------------------------


_HYPOTHESIS_GEN_SYSTEM = """你是一个量化研究员，专长于 A 股因子挖掘。

你的任务是提出可实施的选股因子假设。每个假设应该：
1. 描述一个具体的选股逻辑（如"检测资金流入加速且量比放大的股票"）
2. 基于可获取的数据字段（资金流向、技术指标、筹码分布、融资融券、涨跌幅、换手率等）
3. 具有可量化的条件（如"主力净流入 > 0 且 量比 > 1.5"）
4. 1-3 句话，简洁明确

## 输出格式
每行一个假设，用 `- ` 开头。只输出假设，不要有其他内容。

## 现有因子参考（避免重复）
{existing_factors}

## 过往评估反馈（基于此改进）
{feedback}"""


class RDLoop:
    """R&D 闭环编排器。

    用法:
        loop = RDLoop(tushare_fetcher=fetcher, llm_adapter=adapter)
        result = loop.run(iterations=3, hypotheses_per_round=3)
        print(result.leaderboard_markdown())
    """

    def __init__(self, tushare_fetcher=None, llm_adapter=None, evaluator=None, coder=None):
        self._fetcher = tushare_fetcher
        self._adapter = llm_adapter
        self._evaluator = evaluator or FactorEvaluator(tushare_fetcher=tushare_fetcher)
        self._coder = coder or FactorCoder(llm_adapter=llm_adapter)
        self._sota: List[HypothesisResult] = []
        self._all_results: List[HypothesisResult] = []
        self._trade_dates: Optional[List[str]] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        iterations: int = 5,
        hypotheses_per_round: int = 3,
        initial_hypotheses: Optional[List[str]] = None,
        trade_dates: Optional[List[str]] = None,
        top_n_picks: int = 10,
        sota_threshold: float = 30.0,
    ) -> RDLoopResult:
        """运行完整 R&D 闭环。

        Args:
            iterations: 迭代轮数
            hypotheses_per_round: 每轮生成的假设数
            initial_hypotheses: 初始假设列表，None 则由 LLM 生成
            trade_dates: 评估用交易日列表
            top_n_picks: 每日选取 top N 只股票
            sota_threshold: SOTA 准入的最低 rank_score

        Returns:
            RDLoopResult
        """
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        t0 = time.time()

        self._sota = []
        self._all_results = []
        self._trade_dates = trade_dates

        logger.info(
            "[RDLoop] 启动 R&D 闭环: iterations=%d, hypotheses_per_round=%d",
            iterations, hypotheses_per_round,
        )

        # 加载状态（断点续跑）
        self._load_state()

        # 初始假设
        if initial_hypotheses is None and not self._sota:
            initial_hypotheses = self._generate_hypotheses(
                n=hypotheses_per_round, feedback="首次运行，基于现有数据源提出创新因子"
            )
        current_hypotheses = initial_hypotheses or []

        for iteration in range(1, iterations + 1):
            logger.info("[RDLoop] === 第 %d/%d 轮 ===", iteration, iterations)

            if not current_hypotheses:
                # 从 SOTA 反馈中生成新一轮假设
                fb = self._build_feedback()
                current_hypotheses = self._generate_hypotheses(
                    n=hypotheses_per_round, feedback=fb
                )

            if not current_hypotheses:
                logger.warning("[RDLoop] 第 %d 轮无假设，结束", iteration)
                break

            round_results = self._evaluate_round(
                current_hypotheses, iteration, top_n=top_n_picks
            )
            self._all_results.extend(round_results)

            # 更新 SOTA
            for hr in round_results:
                if hr.eval_result and hr.eval_result.success:
                    score = hr.eval_result.rank_score
                    if score >= sota_threshold:
                        if self._is_novel(hr):
                            hr.is_sota = True
                            self._sota.append(hr)
                            logger.info(
                                "[RDLoop] 新 SOTA: %s (score=%.1f)",
                                hr.eval_result.factor_name, score,
                            )
                        else:
                            logger.info(
                                "[RDLoop] 因子 %s 与已有 SOTA 重复，跳过",
                                hr.eval_result.factor_name,
                            )

            # 排序 SOTA
            self._sota.sort(
                key=lambda x: x.eval_result.rank_score if x.eval_result else 0,
                reverse=True,
            )

            # 保存状态
            self._save_state()

            # 为下一轮生成假设
            current_hypotheses = self._generate_hypotheses(
                n=hypotheses_per_round,
                feedback=self._build_feedback(),
            )

            logger.info(
                "[RDLoop] 第 %d 轮完成: 评估 %d 个, SOTA %d 个",
                iteration, len(round_results), len(self._sota),
            )

        elapsed = time.time() - t0
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        result = RDLoopResult(
            iterations=iterations,
            total_hypotheses=len(self._all_results),
            total_evaluated=sum(1 for h in self._all_results if h.eval_result),
            sota_factors=self._sota[:10],  # top 10
            all_results=self._all_results,
            started_at=started_at,
            finished_at=finished_at,
            elapsed_seconds=elapsed,
        )

        # 持久化 SOTA 因子到 factors 目录，供 auto-discovery 自动注册
        self._persist_sota_factors()

        # 保存最终报告
        self._save_report(result)

        logger.info(
            "[RDLoop] R&D 闭环完成: %d 轮, %d 个假设, %d 个 SOTA, 耗时 %.1fs",
            iterations, result.total_hypotheses, len(result.sota_factors), elapsed,
        )

        return result

    # ------------------------------------------------------------------
    # Hypothesis generation
    # ------------------------------------------------------------------

    def _generate_hypotheses(self, n: int, feedback: str) -> List[str]:
        """使用 LLM 生成新一轮假设。"""
        existing = "（暂无）"
        if self._sota:
            items = []
            for h in self._sota[:5]:
                e = h.eval_result
                name = e.factor_name if e else "?"
                score = e.rank_score if e else 0
                items.append(f"- {name} (综合分 {score:.0f}): {h.hypothesis[:60]}")
            existing = "\n".join(items)

        system = _HYPOTHESIS_GEN_SYSTEM.format(
            existing_factors=existing,
            feedback=feedback,
        )

        user = (
            f"请提出 {n} 个新的 A 股选股因子假设。每个假设 1-3 句话，用 '- ' 开头。"
            f"优先考虑: 结合多个数据源的复合因子、利用技术指标背离的因子、"
            f"或基于资金流向 + 筹码分布的组合信号。"
        )

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

        try:
            response = self._adapter.call_text(messages, temperature=0.7)
        except Exception as e:
            logger.warning("[RDLoop] 假设生成 LLM 调用失败: %s", e)
            return []

        if not response.content:
            return []

        return self._parse_hypotheses(response.content, n)

    @staticmethod
    def _parse_hypotheses(text: str, max_n: int) -> List[str]:
        """从 LLM 回复中提取假设列表。"""
        hypotheses = []
        for line in text.strip().split("\n"):
            line = line.strip()
            # 匹配 "- 假设内容" 或 "1. 假设内容"
            if line.startswith("- ") or line.startswith("* "):
                h = line[2:].strip()
            elif line and line[0].isdigit() and ". " in line[:4]:
                h = line.split(". ", 1)[1].strip()
            else:
                continue
            if h and len(h) > 5:
                hypotheses.append(h)
        return hypotheses[:max_n]

    # ------------------------------------------------------------------
    # Evaluation round
    # ------------------------------------------------------------------

    def _evaluate_round(
        self, hypotheses: List[str], iteration: int, top_n: int = 10
    ) -> List[HypothesisResult]:
        """评估一轮假设：生成代码 → 回测。"""
        results: List[HypothesisResult] = []

        for h in hypotheses:
            hr = HypothesisResult(hypothesis=h, iteration=iteration)
            logger.info("[RDLoop] 生成代码: %s...", h[:50])

            code = self._coder.generate(h)
            if not code:
                logger.warning("[RDLoop] 代码生成失败: %s...", h[:50])
                hr.code = ""
                results.append(hr)
                continue

            hr.code = code

            logger.info("[RDLoop] 评估因子: %s...", h[:50])
            eval_result = self._evaluator.evaluate(
                code, hypothesis=h, trade_dates=self._trade_dates, top_n=top_n,
            )
            hr.eval_result = eval_result
            results.append(hr)

        return results

    # ------------------------------------------------------------------
    # SOTA management
    # ------------------------------------------------------------------

    def _is_novel(self, hr: HypothesisResult) -> bool:
        """检查因子是否与已有 SOTA 足够不同。

        简单策略：rank_score 差距 > 阈值则认为不同。
        更复杂的去重（基于持仓相关性）可在后续迭代中加入。
        """
        if not hr.eval_result or not self._sota:
            return True

        new_score = hr.eval_result.rank_score
        for existing in self._sota:
            if existing.eval_result:
                old_score = existing.eval_result.rank_score
                # 如果综合分极其接近（差 < 1），认为是重复
                if abs(new_score - old_score) < 1.0:
                    return False

        return True

    def _build_feedback(self) -> str:
        """基于当前 SOTA 构建反馈文本。"""
        if not self._sota:
            return "尚无有效 SOTA 因子。请提出基于资金流向、技术指标或筹码分布的基础因子。"

        lines = []
        for h in self._sota[:5]:
            e = h.eval_result
            if e is None:
                continue
            lines.append(
                f"- {e.factor_name}: 累计收益 {e.cumulative_return:.1f}%, "
                f"夏普 {e.sharpe_ratio:.2f}, 胜率 {e.win_rate_1d:.0f}%, "
                f"IC {e.ic_mean:.3f}, 综合分 {e.rank_score:.0f}"
            )

        recent = self._all_results[-5:] if self._all_results else []
        failures = [
            f"- {h.hypothesis[:50]}: {h.eval_result.error}"
            for h in recent
            if h.eval_result and not h.eval_result.success and h.eval_result.error
        ]

        parts = ["当前最佳因子:", "\n".join(lines)]
        if failures:
            parts.append("\n最近失败的因子:\n" + "\n".join(failures))
        parts.append("\n请在此基础上提出改进的或全新的因子假设。")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _save_state(self) -> None:
        """保存当前状态，支持断点续跑。"""
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
        state = {
            "sota": [
                {
                    "hypothesis": h.hypothesis,
                    "iteration": h.iteration,
                    "code": h.code,
                    "eval": self._eval_to_dict(h.eval_result) if h.eval_result else None,
                }
                for h in self._sota
            ],
            "updated_at": datetime.now().isoformat(),
        }
        state_path = _STATE_DIR / "rd_loop_state.json"
        try:
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.warning("[RDLoop] 保存状态失败: %s", e)

    def _load_state(self) -> None:
        """加载上次状态。"""
        state_path = _STATE_DIR / "rd_loop_state.json"
        if not state_path.exists():
            return
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            for s in data.get("sota", []):
                hr = HypothesisResult(
                    hypothesis=s.get("hypothesis", ""),
                    iteration=s.get("iteration", 0),
                    code=s.get("code", ""),
                )
                if s.get("eval"):
                    hr.eval_result = FactorEvalResult(
                        factor_name=s["eval"].get("factor_name", ""),
                        hypothesis=hr.hypothesis,
                        code=hr.code,
                        success=True,
                        cumulative_return=s["eval"].get("cumulative_return", 0),
                        sharpe_ratio=s["eval"].get("sharpe_ratio", 0),
                        win_rate_1d=s["eval"].get("win_rate_1d", 0),
                        ic_mean=s["eval"].get("ic_mean", 0),
                        rank_score=s["eval"].get("rank_score", 0),
                    )
                    hr.is_sota = True
                self._sota.append(hr)
            logger.info("[RDLoop] 从状态恢复 %d 个 SOTA 因子", len(self._sota))
        except Exception as e:
            logger.warning("[RDLoop] 加载状态失败: %s", e)

    @staticmethod
    def _eval_to_dict(e: FactorEvalResult) -> dict:
        return {
            "factor_name": e.factor_name,
            "cumulative_return": e.cumulative_return,
            "sharpe_ratio": e.sharpe_ratio,
            "win_rate_1d": e.win_rate_1d,
            "ic_mean": e.ic_mean,
            "rank_score": e.rank_score,
            "total_days": e.total_days,
            "total_picks": e.total_picks,
        }

    def _save_report(self, result: RDLoopResult) -> None:
        """保存最终报告到 reports/ 目录。"""
        _RD_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        md = result.leaderboard_markdown()

        # 追加各 SOTA 因子详情
        md += "\n## SOTA 因子详情\n\n"
        for i, h in enumerate(result.sota_factors, 1):
            e = h.eval_result
            if e is None:
                continue
            md += f"### #{i} {e.factor_name}\n\n"
            md += f"**假设**: {h.hypothesis}\n\n"
            md += f"**评估**: 累计收益 {e.cumulative_return:.1f}% | "
            md += f"夏普 {e.sharpe_ratio:.2f} | "
            md += f"胜率(1d) {e.win_rate_1d:.0f}% | "
            md += f"IC {e.ic_mean:.3f} | "
            md += f"综合分 {e.rank_score:.0f}\n\n"
            md += "<details>\n<summary>因子代码</summary>\n\n"
            md += f"```python\n{h.code[:2000]}\n```\n"
            md += "\n</details>\n\n"
            md += "---\n\n"

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = _RD_REPORTS_DIR / f"rd_loop_{ts}.md"
        try:
            report_path.write_text(md, encoding="utf-8")
            logger.info("[RDLoop] 报告已保存: %s", report_path)
        except Exception as e:
            logger.warning("[RDLoop] 保存报告失败: %s", e)

    # ------------------------------------------------------------------
    # Factor persistence
    # ------------------------------------------------------------------

    def _persist_sota_factors(self) -> None:
        """将 SOTA 因子代码写入 src/discovery/factors/ 目录。

        文件命名为 rd_gen_{factor_name}.py。
        下次 auto-discovery 运行时，__init__.py 的自动发现机制会加载这些因子。
        已存在的同名文件会被跳过（不覆盖手动修改的版本）。
        """
        if not self._sota:
            return

        _FACTORS_DIR.mkdir(parents=True, exist_ok=True)
        written = 0

        for hr in self._sota:
            if not hr.eval_result or not hr.code:
                continue

            name = hr.eval_result.factor_name
            if not name:
                continue

            # 文件名：rd_gen_{factor_name}.py
            safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
            file_path = _FACTORS_DIR / f"rd_gen_{safe_name}.py"

            if file_path.exists():
                logger.info("[RDLoop] 因子文件已存在，跳过: %s", file_path.name)
                continue

            # 在代码顶部加生成元信息注释
            header = (
                f"# -*- coding: utf-8 -*-\n"
                f"# R&D 闭环自动生成 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"# 假设: {hr.hypothesis}\n"
                f"# 综合评分: {hr.eval_result.rank_score:.0f}\n"
                f"# 累计收益: {hr.eval_result.cumulative_return:.1f}%  "
                f"夏普: {hr.eval_result.sharpe_ratio:.2f}  "
                f"胜率: {hr.eval_result.win_rate_1d:.0f}%\n"
                f"\n"
            )

            try:
                file_path.write_text(header + hr.code, encoding="utf-8")
                logger.info(
                    "[RDLoop] SOTA 因子已持久化: %s (score=%.0f)",
                    file_path.name, hr.eval_result.rank_score,
                )
                written += 1
            except Exception as e:
                logger.warning("[RDLoop] 写入因子文件失败 %s: %s", file_path, e)

        if written > 0:
            logger.info(
                "[RDLoop] 已持久化 %d 个 SOTA 因子到 %s，"
                "下次 auto-discovery 将自动注册",
                written, _FACTORS_DIR,
            )
