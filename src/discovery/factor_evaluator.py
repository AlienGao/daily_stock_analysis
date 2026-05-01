# -*- coding: utf-8 -*-
"""因子评估器 —— 对新生成的因子代码进行历史回测评估。

动态加载 LLM 生成的因子代码，在历史交易日的真实数据上模拟选股，
计算前向收益、胜率、夏普比率等指标，生成结构化反馈供下一轮迭代。
"""

import ast
import importlib.util
import logging
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FactorEvalResult:
    """单次因子评估结果。"""

    factor_name: str = ""
    hypothesis: str = ""
    code: str = ""
    success: bool = False
    error: Optional[str] = None

    # 汇总指标
    total_days: int = 0
    total_picks: int = 0
    avg_forward_return_1d: float = 0.0   # 次日平均收益
    win_rate_1d: float = 0.0             # 次日胜率
    avg_forward_return_3d: float = 0.0   # 3日后平均收益
    win_rate_3d: float = 0.0
    cumulative_return: float = 0.0       # 等权滚动复利（%）
    sharpe_ratio: float = 0.0            # 年化夏普（近似）
    max_drawdown: float = 0.0            # 最大回撤（%）
    ic_mean: float = 0.0                 # 因子 Rank IC 均值

    # 逐日明细
    daily_returns: List[float] = field(default_factory=list)
    daily_dates: List[str] = field(default_factory=list)

    # 排名
    rank_score: float = 0.0  # 综合排名分（越高越好）


# ---------------------------------------------------------------------------
# Code safety validator
# ---------------------------------------------------------------------------

_DANGEROUS_BUILTINS = {"__import__", "eval", "exec", "compile", "open", "input"}
_DANGEROUS_MODULES = {"os", "subprocess", "socket", "shutil", "ctypes", "signal",
                       "multiprocessing", "threading", "requests", "urllib",
                       "http", "ftplib", "smtplib", "telnetlib", "pickle"}


def _validate_code_safety(code: str) -> tuple:
    """AST 检查：拒绝危险的内置函数、导入和系统调用。"""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return False, f"语法错误: {e}"

    for node in ast.walk(tree):
        # 禁止危险导入
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = getattr(node, "module", None) or ""
            for name in node.names:
                full = f"{module}.{name.name}" if module else name.name
                top = full.split(".")[0]
                if top in _DANGEROUS_MODULES:
                    return False, f"禁止导入: {full}"

        # 禁止危险内置函数调用
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in _DANGEROUS_BUILTINS:
                return False, f"禁止调用: {func.id}()"

        # 禁止访问特殊属性
        if isinstance(node, ast.Attribute):
            if node.attr.startswith("__") and node.attr.endswith("__"):
                return False, f"禁止访问特殊属性: {node.attr}"

    return True, ""


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


class FactorEvaluator:
    """因子回测评估器。

    动态加载生成的因子代码，在历史数据上模拟选股并计算前向收益指标。
    """

    def __init__(self, tushare_fetcher=None):
        self._fetcher = tushare_fetcher
        self._price_cache: Dict[str, Dict[str, Dict[str, float]]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(
        self,
        code: str,
        hypothesis: str = "",
        trade_dates: Optional[List[str]] = None,
        top_n: int = 10,
        forward_days: tuple = (1, 3),
    ) -> FactorEvalResult:
        """评估单个因子代码。

        Args:
            code: 完整的 Python 因子类代码（含 BaseFactor 子类定义）
            hypothesis: 该因子的假设描述
            trade_dates: 评估用交易日期列表 (YYYYMMDD)，None 则用最近 20 个交易日
            top_n: 每日选取 top N 只股票
            forward_days: 前向收益计算窗口

        Returns:
            FactorEvalResult
        """
        # 1. 安全检查
        safe, err = _validate_code_safety(code)
        if not safe:
            return FactorEvalResult(hypothesis=hypothesis, code=code, error=err)

        # 2. 动态加载因子类
        factor_cls = self._load_factor_class(code)
        if factor_cls is None:
            return FactorEvalResult(
                hypothesis=hypothesis, code=code,
                error="无法从代码中提取 BaseFactor 子类"
            )

        try:
            factor = factor_cls()
        except Exception as e:
            return FactorEvalResult(
                hypothesis=hypothesis, code=code,
                error=f"实例化因子失败: {e}"
            )

        result = FactorEvalResult(
            factor_name=factor.name or "unnamed",
            hypothesis=hypothesis,
            code=code,
            success=True,
        )

        # 3. 解析交易日
        if trade_dates is None:
            trade_dates = self._get_recent_trading_days(20)
        if len(trade_dates) < 3:
            result.error = f"交易日不足 ({len(trade_dates)}), 需要 >= 3"
            result.success = False
            return result

        # 4. 逐日模拟选股
        picks_by_date: Dict[str, List[str]] = {}
        all_codes: set = set()

        for td in trade_dates[:-max(forward_days)]:
            try:
                df = factor.fetch_data(td, tushare_fetcher=self._fetcher)
                if df is None or df.empty:
                    continue
                scores = factor.score(df, tushare_fetcher=self._fetcher)
                if scores is None or scores.empty:
                    continue
                top = scores.nlargest(top_n)
                codes = [idx.split(".")[0] if "." in str(idx) else str(idx)
                          for idx in top.index]
                if codes:
                    picks_by_date[td] = codes
                    all_codes.update(codes)
            except Exception as e:
                logger.debug("[FactorEval] %s on %s: %s", factor.name, td, e)
                continue

        if not picks_by_date:
            result.error = "所有交易日均无选股结果"
            result.success = False
            return result

        # 5. 获取价格数据
        self._fetch_prices(list(all_codes), trade_dates)

        # 6. 计算前向收益
        all_returns_1d: List[float] = []
        all_returns_3d: List[float] = []
        wins_1d = 0
        wins_3d = 0
        daily_returns: List[float] = []
        daily_dates: List[str] = []
        ic_values: List[float] = []

        for td, picks in picks_by_date.items():
            day_rets = []
            for code in picks:
                r1 = self._forward_return(code, td, 1)
                r3 = self._forward_return(code, td, 3)
                if r1 is not None:
                    all_returns_1d.append(r1)
                    day_rets.append(r1)
                    if r1 > 0:
                        wins_1d += 1
                if r3 is not None:
                    all_returns_3d.append(r3)
                    if r3 > 0:
                        wins_3d += 1

            if day_rets:
                daily_returns.append(sum(day_rets) / len(day_rets))
                daily_dates.append(td)

            # Rank IC: correlation between score rank and forward return
            try:
                scores = {}
                df = factor.fetch_data(td, tushare_fetcher=self._fetcher)
                if df is not None and not df.empty:
                    raw_scores = factor.score(df, tushare_fetcher=self._fetcher)
                    if raw_scores is not None and not raw_scores.empty:
                        for idx, s in raw_scores.items():
                            code = idx.split(".")[0] if "." in str(idx) else str(idx)
                            r = self._forward_return(code, td, 1)
                            if r is not None:
                                scores[code] = (s, r)
                        if len(scores) >= 5:
                            s_series = pd.Series({k: v[0] for k, v in scores.items()})
                            r_series = pd.Series({k: v[1] for k, v in scores.items()})
                            ic = s_series.rank().corr(r_series.rank())
                            if not pd.isna(ic):
                                ic_values.append(ic)
            except Exception:
                pass

        # 7. 计算汇总指标
        n_1d = len(all_returns_1d)
        result.total_days = len(daily_dates)
        result.total_picks = n_1d
        result.avg_forward_return_1d = sum(all_returns_1d) / n_1d * 100 if n_1d else 0
        result.win_rate_1d = wins_1d / n_1d * 100 if n_1d else 0
        result.daily_returns = daily_returns
        result.daily_dates = daily_dates

        n_3d = len(all_returns_3d)
        result.avg_forward_return_3d = sum(all_returns_3d) / n_3d * 100 if n_3d else 0
        result.win_rate_3d = wins_3d / n_3d * 100 if n_3d else 0

        # 滚动复利
        cum = 1.0
        peak = 1.0
        mdd = 0.0
        for r in daily_returns:
            cum *= (1 + r)
            if cum > peak:
                peak = cum
            dd = (cum - peak) / peak
            if dd < mdd:
                mdd = dd
        result.cumulative_return = (cum - 1) * 100
        result.max_drawdown = mdd * 100

        # 夏普（年化 ≈ 日收益均值/日收益标准差 * sqrt(250)）
        if len(daily_returns) >= 3:
            import numpy as np
            rets = np.array(daily_returns)
            mean_ret = rets.mean()
            std_ret = rets.std()
            result.sharpe_ratio = float(mean_ret / std_ret * np.sqrt(250)) if std_ret > 0 else 0.0
        else:
            result.sharpe_ratio = 0.0

        # IC 均值
        result.ic_mean = sum(ic_values) / len(ic_values) if ic_values else 0.0

        # 综合排名分: 50% 夏普 + 20% 累计收益 + 20% 胜率 + 10% IC
        result.rank_score = round(
            0.50 * max(0, result.sharpe_ratio) * 100
            + 0.20 * result.cumulative_return
            + 0.20 * result.win_rate_1d
            + 0.10 * abs(result.ic_mean) * 100,
            1,
        )

        logger.info(
            "[FactorEval] %s: ret_1d=%.2f%%, win_1d=%.0f%%, "
            "cum=%.2f%%, sharpe=%.2f, IC=%.3f, rank_score=%.1f",
            factor.name, result.avg_forward_return_1d, result.win_rate_1d,
            result.cumulative_return, result.sharpe_ratio, result.ic_mean,
            result.rank_score,
        )

        return result

    def format_feedback(self, result: FactorEvalResult) -> str:
        """将评估结果格式化为 LLM 可用的反馈文本。"""
        if not result.success:
            return f"因子评估失败: {result.error}"

        lines = [
            f"【因子评估反馈】{result.factor_name}",
            f"假设: {result.hypothesis}",
            "",
            f"评估天数: {result.total_days} | 总选股次数: {result.total_picks}",
            f"次日平均收益: {result.avg_forward_return_1d:.2f}%",
            f"次日胜率: {result.win_rate_1d:.1f}%",
            f"3日平均收益: {result.avg_forward_return_3d:.2f}%",
            f"3日胜率: {result.win_rate_3d:.1f}%",
            f"累计收益(复利): {result.cumulative_return:.2f}%",
            f"年化夏普: {result.sharpe_ratio:.2f}",
            f"最大回撤: {result.max_drawdown:.2f}%",
            f"Rank IC 均值: {result.ic_mean:.3f}",
            f"综合排名分: {result.rank_score:.1f}",
            "",
            "请根据以上指标，提出改进方向或新的因子假设。",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_factor_class(self, code: str) -> Optional[type]:
        """从代码字符串动态加载 BaseFactor 子类。"""
        tmp_path = None
        try:
            # 写临时文件
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False,
                prefix="factor_", dir="/tmp",
                encoding="utf-8",
            )
            tmp.write(code)
            tmp.flush()
            tmp_path = Path(tmp.name)
            tmp.close()

            # 动态导入
            module_name = f"factor_gen_{tmp_path.stem}"
            spec = importlib.util.spec_from_file_location(module_name, tmp_path)
            if spec is None or spec.loader is None:
                return None
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # 找到 BaseFactor 子类
            for name in dir(module):
                obj = getattr(module, name)
                if (
                    isinstance(obj, type)
                    and issubclass(obj, BaseFactor)
                    and obj is not BaseFactor
                ):
                    return obj

            return None
        except Exception as e:
            logger.warning("[FactorEval] 加载因子类失败: %s", e)
            return None
        finally:
            if tmp_path and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def _forward_return(self, code: str, trade_date: str, n_days: int) -> Optional[float]:
        """计算 code 在 trade_date 后第 n 个交易日的收益率。"""
        # 找到起始日和结束日
        start_prices = self._price_cache.get(trade_date, {}).get(code, {})
        start_price = start_prices.get("close")
        if not start_price:
            return None

        # 在所有已缓存的交易日中找到 n 天后的日期
        sorted_dates = sorted(self._price_cache.keys())
        try:
            idx = sorted_dates.index(trade_date)
        except ValueError:
            return None

        future_idx = idx + n_days
        if future_idx >= len(sorted_dates):
            return None

        future_date = sorted_dates[future_idx]
        future_prices = self._price_cache.get(future_date, {}).get(code, {})
        end_price = future_prices.get("close")
        if not end_price or start_price <= 0:
            return None

        return (end_price - start_price) / start_price

    def _fetch_prices(self, codes: List[str], trading_days: List[str]) -> None:
        """批量获取价格数据，优先本地 DB，回退 Tushare。"""
        if not codes or not trading_days:
            return

        db_codes = set()
        try:
            from src.storage import DatabaseManager, StockDaily
            db = DatabaseManager()
            session = db.get_session()
            try:
                rows = (
                    session.query(StockDaily)
                    .filter(
                        StockDaily.code.in_(codes),
                        StockDaily.date.in_([d for d in trading_days]),
                    )
                    .all()
                )
                for row in rows:
                    ds = row.date.strftime("%Y%m%d") if isinstance(row.date, date) else str(row.date)[:8]
                    self._price_cache.setdefault(ds, {})[row.code] = {
                        "open": float(row.open) if row.open else 0.0,
                        "close": float(row.close) if row.close else 0.0,
                    }
                db_codes = {row.code for row in rows}
            finally:
                session.close()
        except Exception as e:
            logger.debug("[FactorEval] DB 查询失败: %s", e)

        missing = [c for c in codes if c not in db_codes]
        if not missing or self._fetcher is None:
            return

        try:
            ts_codes = []
            for c in missing:
                if c.isdigit() and len(c) == 6:
                    ts_codes.append(f"{c}.SH")
                    ts_codes.append(f"{c}.SZ")
                else:
                    ts_codes.append(c)

            for td in trading_days:
                existing = set((self._price_cache.get(td) or {}).keys())
                if set(missing).issubset(existing):
                    continue
                try:
                    df = self._fetcher._call_api_with_rate_limit(
                        "daily", ts_code=",".join(ts_codes[:200]),
                        trade_date=td,
                    )
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            ts = str(row.get("ts_code", ""))
                            code = ts.split(".")[0] if "." in ts else ts
                            self._price_cache.setdefault(td, {})[code] = {
                                "open": float(row["open"]) if pd.notna(row.get("open")) else 0.0,
                                "close": float(row["close"]) if pd.notna(row.get("close")) else 0.0,
                            }
                except Exception:
                    pass
        except Exception as e:
            logger.debug("[FactorEval] Tushare 取价失败: %s", e)

    def _get_recent_trading_days(self, n: int) -> List[str]:
        """获取最近 n 个交易日。"""
        try:
            import exchange_calendars as xcals
            cal = xcals.get_calendar("XSHG")
            end = date.today()
            start = end - timedelta(days=max(n * 3, 60))
            sessions = cal.sessions_in_range(
                start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
            )
            days = [s.strftime("%Y%m%d") for s in sessions]
            return days[-n:] if len(days) > n else days
        except Exception:
            pass

        if self._fetcher:
            try:
                cal_df = self._fetcher._call_api_with_rate_limit(
                    "trade_cal", exchange="SSE",
                    start_date="20200101",
                    end_date=date.today().strftime("%Y%m%d"),
                    is_open="1",
                )
                if cal_df is not None and not cal_df.empty:
                    all_days = sorted(cal_df["cal_date"].tolist())
                    return all_days[-n:]
            except Exception:
                pass

        # Fallback: all weekdays
        days = []
        d = date.today() - timedelta(days=1)
        while len(days) < n:
            if d.weekday() < 5:
                days.append(d.strftime("%Y%m%d"))
            d -= timedelta(days=1)
        return list(reversed(days))
