# -*- coding: utf-8 -*-
"""因子 IC 追踪器。

追踪各因子得分与未来 N 日收益的 Rank IC（Spearman 相关系数），
用于评估和淘汰低效因子。

数据存储：
- discovery_reports/ic_tracking/ic_history_{eval_days}d.json
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_IC_HISTORY_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports" / "ic_tracking"


class ICTracker:
    """追踪各因子得分与未来收益的 Rank IC。"""

    def __init__(self, eval_days: int = 5):
        self.eval_days = eval_days
        self.history_file = _IC_HISTORY_DIR / f"ic_history_{eval_days}d.json"

    def evaluate(
        self, factor_scores: Dict[str, pd.Series], trade_date: str
    ) -> Dict[str, float]:
        """计算各因子在当前截面的 IC。

        Args:
            factor_scores: {因子名: pd.Series(ts_code -> 分数)}
            trade_date: 交易日期 (YYYYMMDD)

        Returns:
            {因子名: IC 值}
        """
        # 获取候选股票列表
        candidate_codes: set = set()
        for scores in factor_scores.values():
            candidate_codes.update(scores.index.tolist())
        candidate_codes.discard(None)

        future_returns = self._get_future_returns(trade_date, list(candidate_codes))
        if future_returns is None or len(future_returns) < 30:
            return {name: 0.0 for name in factor_scores}

        results = {}
        for name, scores in factor_scores.items():
            common = scores.index.intersection(future_returns.index)
            if len(common) < 30:
                results[name] = 0.0
                continue

            try:
                from scipy.stats import spearmanr

                ic, _ = spearmanr(
                    scores.reindex(common).fillna(0),
                    future_returns.reindex(common).fillna(0),
                )
                results[name] = round(float(ic), 4) if not np.isnan(ic) else 0.0
            except Exception as e:
                logger.debug(f"[IC] {name} IC计算失败: {e}")
                results[name] = 0.0

        self._save_ic(trade_date, results)
        return results

    def _get_future_returns(self, trade_date: str, ts_codes: Optional[List[str]] = None) -> Optional[pd.Series]:
        """获取个股未来 N 日收益率。

        Args:
            trade_date: 交易日期 (YYYYMMDD)
            ts_codes: 候选股票列表（可选，默认使用全市场）
        """
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher()
            if not tf.is_available():
                return None

            # 如果提供了 ts_codes，获取这些股票的未来收益
            if ts_codes:
                returns_map: Dict[str, float] = {}
                for ts_code in ts_codes[:100]:  # 限制数量避免过慢
                    try:
                        clean_code = ts_code.split(".")[0] if "." in ts_code else ts_code
                        df = tf.get_daily_data(clean_code, start_date=trade_date, days=self.eval_days + 5)
                        if df is not None and len(df) >= self.eval_days:
                            prices = pd.to_numeric(df["close"], errors="coerce").dropna()
                            if len(prices) >= self.eval_days + 1:
                                # 未来 N 日收益率：(第N日收盘价 - 今日收盘价) / 今日收盘价
                                ret = (prices.iloc[self.eval_days] - prices.iloc[0]) / prices.iloc[0]
                                if not np.isnan(ret):
                                    returns_map[ts_code] = ret
                    except Exception:
                        continue

                if len(returns_map) >= 10:
                    return pd.Series(returns_map)
        except Exception as e:
            logger.debug(f"[IC] 获取个股收益失败: {e}")

        # Fallback: 使用上证指数作为市场收益代理
        try:
            index_code = "000001.SH"
            df = tf.get_daily_data(index_code, start_date=trade_date, days=self.eval_days + 5)
            if df is None or len(df) < self.eval_days:
                return None

            returns = pd.to_numeric(df["pct_chg"], errors="coerce").dropna()
            if len(returns) < self.eval_days:
                return None

            market_return = returns.iloc[: self.eval_days].mean()
            return pd.Series(dtype=float)
        except Exception as e:
            logger.debug(f"[IC] 获取市场收益失败: {e}")
            return None

    def _save_ic(self, trade_date: str, ic_values: Dict[str, float]) -> None:
        _IC_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        history = self._load_history()
        history[trade_date] = ic_values
        self.history_file.write_text(
            json.dumps(history, ensure_ascii=False, indent=2)
        )

    def _load_history(self) -> Dict:
        if self.history_file.exists():
            try:
                return json.loads(self.history_file.read_text())
            except Exception:
                pass
        return {}

    def get_summary(self, window: int = 30) -> Dict[str, Dict]:
        """获取近 N 日 IC 汇总统计。"""
        history = self._load_history()
        if not history:
            return {}

        dates = sorted(history.keys())[-window:]
        if not dates:
            return {}

        all_factors: set = set()
        for d in dates:
            all_factors.update(history[d].keys())

        summary = {}
        for factor in all_factors:
            ic_series = [history[d].get(factor, 0) for d in dates]
            ic_series = [x for x in ic_series if x != 0]
            if ic_series:
                summary[factor] = {
                    "ic_mean": round(float(np.mean(ic_series)), 4),
                    "ic_std": round(float(np.std(ic_series)), 4),
                    "ic_positive_rate": round(float(np.mean([x > 0 for x in ic_series])), 2),
                    "valid_days": len(ic_series),
                }
        return summary

    def format_ic_report(self, window: int = 30) -> str:
        """生成 IC 报告 Markdown。"""
        summary = self.get_summary(window)
        if not summary:
            return f"## 因子 IC 报告（近 {window} 日）\n\n暂无数据。\n"

        lines = [f"## 因子 IC 报告（近 {window} 日）", ""]
        lines.append("| 因子 | IC均值 | IC标准差 | 正IC率 | 有效天数 | 评价 |")
        lines.append("|------|--------|---------|--------|----------|------|")

        for factor, stats in sorted(summary.items(), key=lambda x: -x[1]["ic_mean"]):
            rate = stats["ic_positive_rate"]
            if stats["ic_mean"] > 0.05 and rate > 0.6:
                label = "✅ 有效"
            elif stats["ic_mean"] > 0.02 and rate > 0.5:
                label = "⚠️  一般"
            elif stats["ic_mean"] < 0:
                label = "❌ 反向"
            else:
                label = "➖  待观察"

            lines.append(
                f"| {factor} | {stats['ic_mean']:.4f} | {stats['ic_std']:.4f} "
                f"| {rate:.0%} | {stats['valid_days']} | {label} |"
            )
        return "\n".join(lines)


if __name__ == "__main__":
    # 测试
    tracker = ICTracker(eval_days=5)
    print(tracker.format_ic_report(window=7))