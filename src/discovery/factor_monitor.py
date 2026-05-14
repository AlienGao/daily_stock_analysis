# -*- coding: utf-8 -*-
"""因子监控器 (Factor Monitor).

追踪每个因子独立选股的表现，用于评估因子质量。

数据存储：
- discovery_reports/factor_monitor/picks_{trade_date}.json  每日选股快照
- discovery_reports/factor_monitor/performance.json         累计表现历史

工作流：
  1. record_picks() → 盘后每个因子 Top N 记录到 picks JSON
  2. backfill()    → 回填 T-N 的 picks 在 N 日后的实际收益
  3. get_performance() → 读取历史表现，计算均值/胜率
  4. format_report()   → 生成 Markdown 因子监控报告
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_MONITOR_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "discovery_reports"
    / "factor_monitor"
)


class FactorMonitor:
    """追踪各因子独立选股的 forward performance。"""

    def __init__(self, top_n: int = 20, eval_days: int = 5):
        self.top_n = top_n
        self.eval_days = eval_days
        self.picks_dir = _MONITOR_DIR / "picks"
        self.perf_file = _MONITOR_DIR / "performance.json"

    # ------------------------------------------------------------------
    # Picks
    # ------------------------------------------------------------------

    def record_picks(
        self, factor_scores: Dict[str, pd.Series], trade_date: str, mode: str = "postmarket"
    ) -> Optional[str]:
        """记录每个因子的 Top N 选股。

        Args:
            factor_scores: {因子名: pd.Series(ts_code → 原始得分 0-100)}
            trade_date: 交易日期 YYYYMMDD
            mode: "intraday" 或 "postmarket"，用于区分盘中/盘后同名因子

        Returns:
            写入的文件路径，或 None
        """
        if not factor_scores:
            return None

        picks: Dict[str, list] = {}
        for name, scores in factor_scores.items():
            if scores is None or scores.empty:
                picks[name] = []
                continue
            clean = scores.dropna()
            if clean.empty:
                picks[name] = []
                continue
            top = clean.nlargest(self.top_n)
            picks[name] = [
                {"ts_code": str(c), "score": round(float(s), 1)}
                for c, s in top.items()
            ]

        self.picks_dir.mkdir(parents=True, exist_ok=True)
        out = {
            "trade_date": trade_date,
            "mode": mode,
            "top_n": self.top_n,
            "eval_days": self.eval_days,
            "factors": picks,
            "recorded_at": datetime.now().isoformat(),
        }
        filepath = self.picks_dir / f"picks_{mode}_{trade_date}.json"
        filepath.write_text(json.dumps(out, ensure_ascii=False, indent=2))
        logger.info(
            "[FactorMonitor] 已记录 %d 个因子选股（%s）→ %s", len(picks), mode, filepath.name
        )
        return str(filepath)

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    def detect_factor_changes(self, current_factors: List[str], mode: str) -> bool:
        """检测因子是否发生变化（新增、改名、删除）。

        Args:
            current_factors: 当前活跃因子名列表
            mode: "intraday" 或 "postmarket"

        Returns:
            True 表示因子有变化，需要重跑
        """
        historical_factors: set = set()
        for fp in self.picks_dir.glob(f"picks_{mode}_*.json"):
            try:
                data = json.loads(fp.read_text())
                historical_factors.update(data.get("factors", {}).keys())
            except Exception:
                continue

        if not historical_factors:
            return False  # 没有历史数据，不需要重跑

        current_set = set(current_factors)
        added = current_set - historical_factors
        removed = historical_factors - current_set

        if added or removed:
            if added:
                logger.info("[FactorMonitor] 检测到新增因子 (%s): %s", mode, ", ".join(added))
            if removed:
                logger.info("[FactorMonitor] 检测到移除因子 (%s): %s", mode, ", ".join(removed))
            return True
        return False

    def replay_history(self, engine, mode: str, days: int = 5) -> int:
        """重跑最近 N 天的发现，替换旧 picks 文件。

        当因子发生变化（新增/改名/删除）时调用，确保 picks 数据与当前因子一致。

        Args:
            engine: StockDiscoveryEngine 实例
            mode: "intraday" 或 "postmarket"
            days: 回放天数（默认 5）

        Returns:
            成功回放的天数
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        # 获取最近 N 个交易日
        with db.get_session() as s:
            from sqlalchemy import text
            rows = s.execute(
                text("SELECT DISTINCT date FROM stock_daily ORDER BY date DESC LIMIT :limit"),
                {"limit": days + 2},  # 多取几天，避免节假日不够
            ).fetchall()

        if not rows:
            logger.warning("[FactorMonitor] 无交易日数据，跳过回放")
            return 0

        trade_dates = sorted([str(r[0].strftime("%Y%m%d")) if hasattr(r[0], 'strftime') else str(r[0]).replace("-", "") for r in rows], reverse=True)[:days]

        success = 0
        for trade_date in trade_dates:
            try:
                # 删除旧 picks 文件（如有）
                old_file = self.picks_dir / f"picks_{mode}_{trade_date}.json"
                if old_file.exists():
                    old_file.unlink()
                    logger.info("[FactorMonitor] 删除旧 picks: %s", old_file.name)

                # 重跑发现
                results = engine.discover(mode=mode, trade_date=trade_date)
                if results:
                    success += 1
                    logger.info("[FactorMonitor] 回放 %s %s 完成: %d 只股票", mode, trade_date, len(results))
                else:
                    logger.warning("[FactorMonitor] 回放 %s %s 无结果", mode, trade_date)
            except Exception as e:
                logger.warning("[FactorMonitor] 回放 %s %s 失败: %s", mode, trade_date, e)

        if success:
            logger.info("[FactorMonitor] 回放完成: %s 模式 %d/%d 天成功", mode, success, len(trade_dates))
        return success

    def backfill(self, trade_date: str) -> Optional[List[dict]]:
        """回填所有未评估的 picks，使用当前 trade_date 作为评估终点。

        对每个尚未评估的 pick_date，计算 pick_date → trade_date 的实际收益。

        Args:
            trade_date: 当前交易日期 YYYYMMDD

        Returns:
            新增的 performance 条目列表，无待评估 picks 时返回 None
        """
        evaluated = self._evaluated_pick_dates()

        pending: List[Tuple[str, str, Path]] = []
        for fp in sorted(self.picks_dir.glob("picks_*.json")):
            # picks_{mode}_{trade_date}.json
            stem = fp.stem  # e.g. picks_postmarket_20260508
            parts = stem.split("_", 1)  # ['picks', 'postmarket_20260508']
            if len(parts) < 2:
                continue
            mode_date = parts[1]  # 'postmarket_20260508'
            mode_parts = mode_date.rsplit("_", 1)
            if len(mode_parts) < 2:
                continue
            mode = mode_parts[0]  # 'postmarket'
            pick_date = mode_parts[1]  # '20260508'
            key = f"{mode}_{pick_date}"
            if key in evaluated or pick_date >= trade_date:
                continue
            pending.append((mode, pick_date, fp))

        if not pending:
            return None

        results: List[dict] = []
        for mode, pick_date, fp in pending:
            try:
                picks_data = json.loads(fp.read_text())
            except Exception:
                logger.warning("[FactorMonitor] 读取 %s 失败，跳过", fp.name)
                continue

            factor_picks = picks_data.get("factors", {})
            actual_eval_days = picks_data.get("eval_days", self.eval_days)

            all_codes: List[str] = []
            for plist in factor_picks.values():
                for p in plist:
                    all_codes.append(p["ts_code"])

            if not all_codes:
                continue

            fwd = self._get_forward_returns(all_codes, pick_date, trade_date)
            if not fwd:
                logger.warning(
                    "[FactorMonitor] 无法获取 %s→%s 的收益数据", pick_date, trade_date
                )
                continue

            perf_factors: Dict[str, dict] = {}
            for name, plist in factor_picks.items():
                returns = []
                for p in plist:
                    ret = fwd.get(p["ts_code"])
                    if ret is not None:
                        returns.append(ret)

                if returns:
                    arr = np.array(returns)
                    perf_factors[name] = {
                        "avg_return": round(float(np.mean(arr)), 4),
                        "median_return": round(float(np.median(arr)), 4),
                        "win_rate": round(
                            float(np.sum(arr > 0) / len(arr)), 4
                        ),
                        "best": round(float(np.max(arr)), 4),
                        "worst": round(float(np.min(arr)), 4),
                        "n_valid": len(returns),
                    }
                else:
                    perf_factors[name] = {
                        "avg_return": 0.0,
                        "win_rate": 0.0,
                        "n_valid": 0,
                    }

            entry = {
                "pick_date": pick_date,
                "mode": mode,
                "eval_date": trade_date,
                "eval_days": actual_eval_days,
                "factors": perf_factors,
                "backfilled_at": datetime.now().isoformat(),
            }
            self._append_performance(entry)
            results.append(entry)
            logger.info(
                "[FactorMonitor] 回填 %s→%s (%d 个因子)",
                pick_date,
                trade_date,
                len(perf_factors),
            )

        return results if results else None

    # ------------------------------------------------------------------
    # Performance query
    # ------------------------------------------------------------------

    def get_performance(self, factor_name: Optional[str] = None, window: int = 20):
        """获取因子近期表现。

        Args:
            factor_name: 因子名，None 返回所有因子
            window: 近 N 条记录

        Returns:
            dict: {factor_name: {mean_return, median_return, std_return, mean_win_rate, ...}}
        """
        history = self._load_performance()
        if not history:
            return {}

        recent = history[-window:]

        if factor_name:
            stats = self._factor_stats(recent, factor_name)
            return {factor_name: stats} if stats else {}

        all_factors: set = set()
        for entry in recent:
            all_factors.update(entry.get("factors", {}).keys())

        result = {}
        for name in sorted(all_factors):
            stats = self._factor_stats(recent, name)
            if stats:
                result[name] = stats
        return result

    def _factor_stats(self, history: List[dict], factor_name: str) -> Optional[dict]:
        """从历史记录中提取单个因子的统计。"""
        avg_returns = []
        win_rates = []
        valid_days = 0

        for entry in history:
            fdata = entry.get("factors", {}).get(factor_name)
            if fdata and fdata.get("n_valid", 0) > 0:
                avg_returns.append(fdata["avg_return"])
                win_rates.append(fdata["win_rate"])
                valid_days += 1

        if not avg_returns:
            return None

        arr_r = np.array(avg_returns)
        arr_w = np.array(win_rates)
        return {
            "mean_return": round(float(np.mean(arr_r)), 4),
            "median_return": round(float(np.median(arr_r)), 4),
            "std_return": round(float(np.std(arr_r)), 4),
            "mean_win_rate": round(float(np.mean(arr_w)), 4),
            "positive_days": int(np.sum(arr_r > 0)),
            "valid_days": valid_days,
        }

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    def format_report(self, window: int = 20) -> str:
        """生成因子监控 Markdown 报告。"""
        perf = self.get_performance(window=window)
        if not perf:
            return "## 因子监控报告\n\n暂无数据。\n"

        lines = [
            f"## 因子监控报告（近 {window} 个交易日）",
            "",
            f"更新: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            "| 因子 | 均收益 | 中位收益 | 胜率 | 正收益天数 | 有效天数 | 评价 |",
            "|------|--------|---------|------|-----------|----------|------|",
        ]

        for name, stats in sorted(
            perf.items(), key=lambda x: -(x[1].get("mean_return", -999))
        ):
            mr = stats["mean_return"]
            wr = stats["mean_win_rate"]
            pd_ = stats["positive_days"]
            vd = stats["valid_days"]

            if mr > 0.01 and wr > 0.6:
                label = "有效"
            elif mr > 0.005 and wr > 0.5:
                label = "一般"
            elif mr < 0:
                label = "反向"
            else:
                label = "待观察"

            lines.append(
                f"| {name} | {mr:+.2%} | {stats['median_return']:+.2%} "
                f"| {wr:.0%} | {pd_}/{vd} | {vd} | {label} |"
            )

        lines.append("")
        lines.append(
            f"> 均收益 = 各日因子 Top {self.top_n} 平均 {self.eval_days}日收益的均值；"
            f"胜率 > 60% + 均收益 > 1% → 有效"
        )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _daily_limit(code: str) -> float:
        """返回股票的日涨跌停幅度（绝对值）。

        A股主板 ±10%，创业板/科创板 ±20%，北交所 ±30%。
        """
        code = str(code).replace(".SZ", "").replace(".SH", "").replace(".BJ", "")
        if len(code) < 2:
            return 0.10
        prefix3 = code[:3]
        if prefix3 in ("300", "301", "688"):
            return 0.20
        if prefix3 in ("400", "420", "430", "830", "831", "832", "833", "834",
                        "835", "836", "837", "838", "839", "870", "871", "872",
                        "873", "920"):
            return 0.30
        if code.startswith("8") or code.startswith("4"):
            return 0.30
        return 0.10

    def _get_forward_returns(
        self, codes: List[str], start_date: str, end_date: str
    ) -> Optional[Dict[str, float]]:
        """计算一批股票从 start_date 到 end_date 的收益率。

        使用 stock_daily 表的 close 价格。超出涨跌停范围的异常收益视为数据错误，排除。
        """
        try:
            from src.storage import DatabaseManager

            db = DatabaseManager()
            start_dt = datetime.strptime(start_date, "%Y%m%d").date()
            end_dt = datetime.strptime(end_date, "%Y%m%d").date()

            margin = timedelta(days=3)
            data = db.get_data_range_batch(codes, start_dt - margin, end_dt)
        except Exception as e:
            logger.warning("[FactorMonitor] 查询价格数据失败: %s", e)
            return None

        if not data:
            return None

        # 估算交易天数（保守偏多，让限制宽松一些）
        cal_days = max(1, (end_dt - start_dt).days)
        est_trading_days = max(1, int(cal_days * 0.75))

        returns: Dict[str, float] = {}
        skipped = 0
        for code, rows in data.items():
            if len(rows) < 2:
                continue

            closes = [(r.date, r.close) for r in rows if r.close is not None]
            if len(closes) < 2:
                continue

            closes.sort(key=lambda x: x[0])

            entry = None
            exit_ = None
            for d, c in closes:
                if d >= start_dt and entry is None:
                    entry = c
                if d <= end_dt:
                    exit_ = c

            if entry and exit_ and entry > 0:
                ret = (exit_ - entry) / entry
                limit = self._daily_limit(code)
                max_ret = limit * est_trading_days * 1.01  # 1% margin for rounding
                min_ret = -limit * est_trading_days * 1.01
                if min_ret <= ret <= max_ret:
                    returns[code] = ret
                else:
                    skipped += 1
                    logger.debug(
                        "[FactorMonitor] 排除异常收益 %s: %.1f%% (limit=±%.0f%%)",
                        code, ret * 100, limit * 100,
                    )

        if skipped:
            logger.info("[FactorMonitor] 排除 %d 只异常收益股票", skipped)

        return returns

    def _load_performance(self) -> List[dict]:
        if self.perf_file.exists():
            try:
                data = json.loads(self.perf_file.read_text())
                if isinstance(data, list):
                    return data
                return data.get("history", [])
            except Exception:
                pass
        return []

    def _save_performance(self, history: List[dict]):
        self.perf_file.parent.mkdir(parents=True, exist_ok=True)
        self.perf_file.write_text(
            json.dumps(history, ensure_ascii=False, indent=2)
        )

    def _append_performance(self, entry: dict):
        history = self._load_performance()
        key = (entry.get("pick_date"), entry.get("mode"))
        for i, existing in enumerate(history):
            if (existing.get("pick_date"), existing.get("mode")) == key:
                history[i] = entry
                self._save_performance(history)
                return
        history.append(entry)
        self._save_performance(history)

    def _evaluated_pick_dates(self) -> set:
        return {f"{e.get('mode', '')}_{e.get('pick_date', '')}" for e in self._load_performance()}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = FactorMonitor(top_n=5, eval_days=5)
    print(f"picks dir: {m.picks_dir}")
    print(f"perf file: {m.perf_file}")
    print(m.format_report(window=10))
