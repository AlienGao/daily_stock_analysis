# -*- coding: utf-8 -*-
"""发现引擎回测模块。

盘中: 当日收盘价买入 → 下一交易日收盘价卖出 → 滚动复利
盘后: 下一交易日开盘价买入 → 再下一交易日开盘价卖出 → 滚动复利

支持日期筛选、资金曲线、逐笔交易记录。
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports"
_DEFAULT_INITIAL_CAPITAL = 5_000_000.0


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class TradeRecord:
    stock_code: str
    stock_name: str
    buy_date: str      # YYYYMMDD
    buy_price: float
    sell_date: str     # YYYYMMDD
    sell_price: float
    return_pct: float  # e.g. 0.03 = 3%
    pnl: float         # 实际盈亏金额
    allocated_capital: float  # 分配到该股的初始资金


@dataclass
class DailyBacktestResult:
    trade_date: str          # discovery 日期的 YYYYMMDD
    stock_returns: Dict[str, float] = field(default_factory=dict)
    avg_return: float = 0.0
    cumulative_return: float = 0.0
    capital: float = _DEFAULT_INITIAL_CAPITAL
    win_count: int = 0
    total_count: int = 0


@dataclass
class BacktestSummary:
    mode: str
    initial_capital: float = _DEFAULT_INITIAL_CAPITAL
    final_capital: float = _DEFAULT_INITIAL_CAPITAL
    cumulative_return: float = 0.0
    total_pnl: float = 0.0
    win_rate: float = 0.0
    total_days: int = 0
    total_trades: int = 0
    daily_results: List[DailyBacktestResult] = field(default_factory=list)
    trade_records: List[TradeRecord] = field(default_factory=list)
    capital_curve: List[Dict] = field(default_factory=list)  # [{date, capital}]


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class DiscoveryBacktest:
    """发现引擎回测计算器。"""

    def __init__(self, tushare_fetcher=None):
        self._fetcher = tushare_fetcher
        self._price_cache: Dict[str, Dict[str, Dict[str, float]]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute(
        self,
        mode: str = "intraday",
        lookback_days: int = 30,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_capital: float = _DEFAULT_INITIAL_CAPITAL,
    ) -> Optional[BacktestSummary]:
        """计算回测结果。

        Args:
            mode: "intraday" 或 "postmarket"
            lookback_days: 默认回看天数（自然日），start_date 未指定时使用。
                最小需要 2 天（盘中）或 3 天（盘后）才能完成一次完整交易。
            start_date: 开始日期 YYYYMMDD（可选，优先于 lookback_days）
            end_date: 结束日期 YYYYMMDD（可选，默认今天）
            initial_capital: 初始资金
        """
        prefix = f"{mode}_"

        if end_date:
            try:
                ed = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
            except (ValueError, IndexError):
                ed = date.today()
        else:
            ed = date.today()

        if start_date:
            try:
                sd = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
            except (ValueError, IndexError):
                sd = ed - timedelta(days=lookback_days)
        else:
            sd = ed - timedelta(days=lookback_days)

        # 扫描 discovery_reports 下所有匹配的 JSON 文件
        files = sorted(_REPORTS_DIR.glob(f"{prefix}*_topn.json"))
        discovery_dates: List[date] = []
        for fp in files:
            stem = fp.stem
            date_str = stem[len(prefix):].replace("_topn", "")
            try:
                d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
                if sd <= d <= ed:
                    discovery_dates.append(d)
            except (ValueError, IndexError):
                continue

        if len(discovery_dates) < 1:
            logger.info("[Backtest] %s 回测数据不足（无历史文件）", mode)
            return None

        trading_days = self._get_relevant_trading_days(discovery_dates, mode)
        if len(trading_days) < 2:
            return None

        # 加载所有 discovery 结果
        picks_by_date: Dict[str, List[dict]] = {}
        for d in discovery_dates:
            ds = d.strftime("%Y%m%d")
            fp = _REPORTS_DIR / f"{prefix}{ds}_topn.json"
            if fp.exists():
                try:
                    data = json.loads(fp.read_text(encoding="utf-8"))
                    if data:
                        picks_by_date[ds] = data if isinstance(data, list) else data.get("top_n", [])
                except (json.JSONDecodeError, KeyError):
                    pass

        # 预取全部所需价格
        all_codes = set()
        for picks in picks_by_date.values():
            for p in picks:
                code = p.get("stock_code", "")
                if code:
                    all_codes.add(code)
        self._prefetch_prices(list(all_codes), trading_days)

        summary: Optional[BacktestSummary]
        if mode == "intraday":
            summary = self._compute_intraday(picks_by_date, trading_days, initial_capital)
        else:
            summary = self._compute_postmarket(picks_by_date, trading_days, initial_capital)

        if summary:
            self._save_backtest_summary(summary)

        return summary

    # ------------------------------------------------------------------
    # Intraday: 当日 close 买入 → 次日 close 卖出
    # ------------------------------------------------------------------

    def _compute_intraday(
        self,
        picks_by_date: Dict[str, List[dict]],
        trading_days: List[str],
        initial_capital: float,
    ) -> BacktestSummary:
        daily_results: List[DailyBacktestResult] = []
        trade_records: List[TradeRecord] = []
        capital_curve: List[Dict] = []
        cum = 0.0
        capital = initial_capital
        total_trades = 0
        total_wins = 0

        for i, td in enumerate(trading_days[:-1]):
            if td not in picks_by_date:
                continue
            td_next = trading_days[i + 1]
            picks = picks_by_date[td]
            n = len(picks)
            if n == 0:
                continue

            alloc = capital / n
            stock_returns: Dict[str, float] = {}
            day_pnl = 0.0
            wins = 0

            for p in picks:
                code = p.get("stock_code", "")
                name = p.get("stock_name", "")
                close_today = self._get_price(code, td, "close")
                close_next = self._get_price(code, td_next, "close")
                if (
                    close_today and close_next and close_today > 0
                    and code and name
                ):
                    ret = (close_next - close_today) / close_today
                    stock_returns[code] = ret
                    pnl = alloc * ret
                    day_pnl += pnl
                    if ret > 0:
                        wins += 1

                    trade_records.append(TradeRecord(
                        stock_code=code,
                        stock_name=name,
                        buy_date=td,
                        buy_price=round(close_today, 2),
                        sell_date=td_next,
                        sell_price=round(close_next, 2),
                        return_pct=round(ret, 6),
                        pnl=round(pnl, 2),
                        allocated_capital=round(alloc, 2),
                    ))

            if not stock_returns:
                continue

            total_trades += len(stock_returns)
            total_wins += wins
            capital += day_pnl
            values = list(stock_returns.values())
            avg_ret = sum(values) / len(values)
            cum = (capital - initial_capital) / initial_capital

            daily_results.append(DailyBacktestResult(
                trade_date=td,
                stock_returns=stock_returns,
                avg_return=avg_ret,
                cumulative_return=cum,
                capital=round(capital, 2),
                win_count=wins,
                total_count=len(values),
            ))
            capital_curve.append({"date": td, "capital": round(capital, 2)})

        return BacktestSummary(
            mode="intraday",
            initial_capital=initial_capital,
            final_capital=round(capital, 2),
            cumulative_return=cum,
            total_pnl=round(capital - initial_capital, 2),
            win_rate=total_wins / total_trades if total_trades > 0 else 0,
            total_days=len(daily_results),
            total_trades=total_trades,
            daily_results=daily_results,
            trade_records=trade_records,
            capital_curve=capital_curve,
        )

    # ------------------------------------------------------------------
    # Postmarket: 次日 open 买入 → 次次日 open 卖出
    # ------------------------------------------------------------------

    def _compute_postmarket(
        self,
        picks_by_date: Dict[str, List[dict]],
        trading_days: List[str],
        initial_capital: float,
    ) -> BacktestSummary:
        daily_results: List[DailyBacktestResult] = []
        trade_records: List[TradeRecord] = []
        capital_curve: List[Dict] = []
        cum = 0.0
        capital = initial_capital
        total_trades = 0
        total_wins = 0

        for i, td in enumerate(trading_days[:-2]):
            if td not in picks_by_date:
                continue
            td_buy = trading_days[i + 1]
            td_sell = trading_days[i + 2]
            picks = picks_by_date[td]
            n = len(picks)
            if n == 0:
                continue

            alloc = capital / n
            stock_returns: Dict[str, float] = {}
            day_pnl = 0.0
            wins = 0

            for p in picks:
                code = p.get("stock_code", "")
                name = p.get("stock_name", "")
                open_buy = self._get_price(code, td_buy, "open")
                open_sell = self._get_price(code, td_sell, "open")
                if (
                    open_buy and open_sell and open_buy > 0
                    and code and name
                ):
                    ret = (open_sell - open_buy) / open_buy
                    stock_returns[code] = ret
                    pnl = alloc * ret
                    day_pnl += pnl
                    if ret > 0:
                        wins += 1

                    trade_records.append(TradeRecord(
                        stock_code=code,
                        stock_name=name,
                        buy_date=td_buy,
                        buy_price=round(open_buy, 2),
                        sell_date=td_sell,
                        sell_price=round(open_sell, 2),
                        return_pct=round(ret, 6),
                        pnl=round(pnl, 2),
                        allocated_capital=round(alloc, 2),
                    ))

            if not stock_returns:
                continue

            total_trades += len(stock_returns)
            total_wins += wins
            capital += day_pnl
            values = list(stock_returns.values())
            avg_ret = sum(values) / len(values)
            cum = (capital - initial_capital) / initial_capital

            daily_results.append(DailyBacktestResult(
                trade_date=td,
                stock_returns=stock_returns,
                avg_return=avg_ret,
                cumulative_return=cum,
                capital=round(capital, 2),
                win_count=wins,
                total_count=len(values),
            ))
            capital_curve.append({"date": td, "capital": round(capital, 2)})

        return BacktestSummary(
            mode="postmarket",
            initial_capital=initial_capital,
            final_capital=round(capital, 2),
            cumulative_return=cum,
            total_pnl=round(capital - initial_capital, 2),
            win_rate=total_wins / total_trades if total_trades > 0 else 0,
            total_days=len(daily_results),
            total_trades=total_trades,
            daily_results=daily_results,
            trade_records=trade_records,
            capital_curve=capital_curve,
        )

    # ------------------------------------------------------------------
    # Price fetching (unchanged)
    # ------------------------------------------------------------------

    def _get_relevant_trading_days(self, discovery_dates: List[date], mode: str) -> List[str]:
        if not discovery_dates:
            return []

        min_d = min(discovery_dates) - timedelta(days=1)
        max_d = max(discovery_dates) + timedelta(days=5)

        if self._fetcher is not None:
            try:
                cal_df = self._fetcher._call_api_with_rate_limit(
                    "trade_cal",
                    exchange="SSE",
                    start_date=min_d.strftime("%Y%m%d"),
                    end_date=max_d.strftime("%Y%m%d"),
                    is_open="1",
                )
                if cal_df is not None and not cal_df.empty:
                    return sorted(cal_df["cal_date"].tolist())
            except Exception:
                pass

        try:
            import exchange_calendars as xcals
            cal = xcals.get_calendar("XSHG")
            sessions = cal.sessions_in_range(
                min_d.strftime("%Y-%m-%d"), max_d.strftime("%Y-%m-%d")
            )
            return [s.strftime("%Y%m%d") for s in sessions]
        except Exception:
            pass

        days = []
        d = min_d
        while d <= max_d:
            if d.weekday() < 5:
                days.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        return days

    def _prefetch_prices(self, codes: List[str], trading_days: List[str]) -> None:
        if not codes or not trading_days:
            return

        db_codes = set()
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            session = db.get_session()
            try:
                from src.storage import StockDaily
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
                        "open": float(row.open) if row.open else None,
                        "close": float(row.close) if row.close else None,
                    }
                db_codes = {row.code for row in rows}
            finally:
                session.close()
        except Exception as e:
            logger.debug("[Backtest] 本地 DB 查询失败: %s", e)

        missing_codes = [c for c in codes if c not in db_codes]
        if not missing_codes or self._fetcher is None:
            return

        try:
            ts_codes = []
            for c in missing_codes:
                if c.isdigit() and len(c) == 6:
                    ts_codes.append(f"{c}.SH")
                    ts_codes.append(f"{c}.SZ")
                else:
                    ts_codes.append(c)

            for td in trading_days:
                existing = set((self._price_cache.get(td) or {}).keys())
                if set(missing_codes).issubset(existing):
                    continue
                try:
                    df = self._fetcher._call_api_with_rate_limit(
                        "daily", ts_code=",".join(ts_codes[:200]), trade_date=td,
                    )
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            ts = str(row.get("ts_code", ""))
                            code = ts.split(".")[0] if "." in ts else ts
                            self._price_cache.setdefault(td, {})[code] = {
                                "open": float(row["open"]) if pd.notna(row.get("open")) else None,
                                "close": float(row["close"]) if pd.notna(row.get("close")) else None,
                            }
                except Exception:
                    pass
        except Exception as e:
            logger.debug("[Backtest] Tushare 批量取价失败: %s", e)

    def _get_price(self, code: str, date_str: str, field: str) -> Optional[float]:
        day_cache = self._price_cache.get(date_str, {})
        stock_cache = day_cache.get(code, {})
        val = stock_cache.get(field)
        if val is not None:
            return float(val)

        # Fallback: 如果当天缺 open/close，尝试前一天 close
        if field in ("open", "close") and self._fetcher is not None:
            try:
                from datetime import timedelta as _td
                prev_str = (date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])) - _td(days=1)).strftime("%Y%m%d")
                prev_cache = self._price_cache.get(prev_str, {}).get(code, {})
                prev_val = prev_cache.get("close")
                if prev_val is not None:
                    return float(prev_val)
            except Exception:
                pass

        return None

    # ------------------------------------------------------------------
    # Backtest summary persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _get_summary_file(mode: str) -> Path:
        return _REPORTS_DIR / f"{mode}_backtest_summary.json"

    def _save_backtest_summary(self, summary: BacktestSummary) -> None:
        """追加回测结果到汇总文件。"""
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        summary_file = self._get_summary_file(summary.mode)

        history = []
        if summary_file.exists():
            try:
                history = json.loads(summary_file.read_text(encoding="utf-8"))
                if not isinstance(history, list):
                    history = []
            except Exception:
                history = []

        entry = {
            "date": date.today().strftime("%Y%m%d"),
            "mode": summary.mode,
            "cumulative_return": summary.cumulative_return,
            "win_rate": summary.win_rate,
            "total_trades": summary.total_trades,
            "total_days": summary.total_days,
            "final_capital": summary.final_capital,
            "initial_capital": summary.initial_capital,
        }

        updated = False
        for i, e in enumerate(history):
            if e.get("date") == entry["date"] and e.get("mode") == entry["mode"]:
                history[i] = entry
                updated = True
                break
        if not updated:
            history.append(entry)

        summary_file.write_text(
            json.dumps(history, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self._check_alerts(history[-5:])

    def _check_alerts(self, recent_entries: List[Dict]) -> None:
        """连续 3 天胜率 < 50% 或最大回撤 > 10% 时告警。"""
        if len(recent_entries) < 3:
            return

        win_rates = [e.get("win_rate", 0) for e in recent_entries]
        returns = [e.get("cumulative_return", 0) for e in recent_entries]

        if all(w < 0.5 for w in win_rates):
            logger.warning(
                "[Backtest] ⚠️ 告警：近 %d 天胜率持续低于 50%%: %s",
                len(win_rates),
                [f"{w*100:.0f}%" for w in win_rates],
            )

        peak = 0.0
        for r in returns:
            if r > peak:
                peak = r
            drawdown = peak - r if peak > 0 else 0
            if drawdown > 0.10:
                logger.warning(
                    "[Backtest] ⚠️ 告警：检测到超过 10%% 回撤 (当前 %.2f%%)",
                    drawdown * 100,
                )
                return
