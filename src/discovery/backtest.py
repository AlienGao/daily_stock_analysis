# -*- coding: utf-8 -*-
"""发现引擎回测模块。

盘中: 当日收盘价买入 → 下一交易日收盘价卖出 → 滚动复利
盘后: 信号日选股 → 次交易日开盘价买入 → 再次日开盘价卖出 → 滚动复利

支持日期筛选、资金曲线、逐笔交易记录、组合 OHLC。
"""

import json
import logging
import re
import requests
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
    is_open: bool = False  # 尚未到卖出时间，未平仓


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
    max_drawdown: float = 0.0
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
                最小需要 2 天才能完成一次完整交易。
            start_date: 开始日期 YYYYMMDD（可选，优先于 lookback_days）
            end_date: 结束日期 YYYYMMDD（可选，默认今天）
            initial_capital: 初始资金
        """
        prefix = f"{mode}_"

        if end_date:
            try:
                ed = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
            except (ValueError, IndexError):
                ed = self._resolve_backtest_date()
        else:
            ed = self._resolve_backtest_date()

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
            self._save_backtest_summary(summary, ed.strftime("%Y%m%d"))

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
        today_str = date.today().strftime("%Y%m%d")

        for i, td in enumerate(trading_days[:-1]):
            if td not in picks_by_date:
                continue
            td_next = trading_days[i + 1]

            # 卖点未到：不展示未平仓交易
            if td_next > today_str:
                continue

            is_open = td_next == today_str  # 当天盘中，用实时价展示
            sell_time = datetime.now().strftime("%H:%M:%S") if is_open else "15:00:00"

            picks = picks_by_date[td]
            n = len(picks)
            if n == 0:
                continue

            alloc = capital / n
            stock_returns: Dict[str, float] = {}
            day_pnl = 0.0
            wins = 0

            # 组合 OHLC：用 收益率×资金=P&L 计算各价位组合价值（避免量纲错误）
            prev_capital = capital
            w_open = 0.0
            w_high = 0.0
            w_low = 0.0
            has_ohlc = False

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
                        buy_date=td + " 15:00:00",
                        buy_price=round(close_today, 2),
                        sell_date=td_next + " " + sell_time,
                        sell_price=round(close_next, 2),
                        return_pct=round(ret, 6),
                        pnl=round(pnl, 2),
                        allocated_capital=round(alloc, 2),
                        is_open=is_open,
                    ))

                    # P&L 贡献 = alloc × (price/buy_price − 1)，量纲为元
                    o = self._get_price(code, td_next, "open")
                    h = self._get_price(code, td_next, "high")
                    lo = self._get_price(code, td_next, "low")
                    if o and h and lo:
                        w_open += alloc * (o / close_today - 1)
                        w_high += alloc * (h / close_today - 1)
                        w_low += alloc * (lo / close_today - 1)
                        has_ohlc = True

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
            # 交易日 + 结算时间（盘中用当前时间，盘后用 15:00）
            settlement_time = sell_time if sell_time else "15:00:00"
            curve_point: Dict = {
                "date": f"{td_next} {settlement_time}",
                "capital": round(capital, 2),
            }
            if has_ohlc:
                # open=买入成本（不含隔夜跳空），body 直接反映交易完整盈亏
                curve_point["open"] = round(prev_capital, 2)
                ph = prev_capital + w_high
                pl = prev_capital + w_low
                curve_point["high"] = round(max(ph, prev_capital, capital), 2)
                curve_point["low"] = round(min(pl, prev_capital, capital), 2)
                curve_point["close"] = round(capital, 2)
            capital_curve.append(curve_point)

        # max drawdown from capital curve
        max_dd = 0.0
        peak = initial_capital
        for pt in capital_curve:
            cap = pt.get("capital", peak)
            if cap > peak:
                peak = cap
            dd = (peak - cap) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        return BacktestSummary(
            mode="intraday",
            initial_capital=initial_capital,
            final_capital=round(capital, 2),
            cumulative_return=cum,
            total_pnl=round(capital - initial_capital, 2),
            win_rate=total_wins / total_trades if total_trades > 0 else 0,
            max_drawdown=round(max_dd, 4),
            total_days=len(daily_results),
            total_trades=total_trades,
            daily_results=daily_results,
            trade_records=trade_records,
            capital_curve=capital_curve,
        )

    # ------------------------------------------------------------------
    # Postmarket: 信号日选股 → 次交易日 open 买入 → 再次日 open 卖出
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

        today_str = date.today().strftime("%Y%m%d")
        now = datetime.now()

        for i, td in enumerate(trading_days[:-2]):
            if td not in picks_by_date:
                continue
            td_buy = trading_days[i + 1]
            td_sell = trading_days[i + 2]

            # 买入日未到：跳过
            if td_buy > today_str:
                continue

            # 卖出日 == 今天且已过 9:30 → 已平仓，用实际开盘价结算
            settled_today = (
                td_sell == today_str
                and now.time() >= datetime.strptime("09:30", "%H:%M").time()
            )
            is_open = td_sell > today_str or (td_sell == today_str and not settled_today)

            picks = picks_by_date[td]
            n = len(picks)
            if n == 0:
                continue

            alloc = capital / n
            stock_returns: Dict[str, float] = {}
            day_pnl = 0.0
            wins = 0

            # 组合 OHLC：基于买入日价格，用收益率×资金=P&L 计算组合价值
            prev_capital = capital
            w_high = 0.0
            w_low = 0.0
            w_close = 0.0
            has_ohlc = False

            for p in picks:
                code = p.get("stock_code", "")
                name = p.get("stock_name", "")
                open_buy = self._get_price(code, td_buy, "open")
                # 未平仓用 Sina 实时价作为卖出参考价
                open_sell = (
                    self._get_price(code, today_str, "close")
                    if is_open
                    else self._get_price(code, td_sell, "open")
                )
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

                    sell_time = datetime.now().strftime("%H:%M:%S") if is_open else "09:30:00"
                    trade_records.append(TradeRecord(
                        stock_code=code,
                        stock_name=name,
                        buy_date=td_buy + " 09:30:00",
                        buy_price=round(open_buy, 2),
                        sell_date=(today_str if is_open else td_sell) + " " + sell_time,
                        sell_price=round(open_sell, 2),
                        return_pct=round(ret, 6),
                        pnl=round(pnl, 2),
                        allocated_capital=round(alloc, 2),
                        is_open=is_open,
                    ))

                    # P&L 贡献 = alloc × (price/buy_price − 1)，买入日 OHLC
                    h = self._get_price(code, td_buy, "high")
                    lo = self._get_price(code, td_buy, "low")
                    c = self._get_price(code, td_buy, "close")
                    if h and lo and c:
                        w_high += alloc * (h / open_buy - 1)
                        w_low += alloc * (lo / open_buy - 1)
                        w_close += alloc * (c / open_buy - 1)
                        has_ohlc = True

            if not stock_returns:
                continue

            total_trades += len(stock_returns)
            total_wins += wins
            capital += day_pnl
            values = list(stock_returns.values())
            avg_ret = sum(values) / len(values)
            cum = (capital - initial_capital) / initial_capital

            daily_results.append(DailyBacktestResult(
                trade_date=td_buy,
                stock_returns=stock_returns,
                avg_return=avg_ret,
                cumulative_return=cum,
                capital=round(capital, 2),
                win_count=wins,
                total_count=len(values),
            ))

            settle_time = datetime.now().strftime("%H:%M:%S") if td_buy == today_str else "15:00:00"
            curve_point: Dict = {"date": f"{td_buy} {settle_time}", "capital": round(capital, 2)}
            if has_ohlc:
                curve_point["open"] = round(prev_capital, 2)
                ph = prev_capital + w_high
                pl = prev_capital + w_low
                pc = prev_capital + w_close
                curve_point["high"] = round(max(ph, prev_capital, pc), 2)
                curve_point["low"] = round(min(pl, prev_capital, pc), 2)
                curve_point["close"] = round(pc, 2)
            capital_curve.append(curve_point)

        # max drawdown from capital curve
        max_dd = 0.0
        peak = initial_capital
        for pt in capital_curve:
            cap = pt.get("capital", peak)
            if cap > peak:
                peak = cap
            dd = (peak - cap) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        return BacktestSummary(
            mode="postmarket",
            initial_capital=initial_capital,
            final_capital=round(capital, 2),
            cumulative_return=cum,
            total_pnl=round(capital - initial_capital, 2),
            win_rate=total_wins / total_trades if total_trades > 0 else 0,
            max_drawdown=round(max_dd, 4),
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
                        "high": float(row.high) if row.high else None,
                        "low": float(row.low) if row.low else None,
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
                                "high": float(row["high"]) if pd.notna(row.get("high")) else None,
                                "low": float(row["low"]) if pd.notna(row.get("low")) else None,
                                "close": float(row["close"]) if pd.notna(row.get("close")) else None,
                            }
                except Exception:
                    pass
        except Exception as e:
            logger.debug("[Backtest] Tushare 批量取价失败: %s", e)

        # 实时行情补充：realtime_spot DB + Sina API 补齐今日盘中价格
        today_str = date.today().strftime("%Y%m%d")
        if today_str in trading_days:
            self._prefetch_realtime_spot(codes, today_str)
            self._prefetch_sina_realtime(codes, today_str)

    def _prefetch_realtime_spot(self, codes: List[str], date_str: str) -> None:
        """从 realtime_spot DB 补齐当日盘中 OHLC。

        realtime_spot 每 30s 由 Scanner 刷新，含 open_price/high/low/price，
        可直接映射为当日 K 线的 O/H/L/C。
        """
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            spot_df = db.get_realtime_spot()
            if spot_df is None or spot_df.empty:
                return

            from datetime import datetime as _dt, timezone, timedelta
            now_cn = _dt.now(timezone(timedelta(hours=8)))
            is_market_open = (
                now_cn.weekday() < 5
                and now_cn.time() >= _dt.strptime("09:25", "%H:%M").time()
            )

            spot_index = spot_df.index.astype(str).str.strip().str.zfill(6)
            for code in codes:
                bare = str(code).strip().zfill(6)
                matches = spot_df[spot_index == bare]
                if matches.empty:
                    continue
                row = matches.iloc[0]
                cache_entry = self._price_cache.setdefault(date_str, {}).setdefault(code, {})

                # price → close（始终填充）
                price = row.get("price")
                if pd.notna(price) and float(price) > 0:
                    cache_entry["close"] = float(price)

                # open/high/low 仅在开盘后写入（盘前用昨日数据覆盖风险）
                if not is_market_open:
                    continue
                if pd.notna(row.get("open_price")):
                    cache_entry.setdefault("open", float(row["open_price"]))
                if pd.notna(row.get("high")):
                    cache_entry.setdefault("high", float(row["high"]))
                if pd.notna(row.get("low")):
                    cache_entry.setdefault("low", float(row["low"]))
        except Exception as e:
            logger.debug("[Backtest] realtime_spot 取价失败: %s", e)

    def _prefetch_sina_realtime(self, codes: List[str], date_str: str) -> None:
        """通过 Sina 实时行情补齐当日 OHLC（盘中可用）。

        非交易时段 Sina 返回的是上一交易日旧数据，open/high/low 会被误存为
        今日数据，导致回测伪造未来时点的买入记录。仅 close（最新价）在非
        交易时段仍有参考意义，open/high/low 只在确认盘中才写入缓存。
        """
        sina_codes: List[str] = []
        for c in codes:
            if not c.isdigit() or len(c) != 6:
                continue
            existing = (self._price_cache.get(date_str, {}).get(c) or {})
            if existing.get("open") and existing.get("high") and existing.get("low") and existing.get("close"):
                continue
            prefix = "sh" if c.startswith(("6", "68")) else "sz" if c.startswith(("0", "3")) else "bj"
            sina_codes.append(f"{prefix}{c}")

        if not sina_codes:
            return

        try:
            url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
            headers = {"Referer": "http://finance.sina.com.cn"}
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "gbk"
            body = resp.text
        except Exception as e:
            logger.debug("[Backtest] Sina 实时行情请求失败: %s", e)
            return

        from datetime import datetime as dt, timezone, timedelta as td
        now_cn = dt.now(timezone(timedelta(hours=8)))
        is_market_open = (
            now_cn.weekday() < 5
            and now_cn.time() >= dt.strptime("09:25", "%H:%M").time()
        )

        for sc in sina_codes:
            try:
                pattern = re.compile(rf"var hq_str_{sc}=\"([^\"]*)\"")
                m = pattern.search(body)
                if not m:
                    continue
                fields = m.group(1).split(",")
                if len(fields) < 6:
                    continue
                code = sc[2:]  # 去掉 sh/sz/bj 前缀
                open_p = float(fields[1]) if fields[1] and fields[1] != "0.000" else None
                high_p = float(fields[4]) if fields[4] and fields[4] != "0.000" else None
                low_p = float(fields[5]) if fields[5] and fields[5] != "0.000" else None
                close_p = float(fields[3]) if fields[3] and fields[3] != "0.000" else None  # 最新价

                if open_p is None and high_p is None and low_p is None and close_p is None:
                    continue

                cache_entry = self._price_cache.setdefault(date_str, {}).setdefault(code, {})
                # 盘前时段 Sina 返回旧数据，open/high/low 不可信
                if is_market_open:
                    if open_p is not None:
                        cache_entry["open"] = open_p
                    if high_p is not None:
                        cache_entry["high"] = high_p
                    if low_p is not None:
                        cache_entry["low"] = low_p
                if close_p is not None:
                    cache_entry["close"] = close_p
            except Exception:
                pass

    def _get_price(self, code: str, date_str: str, field: str) -> Optional[float]:
        day_cache = self._price_cache.get(date_str, {})
        stock_cache = day_cache.get(code, {})
        val = stock_cache.get(field)
        if val is not None:
            return float(val)

        # Fallback：未来日期及当天（收盘价可能尚未生成）用缓存中最近交易日的 close 替代。
        # 仅对 close 字段做 fallback，open/high/low 等缓存未命中直接返回 None，
        # 避免在开盘前用历史收盘价伪造买入记录（产生「今日 09:30」的未来时间标记）。
        today_str = date.today().strftime("%Y%m%d")
        if date_str < today_str or field != "close":
            return None

        for ds in sorted(self._price_cache.keys(), reverse=True):
            cv = self._price_cache[ds].get(code, {}).get("close")
            if cv is not None:
                return float(cv)

        return None

    # ------------------------------------------------------------------
    # Backtest summary persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _get_summary_file(mode: str) -> Path:
        return _REPORTS_DIR / f"{mode}_backtest_summary.json"

    def _resolve_backtest_date(self) -> date:
        """Resolve backtest reference date: use trading date when fetcher available."""
        if self._fetcher:
            trade_date_str = self._fetcher.get_trade_time(
                early_time="00:00", late_time="18:00"
            )
            if trade_date_str:
                try:
                    return date(
                        int(trade_date_str[:4]),
                        int(trade_date_str[4:6]),
                        int(trade_date_str[6:8]),
                    )
                except (ValueError, IndexError):
                    pass
        return date.today()

    def _save_backtest_summary(
        self, summary: BacktestSummary, entry_date: str = None
    ) -> None:
        """追加回测结果到汇总文件（仅交易日）。"""
        from src.discovery.engine import is_trading_day

        if not is_trading_day(self):
            return
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

        if entry_date is None:
            entry_date = date.today().strftime("%Y%m%d")
        entry = {
            "date": entry_date,
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
