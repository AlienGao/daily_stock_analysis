# -*- coding: utf-8 -*-
"""券商金股当月回测「盘中实时估算 / 收盘态今日保留」单元测试。

覆盖:
- 盘中窗口 / 收盘后会话判定 helper 的时间边界;
- _resolve_sell_date_with_adj / _overwrite_live_today_bar 在盘中实时估算模式下
  今日端日不回退、今日 bar 以实时价保留;
- _sync_daily_returns_from_ohlc 在收盘态（15:00–当日因子入库）stock_daily 已写入
  今日收盘 bar 时同样保留今日（端日与累计推进到今日，与 end_price/end_date 一致）;
- compute_backtest / get_current_month_stock_returns 盘中放行（实时快照为今日 →
  is_realtime=True、sell_date=今日、累计收益按实时价×最近因子估算）与收盘后会话
  合并真实收盘快照（is_realtime=False、同样推进到今日、今日收盘 OHLC 写库）;
- 防误放行：快照非今日 / 非会话且当日因子未入库 → 维持上一交易日口径;
- 展开面板 stock-history 链路（get_stock_recommend_history）当月盘中/收盘后与会话
  同步实时口径，历史月不注入。
"""

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.services.broker_recommend_service import BrokerRecommendService

TS_CODE = "600519.SH"
CODE = "600519"


def _d(days_ago: int) -> str:
    """今天往前推 days_ago 天的 YYYYMMDD(仅作排序用日期串,不要求是交易日)。"""
    return (date.today() - timedelta(days=days_ago)).strftime("%Y%m%d")


def _today() -> str:
    return date.today().strftime("%Y%m%d")


def _prev_month() -> str:
    """上一自然月 YYYYMM。"""
    today = date.today()
    first = today.replace(day=1) - timedelta(days=1)
    return first.strftime("%Y%m")


def _ohlc_bar(close: float) -> dict:
    return {"open": close * 0.99, "high": close * 1.01, "low": close * 0.98, "close": close}


class TestIntradayWindowHelpers:
    def test_in_intraday_window_time_boundaries(self):
        # 09:30 与 14:59 在窗口内;09:29 与 15:00 在窗口外
        assert BrokerRecommendService._in_intraday_window(datetime(2026, 9, 9, 9, 30)) is True
        assert BrokerRecommendService._in_intraday_window(datetime(2026, 9, 9, 14, 59)) is True
        assert BrokerRecommendService._in_intraday_window(datetime(2026, 9, 9, 9, 29)) is False
        assert BrokerRecommendService._in_intraday_window(datetime(2026, 9, 9, 15, 0)) is False
        assert BrokerRecommendService._in_intraday_window(datetime(2026, 9, 9, 0, 0)) is False

    def test_is_post_close_session_boundaries(self, monkeypatch):
        monkeypatch.setattr("src.discovery.engine.is_trading_day", lambda: True)
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        # 15:00 起为收盘后会话;盘中窗口 / 开盘前返回 False
        assert svc._is_post_close_session(datetime(2026, 9, 9, 15, 0)) is True
        assert svc._is_post_close_session(datetime(2026, 9, 9, 18, 30)) is True
        assert svc._is_post_close_session(datetime(2026, 9, 9, 23, 59)) is True
        assert svc._is_post_close_session(datetime(2026, 9, 9, 10, 0)) is False
        assert svc._is_post_close_session(datetime(2026, 9, 9, 14, 59)) is False
        assert svc._is_post_close_session(datetime(2026, 9, 9, 9, 0)) is False
        # 非交易日收盘后 → False
        monkeypatch.setattr("src.discovery.engine.is_trading_day", lambda: False)
        assert svc._is_post_close_session(datetime(2026, 9, 9, 16, 0)) is False

    def test_resolve_sell_date_keeps_today_only_in_live_mode(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        d1, d2 = _d(5), _d(3)
        trading_days = [d1, d2, _today()]
        # 仅历史日有精确因子,今日无(盘中当日因子未生成)
        adj_map = {d1: 2.0, d2: 2.0}
        # 盘中估算模式:今日端日保留
        assert svc._resolve_sell_date_with_adj(
            TS_CODE, trading_days, _today(), adj_map, allow_live_today=True
        ) == _today()
        # 非实时模式:回退至最近一个有精确因子的交易日(原行为)
        assert svc._resolve_sell_date_with_adj(
            TS_CODE, trading_days, _today(), adj_map, allow_live_today=False
        ) == d2

    def test_overwrite_live_today_bar_injects_realtime_price_and_ohlc(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        d1 = _d(3)
        bars = [
            {"date": d1, "price": 100.0},
            {"date": _today(), "price": 100.0},
        ]
        svc._overwrite_live_today_bar(
            bars, _today(), 11.25, _ohlc_bar(11.25),
        )
        assert bars[0]["price"] == 100.0  # 非今日 bar 不动
        assert bars[1]["price"] == pytest.approx(11.25)
        assert bars[1]["open"] == pytest.approx(11.25 * 0.99)
        assert bars[1]["high"] == pytest.approx(11.25 * 1.01)
        assert bars[1]["low"] == pytest.approx(11.25 * 0.98)


class TestSyncClosedSessionTodayBar:
    """收盘态（15:00–当日因子入库）stock_daily 已有今日收盘 bar 时，端日与累计推进到今日。"""

    def test_sync_keeps_db_today_bar_without_today_factor(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        d1, d2 = _d(6), _d(2)
        today = _today()
        # DB stock_daily 已入库今日收盘 bar（close 11.0），但 adj_factor 只有历史日
        ohlc = {d1: _ohlc_bar(10.0), d2: _ohlc_bar(10.5), today: _ohlc_bar(11.0)}
        drs = [
            {"date": d1, "price": 100.0},
            {"date": d2, "price": 105.0},
            {"date": today, "price": 110.0},
        ]
        adj_map = {d1: 10.0, d2: 10.0}
        with patch.object(svc, "_prefetch_ohlc", return_value={TS_CODE: ohlc}), \
                patch.object(svc, "_load_all_adj_factors", return_value={CODE: adj_map}):
            out = svc._sync_daily_returns_from_ohlc(
                TS_CODE, drs, d1, today, allow_live_today=False,
            )
        assert out[-1]["date"] == today
        assert out[-1]["price"] == pytest.approx(11.0)
        # 今日按最近因子(10)近似:100 → 110,累计 +10%;因子入库后自动收敛
        assert out[-1]["cumulative"] == pytest.approx((11.0 * 10 - 10.0 * 10) / (10.0 * 10), abs=1e-4)
        assert out[-1]["open"] == pytest.approx(11.0 * 0.99)

    def test_sync_still_rolls_back_when_no_today_bar_anywhere(self):
        svc = BrokerRecommendService.__new__(BrokerRecommendService)
        svc.db = MagicMock()
        d1, d2 = _d(6), _d(2)
        # 今日无任何行情（停牌/非交易日残留）：ohlc 与 daily_returns 均无今日
        ohlc = {d1: _ohlc_bar(10.0), d2: _ohlc_bar(10.5)}
        drs = [
            {"date": d1, "price": 100.0},
            {"date": d2, "price": 105.0},
            {"date": _today(), "price": 110.0},
        ]
        drs[2]["price"] = None
        adj_map = {d1: 10.0, d2: 10.0}
        with patch.object(svc, "_prefetch_ohlc", return_value={TS_CODE: ohlc}), \
                patch.object(svc, "_load_all_adj_factors", return_value={CODE: adj_map}):
            out = svc._sync_daily_returns_from_ohlc(
                TS_CODE, drs, d1, _today(), allow_live_today=False,
            )
        assert out[-1]["date"] == d2
        assert out[-1]["cumulative"] == pytest.approx((105.0 - 100.0) / 100.0, abs=1e-4)


class _LiveFixture:
    """盘中实时估算 / 收盘后会话测试骨架:构造依赖并统一管理 mock 生命周期。

    后复权口径:买入日 d1 收盘 100、d2 收盘 105(因子 10 → 不复权 10.0/10.5);
    今日实时/收盘价 11.2(不复权,×最近因子 10 → 后复权 112)。
    live_window / post_close 控制会话判定;change_date 控制快照 trade_date。
    """

    def __init__(self, live_window: bool, post_close: bool, change_date: str):
        self.svc = BrokerRecommendService.__new__(BrokerRecommendService)
        self.svc.db = MagicMock()
        self.d1, self.d2 = _d(6), _d(2)
        today = _today()
        adj_map = {self.d1: 10.0, self.d2: 10.0}  # 仅历史日因子,今日因子未入库
        monthly = pd.DataFrame([{
            "ts_code": TS_CODE, "name": "贵州茅台",
            "broker": "华泰证券", "broker_count": 1,
        }])
        prices = {TS_CODE: {self.d1: 100.0, self.d2: 105.0}}  # 后复权
        rt_prices = {TS_CODE: {today: 11.2}}                 # 不复权实时/收盘价
        rt_ohlc = {TS_CODE: _ohlc_bar(11.2)}
        rt_changes = {TS_CODE: 0.012}
        ohlc = {TS_CODE: {self.d1: _ohlc_bar(10.0), self.d2: _ohlc_bar(10.5)}}

        self.patches = [
            patch.object(self.svc, "_effective_month_end", return_value=today),
            patch.object(self.svc, "_get_trading_days", return_value=[self.d1, self.d2]),
            patch.object(self.svc, "get_monthly_recommendations", return_value=monthly),
            patch.object(self.svc, "_prefetch_prices", return_value=prices),
            patch.object(
                self.svc, "_get_realtime_prices_batch",
                return_value=(rt_prices, rt_changes, rt_ohlc, {TS_CODE: change_date}),
            ),
            patch.object(self.svc, "_load_all_adj_factors", return_value={CODE: adj_map}),
            patch.object(self.svc, "_is_intraday_live_window", return_value=live_window),
            patch.object(self.svc, "_is_post_close_session", return_value=post_close),
            patch.object(self.svc, "_prefetch_ohlc", return_value=ohlc),
            patch("src.discovery.engine.is_trading_day", return_value=True),
        ]

    def __enter__(self):
        for p in self.patches:
            p.__enter__()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.__exit__(*exc)

    def compute_backtest(self) -> dict:
        month = date.today().strftime("%Y%m")
        return self.svc.compute_backtest(month, top_n_per_broker=10)

    def current_month_returns(self) -> dict:
        return self.svc.get_current_month_stock_returns([TS_CODE])


class TestComputeBacktestIntradayLive:
    def test_live_window_merges_today_realtime_price(self):
        with _LiveFixture(live_window=True, post_close=False, change_date=_today()) as f:
            result = f.compute_backtest()
        today = _today()

        assert result["is_realtime"] is True
        assert result["sell_date"] == today
        assert len(result["brokers"]) == 1
        sr = result["stock_returns"][0]
        assert sr["end_date"] == today
        assert sr["end_price"] == pytest.approx(112.0)  # 11.2 × 最近因子 10
        # 今日 bar 以不复权实时价 + 实时 OHLC 呈现,累计收益按最近因子估算
        last_bar = sr["daily_returns"][-1]
        assert last_bar["date"] == today
        assert last_bar["price"] == pytest.approx(11.2)
        assert last_bar["open"] == pytest.approx(11.2 * 0.99)
        assert last_bar["high"] == pytest.approx(11.2 * 1.01)
        assert last_bar["low"] == pytest.approx(11.2 * 0.98)
        assert sr["month_cumulative_return"] == pytest.approx((11.2 * 10 - 100.0) / 100.0, abs=1e-4)
        # 盘中实时价绝不写库(收盘价由盘后管线照常写入覆盖)
        f.svc.db.save_daily_data.assert_not_called()

    def test_stale_snapshot_not_today_stays_on_prev_trading_day(self):
        # 快照 trade_date 为昨天 → 不是真实今日行情,维持上一交易日口径
        with _LiveFixture(live_window=True, post_close=True, change_date=_d(1)) as f:
            result = f.compute_backtest()

        assert result["is_realtime"] is False
        assert result["sell_date"] == f.d2
        sr = result["stock_returns"][0]
        assert sr["end_date"] == f.d2
        assert sr["end_price"] == pytest.approx(105.0)
        assert sr["daily_returns"][-1]["date"] == f.d2
        f.svc.db.save_daily_data.assert_not_called()

    def test_post_close_session_merges_close_snapshot(self):
        # 收盘后交易日(15:00–当日因子入库)快照为今日 → 并入真实收盘价,推进到今日;
        # 非盘中 → 不标 is_realtime,今日收盘 OHLC 照常写库
        with _LiveFixture(live_window=False, post_close=True, change_date=_today()) as f:
            result = f.compute_backtest()

        today = _today()
        assert result["is_realtime"] is False
        assert result["sell_date"] == today
        sr = result["stock_returns"][0]
        assert sr["end_date"] == today
        assert sr["end_price"] == pytest.approx(112.0)
        assert sr["daily_returns"][-1]["date"] == today
        assert sr["month_cumulative_return"] == pytest.approx((11.2 * 10 - 100.0) / 100.0, abs=1e-4)
        f.svc.db.save_daily_data.assert_called()

    def test_non_session_without_today_factor_stays_on_prev_trading_day(self):
        # 非盘中亦非收盘后会话(如开盘前/非交易时段残留)且当日因子未入库 → 不放行,
        # 维持收盘口径
        with _LiveFixture(live_window=False, post_close=False, change_date=_today()) as f:
            result = f.compute_backtest()

        assert result["is_realtime"] is False
        assert result["sell_date"] == f.d2
        assert result["stock_returns"][0]["end_date"] == f.d2


class TestCurrentMonthStockReturnsIntradayLive:
    def test_live_window_returns_realtime_cumulative(self):
        with _LiveFixture(live_window=True, post_close=False, change_date=_today()) as f:
            result = f.current_month_returns()

        assert result["is_realtime"] is True
        assert result["sell_date"] == _today()
        item = result["items"][0]
        assert item["end_date"] == _today()
        # 买入 100(后复权)→ 今日实时价 11.2 × 因子 10 = 112
        assert item["cumulative_return"] == pytest.approx(0.12, abs=1e-4)

    def test_post_close_session_returns_close_cumulative(self):
        with _LiveFixture(live_window=False, post_close=True, change_date=_today()) as f:
            result = f.current_month_returns()

        assert result["is_realtime"] is False
        assert result["sell_date"] == _today()
        item = result["items"][0]
        assert item["end_date"] == _today()
        assert item["cumulative_return"] == pytest.approx(0.12, abs=1e-4)

    def test_stale_snapshot_stays_on_prev_trading_day(self):
        with _LiveFixture(live_window=True, post_close=False, change_date=_d(1)) as f:
            result = f.current_month_returns()

        assert result["is_realtime"] is False
        assert result["sell_date"] == f.d2
        item = result["items"][0]
        assert item["end_date"] == f.d2
        assert item["cumulative_return"] == pytest.approx((105.0 - 100.0) / 100.0, abs=1e-4)


class _HistoryFixture:
    """get_stock_recommend_history(展开面板)链路骨架。

    rows_month: 推荐记录所在月份(当前月或历史月),决定是否注入今日快照。
    live_window / post_close: 会话判定(盘中或收盘后),决定 allow_live_today 是否放行。
    change_date: 快照 trade_date。
    """

    def __init__(self, rows_month: str, live_window: bool, post_close: bool, change_date: str):
        self.svc = BrokerRecommendService.__new__(BrokerRecommendService)
        self.svc.db = MagicMock()
        self.d1, self.d2 = _d(6), _d(2)
        today = _today()
        adj_map = {self.d1: 10.0, self.d2: 10.0}
        rows = [{
            "month": rows_month, "ts_code": TS_CODE, "name": "贵州茅台",
            "broker": "华泰证券", "broker_count": 1,
        }]
        self.svc.db.get_broker_recommend_by_stock.return_value = rows
        self.svc.db.get_broker_backtest.return_value = None
        prices = {TS_CODE: {self.d1: 100.0, self.d2: 105.0}}
        rt_prices = {TS_CODE: {today: 11.2}}
        rt_ohlc = {TS_CODE: _ohlc_bar(11.2)}
        rt_changes = {TS_CODE: 0.012}
        ohlc = {TS_CODE: {self.d1: _ohlc_bar(10.0), self.d2: _ohlc_bar(10.5)}}

        self.patches = [
            patch.object(self.svc, "_effective_month_end", return_value=today),
            patch.object(self.svc, "_get_trading_days", return_value=[self.d1, self.d2]),
            patch.object(self.svc, "_prefetch_prices", return_value=prices),
            patch.object(
                self.svc, "_get_realtime_prices_batch",
                return_value=(rt_prices, rt_changes, rt_ohlc, {TS_CODE: change_date}),
            ),
            patch.object(self.svc, "_load_all_adj_factors", return_value={CODE: adj_map}),
            patch.object(self.svc, "_is_intraday_live_window", return_value=live_window),
            patch.object(self.svc, "_is_post_close_session", return_value=post_close),
            patch.object(self.svc, "_prefetch_ohlc", return_value=ohlc),
        ]

    def __enter__(self):
        for p in self.patches:
            p.__enter__()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.__exit__(*exc)

    def stock_history(self) -> dict:
        return self.svc.get_stock_recommend_history(TS_CODE)


class TestStockHistoryExpansionRealtime:
    """展开面板与表格一致:当月盘中/收盘后注入今日快照,历史月保持收盘口径。"""

    def test_current_month_live_injects_today(self):
        current = date.today().strftime("%Y%m")
        with _HistoryFixture(rows_month=current, live_window=True, post_close=False, change_date=_today()) as f:
            resp = f.stock_history()

        assert len(resp["entries"]) == 1
        entry = resp["entries"][0]
        assert entry["month"] == current
        assert entry["sell_date"] == _today()
        last_bar = entry["daily_returns"][-1]
        assert last_bar["date"] == _today()
        assert last_bar["price"] == pytest.approx(11.2)
        # 展开累计收益与表格回测 month_cumulative_return 同口径(≈12%)
        assert entry["cumulative_return"] == pytest.approx(0.12, abs=1e-4)

    def test_current_month_post_close_merges_close_snapshot(self):
        current = date.today().strftime("%Y%m")
        with _HistoryFixture(rows_month=current, live_window=False, post_close=True, change_date=_today()) as f:
            resp = f.stock_history()

        entry = resp["entries"][0]
        assert entry["sell_date"] == _today()
        assert entry["daily_returns"][-1]["date"] == _today()
        assert entry["cumulative_return"] == pytest.approx(0.12, abs=1e-4)

    def test_historical_month_keeps_closed_caliber(self):
        prev = _prev_month()
        with _HistoryFixture(rows_month=prev, live_window=True, post_close=False, change_date=_today()) as f:
            resp = f.stock_history()

        entry = resp["entries"][0]
        # 历史月不注入今日,累计仍按持仓期截至月末
        assert entry["daily_returns"][-1]["date"] == f.d2
        assert entry["cumulative_return"] == pytest.approx((105.0 - 100.0) / 100.0, abs=1e-4)
