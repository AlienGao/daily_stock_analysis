# -*- coding: utf-8 -*-
"""券商月度金股推荐服务。

提供券商金股数据的获取、存储和回测功能。
"""

import calendar
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from threading import Lock
from typing import Any, Dict, List, Optional

import pandas as pd

from src.storage import DatabaseManager, StockDaily

logger = logging.getLogger(__name__)


class BrokerRecommendService:
    """券商金股推荐服务。"""

    # 按单只股票缓存增强数据，不同数据类型有独立 TTL
    _enrichment_cache: Dict[str, Any] = {}
    _enrichment_cache_ts: Dict[str, float] = {}
    _cache_lock = Lock()

    # 不同数据类型的 TTL（秒）：盈利预测可缓存更久
    _CACHE_TTL = {
        "nineturn": 14400,   # 4 小时
        "forecast": 86400,   # 24 小时（研报不频繁更新）
        "cyq_perf": 14400,   # 4 小时
    }
    _DEFAULT_CACHE_TTL = 14400

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    @classmethod
    def _make_cache_key(cls, ts_code: str, query_date: str, data_type: str) -> str:
        return f"{ts_code}:{query_date}:{data_type}"

    @classmethod
    def _get_cached(cls, ts_code: str, query_date: str, data_type: str) -> Optional[Any]:
        """按单只股票 + 数据类型读取缓存，过期返回 None。"""
        key = cls._make_cache_key(ts_code, query_date, data_type)
        ttl = cls._CACHE_TTL.get(data_type, cls._DEFAULT_CACHE_TTL)
        with cls._cache_lock:
            if key in cls._enrichment_cache:
                age = time.time() - cls._enrichment_cache_ts.get(key, 0)
                if age < ttl:
                    return cls._enrichment_cache[key]
                del cls._enrichment_cache[key]
                del cls._enrichment_cache_ts[key]
        return None

    @classmethod
    def _set_cached(cls, ts_code: str, query_date: str, data_type: str, data: Any) -> None:
        """按单只股票 + 数据类型写入缓存。"""
        key = cls._make_cache_key(ts_code, query_date, data_type)
        with cls._cache_lock:
            cls._enrichment_cache[key] = data
            cls._enrichment_cache_ts[key] = time.time()

    def fetch_and_store_month(self, month: str) -> int:
        """获取指定月份券商金股并存入数据库。

        Args:
            month: YYYYMM 格式月份

        Returns:
            保存的记录数
        """
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher.get_instance()
            if not tf.is_available():
                logger.error("[BrokerRecommend] Tushare 不可用")
                return 0

            df = tf._api.query("broker_recommend", month=month)
            if df is None or df.empty:
                logger.info(f"[BrokerRecommend] {month} 月无数据")
                return 0

            return self.db.save_broker_recommend_monthly(month, df)
        except Exception as e:
            logger.error(f"[BrokerRecommend] 获取 {month} 月数据失败: {e}")
            return 0

    def get_monthly_recommendations(self, month: str) -> pd.DataFrame:
        """获取指定月份的金股 DataFrame，按券商分组。"""
        records = self.db.get_broker_recommend_monthly(month)
        if not records:
            return pd.DataFrame()

        data = [r.to_dict() for r in records]
        df = pd.DataFrame(data)

        # 按券商分组计算去重后的金股列表
        if not df.empty:
            df = df.sort_values(['broker', 'ts_code'])
        return df

    def _resolve_enrichment_date(self, month: str) -> str:
        """确定增强数据的查询日期：过去月份取最后交易日，否则取当前最近交易日。

        cyq_perf/stk_nineturn 等 API 在非交易日可能返回空数据，
        因此需要精确找到目标月份最后一个交易日。
        """
        year = int(month[:4])
        mon = int(month[4:6])
        last_day = calendar.monthrange(year, mon)[1]
        month_last = f"{month}{last_day:02d}"
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")

        if month_last <= today:
            # 过去月份：查询当月完整交易日历，取最后一天
            try:
                trading_days = self._get_trading_days(f"{month}01", month_last)
                if trading_days:
                    return trading_days[-1]
            except Exception:
                pass

        # 当前/未来月份：取最近可获得数据的交易日
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            trade_date = tf.get_trade_time(early_time='00:00', late_time='19:00')
            if trade_date:
                return trade_date
        except Exception:
            pass
        return month_last

    def get_monthly_enrichment(self, month: str) -> Dict[str, Dict[str, Any]]:
        """获取指定月份所有推荐股票的增强数据（九转、盈利预测、筹码胜率）。

        按单只股票缓存，不同数据类型独立 TTL（forecast 24h，其余 4h）。
        返回 {ts_code: {nineturn, forecast, cyq_perf}} 字典。
        """
        df = self.get_monthly_recommendations(month)
        if df.empty:
            return {}

        ts_codes = df["ts_code"].unique().tolist()
        query_date = self._resolve_enrichment_date(month)

        enrichment: Dict[str, Dict[str, Any]] = {}
        cache_hits = 0

        # 第一步：从缓存中读取（per-stock + per-type）
        uncached_nineturn: List[str] = []
        uncached_forecast: List[str] = []
        uncached_cyq: List[str] = []  # 实际上 cyq 是全量，但我们也按 stock 缓存结果

        for tc in ts_codes:
            entry: Dict[str, Any] = {}
            nt = self._get_cached(tc, query_date, "nineturn")
            if nt is not None:
                entry["nineturn"] = nt
                cache_hits += 1
            else:
                uncached_nineturn.append(tc)

            fc = self._get_cached(tc, query_date, "forecast")
            if fc is not None:
                entry["forecast"] = fc
                cache_hits += 1
            else:
                uncached_forecast.append(tc)

            cyq = self._get_cached(tc, query_date, "cyq_perf")
            if cyq is not None:
                entry["cyq_perf"] = cyq
                cache_hits += 1
            else:
                uncached_cyq.append(tc)

            if entry:
                enrichment[tc] = entry

        total_fields = len(ts_codes) * 3
        if cache_hits == total_fields:
            logger.info(f"[BrokerRecommend] enrichment 全部缓存命中 {month} ({len(ts_codes)} stocks)")
            return enrichment

        logger.info(f"[BrokerRecommend] enrichment {month}: 缓存命中 {cache_hits}/{total_fields}, "
                    f"待获取 nineturn={len(uncached_nineturn)} forecast={len(uncached_forecast)} cyq={len(uncached_cyq)}")

        # 第二步：使用批量接口获取未缓存数据（3 次 API 调用替代逐条 100+ 次）
        from data_provider.tushare_fetcher import TushareFetcher
        tf = TushareFetcher.get_instance()
        if not tf.is_available():
            logger.warning("[BrokerRecommend] Tushare 不可用，仅返回缓存数据")
            return enrichment

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures: dict = {}

            if uncached_nineturn:
                futures[pool.submit(tf.get_bulk_nineturn, uncached_nineturn, query_date)] = "nineturn"
            if uncached_forecast:
                futures[pool.submit(tf.get_bulk_forecast, uncached_forecast, query_date)] = "forecast"
            if uncached_cyq:
                futures[pool.submit(self._fetch_cyq_enrichment, tf, uncached_cyq, query_date)] = "cyq"

            for future in as_completed(futures, timeout=60):
                tag = futures[future]
                try:
                    if tag == "nineturn":
                        nt_data = future.result(timeout=30)
                        if nt_data:
                            for ts_code, nt in nt_data.items():
                                result = {
                                    "up_count": nt.get("up_count", 0),
                                    "down_count": nt.get("down_count", 0),
                                    "nine_up_turn": nt.get("nine_up_turn", 0),
                                    "nine_down_turn": nt.get("nine_down_turn", 0),
                                }
                                BrokerRecommendService._set_cached(ts_code, query_date, "nineturn", result)
                                enrichment.setdefault(ts_code, {})["nineturn"] = result
                    elif tag == "forecast":
                        fc_data = future.result(timeout=30)
                        if fc_data:
                            for ts_code, fc in fc_data.items():
                                result = {
                                    "eps": fc.get("eps"),
                                    "pe": fc.get("pe"),
                                    "roe": fc.get("roe"),
                                    "np": fc.get("np"),
                                    "rating": fc.get("rating", ""),
                                    "min_price": fc.get("min_price"),
                                    "max_price": fc.get("max_price"),
                                    "imp_dg": fc.get("imp_dg", ""),
                                }
                                BrokerRecommendService._set_cached(ts_code, query_date, "forecast", result)
                                enrichment.setdefault(ts_code, {})["forecast"] = result
                    elif tag == "cyq":
                        cyq_data = future.result(timeout=30)
                        if cyq_data:
                            for ts_code, cyq in cyq_data.items():
                                BrokerRecommendService._set_cached(ts_code, query_date, "cyq_perf", cyq)
                                enrichment.setdefault(ts_code, {})["cyq_perf"] = cyq
                except Exception:
                    pass

        logger.info(f"[BrokerRecommend] enrichment 完成 {month}: nineturn={sum(1 for v in enrichment.values() if 'nineturn' in v)}, "
                    f"forecast={sum(1 for v in enrichment.values() if 'forecast' in v)}, "
                    f"cyq={sum(1 for v in enrichment.values() if 'cyq_perf' in v)}")
        return enrichment

    def _fetch_cyq_enrichment(
        self, tf: Any, ts_codes: List[str], query_date: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """获取筹码胜率数据（在线程池中执行）。"""
        try:
            cyq_df = tf.get_bulk_cyq_perf(query_date) if tf.is_available() else None
            if cyq_df is None or cyq_df.empty:
                return None
            result: Dict[str, Dict[str, Any]] = {}
            for ts_code in ts_codes:
                if ts_code in cyq_df.index:
                    row = cyq_df.loc[ts_code]
                    cost_5 = float(row.get("cost_5pct", 0) or 0)
                    cost_95 = float(row.get("cost_95pct", 0) or 0)
                    weight_avg = float(row.get("weight_avg", 0) or 0)
                    winner_rate = float(row.get("winner_rate", 0) or 0) / 100.0
                    result[ts_code] = {
                        "cost_avg": round(weight_avg, 2),
                        "winner_rate": round(winner_rate, 4),
                        "concentration": round(
                            (cost_95 - cost_5) / weight_avg, 4
                        ) if weight_avg > 0 else None,
                    }
            return result
        except Exception as e:
            logger.debug(f"[BrokerRecommend] cyq enrichment 失败: {e}")
            return None

    def get_available_months(self) -> List[str]:
        """获取有数据的月份列表。"""
        return self.db.get_broker_recommend_months()

    @staticmethod
    def _next_month_str(month: str) -> str:
        """返回下一个月，格式 YYYYMM。"""
        year = int(month[:4])
        mon = int(month[4:6])
        if mon == 12:
            return f"{year + 1}01"
        return f"{year}{mon + 1:02d}"

    def _get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取指定日期范围内的交易日列表。"""
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher.get_instance()
            cal_df = tf._call_api_with_rate_limit(
                "trade_cal",
                exchange="SSE",
                start_date=start_date,
                end_date=end_date,
                is_open="1",
            )
            if cal_df is not None and not cal_df.empty:
                return sorted(cal_df["cal_date"].tolist())
        except Exception as e:
            logger.debug(f"[BrokerRecommend] 获取交易日历失败: {e}")

        # Fallback: 简单工作日
        days = []
        d = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
        ed = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
        while d <= ed:
            if d.weekday() < 5:
                days.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        return days

    def _fetch_tushare_prices(
        self, ts_code: str, code: str, start_date: str, end_date: str
    ) -> Dict[str, float]:
        """从 Tushare 拉取日线数据并入库，返回 {YYYYMMDD: close} 字典。"""
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher.get_instance()
            if not tf.is_available():
                return {}

            tushare_code = ts_code if "." in ts_code else tf._convert_stock_code(code)
            df = tf._call_api_with_rate_limit(
                "daily", ts_code=tushare_code, start_date=start_date, end_date=end_date,
            )
            if df is not None and not df.empty:
                prices = {}
                for _, row in df.iterrows():
                    r = row.to_dict()
                    d = str(r.get("trade_date", ""))
                    close_val = r.get("close")
                    if d and close_val is not None:
                        try:
                            prices[d] = float(close_val)
                        except (ValueError, TypeError):
                            pass
                if "trade_date" in df.columns and "date" not in df.columns:
                    df = df.rename(columns={"trade_date": "date"})
                if "vol" in df.columns and "volume" not in df.columns:
                    df = df.rename(columns={"vol": "volume"})
                if "date" in df.columns:
                    df["date"] = df["date"].astype(str).str.replace(
                        r"^(\d{4})(\d{2})(\d{2})$", r"\1-\2-\3", regex=True
                    )
                try:
                    self.db.save_daily_data(df, code, "Tushare")
                except Exception:
                    pass
                return prices
        except Exception:
            pass
        return {}

    def _get_stock_prices(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Dict[str, float]:
        """获取指定股票在日期范围内的收盘价。DB 无数据或不完整时从 Tushare 拉取补全。"""
        try:
            code = ts_code.split(".")[0] if "." in ts_code else ts_code
            records = self.db.get_data_range(
                code,
                date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8])),
                date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8])),
            )
            if records:
                prices = {}
                for r in records:
                    d = r.date.strftime("%Y%m%d") if isinstance(r.date, date) else str(r.date)[:8]
                    if r.close:
                        prices[d] = float(r.close)
                # DB 数据不完整时从 Tushare 补全
                last_db_date = max(prices.keys()) if prices else ""
                if last_db_date < end_date:
                    tf_prices = self._fetch_tushare_prices(ts_code, code, start_date, end_date)
                    prices.update(tf_prices)
                return prices

            # DB 无数据，从 Tushare 拉取
            return self._fetch_tushare_prices(ts_code, code, start_date, end_date)
        except Exception:
            pass
        return {}

    def _prefetch_prices(
        self, ts_codes: List[str], start_date: str, end_date: str, max_workers: int = 20
    ) -> Dict[str, Dict[str, float]]:
        """并行预取多只股票的价格数据，减少串行 Tushare 调用延迟。"""
        prices: Dict[str, Dict[str, float]] = {}
        if not ts_codes:
            return prices
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._get_stock_prices, tc, start_date, end_date): tc for tc in ts_codes}
            for f in as_completed(futures, timeout=120):
                tc = futures[f]
                try:
                    prices[tc] = f.result(timeout=15)
                except Exception:
                    prices[tc] = {}
        return prices

    def compute_backtest(self, month: str, top_n_per_broker: int = 10) -> Dict[str, Any]:
        """对指定月份金股池按券商分组做回测。

        回测逻辑：当月第一个交易日开盘买入 → 当月最后一个交易日收盘卖出。
        按券商分组，每组内等权分配资金。
        结果持久化到数据库，后续相同月份直接返回存储结果。

        Args:
            month: YYYYMM 格式月份
            top_n_per_broker: 每个券商最多取几只金股

        Returns:
            回测结果字典
        """
        # 优先从存储读取（不含 enrichment，前端从独立 enrichment 端点获取）
        stored = self.db.get_broker_backtest(month)
        if stored and stored.get("brokers"):
            # 检查是否有当前月份的股票不在存储结果中（新入库的价格数据）
            current_df = self.get_monthly_recommendations(month)
            if not current_df.empty:
                stored_codes = {sr["ts_code"] for sr in stored["stock_returns"]}
                current_codes = set(current_df["ts_code"].unique())
                missing = current_codes - stored_codes
                if missing:
                    logger.info(f"[BrokerRecommend] 回测 {month} 缓存缺失 {len(missing)} 只股票，补算")
                    year = int(month[:4])
                    mon = int(month[4:6])
                    last_day = calendar.monthrange(year, mon)[1]
                    month_start = f"{month}01"
                    month_end = f"{month}{last_day:02d}"
                    trading_days = self._get_trading_days(month_start, month_end)
                    if len(trading_days) < 2:
                        trading_days = [stored.get("buy_date", month_start), stored.get("sell_date", month_end)]
                    buy_date = trading_days[0]
                    sell_date = trading_days[-1]
                    # 并行预取缺失股票价格
                    price_cache = self._prefetch_prices(list(missing), month_start, month_end)
                    for ts in missing:
                        prices = price_cache.get(ts, {})
                        if not prices:
                            continue
                        available_dates = sorted(prices.keys())
                        if len(available_dates) < 2:
                            continue
                        buy_price = prices[available_dates[0]]
                        sell_price = prices[available_dates[-1]]
                        if not buy_price or not sell_price or buy_price <= 0:
                            continue
                        row = current_df[current_df["ts_code"] == ts]
                        name = str(row["name"].iloc[0]) if not row.empty else ""
                        broker_count = int(row["broker_count"].iloc[0]) if not row.empty else 1
                        broker = str(row["broker"].iloc[0]) if not row.empty else ""
                        daily_rets = []
                        prev_p = None
                        for td in trading_days:
                            p = prices.get(td)
                            if p and buy_price > 0:
                                cumulative = (p - buy_price) / buy_price
                                if prev_p and prev_p > 0:
                                    d_ret = (p - prev_p) / prev_p
                                else:
                                    d_ret = 0.0
                                daily_rets.append({"date": td, "price": round(p, 2), "return": round(d_ret, 4), "cumulative": round(cumulative, 4)})
                                prev_p = p
                        stored["stock_returns"].append({
                            "ts_code": ts, "name": name,
                            "broker_count": broker_count, "broker": broker,
                            "end_price": round(sell_price, 2),
                            "end_date": available_dates[-1],
                            "daily_returns": daily_rets,
                        })
                    # 持久化更新后的结果
                    self.db.save_broker_backtest(
                        month=month,
                        buy_date=stored["buy_date"],
                        sell_date=stored["sell_date"],
                        total_recommendations=stored["total_recommendations"],
                        unique_stocks=len(stored["stock_returns"]),
                        unique_brokers=stored["unique_brokers"],
                        stock_returns=stored["stock_returns"],
                        broker_returns=stored["brokers"],
                    )
            stored["next_month"] = self._next_month_str(month)
            logger.info(f"[BrokerRecommend] 回测 {month} 命中存储")
            return stored

        df = self.get_monthly_recommendations(month)
        if df is None or df.empty:
            return {"error": f"{month} 月无数据"}

        # 当月第一个交易日开盘买入 → 当月最后一个交易日收盘卖出
        next_month = self._next_month_str(month)
        year = int(month[:4])
        mon = int(month[4:6])

        last_day = calendar.monthrange(year, mon)[1]
        month_start = f"{month}01"
        month_end = f"{month}{last_day:02d}"
        trading_days = self._get_trading_days(month_start, month_end)

        if len(trading_days) < 2:
            return {"error": f"{next_month} 交易日不足"}

        buy_date = trading_days[0]
        sell_date = trading_days[-1]

        # 并行预取所有股票价格（DB 有则秒查，无则并发拉 Tushare）
        all_ts = df["ts_code"].unique().tolist()
        logger.info(f"[BrokerRecommend] 回测 {month} 预取 {len(all_ts)} 只股票价格...")
        price_cache = self._prefetch_prices(all_ts, month_start, month_end)

        # 按券商分组回测
        brokers_result: List[Dict[str, Any]] = []
        stock_results: Dict[str, Dict[str, Any]] = {}

        all_brokers = df["broker"].unique()

        for broker in all_brokers:
            broker_df = df[df["broker"] == broker].drop_duplicates("ts_code")
            stocks = broker_df.head(top_n_per_broker)

            broker_daily_returns: Dict[str, List[Dict[str, Any]]] = {}
            broker_wins = 0
            broker_total = 0
            broker_pnl_sum = 0.0

            for _, row in stocks.iterrows():
                ts = str(row["ts_code"])
                name = str(row.get("name", ""))
                broker_count = int(row.get("broker_count", 1))

                prices = price_cache.get(ts, {})
                if not prices:
                    continue

                available_dates = sorted(prices.keys())
                if len(available_dates) < 2:
                    continue
                buy_price = prices[available_dates[0]]
                sell_price = prices[available_dates[-1]]

                if not buy_price or not sell_price or buy_price <= 0:
                    continue

                ret = (sell_price - buy_price) / buy_price
                broker_wins += 1 if ret > 0 else 0
                broker_total += 1
                broker_pnl_sum += ret

                # 每日收益：return = 当日涨跌幅，cumulative = 累计收益
                daily_rets = []
                prev_price = None
                for td in trading_days:
                    p = prices.get(td)
                    if p and buy_price > 0:
                        cumulative = (p - buy_price) / buy_price
                        if prev_price and prev_price > 0:
                            d_ret = (p - prev_price) / prev_price
                        else:
                            d_ret = 0.0
                        daily_rets.append({
                            "date": td,
                            "price": round(p, 2),
                            "return": round(d_ret, 4),
                            "cumulative": round(cumulative, 4),
                        })
                        prev_price = p

                broker_daily_returns[ts] = daily_rets

                # 个股结果
                if ts not in stock_results:
                    stock_results[ts] = {
                        "ts_code": ts,
                        "name": name,
                        "broker_count": broker_count,
                        "broker": broker,
                        "end_price": round(sell_price, 2),
                        "end_date": available_dates[-1],
                        "daily_returns": [],
                    }
                    for td in trading_days:
                        stock_results[ts]["daily_returns"].append({
                            "date": td,
                            "return": None,
                            "cumulative": None,
                        })

                for dr in daily_rets:
                    for sd in stock_results[ts]["daily_returns"]:
                        if sd["date"] == dr["date"]:
                            sd["price"] = dr.get("price")
                            sd["return"] = dr["return"]
                            sd["cumulative"] = dr["cumulative"]

                # 前向填充 null cumulative，确保最后一天有值
                last_cum = None
                for sd in stock_results[ts]["daily_returns"]:
                    if sd["cumulative"] is not None:
                        last_cum = sd["cumulative"]
                    elif last_cum is not None:
                        sd["cumulative"] = last_cum

            if broker_total == 0:
                continue

            avg_ret = broker_pnl_sum / broker_total
            brokers_result.append({
                "broker": broker,
                "stock_count": broker_total,
                "cumulative_return": round(broker_pnl_sum / broker_total, 4),
                "win_rate": round(broker_wins / broker_total, 4),
                "avg_return": round(avg_ret, 4),
                "daily_returns": self._merge_broker_daily_returns(broker_daily_returns, trading_days),
                "stocks": [
                    {"ts_code": str(r["ts_code"]), "name": str(r.get("name", ""))}
                    for _, r in stocks.iterrows()
                ],
            })

        # 按券商组合累计收益率降序排列
        brokers_result.sort(key=lambda x: x["cumulative_return"], reverse=True)

        stock_returns_list = list(stock_results.values())

        # 持久化存储（仅价格收益，不含增强数据）
        self.db.save_broker_backtest(
            month=month,
            buy_date=buy_date,
            sell_date=sell_date,
            total_recommendations=len(df),
            unique_stocks=df["ts_code"].nunique(),
            unique_brokers=len(all_brokers),
            stock_returns=[{
                "ts_code": sr["ts_code"],
                "name": sr["name"],
                "broker_count": sr["broker_count"],
                "broker": sr["broker"],
                "end_price": sr.get("end_price"),
                "end_date": sr.get("end_date"),
                "daily_returns": sr["daily_returns"],
            } for sr in stock_returns_list],
            broker_returns=brokers_result,
        )

        return {
            "month": month,
            "next_month": next_month,
            "buy_date": buy_date,
            "sell_date": sell_date,
            "total_recommendations": len(df),
            "unique_stocks": df["ts_code"].nunique(),
            "unique_brokers": len(all_brokers),
            "brokers": brokers_result,
            "stock_returns": stock_returns_list,
        }

    def compute_ytd_backtest(self, year: str, top_n: int = 5) -> Dict[str, Any]:
        """年初至今累计回测：跨月复合月度回测结果。

        遍历年内所有月份，将每个月的券商组合累计收益跨月乘法复合，
        daily_returns 拼接为连续曲线。月度数据命中 SQLite 缓存，无额外 Tushare 调用。
        """
        available_months = self.get_available_months()
        year_months = sorted([m for m in available_months if str(m).startswith(str(year))])

        if not year_months:
            return {"error": f"Year {year} has no data"}

        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        broker_ytd: Dict[str, Dict[str, Any]] = {}

        for month in year_months:
            # 优先从存储读取，若 sell_date 在未来则跳过（当月未结束、无完整交易数据）
            stored = self.db.get_broker_backtest(month)
            if stored and stored.get("brokers") and stored.get("sell_date", "99991231") <= today:
                bt = stored
            elif stored and stored.get("sell_date", "99991231") > today:
                logger.info(f"[BrokerRecommend] YTD 跳过未完成月份 {month}")
                continue
            else:
                bt = self.compute_backtest(month, top_n_per_broker=10)

            if "error" in bt:
                continue

            for b in bt.get("brokers", []):
                broker_name = b["broker"]
                if broker_name not in broker_ytd:
                    broker_ytd[broker_name] = {
                        "broker": broker_name,
                        "active_months": 0,
                        "cumulative_return": 0.0,
                        "_prev_cum": 0.0,
                        "daily_returns": [],
                        "monthly_returns": [],
                    }

                entry = broker_ytd[broker_name]
                entry["active_months"] += 1

                prev_factor = 1.0 + entry["_prev_cum"]
                for dr in b.get("daily_returns", []):
                    month_day_cum = dr.get("cumulative", 0.0) or 0.0
                    ytd_cum = prev_factor * (1.0 + month_day_cum) - 1.0
                    entry["daily_returns"].append({
                        "date": dr["date"],
                        "cumulative": round(ytd_cum, 4),
                    })

                month_broker_ret = b.get("cumulative_return", 0.0) or 0.0
                entry["_prev_cum"] = (
                    (1.0 + entry["_prev_cum"]) * (1.0 + month_broker_ret) - 1.0
                )
                entry["cumulative_return"] = round(entry["_prev_cum"], 4)

                entry["monthly_returns"].append({
                    "month": month,
                    "cumulative_return": round(month_broker_ret, 4),
                    "stock_count": b.get("stock_count", 0),
                    "win_rate": round(b.get("win_rate", 0.0), 4),
                })

        sorted_brokers = sorted(
            broker_ytd.values(), key=lambda x: x["cumulative_return"], reverse=True,
        )[:top_n]

        all_dates: set = set()
        for b in sorted_brokers:
            del b["_prev_cum"]
            prev_cum = 0.0
            for dr in b["daily_returns"]:
                cum = dr["cumulative"]
                dr["return"] = round(cum - prev_cum, 4)
                prev_cum = cum
                all_dates.add(dr["date"])

        start_date = min(all_dates) if all_dates else f"{year}0101"
        end_date = max(all_dates) if all_dates else f"{year}1231"

        logger.info(f"[BrokerRecommend] YTD {year}: {len(broker_ytd)} brokers, "
                    f"top {len(sorted_brokers)}, {len(year_months)} months")

        return {
            "year": str(year),
            "start_date": start_date,
            "end_date": end_date,
            "total_brokers": len(broker_ytd),
            "brokers": sorted_brokers,
        }

    def _merge_broker_daily_returns(
        self, stock_returns: Dict[str, List[Dict[str, Any]]], trading_days: List[str]
    ) -> List[Dict[str, Any]]:
        """合并多只股票的每日收益为组合每日等权收益。

        用个股累计收益的等权平均得到组合累计收益，
        组合日收益 = 累计的日环比变化。
        """
        if not stock_returns:
            return []

        result = []
        prev_cum = 0.0
        for td in trading_days:
            daily_cums = []
            for ts, rets in stock_returns.items():
                for r in rets:
                    if r["date"] == td and r.get("cumulative") is not None:
                        daily_cums.append(r["cumulative"])
                        break

            if daily_cums:
                cumulative = sum(daily_cums) / len(daily_cums)
            else:
                cumulative = prev_cum

            daily_ret = cumulative - prev_cum
            result.append({
                "date": td,
                "return": round(daily_ret, 4),
                "cumulative": round(cumulative, 4),
            })
            prev_cum = cumulative

        return result

    def _enrich_stock_results(
        self, stock_results: Dict[str, Dict[str, Any]], ts_codes: List[str], trade_date: str
    ) -> None:
        """为回测结果附加筹码胜率、神奇九转、券商盈利预测。

        直接修改 stock_results dict（in-place）。
        """
        # 1. 筹码胜率（全量拉取，fail-open）
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            cyq_df = tf.get_bulk_cyq_perf(trade_date) if tf.is_available() else None
            if cyq_df is not None and not cyq_df.empty:
                for ts_code in ts_codes:
                    if ts_code in cyq_df.index:
                        row = cyq_df.loc[ts_code]
                        cost_5 = float(row.get("cost_5pct", 0) or 0)
                        cost_95 = float(row.get("cost_95pct", 0) or 0)
                        weight_avg = float(row.get("weight_avg", 0) or 0)
                        winner_rate = float(row.get("winner_rate", 0) or 0) / 100.0
                        stock_results[ts_code]["cyq_perf"] = {
                            "cost_avg": round(weight_avg, 2),
                            "winner_rate": round(winner_rate, 4),
                            "concentration": round(
                                (cost_95 - cost_5) / weight_avg, 4
                            ) if weight_avg > 0 else None,
                        }
        except Exception as e:
            logger.debug(f"[BrokerRecommend] 筹码胜率 enrichment 失败: {e}")

        # 2. 神奇九转（逐条，fail-open）
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            if tf.is_available():
                nineturn_data = tf.get_bulk_nineturn(ts_codes, trade_date)
                for ts_code, nt in nineturn_data.items():
                    if ts_code in stock_results:
                        stock_results[ts_code]["nineturn"] = {
                            "up_count": nt.get("up_count", 0),
                            "down_count": nt.get("down_count", 0),
                            "nine_up_turn": nt.get("nine_up_turn", 0),
                            "nine_down_turn": nt.get("nine_down_turn", 0),
                        }
        except Exception as e:
            logger.debug(f"[BrokerRecommend] 神奇九转 enrichment 失败: {e}")

        # 3. 券商盈利预测（逐条，fail-open）
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            if tf.is_available():
                for ts_code in ts_codes:
                    try:
                        fc = tf.get_forecast(
                            ts_code.split(".")[0] if "." in ts_code else ts_code,
                            end_date=trade_date,
                        )
                        if fc and ts_code in stock_results:
                            stock_results[ts_code]["forecast"] = {
                                "eps": fc.get("eps"),
                                "pe": fc.get("pe"),
                                "roe": fc.get("roe"),
                                "np": fc.get("np"),
                                "rating": fc.get("rating", ""),
                                "min_price": fc.get("min_price"),
                                "max_price": fc.get("max_price"),
                                "imp_dg": fc.get("imp_dg", ""),
                            }
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"[BrokerRecommend] 盈利预测 enrichment 失败: {e}")

    def _enrich_stock_results_dict(
        self, stock_list: List[Dict[str, Any]], ts_codes: List[str], trade_date: str
    ) -> None:
        """为存储回测的股票列表（list of dict）附加增强数据，in-place 修改。"""
        # 构建 dict 映射以复用 _enrich_stock_results
        stock_map = {sr["ts_code"]: sr for sr in stock_list}
        self._enrich_stock_results(stock_map, ts_codes, trade_date)
