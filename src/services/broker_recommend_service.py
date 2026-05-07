# -*- coding: utf-8 -*-
"""券商月度金股推荐服务。

提供券商金股数据的获取、存储和回测功能。
"""

import calendar
import logging
import random
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

    # 缓存每个月份的 query_date，避免 trade_cal API 波动导致缓存 key 不一致
    _query_date_cache: Dict[str, str] = {}

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

    def invalidate_enrichment_cache(self, month: str) -> int:
        """清除指定月份的 enrichment 缓存（L1 内存 + L2 SQLite）。

        用于当前月抓取新数据后强制刷新价格和筹码胜率。
        """
        from datetime import datetime
        removed_l1 = 0

        # L1 内存缓存：按 query_date 前缀匹配清除
        query_date = self._resolve_enrichment_date(month)
        # 对于当前月，query_date 返回最近交易日，直接用它清除
        prefix = f"{query_date}:"
        with self._cache_lock:
            keys_to_del = [k for k in self._enrichment_cache if prefix in k]
            for k in keys_to_del:
                del self._enrichment_cache[k]
                self._enrichment_cache_ts.pop(k, None)
            removed_l1 = len(keys_to_del)

        # L2 SQLite 缓存：清除该日期的 enrichment ORM 记录
        try:
            from sqlalchemy import delete as sa_delete
            from src.storage import BrokerEnrichmentNineturn, BrokerEnrichmentForecast, BrokerEnrichmentCyqPerf
            with self.db.get_session() as session:
                for model in (BrokerEnrichmentNineturn, BrokerEnrichmentForecast, BrokerEnrichmentCyqPerf):
                    session.execute(sa_delete(model).where(model.trade_date == query_date))
                session.commit()
        except Exception as e:
            logger.debug(f"[BrokerRecommend] L2 cache clear failed: {e}")

        # 同时清除 _query_date_cache，强制重新计算交易日
        self._query_date_cache.pop(month, None)

        if removed_l1 > 0:
            logger.info(f"[BrokerRecommend] 已清除 {removed_l1} 条 L1 + L2 enrichment 缓存 (month={month}, date={query_date})")
        return removed_l1

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
        """确定增强数据的查询日期。

        历史月份：首次计算后缓存，后续直接返回（确保 cache key 一致）。
        当前月份：返回最近交易日（不缓存，实现每日刷新）。
        """
        from datetime import datetime
        year = int(month[:4])
        mon = int(month[4:6])
        last_day = calendar.monthrange(year, mon)[1]
        month_last = f"{month}{last_day:02d}"
        today = datetime.now().strftime("%Y%m%d")

        if month_last <= today:
            # 历史月份：优先返回缓存结果，避免 trade_cal API 波动
            if month in BrokerRecommendService._query_date_cache:
                return BrokerRecommendService._query_date_cache[month]
            # 尝试获取真实交易日，失败则用 weekday 估算
            try:
                trading_days = self._get_trading_days(f"{month}01", month_last)
                if trading_days:
                    result = trading_days[-1]
                    BrokerRecommendService._query_date_cache[month] = result
                    return result
            except Exception:
                pass
            # fallback: 回退到该月最后一个工作日
            result = self._last_weekday(month_last)
            BrokerRecommendService._query_date_cache[month] = result
            return result

        # 当前/未来月份：动态获取最近交易日，不缓存
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            trade_date = tf.get_trade_time(early_time='00:00', late_time='19:00')
            if trade_date:
                return trade_date
        except Exception:
            pass
        return month_last

    @staticmethod
    def _last_weekday(date_str: str) -> str:
        """回退到最近的工作日（周一到周五）。"""
        from datetime import timedelta
        d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")

    @staticmethod
    def _normalize_cyq_perf(data: Dict[str, Any]) -> Dict[str, Any]:
        """从 L2 原始字段补全 computed 字段（cost_avg、concentration、scr90）。

        L2 存储的是 Tushare 原始字段，前端需要 cost_avg/concentration/scr90。
        """
        if "cost_avg" not in data or data["cost_avg"] is None:
            wavg = data.get("weight_avg")
            if wavg is not None:
                try:
                    data["cost_avg"] = round(float(wavg), 2)
                except (ValueError, TypeError):
                    pass
        if "concentration" not in data or data["concentration"] is None:
            c5 = data.get("cost_5pct")
            c95 = data.get("cost_95pct")
            wavg = data.get("weight_avg")
            if c5 is not None and c95 is not None and wavg and float(wavg) > 0:
                try:
                    data["concentration"] = round((float(c95) - float(c5)) / float(wavg), 4)
                except (ValueError, TypeError):
                    pass
        if "scr90" not in data or data["scr90"] is None:
            c5 = data.get("cost_5pct")
            c15 = data.get("cost_15pct")
            c50 = data.get("cost_50pct")
            c85 = data.get("cost_85pct")
            c95 = data.get("cost_95pct")
            if all(v is not None for v in (c5, c15, c50, c85, c95)) and float(c50) > 0:
                try:
                    cost90 = (float(c95) + float(c85)) / 2
                    cost10 = (float(c5) + float(c15)) / 2
                    data["scr90"] = round((cost90 - cost10) / float(c50) * 100, 2)
                except (ValueError, TypeError):
                    pass
        return data

    def _effective_month_end(self, month: str) -> str:
        """回测有效截止日。历史月取月末；当月：收盘前取前一交易日，收盘后取今天。"""
        year = int(month[:4])
        mon = int(month[4:6])
        last_day = calendar.monthrange(year, mon)[1]
        month_end = f"{month}{last_day:02d}"
        today = date.today()
        today_str = today.strftime("%Y%m%d")
        if month_end > today_str:
            from datetime import datetime
            now = datetime.now()
            # A 股 15:00 收盘，收盘后当天数据完整可展示
            if now.hour >= 15:
                return today_str
            else:
                # 用交易日历找前一个交易日
                month_start = f"{month}01"
                trading_days = self._get_trading_days(month_start, today_str)
                # 排除今天，取前一个交易日
                prev_days = [d for d in trading_days if d < today_str]
                if prev_days:
                    return prev_days[-1]
                # fallback：向前找最近的工作日
                for i in range(1, 8):
                    candidate = today - timedelta(days=i)
                    if candidate.weekday() < 5:
                        return candidate.strftime("%Y%m%d")
                return (today - timedelta(days=1)).strftime("%Y%m%d")
        return month_end

    def get_monthly_enrichment(self, month: str) -> Dict[str, Dict[str, Any]]:
        """获取指定月份所有推荐股票的增强数据（九转、盈利预测、筹码胜率）。

        L1 进程内缓存 → L2 SQLite 持久化缓存 → L3 Tushare API。
        历史月份 trade_date 固定 → SQLite 永久有效；当前月份按交易日刷新。
        返回 {ts_code: {nineturn, forecast, cyq_perf}} 字典。
        """
        df = self.get_monthly_recommendations(month)
        if df.empty:
            return {}

        ts_codes = df["ts_code"].unique().tolist()
        query_date = self._resolve_enrichment_date(month)

        enrichment: Dict[str, Dict[str, Any]] = {}
        uncached_nineturn: List[str] = []
        uncached_forecast: List[str] = []
        uncached_cyq: List[str] = []

        # L1: 进程内缓存
        for tc in ts_codes:
            entry: Dict[str, Any] = {}
            nt = self._get_cached(tc, query_date, "nineturn")
            if nt is not None:
                entry["nineturn"] = nt
            else:
                uncached_nineturn.append(tc)
            fc = self._get_cached(tc, query_date, "forecast")
            if fc is not None:
                entry["forecast"] = fc
            else:
                uncached_forecast.append(tc)
            cyq = self._get_cached(tc, query_date, "cyq_perf")
            if cyq is not None:
                entry["cyq_perf"] = cyq
            else:
                uncached_cyq.append(tc)
            if entry:
                enrichment[tc] = entry

        total_fields = len(ts_codes) * 3
        l1_hits = sum(1 for v in enrichment.values() for _ in v)
        if l1_hits == total_fields:
            logger.info(f"[BrokerRecommend] enrichment L1 全部命中 {month} ({len(ts_codes)} stocks)")
            return enrichment

        # L2: SQLite 持久化缓存
        still_need_nineturn: List[str] = []
        still_need_forecast: List[str] = []
        still_need_cyq: List[str] = []

        if uncached_nineturn or uncached_forecast or uncached_cyq:
            all_missed = list(set(uncached_nineturn + uncached_forecast + uncached_cyq))
            db_cache = self.db.get_enrichment_cache(all_missed, query_date)
            if db_cache:
                for tc, data in db_cache.items():
                    if "nineturn" in data:
                        BrokerRecommendService._set_cached(tc, query_date, "nineturn", data["nineturn"])
                        enrichment.setdefault(tc, {})["nineturn"] = data["nineturn"]
                    if "forecast" in data:
                        BrokerRecommendService._set_cached(tc, query_date, "forecast", data["forecast"])
                        enrichment.setdefault(tc, {})["forecast"] = data["forecast"]
                    if "cyq_perf" in data:
                        normalized = BrokerRecommendService._normalize_cyq_perf(data["cyq_perf"])
                        if normalized.get("cost_avg") is not None:
                            BrokerRecommendService._set_cached(tc, query_date, "cyq_perf", normalized)
                            enrichment.setdefault(tc, {})["cyq_perf"] = normalized

            for tc in uncached_nineturn:
                if tc not in enrichment or "nineturn" not in enrichment[tc]:
                    still_need_nineturn.append(tc)
            for tc in uncached_forecast:
                if tc not in enrichment or "forecast" not in enrichment[tc]:
                    still_need_forecast.append(tc)
            for tc in uncached_cyq:
                if tc not in enrichment or "cyq_perf" not in enrichment[tc]:
                    still_need_cyq.append(tc)

        l2_hits = sum(1 for v in enrichment.values() for _ in v)
        if l2_hits == total_fields:
            logger.info(f"[BrokerRecommend] enrichment L1+L2 全部命中 {month} ({len(ts_codes)} stocks)")
            return enrichment

        logger.info(f"[BrokerRecommend] enrichment {month}: L1+L2 命中 {l2_hits}/{total_fields}, "
                    f"待 fetch nineturn={len(still_need_nineturn)} forecast={len(still_need_forecast)} cyq={len(still_need_cyq)}")

        # L3: Tushare API 批量获取
        from data_provider.tushare_fetcher import TushareFetcher
        tf = TushareFetcher.get_instance()
        if not tf.is_available():
            logger.warning("[BrokerRecommend] Tushare 不可用，仅返回缓存数据")
            return enrichment

        fetched_nineturn: Dict[str, Dict[str, Any]] = {}
        fetched_forecast: Dict[str, Dict[str, Any]] = {}
        fetched_cyq: Dict[str, Dict[str, Any]] = {}

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures: dict = {}

            if still_need_nineturn:
                futures[pool.submit(tf.get_bulk_nineturn, still_need_nineturn, query_date)] = "nineturn"
            if still_need_forecast:
                futures[pool.submit(tf.get_bulk_forecast, still_need_forecast, query_date)] = "forecast"
            if still_need_cyq:
                futures[pool.submit(self._fetch_cyq_enrichment, tf, still_need_cyq, query_date)] = "cyq"

            for future in as_completed(futures, timeout=60):
                tag = futures[future]
                try:
                    if tag == "nineturn":
                        nt_data = future.result(timeout=30)
                        if nt_data:
                            for ts_code, nt in nt_data.items():
                                result = {
                                    "trade_date": query_date,
                                    "up_count": nt.get("up_count", 0),
                                    "down_count": nt.get("down_count", 0),
                                    "nine_up_turn": nt.get("nine_up_turn", 0),
                                    "nine_down_turn": nt.get("nine_down_turn", 0),
                                }
                                BrokerRecommendService._set_cached(ts_code, query_date, "nineturn", result)
                                enrichment.setdefault(ts_code, {})["nineturn"] = result
                                fetched_nineturn[ts_code] = result
                    elif tag == "forecast":
                        fc_data = future.result(timeout=30)
                        if fc_data:
                            for ts_code, fc in fc_data.items():
                                result = {
                                    "trade_date": query_date,
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
                                fetched_forecast[ts_code] = result
                    elif tag == "cyq":
                        cyq_data = future.result(timeout=30)
                        if cyq_data:
                            for ts_code, cyq in cyq_data.items():
                                cyq["trade_date"] = query_date
                                BrokerRecommendService._set_cached(ts_code, query_date, "cyq_perf", cyq)
                                enrichment.setdefault(ts_code, {})["cyq_perf"] = cyq
                                fetched_cyq[ts_code] = cyq
                except Exception:
                    pass

        # L3.5: akshare 筹码集中度覆盖（当日实时数据，优先于 Tushare SCR90）
        akshare_cyq_codes = [tc for tc in ts_codes if tc not in enrichment or "cyq_perf" not in enrichment[tc]]
        if akshare_cyq_codes:
            try:
                akshare_data = BrokerRecommendService._fetch_cyq_akshare(akshare_cyq_codes)
                if akshare_data:
                    for ts_code, cyq in akshare_data.items():
                        cyq["trade_date"] = query_date
                        BrokerRecommendService._set_cached(ts_code, query_date, "cyq_perf", cyq)
                        enrichment.setdefault(ts_code, {})["cyq_perf"] = cyq
                        fetched_cyq[ts_code] = cyq
                    logger.info(f"[BrokerRecommend] akshare cyq 覆盖 {len(akshare_data)} 只")
            except Exception as e:
                logger.debug(f"[BrokerRecommend] akshare cyq 批量获取失败: {e}")

        # 对 Tushare 无数据的股票缓存空标记，避免重复拉取
        for tc in still_need_nineturn:
            if tc not in fetched_nineturn and "nineturn" not in enrichment.get(tc, {}):
                empty = {"trade_date": query_date, "up_count": 0, "down_count": 0,
                         "nine_up_turn": 0, "nine_down_turn": 0}
                BrokerRecommendService._set_cached(tc, query_date, "nineturn", empty)
                enrichment.setdefault(tc, {})["nineturn"] = empty
                fetched_nineturn[tc] = empty
        for tc in still_need_forecast:
            if tc not in fetched_forecast and "forecast" not in enrichment.get(tc, {}):
                empty = {"trade_date": query_date, "eps": None, "pe": None, "roe": None, "np": None,
                         "rating": "", "min_price": None, "max_price": None, "imp_dg": ""}
                BrokerRecommendService._set_cached(tc, query_date, "forecast", empty)
                enrichment.setdefault(tc, {})["forecast"] = empty
                fetched_forecast[tc] = empty
        for tc in still_need_cyq:
            if tc not in fetched_cyq and "cyq_perf" not in enrichment.get(tc, {}):
                empty = {"trade_date": query_date, "winner_rate": None, "cost_5pct": None,
                         "cost_15pct": None, "cost_50pct": None, "cost_85pct": None,
                         "cost_95pct": None, "weight_avg": None, "his_low": None, "his_high": None,
                         "scr90": None, "concentration": None, "cost_avg": None}
                BrokerRecommendService._set_cached(tc, query_date, "cyq_perf", empty)
                enrichment.setdefault(tc, {})["cyq_perf"] = empty
                fetched_cyq[tc] = empty

        # 持久化到 SQLite（含空标记）
        if fetched_nineturn or fetched_forecast or fetched_cyq:
            try:
                self.db.save_enrichment_cache(
                    nineturn_data=fetched_nineturn or None,
                    forecast_data=fetched_forecast or None,
                    cyq_data=fetched_cyq or None,
                )
            except Exception:
                pass

        logger.info(f"[BrokerRecommend] enrichment 完成 {month}: nineturn={sum(1 for v in enrichment.values() if 'nineturn' in v)}, "
                    f"forecast={sum(1 for v in enrichment.values() if 'forecast' in v)}, "
                    f"cyq={sum(1 for v in enrichment.values() if 'cyq_perf' in v)}")
        return enrichment

    @staticmethod
    def _fetch_cyq_akshare(ts_codes: List[str]) -> Optional[Dict[str, Dict[str, Any]]]:
        """使用 akshare (东方财富) 获取筹码分布，支持当日盘中实时数据。"""
        import os
        from unittest.mock import patch

        try:
            import akshare as ak
        except ImportError:
            logger.debug("[BrokerRecommend] akshare 未安装，跳过 cyq")
            return None

        # 构建无代理污染的 requests Session（akshare 内部使用 requests.get）
        import requests as _requests
        _session = _requests.Session()
        _session.trust_env = False  # 禁止读取环境变量中的代理配置
        _session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": "https://quote.eastmoney.com/",
        })

        # 临时清除代理环境变量，防止 akshare/requests 内部读取
        _saved_proxy_vars = {}
        for k in ("all_proxy", "ALL_PROXY", "http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "no_proxy", "NO_PROXY",
                  "USE_PROXY", "PROXY_HOST", "PROXY_PORT"):
            if k in os.environ:
                _saved_proxy_vars[k] = os.environ.pop(k)

        def _patched_get(url, **kwargs):
            return _session.get(url, **kwargs)

        result: Dict[str, Dict[str, Any]] = {}
        try:
            with patch.object(_requests, "get", side_effect=_patched_get):
                for ts_code in ts_codes:
                    try:
                        symbol = ts_code.split(".")[0] if "." in ts_code else ts_code
                        df = ak.stock_cyq_em(symbol=symbol)
                        if df is None or df.empty:
                            continue

                        row = df.iloc[-1]
                        winner_rate = float(row.get("获利比例", 0) or 0)
                        cost_avg = float(row.get("平均成本", 0) or 0)
                        concentration = float(row.get("90集中度", 0) or 0)
                        cost_low_90 = float(row.get("90成本-低", 0) or 0)
                        cost_high_90 = float(row.get("90成本-高", 0) or 0)
                        cost_low_70 = float(row.get("70成本-低", 0) or 0)
                        cost_high_70 = float(row.get("70成本-高", 0) or 0)

                        his_low = float(df["平均成本"].min()) if len(df) > 0 else cost_low_90
                        his_high = float(df["平均成本"].max()) if len(df) > 0 else cost_high_90

                        result[ts_code] = {
                            "cost_avg": round(cost_avg, 2),
                            "winner_rate": round(winner_rate, 4),
                            "concentration": round(concentration, 4),
                            "cost_5pct": cost_low_90,
                            "cost_15pct": cost_low_70,
                            "cost_50pct": None,
                            "cost_85pct": cost_high_70,
                            "cost_95pct": cost_high_90,
                            "weight_avg": cost_avg,
                            "his_low": round(his_low, 2),
                            "his_high": round(his_high, 2),
                        }
                        time.sleep(2.0 + random.random() * 2.0)  # 2-4s 随机延迟，避免触发东方财富反爬
                    except Exception as e:
                        logger.debug(f"[BrokerRecommend] akshare cyq failed for {ts_code}: {e}")
                        continue
        finally:
            # 恢复代理环境变量
            os.environ.update(_saved_proxy_vars)
        return result if result else None

    def _fetch_cyq_enrichment(
        self, tf: Any, ts_codes: List[str], query_date: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """获取筹码胜率数据（在线程池中执行）。使用 Tushare 日线 CYQ 数据。"""
        try:
            cyq_df = tf.get_bulk_cyq_perf(query_date) if tf.is_available() else None
            if cyq_df is None or cyq_df.empty:
                return None
            result = {}
            for ts_code in ts_codes:
                if ts_code in cyq_df.index:
                    row = cyq_df.loc[ts_code]
                    cost_5 = float(row.get("cost_5pct", 0) or 0)
                    cost_95 = float(row.get("cost_95pct", 0) or 0)
                    weight_avg = float(row.get("weight_avg", 0) or 0)
                    winner_rate = float(row.get("winner_rate", 0) or 0) / 100.0
                    cost_5 = float(row.get("cost_5pct", 0) or 0)
                    cost_15 = float(row.get("cost_15pct", 0) or 0)
                    cost_50 = float(row.get("cost_50pct", 0) or 0)
                    cost_85 = float(row.get("cost_85pct", 0) or 0)
                    cost_95 = float(row.get("cost_95pct", 0) or 0)
                    cost90 = (cost_95 + cost_85) / 2
                    cost10 = (cost_5 + cost_15) / 2
                    scr90 = round((cost90 - cost10) / cost_50 * 100, 2) if cost_50 > 0 else None
                    result[ts_code] = {
                        "cost_avg": round(weight_avg, 2),
                        "winner_rate": round(winner_rate, 4),
                        "concentration": round(
                            (cost_95 - cost_5) / weight_avg, 4
                        ) if weight_avg > 0 else None,
                        "scr90": scr90,
                        "cost_5pct": cost_5,
                        "cost_15pct": cost_15,
                        "cost_50pct": cost_50,
                        "cost_85pct": cost_85,
                        "cost_95pct": cost_95,
                        "weight_avg": weight_avg,
                        "his_low": float(row.get("his_low", 0) or 0),
                        "his_high": float(row.get("his_high", 0) or 0),
                    }
            return result
        except Exception as e:
            logger.debug(f"[BrokerRecommend] Tushare cyq fallback 失败: {e}")
            return None

    # ── 本地 CYQ 计算（StockDaily kline + Tushare 总市值换算流通股本） ──

    _total_share_cache: Dict[str, float] = {}

    @classmethod
    def _get_total_shares(cls, ts_codes: List[str]) -> Dict[str, float]:
        """获取总股本（万股），带内存缓存。

        通过 Tushare daily_basic 的 total_mv / close 反推总股本，
        一次 API 调用覆盖全市场，无需 stock_basic 高级权限。
        """
        from data_provider.tushare_fetcher import TushareFetcher

        missing = [tc for tc in ts_codes if tc not in cls._total_share_cache]
        if not missing:
            return {tc: cls._total_share_cache[tc] for tc in ts_codes if tc in cls._total_share_cache}

        tf = TushareFetcher.get_instance()
        if tf.is_available():
            trade_date = tf.get_trade_time(early_time="00:00", late_time="19:00")
            if trade_date:
                try:
                    df_basic = tf.get_daily_basic_all(trade_date)
                    if df_basic is not None and not df_basic.empty:
                        for tc in missing:
                            if tc not in df_basic.index:
                                continue
                            row = df_basic.loc[tc]
                            total_mv = row.get("total_mv")
                            if total_mv is None or float(total_mv) <= 0:
                                continue
                            # 从 StockDaily 取当日 close 来反推总股本
                            try:
                                code = tc.split(".")[0] if "." in tc else tc
                                t_date = date(int(trade_date[:4]), int(trade_date[4:6]), int(trade_date[6:8]))
                                t_records = DatabaseManager.get_instance().get_data_range(code, t_date, t_date)
                                if t_records and t_records[0].close:
                                    close_price = float(t_records[0].close)
                                    if close_price > 0:
                                        # total_share(万股) = total_mv(万元) / close(元)
                                        cls._total_share_cache[tc] = float(total_mv) / close_price
                            except Exception:
                                pass
                except Exception:
                    pass

        return {tc: cls._total_share_cache[tc] for tc in ts_codes if tc in cls._total_share_cache}

    @staticmethod
    def _calc_cyq_from_klines(klines: List[Dict[str, float]]) -> Optional[Dict[str, Any]]:
        """纯 Python 实现东方财富 CYQ 算法（与 akshare stock_cyq_em JS 逻辑一致）。

        klines: [{"open", "close", "high", "low", "hsl"}, ...] 按日期升序，hsl 为换手率百分比(0-100)。
        返回最后一根 K 线的筹码分布指标。
        """
        if len(klines) < 5:
            return None

        factor = 150
        range_days = 120
        start = max(0, len(klines) - range_days)
        kdata = klines[start:]

        maxprice = max(k["high"] for k in kdata)
        minprice = min(k["low"] for k in kdata)
        if maxprice <= minprice:
            return None

        accuracy = max(0.01, (maxprice - minprice) / (factor - 1))
        xdata = [0.0] * factor

        for day in kdata:
            open_p = day["open"]
            close = day["close"]
            high = day["high"]
            low = day["low"]
            hsl = min(1.0, day.get("hsl", 0) / 100.0)
            avg = (open_p + close + high + low) / 4.0

            # 衰减
            for n in range(factor):
                xdata[n] *= (1.0 - hsl)

            h_idx = int((high - minprice) / accuracy)
            l_idx = int((low - minprice) / accuracy + 0.999999)  # ceil
            gp = 2.0 / (high - low) if high != low else float(factor - 1)
            avg_idx = int((avg - minprice) / accuracy)
            avg_idx = max(0, min(factor - 1, avg_idx))

            if high == low:
                xdata[avg_idx] += gp * hsl / 2.0
            else:
                for j in range(l_idx, h_idx + 1):
                    if j < 0 or j >= factor:
                        continue
                    curprice = minprice + accuracy * j
                    if curprice <= avg:
                        if abs(avg - low) < 1e-8:
                            xdata[j] += gp * hsl
                        else:
                            xdata[j] += (curprice - low) / (avg - low) * gp * hsl
                    else:
                        if abs(high - avg) < 1e-8:
                            xdata[j] += gp * hsl
                        else:
                            xdata[j] += (high - curprice) / (high - avg) * gp * hsl

        current_price = kdata[-1]["close"]
        total_chips = sum(xdata)
        if total_chips <= 0:
            return None

        # 获利比例
        below = 0.0
        for i in range(factor):
            if current_price >= minprice + i * accuracy:
                below += xdata[i]
        winner_rate = below / total_chips

        # 成本函数：指定筹码量对应的价格
        def cost_at(chips: float) -> float:
            acc = 0.0
            for i in range(factor):
                if acc + xdata[i] > chips:
                    return minprice + i * accuracy
                acc += xdata[i]
            return minprice + (factor - 1) * accuracy

        avg_cost = cost_at(total_chips * 0.5)

        def percent_chips(pct: float) -> Dict[str, Any]:
            lo = cost_at(total_chips * (1.0 - pct) / 2.0)
            hi = cost_at(total_chips * (1.0 + pct) / 2.0)
            conc = (hi - lo) / (hi + lo) if (hi + lo) != 0 else 0.0
            return {"lo": round(lo, 2), "hi": round(hi, 2), "concentration": round(conc, 4)}

        pct90 = percent_chips(0.9)
        pct70 = percent_chips(0.7)

        return {
            "cost_avg": round(avg_cost, 2),
            "winner_rate": round(winner_rate, 4),
            "concentration": round(pct90["concentration"], 4),
            "cost_5pct": pct90["lo"],
            "cost_15pct": pct70["lo"],
            "cost_50pct": None,
            "cost_85pct": pct70["hi"],
            "cost_95pct": pct90["hi"],
            "weight_avg": round(avg_cost, 2),
            "his_low": round(minprice, 2),
            "his_high": round(maxprice, 2),
        }

    def _compute_cyq_local(
        self, ts_codes: List[str],
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """用本地 StockDaily kline + Tushare 流通股本计算筹码分布。"""
        from datetime import datetime, timedelta

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")

        # 批量获取总股本
        total_shares = self._get_total_shares(ts_codes)
        if not total_shares:
            return None

        result: Dict[str, Dict[str, Any]] = {}
        for ts_code in ts_codes:
            try:
                total_share = total_shares.get(ts_code)
                if not total_share:
                    continue

                code = ts_code.split(".")[0] if "." in ts_code else ts_code
                s_date = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
                e_date = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
                records = self.db.get_data_range(code, s_date, e_date)

                if not records or len(records) < 10:
                    continue

                klines: List[Dict[str, float]] = []
                for r in records:
                    if r.open is None or r.close is None or r.high is None or r.low is None:
                        continue
                    vol = float(r.volume) if r.volume else 0
                    # 换手率(%) = volume(手) / total_share(万股)
                    hsl = (vol / total_share) if total_share > 0 else 0
                    klines.append({
                        "open": float(r.open),
                        "close": float(r.close),
                        "high": float(r.high),
                        "low": float(r.low),
                        "hsl": min(hsl, 100.0),  # 单日换手率上限 100%
                    })

                cyq = BrokerRecommendService._calc_cyq_from_klines(klines)
                if cyq:
                    result[ts_code] = cyq
            except Exception:
                continue

        return result if result else None

    def get_available_months(self) -> List[str]:
        """获取有数据的月份列表。"""
        return self.db.get_broker_recommend_months()

    def get_consecutive_stocks(self, month: str) -> List[Dict[str, Any]]:
        """获取连续两个月都被券商推荐的金股。"""
        return self.db.get_consecutive_monthly_stocks(month)

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
        self, ts_code: str, start_date: str, end_date: str, skip_tushare: bool = False
    ) -> Dict[str, float]:
        """获取指定股票在日期范围内的收盘价。DB 无数据或不完整时从 Tushare 拉取补全。"""
        try:
            code = ts_code.split(".")[0] if "." in ts_code else ts_code
            s_date = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
            e_date = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))

            # 当月 DB 无最新数据时自动向前扩展查询范围（无需 Tushare）
            if skip_tushare:
                from datetime import timedelta
                s_date = s_date - timedelta(days=30)

            records = self.db.get_data_range(code, s_date, e_date)
            if records:
                prices = {}
                for r in records:
                    d = r.date.strftime("%Y%m%d") if isinstance(r.date, date) else str(r.date)[:8]
                    if r.close:
                        prices[d] = float(r.close)
                if not skip_tushare:
                    last_db_date = max(prices.keys()) if prices else ""
                    if last_db_date < end_date:
                        tf_prices = self._fetch_tushare_prices(ts_code, code, start_date, end_date)
                        prices.update(tf_prices)
                return prices

            if skip_tushare:
                return {}
            return self._fetch_tushare_prices(ts_code, code, start_date, end_date)
        except Exception:
            pass
        return {}

    def _get_stock_ohlc(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """获取单只股票的 OHLC 数据，返回 {date: {open, high, low, close}}。"""
        try:
            code = ts_code.split(".")[0] if "." in ts_code else ts_code
            s_date = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
            e_date = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
            records = self.db.get_data_range(code, s_date, e_date)
            if records:
                result: Dict[str, Dict[str, Optional[float]]] = {}
                for r in records:
                    d = r.date.strftime("%Y%m%d") if isinstance(r.date, date) else str(r.date)[:8]
                    result[d] = {
                        "open": float(r.open) if r.open else None,
                        "high": float(r.high) if r.high else None,
                        "low": float(r.low) if r.low else None,
                        "close": float(r.close) if r.close else None,
                    }
                return result
        except Exception:
            pass
        return {}

    def _prefetch_ohlc(
        self, ts_codes: List[str], start_date: str, end_date: str, max_workers: int = 20
    ) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        """并行预取多只股票的 OHLC 数据。"""
        ohlc: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
        if not ts_codes:
            return ohlc
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._get_stock_ohlc, tc, start_date, end_date): tc for tc in ts_codes}
            for f in as_completed(futures, timeout=120):
                tc = futures[f]
                try:
                    ohlc[tc] = f.result(timeout=15)
                except Exception:
                    ohlc[tc] = {}
        return ohlc

    def _prefetch_prices(
        self, ts_codes: List[str], start_date: str, end_date: str, max_workers: int = 20, skip_tushare: bool = False
    ) -> Dict[str, Dict[str, float]]:
        """并行预取多只股票的价格数据，减少串行 Tushare 调用延迟。"""
        prices: Dict[str, Dict[str, float]] = {}
        if not ts_codes:
            return prices
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._get_stock_prices, tc, start_date, end_date, skip_tushare): tc for tc in ts_codes}
            for f in as_completed(futures, timeout=120):
                tc = futures[f]
                try:
                    prices[tc] = f.result(timeout=15)
                except Exception:
                    prices[tc] = {}
        return prices

    def _get_realtime_prices_batch(self, ts_codes: List[str]) -> tuple:
        """批量获取当日实时最新价（Sina 接口，支持逗号分隔批量查询）。

        仅在当月回测使用，作为 DB 数据的补充。批量查询避免逐个调用。
        返回 (prices_dict, daily_changes_dict)。
        """
        from datetime import date as dt_date
        import requests as _requests

        today = dt_date.today().strftime("%Y%m%d")
        prices: Dict[str, Dict[str, float]] = {}
        daily_changes: Dict[str, float] = {}
        symbols: List[str] = []
        sym_to_ts: Dict[str, str] = {}
        for ts in ts_codes:
            parts = ts.split(".") if "." in ts else [ts, ""]
            base = parts[0]
            exchange = parts[1].upper() if len(parts) > 1 else ""
            if exchange == "BJ":
                sym = f"bj{base}"
            elif base.startswith(("6", "5", "90")):
                sym = f"sh{base}"
            else:
                sym = f"sz{base}"
            symbols.append(sym)
            sym_to_ts[sym] = ts

        # Sina 单次最多约 50 个标的，分批
        for i in range(0, len(symbols), 50):
            batch = symbols[i:i + 50]
            url = f"http://hq.sinajs.cn/list={','.join(batch)}"
            try:
                resp = _requests.get(
                    url,
                    headers={"Referer": "https://finance.sina.com.cn"},
                    timeout=8,
                )
                resp.encoding = "gbk"
                for line in resp.text.strip().split("\n"):
                    if '="' not in line:
                        continue
                    parts = line.split('="', 1)
                    if len(parts) != 2:
                        continue
                    label = parts[0].strip()
                    sym = label.split("_")[-1] if "_" in label else label
                    data = parts[1].rstrip('";\n')
                    fields = data.split(",")
                    # fields[2]=昨收, fields[3]=最新价
                    if len(fields) > 3 and fields[3] and fields[2]:
                        try:
                            ts_code = sym_to_ts.get(sym)
                            if ts_code:
                                price = float(fields[3])
                                prev_close = float(fields[2])
                                prices.setdefault(ts_code, {})[today] = price
                                if prev_close > 0:
                                    daily_changes[ts_code] = round((price - prev_close) / prev_close, 4)
                        except (ValueError, TypeError):
                            continue
            except Exception:
                continue
        return prices, daily_changes

    def compute_backtest(self, month: str, top_n_per_broker: int = 10) -> Dict[str, Any]:
        """对指定月份金股池按券商分组做回测。

        回测逻辑：当月第一个交易日开盘买入 → 有效截止日收盘卖出。
        （历史月取月末最后交易日，当月取今天，避免拉 Tushare 补全月末缺失数据）
        按券商分组，每组内等权分配资金。
        结果持久化到数据库，历史月份后续直接返回存储结果。

        Args:
            month: YYYYMM 格式月份
            top_n_per_broker: 每个券商最多取几只金股

        Returns:
            回测结果字典
        """
        # 当月回测截止日：收盘前取前一交易日，收盘后取今天
        effective_end = self._effective_month_end(month)
        is_current = (month == date.today().strftime("%Y%m"))

        # 历史月份优先从存储读取；当月跳过存储（卖价每日变化）
        stored = None
        if not is_current:
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
                    month_end = effective_end
                    trading_days = self._get_trading_days(month_start, month_end)
                    if len(trading_days) < 2:
                        trading_days = [stored.get("buy_date", month_start), stored.get("sell_date", month_end)]
                    buy_date = trading_days[0]
                    sell_date = trading_days[-1]
                    # 并行预取缺失股票价格
                    price_cache = self._prefetch_prices(list(missing), month_start, month_end, skip_tushare=is_current)
                    for ts in missing:
                        prices = price_cache.get(ts, {})
                        if not prices:
                            continue
                        available_dates = sorted(prices.keys())
                        buy_dates = [d for d in available_dates if d >= buy_date]
                        sell_dates = [d for d in available_dates if d <= sell_date]
                        if not buy_dates or not sell_dates:
                            continue
                        buy_price = prices[buy_dates[0]]
                        sell_price = prices[sell_dates[-1]]
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
                            "end_date": sell_dates[-1],
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
            # 补充 OHLC 数据用于蜡烛图
            stored_stocks = {sr["ts_code"]: sr for sr in stored.get("stock_returns", [])}
            if stored_stocks:
                ohlc_cache = self._prefetch_ohlc(list(stored_stocks.keys()), stored.get("buy_date", f"{month}01"), stored.get("sell_date", effective_end))
                ohlc_merged = 0
                for sr in stored["stock_returns"]:
                    ohlc = ohlc_cache.get(sr["ts_code"], {})
                    for dr in sr.get("daily_returns", []):
                        d = dr.get("date", "")
                        if d in ohlc:
                            dr["open"] = ohlc[d].get("open")
                            dr["high"] = ohlc[d].get("high")
                            dr["low"] = ohlc[d].get("low")
                            ohlc_merged += 1
                logger.info(f"[BrokerRecommend] 回测 {month} OHLC 合并 {ohlc_merged} 条 (ohlc_cache 覆盖 {len(ohlc_cache)} 只股票)")

            stored["next_month"] = self._next_month_str(month)
            logger.info(f"[BrokerRecommend] 回测 {month} 命中存储")
            return stored

        df = self.get_monthly_recommendations(month)
        if df is None or df.empty:
            return {"error": f"{month} 月无数据"}

        # 当月第一个交易日开盘买入 → 有效截止日收盘卖出
        next_month = self._next_month_str(month)
        year = int(month[:4])
        mon = int(month[4:6])

        month_start = f"{month}01"
        month_end = effective_end
        trading_days = self._get_trading_days(month_start, month_end)
        single_day = len(trading_days) < 2

        if not trading_days:
            return {"error": f"{month} 月暂无交易日"}

        buy_date = trading_days[0]
        sell_date = trading_days[-1]

        # 并行预取所有股票价格（DB 有则秒查，无则并发拉 Tushare）
        all_ts = df["ts_code"].unique().tolist()
        logger.info(f"[BrokerRecommend] 回测 {month} 预取 {len(all_ts)} 只股票价格...")
        price_cache = self._prefetch_prices(all_ts, month_start, month_end, skip_tushare=is_current)

        # 当月补充实时最新价（Sina 批量接口，2~3s）
        daily_changes: Dict[str, float] = {}
        if is_current:
            try:
                rt_prices, rt_changes = self._get_realtime_prices_batch(all_ts)
                if rt_prices:
                    for ts, p in rt_prices.items():
                        price_cache.setdefault(ts, {}).update(p)
                    logger.info(f"[BrokerRecommend] 回测 {month} 实时价补充 {len(rt_prices)} 只")
                    # 有实时数据时，把今天加入交易日列表
                    today_str = date.today().strftime("%Y%m%d")
                    if today_str not in trading_days:
                        trading_days.append(today_str)
                        trading_days.sort()
                        sell_date = trading_days[-1]
                daily_changes = rt_changes
            except Exception:
                pass

        # 按券商分组回测
        brokers_result: List[Dict[str, Any]] = []
        stock_results: Dict[str, Dict[str, Any]] = {}

        all_brokers = df["broker"].unique()

        for broker in all_brokers:
            broker_df = df[df["broker"] == broker].drop_duplicates("ts_code")
            stocks = broker_df

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
                # 买入价取首交易日或之后第一个有数据日（避免 skip_tushare 扩展导致的跨月取价）
                buy_dates = [d for d in available_dates if d >= buy_date]
                if not buy_dates:
                    continue
                buy_price = prices[buy_dates[0]]
                # 卖出价取截止日或之前最后一个有数据日
                sell_dates = [d for d in available_dates if d <= sell_date]
                if not sell_dates:
                    continue
                sell_price = prices[sell_dates[-1]]
                actual_buy_date = buy_dates[0]
                actual_sell_date = sell_dates[-1]

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
                        "end_date": actual_sell_date,
                        "daily_change": daily_changes.get(ts),
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

        # 并行预取 OHLC 数据用于蜡烛图展示
        if stock_returns_list:
            ohlc_cache = self._prefetch_ohlc(list(stock_results.keys()), month_start, month_end)
            for sr in stock_returns_list:
                ohlc = ohlc_cache.get(sr["ts_code"], {})
                for dr in sr["daily_returns"]:
                    d = dr.get("date", "")
                    if d in ohlc:
                        dr["open"] = ohlc[d].get("open")
                        dr["high"] = ohlc[d].get("high")
                        dr["low"] = ohlc[d].get("low")

        # 持久化存储（仅历史月份；当月不存，避免 sell_date 不完整）
        if not is_current:
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
        # 1. 筹码胜率（Tushare CYQ）
        try:
            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            cyq_df = tf.get_bulk_cyq_perf(trade_date) if tf.is_available() else None
            if cyq_df is not None and not cyq_df.empty:
                cyq_data = {}
                for ts_code in ts_codes:
                    if ts_code in cyq_df.index:
                        row = cyq_df.loc[ts_code]
                        cost_5 = float(row.get("cost_5pct", 0) or 0)
                        cost_95 = float(row.get("cost_95pct", 0) or 0)
                        weight_avg = float(row.get("weight_avg", 0) or 0)
                        winner_rate = float(row.get("winner_rate", 0) or 0) / 100.0
                        cyq_data[ts_code] = {
                            "cost_avg": round(weight_avg, 2),
                            "winner_rate": round(winner_rate, 4),
                            "concentration": round((cost_95 - cost_5) / weight_avg, 4) if weight_avg > 0 else None,
                        }
                if cyq_data:
                    for ts_code, data in cyq_data.items():
                        stock_results[ts_code]["cyq_perf"] = {
                            "cost_avg": data["cost_avg"],
                            "winner_rate": data["winner_rate"],
                            "concentration": data["concentration"],
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
