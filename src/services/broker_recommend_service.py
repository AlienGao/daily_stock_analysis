# -*- coding: utf-8 -*-
"""券商月度金股推荐服务。

提供券商金股数据的获取、存储和回测功能。
"""

import calendar
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from threading import Lock, Thread
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from src.storage import DatabaseManager, StockDaily

logger = logging.getLogger(__name__)


class BrokerRecommendService:
    """券商金股推荐服务。"""

    # 按单只股票缓存增强数据，不同数据类型有独立 TTL
    _enrichment_cache: Dict[str, Any] = {}
    _enrichment_cache_ts: Dict[str, float] = {}
    _historical_stats_cache: Dict[tuple, Dict[str, Any]] = {}
    _historical_stats_cache_ts: Dict[tuple, float] = {}
    _query_date_cache: Dict[str, str] = {}
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
    def _is_empty_nineturn(nt: Optional[Dict[str, Any]]) -> bool:
        """九转四项计数均为 0（含空对象）。"""
        if not nt:
            return True
        return not any(
            nt.get(k)
            for k in ("up_count", "down_count", "nine_up_turn", "nine_down_turn")
        )

    @staticmethod
    def _is_current_month(month: str) -> bool:
        from datetime import datetime
        return month == datetime.now().strftime("%Y%m")

    @classmethod
    def _nineturn_cache_miss(cls, nt: Optional[Dict[str, Any]], month: str) -> bool:
        """当前月份的空九转视为缓存未命中，避免 Tushare 晚间更新前写入的全 0 永久占位。"""
        return cls._is_current_month(month) and cls._is_empty_nineturn(nt)

    @staticmethod
    def _normalize_cyq_perf(data: Dict[str, Any]) -> Dict[str, Any]:
        """从 L2 原始字段补全 computed 字段（cost_avg、concentration、scr90）。

        L2 存储的是 Tushare 原始字段，前端需要 cost_avg/concentration/scr90。
        winner_rate 可能是百分比格式（>1，如 88.0）或小数格式（0-1，如 0.88）；
        统一转换为小数格式，前端统一乘以 100 后显示为 %。
        """
        # winner_rate 单位兼容：小数格式（0-1）保持不变，百分比格式（>1）自动转换
        wr = data.get("winner_rate")
        if wr is not None:
            try:
                wr_float = float(wr)
                # 超过 1 视为百分比格式（如 88.0），转换为小数（如 0.88）
                if wr_float > 1.0:
                    data["winner_rate"] = round(wr_float / 100.0, 4)
            except (ValueError, TypeError):
                pass

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

    def _attach_sectors(self, enrichment: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """附加同花顺行业信息并返回 enrichment。"""
        try:
            industry_map = self.db.get_ths_industry_map()
            if industry_map:
                for tc in enrichment:
                    code = tc.split(".")[0] if "." in tc else tc
                    sector = industry_map.get(code) or industry_map.get(code.zfill(6))
                    if sector:
                        enrichment[tc]["sector"] = sector
        except Exception:
            pass
        return enrichment

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
            if nt is not None and not self._nineturn_cache_miss(nt, month):
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
            return self._attach_sectors(enrichment)

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
                        if self._nineturn_cache_miss(data["nineturn"], month):
                            continue
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
            return self._attach_sectors(enrichment)

        logger.info(f"[BrokerRecommend] enrichment {month}: L1+L2 命中 {l2_hits}/{total_fields}, "
                    f"待 fetch nineturn={len(still_need_nineturn)} forecast={len(still_need_forecast)} cyq={len(still_need_cyq)}")

        # L3: Tushare API 批量获取
        from data_provider.tushare_fetcher import TushareFetcher
        tf = TushareFetcher.get_instance()
        if not tf.is_available():
            logger.warning("[BrokerRecommend] Tushare 不可用，仅返回缓存数据")
            return self._attach_sectors(enrichment)

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
                                # 统一 winner_rate 单位：百分比格式（>1）转为小数格式
                                if cyq.get("winner_rate", 0) > 1.0:
                                    cyq["winner_rate"] = round(cyq["winner_rate"] / 100.0, 4)
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
                enrichment.setdefault(tc, {})["nineturn"] = empty
                if not self._is_current_month(month):
                    BrokerRecommendService._set_cached(tc, query_date, "nineturn", empty)
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
        return self._attach_sectors(enrichment)

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
                        winner_rate = float(row.get("获利比例", 0) or 0) / 100.0
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
                    # winner_rate 可能是百分比格式（>1）或小数格式（<1）；统一去掉 /100.0，让前端乘以 100 显示
                    winner_rate = float(row.get("winner_rate", 0) or 0)
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
                    # DB first
                    df_basic = None
                    try:
                        db = DatabaseManager.get_instance()
                        df_basic = db.get_daily_basic(trade_date)
                        if not df_basic.empty and "total_mv" in df_basic.columns:
                            df_basic = df_basic.reset_index().rename(columns={"code": "ts_code_raw"})
                            codes = df_basic["ts_code_raw"].astype(str).str.zfill(6)
                            pre2 = codes.str[:2]
                            suffix_map = {
                                "60": ".SH", "68": ".SH", "00": ".SZ", "30": ".SZ",
                                "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
                            }
                            df_basic["ts_code"] = codes + pre2.map(suffix_map).fillna("")
                            df_basic = df_basic.set_index("ts_code").drop(columns=["ts_code_raw"], errors="ignore")
                    except Exception:
                        pass
                    # Fallback to Tushare API
                    if df_basic is None or df_basic.empty:
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

    def _next_trading_day_after(self, after_date: str, horizon_days: int = 15) -> Optional[str]:
        """返回 after_date 之后的第一个交易日（可跨自然月）。"""
        days = self._trading_days_after(after_date, max_count=1, horizon_days=horizon_days)
        return days[0] if days else None

    def _trading_days_after(
        self,
        after_date: str,
        max_count: Optional[int] = None,
        horizon_days: int = 45,
    ) -> List[str]:
        """返回 after_date 之后至多 max_count 个交易日（可跨自然月）。"""
        from datetime import datetime, timedelta

        if max_count is None:
            max_count = self._STRATEGY_MONTH_END_DEFER_MAX_DAYS
        try:
            end = (
                datetime.strptime(after_date, "%Y%m%d") + timedelta(days=horizon_days)
            ).strftime("%Y%m%d")
        except ValueError:
            return []
        days = self._get_trading_days(after_date, end)
        result: List[str] = []
        for d in days:
            if d > after_date:
                result.append(d)
                if len(result) >= max_count:
                    break
        return result

    @staticmethod
    def _has_close_on_day(
        close_map: Dict[str, float],
        day: str,
    ) -> bool:
        return close_map.get(day) is not None

    @staticmethod
    def _has_open_quote_on_day(
        open_map: Dict[str, float],
        close_map: Dict[str, float],
        day: str,
    ) -> bool:
        return (
            open_map.get(day) is not None
            or close_map.get(day) is not None
        )

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

    @staticmethod
    def _lookup_adj_factor(adj_map: Dict[str, float], date_str: str) -> float:
        """查找指定日期的复权因子，若无精确匹配则取最近一个 ≤ date_str 的值。"""
        if date_str in adj_map:
            f = adj_map[date_str]
            return f if f > 0 else 1.0
        prev_dates = sorted(d for d in adj_map if d <= date_str and adj_map.get(d, 0) > 0)
        if prev_dates:
            return adj_map[prev_dates[-1]]
        return 1.0


    @staticmethod
    def _has_exact_adj_factor(adj_map: Dict[str, float], date_str: str) -> bool:
        """当日是否有精确复权因子记录（非向前查找）。"""
        if not adj_map or not date_str:
            return False
        f = adj_map.get(date_str)
        return f is not None and f > 0

    def _resolve_sell_date_with_adj(
        self,
        ts_code: str,
        trading_days: List[str],
        sell_date: str,
        adj_map: Optional[Dict[str, float]] = None,
    ) -> str:
        """截止日无当日复权因子时，回退至上一交易日。"""
        if not trading_days:
            return sell_date
        if adj_map is None:
            code = ts_code.split(".")[0] if "." in ts_code else ts_code
            adj_map = self._load_all_adj_factors([ts_code]).get(code, {}) or {}
        candidates = [d for d in trading_days if d <= sell_date]
        if not candidates:
            return sell_date
        for d in reversed(candidates):
            if self._has_exact_adj_factor(adj_map, d):
                return d
        return candidates[0]

    @staticmethod
    def _norm_trade_date(date_str: str) -> str:
        """统一为 YYYYMMDD。"""
        s = str(date_str or "").strip()
        if len(s) >= 10 and s[4] == "-":
            return f"{s[:4]}{s[5:7]}{s[8:10]}"
        return s[:8]

    def _period_return_from_daily_returns(
        self,
        ts_code: str,
        daily_returns: List[Dict[str, Any]],
        adj_all: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> Optional[float]:
        """从持仓期日序列推算期末收益（后复权收盘价，不预取 OHLC，供批量历史统计）。"""
        bars: List[tuple] = []
        for d in daily_returns or []:
            dt = self._norm_trade_date(d.get("date", ""))
            if len(dt) != 8:
                continue
            price = d.get("price")
            if price is None:
                continue
            try:
                bars.append((dt, float(price)))
            except (TypeError, ValueError):
                continue
        if len(bars) < 2:
            return _holding_final_return(daily_returns)

        bars.sort(key=lambda x: x[0])
        code = ts_code.split(".")[0] if "." in ts_code else ts_code
        if adj_all is None:
            adj_all = self._load_all_adj_factors([ts_code])
        adj_map = adj_all.get(code, {})

        adj_closes: List[float] = []
        for dt, p in bars:
            f = self._lookup_adj_factor(adj_map, dt) if adj_map else 1.0
            adj_closes.append(p * f)

        buy_adj = adj_closes[0]
        if buy_adj <= 0:
            return _holding_final_return(daily_returns)
        return round((adj_closes[-1] - buy_adj) / buy_adj, 4)

    def _get_stock_prices(
        self, ts_code: str, start_date: str, end_date: str, skip_tushare: bool = False, adj_all: dict | None = None
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

                # 后复权：adjusted = close × adj_factor
                adj_map = (adj_all or {}).get(code, {})
                if adj_map and prices:
                    for d in list(prices.keys()):
                        f = self._lookup_adj_factor(adj_map, d)
                        if f > 0:
                            prices[d] = round(prices[d] * f, 4)

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

    @staticmethod
    def _load_all_adj_factors(ts_codes: list) -> Dict[str, Dict[str, float]]:
        """批量预加载所有股票的复权因子，避免多线程 SQLite 并发锁。
        返回 {ts_code_bare: {YYYYMMDD: factor}}。
        """
        result: Dict[str, Dict[str, float]] = {}
        if not ts_codes:
            return result
        try:
            from src.storage import DatabaseManager, StockAdjFactor
            db = DatabaseManager.get_instance()
            codes_bare = [c.split(".")[0] if "." in c else c for c in ts_codes]
            with db.get_session() as session:
                rows = session.query(StockAdjFactor).filter(
                    StockAdjFactor.code.in_(codes_bare),
                ).order_by(StockAdjFactor.trade_date.asc()).all()
            for r in rows:
                if not r.adj_factor or r.adj_factor <= 0:
                    continue
                code = str(r.code).strip().zfill(6)
                d = r.trade_date.strftime("%Y%m%d") if hasattr(r.trade_date, "strftime") else str(r.trade_date)[:8]
                result.setdefault(code, {})[d] = float(r.adj_factor)
        except Exception:
            pass
        return result

    def _get_stock_ohlc(
        self, ts_code: str, start_date: str, end_date: str, adj_all: dict | None = None
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """获取单只股票的 OHLC 数据，返回 {date: {open, high, low, close}}。"""
        try:
            code = ts_code.split(".")[0] if "." in ts_code else ts_code
            s_date = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
            e_date = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
            records = self.db.get_data_range(code, s_date, e_date)

            def _build_ohlc(recs) -> Dict[str, Dict[str, Optional[float]]]:
                r: Dict[str, Dict[str, Optional[float]]] = {}
                for rec in recs:
                    d = rec.date.strftime("%Y%m%d") if isinstance(rec.date, date) else str(rec.date)[:8]
                    r[d] = {
                        "open": float(rec.open) if rec.open else None,
                        "high": float(rec.high) if rec.high else None,
                        "low": float(rec.low) if rec.low else None,
                        "close": float(rec.close) if rec.close else None,
                    }
                # 后复权：每个 OHLC 字段 × adj_factor
                adj_map = (adj_all or {}).get(code, {})
                if adj_map and r:
                    for d in list(r.keys()):
                        f = self._lookup_adj_factor(adj_map, d)
                        if f <= 0:
                            continue
                        entry = r[d]
                        for k in ("open", "high", "low", "close"):
                            if entry[k] is not None:
                                entry[k] = round(entry[k] * f, 4)
                return r

            if records:
                return _build_ohlc(records)

            # DB 无数据，尝试从 Tushare 拉取并入库后重新查询
            try:
                self._fetch_tushare_prices(ts_code, code, start_date, end_date)
                records = self.db.get_data_range(code, s_date, e_date)
                if records:
                    return _build_ohlc(records)
            except Exception:
                pass
        except Exception:
            pass
        return {}

    def _prefetch_ohlc(
        self, ts_codes: List[str], start_date: str, end_date: str, max_workers: int = 20, use_adj: bool = False
    ) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        """并行预取多只股票的 OHLC 数据。"""
        ohlc: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
        if not ts_codes:
            return ohlc
        adj_all = self._load_all_adj_factors(ts_codes) if use_adj else {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._get_stock_ohlc, tc, start_date, end_date, adj_all): tc for tc in ts_codes}
            for f in as_completed(futures, timeout=120):
                tc = futures[f]
                try:
                    ohlc[tc] = f.result(timeout=15)
                except Exception:
                    ohlc[tc] = {}
        return ohlc

    def _prefetch_prices(
        self, ts_codes: List[str], start_date: str, end_date: str, max_workers: int = 20, skip_tushare: bool = False, use_adj: bool = False
    ) -> Dict[str, Dict[str, float]]:
        """并行预取多只股票的价格数据，减少串行 Tushare 调用延迟。"""
        prices: Dict[str, Dict[str, float]] = {}
        if not ts_codes:
            return prices
        adj_all = self._load_all_adj_factors(ts_codes) if use_adj else {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._get_stock_prices, tc, start_date, end_date, skip_tushare, adj_all): tc for tc in ts_codes}
            for f in as_completed(futures, timeout=120):
                tc = futures[f]
                try:
                    prices[tc] = f.result(timeout=15)
                except Exception:
                    prices[tc] = {}
        return prices

    def _get_realtime_prices_batch(self, ts_codes: List[str]) -> tuple:
        """批量获取当日实时最新价（从 realtime_spot DB 读取）。

        仅在当月回测使用，作为 DB 数据的补充。
        返回 (prices_dict, daily_changes_dict, today_ohlc_dict, daily_change_dates_dict)。
        """
        from datetime import date as dt_date

        today = dt_date.today().strftime("%Y%m%d")
        today_iso = dt_date.today().isoformat()  # YYYY-MM-DD，匹配 DB 中 trade_date 的格式
        prices: Dict[str, Dict[str, float]] = {}
        daily_changes: Dict[str, float] = {}
        today_ohlc: Dict[str, Dict[str, float]] = {}
        daily_change_dates: Dict[str, str] = {}

        try:
            from src.storage import DatabaseManager
            bare_codes = [ts.split(".")[0] if "." in ts else ts for ts in ts_codes]
            spot_df = DatabaseManager().get_current_prices(bare_codes)
            if spot_df is None or spot_df.empty:
                return prices, daily_changes, today_ohlc, daily_change_dates

            for ts in ts_codes:
                code = ts.split(".")[0] if "." in ts else ts
                try:
                    row = spot_df.loc[code]
                    price = float(row["price"])
                    prices[ts] = {today: price}
                    trade_date = row.get("trade_date")
                    trade_date_str = str(trade_date) if pd.notna(trade_date) else ""
                    # 仅当天快照的涨跌幅有效，过期快照（如跨周末）的 pct_chg 不可用
                    if trade_date_str == today_iso:
                        pct = row.get("pct_chg")
                        if pd.notna(pct):
                            daily_changes[ts] = round(float(pct) / 100, 4)
                        daily_change_dates[ts] = today  # YYYYMMDD，兼容前端 fmtDate
                    ohlc = {}
                    if pd.notna(row.get("open_price")):
                        ohlc["open"] = float(row["open_price"])
                    if pd.notna(row.get("high")):
                        ohlc["high"] = float(row["high"])
                    if pd.notna(row.get("low")):
                        ohlc["low"] = float(row["low"])
                    ohlc["close"] = price
                    if len(ohlc) >= 3:
                        today_ohlc[ts] = ohlc
                except (KeyError, ValueError, TypeError):
                    continue
        except Exception:
            logger.warning("[BrokerRecommend] realtime_spot 读取出错", exc_info=True)

        return prices, daily_changes, today_ohlc, daily_change_dates

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
                    price_cache = self._prefetch_prices(list(missing), month_start, month_end, skip_tushare=is_current, use_adj=True)
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
                        if buy_dates[0] == sell_dates[-1]:
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
                ohlc_merged = 0
                for sr in stored["stock_returns"]:
                    before = len([d for d in sr.get("daily_returns", []) if d.get("open")])
                    sr["daily_returns"] = self._sync_daily_returns_from_ohlc(
                        sr["ts_code"],
                        sr.get("daily_returns", []),
                        stored.get("buy_date", f"{month}01"),
                        stored.get("sell_date", effective_end),
                    )
                    ohlc_merged += len([d for d in sr.get("daily_returns", []) if d.get("open")]) - before
                logger.info(
                    f"[BrokerRecommend] 回测 {month} OHLC 同步 {ohlc_merged} 条 "
                    f"(覆盖 {len(stored_stocks)} 只股票)"
                )

            # 为存储数据补充当月累计收益（与 daily_returns 后复权累计一致）
            for sr in stored["stock_returns"]:
                sr["month_cumulative_return"] = self._month_cumulative_return_from_stock(sr)
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
        price_cache = self._prefetch_prices(all_ts, month_start, month_end, skip_tushare=is_current, use_adj=True)

        # 当月补充实时最新价（Sina 批量接口，2~3s）
        daily_changes: Dict[str, float] = {}
        if is_current:
            try:
                rt_prices, rt_changes, rt_ohlc, rt_change_dates = self._get_realtime_prices_batch(all_ts)
                if rt_prices:
                    # 后复权：实时价也需要 × adj_factor 才能和 price_cache 对齐
                    adj_all = self._load_all_adj_factors(all_ts)
                    today_str = date.today().strftime("%Y%m%d")
                    rt_merged = 0
                    for ts, p in rt_prices.items():
                        code = ts.split(".")[0] if "." in ts else ts
                        adj_map = adj_all.get(code, {}) or {}
                        if not self._has_exact_adj_factor(adj_map, today_str):
                            continue
                        f = adj_map[today_str]
                        p = {d: round(v * f, 4) for d, v in p.items()}
                        price_cache.setdefault(ts, {}).update(p)
                        rt_merged += 1
                    logger.info(f"[BrokerRecommend] 回测 {month} 实时价补充 {rt_merged} 只")
                    # 有实时数据且当日复权因子已入库时，把今天加入交易日列表
                    if rt_merged and today_str not in trading_days:
                        trading_days.append(today_str)
                        trading_days.sort()
                        sell_date = trading_days[-1]
                    # 将实时今日 OHLC 写入 DB（仅交易日）
                    from src.discovery.engine import is_trading_day
                    if rt_ohlc and is_trading_day():
                        ohlc_saved = 0
                        today_date = date.today()
                        for ts, ohlc in rt_ohlc.items():
                            code = ts.split(".")[0] if "." in ts else ts
                            try:
                                row = {
                                    "date": today_date,
                                    "open": ohlc.get("open"),
                                    "high": ohlc.get("high"),
                                    "low": ohlc.get("low"),
                                    "close": ohlc.get("close"),
                                }
                                df_ohlc = pd.DataFrame([row])
                                self.db.save_daily_data(df_ohlc, code, "Sina")
                                ohlc_saved += 1
                            except Exception:
                                pass
                        if ohlc_saved:
                            logger.info(f"[BrokerRecommend] 回测 {month} 今日 OHLC 写入 {ohlc_saved} 只")
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

                # 确保个股信息至少出现在结果列表中（显示最新价、日涨幅）
                if ts not in stock_results:
                    stock_results[ts] = {
                        "ts_code": ts,
                        "name": name,
                        "broker_count": broker_count,
                        "broker": broker,
                        "end_price": round(sell_price, 2),
                        "end_date": actual_sell_date,
                        "daily_change": daily_changes.get(ts),
                        "daily_change_date": rt_change_dates.get(ts),
                        "month_cumulative_return": None,
                        "daily_returns": [],
                    }

                if actual_buy_date == actual_sell_date:
                    continue  # 仅有一天价格数据，不参与回测统计，daily_returns 保持空

                # 初始化该股票的 daily_returns 占位（仅在有足够历史数据时）
                if not stock_results[ts]["daily_returns"]:
                    for td in trading_days:
                        stock_results[ts]["daily_returns"].append({
                            "date": td,
                            "return": None,
                            "cumulative": None,
                        })

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
            ohlc_cache = self._prefetch_ohlc(list(stock_results.keys()), month_start, month_end, use_adj=False)
            for sr in stock_returns_list:
                ohlc = ohlc_cache.get(sr["ts_code"], {})
                sr["daily_returns"] = self._sync_daily_returns_from_ohlc(
                    sr["ts_code"], sr["daily_returns"], buy_date, sell_date,
                )
                sr["month_cumulative_return"] = self._month_cumulative_return_from_stock(sr)

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
                    "month_cumulative_return": sr.get("month_cumulative_return"),
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




    def _month_cumulative_return_from_stock(self, sr: Dict[str, Any]) -> Optional[float]:
        """当月累计收益：与 daily_returns 累计口径一致（后复权收盘价）。"""
        drs = sr.get("daily_returns") or []
        if not drs:
            return None
        cum_vals = [d.get("cumulative") for d in drs if d.get("cumulative") is not None]
        if cum_vals:
            return round(float(cum_vals[-1]), 4)
        return self._period_return_from_daily_returns(str(sr.get("ts_code", "")), drs)


    def _cumulative_return_from_price_window(
        self,
        ts_code: str,
        price_cache: Dict[str, Dict[str, float]],
        trading_days: List[str],
        buy_date: str,
        sell_date: str,
    ) -> tuple[Optional[float], str]:
        """从预取价格缓存计算持仓窗口累计收益（后复权，经 OHLC 同步）。

        Returns:
            (cumulative_return, effective_sell_date)
        """
        effective_sell = self._resolve_sell_date_with_adj(ts_code, trading_days, sell_date)
        window_days = [d for d in trading_days if d <= effective_sell]
        prices = price_cache.get(ts_code, {})
        if not prices or not window_days:
            return None, effective_sell
        available = sorted(prices.keys())
        buy_dates = [d for d in available if d >= buy_date]
        sell_dates = [d for d in available if d <= effective_sell]
        if not buy_dates or not sell_dates:
            return None, effective_sell
        buy_price = prices[buy_dates[0]]
        if not buy_price or buy_price <= 0:
            return None, effective_sell
        if buy_dates[0] == sell_dates[-1] and len(window_days) < 2:
            return 0.0, effective_sell

        daily_rets: List[Dict[str, Any]] = []
        prev_p: Optional[float] = None
        for td in window_days:
            p = prices.get(td)
            if p and buy_price > 0:
                cumulative = (p - buy_price) / buy_price
                d_ret = (p - prev_p) / prev_p if prev_p and prev_p > 0 else 0.0
                daily_rets.append({
                    "date": td,
                    "price": round(p, 2),
                    "return": round(d_ret, 4),
                    "cumulative": round(cumulative, 4),
                })
                prev_p = p
        if not daily_rets:
            return None, effective_sell
        daily_rets = self._sync_daily_returns_from_ohlc(
            ts_code, daily_rets, buy_date, effective_sell,
        )
        cum = self._month_cumulative_return_from_stock({"ts_code": ts_code, "daily_returns": daily_rets})
        return cum, effective_sell

    def get_current_month_stock_returns(self, ts_codes: List[str]) -> Dict[str, Any]:
        """批量计算股票在当前自然月的累计收益（后复权，月初首日至有效截止日）。"""
        month = date.today().strftime("%Y%m")
        effective_end = self._effective_month_end(month)
        month_start = f"{month}01"
        trading_days = self._get_trading_days(month_start, effective_end)
        empty: Dict[str, Any] = {"month": month, "buy_date": "", "sell_date": "", "items": []}
        if not ts_codes:
            return empty
        if not trading_days:
            return empty

        buy_date = trading_days[0]
        sell_date = trading_days[-1]
        price_cache = self._prefetch_prices(
            ts_codes, month_start, effective_end, skip_tushare=False, use_adj=True,
        )
        try:
            rt_prices, _, _, _ = self._get_realtime_prices_batch(ts_codes)
            if rt_prices:
                adj_all = self._load_all_adj_factors(ts_codes)
                today_str = date.today().strftime("%Y%m%d")
                for ts, p in rt_prices.items():
                    code = ts.split(".")[0] if "." in ts else ts
                    adj_map = adj_all.get(code, {}) or {}
                    if not self._has_exact_adj_factor(adj_map, today_str):
                        continue
                    f = adj_map[today_str]
                    p = {d: round(v * f, 4) for d, v in p.items()}
                    price_cache.setdefault(ts, {}).update(p)
                if any(
                    self._has_exact_adj_factor(
                        adj_all.get(ts.split(".")[0] if "." in ts else ts, {}) or {},
                        today_str,
                    )
                    for ts in rt_prices
                ) and today_str not in trading_days:
                    trading_days = sorted(trading_days + [today_str])
                    sell_date = trading_days[-1]
        except Exception:
            logger.warning("[BrokerRecommend] 当前月收益实时价补充失败", exc_info=True)

        items: List[Dict[str, Any]] = []
        response_sell = buy_date
        for ts in ts_codes:
            cum, effective_sell = self._cumulative_return_from_price_window(
                ts, price_cache, trading_days, buy_date, sell_date,
            )
            items.append({
                "ts_code": ts,
                "cumulative_return": cum,
                "end_date": effective_sell,
            })
            if effective_sell and effective_sell > response_sell:
                response_sell = effective_sell

        return {
            "month": month,
            "buy_date": buy_date,
            "sell_date": response_sell if items else sell_date,
            "items": items,
        }



    def _resolve_broker_stock_names(
        self,
        ts_codes: List[str],
        hints: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """补全金股展示名称：DB 快照 → 当月推荐 → 股票索引。"""
        names: Dict[str, str] = {}
        for ts in ts_codes:
            hint = (hints or {}).get(ts, "")
            names[ts] = str(hint or "").strip()
        missing = [ts for ts in ts_codes if not names.get(ts)]
        if missing:
            try:
                from src.data.stock_index_loader import get_index_stock_name
                for ts in missing:
                    n = get_index_stock_name(ts)
                    if n:
                        names[ts] = n
            except Exception:
                pass
        return names

    def get_prev_month_current_returns_top(self, top_n: int = 5) -> Dict[str, Any]:
        """上月推荐金股在当前自然月的累计收益 Top N（后复权，每日截止有效交易日）。"""
        current_month = date.today().strftime("%Y%m")
        prev_month = self._prev_month_str(current_month)
        empty: Dict[str, Any] = {
            "prev_month": prev_month,
            "current_month": current_month,
            "buy_date": "",
            "sell_date": "",
            "items": [],
        }
        df = self.get_monthly_recommendations(prev_month)
        if df is None or df.empty:
            return empty

        name_by_code: Dict[str, str] = {}
        broker_count_by_code: Dict[str, int] = {}
        for _, row in df.iterrows():
            ts = str(row.get("ts_code", "")).strip()
            if not ts:
                continue
            bc = int(row.get("broker_count") or 1)
            if ts not in broker_count_by_code or bc > broker_count_by_code[ts]:
                broker_count_by_code[ts] = bc
                name_by_code[ts] = str(row.get("name") or "").strip()

        ts_codes = list(name_by_code.keys())
        if not ts_codes:
            return empty

        current_codes: set = set()
        current_name_hints: Dict[str, str] = {}
        current_df = self.get_monthly_recommendations(current_month)
        if current_df is not None and not current_df.empty:
            for _, row in current_df.iterrows():
                ts = str(row.get("ts_code", "")).strip()
                if not ts:
                    continue
                current_codes.add(ts)
                nm = str(row.get("name") or "").strip()
                if nm and ts not in current_name_hints:
                    current_name_hints[ts] = nm

        name_hints = dict(name_by_code)
        name_hints.update(current_name_hints)
        resolved_names = self._resolve_broker_stock_names(ts_codes, name_hints)

        result = self.get_current_month_stock_returns(ts_codes)
        items: List[Dict[str, Any]] = []
        for item in result.get("items", []):
            ts = str(item.get("ts_code", ""))
            if not ts:
                continue
            items.append({
                "ts_code": ts,
                "name": resolved_names.get(ts, ""),
                "broker_count": broker_count_by_code.get(ts, 1),
                "cumulative_return": item.get("cumulative_return"),
                "end_date": item.get("end_date"),
                "is_current_month_recommend": ts in current_codes,
            })

        items.sort(
            key=lambda x: (
                x.get("cumulative_return") is None,
                -(float(x["cumulative_return"]) if x.get("cumulative_return") is not None else 0.0),
            ),
        )
        return {
            "prev_month": prev_month,
            "current_month": result.get("month", current_month),
            "buy_date": result.get("buy_date", ""),
            "sell_date": result.get("sell_date", ""),
            "items": items[: max(1, int(top_n))],
        }

    def _sync_daily_returns_from_ohlc(
        self,
        ts_code: str,
        daily_returns: List[Dict[str, Any]],
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """用不复权 OHLC 补 K 线字段；用后复权收盘价重算日收益/累计收益（避免除权月假回撤）。"""
        if not daily_returns:
            return daily_returns
        ohlc = self._prefetch_ohlc([ts_code], start_date, end_date, use_adj=False).get(ts_code, {})
        if not ohlc:
            return daily_returns

        code = ts_code.split(".")[0] if "." in ts_code else ts_code
        adj_map = self._load_all_adj_factors([ts_code]).get(code, {}) or {}
        dr_dates = sorted(
            str(d.get("date", ""))[:8]
            for d in daily_returns
            if d.get("date")
        )
        if dr_dates:
            end_date = self._resolve_sell_date_with_adj(ts_code, dr_dates, end_date, adj_map)

        bars: List[Dict[str, Any]] = []
        adj_closes: List[float] = []
        for dr in sorted(daily_returns, key=lambda x: str(x.get("date", ""))):
            d = str(dr.get("date", ""))[:8]
            if not d or d > end_date or d not in ohlc:
                continue
            bar = ohlc[d]
            close = bar.get("close")
            if close is None:
                close = dr.get("price")
            if close is None:
                continue
            close_f = float(close)
            f = self._lookup_adj_factor(adj_map, d) if adj_map else 1.0
            adj_close = round(close_f * f, 4)
            bars.append({
                "date": d,
                "price": round(close_f, 4),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
            })
            adj_closes.append(adj_close)

        if not bars:
            return daily_returns

        buy_adj = adj_closes[0]
        prev_adj: Optional[float] = None
        for b, adj_p in zip(bars, adj_closes):
            if buy_adj > 0:
                b["cumulative"] = round((adj_p - buy_adj) / buy_adj, 4)
            else:
                b["cumulative"] = 0.0
            if prev_adj and prev_adj > 0:
                b["return"] = round((adj_p - prev_adj) / prev_adj, 4)
            else:
                b["return"] = 0.0
            prev_adj = adj_p
        return bars


    def _build_single_stock_window_returns(
        self,
        ts_code: str,
        month: str,
        buy_date: Optional[str] = None,
        sell_date: Optional[str] = None,
    ) -> tuple:
        """计算单只股票在指定推荐月的持仓期日收益与 OHLC。"""
        effective_end = self._effective_month_end(month)
        is_current = month == date.today().strftime("%Y%m")
        month_start = f"{month}01"
        month_end = effective_end

        if not buy_date or not sell_date:
            trading_days = self._get_trading_days(month_start, month_end)
            if len(trading_days) < 2:
                return [], buy_date or month_start, sell_date or month_end, None
            buy_date = trading_days[0]
            sell_date = trading_days[-1]
        else:
            trading_days = self._get_trading_days(buy_date, sell_date)
            if len(trading_days) < 2:
                return [], buy_date, sell_date, None

        prices = self._prefetch_prices(
            [ts_code], month_start, month_end, skip_tushare=is_current, use_adj=True,
        ).get(ts_code, {})
        if not prices:
            return [], buy_date, sell_date, None

        available_dates = sorted(prices.keys())
        buy_dates = [d for d in available_dates if d >= buy_date]
        sell_dates = [d for d in available_dates if d <= sell_date]
        if not buy_dates or not sell_dates:
            return [], buy_date, sell_date, None
        buy_price = prices[buy_dates[0]]
        sell_price = prices[sell_dates[-1]]
        if not buy_price or not sell_price or buy_price <= 0:
            return [], buy_date, sell_date, None
        if buy_dates[0] == sell_dates[-1]:
            return [], buy_date, sell_date, None

        daily_rets: List[Dict[str, Any]] = []
        prev_p = None
        cum_ret = None
        for td in trading_days:
            p = prices.get(td)
            if p and buy_price > 0:
                cumulative = (p - buy_price) / buy_price
                if prev_p and prev_p > 0:
                    d_ret = (p - prev_p) / prev_p
                else:
                    d_ret = 0.0
                daily_rets.append({
                    "date": td,
                    "price": round(p, 2),
                    "return": round(d_ret, 4),
                    "cumulative": round(cumulative, 4),
                })
                prev_p = p
                cum_ret = cumulative

        daily_rets = self._sync_daily_returns_from_ohlc(ts_code, daily_rets, buy_date, sell_date)
        return daily_rets, buy_date, sell_date, cum_ret


    @staticmethod
    def _sanitize_daily_returns_bars(daily_returns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """过滤无收盘价占位行，按日期升序，供 K 线与累计收益展示。"""
        bars: List[Dict[str, Any]] = []
        for d in daily_returns or []:
            if not d.get("date"):
                continue
            if d.get("price") is None:
                continue
            bars.append(d)
        bars.sort(key=lambda x: str(x["date"]))
        return bars

    def _resolve_stock_holding_daily_returns(
        self,
        ts_code: str,
        month: str,
        stored: Optional[Dict[str, Any]] = None,
        stock_return: Optional[Dict[str, Any]] = None,
    ) -> tuple:
        """与展开历史 K 线一致：必要时重算、OHLC 同步、清洗后返回持仓期序列与期末收益。"""
        buy_date: Optional[str] = None
        sell_date: Optional[str] = None
        daily_returns: List[Dict[str, Any]] = []
        cum_ret = None

        if stored:
            buy_date = stored.get("buy_date")
            sell_date = stored.get("sell_date")
        if stock_return:
            daily_returns = list(stock_return.get("daily_returns") or [])
            with_cum = [d for d in daily_returns if d.get("cumulative") is not None]
            if with_cum:
                cum_ret = with_cum[-1].get("cumulative")

        need_compute = not daily_returns or not any(d.get("price") for d in daily_returns)
        if need_compute:
            daily_returns, buy_date, sell_date, cum_ret = self._build_single_stock_window_returns(
                ts_code, month, buy_date=buy_date, sell_date=sell_date,
            )
        sync_start = buy_date or f"{month}01"
        sync_end = sell_date or self._effective_month_end(month)
        if daily_returns:
            daily_returns = self._sync_daily_returns_from_ohlc(
                ts_code, daily_returns, sync_start, sync_end,
            )

        daily_returns = self._sanitize_daily_returns_bars(daily_returns)
        if daily_returns:
            cum_ret = self._period_return_from_daily_returns(ts_code, daily_returns)
            if cum_ret is None:
                cum_ret = _holding_final_return(daily_returns)
            buy_date = buy_date or daily_returns[0]["date"]
            sell_date = sell_date or daily_returns[-1]["date"]

        return daily_returns, cum_ret, buy_date, sell_date


    def get_historical_recommend_stats(self, ts_codes: List[str], exclude_after: str | None = None) -> Dict[str, Dict[str, Any]]:
        """统计各股票历次推荐持仓期胜率、最高/最低期末收益（与展开历史口径一致）。

        使用内部缓存减少重复计算：缓存 key 为 (frozenset(codes), exclude_after)。
        """
        if not ts_codes:
            return {}
        cache_key = (frozenset(sorted(ts_codes)), exclude_after)
        with self._cache_lock:
            cached = self._historical_stats_cache.get(cache_key)
            if cached and time.time() - self._historical_stats_cache_ts.get(cache_key, 0) < 3600:
                return cached

        month_counts = self.db.get_broker_recommend_month_counts(ts_codes, exclude_after=exclude_after)
        codes_set = set(ts_codes)
        bucket: Dict[str, Dict[str, Any]] = {
            tc: {
                "month_count": month_counts.get(tc, 0),
                "returns": [],
            }
            for tc in ts_codes
        }
        adj_all = self._load_all_adj_factors(ts_codes)
        # 若指定 exclude_after，仅统计该月之前的推荐记录
        if exclude_after and str(exclude_after) >= "202003":
            backtests = [bt for bt in self.db.get_all_broker_backtests() if str(bt.get("month") or "") < str(exclude_after)]
        else:
            backtests = self.db.get_all_broker_backtests()
        for bt in backtests:
            month = str(bt.get("month") or "")
            if not month:
                continue
            for sr in bt.get("stock_returns") or []:
                tc = str(sr.get("ts_code") or "")
                if tc not in codes_set:
                    continue
                daily = list(sr.get("daily_returns") or [])
                if daily and any(d.get("price") is not None for d in daily):
                    ret = self._period_return_from_daily_returns(tc, daily, adj_all)
                else:
                    _drs, ret, _bd, _sd = self._resolve_stock_holding_daily_returns(
                        tc, month, stored=bt, stock_return=sr,
                    )
                if ret is None:
                    continue
                bucket[tc]["returns"].append(ret)

        out: Dict[str, Dict[str, Any]] = {}
        for tc, acc in bucket.items():
            returns: List[float] = acc["returns"]
            if not returns:
                out[tc] = {
                    "month_count": acc["month_count"],
                    "period_count": 0,
                    "win_rate": None,
                    "max_return": None,
                    "max_drawdown": None,
                }
                continue
            wins = sum(1 for r in returns if r > 0)
            out[tc] = {
                "month_count": acc["month_count"],
                "period_count": len(returns),
                "win_rate": round(wins / len(returns), 4),
                "max_return": round(max(returns), 4),
                "max_drawdown": round(min(returns), 4),
            }
        # 写缓存
        with self._cache_lock:
            self._historical_stats_cache[cache_key] = out
            self._historical_stats_cache_ts[cache_key] = time.time()
        return out

    def get_historical_month_counts(self, ts_codes: List[str]) -> Dict[str, int]:
        """返回股票历史上被推荐的月份次数。"""
        return self.db.get_broker_recommend_month_counts(ts_codes)

    def get_monthly_up_to_down_daily(self, month: str) -> Dict[str, Any]:
        """扫描推荐月金股池各交易日九转反转信号：升 1..8 转降、降 1..8 升（推荐月末交易日忽略）。

        历史月份扫描区间延至今日，便于查看推荐月之后的反转；月初第 1 个交易日与上月末对比九转。
        """
        empty = {
            "month": month,
            "buy_date": "",
            "sell_date": "",
            "days": [],
        }
        df = self.get_monthly_recommendations(month)
        if df is None or df.empty:
            return empty

        meta = (
            df.groupby("ts_code", as_index=False)
            .agg(name=("name", "first"), broker_count=("broker_count", "max"))
        )
        pool = meta["ts_code"].tolist()
        name_by_code = {
            str(row.ts_code): str(row.name or "")
            for row in meta.itertuples(index=False)
        }
        broker_count_by_code = {
            str(row.ts_code): int(row.broker_count or 1)
            for row in meta.itertuples(index=False)
        }

        effective_end = self._effective_month_end(month)
        month_start = f"{month}01"
        today_str = date.today().strftime("%Y%m%d")
        # 历史推荐月：信号扫描延至今日，便于查看持仓期外的后续反转（如 5 月金股看 6/11）
        scan_end = max(effective_end, today_str)
        trading_days = self._get_trading_days(month_start, scan_end)
        if not trading_days:
            return empty

        prev_month_last_day = self._prev_month_last_trading_day(month)
        nineturn_cache = self._load_nineturn_by_trade_date_cache()
        date_pools = {td: pool for td in trading_days}
        if prev_month_last_day:
            date_pools[prev_month_last_day] = pool
        self._prefetch_nineturn_for_dates(date_pools, nineturn_cache)

        month_last_trading_day = self._calendar_month_last_trading_day(month)
        days_out: List[Dict[str, Any]] = []
        realtime_prefetched_ohlc: Optional[Dict[str, Dict[str, Dict[str, Optional[float]]]]] = None
        for i, signal_date in enumerate(trading_days):
            if month_last_trading_day and signal_date == month_last_trading_day:
                continue
            stocks: List[Dict[str, Any]] = []
            date_has_cache_data = bool(nineturn_cache.get(signal_date))

            if date_has_cache_data:
                for tc in pool:
                    snap = BrokerRecommendService._nineturn_reversal_snapshot(
                        trading_days, i, tc, nineturn_cache,
                        prev_month_last_day=prev_month_last_day,
                    )
                    if BrokerRecommendService._nineturn_up_to_down_on_day(
                        nineturn_cache, trading_days, i, tc,
                        prev_month_last_day=prev_month_last_day,
                        allow_any_up_count=True,
                    ):
                        prev_up = snap.get("prev_nineturn_up_count")
                        stocks.append({
                            "ts_code": tc,
                            "name": name_by_code.get(tc, ""),
                            "broker_count": broker_count_by_code.get(tc, 1),
                            "signal_type": "up_to_down",
                            "prev_nineturn_up_count": int(prev_up) if prev_up is not None else 0,
                            "prev_nineturn_down_count": 0,
                            "nineturn_up_count": snap.get("nineturn_up_count"),
                            "nineturn_down_count": snap.get("nineturn_down_count"),
                            "is_realtime": False,
                        })
                    elif BrokerRecommendService._nineturn_down_to_up_on_day(
                        nineturn_cache, trading_days, i, tc,
                        prev_month_last_day=prev_month_last_day,
                        allow_any_down_count=True,
                    ):
                        prev_down = snap.get("prev_nineturn_down_count")
                        stocks.append({
                            "ts_code": tc,
                            "name": name_by_code.get(tc, ""),
                            "broker_count": broker_count_by_code.get(tc, 1),
                            "signal_type": "down_to_up",
                            "prev_nineturn_up_count": 0,
                            "prev_nineturn_down_count": int(prev_down) if prev_down is not None else 0,
                            "nineturn_up_count": snap.get("nineturn_up_count"),
                            "nineturn_down_count": snap.get("nineturn_down_count"),
                            "is_realtime": False,
                        })
            else:
                # 无九转缓存数据：用实时价格估算 TD Sequential 翻转
                if i < 4:
                    continue
                if realtime_prefetched_ohlc is None:
                    realtime_prefetched_ohlc = self._prefetch_ohlc(
                        pool, trading_days[i - 4], signal_date, use_adj=False,
                    )
                date_4_ago = trading_days[i - 4]
                # 取今日实时行情作为 OHLC 补充
                _, _, rt_today_ohlc, _ = self._get_realtime_prices_batch(pool)
                prev_day = BrokerRecommendService._nineturn_prev_trade_date(
                    trading_days, i, prev_month_last_day,
                )
                if prev_day:
                    for tc in pool:
                        stock_ohlc = realtime_prefetched_ohlc.get(tc, {})
                        close_4_ago = stock_ohlc.get(date_4_ago, {}).get("close")
                        if close_4_ago is None:
                            continue
                        today_close = stock_ohlc.get(signal_date, {}).get("close")
                        if today_close is None:
                            today_close = rt_today_ohlc.get(tc, {}).get("close")
                        if today_close is None:
                            continue
                        prev_nt = BrokerRecommendService._normalize_nineturn_record(
                            nineturn_cache.get(prev_day, {}).get(tc),
                        )
                        prev_up = prev_nt["up_count"]
                        prev_down = prev_nt["down_count"]
                        # TD Sequential 简化：close > close[-4] → 上涨延续；< → 下跌延续
                        is_up_today = today_close > close_4_ago
                        is_down_today = today_close < close_4_ago
                        if prev_up >= 1 and not is_up_today:
                            stocks.append({
                                "ts_code": tc,
                                "name": name_by_code.get(tc, ""),
                                "broker_count": broker_count_by_code.get(tc, 1),
                                "signal_type": "up_to_down",
                                "prev_nineturn_up_count": prev_up,
                                "prev_nineturn_down_count": 0,
                                "nineturn_up_count": 0,
                                "nineturn_down_count": 0,
                                "is_realtime": True,
                            })
                        elif prev_down >= 1 and not is_down_today:
                                stocks.append({
                                    "ts_code": tc,
                                    "name": name_by_code.get(tc, ""),
                                    "broker_count": broker_count_by_code.get(tc, 1),
                                    "signal_type": "down_to_up",
                                    "prev_nineturn_up_count": 0,
                                    "prev_nineturn_down_count": prev_down,
                                    "nineturn_up_count": 0,
                                    "nineturn_down_count": 0,
                                    "is_realtime": True,
                                })

            if stocks:
                stocks.sort(
                    key=lambda x: (
                        -x["broker_count"],
                        x.get("signal_type") or "",
                        x["ts_code"],
                    ),
                )
                days_out.append({"date": signal_date, "stocks": stocks})

        days_out.sort(key=lambda x: x["date"], reverse=True)
        return {
            "month": month,
            "buy_date": trading_days[0],
            "sell_date": trading_days[-1],
            "days": days_out,
        }

    def get_stock_recommend_history(self, ts_code: str, exclude_after: str | None = None) -> Dict[str, Any]:
        """返回单只股票历次推荐月份及对应持仓期 K 线数据。"""
        rows = self.db.get_broker_recommend_by_stock(ts_code)
        if not rows:
            return {"ts_code": ts_code, "name": "", "entries": []}

        months = sorted({r["month"] for r in rows}, reverse=True)
        if exclude_after and str(exclude_after) >= "202003":
            months = [m for m in months if m < str(exclude_after)]
        name = str(rows[0].get("name") or "")
        entries: List[Dict[str, Any]] = []

        for month in months:
            month_rows = [r for r in rows if r["month"] == month]
            brokers = sorted({str(r.get("broker") or "") for r in month_rows if r.get("broker")})
            broker_count = max(
                (int(r.get("broker_count") or 1) for r in month_rows),
                default=len(brokers),
            )

            stored = self.db.get_broker_backtest(month)
            sr = None
            if stored:
                sr = next(
                    (x for x in stored.get("stock_returns", []) if x.get("ts_code") == ts_code),
                    None,
                )
            daily_returns, cum_ret, buy_date, sell_date = self._resolve_stock_holding_daily_returns(
                ts_code, month, stored=stored, stock_return=sr,
            )

            entries.append({
                "month": month,
                "brokers": brokers,
                "broker_count": broker_count,
                "buy_date": buy_date or f"{month}01",
                "sell_date": sell_date or self._effective_month_end(month),
                "cumulative_return": cum_ret,
                "daily_returns": daily_returns,
            })

        return {"ts_code": ts_code, "name": name, "entries": entries}

    def _append_live_current_month_backtest(
        self, all_backtests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """当月回测不落库，YTD/策略等跨月计算需按需实时补算。"""
        current_month = datetime.now().strftime("%Y%m")
        stored_months = {bt["month"] for bt in all_backtests}
        if current_month in stored_months:
            return all_backtests
        df = self.get_monthly_recommendations(current_month)
        if df is None or df.empty:
            return all_backtests
        try:
            current_bt = self.compute_backtest(current_month)
        except Exception:
            logger.exception("[BrokerRecommend] YTD 当月回测补算失败")
            return all_backtests
        if not current_bt or "error" in current_bt:
            return all_backtests
        merged = list(all_backtests)
        merged.append(current_bt)
        merged.sort(key=lambda x: x["month"])
        return merged

    def compute_ytd_backtest(self, year: Optional[str] = None, top_n: int = 5) -> Dict[str, Any]:
        """跨月复合回测：遍历指定月份（或全部月份），将月度回测结果乘法复合。

        year=None 时使用全部可用月份（有记录以来），
        指定 year 时仅使用该年份内的月份（年初至今）。
        历史月份读 broker_backtest_result；当月按需实时回测补入。
        """
        all_backtests = self._append_live_current_month_backtest(
            self.db.get_all_broker_backtests()
        )
        if year is not None:
            month_data = [bt for bt in all_backtests if bt["month"].startswith(str(year))]
        else:
            month_data = all_backtests

        if not month_data:
            return {"error": f"Year {year} has no data" if year else "No backtest data available"}

        broker_ytd: Dict[str, Dict[str, Any]] = {}

        for bt in month_data:
            month = bt["month"]
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
            b["monthly_returns"].sort(key=lambda mr: mr["month"], reverse=True)
            prev_cum = 0.0
            for dr in b["daily_returns"]:
                cum = dr["cumulative"]
                dr["return"] = round(cum - prev_cum, 4)
                prev_cum = cum
                all_dates.add(dr["date"])

        start_date = min(all_dates) if all_dates else (f"{year}0101" if year else "20200101")
        end_date = max(all_dates) if all_dates else (f"{year}1231" if year else "20991231")

        label = str(year) if year else "all"
        logger.info(f"[BrokerRecommend] YTD {label}: {len(broker_ytd)} brokers, "
                    f"top {len(sorted_brokers)}, {len(month_data)} months")

        return {
            "year": label,
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
                        # winner_rate 可能是百分比格式（>1）或小数格式（<1）；统一保留原始值，让前端乘以 100 显示
                        winner_rate = float(row.get("winner_rate", 0) or 0)
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

    # ------------------------------------------------------------------
    # 机构调研 Top 10
    # ------------------------------------------------------------------

    _SURVEY_WEIGHTS = {
        "特定对象调研": 2.0,
        "策略会": 1.0,
        "分析师会议": 1.0,
    }
    _SURVEY_CACHE_PATH = "/tmp/institution_survey_top10.json"

    @classmethod
    def _calc_survey_weight(cls, rece_mode: str) -> float:
        """根据调研方式计算权重。"""
        if "特定对象调研" in rece_mode:
            return 2.0
        if "策略会" in rece_mode or "分析师会议" in rece_mode:
            return 1.0
        return 0.3

    def refresh_institution_survey(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
        """从 Tushare 拉取机构调研数据并落库，自动补全缺失交易日。

        Args:
            start_date: 起始日期 YYYYMMDD（可选，默认 7 天前）
            end_date: 截止日期 YYYYMMDD（可选，默认今天）

        Returns:
            落库的记录数，失败返回 0。
        """
        from datetime import datetime

        if end_date:
            _end = end_date
        else:
            _end = datetime.now().strftime("%Y%m%d")
        if start_date:
            _start = start_date
        else:
            _start = (datetime.strptime(_end, "%Y%m%d") - timedelta(days=14)).strftime("%Y%m%d")

        from data_provider.tushare_fetcher import TushareFetcher
        tf = TushareFetcher.get_instance()
        if not tf or not tf.is_available():
            logger.warning("[InstitutionSurvey] Tushare 未配置，跳过刷新")
            return 0

        # 1. 批量拉取区间数据
        df = tf.get_stk_surv(_start, _end)
        total = 0
        if df is not None and not df.empty:
            df["weight"] = df["rece_mode"].apply(
                lambda m: self._calc_survey_weight(str(m) if m else "")
            )
            for surv_day in df["surv_date"].dropna().unique():
                day_df = df[df["surv_date"] == surv_day]
                try:
                    saved = self.db.save_institution_survey(day_df, clear_date=str(surv_day))
                    total += saved
                except Exception:
                    pass

        # 2. 检查缺失交易日，逐日补全
        try:
            import exchange_calendars as xcals
            import pandas as pd

            cal = xcals.get_calendar("XSHG")
            dates = pd.date_range(_start, _end)
            trading_days = [d.strftime("%Y%m%d") for d in dates if cal.is_session(d)]
            existing = set(self.db.get_institution_survey_dates())
            missing = [d for d in trading_days if d not in existing]

            if missing:
                logger.info("[InstitutionSurvey] 补全缺失交易日: %s", missing)
                for date in missing:
                    try:
                        day_df = tf.get_stk_surv(start_date=date, end_date=date)
                        if day_df is not None and not day_df.empty:
                            day_df["weight"] = day_df["rece_mode"].apply(
                                lambda m: self._calc_survey_weight(str(m) if m else "")
                            )
                            saved = self.db.save_institution_survey(day_df, clear_date=date)
                            total += saved
                    except Exception:
                        logger.debug("[InstitutionSurvey] 补全 %s 失败", date)
        except Exception:
            logger.debug("[InstitutionSurvey] 缺失日补全检查失败", exc_info=True)

        logger.info("[InstitutionSurvey] 落库完成: %s ~ %s, %d 条", _start, _end, total)
        return total

    def get_institution_survey_top10(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取机构调研加权 Top 10。

        Args:
            start_date: 起始日期 YYYYMMDD（可选，默认与 end_date 相同）
            end_date: 截止日期 YYYYMMDD（可选，默认最近一个有数据的日期）
        """
        import json as _json
        from datetime import datetime

        today_str = datetime.now().strftime("%Y-%m-%d")
        latest_db_dates = self.db.get_institution_survey_dates()

        # 计算日期范围：未指定时默认最近一个有数据的日期
        if end_date:
            _end = end_date
        elif latest_db_dates:
            _end = latest_db_dates[0]
        else:
            _end = datetime.now().strftime("%Y%m%d")
        if start_date:
            _start = start_date
        else:
            _start = _end

        use_db = bool(start_date or end_date or latest_db_dates)

        # 默认模式：走 Tushare API + 缓存
        if not use_db:
            # 尝试读取缓存
            try:
                with open(self._SURVEY_CACHE_PATH, "r", encoding="utf-8") as f:
                    cached = _json.load(f)
                if (
                    cached.get("date") == today_str
                    and cached.get("start_date") == _start
                    and cached.get("end_date") == _end
                ):
                    logger.info("[InstitutionSurvey] 命中缓存")
                    return cached
            except (FileNotFoundError, _json.JSONDecodeError, KeyError):
                pass

            from data_provider.tushare_fetcher import TushareFetcher
            tf = TushareFetcher.get_instance()
            if not tf or not tf.is_available():
                return {"error": "Tushare 未配置", "date": today_str, "items": []}

            df = tf.get_stk_surv(_start, _end)
            if df is None or df.empty:
                return {"date": today_str, "start_date": _start, "end_date": _end, "total_stocks": 0, "items": []}

            # 计算权重列并持久化原始数据
            df["weight"] = df["rece_mode"].apply(
                lambda m: self._calc_survey_weight(str(m) if m else "")
            )
            # 按日期分批写入，每日覆盖旧数据
            for surv_day in df["surv_date"].dropna().unique():
                day_df = df[df["surv_date"] == surv_day]
                try:
                    self.db.save_institution_survey(day_df, clear_date=str(surv_day))
                except Exception:
                    pass

            rows = [row for _, row in df.iterrows()]
        else:
            # 历史模式：走数据库查询
            records = self.db.get_institution_survey(_start, _end)
            if not records:
                return {"date": today_str, "start_date": _start, "end_date": _end, "total_stocks": 0, "items": []}

            # 将 ORM 对象转为 dict 列表（兼容下面的聚合逻辑）
            rows = []
            for r in records:
                rows.append({
                    "ts_code": r.ts_code,
                    "name": r.name,
                    "rece_mode": r.rece_mode or "",
                    "surv_date": r.surv_date,
                    "rece_org": r.rece_org or "",
                    "org_type": r.org_type or "",
                    "fund_visitors": r.fund_visitors or "",
                    "rece_place": r.rece_place or "",
                    "comp_rece": r.comp_rece or "",
                })

        # 按 ts_code 聚合加权分（DB/Tushare 共用）
        stock_scores: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            ts_code = str(row.get("ts_code", ""))
            if not ts_code:
                continue
            name = str(row.get("name", ""))
            if 'ST' in name.upper():
                continue
            if ts_code not in stock_scores:
                stock_scores[ts_code] = {
                    "ts_code": ts_code,
                    "name": name,
                    "weighted_score": 0.0,
                    "visit_count": 0,
                    "details": [],
                }
            rece_mode = str(row.get("rece_mode", ""))
            weight = self._calc_survey_weight(rece_mode)
            stock_scores[ts_code]["weighted_score"] += weight
            stock_scores[ts_code]["visit_count"] += 1
            stock_scores[ts_code]["details"].append({
                "surv_date": str(row.get("surv_date", "")),
                "rece_org": str(row.get("rece_org", "")),
                "org_type": str(row.get("org_type", "")),
                "rece_mode": rece_mode,
                "weight": weight,
                "fund_visitors": str(row.get("fund_visitors", "")),
                "rece_place": str(row.get("rece_place", "")),
                "comp_rece": str(row.get("comp_rece", "")),
            })

        # 排序取 Top 10
        sorted_stocks = sorted(stock_scores.values(), key=lambda x: x["weighted_score"], reverse=True)[:10]

        # 附加摘要信息
        for stock in sorted_stocks:
            stock["weighted_score"] = round(stock["weighted_score"], 1)
            stock["last_surv_date"] = max((d["surv_date"] for d in stock["details"]), default="")
            org_counter: Dict[str, int] = {}
            for d in stock["details"]:
                org = d["rece_org"]
                org_counter[org] = org_counter.get(org, 0) + 1
            stock["top_orgs"] = [o for o, _ in sorted(org_counter.items(), key=lambda x: x[1], reverse=True)[:5]]
            # 按日期降序排列详情
            stock["details"].sort(key=lambda x: x["surv_date"], reverse=True)

        result = {
            "date": today_str,
            "start_date": _start,
            "end_date": _end,
            "total_stocks": len(stock_scores),
            "items": sorted_stocks,
        }

        # 仅默认模式写入缓存
        if not use_db:
            try:
                with open(self._SURVEY_CACHE_PATH, "w", encoding="utf-8") as f:
                    _json.dump(result, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"[InstitutionSurvey] 写入缓存失败: {e}")

        return result

    def get_institution_survey_dates(self) -> List[str]:
        """获取数据库中所有有机构调研数据的日期列表（降序）。"""
        return self.db.get_institution_survey_dates()


    # 等权策略计算结果缓存
    _strategy_cache: Dict[str, Any] = {}
    _strategy_cache_ts: float = 0.0
    _STRATEGY_CACHE_TTL = 300  # 5 分钟
    _strategy_computing = False  # 防止并发重复计算
    _strategy_lock = Lock()
    _STRATEGY_MIN_HIST_WIN_RATE = 0.5
    _STRATEGY_TOTAL_CAPITAL = 1.0
    _STRATEGY_UP_TO_DOWN_ALLOWED_UP_COUNTS = tuple(range(1, 9))
    _STRATEGY_DOWN_TO_UP_ALLOWED_DOWN_COUNTS = tuple(range(1, 9))
    _STRATEGY_MAX_HOLDINGS = 0
    _STRATEGY_FORECAST_SPREAD_TOP_N = 0
    _STRATEGY_PRICE_PATTERN_BUY = ("--+", "+-+")
    _STRATEGY_PRICE_PATTERN_SELL = ("+--", "-+-")
    _STRATEGY_MONTH_END_DEFER_MAX_DAYS = 20

    @staticmethod
    def _forecast_abs_spread(forecast: Optional[Dict[str, Any]]) -> float:
        """券商目标价 max/min 绝对值之差（金股页展示用，策略买入排序已改用历史推荐统计）。"""
        if not forecast:
            return -1.0
        min_p = forecast.get("min_price")
        max_p = forecast.get("max_price")
        if min_p is None or max_p is None:
            return -1.0
        return abs(float(max_p)) - abs(float(min_p))

    @staticmethod
    def _hist_recommend_abs_spread(hist: Optional[Dict[str, Any]]) -> float:
        """历史推荐统计最高/最低持仓期末收益绝对值之差，用于策略买入候选排序。"""
        if not hist:
            return -1.0
        max_ret = hist.get("max_return")
        min_ret = hist.get("max_drawdown")
        if max_ret is None or min_ret is None:
            return -1.0
        return abs(float(max_ret)) - abs(float(min_ret))

    @staticmethod
    def _hist_recommend_positive_equal_extremes(hist: Optional[Dict[str, Any]]) -> bool:
        """历史最高/最低持仓期末收益相同且均为正（历次推荐期收益一致且盈利）。"""
        if not hist:
            return False
        max_ret = hist.get("max_return")
        min_ret = hist.get("max_drawdown")
        if max_ret is None or min_ret is None:
            return False
        max_v = float(max_ret)
        min_v = float(min_ret)
        return max_v == min_v and min_v > 0

    @staticmethod
    def _hist_recommend_buy_sort_key(
        hist: Optional[Dict[str, Any]],
    ) -> tuple:
        """买入优先级：max_return==max_drawdown 且均为正置顶，其余按绝对值差降序。"""
        spread = BrokerRecommendService._hist_recommend_abs_spread(hist)
        if spread < 0:
            return (0, 0.0, 0.0)
        max_v = float(hist["max_return"])
        if BrokerRecommendService._hist_recommend_positive_equal_extremes(hist):
            return (2, max_v, spread)
        return (1, spread, abs(max_v))

    @staticmethod
    def _top_spread_buy_codes(
        candidates: Iterable[str],
        hist_stats: Dict[str, Dict[str, Any]],
        top_n: int = 3,
        min_hist_win_rate: float = 0.5,
    ) -> List[str]:
        """历史 max==min 且均为正优先，其余按绝对值差降序，再过滤历史胜率；top_n<=0 不截断。"""
        ranked = sorted(
            candidates,
            key=lambda tc: (
                BrokerRecommendService._hist_recommend_buy_sort_key(
                    hist_stats.get(tc),
                ),
                tc,
            ),
            reverse=True,
        )
        picked: List[str] = []
        for tc in ranked:
            if not BrokerRecommendService._passes_hist_win_rate_filter(
                hist_stats.get(tc, {}).get("win_rate"), min_hist_win_rate,
            ):
                continue
            picked.append(tc)
            if top_n > 0 and len(picked) >= top_n:
                break
        return picked

    @staticmethod
    def _passes_hist_win_rate_filter(
        win_rate: Optional[float],
        min_rate: float = 0.5,
    ) -> bool:
        """无历史胜率记录或不大于阈值均不买入（默认 >50%）。"""
        if win_rate is None:
            return False
        return float(win_rate) > min_rate

    @staticmethod
    def _strategy_leg_capital_totals(
        monthly_returns: List[Dict[str, Any]],
    ) -> tuple[float, float]:
        """汇总策略各笔买卖额：投入本金=开仓买入额之和，结算资金=平仓卖出额之和。"""
        invested = 0.0
        settled = 0.0
        for month in monthly_returns:
            for leg in month.get("stocks") or []:
                buy_amt = leg.get("buy_amount")
                sell_amt = leg.get("sell_amount")
                if buy_amt:
                    invested += float(buy_amt)
                if sell_amt is not None:
                    settled += float(sell_amt)
        return invested, settled

    @staticmethod
    def _compute_up_to_down_trade_stats(
        monthly_returns: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """按升转降前日上升计数（升 1..8）统计每笔交易平均收益与胜率。"""
        buckets: Dict[int, List[float]] = {}
        for month_row in monthly_returns:
            for leg in month_row.get("stocks") or []:
                buy_reason = leg.get("buy_reason") or {}
                if buy_reason.get("trigger") != "nineturn_up_to_down_buy":
                    continue
                prev_up = buy_reason.get("prev_nineturn_up_count")
                ret = leg.get("month_return")
                if prev_up is None or ret is None:
                    continue
                prev_up = int(prev_up)
                if prev_up not in BrokerRecommendService._STRATEGY_UP_TO_DOWN_ALLOWED_UP_COUNTS:
                    continue
                buckets.setdefault(prev_up, []).append(float(ret))

        stats: List[Dict[str, Any]] = []
        for up in BrokerRecommendService._STRATEGY_UP_TO_DOWN_ALLOWED_UP_COUNTS:
            rets = buckets.get(up, [])
            if not rets:
                stats.append({
                    "up_count": up,
                    "trade_count": 0,
                    "avg_return": 0.0,
                    "win_rate": 0.0,
                })
                continue
            wins = sum(1 for r in rets if r > 0)
            stats.append({
                "up_count": up,
                "trade_count": len(rets),
                "avg_return": round(sum(rets) / len(rets), 4),
                "win_rate": round(wins / len(rets), 4),
            })
        return stats

    def compute_equal_weight_strategy(
        self,
        top_n: int = 4,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> Dict[str, Any]:
        """计算九转选股等权策略收益曲线（带缓存）。

        策略：总资金固定；当日收盘升 1..8 转降 N 股 T+1 开盘均摊买入；
        T+1 买入后 T+2 开盘亏损则 T+2 开盘卖、盈利则 T+3 起收盘跟踪直至亏损或月末强制清仓；升 9+ 转降忽略；
        当日无有效升转降则 T+1 开盘清仓后暂停；交易仅限当月（末交易日信号忽略）；
        总收益 = 结算后总资产 / 固定总资金 - 1；
        月末有行情收盘清仓，月末无行情则顺延至后续有行情交易日开盘清仓（可跨月）。
        首次计算在后台线程执行，接口立即返回 computing 供前端轮询。
        """
        import json
        cache_key = json.dumps({
            "strategy": "nineturn_up_to_down_open_v49",
            "start_month": start_month or "",
            "end_month": end_month or "",
        }, sort_keys=True)

        with BrokerRecommendService._strategy_lock:
            if (
                cache_key in BrokerRecommendService._strategy_cache
                and time.time() - BrokerRecommendService._strategy_cache_ts < self._STRATEGY_CACHE_TTL
            ):
                return BrokerRecommendService._strategy_cache[cache_key]
            if BrokerRecommendService._strategy_computing:
                return {"status": "computing"}
            BrokerRecommendService._strategy_computing = True

        def _worker() -> None:
            try:
                result = self._compute_equal_weight_strategy_impl(
                    top_n, start_month=start_month, end_month=end_month,
                )
            except Exception as exc:
                logger.warning("[BrokerRecommend] 策略回测计算失败: %s", exc)
                result = {"error": str(exc)}
            with BrokerRecommendService._strategy_lock:
                BrokerRecommendService._strategy_cache[cache_key] = result
                BrokerRecommendService._strategy_cache_ts = time.time()
                BrokerRecommendService._strategy_computing = False

        Thread(target=_worker, daemon=True, name="broker-equal-weight-strategy").start()
        return {"status": "computing"}

    def _load_nineturn_by_trade_date_cache(self) -> Dict[str, Dict[str, Dict[str, int]]]:
        """预加载九转缓存：{trade_date: {ts_code: {up_count, down_count, ...}}}。"""
        cache: Dict[str, Dict[str, Dict[str, int]]] = {}
        try:
            from src.storage import BrokerEnrichmentNineturn
            from sqlalchemy import select as sa_select

            db = DatabaseManager.get_instance()
            with db.get_session() as session:
                rows = session.execute(
                    sa_select(
                        BrokerEnrichmentNineturn.trade_date,
                        BrokerEnrichmentNineturn.ts_code,
                        BrokerEnrichmentNineturn.up_count,
                        BrokerEnrichmentNineturn.down_count,
                        BrokerEnrichmentNineturn.nine_up_turn,
                        BrokerEnrichmentNineturn.nine_down_turn,
                    )
                ).all()
            for r in rows:
                td = str(r.trade_date)
                cache.setdefault(td, {})[str(r.ts_code)] = {
                    "up_count": int(r.up_count or 0),
                    "down_count": int(r.down_count or 0),
                    "nine_up_turn": int(r.nine_up_turn or 0),
                    "nine_down_turn": int(r.nine_down_turn or 0),
                }
        except Exception:
            pass
        return cache

    @staticmethod
    def _normalize_nineturn_record(nt: Any) -> Dict[str, int]:
        if isinstance(nt, dict):
            return {
                "up_count": int(nt.get("up_count") or 0),
                "down_count": int(nt.get("down_count") or 0),
                "nine_up_turn": int(nt.get("nine_up_turn") or 0),
                "nine_down_turn": int(nt.get("nine_down_turn") or 0),
            }
        if isinstance(nt, int):
            return {"up_count": nt, "down_count": 0, "nine_up_turn": 0, "nine_down_turn": 0}
        return {"up_count": 0, "down_count": 0, "nine_up_turn": 0, "nine_down_turn": 0}

    @staticmethod
    def _nineturn_meets_buy_up_count(
        nt: Any,
        up_count: int | None = None,
    ) -> bool:
        target = up_count if up_count is not None else 5
        return BrokerRecommendService._normalize_nineturn_record(nt)["up_count"] == target

    @staticmethod
    def _nineturn_in_rising_phase(nt: Any) -> bool:
        """九转处于上升序列（up_count >= 1）。"""
        return BrokerRecommendService._normalize_nineturn_record(nt)["up_count"] >= 1

    @staticmethod
    def _nineturn_ok_for_buy(
        nt: Any,
        max_up_count: int | None = None,
    ) -> bool:
        """买入：九转上升且 up_count 不超过上限（默认 1..10，超过 10 不买）。"""
        cap = (
            BrokerRecommendService._STRATEGY_NINETURN_BUY_MAX_UP_COUNT
            if max_up_count is None
            else max_up_count
        )
        up = BrokerRecommendService._normalize_nineturn_record(nt)["up_count"]
        return 1 <= up <= cap

    @staticmethod
    def _nineturn_still_down_on_day(
        nineturn_cache: Dict[str, Dict[str, Dict[str, int]]],
        trading_days: List[str],
        day_idx: int,
        ts_code: str,
    ) -> bool:
        """升转降信号后 T+1 确认：当日仍处于下降（非上升且仍有下降计数）。"""
        if day_idx < 0 or day_idx >= len(trading_days):
            return False
        curr_nt = BrokerRecommendService._normalize_nineturn_record(
            nineturn_cache.get(trading_days[day_idx], {}).get(ts_code),
        )
        if BrokerRecommendService._nineturn_in_rising_phase(curr_nt):
            return False
        return (
            curr_nt["down_count"] >= 1
            or curr_nt["nine_down_turn"] >= 1
        )

    @staticmethod
    def _daily_close_direction(
        close_map: Dict[str, float],
        day: str,
        prev_day: str,
    ) -> Optional[str]:
        """相对前一交易日收盘：涨 '+'、跌 '-'；持平返回 None。"""
        curr = close_map.get(day)
        prev = close_map.get(prev_day)
        if curr is None or prev is None or prev <= 0:
            return None
        if curr > prev:
            return "+"
        if curr < prev:
            return "-"
        return None

    @staticmethod
    def _pattern_start_idx(
        trading_days: List[str],
        buy_date: Optional[str],
    ) -> int:
        """形态计算起点：买入日（持仓期首个交易日）在 trading_days 中的下标。"""
        if buy_date and buy_date in trading_days:
            return trading_days.index(buy_date)
        return 0

    @staticmethod
    def _three_day_price_pattern(
        close_map: Dict[str, float],
        trading_days: List[str],
        day_idx: int,
        pattern_start_idx: int = 0,
    ) -> Optional[str]:
        """近三个交易日涨跌形态（day_idx 为形态第三日；不计买入日之前）。"""
        if day_idx < pattern_start_idx + 3:
            return None
        if day_idx - 3 < pattern_start_idx:
            return None
        d_m3, d_m2, d_m1, d0 = (
            trading_days[day_idx - 3],
            trading_days[day_idx - 2],
            trading_days[day_idx - 1],
            trading_days[day_idx],
        )
        signs = [
            BrokerRecommendService._daily_close_direction(close_map, d_m2, d_m3),
            BrokerRecommendService._daily_close_direction(close_map, d_m1, d_m2),
            BrokerRecommendService._daily_close_direction(close_map, d0, d_m1),
        ]
        if any(s is None for s in signs):
            return None
        return "".join(signs)

    @staticmethod
    def _price_pattern_is_buy(pattern: Optional[str]) -> bool:
        return pattern in BrokerRecommendService._STRATEGY_PRICE_PATTERN_BUY

    @staticmethod
    def _price_pattern_is_sell(pattern: Optional[str]) -> bool:
        return pattern in BrokerRecommendService._STRATEGY_PRICE_PATTERN_SELL

    @staticmethod
    def _sign_label(sign: Optional[str]) -> str:
        if sign == "+":
            return "涨"
        if sign == "-":
            return "跌"
        return "平"

    @staticmethod
    def _trade_signal_snapshot(
        trading_days: List[str],
        day_idx: int,
        tc: str,
        close_map: Dict[str, float],
        nineturn_cache: Dict[str, Dict[str, Dict[str, int]]],
        pattern_start_idx: int = 0,
    ) -> Dict[str, Any]:
        """信号日近三日收盘走势与九转快照（用于买卖理由）。"""
        if day_idx < 0 or day_idx >= len(trading_days):
            return {}
        day = trading_days[day_idx]
        nt = BrokerRecommendService._normalize_nineturn_record(
            nineturn_cache.get(day, {}).get(tc),
        )
        base = {
            "signal_date": day,
            "nineturn_up_count": nt["up_count"],
            "nineturn_down_count": nt["down_count"],
        }
        if (
            day_idx < pattern_start_idx + 3
            or day_idx - 3 < pattern_start_idx
        ):
            return {
                **base,
                "pattern": None,
                "pattern_days": [],
                "pattern_closes": [],
                "pattern_signs": [],
                "day_moves": [],
            }
        d_m3, d_m2, d_m1, d0 = (
            trading_days[day_idx - 3],
            trading_days[day_idx - 2],
            trading_days[day_idx - 1],
            trading_days[day_idx],
        )
        dates = [d_m3, d_m2, d_m1, d0]
        closes = [close_map.get(d) for d in dates]
        signs = [
            BrokerRecommendService._daily_close_direction(close_map, d_m2, d_m3),
            BrokerRecommendService._daily_close_direction(close_map, d_m1, d_m2),
            BrokerRecommendService._daily_close_direction(close_map, d0, d_m1),
        ]
        pattern = (
            "".join(signs)
            if signs and all(s is not None for s in signs)
            else None
        )
        day_moves: List[Dict[str, Any]] = [{
            "date": dates[0],
            "close": round(closes[0], 2) if closes[0] is not None else None,
            "sign": None,
        }]
        for j in range(1, 4):
            day_moves.append({
                "date": dates[j],
                "close": round(closes[j], 2) if closes[j] is not None else None,
                "sign": signs[j - 1],
            })
        return {
            **base,
            "pattern": pattern,
            "pattern_days": dates,
            "pattern_closes": [
                round(c, 2) if c is not None else None for c in closes
            ],
            "pattern_signs": signs,
            "day_moves": day_moves,
        }

    @staticmethod
    def _build_trade_reason(
        trigger: str,
        snapshot: Dict[str, Any],
        *,
        action: str,
    ) -> Dict[str, Any]:
        """组装买卖理由（含可读 summary）。"""
        pattern = snapshot.get("pattern")
        up = snapshot.get("nineturn_up_count", 0)
        down = snapshot.get("nineturn_down_count", 0)
        signs = snapshot.get("pattern_signs") or []
        sign_path = "/".join(
            BrokerRecommendService._sign_label(s) for s in signs if s
        )
        nt_parts: List[str] = []
        if up:
            nt_parts.append(f"上升↑{up}")
        if down:
            nt_parts.append(f"下降↓{down}")
        nt_text = "、".join(nt_parts) if nt_parts else "无九转计数"
        if trigger == "month_end":
            summary = f"月末强制清仓；当日九转 {nt_text}"
        elif trigger == "month_end_deferred":
            next_day = snapshot.get("next_sell_date") or ""
            summary = (
                f"月末交易日无行情，顺延至有行情交易日"
                f"{f'（{next_day}）' if next_day else ''}开盘清仓"
            )
        elif trigger == "nineturn_buy":
            summary = f"九转上升（{nt_text}），收盘买入"
        elif trigger == "pattern_buy":
            summary = (
                f"九转上升（{nt_text}）；近3日 {sign_path}（{pattern}）"
                f"匹配买入规则，收盘买入"
            )
        elif trigger == "pattern_sell":
            summary = (
                f"九转上升（{nt_text}）；近3日 {sign_path}（{pattern}）"
                f"匹配卖出规则，收盘卖出"
            )
        elif trigger == "nineturn_up_to_down":
            prev_up = snapshot.get("prev_nineturn_up_count")
            confirm_day = snapshot.get("confirm_date") or ""
            summary = (
                f"九转上升转下降（前日↑{prev_up} → 当日{nt_text}）；"
                f"T+1仍下降"
                f"{f'（{confirm_day}）' if confirm_day else ''}，收盘卖出"
            )
        elif trigger == "nineturn_up_to_down_buy":
            prev_up = snapshot.get("prev_nineturn_up_count")
            summary = (
                f"九转升转降信号（前日↑{prev_up} → 当日{nt_text}）；"
                f"T+1 开盘均摊买入"
            )
        elif trigger == "nineturn_up_to_down_sell":
            summary = f"升转降批次 T+2 开盘卖出；信号日九转 {nt_text}"
        elif trigger == "nineturn_up_to_down_sell_loss":
            summary = f"升转降批次 T+2 开盘亏损卖出；信号日九转 {nt_text}"
        elif trigger == "nineturn_up_to_down_sell_profit":
            summary = f"升转降批次 T+2 盈利持有，T+3 收盘卖出；信号日九转 {nt_text}"
        elif trigger == "nineturn_up_to_down_sell_profit_t3":
            summary = (
                f"升转降批次 T+3 收盘未超买入日收盘价，收盘卖出；信号日九转 {nt_text}"
            )
        elif trigger == "nineturn_up_to_down_sell_profit_trail":
            summary = (
                f"升转降盈利持仓：T+3 后收盘超买入日收盘价继续持有，"
                f"当日收盘亏损平仓；信号日九转 {nt_text}"
            )
        elif trigger == "no_signal_liquidate":
            summary = "当日无升转降信号，T+1 开盘清仓后暂停交易"
        else:
            summary = f"{action}；九转 {nt_text}"
        return {
            "trigger": trigger,
            "action": action,
            "summary": summary,
            **snapshot,
        }

    @staticmethod
    def _nineturn_prev_trade_date(
        trading_days: List[str],
        day_idx: int,
        prev_month_last_day: Optional[str] = None,
    ) -> Optional[str]:
        """九转对比的前一交易日；月初第 1 天取上月末（若提供）。"""
        if day_idx < 0 or day_idx >= len(trading_days):
            return None
        if day_idx >= 1:
            return trading_days[day_idx - 1]
        return prev_month_last_day

    @staticmethod
    def _nineturn_reversal_snapshot(
        trading_days: List[str],
        day_idx: int,
        tc: str,
        nineturn_cache: Dict[str, Dict[str, Dict[str, int]]],
        prev_month_last_day: Optional[str] = None,
    ) -> Dict[str, Any]:
        """升转降信号日九转快照（用于卖出理由）。"""
        if day_idx < 0 or day_idx >= len(trading_days):
            return {}
        prev_d = BrokerRecommendService._nineturn_prev_trade_date(
            trading_days, day_idx, prev_month_last_day,
        )
        if not prev_d:
            return {}
        curr_d = trading_days[day_idx]
        prev_nt = BrokerRecommendService._normalize_nineturn_record(
            nineturn_cache.get(prev_d, {}).get(tc),
        )
        curr_nt = BrokerRecommendService._normalize_nineturn_record(
            nineturn_cache.get(curr_d, {}).get(tc),
        )
        return {
            "signal_date": curr_d,
            "nineturn_up_count": curr_nt["up_count"],
            "nineturn_down_count": curr_nt["down_count"],
            "prev_nineturn_up_count": prev_nt["up_count"],
            "prev_nineturn_down_count": prev_nt["down_count"],
            "pattern": None,
            "pattern_days": [],
            "pattern_closes": [],
            "pattern_signs": [],
            "day_moves": [],
        }

    @staticmethod
    def _nineturn_up_to_down_on_day(
        nineturn_cache: Dict[str, Dict[str, Dict[str, int]]],
        trading_days: List[str],
        day_idx: int,
        ts_code: str,
        prev_month_last_day: Optional[str] = None,
        allow_any_up_count: bool = False,
    ) -> bool:
        """第 day_idx 个交易日收盘判定：上升序列转下降（↓1 / 下跌九转 / 上升计数归零）。

        allow_any_up_count=True 时不限制前一日上升计数范围（展示用）；
        allow_any_up_count=False（默认）仅前一日上升计数在 1..8（含）时视为有效（策略用）。
        day_idx=0 时前一日为上月末交易日（若提供）。
        """
        if day_idx < 0 or day_idx >= len(trading_days):
            return False
        prev_d = BrokerRecommendService._nineturn_prev_trade_date(
            trading_days, day_idx, prev_month_last_day,
        )
        if not prev_d:
            return False
        curr_d = trading_days[day_idx]
        prev_raw = nineturn_cache.get(prev_d, {}).get(ts_code)
        curr_raw = nineturn_cache.get(curr_d, {}).get(ts_code)
        # 无真实九转数据时不判定翻转，避免无数据时的全零填充误报
        if prev_raw is None or curr_raw is None:
            return False
        prev_nt = BrokerRecommendService._normalize_nineturn_record(prev_raw)
        curr_nt = BrokerRecommendService._normalize_nineturn_record(curr_raw)
        prev_up = prev_nt["up_count"]
        if prev_up < 1:
            return False
        if not allow_any_up_count and prev_up not in BrokerRecommendService._STRATEGY_UP_TO_DOWN_ALLOWED_UP_COUNTS:
            return False
        return (
            curr_nt["down_count"] >= 1
            or curr_nt["nine_down_turn"] >= 1
            or (curr_nt["up_count"] == 0 and prev_up >= 1)
        )

    @staticmethod
    def _nineturn_down_to_up_on_day(
        nineturn_cache: Dict[str, Dict[str, Dict[str, int]]],
        trading_days: List[str],
        day_idx: int,
        ts_code: str,
        prev_month_last_day: Optional[str] = None,
        allow_any_down_count: bool = False,
    ) -> bool:
        """第 day_idx 个交易日收盘判定：下降序列转上升（↑1 / 上涨九转 / 下降计数归零）。

        allow_any_down_count=True 时不限制前一日下降计数范围（展示用）；
        allow_any_down_count=False（默认）仅前一日下降计数在 1..8（含）时视为有效（策略用）。
        day_idx=0 时前一日为上月末交易日（若提供）。
        """
        if day_idx < 0 or day_idx >= len(trading_days):
            return False
        prev_d = BrokerRecommendService._nineturn_prev_trade_date(
            trading_days, day_idx, prev_month_last_day,
        )
        if not prev_d:
            return False
        curr_d = trading_days[day_idx]
        prev_raw = nineturn_cache.get(prev_d, {}).get(ts_code)
        curr_raw = nineturn_cache.get(curr_d, {}).get(ts_code)
        if prev_raw is None or curr_raw is None:
            return False
        prev_nt = BrokerRecommendService._normalize_nineturn_record(prev_raw)
        curr_nt = BrokerRecommendService._normalize_nineturn_record(curr_raw)
        prev_down = prev_nt["down_count"]
        if prev_down < 1:
            return False
        if not allow_any_down_count and prev_down not in BrokerRecommendService._STRATEGY_DOWN_TO_UP_ALLOWED_DOWN_COUNTS:
            return False
        return (
            curr_nt["up_count"] >= 1
            or curr_nt["nine_up_turn"] >= 1
        )

    def _build_month_trading_days(
        self, month_sell_pairs: List[tuple],
    ) -> Dict[str, List[str]]:
        """一次拉取交易日历，返回各月持仓期交易日列表。"""
        if not month_sell_pairs:
            return {}
        min_start = min(f"{month}01" for month, _ in month_sell_pairs)
        max_end = max(sell_date for _, sell_date in month_sell_pairs)
        all_days = self._get_trading_days(min_start, max_end)
        days_by_month: Dict[str, List[str]] = {}
        for td in all_days:
            days_by_month.setdefault(td[:6], []).append(td)

        month_trading_days: Dict[str, List[str]] = {}
        for month, sell_date in month_sell_pairs:
            month_days = [d for d in days_by_month.get(month, []) if d <= sell_date]
            if month_days:
                month_trading_days[month] = month_days
        return month_trading_days

    def _prefetch_nineturn_for_dates(
        self,
        date_pools: Dict[str, List[str]],
        cache: Dict[str, Dict[str, Dict[str, int]]],
    ) -> None:
        """按交易日批量预取九转：内存/SQLite 缓存优先，每交易日至多一次全量 API。"""
        if not date_pools:
            return

        tf = None
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher.get_instance()
            if not tf.is_available:
                tf = None
        except Exception:
            tf = None

        to_persist: Dict[str, Dict[str, Any]] = {}

        for trade_date in sorted(date_pools.keys()):
            codes = list(dict.fromkeys(date_pools[trade_date]))
            by_date = cache.setdefault(trade_date, {})
            missing = [tc for tc in codes if tc not in by_date]
            if not missing:
                continue

            try:
                db_cache = self.db.get_enrichment_cache(missing, trade_date)
                for tc, data in (db_cache or {}).items():
                    nt = data.get("nineturn")
                    if not nt:
                        continue
                    by_date[tc] = {
                        "up_count": int(nt.get("up_count", 0) or 0),
                        "down_count": int(nt.get("down_count", 0) or 0),
                        "nine_up_turn": int(nt.get("nine_up_turn", 0) or 0),
                        "nine_down_turn": int(nt.get("nine_down_turn", 0) or 0),
                    }
            except Exception as exc:
                logger.debug("[BrokerRecommend] 九转 SQLite 预取失败 %s: %s", trade_date, exc)

            missing = [tc for tc in missing if tc not in by_date]
            if not missing:
                continue

            if tf is None:
                continue

            try:
                bulk = tf.get_bulk_nineturn(
                    missing, trade_date, fallback_per_stock=False,
                )
                for tc, nt in bulk.items():
                    row = {
                        "up_count": int(nt.get("up_count", 0) or 0),
                        "down_count": int(nt.get("down_count", 0) or 0),
                        "nine_up_turn": int(nt.get("nine_up_turn", 0) or 0),
                        "nine_down_turn": int(nt.get("nine_down_turn", 0) or 0),
                    }
                    by_date[tc] = row
                    to_persist[tc] = {
                        "trade_date": trade_date,
                        **row,
                    }
                for tc in missing:
                    pass  # 不留 filler 条目，无真实数据的股票不会出现在 cache 中
            except Exception as exc:
                logger.debug("[BrokerRecommend] 预取九转失败 %s: %s", trade_date, exc)
                for tc in missing:
                    pass  # 同上一并；不填充空记录

        if to_persist:
            try:
                self.db.save_enrichment_cache(nineturn_data=to_persist)
            except Exception:
                pass

    @staticmethod
    def _daily_open_as_raw(
        open_val: Any,
        close_raw: Any,
        adj_factor: float,
    ) -> Optional[float]:
        """将日序列中的开盘价统一为不复权口径（兼容历史已存后复权 open）。"""
        if open_val is None:
            return None
        try:
            o = float(open_val)
        except (TypeError, ValueError):
            return None
        if close_raw is None or adj_factor <= 1.01:
            return o
        try:
            c = float(close_raw)
        except (TypeError, ValueError):
            return o
        if c <= 0:
            return o
        # price 为不复权收盘时，open/price ≈ adj_factor 说明 open 已后复权
        if abs((o / c) - adj_factor) / adj_factor < 0.12:
            return o / adj_factor
        return o

    def _build_stock_adj_ohlc_maps(
        self, ts_code: str, daily_returns: List[Dict[str, Any]],
    ) -> tuple:
        """从回测日序列提取不复权与后复权开盘/收盘价映射。"""
        code = ts_code.split(".")[0] if "." in ts_code else ts_code
        adj_map = self._load_all_adj_factors([ts_code]).get(code, {})
        raw_open: Dict[str, float] = {}
        raw_close: Dict[str, float] = {}
        adj_open: Dict[str, float] = {}
        adj_close: Dict[str, float] = {}
        for dr in sorted(daily_returns, key=lambda x: str(x.get("date", ""))):
            d = str(dr.get("date", ""))
            if not d:
                continue
            f = self._lookup_adj_factor(adj_map, d) if adj_map else 1.0
            close_raw = dr.get("price", dr.get("close"))
            if close_raw is not None:
                close_f = float(close_raw)
                raw_close[d] = round(close_f, 6)
                adj_close[d] = round(close_f * f, 6)
            open_field = dr.get("open", close_raw)
            open_raw = self._daily_open_as_raw(open_field, close_raw, f)
            if open_raw is not None:
                raw_open[d] = round(open_raw, 6)
                adj_open[d] = round(open_raw * f, 6)
        return adj_open, adj_close, raw_open, raw_close

    @staticmethod
    def _enrich_daily_returns_for_trading_days(
        daily_returns: List[Dict[str, Any]],
        trading_days: List[str],
        ohlc_by_date: Dict[str, Dict[str, Optional[float]]],
    ) -> List[Dict[str, Any]]:
        """为策略交易日补齐缺失 K 线（避免末日开盘/收盘无价）。"""
        if not trading_days or not ohlc_by_date:
            return daily_returns
        existing = {str(d.get("date", "")) for d in daily_returns}
        enriched = list(daily_returns)
        for d in trading_days:
            if d in existing:
                continue
            bar = ohlc_by_date.get(d) or {}
            close = bar.get("close")
            if close is None:
                continue
            enriched.append({
                "date": d,
                "price": round(float(close), 4),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
            })
        return sorted(enriched, key=lambda x: str(x.get("date", "")))

    @staticmethod
    def _resolve_leg_exit_raw_price(
        leg: Dict[str, Any],
        tc: str,
        sell_day: Optional[str],
        raw_open_maps: Dict[str, Dict[str, float]],
        raw_close_maps: Dict[str, Dict[str, float]],
    ) -> Optional[float]:
        """卖出展示价：开盘卖优先开盘价，缺失时回退收盘价。"""
        if not sell_day:
            return None
        if leg.get("exit_at_close"):
            order = (raw_close_maps,)
        else:
            order = (raw_open_maps, raw_close_maps)
        for price_map in order:
            px = price_map.get(tc, {}).get(sell_day)
            if px is not None:
                return px
        return None


    @staticmethod
    def _simulate_month_nineturn_rotation(
        trading_days: List[str],
        stock_books: Dict[str, Dict[str, Any]],
        nineturn_cache: Dict[str, Dict[str, Dict[str, int]]],
        nav_start: float,
        hist_stats: Optional[Dict[str, Dict[str, Any]]] = None,
        min_hist_win_rate: float = 0.5,
        max_holdings: int = 3,
        forecast_spread_top_n: int = 3,
        invested_principal_so_far: float = 0.0,
        fixed_trade_amount: Optional[float] = None,
        buy_patterns: tuple = (),
        sell_patterns: tuple = (),
        post_month_trading_days: Optional[List[str]] = None,
        pattern_start_idx: int = 0,
        use_price_pattern: Optional[bool] = None,
        total_capital: Optional[float] = None,
        prev_month_last_day: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """升转降策略：总资金固定；信号日 N 股 T+1 开盘均摊买；无信号 T+1 清仓后暂停。

        卖出：T+2 开盘亏损则 T+2 开盘卖；盈利则 T+3 起收盘评估，
        T+3 收盘超买入日收盘价则继续持有，直至收盘亏损卖出；
        月末最后交易日收盘强制清仓（无行情则顺延开盘清仓，可跨月）。
        升转降买入接受升 1..8 转降。月初第 1 天与上月末对比九转；
        交易仅限当月：末交易日升转降忽略；T+1/T+2 须落在当月内才开仓。
        """
        capital = (
            float(total_capital)
            if total_capital is not None
            else BrokerRecommendService._STRATEGY_TOTAL_CAPITAL
        )
        if len(trading_days) < 3:
            return None

        pool = list(stock_books.keys())
        month_last_day = trading_days[-1]
        post_month_days = [
            d for d in (post_month_trading_days or [])
            if d > month_last_day
        ]
        sim_days = list(trading_days) + post_month_days
        last_month_idx = len(trading_days) - 1
        last_sim_idx = len(sim_days) - 1
        post_month_day_set = set(post_month_days)

        close_maps = {tc: stock_books[tc]["adj_close"] for tc in pool}
        open_maps = {tc: stock_books[tc]["adj_open"] for tc in pool}
        raw_open_maps = {
            tc: stock_books[tc].get("raw_open") or stock_books[tc]["adj_open"]
            for tc in pool
        }
        raw_close_maps = {
            tc: stock_books[tc].get("raw_close") or stock_books[tc]["adj_close"]
            for tc in pool
        }
        month_day_set = set(trading_days)

        cash = float(nav_start if nav_start > 0 else capital)
        shares: Dict[str, float] = {}
        holdings: set = set()
        trading_paused = False

        schedule_buy: Dict[int, tuple] = {}
        schedule_t2_eval: Dict[int, tuple] = {}
        schedule_liquidate: set = set()

        daily_rows: List[Dict[str, Any]] = []
        open_legs: Dict[str, Dict[str, Any]] = {}
        completed_legs: List[Dict[str, Any]] = []
        first_buy_day: Optional[str] = None

        def _open_px(tc: str, day: str) -> Optional[float]:
            px = open_maps.get(tc, {}).get(day)
            if px is None or px <= 0:
                px = close_maps.get(tc, {}).get(day)
            return px if px and px > 0 else None

        def _close_px(tc: str, day: str) -> Optional[float]:
            px = close_maps.get(tc, {}).get(day)
            if px is None or px <= 0:
                px = open_maps.get(tc, {}).get(day)
            return px if px and px > 0 else None

        def _nav_at(day: str) -> float:
            total = cash
            for tc, sh in shares.items():
                px = close_maps.get(tc, {}).get(day)
                if px and sh:
                    total += sh * px
            return total

        def _open_leg(
            tc: str,
            buy_date: str,
            *,
            buy_reason: Optional[Dict[str, Any]] = None,
            buy_amount: Optional[float] = None,
            signal_day_idx: Optional[int] = None,
        ) -> None:
            open_legs[tc] = {
                "ts_code": tc,
                "name": stock_books[tc]["name"],
                "buy_date": buy_date,
                "entry_at_close": False,
                "buy_reason": buy_reason,
                "buy_amount": buy_amount,
                "signal_day_idx": signal_day_idx,
                "profit_trail": False,
                "trail_active": False,
                "entry_close_ref": None,
            }

        def _close_leg(
            tc: str,
            sell_date: str,
            *,
            sell_reason: Optional[Dict[str, Any]] = None,
            sell_amount: Optional[float] = None,
            exit_at_close: bool = False,
        ) -> None:
            leg = open_legs.pop(tc, None)
            if leg:
                completed_legs.append({
                    **leg,
                    "sell_date": sell_date,
                    "exit_at_close": exit_at_close,
                    "sell_reason": sell_reason,
                    "sell_amount": sell_amount,
                })

        def _sell_codes_at_open(
            day: str,
            day_idx: int,
            codes: set,
            *,
            trigger: str,
            action: str,
            signal_day_idx: Optional[int] = None,
        ) -> None:
            nonlocal cash
            for tc in codes:
                sh = shares.get(tc, 0.0)
                if not sh:
                    continue
                op = _open_px(tc, day)
                if not op:
                    continue
                shares.pop(tc, None)
                proceeds = sh * op
                cash += proceeds
                holdings.discard(tc)
                if signal_day_idx is not None:
                    snap = BrokerRecommendService._nineturn_reversal_snapshot(
                        sim_days, signal_day_idx, tc, nineturn_cache,
                        prev_month_last_day=prev_month_last_day,
                    )
                else:
                    prev_sig = BrokerRecommendService._nineturn_prev_trade_date(
                        sim_days, day_idx, prev_month_last_day,
                    )
                    snap = {"signal_date": prev_sig or day}
                sell_reason = BrokerRecommendService._build_trade_reason(
                    trigger, snap, action=action,
                )
                _close_leg(tc, day, sell_reason=sell_reason, sell_amount=proceeds)

        def _sell_codes_at_close(
            day: str,
            day_idx: int,
            codes: set,
            *,
            trigger: str,
            action: str,
            signal_day_idx: Optional[int] = None,
        ) -> None:
            nonlocal cash
            for tc in codes:
                sh = shares.get(tc, 0.0)
                if not sh:
                    continue
                cp = _close_px(tc, day)
                if not cp:
                    continue
                shares.pop(tc, None)
                proceeds = sh * cp
                cash += proceeds
                holdings.discard(tc)
                if signal_day_idx is not None:
                    snap = BrokerRecommendService._nineturn_reversal_snapshot(
                        sim_days, signal_day_idx, tc, nineturn_cache,
                        prev_month_last_day=prev_month_last_day,
                    )
                else:
                    prev_sig = BrokerRecommendService._nineturn_prev_trade_date(
                        sim_days, day_idx, prev_month_last_day,
                    )
                    snap = {"signal_date": prev_sig or day}
                sell_reason = BrokerRecommendService._build_trade_reason(
                    trigger, snap, action=action,
                )
                _close_leg(
                    tc, day, sell_reason=sell_reason, sell_amount=proceeds,
                    exit_at_close=True,
                )

        def _eval_t2_sell_batch(day: str, day_idx: int, codes: set, sig_idx: int) -> None:
            """T+2 开盘按盈亏分岔：亏损 T+2 开盘卖，盈利进入 T+3 起收盘跟踪。"""
            loss_codes: set = set()
            for tc in codes:
                if tc not in holdings:
                    continue
                leg = open_legs.get(tc)
                if not leg:
                    continue
                entry_day = leg.get("buy_date")
                if not entry_day:
                    continue
                entry_px = _open_px(tc, entry_day)
                t2_open = _open_px(tc, day)
                if entry_px is None or t2_open is None:
                    continue
                if t2_open > entry_px:
                    leg["profit_trail"] = True
                    leg["trail_active"] = False
                    leg["entry_close_ref"] = _close_px(tc, entry_day)
                    leg["signal_day_idx"] = sig_idx
                else:
                    loss_codes.add(tc)
            if loss_codes:
                _sell_codes_at_open(
                    day,
                    day_idx,
                    loss_codes,
                    trigger="nineturn_up_to_down_sell_loss",
                    action="T+2开盘亏损卖出",
                    signal_day_idx=sig_idx,
                )

        def _eval_profit_trail_at_close(day: str, day_idx: int) -> None:
            """盈利仓：T+3 收盘超买入日收盘价则继续持有，直至收盘亏损卖出。"""
            pending: List[tuple] = []
            for tc in list(holdings):
                leg = open_legs.get(tc)
                if not leg or not leg.get("profit_trail"):
                    continue
                buy_date = leg.get("buy_date")
                if not buy_date or buy_date not in sim_days:
                    continue
                buy_idx = sim_days.index(buy_date)
                if day_idx < buy_idx + 2:
                    continue
                entry_open = _open_px(tc, buy_date)
                entry_close = leg.get("entry_close_ref")
                if entry_close is None:
                    entry_close = _close_px(tc, buy_date)
                curr_close = _close_px(tc, day)
                if entry_open is None or entry_close is None or curr_close is None:
                    continue
                sig_idx = leg.get("signal_day_idx")
                if not leg.get("trail_active"):
                    if curr_close > entry_close:
                        leg["trail_active"] = True
                        continue
                    pending.append((
                        tc,
                        sig_idx,
                        "nineturn_up_to_down_sell_profit_t3",
                        "T+3收盘未超买入日收盘价卖出",
                    ))
                elif curr_close < entry_open:
                    pending.append((
                        tc,
                        sig_idx,
                        "nineturn_up_to_down_sell_profit_trail",
                        "盈利持仓收盘亏损卖出",
                    ))
            for tc, sig_idx, trigger, action in pending:
                _sell_codes_at_close(
                    day,
                    day_idx,
                    {tc},
                    trigger=trigger,
                    action=action,
                    signal_day_idx=sig_idx,
                )

        def _strict_close_px(tc: str, day: str) -> Optional[float]:
            px = close_maps.get(tc, {}).get(day)
            return px if px and px > 0 else None

        def _force_month_end_liquidate_at_close(day: str, day_idx: int) -> None:
            """月末最后交易日：有收盘价则收盘强制清仓。"""
            if day_idx != last_month_idx or not holdings:
                return
            closable = {
                tc for tc in holdings
                if _strict_close_px(tc, day)
            }
            if closable:
                _sell_codes_at_close(
                    day,
                    day_idx,
                    closable,
                    trigger="month_end",
                    action="月末强制清仓",
                )

        def _deferred_month_end_liquidate_at_open(day: str, day_idx: int) -> None:
            """月末无收盘价时，顺延至下月有开盘价交易日开盘清仓。"""
            if day_idx <= last_month_idx or not holdings:
                return
            sellable = {
                tc for tc in holdings
                if _open_px(tc, day)
            }
            if sellable:
                _sell_codes_at_open(
                    day,
                    day_idx,
                    sellable,
                    trigger="month_end_deferred",
                    action="月末顺延开盘清仓",
                )

        def _liquidate_all_at_open(day: str, day_idx: int) -> None:
            if not holdings:
                return
            _sell_codes_at_open(
                day,
                day_idx,
                set(holdings),
                trigger="no_signal_liquidate",
                action="无信号清仓",
                signal_day_idx=None,
            )

        def _buy_batch_at_open(
            day: str,
            day_idx: int,
            codes: List[str],
            signal_day_idx: int,
        ) -> None:
            nonlocal cash, first_buy_day
            if not codes or holdings:
                return
            n = len(codes)
            deploy = min(capital, cash)
            if deploy <= 0:
                return
            budget_each = deploy / n
            for tc in codes:
                if tc in holdings:
                    continue
                op = _open_px(tc, day)
                if not op:
                    continue
                shares[tc] = budget_each / op
                cash -= budget_each
                holdings.add(tc)
                snap = BrokerRecommendService._nineturn_reversal_snapshot(
                    sim_days, signal_day_idx, tc, nineturn_cache,
                    prev_month_last_day=prev_month_last_day,
                )
                buy_reason = BrokerRecommendService._build_trade_reason(
                    "nineturn_up_to_down_buy", snap, action="升转降买入",
                )
                _open_leg(
                    tc,
                    day,
                    buy_reason=buy_reason,
                    buy_amount=budget_each,
                    signal_day_idx=signal_day_idx,
                )
                if first_buy_day is None:
                    first_buy_day = day

        if trading_days:
            daily_rows.append({
                "date": trading_days[0],
                "nav": cash,
                "stock_count": 0,
            })

        for i, day in enumerate(sim_days):
            if i in schedule_t2_eval:
                codes, sig_idx = schedule_t2_eval.pop(i)
                _eval_t2_sell_batch(day, i, set(codes) & holdings, sig_idx)

            if i in schedule_liquidate:
                _liquidate_all_at_open(day, i)
                trading_paused = True
                schedule_liquidate.discard(i)

            if i in schedule_buy and not trading_paused:
                codes, sig_idx = schedule_buy.pop(i)
                _buy_batch_at_open(day, i, list(codes), sig_idx)

            if 0 <= i < last_month_idx:
                signal_codes = [
                    tc for tc in pool
                    if BrokerRecommendService._nineturn_up_to_down_on_day(
                        nineturn_cache, sim_days, i, tc,
                        prev_month_last_day=prev_month_last_day,
                    )
                ]
                if (
                    signal_codes
                    and i + 2 <= last_month_idx
                    and not holdings
                ):
                    trading_paused = False
                    schedule_buy[i + 1] = (signal_codes, i)
                    schedule_t2_eval[i + 2] = (set(signal_codes), i)
                elif not signal_codes:
                    next_i = i + 1
                    profit_trail_hold = any(
                        open_legs.get(tc, {}).get("profit_trail")
                        for tc in holdings
                    )
                    if (
                        next_i <= last_month_idx
                        and next_i not in schedule_t2_eval
                        and next_i not in schedule_buy
                        and not profit_trail_hold
                    ):
                        schedule_liquidate.add(next_i)

            if i <= last_month_idx:
                _eval_profit_trail_at_close(day, i)
            if i == last_month_idx:
                _force_month_end_liquidate_at_close(day, i)
            elif i > last_month_idx:
                _deferred_month_end_liquidate_at_open(day, i)

            if day in month_day_set:
                nav_close = _nav_at(day) if holdings else cash
                daily_rows.append({
                    "date": day,
                    "nav": nav_close,
                    "stock_count": len(holdings),
                })

        if holdings:
            last_day = sim_days[last_sim_idx]
            for tc in list(holdings):
                sh = shares.get(tc, 0.0)
                if not sh:
                    continue
                op = _open_px(tc, last_day)
                if not op:
                    cp = _close_px(tc, last_day)
                    if not cp:
                        continue
                    op = cp
                shares.pop(tc, None)
                proceeds = sh * op
                cash += proceeds
                holdings.discard(tc)
                snap = {"signal_date": month_last_day, "next_sell_date": last_day}
                sell_reason = BrokerRecommendService._build_trade_reason(
                    "month_end_deferred",
                    snap,
                    action="月末顺延开盘清仓",
                )
                _close_leg(
                    tc,
                    last_day,
                    sell_reason=sell_reason,
                    sell_amount=proceeds,
                    exit_at_close=False,
                )
            if daily_rows and daily_rows[-1]["date"] == month_last_day:
                daily_rows[-1]["nav"] = cash
                daily_rows[-1]["stock_count"] = 0

        nav_end = cash
        month_return = (nav_end / nav_start - 1.0) if nav_start > 0 else 0.0

        stocks_detail = []
        allowed_sell_days = month_day_set | post_month_day_set
        for leg in sorted(
            (
                lg for lg in completed_legs
                if lg.get("buy_date") in month_day_set
                and lg.get("sell_date") in allowed_sell_days
            ),
            key=lambda x: (x.get("buy_date") or "", x["ts_code"]),
        ):
            tc = leg["ts_code"]
            entry = leg.get("buy_date")
            sell_day = leg.get("sell_date")
            entry_px_adj = open_maps[tc].get(entry) if entry else None
            entry_px_raw = raw_open_maps[tc].get(entry) if entry else None
            exit_px_raw = BrokerRecommendService._resolve_leg_exit_raw_price(
                leg, tc, sell_day, raw_open_maps, raw_close_maps,
            )
            exit_px_adj = None
            if sell_day:
                if leg.get("exit_at_close"):
                    exit_px_adj = close_maps[tc].get(sell_day) or open_maps[tc].get(sell_day)
                else:
                    exit_px_adj = open_maps[tc].get(sell_day) or close_maps[tc].get(sell_day)
            if (
                exit_px_adj is None
                and exit_px_raw is not None
                and entry_px_adj
                and entry_px_raw
            ):
                exit_px_adj = entry_px_adj * (exit_px_raw / entry_px_raw)
            buy_amt = leg.get("buy_amount")
            sell_amt = leg.get("sell_amount")
            stock_ret = None
            if sell_amt is not None and buy_amt and buy_amt > 0:
                stock_ret = round((float(sell_amt) - float(buy_amt)) / float(buy_amt), 4)
            elif entry_px_adj and exit_px_adj and entry_px_adj > 0:
                stock_ret = round((exit_px_adj - entry_px_adj) / entry_px_adj, 4)
            stocks_detail.append({
                "ts_code": tc,
                "name": leg["name"],
                "month_return": stock_ret,
                "buy_date": entry,
                "sell_date": sell_day,
                "buy_price": round(entry_px_raw, 2) if entry_px_raw is not None else None,
                "buy_amount": buy_amt,
                "sell_price": round(exit_px_raw, 2) if exit_px_raw is not None else None,
                "sell_amount": round(sell_amt, 4) if sell_amt is not None else None,
                "buy_reason": leg.get("buy_reason"),
                "sell_reason": leg.get("sell_reason"),
            })

        return {
            "buy_date": first_buy_day,
            "month_return": round(month_return, 4),
            "nav_end": nav_end,
            "daily_rows": daily_rows,
            "stocks": stocks_detail,
            "stock_count": len(stocks_detail),
        }


    def _compute_equal_weight_strategy_impl(
        self,
        top_n: int,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> Dict[str, Any]:
        """策略计算：总资金固定；升转降 T+1 开盘买；T+2 亏损开盘卖/盈利 T+3 收盘卖；无信号 T+1 清仓后暂停。"""
        from datetime import datetime

        all_backtests = self._append_live_current_month_backtest(
            self.db.get_all_broker_backtests()
        )
        if not all_backtests:
            return {"error": "No backtest data available"}

        if start_month:
            all_backtests = [bt for bt in all_backtests if bt["month"] >= start_month]
        if end_month:
            all_backtests = [bt for bt in all_backtests if bt["month"] <= end_month]
        if not all_backtests:
            return {"error": "No backtest data available for selected period"}

        nineturn_cache = self._load_nineturn_by_trade_date_cache()
        month_sell_pairs = [
            (bt["month"], bt.get("sell_date") or self._effective_month_end(bt["month"]))
            for bt in all_backtests
        ]
        month_trading_days = self._build_month_trading_days(month_sell_pairs)
        date_pools: Dict[str, List[str]] = {}
        prev_month_last_by_month: Dict[str, Optional[str]] = {}
        for bt in all_backtests:
            month = bt["month"]
            days = month_trading_days.get(month, [])
            if not days:
                continue
            pool = [sr["ts_code"] for sr in bt.get("stock_returns", [])]
            for td in days:
                date_pools.setdefault(td, []).extend(pool)
            prev_last = self._prev_month_last_trading_day(month)
            prev_month_last_by_month[month] = prev_last
            if prev_last:
                date_pools.setdefault(prev_last, []).extend(pool)
        self._prefetch_nineturn_for_dates(date_pools, nineturn_cache)

        strategy_daily: List[Dict[str, Any]] = []
        strategy_monthly: List[Dict[str, Any]] = []
        total_capital = self._STRATEGY_TOTAL_CAPITAL
        nav_start = float(total_capital)

        for bt in all_backtests:
            month = bt["month"]
            trading_days = month_trading_days.get(month, [])
            if len(trading_days) < 3:
                continue

            post_month_days = self._trading_days_after(
                trading_days[-1],
                max_count=self._STRATEGY_MONTH_END_DEFER_MAX_DAYS,
            )
            quote_days = list(trading_days) + post_month_days
            stock_books: Dict[str, Dict[str, Any]] = {}
            ohlc_by_code = self._prefetch_ohlc(
                [sr["ts_code"] for sr in bt.get("stock_returns", [])],
                trading_days[0],
                quote_days[-1],
                use_adj=False,
            )
            for sr in bt.get("stock_returns", []):
                tc = sr["ts_code"]
                synced_returns = self._sync_daily_returns_from_ohlc(
                    tc,
                    sr.get("daily_returns", []),
                    trading_days[0],
                    quote_days[-1],
                )
                synced_returns = self._enrich_daily_returns_for_trading_days(
                    synced_returns,
                    quote_days,
                    ohlc_by_code.get(tc, {}),
                )
                adj_open, adj_close, raw_open, raw_close = self._build_stock_adj_ohlc_maps(
                    tc, synced_returns,
                )
                if not adj_open or not adj_close:
                    continue
                stock_books[tc] = {
                    "name": sr.get("name", ""),
                    "adj_open": adj_open,
                    "adj_close": adj_close,
                    "raw_open": raw_open,
                    "raw_close": raw_close,
                    "forecast": sr.get("forecast") or {},
                }
            if not stock_books:
                continue

            month_result = self._simulate_month_nineturn_rotation(
                trading_days,
                stock_books,
                nineturn_cache,
                nav_start,
                total_capital=total_capital,
                post_month_trading_days=post_month_days,
                prev_month_last_day=prev_month_last_by_month.get(month),
            )
            if not month_result:
                continue

            nav_start = month_result["nav_end"]
            period_cum = (
                nav_start / total_capital - 1.0
                if total_capital > 0
                else 0.0
            )
            strategy_monthly.append({
                "month": month,
                "buy_date": month_result["buy_date"],
                "month_return": round(month_result["month_return"], 4),
                "cumulative_return": round(period_cum, 4),
                "stock_count": month_result["stock_count"],
                "stocks": month_result["stocks"],
            })
            for row in month_result["daily_rows"]:
                row_cum = (
                    row["nav"] / total_capital - 1.0
                    if total_capital > 0
                    else 0.0
                )
                point = {
                    "date": row["date"],
                    "cumulative": round(row_cum, 4),
                    "stock_count": row["stock_count"],
                }
                if strategy_daily and strategy_daily[-1]["date"] == point["date"]:
                    strategy_daily[-1] = point
                else:
                    strategy_daily.append(point)

        if not strategy_daily:
            return {"error": "No matching months for strategy"}

        for i, dr in enumerate(strategy_daily):
            if i == 0:
                dr["daily_return"] = dr["cumulative"]
            else:
                dr["daily_return"] = round(
                    dr["cumulative"] - strategy_daily[i - 1]["cumulative"], 4,
                )

        prev_cum = (
            nav_start / total_capital - 1.0
            if total_capital > 0
            else 0.0
        )
        if strategy_daily:
            strategy_daily[-1]["cumulative"] = round(prev_cum, 4)
        return {
            "strategy": "nineturn_up_to_down_open",
            "total_capital": total_capital,
            "top_n": top_n,
            "period_start_month": strategy_monthly[0]["month"],
            "period_end_month": strategy_monthly[-1]["month"],
            "start_date": strategy_daily[0]["date"],
            "end_date": strategy_daily[-1]["date"],
            "total_months": len(strategy_monthly),
            "cumulative_return": round(prev_cum, 4),
            "daily_returns": strategy_daily,
            "monthly_returns": strategy_monthly,
            "rank_stats": [],
            "up_to_down_stats": self._compute_up_to_down_trade_stats(strategy_monthly),
            "multi_curves": {},
        }

    @staticmethod
    def _rebase_cum_map_from_date(
        cum_map: Dict[str, float], anchor_date: str,
    ) -> Optional[Dict[str, float]]:
        """将月初累计收益序列重算为自 anchor_date 买入后的累计收益。"""
        if not cum_map:
            return None
        sorted_dates = sorted(cum_map.keys())
        anchor: Optional[str] = None
        for d in sorted_dates:
            if d >= anchor_date:
                anchor = d
                break
        if anchor is None:
            return None
        base = cum_map[anchor]
        denom = 1.0 + base
        if denom <= 0:
            return None
        rebased: Dict[str, float] = {}
        for d in sorted_dates:
            if d < anchor:
                continue
            rebased[d] = round((1.0 + cum_map[d]) / denom - 1.0, 4)
        return rebased or None

    @staticmethod
    def _prev_month_str(month: str) -> str:
        """返回上一个月，格式 YYYYMM。"""
        year = int(month[:4])
        mon = int(month[4:6])
        if mon == 1:
            return f"{year - 1}12"
        return f"{year}{mon - 1:02d}"

    def _prev_month_last_trading_day(self, month: str) -> Optional[str]:
        """返回上月最后一个交易日，供月初九转与跨月连续序列对比。"""
        prev_month = self._prev_month_str(month)
        return self._calendar_month_last_trading_day(prev_month)

    def _calendar_month_last_trading_day(self, month: str) -> Optional[str]:
        """返回指定自然月最后一个交易日（与数据截止日无关）。"""
        year = int(month[:4])
        mon = int(month[4:6])
        last_cal = calendar.monthrange(year, mon)[1]
        days = self._get_trading_days(f"{month}01", f"{month}{last_cal:02d}")
        return days[-1] if days else None

    def get_alltime_top_brokers(self, top_n: int = 5) -> List[str]:
        """返回有史以来累计收益前 N 的券商名称列表。

        直接基于 broker_backtest_result 表计算，无额外 API 调用。
        """
        result = self.compute_ytd_backtest(year=None, top_n=top_n)
        brokers = result.get("brokers", [])
        return [b["broker"] for b in brokers]


def _holding_final_return(daily_returns: List[Dict[str, Any]]) -> Optional[float]:
    """持仓期期末累计收益（与金股回测口径一致）。"""
    with_cum = [d for d in daily_returns if d.get("cumulative") is not None]
    if with_cum:
        return float(with_cum[-1]["cumulative"])
    prices = [d.get("price") for d in daily_returns if d.get("price")]
    if len(prices) >= 2 and prices[0] and float(prices[0]) > 0:
        return round((float(prices[-1]) - float(prices[0])) / float(prices[0]), 4)
    return None


def _holding_max_drawdown(daily_returns: List[Dict[str, Any]]) -> Optional[float]:
    """持仓期路径最大回撤（相对累计收益峰值，非正数）。"""
    values: List[float] = []
    for d in daily_returns:
        c = d.get("cumulative")
        if c is not None:
            values.append(float(c))
    if len(values) < 2:
        return None
    peak = values[0]
    max_dd = 0.0
    for v in values:
        if v > peak:
            peak = v
        dd = v - peak
        if dd < max_dd:
            max_dd = dd
    return round(max_dd, 4)

