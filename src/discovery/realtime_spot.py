# -*- coding: utf-8 -*-
"""实时行情数据提供者 (Real-time Spot Data Provider).

为盘中因子提供全市场实时行情快照，双数据源交替拉取，
缓存 30 秒（对齐 :00/:30 秒边界），确保每轮扫描获取最新数据。

数据源:
  - 腾讯 HTTP (qt.gtimg.cn) → 分批拉取，~3s，偶数槽
  - 新浪 (ak.stock_zh_a_spot) → 全量拉取，~22s，奇数槽

用法:
    provider = get_provider()
    df = provider.fetch()  # DataFrame indexed by stock code
"""

import logging
import time
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class RealtimeSpotProvider:
    """全市场实时行情提供者（双源交替，对齐 :00/:30 秒边界）。

    每次 fetch() 检查当前是否已进入新的半分钟时间槽（每 30 秒一个槽），
    若未进入新槽则返回缓存，若进入新槽则拉取新数据。偶数槽用腾讯，奇数槽用新浪。

    Attributes:
        _cache: 缓存字典 {data, slot, source}
        _last_slot: 上次拉取的半分钟槽编号
    """

    BATCH_SIZE = 800  # 腾讯单次请求最大代码数（~7200 字节 URL）
    _code_list_cache: List[str] = []
    _code_list_date: str = ""  # YYYY-MM-DD，每日刷新代码列表
    _code_list_df: Optional[pd.DataFrame] = None  # Sina 拉取代码列表时附带的全量数据

    def __init__(self):
        self._cache: Dict = {"data": None, "slot": -1, "source": ""}
        self._last_slot = -1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self) -> Optional[pd.DataFrame]:
        """获取全市场实时行情快照。

        对齐到每分钟 :00 和 :30 秒边界：同一半分钟槽内返回缓存，
        跨槽时交替使用腾讯/新浪刷新。

        每天首轮拉取优先用 Sina 获取代码列表，同时复用其行情数据，
        避免腾讯槽再重复请求。后续轮次：偶数槽腾讯分批，奇数槽新浪。
        """
        now = time.time()
        slot = int(now // 30)  # 每 30 秒一个槽，一天 2880 个

        if self._last_slot == slot and self._cache["data"] is not None:
            logger.debug(
                "[RealtimeSpot] 缓存命中 (slot=%d, source=%s)", slot, self._cache["source"]
            )
            return self._cache["data"]

        # 确保代码列表可用（每日首次触发 Sina 拉取，附带全量行情数据）
        self._get_code_list()

        # 若代码列表刷新刚拉取了 Sina 全量数据，直接复用（省一次请求）
        if self._code_list_df is not None and not self._code_list_df.empty:
            df = self._code_list_df
            self._code_list_df = None
            source_label = "sina"
            logger.info("[RealtimeSpot] 复用代码列表刷新时的 Sina 全量数据 (slot=%d)", slot)
        else:
            # 偶数槽腾讯，奇数槽新浪
            use_tx = (slot % 2 == 0)
            source_label = "tencent" if use_tx else "sina"

            if use_tx:
                df = self._fetch_tencent()
                if df is None or df.empty:
                    logger.info("[RealtimeSpot] 腾讯失败，尝试新浪回退")
                    df = self._fetch_sina()
                    if df is not None and not df.empty:
                        source_label = "sina"
            else:
                df = self._fetch_sina()
                if df is None or df.empty:
                    logger.info("[RealtimeSpot] 新浪失败，尝试腾讯回退")
                    df = self._fetch_tencent()
                    if df is not None and not df.empty:
                        source_label = "tencent"

        # 两个都失败，返回过期缓存
        if df is None or df.empty:
            if self._cache["data"] is not None:
                logger.warning("[RealtimeSpot] 两个接口均失败，返回过期缓存 (slot=%d)", self._last_slot)
                return self._cache["data"]
            logger.warning("[RealtimeSpot] 无可用数据")
            return None

        # 标准化并缓存
        df = self._normalize(df, source_label)
        self._cache["data"] = df
        self._cache["slot"] = slot
        self._cache["source"] = source_label
        self._last_slot = slot
        logger.info("[RealtimeSpot] 刷新成功: %s (slot=%d), %d 只股票", source_label, slot, len(df))
        return df

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    @classmethod
    def _fetch_tencent(cls) -> Optional[pd.DataFrame]:
        """腾讯 HTTP 全市场实时行情 (qt.gtimg.cn)，分批拉取。"""
        try:
            codes = cls._get_code_list()
            if not codes:
                logger.warning("[RealtimeSpot] 腾讯无可用代码列表")
                return None

            # 转换为腾讯格式: 60*/68* → sh, 00*/30* → sz, 4*/8*/92* → bj
            tx_codes = cls._to_tencent_codes(codes)

            session = requests.Session()
            all_rows = []
            api_start = time.time()

            for i in range(0, len(tx_codes), cls.BATCH_SIZE):
                batch = tx_codes[i : i + cls.BATCH_SIZE]
                url = f"http://qt.gtimg.cn/q={','.join(batch)}"
                r = session.get(url, timeout=30)
                r.encoding = "gbk"

                for line in r.text.strip().split("\n"):
                    line = line.strip()
                    if not line or '"' not in line:
                        continue
                    content = line.split('"')[1]
                    parts = content.split("~")
                    if len(parts) < 40:
                        continue
                    all_rows.append({
                        "code": parts[2],
                        "name": parts[1],
                        "price": parts[3],
                        "pct_chg": parts[32],
                        "pre_close": parts[4],
                        "high": float(parts[33]) if parts[33] else 0,
                        "low": float(parts[34]) if parts[34] else 0,
                        # 腾讯成交量单位「手」，成交额单位「万元」
                        "volume": float(parts[6]) * 100 if parts[6] else 0,
                        "amount": float(parts[37]) * 10000 if parts[37] else 0,
                    })

            elapsed = time.time() - api_start
            if all_rows:
                df = pd.DataFrame(all_rows)
                logger.info("[RealtimeSpot] 腾讯返回 %d 只股票, %d 批, 耗时 %.1fs",
                            len(df), (len(tx_codes) + cls.BATCH_SIZE - 1) // cls.BATCH_SIZE, elapsed)
                return df
            return None
        except Exception as e:
            logger.warning("[RealtimeSpot] 腾讯接口异常: %s", e)
            return None

    @staticmethod
    def _fetch_sina() -> Optional[pd.DataFrame]:
        """新浪全市场行情 (ak.stock_zh_a_spot)。"""
        try:
            import akshare as ak
            logger.debug("[RealtimeSpot] 使用新浪接口获取实时行情...")
            api_start = time.time()
            df = ak.stock_zh_a_spot()
            elapsed = time.time() - api_start
            if df is not None and not df.empty:
                logger.info("[RealtimeSpot] 新浪返回 %d 只股票, 耗时 %.1fs", len(df), elapsed)
            return df
        except Exception as e:
            logger.warning("[RealtimeSpot] 新浪接口异常: %s", e)
            return None

    # ------------------------------------------------------------------
    # Code list helpers
    # ------------------------------------------------------------------

    @classmethod
    def _get_code_list(cls) -> List[str]:
        """获取全市场 A 股代码列表，每日刷新一次。

        优先从 Sina 拉取（同时拿到全量实时数据），回退腾讯自身。
        """
        today = date.today().isoformat()
        if cls._code_list_cache and cls._code_list_date == today:
            return cls._code_list_cache

        codes, df = cls._get_code_list_from_sina()
        if codes:
            cls._code_list_cache = codes
            cls._code_list_date = today
            logger.info("[RealtimeSpot] 代码列表刷新: %d 只", len(codes))
            # 暂存 Sina 全量数据，避免腾讯槽重复拉取
            if df is not None and not df.empty:
                cls._code_list_df = df
        return codes

    @staticmethod
    def _get_code_list_from_sina() -> tuple:
        """从新浪 akshare 获取全市场代码列表及行情数据。
        Returns: (codes_list, raw_dataframe)
        """
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot()
            codes = df["代码"].str.replace(r"^(sz|sh|bj)", "", regex=True).tolist()
            return [str(c).strip().zfill(6) for c in codes], df
        except Exception as e:
            logger.warning("[RealtimeSpot] 新浪代码列表获取失败: %s", e)
            return [], None

    @staticmethod
    def _to_tencent_codes(codes: List[str]) -> List[str]:
        """将裸代码转为腾讯格式 (sh/sz/bj 前缀)。"""
        result = []
        for c in codes:
            c_str = str(c).strip().zfill(6)
            if c_str.startswith(("60", "68")):
                result.append(f"sh{c_str}")
            elif c_str.startswith(("00", "30")):
                result.append(f"sz{c_str}")
            elif c_str.startswith(("4", "8", "92")):
                result.append(f"bj{c_str}")
        return result

    # ------------------------------------------------------------------
    # Column normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
        """标准化不同来源的列名，返回统一 DataFrame。

        兼容东方财富和新浪的列名差异，参照 akshare_fetcher._calc_market_stats 的匹配模式。
        """
        df = df.copy()

        code_col = next(
            (c for c in ["代码", "股票代码", "ts_code", "stock_code", "code"] if c in df.columns), None
        )
        name_col = next(
            (c for c in ["名称", "股票名称", "name"] if c in df.columns), None
        )
        price_col = next(
            (c for c in ["最新价", "close", "lastPrice", "price"] if c in df.columns), None
        )
        pct_col = next(
            (c for c in ["涨跌幅", "pct_chg", "change_pct"] if c in df.columns), None
        )
        preclose_col = next(
            (c for c in ["昨收", "昨日收盘", "pre_close", "lastClose"] if c in df.columns), None
        )
        high_col = next(
            (c for c in ["最高", "最高价", "high"] if c in df.columns), None
        )
        low_col = next(
            (c for c in ["最低", "最低价", "low"] if c in df.columns), None
        )
        volume_col = next(
            (c for c in ["成交量", "volume"] if c in df.columns), None
        )
        amount_col = next(
            (c for c in ["成交额", "amount"] if c in df.columns), None
        )

        if code_col is None or price_col is None:
            logger.warning("[RealtimeSpot] 无法识别列名: %s", list(df.columns)[:10])
            return pd.DataFrame()

        result = pd.DataFrame()
        raw_codes = df[code_col].astype(str).str.strip()
        # 剥离交易所前缀 (sz301666 → 301666, sh600519 → 600519)
        result["code"] = raw_codes.str.replace(r"^(sz|sh|bj|SZ|SH|BJ)", "", regex=True)
        result["name"] = df[name_col].astype(str).str.strip() if name_col else ""
        result["price"] = pd.to_numeric(df[price_col], errors="coerce")
        result["pct_chg"] = pd.to_numeric(df[pct_col], errors="coerce") if pct_col else pd.NA
        result["pre_close"] = (
            pd.to_numeric(df[preclose_col], errors="coerce") if preclose_col else pd.NA
        )
        result["high"] = pd.to_numeric(df[high_col], errors="coerce") if high_col else pd.NA
        result["low"] = pd.to_numeric(df[low_col], errors="coerce") if low_col else pd.NA
        result["volume"] = pd.to_numeric(df[volume_col], errors="coerce") if volume_col else pd.NA
        result["amount"] = pd.to_numeric(df[amount_col], errors="coerce") if amount_col else pd.NA
        result["source"] = source

        # 过滤停牌/无效数据
        result = result.dropna(subset=["price"])
        result = result[result["price"] > 0]

        result = result.set_index("code")
        return result


# ------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------

_provider: Optional[RealtimeSpotProvider] = None


def get_provider() -> RealtimeSpotProvider:
    """获取 RealtimeSpotProvider 单例。"""
    global _provider
    if _provider is None:
        _provider = RealtimeSpotProvider()
    return _provider
