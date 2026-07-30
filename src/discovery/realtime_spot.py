# -*- coding: utf-8 -*-
"""实时行情数据提供者 (Real-time Spot Data Provider).

为盘中因子提供全市场实时行情快照，缓存 30 秒（对齐 :00/:30 秒边界）。

数据源:
  - 主力: 腾讯 HTTP (qt.gtimg.cn) → 全市场价量/名称/涨跌幅，~1.8s
  - 兜底: 新浪 HTTP (hq.sinajs.cn) → 全市场价量/名称，~2.7s
  - 补充: 东财 push2 → 换手率/量比独立落库，60s 间隔，不阻塞行情刷新

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
    """全市场实时行情提供者（腾讯主力 + 新浪兜底 + 东财补充换手率/量比）。

    每次 fetch() 检查当前 30s slot，同一 slot 内返回缓存，
    新 slot 时拉取腾讯，失败则降级新浪，最后用东财补充换手率/量比。
    """

    BATCH_SIZE = 800  # 腾讯/新浪单次请求最大代码数（~7200 字节 URL）
    _code_list_cache: List[str] = []
    _code_list_date: str = ""
    _em_supplement_ts: float = 0  # 东财补充上次更新时间

    def __init__(self):
        self._cache: Dict = {"data": None, "slot": -1, "source": ""}
        self._last_slot = -1

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self) -> Optional[pd.DataFrame]:
        """获取全市场实时行情快照。

        对齐到 30s slot 边界缓存。
        主力: 腾讯 HTTP → 兜底: 新浪 HTTP → 补充: 东财换手率/量比（60s 间隔）。
        """
        now = time.time()
        slot = int(now // 30)

        if self._last_slot == slot and self._cache["data"] is not None:
            logger.debug(
                "[RealtimeSpot] 缓存命中 (slot=%d, source=%s)", slot, self._cache["source"]
            )
            return self._cache["data"]

        self._get_code_list()

        # 主力: 腾讯（全市场 5515 只，1.8s）
        df = self._fetch_tencent()
        source_label = "tencent"

        if df is None or df.empty:
            logger.info("[RealtimeSpot] 腾讯失败，回退新浪")
            df = self._fetch_sina()
            source_label = "sina" if df is not None and not df.empty else "tencent_fallback"

        # 两个都失败，返回过期缓存
        if df is None or df.empty:
            if self._cache["data"] is not None:
                logger.warning("[RealtimeSpot] 两个接口均失败，返回过期缓存 (slot=%d)", self._last_slot)
                return self._cache["data"]
            logger.warning("[RealtimeSpot] 无可用数据")
            return None

        # 标准化
        df = self._normalize(df, source_label)

        # 东财补充: 换手率/量比，60s 更新一次，不阻塞行情
        if now - RealtimeSpotProvider._em_supplement_ts >= 60:
            self._supplement_eastmoney(df)

        # 缓存
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
    def _fetch_tencent(cls, codes: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """腾讯 HTTP 实时行情 (qt.gtimg.cn)，分批拉取。"""
        try:
            if codes is None:
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
                        "open": float(parts[5]) if parts[5] else 0,
                        "high": float(parts[33]) if parts[33] else 0,
                        "low": float(parts[34]) if parts[34] else 0,
                        # 腾讯成交量单位「手」，成交额单位「万元」
                        "volume": float(parts[6]) * 100 if parts[6] else 0,
                        "amount": float(parts[37]) * 10000 if parts[37] else 0,
                        "turnover_rate": float(parts[38]) if parts[38] else pd.NA,
                        "volume_ratio": float(parts[49]) if parts[49] else pd.NA,
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

    # 东财多级降级地址列表: (label, url, use_proxy)
    _EM_ENDPOINTS = [
        ("push2delay", "https://push2delay.eastmoney.com/api/qt/clist/get", False),
        ("push2delay-proxy", "https://push2delay.eastmoney.com/api/qt/clist/get", True),
        ("82.push2-proxy", "https://82.push2.eastmoney.com/api/qt/clist/get", True),
    ]

    @classmethod
    def _fetch_eastmoney(cls, max_pages: int = 60) -> Optional[pd.DataFrame]:
        """东财 push2 全市场实时行情，多级降级拉取。

        分页拉取（每页 100 只，API 硬限制），全量 59 页 ~5850 只，约 11s。
        含换手率 (f8) 和量比 (f10)。

        降级顺序: push2delay 直连 → push2delay 走代理 → 82.push2 走代理
        """
        import os

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        }
        proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
        proxy_port = os.getenv("PROXY_PORT", "42484")
        proxy_url = f"http://{proxy_host}:{proxy_port}"

        base_params = {
            "pn": "1", "pz": "100", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2", "invt": "2", "fid": "f12",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "fields": "f2,f3,f5,f6,f8,f10,f12,f14,f15,f16,f17,f18",
        }

        for label, api_url, use_proxy in cls._EM_ENDPOINTS:
            try:
                session = requests.Session()
                session.headers.update(headers)
                session.trust_env = False
                if use_proxy:
                    session.proxies = {"http": proxy_url, "https": proxy_url}

                logger.debug("[RealtimeSpot] 东财 push2 尝试: %s (proxy=%s)", label, use_proxy)

                # 先查第 1 页拿 total
                r = session.get(api_url, params=base_params, timeout=15)
                r.raise_for_status()
                data = r.json()
                if data.get("rc") != 0 or data.get("data") is None:
                    logger.warning("[RealtimeSpot] 东财 %s 返回异常: rc=%s", label, data.get("rc"))
                    continue
                total = data["data"].get("total", 0)
                actual_pages = min(max_pages, (total + 99) // 100)

                api_start = time.time()
                all_items = data["data"].get("diff", [])

                for page in range(2, actual_pages + 1):
                    params = {**base_params, "pn": str(page)}
                    r = session.get(api_url, params=params, timeout=15)
                    r.close()
                    page_data = r.json()
                    items = page_data["data"].get("diff", [])
                    if not items:
                        break
                    all_items.extend(items)
                    time.sleep(0.15)

                df = pd.DataFrame(all_items)
                elapsed = time.time() - api_start
                logger.info("[RealtimeSpot] 东财 %s 返回 %d 只 (%d 页), 耗时 %.1fs",
                           label, len(df), actual_pages, elapsed)
                return df
            except Exception as e:
                logger.warning("[RealtimeSpot] 东财 %s 失败: %s", label, e)
                continue

        logger.warning("[RealtimeSpot] 东财所有端点均失败")
        return None

    @classmethod
    def _fetch_sina(cls, codes: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """新浪 HTTP 实时行情 (hq.sinajs.cn)，分批拉取。

        新浪不提供换手率和量比，pct_chg 由 price/pre_close 计算。
        """
        try:
            if codes is None:
                codes = cls._get_code_list()
            if not codes:
                logger.warning("[RealtimeSpot] 新浪无可用代码列表")
                return None

            # 复用腾讯格式 (sh/sz/bj 前缀)
            sina_codes = cls._to_tencent_codes(codes)

            session = requests.Session()
            session.headers.update({
                "Referer": "https://finance.sina.com.cn",
            })
            all_rows = []
            api_start = time.time()

            for i in range(0, len(sina_codes), cls.BATCH_SIZE):
                batch = sina_codes[i : i + cls.BATCH_SIZE]
                url = f"http://hq.sinajs.cn/list={','.join(batch)}"
                r = session.get(url, timeout=30)
                r.encoding = "gbk"

                for line in r.text.strip().split("\n"):
                    line = line.strip()
                    if not line or '"' not in line:
                        continue
                    # var hq_str_sh600519="content";
                    # 从行前缀提取代码: "hq_str_sh600519=" → "600519"
                    prefix = line.split('"')[0]
                    raw_code = prefix.split("_")[-1] if "_" in prefix else ""
                    raw_code = raw_code.rstrip("=")
                    if len(raw_code) < 6:
                        continue
                    content = line.split('"')[1]
                    parts = content.split(",")
                    if len(parts) < 30:
                        continue
                    try:
                        price = float(parts[3]) if parts[3] else 0.0
                        pre_close = float(parts[2]) if parts[2] else 0.0
                        pct_chg = round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0.0
                        # 新浪成交量「股」，成交额「元」
                        all_rows.append({
                            "code": raw_code,
                            "name": parts[0],
                            "price": price,
                            "pct_chg": pct_chg,
                            "pre_close": pre_close,
                            "open": float(parts[1]) if parts[1] else 0.0,
                            "high": float(parts[4]) if parts[4] else 0.0,
                            "low": float(parts[5]) if parts[5] else 0.0,
                            "volume": float(parts[8]) if parts[8] else 0.0,
                            "amount": float(parts[9]) if parts[9] else 0.0,
                            "turnover_rate": pd.NA,
                            "volume_ratio": pd.NA,
                        })
                    except (ValueError, IndexError):
                        continue

            elapsed = time.time() - api_start
            if all_rows:
                df = pd.DataFrame(all_rows)
                logger.info("[RealtimeSpot] 新浪返回 %d 只股票, %d 批, 耗时 %.1fs",
                            len(df), (len(sina_codes) + cls.BATCH_SIZE - 1) // cls.BATCH_SIZE, elapsed)
                return df
            return None
        except Exception as e:
            logger.warning("[RealtimeSpot] 新浪接口异常: %s", e)
            return None

    @classmethod
    def fetch_codes(cls, codes: List[str]) -> Optional[pd.DataFrame]:
        """按代码批量拉取最新行情，不读取或写入全市场槽位缓存。"""
        requested = sorted({
            str(code).split(".")[0].strip().zfill(6)
            for code in codes
            if code is not None and str(code).strip()
        })
        if not requested:
            return None

        df = cls._fetch_tencent(requested)
        source_label = "tencent"
        if df is None or df.empty:
            df = cls._fetch_sina(requested)
            source_label = "sina"
        if df is None or df.empty:
            return None
        return cls._normalize(df, source_label)

    # ------------------------------------------------------------------
    # 东财补充：换手率/量比（60s 间隔独立更新）
    # ------------------------------------------------------------------

    def _supplement_eastmoney(self, df: pd.DataFrame) -> None:
        """从东财 push2 补充 turnover_rate/volume_ratio，60s 更新一次。

        直接修改传入的 DataFrame（已由 _normalize 标准化，code 为 index）。
        """
        em_df = self._fetch_eastmoney(max_pages=60)
        if em_df is None or em_df.empty:
            return

        code_col = next(
            (c for c in ["f12", "代码", "code"] if c in em_df.columns), None
        )
        turnover_col = next(
            (c for c in ["f8", "换手率", "turnover_rate"] if c in em_df.columns), None
        )
        vol_ratio_col = next(
            (c for c in ["f10", "量比", "volume_ratio"] if c in em_df.columns), None
        )
        if code_col is None:
            return

        # 标准化东财代码（去掉交易所后缀，匹配 _normalize 后的 index）
        em_codes = em_df[code_col].astype(str).str.strip()
        em_codes = em_codes.str.replace(r"\.(SZ|SH|BJ)$", "", regex=True)
        em_codes = em_codes.str.replace(r"^(sz|sh|bj)", "", regex=True)
        em_codes_set = set(em_codes.values)

        if turnover_col:
            tr_map = pd.Series(
                pd.to_numeric(em_df[turnover_col], errors="coerce").values,
                index=em_codes,
            )
            mask = df.index.isin(em_codes_set)
            df.loc[mask, "turnover_rate"] = df.index[mask].map(tr_map)
        if vol_ratio_col:
            vr_map = pd.Series(
                pd.to_numeric(em_df[vol_ratio_col], errors="coerce").values,
                index=em_codes,
            )
            mask = df.index.isin(em_codes_set)
            df.loc[mask, "volume_ratio"] = df.index[mask].map(vr_map)

        RealtimeSpotProvider._em_supplement_ts = time.time()
        filled = df["turnover_rate"].notna().sum()
        logger.info(
            "[RealtimeSpot] 东财补充: turnover_rate=%d, volume_ratio=%d",
            filled, df["volume_ratio"].notna().sum(),
        )

    # ------------------------------------------------------------------

    @classmethod
    def _get_code_list(cls) -> List[str]:
        """获取全市场 A 股代码列表，每日刷新一次。

        从 DB 获取（stock_daily + realtime_spot 合并去重），
        东财 push2 的 pz 硬限制为 100，不能用于代码列表。
        """
        today = date.today().isoformat()
        if cls._code_list_cache and cls._code_list_date == today:
            return cls._code_list_cache

        codes = cls._get_code_list_from_db()
        if codes:
            cls._code_list_cache = codes
            cls._code_list_date = today
            logger.info("[RealtimeSpot] 代码列表刷新: %d 只 (DB)", len(codes))
            return codes
        return codes

    @staticmethod
    def _get_code_list_from_db() -> List[str]:
        """从本地 DB 获取代码列表（stock_daily + realtime_spot 合并去重），
        作为东财不可用时的兜底。"""
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            with db.get_session() as s:
                from sqlalchemy import text
                rows = s.execute(
                    text("SELECT DISTINCT code FROM stock_daily ORDER BY code")
                ).fetchall()
                codes = {r[0] for r in rows}
                # 补充 realtime_spot 中可能有但 stock_daily 尚未收录的新股
                rows2 = s.execute(
                    text("SELECT DISTINCT code FROM realtime_spot")
                ).fetchall()
                codes.update(r[0] for r in rows2)
                return sorted(codes)
        except Exception as e:
            logger.warning("[RealtimeSpot] DB 代码列表获取失败: %s", e)
            return []

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
            elif c_str.startswith(("43", "83", "87", "92")):
                result.append(f"bj{c_str}")
        return result

    # ------------------------------------------------------------------
    # Column normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
        """标准化不同来源的列名，返回统一 DataFrame。

        兼容东财 push2 字段码 (f2/f3/...) 和腾讯中文/英文列名。
        """
        df = df.copy()

        code_col = next(
            (c for c in ["f12", "代码", "股票代码", "ts_code", "stock_code", "code"] if c in df.columns), None
        )
        name_col = next(
            (c for c in ["f14", "名称", "股票名称", "name"] if c in df.columns), None
        )
        price_col = next(
            (c for c in ["f2", "最新价", "close", "lastPrice", "price"] if c in df.columns), None
        )
        pct_col = next(
            (c for c in ["f3", "涨跌幅", "pct_chg", "change_pct"] if c in df.columns), None
        )
        preclose_col = next(
            (c for c in ["f18", "昨收", "昨日收盘", "pre_close", "lastClose"] if c in df.columns), None
        )
        open_col = next(
            (c for c in ["f17", "今开", "开盘价", "open"] if c in df.columns), None
        )
        high_col = next(
            (c for c in ["f15", "最高", "最高价", "high"] if c in df.columns), None
        )
        low_col = next(
            (c for c in ["f16", "最低", "最低价", "low"] if c in df.columns), None
        )
        volume_col = next(
            (c for c in ["f5", "成交量", "volume"] if c in df.columns), None
        )
        amount_col = next(
            (c for c in ["f6", "成交额", "amount"] if c in df.columns), None
        )
        turnover_col = next(
            (c for c in ["f8", "换手率", "turnover_rate"] if c in df.columns), None
        )
        vol_ratio_col = next(
            (c for c in ["f10", "量比", "volume_ratio"] if c in df.columns), None
        )

        if code_col is None or price_col is None:
            logger.warning("[RealtimeSpot] 无法识别列名: %s", list(df.columns)[:10])
            return pd.DataFrame()

        result = pd.DataFrame()
        raw_codes = df[code_col].astype(str).str.strip()
        # 剥离交易所后缀 (sz301666 → 301666, sh600519 → 600519)
        result["code"] = raw_codes.str.replace(r"^(sz|sh|bj|SZ|SH|BJ)", "", regex=True)
        result["name"] = df[name_col].astype(str).str.strip() if name_col else ""
        result["price"] = pd.to_numeric(df[price_col], errors="coerce")
        result["pct_chg"] = pd.to_numeric(df[pct_col], errors="coerce") if pct_col else pd.NA
        result["pre_close"] = (
            pd.to_numeric(df[preclose_col], errors="coerce") if preclose_col else pd.NA
        )
        result["open_price"] = pd.to_numeric(df[open_col], errors="coerce") if open_col else pd.NA
        result["high"] = pd.to_numeric(df[high_col], errors="coerce") if high_col else pd.NA
        result["low"] = pd.to_numeric(df[low_col], errors="coerce") if low_col else pd.NA
        result["volume"] = pd.to_numeric(df[volume_col], errors="coerce") if volume_col else pd.NA
        result["amount"] = pd.to_numeric(df[amount_col], errors="coerce") if amount_col else pd.NA
        result["turnover_rate"] = pd.to_numeric(df[turnover_col], errors="coerce") if turnover_col else pd.NA
        result["volume_ratio"] = pd.to_numeric(df[vol_ratio_col], errors="coerce") if vol_ratio_col else pd.NA
        result["trade_date"] = date.today().isoformat()
        result["source"] = source

        # 用 price / pre_close 重新计算 pct_chg，不信任 API 原始字段
        # （Tencent/Sina 在涨停无成交时可能返回 0.00，导致展示及涨停扣分失效）
        has_price = result["price"].notna()
        has_preclose = result["pre_close"].notna() & (result["pre_close"] > 0)
        mask = has_price & has_preclose
        result.loc[mask, "pct_chg"] = (
            (result.loc[mask, "price"] - result.loc[mask, "pre_close"])
            / result.loc[mask, "pre_close"] * 100
        ).round(2)

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
