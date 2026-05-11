# -*- coding: utf-8 -*-
"""人气因子 (Popularity Factor).

盘中+盘后因子：基于东方财富人气排行 + Tushare dc_hot 降级，
识别市场关注度高的股票。

4 个子信号：
- 飙升幅度 (0-45)：rank_change 在改善股中的百分位
- 排名强度 (0-35)：当前排名逆线性映射
- 涨跌幅 (0-20)：pct_chg 分段线性
- 排名趋势 (0-15)：5 日排名改善百分位（需 DB 历史数据）
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _linear_map(series: pd.Series, x0: float, y0: float,
                x1: float, y1: float, clip_low: float = 0.0,
                clip_high: float = 1e9) -> pd.Series:
    """两点线性映射，超出范围 clip。"""
    slope = (y1 - y0) / (x1 - x0) if x1 != x0 else 0.0
    return (y0 + slope * (series - x0)).clip(clip_low, clip_high)


class PopularityFactor(BaseFactor):
    """人气因子。

    基于东方财富人气排行榜「飙升榜」。
    关键信号：排名靠前 + 排名在上升（较昨日改善）。
    盘中优先东财，降级使用 Tushare dc_hot。
    """

    name = "popularity"
    available_intraday = True
    available_postmarket = True
    weight = 15.0

    _LABEL_THRESHOLD = 5.0
    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取人气排行数据。

        优先级：东财 API > Tushare dc_hot > DB 缓存。
        """
        tushare_fetcher = kwargs.get("tushare_fetcher")

        df = None
        # ── 1. 东财 API（盘中主路径） ──
        df = self._fetch_eastmoney()
        if df is not None and not df.empty:
            self._trade_date = trade_date
            return df

        # ── 2. Tushare dc_hot 降级 ──
        if tushare_fetcher:
            df = self._fetch_tushare(tushare_fetcher, trade_date)
            if df is not None and not df.empty:
                self._trade_date = trade_date
                return df

        # ── 3. DB 缓存 ──
        df = self._fetch_from_db(trade_date)
        if df is not None and not df.empty:
            df = df[~df.index.duplicated(keep='first')]
            self._trade_date = trade_date
            return df

        return None

    # ------------------------------------------------------------------
    # fetch helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_eastmoney() -> Optional[pd.DataFrame]:
        """直连东财 API（走代理），两步：排名 → 行情数据。"""
        import os
        import requests

        host = os.getenv("PROXY_HOST", "127.0.0.1")
        port = os.getenv("PROXY_PORT", "42484")
        proxy_url = f"http://{host}:{port}"

        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": proxy_url, "https": proxy_url}
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        })

        try:
            logger.info("[PopularityFactor] 拉取东财人气飙升榜...")
            r1 = session.post(
                "https://emappdata.eastmoney.com/stockrank/getAllHisRcList",
                json={
                    "appId": "appId01",
                    "globalId": "786e4c21-70dc-435a-93bb-38",
                    "marketType": "",
                    "pageNo": 1,
                    "pageSize": 100,
                },
                timeout=15,
            )
            r1.raise_for_status()
            rank_data = r1.json().get("data", [])
            if not rank_data:
                logger.warning("[PopularityFactor] 人气排行返回空数据")
                return None

            marks = []
            for item in rank_data:
                sc = item["sc"]
                code = sc[2:].zfill(6) if len(sc) >= 3 else str(sc).zfill(6)
                if "BJ" in sc or "SZ" in sc:
                    marks.append("0." + code)
                else:
                    marks.append("1." + code)
            secids = ",".join(marks) + "?v=08926209912590994"
            r2 = session.get(
                "https://push2.eastmoney.com/api/qt/ulist.np/get",
                params={
                    "ut": "f057cbcbce2a86e2866ab8877db1d059",
                    "fltt": "2", "invt": "2",
                    "fields": "f14,f3,f12,f2",
                    "secids": secids,
                },
                timeout=15,
            )
            r2.raise_for_status()
            price_data = {item["f12"]: item for item in r2.json()["data"]["diff"]}

            rows = []
            for item in rank_data:
                try:
                    sc = item["sc"]
                    bare = sc[2:].zfill(6) if len(sc) >= 3 else str(sc).zfill(6)
                    pinfo = price_data.get(bare, {})
                    rows.append({
                        "ts_code": bare,
                        "name": pinfo.get("f14", ""),
                        "price": pd.to_numeric(pinfo.get("f2", 0), errors="coerce") or 0,
                        "pct_chg": pd.to_numeric(pinfo.get("f3", 0), errors="coerce") or 0,
                        "rank": int(item["rk"]),
                        "rank_change": int(item.get("hrc", 0) or 0),
                    })
                except (KeyError, ValueError, TypeError):
                    pass

            df = pd.DataFrame(rows)
            df = df.set_index("ts_code")

            logger.info("[PopularityFactor] 获取 %d 只股票人气数据", len(df))
            return df

        except Exception as e:
            logger.warning("[PopularityFactor] 获取东财人气数据失败: %s", e)
            return None

    @staticmethod
    def _fetch_tushare(tushare_fetcher, trade_date: str) -> Optional[pd.DataFrame]:
        """Tushare dc_hot 降级路径。"""
        try:
            df = tushare_fetcher.get_dc_hot(trade_date)
            if df is None or df.empty:
                return None

            out = pd.DataFrame()
            out["name"] = df.get("name", "")
            out["pct_chg"] = pd.to_numeric(df.get("pct_change", 0), errors="coerce").fillna(0)
            out["rank"] = pd.to_numeric(df.get("rank", 9999), errors="coerce").fillna(9999).astype(int)
            out["rank_change"] = 0

            codes = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out.index = codes
            out.index.name = "ts_code"

            logger.info("[PopularityFactor] Tushare dc_hot 降级: %d 条", len(out))
            return out
        except Exception as e:
            logger.warning("[PopularityFactor] Tushare dc_hot 降级失败: %s", e)
            return None

    @staticmethod
    def _fetch_from_db(trade_date: str) -> Optional[pd.DataFrame]:
        """从 popularity_rank 表读取当日人气数据。"""
        try:
            from src.storage import DatabaseManager

            db = DatabaseManager()
            df = db.get_popularity_rank_range(start_date=trade_date, end_date=trade_date)
            if df is None or df.empty:
                return None

            out = pd.DataFrame(index=df.index)
            out["name"] = df.get("name", "")
            out["pct_chg"] = pd.to_numeric(df.get("pct_change", 0), errors="coerce").fillna(0)
            out["rank"] = pd.to_numeric(df.get("rank", 9999), errors="coerce").fillna(9999).astype(int)
            out["rank_change"] = 0

            logger.info("[PopularityFactor] DB 读取: %d 条", len(out))
            return out
        except Exception as e:
            logger.warning("[PopularityFactor] DB 读取失败: %s", e)
            return None

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 4 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        rank = df.get("rank", pd.Series(9999, index=idx))
        rank_change = df.get("rank_change", zeros)
        pct_chg = df.get("pct_chg", zeros)

        signals: Dict[str, pd.Series] = {}

        # --- 1. 飙升幅度 (0-45): rank_change 在改善股中的百分位 ---
        s_surge = zeros.copy()
        improvers = rank_change > 0
        if improvers.any():
            surge_pct = rank_change[improvers].rank(pct=True)
            s_surge.loc[improvers] = (surge_pct * 45).clip(0, 45)
        signals["surge"] = s_surge

        # --- 2. 排名强度 (0-35): 逆排名线性衰减 ---
        max_rank = rank.max()
        if max_rank > 1:
            s_rank = (35 * (1 - (rank - 1) / (max_rank - 1))).clip(0, 35).fillna(0)
        else:
            s_rank = pd.Series(35.0, index=idx)
        signals["rank"] = s_rank

        # --- 3. 涨跌幅 (0-20): 分段线性 ---
        s_pct = _linear_map(pct_chg.fillna(0), -5, 0, 10, 20, 0, 20)
        signals["pct_chg"] = s_pct

        # --- 4. 排名趋势 (0-15): 5 日排名改善百分位 ---
        s_trend = self._compute_rank_trend(df)
        signals["rank_trend"] = s_trend

        return signals

    def _compute_rank_trend(self, df: pd.DataFrame) -> pd.Series:
        """从 DB 拉取 5 日历史排名，计算排名改善幅度百分位。

        改善 = (5日均排名 - 当日排名)，正值越大越好。
        """
        zeros = pd.Series(0.0, index=df.index)
        td = getattr(self, "_trade_date", "")
        if not td:
            return zeros

        try:
            from datetime import datetime as dt, timedelta

            from src.storage import DatabaseManager

            target = dt.strptime(str(td).replace("-", "")[:8], "%Y%m%d")
            start = (target - timedelta(days=10)).strftime("%Y%m%d")
            end = target.strftime("%Y%m%d")

            db = DatabaseManager()
            codes = [str(c).zfill(6) for c in df.index]
            hist = db.get_popularity_rank_range(
                codes=codes, start_date=start, end_date=end,
            )
            if hist is None or hist.empty:
                return zeros

            hist = hist.reset_index()
            hist["trade_date"] = hist["trade_date"].astype(str)

            today_rank = hist[hist["trade_date"] == end].set_index("code")["rank"]
            avg_rank = hist.groupby("code")["rank"].mean()

            improvements = {}
            for code in df.index:
                c = str(code).zfill(6)
                t_r = today_rank.get(c)
                a_r = avg_rank.get(c)
                if pd.notna(t_r) and pd.notna(a_r) and a_r > 0:
                    improvements[c] = (a_r - t_r) / a_r * 100
                else:
                    improvements[c] = 0.0

            imp_series = pd.Series(improvements, index=df.index).fillna(0)
            pos = imp_series > 0
            if pos.any():
                pct = imp_series[pos].rank(pct=True)
                imp_series.loc[pos] = (pct * 15).clip(0, 15)
            imp_series.loc[~pos] = 0
            return imp_series
        except Exception as e:
            logger.debug("[PopularityFactor] 排名趋势计算失败: %s", e)
            return zeros

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        df = df[~df.index.duplicated(keep='first')]
        signals = self._compute_signals(df)
        total = sum(signals.values()).clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        df = df[~df.index.duplicated(keep='first')]
        scores = scores[~scores.index.duplicated(keep='first')]

        rank = df.get("rank", pd.Series(9999, index=df.index))
        rank_change = df.get("rank_change", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0.0, index=df.index))

        signals = self._compute_signals(df)

        signal_meta = [
            ("surge", "飙升幅度", 45),
            ("rank", "排名强度", 35),
            ("pct_chg", "涨跌幅", 20),
            ("rank_trend", "排名趋势", 15),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue

            labels: List[str] = []

            for key, label, max_val in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_val * threshold:
                    continue
                if key == "surge":
                    rc = int(rank_change.get(ts_code, 0))
                    labels.append(f"人气飙升(+{rc}位)")
                elif key == "rank":
                    rk = int(rank.get(ts_code, 9999))
                    labels.append(f"人气核心圈(排名{rk})")
                elif key == "pct_chg":
                    pct = float(pct_chg.get(ts_code, 0))
                    direction = "上涨" if pct >= 0 else "下跌"
                    labels.append(f"人气股{direction}({pct:+.1f}%)")
                elif key == "rank_trend":
                    labels.append("人气排名持续改善")

            if labels:
                reasons[ts_code] = labels

        return reasons
