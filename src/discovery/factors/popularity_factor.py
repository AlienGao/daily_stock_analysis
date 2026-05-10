# -*- coding: utf-8 -*-
"""人气因子 (Popularity Factor).

盘中+盘后因子：基于东方财富人气排行，识别市场关注度高的股票。
数据来源: 直连东财 emappdata + push2 API（过代理）

3 个子信号：
- 飙升幅度 (0-45)：rank_change 在改善股中的百分位
- 排名强度 (0-35)：当前排名逆线性映射
- 涨跌幅 (0-20)：pct_chg 分段线性
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
    """

    name = "popularity"
    available_intraday = True
    available_postmarket = True
    weight = 15.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
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

            marks = [
                ("0." + item["sc"][2:] if "SZ" in item["sc"] else "1." + item["sc"][2:])
                for item in rank_data
            ]
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
                sc = item["sc"]
                bare = sc[2:] if len(sc) >= 3 else sc
                pinfo = price_data.get(bare, {})
                rows.append({
                    "ts_code": bare,
                    "name": pinfo.get("f14", ""),
                    "price": pd.to_numeric(pinfo.get("f2", 0), errors="coerce") or 0,
                    "pct_chg": pd.to_numeric(pinfo.get("f3", 0), errors="coerce") or 0,
                    "rank": int(item["rk"]),
                    "rank_change": int(item.get("hrc", 0) or 0),
                })

            df = pd.DataFrame(rows)
            df = df.set_index("ts_code")

            logger.info("[PopularityFactor] 获取 %d 只股票人气数据", len(df))
            return df

        except Exception as e:
            logger.warning("[PopularityFactor] 获取人气数据失败: %s", e)
            return None

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 3 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        rank = df.get("rank", pd.Series(9999, index=idx))
        rank_change = df.get("rank_change", zeros)
        pct_chg = df.get("pct_chg", zeros)

        signals: Dict[str, pd.Series] = {}

        # --- 1. 飙升幅度 (0-45)：rank_change 在改善股中的百分位 ---
        s_surge = zeros.copy()
        improvers = rank_change > 0
        if improvers.any():
            surge_pct = rank_change[improvers].rank(pct=True)
            s_surge.loc[improvers] = (surge_pct * 45).clip(0, 45)
        signals["surge"] = s_surge

        # --- 2. 排名强度 (0-35)：逆排名线性衰减 ---
        max_rank = rank.max()
        if max_rank > 1:
            s_rank = (35 * (1 - (rank - 1) / (max_rank - 1))).clip(0, 35)
        else:
            s_rank = pd.Series(35.0, index=idx)
        signals["rank"] = s_rank

        # --- 3. 涨跌幅 (0-20)：分段线性 ---
        s_pct = _linear_map(pct_chg, -5, 0, 10, 20, 0, 20)
        signals["pct_chg"] = s_pct

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        total = sum(signals.values()).clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        rank = df.get("rank", pd.Series(9999, index=df.index))
        rank_change = df.get("rank_change", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0.0, index=df.index))

        signals = self._compute_signals(df)

        signal_meta = [
            ("surge", "飙升幅度", 45),
            ("rank", "排名强度", 35),
            ("pct_chg", "涨跌幅", 20),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            score_val = scores[ts_code]
            if score_val <= 0:
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

            if labels:
                reasons[ts_code] = labels

        return reasons
