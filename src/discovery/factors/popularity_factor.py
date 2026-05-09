# -*- coding: utf-8 -*-
"""人气因子 (Popularity Factor).

盘中+盘后因子：基于东方财富人气排行，识别市场关注度高的股票。
数据来源: 直连东财 emappdata + push2 API（过代理）
返回列: 当前排名, 排名较昨日变动(正数=排名改善/人气飙升), 股票名称, 最新价, 涨跌幅
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class PopularityFactor(BaseFactor):
    """人气因子。

    基于东方财富人气排行榜「飙升榜」。
    关键信号：排名靠前 + 排名在上升（较昨日改善）。
    """

    name = "popularity"
    available_intraday = True
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """直连东财 API（走代理），两步：排名 → 行情数据。"""
        import os, requests

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
            # Step 1: 排名数据
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

            # Step 2: 行情数据（最新价、涨跌幅）
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

            # 组装 DataFrame
            rows = []
            for item in rank_data:
                sc = item["sc"]                              # e.g. SH600519 or SZ000858
                bare = sc[2:] if len(sc) >= 3 else sc        # strip SH/SZ prefix
                pinfo = price_data.get(bare, {})
                rows.append({
                    "代码": bare,
                    "股票名称": pinfo.get("f14", ""),
                    "最新价": pd.to_numeric(pinfo.get("f2", 0), errors="coerce") or 0,
                    "涨跌幅": pd.to_numeric(pinfo.get("f3", 0), errors="coerce") or 0,
                    "当前排名": int(item["rk"]),
                    "排名较昨日变动": int(item.get("hrc", 0) or 0),
                })

            df = pd.DataFrame(rows)
            df = df.rename(columns={"代码": "ts_code"})
            df = df.set_index("ts_code")

            logger.info("[PopularityFactor] 获取 %d 只股票人气数据", len(df))
            return df

        except Exception as e:
            logger.warning("[PopularityFactor] 获取人气数据失败: %s", e)
            return None

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """飙升榜打分：核心信号是排名改善幅度。

        列名: 当前排名, 排名较昨日变动(正数=排名上升/人气飙升), 涨跌幅
        """
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        rank_col = None
        change_col = None
        pct_col = None

        for col in df.columns:
            col_str = str(col)
            if col_str == "当前排名":
                rank_col = col_str
            elif col_str == "排名较昨日变动":
                change_col = col_str
            elif col_str in ("涨跌幅", "pct_chg"):
                pct_col = col_str

        rank = pd.to_numeric(df.get(rank_col, pd.Series(9999, index=df.index)), errors="coerce").fillna(9999)
        change = pd.to_numeric(df.get(change_col, pd.Series(0, index=df.index)), errors="coerce").fillna(0)
        pct_chg = pd.to_numeric(df.get(pct_col, pd.Series(0, index=df.index)), errors="coerce").fillna(0)

        # ── 飙升幅度（核心信号，只奖励排名改善） ──
        scores.loc[change > 2000] += 45.0
        scores.loc[(change > 1000) & (change <= 2000)] += 30.0
        scores.loc[(change > 500) & (change <= 1000)] += 25.0
        scores.loc[(change > 200) & (change <= 500)] += 15.0
        scores.loc[(change > 0) & (change <= 200)] += 10.0

        # ── 排名加分（触及人气核心圈的更强） ──
        scores.loc[rank <= 50] += 20.0
        scores.loc[(rank > 50) & (rank <= 100)] += 10.0

        # ── 风险过滤 ──
        if (pct_chg < -3).any():
            scores.loc[pct_chg < -3] = (scores.loc[pct_chg < -3] - 20).clip(0, 100)

        # ── 否决项 ──
        scores.loc[rank > 3000] = 0.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        rank_col = None
        change_col = None
        pct_col = None

        for col in df.columns:
            col_str = str(col)
            if col_str == "当前排名":
                rank_col = col_str
            elif col_str == "排名较昨日变动":
                change_col = col_str
            elif col_str in ("涨跌幅", "pct_chg"):
                pct_col = col_str

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []

            rank_val = int(df[rank_col].get(ts_code, 9999)) if rank_col else 9999
            chg_val = int(df[change_col].get(ts_code, 0)) if change_col else 0
            pct_val = float(df[pct_col].get(ts_code, 0)) if pct_col else 0

            if chg_val > 2000:
                r.append(f"人气飙升(+{chg_val}位)")
            elif chg_val > 1000:
                r.append(f"人气大涨(+{chg_val}位)")
            elif chg_val > 500:
                r.append(f"人气上升(+{chg_val}位)")
            elif chg_val > 0:
                r.append(f"人气微升(+{chg_val}位)")

            if rank_val <= 50:
                r.append(f"进入人气核心圈(排名{rank_val})")
            elif rank_val <= 100:
                r.append(f"逼近人气核心圈(排名{rank_val})")

            if pct_val < -3:
                r.append(f"人气股下跌(跌幅{pct_val:.1f}%)")

            if r:
                reasons[ts_code] = r
        return reasons
