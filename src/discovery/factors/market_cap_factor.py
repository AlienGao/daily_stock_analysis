# -*- coding: utf-8 -*-
"""小市值因子 (Market Cap Factor).

盘后因子：市值越小，分数越高。
数据来源: daily_basic.total_mv (万元)
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor, bare_to_ts_code

logger = logging.getLogger(__name__)


class MarketCapFactor(BaseFactor):
    """小市值因子。

    市值越小的股票得分越高，用于捕捉小市值效应。
    """

    name = "market_cap"
    available_intraday = False
    available_postmarket = True
    weight = 10.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """从 daily_basic 表读取全市场市值数据，并获取股票名称用于过滤 ST。"""
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            with db.get_session() as s:
                from sqlalchemy import text
                rows = s.execute(text(
                    "SELECT code, total_mv FROM daily_basic WHERE trade_date = :td"
                ), {"td": trade_date}).fetchall()
            if not rows:
                logger.info("[MarketCapFactor] 无数据: %s", trade_date)
                return None
            df = pd.DataFrame(rows, columns=["code", "total_mv"])
            df["ts_code"] = df["code"].apply(bare_to_ts_code)
            df = df.set_index("ts_code")
            df = df.drop(columns=["code"])
            # 过滤掉市值为 0 或 NaN 的
            df = df[df["total_mv"].notna() & (df["total_mv"] > 0)]

            # 获取股票名称用于 ST 过滤
            try:
                spot = db.get_realtime_spot()
                if spot is not None and not spot.empty and 'name' in spot.columns:
                    name_map = spot['name'].to_dict()
                    df['stock_name'] = df.index.map(lambda ts: name_map.get(ts.split('.')[0], ''))
                else:
                    df['stock_name'] = ''
            except Exception as e:
                logger.debug("[MarketCapFactor] 获取股票名称失败: %s", e)
                df['stock_name'] = ''

            logger.info("[MarketCapFactor] 获取 %d 条市值数据", len(df))
            return df
        except Exception as e:
            logger.error("[MarketCapFactor] 数据获取失败: %s", e)
            return None

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """市值越小分数越高，归一化到 0-100。ST 股票得分 0。"""
        mv = df["total_mv"]
        if mv.empty:
            return pd.Series(50.0, index=df.index)

        # 识别 ST 股票
        stock_name = df.get("stock_name", pd.Series('', index=df.index))
        is_st = stock_name.str.contains(r'^\*?ST', na=False)

        # 百分位排名，市值越小排名越靠前，所以用 1 - rank
        ranked = (1.0 - mv.rank(pct=True)) * 100.0
        scores = ranked.clip(0, 100).fillna(50.0)

        # ST 股票得 0 分
        scores[is_st] = 0.0

        st_count = is_st.sum()
        if st_count > 0:
            logger.info("[MarketCapFactor] 过滤 %d 只 ST 股票", st_count)

        return scores

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        """返回市值描述。"""
        result: Dict[str, List[str]] = {}
        mv = df.get("total_mv")
        if mv is None:
            return result
        for ts_code in df.index:
            val = mv.get(ts_code)
            if val is not None and pd.notna(val):
                mv_yi = val / 10000.0  # 万元 → 亿元
                result[ts_code] = [f"市值: {mv_yi:.1f}亿"]
        return result
