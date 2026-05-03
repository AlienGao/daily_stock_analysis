# -*- coding: utf-8 -*-
"""人气因子 (Popularity Factor).

盘中+盘后因子：基于东方财富人气排行，识别市场关注度高的股票。
数据来源: akshare stock_hot_up_em()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class PopularityFactor(BaseFactor):
    """人气因子。

    基于东方财富人气排行榜，识别市场关注度最高的股票。
    关键信号：高人气值 + 排名靠前 = 资金关注。
    """

    name = "popularity"
    available_intraday = True
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            logger.warning("[PopularityFactor] 未提供 akshare_fetcher")
            return None

        try:
            import akshare as ak

            logger.info("[PopularityFactor] 调用 ak.stock_hot_up_em() 获取人气排行...")
            df = ak.stock_hot_up_em()
            if df is None or df.empty:
                logger.warning("[PopularityFactor] 人气排行返回空数据")
                return None

            # 重命名列：东财返回 '代码' 列作为 ts_code
            if "代码" in df.columns:
                df = df.rename(columns={"代码": "ts_code"})
            elif "股票代码" in df.columns:
                df = df.rename(columns={"股票代码": "ts_code"})

            # 设置 ts_code 为索引
            if "ts_code" in df.columns:
                df = df.set_index("ts_code")
            elif df.index.name is None:
                # 尝试使用第一列作为索引
                first_col = df.columns[0]
                df = df.set_index(first_col)
                df.index.name = "ts_code"

            logger.info(f"[PopularityFactor] 获取 {len(df)} 只股票人气数据")
            return df

        except Exception as e:
            logger.warning(f"[PopularityFactor] 获取人气数据失败: {e}")
            return None

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 东财人气排行列名: 股票代码, 股票名称, 人气值, 人气值变动, 排名
        # 人气值列名可能是 "人气值" 或其他
        popularity_col = None
        change_col = None
        rank_col = None

        for col in df.columns:
            col_str = str(col)
            if col_str == "人气值":
                popularity_col = col_str
            elif col_str == "人气值变动":
                change_col = col_str
            elif "排名" in col_str or "rank" in col_str.lower():
                rank_col = col_str

        if popularity_col is None:
            # 尝试找任一数值列
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    popularity_col = col
                    break

        popularity = df.get(popularity_col, pd.Series(0, index=df.index))
        change = df.get(change_col, pd.Series(0, index=df.index)) if change_col else pd.Series(0, index=df.index)

        # 高人气值得分 (人气值 > 80000: +40分, 50000-80000: +30分, 20000-50000: +20分, <20000: +10分)
        scores.loc[popularity > 80000] += 40.0
        scores.loc[(popularity > 50000) & (popularity <= 80000)] += 30.0
        scores.loc[(popularity > 20000) & (popularity <= 50000)] += 20.0
        scores.loc[popularity > 0] += 10.0

        # 人气值上升 (正值: +20分, 下降: 扣分)
        scores.loc[change > 0] += 20.0
        scores.loc[change < 0] = (scores.loc[change < 0] - 10).clip(0, 100)

        # 排名靠前（前 50 名: +15分, 50-100: +10分）
        if rank_col:
            rank = df[rank_col]
            try:
                rank_num = pd.to_numeric(rank, errors="coerce").fillna(9999)
                scores.loc[rank_num <= 50] += 15.0
                scores.loc[(rank_num > 50) & (rank_num <= 100)] += 10.0
            except Exception:
                pass

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        popularity_col = None
        change_col = None
        rank_col = None

        for col in df.columns:
            col_str = str(col)
            if col_str == "人气值":
                popularity_col = col_str
            elif col_str == "人气值变动":
                change_col = col_str
            elif "排名" in col_str:
                rank_col = col_str

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []

            pop_val = df[popularity_col].get(ts_code, 0) if popularity_col else 0
            chg_val = df[change_col].get(ts_code, 0) if change_col else 0

            if pop_val > 80000:
                r.append(f"超高人气值({pop_val})")
            elif pop_val > 50000:
                r.append(f"高人气值({pop_val})")
            elif pop_val > 20000:
                r.append(f"中等人气({pop_val})")

            if chg_val > 0:
                r.append(f"人气上升(+{chg_val})")
            elif chg_val < 0:
                r.append(f"人气下降({chg_val})")

            if r:
                reasons[ts_code] = r
        return reasons