# -*- coding: utf-8 -*-
"""游资因子 (Hot Money Factor).

盘后因子：基于东财主力资金流（游资），识别短线热门股票。
数据来源: akshare stock_individual_fund_flow(indicator="游资")
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class HotMoneyFactor(BaseFactor):
    """游资因子。

    基于东财个股资金流（indicator="游资"），识别短线游资关注股票。
    关键信号：主力净流入 + 游资净买入 = 短线热点。
    """

    name = "hot_money"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            logger.warning("[HotMoneyFactor] 未提供 akshare_fetcher")
            return None

        try:
            import akshare as ak

            logger.info("[HotMoneyFactor] 调用 ak.stock_individual_fund_flow(indicator='游资')...")
            df = ak.stock_individual_fund_flow(indicator="游资")
            if df is None or df.empty:
                logger.warning("[HotMoneyFactor] 游资资金流返回空数据")
                return None

            # 重命名列：东财返回 '代码' 列
            if "代码" in df.columns:
                df = df.rename(columns={"代码": "ts_code"})
            elif "股票代码" in df.columns:
                df = df.rename(columns={"股票代码": "ts_code"})

            # 设置 ts_code 为索引
            if "ts_code" in df.columns:
                df = df.set_index("ts_code")
            elif df.index.name is None:
                first_col = df.columns[0]
                df = df.set_index(first_col)
                df.index.name = "ts_code"

            logger.info(f"[HotMoneyFactor] 获取 {len(df)} 只股票游资数据")
            return df

        except Exception as e:
            logger.warning(f"[HotMoneyFactor] 获取游资数据失败: {e}")
            return None

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 东财个股资金流列名（游资）：
        # 股票代码, 股票名称, 主力净流入-净额, 主力净流入-占比, ...各种资金类型
        # 找主力净流入相关列
        net_col = None
        ratio_col = None

        for col in df.columns:
            col_str = str(col)
            if "主力净流入-净额" in col_str and net_col is None:
                net_col = col_str
            elif "主力净流入-占比" in col_str and ratio_col is None:
                ratio_col = col_str

        if net_col is None:
            # 尝试找包含"净额"或"净流入"的列
            for col in df.columns:
                if "净额" in str(col) or ("净流入" in str(col) and "占比" not in str(col)):
                    net_col = str(col)
                    break

        net_flow = df.get(net_col, pd.Series(0, index=df.index)) if net_col else pd.Series(0, index=df.index)
        ratio = df.get(ratio_col, pd.Series(0.0, index=df.index)) if ratio_col else pd.Series(0.0, index=df.index)

        # 主力净流入 > 1亿: +40分, 5000万-1亿: +30分, 1000万-5000万: +20分
        scores.loc[net_flow > 1e8] += 40.0
        scores.loc[(net_flow > 5e7) & (net_flow <= 1e8)] += 30.0
        scores.loc[(net_flow > 1e7) & (net_flow <= 5e7)] += 20.0

        # 主力净流入占比 > 10%: +25分, 5-10%: +15分, 1-5%: +5分
        try:
            ratio_num = pd.to_numeric(ratio, errors="coerce").fillna(0)
            scores.loc[ratio_num > 10] += 25.0
            scores.loc[(ratio_num > 5) & (ratio_num <= 10)] += 15.0
            scores.loc[(ratio_num > 1) & (ratio_num <= 5)] += 5.0
        except Exception:
            pass

        # 净流入为负: 扣分
        scores.loc[net_flow < 0] = (scores.loc[net_flow < 0] - 20).clip(0, 100)

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        net_col = None
        ratio_col = None

        for col in df.columns:
            col_str = str(col)
            if "主力净流入-净额" in col_str:
                net_col = col_str
            elif "主力净流入-占比" in col_str:
                ratio_col = col_str

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []

            net_v = df[net_col].get(ts_code, 0) if net_col else 0
            ratio_v = df[ratio_col].get(ts_code, 0) if ratio_col else 0

            if net_v > 1e8:
                r.append(f"主力净流入大({net_v/1e8:.1f}亿)")
            elif net_v > 5e7:
                r.append(f"主力净流入中({net_v/1e7:.0f}万)")

            if ratio_v > 10:
                r.append(f"净流入占比高({ratio_v:.1f}%)")

            if net_v < 0:
                r.append("主力净流出")

            if r:
                reasons[ts_code] = r
        return reasons