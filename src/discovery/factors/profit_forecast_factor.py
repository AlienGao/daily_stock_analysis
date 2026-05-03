# -*- coding: utf-8 -*-
"""盈利预测因子 (Profit Forecast Factor).

盘后因子：基于东财盈利预测与评级数据，识别机构认可的股票。
数据来源: akshare stock_profit_forecast_em()
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class ProfitForecastFactor(BaseFactor):
    """盈利预测因子。

    基于机构评级与 EPS 预测，识别机构一致性看好的股票。
    关键信号：高评级研报多 + EPS 预测增长 = 成长价值。
    """

    name = "profit_forecast"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        return akshare_fetcher.get_profit_forecast()

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 机构评级列
        col_buy = next((c for c in df.columns if "买入" in c), None)
        col_add = next((c for c in df.columns if "增持" in c and "中性" not in c), None)
        col_neutral = next((c for c in df.columns if "中性" in c), None)
        col_reduce = next((c for c in df.columns if "减持" in c), None)
        col_report = next((c for c in df.columns if "研报数" in c), None)

        # EPS 预测列（找最近年份）
        eps_cols = [c for c in df.columns if "预测每股收益" in c]
        eps_col = eps_cols[0] if eps_cols else None

        # 机构综合评级分：买入*2 + 增持*1 + 中性*0 + 减持*-1
        if col_buy and col_add:
            buy = pd.to_numeric(df[col_buy], errors="coerce").fillna(0)
            add = pd.to_numeric(df[col_add], errors="coerce").fillna(0)
            neutral = pd.to_numeric(df[col_neutral], errors="coerce").fillna(0) if col_neutral else pd.Series(0, index=df.index)
            reduce = pd.to_numeric(df[col_reduce], errors="coerce").fillna(0) if col_reduce else pd.Series(0, index=df.index)
            report_count = pd.to_numeric(df[col_report], errors="coerce").fillna(1) if col_report else pd.Series(1, index=df.index)

            rating_score = (buy * 2 + add * 1 + neutral * 0 + reduce * -1) / report_count.clip(1)
            # 评级分 > 1.5: +50分, 1.0-1.5: +35分, 0.5-1.0: +20分
            scores.loc[rating_score > 1.5] += 50.0
            scores.loc[(rating_score > 1.0) & (rating_score <= 1.5)] += 35.0
            scores.loc[(rating_score > 0.5) & (rating_score <= 1.0)] += 20.0

        # EPS 预测增长（如果有多年预测）
        if eps_col:
            try:
                eps_vals = pd.to_numeric(df[eps_col], errors="coerce").fillna(0)
                # EPS > 0 表示盈利
                scores.loc[eps_vals > 0] += 25.0
                # EPS 增长 > 20%: +25分, > 10%: +15分
                # 仅当有多年数据时计算，这里用绝对值
            except Exception:
                pass

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        col_buy = next((c for c in df.columns if "买入" in c), None)
        col_add = next((c for c in df.columns if "增持" in c and "中性" not in c), None)
        col_report = next((c for c in df.columns if "研报数" in c), None)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if col_buy:
                buy = df[col_buy].get(ts_code, 0)
                add = df[col_add].get(ts_code, 0) if col_add else 0
                if buy + add > 0:
                    r.append(f"机构买入+增持:{int(buy)}+{int(add)}")
            if col_report:
                cnt = df[col_report].get(ts_code, 0)
                if cnt > 0:
                    r.append(f"研报数{int(cnt)}")
            if r:
                reasons[ts_code] = r
        return reasons