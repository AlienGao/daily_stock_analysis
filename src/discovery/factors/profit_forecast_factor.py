# -*- coding: utf-8 -*-
"""盈利预测因子 (Profit Forecast Factor).

盘后因子：基于东财盈利预测与评级数据，识别机构认可的股票。
3 个子信号：
- 覆盖度 (0-30)：研报数在全市场中的百分位
- 评级质量 (0-40)：加权评级分在全市场中的百分位
- EPS 增长 (0-30)：2026 vs 2025 EPS 预测增长率百分位

数据来源: akshare stock_profit_forecast_em()
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class ProfitForecastFactor(BaseFactor):
    """盈利预测因子。

    基于机构评级与 EPS 预测，识别机构一致性看好的成长股。
    覆盖度越高说明市场关注度越高，评级越偏买入说明机构越看好，
    EPS 预测增长越快说明业绩预期越强。
    """

    name = "profit_forecast"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    _LABEL_THRESHOLD_RATIO = 0.5

    # EPS 增长率裁剪边界，防止极端值扭曲百分位
    _EPS_GROWTH_CLIP_MIN = -1.0   # -100%
    _EPS_GROWTH_CLIP_MAX = 5.0    # +500%

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """优先从 DB 读取最新盈利预测快照，无数据时 fallback 到 akshare."""

        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df = db.get_latest_profit_forecast()
            if df is not None and not df.empty:
                logger.info("[ProfitForecast] 从 DB 读取 %d 条", len(df))
                return df
        except Exception as e:
            logger.warning("[ProfitForecast] DB 读取失败，fallback 到 akshare: %s", e)

        logger.info("[ProfitForecast] DB 无数据，fallback 到 akshare")
        akshare_fetcher = kwargs.get("akshare_fetcher")
        if akshare_fetcher is None:
            return None
        df = akshare_fetcher.get_profit_forecast()
        if df is not None and not df.empty:
            df = df[~df.index.duplicated(keep='first')]
        return df

    # ------------------------------------------------------------------
    # 列名解析
    # ------------------------------------------------------------------

    @staticmethod
    def _col_buy(df: pd.DataFrame) -> Optional[str]:
        return next((c for c in df.columns if "买入" in c), None)

    @staticmethod
    def _col_add(df: pd.DataFrame) -> Optional[str]:
        return next((c for c in df.columns if "增持" in c and "中性" not in c), None)

    @staticmethod
    def _col_neutral(df: pd.DataFrame) -> Optional[str]:
        return next((c for c in df.columns if "中性" in c), None)

    @staticmethod
    def _col_reduce(df: pd.DataFrame) -> Optional[str]:
        return next((c for c in df.columns if "减持" in c), None)

    @staticmethod
    def _col_report(df: pd.DataFrame) -> Optional[str]:
        return next((c for c in df.columns if "研报数" in c), None)

    @staticmethod
    def _eps_cols(df: pd.DataFrame):
        """返回 (earlier_col, later_col)，取最大的两个预测年份列（最近两期对比）."""
        import re
        from datetime import datetime

        current_year = datetime.now().year
        cols = [c for c in df.columns if "预测每股收益" in c]

        parsed = []
        for c in cols:
            m = re.search(r"(\d{4})", c)
            if m:
                year = int(m.group(1))
                # 只接受合理年份范围（去年 ~ 未来 5 年），过滤误匹配的非年份数字
                if current_year - 1 <= year <= current_year + 5:
                    parsed.append((year, c))
        parsed.sort(key=lambda x: x[0])
        if len(parsed) >= 2:
            return parsed[-2][1], parsed[-1][1]
        if len(parsed) == 1:
            return None, parsed[-1][1]
        return None, None

    # ------------------------------------------------------------------
    # 信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        stock_idx = df.index
        signals: Dict[str, pd.Series] = {}

        # --- 1. 覆盖度 (0-30)：研报数百分位 ---
        col_report = self._col_report(df)
        if col_report is not None:
            report_count = pd.to_numeric(df[col_report], errors="coerce").fillna(1)
            signals['coverage'] = (report_count.rank(pct=True) * 30).clip(0, 30)
        else:
            signals['coverage'] = pd.Series(15.0, index=stock_idx)

        # --- 2. 评级质量 (0-40)：加权评级分百分位 ---
        col_buy = self._col_buy(df)
        col_add = self._col_add(df)
        if col_buy is not None and col_add is not None:
            buy = pd.to_numeric(df[col_buy], errors="coerce").fillna(0)
            add = pd.to_numeric(df[col_add], errors="coerce").fillna(0)
            neutral_col = self._col_neutral(df)
            neutral = pd.to_numeric(df[neutral_col], errors="coerce").fillna(0) if neutral_col else 0
            reduce_col = self._col_reduce(df)
            reduce_val = pd.to_numeric(df[reduce_col], errors="coerce").fillna(0) if reduce_col else 0
            rpt = pd.to_numeric(df[col_report], errors="coerce").fillna(1) if col_report else 1

            rating = (buy * 2 + add * 1 + neutral * 0 + reduce_val * -1) / rpt.clip(1)
            signals['rating_quality'] = (rating.rank(pct=True) * 40).clip(0, 40)
        else:
            signals['rating_quality'] = pd.Series(20.0, index=stock_idx)

        # --- 3. EPS 增长 (0-30)：最近两期预测增长率百分位 ---
        eps_older_col, eps_newer_col = self._eps_cols(df)
        if eps_older_col is not None and eps_newer_col is not None:
            eps_old = pd.to_numeric(df[eps_older_col], errors="coerce")
            eps_new = pd.to_numeric(df[eps_newer_col], errors="coerce").fillna(0)
            has_old = eps_old.notna() & (eps_old.abs() >= 0.005)
            denom = eps_old.abs().clip(0.01)
            growth = ((eps_new - eps_old) / denom).clip(
                self._EPS_GROWTH_CLIP_MIN, self._EPS_GROWTH_CLIP_MAX
            )
            growth = growth.where(has_old, 0.0)
            signals['eps_growth'] = (growth.rank(pct=True) * 30).clip(0, 30)
        else:
            logger.warning("[ProfitForecast] 未找到 EPS 预测年份列，降级使用默认值 15.0")
            signals['eps_growth'] = pd.Series(15.0, index=stock_idx)

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        scores = sum(signals.values()).clip(0, 100)
        scores.name = self.name
        return scores

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        threshold = self._LABEL_THRESHOLD_RATIO

        col_buy = self._col_buy(df)
        col_add = self._col_add(df)
        col_report = self._col_report(df)
        eps_older_col, eps_newer_col = self._eps_cols(df)

        signal_meta = [
            ('coverage', '机构覆盖', 30),
            ('rating_quality', '机构评级', 40),
            ('eps_growth', '盈利增长', 30),
        ]

        for i in df.index:
            score_val = scores.at[i] if i in scores.index else 0
            if score_val <= 0:
                continue

            labels: List[str] = []

            for key, _label, max_val in signal_meta:
                val = signals[key].at[i] if i in signals[key].index else 0
                if val < max_val * threshold:
                    continue

                if key == 'coverage':
                    cnt = int(df.at[i, col_report]) if col_report and col_report in df.columns and pd.notna(df.at[i, col_report]) else 0
                    labels.append(f"机构覆盖({cnt}家研报)")
                elif key == 'rating_quality':
                    buy = int(df.at[i, col_buy]) if col_buy else 0
                    add = int(df.at[i, col_add]) if col_add else 0
                    labels.append(f"机构评级(买入{buy} 增持{add})")
                elif key == 'eps_growth':
                    if eps_older_col and eps_newer_col:
                        old = float(df.at[i, eps_older_col]) if pd.notna(df.at[i, eps_older_col]) else 0
                        new = float(df.at[i, eps_newer_col]) if pd.notna(df.at[i, eps_newer_col]) else 0
                        if old > 0.001:
                            g = (new - old) / old * 100
                            labels.append(f"盈利增长({g:+.0f}%)")
                        else:
                            labels.append(f"盈利预测({new:.2f})")

            if labels:
                reasons[i] = labels

        return reasons
