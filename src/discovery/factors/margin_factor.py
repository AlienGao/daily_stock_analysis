# -*- coding: utf-8 -*-
"""融资融券因子 (Margin Trading Factor).

盘后因子：杠杆资金 5 日趋势分析（需落库支持）。
数据来源: Tushare margin_detail (59) + daily_basic (市值归一化)
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class MarginFactor(BaseFactor):
    """融资融券因子。

    基于 5 日融资融券趋势 + 市值分位数归一化，判断杠杆资金方向。
    多头信号：融资余额增长、融资买入活跃、偿还额下降。
    空头信号：融券卖出、融券占比偏高、融资买入快速萎缩。
    """

    name = "margin"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    _LOOKBACK_DAYS = 5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取 5 日融资融券数据 + 市值。

        优先从 DB 读历史数据，DB 无数据时 fallback 到 Tushare 多日查询。
        返回 DataFrame: index=ts_code, 含多日列 (d0_rzye, d1_rzye...)、name、
        trade_date、total_mv。
        """
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None

        trade_dates = sorted(tushare_fetcher._get_trade_dates())
        if not trade_dates:
            return None
        try:
            idx = trade_dates.index(trade_date)
        except ValueError:
            idx = len(trade_dates) - 1
        start_idx = max(0, idx - self._LOOKBACK_DAYS + 1)
        target_dates = trade_dates[start_idx : idx + 1]

        start_date = target_dates[0]
        end_date = target_dates[-1]

        # 1. 尝试从 DB 读
        margin_df = pd.DataFrame()
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            margin_df = db.get_margin_detail_range(
                start_date=start_date, end_date=end_date,
            )
            if not margin_df.empty:
                logger.info(
                    f"[MarginFactor] DB 命中: {start_date}~{end_date}, "
                    f"{len(margin_df)} 条"
                )
        except Exception as e:
            logger.debug(f"[MarginFactor] DB 查询失败: {e}")

        # 2. DB 无数据，fallback 到 Tushare 多日 API
        if margin_df.empty:
            margin_df = tushare_fetcher.get_bulk_margin_detail_range(
                start_date, end_date,
            )
            if margin_df is None or margin_df.empty:
                logger.warning(f"[MarginFactor] {start_date}~{end_date} 无数据")
                return None

        # 3. 拿市值做归一化
        daily_basic = tushare_fetcher.get_daily_basic_all(trade_date)
        if daily_basic is not None and not daily_basic.empty:
            mv_series = daily_basic.get("total_mv")
        else:
            mv_series = None

        # 4. 组装宽表：每日期一行 → 每日一列
        margin_df = margin_df.reset_index()
        if "trade_date" not in margin_df.columns:
            logger.warning("[MarginFactor] margin 数据缺少 trade_date 列")
            return None

        parts = []
        for i, td in enumerate(target_dates):
            day = margin_df[margin_df["trade_date"] == td].copy()
            if day.empty:
                continue
            prefix = f"d{i}"
            day = day.set_index("ts_code")
            for col in ["rzye", "rzmre", "rzche", "rqye", "rqmre", "rqyl"]:
                if col in day.columns:
                    day = day.rename(columns={col: f"{prefix}_{col}"})
            keep = [c for c in day.columns if c.startswith("d")]
            if keep:
                parts.append(day[keep])

        if not parts:
            return None

        result = pd.concat(parts, axis=1)

        # 附加 name、trade_date（name 仅 DB 有，Tushare API 无）
        info_keep = ["ts_code"]
        for c in ["name", "trade_date"]:
            if c in margin_df.columns:
                info_keep.append(c)
        info_cols = margin_df[info_keep].drop_duplicates(subset="ts_code")
        info_cols = info_cols.set_index("ts_code")
        if "name" in info_cols.columns:
            result["name"] = info_cols["name"]
        if "trade_date" in info_cols.columns:
            result["trade_date"] = info_cols["trade_date"]

        # 附加市值
        if mv_series is not None:
            result["total_mv"] = result.index.map(
                lambda c: mv_series.get(c, np.nan) if hasattr(mv_series, 'get') else np.nan
            )

        logger.info(
            f"[MarginFactor] 组装完成: {len(target_dates)} 日, "
            f"{len(result)} 只股票"
        )
        return result

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        # 数有几个日期的列（d0_rzye, d1_rzye...）
        day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
        ndays = len(set(c.split("_")[0] for c in day_cols))
        if ndays < 2:
            # 少于 2 日数据，退化为单日评分
            return self._score_single_day(df)

        last = ndays - 1
        first = 0

        total_mv = df.get("total_mv")
        has_mv = total_mv is not None and total_mv.notna().any()

        # 取最新日和最早日的列
        rzye_last = df.get(f"d{last}_rzye", pd.Series(0.0, index=df.index))
        rzye_first = df.get(f"d{first}_rzye", pd.Series(0.0, index=df.index))
        rzmre_last = df.get(f"d{last}_rzmre", pd.Series(0.0, index=df.index))
        rzmre_first = df.get(f"d{first}_rzmre", pd.Series(0.0, index=df.index))
        rzche_last = df.get(f"d{last}_rzche", pd.Series(0.0, index=df.index))
        rzche_first = df.get(f"d{first}_rzche", pd.Series(0.0, index=df.index))
        rqye_last = df.get(f"d{last}_rqye", pd.Series(0.0, index=df.index))
        rqmre_last = df.get(f"d{last}_rqmre", pd.Series(0.0, index=df.index))

        # 5 日增幅
        rzye_growth = _safe_pct_change(rzye_last, rzye_first)
        rzmre_growth = _safe_pct_change(rzmre_last, rzmre_first)
        rzche_growth = _safe_pct_change(rzche_last, rzche_first)
        rqye_growth = _safe_pct_change(rqye_last, df.get(f"d{first}_rqye", pd.Series(0.0, index=df.index)))

        # 融资买入活跃度
        rzye_safe = rzye_last.replace(0, np.nan)
        margin_ratio = (rzmre_last / rzye_safe) * 100
        rzye_first_safe = rzye_first.replace(0, np.nan)
        margin_ratio_first = (rzmre_first / rzye_first_safe) * 100
        margin_ratio_trend = _safe_pct_change(
            margin_ratio.fillna(0), margin_ratio_first.fillna(0)
        )

        # 市值归一化
        if has_mv:
            rzye_ratio_pct = _pct_rank(_safe_ratio(rzye_last, total_mv) * 100, df.index)
            rzmre_ratio_pct = _pct_rank(_safe_ratio(rzmre_last, total_mv) * 100, df.index)
            rqye_ratio_pct = _pct_rank(_safe_ratio(rqye_last, total_mv) * 100, df.index)
        else:
            rzye_ratio_pct = pd.Series(50.0, index=df.index)
            rzmre_ratio_pct = pd.Series(50.0, index=df.index)
            rqye_ratio_pct = pd.Series(50.0, index=df.index)

        # ================================================================
        # 正向信号（多头杠杆）
        # ================================================================

        # 融资买入额 5 日增长
        scores.loc[rzmre_growth > 0] += 15.0

        # 融资买入占市值比超全市场中位数
        scores.loc[rzmre_ratio_pct > 50] += 10.0

        # 融资偿还额下降
        scores.loc[rzche_growth < 0] += 10.0

        # 融资买入占比趋势上升
        scores.loc[margin_ratio_trend > 0] += 15.0

        # 融资余额占市值比高（杠杆关注度高）
        scores.loc[rzye_ratio_pct > 70] += 20.0
        scores.loc[(rzye_ratio_pct > 50) & (rzye_ratio_pct <= 70)] += 10.0

        # ================================================================
        # 负向信号（空头压力）
        # ================================================================

        # 有融券卖出
        scores.loc[rqmre_last > 0] -= 10.0

        # 融券余额占市值比偏高
        scores.loc[rqye_ratio_pct > 70] -= 15.0

        # 融资买入占比快速下降
        scores.loc[margin_ratio_trend < -10] -= 20.0

        return scores.clip(0, 100)

    def _score_single_day(self, df: pd.DataFrame) -> pd.Series:
        """单日退化评分（DB 中只有 1 日数据时）。"""
        scores = pd.Series(0.0, index=df.index, name=self.name)

        total_mv = df.get("total_mv")
        has_mv = total_mv is not None and total_mv.notna().any()

        # 取 d0 列（或原始列名兜底）
        rzye = df.get("d0_rzye") if "d0_rzye" in df.columns else df.get("rzye", pd.Series(0.0, index=df.index))
        rzmre = df.get("d0_rzmre") if "d0_rzmre" in df.columns else df.get("rzmre", pd.Series(0.0, index=df.index))
        rqmre = df.get("d0_rqmre") if "d0_rqmre" in df.columns else df.get("rqmre", pd.Series(0.0, index=df.index))
        rqye = df.get("d0_rqye") if "d0_rqye" in df.columns else df.get("rqye", pd.Series(0.0, index=df.index))

        if has_mv:
            rzye_ratio_pct = _pct_rank(_safe_ratio(rzye, total_mv) * 100, df.index)
            rzmre_ratio_pct = _pct_rank(_safe_ratio(rzmre, total_mv) * 100, df.index)
            rqye_ratio_pct = _pct_rank(_safe_ratio(rqye, total_mv) * 100, df.index)
        else:
            rzye_ratio_pct = pd.Series(50.0, index=df.index)
            rzmre_ratio_pct = pd.Series(50.0, index=df.index)
            rqye_ratio_pct = pd.Series(50.0, index=df.index)

        # 融资买入活跃
        scores.loc[rzmre > 0] += 10.0
        # 融资买入占市值比超中位数
        scores.loc[rzmre_ratio_pct > 50] += 10.0
        # 融资余额占市值比高
        scores.loc[rzye_ratio_pct > 70] += 15.0
        scores.loc[(rzye_ratio_pct > 50) & (rzye_ratio_pct <= 70)] += 10.0
        # 融券卖出扣分
        scores.loc[rqmre > 0] -= 10.0
        # 融券占比偏高
        scores.loc[rqye_ratio_pct > 70] -= 15.0

        return scores.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
        ndays = len(set(c.split("_")[0] for c in day_cols))
        if ndays < 2:
            return self._describe_single_day(df, scores)

        last = ndays - 1
        first = 0

        total_mv = df.get("total_mv")
        has_mv = total_mv is not None and total_mv.notna().any()

        rzye_last = df.get(f"d{last}_rzye", pd.Series(0.0, index=df.index))
        rzye_first = df.get(f"d{first}_rzye", pd.Series(0.0, index=df.index))
        rzmre_last = df.get(f"d{last}_rzmre", pd.Series(0.0, index=df.index))
        rzmre_first = df.get(f"d{first}_rzmre", pd.Series(0.0, index=df.index))
        rzche_last = df.get(f"d{last}_rzche", pd.Series(0.0, index=df.index))
        rzche_first = df.get(f"d{first}_rzche", pd.Series(0.0, index=df.index))
        rqmre_last = df.get(f"d{last}_rqmre", pd.Series(0.0, index=df.index))
        rqye_last = df.get(f"d{last}_rqye", pd.Series(0.0, index=df.index))

        rzmre_growth = _safe_pct_change(rzmre_last, rzmre_first)
        rzche_growth = _safe_pct_change(rzche_last, rzche_first)
        rzye_safe = rzye_last.replace(0, np.nan)
        margin_ratio = (rzmre_last / rzye_safe) * 100
        rzye_first_safe = rzye_first.replace(0, np.nan)
        margin_ratio_first = (rzmre_first / rzye_first_safe) * 100
        margin_ratio_trend = _safe_pct_change(
            margin_ratio.fillna(0), margin_ratio_first.fillna(0)
        )

        if has_mv:
            rzye_ratio_pct = _pct_rank(_safe_ratio(rzye_last, total_mv) * 100, df.index)
            rzmre_ratio_pct = _pct_rank(_safe_ratio(rzmre_last, total_mv) * 100, df.index)
            rqye_ratio_pct = _pct_rank(_safe_ratio(rqye_last, total_mv) * 100, df.index)
        else:
            rzye_ratio_pct = pd.Series(50.0, index=df.index)
            rzmre_ratio_pct = pd.Series(50.0, index=df.index)
            rqye_ratio_pct = pd.Series(50.0, index=df.index)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if rzmre_growth.get(ts_code, 0) > 0:
                r.append("融资买入额5日增长")
            if rzmre_ratio_pct.get(ts_code, 50) > 50:
                r.append("融资买入(市值比)活跃")
            if rzche_growth.get(ts_code, 0) < 0:
                r.append("融资偿还额下降")
            if margin_ratio_trend.get(ts_code, 0) > 0:
                r.append("融资买入占比趋势上升")
            _rzye_pct = rzye_ratio_pct.get(ts_code, 50)
            if _rzye_pct > 70:
                r.append(f"融资余额占市值比高({_rzye_pct:.0f}分位)")
            elif _rzye_pct > 50:
                r.append(f"融资余额占市值比中上({_rzye_pct:.0f}分位)")
            if rqmre_last.get(ts_code, 0) > 0:
                r.append("有融券卖出")
            if rqye_ratio_pct.get(ts_code, 50) > 70:
                r.append("融券占比偏高")
            if margin_ratio_trend.get(ts_code, 0) < -10:
                r.append("融资买入快速萎缩")
            if r:
                reasons[ts_code] = r
        return reasons

    def _describe_single_day(self, df: pd.DataFrame, scores: pd.Series) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        total_mv = df.get("total_mv")
        has_mv = total_mv is not None and total_mv.notna().any()

        rzmre = df.get("d0_rzmre") if "d0_rzmre" in df.columns else df.get("rzmre", pd.Series(0.0, index=df.index))
        rqmre = df.get("d0_rqmre") if "d0_rqmre" in df.columns else df.get("rqmre", pd.Series(0.0, index=df.index))
        rqye = df.get("d0_rqye") if "d0_rqye" in df.columns else df.get("rqye", pd.Series(0.0, index=df.index))
        rzye = df.get("d0_rzye") if "d0_rzye" in df.columns else df.get("rzye", pd.Series(0.0, index=df.index))

        if has_mv:
            rzye_ratio_pct = _pct_rank(_safe_ratio(rzye, total_mv) * 100, df.index)
            rzmre_ratio_pct = _pct_rank(_safe_ratio(rzmre, total_mv) * 100, df.index)
            rqye_ratio_pct = _pct_rank(_safe_ratio(rqye, total_mv) * 100, df.index)
        else:
            rzye_ratio_pct = pd.Series(50.0, index=df.index)
            rzmre_ratio_pct = pd.Series(50.0, index=df.index)
            rqye_ratio_pct = pd.Series(50.0, index=df.index)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            if rzmre.get(ts_code, 0) > 0:
                r.append("融资买入活跃")
            if rzmre_ratio_pct.get(ts_code, 50) > 50:
                r.append("融资买入(市值比)活跃")
            _rzye_pct = rzye_ratio_pct.get(ts_code, 50)
            if _rzye_pct > 70:
                r.append(f"融资余额占市值比高({_rzye_pct:.0f}分位)")
            elif _rzye_pct > 50:
                r.append(f"融资余额占市值比中上({_rzye_pct:.0f}分位)")
            if rqmre.get(ts_code, 0) > 0:
                r.append("有融券卖出")
            if rqye_ratio_pct.get(ts_code, 50) > 70:
                r.append("融券占比偏高")
            if r:
                reasons[ts_code] = r
        return reasons


def _safe_pct_change(last_val: pd.Series, first_val: pd.Series) -> pd.Series:
    """安全计算增幅 (last - first) / |first| * 100，first 为 0 时返回 0。"""
    first_safe = first_val.replace(0, np.nan)
    result = (last_val - first_val) / first_safe.abs() * 100
    return result.fillna(0)


def _safe_ratio(series: pd.Series, mv: pd.Series) -> pd.Series:
    """计算 值/市值 比率，市值缺失或为 0 时返回 NaN。"""
    mv_safe = mv.replace(0, np.nan)
    return series / mv_safe


def _pct_rank(series: pd.Series, index) -> pd.Series:
    """全市场百分位排名 (0-100)，缺失值补 50（中位数）。"""
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(50.0, index=index)
    ranks = valid.rank(pct=True) * 100
    return ranks.reindex(index).fillna(50.0)
