# -*- coding: utf-8 -*-
"""基本面因子 (Fundamental Factor).

盘后因子：基于 PE/PB/换手率/量比/市值等估值指标，识别低估值高性价比股票。
数据来源: Tushare daily_basic + stock_basic（行业分类）
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _pct_rank(series: pd.Series) -> pd.Series:
    """返回 0-1 的百分位排名，处理全 NaN 边界。"""
    ranked = series.rank(pct=True)
    return ranked.fillna(0.0)


def _linear_map(series: pd.Series, x0: float, y0: float,
                x1: float, y1: float, clip_low: float = 0.0,
                clip_high: float = 1e9) -> pd.Series:
    """两点线性映射，超出范围 clip。"""
    slope = (y1 - y0) / (x1 - x0) if x1 != x0 else 0.0
    return (y0 + slope * (series - x0)).clip(clip_low, clip_high)


def _group_pct_rank(values: pd.Series, groups: pd.Series,
                    max_points: float) -> pd.Series:
    """任意分组内百分位排名 → 0~max_points。

    组只有 1-2 只股票时退化为全市场百分位。
    """
    result = pd.Series(0.0, index=values.index)
    group_counts = groups.value_counts()
    small_groups = group_counts[group_counts <= 2].index

    for grp in group_counts[group_counts > 2].index:
        mask = (groups == grp) & values.notna()
        if mask.sum() <= 1:
            continue
        result[mask] = _pct_rank(values[mask]) * max_points

    small_mask = groups.isin(small_groups) & values.notna()
    if small_mask.any():
        result[small_mask] = _pct_rank(values[small_mask]) * max_points

    return result


def _industry_pct_rank(values: pd.Series, industries: pd.Series,
                       max_points: float) -> pd.Series:
    """行业内百分位排名 → 0~max_points。"""
    return _group_pct_rank(values, industries, max_points)


def _safe_qcut3(series: pd.Series) -> pd.Series:
    """三分市值分箱，数据变化不足时退化为少箱或单箱。"""
    labels = ["小市值", "中市值", "大市值"]
    unique_vals = series.nunique()
    if unique_vals >= 3:
        try:
            return pd.qcut(series, 3, labels=labels, duplicates="drop")
        except ValueError:
            pass
    if unique_vals >= 2:
        try:
            return pd.qcut(series, 2, labels=labels[:2], duplicates="drop")
        except ValueError:
            pass
    return pd.Series(labels[0], index=series.index)



class FundamentalFactor(BaseFactor):
    """基本面因子。

    行业内的 PE/PB 百分位排名 + 市值组内换手率排名 + 量比/市值分段打分。
    """

    name = "fundamental"
    available_intraday = False
    available_postmarket = False
    weight = 5.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """DB 优先，无数据时 fallback 到 Tushare API。"""
        tushare_fetcher = kwargs.get("tushare_fetcher")
        df_basic: Optional[pd.DataFrame] = None

        # 1. 尝试从 DB 读
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            df_basic = db.get_daily_basic(trade_date)
            if not df_basic.empty:
                # DB 返回 index=code，转回 ts_code 格式
                df_basic = df_basic.reset_index().rename(columns={"code": "ts_code_raw"})
                codes = df_basic["ts_code_raw"].astype(str).str.zfill(6)
                pre2 = codes.str[:2]
                suffix_map = {
                    "60": ".SH", "68": ".SH", "00": ".SZ", "30": ".SZ",
                    "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
                }
                suffix = pre2.map(suffix_map).fillna("")
                df_basic["ts_code"] = codes + suffix
                df_basic = df_basic.set_index("ts_code").drop(columns=["ts_code_raw"], errors="ignore")
                logger.info(
                    f"[FundamentalFactor] DB 命中: {trade_date}, {len(df_basic)} 条"
                )
        except Exception as e:
            logger.debug(f"[FundamentalFactor] DB 查询失败: {e}")

        # 2. DB 无数据，fallback 到 Tushare API
        if df_basic is None or df_basic.empty:
            if tushare_fetcher is None:
                return None
            df_basic = tushare_fetcher.get_daily_basic_all(trade_date)
            if df_basic is None or df_basic.empty:
                return None
            # 落库缓存，下次命中 DB
            try:
                from src.storage import DatabaseManager
                df_save = df_basic.reset_index()
                df_save["code"] = df_save["ts_code"].astype(str).str.replace(r"\..*", "", regex=True)
                DatabaseManager().upsert_daily_basic(df_save, source="tushare")
                logger.info("[FundamentalFactor] 落库 %d 条 daily_basic", len(df_save))
            except Exception as e:
                logger.debug("[FundamentalFactor] 落库 daily_basic 失败: %s", e)

        # 合并同花顺行业分类
        try:
            from src.storage import DatabaseManager
            ths_map = DatabaseManager().get_ths_industry_map()
        except Exception:
            ths_map = {}
        if ths_map:
            bare = df_basic.index.astype(str).str.replace(r"\..*", "", regex=True)
            df_basic["industry"] = bare.map(ths_map).fillna("其他")
        else:
            df_basic["industry"] = "其他"

        return df_basic

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 5 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        pe = df.get("pe", zeros)
        pb = df.get("pb", zeros)
        turnover = df.get("turnover_rate", zeros)
        vol_ratio = df.get("volume_ratio", zeros)
        total_mv = df.get("total_mv", zeros)
        industry = df.get("industry", pd.Series("其他", index=idx))

        signals: Dict[str, pd.Series] = {}

        # 1. PE 行业低估 (0-30)：PE>0 的股票，行业内低 PE 排前面
        pe_pos = pe > 0
        s_pe = zeros.copy()
        if pe_pos.any():
            inv_pe = (1.0 / pe[pe_pos].replace(0, np.nan)).dropna()
            if len(inv_pe) > 0:
                s_pe[inv_pe.index] = _industry_pct_rank(
                    inv_pe, industry.loc[inv_pe.index], 30.0,
                )
        signals["pe"] = s_pe

        # 2. PB 行业低估 (0-20)
        pb_pos = pb > 0
        s_pb = zeros.copy()
        if pb_pos.any():
            inv_pb = (1.0 / pb[pb_pos].replace(0, np.nan)).dropna()
            if len(inv_pb) > 0:
                s_pb[inv_pb.index] = _industry_pct_rank(
                    inv_pb, industry.loc[inv_pb.index], 20.0,
                )
        signals["pb"] = s_pb

        # 3. 换手率活跃度 (0-25)：市值组内百分位排名
        s_turnover = zeros.copy()
        if (total_mv > 0).any():
            mv_valid = total_mv[total_mv > 0]
            mv_terciles = _safe_qcut3(mv_valid)
            s_turnover.loc[mv_terciles.index] = _group_pct_rank(
                turnover.loc[mv_terciles.index], mv_terciles, 25.0,
            )
        signals["turnover"] = s_turnover

        # 4. 量比异动 (0-15)：分段线性
        s_vr = zeros.copy()
        s_vr = s_vr.mask(vol_ratio >= 2.0, 15.0)
        s_vr = s_vr.mask((vol_ratio >= 1.5) & (vol_ratio < 2.0),
                         _linear_map(vol_ratio, 1.5, 10, 2.0, 15))
        s_vr = s_vr.mask((vol_ratio >= 1.0) & (vol_ratio < 1.5),
                         _linear_map(vol_ratio, 1.0, 5, 1.5, 10))
        s_vr = s_vr.mask((vol_ratio >= 0.8) & (vol_ratio < 1.0),
                         _linear_map(vol_ratio, 0.8, 0, 1.0, 5))
        signals["volume_ratio"] = s_vr

        # 5. 中小市值弹性 (0-10)：分段线性，单位万元 → 亿
        mv_b = total_mv / 1e8
        s_mv = zeros.copy()
        s_mv = s_mv.mask((mv_b >= 10) & (mv_b <= 100), 10.0)
        s_mv = s_mv.mask((mv_b > 100) & (mv_b <= 200),
                         _linear_map(mv_b, 100, 10, 200, 5))
        s_mv = s_mv.mask((mv_b > 200) & (mv_b <= 500),
                         _linear_map(mv_b, 200, 5, 500, 0))
        signals["market_cap"] = s_mv

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
        if df.empty:
            return {}

        signals = self._compute_signals(df)

        signal_meta = [
            ("pe", "PE低估"),
            ("pb", "PB低估"),
            ("turnover", "活跃"),
            ("volume_ratio", "放量"),
            ("market_cap", "中小市值"),
        ]
        max_map = {
            "pe": 30, "pb": 20, "turnover": 25,
            "volume_ratio": 15, "market_cap": 10,
        }
        threshold = self._LABEL_THRESHOLD_RATIO

        pe_raw = df.get("pe", pd.Series(0.0, index=df.index))
        pb_raw = df.get("pb", pd.Series(0.0, index=df.index))
        tr_raw = df.get("turnover_rate", pd.Series(0.0, index=df.index))
        vr_raw = df.get("volume_ratio", pd.Series(0.0, index=df.index))
        mv_raw = df.get("total_mv", pd.Series(0.0, index=df.index))

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            labels = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_map[key] * threshold:
                    continue
                if key == "pe":
                    pe_v = pe_raw.get(ts_code, 0)
                    labels.append(f"{label}(PE={pe_v:.0f})")
                elif key == "pb":
                    pb_v = pb_raw.get(ts_code, 0)
                    labels.append(f"{label}(PB={pb_v:.1f})")
                elif key == "turnover":
                    tr_v = tr_raw.get(ts_code, 0)
                    labels.append(f"高换手({tr_v:.1f}%)")
                elif key == "volume_ratio":
                    vr_v = vr_raw.get(ts_code, 0)
                    labels.append(f"量比({vr_v:.1f})")
                elif key == "market_cap":
                    mv_b = mv_raw.get(ts_code, 0) / 1e8
                    labels.append(f"市值({mv_b:.0f}亿)")
            if labels:
                reasons[ts_code] = labels

        return reasons
