# -*- coding: utf-8 -*-
"""筹码因子 (Chip Structure / Winner Rate Factor).

盘后因子：基于 5 日筹码分布趋势和胜率数据分析获利盘压力和反弹潜力。
数据来源: Tushare cyq_perf (293) + stock_daily (收盘价)，落库 broker_enrichment_cyq_perf。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class ChipFactor(BaseFactor):
    """筹码胜率因子。

    基于 5 日 winner_rate 趋势 + 百分位归一化，判断筹码结构和多空方向。
    多头：获利适中、深度套牢反弹、筹码集中、洗盘松动、成本上移、靠近历史低点。
    空头：获利盘过大、追高堆积、成本下移、靠近历史高点、筹码结构偏散。
    """

    name = "chip"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    _LOOKBACK_DAYS = 5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取 5 日筹码胜率数据 + 当日收盘价。

        优先从 DB 读历史数据，DB 无数据时 fallback 到 Tushare 多日查询。
        返回 DataFrame: index=ts_code, 含多日列 (d0_winner_rate, d1_winner_rate...)、
        his_low、his_high、close、trade_date。
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

        # 1. 尝试从 DB 读筹码分布
        cyq_df = pd.DataFrame()
        try:
            from src.storage import DatabaseManager
            db = DatabaseManager()
            cyq_df = db.get_cyq_perf_range(
                start_date=start_date, end_date=end_date,
            )
            if not cyq_df.empty:
                logger.info(
                    f"[ChipFactor] DB 命中: {start_date}~{end_date}, "
                    f"{len(cyq_df)} 条"
                )
        except Exception as e:
            logger.debug(f"[ChipFactor] DB 查询失败: {e}")

        # 2. DB 无数据，fallback 到 Tushare 逐日 API
        if cyq_df.empty:
            frames = []
            for td in target_dates:
                day = tushare_fetcher.get_bulk_cyq_perf(trade_date=td)
                if day is not None and not day.empty:
                    frames.append(day)
            if not frames:
                logger.warning(f"[ChipFactor] {start_date}~{end_date} 无数据")
                return None
            cyq_df = pd.concat(frames)

        # 3. 组装宽表：每日期一行 → 每日一列
        cyq_df = cyq_df.reset_index()
        if "trade_date" not in cyq_df.columns:
            logger.warning("[ChipFactor] cyq_perf 数据缺少 trade_date 列")
            return None

        parts = []
        value_cols = [
            "winner_rate", "cost_5pct", "cost_15pct", "cost_50pct",
            "cost_85pct", "cost_95pct", "weight_avg",
        ]
        for i, td in enumerate(target_dates):
            day = cyq_df[cyq_df["trade_date"] == td].copy()
            if day.empty:
                continue
            prefix = f"d{i}"
            day = day.set_index("ts_code")
            for col in value_cols:
                if col in day.columns:
                    day = day.rename(columns={col: f"{prefix}_{col}"})
            keep = [c for c in day.columns if c.startswith("d")]
            if keep:
                parts.append(day[keep])

        if not parts:
            return None

        result = pd.concat(parts, axis=1)

        # 附加 trade_date
        info_cols = cyq_df[["ts_code", "trade_date"]].drop_duplicates(subset="ts_code")
        info_cols = info_cols.set_index("ts_code")
        if "trade_date" in info_cols.columns:
            result["trade_date"] = info_cols["trade_date"]

        # 附加 his_low / his_high（取最新日期的值）
        latest_day = cyq_df[cyq_df["trade_date"] == end_date].copy()
        if not latest_day.empty:
            latest_day = latest_day.set_index("ts_code")
            for col in ["his_low", "his_high"]:
                if col in latest_day.columns:
                    result[col] = latest_day[col]

        # 4. 查当日收盘价（用于计算距历史高低点的真实距离）
        try:
            from src.storage import DatabaseManager
            from sqlalchemy import text
            db2 = DatabaseManager()
            # end_date 是 YYYYMMDD 格式，stock_daily.date 是 YYYY-MM-DD
            date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            with db2.get_session() as sess:
                rows = sess.execute(
                    text(
                        "SELECT code, close FROM stock_daily "
                        "WHERE date = :dt"
                    ),
                    {"dt": date_fmt},
                ).fetchall()
            if rows:
                close_map = {str(r[0]): float(r[1]) for r in rows if r[1] is not None}
                result["close"] = result.index.map(
                    lambda tc: close_map.get(
                        tc.split(".")[0] if "." in str(tc) else str(tc), np.nan
                    )
                )
                logger.info(
                    f"[ChipFactor] 收盘价匹配: {result['close'].notna().sum()} 只"
                )
        except Exception as e:
            logger.debug(f"[ChipFactor] 收盘价查询失败: {e}")

        logger.info(
            f"[ChipFactor] 组装完成: {len(target_dates)} 日, "
            f"{len(result)} 只股票"
        )
        return result

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """计算所有信号，返回信号名 → Series 的映射。

        score() 和 describe() 共享此方法，保证一致性。
        """
        signals: Dict[str, pd.Series] = {}
        if df.empty:
            return signals

        idx = df.index
        day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
        ndays = len(set(c.split("_")[0] for c in day_cols))

        if ndays < 2:
            return self._compute_signals_single_day(df)

        last = ndays - 1
        first = 0

        # --- 基础字段 ---
        wr_last = df.get(f"d{last}_winner_rate", pd.Series(50.0, index=idx))
        wr_first = df.get(f"d{first}_winner_rate", pd.Series(50.0, index=idx))
        cost_5 = df.get(f"d{last}_cost_5pct", pd.Series(0.0, index=idx))
        cost_15 = df.get(f"d{last}_cost_15pct", pd.Series(0.0, index=idx))
        cost_50 = df.get(f"d{last}_cost_50pct", pd.Series(0.0, index=idx))
        cost_85 = df.get(f"d{last}_cost_85pct", pd.Series(0.0, index=idx))
        cost_95 = df.get(f"d{last}_cost_95pct", pd.Series(0.0, index=idx))
        cost_50_first = df.get(f"d{first}_cost_50pct", pd.Series(0.0, index=idx))
        weight_avg = df.get(f"d{last}_weight_avg", pd.Series(1.0, index=idx))
        his_low = df.get("his_low", pd.Series(np.nan, index=idx))
        his_high = df.get("his_high", pd.Series(np.nan, index=idx))
        close = df.get("close", pd.Series(np.nan, index=idx))

        # --- 5 日 winner_rate 变化 ---
        wr_change = _safe_pct_change(wr_last, wr_first)
        signals["wr_change"] = wr_change

        # --- winner_rate 适中（钟形：50% 最优）---
        wr_dist = (wr_last - 50).abs()
        wr_moderate = (15.0 - wr_dist / 50.0 * 15.0).clip(0, 15)
        signals["wr_moderate"] = wr_moderate

        # --- 深度套牢（wr < 15%，越低越加分）---
        wr_deep = ((15.0 - wr_last) / 15.0 * 15.0).clip(0, 15)
        wr_deep[wr_last >= 15] = 0
        signals["wr_deep"] = wr_deep

        # --- 获利盘过大抛压（wr > 85%，越高越扣分）---
        wr_pressure = ((wr_last - 85.0) / 15.0 * 15.0).clip(0, 15)
        wr_pressure[wr_last <= 85] = 0
        signals["wr_pressure"] = wr_pressure

        # --- 筹码集中度（百分位排名）---
        cost_range = (cost_95 - cost_5).abs()
        concentration = cost_range / weight_avg.replace(0, np.nan)
        conc_pct = _pct_rank(-concentration, idx)
        signals["conc_pct"] = conc_pct

        # --- 距历史低点距离 (close - his_low) / his_low * 100 ---
        low_valid = his_low.notna() & his_low.gt(0) & close.notna() & close.gt(0)
        dist_to_low = pd.Series(np.nan, index=idx)
        dist_to_low[low_valid] = (
            (close[low_valid] - his_low[low_valid]) / his_low[low_valid] * 100
        )
        signals["dist_to_low"] = dist_to_low

        # --- 距历史高点距离 (his_high - close) / his_high * 100 ---
        high_valid = his_high.notna() & his_high.gt(0) & close.notna() & close.gt(0)
        dist_to_high = pd.Series(np.nan, index=idx)
        dist_to_high[high_valid] = (
            (his_high[high_valid] - close[high_valid]) / his_high[high_valid] * 100
        )
        signals["dist_to_high"] = dist_to_high

        # --- 5 日成本中轴趋势 (cost_50pct) ---
        cost50_trend = _safe_pct_change(cost_50, cost_50_first)
        signals["cost50_trend"] = cost50_trend

        # --- 筹码结构不对称性 (skew) ---
        upper_range = (cost_85 - cost_50).abs()
        lower_range = (cost_50 - cost_15).abs()
        lower_safe = lower_range.replace(0, np.nan)
        chip_skew = upper_range / lower_safe
        chip_skew = chip_skew.fillna(1.0).clip(0, 10)
        signals["chip_skew"] = chip_skew

        return signals

    def _compute_signals_single_day(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """单日退化信号计算。"""
        signals: Dict[str, pd.Series] = {}
        idx = df.index

        wr = df.get("d0_winner_rate") if "d0_winner_rate" in df.columns else df.get(
            "winner_rate", pd.Series(50.0, index=idx))
        cost_5 = df.get("d0_cost_5pct") if "d0_cost_5pct" in df.columns else df.get(
            "cost_5pct", pd.Series(0.0, index=idx))
        cost_15 = df.get("d0_cost_15pct") if "d0_cost_15pct" in df.columns else df.get(
            "cost_15pct", pd.Series(0.0, index=idx))
        cost_50 = df.get("d0_cost_50pct") if "d0_cost_50pct" in df.columns else df.get(
            "cost_50pct", pd.Series(0.0, index=idx))
        cost_85 = df.get("d0_cost_85pct") if "d0_cost_85pct" in df.columns else df.get(
            "cost_85pct", pd.Series(0.0, index=idx))
        cost_95 = df.get("d0_cost_95pct") if "d0_cost_95pct" in df.columns else df.get(
            "cost_95pct", pd.Series(0.0, index=idx))
        weight_avg = df.get("d0_weight_avg") if "d0_weight_avg" in df.columns else df.get(
            "weight_avg", pd.Series(1.0, index=idx))
        his_low = df.get("his_low", pd.Series(np.nan, index=idx))
        his_high = df.get("his_high", pd.Series(np.nan, index=idx))
        close = df.get("close", pd.Series(np.nan, index=idx))

        wr_dist = (wr - 50).abs()
        signals["wr_moderate"] = (15.0 - wr_dist / 50.0 * 15.0).clip(0, 15)

        wr_deep = ((15.0 - wr) / 15.0 * 15.0).clip(0, 15)
        wr_deep[wr >= 15] = 0
        signals["wr_deep"] = wr_deep

        wr_pressure = ((wr - 85.0) / 15.0 * 15.0).clip(0, 15)
        wr_pressure[wr <= 85] = 0
        signals["wr_pressure"] = wr_pressure

        cost_range = (cost_95 - cost_5).abs()
        concentration = cost_range / weight_avg.replace(0, np.nan)
        signals["conc_pct"] = _pct_rank(-concentration, idx)

        low_valid = his_low.notna() & his_low.gt(0) & close.notna() & close.gt(0)
        dist_to_low = pd.Series(np.nan, index=idx)
        dist_to_low[low_valid] = (
            (close[low_valid] - his_low[low_valid]) / his_low[low_valid] * 100
        )
        signals["dist_to_low"] = dist_to_low

        high_valid = his_high.notna() & his_high.gt(0) & close.notna() & close.gt(0)
        dist_to_high = pd.Series(np.nan, index=idx)
        dist_to_high[high_valid] = (
            (his_high[high_valid] - close[high_valid]) / his_high[high_valid] * 100
        )
        signals["dist_to_high"] = dist_to_high

        signals["wr_change"] = pd.Series(0.0, index=idx)
        signals["cost50_trend"] = pd.Series(0.0, index=idx)

        upper_range = (cost_85 - cost_50).abs()
        lower_range = (cost_50 - cost_15).abs()
        lower_safe = lower_range.replace(0, np.nan)
        chip_skew = upper_range / lower_safe
        signals["chip_skew"] = chip_skew.fillna(1.0).clip(0, 10)

        return signals

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        signals = self._compute_signals(df)
        if not signals:
            return scores

        wr_moderate = signals.get("wr_moderate", pd.Series(0.0, index=df.index))
        wr_deep = signals.get("wr_deep", pd.Series(0.0, index=df.index))
        wr_pressure = signals.get("wr_pressure", pd.Series(0.0, index=df.index))
        wr_change = signals.get("wr_change", pd.Series(0.0, index=df.index))
        conc_pct = signals.get("conc_pct", pd.Series(50.0, index=df.index))
        dist_to_low = signals.get("dist_to_low", pd.Series(np.nan, index=df.index))
        dist_to_high = signals.get("dist_to_high", pd.Series(np.nan, index=df.index))
        cost50_trend = signals.get("cost50_trend", pd.Series(0.0, index=df.index))
        chip_skew = signals.get("chip_skew", pd.Series(1.0, index=df.index))

        # ================================================================
        # 正向信号
        # ================================================================

        # 获利适中（钟形曲线，50% 最优，0-15 渐变）
        scores = scores + wr_moderate

        # 深度套牢，反弹潜力（0-15 渐变）
        scores = scores + wr_deep

        # 筹码集中（全市场前 20%）
        scores.loc[conc_pct > 80] += 10.0
        scores.loc[(conc_pct > 60) & (conc_pct <= 80)] += 5.0

        # 5 日 winner_rate 下降 >10%（洗盘信号）
        scores.loc[wr_change < -10] += 15.0
        scores.loc[(wr_change < -5) & (wr_change >= -10)] += 5.0

        # 距历史低点距离（需 close 有效）
        scores.loc[dist_to_low < 10] += 10.0
        scores.loc[(dist_to_low >= 10) & (dist_to_low < 30)] += 5.0

        # 5 日成本中轴上移（筹码在向高位转移，有资金抬轿）
        scores.loc[cost50_trend > 0] += 5.0
        scores.loc[cost50_trend > 10] += 5.0

        # 上方筹码松散（skew > 2：卖压分散，上方阻力小）
        scores.loc[chip_skew > 2] += 5.0

        # ================================================================
        # 负向信号
        # ================================================================

        # 获利盘过大抛压（0-15 渐变）
        scores = scores - wr_pressure

        # 5 日 winner_rate 上升 >10%（追高风险）
        scores.loc[wr_change > 10] -= 10.0
        scores.loc[(wr_change > 5) & (wr_change <= 10)] -= 5.0

        # 距历史高点距离 <5%（高位风险）
        scores.loc[dist_to_high < 5] -= 10.0
        scores.loc[(dist_to_high >= 5) & (dist_to_high < 10)] -= 5.0

        # 5 日成本中轴下移 >10%（筹码重心下滑，偏空）
        scores.loc[cost50_trend < -10] -= 5.0

        # 下方筹码松散（skew < 0.5：支撑不足）
        scores.loc[chip_skew < 0.5] -= 5.0

        scores = scores.clip(0, 100)
        scores.name = self.name
        return scores

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)
        if not signals:
            return reasons

        wr_moderate = signals.get("wr_moderate", pd.Series(0.0, index=df.index))
        wr_deep = signals.get("wr_deep", pd.Series(0.0, index=df.index))
        wr_pressure = signals.get("wr_pressure", pd.Series(0.0, index=df.index))
        wr_change = signals.get("wr_change", pd.Series(0.0, index=df.index))
        conc_pct = signals.get("conc_pct", pd.Series(50.0, index=df.index))
        dist_to_low = signals.get("dist_to_low", pd.Series(np.nan, index=df.index))
        dist_to_high = signals.get("dist_to_high", pd.Series(np.nan, index=df.index))
        cost50_trend = signals.get("cost50_trend", pd.Series(0.0, index=df.index))
        chip_skew = signals.get("chip_skew", pd.Series(1.0, index=df.index))

        # 取 latest winner_rate 用于显示
        day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
        ndays = len(set(c.split("_")[0] for c in day_cols))
        if ndays >= 1:
            last = ndays - 1
            wr_last = df.get(f"d{last}_winner_rate", pd.Series(50.0, index=df.index))
        elif "winner_rate" in df.columns:
            wr_last = df["winner_rate"]
        else:
            wr_last = pd.Series(50.0, index=df.index)

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            wr = wr_last.get(ts_code, 50)

            if wr_deep.get(ts_code, 0) > 0:
                r.append(f"深度套牢(获利{wr:.0f}%)，反弹潜力大")
            elif wr_moderate.get(ts_code, 0) > 5:
                r.append(f"获利适中({wr:.0f}%)，抛压不大")
            elif wr_pressure.get(ts_code, 0) > 0:
                r.append(f"获利盘过大({wr:.0f}%)，注意抛压")

            if conc_pct.get(ts_code, 50) > 80:
                r.append(f"筹码高度集中({conc_pct.get(ts_code, 0):.0f}分位)")
            elif conc_pct.get(ts_code, 50) > 60:
                r.append(f"筹码较集中({conc_pct.get(ts_code, 0):.0f}分位)")

            wrc = wr_change.get(ts_code, 0)
            if wrc < -10:
                r.append(f"获利盘快速出清({wrc:.0f}%)，洗盘信号")
            elif wrc > 10:
                r.append(f"获利盘快速堆积({wrc:.0f}%)，追高风险")

            dtl = dist_to_low.get(ts_code, np.nan)
            if not np.isnan(dtl):
                if dtl < 10:
                    r.append(f"距历史成本低点{dtl:.0f}%，强反弹信号")
                elif dtl < 30:
                    r.append(f"距历史成本低点{dtl:.0f}%，有反弹空间")

            dth = dist_to_high.get(ts_code, np.nan)
            if not np.isnan(dth) and dth < 10:
                r.append(f"距历史成本高点{dth:.0f}%，高位风险")

            c50t = cost50_trend.get(ts_code, 0)
            if c50t > 5:
                r.append(f"成本中轴上移({c50t:.0f}%)，资金抬轿")
            elif c50t < -10:
                r.append(f"成本中轴下移({c50t:.0f}%)，重心偏空")

            sk = chip_skew.get(ts_code, 1.0)
            if sk > 2:
                r.append("上方筹码松散，卖压分散")
            elif sk < 0.5:
                r.append("下方筹码松散，支撑不足")

            if r:
                reasons[ts_code] = r
        return reasons


def _safe_pct_change(last_val: pd.Series, first_val: pd.Series) -> pd.Series:
    """安全计算增幅 (last - first) / |first| * 100，first 为 0 时返回 0。"""
    first_safe = first_val.replace(0, np.nan)
    result = (last_val - first_val) / first_safe.abs() * 100
    return result.fillna(0)


def _pct_rank(series: pd.Series, index) -> pd.Series:
    """全市场百分位排名 (0-100)，缺失值补 50（中位数）。"""
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(50.0, index=index)
    ranks = valid.rank(pct=True) * 100
    return ranks.reindex(index).fillna(50.0)
