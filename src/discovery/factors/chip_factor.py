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

    基于 5 日 winner_rate 趋势 + 全市场百分位归一化，判断筹码结构和多空方向。
    多头：获利适中、深度套牢反弹、筹码集中、洗盘松动、成本上移、靠近历史低点。
    空头：获利盘过大、追高堆积、成本下移、靠近历史高点、筹码结构偏散。
    """

    name = "chip"
    available_intraday = False
    available_postmarket = True
    weight = 15.0

    _LOOKBACK_DAYS = 5
    _LABEL_THRESHOLD = 5.0

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
            from datetime import datetime as _dt
            from src.storage import DatabaseManager
            from sqlalchemy import text
            db2 = DatabaseManager()
            date_fmt = _dt.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
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

        # 5. 查 60 日 daily 数据，计算每只股票自身日均振幅（用于波动率归一化）
        try:
            from datetime import timedelta as _td
            from src.storage import DatabaseManager
            from sqlalchemy import text

            target_dt = _dt.strptime(end_date, "%Y%m%d")
            vol_start = (target_dt - _td(days=90)).strftime("%Y-%m-%d")
            vol_end = target_dt.strftime("%Y-%m-%d")

            db3 = DatabaseManager()
            with db3.get_session() as sess:
                vol_rows = sess.execute(
                    text(
                        "SELECT code, high, low, close FROM stock_daily "
                        "WHERE date >= :start AND date <= :end"
                    ),
                    {"start": vol_start, "end": vol_end},
                ).fetchall()

            if vol_rows:
                vol_df = pd.DataFrame(
                    vol_rows, columns=["code", "high", "low", "close"]
                )
                vol_df["daily_range"] = (
                    (vol_df["high"] - vol_df["low"]) / vol_df["close"].replace(0, np.nan)
                ).abs()
                vol_df["code"] = vol_df["code"].astype(str)
                avg_range = vol_df.groupby("code")["daily_range"].mean()
                # 映射到 result index（ts_code 格式如 600519.SH）
                result["avg_range"] = result.index.map(
                    lambda tc: avg_range.get(
                        tc.split(".")[0] if "." in str(tc) else str(tc), np.nan
                    )
                )
                matched = result["avg_range"].notna().sum()
                logger.info(
                    f"[ChipFactor] 波动率归一化: {matched}/{len(result)} 只有效, "
                    f"avg_range 中位数 {result['avg_range'].median():.4f}"
                )
            else:
                result["avg_range"] = np.nan
        except Exception as e:
            logger.warning(f"[ChipFactor] 波动率查询失败: {e}")
            result["avg_range"] = np.nan

        logger.info(
            f"[ChipFactor] 组装完成: {len(target_dates)} 日, "
            f"{len(result)} 只股票"
        )
        return result

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """计算所有信号，返回信号名 → Series 的映射。

        绝对值信号（有经济含义）：wr_moderate, wr_deep, wr_pressure
        波动率归一化百分位信号：wr_change_pct, dist_low_pct, dist_high_pct,
                               cost50_pct, skew_pct, conc_pct
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
        wr_last = df.get(f"d{last}_winner_rate", pd.Series(50.0, index=idx)).fillna(50.0)
        wr_first = df.get(f"d{first}_winner_rate", pd.Series(50.0, index=idx)).fillna(50.0)
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
        avg_range = df.get("avg_range", pd.Series(np.nan, index=idx))

        # --- 计算 5 日 winner_rate 波动率（wr 变化归一化用） ---
        wr_cols = [f"d{i}_winner_rate" for i in range(ndays)
                   if f"d{i}_winner_rate" in df.columns]
        if len(wr_cols) >= 2:
            wr_matrix = pd.concat(
                [df[c] for c in wr_cols], axis=1
            )
            wr_vol = wr_matrix.std(axis=1).clip(lower=1.0)
        else:
            wr_vol = pd.Series(1.0, index=idx)
        wr_vol = wr_vol.fillna(1.0)

        # ================================================================
        # 绝对值信号（经济含义明确，不做百分位）
        # ================================================================

        # winner_rate 适中（钟形：50% 最优, 0-15 渐变）
        wr_dist = (wr_last - 50).abs()
        wr_moderate = (15.0 - wr_dist / 50.0 * 15.0).clip(0, 15)
        signals["wr_moderate"] = wr_moderate

        # 深度套牢（wr < 15%，越低越加分, 0-15 渐变）
        wr_deep = ((15.0 - wr_last) / 15.0 * 15.0).clip(0, 15)
        wr_deep[wr_last >= 15] = 0
        signals["wr_deep"] = wr_deep

        # 获利盘过大抛压（wr > 85%，越高越扣分, 0-15 渐变）
        wr_pressure = ((wr_last - 85.0) / 15.0 * 15.0).clip(0, 15)
        wr_pressure[wr_last <= 85] = 0
        signals["wr_pressure"] = wr_pressure

        # 筹码集中度（已是百分位，保持不变）
        cost_range = (cost_95 - cost_5).abs()
        concentration = cost_range / weight_avg.replace(0, np.nan)
        conc_pct = _pct_rank(-concentration, idx)
        signals["conc_pct"] = conc_pct

        # ================================================================
        # 波动率归一化的百分位信号（横截面 + 自身历史双层归一化）
        # ================================================================

        # 5 日 winner_rate 变化（用自身 wr 波动率归一化）
        wr_change = _safe_pct_change(wr_last, wr_first)
        signals["wr_change"] = wr_change
        wr_change_norm = wr_change / wr_vol  # 多少个"wr 标准差"
        signals["wr_change_pct"] = _pct_rank(-wr_change_norm, idx)

        # 距历史低点距离：原始 % → 归一化为"多少个日均振幅"
        low_valid = his_low.notna() & his_low.gt(0) & close.notna() & close.gt(0)
        dist_to_low = pd.Series(np.nan, index=idx)
        dist_to_low[low_valid] = (
            (close[low_valid] - his_low[low_valid]) / his_low[low_valid] * 100
        )
        signals["dist_to_low"] = dist_to_low
        # 归一化：原始距离 / (日均振幅 * 100)，avg_range 如 0.03 → 放大系数 3
        range_pct = avg_range * 100  # 转为百分比
        range_pct = range_pct.clip(lower=0.5)  # 最低 0.5%，防除零
        dist_low_norm = dist_to_low / range_pct
        signals["dist_low_pct"] = _pct_rank(-dist_low_norm, idx)

        # 距历史高点距离：同样用 avg_range 归一化
        high_valid = his_high.notna() & his_high.gt(0) & close.notna() & close.gt(0)
        dist_to_high = pd.Series(np.nan, index=idx)
        dist_to_high[high_valid] = (
            (his_high[high_valid] - close[high_valid]) / his_high[high_valid] * 100
        )
        signals["dist_to_high"] = dist_to_high
        dist_high_norm = dist_to_high / range_pct
        signals["dist_high_pct"] = _pct_rank(dist_high_norm, idx)

        # 5 日成本中轴趋势（上移越多分位越高）
        cost50_trend = _safe_pct_change(cost_50, cost_50_first)
        signals["cost50_trend"] = cost50_trend
        signals["cost50_pct"] = _pct_rank(cost50_trend, idx)

        # 筹码结构不对称性（上方越松散分位越高）
        upper_range = (cost_85 - cost_50).abs()
        lower_range = (cost_50 - cost_15).abs()
        lower_safe = lower_range.replace(0, np.nan)
        chip_skew = upper_range / lower_safe
        chip_skew = chip_skew.fillna(1.0).clip(0, 10)
        signals["chip_skew"] = chip_skew
        signals["skew_pct"] = _pct_rank(chip_skew, idx)

        return signals

    def _compute_signals_single_day(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """单日退化信号计算（无趋势信号 + 无百分位差异）。"""
        signals: Dict[str, pd.Series] = {}
        idx = df.index

        wr = df.get("d0_winner_rate") if "d0_winner_rate" in df.columns else df.get(
            "winner_rate", pd.Series(50.0, index=idx))
        wr = wr.fillna(50.0)
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
        avg_range = df.get("avg_range", pd.Series(np.nan, index=idx))

        # 绝对值信号
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

        # 距历史高低点（有 avg_range 时归一化，无时 fallback 原始距离）
        range_pct = avg_range * 100
        range_pct = range_pct.fillna(2.0).clip(lower=0.5)

        low_valid = his_low.notna() & his_low.gt(0) & close.notna() & close.gt(0)
        dist_to_low = pd.Series(np.nan, index=idx)
        dist_to_low[low_valid] = (
            (close[low_valid] - his_low[low_valid]) / his_low[low_valid] * 100
        )
        signals["dist_to_low"] = dist_to_low
        dist_low_norm = dist_to_low / range_pct
        signals["dist_low_pct"] = _pct_rank(-dist_low_norm, idx)

        high_valid = his_high.notna() & his_high.gt(0) & close.notna() & close.gt(0)
        dist_to_high = pd.Series(np.nan, index=idx)
        dist_to_high[high_valid] = (
            (his_high[high_valid] - close[high_valid]) / his_high[high_valid] * 100
        )
        signals["dist_to_high"] = dist_to_high
        dist_high_norm = dist_to_high / range_pct
        signals["dist_high_pct"] = _pct_rank(dist_high_norm, idx)

        # 单日无趋势，百分位信号给中位值
        signals["wr_change"] = pd.Series(0.0, index=idx)
        signals["wr_change_pct"] = pd.Series(50.0, index=idx)
        signals["cost50_trend"] = pd.Series(0.0, index=idx)
        signals["cost50_pct"] = pd.Series(50.0, index=idx)

        upper_range = (cost_85 - cost_50).abs()
        lower_range = (cost_50 - cost_15).abs()
        lower_safe = lower_range.replace(0, np.nan)
        chip_skew = upper_range / lower_safe
        signals["chip_skew"] = chip_skew.fillna(1.0).clip(0, 10)
        signals["skew_pct"] = _pct_rank(chip_skew, idx)

        return signals

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        scores = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return scores

        signals = self._compute_signals(df)
        if not signals:
            return scores

        # ================================================================
        # 绝对值信号（保持不变）
        # ================================================================
        scores = scores + signals.get("wr_moderate", pd.Series(0.0, index=df.index))
        scores = scores + signals.get("wr_deep", pd.Series(0.0, index=df.index))
        scores = scores - signals.get("wr_pressure", pd.Series(0.0, index=df.index))

        # 筹码集中度（保持百分位分档）
        conc_pct = signals.get("conc_pct", pd.Series(50.0, index=df.index))
        scores.loc[conc_pct > 80] += 10.0
        scores.loc[(conc_pct > 60) & (conc_pct <= 80)] += 5.0

        # ================================================================
        # 百分位信号（替换硬阈值）
        # ================================================================

        # 距历史低点 (0-15)：越近分位越高
        dist_low_pct = signals.get("dist_low_pct", pd.Series(50.0, index=df.index))
        scores.loc[dist_low_pct > 80] += 15.0
        scores.loc[(dist_low_pct > 60) & (dist_low_pct <= 80)] += 7.0

        # 距历史高点 (-15-0)：越近分位越低
        dist_high_pct = signals.get("dist_high_pct", pd.Series(50.0, index=df.index))
        scores.loc[dist_high_pct < 20] -= 15.0
        scores.loc[(dist_high_pct >= 20) & (dist_high_pct < 40)] -= 7.0

        # 成本中轴上移 (0-10)：互斥分档，修复重叠
        cost50_pct = signals.get("cost50_pct", pd.Series(50.0, index=df.index))
        scores.loc[cost50_pct > 80] += 10.0
        scores.loc[(cost50_pct > 50) & (cost50_pct <= 80)] += 5.0

        # 筹码偏斜 (±5)
        skew_pct = signals.get("skew_pct", pd.Series(50.0, index=df.index))
        scores.loc[skew_pct > 80] += 5.0
        scores.loc[skew_pct < 20] -= 5.0

        # ================================================================
        # wr_change 洗盘/追高（百分位 + 确认）
        # ================================================================
        wr_change_pct = signals.get("wr_change_pct", pd.Series(50.0, index=df.index))
        cost50_trend = signals.get("cost50_trend", pd.Series(0.0, index=df.index))

        # 洗盘：wr 大幅下降 + 成本中轴未降（确认非真出逃）
        cost_stable = cost50_trend >= 0
        cost_declining = cost50_trend < 0
        is_washout = (wr_change_pct > 80) & cost_stable
        scores.loc[is_washout] += 10.0
        scores.loc[(wr_change_pct > 60) & (wr_change_pct <= 80) & cost_stable] += 5.0

        # 量价齐跌真出逃：wr 大幅下降 + 成本中轴也在降
        is_outflow = (wr_change_pct > 80) & cost_declining
        scores.loc[is_outflow] -= 10.0
        scores.loc[(wr_change_pct > 60) & (wr_change_pct <= 80) & cost_declining] -= 5.0

        # 追高：wr 快速上升
        scores.loc[wr_change_pct < 20] -= 10.0
        scores.loc[(wr_change_pct >= 20) & (wr_change_pct < 40)] -= 5.0

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
        wr_change_pct = signals.get("wr_change_pct", pd.Series(50.0, index=df.index))
        conc_pct = signals.get("conc_pct", pd.Series(50.0, index=df.index))
        dist_to_low = signals.get("dist_to_low", pd.Series(np.nan, index=df.index))
        dist_low_pct = signals.get("dist_low_pct", pd.Series(50.0, index=df.index))
        dist_to_high = signals.get("dist_to_high", pd.Series(np.nan, index=df.index))
        dist_high_pct = signals.get("dist_high_pct", pd.Series(50.0, index=df.index))
        cost50_trend = signals.get("cost50_trend", pd.Series(0.0, index=df.index))
        cost50_pct = signals.get("cost50_pct", pd.Series(50.0, index=df.index))
        chip_skew = signals.get("chip_skew", pd.Series(1.0, index=df.index))
        skew_pct = signals.get("skew_pct", pd.Series(50.0, index=df.index))
        avg_range = df.get("avg_range", pd.Series(np.nan, index=df.index))

        # 取 latest winner_rate 用于显示
        day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
        ndays = len(set(c.split("_")[0] for c in day_cols))
        if ndays >= 1:
            last = ndays - 1
            wr_last = df.get(f"d{last}_winner_rate", pd.Series(50.0, index=df.index)).fillna(50.0)
        elif "winner_rate" in df.columns:
            wr_last = df["winner_rate"]
        else:
            wr_last = pd.Series(50.0, index=df.index)

        for ts_code in scores.index:
            if scores[ts_code] < self._LABEL_THRESHOLD:
                continue
            r = []
            wr = wr_last.get(ts_code, 50)

            # 获利结构
            if wr_deep.get(ts_code, 0) > 0:
                r.append(f"深度套牢(获利{wr:.0f}%)，反弹潜力大")
            elif wr_moderate.get(ts_code, 0) > 5:
                r.append(f"获利适中({wr:.0f}%)，抛压不大")
            elif wr_pressure.get(ts_code, 0) > 0:
                r.append(f"获利盘过大({wr:.0f}%)，注意抛压")

            # 筹码集中度
            cp = conc_pct.get(ts_code, 50)
            if cp > 80:
                r.append(f"筹码高度集中({cp:.0f}分位)")
            elif cp > 60:
                r.append(f"筹码较集中({cp:.0f}分位)")

            # 洗盘 / 量价齐跌 / 追高
            wrc = wr_change.get(ts_code, 0)
            wrc_pct = wr_change_pct.get(ts_code, 50)
            c50t = cost50_trend.get(ts_code, 0)
            if wrc_pct > 80 and c50t >= 0:
                r.append(f"获利盘快速出清({wrc:.0f}%)，疑似洗盘")
            elif wrc_pct > 80 and c50t < 0:
                r.append(f"获利盘出逃+成本下移({wrc:.0f}%)，量价齐跌")
            elif wrc_pct > 60 and c50t < 0:
                r.append(f"获利盘流出+成本松动({wrc:.0f}%)，疑似出逃")
            elif wrc_pct < 20:
                r.append(f"获利盘快速堆积({wrc:.0f}%)，追高风险")

            # 距历史低点（波动率归一化后）
            dtl = dist_to_low.get(ts_code, np.nan)
            dlp = dist_low_pct.get(ts_code, 50)
            ar = avg_range.get(ts_code, np.nan)
            if not np.isnan(dtl) and dlp > 80:
                rng_info = f"，日均振幅{ar*100:.1f}%" if not np.isnan(ar) else ""
                r.append(f"距历史成本低点{dtl:.0f}%{rng_info}，强反弹信号")
            elif not np.isnan(dtl) and dlp > 60:
                r.append(f"距历史成本低点{dtl:.0f}%，有反弹空间")

            # 距历史高点（波动率归一化后）
            dth = dist_to_high.get(ts_code, np.nan)
            dhp = dist_high_pct.get(ts_code, 50)
            if not np.isnan(dth) and dhp < 20:
                rng_info = f"，日均振幅{ar*100:.1f}%" if not np.isnan(ar) else ""
                r.append(f"距历史成本高点{dth:.0f}%{rng_info}，高位风险")

            # 成本中轴趋势
            c50p = cost50_pct.get(ts_code, 50)
            if c50p > 80:
                r.append(f"成本中轴强势上移({c50t:.0f}%)，资金抬轿")
            elif c50p > 50:
                r.append(f"成本中轴小幅上移({c50t:.0f}%)")

            # 筹码偏斜
            sk = chip_skew.get(ts_code, 1.0)
            sp = skew_pct.get(ts_code, 50)
            if sp > 80:
                r.append("上方筹码松散，卖压分散")
            elif sp < 20:
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
