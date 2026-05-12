# -*- coding: utf-8 -*-
"""融资融券因子 (Margin Trading Factor).

盘后因子：杠杆资金 5 日趋势分析（需落库支持）。
数据来源: Tushare margin_detail (59) + daily_basic (市值归一化)
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor, safe_pct_change, pct_rank, safe_ratio

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
    _LABEL_THRESHOLD_RATIO = 0.5

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

        # 3. 拿市值做归一化（DB 优先）
        mv_series = None
        try:
            from src.storage import DatabaseManager
            db_basic = DatabaseManager().get_daily_basic(trade_date)
            if not db_basic.empty and "total_mv" in db_basic.columns:
                # DB 以 bare code 为 index，转 ts_code 格式
                mv = db_basic["total_mv"].copy()
                bare_codes = mv.index.astype(str).str.zfill(6)
                pre2 = bare_codes.str[:2]
                suffix_map = {
                    "60": ".SH", "68": ".SH", "00": ".SZ", "30": ".SZ",
                    "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ",
                }
                mv.index = bare_codes + pre2.map(suffix_map).fillna("")
                mv_series = mv
        except Exception:
            pass
        if mv_series is None:
            daily_basic = tushare_fetcher.get_daily_basic_all(trade_date)
            if daily_basic is not None and not daily_basic.empty:
                mv_series = daily_basic.get("total_mv")

        # 4. 组装宽表：每日期一行 → 每日一列
        margin_df = margin_df.reset_index()
        if "code" in margin_df.columns and "ts_code" not in margin_df.columns:
            margin_df = margin_df.rename(columns={"code": "ts_code"})
            # DB returns bare codes; convert to ts_code format for mv_series lookup
            codes = margin_df["ts_code"].astype(str).str.zfill(6)
            pre2 = codes.str[:2]
            suffix = pre2.map(
                {"60": ".SH", "68": ".SH", "00": ".SZ", "30": ".SZ",
                 "43": ".BJ", "83": ".BJ", "87": ".BJ", "92": ".BJ"}
            ).fillna("")
            margin_df["ts_code"] = codes + suffix
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
            for col in ["rzye", "rzmre", "rzche", "rqye", "rqmcl", "rqchl", "rqyl"]:
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

        # 过滤 ETF（5 开头 + 15/16 开头），仅保留 A 股
        bare_codes = result.index.astype(str).str.split(".").str[0].str.zfill(6)
        is_stock = bare_codes.str.match(r"^(60|68|00|30|43|83|87|92)")
        result = result[is_stock]

        logger.info(
            f"[MarginFactor] 组装完成: {len(target_dates)} 日, "
            f"{len(result)} 只股票 (过滤 ETF 后)"
        )
        return result

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 8 个子信号，各自已归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
        ndays = len(set(c.split("_")[0] for c in day_cols))
        if ndays < 2:
            return self._compute_signals_single_day(df)

        last = ndays - 1
        first = 0

        total_mv = df.get("total_mv")
        has_mv = total_mv is not None and total_mv.notna().any()

        rzye_last = df.get(f"d{last}_rzye", zeros)
        rzye_first = df.get(f"d{first}_rzye", zeros)
        rzmre_last = df.get(f"d{last}_rzmre", zeros)
        rzmre_first = df.get(f"d{first}_rzmre", zeros)
        rzche_last = df.get(f"d{last}_rzche", zeros)
        rzche_first = df.get(f"d{first}_rzche", zeros)
        rqye_last = df.get(f"d{last}_rqye", zeros)
        rqmcl_last = df.get(f"d{last}_rqmcl", zeros)
        rqmcl_first = df.get(f"d{first}_rqmcl", zeros)

        # 5 日增幅
        rzmre_growth = safe_pct_change(rzmre_last, rzmre_first)
        rzche_growth = safe_pct_change(rzche_last, rzche_first)
        rqmcl_growth = safe_pct_change(rqmcl_last, rqmcl_first)

        # 融资买入活跃度
        rzye_safe = rzye_last.replace(0, np.nan)
        margin_ratio = (rzmre_last / rzye_safe) * 100
        margin_ratio_first = (rzmre_first / rzye_first.replace(0, np.nan)) * 100
        margin_ratio_trend = safe_pct_change(
            margin_ratio.fillna(0), margin_ratio_first.fillna(0)
        )

        # 市值归一化（无市值时用绝对值排名作为代理）
        if has_mv:
            rzye_ratio_pct = pct_rank(safe_ratio(rzye_last, total_mv) * 100, idx)
            rzmre_ratio_pct = pct_rank(safe_ratio(rzmre_last, total_mv) * 100, idx)
            rqye_ratio_pct = pct_rank(safe_ratio(rqye_last, total_mv) * 100, idx)
        else:
            rzye_ratio_pct = pct_rank(rzye_last, idx)
            rzmre_ratio_pct = pct_rank(rzmre_last, idx)
            rqye_ratio_pct = pct_rank(rqye_last, idx)

        signals: Dict[str, pd.Series] = {}

        # 1. 融资买入额增长 (0-20): 0%→0, +100%→20
        s = zeros.copy()
        pos = rzmre_growth > 0
        s.loc[pos] = (rzmre_growth[pos].clip(0, 100) / 100 * 20).clip(0, 20)
        signals["margin_buy_growth"] = s

        # 2. 融资买入活跃度 (0-20): 分位 50→0, 100→20（rzmre=0 不触发）
        s = zeros.copy()
        hi = (rzmre_ratio_pct > 50) & (rzmre_last > 0)
        s.loc[hi] = ((rzmre_ratio_pct[hi] - 50) / 50 * 20).clip(0, 20)
        signals["margin_buy_active"] = s

        # 3. 融资偿还下降 (0-15): 0%→0, -100%→15
        s = zeros.copy()
        neg = rzche_growth < 0
        s.loc[neg] = ((-rzche_growth[neg]).clip(0, 100) / 100 * 15).clip(0, 15)
        signals["repay_decline"] = s

        # 4. 买入占比趋势上升 (0-20): 0%→0, +100%→20
        s = zeros.copy()
        pos = margin_ratio_trend > 0
        s.loc[pos] = (margin_ratio_trend[pos].clip(0, 100) / 100 * 20).clip(0, 20)
        signals["ratio_trend"] = s

        # 5. 融资余额市值比 (0-25): 分位 50→0, 100→25（rzye=0 不触发）
        s = zeros.copy()
        hi = (rzye_ratio_pct > 50) & (rzye_last > 0)
        s.loc[hi] = ((rzye_ratio_pct[hi] - 50) / 50 * 25).clip(0, 25)
        signals["balance_ratio"] = s

        # 6. 融券卖出 (-10-0): 有→-10
        s = zeros.copy()
        s.loc[rqmcl_last > 0] = -10.0
        signals["short_selling"] = s

        # 7. 融券占比偏高 (-15-0): 分位 50→0, 100→-15（rqye=0 不触发）
        s = zeros.copy()
        hi = (rqye_ratio_pct > 50) & (rqye_last > 0)
        s.loc[hi] = -((rqye_ratio_pct[hi] - 50) / 50 * 15).clip(0, 15)
        signals["short_ratio"] = s

        # 8. 融券卖出量下降→空头平仓 (0-10): -20%→+2, -100%→+10
        s = zeros.copy()
        covering = (rqmcl_growth < -20) & (rqmcl_first > 0)
        s.loc[covering] = ((-rqmcl_growth[covering]).clip(0, 100) / 100 * 10).clip(0, 10)
        signals["short_covering"] = s

        # 9. 买入占比快速萎缩 (-20-0): -5%→0, -100%→-20
        s = zeros.copy()
        crash = margin_ratio_trend < -5
        s.loc[crash] = -(margin_ratio_trend[crash].abs().clip(0, 100) / 100 * 20)
        signals["ratio_crash"] = s

        return signals

    def _compute_signals_single_day(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """单日退化版信号，用绝对值代理趋势信号（上限折半以反映不确定性）。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        total_mv = df.get("total_mv")
        has_mv = total_mv is not None and total_mv.notna().any()

        rzmre = df.get("d0_rzmre", zeros)
        rzche = df.get("d0_rzche", zeros)
        rqmcl = df.get("d0_rqmcl", zeros)
        rqye = df.get("d0_rqye", zeros)
        rzye = df.get("d0_rzye", zeros)

        if has_mv:
            rzye_ratio_pct = pct_rank(safe_ratio(rzye, total_mv) * 100, idx)
            rzmre_ratio_pct = pct_rank(safe_ratio(rzmre, total_mv) * 100, idx)
            rqye_ratio_pct = pct_rank(safe_ratio(rqye, total_mv) * 100, idx)
        else:
            rzye_ratio_pct = pct_rank(rzye, idx)
            rzmre_ratio_pct = pct_rank(rzmre, idx)
            rqye_ratio_pct = pct_rank(rqye, idx)

        signals: Dict[str, pd.Series] = {}

        # 1. margin_buy_growth (0-20): 绝对值在全市场百分位 → 代理买入增长 (0-10)
        s = zeros.copy()
        hi = rzmre_ratio_pct > 50
        s.loc[hi] = ((rzmre_ratio_pct[hi] - 50) / 50 * 10).clip(0, 10)
        signals["margin_buy_growth"] = s

        # 2. margin_buy_active (0-20)（rzmre=0 不触发）
        s = zeros.copy()
        hi = (rzmre_ratio_pct > 50) & (rzmre > 0)
        s.loc[hi] = ((rzmre_ratio_pct[hi] - 50) / 50 * 20).clip(0, 20)
        signals["margin_buy_active"] = s

        # 3. repay_decline (0-15): 当日净买入 (rzmre - rzche) / (rzmre + rzche) → 代理偿还下降 (0-8)
        s = zeros.copy()
        total_margin = rzmre + rzche
        net = (rzmre - rzche) / total_margin.replace(0, np.nan)
        pos = net.fillna(0) > 0
        s.loc[pos] = (net[pos].clip(0, 1) * 8).clip(0, 8)
        signals["repay_decline"] = s

        # 4. ratio_trend (0-20): 当前买入/余额比在全市场百分位 → 代理趋势 (0-10)
        s = zeros.copy()
        rzye_safe = rzye.replace(0, np.nan)
        margin_ratio = (rzmre / rzye_safe * 100).fillna(0)
        ratio_pct = pct_rank(margin_ratio, idx)
        hi = ratio_pct > 50
        s.loc[hi] = ((ratio_pct[hi] - 50) / 50 * 10).clip(0, 10)
        signals["ratio_trend"] = s

        # 5. balance_ratio (0-25)（rzye=0 不触发）
        s = zeros.copy()
        hi = (rzye_ratio_pct > 50) & (rzye > 0)
        s.loc[hi] = ((rzye_ratio_pct[hi] - 50) / 50 * 25).clip(0, 25)
        signals["balance_ratio"] = s

        # 6. short_selling (-10-0)
        s = zeros.copy()
        s.loc[rqmcl > 0] = -10.0
        signals["short_selling"] = s

        # 7. short_ratio (-15-0)（rqye=0 不触发）
        s = zeros.copy()
        hi = (rqye_ratio_pct > 50) & (rqye > 0)
        s.loc[hi] = -((rqye_ratio_pct[hi] - 50) / 50 * 15).clip(0, 15)
        signals["short_ratio"] = s

        # 8. short_covering (0-10): 单日无法检测趋势
        signals["short_covering"] = zeros.copy()

        # 9. ratio_crash (-20-0): 单日无法检测趋势崩溃，保持 0
        signals["ratio_crash"] = zeros.copy()

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
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        signals = self._compute_signals(df)

        signal_meta = [
            ("margin_buy_growth", "融资买入额5日增长"),
            ("margin_buy_active", "融资买入活跃"),
            ("repay_decline", "融资偿还下降"),
            ("ratio_trend", "买入占比趋势上升"),
            ("balance_ratio", "融资余额市值比高"),
            ("short_selling", "融券卖出"),
            ("short_ratio", "融券占比偏高"),
            ("short_covering", "融券卖出下降(空头平仓)"),
            ("ratio_crash", "买入占比快速萎缩"),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue

            labels: List[str] = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                # 正信号阈值 >= max*threshold, 负信号阈值 <= max*threshold (绝对值)
                abs_max = {
                    "margin_buy_growth": 20, "margin_buy_active": 20,
                    "repay_decline": 15, "ratio_trend": 20, "balance_ratio": 25,
                    "short_selling": 10, "short_ratio": 15, "short_covering": 10, "ratio_crash": 20,
                }[key]
                if key in ("short_selling", "short_ratio") or key == "ratio_crash":
                    if abs(val) < abs_max * threshold:
                        continue
                else:
                    if val < abs_max * threshold:
                        continue

                if key == "balance_ratio":
                    # 附加上分位信息
                    day_cols = [c for c in df.columns if c.startswith("d") and "_" in c]
                    ndays = len(set(c.split("_")[0] for c in day_cols))
                    last = max(0, ndays - 1)
                    total_mv = df.get("total_mv")
                    if total_mv is not None and total_mv.notna().any():
                        rzye_last = df.get(f"d{last}_rzye", pd.Series(0.0, index=df.index))
                        pct = pct_rank(safe_ratio(rzye_last, total_mv) * 100, df.index)
                        pct_val = pct.get(ts_code, 50)
                        labels.append(f"{label}({pct_val:.0f}分位)")
                    else:
                        labels.append(label)
                else:
                    labels.append(label)

            if labels:
                reasons[ts_code] = labels
