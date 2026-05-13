# -*- coding: utf-8 -*-
"""排名动量因子 (Ranking Momentum Factor).

检测股票涨跌幅在全市场横截面百分位排名的纵向趋势。
不依赖扫描最终排名（避免循环），用 stock_daily 的日涨跌幅做跨日横截面比较：
排名持续上升 → 资金在持续涌入 → 早期发现潜力股。

盘中可用，盘后不可用（盘后有完整的技术面因子体系）。
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.discovery.factors.base import BaseFactor, bare_to_ts_code

logger = logging.getLogger(__name__)


class RankingMomentumFactor(BaseFactor):
    """排名动量因子。

    纵向对比每只股票过去 N 天的涨跌幅在全市场的百分位排名，
    检测排名持续上升的股票，给早期蓄力股加分。
    """

    name = "ranking_momentum"
    available_intraday = True
    available_postmarket = False
    weight = 15.0

    _LOOKBACK_TRADING_DAYS = 6     # 回看交易日数（天然免疫假期）
    _MIN_TRADING_DAYS = 3          # 最少需要的历史交易日数
    _RANK_FLAT_TOLERANCE = 2.0     # 排名百分位波动容忍带 (pp)，±2pp 内视为平盘

    def __init__(self):
        super().__init__()
        self._rank_trend_cache: Dict[str, Dict] = {}  # {bare_code: {slope, consecutive, ...}}

    # ------------------------------------------------------------------
    # fetch_data
    # ------------------------------------------------------------------

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取过去 N 天每只股票的日涨跌幅 + 当日实时涨跌幅。

        Returns:
            DataFrame index=ts_code, columns=[d0, d1, ..., dN]
            其中 d0=今天（realtime_spot），d1-dN=历史（stock_daily）
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        target_dt = datetime.strptime(trade_date, "%Y%m%d").date()

        # ── 1. 历史 pct_chg ──
        with db.get_session() as s:
            # 先取最近 N 个交易日（免疫假期/停牌）
            trading_dates = [
                row[0] for row in s.execute(
                    text(
                        "SELECT DISTINCT date FROM stock_daily "
                        "WHERE date < :target ORDER BY date DESC LIMIT :limit"
                    ),
                    {"target": target_dt, "limit": self._LOOKBACK_TRADING_DAYS},
                ).fetchall()
            ]
            if not trading_dates:
                logger.warning("[RankingMomentum] stock_daily 无交易日数据 (target=%s)", target_dt)
                return None

            rows = s.execute(
                text(
                    "SELECT code, date, pct_chg FROM stock_daily "
                    "WHERE date IN :dates ORDER BY code, date DESC"
                ),
                {"dates": tuple(trading_dates)},
            ).fetchall()

        if not rows:
            logger.warning("[RankingMomentum] stock_daily 无数据 (%s ~ %s)", cutoff, target_dt)
            return None

        hist = pd.DataFrame(rows, columns=["code", "date", "pct_chg"])
        hist = hist.drop_duplicates(subset=["code", "date"], keep="last")

        hist_dates = sorted(hist["date"].unique(), reverse=True)

        # ── 2. 当日 pct_chg（realtime_spot） ──
        spot = db.get_realtime_spot()
        if spot is None or spot.empty:
            logger.warning("[RankingMomentum] realtime_spot 无数据")
            return None

        spot_pct = spot[["pct_chg"]].copy()
        spot_pct.index = spot_pct.index.astype(str).str.strip().str.zfill(6)
        spot_pct = spot_pct[~spot_pct.index.duplicated(keep="first")]

        # ── 3. 构建每只股票的时序 pct_chg ──
        codes = sorted(set(hist["code"].unique()) | set(spot_pct.index))

        records: Dict[str, Dict[str, float]] = {}
        for code in codes:
            records[code] = {}

        # 今日
        for code in spot_pct.index:
            records.setdefault(code, {})["d0"] = float(spot_pct.loc[code, "pct_chg"])

        # 历史
        for _, row in hist.iterrows():
            code = str(row["code"]).strip().zfill(6)
            row_date = row["date"]
            if isinstance(row_date, pd.Timestamp):
                row_date = row_date.date()
            day_idx = None
            for j, d in enumerate(hist_dates):
                if row_date == d:
                    day_idx = j + 1  # d1, d2, ...
                    break
            if day_idx is None:
                continue
            label = f"d{day_idx}"
            records.setdefault(code, {})[label] = float(row["pct_chg"])

        # 转换为 DataFrame
        df = pd.DataFrame.from_dict(records, orient="index")
        df.index.name = "code"

        day_cols = [c for c in df.columns if c.startswith("d")]
        # 数据不足的股票保留但得 0 分（score() 内处理），避免硬过滤导致样本缩水

        df.index = [bare_to_ts_code(c) for c in df.index]
        df.index.name = "ts_code"

        logger.info(
            "[RankingMomentum] 数据就绪: %d 只股票, %d 天 (d0~d%d)",
            len(df), len(day_cols), len(hist_dates),
        )
        return df

    # ------------------------------------------------------------------
    # score
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df is None or df.empty:
            return pd.Series(dtype=float, name=self.name)

        day_cols = sorted(
            [c for c in df.columns if c.startswith("d") and c[1:].isdigit()],
            key=lambda x: int(x[1:]),
        )
        if len(day_cols) < self._MIN_TRADING_DAYS:
            return pd.Series(0.0, index=df.index, name=self.name)

        idx = df.index
        scores = pd.Series(0.0, index=idx)
        self._rank_trend_cache.clear()

        # ── 每天做横截面百分位排名 ──
        rank_pcts: Dict[str, pd.Series] = {}
        for col in day_cols:
            raw = df[col].dropna()
            if len(raw) < 100:
                rank_pcts[col] = pd.Series(50.0, index=idx)
                continue
            rank_pcts[col] = raw.rank(pct=True) * 100.0
            rank_pcts[col] = rank_pcts[col].reindex(idx)

        # ── 逐股打分 ──
        for ts_code in idx:
            ranks = []
            for col in day_cols:
                v = rank_pcts[col].get(ts_code, np.nan)
                if pd.notna(v):
                    ranks.append(v)

            if len(ranks) < self._MIN_TRADING_DAYS:
                continue

            # 当前百分位 (d0)
            current_pct = ranks[0] if ranks else 50.0

            # --- 1. 趋势斜率 (0-40) ---
            # ranks 是 d0→dN（最新在前）
            # 逐日差分取中位数，抗除权除息等单日毛刺
            diffs = [ranks[i] - ranks[i + 1] for i in range(len(ranks) - 1)]
            slope = float(np.median(diffs)) if diffs else 0.0

            if slope > 8:
                slope_score = 40.0
            elif slope > 2:
                slope_score = 10.0 + (slope - 2.0) / 6.0 * 30.0
            elif slope > 0:
                slope_score = slope / 2.0 * 10.0
            else:
                slope_score = 0.0

            # --- 2. 连续上升天数 (0-30) ---
            # 容忍带：±2pp 内视为平盘，不打断连涨但不计次，排除 73.2%→73.0% 这类噪声
            consecutive = 0
            tol = self._RANK_FLAT_TOLERANCE
            for j in range(len(ranks) - 1):
                delta = ranks[j] - ranks[j + 1]
                if delta > tol:
                    consecutive += 1
                elif delta < -tol:
                    break  # 真下跌，断连
                # else: |delta| <= tol，平盘，继续但不计数

            if consecutive >= 4:
                consec_score = 30.0
            elif consecutive == 3:
                consec_score = 20.0
            elif consecutive == 2:
                consec_score = 10.0
            else:
                consec_score = 0.0

            # --- 3. 当前百分位 (0-30) ---
            pos_score = min(30.0, current_pct * 0.3)

            total = slope_score + consec_score + pos_score

            # 涨停 → 扣分（避免追板，让因子在涨停前发挥作用）
            d0_pct = df.loc[ts_code, day_cols[0]] if day_cols else 0
            if pd.notna(d0_pct) and d0_pct >= 9.8:
                total = max(0, total - 40)

            total = max(0.0, min(100.0, total))
            scores.loc[ts_code] = total

            bare = str(ts_code).split(".")[0] if "." in str(ts_code) else str(ts_code)
            self._rank_trend_cache[bare] = {
                "slope": round(slope, 2),
                "consecutive": consecutive,
                "current_pct": round(current_pct, 1),
                "score": round(total, 2),
            }

        scores.name = self.name
        return scores

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        if df is None or df.empty:
            return {}

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            bare = str(ts_code).split(".")[0] if "." in str(ts_code) else str(ts_code)
            info = self._rank_trend_cache.get(bare, {})
            if not info:
                continue
            labels = []
            if info.get("consecutive", 0) >= 2:
                labels.append(f"排名连续上升({info['consecutive']}天)")
            if info.get("slope", 0) > 2:
                labels.append(f"趋势加速↑({info['slope']:.1f}%/天)")
            if info.get("current_pct", 0) > 60:
                labels.append(f"今日强于{info['current_pct']:.0f}%个股")
            if labels:
                reasons[ts_code] = labels
        return reasons
