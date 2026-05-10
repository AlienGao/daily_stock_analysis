# -*- coding: utf-8 -*-
"""游资胜率跟踪器 (Hot Money Win-Rate Tracker).

基于历史 hm_detail 明细 + stock_daily 行情，统计各游资的 T+1 胜率，
输出 quality_score 供 HotMoneyFactor 加权使用。

每日更新：盘后 pipeline 自动调用 refresh_and_update() → 全量重算 → 写入 DB。
"""

import logging
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import select

from src.storage import DatabaseManager, StockDaily

logger = logging.getLogger(__name__)


class HmTracker:
    """游资历史胜率跟踪器。

    从 hm_detail 表读取全量游资交易记录，关联 stock_daily 计算 T+1 收益，
    按 hm_name 聚合输出胜率、平均收益、quality_score。

    持久化策略：
    - DB hm_quality 表：持久真源，每天全量覆盖
    - JSON data/hm_quality.json：快速读取缓存，供因子 score() 使用
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    # ------------------------------------------------------------------
    # compute
    # ------------------------------------------------------------------

    def compute_performance(
        self, start_date: str = "20220801", end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """计算各游资的历史胜率与质量评分。

        Returns:
            DataFrame index=hm_name, columns=[win_rate, avg_return, total_trades, quality_score]
        """
        df_hm = self.db.get_hm_detail_range(start_date=start_date, end_date=end_date)
        if df_hm.empty:
            logger.warning("[HmTracker] hm_detail 无数据")
            return pd.DataFrame()

        codes = sorted(df_hm.index.unique())
        trade_dates = sorted(df_hm["trade_date"].unique())
        min_date = trade_dates[0]
        max_date = trade_dates[-1]

        sd_min = pd.to_datetime(min_date, format="%Y%m%d")
        sd_max = pd.to_datetime(max_date, format="%Y%m%d") + pd.Timedelta(days=10)

        with self.db.get_session() as session:
            rows = (
                session.execute(
                    select(StockDaily).where(
                        StockDaily.code.in_(codes),
                        StockDaily.date >= sd_min.date(),
                        StockDaily.date <= sd_max.date(),
                    )
                )
                .scalars()
                .all()
            )

        if not rows:
            logger.warning("[HmTracker] stock_daily 无匹配数据")
            return pd.DataFrame()

        price: Dict[str, float] = {}
        for r in rows:
            ds = r.date.strftime("%Y%m%d") if hasattr(r.date, "strftime") else str(r.date).replace("-", "")
            price[(r.code, ds)] = r.close

        date_by_code: Dict[str, list] = {}
        for r in rows:
            ds = r.date.strftime("%Y%m%d") if hasattr(r.date, "strftime") else str(r.date).replace("-", "")
            date_by_code.setdefault(r.code, []).append(ds)
        for c in date_by_code:
            date_by_code[c] = sorted(set(date_by_code[c]))

        records = []
        for idx, row in df_hm.iterrows():
            code = str(idx).strip()
            if not code:
                continue
            td = str(row["trade_date"])[:8]
            hm = str(row.get("hm_name", ""))
            if not hm or not td:
                continue

            close_t = price.get((code, td))
            if close_t is None or close_t <= 0:
                continue

            dates = date_by_code.get(code, [])
            try:
                date_idx = dates.index(td)
                if date_idx + 1 >= len(dates):
                    continue
                t1_date = dates[date_idx + 1]
            except (ValueError, IndexError):
                continue

            close_t1 = price.get((code, t1_date))
            if close_t1 is None or close_t1 <= 0:
                continue

            ret = (close_t1 - close_t) / close_t
            records.append({"hm_name": hm, "code": code, "trade_date": td, "return": ret})

        if not records:
            logger.warning("[HmTracker] 无有效的 T+1 收益记录")
            return pd.DataFrame()

        df_perf = pd.DataFrame(records)
        perf = df_perf.groupby("hm_name").agg(
            win_rate=("return", lambda x: (x > 0).mean()),
            avg_return=("return", "mean"),
            total_trades=("return", "count"),
        )

        win_pct = perf["win_rate"].rank(pct=True, na_option="bottom")
        ret_pct = perf["avg_return"].rank(pct=True, na_option="bottom")
        perf["quality_score"] = ((win_pct + ret_pct) / 2 * 100).round(1)

        perf = perf.sort_values("quality_score", ascending=False)
        logger.info(
            "[HmTracker] 统计完成: %d 家游资, %d 笔交易, quality 范围 [%.1f, %.1f]",
            len(perf), len(df_perf), perf["quality_score"].min(), perf["quality_score"].max(),
        )
        return perf

    # ------------------------------------------------------------------
    # persist
    # ------------------------------------------------------------------

    def save_quality(self, perf: pd.DataFrame) -> int:
        """保存质量评分到 DB hm_quality 表（全量覆盖）。"""
        return self.db.upsert_hm_quality(perf)

    @staticmethod
    def load_quality() -> Dict[str, float]:
        """从 DB 加载游资质量映射，返回 0-1 归一化的 {hm_name: quality_score}。

        无数据时返回空字典，因子侧默认值 0.5。
        """
        try:
            db = DatabaseManager()
            raw = db.get_all_hm_quality()
            # DB 存 0-100，归一化到 0-1
            return {k: v / 100 for k, v in raw.items()}
        except Exception as e:
            logger.warning("[HmTracker] DB 加载质量评分失败: %s", e)
            return {}

    # ------------------------------------------------------------------
    # daily refresh
    # ------------------------------------------------------------------

    def refresh_and_update(self) -> Optional[pd.DataFrame]:
        """每日盘后：全量重算胜率并写入 DB。"""
        perf = self.compute_performance()
        if perf.empty:
            logger.warning("[HmTracker] 当日无数据，跳过质量更新")
            return None
        self.save_quality(perf)
        return perf
