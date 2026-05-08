# -*- coding: utf-8 -*-
"""板块热度因子 (Sector Heat Factor).

基于今日实时涨停数据识别涨停候选，输出板块热度评分作为选股范围权重。
盘中优先使用 akshare 实时行情，Tushare limit_list_d 作为回退。
盘中可用，盘后不可用（盘后有独立的涨跌停因子）。
"""

import logging
import time
from typing import Dict, List, Optional

import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class SectorFactor(BaseFactor):
    """板块热度因子。

    盘中优先使用 akshare stock_zt_pool_em 涨停池（含连板数），
    偶数槽刷新（60s 间隔），奇数槽复用缓存。
    降级到实时行情 pct_chg 过滤，再降级到 Tushare limit_list_d。
    """

    name = "sector"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    def __init__(self):
        super().__init__()
        self._zt_pool_cache: Optional[pd.DataFrame] = None
        self._last_zt_slot: int = -1
        # 所属行业映射 {裸代码: 行业名}，供 engine 读取
        self.sector_map: Dict[str, str] = {}

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取涨停候选数据。

        1. 偶数槽 → stock_zt_pool_em 涨停池（每60s刷新）
        2. 奇数槽 → 复用上一轮涨停池缓存
        3. 降级 → akshare 实时行情 pct_chg >= 9.5%
        4. 再降级 → Tushare limit_list_d
        """
        slot = int(time.time() // 30)

        # ── 偶数槽：拉取 stock_zt_pool_em ──
        if slot % 2 == 0 and slot != self._last_zt_slot:
            df = self._fetch_zt_pool(trade_date)
            if df is not None and not df.empty:
                return df
            logger.info("[SectorFactor] 涨停池拉取失败，降级到实时行情")

        # ── 奇数槽：复用缓存 ──
        if self._zt_pool_cache is not None and not self._zt_pool_cache.empty:
            logger.debug("[SectorFactor] 复用涨停池缓存 (slot=%d)", slot)
            return self._zt_pool_cache

        # ── 降级：实时行情 pct_chg 过滤 ──
        try:
            from src.discovery.realtime_spot import get_provider
            provider = get_provider()
            spot_df = provider.fetch()
            if spot_df is not None and not spot_df.empty:
                pct = spot_df["pct_chg"]
                limit_up = spot_df[pct >= 9.5].copy()
                if not limit_up.empty:
                    limit_up = self._with_ts_code_index(limit_up)
                    logger.info(
                        "[SectorFactor] 实时行情涨停候选: %d 只 (pct_chg >= 9.5%%)",
                        len(limit_up),
                    )
                    return limit_up
                logger.debug("[SectorFactor] 实时行情无涨停候选")
        except Exception as e:
            logger.warning("[SectorFactor] 实时行情获取失败，回退 Tushare: %s", e)

        # ── 再降级：Tushare limit_list_d ──
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        df = tushare_fetcher.get_limit_list(trade_date, limit_type="U")
        if df is not None and not df.empty:
            df = df.copy()
            if "limit_times" in df.columns:
                df["is_leader"] = df["limit_times"] >= 3
                df["is_2board"] = df["limit_times"] == 2
                df["is_first"] = df["limit_times"] == 1
        return df

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        """返回涨停候选评分。

        涨停池数据: limit_times >= 3 → 60（龙头），==2 → 40，==1 → 20。
        实时行情降级: pct_chg * 5.0，封顶 50。
        Tushare 降级: limit_times * 5.0，封顶 100。
        """
        result = pd.Series(0.0, index=df.index, name=self.name)

        if df.empty:
            return result

        if "limit_times" in df.columns:
            lt = df["limit_times"].fillna(0).clip(0, 5)
            result = lt * 20.0  # 1→20, 2→40, 3→60, 4→80, 5→100
        elif "pct_chg" in df.columns:
            pct = df["pct_chg"].fillna(0).clip(lower=0)
            result = (pct * 5.0).clip(0, 50)

        return result.clip(0, 100)

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        """生成各股票上榜理由。"""
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        is_zt_pool = "limit_times" in df.columns and "首次封板时间" in df.columns
        is_realtime = "pct_chg" in df.columns

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []

            if is_zt_pool:
                lt = int(df.loc[ts_code, "limit_times"]) if ts_code in df.index else 0
                if lt >= 3:
                    r.append(f"连板龙头({lt}连板)")
                elif lt >= 2:
                    r.append("2连板")
                elif lt == 1:
                    r.append("首板涨停")
                seal_time = str(df.loc[ts_code, "首次封板时间"]) if ts_code in df.index else ""
                if seal_time and seal_time != "nan":
                    r.append(f"封板 {seal_time}")
                break_count = (
                    int(df.loc[ts_code, "炸板次数"]) if ts_code in df.index and "炸板次数" in df.columns else 0
                )
                if break_count > 0:
                    r.append(f"炸板{break_count}次")
            elif is_realtime:
                pct_val = float(df.loc[ts_code, "pct_chg"]) if ts_code in df.index else 0.0
                if pct_val >= 10.0:
                    r.append(f"盘中涨停({pct_val:.1f}%)")
                else:
                    r.append(f"逼近涨停({pct_val:.1f}%)")
            else:
                lt = int(df.loc[ts_code, "limit_times"]) if ts_code in df.index and "limit_times" in df.columns else 0
                if lt >= 3:
                    r.append(f"板块龙头({lt}连板)")
                elif lt >= 2:
                    r.append(f"板块连板({lt}连板)")
                elif lt == 1:
                    r.append("板块首板")

            if r:
                reasons[ts_code] = r
        return reasons

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_zt_pool(self, trade_date: str) -> Optional[pd.DataFrame]:
        """调用 stock_zt_pool_em 获取涨停池，转为 ts_code 索引的 DataFrame。"""
        try:
            import akshare as ak

            logger.debug("[SectorFactor] 拉取 stock_zt_pool_em (date=%s)", trade_date)
            df = ak.stock_zt_pool_em(date=trade_date)
            if df is None or df.empty:
                return None

            df = df.copy()
            # 重命名列为统一格式
            col_map = {
                "代码": "code",
                "名称": "name",
                "涨跌幅": "pct_chg",
                "最新价": "price",
                "连板数": "limit_times",
                "所属行业": "sector",
                "首次封板时间": "首次封板时间",
                "最后封板时间": "最后封板时间",
                "炸板次数": "炸板次数",
                "涨停统计": "涨停统计",
            }
            df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

            # 构建 sector_map {裸代码: 行业}
            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip().zfill(6)
                sec = str(row.get("sector", "")).strip()
                if code and sec:
                    self.sector_map[code] = sec

            # 转为 ts_code 索引 (e.g. 000839 → 000839.SZ)
            df = df.set_index("code")
            df = self._with_ts_code_index(df)

            # 缓存
            self._zt_pool_cache = df
            self._last_zt_slot = int(time.time() // 30)

            logger.info(
                "[SectorFactor] 涨停池: %d 只, 龙头(≥3板)=%d, 2板=%d, 首板=%d",
                len(df),
                len(df[df["limit_times"] >= 3]),
                len(df[df["limit_times"] == 2]),
                len(df[df["limit_times"] == 1]),
            )
            return df
        except Exception as e:
            logger.warning("[SectorFactor] stock_zt_pool_em 异常: %s", e)
            return None

    @staticmethod
    def _with_ts_code_index(df: pd.DataFrame) -> pd.DataFrame:
        """将裸代码索引转为 ts_code 格式 (e.g. '600519' -> '600519.SH')。"""
        df = df.copy()
        new_index = []
        for code in df.index:
            code_str = str(code).strip().zfill(6)
            if code_str.startswith(("60", "68")):
                new_index.append(f"{code_str}.SH")
            elif code_str.startswith(("00", "30")):
                new_index.append(f"{code_str}.SZ")
            elif code_str.startswith(("4", "8", "92")):
                new_index.append(f"{code_str}.BJ")
            else:
                new_index.append(code_str)
        df.index = new_index
        return df
