# -*- coding: utf-8 -*-
"""板块热度因子 (Sector Heat Factor).

盘中因子：基于涨停池数据识别涨停强度 + 板块集中度。
3 个子信号：
- 连板强度 (0-50)：limit_times 梯度映射
- 板块集中度 (0-30)：同板块涨停数的百分位
- 封板时间 (0-20)：越早越强

数据来源: akshare stock_zt_pool_em → limit_pool DB → Tushare limit_list_d
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
    降级到 limit_pool DB，再降级到 Tushare limit_list_d。
    """

    name = "sector"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    _LABEL_THRESHOLD_RATIO = 0.5
    _LOOKBACK_DAYS = 20  # 历史板块统计回溯交易日数

    def __init__(self):
        super().__init__()
        self._zt_pool_cache: Optional[pd.DataFrame] = None
        self._last_zt_slot: int = -1
        self.sector_map: Dict[str, str] = {}
        self._sector_history: Dict[str, tuple] = {}  # {sector: (mean_cnt, std_cnt, n_days)}

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取涨停候选数据，每次调用重建 sector_map 与 sector_history。"""
        self.sector_map.clear()
        self._sector_history = self._load_sector_history(trade_date)
        tushare_fetcher = kwargs.get("tushare_fetcher")

        slot = int(time.time() // 30)

        # ── 偶数槽：查询 DB（新数据由 Scanner 60s 刷新落库）──
        if slot % 2 == 0 and slot != self._last_zt_slot:
            df = self._read_from_limit_pool(trade_date)
            if df is not None and not df.empty:
                return df
            logger.info("[SectorFactor] limit_pool DB 无数据，降级到 akshare")
            df = self._fetch_zt_pool_fallback(trade_date, tushare_fetcher)
            if df is not None and not df.empty:
                return df

        # ── 奇数槽：复用缓存 ──
        if self._zt_pool_cache is not None and not self._zt_pool_cache.empty:
            logger.debug("[SectorFactor] 复用涨停池缓存 (slot=%d)", slot)
            return self._zt_pool_cache

        # ── 无缓存：读 DB ──
        df = self._read_from_limit_pool(trade_date)
        if df is not None and not df.empty:
            return df

        # ── 降级 1：akshare ──
        df = self._fetch_zt_pool_fallback(trade_date, tushare_fetcher)
        if df is not None and not df.empty:
            return df

        # ── 降级 2：Tushare ──
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

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
        """返回 df 中第一个存在的列名，都不存在返回 None。"""
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 3 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        # 列名兼容（akshare 中文 vs DB 英文）
        sector_col = self._resolve_col(df, "sector", "所属行业")
        seal_col = self._resolve_col(df, "first_seal_time", "首次封板时间")
        limit_times_col = self._resolve_col(df, "limit_times")
        pct_chg_col = self._resolve_col(df, "pct_chg")

        signals: Dict[str, pd.Series] = {}

        # --- 1. 连板强度 (0-50)：梯度映射 ---
        s_chain = zeros.copy()
        if limit_times_col:
            lt = df[limit_times_col].fillna(0).clip(0, 5)
            s_chain = lt.map({0: 0, 1: 15, 2: 28, 3: 38, 4: 45, 5: 50}).clip(0, 50)
        elif pct_chg_col:
            pct = df[pct_chg_col].fillna(0).clip(0, 10)
            s_chain = (pct * 5).clip(0, 50)
        signals["chain"] = s_chain

        # --- 2. 板块集中度 (0-30)：历史 z-score，无历史时降级为当日百分位 ---
        if sector_col and self._sector_history:
            sec = df[sector_col].fillna("").astype(str)
            sec = sec.mask(sec.str.strip() == "")
            today_cnts = sec.groupby(sec).transform("count")
            mean_map = pd.Series({k: v[0] for k, v in self._sector_history.items()})
            std_map = pd.Series({k: v[1] for k, v in self._sector_history.items()})
            sector_mean = sec.map(mean_map)
            sector_std = sec.map(std_map).clip(lower=0.01)
            z = (today_cnts - sector_mean) / sector_std
            # z ∈ [-1, 2] → [0, 30] 线性映射
            s_sector = ((z + 1) / 3 * 30).clip(0, 30)
            s_sector = s_sector.where(sec.isin(mean_map.index), 15.0)
        elif sector_col:
            sec = df[sector_col].fillna("").astype(str)
            sec = sec.mask(sec.str.strip() == "")
            if sec.notna().any():
                counts = sec.groupby(sec).transform("count")
                s_sector = (counts.rank(pct=True) * 30).fillna(0).clip(0, 30)
            else:
                s_sector = pd.Series(15.0, index=idx)
        else:
            s_sector = pd.Series(15.0, index=idx)
        signals["sector_heat"] = s_sector

        # --- 3. 封板时间 (0-20)：越早越强 ---
        s_seal = zeros.copy()
        if seal_col:
            def _seal_score(raw) -> float:
                try:
                    parts = str(raw).strip().split(":")
                    if len(parts) < 2:
                        return 0
                    mins = int(parts[0]) * 60 + int(parts[1]) - 570
                    if mins < 0:
                        return 20
                    if mins > 240:
                        return 0
                    return max(0, 20 - mins / 12)
                except (ValueError, TypeError):
                    return 0
            s_seal = df[seal_col].apply(_seal_score).clip(0, 20)
        signals["seal_time"] = s_seal

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

        limit_times_col = self._resolve_col(df, "limit_times")
        sector_col = self._resolve_col(df, "sector", "所属行业")
        seal_col = self._resolve_col(df, "first_seal_time", "首次封板时间")

        signals = self._compute_signals(df)

        signal_meta = [
            ("chain", "连板强度", 50),
            ("sector_heat", "板块集中度", 30),
            ("seal_time", "封板时间", 20),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            score_val = scores[ts_code]
            if score_val <= 0:
                continue

            labels: List[str] = []

            for key, label, max_val in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_val * threshold:
                    continue
                if key == "chain":
                    lt = int(df[limit_times_col].get(ts_code, 0)) if limit_times_col else 0
                    if lt >= 4:
                        labels.append(f"连板龙头({lt}连板)")
                    elif lt >= 2:
                        labels.append(f"{lt}连板")
                    elif lt == 1:
                        labels.append("首板涨停")
                elif key == "sector_heat":
                    sec = str(df[sector_col].get(ts_code, "")) if sector_col else ""
                    same = (df[sector_col] == sec).sum() if sector_col else 0
                    if sec and sec in self._sector_history:
                        mean_cnt, _, _ = self._sector_history[sec]
                        if same > mean_cnt * 1.5:
                            labels.append(f"板块异常火爆({sec}×{same}只, 历史均值{mean_cnt:.0f})")
                        elif same > mean_cnt:
                            labels.append(f"板块活跃({sec}×{same}只涨停)")
                        else:
                            labels.append(f"板块联动({sec}×{same}只涨停)")
                    else:
                        labels.append(f"板块联动({sec}×{same}只涨停)")
                elif key == "seal_time":
                    st = str(df[seal_col].get(ts_code, "")) if seal_col else ""
                    labels.append(f"封板{st}")

            if labels:
                reasons[ts_code] = labels

        return reasons

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_sector_history(self, trade_date: str) -> Dict[str, tuple]:
        """加载过去 N 个交易日各板块涨停数的均值和标准差。

        Returns:
            {sector: (mean_count, std_count, n_days)}, n_days < 3 的板块不返回。
        """
        try:
            from src.storage import DatabaseManager, LimitPool

            db = DatabaseManager()
            with db.get_session() as session:
                from sqlalchemy import func

                rows = (
                    session.query(
                        LimitPool.trade_date,
                        LimitPool.sector,
                        func.count(LimitPool.code).label("cnt"),
                    )
                    .where(
                        LimitPool.trade_date < trade_date,
                        LimitPool.sector.isnot(None),
                        LimitPool.sector != "",
                    )
                    .group_by(LimitPool.trade_date, LimitPool.sector)
                    .all()
                )

            if not rows:
                return {}

            df_hist = pd.DataFrame(rows, columns=["trade_date", "sector", "cnt"])
            dates = sorted(df_hist["trade_date"].unique(), reverse=True)[: self._LOOKBACK_DAYS]
            df_hist = df_hist[df_hist["trade_date"].isin(dates)]

            stats: Dict[str, tuple] = {}
            for sector, grp in df_hist.groupby("sector"):
                cnts = grp["cnt"]
                if len(cnts) >= 3:
                    stats[sector] = (float(cnts.mean()), float(cnts.std()), len(cnts))

            if stats:
                logger.debug(
                    "[SectorFactor] 板块历史: %d 个板块, %d 天",
                    len(stats), len(dates),
                )
            return stats
        except Exception as e:
            logger.warning("[SectorFactor] 加载板块历史失败: %s", e)
            return {}

    def _read_from_limit_pool(self, trade_date: str) -> Optional[pd.DataFrame]:
        """从 limit_pool DB 读取涨停数据，转为 ts_code 索引 DataFrame。"""
        try:
            from src.storage import DatabaseManager

            db = DatabaseManager()
            lp = db.get_limit_pool(trade_date=trade_date, min_pct_chg=9.5)
            if lp is None or lp.empty:
                return None

            df = lp.reset_index().copy()
            df.rename(columns={"index": "code"}, inplace=False)

            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip().zfill(6)
                sec = str(row.get("sector", "")).strip()
                if code and sec and sec not in ("nan", ""):
                    self.sector_map[code] = sec

            df = df.set_index("code")
            df = self._with_ts_code_index(df)

            self._zt_pool_cache = df
            self._last_zt_slot = int(time.time() // 30)

            n_leader = len(df[df["limit_times"] >= 3]) if "limit_times" in df.columns else 0
            n_2b = len(df[df["limit_times"] == 2]) if "limit_times" in df.columns else 0
            n_1b = len(df[df["limit_times"] == 1]) if "limit_times" in df.columns else 0
            logger.info(
                "[SectorFactor] limit_pool DB 涨停池: %d 只, 龙头=%d, 2板=%d, 首板=%d",
                len(df), n_leader, n_2b, n_1b,
            )
            return df
        except Exception as e:
            logger.warning("[SectorFactor] 读取 limit_pool 失败: %s", e)
            return None

    def _fetch_zt_pool_fallback(self, trade_date: str, tushare_fetcher=None) -> Optional[pd.DataFrame]:
        """调用 akshare stock_zt_pool_em 降级拉取涨停池，并统一为申万行业。"""
        try:
            import akshare as ak

            logger.debug("[SectorFactor] 拉取 stock_zt_pool_em (date=%s)", trade_date)
            df = ak.stock_zt_pool_em(date=trade_date)
            if df is None or df.empty:
                return None

            df = df.copy()
            col_map = {
                "代码": "code", "名称": "name", "涨跌幅": "pct_chg",
                "最新价": "price", "连板数": "limit_times", "所属行业": "sector",
                "首次封板时间": "首次封板时间", "最后封板时间": "最后封板时间",
                "炸板次数": "炸板次数", "涨停统计": "涨停统计",
            }
            df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

            # 统一为申万行业（stock_basic 缓存，零 API 成本）
            if tushare_fetcher is not None:
                try:
                    stock_list = tushare_fetcher.get_stock_list()
                    if stock_list is not None and not stock_list.empty:
                        code_to_sw = dict(zip(stock_list["code"], stock_list["industry"]))
                        df["sector"] = df["code"].map(code_to_sw).fillna(df.get("sector", ""))
                except Exception:
                    pass

            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip().zfill(6)
                sec = str(row.get("sector", "")).strip()
                if code and sec:
                    self.sector_map[code] = sec

            df = df.set_index("code")
            df = self._with_ts_code_index(df)

            self._zt_pool_cache = df
            self._last_zt_slot = int(time.time() // 30)

            n_leader = len(df[df["limit_times"] >= 3])
            n_2b = len(df[df["limit_times"] == 2])
            n_1b = len(df[df["limit_times"] == 1])
            logger.info(
                "[SectorFactor] akshare 涨停池: %d 只, 龙头=%d, 2板=%d, 首板=%d",
                len(df), n_leader, n_2b, n_1b,
            )
            return df
        except Exception as e:
            logger.warning("[SectorFactor] akshare stock_zt_pool_em 异常: %s", e)
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
