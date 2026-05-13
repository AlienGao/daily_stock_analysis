# -*- coding: utf-8 -*-
"""板块热度因子 (Sector Heat Factor).

盘中因子：基于涨停池数据识别涨停强度 + 涨幅强度 + 板块集中度 + 封板质量 + 实时板块动量。
6 个子信号：
- 连板强度 (0-20)：limit_times 梯度映射 + 板块龙头溢价
- 涨幅强度 (0-20)：当日涨幅映射，全员可得（非涨停强势股的主要得分来源）
- 板块集中度 (0-25)：同板块涨停数 vs 历史均值 z-score
- 封板时间 (0-15)：越早越强
- 封板质量 (0-5)：炸板率(0-5) + 封板持续性(0~-3) + 封板资金比(0-5)，上限 5
- 盘中热度 (0-35)：realtime_spot 行业聚合 + 板限逼近度 + 龙头带动 + 3轮SMA动量 + 跨板块共振

数据来源: akshare stock_zt_pool_em → limit_pool DB → Tushare limit_list_d
          + realtime_spot DB + ths_industry_map DB (盘中热度/资金流向)
"""

import logging
import time
from collections import deque
from typing import Dict, List, Optional

import pandas as pd

from data_provider.base import is_bse_code, is_kc_cy_stock, is_st_stock
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
        self._zt_cache_trade_date: Optional[str] = None
        self._last_zt_slot: int = -1
        self.sector_map: Dict[str, str] = {}
        self._sector_history: Dict[str, tuple] = {}  # {sector: (mean_cnt, std_cnt, n_days)}
        self._prev_sector_momentum: Dict[str, deque] = {}   # 最近 3 轮板块动量快照 {industry: deque(maxlen=3)}
        self._prev_capital_share: Dict[str, float] = {}     # 上一轮板块成交额占比 {industry: share}
        self._momentum_trade_date: Optional[str] = None      # 跨日重置标记
        self._cached_momentum: Optional[pd.Series] = None   # score() 缓存的动量序列，describe() 复用
        self._cached_industry_deltas: Dict[str, float] = {}  # 本轮行业级原始 delta (cur_base - prev_base)
        self._prev_leader_pull: Dict[str, float] = {}       # {industry: follower_avg_pct_chg} 龙头带动
        self._cached_leader_pull: Dict[str, float] = {}      # {industry: leader_pull_score} describe() 复用

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取涨停候选 + 板块内涨幅前 20% 非涨停强势股。"""
        self.sector_map.clear()
        self._sector_history = self._load_sector_history(trade_date)
        tushare_fetcher = kwargs.get("tushare_fetcher")

        slot = int(time.time() // 30)
        df: Optional[pd.DataFrame] = None

        # ── 偶数槽：查询 DB（新数据由 Scanner 60s 刷新落库）──
        if slot % 2 == 0 and slot != self._last_zt_slot:
            df = self._read_from_limit_pool(trade_date)
            if df is None or df.empty:
                logger.info("[SectorFactor] limit_pool DB 无数据，降级到 akshare")
                df = self._fetch_zt_pool_fallback(trade_date, tushare_fetcher)

        # ── 奇数槽：复用缓存（仅当日有效）──
        if (df is None or df.empty) and (
            self._zt_pool_cache is not None and not self._zt_pool_cache.empty
            and self._zt_cache_trade_date == trade_date
        ):
            logger.debug("[SectorFactor] 复用涨停池缓存 (slot=%d)", slot)
            df = self._zt_pool_cache

        # ── 无缓存：读 DB ──
        if df is None or df.empty:
            df = self._read_from_limit_pool(trade_date)

        # ── 降级 1：akshare ──
        if df is None or df.empty:
            df = self._fetch_zt_pool_fallback(trade_date, tushare_fetcher)

        # ── 降级 2：Tushare ──
        if (df is None or df.empty) and tushare_fetcher is not None:
            df = tushare_fetcher.get_limit_list(trade_date, limit_type="U")
            if df is not None and not df.empty:
                df = df.copy()
                if "limit_times" in df.columns:
                    df["is_leader"] = df["limit_times"] >= 3
                    df["is_2board"] = df["limit_times"] == 2
                    df["is_first"] = df["limit_times"] == 1
                self._zt_pool_cache = df
                self._zt_cache_trade_date = trade_date
                self._last_zt_slot = int(time.time() // 30)

        if df is None or df.empty:
            return df

        # ── 扩展：板块内涨幅前 20% 非涨停强势股 ──
        expanded = self._expand_with_sector_leaders(trade_date, df)
        return expanded if expanded is not None and not expanded.empty else df

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

    @staticmethod
    def _seal_gap_minutes(first: str, last: str) -> int:
        """计算首次封板到最后封板的分钟差，0 表示一封到底。兼容 HH:MM:SS / HHMMSS 两种格式。"""
        try:
            f, l = str(first).strip(), str(last).strip()
            if not f or not l or f in ("nan", "") or l in ("nan", ""):
                return 0

            def _to_min(s: str) -> int:
                if ":" in s:
                    parts = s.split(":")
                elif len(s) >= 4:
                    parts = [s[:2], s[2:4]]
                elif s.isdigit():
                    s = s.zfill(4)  # "930" → "0930"
                    parts = [s[:2], s[2:4]]
                else:
                    return 0
                return int(parts[0]) * 60 + int(parts[1])

            return max(0, _to_min(l) - _to_min(f))
        except (ValueError, IndexError):
            return 0

    def _compute_signals(self, df: pd.DataFrame,
                          momentum_series: Optional[pd.Series] = None) -> Dict[str, pd.Series]:
        """提取 6 个子信号：连板强度 / 涨幅强度 / 板块集中度 / 封板时间 / 封板质量 / 盘中热度。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        # 列名兼容（akshare 中文 vs DB 英文）
        sector_col = self._resolve_col(df, "sector", "所属行业")
        seal_col = self._resolve_col(df, "first_seal_time", "首次封板时间")
        limit_times_col = self._resolve_col(df, "limit_times")
        pct_chg_col = self._resolve_col(df, "pct_chg")

        signals: Dict[str, pd.Series] = {}

        # --- 1. 连板强度 (0-35)：梯度映射 + 板块龙头溢价 ---
        s_chain = zeros.copy()
        if limit_times_col:
            lt = df[limit_times_col].fillna(0).clip(0, 5)
            s_chain = lt.map({0: 0, 1: 10, 2: 20, 3: 27, 4: 32, 5: 35}).clip(0, 35)
        elif pct_chg_col:
            pct = df[pct_chg_col].fillna(0).clip(0, 10)
            s_chain = (pct * 3.5).clip(0, 35)

        # 板块龙头溢价：同板块内连板最高 + 板块涨停数达标 → 额外加分
        if sector_col and limit_times_col and len(df) > 1:
            sec = df[sector_col].fillna("").astype(str)
            sec = sec.mask(sec.str.strip() == "")
            lt = df[limit_times_col].fillna(0)
            seal_col_leader = self._resolve_col(df, "first_seal_time", "首次封板时间")
            for industry, group_mask in sec.groupby(sec).groups.items():
                if len(group_mask) < 3 or not industry:
                    continue
                # 板块龙头：连板最高，同连板时最早封板者胜
                group_lt = lt.loc[group_mask]
                max_lt = group_lt.max()
                if max_lt <= 0:
                    continue
                candidates = group_mask[group_lt == max_lt]
                if len(candidates) > 1 and seal_col_leader:
                    # 同连板数 → 取最早封板
                    best_seal = df.loc[candidates, seal_col_leader].astype(str).str.strip().min()
                    leader_idx = candidates[df.loc[candidates, seal_col_leader].astype(str).str.strip() == best_seal][:1]
                else:
                    leader_idx = candidates[:1]
                # 溢价：5+只涨停 +8，3-4只 +5
                bonus = 8 if len(group_mask) >= 5 else 5
                s_chain.loc[leader_idx] = s_chain.loc[leader_idx] + bonus

        # 流通市值列（chain 不再加权，但 seal_quality 仍需）
        cap_col = self._resolve_col(df, "float_market_cap")

        s_chain = s_chain.clip(0, 20)
        signals["chain"] = s_chain

        # --- 1b. 涨幅强度 (0-20)：仅非涨停股可得（需有 limit_times 区分 ZT），chain 已覆盖 ZT 股 ---
        s_pct = zeros.copy()
        if pct_chg_col and limit_times_col:
            pct = df[pct_chg_col].fillna(0)
            is_zt = lt > 0
            s_pct = pd.Series(0.0, index=idx)
            s_pct = s_pct.mask(~is_zt & (pct >= 9.5), 20)
            s_pct = s_pct.mask(~is_zt & (pct >= 7) & (pct < 9.5), 16)
            s_pct = s_pct.mask(~is_zt & (pct >= 5) & (pct < 7), 11)
            s_pct = s_pct.mask(~is_zt & (pct >= 3) & (pct < 5), 6)
            s_pct = s_pct.mask(~is_zt & (pct >= 1) & (pct < 3), 3)
        signals["pct_chg_strength"] = s_pct

        # --- 2. 板块集中度 (0-25)：历史 z-score，无历史时降级为当日百分位 ---
        if sector_col and self._sector_history:
            sec = df[sector_col].fillna("").astype(str)
            sec = sec.mask(sec.str.strip() == "")
            today_cnts = sec.groupby(sec).transform("count")
            mean_map = pd.Series({k: v[0] for k, v in self._sector_history.items()})
            std_map = pd.Series({k: v[1] for k, v in self._sector_history.items()})
            sector_mean = sec.map(mean_map)
            sector_std = sec.map(std_map)
            # z-score，std=0 时直接映射到边界：高于均值→+2 低于→-1 等于→0
            z = (today_cnts - sector_mean) / sector_std.where(sector_std > 0, 1.0)
            z = z.mask(sector_std <= 0,
                       pd.Series(0.0, index=z.index)
                       .mask(today_cnts > sector_mean, 2.0)
                       .mask(today_cnts < sector_mean, -1.0))
            # z ∈ [-1, 2] → [0, 25] 线性映射
            s_sector = ((z + 1) / 3 * 25).clip(0, 25)
            s_sector = s_sector.where(sec.isin(mean_map.index), 10.0)
        elif sector_col:
            sec = df[sector_col].fillna("").astype(str)
            sec = sec.mask(sec.str.strip() == "")
            if sec.notna().any():
                counts = sec.groupby(sec).transform("count")
                s_sector = (counts.rank(pct=True) * 20).fillna(0).clip(0, 20)
            else:
                s_sector = pd.Series(10.0, index=idx)
        else:
            s_sector = pd.Series(10.0, index=idx)
        signals["sector_heat"] = s_sector

        # --- 3. 封板时间 (0-15)：越早越强 ---
        s_seal = zeros.copy()
        if seal_col:
            def _seal_score(raw) -> float:
                try:
                    s = str(raw).strip()
                    if ":" in s:
                        parts = s.split(":")
                    elif len(s) >= 4:
                        # HHMMSS or HHMM format from akshare
                        parts = [s[:2], s[2:4]]
                    elif s.isdigit():
                        s = s.zfill(4)  # "930" → "0930"
                        parts = [s[:2], s[2:4]]
                    else:
                        return 0
                    if len(parts) < 2:
                        return 0
                    mins = int(parts[0]) * 60 + int(parts[1]) - 570
                    if mins < 0:
                        return 15
                    if mins > 240:
                        return 0
                    return max(0, 15 - mins / 16)
                except (ValueError, TypeError):
                    return 0
            s_seal = df[seal_col].apply(_seal_score).clip(0, 15)
        signals["seal_time"] = s_seal

        # --- 3b. 封板质量 (0-10)：炸板率 + 封板持续性 + 封板资金比 ---
        s_quality = zeros.copy()
        break_col = self._resolve_col(df, "break_count")
        last_seal_col = self._resolve_col(df, "last_seal_time", "最后封板时间")
        seal_amount_col = self._resolve_col(df, "seal_amount")
        for i in idx:
            score = 0.0
            # 炸板率 (0-5)
            if break_col:
                bc = int(df[break_col].get(i, 0) or 0)
                score += {0: 5, 1: 3, 2: 1}.get(bc, 0)
            # 封板持续性 penalty (0 ~ -3)
            if seal_col and last_seal_col:
                gap = self._seal_gap_minutes(
                    str(df[seal_col].get(i, "")),
                    str(df[last_seal_col].get(i, "")),
                )
                if gap > 15:
                    score -= 3
                elif gap > 5:
                    score -= 2
                elif gap > 0:
                    score -= 1
            # 封板资金比 (0-5)
            if seal_amount_col:
                sa = float(df[seal_amount_col].get(i, 0) or 0)
                if sa > 0 and cap_col:
                    cv = float(df[cap_col].get(i, 0) or 0)
                    if cv > 0:
                        ratio = sa / cv
                        if ratio >= 0.05:
                            score += 5
                        elif ratio >= 0.02:
                            score += 3
                        elif ratio >= 0.01:
                            score += 2
                        elif ratio >= 0.005:
                            score += 1
                elif sa > 0:
                    score += 1  # 有封板资金但无市值数据，给最低加分
            s_quality.loc[i] = max(0.0, min(5.0, score))
        signals["seal_quality"] = s_quality

        # --- 4. 盘中热度 (0-35)：realtime_spot 行业聚合 + 轮次 delta ---
        if momentum_series is not None and not momentum_series.empty:
            # 将裸代码索引的 momentum 映射到 df 的 ts_code 索引
            # momentum 的 index 是裸 code (6 位数字字符串)
            # df 的 index 是 ts_code (如 600519.SH)
            bare_map = momentum_series.copy()
            bare_map.index = bare_map.index.astype(str).str.strip().str.zfill(6)
            # 对 df 的每个 ts_code，提取裸代码 → 查 momentum
            bare_from_ts = pd.Index([
                str(x).split(".")[0] if "." in str(x) else str(x).strip().zfill(6)
                for x in idx
            ])
            s_momentum = pd.Series(
                [bare_map.get(c, 0.0) for c in bare_from_ts],
                index=idx,
            ).fillna(0).clip(0, 35)
        else:
            s_momentum = zeros.copy()
        signals["intraday_momentum"] = s_momentum

        return signals

    # ------------------------------------------------------------------
    # 盘中板块动量（realtime_spot + 同花顺行业）
    # ------------------------------------------------------------------

    def _compute_intraday_momentum(self, trade_date: str) -> pd.Series:
        """基于 realtime_spot 计算每只股票的盘中板块动量得分 (0-30)。

        按同花顺行业聚合 pct_chg / 涨幅广度 / 换手率 / 成交额占比 → base_score，
        与上一轮快照对比：升温加分（最多+5），走弱减分（最多-5）。
        返回 Series (index=stock_code, dtype=float)。
        """
        try:
            from src.storage import DatabaseManager

            db = DatabaseManager()

            # 跨日重置
            if self._momentum_trade_date != trade_date:
                self._prev_sector_momentum.clear()
                self._prev_capital_share.clear()
                self._prev_leader_pull.clear()
                self._momentum_trade_date = trade_date

            # ── 1. 拉取全量实时行情 ──
            spot_df = db.get_realtime_spot()
            if spot_df is None or spot_df.empty:
                logger.warning("[SectorFactor] realtime_spot 为空，动量评分为 0")
                return pd.Series(dtype=float)

            # ── 2. 拉取行业映射 ──
            ths_map = db.get_ths_industry_map()
            if not ths_map:
                logger.warning("[SectorFactor] ths_industry_map 为空，动量评分为 0")
                return pd.Series(dtype=float)

            spot = spot_df.copy()
            spot["industry"] = spot.index.map(ths_map)
            spot = spot[spot["industry"].notna() & (spot["industry"] != "")]
            if spot.empty:
                return pd.Series(dtype=float)

            # ── 2b. 计算每只股票的板限逼近度 (limit_close_ratio) ──
            codes = spot.index.astype(str).str.strip().str.zfill(6)
            names = spot["name"].fillna("").astype(str)

            # 板限优先级：北交 30% > 双创 20% > 主板 ST 5% > 主板 10%
            daily_limit_pct = pd.Series(10.0, index=spot.index)
            daily_limit_pct[
                codes.str.startswith(("92", "43", "81", "82", "83", "87", "88", "889"))
                & ~codes.str.startswith("900")
            ] = 30.0
            daily_limit_pct[
                codes.str.startswith(("688", "689", "300", "301"))
            ] = 20.0
            daily_limit_pct[
                names.str.upper().str.contains("ST", na=False)
                & (daily_limit_pct == 10.0)  # 仅主板 ST 降为 5%
            ] = 5.0

            spot["limit_close_ratio"] = (
                spot["pct_chg"].fillna(0).clip(lower=0) / daily_limit_pct
            ).clip(0, 1.0)

            # ── 3. 按行业聚合 ──
            pct = spot["pct_chg"].fillna(0)
            turnover = spot["turnover_rate"].fillna(0)
            amount = spot["amount"].fillna(0)

            agg = spot.groupby("industry").agg(
                avg_pct_chg=("pct_chg", lambda x: x.fillna(0).mean()),
                std_pct_chg=("pct_chg", lambda x: x.fillna(0).std()),
                near_limit_cnt=("limit_close_ratio", lambda x: (x > 0.7).sum()),
                total_cnt=("pct_chg", "count"),
                avg_turnover=("turnover_rate", lambda x: x.fillna(0).mean()),
                sector_amount=("amount", "sum"),
            )

            # ── 3b. 龙头带动效应 (Leader Pull-Up) ──
            leader_pull_scores: Dict[str, float] = {}
            for industry, grp in spot.groupby("industry"):
                if len(grp) < 2:
                    leader_pull_scores[industry] = 0.0
                    continue
                leader_idx = grp["limit_close_ratio"].idxmax()
                leader_strength = min(float(grp.loc[leader_idx, "limit_close_ratio"]), 1.0)
                followers = grp.drop(leader_idx)
                follower_avg = float(followers["pct_chg"].fillna(0).mean())
                prev_avg = self._prev_leader_pull.get(industry, follower_avg)
                delta = follower_avg - prev_avg
                if leader_strength > 0.5 and delta > 0:
                    leader_mult = (leader_strength - 0.5) * 2.0
                    delta_mult = min(delta / 2.0, 1.0)
                    leader_pull_scores[industry] = float(leader_mult * delta_mult * 5.0)
                else:
                    leader_pull_scores[industry] = 0.0
                self._prev_leader_pull[industry] = follower_avg
            leader_pull_series = pd.Series(leader_pull_scores, name="leader_pull")

            # ── 4. 计算 base_score (0-30) ──
            # score_avg: avg_pct_chg [-2, 8] → [0, 10]
            raw_avg = agg["avg_pct_chg"].clip(-2, 8)
            score_avg = ((raw_avg + 2) / 10 * 10).clip(0, 10)

            # score_near_limit: limit_close_ratio > 0.7 的占比 → [0, 8] (替代 strong_ratio)
            raw_near = (agg["near_limit_cnt"] / agg["total_cnt"].clip(lower=1)).fillna(0)
            score_near_limit = (raw_near * 8).clip(0, 8)

            # score_turn: avg_turnover [0, 10%] → [0, 4]
            raw_turn = agg["avg_turnover"].clip(0, 10)
            score_turn = (raw_turn / 10 * 4).clip(0, 4)

            # score_capital: capital_share rank → [0, 3]
            total_amount = agg["sector_amount"].sum()
            capital_share = (agg["sector_amount"] / total_amount) if total_amount > 0 else pd.Series(0.0, index=agg.index)
            score_capital = (capital_share.rank(pct=True) * 3).clip(0, 3)

            # leader_pull: 0-5 (龙头带动)
            leader_pull_aligned = leader_pull_series.reindex(agg.index).fillna(0)

            base_series = score_avg + score_near_limit + score_turn + score_capital + leader_pull_aligned  # 0-30

            # ── 4b. 板块内部分化度调整 (P4) ──
            # 行业内 pct_chg 标准差越小（齐涨），板块效应越确定，加分；分化大则折扣
            div_series = pd.Series(1.0, index=base_series.index)
            std_all = agg["std_pct_chg"].fillna(0)
            std_max = std_all.max()
            if std_max > 0:
                div_series = 1.0 + (1.0 - std_all / std_max) * 0.15  # 1.0 ~ 1.15
            base_series = base_series * div_series

            # ── 5. 轮次间 delta 调整（升温/走弱）(P1: 3 轮 SMA) ──
            prev_map = self._prev_sector_momentum
            delta_series = pd.Series(0.0, index=base_series.index)
            raw_deltas: Dict[str, float] = {}
            for industry, cur in base_series.items():
                dq = prev_map.get(industry)
                if dq and len(dq) > 0:
                    sma = sum(dq) / len(dq)
                    raw_delta = cur - sma
                    raw_deltas[industry] = raw_delta
                    delta_series[industry] = max(-5, min(5, raw_delta * 0.5))
                else:
                    raw_deltas[industry] = 0.0
                # 首轮不调整

            # ── 5b. 跨板块共振 (P5)：升温板块占比 → 全局情绪系数 ──
            warming_cnt = sum(1 for d in raw_deltas.values() if d > 0)
            total_industries = len(raw_deltas)
            if total_industries > 0:
                warming_ratio = warming_cnt / total_industries
                if warming_ratio >= 0.6:
                    resonance_mult = 1.10
                elif warming_ratio >= 0.4:
                    resonance_mult = 1.05
                elif warming_ratio >= 0.2:
                    resonance_mult = 1.0
                else:
                    resonance_mult = 0.95
            else:
                resonance_mult = 1.0

            momentum_by_industry = (base_series + delta_series) * resonance_mult
            momentum_by_industry = momentum_by_industry.clip(0, 40)

            # 更新快照 (P1: deque maxlen=3)
            for industry, cur in base_series.items():
                dq = prev_map.get(industry)
                if dq is None:
                    dq = deque(maxlen=3)
                    prev_map[industry] = dq
                dq.append(float(cur))
            self._cached_industry_deltas = raw_deltas
            self._cached_leader_pull = leader_pull_scores

            # ── 6. 映射到个股 ──
            stock_momentum = spot["industry"].map(momentum_by_industry)
            stock_momentum = stock_momentum.fillna(0).rename("sector_momentum")
            self._cached_momentum = stock_momentum

            n_industries = len(momentum_by_industry)
            top3 = momentum_by_industry.nlargest(3)
            top_info = ", ".join(
                f"{ind}={v:.1f}(资金{capital_share.get(ind, 0)*100:.1f}%)"
                for ind, v in top3.items()
            )
            logger.info(
                "[SectorFactor] 盘中热度: %d 个行业, Top3: %s",
                n_industries, top_info,
            )
            return stock_momentum

        except Exception as e:
            logger.warning("[SectorFactor] 计算盘中热度失败: %s", e)
            return pd.Series(dtype=float)

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        trade_date = context.get("trade_date", "")
        momentum = self._compute_intraday_momentum(trade_date)
        signals = self._compute_signals(df, momentum_series=momentum)
        total = sum(signals.values()).clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        trade_date = context.get("trade_date", "")
        momentum = self._cached_momentum if self._cached_momentum is not None else pd.Series(dtype=float)

        limit_times_col = self._resolve_col(df, "limit_times")
        pct_chg_col = self._resolve_col(df, "pct_chg")
        sector_col = self._resolve_col(df, "sector", "所属行业")
        seal_col = self._resolve_col(df, "first_seal_time", "首次封板时间")

        signals = self._compute_signals(df, momentum_series=momentum)

        signal_meta = [
            ("chain", "连板强度", 20),
            ("pct_chg_strength", "涨幅强度", 20),
            ("sector_heat", "板块集中度", 25),
            ("seal_time", "封板时间", 15),
            ("seal_quality", "封板质量", 5),
            ("intraday_momentum", "盘中热度", 35),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        # 构建行业名查询（用于动量标签）
        industry_map: Dict[str, str] = {}
        try:
            from src.storage import DatabaseManager
            industry_map = DatabaseManager().get_ths_industry_map()
        except Exception:
            pass

        for ts_code in scores.index:
            score_val = scores[ts_code]
            if score_val <= 0:
                continue

            bare = str(ts_code).split(".")[0] if "." in str(ts_code) else str(ts_code).strip().zfill(6)
            labels: List[str] = []

            for key, label, max_val in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_val * threshold:
                    continue
                if key == "chain":
                    lt = int(df[limit_times_col].get(ts_code, 0)) if limit_times_col else 0
                    sec_name = str(df[sector_col].get(ts_code, "")) if sector_col else ""
                    same = (df[sector_col] == sec_name).sum() if sector_col else 0
                    # 检查是否为板块龙头（连板最高且板块 ≥ 3 只）
                    is_leader = False
                    if sector_col and limit_times_col and sec_name and same >= 3:
                        sec_mask = df[sector_col] == sec_name
                        sec_max_lt = df.loc[sec_mask, limit_times_col].fillna(0).max()
                        if lt >= sec_max_lt and lt > 0:
                            is_leader = True
                    if is_leader:
                        labels.append(f"板块龙头({sec_name}×{same}只, {lt}连板)")
                    elif lt >= 4:
                        labels.append(f"连板龙头({lt}连板)")
                    elif lt >= 2:
                        labels.append(f"{lt}连板")
                    elif lt == 1:
                        labels.append("首板涨停")
                elif key == "pct_chg_strength":
                    pct = float(df[pct_chg_col].get(ts_code, 0)) if pct_chg_col else 0
                    if pct >= 9.5:
                        labels.append("涨停")
                    elif pct >= 7:
                        labels.append(f"大涨+{pct:.1f}%")
                    elif pct >= 5:
                        labels.append(f"强势+{pct:.1f}%")
                    elif pct >= 3:
                        labels.append(f"稳步上涨+{pct:.1f}%")
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
                    bc = int(df.get("break_count", pd.Series(0, index=df.index)).get(ts_code, 0) or 0)
                    if bc == 0 and st and str(st) <= "093000":
                        labels.append(f"秒板({st})")
                    elif bc == 0 and st and str(st) <= "094500":
                        labels.append(f"早封({st})")
                    else:
                        labels.append(f"封板{st}")
                elif key == "seal_quality":
                    bc = int(df.get("break_count", pd.Series(0, index=df.index)).get(ts_code, 0) or 0)
                    seal_amount_col = self._resolve_col(df, "seal_amount")
                    sa = float(df[seal_amount_col].get(ts_code, 0) or 0) if seal_amount_col else 0
                    if bc == 0:
                        labels.append(f"一封到底(封板资金{sa/1e4:.0f}万)" if sa > 0 else "一封到底")
                    elif bc == 1:
                        labels.append("轻微炸板(1次)")
                    else:
                        labels.append(f"多次炸板({bc}次)")
                elif key == "intraday_momentum":
                    ind = industry_map.get(bare, "")
                    raw_delta = self._cached_industry_deltas.get(ind, 0.0)
                    leader_pull = self._cached_leader_pull.get(ind, 0.0)
                    if leader_pull > 2.0:
                        labels.append(f"龙头带动({ind} 跟涨加速)")
                    if raw_delta > 0.01:
                        labels.append(f"板块升温({ind} ↑+{raw_delta:.1f})")
                    elif raw_delta < -0.01:
                        labels.append(f"板块降温({ind} ↓{raw_delta:.1f})")
                    else:
                        labels.append(f"板块活跃({ind} {val:.1f})")

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

            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip().zfill(6)
                sec = str(row.get("sector", "")).strip()
                if code and sec and sec not in ("nan", ""):
                    self.sector_map[code] = sec

            df = df.set_index("code")
            df = self._with_ts_code_index(df)

            self._zt_pool_cache = df
            self._zt_cache_trade_date = trade_date
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
                "流通市值": "float_market_cap", "封板资金": "seal_amount",
            }
            df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)

            # 板块分类：优先保留 akshare 同花顺行业，缺失时用 DB 同花顺映射填充
            if "sector" not in df.columns:
                df["sector"] = ""
            needs_sector = df["sector"].isna() | (
                df["sector"].astype(str).str.strip().isin(["", "nan"])
            )
            if needs_sector.any():
                try:
                    from src.storage import DatabaseManager
                    ths_map = DatabaseManager().get_ths_industry_map()
                    if ths_map:
                        sw = df["code"].map(ths_map)
                        df.loc[needs_sector, "sector"] = sw[needs_sector].fillna("")
                except Exception:
                    pass

            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip().zfill(6)
                sec = str(row.get("sector", "")).strip()
                if code and sec and sec not in ("nan", ""):
                    self.sector_map[code] = sec

            df = df.set_index("code")
            df = self._with_ts_code_index(df)

            self._zt_pool_cache = df
            self._zt_cache_trade_date = trade_date
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

    def _expand_with_sector_leaders(self, trade_date: str, zt_df: pd.DataFrame) -> pd.DataFrame:
        """将板块内涨幅前 20% 的非涨停强势股纳入候选，让 intraday_momentum 结果被有效利用。"""
        try:
            from src.storage import DatabaseManager

            db = DatabaseManager()

            spot = db.get_realtime_spot()
            if spot is None or spot.empty:
                return zt_df

            ths_map = db.get_ths_industry_map()
            if not ths_map:
                return zt_df

            sector_col = self._resolve_col(zt_df, "sector", "所属行业")
            if not sector_col:
                return zt_df
            zt_sectors = set(zt_df[sector_col].fillna("").astype(str).str.strip())
            zt_sectors.discard("")
            zt_sectors.discard("nan")
            if not zt_sectors:
                return zt_df

            spot = spot.copy()
            spot["industry"] = spot.index.astype(str).str.zfill(6).map(ths_map)
            spot = spot[spot["industry"].notna() & (spot["industry"] != "")]
            spot = spot[spot["industry"].isin(zt_sectors)]

            zt_bare = set(
                zt_df.index.astype(str).str.replace(r"\.(SH|SZ|BJ)$", "", regex=True).str.zfill(6)
            )
            spot["bare"] = spot.index.astype(str).str.zfill(6)
            spot = spot[~spot["bare"].isin(zt_bare)]

            if spot.empty:
                return zt_df

            spot["pct_rank"] = spot.groupby("industry")["pct_chg"].rank(pct=True, ascending=False)
            top_spot = spot[spot["pct_rank"] <= 0.2]

            if top_spot.empty:
                return zt_df

            new_rows = []
            for code in top_spot.index:
                row_data = top_spot.loc[code]
                bare = str(code).strip().zfill(6)
                new_rows.append({
                    "code": bare,
                    "name": str(row_data.get("name", "")),
                    "trade_date": trade_date,
                    "limit_type": "",
                    "pct_chg": float(row_data.get("pct_chg", 0)),
                    "price": float(row_data.get("price", 0)),
                    "limit_times": 0,
                    "open_times": 0,
                    "up_stat": "",
                    "first_seal_time": "",
                    "last_seal_time": "",
                    "break_count": 0,
                    "limit_stats": "",
                    "sector": str(row_data.get("industry", "")),
                    "float_market_cap": None,
                    "seal_amount": 0,
                    "source": "realtime_spot",
                    "slot": 0,
                })
                self.sector_map[bare] = str(row_data.get("industry", ""))

            new_df = pd.DataFrame(new_rows).set_index("code")
            new_df = self._with_ts_code_index(new_df)

            combined = pd.concat([zt_df, new_df])
            logger.info(
                "[SectorFactor] 扩展非涨停板块强势股: +%d 只 (来自 %d 个板块), 合并后 %d 只",
                len(new_df), len(zt_sectors), len(combined),
            )
            return combined
        except Exception as e:
            logger.warning("[SectorFactor] 扩展非涨停股失败: %s", e)
            return zt_df

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
            elif code_str.startswith(("43", "83", "87", "92")):
                new_index.append(f"{code_str}.BJ")
            else:
                new_index.append(code_str)
        df.index = new_index
        return df
