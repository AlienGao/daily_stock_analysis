# -*- coding: utf-8 -*-
"""概念热度因子 (Concept Heat Factor).

盘后因子：基于 Tushare limit_cpt_list（涨停概念板块 Top 20 排行），
结合 ths_concept_map（概念→成分股映射）和 momentum_snapshot（资金流确认），
识别「被资金关注的热门主题股」。

得分结构（乘法闸门）：
- 概念强度 = 85/√(rank) × min(up_ratio/4%, 1.0)  排名非线性 + 涨停集中度
- 资金乘数 (闸门)：inflow≤0 → 0.2, 0→3% → 0.2→1.0, >3% → 1.0
- 多概念共振 (0-5)：2概念 +3, 3+概念 +5
- 持续性加分：≥5天 +5, ≥10天 +10
- 最终 = 概念强度 × 资金乘数 + 共振 + 持续性，clip(0, 100)

资金流降级：momentum_snapshot 无当日数据时，跳过资金乘数闸门。
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class ConceptHeatFactor(BaseFactor):
    """概念热度因子。

    不预测涨跌，只回答一个问题：这只股票今天是不是被资金关注的热门主题股？
    涨跌判断交给其他因子。
    """

    name = "concept_heat"
    available_intraday = False
    available_postmarket = True
    weight = 16.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """拉取热门概念 + 成分股 + 资金流确认。"""
        tushare_fetcher = kwargs.get("tushare_fetcher")

        # 1. limit_cpt_list -> Top 20 热门概念
        concepts = self._fetch_hot_concepts(trade_date, tushare_fetcher)
        if not concepts:
            logger.warning("[ConceptHeatFactor] 无热门概念数据")
            return None

        # 过滤：排除 days=1 一日游、name 含 ST
        concepts = [
            c for c in concepts
            if c["days"] > 1 and "ST" not in c["name"]
        ]
        if not concepts:
            logger.warning("[ConceptHeatFactor] 过滤后无有效概念")
            return None

        logger.info("[ConceptHeatFactor] 热门概念 %d 个: %s",
                    len(concepts),
                    ", ".join(f"{c['name']}(#{c['rank']})" for c in concepts[:5]))

        # 2. ths_concept_map -> 成分股
        concept_names = [c["name"] for c in concepts]
        stocks_by_concept = self._fetch_concept_stocks(concept_names)
        if not stocks_by_concept:
            logger.warning("[ConceptHeatFactor] 概念映射为空，请先运行 build_ths_concept_map.py")
            return None

        # 构建 per-stock：取最佳概念排名
        stock_info: Dict[str, dict] = {}
        for c in concepts:
            c_name = c["name"]
            c_rank = c["rank"]
            c_days = c["days"]
            up_ratio = c["up_nums"] / max(c["cons_nums"], 1)
            for stock_code in stocks_by_concept.get(c_name, []):
                if stock_code not in stock_info or c_rank < stock_info[stock_code]["concept_rank"]:
                    stock_info[stock_code] = {
                        "concept_rank": c_rank,
                        "concept_count": 1,
                        "top_concept_name": c_name,
                        "top_concept_days": c_days,
                        "up_ratio": up_ratio,
                    }
                elif c_rank == stock_info[stock_code]["concept_rank"]:
                    stock_info[stock_code]["concept_count"] += 1
                else:
                    stock_info[stock_code]["concept_count"] += 1

        # 3. momentum_snapshot -> 资金流确认
        inflow_map = self._fetch_inflow_rates(trade_date)
        self._has_inflow_data = bool(inflow_map)

        # 4. 构建 DataFrame
        rows = []
        for stock_code, info in stock_info.items():
            rows.append({
                "ts_code": stock_code,
                "concept_rank": info["concept_rank"],
                "concept_count": info["concept_count"],
                "top_concept_name": info["top_concept_name"],
                "top_concept_days": info["top_concept_days"],
                "up_ratio": info["up_ratio"],
                "inflow_rate": inflow_map.get(stock_code, 0.0),
            })

        df = pd.DataFrame(rows)
        df = df.set_index("ts_code")
        logger.info("[ConceptHeatFactor] 获取 %d 只热门主题股", len(df))
        return df

    # ------------------------------------------------------------------
    # fetch helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_hot_concepts(trade_date: str, tushare_fetcher=None) -> List[dict]:
        """调用 limit_cpt_list 获取当日涨停概念板块 Top 20。"""
        try:
            import tushare as ts

            pro = ts.pro_api()
            raw = pro.limit_cpt_list(trade_date=trade_date)
            if raw is None or raw.empty:
                logger.warning("[ConceptHeatFactor] limit_cpt_list 返回空")
                return []

            concepts = []
            for _, row in raw.iterrows():
                concepts.append({
                    "ts_code": str(row["ts_code"]).strip(),
                    "name": str(row["name"]).strip(),
                    "days": int(row.get("days", 1)),
                    "up_nums": int(row.get("up_nums", 0)),
                    "cons_nums": int(row.get("cons_nums", 1)),
                    "pct_chg": float(row.get("pct_chg", 0)),
                    "rank": int(row.get("rank", 20)),
                })
            return concepts
        except Exception as e:
            logger.warning("[ConceptHeatFactor] limit_cpt_list 调用失败: %s", e)
            return []

    @staticmethod
    def _fetch_concept_stocks(concept_names: List[str]) -> Dict[str, List[str]]:
        """从 ths_concept_map 表查询概念的成分股。"""
        try:
            from src.storage import DatabaseManager
            return DatabaseManager().get_stocks_by_concepts(concept_names)
        except Exception as e:
            logger.warning("[ConceptHeatFactor] 概念映射查询失败: %s", e)
            return {}

    @staticmethod
    def _fetch_inflow_rates(trade_date: str) -> Dict[str, float]:
        """从 momentum_snapshot 表查询当日资金流入率。"""
        try:
            from src.storage import DatabaseManager
            from sqlalchemy import text

            td = str(trade_date).replace("-", "")[:8]
            db = DatabaseManager()
            with db.get_session() as s:
                rows = s.execute(
                    text("SELECT code, inflow_rate FROM momentum_snapshot WHERE trade_date = :td"),
                    {"td": td},
                ).fetchall()
            if rows:
                logger.info("[ConceptHeatFactor] momentum_snapshot 命中 %d 条", len(rows))
                return {r[0]: float(r[1] or 0) for r in rows}
        except Exception as e:
            logger.debug("[ConceptHeatFactor] momentum_snapshot 查询失败: %s", e)
        return {}

    # ------------------------------------------------------------------
    # score
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        idx = df.index
        concept_rank = df.get("concept_rank", pd.Series(20, index=idx))
        concept_count = df.get("concept_count", pd.Series(1, index=idx))
        concept_days = df.get("top_concept_days", pd.Series(1, index=idx))
        up_ratio_pct = df.get("up_ratio", pd.Series(0.0, index=idx))
        inflow_rate = df.get("inflow_rate", pd.Series(0.0, index=idx)).copy()
        has_inflow = getattr(self, "_has_inflow_data", False)

        # 哨兵值处理：±1.0 视为无数据
        inflow_rate = inflow_rate.mask(np.abs(inflow_rate) >= 0.999, 0.0)

        # 1. 概念强度：排名非线性 × 涨停集中度
        concept_strength = (85.0 / np.sqrt(concept_rank)) * np.minimum(up_ratio_pct / 0.04, 1.0)

        # 2. 多概念共振 (0-5)
        resonance = pd.Series(0.0, index=idx)
        resonance = resonance.mask((concept_count >= 2) & (concept_count < 3), 3.0)
        resonance = resonance.mask(concept_count >= 3, 5.0)

        if has_inflow:
            # 3. 资金乘数 (闸门)：≤0 → 0.2, 0~3% → 0.2→1.0, >3% → 1.0
            fund_mult = pd.Series(0.2, index=idx)
            fund_mult = fund_mult.mask(
                (inflow_rate > 0) & (inflow_rate <= 0.03),
                0.2 + 0.8 * inflow_rate / 0.03,
            )
            fund_mult = fund_mult.mask(inflow_rate > 0.03, 1.0)
            total = concept_strength * fund_mult
        else:
            total = concept_strength

        total = total + resonance

        # 4. 持续性加分：≥5天 +5, ≥10天 +10
        sustainability = pd.Series(0.0, index=idx)
        sustainability = sustainability.mask((concept_days >= 5) & (concept_days < 10), 5.0)
        sustainability = sustainability.mask(concept_days >= 10, 10.0)
        total = total + sustainability

        total = total.clip(0, 100)
        total.name = self.name
        return total

    # ------------------------------------------------------------------
    # describe
    # ------------------------------------------------------------------

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        threshold = self._LABEL_THRESHOLD_RATIO

        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue

            row = df.loc[ts_code] if ts_code in df.index else None
            if row is None:
                continue

            labels = []
            cname = str(row.get("top_concept_name", ""))
            crank = int(row.get("concept_rank", 20))
            ccount = int(row.get("concept_count", 1))
            cdays = int(row.get("top_concept_days", 1))
            inflow = float(row.get("inflow_rate", 0))
            if abs(inflow) >= 0.999:
                inflow = 0.0  # 哨兵值

            labels.append(f"热门概念({cname}, #{crank})")

            if ccount >= 3:
                labels.append(f"多主题共振({ccount}概念)")
            elif ccount == 2:
                labels.append("双概念共振")

            if inflow > 0.01:
                labels.append(f"资金净流入({inflow*100:.1f}%)")

            if cdays >= 10:
                labels.append(f"持续热点({cdays}天)")
            elif cdays >= 5:
                labels.append(f"中期热点({cdays}天)")

            if labels:
                reasons[ts_code] = labels

        return reasons
