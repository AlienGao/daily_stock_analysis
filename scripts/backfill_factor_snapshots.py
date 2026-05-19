#!/usr/bin/env python3
"""回补 factor_score_snapshots 历史数据。

从 DB 已有表中读历史数据，调用每个因子的 fetch_data() + score() 获取原始 0-100 分，
写入 factor_score_snapshots 表。所有因子均为 DB 优先策略，无外部 API 调用。

用法:
    python scripts/backfill_factor_snapshots.py                    # 默认 2026-03-24 至今
    python scripts/backfill_factor_snapshots.py --start 20260101   # 指定起始
    python scripts/backfill_factor_snapshots.py --start 20260324 --end 20260515
    python scripts/backfill_factor_snapshots.py --dry-run          # 预览不写入
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backfill_snapshots")


# ---------------------------------------------------------------------------
# Mock fetchers — 所有 API 方法返回 None/空，强制因子走 DB 路径
# ---------------------------------------------------------------------------

class MockTushareFetcher:
    """Mock tushare_fetcher，仅提供交易日历（从 stock_daily），API 方法均返回 None。"""

    def __init__(self, trading_dates: List[str]):
        self._trading_dates = sorted(trading_dates, reverse=True)

    def _get_trade_dates(self, end_date=None, start_date=None) -> List[str]:
        return self._trading_dates

    # 覆盖所有因子可能调用的 API 方法
    def get_bulk_money_flow(self, trade_date=None):
        return None

    def get_limit_list(self, trade_date=None, limit_type=None):
        return None

    def get_bulk_hm_detail(self, trade_date=None):
        return None

    def get_bulk_stk_factor(self, trade_date=None):
        return None

    def get_daily_basic_all(self, trade_date=None):
        return None

    def get_broker_recommend(self, month=None):
        return None

    def get_repurchase(self, start_date=None):
        return None

    def get_bulk_cyq_perf(self, trade_date=None):
        return None

    def get_bulk_margin_detail_range(self, start_date=None, end_date=None):
        return None

    def get_dc_hot(self, trade_date=None):
        return None

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class MockAkshareFetcher:
    """Mock akshare_fetcher，所有 API 方法返回 None。"""

    def get_insider_buy(self):
        return None

    def get_institution_holds(self):
        return None

    def get_performance_report_quarter(self, period=None):
        return None

    def get_profit_forecast(self):
        return None

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_trading_dates(start: str, end: str) -> List[str]:
    """从 stock_daily 取指定区间内的交易日（去重排序）。"""
    from src.storage import DatabaseManager
    from sqlalchemy import text

    db = DatabaseManager()
    with db.get_session() as s:
        rows = s.execute(
            text(
                "SELECT DISTINCT date FROM stock_daily "
                "WHERE date >= :start AND date <= :end ORDER BY date"
            ),
            {"start": start, "end": end},
        ).fetchall()
    return [r[0] for r in rows]


def load_all_factors():
    """返回所有内置因子实例列表（同 engine._default_factors）。"""
    from src.discovery.factors import (
        MaEntryFactor,
        MomentumFactor, MoneyFlowFactor, SectorFactor, TechnicalFactor,
        BrokerRecommendFactor, FundamentalFactor, HotMoneyFactor, MarginFactor,
        ChipFactor, InsiderBuyFactor, InstitutionHoldFactor, LimitFactor,
        PerformanceFactor, PopularityFactor, RankingMomentumFactor, ReboundFactor,
        BuybackFactor, ProfitForecastFactor, ConceptHeatFactor,
    )
    from src.discovery.factors.alpha042_factor import Alpha042Factor
    from src.discovery.factors.vwap_deviation_factor import VwapDeviationFactor
    from src.discovery.factors.gap_reversal_factor import GapReversalFactor
    from src.discovery.factors.liquid_oversold_factor import LiquidOversoldFactor
    from src.discovery.factors.vwap_reversal_factor import VwapReversalFactor
    from src.discovery.factors.gtja114_factor import Gtja114Factor

    return [
        MaEntryFactor(),
        MomentumFactor(), MoneyFlowFactor(), SectorFactor(), TechnicalFactor(),
        BrokerRecommendFactor(), FundamentalFactor(), HotMoneyFactor(), MarginFactor(),
        ChipFactor(), InsiderBuyFactor(), InstitutionHoldFactor(), LimitFactor(),
        PerformanceFactor(), PopularityFactor(), RankingMomentumFactor(), ReboundFactor(),
        BuybackFactor(), ProfitForecastFactor(), ConceptHeatFactor(),
        Alpha042Factor(), VwapDeviationFactor(), GapReversalFactor(),
        LiquidOversoldFactor(), VwapReversalFactor(), Gtja114Factor(),
    ]


# ---------------------------------------------------------------------------
# Main backfill
# ---------------------------------------------------------------------------

def backfill(start_date: str, end_date: str, dry_run: bool = False):
    trading_dates = get_trading_dates(start_date, end_date)
    if not trading_dates:
        logger.error("无交易日数据，区间 %s ~ %s", start_date, end_date)
        return

    logger.info("交易日区间: %s ~ %s, 共 %d 个交易日", trading_dates[0], trading_dates[-1], len(trading_dates))

    tushare_mock = MockTushareFetcher(trading_dates)
    akshare_mock = MockAkshareFetcher()
    all_factors = load_all_factors()

    from src.storage import DatabaseManager
    db = DatabaseManager()

    total_saved = 0
    for i, trade_date in enumerate(trading_dates):
        td_str = str(trade_date).replace("-", "").strip()
        if len(td_str) != 8:
            td_str = trade_date

        logger.info("[%d/%d] %s", i + 1, len(trading_dates), td_str)

        for mode in ("intraday", "postmarket"):
            raw_scores: Dict[str, pd.Series] = {}
            available = [f for f in all_factors if f.is_available(mode)]
            if not available:
                continue

            for factor in available:
                try:
                    df = factor.fetch_data(
                        td_str,
                        tushare_fetcher=tushare_mock,
                        akshare_fetcher=akshare_mock,
                    )
                    if df is None or df.empty:
                        continue
                    scores = factor.score(df, trade_date=td_str)
                    if scores is not None and not scores.empty:
                        raw_scores[factor.name] = scores
                except Exception:
                    logger.debug("[%s] %s score 失败", mode, factor.name, exc_info=True)

            if not raw_scores:
                continue

            if dry_run:
                total_rows = sum(len(s) for s in raw_scores.values())
                factor_list = ", ".join(raw_scores.keys())
                logger.info("  [DRY-RUN] %s: %d 因子, ~%d 行 → %s", mode, len(raw_scores), total_rows, factor_list)
            else:
                try:
                    saved = db.save_factor_score_snapshots(raw_scores, td_str, mode)
                    total_saved += saved
                    logger.info("  [%s] 保存 %d 条 (%d 因子)", mode, saved, len(raw_scores))
                except Exception:
                    logger.warning("[%s] 保存失败", mode, exc_info=True)

    if dry_run:
        logger.info("DRY-RUN 完成（未写入）")
    else:
        logger.info("回补完成，共写入 %d 条", total_saved)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="回补 factor_score_snapshots 历史数据")
    parser.add_argument("--start", default="20260324", help="起始日期 YYYYMMDD (默认 20260324)")
    parser.add_argument("--end", default=None, help="结束日期 YYYYMMDD (默认今天)")
    parser.add_argument("--dry-run", action="store_true", help="预览不写入")
    args = parser.parse_args()

    end_date = args.end or datetime.now().strftime("%Y-%m-%d")
    start_date = f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:]}"
    if args.end and len(args.end) == 8:
        end_date = f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:]}"

    logger.info("回补区间: %s ~ %s, dry_run=%s", start_date, end_date, args.dry_run)
    backfill(start_date, end_date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
