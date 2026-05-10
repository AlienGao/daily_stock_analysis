# -*- coding: utf-8 -*-
"""冒烟测试：遍历所有因子 fetch_data() → score() / describe()，不写 DB。

用法: python tests/smoke_test_all_factors.py [trade_date]
默认 trade_date 取最近交易日。
"""

import sys
import os
import traceback
from typing import List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 所有因子 (class_name, mode)
ALL_FACTORS: List[tuple] = [
    ("SectorFactor", "intraday"),
    ("MaEntryFactor", "intraday"),
    ("LimitFactor", "postmarket"),
    ("ReboundFactor", "intraday"),
    ("PopularityFactor", "intraday"),
    ("TechnicalFactor", "postmarket"),
    ("MomentumFactor", "intraday"),
    ("MarginFactor", "postmarket"),
    ("ChipFactor", "postmarket"),
    ("MoneyFlowFactor", "postmarket"),
    ("HotMoneyFactor", "postmarket"),
    ("PerformanceFactor", "postmarket"),
    ("FundamentalFactor", "postmarket"),
    ("BrokerRecommendFactor", "postmarket"),
    ("InsiderBuyFactor", "postmarket"),
    ("ProfitForecastFactor", "postmarket"),
    ("BuybackFactor", "postmarket"),
    ("InstitutionHoldFactor", "postmarket"),
]


def smoke_test(trade_date: str):
    from src.discovery.factors import (
        SectorFactor, MaEntryFactor, LimitFactor, ReboundFactor,
        PopularityFactor, TechnicalFactor, MomentumFactor, MarginFactor,
        ChipFactor, MoneyFlowFactor, HotMoneyFactor, PerformanceFactor,
        FundamentalFactor, BrokerRecommendFactor, InsiderBuyFactor,
        ProfitForecastFactor, BuybackFactor, InstitutionHoldFactor,
    )
    from data_provider.tushare_fetcher import TushareFetcher

    _cls_map = {
        "SectorFactor": SectorFactor,
        "MaEntryFactor": MaEntryFactor,
        "LimitFactor": LimitFactor,
        "ReboundFactor": ReboundFactor,
        "PopularityFactor": PopularityFactor,
        "TechnicalFactor": TechnicalFactor,
        "MomentumFactor": MomentumFactor,
        "MarginFactor": MarginFactor,
        "ChipFactor": ChipFactor,
        "MoneyFlowFactor": MoneyFlowFactor,
        "HotMoneyFactor": HotMoneyFactor,
        "PerformanceFactor": PerformanceFactor,
        "FundamentalFactor": FundamentalFactor,
        "BrokerRecommendFactor": BrokerRecommendFactor,
        "InsiderBuyFactor": InsiderBuyFactor,
        "ProfitForecastFactor": ProfitForecastFactor,
        "BuybackFactor": BuybackFactor,
        "InstitutionHoldFactor": InstitutionHoldFactor,
    }

    tf = TushareFetcher()

    passed, failed, skipped = 0, 0, 0

    for name, mode in ALL_FACTORS:
        cls = _cls_map.get(name)
        if cls is None:
            print(f"  SKIP {name}: class not found")
            skipped += 1
            continue

        factor = cls()
        if not factor.is_available(mode):
            print(f"  SKIP {name}: not available in {mode} mode")
            skipped += 1
            continue

        try:
            print(f"  {name}.fetch_data({trade_date}) ...", end=" ", flush=True)
            df = factor.fetch_data(trade_date, tushare_fetcher=tf)
            if df is None or df.empty:
                print(f"NO DATA (skipped)")
                skipped += 1
                continue
            print(f"{len(df)} stocks", end=" ", flush=True)

            scores = factor.score(df)
            print(f"-> scores [{scores.min():.1f}, {scores.max():.1f}]", end=" ", flush=True)

            reasons = factor.describe(df, scores)
            n_labeled = len(reasons)
            print(f"-> {n_labeled} labeled", end=" ", flush=True)

            print("OK")
            passed += 1
        except Exception:
            print("FAILED")
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Result: {passed} passed, {failed} failed, {skipped} skipped")
    return failed == 0


if __name__ == "__main__":
    trade_date = sys.argv[1] if len(sys.argv) > 1 else None
    if trade_date is None:
        from datetime import date
        trade_date = date.today().strftime("%Y%m%d")

    print(f"Trade date: {trade_date}")
    print(f"{'='*50}")
    ok = smoke_test(trade_date)
    sys.exit(0 if ok else 1)
