#!/usr/bin/env python3
"""重跑 margin / chip 因子评分，补齐 postmarket 缺失数据。

用法:
    python scripts/rerun_margin_chip.py                    # 重跑 margin + chip
    python scripts/rerun_margin_chip.py --factor margin    # 仅 margin
    python scripts/rerun_margin_chip.py --factor chip      # 仅 chip
    python scripts/rerun_margin_chip.py --dry-run          # 预览，不写入

安全: save_factor_score_snapshots 已改为局部删除，只删除本次写入的因子，
不会影响同日期其他因子数据。
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S",
)
logger = logging.getLogger("rerun_mc")

from src.storage import DatabaseManager
from data_provider.tushare_fetcher import TushareFetcher
from sqlalchemy import text


def get_trading_dates(mode: str = "postmarket") -> list:
    db = DatabaseManager()
    with db.get_session() as s:
        rows = s.execute(text(
            "SELECT DISTINCT trade_date FROM factor_score_snapshots "
            "WHERE mode = :mode ORDER BY trade_date",
        ), {"mode": mode}).fetchall()
    return [r[0] for r in rows]


def run_factor(factor, factor_key: str, dates: list, mode: str, dry_run: bool, tf):
    saved_total = 0
    for i, td in enumerate(dates):
        try:
            df = factor.fetch_data(td, tushare_fetcher=tf)
            if df is None or df.empty:
                if (i + 1) % 50 == 0:
                    logger.warning("[%d/%d] %s: fetch_data 返回空", i + 1, len(dates), td)
                continue

            scores = factor.score(df, trade_date=td)
            if scores is None or scores.empty:
                if (i + 1) % 50 == 0:
                    logger.warning("[%d/%d] %s: score 返回空", i + 1, len(dates), td)
                continue

            if dry_run:
                nonzero = (scores > 0).sum()
                if (i + 1) % 50 == 0:
                    logger.info("[DRY-RUN] [%d/%d] %s: %d 条, %d 非零",
                                i + 1, len(dates), td, len(scores), nonzero)
                continue

            db = DatabaseManager()
            saved = db.save_factor_score_snapshots({factor_key: scores}, td, mode)
            saved_total += saved

            nonzero = (scores > 0).sum()
            if (i + 1) % 50 == 0 or nonzero == 0:
                logger.info("[%d/%d] %s: %d 条保存, %d 非零",
                            i + 1, len(dates), td, saved, nonzero)
        except Exception as e:
            logger.warning("[%d/%d] %s: %s", i + 1, len(dates), td, e)

    logger.info("%s 完成: %d 个日期, 共写入 %d 条", factor_key, len(dates), saved_total)


def main():
    parser = argparse.ArgumentParser(description="重跑 margin/chip 因子评分")
    parser.add_argument("--factor", choices=["margin", "chip"], default="margin",
                        help="指定因子 (默认 margin)")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不写入")
    args = parser.parse_args()

    tf = TushareFetcher.get_instance()
    if not tf.is_available:
        logger.error("Tushare 不可用，请检查 Token")
        sys.exit(1)

    # 预加载完整历史交易日历（默认只拉 20 天，历史日期会 fallback 到最新窗口）
    full_cal = tf._get_trade_dates(start_date="20230501")
    logger.info("交易日历缓存: %d 个交易日 (%s ~ %s)", len(full_cal), full_cal[-1], full_cal[0])

    mode = "postmarket"
    dates = get_trading_dates(mode)
    logger.info("%s 待重跑 %d 个日期: %s ~ %s",
                "[DRY-RUN] " if args.dry_run else "", len(dates), dates[0], dates[-1])

    if args.factor == "margin":
        from src.discovery.factors.margin_factor import MarginFactor
        factor = MarginFactor()
        run_factor(factor, "margin", dates, mode, args.dry_run, tf)
    elif args.factor == "chip":
        from src.discovery.factors.chip_factor import ChipFactor
        factor = ChipFactor()
        run_factor(factor, "chip", dates, mode, args.dry_run, tf)


if __name__ == "__main__":
    main()
