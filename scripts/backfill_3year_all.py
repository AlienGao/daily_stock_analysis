#!/usr/bin/env python3
"""一键回填 3 年回测所需全部数据。

按依赖顺序依次执行 6 个子脚本（数据表 → 计算表），
每个子脚本独立处理断点续跑（已缓存日期自动跳过）。

顺序:
  1. backfill_money_flow.py           — 资金流向
  2. backfill_limit_pool.py           — 涨跌停
  3. backfill_broker_recommend_monthly.py — 券商金股
  4. backfill_popularity_rank.py      — 人气排行
  5. backfill_tech_indicators.py      — 技术指标
  6. backfill_factor_snapshots.py     — 因子得分快照 (依赖前 5 步 + stock_daily)

用法:
    python scripts/backfill_3year_all.py              # 全部回填
    python scripts/backfill_3year_all.py --dry-run    # 预览各表缺多少
    python scripts/backfill_3year_all.py --step 2     # 仅执行第 2 步 (limit_pool)
    python scripts/backfill_3year_all.py --from 3     # 从第 3 步开始
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_3y")

STEPS = [
    # (label, script, extra_args)
    ("money_flow 资金流向",            "scripts/backfill_money_flow.py",              []),
    ("limit_pool 涨跌停",              "scripts/backfill_limit_pool.py",              []),
    ("broker_recommend 券商金股",       "scripts/backfill_broker_recommend_monthly.py", []),
    ("popularity_rank 人气排行",        "scripts/backfill_popularity_rank.py",         []),
    ("tech_indicators 技术指标",        "scripts/backfill_tech_indicators.py",         ["--delay", "1.0"]),
    ("factor_snapshots 因子得分快照",    "scripts/backfill_factor_snapshots.py",        ["--start", "20230517"]),
]


def run_step(step_index: int, dry_run: bool) -> tuple[bool, float]:
    label, script, extra_args = STEPS[step_index]
    cmd = [sys.executable, str(ROOT / script)]
    if dry_run:
        cmd.append("--dry-run")
    cmd.extend(extra_args)

    logger.info("=" * 60)
    logger.info("Step %d/%d: %s", step_index + 1, len(STEPS), label)
    logger.info("  %s", " ".join(cmd))
    logger.info("=" * 60)

    t0 = time.time()
    try:
        result = subprocess.run(cmd, cwd=str(ROOT), timeout=86400)
        elapsed = time.time() - t0
        if result.returncode != 0:
            logger.error("Step %d FAILED (exit=%d, %.0fs)", step_index + 1, result.returncode, elapsed)
            return False, elapsed
        logger.info("Step %d OK (%.0fs)", step_index + 1, elapsed)
        return True, elapsed
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        logger.error("Step %d TIMEOUT (%.0fs)", step_index + 1, elapsed)
        return False, elapsed
    except Exception as e:
        elapsed = time.time() - t0
        logger.error("Step %d ERROR: %s (%.0fs)", step_index + 1, e, elapsed)
        return False, elapsed


def main():
    parser = argparse.ArgumentParser(description="一键回填 3 年回测所需全部数据")
    parser.add_argument("--dry-run", action="store_true", help="预览各表缺失量，不实际写入")
    parser.add_argument("--step", type=int, default=0, help="仅执行第 N 步 (1-%d)" % len(STEPS))
    parser.add_argument("--from", dest="from_step", type=int, default=1, help="从第 N 步开始")
    args = parser.parse_args()

    if args.step:
        idx = args.step - 1
        if idx < 0 or idx >= len(STEPS):
            logger.error("--step 必须在 1-%d 之间", len(STEPS))
            sys.exit(1)
        success, _ = run_step(idx, dry_run=args.dry_run)
        sys.exit(0 if success else 1)

    start_idx = max(0, args.from_step - 1)
    steps_to_run = list(range(start_idx, len(STEPS)))
    logger.info("将执行 %d 步 (%d-%d)%s",
                len(steps_to_run), start_idx + 1, len(STEPS),
                " [DRY-RUN]" if args.dry_run else "")

    total_elapsed = 0.0
    failed: list[str] = []
    for idx in steps_to_run:
        success, elapsed = run_step(idx, dry_run=args.dry_run)
        total_elapsed += elapsed
        if not success:
            failed.append(STEPS[idx][0])
            if not args.dry_run:
                logger.warning("Step %d 失败但继续执行后续步骤...", idx + 1)

    logger.info("=" * 60)
    logger.info("全部步骤完成 | 总耗时 %.0fs (%.1f 分钟)", total_elapsed, total_elapsed / 60)
    if failed:
        logger.warning("失败步骤: %s", ", ".join(failed))
        sys.exit(1)
    else:
        logger.info("所有步骤均成功")


if __name__ == "__main__":
    main()
