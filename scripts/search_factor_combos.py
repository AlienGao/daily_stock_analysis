#!/usr/bin/env python3
"""在固定 7 因子池上搜索 3/4/5 因子组合（方案 A 固定权重），输出排名报告。

方案 A 权重（仅组合内因子取对应权重，不归一化）：
  ranking_momentum=10, margin=20, performance=5, buyback=5,
  profit_forecast=20, institution_hold=15, insider_buy=5

用法:
  python scripts/search_factor_combos.py
  python scripts/search_factor_combos.py --periods 20250101,20260101 --save-top 5
  python scripts/search_factor_combos.py --periods 20260101 --sizes 3,4
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from itertools import combinations
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_provider.tushare_fetcher import TushareFetcher
from src.discovery.factor_backtest_engine import FactorBacktestEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("combo_search")

REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports_simple_backtest" / "combo_search"

FACTOR_POOL = [
    "profit_forecast",
    "margin",
    "institution_hold",
    "ranking_momentum",
    "buyback",
    "insider_buy",
    "performance",
]

PLAN_A_WEIGHTS: dict[str, float] = {
    "ranking_momentum": 10,
    "margin": 20,
    "performance": 5,
    "buyback": 5,
    "profit_forecast": 20,
    "institution_hold": 15,
    "insider_buy": 5,
}

FLM = {
    "institution_hold": "机构持股", "profit_forecast": "盈利预测",
    "buyback": "回购", "insider_buy": "高管增持", "performance": "业绩",
    "ranking_momentum": "排名动量", "margin": "融资融券",
}

HOLD_DAYS = [1, 3, 5, 10, 20]
TOP_N = 1
INITIAL_CAPITAL = 5_000_000
RISK_FREE_RATE = 0.02
PRIMARY_HOLD = 5


def period_stats_for_hold(result_dict: dict, hold_days: int) -> dict | None:
    curves = result_dict.get("capital_curves", {})
    trades = result_dict.get("trade_records", [])
    params = result_dict.get("params", {})
    init_cap = params.get("initial_capital", INITIAL_CAPITAL)
    rfr = params.get("risk_free_rate", RISK_FREE_RATE)
    curve = curves.get(str(hold_days), [])
    closed = [
        t for t in trades
        if t.get("hold_days") == hold_days and t.get("status") in ("closed", "extended")
    ]
    if not curve or len(curve) < 2:
        return None
    final_cap = curve[-1]["capital"]
    total_ret = (final_cap - init_cap) / init_cap
    n_periods = len(curve) - 1
    ann_ret = (1 + total_ret) ** (252 / max(n_periods, 1)) - 1 if total_ret > -1 else total_ret
    wins = sum(1 for t in closed if t.get("return_pct", 0) > 0)
    win_rate = wins / len(closed) if closed else 0
    peak = init_cap
    mdd = 0.0
    for pt in curve:
        if pt["capital"] > peak:
            peak = pt["capital"]
        dd = (peak - pt["capital"]) / peak if peak > 0 else 0
        if dd > mdd:
            mdd = dd
    dr = [(curve[i]["capital"] - curve[i - 1]["capital"]) / curve[i - 1]["capital"] for i in range(1, len(curve))]
    mean_ret = sum(dr) / len(dr) if dr else 0
    std_ret = (sum((r - mean_ret) ** 2 for r in dr) / (len(dr) - 1)) ** 0.5 if len(dr) > 1 else 0
    daily_rf = (1 + rfr) ** (1 / 252) - 1
    sharpe = (mean_ret - daily_rf) / std_ret * (252 ** 0.5) if std_ret > 0 else 0
    return {
        "total_return": total_ret,
        "annual_return": ann_ret,
        "win_rate": win_rate,
        "max_drawdown": mdd,
        "sharpe": sharpe,
        "trade_count": len(closed),
    }


def combo_label(factors: tuple[str, ...]) -> str:
    return "+".join(FLM.get(f, f) for f in factors)


def combo_slug(factors: tuple[str, ...]) -> str:
    parts = [f"w{int(PLAN_A_WEIGHTS[f])}_{f}" for f in sorted(factors, key=lambda x: (-PLAN_A_WEIGHTS[x], x))]
    return "_".join(parts)


def build_combos(sizes: list[int]) -> list[tuple[int, tuple[str, ...]]]:
    out: list[tuple[int, tuple[str, ...]]] = []
    for k in sizes:
        for combo in combinations(FACTOR_POOL, k):
            out.append((k, combo))
    return out


def run_one(engine: FactorBacktestEngine, factors: tuple[str, ...], start_date: str, end_date: str | None):
    fw = {f: PLAN_A_WEIGHTS[f] for f in factors}
    return engine.compute(
        mode="postmarket",
        factor_weights=fw,
        start_date=start_date,
        end_date=end_date,
        top_n=TOP_N,
        hold_days=HOLD_DAYS,
        initial_capital=INITIAL_CAPITAL,
        risk_free_rate=RISK_FREE_RATE,
    )


def save_detail_md(result_dict: dict, factors: tuple[str, ...], dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    mode = result_dict.get("mode", "postmarket")
    filepath = dest_dir / f"backtest_{mode}_{combo_slug(factors)}.md"

    params = result_dict.get("params", {})
    hold_days = params.get("hold_days", HOLD_DAYS)
    init_cap = params.get("initial_capital", INITIAL_CAPITAL)
    date_range = result_dict.get("date_range", {})

    lines = [
        "# 因子组合回测报告",
        "",
        f"- **模式**: {mode}",
        f"- **回测区间**: {date_range.get('start', '?')} ~ {date_range.get('end', '?')}",
        f"- **初始资金**: {init_cap:,.0f}",
        f"- **每期选股数**: {params.get('top_n', TOP_N)}",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 因子",
        "",
        "| 因子 | 权重 |",
        "|------|------|",
    ]
    for f in sorted(factors, key=lambda x: (-PLAN_A_WEIGHTS[x], x)):
        lines.append(f"| {FLM.get(f, f)} | {PLAN_A_WEIGHTS[f]:.1f} |")

    lines.extend([
        "",
        "## 各持有期汇总",
        "",
        "| 持有期 | 总收益 | 年化收益 | 胜率 | 最大回撤 | Sharpe | 交易数 |",
        "|--------|--------|----------|------|----------|--------|--------|",
    ])
    for hd in hold_days:
        stats = period_stats_for_hold(result_dict, hd)
        if stats:
            lines.append(
                f"| {hd}日 | {stats['total_return'] * 100:+.2f}% | {stats['annual_return'] * 100:+.2f}% "
                f"| {stats['win_rate'] * 100:.1f}% | {stats['max_drawdown'] * 100:.2f}% "
                f"| {stats['sharpe']:+.2f} | {stats['trade_count']} |"
            )
        else:
            lines.append(f"| {hd}日 | - | - | - | - | - | - |")

    filepath.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return filepath


def format_sizes_label(sizes: list[int]) -> str:
    """将 sizes 列表格式化为可读文案，如「3 / 4 / 5 因子」。"""
    if not sizes:
        return "（未指定）"
    return " / ".join(str(s) for s in sorted(sizes)) + " 因子"


def save_summary_md(
    rows: list[dict],
    start_date: str,
    end_date: str | None,
    dest: Path,
    sizes: list[int],
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    period_label = f"{start_date} ~ {end_date or '最新'}"
    lines = [
        "# 因子组合搜索排名（方案 A 权重）",
        "",
        f"- **因子池**: {', '.join(FLM.get(f, f) for f in FACTOR_POOL)}",
        f"- **组合规模**: {format_sizes_label(sizes)}",
        f"- **回测区间**: {period_label}",
        f"- **排序**: {PRIMARY_HOLD} 日持有期总收益",
        f"- **组合数**: {len(rows)}",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"## Top 20（{PRIMARY_HOLD}日总收益）",
        "",
        "| 排名 | 规模 | 组合 | 5日收益 | 年化 | Sharpe | 回撤 | 胜率 | 交易数 |",
        "|------|------|------|---------|------|--------|------|------|--------|",
    ]
    for i, r in enumerate(rows[:20], 1):
        lines.append(
            f"| {i} | {r['combo_size']} | {r['label']} | {r['ret5'] * 100:+.2f}% "
            f"| {r['ann5'] * 100:+.2f}% | {r['sharpe5']:+.2f} | {r['mdd5'] * 100:.2f}% "
            f"| {r['win5'] * 100:.1f}% | {r['trades5']} |"
        )

    lines.extend([
        "",
        "## 全部组合",
        "",
        "| 排名 | 规模 | 因子 keys | 5日收益 | Sharpe |",
        "|------|------|-----------|---------|--------|",
    ])
    for i, r in enumerate(rows, 1):
        lines.append(
            f"| {i} | {r['combo_size']} | {r['factors_key']} | {r['ret5'] * 100:+.2f}% | {r['sharpe5']:+.2f} |"
        )
    lines.append("")
    dest.write_text("\n".join(lines), encoding="utf-8")


def load_period_rows(start_date: str) -> list[dict]:
    """从区间目录 CSV 加载结果（若不存在返回空列表）。"""
    csv_path = REPORTS_DIR / start_date / "results.csv"
    if not csv_path.exists():
        return []
    rows: list[dict] = []
    with csv_path.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            for k in ("ret5", "ann5", "sharpe5", "mdd5", "win5", "ret1", "ret3", "ret10", "ret20"):
                if row.get(k) not in (None, ""):
                    row[k] = float(row[k])
            if row.get("trades5") not in (None, ""):
                row["trades5"] = int(float(row["trades5"]))
            if row.get("combo_size") not in (None, ""):
                row["combo_size"] = int(float(row["combo_size"]))
            rows.append(row)
    rows.sort(key=lambda x: x.get("ret5", 0), reverse=True)
    return rows


def save_cross_period_summary(periods: list[str], sizes: list[int], end_date: str | None) -> Path:
    """多区间跑完后生成跨区间对比总结。"""
    dest = REPORTS_DIR / "cross_period_summary.md"
    period_data: dict[str, list[dict]] = {p: load_period_rows(p) for p in periods}

    lines = [
        "# 因子组合搜索 — 跨区间总结（方案 A 权重）",
        "",
        f"- **因子池**: {', '.join(FLM.get(f, f) for f in FACTOR_POOL)}",
        f"- **组合规模**: {format_sizes_label(sizes)}",
        f"- **区间**: {', '.join(periods)}（结束日: {end_date or '最新'}）",
        f"- **排序基准**: {PRIMARY_HOLD} 日持有期总收益",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 各区间概览",
        "",
        "| 区间 | 有效组合数 | Top1 组合 | 5日收益 | Sharpe |",
        "|------|------------|-----------|---------|--------|",
    ]
    for p in periods:
        rows = period_data.get(p, [])
        if rows:
            top = rows[0]
            lines.append(
                f"| {p} | {len(rows)} | {top['label']} | {top['ret5'] * 100:+.2f}% | {top['sharpe5']:+.2f} |"
            )
        else:
            lines.append(f"| {p} | 0 | - | - | - |")

    if len(periods) >= 2:
        p_a, p_b = periods[0], periods[1]
        map_a = {r["factors_key"]: r for r in period_data.get(p_a, [])}
        map_b = {r["factors_key"]: r for r in period_data.get(p_b, [])}
        common_keys = set(map_a) & set(map_b)
        blended: list[dict] = []
        for key in common_keys:
            ra, rb = map_a[key], map_b[key]
            blended.append({
                "factors_key": key,
                "label": ra.get("label") or rb.get("label", key),
                "combo_size": ra.get("combo_size") or rb.get("combo_size"),
                "ret5_a": ra["ret5"],
                "ret5_b": rb["ret5"],
                "avg_ret5": (ra["ret5"] + rb["ret5"]) / 2,
                "sharpe_a": ra["sharpe5"],
                "sharpe_b": rb["sharpe5"],
            })
        blended.sort(key=lambda x: x["avg_ret5"], reverse=True)

        lines.extend([
            "",
            f"## 两区间 Top 15（按 5 日收益均值，{p_a} + {p_b}）",
            "",
            f"| 排名 | 规模 | 组合 | {p_a} 5日 | {p_b} 5日 | 均值 |",
            "|------|------|------|---------|---------|------|",
        ])
        for i, r in enumerate(blended[:15], 1):
            lines.append(
                f"| {i} | {r['combo_size']} | {r['label']} "
                f"| {r['ret5_a'] * 100:+.2f}% | {r['ret5_b'] * 100:+.2f}% | {r['avg_ret5'] * 100:+.2f}% |"
            )

        top_a = {r["factors_key"] for r in period_data.get(p_a, [])[:20]}
        top_b = {r["factors_key"] for r in period_data.get(p_b, [])[:20]}
        robust = top_a & top_b
        if robust:
            robust_rows = [r for r in blended if r["factors_key"] in robust]
            robust_rows.sort(key=lambda x: x["avg_ret5"], reverse=True)
            lines.extend([
                "",
                "## 两区间均进 Top 20 的稳健组合",
                "",
                f"| 组合 | {p_a} 5日 | {p_b} 5日 | 均值 | Sharpe({p_a}/{p_b}) |",
                "|------|---------|---------|------|-------------------|",
            ])
            for r in robust_rows[:20]:
                lines.append(
                    f"| {r['label']} | {r['ret5_a'] * 100:+.2f}% | {r['ret5_b'] * 100:+.2f}% "
                    f"| {r['avg_ret5'] * 100:+.2f}% | {r['sharpe_a']:+.2f} / {r['sharpe_b']:+.2f} |"
                )

    for p in periods:
        rows = period_data.get(p, [])
        if not rows:
            continue
        lines.extend([
            "",
            f"## {p} 区间 — 各规模 Top 3（5日收益）",
            "",
            "| 规模 | 组合 | 5日收益 | Sharpe | 回撤 |",
            "|------|------|---------|--------|------|",
        ])
        for size in sorted(sizes):
            size_rows = [r for r in rows if r.get("combo_size") == size][:3]
            for r in size_rows:
                lines.append(
                    f"| {size} | {r['label']} | {r['ret5'] * 100:+.2f}% "
                    f"| {r['sharpe5']:+.2f} | {r['mdd5'] * 100:.2f}% |"
                )

    lines.append("")
    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def save_csv(rows: list[dict], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "rank", "combo_size", "factors_key", "label", "ret5", "ann5", "sharpe5", "mdd5", "win5", "trades5",
        "ret1", "ret3", "ret10", "ret20",
    ]
    with dest.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for i, r in enumerate(rows, 1):
            w.writerow({"rank": i, **{k: r.get(k) for k in fields if k != "rank"}})


def search_period(
    engine: FactorBacktestEngine,
    start_date: str,
    end_date: str | None,
    sizes: list[int],
    save_top: int,
) -> list[dict]:
    combos = build_combos(sizes)
    logger.info("区间 %s ~ %s：共 %d 个组合", start_date, end_date or "最新", len(combos))
    rows: list[dict] = []
    result_by_key: dict[str, dict] = {}

    for idx, (k, factors) in enumerate(combos, 1):
        t0 = time.time()
        logger.info("[%d/%d] %d因子: %s", idx, len(combos), k, combo_label(factors))
        try:
            result = run_one(engine, factors, start_date, end_date)
        except Exception:
            logger.exception("回测失败: %s", factors)
            continue
        if result is None:
            logger.warning("数据不足，跳过: %s", factors)
            continue

        rd = asdict(result)
        stats5 = period_stats_for_hold(rd, PRIMARY_HOLD)
        if not stats5:
            continue

        row = {
            "combo_size": k,
            "factors": factors,
            "factors_key": "+".join(factors),
            "label": combo_label(factors),
            "ret5": stats5["total_return"],
            "ann5": stats5["annual_return"],
            "sharpe5": stats5["sharpe"],
            "mdd5": stats5["max_drawdown"],
            "win5": stats5["win_rate"],
            "trades5": stats5["trade_count"],
        }
        for hd in HOLD_DAYS:
            st = period_stats_for_hold(rd, hd)
            row[f"ret{hd}"] = st["total_return"] if st else None
        rows.append(row)
        result_by_key[row["factors_key"]] = rd
        logger.info(
            "  完成 %.1fs — 5日 %+.2f%% Sharpe %+.2f",
            time.time() - t0, stats5["total_return"] * 100, stats5["sharpe"],
        )

    rows.sort(key=lambda x: x["ret5"], reverse=True)

    period_dir = REPORTS_DIR / start_date
    save_csv(rows, period_dir / "results.csv")
    save_summary_md(rows, start_date, end_date, period_dir / "summary.md", sizes)

    if save_top > 0 and rows:
        detail_dir = period_dir / "top_details"
        saved = 0
        for r in rows[:save_top]:
            rd = result_by_key.get(r["factors_key"])
            if rd:
                save_detail_md(rd, r["factors"], detail_dir)
                saved += 1
        logger.info("已保存 Top %d 详情报告到 %s", saved, detail_dir)

    return rows


def parse_args():
    p = argparse.ArgumentParser(description="七因子 3/4/5 组合搜索（方案 A 权重）")
    p.add_argument("--periods", default="20250101,20260101", help="逗号分隔开始日期")
    p.add_argument("--end-date", default=None, help="结束日期 YYYYMMDD，默认到最新")
    p.add_argument("--sizes", default="3,4,5", help="组合规模，逗号分隔")
    p.add_argument("--save-top", type=int, default=10, help="每区间保存 Top N 详细 MD，0 不保存")
    p.add_argument("--dry-run", action="store_true", help="只打印组合数量")
    return p.parse_args()


def main():
    args = parse_args()
    sizes = [int(s.strip()) for s in args.sizes.split(",") if s.strip()]
    periods = [p.strip() for p in args.periods.split(",") if p.strip()]

    combos = build_combos(sizes)
    logger.info("因子池: %s", ", ".join(FACTOR_POOL))
    logger.info("方案 A 权重: %s", PLAN_A_WEIGHTS)
    logger.info("每区间组合数: %d (sizes=%s)", len(combos), sizes)

    if args.dry_run:
        for k in sizes:
            logger.info("  %d 因子: %d 组", k, len(list(combinations(FACTOR_POOL, k))))
        return 0

    fetcher = TushareFetcher.get_instance()
    engine = FactorBacktestEngine(fetcher)

    for start_date in periods:
        logger.info("=" * 60)
        logger.info("开始区间: %s", start_date)
        search_period(engine, start_date, args.end_date, sizes, args.save_top)
        engine._price_cache.clear()
        FactorBacktestEngine._spot_df_cache = ()

    if len(periods) > 1:
        cross_path = save_cross_period_summary(periods, sizes, args.end_date)
        logger.info("跨区间总结已写入: %s", cross_path)

    logger.info("全部完成，报告目录: %s", REPORTS_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
