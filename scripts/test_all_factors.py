#!/usr/bin/env python3
"""逐一测试所有因子：每次只选一个因子，使用默认回测参数，生成 MD 报告到 reports_simple_backtest/。"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import re
import time
import numpy as np
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("factor_test")

from data_provider.tushare_fetcher import TushareFetcher
from src.discovery.factor_backtest_engine import FactorBacktestEngine
from src.discovery.factors import __all__ as all_factor_names
from src.discovery.factors.base import BaseFactor

HOLD_DAYS = [1, 3, 5, 10, 20]
TOP_N = 3
INITIAL_CAPITAL = 5_000_000
RISK_FREE_RATE = 0.02
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports_simple_backtest"


def get_available_factors():
    """获取所有 postmarket 可用的因子，返回 {name: weight}。"""
    factors = {}
    for name in all_factor_names:
        if name in ("BaseFactor", "DiscoveryResult"):
            continue
        try:
            mod = __import__("src.discovery.factors", fromlist=[name])
            cls = getattr(mod, name)
            if isinstance(cls, type) and issubclass(cls, BaseFactor) and cls is not BaseFactor:
                inst = cls()
                if inst.available_postmarket:
                    factors[inst.name] = inst.weight
        except Exception:
            pass
    return factors


FLM = {
    "money_flow": "资金流向", "margin": "融资融券", "chip": "筹码分布",
    "technical": "技术形态", "limit": "涨跌停", "fundamental": "基本面",
    "institution_hold": "机构持股", "profit_forecast": "盈利预测",
    "buyback": "回购", "insider_buy": "高管增持", "broker_recommend": "券商推荐",
    "popularity": "人气", "hot_money": "游资", "performance": "业绩",
    "momentum": "动量", "rebound": "反弹", "sector": "板块", "ma_entry": "均线",
    "ranking_momentum": "排名动量", "concept_heat": "概念热度",
    "alpha042": "均值回归Alpha042", "vwap_deviation": "VWAP偏离",
    "gap_reversal": "跳空反转", "liquid_oversold": "流动性超卖",
    "vwap_reversal": "VWAP动量反转", "gtja114": "GTJA114",
    "alpha60": "Alpha60收盘位置", "money_flow_osc": "资金流振荡",
    "market_cap": "小市值",
}


def _period_stats(curve, trades, init_cap, rfr):
    closed = [t for t in trades if t.get("status") in ("closed", "extended")]
    if not curve or len(curve) < 2:
        return None
    fc = curve[-1]["capital"]
    tr = (fc - init_cap) / init_cap
    n = len(curve) - 1
    ar = (1 + tr) ** (252 / max(n, 1)) - 1 if tr > -1 else tr
    wins = sum(1 for t in closed if t.get("return_pct", 0) > 0)
    wr = wins / len(closed) if closed else 0
    peak = init_cap
    mdd = 0.0
    for pt in curve:
        if pt["capital"] > peak:
            peak = pt["capital"]
        dd = (peak - pt["capital"]) / peak if peak > 0 else 0
        if dd > mdd:
            mdd = dd
    dr = [(curve[i]["capital"] - curve[i - 1]["capital"]) / curve[i - 1]["capital"] for i in range(1, len(curve))]
    mr = np.mean(dr) if dr else 0
    sr = np.std(dr, ddof=1) if len(dr) > 1 else 0
    drf = (1 + rfr) ** (1 / 252) - 1
    sh = (mr - drf) / sr * np.sqrt(252) if sr > 0 else 0
    return {
        "total_return": tr, "annual_return": ar, "win_rate": wr,
        "max_drawdown": mdd, "sharpe": sh, "trade_count": len(closed),
        "open_count": sum(1 for t in trades if t.get("status") == "open"),
        "canceled_count": sum(1 for t in trades if t.get("status") == "canceled"),
    }


def save_md(factor_name: str, d: dict):
    """保存单因子回测 Markdown 报告。"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    weight = int(d.get('factors', [{}])[0].get('weight', 0)) if d.get('factors') else 0
    filepath = REPORTS_DIR / f"backtest_postmarket_w{weight}_{factor_name}.md"

    params = d.get("params", {})
    hold_days = params.get("hold_days", [])
    init_cap = params.get("initial_capital", INITIAL_CAPITAL)
    rfr = params.get("risk_free_rate", RISK_FREE_RATE)
    dr = d.get("date_range", {})
    curves = d.get("capital_curves", {})
    all_trades = d.get("trade_records", [])
    rank_ic = d.get("rank_ic", {})

    lines = [
        f"# {FLM.get(factor_name, factor_name)} 单因子回测",
        "",
        f"- **因子**: {factor_name}",
        f"- **回测区间**: {dr.get('start', '?')} ~ {dr.get('end', '?')}",
        f"- **初始资金**: {init_cap:,.0f}",
        f"- **无风险利率**: {rfr * 100:.1f}%",
        f"- **每期选股数**: {params.get('top_n', '-')}",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 因子",
        "",
        "| 因子 | 权重 |",
        "|------|------|",
        f"| {factor_name} | {d.get('factors', [{}])[0].get('weight', '-') if d.get('factors') else '-'} |",
        "",
        "## 各持有期汇总",
        "",
        "| 持有期 | 总收益 | 年化收益 | 胜率 | 最大回撤 | Sharpe | 交易数 | 持仓中 | 跳过 |",
        "|--------|--------|----------|------|----------|--------|--------|--------|------|",
    ]
    for hd in hold_days:
        hds = str(hd)
        stats = _period_stats(curves.get(hds, []), [t for t in all_trades if t.get("hold_days") == hd], init_cap, rfr)
        if stats:
            lines.append(
                f"| {hd}日 | {stats['total_return'] * 100:+.2f}% | {stats['annual_return'] * 100:+.2f}% "
                f"| {stats['win_rate'] * 100:.1f}% | {stats['max_drawdown'] * 100:.2f}% "
                f"| {stats['sharpe']:+.2f} | {stats['trade_count']} "
                f"| {stats['open_count']} | {stats['canceled_count']} |")
        else:
            lines.append(f"| {hd}日 | - | - | - | - | - | - | - | - |")

    lines.append("")
    if rank_ic:
        all_factors = sorted(set(fn for hd_ic in rank_ic.values() for fn in hd_ic))
        lines.append("| 因子 | " + " | ".join(f"{h}日" for h in hold_days) + " |")
        lines.append("|------|" + "|".join("------" for _ in hold_days) + "|")
        for fn in all_factors:
            label = FLM.get(fn, fn)
            vals = [f"{rank_ic.get(str(h), {}).get(fn, 0):+.4f}" for h in hold_days]
            lines.append(f"| {label} | " + " | ".join(vals) + " |")

    lines.append("")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("  报告已保存: %s", filepath)


def main():
    fetcher = TushareFetcher.get_instance()
    engine = FactorBacktestEngine(fetcher)
    factors = get_available_factors()
    logger.info("共 %d 个 postmarket 可用因子: %s", len(factors), sorted(factors.keys()))

    results = []
    for i, (factor_name, fw) in enumerate(factors.items(), 1):
        logger.info("[%d/%d] 测试因子: %s (权重=%.1f)", i, len(factors), factor_name, fw)
        start_time = time.time()
        try:
            result = engine.compute(
                mode="postmarket",
                factor_weights={factor_name: fw},
                start_date="20250101",
                top_n=TOP_N,
                hold_days=HOLD_DAYS,
                initial_capital=INITIAL_CAPITAL,
                risk_free_rate=RISK_FREE_RATE,
                progress_cb=lambda msg: logger.info("  %s", msg),
            )
        except Exception as e:
            logger.exception("因子 %s 回测异常", factor_name)
            results.append((factor_name, None, str(e)))
            continue

        elapsed = time.time() - start_time

        if result is None:
            logger.warning("因子 %s 回测数据不足，跳过", factor_name)
            results.append((factor_name, None, "数据不足"))
            continue

        from dataclasses import asdict
        result_dict = asdict(result)
        save_md(factor_name, result_dict)

        elapsed_str = f"{elapsed:.1f}s"
        cr = result_dict.get("summary", {}).get("cumulative_return", "N/A")
        logger.info(
            "因子 %s 完成 (%s) — 累计收益: %s",
            factor_name, elapsed_str, f"{cr * 100:+.2f}%" if isinstance(cr, (int, float)) else cr,
        )
        results.append((factor_name, result_dict, None))

    # 汇总
    logger.info("=" * 50)
    logger.info("全部因子测试完成：")
    for name, result_dict, error in results:
        if error:
            logger.info("  %-25s ❌ %s", name, error)
        elif result_dict is None:
            logger.info("  %-25s ⚠️ 无结果", name)
        else:
            cr = result_dict.get("summary", {}).get("cumulative_return", 0)
            logger.info("  %-25s %+.2f%%", name, cr * 100)

    # 生成总结报告
    save_summary(results)


def save_summary(results):
    """生成所有因子横向对比的总结 MD。"""
    filepath = REPORTS_DIR / "summary_all_factors.md"

    # 读取前一次排名
    prev_rank: dict = {}
    if filepath.exists():
        with open(filepath, "r") as fh:
            prev_content = fh.read()
        for m in re.finditer(r'\|\s*(\d+)\s*\|\s*(.+?)\s*\|', prev_content):
            rank = int(m.group(1))
            label = m.group(2).strip()
            # 跳过表头和非因子行
            if label in ("排名", "因子", "------"):
                continue
            # reverse lookup label -> factor name
            fn = None
            for k, v in FLM.items():
                if v == label:
                    fn = k
                    break
            if fn:
                prev_rank[fn] = rank

    # 收集有效结果
    valid = []
    for name, d, err in results:
        if err or d is None:
            continue
        s = d.get("summary", {})
        curves = d.get("capital_curves", {})
        all_trades = d.get("trade_records", [])
        params = d.get("params", {})
        init_cap = params.get("initial_capital", INITIAL_CAPITAL)
        rfr = params.get("risk_free_rate", RISK_FREE_RATE)
        stats5 = _period_stats(
            curves.get("5", []),
            [t for t in all_trades if t.get("hold_days") == 5],
            init_cap, rfr,
        )
        rank_ic = d.get("rank_ic", {})
        ic5 = rank_ic.get("5", {}).get(name, 0) if rank_ic else 0
        if stats5:
            valid.append({
                "name": name,
                "label": FLM.get(name, name),
                "tr": stats5["total_return"],
                "ar": stats5["annual_return"],
                "wr": stats5["win_rate"],
                "mdd": stats5["max_drawdown"],
                "sh": stats5["sharpe"],
                "trades": stats5["trade_count"],
                "ic5": ic5,
            })

    # 按累计收益降序排列
    valid.sort(key=lambda x: x["tr"], reverse=True)

    # 分类
    effective = [v for v in valid if v["tr"] > 0]
    ineffective = [v for v in valid if v["tr"] <= 0]

    lines = [
        "# 因子全面测试总结",
        "",
        f"- **测试因子数**: {len(valid)}",
        f"- **有效因子 (累计收益>0)**: {len(effective)}",
        f"- **无效因子 (累计收益≤0)**: {len(ineffective)}",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 综合排名（按 5 日持有期累计收益）",
        "",
        "| 排名 | 变化 | 因子 | 累计收益 | 年化收益 | 胜率 | 最大回撤 | Sharpe | IC(5日) | 交易数 |",
        "|------|------|------|----------|----------|------|----------|--------|---------|--------|",
    ]

    for rank, v in enumerate(valid, 1):
        prev = prev_rank.get(v["name"])
        if prev is None:
            change = "🆕"
        elif rank < prev:
            change = f"↑{prev - rank}"
        elif rank > prev:
            change = f"↓{rank - prev}"
        else:
            change = "-"
        lines.append(
            f"| {rank} | {change} | {v['label']} | {v['tr'] * 100:+.2f}% | {v['ar'] * 100:+.2f}% "
            f"| {v['wr'] * 100:.1f}% | {v['mdd'] * 100:.2f}% "
            f"| {v['sh']:+.2f} | {v['ic5']:+.4f} | {v['trades']} |")

    lines.append("")
    lines.append("## 有效因子")
    lines.append("")
    if effective:
        lines.append("| 因子 | 累计收益 | 胜率 | Sharpe | IC(5日) |")
        lines.append("|------|----------|------|--------|---------|")
        for v in effective:
            lines.append(f"| {v['label']} | {v['tr'] * 100:+.2f}% | {v['wr'] * 100:.1f}% | {v['sh']:+.2f} | {v['ic5']:+.4f} |")
    else:
        lines.append("无")

    lines.append("")
    lines.append("## 无效因子")
    lines.append("")
    if ineffective:
        lines.append("| 因子 | 累计收益 | 胜率 | Sharpe | IC(5日) |")
        lines.append("|------|----------|------|--------|---------|")
        for v in ineffective:
            lines.append(f"| {v['label']} | {v['tr'] * 100:+.2f}% | {v['wr'] * 100:.1f}% | {v['sh']:+.2f} | {v['ic5']:+.4f} |")
    else:
        lines.append("无")

    lines.append("")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("总结报告已保存: %s", filepath)


if __name__ == "__main__":
    main()
