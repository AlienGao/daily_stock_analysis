# -*- coding: utf-8 -*-
"""用同花顺板块接口重算今日盘中排名，并更新 DB + JSON + MD。

用法:
    https_proxy=http://127.0.0.1:42484 python scripts/recalc_intraday_ths.py

更新内容:
    1. factor_score_snapshots 表中 sector 因子分
    2. ScanResultIntraday 表中 total_score / rank / factor_scores_json
    3. discovery_reports/intraday_{date}_topn.json
    4. discovery_reports/intraday_{date}.md
"""
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载 .env
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logging.getLogger("src.discovery").setLevel(logging.INFO)


def _ts_to_bare(ts_code: str) -> str:
    """Extract 6-digit code from any ts_code format."""
    return str(ts_code).split(".")[0].zfill(6)


def main():
    trade_date = os.getenv("TRADE_DATE", date.today().strftime("%Y%m%d"))

    # ── 1. THS 板块动量 ──
    from src.discovery.factors.sector_factor import SectorFactor

    sf = SectorFactor()
    ths_momentum = sf._compute_intraday_momentum_from_board(trade_date)
    if ths_momentum is None or ths_momentum.empty:
        print("[1] 同花顺接口返回空，降级到个股聚合")
        ths_momentum = sf._compute_intraday_momentum_from_stocks(trade_date)
    if ths_momentum is None or ths_momentum.empty:
        print("FAIL: 板块动量计算完全失败")
        sys.exit(1)

    n_boards = sf._cached_industry_deltas and len(sf._cached_industry_deltas)
    print(f"[1] THS 板块动量: {len(ths_momentum)} 只个股, 板块数={n_boards}")

    # ── 2. 加载 DB factor_score_snapshots ──
    import pandas as pd
    from src.storage import DatabaseManager, FactorScoreSnapshot, ScanResultIntraday

    db = DatabaseManager()
    with db.get_session() as s:
        rows = (
            s.query(FactorScoreSnapshot)
            .filter(
                FactorScoreSnapshot.trade_date == trade_date,
                FactorScoreSnapshot.mode == "intraday",
            )
            .all()
        )
    if not rows:
        print("FAIL: factor_score_snapshots 无今日盘中数据")
        sys.exit(1)

    records = []
    for r in rows:
        records.append({
            "ts_code": _ts_to_bare(r.ts_code),
            "factor_name": r.factor_name,
            "score": r.score or 0.0,
        })
    df_snaps = pd.DataFrame(records)
    factor_scores = df_snaps.pivot_table(
        index="ts_code", columns="factor_name", values="score", aggfunc="max"
    )
    print(f"[2] factor_score_snapshots: {len(factor_scores)} 只个股, "
          f"因子列={list(factor_scores.columns)}")

    if "sector" not in factor_scores.columns:
        print("FAIL: 无 sector 因子列")
        sys.exit(1)

    # ── 3. 读取实际权重（from env / config）──
    from src.discovery.config import get_discovery_config
    cfg = get_discovery_config()

    # 盘中因子名 → 权重
    intraday_factor_names = ["ma_entry", "sector", "momentum",
                              "ranking_momentum", "rebound", "popularity"]
    weights = {}
    for fn in intraday_factor_names:
        w = getattr(cfg, f"weight_{fn}_intraday", None)
        if w is None:
            w = getattr(cfg, f"weight_{fn}", 0.0)
        weights[fn] = w

    print(f"[3] 盘中权重: {json.dumps(weights, ensure_ascii=False)} "
          f"(total={sum(weights.values()):.0f})")

    # ── 4. 重算 sector 总分 ──
    old_sector = factor_scores["sector"].copy()

    # THS 动量归一化到 0-35（与 sector 因子的 intraday_momentum 子信号范围匹配）
    ths_momentum_norm = ths_momentum.clip(0, 40) / 40 * 35
    ths_aligned = ths_momentum_norm.reindex(factor_scores.index).fillna(0)

    # 旧 sector = 5 个非动量子信号(0-65) + intraday_momentum(0-35)
    # 保留非动量部分(65%)，替换动量部分
    new_sector = old_sector.fillna(0) * 0.65 + ths_aligned
    new_sector = new_sector.clip(0, 100)

    n_up = (new_sector > old_sector.fillna(0) + 0.1).sum()
    n_down = (new_sector < old_sector.fillna(0) - 0.1).sum()
    print(f"[4] Sector 重算: 提升={n_up}, 下降={n_down}, "
          f"不变={len(new_sector) - n_up - n_down}")

    factor_scores["sector"] = new_sector

    # ── 5. 综合分 ──
    total_weight = sum(weights.values())
    composite = pd.Series(0.0, index=factor_scores.index)
    for fn, w in weights.items():
        if fn in factor_scores.columns:
            composite += factor_scores[fn].fillna(0) * w / total_weight

    # ── 5. 跑 StockScorer 获取 tech_score（top 300，与 engine 行为一致）──
    print(f"[5] 运行 StockScorer 技术评分…")
    alpha = get_discovery_config().effective_score_blend_alpha

    # 先用因子综合分粗排，取 top 300 跑 StockScorer
    composite_rough = pd.Series(0.0, index=factor_scores.index)
    for fn, w in weights.items():
        if fn in factor_scores.columns:
            composite_rough += factor_scores[fn].fillna(0) * w / total_weight
    top_codes = composite_rough.nlargest(300).index.tolist()

    # 获取交易日列表
    from src.storage import DailyBasic
    with db.get_session() as s:
        trading_days = [
            r[0] for r in
            s.query(DailyBasic.trade_date).distinct().order_by(DailyBasic.trade_date.desc()).limit(120).all()
        ]
    trading_days.sort()

    # 批量 StockScorer（return_full=True 获取子维度分）
    from src.discovery.factor_backtest_engine import FactorBacktestEngine
    comp_dict = {code: float(composite_rough.get(code, 50)) for code in top_codes}
    tech_full = FactorBacktestEngine._batch_stockscorer_static(
        top_codes, trade_date, trading_days, composite=comp_dict, return_full=True
    )

    # 构建 tech_series 和 sub-score 映射
    tech_series = pd.Series(50.0, index=factor_scores.index)
    tech_details = {}  # code → {composite, rr_score, market_score, ...}
    for code, detail in (tech_full or {}).items():
        if isinstance(detail, dict):
            tech_series[code] = detail.get("composite", 50.0)
            tech_details[code] = detail
        else:
            tech_series[code] = float(detail) if detail else 50.0

    n_scored = sum(1 for v in tech_series.values if v != 50.0)
    print(f"  StockScorer: {n_scored} 只评分, alpha={alpha}")

    final_composite = alpha * composite + (1 - alpha) * tech_series

    # ── 6. 行业映射 + 股票名称 ──
    ths_map = db.get_ths_industry_map()
    spot = db.get_realtime_spot()
    names = {}
    if spot is not None and not spot.empty:
        for idx, row in spot.iterrows():
            names[_ts_to_bare(str(idx))] = str(row.get("name", "")) or ""

    # ── 7. 构建 Top N 结构化数据 ──
    ranked = final_composite.sort_values(ascending=False)
    top_n = 4
    top_entries = []

    for i, (code, score) in enumerate(ranked.head(top_n).items(), 1):
        bare = _ts_to_bare(str(code))
        name = names.get(bare, "")
        industry = ths_map.get(bare, "未知")

        fs_dict = {}
        for fn in intraday_factor_names:
            if fn in factor_scores.columns:
                val = factor_scores[fn].get(code, 0)
                fs_dict[fn] = round(float(val) if not pd.isna(val) else 0.0, 1)
            else:
                fs_dict[fn] = 0.0

        entry = {
            "rank": i,
            "stock_code": bare,
            "stock_name": name,
            "score": round(float(score), 1),
            "sector": industry,
            "factor_scores": fs_dict,
            "factor_weights": {fn: weights.get(fn, 0.0) for fn in intraday_factor_names},
            "reasons": [],
            "buy_price_low": None,
            "buy_price_high": None,
            "stop_loss": None,
            "take_profit_1": None,
            "take_profit_2": None,
            "discovered_at": time.strftime("%H:%M:%S", time.localtime()),
            "price_at_discovery": None,
            "pct_chg": 0.0,
            "tech_score": round(float(tech_series.get(code, 50.0)), 1),
            "rr_score": round(float(tech_details.get(code, {}).get("rr_score", 0)), 1),
            "market_score": round(float(tech_details.get(code, {}).get("market_score", 0)), 1),
            "sector_score": round(float(tech_details.get(code, {}).get("sector_score", 0)), 1),
            "volume_score": round(float(tech_details.get(code, {}).get("volume_score", 0)), 1),
            "position_score": round(float(tech_details.get(code, {}).get("position_score", 0)), 1),
            "formation_score": round(float(tech_details.get(code, {}).get("formation_score", 0)), 1),
            "composite_score": round(float(score), 1),
        }
        top_entries.append(entry)

    # ── 8. 更新 DB ──
    print(f"\n[6] 更新数据库…")

    # 8a. factor_score_snapshots: upsert sector scores (DB stores bare codes)
    updated_fss = 0
    inserted_fss = 0
    with db.get_session() as s:
        for bare_code in factor_scores.index:
            new_sec = float(new_sector[bare_code])
            if pd.isna(new_sec):
                new_sec = 0.0
            new_sec = round(new_sec, 2)

            existing = s.query(FactorScoreSnapshot).filter(
                FactorScoreSnapshot.trade_date == trade_date,
                FactorScoreSnapshot.ts_code == bare_code,
                FactorScoreSnapshot.mode == "intraday",
                FactorScoreSnapshot.factor_name == "sector",
            ).first()

            if existing:
                existing.score = new_sec
                updated_fss += 1
            else:
                s.add(FactorScoreSnapshot(
                    trade_date=trade_date,
                    ts_code=bare_code,
                    mode="intraday",
                    factor_name="sector",
                    score=new_sec,
                ))
                inserted_fss += 1

            if (updated_fss + inserted_fss) % 1000 == 0:
                s.flush()
        s.commit()
    print(f"  factor_score_snapshots: 更新 {updated_fss}, 新增 {inserted_fss}")

    # 8b. ScanResultIntraday: update total_score, rank, factor_scores_json
    # Build a lookup: bare_code → {new_total, new_sector}
    update_map = {}
    for bare_code in factor_scores.index:
        fs_dict = {}
        for fn in intraday_factor_names:
            if fn in factor_scores.columns:
                val = factor_scores[fn].get(bare_code, 0)
                fs_dict[fn] = round(float(val) if not pd.isna(val) else 0.0, 1)
            else:
                fs_dict[fn] = 0.0
        total = float(final_composite.get(bare_code, 0))
        update_map[bare_code] = {
            "total": round(total, 1),
            "factor_json": json.dumps(fs_dict, ensure_ascii=False),
            "sector": fs_dict.get("sector", 0),
            "tech_score": round(float(tech_series.get(bare_code, 50.0)), 1),
        }

    # Re-rank: sort by total_score desc
    rank_order = sorted(update_map.items(), key=lambda x: -x[1]["total"])
    rank_map = {bare: i + 1 for i, (bare, _) in enumerate(rank_order)}

    updated_scan = 0
    with db.get_session() as s:
        all_rows = s.query(ScanResultIntraday).filter(
            ScanResultIntraday.scan_date == trade_date
        ).all()
        for r in all_rows:
            bare = _ts_to_bare(r.ts_code)
            if bare in update_map:
                r.total_score = update_map[bare]["total"]
                r.factor_scores_json = update_map[bare]["factor_json"]
                r.tech_score = update_map[bare]["tech_score"]
                r.rank = rank_map.get(bare, r.rank)
                updated_scan += 1
            if updated_scan % 1000 == 0:
                s.flush()
        s.commit()
    print(f"  ScanResultIntraday: 更新 {updated_scan} 条, 重排")

    # ── 9. 更新本地文件 ──
    reports_dir = Path(__file__).resolve().parent.parent / "discovery_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # 9a. Top N JSON
    json_path = reports_dir / f"intraday_{trade_date}_topn.json"
    json_path.write_text(json.dumps(top_entries, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  JSON: {json_path} ({len(top_entries)} 条)")

    # 9b. Markdown 报告
    md_lines = [f"## 盘中扫描 Top {min(len(top_entries), 15)}", ""]
    hot_sectors = {"半导体", "元件", "IT服务"}

    for e in top_entries[:15]:
        code = e["stock_code"]
        name = e["stock_name"]
        score = e["score"]
        industry = e["sector"]
        fs = e["factor_scores"]
        marker = " ← 热门" if industry in hot_sectors else ""

        md_lines.append(f"### #{e['rank']} {code} {name} · {industry} — 综合评分 {score:.1f}{marker}")
        md_lines.append("")
        if e.get("reasons"):
            md_lines.append("**推荐理由：**")
            for reason in e["reasons"]:
                md_lines.append(f"- {reason}")
        else:
            md_lines.append("**推荐理由：**")
            md_lines.append(f"- 所属板块: {industry}（同花顺板块动量重算）")
        md_lines.append("")

        # 因子得分一行
        fs_parts = []
        fs_labels = {
            "ma_entry": "ma_entry（均线）", "sector": "sector（板块）",
            "momentum": "momentum（动量）", "ranking_momentum": "ranking_momentum（排名动量）",
            "rebound": "rebound（反弹）", "popularity": "popularity（人气）",
        }
        for fn in intraday_factor_names:
            label = fs_labels.get(fn, fn)
            fs_parts.append(f"{label}:{fs.get(fn, 0):.0f}")
        md_lines.append(f"*因子得分：{' | '.join(fs_parts)}*")
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")

    md_lines.append(f"*共 {len(top_entries)} 只候选（同花顺板块接口重算）*")
    md_path = reports_dir / f"intraday_{trade_date}.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    print(f"  MD:   {md_path}")

    # ── 10. 打印 Top 15 ──
    print(f"\n{'='*70}")
    print(f"同花顺板块接口重算 — 盘中 Top 15 ({trade_date})")
    print(f"{'='*70}")
    print(f"{'#':<3} {'代码':<8} {'名称':<8} {'综合':>6} {'板块分':>6} {'行业'}")
    print("-" * 70)

    for e in top_entries[:15]:
        code = e["stock_code"]
        name = e["stock_name"]
        score = e["score"]
        sec = e["factor_scores"]["sector"]
        ind = e["sector"]
        marker = " ← 热门" if ind in hot_sectors else ""
        print(f"{e['rank']:<3} {code:<8} {name:<8} {score:>5.1f}  {sec:>5.1f}  {ind}{marker}")

    # 热门板块统计
    print(f"\n─ 热门板块（半导体/元件/IT服务）进入 Top 50 ─")
    for ind_name in hot_sectors:
        in_ind = [e for e in top_entries if e["sector"] == ind_name]
        if in_ind:
            top_codes = [e["stock_code"] for e in in_ind[:3]]
            top_names = [f"{c} {names.get(c, '')}" for c in top_codes]
            print(f"  {ind_name}: Top50 内 {len(in_ind)} 只, Top: {', '.join(top_names)}")

    print(f"\n✅ 全部更新完成: DB + JSON + MD")


if __name__ == "__main__":
    main()
