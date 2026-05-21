#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""滚动窗口 LightGBM 回测。

训练窗口从 2025-01-01 开始每月向后滑动，用当月模型预测后续每日 Top 5。
分别使用 forward_days=1 和 forward_days=3 各跑一轮。
每日预测结果保存为独立 MD + JSON 报告到 lgb_reports/。
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta
from sqlalchemy import distinct as _distinct, func as _func

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.discovery.ml.lgb_trainer import LGBTrainer
from src.storage import DatabaseManager, StockDaily

REPORTS_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lgb_reports"
)
# EXEC_SUBDIR is set in main() after EXEC_MODE is known

TRAIN_START = "20240101"
PRED_START = "20250101"
FORWARD_DAYS_LIST = [20]
# PRED_END is computed dynamically from StockDaily in main()
MODE = "postmarket"
EXEC_MODE_LIST = ["open", "close"]  # "open" = open→open labels, "close" = close→close labels
TOP_N = 5


def _ymd(d) -> str:
    """Normalize any date-ish value to YYYYMMDD string."""
    if hasattr(d, "strftime"):
        return d.strftime("%Y%m%d")
    return str(d).replace("-", "")[:8]


def _to_iso(d: str) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD for StockDaily date filtering."""
    d = d.replace("-", "")
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def get_trading_days(start: str, end: str, min_stocks: int = 100) -> list:
    """Return sorted real trading days in [start, end] from StockDaily.

    StockDaily.date is stored as ISO format (YYYY-MM-DD) in SQLite,
    so input YYYYMMDD strings are converted before filtering.

    Uses HAVING count(code) >= min_stocks to exclude sparse/fake dates
    (weekends, holidays with only index data, etc.).
    """
    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        rows = (
            session.query(StockDaily.date)
            .filter(
                StockDaily.date >= _to_iso(start),
                StockDaily.date <= _to_iso(end),
            )
            .group_by(StockDaily.date)
            .having(_func.count(StockDaily.code) >= min_stocks)
            .order_by(StockDaily.date)
            .all()
        )
    return [r[0] for r in rows]


def save_daily_report(trainer: LGBTrainer, pred_date: str) -> str:
    """Save a single-day prediction report (MD + JSON), including model diagnostics."""
    predictions = trainer.get_latest_predictions(top_n=TOP_N)
    sd = _ymd(getattr(trainer, "_train_start", ""))
    ed = _ymd(getattr(trainer, "_train_end", ""))

    exec_suffix = "open2open" if getattr(trainer, "exec_mode", "close") == "open" else "close2close"
    fwd_dir = f"fwd{trainer.forward_days}d"
    base = f"{trainer.mode}_fwd{trainer.forward_days}d_{sd}_{ed}_pred_{pred_date}_{exec_suffix}"
    report_dir = os.path.join(REPORTS_ROOT, exec_suffix, fwd_dir)
    os.makedirs(report_dir, exist_ok=True)
    md_path = os.path.join(report_dir, f"{base}.md")
    json_path = os.path.join(report_dir, f"{base}.json")

    tree_diag = None
    pred_stats = None
    try:
        tree_diag = trainer.get_tree_diagnostics()
    except Exception:
        pass
    try:
        pred_stats = trainer.get_prediction_stats()
    except Exception:
        pass
    metrics = trainer._training_metrics

    # ── Markdown ──
    md_parts = [
        f"# LGB 预测 · {trainer.mode} · 前向 {trainer.forward_days} 日",
        "",
        f"**训练范围**: {sd} ~ {ed}",
        f"**预测日期**: {pred_date}",
        f"**生成时间**: {datetime.now().strftime('%Y%m%d %H:%M:%S')}",
    ]
    if isinstance(metrics.get("cv_rmse_mean"), float):
        md_parts.append(f"**CV RMSE**: {metrics['cv_rmse_mean']:.4f}")
    if tree_diag:
        md_parts.append(f"**树数**: {tree_diag['n_trees']} | "
                        f"**平均深度**: {tree_diag['avg_depth']:.1f}")
    md_parts.extend([
        "",
        "## Top 5 预测",
        "",
        "| 排名 | 代码 | 名称 | LGB 评分 | 原始得分 |",
        "|------|------|------|----------|----------|",
    ])
    for p in predictions:
        md_parts.append(
            f"| {p['rank']} | {p['stock_code']} | {p['stock_name']} "
            f"| {p['lgb_score']:.2f} | {p['raw_score']:.4f} |"
        )
    md_parts.append("")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_parts))

    report = {
        "mode": trainer.mode,
        "forward_days": trainer.forward_days,
        "train_start": sd,
        "train_end": ed,
        "pred_date": pred_date,
        "predictions": predictions,
        "tree_diagnostics": tree_diag,
        "prediction_stats": pred_stats,
        "training_metrics": {
            k: v for k, v in metrics.items()
            if k in ("cv_rmse_mean", "cv_rmse_std", "n_samples", "n_features",
                     "cv_scores", "rank_ic_mean", "rank_ic_std", "icir", "oof_corr")
        },
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return md_path


def generate_monthly_windows(pred_end: str):
    """Yield (train_start, train_end, pred_start, pred_end) YYYYMMDD tuples.

    Training window ends the day before prediction starts, and slides
    forward monthly along with the prediction window.
    """
    train_s = datetime.strptime(TRAIN_START, "%Y%m%d")
    pred_s = datetime.strptime(PRED_START, "%Y%m%d")
    final_pred_e = datetime.strptime(pred_end, "%Y%m%d")

    windows = []
    while pred_s < final_pred_e:
        pred_e = pred_s + relativedelta(months=1) - timedelta(days=1)
        if pred_e > final_pred_e:
            pred_e = final_pred_e

        train_e = pred_s - timedelta(days=1)

        windows.append((
            train_s.strftime("%Y%m%d"),
            train_e.strftime("%Y%m%d"),
            pred_s.strftime("%Y%m%d"),
            pred_e.strftime("%Y%m%d"),
        ))

        train_s += relativedelta(months=1)
        pred_s = pred_e + timedelta(days=1)

    return windows


def run_window(trainer: LGBTrainer, train_s: str, train_e: str,
               pred_s: str, pred_e: str) -> dict:
    """Train on [train_s, train_e], predict every trading day in [pred_s, pred_e]."""
    trading_days = get_trading_days(pred_s, pred_e)
    if not trading_days:
        return {"status": "skip", "reason": "no trading days"}

    print(f"  训练集交易日: {len(get_trading_days(train_s, train_e))}")
    print(f"  预测期交易日: {len(trading_days)} ({pred_s} ~ {pred_e})")

    trainer.prepare_data(start_date=train_s, end_date=train_e)
    trainer.train()


    ok = 0
    fail = 0
    for td in trading_days:
        td_str = _ymd(td)
        try:
            trainer.predict(target_date=td_str)
            save_daily_report(trainer, td_str)
            ok += 1
        except Exception:
            fail += 1
            if fail <= 3:
                print(f"  FAIL {td_str}: {traceback.format_exc(limit=1).strip().split(chr(10))[-1]}")

    return {"status": "done", "ok": ok, "fail": fail, "total": len(trading_days)}


def main():
    os.makedirs(REPORTS_ROOT, exist_ok=True)

    # Compute PRED_END from StockDaily (latest trading day with >= 3000 stocks)
    db = DatabaseManager.get_instance()
    with db.get_session() as session:
        dates_raw = (
            session.query(StockDaily.date)
            .group_by(StockDaily.date)
            .having(_func.count(StockDaily.code) >= 3000)
            .order_by(StockDaily.date.desc())
            .first()
        )
    if not dates_raw:
        print("错误: StockDaily 中没有足够的交易数据")
        return
    pred_end = dates_raw[0].strftime("%Y%m%d") if hasattr(dates_raw[0], "strftime") else str(dates_raw[0]).replace("-", "")[:8]

    windows = generate_monthly_windows(pred_end)

    print("=" * 64)
    print(f"滚动窗口 LGB 回测")
    print(f"模式: {MODE} | exec: {EXEC_MODE_LIST} | Top {TOP_N}")
    print(f"训练起点: {TRAIN_START} (逐月右移 12 个月窗口)")
    print(f"预测范围: {PRED_START} ~ {pred_end}")
    print(f"窗口数: {len(windows)} | Forward: {FORWARD_DAYS_LIST}")
    print(f"报告目录: {REPORTS_ROOT}/{{open2open,close2close}}/{{fwd3d,fwd5d,fwd10d}}")
    print("=" * 64)

    grand_total_ok = 0
    grand_total_fail = 0

    for exec_mode in EXEC_MODE_LIST:
        exec_suffix = "open2open" if exec_mode == "open" else "close2close"
        print(f"\n{'#'*60}")
        print(f"# EXEC = {exec_mode} ({exec_suffix})")
        print(f"{'#'*60}")

        for fwd in FORWARD_DAYS_LIST:
            print(f"\n{'='*50}")
            print(f"  Forward = {fwd} 日")
            print(f"{'='*50}")

            fwd_ok = 0
            fwd_fail = 0

            for wi, (train_s, train_e, pred_s, pred_e) in enumerate(windows):
                print(f"\n--- Window {wi + 1}/{len(windows)} "
                      f"train={train_s}~{train_e}  pred={pred_s}~{pred_e} ---")

                trainer = LGBTrainer(mode=MODE, forward_days=fwd, exec_mode=exec_mode)
                result = run_window(trainer, train_s, train_e, pred_s, pred_e)

                if result["status"] == "skip":
                    print(f"  跳过: {result['reason']}")
                    continue

                fwd_ok += result["ok"]
                fwd_fail += result["fail"]
                print(f"  成功: {result['ok']}/{result['total']}"
                      + (f"  失败: {result['fail']}" if result["fail"] else ""))

                trainer.save()

            print(f"\n  EXEC={exec_suffix} Forward={fwd}d 汇总: 成功 {fwd_ok}, 失败 {fwd_fail}")
            grand_total_ok += fwd_ok
            grand_total_fail += fwd_fail

    print(f"\n{'='*60}")
    print(f"全部完成: 成功 {grand_total_ok}, 失败 {grand_total_fail}")
    print(f"{'='*60}")

    # NOTE: cleanup removed — historical predictions needed for backtest-sim
    cleanup_old_models()
    print(f"\n清理完成（旧模型）")


def cleanup_old_models():
    """Remove old model files, keeping only the latest open and latest close model."""
    models_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "data", "lgb_models",
    )
    if not os.path.isdir(models_dir):
        return

    models = [f for f in os.listdir(models_dir) if f.endswith(".joblib") and f.startswith("lgb_")]
    if not models:
        return

    # Only clean models for forward_days in FORWARD_DAYS_LIST.
    # Models for other forward_days (e.g. fwd3, fwd5) are left untouched.
    import re as _re
    keep: set = set()
    for fwd in FORWARD_DAYS_LIST:
        _fwd_pat = _re.compile(rf"_fwd{fwd}d_")
        for suffix in ["open2open", "close2close"]:
            group = sorted(
                [m for m in models if _fwd_pat.search(m) and suffix in m],
                reverse=True,
            )
            if group:
                keep.add(group[0])  # latest per (fwd, exec_mode)

    to_delete: list = []
    for fwd in FORWARD_DAYS_LIST:
        _fwd_pat = _re.compile(rf"_fwd{fwd}d_")
        for m in models:
            if m not in keep and _fwd_pat.search(m):
                to_delete.append(m)

    deleted = 0
    for m in set(to_delete):
        os.remove(os.path.join(models_dir, m))
        deleted += 1

    if deleted:
        print(f"  删除历史模型: {deleted}  保留: {keep}")


def cleanup_old_reports():
    """Remove old report files, keeping only the latest per (fwd, exec_mode).

    Both training reports ({base}_{exec_suffix}.json/.md) and daily prediction
    reports ({base}_pred_{date}_{exec_suffix}.json/.md) are cleaned.
    """
    import glob as _g

    deleted = 0
    for fwd in FORWARD_DAYS_LIST:
        for exec_suffix in ["open2open", "close2close"]:
            report_dir = os.path.join(REPORTS_ROOT, exec_suffix, f"fwd{fwd}d")
            if not os.path.isdir(report_dir):
                continue

            all_files = sorted(os.listdir(report_dir))
            if not all_files:
                continue

            # Extract unique training windows (train_start-train_end pairs)
            windows = set()
            for fn in all_files:
                # Pattern: {mode}_fwd{N}d_{train_s}_{train_e}_pred_{pred_date}_{exec}.{ext}
                #                or {mode}_fwd{N}d_{train_s}_{train_e}_{exec}.{ext}
                parts = fn.replace(".json", "").replace(".md", "").split("_")
                # Find the date range: after fwd part, before exec suffix or _pred_
                try:
                    fwd_idx = next(i for i, p in enumerate(parts) if p.startswith("fwd"))
                except StopIteration:
                    continue
                # train_s is at fwd_idx+1, train_e at fwd_idx+2
                if fwd_idx + 2 < len(parts):
                    train_key = f"{parts[fwd_idx + 1]}_{parts[fwd_idx + 2]}"
                    windows.add(train_key)

            if not windows:
                continue

            # Keep reports from the latest training window only
            latest_window = sorted(windows)[-1]
            train_s, train_e = latest_window.split("_")
            keep_marker = f"_{train_s}_{train_e}_"

            for fn in all_files:
                if keep_marker not in fn:
                    fp = os.path.join(report_dir, fn)
                    try:
                        os.remove(fp)
                        deleted += 1
                    except OSError:
                        pass

    if deleted:
        print(f"  删除历史报告: {deleted}")


if __name__ == "__main__":
    main()
