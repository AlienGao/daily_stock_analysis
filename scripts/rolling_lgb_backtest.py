#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""滚动窗口 LightGBM 回测。

训练窗口从 2024-01-01 开始每月向后滑动，用当月模型预测后续每日 Top 5。
支持 fixed（固定持有期）和 peak_speed（峰值速度）两种标签模式。
每日预测结果保存为独立 MD + JSON 报告到 lgb_reports/。
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from sqlalchemy import func as _func

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.discovery.ml.lgb_trainer import LGBTrainer
from src.storage import DatabaseManager, StockDaily

REPORTS_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lgb_reports"
)
FACTOR_SUBSET_DIR = os.path.join(REPORTS_ROOT, "factor_subset")
# EXEC_SUBDIR is set in main() after EXEC_MODE is known

TRAIN_START = "20240101"
PRED_START = "20250101"
FORWARD_DAYS_LIST = [3, 5, 10, 20]
LABEL_MODES = ["peak_speed", "fixed"]
WINDOW_DAYS = 20  # peak_speed 窗口天数
# PRED_END is computed dynamically from StockDaily in main()
MODE = "postmarket"
EXEC_MODE_LIST = ["open"]  # "open" = open→open labels
TOP_N = 5


# ── 因子子集配置 ──
# 从 factor_subset/ 目录自动读取最优因子配置
# Key: (exec_mode, label_mode, forward_days_or_window)
# Value: {"excluded": [...], "final_subset": [...]}

def _parse_subset_filename(filename: str) -> tuple | None:
    """Parse subset report filename.

    Examples:
      'subset_open_fixed_5d_20260526_193548.json' -> ('open', 'fixed', '5d')
      'subset_open_peak20d_20260526_220650.json'  -> ('open', 'peak_speed', '20d')
    """
    if not filename.startswith("subset_") or not filename.endswith(".json"):
        return None
    inner = filename[7:-5]  # strip 'subset_' and '.json'
    parts = inner.split("_")  # e.g. ['open', 'fixed', '5d', '20260526', '193548']
    if len(parts) < 3:
        return None
    exec_mode = parts[0]
    label_part = parts[1]
    if label_part.startswith("peak"):
        # 'peak20d' -> ('peak_speed', '20d')
        label_mode = "peak_speed"
        period = label_part[4:]  # strip 'peak' -> '20d'
    else:
        label_mode = label_part  # 'fixed'
        period = parts[2]        # '5d', '10d', '20d'
    return (exec_mode, label_mode, period)


def load_factor_subsets() -> dict:
    """Scan factor_subset/ directory and return latest config for each type.

    Returns dict keyed by (exec_mode, label_mode, period) where period is like '5d' or '20d'.
    Each value contains 'excluded_factors' and 'final_subset'.
    """
    if not os.path.isdir(FACTOR_SUBSET_DIR):
        print(f"[WARN] factor_subset 目录不存在: {FACTOR_SUBSET_DIR}")
        return {}

    # Group files by (exec_mode, label_mode, period)
    groups = {}
    for fn in os.listdir(FACTOR_SUBSET_DIR):
        parsed = _parse_subset_filename(fn)
        if not parsed:
            continue
        key = parsed
        if key not in groups:
            groups[key] = []
        groups[key].append(fn)

    configs = {}
    for key, files in groups.items():
        # Use the latest file (sorted by filename, timestamp is embedded)
        latest = sorted(files)[-1]
        path = os.path.join(FACTOR_SUBSET_DIR, latest)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            configs[key] = {
                "excluded_factors": data.get("excluded_factors", []),
                "final_subset": data.get("final_subset", []),
                "report": latest,
            }
            exec_mode, label_mode, period = key
            print(f"  [因子配置] {exec_mode} / {label_mode} / {period}: "
                  f"使用 {len(data.get('final_subset', []))} 个因子 "
                  f"(排除 {len(data.get('excluded_factors', []))} 个)")
        except Exception as e:
            print(f"  [WARN] 读取 {latest} 失败: {e}")

    return configs


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
    label_mode = getattr(trainer, "label_mode", "fixed")
    if label_mode == "peak_speed":
        label_dir = f"peak{trainer.window_days}d"
        label_tag = f"peak{trainer.window_days}d"
    else:
        label_dir = f"fwd{trainer.forward_days}d"
        label_tag = f"fwd{trainer.forward_days}d"
    base = f"{trainer.mode}_{label_tag}_{sd}_{ed}_pred_{pred_date}_{exec_suffix}"
    report_dir = os.path.join(REPORTS_ROOT, exec_suffix, label_dir)
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
    if label_mode == "peak_speed":
        title = f"LGB 预测 · {trainer.mode} · 峰值速度 {trainer.window_days} 日窗口"
    else:
        title = f"LGB 预测 · {trainer.mode} · 前向 {trainer.forward_days} 日"
    md_parts = [
        f"# {title}",
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
    has_predicted_days = any("predicted_days" in p for p in predictions)
    md_parts.extend([
        "",
        "## Top 5 预测",
        "",
    ])
    if has_predicted_days:
        md_parts.append(
            "| 排名 | 代码 | 名称 | LGB 评分 | 原始得分 | 预计天数 |"
        )
        md_parts.append(
            "|------|------|------|----------|----------|----------|"
        )
    else:
        md_parts.append(
            "| 排名 | 代码 | 名称 | LGB 评分 | 原始得分 |"
        )
        md_parts.append(
            "|------|------|------|----------|----------|"
        )
    for p in predictions:
        if has_predicted_days:
            md_parts.append(
                f"| {p['rank']} | {p['stock_code']} | {p['stock_name']} "
                f"| {p['lgb_score']:.2f} | {p['raw_score']:.4f} | {p.get('predicted_days', '-')} |"
            )
        else:
            md_parts.append(
                f"| {p['rank']} | {p['stock_code']} | {p['stock_name']} "
                f"| {p['lgb_score']:.2f} | {p['raw_score']:.4f} |"
            )
    md_parts.append("")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_parts))

    report = {
        "mode": trainer.mode,
        "label_mode": label_mode,
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
    if label_mode == "peak_speed":
        report["window_days"] = trainer.window_days
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
               pred_s: str, pred_e: str, final_subset: list | None = None) -> dict:
    """Train on [train_s, train_e], predict every trading day in [pred_s, pred_e]."""
    trading_days = get_trading_days(pred_s, pred_e)
    if not trading_days:
        return {"status": "skip", "reason": "no trading days"}

    print(f"  训练集交易日: {len(get_trading_days(train_s, train_e))}")
    print(f"  预测期交易日: {len(trading_days)} ({pred_s} ~ {pred_e})")

    trainer.prepare_data(start_date=train_s, end_date=train_e)

    # LGB_ENABLED_FACTORS 白名单（最高优先级）
    enabled = os.environ.get("LGB_ENABLED_FACTORS", "").strip()
    if enabled:
        enabled_set = {f.strip() for f in enabled.split(",") if f.strip()}
        available = set(trainer.feature_names)
        to_use = [f for f in enabled_set if f in available]
        skipped = [f for f in enabled_set if f not in available]
        if to_use:
            trainer.feature_names = to_use
            print(f"  [LGB_ENABLED_FACTORS] 使用 {len(to_use)} 个因子: {sorted(to_use)}" +
                  (f" (跳过: {skipped})" if skipped else ""))
        else:
            print(f"  [LGB_ENABLED_FACTORS] 白名单中无可用因子 (设置: {sorted(enabled_set)}，可用前10: {sorted(available)[:10]}...)")

    # 如果指定了因子子集，只使用 final_subset 中的因子
    elif final_subset:
        available = set(trainer.feature_names)
        to_use = [f for f in final_subset if f in available]
        skipped = [f for f in final_subset if f not in available]
        if skipped:
            print(f"  [因子子集] 跳过不可用因子: {skipped}")
        if to_use:
            trainer.feature_names = to_use
            print(f"  [因子子集] 使用 {len(to_use)} 个因子训练")
        else:
            print(f"  [WARN] final_subset 中无可用因子，使用全部 {len(trainer.feature_names)} 个因子")

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

    # ── 加载因子子集配置 ──
    print("=" * 64)
    print("加载因子子集配置...")
    factor_configs = load_factor_subsets()
    if not factor_configs:
        print("[WARN] 未找到因子子集配置，将使用全部因子")
    print("=" * 64)

    print("=" * 64)
    print(f"滚动窗口 LGB 回测")
    print(f"模式: {MODE} | exec: {EXEC_MODE_LIST} | Top {TOP_N}")
    print(f"训练起点: {TRAIN_START} (逐月右移 12 个月窗口)")
    print(f"预测范围: {PRED_START} ~ {pred_end}")
    print(f"窗口数: {len(windows)}")
    print(f"报告目录: {REPORTS_ROOT}/open2open/{{peak20d,fwd3d,fwd5d,fwd10d,fwd20d}}")
    print("=" * 64)

    grand_total_ok = 0
    grand_total_fail = 0

    # ── 从 factor_configs 提取所有可用的配置 ──
    # 按 (exec_mode, label_mode, period) 组织
    def _exec_label(k): return k[0]  # exec_mode
    def _exec_suffix(k): return "open2open" if k == "open" else "close2close"

    all_exec_modes = sorted({_exec_label(k) for k in factor_configs})
    if not all_exec_modes:
        all_exec_modes = EXEC_MODE_LIST  # fallback

    # ── peak_speed ──
    for exec_mode in all_exec_modes:
        suffix = _exec_suffix(exec_mode)
        # 提取该 exec_mode 下所有可用的 peak_speed 窗口天数
        available_peaks = sorted({
            int(key[2].rstrip("d"))
            for key in factor_configs
            if key[0] == exec_mode and key[1] == "peak_speed"
        })

        for window_days in available_peaks:
            config_key = (exec_mode, "peak_speed", f"{window_days}d")
            subset_cfg = factor_configs[config_key]
            final_subset = subset_cfg["final_subset"]
            print(f"\n{'#'*60}")
            print(f"# EXEC = {exec_mode} ({suffix}) | peak_speed {window_days}d")
            print(f"# 因子子集: {subset_cfg['report']} ({len(final_subset)} 因子)")
            print(f"{'#'*60}")

            exec_ok, exec_fail = 0, 0
            for wi, (train_s, train_e, pred_s, pred_e) in enumerate(windows):
                print(f"\n--- Window {wi + 1}/{len(windows)} "
                      f"train={train_s}~{train_e}  pred={pred_s}~{pred_e} ---")
                trainer = LGBTrainer(
                    mode=MODE, exec_mode=exec_mode,
                    label_mode="peak_speed", window_days=window_days,
                )
                result = run_window(trainer, train_s, train_e, pred_s, pred_e, final_subset)
                if result["status"] == "skip":
                    print(f"  跳过: {result['reason']}")
                    continue
                exec_ok += result["ok"]
                exec_fail += result["fail"]
                print(f"  成功: {result['ok']}/{result['total']}"
                      + (f"  失败: {result['fail']}" if result["fail"] else ""))
                trainer.save()
                cleanup_old_models()
            print(f"\n  {suffix} peak{window_days}d 汇总: 成功 {exec_ok}, 失败 {exec_fail}")
            grand_total_ok += exec_ok
            grand_total_fail += exec_fail

    # ── fixed ──
    for exec_mode in all_exec_modes:
        suffix = _exec_suffix(exec_mode)
        available_fwds = sorted({
            int(key[2].rstrip("d"))
            for key in factor_configs
            if key[0] == exec_mode and key[1] == "fixed"
        })
        if not available_fwds:
            continue

        print(f"\n{'#'*60}")
        print(f"# EXEC = {exec_mode} ({suffix}) | fixed")
        print(f"# 可用持有期: {available_fwds}")
        print(f"{'#'*60}")

        for fwd in available_fwds:
            config_key = (exec_mode, "fixed", f"{fwd}d")
            subset_cfg = factor_configs[config_key]
            final_subset = subset_cfg["final_subset"]
            print(f"\n{'='*50}")
            print(f"  Forward = {fwd}d | 因子: {len(final_subset)} 个")
            print(f"  报告: {subset_cfg['report']}")
            print(f"{'='*50}")

            fwd_ok, fwd_fail = 0, 0
            for wi, (train_s, train_e, pred_s, pred_e) in enumerate(windows):
                print(f"\n--- Window {wi + 1}/{len(windows)} "
                      f"train={train_s}~{train_e}  pred={pred_s}~{pred_e} ---")
                trainer = LGBTrainer(
                    mode=MODE, forward_days=fwd, exec_mode=exec_mode,
                    label_mode="fixed",
                )
                result = run_window(trainer, train_s, train_e, pred_s, pred_e, final_subset)
                if result["status"] == "skip":
                    print(f"  跳过: {result['reason']}")
                    continue
                fwd_ok += result["ok"]
                fwd_fail += result["fail"]
                print(f"  成功: {result['ok']}/{result['total']}"
                      + (f"  失败: {result['fail']}" if result["fail"] else ""))
                trainer.save()
                cleanup_old_models()
            print(f"\n  {suffix} fwd{fwd}d 汇总: 成功 {fwd_ok}, 失败 {fwd_fail}")
            grand_total_ok += fwd_ok
            grand_total_fail += fwd_fail

    print(f"\n{'='*60}")
    print(f"全部完成: 成功 {grand_total_ok}, 失败 {grand_total_fail}")
    print(f"{'='*60}")

    # NOTE: cleanup is called after each window save, no need for final cleanup here


def cleanup_old_models():
    """Remove old model files, keeping only the latest per (label_mode, exec_mode, forward/peak)."""
    models_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "data", "lgb_models",
    )
    if not os.path.isdir(models_dir):
        return

    models = [f for f in os.listdir(models_dir) if f.endswith(".joblib") and f.startswith("lgb_")]
    if not models:
        return

    import re as _re
    keep: set = set()

    # Fixed models: keep latest per (fwd, exec_mode)
    for fwd in FORWARD_DAYS_LIST:
        _fwd_pat = _re.compile(rf"_fwd{fwd}d_")
        for suffix in ["open2open", "close2close"]:
            group = sorted(
                [m for m in models if _fwd_pat.search(m) and suffix in m],
                reverse=True,
            )
            if group:
                keep.add(group[0])

    # Peak models: keep latest per (peak window, exec_mode)
    _peak_pat = _re.compile(rf"_peak{WINDOW_DAYS}d_")
    for suffix in ["open2open", "close2close"]:
        group = sorted(
            [m for m in models if _peak_pat.search(m) and suffix in m
             and not m.endswith("_days.joblib")],
            reverse=True,
        )
        if group:
            kept = group[0]
            keep.add(kept)
            # Also keep the companion _days.joblib if it exists
            days_companion = kept.replace(".joblib", "_days.joblib")
            if days_companion in models:
                keep.add(days_companion)

    to_delete: list = []
    for fwd in FORWARD_DAYS_LIST:
        _fwd_pat = _re.compile(rf"_fwd{fwd}d_")
        for m in models:
            if m not in keep and _fwd_pat.search(m):
                to_delete.append(m)
    for m in models:
        if m not in keep and _peak_pat.search(m):
            to_delete.append(m)

    deleted = 0
    for m in set(to_delete):
        os.remove(os.path.join(models_dir, m))
        deleted += 1

    if deleted:
        print(f"  删除历史模型: {deleted}  保留: {keep}")


def cleanup_old_reports():
    """Remove old report files, keeping only the latest per (fwd/peak, exec_mode)."""

    def _clean_dir(report_dir: str, label_prefix="fwd"):
        """Clean one report directory, keeping only the latest training window."""
        if not os.path.isdir(report_dir):
            return 0
        all_files = sorted(os.listdir(report_dir))
        if not all_files:
            return 0

        windows = set()
        for fn in all_files:
            parts = fn.replace(".json", "").replace(".md", "").split("_")
            try:
                idx = next(i for i, p in enumerate(parts) if p.startswith(label_prefix))
            except StopIteration:
                continue
            if idx + 2 < len(parts):
                windows.add(f"{parts[idx + 1]}_{parts[idx + 2]}")

        if not windows:
            return 0

        latest_window = sorted(windows)[-1]
        train_s, train_e = latest_window.split("_")
        keep_marker = f"_{train_s}_{train_e}_"

        deleted = 0
        for fn in all_files:
            if keep_marker not in fn:
                try:
                    os.remove(os.path.join(report_dir, fn))
                    deleted += 1
                except OSError:
                    pass
        return deleted

    total_deleted = 0
    for exec_suffix in ["open2open", "close2close"]:
        for fwd in FORWARD_DAYS_LIST:
            total_deleted += _clean_dir(
                os.path.join(REPORTS_ROOT, exec_suffix, f"fwd{fwd}d"), "fwd"
            )
        if "peak_speed" in LABEL_MODES:
            total_deleted += _clean_dir(
                os.path.join(REPORTS_ROOT, exec_suffix, f"peak{WINDOW_DAYS}d"), "peak"
            )

    if total_deleted:
        print(f"  删除历史报告: {total_deleted}")


if __name__ == "__main__":
    main()
