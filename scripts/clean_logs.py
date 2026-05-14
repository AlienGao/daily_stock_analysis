# -*- coding: utf-8 -*-
"""运行日志清理脚本。

功能：
- 删除超过指定交易日数的 debug 日志文件
- 清理 replay 测试日志
- 可选：清理数据库 WAL 文件（需无连接）

用法：
    python scripts/clean_logs.py                    # 默认保留 7 个交易日
    python scripts/clean_logs.py --days 5           # 保留 5 个交易日
    python scripts/clean_logs.py --dry-run         # 预览不删除
    python scripts/clean_logs.py --include-wal     # 同时清理数据库 WAL
"""

import argparse
import glob
import os
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "stock_analysis.db")


def get_trading_days_ago(n: int) -> datetime:
    """获取 N 个交易日前的日期（跳过周末）。

    从今天往前数 n 个交易日（排除周六、周日），
    用于日志清理的截止日期判断。
    """
    today = datetime.now()
    count = 0
    current = today

    while count < n:
        current -= timedelta(days=1)
        # 0=周一, 6=周日
        if current.weekday() < 5:
            count += 1

    return current


def clean_debug_logs(cutoff: datetime, dry_run: bool = False) -> tuple:
    """删除 modification time 早于 cutoff 的 debug 日志。"""
    removed = []
    total_size = 0

    pattern = os.path.join(LOGS_DIR, "stock_analysis_debug_*.log*")
    for path in glob.glob(pattern):
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                size = os.path.getsize(path)
                total_size += size
                if dry_run:
                    print(f"  [DRY-RUN] 将删除: {path} ({size/1024/1024:.1f}MB, 修改于 {mtime.strftime('%Y-%m-%d')})")
                else:
                    os.remove(path)
                    print(f"  删除: {os.path.basename(path)} ({size/1024/1024:.1f}MB)")
                removed.append(path)
        except Exception as e:
            print(f"  警告: 处理失败 {path}: {e}")
    return removed, total_size


def clean_replay_logs(dry_run: bool = False) -> list:
    """清理所有 replay 测试日志。"""
    removed = []
    pattern = os.path.join(LOGS_DIR, "top_n_replay*.log*")
    for path in glob.glob(pattern):
        try:
            size = os.path.getsize(path)
            if dry_run:
                print(f"  [DRY-RUN] 将删除: {path} ({size/1024/1024:.1f}MB)")
            else:
                os.remove(path)
                print(f"  删除: {os.path.basename(path)} ({size/1024/1024:.1f}MB)")
            removed.append(path)
        except Exception as e:
            print(f"  警告: 处理失败 {path}: {e}")
    return removed


def clean_wal_files(dry_run: bool = False) -> tuple:
    """清理数据库 WAL 和 SHM 文件（需无连接）。"""
    import sqlite3
    removed = []
    total_size = 0

    wal_path = DB_PATH + "-wal"
    shm_path = DB_PATH + "-shm"
    for path in [wal_path, shm_path]:
        if os.path.exists(path):
            size = os.path.getsize(path)
            if size > 0:
                try:
                    if path == wal_path:
                        conn = sqlite3.connect(DB_PATH, timeout=5)
                        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                        conn.close()
                        print(f"  WAL checkpoint 完成")
                    if dry_run:
                        print(f"  [DRY-RUN] 将删除: {path} ({size/1024/1024:.1f}MB)")
                    else:
                        os.remove(path)
                        print(f"  删除: {os.path.basename(path)} ({size/1024/1024:.1f}MB)")
                    removed.append(path)
                    total_size += size
                except Exception as e:
                    print(f"  警告: 处理失败 {path}: {e}")
    return removed, total_size


def main():
    parser = argparse.ArgumentParser(description="清理运行日志，保留最近 N 个交易日")
    parser.add_argument("--days", type=int, default=7, help="保留交易日数（默认 7）")
    parser.add_argument("--dry-run", action="store_true", help="预览模式，不实际删除")
    parser.add_argument("--include-wal", action="store_true", help="同时清理数据库 WAL 文件")
    args = parser.parse_args()

    cutoff = get_trading_days_ago(args.days)

    print("=" * 50)
    print("运行日志清理")
    print("=" * 50)
    print(f"保留最近 {args.days} 个交易日（截止: {cutoff.strftime('%Y-%m-%d')}）")
    print(f"日志目录: {LOGS_DIR}")
    if args.dry_run:
        print("DRY-RUN 模式：仅预览不删除")
    print()

    print("清理 debug 日志（按交易日）...")
    debug_removed, debug_size = clean_debug_logs(cutoff, args.dry_run)

    print()
    print("清理 replay 日志...")
    replay_removed = clean_replay_logs(args.dry_run)

    wal_size = 0
    if args.include_wal:
        print()
        print("清理数据库 WAL 文件...")
        wal_removed, wal_size = clean_wal_files(args.dry_run)

    total_removed = len(debug_removed) + len(replay_removed)
    total_size = (debug_size + wal_size) / 1024 / 1024

    print()
    print("=" * 50)
    print(f"删除文件数: {total_removed}")
    print(f"释放空间: {total_size:.1f}MB")
    print(f"保留: {args.days} 个交易日（截止 {cutoff.strftime('%Y-%m-%d')}）")


if __name__ == "__main__":
    main()
