"""Clean test data from production analysis_history and related tables.

Usage: python scripts/clean_test_data.py [--dry-run]
"""
import sys
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "stock_analysis.db"

TEST_QUERY_IDS = [
    'query_001', 'query_002', 'query_003', 'query_004', 'query_005', 'query_006', 'query_007',
    'query_change_pct_zero', 'query_change_pct_fallback', 'query_change_pct_non_dict_raw',
    'query_fundamental_fallback_001', 'query_fundamental_fallback_002', 'query_fundamental_failed_boards_001',
    'query_delete_api_001', 'query_delete_api_002',
    'query_non_cn_board_001',
    'query_english_markdown_001', 'query_english_markdown_bias_001', 'query_english_detail_001',
    'batch_q', 'q_cli', 'q_null', 'q1',
]


def main():
    dry_run = '--dry-run' in sys.argv

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON")

    placeholders = ','.join(['?'] * len(TEST_QUERY_IDS))

    # Count before
    ah_count = conn.execute(
        f"SELECT COUNT(*) FROM analysis_history WHERE query_id IN ({placeholders})",
        TEST_QUERY_IDS,
    ).fetchone()[0]
    fs_count = conn.execute(
        f"SELECT COUNT(*) FROM fundamental_snapshot WHERE query_id IN ({placeholders})",
        TEST_QUERY_IDS,
    ).fetchone()[0]
    br_count = conn.execute(
        f"""SELECT COUNT(*) FROM backtest_results
            WHERE analysis_history_id IN (
                SELECT id FROM analysis_history WHERE query_id IN ({placeholders})
            )""",
        TEST_QUERY_IDS,
    ).fetchone()[0]

    print(f"Test records found:")
    print(f"  analysis_history:      {ah_count}")
    print(f"  fundamental_snapshot:   {fs_count}")
    print(f"  backtest_results:       {br_count}")

    if dry_run:
        print("\n[Dry run] No changes made.")
        conn.close()
        return

    if ah_count == 0 and fs_count == 0 and br_count == 0:
        print("\nNothing to clean.")
        conn.close()
        return

    # Delete in order: backtest_results first (FK), then snapshots, then analysis_history
    br_deleted = conn.execute(
        f"""DELETE FROM backtest_results
            WHERE analysis_history_id IN (
                SELECT id FROM analysis_history WHERE query_id IN ({placeholders})
            )""",
        TEST_QUERY_IDS,
    ).rowcount
    fs_deleted = conn.execute(
        f"DELETE FROM fundamental_snapshot WHERE query_id IN ({placeholders})",
        TEST_QUERY_IDS,
    ).rowcount
    ah_deleted = conn.execute(
        f"DELETE FROM analysis_history WHERE query_id IN ({placeholders})",
        TEST_QUERY_IDS,
    ).rowcount

    conn.commit()

    print(f"\nDeleted:")
    print(f"  backtest_results:       {br_deleted}")
    print(f"  fundamental_snapshot:   {fs_deleted}")
    print(f"  analysis_history:       {ah_deleted}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
