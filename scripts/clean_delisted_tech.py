"""Clean delisted stock data from stock_tech_indicator.

Removes tech indicator rows for codes that no longer appear in stock_daily.

Usage: python scripts/clean_delisted_tech.py
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "stock_analysis.db"


def main():
    conn = sqlite3.connect(str(DB_PATH))

    before_rows = conn.execute("SELECT COUNT(*) FROM stock_tech_indicator").fetchone()[0]
    before_codes = conn.execute("SELECT COUNT(DISTINCT code) FROM stock_tech_indicator").fetchone()[0]

    deleted = conn.execute(
        "DELETE FROM stock_tech_indicator WHERE code NOT IN (SELECT DISTINCT code FROM stock_daily)"
    ).rowcount

    conn.commit()

    after_rows = conn.execute("SELECT COUNT(*) FROM stock_tech_indicator").fetchone()[0]
    after_codes = conn.execute("SELECT COUNT(DISTINCT code) FROM stock_tech_indicator").fetchone()[0]

    print(f"Before: {before_rows} rows, {before_codes} codes")
    print(f"Deleted: {deleted} rows, {before_codes - after_codes} codes")
    print(f"After: {after_rows} rows, {after_codes} codes")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
