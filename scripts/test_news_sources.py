#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试三个消息面数据源的格式与可用性。

1. 同花顺热点 → stock_board_concept_spot_em / stock_board_concept_name_ths
2. 东财研报   → stock_research_report_em (单只股票)
3. 巨潮公告   → stock_notice_report(symbol='全部') / stock_zh_a_disclosure_report_cninfo
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timedelta

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 50)


def test_1_hot_concept():
    """同花顺概念板块热点（东财实时 + THS 列表）。"""
    print("=" * 80)
    print("1. 同花顺热点")
    print("=" * 80)
    import akshare as ak

    # 1a: 概念板块实时行情
    try:
        df = ak.stock_board_concept_spot_em()
        if df is not None and not df.empty:
            print(f"  [概念板块实时] {len(df)} 条")
            print(f"  列名: {list(df.columns)}")
            print(df.head(5).to_string())
            print(f"\n  dtypes:\n{df.dtypes}")
    except Exception as e:
        print(f"  [概念实时 失败] {type(e).__name__}: {str(e)[:150]}")

    # 1b: THS 概念名称列表
    try:
        df = ak.stock_board_concept_name_ths()
        if df is not None and not df.empty:
            print(f"\n  [THS概念名称] {len(df)} 条")
            print(f"  列名: {list(df.columns)}")
            print(df.head(5).to_string())
    except Exception as e:
        print(f"  [THS概念 失败] {type(e).__name__}: {str(e)[:150]}")

    # 1c: THS 行业名称
    try:
        df = ak.stock_board_industry_name_ths()
        if df is not None and not df.empty:
            print(f"\n  [THS行业名称] {len(df)} 条")
            print(f"  列名: {list(df.columns)}")
            print(df.head(5).to_string())
    except Exception as e:
        print(f"  [THS行业 失败] {type(e).__name__}: {str(e)[:150]}")


def test_2_research_report():
    """东财研报 — 按个股查询。"""
    print("\n" + "=" * 80)
    print("2. 东财研报 — stock_research_report_em(symbol=)")
    print("=" * 80)
    try:
        import akshare as ak
        for code in ["000001", "600519"]:
            print(f"\n  --- {code} ---")
            try:
                df = ak.stock_research_report_em(symbol=code)
                if df is not None and not df.empty:
                    print(f"  记录数: {len(df)}")
                    print(f"  列名: {list(df.columns)}")
                    print(df.head(3).to_string())
                else:
                    print("  [空]")
            except Exception as e:
                print(f"  [失败] {type(e).__name__}: {str(e)[:150]}")
    except Exception as e:
        print(f"  [失败] {e}")


def test_3_juchao():
    """巨潮公告 — symbol='全部' 获取当日全市场公告。"""
    print("\n" + "=" * 80)
    print("3. 巨潮公告 — stock_notice_report(symbol='全部')")
    print("=" * 80)
    try:
        import akshare as ak
        today = datetime.now().strftime("%Y%m%d")
        df = ak.stock_notice_report(symbol="全部", date=today)
        if df is not None and not df.empty:
            print(f"  记录数: {len(df)}")
            print(f"  列名: {list(df.columns)}")
            print(df.head(5).to_string())
            print(f"\n  dtypes:\n{df.dtypes}")
        else:
            print(f"  [空] 今日无，试昨天")
            d = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            df = ak.stock_notice_report(symbol="全部", date=d)
            if df is not None and not df.empty:
                print(f"  {d} 有 {len(df)} 条")
                print(f"  列名: {list(df.columns)}")
                print(df.head(3).to_string())
    except Exception as e:
        print(f"  [失败] {type(e).__name__}: {str(e)[:200]}")


def test_3b_juchao_detail():
    """巨潮公告 — 个股披露明细。"""
    print("\n" + "=" * 80)
    print("3b. 巨潮披露明细 — stock_zh_a_disclosure_report_cninfo")
    print("=" * 80)
    try:
        import akshare as ak
        today = datetime.now().strftime("%Y%m%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        df = ak.stock_zh_a_disclosure_report_cninfo(
            symbol="600519", market="沪深京",
            start_date=week_ago, end_date=today,
        )
        if df is not None and not df.empty:
            print(f"  记录数: {len(df)}")
            print(f"  列名: {list(df.columns)}")
            print(df.head(3).to_string())
            print(f"\n  dtypes:\n{df.dtypes}")
        else:
            print("  [空]")
    except Exception as e:
        print(f"  [失败] {type(e).__name__}: {str(e)[:200]}")


def main():
    test_1_hot_concept()
    test_2_research_report()
    test_3_juchao()
    test_3b_juchao_detail()


if __name__ == "__main__":
    main()
