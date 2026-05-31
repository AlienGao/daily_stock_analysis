#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告比较测试脚本

直接比较两个已生成的报告文件，输出评级变化结果
"""

import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.report_comparator import ReportComparator


def compare_reports(today_report_path, previous_report_path):
    """比较两个报告文件"""
    print("=" * 60)
    print("报告比较测试")
    print("=" * 60)
    
    try:
        # 创建比较器实例
        comparator = ReportComparator()
        
        # 解析报告日期
        today_filename = os.path.basename(today_report_path)
        today_date_str = today_filename.split('_')[1].split('.')[0]
        today_date = datetime.strptime(today_date_str, "%Y%m%d")
        
        previous_filename = os.path.basename(previous_report_path)
        previous_date_str = previous_filename.split('_')[1].split('.')[0]
        previous_date = datetime.strptime(previous_date_str, "%Y%m%d")
        
        print(f"比较报告: {previous_date_str} → {today_date_str}")
        print("-" * 60)
        
        # 比较报告
        changes = comparator.compare_reports(today_report_path, previous_report_path)
        
        if changes:
            # 生成变化报告
            change_report = comparator.generate_change_report(changes, today_date, previous_date)
            
            # 打印完整报告
            print("\n===== 完整评级变化报告 =====")
            print(change_report)
            
            # 打印摘要
            print("\n===== 评级变化摘要 =====")
            print(f"比较日期: {previous_date.strftime('%Y-%m-%d')} → {today_date.strftime('%Y-%m-%d')}")
            print(f"变化股票数: {len(changes)}")
            for stock, (stock_name, old_rating, new_rating) in changes.items():
                print(f"- {stock_name}({stock}): {old_rating} → {new_rating}")
        else:
            print("未检测到评级变化")
            
    except Exception as e:
        print(f"比较失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("=" * 60)


if __name__ == "__main__":
    # 报告文件路径
    today_report = "reports/report_20260420.md"
    previous_report = "reports/report_20260417.md"
    
    # 检查文件是否存在
    if not os.path.exists(today_report):
        print(f"错误: 今天的报告文件不存在: {today_report}")
        sys.exit(1)
    
    if not os.path.exists(previous_report):
        print(f"错误: 前一个报告文件不存在: {previous_report}")
        sys.exit(1)
    
    # 执行比较
    compare_reports(today_report, previous_report)