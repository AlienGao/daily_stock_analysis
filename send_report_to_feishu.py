#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书通知测试脚本

将报告比较结果发送到飞书
"""

import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.report_comparator import ReportComparator
from src.notification import get_notification_service


def send_report_to_feishu(today_report_path, previous_report_path):
    """比较报告并发送到飞书"""
    print("=" * 60)
    print("发送报告到飞书")
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
            
            # 获取通知服务
            notifier = get_notification_service()
            
            # 检查服务是否可用
            if notifier.is_available():
                print("发送报告到飞书...")
                success = notifier.send(change_report)
                if success:
                    print("报告发送成功！")
                else:
                    print("报告发送失败！")
            else:
                print("通知服务不可用，请检查配置")
        else:
            print("未检测到评级变化，无需发送")
            
    except Exception as e:
        print(f"发送失败: {e}")
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
    
    # 执行发送
    send_report_to_feishu(today_report, previous_report)