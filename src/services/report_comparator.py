#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
报告比较工具

比较当天报告与前一个交易日的报告，找出评级变化的股票
"""

import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import logging

from src.utils.rating_trend import rating_change_emoji, sort_rating_changes

logger = logging.getLogger(__name__)


class ReportComparator:
    """报告比较器"""
    
    def __init__(self, reports_dir: str = "reports"):
        """初始化
        
        Args:
            reports_dir: 报告目录
        """
        self.reports_dir = reports_dir
        
    def get_report_files(self) -> List[str]:
        """获取所有报告文件，按日期排序"""
        files = []
        pattern = r"report_(\d{8})\.md"
        
        for filename in os.listdir(self.reports_dir):
            match = re.match(pattern, filename)
            if match:
                date_str = match.group(1)
                try:
                    date = datetime.strptime(date_str, "%Y%m%d")
                    files.append((date, os.path.join(self.reports_dir, filename)))
                except ValueError:
                    pass
        
        # 按日期排序
        files.sort(key=lambda x: x[0], reverse=True)
        return [f[1] for f in files]
    
    def parse_report(self, report_file: str) -> Dict[str, Tuple[str, str]]:
        """解析报告文件，提取股票评级和名称
        
        Args:
            report_file: 报告文件路径
            
        Returns:
            Dict[股票代码, (股票名称, 评级)]
        """
        stock_info = {}
        pattern = r"(🟢|🟡|⚪|🟠|🔴)\s+\*\*(.*?)\((.*?)\)\*\*.*?(买入|持有|观望|减持|卖出)"
        
        try:
            with open(report_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            matches = re.findall(pattern, content)
            for emoji, name, code, rating in matches:
                stock_info[code] = (name.strip(), rating)
        except Exception as e:
            logger.error(f"解析报告文件失败 {report_file}: {e}")
        
        return stock_info
    
    def get_previous_trading_day_report(self, current_date: datetime) -> str:
        """获取前一个交易日的报告
        
        Args:
            current_date: 当前日期
            
        Returns:
            前一个交易日的报告文件路径，不存在则返回None
        """
        report_files = self.get_report_files()
        if not report_files:
            return None
        
        # 从最新的报告开始找，排除今天的报告
        current_date_str = current_date.strftime("%Y%m%d")
        
        for report_file in report_files:
            filename = os.path.basename(report_file)
            report_date_str = filename.split('_')[1].split('.')[0]
            
            if report_date_str < current_date_str:
                return report_file
        
        return None
    
    def compare_reports(self, current_report: str, previous_report: str) -> Dict[str, Tuple[str, str, str]]:
        """比较两个报告，找出评级变化的股票
        
        Args:
            current_report: 当前报告文件路径
            previous_report: 前一个报告文件路径
            
        Returns:
            Dict[股票代码, (股票名称, 前评级, 当前评级)]
        """
        current_ratings = self.parse_report(current_report)
        previous_ratings = self.parse_report(previous_report)
        
        changes = {}
        
        # 检查在两个报告中都存在的股票
        common_stocks = set(current_ratings.keys()) & set(previous_ratings.keys())
        for stock in common_stocks:
            if current_ratings[stock][1] != previous_ratings[stock][1]:
                # 使用当前报告中的股票名称
                stock_name = current_ratings[stock][0]
                old_rating = previous_ratings[stock][1]
                new_rating = current_ratings[stock][1]
                changes[stock] = (stock_name, old_rating, new_rating)
        
        return changes
    
    def generate_change_report(self, changes: Dict[str, Tuple[str, str, str]], current_date: datetime, previous_date: datetime) -> str:
        """生成变化报告
        
        Args:
            changes: 评级变化字典
            current_date: 当前日期
            previous_date: 前一个交易日日期
            
        Returns:
            变化报告内容
        """
        if not changes:
            return """# 📊 评级变化报告

**未检测到评级变化**

"""
        sorted_changes = sort_rating_changes(list(changes.items()))
        
        content = "# 📊 评级变化报告\n\n"
        content += "**比较日期**: " + previous_date.strftime('%Y-%m-%d') + " → " + current_date.strftime('%Y-%m-%d') + "\n\n"
        
        content += "## 🔄 评级变化股票\n\n"
        
        for stock, (stock_name, old_rating, new_rating) in sorted_changes:
            emoji = rating_change_emoji(old_rating, new_rating)
            
            # 简化评级显示
            def simplify_rating(rating):
                if rating == "买入":
                    return "买入"
                elif rating == "持有":
                    return "持有"
                elif rating == "观望":
                    return "观望"
                elif rating == "减持":
                    return "减持"
                elif rating == "卖出":
                    return "卖出"
                return rating
            
            content += "- " + emoji + " **" + stock_name + "(" + stock + ")**: " + simplify_rating(old_rating) + " → " + simplify_rating(new_rating) + "\n"
        
        content += "\n**总计**: " + str(len(changes)) + " 只股票评级发生变化\n"
        
        return content