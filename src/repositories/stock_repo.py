# -*- coding: utf-8 -*-
"""
===================================
股票数据访问层
===================================

职责：
1. 封装股票数据的数据库操作
2. 提供日线数据查询接口
"""

import logging
from datetime import date
from typing import Optional, List, Dict, Any

import pandas as pd
from sqlalchemy import and_, desc, select

from src.storage import DatabaseManager, StockDaily, StockTechIndicator

logger = logging.getLogger(__name__)


class StockRepository:
    """
    股票数据访问层
    
    封装 StockDaily 表的数据库操作
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        初始化数据访问层
        
        Args:
            db_manager: 数据库管理器（可选，默认使用单例）
        """
        self.db = db_manager or DatabaseManager.get_instance()
    
    def get_latest(self, code: str, days: int = 2) -> List[StockDaily]:
        """
        获取最近 N 天的数据
        
        Args:
            code: 股票代码
            days: 获取天数
            
        Returns:
            StockDaily 对象列表（按日期降序）
        """
        try:
            return self.db.get_latest_data(code, days)
        except Exception as e:
            logger.error(f"获取最新数据失败: {e}")
            return []
    
    def get_range(
        self,
        code: str,
        start_date: date,
        end_date: date
    ) -> List[StockDaily]:
        """
        获取指定日期范围的数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            StockDaily 对象列表
        """
        try:
            return self.db.get_data_range(code, start_date, end_date)
        except Exception as e:
            logger.error(f"获取日期范围数据失败: {e}")
            return []
    
    def save_dataframe(
        self,
        df: pd.DataFrame,
        code: str,
        data_source: str = "Unknown"
    ) -> int:
        """
        保存 DataFrame 到数据库
        
        Args:
            df: 包含日线数据的 DataFrame
            code: 股票代码
            data_source: 数据来源
            
        Returns:
            保存的记录数
        """
        try:
            return self.db.save_daily_data(df, code, data_source)
        except Exception as e:
            logger.error(f"保存日线数据失败: {e}")
            return 0
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        检查是否有指定日期的数据
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            是否存在数据
        """
        try:
            return self.db.has_today_data(code, target_date)
        except Exception as e:
            logger.error(f"检查数据存在失败: {e}")
            return False
    
    def get_analysis_context(
        self, 
        code: str, 
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取分析上下文
        
        Args:
            code: 股票代码
            target_date: 目标日期
            
        Returns:
            分析上下文字典
        """
        try:
            return self.db.get_analysis_context(code, target_date)
        except Exception as e:
            logger.error(f"获取分析上下文失败: {e}")
            return None

    def get_start_daily(self, *, code: str, analysis_date: date) -> Optional[StockDaily]:
        """Return StockDaily for analysis_date (preferred) or nearest previous date."""
        candidate_codes = self._candidate_codes(code)
        with self.db.get_session() as session:
            row = session.execute(
                select(StockDaily)
                .where(and_(StockDaily.code.in_(candidate_codes), StockDaily.date <= analysis_date))
                .order_by(desc(StockDaily.date))
                .limit(1)
            ).scalar_one_or_none()
            return row

    def get_forward_bars(self, *, code: str, analysis_date: date, eval_window_days: int) -> List[StockDaily]:
        """Return forward daily bars after analysis_date, up to eval_window_days."""
        candidate_codes = self._candidate_codes(code)
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily)
                .where(and_(StockDaily.code.in_(candidate_codes), StockDaily.date > analysis_date))
                .order_by(StockDaily.date)
                .limit(eval_window_days)
            ).scalars().all()
            return list(rows)

    # ------------------------------------------------------------------
    # Tushare 技术指标缓存
    # ------------------------------------------------------------------

    def upsert_tech_indicators(self, df: pd.DataFrame, code: str) -> int:
        """批量 upsert Tushare 技术指标缓存。

        Args:
            df: 包含 stk_factor 字段的 DataFrame
            code: 股票代码

        Returns:
            写入的记录数
        """
        try:
            return self.db.upsert_tech_indicators(df, code)
        except Exception as e:
            logger.error(f"保存技术指标缓存失败 {code}: {e}")
            return 0

    def get_tech_indicator(
        self, code: str, target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """获取单只股票指定日期的缓存技术指标。"""
        try:
            return self.db.get_tech_indicator(code, target_date)
        except Exception as e:
            logger.error(f"获取技术指标缓存失败 {code}: {e}")
            return None

    def get_tech_indicators_batch(
        self, codes: List[str], target_date: Optional[date] = None
    ) -> Dict[str, Dict[str, Any]]:
        """批量获取多只股票指定日期的缓存技术指标。"""
        try:
            return self.db.get_tech_indicators_batch(codes, target_date)
        except Exception as e:
            logger.error(f"批量获取技术指标缓存失败: {e}")
            return {}

    @staticmethod
    def _candidate_codes(code: str) -> List[str]:
        """Return alias candidates to tolerate mixed code formats in stock_daily.

        Examples:
        - 000614 -> [000614, 000614.SZ, 000614.SH]
        - 000614.SZ -> [000614.SZ, 000614, 000614.SH]
        - HK00700 -> [HK00700, 00700.HK, 00700]
        """
        raw = str(code or "").strip().upper()
        if not raw:
            return []

        candidates: List[str] = [raw]

        def _add(value: str) -> None:
            normalized = str(value or "").strip().upper()
            if normalized and normalized not in candidates:
                candidates.append(normalized)

        if raw.endswith((".SZ", ".SH", ".SS", ".BJ")):
            base = raw.rsplit(".", 1)[0]
            _add(base)
            if base.isdigit() and len(base) == 6:
                _add(f"{base}.SZ")
                _add(f"{base}.SH")
            return candidates

        if raw.endswith(".HK"):
            base = raw.rsplit(".", 1)[0]
            _add(base)
            if base.isdigit():
                _add(f"HK{base.zfill(5)}")
            return candidates

        if raw.startswith("HK") and raw[2:].isdigit():
            digits = raw[2:].zfill(5)
            _add(digits)
            _add(f"{digits}.HK")
            return candidates

        if raw.isdigit() and len(raw) == 6:
            _add(f"{raw}.SZ")
            _add(f"{raw}.SH")
            return candidates

        if raw.isdigit() and len(raw) == 5:
            _add(f"HK{raw}")
            _add(f"{raw}.HK")
            return candidates

        return candidates
