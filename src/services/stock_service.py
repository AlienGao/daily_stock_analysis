# -*- coding: utf-8 -*-
"""
===================================
股票数据服务层
===================================

职责：
1. 封装股票数据获取逻辑
2. 提供实时行情和历史数据接口
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List

from src.repositories.stock_repo import StockRepository

logger = logging.getLogger(__name__)


def _append_today_kl(data: list, stock_code: str) -> None:
    """从 realtime_spot 取当日 OHLC 追加到 K 线列表末尾。

    stock_daily 只有历史日线，盘中/盘后当日 K 线尚未落库时用此补齐。
    """
    if not data:
        return
    from datetime import date as dt_date
    today_str = dt_date.today().isoformat()
    if data[-1].get("date") == today_str:
        return  # 已有当日数据
    try:
        from src.storage import DatabaseManager, RealtimeSpot
        db = DatabaseManager()
        with db.get_session() as s:
            spot = s.execute(
                s.query(RealtimeSpot).filter(RealtimeSpot.code == stock_code)
            ).scalars().first()
            if spot is None:
                return
            if spot.trade_date and spot.trade_date != today_str:
                return
        if not (spot.open_price and spot.high and spot.low and spot.price):
            return
        prev_close = data[-1].get("close", 0) or 0
        pct = 0.0
        if prev_close > 0:
            pct = round((float(spot.price) - prev_close) / prev_close * 100, 2)
        data.append({
            "date": today_str,
            "open": float(spot.open_price),
            "high": float(spot.high),
            "low": float(spot.low),
            "close": float(spot.price),
            "volume": float(spot.volume) if spot.volume else None,
            "amount": float(spot.amount) if spot.amount else None,
            "change_percent": pct,
        })
    except Exception:
        pass


class StockService:
    """
    股票数据服务
    
    封装股票数据获取的业务逻辑
    """
    
    def __init__(self):
        """初始化股票数据服务"""
        self.repo = StockRepository()
    
    def get_realtime_quote(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票实时行情
        
        Args:
            stock_code: 股票代码
            
        Returns:
            实时行情数据字典
        """
        try:
            # 调用数据获取器获取实时行情
            from data_provider.base import DataFetcherManager
            
            manager = DataFetcherManager()
            quote = manager.get_realtime_quote(stock_code)
            
            if quote is None:
                logger.warning(f"获取 {stock_code} 实时行情失败")
                return None
            
            # UnifiedRealtimeQuote 是 dataclass，使用 getattr 安全访问字段
            # 字段映射: UnifiedRealtimeQuote -> API 响应
            # - code -> stock_code
            # - name -> stock_name
            # - price -> current_price
            # - change_amount -> change
            # - change_pct -> change_percent
            # - open_price -> open
            # - high -> high
            # - low -> low
            # - pre_close -> prev_close
            # - volume -> volume
            # - amount -> amount
            return {
                "stock_code": getattr(quote, "code", stock_code),
                "stock_name": getattr(quote, "name", None),
                "current_price": getattr(quote, "price", 0.0) or 0.0,
                "change": getattr(quote, "change_amount", None),
                "change_percent": getattr(quote, "change_pct", None),
                "open": getattr(quote, "open_price", None),
                "high": getattr(quote, "high", None),
                "low": getattr(quote, "low", None),
                "prev_close": getattr(quote, "pre_close", None),
                "volume": getattr(quote, "volume", None),
                "amount": getattr(quote, "amount", None),
                "update_time": datetime.now().isoformat(),
            }
            
        except ImportError:
            logger.warning("DataFetcherManager 未找到，使用占位数据")
            return self._get_placeholder_quote(stock_code)
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}", exc_info=True)
            return None
    
    def get_history_data(
        self,
        stock_code: str,
        period: str = "daily",
        days: int = 30
    ) -> Dict[str, Any]:
        """
        获取股票历史行情
        
        Args:
            stock_code: 股票代码
            period: K 线周期 (daily/weekly/monthly)
            days: 获取天数
            
        Returns:
            历史行情数据字典
            
        Raises:
            ValueError: 当 period 不是 daily 时抛出（weekly/monthly 暂未实现）
        """
        # 验证 period 参数，只支持 daily
        if period != "daily":
            raise ValueError(
                f"暂不支持 '{period}' 周期，目前仅支持 'daily'。"
                "weekly/monthly 聚合功能将在后续版本实现。"
            )
        
        try:
            # 1. 优先从本地 DB 读取
            from src.storage import DatabaseManager
            db = DatabaseManager()
            end_dt = date.today()
            # 请求 N 个交易日，DB 按日历日扩宽范围（周末/节假日无数据）
            start_dt = end_dt - timedelta(days=days * 2)
            db_rows = db.get_data_range(stock_code, start_dt, end_dt)

            # 至少要有 60% 的期望交易日才算命中
            min_expected = max(5, int(days * 0.6))
            if db_rows and len(db_rows) >= min_expected:
                data = []
                for row in db_rows:
                    row_dict = row.to_dict() if hasattr(row, 'to_dict') else row
                    date_val = row_dict.get('date')
                    if hasattr(date_val, 'strftime'):
                        date_str = date_val.strftime("%Y-%m-%d")
                    else:
                        date_str = str(date_val)
                    data.append({
                        "date": date_str,
                        "open": float(row_dict.get("open", 0) or 0),
                        "high": float(row_dict.get("high", 0) or 0),
                        "low": float(row_dict.get("low", 0) or 0),
                        "close": float(row_dict.get("close", 0) or 0),
                        "volume": float(row_dict.get("volume", 0)) if row_dict.get("volume") else None,
                        "amount": float(row_dict.get("amount", 0)) if row_dict.get("amount") else None,
                        "change_percent": float(row_dict.get("pct_chg", 0)) if row_dict.get("pct_chg") else None,
                    })
                logger.info(f"[{stock_code}] DB 命中: {len(data)} 条, 请求 {days} 天")
                _append_today_kl(data, stock_code)
                return {
                    "stock_code": stock_code,
                    "stock_name": stock_code,
                    "period": period,
                    "data": data,
                }

            # 2. DB 数据不足，回退到外部数据源
            logger.info(f"[{stock_code}] DB 数据不足 ({len(db_rows) if db_rows else 0} 条), 回退外部 API")
            from data_provider.base import DataFetcherManager

            manager = DataFetcherManager()
            df, source = manager.get_daily_data(stock_code, days=days)

            if df is None or df.empty:
                logger.warning(f"获取 {stock_code} 历史数据失败")
                return {"stock_code": stock_code, "period": period, "data": []}

            # 获取股票名称
            stock_name = manager.get_stock_name(stock_code)

            # 写入 DB 缓存供后续请求使用
            try:
                db.save_daily_data(df, stock_code, source)
            except Exception:
                pass

            # 转换为响应格式
            data = []
            for _, row in df.iterrows():
                date_val = row.get("date")
                if hasattr(date_val, "strftime"):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_val)

                data.append({
                    "date": date_str,
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)) if row.get("volume") else None,
                    "amount": float(row.get("amount", 0)) if row.get("amount") else None,
                    "change_percent": float(row.get("pct_chg", 0)) if row.get("pct_chg") else None,
                })

            _append_today_kl(data, stock_code)
            return {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "period": period,
                "data": data,
            }

        except ImportError:
            logger.warning("DataFetcherManager 未找到，返回空数据")
            return {"stock_code": stock_code, "period": period, "data": []}
        except Exception as e:
            logger.error(f"获取历史数据失败: {e}", exc_info=True)
            return {"stock_code": stock_code, "period": period, "data": []}
    
    def _get_placeholder_quote(self, stock_code: str) -> Dict[str, Any]:
        """
        获取占位行情数据（用于测试）
        
        Args:
            stock_code: 股票代码
            
        Returns:
            占位行情数据
        """
        return {
            "stock_code": stock_code,
            "stock_name": f"股票{stock_code}",
            "current_price": 0.0,
            "change": None,
            "change_percent": None,
            "open": None,
            "high": None,
            "low": None,
            "prev_close": None,
            "volume": None,
            "amount": None,
            "update_time": datetime.now().isoformat(),
        }
