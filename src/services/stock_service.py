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
import math
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any, List

from src.repositories.stock_repo import StockRepository

logger = logging.getLogger(__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce to float; NaN/inf become *default* so JSON serialization stays valid."""
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return default if not math.isfinite(out) else out


def _safe_optional_float(value: Any) -> Optional[float]:
    """Coerce to float or None; NaN/inf/empty become None."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if not math.isfinite(out) else out


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
            "open": _safe_float(spot.open_price),
            "high": _safe_float(spot.high),
            "low": _safe_float(spot.low),
            "close": _safe_float(spot.price),
            "volume": _safe_optional_float(spot.volume),
            "amount": _safe_optional_float(spot.amount),
            "change_percent": pct,
        })
    except Exception:
        pass




def _parse_history_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())[:8]
    if len(digits) != 8:
        return None
    return datetime.strptime(digits, "%Y%m%d").date()


def _filter_history_rows(
    rows: List[Dict[str, Any]],
    start_dt: Optional[date],
    end_dt: Optional[date],
) -> List[Dict[str, Any]]:
    if not start_dt and not end_dt:
        return rows
    out: List[Dict[str, Any]] = []
    for row in rows:
        d = _parse_history_date(str(row.get("date") or ""))
        if d is None:
            continue
        if start_dt and d < start_dt:
            continue
        if end_dt and d > end_dt:
            continue
        out.append(row)
    return out


def _dataframe_to_history_rows(df) -> List[Dict[str, Any]]:
    """将日线 DataFrame 转为 API 响应行列表。"""
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        date_val = row.get("date")
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = str(date_val)
        rows.append({
            "date": date_str,
            "open": _safe_float(row.get("open", 0)),
            "high": _safe_float(row.get("high", 0)),
            "low": _safe_float(row.get("low", 0)),
            "close": _safe_float(row.get("close", 0)),
            "volume": _safe_optional_float(row.get("volume")),
            "amount": _safe_optional_float(row.get("amount")),
            "change_percent": _safe_optional_float(row.get("pct_chg")),
        })
    return rows

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
        days: int = 30,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
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
            from src.services.history_loader import load_history_df
            from src.storage import DatabaseManager

            start_dt = _parse_history_date(start_date)
            end_dt = _parse_history_date(end_date)
            df, source = load_history_df(
                stock_code,
                days=days,
                target_date=end_dt,
                start_date=start_dt,
            )
            if df is None or df.empty:
                logger.warning(f"获取 {stock_code} 历史数据失败")
                return {"stock_code": stock_code, "period": period, "data": []}

            if source != "db_cache":
                try:
                    DatabaseManager().save_daily_data(df, stock_code, source)
                except Exception:
                    pass

            stock_name = stock_code
            try:
                from data_provider.base import DataFetcherManager
                stock_name = DataFetcherManager().get_stock_name(stock_code) or stock_code
            except Exception:
                pass

            data = _dataframe_to_history_rows(df)
            data = _filter_history_rows(data, start_dt, end_dt)
            _append_today_kl(data, stock_code)
            logger.info(f"[{stock_code}] 历史行情 {len(data)} 条, source={source}, 请求 {days} 天")
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
