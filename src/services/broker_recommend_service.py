# -*- coding: utf-8 -*-
"""券商月度金股推荐服务。

提供券商金股数据的获取、存储和回测功能。
"""

import calendar
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from src.storage import DatabaseManager, StockDaily

logger = logging.getLogger(__name__)


class BrokerRecommendService:
    """券商金股推荐服务。"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def fetch_and_store_month(self, month: str) -> int:
        """获取指定月份券商金股并存入数据库。

        Args:
            month: YYYYMM 格式月份

        Returns:
            保存的记录数
        """
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher()
            if not tf.is_available():
                logger.error("[BrokerRecommend] Tushare 不可用")
                return 0

            df = tf._api.query("broker_recommend", month=month)
            if df is None or df.empty:
                logger.info(f"[BrokerRecommend] {month} 月无数据")
                return 0

            return self.db.save_broker_recommend_monthly(month, df)
        except Exception as e:
            logger.error(f"[BrokerRecommend] 获取 {month} 月数据失败: {e}")
            return 0

    def get_monthly_recommendations(self, month: str) -> pd.DataFrame:
        """获取指定月份的金股 DataFrame，按券商分组。"""
        records = self.db.get_broker_recommend_monthly(month)
        if not records:
            return pd.DataFrame()

        data = [r.to_dict() for r in records]
        df = pd.DataFrame(data)

        # 按券商分组计算去重后的金股列表
        if not df.empty:
            df = df.sort_values(['broker', 'ts_code'])
        return df

    def get_available_months(self) -> List[str]:
        """获取有数据的月份列表。"""
        return self.db.get_broker_recommend_months()

    def _get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """获取指定日期范围内的交易日列表。"""
        try:
            from data_provider.tushare_fetcher import TushareFetcher

            tf = TushareFetcher()
            cal_df = tf._call_api_with_rate_limit(
                "trade_cal",
                exchange="SSE",
                start_date=start_date,
                end_date=end_date,
                is_open="1",
            )
            if cal_df is not None and not cal_df.empty:
                return sorted(cal_df["cal_date"].tolist())
        except Exception as e:
            logger.debug(f"[BrokerRecommend] 获取交易日历失败: {e}")

        # Fallback: 简单工作日
        days = []
        d = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
        ed = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
        while d <= ed:
            if d.weekday() < 5:
                days.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
        return days

    def _get_stock_prices(
        self, ts_code: str, start_date: str, end_date: str
    ) -> Dict[str, float]:
        """获取指定股票在日期范围内的收盘价。"""
        try:
            code = ts_code.split(".")[0] if "." in ts_code else ts_code
            records = self.db.get_data_range(
                code,
                date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8])),
                date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8])),
            )
            prices = {}
            for r in records:
                d = r.date.strftime("%Y%m%d") if isinstance(r.date, date) else str(r.date)[:8]
                if r.close:
                    prices[d] = float(r.close)
            return prices
        except Exception:
            return {}

    def compute_backtest(self, month: str, top_n_per_broker: int = 10) -> Dict[str, Any]:
        """对指定月份金股池按券商分组做回测。

        回测逻辑：次月第一个交易日开盘买入 -> 次月最后一个交易日收盘卖出。
        按券商分组，每组内等权分配资金。

        Args:
            month: YYYYMM 格式月份
            top_n_per_broker: 每个券商最多取几只金股

        Returns:
            回测结果字典
        """
        df = self.get_monthly_recommendations(month)
        if df is None or df.empty:
            return {"error": f"{month} 月无数据"}

        # 当月第一个交易日开盘买入 → 当月最后一个交易日收盘卖出
        year = int(month[:4])
        mon = int(month[4:6])
        if mon == 12:
            next_month = f"{year + 1}01"
        else:
            next_month = f"{year}{mon + 1:02d}"

        last_day = calendar.monthrange(year, mon)[1]
        month_start = f"{month}01"
        month_end = f"{month}{last_day:02d}"
        trading_days = self._get_trading_days(month_start, month_end)

        if len(trading_days) < 2:
            return {"error": f"{next_month} 交易日不足"}

        buy_date = trading_days[0]
        sell_date = trading_days[-1]

        # 按券商分组回测
        brokers_result: List[Dict[str, Any]] = []
        stock_results: Dict[str, Dict[str, Any]] = {}

        all_brokers = df["broker"].unique()

        for broker in all_brokers:
            broker_df = df[df["broker"] == broker].drop_duplicates("ts_code")
            stocks = broker_df.head(top_n_per_broker)

            broker_daily_returns: Dict[str, List[Dict[str, Any]]] = {}
            broker_wins = 0
            broker_total = 0
            broker_pnl_sum = 0.0

            for _, row in stocks.iterrows():
                ts = str(row["ts_code"])
                name = str(row.get("name", ""))
                broker_count = int(row.get("broker_count", 1))

                prices = self._get_stock_prices(ts, month_start, month_end)
                if not prices:
                    continue

                buy_price = prices.get(buy_date)
                sell_price = prices.get(sell_date)

                if not buy_price or not sell_price or buy_price <= 0:
                    continue

                ret = (sell_price - buy_price) / buy_price
                broker_wins += 1 if ret > 0 else 0
                broker_total += 1
                broker_pnl_sum += ret

                # 每日累计收益
                daily_rets = []
                cumulative = 0.0
                for td in trading_days:
                    p = prices.get(td)
                    if p and buy_price > 0:
                        d_ret = (p - buy_price) / buy_price
                        cumulative = d_ret
                        daily_rets.append({
                            "date": td,
                            "return": round(d_ret, 4),
                            "cumulative": round(cumulative, 4),
                        })

                broker_daily_returns[ts] = daily_rets

                # 个股结果
                if ts not in stock_results:
                    stock_results[ts] = {
                        "ts_code": ts,
                        "name": name,
                        "broker_count": broker_count,
                        "broker": broker,
                        "daily_returns": [],
                    }
                    for td in trading_days:
                        stock_results[ts]["daily_returns"].append({
                            "date": td,
                            "return": None,
                            "cumulative": None,
                        })

                for dr in daily_rets:
                    for sd in stock_results[ts]["daily_returns"]:
                        if sd["date"] == dr["date"]:
                            sd["return"] = dr["return"]
                            sd["cumulative"] = dr["cumulative"]

            if broker_total == 0:
                continue

            avg_ret = broker_pnl_sum / broker_total
            brokers_result.append({
                "broker": broker,
                "stock_count": broker_total,
                "cumulative_return": round(broker_pnl_sum, 4),
                "win_rate": round(broker_wins / broker_total, 4),
                "avg_return": round(avg_ret, 4),
                "daily_returns": self._merge_broker_daily_returns(broker_daily_returns, trading_days),
                "stocks": [
                    {"ts_code": str(r["ts_code"]), "name": str(r.get("name", ""))}
                    for _, r in stocks.iterrows()
                ],
            })

        return {
            "month": month,
            "next_month": next_month,
            "buy_date": buy_date,
            "sell_date": sell_date,
            "total_recommendations": len(df),
            "unique_stocks": df["ts_code"].nunique(),
            "unique_brokers": len(all_brokers),
            "brokers": brokers_result,
            "stock_returns": list(stock_results.values()),
        }

    def _merge_broker_daily_returns(
        self, stock_returns: Dict[str, List[Dict[str, Any]]], trading_days: List[str]
    ) -> List[Dict[str, Any]]:
        """合并多只股票的每日收益为组合每日等权收益。

        个股 return 已是「(当日价 - 买入价) / 买入价」即累计收益，
        组合累计 = 当日个股票计收益的等权平均，组合日收益 = 累计的日环比变化。
        """
        if not stock_returns:
            return []

        result = []
        prev_cum = 0.0
        for td in trading_days:
            daily_rets = []
            for ts, rets in stock_returns.items():
                for r in rets:
                    if r["date"] == td and r["return"] is not None:
                        daily_rets.append(r["return"])
                        break

            if daily_rets:
                cumulative = sum(daily_rets) / len(daily_rets)
            else:
                cumulative = prev_cum

            daily_ret = cumulative - prev_cum
            result.append({
                "date": td,
                "return": round(daily_ret, 4),
                "cumulative": round(cumulative, 4),
            })
            prev_cum = cumulative

        return result
