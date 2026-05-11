# -*- coding: utf-8 -*-
"""券商金股因子 (Broker Recommend Factor).

盘后因子：基于本地 DB broker_recommend_monthly 数据 + 历史回测结果。
3 个子信号：
- 推荐覆盖度 (0-40)：券商数量在当月金股中的百分位
- 券商质量加权 (0-40)：历史回测胜率 + 平均收益
- 连续推荐加成 (0-20)：连续月份被多家券商推荐

数据来源: 本地 SQLite (broker_recommend_monthly + broker_backtest_result)
"""

import logging
from datetime import date
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


class BrokerRecommendFactor(BaseFactor):
    """券商金股因子。

    被越多券商覆盖的股票说明机构关注度越高，
    推荐券商历史胜率越高信号越可靠，
    连续月份推荐是更强的共识信号。
    """

    name = "broker_recommend"
    available_intraday = False
    available_postmarket = True
    weight = 20.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """获取当月券商金股推荐数据：优先 DB，无数据时降级 Tushare API。

        Args:
            trade_date: 交易日期 YYYYMMDD，用于推断目标月份
        """
        from src.storage import DatabaseManager

        trade_date_clean = str(trade_date).replace("-", "").strip()
        if len(trade_date_clean) >= 6:
            month = trade_date_clean[:6]
        else:
            month = date.today().strftime("%Y%m")

        db = DatabaseManager()
        try:
            rows = db.get_broker_recommend_monthly(month)
        except Exception as e:
            logger.warning("[BrokerRecommend] DB 读取失败: %s", e)
            return None

        # 无数据时尝试 Tushare API 兜底并自动落库
        if not rows:
            tushare_fetcher = kwargs.get("tushare_fetcher")
            if tushare_fetcher is not None:
                logger.info(
                    "[BrokerRecommend] 本地 DB 无 %s 月数据，尝试 Tushare API 兜底", month
                )
                df_api = tushare_fetcher.get_broker_recommend(month)
                if df_api is not None and not df_api.empty:
                    try:
                        db.save_broker_recommend_monthly(month, df_api.reset_index())
                        rows = db.get_broker_recommend_monthly(month)
                    except Exception as e:
                        logger.warning("[BrokerRecommend] 落库/重读失败: %s", e)
                        return None

            if not rows:
                logger.warning("[BrokerRecommend] %s 月数据不可用（DB+Tushare 均空）", month)
                return None

        df = pd.DataFrame([{
            'ts_code': r.ts_code,
            'broker': r.broker,
            'name': r.name,
            'broker_count': r.broker_count,
        } for r in rows])

        # 附加数据供 _compute_signals 使用
        df.attrs['month'] = month

        try:
            broker_quality = self._load_broker_quality(db, month)
            df.attrs['broker_quality'] = broker_quality
        except Exception as e:
            logger.warning("[BrokerRecommend] 加载券商质量失败: %s", e)
            df.attrs['broker_quality'] = {}

        try:
            consecutive = db.get_consecutive_monthly_stocks(month)
            df.attrs['consecutive_stocks'] = {c['ts_code']: c for c in consecutive}
        except Exception as e:
            logger.warning("[BrokerRecommend] 加载连续推荐失败: %s", e)
            df.attrs['consecutive_stocks'] = {}

        logger.info(
            "[BrokerRecommend] %s 月: %d 条推荐, %d 只股票, %d 家券商, %d 家有历史质量",
            month, len(df), df['ts_code'].nunique(), df['broker'].nunique(),
            len(broker_quality),
        )
        return df

    # ------------------------------------------------------------------
    # 券商历史质量
    # ------------------------------------------------------------------

    @staticmethod
    def _load_broker_quality(db, current_month: str) -> Dict[str, float]:
        """加载券商历史回测综合质量分 (0-1)。

        取最近 3 个有回测结果的月份，按 stock_count 加权汇总。
        quality = win_rate * 0.7 + sigmoid(avg_return) * 0.3
        无历史数据的券商不在此 map 中，后续默认给 0.5 中性分。
        """
        available_months = db.get_broker_recommend_months()
        past_months = [m for m in available_months if m != current_month][:3]

        broker_stats: Dict[str, Dict[str, float]] = {}
        for pm in past_months:
            bt = db.get_broker_backtest(pm)
            if not bt:
                continue
            for br in bt.get('brokers', []):
                name = br['broker']
                if name not in broker_stats:
                    broker_stats[name] = {'wins': 0.0, 'total': 0.0, 'ret_sum': 0.0}
                n = br.get('stock_count', 1)
                broker_stats[name]['wins'] += br.get('win_rate', 0) * n
                broker_stats[name]['total'] += n
                broker_stats[name]['ret_sum'] += br.get('avg_return', 0) * n

        quality: Dict[str, float] = {}
        for name, stats in broker_stats.items():
            if stats['total'] <= 0:
                continue
            wr = stats['wins'] / stats['total']
            avg_ret = stats['ret_sum'] / stats['total']
            # sigmoid: 0%→0.5, 2%→0.73, 5%→0.92, 10%→0.99
            ret_score = 1.0 / (1.0 + np.exp(-avg_ret * 50))
            quality[name] = round(wr * 0.7 + ret_score * 0.3, 4)

        return quality

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 3 个子信号，各自归一化到满分区间。

        Signals 以唯一 ts_code 为索引（per-stock），score() 再 map 回 per-row。
        """
        ts_code_col = 'ts_code'
        broker_col = 'broker'

        stock_idx = pd.Index(df[ts_code_col].unique())
        signals: Dict[str, pd.Series] = {}

        # --- 1. 推荐覆盖度 (0-40)：broker_count 在当月金股中的百分位 ---
        broker_count = df.groupby(ts_code_col)[broker_col].nunique()
        coverage_pct = broker_count.rank(pct=True).reindex(stock_idx).fillna(0)
        signals['coverage'] = (coverage_pct * 40).clip(0, 40)

        # --- 2. 券商质量加权 (0-40) ---
        broker_quality = df.attrs.get('broker_quality', {})
        if broker_quality:
            bq_series = df[[ts_code_col, broker_col]].copy()
            bq_series['_bq'] = bq_series[broker_col].map(broker_quality).fillna(0.5)
            stock_quality = bq_series.groupby(ts_code_col)['_bq'].mean()
            signals['broker_quality'] = (
                stock_quality.reindex(stock_idx).fillna(0.5) * 40
            ).clip(0, 40)
        else:
            signals['broker_quality'] = pd.Series(20.0, index=stock_idx)

        # --- 3. 连续推荐加成 (0-20) ---
        consecutive_stocks = df.attrs.get('consecutive_stocks', {})
        cons_scores = pd.Series(0.0, index=stock_idx)
        for ts in stock_idx:
            c = consecutive_stocks.get(ts)
            if c:
                ratio = min(c['broker_count_prev'] / max(c['broker_count_current'], 1), 1.0)
                cons_scores[ts] = round(ratio * 20, 1)
        signals['consecutive'] = cons_scores

        return signals

    # ------------------------------------------------------------------
    # score / describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        stock_scores = sum(signals.values()).clip(0, 100)

        # 索引归一化为裸代码（与 engine 中其他因子对齐）
        stock_scores.index = stock_scores.index.map(
            lambda x: x.split(".")[0] if "." in str(x) else str(x)
        )
        stock_scores.name = self.name
        return stock_scores

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        reasons: Dict[str, List[str]] = {}
        if df.empty:
            return reasons

        ts_code_col = next((c for c in df.columns if 'ts_code' in c), 'ts_code')
        broker_col = next((c for c in df.columns if 'broker' in c), 'broker')
        name_col = next((c for c in df.columns if c == 'name'), 'name')

        signals = self._compute_signals(df)

        # groupby 替代 iterrows：券商列表（去重排序）+ 股票名称
        broker_by_stock: Dict[str, List[str]] = (
            df.groupby(ts_code_col)[broker_col]
            .apply(lambda x: sorted(x.unique()))
            .to_dict()
        )
        name_by_stock: Dict[str, str] = (
            df.groupby(ts_code_col)[name_col].first().to_dict()
        )

        signal_meta = [
            ('coverage', '券商覆盖', 40),
            ('broker_quality', '券商质量', 40),
            ('consecutive', '连续推荐', 20),
        ]
        threshold = self._LABEL_THRESHOLD_RATIO

        stock_scores = scores.groupby(df[ts_code_col]).first()

        for ts, brokers in broker_by_stock.items():
            # ts 是 ts_code 格式，lookup 同时尝试裸代码（兼容 score() 的 bare code 索引）
            bare = ts.split(".")[0] if "." in str(ts) else str(ts)
            score_val = stock_scores.get(ts, 0) or scores.get(bare, 0)
            if score_val <= 0:
                continue

            n = len(brokers)
            labels: List[str] = []

            for key, label, max_val in signal_meta:
                val = signals[key].get(ts, 0.0)
                if val < max_val * threshold:
                    continue
                if key == 'coverage':
                    labels.append(f"券商金股({n}家推荐)")
                elif key == 'broker_quality':
                    bq = df.attrs.get('broker_quality', {})
                    avg_q = sum(bq.get(b, 0.5) for b in brokers) / max(len(brokers), 1)
                    labels.append(f"券商质量({avg_q:.0%})")
                elif key == 'consecutive':
                    c = df.attrs.get('consecutive_stocks', {}).get(ts, {})
                    prev_n = c.get('broker_count_prev', 0)
                    curr_n = c.get('broker_count_current', n)
                    labels.append(f"连续推荐(上月{prev_n}家→本月{curr_n}家)")

            if labels:
                reasons[bare] = labels

        return reasons
