# -*- coding: utf-8 -*-
"""股票发现主引擎。

协调因子注册、数据获取、加权评分、去重排序，输出发现结果。
"""

import json
import logging
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
import requests

from src.discovery.config import DiscoveryConfig
from src.discovery.factors.base import BaseFactor, DiscoveryResult
from data_provider.base import is_st_stock

logger = logging.getLogger(__name__)

ModeStr = Literal["intraday", "postmarket"]

_FACTOR_DISPLAY: Dict[str, str] = {
    "money_flow": "资金流向",
    "margin": "融资融券",
    "chip": "筹码分布",
    "technical": "技术形态",
    "limit": "涨跌停",
    "momentum": "动量",
    "rebound": "反弹",
    "sector": "板块",
    "ma_entry": "均线",
    "fundamental": "基本面",
    "popularity": "人气",
    "hot_money": "游资",
    "institution_hold": "机构持仓",
    "profit_forecast": "盈利预测",
    "performance": "业绩",
    "buyback": "回购",
    "insider_buy": "险资举牌",
}

_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports"
_SELECTION_HISTORY_FILE = _REPORTS_DIR / "selection_history.json"


def is_trading_day(engine=None) -> bool:
    """检查今天是否为 A 股交易日，用于判断是否应保存回测文件。"""
    fetcher = None
    if engine is not None:
        fetcher = getattr(engine, "tushare_fetcher", None) or getattr(engine, "_fetcher", None)
    if fetcher is not None and hasattr(fetcher, "is_trading_day"):
        return fetcher.is_trading_day()
    from datetime import date
    return date.today().weekday() < 5


def _calc_price_levels(prices: Dict[str, float], factor_score: float = 50.0) -> tuple:
    """根据技术面价格数据和因子综合分计算买卖点位。

    Args:
        prices: 可能包含 close, boll_mid, boll_lower 的字典
        factor_score: 因子综合评分 (0-100)，影响止损宽度
    """
    close = prices.get("close", 0)
    boll_mid = prices.get("boll_mid", 0)
    boll_lower = prices.get("boll_lower", 0)

    if boll_lower > 0 and boll_mid > 0:
        if factor_score >= 35:
            # 强信号（top ~3%）：止损更宽，给更多缓冲
            buy_low = round(boll_lower * 0.97, 1)
            buy_high = round(boll_mid * 1.01, 1)
            stop_loss = round(boll_lower * 0.94, 1)
        elif factor_score >= 25:
            buy_low = round(boll_lower * 0.98, 1)
            buy_high = round(boll_mid, 1)
            stop_loss = round(boll_lower * 0.95, 1)
        else:
            buy_low = round(boll_lower * 0.99, 1)
            buy_high = round(boll_mid * 0.99, 1)
            stop_loss = round(boll_lower * 0.96, 1)
    elif close > 0:
        if factor_score >= 35:
            buy_low = round(close * 0.95, 1)
            buy_high = round(close * 1.03, 1)
            stop_loss = round(close * 0.92, 1)
        elif factor_score >= 25:
            buy_low = round(close * 0.97, 1)
            buy_high = round(close * 1.02, 1)
            stop_loss = round(close * 0.94, 1)
        else:
            buy_low = round(close * 0.98, 1)
            buy_high = round(close * 1.01, 1)
            stop_loss = round(close * 0.95, 1)
    else:
        return (None, None, None, None, None)

    take_profit_1 = round(close * 1.05, 1) if close > 0 else None
    take_profit_2 = round(close * 1.10, 1) if close > 0 else None

    return (buy_low, buy_high, stop_loss, take_profit_1, take_profit_2)


class StockDiscoveryEngine:
    """股票自动发现引擎。"""

    def __init__(self, config: DiscoveryConfig, tushare_fetcher=None, akshare_fetcher=None):
        self.config = config
        self.tushare_fetcher = tushare_fetcher
        self.akshare_fetcher = akshare_fetcher
        self._factors: Dict[str, BaseFactor] = {}
        self._stock_names: Dict[str, str] = {}
        self._selection_count: Dict[str, list] = self._load_selection_history()
        # 同 session 因子数据缓存，避免重复拉取
        self._factor_data_cache: Dict[str, Dict[str, pd.DataFrame]] = {}
        self._cache_trade_date: Optional[str] = None

    # ------------------------------------------------------------------
    # Factor management
    # ------------------------------------------------------------------

    def register_factor(self, factor: BaseFactor) -> None:
        if not factor.name:
            raise ValueError(f"Factor {factor!r} must have a non-empty name")
        self._factors[factor.name] = factor
        logger.info(f"[Discovery] 注册因子: {factor.name} (weight={factor.weight})")

    def register_factors(self, factors: List[BaseFactor]) -> None:
        for f in factors:
            self.register_factor(f)

    def unregister_factor(self, name: str) -> None:
        self._factors.pop(name, None)

    def get_factor(self, name: str) -> Optional[BaseFactor]:
        return self._factors.get(name)

    # ------------------------------------------------------------------
    # Selection history (crowding penalty)
    # 格式: {date: [codes]}，保留最近 10 个交易日，按天去重
    # ------------------------------------------------------------------

    def _load_selection_history(self) -> Dict[str, list]:
        if _SELECTION_HISTORY_FILE.exists():
            try:
                raw = json.loads(_SELECTION_HISTORY_FILE.read_text())
            except Exception:
                return {}
            # 迁移旧格式 {code: count} → {date: [codes]}
            if raw and not any(isinstance(v, list) for v in raw.values()):
                logger.info("[Discovery] 迁移旧格式拥挤惩罚数据")
                raw = {"legacy": sorted(raw.keys())}
            return raw
        return {}

    def _save_selection_history(self) -> None:
        if not is_trading_day(self):
            return
        # 只保留最近 10 天
        dates = sorted(self._selection_count.keys(), reverse=True)
        if len(dates) > 10:
            for old in dates[10:]:
                del self._selection_count[old]
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        _SELECTION_HISTORY_FILE.write_text(json.dumps(self._selection_count, ensure_ascii=False))

    def _apply_crowding_penalty(
        self, results: List[DiscoveryResult], trade_date: Optional[str] = None
    ) -> List[DiscoveryResult]:
        """近 5 个交易日被选中天数越多，惩罚越重。同一天内去重。"""
        if not results:
            return results

        today = trade_date or self.tushare_fetcher.get_trade_time(
            early_time="00:00", late_time="18:00"
        ) if self.tushare_fetcher else None
        if not today:
            today = __import__("datetime").date.today().strftime("%Y%m%d")

        # 当天已选中集合（同一天多次扫描不去重累加）
        today_set = set(self._selection_count.get(today, []))
        new_today = {r.ts_code for r in results} - today_set

        # 合并当天
        self._selection_count[today] = sorted(today_set | set(r.ts_code for r in results))

        # 最近 5 个交易日窗口（含今天）
        recent = sorted(self._selection_count.keys(), reverse=True)[:5]
        recent_codes: Dict[str, int] = {}
        for d in recent:
            for c in self._selection_count.get(d, []):
                recent_codes[c] = recent_codes.get(c, 0) + 1

        # 只对今天新出现的票施加惩罚（避免每 60s 重复扣同一批票）
        for r in results:
            days = recent_codes.get(r.ts_code, 0)
            if days >= 5:
                r.score = max(0, r.score - 30)
                r.reasons.append(f"拥挤惩罚(近5日全勤-30分)")
            elif days == 4:
                r.score = max(0, r.score - 20)
                r.reasons.append(f"拥挤惩罚(近5日选中4天-20分)")
            elif days == 3:
                r.score = max(0, r.score - 10)
                r.reasons.append(f"拥挤惩罚(近5日选中3天-10分)")

        self._save_selection_history()
        return results

    # ------------------------------------------------------------------
    # Industry mapping (for neutralization)
    # ------------------------------------------------------------------

    def _get_industry_map(self, ts_codes: List[str]) -> Dict[str, str]:
        """从 Tushare 获取股票行业映射，用于行业中性化。"""
        if self.tushare_fetcher is None:
            return {}
        try:
            df = self.tushare_fetcher.get_stock_list()
            if df is None or df.empty:
                return {}
            industry_col = next((c for c in df.columns if c == "industry"), None)
            code_col = next((c for c in df.columns if c == "code"), None)
            if industry_col is None or code_col is None:
                return {}
            result = {}
            for _, row in df.iterrows():
                code = str(row.get(code_col, "")).strip()
                industry = str(row.get(industry_col, "")).strip()
                if code and industry and industry not in ("", "nan", "None"):
                    result[code] = industry
            return result
        except Exception as e:
            logger.debug(f"[Discovery] 获取行业映射失败: {e}")
            return {}

    # ------------------------------------------------------------------
    # Real-time prices (akshare primary, Sina fallback)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_batch_realtime_prices_akshare(ts_codes: List[str]) -> Dict[str, float]:
        """通过 akshare 获取全 A 股实时价格（单次调用）。"""
        if not ts_codes:
            return {}
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                return {}
            # akshare 返回列：代码, 名称, 最新价, ...
            price_map: Dict[str, float] = {}
            for _, row in df.iterrows():
                code = str(row.get('代码', '')).strip()
                price = row.get('最新价')
                if code and price is not None:
                    try:
                        price_map[code] = float(price)
                    except (ValueError, TypeError):
                        pass
            # map ts_code → price (akshare code has no suffix)
            result: Dict[str, float] = {}
            for ts_code in ts_codes:
                code = ts_code.split(".")[0] if "." in ts_code else ts_code
                if code in price_map:
                    result[ts_code] = price_map[code]
            return result
        except Exception as e:
            logger.debug(f"[Discovery] akshare 实时价格获取失败: {e}")
            return {}


    @staticmethod
    def _to_sina_symbol(ts_code) -> str:
        """将 ts_code 转为新浪行情符号，如 600379.SH → sh600379"""
        code = str(ts_code).split(".")[0]
        if code.startswith(("60", "68")):
            return f"sh{code}"
        return f"sz{code}"

    @staticmethod
    def _get_batch_realtime_prices(ts_codes: List[str]) -> Dict[str, float]:
        """通过新浪批量接口获取实时价格。"""
        if not ts_codes:
            return {}
        symbols = [StockDiscoveryEngine._to_sina_symbol(c) for c in ts_codes]
        url = f"http://hq.sinajs.cn/list={','.join(symbols)}"
        try:
            resp = requests.get(
                url,
                headers={"Referer": "http://finance.sina.com.cn"},
                timeout=10,
            )
            resp.encoding = "gbk"
            prices: Dict[str, float] = {}
            for line in resp.text.strip().split("\n"):
                m = re.search(r'hq_str_(\w+)="([^"]*)"', line)
                if not m:
                    continue
                sym = m.group(1)
                fields = m.group(2).split(",")
                if len(fields) < 4:
                    continue
                try:
                    prices[sym] = float(fields[3])  # fields[3] = 当前价
                except (ValueError, IndexError):
                    pass
            # map back: sina symbol → ts_code
            result: Dict[str, float] = {}
            for i, ts_code in enumerate(ts_codes):
                if i < len(symbols) and symbols[i] in prices:
                    result[ts_code] = prices[symbols[i]]
            return result
        except Exception as e:
            logger.debug(f"[Discovery] 批量实时价格获取失败: {e}")
            return {}

    # ------------------------------------------------------------------
    # Sector labels (concept tags)
    # ------------------------------------------------------------------

    def _get_sector_labels(self, ts_codes: List[str]) -> Dict[str, List[str]]:
        """获取各股票的所属板块标签。

        优先从 SectorFactor 涨停池的 sector_map 读取（akshare stock_zt_pool_em），
        北向持股数据已 geo-blocked，降级到 Tushare industry。
        """
        labels: Dict[str, List[str]] = {}

        # ── 优先：SectorFactor 涨停池 sector_map ──
        try:
            sector_factor = self._factors.get("sector")
            if sector_factor is not None and hasattr(sector_factor, "sector_map"):
                smap = sector_factor.sector_map
                for ts_code in ts_codes:
                    stock_code = ts_code.split(".")[0] if "." in ts_code else ts_code
                    sec = smap.get(stock_code)
                    if sec and sec != "nan":
                        labels[stock_code] = [sec]
                if labels:
                    logger.debug("[Discovery] 从涨停池获取板块标签: %d 只", len(labels))
                    return labels
        except Exception as e:
            logger.debug("[Discovery] 涨停池板块标签获取失败: %s", e)

        # ── 降级: akshare 北向持股（已被 geo-blocked，静默失败）──
        try:
            import akshare as ak

            df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
            if df is not None and not df.empty:
                code_col = next((c for c in df.columns if "代码" in c), None)
                sector_col = next((c for c in df.columns if "所属板块" in c), None)
                if code_col and sector_col:
                    for _, row in df.iterrows():
                        code = str(row.get(code_col, "")).strip()
                        sector = str(row.get(sector_col, "")).strip()
                        if code and sector and sector != "nan":
                            labels[code] = sector.split(",")[:3]
        except Exception:
            pass  # geo-blocked，静默

        return labels

    # ------------------------------------------------------------------
    # Dynamic weight adjustment
    # ------------------------------------------------------------------

    def _calc_dynamic_weights(self, mode: str) -> Dict[str, float]:
        """根据近期市场状态动态调整因子权重。"""
        try:
            if self.tushare_fetcher is None:
                return {}
            # 获取近期市场数据（用上证指数）
            df = self.tushare_fetcher.get_daily_data("000001.SH", start_date="20260101", days=20)
            if df is None or len(df) < 10:
                return {}
            returns = pd.to_numeric(df["pct_chg"], errors="coerce").dropna()
            if len(returns) < 5:
                return {}

            volatility = returns.std()
            trend_strength = abs(returns.mean() / (returns.std() + 1e-9))

            if trend_strength > 0.8:
                # 强趋势市场：增配动量、北向
                logger.info(f"[Discovery] 市场状态: 强趋势 (trend={trend_strength:.2f})")
                return {"momentum": 1.3, "rebound": 0.7, "technical": 1.1}
            elif volatility > 1.5:
                # 高波动震荡：增配反弹、业绩
                logger.info(f"[Discovery] 市场状态: 高波动 (vol={volatility:.2f})")
                return {"rebound": 1.4, "performance": 1.2, "profit_forecast": 1.1, "momentum": 0.6}
            else:
                return {}
        except Exception as e:
            logger.debug(f"[Discovery] 动态权重计算失败: {e}")
            return {}

    # ------------------------------------------------------------------
    # Stock name resolution
    # ------------------------------------------------------------------

    def _resolve_stock_names(self, ts_codes: List[str]) -> Dict[str, str]:
        unresolved = [c for c in ts_codes if c not in self._stock_names]
        if unresolved and self.tushare_fetcher:
            # 批量拉取全量 A 股名称（一次 API 调用），写入 self._stock_names
            try:
                api = getattr(self.tushare_fetcher, '_api', None)
                if api:
                    df = api.stock_basic(exchange='', list_status='L', fields='ts_code,name')
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            ts = row['ts_code']
                            code = ts.split('.')[0] if '.' in ts else ts
                            name = row['name']
                            self._stock_names[ts] = name
                            self._stock_names[code] = name
                        logger.info("[Discovery] 预加载 %d 只股票名称", len(df))
            except Exception as e:
                logger.debug("[Discovery] 批量预加载名称失败: %s", e)
        return {c: self._stock_names.get(c, c) for c in ts_codes}

    # ------------------------------------------------------------------
    # Discovery core
    # ------------------------------------------------------------------

    def _decorrelate_scores(
        self, score_columns: Dict[str, pd.Series]
    ) -> Dict[str, pd.Series]:
        """对高相关因子组做去相关处理，避免资金流信号重复放大。"""
        if len(score_columns) < 2:
            return score_columns

        try:
            df_scores = pd.DataFrame(score_columns)
            corr_matrix = df_scores.corr()

            # 资金流因子组（高度相关）
            money_group = ["money_flow", "hot_money"]
            existing = [f for f in money_group if f in corr_matrix.columns]

            if len(existing) > 1:
                # 计算组内均值作为主成分代理
                sub = df_scores[existing]
                pc = sub.mean(axis=1)

                for f in existing:
                    orig = df_scores[f]
                    # 与均值的相关性
                    corr_with_mean = corr_matrix.loc[f, existing].mean()
                    # 正交化：原分 - PC * corr
                    residual = orig - pc * corr_with_mean
                    score_columns[f] = residual.clip(0, 100).fillna(0)

        except Exception as e:
            logger.debug(f"[Discovery] 去相关处理失败: {e}")

        return score_columns

    def _apply_industry_neutral(
        self, factor_scores: Dict[str, pd.Series], factor_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.Series]:
        """对因子得分做行业中性化（行业内排名百分位）。

        使用 Tushare stock_basic 的 industry 字段，非北向持股数据。
        """
        # 构建全市场行业映射
        all_codes = set()
        for scores in factor_scores.values():
            all_codes.update(scores.index.tolist())
        industry_map = self._get_industry_map(list(all_codes))

        if not industry_map:
            return factor_scores

        neutral_scores = {}
        for name, scores in factor_scores.items():
            neutral = pd.Series(50.0, index=scores.index, name=name)

            # Build sector → position list, avoiding label-based ops on duplicate indices
            sectors_values = [industry_map.get(idx, "未知") for idx in scores.index]
            sector_positions: Dict[str, list] = {}
            for i, sector in enumerate(sectors_values):
                sector_positions.setdefault(sector, []).append(i)

            for sector, positions in sector_positions.items():
                group_scores = scores.iloc[positions]
                if group_scores.std() > 1e-6:
                    normalized = (group_scores - group_scores.mean()) / group_scores.std()
                    neutral.iloc[positions] = ((normalized + 2) / 4 * 100).clip(0, 100)
                else:
                    neutral.iloc[positions] = 50.0

            neutral_scores[name] = neutral

        return neutral_scores

    @staticmethod
    def _calc_factor_data_hash(factor_data: Dict[str, pd.DataFrame]) -> str:
        """对因子数据取指纹，快速判断数据是否变化。"""
        import hashlib
        parts = []
        for name, df in sorted(factor_data.items()):
            if df is not None and not df.empty:
                n = len(df)
                # 用行数 + 首尾 index 作为指纹，快速不耗 CPU
                first_idx = str(df.index[0]) if n > 0 else ""
                last_idx = str(df.index[-1]) if n > 1 else first_idx
                parts.append(f"{name}:{n}:{first_idx}:{last_idx}")
        return hashlib.md5("|".join(parts).encode()).hexdigest()[:12]

    def discover(self, mode: ModeStr, trade_date: Optional[str] = None) -> List[DiscoveryResult]:
        start_time = time.time()

        if trade_date is None and self.tushare_fetcher:
            trade_date = self.tushare_fetcher.get_trade_time(
                early_time="00:00", late_time="18:00"
            )
        if not trade_date:
            logger.warning("[Discovery] 无法解析交易日期，取消发现")
            return []

        available = [
            f for f in self._factors.values() if f.is_available(mode)
        ]
        if not available:
            logger.warning(f"[Discovery] 模式 {mode} 无可用因子")
            return []

        logger.info(
            f"[Discovery] 开始 {mode} 发现 (date={trade_date}, "
            f"factors={[f.name for f in available]})"
        )

        # Phase 1: 拉取因子数据（优先复用 session 缓存）
        # 实时因子（如 sector）每轮重新拉取，不缓存
        _REALTIME_FACTORS = {"sector", "momentum"}

        factor_data: Dict[str, pd.DataFrame] = {}
        if self._factor_data_cache and self._cache_trade_date == trade_date:
            # 复用非实时缓存
            factor_data = {
                k: v for k, v in self._factor_data_cache.items()
                if k not in _REALTIME_FACTORS
            }
            if factor_data:
                logger.info("[Discovery] 因子数据命中 session 缓存（%s），跳过拉取",
                            ", ".join(factor_data.keys()))
            # 实时因子始终重新拉取
            for factor in available:
                if factor.name not in _REALTIME_FACTORS:
                    continue
                try:
                    logger.debug(f"[Discovery] 拉取实时因子数据: {factor.name}")
                    df = factor.fetch_data(
                        trade_date,
                        tushare_fetcher=self.tushare_fetcher,
                        akshare_fetcher=self.akshare_fetcher,
                    )
                    if df is not None and not df.empty:
                        factor_data[factor.name] = df
                        logger.info(f"[Discovery] {factor.name}: 获取 {len(df)} 条数据")
                    else:
                        logger.warning(f"[Discovery] {factor.name}: 无数据")
                except Exception as e:
                    logger.warning(f"[Discovery] 拉取实时因子 {factor.name} 失败: {e}")
        else:
            for factor in available:
                try:
                    logger.debug(f"[Discovery] 拉取因子数据: {factor.name}")
                    df = factor.fetch_data(
                        trade_date,
                        tushare_fetcher=self.tushare_fetcher,
                        akshare_fetcher=self.akshare_fetcher,
                    )
                    if df is not None and not df.empty:
                        factor_data[factor.name] = df
                        logger.info(f"[Discovery] {factor.name}: 获取 {len(df)} 条数据")
                    else:
                        logger.warning(f"[Discovery] {factor.name}: 无数据")
                except Exception as e:
                    logger.warning(f"[Discovery] 拉取因子 {factor.name} 失败: {e}")

            # 更新 session 缓存（排除实时因子）
            if factor_data:
                self._factor_data_cache = {
                    k: v for k, v in factor_data.items()
                    if k not in _REALTIME_FACTORS
                }
                self._cache_trade_date = trade_date

        if not factor_data:
            logger.warning("[Discovery] 所有因子数据为空，取消发现")
            return []

        # Phase 2: 收集所有出现过的 ts_code
        all_codes: set = set()
        for df in factor_data.values():
            all_codes.update(df.index.tolist())
        all_codes.discard(None)

        if not all_codes:
            logger.warning("[Discovery] 无候选股票")
            return []

        # Phase 3: 逐因子打分
        score_columns: Dict[str, pd.Series] = {}
        raw_scores: Dict[str, pd.Series] = {}

        # 动态权重（市场状态自适应）
        dynamic_adjustments = self._calc_dynamic_weights(mode)

        for factor in available:
            if factor.name not in factor_data:
                continue
            try:
                raw = factor.score(
                    factor_data[factor.name],
                    tushare_fetcher=self.tushare_fetcher,
                    trade_date=trade_date,
                )
                if raw is not None and not raw.empty:
                    if raw.index.has_duplicates:
                        raw = raw.groupby(raw.index).mean()
                    raw.index = raw.index.map(str)
                    raw_scores[factor.name] = raw
                    score_columns[factor.name] = raw  # 暂存原始分，标准化后再加权
                    logger.debug(
                        f"[Discovery] {factor.name}: scored {len(raw)} stocks, "
                        f"max={raw.max():.1f}"
                    )
            except Exception as e:
                logger.warning(f"[Discovery] 因子 {factor.name} 打分失败: {e}")

        if not score_columns:
            logger.warning("[Discovery] 无有效评分")
            return []

        # Phase 3.5: 因子去相关（资金流组）
        score_columns = self._decorrelate_scores(score_columns)

        # Phase 3.6: 行业中性化
        score_columns = self._apply_industry_neutral(score_columns, factor_data)

        # Phase 3.7: 横截面百分位标准化 + 加权
        # 在行业中性化之后做：每个因子的行业内排名(0-100) → 全市场百分位(0-1) × 权重。
        # 行业中性化已经在行业内做了标准化，这里再做跨因子的量纲统一。
        total_weight = sum(
            self._factors[n].weight for n in score_columns if n in self._factors
        )
        if total_weight <= 0:
            total_weight = 1.0
        for name in list(score_columns.keys()):
            factor = self._factors.get(name)
            if factor is None or factor.weight <= 0:
                del score_columns[name]
                continue
            col = score_columns[name]
            pct = col.rank(pct=True, na_option="bottom")  # 0-1
            adj = dynamic_adjustments.get(name, 1.0)
            effective_weight = factor.weight * adj / total_weight
            score_columns[name] = pct * 100 * effective_weight
            logger.debug(
                f"[Discovery] {name}: pct-std max={score_columns[name].max():.1f}, "
                f"weight={factor.weight} adj={adj:.2f} eff={effective_weight:.2f}"
            )

        # Phase 4: 合并评分 → 综合评分
        combined = pd.DataFrame(score_columns).fillna(0)
        combined["_total"] = combined.sum(axis=1)  # 权重已归一化，sum 即为加权总分 (0-100)
        combined = combined.sort_values("_total", ascending=False)

        # Phase 4.5: 收集推荐理由
        all_reasons: Dict[str, List[str]] = {}
        for factor in available:
            if factor.name not in factor_data or factor.name not in raw_scores:
                continue
            try:
                desc = factor.describe(
                    factor_data[factor.name],
                    raw_scores[factor.name],
                    tushare_fetcher=self.tushare_fetcher,
                    trade_date=trade_date,
                )
                for ts_code, reasons in desc.items():
                    if ts_code not in all_reasons:
                        all_reasons[ts_code] = []
                    all_reasons[ts_code].extend(reasons)
            except Exception as e:
                logger.debug(f"[Discovery] {factor.name} describe() 失败: {e}")

        # Phase 4.6: 提取价格数据
        price_map: Dict[str, Dict[str, float]] = {}
        tech_df = factor_data.get("technical")
        if tech_df is None:
            tech_df = factor_data.get("ma_entry")
        if tech_df is not None and not tech_df.empty:
            for col in ["close", "boll_mid", "boll_lower"]:
                if col not in tech_df.columns:
                    continue
                for ts_code, val in tech_df[col].items():
                    try:
                        fval = float(val)
                        if fval > 0:
                            price_map.setdefault(ts_code, {})[col] = fval
                    except (ValueError, TypeError):
                        pass

        # Phase 5: 解析名称 → 剔除 ST → 构建结果
        top_n = self.config.auto_discover_count
        if mode == "intraday":
            top_n = self.config.scan_top_n

        # --- 扫描范围过滤 ---
        universe_code_set: Optional[set] = None
        universe = self.config.intraday_scan_universe if mode == "intraday" else self.config.postmarket_scan_universe
        if universe == "whitelist" and self.config.discover_whitelist:
            universe_code_set = self.config.discover_whitelist
        elif universe == "broker_gold":
            from src.services.broker_recommend_service import BrokerRecommendService
            from datetime import datetime as _dt
            month = _dt.now().strftime("%Y%m")
            try:
                service = BrokerRecommendService()
                df = service.get_monthly_recommendations(month)
                if df is not None and not df.empty:
                    universe_code_set = set(
                        ts.split(".")[0] if "." in ts else ts
                        for ts in df["ts_code"].unique()
                    )
            except Exception:
                logger.warning("[Discovery] 获取金股列表失败，回退全市场扫描", exc_info=True)

        candidate_codes = combined.index.tolist()
        # 解析所有候选股票名称
        names = self._resolve_stock_names(candidate_codes)

        # 获取板块标签 & 实时价格
        sector_labels = self._get_sector_labels(candidate_codes)
        industry_map = self._get_industry_map(candidate_codes)  # 行业映射作为 fallback
        live_prices: Dict[str, float] = {}
        if mode in ("intraday", "postmarket"):
            try:
                from src.storage import DatabaseManager
                bare_codes = [c.split(".")[0] if "." in c else c for c in candidate_codes]
                spot_df = DatabaseManager().get_current_prices(bare_codes)
                if not spot_df.empty:
                    for ts_code in candidate_codes:
                        code = ts_code.split(".")[0] if "." in ts_code else ts_code
                        try:
                            val = spot_df.at[code, "price"]
                            if pd.notna(val):
                                live_prices[ts_code] = float(val)
                        except (KeyError, ValueError, TypeError):
                            pass
            except Exception:
                logger.warning("[Discovery] 从 realtime_spot 获取实时价格失败，回退 HTTP", exc_info=True)
                for i in range(0, len(candidate_codes), 20):
                    chunk = candidate_codes[i:i + 20]
                    prices = self._get_batch_realtime_prices(chunk)
                    if prices:
                        live_prices.update(prices)
                if not live_prices:
                    live_prices = self._get_batch_realtime_prices_akshare(candidate_codes)

        # Phase 4.9: 暂存全量评分数据供外部（Scanner/main）落库
        self._last_full_scan_df = combined
        self._last_scan_names = names
        self._last_scan_sectors = sector_labels
        self._last_scan_industry_map = industry_map
        self._last_scan_trade_date = trade_date
        self._last_scan_time = time.strftime("%H:%M:%S")
        self._last_scan_mode = mode

        results = []
        st_skipped = 0
        overbought_skipped = 0
        lowpnl_skipped = 0
        whitelist_skipped = 0
        for ts_code, row in combined.iterrows():
            if len(results) >= top_n:
                break
            stock_code = ts_code.split(".")[0] if "." in ts_code else ts_code
            stock_name = names.get(ts_code) or self._stock_names.get(ts_code) or self._stock_names.get(stock_code) or stock_code

            if universe_code_set and stock_code not in universe_code_set:
                whitelist_skipped += 1
                continue

            if is_st_stock(stock_name):
                st_skipped += 1
                continue

            # 还原原始 0-100 评分（中性化后 row[name] 已是 0-100，无需除权重）
            factor_breakdown = {}
            raw_score = row["_total"]
            for name in row.index:
                if name.startswith("_"):
                    continue
                factor_breakdown[name] = row[name]

            prices = price_map.get(ts_code, {})
            buy_low, buy_high, stop, tp1, tp2 = _calc_price_levels(prices, factor_score=raw_score)

            # 过滤超买股 & 低盈亏比股（盘中用实时价，盘后用收盘价）
            discovery_price = live_prices.get(ts_code) or prices.get("close")
            if discovery_price and tp1 and discovery_price >= tp1:
                overbought_skipped += 1
                continue
            if discovery_price and tp1 and stop:
                if discovery_price <= stop:
                    lowpnl_skipped += 1  # 现价已跌破止损线
                    continue
                pnl_ratio = (tp1 - discovery_price) / (discovery_price - stop)
                if pnl_ratio <= 0:
                    lowpnl_skipped += 1
                    continue

            # 追加板块标签到推荐理由
            reasons = list(all_reasons.get(ts_code, []))
            labels = sector_labels.get(stock_code, [])
            if labels:
                reasons.append(f"所属板块: {', '.join(labels)}")

            # 板块/行业
            sector = labels[0] if labels else industry_map.get(ts_code, "")

            factor_weights = {
                name: self._factors[name].weight
                for name in factor_breakdown
                if name in self._factors
            }

            results.append(
                DiscoveryResult(
                    ts_code=ts_code,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    score=round(raw_score, 1),
                    sector=sector,
                    factor_scores=factor_breakdown,
                    factor_weights=factor_weights,
                    reasons=reasons,
                    buy_price_low=buy_low,
                    buy_price_high=buy_high,
                    stop_loss=stop,
                    take_profit_1=tp1,
                    take_profit_2=tp2,
                    discovered_at=time.strftime("%H:%M:%S"),
                    price_at_discovery=live_prices.get(ts_code) or prices.get("close"),
                )
            )

        if st_skipped > 0:
            logger.info("[Discovery] 已剔除 %d 只 ST 股", st_skipped)
        if overbought_skipped > 0:
            logger.info("[Discovery] 已剔除 %d 只超买股（发现价 >= 止盈目标）", overbought_skipped)
        if lowpnl_skipped > 0:
            logger.info("[Discovery] 已剔除 %d 只低盈亏比股（盈亏比 <= 0）", lowpnl_skipped)
        if whitelist_skipped > 0:
            logger.info("[Discovery] 已剔除 %d 只非白名单股", whitelist_skipped)

        # Phase 5.5: 拥挤度惩罚
        results = self._apply_crowding_penalty(results, trade_date)

        # Phase 5.6: IC 追踪（后台线程，不阻塞）
        try:
            from concurrent.futures import ThreadPoolExecutor
            from src.discovery.ic_tracker import ICTracker
            def _run_ic():
                tracker = ICTracker(eval_days=5)
                ic_results = tracker.evaluate(raw_scores, trade_date)
                if ic_results:
                    logger.info(f"[IC] {trade_date}: " + ", ".join(f"{k}={v:.3f}" for k, v in ic_results.items()))
            ThreadPoolExecutor(max_workers=1).submit(_run_ic)
        except Exception as e:
            logger.debug(f"[IC] IC评估失败: {e}")

        elapsed = time.time() - start_time
        top_info = f"{results[0].stock_name} ({results[0].score:.1f})" if results else "N/A (0)"
        logger.info(
            f"[Discovery] {mode} 发现完成: {len(results)} 只, "
            f"top={top_info}, "
            f"耗时 {elapsed:.1f}s"
        )

        return results

    def get_last_full_scan_records(self, scan_round: int = 0) -> List[Dict[str, Any]]:
        """返回最近一次 discover() 的全市场评分记录，供落库。

        Args:
            scan_round: 盘中轮次号（盘后恒为 0）

        Returns:
            list of dicts: scan_date, scan_round, scan_time, ts_code,
            stock_code, stock_name, rank, total_score, factor_scores, sector
        """
        df = getattr(self, '_last_full_scan_df', None)
        if df is None or df.empty:
            return []

        names = getattr(self, '_last_scan_names', {})
        sectors = getattr(self, '_last_scan_sectors', {})
        industry_map = getattr(self, '_last_scan_industry_map', {})
        trade_date = getattr(self, '_last_scan_trade_date', '')
        scan_time = getattr(self, '_last_scan_time', '')
        mode = getattr(self, '_last_scan_mode', '')

        factor_cols = [c for c in df.columns if not c.startswith('_')]
        records: List[Dict[str, Any]] = []

        for rank, (ts_code, row) in enumerate(df.iterrows(), start=1):
            ts_code = str(ts_code)
            stock_code = ts_code.split(".")[0] if "." in ts_code else ts_code
            stock_name = (
                names.get(ts_code)
                or self._stock_names.get(ts_code)
                or self._stock_names.get(stock_code)
                or stock_code
            )

            labels = sectors.get(stock_code, [])
            if labels:
                sector = labels[0]
            else:
                sector = industry_map.get(ts_code, "")

            factor_scores: Dict[str, float] = {}
            for col in factor_cols:
                val = row.get(col)
                if val is not None and not pd.isna(val):
                    factor_scores[col] = round(float(val), 2)

            records.append({
                "scan_date": trade_date,
                "scan_round": scan_round if mode == "intraday" else 0,
                "scan_time": scan_time,
                "ts_code": ts_code,
                "stock_code": stock_code,
                "stock_name": stock_name,
                "rank": rank,
                "total_score": float(row.get("_total", 0)),
                "factor_scores": factor_scores,
                "sector": sector,
            })

        return records

    # ------------------------------------------------------------------
    # Report formatting
    # ------------------------------------------------------------------

    def format_report(self, results: List[DiscoveryResult], mode: ModeStr = "postmarket") -> str:
        if not results:
            mode_label = "盘中扫描" if mode == "intraday" else "盘后发现"
            return f"## {mode_label}\n\n暂无推荐。\n"

        mode_label = "盘中扫描" if mode == "intraday" else "盘后发现"
        lines = [f"## {mode_label} Top {len(results)}", ""]

        for i, r in enumerate(results, 1):
            sector_tag = f" · {r.sector}" if r.sector else ""
            lines.append(f"### #{i} {r.stock_code} {r.stock_name}{sector_tag} — 综合评分 {r.score:.1f}")
            if r.discovered_at:
                price_str = f"¥{r.price_at_discovery:.2f}" if r.price_at_discovery else "-"
                lines.append(f"*发现 {r.discovered_at} · {price_str}*")
            lines.append("")

            if r.reasons:
                lines.append("**推荐理由：**")
                for reason in r.reasons:
                    lines.append(f"- {reason}")
                lines.append("")

            has_prices = any([
                r.buy_price_low, r.buy_price_high,
                r.take_profit_1, r.take_profit_2, r.stop_loss,
            ])
            if has_prices:
                lines.append("| 买入区间 | 止盈1 | 止盈2 | 止损 |")
                lines.append("|---------|-------|-------|------|")

                def _fmt(v):
                    if v is None:
                        return "-"
                    return f"{v:.1f}"

                buy_range = "-"
                if r.buy_price_low and r.buy_price_high:
                    buy_range = f"{_fmt(r.buy_price_low)}-{_fmt(r.buy_price_high)}"
                elif r.buy_price_low:
                    buy_range = _fmt(r.buy_price_low)
                elif r.buy_price_high:
                    buy_range = _fmt(r.buy_price_high)

                lines.append(
                    f"| {buy_range} | {_fmt(r.take_profit_1)} | "
                    f"{_fmt(r.take_profit_2)} | {_fmt(r.stop_loss)} |"
                )
                lines.append("")

            if r.factor_scores:
                factor_parts = []
                for name, score in r.factor_scores.items():
                    zh = _FACTOR_DISPLAY.get(name, "")
                    label = f"{name}（{zh}）" if zh else name
                    factor_parts.append(f"{label}:{score:.0f}")
                lines.append(f"*因子得分：{' | '.join(factor_parts)}*")
                lines.append("")

            lines.append("---")
            lines.append("")

        lines.append(f"*共 {len(results)} 只候选*")
        return "\n".join(lines)