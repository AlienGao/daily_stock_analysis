# -*- coding: utf-8 -*-
"""股票发现主引擎。

协调因子注册、数据获取、加权评分、去重排序，输出发现结果。
"""

import logging
import time
from typing import Dict, List, Literal, Optional

import pandas as pd

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
}


def _calc_price_levels(prices: Dict[str, float]) -> tuple:
    """根据技术面价格数据计算买卖点位。

    Args:
        prices: 可能包含 close, boll_mid, boll_lower 的字典

    Returns:
        (buy_low, buy_high, stop_loss, take_profit_1, take_profit_2)
    """
    close = prices.get("close", 0)
    boll_mid = prices.get("boll_mid", 0)
    boll_lower = prices.get("boll_lower", 0)

    if boll_lower > 0 and boll_mid > 0:
        buy_low = round(boll_lower, 1)
        buy_high = round(boll_mid, 1)
        stop_loss = round(boll_lower * 0.97, 1)
    elif close > 0:
        buy_low = round(close * 0.97, 1)
        buy_high = round(close * 1.02, 1)
        stop_loss = round(close * 0.95, 1)
    else:
        return (None, None, None, None, None)

    take_profit_1 = round(close * 1.05, 1) if close > 0 else None
    take_profit_2 = round(close * 1.10, 1) if close > 0 else None

    return (buy_low, buy_high, stop_loss, take_profit_1, take_profit_2)


class StockDiscoveryEngine:
    """股票自动发现引擎。

    使用方法:
        config = DiscoveryConfig()
        engine = StockDiscoveryEngine(config, tushare_fetcher)
        engine.register_factors([...])
        results = engine.discover(mode="postmarket")
        for r in results:
            print(f"{r.stock_code} {r.stock_name}: {r.score:.1f}")
    """

    def __init__(self, config: DiscoveryConfig, tushare_fetcher=None, akshare_fetcher=None):
        self.config = config
        self.tushare_fetcher = tushare_fetcher
        self.akshare_fetcher = akshare_fetcher
        self._factors: Dict[str, BaseFactor] = {}
        self._stock_names: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Factor management
    # ------------------------------------------------------------------

    def register_factor(self, factor: BaseFactor) -> None:
        """注册一个因子。"""
        if not factor.name:
            raise ValueError(f"Factor {factor!r} must have a non-empty name")
        self._factors[factor.name] = factor
        logger.info(f"[Discovery] 注册因子: {factor.name} (weight={factor.weight})")

    def register_factors(self, factors: List[BaseFactor]) -> None:
        """批量注册因子。"""
        for f in factors:
            self.register_factor(f)

    def unregister_factor(self, name: str) -> None:
        """移除一个因子。"""
        self._factors.pop(name, None)

    def get_factor(self, name: str) -> Optional[BaseFactor]:
        """按名称获取因子。"""
        return self._factors.get(name)

    # ------------------------------------------------------------------
    # Stock name resolution
    # ------------------------------------------------------------------

    def _resolve_stock_names(self, ts_codes: List[str]) -> Dict[str, str]:
        """批量解析 ts_code → 股票名称。优先用缓存。"""
        unresolved = [c for c in ts_codes if c not in self._stock_names]
        if unresolved and self.tushare_fetcher:
            for ts_code in unresolved:
                try:
                    clean_code = ts_code.split(".")[0] if "." in ts_code else ts_code
                    name = self.tushare_fetcher.get_stock_name(clean_code)
                    if name:
                        self._stock_names[ts_code] = name
                except Exception:
                    pass
        return {c: self._stock_names.get(c, c) for c in ts_codes}

    # ------------------------------------------------------------------
    # Discovery core
    # ------------------------------------------------------------------

    def discover(self, mode: ModeStr, trade_date: Optional[str] = None) -> List[DiscoveryResult]:
        """执行发现。

        Args:
            mode: "intraday" (盘中) 或 "postmarket" (盘后)
            trade_date: 交易日期 (YYYYMMDD)，None=自动解析

        Returns:
            按综合评分降序排列的 DiscoveryResult 列表
        """
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

        # Phase 1: 并行拉取各因子数据
        factor_data: Dict[str, pd.DataFrame] = {}
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
                    logger.info(
                        f"[Discovery] {factor.name}: 获取 {len(df)} 条数据"
                    )
                else:
                    logger.warning(f"[Discovery] {factor.name}: 无数据")
            except Exception as e:
                logger.warning(f"[Discovery] 拉取因子 {factor.name} 失败: {e}")

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

        # Phase 3: 逐因子打分 → 加权求和（动态归一化权重，适应新增因子）
        score_columns: Dict[str, pd.Series] = {}
        raw_scores: Dict[str, pd.Series] = {}  # 保留原始评分供 describe() 使用

        # 动态归一化：可用因子权重之和归一化到 100%
        total_weight = sum(f.weight for f in available)
        if total_weight <= 0:
            total_weight = 1.0
        weight_scale = 100.0 / total_weight

        for factor in available:
            if factor.name not in factor_data:
                continue
            try:
                raw = factor.score(factor_data[factor.name], tushare_fetcher=self.tushare_fetcher)
                if raw is not None and not raw.empty:
                    raw_scores[factor.name] = raw
                    effective_weight = factor.weight * weight_scale / 100.0
                    weighted = raw * effective_weight
                    score_columns[factor.name] = weighted
                    logger.debug(
                        f"[Discovery] {factor.name}: scored {len(raw)} stocks, "
                        f"weight={factor.weight}->eff={effective_weight*100:.1f}%, "
                        f"max={raw.max():.1f}"
                    )
            except Exception as e:
                logger.warning(f"[Discovery] 因子 {factor.name} 打分失败: {e}")

        if not score_columns:
            logger.warning("[Discovery] 无有效评分")
            return []

        # Phase 4: 合并评分 → 综合评分
        combined = pd.DataFrame(score_columns).fillna(0)
        combined["_total"] = combined.sum(axis=1)
        combined = combined.sort_values("_total", ascending=False)

        # Phase 4.5: 收集推荐理由（逐因子 describe）
        all_reasons: Dict[str, List[str]] = {}
        for factor in available:
            if factor.name not in factor_data or factor.name not in raw_scores:
                continue
            try:
                desc = factor.describe(
                    factor_data[factor.name],
                    raw_scores[factor.name],
                    tushare_fetcher=self.tushare_fetcher,
                )
                for ts_code, reasons in desc.items():
                    if ts_code not in all_reasons:
                        all_reasons[ts_code] = []
                    all_reasons[ts_code].extend(reasons)
            except Exception as e:
                logger.debug(f"[Discovery] {factor.name} describe() 失败: {e}")

        # Phase 4.6: 提取价格数据（从技术面因子）
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

        # 取更多候选以应对 ST 剔除
        candidate_codes = combined.head(max(top_n * 3, 30)).index.tolist()
        names = self._resolve_stock_names(candidate_codes)

        results = []
        st_skipped = 0
        for ts_code, row in combined.iterrows():
            if len(results) >= top_n:
                break
            stock_code = ts_code.split(".")[0] if "." in ts_code else ts_code
            stock_name = names.get(ts_code, stock_code)

            # 剔除 ST 股
            if is_st_stock(stock_name):
                st_skipped += 1
                continue

            # 还原原始 0-100 评分（行值是 raw * effective_weight）
            factor_breakdown = {}
            for name, f in self._factors.items():
                if name not in row.index:
                    continue
                eff_w = f.weight * weight_scale / 100.0  # effective weight used in scoring
                factor_breakdown[name] = row[name] / eff_w if eff_w > 0 else 0.0

            # 计算买卖点位
            prices = price_map.get(ts_code, {})
            buy_low, buy_high, stop, tp1, tp2 = _calc_price_levels(prices)

            results.append(
                DiscoveryResult(
                    ts_code=ts_code,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    score=round(row["_total"], 1),
                    factor_scores=factor_breakdown,
                    reasons=all_reasons.get(ts_code, []),
                    buy_price_low=buy_low,
                    buy_price_high=buy_high,
                    stop_loss=stop,
                    take_profit_1=tp1,
                    take_profit_2=tp2,
                )
            )

        if st_skipped > 0:
            logger.info("[Discovery] 已剔除 %d 只 ST 股", st_skipped)

        elapsed = time.time() - start_time
        top_info = f"{results[0].stock_name} ({results[0].score:.1f})" if results else "N/A (0)"
        logger.info(
            f"[Discovery] {mode} 发现完成: {len(results)} 只, "
            f"top={top_info}, "
            f"耗时 {elapsed:.1f}s"
        )

        return results

    # ------------------------------------------------------------------
    # Report formatting
    # ------------------------------------------------------------------

    def format_report(self, results: List[DiscoveryResult], mode: ModeStr = "postmarket") -> str:
        """将发现结果格式化为 Markdown 报告，含推荐理由和买卖点位。"""
        if not results:
            mode_label = "盘中扫描" if mode == "intraday" else "盘后发现"
            return f"## {mode_label}\n\n暂无推荐。\n"

        mode_label = "盘中扫描" if mode == "intraday" else "盘后发现"
        lines = [f"## {mode_label} Top {len(results)}", ""]

        for i, r in enumerate(results, 1):
            # 标题行：排名 + 代码 + 名称 + 评分
            lines.append(f"### #{i} {r.stock_code} {r.stock_name} — 综合评分 {r.score:.1f}")
            lines.append("")

            # 推荐理由
            if r.reasons:
                lines.append("**推荐理由：**")
                for reason in r.reasons:
                    lines.append(f"- {reason}")
                lines.append("")

            # 买卖点位
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

            # 各因子得分
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
