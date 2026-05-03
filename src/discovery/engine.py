# -*- coding: utf-8 -*-
"""股票发现主引擎。

协调因子注册、数据获取、加权评分、去重排序，输出发现结果。
"""

import json
import logging
import time
from pathlib import Path
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
    "fundamental": "基本面",
    "popularity": "人气",
    "hot_money": "游资",
    "northbound": "北向资金",
    "institution_hold": "机构持仓",
    "profit_forecast": "盈利预测",
    "performance": "业绩",
    "buyback": "回购",
    "insider_buy": "险资举牌",
}

_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports"
_SELECTION_HISTORY_FILE = _REPORTS_DIR / "selection_history.json"


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
        if factor_score >= 70:
            # 强信号：止损更宽（给更多缓冲）
            buy_low = round(boll_lower * 0.97, 1)
            buy_high = round(boll_mid * 1.01, 1)
            stop_loss = round(boll_lower * 0.94, 1)
        elif factor_score >= 50:
            buy_low = round(boll_lower * 0.98, 1)
            buy_high = round(boll_mid, 1)
            stop_loss = round(boll_lower * 0.95, 1)
        else:
            buy_low = round(boll_lower * 0.99, 1)
            buy_high = round(boll_mid * 0.99, 1)
            stop_loss = round(boll_lower * 0.96, 1)
    elif close > 0:
        if factor_score >= 70:
            buy_low = round(close * 0.95, 1)
            buy_high = round(close * 1.03, 1)
            stop_loss = round(close * 0.92, 1)
        elif factor_score >= 50:
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
        self._selection_count: Dict[str, int] = self._load_selection_history()
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
    # ------------------------------------------------------------------

    def _load_selection_history(self) -> Dict[str, int]:
        if _SELECTION_HISTORY_FILE.exists():
            try:
                return json.loads(_SELECTION_HISTORY_FILE.read_text())
            except Exception:
                pass
        return {}

    def _save_selection_history(self) -> None:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        _SELECTION_HISTORY_FILE.write_text(json.dumps(self._selection_count, ensure_ascii=False))

    def _apply_crowding_penalty(self, results: List[DiscoveryResult]) -> List[DiscoveryResult]:
        """对频繁上榜的股票施加负向惩罚，防止策略拥挤。"""
        for r in results:
            cnt = self._selection_count.get(r.ts_code, 0)
            if cnt >= 3:
                penalty = min(cnt * 3, 30)
                r.score = max(0, r.score - penalty)
                r.reasons.append(f"拥挤惩罚(-{penalty}分)")
        # 更新历史计数
        for r in results:
            self._selection_count[r.ts_code] = self._selection_count.get(r.ts_code, 0) + 1
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
    # Sector labels (concept tags)
    # ------------------------------------------------------------------

    def _get_sector_labels(self, ts_codes: List[str]) -> Dict[str, List[str]]:
        """从北向持股数据中获取各股票的所属板块标签。"""
        try:
            import akshare as ak

            df = ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")
            if df is None or df.empty:
                return {}

            labels: Dict[str, List[str]] = {}
            code_col = next((c for c in df.columns if "代码" in c), None)
            sector_col = next((c for c in df.columns if "所属板块" in c), None)

            if code_col is None or sector_col is None:
                return {}

            for _, row in df.iterrows():
                code = str(row.get(code_col, "")).strip()
                sector = str(row.get(sector_col, "")).strip()
                if code and sector and sector != "nan":
                    labels[code] = sector.split(",")[:3]  # 最多3个板块
            return labels
        except Exception as e:
            logger.debug(f"[Discovery] 获取板块标签失败: {e}")
            return {}

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
                return {"momentum": 1.3, "northbound": 1.2, "rebound": 0.7, "technical": 1.1}
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
            money_group = ["money_flow", "hot_money", "northbound"]
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

            sectors = {idx: industry_map.get(idx, "未知") for idx in scores.index}
            series_with_sector = pd.Series(list(sectors.values()), index=scores.index)

            for sector, group_idx in series_with_sector.groupby(series_with_sector, dropna=False).items():
                group_scores = scores.reindex(group_idx)
                if group_scores.std() > 1e-6:
                    normalized = (group_scores - group_scores.mean()) / group_scores.std()
                    neutral.loc[group_idx] = ((normalized + 2) / 4 * 100).clip(0, 100)
                else:
                    neutral.loc[group_idx] = 50.0

            neutral_scores[name] = neutral.reindex(scores.index, fill_value=50.0)

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
        factor_data: Dict[str, pd.DataFrame] = {}
        if self._factor_data_cache and self._cache_trade_date == trade_date:
            logger.info("[Discovery] 因子数据命中 session 缓存，跳过拉取")
            factor_data = self._factor_data_cache
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

            # 更新 session 缓存
            if factor_data:
                self._factor_data_cache = factor_data
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

        # 动态归一化
        total_weight = sum(f.weight for f in available)
        if total_weight <= 0:
            total_weight = 1.0
        weight_scale = 100.0 / total_weight

        for factor in available:
            if factor.name not in factor_data:
                continue
            try:
                raw = factor.score(
                    factor_data[factor.name],
                    tushare_fetcher=self.tushare_fetcher,
                )
                if raw is not None and not raw.empty:
                    raw_scores[factor.name] = raw
                    base_weight = factor.weight
                    # 应用动态调整系数
                    adj = dynamic_adjustments.get(factor.name, 1.0)
                    effective_weight = base_weight * adj * weight_scale / 100.0
                    weighted = raw * effective_weight
                    score_columns[factor.name] = weighted
                    logger.debug(
                        f"[Discovery] {factor.name}: scored {len(raw)} stocks, "
                        f"weight={base_weight}*adj={adj:.2f}->eff={effective_weight*100:.1f}%, "
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

        # Phase 4: 合并评分 → 综合评分
        combined = pd.DataFrame(score_columns).fillna(0)
        combined["_total"] = combined.sum(axis=1)
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

        candidate_codes = combined.head(max(top_n * 3, 30)).index.tolist()
        names = self._resolve_stock_names(candidate_codes)

        # 获取板块标签
        sector_labels = self._get_sector_labels(candidate_codes)

        results = []
        st_skipped = 0
        for ts_code, row in combined.iterrows():
            if len(results) >= top_n:
                break
            stock_code = ts_code.split(".")[0] if "." in ts_code else ts_code
            stock_name = names.get(ts_code, stock_code)

            if is_st_stock(stock_name):
                st_skipped += 1
                continue

            # 还原原始 0-100 评分
            factor_breakdown = {}
            raw_score = row["_total"]
            for name, f in self._factors.items():
                if name not in row.index:
                    continue
                adj = dynamic_adjustments.get(name, 1.0)
                eff_w = f.weight * adj * weight_scale / 100.0
                factor_breakdown[name] = row[name] / eff_w if eff_w > 0 else 0.0

            prices = price_map.get(ts_code, {})
            buy_low, buy_high, stop, tp1, tp2 = _calc_price_levels(prices, factor_score=raw_score)

            # 追加板块标签到推荐理由
            reasons = list(all_reasons.get(ts_code, []))
            labels = sector_labels.get(stock_code, [])
            if labels:
                reasons.append(f"所属板块: {', '.join(labels)}")

            results.append(
                DiscoveryResult(
                    ts_code=ts_code,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    score=round(raw_score, 1),
                    factor_scores=factor_breakdown,
                    reasons=reasons,
                    buy_price_low=buy_low,
                    buy_price_high=buy_high,
                    stop_loss=stop,
                    take_profit_1=tp1,
                    take_profit_2=tp2,
                )
            )

        if st_skipped > 0:
            logger.info("[Discovery] 已剔除 %d 只 ST 股", st_skipped)

        # Phase 5.5: 拥挤度惩罚
        results = self._apply_crowding_penalty(results)

        # Phase 5.6: IC 追踪（异步，不阻断）
        try:
            from src.discovery.ic_tracker import ICTracker
            tracker = ICTracker(eval_days=5)
            ic_results = tracker.evaluate(raw_scores, trade_date)
            if ic_results:
                logger.info(f"[IC] {trade_date}: " + ", ".join(f"{k}={v:.3f}" for k, v in ic_results.items()))
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
            lines.append(f"### #{i} {r.stock_code} {r.stock_name} — 综合评分 {r.score:.1f}")
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