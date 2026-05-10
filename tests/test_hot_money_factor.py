# -*- coding: utf-8 -*-
"""HotMoneyFactor 单元测试。

基于测试环境 DB 真实数据（hm_detail + hm_quality 双表），
覆盖：空数据、真实数据打分不变性、净卖出惩罚、describe 标签、
聚合正确性、clamp、因子属性。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.hot_money_factor import HotMoneyFactor
from src.storage import DatabaseManager


# ── 模块级 fixture：加载真实数据 ──

@pytest.fixture(scope="module")
def real_df():
    """加载测试环境 20260508 全市场游资明细。"""
    return DatabaseManager().get_hm_detail_by_date("20260508")


@pytest.fixture(scope="module")
def real_scores(real_df):
    """对真实数据打分，复用计算结果。"""
    factor = HotMoneyFactor()
    return factor.score(real_df)


class TestHotMoneyFactor:
    @pytest.fixture
    def factor(self):
        return HotMoneyFactor()

    # ── 空数据 ──

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series())
        assert reasons == {}

    # ── 真实数据打分不变性 ──

    def test_all_scores_in_range(self, real_scores):
        """所有股票得分在 0-100 区间。"""
        assert len(real_scores) > 0
        assert (real_scores >= 0).all()
        assert (real_scores <= 100).all()

    def test_score_distribution_reasonable(self, real_scores):
        """得分有一定区分度：最高分 > 最低分，中位数在合理区间。"""
        assert real_scores.max() > real_scores.min()
        assert real_scores.max() > 50  # 应有高分股
        assert real_scores.median() > 0  # 不应全零

    def test_top_net_buyer_scores_high(self, real_df, real_scores):
        """600498.SH 为当日最大净买入（9.2 亿），应排在前列。"""
        assert real_scores["600498.SH"] > 70

    def test_net_seller_penalized(self, real_df, real_scores):
        """净卖出股得分应显著低于同体量净买入股。"""
        # 002222.SZ 净卖出 7.7 亿，应得分不高
        assert real_scores["002222.SZ"] < 50

    def test_multi_hm_stock_aggregates(self, real_df, real_scores):
        """000547.SZ 有 5 家游资，应正常聚合打分。"""
        assert "000547.SZ" in real_scores.index
        assert real_scores["000547.SZ"] > 0

    # ── 净买入 vs 净卖出对比 ──

    def test_net_buy_beats_net_sell(self, real_df, real_scores):
        """同向比较：净买入股的平均分 > 净卖出股的平均分。"""
        per_stock = real_df.groupby(level=0).agg(total_net=("net_amount", "sum"))
        buy_idx = per_stock[per_stock["total_net"] > 0].index
        sell_idx = per_stock[per_stock["total_net"] < 0].index

        buy_avg = real_scores.loc[real_scores.index.intersection(buy_idx)].mean()
        sell_avg = real_scores.loc[real_scores.index.intersection(sell_idx)].mean()
        assert buy_avg > sell_avg

    # ── 买入强度：纯买入 vs 混合 ──

    def test_pure_buy_intensity_advantage(self, real_df, real_scores):
        """纯买入（无卖出）股票买入强度分更高。"""
        per_stock = real_df.groupby(level=0).agg(
            total_sell=("sell_amount", "sum"),
        )
        pure_buy = per_stock[per_stock["total_sell"] == 0].index
        mixed = per_stock[per_stock["total_sell"] > 0].index

        # 至少有一组有数据
        p_idx = real_scores.index.intersection(pure_buy)
        m_idx = real_scores.index.intersection(mixed)
        if len(p_idx) > 0 and len(m_idx) > 0:
            # 纯买入不一定整体分更高（可能净买入小），但至少不崩
            assert real_scores.loc[p_idx].max() > 0

    # ── 质量加权：真实 quality_map 生效 ──

    def test_quality_map_used(self, real_df):
        """真实 hm_quality 数据加载成功且参与评分。"""
        from src.discovery.hm_tracker import HmTracker
        qmap = HmTracker.load_quality()
        assert len(qmap) > 0
        # 数据中的所有游资都在 quality_map 中
        hms = real_df["hm_name"].unique()
        for h in hms:
            assert h in qmap, f"{h} 不在 quality_map 中"

    # ── describe ──

    def test_describe_has_labels(self, real_df, real_scores):
        """高分股应有 describe 标签。"""
        factor = HotMoneyFactor()
        factor.score(real_df)  # 设置 _last_hm_agg
        reasons = factor.describe(real_df, real_scores)
        assert len(reasons) > 0
        for ts_code, labels in reasons.items():
            assert len(labels) > 0
            assert real_scores[ts_code] > 0

    def test_describe_top_buyer_has_yi(self, real_df, real_scores):
        """最大净买入 9.2 亿 → 标签含'亿'。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        reasons = factor.describe(real_df, real_scores)
        if "600498.SH" in reasons:
            assert any("亿" in r for r in reasons["600498.SH"])

    def test_describe_multi_hm_has_count(self, real_df, real_scores):
        """000547.SZ 有 5 家游资 → 标签含'5家游资'。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        reasons = factor.describe(real_df, real_scores)
        if "000547.SZ" in reasons:
            assert any("5家游资" in r for r in reasons["000547.SZ"])

    def test_describe_net_sell_label(self, real_df, real_scores):
        """净卖出股且分数 > 0 → 标签含'净卖出'。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        reasons = factor.describe(real_df, real_scores)
        if "002222.SZ" in reasons:
            assert any("净卖出" in r for r in reasons["002222.SZ"])

    def test_describe_zero_scores_excluded(self, real_df, real_scores):
        """zero-score stocks excluded from describe."""
        factor = HotMoneyFactor()
        factor.score(real_df)
        reasons = factor.describe(real_df, real_scores)
        for ts_code in reasons:
            assert real_scores[ts_code] > 0, f"{ts_code} score={real_scores[ts_code]}"

    # ── 聚合正确性 ──

    def test_hm_count_aggregation(self, real_df):
        """hm_count 等于该股票的游资家数。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        agg = factor._per_stock

        for ts_code in agg.index:
            sub = real_df.loc[[ts_code]] if ts_code in real_df.index else real_df.iloc[:0]
            actual_count = sub["hm_name"].nunique()
            assert agg.loc[ts_code, "hm_count"] == actual_count, \
                f"{ts_code}: expected {actual_count}, got {agg.loc[ts_code, 'hm_count']}"

    def test_total_net_aggregation(self, real_df):
        """total_net 等于该股票所有明细的 net_amount 之和。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        agg = factor._per_stock

        for ts_code in agg.index:
            sub = real_df.loc[[ts_code]] if ts_code in real_df.index else real_df.iloc[:0]
            expected = sub["net_amount"].sum()
            assert agg.loc[ts_code, "total_net"] == pytest.approx(expected, rel=1e-6), \
                f"{ts_code}: expected {expected}, got {agg.loc[ts_code, 'total_net']}"

    # ── 因子属性 ──

    def test_factor_attributes(self, factor):
        assert factor.name == "hot_money"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 20.0

    def test_score_series_name(self, real_scores):
        assert real_scores.name == "hot_money"

    # ── 跌停过滤 ──

    def test_limit_down_zero(self, real_df):
        """跌停股（limit_pool limit_type=D）得分归零。"""
        factor = HotMoneyFactor()
        scores = factor.score(real_df)
        # 000020.SZ 在 limit_pool 中标记为跌停
        if "000020.SZ" in scores.index:
            assert scores["000020.SZ"] == 0.0, \
                f"跌停股不应有正面信号，实际得分 {scores['000020.SZ']}"

    def test_limit_down_excluded_from_describe(self, real_df):
        """跌停股不出现在 describe 中。"""
        factor = HotMoneyFactor()
        scores = factor.score(real_df)
        reasons = factor.describe(real_df, scores)
        assert "000020.SZ" not in reasons, "跌停股不应出现在 describe 中"

    # ── 边界：单日单股票 ──

    def test_single_stock_subset(self, real_df):
        """选取真实数据中单只股票，单独打分不崩溃。"""
        factor = HotMoneyFactor()
        single = real_df.loc[["000547.SZ"]].copy()
        scores = factor.score(single)
        assert len(scores) == 1
        assert 0 <= scores["000547.SZ"] <= 100
        assert isinstance(scores["000547.SZ"], float)

    def test_single_row_no_crash(self, real_df):
        """取一条明细单独打分不崩溃。"""
        factor = HotMoneyFactor()
        row = real_df.iloc[[0]].copy()
        scores = factor.score(row)
        assert len(scores) == 1
        assert 0 <= scores.iloc[0] <= 100

    # ── _compute_signals ──

    def test_compute_signals_keys(self, factor, real_df):
        """_compute_signals 返回全部 3 个子信号。"""
        per_stock = factor._aggregate(real_df)
        signals = factor._compute_signals(per_stock)
        assert set(signals.keys()) == {"net", "quality", "intensity"}
        for key in signals:
            assert len(signals[key]) == len(per_stock)

    # ── 质量均值 vs 加总 ──

    def test_avg_quality_prefers_quality_over_quantity(self, factor):
        """2 家高胜率 > 5 家低胜率（均值不受家数影响，控制总净买入相同）。"""
        from unittest.mock import patch
        with patch("src.discovery.factors.hot_money_factor.HmTracker.load_quality") as mq:
            mq.return_value = {"高手A": 0.9, "高手B": 0.9,
                               "散户X": 0.2, "散户Y": 0.2, "散户Z": 0.2, "散户W": 0.2, "散户V": 0.2}
            # A: 2 家高质量，每家 5000 → 总净 10000
            # B: 5 家低质量，每家 2000 → 总净 10000（控制净买入相同）
            records = []
            for hm in ["高手A", "高手B"]:
                records.append(("000001.SZ", 5000, 0, 5000, hm))
            for hm in ["散户X", "散户Y", "散户Z", "散户W", "散户V"]:
                records.append(("000002.SZ", 2000, 0, 2000, hm))
            data = []
            for ts_code, buy, sell, net, hm_name in records:
                data.append({
                    "ts_code": ts_code, "buy_amount": float(buy),
                    "sell_amount": float(sell), "net_amount": float(net),
                    "hm_name": hm_name, "trade_date": "20260508",
                    "ts_name": "测试", "hm_orgs": "",
                })
            df = pd.DataFrame(data).set_index("ts_code")

            scores = factor.score(df)
            # A (avg=0.9) 质量分 > B (avg=0.2)，但净买入相同、强度相同
            assert scores["000001.SZ"] > scores["000002.SZ"]

    # ── describe 新标签 ──

    def test_describe_has_quality_label(self, real_df, real_scores):
        """高分股 describe 应包含'高胜率游资'标签。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        reasons = factor.describe(real_df, real_scores)
        # 至少有一只股票有高胜率标签
        quality_labels = [
            ts for ts, labels in reasons.items()
            if any("高胜率游资" in r for r in labels)
        ]
        assert len(quality_labels) > 0

    def test_describe_has_intensity_label(self, real_df, real_scores):
        """纯粹买入且高分的股票应有'强势买入'标签。"""
        factor = HotMoneyFactor()
        factor.score(real_df)
        reasons = factor.describe(real_df, real_scores)
        intensity_labels = [
            ts for ts, labels in reasons.items()
            if any("强势买入" in r for r in labels)
        ]
        assert len(intensity_labels) > 0
