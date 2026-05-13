# -*- coding: utf-8 -*-
"""ProfitForecastFactor 单元测试.

覆盖：因子属性、空数据、覆盖度(0-30)、评级质量(0-40)、
EPS增长(0-30)、综合打分、describe 标签、真实数据集成。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.profit_forecast_factor import ProfitForecastFactor

# akshare 返回的列名（中文）
COL_RPT = "研报数"
COL_BUY = "机构投资评级(近六个月)-买入"
COL_ADD = "机构投资评级(近六个月)-增持"
COL_NEU = "机构投资评级(近六个月)-中性"
COL_RED = "机构投资评级(近六个月)-减持"
COL_EPS25 = "2025预测每股收益"
COL_EPS26 = "2026预测每股收益"


def _make_df(codes, reports, buys, adds, neutrals=None, reduces=None,
             eps25s=None, eps26s=None):
    """构造盈利预测 DataFrame，index=ts_code，列名为 akshare 中文列名."""
    n = len(codes)
    data = {
        COL_RPT: reports,
        COL_BUY: buys,
        COL_ADD: adds,
        COL_NEU: neutrals if neutrals else [0] * n,
        COL_RED: reduces if reduces else [0] * n,
        COL_EPS25: eps25s if eps25s else [1.0] * n,
        COL_EPS26: eps26s if eps26s else [1.1] * n,
    }
    return pd.DataFrame(data, index=codes)


def _score_of(factor, df, ts_code):
    """获取指定 ts_code 的得分."""
    scores = factor.score(df)
    return float(scores.loc[ts_code])


class TestProfitForecastFactor:
    @pytest.fixture
    def factor(self):
        return ProfitForecastFactor()

    # --- 因子属性 ---

    def test_factor_attributes(self, factor):
        assert factor.name == "profit_forecast"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 20.0

    # --- 空数据 ---

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0
        assert scores.name == "profit_forecast"

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series(dtype=float))
        assert reasons == {}

    # --- 覆盖度 (0-30) ---

    def test_coverage_more_reports_higher(self, factor):
        """控制评级和增长相同，仅覆盖度不同."""
        df = _make_df(
            ['A.SH', 'B.SZ', 'C.SH'],
            reports=[10, 1, 5],
            buys=[8, 1, 4],
            adds=[2, 0, 1],
        )
        signals = factor._compute_signals(df)
        assert signals['coverage']['A.SH'] > signals['coverage']['C.SH'] > signals['coverage']['B.SZ']

    def test_coverage_single_report_lowest(self, factor):
        """仅 1 家研报覆盖度最低 — 直接验证 coverage 子信号."""
        df = _make_df(
            ['A.SH', 'B.SZ', 'C.SH', 'D.SZ', 'E.SH'],
            reports=[15, 1, 1, 1, 1],
            buys=[10, 1, 1, 1, 1],
            adds=[5, 0, 0, 0, 0],
        )
        signals = factor._compute_signals(df)
        assert signals['coverage']['A.SH'] > signals['coverage']['B.SZ']

    # --- 评级质量 (0-40) ---

    def test_rating_more_buys_higher(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ'],
            reports=[5, 5],
            buys=[5, 1],
            adds=[0, 4],
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert a > b

    def test_rating_neutral_lower(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ'],
            reports=[5, 5],
            buys=[4, 4],
            adds=[0, 0],
            neutrals=[1, 0],
            reduces=[0, 1],
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert a > b

    # --- EPS 增长 (0-30) ---

    def test_eps_high_growth_higher(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ', 'C.SH'],
            reports=[5, 5, 5],
            buys=[4, 4, 4],
            adds=[1, 1, 1],
            eps25s=[1.0, 1.0, 1.0],
            eps26s=[2.0, 1.0, 0.5],
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        c = _score_of(factor, df, 'C.SH')
        assert a > b > c

    def test_eps_negative_growth_penalized(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ'],
            reports=[5, 5],
            buys=[4, 4],
            adds=[1, 1],
            eps25s=[1.0, 1.0],
            eps26s=[0.3, 1.5],
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert b > a

    # --- 综合打分 ---

    def test_score_clamp_0_100(self, factor):
        n = 30
        codes = [f'{600000 + i:06d}.SH' for i in range(n)]
        rng = np.random.default_rng(42)
        df = _make_df(
            codes,
            reports=rng.integers(1, 20, n).tolist(),
            buys=rng.integers(1, 15, n).tolist(),
            adds=rng.integers(0, 5, n).tolist(),
            eps25s=rng.uniform(0.1, 5, n).tolist(),
            eps26s=rng.uniform(0.1, 6, n).tolist(),
        )
        scores = factor.score(df)
        assert scores.min() >= 0
        assert scores.max() <= 100

    def test_score_combined_three_signals(self, factor):
        df = _make_df(
            ['A.SH'],
            reports=[5],
            buys=[4],
            adds=[1],
            eps25s=[1.0],
            eps26s=[1.5],
        )
        signals = factor._compute_signals(df)
        expected = float(signals['coverage']['A.SH'] +
                         signals['rating_quality']['A.SH'] +
                         signals['eps_growth']['A.SH'])
        assert _score_of(factor, df, 'A.SH') == pytest.approx(expected, abs=0.01)

    def test_score_differentiation(self, factor):
        n = 50
        codes = [f'{600000 + i:06d}.SH' for i in range(n)]
        rng = np.random.default_rng(7)
        df = _make_df(
            codes,
            reports=rng.integers(1, 30, n).tolist(),
            buys=rng.integers(1, 20, n).tolist(),
            adds=rng.integers(0, 10, n).tolist(),
            eps25s=rng.uniform(0.1, 5, n).tolist(),
            eps26s=rng.uniform(0.05, 6, n).tolist(),
        )
        scores = factor.score(df)
        assert scores.max() - scores.min() > 10

    # --- describe 标签 ---

    def test_describe_coverage_label(self, factor):
        df = _make_df(
            ['A.SH'],
            reports=[20],
            buys=[15],
            adds=[5],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert 'A.SH' in reasons
        assert any('机构覆盖' in lab for lab in reasons['A.SH'])

    def test_describe_rating_label(self, factor):
        df = _make_df(
            ['A.SH'],
            reports=[10],
            buys=[9],
            adds=[1],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert 'A.SH' in reasons
        assert any('机构评级' in lab for lab in reasons['A.SH'])

    def test_describe_growth_label(self, factor):
        df = _make_df(
            ['A.SH'],
            reports=[5],
            buys=[4],
            adds=[1],
            eps25s=[1.0],
            eps26s=[2.0],
        )
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert 'A.SH' in reasons
        assert any('盈利增长' in lab for lab in reasons['A.SH'])

    def test_describe_below_threshold_skipped(self, factor):
        """低于阈值的信号不生成标签 — 需要多只股票让百分位拉开."""
        codes = ['A.SH'] + [f'X{i:03d}.SH' for i in range(1, 20)]
        reports = [1] + [10] * 19
        buys = [0] + [8] * 19
        adds = [1] + [2] * 19
        eps25 = [1.0] + [2.0] * 19
        eps26 = [1.005] + [3.0] * 19
        df = _make_df(codes, reports=reports, buys=buys, adds=adds,
                       eps25s=eps25, eps26s=eps26)
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        # A 在 20 只股票中各项百分位都极低 → 所有信号 < 阈值 → 无标签
        assert 'A.SH' not in reasons

    def test_describe_zero_score_no_label(self, factor):
        df = _make_df(
            ['A.SH'],
            reports=[1],
            buys=[0],
            adds=[1],
        )
        scores = pd.Series(0.0, index=['A.SH'], name='profit_forecast')
        reasons = factor.describe(df, scores)
        assert 'A.SH' not in reasons

    # --- 列名缺失 fallback ---

    def test_missing_buy_column_fallback(self, factor):
        df = pd.DataFrame({
            "研报数": [1],
        }, index=['A.SH'])
        signals = factor._compute_signals(df)
        assert 'rating_quality' in signals
        assert signals['rating_quality']['A.SH'] == 20.0

    def test_missing_eps_columns_fallback(self, factor):
        df = pd.DataFrame({
            "研报数": [1],
        }, index=['A.SH'])
        signals = factor._compute_signals(df)
        assert 'eps_growth' in signals
        assert signals['eps_growth']['A.SH'] == 15.0


class TestProfitForecastIntegration:
    """使用真实数据库的集成测试."""

    @pytest.fixture
    def factor(self):
        return ProfitForecastFactor()

    def test_fetch_data_from_db(self, factor):
        df = factor.fetch_data('20260510')
        assert df is not None
        assert len(df) > 0
        assert COL_RPT in df.columns
        assert COL_BUY in df.columns
        assert COL_ADD in df.columns
        assert COL_EPS25 in df.columns
        assert COL_EPS26 in df.columns

    def test_score_with_real_data(self, factor):
        df = factor.fetch_data('20260510')
        scores = factor.score(df)
        assert len(scores) == len(df)
        assert scores.min() >= 0
        assert scores.max() <= 100
        assert scores.max() - scores.min() > 10

    def test_describe_with_real_data(self, factor):
        df = factor.fetch_data('20260510')
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert len(reasons) > 0

    def test_top_scores_are_heavily_covered(self, factor):
        df = factor.fetch_data('20260510')
        scores = factor.score(df)
        top10 = scores.nlargest(10).index
        top_reports = df.loc[top10, COL_RPT].mean()
        all_reports = df[COL_RPT].mean()
        assert top_reports > all_reports
