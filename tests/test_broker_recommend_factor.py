# -*- coding: utf-8 -*-
"""BrokerRecommendFactor 单元测试.

覆盖：因子属性、空数据、推荐覆盖度(0-40)、券商质量加权(0-40)、
连续推荐加成(0-20)、综合打分、describe 标签、clamp 边界、真实数据集成。
"""

import numpy as np
import pandas as pd
import pytest

from src.discovery.factors.broker_recommend_factor import BrokerRecommendFactor


def _make_df(index_codes, brokers, broker_count, **extra):
    """构造 broker 推荐数据，每个 ts_code 可以有多个 broker 行."""
    rows = []
    for i, ts in enumerate(index_codes):
        bs = brokers[i] if isinstance(brokers[i], list) else [brokers[i]]
        bc = broker_count[i] if isinstance(broker_count, list) else broker_count
        for b in bs:
            row = {'ts_code': ts, 'broker': b, 'name': f'stock_{ts}',
                   'broker_count': bc}
            row.update({k: v[i] if isinstance(v, list) else v for k, v in extra.items()})
            rows.append(row)
    return pd.DataFrame(rows)


def _score_of(factor, df, ts_code):
    """获取指定 ts_code 的得分."""
    scores = factor.score(df)
    return float(scores[ts_code])


class TestBrokerRecommendFactor:
    @pytest.fixture
    def factor(self):
        return BrokerRecommendFactor()

    # --- 因子属性 ---

    def test_factor_attributes(self, factor):
        assert factor.name == "broker_recommend"
        assert factor.available_intraday is False
        assert factor.available_postmarket is True
        assert factor.weight == 20.0

    # --- 空数据 ---

    def test_empty_df_score(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0
        assert scores.name == "broker_recommend"

    def test_empty_df_describe(self, factor):
        reasons = factor.describe(pd.DataFrame(), pd.Series(dtype=float))
        assert reasons == {}

    # --- 推荐覆盖度 (0-40) ---

    def test_coverage_more_brokers_higher_score(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ', 'C.SH'],
            brokers=[['银河', '华泰', '中信'], ['银河'], ['银河', '华泰']],
            broker_count=1,
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        c = _score_of(factor, df, 'C.SH')
        assert a > c > b

    def test_coverage_equal_brokers_equal_score(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ'],
            brokers=[['银河', '华泰'], ['中信', '国信']],
            broker_count=1,
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert a == pytest.approx(b, abs=0.01)

    def test_coverage_single_broker_lowest_coverage(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ', 'C.SH', 'D.SZ', 'E.SH'],
            brokers=[['银河', '华泰', '中信'], ['银河'], ['华泰'], ['中信'], ['国信']],
            broker_count=1,
        )
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert a > b

    # --- 券商质量加权 (0-40) ---

    def test_broker_quality_high_quality_higher_score(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ'],
            brokers=[['银河'], ['小券商']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {'银河': 0.9, '小券商': 0.3}
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert a > b

    def test_broker_quality_no_quality_neutral(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {}
        s = _score_of(factor, df, 'A.SH')
        assert 55 <= s <= 65

    def test_broker_quality_multi_broker_average(self, factor):
        """多券商取质量分均值 — 控制覆盖度相同，仅质量不同."""
        df = _make_df(
            ['A.SH', 'B.SZ'],
            brokers=[['银河', '华泰'], ['中信', '国信']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {'银河': 0.9, '华泰': 0.8, '中信': 0.4, '国信': 0.4}
        df.attrs['consecutive_stocks'] = {}
        a = _score_of(factor, df, 'A.SH')   # avg 0.85 -> quality 34
        b = _score_of(factor, df, 'B.SZ')   # avg 0.40 -> quality 16
        assert a > b

    # --- 连续推荐加成 (0-20) ---

    def test_consecutive_bonus(self, factor):
        df = _make_df(
            ['A.SH', 'B.SZ'],
            brokers=[['银河'], ['华泰']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {
            'A.SH': {'broker_count_current': 2, 'broker_count_prev': 2},
        }
        a = _score_of(factor, df, 'A.SH')
        b = _score_of(factor, df, 'B.SZ')
        assert a > b

    def test_consecutive_partial_bonus(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河', '华泰', '中信']],
            broker_count=3,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {
            'A.SH': {'broker_count_current': 3, 'broker_count_prev': 1},
        }
        s = _score_of(factor, df, 'A.SH')
        assert 64 <= s <= 70

    def test_consecutive_growth_full_bonus(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {
            'A.SH': {'broker_count_current': 1, 'broker_count_prev': 5},
        }
        s = _score_of(factor, df, 'A.SH')
        assert 78 <= s <= 82

    def test_no_consecutive_zero_bonus(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {}
        s = _score_of(factor, df, 'A.SH')
        assert 55 <= s <= 65

    # --- 综合打分 ---

    def test_score_clamp_0_100(self, factor):
        codes = [f'{i:06d}.SH' for i in range(600000, 600020)]
        brokers_list = [['银河'] for _ in codes]
        df = _make_df(codes, brokers=brokers_list, broker_count=1)
        df.attrs['broker_quality'] = {'银河': 0.99}
        df.attrs['consecutive_stocks'] = {}
        scores = factor.score(df)
        assert scores.min() >= 0
        assert scores.max() <= 100

    def test_score_combined_three_signals(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河', '华泰']],
            broker_count=2,
        )
        df.attrs['broker_quality'] = {'银河': 0.8, '华泰': 0.6}
        df.attrs['consecutive_stocks'] = {
            'A.SH': {'broker_count_current': 2, 'broker_count_prev': 2},
        }
        signals = factor._compute_signals(df)
        expected = float(signals['coverage']['A.SH'] +
                         signals['broker_quality']['A.SH'] +
                         signals['consecutive']['A.SH'])
        assert _score_of(factor, df, 'A.SH') == pytest.approx(expected, abs=0.01)

    # --- describe 标签 ---

    def test_describe_coverage_label(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河', '华泰', '中信', '国信']],
            broker_count=4,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {}
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert 'A' in reasons
        assert any('券商金股' in lab for lab in reasons['A'])

    def test_describe_quality_label(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河', '华泰']],
            broker_count=2,
        )
        df.attrs['broker_quality'] = {'银河': 0.9, '华泰': 0.8}
        df.attrs['consecutive_stocks'] = {}
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert 'A' in reasons
        assert any('券商质量' in lab for lab in reasons['A'])

    def test_describe_consecutive_label(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河', '华泰']],
            broker_count=2,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {
            'A.SH': {'broker_count_current': 2, 'broker_count_prev': 2},
        }
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert 'A' in reasons
        assert any('连续推荐' in lab for lab in reasons['A'])

    def test_describe_below_threshold_skipped(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河']],
            broker_count=1,
        )
        df.attrs['broker_quality'] = {'银河': 0.3}
        df.attrs['consecutive_stocks'] = {}
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        if 'A' in reasons:
            labels = reasons['A']
            assert not any('券商质量' in lab for lab in labels)
            assert not any('连续推荐' in lab for lab in labels)

    def test_describe_zero_score_no_label(self, factor):
        df = _make_df(
            ['A.SH'],
            brokers=[['银河']],
            broker_count=0,
        )
        df.attrs['broker_quality'] = {}
        df.attrs['consecutive_stocks'] = {}
        scores = pd.Series(0.0, index=['A'], name='broker_recommend')
        reasons = factor.describe(df, scores)
        assert 'A' not in reasons


class TestBrokerRecommendFactorIntegration:
    """使用真实数据库的集成测试 (需已导入 broker 测试数据)."""

    @pytest.fixture
    def factor(self):
        return BrokerRecommendFactor()

    def test_fetch_data_returns_dataframe(self, factor):
        df = factor.fetch_data('20260501')
        assert df is not None
        assert len(df) > 0
        assert 'ts_code' in df.columns
        assert 'broker' in df.columns
        assert 'name' in df.columns
        assert 'broker_count' in df.columns
        assert 'month' in df.attrs
        assert 'broker_quality' in df.attrs
        assert 'consecutive_stocks' in df.attrs

    def test_fetch_data_all_months(self, factor):
        for month_day in ['20260101', '20260201', '20260301', '20260401', '20260501']:
            df = factor.fetch_data(month_day)
            assert df is not None, f'Failed for {month_day}'
            assert len(df) > 0, f'Empty for {month_day}'

    def test_score_with_real_data(self, factor):
        df = factor.fetch_data('20260501')
        scores = factor.score(df)
        assert len(scores) == df['ts_code'].nunique()
        assert scores.min() >= 0
        assert scores.max() <= 100
        assert scores.max() - scores.min() > 10

    def test_describe_with_real_data(self, factor):
        df = factor.fetch_data('20260501')
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert len(reasons) > 0

    def test_fetch_data_invalid_month(self, factor):
        df = factor.fetch_data('20990101')
        assert df is None

    def test_consecutive_integration(self, factor):
        df = factor.fetch_data('20260501')
        consecutive = df.attrs.get('consecutive_stocks', {})
        assert len(consecutive) > 0

    def test_broker_quality_has_data(self, factor):
        df = factor.fetch_data('20260501')
        broker_quality = df.attrs.get('broker_quality', {})
        assert len(broker_quality) > 0
        for _name, q in broker_quality.items():
            assert 0 <= q <= 1
