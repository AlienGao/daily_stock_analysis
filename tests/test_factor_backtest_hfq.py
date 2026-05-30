"""快测后复权价格换算回归测试。"""
from src.discovery.factor_backtest_engine import FactorBacktestEngine


class _EngineStub(FactorBacktestEngine):
    def __init__(self, adj_cache, price_cache):
        self._adj_cache = adj_cache
        self._adj_max = {}
        self._price_cache = price_cache


def test_hfq_return_near_flat_across_ex_div():
    """除权除息前后：后复权收益率应接近连续，不应放大为大幅亏损。"""
    engine = _EngineStub(
        adj_cache={
            "20250527": {"300502": 1.0},
            "20250528": {"300502": 1.404},
        },
        price_cache={
            "20250527": {"300502": {"open": 114.12}},
            "20250528": {"300502": {"open": 81.03}},
        },
    )
    buy = engine._get_price("300502", "20250527", "open")
    sell = engine._get_price("300502", "20250528", "open")
    ret = (sell - buy) / buy

    raw_ret = (81.03 - 114.12) / 114.12
    assert raw_ret < -0.25
    assert abs(ret) < 0.05
    assert ret > raw_ret


def test_hfq_adj_lookup_and_return_formula():
    """后复权：raw×adj_factor，跨除权日收益率应连续。"""
    from src.discovery.factors.base import _lookup_adj_factor_map

    adj_by_code = {"300502": {"20250527": 1.0, "20250528": 1.404}}
    raw_buy, raw_sell = 114.12, 81.03
    adj_buy = _lookup_adj_factor_map(adj_by_code, "300502", "20250527")
    adj_sell = _lookup_adj_factor_map(adj_by_code, "300502", "20250528")
    buy = raw_buy * adj_buy
    sell = raw_sell * adj_sell
    ret = (sell - buy) / buy

    assert adj_buy == 1.0
    assert adj_sell == 1.404
    assert abs(ret) < 0.05

    wrong_buy = raw_buy * (1.404 / 1.0)
    wrong_ret = (raw_sell - wrong_buy) / wrong_buy
    assert wrong_ret < ret

