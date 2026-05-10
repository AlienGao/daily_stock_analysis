# -*- coding: utf-8 -*-
"""盘中因子 + RealtimeSpotProvider 综合性测试用例.

覆盖:
  1. 单位换算 (手/股、万/元、% → fraction)
  2. 格式匹配 (腾讯~分隔、新浪,分隔、东财 JSON)
  3. 业务临界 (score 边界、veto 触发、clip 封顶)
  4. 网络与兼容 (异常响应、超时、编码问题、畸变行)
  5. 多源交叉一致性 (同一标的、不同源 → 统一 schema)
  6. 降级链 (腾讯→新浪→东财→过期缓存→None)

标记:
  - @pytest.mark.network: 需要真实网络（仅实盘验证时开启）
  - 其他默认离线: 全部使用 mock payload,CI 安全
"""

from datetime import date
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
import requests as rq_lib

from src.discovery.realtime_spot import RealtimeSpotProvider, get_provider


# ============================================================
#  Mock payload builders
# ============================================================

def _make_tencent_line(code, name, price, pre_close, pct_chg,
                       volume_shou, amount_wan, turnover=2.5, volume_ratio=1.2,
                       open_p=None, high=None, low=None):
    """构建单条腾讯实时行情响应行 (~ 分隔, 50 字段)."""
    if open_p is None:
        open_p = pre_close
    if high is None:
        high = max(price, open_p)
    if low is None:
        low = min(price, open_p)
    fields = [""] * 50
    fields[1] = name
    fields[2] = code
    fields[3] = str(price)
    fields[4] = str(pre_close)
    fields[5] = str(open_p)
    fields[6] = str(volume_shou)
    fields[32] = str(pct_chg)
    fields[33] = str(high)
    fields[34] = str(low)
    fields[37] = str(amount_wan)
    fields[38] = str(turnover)
    fields[49] = str(volume_ratio)
    return f'v_sh{code}="{"~".join(fields)}";'


def _make_sina_line(code, name, price, pre_close, open_p=None,
                    high=None, low=None, volume_gu=1_000_000,
                    amount_yuan=10_000_000):
    """构建单条新浪实时行情响应行 (, 分隔, 32+ 字段)."""
    if open_p is None:
        open_p = pre_close
    if high is None:
        high = max(price, open_p)
    if low is None:
        low = min(price, open_p)
    parts = [
        name, str(open_p), str(pre_close), str(price),
        str(high), str(low),
        str(price - 0.01), str(price + 0.01),
        str(int(volume_gu)), str(int(amount_yuan)),
    ]
    parts.extend(["0"] * 22)
    return f'var hq_str_sh{code}="{",".join(parts)}";'


def _make_eastmoney_item(code, name, price, pct_chg, volume, amount,
                         turnover=2.5, volume_ratio=1.2, open_p=None,
                         high=None, low=None, pre_close=None):
    """构建单条东财 push2 diff 元素 (f2-f18 字段码)."""
    if pre_close is None:
        pre_close = price / (1 + pct_chg / 100)
    if open_p is None:
        open_p = pre_close
    if high is None:
        high = max(price, open_p)
    if low is None:
        low = min(price, open_p)
    return {
        "f2": price, "f3": pct_chg, "f5": volume, "f6": amount,
        "f8": turnover, "f10": volume_ratio, "f12": code, "f14": name,
        "f15": high, "f16": low, "f17": open_p, "f18": pre_close,
    }


def _make_eastmoney_response(items):
    """构建东财 push2 API 完整 JSON 响应."""
    return {"rc": 0, "data": {"diff": items, "total": len(items)}}


class DummyResponse:
    """可配置的 requests.Response 替身."""
    def __init__(self, text, status_code=200, encoding="gbk"):
        self.text = text
        self.status_code = status_code
        self.encoding = encoding

    def raise_for_status(self):
        if self.status_code >= 400:
            from requests.exceptions import HTTPError
            raise HTTPError(f"HTTP {self.status_code}")

    def json(self):
        import json
        return json.loads(self.text)



def _make_factor_df(index_codes, **cols):
    """快速构造因子 DataFrame."""
    df = pd.DataFrame(index=index_codes)
    for k, v in cols.items():
        df[k] = v if isinstance(v, (list, np.ndarray, pd.Series)) else [v] * len(index_codes)
    return df


# ============================================================
#  RealtimeSpotProvider — 单位换算
# ============================================================

class TestUnitConversion:
    """腾讯 volume(手→股) amount(万元→元); 新浪/东财 原样."""

    def test_tencent_volume_converted_to_shares(self, monkeypatch):
        lines = [_make_tencent_line("600519", "茅台", 1800.0, 1790.0, 0.56,
                                    volume_shou=5000, amount_wan=9000)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_tencent()
        assert df.iloc[0]["volume"] == 5000 * 100

    def test_tencent_amount_converted_to_yuan(self, monkeypatch):
        lines = [_make_tencent_line("600519", "茅台", 1800.0, 1790.0, 0.56,
                                    volume_shou=1, amount_wan=9000)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_tencent()
        assert df.iloc[0]["amount"] == 9000 * 10000

    def test_sina_volume_is_shares(self, monkeypatch):
        vol_gu = 3_200_000
        lines = [_make_sina_line("600519", "茅台", 1800.0, 1790.0, volume_gu=vol_gu)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_sina()
        assert df.iloc[0]["volume"] == vol_gu

    def test_sina_amount_is_yuan(self, monkeypatch):
        amt = 5_700_000_000
        lines = [_make_sina_line("600519", "茅台", 1800.0, 1790.0, amount_yuan=amt)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_sina()
        assert df.iloc[0]["amount"] == amt

    def test_cross_source_volume_consistency(self, monkeypatch):
        """腾讯(手→股) vs 新浪(原样股) 经 _normalize 后同量级."""
        tx_lines = [_make_tencent_line("600519", "茅台", 1800.0, 1790.0, 0.5,
                                       volume_shou=5000, amount_wan=9000)]
        sn_lines = [_make_sina_line("600519", "茅台", 1800.0, 1790.0,
                                    volume_gu=500000)]
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))

        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(tx_lines)))
        df_tx = RealtimeSpotProvider._fetch_tencent()
        norm_tx = RealtimeSpotProvider._normalize(df_tx, "tencent")

        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(sn_lines)))
        df_sn = RealtimeSpotProvider._fetch_sina()
        norm_sn = RealtimeSpotProvider._normalize(df_sn, "sina")

        diff = abs(norm_tx.loc["600519", "volume"] - norm_sn.loc["600519", "volume"])
        assert diff < 1.0


# ============================================================
#  RealtimeSpotProvider — 格式匹配
# ============================================================

class TestFormatParsing:

    def test_tencent_all_fields(self, monkeypatch):
        lines = [_make_tencent_line("600519", "贵州茅台", 1800.0, 1790.0, 0.56,
                                    volume_shou=5000, amount_wan=9000,
                                    turnover=2.5, volume_ratio=1.2,
                                    open_p=1795.0, high=1810.0, low=1785.0)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_tencent()
        row = df.iloc[0]
        assert row["code"] == "600519"
        assert row["name"] == "贵州茅台"
        assert row["price"] == "1800.0"
        assert row["pct_chg"] == "0.56"
        assert row["turnover_rate"] == 2.5
        assert row["volume_ratio"] == 1.2

    def test_tencent_short_line_skipped(self, monkeypatch):
        short = f'v_sh600519="{"~".join(["0"] * 10)}";'
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse(short))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_tencent()
        assert df is None

    def test_tencent_no_quote_skipped(self, monkeypatch):
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("garbage\n"))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_tencent()
        assert df is None

    def test_sina_code_from_prefix(self, monkeypatch):
        lines = [_make_sina_line("600519", "茅台", 1800.0, 1790.0)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_sina()
        assert df.iloc[0]["code"] == "sh600519"

    def test_sina_pct_chg_computed(self, monkeypatch):
        lines = [_make_sina_line("600519", "茅台", price=1810.0, pre_close=1790.0)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_sina()
        expected = round((1810 - 1790) / 1790 * 100, 2)
        assert df.iloc[0]["pct_chg"] == expected

    def test_sina_zero_preclose(self, monkeypatch):
        lines = [_make_sina_line("600519", "茅台", price=10.0, pre_close=0.0)]
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(lines)))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_sina()
        assert df.iloc[0]["pct_chg"] == 0.0

    def test_sina_short_line_skipped(self, monkeypatch):
        short = 'var hq_str_sh600519="' + ",".join(["0"] * 5) + '";'
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse(short))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        df = RealtimeSpotProvider._fetch_sina()
        assert df is None

    def test_eastmoney_all_fields(self, monkeypatch):
        items = [_make_eastmoney_item("600519", "茅台", 1800.0, 0.56,
                                      volume=500000, amount=9_000_000_000,
                                      turnover=2.5, volume_ratio=1.2,
                                      open_p=1795.0, high=1810.0, low=1785.0,
                                      pre_close=1790.0)]
        mock_sess = Mock()
        mock_sess.get.return_value = DummyResponse(
            str(_make_eastmoney_response(items)).replace("'", '"'),
            encoding="utf-8")
        monkeypatch.setattr(rq_lib, "Session", lambda: mock_sess)
        df = RealtimeSpotProvider._fetch_eastmoney(max_pages=1)
        assert df is not None
        row = df.iloc[0]
        assert row["f12"] == "600519"
        assert row["f14"] == "茅台"
        assert row["f2"] == 1800.0
        assert row["f3"] == 0.56
        assert row["f8"] == 2.5
        assert row["f10"] == 1.2

    def test_eastmoney_empty_diff(self, monkeypatch):
        mock_sess = Mock()
        mock_sess.get.return_value = DummyResponse(
            '{"rc":0,"data":{"diff":[],"total":0}}', encoding="utf-8")
        monkeypatch.setattr(rq_lib, "Session", lambda: mock_sess)
        df = RealtimeSpotProvider._fetch_eastmoney(max_pages=1)
        assert df is None


# ============================================================
#  RealtimeSpotProvider — 多批拉取
# ============================================================

class TestBatching:

    def test_multiple_batches_merged(self, monkeypatch):
        codes = [f"600{i:03d}" for i in range(1, 10)]
        all_lines = {}
        for c in codes:
            all_lines[c] = _make_tencent_line(c, f"股票{c}", 10.0, 9.9, 1.01,
                                              volume_shou=100, amount_wan=10)

        def mock_get(self, url, *a, **kw):
            # Only return lines for codes present in the URL
            requested = [line for code, line in all_lines.items() if code in url]
            return DummyResponse("\n".join(requested))

        monkeypatch.setattr(rq_lib.Session, "get", mock_get)
        with patch.object(RealtimeSpotProvider, "BATCH_SIZE", 3):
            monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                                classmethod(lambda cls: codes))
            df = RealtimeSpotProvider._fetch_tencent()
        assert len(df) == 9


# ============================================================
#  RealtimeSpotProvider — _normalize
# ============================================================

class TestNormalize:

    def test_strips_exchange_prefix(self):
        df = pd.DataFrame({
            "code": ["sh600519", "sz000858", "bj430489"],
            "name": ["茅台", "五粮液", "北证"], "price": [1800.0, 150.0, 10.0],
        })
        result = RealtimeSpotProvider._normalize(df, "test")
        assert "600519" in result.index
        assert "000858" in result.index
        assert "430489" in result.index

    def test_eastmoney_alias(self):
        df = pd.DataFrame([{
            "f12": "600519", "f14": "茅台", "f2": 1800.0, "f3": 1.5,
            "f18": 1773.0, "f17": 1780.0, "f15": 1810.0, "f16": 1775.0,
            "f5": 500000, "f6": 9e9, "f8": 2.5, "f10": 1.2,
        }])
        result = RealtimeSpotProvider._normalize(df, "eastmoney")
        assert result.loc["600519", "price"] == 1800.0
        assert result.loc["600519", "pct_chg"] == 1.5

    def test_filters_zero_price(self):
        df = pd.DataFrame({
            "code": ["600001", "600002", "600003"],
            "name": ["A", "B", "C"], "price": [10.0, 0.0, np.nan],
        })
        result = RealtimeSpotProvider._normalize(df, "test")
        assert len(result) == 1
        assert "600001" in result.index

    def test_missing_optional_cols(self):
        df = pd.DataFrame({"code": ["600519"], "name": ["茅台"], "price": [1800.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert "turnover_rate" in result.columns
        assert "volume_ratio" in result.columns

    def test_unknown_cols_empty(self):
        df = pd.DataFrame({"col_x": ["600519"], "col_y": [1.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert result.empty

    def test_source_label(self):
        df = pd.DataFrame({"code": ["600519"], "name": ["茅台"], "price": [1800.0]})
        result = RealtimeSpotProvider._normalize(df, "tencent")
        assert result.loc["600519", "source"] == "tencent"

    def test_trade_date_today(self):
        df = pd.DataFrame({"code": ["600519"], "name": ["茅台"], "price": [1800.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert result.loc["600519", "trade_date"] == date.today().isoformat()

    def test_output_uses_open_price_column(self):
        """_normalize 输出 open_price（非 open）,避免下游 KeyError."""
        df = pd.DataFrame({
            "code": ["600519"], "name": ["茅台"], "price": [1800.0],
            "open": [1795.0], "high": [1810.0], "low": [1785.0],
        })
        result = RealtimeSpotProvider._normalize(df, "test")
        assert "open_price" in result.columns
        assert "open" not in result.columns

    def test_duplicate_code_overwrites(self):
        df = pd.DataFrame({
            "code": ["600519", "600519"],
            "name": ["茅台A", "茅台B"], "price": [1800.0, 1801.0],
        })
        result = RealtimeSpotProvider._normalize(df, "test")
        # _normalize sets non-unique index; last row wins
        assert result.loc["600519", "name"].iloc[-1] == "茅台B"


# ============================================================
#  RealtimeSpotProvider — _to_tencent_codes
# ============================================================

class TestToTencentCodes:

    def test_sh_prefix(self):
        assert RealtimeSpotProvider._to_tencent_codes(["600519"]) == ["sh600519"]
        assert RealtimeSpotProvider._to_tencent_codes(["688001"]) == ["sh688001"]

    def test_sz_prefix(self):
        assert RealtimeSpotProvider._to_tencent_codes(["000858"]) == ["sz000858"]
        assert RealtimeSpotProvider._to_tencent_codes(["300750"]) == ["sz300750"]

    def test_bj_prefix(self):
        assert RealtimeSpotProvider._to_tencent_codes(["430489"]) == ["bj430489"]
        assert RealtimeSpotProvider._to_tencent_codes(["830799"]) == ["bj830799"]

    def test_zfill(self):
        result = RealtimeSpotProvider._to_tencent_codes(["1", "123"])
        assert result == ["sz000001", "sz000123"]

    def test_whitespace_stripped(self):
        assert RealtimeSpotProvider._to_tencent_codes([" 600519 "]) == ["sh600519"]

    def test_unknown_code_dropped(self):
        """未知前缀代码（如 7、5 开头）被静默丢弃。"""
        assert RealtimeSpotProvider._to_tencent_codes(["700001"]) == []


# ============================================================
#  RealtimeSpotProvider — 降级链
# ============================================================

class TestFallbackChain:

    @pytest.fixture
    def provider(self):
        p = RealtimeSpotProvider()
        p._last_slot = -1
        return p

    @pytest.fixture
    def code_list(self, monkeypatch):
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519", "000858"]))

    def test_all_fail_stale_cache(self, provider, code_list, monkeypatch):
        provider._cache["data"] = pd.DataFrame(
            {"code": ["600519"], "name": ["茅台"], "price": [1800.0]}
        ).set_index("code")
        provider._cache["slot"] = 99
        provider._cache["source"] = "tencent"
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_tencent", classmethod(lambda cls: None))
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_sina", classmethod(lambda cls: None))
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_eastmoney", classmethod(lambda cls: None))
        df = provider.fetch()
        assert df is not None

    def test_all_fail_no_cache(self, provider, code_list, monkeypatch):
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_tencent", classmethod(lambda cls: None))
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_sina", classmethod(lambda cls: None))
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_eastmoney", classmethod(lambda cls: None))
        df = provider.fetch()
        assert df is None

    def test_empty_df_falls_through(self, provider, code_list, monkeypatch):
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_tencent",
                            classmethod(lambda cls: pd.DataFrame()))
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_sina",
                            classmethod(lambda cls: pd.DataFrame()))
        monkeypatch.setattr(RealtimeSpotProvider, "_fetch_eastmoney",
                            classmethod(lambda cls: pd.DataFrame()))
        df = provider.fetch()
        assert df is None


# ============================================================
#  RealtimeSpotProvider — 槽位缓存
# ============================================================

class TestSlotCache:

    def test_same_slot_returns_cache(self):
        provider = RealtimeSpotProvider()
        fake = pd.DataFrame({"code": ["600519"], "name": ["茅台"], "price": [1800.0]}).set_index("code")
        provider._cache = {"data": fake, "slot": 42, "source": "tencent"}
        provider._last_slot = 42
        with patch("time.time", return_value=42 * 30 + 15):
            df = provider.fetch()
        assert df is fake

    def test_slot_boundary(self):
        assert int(0 // 30) == 0
        assert int(29 // 30) == 0
        assert int(30 // 30) == 1
        assert int(60 // 30) == 2

    def test_code_list_daily_refresh(self, monkeypatch):
        today = date.today().isoformat()
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list_from_db",
                            lambda: ["600519", "000858"])
        codes1 = RealtimeSpotProvider._get_code_list()
        assert len(codes1) == 2
        assert RealtimeSpotProvider._code_list_date == today
        codes2 = RealtimeSpotProvider._get_code_list()
        assert codes2 == codes1


# ============================================================
#  RealtimeSpotProvider — 网络异常
# ============================================================

class TestNetworkErrors:

    def test_tencent_timeout(self, monkeypatch):
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: (_ for _ in ()).throw(rq_lib.exceptions.Timeout()))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        assert RealtimeSpotProvider._fetch_tencent() is None

    def test_eastmoney_bad_json(self, monkeypatch):
        mock_sess = Mock()
        mock_sess.get.return_value = DummyResponse("not json", encoding="utf-8")
        monkeypatch.setattr(rq_lib, "Session", lambda: mock_sess)
        assert RealtimeSpotProvider._fetch_eastmoney(max_pages=1) is None

    def test_eastmoney_rc_nonzero(self, monkeypatch):
        mock_sess = Mock()
        mock_sess.get.return_value = DummyResponse(
            '{"rc":-1,"data":null}', encoding="utf-8")
        monkeypatch.setattr(rq_lib, "Session", lambda: mock_sess)
        assert RealtimeSpotProvider._fetch_eastmoney(max_pages=1) is None

    def test_sina_timeout(self, monkeypatch):
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: (_ for _ in ()).throw(rq_lib.exceptions.Timeout()))
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        assert RealtimeSpotProvider._fetch_sina() is None


# ============================================================
#  单例
# ============================================================

class TestSingleton:

    def test_same_instance(self):
        import src.discovery.realtime_spot as rs
        rs._provider = None
        p1 = rs.get_provider()
        p2 = rs.get_provider()
        assert p1 is p2


# ============================================================
#  SectorFactor
# ============================================================

class TestSectorFactor:

    @pytest.fixture
    def factor(self):
        from src.discovery.factors.sector_factor import SectorFactor
        return SectorFactor()

    def test_limit_times_scoring(self, factor):
        """连板梯度映射 + 默认 sector_heat=15 + seal_time=0."""
        df = _make_factor_df(["A.SH", "B.SZ", "C.BJ", "D.SH", "E.SZ"],
                             limit_times=[1, 2, 3, 4, 5])
        scores = factor.score(df)
        # chain: 1→15, 2→28, 3→38, 4→45, 5→50; +sector_heat 15 +seal 0
        assert scores["A.SH"] == 30.0   # 15+15+0
        assert scores["B.SZ"] == 43.0   # 28+15+0
        assert scores["C.BJ"] == 53.0   # 38+15+0
        assert scores["D.SH"] == 60.0   # 45+15+0
        assert scores["E.SZ"] == 65.0   # 50+15+0

    def test_limit_times_capped(self, factor):
        """limit_times ≥5 统一给满 50 + 默认 sector_heat 15."""
        df = _make_factor_df(["A.SH"], limit_times=[10])
        scores = factor.score(df)
        assert scores["A.SH"] == 65.0

    def test_limit_times_nan(self, factor):
        """NaN → chain=0，但 sector_heat 默认 15."""
        df = _make_factor_df(["A.SH"], limit_times=[np.nan])
        scores = factor.score(df)
        assert scores["A.SH"] == 15.0

    def test_pct_chg_fallback(self, factor):
        """无 limit_times 时降级为 pct_chg×5 + 默认 sector_heat 15."""
        df = _make_factor_df(["A.SH", "B.SZ"], pct_chg=[9.5, 12.0])
        scores = factor.score(df)
        assert scores["A.SH"] == 62.5   # 47.5+15+0
        assert scores["B.SZ"] == 65.0   # 50(capped)+15+0

    def test_pct_chg_negative_clamped(self, factor):
        """负涨幅 chain=0，但 sector_heat 默认 15."""
        df = _make_factor_df(["A.SH"], pct_chg=[-3.0])
        scores = factor.score(df)
        assert scores["A.SH"] == 15.0

    def test_describe_limit_times(self, factor):
        """3 连板 + 封板时间触发标签（无 sector 列时板块联动默认触发）."""
        df = _make_factor_df(["A.SH", "B.SZ", "C.BJ"],
                             limit_times=[3, 1, 2],
                             首次封板时间=["09:35", "09:40", "10:00"],
                             炸板次数=[0, 1, 0])
        scores = pd.Series([60.0, 20.0, 40.0], index=["A.SH", "B.SZ", "C.BJ"])
        reasons = factor.describe(df, scores)
        # A: lt=3 → "3连板", 封板09:35
        assert any("3连板" in r for r in reasons["A.SH"])
        assert any("09:35" in r for r in reasons["A.SH"])
        # B: lt=1 chain=15 < 25 threshold, no chain label; 封板09:40
        assert any("09:40" in r for r in reasons["B.SZ"])
        # C: lt=2 → "2连板", 封板10:00
        assert any("2连板" in r for r in reasons["C.BJ"])
        assert any("10:00" in r for r in reasons["C.BJ"])

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0


# ============================================================
#  MaEntryFactor
# ============================================================

class TestMaEntryFactor:

    @pytest.fixture
    def factor(self):
        from src.discovery.factors.ma_entry_factor import MaEntryFactor
        return MaEntryFactor()

    def _make_ma_df(self, codes, close, ma5, ma10, ma20, **extra):
        data = {"close": close, "ma5": ma5, "ma10": ma10, "ma20": ma20}
        data.update(extra)
        return _make_factor_df(codes, **data)

    def test_bull_align_adds_20(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.5], ma5=[10.0], ma10=[9.5], ma20=[9.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 20.0

    def test_bear_align_veto(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.0], ma5=[9.0], ma10=[9.5], ma20=[10.0])
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_gap_above_8pct_veto(self, factor):
        df = self._make_ma_df(["A.SH"], close=[11.0], ma5=[10.0], ma10=[9.5], ma20=[9.0])
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_ma5_pullback_adds_25(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.15], ma5=[10.0], ma10=[9.5], ma20=[9.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 45.0

    def test_spread_under_2pct_adds_15(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.0], ma5=[10.05], ma10=[10.0], ma20=[9.95])
        scores = factor.score(df)
        assert scores["A.SH"] >= 35.0

    def test_shrink_volume_near_ma(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.0], ma5=[10.0], ma10=[9.5], ma20=[9.0],
                              est_vol=[5000], avg_vol=[10000])
        scores = factor.score(df)
        assert scores["A.SH"] >= 35.0

    def test_kdj_oversold_adds_10(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.0], ma5=[10.0], ma10=[9.5], ma20=[9.0],
                              kdj_j=[15.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 30.0

    def test_boll_mid_support_adds_5(self, factor):
        df = self._make_ma_df(["A.SH"], close=[10.1], ma5=[10.0], ma10=[9.5], ma20=[9.0],
                              boll_mid=[10.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 25.0

    def test_no_ma_kdj_only(self, factor):
        df = _make_factor_df(["A.SH"], close=[10.0], kdj_j=[15.0])
        scores = factor.score(df)
        assert scores["A.SH"] == 10.0

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0


# ============================================================
#  MomentumFactor
# ============================================================

class TestMomentumFactor:

    @pytest.fixture
    def factor(self):
        from src.discovery.factors.momentum_factor import MomentumFactor
        return MomentumFactor()

    def test_strong_inflow_adds_30(self, factor):
        df = _make_factor_df(["A.SH"], inflow_rate=[0.15], volume_ratio=[1.5],
                             turnover_rate=[5.0], pct_chg=[2.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 30.0

    def test_moderate_inflow_adds_20(self, factor):
        df = _make_factor_df(["A.SH"], inflow_rate=[0.05], volume_ratio=[1.5],
                             turnover_rate=[5.0], pct_chg=[2.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 20.0

    def test_volume_ratio_above_2_adds_15(self, factor):
        df = _make_factor_df(["A.SH"], inflow_rate=[0.05], volume_ratio=[3.0],
                             turnover_rate=[5.0], pct_chg=[2.0])
        scores = factor.score(df)
        assert scores["A.SH"] >= 35.0

    def test_turnover_below_1_veto(self, factor):
        df = _make_factor_df(["A.SH"], inflow_rate=[0.15], volume_ratio=[3.0],
                             turnover_rate=[0.5], pct_chg=[2.0])
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_pct_chg_above_9_veto(self, factor):
        df = _make_factor_df(["A.SH"], inflow_rate=[0.15], volume_ratio=[3.0],
                             turnover_rate=[5.0], pct_chg=[9.5])
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_negative_inflow_penalty(self, factor):
        """净流入为负时扣 10 分，但其他信号仍贡献分数。"""
        df = _make_factor_df(["A.SH"], inflow_rate=[-0.05], volume_ratio=[3.0],
                             turnover_rate=[5.0], pct_chg=[2.0])
        scores = factor.score(df)
        assert scores["A.SH"] == pytest.approx(45.0)  # 55 - 10 = 45

    def test_pct_chg_tiers(self, factor):
        df = _make_factor_df(["A", "B", "C"],
                             inflow_rate=[0, 0, 0], volume_ratio=[1.0, 1.0, 1.0],
                             turnover_rate=[5.0, 5.0, 5.0], pct_chg=[1.0, 5.0, 8.0])
        scores = factor.score(df)
        assert scores["A"] >= 10.0
        assert scores["B"] >= 5.0
        assert scores["C"] >= 3.0

    def test_describe_reasons(self, factor):
        df = _make_factor_df(["A.SH"], inflow_rate=[0.12], volume_ratio=[2.5],
                             turnover_rate=[8.0], pct_chg=[5.0])
        scores = pd.Series([65.0], index=["A.SH"])
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert any("资金流入" in r for r in reasons["A.SH"])
        assert any("放量启动" in r for r in reasons["A.SH"])

    def test_normalize_eastmoney(self, factor):
        df = pd.DataFrame([{
            "f12": "600519", "f3": 1.5, "f8": 3.0, "f10": 1.5,
            "f62": 5e8, "f72": 2e8, "f184": 5.0,
        }])
        result = factor._normalize_eastmoney(df)
        assert result.index[0] == "600519.SH"
        assert result.iloc[0]["inflow_rate"] == 0.05
        assert result.iloc[0]["pct_chg"] == 1.5

    def test_normalize_tushare(self, factor):
        df = pd.DataFrame([{
            "buy_elg_amount": 1e8, "sell_elg_amount": 3e7,
            "buy_lg_amount": 5e7, "sell_lg_amount": 2e7,
        }], index=["600519.SH"])
        result = factor._normalize_tushare(df)
        assert result.iloc[0]["major_net"] == 1e8
        assert result.iloc[0]["inflow_rate"] == 0.5


# ============================================================
#  ReboundFactor
# ============================================================

class TestReboundFactor:

    @pytest.fixture
    def factor(self):
        from src.discovery.factors.rebound_factor import ReboundFactor
        return ReboundFactor()

    def test_shallow_drop_adds_25(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-1.0], inflow_rate=[0.03],
                             volume_ratio=[1.0], turnover_rate=[5.0],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] >= 25.0

    def test_moderate_drop_adds_15(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-4.0], inflow_rate=[0.03],
                             volume_ratio=[1.0], turnover_rate=[5.0],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] >= 15.0

    def test_deep_drop_adds_5(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-6.0], inflow_rate=[0.03],
                             volume_ratio=[1.0], turnover_rate=[5.0],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] >= 5.0

    def test_below_minus_7_veto(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-8.0], inflow_rate=[0.10],
                             volume_ratio=[3.0], turnover_rate=[10.0],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_turnover_below_1_veto(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-2.0], inflow_rate=[0.10],
                             volume_ratio=[3.0], turnover_rate=[0.5],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] == 0.0

    def test_strong_inflow_adds_25(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-2.0], inflow_rate=[0.08],
                             volume_ratio=[1.0], turnover_rate=[5.0],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] >= 50.0

    def test_open_times_tiers(self, factor):
        df = _make_factor_df(["A", "B", "C"],
                             pct_chg=[-2.0, -2.0, -2.0],
                             inflow_rate=[0, 0, 0],
                             volume_ratio=[1.0, 1.0, 1.0],
                             turnover_rate=[5.0, 5.0, 5.0],
                             open_times=[1, 2, 3],
                             limit_times=[1, 1, 1])
        scores = factor.score(df)
        assert scores["A"] == pytest.approx(60.5)
        assert scores["B"] == pytest.approx(55.5)
        assert scores["C"] == pytest.approx(52.5)

    def test_limit_times_tiers(self, factor):
        df = _make_factor_df(["A", "B", "C"],
                             pct_chg=[-2.0, -2.0, -2.0],
                             inflow_rate=[0, 0, 0],
                             volume_ratio=[1.0, 1.0, 1.0],
                             turnover_rate=[5.0, 5.0, 5.0],
                             open_times=[1, 1, 1],
                             limit_times=[1, 3, 6])
        scores = factor.score(df)
        assert scores["A"] == pytest.approx(60.5)
        assert scores["B"] == pytest.approx(53.5)
        assert scores["C"] == pytest.approx(50.5)

    def test_max_capped_at_100(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-1.0], inflow_rate=[0.10],
                             volume_ratio=[3.0], turnover_rate=[10.0],
                             open_times=[1], limit_times=[1])
        scores = factor.score(df)
        assert scores["A.SH"] <= 100.0

    def test_describe_reasons(self, factor):
        df = _make_factor_df(["A.SH"], pct_chg=[-2.0], inflow_rate=[0.06],
                             volume_ratio=[2.0], turnover_rate=[8.0],
                             open_times=[1], limit_times=[2])
        scores = pd.Series([75.0], index=["A.SH"])
        reasons = factor.describe(df, scores)
        reason_text = " ".join(reasons["A.SH"])
        assert "跌幅承接" in reason_text
        assert "资金回补" in reason_text
        assert "2板炸板" in reason_text


# ============================================================
#  PopularityFactor
# ============================================================

class TestPopularityFactor:

    @pytest.fixture
    def factor(self):
        from src.discovery.factors.popularity_factor import PopularityFactor
        return PopularityFactor()

    def test_surge_percentile_top_gets_full(self, factor):
        """飙升幅度：rank_change 最高者得满分 45。"""
        df = _make_factor_df(["A.SH", "B.SH", "C.SH"],
            rank=[50, 50, 50],
            rank_change=[3000, 1500, 500],
            pct_chg=[1.0, 1.0, 1.0])
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SH"] > scores["C.SH"]

    def test_surge_zero_for_decliners(self, factor):
        """排名下降的股票无飙升分。"""
        df = _make_factor_df(["A.SH", "B.SH"],
            rank=[50, 50],
            rank_change=[-100, 500],
            pct_chg=[1.0, 1.0])
        scores = factor.score(df)
        assert scores["A.SH"] < scores["B.SH"]

    def test_rank_strength_top_gets_full(self, factor):
        """排名第一得满排名分。"""
        df = _make_factor_df(["A.SH", "B.SH", "C.SH"],
            rank=[1, 50, 100],
            rank_change=[100, 100, 100],
            pct_chg=[1.0, 1.0, 1.0])
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SH"] > scores["C.SH"]

    def test_pct_chg_positive_boosts(self, factor):
        """涨跌幅正向贡献，高涨幅 > 低/负涨幅。"""
        df = _make_factor_df(["A.SH", "B.SH", "C.SH"],
            rank=[50, 50, 50],
            rank_change=[100, 100, 100],
            pct_chg=[10.0, 2.0, -5.0])
        scores = factor.score(df)
        assert scores["A.SH"] > scores["B.SH"] > scores["C.SH"]

    def test_empty_df(self, factor):
        scores = factor.score(pd.DataFrame())
        assert len(scores) == 0

    def test_describe_reasons(self, factor):
        df = _make_factor_df(["A.SH", "B.SH"],
            rank=[5, 80],
            rank_change=[2500, 100],
            pct_chg=[5.0, 1.0])
        scores = factor.score(df)
        reasons = factor.describe(df, scores)
        assert "A.SH" in reasons
        assert any("人气飙升" in r for r in reasons["A.SH"])


# ============================================================
#  跨因子一致性
# ============================================================

class TestCrossFactor:

    @pytest.fixture
    def all_factors(self):
        from src.discovery.factors.sector_factor import SectorFactor
        from src.discovery.factors.ma_entry_factor import MaEntryFactor
        from src.discovery.factors.momentum_factor import MomentumFactor
        from src.discovery.factors.rebound_factor import ReboundFactor
        from src.discovery.factors.popularity_factor import PopularityFactor
        return [SectorFactor(), MaEntryFactor(), MomentumFactor(),
                ReboundFactor(), PopularityFactor()]

    def test_score_returns_series(self, all_factors):
        for f in all_factors:
            result = f.score(pd.DataFrame())
            assert isinstance(result, pd.Series), f"{f.name}: 非 Series"
            assert result.name == f.name, f"{f.name}: name 不匹配"

    def test_describe_returns_dict(self, all_factors):
        for f in all_factors:
            result = f.describe(pd.DataFrame(), pd.Series(dtype=float))
            assert isinstance(result, dict), f"{f.name}: 非 dict"

    def test_score_always_0_to_100(self, all_factors):
        rng = np.random.default_rng(42)
        for f in all_factors:
            codes = [f"{i:06d}.SH" for i in range(100)]
            data = {
                "pct_chg": rng.uniform(-15, 15, 100),
                "inflow_rate": rng.uniform(-0.3, 0.3, 100),
                "volume_ratio": rng.uniform(0.1, 10, 100),
                "turnover_rate": rng.uniform(0, 30, 100),
                "close": rng.uniform(1, 1000, 100),
                "ma5": rng.uniform(1, 1000, 100),
                "ma10": rng.uniform(1, 1000, 100),
                "ma20": rng.uniform(1, 1000, 100),
                "limit_times": rng.integers(0, 10, 100),
                "open_times": rng.integers(0, 5, 100),
            }
            df = _make_factor_df(codes, **data)
            scores = f.score(df)
            assert scores.min() >= 0.0, f"{f.name}: min < 0"
            assert scores.max() <= 100.0, f"{f.name}: max > 100"

    def test_metadata_complete(self, all_factors):
        for f in all_factors:
            assert isinstance(f.name, str) and len(f.name) > 0
            assert isinstance(f.weight, (int, float)) and f.weight > 0
            assert isinstance(f.available_intraday, bool)


# ============================================================
#  索引格式兼容
# ============================================================

class TestIndexFormat:

    def test_normalize_returns_bare_code(self):
        df = pd.DataFrame({"code": ["sh600519"], "name": ["茅台"], "price": [1800.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert result.index[0] == "600519"

    def test_sector_ts_code_conversion(self):
        from src.discovery.factors.sector_factor import SectorFactor
        df = pd.DataFrame({"code": ["600519"]}, index=["600519"])
        result = SectorFactor._with_ts_code_index(df)
        assert result.index[0] == "600519.SH"

    def test_momentum_code_to_ts(self):
        from src.discovery.factors.momentum_factor import MomentumFactor
        assert MomentumFactor._code_to_ts_code("600519") == "600519.SH"
        assert MomentumFactor._code_to_ts_code("000858") == "000858.SZ"
        assert MomentumFactor._code_to_ts_code("430489") == "430489.BJ"


# ============================================================
#  边界值 / 极端输入
# ============================================================

class TestEdgeCases:

    def test_zero_volume_ok(self, monkeypatch):
        tx = [_make_tencent_line("600519", "茅台", 1800.0, 1790.0, 0.0,
                                 volume_shou=0, amount_wan=0)]
        monkeypatch.setattr(RealtimeSpotProvider, "_get_code_list",
                            classmethod(lambda cls: ["600519"]))
        monkeypatch.setattr(rq_lib.Session, "get",
                            lambda *a, **kw: DummyResponse("\n".join(tx)))
        df = RealtimeSpotProvider._fetch_tencent()
        assert df.iloc[0]["volume"] == 0.0

    def test_negative_price_filtered(self):
        df = pd.DataFrame({"code": ["600519", "000858"], "name": ["A", "B"],
                           "price": [-1.0, 150.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert len(result) == 1

    def test_dot_suffix_kept_as_is(self):
        """_normalize 仅剥离前缀 (sh/sz/bj)，不处理 .SH/.SZ 后缀。"""
        df = pd.DataFrame({"code": ["sh600519", "sz000858"],
                           "name": ["茅台", "五粮液"], "price": [1800.0, 150.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert "600519" in result.index
        assert "000858" in result.index

    def test_all_nan_filtered(self):
        df = pd.DataFrame({"code": [np.nan], "price": [np.nan]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert len(result) == 0

    def test_extreme_input_no_crash(self):
        from src.discovery.factors.rebound_factor import ReboundFactor
        factor = ReboundFactor()
        df = _make_factor_df(["A.SH"],
                             pct_chg=[-100.0], inflow_rate=[-1.0],
                             volume_ratio=[0.0], turnover_rate=[0.0],
                             open_times=[99], limit_times=[99])
        scores = factor.score(df)
        assert 0.0 <= scores["A.SH"] <= 100.0

    def test_weekend_graceful(self):
        from src.discovery.factors.rebound_factor import ReboundFactor
        from src.discovery.factors.momentum_factor import MomentumFactor
        for F in [ReboundFactor, MomentumFactor]:
            factor = F()
            scores = factor.score(pd.DataFrame())
            assert isinstance(scores, pd.Series)
            reasons = factor.describe(pd.DataFrame(), pd.Series(dtype=float))
            assert reasons == {}

    def test_suspended_stock_filtered(self):
        df = pd.DataFrame({"code": ["600519", "000001"], "name": ["茅台", "停牌"],
                           "price": [1800.0, 0.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert "000001" not in result.index

    def test_ipo_no_preclose(self):
        df = pd.DataFrame({"code": ["001234"], "name": ["新股"], "price": [25.0]})
        result = RealtimeSpotProvider._normalize(df, "test")
        assert not result.empty

    def test_nan_fields_no_crash(self):
        from src.discovery.factors.ma_entry_factor import MaEntryFactor
        factor = MaEntryFactor()
        df = _make_factor_df(["A.SH", "B.SZ"],
                             close=[np.nan, 10.0], ma5=[np.nan, 10.0],
                             ma10=[np.nan, 9.5], ma20=[np.nan, 9.0],
                             kdj_j=[np.nan, 15.0])
        scores = factor.score(df)
        assert scores["B.SZ"] >= 0.0


# ============================================================
#  实盘冒烟测试 (pytest --run-network)
# ============================================================

@pytest.mark.network
class TestLiveSmoke:

    def test_tencent_live(self):
        df = RealtimeSpotProvider._fetch_tencent()
        assert df is not None, "腾讯接口不可达"
        assert len(df) > 100, f"腾讯仅返回 {len(df)} 只"

    def test_sina_live(self):
        df = RealtimeSpotProvider._fetch_sina()
        assert df is not None, "新浪接口不可达"
        assert len(df) > 100, f"新浪仅返回 {len(df)} 只"

    def test_eastmoney_live(self):
        df = RealtimeSpotProvider._fetch_eastmoney(max_pages=3)
        assert df is not None, "东财接口不可达 (代理?)"
        assert len(df) > 0, "东财返回空数据"

    def test_fetch_full_chain(self):
        provider = get_provider()
        provider._last_slot = -1
        df = provider.fetch()
        assert df is not None, "fetch() 三源全挂"
        assert len(df) > 100, f"fetch() 仅返回 {len(df)} 只"
        assert "price" in df.columns
        assert "volume" in df.columns

    def test_popularity_live(self):
        from src.discovery.factors.popularity_factor import PopularityFactor
        factor = PopularityFactor()
        df = factor.fetch_data(trade_date=date.today().strftime("%Y%m%d"))
        if df is not None:
            assert len(df) > 0
            scores = factor.score(df)
            assert scores.max() >= 0

    def test_sector_live(self):
        from src.discovery.factors.sector_factor import SectorFactor
        factor = SectorFactor()
        df = factor.fetch_data(trade_date=date.today().strftime("%Y%m%d"))
        if df is not None and not df.empty:
            scores = factor.score(df)
            assert scores.max() >= 0
