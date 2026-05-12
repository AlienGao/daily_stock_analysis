# -*- coding: utf-8 -*-
"""盘中资金流共享数据源 (Intraday Money Flow Source).

提供 3 级降级拉取：东财 push2（实时全粒度）→ 同花顺 akshare（实时粗粒度）→ Tushare 资金流 + realtime_spot（盘后兜底）。

统一输出 7 列 DataFrame，index 为 ts_code（600519.SH）格式：
  major_net, lg_net, inflow_rate, pct_chg, turnover_rate, volume_ratio, data_source

Tier 1/2 成功后自动写入 momentum_snapshot 表，供后续降级读取。
"""

import logging
import os
from datetime import datetime as dt, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================================
# Public API
# ============================================================================


def fetch_intraday_money_flow(
    trade_date: str,
    tushare_fetcher=None,
) -> Optional[pd.DataFrame]:
    """3 级降级拉取盘中资金流数据 + DB 落库。

    Tier 1: East Money push2（实时全粒度，全市场）
    Tier 2: akshare 同花顺个股资金流（实时，粗粒度）
    Tier 3: Tushare 资金流 + realtime_spot 实时指标（盘后兜底）
    """

    # ── Tier 1: East Money push2 ──
    try:
        logger.info("[MoneyFlow] Tier 1: 拉取 East Money push2 实时资金流...")
        df = _fetch_tier1_eastmoney()
        if df is not None and not df.empty:
            logger.info("[MoneyFlow] Tier 1 成功: %d 只股票", len(df))
            _cache_to_db(df, trade_date, source="eastmoney")
            return df
    except Exception as e:
        logger.warning("[MoneyFlow] Tier 1 (East Money push2) 失败: %s", e)

    # ── Tier 2: 同花顺 akshare ──
    try:
        logger.info("[MoneyFlow] Tier 2: 拉取 akshare/同花顺 个股资金流...")
        df = _fetch_tier2_tonghuashun(trade_date)
        if df is not None and not df.empty:
            logger.info("[MoneyFlow] Tier 2 成功: %d 只股票", len(df))
            _cache_to_db(df, trade_date, source="akshare")
            return df
    except Exception as e:
        logger.warning("[MoneyFlow] Tier 2 (akshare/同花顺) 失败: %s", e)

    # ── Tier 3: Tushare ──
    try:
        logger.info("[MoneyFlow] Tier 3: 回退 Tushare 资金流...")
        df = _fetch_tier3_tushare(trade_date, tushare_fetcher)
        if df is not None and not df.empty:
            logger.info("[MoneyFlow] Tier 3 成功: %d 只股票", len(df))
            return df
    except Exception as e:
        logger.warning("[MoneyFlow] Tier 3 (Tushare) 失败: %s", e)

    logger.warning("[MoneyFlow] 所有数据源均失败")
    return None


# ============================================================================
# DB cache
# ============================================================================


def _cache_to_db(df: pd.DataFrame, trade_date: str, source: str = "eastmoney") -> None:
    """将资金流数据写入 momentum_snapshot 表（best-effort）。"""
    try:
        from src.storage import DatabaseManager

        cache = pd.DataFrame(index=df.index)
        cache["code"] = [str(c).split(".")[0].zfill(6) for c in df.index]
        cache["name"] = df.get("name", pd.Series("", index=df.index)).fillna("")
        cache["trade_date"] = str(trade_date)[:8]
        cache["major_net"] = df.get("major_net", 0)
        cache["lg_net"] = df.get("lg_net", 0)
        cache["inflow_rate"] = df.get("inflow_rate", 0)
        cache["pct_chg"] = df.get("pct_chg", 0)
        cache["turnover_rate"] = df.get("turnover_rate", 0)
        cache["volume_ratio"] = df.get("volume_ratio", 1.0)
        cache["data_source"] = df.get("data_source", source)

        db = DatabaseManager()
        n = db.upsert_momentum_snapshot(cache, source=source)
        logger.debug("[MoneyFlow] DB 缓存写入 %d 条 (source=%s)", n, source)
    except Exception as e:
        logger.debug("[MoneyFlow] DB 缓存写入失败: %s", e)


# ============================================================================
# Helpers
# ============================================================================


def _code_to_ts_code(code: str) -> str:
    """6 位代码 → ts_code 格式 (e.g. '600519' → '600519.SH')."""
    code_str = str(code).strip().zfill(6)
    if code_str.startswith(("60", "68")):
        return f"{code_str}.SH"
    elif code_str.startswith(("00", "30")):
        return f"{code_str}.SZ"
    elif code_str.startswith(("43", "83", "87", "92")):
        return f"{code_str}.BJ"
    return code_str


def _trading_minutes_elapsed() -> int:
    """A 股当日已过交易分钟数，排除午休（11:30-13:00）。

    9:30 前返回 0，15:00 后返回 240。
    """
    now = dt.now(ZoneInfo("Asia/Shanghai"))
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    morning_close = now.replace(hour=11, minute=30, second=0, microsecond=0)
    afternoon_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=0, second=0, microsecond=0)

    if now < market_open:
        return 0
    if now > market_close:
        return 240
    if now <= morning_close:
        return int((now - market_open).total_seconds() / 60)
    return 120 + int((now - afternoon_open).total_seconds() / 60)


def _parse_cn_amount(val) -> float:
    """解析中文金额字符串：'9937.50万'→99375000, '6.10亿'→610000000."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return 0.0
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    mul = 1.0
    if "亿" in s:
        s = s.replace("亿", "")
        mul = 1e8
    elif "万" in s:
        s = s.replace("万", "")
        mul = 1e4
    try:
        num = float(s) if s else 0.0
    except ValueError:
        return 0.0
    return -num * mul if neg else num * mul


def _parse_cn_pct(val) -> float:
    """解析百分比字符串：'20.01%'→20.01, '-3.50%'→-3.50."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("%", "")
    try:
        return float(s) if s else 0.0
    except ValueError:
        return 0.0


def _get_avg_volume(db, trade_date: str, window: int = 5) -> Optional["pd.Series"]:
    """获取每只股票过去 window 个交易日的日均成交量（股）。"""
    from sqlalchemy import text

    target = dt.strptime(trade_date, "%Y%m%d").date()
    cutoff = target - timedelta(days=window + 3)
    with db.get_session() as s:
        rows = s.execute(
            text(
                "SELECT code, volume FROM stock_daily "
                "WHERE date >= :cutoff AND date < :target AND volume > 0 "
                "ORDER BY code, date DESC"
            ),
            {"target": target, "cutoff": cutoff},
        ).fetchall()
        if not rows:
            return None
        vdf = pd.DataFrame(rows, columns=["code", "volume"])
        avg = vdf.groupby("code")["volume"].apply(
            lambda x: x.head(window).mean() if len(x) > 0 else x.mean()
        )
        return avg


def _compute_volume_ratio(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    """量比 = 当日预估全天量 / 过去5日均量。"""
    from src.storage import DatabaseManager

    db = DatabaseManager()
    spot = db.get_realtime_spot()
    if spot is None or spot.empty or "volume" not in spot.columns:
        logger.debug("[MoneyFlow] realtime_spot 无数据，跳过量比自算")
        return df

    elapsed = _trading_minutes_elapsed()
    if elapsed < 15:
        logger.debug("[MoneyFlow] 开盘不足 15 分钟，跳过量比自算")
        return df

    avg_vol = _get_avg_volume(db, trade_date)
    if avg_vol is None or avg_vol.empty:
        return df

    def _bare(x):
        return str(x).split(".")[0].strip().zfill(6)

    codes = pd.Series([_bare(x) for x in df.index], index=df.index, name="_code")

    spot_vol = spot[["volume"]].copy()
    spot_vol["_code"] = [_bare(x) for x in spot_vol.index]
    today_map = spot_vol.groupby("_code")["volume"].first()

    avg_vol_df = avg_vol.reset_index()
    avg_vol_df.columns = ["raw_code", "avg_vol"]
    avg_vol_df["_code"] = [_bare(x) for x in avg_vol_df["raw_code"]]
    avg_map = avg_vol_df.groupby("_code")["avg_vol"].first()

    df["today_vol"] = codes.map(today_map)
    df["avg_vol_5d"] = codes.map(avg_map)

    has_data = df["today_vol"].notna() & (df["avg_vol_5d"] > 0)
    est_vol = df["today_vol"] * (240.0 / elapsed)
    df.loc[has_data, "volume_ratio"] = (
        est_vol[has_data] / df.loc[has_data, "avg_vol_5d"]
    ).clip(lower=0)

    result = df.drop(columns=["today_vol", "avg_vol_5d"], errors="ignore")
    logger.debug(
        "[MoneyFlow] 量比自算完成: %d/%d 只有效",
        has_data.sum(), len(result),
    )
    return result


# ============================================================================
# Tier 1: East Money push2
# ============================================================================


def _fetch_tier1_eastmoney() -> Optional[pd.DataFrame]:
    """通过 Clash 代理调用东财 push2 API，一次拉取全市场资金流+行情。"""
    import requests

    host = os.getenv("PROXY_HOST", "127.0.0.1")
    port = os.getenv("PROXY_PORT", "42484")
    proxy_url = f"http://{host}:{port}"

    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": proxy_url, "https": proxy_url}
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    })

    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f62",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        "fields": "f2,f3,f8,f10,f12,f14,f62,f72,f184",
    }

    r = session.get(
        "https://82.push2.eastmoney.com/api/qt/clist/get",
        params=params, timeout=15,
    )
    r.raise_for_status()
    data = r.json()

    if data.get("rc") != 0 or data.get("data") is None:
        logger.warning("[MoneyFlow] 东财 API 返回异常: rc=%s", data.get("rc"))
        return None

    items = data["data"].get("diff", [])
    if not items:
        return None

    raw = pd.DataFrame(items)
    return _normalize_eastmoney(raw)


def _normalize_eastmoney(df: pd.DataFrame) -> pd.DataFrame:
    """东财 push2 字段 → 统一列。"""
    df = df.copy()

    def _num(col, default=0):
        return pd.to_numeric(df.get(col, default), errors="coerce").fillna(default)

    result = pd.DataFrame(index=df.index)
    result["name"] = df.get("f14", "").fillna("")
    result["major_net"] = _num("f62", 0)
    result["lg_net"] = _num("f72", 0)
    result["inflow_rate"] = _num("f184", 0) / 100.0
    result["pct_chg"] = _num("f3", 0)
    result["turnover_rate"] = _num("f8", 0)
    result["volume_ratio"] = _num("f10", 1.0)
    result["data_source"] = "eastmoney_push2"

    ts_codes = df["f12"].astype(str).str.zfill(6).apply(_code_to_ts_code)
    result.index = ts_codes
    result = result[~result.index.duplicated(keep="first")]
    return result


# ============================================================================
# Tier 2: 同花顺 akshare
# ============================================================================


def _fetch_tier2_tonghuashun(trade_date: str) -> Optional[pd.DataFrame]:
    """akshare 同花顺个股资金流（即时），量比通过 DB 自算。"""
    import akshare as ak

    df = ak.stock_fund_flow_individual(symbol="即时")
    if df is None or df.empty:
        logger.warning("[MoneyFlow] Tier 2 同花顺返回空数据")
        return None

    result = _normalize_tonghuashun(df)

    try:
        result = _compute_volume_ratio(result, trade_date)
    except Exception as e:
        logger.warning("[MoneyFlow] Tier 2 量比自算失败，使用默认 1.0: %s", e)

    return result


def _normalize_tonghuashun(df: pd.DataFrame) -> pd.DataFrame:
    """同花顺资金流字段 → 统一列。"""
    df = df.copy()

    code_col = next((c for c in df.columns if "代码" in str(c)), None)
    name_col = next((c for c in df.columns if "名称" in str(c)), None)
    pct_col = next((c for c in df.columns if "涨幅" in str(c) or "涨跌" in str(c)), None)
    hs_col = next((c for c in df.columns if "换手" in str(c)), None)
    net_col = next((c for c in df.columns if "净额" in str(c)), None)
    amt_col = next((c for c in df.columns if "成交额" in str(c)), None)

    def _parse_series(col, parser):
        if col is None:
            return pd.Series(0.0, index=df.index)
        return df[col].apply(parser)

    names = df[name_col].fillna("") if name_col else pd.Series("", index=df.index)
    major_net = _parse_series(net_col, _parse_cn_amount)
    amount = _parse_series(amt_col, _parse_cn_amount)
    pct_chg = _parse_series(pct_col, _parse_cn_pct)
    turnover_rate = _parse_series(hs_col, _parse_cn_pct)

    inflow_rate = (major_net / amount.replace(0, float("nan"))).fillna(0)

    code_series = df[code_col].astype(str).str.zfill(6) if code_col else pd.Series(index=df.index)

    result = pd.DataFrame({
        "name": names.values,
        "major_net": major_net.values,
        "lg_net": 0.0,
        "inflow_rate": inflow_rate.values,
        "pct_chg": pct_chg.values,
        "turnover_rate": turnover_rate.values,
        "volume_ratio": 1.0,
        "data_source": "akshare_tonghuashun",
    }, index=[_code_to_ts_code(c) for c in code_series])

    result = result[~result.index.duplicated(keep="first")]
    return result


# ============================================================================
# Tier 3: Tushare
# ============================================================================


def _fetch_tier3_tushare(trade_date: str, tushare_fetcher=None) -> Optional[pd.DataFrame]:
    """Tier 3: Tushare 资金流 + realtime_spot 实时指标（盘后兜底）。"""
    if tushare_fetcher is None:
        return None

    mf = tushare_fetcher.get_bulk_money_flow(trade_date)
    if mf is None or mf.empty:
        return None

    result = mf.copy()

    try:
        from src.storage import DatabaseManager
        spot = DatabaseManager().get_realtime_spot()
        if spot is not None and not spot.empty:
            bare_codes = result.index.str.split(".").str[0].str.zfill(6)
            for col in ["pct_chg", "turnover_rate", "volume_ratio"]:
                if col in spot.columns:
                    result[col] = bare_codes.map(spot[col])
    except Exception:
        pass

    return _normalize_tushare(result)


def _normalize_tushare(df: pd.DataFrame) -> pd.DataFrame:
    """Tushare 原始字段 → 统一列。"""
    df = df.copy()

    buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
    sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
    buy_lg = df.get("buy_lg_amount", pd.Series(0, index=df.index))
    sell_lg = df.get("sell_lg_amount", pd.Series(0, index=df.index))

    major_net = (buy_elg - sell_elg) + (buy_lg - sell_lg)
    lg_net = buy_lg - sell_lg
    total = buy_elg + sell_elg + buy_lg + sell_lg
    inflow_rate = (major_net / total.replace(0, float("nan"))).fillna(0)

    df["name"] = df.get("name", pd.Series("", index=df.index)).fillna("")
    df["major_net"] = major_net
    df["lg_net"] = lg_net
    df["inflow_rate"] = inflow_rate
    df["pct_chg"] = df.get("pct_chg", pd.Series(0, index=df.index))
    df["turnover_rate"] = df.get("turnover_rate", pd.Series(0, index=df.index))
    df["volume_ratio"] = df.get("volume_ratio", pd.Series(1.0, index=df.index))
    df["data_source"] = "tushare"

    keep_cols = ["name", "major_net", "lg_net", "inflow_rate",
                 "pct_chg", "turnover_rate", "volume_ratio", "data_source"]
    return df[keep_cols]
