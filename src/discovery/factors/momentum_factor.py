# -*- coding: utf-8 -*-
"""强势启动因子 (Momentum / Breakout Factor).

在均线买点基础上叠加强势信号：资金流入、放量启动。
盘中 3 级数据源降级：东财 push2（实时全粒度）→ 同花顺（实时粗粒度）→ Tushare 资金流 + realtime_spot 实时指标（盘后兜底）。
盘中可用，盘后不可用（盘后有独立的技术面因子）。
"""

import logging
import os
from datetime import datetime as dt
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.discovery.factors.base import BaseFactor

logger = logging.getLogger(__name__)


def _linear_map(series: pd.Series, x0: float, y0: float,
                x1: float, y1: float, clip_low: float = 0.0,
                clip_high: float = 1e9) -> pd.Series:
    """两点线性映射，超出范围 clip。"""
    slope = (y1 - y0) / (x1 - x0) if x1 != x0 else 0.0
    return (y0 + slope * (series - x0)).clip(clip_low, clip_high)


class MomentumFactor(BaseFactor):
    """强势启动因子。

    检测资金流入、量比放大、换手健康、涨幅温和的启动信号。
    排除主力净流出、换手过低、涨幅接近涨停。
    """

    name = "momentum"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    _LABEL_THRESHOLD_RATIO = 0.5

    def fetch_data(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """3 级降级拉取数据：东财 push2 → 同花顺 → Tushare。"""

        # ── Tier 1: East Money push2（实时全粒度） ──
        try:
            logger.info("[MomentumFactor] Tier 1: 拉取 East Money push2 实时资金流...")
            df = self._fetch_tier1_eastmoney()
            if df is not None and not df.empty:
                logger.info("[MomentumFactor] Tier 1 成功: %d 只股票", len(df))
                return df
        except Exception as e:
            logger.warning("[MomentumFactor] Tier 1 (East Money push2) 失败: %s", e)

        # ── Tier 2: 同花顺 akshare（实时粗粒度） ──
        try:
            logger.info("[MomentumFactor] Tier 2: 拉取 akshare/同花顺 个股资金流...")
            df = self._fetch_tier2_tonghuashun(trade_date)
            if df is not None and not df.empty:
                logger.info("[MomentumFactor] Tier 2 成功: %d 只股票", len(df))
                return df
        except Exception as e:
            logger.warning("[MomentumFactor] Tier 2 (akshare/同花顺) 失败: %s", e)

        # ── Tier 3: Tushare（盘后兜底） ──
        try:
            logger.info("[MomentumFactor] Tier 3: 回退 Tushare 资金流...")
            df = self._fetch_tier3_tushare(trade_date, **kwargs)
            if df is not None and not df.empty:
                logger.info("[MomentumFactor] Tier 3 成功: %d 只股票", len(df))
                return df
        except Exception as e:
            logger.warning("[MomentumFactor] Tier 3 (Tushare) 失败: %s", e)

        logger.warning("[MomentumFactor] 所有数据源均失败")
        return None

    # ------------------------------------------------------------------
    # 共享信号提取
    # ------------------------------------------------------------------

    def _compute_signals(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """提取 4 个子信号，各自归一化到满分区间。"""
        idx = df.index
        zeros = pd.Series(0.0, index=idx)

        inflow_rate = df.get("inflow_rate", zeros)
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=idx))
        turnover_rate = df.get("turnover_rate", zeros)
        pct_chg = df.get("pct_chg", zeros)

        signals: Dict[str, pd.Series] = {}

        # 1. 资金流入强度 (0-35)
        s_inflow = zeros.copy()
        s_inflow = s_inflow.mask(inflow_rate > 0.10, 35.0)
        s_inflow = s_inflow.mask((inflow_rate >= 0.03) & (inflow_rate <= 0.10),
                                 _linear_map(inflow_rate, 0.03, 17, 0.10, 35))
        s_inflow = s_inflow.mask((inflow_rate > 0) & (inflow_rate < 0.03),
                                 _linear_map(inflow_rate, 0, 0, 0.03, 17))
        signals["inflow"] = s_inflow

        # 2. 放量启动 (0-25)
        s_vol = zeros.copy()
        s_vol = s_vol.mask(volume_ratio > 2.5, 25.0)
        s_vol = s_vol.mask((volume_ratio >= 1.2) & (volume_ratio <= 2.5),
                           _linear_map(volume_ratio, 1.2, 12, 2.5, 25))
        s_vol = s_vol.mask((volume_ratio >= 0.8) & (volume_ratio < 1.2),
                           _linear_map(volume_ratio, 0.8, 4, 1.2, 12))
        signals["volume_ratio"] = s_vol

        # 3. 换手健康 (0-15)：3-10% 最优，向两侧衰减
        s_tr = zeros.copy()
        s_tr = s_tr.mask((turnover_rate >= 3) & (turnover_rate <= 10), 15.0)
        s_tr = s_tr.mask((turnover_rate > 10) & (turnover_rate <= 15),
                         _linear_map(turnover_rate, 10, 15, 15, 5))
        s_tr = s_tr.mask((turnover_rate >= 1) & (turnover_rate < 3),
                         _linear_map(turnover_rate, 1, 2, 3, 15))
        signals["turnover"] = s_tr

        # 4. 涨幅合理 (0-25)：2-5% 最优，温和启动不追高
        s_pct = zeros.copy()
        s_pct = s_pct.mask((pct_chg >= 2) & (pct_chg <= 5), 25.0)
        s_pct = s_pct.mask((pct_chg >= 0) & (pct_chg < 2),
                           _linear_map(pct_chg, 0, 6, 2, 25))
        s_pct = s_pct.mask((pct_chg > 5) & (pct_chg <= 7),
                           _linear_map(pct_chg, 5, 25, 7, 10))
        s_pct = s_pct.mask((pct_chg > 7) & (pct_chg <= 9),
                           _linear_map(pct_chg, 7, 10, 9, 3))
        signals["pct_chg"] = s_pct

        return signals

    # ------------------------------------------------------------------
    # Score / Describe
    # ------------------------------------------------------------------

    def score(self, df: pd.DataFrame, **context) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        signals = self._compute_signals(df)
        total = sum(signals.values())

        # 净流出惩罚
        inflow_rate = df.get("inflow_rate", pd.Series(0, index=df.index))
        total.loc[inflow_rate < 0] = (total - 10).clip(0, 100)

        # 否决项
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        pct_chg = df.get("pct_chg", pd.Series(0, index=df.index))
        total.loc[turnover_rate < 1] = 0.0
        total.loc[pct_chg > 9] = 0.0

        total = total.clip(0, 100)
        total.name = self.name
        return total

    def describe(self, df: pd.DataFrame, scores: pd.Series, **context) -> Dict[str, List[str]]:
        if df.empty:
            return {}

        signals = self._compute_signals(df)

        signal_meta = [
            ("inflow", "资金流入"),
            ("volume_ratio", "放量启动"),
            ("turnover", "换手活跃"),
            ("pct_chg", "温和启动"),
        ]
        max_map = {"inflow": 35, "volume_ratio": 25, "turnover": 15, "pct_chg": 25}
        threshold = self._LABEL_THRESHOLD_RATIO

        inflow_raw = df.get("inflow_rate", pd.Series(0, index=df.index))
        vol_r = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        tr_r = df.get("turnover_rate", pd.Series(0, index=df.index))
        pct_r = df.get("pct_chg", pd.Series(0, index=df.index))

        reasons: Dict[str, List[str]] = {}
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            labels = []
            for key, label in signal_meta:
                val = signals[key].get(ts_code, 0.0)
                if val < max_map[key] * threshold:
                    continue
                if key == "inflow":
                    ir = inflow_raw.get(ts_code, 0)
                    labels.append(f"{label}(流入率{ir*100:.1f}%)")
                elif key == "volume_ratio":
                    vr = vol_r.get(ts_code, 1)
                    labels.append(f"{label}(量比{vr:.1f})")
                elif key == "turnover":
                    tr = tr_r.get(ts_code, 0)
                    labels.append(f"{label}({tr:.1f}%)")
                elif key == "pct_chg":
                    pct = pct_r.get(ts_code, 0)
                    labels.append(f"{label}(涨幅{pct:.1f}%)")
            if labels:
                reasons[ts_code] = labels
        return reasons

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

    @staticmethod
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

    @staticmethod
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

    # ------------------------------------------------------------------
    # Tier 1: East Money push2
    # ------------------------------------------------------------------

    def _fetch_tier1_eastmoney(self) -> Optional[pd.DataFrame]:
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
            logger.warning("[MomentumFactor] 东财 API 返回异常: rc=%s", data.get("rc"))
            return None

        items = data["data"].get("diff", [])
        if not items:
            return None

        raw = pd.DataFrame(items)
        return self._normalize_eastmoney(raw)

    @staticmethod
    def _normalize_eastmoney(df: pd.DataFrame) -> pd.DataFrame:
        """东财 push2 字段 → 统一列。"""
        df = df.copy()

        def _num(col, default=0):
            return pd.to_numeric(df.get(col, default), errors="coerce").fillna(default)

        result = pd.DataFrame(index=df.index)
        result["major_net"] = _num("f62", 0)
        result["lg_net"] = _num("f72", 0)
        result["inflow_rate"] = _num("f184", 0) / 100.0       # % → fraction
        result["pct_chg"] = _num("f3", 0)
        result["turnover_rate"] = _num("f8", 0)
        result["volume_ratio"] = _num("f10", 1.0)
        result["data_source"] = "eastmoney_push2"

        ts_codes = df["f12"].astype(str).str.zfill(6).apply(
            MomentumFactor._code_to_ts_code
        )
        result.index = ts_codes
        result = result[~result.index.duplicated(keep="first")]
        return result

    # ------------------------------------------------------------------
    # Tier 2: 同花顺 akshare
    # ------------------------------------------------------------------

    def _fetch_tier2_tonghuashun(self, trade_date: str) -> Optional[pd.DataFrame]:
        """akshare 同花顺个股资金流（即时），量比通过 DB 自算。"""
        import akshare as ak

        df = ak.stock_fund_flow_individual(symbol="即时")
        if df is None or df.empty:
            logger.warning("[MomentumFactor] Tier 2 同花顺返回空数据")
            return None

        result = self._normalize_tonghuashun(df)

        # 量比自算：realtime_spot.volume × 240/elapsed / avg_5d_volume
        try:
            result = self._compute_volume_ratio(result, trade_date)
        except Exception as e:
            logger.warning("[MomentumFactor] Tier 2 量比自算失败，使用默认 1.0: %s", e)

        return result

    @staticmethod
    def _parse_cn_amount(val) -> float:
        """解析中文金额字符串：'9937.50万'→99375000, '6.10亿'→610000000, '-8094.95万'→-80949500."""
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

    @staticmethod
    def _parse_cn_pct(val) -> float:
        """解析百分比字符串：'20.01%'→20.01, '-3.50%'→-3.50."""
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip().replace("%", "")
        try:
            return float(s) if s else 0.0
        except ValueError:
            return 0.0

    @staticmethod
    def _normalize_tonghuashun(df: pd.DataFrame) -> pd.DataFrame:
        """同花顺资金流字段 → 统一列。"""
        df = df.copy()

        # 列名容错：akshare 版本间可能有差异
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        pct_col = next((c for c in df.columns if "涨幅" in str(c) or "涨跌" in str(c)), None)
        hs_col = next((c for c in df.columns if "换手" in str(c)), None)
        net_col = next((c for c in df.columns if "净额" in str(c)), None)
        amt_col = next((c for c in df.columns if "成交额" in str(c)), None)

        def _parse_series(col, parser):
            if col is None:
                return pd.Series(0.0, index=df.index)
            return df[col].apply(parser)

        major_net = _parse_series(net_col, MomentumFactor._parse_cn_amount)
        amount = _parse_series(amt_col, MomentumFactor._parse_cn_amount)
        pct_chg = _parse_series(pct_col, MomentumFactor._parse_cn_pct)
        turnover_rate = _parse_series(hs_col, MomentumFactor._parse_cn_pct)

        # inflow_rate = 净额 / 成交额
        inflow_rate = (major_net / amount.replace(0, float("nan"))).fillna(0)

        code_series = df[code_col].astype(str).str.zfill(6) if code_col else pd.Series(index=df.index)

        result = pd.DataFrame({
            "major_net": major_net.values,
            "lg_net": 0.0,
            "inflow_rate": inflow_rate.values,
            "pct_chg": pct_chg.values,
            "turnover_rate": turnover_rate.values,
            "volume_ratio": 1.0,     # 先填默认，后续 _compute_volume_ratio 覆盖
            "data_source": "akshare_tonghuashun",
        }, index=[MomentumFactor._code_to_ts_code(c) for c in code_series])

        result = result[~result.index.duplicated(keep="first")]
        return result

    def _compute_volume_ratio(self, df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        """量比 = 当日预估全天量 / 过去5日均量。

        需要 realtime_spot（当日累计量）和 stock_daily（历史日量）。
        """
        from src.storage import DatabaseManager

        db = DatabaseManager()
        spot = db.get_realtime_spot()
        if spot is None or spot.empty or "volume" not in spot.columns:
            logger.debug("[MomentumFactor] realtime_spot 无数据，跳过量比自算")
            return df

        elapsed = self._trading_minutes_elapsed()
        if elapsed < 15:
            logger.debug("[MomentumFactor] 开盘不足 15 分钟，跳过量比自算")
            return df

        # 获取 5 日均量（不含当日）
        avg_vol = self._get_avg_volume(db, trade_date)
        if avg_vol is None or avg_vol.empty:
            return df

        # 两边均归一化为裸 6 位代码，避免 index 格式不一致导致 merge 静默失败
        def _bare(x):
            return str(x).split(".")[0].strip().zfill(6)

        df["_code"] = [_bare(x) for x in df.index]

        spot_vol = spot[["volume"]].copy()
        spot_vol["_code"] = [_bare(x) for x in spot_vol.index]
        spot_vol = spot_vol.rename(columns={"volume": "today_vol"})

        merged = df.merge(spot_vol[["_code", "today_vol"]], on="_code", how="left")

        avg_vol_df = avg_vol.reset_index()
        avg_vol_df.columns = ["raw_code", "avg_vol"]
        avg_vol_df["_code"] = [_bare(x) for x in avg_vol_df["raw_code"]]
        merged = merged.merge(avg_vol_df[["_code", "avg_vol"]], on="_code", how="left")

        has_data = merged["today_vol"].notna() & (merged["avg_vol"] > 0)
        est_vol = merged["today_vol"] * (240.0 / elapsed)
        merged.loc[has_data, "volume_ratio"] = (
            est_vol[has_data] / merged.loc[has_data, "avg_vol"]
        ).clip(lower=0)

        result = merged.drop(columns=["_code", "today_vol", "avg_vol"], errors="ignore")
        logger.debug(
            "[MomentumFactor] 量比自算完成: %d/%d 只有效",
            has_data.sum(), len(result),
        )
        return result

    @staticmethod
    def _get_avg_volume(db, trade_date: str, window: int = 5) -> Optional["pd.Series"]:
        """获取每只股票过去 window 个交易日的日均成交量（股）。"""
        from datetime import timedelta
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

    # ------------------------------------------------------------------
    # Tier 3: Tushare（原逻辑）
    # ------------------------------------------------------------------

    def _fetch_tier3_tushare(self, trade_date: str, **kwargs) -> Optional[pd.DataFrame]:
        """Tier 3: Tushare 资金流 + realtime_spot 实时指标（盘后兜底）。"""
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None

        mf = tushare_fetcher.get_bulk_money_flow(trade_date)
        if mf is None:
            return None

        result = mf.copy()

        # 从 realtime_spot 获取实时涨跌幅、换手率、量比（替代 stale Tushare daily_basic + limit_list）
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

        return self._normalize_tushare(result)

    @staticmethod
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

        df["major_net"] = major_net
        df["lg_net"] = lg_net
        df["inflow_rate"] = inflow_rate
        df["pct_chg"] = df.get("pct_chg", pd.Series(0, index=df.index))
        df["turnover_rate"] = df.get("turnover_rate", pd.Series(0, index=df.index))
        df["volume_ratio"] = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        df["data_source"] = "tushare"

        return df
