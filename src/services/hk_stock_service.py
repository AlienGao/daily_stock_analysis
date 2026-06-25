# -*- coding: utf-8 -*-
"""港股通成份股日线服务：成份快照、日 K 线查询、BOLL 叠加。"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

BOLL_PERIOD = 20
BOLL_MULT = 2.0
BOLL_NEAR_PCT = 1.5

from src.config import get_config
from src.core.trading_calendar import get_market_now
from src.storage import DatabaseManager

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 180
CACHE_TTL_SEC = 300


def _compute_boll_realtime(
    closes: List[float],
    period: int = BOLL_PERIOD,
    mult: float = BOLL_MULT,
) -> Optional[Tuple[float, float, float, float]]:
    """Return (close, mid, upper, lower) for last bar."""
    valid = [float(c) for c in closes if c is not None and math.isfinite(c)]
    if len(valid) < period:
        return None
    window = valid[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period
    std = math.sqrt(variance)
    lower = mid - mult * std
    upper = mid + mult * std
    close = valid[-1]
    return close, mid, upper, lower


def _band_distance_pct(close: float, band: float) -> Optional[float]:
    if not math.isfinite(close) or not math.isfinite(band) or band <= 0:
        return None
    return round((close - band) / band * 100, 2)


def _is_near_band(close: float, band: float, near_pct: float = BOLL_NEAR_PCT) -> bool:
    dist = _band_distance_pct(close, band)
    return dist is not None and abs(dist) <= near_pct


def _mid_slope(closes: List[float], period: int = BOLL_PERIOD, lookback: int = 3) -> Optional[float]:
    """计算 BOLL 中轨在最近 lookback 个有效值上的平均斜率（正值=上移）。"""
    valid = [float(c) for c in closes if c is not None and math.isfinite(c)]
    if len(valid) < period + lookback - 1:
        return None
    mids = []
    for i in range(len(valid) - lookback + 1, len(valid) + 1):
        if i < period:
            return None
        window = valid[i - period:i]
        mids.append(sum(window) / period)
    if len(mids) < 2:
        return None
    # 线性回归斜率简化为首尾差 / (n-1)
    slope = (mids[-1] - mids[0]) / (len(mids) - 1)
    return round(slope, 4)


def _norm_hk_code(code: str) -> str:
    return str(code or "").lower().replace("hk", "").zfill(5)


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else None
    except (ValueError, TypeError):
        return None


def _fmt_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(str(s).replace("-", "")[:8], "%Y%m%d").date()


def _compute_boll(
    closes: List[float],
    period: int = 20,
    multiplier: float = 2.0,
) -> List[Tuple[Optional[float], Optional[float], Optional[float]]]:
    """返回 [(mid, upper, lower)] 与 closes 等长，前 period-1 个为 None。"""
    result: List[Tuple[Optional[float], Optional[float], Optional[float]]] = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append((None, None, None))
            continue
        sl = closes[i - period + 1 : i + 1]
        mid = sum(sl) / period
        variance = sum((x - mid) ** 2 for x in sl) / period
        std = math.sqrt(variance)
        upper = mid + multiplier * std
        lower = mid - multiplier * std
        result.append((mid, upper, lower))
    return result


class HkStockService:
    """港股通成份股服务：成份列表 + 日 K 线 + BOLL。"""
    # 记录已成功回填到的目标交易日（新浪最新日）。同一目标日仅触发一次回填，
    # 进程重启后重置，因此长跑跨天时会自动对新交易日重新回填。
    _backfill_completed_for: Optional[str] = None

    def __init__(self, db: Optional[DatabaseManager] = None) -> None:
        self._db = db or DatabaseManager()

    # ── 成份股列表 ──────────────────────────────────────────────

    def list_components(self) -> Dict[str, Any]:
        """返回所有港股通成份股快照（从 hk_ggt_component 表读取最新交易日），按 BOLL 中轨斜率降序排列。"""
        trade_date = self._db.get_latest_hk_ggt_trade_date()
        if not trade_date:
            return {"trade_date": "", "total": 0, "items": []}

        rows = self._db.list_hk_ggt_components(trade_date)
        items = []
        for r in rows:
            d = r.to_dict()
            # 补充最新价/涨跌幅（从日线表获取最新收盘）
            code = _norm_hk_code(d["hk_code"])
            latest = self._get_latest_price(code)
            if latest:
                d.update(latest)
            # 计算 BOLL 中轨斜率
            try:
                klines = self.get_klines(code)
                data = klines.get("data", [])
                closes = [x["close"] for x in data if x.get("close") is not None]
                slope = _mid_slope(closes)
                d["mid_slope"] = slope
            except Exception:
                d["mid_slope"] = None
            items.append(d)

        # 按中轨斜率降序排列（slope=None 排最后）
        items.sort(key=lambda x: (0 if (x.get("mid_slope") or 0) > 0 else 1, -(x.get("mid_slope") or 0)))

        return {
            "trade_date": trade_date,
            "total": len(items),
            "items": items,
        }

    def _get_latest_price(self, hk_code: str) -> Optional[Dict[str, Any]]:
        """从日线表获取最新收盘价和涨跌幅。"""
        bar = self._db.get_latest_hk_stock_daily_trade_date(hk_code)
        if not bar:
            return None
        bars = self._db.list_hk_stock_daily_bars(hk_code, start_date=bar, end_date=bar)
        if not bars:
            return None
        latest = bars[-1]
        prev_date = _fmt_date(
            _parse_yyyymmdd(latest.trade_date) - timedelta(days=7)
        )
        prev_bars = self._db.list_hk_stock_daily_bars(
            hk_code, start_date=prev_date, end_date=latest.trade_date
        )
        prev_close: Optional[float] = None
        for pb in reversed(prev_bars):
            if pb.trade_date < latest.trade_date and pb.close is not None:
                prev_close = pb.close
                break
        pct_chg = None
        if prev_close and prev_close > 0 and latest.close:
            pct_chg = (latest.close - prev_close) / prev_close * 100
        return {
            "latest_price": latest.close,
            "pct_change": pct_chg,
        }

    # ── 日 K 线 ──────────────────────────────────────────────────

    def get_klines(
        self,
        hk_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取港股通个股日 K 线（复权价），叠加 BOLL。"""
        code = _norm_hk_code(hk_code)
        today = get_market_now("hk").date()
        end = _parse_yyyymmdd(end_date) if end_date else today
        start = _parse_yyyymmdd(start_date) if start_date else end - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        start_str = _fmt_date(start)
        end_str = _fmt_date(end)

        bars = self._db.list_hk_stock_daily_bars(code, start_date=start_str, end_date=end_str)
        raw: List[Dict[str, Any]] = [b.to_dict() for b in bars]

        closes = [b["close"] for b in raw if b.get("close") is not None]
        if not closes:
            return {"hk_code": code, "start_date": start_str, "end_date": end_str, "data": []}

        boll = _compute_boll(closes)
        boll_idx = 0
        data = []
        for b in raw:
            close = b.get("close")
            if close is None:
                continue
            mid, upper, lower = boll[boll_idx] if boll_idx < len(boll) else (None, None, None)
            boll_idx += 1
            data.append({
                "date": b["trade_date"],
                "open": b.get("open"),
                "high": b.get("high"),
                "low": b.get("low"),
                "close": close,
                "volume": b.get("volume"),
                "boll_mid": mid,
                "boll_upper": upper,
                "boll_lower": lower,
            })

        return {
            "hk_code": code,
            "start_date": start_str,
            "end_date": end_str,
            "data": data,
        }

    # ── BOLL 推荐 ──────────────────────────────────────────────

    def _auto_backfill_if_needed(self) -> None:
        """检查新浪最新日 K，若成份股集合未同步到新浪最新交易日则全量回填。

        判定依据：以成份股集合在 DB 中的「最小最新交易日」与新浪最新交易日对比。
        只要有任意一只成份股落后于新浪最新日，就对该交易日执行全量成份股回填。
        同一目标交易日（新浪最新日）仅触发一次回填，进程重启后自动重新检查。
        """
        try:
            import akshare as ak
            from data_provider.akshare_fetcher import AkshareFetcher

            trade_date = self._db.get_latest_hk_ggt_trade_date()
            if not trade_date:
                logger.debug("[HkStock] auto-backfill skipped: no ggt component snapshot")
                return
            codes = self._db.list_hk_ggt_codes_for_date(trade_date)
            if not codes:
                logger.debug("[HkStock] auto-backfill skipped: empty ggt code list for %s", trade_date)
                return

            # 新浪最新交易日（用 00700 作为风向标，覆盖率最稳定）
            fetcher = AkshareFetcher()
            fetcher._set_random_user_agent()
            fetcher._enforce_rate_limit()
            df = ak.stock_hk_daily(symbol='00700', adjust='')
            if df is None or df.empty:
                return
            raw_last = df['date'].iloc[-1]
            if hasattr(raw_last, 'strftime'):
                latest_sina = str(raw_last.strftime("%Y%m%d"))
            else:
                latest_sina = str(raw_last).replace('-', '')[:8]
            if not latest_sina:
                return

            # 同一目标日仅回填一次（幂等锁）
            if HkStockService._backfill_completed_for == latest_sina:
                return

            # 取成份股集合在 DB 中的最小「最新交易日」：只要有一只落后于新浪最新日，
            # 就说明集合整体需要补数据，触发一次全量回填。
            db_min_latest: Optional[str] = None
            for code in codes:
                norm = _norm_hk_code(code)
                latest = self._db.get_latest_hk_stock_daily_trade_date(norm)
                if not latest:
                    db_min_latest = ""  # 存在完全无数据的成份股，必须回填
                    break
                if db_min_latest is None or latest < db_min_latest:
                    db_min_latest = latest

            if db_min_latest and db_min_latest >= latest_sina:
                # 全员已同步到新浪最新日，记录幂等标记后返回
                HkStockService._backfill_completed_for = latest_sina
                return

            logger.info(
                "[HkStock] auto-backfill: sina latest=%s, db min latest=%s, codes=%d",
                latest_sina, db_min_latest or "<empty>", len(codes),
            )
            self.backfill_daily(start_date=db_min_latest or None, end_date=latest_sina)
            HkStockService._backfill_completed_for = latest_sina
        except Exception as exc:
            logger.debug("[HkStock] auto-backfill check failed: %s", exc)

    def scan_boll_picks(
        self,
        near_pct: float = BOLL_NEAR_PCT,
    ) -> Dict[str, Any]:
        """扫描港股通成份股，找出收盘价靠近 BOLL 上轨/中轨/下轨 ±near_pct% 的个股。"""
        self._auto_backfill_if_needed()
        comps = self.list_components()
        codes: List[str] = []
        code_names: Dict[str, str] = {}
        for it in comps.get("items", []):
            c = _norm_hk_code(it["hk_code"])
            codes.append(c)
            code_names[c] = it.get("name") or c

        upper_picks: List[Dict[str, Any]] = []
        mid_picks: List[Dict[str, Any]] = []
        lower_picks: List[Dict[str, Any]] = []

        for code in codes:
            klines = self.get_klines(code)
            data = klines.get("data", [])
            closes = [d["close"] for d in data if d.get("close") is not None]
            if len(closes) < BOLL_PERIOD:
                continue
            boll = _compute_boll_realtime(closes)
            if not boll:
                continue
            close, mid, upper, lower = boll
            name = code_names.get(code, code)

            _slope = _mid_slope(closes)

            if close >= upper or _is_near_band(close, upper, near_pct):
                upper_picks.append({
                    "hk_code": code,
                    "name": name,
                    "close": close,
                    "band": "upper",
                    "boll_mid": mid,
                    "boll_upper": upper,
                    "boll_lower": lower,
                    "dist_pct": _band_distance_pct(close, upper),
                    "mid_slope": _slope,
                })
            if _is_near_band(close, mid, near_pct):
                mid_picks.append({
                    "hk_code": code,
                    "name": name,
                    "close": close,
                    "band": "mid",
                    "boll_mid": mid,
                    "boll_upper": upper,
                    "boll_lower": lower,
                    "dist_pct": _band_distance_pct(close, mid),
                    "mid_slope": _slope,
                })
            if _is_near_band(close, lower, near_pct):
                lower_picks.append({
                    "hk_code": code,
                    "name": name,
                    "close": close,
                    "band": "lower",
                    "boll_mid": mid,
                    "boll_upper": upper,
                    "boll_lower": lower,
                    "dist_pct": _band_distance_pct(close, lower),
                    "mid_slope": _slope,
                })

        def _sort_key(item: Dict[str, Any]) -> tuple:
            """各轨道内按中轨斜率降序（斜率越大越靠前），slope=None 排最后。"""
            slope = item.get("mid_slope")
            if slope is None:
                return (1, 0.0)
            return (0, -slope)

        return {
            "near_pct": near_pct,
            "upper": sorted(upper_picks, key=_sort_key),
            "mid": sorted(mid_picks, key=_sort_key),
            "lower": sorted(lower_picks, key=_sort_key),
        }

    # ── 数据回填 ──────────────────────────────────────────────────

    def backfill_daily(
        self,
        codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """用新浪 stock_hk_daily 回填港股通成份股日线数据。"""
        import akshare as ak

        today = get_market_now("hk").date()
        end = _parse_yyyymmdd(end_date) if end_date else today
        start = _parse_yyyymmdd(start_date) if start_date else end - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        start_str = _fmt_date(start)
        end_str = _fmt_date(end)

        if not codes:
            trade_date = self._db.get_latest_hk_ggt_trade_date()
            if not trade_date:
                return 0
            codes = self._db.list_hk_ggt_codes_for_date(trade_date)

        from data_provider.akshare_fetcher import AkshareFetcher
        fetcher = AkshareFetcher()

        total = 0
        for code in codes:
            norm = _norm_hk_code(code)
            try:
                fetcher._set_random_user_agent()
                fetcher._enforce_rate_limit()
                df = ak.stock_hk_daily(symbol=norm, adjust="")
            except Exception as exc:
                logger.warning("[HkStock] backfill %s failed: %s", norm, exc)
                continue
            if df is None or df.empty:
                continue
            rows = []
            for _, row in df.iterrows():
                raw_date = row.get("date")
                if hasattr(raw_date, "strftime"):
                    trade_date = raw_date.strftime("%Y%m%d")
                else:
                    trade_date = str(raw_date).replace("-", "")[:8]
                if not trade_date or trade_date < start_str or trade_date > end_str:
                    continue
                close_val = _safe_float(row.get("close"))
                if close_val is None:
                    continue
                open_val = _safe_float(row.get("open"))
                volume_val = _safe_float(row.get("vol", row.get("volume")))
                rows.append({
                    "hk_code": norm,
                    "trade_date": trade_date,
                    "open": open_val,
                    "high": _safe_float(row.get("high")),
                    "low": _safe_float(row.get("low")),
                    "close": close_val,
                    "volume": volume_val,
                })
            if rows:
                saved = self._db.upsert_hk_stock_daily_bars(rows)
                total += saved
                logger.info("[HkStock] backfill %s: %d bars", norm, saved)
        logger.info("[HkStock] backfill done: %d codes, %d bars", len(codes), total)
        return total
