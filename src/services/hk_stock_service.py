# -*- coding: utf-8 -*-
"""港股通成份股日线服务：成份快照、日 K 线查询、BOLL 叠加。"""

from __future__ import annotations

import logging
import math
import time as _time
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



def _fetch_hk_daily_from_sina(ak_module, fetcher, norm_code: str) -> Optional["pd.DataFrame"]:
    """从新浪 stock_hk_daily 获取港股日 K 线（主数据源）。"""
    import pandas as _pd
    for attempt in range(3):
        try:
            fetcher._set_random_user_agent()
            fetcher._enforce_rate_limit()
            df = ak_module.stock_hk_daily(symbol=norm_code, adjust="")
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            if attempt < 2:
                import time as _time
                _time.sleep(1.0)
            else:
                import logging as _logging
                _logging.getLogger(__name__).debug(
                    "[HkStock] sina fetch %s failed after 3 attempts: %s", norm_code, exc
                )
    return None


class HkStockService:
    """港股通成份股服务：成份列表 + 日 K 线 + BOLL。"""
    # 记录已成功回填到的目标交易日（新浪最新日）。同一目标日仅触发一次回填，
    # 进程重启后重置，因此长跑跨天时会自动对新交易日重新回填。
    _backfill_completed_for: Optional[str] = None

    def __init__(self, db: Optional[DatabaseManager] = None) -> None:
        self._db = db or DatabaseManager()
        self._boll_picks_cache: Optional[Dict[str, Any]] = None
        self._boll_picks_cache_ts: float = 0.0

    # ── 成份股列表 ──────────────────────────────────────────────

    def list_components(self) -> Dict[str, Any]:
        """返回所有港股通成份股快照（从 hk_ggt_component 表读取最新交易日）。

        仅查 DB，不触发网络回填或 BOLL 计算，确保接口快速返回。
        """
        now = _time.time()
        if hasattr(self, '_list_cache') and self._list_cache is not None and now - getattr(self, '_list_cache_ts', 0) < CACHE_TTL_SEC:
            return self._list_cache
        trade_date = self._db.get_latest_hk_ggt_trade_date()
        if not trade_date:
            return {"trade_date": "", "total": 0, "items": []}

        rows = self._db.list_hk_ggt_components(trade_date)
        entries: List[Tuple[Dict[str, Any], str, Optional[str]]] = []
        latest_trade_date: Optional[str] = None
        for r in rows:
            d = r.to_dict()
            code = _norm_hk_code(d["hk_code"])
            latest = self._db.get_latest_hk_stock_daily_trade_date(code)
            if latest and (latest_trade_date is None or latest > latest_trade_date):
                latest_trade_date = latest
            entries.append((d, code, latest))

        items = []
        for d, code, latest in entries:
            d["latest_price"] = None
            d["pct_change"] = None
            if latest_trade_date and latest == latest_trade_date:
                bars = self._db.list_hk_stock_daily_bars(
                    code,
                    start_date=latest_trade_date,
                    end_date=latest_trade_date,
                )
                if bars:
                    latest_bar = bars[-1]
                    close = _safe_float(getattr(latest_bar, "close", None))
                    d["latest_price"] = close
                    pct_chg = _safe_float(getattr(latest_bar, "pct_chg", None))
                    if pct_chg is not None:
                        d["pct_change"] = pct_chg
                    elif close is not None:
                        prev = self._db.list_hk_stock_daily_bars(
                            code,
                            start_date=_fmt_date(_parse_yyyymmdd(latest_trade_date) - timedelta(days=7)),
                            end_date=latest_trade_date,
                        )
                        prev_close = None
                        for pb in reversed(prev):
                            if pb.trade_date < latest_trade_date and pb.close is not None:
                                prev_close = pb.close
                                break
                        if prev_close and prev_close > 0:
                            d["pct_change"] = (close - prev_close) / prev_close * 100
            items.append(d)

        self._list_cache = {
            "trade_date": latest_trade_date or trade_date,
            "total": len(items),
            "items": items,
        }
        self._list_cache_ts = _time.time()
        return self._list_cache

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
        self._trigger_backfill_async()
        code = _norm_hk_code(hk_code)
        today = get_market_now("hk").date()
        end = _parse_yyyymmdd(end_date) if end_date else today
        start = _parse_yyyymmdd(start_date) if start_date else end - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        start_str = _fmt_date(start)
        end_str = _fmt_date(end)

        # 实时从接口拉取最近 2 个交易日数据覆盖 DB，确保盘后最终价格准确
        self._sync_latest_realtime(code)

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

    def _sync_latest_realtime(self, code: str) -> None:
        """实时拉取最近 2 个交易日数据覆盖 DB，确保盘后最终价格准确。"""
        import akshare as ak
        from data_provider.akshare_fetcher import AkshareFetcher
        fetcher = AkshareFetcher()
        df = self._fetch_tencent_hk_kline(code, days=5)
        if df is None or df.empty:
            df = _fetch_hk_daily_from_sina(ak, fetcher, code)
        if df is None or df.empty:
            return
        import pandas as _pd
        rows = []
        for _, row in df.iterrows():
            raw_date = row.get("date")
            if hasattr(raw_date, "strftime"):
                trade_date = raw_date.strftime("%Y%m%d")
            else:
                trade_date = str(raw_date).replace("-", "")[:8]
            close_val = _safe_float(row.get("close"))
            if close_val is None:
                continue
            rows.append({
                "hk_code": code,
                "trade_date": trade_date,
                "open": _safe_float(row.get("open")),
                "high": _safe_float(row.get("high")),
                "low": _safe_float(row.get("low")),
                "close": close_val,
                "volume": _safe_float(row.get("volume") if _pd.notna(row.get("volume")) else row.get("vol")),
            })
        if rows:
            saved = self._db.upsert_hk_stock_daily_bars(rows)
            logger.debug("[HkStock] sync_latest_realtime %s: %d bars", code, saved)

    def _trigger_backfill_async(self) -> None:
        """异步触发盘后回填（不阻塞当前请求），同一交易日仅触发一次。"""
        # 用腾讯00700快速获取最新交易日
        df = self._fetch_tencent_hk_kline("00700", days=5)
        if df is None or df.empty:
            return
        raw = df["date"].iloc[-1]
        latest_td = str(raw.strftime("%Y%m%d")) if hasattr(raw, "strftime") else str(raw).replace("-", "")[:8]
        if not latest_td:
            return
        # 幂等锁（持久化到 DB，进程重启不丢失）
        if self._db.get_hk_backfill_marker() >= latest_td:
            return
        # 只回填最新缺失的天数（2 个自然日），不做全量回填
        import threading as _th
        def _do():
            try:
                self.backfill_daily(end_date=latest_td)
                self._db.set_hk_backfill_marker(latest_td)
            except Exception:
                logger.debug("[HkStock] async backfill failed", exc_info=True)
        _th.Thread(target=_do, daemon=True).start()
        logger.info("[HkStock] async backfill triggered for %s", latest_td)

    def _auto_backfill_if_needed(self) -> None:
        """检查最新日 K，若成份股集合未同步到最新交易日则全量回填（同步阻塞）。
        
        仅用于需要同步等待回填完成的场景（如 _auto_backfill_if_needed 原调用方）。
        同一目标交易日仅触发一次回填，进程重启后自动重新检查。
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

            # 取最新交易日（优先用腾讯接口，实时性更好；失败则回退新浪）
            latest_source = self._fetch_tencent_hk_kline("00700", days=5)
            latest_td: Optional[str] = None
            if latest_source is not None and not latest_source.empty:
                raw = latest_source["date"].iloc[-1]
                if hasattr(raw, "strftime"):
                    latest_td = str(raw.strftime("%Y%m%d"))
                else:
                    latest_td = str(raw).replace("-", "")[:8]
            else:
                # 腾讯失败，回退新浪
                fetcher = AkshareFetcher()
                fetcher._set_random_user_agent()
                fetcher._enforce_rate_limit()
                df = ak.stock_hk_daily(symbol='00700', adjust='')
                if df is None or df.empty:
                    return
                raw_last = df['date'].iloc[-1]
                if hasattr(raw_last, 'strftime'):
                    latest_td = str(raw_last.strftime("%Y%m%d"))
                else:
                    latest_td = str(raw_last).replace('-', '')[:8]

            if not latest_td:
                return

            # 同一目标日仅回填一次（幂等锁）
            if HkStockService._backfill_completed_for == latest_td:
                return

            # 取成份股集合在 DB 中的最小「最新交易日」
            db_min_latest: Optional[str] = None
            for code in codes:
                norm = _norm_hk_code(code)
                latest = self._db.get_latest_hk_stock_daily_trade_date(norm)
                if not latest:
                    db_min_latest = ""
                    break
                if db_min_latest is None or latest < db_min_latest:
                    db_min_latest = latest

            if db_min_latest and db_min_latest >= latest_td:
                HkStockService._backfill_completed_for = latest_td
                return

            logger.info(
                "[HkStock] auto-backfill: latest=%s, db min latest=%s, codes=%d",
                latest_td, db_min_latest or "<empty>", len(codes),
            )
            self.backfill_daily(start_date=db_min_latest or None, end_date=latest_td)
            HkStockService._backfill_completed_for = latest_td
        except Exception as exc:
            logger.debug("[HkStock] auto-backfill check failed: %s", exc)

    def scan_boll_picks(
        self,
        near_pct: float = BOLL_NEAR_PCT,
    ) -> Dict[str, Any]:
        """扫描港股通成份股，找出收盘价靠近 BOLL 上轨/中轨/下轨 ±near_pct% 的个股。

        优化：一次批量拉取所有日 K 线 + 成份快照，内存内计算，避免 ~800 次独立 DB 查询。
        """
        # 30 秒内存缓存，避免高频请求穿透
        now = _time.time()
        if self._boll_picks_cache is not None and now - self._boll_picks_cache_ts < CACHE_TTL_SEC:
            return self._boll_picks_cache
        self._trigger_backfill_async()
        trade_date = self._db.get_latest_hk_ggt_trade_date()
        if not trade_date:
            return {"near_pct": near_pct, "upper": [], "mid": [], "lower": []}

        comp_rows = self._db.list_hk_ggt_components(trade_date)
        if not comp_rows:
            return {"near_pct": near_pct, "upper": [], "mid": [], "lower": []}

        codes: List[str] = []
        code_names: Dict[str, str] = {}
        for r in comp_rows:
            d = r.to_dict()
            c = _norm_hk_code(d["hk_code"])
            codes.append(c)
            code_names[c] = d.get("name") or c

        # 一次批量拉取所有成份股的日 K 线（最新 180 天）
        today = date.today()
        start = _fmt_date(today - timedelta(days=DEFAULT_LOOKBACK_DAYS))
        end = _fmt_date(today)
        batch = self._db.list_hk_stock_daily_bars_batch(codes, start_date=start, end_date=end)

        upper_picks: List[Dict[str, Any]] = []
        mid_picks: List[Dict[str, Any]] = []
        lower_picks: List[Dict[str, Any]] = []

        for code in codes:
            bars = batch.get(code, [])
            closes = [b.close for b in bars if b.close is not None]
            if len(closes) < BOLL_PERIOD:
                continue
            boll = _compute_boll_realtime(closes)
            if not boll:
                continue
            close, mid, upper, lower = boll
            name = code_names.get(code, code)
            _slope = _mid_slope(closes)

            pick = {
                "hk_code": code,
                "name": name,
                "close": close,
                "boll_mid": mid,
                "boll_upper": upper,
                "boll_lower": lower,
            }

            if close >= upper or _is_near_band(close, upper, near_pct):
                p = dict(pick, band="upper", dist_pct=_band_distance_pct(close, upper), mid_slope=_slope)
                upper_picks.append(p)
            if _is_near_band(close, mid, near_pct):
                p = dict(pick, band="mid", dist_pct=_band_distance_pct(close, mid), mid_slope=_slope)
                mid_picks.append(p)
            if _is_near_band(close, lower, near_pct):
                p = dict(pick, band="lower", dist_pct=_band_distance_pct(close, lower), mid_slope=_slope)
                lower_picks.append(p)

        def _sort_key(item: Dict[str, Any]) -> tuple:
            slope = item.get("mid_slope")
            if slope is None:
                return (1, 0.0)
            return (0, -slope)

        result = {
            "near_pct": near_pct,
            "upper": sorted(upper_picks, key=_sort_key),
            "mid": sorted(mid_picks, key=_sort_key),
            "lower": sorted(lower_picks, key=_sort_key),
        }
        self._boll_picks_cache = result
        self._boll_picks_cache_ts = now
        return result

    # ── 数据回填 ──────────────────────────────────────────────────

    def _fetch_tencent_hk_kline(self, hk_code: str, days: int = 180) -> Optional["pd.DataFrame"]:
        """用腾讯接口获取港股日 K 线作为 fallback 数据源。"""
        import pandas as _pd
        import requests as _requests

        norm = _norm_hk_code(hk_code)
        url = f"http://web.ifzq.gtimg.cn/appstock/app/kline/kline?param=hk{norm},day,,,{days}"
        try:
            r = _requests.get(url, timeout=10,
                              headers={"User-Agent": "Mozilla/5.0"},
                              proxies={"http": None, "https": None})
            if r.status_code != 200:
                return None
            data = r.json()
            if data.get("code") != 0:
                return None
            day_data = (data.get("data") or {}).get(f"hk{norm}", {}).get("day", [])
            if not day_data:
                return None
            rows = []
            for row in day_data:
                dt = row[0].replace("-", "")[:8] if len(row) > 0 else None
                if not dt:
                    continue
                rows.append({
                    "date": row[0],
                    "open": float(row[1]) if len(row) > 1 else None,
                    "close": float(row[2]) if len(row) > 2 else None,
                    "high": float(row[3]) if len(row) > 3 else None,
                    "low": float(row[4]) if len(row) > 4 else None,
                    "vol": float(row[5]) if len(row) > 5 else None,
                    "volume": float(row[5]) if len(row) > 5 else None,
                })
            return _pd.DataFrame(rows)
        except Exception as exc:
            logger.debug("[HkStock] tencent fallback %s failed: %s", norm, exc)
            return None

    def backfill_daily(
        self,
        codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """回填港股通成份股日线数据。

        数据源优先级：腾讯 kline 接口 → 新浪 stock_hk_daily。
        腾讯响应快（~0.2s/只）、实时性好（收盘后即有今日数据），新浪兜底。
        """
        import akshare as ak
        import pandas as pd

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

        from concurrent.futures import ThreadPoolExecutor, as_completed
        from data_provider.akshare_fetcher import AkshareFetcher
        fetcher = AkshareFetcher()

        # 并发拉取：优先腾讯（~20并发），失败单只回退新浪
        def _fetch_single(norm_code: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
            df = self._fetch_tencent_hk_kline(norm_code, days=180)
            source = "tencent"
            if df is None or df.empty:
                df = _fetch_hk_daily_from_sina(ak, fetcher, norm_code)
                source = "sina"
            if df is None or df.empty:
                return (None, [])
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
                volume_val = _safe_float(row.get("volume") if pd.notna(row.get("volume")) else row.get("vol"))
                rows.append({
                    "hk_code": norm_code,
                    "trade_date": trade_date,
                    "open": open_val,
                    "high": _safe_float(row.get("high")),
                    "low": _safe_float(row.get("low")),
                    "close": close_val,
                    "volume": volume_val,
                })
            return (source, rows)

        total = 0
        normed_codes = [_norm_hk_code(c) for c in codes]
        # 腾讯一次 HTTP ~0.2s，20 并发 ≈ 800 只在 8 秒完成
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {pool.submit(_fetch_single, code): code for code in normed_codes}
            for fut in as_completed(futures):
                code = futures[fut]
                try:
                    source, rows = fut.result()
                    if rows:
                        saved = self._db.upsert_hk_stock_daily_bars(rows)
                        total += saved
                        logger.debug("[HkStock] backfill %s (%s): %d bars", code, source, saved)
                except Exception as exc:
                    logger.debug("[HkStock] backfill %s failed: %s", code, exc)
        logger.info("[HkStock] backfill done: %d codes, %d bars (concurrent)", len(codes), total)
        return total
