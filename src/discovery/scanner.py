# -*- coding: utf-8 -*-
"""盘中扫描器 (Intraday Scanner).

持久化守护进程：自动识别交易日和盘中时段（9:30-15:00），
非交易时段休眠等待，交易时段按 scan_interval_seconds 轮询扫描。

用法:
    scanner = IntradayScanner(config, engine)
    scanner.start()   # 阻塞循环，永久运行
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
import traceback
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set  # noqa: F811

import pandas as pd
import requests

from src.discovery.config import DiscoveryConfig, set_active_config, _load_runtime_state_into
from src.discovery.engine import StockDiscoveryEngine, is_trading_day
from src.discovery.factors.base import DiscoveryResult

logger = logging.getLogger(__name__)

_OUTPUT_PATH = "/tmp/discovery_top10.json"
_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports"

_TZ_CN = timezone(timedelta(hours=8))
_MARKET_OPEN = (9, 25)   # 盘中扫描开始
_MARKET_CLOSE = (15, 1)  # 盘中扫描结束（收盘后再扫一轮）


class IntradayScanner:
    """盘中实时扫描器（持久守护进程）。

    Attributes:
        config: 发现引擎配置
        engine: 已注册因子的发现引擎（含 tushare_fetcher）
        _previous: 上一轮 Top N 结果 (ts_code → rank)
        _round: 当前轮次计数
    """

    def __init__(self, config: DiscoveryConfig, engine: StockDiscoveryEngine):
        self.config = config
        self.engine = engine
        self._previous: Dict[str, int] = {}
        self._round = 0
        self._notified: Set[str] = set()  # 已通知过的 ts_code，避免重复推送
        self._last_realtime_slot: int = -1  # 上次写入 DB 的 slot
        self._last_limit_slot: int = -1  # 上次刷新 limit_pool 的 slot

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """启动盘中扫描轮询（阻塞，永久运行）。"""
        logger.info(
            "[Scanner] 盘中扫描守护进程启动, interval=%ss, top_n=%s",
            self.config.scan_interval_seconds,
            self.config.scan_top_n,
        )

        while True:
            try:
                self._wait_for_market_and_scan()
            except Exception as e:
                logger.warning("[Scanner] 扫描周期异常，60s 后重试: %s", e)
                time.sleep(60)

    # ------------------------------------------------------------------
    # Market timing
    # ------------------------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(_TZ_CN)

    @staticmethod
    def _now_str() -> str:
        return datetime.now(_TZ_CN).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _time_to(hour: int, minute: int) -> datetime:
        """返回今天指定时刻（北京时间），若已过则返回明天。"""
        now = datetime.now(_TZ_CN)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target

    def _is_trading_day(self) -> bool:
        return is_trading_day(self.engine)

    def _wait_for_market_and_scan(self) -> None:
        """等待到盘中交易时段，然后执行扫描循环。"""
        # Step 1: 等到下一个交易日
        while not self._is_trading_day():
            next_check = self._time_to(8, 0)
            wait_min = max(1, (next_check - self._now()).total_seconds() / 60)
            logger.info(
                "[Scanner] 非交易日，下次检查 %s (%.0f 分钟后)",
                next_check.strftime("%m-%d %H:%M"), wait_min,
            )
            time.sleep(min(3600, (next_check - self._now()).total_seconds()))

        # Step 1.5: 确保历史日K线数据完整（前 60 个交易日）
        self._ensure_daily_kline_complete()

        # Step 2: 等到盘中开盘
        market_open = self._now().replace(
            hour=_MARKET_OPEN[0], minute=_MARKET_OPEN[1], second=0, microsecond=0
        )
        if self._now() < market_open:
            wait_sec = (market_open - self._now()).total_seconds()
            logger.info(
                "[Scanner] 距开盘还有 %.0f 分钟，休眠至 %s",
                wait_sec / 60, market_open.strftime("%H:%M"),
            )
            time.sleep(wait_sec)

        # Step 3: 盘中扫描循环
        market_close = self._now().replace(
            hour=_MARKET_CLOSE[0], minute=_MARKET_CLOSE[1], second=0, microsecond=0
        )
        logger.info(
            "[Scanner] 进入盘中扫描时段 (%s → %s)",
            market_open.strftime("%H:%M"), market_close.strftime("%H:%M"),
        )

        self._round = 0
        self._previous = {}

        while self._now() < market_close:
            self._round += 1
            round_start = time.time()

            try:
                self._refresh_realtime_spot()
                self._refresh_limit_pool()
                results = self.engine.discover(mode="intraday")
                if results:
                    annotated = self._annotate_changes(results)
                    self._write_output(annotated, results)
                    self._print_round(annotated)
                    self._notify_new_stocks(annotated)
                    self._save_full_scan_to_db()
                    self._previous = {
                        r.ts_code: i for i, r in enumerate(results)
                    }
                elif self._round == 1 or self._previous:
                    # 结果为空时清空输出，避免前端展示过期数据
                    self._write_empty_output()
                    self._previous = {}
            except Exception as e:
                logger.warning("[Scanner] 本轮扫描异常: %s", e)
                logger.warning("[Scanner] 异常详情:\n%s", traceback.format_exc())

            # 对齐到固定时间点扫描（如 9:25, 9:30, 9:35...）
            # 计算从开盘到现在的间隔数，下一轮对准下一个整间隔
            interval = self.config.scan_interval_seconds
            now = self._now()
            elapsed_from_open = (now - market_open).total_seconds()
            next_round_idx = int(elapsed_from_open / interval) + 1
            next_target = market_open + timedelta(seconds=next_round_idx * interval)
            sleep_sec = max(1, min(
                (next_target - self._now()).total_seconds(),
                (market_close - self._now()).total_seconds(),
            ))
            if sleep_sec > 0:
                logger.debug(
                    "[Scanner] 第 %d 轮耗时 %.1fs, 下次扫描 %s (%.0fs 后)",
                    self._round, time.time() - round_start,
                    next_target.strftime("%H:%M:%S"), sleep_sec,
                )
                time.sleep(sleep_sec)

        logger.info("[Scanner] 盘中扫描结束（已收盘），共 %d 轮", self._round)

        # Step 3.5: 等待 Tushare 日线数据更新（~15:30），补全当日日K线
        data_ready = self._now().replace(hour=15, minute=30, second=0, microsecond=0)
        if self._now() < data_ready:
            wait = (data_ready - self._now()).total_seconds()
            logger.info("[Scanner] 等待日线数据更新，%ds 后同步...", int(wait))
            time.sleep(wait)
        self._ensure_daily_kline_complete()

        # Step 4: 收盘后休眠至下一交易日 8:00
        next_open = self._time_to(8, 0)
        wait_min = (next_open - self._now()).total_seconds() / 60
        logger.info("[Scanner] 休眠至 %s (%.0f 分钟)", next_open.strftime("%m-%d %H:%M"), wait_min)
        time.sleep((next_open - self._now()).total_seconds())

    def _refresh_realtime_spot(self) -> bool:
        """拉取最新实时行情并落库。

        与 RealtimeSpotProvider slot 对齐：同一 slot 内多次调用复用缓存，
        slot 变更时才写入 DB。返回 True 表示有更新。
        """
        try:
            from src.discovery.realtime_spot import get_provider
            from src.storage import DatabaseManager
            import time as _time
            provider = get_provider()
            df = provider.fetch()
            if df is None or df.empty:
                return False
            slot = int(_time.time() // 30)
            if slot == self._last_realtime_slot:
                return False
            if hasattr(df, "reset_index"):
                df = df.reset_index()
            source = provider._cache.get("source", "unknown")
            db = DatabaseManager()
            db.upsert_realtime_spot(df, source=source, slot=slot)
            self._last_realtime_slot = slot
            logger.debug("[Scanner] 实时行情落库: %d 条 (slot=%d, source=%s)", len(df), slot, source)
            return True
        except Exception as e:
            logger.warning("[Scanner] 实时行情刷新失败: %s", e)
            return False

    def _refresh_limit_pool(self) -> bool:
        """拉取最新涨停数据落库（每 60s 刷新，偶 30s slot）。

        3-tier fallback: akshare stock_zt_pool_em → realtime_spot DB → Tushare
        盘中 delete-then-insert（炸板退池自动清除），同时填充 SectorFactor.sector_map。
        """
        try:
            import time as _time
            from src.storage import DatabaseManager

            slot = int(_time.time() // 30)
            if slot % 2 != 0:
                return False
            if slot == self._last_limit_slot:
                return False

            db = DatabaseManager()
            today = date.today().strftime("%Y%m%d")

            df, source = self._fetch_limit_pool_akshare(today)
            if df is None or df.empty:
                df, source = self._fetch_limit_pool_realtime_spot(db)
            if df is None or df.empty:
                df, source = self._fetch_limit_pool_tushare(today)

            if df is None or df.empty:
                return False

            # ── 炸板检测（clear 之前完成新旧比对） ──
            self._detect_limit_breaks(db, df, today, source)

            db.clear_limit_pool_date(today)
            saved = db.insert_limit_pool_bulk(df, source=source, slot=slot)
            self._fill_sector_map_from_pool(df)
            self._last_limit_slot = slot
            logger.info("[Scanner] limit_pool 刷新: %d 条 (source=%s)", saved, source)
            return True
        except Exception as e:
            logger.warning("[Scanner] limit_pool 刷新失败: %s", e)
            return False

    @staticmethod
    def _detect_limit_breaks(db, df: pd.DataFrame, today: str, source: str) -> None:
        """差集检测炸板：history - current → limit_break，current - history → limit_up_history。"""
        from datetime import datetime as _dt
        now = _dt.now()

        if df is None:
            return
        if df.empty or "code" not in df.columns:
            current_codes: set = set()
        else:
            current_codes = set(df["code"].astype(str).str.strip().str.zfill(6))
        history_codes = db.get_limit_up_history_codes(today)
        broke_codes = db.get_limit_break_codes(today, status="broke")

        # 1) 新涨停票 → 补入 limit_up_history（统一 zfill 存入）
        new_codes = current_codes - history_codes
        if new_codes:
            raw_codes = df["code"].astype(str).str.strip().str.zfill(6)
            new_rows = df[raw_codes.isin(new_codes)].copy()
            new_df = pd.DataFrame()
            new_df["code"] = new_rows["code"].astype(str).str.strip().str.zfill(6)
            new_df["name"] = new_rows.get("name", "")
            new_df["trade_date"] = today
            new_df["open_times"] = new_rows.get("open_times", 0)
            new_df["limit_times"] = new_rows.get("limit_times", 0)
            new_df["sector"] = new_rows.get("sector", "")
            db.insert_limit_up_history_bulk(new_df, source=source)
            logger.info("[Scanner] 涨停历史补入 %d 只: %s", len(new_codes), new_codes)

        # 2) 回封检测：当前在涨停池 + 之前炸板 → 标记 recovered
        recovered_codes = list(current_codes & broke_codes)
        if recovered_codes:
            db.recover_limit_breaks(recovered_codes, today)
            logger.info("[Scanner] 回封 %d 只: %s", len(recovered_codes), recovered_codes)

        # 3) 炸板检测：history - current（曾涨停但当前不在）→ limit_break
        missing_codes = history_codes - current_codes
        if missing_codes:
            # 从 limit_up_history 带出完整字段
            hist_df = db.get_limit_up_history(today)
            code_to_lt = {}
            code_to_name = {}
            code_to_sector = {}
            code_to_ot = {}
            if not hist_df.empty:
                code_to_lt = hist_df["limit_times"].to_dict()
                if "name" in hist_df.columns:
                    code_to_name = hist_df["name"].to_dict()
                if "sector" in hist_df.columns:
                    code_to_sector = hist_df["sector"].to_dict()
                if "open_times" in hist_df.columns:
                    code_to_ot = hist_df["open_times"].to_dict()
            break_df = pd.DataFrame()
            break_df["code"] = list(missing_codes)
            break_df["name"] = [str(code_to_name.get(c, "")) for c in missing_codes]
            break_df["trade_date"] = today
            break_df["status"] = "broke"
            break_df["first_break_at"] = now
            break_df["limit_times"] = [int(code_to_lt.get(c, 0) or 0) for c in missing_codes]
            break_df["open_times"] = [int(code_to_ot.get(c, 0) or 0) for c in missing_codes]
            break_df["sector"] = [str(code_to_sector.get(c, "")) for c in missing_codes]
            break_df["source"] = source
            db.upsert_limit_break(break_df, source=source)
            logger.info("[Scanner] 检测到炸板 %d 只: %s", len(missing_codes), missing_codes)

    @staticmethod
    def _fetch_limit_pool_akshare(trade_date: str):
        """Tier 1: akshare stock_zt_pool_em → limit_pool 格式 DataFrame。"""
        try:
            import akshare as ak
            df = ak.stock_zt_pool_em(date=trade_date)
            if df is None or df.empty:
                return None, None
            df = df.copy()
            col_map = {
                "代码": "code", "名称": "name", "涨跌幅": "pct_chg",
                "最新价": "price", "连板数": "limit_times", "所属行业": "sector",
                "首次封板时间": "first_seal_time", "最后封板时间": "last_seal_time",
                "炸板次数": "break_count", "涨停统计": "limit_stats",
            }
            df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)
            df["code"] = df["code"].astype(str).str.strip().str.zfill(6)
            df["trade_date"] = trade_date
            for c in ("pct_chg", "price"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            for c in ("limit_times", "break_count"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
            return df, "akshare"
        except Exception:
            return None, None

    @staticmethod
    def _fetch_limit_pool_realtime_spot(db) -> tuple:
        """Tier 2: realtime_spot DB pct_chg >= 9.5% → limit_pool 格式 DataFrame。"""
        try:
            spot = db.get_realtime_spot()
            if spot is None or spot.empty:
                return None, None
            if "code" in spot.index.name or spot.index.name is None:
                pass
            pct = spot["pct_chg"]
            limit_up = spot[pct >= 9.5].copy()
            if limit_up.empty:
                return None, None
            today = date.today().strftime("%Y%m%d")
            out = pd.DataFrame()
            out["code"] = limit_up.index.astype(str).str.strip().str.zfill(6)
            out["name"] = limit_up.get("name", pd.Series("", index=limit_up.index)).values
            out["pct_chg"] = limit_up["pct_chg"].values
            out["price"] = limit_up.get("price", pd.Series(index=limit_up.index)).values
            out["trade_date"] = today
            return out, "realtime_spot"
        except Exception:
            return None, None

    def _fetch_limit_pool_tushare(self, trade_date: str) -> tuple:
        """Tier 3: Tushare get_limit_list(U) → limit_pool 格式 DataFrame。"""
        try:
            fetcher = getattr(self.engine, "tushare_fetcher", None)
            if fetcher is None:
                return None, None
            df = fetcher.get_limit_list(trade_date, limit_type="U")
            if df is None or df.empty:
                return None, None
            df = df.reset_index()
            out = pd.DataFrame()
            out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out["name"] = df.get("name", pd.Series("", index=df.index)).values if "name" in df.columns else ""
            out["trade_date"] = trade_date
            out["limit_type"] = df.get("limit_type", "U")
            for c in ("pct_chg", "limit_times", "open_times", "up_stat", "limit_stats"):
                if c in df.columns:
                    out[c] = df[c].values
            if "limit" in df.columns and "limit_stats" not in out.columns:
                out["limit_stats"] = df["limit"].values
            return out, "tushare"
        except Exception:
            return None, None

    def _fill_sector_map_from_pool(self, df) -> None:
        """从 limit_pool 数据填充 SectorFactor.sector_map。"""
        if df is None or df.empty or "sector" not in df.columns:
            return
        try:
            sf = self.engine.get_factor("sector")
            if sf is None or not hasattr(sf, "sector_map"):
                return
            for _, row in df.iterrows():
                code = str(row.get("code", "")).strip().zfill(6)
                sec = str(row.get("sector", "")).strip()
                if code and sec and sec not in ("nan", ""):
                    sf.sector_map[code] = sec
        except Exception:
            pass

    def _ensure_daily_kline_complete(self) -> None:
        """校验 stock_daily 是否有近 60 个交易日数据，缺失则自动补全。

        在每日首次进入扫描前调用一次（开盘前），全量同步耗时约 10~12 分钟。
        若数据已完整则秒级返回（仅查询 MAX(date)）。
        """
        try:
            from datetime import date as _date, timedelta
            from src.storage import DatabaseManager
            from sqlalchemy import text

            db = DatabaseManager()
            today = _date.today()
            cutoff = today - timedelta(days=75)

            with db.get_session() as s:
                max_date_raw = s.execute(text("SELECT MAX(date) FROM stock_daily")).scalar()
                stock_count = s.execute(text(
                    "SELECT COUNT(DISTINCT code) FROM stock_daily WHERE date >= :cutoff"
                ), {"cutoff": cutoff}).scalar()

            if isinstance(max_date_raw, str):
                max_date = _date.fromisoformat(max_date_raw)
            elif hasattr(max_date_raw, 'date'):
                max_date = max_date_raw.date()
            else:
                max_date = max_date_raw

            if max_date is not None and (today - max_date).days <= 1:
                logger.info(
                    "[Scanner] 日K线数据完整 (max_date=%s, %d stocks), 跳过同步",
                    max_date, stock_count,
                )
                return

            if max_date is None:
                logger.info("[Scanner] stock_daily 为空，开始全量同步日K线...")
            else:
                logger.info(
                    "[Scanner] 日K线数据滞后 (max_date=%s, today=%s), 开始同步...",
                    max_date, today,
                )

            fetcher = getattr(self.engine, "tushare_fetcher", None)
            if fetcher is None:
                logger.warning("[Scanner] 无 TushareFetcher，跳过日K线同步")
                return

            from scripts.sync_daily_kline import sync_all_daily, normalize_tushare_daily
            import pandas as pd

            trade_date = today.strftime("%Y%m%d")
            raw_dfs = sync_all_daily(fetcher, trade_date=trade_date, lookback_calendar_days=75)
            if not raw_dfs:
                logger.warning("[Scanner] 日K线同步无数据返回")
                return

            normalized = [d for d in (normalize_tushare_daily(df) for df in raw_dfs) if not d.empty]
            if not normalized:
                return
            merged = pd.concat(normalized, ignore_index=True)
            saved = db.save_daily_batch(merged, data_source="tushare_auto")
            logger.info("[Scanner] 日K线自动补全完成: %d 行", saved)
        except Exception as e:
            logger.warning("[Scanner] 日K线完整性校验失败 (fail-open): %s", e)

    def _notify_new_stocks(self, annotated: List[dict]) -> None:
        """飞书通知新上榜股票。同一股票每天只通知一次。"""
        url = self.config.feishu_webhook_url
        if not url:
            return

        new_entries = [e for e in annotated if e["change"] == "new" and e["ts_code"] not in self._notified]
        if not new_entries:
            return

        lines = ["**盘中扫描 — 新上榜股票**\n"]
        for e in new_entries:
            price_str = f"¥{e['price_at_discovery']:.2f}" if e.get("price_at_discovery") else "-"
            lines.append(
                f"- **{e['stock_name']}**({e['stock_code']}) "
                f"评分 {e['score']:.1f} | {price_str} | {e.get('discovered_at', '')}"
            )
            self._notified.add(e["ts_code"])

        content = "\n".join(lines)
        try:
            self._post_feishu_card(content)
        except Exception as e:
            logger.warning("[Scanner] 飞书通知发送失败: %s", e)

    def _post_feishu_card(self, content: str) -> None:
        """发送飞书交互卡片消息。"""
        url = self.config.feishu_webhook_url
        secret = self.config.feishu_webhook_secret

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": "盘中扫描发现"}
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": content}}
                ],
            },
        }

        if secret:
            timestamp = str(int(time.time()))
            sign = base64.b64encode(
                hmac.new(
                    f"{timestamp}\n{secret}".encode("utf-8"),
                    digestmod=hashlib.sha256,
                ).digest()
            ).decode("utf-8")
            payload["timestamp"] = timestamp
            payload["sign"] = sign

        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            result = resp.json()
            if result.get("code") == 0:
                logger.info("[Scanner] 飞书通知已发送，%d 只新股", len(self._notified))
            else:
                logger.warning("[Scanner] 飞书返回错误: %s", result.get("msg", ""))
        else:
            logger.warning("[Scanner] 飞书请求失败: HTTP %d", resp.status_code)

    # ------------------------------------------------------------------
    # Change detection
    # ------------------------------------------------------------------

    def _annotate_changes(self, results: List[DiscoveryResult]) -> List[dict]:
        """对比上一轮结果，标注变化。"""
        annotated = []
        current_codes = {r.ts_code for r in results}

        for i, r in enumerate(results):
            entry = {
                "rank": i + 1,
                "ts_code": r.ts_code,
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "score": r.score,
                "sector": r.sector,
                "factor_scores": r.factor_scores,
                "factor_weights": getattr(r, "factor_weights", {}),
                "reasons": r.reasons,
                "buy_price_low": r.buy_price_low,
                "buy_price_high": r.buy_price_high,
                "stop_loss": r.stop_loss,
                "take_profit_1": r.take_profit_1,
                "take_profit_2": r.take_profit_2,
                "discovered_at": r.discovered_at,
                "price_at_discovery": r.price_at_discovery,
                "change": "",
            }

            if r.ts_code not in self._previous:
                entry["change"] = "new"
            else:
                prev_rank = self._previous[r.ts_code]
                if i < prev_rank:
                    entry["change"] = "up"
                elif i > prev_rank:
                    entry["change"] = "down"

            annotated.append(entry)

        for ts_code, prev_rank in self._previous.items():
            if ts_code not in current_codes:
                annotated.append({
                    "rank": -1,
                    "ts_code": ts_code,
                    "stock_code": ts_code.split(".")[0] if "." in ts_code else ts_code,
                    "stock_name": "",
                    "score": 0,
                    "sector": "",
                    "factor_scores": {},
                    "factor_weights": {},
                    "change": "out",
                })

        return annotated

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    @staticmethod
    def _change_marker(change: str) -> str:
        _map = {"new": "🆕", "up": "⬆", "down": "⬇", "out": "➖"}
        return _map.get(change, "")

    def _print_round(self, annotated: List[dict]) -> None:
        """打印本轮扫描结果到日志。"""
        now = self._now().strftime("%H:%M:%S")
        lines = [f"[Scanner] Round {self._round} ({now}) Top {self.config.scan_top_n}:"]
        for entry in annotated:
            if entry["rank"] > 0:
                marker = self._change_marker(entry["change"])
                lines.append(
                    f"  {entry['rank']:2d}. {marker} {entry['stock_code']} "
                    f"{entry['stock_name']} ({entry['score']:.1f})"
                )
        for entry in annotated:
            if entry["rank"] < 0:
                lines.append(f"  {self._change_marker('out')} {entry['stock_code']} 退出榜单")
        logger.info("\n".join(lines))

    def _write_output(self, annotated: List[dict], results: List[DiscoveryResult]) -> None:
        """将 Top N 写入 JSON 文件供 WebUI 消费，同时落盘 Markdown 报告。"""
        try:
            os.makedirs(os.path.dirname(_OUTPUT_PATH), exist_ok=True)
            active = [e for e in annotated if e["rank"] > 0]
            payload = {
                "updated": datetime.now(timezone.utc).isoformat(),
                "round": self._round,
                "top_n": active,
                "dropped": [e for e in annotated if e["rank"] < 0],
            }
            with open(_OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("[Scanner] 写入 %s 失败: %s", _OUTPUT_PATH, e)

        # 落盘 Markdown 报告到 discovery_reports
        # 交易日 → 直接保存（供回测使用）；非交易日 → non_trading/ 子目录（仅展示，不回测）
        try:
            date_str = date.today().strftime('%Y%m%d')
            if self._is_trading_day():
                save_dir = _REPORTS_DIR
            else:
                save_dir = _REPORTS_DIR / "non_trading"
            save_dir.mkdir(parents=True, exist_ok=True)
            report = self.engine.format_report(results, mode="intraday")
            filepath = save_dir / f"intraday_{date_str}.md"
            filepath.write_text(report, encoding="utf-8")
            logger.debug("[Scanner] 盘中报告已保存: %s", filepath)

            # 同时落盘结构化 Top N JSON 供回测使用
            topn = []
            for i, r in enumerate(results, 1):
                topn.append({
                    "rank": i,
                    "stock_code": r.stock_code,
                    "stock_name": r.stock_name,
                    "score": r.score,
                    "sector": getattr(r, "sector", ""),
                    "factor_scores": getattr(r, "factor_scores", {}),
                    "factor_weights": getattr(r, "factor_weights", {}),
                    "reasons": getattr(r, "reasons", []),
                    "buy_price_low": getattr(r, "buy_price_low", None),
                    "buy_price_high": getattr(r, "buy_price_high", None),
                    "stop_loss": getattr(r, "stop_loss", None),
                    "take_profit_1": getattr(r, "take_profit_1", None),
                    "take_profit_2": getattr(r, "take_profit_2", None),
                })
            json_file = save_dir / f"intraday_{date_str}_topn.json"
            json_file.write_text(json.dumps(topn, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.debug("[Scanner] 盘中 TopN JSON 已保存: %s", json_file)
        except Exception as e:
            logger.warning("[Scanner] 保存盘中报告失败: %s", e)

    def _write_empty_output(self) -> None:
        """清空 Top N JSON，避免白名单无结果时前端展示过期数据。"""
        try:
            os.makedirs(os.path.dirname(_OUTPUT_PATH), exist_ok=True)
            payload = {
                "updated": datetime.now(timezone.utc).isoformat(),
                "round": self._round,
                "top_n": [],
                "dropped": [],
            }
            with open(_OUTPUT_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            # 同步清空 discovery_reports 下的 topn JSON
            if self._is_trading_day():
                save_dir = _REPORTS_DIR
            else:
                save_dir = _REPORTS_DIR / "non_trading"
            save_dir.mkdir(parents=True, exist_ok=True)
            date_str = date.today().strftime('%Y%m%d')
            json_file = save_dir / f"intraday_{date_str}_topn.json"
            json_file.write_text("[]", encoding="utf-8")

            logger.info("[Scanner] 本轮无符合条件的股票，已清空输出")
        except Exception as e:
            logger.warning("[Scanner] 写入空输出失败: %s", e)

    def _save_full_scan_to_db(self) -> None:
        """将本轮全市场评分落库（覆盖当日已有数据）。"""
        from src.storage import DatabaseManager

        df = getattr(self.engine, '_last_full_scan_df', None)
        if df is None or df.empty:
            return
        try:
            records = self.engine.get_last_full_scan_records(scan_round=self._round)
            if records:
                scan_date = getattr(self.engine, '_last_scan_trade_date', '')
                DatabaseManager().save_scan_results_intraday(records, scan_date)
        except Exception as e:
            logger.warning("[Scanner] 全量扫描结果落库失败: %s", e)


def refresh_limit_pool_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare limit_list_d 全量刷新 limit_pool（U/D/Z 三类全部）。

    Returns:
        落库条数，失败返回 0
    """
    import time as _time
    from datetime import date

    try:
        from src.storage import DatabaseManager

        today = date.today().strftime("%Y%m%d")

        # 拉取全部涨跌停（U/D/Z）
        df = tushare_fetcher.get_limit_list(today)
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 limit_pool 刷新: Tushare 无数据")
            return 0

        df = df.reset_index()
        out = pd.DataFrame()
        out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        out["name"] = df.get("name", pd.Series("", index=df.index)).values if "name" in df.columns else ""
        out["trade_date"] = today
        out["limit_type"] = df.get("limit", df.get("limit_type", ""))
        for c in ("pct_chg", "limit_times", "open_times", "up_stat"):
            if c in df.columns:
                out[c] = pd.to_numeric(df[c], errors="coerce") if c in ("pct_chg",) else df[c].values
        if "limit" in df.columns:
            out["limit_stats"] = df["limit"].values
        if "limit_times" in df.columns:
            out["limit_times"] = pd.to_numeric(out.get("limit_times", 0), errors="coerce").fillna(0).astype(int)
        if "open_times" in df.columns:
            out["open_times"] = pd.to_numeric(out.get("open_times", 0), errors="coerce").fillna(0).astype(int)

        db = DatabaseManager()
        slot = int(_time.time() // 30)
        saved = db.upsert_limit_pool(out, source="tushare", slot=slot)
        logger.info("[Scanner] 盘后 limit_pool 全量刷新: %d 条 (U/D/Z)", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 limit_pool 刷新失败: %s", e)
        return 0


def refresh_money_flow_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare moneyflow 全量刷新 money_flow 表。

    Returns:
        落库条数，失败返回 0
    """
    import time as _time

    try:
        from src.storage import DatabaseManager

        df = tushare_fetcher.get_bulk_money_flow()
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 money_flow 刷新: Tushare 无数据")
            return 0

        df = df.reset_index()
        out = pd.DataFrame()
        out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        out["name"] = df.get("name", pd.Series("", index=df.index)).values if "name" in df.columns else ""
        out["trade_date"] = df.get("trade_date", "")
        for c in ("buy_elg_amount", "sell_elg_amount", "buy_lg_amount",
                   "sell_lg_amount", "buy_md_amount", "sell_md_amount",
                   "buy_sm_amount", "sell_sm_amount", "net_mf_amount"):
            if c in df.columns:
                out[c] = pd.to_numeric(df[c], errors="coerce")

        db = DatabaseManager()
        saved = db.upsert_money_flow(out, source="tushare")
        logger.info("[Scanner] 盘后 money_flow 全量刷新: %d 条", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 money_flow 刷新失败: %s", e)
        return 0


def refresh_margin_detail_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare margin_detail 全量刷新 margin_detail 表。

    拉取最近 2 个交易日，覆盖边缘日期（如当天数据尚未完整发布时，
    次日再跑可补齐前一日的剩余股票）。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 margin_detail 刷新: 无交易日")
            return 0

        # 最近 2 个交易日，覆盖边缘日期的不完整数据
        target_dates = trade_dates[-2:] if len(trade_dates) >= 2 else trade_dates

        db = DatabaseManager()
        total_saved = 0
        for td in target_dates:
            df = tushare_fetcher.get_bulk_margin_detail(trade_date=td)
            if df is None or df.empty:
                logger.warning(f"[Scanner] 盘后 margin_detail 刷新: {td} 无数据")
                continue

            df = df.reset_index()
            out = pd.DataFrame()
            out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out["name"] = df.get("name", pd.Series("", index=df.index)).values if "name" in df.columns else ""
            out["trade_date"] = df.get("trade_date", td)
            for c in ("rzye", "rzmre", "rzche", "rqye", "rqmre", "rqyl"):
                if c in df.columns:
                    out[c] = pd.to_numeric(df[c], errors="coerce")

            saved = db.upsert_margin_detail(out, source="tushare")
            total_saved += saved
            logger.info(f"[Scanner] 盘后 margin_detail 刷新 {td}: {saved} 条")

        logger.info("[Scanner] 盘后 margin_detail 全量刷新: 合计 %d 条", total_saved)
        return total_saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 margin_detail 刷新失败: %s", e)
        return 0


def refresh_hm_detail_postmarket(tushare_fetcher, start: Optional[str] = None) -> int:
    """盘后用 Tushare hm_detail 刷新游资明细表。

    默认拉取最近 2 个交易日（日常增量），传入 start="20220801" 可全量回填。
    """
    try:
        from src.storage import DatabaseManager

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 hm_detail 刷新: 无交易日")
            return 0

        if start is not None:
            target_dates = sorted(d for d in trade_dates if d >= start)
        else:
            target_dates = trade_dates[-2:] if len(trade_dates) >= 2 else trade_dates

        if not target_dates:
            logger.warning("[Scanner] 盘后 hm_detail 刷新: 无目标日期")
            return 0

        db = DatabaseManager()
        total_saved = 0
        for i, td in enumerate(target_dates):
            df = tushare_fetcher.get_bulk_hm_detail(trade_date=td)
            if df is None or df.empty:
                continue

            df = df.reset_index()
            out = pd.DataFrame()
            out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            out["ts_name"] = df.get("ts_name", pd.Series("", index=df.index)).values if "ts_name" in df.columns else ""
            out["trade_date"] = df.get("trade_date", td)
            for c in ("buy_amount", "sell_amount", "net_amount"):
                if c in df.columns:
                    out[c] = pd.to_numeric(df[c], errors="coerce")
            out["hm_name"] = df.get("hm_name", pd.Series("", index=df.index)).values if "hm_name" in df.columns else ""
            out["hm_orgs"] = df.get("hm_orgs", pd.Series("", index=df.index)).values if "hm_orgs" in df.columns else ""

            saved = db.upsert_hm_detail(out, source="tushare")
            total_saved += saved
            logger.info("[Scanner] 盘后 hm_detail 刷新 %s: %d 条", td, saved)

        logger.info("[Scanner] 盘后 hm_detail 刷新完成: %d 天, 合计 %d 条", len(target_dates), total_saved)
        return total_saved
    except Exception as e:
        logger.warning("[Scanner] hm_detail 刷新失败: %s", e)
        return 0


def refresh_cyq_perf_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare cyq_perf 全量刷新筹码胜率表。

    拉取最近 2 个交易日，覆盖边缘日期的不完整数据。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 cyq_perf 刷新: 无交易日")
            return 0

        # 最近 2 个交易日
        target_dates = trade_dates[-2:] if len(trade_dates) >= 2 else trade_dates

        db = DatabaseManager()
        total_saved = 0
        for td in target_dates:
            df = tushare_fetcher.get_bulk_cyq_perf(trade_date=td)
            if df is None or df.empty:
                logger.warning(f"[Scanner] 盘后 cyq_perf 刷新: {td} 无数据")
                continue

            df = df.reset_index()
            df["trade_date"] = df.get("trade_date", td)
            numeric_cols = [
                "winner_rate", "cost_5pct", "cost_15pct", "cost_50pct",
                "cost_85pct", "cost_95pct", "weight_avg", "his_low", "his_high",
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            saved = db.upsert_cyq_perf(df, source="tushare")
            total_saved += saved
            logger.info(f"[Scanner] 盘后 cyq_perf 刷新 {td}: {saved} 条")

        logger.info("[Scanner] 盘后 cyq_perf 全量刷新: 合计 %d 条", total_saved)
        return total_saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 cyq_perf 刷新失败: %s", e)
        return 0


def refresh_insider_buy_postmarket(akshare_fetcher=None) -> int:
    """盘后用 akshare 拉取险资举牌数据并落库。"""
    try:
        from src.storage import DatabaseManager

        if akshare_fetcher is None:
            from data_provider.akshare_fetcher import AkshareFetcher
            akshare_fetcher = AkshareFetcher()

        raw = akshare_fetcher.get_insider_buy()
        if raw is None or raw.empty:
            logger.warning("[Scanner] 盘后 insider_buy 刷新: 无数据")
            return 0

        col_map = {
            "股票简称": "stock_name", "举牌公告日": "announce_date",
            "举牌方": "buyer", "增持数量": "buy_shares",
            "交易均价": "avg_price", "增持数量占总股本比例": "add_ratio",
            "变动后持股总数": "hold_shares", "变动后持股比例": "hold_ratio",
        }
        df = raw.rename(columns={k: v for k, v in col_map.items() if k in raw.columns})
        for c in ["add_ratio", "hold_ratio", "avg_price"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        db = DatabaseManager()
        saved = db.upsert_insider_buy(df, source="akshare")
        logger.info("[Scanner] 盘后 insider_buy 刷新: %d 条", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 insider_buy 刷新失败: %s", e)
        return 0


def run_intraday_scan(config: DiscoveryConfig, tushare_fetcher=None, akshare_fetcher=None) -> None:
    """一键启动盘中扫描（注册全部盘中因子）。"""
    from src.discovery.factors import (
        SectorFactor, MaEntryFactor, MomentumFactor, ReboundFactor,
        PopularityFactor,
    )

    engine = StockDiscoveryEngine(config, tushare_fetcher, akshare_fetcher)
    engine.register_factors([
        SectorFactor(),
        MaEntryFactor(),
        MomentumFactor(),
        ReboundFactor(),
        PopularityFactor(),
    ])

    set_active_config(config)
    _load_runtime_state_into(config)
    scanner = IntradayScanner(config, engine)
    scanner.start()


def ensure_postmarket_scan(
    tushare_fetcher, akshare_fetcher=None, force: bool = False
) -> Dict[str, Dict[str, Any]]:
    """确保当天盘后扫描已完成（数据刷新 + 因子评分 + 结果落库）。

    若当天已扫描过（scan_result_postmarket 有记录）则直接加载缓存；
    否则完整跑一遍 refresh → discover → 落库。

    Args:
        tushare_fetcher: TushareFetcher 实例
        akshare_fetcher: AkshareFetcher 实例（可选）
        force: 强制重新扫描，忽略已有缓存

    Returns:
        {stock_code: {score, factor_scores, reasons, ...}} 因子信号缓存
    """
    from datetime import date as dt_date

    from src.discovery.config import get_discovery_config
    from src.discovery.engine import StockDiscoveryEngine
    from src.discovery.factors import (
        MoneyFlowFactor, MarginFactor, ChipFactor,
        TechnicalFactor, LimitFactor,
        FundamentalFactor, PopularityFactor, HotMoneyFactor,
        NorthboundFactor, InstitutionHoldFactor, ProfitForecastFactor,
        PerformanceFactor, BuybackFactor, InsiderBuyFactor,
        BrokerRecommendFactor,
    )
    from src.storage import DatabaseManager

    today = dt_date.today().strftime("%Y%m%d")
    db = DatabaseManager()

    if not force and db.has_postmarket_scan_today(today):
        cached = db.load_factor_signals_for_date(today)
        if cached:
            logger.info("[Scanner] 今日已扫描，加载 %d 条缓存", len(cached))
            return cached

    logger.info("[Scanner] 开始完整盘后扫描 (date=%s)...", today)

    # ---- 数据刷新 ----
    refreshers = [
        ("limit_pool", lambda: refresh_limit_pool_postmarket(tushare_fetcher)),
        ("money_flow", lambda: refresh_money_flow_postmarket(tushare_fetcher)),
        ("margin_detail", lambda: refresh_margin_detail_postmarket(tushare_fetcher)),
        ("cyq_perf", lambda: refresh_cyq_perf_postmarket(tushare_fetcher)),
        ("insider_buy", lambda: refresh_insider_buy_postmarket()),
        ("hm_detail", lambda: refresh_hm_detail_postmarket(tushare_fetcher)),
    ]
    for name, fn in refreshers:
        try:
            fn()
        except Exception:
            logger.warning("[Scanner] %s 刷新失败，继续", name, exc_info=True)

    # 游资质量更新（hm_detail 有新数据才重算）
    try:
        from src.discovery.hm_tracker import HmTracker
        HmTracker(db).refresh_and_update()
    except Exception:
        logger.warning("[Scanner] hm_quality 更新失败，继续", exc_info=True)

    # ---- 因子评分 ----
    discovery_config = get_discovery_config()
    engine = StockDiscoveryEngine(discovery_config, tushare_fetcher, akshare_fetcher)
    engine.register_factors([
        MoneyFlowFactor(), MarginFactor(), ChipFactor(),
        TechnicalFactor(), LimitFactor(),
        FundamentalFactor(), PopularityFactor(), HotMoneyFactor(),
        NorthboundFactor(), InstitutionHoldFactor(), ProfitForecastFactor(),
        PerformanceFactor(), BuybackFactor(), InsiderBuyFactor(),
        BrokerRecommendFactor(),
    ])

    results = engine.discover(mode="postmarket")

    # ---- 结果落库 ----
    records = engine.get_last_full_scan_records()
    if records:
        try:
            db.save_scan_results_postmarket(records, today)
        except Exception:
            logger.warning("[Scanner] 全量评分落库失败", exc_info=True)

    # ---- 构建缓存（全量扫描记录，非仅 top-N 结果）----
    cache: Dict[str, Dict[str, Any]] = {}
    if records:
        for r in records:
            cache[r.get("stock_code", r.get("ts_code", ""))] = {
                "score": r.get("total_score", 0),
                "factor_scores": r.get("factor_scores", {}),
                "reasons": [],
                "buy_price_low": None,
                "buy_price_high": None,
                "stop_loss": None,
                "take_profit_1": None,
                "take_profit_2": None,
            }

    logger.info("[Scanner] 盘后扫描完成: %d 只候选股, %d 条缓存", len(results or []), len(cache))
    return cache
