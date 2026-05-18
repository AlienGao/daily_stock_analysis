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
_MIDDAY_BREAK_START = (11, 30)
_MIDDAY_BREAK_END = (13, 0)


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
            now = self._now()
            midday_start = now.replace(
                hour=_MIDDAY_BREAK_START[0], minute=_MIDDAY_BREAK_START[1],
                second=0, microsecond=0,
            )
            midday_end = now.replace(
                hour=_MIDDAY_BREAK_END[0], minute=_MIDDAY_BREAK_END[1],
                second=0, microsecond=0,
            )
            if midday_start <= now < midday_end:
                sleep_sec = (midday_end - now).total_seconds()
                logger.info(
                    "[Scanner] 午间休市，暂停扫描 %.0f 分钟，%s 恢复",
                    sleep_sec / 60, midday_end.strftime("%H:%M"),
                )
                time.sleep(sleep_sec)
                continue

            self._round += 1
            round_start = time.time()

            try:
                self._refresh_realtime_spot()
                self._refresh_limit_pool()
                # 跨进程同步：API 进程可能已切换扫描模式，重新加载运行时状态
                _load_runtime_state_into(self.config)
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
        盘中 upsert（name 为空不覆盖旧值），差集清理退池股票。
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

            # ── 板块分类：优先保留 akshare 同花顺行业，缺失时用申万填充 ──
            if "sector" not in df.columns:
                df["sector"] = pd.Series("", index=df.index)
            needs_sector = df["sector"].isna() | (
                df["sector"].astype(str).str.strip().isin(["", "nan"])
            )
            if needs_sector.any():
                try:
                    ths_map = db.get_ths_industry_map()
                    if ths_map:
                        sw = df["code"].map(ths_map)
                        df.loc[needs_sector, "sector"] = sw[needs_sector].fillna("")
                except Exception:
                    pass

            # ── 炸板检测（upsert 之前完成新旧比对） ──
            self._detect_limit_breaks(db, df, today, source)

            # upsert：新记录写入，已有记录更新；name 为空时保留旧值
            saved = db.upsert_limit_pool(df, source=source, slot=slot)

            # ── 清理退池股票（DB 中有但新数据中无的 code） ──
            new_codes = set(df["code"].astype(str).str.strip().str.zfill(6))
            old_pool = db.get_limit_pool(trade_date=today)
            if old_pool is not None and not old_pool.empty:
                stale_codes = set(old_pool.index.astype(str).str.strip().str.zfill(6)) - new_codes
                if stale_codes:
                    db.delete_limit_pool_by_codes(today, list(stale_codes))

            self._last_limit_slot = slot
            logger.info("[Scanner] limit_pool 刷新: %d 条 (source=%s)", saved, source)
            return True
        except Exception as e:
            logger.warning("[Scanner] limit_pool 刷新失败: %s", e)
            return False

    @staticmethod
    def _detect_limit_breaks(db, df: pd.DataFrame, today: str, source: str) -> None:
        """差集检测炸板：history - current → limit_break，current - history → limit_up_history。"""

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
            break_df["limit_times"] = [int(code_to_lt.get(c, 0) or 0) for c in missing_codes]
            break_df["open_times"] = [int(code_to_ot.get(c, 0) or 0) for c in missing_codes]
            break_df["sector"] = [str(code_to_sector.get(c, "")) for c in missing_codes]
            break_df["source"] = source
            db.upsert_limit_break(break_df, source=source)
            logger.info("[Scanner] 检测到炸板 %d 只: %s", len(missing_codes), missing_codes)

        # 4) Z-type 炸板检测：limit_type='Z' 仍在地中但已炸板（Tushare 数据才有 limit_type）
        if "limit_type" in df.columns:
            z_mask = df["limit_type"] == "Z"
            if z_mask.any():
                z_codes_raw = df.loc[z_mask, "code"].astype(str).str.strip().str.zfill(6)
                z_codes = set(z_codes_raw)
                z_new = z_codes - broke_codes  # 排除已记录的
                if z_new:
                    z_rows = df[df["code"].astype(str).str.strip().str.zfill(6).isin(z_new)]
                    z_break_df = pd.DataFrame()
                    z_break_df["code"] = list(z_new)
                    z_break_df["name"] = z_rows.get("name", pd.Series(dtype=str))
                    z_break_df["trade_date"] = today
                    z_break_df["status"] = "broke"
                    if "limit_times" in z_rows.columns:
                        z_break_df["limit_times"] = z_rows["limit_times"].values
                    else:
                        z_break_df["limit_times"] = 0
                    if "open_times" in z_rows.columns:
                        z_break_df["open_times"] = z_rows["open_times"].values
                    else:
                        z_break_df["open_times"] = 0
                    z_break_df["sector"] = z_rows.get("sector", pd.Series(dtype=str))
                    z_break_df["source"] = source
                    db.upsert_limit_break(z_break_df, source=source)
                    logger.info("[Scanner] Z型炸板检测 %d 只: %s", len(z_new), z_new)

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
                "流通市值": "float_market_cap", "封板资金": "seal_amount",
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
                "pct_chg": getattr(r, "change_pct", 0.0),
                "tech_score": getattr(r, "tech_score", 0.0),
                "rr_score": getattr(r, "rr_score", 0.0),
                "market_score": getattr(r, "market_score", 0.0),
                "sector_score": getattr(r, "sector_score", 0.0),
                "volume_score": getattr(r, "volume_score", 0.0),
                "position_score": getattr(r, "position_score", 0.0),
                "formation_score": getattr(r, "formation_score", 0.0),
                "composite_score": getattr(r, "composite_score", 0.0),
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
            now_utc = datetime.now(timezone.utc)
            now_local = (now_utc + timedelta(hours=8)).strftime("%H:%M:%S")
            for e in active:
                e["discovered_at"] = now_local
            payload = {
                "updated": now_utc.isoformat(),
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
                    "discovered_at": getattr(r, "discovered_at", ""),
                    "price_at_discovery": getattr(r, "price_at_discovery", None),
                    "pct_chg": getattr(r, "change_pct", 0.0),
                    "tech_score": getattr(r, "tech_score", 0.0),
                    "rr_score": getattr(r, "rr_score", 0.0),
                    "market_score": getattr(r, "market_score", 0.0),
                    "sector_score": getattr(r, "sector_score", 0.0),
                    "volume_score": getattr(r, "volume_score", 0.0),
                    "position_score": getattr(r, "position_score", 0.0),
                    "formation_score": getattr(r, "formation_score", 0.0),
                    "composite_score": getattr(r, "composite_score", 0.0),
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
    """盘后用 Tushare limit_list_ths 全量刷新 limit_pool（U/D/Z 三类全部）。

    相比 limit_list_d，limit_list_ths 额外提供 name/price/seal_amount(float_market_cap 等字段，
    可补齐盘中 akshare 未覆盖的炸板股数据。

    Returns:
        落库条数，失败返回 0
    """
    import time as _time
    from datetime import date

    try:
        from src.storage import DatabaseManager

        today = date.today().strftime("%Y%m%d")

        # 拉取全部涨跌停（U/D/Z），字段更丰富
        df = tushare_fetcher.get_limit_list_ths(today)
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 limit_pool 刷新: Tushare limit_list_ths 无数据")
            return 0

        df = df.reset_index()
        out = pd.DataFrame()
        out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        out["name"] = df.get("name", pd.Series("", index=df.index)).fillna("").values
        out["trade_date"] = today
        out["limit_type"] = df.get("limit_type", "")

        for c in ("pct_chg", "price", "open_times", "turnover_rate",
                  "seal_amount", "float_market_cap", "up_stat"):
            if c in df.columns:
                out[c] = df[c].values

        # limit_stats: 优先 lu_desc（连板描述），否则用 limit_type
        if "lu_desc" in df.columns:
            out["limit_stats"] = df["lu_desc"].fillna("").values
        else:
            out["limit_stats"] = out["limit_type"]

        for c in ("open_times",):
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)
        for c in ("pct_chg", "price", "seal_amount", "float_market_cap", "turnover_rate"):
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce")

        db = DatabaseManager()

        # 从同花顺行业映射 DB 补板块分类（只补空，不覆盖盘中已写入板块）
        try:
            ths_map = db.get_ths_industry_map()
        except Exception:
            ths_map = {}
        if ths_map:
            existing_sec = db.get_existing_sectors(out["trade_date"].iloc[0])
            codes_missing_sec = [
                c for c in out["code"].tolist()
                if c not in existing_sec or not existing_sec[c]
            ]
            if codes_missing_sec:
                fill_map = {c: ths_map.get(c, "") for c in codes_missing_sec}
                mask = out["code"].isin(codes_missing_sec)
                out.loc[mask, "sector"] = out.loc[mask, "code"].map(fill_map)

        # 保留盘中 akshare 写入的封板时间（limit_list_ths 不提供此字段）
        existing_seal = db.get_limit_pool_seal_times(today)
        if existing_seal:
            out["first_seal_time"] = out["code"].map(
                {c: s[0] for c, s in existing_seal.items()}
            )
            out["last_seal_time"] = out["code"].map(
                {c: s[1] for c, s in existing_seal.items()}
            )

        slot = int(_time.time() // 30)
        saved = db.upsert_limit_pool(out, source="tushare", slot=slot)
        logger.info("[Scanner] 盘后 limit_pool 全量刷新: %d 条 (limit_list_ths)", saved)
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

        # 过滤 trade_date 非法行（Tushare 个别返回 NaN）
        out = out[out["trade_date"].notna() & (out["trade_date"].astype(str).str.match(r"^\d{8}$"))]
        if out.empty:
            logger.warning("[Scanner] 盘后 money_flow 刷新: trade_date 全部非法，跳过")
            return 0

        db = DatabaseManager()
        saved = db.upsert_money_flow(out, source="tushare")
        logger.info("[Scanner] 盘后 money_flow 全量刷新: %d 条", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 money_flow 刷新失败: %s", e)
        return 0


def refresh_margin_detail_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare margin_detail 全量刷新 margin_detail 表。

    检查最近 7 个交易日，跳过 DB 已完整的日期，补齐仍不完整的。
    Tushare 数据分批发布，只拉 2 天可能错过延迟补齐的窗口。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager
        from sqlalchemy import text as _text

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 margin_detail 刷新: 无交易日")
            return 0

        # 最近 7 个交易日（_get_trade_dates 降序，[:7] 取最新）
        target_dates = trade_dates[:7]

        db = DatabaseManager()
        total_saved = 0
        with db.get_session() as sess:
            for td in target_dates:
                cnt = sess.execute(
                    _text("SELECT COUNT(*) FROM margin_detail WHERE trade_date = :dt"),
                    {"dt": td},
                ).scalar() or 0
                if cnt >= 4000:
                    logger.debug(f"[Scanner] 盘后 margin_detail 刷新: {td} 已完整({cnt}), 跳过")
                    continue

                df = tushare_fetcher.get_bulk_margin_detail(trade_date=td)
                if df is None or df.empty:
                    logger.warning(f"[Scanner] 盘后 margin_detail 刷新: {td} 无数据")
                    continue

                df = df.reset_index()
                out = pd.DataFrame()
                out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
                out["name"] = df.get("name", pd.Series("", index=df.index)).values if "name" in df.columns else ""
                out["trade_date"] = df.get("trade_date", td)
                for c in ("rzye", "rzmre", "rzche", "rqye", "rqmcl", "rqchl", "rqyl"):
                    if c in df.columns:
                        out[c] = pd.to_numeric(df[c], errors="coerce")

                saved = db.upsert_margin_detail(out, source="tushare")
                total_saved += saved
                logger.info(f"[Scanner] 盘后 margin_detail 刷新 {td}: DB已有{cnt}, 补{saved} 条")

        # 清理超 10 年数据
        cutoff = str(int(trade_dates[-1][:4]) - 10) + trade_dates[-1][4:]
        with db.get_session() as sess:
            deleted = sess.execute(
                _text("DELETE FROM margin_detail WHERE trade_date < :cutoff"),
                {"cutoff": cutoff},
            ).rowcount
            sess.commit()
        if deleted:
            logger.info("[Scanner] margin_detail 清理超 10 年数据: %d 条 (早于 %s)", deleted, cutoff)

        logger.info("[Scanner] 盘后 margin_detail 全量刷新: 合计 %d 条", total_saved)
        return total_saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 margin_detail 刷新失败: %s", e)
        return 0


def refresh_daily_basic_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare daily_basic 全量刷新 daily_basic 表。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 daily_basic 刷新: 无交易日")
            return 0

        td = trade_dates[0]
        df = tushare_fetcher.get_daily_basic_all(trade_date=td)
        if df is None or df.empty:
            logger.warning(f"[Scanner] 盘后 daily_basic 刷新: {td} 无数据")
            return 0

        df = df.reset_index()
        out = pd.DataFrame()
        out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        out["trade_date"] = df.get("trade_date", td)
        for c in ("turnover_rate", "volume_ratio", "pe", "pb", "total_mv"):
            if c in df.columns:
                out[c] = pd.to_numeric(df[c], errors="coerce")

        db = DatabaseManager()
        saved = db.upsert_daily_basic(out, source="tushare")
        logger.info(f"[Scanner] 盘后 daily_basic 刷新 {td}: {saved} 条")

        # 自动清理超 10 年数据
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y%m%d")
        db.delete_daily_basic_before(cutoff)

        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 daily_basic 刷新失败: %s", e)
        return 0


def refresh_hm_detail_postmarket(tushare_fetcher, start: Optional[str] = None) -> int:
    """盘后用 Tushare hm_detail 刷新游资明细表。

    默认拉取最近 2 个交易日（日常增量），传入 start="20220801" 可全量回填。
    """
    try:
        from src.storage import DatabaseManager
        from sqlalchemy import text as _text

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 hm_detail 刷新: 无交易日")
            return 0

        if start is not None:
            target_dates = sorted(d for d in trade_dates if d >= start)
        else:
            target_dates = trade_dates[:2]

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

        # 清理超 10 年数据
        cutoff = str(int(trade_dates[-1][:4]) - 10) + trade_dates[-1][4:]
        with db.get_session() as sess:
            deleted = sess.execute(
                _text("DELETE FROM hm_detail WHERE trade_date < :cutoff"),
                {"cutoff": cutoff},
            ).rowcount
            sess.commit()
        if deleted:
            logger.info("[Scanner] hm_detail 清理超 10 年数据: %d 条 (早于 %s)", deleted, cutoff)

        return total_saved
    except Exception as e:
        logger.warning("[Scanner] hm_detail 刷新失败: %s", e)
        return 0


def refresh_tech_indicator_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare stk_factor 刷新技术指标表。

    get_bulk_stk_factor 内部已自动写 DB（_cache_bulk_stk_factor），
    此处仅触发拉取，写入在 TushareFetcher 中完成。
    若首次拉取行数不足 4000，等 5s 后重试一次。
    """
    import time as _time
    df = tushare_fetcher.get_bulk_stk_factor()
    count = len(df) if df is not None else 0
    if count < 4000:
        logger.warning("[Scanner] tech_indicator 首次拉取仅 %d 行, 5s后重试...", count)
        _time.sleep(5)
        df = tushare_fetcher.get_bulk_stk_factor()
        count2 = len(df) if df is not None else 0
        if count2 > count:
            logger.info("[Scanner] tech_indicator 重试成功: %d → %d 行", count, count2)
            count = count2
    return count


def refresh_cyq_perf_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare cyq_perf 全量刷新筹码胜率表。

    检查最近 7 个交易日，跳过 DB 已完整的日期，补齐仍不完整的。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager
        from sqlalchemy import text as _text

        trade_dates = tushare_fetcher._get_trade_dates()
        if not trade_dates:
            logger.warning("[Scanner] 盘后 cyq_perf 刷新: 无交易日")
            return 0

        # 最近 7 个交易日（_get_trade_dates 降序，[:7] 取最新）
        target_dates = trade_dates[:7]

        db = DatabaseManager()
        total_saved = 0
        with db.get_session() as sess:
            for td in target_dates:
                cnt = sess.execute(
                    _text("SELECT COUNT(*) FROM broker_enrichment_cyq_perf WHERE trade_date = :dt"),
                    {"dt": td},
                ).scalar() or 0
                if cnt >= 5000:
                    logger.debug(f"[Scanner] 盘后 cyq_perf 刷新: {td} 已完整({cnt}), 跳过")
                    continue

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
                logger.info(f"[Scanner] 盘后 cyq_perf 刷新 {td}: DB已有{cnt}, 补{saved} 条")

        # 清理超 10 年数据
        cutoff = str(int(trade_dates[-1][:4]) - 10) + trade_dates[-1][4:]
        with db.get_session() as sess:
            deleted = sess.execute(
                _text("DELETE FROM broker_enrichment_cyq_perf WHERE trade_date < :cutoff"),
                {"cutoff": cutoff},
            ).rowcount
            sess.commit()
        if deleted:
            logger.info("[Scanner] 盘后 cyq_perf 清理超 10 年数据: %d 条 (早于 %s)", deleted, cutoff)

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


def refresh_profit_forecast_postmarket(trade_date: str, akshare_fetcher=None) -> int:
    """盘后用 akshare stock_profit_forecast_em 刷新 profit_forecast 表。

    全量覆盖写入当日快照。每日运行，无重复检测（数据量小，直接覆盖）。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        if akshare_fetcher is None:
            from data_provider.akshare_fetcher import AkshareFetcher
            akshare_fetcher = AkshareFetcher()

        db = DatabaseManager()
        df = akshare_fetcher.get_profit_forecast()
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 profit_forecast 刷新: 无数据")
            return 0

        saved = db.save_profit_forecast(df, trade_date)
        logger.info("[Scanner] 盘后 profit_forecast 全量刷新 date=%s: %d 条", trade_date, saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 profit_forecast 刷新失败: %s", e)
        return 0


def refresh_institution_hold_postmarket(akshare_fetcher=None) -> int:
    """盘后用 akshare stock_institute_hold 刷新 institution_hold 表。

    季报数据，同季度多次运行自动跳过（按 quarter 去重）。
    quarter 由系统日期推导，如当前为 5 月 → '202xQ1'。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        if akshare_fetcher is None:
            from data_provider.akshare_fetcher import AkshareFetcher
            akshare_fetcher = AkshareFetcher()

        db = DatabaseManager()
        quarter = db._derive_current_quarter()

        if db.has_institution_hold_quarter(quarter):
            logger.info("[Scanner] 机构持仓 %s 已有数据，跳过刷新", quarter)
            return 0

        raw = akshare_fetcher.get_institution_holds()
        if raw is None or raw.empty:
            logger.warning("[Scanner] 盘后 institution_hold 刷新: 无数据")
            return 0

        col_map = {
            "机构数": "inst_count",
            "机构数变化": "inst_count_change",
            "持股比例": "hold_ratio",
            "持股比例增幅": "hold_ratio_change",
            "占流通股比例": "circulate_ratio",
            "占流通股比例增幅": "circulate_ratio_change",
        }
        df = raw.rename(columns=col_map)
        if "ts_code" in df.columns:
            df["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        elif df.index.name == "ts_code":
            codes = df.index.astype(str).str.split(".").str[0].str.zfill(6)
            df = df.reset_index(drop=True)
            df["code"] = codes
        if "证券简称" in df.columns:
            df["name"] = df["证券简称"]
        elif "name" not in df.columns:
            df["name"] = ""

        saved = db.upsert_institution_hold(df, quarter=quarter, source="akshare")
        logger.info("[Scanner] 盘后 institution_hold 全量刷新 quarter=%s: %d 条",
                     quarter, saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 institution_hold 刷新失败: %s", e)
        return 0


def refresh_performance_report_postmarket(akshare_fetcher=None) -> int:
    """盘后用 akshare stock_yjbb_em 刷新 performance_report 表。

    按报告期去重，同报告期多次运行自动跳过。
    默认拉取最新 2 个季度的数据（支持趋势计算）。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from datetime import date as _date
        from src.storage import DatabaseManager

        if akshare_fetcher is None:
            from data_provider.akshare_fetcher import AkshareFetcher
            akshare_fetcher = AkshareFetcher()

        db = DatabaseManager()
        today = _date.today().strftime("%Y%m%d")

        # Compute recent 2 quarter-end dates
        from src.discovery.factors.performance_factor import _quarter_end_dates
        periods = _quarter_end_dates(today, 2)
        if not periods:
            logger.warning("[Scanner] 无法确定业绩报告期")
            return 0

        total_saved = 0
        for period in periods:
            existing = db.get_performance_report(period)
            if not existing.empty:
                logger.debug("[Scanner] performance_report %s 已有 %d 条，跳过", period, len(existing))
                continue

            raw = akshare_fetcher.get_performance_report_quarter(period)
            if raw is None or raw.empty:
                logger.warning("[Scanner] performance_report %s: 无数据", period)
                continue

            saved = db.upsert_performance_report(raw, period, source="akshare")
            logger.info("[Scanner] performance_report %s: %d 条", period, saved)
            total_saved += saved

        # 清理超过 10 年的数据
        from datetime import date as _date, timedelta
        cutoff = (_date.today() - timedelta(days=3652)).strftime("%Y%m%d")
        deleted = db.delete_performance_report_before(cutoff)
        if deleted > 0:
            logger.info("[Scanner] 清理 performance_report < %s: %d 条", cutoff, deleted)

        return total_saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 performance_report 刷新失败: %s", e)
        return 0


def refresh_repurchase_postmarket(tushare_fetcher=None) -> int:
    """盘后用 Tushare repurchase 刷新 repurchase 表。

    拉取近 180 天的回购公告数据并 upsert 入库，同时清理超出 10 年的旧数据。
    与 institution_hold 不同，回购数据可能每日有新公告，每次都拉取更新。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from datetime import date as _date, timedelta
        from src.storage import DatabaseManager

        if tushare_fetcher is None:
            from data_provider.tushare_fetcher import TushareFetcher
            tushare_fetcher = TushareFetcher.get_instance()

        today = _date.today()
        start_date = (today - timedelta(days=180)).strftime("%Y%m%d")

        df = tushare_fetcher.get_repurchase(start_date=start_date)
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 repurchase 刷新: 无数据")
            return 0

        db = DatabaseManager()
        saved = db.upsert_repurchase(df, source="tushare")
        logger.info("[Scanner] 盘后 repurchase 刷新: %d 条", saved)

        # 超出 10 年自动删除
        from sqlalchemy import text as _text
        cutoff = str(int(today.strftime("%Y%m%d")[:4]) - 10) + today.strftime("%m%d")
        with db.get_session() as sess:
            deleted = sess.execute(
                _text("DELETE FROM repurchase WHERE ann_date < :cutoff"),
                {"cutoff": cutoff},
            ).rowcount
            sess.commit()
        if deleted:
            logger.info("[Scanner] repurchase 清理: 删除 %d 条 (>10年)", deleted)

        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 repurchase 刷新失败: %s", e)
        return 0


def refresh_broker_recommend_postmarket(tushare_fetcher=None) -> int:
    """盘后用 Tushare broker_recommend 刷新当月券商金股数据。

    月初 1-3 日 Tushare 更新当月数据，每日调用确保数据及时入库。
    已存在数据时跳过（当月数据不会变化）。

    Returns:
        落库条数，失败或已存在返回 0
    """
    try:
        from datetime import date as _date
        from src.storage import DatabaseManager

        if tushare_fetcher is None:
            from data_provider.tushare_fetcher import TushareFetcher
            tushare_fetcher = TushareFetcher.get_instance()

        month = _date.today().strftime("%Y%m")
        db = DatabaseManager()

        existing = db.get_broker_recommend_monthly(month)
        if existing:
            return 0

        df = tushare_fetcher.get_broker_recommend(month)
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 broker_recommend 刷新: %s 月无数据", month)
            return 0

        saved = db.save_broker_recommend_monthly(month, df.reset_index())
        logger.info("[Scanner] 盘后 broker_recommend 刷新: %s 月 %d 条", month, saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 broker_recommend 刷新失败: %s", e)
        return 0


def refresh_popularity_postmarket(tushare_fetcher) -> int:
    """盘后用 Tushare dc_hot 全量刷新 popularity_rank 表。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        df = tushare_fetcher.get_dc_hot()
        if df is None or df.empty:
            logger.warning("[Scanner] 盘后 popularity_rank 刷新: Tushare dc_hot 无数据")
            return 0

        df = df.reset_index(drop=True)
        out = pd.DataFrame()
        out["code"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        out["name"] = df.get("name", "")
        out["trade_date"] = df.get("trade_date", "")
        out["rank"] = pd.to_numeric(df.get("rank", 0), errors="coerce").fillna(0).astype(int)
        out["pct_change"] = pd.to_numeric(df.get("pct_change", 0), errors="coerce")
        out["hot"] = pd.to_numeric(df.get("hot", 0), errors="coerce") if "hot" in df.columns else None
        out["concept"] = df.get("concept", "") if "concept" in df.columns else ""

        db = DatabaseManager()
        saved = db.upsert_popularity_rank(out, source="tushare")
        logger.info("[Scanner] 盘后 popularity_rank 全量刷新: %d 条", saved)

        # 清理超 10 年数据
        from sqlalchemy import text as _text
        latest_td = str(out["trade_date"].max())[:8] if not out.empty else ""
        if latest_td:
            cutoff = str(int(latest_td[:4]) - 10) + latest_td[4:]
            with db.get_session() as sess:
                deleted = sess.execute(
                    _text("DELETE FROM popularity_rank WHERE trade_date < :cutoff"),
                    {"cutoff": cutoff},
                ).rowcount
                sess.commit()
            if deleted:
                logger.info("[Scanner] popularity_rank 清理超 10 年数据: %d 条 (早于 %s)", deleted, cutoff)

        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 popularity_rank 刷新失败: %s", e)
        return 0


def refresh_stock_daily_postmarket(tushare_fetcher) -> int:
    """盘后补全当日 stock_daily 日K线数据。

    增量拉取最近 2 个交易日，覆盖当日及前一天可能的遗漏。
    盘中 IntradayScanner._ensure_daily_kline_complete 已做过一次，
    盘后再跑一次确保当日收盘数据入库。

    Returns:
        落库条数，失败返回 0
    """
    try:
        from datetime import date as _date
        from src.storage import DatabaseManager
        from scripts.sync_daily_kline import sync_all_daily, normalize_tushare_daily

        today = _date.today()
        trade_date = today.strftime("%Y%m%d")
        raw_dfs = sync_all_daily(tushare_fetcher, trade_date=trade_date, lookback_calendar_days=5)
        if not raw_dfs:
            logger.warning("[Scanner] 盘后 stock_daily 刷新: 无数据")
            return 0

        db = DatabaseManager()
        normalized = [d for d in (normalize_tushare_daily(df) for df in raw_dfs) if not d.empty]
        if not normalized:
            return 0
        import pandas as pd
        merged = pd.concat(normalized, ignore_index=True)
        saved = db.save_daily_batch(merged, data_source="tushare_postmarket")
        logger.info("[Scanner] 盘后 stock_daily 刷新: %d 行", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] 盘后 stock_daily 刷新失败: %s", e)
        return 0


def refresh_ths_industry_map_postmarket(tushare_fetcher) -> int:
    """盘后维护同花顺行业映射，每隔 7 天全量刷新一次。

    Returns:
        入库条数，跳过或失败返回 0
    """
    try:
        from src.storage import DatabaseManager

        db = DatabaseManager()
        age_hours = db.get_ths_industry_map_age_hours()
        if age_hours is not None and age_hours < 168:  # 7 天内不重复刷
            logger.debug("[Scanner] ths_industry_map 仍新鲜 (%.0fh)，跳过", age_hours)
            return 0

        if age_hours is None:
            logger.info("[Scanner] ths_industry_map 为空，开始构建...")
        else:
            logger.info("[Scanner] ths_industry_map 已过期 (%.0fh)，重新构建...", age_hours)

        # 获取行业列表
        import akshare as ak
        import pandas as pd
        import time as _time

        industry_df = ak.stock_board_industry_name_ths()
        industry_df["ths_code"] = industry_df["code"].astype(str).str.strip()
        industry_df["industry_name"] = industry_df["name"].astype(str).str.strip()

        tf = tushare_fetcher
        if tf is None or tf._api is None:
            logger.warning("[Scanner] Tushare API 不可用，跳过 ths_industry_map 刷新")
            return 0

        code_to_industry: dict = {}
        for _, row in industry_df.iterrows():
            ths_code = row["ths_code"]
            name = row["industry_name"]
            ts_code_full = f"{ths_code}.TI"
            try:
                raw = tf._api.ths_member(ts_code=ts_code_full, fields="ts_code,con_code")
                if raw is not None and not raw.empty and "con_code" in raw.columns:
                    codes = raw["con_code"].astype(str).str.strip()
                    for c in codes:
                        if "." in c:
                            code = c.split(".")[0].zfill(6)
                            if code not in code_to_industry:
                                code_to_industry[code] = name
            except Exception:
                continue
            _time.sleep(0.8)

        if not code_to_industry:
            logger.warning("[Scanner] ths_industry_map 构建结果为空")
            return 0

        out = pd.DataFrame([
            {"stock_code": k, "industry_name": v}
            for k, v in code_to_industry.items()
        ])
        saved = db.upsert_ths_industry_map(out, source="tushare")
        logger.info("[Scanner] ths_industry_map 刷新完成: %d 条", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] ths_industry_map 刷新失败: %s", e)
        return 0


def refresh_ths_concept_map_postmarket(tushare_fetcher) -> int:
    """盘后维护同花顺概念映射，每隔 7 天全量刷新一次。"""
    try:
        from src.storage import DatabaseManager

        db = DatabaseManager()
        age_hours = db.get_ths_concept_map_age_hours()
        if age_hours is not None and age_hours < 168:
            logger.debug("[Scanner] ths_concept_map 仍新鲜 (%.0fh)，跳过", age_hours)
            return 0

        if age_hours is None:
            logger.info("[Scanner] ths_concept_map 为空，开始构建...")
        else:
            logger.info("[Scanner] ths_concept_map 已过期 (%.0fh)，重新构建...", age_hours)

        import pandas as pd
        import time as _time

        tf = tushare_fetcher
        if tf is None or tf._api is None:
            logger.warning("[Scanner] Tushare API 不可用，跳过 ths_concept_map 刷新")
            return 0

        all_indices = tf._api.ths_index()
        concept_indices = all_indices[all_indices["type"] == "N"]
        logger.info("[Scanner] 获取 %d 个概念板块", len(concept_indices))

        code_to_concepts: dict = {}
        for i, (_, row) in enumerate(concept_indices.iterrows()):
            ts_code = str(row["ts_code"]).strip()
            name = str(row["name"]).strip()
            try:
                raw = tf._api.ths_member(ts_code=ts_code, fields="ts_code,con_code")
                if raw is not None and not raw.empty and "con_code" in raw.columns:
                    codes = raw["con_code"].astype(str).str.strip()
                    for c in codes:
                        if "." in c:
                            code = c.split(".")[0].zfill(6)
                            code_to_concepts.setdefault(code, []).append(name)
            except Exception:
                continue
            _time.sleep(0.8)

        if not code_to_concepts:
            logger.warning("[Scanner] ths_concept_map 构建结果为空")
            return 0

        rows = []
        for stock_code, concepts in code_to_concepts.items():
            for cn in concepts:
                rows.append({"stock_code": stock_code, "concept_name": cn})
        out = pd.DataFrame(rows)

        saved = db.upsert_ths_concept_map(out, source="tushare")
        logger.info("[Scanner] ths_concept_map 刷新完成: %d 条", saved)
        return saved
    except Exception as e:
        logger.warning("[Scanner] ths_concept_map 刷新失败: %s", e)
        return 0


def run_intraday_scan(config: DiscoveryConfig, tushare_fetcher=None, akshare_fetcher=None) -> None:
    """一键启动盘中扫描（注册全部盘中因子）。"""
    from src.discovery.factors import (
        MaEntryFactor,
        SectorFactor, MomentumFactor,
        RankingMomentumFactor, ReboundFactor, PopularityFactor,
    )

    engine = StockDiscoveryEngine(config, tushare_fetcher, akshare_fetcher)
    engine.register_factors([
        MaEntryFactor(),
        SectorFactor(), MomentumFactor(),
        RankingMomentumFactor(), ReboundFactor(), PopularityFactor(),
    ])

    set_active_config(config)
    _load_runtime_state_into(config)
    scanner = IntradayScanner(config, engine)
    scanner.start()


def _build_sector_index_mapping() -> Dict[str, str]:
    """构建 THS 行业名称 → 同花顺指数代码的映射，优先选择 881xxx.TI 标准行业指数。"""
    import tushare as ts

    pro = ts.pro_api()
    all_indices = pro.ths_index()
    if all_indices is None or all_indices.empty:
        logger.warning("[Scanner] ths_index 返回为空，无法构建行业映射")
        return {}

    mapping: Dict[str, str] = {}
    for _, row in all_indices.iterrows():
        name = str(row["name"]).strip()
        code = str(row["ts_code"]).strip()
        idx_type = str(row.get("type", ""))
        if name not in mapping:
            mapping[name] = code
        else:
            # 优先 881xxx.TI（标准行业指数），其次 I 类
            existing = mapping[name]
            if code.startswith("881") and idx_type == "I":
                mapping[name] = code
            elif code.startswith("881") and not existing.startswith("881"):
                mapping[name] = code
            elif idx_type == "I" and not existing.startswith("881"):
                mapping[name] = code

    return mapping


def refresh_sector_daily_postmarket() -> int:
    """盘后用 Tushare ths_daily 拉取近 60 日板块日线。

    通过 ths_index 建立行业名称→指数代码映射，逐指数获取日线 OHLCV，
    upsert 到 sector_daily 表，供 StockScorer 板块状态判定使用。
    """
    try:
        import time as _time
        import tushare as ts
        from datetime import date as dt_date, datetime as dt_datetime, timedelta
        from src.storage import DatabaseManager

        db = DatabaseManager()
        ths_map = db.get_ths_industry_map()
        if not ths_map:
            logger.warning("[Scanner] ths_industry_map 为空，跳过板块日线刷新")
            return 0

        sectors = sorted(set(ths_map.values()))
        index_mapping = _build_sector_index_mapping()
        if not index_mapping:
            logger.warning("[Scanner] 行业指数映射为空，跳过板块日线刷新")
            return 0

        # 统计匹配情况
        unmatched = [s for s in sectors if s != "-" and s not in index_mapping]
        if unmatched:
            logger.info("[Scanner] %d 个行业未匹配到指数代码: %s", len(unmatched), unmatched)

        end_date = dt_date.today().strftime("%Y%m%d")
        start_date = (dt_date.today() - timedelta(days=60)).strftime("%Y%m%d")

        pro = ts.pro_api()
        total_saved = 0
        fetched = 0

        for i, sector in enumerate(sectors):
            if sector == "-":
                continue
            ts_code = index_mapping.get(sector)
            if not ts_code:
                continue

            try:
                df = pro.ths_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )
                if df is None or df.empty:
                    continue

                fetched += 1
                records = []
                for _, row in df.iterrows():
                    trade_date_str = str(row.get("trade_date", ""))
                    if not trade_date_str:
                        continue
                    try:
                        trade_date = dt_datetime.strptime(trade_date_str, "%Y%m%d").date()
                    except ValueError:
                        continue
                    try:
                        records.append({
                            "sector_name": sector,
                            "trade_date": trade_date,
                            "close": float(row.get("close", 0)),
                            "high": float(row.get("high", 0)),
                            "low": float(row.get("low", 0)),
                            "open": float(row.get("open", 0)),
                            "pct_chg": float(row.get("pct_change", 0)) if row.get("pct_change") is not None else 0.0,
                        })
                    except (ValueError, TypeError):
                        continue

                if records:
                    saved = db.upsert_sector_daily(records)
                    total_saved += saved

                _time.sleep(0.3)
            except Exception:
                logger.debug("[Scanner] 板块 %s (%s) 日线获取失败，跳过", sector, ts_code)

        logger.info("[Scanner] 板块日线刷新完成: %d/%d 个行业, %d 条新增",
                     fetched, len(sectors) - 1, total_saved)
        return total_saved
    except Exception as e:
        logger.warning("[Scanner] 板块日线刷新失败: %s", e)
        return 0


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
        (cache, results, engine) 三元组:
        - cache: {stock_code: {score, factor_scores, reasons, ...}} 因子信号缓存
        - results: engine.discover() 返回的 DiscoveredResult 列表
        - engine: 已注册因子并执行过 discover 的 StockDiscoveryEngine（可复用 format_report）
    """
    from datetime import date as dt_date

    from src.discovery.config import get_discovery_config
    from src.discovery.engine import create_discovery_engine
    from src.storage import DatabaseManager

    today = (
        tushare_fetcher.get_trade_time(early_time="18:01", late_time="04:59")
        or dt_date.today().strftime("%Y%m%d")
    )
    db = DatabaseManager()

    if not force and db.has_postmarket_scan_today(today):
        cached = db.load_factor_signals_for_date(today)
        if cached:
            logger.info("[Scanner] 今日已扫描，加载 %d 条缓存", len(cached))
            return cached, None, None

    logger.info("[Scanner] 开始完整盘后扫描 (date=%s)...", today)

    # ---- 数据刷新 ----
    refreshers = [
        ("ths_industry_map", lambda: refresh_ths_industry_map_postmarket(tushare_fetcher)),
        ("ths_concept_map", lambda: refresh_ths_concept_map_postmarket(tushare_fetcher)),
        ("sector_daily", lambda: refresh_sector_daily_postmarket()),
        ("stock_daily", lambda: refresh_stock_daily_postmarket(tushare_fetcher)),
        ("limit_pool", lambda: refresh_limit_pool_postmarket(tushare_fetcher)),
        ("money_flow", lambda: refresh_money_flow_postmarket(tushare_fetcher)),
        ("daily_basic", lambda: refresh_daily_basic_postmarket(tushare_fetcher)),
        ("margin_detail", lambda: refresh_margin_detail_postmarket(tushare_fetcher)),
        ("cyq_perf", lambda: refresh_cyq_perf_postmarket(tushare_fetcher)),
        ("insider_buy", lambda: refresh_insider_buy_postmarket()),
        ("institution_hold", lambda: refresh_institution_hold_postmarket()),
        ("repurchase", lambda: refresh_repurchase_postmarket(tushare_fetcher)),
        ("profit_forecast", lambda: refresh_profit_forecast_postmarket(today, akshare_fetcher)),
        ("performance_report", lambda: refresh_performance_report_postmarket(akshare_fetcher)),
        ("hm_detail", lambda: refresh_hm_detail_postmarket(tushare_fetcher)),
        ("popularity", lambda: refresh_popularity_postmarket(tushare_fetcher)),
        ("tech_indicator", lambda: refresh_tech_indicator_postmarket(tushare_fetcher)),
    ]
    refresher_counts: Dict[str, int] = {}
    integrity_warnings: List[str] = []
    for name, fn in refreshers:
        try:
            count = fn()
            refresher_counts[name] = count
        except Exception:
            logger.warning("[Scanner] %s 刷新失败，继续", name, exc_info=True)
            refresher_counts[name] = -1

    # 数据入口完整性：零行检测
    for name, count in refresher_counts.items():
        if count == 0:
            integrity_warnings.append(f"数据源 '{name}' 返回 0 行")
        if count > 0 and count < 4000 and name in ("money_flow", "daily_basic", "margin_detail"):
            logger.warning(
                "[Scanner] %s 行数偏低(%d), 后续 Tier1 校验将兜底", name, count
            )

    # 盘后 Tushare 全量刷新 limit_pool 后，用正确数据重跑炸板检测，
    # 纠正盘中基于过期 AkShare 数据产生的误判。
    try:
        fresh_pool = db.get_limit_pool(trade_date=today)
        if fresh_pool is not None and not fresh_pool.empty:
            fresh_pool = fresh_pool.reset_index()  # get_limit_pool 以 code 为 index，需还原为列
            IntradayScanner._detect_limit_breaks(db, fresh_pool, today, "tushare")
            logger.info("[Scanner] 盘后炸板检测已用 Tushare 数据重新校正")
    except Exception:
        logger.warning("[Scanner] 盘后炸板重检测失败", exc_info=True)

    # 游资质量更新（hm_detail 有新数据才重算）
    try:
        from src.discovery.hm_tracker import HmTracker
        HmTracker(db).refresh_and_update()
    except Exception:
        logger.warning("[Scanner] hm_quality 更新失败，继续", exc_info=True)

    # ---- 因子评分 ----
    discovery_config = get_discovery_config()
    engine = create_discovery_engine(discovery_config, tushare_fetcher, akshare_fetcher)

    results = engine.discover(mode="postmarket")

    # ---- 结果落库 ----
    records = engine.get_last_full_scan_records()
    if records:
        # 合并 tech_score / composite_score 到落库记录
        score_map: Dict[str, Dict[str, float]] = {}
        for r in results:
            code = r.stock_code
            score_map[code] = {
                "tech_score": r.tech_score,
                "composite_score": r.composite_score,
            }
        tech_scores_map: Dict[str, float] = getattr(engine, '_last_tech_scores_map', {}) or {}
        score_blend_alpha = getattr(engine, '_last_score_blend_alpha', 1.0) or 1.0
        for rec in records:
            sm = score_map.get(rec.get("stock_code", ""))
            ts_code = rec.get("ts_code", "")
            stock_code = rec.get("stock_code", "")
            if sm:
                rec["tech_score"] = sm["tech_score"]
                rec["composite_score"] = sm["composite_score"]
            else:
                tech = tech_scores_map.get(ts_code) or tech_scores_map.get(stock_code)
                if tech:
                    rec["tech_score"] = tech
                    rec["composite_score"] = (
                        score_blend_alpha * rec.get("total_score", 0)
                        + (1 - score_blend_alpha) * tech
                    )
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

    # ---- 数据完整性汇总 ----
    # 收集 engine 层的 Tier 1 校验异常
    if engine is not None and hasattr(engine, '_integrity_warnings') and engine._integrity_warnings:
        integrity_warnings.extend(engine._integrity_warnings)

    if integrity_warnings:
        logger.warning(
            "⚠️ 数据完整性异常(%d项):\n  %s",
            len(integrity_warnings), "\n  ".join(integrity_warnings),
        )
    else:
        logger.info("✅ 数据完整性检查通过")

    return cache, results, engine
