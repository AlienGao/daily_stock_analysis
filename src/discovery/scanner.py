# -*- coding: utf-8 -*-
"""盘中扫描器 (Intraday Scanner).

持久化守护进程：自动识别交易日和盘中时段（9:30-15:00），
非交易时段休眠等待，交易时段按 scan_interval_seconds 轮询扫描。

用法:
    scanner = IntradayScanner(config, engine)
    scanner.start()   # 阻塞循环，永久运行
"""

import json
import logging
import os
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from src.discovery.config import DiscoveryConfig
from src.discovery.engine import StockDiscoveryEngine
from src.discovery.factors.base import DiscoveryResult

logger = logging.getLogger(__name__)

_OUTPUT_PATH = "/tmp/discovery_top10.json"
_REPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "discovery_reports"

_TZ_CN = timezone(timedelta(hours=8))
_MARKET_OPEN = (9, 30)   # 盘中扫描开始
_MARKET_CLOSE = (15, 0)  # 盘中扫描结束


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
        """检查今天是否为 A 股交易日。"""
        fetcher = getattr(self.engine, "tushare_fetcher", None)
        if fetcher is None:
            # 无 fetcher 时默认周一至周五都是交易日
            return self._now().weekday() < 5
        return fetcher.get_trade_time(early_time="09:30", late_time="15:00") is not None

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
                results = self.engine.discover(mode="intraday")
                if results:
                    annotated = self._annotate_changes(results)
                    self._write_output(annotated, results)
                    self._print_round(annotated)
                    self._previous = {
                        r.ts_code: i for i, r in enumerate(results)
                    }
            except Exception as e:
                logger.warning("[Scanner] 本轮扫描异常: %s", e)

            elapsed = time.time() - round_start
            sleep_sec = max(1, min(
                self.config.scan_interval_seconds - elapsed,
                (market_close - self._now()).total_seconds(),
            ))
            if sleep_sec > 0:
                logger.debug(
                    "[Scanner] 第 %d 轮耗时 %.1fs, 休眠 %.0fs",
                    self._round, elapsed, sleep_sec,
                )
                time.sleep(sleep_sec)

        logger.info("[Scanner] 盘中扫描结束（已收盘），共 %d 轮", self._round)

        # Step 4: 收盘后休眠至下一交易日 8:00
        next_open = self._time_to(8, 0)
        wait_min = (next_open - self._now()).total_seconds() / 60
        logger.info("[Scanner] 休眠至 %s (%.0f 分钟)", next_open.strftime("%m-%d %H:%M"), wait_min)
        time.sleep((next_open - self._now()).total_seconds())

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
                "reasons": r.reasons,
                "buy_price_low": r.buy_price_low,
                "buy_price_high": r.buy_price_high,
                "stop_loss": r.stop_loss,
                "take_profit_1": r.take_profit_1,
                "take_profit_2": r.take_profit_2,
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

        # 落盘 Markdown 报告到 discovery_reports/intraday_YYYYMMDD.md
        try:
            _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            date_str = date.today().strftime('%Y%m%d')
            report = self.engine.format_report(results, mode="intraday")
            filepath = _REPORTS_DIR / f"intraday_{date_str}.md"
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
                    "reasons": getattr(r, "reasons", []),
                    "buy_price_low": getattr(r, "buy_price_low", None),
                    "buy_price_high": getattr(r, "buy_price_high", None),
                    "stop_loss": getattr(r, "stop_loss", None),
                    "take_profit_1": getattr(r, "take_profit_1", None),
                    "take_profit_2": getattr(r, "take_profit_2", None),
                })
            json_file = _REPORTS_DIR / f"intraday_{date_str}_topn.json"
            json_file.write_text(json.dumps(topn, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.debug("[Scanner] 盘中 TopN JSON 已保存: %s", json_file)
        except Exception as e:
            logger.warning("[Scanner] 保存盘中报告失败: %s", e)


def run_intraday_scan(config: DiscoveryConfig, tushare_fetcher=None) -> None:
    """一键启动盘中扫描（注册全部盘中因子）。"""
    from src.discovery.factors import (
        SectorFactor, MaEntryFactor, MomentumFactor, ReboundFactor,
    )

    engine = StockDiscoveryEngine(config, tushare_fetcher)
    engine.register_factors([
        SectorFactor(),
        MaEntryFactor(),
        MomentumFactor(),
        ReboundFactor(),
    ])

    scanner = IntradayScanner(config, engine)
    scanner.start()
