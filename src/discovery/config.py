# -*- coding: utf-8 -*-
"""股票发现引擎配置。

从环境变量读取，支持 .env 配置和 WebUI 运行时覆盖。
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


def _env_bool(key: str, default: bool = False) -> bool:
    val = os.getenv(key, "").strip().lower()
    if not val:
        return default
    return val in ("true", "1", "yes", "on")


def _env_float(key: str, default: float) -> float:
    val = os.getenv(key, "").strip()
    if not val:
        return default
    try:
        return float(val)
    except ValueError:
        return default


def _env_int(key: str, default: int) -> int:
    val = os.getenv(key, "").strip()
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        return default


@dataclass
class DiscoveryConfig:
    """股票自动发现引擎配置。

    所有字段从环境变量读取，带合理默认值。
    """

    # --- 自动发现开关 ---
    auto_discover: bool = field(
        default_factory=lambda: _env_bool("AUTO_DISCOVER", False)
    )
    auto_discover_count: int = field(
        default_factory=lambda: _env_int("AUTO_DISCOVER_COUNT", 10)
    )

    # --- 盘中扫描权重 (4因子，相加=100) ---
    weight_sector: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_SECTOR", 25.0)
    )
    weight_ma_entry: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_MA_ENTRY", 35.0)
    )
    weight_momentum: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_MOMENTUM", 25.0)
    )
    weight_rebound: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_REBOUND", 15.0)
    )

    # --- 盘后深度权重 (5因子，相加=100) ---
    weight_moneyflow: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_MONEYFLOW", 25.0)
    )
    weight_margin: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_MARGIN", 20.0)
    )
    weight_chip: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_CHIP", 15.0)
    )
    weight_technical: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_TECHNICAL", 25.0)
    )
    weight_limit_post: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_LIMIT_POST", 15.0)
    )

    # --- 盘中扫描器设置 ---
    scan_interval_seconds: int = field(
        default_factory=lambda: _env_int("DISCOVER_SCAN_INTERVAL", 30)
    )
    scan_max_runtime_minutes: int = field(
        default_factory=lambda: _env_int("DISCOVER_SCAN_MAX_RUNTIME", 240)
    )
    scan_top_n: int = field(
        default_factory=lambda: _env_int("DISCOVER_SCAN_TOP_N", 10)
    )

    # --- 通知 ---
    feishu_webhook_url: str = field(
        default_factory=lambda: os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    )
    feishu_webhook_secret: str = field(
        default_factory=lambda: os.getenv("FEISHU_WEBHOOK_SECRET", "").strip()
    )

    # --- 股票白名单 ---
    discover_whitelist: set = field(
        default_factory=lambda: set(
            c.strip()
            for c in os.getenv("DISCOVERY_STOCK_WHITELIST", "").split(",")
            if c.strip()
        )
    )
    use_whitelist: bool = field(
        default_factory=lambda: _env_bool("DISCOVERY_USE_WHITELIST", False)
    )

    # --- 扫描范围：full_market / whitelist / broker_gold（盘中/盘后独立） ---
    intraday_scan_universe: str = field(
        default_factory=lambda: os.getenv("DISCOVERY_INTRADAY_SCAN_UNIVERSE",
                                  os.getenv("DISCOVERY_SCAN_UNIVERSE", "full_market")).strip()
    )
    postmarket_scan_universe: str = field(
        default_factory=lambda: os.getenv("DISCOVERY_POSTMARKET_SCAN_UNIVERSE",
                                  os.getenv("DISCOVERY_SCAN_UNIVERSE", "full_market")).strip()
    )
    # 兼容旧配置（盘中/盘后共用时用此值作为默认）
    scan_universe: str = field(
        default_factory=lambda: os.getenv("DISCOVERY_SCAN_UNIVERSE", "full_market").strip()
    )

    # --- StockScorer 多维技术评分 ---
    enable_stock_scorer: bool = field(
        default_factory=lambda: _env_bool("ENABLE_STOCK_SCORER", False)
    )

    @staticmethod
    def env_config_keys() -> List[str]:
        """返回所有环境变量键名，用于 .env.example 同步和 WebUI 配置。"""
        return [
            "AUTO_DISCOVER",
            "AUTO_DISCOVER_COUNT",
            "DISCOVER_WEIGHT_SECTOR",
            "DISCOVER_WEIGHT_MA_ENTRY",
            "DISCOVER_WEIGHT_MOMENTUM",
            "DISCOVER_WEIGHT_REBOUND",
            "DISCOVER_WEIGHT_MONEYFLOW",
            "DISCOVER_WEIGHT_MARGIN",
            "DISCOVER_WEIGHT_CHIP",
            "DISCOVER_WEIGHT_TECHNICAL",
            "DISCOVER_WEIGHT_LIMIT_POST",
            "DISCOVER_SCAN_INTERVAL",
            "DISCOVER_SCAN_MAX_RUNTIME",
            "DISCOVER_SCAN_TOP_N",
            "DISCOVERY_STOCK_WHITELIST",
            "DISCOVERY_USE_WHITELIST",
            "DISCOVERY_SCAN_UNIVERSE",
            "ENABLE_STOCK_SCORER",
        ]


def get_discovery_config() -> DiscoveryConfig:
    """获取发现引擎配置单例。"""
    return DiscoveryConfig()


# --- 运行时 active config 持有器（供 API 端点访问同一个实例） ---
_active_config: Optional[DiscoveryConfig] = None


def set_active_config(config: DiscoveryConfig) -> None:
    """注册当前运行中的 config 实例。"""
    global _active_config
    _active_config = config


def get_active_config() -> Optional[DiscoveryConfig]:
    """获取当前运行中的 config 实例，未启动时返回 None。"""
    return _active_config


# --- 运行时状态持久化（JSON 文件，跨服务重启保留扫描模式和白名单） ---

import json as _json
from pathlib import Path as _Path

_RUNTIME_STATE_PATH = _Path(__file__).resolve().parent.parent.parent / "discovery_runtime.json"


def _ensure_active_config() -> DiscoveryConfig:
    """获取或创建 active config（供 API 修改用）。"""
    global _active_config
    if _active_config is None:
        _active_config = DiscoveryConfig()
        _load_runtime_state_into(_active_config)
    return _active_config


def _load_runtime_state_into(cfg: DiscoveryConfig) -> None:
    """从 JSON 文件加载运行时状态到 config 实例。"""
    try:
        if _RUNTIME_STATE_PATH.exists():
            data = _json.loads(_RUNTIME_STATE_PATH.read_text(encoding="utf-8"))
            if "intraday_scan_universe" in data:
                cfg.intraday_scan_universe = data["intraday_scan_universe"]
            if "postmarket_scan_universe" in data:
                cfg.postmarket_scan_universe = data["postmarket_scan_universe"]
            if "discover_whitelist" in data:
                cfg.discover_whitelist = set(data["discover_whitelist"])
    except Exception:
        pass


def save_runtime_state() -> None:
    """持久化当前 active config 的扫描模式和白名单到 JSON 文件。"""
    cfg = _ensure_active_config()
    data = {
        "intraday_scan_universe": cfg.intraday_scan_universe,
        "postmarket_scan_universe": cfg.postmarket_scan_universe,
        "discover_whitelist": sorted(cfg.discover_whitelist),
    }
    _RUNTIME_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _RUNTIME_STATE_PATH.write_text(_json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_effective_whitelist() -> list:
    """返回当前生效的白名单列表（优先 active config，其次 env）。"""
    cfg = get_active_config()
    if cfg and cfg.discover_whitelist:
        return sorted(cfg.discover_whitelist)
    tmp = DiscoveryConfig()
    return sorted(tmp.discover_whitelist)


def set_whitelist(codes: list) -> None:
    """运行时更新白名单并持久化（立即生效，无需重启）。"""
    cfg = _ensure_active_config()
    cfg.discover_whitelist = set(c.strip() for c in codes if c.strip())
    save_runtime_state()
