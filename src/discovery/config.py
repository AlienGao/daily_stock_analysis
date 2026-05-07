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
        default_factory=lambda: _env_int("DISCOVER_SCAN_INTERVAL", 300)
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
