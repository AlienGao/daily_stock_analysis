# -*- coding: utf-8 -*-
"""股票发现引擎配置。

从环境变量读取，支持 .env 配置和 WebUI 运行时覆盖。
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


def _get_env_value(key: str) -> str:
    """读取环境变量，优先 os.environ，其次 main.py 的 .env 解析缓存。"""
    val = os.getenv(key)
    if val is not None:
        return val
    try:
        from main import _ACTIVE_ENV_FILE_VALUES
        return _ACTIVE_ENV_FILE_VALUES.get(key, "")
    except Exception:
        return ""


def _env_bool(key: str, default: bool = False) -> bool:
    val = _get_env_value(key).strip().lower()
    if not val:
        return default
    return val in ("true", "1", "yes", "on")


def _env_float(key: str, default: float) -> float:
    val = _get_env_value(key).strip()
    if not val:
        return default
    try:
        return float(val)
    except ValueError:
        return default


def _env_int(key: str, default: int) -> int:
    val = _get_env_value(key).strip()
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

    # --- 盘中扫描权重 (6因子) ---
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

    # --- 盘后深度权重 (22因子) ---
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
    weight_broker_recommend: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_BROKER_RECOMMEND", 22.0)
    )
    weight_buyback: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_BUYBACK", 5.0)
    )
    weight_concept_heat: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_CONCEPT_HEAT", 16.0)
    )
    weight_hot_money: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_HOT_MONEY", 8.0)
    )
    weight_popularity_intraday: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_POPULARITY_INTRADAY", 18.0)
    )
    weight_popularity_postmarket: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_POPULARITY_POSTMARKET", 15.0)
    )
    weight_insider_buy: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_INSIDER_BUY", 5.0)
    )
    weight_institution_hold: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_INSTITUTION_HOLD", 8.0)
    )
    weight_fundamental: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_FUNDAMENTAL", 5.0)
    )
    weight_ranking_momentum: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_RANKING_MOMENTUM", 15.0)
    )
    weight_ranking_momentum_postmarket: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_RANKING_MOMENTUM_POSTMARKET", 10.0)
    )
    weight_performance: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_PERFORMANCE", 10.0)
    )
    weight_profit_forecast: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_PROFIT_FORECAST", 20.0)
    )
    weight_alpha042: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_ALPHA042", 10.0)
    )
    weight_vwap_deviation: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_VWAP_DEVIATION", 13.0)
    )
    weight_gap_reversal: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_GAP_REVERSAL", 10.0)
    )
    weight_liquid_oversold: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_LIQUID_OVERSOLD", 13.0)
    )
    weight_vwap_reversal: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_VWAP_REVERSAL", 10.0)
    )
    weight_gtja114: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_GTJA114", 14.0)
    )
    weight_alpha60: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_ALPHA60", 10.0)
    )
    weight_money_flow_osc: float = field(
        default_factory=lambda: _env_float("DISCOVER_WEIGHT_MONEY_FLOW_OSC", 10.0)
    )

    # --- 综合分混合比例（factor_score × alpha + tech_score × (1-alpha)）---
    score_blend_alpha: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORE_BLEND_ALPHA", 0.3)
    )

    # --- StockScorer 技术评分权重 ---
    scorer_weight_rr: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORER_WEIGHT_RR", 0.30)
    )
    scorer_weight_market: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORER_WEIGHT_MARKET", 0.20)
    )
    scorer_weight_sector: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORER_WEIGHT_SECTOR", 0.15)
    )
    scorer_weight_volume: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORER_WEIGHT_VOLUME", 0.15)
    )
    scorer_weight_position: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORER_WEIGHT_POSITION", 0.10)
    )
    scorer_weight_formation: float = field(
        default_factory=lambda: _env_float("DISCOVER_SCORER_WEIGHT_FORMATION", 0.10)
    )

    # --- 盘中扫描器设置 ---
    scan_interval_seconds: int = field(
        default_factory=lambda: _env_int("DISCOVER_SCAN_INTERVAL", 30)
    )
    scan_max_runtime_minutes: int = field(
        default_factory=lambda: _env_int("DISCOVER_SCAN_MAX_RUNTIME", 240)
    )
    scan_top_n: int = field(
        default_factory=lambda: _env_int("DISCOVER_SCAN_TOP_N", 300)
    )

    # --- 通知 ---
    feishu_webhook_url: str = field(
        default_factory=lambda: _get_env_value("FEISHU_WEBHOOK_URL").strip()
    )
    feishu_webhook_secret: str = field(
        default_factory=lambda: _get_env_value("FEISHU_WEBHOOK_SECRET").strip()
    )

    # --- 禁用因子列表 ---
    disabled_factors: set = field(
        default_factory=lambda: set(
            c.strip().lower()
            for c in _get_env_value("DISCOVERY_DISABLED_FACTORS").split(",")
            if c.strip()
        )
    )

    # --- 股票白名单 ---
    discover_whitelist: set = field(
        default_factory=lambda: set(
            c.strip()
            for c in _get_env_value("DISCOVERY_STOCK_WHITELIST").split(",")
            if c.strip()
        )
    )
    use_whitelist: bool = field(
        default_factory=lambda: _env_bool("DISCOVERY_USE_WHITELIST", False)
    )

    # --- 扫描范围：full_market / whitelist / broker_gold（盘中/盘后独立） ---
    intraday_scan_universe: str = field(
        default_factory=lambda: (_get_env_value("DISCOVERY_INTRADAY_SCAN_UNIVERSE") or _get_env_value("DISCOVERY_SCAN_UNIVERSE") or "full_market").strip()
    )
    postmarket_scan_universe: str = field(
        default_factory=lambda: (_get_env_value("DISCOVERY_POSTMARKET_SCAN_UNIVERSE")
                                 or _get_env_value("DISCOVERY_SCAN_UNIVERSE") or "full_market").strip()
    )
    # 兼容旧配置（盘中/盘后共用时用此值作为默认）
    scan_universe: str = field(
        default_factory=lambda: _get_env_value("DISCOVERY_SCAN_UNIVERSE") or "full_market".strip()
    )

    # --- StockScorer 多维技术评分 ---
    enable_stock_scorer: bool = field(
        default_factory=lambda: _env_bool("ENABLE_STOCK_SCORER", False)
    )

    enable_discovery_pipeline: bool = field(
        default_factory=lambda: _env_bool("DISCOVERY_PIPELINE_ENABLED", True)
    )

    # 运行时覆盖（None = 使用 env / 默认值），由 API 修改并持久化到 discovery_runtime.json
    _intraday_pipeline_enabled: Optional[bool] = field(default=None)
    _postmarket_pipeline_enabled: Optional[bool] = field(default=None)
    _score_blend_alpha: Optional[float] = field(default=None)

    @property
    def enable_intraday_pipeline(self) -> bool:
        """盘中管线开关（运行时覆盖 > 环境变量 > enable_discovery_pipeline）。"""
        if self._intraday_pipeline_enabled is not None:
            return self._intraday_pipeline_enabled
        val = _get_env_value("DISCOVERY_INTRADAY_PIPELINE_ENABLED").strip().lower()
        if val:
            return val in ("true", "1", "yes", "on")
        return self.enable_discovery_pipeline

    @enable_intraday_pipeline.setter
    def enable_intraday_pipeline(self, v: bool) -> None:
        self._intraday_pipeline_enabled = v

    @property
    def enable_postmarket_pipeline(self) -> bool:
        """盘后管线开关（运行时覆盖 > 环境变量 > enable_discovery_pipeline）。"""
        if self._postmarket_pipeline_enabled is not None:
            return self._postmarket_pipeline_enabled
        val = _get_env_value("DISCOVERY_POSTMARKET_PIPELINE_ENABLED").strip().lower()
        if val:
            return val in ("true", "1", "yes", "on")
        return self.enable_discovery_pipeline

    @enable_postmarket_pipeline.setter
    def enable_postmarket_pipeline(self, v: bool) -> None:
        self._postmarket_pipeline_enabled = v

    @property
    def effective_score_blend_alpha(self) -> float:
        """综合分混合比例（运行时覆盖 > 环境变量 > 默认 0.3）。"""
        if self._score_blend_alpha is not None:
            return self._score_blend_alpha
        return self.score_blend_alpha

    @effective_score_blend_alpha.setter
    def effective_score_blend_alpha(self, v: float) -> None:
        self._score_blend_alpha = v

    @staticmethod
    def env_config_keys() -> List[str]:
        """返回所有环境变量键名，用于 .env.example 同步和 WebUI 配置。"""
        return [
            "AUTO_DISCOVER",
            "AUTO_DISCOVER_COUNT",
            # --- 盘中权重 ---
            "DISCOVER_WEIGHT_SECTOR",
            "DISCOVER_WEIGHT_MA_ENTRY",
            "DISCOVER_WEIGHT_MOMENTUM",
            "DISCOVER_WEIGHT_REBOUND",
            "DISCOVER_WEIGHT_RANKING_MOMENTUM",
            "DISCOVER_WEIGHT_POPULARITY_INTRADAY",
            # --- 盘后权重 ---
            "DISCOVER_WEIGHT_MONEYFLOW",
            "DISCOVER_WEIGHT_MARGIN",
            "DISCOVER_WEIGHT_CHIP",
            "DISCOVER_WEIGHT_TECHNICAL",
            "DISCOVER_WEIGHT_LIMIT_POST",
            "DISCOVER_WEIGHT_BROKER_RECOMMEND",
            "DISCOVER_WEIGHT_BUYBACK",
            "DISCOVER_WEIGHT_CONCEPT_HEAT",
            "DISCOVER_WEIGHT_HOT_MONEY",
            "DISCOVER_WEIGHT_POPULARITY_POSTMARKET",
            "DISCOVER_WEIGHT_INSIDER_BUY",
            "DISCOVER_WEIGHT_INSTITUTION_HOLD",
            "DISCOVER_WEIGHT_FUNDAMENTAL",
            "DISCOVER_WEIGHT_RANKING_MOMENTUM_POSTMARKET",
            "DISCOVER_WEIGHT_PERFORMANCE",
            "DISCOVER_WEIGHT_PROFIT_FORECAST",
            "DISCOVER_WEIGHT_ALPHA042",
            "DISCOVER_WEIGHT_VWAP_DEVIATION",
            "DISCOVER_WEIGHT_GAP_REVERSAL",
            "DISCOVER_WEIGHT_LIQUID_OVERSOLD",
            "DISCOVER_WEIGHT_VWAP_REVERSAL",
            "DISCOVER_WEIGHT_GTJA114",
            # --- 综合分混合 ---
            "DISCOVER_SCORE_BLEND_ALPHA",
            # --- 因子自动调权 ---
            "DISCOVER_TUNE_ENABLED",
            "DISCOVER_TUNE_MIN_DAYS",
            # --- StockScorer 技术评分权重 ---
            "DISCOVER_SCORER_WEIGHT_RR",
            "DISCOVER_SCORER_WEIGHT_MARKET",
            "DISCOVER_SCORER_WEIGHT_SECTOR",
            "DISCOVER_SCORER_WEIGHT_VOLUME",
            "DISCOVER_SCORER_WEIGHT_POSITION",
            "DISCOVER_SCORER_WEIGHT_FORMATION",
            # --- 扫描器设置 ---
            "DISCOVER_SCAN_INTERVAL",
            "DISCOVER_SCAN_MAX_RUNTIME",
            "DISCOVER_SCAN_TOP_N",
            # --- 通知 ---
            "FEISHU_WEBHOOK_URL",
            "FEISHU_WEBHOOK_SECRET",
            # --- 禁用因子 ---
            "DISCOVERY_DISABLED_FACTORS",
            # --- 白名单与扫描范围 ---
            "DISCOVERY_STOCK_WHITELIST",
            "DISCOVERY_USE_WHITELIST",
            "DISCOVERY_INTRADAY_SCAN_UNIVERSE",
            "DISCOVERY_POSTMARKET_SCAN_UNIVERSE",
            "DISCOVERY_SCAN_UNIVERSE",
            # --- 其他 ---
            "ENABLE_STOCK_SCORER",
            "DISCOVERY_PIPELINE_ENABLED",
            "DISCOVERY_INTRADAY_PIPELINE_ENABLED",
            "DISCOVERY_POSTMARKET_PIPELINE_ENABLED",
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
            if "intraday_pipeline_enabled" in data:
                cfg._intraday_pipeline_enabled = data["intraday_pipeline_enabled"]
            if "postmarket_pipeline_enabled" in data:
                cfg._postmarket_pipeline_enabled = data["postmarket_pipeline_enabled"]
            if "score_blend_alpha" in data:
                cfg._score_blend_alpha = float(data["score_blend_alpha"])
    except Exception:
        pass


def save_runtime_state() -> None:
    """持久化当前 active config 的扫描模式、管线开关、alpha 和白名单到 JSON 文件。"""
    cfg = _ensure_active_config()
    data = {
        "intraday_scan_universe": cfg.intraday_scan_universe,
        "postmarket_scan_universe": cfg.postmarket_scan_universe,
        "discover_whitelist": sorted(cfg.discover_whitelist),
        "intraday_pipeline_enabled": cfg._intraday_pipeline_enabled,
        "postmarket_pipeline_enabled": cfg._postmarket_pipeline_enabled,
        "score_blend_alpha": cfg._score_blend_alpha,
    }
    # 清理 None 值避免 JSON null 污染
    data = {k: v for k, v in data.items() if v is not None or k in ("intraday_scan_universe", "postmarket_scan_universe", "discover_whitelist")}
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
