# -*- coding: utf-8 -*-
"""LightGBM 研究模块 Pydantic Schema。"""

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class LGBTrainRequest(BaseModel):
    mode: str = Field("postmarket", description="扫描模式: intraday | postmarket")
    forward_days: int = Field(3, ge=1, le=60, description="预测未来 N 日收益（fixed 模式）")
    exec_mode: str = Field("close", description="标签模式: open (开盘→开盘) | close (收盘→收盘)")
    label_mode: str = Field("fixed", description="标签构造: fixed | peak_speed")
    window_days: int = Field(20, ge=5, le=60, description="峰值搜索窗口天数（peak_speed 模式）")
    peak_min_return: float = Field(0.01, ge=0.0, le=0.1, description="最小峰值门槛")
    start_date: Optional[str] = Field(None, description="训练起始日期 YYYYMMDD")
    end_date: Optional[str] = Field(None, description="训练结束日期 YYYYMMDD")
    n_estimators: int = Field(200, ge=10, le=2000)
    num_leaves: int = Field(31, ge=2, le=255)
    learning_rate: float = Field(0.05, ge=0.001, le=1.0)
    cv_folds: int = Field(5, ge=1, le=10)


class LGBTaskStatusResponse(BaseModel):
    task_id: str
    status: str
    status_message: str = ""
    error: str = ""
    result: Optional[Dict] = None


class LGBFeatureImportanceResponse(BaseModel):
    gain: Dict[str, float]
    split: Dict[str, float]


class LGBPredictionItem(BaseModel):
    rank: int
    ts_code: str
    stock_code: str
    stock_name: str = ""
    lgb_score: float
    raw_score: float
    predicted_days: Optional[int] = None
    win_rate: Optional[float] = None
    avg_return: Optional[float] = None
    max_return: Optional[float] = None
    max_loss: Optional[float] = None
    profit_loss_ratio: Optional[float] = None
    hit_count: Optional[int] = None
    score_percentile: Optional[float] = None


class LGBPredictionsResponse(BaseModel):
    model_date: str
    forward_days: int
    mode: str
    predictions: List[LGBPredictionItem]


class LGBBacktestCompareResponse(BaseModel):
    lgb_metrics: Dict = Field(default_factory=dict)
    factor_metrics: Dict = Field(default_factory=dict)
    capital_curve: List[Dict] = Field(default_factory=list)
    comparison: Dict = Field(default_factory=dict)


class LGBModelInfo(BaseModel):
    name: str
    path: str
    size_kb: float
    saved_at: str


class LGBModelListResponse(BaseModel):
    models: List[LGBModelInfo]


class LGBDateRangeResponse(BaseModel):
    intraday: Optional[Dict[str, str]] = None
    postmarket: Optional[Dict[str, str]] = None


class LGBStockLookupItem(BaseModel):
    """个股 LGB 预测详情。"""
    stock_code: str
    ts_code: str
    stock_name: str = ""
    rank: int
    lgb_score: float       # 归一化 0-100
    raw_score: float        # 模型原始输出
    total_stocks: int       # 全市场参评股票数


class LGBStockLookupResponse(BaseModel):
    found: bool
    item: Optional[LGBStockLookupItem] = None
    message: str = ""


# ── Backtest Simulation ──

class LGBBacktestTradeItem(BaseModel):
    """单笔回测交易明细。"""
    pred_date: str
    stock_code: str
    ts_code: str
    stock_name: str = ""
    rank: int
    buy_date: str
    buy_price: float
    sell_date: str
    sell_price: float
    return_pct: float
    skipped: bool = False
    expected_sell_date: str = ""


class LGBBacktestSimMetrics(BaseModel):
    cumulative_return: float
    win_rate: float
    max_drawdown: float
    total_trades: int
    skipped_trades: int
    holding_trades: int = 0


class LGBBacktestSimResponse(BaseModel):
    forward_days: int
    top_n: int
    exec_mode: str = "open"
    metrics: LGBBacktestSimMetrics
    capital_curve: List[Dict] = Field(default_factory=list)
    trades: List[LGBBacktestTradeItem] = Field(default_factory=list)


class LGBBacktestSimAvailableResponse(BaseModel):
    open: List[int] = Field(default_factory=list)
    close: List[int] = Field(default_factory=list)
    has_peak: bool = False


# ── Model Diagnostics ──

class LGBTrainingMetrics(BaseModel):
    cv_rmse_mean: float = 0.0
    cv_rmse_std: float = 0.0
    n_samples: int = 0
    n_features: int = 0
    cv_scores: List[float] = Field(default_factory=list)
    rank_ic_mean: Optional[float] = None
    rank_ic_std: Optional[float] = None
    icir: Optional[float] = None
    oof_corr: Optional[float] = None


class LGBTreeDiagnostics(BaseModel):
    n_trees: int = 0
    avg_depth: float = 0.0
    avg_n_leaves: float = 0.0
    total_n_leaves: int = 0


class LGBPredictionStats(BaseModel):
    mean: float
    std: float
    skew: float
    kurtosis: float
    min: float
    max: float
    median: float


class LGBDiagnosticsResponse(BaseModel):
    training_metrics: LGBTrainingMetrics
    tree_diagnostics: LGBTreeDiagnostics
    prediction_stats: Optional[LGBPredictionStats] = None


# ── Cross-Model Overlap ──

class LGBCrossModelOverlapStock(BaseModel):
    stock_code: str
    ts_code: str
    stock_name: str = ""
    count: int
    model_names: List[str] = Field(default_factory=list)


class LGBCrossModelOverlapResponse(BaseModel):
    exec_mode: str
    total_models: int
    stocks: List[LGBCrossModelOverlapStock]


# ── Brute Force Search ──

class LGBBruteForceItem(BaseModel):
    """单个参数组合的回测结果。"""
    exec_mode: str
    forward_days: int
    top_n: int
    stop_strategy: str = "none"
    cumulative_return: float
    sharpe_ratio: float
    win_rate: float
    max_drawdown: float
    total_trades: int
    skipped_trades: int
    error: str = ""


class LGBBruteForceResult(BaseModel):
    """全方案搜索结果。"""
    best_by_return: Optional[LGBBruteForceItem] = None
    best_by_sharpe: Optional[LGBBruteForceItem] = None
    top5_by_return: List[LGBBruteForceItem] = Field(default_factory=list)
    top5_by_sharpe: List[LGBBruteForceItem] = Field(default_factory=list)
    all_results: List[LGBBruteForceItem] = Field(default_factory=list)
    report_path: str = ""


class LGBBruteForceTaskStatus(BaseModel):
    task_id: str
    status: str  # running | completed | failed
    progress_current: int = 0
    progress_total: int = 90
    status_message: str = ""
    result: Optional[LGBBruteForceResult] = None
    error: str = ""
