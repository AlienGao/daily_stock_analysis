# -*- coding: utf-8 -*-
"""LightGBM 研究模块 Pydantic Schema。"""

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class LGBTrainRequest(BaseModel):
    mode: str = Field("postmarket", description="扫描模式: intraday | postmarket")
    forward_days: int = Field(5, ge=1, le=60, description="预测未来 N 日收益")
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
    lgb_score: float
    raw_score: float


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
    rank: int
    lgb_score: float       # 归一化 0-100
    raw_score: float        # 模型原始输出
    total_stocks: int       # 全市场参评股票数


class LGBStockLookupResponse(BaseModel):
    found: bool
    item: Optional[LGBStockLookupItem] = None
    message: str = ""
