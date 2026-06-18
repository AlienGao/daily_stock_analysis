# -*- coding: utf-8 -*-
"""Market statistics API schemas."""

from typing import List, Optional

from pydantic import BaseModel, Field


class NewHighDateItem(BaseModel):
    date: str = Field(..., description="创新高日期 YYYYMMDD")
    hfq_close: float = Field(..., description="后复权收盘价")


class HfqNewHighItem(BaseModel):
    ts_code: str
    stock_code: str
    stock_name: str
    latest_new_high_date: str
    latest_new_high_close: float
    new_high_count: int
    current_hfq_close: Optional[float] = None
    drawdown_from_high_pct: Optional[float] = None
    ytd_hfq_return_pct: Optional[float] = Field(
        None, description="2026 年初首个交易日后复权收盘至今涨幅（%）"
    )
    new_high_dates: List[NewHighDateItem] = Field(default_factory=list)


class HfqNewHighListResponse(BaseModel):
    start_date: str
    as_of_date: str
    total: int
    items: List[HfqNewHighItem] = Field(default_factory=list)


class HfqBollPickItem(BaseModel):
    ts_code: str
    stock_code: str
    stock_name: str
    latest_new_high_date: str
    latest_new_high_close: Optional[float] = None
    current_hfq_close: float
    drawdown_from_high_pct: Optional[float] = Field(None, description="现价相对最近新高偏离（%），越接近 0 越近新高")
    boll_mid: float
    boll_lower: float
    dist_mid_pct: float = Field(..., description="现价相对 BOLL 中轨偏离（%）")
    dist_lower_pct: float = Field(..., description="现价相对 BOLL 下轨偏离（%）")
    band_zone: str = Field(..., description="mid / lower / both")


class HfqBollPickListResponse(BaseModel):
    start_date: str
    as_of_date: str
    lookback_days: int
    near_pct: float
    max_drawdown_from_high_pct: float = Field(
        default=20.0,
        description="距最近新高最大回撤（%），超出则排除",
    )
    cutoff_date: str
    total: int
    items: List[HfqBollPickItem] = Field(default_factory=list)


class HfqKLineItem(BaseModel):
    date: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float
    volume: Optional[float] = None


class HfqKLineResponse(BaseModel):
    stock_code: str
    start_date: str
    end_date: str
    data: List[HfqKLineItem] = Field(default_factory=list)
