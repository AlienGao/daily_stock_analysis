# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 存储层
===================================

职责：
1. 管理 SQLite 数据库连接（单例模式）
2. 定义 ORM 数据模型
3. 提供数据存取接口
4. 实现智能更新逻辑（断点续传）
"""

import atexit
from contextlib import contextmanager
import hashlib
import json
import logging
import threading
import time
from datetime import datetime, date, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any, TYPE_CHECKING, Tuple, Callable, TypeVar, Union

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Boolean,
    Date,
    DateTime,
    Integer,
    ForeignKey,
    Index,
    UniqueConstraint,
    CheckConstraint,
    Text,
    text,
    select,
    insert,
    and_,
    or_,
    delete,
    update,
    desc,
    event,
    func,
    text,
    case,
    inspect,
    MetaData,
    Table,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError, OperationalError

from src.agent.provider_trace import PROVIDER_TRACE_RETENTION_LIMIT
from src.config import get_config
from src.schemas.decision_profile import extract_legacy_decision_profile
from src.utils.sniper_points import extract_sniper_points, parse_sniper_value

logger = logging.getLogger(__name__)
T = TypeVar("T")
CURRENT_SCHEMA_VERSION = "2026-06-05-create-all-baseline"
INTELLIGENCE_ITEM_NULL_SCOPE_VALUE = "__dsa_null_scope__"

# 与 StockAnalysisPipeline：首页/接口异步单股在保存 analysis_history 时使用；同日同股仅保留最新一条
INTERACTIVE_ANALYSIS_QUERY_SOURCES = frozenset({"api", "web"})


def shanghai_calendar_day_bounds_now() -> tuple[datetime, datetime]:
    """
    上海自然日的 [lo, hi) 区间，用于与 naive 的 created_at 按同一日历日比较。

    假设分析进程的本地时间与业务日一致，或与 Asia/Shanghai 同日界对齐。
    """
    d = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    lo = datetime.combine(d, dt_time.min)
    hi = lo + timedelta(days=1)
    return lo, hi

# SQLAlchemy ORM 基类
Base = declarative_base()

if TYPE_CHECKING:
    from src.search_service import SearchResponse


def utc_naive_now() -> datetime:
    """Return current UTC time without tzinfo for SQLite DateTime columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_utc_naive_datetime(value: datetime) -> datetime:
    """Normalize aware datetimes to UTC-naive; treat naive values as UTC-naive."""
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


# === 数据模型定义 ===

class DatabaseSchemaMigration(Base):
    """Applied database schema version marker."""

    __tablename__ = 'schema_migrations'

    version = Column(String(64), primary_key=True)
    description = Column(String(255), nullable=False)
    applied_at = Column(DateTime, default=datetime.now, nullable=False, index=True)


class StockDaily(Base):
    """
    股票日线数据模型
    
    存储每日行情数据和计算的技术指标
    支持多股票、多日期的唯一约束
    """
    __tablename__ = 'stock_daily'
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 股票代码（如 600519, 000001）
    code = Column(String(10), nullable=False, index=True)
    
    # 交易日期
    date = Column(Date, nullable=False, index=True)
    
    # OHLC 数据
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    
    # 成交数据
    volume = Column(Float)  # 成交量（股）
    amount = Column(Float)  # 成交额（元）
    pct_chg = Column(Float)  # 涨跌幅（%）
    
    # 技术指标
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    volume_ratio = Column(Float)  # 量比
    
    # 数据来源
    data_source = Column(String(50))  # 记录数据来源（如 AkshareFetcher）
    
    # 更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 唯一约束：同一股票同一日期只能有一条数据
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )
    
    def __repr__(self):
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }


class StockAdjFactor(Base):
    """复权因子表（Tushare adj_factor），用于后复权价格计算。

    后复权价格 = 未复权价格 × adj_factor
    前复权价格 = 未复权价格 × adj_factor / adj_factor[latest]
    adj_factor 随时间递增（分红送股时跳升），直接乘即得后复权价格。
    """

    __tablename__ = "stock_adj_factor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    adj_factor = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("code", "trade_date", name="uix_adj_factor_code_date"),
        Index("ix_adj_factor_code_date", "code", "trade_date"),
    )

    def __repr__(self):
        return f"<StockAdjFactor(code={self.code}, date={self.trade_date}, factor={self.adj_factor})>"


class NewsIntel(Base):
    """
    新闻情报数据模型

    存储搜索到的新闻情报条目，用于后续分析与查询
    """
    __tablename__ = 'news_intel'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联用户查询操作
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # 搜索上下文
    dimension = Column(String(32), index=True)  # latest_news / risk_check / earnings / market_analysis / industry
    query = Column(String(255))
    provider = Column(String(32), index=True)

    # 新闻内容
    title = Column(String(300), nullable=False)
    snippet = Column(Text)
    url = Column(String(1000), nullable=False)
    source = Column(String(100))
    published_date = Column(DateTime, index=True)

    # 入库时间
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    query_source = Column(String(32), index=True)  # bot/web/cli/system
    requester_platform = Column(String(20))
    requester_user_id = Column(String(64))
    requester_user_name = Column(String(64))
    requester_chat_id = Column(String(64))
    requester_message_id = Column(String(64))
    requester_query = Column(String(255))

    __table_args__ = (
        UniqueConstraint('url', name='uix_news_url'),
        Index('ix_news_code_pub', 'code', 'published_date'),
    )

    def __repr__(self) -> str:
        return f"<NewsIntel(code={self.code}, title={self.title[:20]}...)>"


class IntelligenceSource(Base):
    """可配置资讯源。"""

    __tablename__ = 'intelligence_sources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True, index=True)
    source_type = Column(String(32), nullable=False, default='rss', index=True)
    url = Column(String(1000), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    scope_type = Column(String(32), nullable=False, default='market', index=True)
    scope_value = Column(String(64), index=True)
    market = Column(String(32), nullable=False, default='cn', index=True)
    description = Column(Text)
    last_status = Column(String(32))
    last_error = Column(Text)
    last_fetched_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_intel_source_scope', 'scope_type', 'scope_value', 'market'),
    )


class IntelligenceItem(Base):
    """沉淀后的资讯 / 情报条目。"""

    __tablename__ = 'intelligence_items'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey('intelligence_sources.id', ondelete='SET NULL'), nullable=True, index=True)
    source_name = Column(String(100), index=True)
    source_type = Column(String(32), nullable=False, default='rss', index=True)
    title = Column(String(300), nullable=False)
    summary = Column(Text)
    url = Column(String(1000), nullable=False, index=True)
    source = Column(String(100))
    published_at = Column(DateTime, index=True)
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    scope_type = Column(String(32), nullable=False, default='market', index=True)
    scope_value = Column(String(64), nullable=False, default=INTELLIGENCE_ITEM_NULL_SCOPE_VALUE, index=True)
    market = Column(String(32), nullable=False, default='cn', index=True)
    raw_payload = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'source_id',
            'url',
            'scope_type',
            'scope_value',
            'market',
            name='uix_intel_item_source_scope_url',
        ),
        Index('ix_intel_item_scope_time', 'scope_type', 'scope_value', 'market', 'published_at'),
        Index('ix_intel_item_fetch_time', 'fetched_at'),
    )


class FundamentalSnapshot(Base):
    """
    基本面上下文快照（P0 write-only）。

    仅用于写入，主链路不依赖读取该表，便于后续回测/画像扩展。
    """
    __tablename__ = 'fundamental_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(String(64), nullable=False, index=True)
    code = Column(String(10), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    source_chain = Column(Text)
    coverage = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_fundamental_snapshot_query_code', 'query_id', 'code'),
        Index('ix_fundamental_snapshot_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<FundamentalSnapshot(query_id={self.query_id}, code={self.code})>"


class RealtimeSpot(Base):
    """盘中实时行情快照。

    每 30 秒由 Scanner 刷新一次，按 code 去重 upsert。
    各盘中因子从此表查询当前行情，避免重复拉取。
    """
    __tablename__ = 'realtime_spot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    price = Column(Float)
    pct_chg = Column(Float)
    pre_close = Column(Float)
    high = Column(Float)
    open_price = Column(Float)
    low = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    turnover_rate = Column(Float)
    volume_ratio = Column(Float)
    trade_date = Column(String(10))  # YYYY-MM-DD
    source = Column(String(20))
    slot = Column(Integer)
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', name='uix_realtime_spot_code'),
    )

    def __repr__(self) -> str:
        return f"<RealtimeSpot(code={self.code}, price={self.price}, slot={self.slot})>"


class LimitPool(Base):
    """统一涨跌停池。

    盘中由 Scanner 每 60 秒用 akshare stock_zt_pool_em 先删后插刷新，
    盘后由 Tushare limit_list_d 全量 upsert 覆盖。
    每日独立保存 (code, trade_date 联合唯一），各因子从此表读取。
    """
    __tablename__ = 'limit_pool'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    limit_type = Column(String(2))         # U/D/Z, None from akshare (涨停)
    pct_chg = Column(Float)
    price = Column(Float)
    limit_times = Column(Integer, default=0)    # 连板数
    open_times = Column(Integer, default=0)     # 炸板/打开次数
    up_stat = Column(String(10))                # 封板状态
    first_seal_time = Column(String(10))        # 首次封板时间
    last_seal_time = Column(String(10))         # 最后封板时间
    break_count = Column(Integer, default=0)    # 炸板次数
    limit_stats = Column(String(50))            # 涨停统计
    sector = Column(String(100))                # 所属行业
    float_market_cap = Column(Float)             # 流通市值（元）
    seal_amount = Column(Float)                  # 封板资金（元）
    source = Column(String(20))                 # akshare / tushare / realtime_spot
    slot = Column(Integer)
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_limit_pool_code_date'),
        Index('ix_limit_pool_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<LimitPool(code={self.code}, date={self.trade_date}, type={self.limit_type})>"


class LimitUpHistory(Base):
    """今日涨停过的股票（只增不删，用于差集检测炸板）。

    每次 _refresh_limit_pool() 新股补入，已存在的不更新（保留首次出现时间）。
    """

    __tablename__ = 'limit_up_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    first_seen = Column(DateTime, default=datetime.now)
    last_seen = Column(DateTime, default=datetime.now)
    open_times = Column(Integer, default=0)
    limit_times = Column(Integer, default=0)
    sector = Column(String(100))
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_limit_up_history_code_date'),
        Index('ix_limit_up_history_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<LimitUpHistory(code={self.code}, date={self.trade_date})>"


class LimitBreak(Base):
    """炸板股票（实时 upsert，盘中检测到差集时写入/更新）。

    status: broke（炸板中）/ recovered（已回封）
    """

    __tablename__ = 'limit_break'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    status = Column(String(10), default="broke")   # broke / recovered
    last_pct_chg = Column(Float)
    last_price = Column(Float)
    open_times = Column(Integer, default=0)
    limit_times = Column(Integer, default=0)
    sector = Column(String(100))
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_limit_break_code_date'),
        Index('ix_limit_break_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<LimitBreak(code={self.code}, status={self.status})>"


class MoneyFlow(Base):
    """个股资金流向（盘后 Tushare moneyflow 落库）。

    每日按 (code, trade_date) 唯一，盘后定时任务全量 upsert。
    """

    __tablename__ = 'money_flow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    buy_elg_amount = Column(Float)
    sell_elg_amount = Column(Float)
    buy_lg_amount = Column(Float)
    sell_lg_amount = Column(Float)
    buy_md_amount = Column(Float)
    sell_md_amount = Column(Float)
    buy_sm_amount = Column(Float)
    sell_sm_amount = Column(Float)
    net_mf_amount = Column(Float)
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_money_flow_code_date'),
        Index('ix_money_flow_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<MoneyFlow(code={self.code}, date={self.trade_date})>"


class MarginDetail(Base):
    """个股融资融券明细（盘后 Tushare margin_detail 落库）。

    每日按 (code, trade_date) 唯一，盘后定时任务全量 upsert。
    """

    __tablename__ = 'margin_detail'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    rzye = Column(Float)      # 融资余额
    rzmre = Column(Float)     # 融资买入额
    rzche = Column(Float)     # 融资偿还额
    rqye = Column(Float)      # 融券余额
    rqmcl = Column(Float)     # 融券卖出量
    rqchl = Column(Float)     # 融券偿还量
    rqyl = Column(Float)      # 融券余量
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_margin_detail_code_date'),
        Index('ix_margin_detail_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<MarginDetail(code={self.code}, date={self.trade_date})>"


class PopularityRank(Base):
    """个股人气排行（盘后 Tushare dc_hot 落库）。

    每日按 (code, trade_date) 唯一，盘后定时任务全量 upsert。
    """

    __tablename__ = 'popularity_rank'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    rank = Column(Integer)
    pct_change = Column(Float)
    hot = Column(Float)
    concept = Column(String(200))
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_popularity_rank_code_date'),
        Index('ix_popularity_rank_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<PopularityRank(code={self.code}, date={self.trade_date}, rank={self.rank})>"


class MomentumSnapshot(Base):
    """盘中资金流快照 (intraday money flow snapshot)。

    按 (code, trade_date) 唯一，盘中每轮扫描 upsert 覆盖当日最新数据。
    字段对齐 MomentumFactor._normalize_eastmoney() 的 7 列输出。
    """

    __tablename__ = 'momentum_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    major_net = Column(Float)
    lg_net = Column(Float)
    inflow_rate = Column(Float)
    pct_chg = Column(Float)
    turnover_rate = Column(Float)
    volume_ratio = Column(Float)
    data_source = Column(String(30))
    source = Column(String(20))
    fetch_time = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_momentum_snapshot_code_date'),
        Index('ix_momentum_snapshot_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<MomentumSnapshot(code={self.code}, date={self.trade_date})>"


class HmDetail(Base):
    """个股游资交易明细（盘后 Tushare hm_detail 落库）。

    按 (code, trade_date, hm_name) 唯一，记录游资每日买卖明细。
    """

    __tablename__ = 'hm_detail'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    trade_date = Column(String(8))
    buy_amount = Column(Float)
    sell_amount = Column(Float)
    net_amount = Column(Float)
    hm_name = Column(String(100))
    hm_orgs = Column(String(200))
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', 'hm_name', name='uix_hm_detail'),
        Index('ix_hm_detail_trade_date', 'trade_date'),
    )

    def __repr__(self) -> str:
        return f"<HmDetail(code={self.code}, date={self.trade_date}, hm={self.hm_name})>"


class HmQuality(Base):
    """游资质量评分（按 hm_name 唯一，每次全量计算后覆盖）。

    由 HmTracker compute_performance 产出，供 HotMoneyFactor 加权使用。
    """

    __tablename__ = 'hm_quality'

    id = Column(Integer, primary_key=True, autoincrement=True)
    hm_name = Column(String(100), unique=True, nullable=False, index=True)
    win_rate = Column(Float)
    avg_return = Column(Float)
    total_trades = Column(Integer)
    quality_score = Column(Float)
    computed_at = Column(DateTime, default=datetime.now)

    def __repr__(self) -> str:
        return f"<HmQuality(hm={self.hm_name}, q={self.quality_score:.1f})>"

class ScreeningRun(Base):
    """A completed built-in screening run persisted by DSA."""

    __tablename__ = 'screening_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, unique=True, index=True)
    strategy = Column(String(64), nullable=False, index=True)
    market = Column(String(16), nullable=False, index=True)
    snapshot_source = Column(String(64), index=True)
    snapshot_count = Column(Integer)
    after_filter_count = Column(Integer)
    candidate_count = Column(Integer, nullable=False, default=0)
    llm_ranked = Column(Boolean)
    daily_enriched = Column(Boolean)
    source_errors_json = Column(Text)
    warnings_json = Column(Text)
    result_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_screening_run_strategy_created', 'strategy', 'created_at'),
        Index('ix_screening_run_market_created', 'market', 'created_at'),
    )


class AnalysisHistory(Base):
    """
    分析结果历史记录模型

    保存每次分析结果，支持按 query_id/股票代码检索
    """
    __tablename__ = 'analysis_history'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联查询链路
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_type = Column(String(16), index=True)

    # 核心结论
    sentiment_score = Column(Integer)
    operation_advice = Column(String(20))
    trend_prediction = Column(String(50))
    analysis_summary = Column(Text)

    # 详细数据
    raw_result = Column(Text)
    news_content = Column(Text)
    context_snapshot = Column(Text)

    # 狙击点位（用于回测）
    ideal_buy = Column(Float)
    secondary_buy = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)

    # 与 StockAnalysisPipeline.query_source 一致：api/web=首页/异步接口；cli/bot/system 等
    query_source = Column(String(32), nullable=True, index=True)

    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_analysis_code_time', 'code', 'created_at'),
    )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'query_id': self.query_id,
            'code': self.code,
            'name': self.name,
            'report_type': self.report_type,
            'sentiment_score': self.sentiment_score,
            'operation_advice': self.operation_advice,
            'trend_prediction': self.trend_prediction,
            'analysis_summary': self.analysis_summary,
            'raw_result': self.raw_result,
            'news_content': self.news_content,
            'context_snapshot': self.context_snapshot,
            'ideal_buy': self.ideal_buy,
            'secondary_buy': self.secondary_buy,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'query_source': self.query_source,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class BacktestResult(Base):
    """单条分析记录的回测结果。"""

    __tablename__ = 'backtest_results'

    id = Column(Integer, primary_key=True, autoincrement=True)

    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        index=True,
    )

    # 冗余字段，便于按股票筛选
    code = Column(String(10), nullable=False, index=True)
    analysis_date = Column(Date, index=True)

    # 回测参数
    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')

    # 状态
    eval_status = Column(String(16), nullable=False, default='pending')
    evaluated_at = Column(DateTime, default=datetime.now, index=True)

    # 建议快照（避免未来分析字段变化导致回测不可解释）
    operation_advice = Column(String(20))
    trigger_source = Column(String(16), index=True)  # auto/manual
    position_recommendation = Column(String(8))  # long/cash

    # 价格与收益
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    # 方向与结果
    direction_expected = Column(String(16))  # up/down/flat/not_down
    direction_correct = Column(Boolean, nullable=True)
    outcome = Column(String(16))  # win/loss/neutral

    # 目标价命中（仅 long 且配置了止盈/止损时有意义）
    stop_loss = Column(Float)
    take_profit = Column(Float)
    hit_stop_loss = Column(Boolean)
    hit_take_profit = Column(Boolean)
    first_hit = Column(String(16))  # take_profit/stop_loss/ambiguous/neither/not_applicable
    first_hit_date = Column(Date)
    first_hit_trading_days = Column(Integer)

    # 模拟执行（long-only）
    simulated_entry_price = Column(Float)
    simulated_exit_price = Column(Float)
    simulated_exit_reason = Column(String(24))  # stop_loss/take_profit/window_end/cash/ambiguous_stop_loss
    simulated_return_pct = Column(Float)

    __table_args__ = (
        UniqueConstraint(
            'analysis_history_id',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_analysis_window_version',
        ),
        Index('ix_backtest_code_date', 'code', 'analysis_date'),
    )


class BacktestSummary(Base):
    """回测汇总指标（按股票或全局）。"""

    __tablename__ = 'backtest_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)

    scope = Column(String(16), nullable=False, index=True)  # overall/stock
    code = Column(String(16), index=True)

    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')
    computed_at = Column(DateTime, default=datetime.now, index=True)

    # 计数
    total_evaluations = Column(Integer, default=0)
    completed_count = Column(Integer, default=0)
    insufficient_count = Column(Integer, default=0)
    long_count = Column(Integer, default=0)
    cash_count = Column(Integer, default=0)

    win_count = Column(Integer, default=0)
    loss_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)

    # 准确率/胜率
    direction_accuracy_pct = Column(Float)
    win_rate_pct = Column(Float)
    neutral_rate_pct = Column(Float)

    # 收益
    avg_stock_return_pct = Column(Float)
    avg_simulated_return_pct = Column(Float)

    # 目标价触发统计（仅 long 且配置止盈/止损时统计）
    stop_loss_trigger_rate = Column(Float)
    take_profit_trigger_rate = Column(Float)
    ambiguous_rate = Column(Float)
    avg_days_to_first_hit = Column(Float)

    # 诊断字段（JSON 字符串）
    advice_breakdown_json = Column(Text)
    diagnostics_json = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'scope',
            'code',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_summary_scope_code_window_version',
        ),
    )


def compute_param_fingerprint(req_dict: dict) -> str:
    """Compute a deterministic SHA-256 hash of backtest parameters.

    Normalizes: sorts factor_weights keys and hold_days, compact JSON.
    """
    canonical = {
        "factor_weights": dict(sorted(req_dict.get("factor_weights", {}).items())),
        "start_date": req_dict.get("start_date"),
        "end_date": req_dict.get("end_date"),
        "top_n": req_dict.get("top_n", 5),
        "hold_days": sorted(req_dict.get("hold_days", [1, 3, 5, 10, 20])),
        "initial_capital": req_dict.get("initial_capital", 1_000_000.0),
        "risk_free_rate": req_dict.get("risk_free_rate", 0.02),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


class SimpleFactorBacktestCache(Base):
    """快测回测结果缓存，以参数指纹为唯一键。"""

    __tablename__ = 'simple_factor_backtest_cache'

    id = Column(Integer, primary_key=True, autoincrement=True)
    param_fingerprint = Column(String(64), nullable=False, unique=True)

    # 参数副本（用于列表展示，避免解析大 JSON）
    factor_weights_json = Column(Text, nullable=False)
    start_date = Column(String(16))
    end_date = Column(String(16))
    top_n = Column(Integer, nullable=False, default=5)
    hold_days_json = Column(String(128), nullable=False)
    initial_capital = Column(Float, nullable=False, default=1_000_000.0)
    risk_free_rate = Column(Float, nullable=False, default=0.02)

    # 完整回测结果
    result_json = Column(Text, nullable=False)

    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)

    __table_args__ = (
        Index('ix_sfb_cache_created', 'created_at'),
    )


class PortfolioAccount(Base):
    """Portfolio account metadata."""

    __tablename__ = 'portfolio_accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(String(64), index=True)
    name = Column(String(64), nullable=False)
    broker = Column(String(64))
    market = Column(String(8), nullable=False, default='cn', index=True)  # cn/hk/us
    base_currency = Column(String(8), nullable=False, default='CNY')
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index('ix_portfolio_account_owner_active', 'owner_id', 'is_active'),
    )


class PortfolioTrade(Base):
    """Executed trade events used as the source of truth for replay."""

    __tablename__ = 'portfolio_trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    trade_uid = Column(String(128))
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    trade_date = Column(Date, nullable=False, index=True)
    side = Column(String(8), nullable=False)  # buy/sell
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fee = Column(Float, default=0.0)
    tax = Column(Float, default=0.0)
    note = Column(String(255))
    dedup_hash = Column(String(64), index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('account_id', 'trade_uid', name='uix_portfolio_trade_uid'),
        UniqueConstraint('account_id', 'dedup_hash', name='uix_portfolio_trade_dedup_hash'),
        Index('ix_portfolio_trade_account_date', 'account_id', 'trade_date'),
    )


class PortfolioCashLedger(Base):
    """Cash in/out events."""

    __tablename__ = 'portfolio_cash_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    direction = Column(String(8), nullable=False)  # in/out
    amount = Column(Float, nullable=False)
    currency = Column(String(8), nullable=False, default='CNY')
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_cash_account_date', 'account_id', 'event_date'),
    )


class PortfolioCorporateAction(Base):
    """Corporate actions that impact cash or share quantity."""

    __tablename__ = 'portfolio_corporate_actions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    effective_date = Column(Date, nullable=False, index=True)
    action_type = Column(String(24), nullable=False)  # cash_dividend/split_adjustment
    cash_dividend_per_share = Column(Float)
    split_ratio = Column(Float)
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_ca_account_date', 'account_id', 'effective_date'),
    )


class PortfolioPosition(Base):
    """Latest replayed position snapshot for each symbol in one account."""

    __tablename__ = 'portfolio_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    quantity = Column(Float, nullable=False, default=0.0)
    avg_cost = Column(Float, nullable=False, default=0.0)
    total_cost = Column(Float, nullable=False, default=0.0)
    last_price = Column(Float, nullable=False, default=0.0)
    market_value_base = Column(Float, nullable=False, default=0.0)
    unrealized_pnl_base = Column(Float, nullable=False, default=0.0)
    valuation_currency = Column(String(8), nullable=False, default='CNY')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'symbol',
            'market',
            'currency',
            'cost_method',
            name='uix_portfolio_position_account_symbol_market_currency',
        ),
    )


class PortfolioPositionLot(Base):
    """Lot-level remaining quantities used by FIFO replay."""

    __tablename__ = 'portfolio_position_lots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    open_date = Column(Date, nullable=False, index=True)
    remaining_quantity = Column(Float, nullable=False, default=0.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    source_trade_id = Column(Integer, ForeignKey('portfolio_trades.id'))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_lot_account_symbol', 'account_id', 'symbol'),
    )


class PortfolioDailySnapshot(Base):
    """Daily account snapshot generated by read-time replay."""

    __tablename__ = 'portfolio_daily_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')  # fifo/avg
    base_currency = Column(String(8), nullable=False, default='CNY')
    total_cash = Column(Float, nullable=False, default=0.0)
    total_market_value = Column(Float, nullable=False, default=0.0)
    total_equity = Column(Float, nullable=False, default=0.0)
    unrealized_pnl = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    fee_total = Column(Float, nullable=False, default=0.0)
    tax_total = Column(Float, nullable=False, default=0.0)
    fx_stale = Column(Boolean, nullable=False, default=False)
    payload = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'snapshot_date',
            'cost_method',
            name='uix_portfolio_snapshot_account_date_method',
        ),
    )


class PortfolioFxRate(Base):
    """Cached FX rates used for cross-currency portfolio conversion."""

    __tablename__ = 'portfolio_fx_rates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_currency = Column(String(8), nullable=False, index=True)
    to_currency = Column(String(8), nullable=False, index=True)
    rate_date = Column(Date, nullable=False, index=True)
    rate = Column(Float, nullable=False)
    source = Column(String(32), nullable=False, default='manual')
    is_stale = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'from_currency',
            'to_currency',
            'rate_date',
            name='uix_portfolio_fx_pair_date',
        ),
    )


class ConversationMessage(Base):
    """
    Agent 对话历史记录表
    """
    __tablename__ = 'conversation_messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), index=True, nullable=False)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, index=True)


class ConversationSessionState(Base):
    """Persisted user selections for an Agent chat session."""

    __tablename__ = 'conversation_session_states'

    session_id = Column(String(100), primary_key=True)
    selected_skill_ids_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)


class ConversationSummary(Base):
    """Rolling summary for visible Agent chat history."""

    __tablename__ = 'conversation_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, unique=True, index=True)
    summary = Column(Text, nullable=False)
    covered_message_id = Column(Integer, nullable=False, default=0)
    source_message_count = Column(Integer, nullable=False, default=0)
    estimated_tokens = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)


class AgentProviderTurn(Base):
    """Provider protocol trace required for thinking/tool-call roundtrip."""

    __tablename__ = 'agent_provider_turns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, index=True)
    run_id = Column(String(64), nullable=False, index=True)
    provider = Column(String(64), nullable=False, index=True)
    model = Column(String(160), nullable=False, index=True)
    anchor_user_message_id = Column(Integer, nullable=False, index=True)
    anchor_assistant_message_id = Column(Integer, nullable=False, index=True)
    messages_json = Column(Text, nullable=False)
    contains_reasoning = Column(Boolean, nullable=False, default=False)
    contains_tool_calls = Column(Boolean, nullable=False, default=False)
    contains_thinking_blocks = Column(Boolean, nullable=False, default=False)
    must_roundtrip = Column(Boolean, nullable=False, default=False, index=True)
    estimated_tokens = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_agent_provider_turn_bucket', 'session_id', 'provider', 'model', 'must_roundtrip'),
    )


class LLMUsage(Base):
    """One row per litellm.completion() call — token-usage audit log."""

    __tablename__ = 'llm_usage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 'analysis' | 'agent' | 'market_review'
    call_type = Column(String(32), nullable=False, index=True)
    model = Column(String(128), nullable=False)
    stock_code = Column(String(16), nullable=True)
    provider = Column(String(64), nullable=True)
    prompt_tokens = Column(Integer, nullable=False, default=0)
    completion_tokens = Column(Integer, nullable=False, default=0)
    total_tokens = Column(Integer, nullable=False, default=0)

    # Sanitized provider usage snapshot; raw prompts, messages, headers, and
    # tokenizer free-text fields are intentionally not persisted here.
    provider_usage_json = Column(Text, nullable=True)
    provider_usage_schema_name = Column(String(64), nullable=True)
    provider_usage_schema_version = Column(String(32), nullable=True)
    provider_usage_observed_at = Column(String(32), nullable=True)

    # Normalized telemetry values are derived from provider usage and may stay
    # NULL when the provider payload is absent or explicitly invalid.
    normalized_prompt_tokens = Column(Integer, nullable=True)
    normalized_completion_tokens = Column(Integer, nullable=True)
    normalized_total_tokens = Column(Integer, nullable=True)
    normalized_cache_read_tokens = Column(Integer, nullable=True)
    normalized_cache_write_tokens = Column(Integer, nullable=True)
    normalized_cache_miss_tokens = Column(Integer, nullable=True)
    normalized_uncached_input_tokens = Column(Integer, nullable=True)
    normalized_cache_eligible_input_tokens = Column(Integer, nullable=True)
    normalized_cache_hit_ratio = Column(Float, nullable=True)
    normalized_cache_write_ratio = Column(Float, nullable=True)
    cache_capability = Column(String(32), nullable=True)
    cache_eligibility = Column(String(32), nullable=True)
    cache_observation = Column(String(32), nullable=True)
    estimated_prefix_tokens = Column(Integer, nullable=True)
    provider_reported_prompt_tokens = Column(Integer, nullable=True)
    provider_reported_cached_tokens = Column(Integer, nullable=True)
    provider_min_cache_tokens = Column(Integer, nullable=True)
    eligibility_confidence = Column(String(32), nullable=True)

    # Kept nullable for schema compatibility; new writes do not store provider
    # or proxy tokenizer free-text values.
    tokenizer_name = Column(String(128), nullable=True)
    tokenizer_version = Column(String(64), nullable=True)

    # HMAC fingerprints let deployments compare message shapes without storing
    # raw prompt/message content.
    messages_hmac = Column(String(64), nullable=True)
    system_message_hmac = Column(String(64), nullable=True)
    user_message_hmac = Column(String(64), nullable=True)
    hmac_key_version = Column(String(64), nullable=True)
    hmac_domain = Column(String(32), nullable=True)
    hash_scope = Column(String(32), nullable=True)

    # P0.5a internal legacy message stability audit. These diagnostics are
    # stored locally only and are not returned by public usage APIs.
    language = Column(String(16), nullable=True)
    market_group = Column(String(16), nullable=True)
    analysis_mode = Column(String(64), nullable=True)
    legacy_prompt_mode = Column(String(32), nullable=True)
    skill_config_hmac = Column(String(64), nullable=True)
    transport = Column(String(64), nullable=True)
    message_count = Column(Integer, nullable=True)
    estimated_total_prompt_tokens = Column(Integer, nullable=True)
    approx_common_prefix_chars = Column(Integer, nullable=True)
    approx_common_prefix_tokens = Column(Integer, nullable=True)
    known_dynamic_marker_positions = Column(Text, nullable=True)
    called_at = Column(DateTime, default=datetime.now, index=True)


class StockTechIndicator(Base):
    """Tushare stk_factor_pro 技术指标缓存。

    缓存 Tushare 预计算的前复权技术指标（MACD/RSI/KDJ/BOLL/CCI/ATR/MA），
    避免重复调用 Tushare API，节省积分和请求配额。
    """

    __tablename__ = 'stock_tech_indicator'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)

    # Tushare stk_factor_pro 字段（前复权口径）
    close_qfq = Column(Float)
    macd_dif = Column(Float)
    macd_dea = Column(Float)
    macd = Column(Float)
    rsi_6 = Column(Float)
    rsi_12 = Column(Float)
    rsi_24 = Column(Float)
    kdj_k = Column(Float)
    kdj_d = Column(Float)
    kdj_j = Column(Float)
    boll_upper = Column(Float)
    boll_mid = Column(Float)
    boll_lower = Column(Float)
    cci = Column(Float)
    vol = Column(Float)
    atr = Column(Float)
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    ma60 = Column(Float)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_tech_indicator_code_date'),
        Index('ix_tech_indicator_code_date', 'code', 'date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'date': self.date.isoformat() if self.date else None,
            'close_qfq': self.close_qfq,
            'macd_dif': self.macd_dif,
            'macd_dea': self.macd_dea,
            'macd': self.macd,
            'rsi_6': self.rsi_6,
            'rsi_12': self.rsi_12,
            'rsi_24': self.rsi_24,
            'kdj_k': self.kdj_k,
            'kdj_d': self.kdj_d,
            'kdj_j': self.kdj_j,
            'boll_upper': self.boll_upper,
            'boll_mid': self.boll_mid,
            'boll_lower': self.boll_lower,
            'cci': self.cci,
            'vol': self.vol,
            'atr': self.atr,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma60': self.ma60,
        }


class DailyBasic(Base):
    """每日基本面指标缓存 (Tushare daily_basic)。

    存储 PE/PB/换手率/量比/总市值等日频估值指标，
    供 FundamentalFactor、MarginFactor 等盘后因子复用。
    """

    __tablename__ = 'daily_basic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(String(8))
    turnover_rate = Column(Float)  # 换手率（%）
    volume_ratio = Column(Float)   # 量比
    pe = Column(Float)             # 市盈率
    pb = Column(Float)             # 市净率
    total_mv = Column(Float)       # 总市值（元）
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_daily_basic_code_date'),
        Index('ix_daily_basic_trade_date', 'trade_date'),
    )


class BrokerRecommendMonthly(Base):
    """券商月度金股推荐快照。

    存储每月各券商金股推荐数据，用于历史回测和分析。
    """

    __tablename__ = 'broker_recommend_monthly'

    id = Column(Integer, primary_key=True, autoincrement=True)
    month = Column(String(6), nullable=False, index=True)
    broker = Column(String(100), nullable=False)
    ts_code = Column(String(12), nullable=False, index=True)
    name = Column(String(50))
    broker_count = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('month', 'broker', 'ts_code', name='uix_br_month_broker_ts'),
        Index('ix_br_month_ts', 'month', 'ts_code'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'month': self.month,
            'broker': self.broker,
            'ts_code': self.ts_code,
            'name': self.name,
            'broker_count': self.broker_count,
        }


class InstitutionSurvey(Base):
    """机构调研明细。

    存储每日机构调研原始记录，每条记录对应一次调研活动。
    """

    __tablename__ = 'institution_survey'

    id = Column(Integer, primary_key=True, autoincrement=True)
    surv_date = Column(String(8), nullable=False, index=True)
    ts_code = Column(String(12), nullable=False, index=True)
    name = Column(String(50))
    rece_org = Column(String(200))
    org_type = Column(String(50))
    rece_mode = Column(String(100))
    weight = Column(Float, default=0.0)
    fund_visitors = Column(String(200))
    rece_place = Column(String(200))
    comp_rece = Column(String(200))
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('surv_date', 'ts_code', 'rece_org', name='uix_is_surv_date_ts_org'),
        Index('ix_is_surv_date', 'surv_date'),
        Index('ix_is_ts_code', 'ts_code'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'surv_date': self.surv_date,
            'ts_code': self.ts_code,
            'name': self.name,
            'rece_org': self.rece_org,
            'org_type': self.org_type,
            'rece_mode': self.rece_mode,
            'weight': self.weight,
            'fund_visitors': self.fund_visitors,
            'rece_place': self.rece_place,
            'comp_rece': self.comp_rece,
        }


class HkGgtComponent(Base):
    """港股通成份股每日快照 (AkShare stock_hk_ggt_components_em)。"""

    __tablename__ = 'hk_ggt_component'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(String(8), nullable=False, index=True)
    hk_code = Column(String(5), nullable=False, index=True)
    name = Column(String(100))
    latest_price = Column(Float)
    pct_change = Column(Float)
    change_amount = Column(Float)
    open_price = Column(Float)
    high = Column(Float)
    low = Column(Float)
    prev_close = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('trade_date', 'hk_code', name='uix_hk_ggt_comp_date_code'),
        Index('ix_hk_ggt_comp_date', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trade_date': self.trade_date,
            'hk_code': self.hk_code,
            'name': self.name,
            'latest_price': self.latest_price,
            'pct_change': self.pct_change,
            'change_amount': self.change_amount,
            'open': self.open_price,
            'high': self.high,
            'low': self.low,
            'prev_close': self.prev_close,
            'volume': self.volume,
            'amount': self.amount,
        }


class HkGgtMinuteBar(Base):
    """港股通成份股分钟行情快照。"""

    __tablename__ = 'hk_ggt_minute_bar'

    id = Column(Integer, primary_key=True, autoincrement=True)
    hk_code = Column(String(5), nullable=False, index=True)
    trade_date = Column(String(8), nullable=False, index=True)
    bar_time = Column(String(19), nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float, nullable=False)
    prev_close = Column(Float)
    pct_change = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    avg_price = Column(Float)
    period = Column(String(8), nullable=False, default='1')
    source = Column(String(32), nullable=False, default='tushare_rt')
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('hk_code', 'bar_time', 'period', name='uix_hk_ggt_minute_code_time_period'),
        Index('ix_hk_ggt_minute_date_code_time', 'trade_date', 'hk_code', 'bar_time'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'hk_code': self.hk_code,
            'trade_date': self.trade_date,
            'bar_time': self.bar_time,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'prev_close': self.prev_close,
            'pct_change': self.pct_change,
            'volume': self.volume,
            'amount': self.amount,
            'avg_price': self.avg_price,
            'period': self.period,
            'source': self.source,
        }


class HkStockDaily(Base):
    """港股通个股日线数据。"""

    __tablename__ = 'hk_stock_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    hk_code = Column(String(10), nullable=False, index=True)
    trade_date = Column(String(8), nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    pct_chg = Column(Float)

    __table_args__ = (
        UniqueConstraint('hk_code', 'trade_date', name='uix_hk_stock_daily_code_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hk_code": self.hk_code,
            "trade_date": self.trade_date,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "pct_chg": self.pct_chg,
        }




class InstitutionHold(Base):
    """机构持仓季度汇总 (akshare stock_institute_hold 落库)。

    按 (code, quarter) 唯一，存储每季度机构持股汇总数据。
    quarter 由刷新时的系统日期推导（如 '2025Q1'），非 API 直接返回。
    """

    __tablename__ = 'institution_hold'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    quarter = Column(String(6), nullable=False, index=True)
    name = Column(String(50))
    inst_count = Column(Integer)
    inst_count_change = Column(Integer)
    hold_ratio = Column(Float)
    hold_ratio_change = Column(Float)
    circulate_ratio = Column(Float)
    circulate_ratio_change = Column(Float)
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'quarter', name='uix_institution_hold_code_quarter'),
    )

    def __repr__(self) -> str:
        return (f"<InstitutionHold(code={self.code}, quarter={self.quarter}, "
                f"count={self.inst_count})>")


class Repurchase(Base):
    """股票回购数据 (Tushare repurchase, doc_id 124 落库)。

    按 (ts_code, ann_date) 唯一，存储上市公司回购公告明细。
    """

    __tablename__ = 'repurchase'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(12), nullable=False, index=True)
    ann_date = Column(String(8), nullable=False, index=True)
    end_date = Column(String(8))
    proc = Column(String(50))
    exp_date = Column(String(8))
    vol = Column(Float)
    amount = Column(Float)
    high_limit = Column(Float)
    low_limit = Column(Float)
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'ann_date',
                         name='uix_repurchase_ts_code_ann_date'),
    )

    def __repr__(self) -> str:
        return (f"<Repurchase(ts_code={self.ts_code}, ann_date={self.ann_date}, "
                f"proc={self.proc})>")


class ScanResultIntraday(Base):
    """盘中扫描全量结果（每轮覆盖当日全市场股票评分）。

    按 (scan_date, ts_code) 唯一约束，同一天多轮扫描会覆盖前一轮数据。
    factor_scores_json 存储各因子的加权得分 JSON。
    """

    __tablename__ = 'scan_result_intraday'

    id = Column(Integer, primary_key=True, autoincrement=True)
    scan_date = Column(String(8), nullable=False, index=True)
    scan_round = Column(Integer, default=0)
    scan_time = Column(String(6), nullable=False, default="")
    ts_code = Column(String(12), nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50))
    rank = Column(Integer, nullable=False)
    total_score = Column(Float)
    tech_score = Column(Float, nullable=True)
    composite_score = Column(Float, nullable=True)
    factor_scores_json = Column(Text)
    sector = Column(String(100), default="")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('scan_date', 'ts_code', name='uix_isr_date_code'),
        Index('ix_isr_scan_date', 'scan_date'),
        Index('ix_isr_rank', 'scan_date', 'rank'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'scan_date': self.scan_date,
            'scan_round': self.scan_round,
            'scan_time': self.scan_time,
            'ts_code': self.ts_code,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'rank': self.rank,
            'total_score': self.total_score,
            'factor_scores': json.loads(self.factor_scores_json or "{}"),
            'sector': self.sector,
        }


class ScanResultPostmarket(Base):
    """盘后扫描全量结果（每日覆盖当日全市场股票评分）。

    结构同 ScanResultIntraday，scan_round 恒为 0。
    """

    __tablename__ = 'scan_result_postmarket'

    id = Column(Integer, primary_key=True, autoincrement=True)
    scan_date = Column(String(8), nullable=False, index=True)
    scan_round = Column(Integer, default=0)
    scan_time = Column(String(6), nullable=False, default="")
    ts_code = Column(String(12), nullable=False, index=True)
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50))
    rank = Column(Integer, nullable=False)
    total_score = Column(Float)
    tech_score = Column(Float, nullable=True)
    composite_score = Column(Float, nullable=True)
    factor_scores_json = Column(Text)
    sector = Column(String(100), default="")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('scan_date', 'ts_code', name='uix_psr_date_code'),
        Index('ix_psr_scan_date', 'scan_date'),
        Index('ix_psr_rank', 'scan_date', 'rank'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'scan_date': self.scan_date,
            'scan_round': self.scan_round,
            'scan_time': self.scan_time,
            'ts_code': self.ts_code,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'rank': self.rank,
            'total_score': self.total_score,
            'tech_score': self.tech_score,
            'composite_score': self.composite_score,
            'factor_scores': json.loads(self.factor_scores_json or "{}"),
            'sector': self.sector,
        }


class FactorScoreSnapshot(Base):
    """因子得分快照（每轮扫描后保存每只股票在各因子上的原始得分）。

    按 (trade_date, ts_code, mode, factor_name) 唯一约束。
    同一 mode + trade_date 的新扫描会覆盖旧数据。
    """
    __tablename__ = 'factor_score_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(String(8), nullable=False, index=True)
    ts_code = Column(String(12), nullable=False, index=True)
    mode = Column(String(16), nullable=False, index=True)
    factor_name = Column(String(64), nullable=False)
    score = Column(Float)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'trade_date', 'ts_code', 'mode', 'factor_name',
            name='uix_fss_date_code_mode_factor',
        ),
        Index('ix_fss_date_mode', 'trade_date', 'mode'),
        Index('ix_fss_mode_factor_date', 'mode', 'factor_name', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trade_date': self.trade_date,
            'ts_code': self.ts_code,
            'mode': self.mode,
            'factor_name': self.factor_name,
            'score': self.score,
        }


class BrokerBacktestResult(Base):
    """券商金股月度回测结果快照。

    存储每月的回测收益数据（不含增强数据如九转/盈利预测/筹码），
    增强数据通过独立的 enrichment 缓存机制管理。
    """

    __tablename__ = 'broker_backtest_result'

    id = Column(Integer, primary_key=True, autoincrement=True)
    month = Column(String(6), nullable=False, unique=True, index=True)
    buy_date = Column(String(8), nullable=False)
    sell_date = Column(String(8), nullable=False)
    total_recommendations = Column(Integer, default=0)
    unique_stocks = Column(Integer, default=0)
    unique_brokers = Column(Integer, default=0)
    stock_returns_json = Column(Text)
    broker_returns_json = Column(Text)
    computed_at = Column(DateTime, default=datetime.now)


class BrokerEnrichmentNineturn(Base):
    """九转信号增强数据缓存。

    按 (ts_code, trade_date) 缓存，过去月份的 trade_date 固定，永久有效；
    当前月份 trade_date 随交易日刷新，实现每日自动更新。
    """

    __tablename__ = 'broker_enrichment_nineturn'

    ts_code = Column(String(12), primary_key=True)
    trade_date = Column(String(8), primary_key=True)
    up_count = Column(Integer, default=0)
    down_count = Column(Integer, default=0)
    nine_up_turn = Column(Integer, default=0)
    nine_down_turn = Column(Integer, default=0)
    cached_at = Column(DateTime, default=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "up_count": self.up_count,
            "down_count": self.down_count,
            "nine_up_turn": self.nine_up_turn,
            "nine_down_turn": self.nine_down_turn,
        }


class BrokerEnrichmentForecast(Base):
    """盈利预测增强数据缓存。"""

    __tablename__ = 'broker_enrichment_forecast'

    ts_code = Column(String(12), primary_key=True)
    trade_date = Column(String(8), primary_key=True)
    eps = Column(Float)
    pe = Column(Float)
    roe = Column(Float)
    np = Column(Float)
    rating = Column(String(50))
    min_price = Column(Float)
    max_price = Column(Float)
    imp_dg = Column(String(200))
    cached_at = Column(DateTime, default=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eps": self.eps,
            "pe": self.pe,
            "roe": self.roe,
            "np": self.np,
            "rating": self.rating or "",
            "min_price": self.min_price,
            "max_price": self.max_price,
            "imp_dg": self.imp_dg or "",
        }


class InsiderBuy(Base):
    """险资举牌事件缓存。"""

    __tablename__ = 'insider_buy'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(12), nullable=False)
    stock_name = Column(String(50))
    announce_date = Column(String(10))
    buyer = Column(String(200))
    buy_shares = Column(Float)
    avg_price = Column(Float)
    add_ratio = Column(Float)
    hold_shares = Column(Float)
    hold_ratio = Column(Float)
    source = Column(String(50), default="akshare")
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'announce_date', 'buyer', name='uix_insider_buy'),
    )


class BrokerEnrichmentCyqPerf(Base):
    """筹码胜率增强数据缓存。"""

    __tablename__ = 'broker_enrichment_cyq_perf'

    ts_code = Column(String(12), primary_key=True)
    trade_date = Column(String(8), primary_key=True)
    winner_rate = Column(Float)
    cost_5pct = Column(Float)
    cost_15pct = Column(Float)
    cost_50pct = Column(Float)
    cost_85pct = Column(Float)
    cost_95pct = Column(Float)
    weight_avg = Column(Float)
    his_low = Column(Float)
    his_high = Column(Float)
    cached_at = Column(DateTime, default=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "winner_rate": self.winner_rate,
            "cost_5pct": self.cost_5pct,
            "cost_15pct": self.cost_15pct,
            "cost_50pct": self.cost_50pct,
            "cost_85pct": self.cost_85pct,
            "cost_95pct": self.cost_95pct,
            "weight_avg": self.weight_avg,
            "his_low": self.his_low,
            "his_high": self.his_high,
        }


class ProfitForecast(Base):
    """盈利预测快照 (akshare stock_profit_forecast_em 落库)。

    按 (trade_date, ts_code) 唯一，每次刷新全量覆盖当日数据。
    """

    __tablename__ = 'profit_forecast'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(String(8), nullable=False, index=True)
    ts_code = Column(String(12), nullable=False, index=True)
    name = Column(String(50))
    report_count = Column(Integer)
    buy_count = Column(Integer)
    add_count = Column(Integer)
    neutral_count = Column(Integer)
    reduce_count = Column(Integer)
    sell_count = Column(Integer)
    eps_2025 = Column(Float)
    eps_2026 = Column(Float)
    eps_2027 = Column(Float)
    eps_2028 = Column(Float)
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('trade_date', 'ts_code', name='uix_pf_date_code'),
        Index('ix_pf_trade_date', 'trade_date'),
    )


class PerformanceReport(Base):
    """业绩报表快照（akshare stock_yjbb_em 按季度落库）。

    按 (code, report_period) 唯一，每次刷新全量覆盖该季度数据。
    """

    __tablename__ = 'performance_report'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_period = Column(String(8), nullable=False, index=True)
    report_date = Column(String(10))
    eps = Column(Float)
    total_revenue = Column(Float)
    revenue_yoy = Column(Float)
    revenue_qoq = Column(Float)
    net_profit = Column(Float)
    net_profit_yoy = Column(Float)
    net_profit_qoq = Column(Float)
    bps = Column(Float)
    roe = Column(Float)
    ocf_per_share = Column(Float)
    gross_margin = Column(Float)
    industry = Column(String(50))
    source = Column(String(20))
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_period', name='uix_pr_code_period'),
        Index('ix_pr_report_period', 'report_period'),
    )


class ThsIndustryMap(Base):
    """同花顺行业映射 (stock_code → 同花顺 industry name).

    按 stock_code 唯一，由 scripts/build_ths_industry_map.py 定期全量刷新。
    """

    __tablename__ = 'ths_industry_map'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False, unique=True, index=True)
    industry_name = Column(String(100), nullable=False)
    source = Column(String(20), default="tushare")
    updated_at = Column(DateTime, default=datetime.now)

    def __repr__(self) -> str:
        return f"<ThsIndustryMap(code={self.stock_code}, industry={self.industry_name})>"


class ThsConceptMap(Base):
    """同花顺概念映射 (stock_code → 同花顺 concept name).

    按 (stock_code, concept_name) 唯一，一只股票可属于多个概念。
    由 scripts/build_ths_concept_map.py 定期全量刷新。
    """

    __tablename__ = 'ths_concept_map'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False, index=True)
    concept_name = Column(String(100), nullable=False)
    source = Column(String(20), default="tushare")
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (UniqueConstraint('stock_code', 'concept_name', name='uq_code_concept'),)

    def __repr__(self) -> str:
        return f"<ThsConceptMap(code={self.stock_code}, concept={self.concept_name})>"


class EtfDaily(Base):
    """ETF 日线行情。

    数据来源：Tushare fund_daily，回填 2026 年至今。
    按 (code, date) 唯一。
    """

    __tablename__ = 'etf_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    name = Column(String(50))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    pct_chg = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_etf_daily_code_date'),
        Index('ix_etf_daily_code_date', 'code', 'date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "date": self.date.strftime("%Y%m%d") if hasattr(self.date, "strftime") else str(self.date),
            "name": self.name,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "pct_chg": self.pct_chg,
        }


class FundAdjFactor(Base):
    """ETF 复权因子表（Tushare fund_adj），用于计算后复权价格。

    后复权价格 = 未复权价格 × adj_factor
    按 (code, date) 唯一。
    """

    __tablename__ = 'fund_adj_factor'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    adj_factor = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_fund_adj_code_date'),
        Index('ix_fund_adj_code_date', 'code', 'trade_date'),
    )

    def __repr__(self):
        return f"<FundAdjFactor(code={self.code}, date={self.trade_date}, factor={self.adj_factor})>"


class GlobalIndexDaily(Base):
    """全球主要指数日线行情。

    数据来源：Tushare index_global API，回填 2026 年至今。
    按 (ts_code, trade_date) 唯一。ts_code 为 Tushare 定义的指数代码（如 DJI, SPX, IXIC）。
    """

    __tablename__ = 'global_index_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True)
    trade_date = Column(String(8), nullable=False, index=True)
    name = Column(String(100))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    pct_chg = Column(Float)
    change = Column(Float)
    swing = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_global_index_code_date'),
        Index('ix_global_index_code_date', 'ts_code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts_code": self.ts_code,
            "trade_date": self.trade_date,
            "name": self.name,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "pre_close": self.pre_close,
            "pct_chg": self.pct_chg,
            "change": self.change,
            "swing": self.swing,
            "vol": self.vol,
            "amount": self.amount,
        }


class IndexBasic(Base):
    """A 股指数基本信息（Tushare index_basic）。按 ts_code 唯一。"""

    __tablename__ = 'index_basic'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True, unique=True)
    name = Column(String(100))
    fullname = Column(String(200))
    market = Column(String(20))
    publisher = Column(String(50))
    index_type = Column(String(50))
    category = Column(String(50))
    base_date = Column(String(8))
    list_date = Column(String(8))
    created_at = Column(DateTime, default=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {"ts_code": self.ts_code, "name": self.name, "fullname": self.fullname,
                "market": self.market, "publisher": self.publisher, "index_type": self.index_type,
                "category": self.category}

    def __repr__(self): return f"<IndexBasic(ts_code={self.ts_code}, name={self.name})>"


class IndexDaily(Base):
    """A 股指数日线行情（Tushare index_daily）。按 (ts_code, trade_date) 唯一。"""

    __tablename__ = 'index_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True)
    trade_date = Column(String(8), nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_index_daily_code_date'),
        Index('ix_index_daily_code_date', 'ts_code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {"ts_code": self.ts_code, "trade_date": self.trade_date,
                "open": self.open, "high": self.high, "low": self.low, "close": self.close,
                "pre_close": self.pre_close, "pct_chg": self.pct_chg}

    def __repr__(self): return f"<IndexDaily(ts_code={self.ts_code}, date={self.trade_date})>"


class IndexWeekly(Base):
    """A 股指数周线行情（Tushare index_weekly）。trade_date 为当周最后一个交易日。按 (ts_code, trade_date) 唯一。"""

    __tablename__ = 'index_weekly'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, index=True)
    trade_date = Column(String(8), nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_index_weekly_code_date'),
        Index('ix_index_weekly_code_date', 'ts_code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {"ts_code": self.ts_code, "trade_date": self.trade_date,
                "open": self.open, "high": self.high, "low": self.low, "close": self.close,
                "pre_close": self.pre_close, "pct_chg": self.pct_chg}

    def __repr__(self): return f"<IndexWeekly(ts_code={self.ts_code}, date={self.trade_date})>"


class IndexConstituent(Base):
    """指数成分股及权重（Tushare index_weight）。

    按 (index_code, con_code) 唯一，trade_date 为最近更新日期。
    """

    __tablename__ = 'index_constituent'

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(String(20), nullable=False, index=True)
    con_code = Column(String(10), nullable=False)
    con_name = Column(String(50))
    weight = Column(Float)
    trade_date = Column(String(8), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('index_code', 'con_code', name='uix_index_constituent_code'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "con_code": self.con_code,
            "con_name": self.con_name,
            "weight": self.weight,
            "trade_date": self.trade_date,
        }

    def __repr__(self): return f"<IndexConstituent(index={self.index_code}, con={self.con_code})>"


class SectorDaily(Base):
    """板块日线历史行情（用于 StockScorer 板块状态判定）。

    数据来源：akshare stock_board_industry_hist_em，盘后全量刷新近60日。
    按 (sector_name, trade_date) 唯一。
    """

    __tablename__ = 'sector_daily'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sector_name = Column(String(100), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    pct_chg = Column(Float)  # 涨跌幅 %
    updated_at = Column(DateTime, default=datetime.now)

    __table_args__ = (UniqueConstraint('sector_name', 'trade_date', name='uq_sector_date'),)

    def __repr__(self) -> str:
        return f"<SectorDaily(sector={self.sector_name}, date={self.trade_date}, close={self.close})>"




_LLM_USAGE_TELEMETRY_COLUMN_SQL: Dict[str, str] = {
    "provider_usage_json": "TEXT",
    "provider": "VARCHAR(64)",
    "provider_usage_schema_name": "VARCHAR(64)",
    "provider_usage_schema_version": "VARCHAR(32)",
    "provider_usage_observed_at": "VARCHAR(32)",
    "normalized_prompt_tokens": "INTEGER",
    "normalized_completion_tokens": "INTEGER",
    "normalized_total_tokens": "INTEGER",
    "normalized_cache_read_tokens": "INTEGER",
    "normalized_cache_write_tokens": "INTEGER",
    "normalized_cache_miss_tokens": "INTEGER",
    "normalized_uncached_input_tokens": "INTEGER",
    "normalized_cache_eligible_input_tokens": "INTEGER",
    "normalized_cache_hit_ratio": "FLOAT",
    "normalized_cache_write_ratio": "FLOAT",
    "cache_capability": "VARCHAR(32)",
    "cache_eligibility": "VARCHAR(32)",
    "cache_observation": "VARCHAR(32)",
    "estimated_prefix_tokens": "INTEGER",
    "provider_reported_prompt_tokens": "INTEGER",
    "provider_reported_cached_tokens": "INTEGER",
    "provider_min_cache_tokens": "INTEGER",
    "eligibility_confidence": "VARCHAR(32)",
    "tokenizer_name": "VARCHAR(128)",
    "tokenizer_version": "VARCHAR(64)",
    "messages_hmac": "VARCHAR(64)",
    "system_message_hmac": "VARCHAR(64)",
    "user_message_hmac": "VARCHAR(64)",
    "hmac_key_version": "VARCHAR(64)",
    "hmac_domain": "VARCHAR(32)",
    "hash_scope": "VARCHAR(32)",
    "language": "VARCHAR(16)",
    "market_group": "VARCHAR(16)",
    "analysis_mode": "VARCHAR(64)",
    "legacy_prompt_mode": "VARCHAR(32)",
    "skill_config_hmac": "VARCHAR(64)",
    "transport": "VARCHAR(64)",
    "message_count": "INTEGER",
    "estimated_total_prompt_tokens": "INTEGER",
    "approx_common_prefix_chars": "INTEGER",
    "approx_common_prefix_tokens": "INTEGER",
    "known_dynamic_marker_positions": "TEXT",
}
_LLM_USAGE_INTEGER_TELEMETRY_COLUMNS = {
    column
    for column, column_type in _LLM_USAGE_TELEMETRY_COLUMN_SQL.items()
    if column_type == "INTEGER"
}
_LLM_USAGE_DROPPED_FREE_TEXT_COLUMNS = {"tokenizer_name", "tokenizer_version"}
_LLM_PROMPT_CACHE_TELEMETRY_DISABLED_ATTR = "prompt_cache_telemetry_disabled"
_LLM_PROMPT_CACHE_TELEMETRY_COLUMNS = {
    "provider_usage_json",
    "provider_usage_schema_name",
    "provider_usage_schema_version",
    "provider_usage_observed_at",
    "normalized_cache_read_tokens",
    "normalized_cache_write_tokens",
    "normalized_cache_miss_tokens",
    "normalized_uncached_input_tokens",
    "normalized_cache_eligible_input_tokens",
    "normalized_cache_hit_ratio",
    "normalized_cache_write_ratio",
    "cache_capability",
    "cache_eligibility",
    "cache_observation",
    "estimated_prefix_tokens",
    "provider_reported_cached_tokens",
    "provider_min_cache_tokens",
    "eligibility_confidence",
}


class AlertRuleRecord(Base):
    """Persisted alert rule managed through the Alert API."""

    __tablename__ = 'alert_rules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    target_scope = Column(String(32), nullable=False, default='single_symbol', index=True)
    target = Column(String(64), nullable=False, index=True)
    alert_type = Column(String(32), nullable=False, index=True)
    parameters = Column(Text, nullable=False, default='{}')
    severity = Column(String(16), nullable=False, default='warning', index=True)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    source = Column(String(16), nullable=False, default='api', index=True)
    cooldown_policy = Column(Text)
    notification_policy = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_alert_rule_type_target', 'alert_type', 'target'),
    )


class AlertTriggerRecord(Base):
    """Alert trigger history row.

    P1 exposes read APIs and table shape; runtime writer integration lands in
    later phases.
    """

    __tablename__ = 'alert_triggers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, index=True)
    target = Column(String(64), nullable=False, index=True)
    observed_value = Column(Float)
    threshold = Column(Float)
    reason = Column(Text)
    data_source = Column(String(64))
    data_timestamp = Column(DateTime, index=True)
    triggered_at = Column(DateTime, default=datetime.now, index=True)
    status = Column(String(16), nullable=False, default='triggered', index=True)
    diagnostics = Column(Text)

    __table_args__ = (
        Index('ix_alert_trigger_rule_time', 'rule_id', 'triggered_at'),
    )


class AlertNotificationRecord(Base):
    """Notification attempt row for alert triggers.

    P1 exposes read APIs and table shape; runtime writer integration lands in
    later phases.
    """

    __tablename__ = 'alert_notifications'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trigger_id = Column(Integer, index=True)
    channel = Column(String(32), nullable=False, index=True)
    attempt = Column(Integer, nullable=False, default=1)
    success = Column(Boolean, nullable=False, default=False, index=True)
    error_code = Column(String(64))
    retryable = Column(Boolean, nullable=False, default=False)
    latency_ms = Column(Integer)
    diagnostics = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_alert_notification_trigger_channel', 'trigger_id', 'channel'),
    )


class AlertCooldownRecord(Base):
    """Persisted alert cooldown state for DB-managed alert rules."""

    __tablename__ = 'alert_cooldowns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, index=True)
    # Reserved for future non-DB/expanded-scope rules; P4 queries by rule_id.
    rule_key = Column(String(255), index=True)
    target = Column(String(64), nullable=False, index=True)
    severity = Column(String(16), nullable=False, default='warning', index=True)
    last_triggered_at = Column(DateTime, index=True)
    cooldown_until = Column(DateTime, index=True)
    reason = Column(Text)
    state = Column(String(16), nullable=False, default='active', index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('rule_id', 'target', 'severity', name='uix_alert_cooldown_rule_target_severity'),
    )


class DecisionSignalRecord(Base):
    """Persisted AI decision signal asset for Issue #1390 P1."""

    __tablename__ = 'decision_signals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    market = Column(String(8), nullable=False, index=True)
    source_type = Column(String(32), nullable=False, index=True)
    source_agent = Column(String(64))
    source_report_id = Column(Integer, index=True)
    trace_id = Column(String(64), index=True)
    decision_profile = Column(String(16), index=True)
    market_phase = Column(String(24), index=True)
    trigger_source = Column(String(64), nullable=False, index=True)
    action = Column(String(16), nullable=False, index=True)
    action_label = Column(String(32))
    confidence = Column(Float)
    score = Column(Integer)
    horizon = Column(String(16), index=True)
    entry_low = Column(Float)
    entry_high = Column(Float)
    stop_loss = Column(Float)
    target_price = Column(Float)
    invalidation = Column(Text)
    watch_conditions = Column(Text)
    reason = Column(Text)
    risk_summary = Column(Text)
    catalyst_summary = Column(Text)
    evidence_json = Column(Text)
    data_quality_summary_json = Column(Text)
    plan_quality = Column(String(16), nullable=False, default='unknown', index=True)
    status = Column(String(16), nullable=False, default='active', index=True)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)
    metadata_json = Column(Text)

    __table_args__ = (
        Index('ix_decision_signal_stock_status_time', 'stock_code', 'status', 'created_at'),
        Index('ix_decision_signal_market_status_time', 'market', 'status', 'created_at'),
        Index(
            'ix_decision_signal_report_type_market_stock_action_horizon_phase',
            'source_report_id',
            'source_type',
            'market',
            'stock_code',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_trace_type_market_stock_action_horizon_phase',
            'trace_id',
            'source_type',
            'market',
            'stock_code',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_report_type_market_stock_profile_action_horizon_phase',
            'source_report_id',
            'source_type',
            'market',
            'stock_code',
            'decision_profile',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase',
            'trace_id',
            'source_type',
            'market',
            'stock_code',
            'decision_profile',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_market_stock_profile_created',
            'market',
            'stock_code',
            'decision_profile',
            'created_at',
        ),
    )


class DecisionSignalOutcomeRecord(Base):
    """Signal-level forward outcome for Issue #1390 P5."""

    __tablename__ = 'decision_signal_outcomes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=False, index=True)
    horizon = Column(String(16), nullable=False, index=True)
    engine_version = Column(String(32), nullable=False, index=True)
    eval_status = Column(String(24), nullable=False, default='unable', index=True)
    outcome = Column(String(16), index=True)
    direction_expected = Column(String(16), index=True)
    direction_correct = Column(Boolean)
    unable_reason = Column(String(64), index=True)
    anchor_date = Column(Date, index=True)
    eval_window_days = Column(Integer)
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    action = Column(String(16), index=True)
    market = Column(String(8), index=True)
    market_phase = Column(String(24), index=True)
    source_type = Column(String(32), index=True)
    source_agent = Column(String(64), index=True)
    plan_quality = Column(String(16), index=True)
    data_quality_level = Column(String(24), index=True)
    holding_state = Column(String(16), nullable=False, default='unknown', index=True)

    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint('signal_id', 'horizon', 'engine_version', name='uix_decision_signal_outcome_key'),
        Index('ix_decision_signal_outcome_stats_action', 'engine_version', 'action', 'horizon'),
        Index('ix_decision_signal_outcome_stats_market', 'engine_version', 'market', 'horizon'),
    )


class DecisionSignalFeedbackRecord(Base):
    """Latest user feedback for a decision signal."""

    __tablename__ = 'decision_signal_feedback'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=False, unique=True, index=True)
    feedback_value = Column(String(16), nullable=False, index=True)
    reason_code = Column(String(64), index=True)
    note = Column(Text)
    source = Column(String(16), nullable=False, default='api', index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)


class SkillOpinionSampleRecord(Base):
    """Immutable, low-sensitivity skill opinion sample for Issue #1904 P2 PR1."""

    __tablename__ = 'skill_opinion_samples'

    id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        index=True,
    )
    stock_code = Column(String(16), nullable=False, index=True)
    skill_id = Column(String(128), nullable=False, index=True)
    skill_version = Column(String(64), index=True)
    signal = Column(String(16), nullable=False, index=True)
    confidence = Column(Float, nullable=False)
    horizon = Column(String(16), index=True)
    data_quality_level = Column(String(24), index=True)
    opinion_created_at = Column(DateTime, index=True)
    sample_schema_version = Column(String(32), nullable=False, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'analysis_history_id',
            'skill_id',
            'sample_schema_version',
            name='uix_skill_opinion_sample_key',
        ),
        Index(
            'ix_skill_opinion_sample_skill_horizon_created',
            'skill_id',
            'horizon',
            'created_at',
        ),
        Index(
            'ix_skill_opinion_sample_stock_created',
            'stock_code',
            'created_at',
        ),
    )


class SkillOpinionOutcomeRecord(Base):
    """Forward outcome for one immutable skill opinion sample and horizon."""

    __tablename__ = 'skill_opinion_outcomes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    skill_opinion_sample_id = Column(
        Integer,
        ForeignKey('skill_opinion_samples.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )
    horizon = Column(String(16), nullable=False, index=True)
    engine_version = Column(String(32), nullable=False, index=True)
    eval_status = Column(String(24), nullable=False, default='pending', index=True)
    outcome = Column(String(16), index=True)
    direction_correct = Column(Boolean)
    unable_reason = Column(String(64), index=True)
    analysis_date = Column(Date, index=True)
    start_trade_date = Column(Date, index=True)
    end_trade_date = Column(Date, index=True)
    start_price = Column(Float)
    end_close = Column(Float)
    stock_return_pct = Column(Float)
    directional_return_pct = Column(Float)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'skill_opinion_sample_id',
            'horizon',
            'engine_version',
            name='uix_skill_opinion_outcome_key',
        ),
        CheckConstraint(
            "horizon IN ('1d', '3d', '5d', '10d')",
            name='ck_skill_opinion_outcome_horizon',
        ),
        CheckConstraint(
            "eval_status IN ('pending', 'evaluated', 'observational', 'unable')",
            name='ck_skill_opinion_outcome_eval_status',
        ),
        CheckConstraint(
            "outcome IS NULL OR outcome IN ('hit', 'miss', 'observational')",
            name='ck_skill_opinion_outcome_value',
        ),
        CheckConstraint(
            "(eval_status IN ('pending', 'unable') "
            "AND outcome IS NULL "
            "AND direction_correct IS NULL "
            "AND directional_return_pct IS NULL) "
            "OR (eval_status = 'observational' "
            "AND outcome = 'observational' "
            "AND direction_correct IS NULL "
            "AND directional_return_pct IS NULL) "
            "OR (eval_status = 'evaluated' "
            "AND outcome IN ('hit', 'miss') "
            "AND direction_correct IS NOT NULL "
            "AND directional_return_pct IS NOT NULL)",
            name='ck_skill_opinion_outcome_state_fields',
        ),
        Index(
            'ix_skill_opinion_outcome_candidate',
            'engine_version',
            'eval_status',
            'updated_at',
        ),
        Index(
            'ix_skill_opinion_outcome_horizon_status',
            'engine_version',
            'horizon',
            'eval_status',
        ),
    )


class _DatabaseManagerMeta(type):
    """Serialize DatabaseManager construction across __new__ and __init__."""

    def __call__(cls, *args, **kwargs):
        with cls._init_lock:
            return super().__call__(*args, **kwargs)


class DatabaseManager(metaclass=_DatabaseManagerMeta):
    """
    数据库管理器 - 单例模式

    职责：
    1. 管理数据库连接池
    2. 提供 Session 上下文管理
    3. 封装数据存取操作
    """
    
    _instance: Optional['DatabaseManager'] = None
    _init_lock = threading.RLock()
    _initialized: bool = False
    
    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: Optional[str] = None):
        """
        初始化数据库管理器
        
        Args:
            db_url: 数据库连接 URL（可选，默认从配置读取）
        """
        if getattr(self, '_initialized', False):
            return

        created_engine = None

        try:
            config = get_config()
            if db_url is None:
                db_url = config.get_db_url()

            self._db_url = db_url
            self._sqlite_wal_enabled = config.sqlite_wal_enabled
            self._sqlite_busy_timeout_ms = config.sqlite_busy_timeout_ms
            self._sqlite_write_retry_max = config.sqlite_write_retry_max
            self._sqlite_write_retry_base_delay = config.sqlite_write_retry_base_delay

            engine_kwargs = {
                "echo": False,
                "pool_pre_ping": True,
            }
            if str(db_url).startswith("sqlite:") and self._sqlite_busy_timeout_ms > 0:
                engine_kwargs["connect_args"] = {
                    "timeout": self._sqlite_busy_timeout_ms / 1000,
                }

            # 创建数据库引擎
            created_engine = create_engine(
                db_url,
                **engine_kwargs,
            )
            self._engine = created_engine
            self._is_sqlite_engine = self._engine.url.get_backend_name() == 'sqlite'
            self._sqlite_file_db = self._is_sqlite_engine and self._is_file_sqlite_database()
            self._install_sqlite_pragma_handler()

            # 创建 Session 工厂
            self._SessionLocal = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False,
            )

            # 创建所有表
            Base.metadata.create_all(self._engine)
            self._ensure_llm_usage_telemetry_columns()
            self._ensure_decision_signal_profile_schema()
            self._ensure_intelligence_item_scope_values()
            self._ensure_schema_migration_record()
            self._ensure_intelligence_items_unique_index()

            self._initialized = True
            logger.info(f"数据库初始化完成: {db_url}")

            # 注册退出钩子，确保程序退出时关闭数据库连接
            atexit.register(DatabaseManager._cleanup_engine, self._engine)
        except Exception:
            self._initialized = False
            try:
                if created_engine is not None:
                    created_engine.dispose()
            except Exception as cleanup_exc:
                logger.warning("数据库初始化失败后的引擎清理也失败: %s", cleanup_exc)
            self._engine = None
            self._SessionLocal = None
            self.__class__._instance = None
            raise

    def _ensure_analysis_history_query_source_column(self) -> None:
        """SQLite: add analysis_history.query_source if missing (no Alembic in this project)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("PRAGMA table_info(analysis_history)")
                ).fetchall()
            col_names = {r[1] for r in rows}
            if "query_source" in col_names:
                return
        except Exception as exc:
            logger.warning("检查 analysis_history 表结构失败: %s", exc)
            return
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE analysis_history "
                        "ADD COLUMN query_source VARCHAR(32)"
                    )
                )
            logger.info("已添加列 analysis_history.query_source")
        except Exception as exc:
            logger.warning("添加 analysis_history.query_source 失败: %s", exc)

    def _ensure_backtest_results_trigger_source_column(self) -> None:
        """SQLite: add backtest_results.trigger_source if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("PRAGMA table_info(backtest_results)")
                ).fetchall()
            col_names = {r[1] for r in rows}
            if "trigger_source" in col_names:
                return
        except Exception as exc:
            logger.warning("检查 backtest_results 表结构失败: %s", exc)
            return
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE backtest_results "
                        "ADD COLUMN trigger_source VARCHAR(16)"
                    )
                )
            logger.info("已添加列 backtest_results.trigger_source")
        except Exception as exc:
            logger.warning("添加 backtest_results.trigger_source 失败: %s", exc)

    def _ensure_stock_tech_indicator_table(self) -> None:
        """SQLite: create stock_tech_indicator if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_tech_indicator'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 stock_tech_indicator 表存在性失败: %s", exc)
            return
        try:
            StockTechIndicator.__table__.create(self._engine)
            logger.info("已创建表 stock_tech_indicator")
        except Exception as exc:
            logger.warning("创建 stock_tech_indicator 失败: %s", exc)

    def _ensure_scan_result_intraday_table(self) -> None:
        """SQLite: create scan_result_intraday if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='scan_result_intraday'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 scan_result_intraday 表存在性失败: %s", exc)
            return
        try:
            ScanResultIntraday.__table__.create(self._engine)
            logger.info("已创建表 scan_result_intraday")
        except Exception as exc:
            logger.warning("创建 scan_result_intraday 失败: %s", exc)

    def _ensure_scan_result_postmarket_table(self) -> None:
        """SQLite: create scan_result_postmarket if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='scan_result_postmarket'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 scan_result_postmarket 表存在性失败: %s", exc)
            return
        try:
            ScanResultPostmarket.__table__.create(self._engine)
            logger.info("已创建表 scan_result_postmarket")
        except Exception as exc:
            logger.warning("创建 scan_result_postmarket 失败: %s", exc)

    def _ensure_limit_pool_table(self) -> None:
        """SQLite: create limit_pool if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='limit_pool'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 limit_pool 表存在性失败: %s", exc)
            return
        try:
            LimitPool.__table__.create(self._engine)
            logger.info("已创建表 limit_pool")
        except Exception as exc:
            logger.warning("创建 limit_pool 失败: %s", exc)

    def _ensure_margin_detail_table(self) -> None:
        """SQLite: create margin_detail if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='margin_detail'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 margin_detail 表存在性失败: %s", exc)
            return
        try:
            MarginDetail.__table__.create(self._engine)
            logger.info("已创建表 margin_detail")
        except Exception as exc:
            logger.warning("创建 margin_detail 失败: %s", exc)
        # 迁移：旧表有 rqmre 列，新列 rqmcl/rqchl
        try:
            with self._engine.connect() as conn:
                for col, typ in [("rqmcl", "FLOAT"), ("rqchl", "FLOAT")]:
                    conn.execute(text(
                        f"ALTER TABLE margin_detail ADD COLUMN {col} {typ}"
                    ))
                    conn.commit()
            logger.info("已添加 margin_detail.rqmcl/rqchl 列")
        except Exception:
            pass  # 列已存在

    def _ensure_daily_basic_table(self) -> None:
        """SQLite: create daily_basic if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_basic'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 daily_basic 表存在性失败: %s", exc)
            return
        try:
            DailyBasic.__table__.create(self._engine)
            logger.info("已创建表 daily_basic")
        except Exception as exc:
            logger.warning("创建 daily_basic 失败: %s", exc)

    def _ensure_popularity_rank_table(self) -> None:
        """SQLite: create popularity_rank if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='popularity_rank'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 popularity_rank 表存在性失败: %s", exc)
            return
        try:
            PopularityRank.__table__.create(self._engine)
            logger.info("已创建表 popularity_rank")
        except Exception as exc:
            logger.warning("创建 popularity_rank 失败: %s", exc)

    def _ensure_momentum_snapshot_table(self) -> None:
        """SQLite: create momentum_snapshot if missing (pre-create_all catch-up)."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='momentum_snapshot'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 momentum_snapshot 表存在性失败: %s", exc)
            return
        try:
            MomentumSnapshot.__table__.create(self._engine)
            logger.info("已创建表 momentum_snapshot")
        except Exception as exc:
            logger.warning("创建 momentum_snapshot 失败: %s", exc)

    def _ensure_cyq_perf_table(self) -> None:
        """SQLite: create broker_enrichment_cyq_perf if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='broker_enrichment_cyq_perf'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 broker_enrichment_cyq_perf 表存在性失败: %s", exc)
            return
        try:
            BrokerEnrichmentCyqPerf.__table__.create(self._engine)
            logger.info("已创建表 broker_enrichment_cyq_perf")
        except Exception as exc:
            logger.warning("创建 broker_enrichment_cyq_perf 失败: %s", exc)

    def _ensure_insider_buy_table(self) -> None:
        """SQLite: create insider_buy if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='insider_buy'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 insider_buy 表存在性失败: %s", exc)
            return
        try:
            InsiderBuy.__table__.create(self._engine)
            logger.info("已创建表 insider_buy")
        except Exception as exc:
            logger.warning("创建 insider_buy 失败: %s", exc)

    def _ensure_performance_report_table(self) -> None:
        """SQLite: create performance_report if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='performance_report'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 performance_report 表存在性失败: %s", exc)
            return
        try:
            PerformanceReport.__table__.create(self._engine)
            logger.info("已创建表 performance_report")
        except Exception as exc:
            logger.warning("创建 performance_report 失败: %s", exc)

    def _ensure_ths_industry_map_table(self) -> None:
        """SQLite: create ths_industry_map if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='ths_industry_map'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 ths_industry_map 表存在性失败: %s", exc)
            return
        try:
            ThsIndustryMap.__table__.create(self._engine)
            logger.info("已创建表 ths_industry_map")
        except Exception as exc:
            logger.warning("创建 ths_industry_map 失败: %s", exc)

    def _ensure_ths_concept_map_table(self) -> None:
        """SQLite: create ths_concept_map if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='ths_concept_map'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 ths_concept_map 表存在性失败: %s", exc)
            return
        try:
            ThsConceptMap.__table__.create(self._engine)
            logger.info("已创建表 ths_concept_map")
        except Exception as exc:
            logger.warning("创建 ths_concept_map 失败: %s", exc)

    def _ensure_limit_up_history_table(self) -> None:
        """SQLite: create limit_up_history if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='limit_up_history'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 limit_up_history 表存在性失败: %s", exc)
            return
        try:
            LimitUpHistory.__table__.create(self._engine)
            logger.info("已创建表 limit_up_history")
        except Exception as exc:
            logger.warning("创建 limit_up_history 失败: %s", exc)

    def _ensure_limit_break_table(self) -> None:
        """SQLite: create limit_break if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name='limit_break'")
                ).fetchall()
            if rows:
                return
        except Exception as exc:
            logger.warning("检查 limit_break 表存在性失败: %s", exc)
            return
        try:
            LimitBreak.__table__.create(self._engine)
            logger.info("已创建表 limit_break")
        except Exception as exc:
            logger.warning("创建 limit_break 失败: %s", exc)

    def _ensure_stock_daily_date_index(self) -> None:
        """SQLite: create ix_stock_daily_date if missing."""
        if not self._is_sqlite_engine:
            return
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text("CREATE INDEX IF NOT EXISTS ix_stock_daily_date ON stock_daily (date)")
                )
        except Exception as exc:
            err_msg = str(exc)
            if "already exists" in err_msg.lower():
                return
            logger.warning("创建 ix_stock_daily_date 索引失败: %s", exc)

    # ------------------------------------------------------------------
    # StockTechIndicator CRUD
    # ------------------------------------------------------------------

    def upsert_tech_indicators(
        self,
        df: pd.DataFrame,
        code: str,
    ) -> int:
        """批量 upsert Tushare 技术指标缓存。

        按 (code, date) 做 UPSERT，已存在记录覆盖更新。
        """
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for row in df.to_dict(orient='records'):
            row_date = self._normalize_daily_date(row.get('date') or row.get('trade_date'))
            if row_date is None:
                continue
            records.append({
                'code': code,
                'date': row_date,
                'close_qfq': self._normalize_sql_value(row.get('close_qfq')),
                'macd_dif': self._normalize_sql_value(row.get('macd_dif')),
                'macd_dea': self._normalize_sql_value(row.get('macd_dea')),
                'macd': self._normalize_sql_value(row.get('macd')),
                'rsi_6': self._normalize_sql_value(row.get('rsi_6')),
                'rsi_12': self._normalize_sql_value(row.get('rsi_12')),
                'rsi_24': self._normalize_sql_value(row.get('rsi_24')),
                'kdj_k': self._normalize_sql_value(row.get('kdj_k')),
                'kdj_d': self._normalize_sql_value(row.get('kdj_d')),
                'kdj_j': self._normalize_sql_value(row.get('kdj_j')),
                'boll_upper': self._normalize_sql_value(row.get('boll_upper')),
                'boll_mid': self._normalize_sql_value(row.get('boll_mid')),
                'boll_lower': self._normalize_sql_value(row.get('boll_lower')),
                'cci': self._normalize_sql_value(row.get('cci')),
                'created_at': now,
                'updated_at': now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _SQLITE_CHUNK = 50
                for i in range(0, len(records), _SQLITE_CHUNK):
                    chunk = records[i : i + _SQLITE_CHUNK]
                    stmt = sqlite_insert(StockTechIndicator).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=['code', 'date'],
                            set_={
                                'close_qfq': excluded.close_qfq,
                                'macd_dif': excluded.macd_dif,
                                'macd_dea': excluded.macd_dea,
                                'macd': excluded.macd,
                                'rsi_6': excluded.rsi_6,
                                'rsi_12': excluded.rsi_12,
                                'rsi_24': excluded.rsi_24,
                                'kdj_k': excluded.kdj_k,
                                'kdj_d': excluded.kdj_d,
                                'kdj_j': excluded.kdj_j,
                                'boll_upper': excluded.boll_upper,
                                'boll_mid': excluded.boll_mid,
                                'boll_lower': excluded.boll_lower,
                                'cci': excluded.cci,
                                'updated_at': excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                new_count = 0
                for record in records:
                    existing = session.execute(
                        select(StockTechIndicator).where(
                            and_(
                                StockTechIndicator.code == record['code'],
                                StockTechIndicator.date == record['date'],
                            )
                        )
                    ).scalar_one_or_none()
                    if existing is None:
                        session.add(StockTechIndicator(**record))
                        new_count += 1
                    else:
                        for key, val in record.items():
                            if key not in ('code', 'date', 'created_at'):
                                setattr(existing, key, val)
                return new_count

        try:
            return self._run_write_transaction(
                f"upsert_tech_indicators[{code}]",
                _write,
            )
        except Exception as e:
            logger.warning(f"保存技术指标缓存失败 {code}: {e}")
            return 0

    def get_tech_indicator(
        self, code: str, target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """获取单只股票指定日期的缓存技术指标。"""
        if target_date is None:
            target_date = date.today()

        with self.get_session() as session:
            row = session.execute(
                select(StockTechIndicator).where(
                    and_(
                        StockTechIndicator.code == code,
                        StockTechIndicator.date == target_date,
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return row.to_dict()

    def get_tech_indicators_batch(
        self, codes: List[str], target_date: Optional[date] = None
    ) -> Dict[str, Dict[str, Any]]:
        """批量获取多只股票指定日期的缓存技术指标。

        Returns:
            {code: indicator_dict} 字典，未缓存的 code 不出现在结果中
        """
        if not codes:
            return {}
        if target_date is None:
            target_date = date.today()

        with self.get_session() as session:
            rows = session.execute(
                select(StockTechIndicator).where(
                    and_(
                        StockTechIndicator.code.in_(codes),
                        StockTechIndicator.date == target_date,
                    )
                )
            ).scalars().all()
            return {r.code: r.to_dict() for r in rows}

    def get_tech_indicators_all(
        self, trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取全市场技术指标，返回 DataFrame (index=ts_code)。

        从 stock_tech_indicator 表读取，裸代码转 ts_code，
        close_qfq 映射为 close 列以匹配 get_bulk_stk_factor 返回格式。
        """
        if trade_date is None:
            trade_date = date.today().strftime("%Y%m%d")
        target_dt = datetime.strptime(trade_date, "%Y%m%d").date()
        with self.get_session() as session:
            rows = session.execute(
                select(StockTechIndicator).where(
                    StockTechIndicator.date == target_dt,
                )
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            data = [r.to_dict() for r in rows]
        df = pd.DataFrame(data)
        # 裸代码 → ts_code
        def _bare_to_ts(codes):
            codes = codes.astype(str).str.zfill(6)
            pre2 = codes.str[:2]
            sfx = pd.Series("SZ", index=codes.index)
            sfx[pre2.isin(["60", "68"])] = "SH"
            sfx[pre2.isin(["43", "83", "87", "92"])] = "BJ"
            return codes + "." + sfx
        df["ts_code"] = _bare_to_ts(df["code"])
        df = df.set_index("ts_code")
        # 列名映射: DB close_qfq → factor 期望的 close
        df = df.rename(columns={"close_qfq": "close"})
        keep = [
            "close", "macd_dif", "macd_dea", "macd",
            "rsi_6", "rsi_12", "rsi_24",
            "kdj_k", "kdj_d", "kdj_j",
            "boll_upper", "boll_mid", "boll_lower", "cci", "vol",
            "ma5", "ma10", "ma20",
        ]
        return df[[c for c in keep if c in df.columns]]
    def _ensure_schema_migration_record(self) -> None:
        session = self._SessionLocal()
        values = {
            "version": CURRENT_SCHEMA_VERSION,
            "description": "Baseline schema created through SQLAlchemy metadata.create_all",
        }
        try:
            if self._is_sqlite_engine:
                statement = sqlite_insert(DatabaseSchemaMigration).values(**values)
                statement = statement.on_conflict_do_nothing(index_elements=["version"])
                session.execute(statement)
            else:
                session.execute(DatabaseSchemaMigration.__table__.insert().values(**values))
            session.commit()
        except IntegrityError:
            session.rollback()
            with self._SessionLocal() as verify_session:
                existing = verify_session.get(DatabaseSchemaMigration, CURRENT_SCHEMA_VERSION)
            if existing is None:
                raise
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _ensure_decision_signal_profile_schema(self) -> None:
        """Add and backfill nullable decision_profile for existing SQLite DBs."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(DecisionSignalRecord.__tablename__):
            return

        try:
            existing = {
                column["name"]
                for column in inspector.get_columns(DecisionSignalRecord.__tablename__)
            }
        except Exception as exc:
            logger.error(
                "[DecisionSignal] failed to inspect decision_profile column; "
                "profile migration cannot continue safely: %s",
                exc,
            )
            raise

        if "decision_profile" not in existing:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {DecisionSignalRecord.__tablename__} "
                        "ADD COLUMN decision_profile VARCHAR(16)"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "decision_profile"):
                    raise

        self._ensure_decision_signal_profile_indexes()
        self._backfill_decision_signal_profile_from_metadata()

    def _ensure_decision_signal_profile_indexes(self) -> None:
        """Create profile-aware indexes without dropping legacy indexes."""

        expected_indexes = {
            "ix_decision_signals_decision_profile": ["decision_profile"],
            "ix_decision_signal_market_stock_profile_created": [
                "market", "stock_code", "decision_profile", "created_at",
            ],
            "ix_decision_signal_report_type_market_stock_profile_action_horizon_phase": [
                "source_report_id", "source_type", "market", "stock_code",
                "decision_profile", "action", "horizon", "market_phase",
            ],
            "ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase": [
                "trace_id", "source_type", "market", "stock_code",
                "decision_profile", "action", "horizon", "market_phase",
            ],
        }
        with self._engine.begin() as connection:
            for index_name, columns in expected_indexes.items():
                connection.exec_driver_sql(
                    f"CREATE INDEX IF NOT EXISTS {index_name} "
                    f"ON decision_signals ({', '.join(columns)})"
                )

        actual_indexes = {
            index["name"]: index["column_names"]
            for index in inspect(self._engine).get_indexes(
                DecisionSignalRecord.__tablename__
            )
        }
        for index_name, expected_columns in expected_indexes.items():
            if actual_indexes.get(index_name) != expected_columns:
                raise RuntimeError(
                    "decision_profile index verification failed: "
                    f"index={index_name} expected={expected_columns} "
                    f"actual={actual_indexes.get(index_name)}"
                )

    def _backfill_decision_signal_profile_from_metadata(self) -> None:
        stats = {
            "candidate_count": 0,
            "backfilled_count": 0,
            "guard_skipped_count": 0,
            "missing_metadata_count": 0,
            "missing_profile_count": 0,
            "invalid_json_count": 0,
            "non_object_count": 0,
            "invalid_profile_count": 0,
            "skipped_existing_profile_count": 0,
        }
        with self._engine.begin() as connection:
            stats["skipped_existing_profile_count"] = connection.execute(
                text(
                    "SELECT COUNT(*) FROM decision_signals "
                    "WHERE decision_profile IS NOT NULL"
                )
            ).scalar_one()
            candidate_rows = [
                (row["id"], row["metadata_json"])
                for row in connection.execute(
                    text(
                        "SELECT id, metadata_json FROM decision_signals "
                        "WHERE decision_profile IS NULL ORDER BY id"
                    )
                ).mappings()
            ]
            stats["candidate_count"] = len(candidate_rows)

            for signal_id, metadata_json in candidate_rows:
                if metadata_json is None:
                    stats["missing_metadata_count"] += 1
                    continue
                try:
                    metadata = json.loads(metadata_json)
                except (TypeError, ValueError, RecursionError):
                    stats["invalid_json_count"] += 1
                    continue
                if not isinstance(metadata, dict):
                    stats["non_object_count"] += 1
                    continue

                raw_profile = metadata.get("decision_profile")
                if raw_profile is None or (
                    isinstance(raw_profile, str) and not raw_profile.strip()
                ):
                    stats["missing_profile_count"] += 1
                    continue
                profile = extract_legacy_decision_profile(metadata)
                if profile is None:
                    stats["invalid_profile_count"] += 1
                    continue

                result = connection.execute(
                    text(
                        "UPDATE decision_signals "
                        "SET decision_profile = :decision_profile "
                        "WHERE id = :signal_id AND decision_profile IS NULL"
                    ),
                    {"decision_profile": profile, "signal_id": signal_id},
                )
                if result.rowcount == 1:
                    stats["backfilled_count"] += 1
                elif result.rowcount == 0:
                    stats["guard_skipped_count"] += 1
                else:
                    raise RuntimeError(
                        "decision_profile backfill updated an unexpected number "
                        f"of rows for signal_id={signal_id}: {result.rowcount}"
                    )

            classified_count = sum(
                stats[key]
                for key in (
                    "backfilled_count",
                    "guard_skipped_count",
                    "missing_metadata_count",
                    "missing_profile_count",
                    "invalid_json_count",
                    "non_object_count",
                    "invalid_profile_count",
                )
            )
            if classified_count != stats["candidate_count"]:
                raise RuntimeError(
                    "decision_profile migration stats did not classify every "
                    f"candidate: candidates={stats['candidate_count']} "
                    f"classified={classified_count}"
                )
        logger.info(
            "[DecisionSignal] decision_profile migration stats: "
            "candidate_count=%s backfilled_count=%s guard_skipped_count=%s "
            "missing_metadata_count=%s missing_profile_count=%s "
            "invalid_json_count=%s non_object_count=%s invalid_profile_count=%s "
            "skipped_existing_profile_count=%s",
            stats["candidate_count"],
            stats["backfilled_count"],
            stats["guard_skipped_count"],
            stats["missing_metadata_count"],
            stats["missing_profile_count"],
            stats["invalid_json_count"],
            stats["non_object_count"],
            stats["invalid_profile_count"],
            stats["skipped_existing_profile_count"],
        )

    def _ensure_intelligence_items_unique_index(self) -> None:
        if not self._is_sqlite_engine:
            return

        if not inspect(self._engine).has_table("intelligence_items"):
            return

        try:
            unique_indexes = self._list_sqlite_unique_indexes("intelligence_items")
        except Exception as exc:
            logger.warning(
                "[Intelligence items] failed to inspect unique indexes; "
                "skip migration/repair: %s",
                exc,
            )
            return

        target_columns = ("source_id", "url", "scope_type", "scope_value", "market")
        has_target_index = any(tuple(cols) == target_columns for cols in unique_indexes)
        has_legacy_url_unique = any(tuple(cols) == ("url",) for cols in unique_indexes)

        if has_target_index:
            return
        if unique_indexes and not has_legacy_url_unique:
            # Table has other unique index shapes; avoid aggressive changes and add
            # the expected scoped uniqueness directly.
            self._ensure_intelligence_items_scoped_unique_index_once()
            return

        self._rebuild_intelligence_items_table()

    def _rebuild_intelligence_items_table(self) -> None:
        temporary_table = f"intelligence_items_recreate_tmp_{int(time.time() * 1_000_000_000)}"
        columns = [column.name for column in IntelligenceItem.__table__.columns]
        select_clause = ", ".join(f'"{column}"' for column in columns)
        scoped_index_columns = ", ".join(["source_id", "url", "scope_type", "scope_value", "market"])
        scoped_index_name = "uix_intel_item_scope"

        tmp_metadata = MetaData()
        tmp_table = Table(
            temporary_table,
            tmp_metadata,
            *(column.copy() for column in IntelligenceItem.__table__.columns),
        )
        logger.info("Rebuilding intelligence_items table to align composite uniqueness constraints.")
        with self._engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temporary_table}"'))
            tmp_table.create(connection)
            connection.execute(
                text(
                    f"INSERT INTO \"{temporary_table}\" ({select_clause}) "
                    f"SELECT {select_clause} FROM intelligence_items"
                )
            )
            connection.execute(text('DROP TABLE "intelligence_items"'))
            connection.execute(
                text(f'ALTER TABLE "{temporary_table}" RENAME TO intelligence_items')
            )
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {scoped_index_name} ON "
                    f"intelligence_items ({scoped_index_columns})"
                )
            )

    def _ensure_intelligence_items_scoped_unique_index_once(self) -> None:
        target_index_name = "uix_intel_item_scope"
        with self._engine.begin() as connection:
            rows = connection.execute(
                text("PRAGMA index_list(intelligence_items)")
            ).fetchall()
            for row in rows:
                if row[1] == target_index_name:
                    return
            index_columns = ", ".join(["source_id", "url", "scope_type", "scope_value", "market"])
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {target_index_name} ON "
                    f"intelligence_items ({index_columns})"
                )
            )

    def _list_sqlite_unique_indexes(self, table_name: str):
        with self._engine.connect() as connection:
            rows = connection.execute(
                text(f"PRAGMA index_list({table_name})")
            ).fetchall()
            unique_indexes = []
            for row in rows:
                # row: (seq, name, unique, origin, partial)
                if int(row[2]) != 1:
                    continue
                index_name = row[1]
                index_columns = []
                for index_info in connection.execute(
                    text(f"PRAGMA index_xinfo({index_name})")
                ).fetchall():
                    # index_xinfo: (seqno, cid, name, desc, coll, key, ... )
                    column_name = index_info[2]
                    if column_name is None:
                        continue
                    index_columns.append(column_name)
                unique_indexes.append(index_columns)
            return unique_indexes

    def _ensure_llm_usage_telemetry_columns(self) -> None:
        """Add nullable P0a usage telemetry columns to existing SQLite DBs."""
        if not self._is_sqlite_engine:
            return
        try:
            existing = {
                column["name"]
                for column in inspect(self._engine).get_columns(LLMUsage.__tablename__)
            }
        except Exception as exc:
            logger.warning(
                "[LLM usage] failed to inspect telemetry columns; "
                "skipping best-effort SQLite telemetry column backfill: %s",
                exc,
            )
            return

        max_retries = self._sqlite_write_retry_max
        for column, column_type in _LLM_USAGE_TELEMETRY_COLUMN_SQL.items():
            if column in existing:
                continue
            for attempt in range(max_retries + 1):
                try:
                    with self._engine.begin() as connection:
                        connection.exec_driver_sql(
                            f"ALTER TABLE {LLMUsage.__tablename__} "
                            f"ADD COLUMN {column} {column_type}"
                        )
                    existing.add(column)
                    break
                except OperationalError as exc:
                    if self._is_sqlite_duplicate_column_error(exc, column):
                        existing.add(column)
                        break
                    if self._is_sqlite_locked_error(exc) and attempt < max_retries:
                        delay = self._sqlite_write_retry_base_delay * (2 ** attempt)
                        logger.warning(
                            "[LLM usage] SQLite telemetry column backfill locked, "
                            "retrying: %s (%s/%s, %.2fs)",
                            column,
                            attempt + 1,
                            max_retries,
                            delay,
                        )
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    raise

    def _ensure_intelligence_item_scope_values(self) -> None:
        """Backfill nullable intelligence item scopes so SQLite unique keys work."""
        if not self._is_sqlite_engine:
            return
        try:
            existing = {
                column["name"]
                for column in inspect(self._engine).get_columns(IntelligenceItem.__tablename__)
            }
        except Exception as exc:
            logger.warning("资讯池 scope_value 回填检查失败，已跳过: %s", exc)
            return
        if "scope_value" not in existing:
            return
        try:
            with self._engine.begin() as connection:
                connection.exec_driver_sql(
                    f"UPDATE {IntelligenceItem.__tablename__} "
                    "SET scope_value = ? "
                    "WHERE scope_value IS NULL OR scope_value = ''",
                    (INTELLIGENCE_ITEM_NULL_SCOPE_VALUE,),
                )
        except Exception as exc:
            logger.warning("资讯池 scope_value 回填失败，已跳过: %s", exc)

    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """获取单例实例"""
        with cls._init_lock:
            if cls._instance is None:
                cls()
            return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（用于测试）"""
        with cls._init_lock:
            if cls._instance is not None:
                if hasattr(cls._instance, '_engine') and cls._instance._engine is not None:
                    cls._instance._engine.dispose()
                cls._instance._initialized = False
                cls._instance = None

    @classmethod
    def _cleanup_engine(cls, engine) -> None:
        """
        清理数据库引擎（atexit 钩子）

        确保程序退出时关闭所有数据库连接，避免 ResourceWarning

        Args:
            engine: SQLAlchemy 引擎对象
        """
        try:
            if engine is not None:
                engine.dispose()
                logger.debug("数据库引擎已清理")
        except Exception as e:
            logger.warning(f"清理数据库引擎时出错: {e}")

    def _install_sqlite_pragma_handler(self) -> None:
        """为 SQLite 连接安装竞争保护参数。"""
        if not self._is_sqlite_engine:
            return

        @event.listens_for(self._engine, "connect")
        def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"PRAGMA busy_timeout={int(self._sqlite_busy_timeout_ms)}")
                if self._sqlite_file_db and self._sqlite_wal_enabled:
                    cursor.execute("PRAGMA journal_mode=WAL")
                    cursor.execute("PRAGMA wal_autocheckpoint=500")
                cursor.execute("PRAGMA cache_size=-50000")
            except Exception as exc:
                logger.warning("初始化 SQLite PRAGMA 失败: %s", exc)
            finally:
                cursor.close()

    def _is_file_sqlite_database(self) -> bool:
        database = (self._engine.url.database or "").strip()
        return bool(database) and database.lower() != ":memory:"

    def _run_write_transaction(
        self,
        operation_name: str,
        write_operation: Callable[[Session], T],
    ) -> T:
        max_retries = self._sqlite_write_retry_max if self._is_sqlite_engine else 0

        for attempt in range(max_retries + 1):
            session = self.get_session()
            try:
                if self._is_sqlite_engine:
                    # Acquire the SQLite writer lock before any reads inside
                    # `write_operation()` so pre-write existence checks and the
                    # later upsert share one consistent write window.
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                result = write_operation(session)
                session.commit()
                return result
            except OperationalError as exc:
                session.rollback()
                if self._is_sqlite_engine and attempt < max_retries:
                    delay = self._sqlite_write_retry_base_delay * (2 ** attempt)
                    logger.warning(
                        "SQLite 写入锁冲突，准备重试: %s (%s/%s, %.2fs)",
                        operation_name,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    @staticmethod
    def _is_sqlite_locked_error(exc: OperationalError) -> bool:
        err_text = str(getattr(exc, "orig", exc)).lower()
        return any(
            token in err_text
            for token in (
                "database is locked",
                "database schema is locked",
                "database table is locked",
                "disk i/o error",
            )
        )

    @staticmethod
    def _is_sqlite_duplicate_column_error(exc: OperationalError, column: str) -> bool:
        err_text = str(getattr(exc, "orig", exc)).lower()
        return "duplicate column name" in err_text and column.lower() in err_text

    @staticmethod
    def _normalize_daily_date(value: Any) -> Any:
        if isinstance(value, str):
            if len(value) == 8 and value.isdigit():
                return datetime.strptime(value, '%Y%m%d').date()
            return datetime.strptime(value, '%Y-%m-%d').date()
        if isinstance(value, (int, float)):
            return datetime.strptime(str(int(value)), '%Y%m%d').date()
        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, datetime):
            return value.date()
        return value

    @staticmethod
    def _normalize_sql_value(value: Any) -> Any:
        return None if pd.isna(value) else value
    
    def get_session(self) -> Session:
        """
        获取数据库 Session
        
        使用示例:
            with db.get_session() as session:
                # 执行查询
                session.commit()  # 如果需要
        """
        if not getattr(self, '_initialized', False) or not hasattr(self, '_SessionLocal'):
            raise RuntimeError(
                "DatabaseManager 未正确初始化。"
                "请确保通过 DatabaseManager.get_instance() 获取实例。"
            )
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            session.close()
            raise

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        检查是否已有指定日期的数据
        
        用于断点续传逻辑：如果已有数据则跳过网络请求
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            是否存在数据
        """
        if target_date is None:
            target_date = date.today()
        # 注意：这里的 target_date 语义是“自然日”，而不是“最新交易日”。
        # 在周末/节假日/非交易日运行时，即使数据库已有最新交易日数据，这里也会返回 False。
        # 该行为目前保留（按需求不改逻辑）。
        
        with self.get_session() as session:
            result = session.execute(
                select(StockDaily).where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date == target_date
                    )
                )
            ).scalar_one_or_none()
            
            return result is not None
    
    def get_latest_data(
        self, 
        code: str, 
        days: int = 2
    ) -> List[StockDaily]:
        """
        获取最近 N 天的数据
        
        用于计算"相比昨日"的变化
        
        Args:
            code: 股票代码
            days: 获取天数
            
        Returns:
            StockDaily 对象列表（按日期降序）
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()
            
            return list(results)

    def save_news_intel(
        self,
        code: str,
        name: str,
        dimension: str,
        query: str,
        response: 'SearchResponse',
        query_context: Optional[Dict[str, str]] = None
    ) -> int:
        """
        保存新闻情报到数据库

        去重策略：
        - 优先按 URL 去重（唯一约束）
        - URL 缺失时按 title + source + published_date 进行软去重

        关联策略：
        - query_context 记录用户查询信息（平台、用户、会话、原始指令等）
        """
        if not response or not response.results:
            return 0

        saved_count = 0
        query_ctx = query_context or {}
        current_query_id = (query_ctx.get("query_id") or "").strip()

        def _write(session: Session) -> int:
            local_saved_count = 0

            for item in response.results:
                title = (item.title or '').strip()
                url = (item.url or '').strip()
                source = (item.source or '').strip()
                snippet = (item.snippet or '').strip()
                published_date = self._parse_published_date(item.published_date)

                if not title and not url:
                    continue

                url_key = url or self._build_fallback_url_key(
                    code=code,
                    title=title,
                    source=source,
                    published_date=published_date
                )

                existing = session.execute(
                    select(NewsIntel).where(NewsIntel.url == url_key)
                ).scalar_one_or_none()

                if existing:
                    existing.name = name or existing.name
                    existing.dimension = dimension or existing.dimension
                    existing.query = query or existing.query
                    existing.provider = response.provider or existing.provider
                    existing.snippet = snippet or existing.snippet
                    existing.source = source or existing.source
                    existing.published_date = published_date or existing.published_date
                    existing.fetched_at = datetime.now()

                    if query_context:
                        if not existing.query_id and current_query_id:
                            existing.query_id = current_query_id
                        existing.query_source = (
                            query_context.get("query_source") or existing.query_source
                        )
                        existing.requester_platform = (
                            query_context.get("requester_platform") or existing.requester_platform
                        )
                        existing.requester_user_id = (
                            query_context.get("requester_user_id") or existing.requester_user_id
                        )
                        existing.requester_user_name = (
                            query_context.get("requester_user_name") or existing.requester_user_name
                        )
                        existing.requester_chat_id = (
                            query_context.get("requester_chat_id") or existing.requester_chat_id
                        )
                        existing.requester_message_id = (
                            query_context.get("requester_message_id") or existing.requester_message_id
                        )
                        existing.requester_query = (
                            query_context.get("requester_query") or existing.requester_query
                        )
                    continue

                try:
                    with session.begin_nested():
                        record = NewsIntel(
                            code=code,
                            name=name,
                            dimension=dimension,
                            query=query,
                            provider=response.provider,
                            title=title,
                            snippet=snippet,
                            url=url_key,
                            source=source,
                            published_date=published_date,
                            fetched_at=datetime.now(),
                            query_id=current_query_id or None,
                            query_source=query_ctx.get("query_source"),
                            requester_platform=query_ctx.get("requester_platform"),
                            requester_user_id=query_ctx.get("requester_user_id"),
                            requester_user_name=query_ctx.get("requester_user_name"),
                            requester_chat_id=query_ctx.get("requester_chat_id"),
                            requester_message_id=query_ctx.get("requester_message_id"),
                            requester_query=query_ctx.get("requester_query"),
                        )
                        session.add(record)
                        session.flush()
                    local_saved_count += 1
                except IntegrityError:
                    logger.debug("新闻情报重复（已跳过）: %s %s", code, url_key)

            return local_saved_count

        try:
            saved_count = self._run_write_transaction(
                f"save_news_intel[{code}]",
                _write,
            )
            logger.info(f"保存新闻情报成功: {code}, 新增 {saved_count} 条")
        except Exception as e:
            logger.error(f"保存新闻情报失败: {e}")
            raise

        return saved_count

    def save_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
        payload: Optional[Dict[str, Any]],
        source_chain: Optional[Any] = None,
        coverage: Optional[Any] = None,
    ) -> int:
        """
        保存基本面快照（P0 write-only）。失败不抛异常，返回写入条数 0/1。
        """
        if not query_id or not code or payload is None:
            return 0

        try:
            def _write(session: Session) -> int:
                session.add(
                    FundamentalSnapshot(
                        query_id=query_id,
                        code=code,
                        payload=self._safe_json_dumps(payload),
                        source_chain=self._safe_json_dumps(source_chain or []),
                        coverage=self._safe_json_dumps(coverage or {}),
                    )
                )
                return 1
            return self._run_write_transaction(
                f"save_fundamental_snapshot[{query_id}:{code}]",
                _write,
            )
        except Exception as e:
            logger.debug(
                "基本面快照写入失败（fail-open）: query_id=%s code=%s err=%s",
                query_id,
                code,
                e,
            )
            return 0

    def get_latest_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        获取指定 query_id + code 的最新基本面快照 payload。

        读取失败或不存在时返回 None（fail-open）。
        """
        if not query_id or not code:
            return None

        with self.get_session() as session:
            try:
                row = session.execute(
                    select(FundamentalSnapshot)
                    .where(
                        and_(
                            FundamentalSnapshot.query_id == query_id,
                            FundamentalSnapshot.code == code,
                        )
                    )
                    .order_by(desc(FundamentalSnapshot.created_at))
                    .limit(1)
                ).scalar_one_or_none()
            except Exception as e:
                logger.debug(
                    "基本面快照读取失败（fail-open）: query_id=%s code=%s err=%s",
                    query_id,
                    code,
                    e,
                )
                return None

            if row is None:
                return None
            try:
                payload = json.loads(row.payload or "{}")
                return payload if isinstance(payload, dict) else None
            except Exception:
                return None

    def save_screening_run(self, payload: Dict[str, Any]) -> int:
        """Persist one completed screening response without blocking screening on DB errors."""
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            return 0
        normalized_payload = dict(payload)
        warnings = self._screening_warning_values(normalized_payload)
        normalized_payload["warnings"] = warnings

        values = {
            "strategy": str(normalized_payload.get("strategy") or "").strip() or "unknown",
            "market": str(normalized_payload.get("market") or "").strip() or "cn",
            "snapshot_source": str(normalized_payload.get("snapshot_source") or "").strip() or None,
            "snapshot_count": self._optional_int(normalized_payload.get("snapshot_count")),
            "after_filter_count": self._optional_int(normalized_payload.get("after_filter_count")),
            "candidate_count": self._optional_int(normalized_payload.get("candidate_count")) or 0,
            "llm_ranked": self._optional_bool(normalized_payload.get("llm_ranked")),
            "daily_enriched": self._optional_bool(normalized_payload.get("daily_enriched")),
            "source_errors_json": self._safe_json_dumps(normalized_payload.get("source_errors") or []),
            "warnings_json": self._safe_json_dumps(warnings),
            "result_json": self._safe_json_dumps(normalized_payload),
        }

        try:
            def _write(session: Session) -> int:
                row = session.execute(
                    select(ScreeningRun).where(ScreeningRun.run_id == run_id)
                ).scalar_one_or_none()
                if row is None:
                    session.add(ScreeningRun(run_id=run_id, **values))
                else:
                    for key, value in values.items():
                        setattr(row, key, value)
                return 1

            return self._run_write_transaction(
                f"save_screening_run[{run_id}]",
                _write,
            )
        except Exception as exc:
            logger.warning(
                "选股运行历史写入失败（fail-open）: run_id=%s err=%s",
                run_id,
                exc,
            )
            return 0

    def list_screening_runs(
        self,
        *,
        limit: int = 20,
        strategy: Optional[str] = None,
        market: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List recent screening runs as compact summaries."""
        normalized_limit = max(0, min(int(limit), 100))
        if normalized_limit <= 0:
            return []

        with self.get_session() as session:
            statement = select(ScreeningRun)
            if strategy:
                statement = statement.where(ScreeningRun.strategy == str(strategy).strip())
            if market:
                statement = statement.where(ScreeningRun.market == str(market).strip())
            rows = session.execute(
                statement.order_by(desc(ScreeningRun.created_at), desc(ScreeningRun.id)).limit(normalized_limit)
            ).scalars().all()
            return [self._screening_run_to_dict(row, include_result=False) for row in rows]

    def get_screening_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Load a completed screening run by its stable run id."""
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return None
        with self.get_session() as session:
            row = session.execute(
                select(ScreeningRun).where(ScreeningRun.run_id == normalized_run_id)
            ).scalar_one_or_none()
            if row is None:
                return None
            return self._screening_run_to_dict(row, include_result=True)

    @staticmethod
    def _optional_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_bool(value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
            return None
        return bool(value)

    @staticmethod
    def _screening_json_list(value: Optional[str]) -> List[Any]:
        try:
            decoded = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        return decoded if isinstance(decoded, list) else []

    @staticmethod
    def _screening_text_list(value: Any) -> List[str]:
        if isinstance(value, list):
            result = []
            for item in value:
                text = str(item or "").strip()
                if text:
                    result.append(text)
            return result
        text = str(value or "").strip()
        return [text] if text else []

    @classmethod
    def _screening_warning_values(cls, payload: Dict[str, Any]) -> List[str]:
        warnings: List[str] = []
        seen: set[str] = set()
        for key in ("warnings", "degradation"):
            for item in cls._screening_text_list(payload.get(key)):
                if item in seen:
                    continue
                seen.add(item)
                warnings.append(item)
        return warnings

    @classmethod
    def _screening_run_to_dict(
        cls,
        row: ScreeningRun,
        *,
        include_result: bool,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "run_id": row.run_id,
            "strategy": row.strategy,
            "market": row.market,
            "snapshot_source": row.snapshot_source or "",
            "snapshot_count": row.snapshot_count,
            "after_filter_count": row.after_filter_count,
            "candidate_count": row.candidate_count,
            "llm_ranked": row.llm_ranked,
            "daily_enriched": row.daily_enriched,
            "source_errors": cls._screening_json_list(row.source_errors_json),
            "warnings": cls._screening_json_list(row.warnings_json),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        if include_result:
            try:
                result = json.loads(row.result_json or "{}")
            except (TypeError, ValueError):
                result = {}
            payload["result"] = result if isinstance(result, dict) else {}
        return payload

    def get_recent_news(self, code: str, days: int = 7, limit: int = 20) -> List[NewsIntel]:
        """
        获取指定股票最近 N 天的新闻情报
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= cutoff_date
                    )
                )
                .order_by(desc(NewsIntel.fetched_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_news_intel_by_query_id(self, query_id: str, limit: int = 20) -> List[NewsIntel]:
        """
        根据 query_id 获取新闻情报列表

        Args:
            query_id: 分析记录唯一标识
            limit: 返回数量限制

        Returns:
            NewsIntel 列表（按发布时间或抓取时间倒序）
        """
        from sqlalchemy import func

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(NewsIntel.query_id == query_id)
                .order_by(
                    desc(func.coalesce(NewsIntel.published_date, NewsIntel.fetched_at)),
                    desc(NewsIntel.fetched_at)
                )
                .limit(limit)
            ).scalars().all()

            return list(results)

    def delete_interactive_analysis_history_for_code_same_shanghai_day(
        self,
        code: str,
        day_lo: Optional[datetime] = None,
        day_hi: Optional[datetime] = None,
    ) -> int:
        """
        删除「交互式单股」来源下、同一上海自然日、同一 code 的 analysis_history 行（api/web）。

        不删除 query_source 为 cli/bot/system 或 NULL 的历史行，避免与批量/定时结果混淆。
        """
        if not code:
            return 0
        lo, hi = (
            (day_lo, day_hi) if day_lo is not None and day_hi is not None
            else shanghai_calendar_day_bounds_now()
        )

        def _delete(session: Session) -> int:
            stmt = delete(AnalysisHistory).where(
                and_(
                    AnalysisHistory.code == code,
                    AnalysisHistory.created_at >= lo,
                    AnalysisHistory.created_at < hi,
                    or_(
                        AnalysisHistory.query_source == "api",
                        AnalysisHistory.query_source == "web",
                    ),
                )
            )
            res = session.execute(stmt)
            return res.rowcount or 0

        try:
            return self._run_write_transaction(
                f"delete_interactive_history[{code}]",
                _delete,
            )
        except Exception as e:
            logger.error("删除交互式分析历史失败 code=%s: %s", code, e)
            return 0

    def save_analysis_history(
        self,
        result: Any,
        query_id: str,
        report_type: str,
        news_content: Optional[str],
        context_snapshot: Optional[Dict[str, Any]] = None,
        save_snapshot: bool = True,
        query_source: Optional[str] = None,
    ) -> int:
        """
        保存分析结果历史记录。

        Returns:
            新保存的 AnalysisHistory.id；保存失败返回 0。
        """
        if result is None:
            return 0

        sniper_points = self._extract_sniper_points(result)
        raw_result = self._build_raw_result(result)
        context_text = None
        if save_snapshot and context_snapshot is not None:
            context_text = self._safe_json_dumps(context_snapshot)

        try:
            def _write(session: Session) -> int:
                history = AnalysisHistory(
                        query_id=query_id,
                        code=result.code,
                        name=result.name,
                        report_type=report_type,
                        sentiment_score=result.sentiment_score,
                        operation_advice=result.operation_advice,
                        trend_prediction=result.trend_prediction,
                        analysis_summary=result.analysis_summary,
                        raw_result=self._safe_json_dumps(raw_result),
                        news_content=news_content,
                        context_snapshot=context_text,
                        ideal_buy=sniper_points.get("ideal_buy"),
                        secondary_buy=sniper_points.get("secondary_buy"),
                        stop_loss=sniper_points.get("stop_loss"),
                        take_profit=sniper_points.get("take_profit"),
                        created_at=datetime.now(),
                        query_source=query_source,
                )
                session.add(history)
                session.flush()
                return int(history.id or 0)
            return self._run_write_transaction(
                f"save_analysis_history[{result.code}]",
                _write,
            )
        except Exception as e:
            logger.error(f"保存分析历史失败: {e}")
            return 0

    def delete_analysis_history_by_query_and_code(self, query_id: str, code: str) -> int:
        """Remove existing row(s) so a recheck can replace without duplicate rows."""
        if not query_id or not code:
            return 0

        def _delete(session: Session) -> int:
            stmt = delete(AnalysisHistory).where(
                and_(
                    AnalysisHistory.query_id == query_id,
                    AnalysisHistory.code == code,
                )
            )
            res = session.execute(stmt)
            return res.rowcount or 0

        try:
            return self._run_write_transaction(
                f"delete_analysis_history[{code}]",
                _delete,
            )
        except Exception as e:
            logger.error(f"删除分析历史失败 query_id={query_id} code={code}: {e}")
            return 0

    def update_analysis_history_diagnostics(
        self,
        *,
        query_id: str,
        code: Optional[str] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
        notification_runs: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        """
        更新已保存分析历史的运行诊断快照。

        通知结果通常在分析历史落库后才产生，因此这里仅补写
        context_snapshot.diagnostics，不改变报告正文或其它历史字段。
        """
        if not query_id or (diagnostics is None and not notification_runs):
            return 0

        try:
            def _write(session: Session) -> int:
                conditions = [AnalysisHistory.query_id == query_id]
                if code:
                    conditions.append(AnalysisHistory.code == code)

                row = session.execute(
                    select(AnalysisHistory)
                    .where(and_(*conditions))
                    .order_by(desc(AnalysisHistory.created_at))
                    .limit(1)
                ).scalars().first()
                if row is None:
                    return 0

                context_snapshot: Dict[str, Any] = {}
                if row.context_snapshot:
                    try:
                        parsed = json.loads(row.context_snapshot)
                        if isinstance(parsed, dict):
                            context_snapshot = parsed
                    except Exception:
                        context_snapshot = {}

                if diagnostics is not None:
                    context_snapshot["diagnostics"] = diagnostics
                else:
                    existing_diagnostics = context_snapshot.get("diagnostics")
                    if not isinstance(existing_diagnostics, dict):
                        existing_diagnostics = {
                            "query_id": query_id,
                            "stock_code": code,
                            "notification_runs": [],
                        }
                    runs = existing_diagnostics.get("notification_runs")
                    if not isinstance(runs, list):
                        runs = []
                    trace_id = existing_diagnostics.get("trace_id")
                    for run in notification_runs or []:
                        if isinstance(run, dict):
                            run_payload = dict(run)
                            if trace_id and not run_payload.get("trace_id"):
                                run_payload["trace_id"] = trace_id
                            runs.append(run_payload)
                    existing_diagnostics["notification_runs"] = runs
                    context_snapshot["diagnostics"] = existing_diagnostics
                row.context_snapshot = self._safe_json_dumps(context_snapshot)
                return 1

            return self._run_write_transaction(
                f"update_analysis_history_diagnostics[{query_id}:{code or '*'}]",
                _write,
            )
        except Exception as e:
            logger.warning(
                "更新分析历史诊断快照失败（fail-open）: query_id=%s code=%s err=%s",
                query_id,
                code,
                e,
            )
            return 0

    def get_analysis_history(
        self,
        code: Optional[str] = None,
        query_id: Optional[str] = None,
        days: int = 30,
        limit: int = 50,
        exclude_query_id: Optional[str] = None,
    ) -> List[AnalysisHistory]:
        """
        Query analysis history records.

        Notes:
        - If query_id is provided, perform exact lookup and ignore days window.
        - If query_id is not provided, apply days-based time filtering.
        - exclude_query_id: exclude records with this query_id (for history comparison).
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            conditions = []

            if query_id:
                conditions.append(AnalysisHistory.query_id == query_id)
            else:
                conditions.append(AnalysisHistory.created_at >= cutoff_date)

            if code:
                conditions.append(AnalysisHistory.code == code)

            # exclude_query_id only applies when not doing exact lookup (query_id is None)
            if exclude_query_id and not query_id:
                conditions.append(AnalysisHistory.query_id != exclude_query_id)

            results = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_latest_analysis_history_id(
        self,
        *,
        query_id: str,
        code: str,
        report_type: str,
    ) -> Optional[int]:
        """Return the latest matching history id for read-only lookups.

        P2 automatic DecisionSignal extraction receives the freshly saved id
        directly from ``save_analysis_history()`` and does not use this helper.
        """

        if not query_id or not code or not report_type:
            return None

        with self.get_session() as session:
            return session.execute(
                select(AnalysisHistory.id)
                .where(
                    AnalysisHistory.query_id == query_id,
                    AnalysisHistory.code == code,
                    AnalysisHistory.report_type == report_type,
                )
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(1)
            ).scalar_one_or_none()
    
    def get_analysis_history_paginated(
        self,
        code: Optional[Union[str, List[str]]] = None,
        report_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[AnalysisHistory], int]:
        """
        分页查询分析历史记录（带总数）
        
        Args:
            code: 股票代码筛选
            report_type: 报告类型筛选
            start_date: 开始日期（含）
            end_date: 结束日期（含）
            offset: 偏移量（跳过前 N 条）
            limit: 每页数量
            
        Returns:
            Tuple[List[AnalysisHistory], int]: (记录列表, 总数)
        """
        from sqlalchemy import func
        
        with self.get_session() as session:
            conditions = []
            
            if code:
                if isinstance(code, list):
                    codes = [c for c in code if c]
                    if codes:
                        conditions.append(AnalysisHistory.code.in_(codes))
                else:
                    conditions.append(AnalysisHistory.code == code)
            if report_type:
                conditions.append(AnalysisHistory.report_type == report_type)
            if start_date:
                # created_at >= start_date 00:00:00
                conditions.append(AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time()))
            if end_date:
                # created_at < end_date+1 00:00:00 (即 <= end_date 23:59:59)
                conditions.append(AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
            
            # 构建 where 子句
            where_clause = and_(*conditions) if conditions else True
            
            # 查询总数
            total_query = select(func.count(AnalysisHistory.id)).where(where_clause)
            total = session.execute(total_query).scalar() or 0
            
            # 查询分页数据
            data_query = (
                select(AnalysisHistory)
                .where(where_clause)
                .order_by(desc(AnalysisHistory.created_at))
                .offset(offset)
                .limit(limit)
            )
            results = session.execute(data_query).scalars().all()
            
            return list(results), total
    
    def get_analysis_history_by_id(self, record_id: int) -> Optional[AnalysisHistory]:
        """
        根据数据库主键 ID 查询单条分析历史记录
        
        由于 query_id 可能重复（批量分析时多条记录共享同一 query_id），
        使用主键 ID 确保精确查询唯一记录。
        
        Args:
            record_id: 分析历史记录的主键 ID
            
        Returns:
            AnalysisHistory 对象，不存在返回 None
        """
        with self.get_session() as session:
            result = session.execute(
                select(AnalysisHistory).where(AnalysisHistory.id == record_id)
            ).scalars().first()
            return result

    def delete_analysis_history_records(self, record_ids: List[int]) -> int:
        """
        删除指定的分析历史记录。

        同时清理依赖这些历史记录的回测结果和分析来源决策信号，避免
        依赖历史记录的派生数据残留。DecisionSignal 的 source_report_id
        允许弱引用，因此这里只清理 source_type=analysis 的真实历史绑定信号。

        Args:
            record_ids: 要删除的历史记录主键 ID 列表

        Returns:
            实际删除的历史记录数量
        """
        ids = sorted({int(record_id) for record_id in record_ids if record_id is not None})
        if not ids:
            return 0

        def _write(session: Session) -> int:
            existing_ids = sorted(
                session.execute(
                    select(AnalysisHistory.id).where(AnalysisHistory.id.in_(ids))
                ).scalars().all()
            )
            if not existing_ids:
                return 0

            linked_signal_ids = sorted(
                session.execute(
                    select(DecisionSignalRecord.id).where(
                        and_(
                            DecisionSignalRecord.source_type == "analysis",
                            DecisionSignalRecord.source_report_id.in_(existing_ids),
                        )
                    )
                ).scalars().all()
            )
            if linked_signal_ids:
                session.execute(
                    delete(DecisionSignalOutcomeRecord).where(
                        DecisionSignalOutcomeRecord.signal_id.in_(linked_signal_ids)
                    )
                )
                session.execute(
                    delete(DecisionSignalFeedbackRecord).where(
                        DecisionSignalFeedbackRecord.signal_id.in_(linked_signal_ids)
                    )
                )
                session.execute(
                    delete(DecisionSignalRecord).where(DecisionSignalRecord.id.in_(linked_signal_ids))
                )
            session.execute(
                delete(BacktestResult).where(BacktestResult.analysis_history_id.in_(existing_ids))
            )
            linked_skill_sample_ids = sorted(
                session.execute(
                    select(SkillOpinionSampleRecord.id).where(
                        SkillOpinionSampleRecord.analysis_history_id.in_(existing_ids)
                    )
                ).scalars().all()
            )
            if linked_skill_sample_ids:
                session.execute(
                    delete(SkillOpinionOutcomeRecord).where(
                        SkillOpinionOutcomeRecord.skill_opinion_sample_id.in_(
                            linked_skill_sample_ids
                        )
                    )
                )
            session.execute(
                delete(SkillOpinionSampleRecord).where(
                    SkillOpinionSampleRecord.analysis_history_id.in_(existing_ids)
                )
            )
            result = session.execute(
                delete(AnalysisHistory).where(AnalysisHistory.id.in_(existing_ids))
            )
            return result.rowcount or 0

        return self._run_write_transaction(
            "delete analysis history records",
            _write,
        )

    def get_distinct_stocks_from_history(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 200,
        include_market_review: bool = False,
    ) -> List[AnalysisHistory]:
        """
        获取历史记录中的不重复股票列表，每只股票取最新一条记录。

        使用子查询按 code 分组取 MAX(id)，再 JOIN 回查完整记录。
        默认排除大盘复盘，避免混入普通个股栏。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            limit: 最大返回数量
            include_market_review: 是否包含大盘复盘记录

        Returns:
            每条股票最新一条 AnalysisHistory 记录列表
        """
        with self.get_session() as session:
            subq = (
                select(
                    AnalysisHistory.code,
                    func.max(AnalysisHistory.id).label("max_id"),
                )
            )
            if start_date:
                subq = subq.where(
                    AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time())
                )
            if end_date:
                subq = subq.where(
                    AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time())
                )
            if not include_market_review:
                subq = subq.where(
                    and_(
                        AnalysisHistory.code != "MARKET",
                        or_(
                            AnalysisHistory.report_type.is_(None),
                            AnalysisHistory.report_type != "market_review",
                        ),
                    )
                )
            subq = subq.group_by(AnalysisHistory.code).subquery()

            results = (
                session.execute(
                    select(AnalysisHistory)
                    .join(subq, AnalysisHistory.id == subq.c.max_id)
                    .order_by(
                        desc(AnalysisHistory.created_at),
                    )
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            return list(results)

    def get_latest_analysis_by_query_id(
        self,
        query_id: str,
        *,
        code: Optional[str] = None,
        report_type: Optional[str] = None,
    ) -> Optional[AnalysisHistory]:
        """
        根据 query_id 查询最新一条分析历史记录

        query_id 在批量分析时可能重复，故返回最近创建的一条。

        Args:
            query_id: 分析记录关联的 query_id
            code: 可选股票代码过滤，用于区分同一 query_id 下的 MARKET 与个股记录
            report_type: 可选报告类型过滤

        Returns:
            AnalysisHistory 对象，不存在返回 None
        """
        with self.get_session() as session:
            conditions = [AnalysisHistory.query_id == query_id]
            if code:
                conditions.append(AnalysisHistory.code == code)
            if report_type:
                conditions.append(AnalysisHistory.report_type == report_type)

            result = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(1)
            ).scalars().first()
            return result
    
    def get_data_range(
        self, 
        code: str, 
        start_date: date, 
        end_date: date
    ) -> List[StockDaily]:
        """
        获取指定日期范围的数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            StockDaily 对象列表
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(StockDaily.date)
            ).scalars().all()
            
            return list(results)

    def get_data_range_batch(
        self,
        codes: List[str],
        start_date: date,
        end_date: date,
        max_retries: int = 3,
    ) -> Dict[str, List[StockDaily]]:
        """批量获取多只股票的 OHLCV 数据。

        SQLite 并发场景下偶发 disk I/O error，内置重试逻辑。

        Returns:
            {code: [StockDaily]} 字典，按日期升序排列
        """
        if not codes:
            return {}

        last_err = None
        for attempt in range(max_retries):
            try:
                with self.get_session() as session:
                    rows = session.execute(
                        select(StockDaily)
                        .where(
                            and_(
                                StockDaily.code.in_(codes),
                                StockDaily.date >= start_date,
                                StockDaily.date <= end_date,
                            )
                        )
                        .order_by(StockDaily.code, StockDaily.date)
                    ).scalars().all()

                result: Dict[str, List[StockDaily]] = {}
                for r in rows:
                    result.setdefault(r.code, []).append(r)
                return result
            except Exception as e:
                last_err = e
                if attempt < max_retries - 1:
                    import time as _time
                    _time.sleep(0.3 * (attempt + 1))
        raise last_err

    # ------------------------------------------------------------------
    # Realtime spot (intraday snapshot)
    # ------------------------------------------------------------------

    def upsert_realtime_spot(
        self,
        df: pd.DataFrame,
        source: str,
        slot: int,
    ) -> int:
        """批量 upsert 实时行情快照。

        DataFrame 列: code, name, price, pct_chg, pre_close, volume, amount
        按 code 唯一键 upsert，同一 code 新数据覆盖旧数据。
        """
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for idx, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                code = str(idx).strip()  # code may be the DataFrame index
            if not code:
                continue
            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50],
                "price": self._normalize_sql_value(row.get("price")),
                "pct_chg": self._normalize_sql_value(row.get("pct_chg")),
                "pre_close": self._normalize_sql_value(row.get("pre_close")),
                "high": self._normalize_sql_value(row.get("high")),
                "open_price": self._normalize_sql_value(row.get("open_price")),
                "low": self._normalize_sql_value(row.get("low")),
                "volume": self._normalize_sql_value(row.get("volume")),
                "amount": self._normalize_sql_value(row.get("amount")),
                "turnover_rate": self._normalize_sql_value(row.get("turnover_rate")),
                "volume_ratio": self._normalize_sql_value(row.get("volume_ratio")),
                "trade_date": str(row.get("trade_date", date.today().isoformat()))[:10],
                "source": source,
                "slot": slot,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            # 可空字段：新值为 None/NaN 时保留 DB 已有值，防止不同槽数据源互相覆盖
            _NULLABLE_KEYS = {"turnover_rate", "volume_ratio", "pct_chg"}

            def _is_null(v: Any) -> bool:
                if v is None:
                    return True
                try:
                    return bool(pd.isna(v)) if hasattr(v, "__float__") else False
                except (TypeError, ValueError):
                    return False

            if self._is_sqlite_engine:
                _CHUNK = 100
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    codes_in_chunk = [r["code"] for r in chunk]
                    existing_rows = session.execute(
                        select(RealtimeSpot.code, RealtimeSpot.turnover_rate,
                               RealtimeSpot.volume_ratio, RealtimeSpot.pct_chg)
                        .where(RealtimeSpot.code.in_(codes_in_chunk))
                    ).fetchall()
                    existing_map = {row.code: row for row in existing_rows}

                    clean_chunk = []
                    for rec in chunk:
                        ex = existing_map.get(rec["code"])
                        clean = dict(rec)
                        if ex is not None:
                            for key in _NULLABLE_KEYS:
                                if _is_null(clean.get(key)):
                                    clean[key] = ex.turnover_rate if key == "turnover_rate" else (
                                        ex.volume_ratio if key == "volume_ratio" else ex.pct_chg
                                    )
                        clean_chunk.append(clean)

                    stmt = sqlite_insert(RealtimeSpot).values(clean_chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code"],
                            set_={
                                "name": excluded.name,
                                "price": excluded.price,
                                "pct_chg": excluded.pct_chg,
                                "pre_close": excluded.pre_close,
                                "high": excluded.high,
                                "open_price": excluded.open_price,
                                "low": excluded.low,
                                "volume": excluded.volume,
                                "amount": excluded.amount,
                                "turnover_rate": excluded.turnover_rate,
                                "volume_ratio": excluded.volume_ratio,
                                "trade_date": excluded.trade_date,
                                "source": excluded.source,
                                "slot": excluded.slot,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                existing = {
                    row.code: row
                    for row in session.execute(
                        select(RealtimeSpot).where(RealtimeSpot.code.in_(codes))
                    ).scalars().all()
                }
                new_count = 0
                for rec in records:
                    ent = existing.get(rec["code"])
                    if ent is None:
                        session.add(RealtimeSpot(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "price", "pct_chg", "pre_close",
                                     "high", "open_price", "low", "volume", "amount",
                                     "turnover_rate", "volume_ratio", "trade_date", "source", "slot"):
                            new_val = rec[key]
                            if key in _NULLABLE_KEYS and _is_null(new_val):
                                continue  # 保留 DB 已有值
                            setattr(ent, key, new_val)
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_realtime_spot", _write)
            logger.debug("[DB] upsert_realtime_spot: %d 条 (slot=%d, source=%s)", saved, slot, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_realtime_spot 失败: %s", e)
            raise

    def get_realtime_spot(self) -> pd.DataFrame:
        """获取全量实时行情快照，返回 DataFrame (index=code)。"""
        with self.get_session() as session:
            rows = session.execute(
                select(RealtimeSpot)
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "price": r.price,
                "pct_chg": r.pct_chg, "pre_close": r.pre_close,
                "open_price": r.open_price,
                "high": r.high, "low": r.low,
                "volume": r.volume, "amount": r.amount,
                "turnover_rate": r.turnover_rate,
                "volume_ratio": r.volume_ratio,
                "trade_date": r.trade_date,
                "source": r.source, "slot": r.slot,
            } for r in rows])
            return df.set_index("code")

    def get_realtime_spot_for_codes(self, codes: List[str]) -> pd.DataFrame:
        """按代码列表查询实时行情。"""
        if not codes:
            return pd.DataFrame()
        with self.get_session() as session:
            rows = session.execute(
                select(RealtimeSpot).where(RealtimeSpot.code.in_(codes))
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "price": r.price,
                "pct_chg": r.pct_chg, "pre_close": r.pre_close,
                "open_price": r.open_price,
                "high": r.high, "low": r.low,
                "volume": r.volume, "amount": r.amount,
                "turnover_rate": r.turnover_rate,
                "volume_ratio": r.volume_ratio,
                "trade_date": r.trade_date,
                "source": r.source, "slot": r.slot,
            } for r in rows])
            return df.set_index("code")

    def _get_latest_daily_spot(self) -> pd.DataFrame:
        """获取全市场最近交易日收盘价快照（非交易日回退用）。"""
        try:
            with self.get_session() as s:
                from sqlalchemy import text
                last_date_row = s.execute(
                    text("SELECT MAX(date) FROM stock_daily")
                ).fetchone()
                if not last_date_row or not last_date_row[0]:
                    return pd.DataFrame()
                last_date = last_date_row[0]

                rows = s.execute(
                    select(StockDaily).where(StockDaily.date == last_date)
                ).scalars().all()

                if not rows:
                    return pd.DataFrame()

                df = pd.DataFrame([{
                    "code": r.code,
                    "name": "",
                    "price": r.close,
                    "pct_chg": pd.NA,
                    "pre_close": pd.NA,
                    "open_price": r.open,
                    "high": r.high,
                    "low": r.low,
                    "volume": pd.NA,
                    "amount": pd.NA,
                    "turnover_rate": pd.NA,
                    "volume_ratio": pd.NA,
                    "trade_date": str(last_date),
                } for r in rows])
                return df.set_index("code")
        except Exception as e:
            logger.warning("[DB] _get_latest_daily_spot 失败: %s", e)
            return pd.DataFrame()

    def get_current_spot(self) -> pd.DataFrame:
        """获取全市场当前价格快照，交易日用 realtime_spot，非交易日用 stock_daily 最近收盘。"""
        from src.discovery.engine import is_trading_day

        if is_trading_day():
            df = self.get_realtime_spot()
            if not df.empty:
                return df
        return self._get_latest_daily_spot()

    def get_current_prices(self, codes: List[str]) -> pd.DataFrame:
        """获取指定代码当前价格快照，交易日用 realtime_spot，非交易日用 stock_daily 最近收盘。

        返回 DataFrame index=code，列: price, pct_chg, open, high, low, pre_close。
        """
        from src.discovery.engine import is_trading_day

        if is_trading_day():
            df = self.get_realtime_spot_for_codes(codes)
            if not df.empty:
                return df

        # 非交易日：取最近交易日收盘价
        full = self._get_latest_daily_spot()
        if full.empty:
            return full
        return full[full.index.isin(codes)]

    # ------------------------------------------------------------------
    # Limit pool (unified limit-up/down/broken data)
    # ------------------------------------------------------------------

    def clear_limit_pool_date(self, trade_date: str) -> int:
        """删除指定交易日全部涨跌停记录（盘中刷新前调用）。"""
        try:
            def _clear(session: Session) -> int:
                result = session.execute(
                    delete(LimitPool).where(LimitPool.trade_date == trade_date)
                )
                return result.rowcount
            count = self._run_write_transaction("clear_limit_pool_date", _clear)
            if count:
                logger.debug("[DB] clear_limit_pool_date(%s): %d 条", trade_date, count)
            return count
        except Exception as e:
            logger.error("[DB] clear_limit_pool_date 失败: %s", e)
            raise

    def delete_limit_pool_by_codes(self, trade_date: str, codes: List[str]) -> int:
        """按 code 列表删除指定交易日的涨跌停记录（退池清理）。"""
        if not codes:
            return 0
        try:
            def _del(session: Session) -> int:
                result = session.execute(
                    delete(LimitPool).where(
                        and_(LimitPool.trade_date == trade_date, LimitPool.code.in_(codes))
                    )
                )
                return result.rowcount
            count = self._run_write_transaction("delete_limit_pool_by_codes", _del)
            if count:
                logger.debug("[DB] delete_limit_pool_by_codes(%s): %d 条", trade_date, count)
            return count
        except Exception as e:
            logger.error("[DB] delete_limit_pool_by_codes 失败: %s", e)
            raise

    def insert_limit_pool_bulk(
        self, df: pd.DataFrame, source: str, slot: int
    ) -> int:
        """批量插入涨跌停记录（盘中用，已先删当天数据）。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            def _s(v, max_len=50):
                s_val = str(v) if v is not None and str(v) != "nan" else ""
                return s_val[:max_len] if max_len else s_val
            records.append({
                "code": code,
                "name": _s(row.get("name"), 50),
                "trade_date": _s(row.get("trade_date"), 8),
                "limit_type": _s(row.get("limit_type"), 2) or None,
                "pct_chg": self._normalize_sql_value(row.get("pct_chg")),
                "price": self._normalize_sql_value(row.get("price")),
                "limit_times": int(row.get("limit_times", 0) or 0),
                "open_times": int(row.get("open_times", 0) or 0),
                "up_stat": _s(row.get("up_stat"), 10) or None,
                "first_seal_time": _s(row.get("first_seal_time"), 10) or None,
                "last_seal_time": _s(row.get("last_seal_time"), 10) or None,
                "break_count": int(row.get("break_count", 0) or 0),
                "limit_stats": _s(row.get("limit_stats"), 50) or None,
                "sector": _s(row.get("sector"), 100) or None,
                "source": source,
                "slot": slot,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            _CHUNK = 100
            for i in range(0, len(records), _CHUNK):
                chunk = records[i : i + _CHUNK]
                session.execute(
                    insert(LimitPool).values(chunk)
                )
            return len(records)

        try:
            saved = self._run_write_transaction("insert_limit_pool", _write)
            logger.debug("[DB] insert_limit_pool: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] insert_limit_pool 失败: %s", e)
            raise

    def upsert_limit_pool(
        self, df: pd.DataFrame, source: str, slot: int
    ) -> int:
        """upsert 涨跌停记录 by (code, trade_date)。name 为空时不覆盖旧值。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue
            def _s(v, max_len=50):
                s_val = str(v) if v is not None and str(v) != "nan" else ""
                return s_val[:max_len] if max_len else s_val
            records.append({
                "code": code,
                "name": _s(row.get("name"), 50),
                "trade_date": trade_date,
                "limit_type": _s(row.get("limit_type"), 2) or None,
                "pct_chg": self._normalize_sql_value(row.get("pct_chg")),
                "price": self._normalize_sql_value(row.get("price")),
                "limit_times": int(row.get("limit_times", 0) or 0),
                "open_times": int(row.get("open_times", 0) or 0),
                "up_stat": _s(row.get("up_stat"), 10) or None,
                "first_seal_time": _s(row.get("first_seal_time"), 10) or None,
                "last_seal_time": _s(row.get("last_seal_time"), 10) or None,
                "break_count": int(row.get("break_count", 0) or 0),
                "limit_stats": _s(row.get("limit_stats"), 50) or None,
                "sector": _s(row.get("sector"), 100) or None,
                "float_market_cap": self._normalize_sql_value(row.get("float_market_cap")),
                "seal_amount": self._normalize_sql_value(row.get("seal_amount")),
                "source": source,
                "slot": slot,
                "updated_at": now,
            })

        if not records:
            return 0

        _UPDATE_KEYS = (
            "name", "limit_type", "pct_chg", "price",
            "limit_times", "open_times", "up_stat",
            "first_seal_time", "last_seal_time",
            "break_count", "limit_stats", "sector",
            "float_market_cap", "seal_amount",
            "source", "slot",
        )

        def _write(session: Session) -> int:
            codes = [r["code"] for r in records]
            dates = [r["trade_date"] for r in records]
            existing = {}
            for row in session.execute(
                select(LimitPool).where(
                    and_(LimitPool.code.in_(codes), LimitPool.trade_date.in_(dates))
                )
            ).scalars().all():
                existing[(row.code, row.trade_date)] = row
            _EMPTY_GUARD_KEYS = ("name", "first_seal_time", "last_seal_time",
                                 "float_market_cap", "seal_amount")
            new_count = 0
            for rec in records:
                ent = existing.get((rec["code"], rec["trade_date"]))
                if ent is None:
                    # 新记录也不写入空值字段，留给后续 akshare 补充
                    clean = {k: v for k, v in rec.items()
                             if k not in _EMPTY_GUARD_KEYS or v}
                    session.add(LimitPool(**clean))
                    new_count += 1
                else:
                    for key in _UPDATE_KEYS:
                        val = rec[key]
                        # Tushare 不提供这些字段，空值不覆盖 akshare 已写入的数据
                        if key in ("name", "first_seal_time", "last_seal_time",
                                    "float_market_cap", "seal_amount") and not val:
                            continue
                        setattr(ent, key, val)
                    ent.updated_at = now
            return new_count

        try:
            saved = self._run_write_transaction("upsert_limit_pool", _write)
            logger.debug("[DB] upsert_limit_pool: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_limit_pool 失败: %s", e)
            raise

    def upsert_sector_daily(self, records: List[Dict[str, Any]]) -> int:
        """upsert 板块日线数据 by (sector_name, trade_date)。"""
        if not records:
            return 0

        now = datetime.now()

        def _write(session: Session) -> int:
            names = list({r["sector_name"] for r in records})
            dates = list({r["trade_date"] for r in records})
            existing = {}
            for row in session.execute(
                select(SectorDaily).where(
                    and_(
                        SectorDaily.sector_name.in_(names),
                        SectorDaily.trade_date.in_(dates),
                    )
                )
            ).scalars().all():
                existing[(row.sector_name, row.trade_date)] = row

            new_count = 0
            for rec in records:
                key = (rec["sector_name"], rec["trade_date"])
                ent = existing.get(key)
                if ent is None:
                    session.add(SectorDaily(**rec))
                    new_count += 1
                else:
                    for col in ("close", "high", "low", "open", "pct_chg"):
                        if rec.get(col) is not None:
                            setattr(ent, col, rec[col])
                    ent.updated_at = now
            return new_count

        try:
            saved = self._run_write_transaction("upsert_sector_daily", _write)
            logger.info("[DB] upsert_sector_daily: %d 条新增", saved)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_sector_daily 失败: %s", e)
            raise

    def get_limit_pool(
        self, trade_date: Optional[str] = None,
        limit_type: Optional[str] = None,
        min_pct_chg: Optional[float] = None,
    ) -> pd.DataFrame:
        """获取涨跌停池，返回 DataFrame (index=code)。默认查今天。"""
        if trade_date is None:
            from datetime import date
            trade_date = date.today().strftime("%Y%m%d")
        with self.get_session() as session:
            stmt = select(LimitPool).where(LimitPool.trade_date == trade_date)
            if limit_type:
                stmt = stmt.where(LimitPool.limit_type == limit_type)
            if min_pct_chg is not None:
                stmt = stmt.where(LimitPool.pct_chg >= min_pct_chg)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "limit_type": r.limit_type, "pct_chg": r.pct_chg,
                "price": r.price, "limit_times": r.limit_times,
                "open_times": r.open_times, "up_stat": r.up_stat,
                "first_seal_time": r.first_seal_time,
                "last_seal_time": r.last_seal_time,
                "break_count": r.break_count, "limit_stats": r.limit_stats,
                "sector": r.sector,
                "float_market_cap": r.float_market_cap,
                "seal_amount": r.seal_amount,
                "source": r.source, "slot": r.slot,
            } for r in rows])
            return df.set_index("code")

    def get_existing_sectors(self, trade_date: str) -> Dict[str, str]:
        """查询指定交易日已有板块的代码→sector 映射（非空且非空字符串）。"""
        with self.get_session() as session:
            rows = session.execute(
                select(LimitPool.code, LimitPool.sector).where(
                    LimitPool.trade_date == trade_date,
                    LimitPool.sector.isnot(None),
                    LimitPool.sector != "",
                )
            ).all()
            return {r.code: r.sector for r in rows}

    def get_limit_pool_for_codes(
        self, codes: List[str], trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """按代码列表查询涨跌停记录。"""
        if not codes:
            return pd.DataFrame()
        if trade_date is None:
            from datetime import date
            trade_date = date.today().strftime("%Y%m%d")
        with self.get_session() as session:
            rows = session.execute(
                select(LimitPool).where(
                    and_(LimitPool.code.in_(codes), LimitPool.trade_date == trade_date)
                )
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "limit_type": r.limit_type, "pct_chg": r.pct_chg,
                "price": r.price, "limit_times": r.limit_times,
                "open_times": r.open_times, "up_stat": r.up_stat,
                "first_seal_time": r.first_seal_time,
                "last_seal_time": r.last_seal_time,
                "break_count": r.break_count, "limit_stats": r.limit_stats,
                "sector": r.sector,
                "float_market_cap": r.float_market_cap,
                "seal_amount": r.seal_amount,
                "source": r.source, "slot": r.slot,
            } for r in rows])
            return df.set_index("code")

    def get_limit_pool_seal_times(self, trade_date: str) -> Dict[str, tuple]:
        """获取指定日期已存储的封板时间。

        Returns:
            {code: (first_seal_time, last_seal_time)}，仅返回有值的记录
        """
        with self.get_session() as session:
            rows = session.execute(
                select(LimitPool.code, LimitPool.first_seal_time, LimitPool.last_seal_time)
                .where(
                    and_(
                        LimitPool.trade_date == trade_date,
                        LimitPool.first_seal_time.isnot(None),
                        LimitPool.first_seal_time != "",
                    )
                )
            ).fetchall()
            return {r[0]: (r[1], r[2]) for r in rows}

    # ------------------------------------------------------------------
    # LimitUpHistory CRUD
    # ------------------------------------------------------------------

    def get_limit_up_history_codes(self, trade_date: str) -> set:
        """获取今日涨停过的代码集合（用于差集检测）。"""
        with self.get_session() as session:
            rows = session.execute(
                select(LimitUpHistory.code).where(LimitUpHistory.trade_date == trade_date)
            ).fetchall()
            return {r[0] for r in rows}

    def get_limit_up_history(
        self, trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取今日涨停历史记录，返回 DataFrame (index=code)。"""
        if trade_date is None:
            from datetime import date
            trade_date = date.today().strftime("%Y%m%d")
        with self.get_session() as session:
            rows = session.execute(
                select(LimitUpHistory).where(LimitUpHistory.trade_date == trade_date)
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "first_seen": r.first_seen, "last_seen": r.last_seen,
                "open_times": r.open_times, "limit_times": r.limit_times,
                "sector": r.sector, "source": r.source,
            } for r in rows])
            return df.set_index("code")

    def insert_limit_up_history_bulk(
        self, df: pd.DataFrame, source: str
    ) -> int:
        """批量插入涨停历史（新票，已存在的 (code, trade_date) 跳过）。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if pd.notna(row.get("name")) else "",
                "trade_date": str(row.get("trade_date", ""))[:8],
                "first_seen": now,
                "last_seen": now,
                "open_times": int(row.get("open_times", 0) or 0),
                "limit_times": int(row.get("limit_times", 0) or 0),
                "sector": str(row.get("sector", ""))[:100] if pd.notna(row.get("sector")) else "",
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 100
                inserted = 0
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(LimitUpHistory).values(chunk)
                    try:
                        session.execute(
                            stmt.on_conflict_do_nothing(
                                index_elements=["code", "trade_date"]
                            )
                        )
                        inserted += len(chunk)
                    except Exception:
                        for rec in chunk:
                            try:
                                session.execute(
                                    sqlite_insert(LimitUpHistory).values(rec).
                                    on_conflict_do_nothing(
                                        index_elements=["code", "trade_date"]
                                    )
                                )
                                inserted += 1
                            except Exception:
                                pass
                return inserted
            else:
                inserted = 0
                for rec in records:
                    try:
                        session.add(LimitUpHistory(**rec))
                        inserted += 1
                    except Exception:
                        pass
                return inserted

        try:
            saved = self._run_write_transaction("insert_limit_up_history", _write)
            logger.debug("[DB] insert_limit_up_history: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] insert_limit_up_history 失败: %s", e)
            raise

    def clear_limit_up_history_date(self, trade_date: str) -> int:
        """删除指定交易日涨停历史记录。"""
        try:
            def _clear(session: Session) -> int:
                result = session.execute(
                    delete(LimitUpHistory).where(LimitUpHistory.trade_date == trade_date)
                )
                return result.rowcount
            count = self._run_write_transaction("clear_limit_up_history_date", _clear)
            if count:
                logger.debug("[DB] clear_limit_up_history_date(%s): %d 条", trade_date, count)
            return count
        except Exception as e:
            logger.error("[DB] clear_limit_up_history_date 失败: %s", e)
            raise

    # ------------------------------------------------------------------
    # LimitBreak CRUD
    # ------------------------------------------------------------------

    def get_limit_break(
        self, trade_date: Optional[str] = None, status: Optional[str] = None
    ) -> pd.DataFrame:
        """获取炸板记录，返回 DataFrame (index=code)。默认只查 broke 状态。"""
        if trade_date is None:
            from datetime import date
            trade_date = date.today().strftime("%Y%m%d")
        with self.get_session() as session:
            stmt = select(LimitBreak).where(LimitBreak.trade_date == trade_date)
            if status:
                stmt = stmt.where(LimitBreak.status == status)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "status": r.status, "last_pct_chg": r.last_pct_chg,
                "last_price": r.last_price, "open_times": r.open_times,
                "limit_times": r.limit_times,
                "sector": r.sector, "source": r.source,
            } for r in rows])
            return df.set_index("code")

    def get_limit_break_codes(self, trade_date: str, status: str = "broke") -> set:
        """获取当前炸板中/已回封的代码集合。"""
        with self.get_session() as session:
            rows = session.execute(
                select(LimitBreak.code).where(
                    LimitBreak.trade_date == trade_date,
                    LimitBreak.status == status,
                )
            ).fetchall()
            return {r[0] for r in rows}

    def upsert_limit_break(self, df: pd.DataFrame, source: str) -> int:
        """upsert 炸板记录 by (code, trade_date)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue
            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if pd.notna(row.get("name")) else "",
                "trade_date": trade_date,
                "status": str(row.get("status", "broke"))[:10],
                "last_pct_chg": self._normalize_sql_value(row.get("last_pct_chg")),
                "last_price": self._normalize_sql_value(row.get("last_price")),
                "open_times": int(row.get("open_times", 0) or 0),
                "limit_times": int(row.get("limit_times", 0) or 0),
                "sector": str(row.get("sector", ""))[:100] if pd.notna(row.get("sector")) else "",
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 100
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(LimitBreak).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "name": excluded.name,
                                "status": excluded.status,
                                "last_pct_chg": excluded.last_pct_chg,
                                "last_price": excluded.last_price,
                                "open_times": excluded.open_times,
                                "limit_times": excluded.limit_times,
                                "sector": excluded.sector,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                updated = 0
                for rec in records:
                    ent = session.execute(
                        select(LimitBreak).where(
                            LimitBreak.code == rec["code"],
                            LimitBreak.trade_date == rec["trade_date"],
                        )
                    ).scalars().first()
                    if ent is None:
                        session.add(LimitBreak(**rec))
                    else:
                        for key in ("name", "status",
                                    "last_pct_chg", "last_price",
                                    "open_times", "limit_times", "sector", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                    updated += 1
                return updated

        try:
            saved = self._run_write_transaction("upsert_limit_break", _write)
            logger.debug("[DB] upsert_limit_break: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_limit_break 失败: %s", e)
            raise

    def recover_limit_breaks(self, codes: List[str], trade_date: str) -> int:
        """标记指定股票为已回封（status='recovered'）。"""
        if not codes:
            return 0
        now = datetime.now()
        def _write(session: Session) -> int:
            result = session.execute(
                update(LimitBreak).where(
                    LimitBreak.code.in_(codes),
                    LimitBreak.trade_date == trade_date,
                    LimitBreak.status == "broke",
                ).values(status="recovered", updated_at=now)
            )
            return result.rowcount
        try:
            count = self._run_write_transaction("recover_limit_breaks", _write)
            if count:
                logger.debug("[DB] recover_limit_breaks: %d 只", count)
            return count
        except Exception as e:
            logger.error("[DB] recover_limit_breaks 失败: %s", e)
            raise

    def clear_limit_break_date(self, trade_date: str) -> int:
        """删除指定交易日炸板记录。"""
        try:
            def _clear(session: Session) -> int:
                result = session.execute(
                    delete(LimitBreak).where(LimitBreak.trade_date == trade_date)
                )
                return result.rowcount
            count = self._run_write_transaction("clear_limit_break_date", _clear)
            if count:
                logger.debug("[DB] clear_limit_break_date(%s): %d 条", trade_date, count)
            return count
        except Exception as e:
            logger.error("[DB] clear_limit_break_date 失败: %s", e)
            raise

    def upsert_money_flow(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """upsert 资金流向 by (code, trade_date)（盘后 Tushare 全量覆盖）。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue

            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if row.get("name") else "",
                "trade_date": trade_date,
                "buy_elg_amount": self._normalize_sql_value(row.get("buy_elg_amount")),
                "sell_elg_amount": self._normalize_sql_value(row.get("sell_elg_amount")),
                "buy_lg_amount": self._normalize_sql_value(row.get("buy_lg_amount")),
                "sell_lg_amount": self._normalize_sql_value(row.get("sell_lg_amount")),
                "buy_md_amount": self._normalize_sql_value(row.get("buy_md_amount")),
                "sell_md_amount": self._normalize_sql_value(row.get("sell_md_amount")),
                "buy_sm_amount": self._normalize_sql_value(row.get("buy_sm_amount")),
                "sell_sm_amount": self._normalize_sql_value(row.get("sell_sm_amount")),
                "net_mf_amount": self._normalize_sql_value(row.get("net_mf_amount")),
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(MoneyFlow).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "name": excluded.name,
                                "buy_elg_amount": excluded.buy_elg_amount,
                                "sell_elg_amount": excluded.sell_elg_amount,
                                "buy_lg_amount": excluded.buy_lg_amount,
                                "sell_lg_amount": excluded.sell_lg_amount,
                                "buy_md_amount": excluded.buy_md_amount,
                                "sell_md_amount": excluded.sell_md_amount,
                                "buy_sm_amount": excluded.buy_sm_amount,
                                "sell_sm_amount": excluded.sell_sm_amount,
                                "net_mf_amount": excluded.net_mf_amount,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(MoneyFlow).where(
                        and_(MoneyFlow.code.in_(codes), MoneyFlow.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.code, row.trade_date)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["trade_date"]))
                    if ent is None:
                        session.add(MoneyFlow(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "buy_elg_amount", "sell_elg_amount",
                                     "buy_lg_amount", "sell_lg_amount",
                                     "buy_md_amount", "sell_md_amount",
                                     "buy_sm_amount", "sell_sm_amount",
                                     "net_mf_amount", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_money_flow", _write)
            logger.debug("[DB] upsert_money_flow: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_money_flow 失败: %s", e)
            raise

    def upsert_popularity_rank(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """upsert 人气排行 by (code, trade_date)（盘后 Tushare dc_hot 全量覆盖）。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue

            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if row.get("name") else "",
                "trade_date": trade_date,
                "rank": int(row.get("rank", 0)) if pd.notna(row.get("rank")) else None,
                "pct_change": self._normalize_sql_value(row.get("pct_change")),
                "hot": self._normalize_sql_value(row.get("hot")),
                "concept": str(row.get("concept", ""))[:200] if row.get("concept") else "",
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(PopularityRank).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "name": excluded.name,
                                "rank": excluded.rank,
                                "pct_change": excluded.pct_change,
                                "hot": excluded.hot,
                                "concept": excluded.concept,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(PopularityRank).where(
                        and_(PopularityRank.code.in_(codes), PopularityRank.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.code, row.trade_date)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["trade_date"]))
                    if ent is None:
                        session.add(PopularityRank(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "rank", "pct_change", "hot", "concept", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_popularity_rank", _write)
            logger.debug("[DB] upsert_popularity_rank: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_popularity_rank 失败: %s", e)
            raise

    def upsert_momentum_snapshot(self, df: pd.DataFrame, source: str = "eastmoney") -> int:
        """upsert 盘中资金流快照 by (code, trade_date)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue

            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if row.get("name") else "",
                "trade_date": trade_date,
                "major_net": self._normalize_sql_value(row.get("major_net")),
                "lg_net": self._normalize_sql_value(row.get("lg_net")),
                "inflow_rate": self._normalize_sql_value(row.get("inflow_rate")),
                "pct_chg": self._normalize_sql_value(row.get("pct_chg")),
                "turnover_rate": self._normalize_sql_value(row.get("turnover_rate")),
                "volume_ratio": self._normalize_sql_value(row.get("volume_ratio")),
                "data_source": str(row.get("data_source", ""))[:30],
                "source": source,
                "fetch_time": now,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(MomentumSnapshot).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "name": excluded.name,
                                "major_net": excluded.major_net,
                                "lg_net": excluded.lg_net,
                                "inflow_rate": excluded.inflow_rate,
                                "pct_chg": excluded.pct_chg,
                                "turnover_rate": excluded.turnover_rate,
                                "volume_ratio": excluded.volume_ratio,
                                "data_source": excluded.data_source,
                                "source": excluded.source,
                                "fetch_time": excluded.fetch_time,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(MomentumSnapshot).where(
                        and_(MomentumSnapshot.code.in_(codes),
                             MomentumSnapshot.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.code, row.trade_date)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["trade_date"]))
                    if ent is None:
                        session.add(MomentumSnapshot(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "major_net", "lg_net", "inflow_rate",
                                    "pct_chg", "turnover_rate", "volume_ratio",
                                    "data_source", "source"):
                            setattr(ent, key, rec[key])
                        ent.fetch_time = now
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_momentum_snapshot", _write)
            logger.debug("[DB] upsert_momentum_snapshot: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_momentum_snapshot 失败: %s", e)
            raise

    def get_momentum_snapshot(
        self, trade_date: Optional[str] = None,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """读取盘中资金流快照。"""
        try:
            with self.get_session() as session:
                filters = []
                if trade_date:
                    filters.append(MomentumSnapshot.trade_date == str(trade_date)[:8])
                if codes:
                    filters.append(MomentumSnapshot.code.in_(codes))
                query = select(MomentumSnapshot)
                if filters:
                    query = query.where(and_(*filters))
                rows = session.execute(query).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code,
                "name": r.name,
                "trade_date": r.trade_date,
                "major_net": r.major_net,
                "lg_net": r.lg_net,
                "inflow_rate": r.inflow_rate,
                "pct_chg": r.pct_chg,
                "turnover_rate": r.turnover_rate,
                "volume_ratio": r.volume_ratio,
                "data_source": r.data_source,
                "source": r.source,
            } for r in rows])
            df.index = df["code"]
            return df
        except Exception as e:
            logger.warning("[DB] get_momentum_snapshot 失败: %s", e)
            return pd.DataFrame()

    def get_money_flow(
        self, trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取资金流向，返回 DataFrame (index=code)。默认查今天。"""
        if trade_date is None:
            trade_date = date.today().strftime("%Y%m%d")
        with self.get_session() as session:
            stmt = select(MoneyFlow).where(MoneyFlow.trade_date == trade_date)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "buy_elg_amount": r.buy_elg_amount,
                "sell_elg_amount": r.sell_elg_amount,
                "buy_lg_amount": r.buy_lg_amount,
                "sell_lg_amount": r.sell_lg_amount,
                "buy_md_amount": r.buy_md_amount,
                "sell_md_amount": r.sell_md_amount,
                "buy_sm_amount": r.buy_sm_amount,
                "sell_sm_amount": r.sell_sm_amount,
                "net_mf_amount": r.net_mf_amount,
                "source": r.source,
            } for r in rows])
            return df.set_index("code")

    def upsert_margin_detail(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """upsert 融资融券明细 by (code, trade_date)（盘后 Tushare 全量覆盖）。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue

            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if row.get("name") else "",
                "trade_date": trade_date,
                "rzye": self._normalize_sql_value(row.get("rzye")),
                "rzmre": self._normalize_sql_value(row.get("rzmre")),
                "rzche": self._normalize_sql_value(row.get("rzche")),
                "rqye": self._normalize_sql_value(row.get("rqye")),
                "rqyl": self._normalize_sql_value(row.get("rqyl")),
                "rqmcl": self._normalize_sql_value(row.get("rqmcl")),
                "rqchl": self._normalize_sql_value(row.get("rqchl")),
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(MarginDetail).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "name": excluded.name,
                                "rzye": excluded.rzye,
                                "rzmre": excluded.rzmre,
                                "rzche": excluded.rzche,
                                "rqye": excluded.rqye,
                                "rqyl": excluded.rqyl,
                                "rqmcl": excluded.rqmcl,
                                "rqchl": excluded.rqchl,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(MarginDetail).where(
                        and_(MarginDetail.code.in_(codes), MarginDetail.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.code, row.trade_date)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["trade_date"]))
                    if ent is None:
                        session.add(MarginDetail(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "rzye", "rzmre", "rzche",
                                     "rqye", "rqyl", "rqmcl", "rqchl", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_margin_detail", _write)
            logger.debug("[DB] upsert_margin_detail: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_margin_detail 失败: %s", e)
            raise

    # ------------------------------------------------------------------
    # 每日基本面指标
    # ------------------------------------------------------------------

    def upsert_daily_basic(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """全量 upsert 每日基本面指标 (daily_basic)。

        df 需包含列: code, trade_date, turnover_rate, volume_ratio, pe, pb, total_mv。
        按 (code, trade_date) 去重 upsert。
        """
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records = []
        for _, row in df.iterrows():
            records.append({
                "code": str(row.get("code", "")).strip().zfill(6),
                "trade_date": str(row.get("trade_date", "")),
                "turnover_rate": self._normalize_sql_value(row.get("turnover_rate")),
                "volume_ratio": self._normalize_sql_value(row.get("volume_ratio")),
                "pe": self._normalize_sql_value(row.get("pe")),
                "pb": self._normalize_sql_value(row.get("pb")),
                "total_mv": self._normalize_sql_value(row.get("total_mv")),
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 500
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(DailyBasic).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "turnover_rate": excluded.turnover_rate,
                                "volume_ratio": excluded.volume_ratio,
                                "pe": excluded.pe,
                                "pb": excluded.pb,
                                "total_mv": excluded.total_mv,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(DailyBasic).where(
                        and_(DailyBasic.code.in_(codes), DailyBasic.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.code, row.trade_date)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["trade_date"]))
                    if ent is None:
                        session.add(DailyBasic(**rec))
                        new_count += 1
                    else:
                        for key in ("turnover_rate", "volume_ratio", "pe",
                                     "pb", "total_mv", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_daily_basic", _write)
            logger.debug("[DB] upsert_daily_basic: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_daily_basic 失败: %s", e)
            raise

    def upsert_adj_factors(self, df: pd.DataFrame) -> int:
        """批量 upsert 复权因子 (stock_adj_factor)。

        df 需包含列: code, trade_date, adj_factor。
        按 (code, trade_date) 去重 upsert。
        """
        if df is None or df.empty:
            return 0

        now = datetime.now()
        from datetime import date as _date_cls
        records = []
        for _, row in df.iterrows():
            td = row.get("trade_date", "")
            if td is None or (isinstance(td, str) and not td.strip()):
                continue
            if isinstance(td, str):
                td = td.strip()
                td = _date_cls(int(td[:4]), int(td[4:6]), int(td[6:8]))
            records.append({
                "code": str(row.get("code", "")).strip().zfill(6),
                "trade_date": td,
                "adj_factor": float(row.get("adj_factor", 0.0)),
                "created_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 500
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(StockAdjFactor).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date"],
                            set_={
                                "adj_factor": excluded.adj_factor,
                                "created_at": excluded.created_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                _CHUNK = 500
                for j in range(0, len(dates), _CHUNK):
                    chunk_codes = codes[j : j + _CHUNK]
                    chunk_dates = dates[j : j + _CHUNK]
                    for row_o in session.execute(
                        select(StockAdjFactor).where(
                            and_(
                                StockAdjFactor.code.in_(chunk_codes),
                                StockAdjFactor.trade_date.in_(chunk_dates),
                            )
                        )
                    ).scalars().all():
                        existing[(row_o.code, str(row_o.trade_date))] = row_o
                new_count = 0
                for rec in records:
                    td = rec["trade_date"]
                    if isinstance(td, str):
                        from datetime import date as _d
                        td = _d(int(td[:4]), int(td[4:6]), int(td[6:8]))
                        rec["trade_date"] = td
                    key = (rec["code"], str(td))
                    ent = existing.get(key)
                    if ent:
                        ent.adj_factor = rec["adj_factor"]
                        ent.created_at = now
                    else:
                        session.add(StockAdjFactor(**rec))
                        new_count += 1
                return new_count

        try:
            saved = self._run_write_transaction("upsert_adj_factors", _write)
            logger.debug("[DB] upsert_adj_factors: %d 条", saved)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_adj_factors 失败: %s", e)
            raise

    def get_daily_basic(self, trade_date: str,
                        codes: Optional[List[str]] = None) -> pd.DataFrame:
        """读取指定交易日的基本面指标。

        Returns:
            DataFrame indexed by code
        """
        try:
            with self._SessionLocal() as session:
                stmt = select(DailyBasic).where(DailyBasic.trade_date == trade_date)
                if codes:
                    stmt = stmt.where(DailyBasic.code.in_(codes))
                rows = session.execute(stmt).scalars().all()
                if not rows:
                    return pd.DataFrame()
                df = pd.DataFrame([{
                    "code": r.code, "trade_date": r.trade_date,
                    "turnover_rate": r.turnover_rate, "volume_ratio": r.volume_ratio,
                    "pe": r.pe, "pb": r.pb, "total_mv": r.total_mv,
                    "source": r.source,
                } for r in rows])
                return df.set_index("code")
        except Exception as e:
            logger.error("[DB] get_daily_basic 失败: %s", e)
            return pd.DataFrame()

    def delete_daily_basic_before(self, cutoff_date: str) -> int:
        """删除指定日期之前的 daily_basic 数据。

        Args:
            cutoff_date: 截止日期 (YYYYMMDD)，早于此日期的数据会被删除。

        Returns:
            删除行数
        """
        try:
            with self._SessionLocal() as session:
                result = session.execute(
                    delete(DailyBasic).where(DailyBasic.trade_date < cutoff_date)
                )
                session.commit()
                deleted = result.rowcount
                if deleted > 0:
                    logger.info("[DB] 清理 daily_basic < %s: %d 条", cutoff_date, deleted)
                return deleted
        except Exception as e:
            logger.error("[DB] 清理 daily_basic 失败: %s", e)
            return 0

    def delete_performance_report_before(self, cutoff_period: str) -> int:
        """删除指定报告期之前的 performance_report 数据。

        Args:
            cutoff_period: 截止报告期 (YYYYMMDD)，早于此报告期的数据会被删除。

        Returns:
            删除行数
        """
        try:
            with self._SessionLocal() as session:
                result = session.execute(
                    delete(PerformanceReport).where(
                        PerformanceReport.report_period < cutoff_period
                    )
                )
                session.commit()
                deleted = result.rowcount
                if deleted > 0:
                    logger.info(
                        "[DB] 清理 performance_report < %s: %d 条",
                        cutoff_period, deleted,
                    )
                return deleted
        except Exception as e:
            logger.error("[DB] 清理 performance_report 失败: %s", e)
            return 0

    # ------------------------------------------------------------------
    # 同花顺行业映射
    # ------------------------------------------------------------------

    def upsert_ths_industry_map(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """全量刷入同花顺行业映射。先清空，再批量 insert。"""
        records = []
        for _, row in df.iterrows():
            code = str(row.get("stock_code", "")).strip().zfill(6)
            industry = str(row.get("industry_name", "")).strip()
            if not code or not industry:
                continue
            records.append({
                "stock_code": code,
                "industry_name": industry,
                "source": source,
                "updated_at": datetime.now(),
            })

        def _write(session: Session) -> int:
            session.execute(text("DELETE FROM ths_industry_map"))
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(ThsIndustryMap).values(chunk)
                    session.execute(stmt)
            else:
                for rec in records:
                    session.add(ThsIndustryMap(**rec))
            return len(records)

        saved = self._run_write_transaction("upsert_ths_industry_map", _write)
        logger.info("[DB] 同花顺行业映射入库 %d 条", saved)
        return saved

    def get_ths_industry_map(self) -> Dict[str, str]:
        """获取全量同花顺行业映射 {stock_code: industry_name}。"""
        from sqlalchemy import select as _select

        with self.get_session() as session:
            rows = session.execute(_select(ThsIndustryMap)).scalars().all()
            result: Dict[str, str] = {}
            for r in rows:
                result[r.stock_code] = r.industry_name
            return result

    def upsert_ths_industry_single(self, stock_code: str, industry_name: str,
                                   source: str = "akshare") -> bool:
        """插入或更新单条同花顺行业映射。"""
        code = str(stock_code).strip().zfill(6)
        industry = str(industry_name).strip()
        if not code or not industry:
            return False

        def _write(session: Session) -> bool:
            existing = session.query(ThsIndustryMap).filter(
                ThsIndustryMap.stock_code == code
            ).first()
            if existing:
                existing.industry_name = industry
                existing.source = source
                existing.updated_at = datetime.now()
            else:
                session.add(ThsIndustryMap(
                    stock_code=code,
                    industry_name=industry,
                    source=source,
                    updated_at=datetime.now(),
                ))
            return True

        return self._run_write_transaction("upsert_ths_industry_single", _write)

    def get_ths_industry_map_age_hours(self) -> Optional[float]:
        """返回 ths_industry_map 最近更新时间距今的小时数，表为空返回 None。"""
        try:
            from sqlalchemy import func, select as _select
            with self.get_session() as session:
                latest = session.execute(
                    _select(func.max(ThsIndustryMap.updated_at))
                ).scalar()
            if latest is None:
                return None
            delta = datetime.now() - latest
            return delta.total_seconds() / 3600
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 同花顺概念映射
    # ------------------------------------------------------------------

    def upsert_ths_concept_map(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """全量刷入同花顺概念映射。先清空，再批量 insert。"""
        records = []
        for _, row in df.iterrows():
            code = str(row.get("stock_code", "")).strip().zfill(6)
            concept = str(row.get("concept_name", "")).strip()
            if not code or not concept:
                continue
            records.append({
                "stock_code": code,
                "concept_name": concept,
                "source": source,
                "updated_at": datetime.now(),
            })

        def _write(session: Session) -> int:
            session.execute(text("DELETE FROM ths_concept_map"))
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(ThsConceptMap).values(chunk)
                    session.execute(stmt)
            else:
                for rec in records:
                    session.add(ThsConceptMap(**rec))
            return len(records)

        saved = self._run_write_transaction("upsert_ths_concept_map", _write)
        logger.info("[DB] 同花顺概念映射入库 %d 条", saved)
        return saved

    def get_ths_concept_map(self) -> Dict[str, List[str]]:
        """获取全量同花顺概念映射 {stock_code: [concept_names]}。"""
        from sqlalchemy import select as _select

        with self.get_session() as session:
            rows = session.execute(_select(ThsConceptMap)).scalars().all()
            result: Dict[str, List[str]] = {}
            for r in rows:
                result.setdefault(r.stock_code, []).append(r.concept_name)
            return result

    def get_stocks_by_concepts(self, concept_names: List[str]) -> Dict[str, List[str]]:
        """按概念名查询成分股 {concept_name: [stock_codes]}。

        Args:
            concept_names: 概念名称列表，精确匹配。
        """
        from sqlalchemy import select as _select

        if not concept_names:
            return {}

        with self.get_session() as session:
            stmt = _select(ThsConceptMap).where(
                ThsConceptMap.concept_name.in_(concept_names)
            )
            rows = session.execute(stmt).scalars().all()
            result: Dict[str, List[str]] = {}
            for r in rows:
                result.setdefault(r.concept_name, []).append(r.stock_code)
            return result

    def get_ths_concept_map_age_hours(self) -> Optional[float]:
        """返回 ths_concept_map 最近更新时间距今的小时数，表为空返回 None。"""
        try:
            from sqlalchemy import func, select as _select
            with self.get_session() as session:
                latest = session.execute(
                    _select(func.max(ThsConceptMap.updated_at))
                ).scalar()
            if latest is None:
                return None
            delta = datetime.now() - latest
            return delta.total_seconds() / 3600
        except Exception:
            return None

    def get_margin_detail_range(
        self, codes: Optional[List[str]] = None,
        start_date: Optional[str] = None, end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取融资融券明细多日数据，返回 DataFrame (index=code)。

        Args:
            codes: 股票代码列表，None 则查全市场
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        start_date = start_date.replace("-", "") if start_date else None
        end_date = end_date.replace("-", "") if end_date else None
        with self.get_session() as session:
            stmt = select(MarginDetail)
            if codes:
                stmt = stmt.where(MarginDetail.code.in_(codes))
            if start_date:
                stmt = stmt.where(MarginDetail.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(MarginDetail.trade_date <= end_date)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "rzye": r.rzye, "rzmre": r.rzmre, "rzche": r.rzche,
                "rqye": r.rqye, "rqyl": r.rqyl,
                "rqmcl": r.rqmcl, "rqchl": r.rqchl,
                "source": r.source,
            } for r in rows])
            return df.set_index("code")

    def get_popularity_rank_range(
        self, codes: Optional[List[str]] = None,
        start_date: Optional[str] = None, end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取人气排行多日数据，返回 DataFrame (index=code)。

        Args:
            codes: 股票代码列表，None 则查全市场
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        with self.get_session() as session:
            stmt = select(PopularityRank)
            if codes:
                stmt = stmt.where(PopularityRank.code.in_(codes))
            if start_date:
                stmt = stmt.where(PopularityRank.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(PopularityRank.trade_date <= end_date)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "rank": r.rank, "pct_change": r.pct_change,
                "hot": r.hot, "concept": r.concept,
                "source": r.source,
            } for r in rows])
            return df.set_index("code")

    def upsert_hm_detail(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """upsert 游资交易明细 by (code, trade_date, hm_name)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue

            records.append({
                "code": code,
                "name": str(row.get("ts_name", ""))[:50] if row.get("ts_name") else "",
                "trade_date": trade_date,
                "buy_amount": self._normalize_sql_value(row.get("buy_amount")),
                "sell_amount": self._normalize_sql_value(row.get("sell_amount")),
                "net_amount": self._normalize_sql_value(row.get("net_amount")),
                "hm_name": str(row.get("hm_name", ""))[:100] if row.get("hm_name") else "",
                "hm_orgs": str(row.get("hm_orgs", ""))[:200] if row.get("hm_orgs") else "",
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(HmDetail).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "trade_date", "hm_name"],
                            set_={
                                "name": excluded.name,
                                "buy_amount": excluded.buy_amount,
                                "sell_amount": excluded.sell_amount,
                                "net_amount": excluded.net_amount,
                                "hm_orgs": excluded.hm_orgs,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(HmDetail).where(
                        and_(HmDetail.code.in_(codes), HmDetail.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.code, row.trade_date, row.hm_name)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["trade_date"], rec["hm_name"]))
                    if ent is None:
                        session.add(HmDetail(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "buy_amount", "sell_amount",
                                     "net_amount", "hm_orgs", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_hm_detail", _write)
            logger.debug("[DB] upsert_hm_detail: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_hm_detail 失败: %s", e)
            raise

    def get_hm_detail_range(
        self, codes: Optional[List[str]] = None,
        start_date: Optional[str] = None, end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取游资交易明细多日数据，返回 DataFrame (index=code)。

        Args:
            codes: 股票代码列表，None 则查全市场
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        with self.get_session() as session:
            stmt = select(HmDetail)
            if codes:
                stmt = stmt.where(HmDetail.code.in_(codes))
            if start_date:
                stmt = stmt.where(HmDetail.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(HmDetail.trade_date <= end_date)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name, "trade_date": r.trade_date,
                "buy_amount": r.buy_amount, "sell_amount": r.sell_amount,
                "net_amount": r.net_amount, "hm_name": r.hm_name,
                "hm_orgs": r.hm_orgs, "source": r.source,
            } for r in rows])
            return df.set_index("code")

    def get_hm_detail_by_date(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """获取单日全市场游资明细，返回 DataFrame (index=ts_code)。

        用于 HotMoneyFactor DB 读路径，列名与 Tushare 返回保持一致。
        """
        if target_date is None:
            target_date = datetime.now().strftime("%Y%m%d")
        with self.get_session() as session:
            rows = session.execute(
                select(HmDetail).where(HmDetail.trade_date == target_date)
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "trade_date": r.trade_date,
                "ts_name": r.name,
                "buy_amount": r.buy_amount, "sell_amount": r.sell_amount,
                "net_amount": r.net_amount, "hm_name": r.hm_name,
                "hm_orgs": r.hm_orgs,
            } for r in rows])

        def _bare_to_ts(codes: pd.Series) -> pd.Series:
            codes = codes.astype(str).str.zfill(6)
            pre2 = codes.str[:2]
            sfx = pd.Series("SZ", index=codes.index)
            sfx[pre2.isin(["60", "68"])] = "SH"
            sfx[pre2.isin(["43", "83", "87", "92"])] = "BJ"
            return codes + "." + sfx

        df["ts_code"] = _bare_to_ts(df["code"])
        return df.drop(columns=["code"]).set_index("ts_code")

    def upsert_hm_quality(self, perf: pd.DataFrame) -> int:
        """全量覆盖游资质量评分表（每次 compute_performance 后调用）。"""
        if perf is None or perf.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for hm_name, row in perf.iterrows():
            records.append({
                "hm_name": str(hm_name),
                "win_rate": self._normalize_sql_value(row.get("win_rate")),
                "avg_return": self._normalize_sql_value(row.get("avg_return")),
                "total_trades": int(row.get("total_trades", 0)),
                "quality_score": self._normalize_sql_value(row.get("quality_score")),
                "computed_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                stmt = sqlite_insert(HmQuality).values(records)
                excluded = stmt.excluded
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["hm_name"],
                        set_={
                            "win_rate": excluded.win_rate,
                            "avg_return": excluded.avg_return,
                            "total_trades": excluded.total_trades,
                            "quality_score": excluded.quality_score,
                            "computed_at": excluded.computed_at,
                        },
                    )
                )
                return len(records)
            else:
                for r in records:
                    existing = session.execute(
                        select(HmQuality).where(HmQuality.hm_name == r["hm_name"])
                    ).scalar_one_or_none()
                    if existing:
                        for k, v in r.items():
                            if k != "hm_name":
                                setattr(existing, k, v)
                    else:
                        session.add(HmQuality(**r))
                return len(records)

        try:
            saved = self._run_write_transaction("upsert_hm_quality", _write)
            logger.debug("[DB] upsert_hm_quality: %d 条", saved)
            return saved
        except Exception:
            logger.warning("[DB] upsert_hm_quality 失败", exc_info=True)
            return 0

    def get_all_hm_quality(self) -> Dict[str, float]:
        """返回 {hm_name: quality_score} 映射（0-1 归一化值）。"""
        with self.get_session() as session:
            rows = session.execute(select(HmQuality)).scalars().all()
            return {r.hm_name: r.quality_score for r in rows}

    def upsert_cyq_perf(self, df: pd.DataFrame, source: str = "tushare") -> int:
        """upsert 筹码胜率数据 by (ts_code, trade_date)（盘后 Tushare 全量覆盖）。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for idx_val, row in df.iterrows():
            ts_code = str(row.get("ts_code", "")).strip()
            if not ts_code:
                ts_code = str(idx_val).strip()  # ts_code 可能在 index 上
            if not ts_code:
                continue
            trade_date = str(row.get("trade_date", ""))[:8]
            if not trade_date:
                continue

            records.append({
                "ts_code": ts_code,
                "trade_date": trade_date,
                "winner_rate": self._normalize_sql_value(row.get("winner_rate")),
                "cost_5pct": self._normalize_sql_value(row.get("cost_5pct")),
                "cost_15pct": self._normalize_sql_value(row.get("cost_15pct")),
                "cost_50pct": self._normalize_sql_value(row.get("cost_50pct")),
                "cost_85pct": self._normalize_sql_value(row.get("cost_85pct")),
                "cost_95pct": self._normalize_sql_value(row.get("cost_95pct")),
                "weight_avg": self._normalize_sql_value(row.get("weight_avg")),
                "his_low": self._normalize_sql_value(row.get("his_low")),
                "his_high": self._normalize_sql_value(row.get("his_high")),
                "cached_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(BrokerEnrichmentCyqPerf).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["ts_code", "trade_date"],
                            set_={
                                "winner_rate": excluded.winner_rate,
                                "cost_5pct": excluded.cost_5pct,
                                "cost_15pct": excluded.cost_15pct,
                                "cost_50pct": excluded.cost_50pct,
                                "cost_85pct": excluded.cost_85pct,
                                "cost_95pct": excluded.cost_95pct,
                                "weight_avg": excluded.weight_avg,
                                "his_low": excluded.his_low,
                                "his_high": excluded.his_high,
                                "cached_at": excluded.cached_at,
                            },
                        )
                    )
                return len(records)
            else:
                ts_codes = [r["ts_code"] for r in records]
                dates = [r["trade_date"] for r in records]
                existing = {}
                for row in session.execute(
                    select(BrokerEnrichmentCyqPerf).where(
                        and_(BrokerEnrichmentCyqPerf.ts_code.in_(ts_codes),
                             BrokerEnrichmentCyqPerf.trade_date.in_(dates))
                    )
                ).scalars().all():
                    existing[(row.ts_code, row.trade_date)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["ts_code"], rec["trade_date"]))
                    if ent is None:
                        session.add(BrokerEnrichmentCyqPerf(**rec))
                        new_count += 1
                    else:
                        for key in ("winner_rate", "cost_5pct", "cost_15pct", "cost_50pct",
                                     "cost_85pct", "cost_95pct", "weight_avg",
                                     "his_low", "his_high"):
                            setattr(ent, key, rec[key])
                        ent.cached_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_cyq_perf", _write)
            logger.debug("[DB] upsert_cyq_perf: %d 条 (source=%s)", saved, source)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_cyq_perf 失败: %s", e)
            raise

    def get_cyq_perf_range(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取筹码胜率多日数据，返回 DataFrame (index=ts_code)。

        Args:
            start_date: 起始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
        """
        start_date = start_date.replace("-", "") if start_date else None
        end_date = end_date.replace("-", "") if end_date else None
        with self.get_session() as session:
            stmt = select(BrokerEnrichmentCyqPerf)
            if start_date:
                stmt = stmt.where(BrokerEnrichmentCyqPerf.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(BrokerEnrichmentCyqPerf.trade_date <= end_date)
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "ts_code": r.ts_code, "trade_date": r.trade_date,
                "winner_rate": r.winner_rate, "cost_5pct": r.cost_5pct,
                "cost_15pct": r.cost_15pct, "cost_50pct": r.cost_50pct,
                "cost_85pct": r.cost_85pct, "cost_95pct": r.cost_95pct,
                "weight_avg": r.weight_avg, "his_low": r.his_low,
                "his_high": r.his_high,
            } for r in rows])
            return df.set_index("ts_code")

    @staticmethod
    def _parse_cn_number(val: Any) -> Optional[float]:
        """解析中文数字格式（万/亿）为 float。"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if not s:
            return None
        try:
            if "亿" in s:
                return float(s.replace("亿", "")) * 1e8
            if "万" in s:
                return float(s.replace("万", "")) * 1e4
            return float(s)
        except (ValueError, TypeError):
            return None

    def upsert_insider_buy(self, df: pd.DataFrame, source: str = "akshare") -> int:
        """upsert 险资举牌事件 by (ts_code, announce_date, buyer)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            ts_code = str(row.get("ts_code", row.name) if "ts_code" in df.columns else row.name).strip()
            announce_date = str(row.get("举牌公告日", row.get("announce_date", "")))[:10].strip()
            if not ts_code or not announce_date:
                continue
            records.append({
                "ts_code": ts_code,
                "stock_name": str(row.get("股票简称", row.get("stock_name", ""))).strip(),
                "announce_date": announce_date,
                "buyer": str(row.get("举牌方", row.get("buyer", ""))).strip(),
                "buy_shares": self._parse_cn_number(row.get("增持数量", row.get("buy_shares"))),
                "avg_price": self._normalize_sql_value(row.get("交易均价", row.get("avg_price"))),
                "add_ratio": self._normalize_sql_value(row.get("增持数量占总股本比例", row.get("add_ratio"))),
                "hold_shares": self._parse_cn_number(row.get("变动后持股总数", row.get("hold_shares"))),
                "hold_ratio": self._normalize_sql_value(row.get("变动后持股比例", row.get("hold_ratio"))),
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 100
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(InsiderBuy).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["ts_code", "announce_date", "buyer"],
                            set_={
                                "stock_name": excluded.stock_name,
                                "buy_shares": excluded.buy_shares,
                                "avg_price": excluded.avg_price,
                                "add_ratio": excluded.add_ratio,
                                "hold_shares": excluded.hold_shares,
                                "hold_ratio": excluded.hold_ratio,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                new_count = 0
                for rec in records:
                    ent = session.execute(
                        select(InsiderBuy).where(
                            and_(InsiderBuy.ts_code == rec["ts_code"],
                                 InsiderBuy.announce_date == rec["announce_date"],
                                 InsiderBuy.buyer == rec["buyer"])
                        )
                    ).scalar_one_or_none()
                    if ent is None:
                        session.add(InsiderBuy(**rec))
                        new_count += 1
                    else:
                        for key in ("stock_name", "buy_shares", "avg_price",
                                     "add_ratio", "hold_shares", "hold_ratio"):
                            setattr(ent, key, rec[key])
                        ent.source = source
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_insider_buy", _write)
            logger.debug("[DB] upsert_insider_buy: %d 条", saved)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_insider_buy 失败: %s", e)
            raise

    def get_insider_buy_recent(self, months: int = 6) -> pd.DataFrame:
        """获取近 N 个月的险资举牌事件，返回 DataFrame (index=ts_code)。"""
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
        with self.get_session() as session:
            rows = session.execute(
                select(InsiderBuy).where(
                    InsiderBuy.announce_date >= cutoff
                ).order_by(InsiderBuy.announce_date.desc())
            ).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "ts_code": r.ts_code, "stock_name": r.stock_name,
                "announce_date": r.announce_date, "buyer": r.buyer,
                "buy_shares": r.buy_shares, "avg_price": r.avg_price,
                "add_ratio": r.add_ratio, "hold_shares": r.hold_shares,
                "hold_ratio": r.hold_ratio,
            } for r in rows])
            # 保留全部事件，由调用方 _aggregate() 统一处理多事件聚合
            return df.set_index("ts_code")

    # ------------------------------------------------------------------
    # Institution Hold (机构持仓季度数据)

    def _derive_current_quarter(self) -> str:
        """根据系统日期推导当前财报季度。

        假设季报在季度结束后约 1-2 个月陆续披露，因此：
        - 1-3 月 → 上一年 Q4
        - 4-6 月 → 当年 Q1
        - 7-9 月 → 当年 Q2
        - 10-12 月 → 当年 Q3
        """
        now = datetime.now()
        y, m = now.year, now.month
        if m <= 3:
            return f"{y - 1}Q4"
        elif m <= 6:
            return f"{y}Q1"
        elif m <= 9:
            return f"{y}Q2"
        else:
            return f"{y}Q3"

    def has_institution_hold_quarter(self, quarter: str) -> bool:
        """检查指定季度是否已有机构持仓数据。"""
        with self.get_session() as session:
            try:
                count = session.execute(
                    select(InstitutionHold).where(
                        InstitutionHold.quarter == quarter
                    ).limit(1)
                ).scalar_one_or_none()
                return count is not None
            except Exception as e:
                logger.debug("[DB] has_institution_hold_quarter 失败: %s", e)
                return False

    def get_latest_institution_hold(self) -> pd.DataFrame:
        """获取最新季度的机构持仓数据，返回 DataFrame (index=code)。"""
        with self.get_session() as session:
            try:
                latest_q = session.execute(
                    select(InstitutionHold.quarter).order_by(
                        InstitutionHold.quarter.desc()
                    ).limit(1)
                ).scalar_one_or_none()
                if not latest_q:
                    return pd.DataFrame()

                rows = session.execute(
                    select(InstitutionHold).where(
                        InstitutionHold.quarter == latest_q
                    )
                ).scalars().all()

                if not rows:
                    return pd.DataFrame()

                return pd.DataFrame([{
                    "code": r.code, "name": r.name,
                    "inst_count": r.inst_count,
                    "inst_count_change": r.inst_count_change,
                    "hold_ratio": r.hold_ratio,
                    "hold_ratio_change": r.hold_ratio_change,
                    "circulate_ratio": r.circulate_ratio,
                    "circulate_ratio_change": r.circulate_ratio_change,
                    "quarter": r.quarter,
                } for r in rows]).set_index("code")
            except Exception as e:
                logger.warning("[DB] get_latest_institution_hold 失败: %s", e)
                return pd.DataFrame()

    def get_institution_hold_for_quarters(
        self, quarters: List[str]
    ) -> pd.DataFrame:
        """获取指定季度的机构持仓数据，返回 DataFrame (index=code)。"""
        if not quarters:
            return pd.DataFrame()
        with self.get_session() as session:
            try:
                rows = session.execute(
                    select(InstitutionHold).where(
                        InstitutionHold.quarter.in_(quarters)
                    )
                ).scalars().all()
                if not rows:
                    return pd.DataFrame()
                return pd.DataFrame([{
                    "code": r.code, "name": r.name,
                    "inst_count": r.inst_count,
                    "inst_count_change": r.inst_count_change,
                    "hold_ratio": r.hold_ratio,
                    "hold_ratio_change": r.hold_ratio_change,
                    "circulate_ratio": r.circulate_ratio,
                    "circulate_ratio_change": r.circulate_ratio_change,
                    "quarter": r.quarter,
                } for r in rows]).set_index("code")
            except Exception as e:
                logger.warning("[DB] get_institution_hold_for_quarters 失败: %s", e)
                return pd.DataFrame()

    def upsert_institution_hold(
        self, df: pd.DataFrame, quarter: str, source: str = "akshare"
    ) -> int:
        """upsert 机构持仓 by (code, quarter)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            code = str(row.get("code", row.name) if "code" in df.columns else row.name).strip()
            if not code:
                continue
            records.append({
                "code": code,
                "quarter": quarter,
                "name": str(row.get("name", ""))[:50] if row.get("name") else "",
                "inst_count": int(row.get("inst_count", 0)) if pd.notna(row.get("inst_count")) else 0,
                "inst_count_change": int(row.get("inst_count_change", 0)) if pd.notna(row.get("inst_count_change")) else 0,
                "hold_ratio": self._normalize_sql_value(row.get("hold_ratio")),
                "hold_ratio_change": self._normalize_sql_value(row.get("hold_ratio_change")),
                "circulate_ratio": self._normalize_sql_value(row.get("circulate_ratio")),
                "circulate_ratio_change": self._normalize_sql_value(row.get("circulate_ratio_change")),
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(InstitutionHold).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "quarter"],
                            set_={
                                "name": excluded.name,
                                "inst_count": excluded.inst_count,
                                "inst_count_change": excluded.inst_count_change,
                                "hold_ratio": excluded.hold_ratio,
                                "hold_ratio_change": excluded.hold_ratio_change,
                                "circulate_ratio": excluded.circulate_ratio,
                                "circulate_ratio_change": excluded.circulate_ratio_change,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                new_count = 0
                for rec in records:
                    ent = session.execute(
                        select(InstitutionHold).where(
                            and_(InstitutionHold.code == rec["code"],
                                 InstitutionHold.quarter == rec["quarter"])
                        )
                    ).scalar_one_or_none()
                    if ent is None:
                        session.add(InstitutionHold(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "inst_count", "inst_count_change",
                                     "hold_ratio", "hold_ratio_change",
                                     "circulate_ratio", "circulate_ratio_change"):
                            setattr(ent, key, rec[key])
                        ent.source = source
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_institution_hold", _write)
            logger.debug("[DB] upsert_institution_hold quarter=%s: %d 条", quarter, saved)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_institution_hold 失败: %s", e)
            raise

    # ------------------------------------------------------------------
    # 业绩报表数据
    # ------------------------------------------------------------------

    def get_performance_report(self, report_period: str) -> pd.DataFrame:
        """获取指定报告期的业绩报表，返回 DataFrame (index=code)。"""
        with self.get_session() as session:
            stmt = select(PerformanceReport).where(
                PerformanceReport.report_period == report_period
            )
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([{
                "code": r.code, "name": r.name,
                "report_period": r.report_period, "report_date": r.report_date,
                "eps": r.eps, "total_revenue": r.total_revenue,
                "revenue_yoy": r.revenue_yoy, "revenue_qoq": r.revenue_qoq,
                "net_profit": r.net_profit, "net_profit_yoy": r.net_profit_yoy,
                "net_profit_qoq": r.net_profit_qoq, "bps": r.bps,
                "roe": r.roe, "ocf_per_share": r.ocf_per_share,
                "gross_margin": r.gross_margin, "industry": r.industry,
                "source": r.source,
            } for r in rows])
            if not df.empty:
                df = df.set_index("code")
            return df

    def get_performance_report_multi_period(
        self, codes: List[str], periods: List[str]
    ) -> pd.DataFrame:
        """获取多只股票在多个报告期的数据，返回 DataFrame (index=code, columns=period-prefixed)。"""
        with self.get_session() as session:
            stmt = select(PerformanceReport).where(
                and_(
                    PerformanceReport.code.in_(codes),
                    PerformanceReport.report_period.in_(periods),
                )
            )
            rows = session.execute(stmt).scalars().all()
            if not rows:
                return pd.DataFrame()

            records = []
            for r in rows:
                records.append({
                    "code": r.code, "report_period": r.report_period,
                    "net_profit_yoy": r.net_profit_yoy,
                    "revenue_yoy": r.revenue_yoy,
                    "roe": r.roe, "gross_margin": r.gross_margin,
                    "eps": r.eps, "industry": r.industry,
                })

            df = pd.DataFrame(records)
            # Pivot to wide format: one row per code, columns like 20251231_net_profit_yoy
            pivoted = df.pivot(index="code", columns="report_period")
            # Flatten multi-level columns: net_profit_yoy_20251231
            pivoted.columns = [f"{col[1]}_{col[0]}" for col in pivoted.columns]
            return pivoted

    def upsert_performance_report(
        self, df: pd.DataFrame, report_period: str, source: str = "akshare"
    ) -> int:
        """upsert 业绩报表 by (code, report_period)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for idx, row in df.iterrows():
            code = str(idx).strip()
            if not code:
                continue
            records.append({
                "code": code,
                "name": str(row.get("name", ""))[:50] if pd.notna(row.get("name")) else "",
                "report_period": report_period,
                "report_date": str(row.get("report_date", ""))[:10] if pd.notna(row.get("report_date")) else "",
                "eps": self._normalize_sql_value(row.get("eps")),
                "total_revenue": self._normalize_sql_value(row.get("total_revenue")),
                "revenue_yoy": self._normalize_sql_value(row.get("revenue_yoy")),
                "revenue_qoq": self._normalize_sql_value(row.get("revenue_qoq")),
                "net_profit": self._normalize_sql_value(row.get("net_profit")),
                "net_profit_yoy": self._normalize_sql_value(row.get("net_profit_yoy")),
                "net_profit_qoq": self._normalize_sql_value(row.get("net_profit_qoq")),
                "bps": self._normalize_sql_value(row.get("bps")),
                "roe": self._normalize_sql_value(row.get("roe")),
                "ocf_per_share": self._normalize_sql_value(row.get("ocf_per_share")),
                "gross_margin": self._normalize_sql_value(row.get("gross_margin")),
                "industry": str(row.get("industry", ""))[:50] if pd.notna(row.get("industry")) else "",
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(PerformanceReport).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "report_period"],
                            set_={
                                "name": excluded.name,
                                "report_date": excluded.report_date,
                                "eps": excluded.eps,
                                "total_revenue": excluded.total_revenue,
                                "revenue_yoy": excluded.revenue_yoy,
                                "revenue_qoq": excluded.revenue_qoq,
                                "net_profit": excluded.net_profit,
                                "net_profit_yoy": excluded.net_profit_yoy,
                                "net_profit_qoq": excluded.net_profit_qoq,
                                "bps": excluded.bps,
                                "roe": excluded.roe,
                                "ocf_per_share": excluded.ocf_per_share,
                                "gross_margin": excluded.gross_margin,
                                "industry": excluded.industry,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                codes = [r["code"] for r in records]
                existing = {}
                for row in session.execute(
                    select(PerformanceReport).where(
                        and_(
                            PerformanceReport.code.in_(codes),
                            PerformanceReport.report_period == report_period,
                        )
                    )
                ).scalars().all():
                    existing[(row.code, row.report_period)] = row
                new_count = 0
                for rec in records:
                    ent = existing.get((rec["code"], rec["report_period"]))
                    if ent is None:
                        session.add(PerformanceReport(**rec))
                        new_count += 1
                    else:
                        for key in ("name", "report_date", "eps", "total_revenue",
                                     "revenue_yoy", "revenue_qoq", "net_profit",
                                     "net_profit_yoy", "net_profit_qoq", "bps",
                                     "roe", "ocf_per_share", "gross_margin",
                                     "industry", "source"):
                            setattr(ent, key, rec[key])
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_performance_report", _write)
            logger.debug(
                "[DB] upsert_performance_report period=%s: %d 条 (source=%s)",
                report_period, saved, source,
            )
            return saved
        except Exception as e:
            logger.error("[DB] upsert_performance_report 失败: %s", e)
            raise

    # ------------------------------------------------------------------
    # 回购数据
    # ------------------------------------------------------------------

    def has_repurchase_data(self, ann_date: str) -> bool:
        """检查指定公告日期之后是否有回购数据。"""
        with self.get_session() as session:
            try:
                count = session.execute(
                    select(Repurchase).where(
                        Repurchase.ann_date >= ann_date
                    ).limit(1)
                ).scalar_one_or_none()
                return count is not None
            except Exception as e:
                logger.debug("[DB] has_repurchase_data 失败: %s", e)
                return False

    def get_repurchase_recent(
        self, ann_date_from: Optional[str] = None
    ) -> pd.DataFrame:
        """获取近期回购数据，返回 DataFrame (index=ts_code)。

        默认取最近 90 天的公告数据。
        """
        with self.get_session() as session:
            try:
                stmt = select(Repurchase)
                if ann_date_from:
                    stmt = stmt.where(Repurchase.ann_date >= ann_date_from)
                stmt = stmt.order_by(Repurchase.ann_date.desc())
                rows = session.execute(stmt).scalars().all()
                if not rows:
                    return pd.DataFrame()
                return pd.DataFrame([{
                    "ts_code": r.ts_code,
                    "ann_date": r.ann_date,
                    "end_date": r.end_date,
                    "proc": r.proc,
                    "exp_date": r.exp_date,
                    "vol": r.vol,
                    "amount": r.amount,
                    "high_limit": r.high_limit,
                    "low_limit": r.low_limit,
                } for r in rows]).set_index("ts_code")
            except Exception as e:
                logger.warning("[DB] get_repurchase_recent 失败: %s", e)
                return pd.DataFrame()

    def upsert_repurchase(
        self, df: pd.DataFrame, source: str = "tushare"
    ) -> int:
        """upsert 回购数据 by (ts_code, ann_date)。"""
        if df is None or df.empty:
            return 0

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        for idx, row in df.iterrows():
            ts_code = str(idx) if df.index.name == "ts_code" else str(
                row.get("ts_code", row.name)
            ).strip()
            if not ts_code:
                continue
            records.append({
                "ts_code": ts_code,
                "ann_date": str(row.get("ann_date", ""))[:8],
                "end_date": str(row.get("end_date", ""))[:8] if pd.notna(row.get("end_date")) else "",
                "proc": str(row.get("proc", ""))[:50] if pd.notna(row.get("proc")) else "",
                "exp_date": str(row.get("exp_date", ""))[:8] if pd.notna(row.get("exp_date")) else "",
                "vol": self._normalize_sql_value(row.get("vol")),
                "amount": self._normalize_sql_value(row.get("amount")),
                "high_limit": self._normalize_sql_value(row.get("high_limit")),
                "low_limit": self._normalize_sql_value(row.get("low_limit")),
                "source": source,
                "updated_at": now,
            })

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 200
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(Repurchase).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["ts_code", "ann_date"],
                            set_={
                                "end_date": excluded.end_date,
                                "proc": excluded.proc,
                                "exp_date": excluded.exp_date,
                                "vol": excluded.vol,
                                "amount": excluded.amount,
                                "high_limit": excluded.high_limit,
                                "low_limit": excluded.low_limit,
                                "source": excluded.source,
                                "updated_at": excluded.updated_at,
                            },
                        )
                    )
                return len(records)
            else:
                new_count = 0
                for rec in records:
                    ent = session.execute(
                        select(Repurchase).where(
                            and_(Repurchase.ts_code == rec["ts_code"],
                                 Repurchase.ann_date == rec["ann_date"])
                        )
                    ).scalar_one_or_none()
                    if ent is None:
                        session.add(Repurchase(**rec))
                        new_count += 1
                    else:
                        for key in ("end_date", "proc", "exp_date",
                                     "vol", "amount", "high_limit", "low_limit"):
                            setattr(ent, key, rec[key])
                        ent.source = source
                        ent.updated_at = now
                return new_count

        try:
            saved = self._run_write_transaction("upsert_repurchase", _write)
            logger.debug("[DB] upsert_repurchase: %d 条", saved)
            return saved
        except Exception as e:
            logger.error("[DB] upsert_repurchase 失败: %s", e)
            raise

    # ------------------------------------------------------------------
    # 盈利预测快照
    # ------------------------------------------------------------------

    def save_profit_forecast(self, df: pd.DataFrame, trade_date: str) -> int:
        """全量覆盖写入某日的盈利预测数据。

        Args:
            df: akshare get_profit_forecast() 返回的 DataFrame (index=ts_code)
            trade_date: YYYYMMDD
        Returns:
            落库条数
        """
        if df is None or df.empty:
            return 0

        now = datetime.now()

        col_buy = next((c for c in df.columns if "买入" in c), None)
        col_add = next((c for c in df.columns if "增持" in c and "中性" not in c), None)
        col_neutral = next((c for c in df.columns if "中性" in c), None)
        col_reduce = next((c for c in df.columns if "减持" in c), None)
        col_sell = next((c for c in df.columns if "卖出" in c), None)
        col_report = next((c for c in df.columns if "研报数" in c), None)

        eps_cols = [c for c in df.columns if "预测每股收益" in c]
        eps_map = {}
        for col in eps_cols:
            m = re.search(r"(\d{4})", col)
            if m:
                eps_map[int(m.group(1))] = col

        def _get_int(row, col_name):
            if col_name is None:
                return 0
            v = row.get(col_name, 0)
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return 0

        def _get_float(row, col_name):
            if col_name is None:
                return None
            v = row.get(col_name)
            try:
                return float(v) if pd.notna(v) else None
            except (TypeError, ValueError):
                return None

        def _write(session):
            session.query(ProfitForecast).filter(
                ProfitForecast.trade_date == trade_date
            ).delete()
            session.flush()
            seen = set()
            new_count = 0
            for idx, row in df.iterrows():
                ts_code = str(idx) if isinstance(idx, str) else str(row.get("ts_code", ""))
                if not ts_code or ts_code in seen:
                    continue
                seen.add(ts_code)
                name = str(row.get("名称", row.get("name", "")))
                session.add(ProfitForecast(
                    trade_date=trade_date,
                    ts_code=ts_code,
                    name=name if name != "nan" else "",
                    report_count=_get_int(row, col_report),
                    buy_count=_get_int(row, col_buy),
                    add_count=_get_int(row, col_add),
                    neutral_count=_get_int(row, col_neutral),
                    reduce_count=_get_int(row, col_reduce),
                    sell_count=_get_int(row, col_sell),
                    eps_2025=_get_float(row, eps_map.get(2025)),
                    eps_2026=_get_float(row, eps_map.get(2026)),
                    eps_2027=_get_float(row, eps_map.get(2027)),
                    eps_2028=_get_float(row, eps_map.get(2028)),
                    updated_at=now,
                ))
                new_count += 1
            return new_count

        try:
            saved = self._run_write_transaction("save_profit_forecast", _write)
            logger.info("[DB] save_profit_forecast date=%s: %d 条", trade_date, saved)
            return saved
        except Exception as e:
            logger.error("[DB] save_profit_forecast 失败: %s", e)
            raise

    def get_latest_profit_forecast(self) -> Optional[pd.DataFrame]:
        """获取最新日期的盈利预测快照，返回 DataFrame (index=ts_code)。"""
        with self.get_session() as session:
            try:
                latest = session.execute(
                    select(ProfitForecast.trade_date).order_by(
                        ProfitForecast.trade_date.desc()
                    ).limit(1)
                ).scalar_one_or_none()
                if not latest:
                    return None

                rows = session.execute(
                    select(ProfitForecast).where(
                        ProfitForecast.trade_date == latest
                    )
                ).scalars().all()
                if not rows:
                    return None

                data = []
                for r in rows:
                    data.append({
                        "ts_code": r.ts_code,
                        "名称": r.name,
                        "研报数": r.report_count,
                        "机构投资评级(近六个月)-买入": r.buy_count,
                        "机构投资评级(近六个月)-增持": r.add_count,
                        "机构投资评级(近六个月)-中性": r.neutral_count,
                        "机构投资评级(近六个月)-减持": r.reduce_count,
                        "机构投资评级(近六个月)-卖出": r.sell_count,
                        "2025预测每股收益": r.eps_2025,
                        "2026预测每股收益": r.eps_2026,
                        "2027预测每股收益": r.eps_2027,
                        "2028预测每股收益": r.eps_2028,
                    })
                df = pd.DataFrame(data).set_index("ts_code")
                logger.debug("[DB] get_latest_profit_forecast: date=%s, %d rows", latest, len(df))
                return df
            except Exception as e:
                logger.warning("[DB] get_latest_profit_forecast 失败: %s", e)
                return None

    # ------------------------------------------------------------------
    # Daily data
    # ------------------------------------------------------------------

    def save_daily_data(
        self,
        df: pd.DataFrame,
        code: str,
        data_source: str = "Unknown"
    ) -> int:
        """
        保存日线数据到数据库
        
        策略：
        - 按 `(code, date)` 做批量 UPSERT，已存在记录会覆盖更新
        - 同一批次内若存在重复日期，以最后一条记录为准
        - SQLite 分支按 chunk 写入以避免绑定参数上限
        
        Args:
            df: 包含日线数据的 DataFrame
            code: 股票代码
            data_source: 数据来源名称
            
        Returns:
            本次实际新增的记录数（不含更新）
        """
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0

        now = datetime.now()
        records_by_date: Dict[date, Dict[str, Any]] = {}
        for row in df.to_dict(orient='records'):
            row_date = self._normalize_daily_date(row.get('date'))
            records_by_date[row_date] = {
                'code': code,
                'date': row_date,
                'open': self._normalize_sql_value(row.get('open')),
                'high': self._normalize_sql_value(row.get('high')),
                'low': self._normalize_sql_value(row.get('low')),
                'close': self._normalize_sql_value(row.get('close')),
                'volume': self._normalize_sql_value(row.get('volume')),
                'amount': self._normalize_sql_value(row.get('amount')),
                'pct_chg': self._normalize_sql_value(row.get('pct_chg')),
                'ma5': self._normalize_sql_value(row.get('ma5')),
                'ma10': self._normalize_sql_value(row.get('ma10')),
                'ma20': self._normalize_sql_value(row.get('ma20')),
                'volume_ratio': self._normalize_sql_value(row.get('volume_ratio')),
                'data_source': data_source,
                'created_at': now,
                'updated_at': now,
            }

        if not records_by_date:
            return 0

        records = list(records_by_date.values())
        batch_dates = list(records_by_date.keys())

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                # SQLite has a per-statement bind-parameter limit (commonly 999).
                # Each record has ~15 columns, so chunk upserts to stay within bounds.
                _SQLITE_CHUNK = 50
                # `_run_write_transaction()` opens SQLite writes with
                # `BEGIN IMMEDIATE`, so existence checks and upsert execute
                # within one stable write window.
                existing_dates = set()
                _COUNT_CHUNK = 500
                for j in range(0, len(batch_dates), _COUNT_CHUNK):
                    chunk_dates = batch_dates[j : j + _COUNT_CHUNK]
                    if not chunk_dates:
                        continue
                    existing_dates.update(
                        session.execute(
                            select(StockDaily.date).where(
                                and_(
                                    StockDaily.code == code,
                                    StockDaily.date.in_(chunk_dates),
                                )
                            )
                        ).scalars().all()
                    )
                new_records = [
                    record for record in records if record['date'] not in existing_dates
                ]
                for i in range(0, len(records), _SQLITE_CHUNK):
                    chunk = records[i : i + _SQLITE_CHUNK]
                    stmt = sqlite_insert(StockDaily).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=['code', 'date'],
                            set_={
                                'open': excluded.open,
                                'high': excluded.high,
                                'low': excluded.low,
                                'close': excluded.close,
                                'volume': excluded.volume,
                                'amount': excluded.amount,
                                'pct_chg': excluded.pct_chg,
                                'ma5': excluded.ma5,
                                'ma10': excluded.ma10,
                                'ma20': excluded.ma20,
                                'volume_ratio': excluded.volume_ratio,
                                'data_source': excluded.data_source,
                                'updated_at': excluded.updated_at,
                            },
                        )
                    )
                return len(new_records)
            else:
                existing_rows = {
                    row.date: row
                    for row in session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,
                                StockDaily.date.in_(batch_dates),
                            )
                        )
                    ).scalars().all()
                }
                new_count = 0
                for record in records:
                    existing = existing_rows.get(record['date'])
                    if existing is None:
                        session.add(StockDaily(**record))
                        new_count += 1
                        continue
                    existing.open = record['open']
                    existing.high = record['high']
                    existing.low = record['low']
                    existing.close = record['close']
                    existing.volume = record['volume']
                    existing.amount = record['amount']
                    existing.pct_chg = record['pct_chg']
                    existing.ma5 = record['ma5']
                    existing.ma10 = record['ma10']
                    existing.ma20 = record['ma20']
                    existing.volume_ratio = record['volume_ratio']
                    existing.data_source = record['data_source']
                    existing.updated_at = record['updated_at']
                return new_count

        try:
            saved_count = self._run_write_transaction(
                f"save_daily_data[{code}]",
                _write,
            )
            logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条")
            return saved_count
        except Exception as e:
            logger.error(f"保存 {code} 数据失败: {e}")
            raise

    def save_daily_batch(self, df: pd.DataFrame, data_source: str = "tushare_sync") -> int:
        """批量保存多股票日线数据。

        与 save_daily_data 不同，此方法接收多只股票的混合 DataFrame，
        按 (code, date) 执行 bulk UPSERT。

        Args:
            df: 包含 ts_code, trade_date, open, high, low, close, vol, amount, pct_chg 的 DataFrame
            data_source: 数据来源标签

        Returns:
            写入总行数（含更新）
        """
        if df is None or df.empty:
            return 0

        code_col = next(
            (c for c in ["ts_code", "code"] if c in df.columns), None
        )
        if code_col is None:
            logger.warning("[save_daily_batch] 无代码列")
            return 0

        # Indicator columns: only included when the source DataFrame carries them
        # (e.g. after BaseFetcher._calculate_indicators).  Tushare daily API does
        # not return ma5/ma10/ma20/volume_ratio, so batch syncs without indicator
        # computation must NOT overwrite existing values with NULL.
        _INDICATOR_COLS = ("ma5", "ma10", "ma20", "volume_ratio")
        has_indicators = any(c in df.columns for c in _INDICATOR_COLS)

        now = datetime.now()
        records: List[Dict[str, Any]] = []
        seen: set = set()
        for _, row in df.iterrows():
            raw_code = str(row.get(code_col, "")).strip()
            code = raw_code.split(".")[0] if "." in raw_code else raw_code
            row_date = self._normalize_daily_date(row.get("trade_date") or row.get("date"))
            if not code or not row_date:
                continue
            key = (code, row_date)
            if key in seen:
                continue
            seen.add(key)
            rec: Dict[str, Any] = {
                "code": code,
                "date": row_date,
                "open": self._normalize_sql_value(row.get("open")),
                "high": self._normalize_sql_value(row.get("high")),
                "low": self._normalize_sql_value(row.get("low")),
                "close": self._normalize_sql_value(row.get("close")),
                "volume": self._normalize_sql_value(row.get("vol") or row.get("volume")),
                "amount": self._normalize_sql_value(row.get("amount")),
                "pct_chg": self._normalize_sql_value(row.get("pct_chg")),
                "data_source": data_source,
                "created_at": now,
                "updated_at": now,
            }
            if has_indicators:
                for col in _INDICATOR_COLS:
                    if col in df.columns:
                        rec[col] = self._normalize_sql_value(row.get(col))
            records.append(rec)

        if not records:
            return 0

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                _CHUNK = 40
                for i in range(0, len(records), _CHUNK):
                    chunk = records[i : i + _CHUNK]
                    stmt = sqlite_insert(StockDaily).values(chunk)
                    excluded = stmt.excluded
                    set_: Dict[str, Any] = {
                        "open": excluded.open,
                        "high": excluded.high,
                        "low": excluded.low,
                        "close": excluded.close,
                        "volume": excluded.volume,
                        "amount": excluded.amount,
                        "pct_chg": excluded.pct_chg,
                        "data_source": excluded.data_source,
                        "updated_at": excluded.updated_at,
                    }
                    # Only update indicator columns when the incoming value is
                    # non-NULL; otherwise keep the existing DB value intact.
                    if has_indicators:
                        for col in _INDICATOR_COLS:
                            if col in df.columns:
                                set_[col] = case(
                                    (getattr(excluded, col).isnot(None),
                                     getattr(excluded, col)),
                                    else_=getattr(StockDaily, col),
                                )
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=["code", "date"],
                            set_=set_,
                        )
                    )
                return len(records)
            else:
                saved = 0
                for rec in records:
                    existing = session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == rec["code"],
                                StockDaily.date == rec["date"],
                            )
                        )
                    ).scalars().first()
                    if existing is None:
                        session.add(StockDaily(**rec))
                    else:
                        for col in ("open", "high", "low", "close", "volume",
                                     "amount", "pct_chg"):
                            setattr(existing, col, rec[col])
                        # Only update indicators when incoming value is present
                        if has_indicators:
                            for col in _INDICATOR_COLS:
                                if col in rec and rec[col] is not None:
                                    setattr(existing, col, rec[col])
                        existing.data_source = rec["data_source"]
                        existing.updated_at = now
                    saved += 1
                return saved

        try:
            saved = self._run_write_transaction("save_daily_batch", _write)
            logger.info("[save_daily_batch] 写入 %d 行 (%d 只股票)", saved, len(seen))
            return saved
        except Exception as e:
            logger.error("[save_daily_batch] 失败: %s", e)
            raise

    def get_recent_close_matrix(
        self, trade_date: str, lookback_trading_days: int = 60,
    ) -> pd.DataFrame:
        """获取全市场近期收盘价矩阵。

        返回 pivot DataFrame: index=code, columns=date (YYYY-MM-DD), values=close。
        用于计算 MA5/MA10/MA20/MA30 等均线指标。

        Args:
            trade_date: 目标交易日期 (YYYYMMDD)，以此为截止日期
            lookback_trading_days: 往前推的交易天数

        Returns:
            pivot DataFrame，若无数据返回空 DataFrame
        """
        from datetime import timedelta
        end_dt = datetime.strptime(trade_date, "%Y%m%d").date()
        cutoff = end_dt - timedelta(days=lookback_trading_days * 2)

        with self.get_session() as session:
            rows = session.execute(
                select(
                    StockDaily.code,
                    StockDaily.date,
                    StockDaily.close,
                ).where(
                    StockDaily.date >= cutoff,
                ).order_by(StockDaily.code, StockDaily.date)
            ).all()

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=["code", "date", "close"])
            # Drop duplicates keeping last
            df = df.drop_duplicates(subset=["code", "date"], keep="last")
            pivot = df.pivot(index="code", columns="date", values="close")
            pivot = pivot.sort_index(axis=1)
            return pivot

    def get_recent_ohlc_matrix(
        self, trade_date: str, lookback_trading_days: int = 30,
    ) -> pd.DataFrame:
        """获取全市场近期 OHLC 矩阵，用于本地计算 KDJ/BOLL。

        返回 pivot DataFrame: index=code, columns=MultiIndex (ohlc, date)
        ohlc 层级: high, low, close
        """
        from datetime import timedelta
        end_dt = datetime.strptime(trade_date, "%Y%m%d").date()
        cutoff = end_dt - timedelta(days=lookback_trading_days * 2)

        with self.get_session() as session:
            rows = session.execute(
                select(
                    StockDaily.code,
                    StockDaily.date,
                    StockDaily.high,
                    StockDaily.low,
                    StockDaily.close,
                ).where(
                    StockDaily.date >= cutoff,
                ).order_by(StockDaily.code, StockDaily.date)
            ).all()

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=["code", "date", "high", "low", "close"])
            df = df.drop_duplicates(subset=["code", "date"], keep="last")
            # MultiIndex: columns = (ohlc_field, date)
            pivot = df.pivot(index="code", columns="date", values=["high", "low", "close"])
            pivot = pivot.sort_index(axis=1)
            return pivot

    def prune_historical_data(self, retention_years: int = 10) -> dict:
        """清理超过保留年限的历史数据，覆盖所有日期驱动表。

        Args:
            retention_years: 保留多少年的数据，默认 10 年

        Returns:
            dict: 各表删除行数 + elapsed_seconds
        """
        from datetime import timedelta
        cutoff = datetime.now().date() - timedelta(days=retention_years * 365)
        cutoff_str = cutoff.strftime("%Y%m%d")

        # (表名, 日期列名, 参数值, param_name)
        # Date 类型列用 cutoff (Python date)，VARCHAR 类型列用 cutoff_str
        tables = [
            # ── Date 类型 ──
            ("stock_daily", "date", cutoff, "date_cutoff"),
            ("stock_tech_indicator", "date", cutoff, "date_cutoff"),
            ("sector_daily", "trade_date", cutoff, "date_cutoff"),
            # ── VARCHAR(8) 类型 ──
            ("daily_basic", "trade_date", cutoff_str, "str_cutoff"),
            ("momentum_snapshot", "trade_date", cutoff_str, "str_cutoff"),
            ("limit_pool", "trade_date", cutoff_str, "str_cutoff"),
            ("limit_up_history", "trade_date", cutoff_str, "str_cutoff"),
            ("limit_break", "trade_date", cutoff_str, "str_cutoff"),
            ("money_flow", "trade_date", cutoff_str, "str_cutoff"),
            ("margin_detail", "trade_date", cutoff_str, "str_cutoff"),
            ("popularity_rank", "trade_date", cutoff_str, "str_cutoff"),
            ("hm_detail", "trade_date", cutoff_str, "str_cutoff"),
            ("scan_result_intraday", "scan_date", cutoff_str, "str_cutoff"),
            ("scan_result_postmarket", "scan_date", cutoff_str, "str_cutoff"),
        ]

        result: dict = {}
        t0 = time.time()

        for table_name, date_col, param_val, _param_name in tables:
            key = f"{table_name}_deleted"
            result[key] = 0
            try:
                with self._engine.begin() as conn:
                    r = conn.execute(
                        text(f"DELETE FROM {table_name} WHERE {date_col} < :cutoff"),
                        {"cutoff": param_val},
                    )
                    result[key] = r.rowcount
            except Exception as exc:
                logger.warning("清理 %s 历史数据失败: %s", table_name, exc)

        result["elapsed_seconds"] = time.time() - t0
        deleted_any = any(
            v for k, v in result.items() if k != "elapsed_seconds"
        )
        if deleted_any:
            parts = ", ".join(
                f"{k.replace('_deleted', '')}={v}"
                for k, v in result.items()
                if k != "elapsed_seconds" and v
            )
            logger.info(
                "历史数据清理完成: %s, 耗时 %.2fs, 保留 %d 年",
                parts, result["elapsed_seconds"], retention_years,
            )
        return result

    def save_broker_recommend_monthly(self, month: str, df: pd.DataFrame) -> int:
        """批量保存券商月度金股推荐数据。

        Args:
            month: YYYYMM 格式月份
            df: 包含 broker, ts_code, name 列的 DataFrame

        Returns:
            保存的记录数
        """
        if df is None or df.empty:
            return 0

        with self.get_session() as session:
            try:
                # 计算每只股票被多少家券商推荐
                if 'broker_count' not in df.columns:
                    broker_count_df = df.groupby('ts_code')['broker'].nunique().reset_index()
                    broker_count_df.columns = ['ts_code', 'broker_count']
                    df = df.merge(broker_count_df, on='ts_code', how='left')
                    df['broker_count'] = df['broker_count'].fillna(1).astype(int)

                records = []
                for _, row in df.iterrows():
                    records.append(BrokerRecommendMonthly(
                        month=str(month),
                        broker=str(row.get('broker', '')),
                        ts_code=str(row.get('ts_code', '')),
                        name=str(row.get('name', '')) if pd.notna(row.get('name')) else '',
                        broker_count=int(row.get('broker_count', 1)),
                    ))

                session.query(BrokerRecommendMonthly).filter(
                    BrokerRecommendMonthly.month == str(month)
                ).delete()

                session.add_all(records)
                session.commit()
                logger.info(f"[BrokerRecommend] 保存 {month} 月数据 {len(records)} 条")
                return len(records)
            except Exception as e:
                session.rollback()
                logger.error(f"[BrokerRecommend] 保存失败: {e}")
                raise

    def get_broker_recommend_monthly(self, month: str) -> List[BrokerRecommendMonthly]:
        """获取指定月份的券商金股推荐数据。"""
        with self.get_session() as session:
            return list(session.execute(
                select(BrokerRecommendMonthly).where(
                    BrokerRecommendMonthly.month == str(month)
                )
            ).scalars().all())

    def get_broker_recommend_months(self) -> List[str]:
        """获取有数据的月份列表。"""
        with self.get_session() as session:
            months = session.execute(
                select(BrokerRecommendMonthly.month).distinct().order_by(desc(BrokerRecommendMonthly.month))
            ).scalars().all()
            return list(months)

    def get_broker_recommend_by_stock(self, ts_code: str) -> List[Dict[str, Any]]:
        """获取单只股票历次被推荐的月度记录（按月份降序）。"""
        with self.get_session() as session:
            rows = session.execute(
                select(BrokerRecommendMonthly).where(
                    BrokerRecommendMonthly.ts_code == str(ts_code)
                ).order_by(desc(BrokerRecommendMonthly.month))
            ).scalars().all()
            return [r.to_dict() for r in rows]


    def get_broker_recommend_month_counts(self, ts_codes: List[str], exclude_after: str | None = None) -> Dict[str, int]:
        """统计各股票历史上被推荐的月份数（去重 month）。"""
        if not ts_codes:
            return {}
        from sqlalchemy import func
        codes = [str(c) for c in ts_codes]
        with self.get_session() as session:
            query = select(
                BrokerRecommendMonthly.ts_code,
                func.count(func.distinct(BrokerRecommendMonthly.month)),
            ).where(
                BrokerRecommendMonthly.ts_code.in_(codes)
            )
            if exclude_after and str(exclude_after) >= "202003":
                query = query.where(BrokerRecommendMonthly.month < str(exclude_after))
            rows = session.execute(
                query.group_by(BrokerRecommendMonthly.ts_code)
            ).all()
            return {str(r[0]): int(r[1]) for r in rows}

    def get_consecutive_monthly_stocks(self, month: str) -> List[Dict[str, Any]]:
        """获取连续两个月都被券商推荐的金股。

        Args:
            month: YYYYMM 格式的当前月份

        Returns:
            [{"ts_code", "name", "broker_count_current", "broker_count_prev",
              "brokers_current": ["券商A", ...], "brokers_prev": ["券商B", ...]}]
        """
        # 计算上个月
        year = int(month[:4])
        mon = int(month[4:6])
        if mon == 1:
            prev_month = f"{year - 1}12"
        else:
            prev_month = f"{year}{mon - 1:02d}"

        with self.get_session() as session:
            # 当前月数据
            current_rows = session.execute(
                select(BrokerRecommendMonthly).where(
                    BrokerRecommendMonthly.month == month
                )
            ).scalars().all()

            # 上月数据
            prev_rows = session.execute(
                select(BrokerRecommendMonthly).where(
                    BrokerRecommendMonthly.month == prev_month
                )
            ).scalars().all()

        if not current_rows or not prev_rows:
            return []

        # 按 ts_code 分组
        prev_by_code: Dict[str, List[BrokerRecommendMonthly]] = {}
        for r in prev_rows:
            prev_by_code.setdefault(r.ts_code, []).append(r)

        result = []
        current_by_code: Dict[str, List[BrokerRecommendMonthly]] = {}
        for r in current_rows:
            current_by_code.setdefault(r.ts_code, []).append(r)

        for ts_code, cur_list in current_by_code.items():
            prev_list = prev_by_code.get(ts_code)
            if not prev_list:
                continue
            result.append({
                "ts_code": ts_code,
                "name": cur_list[0].name,
                "broker_count_current": len(cur_list),
                "broker_count_prev": len(prev_list),
                "brokers_current": [r.broker for r in cur_list],
                "brokers_prev": [r.broker for r in prev_list],
            })

        # 按当月推荐券商数降序
        result.sort(key=lambda x: -x["broker_count_current"])
        return result

    def save_broker_backtest(self, month: str, buy_date: str, sell_date: str,
                             total_recommendations: int, unique_stocks: int, unique_brokers: int,
                             stock_returns: List[Dict[str, Any]],
                             broker_returns: List[Dict[str, Any]]) -> None:
        """保存月度回测结果（价格收益部分，不含增强数据）。"""
        import json
        with self.get_session() as session:
            try:
                existing = session.execute(
                    select(BrokerBacktestResult).where(
                        BrokerBacktestResult.month == str(month)
                    )
                ).scalars().first()

                if existing:
                    existing.buy_date = buy_date
                    existing.sell_date = sell_date
                    existing.total_recommendations = total_recommendations
                    existing.unique_stocks = unique_stocks
                    existing.unique_brokers = unique_brokers
                    existing.stock_returns_json = json.dumps(stock_returns, ensure_ascii=False)
                    existing.broker_returns_json = json.dumps(broker_returns, ensure_ascii=False)
                    existing.computed_at = datetime.now()
                else:
                    session.add(BrokerBacktestResult(
                        month=str(month),
                        buy_date=buy_date,
                        sell_date=sell_date,
                        total_recommendations=total_recommendations,
                        unique_stocks=unique_stocks,
                        unique_brokers=unique_brokers,
                        stock_returns_json=json.dumps(stock_returns, ensure_ascii=False),
                        broker_returns_json=json.dumps(broker_returns, ensure_ascii=False),
                    ))
                session.commit()
                logger.info(f"[BrokerBacktest] 保存 {month} 回测结果")
            except Exception as e:
                session.rollback()
                logger.error(f"[BrokerBacktest] 保存失败: {e}")

    def get_broker_backtest(self, month: str) -> Optional[Dict[str, Any]]:
        """获取已存储的月度回测结果（不含增强数据）。"""
        import json
        with self.get_session() as session:
            row = session.execute(
                select(BrokerBacktestResult).where(
                    BrokerBacktestResult.month == str(month)
                )
            ).scalars().first()

            if not row:
                return None

            return {
                "month": row.month,
                "buy_date": row.buy_date,
                "sell_date": row.sell_date,
                "total_recommendations": row.total_recommendations,
                "unique_stocks": row.unique_stocks,
                "unique_brokers": row.unique_brokers,
                "brokers": json.loads(row.broker_returns_json or "[]"),
                "stock_returns": json.loads(row.stock_returns_json or "[]"),
            }

    def get_broker_backtest_months(self) -> List[str]:
        """获取 broker_backtest_result 表中有回测结果的月份列表（升序）。"""
        with self.get_session() as session:
            months = session.execute(
                select(BrokerBacktestResult.month).order_by(BrokerBacktestResult.month)
            ).scalars().all()
            return list(months)

    def get_all_broker_backtests(self) -> List[Dict[str, Any]]:
        """一次性获取所有月度回测结果，用于有记录以来复合计算。"""
        import json
        with self.get_session() as session:
            rows = session.execute(
                select(BrokerBacktestResult).order_by(BrokerBacktestResult.month)
            ).scalars().all()
            results = []
            for row in rows:
                results.append({
                    "month": row.month,
                    "buy_date": row.buy_date,
                    "sell_date": row.sell_date,
                    "total_recommendations": row.total_recommendations,
                    "unique_stocks": row.unique_stocks,
                    "unique_brokers": row.unique_brokers,
                    "brokers": json.loads(row.broker_returns_json or "[]"),
                    "stock_returns": json.loads(row.stock_returns_json or "[]"),
                })
            return results

    # ------------------------------------------------------------------
    # 港股通监控
    # ------------------------------------------------------------------

    def replace_hk_ggt_components(self, trade_date: str, rows: List[Dict[str, Any]]) -> int:
        """替换指定交易日的港股通成份快照。"""
        if not trade_date:
            return 0

        def _write(session: Session) -> int:
            session.query(HkGgtComponent).filter(
                HkGgtComponent.trade_date == trade_date
            ).delete(synchronize_session=False)
            if not rows:
                return 0
            records = []
            for row in rows:
                hk_code = str(row.get('hk_code', '')).strip()
                if not hk_code:
                    continue
                records.append(HkGgtComponent(
                    trade_date=trade_date,
                    hk_code=hk_code,
                    name=str(row.get('name', '') or '')[:100],
                    latest_price=self._normalize_sql_value(row.get('latest_price')),
                    pct_change=self._normalize_sql_value(row.get('pct_change')),
                    change_amount=self._normalize_sql_value(row.get('change_amount')),
                    open_price=self._normalize_sql_value(row.get('open')),
                    high=self._normalize_sql_value(row.get('high')),
                    low=self._normalize_sql_value(row.get('low')),
                    prev_close=self._normalize_sql_value(row.get('prev_close')),
                    volume=self._normalize_sql_value(row.get('volume')),
                    amount=self._normalize_sql_value(row.get('amount')),
                ))
            session.add_all(records)
            return len(records)

        saved = self._run_write_transaction("replace_hk_ggt_components", _write)
        logger.info("[HkGgt] 保存成份 %d 条 trade_date=%s", saved, trade_date)
        return saved

    def list_hk_ggt_components(self, trade_date: str) -> List[HkGgtComponent]:
        with self.get_session() as session:
            stmt = (
                select(HkGgtComponent)
                .where(HkGgtComponent.trade_date == trade_date)
                .order_by(desc(HkGgtComponent.pct_change))
            )
            return list(session.execute(stmt).scalars().all())

    def get_latest_hk_ggt_trade_date(self) -> Optional[str]:
        with self.get_session() as session:
            row = session.execute(
                select(HkGgtComponent.trade_date)
                .order_by(desc(HkGgtComponent.trade_date))
                .limit(1)
            ).scalar_one_or_none()
            return str(row) if row else None

    def list_hk_ggt_component_dates(self) -> List[str]:
        from sqlalchemy import distinct

        with self.get_session() as session:
            stmt = (
                select(distinct(HkGgtComponent.trade_date))
                .order_by(desc(HkGgtComponent.trade_date))
            )
            return [str(row[0]) for row in session.execute(stmt).all() if row[0]]

    def list_hk_ggt_codes_for_date(self, trade_date: str) -> List[str]:
        with self.get_session() as session:
            stmt = select(HkGgtComponent.hk_code).where(
                HkGgtComponent.trade_date == trade_date
            )
            return [str(row[0]) for row in session.execute(stmt).all() if row[0]]

    def upsert_hk_ggt_minute_bars(self, rows: List[Dict[str, Any]]) -> int:
        """批量插入或更新港股通分钟行情，同一分钟保留最新来源快照。"""
        records = []
        for row in rows:
            code_digits = ''.join(char for char in str(row.get('hk_code') or '') if char.isdigit())
            hk_code = code_digits[-5:].zfill(5) if code_digits else ''
            trade_date = str(row.get('trade_date') or '').replace('-', '')[:8]
            bar_time = str(row.get('bar_time') or '')[:19]
            close = self._normalize_sql_value(row.get('close'))
            if not hk_code or not trade_date or len(bar_time) < 16 or close is None:
                continue
            records.append({
                'hk_code': hk_code,
                'trade_date': trade_date,
                'bar_time': bar_time,
                'open': self._normalize_sql_value(row.get('open')),
                'high': self._normalize_sql_value(row.get('high')),
                'low': self._normalize_sql_value(row.get('low')),
                'close': close,
                'prev_close': self._normalize_sql_value(row.get('prev_close')),
                'pct_change': self._normalize_sql_value(row.get('pct_change')),
                'volume': self._normalize_sql_value(row.get('volume')),
                'amount': self._normalize_sql_value(row.get('amount')),
                'avg_price': self._normalize_sql_value(row.get('avg_price')),
                'period': str(row.get('period') or '1')[:8],
                'source': str(row.get('source') or 'unknown')[:32],
            })
        if not records:
            return 0

        def _write(session: Session) -> int:
            statement = sqlite_insert(HkGgtMinuteBar).values(records)
            excluded = statement.excluded
            statement = statement.on_conflict_do_update(
                index_elements=['hk_code', 'bar_time', 'period'],
                set_={
                    'trade_date': excluded.trade_date,
                    'open': excluded.open,
                    'high': excluded.high,
                    'low': excluded.low,
                    'close': excluded.close,
                    'prev_close': excluded.prev_close,
                    'pct_change': excluded.pct_change,
                    'volume': excluded.volume,
                    'amount': excluded.amount,
                    'avg_price': excluded.avg_price,
                    'source': excluded.source,
                    'created_at': datetime.now(),
                },
            )
            session.execute(statement)
            return len(records)

        saved = self._run_write_transaction('upsert_hk_ggt_minute_bars', _write)
        logger.info('[HkGgt] 保存分钟行情 %d 条 trade_date=%s', saved, records[0]['trade_date'])
        return saved

    def list_hk_ggt_minute_bars(
        self,
        hk_code: str,
        trade_date: str,
    ) -> List[HkGgtMinuteBar]:
        code_digits = ''.join(char for char in str(hk_code or '') if char.isdigit())
        code = code_digits[-5:].zfill(5) if code_digits else ''
        normalized_date = str(trade_date or '').replace('-', '')[:8]
        with self.get_session() as session:
            stmt = (
                select(HkGgtMinuteBar)
                .where(
                    HkGgtMinuteBar.hk_code == code,
                    HkGgtMinuteBar.trade_date == normalized_date,
                )
                .order_by(HkGgtMinuteBar.bar_time)
            )
            return list(session.execute(stmt).scalars().all())

    def list_hk_ggt_minute_bars_batch(
        self,
        codes: List[str],
        trade_date: str,
    ) -> Dict[str, List[HkGgtMinuteBar]]:
        if not codes:
            return {}
        normalized_codes = []
        for code in codes:
            code_digits = ''.join(char for char in str(code or '') if char.isdigit())
            if code_digits:
                normalized_codes.append(code_digits[-5:].zfill(5))
        if not normalized_codes:
            return {}
        normalized_date = str(trade_date or '').replace('-', '')[:8]
        with self.get_session() as session:
            stmt = (
                select(HkGgtMinuteBar)
                .where(
                    HkGgtMinuteBar.hk_code.in_(normalized_codes),
                    HkGgtMinuteBar.trade_date == normalized_date,
                )
                .order_by(HkGgtMinuteBar.hk_code, HkGgtMinuteBar.bar_time)
            )
            rows = list(session.execute(stmt).scalars().all())

        grouped: Dict[str, List[HkGgtMinuteBar]] = {}
        for row in rows:
            grouped.setdefault(row.hk_code, []).append(row)
        return grouped

    def upsert_hk_stock_daily_bars(self, rows: List[Dict[str, Any]]) -> int:
        """批量插入或更新港股通日线数据。"""
        if not rows:
            return 0
        saved = 0
        with self.get_session() as session:
            for row in rows:
                stmt = select(HkStockDaily).where(
                    HkStockDaily.hk_code == row["hk_code"],
                    HkStockDaily.trade_date == row["trade_date"],
                )
                existing = session.execute(stmt).scalar_one_or_none()
                if existing:
                    for key, val in row.items():
                        if hasattr(existing, key):
                            setattr(existing, key, val)
                else:
                    session.add(HkStockDaily(**row))
                saved += 1
            session.commit()
        logger.info("[Storage] upsert_hk_stock_daily_bars: %d rows", saved)
        return saved

    def list_hk_stock_daily_bars(
        self,
        hk_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[HkStockDaily]:
        code = str(hk_code).zfill(5)
        with self.get_session() as session:
            stmt = select(HkStockDaily).where(HkStockDaily.hk_code == code)
            if start_date:
                stmt = stmt.where(HkStockDaily.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(HkStockDaily.trade_date <= end_date)
            stmt = stmt.order_by(HkStockDaily.trade_date)
            return list(session.execute(stmt).scalars().all())

    def list_hk_stock_daily_bars_batch(
        self,
        codes: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[HkStockDaily]]:
        """批量查询多只港股通个股的日 K 线数据。

        Returns:
            {code: [HkStockDaily, ...]} 按 hk_code 分组，组内按 trade_date 升序。
        """
        if not codes:
            return {}
        from sqlalchemy import or_
        normed = [str(c).zfill(5) for c in codes]
        with self.get_session() as session:
            stmt = select(HkStockDaily).where(HkStockDaily.hk_code.in_(normed))
            if start_date:
                stmt = stmt.where(HkStockDaily.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(HkStockDaily.trade_date <= end_date)
            stmt = stmt.order_by(HkStockDaily.hk_code, HkStockDaily.trade_date)
            rows = list(session.execute(stmt).scalars().all())

        result: Dict[str, List[HkStockDaily]] = {}
        for r in rows:
            result.setdefault(r.hk_code, []).append(r)
        return result

    def get_latest_hk_stock_daily_trade_date(self, hk_code: str) -> Optional[str]:
        code = str(hk_code).zfill(5)
        with self.get_session() as session:
            row = session.execute(
                select(HkStockDaily.trade_date)
                .where(HkStockDaily.hk_code == code)
                .order_by(desc(HkStockDaily.trade_date))
                .limit(1)
            ).scalar_one_or_none()
            return str(row) if row else None

    def batch_get_latest_hk_stock_daily_trade_date(self, codes: List[str]) -> Dict[str, Optional[str]]:
        """批量获取多只港股的最新交易日。

        Returns:
            {hk_code: latest_trade_date or None}
        """
        if not codes:
            return {}
        normed = [str(c).zfill(5) for c in codes]
        from sqlalchemy import func
        with self.get_session() as session:
            stmt = (
                select(HkStockDaily.hk_code, func.max(HkStockDaily.trade_date))
                .where(HkStockDaily.hk_code.in_(normed))
                .group_by(HkStockDaily.hk_code)
            )
            rows = session.execute(stmt).all()
            return {str(row[0]): str(row[1]) if row[1] else None for row in rows}

    def batch_get_hk_stock_all_time_high(self, codes: List[str]) -> Dict[str, float]:
        """批量获取多只港股在数据库全部日线中的最高价。"""
        if not codes:
            return {}
        normed = [str(c).zfill(5) for c in codes]
        with self.get_session() as session:
            stmt = (
                select(HkStockDaily.hk_code, func.max(HkStockDaily.high))
                .where(
                    HkStockDaily.hk_code.in_(normed),
                    HkStockDaily.high.is_not(None),
                    HkStockDaily.high > 0,
                )
                .group_by(HkStockDaily.hk_code)
            )
            rows = session.execute(stmt).all()
            return {str(row[0]): float(row[1]) for row in rows if row[1] is not None}

    def get_min_hk_stock_daily_trade_date(self) -> Optional[str]:
        """取所有港股通日K线中最小的最新交易日（即数据最落后的那只）。"""
        import sqlalchemy as sa
        with self.get_session() as session:
            # 子查询：每只股票的最新交易日
            subq = (
                select(
                    HkStockDaily.hk_code,
                    sa.func.max(HkStockDaily.trade_date).label('max_td')
                )
                .group_by(HkStockDaily.hk_code)
                .subquery()
            )
            row = session.execute(
                select(sa.func.min(subq.c.max_td))
            ).scalar_one_or_none()
            return str(row) if row else None

    def get_hk_backfill_marker(self) -> str:
        """获取港股回填标记（最新已回填到哪一天）。"""
        self._ensure_kv_store()
        from sqlalchemy import text
        with self.get_session() as session:
            row = session.execute(
                text("SELECT val FROM kv_store WHERE key = 'hk_backfill_marker'")
            ).scalar_one_or_none()
            return str(row) if row else ""

    def set_hk_backfill_marker(self, trade_date: str) -> None:
        """设置港股回填标记。"""
        self._ensure_kv_store()
        from sqlalchemy import text
        with self.get_session() as session:
            session.execute(
                text("""
                    INSERT INTO kv_store (key, val, updated_at)
                    VALUES ('hk_backfill_marker', :val, datetime('now'))
                    ON CONFLICT(key) DO UPDATE SET val = :val2, updated_at = datetime('now')
                """),
                {"val": trade_date, "val2": trade_date},
            )
            session.commit()

    def _ensure_kv_store(self) -> None:
        """确保 kv_store 表存在。"""
        from sqlalchemy import text
        with self.get_session() as session:
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    val TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """))
            session.commit()

    # ------------------------------------------------------------------
    # 机构调研
    # ------------------------------------------------------------------

    def save_institution_survey(self, df: pd.DataFrame, clear_date: Optional[str] = None) -> int:
        """批量保存机构调研数据。

        Args:
            df: 包含 surv_date, ts_code, name, rece_org, org_type, rece_mode,
                weight, fund_visitors, rece_place, comp_rece 的 DataFrame
            clear_date: 若提供，先清除该日期的旧数据再写入

        Returns:
            保存的记录数
        """
        if df is None or df.empty:
            return 0

        with self.get_session() as session:
            try:
                if clear_date:
                    session.query(InstitutionSurvey).filter(
                        InstitutionSurvey.surv_date == clear_date
                    ).delete()

                records = []
                for _, row in df.iterrows():
                    records.append(InstitutionSurvey(
                        surv_date=str(row.get('surv_date', '')),
                        ts_code=str(row.get('ts_code', '')),
                        name=str(row.get('name', '')) if pd.notna(row.get('name')) else '',
                        rece_org=str(row.get('rece_org', '')) if pd.notna(row.get('rece_org')) else '',
                        org_type=str(row.get('org_type', '')) if pd.notna(row.get('org_type')) else '',
                        rece_mode=str(row.get('rece_mode', '')) if pd.notna(row.get('rece_mode')) else '',
                        weight=float(row.get('weight', 0.0) or 0.0),
                        fund_visitors=str(row.get('fund_visitors', '')) if pd.notna(row.get('fund_visitors')) else '',
                        rece_place=str(row.get('rece_place', '')) if pd.notna(row.get('rece_place')) else '',
                        comp_rece=str(row.get('comp_rece', '')) if pd.notna(row.get('comp_rece')) else '',
                    ))

                session.add_all(records)
                session.commit()
                logger.info(f"[InstitutionSurvey] 保存 {len(records)} 条 (clear_date={clear_date})")
                return len(records)
            except Exception as e:
                session.rollback()
                logger.error(f"[InstitutionSurvey] 保存失败: {e}")
                raise

    def get_institution_survey(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> List[InstitutionSurvey]:
        """获取机构调研数据。

        Args:
            start_date: 起始日期 YYYYMMDD（含）
            end_date: 截止日期 YYYYMMDD（含）

        Returns:
            InstitutionSurvey 列表
        """
        with self.get_session() as session:
            stmt = select(InstitutionSurvey)
            if start_date:
                stmt = stmt.where(InstitutionSurvey.surv_date >= start_date)
            if end_date:
                stmt = stmt.where(InstitutionSurvey.surv_date <= end_date)
            stmt = stmt.order_by(desc(InstitutionSurvey.surv_date))
            return list(session.execute(stmt).scalars().all())

    def get_institution_survey_dates(self) -> List[str]:
        """获取机构调研数据中所有有数据的日期列表（降序）。"""
        from sqlalchemy import distinct

        with self.get_session() as session:
            stmt = (
                select(distinct(InstitutionSurvey.surv_date))
                .order_by(desc(InstitutionSurvey.surv_date))
            )
            return [row[0] for row in session.execute(stmt).all()]

    # ------------------------------------------------------------------
    # Scan Results (intraday / postmarket full-replacement)
    # ------------------------------------------------------------------

    def save_scan_results_intraday(
        self, records: List[Dict[str, Any]], scan_date: str
    ) -> int:
        """保存盘中扫描全量结果（当日全覆盖，每轮替换前一轮）。

        Args:
            records: list of dicts with keys: scan_date, scan_round, scan_time,
                     ts_code, stock_code, stock_name, rank, total_score,
                     factor_scores (dict), sector
            scan_date: YYYYMMDD

        Returns:
            保存的记录数
        """
        if not records:
            return 0

        def _write(session: Session) -> int:
            session.query(ScanResultIntraday).filter(
                ScanResultIntraday.scan_date == scan_date
            ).delete()

            entities = []
            for r in records:
                factor_json = json.dumps(r.get("factor_scores", {}), ensure_ascii=False)
                entities.append(
                    ScanResultIntraday(
                        scan_date=scan_date,
                        scan_round=r.get("scan_round", 0),
                        scan_time=r.get("scan_time", ""),
                        ts_code=r.get("ts_code", ""),
                        stock_code=r.get("stock_code", ""),
                        stock_name=r.get("stock_name", ""),
                        rank=r.get("rank", 0),
                        total_score=self._normalize_sql_value(r.get("total_score")),
                        tech_score=self._normalize_sql_value(r.get("tech_score")),
                        composite_score=self._normalize_sql_value(r.get("composite_score")),
                        factor_scores_json=factor_json,
                        sector=r.get("sector", ""),
                    )
                )
            session.add_all(entities)
            return len(entities)

        try:
            saved = self._run_write_transaction("save_scan_results_intraday", _write)
            logger.info(
                "[ScanResultIntraday] 保存 %d 条 (scan_date=%s)",
                saved, scan_date,
            )
            return saved
        except Exception as e:
            logger.error("[ScanResultIntraday] 保存失败: %s", e)
            raise

    def has_postmarket_scan_today(self, scan_date: str) -> bool:
        """检查当天是否已完成盘后全量扫描。"""
        with self.get_session() as session:
            from sqlalchemy import exists
            return session.execute(
                select(exists().where(ScanResultPostmarket.scan_date == scan_date))
            ).scalar()

    def load_factor_signals_for_date(self, scan_date: str) -> Dict[str, Dict[str, Any]]:
        """加载指定日期的盘后全量因子评分缓存。

        Returns:
            {stock_code: {score, factor_scores, reasons, ...}}
        """
        with self.get_session() as session:
            rows = session.execute(
                select(ScanResultPostmarket).where(
                    ScanResultPostmarket.scan_date == scan_date
                )
            ).scalars().all()
            cache: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                cache[r.stock_code] = {
                    "score": r.total_score or 0,
                    "factor_scores": json.loads(r.factor_scores_json or "{}"),
                    "reasons": [],
                    "buy_price_low": None,
                    "buy_price_high": None,
                    "stop_loss": None,
                    "take_profit_1": None,
                    "take_profit_2": None,
                }
            return cache

    def get_top_scan_results(
        self, scan_date: str, mode: str = "postmarket", limit: int = 5
    ) -> List[str]:
        """获取指定日期扫描结果的 Top N stock_code 列表。

        优先读取 reports_discovery/{mode}_{date}_topn.json（已过滤超买/ST/低盈亏比），
        文件不存在或日期不一致时降级读 DB。

        Args:
            scan_date: YYYYMMDD
            mode: "intraday" 或 "postmarket"
            limit: 返回条数

        Returns:
            stock_code 列表（按 rank ASC 排序）
        """
        # 优先读 JSON（过滤后的准确结果）
        try:
            reports_dir = Path(__file__).resolve().parent.parent / "reports_discovery"
            json_file = reports_dir / f"{mode}_{scan_date}_topn.json"
            if json_file.exists():
                data = json.loads(json_file.read_text(encoding="utf-8"))
                if isinstance(data, list) and data:
                    # 验证第一个元素的日期一致性（rank/stock_code 必须有值）
                    codes = [
                        str(item.get("stock_code", "")).strip().zfill(6)
                        for item in data[:limit]
                        if item.get("stock_code")
                    ]
                    if codes:
                        logger.debug(
                            "[get_top_scan_results] 读取 JSON: %s, 返回 %d 个代码",
                            json_file.name, len(codes),
                        )
                        return codes
        except Exception as e:
            logger.debug("[get_top_scan_results] JSON 读取失败，降级 DB: %s", e)

        # 降级读 DB
        model = ScanResultIntraday if mode == "intraday" else ScanResultPostmarket
        with self.get_session() as session:
            rows = session.execute(
                select(model.stock_code)
                .where(model.scan_date == scan_date)
                .order_by(model.rank.asc())
                .limit(limit)
            ).scalars().all()
            return [str(r).strip().zfill(6) for r in rows if r]

    def save_scan_results_postmarket(
        self, records: List[Dict[str, Any]], scan_date: str
    ) -> int:
        """保存盘后扫描全量结果（每日全覆盖）。

        Args:
            records: list of dicts (same structure as intraday)
            scan_date: YYYYMMDD

        Returns:
            保存的记录数
        """
        if not records:
            return 0

        def _write(session: Session) -> int:
            session.query(ScanResultPostmarket).filter(
                ScanResultPostmarket.scan_date == scan_date
            ).delete()

            entities = []
            for r in records:
                factor_json = json.dumps(r.get("factor_scores", {}), ensure_ascii=False)
                entities.append(
                    ScanResultPostmarket(
                        scan_date=scan_date,
                        scan_round=r.get("scan_round", 0),
                        scan_time=r.get("scan_time", ""),
                        ts_code=r.get("ts_code", ""),
                        stock_code=r.get("stock_code", ""),
                        stock_name=r.get("stock_name", ""),
                        rank=r.get("rank", 0),
                        total_score=self._normalize_sql_value(r.get("total_score")),
                        tech_score=self._normalize_sql_value(r.get("tech_score")),
                        composite_score=self._normalize_sql_value(r.get("composite_score")),
                        factor_scores_json=factor_json,
                        sector=r.get("sector", ""),
                    )
                )
            session.add_all(entities)
            return len(entities)

        try:
            saved = self._run_write_transaction("save_scan_results_postmarket", _write)
            logger.info(
                "[ScanResultPostmarket] 保存 %d 条 (scan_date=%s)",
                saved, scan_date,
            )
            return saved
        except Exception as e:
            logger.error("[ScanResultPostmarket] 保存失败: %s", e)
            raise

    def save_factor_score_snapshots(
        self, raw_scores: Dict[str, object], trade_date: str, mode: str
    ) -> int:
        """保存因子得分快照（每轮扫描后调用）。

        Args:
            raw_scores: {factor_name: pd.Series(index=ts_code, values=0-100)}
            trade_date: YYYYMMDD
            mode: intraday / postmarket

        Returns:
            保存的记录数
        """
        if not raw_scores:
            return 0

        def _write(session: Session) -> int:
            # 只删除本次要写入的因子（避免误删同日期其他因子的数据）
            from sqlalchemy import delete as sa_delete
            factor_names = list(raw_scores.keys())
            session.execute(
                sa_delete(FactorScoreSnapshot).where(
                    FactorScoreSnapshot.trade_date == trade_date,
                    FactorScoreSnapshot.mode == mode,
                    FactorScoreSnapshot.factor_name.in_(factor_names),
                )
            )

            # 用 dict 去重：同 (ts_code, factor_name) 取 max score
            deduped: Dict[tuple, Any] = {}
            for factor_name, scores in raw_scores.items():
                if scores is None:
                    continue
                s = scores if hasattr(scores, 'items') else {}
                for ts_code, score_val in s.items():
                    if ts_code is None:
                        continue
                    key = (str(ts_code), factor_name)
                    val = self._normalize_sql_value(score_val)
                    existing = deduped.get(key)
                    if existing is None or (val is not None and val > (existing.score or 0)):
                        deduped[key] = FactorScoreSnapshot(
                            trade_date=trade_date,
                            ts_code=str(ts_code),
                            mode=mode,
                            factor_name=factor_name,
                            score=val,
                        )
            entities = list(deduped.values())
            session.add_all(entities)
            return len(entities)

        try:
            saved = self._run_write_transaction("save_factor_score_snapshots", _write)
            logger.info(
                "[FactorScoreSnapshot] 保存 %d 条 (trade_date=%s mode=%s)",
                saved, trade_date, mode,
            )
            return saved
        except Exception as e:
            logger.error("[FactorScoreSnapshot] 保存失败: %s", e)
            raise

    # ------------------------------------------------------------------
    # Enrichment 缓存（九转/盈利预测/筹码胜率持久化）
    # ------------------------------------------------------------------

    def get_enrichment_cache(
        self, ts_codes: List[str], trade_date: str
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """批量读取增强数据缓存。

        Returns:
            {ts_code: {"nineturn": {...}, "forecast": {...}, "cyq_perf": {...}}}
        """
        result: Dict[str, Dict[str, Dict[str, Any]]] = {}
        with self.get_session() as session:
            nt_rows = session.execute(
                select(BrokerEnrichmentNineturn).where(
                    BrokerEnrichmentNineturn.ts_code.in_(ts_codes),
                    BrokerEnrichmentNineturn.trade_date == trade_date,
                )
            ).scalars().all()
            for r in nt_rows:
                result.setdefault(r.ts_code, {})["nineturn"] = r.to_dict()

            fc_rows = session.execute(
                select(BrokerEnrichmentForecast).where(
                    BrokerEnrichmentForecast.ts_code.in_(ts_codes),
                    BrokerEnrichmentForecast.trade_date == trade_date,
                )
            ).scalars().all()
            for r in fc_rows:
                result.setdefault(r.ts_code, {})["forecast"] = r.to_dict()

            cyq_rows = session.execute(
                select(BrokerEnrichmentCyqPerf).where(
                    BrokerEnrichmentCyqPerf.ts_code.in_(ts_codes),
                    BrokerEnrichmentCyqPerf.trade_date == trade_date,
                )
            ).scalars().all()
            for r in cyq_rows:
                result.setdefault(r.ts_code, {})["cyq_perf"] = r.to_dict()

        return result

    def save_enrichment_cache(
        self,
        nineturn_data: Optional[Dict[str, Dict[str, Any]]] = None,
        forecast_data: Optional[Dict[str, Dict[str, Any]]] = None,
        cyq_data: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        """批量保存增强数据缓存。使用 merge 避免重复插入。"""
        with self.get_session() as session:
            try:
                if nineturn_data:
                    for ts_code, data in nineturn_data.items():
                        trade_date = data.get("trade_date", "")
                        if not trade_date:
                            continue
                        session.merge(BrokerEnrichmentNineturn(
                            ts_code=ts_code,
                            trade_date=trade_date,
                            up_count=data.get("up_count", 0),
                            down_count=data.get("down_count", 0),
                            nine_up_turn=data.get("nine_up_turn", 0),
                            nine_down_turn=data.get("nine_down_turn", 0),
                        ))
                if forecast_data:
                    for ts_code, data in forecast_data.items():
                        session.merge(BrokerEnrichmentForecast(
                            ts_code=ts_code,
                            trade_date=data.get("trade_date", ""),
                            eps=data.get("eps"),
                            pe=data.get("pe"),
                            roe=data.get("roe"),
                            np=data.get("np"),
                            rating=data.get("rating", ""),
                            min_price=data.get("min_price"),
                            max_price=data.get("max_price"),
                            imp_dg=data.get("imp_dg", ""),
                        ))
                if cyq_data:
                    for ts_code, data in cyq_data.items():
                        session.merge(BrokerEnrichmentCyqPerf(
                            ts_code=ts_code,
                            trade_date=data.get("trade_date", ""),
                            winner_rate=data.get("winner_rate"),
                            cost_5pct=data.get("cost_5pct"),
                            cost_15pct=data.get("cost_15pct"),
                            cost_50pct=data.get("cost_50pct"),
                            cost_85pct=data.get("cost_85pct"),
                            cost_95pct=data.get("cost_95pct"),
                            weight_avg=data.get("weight_avg"),
                            his_low=data.get("his_low"),
                            his_high=data.get("his_high"),
                        ))
                session.commit()
            except Exception as e:
                session.rollback()
                logger.warning(f"[EnrichmentCache] 保存失败: {e}")

    def get_analysis_context(
        self,
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取分析所需的上下文数据
        
        返回今日数据 + 昨日数据的对比信息
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            包含今日数据、昨日对比等信息的字典
        """
        if target_date is None:
            target_date = date.today()
        # 注意：尽管入参提供了 target_date，但当前实现实际使用的是“最新两天数据”（get_latest_data），
        # 并不会按 target_date 精确取当日/前一交易日的上下文。
        # 因此若未来需要支持“按历史某天复盘/重算”的可解释性，这里需要调整。
        # 该行为目前保留（按需求不改逻辑）。
        
        # 获取最近2天数据
        recent_data = self.get_latest_data(code, days=2)
        
        if not recent_data:
            logger.warning(f"未找到 {code} 的数据")
            return None
        
        today_data = recent_data[0]
        yesterday_data = recent_data[1] if len(recent_data) > 1 else None
        
        context = {
            'code': code,
            'date': today_data.date.isoformat(),
            'today': today_data.to_dict(),
        }
        
        if yesterday_data:
            context['yesterday'] = yesterday_data.to_dict()
            
            # 计算相比昨日的变化
            if (
                yesterday_data.volume and yesterday_data.volume > 0
                and today_data.volume is not None
            ):
                context['volume_change_ratio'] = round(
                    today_data.volume / yesterday_data.volume, 2
                )

            if (
                yesterday_data.close and yesterday_data.close > 0
                and today_data.close is not None
            ):
                context['price_change_ratio'] = round(
                    (today_data.close - yesterday_data.close) / yesterday_data.close * 100, 2
                )
            
            # 均线形态判断
            context['ma_status'] = self._analyze_ma_status(today_data)
        
        return context
    
    def _analyze_ma_status(self, data: StockDaily) -> str:
        """
        分析均线形态
        
        判断条件：
        - 多头排列：close > ma5 > ma10 > ma20
        - 空头排列：close < ma5 < ma10 < ma20
        - 震荡整理：其他情况
        """
        # 注意：这里的均线形态判断基于“close/ma5/ma10/ma20”静态比较，
        # 未考虑均线拐点、斜率、或不同数据源复权口径差异。
        # 该行为目前保留（按需求不改逻辑）。
        close = data.close or 0
        ma5 = data.ma5 or 0
        ma10 = data.ma10 or 0
        ma20 = data.ma20 or 0
        
        if close > ma5 > ma10 > ma20 > 0:
            return "多头排列 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "空头排列 📉"
        elif close > ma5 and ma5 > ma10:
            return "短期向好 🔼"
        elif close < ma5 and ma5 < ma10:
            return "短期走弱 🔽"
        else:
            return "震荡整理 ↔️"

    @staticmethod
    def _parse_published_date(value: Optional[str]) -> Optional[datetime]:
        """
        解析发布时间字符串（失败返回 None）
        """
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        # 优先尝试 ISO 格式
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _safe_json_dumps(data: Any) -> str:
        """
        安全序列化为 JSON 字符串
        """
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps(str(data), ensure_ascii=False)

    @staticmethod
    def _build_raw_result(result: Any) -> Dict[str, Any]:
        """
        生成完整分析结果字典
        """
        data = result.to_dict() if hasattr(result, "to_dict") else {}
        data.update({
            'data_sources': getattr(result, 'data_sources', ''),
            'raw_response': getattr(result, 'raw_response', None),
        })
        return data

    @staticmethod
    def _parse_sniper_value(value: Any) -> Optional[float]:
        return parse_sniper_value(value)

    def _extract_sniper_points(self, result: Any) -> Dict[str, Optional[float]]:
        """Extract normalized sniper point values from an AnalysisResult."""

        return extract_sniper_points(result)

    @staticmethod
    def _build_fallback_url_key(
        code: str,
        title: str,
        source: str,
        published_date: Optional[datetime]
    ) -> str:
        """
        生成无 URL 时的去重键（确保稳定且较短）
        """
        date_str = published_date.isoformat() if published_date else ""
        raw_key = f"{code}|{title}|{source}|{date_str}"
        digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
        return f"no-url:{code}:{digest}"

    def save_conversation_message(self, session_id: str, role: str, content: str) -> int:
        """
        保存 Agent 对话消息
        """
        with self.session_scope() as session:
            msg = ConversationMessage(
                session_id=session_id,
                role=role,
                content=content
            )
            session.add(msg)
            session.flush()
            return int(msg.id)

    def save_conversation_user_turn(
        self,
        session_id: str,
        content: str,
        selected_skill_ids: Optional[List[str]] = None,
    ) -> int:
        """Persist a user message and an optional session Skill selection atomically."""
        with self.session_scope() as session:
            msg = ConversationMessage(
                session_id=session_id,
                role="user",
                content=content,
            )
            session.add(msg)
            session.flush()

            if selected_skill_ids is not None:
                now = datetime.now()
                values = {
                    "session_id": session_id,
                    "selected_skill_ids_json": json.dumps(selected_skill_ids, ensure_ascii=False),
                    "created_at": now,
                    "updated_at": now,
                }
                stmt = sqlite_insert(ConversationSessionState).values(**values)
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["session_id"],
                        set_={
                            "selected_skill_ids_json": values["selected_skill_ids_json"],
                            "updated_at": now,
                        },
                    )
                )

            return int(msg.id)

    def get_conversation_session_selected_skill_ids(
        self,
        session_id: str,
    ) -> Optional[List[str]]:
        """Return the saved Skill selection, or None when the session has no state row."""
        with self.session_scope() as session:
            state = session.get(ConversationSessionState, session_id)
            if state is None:
                return None
            return json.loads(state.selected_skill_ids_json)

    def get_conversation_history(self, session_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        获取 Agent 对话历史
        """
        with self.session_scope() as session:
            stmt = select(ConversationMessage).filter(
                ConversationMessage.session_id == session_id
            ).order_by(ConversationMessage.created_at.desc()).limit(limit)
            messages = session.execute(stmt).scalars().all()

            # 倒序返回，保证时间顺序
            return [{"role": msg.role, "content": msg.content} for msg in reversed(messages)]

    def get_visible_conversation_messages(self, session_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return visible user/assistant conversation messages in chronological order."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(
                    and_(
                        ConversationMessage.session_id == session_id,
                        ConversationMessage.role.in_(["user", "assistant"]),
                    )
                )
                .order_by(ConversationMessage.created_at, ConversationMessage.id)
            )
            if limit is not None:
                stmt = (
                    stmt.order_by(None)
                    .order_by(ConversationMessage.created_at.desc(), ConversationMessage.id.desc())
                    .limit(limit)
                )
            messages = session.execute(stmt).scalars().all()
            if limit is not None:
                messages = list(reversed(messages))
            return [
                {
                    "id": msg.id,
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at,
                }
                for msg in messages
                if msg.content
            ]

    def get_conversation_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return the rolling summary for a conversation session, if present."""
        with self.session_scope() as session:
            stmt = select(ConversationSummary).where(
                ConversationSummary.session_id == session_id
            )
            row = session.execute(stmt).scalar_one_or_none()
            if row is None:
                return None
            return {
                "id": row.id,
                "session_id": row.session_id,
                "summary": row.summary,
                "covered_message_id": row.covered_message_id,
                "source_message_count": row.source_message_count,
                "estimated_tokens": row.estimated_tokens,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }

    def save_agent_provider_turn(
        self,
        *,
        session_id: str,
        run_id: str,
        provider: str,
        model: str,
        anchor_user_message_id: int,
        anchor_assistant_message_id: int,
        messages: List[Dict[str, Any]],
        contains_reasoning: bool,
        contains_tool_calls: bool,
        contains_thinking_blocks: bool,
        must_roundtrip: bool,
        estimated_tokens: int,
    ) -> int:
        """Persist one provider protocol trace and enforce per-model retention."""
        with self.session_scope() as session:
            row = AgentProviderTurn(
                session_id=session_id,
                run_id=run_id,
                provider=provider,
                model=model,
                anchor_user_message_id=int(anchor_user_message_id or 0),
                anchor_assistant_message_id=int(anchor_assistant_message_id or 0),
                messages_json=json.dumps(messages or [], ensure_ascii=False, default=str),
                contains_reasoning=bool(contains_reasoning),
                contains_tool_calls=bool(contains_tool_calls),
                contains_thinking_blocks=bool(contains_thinking_blocks),
                must_roundtrip=bool(must_roundtrip),
                estimated_tokens=int(estimated_tokens or 0),
            )
            session.add(row)
            session.flush()
            row_id = int(row.id)
            if row.must_roundtrip:
                self._trim_agent_provider_turns(
                    session=session,
                    session_id=session_id,
                    provider=provider,
                    model=model,
                    keep=PROVIDER_TRACE_RETENTION_LIMIT,
                )
            return row_id

    def get_agent_provider_turns(
        self,
        session_id: str,
        *,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        must_roundtrip_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return provider trace turns in chronological order."""
        with self.session_scope() as session:
            conditions = [AgentProviderTurn.session_id == session_id]
            if provider:
                conditions.append(AgentProviderTurn.provider == provider)
            if model:
                conditions.append(AgentProviderTurn.model == model)
            if must_roundtrip_only:
                conditions.append(AgentProviderTurn.must_roundtrip.is_(True))
            stmt = (
                select(AgentProviderTurn)
                .where(and_(*conditions))
                .order_by(AgentProviderTurn.created_at, AgentProviderTurn.id)
            )
            rows = session.execute(stmt).scalars().all()
            result = []
            for row in rows:
                try:
                    messages = json.loads(row.messages_json or "[]")
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Invalid provider trace messages_json skipped for session %s turn %s: %s",
                        row.session_id,
                        row.id,
                        exc,
                    )
                    messages = []
                result.append({
                    "id": row.id,
                    "session_id": row.session_id,
                    "run_id": row.run_id,
                    "provider": row.provider,
                    "model": row.model,
                    "anchor_user_message_id": row.anchor_user_message_id,
                    "anchor_assistant_message_id": row.anchor_assistant_message_id,
                    "messages": messages if isinstance(messages, list) else [],
                    "messages_json": row.messages_json,
                    "contains_reasoning": row.contains_reasoning,
                    "contains_tool_calls": row.contains_tool_calls,
                    "contains_thinking_blocks": row.contains_thinking_blocks,
                    "must_roundtrip": row.must_roundtrip,
                    "estimated_tokens": row.estimated_tokens,
                    "created_at": row.created_at,
                })
            return result

    def _trim_agent_provider_turns(
        self,
        *,
        session: Session,
        session_id: str,
        provider: str,
        model: str,
        keep: int,
    ) -> int:
        old_ids_stmt = (
            select(AgentProviderTurn.id)
            .where(
                and_(
                    AgentProviderTurn.session_id == session_id,
                    AgentProviderTurn.provider == provider,
                    AgentProviderTurn.model == model,
                    AgentProviderTurn.must_roundtrip.is_(True),
                )
            )
            .order_by(AgentProviderTurn.created_at.desc(), AgentProviderTurn.id.desc())
            .offset(max(0, int(keep)))
        )
        old_ids = list(session.execute(old_ids_stmt).scalars().all())
        if not old_ids:
            return 0
        result = session.execute(
            delete(AgentProviderTurn).where(AgentProviderTurn.id.in_(old_ids))
        )
        return int(result.rowcount or 0)

    def upsert_conversation_summary(
        self,
        session_id: str,
        summary: str,
        covered_message_id: int,
        source_message_count: int,
        estimated_tokens: int,
    ) -> None:
        """Create or update the rolling summary for a conversation session."""
        with self.session_scope() as session:
            now = datetime.now()
            values = {
                "session_id": session_id,
                "summary": summary,
                "covered_message_id": int(covered_message_id or 0),
                "source_message_count": int(source_message_count or 0),
                "estimated_tokens": int(estimated_tokens or 0),
                "updated_at": now,
            }
            stmt = sqlite_insert(ConversationSummary).values(**values)
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["session_id"],
                    set_=values,
                )
            )

    def conversation_session_exists(self, session_id: str) -> bool:
        """Return True when at least one message exists for the given session."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage.id)
                .where(ConversationMessage.session_id == session_id)
                .limit(1)
            )
            return session.execute(stmt).scalar() is not None

    def get_chat_sessions(
        self,
        limit: int = 50,
        session_prefix: Optional[str] = None,
        extra_session_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取聊天会话列表（从 conversation_messages 聚合）

        Args:
            limit: Maximum number of sessions to return.
            session_prefix: If provided, only return sessions whose session_id
                starts with this prefix.  Used for per-user isolation (e.g.
                ``"telegram_12345"``).
            extra_session_ids: Optional exact session ids to include in
                addition to the scoped prefix.

        Returns:
            按最近活跃时间倒序的会话列表，每条包含 session_id, title, message_count, last_active
        """
        from sqlalchemy import func

        with self.session_scope() as session:
            normalized_prefix = None
            if session_prefix:
                normalized_prefix = session_prefix if session_prefix.endswith(":") else f"{session_prefix}:"
            exact_ids = [sid for sid in (extra_session_ids or []) if sid]

            # 聚合每个 session 的消息数和最后活跃时间
            base = (
                select(
                    ConversationMessage.session_id,
                    func.count(ConversationMessage.id).label("message_count"),
                    func.min(ConversationMessage.created_at).label("created_at"),
                    func.max(ConversationMessage.created_at).label("last_active"),
                )
            )
            conditions = []
            if normalized_prefix:
                conditions.append(ConversationMessage.session_id.startswith(normalized_prefix))
            if exact_ids:
                conditions.append(ConversationMessage.session_id.in_(exact_ids))
            if conditions:
                base = base.where(or_(*conditions))
            stmt = (
                base
                .group_by(ConversationMessage.session_id)
                .order_by(desc(func.max(ConversationMessage.created_at)))
                .limit(limit)
            )
            rows = session.execute(stmt).all()

            results = []
            for row in rows:
                sid = row.session_id
                # 取该会话第一条 user 消息作为标题
                first_user_msg = session.execute(
                    select(ConversationMessage.content)
                    .where(
                        and_(
                            ConversationMessage.session_id == sid,
                            ConversationMessage.role == "user",
                        )
                    )
                    .order_by(ConversationMessage.created_at)
                    .limit(1)
                ).scalar()
                title = (first_user_msg or "新对话")[:60]

                results.append({
                    "session_id": sid,
                    "title": title,
                    "message_count": row.message_count,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "last_active": row.last_active.isoformat() if row.last_active else None,
                })
            return results

    def get_conversation_messages(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取单个会话的完整消息列表（用于前端恢复历史）
        """
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(ConversationMessage.session_id == session_id)
                .order_by(ConversationMessage.created_at)
                .limit(limit)
            )
            messages = session.execute(stmt).scalars().all()
            return [
                {
                    "id": str(msg.id),
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at.isoformat() if msg.created_at else None,
                }
                for msg in messages
            ]

    def delete_conversation_session(self, session_id: str) -> int:
        """
        删除指定会话的所有消息

        Returns:
            删除的消息数
        """
        with self.session_scope() as session:
            session.execute(
                delete(ConversationSessionState).where(
                    ConversationSessionState.session_id == session_id
                )
            )
            session.execute(
                delete(AgentProviderTurn).where(
                    AgentProviderTurn.session_id == session_id
                )
            )
            session.execute(
                delete(ConversationSummary).where(
                    ConversationSummary.session_id == session_id
                )
            )
            result = session.execute(
                delete(ConversationMessage).where(
                    ConversationMessage.session_id == session_id
                )
            )
            return result.rowcount

    # ------------------------------------------------------------------
    # LLM usage tracking
    # ------------------------------------------------------------------

    def record_llm_usage(
        self,
        call_type: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        stock_code: Optional[str] = None,
        **telemetry: Any,
    ) -> None:
        """Append one LLM call record to llm_usage."""
        row_values: Dict[str, Any] = {
            "call_type": call_type,
            "model": model or "unknown",
            "stock_code": stock_code,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }
        for column in _LLM_USAGE_TELEMETRY_COLUMN_SQL:
            row_values[column] = None if column in _LLM_USAGE_DROPPED_FREE_TEXT_COLUMNS else telemetry.get(column)
        row = LLMUsage(**row_values)
        with self.session_scope() as session:
            session.add(row)

    def get_llm_usage_summary(
        self,
        from_dt: datetime,
        to_dt: datetime,
    ) -> Dict[str, Any]:
        """Return aggregated token usage between from_dt and to_dt.

        Returns a dict with keys:
          total_calls, total_prompt_tokens, total_completion_tokens, total_tokens,
          by_call_type: list of {call_type, calls, prompt_tokens,
            completion_tokens, total_tokens},
          by_model: list of {model, calls, prompt_tokens, completion_tokens,
            total_tokens, max_total_tokens}
        """
        with self.session_scope() as session:
            base_filter = and_(
                LLMUsage.called_at >= from_dt,
                LLMUsage.called_at <= to_dt,
            )

            # Overall totals
            totals = session.execute(
                select(
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                ).where(base_filter)
            ).one()

            # Breakdown by call_type
            by_type_rows = session.execute(
                select(
                    LLMUsage.call_type,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.call_type)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

            # Breakdown by model
            by_model_rows = session.execute(
                select(
                    LLMUsage.model,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                    func.coalesce(func.max(LLMUsage.total_tokens), 0).label("max_total_tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.model)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

        return {
            "total_calls": totals.calls,
            "total_prompt_tokens": totals.prompt_tokens,
            "total_completion_tokens": totals.completion_tokens,
            "total_tokens": totals.tokens,
            "by_call_type": [
                {
                    "call_type": r.call_type,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.tokens,
                }
                for r in by_type_rows
            ],
            "by_model": [
                {
                    "model": r.model,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.tokens,
                    "max_total_tokens": r.max_total_tokens,
                }
                for r in by_model_rows
            ],
        }

    def get_llm_usage_records(
        self,
        from_dt: datetime,
        to_dt: datetime,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return recent LLM usage audit rows between from_dt and to_dt.

        Each row contains id, call_type, model, stock_code, prompt_tokens,
        completion_tokens, total_tokens, and called_at. Results are ordered by
        newest call first, and limit is clamped to the public API range.
        """
        normalized_limit = max(1, min(int(limit or 50), 200))
        with self.session_scope() as session:
            rows = session.execute(
                select(
                    LLMUsage.id,
                    LLMUsage.call_type,
                    LLMUsage.model,
                    LLMUsage.stock_code,
                    LLMUsage.prompt_tokens,
                    LLMUsage.completion_tokens,
                    LLMUsage.total_tokens,
                    LLMUsage.called_at,
                )
                .where(
                    and_(
                        LLMUsage.called_at >= from_dt,
                        LLMUsage.called_at <= to_dt,
                    )
                )
                .order_by(desc(LLMUsage.called_at), desc(LLMUsage.id))
                .limit(normalized_limit)
            ).all()

        return [
            {
                "id": r.id,
                "call_type": r.call_type,
                "model": r.model,
                "stock_code": r.stock_code,
                "prompt_tokens": r.prompt_tokens,
                "completion_tokens": r.completion_tokens,
                "total_tokens": r.total_tokens,
                "called_at": r.called_at,
            }
            for r in rows
        ]


# 便捷函数
def get_db() -> DatabaseManager:
    """获取数据库管理器实例的快捷方式"""
    return DatabaseManager.get_instance()


def persist_llm_usage(
    usage: Dict[str, Any],
    model: str,
    call_type: str,
    stock_code: Optional[str] = None,
) -> None:
    """Fire-and-forget: write one LLM call record to llm_usage. Never raises."""
    try:
        if usage is None:
            usage = {}
        prompt_cache_telemetry_disabled = bool(
            getattr(usage, _LLM_PROMPT_CACHE_TELEMETRY_DISABLED_ATTR, False)
        )
        prompt_tokens = _coerce_llm_usage_non_negative_int(usage.get("prompt_tokens")) or 0
        completion_tokens = _coerce_llm_usage_non_negative_int(usage.get("completion_tokens")) or 0
        total_tokens = _coerce_llm_usage_non_negative_int(usage.get("total_tokens")) or 0
        telemetry = {
            column: usage.get(column)
            for column in _LLM_USAGE_TELEMETRY_COLUMN_SQL
        }
        if prompt_cache_telemetry_disabled:
            for column in _LLM_PROMPT_CACHE_TELEMETRY_COLUMNS:
                telemetry[column] = None
        for column in _LLM_USAGE_INTEGER_TELEMETRY_COLUMNS:
            telemetry[column] = _coerce_llm_usage_non_negative_int(telemetry.get(column))
        telemetry["normalized_prompt_tokens"] = (
            telemetry.get("normalized_prompt_tokens")
            if telemetry.get("normalized_prompt_tokens") is not None
            else prompt_tokens
        )
        telemetry["normalized_completion_tokens"] = (
            telemetry.get("normalized_completion_tokens")
            if telemetry.get("normalized_completion_tokens") is not None
            else completion_tokens
        )
        telemetry["normalized_total_tokens"] = (
            telemetry.get("normalized_total_tokens")
            if telemetry.get("normalized_total_tokens") is not None
            else total_tokens
        )
        has_usage_payload = bool(usage.get("provider_usage_json")) or any(
            key in usage
            for key in (
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "normalized_prompt_tokens",
                "normalized_completion_tokens",
                "normalized_total_tokens",
            )
        )
        if not prompt_cache_telemetry_disabled:
            telemetry["cache_capability"] = usage.get("cache_capability") or "unknown"
            telemetry["cache_eligibility"] = usage.get("cache_eligibility") or "unknown"
            telemetry["cache_observation"] = usage.get("cache_observation") or (
                "no_usage" if not has_usage_payload else "unknown"
            )
        db = DatabaseManager.get_instance()
        db.record_llm_usage(
            call_type=call_type,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            stock_code=stock_code,
            **telemetry,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("[LLM usage] failed to persist usage record: %s", exc)


def _coerce_llm_usage_non_negative_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if value < 0 or not value.is_integer():
            return None
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text or not text.isdigit():
            return None
        return int(text)
    return None


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    db = get_db()
    
    print("=== 数据库测试 ===")
    print(f"数据库初始化成功")
    
    # 测试检查今日数据
    has_data = db.has_today_data('600519')
    print(f"茅台今日是否有数据: {has_data}")
    
    # 测试保存数据
    test_df = pd.DataFrame({
        'date': [date.today()],
        'open': [1800.0],
        'high': [1850.0],
        'low': [1780.0],
        'close': [1820.0],
        'volume': [10000000],
        'amount': [18200000000],
        'pct_chg': [1.5],
        'ma5': [1810.0],
        'ma10': [1800.0],
        'ma20': [1790.0],
        'volume_ratio': [1.2],
    })
    
    saved = db.save_daily_data(test_df, '600519', 'TestSource')
    print(f"保存测试数据: {saved} 条")
    
    # 测试获取上下文
    context = db.get_analysis_context('600519')
    print(f"分析上下文: {context}")
