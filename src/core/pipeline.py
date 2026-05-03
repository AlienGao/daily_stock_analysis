# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 核心分析流水线
===================================

职责：
1. 管理整个分析流程
2. 协调数据获取、存储、搜索、分析、通知等模块
3. 实现并发控制和异常处理
4. 提供股票分析的核心功能
"""

import logging
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, Callable

import pandas as pd

from src.config import get_config, Config
from src.storage import get_db, INTERACTIVE_ANALYSIS_QUERY_SOURCES
from data_provider import DataFetcherManager
from data_provider.base import normalize_stock_code
from data_provider.realtime_types import ChipDistribution
from src.analyzer import (
    GeminiAnalyzer,
    AnalysisResult,
    fill_chip_structure_if_needed,
    fill_price_position_if_needed,
    _sanitize_matched_skills,
)
from src.data.stock_mapping import STOCK_NAME_MAP
from src.notification import NotificationService, NotificationChannel
from src.report_language import (
    get_unknown_text,
    localize_confidence_level,
    normalize_report_language,
)
from src.search_service import SearchService
from src.services.social_sentiment_service import SocialSentimentService
from src.enums import ReportType
from src.stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult
from src.core.trading_calendar import (
    get_effective_trading_date,
    get_market_for_stock,
    get_market_now,
    is_market_open,
)
from data_provider.us_index_mapping import is_us_stock_code
from bot.models import BotMessage


logger = logging.getLogger(__name__)

# 防御性 guard：当实例绕过 __init__（如测试中 __new__）构造时，
# double-check 初始化 _single_stock_notify_lock 仍然线程安全。
_SINGLE_STOCK_NOTIFY_LOCK_INIT_GUARD = threading.Lock()


class StockAnalysisPipeline:
    """
    股票分析主流程调度器
    
    职责：
    1. 管理整个分析流程
    2. 协调数据获取、存储、搜索、分析、通知等模块
    3. 实现并发控制和异常处理
    """
    
    def __init__(
        self,
        config: Optional[Config] = None,
        max_workers: Optional[int] = None,
        source_message: Optional[BotMessage] = None,
        query_id: Optional[str] = None,
        query_source: Optional[str] = None,
        save_context_snapshot: Optional[bool] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ):
        """
        初始化调度器
        
        Args:
            config: 配置对象（可选，默认使用全局配置）
            max_workers: 最大并发线程数（可选，默认从配置读取）
        """
        self.config = config or get_config()
        self.max_workers = max_workers or self.config.max_workers
        self.source_message = source_message
        self.query_id = query_id
        self.query_source = self._resolve_query_source(query_source)
        self.save_context_snapshot = (
            self.config.save_context_snapshot if save_context_snapshot is None else save_context_snapshot
        )
        self.progress_callback = progress_callback
        
        # 初始化各模块
        self.db = get_db()
        self.fetcher_manager = DataFetcherManager()
        # 不再单独创建 akshare_fetcher，统一使用 fetcher_manager 获取增强数据
        self.trend_analyzer = StockTrendAnalyzer()  # 技术分析器
        self.analyzer = GeminiAnalyzer(config=self.config)
        self.notifier = NotificationService(source_message=source_message)
        self._single_stock_notify_lock = threading.Lock()
        
        # 初始化搜索服务（可选，初始化失败不应阻断主分析流程）
        try:
            self.search_service = SearchService(
                bocha_keys=self.config.bocha_api_keys,
                tavily_keys=self.config.tavily_api_keys,
                anspire_keys=self.config.anspire_api_keys,
                brave_keys=self.config.brave_api_keys,
                serpapi_keys=self.config.serpapi_keys,
                minimax_keys=self.config.minimax_api_keys,
                searxng_base_urls=self.config.searxng_base_urls,
                searxng_public_instances_enabled=self.config.searxng_public_instances_enabled,
                news_max_age_days=self.config.news_max_age_days,
                news_strategy_profile=getattr(self.config, "news_strategy_profile", "short"),
            )
        except Exception as exc:
            logger.warning("搜索服务初始化失败，将以无搜索模式运行: %s", exc, exc_info=True)
            self.search_service = None
        
        logger.info(f"调度器初始化完成，最大并发数: {self.max_workers}")
        logger.info("已启用技术分析引擎（均线/趋势/量价指标）")
        # 打印实时行情/筹码配置状态
        if self.config.enable_realtime_quote:
            logger.info(f"实时行情已启用 (优先级: {self.config.realtime_source_priority})")
        else:
            logger.info("实时行情已禁用，将使用历史收盘价")
        if self.config.enable_chip_distribution:
            logger.info("筹码分布分析已启用")
        else:
            logger.info("筹码分布分析已禁用")
        if self.search_service is None:
            logger.warning("搜索服务未启用（初始化失败或依赖缺失）")
        elif self.search_service.is_available:
            logger.info("搜索服务已启用")
        else:
            logger.warning("搜索服务未启用（未配置搜索能力）")

        # 初始化社交舆情服务（仅美股，可选）
        try:
            self.social_sentiment_service = SocialSentimentService(
                api_key=self.config.social_sentiment_api_key,
                api_url=self.config.social_sentiment_api_url,
            )
            if self.social_sentiment_service.is_available:
                logger.info("Social sentiment service enabled (Reddit/X/Polymarket, US stocks only)")
        except Exception as exc:
            logger.warning(
                "社交舆情服务初始化失败，将跳过舆情分析: %s",
                exc,
                exc_info=True,
            )
            self.social_sentiment_service = None

        # R&D factor signals cache: batch-level, one discovery run shared across all stocks
        self._factor_signals_cache: Dict[str, Dict[str, Any]] = {}

    def _emit_progress(self, progress: int, message: str) -> None:
        """Best-effort bridge from pipeline stages to task SSE progress."""
        callback = getattr(self, "progress_callback", None)
        if callback is None:
            return
        try:
            callback(progress, message)
        except Exception as exc:
            query_id = getattr(self, "query_id", None)
            logger.warning(
                "[pipeline] progress callback failed: %s (progress=%s, message=%r, query_id=%s)",
                exc,
                progress,
                message,
                query_id,
                extra={
                    "progress": progress,
                    "progress_message": message,
                    "query_id": query_id,
                },
            )

    def fetch_and_save_stock_data(
        self, 
        code: str,
        force_refresh: bool = False,
        current_time: Optional[datetime] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        获取并保存单只股票数据
        
        断点续传逻辑：
        1. 检查数据库是否已有最新可复用交易日数据
        2. 如果有且不强制刷新，则跳过网络请求
        3. 否则从数据源获取并保存
        
        Args:
            code: 股票代码
            force_refresh: 是否强制刷新（忽略本地缓存）
            current_time: 本轮运行冻结的参考时间，用于统一断点续传目标交易日判断
            
        Returns:
            Tuple[是否成功, 错误信息]
        """
        stock_name = code
        try:
            # 首先获取股票名称
            stock_name = self.fetcher_manager.get_stock_name(code, allow_realtime=False)

            target_date = self._resolve_resume_target_date(
                code, current_time=current_time
            )

            # 断点续传检查：如果最新可复用交易日的数据已存在，则跳过
            if not force_refresh and self.db.has_today_data(code, target_date):
                logger.info(
                    f"{stock_name}({code}) {target_date} 数据已存在，跳过获取（断点续传）"
                )
                return True, None

            # 从数据源获取数据
            logger.info(f"{stock_name}({code}) 开始从数据源获取数据...")
            df, source_name = self.fetcher_manager.get_daily_data(code, days=30)

            if df is None or df.empty:
                return False, "获取数据为空"

            # 保存到数据库
            saved_count = self.db.save_daily_data(df, code, source_name)
            logger.info(f"{stock_name}({code}) 数据保存成功（来源: {source_name}，新增 {saved_count} 条）")

            return True, None

        except Exception as e:
            error_msg = f"获取/保存数据失败: {str(e)}"
            logger.error(f"{stock_name}({code}) {error_msg}")
            return False, error_msg
    
    def analyze_stock(
        self,
        code: str,
        report_type: ReportType,
        query_id: str,
        *,
        agent_exec_config: Optional[Config] = None,
        force_agent: bool = False,
        replace_history: bool = False,
        persist_history: bool = True,
    ) -> Optional[AnalysisResult]:
        """
        分析单只股票（增强版：含量比、换手率、筹码分析、多维度情报）
        
        流程：
        1. 获取实时行情（量比、换手率）- 通过 DataFetcherManager 自动故障切换
        2. 获取筹码分布 - 通过 DataFetcherManager 带熔断保护
        3. 进行趋势分析（基于交易理念）
        4. 多维度情报搜索（最新消息+风险排查+业绩预期）
        5. 从数据库获取分析上下文
        6. 调用 AI 进行综合分析
        
        Args:
            query_id: 查询链路关联 id
            code: 股票代码
            report_type: 报告类型
            
        Returns:
            AnalysisResult 或 None（如果分析失败）
        """
        stock_name = code
        try:
            self._emit_progress(18, f"{code}：正在获取行情与筹码数据")
            # 获取股票名称（先走轻量名称路径，后续若 realtime_quote 有 name 再覆盖）
            stock_name = self.fetcher_manager.get_stock_name(code, allow_realtime=False)

            # Step 1: 获取实时行情（量比、换手率等）- 使用统一入口，自动故障切换
            realtime_quote = None
            try:
                if self.config.enable_realtime_quote:
                    realtime_quote = self.fetcher_manager.get_realtime_quote(code, log_final_failure=False)
                    if realtime_quote:
                        # 使用实时行情返回的真实股票名称
                        if realtime_quote.name:
                            stock_name = realtime_quote.name
                        # 兼容不同数据源的字段（有些数据源可能没有 volume_ratio）
                        volume_ratio = getattr(realtime_quote, 'volume_ratio', None)
                        turnover_rate = getattr(realtime_quote, 'turnover_rate', None)
                        logger.info(f"{stock_name}({code}) 实时行情: 价格={realtime_quote.price}, "
                                  f"量比={volume_ratio}, 换手率={turnover_rate}% "
                                  f"(来源: {realtime_quote.source.value if hasattr(realtime_quote, 'source') else 'unknown'})")
                    else:
                        logger.warning(f"{stock_name}({code}) 所有实时行情数据源均不可用，已降级为历史收盘价继续分析")
                else:
                    logger.info(f"{stock_name}({code}) 实时行情已禁用，使用历史收盘价继续分析")
            except Exception as e:
                logger.warning(f"{stock_name}({code}) 实时行情链路异常，已降级为历史收盘价继续分析: {e}")

            # 如果还是没有名称，使用代码作为名称
            if not stock_name:
                stock_name = f'股票{code}'

            # Step 2: 获取筹码分布 - 使用统一入口，带熔断保护
            chip_data = None
            try:
                chip_data = self.fetcher_manager.get_chip_distribution(code)
                if chip_data:
                    logger.info(f"{stock_name}({code}) 筹码分布: 获利比例={chip_data.profit_ratio:.1%}, "
                              f"90%集中度={chip_data.concentration_90:.2%}")
                else:
                    logger.debug(f"{stock_name}({code}) 筹码分布获取失败或已禁用")
            except Exception as e:
                logger.warning(f"{stock_name}({code}) 获取筹码分布失败: {e}")

            # Step 2b: 尝试分钟级筹码分布（需 Tushare stk_mins 单独开通权限，默认禁用水水水）
            # minute_chip_data = None
            # if getattr(self.config, 'enable_minute_chip_distribution', False):
            #     try:
            #         minute_chip_data = self.fetcher_manager.get_minute_chip_distribution(
            #             code,
            #             freq=getattr(self.config, 'tushare_minute_chip_freq', '5min'),
            #             days=getattr(self.config, 'tushare_minute_chip_days', 1),
            #         )
            #         if minute_chip_data:
            #             logger.info(f"{stock_name}({code}) 分钟筹码: 获利比例={minute_chip_data.profit_ratio:.1%}, "
            #                       f"90%集中度={minute_chip_data.concentration_90:.2%}")
            #     except Exception as e:
            #         logger.warning(f"{stock_name}({code}) 获取分钟筹码失败: {e}")

            # If agent mode is explicitly enabled, or specific agent skills are configured, use the Agent analysis pipeline.
            # NOTE: use config.agent_mode (explicit opt-in) instead of
            # config.is_agent_available() so that users who only configured an
            # API Key for the traditional analysis path are not silently
            # switched to Agent mode (which is slower and more expensive).
            use_agent = getattr(self.config, 'agent_mode', False)
            if not use_agent:
                # Auto-enable agent mode when specific skills are configured (e.g., scheduled task with strategy)
                configured_skills = getattr(self.config, 'agent_skills', [])
                if configured_skills and configured_skills != ['all']:
                    use_agent = True
                    logger.info(f"{stock_name}({code}) Auto-enabled agent mode due to configured skills: {configured_skills}")
            if force_agent or agent_exec_config is not None:
                use_agent = True

            self._emit_progress(32, f"{stock_name}：正在聚合基本面与趋势数据")

            # Step 2.5: 基本面能力聚合（统一入口，异常降级）
            # - 失败时返回 partial/failed，不影响既有技术面/新闻链路
            # - 关闭开关时仍返回 not_supported 结构
            fundamental_context = None
            try:
                fundamental_context = self.fetcher_manager.get_fundamental_context(
                    code,
                    budget_seconds=getattr(self.config, 'fundamental_stage_timeout_seconds', 1.5),
                )
            except Exception as e:
                logger.warning(f"{stock_name}({code}) 基本面聚合失败: {e}")
                fundamental_context = self.fetcher_manager.build_failed_fundamental_context(code, str(e))

            fundamental_context = self._attach_belong_boards_to_fundamental_context(
                code,
                fundamental_context,
            )

            # P0: write-only snapshot, fail-open, no read dependency on this table.
            try:
                self.db.save_fundamental_snapshot(
                    query_id=query_id,
                    code=code,
                    payload=fundamental_context,
                    source_chain=fundamental_context.get("source_chain", []),
                    coverage=fundamental_context.get("coverage", {}),
                )
            except Exception as e:
                logger.debug(f"{stock_name}({code}) 基本面快照写入失败: {e}")

            # Step 2.7: Tushare 技术指标优先获取（缓存优先，API 降级）
            # 在趋势分析之前获取，使 StockTrendAnalyzer 能使用前复权(qfq)数据
            tushare_fetcher = self.fetcher_manager._get_tushare_fetcher()
            tech_factors = None

            if tushare_fetcher is not None:
                tushare_code = normalize_stock_code(code)
                try:
                    trade_date_str = tushare_fetcher.get_trade_time(
                        early_time='00:00', late_time='18:00'
                    )
                    if trade_date_str:
                        from datetime import datetime as _dt
                        trade_date_obj = _dt.strptime(trade_date_str, '%Y%m%d').date()
                        # 优先从 DB 缓存读取
                        cached = self.db.get_tech_indicator(tushare_code, trade_date_obj)
                        if cached:
                            tech_factors = cached
                            logger.debug(
                                f"{stock_name}({code}) 技术指标命中 DB 缓存 "
                                f"(date={trade_date_obj})"
                            )
                        else:
                            # 缓存未命中，从 Tushare API 获取
                            tech_factors = tushare_fetcher.get_technical_factors(
                                tushare_code, trade_date_str
                            )
                            if tech_factors:
                                # 写入 DB 缓存
                                cache_df = pd.DataFrame([tech_factors])
                                self.db.upsert_tech_indicators(cache_df, tushare_code)
                                logger.debug(
                                    f"{stock_name}({code}) 技术指标已写入 DB 缓存"
                                )
                except Exception as e:
                    logger.debug(f"{stock_name}({code}) 技术指标缓存/获取失败: {e}")

            # Step 3: 趋势分析（基于交易理念）— 在 Agent 分支之前执行，供两条路径共用
            trend_result: Optional[TrendAnalysisResult] = None
            try:
                from src.services.history_loader import get_frozen_target_date
                _mkt = get_market_for_stock(normalize_stock_code(code))
                frozen = get_frozen_target_date()
                end_date = frozen if frozen else get_market_now(_mkt).date()
                start_date = end_date - timedelta(days=89)  # ~60 trading days for MA60
                historical_bars = self.db.get_data_range(code, start_date, end_date)
                if historical_bars:
                    df = pd.DataFrame([bar.to_dict() for bar in historical_bars])
                    # Issue #234: Augment with realtime for intraday MA calculation
                    if self.config.enable_realtime_quote and realtime_quote:
                        df = self._augment_historical_with_realtime(df, realtime_quote, code)
                    trend_result = self.trend_analyzer.analyze(
                        df, code, tech_indicators=tech_factors,
                    )
                    logger.info(f"{stock_name}({code}) 趋势分析: {trend_result.trend_status.value}, "
                              f"买入信号={trend_result.buy_signal.value}, 评分={trend_result.signal_score}"
                              f"{' (Tushare)' if tech_factors else ' (本地)'}")
            except Exception as e:
                logger.warning(f"{stock_name}({code}) 趋势分析失败: {e}", exc_info=True)

            # Step 3.5: Tushare 数据增强（资金流向/融资融券/筹码胜率）
            # 技术面因子已在 Step 2.7 获取，此处仅获取其他 Tushare 数据
            # 所有接口失败不阻塞主流程（降级容错）
            money_flow_data = None
            margin_data = None
            winner_data = None

            if tushare_fetcher is not None:
                try:
                    money_flow_data = tushare_fetcher.get_money_flow(tushare_code)
                except Exception as e:
                    logger.debug(f"{stock_name}({code}) 资金流向获取失败: {e}")
                try:
                    margin_data = tushare_fetcher.get_margin_detail(tushare_code)
                except Exception as e:
                    logger.debug(f"{stock_name}({code}) 融资融券获取失败: {e}")
                try:
                    winner_data = tushare_fetcher.get_cyq_winner(tushare_code)
                except Exception as e:
                    logger.debug(f"{stock_name}({code}) 筹码胜率获取失败: {e}")

            # Look up pre-computed discovery factor signals for this stock
            factor_signals = self._factor_signals_cache.get(code)

            if use_agent:
                logger.info(f"{stock_name}({code}) 启用 Agent 模式进行分析")
                self._emit_progress(58, f"{stock_name}：正在切换 Agent 分析链路")
                return self._analyze_with_agent(
                    code,
                    report_type,
                    query_id,
                    stock_name,
                    realtime_quote,
                    chip_data,
                    fundamental_context,
                    trend_result,
                    money_flow_data=money_flow_data,
                    margin_data=margin_data,
                    winner_data=winner_data,
                    tech_factors=tech_factors,
                    factor_signals=factor_signals,
                    agent_exec_config=agent_exec_config,
                    replace_history=replace_history,
                    persist_history=persist_history,
                )

            # Step 4: 多维度情报搜索（最新消息+风险排查+业绩预期）
            news_context = None
            self._emit_progress(46, f"{stock_name}：正在检索新闻与舆情")
            if self.search_service is not None and self.search_service.is_available:
                logger.info(f"{stock_name}({code}) 开始多维度情报搜索...")

                # 使用多维度搜索（最多5次搜索）
                intel_results = self.search_service.search_comprehensive_intel(
                    stock_code=code,
                    stock_name=stock_name,
                    max_searches=5
                )

                # 格式化情报报告
                if intel_results:
                    news_context = self.search_service.format_intel_report(intel_results, stock_name)
                    total_results = sum(
                        len(r.results) for r in intel_results.values() if r.success
                    )
                    logger.info(f"{stock_name}({code}) 情报搜索完成: 共 {total_results} 条结果")
                    logger.debug(f"{stock_name}({code}) 情报搜索结果:\n{news_context}")

                    # 保存新闻情报到数据库（用于后续复盘与查询）
                    try:
                        query_context = self._build_query_context(query_id=query_id)
                        for dim_name, response in intel_results.items():
                            if response and response.success and response.results:
                                self.db.save_news_intel(
                                    code=code,
                                    name=stock_name,
                                    dimension=dim_name,
                                    query=response.query,
                                    response=response,
                                    query_context=query_context
                                )
                    except Exception as e:
                        logger.warning(f"{stock_name}({code}) 保存新闻情报失败: {e}")
            else:
                logger.info(f"{stock_name}({code}) 搜索服务不可用，跳过情报搜索")

            # Step 4b: 补充 Tushare 结构化新闻（公告/快讯/研报/政策），需语料权限
            if getattr(self.config, 'enable_tushare_news', False):
                try:
                    tushare_news = self.fetcher_manager.get_tushare_news(
                        code, days=getattr(self.config, 'news_max_age_days', 7)
                    )
                    tushare_ann = self.fetcher_manager.get_tushare_announcements(code, days=30)
                    tushare_report = self.fetcher_manager.get_tushare_research_report(code, days=30)
                    tushare_policy = self.fetcher_manager.get_tushare_policy_news(
                        days=getattr(self.config, 'news_max_age_days', 7)
                    )
                    extras = [x for x in [tushare_news, tushare_ann, tushare_report, tushare_policy] if x]
                    if extras:
                        news_context = (news_context or "") + "\n\n" + "\n\n".join(extras)
                        logger.info(f"{stock_name}({code}) Tushare 语料已追加到新闻上下文")
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) 获取 Tushare 语料失败: {e}")

            # Step 4.5: Social sentiment intelligence (US stocks only)
            if self.social_sentiment_service is not None and self.social_sentiment_service.is_available and is_us_stock_code(code):
                try:
                    social_context = self.social_sentiment_service.get_social_context(code)
                    if social_context:
                        logger.info(f"{stock_name}({code}) Social sentiment data retrieved")
                        if news_context:
                            news_context = news_context + "\n\n" + social_context
                        else:
                            news_context = social_context
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) Social sentiment fetch failed: {e}")

            # Step 5: 获取分析上下文（技术面数据）
            self._emit_progress(58, f"{stock_name}：正在整理分析上下文")
            context = self.db.get_analysis_context(code)

            if context is None:
                logger.warning(f"{stock_name}({code}) 无法获取历史行情数据，将仅基于新闻和实时行情分析")
                _mkt_date = get_market_now(
                    get_market_for_stock(normalize_stock_code(code))
                ).date()
                context = {
                    'code': code,
                    'stock_name': stock_name,
                    'date': _mkt_date.isoformat(),
                    'data_missing': True,
                    'today': {},
                    'yesterday': {}
                }
            
            # Step 6: 增强上下文数据
            enhanced_context = self._enhance_context(
                context,
                realtime_quote,
                chip_data,
                trend_result,
                stock_name,
                fundamental_context,
                money_flow_data=money_flow_data,
                margin_data=margin_data,
                winner_data=winner_data,
                tech_factors=tech_factors,
                factor_signals=factor_signals,
            )
            
            # Step 7: 调用 AI 分析（传入增强的上下文和新闻）
            llm_progress_state = {"last_progress": 64}

            def _on_llm_stream(chars_received: int) -> None:
                dynamic_progress = min(92, 64 + min(chars_received // 80, 28))
                if dynamic_progress <= llm_progress_state["last_progress"]:
                    return
                llm_progress_state["last_progress"] = dynamic_progress
                self._emit_progress(
                    dynamic_progress,
                    f"{stock_name}：LLM 正在生成分析结果（已接收 {chars_received} 字符）",
                )

            self._emit_progress(64, f"{stock_name}：正在请求 LLM 生成报告")
            result = self.analyzer.analyze(
                enhanced_context,
                news_context=news_context,
                progress_callback=self._emit_progress,
                stream_progress_callback=_on_llm_stream,
            )

            # Step 7.5: 填充分析时的价格信息到 result
            if result:
                self._emit_progress(94, f"{stock_name}：正在校验并整理分析结果")
                result.query_id = query_id
                realtime_data = enhanced_context.get('realtime', {})
                result.current_price = realtime_data.get('price')
                result.change_pct = realtime_data.get('change_pct')

            # Step 7.6: chip_structure fallback (Issue #589)
            if result and chip_data:
                fill_chip_structure_if_needed(result, chip_data)

            # Step 7.7: price_position fallback
            if result:
                fill_price_position_if_needed(result, trend_result, realtime_quote)

            # Step 8: 保存分析历史记录
            if result and result.success:
                try:
                    self._emit_progress(97, f"{stock_name}：正在保存分析报告")
                    context_snapshot = self._build_context_snapshot(
                        enhanced_context=enhanced_context,
                        news_content=news_context,
                        realtime_quote=realtime_quote,
                        chip_data=chip_data
                    )
                    self._save_analysis_history_row(
                        result=result,
                        query_id=query_id,
                        report_type=report_type,
                        news_content=news_context,
                        context_snapshot=context_snapshot,
                        save_snapshot=self.save_context_snapshot,
                        replace_query_code=False,
                    )
                except Exception as e:
                    logger.warning(f"{stock_name}({code}) 保存分析历史失败: {e}")

            return result

        except Exception as e:
            logger.error(f"{stock_name}({code}) 分析失败: {e}")
            logger.exception(f"{stock_name}({code}) 详细错误信息:")
            return None
    
    def _enhance_context(
        self,
        context: Dict[str, Any],
        realtime_quote,
        chip_data: Optional[ChipDistribution],
        trend_result: Optional[TrendAnalysisResult],
        stock_name: str = "",
        fundamental_context: Optional[Dict[str, Any]] = None,
        money_flow_data: Optional[Dict[str, Any]] = None,
        margin_data: Optional[Dict[str, Any]] = None,
        winner_data: Optional[Dict[str, Any]] = None,
        tech_factors: Optional[Dict[str, Any]] = None,
        factor_signals: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        增强分析上下文

        将实时行情、筹码分布、趋势分析、Tushare 增强数据添加到上下文中
        """
        enhanced = context.copy()
        enhanced["report_language"] = normalize_report_language(getattr(self.config, "report_language", "zh"))
        
        # 添加股票名称
        if stock_name:
            enhanced['stock_name'] = stock_name
        elif realtime_quote and getattr(realtime_quote, 'name', None):
            enhanced['stock_name'] = realtime_quote.name

        # 将运行时搜索窗口透传给 analyzer，避免与全局配置重新读取产生窗口不一致
        enhanced['news_window_days'] = getattr(self.search_service, "news_window_days", 3)
        
        # 添加实时行情（兼容不同数据源的字段差异）
        if realtime_quote:
            # 使用 getattr 安全获取字段，缺失字段返回 None 或默认值
            volume_ratio = getattr(realtime_quote, 'volume_ratio', None)
            enhanced['realtime'] = {
                'name': getattr(realtime_quote, 'name', ''),
                'price': getattr(realtime_quote, 'price', None),
                'change_pct': getattr(realtime_quote, 'change_pct', None),
                'volume_ratio': volume_ratio,
                'volume_ratio_desc': self._describe_volume_ratio(volume_ratio) if volume_ratio else '无数据',
                'turnover_rate': getattr(realtime_quote, 'turnover_rate', None),
                'pe_ratio': getattr(realtime_quote, 'pe_ratio', None),
                'pb_ratio': getattr(realtime_quote, 'pb_ratio', None),
                'total_mv': getattr(realtime_quote, 'total_mv', None),
                'circ_mv': getattr(realtime_quote, 'circ_mv', None),
                'change_60d': getattr(realtime_quote, 'change_60d', None),
                'source': getattr(realtime_quote, 'source', None),
            }
            # 移除 None 值以减少上下文大小
            enhanced['realtime'] = {k: v for k, v in enhanced['realtime'].items() if v is not None}
        
        # 添加筹码分布
        if chip_data:
            current_price = getattr(realtime_quote, 'price', 0) if realtime_quote else 0
            chip_dict = {
                'profit_ratio': chip_data.profit_ratio,
                'avg_cost': chip_data.avg_cost,
                'concentration_90': chip_data.concentration_90,
                'concentration_70': chip_data.concentration_70,
                'chip_status': chip_data.get_chip_status(current_price or 0),
            }
            if getattr(chip_data, 'source', '') == 'tushare_minute':
                chip_dict['chip_source'] = 'minute'
            enhanced['chip'] = chip_dict
        
        # 添加趋势分析结果（Tushare stk_factor 优先，本地计算降级）
        if trend_result:
            tf = tech_factors if isinstance(tech_factors, dict) else {}
            # 均线：Tushare 优先
            ma5 = tf.get('ma_5') if tf.get('ma_5') is not None else trend_result.ma5
            ma10 = tf.get('ma_10') if tf.get('ma_10') is not None else trend_result.ma10
            ma20 = tf.get('ma_20') if tf.get('ma_20') is not None else trend_result.ma20
            ma60 = tf.get('ma_60')  # 本地没有 MA60

            # 乖离率：用最终选定的 MA 重算
            price = getattr(realtime_quote, 'price', 0) if realtime_quote else 0
            if price and ma5:
                bias_ma5 = round((price - ma5) / ma5 * 100, 2)
            else:
                bias_ma5 = trend_result.bias_ma5
            if price and ma10:
                bias_ma10 = round((price - ma10) / ma10 * 100, 2)
            else:
                bias_ma10 = trend_result.bias_ma10

            ta = {
                'trend_status': trend_result.trend_status.value,
                'ma_alignment': trend_result.ma_alignment,
                'trend_strength': trend_result.trend_strength,
                'bias_ma5': bias_ma5,
                'bias_ma10': bias_ma10,
                'volume_status': trend_result.volume_status.value,
                'volume_trend': trend_result.volume_trend,
                'buy_signal': trend_result.buy_signal.value,
                'signal_score': trend_result.signal_score,
                'signal_reasons': trend_result.signal_reasons,
                'risk_factors': trend_result.risk_factors,
                # MA 原始值
                'ma5': ma5,
                'ma10': ma10,
                'ma20': ma20,
                'ma60': ma60,
                # MACD：Tushare 优先，本地 fallback
                'macd_dif': tf.get('macd_dif') if tf.get('macd_dif') is not None else trend_result.macd_dif,
                'macd_dea': tf.get('macd_dea') if tf.get('macd_dea') is not None else trend_result.macd_dea,
                'macd_bar': tf.get('macd') if tf.get('macd') is not None else trend_result.macd_bar,
                # RSI：Tushare 优先，本地 fallback
                'rsi_6': tf.get('rsi_6') if tf.get('rsi_6') is not None else trend_result.rsi_6,
                'rsi_12': tf.get('rsi_12') if tf.get('rsi_12') is not None else trend_result.rsi_12,
                'rsi_24': tf.get('rsi_24'),  # 本地没有
                # KDJ：仅 Tushare 有
                'kdj_k': tf.get('kdj_k'),
                'kdj_d': tf.get('kdj_d'),
                'kdj_j': tf.get('kdj_j'),
                # BOLL：仅 Tushare 有
                'boll_upper': tf.get('boll_upper'),
                'boll_mid': tf.get('boll_mid'),
                'boll_lower': tf.get('boll_lower'),
                # 乖离率（Tushare 直接提供）
                'bias1': tf.get('bias1') if tf.get('bias1') is not None else bias_ma5,
                'bias2': tf.get('bias2') if tf.get('bias2') is not None else bias_ma10,
                'bias3': tf.get('bias3'),
                'tushare_factors_available': bool(tf),
            }
            # 清理 None 值
            enhanced['trend_analysis'] = {k: v for k, v in ta.items() if v is not None}

        # Issue #234: Override today with realtime OHLC + MA for intraday analysis
        # Use Tushare MAs when available, local as fallback
        if realtime_quote and trend_result:
            _ma5 = ma5 if (isinstance(tech_factors, dict) and tech_factors.get('ma_5') is not None) else trend_result.ma5
            _ma10 = ma10 if (isinstance(tech_factors, dict) and tech_factors.get('ma_10') is not None) else trend_result.ma10
            _ma20 = ma20 if (isinstance(tech_factors, dict) and tech_factors.get('ma_20') is not None) else trend_result.ma20
            if _ma5 and _ma5 > 0:
                price_val = getattr(realtime_quote, 'price', None)
                if price_val is not None and price_val > 0:
                    yesterday_close = None
                    if enhanced.get('yesterday') and isinstance(enhanced['yesterday'], dict):
                        yesterday_close = enhanced['yesterday'].get('close')
                    orig_today = enhanced.get('today') or {}
                    open_p = getattr(realtime_quote, 'open_price', None) or getattr(
                        realtime_quote, 'pre_close', None
                    ) or yesterday_close or orig_today.get('open') or price_val
                    high_p = getattr(realtime_quote, 'high', None) or price_val
                    low_p = getattr(realtime_quote, 'low', None) or price_val
                    vol = getattr(realtime_quote, 'volume', None)
                    amt = getattr(realtime_quote, 'amount', None)
                    pct = getattr(realtime_quote, 'change_pct', None)
                    realtime_today = {
                        'close': price_val,
                        'open': open_p,
                        'high': high_p,
                        'low': low_p,
                        'ma5': _ma5,
                        'ma10': _ma10,
                        'ma20': _ma20,
                    }
                    if vol is not None:
                        realtime_today['volume'] = vol
                    if amt is not None:
                        realtime_today['amount'] = amt
                    if pct is not None:
                        realtime_today['pct_chg'] = pct
                    for k, v in orig_today.items():
                        if k not in realtime_today and v is not None:
                            realtime_today[k] = v
                    enhanced['today'] = realtime_today
                    enhanced['ma_status'] = self._compute_ma_status(
                        price_val, _ma5, _ma10, _ma20
                    )
                    enhanced['date'] = get_market_now(
                        get_market_for_stock(normalize_stock_code(enhanced.get('code', '')))
                    ).date().isoformat()
                    if yesterday_close is not None:
                        try:
                            yc = float(yesterday_close)
                            if yc > 0:
                                enhanced['price_change_ratio'] = round(
                                    (price_val - yc) / yc * 100, 2
                                )
                        except (TypeError, ValueError):
                            pass
                    if vol is not None and enhanced.get('yesterday'):
                        yest_vol = enhanced['yesterday'].get('volume') if isinstance(
                            enhanced['yesterday'], dict
                        ) else None
                        if yest_vol is not None:
                            try:
                                yv = float(yest_vol)
                                if yv > 0:
                                    enhanced['volume_change_ratio'] = round(
                                        float(vol) / yv, 2
                                    )
                            except (TypeError, ValueError):
                                pass

        # ETF/index flag for analyzer prompt (Fixes #274)
        enhanced['is_index_etf'] = SearchService.is_index_or_etf(
            context.get('code', ''), enhanced.get('stock_name', stock_name)
        )

        # P0: append unified fundamental block; keep as additional context only
        enhanced["fundamental_context"] = (
            fundamental_context
            if isinstance(fundamental_context, dict)
            else self.fetcher_manager.build_failed_fundamental_context(
                context.get("code", ""),
                "invalid fundamental context",
            )
        )

        # Tushare 增强数据块（降级容错：缺失不阻塞）
        if money_flow_data and isinstance(money_flow_data, dict):
            enhanced['money_flow'] = {
                'net_mf_amount': money_flow_data.get('net_mf_amount'),
                'major_net_amount': money_flow_data.get('major_net_amount'),
                'retail_net_amount': money_flow_data.get('retail_net_amount'),
                'buy_elg_amount': money_flow_data.get('buy_elg_amount'),
                'sell_elg_amount': money_flow_data.get('sell_elg_amount'),
                'trade_date': money_flow_data.get('trade_date'),
            }

        if margin_data and isinstance(margin_data, dict):
            enhanced['margin_status'] = {
                'rzye': margin_data.get('rzye'),
                'rzmre': margin_data.get('rzmre'),
                'rzyeb': margin_data.get('rzyeb'),
                'rqye': margin_data.get('rqye'),
                'rqmre': margin_data.get('rqmre'),
                'trade_date': margin_data.get('trade_date'),
            }

        if winner_data and isinstance(winner_data, dict):
            enhanced['winner_profile'] = {
                'winner_rate': winner_data.get('winner_rate'),
                'cost_avg': winner_data.get('cost_avg'),
                'cost_5pct': winner_data.get('cost_5pct'),
                'cost_95pct': winner_data.get('cost_95pct'),
                'concentration': winner_data.get('concentration'),
                'trade_date': winner_data.get('trade_date'),
            }

        # Discovery engine factor signals (R&D loop generated factors included)
        if factor_signals:
            enhanced['discovery_signals'] = factor_signals

        return enhanced

    def _attach_belong_boards_to_fundamental_context(
        self,
        code: str,
        fundamental_context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Attach A-share board membership as a top-level supplemental field.

        Keep this as a shallow copy so cached fundamental contexts are not
        mutated in place after retrieval.
        """
        if isinstance(fundamental_context, dict):
            enriched_context = dict(fundamental_context)
        else:
            enriched_context = self.fetcher_manager.build_failed_fundamental_context(
                code,
                "invalid fundamental context",
            )

        existing_boards = enriched_context.get("belong_boards")
        if isinstance(existing_boards, list):
            enriched_context["belong_boards"] = list(existing_boards)
            return enriched_context

        boards_block = enriched_context.get("boards")
        boards_status = boards_block.get("status") if isinstance(boards_block, dict) else None
        coverage = enriched_context.get("coverage")
        boards_coverage = coverage.get("boards") if isinstance(coverage, dict) else None
        market = enriched_context.get("market")
        if not isinstance(market, str) or not market.strip():
            market = get_market_for_stock(normalize_stock_code(code))

        if (
            market != "cn"
            or boards_status == "not_supported"
            or boards_coverage == "not_supported"
        ):
            enriched_context["belong_boards"] = []
            return enriched_context

        boards: List[Dict[str, Any]] = []
        try:
            raw_boards = self.fetcher_manager.get_belong_boards(code)
            if isinstance(raw_boards, list):
                boards = raw_boards
        except Exception as e:
            logger.debug("%s attach belong_boards failed (fail-open): %s", code, e)

        enriched_context["belong_boards"] = boards
        return enriched_context

    def _ensure_agent_history(self, code: str, min_days: int = 240) -> None:
        """Ensure at least *min_days* of K-line history is in DB for agent tools."""
        from src.services.history_loader import get_frozen_target_date

        target = get_frozen_target_date()
        if target is None:
            target = self._resolve_resume_target_date(code)
        start = target - timedelta(days=int(min_days * 1.8))
        bars = self.db.get_data_range(code, start, target)
        if bars and len(bars) >= min(min_days, 200):
            logger.debug("[%s] Agent history: %d bars in DB, sufficient", code, len(bars))
            return
        try:
            df, source = self.fetcher_manager.get_daily_data(code, days=min_days)
            if df is not None and not df.empty:
                self.db.save_daily_data(df, code, source)
                logger.info("[%s] Prefetched %d rows of history for agent (source: %s)", code, len(df), source)
        except Exception as e:
            logger.warning("[%s] Agent history prefetch failed: %s", code, e)

    def _analyze_with_agent(
        self,
        code: str,
        report_type: ReportType,
        query_id: str,
        stock_name: str,
        realtime_quote: Any,
        chip_data: Optional[ChipDistribution],
        fundamental_context: Optional[Dict[str, Any]] = None,
        trend_result: Optional[TrendAnalysisResult] = None,
        money_flow_data: Optional[Dict[str, Any]] = None,
        margin_data: Optional[Dict[str, Any]] = None,
        winner_data: Optional[Dict[str, Any]] = None,
        tech_factors: Optional[Dict[str, Any]] = None,
        factor_signals: Optional[Dict[str, Any]] = None,
        agent_exec_config: Optional[Config] = None,
        replace_history: bool = False,
        persist_history: bool = True,
    ) -> Optional[AnalysisResult]:
        """
        使用 Agent 模式分析单只股票。
        """
        try:
            from src.agent.factory import build_agent_executor
            report_language = normalize_report_language(getattr(self.config, "report_language", "zh"))

            # Build executor from shared factory (ToolRegistry and SkillManager prototype are cached)
            _cfg = agent_exec_config or self.config
            executor = build_agent_executor(_cfg, getattr(self.config, 'agent_skills', None) or None)

            # Build initial context to avoid redundant tool calls
            initial_context = {
                "stock_code": code,
                "stock_name": stock_name,
                "report_type": report_type.value,
                "report_language": report_language,
                "fundamental_context": fundamental_context,
            }
            
            if realtime_quote:
                initial_context["realtime_quote"] = self._safe_to_dict(realtime_quote)
            if chip_data:
                initial_context["chip_distribution"] = self._safe_to_dict(chip_data)
            if trend_result:
                initial_context["trend_result"] = self._safe_to_dict(trend_result)
            if money_flow_data:
                initial_context["money_flow_data"] = money_flow_data
            if margin_data:
                initial_context["margin_data"] = margin_data
            if winner_data:
                initial_context["winner_data"] = winner_data
            if tech_factors:
                initial_context["tech_factors"] = tech_factors
            if factor_signals:
                initial_context["discovery_signals"] = factor_signals

            # Agent path: inject social sentiment as news_context so both
            # executor (_build_user_message) and orchestrator (ctx.set_data)
            # can consume it through the existing news_context channel
            if self.social_sentiment_service is not None and self.social_sentiment_service.is_available and is_us_stock_code(code):
                try:
                    social_context = self.social_sentiment_service.get_social_context(code)
                    if social_context:
                        existing = initial_context.get("news_context")
                        if existing:
                            initial_context["news_context"] = existing + "\n\n" + social_context
                        else:
                            initial_context["news_context"] = social_context
                        logger.info(f"[{code}] Agent mode: social sentiment data injected into news_context")
                except Exception as e:
                    logger.warning(f"[{code}] Agent mode: social sentiment fetch failed: {e}")

            # Issue #1066: ensure deep history is in DB before agent tools run
            self._ensure_agent_history(code)

            # 运行 Agent
            if report_language == "en":
                message = f"Analyze stock {code} ({stock_name}) and return the full decision dashboard JSON in English."
            else:
                message = f"请分析股票 {code} ({stock_name})，并生成决策仪表盘报告。"
            agent_result = executor.run(message, context=initial_context)

            # 转换为 AnalysisResult
            result = self._agent_result_to_analysis_result(
                agent_result,
                code,
                stock_name,
                report_type,
                query_id,
                trend_result=trend_result,
            )
            if result:
                result.query_id = query_id
            # Agent weak integrity: placeholder fill only, no LLM retry
            if result and getattr(self.config, "report_integrity_enabled", False):
                from src.analyzer import check_content_integrity, apply_placeholder_fill

                pass_integrity, missing = check_content_integrity(result)
                if not pass_integrity:
                    apply_placeholder_fill(result, missing)
                    logger.info(
                        "[LLM完整性] integrity_mode=agent_weak 必填字段缺失 %s，已占位补全",
                        missing,
                    )
            # chip_structure fallback (Issue #589), before save_analysis_history
            if result and chip_data:
                fill_chip_structure_if_needed(result, chip_data)

            # price_position fallback (same as non-agent path Step 7.7)
            if result:
                fill_price_position_if_needed(result, trend_result, realtime_quote)

            resolved_stock_name = result.name if result and result.name else stock_name

            # 保存新闻情报到数据库（Agent 工具结果仅用于 LLM 上下文，未持久化，Fixes #396）
            # 使用 search_stock_news（与 Agent 工具调用逻辑一致），仅 1 次 API 调用，无额外延迟
            if self.search_service is not None and self.search_service.is_available:
                try:
                    news_response = self.search_service.search_stock_news(
                        stock_code=code,
                        stock_name=resolved_stock_name,
                        max_results=5
                    )
                    if news_response.success and news_response.results:
                        query_context = self._build_query_context(query_id=query_id)
                        self.db.save_news_intel(
                            code=code,
                            name=resolved_stock_name,
                            dimension="latest_news",
                            query=news_response.query,
                            response=news_response,
                            query_context=query_context
                        )
                        logger.info(f"[{code}] Agent 模式: 新闻情报已保存 {len(news_response.results)} 条")
                except Exception as e:
                    logger.warning(f"[{code}] Agent 模式保存新闻情报失败: {e}")

            # 保存分析历史记录
            if result and result.success and persist_history:
                try:
                    initial_context["stock_name"] = resolved_stock_name
                    self._save_analysis_history_row(
                        result=result,
                        query_id=query_id,
                        report_type=report_type,
                        news_content=None,
                        context_snapshot=initial_context,
                        save_snapshot=self.save_context_snapshot,
                        replace_query_code=replace_history,
                    )
                except Exception as e:
                    logger.warning(f"[{code}] 保存 Agent 分析历史失败: {e}")

            return result

        except Exception as e:
            logger.error(f"[{code}] Agent 分析失败: {e}")
            logger.exception(f"[{code}] Agent 详细错误信息:")
            return None

    def _agent_result_to_analysis_result(
        self,
        agent_result,
        code: str,
        stock_name: str,
        report_type: ReportType,
        query_id: str,
        trend_result: Optional[TrendAnalysisResult] = None,
    ) -> AnalysisResult:
        """
        将 AgentResult 转换为 AnalysisResult。
        """
        report_language = normalize_report_language(getattr(self.config, "report_language", "zh"))
        result = AnalysisResult(
            code=code,
            name=stock_name,
            sentiment_score=50,
            trend_prediction="Unknown" if report_language == "en" else "未知",
            operation_advice="Watch" if report_language == "en" else "观望",
            confidence_level=localize_confidence_level("medium", report_language),
            report_language=report_language,
            success=agent_result.success,
            error_message=agent_result.error or None,
            data_sources=f"agent:{agent_result.provider}",
            model_used=agent_result.model or None,
        )

        if agent_result.success and agent_result.dashboard:
            dash = agent_result.dashboard
            ai_stock_name = str(dash.get("stock_name", "")).strip()
            if ai_stock_name and self._is_placeholder_stock_name(stock_name, code):
                result.name = ai_stock_name
            result.sentiment_score = self._safe_int(dash.get("sentiment_score"), 50)
            result.trend_prediction = dash.get("trend_prediction", "Unknown" if report_language == "en" else "未知")
            raw_advice = dash.get("operation_advice", "Watch" if report_language == "en" else "观望")
            if isinstance(raw_advice, dict):
                # LLM may return {"no_position": "...", "has_position": "..."}
                # Derive a short string from decision_type for the scalar field
                _signal_to_advice = {
                    "buy": "Buy" if report_language == "en" else "买入",
                    "sell": "Sell" if report_language == "en" else "卖出",
                    "hold": "Hold" if report_language == "en" else "持有",
                    "strong_buy": "Strong Buy" if report_language == "en" else "强烈买入",
                    "strong_sell": "Strong Sell" if report_language == "en" else "强烈卖出",
                }
                # Normalize decision_type (strip/lower) before lookup so
                # variants like "BUY" or " Buy " map correctly.
                raw_dt = str(dash.get("decision_type") or "hold").strip().lower()
                result.operation_advice = _signal_to_advice.get(raw_dt, "Watch" if report_language == "en" else "观望")
            else:
                result.operation_advice = str(raw_advice) if raw_advice else ("Watch" if report_language == "en" else "观望")
            from src.agent.protocols import normalize_decision_signal

            result.decision_type = normalize_decision_signal(
                dash.get("decision_type", "hold")
            )
            result.confidence_level = localize_confidence_level(
                dash.get("confidence_level", result.confidence_level),
                report_language,
            )
            result.analysis_summary = dash.get("analysis_summary", "")
            # Capture matched trading skills (from AGENT_SKILLS hit list).
            result.matched_skills = _sanitize_matched_skills(dash.get("matched_skills"))
            # The AI returns a top-level dict that contains a nested 'dashboard' sub-key
            # with core_conclusion / battle_plan / intelligence.  AnalysisResult's helper
            # methods (get_sniper_points, get_core_conclusion, etc.) expect that inner
            # structure, so we unwrap it here.
            result.dashboard = dash.get("dashboard") or dash
        else:
            self._apply_trend_fallback(result, trend_result, report_language)
            if not result.error_message:
                result.error_message = "Agent failed to generate a valid decision dashboard" if report_language == "en" else "Agent 未能生成有效的决策仪表盘"

        return result

    @staticmethod
    def _apply_trend_fallback(
        result: AnalysisResult,
        trend_result: Optional[TrendAnalysisResult],
        report_language: str,
    ) -> None:
        if trend_result is None:
            result.sentiment_score = 50
            result.operation_advice = "Watch" if report_language == "en" else "观望"
            return

        score = getattr(trend_result, "signal_score", None)
        try:
            numeric_score = int(score)
        except (TypeError, ValueError):
            numeric_score = 50
        result.sentiment_score = numeric_score if numeric_score > 0 else 50

        trend_status = getattr(trend_result, "trend_status", None)
        trend_label = getattr(trend_status, "value", None) or str(trend_status or "").strip()
        if trend_label:
            result.trend_prediction = trend_label

        buy_signal = getattr(trend_result, "buy_signal", None)
        signal_label = getattr(buy_signal, "value", None) or str(buy_signal or "").strip()
        if signal_label:
            result.operation_advice = signal_label
        else:
            result.operation_advice = "Watch" if report_language == "en" else "观望"

        from src.agent.protocols import normalize_decision_signal

        signal_name = getattr(buy_signal, "name", "").lower()
        signal_to_decision = {
            "strong_buy": "buy",
            "buy": "buy",
            "hold": "hold",
            "wait": "hold",
            "sell": "sell",
            "strong_sell": "sell",
        }
        result.decision_type = signal_to_decision.get(signal_name, result.decision_type or "hold")
        result.decision_type = normalize_decision_signal(result.decision_type)
        result.data_sources = f"{result.data_sources},trend:fallback" if result.data_sources else "trend:fallback"

    @staticmethod
    def _is_placeholder_stock_name(name: str, code: str) -> bool:
        """Return True when the stock name is missing or placeholder-like."""
        if not name:
            return True
        normalized = str(name).strip()
        if not normalized:
            return True
        if normalized == code:
            return True
        if normalized.startswith("股票"):
            return True
        if "Unknown" in normalized:
            return True
        return False

    @staticmethod
    def _safe_int(value: Any, default: int = 50) -> int:
        """安全地将值转换为整数。"""
        if value is None:
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            import re
            match = re.search(r'-?\d+', value)
            if match:
                return int(match.group())
        return default
    
    def _describe_volume_ratio(self, volume_ratio: float) -> str:
        """
        量比描述
        
        量比 = 当前成交量 / 过去5日平均成交量
        """
        if volume_ratio < 0.5:
            return "极度萎缩"
        elif volume_ratio < 0.8:
            return "明显萎缩"
        elif volume_ratio < 1.2:
            return "正常"
        elif volume_ratio < 2.0:
            return "温和放量"
        elif volume_ratio < 3.0:
            return "明显放量"
        else:
            return "巨量"

    @staticmethod
    def _compute_ma_status(close: float, ma5: float, ma10: float, ma20: float) -> str:
        """
        Compute MA alignment status from price and MA values.
        Logic mirrors storage._analyze_ma_status (Issue #234).
        """
        close = close or 0
        ma5 = ma5 or 0
        ma10 = ma10 or 0
        ma20 = ma20 or 0
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

    def _augment_historical_with_realtime(
        self, df: pd.DataFrame, realtime_quote: Any, code: str
    ) -> pd.DataFrame:
        """
        Augment historical OHLCV with today's realtime quote for intraday MA calculation.
        Issue #234: Use realtime price instead of yesterday's close for technical indicators.
        """
        if df is None or df.empty or 'close' not in df.columns:
            return df
        if realtime_quote is None:
            return df
        price = getattr(realtime_quote, 'price', None)
        if price is None or not (isinstance(price, (int, float)) and price > 0):
            return df

        # Optional: skip augmentation on non-trading days (fail-open)
        enable_realtime_tech = getattr(
            self.config, 'enable_realtime_technical_indicators', True
        )
        if not enable_realtime_tech:
            return df
        market = get_market_for_stock(code)
        market_today = get_market_now(market).date()
        if market and not is_market_open(market, market_today):
            return df

        last_val = df['date'].max()
        last_date = (
            last_val.date() if hasattr(last_val, 'date') else
            (last_val if isinstance(last_val, date) else pd.Timestamp(last_val).date())
        )
        yesterday_close = float(df.iloc[-1]['close']) if len(df) > 0 else price
        open_p = getattr(realtime_quote, 'open_price', None) or getattr(
            realtime_quote, 'pre_close', None
        ) or yesterday_close
        high_p = getattr(realtime_quote, 'high', None) or price
        low_p = getattr(realtime_quote, 'low', None) or price
        vol = getattr(realtime_quote, 'volume', None) or 0
        amt = getattr(realtime_quote, 'amount', None)
        pct = getattr(realtime_quote, 'change_pct', None)

        if last_date >= market_today:
            # Update last row with realtime close (copy to avoid mutating caller's df)
            df = df.copy()
            idx = df.index[-1]
            df.loc[idx, 'close'] = price
            if open_p is not None:
                df.loc[idx, 'open'] = open_p
            if high_p is not None:
                df.loc[idx, 'high'] = high_p
            if low_p is not None:
                df.loc[idx, 'low'] = low_p
            if vol:
                df.loc[idx, 'volume'] = vol
            if amt is not None:
                df.loc[idx, 'amount'] = amt
            if pct is not None:
                df.loc[idx, 'pct_chg'] = pct
        else:
            # Append virtual today row
            new_row = {
                'code': code,
                'date': market_today,
                'open': open_p,
                'high': high_p,
                'low': low_p,
                'close': price,
                'volume': vol,
                'amount': amt if amt is not None else 0,
                'pct_chg': pct if pct is not None else 0,
            }
            new_df = pd.DataFrame([new_row])
            df = pd.concat([df, new_df], ignore_index=True)
        return df

    def _build_context_snapshot(
        self,
        enhanced_context: Dict[str, Any],
        news_content: Optional[str],
        realtime_quote: Any,
        chip_data: Optional[ChipDistribution]
    ) -> Dict[str, Any]:
        """
        构建分析上下文快照
        """
        return {
            "enhanced_context": enhanced_context,
            "news_content": news_content,
            "realtime_quote_raw": self._safe_to_dict(realtime_quote),
            "chip_distribution_raw": self._safe_to_dict(chip_data),
        }

    @staticmethod
    def _resolve_resume_target_date(
        code: str, current_time: Optional[datetime] = None
    ) -> date:
        """
        Resolve the trading date used by checkpoint/resume checks.
        """
        market = get_market_for_stock(normalize_stock_code(code))
        return get_effective_trading_date(market, current_time=current_time)

    @staticmethod
    def _safe_to_dict(value: Any) -> Optional[Dict[str, Any]]:
        """
        安全转换为字典
        """
        if value is None:
            return None
        if hasattr(value, "to_dict"):
            try:
                return value.to_dict()
            except Exception:
                return None
        if hasattr(value, "__dict__"):
            try:
                return dict(value.__dict__)
            except Exception:
                return None
        return None

    def _resolve_query_source(self, query_source: Optional[str]) -> str:
        """
        解析请求来源。

        优先级（从高到低）：
        1. 显式传入的 query_source：调用方明确指定时优先使用，便于覆盖推断结果或兼容未来 source_message 来自非 bot 的场景
        2. 存在 source_message 时推断为 "bot"：当前约定为机器人会话上下文
        3. 存在 query_id 时推断为 "web"：Web 触发的请求会带上 query_id
        4. 默认 "system"：定时任务或 CLI 等无上述上下文时

        Args:
            query_source: 调用方显式指定的来源，如 "bot" / "web" / "cli" / "system"

        Returns:
            归一化后的来源标识字符串，如 "bot" / "web" / "cli" / "system"
        """
        if query_source:
            return query_source
        if self.source_message:
            return "bot"
        if self.query_id:
            return "web"
        return "system"

    def _build_query_context(self, query_id: Optional[str] = None) -> Dict[str, str]:
        """
        生成用户查询关联信息
        """
        effective_query_id = query_id or self.query_id or ""

        context: Dict[str, str] = {
            "query_id": effective_query_id,
            "query_source": self.query_source or "",
        }

        if self.source_message:
            context.update({
                "requester_platform": self.source_message.platform or "",
                "requester_user_id": self.source_message.user_id or "",
                "requester_user_name": self.source_message.user_name or "",
                "requester_chat_id": self.source_message.chat_id or "",
                "requester_message_id": self.source_message.message_id or "",
                "requester_query": self.source_message.content or "",
            })

        return context

    def _save_analysis_history_row(
        self,
        result: AnalysisResult,
        query_id: str,
        report_type: ReportType,
        news_content: Optional[str],
        context_snapshot: Optional[Dict[str, Any]],
        *,
        save_snapshot: bool,
        replace_query_code: bool = False,
    ) -> None:
        """写入 analysis_history；交互式 api/web 先删同日旧行再插入，并可选落盘 Markdown。"""
        if replace_query_code:
            deleted = self.db.delete_analysis_history_by_query_and_code(query_id, result.code)
            if deleted:
                logger.info(
                    "[%s] Top-N multi 覆盖：已删除旧分析历史 query_id=%s",
                    result.code,
                    query_id,
                )
        if self.query_source in INTERACTIVE_ANALYSIS_QUERY_SOURCES:
            n = self.db.delete_interactive_analysis_history_for_code_same_shanghai_day(result.code)
            if n:
                logger.info(
                    "[%s] 交互式分析：已删除同日旧记录 %s 条",
                    result.code,
                    n,
                )
        self.db.save_analysis_history(
            result=result,
            query_id=query_id,
            report_type=report_type.value,
            news_content=news_content,
            context_snapshot=context_snapshot,
            save_snapshot=save_snapshot,
            query_source=self.query_source,
        )
        if self.query_source in INTERACTIVE_ANALYSIS_QUERY_SOURCES:
            from src.home_report_file import write_home_interactive_analysis_markdown

            write_home_interactive_analysis_markdown(result)
    
    def process_single_stock(
        self,
        code: str,
        skip_analysis: bool = False,
        single_stock_notify: bool = False,
        report_type: ReportType = ReportType.SIMPLE,
        analysis_query_id: Optional[str] = None,
        current_time: Optional[datetime] = None,
    ) -> Optional[AnalysisResult]:
        """
        处理单只股票的完整流程

        包括：
        1. 获取数据
        2. 保存数据
        3. AI 分析
        4. 单股推送（可选，#55）

        此方法会被线程池调用，需要处理好异常

        Args:
            analysis_query_id: 查询链路关联 id
            code: 股票代码
            skip_analysis: 是否跳过 AI 分析
            single_stock_notify: 是否启用单股推送模式（每分析完一只立即推送）
            report_type: 报告类型枚举（从配置读取，Issue #119）
            current_time: 本轮运行冻结的参考时间，用于统一断点续传目标交易日判断

        Returns:
            AnalysisResult 或 None
        """
        logger.info(f"========== 开始处理 {code} ==========")

        from src.services.history_loader import set_frozen_target_date, reset_frozen_target_date
        frozen_td = self._resolve_resume_target_date(code, current_time=current_time)
        token = set_frozen_target_date(frozen_td)
        try:
            self._emit_progress(12, f"{code}：正在准备分析任务")
            # Step 1: 获取并保存数据
            success, error = self.fetch_and_save_stock_data(
                code, current_time=current_time
            )
            
            if not success:
                logger.warning(f"[{code}] 数据获取失败: {error}")
                # 即使获取失败，也尝试用已有数据分析
            else:
                self._emit_progress(16, f"{code}：行情数据准备完成")
            
            # Step 2: AI 分析
            if skip_analysis:
                logger.info(f"[{code}] 跳过 AI 分析（dry-run 模式）")
                return None
            
            effective_query_id = analysis_query_id or self.query_id or uuid.uuid4().hex
            result = self.analyze_stock(code, report_type, query_id=effective_query_id)
            
            if result and result.success:
                logger.info(
                    f"[{code}] 分析完成: {result.operation_advice}, "
                    f"评分 {result.sentiment_score}"
                )
                
                # 单股推送模式（#55）：每分析完一只股票立即推送
                if single_stock_notify:
                    self._send_single_stock_notification(
                        result,
                        report_type=report_type,
                        fallback_code=code,
                    )
            elif result:
                logger.warning(
                    f"[{code}] 分析未成功: {result.error_message or '未知错误'}"
                )
            
            return result
            
        except Exception as e:
            # 捕获所有异常，确保单股失败不影响整体
            logger.exception(f"[{code}] 处理过程发生未知异常: {e}")
            return None
        finally:
            reset_frozen_target_date(token)
    
    def run(
        self,
        stock_codes: Optional[List[str]] = None,
        dry_run: bool = False,
        send_notification: bool = True,
        merge_notification: bool = False,
        defer_aggregate_report: bool = False,
    ) -> List[AnalysisResult]:
        """
        运行完整的分析流程（定时任务 / 立即分析的主入口）。

        Wraps the underlying implementation in ``batch_analysis_scope()`` so
        that every LLM call spawned from this run is tagged as "batch mode".
        ``llm_adapter.get_thinking_extra_body`` uses that tag to skip the
        DeepSeek thinking payload by default (可通过 ``DEEPSEEK_BATCH_THINKING_ENABLED``
        环境变量打开).  Agent 对话等非 pipeline 路径不经过这里，thinking 维持开启。

        流程：
        1. 获取待分析的股票列表
        2. 使用线程池并发处理
        3. 收集分析结果
        4. 发送通知

        Args:
            stock_codes: 股票代码列表（可选，默认使用配置中的自选股）
            dry_run: 是否仅获取数据不分析
            send_notification: 是否发送推送通知
            merge_notification: 是否合并推送（跳过本次推送，由 main 层合并个股+大盘后统一发送，Issue #190）
            defer_aggregate_report: 为 True 时不写 ``report_*.md``、不发汇总类通知；由 main 在 Top-N multi 合并后再调用
                ``_save_local_report`` / ``_send_notifications``。

        Returns:
            分析结果列表
        """
        import os

        from src.agent.llm_adapter import batch_analysis_scope

        batch_thinking_env = (os.getenv("DEEPSEEK_BATCH_THINKING_ENABLED") or "").strip().lower()
        thinking_on = batch_thinking_env in {"1", "true", "yes", "on"}
        logger.info(
            "[batch-mode] pipeline.run() entering batch scope → deepseek-chat "
            "thinking=%s (DEEPSEEK_BATCH_THINKING_ENABLED=%r)",
            "enabled" if thinking_on else "disabled",
            batch_thinking_env or "<unset>",
        )

        with batch_analysis_scope():
            return self._run_impl(
                stock_codes=stock_codes,
                dry_run=dry_run,
                send_notification=send_notification,
                merge_notification=merge_notification,
                defer_aggregate_report=defer_aggregate_report,
            )

    def _prepare_factor_signals_cache(
        self, stock_codes: List[str], tushare_fetcher, akshare_fetcher=None
    ) -> None:
        """Run discovery engine once at batch level, cache per-stock factor scores.

        This avoids repeated full-market API calls per stock during concurrent analysis.
        All registered factors (including rd_gen_* from R&D loop) participate.
        """
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

        discovery_config = get_discovery_config()
        engine = StockDiscoveryEngine(discovery_config, tushare_fetcher, akshare_fetcher)
        # Register all postmarket factors (same as auto-discovery)
        engine.register_factors([
            MoneyFlowFactor(),
            MarginFactor(),
            ChipFactor(),
            TechnicalFactor(),
            LimitFactor(),
            FundamentalFactor(),
            PopularityFactor(),
            HotMoneyFactor(),
            NorthboundFactor(),
            InstitutionHoldFactor(),
            ProfitForecastFactor(),
            PerformanceFactor(),
            BuybackFactor(),
            InsiderBuyFactor(),
            BrokerRecommendFactor(),
        ])

        results = engine.discover(mode="postmarket")
        if not results:
            logger.info("[FactorSignals] 未发现候选股，因子信号缓存为空")
            return

        # Build per-stock cache from discovery results
        for r in results:
            signals = {
                "score": r.score,
                "factor_scores": dict(r.factor_scores),
                "reasons": list(r.reasons),
                "buy_price_low": getattr(r, "buy_price_low", None),
                "buy_price_high": getattr(r, "buy_price_high", None),
                "stop_loss": getattr(r, "stop_loss", None),
                "take_profit_1": getattr(r, "take_profit_1", None),
                "take_profit_2": getattr(r, "take_profit_2", None),
            }
            self._factor_signals_cache[r.stock_code] = signals

        logger.info(
            "[FactorSignals] 因子信号缓存已构建: %d 只股票",
            len(self._factor_signals_cache),
        )

    def _run_impl(
        self,
        stock_codes: Optional[List[str]] = None,
        dry_run: bool = False,
        send_notification: bool = True,
        merge_notification: bool = False,
        defer_aggregate_report: bool = False,
    ) -> List[AnalysisResult]:
        """Actual pipeline body. Do not call directly — use ``run()`` so the
        batch-mode thinking switch is correctly scoped."""
        import os

        start_time = time.time()

        # 使用配置中的股票列表
        if stock_codes is None:
            self.config.refresh_stock_list()
            stock_codes = self.config.stock_list

        # Factor signals: batch-level discovery run to inject per-stock factor scores into analysis
        self._factor_signals_cache = {}
        if (
            not dry_run
            and os.getenv("DISCOVERY_FACTOR_SIGNALS_ENABLED", "true").strip().lower() in ("true", "1", "yes", "on")
        ):
            tushare_fetcher = self.fetcher_manager._get_tushare_fetcher()
            akshare_fetcher = self.fetcher_manager._get_akshare_fetcher()
            if tushare_fetcher and tushare_fetcher.is_available():
                try:
                    self._prepare_factor_signals_cache(stock_codes, tushare_fetcher, akshare_fetcher)
                except Exception as e:
                    logger.warning("[FactorSignals] 因子信号缓存构建失败（不阻断主流程）: %s", e)

        # Auto-discovery: 盘后深度扫描，自动发现高潜力股票并入分析列表
        if getattr(self.config, 'auto_discover', False) and not dry_run:
            tushare_fetcher = self.fetcher_manager._get_tushare_fetcher()
            akshare_fetcher = self.fetcher_manager._get_akshare_fetcher()
            if tushare_fetcher and tushare_fetcher.is_available():
                try:
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

                    discovery_config = get_discovery_config()
                    engine = StockDiscoveryEngine(
                        discovery_config,
                        tushare_fetcher,
                        akshare_fetcher,
                    )
                    engine.register_factors([
                        MoneyFlowFactor(),
                        MarginFactor(),
                        ChipFactor(),
                        TechnicalFactor(),
                        LimitFactor(),
                        FundamentalFactor(),
                        PopularityFactor(),
                        HotMoneyFactor(),
                        NorthboundFactor(),
                        InstitutionHoldFactor(),
                        ProfitForecastFactor(),
                        PerformanceFactor(),
                        BuybackFactor(),
                        InsiderBuyFactor(),
                        BrokerRecommendFactor(),
                    ])

                    discovered = engine.discover(mode="postmarket")
                    if discovered:
                        discovered_codes = [
                            r.stock_code for r in discovered[:discovery_config.auto_discover_count]
                        ]
                        logger.info(
                            "[Discovery] 盘后发现 %d 只候选股: %s",
                            len(discovered_codes),
                            ", ".join(discovered_codes),
                        )
                        # 生成唯一发现轮次 ID（关联 Pipeline 分析与回测结果）
                        discovery_run_id = str(uuid.uuid4())[:8]
                        # 落盘发现报告到 discovery_reports/
                        try:
                            import json
                            from datetime import date
                            from pathlib import Path
                            report = engine.format_report(discovered, mode="postmarket")
                            reports_dir = Path(__file__).resolve().parent.parent.parent / "discovery_reports"
                            reports_dir.mkdir(parents=True, exist_ok=True)
                            date_str = date.today().strftime('%Y%m%d')

                            # 数据指纹：若与已有文件相同则跳过报告写入（数据未变，不覆盖更好的新发现）
                            json_file = reports_dir / f"postmarket_{date_str}_topn.json"
                            new_hash = engine._calc_factor_data_hash(
                                getattr(engine, '_factor_data_cache', {})
                            ) if hasattr(engine, '_calc_factor_data_hash') else ""
                            existing_hash = ""
                            if json_file.exists():
                                try:
                                    existing = json.loads(json_file.read_text(encoding="utf-8"))
                                    existing_hash = existing[0].get("data_hash", "") if existing else ""
                                except Exception:
                                    pass

                            filepath = reports_dir / f"postmarket_{date_str}.md"
                            if existing_hash != new_hash:
                                filepath.write_text(report, encoding="utf-8")
                                logger.info("[Discovery] 发现报告已保存: %s", filepath)
                            else:
                                logger.info("[Discovery] 数据未变化，跳过报告写入（hash=%s）", new_hash)

                            # 同时保存完整结构化 JSON（包含 discovery_run_id，保留所有候选股用于回测复盘）
                            topn = []
                            for i, r in enumerate(discovered, 1):
                                topn.append({
                                    "rank": i,
                                    "discovery_run_id": discovery_run_id,
                                    "data_hash": new_hash,
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
                            json_file.write_text(json.dumps(topn, ensure_ascii=False, indent=2), encoding="utf-8")
                            logger.info("[Discovery] TopN JSON 已保存: %s", json_file)

                            # 回测闭环：发现完成后自动触发回测，追加结果到报告
                            # 不传 start_date/end_date，让 backtest 用默认 lookback_days=30 找有历史数据的日期
                            try:
                                from src.discovery.backtest import DiscoveryBacktest
                                bt = DiscoveryBacktest(tushare_fetcher)
                                summary = bt.compute(mode="postmarket")
                                if summary and summary.trade_records:
                                    bt_md = _format_backtest_summary_md(summary, date_str)
                                    bt_file = reports_dir / f"postmarket_{date_str}_backtest.md"
                                    bt_file.write_text(bt_md, encoding="utf-8")
                                    logger.info("[Discovery] 回测报告已保存: %s", bt_file)
                            except Exception as e:
                                logger.debug("[Discovery] 回测执行失败: %s", e)
                        except Exception as e:
                            logger.debug("[Discovery] 保存报告失败: %s", e)
                        existing = set(stock_codes or [])
                        new_codes = [c for c in discovered_codes if c not in existing]
                        if new_codes:
                            stock_codes = (list(stock_codes or []) if stock_codes else []) + new_codes
                            logger.info(
                                "[Discovery] 新增 %d 只到分析列表, 合并后共 %d 只",
                                len(new_codes),
                                len(stock_codes),
                            )
                        else:
                            logger.info("[Discovery] 发现的股票均已在自选列表中")
                        # 将发现结果回写到 .env STOCK_LIST，确保持久化（即使后续分析失败也有兜底）
                        try:
                            from src.services.system_config_service import SystemConfigService
                            merged_stock_list = ",".join(stock_codes if stock_codes else [])
                            SystemConfigService().apply_simple_updates([("STOCK_LIST", merged_stock_list)])
                            logger.info("[Discovery] STOCK_LIST 已同步到 .env: %d 只", len(stock_codes if stock_codes else []))
                        except Exception as e:
                            logger.debug("[Discovery] 同步 STOCK_LIST 失败: %s", e)
                    else:
                        logger.info("[Discovery] 未发现符合条件的股票，继续分析现有自选股")
                except Exception as e:
                    logger.warning("[Discovery] 自动发现失败（不阻断主流程）: %s", e)

        if not stock_codes:
            logger.error("未配置自选股列表，请在 .env 文件中设置 STOCK_LIST 或开启 AUTO_DISCOVER")
            return []

        logger.info(f"===== 开始分析 {len(stock_codes)} 只股票 =====")
        logger.info(f"股票列表: {', '.join(stock_codes)}")
        logger.info(f"并发数: {self.max_workers}, 模式: {'仅获取数据' if dry_run else '完整分析'}")

        # 冻结本轮运行的统一参考时间，避免跨市场收盘边界时同批股票使用不同目标交易日。
        resume_reference_time = datetime.now(timezone.utc)
        
        # === 批量预取实时行情（优化：避免每只股票都触发全量拉取）===
        # 只有股票数量 >= 5 时才进行预取，少量股票直接逐个查询更高效
        if len(stock_codes) >= 5:
            prefetch_count = self.fetcher_manager.prefetch_realtime_quotes(stock_codes)
            if prefetch_count > 0:
                logger.info(f"已启用批量预取架构：一次拉取全市场数据，{len(stock_codes)} 只股票共享缓存")

        # Issue #455: 预取股票名称，避免并发分析时显示「股票xxxxx」
        # dry_run 仅做数据拉取，不需要名称预取，避免额外网络开销
        if not dry_run:
            self.fetcher_manager.prefetch_stock_names(stock_codes, use_bulk=False)

        # 单股推送模式（#55）：从配置读取
        single_stock_notify = getattr(self.config, 'single_stock_notify', False)
        # Issue #119: 从配置读取报告类型
        report_type_str = getattr(self.config, 'report_type', 'simple').lower()
        if report_type_str == 'brief':
            report_type = ReportType.BRIEF
        elif report_type_str == 'full':
            report_type = ReportType.FULL
        else:
            report_type = ReportType.SIMPLE
        # Issue #128: 从配置读取分析间隔
        analysis_delay = getattr(self.config, 'analysis_delay', 0)

        if single_stock_notify:
            logger.info(
                "已启用单股推送模式：分析仍并发执行，通知改为在结果收集侧串行发送（报告类型: %s）",
                report_type_str,
            )
        
        results: List[AnalysisResult] = []
        
        # 使用线程池并发处理
        # 注意：max_workers 设置较低（默认3）以避免触发反爬
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(
                    self.process_single_stock,
                    code,
                    skip_analysis=dry_run,
                    single_stock_notify=False,
                    report_type=report_type,  # Issue #119: 传递报告类型
                    analysis_query_id=uuid.uuid4().hex,
                    current_time=resume_reference_time,
                ): code
                for code in stock_codes
            }
            
            # 收集结果
            for idx, future in enumerate(as_completed(future_to_code)):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result and result.success:
                        results.append(result)
                        if single_stock_notify and send_notification and not dry_run:
                            self._send_single_stock_notification(
                                result,
                                report_type=report_type,
                                fallback_code=code,
                            )
                    elif result and not result.success:
                        logger.warning(
                            f"[{code}] 分析结果标记为失败，不计入汇总: "
                            f"{result.error_message or '未知原因'}"
                        )

                    # Issue #128: 分析间隔 - 在个股分析和大盘分析之间添加延迟
                    if idx < len(stock_codes) - 1 and analysis_delay > 0:
                        # 注意：此 sleep 发生在“主线程收集 future 的循环”中，
                        # 并不会阻止线程池中的任务同时发起网络请求。
                        # 因此它对降低并发请求峰值的效果有限；真正的峰值主要由 max_workers 决定。
                        # 该行为目前保留（按需求不改逻辑）。
                        logger.debug(f"等待 {analysis_delay} 秒后继续下一只股票...")
                        time.sleep(analysis_delay)

                except Exception as e:
                    logger.error(f"[{code}] 任务执行失败: {e}")
        
        # 统计
        elapsed_time = time.time() - start_time
        
        # dry-run 模式下，数据获取成功即视为成功
        if dry_run:
            # 检查哪些股票的最新可复用交易日数据已存在
            success_count = sum(
                1
                for code in stock_codes
                if self.db.has_today_data(
                    code,
                    self._resolve_resume_target_date(
                        code, current_time=resume_reference_time
                    ),
                )
            )
            fail_count = len(stock_codes) - success_count
        else:
            success_count = len(results)
            fail_count = len(stock_codes) - success_count
        
        logger.info("===== 分析完成 =====")
        logger.info(f"成功: {success_count}, 失败: {fail_count}, 耗时: {elapsed_time:.2f} 秒")

        if defer_aggregate_report and results and not dry_run:
            logger.info(
                "defer_aggregate_report=True：已跳过本批汇总日报落盘与汇总推送，等待 main 层合并后再写入。"
            )
        else:
            # 保存报告到本地文件（无论是否推送通知都保存）
            if results and not dry_run:
                self._save_local_report(results, report_type)

            # 发送通知（单股推送模式下跳过汇总推送，避免重复）
            if results and send_notification and not dry_run:
                if single_stock_notify:
                    # 单股推送模式：只保存汇总报告，不再重复推送
                    logger.info("单股推送模式：跳过汇总推送，仅保存报告到本地")
                    self._send_notifications(results, report_type, skip_push=True)
                elif merge_notification:
                    # 合并模式（Issue #190）：仅保存，不推送，由 main 层合并个股+大盘后统一发送
                    logger.info("合并推送模式：跳过本次推送，将在个股+大盘复盘后统一发送")
                    self._send_notifications(results, report_type, skip_push=True)
                else:
                    self._send_notifications(results, report_type)
        
        return results

    def _send_single_stock_notification(
        self,
        result: AnalysisResult,
        report_type: ReportType = ReportType.SIMPLE,
        fallback_code: Optional[str] = None,
    ) -> None:
        """发送单股通知，供直接单股入口和批量串行推送共用。"""
        if not self.notifier.is_available():
            return

        stock_code = getattr(result, "code", None) or fallback_code or "unknown"
        notify_lock = getattr(self, "_single_stock_notify_lock", None)
        if notify_lock is None:
            with _SINGLE_STOCK_NOTIFY_LOCK_INIT_GUARD:
                notify_lock = getattr(self, "_single_stock_notify_lock", None)
                if notify_lock is None:
                    notify_lock = threading.Lock()
                    setattr(self, "_single_stock_notify_lock", notify_lock)

        with notify_lock:
            try:
                if report_type == ReportType.FULL:
                    report_content = self.notifier.generate_dashboard_report([result])
                    logger.info(f"[{stock_code}] 使用完整报告格式")
                elif report_type == ReportType.BRIEF:
                    report_content = self.notifier.generate_brief_report([result])
                    logger.info(f"[{stock_code}] 使用简洁报告格式")
                else:
                    report_content = self.notifier.generate_single_stock_report(result)
                    logger.info(f"[{stock_code}] 使用精简报告格式")

                if self.notifier.send(report_content, email_stock_codes=[stock_code]):
                    logger.info(f"[{stock_code}] 单股推送成功")
                else:
                    logger.warning(f"[{stock_code}] 单股推送失败")
            except Exception as e:
                logger.error(f"[{stock_code}] 单股推送异常: {e}")

    def get_config_report_type(self) -> ReportType:
        """Map config.report_type string to :class:`ReportType` (same rules as ``_run_impl``)."""
        report_type_str = getattr(self.config, "report_type", "simple").lower()
        if report_type_str == "brief":
            return ReportType.BRIEF
        if report_type_str == "full":
            return ReportType.FULL
        return ReportType.SIMPLE

    def _save_local_report(
        self,
        results: List[AnalysisResult],
        report_type: ReportType = ReportType.SIMPLE,
    ) -> None:
        """保存分析报告到本地文件（与通知推送解耦）"""
        try:
            report = self._generate_aggregate_report(results, report_type)
            filepath = self.notifier.save_report_to_file(report)
            logger.info(f"决策仪表盘日报已保存: {filepath}")
            self._sync_env_categories_from_results(results)
        except Exception as e:
            logger.error(f"保存本地报告失败: {e}")

    def _sync_env_categories_from_results(self, results: List[AnalysisResult]) -> None:
        """根据当次报告结果同步 .env 的 BUY/HOLD/LOOK/SELL 与 STOCK_LIST。"""
        if not results:
            return
        try:
            from src.services.system_config_service import SystemConfigService
            from src.utils.rating_category import operation_advice_to_category

            category_stocks: Dict[str, List[str]] = {
                "BUY": [],
                "HOLD": [],
                "LOOK": [],
                "SELL": [],
            }
            unmapped: set[str] = set()

            for result in results:
                advice = (getattr(result, "operation_advice", "") or "").strip()
                category = operation_advice_to_category(advice, unmapped=unmapped)
                category_stocks[category].append(getattr(result, "code", ""))

            updates = []
            for category in ("BUY", "HOLD", "LOOK", "SELL"):
                stocks = sorted({code for code in category_stocks[category] if code})
                updates.append((category, ",".join(stocks)))

            config_service = SystemConfigService()
            config_service.apply_simple_updates(updates)

            buy_stocks = set(code for code in category_stocks["BUY"] if code)
            hold_stocks = set(code for code in category_stocks["HOLD"] if code)
            stock_list = ",".join(sorted(buy_stocks | hold_stocks))
            config_service.apply_simple_updates([("STOCK_LIST", stock_list)])

            logger.info(
                "日报生成后已自动同步 .env 分类: BUY=%s HOLD=%s LOOK=%s SELL=%s",
                len(buy_stocks),
                len(hold_stocks),
                len([c for c in category_stocks["LOOK"] if c]),
                len([c for c in category_stocks["SELL"] if c]),
            )
            if unmapped:
                logger.warning(
                    "检测到未登记评级，已按 LOOK 归类: %s",
                    ", ".join(sorted(unmapped)),
                )
        except Exception as exc:
            logger.error("日报生成后自动同步 .env 分类失败: %s", exc)

    def _send_notifications(
        self,
        results: List[AnalysisResult],
        report_type: ReportType = ReportType.SIMPLE,
        skip_push: bool = False,
    ) -> None:
        """
        发送分析结果通知
        
        生成决策仪表盘格式的报告
        
        Args:
            results: 分析结果列表
            skip_push: 是否跳过推送（仅保存到本地，用于单股推送模式）
        """
        try:
            logger.info("生成决策仪表盘日报...")
            report = self._generate_aggregate_report(results, report_type)
            
            # 跳过推送（单股推送模式 / 合并模式：报告已由 _save_local_report 保存）
            if skip_push:
                return
            
            # 推送通知
            if self.notifier.is_available():
                channels = self.notifier.get_available_channels()
                context_success = self.notifier.send_to_context(report)

                # Issue #455: Markdown 转图片（与 notification.send 逻辑一致）
                from src.md2img import markdown_to_image

                channels_needing_image = {
                    ch for ch in channels
                    if ch.value in self.notifier._markdown_to_image_channels
                }
                non_wechat_channels_needing_image = {
                    ch for ch in channels_needing_image if ch != NotificationChannel.WECHAT
                }

                def _get_md2img_hint() -> str:
                    try:
                        engine = getattr(get_config(), "md2img_engine", "wkhtmltoimage")
                    except Exception:
                        engine = "wkhtmltoimage"
                    return (
                        "npm i -g markdown-to-file" if engine == "markdown-to-file"
                        else "wkhtmltopdf (apt install wkhtmltopdf / brew install wkhtmltopdf)"
                    )

                image_bytes = None
                if non_wechat_channels_needing_image:
                    image_bytes = markdown_to_image(
                        report, max_chars=self.notifier._markdown_to_image_max_chars
                    )
                    if image_bytes:
                        logger.info(
                            "Markdown 已转换为图片，将向 %s 发送图片",
                            [ch.value for ch in non_wechat_channels_needing_image],
                        )
                    else:
                        logger.warning(
                            "Markdown 转图片失败，将回退为文本发送。请检查 MARKDOWN_TO_IMAGE_CHANNELS 配置并安装 %s",
                            _get_md2img_hint(),
                        )

                # 企业微信：只发精简版（平台限制）
                wechat_success = False
                if NotificationChannel.WECHAT in channels:
                    if report_type == ReportType.BRIEF:
                        dashboard_content = self.notifier.generate_brief_report(results)
                    else:
                        dashboard_content = self.notifier.generate_wechat_dashboard(results)
                    logger.info(f"企业微信仪表盘长度: {len(dashboard_content)} 字符")
                    logger.debug(f"企业微信推送内容:\n{dashboard_content}")
                    wechat_image_bytes = None
                    if NotificationChannel.WECHAT in channels_needing_image:
                        wechat_image_bytes = markdown_to_image(
                            dashboard_content,
                            max_chars=self.notifier._markdown_to_image_max_chars,
                        )
                        if wechat_image_bytes is None:
                            logger.warning(
                                "企业微信 Markdown 转图片失败，将回退为文本发送。请检查 MARKDOWN_TO_IMAGE_CHANNELS 配置并安装 %s",
                                _get_md2img_hint(),
                            )
                    use_image = self.notifier._should_use_image_for_channel(
                        NotificationChannel.WECHAT, wechat_image_bytes
                    )
                    if use_image:
                        wechat_success = self.notifier._send_wechat_image(wechat_image_bytes)
                    else:
                        wechat_success = self.notifier.send_to_wechat(dashboard_content)

                # 其他渠道：发完整报告（避免自定义 Webhook 被 wechat 截断逻辑污染）
                non_wechat_success = False
                stock_email_groups = getattr(self.config, 'stock_email_groups', []) or []
                for channel in channels:
                    if channel == NotificationChannel.WECHAT:
                        continue
                    if channel == NotificationChannel.FEISHU:
                        non_wechat_success = self.notifier.send_to_feishu(report) or non_wechat_success
                    elif channel == NotificationChannel.TELEGRAM:
                        use_image = self.notifier._should_use_image_for_channel(
                            channel, image_bytes
                        )
                        if use_image:
                            result = self.notifier._send_telegram_photo(image_bytes)
                        else:
                            result = self.notifier.send_to_telegram(report)
                        non_wechat_success = result or non_wechat_success
                    elif channel == NotificationChannel.EMAIL:
                        if stock_email_groups:
                            code_to_emails: Dict[str, Optional[List[str]]] = {}
                            for r in results:
                                if r.code not in code_to_emails:
                                    canonical = normalize_stock_code(r.code)
                                    emails = []
                                    for stocks, emails_list in stock_email_groups:
                                        if canonical in stocks:
                                            emails.extend(emails_list)
                                    code_to_emails[r.code] = list(dict.fromkeys(emails)) if emails else None
                            emails_to_results: Dict[Optional[Tuple], List] = defaultdict(list)
                            for r in results:
                                recs = code_to_emails.get(r.code)
                                key = tuple(recs) if recs else None
                                emails_to_results[key].append(r)
                            for key, group_results in emails_to_results.items():
                                grp_report = self._generate_aggregate_report(group_results, report_type)
                                grp_image_bytes = None
                                if channel.value in self.notifier._markdown_to_image_channels:
                                    grp_image_bytes = markdown_to_image(
                                        grp_report,
                                        max_chars=self.notifier._markdown_to_image_max_chars,
                                    )
                                use_image = self.notifier._should_use_image_for_channel(
                                    channel, grp_image_bytes
                                )
                                receivers = list(key) if key is not None else None
                                if use_image:
                                    result = self.notifier._send_email_with_inline_image(
                                        grp_image_bytes, receivers=receivers
                                    )
                                else:
                                    result = self.notifier.send_to_email(
                                        grp_report, receivers=receivers
                                    )
                                non_wechat_success = result or non_wechat_success
                        else:
                            use_image = self.notifier._should_use_image_for_channel(
                                channel, image_bytes
                            )
                            if use_image:
                                result = self.notifier._send_email_with_inline_image(image_bytes)
                            else:
                                result = self.notifier.send_to_email(report)
                            non_wechat_success = result or non_wechat_success
                    elif channel == NotificationChannel.CUSTOM:
                        use_image = self.notifier._should_use_image_for_channel(
                            channel, image_bytes
                        )
                        if use_image:
                            result = self.notifier._send_custom_webhook_image(
                                image_bytes, fallback_content=report
                            )
                        else:
                            result = self.notifier.send_to_custom(report)
                        non_wechat_success = result or non_wechat_success
                    elif channel == NotificationChannel.PUSHPLUS:
                        non_wechat_success = self.notifier.send_to_pushplus(report) or non_wechat_success
                    elif channel == NotificationChannel.SERVERCHAN3:
                        non_wechat_success = self.notifier.send_to_serverchan3(report) or non_wechat_success
                    elif channel == NotificationChannel.DISCORD:
                        non_wechat_success = self.notifier.send_to_discord(report) or non_wechat_success
                    elif channel == NotificationChannel.PUSHOVER:
                        non_wechat_success = self.notifier.send_to_pushover(report) or non_wechat_success
                    elif channel == NotificationChannel.ASTRBOT:
                        non_wechat_success = self.notifier.send_to_astrbot(report) or non_wechat_success
                    elif channel == NotificationChannel.SLACK:
                        use_image = self.notifier._should_use_image_for_channel(
                            channel, image_bytes
                        )
                        if use_image and self.notifier._slack_bot_token and self.notifier._slack_channel_id:
                            result = self.notifier._send_slack_image(
                                image_bytes, fallback_content=report
                            )
                        else:
                            result = self.notifier.send_to_slack(report)
                        non_wechat_success = result or non_wechat_success
                    else:
                        logger.warning(f"未知通知渠道: {channel}")

                success = wechat_success or non_wechat_success or context_success
                if success:
                    logger.info("决策仪表盘推送成功")
                else:
                    logger.warning("决策仪表盘推送失败")
            else:
                logger.info("通知渠道未配置，跳过推送")
                
        except Exception as e:
            import traceback
            logger.error(f"发送通知失败: {e}\n{traceback.format_exc()}")

    def _generate_aggregate_report(
        self,
        results: List[AnalysisResult],
        report_type: ReportType,
    ) -> str:
        """Generate aggregate report with backward-compatible notifier fallback."""
        generator = getattr(self.notifier, "generate_aggregate_report", None)
        if callable(generator):
            return generator(results, report_type)
        if report_type == ReportType.BRIEF and hasattr(self.notifier, "generate_brief_report"):
            return self.notifier.generate_brief_report(results)
        return self.notifier.generate_dashboard_report(results)

    @staticmethod
    def _format_backtest_summary_md(summary, trade_date: str) -> str:
        """格式化回测结果为 Markdown，用于追加到发现报告。"""
        from src.discovery.backtest import BacktestSummary

        lines = [
            f"## 回测表现（{trade_date}）",
            "",
            "| 指标 | 值 |",
            "|------|-----|",
            f"| 初始资金 | {summary.initial_capital:,.0f} 元 |",
            f"| 最终资金 | {summary.final_capital:,.0f} 元 |",
            f"| 累计收益率 | {summary.cumulative_return*100:.2f}% |",
            f"| 总盈利 | {summary.total_pnl:,.0f} 元 |",
            f"| 胜率 | {summary.win_rate*100:.1f}% |",
            f"| 交易次数 | {summary.total_trades} |",
            f"| 交易天数 | {summary.total_days} |",
            "",
        ]

        if summary.trade_records:
            lines.append("### Top 收益个股")
            lines.append("")
            lines.append("| 股票 | 买入日期 | 买入价 | 卖出日期 | 卖出价 | 收益率 |")
            lines.append("|------|----------|--------|----------|--------|--------|")

            # 按收益率排序，取前5
            sorted_trades = sorted(
                summary.trade_records,
                key=lambda t: t.return_pct,
                reverse=True,
            )
            for t in sorted_trades[:5]:
                lines.append(
                    f"| {t.stock_name}({t.stock_code}) | {t.buy_date} | "
                    f"{t.buy_price:.2f} | {t.sell_date} | {t.sell_price:.2f} | "
                    f"{t.return_pct*100:.2f}% |"
                )
            lines.append("")

        return "\n".join(lines)
