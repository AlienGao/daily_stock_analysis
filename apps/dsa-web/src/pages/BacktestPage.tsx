import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { Check, Minus, X } from 'lucide-react';
import { backtestApi } from '../api/backtest';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, Card, Badge, EmptyState, Pagination, StatusDot, Tooltip } from '../components/common';
import type {
  BacktestResultItem,
  BacktestRunResponse,
  PerformanceMetrics,
} from '../types/backtest';

type BacktestFilterMode = 'all' | 'signal' | 'score' | 'signal_and_score';
type BacktestSortBy = 'analysis_date' | 'actual_return_pct' | 'sentiment_score';

const BACKTEST_INPUT_CLASS =
  'input-surface input-focus-glow h-11 w-full rounded-xl border bg-transparent px-4 text-sm transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';
const BACKTEST_COMPACT_INPUT_CLASS =
  'input-surface input-focus-glow h-10 rounded-xl border bg-transparent px-3 py-2 text-xs transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';

// ============ Helpers ============

function pct(value?: number | null): string {
  if (value == null) return '--';
  return `${value.toFixed(1)}%`;
}

function pct2(value?: number | null): string {
  if (value == null) return '--';
  return `${value.toFixed(2)}%`;
}

function outcomeBadge(outcome?: string) {
  if (!outcome) return <Badge variant="default">--</Badge>;
  switch (outcome) {
    case 'win':
      return <Badge variant="success" glow>胜</Badge>;
    case 'loss':
      return <Badge variant="danger" glow>负</Badge>;
    case 'neutral':
      return <Badge variant="warning">中性</Badge>;
    default:
      return <Badge variant="default">{outcome}</Badge>;
  }
}

function statusBadge(status: string) {
  switch (status) {
    case 'completed':
      return <Badge variant="success">已完成</Badge>;
    case 'insufficient':
    case 'insufficient_data':
      return <Badge variant="warning">数据不足</Badge>;
    case 'error':
      return <Badge variant="danger">错误</Badge>;
    default:
      return <Badge variant="default">{status}</Badge>;
  }
}

function actualMovementBadge(movement?: string | null) {
  switch (movement) {
    case 'up':
      return <Badge variant="success">上涨</Badge>;
    case 'down':
      return <Badge variant="danger">下跌</Badge>;
    case 'flat':
      return <Badge variant="warning">持平</Badge>;
    default:
      return <Badge variant="default">--</Badge>;
  }
}

function boolIcon(value?: boolean | null) {
  if (value === true) {
    return (
      <span
        className="backtest-status-chip backtest-status-chip-success"
        aria-label="yes"
      >
        <StatusDot tone="success" className="backtest-status-chip-dot" />
        <Check className="h-3.5 w-3.5" />
      </span>
    );
  }

  if (value === false) {
    return (
      <span
        className="backtest-status-chip backtest-status-chip-danger"
        aria-label="no"
      >
        <StatusDot tone="danger" className="backtest-status-chip-dot" />
        <X className="h-3.5 w-3.5" />
      </span>
    );
  }

  return (
    <span
      className="backtest-status-chip backtest-status-chip-neutral"
      aria-label="unknown"
    >
      <StatusDot tone="neutral" className="backtest-status-chip-dot" />
      <Minus className="h-3.5 w-3.5" />
    </span>
  );
}

function directionExpectedLabel(value?: string | null): string {
  switch ((value || '').toLowerCase()) {
    case 'up':
      return '看涨';
    case 'down':
      return '看跌';
    case 'flat':
      return '震荡';
    case 'not_down':
      return '不跌';
    default:
      return value || '';
  }
}

function scoreBadge(value?: number | null) {
  if (value == null || Number.isNaN(Number(value))) {
    return <Badge variant="default">--</Badge>;
  }
  const score = Number(value);
  if (score >= 70) return <Badge variant="success">{score}</Badge>;
  if (score >= 50) return <Badge variant="warning">{score}</Badge>;
  return <Badge variant="danger">{score}</Badge>;
}

// ============ Metric Row ============

const MetricRow: React.FC<{ label: string; value: string; accent?: boolean }> = ({ label, value, accent }) => (
  <div className="backtest-metric-row">
    <span className="label">{label}</span>
    <span className={`value ${accent ? 'accent' : ''}`}>{value}</span>
  </div>
);

// ============ Performance Card ============

const PerformanceCard: React.FC<{ metrics: PerformanceMetrics; title: string }> = ({ metrics, title }) => (
  <Card variant="gradient" padding="md" className="animate-fade-in">
    <div className="mb-3">
      <span className="label-uppercase">{title}</span>
    </div>
    <MetricRow label="方向准确率" value={pct(metrics.directionAccuracyPct)} accent />
    <MetricRow label="胜率" value={pct(metrics.winRatePct)} accent />
    <MetricRow label="平均模拟收益" value={pct(metrics.avgSimulatedReturnPct)} />
    <MetricRow label="平均股票收益" value={pct(metrics.avgStockReturnPct)} />
    <MetricRow label="止损触发率" value={pct(metrics.stopLossTriggerRate)} />
    <MetricRow label="止盈触发率" value={pct(metrics.takeProfitTriggerRate)} />
    <MetricRow label="平均命中天数" value={metrics.avgDaysToFirstHit != null ? metrics.avgDaysToFirstHit.toFixed(1) : '--'} />
    <div className="backtest-metric-footer">
      <span className="text-xs text-muted-text">评估数</span>
      <span className="text-xs text-secondary-text font-mono">
        {Number(metrics.completedCount)} / {Number(metrics.totalEvaluations)}
      </span>
    </div>
    <div className="flex items-center justify-between">
      <span className="text-xs text-muted-text">胜 / 负 / 中</span>
      <span className="text-xs font-mono">
        <span className="text-success">{metrics.winCount}</span>
        {' / '}
        <span className="text-danger">{metrics.lossCount}</span>
        {' / '}
        <span className="text-warning">{metrics.neutralCount}</span>
      </span>
    </div>
  </Card>
);

// ============ Run Summary ============

const RunSummary: React.FC<{ data: BacktestRunResponse }> = ({ data }) => (
  <div className="backtest-summary animate-fade-in">
    <span className="label">处理: <span className="value">{data.processed}</span></span>
    <span className="label">写入: <span className="value primary">{data.saved}</span></span>
    <span className="label">完成: <span className="value success">{data.completed}</span></span>
    <span className="label">不足: <span className="value warning">{data.insufficient}</span></span>
    {data.errors > 0 && (
      <span className="label">错误: <span className="value danger">{data.errors}</span></span>
    )}
  </div>
);

// ============ Main Page ============

const BacktestPage: React.FC = () => {
  const triggerSourceLabel = (value: 'auto' | 'manual' | '') => {
    if (value === 'auto') return '自动';
    if (value === 'manual') return '手动';
    return '全部';
  };
  // Set page title
  useEffect(() => {
    document.title = '策略回测 - DSA';
  }, []);

  // Input state
  const [codeFilter, setCodeFilter] = useState('');
  const [triggerSourceFilter, setTriggerSourceFilter] = useState<'auto' | 'manual' | ''>('');
  const [analysisDateFrom, setAnalysisDateFrom] = useState('');
  const [analysisDateTo, setAnalysisDateTo] = useState('');
  const [sortBy, setSortBy] = useState<BacktestSortBy>('analysis_date');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [evalDays, setEvalDays] = useState('');
  const [forceRerun, setForceRerun] = useState(false);
  const [runFilterMode, setRunFilterMode] = useState<BacktestFilterMode>('signal');
  const [scoreMin, setScoreMin] = useState('');
  const [scoreMax, setScoreMax] = useState('');
  const [isRunning, setIsRunning] = useState(false);
  const [runResult, setRunResult] = useState<BacktestRunResponse | null>(null);
  const [runError, setRunError] = useState<ParsedApiError | null>(null);
  const [pageError, setPageError] = useState<ParsedApiError | null>(null);

  // Results state
  const [results, setResults] = useState<BacktestResultItem[]>([]);
  const [totalResults, setTotalResults] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [isLoadingResults, setIsLoadingResults] = useState(false);
  const pageSize = 20;

  // Performance state
  const [overallPerf, setOverallPerf] = useState<PerformanceMetrics | null>(null);
  const [stockPerf, setStockPerf] = useState<PerformanceMetrics | null>(null);
  const [isLoadingPerf, setIsLoadingPerf] = useState(false);
  const effectiveWindowDays = evalDays ? parseInt(evalDays, 10) : overallPerf?.evalWindowDays;
  const isNextDayValidation = effectiveWindowDays === 1;
  const showNextDayActualColumns = isNextDayValidation;

  // Fetch results
  const fetchResults = useCallback(async (
    page = 1,
    code?: string,
    triggerSource?: 'auto' | 'manual' | '',
    windowDays?: number,
    startDate?: string,
    endDate?: string,
    currentSortBy: BacktestSortBy = 'analysis_date',
    currentSortOrder: 'asc' | 'desc' = 'desc',
  ) => {
    setIsLoadingResults(true);
    try {
      const response = await backtestApi.getResults({
        code: code || undefined,
        triggerSource: triggerSource || undefined,
        evalWindowDays: windowDays,
        analysisDateFrom: startDate || undefined,
        analysisDateTo: endDate || undefined,
        sortBy: currentSortBy,
        sortOrder: currentSortOrder,
        page,
        limit: pageSize,
      });
      setResults(response.items);
      setTotalResults(response.total);
      setCurrentPage(response.page);
      setPageError(null);
    } catch (err) {
      console.error('Failed to fetch backtest results:', err);
      setPageError(getParsedApiError(err));
    } finally {
      setIsLoadingResults(false);
    }
  }, []);

  // Fetch performance
  const fetchPerformance = useCallback(async (
    code?: string,
    triggerSource?: 'auto' | 'manual' | '',
    windowDays?: number,
    startDate?: string,
    endDate?: string,
  ) => {
    setIsLoadingPerf(true);
    try {
      const overall = await backtestApi.getOverallPerformance({
        triggerSource: triggerSource || undefined,
        evalWindowDays: windowDays,
        analysisDateFrom: startDate || undefined,
        analysisDateTo: endDate || undefined,
      });
      setOverallPerf(overall);

      if (code) {
        const stock = await backtestApi.getStockPerformance(code, {
          triggerSource: triggerSource || undefined,
          evalWindowDays: windowDays,
          analysisDateFrom: startDate || undefined,
          analysisDateTo: endDate || undefined,
        });
        setStockPerf(stock);
      } else {
        setStockPerf(null);
      }
      setPageError(null);
    } catch (err) {
      console.error('Failed to fetch performance:', err);
      setPageError(getParsedApiError(err));
    } finally {
      setIsLoadingPerf(false);
    }
  }, []);

  // Initial load — fetch performance first, then filter results by its window
  useEffect(() => {
    const init = async () => {
      // Get latest performance (unfiltered returns most recent summary)
      const overall = await backtestApi.getOverallPerformance();
      setOverallPerf(overall);
      // Use the summary's eval_window_days to filter results consistently
      const windowDays = overall?.evalWindowDays;
      if (windowDays && !evalDays) {
        setEvalDays(String(windowDays));
      }
      fetchResults(1, undefined, '', windowDays, undefined, undefined, sortBy, sortOrder);
    };
    init();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Run backtest
  const handleRun = async () => {
    setIsRunning(true);
    setRunResult(null);
    setRunError(null);
    try {
      const code = codeFilter.trim() || undefined;
      const evalWindowDays = evalDays ? parseInt(evalDays, 10) : undefined;
      const useSignalFilter = runFilterMode === 'signal' || runFilterMode === 'signal_and_score';
      const useScoreFilter = runFilterMode === 'score' || runFilterMode === 'signal_and_score';
      const response = await backtestApi.run({
        code,
        force: forceRerun || undefined,
        minAgeDays: forceRerun ? 0 : undefined,
        evalWindowDays,
        allowedCategories: useSignalFilter ? ['BUY', 'HOLD'] : undefined,
        sentimentScoreMin: useScoreFilter && scoreMin !== '' ? parseInt(scoreMin, 10) : undefined,
        sentimentScoreMax: useScoreFilter && scoreMax !== '' ? parseInt(scoreMax, 10) : undefined,
      });
      setRunResult(response);
      // Refresh data with same eval_window_days
      fetchResults(
        1,
        codeFilter.trim() || undefined,
        triggerSourceFilter,
        evalWindowDays,
        analysisDateFrom,
        analysisDateTo,
        sortBy,
        sortOrder,
      );
      fetchPerformance(
        codeFilter.trim() || undefined,
        triggerSourceFilter,
        evalWindowDays,
        analysisDateFrom,
        analysisDateTo,
      );
    } catch (err) {
      setRunError(getParsedApiError(err));
    } finally {
      setIsRunning(false);
    }
  };

  // Filter by code
  const handleFilter = () => {
    const code = codeFilter.trim() || undefined;
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    setCurrentPage(1);
    fetchResults(1, code, triggerSourceFilter, windowDays, analysisDateFrom, analysisDateTo, sortBy, sortOrder);
    fetchPerformance(code, triggerSourceFilter, windowDays, analysisDateFrom, analysisDateTo);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleFilter();
    }
  };

  const handleShowNextDay = () => {
    const code = codeFilter.trim() || undefined;
    setEvalDays('1');
    setCurrentPage(1);
    fetchResults(1, code, triggerSourceFilter, 1, analysisDateFrom, analysisDateTo, sortBy, sortOrder);
    fetchPerformance(code, triggerSourceFilter, 1, analysisDateFrom, analysisDateTo);
  };

  const applySort = (nextSortBy: BacktestSortBy, nextSortOrder: 'asc' | 'desc') => {
    setSortBy(nextSortBy);
    setSortOrder(nextSortOrder);
    const code = codeFilter.trim() || undefined;
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    setCurrentPage(1);
    fetchResults(1, code, triggerSourceFilter, windowDays, analysisDateFrom, analysisDateTo, nextSortBy, nextSortOrder);
  };

  const handleSortModeChange = (value: string) => {
    const [nextSortByRaw, nextSortOrderRaw] = value.split(':');
    const nextSortBy = (nextSortByRaw as BacktestSortBy) || 'analysis_date';
    const nextSortOrder = nextSortOrderRaw === 'asc' ? 'asc' : 'desc';
    applySort(nextSortBy, nextSortOrder);
  };

  const toggleAnalysisDateSort = () => {
    if (sortBy === 'analysis_date') {
      applySort('analysis_date', sortOrder === 'desc' ? 'asc' : 'desc');
      return;
    }
    applySort('analysis_date', 'desc');
  };

  const toggleActualSort = () => {
    if (sortBy === 'actual_return_pct') {
      applySort('actual_return_pct', sortOrder === 'desc' ? 'asc' : 'desc');
      return;
    }
    applySort('actual_return_pct', 'desc');
  };

  const toggleScoreSort = () => {
    if (sortBy === 'sentiment_score') {
      applySort('sentiment_score', sortOrder === 'desc' ? 'asc' : 'desc');
      return;
    }
    applySort('sentiment_score', 'desc');
  };

  // Pagination
  const totalPages = Math.ceil(totalResults / pageSize);
  const handlePageChange = (page: number) => {
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    fetchResults(
      page,
      codeFilter.trim() || undefined,
      triggerSourceFilter,
      windowDays,
      analysisDateFrom,
      analysisDateTo,
      sortBy,
      sortOrder,
    );
  };

  return (
    <div className="min-h-full flex flex-col rounded-[1.5rem] bg-transparent">
      {/* Header */}
      <header className="flex-shrink-0 border-b border-white/5 px-3 py-3 sm:px-4">
        <div className="flex max-w-6xl flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="label-uppercase">结果筛选</span>
            <span className="text-xs text-muted-text">用于查询结果与统计卡片</span>
          </div>
          <div className="flex flex-wrap items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.02] p-2.5">
            <div className="relative min-w-0 flex-[1_1_220px]">
              <input
                type="text"
                value={codeFilter}
                onChange={(e) => setCodeFilter(e.target.value.toUpperCase())}
                onKeyDown={handleKeyDown}
                placeholder="按股票代码筛选（留空=全部）"
                disabled={isRunning}
                className={BACKTEST_INPUT_CLASS}
              />
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap lg:w-40 lg:justify-between">
              <span className="text-xs text-muted-text">窗口</span>
              <input
                type="number"
                min={1}
                max={120}
                value={evalDays}
                onChange={(e) => setEvalDays(e.target.value)}
                placeholder="10天"
                disabled={isRunning}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-24 text-center tabular-nums`}
              />
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">来源</span>
              <select
                value={triggerSourceFilter}
                onChange={(e) => setTriggerSourceFilter(e.target.value as 'auto' | 'manual' | '')}
                disabled={isRunning}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-32 text-center tabular-nums`}
              >
                <option value="">全部</option>
                <option value="auto">自动</option>
                <option value="manual">手动</option>
              </select>
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">开始</span>
              <input
                type="date"
                aria-label="Analysis date from"
                value={analysisDateFrom}
                onChange={(e) => setAnalysisDateFrom(e.target.value)}
                onKeyDown={handleKeyDown}
                disabled={isRunning}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-40 text-center tabular-nums`}
              />
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">结束</span>
              <input
                type="date"
                aria-label="Analysis date to"
                value={analysisDateTo}
                onChange={(e) => setAnalysisDateTo(e.target.value)}
                onKeyDown={handleKeyDown}
                disabled={isRunning}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-40 text-center tabular-nums`}
              />
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">排序</span>
              <select
                value={`${sortBy}:${sortOrder}`}
                onChange={(e) => handleSortModeChange(e.target.value)}
                disabled={isRunning || isLoadingResults}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-48 text-center tabular-nums`}
              >
                <option value="analysis_date:desc">分析日期（新到旧）</option>
                <option value="analysis_date:asc">分析日期（旧到新）</option>
                <option value="actual_return_pct:desc">实际表现（高到低）</option>
                <option value="actual_return_pct:asc">实际表现（低到高）</option>
                <option value="sentiment_score:desc">分数（高到低）</option>
                <option value="sentiment_score:asc">分数（低到高）</option>
              </select>
            </div>
            <button
              type="button"
              onClick={handleShowNextDay}
              disabled={isLoadingResults || isLoadingPerf}
              className={`backtest-force-btn ${isNextDayValidation ? 'active' : ''}`}
            >
              <span className="dot" />
              次日验证
            </button>
            <button
              type="button"
              onClick={handleFilter}
              disabled={isLoadingResults}
              className="btn-secondary flex items-center gap-1.5 whitespace-nowrap"
            >
              筛选
            </button>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="label-uppercase">运行回测</span>
            <span className="text-xs text-muted-text">用于生成/重算回测结果</span>
          </div>
          <div className="flex flex-wrap items-center gap-2 rounded-2xl border border-cyan-400/15 bg-cyan-500/[0.04] p-2.5">
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">模式</span>
              <select
                value={runFilterMode}
                onChange={(e) => setRunFilterMode(e.target.value as BacktestFilterMode)}
                disabled={isRunning}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-44 text-center tabular-nums`}
              >
                <option value="all">全部</option>
                <option value="signal">买持信号</option>
                <option value="score">评分</option>
                <option value="signal_and_score">信号+评分</option>
              </select>
            </div>
            {(runFilterMode === 'score' || runFilterMode === 'signal_and_score') && (
              <>
                <div className="flex items-center gap-2 whitespace-nowrap">
                  <span className="text-xs text-muted-text">评分≥</span>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    value={scoreMin}
                    onChange={(e) => setScoreMin(e.target.value)}
                    disabled={isRunning}
                    className={`${BACKTEST_COMPACT_INPUT_CLASS} w-20 text-center tabular-nums`}
                  />
                </div>
                <div className="flex items-center gap-2 whitespace-nowrap">
                  <span className="text-xs text-muted-text">评分≤</span>
                  <input
                    type="number"
                    min={0}
                    max={100}
                    value={scoreMax}
                    onChange={(e) => setScoreMax(e.target.value)}
                    disabled={isRunning}
                    className={`${BACKTEST_COMPACT_INPUT_CLASS} w-20 text-center tabular-nums`}
                  />
                </div>
              </>
            )}
            <button
              type="button"
              onClick={() => setForceRerun(!forceRerun)}
              disabled={isRunning}
              className={`backtest-force-btn ${forceRerun ? 'active' : ''}`}
            >
              <span className="dot" />
              强制重算
            </button>
            <button
              type="button"
              onClick={handleRun}
              disabled={isRunning}
              className="btn-primary flex items-center gap-1.5 whitespace-nowrap"
            >
              {isRunning ? (
                <>
                  <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  运行中...
                </>
              ) : (
                '运行回测'
              )}
            </button>
          </div>
        </div>
        {runResult && (
          <div className="mt-2 max-w-4xl">
            <RunSummary data={runResult} />
          </div>
        )}
        {runError && (
          <ApiErrorAlert error={runError} className="mt-2 max-w-4xl" />
        )}
        <p className="mt-2 text-xs text-muted-text">
          {isNextDayValidation
            ? '次日验证模式：对比 AI 预测与下一交易日收盘结果。'
            : '将窗口设为 1，可查看 AI 建议与下一交易日收盘的对照结果。'}
        </p>
      </header>

      {/* Main content */}
      <main className="flex min-h-0 flex-1 flex-col gap-3 overflow-hidden p-3 lg:flex-row">
        {/* Left sidebar - Performance */}
        <div className="flex max-h-[38vh] flex-col gap-3 overflow-y-auto lg:max-h-none lg:w-60 lg:flex-shrink-0">
          {isLoadingPerf ? (
            <div className="flex items-center justify-center py-8">
              <div className="backtest-spinner sm" />
            </div>
          ) : overallPerf ? (
            <PerformanceCard metrics={overallPerf} title={`总览（${triggerSourceLabel(triggerSourceFilter)}）`} />
          ) : (
            <EmptyState
              title="暂无统计"
              description="先运行回测后，再查看总体表现指标。"
              className="h-full min-h-[12rem] border-dashed bg-card/45 shadow-none"
            />
          )}

          {stockPerf && (
            <PerformanceCard metrics={stockPerf} title={`个股（${stockPerf.code || codeFilter}）`} />
          )}
        </div>

        {/* Right content - Results table */}
        <section className="min-h-0 flex-1 overflow-y-auto">
          {pageError ? (
            <ApiErrorAlert error={pageError} className="mb-3" />
          ) : null}
          {isLoadingResults ? (
            <div className="flex flex-col items-center justify-center h-64">
              <div className="backtest-spinner md" />
              <p className="mt-3 text-secondary-text text-sm">正在加载结果...</p>
            </div>
          ) : results.length === 0 ? (
            <EmptyState
              title="暂无结果"
              description="运行回测后可查看历史建议准确性。"
              className="backtest-empty-state border-dashed"
              icon={(
                <svg className="h-6 w-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                </svg>
              )}
            />
          ) : (
            <div className="animate-fade-in">
              <div className="backtest-table-toolbar">
                <div className="backtest-table-toolbar-meta">
                  <span className="label-uppercase">{isNextDayValidation ? '次日验证' : '结果集'}</span>
                  <span className="text-xs text-secondary-text">
                    {codeFilter.trim() ? `股票 ${codeFilter.trim()}` : '全部股票'}
                    {` · 来源 ${triggerSourceLabel(triggerSourceFilter)}`}
                    {evalDays ? ` · 窗口 ${evalDays} 天` : ''}
                    {analysisDateFrom ? ` · 从 ${analysisDateFrom}` : ''}
                    {analysisDateTo ? ` · 到 ${analysisDateTo}` : ''}
                  </span>
                </div>
                <span className="backtest-table-scroll-hint">小屏可左右滚动查看</span>
              </div>
              <div className="backtest-table-wrapper">
                <table className="backtest-table min-w-[840px] w-full text-sm">
                  <thead className="backtest-table-head">
                    <tr className="text-left">
                      <th className="backtest-table-head-cell">股票</th>
                      <th className="backtest-table-head-cell">
                        <button
                          type="button"
                          onClick={toggleAnalysisDateSort}
                          className="inline-flex items-center gap-1 text-left"
                          aria-label="按分析日期排序"
                        >
                          <span>分析日期</span>
                          <span className="text-xs text-muted-text">
                            {sortBy === 'analysis_date' ? (sortOrder === 'desc' ? '↓' : '↑') : '↕'}
                          </span>
                        </button>
                      </th>
                      <th className="backtest-table-head-cell w-[260px] min-w-[260px] max-w-[260px]">AI 预测</th>
                      <th className="backtest-table-head-cell">
                        <button
                          type="button"
                          onClick={toggleScoreSort}
                          className="inline-flex items-center gap-1 text-left"
                          aria-label="按分数排序"
                        >
                          <span>分数</span>
                          <span className="text-xs text-muted-text">
                            {sortBy === 'sentiment_score' ? (sortOrder === 'desc' ? '↓' : '↑') : '↕'}
                          </span>
                        </button>
                      </th>
                      <th className="backtest-table-head-cell">
                        <button
                          type="button"
                          onClick={toggleActualSort}
                          className="inline-flex items-center gap-1 text-left"
                          aria-label="按实际表现排序"
                        >
                          <span>{showNextDayActualColumns ? '实际表现' : '窗口收益'}</span>
                          <span className="text-xs text-muted-text">
                            {sortBy === 'actual_return_pct' ? (sortOrder === 'desc' ? '↓' : '↑') : '↕'}
                          </span>
                        </button>
                      </th>
                      <th className="backtest-table-head-cell">
                        {showNextDayActualColumns ? '准确度' : '方向匹配'}
                      </th>
                      <th className="backtest-table-head-cell">结果</th>
                      <th className="backtest-table-head-cell">状态</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((row) => (
                      <tr
                        key={row.analysisHistoryId}
                        className="backtest-table-row"
                      >
                        <td className="backtest-table-cell backtest-table-code">
                          <div className="flex flex-col">
                            <span>{row.code}</span>
                            <span className="text-xs text-muted-text">{row.stockName || '--'}</span>
                          </div>
                        </td>
                        <td className="backtest-table-cell text-secondary-text">{row.analysisDate || '--'}</td>
                        <td className="backtest-table-cell w-[260px] min-w-[260px] max-w-[260px] overflow-hidden text-foreground">
                          {(row.trendPrediction || row.operationAdvice) ? (
                            <Tooltip
                              content={(
                                <div className="max-w-[28rem] whitespace-normal break-words">
                                  {[row.trendPrediction, row.operationAdvice].filter(Boolean).join(' / ')}
                                </div>
                              )}
                              focusable
                              className="w-full min-w-0"
                            >
                              <div className="flex min-w-0 w-full flex-col gap-1 overflow-hidden">
                                <span className="block w-full truncate">{row.trendPrediction || '--'}</span>
                                <span className="block w-full truncate text-xs text-secondary-text">{row.operationAdvice || '--'}</span>
                              </div>
                            </Tooltip>
                          ) : (
                            '--'
                          )}
                        </td>
                        <td className="backtest-table-cell">{scoreBadge(row.sentimentScore)}</td>
                        <td className="backtest-table-cell">
                          <div className="flex items-center gap-2">
                            {actualMovementBadge(row.actualMovement)}
                            <span className={
                              row.actualReturnPct != null
                                ? row.actualReturnPct > 0 ? 'text-success' : row.actualReturnPct < 0 ? 'text-danger' : 'text-secondary-text'
                                : 'text-muted-text'
                            }>
                              {pct2(row.actualReturnPct)}
                            </span>
                          </div>
                        </td>
                        <td className="backtest-table-cell">
                          <span className="flex items-center gap-2">
                            {boolIcon(row.directionCorrect)}
                            <span className="text-muted-text">{directionExpectedLabel(row.directionExpected)}</span>
                          </span>
                        </td>
                        <td className="backtest-table-cell">{outcomeBadge(row.outcome)}</td>
                        <td className="backtest-table-cell">{statusBadge(row.evalStatus)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              <div className="mt-4">
                <Pagination
                  currentPage={currentPage}
                  totalPages={totalPages}
                  onPageChange={handlePageChange}
                />
              </div>

              <p className="text-xs text-muted-text text-center mt-2">
                共 {totalResults} 条 · 第 {currentPage}/{Math.max(totalPages, 1)} 页
              </p>
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default BacktestPage;
