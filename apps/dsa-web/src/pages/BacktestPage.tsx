import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { Check, Minus, X } from 'lucide-react';
import { backtestApi } from '../api/backtest';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import { ApiErrorAlert, Card, Badge, EmptyState, Pagination, StatusDot, Tooltip } from '../components/common';
import { useUiLanguage } from '../contexts/UiLanguageContext';
import { formatUiText, type UiLanguage } from '../i18n/uiText';
import {
  BACKTEST_DIRECTION_EXPECTED_LABELS,
  BACKTEST_MOVEMENT_LABELS,
  BACKTEST_OUTCOME_LABELS,
  BACKTEST_PHASE_FILTER_OPTIONS,
  BACKTEST_PHASE_LABELS,
  BACKTEST_STATUS_LABELS,
  BACKTEST_TEXT,
} from '../locales/featureText';
import type {
  BacktestResultItem,
  BacktestRunResponse,
  PerformanceMetrics,
  BacktestPhaseFilter,
} from '../types/backtest';
import { buildDecisionActionLabelMap, getDecisionActionLabel } from '../utils/decisionAction';
import { getMarketPhaseSummaryLabel } from '../utils/marketPhase';

type BacktestFilterMode = 'all' | 'signal' | 'score' | 'signal_and_score';
type BacktestSortBy = 'analysis_date' | 'actual_return_pct' | 'sentiment_score';

const BACKTEST_INPUT_CLASS =
  'input-surface input-focus-glow h-11 w-full rounded-xl border bg-transparent px-4 text-sm transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';
const BACKTEST_COMPACT_INPUT_CLASS =
  'input-surface input-focus-glow h-10 rounded-xl border bg-transparent px-3 py-2 text-xs transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';

type BacktestText = (typeof BACKTEST_TEXT)[UiLanguage];

// ============ Helpers ============

function pct(value?: number | null): string {
  if (value == null) return '--';
  return `${value.toFixed(1)}%`;
}

function phaseLabel(row: BacktestResultItem, language: UiLanguage): string {
  const label = getMarketPhaseSummaryLabel(row.marketPhaseSummary, language);
  if (label) {
    return label
      .replace('市场阶段: ', '')
      .replace('市场阶段：', '')
      .replace('Market phase: ', '');
  }
  return (row.marketPhase ? BACKTEST_PHASE_LABELS[language][row.marketPhase] : undefined) || row.marketPhase || '--';
}



function labelFromMap(value: string | null | undefined, labels: Record<string, string>): string {
  if (!value) return '--';
  return labels[value] ?? value;
}

function pct2(value?: number | null): string {
  if (value == null) return '--';
  return `${value.toFixed(2)}%`;
}

function outcomeBadge(outcome: string | undefined, language: UiLanguage) {
  const labels = BACKTEST_OUTCOME_LABELS[language];
  if (!outcome) return <Badge variant="default">--</Badge>;
  switch (outcome) {
    case 'win':
      return <Badge variant="success" glow>{labels.win}</Badge>;
    case 'loss':
      return <Badge variant="danger" glow>{labels.loss}</Badge>;
    case 'neutral':
      return <Badge variant="warning">{labels.neutral}</Badge>;
    default:
      return <Badge variant="default">{outcome}</Badge>;
  }
}

function statusBadge(status: string, language: UiLanguage) {
  const labels = BACKTEST_STATUS_LABELS[language];
  switch (status) {
    case 'completed':
      return <Badge variant="success">{labels.completed}</Badge>;
    case 'insufficient':
    case 'insufficient_data':
      return <Badge variant="warning">{labels.insufficient}</Badge>;
    case 'error':
      return <Badge variant="danger">{labels.error}</Badge>;
    default:
      return <Badge variant="default">{status}</Badge>;
  }
}

function actualMovementBadge(movement: string | null | undefined, language: UiLanguage) {
  const labels = BACKTEST_MOVEMENT_LABELS[language];
  switch (movement) {
    case 'up':
      return <Badge variant="success">{labels.up}</Badge>;
    case 'down':
      return <Badge variant="danger">{labels.down}</Badge>;
    case 'flat':
      return <Badge variant="warning">{labels.flat}</Badge>;
    default:
      return <Badge variant="default">--</Badge>;
  }
}

function boolIcon(value: boolean | null | undefined, text: BacktestText) {
  if (value === true) {
    return (
      <span
        className="backtest-status-chip backtest-status-chip-success"
        aria-label={text.yes}
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
        aria-label={text.no}
      >
        <StatusDot tone="danger" className="backtest-status-chip-dot" />
        <X className="h-3.5 w-3.5" />
      </span>
    );
  }

  return (
    <span
      className="backtest-status-chip backtest-status-chip-neutral"
      aria-label={text.unknown}
    >
      <StatusDot tone="neutral" className="backtest-status-chip-dot" />
      <Minus className="h-3.5 w-3.5" />
    </span>
  );
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

function phaseBreakdownText(metrics: PerformanceMetrics, language: UiLanguage): string | null {
  const breakdown = metrics.diagnostics?.phaseBreakdown;
  if (!breakdown || typeof breakdown !== 'object') return null;
  const item = breakdown as Record<string, unknown>;
  const phaseLabels = BACKTEST_PHASE_LABELS[language];
  const parts = [
    [phaseLabels.premarket, item.premarket],
    [phaseLabels.intraday, item.intraday],
    [phaseLabels.postmarket, item.postmarket],
    [phaseLabels.unknown, item.unknown],
  ]
    .map(([label, value]) => `${label} ${Number(value || 0)}`)
    .join(' / ');
  return parts;
}

// ============ Performance Card ============

const PerformanceCard: React.FC<{ metrics: PerformanceMetrics; title: string; language: UiLanguage }> = ({ metrics, title, language }) => {
  const text = BACKTEST_TEXT[language];
  const phaseText = phaseBreakdownText(metrics, language);
  return (
    <Card variant="gradient" padding="md" className="animate-fade-in">
      <div className="mb-3">
        <span className="label-uppercase">{title}</span>
      </div>
      <MetricRow label={text.directionAccuracy} value={pct(metrics.directionAccuracyPct)} accent />
      <MetricRow label={text.winRate} value={pct(metrics.winRatePct)} accent />
      <MetricRow label={text.avgSimulatedReturn} value={pct(metrics.avgSimulatedReturnPct)} />
      <MetricRow label={text.avgStockReturn} value={pct(metrics.avgStockReturnPct)} />
      <MetricRow label={text.stopLossTriggerRate} value={pct(metrics.stopLossTriggerRate)} />
      <MetricRow label={text.takeProfitTriggerRate} value={pct(metrics.takeProfitTriggerRate)} />
      <MetricRow label={text.avgDaysToFirstHit} value={metrics.avgDaysToFirstHit != null ? metrics.avgDaysToFirstHit.toFixed(1) : '--'} />
      <div className="backtest-metric-footer">
        <span className="text-xs text-muted-text">{text.evaluationCount}</span>
        <span className="text-xs text-secondary-text font-mono">
          {Number(metrics.completedCount)} / {Number(metrics.totalEvaluations)}
        </span>
      </div>
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-text">{text.outcomeSummary}</span>
        <span className="text-xs font-mono">
          <span className="text-success">{metrics.winCount}</span>
          {' / '}
          <span className="text-danger">{metrics.lossCount}</span>
          {' / '}
          <span className="text-warning">{metrics.neutralCount}</span>
        </span>
      </div>
      {phaseText ? (
        <div className="mt-3 border-t border-white/10 pt-2 text-xs text-muted-text">
          {formatUiText(text.phaseDistribution, { text: phaseText })}
        </div>
      ) : null}
    </Card>
  );
};

// ============ Run Summary ============

const RunSummary: React.FC<{ data: BacktestRunResponse; language: UiLanguage }> = ({ data, language }) => {
  const text = BACKTEST_TEXT[language];
  return (
    <div className="backtest-summary animate-fade-in">
      <span className="label">{text.processed} <span className="value">{data.processed}</span></span>
      <span className="label">{text.saved} <span className="value primary">{data.saved}</span></span>
      <span className="label">{text.completed} <span className="value success">{data.completed}</span></span>
      <span className="label">{text.insufficient} <span className="value warning">{data.insufficient}</span></span>
      {data.errors > 0 && (
        <span className="label">{text.errors} <span className="value danger">{data.errors}</span></span>
      )}
    </div>
  );
};

// ============ Main Page ============

const BacktestPage: React.FC = () => {
  const { language, t } = useUiLanguage();
  const text = BACKTEST_TEXT[language];
  const phaseFilterOptions = BACKTEST_PHASE_FILTER_OPTIONS[language];
  const actionLabels = buildDecisionActionLabelMap(t);

  const triggerSourceLabel = (value: 'auto' | 'manual' | '') => {
    if (value === 'auto') return language === 'en' ? 'Auto' : '自动';
    if (value === 'manual') return language === 'en' ? 'Manual' : '手动';
    return language === 'en' ? 'All' : '全部';
  };

  useEffect(() => {
    document.title = text.documentTitle;
  }, [text.documentTitle]);
  const [codeFilter, setCodeFilter] = useState('');
  const [triggerSourceFilter, setTriggerSourceFilter] = useState<'auto' | 'manual' | ''>('');
  const [analysisDateFrom, setAnalysisDateFrom] = useState('');
  const [analysisDateTo, setAnalysisDateTo] = useState('');
  const [sortBy, setSortBy] = useState<BacktestSortBy>('analysis_date');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [phaseFilter, setPhaseFilter] = useState<BacktestPhaseFilter>('all');
  const [evalDays, setEvalDays] = useState('');
  const [forceRerun, setForceRerun] = useState(false);
  const [runFilterMode, setRunFilterMode] = useState<BacktestFilterMode>('all');
  const [scoreMin, setScoreMin] = useState('');
  const [scoreMax, setScoreMax] = useState('');
  const [isRunning, setIsRunning] = useState(false);
  const [runResult, setRunResult] = useState<BacktestRunResponse | null>(null);
  const [runError, setRunError] = useState<ParsedApiError | null>(null);
  const [runHint, setRunHint] = useState<string>('');
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
  const effectiveWindowDays = parseEvalWindowDays(evalDays) ?? overallPerf?.evalWindowDays;
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
    phase: BacktestPhaseFilter = 'all',
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
        analysisPhase: phase && phase !== 'all' ? phase : undefined,
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
    phase?: BacktestPhaseFilter,
  ) => {
    setIsLoadingPerf(true);
    try {
      const overall = await backtestApi.getOverallPerformance({
        triggerSource: triggerSource || undefined,
        evalWindowDays: windowDays,
        analysisDateFrom: startDate || undefined,
        analysisDateTo: endDate || undefined,
        analysisPhase: phase && phase !== 'all' ? phase : undefined,
      });
      setOverallPerf(overall);

      if (code) {
        const stock = await backtestApi.getStockPerformance(code, {
          triggerSource: triggerSource || undefined,
          evalWindowDays: windowDays,
          analysisDateFrom: startDate || undefined,
          analysisDateTo: endDate || undefined,
          analysisPhase: phase && phase !== 'all' ? phase : undefined,
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
      fetchResults(1, undefined, '', windowDays, undefined, undefined, sortBy, sortOrder, 'all');
    };
    init();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Run backtest
  const handleRun = async () => {
    setIsRunning(true);
    setRunResult(null);
    setRunError(null);
    setRunHint('');
    try {
      const code = codeFilter.trim() || undefined;
      const evalWindowDays = evalDays ? parseInt(evalDays, 10) : undefined;
      const useSignalFilter = runFilterMode === 'signal' || runFilterMode === 'signal_and_score';
      const useScoreFilter = runFilterMode === 'score' || runFilterMode === 'signal_and_score';
      const result = await backtestApi.run({
        code,
        force: forceRerun || undefined,
        minAgeDays: forceRerun ? 0 : undefined,
        evalWindowDays,
        allowedCategories: useSignalFilter ? ['BUY', 'HOLD'] : undefined,
        sentimentScoreMin: useScoreFilter && scoreMin !== '' ? parseInt(scoreMin, 10) : undefined,
        sentimentScoreMax: useScoreFilter && scoreMax !== '' ? parseInt(scoreMax, 10) : undefined,
      });
      if ('taskId' in result) {
        throw new Error('Unexpected async backtest task response');
      }
      setRunResult(result);
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
        phaseFilter,
      );
      fetchPerformance(
        codeFilter.trim() || undefined,
        triggerSourceFilter,
        evalWindowDays,
        analysisDateFrom,
        analysisDateTo,
        phaseFilter,
      );
    } catch (err) {
      setRunError(getParsedApiError(err));
      setRunHint('');
    } finally {
      setIsRunning(false);
    }
  };

  // Filter by code
  const handleFilter = () => {
    const code = normalizeBacktestCode(codeFilter);
    const windowDays = parseEvalWindowDays(evalDays);
    setCurrentPage(1);
    fetchResults(1, code, triggerSourceFilter, windowDays, analysisDateFrom, analysisDateTo, sortBy, sortOrder, phaseFilter);
    fetchPerformance(code, triggerSourceFilter, windowDays, analysisDateFrom, analysisDateTo, phaseFilter);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleFilter();
    }
  };

  const handleShowNextDay = () => {
    const code = normalizeBacktestCode(codeFilter);
    setEvalDays('1');
    setCurrentPage(1);
    fetchResults(1, code, triggerSourceFilter, 1, analysisDateFrom, analysisDateTo, sortBy, sortOrder, phaseFilter);
    fetchPerformance(code, triggerSourceFilter, 1, analysisDateFrom, analysisDateTo, phaseFilter);
  };

  const applySort = (nextSortBy: BacktestSortBy, nextSortOrder: 'asc' | 'desc') => {
    setSortBy(nextSortBy);
    setSortOrder(nextSortOrder);
    const code = codeFilter.trim() || undefined;
    const windowDays = evalDays ? parseInt(evalDays, 10) : undefined;
    setCurrentPage(1);
    fetchResults(1, code, triggerSourceFilter, windowDays, analysisDateFrom, analysisDateTo, nextSortBy, nextSortOrder, phaseFilter);
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
      phaseFilter,
    );
  };

  return (
    <div className="min-h-full flex flex-col rounded-[1.5rem] bg-transparent">
      {/* Header */}
      <header className="flex-shrink-0 border-b border-white/5 px-3 py-3 sm:px-4">
        <div className="flex max-w-6xl flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="label-uppercase">{text.resultsFilterTitle}</span>
            <span className="text-xs text-muted-text">{text.resultsFilterHint}</span>
          </div>
          <div className="flex flex-wrap items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.02] p-2.5">
            <div className="relative min-w-0 flex-[1_1_220px]">
              <input
                type="text"
                value={codeFilter}
                onChange={(e) => setCodeFilter(e.target.value.toUpperCase())}
                onKeyDown={handleKeyDown}
                placeholder={text.codePlaceholder}
                disabled={isRunning}
                className={BACKTEST_INPUT_CLASS}
              />
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap lg:w-40 lg:justify-between">
              <span className="text-xs text-muted-text">{text.evalWindow}</span>
              <input
                type="number"
                min={1}
                max={120}
                value={evalDays}
                onChange={(e) => setEvalDays(e.target.value)}
                placeholder="10"
                disabled={isRunning}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-24 text-center tabular-nums`}
              />
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">{text.phase}</span>
              <select
                value={phaseFilter}
                onChange={(e) => setPhaseFilter(e.target.value as BacktestPhaseFilter)}
                disabled={isRunning || isLoadingResults}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-28`}
              >
                {phaseFilterOptions.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </div>
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">{text.sourceLabel}</span>
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
              <span className="text-xs text-muted-text">{text.startDate}</span>
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
              <span className="text-xs text-muted-text">{text.endDate}</span>
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
              <span className="text-xs text-muted-text">{text.sortLabel}</span>
              <select
                value={`${sortBy}:${sortOrder}`}
                onChange={(e) => handleSortModeChange(e.target.value)}
                disabled={isRunning || isLoadingResults}
                className={`${BACKTEST_COMPACT_INPUT_CLASS} w-48 text-center tabular-nums`}
              >
                <option value="analysis_date:desc">{text.sortByAnalysisDateDesc}</option>
                <option value="analysis_date:asc">{text.sortByAnalysisDateAsc}</option>
                <option value="actual_return_pct:desc">{text.sortByActualDesc}</option>
                <option value="actual_return_pct:asc">{text.sortByActualAsc}</option>
                <option value="sentiment_score:desc">{text.sortByScoreDesc}</option>
                <option value="sentiment_score:asc">{text.sortByScoreAsc}</option>
              </select>
            </div>
            <button
              type="button"
              onClick={handleShowNextDay}
              disabled={isLoadingResults || isLoadingPerf}
              className={`backtest-force-btn ${isNextDayValidation ? 'active' : ''}`}
            >
              <span className="dot" />
              {text.nextDayValidation}
            </button>
            <button
              type="button"
              onClick={handleFilter}
              disabled={isLoadingResults}
              className="btn-secondary flex items-center gap-1.5 whitespace-nowrap"
            >
              {text.filter}
            </button>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="label-uppercase">{text.runBacktestTitle}</span>
            <span className="text-xs text-muted-text">{text.runBacktestHint}</span>
          </div>
          <div className="flex flex-wrap items-center gap-2 rounded-2xl border border-cyan-400/15 bg-cyan-500/[0.04] p-2.5">
            <div className="flex items-center gap-2 whitespace-nowrap">
              <span className="text-xs text-muted-text">{text.modeLabel}</span>
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
                  <span className="text-xs text-muted-text">{text.scoreMinLabel}</span>
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
                  <span className="text-xs text-muted-text">{text.scoreMaxLabel}</span>
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
              {text.forceRerun}
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
                  {text.running}
                </>
              ) : (
                text.runBacktest
              )}
            </button>
          </div>
        </div>
        {runResult && (
          <div className="mt-2 max-w-4xl">
            <RunSummary data={runResult} language={language} />
          </div>
        )}
        {runError && (
          <ApiErrorAlert error={runError} className="mt-2 max-w-4xl" />
        )}
        {!runError && runHint && (
          <p className="mt-2 text-xs text-secondary-text">{runHint}</p>
        )}
        <p className="mt-2 text-xs text-muted-text">
          {isNextDayValidation
            ? text.oneDayModeDescription
            : text.windowModeDescription}
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
            <PerformanceCard metrics={overallPerf} title={text.overallPerformance} language={language} />
          ) : (
            <EmptyState
              title={text.noMetricsTitle}
              description={text.noMetricsDescription}
              className="h-full min-h-[12rem] border-dashed bg-card/45 shadow-none"
            />
          )}

          {stockPerf && (
            <PerformanceCard metrics={stockPerf} title={`${stockPerf.code || codeFilter}`} language={language} />
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
              <p className="mt-3 text-secondary-text text-sm">{text.loadingResults}</p>
            </div>
          ) : results.length === 0 ? (
            <EmptyState
              title={text.noResultsTitle}
              description={text.noResultsDescription}
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
                  <span className="label-uppercase">{isNextDayValidation ? text.nextDayValidation : text.resultSet}</span>
                  <span className="text-xs text-secondary-text">
                    {codeFilter.trim() ? formatUiText(text.filteredStock, { code: codeFilter.trim() }) : text.allStocks}
                    {` · 来源 ${triggerSourceLabel(triggerSourceFilter)}`}
                    {evalDays ? ` · ${formatUiText(text.dayWindow, { days: evalDays })}` : ''}
                    {phaseFilter !== 'all' ? ` · ${phaseFilterOptions.find((item) => item.value === phaseFilter)?.label ?? phaseFilter}` : ''}
                    {analysisDateFrom ? ` · ${formatUiText(text.fromDate, { date: analysisDateFrom })}` : ''}
                    {analysisDateTo ? ` · ${formatUiText(text.toDate, { date: analysisDateTo })}` : ''}
                  </span>
                </div>
                <span className="backtest-table-scroll-hint">{text.scrollHint}</span>
              </div>
              <div className="backtest-table-wrapper">
                <table className="backtest-table min-w-[900px] w-full text-sm">
                  <thead className="backtest-table-head">
                    <tr className="text-left">
                      <th className="backtest-table-head-cell">{text.stock}</th>
                      <th className="backtest-table-head-cell">{text.phase}</th>
                      <th className="backtest-table-head-cell">
                        <button
                          type="button"
                          onClick={toggleAnalysisDateSort}
                          className="inline-flex items-center gap-1 text-left"
                          aria-label={text.sortByAnalysisDateAria}
                        >
                          <span>{text.analysisDate}</span>
                          <span className="text-xs text-muted-text">
                            {sortBy === 'analysis_date' ? (sortOrder === 'desc' ? '↓' : '↑') : '↕'}
                          </span>
                        </button>
                      </th>
                      <th className="backtest-table-head-cell w-[260px] min-w-[260px] max-w-[260px]">{text.aiPrediction}</th>
                      <th className="backtest-table-head-cell">
                        <button
                          type="button"
                          onClick={toggleScoreSort}
                          className="inline-flex items-center gap-1 text-left"
                          aria-label={text.sortByScoreAria}
                        >
                          <span>{text.scoreColumn}</span>
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
                          aria-label={text.sortByActualAria}
                        >
                          <span>{showNextDayActualColumns ? text.actualPerformance : text.windowReturn}</span>
                          <span className="text-xs text-muted-text">
                            {sortBy === 'actual_return_pct' ? (sortOrder === 'desc' ? '↓' : '↑') : '↕'}
                          </span>
                        </button>
                      </th>
                      <th className="backtest-table-head-cell">
                        {showNextDayActualColumns ? text.accuracy : text.directionMatch}
                      </th>
                      <th className="backtest-table-head-cell">{text.result}</th>
                      <th className="backtest-table-head-cell">{text.status}</th>
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
                        <td className="backtest-table-cell text-secondary-text">{phaseLabel(row, language)}</td>
                        <td className="backtest-table-cell text-secondary-text">{row.analysisDate || '--'}</td>
                        <td className="backtest-table-cell w-[260px] min-w-[260px] max-w-[260px] overflow-hidden text-foreground">
                          {(() => {
                            const actionLabel = getDecisionActionLabel(row.action, row.actionLabel, null, null, actionLabels);
                            const predictionParts = [actionLabel, row.trendPrediction, row.operationAdvice]
                              .filter((part): part is string => Boolean(part));
                            if (!predictionParts.length) return '--';
                            return (
                              <Tooltip
                                content={(
                                  <div className="max-w-[28rem] whitespace-normal break-words">
                                    {predictionParts.join(' / ')}
                                  </div>
                                )}
                                focusable
                                className="w-full min-w-0"
                              >
                                <div className="flex min-w-0 w-full flex-col gap-1 overflow-hidden">
                                  <span className="block w-full truncate">{actionLabel || row.trendPrediction || '--'}</span>
                                  {actionLabel && row.trendPrediction ? (
                                    <span className="block w-full truncate text-xs text-secondary-text">{row.trendPrediction}</span>
                                  ) : null}
                                  {row.operationAdvice ? (
                                    <span className="block w-full truncate text-xs text-secondary-text">{row.operationAdvice}</span>
                                  ) : null}
                                </div>
                              </Tooltip>
                            );
                          })()}
                        </td>
                        <td className="backtest-table-cell">{scoreBadge(row.sentimentScore)}</td>
                        <td className="backtest-table-cell">
                          <div className="flex items-center gap-2">
                            {actualMovementBadge(row.actualMovement, language)}
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
                            {boolIcon(row.directionCorrect, text)}
                            <span className="text-muted-text">{row.directionExpected ? labelFromMap(row.directionExpected, BACKTEST_DIRECTION_EXPECTED_LABELS[language]) : ''}</span>
                          </span>
                        </td>
                        <td className="backtest-table-cell">{outcomeBadge(row.outcome, language)}</td>
                        <td className="backtest-table-cell">{statusBadge(row.evalStatus, language)}</td>
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
                {formatUiText(text.totalPage, { total: totalResults, page: currentPage, pages: Math.max(totalPages, 1) })}
              </p>
            </div>
          )}
        </section>
      </main>
    </div>
  );
};

export default BacktestPage;
