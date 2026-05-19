import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Segmented, InputNumber, Switch, Table, Modal } from 'antd';
import { Play, Loader2, ChevronDown, ChevronRight } from 'lucide-react';
import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { AppPage, Card, StatCard, EmptyState, ApiErrorAlert } from '../components/common';
import { discoveryApi, type FactorOptimizeHistoryItem } from '../api/discovery';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';

type TabKey = 'intraday' | 'postmarket';

const TASK_KEY = 'factor_optimize_task';
const RESULT_KEY = 'factor_optimize_result';

const FACTOR_LABELS: Record<string, string> = {
  money_flow: '资金流向', margin: '融资融券', chip: '筹码分布',
  technical: '技术形态', limit: '涨跌停', fundamental: '基本面',
  institution_hold: '机构持股', profit_forecast: '盈利预测',
  buyback: '回购', insider_buy: '高管增持', broker_recommend: '券商推荐',
  popularity: '人气', hot_money: '游资', performance: '业绩',
  momentum: '动量', rebound: '反弹', sector: '板块', ma_entry: '均线',
  ranking_momentum: '排名动量', concept_heat: '概念热度',
  alpha042: '均值回归Alpha042', vwap_deviation: 'VWAP偏离',
  gap_reversal: '跳空反转', liquid_oversold: '流动性超卖',
  vwap_reversal: 'VWAP动量反转', gtja114: 'GTJA114',
};

const PHASE_LABELS: Record<string, string> = {
  starting: '初始化', screen: '因子筛选', preload: '数据预加载',
  tpe: 'TPE 搜索', guardrails: '护栏检查', done: '完成',
};

function pctNum(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

const FactorTuningPage: React.FC = () => {
  const isOwnerRef = useRef(false);
  const abortRef = useRef(false);
  const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const [mode, setMode] = useState<TabKey>('postmarket');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ParsedApiError | null>(null);

  /* params */
  const [optWindow, setOptWindow] = useState(60);
  const [nTrials, setNTrials] = useState(100);
  const [normalize, setNormalize] = useState(false);
  const [autoApply, setAutoApply] = useState(true);

  /* progress */
  const [phase, setPhase] = useState('');
  const [statusMsg, setStatusMsg] = useState('');
  const [pTrials, setPTrials] = useState({ trial: 0, n_trials: 100, best_value: null as number | null });

  /* current weights & pipeline mode from .env */
  const [currentWeights, setCurrentWeights] = useState<Record<string, number>>({});
  const [weightsLoading, setWeightsLoading] = useState(false);

  /* 发现管线配置（运行时覆盖，持久化到服务端） */
  const [intradayPipeline, setIntradayPipeline] = useState(true);
  const [postmarketPipeline, setPostmarketPipeline] = useState(true);
  const [pipelineAlpha, setPipelineAlpha] = useState(0.3);
  const [pipelineSaving, setPipelineSaving] = useState(false);

  const usePipeline = mode === 'intraday' ? intradayPipeline : postmarketPipeline;
  const blendAlpha = pipelineAlpha;

  /* result */
  const [result, setResult] = useState<{
    report_path: string;
    recommendation: Record<string, number>;
    baseline: Record<string, number>;
    applied: boolean;
  } | null>(null);
  const [reportContent, setReportContent] = useState('');
  const [reportExpanded, setReportExpanded] = useState(false);

  /* confirmation modal */
  const [confirmModalOpen, setConfirmModalOpen] = useState(false);
  const [applyLoading, setApplyLoading] = useState(false);

  /* history apply */
  const [applyHistoryLoading, setApplyHistoryLoading] = useState<string | null>(null);

  /* history */
  const [history, setHistory] = useState<FactorOptimizeHistoryItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  /* load history */
  const loadHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const resp = await discoveryApi.getFactorOptimizeHistory();
      setHistory(resp.items);
    } catch {
      /* silent */
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  useEffect(() => { void loadHistory(); }, [loadHistory]);

  /* load current weights on mount / mode change */
  useEffect(() => {
    let cancelled = false;
    setCurrentWeights({});
    setWeightsLoading(true);
    discoveryApi.getFactorWeights(mode).then((data) => {
      if (cancelled) return;
      setCurrentWeights(data.weights);
      setWeightsLoading(false);
    }).catch(() => {
      if (!cancelled) setWeightsLoading(false);
    });
    return () => { cancelled = true; };
  }, [mode]);

  /* silently refresh weights after optimization completes */
  const refreshWeights = useCallback(() => {
    discoveryApi.getFactorWeights(mode).then((data) => {
      setCurrentWeights(data.weights);
    }).catch(() => { /* silent */ });
  }, [mode]);

  /* load pipeline runtime config from server (intraday/postmarket toggles + alpha) */
  useEffect(() => {
    let cancelled = false;
    discoveryApi.getPipelineConfig().then((data) => {
      if (cancelled) return;
      if (data.intraday_pipeline_enabled != null) setIntradayPipeline(data.intraday_pipeline_enabled);
      if (data.postmarket_pipeline_enabled != null) setPostmarketPipeline(data.postmarket_pipeline_enabled);
      if (data.score_blend_alpha != null) setPipelineAlpha(data.score_blend_alpha);
    }).catch(() => { /* ignore */ });
    return () => { cancelled = true; };
  }, []);

  /* persist pipeline config to server (debounced by caller) */
  const savePipelineConfig = useCallback(async (intra: boolean, post: boolean, alpha: number) => {
    setPipelineSaving(true);
    try {
      await discoveryApi.setPipelineConfig({
        intraday_pipeline_enabled: intra,
        postmarket_pipeline_enabled: post,
        score_blend_alpha: alpha,
      });
      refreshWeights();
    } catch { /* ignore */ }
    finally { setPipelineSaving(false); }
  }, [refreshWeights]);

  /* resume polling on mount (cross-route) or when another tab starts a task */
  useEffect(() => {
    let active = true;
    const run = async () => {
      const raw = localStorage.getItem(TASK_KEY);
      if (!raw || isOwnerRef.current) return;
      let taskId = '';
      let taskMode = '';
      try {
        const parsed = JSON.parse(raw);
        taskId = parsed.task_id;
        taskMode = parsed.mode || '';
      } catch { return; }
      if (!taskId) return;

      if (taskMode && taskMode !== mode) {
        setMode(taskMode as TabKey);
      }

      setLoading(true);
      setPhase('starting');
      setStatusMsg('恢复轮询…');
      setError(null);
      setResult(null);
      setReportContent('');
      setReportExpanded(false);

      try {
        const poll = async () => {
          if (!active) throw new Error('aborted');
          const status = await discoveryApi.getFactorOptimizeStatus(taskId);
          if (!active) throw new Error('aborted');
          setPhase(status.phase);
          setStatusMsg(status.status_message || '');
          setPTrials(status.progress);
          if (status.status === 'completed' && status.result) {
            localStorage.setItem(RESULT_KEY, JSON.stringify({ result: status.result, mode: taskMode }));
            localStorage.removeItem(TASK_KEY);
            setResult(status.result);
            void loadHistory();
            refreshWeights();
            return;
          }
          if (status.status === 'failed') {
            setError(getParsedApiError(new Error(status.error || '优化失败')));
            localStorage.removeItem(TASK_KEY);
            return;
          }
          await new Promise((r) => setTimeout(r, 1000));
          return poll();
        };
        await poll();
      } catch (e) {
        if (active && (e as Error).message !== 'aborted') {
          setError(getParsedApiError(e));
          localStorage.removeItem(TASK_KEY);
        }
      } finally {
        if (active) {
          setLoading(false);
          setPhase('');
          setStatusMsg('');
        }
      }
    };

    run();

    if (!localStorage.getItem(TASK_KEY)) {
      const cachedRaw = localStorage.getItem(RESULT_KEY);
      if (cachedRaw) {
        try {
          const cached = JSON.parse(cachedRaw);
          if (cached.result && cached.mode === mode) {
            setResult(cached.result);
          }
        } catch { /* ignore */ }
      }
    }

    return () => { active = false; };
  }, [mode, loadHistory, refreshWeights]);

  const autoApplyRef = useRef(autoApply);
  autoApplyRef.current = autoApply;

  /* poll */
  const startPoll = useCallback((taskId: string) => {
    const poll = async () => {
      if (abortRef.current) return;
      try {
        const status = await discoveryApi.getFactorOptimizeStatus(taskId);
        if (abortRef.current) return;
        setPhase(status.phase);
        setStatusMsg(status.status_message || '');
        setPTrials(status.progress);
        if (status.status === 'completed' && status.result) {
          localStorage.setItem(RESULT_KEY, JSON.stringify({ result: status.result, mode }));
          localStorage.removeItem(TASK_KEY);
          setLoading(false);
          setPhase('');
          setStatusMsg('');
          void loadHistory();
          if (autoApplyRef.current && status.result.recommendation && Object.keys(status.result.recommendation).length > 0) {
            setResult(status.result);
            setConfirmModalOpen(true);
          } else {
            setResult(status.result);
            refreshWeights();
          }
          return;
        }
        if (status.status === 'failed') {
          setError(getParsedApiError(new Error(status.error || '优化失败')));
          setLoading(false);
          setPhase('');
          setStatusMsg('');
          localStorage.removeItem(TASK_KEY);
          return;
        }
        pollTimerRef.current = setTimeout(poll, 1000);
      } catch (e) {
        if (!abortRef.current) {
          setError(getParsedApiError(e));
          setLoading(false);
          setPhase('');
          setStatusMsg('');
          localStorage.removeItem(TASK_KEY);
        }
      }
    };
    poll();
  }, [loadHistory, refreshWeights, mode]);

  /* cleanup */
  useEffect(() => {
    return () => {
      abortRef.current = true;
      if (pollTimerRef.current) clearTimeout(pollTimerRef.current);
    };
  }, []);

  /* cross-tab sync */
  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === TASK_KEY) {
        if (!localStorage.getItem(TASK_KEY)) {
          // 任务被其他标签页清除 → 恢复缓存结果
          if (!isOwnerRef.current) {
            setLoading(false);
            setPhase('');
            setStatusMsg('');
            const cachedRaw = localStorage.getItem(RESULT_KEY);
            if (cachedRaw) {
              try {
                const cached = JSON.parse(cachedRaw);
                if (cached.result) {
                  setResult(cached.result);
                  refreshWeights();
                }
              } catch { /* ignore */ }
            }
          }
        }
      }
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, [refreshWeights]);

  /* start */
  const handleStart = useCallback(async () => {
    isOwnerRef.current = true;
    abortRef.current = false;
    setLoading(true);
    setError(null);
    setPhase('starting');
    setStatusMsg('提交优化任务…');
    setResult(null);
    setReportContent('');
    setReportExpanded(false);
    try {
      const resp = await discoveryApi.runFactorOptimize({
        mode,
        window: optWindow,
        normalize,
        n_trials: nTrials,
        auto_apply: autoApply,
      });
      localStorage.setItem(TASK_KEY, JSON.stringify({ task_id: resp.task_id, mode, started_at: Date.now() }));
      localStorage.removeItem(RESULT_KEY);
      startPoll(resp.task_id);
    } catch (e) {
      setLoading(false);
      setPhase('');
      setStatusMsg('');
      setError(getParsedApiError(e));
    }
  }, [mode, optWindow, normalize, nTrials, autoApply, startPoll]);

  /* confirm apply */
  const handleConfirmApply = useCallback(async () => {
    if (!result?.recommendation) return;
    setApplyLoading(true);
    try {
      await discoveryApi.applyFactorWeights(mode, result.recommendation, result.report_path || undefined);
      setResult((prev) => prev ? { ...prev, applied: true } : prev);
      // 同步更新 localStorage 缓存，避免切换模式后再切回时复活旧 applied=false
      try {
        const raw = localStorage.getItem(RESULT_KEY);
        if (raw) {
          const cached = JSON.parse(raw);
          if (cached.result && cached.mode === mode) {
            cached.result.applied = true;
            localStorage.setItem(RESULT_KEY, JSON.stringify(cached));
          }
        }
      } catch { /* ignore */ }
      setConfirmModalOpen(false);
      refreshWeights();
      void loadHistory();
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setApplyLoading(false);
    }
  }, [result, mode, refreshWeights, loadHistory]);

  const handleCancelApply = useCallback(() => {
    setConfirmModalOpen(false);
  }, []);

  /* apply from history */
  const handleApplyHistory = useCallback(async (item: FactorOptimizeHistoryItem) => {
    if (!item.recommendation || Object.keys(item.recommendation).length === 0) return;
    if (applyHistoryLoading) return;
    setApplyHistoryLoading(item.report_path);
    try {
      await discoveryApi.applyFactorWeights(
        item.mode === 'intraday' ? 'intraday' : 'postmarket',
        item.recommendation,
        item.report_path || undefined,
      );
      // 同步更新 localStorage 缓存
      try {
        const raw = localStorage.getItem(RESULT_KEY);
        if (raw) {
          const cached = JSON.parse(raw);
          if (cached.result && cached.mode === item.mode) {
            cached.result.applied = true;
            localStorage.setItem(RESULT_KEY, JSON.stringify(cached));
          }
        }
      } catch { /* ignore */ }
      void loadHistory();
      refreshWeights();
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setApplyHistoryLoading(null);
    }
  }, [applyHistoryLoading, loadHistory, refreshWeights]);

  /* reason text for weight changes */
  const reasonText = useCallback((change: number): string => {
    if (change > 5) return 'TPE 寻优建议大幅提升（因子贡献显著）';
    if (change > 0) return 'TPE 寻优建议适当提升';
    if (change === 0) return '权重不变';
    if (change > -5) return 'TPE 寻优建议适当降低（因子贡献较弱）';
    return 'TPE 寻优建议大幅降低（因子贡献不足）';
  }, []);

  const confirmColumns = [
    { title: '因子', dataIndex: 'label', key: 'label', width: 100 },
    { title: '原权重', dataIndex: 'oldWeight', key: 'oldWeight', width: 70 },
    {
      title: '新权重', dataIndex: 'newWeight', key: 'newWeight', width: 70,
      render: (_: unknown, r: { oldWeight: number; newWeight: number }) => {
        const changed = r.newWeight !== r.oldWeight;
        return <span className={changed ? 'font-semibold text-cyan' : ''}>{r.newWeight}</span>;
      },
    },
    {
      title: '变化', dataIndex: 'change', key: 'change', width: 60,
      render: (_: unknown, r: { change: number; oldWeight: number; newWeight: number }) => {
        if (r.oldWeight === r.newWeight) return <span className="text-tertiary-text">0</span>;
        const cls = r.change > 0 ? 'text-emerald-500' : 'text-red-400';
        return <span className={cls}>{r.change > 0 ? '+' : ''}{r.change}</span>;
      },
    },
    {
      title: '修改理由', dataIndex: 'change', key: 'reason',
      render: (_: unknown, r: { change: number }) => reasonText(r.change),
    },
  ];

  const reportLoadingRef = useRef(false);
  const reportPathRef = useRef('');

  /* load report — ref-based to avoid stale closure issues */
  const handleViewReportRef = useRef<(reportPath: string) => Promise<void>>(async () => {});
  handleViewReportRef.current = async (reportPath: string) => {
    if (!reportPath || reportLoadingRef.current) return;
    if (reportPath === reportPathRef.current) {
      setReportExpanded((prev) => !prev);
      return;
    }
    reportLoadingRef.current = true;
    setReportContent('');
    setReportExpanded(true);
    try {
      const resp = await discoveryApi.getFactorOptimizeReport(reportPath);
      reportPathRef.current = reportPath;
      setReportContent(resp.content);
    } catch (e) {
      setReportExpanded(false);
      setError(getParsedApiError(e));
    } finally {
      reportLoadingRef.current = false;
    }
  };

  const handleViewReport = useCallback((reportPath: string) => {
    void handleViewReportRef.current(reportPath);
  }, []);

  /* weight comparison table */
  const weightData = useMemo(() => {
    if (!result?.baseline) return [];
    const allFactors = new Set([
      ...Object.keys(result.baseline),
      ...Object.keys(result.recommendation),
    ]);
    return Array.from(allFactors).sort().map((fn) => ({
      key: fn,
      factor: fn,
      label: FACTOR_LABELS[fn] || fn,
      oldWeight: result.baseline[fn] ?? 0,
      newWeight: result.recommendation[fn] ?? result.baseline[fn] ?? 0,
      change: (result.recommendation[fn] ?? result.baseline[fn] ?? 0) - (result.baseline[fn] ?? 0),
    }));
  }, [result]);

  const weightColumns = [
    { title: '因子', dataIndex: 'label', key: 'label', width: 120 },
    { title: '原权重', dataIndex: 'oldWeight', key: 'oldWeight', width: 80 },
    {
      title: '新权重', dataIndex: 'newWeight', key: 'newWeight', width: 80,
      render: (_: unknown, r: { oldWeight: number; newWeight: number }) => {
        const changed = r.newWeight !== r.oldWeight;
        return <span className={changed ? 'font-semibold text-cyan' : ''}>{r.newWeight}</span>;
      },
    },
    {
      title: '变化', dataIndex: 'change', key: 'change', width: 80,
      render: (_: unknown, r: { change: number; oldWeight: number; newWeight: number }) => {
        const unchanged = r.oldWeight === r.newWeight;
        if (unchanged) return <span className="text-tertiary-text">0</span>;
        const cls = r.change > 0 ? 'text-emerald-500' : 'text-red-400';
        return <span className={cls}>{r.change > 0 ? '+' : ''}{r.change}</span>;
      },
    },
    {
      title: '状态', key: 'status', width: 80,
      render: (_: unknown, r: { oldWeight: number; newWeight: number }) => {
        if (r.oldWeight === r.newWeight) return <span className="text-tertiary-text">未变</span>;
        return <span className="text-cyan">已调整</span>;
      },
    },
  ];

  const historyColumns = [
    { title: '时间', dataIndex: 'timestamp', key: 'timestamp', width: 150,
      render: (_: unknown, r: FactorOptimizeHistoryItem) => {
        try {
          return new Date(r.timestamp).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
        } catch { return r.timestamp; }
      },
    },
    { title: '模式', dataIndex: 'mode', key: 'mode', width: 60,
      render: (_: unknown, r: FactorOptimizeHistoryItem) => r.mode === 'postmarket' ? '盘后' : '盘中',
    },
    { title: '修改因子数', dataIndex: 'changed_count', key: 'changed_count', width: 90 },
    {
      title: '已应用', dataIndex: 'applied', key: 'applied', width: 70,
      render: (_: unknown, r: FactorOptimizeHistoryItem) =>
        r.applied ? <span className="text-emerald-500">是</span> : <span className="text-tertiary-text">否</span>,
    },
    {
      title: '操作', key: 'actions', width: 160,
      render: (_: unknown, r: FactorOptimizeHistoryItem) => (
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="text-xs text-cyan hover:underline"
            onClick={() => handleViewReport(r.report_path)}
          >
            查看报告
          </button>
          {!r.applied && (
            <button
              type="button"
              className="text-xs text-emerald-500 hover:underline disabled:opacity-50"
              disabled={applyHistoryLoading === r.report_path}
              onClick={() => {
                if (window.confirm(`确认将以下推荐权重应用到 ${r.mode === 'intraday' ? '盘中' : '盘后'} 模式？`)) {
                  void handleApplyHistory(r);
                }
              }}
            >
              {applyHistoryLoading === r.report_path ? <Loader2 className="h-3 w-3 animate-spin inline" /> : '应用'}
            </button>
          )}
        </div>
      ),
    },
  ];

  const showProgress = loading && phase && phase !== 'done';
  const hasResult = !loading && result !== null;

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="flex flex-col lg:flex-row gap-5">
        {/* Left Panel */}
        <div className="lg:w-[260px] shrink-0 space-y-4">
          <Card>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="font-medium text-sm text-secondary-text">扫描模式</span>
                <button
                  type="button"
                  disabled={loading}
                  onClick={handleStart}
                  className="flex items-center gap-1.5 rounded-md bg-cyan px-3 py-1.5 text-xs font-medium text-white transition-opacity hover:opacity-90 disabled:opacity-50"
                >
                  {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                  {loading ? '运行中' : '开始优化'}
                </button>
              </div>
              <Segmented
                block
                value={mode}
                onChange={(v) => setMode(v as TabKey)}
                options={[
                  { label: '盘后', value: 'postmarket' },
                  { label: '盘中', value: 'intraday' },
                ]}
              />

              {showProgress && (
                <div className="space-y-2 text-xs text-tertiary-text pt-2 border-t border-border">
                  <div className="flex items-center justify-between">
                    <span>阶段</span>
                    <span className="text-foreground/80">{PHASE_LABELS[phase] || phase}</span>
                  </div>
                  {phase === 'tpe' && (
                    <>
                      <div className="flex items-center justify-between">
                        <span>试验</span>
                        <span className="text-foreground/80">{pTrials.trial} / {pTrials.n_trials}</span>
                      </div>
                      <div className="h-1.5 w-full overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700">
                        <div
                          className="h-full rounded-full bg-cyan transition-all duration-300"
                          style={{ width: `${pTrials.n_trials > 0 ? Math.min(100, (pTrials.trial / pTrials.n_trials) * 100) : 0}%` }}
                        />
                      </div>
                      {pTrials.best_value !== null && (
                        <div className="flex items-center justify-between">
                          <span>当前最优</span>
                          <span className="text-cyan font-medium">+{pctNum(pTrials.best_value)}</span>
                        </div>
                      )}
                    </>
                  )}
                  {statusMsg && (
                    <div className="text-tertiary-text italic">{statusMsg}</div>
                  )}
                </div>
              )}

              {hasResult && (
                <div className="pt-2 border-t border-border">
                  <div className="flex items-center gap-2 text-xs text-emerald-500">
                    <div className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
                    <span>优化完成</span>
                    {result.applied && <span className="text-tertiary-text">· 已应用</span>}
                  </div>
                </div>
              )}
            </div>
          </Card>

          <Card>
            <div className="space-y-2 overflow-hidden">
              <div className="font-medium text-sm text-secondary-text">当前权重</div>
              {weightsLoading || Object.keys(currentWeights).length === 0 ? (
                <div className="flex items-center justify-center py-4">
                  <Loader2 className="h-4 w-4 animate-spin text-tertiary-text" />
                </div>
              ) : (
                <>
                  <div className="max-h-[360px] overflow-y-auto space-y-1">
                    {Object.entries(currentWeights).sort((a, b) => b[1] - a[1]).map(([name, w]) => (
                      <div key={name} className="flex items-center justify-between text-xs">
                        <span className="text-tertiary-text truncate min-w-0 mr-2">{FACTOR_LABELS[name] || name}</span>
                        <span className="text-foreground/80 font-mono shrink-0 text-right">{w}</span>
                      </div>
                    ))}
                  </div>
                  <div className="text-[10px] text-tertiary-text pt-1 border-t border-border">
                    来源: .env 配置
                  </div>
                </>
              )}
            </div>
          </Card>

          <Card>
            <div className="space-y-4">
              <div className="font-medium text-sm text-secondary-text">优化参数</div>

              <div className="rounded-lg bg-gray-50 dark:bg-gray-800/50 px-3 py-2 space-y-1.5 text-xs">
                <div className="flex items-center justify-between">
                  <span className="text-tertiary-text">计算方式</span>
                  <span className={usePipeline ? 'text-cyan font-medium' : 'text-amber-500 font-medium'}>
                    {usePipeline ? '管线模式' : '纯因子模式'}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-tertiary-text">技术综合 α</span>
                  <span className="text-foreground/80 font-mono">{blendAlpha.toFixed(1)}</span>
                </div>
              </div>

              <div className="flex items-center justify-between">
                <span className="text-xs text-tertiary-text">回测窗口</span>
                <InputNumber
                  size="small"
                  min={20}
                  max={252}
                  value={optWindow}
                  onChange={(v) => setOptWindow(v ?? 60)}
                  className="w-[80px]"
                />
              </div>

              <div className="flex items-center justify-between">
                <span className="text-xs text-tertiary-text">TPE 试验次数</span>
                <InputNumber
                  size="small"
                  min={10}
                  max={500}
                  value={nTrials}
                  onChange={(v) => setNTrials(v ?? 100)}
                  className="w-[80px]"
                />
              </div>

              <div className="flex items-center justify-between">
                <span className="text-xs text-tertiary-text">自动应用</span>
                <Switch size="small" checked={autoApply} onChange={setAutoApply} />
              </div>

              <div className="flex items-center justify-between">
                <span className="text-xs text-tertiary-text">归一化</span>
                <Switch size="small" checked={normalize} onChange={setNormalize} />
              </div>
            </div>
          </Card>

          <Card>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="font-medium text-sm text-secondary-text">管线配置</span>
                {pipelineSaving && <span className="text-[11px] text-tertiary-text">保存中…</span>}
              </div>
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={usePipeline}
                  onChange={(e) => {
                    if (mode === 'intraday') {
                      setIntradayPipeline(e.target.checked);
                      savePipelineConfig(e.target.checked, postmarketPipeline, pipelineAlpha);
                    } else {
                      setPostmarketPipeline(e.target.checked);
                      savePipelineConfig(intradayPipeline, e.target.checked, pipelineAlpha);
                    }
                  }}
                  className="sr-only"
                />
                <span className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${usePipeline ? 'bg-cyan' : 'bg-gray-300'}`}>
                  <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${usePipeline ? 'translate-x-[18px]' : 'translate-x-[2px]'}`} />
                </span>
                <span className="text-xs text-foreground/70">{mode === 'intraday' ? '盘中管线' : '盘后管线'}</span>
              </label>
              <div className="space-y-1.5">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-foreground/70 whitespace-nowrap">
                    综合分混合 α = {pipelineAlpha.toFixed(2)}
                  </span>
                  <input
                    type="range"
                    min="0"
                    max="100"
                    value={Math.round(pipelineAlpha * 100)}
                    onChange={(e) => {
                      setPipelineAlpha(Number(e.target.value) / 100);
                    }}
                    onMouseUp={() => savePipelineConfig(intradayPipeline, postmarketPipeline, pipelineAlpha)}
                    onTouchEnd={() => savePipelineConfig(intradayPipeline, postmarketPipeline, pipelineAlpha)}
                    className="w-20 h-1 accent-cyan"
                  />
                </div>
                <div className="text-[11px] text-foreground/40">
                  factor&times;{pipelineAlpha.toFixed(2)} + tech&times;{(1 - pipelineAlpha).toFixed(2)}
                </div>
              </div>
            </div>
          </Card>

        </div>

        {/* Right Panel */}
        <div className="flex-1 min-w-0 space-y-4">
          {error && (
            <ApiErrorAlert error={error} />
          )}

          {/* Progress steps */}
          {showProgress && (
            <Card>
              <div className="space-y-4">
                <div className="font-medium text-sm text-secondary-text">优化进度</div>
                <div className="flex items-center gap-3 text-sm flex-wrap">
                  {['screen', 'preload', 'tpe', 'guardrails', 'done'].map((p, i) => {
                    const phases = ['screen', 'preload', 'tpe', 'guardrails', 'done'];
                    const active = phase === p;
                    const done = phases.indexOf(phase) > phases.indexOf(p);
                    return (
                      <div key={p} className="flex items-center gap-2">
                        <div className={`h-6 w-6 rounded-full flex items-center justify-center text-xs font-medium ${
                          active ? 'bg-cyan text-white' : done ? 'bg-emerald-100 text-emerald-600 dark:bg-emerald-900 dark:text-emerald-400' : 'bg-gray-100 text-gray-400 dark:bg-gray-800 dark:text-gray-500'
                        }`}>
                          {done ? '✓' : (i + 1)}
                        </div>
                        <span className={`text-xs ${active ? 'text-foreground font-medium' : done ? 'text-emerald-500' : 'text-tertiary-text'}`}>
                          {PHASE_LABELS[p]}
                        </span>
                        {i < 4 && <div className={`w-8 h-0.5 ${done ? 'bg-emerald-200 dark:bg-emerald-800' : 'bg-gray-200 dark:bg-gray-700'}`} />}
                      </div>
                    );
                  })}
                </div>
              </div>
            </Card>
          )}

          {/* Result area */}
          {hasResult && (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <StatCard label="优化因子数" value={Object.keys(result.recommendation).length} className="basis-48" />
                <StatCard label="归一化" value={normalize ? '是' : '否'} className="basis-48" />
                <StatCard label="已应用" value={result.applied ? '是' : '否'} className="basis-48" />
                <StatCard label="模式" value={mode === 'postmarket' ? '盘后' : '盘中'} className="basis-48" />
              </div>

              {weightData.length > 0 && (
                <Card>
                  <div className="space-y-3">
                    <div className="font-medium text-sm text-secondary-text">权重对比</div>
                    <Table
                      size="small"
                      columns={weightColumns}
                      dataSource={weightData}
                      pagination={false}
                      scroll={{ x: 460 }}
                    />
                  </div>
                </Card>
              )}

              {/* Report */}
              {result.report_path && (
                <Card>
                  <div className="space-y-3">
                    <button
                      type="button"
                      className="flex items-center gap-1 text-sm font-medium text-secondary-text hover:text-foreground"
                      onClick={() => handleViewReport(result.report_path)}
                    >
                      {reportExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                      完整优化报告
                    </button>
                    {reportExpanded && reportContent && (
                      <div className="max-h-[600px] overflow-y-auto rounded-lg border border-border p-4 bg-gray-50 dark:bg-gray-900
                        home-markdown-prose prose prose-invert prose-sm max-w-none
                        prose-headings:text-foreground prose-headings:font-semibold prose-headings:mt-4 prose-headings:mb-2
                        prose-h1:text-xl prose-h2:text-lg prose-h3:text-base
                        prose-p:leading-relaxed prose-p:mb-3 prose-p:last:mb-0
                        prose-strong:text-foreground prose-strong:font-semibold
                        prose-ul:my-2 prose-ol:my-2 prose-li:my-1
                        prose-code:px-1.5 prose-code:py-0.5 prose-code:rounded prose-code:before:content-none prose-code:after:content-none
                        prose-pre:border prose-table:border-collapse
                        prose-hr:my-4 prose-a:no-underline hover:prose-a:underline
                        prose-blockquote:text-secondary-text
                        whitespace-pre-line break-words
                      ">
                        <Markdown remarkPlugins={[remarkGfm]}>{reportContent}</Markdown>
                      </div>
                    )}
                    {reportExpanded && !reportContent && (
                      <div className="flex items-center justify-center py-4">
                        <Loader2 className="h-4 w-4 animate-spin text-tertiary-text" />
                      </div>
                    )}
                  </div>
                </Card>
              )}

                          </>
          )}

          {/* Empty state */}
          {!showProgress && !hasResult && !error && !reportContent && (
            <EmptyState
              icon={<Play className="h-8 w-8 text-tertiary-text" />}
              title="开始因子权重优化"
              description="选择模式、调整参数后点击「开始优化」，系统将使用 Optuna TPE 搜索最优权重组合"
            />
          )}

          {/* Report viewer — renders independently so history "查看报告" works */}
          {(reportExpanded || reportContent) && (
            <Card>
              <div className="space-y-3">
                <button
                  type="button"
                  className="flex items-center gap-1 text-sm font-medium text-secondary-text hover:text-foreground"
                  onClick={() => setReportExpanded((prev) => !prev)}
                >
                  {reportExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                  优化报告
                </button>
                {reportExpanded && reportContent && (
                  <div className="max-h-[600px] overflow-y-auto rounded-lg border border-border p-4 bg-gray-50 dark:bg-gray-900
                    home-markdown-prose prose prose-invert prose-sm max-w-none
                    prose-headings:text-foreground prose-headings:font-semibold prose-headings:mt-4 prose-headings:mb-2
                    prose-h1:text-xl prose-h2:text-lg prose-h3:text-base
                    prose-p:leading-relaxed prose-p:mb-3 prose-p:last:mb-0
                    prose-strong:text-foreground prose-strong:font-semibold
                    prose-ul:my-2 prose-ol:my-2 prose-li:my-1
                    prose-code:px-1.5 prose-code:py-0.5 prose-code:rounded prose-code:before:content-none prose-code:after:content-none
                    prose-pre:border prose-table:border-collapse
                    prose-hr:my-4 prose-a:no-underline hover:prose-a:underline
                    prose-blockquote:text-secondary-text
                    whitespace-pre-line break-words
                  ">
                    <Markdown remarkPlugins={[remarkGfm]}>{reportContent}</Markdown>
                  </div>
                )}
                {reportExpanded && !reportContent && (
                  <div className="flex items-center justify-center py-4">
                    <Loader2 className="h-4 w-4 animate-spin text-tertiary-text" />
                  </div>
                )}
              </div>
            </Card>
          )}

          {/* History */}
          <Card>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="font-medium text-sm text-secondary-text">优化历史</span>
                <button
                  type="button"
                  className="text-xs text-cyan hover:underline"
                  onClick={loadHistory}
                  disabled={historyLoading}
                >
                  {historyLoading ? <Loader2 className="h-3 w-3 animate-spin inline" /> : '刷新'}
                </button>
              </div>
              {history.length > 0 ? (
                <Table
                  size="small"
                  columns={historyColumns}
                  dataSource={history.map((h, i) => ({ ...h, key: i }))}
                  pagination={false}
                  scroll={{ x: 540 }}
                />
              ) : (
                <div className="text-xs text-tertiary-text py-4 text-center">
                  {historyLoading ? '加载中…' : '暂无优化历史'}
                </div>
              )}
            </div>
          </Card>
        </div>
      </div>

      {/* Confirmation Modal */}
      <Modal
        title="确认应用权重"
        open={confirmModalOpen}
        onOk={handleConfirmApply}
        onCancel={handleCancelApply}
        confirmLoading={applyLoading}
        okText="确认应用"
        cancelText="取消"
        width={700}
        destroyOnClose
      >
        <div className="space-y-4 pt-2">
          <div className="text-sm text-secondary-text">
            以下权重将写入 <code className="text-cyan">.env</code> 配置文件，发现引擎和回测引擎将自动同步生效。
          </div>
          <Table
            size="small"
            columns={confirmColumns}
            dataSource={weightData}
            pagination={false}
            scroll={{ y: 360 }}
          />
          <div className="text-xs text-amber-500">
            注意：确认后将直接修改 .env 文件（自动备份旧文件），请仔细核对后再确认。
          </div>
        </div>
      </Modal>
    </AppPage>
  );
};

export default FactorTuningPage;
