import type React from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  BarChart, Bar, CartesianGrid,
} from 'recharts';
import { DatePicker, Segmented, Table, InputNumber, Button, Select, Input, Tooltip as AntTooltip } from 'antd';
import { Brain, Play, Loader2, Search } from 'lucide-react';
import { AppPage, Card, StatCard, EmptyState, ApiErrorAlert } from '../components/common';
import { researchApi, type LGBTaskStatusResponse, type LGBPredictionItem, type LGBBacktestCompareResponse, type LGBModelInfo, type LGBDateRangeResponse, type LGBStockLookupItem, type LGBBacktestSimResponse, type LGBBacktestSimAvailableResponse, type LGBBruteForceTaskStatus, type LGBDiagnosticsResponse, type LGBCrossModelOverlapResponse, type CatchUpTaskStatus, type LGBBruteForceResult } from '../api/research';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import dayjs from 'dayjs';

function pctNum(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

function exportBacktestExcel(trades: LGBBacktestSimResponse['trades'], forwardDays: number, topN: number, execMode: string) {
  const rows = trades.filter((t) => !t.skipped);
  const header = ['预测日', '股票名称', '股票代码', '买入日', '买入价', '股数', '买入金额', '卖出日', '卖出价', '收益%'];
  const body = rows.map((t) => [
    t.pred_date,
    t.stock_name,
    t.stock_code,
    t.buy_date,
    t.buy_price.toFixed(2),
    t.shares,
    t.actual_cost.toFixed(2),
    t.sell_date || '持仓中',
    t.sell_price.toFixed(2),
    (t.return_pct * 100).toFixed(2),
  ].map((v) => `<td>${v}</td>`).join('')).map((r) => `<tr>${r}</tr>`).join('');
  const html = `<html><head><meta charset="utf-8"></head><body><table border="1"><thead><tr>${header.map((h) => `<th>${h}</th>`).join('')}</tr></thead><tbody>${body}</tbody></table></body></html>`;
  const blob = new Blob(['﻿' + html], { type: 'application/vnd.ms-excel;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `lgb_backtest_fwd${forwardDays}_top${topN}_${execMode}.xls`;
  a.click();
  URL.revokeObjectURL(url);
}

const LightGBMPage: React.FC = () => {
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const [forwardDays, setForwardDays] = useState(3);
  const [labelMode, setLabelMode] = useState<'fixed' | 'peak_speed'>('fixed');
  const [windowDays, setWindowDays] = useState(20);
  const [peakMinReturn, setPeakMinReturn] = useState(0.01);
  const [trainExecMode, setTrainExecMode] = useState('close');
  const [nEstimators, setNEstimators] = useState(200);
  const [numLeaves, setNumLeaves] = useState(31);
  const [learningRate, setLearningRate] = useState(0.05);
  const [cvFolds, setCvFolds] = useState(5);
  const [startDate, setStartDate] = useState<string | null>(null);
  const [endDate, setEndDate] = useState<string | null>(null);

  const [training, setTraining] = useState(false);
  const [statusMsg, setStatusMsg] = useState('');
  const [error, setError] = useState<ParsedApiError | null>(null);

  const [featureImportance, setFeatureImportance] = useState<{ name: string; gain: number; split: number }[]>([]);
  const [predictions, setPredictions] = useState<LGBPredictionItem[]>([]);
  const [backtest, setBacktest] = useState<LGBBacktestCompareResponse | null>(null);
  const [models, setModels] = useState<LGBModelInfo[]>([]);
  const [selectedModel, setSelectedModel] = useState<string | undefined>(undefined);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [dateRange, setDateRange] = useState<LGBDateRangeResponse | null>(null);
  const [stockCode, setStockCode] = useState('');
  const [stockLookup, setStockLookup] = useState<LGBStockLookupItem | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState('');
  const [backtestSim, setBacktestSim] = useState<LGBBacktestSimResponse | null>(null);
  const [backtestSimLoading, setBacktestSimLoading] = useState(false);
  const [backtestSimAvailable, setBacktestSimAvailable] = useState<LGBBacktestSimAvailableResponse | null>(null);
  const [backtestFwd, setBacktestFwd] = useState(3);
  const [backtestTopN, setBacktestTopN] = useState(1);
  const [peakStopLoss, setPeakStopLoss] = useState(-0.10);
  const [stopStrategy, setStopStrategy] = useState('none');
  const [bruteForceStatus, setBruteForceStatus] = useState<LGBBruteForceTaskStatus | null>(null);
  const [bruteForceLoading, setBruteForceLoading] = useState(false);
  const bruteForcePollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [latestBfReport, setLatestBfReport] = useState<LGBBruteForceResult | null>(null);
  const [bfReportLoading, setBfReportLoading] = useState(false);
  const bfAutoAppliedRef = useRef(false);
  const bfModelAutoSelectedRef = useRef(false);
  const [diagnostics, setDiagnostics] = useState<LGBDiagnosticsResponse | null>(null);
  const [catchUpStatus, setCatchUpStatus] = useState<CatchUpTaskStatus | null>(null);
  const catchUpPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Cross-model overlap
  const [overlapData, setOverlapData] = useState<LGBCrossModelOverlapResponse | null>(null);
  const [overlapLoading, setOverlapLoading] = useState(false);
  const overlapHighlight = new Set(
    overlapData?.stocks.filter((s) => s.count >= 3).map((s) => s.ts_code) ?? [],
  );

  const selectedExecMode = selectedModel
    ? (selectedModel.endsWith('open2open') ? 'open' : 'close')
    : trainExecMode;

  const modelDisplayName = (() => {
    if (!selectedModel) return '';
    const raw = models.find(m => m.path === selectedModel)?.name ?? selectedModel.split('/').pop()?.replace('.joblib', '') ?? '';
    return raw.replace(/^lgb_(postmarket|intraday)_/, '');
  })();

  /* ── Derived per-mode bounds ── */
  const dateBounds = dateRange?.postmarket;
  const dateMin = dateBounds ? dayjs(dateBounds.min) : null;
  const dateMax = dateBounds ? dayjs(dateBounds.max) : null;
  const disableDate = useCallback(
    (d: dayjs.Dayjs) => dateMin !== null && dateMax !== null && (d.isBefore(dateMin) || d.isAfter(dateMax)),
    [dateMin, dateMax],
  );

  /* ── Load date range ── */
  const fetchDateRange = useCallback(async () => {
    try {
      const dr = await researchApi.getDateRange();
      setDateRange(dr);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchDateRange(); }, [fetchDateRange]);

  /* ── Auto-load latest brute force report ── */
  useEffect(() => {
    setBfReportLoading(true);
    researchApi.getLatestBruteForceReport()
      .then((report) => setLatestBfReport(report))
      .catch(() => { /* no report yet, ignore */ })
      .finally(() => setBfReportLoading(false));
  }, []);

  /* ── Auto-apply best return params from latest report ── */
  useEffect(() => {
    if (!latestBfReport?.best_by_return || bfAutoAppliedRef.current) return;
    const best = latestBfReport.best_by_return;
    bfAutoAppliedRef.current = true;
    setTrainExecMode(best.exec_mode);
    if (best.label_mode === 'peak_speed') {
      setLabelMode('peak_speed');
      setWindowDays(best.window_days || 20);
    } else {
      setLabelMode('fixed');
      setForwardDays(best.forward_days);
    }
    setStopStrategy(best.stop_strategy);
    setBacktestTopN(best.top_n);
    setBacktestFwd(best.forward_days);
  }, [latestBfReport]);

  /* ── Load model list ── */
  const loadModels = useCallback(async () => {
    setModelsLoading(true);
    try {
      const data = await researchApi.listModels(labelMode);
      setModels(data.models);
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setModelsLoading(false);
    }
  }, [labelMode]);

  useEffect(() => { loadModels(); }, [loadModels]);

  const [predictLoading, setPredictLoading] = useState(false);
  const [predictionDate, setPredictionDate] = useState('');

  /* ── Predict Top 5 with selected model ── */
  const handlePredictTop5 = useCallback(async () => {
    if (!selectedModel) return;
    setError(null);
    setPredictLoading(true);
    try {
      const pred = await researchApi.getPredictions(selectedModel);
      setPredictions(pred.predictions);
      setPredictionDate(pred.model_date);
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setPredictLoading(false);
    }
  }, [selectedModel]);

  const handleCrossModelOverlap = useCallback(async () => {
    setError(null);
    setOverlapLoading(true);
    try {
      const data = await researchApi.getCrossModelOverlap('all', 5);
      setOverlapData(data);
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setOverlapLoading(false);
    }
  }, [selectedExecMode]);

  const loadModelResults = useCallback(async (modelPath: string) => {
    setError(null);
    setPredictions([]);
    setFeatureImportance([]);
    setDiagnostics(null);
    setStockLookup(null);
    setOverlapData(null);
    setBacktest(null);
    try {
      const [fi, pred] = await Promise.all([
        researchApi.getFeatureImportance(modelPath),
        researchApi.getPredictions(modelPath),
      ]);
      const fiList = Object.keys(fi.gain).map((name) => ({
        name,
        gain: fi.gain[name],
        split: fi.split[name] ?? 0,
      })).sort((a, b) => b.gain - a.gain);
      setFeatureImportance(fiList);
      setPredictions(pred.predictions);
      setPredictionDate(pred.model_date);
    } catch (e) {
      setError(getParsedApiError(e));
    }
    researchApi.getDiagnostics(modelPath).then(setDiagnostics).catch(() => {});
  }, []);

  /* ── Auto-select best model matching brute force best return ── */
  useEffect(() => {
    if (!latestBfReport?.best_by_return || !bfAutoAppliedRef.current || bfModelAutoSelectedRef.current) return;
    if (models.length === 0 || selectedModel) return;
    const best = latestBfReport.best_by_return;
    const execSuffix = best.exec_mode === 'open' ? 'open2open' : 'close2close';
    const modePrefix = best.label_mode === 'peak_speed'
      ? `peak${best.window_days}d`
      : `fwd${best.forward_days}d`;
    const match = models.find((m) =>
      m.name.includes(modePrefix) && m.name.includes(execSuffix),
    );
    if (match) {
      bfModelAutoSelectedRef.current = true;
      setSelectedModel(match.path);
      loadModelResults(match.path);
    }
  }, [latestBfReport, models, selectedModel, loadModelResults]);

  /* ── Train ── */
  const handleTrain = useCallback(async () => {
    setError(null);
    setTraining(true);
    setStatusMsg('');
    setFeatureImportance([]);
    setPredictions([]);
    setBacktest(null);

    try {
      const { task_id } = await researchApi.train({
        mode: 'postmarket',
        forward_days: forwardDays,
        exec_mode: trainExecMode,
        label_mode: labelMode,
        window_days: windowDays,
        peak_min_return: peakMinReturn,
        start_date: startDate,
        end_date: endDate,
        n_estimators: nEstimators,
        num_leaves: numLeaves,
        learning_rate: learningRate,
        cv_folds: cvFolds,
      });
      const finalResult = await new Promise<LGBTaskStatusResponse>((resolve, reject) => {
        pollRef.current = setInterval(async () => {
          try {
            const status = await researchApi.getStatus(task_id);
            if (status.status_message) setStatusMsg(status.status_message);
            if (status.status === 'completed') {
              setStatusMsg('');
              clearInterval(pollRef.current!);
              pollRef.current = null;
              resolve(status);
            } else if (status.status === 'failed') {
              clearInterval(pollRef.current!);
              pollRef.current = null;
              reject(new Error(status.error || '训练失败'));
            }
          } catch (e) {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            reject(e);
          }
        }, 1000);
      });

      if (finalResult?.result) {
        const fi = finalResult.result.feature_importance;
        const fiList = Object.keys(fi.gain).map((name) => ({
          name,
          gain: fi.gain[name],
          split: fi.split[name] ?? 0,
        })).sort((a, b) => b.gain - a.gain);
        setFeatureImportance(fiList);
        setPredictions(finalResult.result.predictions);
        if (finalResult.result.model_date) setPredictionDate(finalResult.result.model_date);

        if (finalResult.result.training_metrics || finalResult.result.tree_diagnostics) {
          setDiagnostics({
            training_metrics: finalResult.result.training_metrics ?? { cv_rmse_mean: 0, cv_rmse_std: 0, n_samples: 0, n_features: 0, cv_scores: [], rank_ic_mean: null, rank_ic_std: null, icir: null, oof_corr: null },
            tree_diagnostics: finalResult.result.tree_diagnostics ?? { n_trees: 0, avg_depth: 0, avg_n_leaves: 0, total_n_leaves: 0 },
            prediction_stats: finalResult.result.prediction_stats ?? null,
          });
        }

        setTraining(false);
        setStatusMsg('');

        loadModels();
        researchApi.getBacktestCompare({
          mode: 'postmarket',
          top_n: 10,
          forward_days: forwardDays,
        }).then((bt) => setBacktest(bt)).catch(() => {});
      }
    } catch (e) {
      setError(getParsedApiError(e));
      setTraining(false);
      setStatusMsg('');
    }
  }, [forwardDays, trainExecMode, labelMode, windowDays, peakMinReturn, nEstimators, numLeaves, learningRate, cvFolds, startDate, endDate, loadModels]);

  /* ── Stock lookup ── */
  const handleLookup = useCallback(async () => {
    const code = stockCode.trim();
    if (!code) return;
    setLookupLoading(true);
    setLookupError('');
    setStockLookup(null);
    try {
      const resp = await researchApi.lookupStock(code, selectedModel);
      if (resp.found && resp.item) {
        setStockLookup(resp.item);
      } else {
        setLookupError(resp.message || '未找到该股票');
      }
    } catch (e) {
      setLookupError('查询失败，请确认模型已训练完成');
    } finally {
      setLookupLoading(false);
    }
  }, [stockCode, selectedModel]);

  /* ── Backtest Sim ── */
  const backtestAbortRef = useRef<AbortController | null>(null);

  const fetchBacktestSim = useCallback(async (fwd: number, exec: string, topN: number, st: string) => {
    backtestAbortRef.current?.abort();
    const controller = new AbortController();
    backtestAbortRef.current = controller;

    setBacktestSimLoading(true);
    setBacktestSim(null);
    try {
      const data = await researchApi.getBacktestSim({ forward_days: fwd, top_n: topN, exec_mode: exec, stop_strategy: st }, controller.signal);
      setBacktestSim(data);
    } catch (e: any) {
      if (e?.name === 'CanceledError' || e?.code === 'ERR_CANCELED') return;
    } finally {
      setBacktestSimLoading(false);
    }
  }, []);

  const fetchBacktestSimPeak = useCallback(async (exec: string, topN: number, sl: number) => {
    backtestAbortRef.current?.abort();
    const controller = new AbortController();
    backtestAbortRef.current = controller;

    setBacktestSimLoading(true);
    setBacktestSim(null);
    try {
      const data = await researchApi.getBacktestSimPeak({ top_n: topN, exec_mode: exec, stop_loss: sl }, controller.signal);
      setBacktestSim(data);
    } catch (e: any) {
      if (e?.name === 'CanceledError' || e?.code === 'ERR_CANCELED') return;
    } finally {
      setBacktestSimLoading(false);
    }
  }, []);

  /* ── Fetch available backtest-sim options ── */
  const fetchBacktestSimAvailable = useCallback(async () => {
    try {
      const data = await researchApi.getBacktestSimAvailable();
      setBacktestSimAvailable(data);
      const avail = trainExecMode === 'open' ? data.open : data.close;
      if (avail.length > 0) {
        setBacktestFwd((prev) => avail.includes(prev) ? prev : avail[0]);
      }
    } catch { /* ignore */ }
  }, [trainExecMode]);

  useEffect(() => { fetchBacktestSimAvailable(); }, []); // mount
  useEffect(() => { fetchBacktestSimAvailable(); }, [trainExecMode]);

  useEffect(() => {
    if (labelMode === 'peak_speed') {
      fetchBacktestSimPeak(trainExecMode, backtestTopN, peakStopLoss);
      return;
    }
    fetchBacktestSim(backtestFwd, trainExecMode, backtestTopN, stopStrategy);
  }, [fetchBacktestSim, fetchBacktestSimPeak, backtestFwd, trainExecMode, backtestTopN, stopStrategy, labelMode, peakStopLoss]);

  /* ── Cleanup polling on unmount ── */
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      if (bruteForcePollRef.current) clearInterval(bruteForcePollRef.current);
    };
  }, []);

  /* ── Brute-Force Search ── */
  const BF_TASK_KEY = 'lgb_brute_force_task_id';

  const pollBruteForce = useCallback((taskId: string): Promise<LGBBruteForceTaskStatus> => {
    return new Promise<LGBBruteForceTaskStatus>((resolve, reject) => {
      bruteForcePollRef.current = setInterval(async () => {
        try {
          const status = await researchApi.getBruteForceStatus(taskId);
          setBruteForceStatus(status);
          if (status.status === 'completed') {
            clearInterval(bruteForcePollRef.current!);
            bruteForcePollRef.current = null;
            localStorage.removeItem(BF_TASK_KEY);
            resolve(status);
          } else if (status.status === 'failed') {
            clearInterval(bruteForcePollRef.current!);
            bruteForcePollRef.current = null;
            localStorage.removeItem(BF_TASK_KEY);
            reject(new Error(status.error || '搜索失败'));
          }
        } catch (e) {
          clearInterval(bruteForcePollRef.current!);
          bruteForcePollRef.current = null;
          localStorage.removeItem(BF_TASK_KEY);
          reject(e);
        }
      }, 1500);
    });
  }, []);

  // Auto-resume on mount if there's a stored task_id
  useEffect(() => {
    const stored = localStorage.getItem(BF_TASK_KEY);
    if (!stored) return;
    setBruteForceLoading(true);
    pollBruteForce(stored)
      .then((result) => setBruteForceStatus(result))
      .catch(() => {})
      .finally(() => setBruteForceLoading(false));
  }, [pollBruteForce]);

  const handleBruteForceStart = useCallback(async () => {
    setBruteForceLoading(true);
    setBruteForceStatus(null);
    try {
      const { task_id } = await researchApi.startBruteForce();
      localStorage.setItem(BF_TASK_KEY, task_id);
      const finalResult = await pollBruteForce(task_id);
      setBruteForceStatus(finalResult);
    } catch {
      // error already set via polling
    } finally {
      setBruteForceLoading(false);
    }
  }, [pollBruteForce]);

  /* ── Catch-up prediction ── */
  const CU_TASK_KEY = 'lgb_catchup_task';

  const pollCatchUp = useCallback(async (taskId: string): Promise<CatchUpTaskStatus> => {
    return new Promise((resolve, reject) => {
      const interval = setInterval(async () => {
        try {
          const status = await researchApi.getCatchUpStatus(taskId);
          setCatchUpStatus(status);
          if (status.status === 'completed' || status.status === 'failed') {
            clearInterval(interval);
            catchUpPollRef.current = null;
            localStorage.removeItem(CU_TASK_KEY);
            resolve(status);
          }
        } catch (e) {
          clearInterval(interval);
          catchUpPollRef.current = null;
          reject(e);
        }
      }, 2000);
      catchUpPollRef.current = interval;
    });
  }, []);

  const handleCatchUpStart = useCallback(async () => {
    setCatchUpStatus(null);
    try {
      const { task_id } = await researchApi.startCatchUp();
      localStorage.setItem(CU_TASK_KEY, task_id);
      await pollCatchUp(task_id);
      // Refresh backtest available after catch-up
      fetchBacktestSimAvailable();
    } catch {
      // error already set via polling
    }
  }, [pollCatchUp, fetchBacktestSimAvailable]);

  // Auto-resume catch-up if page refreshed during task
  useEffect(() => {
    const stored = localStorage.getItem(CU_TASK_KEY);
    if (!stored) return;
    pollCatchUp(stored).catch(() => {});
  }, [pollCatchUp]);

  /* ── Capital curve chart data ── */
  const capitalData = backtest?.capital_curve?.map((p) => ({
    date: p.date,
    lgb: p.lgb,
    benchmark: p.benchmark,
  })) ?? [];

  const predColumns = [
    { title: '排名', dataIndex: 'rank', key: 'rank', width: 50 },
    { title: '股票', dataIndex: 'ts_code', key: 'stock', width: 100, render: (_: unknown, r: LGBPredictionItem) => (
      <div className="leading-tight">
        <div className="text-xs font-medium">{r.stock_name || r.stock_code}</div>
        <div className="text-[10px] text-tertiary-text">{r.ts_code}</div>
      </div>
    )},
    { title: 'LGB 评分（LGB Score）', dataIndex: 'lgb_score', key: 'lgb_score', width: 80, render: (_: unknown, r: LGBPredictionItem) => r.lgb_score.toFixed(2) },
    { title: '预期涨幅（Exp. Return）', dataIndex: 'raw_score', key: 'raw_score', width: 90, render: (_: unknown, r: LGBPredictionItem) => <span className={r.raw_score >= 0 ? 'text-red-400' : 'text-green-400'}>{(r.raw_score * 100).toFixed(2)}%</span> },
    ...(predictions.some(p => p.predicted_days != null) ? [{ title: '预计见顶', dataIndex: 'predicted_days', key: 'predicted_days', width: 80, render: (_: unknown, r: LGBPredictionItem) => r.predicted_days != null ? `${r.predicted_days}天` : '-' }] : []),
    { title: '胜率（Win Rate）', dataIndex: 'win_rate', key: 'win_rate', width: 80, render: (_: unknown, r: LGBPredictionItem) => r.win_rate != null ? `${(r.win_rate * 100).toFixed(1)}%` : '-' },
    { title: '历史均收益（Avg Return）', dataIndex: 'avg_return', key: 'avg_return', width: 100, render: (_: unknown, r: LGBPredictionItem) => r.avg_return != null ? <span className={r.avg_return >= 0 ? 'text-red-400' : 'text-green-400'}>{(r.avg_return * 100).toFixed(2)}%</span> : '-' },
    { title: '最大盈（Max Gain）', dataIndex: 'max_return', key: 'max_return', width: 85, render: (_: unknown, r: LGBPredictionItem) => r.max_return != null ? <span className="text-red-400">{(r.max_return * 100).toFixed(1)}%</span> : '-' },
    { title: '最大亏（Max Loss）', dataIndex: 'max_loss', key: 'max_loss', width: 85, render: (_: unknown, r: LGBPredictionItem) => r.max_loss != null ? <span className="text-green-400">{(r.max_loss * 100).toFixed(1)}%</span> : '-' },
    { title: '盈亏比（P/L Ratio）', dataIndex: 'profit_loss_ratio', key: 'profit_loss_ratio', width: 85, render: (_: unknown, r: LGBPredictionItem) => r.profit_loss_ratio != null ? r.profit_loss_ratio.toFixed(2) : '-' },
    { title: '入选次数（Hits）', dataIndex: 'hit_count', key: 'hit_count', width: 75, render: (_: unknown, r: LGBPredictionItem) => r.hit_count ?? '-' },
    { title: '分位数（%ile）', dataIndex: 'score_percentile', key: 'score_percentile', width: 70, render: (_: unknown, r: LGBPredictionItem) => r.score_percentile != null ? `${r.score_percentile}%` : '-' },
    ...(overlapData ? [{
      title: '交叉命中（Overlap）',
      dataIndex: 'ts_code',
      key: 'overlap',
      width: 90,
      render: (_: unknown, r: LGBPredictionItem) => {
        const found = overlapData.stocks.find(s => s.ts_code === r.ts_code);
        const cnt = found?.count ?? 0;
        if (cnt === 0) return '-';
        const tipLines = found?.model_names?.length
          ? found.model_names.map((n) => {
              const exec = n.endsWith('open2open') ? 'open' : n.endsWith('close2close') ? 'close' : '';
              const m = n.replace(/_open2open$|_close2close$/, '');
              const p = m.split('_');
              const fwd = p.find(x => x.startsWith('fwd') || x.startsWith('peak'));
              const dates = p.filter(x => /^\d{8}$/.test(x));
              const short = [exec, fwd, ...dates].filter(Boolean).join(' ');
              return short || n;
            })
          : [];
        const tip = tipLines.join('\n');
        const el = cnt >= 3 ? <span className="text-amber-400 font-medium">{cnt}/{overlapData.total_models}</span> : <span>{cnt}/{overlapData.total_models}</span>;
        return tip ? <AntTooltip overlayStyle={{ maxWidth: 360 }} title={<pre className="text-[11px] leading-relaxed m-0 whitespace-pre-wrap">{tip}</pre>} placement="top">{el}</AntTooltip> : el;
      },
    }] : []),
  ];

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="flex flex-col lg:flex-row gap-5">
        {/* ──── Left Panel ──── */}
        <div className="lg:w-[260px] shrink-0 space-y-4">
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">训练标签模式</div>
              <Segmented
                block
                value={trainExecMode}
                onChange={(v) => { setTrainExecMode(v as string); setSelectedModel(undefined); setPredictions([]); setFeatureImportance([]); setDiagnostics(null); setStockLookup(null); setOverlapData(null); }}
                options={[
                  { label: '收盘→收盘', value: 'close' },
                  { label: '开盘→开盘', value: 'open' },
                ]}
              />
              <div className="text-[10px] text-tertiary-text">
                训练标签与回测执行模式对应，训练/回测一致才可对比
              </div>
            </div>
          </Card>

          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">标签模式</div>
              <Segmented
                block
                value={labelMode}
                onChange={(v) => {
                  setLabelMode(v as 'fixed' | 'peak_speed');
                  setSelectedModel(undefined);
                  setPredictions([]);
                  setFeatureImportance([]);
                  setDiagnostics(null);
                }}
                options={[
                  { label: '固定持有期', value: 'fixed' },
                  { label: '峰值速度', value: 'peak_speed' },
                ]}
              />
              <div className="text-[10px] text-tertiary-text">
                {labelMode === 'fixed'
                  ? '预测固定第N天的涨跌幅'
                  : '预测窗口内最大涨幅与到达天数'}
              </div>
            </div>
          </Card>

          <Card>
            <div className="space-y-4">
              <div className="font-medium text-sm text-secondary-text">训练参数</div>

              {/* 标签参数 - 根据模式切换 */}
              {labelMode === 'fixed' ? (
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">预测天数 <span className="text-tertiary-text">（未来N日后涨跌幅，推荐3）</span></div>
                  <InputNumber size="small" min={1} max={60} value={forwardDays} onChange={(v) => setForwardDays(v ?? 3)} className="w-full" />
                </div>
              ) : (
                <>
                  <div className="space-y-1.5">
                    <div className="text-xs text-secondary-text">观察窗口 <span className="text-tertiary-text">（未来N日内搜索峰值，推荐20）</span></div>
                    <InputNumber size="small" min={5} max={60} value={windowDays} onChange={(v) => setWindowDays(v ?? 20)} className="w-full" />
                  </div>
                  <div className="space-y-1.5">
                    <div className="text-xs text-secondary-text">最小涨幅门槛 <span className="text-tertiary-text">（低于此值视为无效）</span></div>
                    <InputNumber size="small" min={0} max={0.1} step={0.005} value={peakMinReturn} onChange={(v) => setPeakMinReturn(v ?? 0.01)} className="w-full" />
                  </div>
                </>
              )}

              {/* 树结构参数：一行两列 */}
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">树数量 <span className="text-tertiary-text">n_estimators，迭代轮数，推荐200</span></div>
                  <InputNumber size="small" min={10} max={1000} value={nEstimators} onChange={(v) => setNEstimators(v ?? 200)} className="w-full" />
                </div>
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">叶子数 <span className="text-tertiary-text">num_leaves，单树复杂度，推荐31</span></div>
                  <InputNumber size="small" min={2} max={255} value={numLeaves} onChange={(v) => setNumLeaves(v ?? 31)} className="w-full" />
                </div>
              </div>

              {/* 学习率 + CV：一行两列 */}
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">学习率 <span className="text-tertiary-text">lr，每轮步长，推荐0.05</span></div>
                  <InputNumber size="small" min={0.001} max={1} step={0.01} value={learningRate} onChange={(v) => setLearningRate(v ?? 0.05)} className="w-full" />
                </div>
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">交叉验证 <span className="text-tertiary-text">cv，时序切分评估泛化，推荐5</span></div>
                  <InputNumber size="small" min={2} max={20} value={cvFolds} onChange={(v) => setCvFolds(v ?? 5)} className="w-full" />
                </div>
              </div>

              {/* 日期范围 */}
              <div className="space-y-1.5">
                <div className="text-xs text-secondary-text">日期范围 <span className="text-tertiary-text">（可选）</span></div>
                {dateBounds && (
                  <>
                    <div className="text-[10px] text-tertiary-text/70">{dateBounds.min} ~ {dateBounds.max}</div>
                    <div className="flex flex-wrap gap-1">
                      {[{ label: '30日', days: 42 }, { label: '60日', days: 85 }, { label: '120日', days: 170 }, { label: '240日', days: 340 }].map(({ label, days }) => (
                        <Button
                          key={label}
                          size="small"
                          type="default"
                          className="text-[10px] h-5 px-1.5"
                          onClick={() => {
                            const end = dateMax ?? dayjs();
                            setStartDate(end.subtract(days, 'day').format('YYYYMMDD'));
                            setEndDate(end.format('YYYYMMDD'));
                          }}
                        >
                          近{label}
                        </Button>
                      ))}
                    </div>
                  </>
                )}
                <DatePicker
                  size="small"
                  placeholder="起始日"
                  disabledDate={disableDate}
                  value={startDate ? dayjs(startDate) : null}
                  onChange={(_, ds) => setStartDate(typeof ds === 'string' ? ds : null)}
                  className="w-full"
                />
                <div className="mt-2">
                  <DatePicker
                    size="small"
                    placeholder="结束日"
                    disabledDate={disableDate}
                    value={endDate ? dayjs(endDate) : null}
                    onChange={(_, ds) => setEndDate(typeof ds === 'string' ? ds : null)}
                    className="w-full"
                  />
                </div>
              </div>

              <Button
                block
                type="primary"
                icon={training ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                onClick={handleTrain}
                disabled={training}
                loading={training}
              >
                {training ? (statusMsg || '训练中...') : '开始训练'}
              </Button>
            </div>
          </Card>

          {models.length > 0 && (
            <Card>
              <div className="space-y-3">
                <div className="font-medium text-sm text-secondary-text">已有模型</div>
                <Select
                  size="small"
                  className="w-full"
                  placeholder="选择已保存的模型"
                  allowClear
                  loading={modelsLoading}
                  value={selectedModel}
                  onChange={(v) => {
                    setSelectedModel(v);
                    if (v) loadModelResults(v);
                  }}
                  options={models
                    .filter((m) => {
                      const suffix = trainExecMode === 'open' ? 'open2open' : 'close2close';
                      return m.name.includes(suffix);
                    })
                    .map((m) => ({
                      label: `${m.name.replace(/^lgb_(postmarket|intraday)_/, '')} (${new Date(m.saved_at).toLocaleDateString()})`,
                      value: m.path,
                    }))}
                />
                {selectedModel && (
                  <>
                    <div className="border-t border-white/5" />
                    <Button
                      block
                      size="small"
                      type="primary"
                      icon={predictLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Brain className="h-4 w-4" />}
                      onClick={handlePredictTop5}
                      loading={predictLoading}
                    >
                      预测当前 Top 5
                    </Button>
                  </>
                )}
              </div>
            </Card>
          )}

          {/* Brute-Force Search */}
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">全方案搜索</div>
              <div className="text-xs text-tertiary-text">
                遍历 lgb_reports/ 缓存中所有参数组合（止损策略 × 执行模式 × 持有期 × top_n），寻找收益/夏普最优方案。后台运行，结果保存至 lgb_reports/。
                {bfReportLoading && !latestBfReport && (
                  <span className="text-blue-400 ml-1 inline-flex items-center gap-1">
                    <Loader2 className="h-3 w-3 animate-spin" />加载报告中...
                  </span>
                )}
                {latestBfReport && !bruteForceStatus && (
                  <span className="text-green-400 ml-1">
                    （已加载最新报告{latestBfReport.report_path ? `: ${latestBfReport.report_path.split('/').pop()}` : ''}，已自动应用最佳收益参数）
                  </span>
                )}
                {(() => {
                  const results = bruteForceStatus?.result?.all_results;
                  if (!results || results.length === 0) return null;
                  const strs = [...new Set(results.map((r) => r.stop_strategy).filter(Boolean))];
                  const ems = [...new Set(results.map((r) => r.exec_mode).filter(Boolean))];
                  const fwds = [...new Set(results.map((r) => r.forward_days))].sort((a, b) => a - b);
                  const tns = [...new Set(results.map((r) => r.top_n))].sort((a, b) => a - b);
                  return ` (本次: ${strs.length}策略 × ${ems.length}模式 × ${fwds.length}持有期 × ${tns.length}top_n = ${results.length} 组合)`;
                })()}
              </div>

              {bruteForceStatus && bruteForceStatus.status === 'running' && (
                <div className="space-y-1.5">
                  <div className="flex items-center gap-2 text-xs text-blue-400">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    {bruteForceStatus.status_message}
                  </div>
                  <div className="h-1.5 rounded-full bg-white/10 overflow-hidden">
                    <div
                      className="h-full rounded-full bg-blue-500 transition-all duration-300"
                      style={{
                        width: `${(bruteForceStatus.progress_current / bruteForceStatus.progress_total) * 100}%`,
                      }}
                    />
                  </div>
                  <div className="text-[10px] text-tertiary-text text-right">
                    {bruteForceStatus.progress_current}/{bruteForceStatus.progress_total}
                  </div>
                </div>
              )}

              {(() => {
                const displayResult = bruteForceStatus?.result || latestBfReport;
                if (!displayResult || (!displayResult.best_by_return && !displayResult.best_by_sharpe)) return null;
                const modeLabel = (r: typeof displayResult.best_by_return) => {
                  if (!r) return '';
                  return r.label_mode === 'peak_speed' ? `peak${r.window_days}d` : `fwd${r.forward_days}d`;
                };
                return (
                <div className="space-y-2 text-xs">
                  {displayResult.best_by_return && (
                    <div className="p-2 rounded bg-green-500/10 border border-green-500/20">
                      <div className="text-tertiary-text">最佳收益</div>
                      <div className="font-medium">
                        {displayResult.best_by_return.stop_strategy} {displayResult.best_by_return.exec_mode} {modeLabel(displayResult.best_by_return)} top={displayResult.best_by_return.top_n}
                      </div>
                      <div className="text-red-400">
                        {(displayResult.best_by_return.cumulative_return * 100).toFixed(1)}%
                      </div>
                    </div>
                  )}
                  {displayResult.best_by_sharpe && (
                    <div className="p-2 rounded bg-blue-500/10 border border-blue-500/20">
                      <div className="text-tertiary-text">最佳夏普</div>
                      <div className="font-medium">
                        {displayResult.best_by_sharpe.stop_strategy} {displayResult.best_by_sharpe.exec_mode} {modeLabel(displayResult.best_by_sharpe)} top={displayResult.best_by_sharpe.top_n}
                      </div>
                      <div className="text-blue-400">
                        {displayResult.best_by_sharpe.sharpe_ratio.toFixed(2)}
                      </div>
                    </div>
                  )}
                  {displayResult.report_path && (
                    <div className="text-[10px] text-tertiary-text truncate" title={displayResult.report_path}>
                      报告: {displayResult.report_path.split('/').pop()}
                    </div>
                  )}
                </div>
                );
              })()}

              {bruteForceStatus && bruteForceStatus.status === 'failed' && (
                <div className="text-xs text-red-400">
                  {bruteForceStatus.error || '搜索失败'}
                </div>
              )}

              <Button
                block
                size="small"
                type="primary"
                icon={bruteForceLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
                onClick={handleBruteForceStart}
                disabled={bruteForceLoading || bruteForceStatus?.status === 'running'}
                loading={bruteForceLoading}
              >
                {bruteForceLoading ? '搜索中...' : '开始搜索'}
              </Button>
            </div>
          </Card>
        </div>

        {/* ──── Right Panel ──── */}
        <div className="flex-1 min-w-0 space-y-4">
          {error && <ApiErrorAlert error={error} />}

          {!featureImportance.length && !predictions.length && !backtest && !training && (
            <EmptyState
              icon={<Brain className="h-12 w-12 text-tertiary-text" />}
              title="LightGBM 因子研究"
              description="配置左侧参数后点击「开始训练」，系统将使用因子快照数据训练 LightGBM 模型并展示分析结果。"
            />
          )}

          {/* Feature Importance */}
          {featureImportance.length > 0 && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">特征重要性 (Gain){modelDisplayName ? <span className="text-tertiary-text ml-1 text-xs">| {modelDisplayName}</span> : ''}</div>
              <ResponsiveContainer width="100%" height={Math.max(240, featureImportance.length * 28)}>
                <BarChart data={featureImportance} layout="vertical" margin={{ left: 80, right: 20, top: 5, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                  <XAxis type="number" tick={{ fontSize: 11 }} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 11 }} width={75} />
                  <Tooltip
                    contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }}
                    formatter={(value) => [Number(value).toFixed(4), 'Gain']}
                  />
                  <Bar dataKey="gain" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          )}

          {/* Model Diagnostics */}
          {diagnostics && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">模型诊断{modelDisplayName ? <span className="text-tertiary-text ml-1 text-xs">| {modelDisplayName}</span> : ''}</div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                <StatCard label="CV RMSE" value={`${diagnostics.training_metrics.cv_rmse_mean.toFixed(4)} ± ${diagnostics.training_metrics.cv_rmse_std.toFixed(4)}`} />
                <StatCard label="样本数" value={diagnostics.training_metrics.n_samples.toLocaleString()} />
                <StatCard label="特征数" value={String(diagnostics.training_metrics.n_features)} />
                {diagnostics.training_metrics.rank_ic_mean != null && (
                  <StatCard label="Rank IC" value={`${diagnostics.training_metrics.rank_ic_mean.toFixed(4)} ± ${(diagnostics.training_metrics.rank_ic_std ?? 0).toFixed(4)}`} />
                )}
                {diagnostics.training_metrics.icir != null && (
                  <StatCard label="ICIR" value={diagnostics.training_metrics.icir.toFixed(3)} />
                )}
                {diagnostics.training_metrics.oof_corr != null && (
                  <StatCard label="OOF 相关性" value={diagnostics.training_metrics.oof_corr.toFixed(4)} />
                )}
                <StatCard label="树数量" value={String(diagnostics.tree_diagnostics.n_trees)} />
                <StatCard label="平均深度" value={diagnostics.tree_diagnostics.avg_depth.toFixed(1)} />
              </div>

              {diagnostics.training_metrics.cv_scores.length > 1 && (
                <>
                  <div className="text-xs text-tertiary-text mb-2">逐折 CV RMSE</div>
                  <ResponsiveContainer width="100%" height={120}>
                    <LineChart data={diagnostics.training_metrics.cv_scores.map((v, i) => ({ fold: `Fold ${i + 1}`, rmse: v }))} margin={{ left: 20, right: 20, top: 5, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                      <XAxis dataKey="fold" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }} />
                      <Line type="monotone" dataKey="rmse" stroke="#f59e0b" strokeWidth={2} dot={{ r: 3 }} />
                    </LineChart>
                  </ResponsiveContainer>
                </>
              )}

              {diagnostics.prediction_stats && (
                <div className="mt-3">
                  <div className="text-xs text-tertiary-text mb-2">预测分布</div>
                  <div className="grid grid-cols-4 gap-2 text-xs">
                    <div><span className="text-tertiary-text">Mean:</span> {diagnostics.prediction_stats.mean.toFixed(4)}</div>
                    <div><span className="text-tertiary-text">Std:</span> {diagnostics.prediction_stats.std.toFixed(4)}</div>
                    <div><span className="text-tertiary-text">Skew:</span> {diagnostics.prediction_stats.skew.toFixed(3)}</div>
                    <div><span className="text-tertiary-text">Kurt:</span> {diagnostics.prediction_stats.kurtosis.toFixed(3)}</div>
                    <div><span className="text-tertiary-text">Min:</span> {diagnostics.prediction_stats.min.toFixed(4)}</div>
                    <div><span className="text-tertiary-text">Max:</span> {diagnostics.prediction_stats.max.toFixed(4)}</div>
                    <div><span className="text-tertiary-text">Median:</span> {diagnostics.prediction_stats.median.toFixed(4)}</div>
                  </div>
                </div>
              )}
            </Card>
          )}

          {/* Stock Lookup */}
          {(predictions.length > 0 || selectedModel) && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">个股查询{modelDisplayName ? <span className="text-tertiary-text ml-1 text-xs">| {modelDisplayName}</span> : ''}</div>
              <div className="flex gap-2">
                <Input
                  size="small"
                  placeholder="输入股票代码，如 600519"
                  value={stockCode}
                  onChange={(e) => setStockCode(e.target.value)}
                  onPressEnter={handleLookup}
                  style={{ maxWidth: 200 }}
                />
                <Button
                  size="small"
                  type="primary"
                  icon={lookupLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Search className="h-3 w-3" />}
                  onClick={handleLookup}
                  loading={lookupLoading}
                >
                  查询
                </Button>
              </div>
              {lookupError && <div className="text-red-400 text-xs mt-2">{lookupError}</div>}
              {stockLookup && (
                <div className="mt-3 p-3 rounded bg-blue-500/10 border border-blue-500/20">
                  <div className="grid grid-cols-3 md:grid-cols-6 gap-2 text-xs">
                    <div>
                      <span className="text-tertiary-text">代码</span>
                      <div className="font-medium text-sm">{stockLookup.ts_code}</div>
                    </div>
                    {stockLookup.stock_name && (
                      <div>
                        <span className="text-tertiary-text">名称</span>
                        <div className="font-medium text-sm">{stockLookup.stock_name}</div>
                      </div>
                    )}
                    <div>
                      <span className="text-tertiary-text">LGB 评分</span>
                      <div className="font-medium text-sm text-blue-400">{stockLookup.lgb_score.toFixed(2)}</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">全市场排名</span>
                      <div className="font-medium text-sm">{stockLookup.rank} / {stockLookup.total_stocks}</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">百分位</span>
                      <div className="font-medium text-sm">Top {(stockLookup.rank / stockLookup.total_stocks * 100).toFixed(1)}%</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">原始分</span>
                      <div className="font-medium text-sm">{stockLookup.raw_score.toFixed(4)}</div>
                    </div>
                  </div>
                </div>
              )}
            </Card>
          )}

          {/* Predictions */}
          {predictions.length > 0 && (
            <Card>
              <div className="flex items-center justify-between mb-3">
                <div className="font-medium text-sm text-secondary-text">
                  预测结果 Top 5{predictionDate ? <span className="text-tertiary-text ml-2 text-xs">数据日期: {predictionDate}</span> : ''}
                  {modelDisplayName ? <span className="text-tertiary-text ml-1 text-xs">| {modelDisplayName}</span> : ''}
                </div>
                <Button
                  size="small"
                  type="default"
                  onClick={handleCrossModelOverlap}
                  loading={overlapLoading}
                >
                  交叉验证 (全部)
                </Button>
              </div>
              {overlapData && (
                <div className="mb-2 text-xs text-tertiary-text">
                  共 {overlapData.total_models} 个模型，{overlapData.stocks.filter(s => s.count >= 3).length} 只股票出现在 ≥3 个模型的 Top 5 中
                </div>
              )}
              <Table
                size="small"
                dataSource={predictions}
                rowKey="ts_code"
                pagination={{ pageSize: 20, size: 'small' }}
                columns={predColumns}
                scroll={{ x: 400 }}
                rowClassName={(r) => overlapHighlight.has(r.ts_code) ? 'bg-amber-500/10' : ''}
              />
            </Card>
          )}

          {/* Backtest Simulation (from prediction files) */}
          <Card>
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-3">
                <div className="font-medium text-sm text-secondary-text">回测模拟（预测文件）</div>
                <Button
                  size="small"
                  type={catchUpStatus?.status === 'running' || catchUpPollRef.current ? 'default' : 'primary'}
                  onClick={handleCatchUpStart}
                  loading={catchUpStatus?.status === 'running' || !!catchUpPollRef.current}
                >
                  {catchUpStatus?.status === 'running' || catchUpPollRef.current
                    ? `补全中… ${catchUpStatus?.status_message || ''}`
                    : '补全预测'}
                </Button>
              </div>
              {labelMode === 'peak_speed' ? (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-tertiary-text">Top</span>
                  <InputNumber
                    size="small"
                    min={1}
                    max={5}
                    value={backtestTopN}
                    onChange={(v) => setBacktestTopN(v ?? 1)}
                    style={{ width: 52 }}
                  />
                  <span className="text-xs text-tertiary-text ml-1">止损</span>
                  <Segmented
                    size="small"
                    value={peakStopLoss.toString()}
                    onChange={(v) => setPeakStopLoss(Number(v))}
                    options={[
                      { label: '-10%', value: '-0.1' },
                      { label: '-15%', value: '-0.15' },
                      { label: '-20%', value: '-0.2' },
                    ]}
                  />
                </div>
              ) : (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-tertiary-text">Top</span>
                  <InputNumber
                    size="small"
                    min={1}
                    max={5}
                    value={backtestTopN}
                    onChange={(v) => setBacktestTopN(v ?? 1)}
                    style={{ width: 52 }}
                  />
                  <Segmented
                    size="small"
                    value={backtestFwd.toString()}
                    onChange={(v) => setBacktestFwd(Number(v))}
                    options={(() => {
                      const raw = backtestSimAvailable
                        ? (trainExecMode === 'open'
                          ? backtestSimAvailable.open
                          : backtestSimAvailable.close)
                        : [3, 5, 10];
                      const set = new Set(raw);
                      set.add(backtestFwd);
                      return [...set].sort((a, b) => a - b).map((d) => ({ label: `${d} 日`, value: String(d) }));
                    })()}
                  />
                  <Segmented
                    size="small"
                    value={stopStrategy}
                    onChange={(v) => setStopStrategy(v as string)}
                    options={[
                      { label: <AntTooltip title="到期日直接卖出，不判断盈亏">默认</AntTooltip>, value: 'none' },
                      { label: <AntTooltip title="持仓期间逐日检查，当日收盘亏损超过 10% 即卖出，不等到期日">亏损厌恶</AntTooltip>, value: 'loss_aversion' },
                      { label: <AntTooltip title="到期日若亏损，延长持仓最多20个交易日，回本即卖，否则到期强平">跌了死扛</AntTooltip>, value: 'dead_hold' },
                    ]}
                  />
                </div>
              )}
            </div>

            {backtestSimLoading && (
              <div className="flex items-center gap-2 text-sm text-tertiary-text py-4">
                <Loader2 className="h-4 w-4 animate-spin" />
                计算中...
              </div>
            )}

            {backtestSim && !backtestSimLoading && (
              <>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
                  <StatCard
                    label="总收益率"
                    value={pctNum(backtestSim.metrics.cumulative_return)}
                  />
                  <StatCard
                    label="胜率"
                    value={pctNum(backtestSim.metrics.win_rate)}
                  />
                  <StatCard
                    label="最大回撤"
                    value={pctNum(backtestSim.metrics.max_drawdown)}
                  />
                  <StatCard
                    label="已平仓"
                    value={String(backtestSim.metrics.total_trades)}
                  />
                  <StatCard
                    label="持仓中"
                    value={String(backtestSim.metrics.holding_trades || 0)}
                  />
                  <StatCard
                    label="跳过（涨停）"
                    value={String(backtestSim.metrics.skipped_trades)}
                  />
                </div>

                {backtestSim.capital_curve.length > 1 && (
                  <ResponsiveContainer width="100%" height={280} className="mb-4">
                    <LineChart data={backtestSim.capital_curve}>
                      <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                      <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
                      <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                      <Tooltip
                        contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }}
                        formatter={(value) => [Number(value).toFixed(4), '资金']}
                      />
                      <Line
                        type="monotone"
                        dataKey="capital"
                        name="资金曲线"
                        stroke="#22c55e"
                        strokeWidth={2}
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                )}

                {backtestSim.trades.length > 0 && (() => {
                  const holding = backtestSim.trades.filter((t) => !t.skipped && !t.sell_date);
                  const done = backtestSim.trades.filter((t) => !t.skipped && t.sell_date);
                  const skipped = backtestSim.trades.filter((t) => t.skipped);
                  const holdingAvgRet = holding.length > 0
                    ? holding.reduce((s, t) => s + t.return_pct, 0) / holding.length
                    : 0;
                  return (
                  <details open>
                    <summary className="cursor-pointer text-xs text-tertiary-text mb-2 select-none flex items-center gap-2">
                      <span>交易明细（{done.length} 笔已平仓{holding.length > 0 ? `，${holding.length} 笔持仓中 均收益 ${pctNum(holdingAvgRet)}` : ''}{skipped.length > 0 ? `，${skipped.length} 笔涨停跳过` : ''}）</span>
                      <Button size="small" type="link" onClick={() => exportBacktestExcel(backtestSim.trades, backtestSim.forward_days, backtestSim.top_n, backtestSim.exec_mode)}>
                        导出 Excel
                      </Button>
                    </summary>
                    <Table
                      size="small"
                      dataSource={backtestSim.trades.filter((t) => !t.skipped).sort((a, b) => a.pred_date.localeCompare(b.pred_date))}
                      rowKey={(r) => `${r.pred_date}_${r.stock_code}_${r.buy_date}_${r.sell_date}`}
                      pagination={{ pageSize: 50, size: 'small', showSizeChanger: true, pageSizeOptions: ['20', '50', '100'], showTotal: (total) => `共 ${total} 笔` }}
                      scroll={{ x: 900, y: 400 }}
                      columns={[
                        { title: '预测日', dataIndex: 'pred_date', key: 'pred_date', width: 85, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.pred_date },
                        { title: '股票', dataIndex: 'stock_name', key: 'stock', width: 90, render: (_: unknown, r: typeof backtestSim.trades[0]) => (
                          <div className="leading-tight">
                            <div>{r.stock_name || '--'}</div>
                            <div className="text-xs text-tertiary-text">{r.stock_code}</div>
                          </div>
                        )},
                        { title: '买入日', dataIndex: 'buy_date', key: 'buy_date', width: 85 },
                        { title: '买入价', dataIndex: 'buy_price', key: 'buy_price', width: 70, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.buy_price.toFixed(2) },
                        { title: '股数', dataIndex: 'shares', key: 'shares', width: 60, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.skipped ? '-' : (r.shares ? r.shares.toLocaleString() : '--') },
                        { title: '买入金额', dataIndex: 'actual_cost', key: 'actual_cost', width: 80, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.skipped ? '-' : (r.actual_cost ? `${(r.actual_cost / 10000).toFixed(2)}万` : '--') },
                        ...(labelMode === 'peak_speed' ? [{
                          title: '预期卖出日', dataIndex: 'expected_sell_date', key: 'expected_sell_date', width: 100,
                          render: (_: unknown, r: typeof backtestSim.trades[0]) => {
                            const d = (r as any).expected_sell_date;
                            const t = (r as any).target_return;
                            return d ? (
                              <div className="leading-tight">
                                <div>{d}</div>
                                {t != null && <div className="text-[11px] text-amber-400">+{(t * 100).toFixed(1)}%</div>}
                              </div>
                            ) : '--';
                          },
                        }] : []),
                        { title: '卖出日', dataIndex: 'sell_date', key: 'sell_date', width: 85, render: (_: unknown, r: typeof backtestSim.trades[0]) => {
                          if (r.skipped) return '（涨停）';
                          if (!r.sell_date) return '--';
                          let colorClass = 'text-white';
                          if (r.expected_sell_date) {
                            if (r.sell_date < r.expected_sell_date) colorClass = 'text-green-400';
                            else if (r.sell_date > r.expected_sell_date) colorClass = 'text-red-400';
                          }
                          return <span className={colorClass}>{r.sell_date}</span>;
                        }},
                        { title: '卖出价', dataIndex: 'sell_price', key: 'sell_price', width: 70, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.skipped ? '-' : (r.sell_date ? r.sell_price.toFixed(2) : r.sell_price.toFixed(2)) },
                        {
                          title: '收益', dataIndex: 'return_pct', key: 'return_pct', width: 70,
                          render: (_: unknown, r: typeof backtestSim.trades[0]) => {
                            if (r.skipped) return <span className="text-tertiary-text">-</span>;
                            const isHolding = !r.sell_date;
                            return (
                              <span className={r.return_pct >= 0 ? 'text-red-400' : 'text-green-400'}>
                                {pctNum(r.return_pct)}{isHolding ? ' *' : ''}
                              </span>
                            );
                          },
                        },
                      ]}
                    />
                  </details>
                  );
                })()}
              </>
            )}

            {!backtestSim && !backtestSimLoading && (
              <div className="text-xs text-tertiary-text">
                选择天数点击查询，系统将读取预测文件模拟实盘交易。
              </div>
            )}
          </Card>

          {/* Backtest Compare */}
          {backtest && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">回测对比</div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                {backtest.lgb_metrics && Object.entries(backtest.lgb_metrics).slice(0, 4).map(([k, v]) => (
                  <StatCard key={`lgb_${k}`} label={`LGB ${k}`} value={typeof v === 'number' ? v.toFixed(4) : String(v)} />
                ))}
                {backtest.comparison && Object.entries(backtest.comparison).slice(0, 4).map(([k, v]) => (
                  <StatCard key={`cmp_${k}`} label={k} value={typeof v === 'number' ? pctNum(v as number) : String(v)} />
                ))}
              </div>

              {capitalData.length > 0 && (
                <ResponsiveContainer width="100%" height={320}>
                  <LineChart data={capitalData}>
                    <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                    <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                    <Tooltip contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }} />
                    <Legend />
                    <Line type="monotone" dataKey="lgb" name="LightGBM" stroke="#3b82f6" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="benchmark" name="基准" stroke="#fbbf24" strokeWidth={1.5} strokeDasharray="3 3" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </Card>
          )}
        </div>
      </div>
    </AppPage>
  );
};

export default LightGBMPage;
