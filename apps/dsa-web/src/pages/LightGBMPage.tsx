import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  BarChart, Bar, CartesianGrid,
} from 'recharts';
import { DatePicker, Segmented, Table, InputNumber, Button, Select, Input, Tooltip as AntTooltip, Modal, message } from 'antd';
import { Brain, Play, Loader2, Search, Sparkles } from 'lucide-react';
import { AppPage, Card, StatCard, EmptyState, ApiErrorAlert } from '../components/common';
import { CapitalCurveTooltip, buildCapitalCurveChartMeta } from '../components/charts/CapitalCurveTooltip';
import { researchApi, type LGBTaskStatusResponse, type LGBPredictionItem, type LGBBacktestCompareResponse, type LGBModelInfo, type LGBDateRangeResponse, type LGBStockLookupItem, type LGBBacktestSimResponse, type LGBBacktestSimAvailableResponse, type LGBBruteForceTaskStatus, type LGBBruteForceItem, type LGBDiagnosticsResponse, type LGBCrossModelOverlapResponse, type LGBCrossModelOverlapStock, type CatchUpTaskStatus, type LGBBruteForceResult, type LGBFactorSubsetTaskStatus } from '../api/research';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import dayjs from 'dayjs';

function pctNum(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

const FACTOR_LABELS: Record<string, string> = {
  money_flow: '资金流向',
  margin: '融资融券',
  chip: '筹码分布',
  technical: '技术形态',
  limit: '涨跌停',
  momentum: '动量',
  rebound: '反弹',
  sector: '板块',
  ma_entry: '均线',
  fundamental: '基本面',
  popularity: '人气',
  hot_money: '游资',
  institution_hold: '机构持仓',
  profit_forecast: '盈利预测',
  performance: '业绩',
  buyback: '回购',
  insider_buy: '险资举牌',
  concept_heat: '概念热度',
  ranking_momentum: '排名动量',
  alpha042: '均值回归Alpha042',
  alpha60: 'Alpha60',
  vwap_deviation: 'VWAP偏离',
  gap_reversal: '跳空反转',
  liquid_oversold: '流动性超卖',
  vwap_reversal: 'VWAP动量反转',
  gtja114: 'GTJA114',
  broker_recommend: '券商推荐',
  money_flow_osc: '资金振荡',
};

function factorLabel(name: string): string {
  return FACTOR_LABELS[name] || name;
}

function exportBacktestExcel(trades: LGBBacktestSimResponse['trades'], forwardDays: number, topN: number, execMode: string, _stopStrategy?: string) {
  const rows = trades.filter((t) => !t.skipped);
  const header = ['预测日', '股票名称', '股票代码', '买入日', '买入价', '股数', '买入金额', '卖出日', '卖出价', '收益%'];
  const tf = (v: string | number) => `<td style="mso-number-format:\\@">${v}</td>`;
  const body = rows.map((t) => `<tr>${
    [
      tf(t.pred_date),
      `<td>${t.stock_name}</td>`,
      tf(t.stock_code),
      tf(t.buy_date),
      `<td>${t.buy_price.toFixed(2)}</td>`,
      `<td>${t.shares}</td>`,
      `<td>${t.actual_cost.toFixed(2)}</td>`,
      tf(t.sell_date || '持仓中'),
      `<td>${t.sell_price.toFixed(2)}</td>`,
      `<td>${(t.return_pct * 100).toFixed(2)}</td>`,
    ].join('')
  }</tr>`).join('');
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
  const [expandedPredKeys, setExpandedPredKeys] = useState<React.Key[]>([]);
  useEffect(() => { setExpandedPredKeys([]); setFinbertResults({}); }, [predictions]);
  const [backtest, setBacktest] = useState<LGBBacktestCompareResponse | null>(null);
  const [models, setModels] = useState<LGBModelInfo[]>([]);
  const [selectedModel, setSelectedModel] = useState<string | undefined>(undefined);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [dateRange, setDateRange] = useState<LGBDateRangeResponse | null>(null);
  const [stockCode, setStockCode] = useState('');
  const [stockLookup, setStockLookup] = useState<LGBStockLookupItem | null>(null);
  const [stockLookupItems, setStockLookupItems] = useState<LGBStockLookupItem[]>([]);
  const [expandedLookupTsCode, setExpandedLookupTsCode] = useState<string | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState('');
  const [newsExpanded, setNewsExpanded] = useState(false);
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
  const [bfActiveBest, setBfActiveBest] = useState<'return' | 'sharpe'>('return');
  const [latestBfReport, setLatestBfReport] = useState<LGBBruteForceResult | null>(null);
  const [bfReportLoading, setBfReportLoading] = useState(false);

  const [subsetSearchStatus, setSubsetSearchStatus] = useState<LGBFactorSubsetTaskStatus | null>(null);
  const [subsetSearchLoading, setSubsetSearchLoading] = useState(false);
  const subsetSearchPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [batchSearchStatus, setBatchSearchStatus] = useState<{ task_id: string; status: string; status_message: string; result: any; error: string } | null>(null);
  const [batchSearchLoading, setBatchSearchLoading] = useState(false);
  const batchSearchPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [rollingStatus, setRollingStatus] = useState<{ task_id: string; status: string; status_message: string; result?: { output: string } | null; error: string } | null>(null);
  const [rollingLoading, setRollingLoading] = useState(false);
  const rollingPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const bfAutoAppliedRef = useRef(false);
  const bfModelAutoSelectedRef = useRef(false);
  const [diagnostics, setDiagnostics] = useState<LGBDiagnosticsResponse | null>(null);
  const [catchUpStatus, setCatchUpStatus] = useState<CatchUpTaskStatus | null>(null);
  const catchUpPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Cross-model overlap
  const [overlapData, setOverlapData] = useState<LGBCrossModelOverlapResponse | null>(null);
  const [overlapLoading, setOverlapLoading] = useState(false);

  // Per-stock FinBERT on-demand
  const [finbertResults, setFinbertResults] = useState<Record<string, { finbert_label: string | null; finbert_score: number | null; finbert_summary: string | null; news_items: Array<{ title: string; snippet: string; source: string; url: string; date: string; sentiment_label?: string; sentiment_score?: number }> | null }>>({});
  const [finbertLoading, setFinbertLoading] = useState<Record<string, boolean>>({});
  const overlapHighlight = new Set(
    overlapData?.stocks.filter((s) => s.count >= 3).map((s) => s.ts_code) ?? [],
  );

  const fetchFinbert = useCallback(async (stockCode: string, stockName?: string) => {
    if (finbertResults[stockCode] || finbertLoading[stockCode]) return;
    setFinbertLoading(prev => ({ ...prev, [stockCode]: true }));
    try {
      const resp = await researchApi.getFinbertForStock(stockCode, stockName);
      const { finbert_label, finbert_score, finbert_summary, news_items } = resp;
      setFinbertResults(prev => ({ ...prev, [stockCode]: { finbert_label, finbert_score, finbert_summary, news_items } }));
      if (news_items?.length || finbert_summary) {
        setExpandedPredKeys(prev => prev.includes(stockCode) ? prev : [...prev, stockCode]);
      }
    } catch { /* ignore */ }
    finally { setFinbertLoading(prev => ({ ...prev, [stockCode]: false })); }
  }, [finbertResults, finbertLoading]);

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

  const applyBfBest = useCallback((best: LGBBruteForceItem | null | undefined) => {
    if (!best) return;
    setTrainExecMode(best.exec_mode);
    if (best.label_mode === 'peak_speed') {
      setLabelMode('peak_speed');
      setWindowDays(best.window_days || 20);
    } else {
      setLabelMode('fixed');
      setForwardDays(best.forward_days);
    }
    // Map brute force stop_strategy to valid backtest-sim values
    const rawSt = best.stop_strategy;
    const validSt = (rawSt === 'loss_aversion' || rawSt === 'dead_hold') ? rawSt : 'none';
    setStopStrategy(validSt);
    setBacktestTopN(best.top_n);
    setBacktestFwd(best.forward_days);
    // Reset model selection so auto-select picks the matching model
    setSelectedModel(undefined);
    bfModelAutoSelectedRef.current = false;
  }, []);

  /* ── Auto-apply best sharpe params from latest report ── */
  useEffect(() => {
    const best = latestBfReport?.best_by_sharpe || latestBfReport?.best_by_return;
    if (!best || bfAutoAppliedRef.current) return;
    bfAutoAppliedRef.current = true;
    applyBfBest(best);
    setBfActiveBest(latestBfReport?.best_by_sharpe ? 'sharpe' : 'return');
  }, [latestBfReport, applyBfBest]);

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

  const modelAbortRef = useRef<AbortController | null>(null);
  const [modelLoading, setModelLoading] = useState(false);

  const loadModelResults = useCallback(async (modelPath: string) => {
    // Cancel any in-flight request
    if (modelAbortRef.current) modelAbortRef.current.abort();
    const controller = new AbortController();
    modelAbortRef.current = controller;

    setError(null);
    setPredictions([]);
    setFeatureImportance([]);
    setDiagnostics(null);
    setStockLookup(null); setStockLookupItems([]); setExpandedLookupTsCode(null);
    setOverlapData(null);
    setBacktest(null);
    setModelLoading(true);

    // Fire feature importance first — render as soon as it returns
    const fiPromise = researchApi.getFeatureImportance(modelPath, controller.signal)
      .then((fi) => {
        if (controller.signal.aborted) return;
        const fiList = Object.keys(fi.gain).map((name) => ({
          name,
          gain: fi.gain[name],
          split: fi.split[name] ?? 0,
        })).sort((a, b) => b.gain - a.gain);
        setFeatureImportance(fiList);
      })
      .catch((e) => { if (!controller.signal.aborted) setError(getParsedApiError(e)); });

    // Predictions in parallel, loading state shown until it resolves
    const predPromise = researchApi.getPredictions(modelPath, controller.signal)
      .then((pred) => {
        if (controller.signal.aborted) return;
        setPredictions(pred.predictions);
        setPredictionDate(pred.model_date);
      })
      .catch((e) => { if (!controller.signal.aborted) setError(getParsedApiError(e)); });

    await Promise.allSettled([fiPromise, predPromise]);
    if (!controller.signal.aborted) setModelLoading(false);

    if (controller.signal.aborted) return;
    researchApi.getDiagnostics(modelPath).then(setDiagnostics).catch(() => {});
  }, []);

  /* ── Auto-select best model matching brute force best ── */
  useEffect(() => {
    const best = bfActiveBest === 'sharpe' ? latestBfReport?.best_by_sharpe : latestBfReport?.best_by_return;
    if (!best || !bfAutoAppliedRef.current || bfModelAutoSelectedRef.current) return;
    if (models.length === 0) return;
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
  }, [latestBfReport, models, selectedModel, loadModelResults, bfActiveBest]);

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
    setStockLookup(null); setStockLookupItems([]); setExpandedLookupTsCode(null);
    try {
      const resp = await researchApi.lookupStock(code, selectedModel);
      if (resp.found && resp.item) {
        setStockLookup(resp.item);
        setStockLookupItems(resp.items && resp.items.length > 0 ? resp.items : []);
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
      if (subsetSearchPollRef.current) clearInterval(subsetSearchPollRef.current);
      if (batchSearchPollRef.current) clearInterval(batchSearchPollRef.current);
      if (rollingPollRef.current) clearInterval(rollingPollRef.current);
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

  /* ── Factor Subset Search ── */
  const SS_TASK_KEY = 'lgb_subset_search_task_id';

  const pollSubsetSearch = useCallback((taskId: string): Promise<LGBFactorSubsetTaskStatus> => {
    return new Promise<LGBFactorSubsetTaskStatus>((resolve, reject) => {
      subsetSearchPollRef.current = setInterval(async () => {
        try {
          const status = await researchApi.getFactorSubsetSearchStatus(taskId);
          setSubsetSearchStatus(status);
          if (status.status === 'completed') {
            clearInterval(subsetSearchPollRef.current!);
            subsetSearchPollRef.current = null;
            localStorage.removeItem(SS_TASK_KEY);
            resolve(status);
          } else if (status.status === 'failed') {
            clearInterval(subsetSearchPollRef.current!);
            subsetSearchPollRef.current = null;
            localStorage.removeItem(SS_TASK_KEY);
            reject(new Error(status.error || '搜索失败'));
          }
        } catch (e) {
          clearInterval(subsetSearchPollRef.current!);
          subsetSearchPollRef.current = null;
          localStorage.removeItem(SS_TASK_KEY);
          reject(e);
        }
      }, 2000);
    });
  }, []);

  useEffect(() => {
    const stored = localStorage.getItem(SS_TASK_KEY);
    if (!stored) return;
    setSubsetSearchLoading(true);
    pollSubsetSearch(stored)
      .then((result) => setSubsetSearchStatus(result))
      .catch(() => {})
      .finally(() => setSubsetSearchLoading(false));
  }, [pollSubsetSearch]);

  const handleSubsetSearchStart = useCallback(async () => {
    setSubsetSearchLoading(true);
    setSubsetSearchStatus(null);
    try {
      const { task_id } = await researchApi.startFactorSubsetSearch({
        label_mode: labelMode,
        forward_days: forwardDays,
        window_days: windowDays,
        exec_mode: trainExecMode,
        mode: 'postmarket',
        tpe_trials: 80,
        top_n: backtestTopN,
      });
      localStorage.setItem(SS_TASK_KEY, task_id);
      const finalResult = await pollSubsetSearch(task_id);
      setSubsetSearchStatus(finalResult);
    } catch {
      // error already set via polling
    } finally {
      setSubsetSearchLoading(false);
    }
  }, [pollSubsetSearch, labelMode, forwardDays, windowDays, trainExecMode, backtestTopN]);

  const handleSubsetApply = useCallback(async () => {
    const result = subsetSearchStatus?.result;
    if (!result) return;
    Modal.confirm({
      title: '应用因子子集搜索结果',
      content: (
        <div className="space-y-2">
          <p>本次搜索将排除以下 {result.excluded_factors.length} 个因子：</p>
          <div className="text-xs bg-gray-800 rounded p-2">
            {result.excluded_factors.join(', ')}
          </div>
          <p className="text-xs text-gray-400">
            保留 {result.final_subset.length} 个因子，Rank IC: {result.final_ic.toFixed(4)}
            （基线: {result.baseline_ic.toFixed(4)}, Δ: {result.delta_ic >= 0 ? '+' : ''}{result.delta_ic.toFixed(4)}）
          </p>
          <p className="text-xs text-orange-400">
            注意：将与 .env 中已有的 LGB_DISABLE_FACTOR 合并（取并集），写入后需重启服务生效。
          </p>
        </div>
      ),
      okText: '确认应用',
      cancelText: '取消',
      onOk: async () => {
        try {
          const resp = await researchApi.applyFactorSubsetResult();
          if (resp.applied && resp.env_value) {
            message.success(`已合并写入 .env: ${resp.env_value}，重启服务后生效`);
          } else {
            message.success(resp.message || '已应用');
          }
        } catch {
          message.error('应用失败');
        }
      },
    });
  }, [subsetSearchStatus]);

  /* ── Factor Subset Batch Search ── */
  const BS_TASK_KEY = 'lgb_batch_search_task_id';

  const pollBatchSearch = useCallback((taskId: string) => {
    batchSearchPollRef.current = setInterval(async () => {
      try {
        const status = await researchApi.getFactorSubsetBatchStatus(taskId);
        setBatchSearchStatus(status);
        if (status.status === 'completed' || status.status === 'failed') {
          clearInterval(batchSearchPollRef.current!);
          batchSearchPollRef.current = null;
          localStorage.removeItem(BS_TASK_KEY);
          setBatchSearchLoading(false);
        }
      } catch {
        clearInterval(batchSearchPollRef.current!);
        batchSearchPollRef.current = null;
        localStorage.removeItem(BS_TASK_KEY);
        setBatchSearchLoading(false);
      }
    }, 5000);
  }, []);

  useEffect(() => {
    const stored = localStorage.getItem(BS_TASK_KEY);
    if (!stored) return;
    setBatchSearchLoading(true);
    pollBatchSearch(stored);
  }, [pollBatchSearch]);

  const handleBatchSearchStart = useCallback(async () => {
    setBatchSearchLoading(true);
    setBatchSearchStatus(null);
    try {
      const { task_id } = await researchApi.startFactorSubsetBatch();
      localStorage.setItem(BS_TASK_KEY, task_id);
      pollBatchSearch(task_id);
    } catch {
      setBatchSearchLoading(false);
    }
  }, [pollBatchSearch]);

  /* ── Rolling Backtest ── */
  const RB_TASK_KEY = 'lgb_rolling_task_id';

  const pollRolling = useCallback((taskId: string) => {
    rollingPollRef.current = setInterval(async () => {
      try {
        const status = await researchApi.getRollingBacktestStatus(taskId);
        setRollingStatus(status);
        if (status.status === 'completed' || status.status === 'failed') {
          clearInterval(rollingPollRef.current!);
          rollingPollRef.current = null;
          localStorage.removeItem(RB_TASK_KEY);
          setRollingLoading(false);
        }
      } catch {
        clearInterval(rollingPollRef.current!);
        rollingPollRef.current = null;
        localStorage.removeItem(RB_TASK_KEY);
        setRollingLoading(false);
      }
    }, 5000);
  }, []);

  useEffect(() => {
    const stored = localStorage.getItem(RB_TASK_KEY);
    if (!stored) return;
    setRollingLoading(true);
    pollRolling(stored);
  }, [pollRolling]);

  const handleRollingStart = useCallback(async () => {
    setRollingLoading(true);
    setRollingStatus(null);
    try {
      const { task_id } = await researchApi.startRollingBacktest();
      localStorage.setItem(RB_TASK_KEY, task_id);
      pollRolling(task_id);
    } catch {
      setRollingLoading(false);
    }
  }, [pollRolling]);

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

  const backtestSimChartMeta = useMemo(
    () => buildCapitalCurveChartMeta(backtestSim?.capital_curve ?? []),
    [backtestSim?.capital_curve],
  );

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
    {
      title: 'FinBERT 评价',
      key: 'finbert',
      width: 120,
      render: (_: unknown, r: LGBPredictionItem) => {
        const fb = finbertResults[r.ts_code] ?? (r.finbert_label != null ? r : null);
        const loading = finbertLoading[r.ts_code];
        if (loading) {
          return <span className="text-xs text-tertiary-text"><Loader2 className="inline w-3.5 h-3.5 animate-spin" /> 分析中…</span>;
        }
        if (!fb?.finbert_label) {
          return (
            <Button size="small" type="link" className="p-0 h-auto text-xs"
              onClick={(e) => { e.stopPropagation(); fetchFinbert(r.ts_code, r.stock_name || r.stock_code); }}>
              评价
            </Button>
          );
        }
        const labelMap: Record<string, string> = { positive: '正面', negative: '负面', neutral: '中性' };
        const colorMap: Record<string, string> = { positive: 'text-red-400', negative: 'text-green-400', neutral: 'text-purple-400' };
        const label = labelMap[fb.finbert_label] || fb.finbert_label;
        const score = fb.finbert_score != null ? `${fb.finbert_score >= 0 ? '+' : ''}${fb.finbert_score.toFixed(2)}` : '';
        const isExpanded = expandedPredKeys.includes(r.ts_code);
        const hasDetails = !!(fb.news_items?.length || fb.finbert_summary);
        return (
          <button
            type="button"
            className={`text-left ${hasDetails ? 'cursor-pointer hover:underline' : ''} ${colorMap[fb.finbert_label] || ''} font-medium`}
            onClick={hasDetails ? (e) => {
              e.stopPropagation();
              setExpandedPredKeys(prev =>
                isExpanded ? prev.filter(k => k !== r.ts_code) : [...prev, r.ts_code]
              );
            } : undefined}
          >
            {label} {score}
            {hasDetails && <span className="ml-1 text-[10px] opacity-60">{isExpanded ? '▲' : '▼'}</span>}
          </button>
        );
      },
    },
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
                onChange={(v) => { setTrainExecMode(v as string); setSelectedModel(undefined); setPredictions([]); setFeatureImportance([]); setDiagnostics(null); setStockLookup(null); setStockLookupItems([]); setExpandedLookupTsCode(null); setOverlapData(null); }}
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
                遍历 reports_lgb/ 缓存中所有参数组合（止损策略 × 执行模式 × 持有期 × top_n），寻找收益/夏普最优方案。后台运行，结果保存至 reports_lgb/。
                {bfReportLoading && !latestBfReport && (
                  <span className="text-blue-400 ml-1 inline-flex items-center gap-1">
                    <Loader2 className="h-3 w-3 animate-spin" />加载报告中...
                  </span>
                )}
                {latestBfReport && !bruteForceStatus && (
                  <span className="text-green-400 ml-1">
                    （已加载最新报告{latestBfReport.report_path ? `: ${latestBfReport.report_path.split('/').pop()}` : ''}，点击下方卡片切换最佳收益/夏普参数）
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
                    <button
                      type="button"
                      className={`w-full p-2 rounded border text-left transition-colors ${bfActiveBest === 'return' ? 'bg-green-500/20 border-green-500/50' : 'bg-green-500/10 border-green-500/20 hover:bg-green-500/15 cursor-pointer'}`}
                      onClick={() => { setBfActiveBest('return'); applyBfBest(displayResult.best_by_return); }}
                    >
                      <div className="flex items-center justify-between">
                        <span className="text-tertiary-text">最佳收益</span>
                        {bfActiveBest === 'return' && <span className="text-green-400 text-[10px]">● 已应用</span>}
                      </div>
                      <div className="font-medium">
                        {displayResult.best_by_return.stop_strategy} {displayResult.best_by_return.exec_mode} {modeLabel(displayResult.best_by_return)} top={displayResult.best_by_return.top_n}
                      </div>
                      <div className="text-red-400">
                        {(displayResult.best_by_return.cumulative_return * 100).toFixed(1)}%
                      </div>
                    </button>
                  )}
                  {displayResult.best_by_sharpe && (
                    <button
                      type="button"
                      className={`w-full p-2 rounded border text-left transition-colors ${bfActiveBest === 'sharpe' ? 'bg-blue-500/20 border-blue-500/50' : 'bg-blue-500/10 border-blue-500/20 hover:bg-blue-500/15 cursor-pointer'}`}
                      onClick={() => { setBfActiveBest('sharpe'); applyBfBest(displayResult.best_by_sharpe); }}
                    >
                      <div className="flex items-center justify-between">
                        <span className="text-tertiary-text">最佳夏普</span>
                        {bfActiveBest === 'sharpe' && <span className="text-blue-400 text-[10px]">● 已应用</span>}
                      </div>
                      <div className="font-medium">
                        {displayResult.best_by_sharpe.stop_strategy} {displayResult.best_by_sharpe.exec_mode} {modeLabel(displayResult.best_by_sharpe)} top={displayResult.best_by_sharpe.top_n}
                      </div>
                      <div className="text-blue-400">
                        {displayResult.best_by_sharpe.sharpe_ratio.toFixed(2)}
                      </div>
                    </button>
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

          {/* Factor Subset Search */}
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">因子最优组合搜索</div>
              <div className="text-xs text-tertiary-text">
                三阶段搜索: 基线重要性 → 贪心前向选择 → Optuna TPE 精调。优化目标: 日均收益率最高。自动寻找最优因子子集，结果保存至 reports_lgb/factor_subset/。
              </div>

              {subsetSearchStatus && subsetSearchStatus.status === 'completed' && subsetSearchStatus.result && (
                <div className="space-y-2">
                  <div className="border-t border-white/5" />
                  <div className="text-xs space-y-1">
                    <div className="flex justify-between">
                      <span>基线日均收益 (全部因子)</span>
                      <span>{(subsetSearchStatus.result.baseline_daily_return * 100).toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span>最优日均收益 ({subsetSearchStatus.result.final_subset.length}因子)</span>
                      <span className={subsetSearchStatus.result.delta_daily_return >= 0 ? 'text-green-400' : 'text-red-400'}>
                        {(subsetSearchStatus.result.final_daily_return * 100).toFixed(2)}%
                        <span className="ml-1 text-[10px]">
                          ({subsetSearchStatus.result.delta_daily_return >= 0 ? '+' : ''}{(subsetSearchStatus.result.delta_daily_return * 100).toFixed(2)}%)
                        </span>
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span>Rank IC</span>
                      <span>{subsetSearchStatus.result.final_ic.toFixed(4)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>排除因子</span>
                      <span className="text-tertiary-text">{subsetSearchStatus.result.excluded_factors.length} 个</span>
                    </div>
                  </div>
                  <Button
                    block
                    size="small"
                    type="default"
                    icon={<Sparkles className="h-3.5 w-3.5" />}
                    onClick={handleSubsetApply}
                  >
                    应用最优因子组合
                  </Button>
                </div>
              )}

              {subsetSearchStatus && subsetSearchStatus.status === 'running' && (
                <div className="text-xs text-blue-400 flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  {subsetSearchStatus.status_message || '搜索中...'}
                </div>
              )}

              {subsetSearchStatus && subsetSearchStatus.status === 'failed' && (
                <div className="text-xs text-red-400">
                  {subsetSearchStatus.error || '搜索失败'}
                </div>
              )}

              <Button
                block
                size="small"
                type="primary"
                icon={subsetSearchLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                onClick={handleSubsetSearchStart}
                disabled={subsetSearchLoading || subsetSearchStatus?.status === 'running'}
                loading={subsetSearchLoading}
              >
                {subsetSearchLoading ? '搜索中...' : '搜索最优因子组合'}
              </Button>
            </div>
          </Card>

          {/* Batch Factor Subset Search */}
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">批量因子搜索</div>
              <div className="text-xs text-tertiary-text">
                测试 20 种参数组合（open/close × fixed3d/5d/10d/20d + peak20d × 2种top_n），数据使用最新 250 个交易日。按日均收益排名，每组内部自动选最优 top_n。
              </div>

              {batchSearchStatus && batchSearchStatus.status === 'completed' && batchSearchStatus.result && (
                <div className="space-y-2">
                  <div className="border-t border-white/5" />
                  <div className="text-xs overflow-x-auto">
                    <table className="w-full">
                      <thead>
                        <tr className="text-tertiary-text">
                          <th className="text-left pr-2">#</th>
                          <th className="text-left pr-2">执行</th>
                          <th className="text-left pr-2">模式</th>
                          <th className="text-left pr-2">持有</th>
                          <th className="text-left pr-2">Top</th>
                          <th className="text-right pr-2">日均收益</th>
                          <th className="text-right pr-2">IC</th>
                          <th className="text-right">因子</th>
                        </tr>
                      </thead>
                      <tbody>
                        {batchSearchStatus.result.summary.slice(0, 20).map((item: any) => (
                          <tr key={item.rank} className={item.rank === 1 ? 'text-green-400 font-medium' : ''}>
                            <td className="pr-2">{item.rank}</td>
                            <td className="pr-2">{item.exec_mode || '-'}</td>
                            <td className="pr-2">{item.label_mode === 'fixed' ? 'fixed' : 'peak'}</td>
                            <td className="pr-2">{item.label_mode === 'fixed' ? `${item.forward_days}d` : '-'}</td>
                            <td className="pr-2">{item.top_n}</td>
                            <td className="text-right pr-2">{(item.daily_return_mean * 100).toFixed(2)}%</td>
                            <td className="text-right pr-2">{item.rank_ic_mean.toFixed(4)}</td>
                            <td className="text-right">{item.n_factors}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {batchSearchStatus.result.best && (
                    <div className="text-xs space-y-1">
                      <div className="text-green-400 font-medium">
                        最优: {batchSearchStatus.result.best.exec_mode || ''} {' '}
                        {batchSearchStatus.result.best.label_mode === 'fixed' ? 'fixed' : 'peak'}
                        {batchSearchStatus.result.best.label_mode === 'fixed' ? ` ${batchSearchStatus.result.best.forward_days}d` : ''}
                        {' '}top{batchSearchStatus.result.best.top_n}
                        {' '}{(batchSearchStatus.result.best.daily_return_mean * 100).toFixed(2)}%
                      </div>
                      <div className="text-tertiary-text">
                        因子: {batchSearchStatus.result.best.final_subset.join(', ')}
                      </div>
                    </div>
                  )}
                </div>
              )}

              {batchSearchStatus && batchSearchStatus.status === 'running' && (
                <div className="text-xs text-blue-400 flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  {batchSearchStatus.status_message || '搜索中...'}
                </div>
              )}

              {batchSearchStatus && batchSearchStatus.status === 'failed' && (
                <div className="text-xs text-red-400">
                  {batchSearchStatus.error || '搜索失败'}
                </div>
              )}

              <Button
                block
                size="small"
                type="primary"
                icon={batchSearchLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
                onClick={handleBatchSearchStart}
                disabled={batchSearchLoading || batchSearchStatus?.status === 'running'}
                loading={batchSearchLoading}
              >
                {batchSearchLoading ? '搜索中...' : '开始批量搜索'}
              </Button>
            </div>
          </Card>

          {/* Rolling Backtest */}
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">滚动窗口回测</div>
              <div className="text-xs text-tertiary-text">
                自动发现 factor_subset/ 下的因子配置，按月滚动训练并预测每日 Top 5，报告保存至 reports_lgb/。
              </div>

              {rollingStatus && rollingStatus.status === 'completed' && (
                <div className="space-y-2">
                  <div className="border-t border-white/5" />
                  <div className="text-xs text-green-400">
                    滚动回测完成
                  </div>
                  {rollingStatus.result?.output && (
                    <pre className="text-xs text-tertiary-text bg-gray-900 rounded p-2 max-h-40 overflow-y-auto whitespace-pre-wrap">
                      {rollingStatus.result.output}
                    </pre>
                  )}
                </div>
              )}

              {rollingStatus && rollingStatus.status === 'running' && (
                <div className="text-xs text-blue-400 flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  {rollingStatus.status_message || '运行中...'}
                </div>
              )}

              {rollingStatus && rollingStatus.status === 'failed' && (
                <div className="text-xs text-red-400">
                  {rollingStatus.error || '回测失败'}
                </div>
              )}

              <Button
                block
                size="small"
                type="primary"
                icon={rollingLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                onClick={handleRollingStart}
                disabled={rollingLoading || rollingStatus?.status === 'running'}
                loading={rollingLoading}
              >
                {rollingLoading ? '运行中...' : '运行滚动回测'}
              </Button>
            </div>
          </Card>
        </div>

        {/* ──── Right Panel ──── */}
        <div className="flex-1 min-w-0 space-y-4">
          {error && <ApiErrorAlert error={error} />}

          {!featureImportance.length && !predictions.length && !backtest && !training && !modelLoading && (
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
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 11 }} width={75} tickFormatter={factorLabel} />
                  <Tooltip
                    contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }}
                    formatter={(value, _name, props) => [Number(value).toFixed(4), `${factorLabel(props.payload.name)} (${props.payload.name})`]}
                    labelFormatter={(label) => factorLabel(label as string)}
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
              {stockLookupItems.length > 1 && (
                <div className="mt-3 space-y-0.5">
                  <div className="text-xs text-tertiary-text mb-1">匹配到 {stockLookupItems.length} 只股票：</div>
                  {stockLookupItems.map((item) => {
                    const expanded = expandedLookupTsCode === item.ts_code;
                    const fb = finbertResults[item.ts_code];
                    const fbLoading = finbertLoading[item.ts_code];
                    return (
                      <div key={item.ts_code} className="rounded border border-white/5 overflow-hidden">
                        {/* Header row */}
                        <div
                          className="flex items-center gap-3 px-2 py-1.5 text-xs cursor-pointer hover:bg-white/5 transition-colors"
                          onClick={() => {
                            const next = expanded ? null : item.ts_code;
                            setExpandedLookupTsCode(next);
                            if (next) fetchFinbert(item.ts_code, item.stock_name);
                          }}
                        >
                          <span className="text-tertiary-text w-3 shrink-0">{expanded ? '▼' : '▶'}</span>
                          <span className="font-medium text-secondary-text w-20 shrink-0">{item.ts_code}</span>
                          <span className="text-secondary-text truncate flex-1">{item.stock_name}</span>
                          <span className="text-blue-400 w-14 text-right shrink-0">{item.lgb_score.toFixed(2)}</span>
                          <span className="text-tertiary-text w-20 text-right shrink-0">{item.rank}/{item.total_stocks}</span>
                        </div>
                        {/* Expanded detail */}
                        {expanded && (
                          <div className="px-3 pb-3 pt-1 bg-blue-500/5 border-t border-white/5">
                            <div className="grid grid-cols-3 md:grid-cols-5 gap-2 text-xs">
                              <div>
                                <span className="text-tertiary-text">LGB 评分</span>
                                <div className="font-medium text-sm text-blue-400">{item.lgb_score.toFixed(2)}</div>
                              </div>
                              <div>
                                <span className="text-tertiary-text">全市场排名</span>
                                <div className="font-medium text-sm">{item.rank} / {item.total_stocks}</div>
                              </div>
                              <div>
                                <span className="text-tertiary-text">百分位</span>
                                <div className="font-medium text-sm">Top {(item.rank / item.total_stocks * 100).toFixed(1)}%</div>
                              </div>
                              <div>
                                <span className="text-tertiary-text">原始分</span>
                                <div className="font-medium text-sm">{item.raw_score.toFixed(4)}</div>
                              </div>
                              <div>
                                <span className="text-tertiary-text">FinBERT</span>
                                {fbLoading ? (
                                  <div className="text-xs text-tertiary-text"><Loader2 className="h-3 w-3 animate-spin inline" /> 加载中</div>
                                ) : fb ? (
                                  <div>
                                    <span className={`font-medium text-sm ${
                                      fb.finbert_label === 'positive' ? 'text-green-400' :
                                      fb.finbert_label === 'negative' ? 'text-red-400' : 'text-yellow-400'
                                    }`}>
                                      {fb.finbert_label === 'positive' ? '正面' :
                                       fb.finbert_label === 'negative' ? '负面' : '中性'}
                                      {fb.finbert_score != null && ` (${fb.finbert_score > 0 ? '+' : ''}${fb.finbert_score.toFixed(2)})`}
                                    </span>
                                  </div>
                                ) : (
                                  <div className="text-xs text-tertiary-text">-</div>
                                )}
                              </div>
                            </div>
                            {fb?.finbert_summary && (
                              <div className="mt-2 text-xs text-secondary-text">{fb.finbert_summary}</div>
                            )}
                            {fb?.news_items && fb.news_items.length > 0 && (
                              <div className="mt-2 max-h-32 overflow-y-auto space-y-1">
                                {fb.news_items.map((n, i) => (
                                  <div key={i} className="text-xs py-1 px-2 rounded bg-black/20 border border-white/5">
                                    <div className="text-secondary-text">{n.title}</div>
                                    {n.snippet && <div className="text-tertiary-text mt-0.5 line-clamp-1">{n.snippet}</div>}
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
              {stockLookup && stockLookupItems.length === 0 && (
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
                  {stockLookup.finbert_sentiment && (
                    <div className="mt-3 pt-3 border-t border-blue-500/20">
                      <div className="text-xs text-tertiary-text mb-1">FinBERT 新闻情感</div>
                      <div className="flex items-center gap-3 text-xs">
                        <span className={`font-medium text-sm ${
                          stockLookup.finbert_sentiment.overall_label === 'positive' ? 'text-green-400' :
                          stockLookup.finbert_sentiment.overall_label === 'negative' ? 'text-red-400' :
                          'text-yellow-400'
                        }`}>
                          {stockLookup.finbert_sentiment.overall_label === 'positive' ? '正面' :
                           stockLookup.finbert_sentiment.overall_label === 'negative' ? '负面' : '中性'}
                          {stockLookup.finbert_sentiment.overall_score != null &&
                            ` (${stockLookup.finbert_sentiment.overall_score > 0 ? '+' : ''}${stockLookup.finbert_sentiment.overall_score.toFixed(2)})`}
                        </span>
                        <span className="text-green-400">正面 {stockLookup.finbert_sentiment.positive_count ?? 0}</span>
                        <span className="text-red-400">负面 {stockLookup.finbert_sentiment.negative_count ?? 0}</span>
                        <span className="text-yellow-400">中性 {stockLookup.finbert_sentiment.neutral_count ?? 0}</span>
                      </div>
                      {stockLookup.finbert_sentiment.summary && (
                        <div className="mt-1 text-xs text-secondary-text">{stockLookup.finbert_sentiment.summary}</div>
                      )}
                      {stockLookup.finbert_sentiment.news_items && stockLookup.finbert_sentiment.news_items.length > 0 && (
                        <div className="mt-2">
                          <button
                            type="button"
                            className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
                            onClick={() => setNewsExpanded(!newsExpanded)}
                          >
                            {newsExpanded ? '收起新闻详情 ▲' : `展开新闻详情（${stockLookup.finbert_sentiment.news_items.length} 条）▼`}
                          </button>
                          {newsExpanded && (
                            <div className="mt-2 max-h-64 overflow-y-auto space-y-1.5">
                              {stockLookup.finbert_sentiment.news_items.map((item, i) => (
                                <div key={i} className="text-xs py-1.5 px-2 rounded bg-black/20 border border-white/5">
                                  <div className="flex items-start gap-1.5">
                                    {item.sentiment_label && (
                                      <span className={`mt-0.5 w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                                        item.sentiment_label === 'positive' ? 'bg-green-400' :
                                        item.sentiment_label === 'negative' ? 'bg-red-400' : 'bg-yellow-400'
                                      }`} />
                                    )}
                                    <div className="min-w-0">
                                      <div className="text-secondary-text leading-snug">{item.title}</div>
                                      {item.snippet && (
                                        <div className="text-tertiary-text mt-0.5 leading-snug line-clamp-2">{item.snippet}</div>
                                      )}
                                      <div className="flex items-center gap-2 mt-1 text-tertiary-text/60">
                                        {item.source && <span>{item.source}</span>}
                                        {item.date && <span>{item.date}</span>}
                                        {item.sentiment_label && (
                                          <span className={`${
                                            item.sentiment_label === 'positive' ? 'text-green-400/80' :
                                            item.sentiment_label === 'negative' ? 'text-red-400/80' : 'text-yellow-400/80'
                                          }`}>
                                            {item.sentiment_label === 'positive' ? '正面' :
                                             item.sentiment_label === 'negative' ? '负面' : '中性'}
                                            {item.sentiment_score != null && ` ${(item.sentiment_score * 100).toFixed(0)}%`}
                                          </span>
                                        )}
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </Card>
          )}

          {/* Predictions */}
          {(predictions.length > 0 || modelLoading) && (
            <Card>
              <div className="flex items-center justify-between mb-3">
                <div className="font-medium text-sm text-secondary-text">
                  预测结果 Top 5{predictionDate ? <span className="text-tertiary-text ml-2 text-xs">数据日期: {predictionDate}</span> : ''}
                  {modelDisplayName ? <span className="text-tertiary-text ml-1 text-xs">| {modelDisplayName}</span> : ''}
                  {modelLoading && predictions.length === 0 && <span className="text-blue-400 ml-2 inline-flex items-center gap-1 text-xs"><Loader2 className="h-3 w-3 animate-spin" />加载中…</span>}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    size="small"
                    type="default"
                    icon={predictLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : undefined}
                    onClick={handlePredictTop5}
                    loading={predictLoading}
                  >
                    刷新
                  </Button>
                  <Button
                    size="small"
                    type="default"
                    onClick={handleCrossModelOverlap}
                    loading={overlapLoading}
                  >
                    交叉验证 (全部)
                  </Button>
                </div>
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
                pagination={false}
                columns={predColumns}
                scroll={{ x: 400 }}
                rowClassName={(r) => overlapHighlight.has(r.ts_code) ? 'bg-amber-500/10' : ''}
                expandable={{
                  expandedRowKeys: expandedPredKeys,
                  onExpand: (_expanded, record) => {
                    setExpandedPredKeys(prev =>
                      prev.includes(record.ts_code)
                        ? prev.filter(k => k !== record.ts_code)
                        : [...prev, record.ts_code]
                    );
                  },
                  expandedRowRender: (r: LGBPredictionItem) => {
                    const fb = finbertResults[r.ts_code] ?? r;
                    if (!fb.news_items?.length && !fb.finbert_summary) return null;
                    const sentimentColor: Record<string, string> = { positive: 'text-red-400', negative: 'text-green-400', neutral: 'text-purple-400' };
                    const sentimentLabel: Record<string, string> = { positive: '正面', negative: '负面', neutral: '中性' };
                    return (
                      <div className="px-2 py-2 space-y-2">
                        {fb.finbert_summary && (
                          <div className="text-xs text-secondary-text">{fb.finbert_summary}</div>
                        )}
                        {fb.news_items && fb.news_items.length > 0 && (
                          <div className="space-y-1.5">
                            {fb.news_items.map((n, i) => (
                              <div key={i} className="flex gap-2 text-xs">
                                <span className={`shrink-0 ${sentimentColor[n.sentiment_label || ''] || 'text-tertiary-text'}`}>
                                  {sentimentLabel[n.sentiment_label || ''] || '-'}
                                </span>
                                <div className="min-w-0">
                                  <span className="font-medium">{n.title}</span>
                                  {n.snippet && <span className="text-tertiary-text ml-1">{n.snippet.slice(0, 80)}{n.snippet.length > 80 ? '...' : ''}</span>}
                                  {n.date && <span className="text-tertiary-text ml-1">({n.date})</span>}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  },
                  showExpandColumn: false,
                }}
              />
              {overlapData && overlapData.stocks.length > 0 && (
                <div className="mt-4">
                  <div className="font-medium text-sm text-secondary-text mb-2">
                    交叉命中个股（≥3 个模型）
                  </div>
                  <Table
                    size="small"
                    dataSource={overlapData.stocks.filter(s => s.count >= 3)}
                    rowKey="ts_code"
                    pagination={false}
                    columns={[
                      { title: '股票', key: 'stock', width: 130, render: (_: unknown, r: LGBCrossModelOverlapStock) => (
                        <div className="leading-tight">
                          <div>{r.stock_name}</div>
                          <div className="text-xs text-secondary-text">{r.ts_code}</div>
                        </div>
                      )},
                      { title: '命中', dataIndex: 'count', key: 'count', width: 55 },
                      { title: '来源模型', dataIndex: 'model_names', key: 'model_names', render: (_: unknown, r: LGBCrossModelOverlapStock) => (
                        <div className="text-xs leading-snug">{r.model_names.join(', ')}</div>
                      )},
                    ]}
                    scroll={{ x: 500 }}
                  />
                </div>
              )}
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
                        content={(
                          <CapitalCurveTooltip
                            latestByKey={backtestSimChartMeta.latestByKey}
                            latestDate={backtestSimChartMeta.latestDate}
                            baseByKey={backtestSimChartMeta.baseByKey}
                          />
                        )}
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
                      <Button size="small" type="link" onClick={() => exportBacktestExcel(backtestSim.trades, backtestSim.forward_days, backtestSim.top_n, backtestSim.exec_mode, stopStrategy)}>
                        导出 Excel
                      </Button>
                    </summary>
                    <Table
                      size="small"
                      dataSource={backtestSim.trades.filter((t) => !t.skipped).sort((a, b) => a.pred_date.localeCompare(b.pred_date))}
                      rowKey={(r) => `${r.pred_date}_${r.stock_code}_${r.buy_date}_${r.sell_date}`}
                      pagination={{ pageSize: 100, size: 'small', showSizeChanger: true, pageSizeOptions: ['20', '50', '100', '200'], showTotal: (total) => `共 ${total} 笔` }}
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
                        { title: '股数', dataIndex: 'shares', key: 'shares', width: 75, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.skipped ? '-' : (r.shares ? r.shares.toLocaleString() : '--') },
                        { title: '买入金额', dataIndex: 'actual_cost', key: 'actual_cost', width: 80, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.skipped ? '-' : (r.actual_cost ? `${(r.actual_cost / 10000).toFixed(2)}万` : '--') },
                        { title: '目标收益', dataIndex: 'target_return', key: 'target_return', width: 75, render: (_: unknown, r: typeof backtestSim.trades[0]) => {
                          const t = (r as any).target_return;
                          return t ? <span className="text-amber-400">+{(t * 100).toFixed(1)}%</span> : '--';
                        }},
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
