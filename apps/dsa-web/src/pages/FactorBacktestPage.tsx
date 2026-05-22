import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import dayjs from 'dayjs';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  BarChart, Bar, CartesianGrid,
} from 'recharts';
import { DatePicker, Segmented, Table, InputNumber, Checkbox, Switch } from 'antd';
import { Activity, Download, Play, Loader2 } from 'lucide-react';
import { AppPage, Card, StatCard, EmptyState, ApiErrorAlert } from '../components/common';
import { discoveryApi, type FactorSnapshotDatesResponse, type FactorBacktestResultResponse, type FactorBacktestCapitalPoint, type FactorBacktestTrade } from '../api/discovery';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';

type TabKey = 'intraday' | 'postmarket';

const HOLD_DAY_OPTIONS = [
  { label: '1日', value: 1 },
  { label: '3日', value: 3 },
  { label: '5日', value: 5 },
  { label: '10日', value: 10 },
  { label: '20日', value: 20 },
  { label: '60日', value: 60 },
];

function pct(v: number): string {
  return `${(v * 100).toFixed(2)}%`;
}

function pctNum(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

function fmtMoney(v: number): string {
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(0)}万`;
  return v.toFixed(0);
}

const CAPITAL_COLORS = ['#22c55e', '#3b82f6', '#f59e0b', '#8b5cf6', '#ef4444'];

const BT_TASK_KEY = 'factor_backtest_task';
const BT_RESULT_KEY = 'factor_backtest_result';
const BT_PARAMS_KEY = 'factor_backtest_params';

const FactorBacktestPage: React.FC = () => {
  const isOwnerRef = useRef(false);
  const abortRef = useRef(false);
  const taskIdRef = useRef('');

  const [mode, setMode] = useState<TabKey>('postmarket');
  const [loading, setLoading] = useState(false);
  const [snapLoading, setSnapLoading] = useState(false);
  const [progressMsg, setProgressMsg] = useState('');
  const [error, setError] = useState<ParsedApiError | null>(null);

  // snapshot data
  const [snapData, setSnapData] = useState<FactorSnapshotDatesResponse | null>(null);

  // factor selection
  const [selectedFactors, setSelectedFactors] = useState<Record<string, boolean>>({});
  const [factorWeights, setFactorWeights] = useState<Record<string, number>>({});
  const [dateRangeIntersection, setDateRangeIntersection] = useState<[string, string]>(['', '']);

  // params
  const [holdDays, setHoldDays] = useState<number[]>([1, 5]);
  const [topN, setTopN] = useState(1);
  const [startDate, setStartDate] = useState<string>('');
  const [endDate, setEndDate] = useState<string>('');
  const [initialCapital, setInitialCapital] = useState(1_000_000);
  const [riskFreeRate, setRiskFreeRate] = useState(2.0);
  const [usePipeline, setUsePipeline] = useState(true);
  const [blendAlpha, setBlendAlpha] = useState(0.3);
  const [reoptimize, setReoptimize] = useState(false);
  const [reoptimizeInterval, setReoptimizeInterval] = useState(10);
  const [optWindow, setOptWindow] = useState(60);
  const [quickRange, setQuickRange] = useState<number | null>(null);


  // result
  const [result, setResult] = useState<FactorBacktestResultResponse | null>(null);
  const [summaryPeriod, setSummaryPeriod] = useState('1');
  const [selectedCurves, setSelectedCurves] = useState<Record<string, boolean>>({});

  // load snapshot dates on mount + mode change
  useEffect(() => {
    let cancelled = false;
    setSnapLoading(true);
    discoveryApi.getFactorSnapshotDates(mode).then((data) => {
      if (cancelled) return;
      setSnapData(data);
      setError(null);
      setSnapLoading(false);
      // 恢复缓存的回测结果（无活跃任务 & 模式匹配时）
      let restored = false;
      const taskRaw = localStorage.getItem(BT_TASK_KEY);
      if (!taskRaw) {
        const cachedRaw = localStorage.getItem(BT_RESULT_KEY);
        if (cachedRaw) {
          try {
            const cached = JSON.parse(cachedRaw);
            if (cached.task_id && cached.mode === mode) {
              // 重新从 API 获取结果（localStorage 不存大数据）
              discoveryApi.getFactorBacktestStatus(cached.task_id).then((status) => {
                if (status.status === 'completed' && status.result) {
                  setResult(status.result);
                  const sc: Record<string, boolean> = {};
                  const curveKeys = Object.keys(status.result.capital_curves);
                  for (const k of curveKeys) sc[k] = true;
                  setSelectedCurves(sc);
                  if (curveKeys.length > 0) setSummaryPeriod(curveKeys[0]);
                }
              }).catch(() => {
                localStorage.removeItem(BT_RESULT_KEY);
              });
              restored = true;
            } else {
              setResult(null);
            }
          } catch { setResult(null); }
        } else {
          setResult(null);
        }
      } else {
        setResult(null);
      }
      // init selection: all checked, use .env live weights from merged response
      const sf: Record<string, boolean> = {};
      const fw: Record<string, number> = {};
      for (const f of data.factors) {
        sf[f.name] = true;
        fw[f.name] = data.weights[f.name] ?? f.default_weight ?? 0;
      }
      setSelectedFactors(sf);
      setFactorWeights(fw);
      if (data.global.available_from) {
        setStartDate(data.global.available_from);
        setEndDate(data.global.available_to);
      }
      // 回填上次回测参数
      if (!restored) {
        const paramsRaw = localStorage.getItem(BT_PARAMS_KEY);
        if (paramsRaw) {
          try {
            const p = JSON.parse(paramsRaw);
            if (p.mode === mode) {
              if (p.holdDays) setHoldDays(p.holdDays);
              if (p.topN != null) setTopN(p.topN);
              if (p.startDate) setStartDate(p.startDate);
              if (p.endDate) setEndDate(p.endDate);
              if (p.initialCapital) setInitialCapital(p.initialCapital);
              if (p.riskFreeRate != null) setRiskFreeRate(p.riskFreeRate);
              if (p.usePipeline != null) setUsePipeline(p.usePipeline);
              if (p.blendAlpha != null) setBlendAlpha(p.blendAlpha);
              if (p.reoptimize != null) setReoptimize(p.reoptimize);
              if (p.reoptimizeInterval != null) setReoptimizeInterval(p.reoptimizeInterval);
              if (p.optWindow != null) setOptWindow(p.optWindow);
              // 因子选择与权重：仅恢复快照中仍存在的因子
              if (p.selectedFactors) setSelectedFactors((prev) => {
                const next = { ...prev };
                for (const k of Object.keys(next)) next[k] = false;
                for (const k of Object.keys(p.selectedFactors)) {
                  if (p.selectedFactors[k] && k in next) next[k] = true;
                }
                return next;
              });
            }
          } catch { /* ignore parse errors */ }
        }
      }
      if (!restored) setSelectedCurves({});
    }).catch((e) => {
      if (!cancelled) { setError(getParsedApiError(e)); setSnapLoading(false); }
    });
    return () => { cancelled = true; };
  }, [mode]);

  /* 同步 usePipeline / blendAlpha 与当前模式的实际管线配置（无已保存参数时） */
  useEffect(() => {
    const paramsRaw = localStorage.getItem(BT_PARAMS_KEY);
    if (paramsRaw) {
      try {
        const p = JSON.parse(paramsRaw);
        if (p.mode === mode) return; // 已有保存参数，跳过默认覆盖
      } catch { /* continue */ }
    }
    let cancelled = false;
    discoveryApi.getPipelineConfig().then((data) => {
      if (cancelled) return;
      const enabled = mode === 'intraday'
        ? (data.intraday_pipeline_enabled ?? true)
        : (data.postmarket_pipeline_enabled ?? true);
      setUsePipeline(enabled);
      if (data.score_blend_alpha != null) setBlendAlpha(data.score_blend_alpha);
    }).catch(() => { /* 忽略 */ });
    return () => { cancelled = true; };
  }, [mode]);

  // quick range → auto-set start/end dates from trading_dates
  useEffect(() => {
    if (!quickRange || !snapData?.global.trading_dates?.length) return;
    const tds = snapData.global.trading_dates;
    const end = tds[tds.length - 1];
    const startIdx = Math.max(0, tds.length - quickRange);
    const start = tds[startIdx];
    setStartDate(start);
    setEndDate(end);
  }, [quickRange, snapData]);

  // cross-tab backtest sync: resume polling on mount or when another tab starts one
  useEffect(() => {
    let active = true;
    const run = async () => {
      const raw = localStorage.getItem(BT_TASK_KEY);
      if (!raw || isOwnerRef.current) return;
      let taskId = '';
      let taskMode = '';
      try {
        const parsed = JSON.parse(raw);
        taskId = parsed.task_id;
        taskMode = parsed.mode || '';
      } catch { return; }
      if (!taskId) return;

      setLoading(true);
      setProgressMsg('');
      setError(null);
      setResult(null);

      try {
        let pollFailures = 0;
        const poll = async (): Promise<FactorBacktestResultResponse> => {
          if (!active) throw new Error('aborted');
          let status: Awaited<ReturnType<typeof discoveryApi.getFactorBacktestStatus>>;
          try {
            status = await discoveryApi.getFactorBacktestStatus(taskId);
            pollFailures = 0;
          } catch (e) {
            pollFailures++;
            if (pollFailures > 3) throw e;
            await new Promise((r) => setTimeout(r, 2000 * pollFailures));
            return poll();
          }
          if (!active) throw new Error('aborted');
          if (status.status_message) setProgressMsg(status.status_message);
          if (status.status === 'completed' && status.result) return status.result;
          if (status.status === 'failed') throw new Error(status.error || '回测失败');
          await new Promise((r) => setTimeout(r, 1000));
          return poll();
        };
        const data = await poll();
        if (!active) return;
        setResult(data);
        const sc: Record<string, boolean> = {};
        const curveKeys = Object.keys(data.capital_curves);
        for (const k of curveKeys) sc[k] = true;
        setSelectedCurves(sc);
        if (curveKeys.length > 0) setSummaryPeriod(curveKeys[0]);
        localStorage.setItem(BT_RESULT_KEY, JSON.stringify({ task_id: taskId, mode: taskMode }));
        localStorage.removeItem(BT_TASK_KEY);
      } catch (e) {
        if (active && (e as Error).message !== 'aborted') {
          setError(getParsedApiError(e));
          localStorage.removeItem(BT_TASK_KEY);
        }
      } finally {
        if (active) {
          setLoading(false);
          setProgressMsg('');
        }
      }
    };

    run();

    const onStorage = (e: StorageEvent) => {
      if (e.key === BT_TASK_KEY) {
        if (!localStorage.getItem(BT_TASK_KEY)) {
          // task cleared → reset loading if we were polling (not owner) and restore cached result
          if (!isOwnerRef.current && active) {
            setLoading(false);
            setProgressMsg('');
            const cachedRaw = localStorage.getItem(BT_RESULT_KEY);
            if (cachedRaw) {
              try {
                const cached = JSON.parse(cachedRaw);
                if (cached.task_id) {
                  discoveryApi.getFactorBacktestStatus(cached.task_id).then((status) => {
                    if (status.status === 'completed' && status.result) {
                      setResult(status.result);
                      const sc: Record<string, boolean> = {};
                      const curveKeys = Object.keys(status.result.capital_curves);
                      for (const k of curveKeys) sc[k] = true;
                      setSelectedCurves(sc);
                      if (curveKeys.length > 0) setSummaryPeriod(curveKeys[0]);
                    }
                  }).catch(() => {});
                }
              } catch { /* ignore parse errors */ }
            }
          }
        } else {
          run();
        }
      }
    };
    window.addEventListener('storage', onStorage);
    return () => {
      active = false;
      window.removeEventListener('storage', onStorage);
    };
  }, [holdDays]);

  // abort owner poll on unmount
  useEffect(() => {
    return () => { abortRef.current = true; };
  }, []);

  // compute date range intersection + bottleneck
  const bottleneckFactor = useMemo(() => {
    if (!snapData) return null;
    let minFrom = '';
    let maxTo = '';
    let earlyName = ''; let earlyDate = '';
    let lateName = ''; let lateDate = '';
    for (const f of snapData.factors) {
      if (!selectedFactors[f.name]) continue;
      if (!minFrom || f.available_from > minFrom) {
        minFrom = f.available_from; earlyName = f.label; earlyDate = f.available_from;
      }
      if (!maxTo || f.available_to < maxTo) {
        maxTo = f.available_to; lateName = f.label; lateDate = f.available_to;
      }
    }
    setDateRangeIntersection([minFrom, maxTo]);
    return minFrom ? { earlyName, earlyDate, lateName, lateDate } : null;
  }, [selectedFactors, snapData]);

  const dateOutOfRange = useMemo(() => {
    if (!dateRangeIntersection[0]) return false;
    return startDate < dateRangeIntersection[0] || endDate > dateRangeIntersection[1];
  }, [startDate, endDate, dateRangeIntersection]);

  const activeFactors = useMemo(() => {
    if (!snapData) return [];
    const active: string[] = [];
    for (const f of snapData.factors) {
      if (selectedFactors[f.name]) active.push(f.name);
    }
    return active;
  }, [snapData, selectedFactors]);

  // run backtest
  const handleRun = useCallback(async () => {
    isOwnerRef.current = true;
    abortRef.current = false;
    setLoading(true);
    setProgressMsg('');
    setError(null);
    setResult(null);
    localStorage.removeItem(BT_RESULT_KEY);
    try {
      const fw: Record<string, number> = {};
      for (const fn of activeFactors) {
        fw[fn] = factorWeights[fn] || 0;
      }
      // start async task
      const { task_id } = await discoveryApi.runFactorBacktest({
        mode,
        factor_weights: fw,
        start_date: startDate,
        end_date: endDate,
        top_n: topN,
        hold_days: holdDays,
        initial_capital: initialCapital,
        risk_free_rate: riskFreeRate / 100,
        use_pipeline: usePipeline,
        score_blend_alpha: blendAlpha,
        reoptimize_interval: reoptimize ? reoptimizeInterval : null,
        opt_window: reoptimize ? optWindow : undefined,
      });

      taskIdRef.current = task_id;
      localStorage.setItem(BT_TASK_KEY, JSON.stringify({ task_id, mode, started_at: Date.now() }));
      localStorage.setItem(BT_PARAMS_KEY, JSON.stringify({
        mode, holdDays, topN, startDate, endDate, initialCapital, riskFreeRate,
        usePipeline, blendAlpha, reoptimize, reoptimizeInterval, optWindow, selectedFactors,
      }));

      // poll until complete (with retry for transient network errors)
      let pollFailures = 0;
      const poll = async (): Promise<FactorBacktestResultResponse> => {
        if (abortRef.current) throw new Error('aborted');
        let status: Awaited<ReturnType<typeof discoveryApi.getFactorBacktestStatus>>;
        try {
          status = await discoveryApi.getFactorBacktestStatus(task_id);
          pollFailures = 0;
        } catch (e) {
          pollFailures++;
          if (pollFailures > 3) throw e;
          await new Promise((r) => setTimeout(r, 2000 * pollFailures));
          return poll();
        }
        if (abortRef.current) throw new Error('aborted');
        if (status.status_message) {
          setProgressMsg(status.status_message);
        }
        if (status.status === 'completed' && status.result) {
          return status.result;
        }
        if (status.status === 'failed') {
          throw new Error(status.error || '回测失败');
        }
        await new Promise((r) => setTimeout(r, 1000));
        return poll();
      };

      const data = await poll();
      setResult(data);
      // init curve visibility
      const sc: Record<string, boolean> = {};
      const curveKeys = Object.keys(data.capital_curves);
      for (const k of curveKeys) {
        sc[k] = true;
      }
      setSelectedCurves(sc);
      if (curveKeys.length > 0) setSummaryPeriod(curveKeys[0]);
      localStorage.setItem(BT_RESULT_KEY, JSON.stringify({ task_id: taskIdRef.current, mode }));
    } catch (e) {
      if ((e as Error).message !== 'aborted') {
        setError(getParsedApiError(e));
      }
    } finally {
      setLoading(false);
      setProgressMsg('');
      /* 仅任务真正完成/失败时清理标记；卸载中止时保留以便回页恢复轮询 */
      if (!abortRef.current) {
        localStorage.removeItem(BT_TASK_KEY);
        isOwnerRef.current = false;
      }
    }
  }, [mode, activeFactors, factorWeights, startDate, endDate, topN, holdDays, initialCapital, riskFreeRate, usePipeline, blendAlpha, reoptimize]);

  // 切换持有期 → 曲线联动
  useEffect(() => {
    if (!result) return;
    const isDyn = result.params?.reoptimize_interval != null;
    setSelectedCurves((prev) => {
      const n: Record<string, boolean> = {};
      for (const k of Object.keys(prev)) n[k] = false;
      if (isDyn) {
        n[`${summaryPeriod}_fixed`] = true;
        n[`${summaryPeriod}_dynamic`] = true;
      } else {
        n[summaryPeriod] = true;
      }
      return n;
    });
  }, [summaryPeriod, result]);

  const selectAll = () => {
    if (!snapData) return;
    const sf: Record<string, boolean> = {};
    for (const f of snapData.factors) sf[f.name] = true;
    setSelectedFactors(sf);
  };

  const clearAll = () => {
    if (!snapData) return;
    const sf: Record<string, boolean> = {};
    for (const f of snapData.factors) sf[f.name] = false;
    setSelectedFactors(sf);
  };

  const resetWeights = () => {
    if (!snapData) return;
    const fw: Record<string, number> = {};
    for (const f of snapData.factors) fw[f.name] = 0;
    setFactorWeights(fw);
  };

  // chart data
  const chartData = useMemo(() => {
    if (!result) return null;
    const allDates = new Set<string>();
    for (const kd of Object.keys(result.capital_curves)) {
      if (!selectedCurves[kd]) continue;
      for (const pt of result.capital_curves[kd]) {
        allDates.add(pt.date);
      }
    }
    // 始终包含上证指数日期
    if (result.benchmark_curve?.length) {
      for (const pt of result.benchmark_curve) {
        allDates.add(pt.date);
      }
    }
    const sorted = Array.from(allDates).sort();
    const data: Record<string, string | number | undefined>[] = [];
    for (const d of sorted) {
      const row: Record<string, string | number | undefined> = { date: `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}` };
      for (const kd of Object.keys(result.capital_curves)) {
        if (!selectedCurves[kd]) continue;
        const curve = result.capital_curves[kd];
        const pt = curve.find((p: FactorBacktestCapitalPoint) => p.date === d);
        row[`h${kd}`] = pt ? pt.capital : undefined;
      }
      // 上证指数基准线（始终显示）
      if (result.benchmark_curve?.length) {
        const bpt = result.benchmark_curve.find((p: FactorBacktestCapitalPoint) => p.date === d);
        row['h_benchmark'] = bpt ? bpt.capital : undefined;
      }
      data.push(row);
    }
    return data;
  }, [result, selectedCurves]);

  const quantileData = useMemo(() => {
    if (!result?.quantile_returns) return null;
    const q = result.quantile_returns[summaryPeriod] || result.quantile_returns[`${summaryPeriod}_fixed`];
    if (!q) return null;
    return [
      { name: 'Top 10%', value: q.top_10pct * 100 },
      { name: 'Top 20%', value: q.top_20pct * 100 },
      { name: '全体均值', value: q.top_50pct * 100 },
    ];
  }, [result, summaryPeriod]);

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
    _pipeline: '管线综合',
  };

  const isDynamicMode = result?.params?.reoptimize_interval != null;

  const periodStats = useMemo(() => {
    if (!result) return {};
    const stats: Record<string, { closed: FactorBacktestTrade[]; wr: number; cumRet: number; mdd: number; mddStart: string; mddEnd: string; annRet: number; finalCapital: number; sharpe: number; totalTrades: number }> = {};
    const hds = [...new Set(result.trade_records.map((t: FactorBacktestTrade) => t.hold_days))].sort();

    const computeStats = (curveKey: string, trades: FactorBacktestTrade[]) => {
      const curve = result.capital_curves[curveKey] || [];
      const closed = trades.filter((t: FactorBacktestTrade) => t.status === 'closed' || t.status === 'extended');
      const wins = closed.filter((t: FactorBacktestTrade) => t.return_pct > 0).length;
      const wr = closed.length > 0 ? wins / closed.length : 0;
      const ic = result.params?.initial_capital ?? 1_000_000;
      const finalCapital = curve.length > 0 ? curve[curve.length - 1].capital : ic;
      const cumRet = (finalCapital / ic) - 1;
      let annRet = 0;
      if (cumRet > -1 && curve.length > 1) {
        const periods = result.summary?.total_periods || (curve.length - 1);
        const logAr = Math.log1p(cumRet) * 252 / Math.max(periods, 1);
        annRet = logAr < 700 ? Math.exp(logAr) - 1 : Infinity;
      }
      let peak = ic, mdd = 0, mddStart = '', mddEnd = '';
      let peakDate = curve[0]?.date || '', ddStart = '';
      for (const pt of curve) {
        if (pt.capital > peak) { peak = pt.capital; peakDate = pt.date; }
        const dd = peak > 0 ? (peak - pt.capital) / peak : 0;
        if (dd > 0 && !ddStart) ddStart = pt.date;
        if (dd > mdd) { mdd = dd; mddStart = ddStart || peakDate; mddEnd = pt.date; }
        if (dd === 0) ddStart = '';
      }
      let sharpe = 0;
      if (curve.length > 1) {
        const drs: number[] = [];
        for (let i = 1; i < curve.length; i++) {
          drs.push((curve[i].capital - curve[i - 1].capital) / curve[i - 1].capital);
        }
        const avgDr = drs.reduce((a, b) => a + b, 0) / drs.length;
        const stdDr = Math.sqrt(drs.reduce((s, r) => s + (r - avgDr) ** 2, 0) / drs.length);
        sharpe = stdDr > 0 ? (avgDr / stdDr) * Math.sqrt(252) : 0;
      }
      return { closed, wr, cumRet, mdd, mddStart, mddEnd, annRet, finalCapital, sharpe, totalTrades: trades.length };
    };

    for (const hd of hds) {
      const trades = result.trade_records.filter((t: FactorBacktestTrade) => t.hold_days === hd);
      if (isDynamicMode) {
        const fixedTrades = trades.filter((t: any) => !t.reoptimized);
        const dynamicTrades = trades.filter((t: any) => t.reoptimized);
        stats[`${hd}_fixed`] = computeStats(`${hd}_fixed`, fixedTrades);
        stats[`${hd}_dynamic`] = computeStats(`${hd}_dynamic`, dynamicTrades);
      } else {
        stats[String(hd)] = computeStats(String(hd), trades);
      }
    }
    return stats;
  }, [result, isDynamicMode]);

  const currentStats = isDynamicMode
    ? periodStats[`${summaryPeriod}_fixed`]
    : periodStats[summaryPeriod];
  const currentDynamicStats = isDynamicMode
    ? periodStats[`${summaryPeriod}_dynamic`]
    : undefined;

  const [icSortOrder, setIcSortOrder] = useState<'descend' | 'ascend'>('descend');

  /* 长周期收缩：交易日 > 120 时默认只展示最近 120 日 */
  const [showAllHistory, setShowAllHistory] = useState(false);

  const maxTradingDays = useMemo(() => {
    if (!result) return 0;
    return Math.max(0, ...Object.values(result.capital_curves).map((c) => c.length));
  }, [result]);

  const recentCutoffDate = useMemo(() => {
    if (!result || maxTradingDays <= 120 || showAllHistory) return null;
    const curves = Object.values(result.capital_curves);
    const longest = curves.reduce((a, b) => (a.length >= b.length ? a : b), [] as FactorBacktestCapitalPoint[]);
    if (longest.length <= 120) return null;
    return longest[longest.length - 120].date;
  }, [result, maxTradingDays, showAllHistory]);

  const displayChartData = useMemo(() => {
    if (!chartData || !recentCutoffDate) return chartData;
    return chartData.filter((d) => (d.date as string).replace(/-/g, '') >= recentCutoffDate);
  }, [chartData, recentCutoffDate]);

  const displayTrades = useMemo(() => {
    if (!result) return [];
    if (!recentCutoffDate) return result.trade_records;
    return result.trade_records.filter((t: FactorBacktestTrade) => t.trade_date >= recentCutoffDate);
  }, [result, recentCutoffDate]);

  const icData = useMemo(() => {
    if (!result?.rank_ic) return null;
    const dayData = result.rank_ic[summaryPeriod] || result.rank_ic[`${summaryPeriod}_fixed`];
    if (!dayData) return null;
    const raw = Object.entries(dayData).map(([name, ic]) => ({
      name,
      label: FACTOR_LABELS[name] || name,
      ic: Number(ic.toFixed(4)),
    }));
    raw.sort((a, b) => icSortOrder === 'descend' ? b.ic - a.ic : a.ic - b.ic);
    return raw;
  }, [result, icSortOrder, summaryPeriod]);

  const tradeColumns = [
    { title: '信号日', dataIndex: 'trade_date', key: 'trade_date', width: 100 },
    { title: '持有期', dataIndex: 'hold_days', key: 'hold_days', width: 70 },
    { title: '股票', key: 'stock', width: 110, render: (_: unknown, r: FactorBacktestTrade) => (
      <div className="leading-tight">
        <div>{r.stock_name}</div>
        <div className="text-xs text-secondary-text">{r.stock_code}</div>
      </div>
    )},
    { title: '买入价', dataIndex: 'buy_price', key: 'buy_price', width: 80, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.buy_price },
    { title: '卖出日', dataIndex: 'sell_date', key: 'sell_date', width: 100, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.sell_date },
    { title: '卖出价', dataIndex: 'sell_price', key: 'sell_price', width: 80, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.sell_price },
    { title: '买入额', dataIndex: 'allocated', key: 'allocated', width: 90, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : (r.allocated > 0 ? r.allocated.toFixed(2) : '--') },
    { title: '卖出额', dataIndex: 'allocated', key: 'sell_amount', width: 90, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : (r.allocated > 0 ? (r.allocated + r.pnl).toFixed(2) : '--') },
    { title: '收益', dataIndex: 'return_pct', key: 'return_pct', width: 80, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : pct(r.return_pct) },
    { title: '盈亏', dataIndex: 'pnl', key: 'pnl', width: 100, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.pnl.toFixed(2) },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80, render: (_: unknown, r: FactorBacktestTrade) => {
      const m: Record<string, string> = { closed: '已平', extended: '延期', canceled: '取消', open: '持仓', pending: '待执行', locked: '锁仓' };
      return m[r.status] || r.status;
    }},
  ];

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="flex flex-col lg:flex-row gap-5">
        {/* ──── Left Panel ──── */}
        <div className="lg:w-[260px] shrink-0 space-y-4">
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">扫描模式</div>
              <Segmented
                block
                value={mode}
                onChange={(v) => setMode(v as TabKey)}
                disabled={snapLoading}
                options={[
                  { label: '盘后', value: 'postmarket' },
                  { label: '盘中', value: 'intraday' },
                ]}
              />
            </div>
          </Card>

          <Card>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="font-medium text-sm text-secondary-text">因子选择</span>
                <div className="flex gap-2">
                  <button type="button" className="text-xs text-cyan hover:underline" onClick={selectAll}>全选</button>
                  <button type="button" className="text-xs text-tertiary-text hover:underline" onClick={clearAll}>清空</button>
                  <button type="button" className="text-xs text-tertiary-text hover:underline" onClick={resetWeights}>默认权重</button>
                </div>
              </div>

              {snapData && (
                <div className="text-xs text-tertiary-text">
                  可用范围: {dateRangeIntersection[0] || '--'} ~ {dateRangeIntersection[1] || '--'}
                  {bottleneckFactor && (
                    <span className="ml-1 text-amber-500">
                      （最早由「{bottleneckFactor.earlyName}」限制: {bottleneckFactor.earlyDate}）
                    </span>
                  )}
                </div>
              )}

              <div className="max-h-[560px] overflow-y-auto space-y-1.5 pr-1">
                {[...(snapData?.factors ?? [])].sort((a, b) => (factorWeights[b.name] ?? 0) - (factorWeights[a.name] ?? 0)).map((f) => (
                  <div key={f.name} className="flex items-center gap-2 py-1">
                    <Checkbox
                      checked={!!selectedFactors[f.name]}
                      onChange={(e) => setSelectedFactors((p) => ({ ...p, [f.name]: e.target.checked }))}
                    />
                    <span className="text-sm flex-1 truncate text-foreground/85">{f.label} <span className="text-tertiary-text text-[11px]">({f.name})</span></span>
                    <InputNumber
                      size="small"
                      min={0}
                      max={100}
                      value={factorWeights[f.name] ?? 0}
                      onChange={(v) => setFactorWeights((p) => ({ ...p, [f.name]: v ?? 0 }))}
                      className="w-[60px]"
                      style={{ fontSize: 12 }}
                      placeholder="0"
                    />
                  </div>
                ))}
              </div>
              {!snapData && !error && (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-5 w-5 animate-spin text-tertiary-text" />
                </div>
              )}
            </div>
          </Card>
        </div>

        {/* ──── Right Panel ──── */}
        <div className="flex-1 min-w-0 space-y-4">
          {snapLoading && (
            <Card>
              <div className="flex flex-col items-center justify-center py-20 gap-3">
                <Loader2 className="h-6 w-6 animate-spin text-cyan" />
                <span className="text-sm text-secondary-text">加载中…</span>
              </div>
            </Card>
          )}
          {!snapLoading && (<>
          {/* Parameters */}
          <Card>
            <div className="space-y-5">
              <div className="font-medium text-sm text-secondary-text">回测参数</div>

              {/* 持有期 */}
              <div className="flex items-center gap-2">
                <span className="text-xs text-tertiary-text w-12 shrink-0">持有期</span>
                <div className="flex gap-1">
                  {HOLD_DAY_OPTIONS.map((o) => (
                    <button
                      key={o.value}
                      type="button"
                      onClick={() => {
                        if (holdDays.includes(o.value)) setHoldDays(holdDays.filter((d) => d !== o.value));
                        else setHoldDays([...holdDays, o.value].sort());
                      }}
                      className={`inline-flex items-center rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${
                        holdDays.includes(o.value)
                          ? 'bg-cyan text-white'
                          : 'bg-gray-100 text-gray-500 hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700'
                      }`}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* 数值参数 */}
              <div className="flex items-center gap-4 flex-wrap">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-tertiary-text whitespace-nowrap">Top-N</span>
                  <InputNumber size="small" min={1} max={50} value={topN} onChange={(v) => setTopN(v ?? 5)} className="w-20" />
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-tertiary-text whitespace-nowrap">初始资金</span>
                  <InputNumber size="small" min={10000} step={100000} value={initialCapital}
                    onChange={(v) => setInitialCapital(v ?? 1_000_000)}
                    className="w-32" formatter={(v) => `${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')} />
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-tertiary-text whitespace-nowrap">无风险利率</span>
                  <InputNumber size="small" min={0} max={10} step={0.5} value={riskFreeRate}
                    onChange={(v) => setRiskFreeRate(v ?? 2.0)} className="w-20" />
                  <span className="text-xs text-tertiary-text">%</span>
                </div>
              </div>

              {/* 日期 */}
              <div className="flex items-center gap-4 flex-wrap">
                <div className="flex items-center gap-2">
                  <span className={`text-xs whitespace-nowrap ${dateOutOfRange ? 'text-red-500' : 'text-tertiary-text'}`}>
                    起始日期{dateOutOfRange ? '（超范围）' : ''}
                  </span>
                  <DatePicker
                    size="small"
                    value={startDate ? dayjs(startDate, 'YYYYMMDD') : null}
                    onChange={(d) => { setStartDate(d ? d.format('YYYYMMDD') : ''); setQuickRange(null); }}
                    status={dateOutOfRange ? 'error' : undefined}
                    disabled={!!quickRange || activeFactors.length === 0 || !dateRangeIntersection[0]}
                    disabledDate={(d) => {
                      if (!dateRangeIntersection[0]) return true;
                      return d.isBefore(dayjs(dateRangeIntersection[0], 'YYYYMMDD')) || d.isAfter(dayjs(dateRangeIntersection[1], 'YYYYMMDD')) || d.isSame(dayjs(), 'day') || d.isAfter(dayjs(), 'day');
                    }}
                  />
                </div>
                <div className="flex items-center gap-2">
                  <span className={`text-xs whitespace-nowrap ${dateOutOfRange ? 'text-red-500' : 'text-tertiary-text'}`}>
                    结束日期{dateOutOfRange ? '（超范围）' : ''}
                  </span>
                  <DatePicker
                    size="small"
                    value={endDate ? dayjs(endDate, 'YYYYMMDD') : null}
                    onChange={(d) => { setEndDate(d ? d.format('YYYYMMDD') : ''); setQuickRange(null); }}
                    status={dateOutOfRange ? 'error' : undefined}
                    disabled={!!quickRange || activeFactors.length === 0 || !dateRangeIntersection[0]}
                    disabledDate={(d) => {
                      if (!dateRangeIntersection[0]) return true;
                      return d.isBefore(dayjs(dateRangeIntersection[0], 'YYYYMMDD')) || d.isAfter(dayjs(dateRangeIntersection[1], 'YYYYMMDD')) || d.isSame(dayjs(), 'day') || d.isAfter(dayjs(), 'day');
                    }}
                  />
                </div>
                <Segmented
                  size="small"
                  value={quickRange}
                  onChange={(v) => setQuickRange(v as number | null)}
                  options={[
                    { label: '近30日', value: 30 },
                    { label: '近60日', value: 60 },
                    { label: '近120日', value: 120 },
                  ]}
                />
                {quickRange && (
                  <button
                    type="button"
                    className="text-xs text-tertiary-text hover:text-foreground underline"
                    onClick={() => setQuickRange(null)}
                  >
                    取消
                  </button>
                )}
              </div>

              {/* 操作区 */}
              <div className="flex items-center gap-4 pt-1">
                <button
                  type="button"
                  className="inline-flex items-center gap-2 rounded-lg bg-cyan px-4 py-2 text-sm font-medium text-white transition-opacity hover:opacity-90 disabled:opacity-50"
                  onClick={handleRun}
                  disabled={loading || activeFactors.length === 0}
                >
                  {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                  开始回测
                </button>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={usePipeline}
                    onChange={(e) => setUsePipeline(e.target.checked)}
                    className="sr-only"
                  />
                  <span className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${usePipeline ? 'bg-cyan' : 'bg-gray-300'}`}>
                    <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${usePipeline ? 'translate-x-[18px]' : 'translate-x-[2px]'}`} />
                  </span>
                  <span className="text-xs text-foreground/70">使用管线加工得分（去相关/中性化/标准化/融合）</span>
                </label>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-foreground/70">动态调优 (Walk-Forward TPE)</span>
                  <Switch checked={reoptimize} onChange={setReoptimize} size="small" />
                  {reoptimize && (
                    <span className="text-[11px] text-amber-500">
                      每 <InputNumber size="small" min={1} max={60} value={reoptimizeInterval}
                        onChange={(v) => setReoptimizeInterval(v ?? 10)} className="w-14" style={{ fontSize: 11 }} />
                      {' '}日，回看{' '}
                      <InputNumber size="small" min={20} max={252} value={optWindow}
                        onChange={(v) => setOptWindow(v ?? 60)} className="w-16" style={{ fontSize: 11 }} />
                      {' '}日窗口 TPE 调优权重（内存 study，不污染生产数据）
                    </span>
                  )}
                </div>
              </div>
            </div>
          </Card>

          {error && <ApiErrorAlert error={error} />}

          {/* Results */}
          {result && (
            <>
              {/* 持有期切换 + 摘要 */}
              <div className="flex items-center gap-3 mb-3">
                <Segmented
                  size="small"
                  value={summaryPeriod}
                  onChange={(v) => setSummaryPeriod(v as string)}
                  options={(() => {
                    if (isDynamicMode) {
                      const seen = new Set<string>();
                      return Object.keys(periodStats)
                        .filter((k) => {
                          const hd = k.split('_')[0];
                          if (seen.has(hd)) return false;
                          seen.add(hd);
                          return true;
                        })
                        .map((k) => {
                          const hd = k.split('_')[0];
                          const fixed = periodStats[`${hd}_fixed`];
                          const dynamic = periodStats[`${hd}_dynamic`];
                          const totalClosed = (fixed?.closed.length || 0) + (dynamic?.closed.length || 0);
                          return { label: `${hd}日 (${totalClosed}笔)`, value: hd };
                        });
                    }
                    return Object.keys(periodStats).map((k) => {
                      const s = periodStats[k];
                      return { label: `${k}日 (${s.closed.length}笔)`, value: k };
                    });
                  })()}
                />
              </div>
              {isDynamicMode && currentDynamicStats ? (
                <>
                  <div className="text-xs font-medium text-foreground/60 mb-2">固定权重</div>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                    <StatCard label="累计收益" value={currentStats ? pctNum(currentStats.cumRet) : '--'} />
                    <StatCard label="最终资金" value={currentStats ? fmtMoney(currentStats.finalCapital) : '--'} />
                    <StatCard label="胜率" value={currentStats ? pctNum(currentStats.wr) : '--'} />
                    <StatCard label="最大回撤" value={currentStats ? pctNum(currentStats.mdd) : '--'}
                      hint={currentStats?.mddStart && currentStats?.mddEnd ? `${currentStats.mddStart} ~ ${currentStats.mddEnd}` : undefined} />
                    <StatCard label="年化收益" value={currentStats ? pctNum(currentStats.annRet) : '--'} />
                    <StatCard label="夏普比率" value={currentStats ? currentStats.sharpe.toFixed(2) : '--'} />
                    <StatCard label="交易笔数" value={currentStats ? String(currentStats.totalTrades) : '--'} />
                    <StatCard label="已平仓" value={currentStats ? String(currentStats.closed.length) : '--'} />
                  </div>
                  <div className="text-xs font-medium text-foreground/60 mb-2">动态调优</div>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                    <StatCard label="累计收益" value={pctNum(currentDynamicStats.cumRet)} />
                    <StatCard label="最终资金" value={fmtMoney(currentDynamicStats.finalCapital)} />
                    <StatCard label="胜率" value={pctNum(currentDynamicStats.wr)} />
                    <StatCard label="最大回撤" value={pctNum(currentDynamicStats.mdd)}
                      hint={currentDynamicStats?.mddStart && currentDynamicStats?.mddEnd ? `${currentDynamicStats.mddStart} ~ ${currentDynamicStats.mddEnd}` : undefined} />
                    <StatCard label="年化收益" value={pctNum(currentDynamicStats.annRet)} />
                    <StatCard label="夏普比率" value={currentDynamicStats.sharpe.toFixed(2)} />
                    <StatCard label="交易笔数" value={String(currentDynamicStats.totalTrades)} />
                    <StatCard label="已平仓" value={String(currentDynamicStats.closed.length)} />
                  </div>
                </>
              ) : (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <StatCard label="累计收益" value={currentStats ? pctNum(currentStats.cumRet) : '--'} />
                  <StatCard label="最终资金" value={currentStats ? fmtMoney(currentStats.finalCapital) : '--'} />
                  <StatCard label="胜率" value={currentStats ? pctNum(currentStats.wr) : '--'} />
                  <StatCard label="最大回撤" value={currentStats ? pctNum(currentStats.mdd) : '--'}
                    hint={currentStats?.mddStart && currentStats?.mddEnd ? `${currentStats.mddStart} ~ ${currentStats.mddEnd}` : undefined} />
                  <StatCard label="年化收益" value={currentStats ? pctNum(currentStats.annRet) : '--'} />
                  <StatCard label="夏普比率" value={currentStats ? currentStats.sharpe.toFixed(2) : '--'} />
                  <StatCard label="交易笔数" value={currentStats ? String(currentStats.totalTrades) : '--'} />
                  <StatCard label="已平仓" value={currentStats ? String(currentStats.closed.length) : '--'} />
                </div>
              )}

              {/* Walk-forward comparison */}
              {isDynamicMode && result.summary.dynamic && currentDynamicStats && (() => {
                const hdClean = summaryPeriod.replace('_fixed', '').replace('_dynamic', '');
                const rows = [
                  { metric: '累计收益', fixedVal: pctNum(currentStats!.cumRet), dynamicVal: pctNum(currentDynamicStats.cumRet), better: currentDynamicStats.cumRet > currentStats!.cumRet ? 'dynamic' : currentStats!.cumRet > currentDynamicStats.cumRet ? 'fixed' : 'tie' as const },
                  { metric: '年化收益', fixedVal: pctNum(currentStats!.annRet), dynamicVal: pctNum(currentDynamicStats.annRet), better: currentDynamicStats.annRet > currentStats!.annRet ? 'dynamic' : currentStats!.annRet > currentDynamicStats.annRet ? 'fixed' : 'tie' as const },
                  { metric: '胜率', fixedVal: pctNum(currentStats!.wr), dynamicVal: pctNum(currentDynamicStats.wr), better: currentDynamicStats.wr > currentStats!.wr ? 'dynamic' : currentStats!.wr > currentDynamicStats.wr ? 'fixed' : 'tie' as const },
                  { metric: '最大回撤', fixedVal: pctNum(currentStats!.mdd), dynamicVal: pctNum(currentDynamicStats.mdd), better: currentDynamicStats.mdd < currentStats!.mdd ? 'dynamic' : currentStats!.mdd < currentDynamicStats.mdd ? 'fixed' : 'tie' as const },
                  { metric: '夏普比率', fixedVal: currentStats!.sharpe.toFixed(2), dynamicVal: currentDynamicStats.sharpe.toFixed(2), better: currentDynamicStats.sharpe > currentStats!.sharpe ? 'dynamic' : currentStats!.sharpe > currentDynamicStats.sharpe ? 'fixed' : 'tie' as const },
                  { metric: '最终资金', fixedVal: fmtMoney(currentStats!.finalCapital), dynamicVal: fmtMoney(currentDynamicStats.finalCapital), better: currentDynamicStats.finalCapital > currentStats!.finalCapital ? 'dynamic' : currentStats!.finalCapital > currentDynamicStats.finalCapital ? 'fixed' : 'tie' as const },
                  { metric: '交易笔数', fixedVal: String(currentStats!.totalTrades), dynamicVal: String(currentDynamicStats.totalTrades), better: 'tie' as const },
                ];
                return (
                  <Card>
                    <div className="font-medium text-sm text-secondary-text mb-3">固定 vs 动态对比 · {hdClean}日</div>
                    <Table
                      size="small"
                      dataSource={rows}
                      rowKey="metric"
                      pagination={false}
                      columns={[
                        { title: '指标', dataIndex: 'metric', key: 'metric' },
                        { title: '固定权重', dataIndex: 'fixedVal', key: 'fixed', render: (_: unknown, r: typeof rows[0]) => <span className={r.better === 'fixed' ? 'text-green-500 font-medium' : ''}>{r.fixedVal}</span> },
                        { title: '动态调优', dataIndex: 'dynamicVal', key: 'dynamic', render: (_: unknown, r: typeof rows[0]) => <span className={r.better === 'dynamic' ? 'text-green-500 font-medium' : ''}>{r.dynamicVal}</span> },
                      ]}
                    />
                    {result.summary.dynamic.nodes_evaluated != null && (
                      <div className="mt-2 text-xs text-tertiary-text">Walk-forward 共评估 {result.summary.dynamic.nodes_evaluated} 个调优节点</div>
                    )}
                  </Card>
                );
              })()}

              {/* Recent-only toggle */}
              {maxTradingDays > 120 && (
                <div className="flex items-center gap-3 px-1">
                  <span className="text-xs text-tertiary-text">
                    共 {maxTradingDays} 个交易日
                  </span>
                  <button
                    type="button"
                    onClick={() => setShowAllHistory((v) => !v)}
                    className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                      showAllHistory
                        ? 'bg-gray-100 text-gray-500 hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-400'
                        : 'bg-cyan/10 text-cyan hover:bg-cyan/20'
                    }`}
                  >
                    {showAllHistory ? '展开全部' : '最近 120 日'}
                    <span className={`text-[10px] transition-transform ${showAllHistory ? 'rotate-180' : ''}`}>▼</span>
                  </button>
                </div>
              )}

              {/* Capital Curve */}
              {displayChartData && displayChartData.length > 0 && (
                <Card>
                  <div className="font-medium text-sm text-secondary-text mb-3">资金曲线</div>
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart data={displayChartData}>
                      <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                      <Tooltip contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }} />
                      <Legend onClick={(e) => {
                        if (!e.dataKey || typeof e.dataKey !== 'string') return;
                        if (e.dataKey === 'h_benchmark') return; // 基准线不可切换
                        const key = e.dataKey.replace('h', '');
                        setSelectedCurves((prev) => {
                          const next = { ...prev, [key]: !prev[key] };
                          const anyVisible = Object.values(next).some(v => v);
                          if (!anyVisible) {
                            for (const k of Object.keys(next)) next[k] = true;
                          }
                          return next;
                        });
                      }} />
                      {Object.keys(result.capital_curves).filter((k) => {
                        if (isDynamicMode) {
                          const prefix = String(summaryPeriod);
                          return k === `${prefix}_fixed` || k === `${prefix}_dynamic`;
                        }
                        return true;
                      }).map((k) => {
                        const isFixed = k.endsWith('_fixed');
                        const isDynSfx = k.endsWith('_dynamic');
                        const hdNum = parseInt(k.split('_')[0]) || parseInt(k) || 0;
                        const colorIdx = HOLD_DAY_OPTIONS.findIndex(o => o.value === hdNum);
                        const colorIdxSafe = colorIdx >= 0 ? colorIdx : Object.keys(result.capital_curves).indexOf(k) % CAPITAL_COLORS.length;
                        let label = `${hdNum}日`;
                        if (isFixed) label += ' (固定)';
                        else if (isDynSfx) label += ' (动态)';
                        return (
                          <Line
                            key={k}
                            type="monotone"
                            dataKey={`h${k}`}
                            name={label}
                            stroke={CAPITAL_COLORS[colorIdxSafe]}
                            strokeWidth={2}
                            strokeDasharray={isFixed ? '5 5' : undefined}
                            strokeOpacity={selectedCurves[k] ? 1 : 0.15}
                            dot={false}
                            connectNulls
                          />
                        );
                      })}
                      {result.benchmark_curve?.length > 0 && (
                        <Line
                          key="_benchmark"
                          type="monotone"
                          dataKey="h_benchmark"
                          name="上证指数"
                          stroke="#fbbf24"
                          strokeWidth={1.5}
                          strokeDasharray="3 3"
                          strokeOpacity={0.7}
                          dot={false}
                          connectNulls
                        />
                      )}
                    </LineChart>
                  </ResponsiveContainer>
                </Card>
              )}

              {/* Rank IC + Quantile Returns */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {icData && icData.length > 0 && (
                  <Card>
                    <div className="font-medium text-sm text-secondary-text mb-3">Rank IC · {summaryPeriod}日</div>
                    <Table
                      size="small"
                      dataSource={icData}
                      rowKey="name"
                      pagination={false}
                      onChange={(_pagination, _filters, sorter) => {
                        if (!Array.isArray(sorter) && sorter.order) {
                          setIcSortOrder(sorter.order as 'descend' | 'ascend');
                        }
                      }}
                      columns={[
                        { title: '因子', dataIndex: 'label', key: 'label' },
                        { title: 'IC', dataIndex: 'ic', key: 'ic', sorter: true, sortOrder: icSortOrder, render: (_: unknown, r: typeof icData[0]) => (
                          <span className={r.ic >= 0 ? 'text-green-500' : 'text-red-500'}>{r.ic.toFixed(4)}</span>
                        )},
                      ]}
                    />
                  </Card>
                )}

                {quantileData && (
                  <Card>
                    <div className="font-medium text-sm text-secondary-text mb-3">分位数收益 · {summaryPeriod}日</div>
                    <ResponsiveContainer width="100%" height={200}>
                      <BarChart data={quantileData}>
                        <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                        <XAxis dataKey="name" tick={{ fontSize: 11 }} />
                        <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => `${v.toFixed(1)}%`} />
                        <Tooltip formatter={(v) => `${Number(v).toFixed(2)}%`} contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }} />
                        <Bar dataKey="value" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </Card>
                )}
              </div>

              {/* Trade Records */}
              {displayTrades.length > 0 && (
                <Card>
                  <div className="flex items-center justify-between mb-3">
                    <span className="font-medium text-sm text-secondary-text">
                      交易明细（{currentStats ? currentStats.closed.length : displayTrades.length} 笔 · {summaryPeriod}日）
                    </span>
                    <div className="flex gap-2">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1 rounded-lg border px-2 py-1 text-xs text-secondary-text hover:text-foreground transition-colors"
                      onClick={() => {
                        const header = '信号日,持有期,股票,买入价,卖出日,卖出价,买入额,卖出额,收益率,盈亏,状态';
                        const rows = displayTrades.filter((t: FactorBacktestTrade) => t.hold_days === Number(summaryPeriod)).map((t: FactorBacktestTrade) =>
                          `${t.trade_date},${t.hold_days},"${t.stock_name}\n${t.stock_code}",${t.buy_price},${t.sell_date},${t.sell_price},${t.allocated || 0},${(t.allocated || 0) + (t.pnl || 0)},${t.return_pct},${t.pnl},${t.status}`
                        ).join('\n');
                        const bom = '\uFEFF';
                        const blob = new Blob([bom + header + '\n' + rows], { type: 'text/csv;charset=utf-8;' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url; a.download = `factor_backtest_${result.mode}_${summaryPeriod}d_${result.date_range.start}_${result.date_range.end}${usePipeline ? '_pipeline' : ''}.csv`;
                        a.click(); URL.revokeObjectURL(url);
                      }}
                    >
                      <Download className="h-3.5 w-3.5" />导出 CSV
                    </button>
                    <button
                      type="button"
                      className="inline-flex items-center gap-1 rounded-lg border px-2 py-1 text-xs text-secondary-text hover:text-foreground transition-colors"
                      onClick={() => {
                        const st = periodStats[summaryPeriod] || periodStats[`${summaryPeriod}_fixed`];
                        if (!st) return;
                        const curves = result.capital_curves;
                        const pts = curves[summaryPeriod] || curves[`${summaryPeriod}_fixed`];
                        if (!pts || pts.length < 2) return;
                        const allCap = pts.map((p: FactorBacktestCapitalPoint) => p.capital);
                        const minC = Math.min(...allCap), maxC = Math.max(...allCap);
                        const pad = (maxC - minC) * 0.1 || 10000;
                        const yMin = minC - pad, yMax = maxC + pad;
                        const W = 800, H = 360, R = 30;
                        const color = CAPITAL_COLORS[0];
                        const toX = (i: number, n: number) => R + (i / Math.max(n - 1, 1)) * (W - 2 * R);
                        const toY = (v: number) => H - R - ((v - yMin) / (yMax - yMin)) * (H - 2 * R);
                        const d = pts.map((p: FactorBacktestCapitalPoint, i: number) => `${i === 0 ? 'M' : 'L'}${toX(i, pts.length).toFixed(1)},${toY(p.capital).toFixed(1)}`).join(' ');
                        const path = `<path d="${d}" fill="none" stroke="${color}" stroke-width="2"/>\n`;
                        let yLabels = '';
                        for (let yi = 0; yi <= 4; yi++) {
                          const v = yMin + (yi / 4) * (yMax - yMin);
                          yLabels += `<text x="${R - 8}" y="${toY(v).toFixed(1)}" text-anchor="end" font-size="10" fill="#888">${(v / 10000).toFixed(1)}万</text>\n`;
                        }
                        const legend = `<rect x="${R}" y="${H - R + 8}" width="12" height="12" fill="${color}"/><text x="${R + 16}" y="${H - R + 18}" font-size="11" fill="#ccc">${summaryPeriod}日</text>`;
                        const trades = displayTrades.filter((t: FactorBacktestTrade) => t.hold_days === Number(summaryPeriod));
                        const tradeRows = trades.map((t: FactorBacktestTrade) => '<tr><td>'+t.trade_date+'</td><td>'+t.hold_days+'日</td><td>'+t.stock_name+'<br/><span style="color:#888;font-size:11px">'+t.stock_code+'</span></td><td>'+t.buy_price+'</td><td>'+t.sell_date+'</td><td>'+t.sell_price+'</td><td>'+(t.allocated||0).toFixed(0)+'</td><td>'+((t.allocated||0)+(t.pnl||0)).toFixed(0)+'</td><td>'+(t.return_pct*100).toFixed(2)+'%</td><td>'+t.pnl.toFixed(0)+'</td><td>'+t.status+'</td></tr>').join('');
                        const html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>因子回测 '+result.mode+' '+summaryPeriod+'日 '+result.date_range.start+'-'+result.date_range.end+'</title><style>body{font-family:-apple-system,sans-serif;background:#111;color:#ddd;padding:24px;max-width:900px;margin:auto}h1{font-size:18px;margin-bottom:16px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}.card{background:#1a1a1a;border-radius:8px;padding:14px}.card .label{font-size:11px;color:#888}.card .value{font-size:20px;font-weight:600;color:#fff}svg{display:block;margin:0 auto}table{width:100%;border-collapse:collapse;margin-top:24px;font-size:12px}th,td{padding:6px 8px;text-align:left;border-bottom:1px solid #333}th{color:#888;font-weight:500}td{color:#ccc}</style></head><body><h1>因子回测报告 '+result.mode+' · '+summaryPeriod+'日持有 · '+result.date_range.start+' ~ '+result.date_range.end+'</h1><div class="grid"><div class="card"><div class="label">累计收益</div><div class="value">'+(st.cumRet*100).toFixed(2)+'%</div></div><div class="card"><div class="label">年化收益</div><div class="value">'+(st.annRet !== null && isFinite(st.annRet) ? (st.annRet*100).toFixed(2)+'%' : 'N/A')+'</div></div><div class="card"><div class="label">胜率</div><div class="value">'+(st.wr*100).toFixed(1)+'%</div></div><div class="card"><div class="label">夏普</div><div class="value">'+st.sharpe.toFixed(2)+'</div></div><div class="card"><div class="label">最大回撤</div><div class="value" style="color:#ef4444">'+(st.mdd*100).toFixed(2)+'%</div></div><div class="card"><div class="label">已平仓</div><div class="value">'+st.closed.length+'</div></div><div class="card"><div class="label">最终资金</div><div class="value">'+(st.finalCapital/10000).toFixed(1)+'万</div></div><div class="card"><div class="label">初始</div><div class="value">'+((result.params?.initial_capital??0)/10000).toFixed(0)+'万</div></div></div><svg width="'+W+'" height="'+(H+20)+'"><line x1="'+R+'" y1="'+(H-R)+'" x2="'+(W-R)+'" y2="'+(H-R)+'" stroke="#444"/><line x1="'+R+'" y1="'+R+'" x2="'+R+'" y2="'+(H-R)+'" stroke="#444"/>'+yLabels+path+legend+'</svg><table><thead><tr><th>信号日</th><th>持有期</th><th>股票</th><th>买入价</th><th>卖出日</th><th>卖出价</th><th>买入额</th><th>卖出额</th><th>收益</th><th>盈亏</th><th>状态</th></tr></thead><tbody>'+tradeRows+'</tbody></table></body></html>';
                        const blob = new Blob([html], { type: 'text/html;charset=utf-8;' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url; a.download = 'factor_backtest_'+result.mode+'_'+summaryPeriod+'d_'+result.date_range.start+'_'+result.date_range.end+(usePipeline ? '_pipeline' : '')+'.html';
                        a.click(); URL.revokeObjectURL(url);
                      }}
                    >
                      <Download className="h-3.5 w-3.5" />导出 HTML
                    </button>
                    </div>
                  </div>
                  <Table
                    size="small"
                    dataSource={displayTrades.filter((t: FactorBacktestTrade) => t.hold_days === Number(summaryPeriod))}
                    rowKey={(r) => `${r.trade_date}_${r.hold_days}_${r.stock_code}`}
                    pagination={{ pageSize: 50, showSizeChanger: false }}
                    columns={tradeColumns}
                    scroll={{ x: 800 }}
                  />
                </Card>
              )}
            </>
          )}

          {loading && (
            <div className="flex flex-col items-center justify-center py-12 gap-3">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
              <span className="text-sm text-secondary-text">
                {progressMsg || '回测计算中…'}
              </span>
            </div>
          )}
          {!result && !error && !loading && (
            <EmptyState icon={<Activity className="h-8 w-8" />} title="选择因子和参数后开始回测" />
          )}
          </>)}
        </div>
      </div>
    </AppPage>
  );
};

export default FactorBacktestPage;
