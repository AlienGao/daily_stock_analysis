import type React from 'react';
import { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, Brush,
  BarChart, Bar, CartesianGrid,
} from 'recharts';
import { DatePicker, Table, InputNumber, Checkbox, Button, Tooltip as AntTooltip } from 'antd';
import dayjs from 'dayjs';
import { Play, Loader2, Activity, Download, Trash2 } from 'lucide-react';
import { AppPage, Card, StatCard, EmptyState, ApiErrorAlert } from '../components/common';
import { CapitalCurveTooltip, fmtMoney, fmtSignedPct, buildCapitalCurveChartMeta } from '../components/charts/CapitalCurveTooltip';
import apiClient from '../api';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
const HOLD_DAY_OPTIONS = [
  { label: '1日', value: 1 },
  { label: '3日', value: 3 },
  { label: '5日', value: 5 },
  { label: '10日', value: 10 },
  { label: '20日', value: 20 },
];

function pct(v: number): string {
  return `${(v * 100).toFixed(2)}%`;
}

const CAPITAL_COLORS = ['#22c55e', '#3b82f6', '#f59e0b', '#8b5cf6', '#ef4444'];

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
  alpha60: 'Alpha60收盘位置', money_flow_osc: '资金流振荡',
  market_cap: '小市值',
};

interface FactorInfo {
  name: string;
  weight: number;
}

interface BacktestTrade {
  trade_date: string;
  buy_date: string;
  hold_days: number;
  stock_code: string;
  stock_name: string;
  buy_price: number;
  buy_price_adj?: number;
  sell_date: string;
  sell_price: number;
  sell_price_adj?: number;
  return_pct: number;
  pnl: number;
  allocated: number;
  status: string;
  shares: number;
}

interface BacktestResult {
  mode: string;
  date_range: Record<string, string>;
  factors: Array<Record<string, unknown>>;
  params: Record<string, unknown>;
  summary: Record<string, Record<string, number>>;
  capital_curves: Record<string, Array<{ date: string; capital: number }>>;
  rank_ic: Record<string, Record<string, number>>;
  trade_records: BacktestTrade[];
  benchmark_curve: Array<{ date: string; capital: number }>;
}


interface CacheHistoryItem {
  id: number;
  factor_weights: Record<string, number>;
  start_date: string | null;
  end_date: string | null;
  top_n: number;
  hold_days: number[];
  initial_capital?: number;
  created_at: string;
  updated_at?: string;
}


function cacheItemTime(item: CacheHistoryItem): string {
  return item.updated_at || item.created_at || '';
}

function formatCacheHistoryTooltip(item: CacheHistoryItem): string {
  const factors = Object.entries(item.factor_weights)
    .map(([fn, w]) => `${FACTOR_LABELS[fn] || fn}(${w})`)
    .join(' + ') || '默认因子';
  const start = item.start_date || '最早';
  const end = item.end_date || '最新';
  const capital = item.initial_capital != null ? fmtMoney(item.initial_capital) : '--';
  const time = cacheItemTime(item);
  const cachedAt = time ? time.slice(0, 16).replace('T', ' ') : '--';
  return [
    `因子: ${factors}`,
    `选股: ${item.top_n}只`,
    `持有: ${(item.hold_days || []).join('/')}日`,
    `区间: ${start} ~ ${end}`,
    `资金: ${capital}`,
    `缓存: ${cachedAt}`,
  ].join('\n');
}

function cacheParamsMatch(
  item: CacheHistoryItem,
  factorWeights: Record<string, number>,
  startDate: string,
  endDate: string,
  topN: number,
  holdDays: number[],
  initialCapital: number,
): boolean {
  const keys = new Set([...Object.keys(item.factor_weights), ...Object.keys(factorWeights)]);
  for (const key of keys) {
    if ((item.factor_weights[key] ?? 0) !== (factorWeights[key] ?? 0)) return false;
  }
  const itemStart = item.start_date || null;
  const itemEnd = item.end_date || null;
  const reqStart = startDate || null;
  const reqEnd = endDate || null;
  if (itemStart !== reqStart || itemEnd !== reqEnd || item.top_n !== topN) return false;
  const a = [...(item.hold_days || [])].sort((x, y) => x - y).join(',');
  const b = [...holdDays].sort((x, y) => x - y).join(',');
  if (a !== b) return false;
  if (item.initial_capital != null && item.initial_capital !== initialCapital) return false;
  return true;
}


function formatTradePrice(raw: number): string {
  if (!raw || raw <= 0) return '--';
  return raw.toFixed(2);
}

function priceDiffersFromAdj(raw: number, adj?: number): boolean {
  return !!adj && adj > 0 && Math.abs(adj - raw) > 0.01;
}

function renderTradePrice(raw: number, adj?: number, label = '后复权') {
  const text = formatTradePrice(raw);
  if (text === '--') return text;
  if (!priceDiffersFromAdj(raw, adj)) return text;
  return (
    <AntTooltip title={`${label}: ${adj!.toFixed(2)}（含权收益，用于盈亏计算）`}>
      <span className="cursor-help border-b border-dotted border-secondary-text/40">{text}</span>
    </AntTooltip>
  );
}

const TASK_KEY = 'simple_factor_bt_task';

const SimpleFactorBacktestPage: React.FC = () => {
  const abortRef = useRef(false);
  const taskIdRef = useRef('');

  const [loading, setLoading] = useState(false);
  const [progressMsg, setProgressMsg] = useState('');
  const [error, setError] = useState<ParsedApiError | null>(null);

  // factors
  const [availableFactors, setAvailableFactors] = useState<FactorInfo[]>([]);
  const [selectedFactors, setSelectedFactors] = useState<Record<string, boolean>>({});
  const [factorWeights, setFactorWeights] = useState<Record<string, number>>({});

  // params
  const [holdDays, setHoldDays] = useState<number[]>([1, 3, 5, 10, 20]);
  const [topN, setTopN] = useState(1);
  const [startDate, setStartDate] = useState<string>('20250101');
  const [endDate, setEndDate] = useState<string>('');
  const [initialCapital, setInitialCapital] = useState(5_000_000);

  // result
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [summaryPeriod, setSummaryPeriod] = useState('5');
  const [hiddenLines, setHiddenLines] = useState<Set<string>>(new Set());

  // 切换持有期时，重置为只展示当前持有期曲线
  useEffect(() => {
    setHiddenLines(new Set(holdDays.filter(h => String(h) !== summaryPeriod).map(h => `hd${h}`)));
  }, [summaryPeriod, holdDays]);

  // presets
  const [presets, setPresets] = useState<Array<{ name: string; factor_weights: Record<string, number> }>>([]);

  // cache
  const [forceRerun, setForceRerun] = useState(false);
  const [cacheHistory, setCacheHistory] = useState<CacheHistoryItem[]>([]);
  const [activeCacheId, setActiveCacheId] = useState<number | null>(null);
  const rightPanelRef = useRef<HTMLDivElement>(null);
  const [leftMaxHeight, setLeftMaxHeight] = useState<number | null>(null);

  const loadPresets = useCallback(async () => {
    try {
      const resp = await apiClient.get('/api/v1/factor-backtest-simple/presets');
      setPresets(resp.data.presets || []);
    } catch { /* ignore */ }
  }, []);

  const loadCacheHistory = useCallback(async (highlight?: {
    factor_weights: Record<string, number>;
    start_date?: string;
    end_date?: string;
    top_n: number;
    hold_days: number[];
    initial_capital: number;
  }) => {
    try {
      const resp = await apiClient.get('/api/v1/factor-backtest-simple/cache');
      const items: CacheHistoryItem[] = (resp.data.items || []).sort((a: CacheHistoryItem, b: CacheHistoryItem) => {
        const ac = Object.keys(a.factor_weights).length;
        const bc = Object.keys(b.factor_weights).length;
        if (ac !== bc) return bc - ac; // 多因子排前面
        return cacheItemTime(b).localeCompare(cacheItemTime(a)); // 同类按最近更新时间倒序
      });
      setCacheHistory(items);
      if (highlight) {
        const match = items.find((item) => cacheParamsMatch(
          item,
          highlight.factor_weights,
          highlight.start_date || '',
          highlight.end_date || '',
          highlight.top_n,
          highlight.hold_days,
          highlight.initial_capital,
        ));
        setActiveCacheId(match?.id ?? null);
      }
    } catch { /* ignore */ }
  }, []);

  // load available factors
  useEffect(() => {
    apiClient.get('/api/v1/factor-backtest-simple/factors').then((resp: { data: { factors: FactorInfo[] } }) => {
      const factors: FactorInfo[] = resp.data.factors || [];
      setAvailableFactors(factors);
      const sel: Record<string, boolean> = {};
      const w: Record<string, number> = {};
      factors.forEach((f) => {
        sel[f.name] = false;
        w[f.name] = f.weight;
      });
      setSelectedFactors(sel);
      setFactorWeights(w);
    }).catch(() => {});
    loadPresets();

    // load latest cached result
    apiClient.get('/api/v1/factor-backtest-simple/cache', { params: { limit: 1 } })
      .then((resp) => {
        const items = resp.data?.items || [];
        if (items.length > 0) {
          handleLoadFromCache(items[0].id);
        }
      })
      .catch(() => {});

    // load cache history
    loadCacheHistory();
  }, [loadPresets, loadCacheHistory]);

  const selectedCount = useMemo(
    () => Object.values(selectedFactors).filter(Boolean).length,
    [selectedFactors],
  );

  const handleLoadFromCache = useCallback(async (cacheId: number) => {
    try {
      const resp = await apiClient.get(`/api/v1/factor-backtest-simple/cache/${cacheId}`);
      setResult(resp.data.result);
      setActiveCacheId(cacheId);
    } catch { /* ignore */ }
  }, []);

  const handleDeleteCache = useCallback(async (cacheId: number) => {
    if (!confirm('确定要删除该历史记录吗？')) {
      return;
    }
    try {
      await apiClient.delete(`/api/v1/factor-backtest-simple/cache/${cacheId}`);
      setCacheHistory((prev) => prev.filter((item) => item.id !== cacheId));
      setActiveCacheId((prev) => (prev === cacheId ? null : prev));
    } catch { /* ignore */ }
  }, []);

  const handleDeletePreset = useCallback(async (presetName: string) => {
    if (!confirm(`确定要删除预设 "${presetName}" 及其历史记录吗？`)) {
      return;
    }
    try {
      await apiClient.delete(`/api/v1/factor-backtest-simple/presets/${presetName}`);
      setPresets((prev) => prev.filter((p) => p.name !== presetName));
      // 刷新缓存历史记录，因为后端已删除对应的 DB 缓存
      loadCacheHistory();
    } catch (err) {
      const apiError = getParsedApiError(err as ParsedApiError);
      console.error('删除预设失败:', apiError?.message || err);
    }
  }, [loadCacheHistory]);

  const handleRun = useCallback(async () => {
    if (selectedCount === 0) return;
    setLoading(true);
    setError(null);
    setProgressMsg('提交中...');
    abortRef.current = false;

    const fw: Record<string, number> = {};
    for (const [name, sel] of Object.entries(selectedFactors)) {
      if (sel) fw[name] = factorWeights[name] || 1.0;
    }

    try {
      const resp = await apiClient.post(
        `/api/v1/factor-backtest-simple/run${forceRerun ? '?force=true' : ''}`,
        {
          factor_weights: fw,
          start_date: startDate || undefined,
          end_date: endDate || undefined,
          top_n: topN,
          hold_days: holdDays,
          initial_capital: initialCapital,
          risk_free_rate: 0.02,
        },
      );

      // 缓存命中：直接返回结果，无需轮询
      if (resp.data.cache_hit && resp.data.status === 'completed') {
        setResult(resp.data.result);
        setLoading(false);
        setProgressMsg('已从缓存加载');
        loadCacheHistory({ factor_weights: fw, start_date: startDate, end_date: endDate, top_n: topN, hold_days: holdDays, initial_capital: initialCapital });
        if (Object.keys(fw).length >= 2) {
          loadPresets();
        }
        return;
      }

      const taskId = resp.data.task_id;
      taskIdRef.current = taskId;
      localStorage.setItem(TASK_KEY, taskId);
      setProgressMsg('回测运行中...');

      // poll
      const poll = async () => {
        while (!abortRef.current) {
          await new Promise((r) => setTimeout(r, 2000));
          if (abortRef.current) break;
          try {
            const statusResp = await apiClient.get('/api/v1/factor-backtest-simple/status', {
              params: { task_id: taskId },
            });
            const data = statusResp.data;
            if (data.status_message) setProgressMsg(data.status_message);
            if (data.status === 'completed') {
              setResult(data.result);
              localStorage.removeItem(TASK_KEY);
              setLoading(false);
              loadCacheHistory({ factor_weights: fw, start_date: startDate, end_date: endDate, top_n: topN, hold_days: holdDays, initial_capital: initialCapital });
              if (Object.keys(fw).length >= 2) {
                loadPresets();
              }
              return;
            }
            if (data.status === 'failed') {
              setError({ message: data.error || '回测失败' } as ParsedApiError);
              localStorage.removeItem(TASK_KEY);
              setLoading(false);
              return;
            }
          } catch {
            // retry
          }
        }
      };
      poll();
    } catch (err) {
      setError(getParsedApiError(err));
      setLoading(false);
    }
  }, [selectedFactors, factorWeights, startDate, endDate, topN, holdDays, initialCapital, selectedCount, forceRerun, loadCacheHistory, loadPresets]);

  const handleStop = useCallback(() => {
    abortRef.current = true;
    setLoading(false);
    localStorage.removeItem(TASK_KEY);
  }, []);

  const [batchLoading, setBatchLoading] = useState(false);
  const [batchMsg, setBatchMsg] = useState('');

  const handleBatchTest = useCallback(async () => {
    setBatchLoading(true);
    setError(null);
    setBatchMsg('提交中...');
    try {
      const resp = await apiClient.post(
        `/api/v1/factor-backtest-simple/batch-test${forceRerun ? '?force=true' : ''}`,
        {
          factor_weights: {},
          start_date: startDate || undefined,
          end_date: endDate || undefined,
          top_n: topN,
          hold_days: holdDays,
          initial_capital: initialCapital,
          risk_free_rate: 0.02,
        },
      );
      const taskId = resp.data.task_id;
      const poll = async () => {
        await new Promise((r) => setTimeout(r, 2000));
        try {
          const s = await apiClient.get('/api/v1/factor-backtest-simple/status', { params: { task_id: taskId } });
          if (s.data.status_message) setBatchMsg(s.data.status_message);
          if (s.data.status === 'completed') {
            const cached = s.data.result?.cached_count || 0;
            const total = s.data.result?.factors_tested || 0;
            setBatchLoading(false);
            setBatchMsg(`批量测试完成 (${cached}/${total} 命中缓存)，报告已保存`);
            loadCacheHistory();
          } else if (s.data.status === 'failed') {
            setBatchLoading(false);
            setError({ message: s.data.error || '批量测试失败' } as ParsedApiError);
          } else {
            poll();
          }
        } catch { poll(); }
      };
      poll();
    } catch (err) {
      setBatchLoading(false);
      setError(getParsedApiError(err));
    }
  }, [startDate, endDate, topN, holdDays, initialCapital, forceRerun, loadCacheHistory]);

  const [cvLoading, setCvLoading] = useState(false);
  const [cvResult, setCvResult] = useState<{
    latest_date: string; total_presets: number;
    cross_stocks: Array<{ ts_code: string; stock_name: string; count: number; presets: string[] }>;
  } | null>(null);

  const handleCrossValidate = useCallback(async () => {
    setCvLoading(true);
    setError(null);
    try {
      const resp = await apiClient.post('/api/v1/factor-backtest-simple/cross-validate');
      setCvResult(resp.data);
    } catch (err) {
      setError(getParsedApiError(err));
    } finally {
      setCvLoading(false);
    }
  }, []);

  // chart data
  const chartData = useMemo(() => {
    if (!result?.capital_curves) return [];
    const curves = result.capital_curves;
    const allDates = new Set<string>();
    Object.values(curves).forEach((c) => c.forEach((p) => allDates.add(p.date)));
    const sortedDates = [...allDates].sort();
    return sortedDates.map((date) => {
      const point: Record<string, unknown> = { date };
      for (const [hd, curve] of Object.entries(curves)) {
        const found = curve.find((p) => p.date === date);
        if (found) point[`hd${hd}`] = found.capital;
      }
      if (result.benchmark_curve) {
        const bm = result.benchmark_curve.find((p) => p.date === date);
        if (bm) point.benchmark = bm.capital;
      }
      return point;
    });
  }, [result]);

  const chartLatestMeta = useMemo(
    () => buildCapitalCurveChartMeta(chartData),
    [chartData],
  );

  // summary stats: compute per hold period from capital curve + trades
  const currentStats = useMemo(() => {
    if (!result) return null;
    const curve = result.capital_curves?.[summaryPeriod];
    if (!curve || curve.length < 2) return null;
    const trades = result.trade_records?.filter((t) => t.hold_days === Number(summaryPeriod)) || [];
    const closed = trades.filter((t) => t.status === 'closed' || t.status === 'extended');
    const openCount = trades.filter((t) => t.status === 'open').length;
    const canceledCount = trades.filter((t) => t.status === 'canceled').length;
    const initCap = (result.params as Record<string, unknown>)?.initial_capital as number || 5_000_000;
    const rfr = (result.params as Record<string, unknown>)?.risk_free_rate as number || 0.02;
    const finalCap = curve[curve.length - 1].capital;
    const totalReturn = (finalCap - initCap) / initCap;
    const periods = curve.length - 1;
    const annualReturn = totalReturn > -1 ? Math.pow(1 + totalReturn, 252 / Math.max(periods, 1)) - 1 : totalReturn;
    const wins = closed.filter((t) => t.return_pct > 0).length;
    const winRate = closed.length > 0 ? wins / closed.length : 0;
    let peak = initCap, mdd = 0;
    for (const pt of curve) {
      if (pt.capital > peak) peak = pt.capital;
      const dd = (peak - pt.capital) / peak;
      if (dd > mdd) mdd = dd;
    }
    const dr: number[] = [];
    for (let i = 1; i < curve.length; i++) dr.push((curve[i].capital - curve[i - 1].capital) / curve[i - 1].capital);
    const meanR = dr.length > 0 ? dr.reduce((a, b) => a + b, 0) / dr.length : 0;
    const stdR = dr.length > 1 ? Math.sqrt(dr.reduce((s, v) => s + (v - meanR) ** 2, 0) / (dr.length - 1)) : 0;
    const dailyRf = Math.pow(1 + rfr, 1 / 252) - 1;
    const sharpe = stdR > 0 ? (meanR - dailyRf) / stdR * Math.sqrt(252) : 0;
    return {
      total_return: totalReturn, annual_return: annualReturn, sharpe,
      max_drawdown: mdd, win_rate: winRate, trade_count: closed.length,
      openCount, canceledCount,
    };
  }, [result, summaryPeriod]);

  // trades for selected period
  const displayTrades = useMemo(() => {
    if (!result?.trade_records) return [];
    const hd = Number(summaryPeriod);
    return result.trade_records.filter((t) => t.hold_days === hd);
  }, [result, summaryPeriod]);

  // 按个股汇总已平仓盈亏，取贡献 Top5
  const topContributionStocks = useMemo(() => {
    const closed = displayTrades.filter((t) => t.status === 'closed' || t.status === 'extended');
    if (closed.length === 0) return [];

    const byCode = new Map<string, {
      stock_code: string;
      stock_name: string;
      trade_count: number;
      win_count: number;
      total_pnl: number;
      total_return_pct: number;
    }>();

    for (const t of closed) {
      const prev = byCode.get(t.stock_code) ?? {
        stock_code: t.stock_code,
        stock_name: t.stock_name,
        trade_count: 0,
        win_count: 0,
        total_pnl: 0,
        total_return_pct: 0,
      };
      prev.trade_count += 1;
      if (t.pnl > 0) prev.win_count += 1;
      prev.total_pnl += t.pnl;
      prev.total_return_pct += t.return_pct;
      prev.stock_name = t.stock_name || prev.stock_name;
      byCode.set(t.stock_code, prev);
    }

    const totalPnl = [...byCode.values()].reduce((s, r) => s + r.total_pnl, 0);

    return [...byCode.values()]
      .map((r) => ({
        ...r,
        avg_return_pct: r.trade_count > 0 ? r.total_return_pct / r.trade_count : 0,
        win_rate: r.trade_count > 0 ? r.win_count / r.trade_count : 0,
        contribution_pct: totalPnl !== 0 ? r.total_pnl / totalPnl : 0,
      }))
      .sort((a, b) => b.total_pnl - a.total_pnl)
      .slice(0, 5)
      .map((r, i) => ({ ...r, rank: i + 1 }));
  }, [displayTrades]);

  // 大屏：左侧栏高度跟随右侧内容，历史记录在栏内滚动
  useEffect(() => {
    const el = rightPanelRef.current;
    if (!el) return;

    const syncHeight = () => {
      if (window.innerWidth < 1024) {
        setLeftMaxHeight(null);
        return;
      }
      setLeftMaxHeight(el.offsetHeight);
    };

    syncHeight();
    const ro = new ResizeObserver(syncHeight);
    ro.observe(el);
    window.addEventListener('resize', syncHeight);
    return () => {
      ro.disconnect();
      window.removeEventListener('resize', syncHeight);
    };
  }, [result, summaryPeriod, cacheHistory.length, loading, displayTrades.length, topContributionStocks.length]);

  // IC data
  const icData = useMemo(() => {
    if (!result?.rank_ic) return null;
    const dayData = result.rank_ic[summaryPeriod];
    if (!dayData) return null;
    return Object.entries(dayData).map(([name, ic]) => ({
      name,
      label: FACTOR_LABELS[name] || name,
      ic: Number((ic as number).toFixed(4)),
    })).sort((a, b) => b.ic - a.ic);
  }, [result, summaryPeriod]);

  const tradeColumns = [
    { title: '发现日', dataIndex: 'trade_date', key: 'trade_date', width: 90 },
    { title: '股票', key: 'stock', width: 100, render: (_: unknown, r: BacktestTrade) => (
      <div className="leading-tight">
        <div>{r.stock_name}</div>
        <div className="text-xs text-secondary-text">{r.stock_code}</div>
      </div>
    )},
    { title: '买入日', dataIndex: 'buy_date', key: 'buy_date', width: 90 },
    {
      title: (
        <AntTooltip title="表格为不复权价；盈亏、收益率、买卖金额按后复权价计算（含分红送转）">
          <span className="cursor-help border-b border-dotted border-secondary-text/40">买入价</span>
        </AntTooltip>
      ),
      dataIndex: 'buy_price',
      key: 'buy_price',
      width: 75,
      render: (_: unknown, r: BacktestTrade) => renderTradePrice(r.buy_price, r.buy_price_adj),
    },
    { title: '股数', dataIndex: 'shares', key: 'shares', width: 70, render: (_: unknown, r: BacktestTrade) => r.shares > 0 ? r.shares.toLocaleString() : '--' },
    {
      title: (
        <AntTooltip title="后复权买入价 × 股数（除权日前后与不复权价可能不一致）">
          <span className="cursor-help border-b border-dotted border-secondary-text/40">买入额</span>
        </AntTooltip>
      ),
      key: 'buy_amount',
      width: 90,
      render: (_: unknown, r: BacktestTrade) => {
        if (!r.shares) return '--';
        return Math.round(r.allocated).toLocaleString();
      },
    },
    { title: '卖出日', dataIndex: 'sell_date', key: 'sell_date', width: 90 },
    {
      title: (
        <AntTooltip title="表格为不复权价；盈亏、收益率按后复权价计算">
          <span className="cursor-help border-b border-dotted border-secondary-text/40">卖出价</span>
        </AntTooltip>
      ),
      dataIndex: 'sell_price',
      key: 'sell_price',
      width: 75,
      render: (_: unknown, r: BacktestTrade) => renderTradePrice(r.sell_price, r.sell_price_adj),
    },
    {
      title: (
        <AntTooltip title="后复权卖出价 × 股数">
          <span className="cursor-help border-b border-dotted border-secondary-text/40">卖出额</span>
        </AntTooltip>
      ),
      key: 'sell_amount',
      width: 90,
      render: (_: unknown, r: BacktestTrade) => {
        if (!r.shares) return '--';
        return Math.round(r.allocated + r.pnl).toLocaleString();
      },
    },
    {
      title: (
        <AntTooltip title="按后复权买卖价计算（除权日连续），非不复权价直接相除">
          <span className="cursor-help border-b border-dotted border-secondary-text/40">收益</span>
        </AntTooltip>
      ),
      dataIndex: 'return_pct',
      key: 'return_pct',
      width: 75,
      render: (_: unknown, r: BacktestTrade) => (
        <span className={r.return_pct >= 0 ? 'text-red-400' : 'text-emerald-400'}>
          {r.return_pct >= 0 ? '+' : ''}{pct(r.return_pct)}
        </span>
      ),
    },
    { title: '盈亏', dataIndex: 'pnl', key: 'pnl', width: 90, render: (_: unknown, r: BacktestTrade) => (
      <span className={r.pnl >= 0 ? 'text-red-400' : 'text-emerald-400'}>
        {r.pnl >= 0 ? '+' : ''}{r.pnl.toFixed(0)}
      </span>
    )},
    { title: '状态', dataIndex: 'status', key: 'status', width: 75, render: (_: unknown, r: BacktestTrade) => {
      const m: Record<string, string> = { closed: '已平', extended: '延期', canceled: '取消', open: '持仓', pending: '待执行', locked: '锁仓' };
      return m[r.status] || r.status;
    }},
  ];

  const contributionColumns = [
    { title: '#', dataIndex: 'rank', key: 'rank', width: 40 },
    { title: '股票', key: 'stock', width: 120, render: (_: unknown, r: typeof topContributionStocks[0]) => (
      <div className="leading-tight">
        <div>{r.stock_name}</div>
        <div className="text-xs text-secondary-text">{r.stock_code}</div>
      </div>
    )},
    { title: '交易次数', dataIndex: 'trade_count', key: 'trade_count', width: 80 },
    { title: '累计盈亏', dataIndex: 'total_pnl', key: 'total_pnl', width: 100, render: (_: unknown, r: typeof topContributionStocks[0]) => (
      <span className={r.total_pnl >= 0 ? 'text-red-400' : 'text-emerald-400'}>
        {r.total_pnl >= 0 ? '+' : ''}{r.total_pnl.toFixed(0)}
      </span>
    )},
    {
      title: (
        <AntTooltip title="该股累计盈亏占全部已平仓盈亏之和的比例">
          <span className="cursor-help border-b border-dotted border-secondary-text/40">贡献占比</span>
        </AntTooltip>
      ),
      dataIndex: 'contribution_pct',
      key: 'contribution_pct',
      width: 90,
      render: (_: unknown, r: typeof topContributionStocks[0]) => pct(r.contribution_pct),
    },
    { title: '胜率', dataIndex: 'win_rate', key: 'win_rate', width: 70, render: (_: unknown, r: typeof topContributionStocks[0]) => pct(r.win_rate) },
    { title: '均收益', dataIndex: 'avg_return_pct', key: 'avg_return_pct', width: 80, render: (_: unknown, r: typeof topContributionStocks[0]) => (
      <span className={r.avg_return_pct >= 0 ? 'text-red-400' : 'text-emerald-400'}>
        {fmtSignedPct(r.avg_return_pct)}
      </span>
    )},
  ];

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="flex flex-col lg:flex-row lg:items-start gap-5">
        {/* Left Panel */}
        <div
          className="lg:w-[260px] shrink-0 flex flex-col lg:min-h-0 lg:overflow-hidden"
          style={leftMaxHeight != null ? { maxHeight: leftMaxHeight } : undefined}
        >
          <div className="flex shrink-0 flex-col gap-4">
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">选择因子</div>
              <div className="text-xs text-secondary-text">
                已选 {selectedCount} 个因子
              </div>
              <div className="space-y-2 max-h-[50vh] overflow-y-auto">
                {availableFactors.map((f) => (
                  <div key={f.name} className="flex items-center gap-2">
                    <Checkbox
                      checked={!!selectedFactors[f.name]}
                      onChange={(e) => setSelectedFactors((p) => ({ ...p, [f.name]: e.target.checked }))}
                    />
                    <span className="text-sm flex-1 truncate">{FACTOR_LABELS[f.name] || f.name} <span className="text-tertiary-text text-xs">({f.name})</span></span>
                    {selectedFactors[f.name] && (
                      <InputNumber
                        size="small"
                        min={0.1}
                        max={100}
                        step={0.5}
                        value={factorWeights[f.name]}
                        onChange={(v) => v !== null && setFactorWeights((p) => ({ ...p, [f.name]: v }))}
                        className="w-16"
                      />
                    )}
                  </div>
                ))}
              </div>

              {presets.length > 0 && (
                <div className="pt-3 border-t border-divider">
                  <div className="text-xs text-secondary-text mb-2">快捷组合</div>
                  <div className="space-y-1.5">
                    {presets.map((p) => {
                      const entries = Object.entries(p.factor_weights);
                      const label = entries
                        .map(([fn, w]) => `${FACTOR_LABELS[fn] || fn}(${w})`)
                        .join(' + ');
                      return (
                        <div
                          key={p.name}
                          className="group flex items-center gap-1"
                        >
                          <button
                            type="button"
                            className="flex-1 cursor-pointer text-left px-2 py-1.5 rounded text-xs border border-divider hover:border-blue-500/50 hover:bg-blue-500/10 transition-colors leading-relaxed"
                            onClick={() => {
                              const sel: Record<string, boolean> = {};
                              const w: Record<string, number> = {};
                              availableFactors.forEach((f) => { sel[f.name] = false; w[f.name] = f.weight; });
                              for (const [fn, fw] of entries) {
                                sel[fn] = true;
                                w[fn] = fw;
                              }
                              setSelectedFactors(sel);
                              setFactorWeights(w);
                            }}
                          >
                            {label}
                          </button>
                          <button
                            type="button"
                            className="cursor-pointer px-1.5 py-1.5 rounded text-xs opacity-0 group-hover:opacity-100 hover:bg-red-500/20 hover:text-red-400 transition-all"
                            title="删除该预设"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleDeletePreset(p.name);
                            }}
                          >
                            <Trash2 size={14} />
                          </button>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>
          </Card>

          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">回测参数</div>

              <div>
                <div className="text-xs text-secondary-text mb-1">持有天数</div>
                <div className="flex flex-wrap gap-1">
                  {HOLD_DAY_OPTIONS.map((opt) => (
                    <button
                      key={opt.value}
                      type="button"
                      className={`cursor-pointer px-2 py-1 text-xs rounded border ${
                        holdDays.includes(opt.value)
                          ? 'bg-blue-500/20 border-blue-500 text-blue-400'
                          : 'border-divider text-secondary-text hover:border-secondary-text'
                      }`}
                      onClick={() => {
                        setHoldDays((prev) =>
                          prev.includes(opt.value)
                            ? prev.filter((d) => d !== opt.value)
                            : [...prev, opt.value].sort((a, b) => a - b)
                        );
                      }}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="flex gap-2">
                <div className="w-24">
                  <div className="text-xs text-secondary-text mb-1">选股数</div>
                  <InputNumber
                    min={1}
                    max={50}
                    value={topN}
                    onChange={(v) => v !== null && setTopN(v)}
                    className="w-full"
                  />
                </div>
                <div className="flex-1">
                  <div className="text-xs text-secondary-text mb-1">初始资金</div>
                  <InputNumber
                    min={10000}
                    step={100000}
                    value={initialCapital}
                    onChange={(v) => v !== null && setInitialCapital(v)}
                    className="w-full"
                    formatter={(v) => `${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
                    parser={(v) => Number((v || '').replace(/,/g, '')) as unknown as 0}
                  />
                </div>
              </div>

              <div className="flex gap-2">
                <div className="flex-1">
                  <div className="text-xs text-secondary-text mb-1">开始日期</div>
                  <DatePicker
                    className="w-full"
                    value={startDate ? dayjs(startDate) : null}
                    onChange={(d) => setStartDate(d ? d.format('YYYYMMDD') : '')}
                    placeholder="最早"
                  />
                </div>
                <div className="flex-1">
                  <div className="text-xs text-secondary-text mb-1">结束日期</div>
                  <DatePicker
                    className="w-full"
                    value={endDate ? dayjs(endDate) : null}
                    onChange={(d) => setEndDate(d ? d.format('YYYYMMDD') : '')}
                    disabledDate={(d) => d.isAfter(dayjs(), 'day')}
                    placeholder="最新"
                  />
                </div>
              </div>

            </div>
          </Card>

          {error && <ApiErrorAlert error={error} />}

          <div className="flex gap-2">
            {!loading ? (
              <Button
                type="primary"
                block
                icon={<Play className="w-4 h-4" />}
                onClick={handleRun}
                disabled={selectedCount === 0}
              >
                开始回测
              </Button>
            ) : (
              <Button
                block
                danger
                icon={<Loader2 className="w-4 h-4 animate-spin" />}
                onClick={handleStop}
              >
                停止
              </Button>
            )}
          </div>
          <label className="flex items-center gap-1.5 text-xs text-secondary-text cursor-pointer">
            <input
              type="checkbox"
              checked={forceRerun}
              onChange={(e) => setForceRerun(e.target.checked)}
              className="accent-blue-500"
            />
            强制重跑（忽略缓存）
          </label>

          {loading && progressMsg && (
            <div className="text-xs text-blue-400 text-center">{progressMsg}</div>
          )}

          <div className="pt-2 border-t border-divider space-y-2">
            <Button
              block
              size="small"
              onClick={handleBatchTest}
              loading={batchLoading}
              disabled={loading}
            >
              逐一测试所有因子
            </Button>
            <Button
              block
              size="small"
              onClick={handleCrossValidate}
              loading={cvLoading}
              disabled={loading || presets.length === 0}
            >
              交叉验证
            </Button>
            {batchMsg && (
              <div className="text-xs text-blue-400 text-center mt-1">{batchMsg}</div>
            )}
            {cvResult && (
              <div className="text-xs text-secondary-text mt-2 space-y-1">
                <div>数据日期: {cvResult.latest_date}，共 {cvResult.total_presets} 个组合</div>
                {cvResult.cross_stocks.filter(s => s.count >= 2).map(s => (
                  <div key={s.ts_code} className="flex items-center gap-1">
                    <span className="text-amber-400 font-medium">{s.count}✓</span>
                    <span>{s.stock_name}</span>
                    <span className="text-tertiary-text">{s.ts_code}</span>
                  </div>
                ))}
                {cvResult.cross_stocks.filter(s => s.count >= 2).length === 0 && (
                  <div className="text-tertiary-text">无交叉命中个股</div>
                )}
              </div>
            )}
          </div>
          </div>

          {/* Cache History */}
          {cacheHistory.length > 0 && (
            <div className="flex min-h-0 flex-1 flex-col border-t border-divider pt-2">
              <div className="mb-2 shrink-0 text-xs text-secondary-text">历史记录</div>
              <div className="min-h-0 flex-1 space-y-1 overflow-y-auto">
                {cacheHistory.map((item) => {
                  const factorNames = Object.entries(item.factor_weights)
                    .map(([fn, w]) => `${FACTOR_LABELS[fn] || fn}(${w})`)
                    .join('+') || '默认因子';
                  const dateStr = cacheItemTime(item) ? cacheItemTime(item).slice(5, 16).replace('T', ' ') : '';
                  return (
                    <div key={item.id} className="flex items-center gap-1 group">
                      <AntTooltip
                        title={(
                          <pre className="m-0 whitespace-pre-wrap text-[11px] leading-relaxed">
                            {formatCacheHistoryTooltip(item)}
                          </pre>
                        )}
                        placement="right"
                        mouseEnterDelay={0.3}
                      >
                        <button
                          type="button"
                          className={`flex-1 cursor-pointer text-left px-2 py-1.5 rounded text-xs border transition-colors truncate ${
                            item.id === activeCacheId
                              ? 'border-blue-500 bg-blue-500/15 ring-1 ring-blue-500/40'
                              : 'border-divider hover:border-blue-500/50 hover:bg-blue-500/10'
                          }`}
                          onClick={() => handleLoadFromCache(item.id)}
                        >
                          <div className="truncate">{factorNames}</div>
                          <div className="text-tertiary-text">
                            {item.top_n}选 · {item.hold_days?.join('/')}日 · {dateStr}
                          </div>
                        </button>
                      </AntTooltip>
                      <button
                        type="button"
                        className="cursor-pointer p-1 rounded text-tertiary-text hover:text-red-400 hover:bg-red-500/10 opacity-0 group-hover:opacity-100 transition-opacity"
                        onClick={(e) => { e.stopPropagation(); handleDeleteCache(item.id); }}
                      >
                        <Trash2 className="h-3 w-3" />
                      </button>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Right Panel */}
        <div ref={rightPanelRef} className="flex-1 space-y-4 min-w-0">
          {!result ? (
            <Card>
              <EmptyState
                icon={<Activity className="w-10 h-10" />}
                title="选择因子开始回测"
                description="在左侧选择一个或多个因子，配置参数后点击开始回测"
              />
            </Card>
          ) : (
            <>
              {/* Summary Stats */}
              <div className="flex items-center gap-2 mb-2">
                <span className="text-sm text-secondary-text">持有期:</span>
                {holdDays.map((hd) => (
                  <button
                    key={hd}
                    type="button"
                    className={`cursor-pointer px-2 py-0.5 text-xs rounded ${
                      summaryPeriod === String(hd)
                        ? 'bg-blue-500/20 text-blue-400'
                        : 'text-secondary-text hover:text-primary-text'
                    }`}
                    onClick={() => setSummaryPeriod(String(hd))}
                  >
                    {hd}日
                  </button>
                ))}
              </div>

              {currentStats && (
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
                  <StatCard label="总收益" value={pct(currentStats.total_return)} tone={currentStats.total_return >= 0 ? 'success' : 'danger'} />
                  <StatCard label="年化收益" value={pct(currentStats.annual_return)} tone={currentStats.annual_return >= 0 ? 'success' : 'danger'} />
                  <StatCard label="Sharpe" value={currentStats.sharpe?.toFixed(2) || '--'} />
                  <StatCard label="最大回撤" value={pct(currentStats.max_drawdown)} tone="danger" />
                  <StatCard label="胜率" value={pct(currentStats.win_rate)} tone={currentStats.win_rate >= 0.5 ? 'success' : 'warning'} />
                  <StatCard label="交易次数" value={String(currentStats.trade_count || 0)} />
                </div>
              )}

              {/* Capital Curve */}
              <Card title="资金曲线">
                {currentStats && (
                  <div className="mb-3 flex flex-wrap gap-x-4 gap-y-1">
                    <span className="text-xs text-secondary-text">
                      总收益:
                      <span className={currentStats.total_return >= 0 ? 'text-red-400' : 'text-emerald-400'}>
                        {pct(currentStats.total_return)}
                      </span>
                    </span>
                    <span className="text-xs text-secondary-text">胜率: {pct(currentStats.win_rate)}</span>
                    <span className="text-xs text-secondary-text">最大回撤: {pct(currentStats.max_drawdown)}</span>
                    <span className="text-xs text-secondary-text">Sharpe: {currentStats.sharpe?.toFixed(2) || '--'}</span>
                    <span className="text-xs text-secondary-text">持仓中: {currentStats.openCount}</span>
                    <span className="text-xs text-secondary-text">跳过: {currentStats.canceledCount}</span>
                  </div>
                )}
                {chartData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2a2a3e" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11, fill: '#888' }}
                        tickFormatter={(v: string) => v.slice(5)}
                      />
                      <YAxis
                        tick={{ fontSize: 11, fill: '#888' }}
                        tickFormatter={(v: number) => fmtMoney(v)}
                      />
                      <Tooltip
                        content={(
                          <CapitalCurveTooltip
                            latestByKey={chartLatestMeta.latestByKey}
                            latestDate={chartLatestMeta.latestDate}
                            baseByKey={chartLatestMeta.baseByKey}
                          />
                        )}
                      />
                      <Legend
                        wrapperStyle={{ cursor: 'pointer' }}
                        formatter={(v) => String(v) === 'benchmark' ? '基准' : `${String(v).replace('hd', '')}日持有`}
                        onClick={(e) => {
                          setHiddenLines((prev) => {
                            const next = new Set(prev);
                            const dk = String(e.dataKey);
                            if (next.has(dk)) next.delete(dk); else next.add(dk);
                            return next;
                          });
                        }}
                      />
                      {holdDays.map((hd, i) => (
                        <Line
                          key={hd}
                          type="monotone"
                          dataKey={`hd${hd}`}
                          stroke={CAPITAL_COLORS[i % CAPITAL_COLORS.length]}
                          dot={false}
                          strokeWidth={1.5}
                          hide={hiddenLines.has(`hd${hd}`)}
                        />
                      ))}
                      {result.benchmark_curve && result.benchmark_curve.length > 0 && (
                        <Line
                          type="monotone"
                          dataKey="benchmark"
                          stroke="#666"
                          dot={false}
                          strokeWidth={1}
                          strokeDasharray="4 4"
                          hide={hiddenLines.has('benchmark')}
                        />
                      )}
                      <Brush dataKey="date" height={24} stroke="#444" fill="#1a1a2e" />
                    </LineChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="text-secondary-text text-sm py-8 text-center">无数据</div>
                )}
                {result.factors && result.factors.length > 0 && (
                  <div className="mt-3 flex flex-wrap gap-1.5">
                    <span className="text-xs text-secondary-text mr-1">因子:</span>
                    {result.factors.map((f) => {
                      const name = String((f as Record<string, unknown>).name || '');
                      const weight = Number((f as Record<string, unknown>).weight || 0);
                      return (
                        <span key={name} className="inline-flex items-center gap-1 rounded bg-[hsl(var(--muted))] px-1.5 py-0.5 text-xs text-secondary-text">
                          {FACTOR_LABELS[name] || name}
                          <span className="text-[hsl(var(--primary))]">{weight.toFixed(1)}</span>
                        </span>
                      );
                    })}
                  </div>
                )}
              </Card>

              {/* Rank IC */}
              {icData && icData.length > 0 && (
                <Card title="Rank IC (因子有效性)">
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={icData} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" stroke="#2a2a3e" />
                      <XAxis type="number" tick={{ fontSize: 11, fill: '#888' }} />
                      <YAxis
                        type="category"
                        dataKey="label"
                        width={100}
                        tick={{ fontSize: 11, fill: '#888' }}
                      />
                      <Tooltip
                        contentStyle={{ backgroundColor: '#000', border: '1px solid #333' }}
                        labelStyle={{ color: '#fff' }}
                        itemStyle={{ color: '#fff' }}
                        formatter={(v) => Number(v).toFixed(4)}
                      />
                      <Bar dataKey="ic" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </Card>
              )}

              {/* Trade Records */}
              {displayTrades.length > 0 && (
                <Card>
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <div>
                      <h3 className="text-lg font-semibold text-foreground">
                        交易记录 ({summaryPeriod}日持有)
                      </h3>
                      <p className="mt-1 text-xs text-secondary-text">
                        买入价/卖出价为不复权价；盈亏、收益率、买卖金额按后复权价计算（除权除息日价格连续，含分红送转）
                      </p>
                    </div>
                    <button
                      type="button"
                      className="inline-flex shrink-0 cursor-pointer items-center gap-1 rounded-lg border px-2 py-1 text-xs text-secondary-text hover:text-foreground transition-colors"
                      onClick={() => {
                        const header = '发现日,股票代码,股票名称,买入日,买入价(不复权),后复权买入价,股数,买入额(复权),卖出日,卖出价(不复权),后复权卖出价,卖出额(复权),收益率(复权),盈亏,状态';
                        const rows = displayTrades.map((t) => {
                          const amount = Math.round(t.allocated || 0);
                          const sellAmount = Math.round((t.allocated || 0) + (t.pnl || 0));
                          const statusMap: Record<string, string> = { closed: '已平', extended: '延期', canceled: '取消', open: '持仓', pending: '待执行', locked: '锁仓' };
                          return `${t.trade_date},${t.stock_code},"${t.stock_name}",${t.buy_date},${t.buy_price},${t.buy_price_adj ?? t.buy_price},${t.shares || 0},${amount},${t.sell_date},${t.sell_price},${t.sell_price_adj ?? t.sell_price},${sellAmount},${t.return_pct},${t.pnl},${statusMap[t.status] || t.status}`;
                        }).join('\n');
                        const blob = new Blob(['﻿' + header + '\n' + rows], { type: 'text/csv;charset=utf-8;' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = `快测交易记录_${summaryPeriod}日_${result?.date_range?.start || ''}_${result?.date_range?.end || ''}.csv`;
                        a.click();
                        URL.revokeObjectURL(url);
                      }}
                    >
                      <Download className="h-3.5 w-3.5" />导出 Excel
                    </button>
                  </div>
                  <Table
                    dataSource={displayTrades}
                    columns={tradeColumns}
                    rowKey={(r) => `${r.trade_date}_${r.stock_code}_${r.hold_days}`}
                    size="small"
                    scroll={{ x: 1060, y: 640 }}
                    pagination={{ pageSize: 100, showSizeChanger: true, showTotal: (t) => `共 ${t} 条` }}
                  />
                  {topContributionStocks.length > 0 && (
                    <div className="mt-6 border-t border-divider pt-4">
                      <h4 className="mb-1 text-sm font-semibold text-foreground">
                        收益贡献 Top5 个股
                      </h4>
                      <p className="mb-3 text-xs text-secondary-text">
                        按已平仓/延期交易累计盈亏排序；贡献占比 = 该股盈亏 ÷ 全部已平仓盈亏之和
                      </p>
                      <Table
                        dataSource={topContributionStocks}
                        columns={contributionColumns}
                        rowKey="stock_code"
                        size="small"
                        pagination={false}
                      />
                    </div>
                  )}
                </Card>
              )}
            </>
          )}
        </div>
      </div>
    </AppPage>
  );
};

export default SimpleFactorBacktestPage;