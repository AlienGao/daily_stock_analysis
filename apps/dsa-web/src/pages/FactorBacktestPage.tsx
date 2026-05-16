import type React from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import dayjs from 'dayjs';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  BarChart, Bar, CartesianGrid,
} from 'recharts';
import { DatePicker, Segmented, Table, InputNumber, Checkbox } from 'antd';
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

const FactorBacktestPage: React.FC = () => {
  const [mode, setMode] = useState<TabKey>('postmarket');
  const [loading, setLoading] = useState(false);
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

  // result
  const [result, setResult] = useState<FactorBacktestResultResponse | null>(null);
  const [summaryPeriod, setSummaryPeriod] = useState('1');
  const [selectedCurves, setSelectedCurves] = useState<Record<string, boolean>>({});

  // load snapshot dates on mount + mode change
  useEffect(() => {
    let cancelled = false;
    discoveryApi.getFactorSnapshotDates(mode).then((data) => {
      if (cancelled) return;
      setSnapData(data);
      setResult(null);
      setError(null);
      // init selection: all checked, 0 weight (use default)
      const sf: Record<string, boolean> = {};
      const fw: Record<string, number> = {};
      for (const f of data.factors) {
        sf[f.name] = true;
        fw[f.name] = f.default_weight || 0;
      }
      setSelectedFactors(sf);
      setFactorWeights(fw);
      if (data.global.available_from) {
        setStartDate(data.global.available_from);
        setEndDate(data.global.available_to);
      }
      setSelectedCurves({});
    }).catch((e) => {
      if (!cancelled) setError(getParsedApiError(e));
    });
    return () => { cancelled = true; };
  }, [mode]);

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
    setLoading(true);
    setError(null);
    setResult(null);
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
      });

      // poll until complete
      const poll = async (): Promise<FactorBacktestResultResponse> => {
        const status = await discoveryApi.getFactorBacktestStatus(task_id);
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
      for (const k of Object.keys(data.capital_curves)) {
        sc[k] = true;
      }
      setSelectedCurves(sc);
      setSummaryPeriod(String(Math.min(...holdDays)));
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setLoading(false);
    }
  }, [mode, activeFactors, factorWeights, startDate, endDate, topN, holdDays, initialCapital, riskFreeRate, usePipeline]);

  // 切换持有期 → 曲线联动
  useEffect(() => {
    if (!result) return;
    setSelectedCurves((prev) => {
      const n: Record<string, boolean> = {};
      for (const k of Object.keys(prev)) n[k] = false;
      n[summaryPeriod] = true;
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
    const sorted = Array.from(allDates).sort();
    const data: Record<string, string | number | undefined>[] = [];
    for (const d of sorted) {
      const row: Record<string, string | number | undefined> = { date: d.slice(4) };
      for (const kd of Object.keys(result.capital_curves)) {
        if (!selectedCurves[kd]) continue;
        const curve = result.capital_curves[kd];
        const pt = curve.find((p: FactorBacktestCapitalPoint) => p.date === d);
        row[`h${kd}`] = pt ? pt.capital : undefined;
      }
      data.push(row);
    }
    return data;
  }, [result, selectedCurves]);

  const quantileData = useMemo(() => {
    if (!result?.quantile_returns) return null;
    const q = result.quantile_returns[summaryPeriod];
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
    _pipeline: '管线综合',
  };

  const periodStats = useMemo(() => {
    if (!result) return {};
    const stats: Record<string, { closed: FactorBacktestTrade[]; wr: number; cumRet: number; mdd: number; annRet: number }> = {};
    const hds = [...new Set(result.trade_records.map((t: FactorBacktestTrade) => t.hold_days))].sort();
    for (const hd of hds) {
      const trades = result.trade_records.filter((t: FactorBacktestTrade) => t.hold_days === hd);
      const closed = trades.filter((t: FactorBacktestTrade) => t.status === 'closed' || t.status === 'extended');
      const wins = closed.filter((t: FactorBacktestTrade) => t.return_pct > 0).length;
      const wr = closed.length > 0 ? wins / closed.length : 0;
      // 从资金曲线终点算（考虑仓位分配）
      const curve = result.capital_curves[String(hd)] || [];
      const cumRet = curve.length > 1 ? (curve[curve.length - 1].capital / result.params.initial_capital) - 1 : 0;
      const annRet = cumRet > -1 && curve.length > 1 ? (1 + cumRet) ** (252 / (curve.length - 1)) - 1 : 0;
      
      // max drawdown
      let peak = result.params.initial_capital, mdd = 0;
      for (const pt of curve) {
        if (pt.capital > peak) peak = pt.capital;
        const dd = peak > 0 ? (peak - pt.capital) / peak : 0;
        if (dd > mdd) mdd = dd;
      }
      stats[String(hd)] = { closed, wr, cumRet, mdd, annRet };
    }
    return stats;
  }, [result]);

  const currentStats = periodStats[summaryPeriod];

  const [icSortOrder, setIcSortOrder] = useState<'descend' | 'ascend'>('descend');

  const icData = useMemo(() => {
    if (!result?.rank_ic) return null;
    const dayData = result.rank_ic[summaryPeriod];
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
    { title: '代码', dataIndex: 'stock_code', key: 'stock_code', width: 90 },
    { title: '名称', dataIndex: 'stock_name', key: 'stock_name', width: 100 },
    { title: '买入价', dataIndex: 'buy_price', key: 'buy_price', width: 80, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.buy_price },
    { title: '卖出日', dataIndex: 'sell_date', key: 'sell_date', width: 100, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.sell_date },
    { title: '卖出价', dataIndex: 'sell_price', key: 'sell_price', width: 80, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.sell_price },
    { title: '买入额', dataIndex: 'allocated', key: 'allocated', width: 90, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : (r.allocated > 0 ? r.allocated.toFixed(2) : '--') },
    { title: '卖出额', dataIndex: 'allocated', key: 'sell_amount', width: 90, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : (r.allocated > 0 ? (r.allocated + r.pnl).toFixed(2) : '--') },
    { title: '收益', dataIndex: 'return_pct', key: 'return_pct', width: 80, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : pct(r.return_pct) },
    { title: '盈亏', dataIndex: 'pnl', key: 'pnl', width: 100, render: (_: unknown, r: FactorBacktestTrade) => r.status === 'pending' ? '--' : r.pnl.toFixed(2) },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80, render: (_: unknown, r: FactorBacktestTrade) => {
      const m: Record<string, string> = { closed: '已平', extended: '延期', canceled: '取消', open: '持仓', pending: '待执行' };
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
                {snapData?.factors.map((f) => (
                  <div key={f.name} className="flex items-center gap-2 py-1">
                    <Checkbox
                      checked={!!selectedFactors[f.name]}
                      onChange={(e) => setSelectedFactors((p) => ({ ...p, [f.name]: e.target.checked }))}
                    />
                    <span className="text-sm flex-1 truncate text-foreground/85">{f.label}</span>
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
                    value={startDate ? dayjs(startDate) : null}
                    onChange={(d) => setStartDate(d ? d.format('YYYYMMDD') : '')}
                    status={dateOutOfRange ? 'error' : undefined}
                    disabled={activeFactors.length === 0 || !dateRangeIntersection[0]}
                    disabledDate={(d) => {
                      if (!dateRangeIntersection[0]) return true;
                      return d.isBefore(dayjs(dateRangeIntersection[0])) || d.isAfter(dayjs(dateRangeIntersection[1]));
                    }}
                  />
                </div>
                <div className="flex items-center gap-2">
                  <span className={`text-xs whitespace-nowrap ${dateOutOfRange ? 'text-red-500' : 'text-tertiary-text'}`}>
                    结束日期{dateOutOfRange ? '（超范围）' : ''}
                  </span>
                  <DatePicker
                    size="small"
                    value={endDate ? dayjs(endDate) : null}
                    onChange={(d) => setEndDate(d ? d.format('YYYYMMDD') : '')}
                    status={dateOutOfRange ? 'error' : undefined}
                    disabled={activeFactors.length === 0 || !dateRangeIntersection[0]}
                    disabledDate={(d) => {
                      if (!dateRangeIntersection[0]) return true;
                      return d.isBefore(dayjs(dateRangeIntersection[0])) || d.isAfter(dayjs(dateRangeIntersection[1]));
                    }}
                  />
                </div>
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
                  options={Object.keys(periodStats).map((k) => {
                    const s = periodStats[k];
                    return { label: `${k}日 (${s.closed.length}笔)`, value: k };
                  })}
                />
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <StatCard label="累计收益" value={currentStats ? pctNum(currentStats.cumRet) : '--'} />
                <StatCard label="胜率" value={currentStats ? pctNum(currentStats.wr) : '--'} />
                <StatCard label="最大回撤" value={currentStats ? pctNum(currentStats.mdd) : '--'} />
                <StatCard label="已平仓" value={currentStats ? String(currentStats.closed.length) : '--'} />
                <StatCard label="期数" value={String(result.summary.total_periods)} />
                <StatCard label="最终资金" value={fmtMoney(result.summary.final_capital)} />
                <StatCard label="年化收益" value={currentStats ? pctNum(currentStats.annRet) : '--'} />
                <StatCard label="夏普比率" value={result.summary.sharpe_ratio.toFixed(2)} />
              </div>

              {/* Capital Curve */}
              {chartData && chartData.length > 0 && (
                <Card>
                  <div className="font-medium text-sm text-secondary-text mb-3">资金曲线</div>
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                      <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                      <Tooltip contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }} />
                      <Legend onClick={(e) => {
                        if (!e.dataKey || typeof e.dataKey !== 'string') return;
                        const key = e.dataKey.replace('h', '');
                        // 同步 summaryPeriod
                        if (selectedCurves[key]) return;
                        setSummaryPeriod(key);
                      }} />
                      {Object.keys(result.capital_curves).map((k, i) => (
                        <Line
                          key={k}
                          type="monotone"
                          dataKey={`h${k}`}
                          name={`${k}日`}
                          stroke={CAPITAL_COLORS[i % CAPITAL_COLORS.length]}
                          strokeWidth={2}
                          strokeOpacity={selectedCurves[k] ? 1 : 0.15}
                          dot={false}
                          connectNulls
                        />
                      ))}
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
              {result.trade_records.length > 0 && (
                <Card>
                  <div className="font-medium text-sm text-secondary-text mb-3">
                    交易明细（{currentStats ? currentStats.closed.length : result.trade_records.length} 笔 · {summaryPeriod}日）
                    <button
                      type="button"
                      className="ml-auto inline-flex items-center gap-1 rounded-lg border px-2 py-1 text-xs text-secondary-text hover:text-foreground transition-colors"
                      onClick={() => {
                        const header = '信号日,持有期,代码,名称,买入价,卖出日,卖出价,买入额,卖出额,收益率,盈亏,状态';
                        const rows = result.trade_records.filter((t: FactorBacktestTrade) => t.hold_days === Number(summaryPeriod)).map((t: FactorBacktestTrade) =>
                          `${t.trade_date},${t.hold_days},${t.stock_code},${t.stock_name},${t.buy_price},${t.sell_date},${t.sell_price},${t.allocated || 0},${(t.allocated || 0) + (t.pnl || 0)},${t.return_pct},${t.pnl},${t.status}`
                        ).join('\n');
                        const bom = '\uFEFF';
                        const blob = new Blob([bom + header + '\n' + rows], { type: 'text/csv;charset=utf-8;' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url; a.download = `factor_backtest_${result.date_range.start}_${result.date_range.end}.csv`;
                        a.click(); URL.revokeObjectURL(url);
                      }}
                    >
                      <Download className="h-3.5 w-3.5" />导出 CSV
                    </button>
                    <button
                      type="button"
                      className="inline-flex items-center gap-1 rounded-lg border px-2 py-1 text-xs text-secondary-text hover:text-foreground transition-colors"
                      onClick={() => {
                        const curves = result.capital_curves;
                        const keys = Object.keys(curves).filter(k => curves[k].length > 1);
                        if (!keys.length) return;
                        const allCap: number[] = [];
                        keys.forEach(k => curves[k].forEach((p: FactorBacktestCapitalPoint) => allCap.push(p.capital)));
                        const minC = Math.min(...allCap), maxC = Math.max(...allCap);
                        const pad = (maxC - minC) * 0.1 || 10000;
                        const yMin = minC - pad, yMax = maxC + pad;
                        const W = 800, H = 360, R = 30;
                        const colors = ['#22c55e','#3b82f6','#f59e0b','#8b5cf6','#ef4444'];
                        const toX = (i: number, n: number) => R + (i / Math.max(n - 1, 1)) * (W - 2 * R);
                        const toY = (v: number) => H - R - ((v - yMin) / (yMax - yMin)) * (H - 2 * R);
                        let paths = '';
                        keys.forEach((k, ki) => {
                          const pts = curves[k];
                          const d = pts.map((p: FactorBacktestCapitalPoint, i: number) => `${i === 0 ? 'M' : 'L'}${toX(i, pts.length).toFixed(1)},${toY(p.capital).toFixed(1)}`).join(' ');
                          paths += `<path d="${d}" fill="none" stroke="${colors[ki % colors.length]}" stroke-width="2"/>\n`;
                        });
                        let yLabels = '';
                        for (let yi = 0; yi <= 4; yi++) {
                          const v = yMin + (yi / 4) * (yMax - yMin);
                          yLabels += `<text x="${R - 8}" y="${toY(v).toFixed(1)}" text-anchor="end" font-size="10" fill="#888">${(v / 10000).toFixed(1)}万</text>\n`;
                        }
                        let legend = '';
                        keys.forEach((k, ki) => {
                          legend += `<rect x="${R + ki * 70}" y="${H - R + 8}" width="12" height="12" fill="${colors[ki % colors.length]}"/><text x="${R + ki * 70 + 16}" y="${H - R + 18}" font-size="11" fill="#ccc">${k}日</text>`;
                        });
                        const s = result.summary;
                        const html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>因子回测 '+result.date_range.start+'-'+result.date_range.end+'</title><style>body{font-family:-apple-system,sans-serif;background:#111;color:#ddd;padding:24px;max-width:900px;margin:auto}h1{font-size:18px;margin-bottom:16px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}.card{background:#1a1a1a;border-radius:8px;padding:14px}.card .label{font-size:11px;color:#888}.card .value{font-size:20px;font-weight:600;color:#fff}svg{display:block;margin:0 auto}table{width:100%;border-collapse:collapse;margin-top:24px;font-size:12px}th,td{padding:6px 8px;text-align:left;border-bottom:1px solid #333}th{color:#888;font-weight:500}td{color:#ccc}</style></head><body><h1>因子回测报告 '+result.date_range.start+' ~ '+result.date_range.end+' ('+result.mode+')</h1><div class="grid"><div class="card"><div class="label">累计收益</div><div class="value">'+(s.cumulative_return*100).toFixed(2)+'%</div></div><div class="card"><div class="label">年化收益</div><div class="value">'+(s.annualized_return*100).toFixed(2)+'%</div></div><div class="card"><div class="label">胜率</div><div class="value">'+(s.win_rate*100).toFixed(1)+'%</div></div><div class="card"><div class="label">夏普</div><div class="value">'+s.sharpe_ratio.toFixed(2)+'</div></div><div class="card"><div class="label">最大回撤</div><div class="value" style="color:#ef4444">'+(s.max_drawdown*100).toFixed(2)+'%</div></div><div class="card"><div class="label">已平仓</div><div class="value">'+s.total_trades+'</div></div><div class="card"><div class="label">最终资金</div><div class="value">'+(s.final_capital/10000).toFixed(1)+'万</div></div><div class="card"><div class="label">初始</div><div class="value">'+(result.params.initial_capital/10000).toFixed(0)+'万</div></div></div><svg width="'+W+'" height="'+(H+20)+'"><line x1="'+R+'" y1="'+(H-R)+'" x2="'+(W-R)+'" y2="'+(H-R)+'" stroke="#444"/><line x1="'+R+'" y1="'+R+'" x2="'+R+'" y2="'+(H-R)+'" stroke="#444"/>'+yLabels+paths+legend+'</svg><table><thead><tr><th>信号日</th><th>持有期</th><th>代码</th><th>名称</th><th>买入价</th><th>卖出日</th><th>卖出价</th><th>买入额</th><th>卖出额</th><th>收益</th><th>盈亏</th><th>状态</th></tr></thead><tbody>'+result.trade_records.filter((t: FactorBacktestTrade) => t.hold_days === Number(summaryPeriod)).map((t: FactorBacktestTrade) => '<tr><td>'+t.trade_date+'</td><td>'+t.hold_days+'日</td><td>'+t.stock_code+'</td><td>'+t.stock_name+'</td><td>'+t.buy_price+'</td><td>'+t.sell_date+'</td><td>'+t.sell_price+'</td><td>'+(t.allocated||0).toFixed(0)+'</td><td>'+((t.allocated||0)+(t.pnl||0)).toFixed(0)+'</td><td>'+(t.return_pct*100).toFixed(2)+'%</td><td>'+t.pnl.toFixed(0)+'</td><td>'+t.status+'</td></tr>').join('')+'</tbody></table></body></html>';
                        const blob = new Blob([html], { type: 'text/html;charset=utf-8;' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url; a.download = 'factor_backtest_'+result.date_range.start+'_'+result.date_range.end+'.html';
                        a.click(); URL.revokeObjectURL(url);
                      }}
                    >
                      <Download className="h-3.5 w-3.5" />导出 HTML
                    </button>
                  </div>
                  <Table
                    size="small"
                    dataSource={result.trade_records.filter((t: FactorBacktestTrade) => t.hold_days === Number(summaryPeriod))}
                    rowKey={(r) => `${r.trade_date}_${r.hold_days}_${r.stock_code}`}
                    pagination={{ pageSize: 50, showSizeChanger: false }}
                    columns={tradeColumns}
                    scroll={{ x: 800 }}
                  />
                </Card>
              )}
            </>
          )}

          {!result && !error && !loading && (
            <EmptyState icon={<Activity className="h-8 w-8" />} title="选择因子和参数后开始回测" />
          )}
        </div>
      </div>
    </AppPage>
  );
};

export default FactorBacktestPage;
