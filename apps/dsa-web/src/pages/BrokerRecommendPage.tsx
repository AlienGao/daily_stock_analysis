import type React from 'react';
import { useState, useEffect, useCallback, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import dayjs, { type Dayjs } from 'dayjs';
import { DatePicker, Table, Tabs } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import type { ColumnsType } from 'antd/es/table';
import { TrendingUp, RefreshCw, ChevronDown, ChevronRight, Loader2 } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { AppPage, Button, Card, EmptyState } from '../components/common';
import {
  getMonthlyRecommendations,
  fetchMonth,
  getBacktest,
  getMonthlyEnrichment,
  getYtdBacktest,
  type BrokerRecommendResponse,
  type BrokerRecommendItem,
  type BrokerBacktestResponse,
  type EnrichmentResponse,
  type YtdBacktestResponse,
} from '../api/brokerRecommend';

const BROKER_COLORS = [
  '#34d399', '#60a5fa', '#f472b6', '#fbbf24', '#a78bfa',
  '#fb923c', '#2dd4bf', '#e879f9', '#facc15', '#38bdf8',
];

function fmtDate(s: string): string {
  if (!s || s.length < 8) return s;
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

function fmtPct(v?: number | null): string {
  if (v == null) return '--';
  return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`;
}

function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload) return null;
  return (
    <div
      style={{
        background: 'hsl(var(--card))',
        border: '1px solid hsl(var(--border))',
        borderRadius: '8px',
        fontSize: '12px',
        padding: '8px 12px',
        minWidth: '170px',
      }}
    >
      <div className="text-xs font-medium mb-1 text-secondary-text">{label}</div>
      <div style={{ maxHeight: '120px', overflowY: 'auto' }}>
        {payload
          .filter((p: any) => p.value != null)
          .map((p: any) => (
            <div key={p.name} className="flex items-center gap-2 text-xs py-0.5">
              <span
                className="w-2 h-2 rounded-full shrink-0"
                style={{ backgroundColor: p.color }}
              />
              <span className="text-secondary-text">{p.name}</span>
              <span className="font-medium ml-auto tabular-nums">
                {`${(p.value * 100).toFixed(2)}%`}
              </span>
            </div>
          ))}
      </div>
    </div>
  );
}

/** Deduplicate by ts_code, keep max broker_count */
function dedupStocks(items: BrokerRecommendItem[]): BrokerRecommendItem[] {
  const map = new Map<string, BrokerRecommendItem>();
  for (const item of items) {
    const existing = map.get(item.ts_code);
    if (!existing || item.broker_count > existing.broker_count) {
      map.set(item.ts_code, item);
    }
  }
  return Array.from(map.values());
}

type StockRow = {
  ts_code: string;
  name: string;
  broker_count: number;
  endPrice?: number;
  endDate?: string;
  cumRet?: number;
  nineturn?: {
    up_count?: number | null;
    down_count?: number | null;
    nine_up_turn?: number | null;
    nine_down_turn?: number | null;
  } | null;
  forecast?: {
    eps?: number | null;
    pe?: number | null;
    roe?: number | null;
    np?: number | null;
    rating?: string | null;
    min_price?: number | null;
    max_price?: number | null;
    imp_dg?: string | null;
  } | null;
  cyq_perf?: {
    cost_avg?: number | null;
    winner_rate?: number | null;
    concentration?: number | null;
  } | null;
};

const BrokerRecommendPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const monthParam = searchParams.get('month');
  const selectedMonth: Dayjs = monthParam ? dayjs(monthParam, 'YYYYMM') : dayjs();
  const [loadingData, setLoadingData] = useState(false);
  const [recommendData, setRecommendData] = useState<BrokerRecommendResponse | null>(null);
  const [backtestData, setBacktestData] = useState<BrokerBacktestResponse | null>(null);
  const [enrichmentData, setEnrichmentData] = useState<EnrichmentResponse | null>(null);
  const [loadingEnrichment, setLoadingEnrichment] = useState(false);
  const [expandedBrokers, setExpandedBrokers] = useState<Set<string>>(new Set());
  const [viewMode, setViewMode] = useState<'broker' | 'stock'>('broker');
  const [visibleChartBrokers, setVisibleChartBrokers] = useState<Set<string>>(new Set());
  const [expandedStock, setExpandedStock] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string>('monthly');
  const [ytdData, setYtdData] = useState<YtdBacktestResponse | null>(null);
  const [ytdLoading, setYtdLoading] = useState(false);

  const monthStr = selectedMonth.format('YYYYMM');

  // Auto-load recommendations when selectedMonth changes
  useEffect(() => {
    if (!monthStr) return;
    async function load() {
      setLoadingData(true);
      setLoadingEnrichment(true);
      setRecommendData(null);
      setBacktestData(null);
      setEnrichmentData(null);
      setExpandedStock(null);
      try {
        const data = await getMonthlyRecommendations(monthStr);
        setRecommendData(data);
        const [bt, enrich] = await Promise.all([
          getBacktest(monthStr),
          getMonthlyEnrichment(monthStr),
        ]);
        setBacktestData(bt);
        setEnrichmentData(enrich);
      } catch (e) {
        console.error('Failed to load:', e);
      } finally {
        setLoadingData(false);
        setLoadingEnrichment(false);
      }
    }
    load();
  }, [monthStr]);

  const handleFetch = useCallback(async () => {
    if (!monthStr) return;
    setLoadingData(true);
    try {
      await fetchMonth(monthStr);
    } catch (e) {
      console.error('Failed to fetch:', e);
    } finally {
      setLoadingData(false);
    }
  }, [monthStr]);

  // Init chart to top 5 brokers by cumulative return
  useEffect(() => {
    if (backtestData?.brokers?.length) {
      const top5 = [...backtestData.brokers]
        .sort((a, b) => b.cumulative_return - a.cumulative_return)
        .slice(0, 5)
        .map(b => b.broker);
      setVisibleChartBrokers(new Set(top5));
    }
  }, [backtestData]);

  // Load YTD data when switching to YTD tab
  useEffect(() => {
    if (activeTab !== 'ytd' || ytdData) return;
    const currentYear = String(dayjs().year());
    setYtdLoading(true);
    getYtdBacktest(currentYear, 5)
      .then(setYtdData)
      .catch((e) => console.error('Failed to load YTD:', e))
      .finally(() => setYtdLoading(false));
  }, [activeTab, ytdData]);

  const toggleBroker = (broker: string) => {
    setExpandedBrokers(prev => {
      const next = new Set(prev);
      if (next.has(broker)) next.delete(broker);
      else next.add(broker);
      return next;
    });
  };

  const toggleChartBroker = (broker: string) => {
    setVisibleChartBrokers(prev => {
      const next = new Set(prev);
      if (next.has(broker)) next.delete(broker);
      else next.add(broker);
      return next;
    });
  };

  // Chart data
  const chartData = (() => {
    if (!backtestData) return [];
    const dateSet = new Set<string>();
    backtestData.brokers.forEach(b => {
      b.daily_returns.forEach(d => dateSet.add(d.date));
    });
    const dates = Array.from(dateSet).sort();
    return dates.map(date => {
      const entry: Record<string, string | number | undefined> = { date: fmtDate(date) };
      backtestData.brokers.forEach((b) => {
        const dr = b.daily_returns.find(d => d.date === date);
        entry[b.broker] = dr?.cumulative;
      });
      return entry;
    });
  })();

  // YTD chart data
  const ytdChartData = useMemo(() => {
    if (!ytdData) return [];
    const dateSet = new Set<string>();
    ytdData.brokers.forEach(b => {
      b.daily_returns.forEach(d => dateSet.add(d.date));
    });
    const dates = Array.from(dateSet).sort();
    return dates.map(date => {
      const entry: Record<string, string | number | undefined> = { date: fmtDate(date) };
      ytdData.brokers.forEach(b => {
        const dr = b.daily_returns.find(d => d.date === date);
        entry[b.broker] = dr?.cumulative;
      });
      return entry;
    });
  }, [ytdData]);

  // Build deduped stock rows with enrichment
  const stockRows = useMemo((): StockRow[] => {
    if (!recommendData?.items) return [];
    return dedupStocks(recommendData.items).map(item => {
      const stockRet = backtestData?.stock_returns?.find(
        s => s.ts_code === item.ts_code
      );
      const cumRet = stockRet?.daily_returns?.length
        ? stockRet.daily_returns[stockRet.daily_returns.length - 1].cumulative
        : undefined;
      return {
        ts_code: item.ts_code,
        name: item.name,
        broker_count: item.broker_count,
        endPrice: stockRet?.end_price,
        endDate: stockRet?.end_date,
        cumRet,
        nineturn: enrichmentData?.data[item.ts_code]?.nineturn ?? null,
        forecast: enrichmentData?.data[item.ts_code]?.forecast ?? null,
        cyq_perf: enrichmentData?.data[item.ts_code]?.cyq_perf ?? null,
      };
    });
  }, [recommendData, backtestData, enrichmentData]);

  // --- Table column definitions ---
  const stockColumns: ColumnsType<StockRow> = useMemo(() => [
    {
      title: '代码', dataIndex: 'ts_code', key: 'ts_code',
      render: (v: string) => <span className="font-mono text-xs">{v}</span>,
    },
    {
      title: '名称', dataIndex: 'name', key: 'name',
      render: (v: string) => <span className="text-xs text-secondary-text">{v}</span>,
    },
    {
      title: '月末价', dataIndex: 'endPrice', key: 'endPrice',
      render: (_: any, row: StockRow) => (
        <span className="text-xs text-secondary-text whitespace-nowrap">
          {row.endPrice != null ? row.endPrice.toFixed(2) : '--'}
          {row.endDate ? <span className="text-tertiary-text ml-1">({fmtDate(row.endDate).slice(5)})</span> : null}
        </span>
      ),
    },
    {
      title: <>九转信号{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</>,
      key: 'nineturn',
      render: (_, row) => {
        const nt = row.nineturn;
        if (!nt) return <span className="text-xs text-tertiary-text">--</span>;
        if (nt.nine_up_turn) return <span className="text-xs text-emerald-400 font-medium">上涨9转</span>;
        if (nt.nine_down_turn) return <span className="text-xs text-red-400 font-medium">下跌9转</span>;
        if (nt.up_count || nt.down_count) return (
          <span className="text-xs">
            {nt.up_count ? <span className="text-red-400">↑{nt.up_count}</span> : null}
            {nt.up_count && nt.down_count ? ' ' : null}
            {nt.down_count ? <span className="text-emerald-400">↓{nt.down_count}</span> : null}
          </span>
        );
        return <span className="text-xs text-tertiary-text">--</span>;
      },
    },
    {
      title: <>盈利预测{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</>,
      key: 'forecast',
      render: (_, row) => {
        const fc = row.forecast;
        if (!fc) return <span className="text-xs text-tertiary-text">--</span>;
        const hasRating = !!fc.rating;
        const hasPrice = fc.min_price != null || fc.max_price != null;
        const hasImpDg = !!fc.imp_dg;
        if (!hasRating && !hasPrice && !hasImpDg) return <span className="text-xs text-tertiary-text">--</span>;
        return (
          <div className="text-xs">
            {hasRating && <div className="font-medium text-cyan-400">{fc.rating}</div>}
            {hasPrice && (
              <div className="text-secondary-text">
                {fc.min_price != null ? fc.min_price!.toFixed(2) : '?'}~{fc.max_price != null ? fc.max_price!.toFixed(2) : '?'}
              </div>
            )}
            {hasImpDg && <div className="text-tertiary-text">{fc.imp_dg}</div>}
          </div>
        );
      },
    },
    {
      title: <>筹码胜率{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</>,
      key: 'cyq_perf',
      sorter: (a, b) => (a.cyq_perf?.winner_rate ?? -Infinity) - (b.cyq_perf?.winner_rate ?? -Infinity),
      render: (_, row) => {
        const cyq = row.cyq_perf;
        if (!cyq) return <span className="text-xs text-tertiary-text">--</span>;
        return (
          <div className="text-xs">
            {cyq.winner_rate != null && (
              <div className={cyq.winner_rate >= 0.5 ? 'text-red-400' : 'text-emerald-400'}>
                {(cyq.winner_rate * 100).toFixed(1)}%
              </div>
            )}
            {cyq.cost_avg != null && (
              <div className="text-tertiary-text">{cyq.cost_avg.toFixed(2)}</div>
            )}
          </div>
        );
      },
    },
    {
      title: '累计收益', key: 'cumRet',
      sorter: (a, b) => (a.cumRet ?? -Infinity) - (b.cumRet ?? -Infinity),
      defaultSortOrder: 'descend',
      render: (_, row) => (
        <span className={`text-xs font-medium ${row.cumRet != null ? (row.cumRet >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-tertiary-text'}`}>
          {fmtPct(row.cumRet)}
        </span>
      ),
    },
    {
      title: <span style={{ whiteSpace: 'nowrap' }}>推荐数</span>, dataIndex: 'broker_count', key: 'broker_count',
      sorter: (a, b) => a.broker_count - b.broker_count,
      render: (v: number) => <span className="text-xs text-tertiary-text whitespace-nowrap">{v}</span>,
    },
  ], [loadingEnrichment]);

  // Broker groups
  const brokerGroups = useMemo((): Map<string, BrokerRecommendItem[]> => {
    if (!recommendData?.items?.length) return new Map();
    const map = new Map<string, BrokerRecommendItem[]>();
    for (const item of recommendData.items) {
      const existing = map.get(item.broker) || [];
      existing.push(item);
      map.set(item.broker, existing);
    }
    return map;
  }, [recommendData]);

  return (
    <AppPage>
      <Tabs
        activeKey={activeTab}
        onChange={(key) => setActiveTab(key)}
        items={[
          {
            key: 'monthly',
            label: '月度金股',
            children: (
              <div className="space-y-4 pt-2">
        {/* Controls */}
        <Card className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2">
              <label className="text-sm text-secondary-text">月份</label>
              <DatePicker
                picker="month"
                locale={zhCN.DatePicker}
                value={selectedMonth}
                onChange={(d) => { if (d) setSearchParams({ month: d.format('YYYYMM') }); }}
                allowClear={false}
                disabledDate={(d) => d.isAfter(dayjs(), 'month')}
                className="h-9"
              />
            </div>

            <Button
              variant="outline"
              size="sm"
              onClick={handleFetch}
              disabled={loadingData}
            >
              {loadingData ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <RefreshCw className="h-4 w-4 mr-1" />}
              获取当月数据
            </Button>

            <span className="text-xs text-tertiary-text ml-auto">
              {backtestData
                ? `回测区间: ${fmtDate(backtestData.buy_date)} → ${fmtDate(backtestData.sell_date)}`
                : recommendData
                ? `${monthStr} 月券商金股`
                : '--'}
            </span>
          </div>
        </Card>

        {/* Loading */}
        {loadingData && (
          <Card className="p-4 text-center text-sm text-tertiary-text">
            <Loader2 className="h-4 w-4 animate-spin inline mr-2" />
            加载中...
          </Card>
        )}

        {/* Overview */}
        {recommendData && !loadingData && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <Card className="p-3 text-center">
              <div className="text-lg font-bold">{recommendData.total_recommendations}</div>
              <div className="text-xs text-secondary-text">推荐总数</div>
            </Card>
            <button
              className={`rounded-2xl terminal-card p-3 text-center cursor-pointer transition-colors w-full ${viewMode === 'stock' ? 'ring-2 ring-cyan/50 bg-cyan/[0.05]' : 'hover:bg-muted/50'}`}
              onClick={() => setViewMode('stock')}
            >
              <div className="text-lg font-bold">{recommendData.unique_stocks}</div>
              <div className="text-xs text-secondary-text">涉及股票</div>
            </button>
            <button
              className={`rounded-2xl terminal-card p-3 text-center cursor-pointer transition-colors w-full ${viewMode === 'broker' ? 'ring-2 ring-cyan/50 bg-cyan/[0.05]' : 'hover:bg-muted/50'}`}
              onClick={() => setViewMode('broker')}
            >
              <div className="text-lg font-bold">{recommendData.unique_brokers}</div>
              <div className="text-xs text-secondary-text">券商数量</div>
            </button>
            <Card className="p-3 text-center">
              <div className={`text-lg font-bold ${(backtestData?.brokers[0]?.cumulative_return || 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                {fmtPct(backtestData?.brokers[0]?.cumulative_return ?? 0)}
              </div>
              <div className="text-xs text-secondary-text">最优券商收益</div>
            </Card>
          </div>
        )}

        {/* Chart */}
        {backtestData && chartData.length > 0 && backtestData.brokers.length > 0 && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-2">券商组合收益曲线</div>
            {/* Legend: click to toggle, greyed out when hidden */}
            <div className="flex flex-wrap gap-x-3 gap-y-1 mb-1">
              {backtestData.brokers.map((b, i) => {
                const visible = visibleChartBrokers.has(b.broker);
                return (
                  <button
                    key={b.broker}
                    onClick={() => toggleChartBroker(b.broker)}
                    className={`inline-flex items-center gap-1 text-xs transition-opacity ${
                      visible ? 'opacity-100' : 'opacity-30 hover:opacity-60'
                    }`}
                  >
                    <span
                      className="w-2 h-2 rounded-full shrink-0"
                      style={{ backgroundColor: BROKER_COLORS[i % BROKER_COLORS.length] }}
                    />
                    <span className="text-secondary-text">{b.broker}</span>
                  </button>
                );
              })}
            </div>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={chartData} margin={{ top: 4, right: 0, bottom: 6, left: -20 }}>
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#9ca3af' }} stroke="#6b7280" />
                <YAxis
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  stroke="#6b7280"
                  tickFormatter={v => `${(v * 100).toFixed(0)}%`}
                />
                <Tooltip content={<CustomTooltip />} />
                {backtestData.brokers.map((b, i) => (
                  <Line
                    key={b.broker}
                    type="monotone"
                    dataKey={String(b.broker)}
                    stroke={BROKER_COLORS[i % BROKER_COLORS.length]}
                    strokeWidth={1.5}
                    dot={false}
                    connectNulls
                    hide={!visibleChartBrokers.has(b.broker)}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </Card>
        )}

        {/* Tables */}
        {recommendData && brokerGroups.size > 0 && !loadingData && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">
              {viewMode === 'broker' ? '券商金股明细' : '全部金股明细'}
            </div>

            {/* Stock view: flat table */}
            {viewMode === 'stock' && (
              <Table
                columns={stockColumns}
                dataSource={stockRows}
                rowKey="ts_code"
                size="small"
                pagination={false}
                scroll={{ x: 700 }}
              />
            )}

            {/* Broker view: grouped by broker */}
            {viewMode === 'broker' && (
              <div className="space-y-2">
                {Array.from(brokerGroups.entries())
                  .sort(([, aItems], [, bItems]) => {
                    const aBt = backtestData?.brokers.find(b => b.broker === aItems[0]?.broker);
                    const bBt = backtestData?.brokers.find(b => b.broker === bItems[0]?.broker);
                    return (bBt?.cumulative_return ?? -Infinity) - (aBt?.cumulative_return ?? -Infinity);
                  })
                  .map(([broker, items], idx) => {
                  const brokerBt = backtestData?.brokers.find(b => b.broker === broker);
                  const brokerRows: StockRow[] = items.map(item => {
                    const stockRet = backtestData?.stock_returns?.find(
                      s => s.ts_code === item.ts_code
                    );
                    const cumRet = stockRet?.daily_returns?.length
                      ? stockRet.daily_returns[stockRet.daily_returns.length - 1].cumulative
                      : undefined;
                    return {
                      ts_code: item.ts_code,
                      name: item.name,
                      broker_count: item.broker_count,
                      endPrice: stockRet?.end_price,
                      endDate: stockRet?.end_date,
                      cumRet,
                      nineturn: enrichmentData?.data[item.ts_code]?.nineturn ?? null,
                      forecast: enrichmentData?.data[item.ts_code]?.forecast ?? null,
                      cyq_perf: enrichmentData?.data[item.ts_code]?.cyq_perf ?? null,
                    };
                  });

                  return (
                    <div key={broker} className="border border-border/20 rounded-lg overflow-hidden">
                      {/* Broker header */}
                      <button
                        onClick={() => toggleBroker(broker)}
                        className="w-full flex items-center gap-3 px-3 py-2 hover:bg-foreground/[0.02] transition-colors"
                      >
                        <span className="text-xs">
                          {expandedBrokers.has(broker) ? (
                            <ChevronDown className="h-3 w-3" />
                          ) : (
                            <ChevronRight className="h-3 w-3" />
                          )}
                        </span>
                        <span
                          className="w-2 h-2 rounded-full shrink-0"
                          style={{ backgroundColor: BROKER_COLORS[idx % BROKER_COLORS.length] }}
                        />
                        <span className="text-sm font-medium flex-1 text-left">{broker}</span>
                        <span className="text-xs text-secondary-text">{items.length}只</span>
                        <span className={`text-xs font-medium ${(brokerBt?.cumulative_return ?? 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                          {fmtPct(brokerBt?.cumulative_return)}
                        </span>
                        {brokerBt && (
                          <span className="text-xs text-secondary-text">
                            胜率 {brokerBt.win_rate != null
                              ? `${(brokerBt.win_rate * 100).toFixed(0)}%`
                              : '--'}
                          </span>
                        )}
                      </button>

                      {/* Expanded broker detail */}
                      {expandedBrokers.has(broker) && (
                        <div className="px-4 py-2 border-t border-border/10 bg-muted/20">
                          {backtestData ? (
                            <>
                              <Table
                                columns={stockColumns}
                                dataSource={brokerRows}
                                rowKey="ts_code"
                                size="small"
                                pagination={false}
                                scroll={{ x: 700 }}
                                onRow={(record) => ({
                                  onClick: () => setExpandedStock(prev => prev === record.ts_code ? null : record.ts_code),
                                  style: {
                                    cursor: 'pointer',
                                    background: expandedStock === record.ts_code ? 'hsl(var(--accent))' : undefined,
                                  },
                                })}
                              />
                              {/* Expanded stock monthly trend chart */}
                              {expandedStock && brokerRows.some(r => r.ts_code === expandedStock) && (() => {
                                const stockRet = backtestData?.stock_returns?.find(
                                  s => s.ts_code === expandedStock
                                );
                                if (!stockRet?.daily_returns?.length) return null;
                                return (
                                  <div className="mt-3 p-3 border border-border/20 rounded-lg bg-muted/10">
                                    <div className="text-xs font-medium mb-2 text-secondary-text">
                                      {stockRet.name || expandedStock} 月度走势
                                    </div>
                                    <ResponsiveContainer width="100%" height={120}>
                                      <LineChart
                                        margin={{ top: 2, right: 0, bottom: 4, left: -20 }}
                                        data={stockRet.daily_returns.map((d: any) => ({
                                          date: fmtDate(d.date),
                                          cumulative: d.cumulative,
                                          daily_return: d.return ?? d.daily_return,
                                          price: d.price,
                                        }))}
                                      >
                                        <XAxis dataKey="date" tick={{ fontSize: 8, fill: '#9ca3af' }} stroke="#6b7280" interval={3} />
                                        <YAxis tick={{ fontSize: 8, fill: '#9ca3af' }} stroke="#6b7280" tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} />
                                        <Tooltip
                                          content={({ active, payload, label }: any) => {
                                            if (!active || !payload?.length) return null;
                                            const data = payload[0]?.payload;
                                            const dr = data?.daily_return;
                                            const drColor = dr != null ? (dr >= 0 ? '#ef4444' : '#10b981') : '#9ca3af';
                                            const cum = data?.cumulative;
                                            const cumColor = cum != null ? (cum >= 0 ? '#ef4444' : '#10b981') : '#9ca3af';
                                            return (
                                              <div style={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', borderRadius: '6px', padding: '6px 10px', fontSize: 11 }}>
                                                <div style={{ marginBottom: 2, color: '#9ca3af' }}>{label}</div>
                                                <div>涨跌幅: <span style={{ color: drColor }}>{dr != null ? `${(dr * 100).toFixed(2)}%` : '--'}</span></div>
                                                <div>价格: <span style={{ color: '#e2e8f0' }}>{data?.price != null ? data.price.toFixed(2) : '--'}</span></div>
                                                <div>累计: <span style={{ color: cumColor }}>{cum != null ? `${(cum * 100).toFixed(2)}%` : '--'}</span></div>
                                              </div>
                                            );
                                          }}
                                        />
                                        <Line type="monotone" dataKey="cumulative" stroke="#34d399" strokeWidth={1.5} dot={false} connectNulls />
                                        <ReferenceLine y={0} stroke="#4b5563" strokeWidth={1} strokeDasharray="4 4" />
                                      </LineChart>
                                    </ResponsiveContainer>
                                  </div>
                                );
                              })()}
                            </>
                          ) : (
                            <div className="space-y-1">
                              {items.map((item) => (
                                <div key={item.ts_code} className="flex items-center gap-2 text-xs">
                                  <span className="font-mono">{item.ts_code}</span>
                                  <span className="text-secondary-text">{item.name}</span>
                                  <span className="text-tertiary-text ml-auto">被{item.broker_count}家推荐</span>
                                </div>
                              ))}
                            </div>
                          )}
                          {/* Mini chart for this broker */}
                          {backtestData && brokerBt && brokerBt.daily_returns.length > 0 && (() => {
                            const finalCum = brokerBt.daily_returns[brokerBt.daily_returns.length - 1]?.cumulative ?? 0;
                            const cumColor = finalCum >= 0 ? '#ef4444' : '#10b981';
                            return (
                            <div className="mt-2">
                              <ResponsiveContainer width="100%" height={130}>
                                <LineChart
                                  margin={{ top: 4, right: 0, bottom: 4, left: -20 }}
                                  data={brokerBt.daily_returns.map(d => ({
                                    date: fmtDate(d.date),
                                    cumulative: d.cumulative,
                                    daily_return: d.daily_return,
                                  }))}
                                >
                                  <XAxis dataKey="date" tick={{ fontSize: 8, fill: '#9ca3af' }} stroke="#6b7280" interval={3} />
                                  <YAxis tick={{ fontSize: 8, fill: '#9ca3af' }} stroke="#6b7280" tickFormatter={v => `${(v * 100).toFixed(0)}%`} />
                                  <Tooltip
                                    contentStyle={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', borderRadius: '6px', fontSize: 10 }}
                                    formatter={(val: unknown, name: unknown) => {
                                      const n = Number(val);
                                      if (isNaN(n)) return ['-'];
                                      const color = n >= 0 ? '#ef4444' : '#10b981';
                                      const label = String(name ?? '') === 'cumulative' ? '累计' : '当日';
                                      return [<span style={{ color }}>{`${(n * 100).toFixed(2)}%`}</span>, label];
                                    }}
                                  />
                                  <Line type="monotone" dataKey="cumulative" stroke={cumColor} strokeWidth={1.5} dot={false} />
                                  <Line type="monotone" dataKey="daily_return" stroke="#60a5fa" strokeWidth={1} dot={false} strokeDasharray="3 2" />
                                  <ReferenceLine y={0} stroke="#4b5563" strokeWidth={1} strokeDasharray="4 4" />
                                </LineChart>
                              </ResponsiveContainer>
                            </div>
                            );
                          })()}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </Card>
        )}

        {/* Empty state */}
        {!recommendData && !loadingData && (
          <EmptyState
            icon={<TrendingUp className="h-8 w-8" />}
            title="暂无券商金股数据"
            description="点击「获取当月数据」从 Tushare 抓取券商金股推荐"
          />
        )}
              </div>
            ),
          },
          {
            key: 'ytd',
            label: '年初至今',
            children: (
              <div className="space-y-4 pt-2">
        {/* YTD Loading */}
        {ytdLoading && (
          <Card className="p-4 text-center text-sm text-tertiary-text">
            <Loader2 className="h-4 w-4 animate-spin inline mr-2" />
            加载中...
          </Card>
        )}

        {/* YTD Overview */}
        {ytdData && !ytdLoading && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <Card className="p-3 text-center">
              <div className="text-lg font-bold">{ytdData.total_brokers}</div>
              <div className="text-xs text-secondary-text">券商总数</div>
            </Card>
            <Card className="p-3 text-center">
              <div className="text-lg font-bold">{ytdData.brokers.length}</div>
              <div className="text-xs text-secondary-text">Top 券商</div>
            </Card>
            <Card className="p-3 text-center">
              <div className={`text-lg font-bold ${(ytdData.brokers[0]?.cumulative_return ?? 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                {fmtPct(ytdData.brokers[0]?.cumulative_return)}
              </div>
              <div className="text-xs text-secondary-text">最优 YTD 收益</div>
            </Card>
            <Card className="p-3 text-center">
              <div className="text-lg font-bold text-sm">
                {fmtDate(ytdData.start_date).slice(0, 7)} ~ {fmtDate(ytdData.end_date).slice(5)}
              </div>
              <div className="text-xs text-secondary-text">回测区间</div>
            </Card>
          </div>
        )}

        {/* YTD Chart */}
        {ytdData && ytdChartData.length > 0 && !ytdLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-2">年初至今 Top 5 券商累计收益</div>
            <div className="flex flex-wrap gap-x-3 gap-y-1 mb-1">
              {ytdData.brokers.map((b, i) => (
                <div key={b.broker} className="inline-flex items-center gap-1 text-xs">
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{ backgroundColor: BROKER_COLORS[i % BROKER_COLORS.length] }}
                  />
                  <span className="text-secondary-text">{b.broker}</span>
                  <span className={`font-medium ${b.cumulative_return >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                    {fmtPct(b.cumulative_return)}
                  </span>
                </div>
              ))}
            </div>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={ytdChartData} margin={{ top: 4, right: 0, bottom: 6, left: -20 }}>
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#9ca3af' }} stroke="#6b7280" />
                <YAxis
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  stroke="#6b7280"
                  tickFormatter={v => `${(v * 100).toFixed(0)}%`}
                />
                <Tooltip content={<CustomTooltip />} />
                <ReferenceLine y={0} stroke="#4b5563" strokeWidth={1} strokeDasharray="4 4" />
                {ytdData.brokers.map((b, i) => (
                  <Line
                    key={b.broker}
                    type="monotone"
                    dataKey={String(b.broker)}
                    stroke={BROKER_COLORS[i % BROKER_COLORS.length]}
                    strokeWidth={1.5}
                    dot={false}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </Card>
        )}

        {/* YTD Broker Table */}
        {ytdData && !ytdLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">券商 YTD 表现</div>
            <Table
              dataSource={ytdData.brokers.map((b, i) => ({
                key: b.broker,
                rank: i + 1,
                broker: b.broker,
                cumulative_return: b.cumulative_return,
                active_months: b.active_months,
                monthly_returns: b.monthly_returns,
                colorIdx: i,
              }))}
              columns={[
                { title: '#', dataIndex: 'rank', key: 'rank', width: 40, render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span> },
                { title: '券商', dataIndex: 'broker', key: 'broker', render: (v: string, _: any, i: number) => (
                  <span className="inline-flex items-center gap-2 text-xs">
                    <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: BROKER_COLORS[i % BROKER_COLORS.length] }} />
                    {v}
                  </span>
                )},
                { title: 'YTD 累计收益', dataIndex: 'cumulative_return', key: 'cumulative_return', render: (v: number) => (
                  <span className={`text-xs font-medium ${v >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(v)}</span>
                )},
                { title: '活跃月份', dataIndex: 'active_months', key: 'active_months', render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span> },
              ]}
              size="small"
              pagination={false}
              expandable={{
                expandedRowRender: (record: any) => {
                  const monthly = record.monthly_returns || [];
                  if (!monthly.length) return <span className="text-xs text-tertiary-text">暂无月度明细</span>;
                  return (
                    <Table
                      dataSource={monthly.map((mr: any) => ({
                        key: mr.month,
                        month: mr.month,
                        cumulative_return: mr.cumulative_return,
                        stock_count: mr.stock_count,
                        win_rate: mr.win_rate,
                      }))}
                      columns={[
                        { title: '月份', dataIndex: 'month', key: 'month', width: 100, render: (v: string) => <span className="text-xs">{fmtDate(v).slice(0, 7)}</span> },
                        { title: '月收益', dataIndex: 'cumulative_return', key: 'cumulative_return', render: (v: number) => (
                          <span className={`text-xs font-medium ${v >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(v)}</span>
                        )},
                        { title: '推荐股数', dataIndex: 'stock_count', key: 'stock_count', render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span> },
                        { title: '胜率', dataIndex: 'win_rate', key: 'win_rate', render: (v: number) => (
                          <span className={`text-xs font-medium ${v >= 0.5 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(v)}</span>
                        )},
                      ]}
                      size="small"
                      pagination={false}
                      showHeader={false}
                    />
                  );
                },
              }}
            />
          </Card>
        )}

        {/* YTD Empty */}
        {!ytdData && !ytdLoading && (
          <EmptyState
            icon={<TrendingUp className="h-8 w-8" />}
            title="暂无年初至今数据"
            description="请先确保当前年份有月度金股数据"
          />
        )}
              </div>
            ),
          },
        ]}
      />
    </AppPage>
  );
};

export default BrokerRecommendPage;
