import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { TrendingUp, RefreshCw, ChevronDown, ChevronRight, Loader2 } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { AppPage, Button, Card, EmptyState } from '../components/common';
import {
  getAvailableMonths,
  getMonthlyRecommendations,
  fetchMonth,
  getBacktest,
  type BrokerRecommendResponse,
  type BrokerRecommendItem,
  type BrokerBacktestResponse,
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

// Group recommendations by broker
function groupByBroker(items: BrokerRecommendItem[]): Map<string, BrokerRecommendItem[]> {
  if (!items || items.length === 0) return new Map();
  const map = new Map<string, BrokerRecommendItem[]>();
  for (const item of items) {
    const existing = map.get(item.broker) || [];
    existing.push(item);
    map.set(item.broker, existing);
  }
  return map;
}

const BrokerRecommendPage: React.FC = () => {
  const [months, setMonths] = useState<string[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string>('');
  const [loadingMonths, setLoadingMonths] = useState(true);
  const [loadingData, setLoadingData] = useState(false);
  const [recommendData, setRecommendData] = useState<BrokerRecommendResponse | null>(null);
  const [backtestData, setBacktestData] = useState<BrokerBacktestResponse | null>(null);
  const [expandedBrokers, setExpandedBrokers] = useState<Set<string>>(new Set());
  const [viewMode, setViewMode] = useState<'broker' | 'stock'>('broker');
  const [sortKey, setSortKey] = useState<'cumRet' | 'brokerCount' | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const handleSort = (key: 'cumRet' | 'brokerCount') => {
    if (sortKey !== key) {
      setSortKey(key);
      setSortDir('desc');
    } else if (sortDir === 'desc') {
      setSortDir('asc');
    } else {
      setSortKey(null);
    }
  };

  const sortIndicator = (key: 'cumRet' | 'brokerCount'): string => {
    if (sortKey !== key) return '';
    return sortDir === 'desc' ? ' ↓' : ' ↑';
  };

  // Load available months on mount
  useEffect(() => {
    async function load() {
      try {
        const m = await getAvailableMonths();
        setMonths(m);
        if (m.length > 0) {
          setSelectedMonth(m[0]);
        } else {
          // No months in DB yet - set current month as default
          const now = new Date();
          const yyyyMM = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}`;
          setSelectedMonth(yyyyMM);
        }
      } catch (e) {
        console.error('Failed to load months:', e);
        // Still set current month on error
        const now = new Date();
        const yyyyMM = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}`;
        setSelectedMonth(yyyyMM);
      } finally {
        setLoadingMonths(false);
      }
    }
    load();
  }, []);

  // Auto-load recommendations when selectedMonth changes
  useEffect(() => {
    if (!selectedMonth) return;
    async function load() {
      setLoadingData(true);
      try {
        const data = await getMonthlyRecommendations(selectedMonth);
        setRecommendData(data);
        // Auto-trigger backtest
        const bt = await getBacktest(selectedMonth);
        setBacktestData(bt);
      } catch (e) {
        console.error('Failed to load:', e);
      } finally {
        setLoadingData(false);
      }
    }
    load();
  }, [selectedMonth]);

  const handleFetch = useCallback(async () => {
    if (!selectedMonth) return;
    setLoadingData(true);
    try {
      await fetchMonth(selectedMonth);
      const m = await getAvailableMonths();
      setMonths(m);
      if (!m.includes(selectedMonth)) {
        setSelectedMonth(m[0] || '');
      }
    } catch (e) {
      console.error('Failed to fetch:', e);
    } finally {
      setLoadingData(false);
    }
  }, [selectedMonth]);

  const toggleBroker = (broker: string) => {
    setExpandedBrokers(prev => {
      const next = new Set(prev);
      if (next.has(broker)) next.delete(broker);
      else next.add(broker);
      return next;
    });
  };

  // Prepare chart data: one line per broker
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

  const brokerGroups = (recommendData?.items?.length ?? 0) > 0 && recommendData?.items
    ? groupByBroker(recommendData.items)
    : new Map<string, BrokerRecommendItem[]>();

  return (
    <AppPage>
      <div className="space-y-4">
        {/* Controls */}
        <Card className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2">
              <label className="text-sm text-secondary-text">月份</label>
              {loadingMonths ? (
                <Loader2 className="h-4 w-4 animate-spin text-tertiary-text" />
              ) : months.length > 0 ? (
                <select
                  value={selectedMonth}
                  onChange={e => setSelectedMonth(e.target.value)}
                  className="h-9 rounded-lg border border-border/30 bg-muted/30 px-3 text-sm"
                >
                  {months.map(m => (
                    <option key={m} value={m}>{m}</option>
                  ))}
                </select>
              ) : (
                <span className="text-sm text-tertiary-text">暂无数据</span>
              )}
            </div>

            <Button
              variant="outline"
              size="sm"
              onClick={handleFetch}
              disabled={!selectedMonth || loadingData}
            >
              {loadingData ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <RefreshCw className="h-4 w-4 mr-1" />}
              获取当月数据
            </Button>

            <span className="text-xs text-tertiary-text ml-auto">
              {backtestData
                ? `回测区间: ${fmtDate(backtestData.buy_date)} → ${fmtDate(backtestData.sell_date)}`
                : recommendData
                ? `展示 ${selectedMonth} 月券商金股`
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
            <div className="text-sm font-medium mb-3">券商组合收益曲线</div>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={chartData}>
                <XAxis dataKey="date" tick={{ fontSize: 10 }} stroke="hsl(var(--border))" />
                <YAxis
                  tick={{ fontSize: 10 }}
                  stroke="hsl(var(--border))"
                  tickFormatter={v => `${(v * 100).toFixed(0)}%`}
                />
                <Tooltip
                  contentStyle={{
                    background: 'hsl(var(--card))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                    fontSize: '12px',
                  }}
                  formatter={(val: unknown) => {
                    const n = Number(val);
                    return isNaN(n) ? ['-'] : [`${(n * 100).toFixed(2)}%`];
                  }}
                />
                <Legend
                  wrapperStyle={{ fontSize: 10 }}
                  formatter={(value: string) => <span className="text-xs">{value}</span>}
                />
                {backtestData.brokers.map((b, i) => (
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

        {/* Broker table - always show when recommendData is available */}
        {recommendData && brokerGroups.size > 0 && !loadingData && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">
              {viewMode === 'broker' ? '券商金股明细' : '全部金股明细'}
            </div>

            {/* Stock view: flat table of unique stocks */}
            {viewMode === 'stock' && (
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border/10 text-secondary-text">
                    <th className="text-left py-1.5 font-medium">代码</th>
                    <th className="text-left py-1.5 font-medium">名称</th>
                    <th className="text-center py-1.5 font-medium w-16">
                      <button
                        onClick={() => handleSort('brokerCount')}
                        className="hover:text-foreground transition-colors inline-flex items-center gap-0.5"
                      >
                        推荐数<span className="w-3 text-xs">{sortIndicator('brokerCount')}</span>
                      </button>
                    </th>
                    <th className="text-right py-1.5 font-medium w-20">
                      <button
                        onClick={() => handleSort('cumRet')}
                        className="hover:text-foreground transition-colors inline-flex items-center gap-0.5"
                      >
                        累计收益<span className="w-3 text-xs">{sortIndicator('cumRet')}</span>
                      </button>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    // Deduplicate by ts_code, keep max broker_count
                    const stockMap = new Map<string, { item: BrokerRecommendItem; cumRet?: number }>();
                    for (const items of brokerGroups.values()) {
                      for (const item of items) {
                        const existing = stockMap.get(item.ts_code);
                        if (!existing || item.broker_count > existing.item.broker_count) {
                          const stockRet = backtestData?.stock_returns?.find(
                            s => s.ts_code === item.ts_code
                          );
                          const cumRet = stockRet?.daily_returns?.length
                            ? stockRet.daily_returns[stockRet.daily_returns.length - 1].cumulative
                            : undefined;
                          stockMap.set(item.ts_code, { item, cumRet });
                        }
                      }
                    }
                    const stocks = Array.from(stockMap.values());
                    if (sortKey) {
                      const dir = sortDir === 'desc' ? -1 : 1;
                      stocks.sort((a, b) => {
                        const va = sortKey === 'brokerCount' ? a.item.broker_count : (a.cumRet ?? -Infinity);
                        const vb = sortKey === 'brokerCount' ? b.item.broker_count : (b.cumRet ?? -Infinity);
                        return (va > vb ? -1 : va < vb ? 1 : 0) * dir;
                      });
                    } else {
                      stocks.sort((a, b) => (b.cumRet ?? -Infinity) - (a.cumRet ?? -Infinity));
                    }
                    return stocks
                      .map(({ item, cumRet }) => (
                        <tr key={item.ts_code} className="border-b border-border/5 hover:bg-foreground/[0.02]">
                          <td className="py-1.5 font-mono">{item.ts_code}</td>
                          <td className="py-1.5 text-secondary-text">{item.name}</td>
                          <td className="py-1.5 text-center text-tertiary-text">{item.broker_count}</td>
                          <td className={`py-1.5 text-right font-medium ${cumRet != null ? (cumRet >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-tertiary-text'}`}>
                            {fmtPct(cumRet)}
                          </td>
                        </tr>
                      ));
                  })()}
                </tbody>
              </table>
            )}

            {/* Broker view: grouped by broker */}
            {viewMode === 'broker' && (
            <div className="space-y-2">
              {Array.from(brokerGroups.entries()).map(([broker, items], idx) => (
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
                    <span className={`text-xs font-medium ${(backtestData?.brokers.find(b => b.broker === broker)?.cumulative_return ?? 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                      {fmtPct(backtestData?.brokers.find(b => b.broker === broker)?.cumulative_return)}
                    </span>
                    {backtestData && (
                      <span className="text-xs text-secondary-text">
                        胜率 {backtestData.brokers.find(b => b.broker === broker)?.win_rate != null
                          ? `${(backtestData.brokers.find(b => b.broker === broker)!.win_rate! * 100).toFixed(0)}%`
                          : '--'}
                      </span>
                    )}
                  </button>

                  {/* Expanded broker detail */}
                  {expandedBrokers.has(broker) && (
                    <div className="px-4 py-2 border-t border-border/10 bg-muted/20">
                      {/* Stock returns table */}
                      {backtestData ? (
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="border-b border-border/10 text-secondary-text">
                              <th className="text-left py-1.5 font-medium">代码</th>
                              <th className="text-left py-1.5 font-medium">名称</th>
                              <th className="text-center py-1.5 font-medium w-16">
                                <button
                                  onClick={() => handleSort('brokerCount')}
                                  className="hover:text-foreground transition-colors inline-flex items-center gap-0.5"
                                >
                                  推荐数<span className="w-3 text-xs">{sortIndicator('brokerCount')}</span>
                                </button>
                              </th>
                              <th className="text-right py-1.5 font-medium w-20">
                                <button
                                  onClick={() => handleSort('cumRet')}
                                  className="hover:text-foreground transition-colors inline-flex items-center gap-0.5"
                                >
                                  累计收益<span className="w-3 text-xs">{sortIndicator('cumRet')}</span>
                                </button>
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {items
                              .map((item: BrokerRecommendItem) => {
                                const stockRet = backtestData.stock_returns?.find(
                                  s => s.broker === broker && s.ts_code === item.ts_code
                                );
                                const cumRet = stockRet?.daily_returns?.length
                                  ? stockRet.daily_returns[stockRet.daily_returns.length - 1].cumulative
                                  : undefined;
                                return { item, cumRet };
                              })
                              .sort((a, b) => {
                                if (!sortKey) return (b.cumRet ?? -Infinity) - (a.cumRet ?? -Infinity);
                                const dir = sortDir === 'desc' ? -1 : 1;
                                const va = sortKey === 'brokerCount' ? a.item.broker_count : (a.cumRet ?? -Infinity);
                                const vb = sortKey === 'brokerCount' ? b.item.broker_count : (b.cumRet ?? -Infinity);
                                return (va > vb ? -1 : va < vb ? 1 : 0) * dir;
                              })
                              .map(({ item, cumRet }) => (
                                <tr key={item.ts_code} className="border-b border-border/5 hover:bg-foreground/[0.02]">
                                  <td className="py-1.5 font-mono">{item.ts_code}</td>
                                  <td className="py-1.5 text-secondary-text">{item.name}</td>
                                  <td className="py-1.5 text-center text-tertiary-text">{item.broker_count}</td>
                                  <td className={`py-1.5 text-right font-medium ${cumRet != null ? (cumRet >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-tertiary-text'}`}>
                                    {fmtPct(cumRet)}
                                  </td>
                                </tr>
                              ))}
                          </tbody>
                        </table>
                      ) : (
                        <div className="space-y-1">
                          {items.map((item: BrokerRecommendItem) => (
                            <div key={item.ts_code} className="flex items-center gap-2 text-xs">
                              <span className="font-mono">{item.ts_code}</span>
                              <span className="text-secondary-text">{item.name}</span>
                              <span className="text-tertiary-text ml-auto">被{item.broker_count}家推荐</span>
                            </div>
                          ))}
                        </div>
                      )}
                      {/* Mini chart for this broker */}
                      {backtestData && (() => {
                        const brokerBt = backtestData.brokers.find(b => b.broker === broker);
                        if (!brokerBt || brokerBt.daily_returns.length === 0) return null;
                        return (
                          <div className="mt-2">
                            <ResponsiveContainer width="100%" height={80}>
                              <LineChart
                                data={brokerBt.daily_returns.map(d => ({
                                  date: fmtDate(d.date),
                                  cumulative: d.cumulative,
                                }))}
                              >
                                <XAxis dataKey="date" tick={{ fontSize: 8 }} stroke="hsl(var(--border))" />
                                <YAxis tick={{ fontSize: 8 }} stroke="hsl(var(--border))" tickFormatter={v => `${(v * 100).toFixed(0)}%`} />
                                <Tooltip
                                  contentStyle={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', borderRadius: '6px', fontSize: 10 }}
                                  formatter={(val: unknown) => {
                                    const n = Number(val);
                                    return isNaN(n) ? ['-'] : [`${(n * 100).toFixed(2)}%`];
                                  }}
                                />
                                <Line
                                  type="monotone"
                                  dataKey="cumulative"
                                  stroke={BROKER_COLORS[idx % BROKER_COLORS.length]}
                                  strokeWidth={1.5}
                                  dot={false}
                                />
                              </LineChart>
                            </ResponsiveContainer>
                          </div>
                        );
                      })()}
                    </div>
                  )}
                </div>
              ))}
            </div>
            )}
          </Card>
        )}

        {/* Empty state */}
        {!recommendData && !loadingData && !loadingMonths && (
          <EmptyState
            icon={<TrendingUp className="h-8 w-8" />}
            title="暂无券商金股数据"
            description="点击「获取当月数据」从 Tushare 抓取券商金股推荐"
          />
        )}
      </div>
    </AppPage>
  );
};

export default BrokerRecommendPage;
