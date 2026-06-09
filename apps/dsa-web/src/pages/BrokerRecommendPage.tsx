import type React from 'react';
import { useState, useEffect, useLayoutEffect, useCallback, useMemo, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import dayjs, { type Dayjs } from 'dayjs';
import { DatePicker, Table, Tabs, Tooltip as AntTooltip } from 'antd';

const { RangePicker } = DatePicker;
import zhCN from 'antd/locale/zh_CN';
import type { ColumnsType } from 'antd/es/table';
import { TrendingUp, RefreshCw, ChevronDown, ChevronRight, Loader2, CheckSquare, Square, ChevronUp } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { AppPage, Button, Card, EmptyState } from '../components/common';

/** SVG candlestick chart for monthly stock trend.
 *  Shows OHLC candles with 5-day moving average overlay. */
const CandlestickMiniChart: React.FC<{
  data: Array<{ date: string; price?: number | null; open?: number | null; high?: number | null; low?: number | null }>;
  height?: number;
  /** 长区间（如近 6 个月）：K 线横向撑满容器宽度 */
  longSeriesScroll?: boolean;
  barPitch?: number;
}> = ({ data, height = 160, longSeriesScroll = false, barPitch }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerW, setContainerW] = useState(0);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const validData = data.filter(d => d.price != null);
  const pads = { t: 14, r: 6, b: 20, l: 42 };
  const count = validData.length;
  const pitch = barPitch ?? 14;
  const fallbackW = count > 0
    ? Math.max(count * pitch + pads.l + pads.r, pads.l + pads.r + 40)
    : pads.l + pads.r + 40;

  useLayoutEffect(() => {
    if (!longSeriesScroll) return;
    const el = containerRef.current;
    if (!el) return;
    const apply = () => {
      const w = Math.floor(el.clientWidth);
      if (w > 0) setContainerW(w);
    };
    apply();
    const ro = new ResizeObserver(() => apply());
    ro.observe(el);
    return () => ro.disconnect();
  }, [longSeriesScroll, count]);

  if (count < 1) return null;

  const chartW = longSeriesScroll && containerW > 0 ? containerW : fallbackW;

  const chartH = height - pads.t - pads.b;
  const xStep = (chartW - pads.l - pads.r) / Math.max(count - 1, 1);

  const allPrices: number[] = [];
  validData.forEach(d => {
    if (d.high != null) allPrices.push(d.high);
    if (d.low != null) allPrices.push(d.low);
    allPrices.push(d.price!);
    if (d.open != null) allPrices.push(d.open);
  });
  const priceMin = Math.min(...allPrices);
  const priceMax = Math.max(...allPrices);
  const margin = (priceMax - priceMin) * 0.08 || priceMin * 0.02 || 0.1;
  const yMin = priceMin - margin;
  const yMax = priceMax + margin;
  const yRange = yMax - yMin || 1;
  const scaleY = (p: number) => pads.t + chartH * (1 - (p - yMin) / yRange);

  const candleW = Math.max(Math.min(xStep * 0.65, 10), 2.5);

  // 5-day simple moving average
  const sma5: Array<{ x: number; y: number }> = [];
  for (let i = 4; i < count; i++) {
    let sum = 0;
    for (let j = i - 4; j <= i; j++) sum += validData[j].price!;
    const avg = sum / 5;
    const x = pads.l + i * xStep;
    sma5.push({ x, y: scaleY(avg) });
  }

  const gridLines = 4;
  const yTicks: number[] = [];
  for (let i = 1; i < gridLines; i++) {
    yTicks.push(yMax - (yRange * i) / gridLines);
  }

  const xTickInterval = Math.max(Math.ceil(count / (longSeriesScroll ? 8 : 5)), 1);

  const svgEl = (
      <svg
        width={chartW}
        height={height}
        viewBox={`0 0 ${chartW} ${height}`}
        style={
          longSeriesScroll
            ? { display: 'block', width: '100%', height }
            : { display: 'block', minWidth: '100%' }
        }
      >
        <defs>
          <filter id="tipShadow" x="-10%" y="-10%" width="130%" height="130%">
            <feDropShadow dx={0} dy={1} stdDeviation={2} floodColor="#000" floodOpacity={0.4} />
          </filter>
        </defs>
        {/* Grid */}
        {yTicks.map((price, i) => {
          const y = scaleY(price);
          return (
            <g key={`g-${i}`}>
              <line x1={pads.l} x2={chartW - pads.r} y1={y} y2={y}
                stroke="#1f2937" strokeWidth={0.8} />
              <text x={pads.l - 3} y={y + 3.5} textAnchor="end" fill="#6b7280"
                fontSize={9} fontFamily="monospace">{price.toFixed(2)}</text>
            </g>
          );
        })}
        {/* Top & bottom price labels */}
        <text x={pads.l - 3} y={pads.t + 3.5} textAnchor="end" fill="#6b7280"
          fontSize={9} fontFamily="monospace">{yMax.toFixed(2)}</text>
        <text x={pads.l - 3} y={pads.t + chartH + 3.5} textAnchor="end" fill="#6b7280"
          fontSize={9} fontFamily="monospace">{yMin.toFixed(2)}</text>

        {/* MA5 line */}
        {sma5.length > 1 && (
          <polyline
            points={sma5.map(p => `${p.x},${p.y}`).join(' ')}
            fill="none" stroke="#f59e0b" strokeWidth={1.2}
            strokeDasharray="3 2" opacity={0.8}
          />
        )}

        {/* Candles */}
        {validData.map((d, i) => {
          const x = pads.l + i * xStep;
          const closeP = d.price!;
          const hasOhlc = d.open != null && d.high != null && d.low != null;
          const isUp = hasOhlc ? closeP >= d.open! : true;
          const color = isUp ? '#ef4444' : '#10b981';
          const bodyTop = hasOhlc ? scaleY(Math.max(d.open!, closeP)) : scaleY(closeP) - 1.5;
          const bodyBot = hasOhlc ? scaleY(Math.min(d.open!, closeP)) : scaleY(closeP) + 1.5;
          const bodyH = Math.max(bodyBot - bodyTop, 1);
          return (
            <g key={i} onMouseEnter={() => setHoverIdx(i)} onMouseLeave={() => setHoverIdx(null)}
              style={{ cursor: 'crosshair' }}>
              {/* Wick */}
              {hasOhlc && (
                <line x1={x} x2={x} y1={scaleY(d.high!)} y2={scaleY(d.low!)}
                  stroke={color} strokeWidth={1} />
              )}
              {/* Body */}
              <rect x={x - candleW / 2} y={bodyTop} width={candleW} height={bodyH} rx={0.8}
                fill={isUp ? color : (hasOhlc ? '#0f1723' : color)} stroke={color} strokeWidth={1}
                opacity={hasOhlc && !isUp ? 0.9 : 1} />
              {/* Hover indicator */}
              {hoverIdx === i && (
                <line x1={x} x2={x} y1={pads.t} y2={pads.t + chartH}
                  stroke="#e2e8f0" strokeWidth={0.8} strokeDasharray="2 3" opacity={0.5} />
              )}
            </g>
          );
        })}

        {/* X-axis labels */}
        {validData.map((d, i) => {
          if (i % xTickInterval !== 0 && i !== count - 1) return null;
          const x = pads.l + i * xStep;
          const label = d.date.length >= 8 ? `${d.date.slice(4,6)}/${d.date.slice(6,8)}` : d.date;
          return (
            <text key={`xl-${i}`} x={x} y={height - 3} textAnchor="middle" fill="#6b7280"
              fontSize={9} fontFamily="monospace">{label}</text>
          );
        })}
        {/* Tooltip — SVG-native, perfectly aligned */}
        {hoverIdx != null && validData[hoverIdx] && (() => {
          const d = validData[hoverIdx];
          const hasOhlc = d.open != null && d.high != null && d.low != null;
          const isUp = hasOhlc ? d.price! >= d.open! : true;
          const chgColor = isUp ? '#ef4444' : '#10b981';
          const chg = hasOhlc && d.open! > 0 ? ((d.price! - d.open!) / d.open! * 100) : null;
          const cx = pads.l + hoverIdx * xStep;
          const tipW = 105, tipH = hasOhlc ? 52 : 30;
          const tipX = cx + tipW + 6 > chartW - pads.r ? cx - tipW - 6 : cx + 6;
          const tipY = pads.t;
          const dateStr = d.date.length >= 8 ? `${d.date.slice(0,4)}-${d.date.slice(4,6)}-${d.date.slice(6,8)}` : d.date;
          return (
            <g pointerEvents="none">
              <rect x={tipX} y={tipY} width={tipW} height={tipH} rx={4}
                fill="#111827" stroke="#374151" strokeWidth={0.8}
                filter="url(#tipShadow)" />
              <text x={tipX + 4} y={tipY + 12} fill="#9ca3af" fontSize={9} fontFamily="monospace">{dateStr}</text>
              {hasOhlc ? (
                <>
                  <text x={tipX + 4} y={tipY + 25} fill="#9ca3af" fontSize={9} fontFamily="monospace">
                    O {d.open!.toFixed(2)}  </text>
                  <text x={tipX + 60} y={tipY + 25} fill={chgColor} fontSize={9} fontFamily="monospace">
                    C {d.price!.toFixed(2)}</text>
                  <text x={tipX + 4} y={tipY + 38} fill="#9ca3af" fontSize={9} fontFamily="monospace">
                    H {d.high!.toFixed(2)}  </text>
                  <text x={tipX + 60} y={tipY + 38} fill="#9ca3af" fontSize={9} fontFamily="monospace">
                    L {d.low!.toFixed(2)}</text>
                  {chg != null && (
                    <text x={tipX + 4} y={tipY + 50} fill={chgColor} fontSize={9} fontFamily="monospace"
                      fontWeight="bold">
                      {chg >= 0 ? '+' : ''}{chg.toFixed(2)}%
                    </text>
                  )}
                </>
              ) : (
                <text x={tipX + 4} y={tipY + 22} fill="#e2e8f0" fontSize={9} fontFamily="monospace">
                  收盘: {d.price!.toFixed(2)}</text>
              )}
            </g>
          );
        })()}
      </svg>
  );

  if (longSeriesScroll) {
    return (
      <div ref={containerRef} className="w-full max-w-full min-w-0" style={{ height }}>
        {containerW > 0 ? svgEl : null}
      </div>
    );
  }

  return (
    <div className="w-full overflow-x-auto">
      {svgEl}
    </div>
  );
};
import {
  getMonthlyRecommendations,
  fetchMonth,
  getBacktest,
  getMonthlyEnrichment,
  getYtdBacktest,
  getConsecutiveStocks,
  getTopBrokers,
  getStockHistory,
  getHistoricalRecommendStats,
  getEqualWeightStrategy,
  getMonthlyUpToDownDaily,
  type HistoricalRecommendStatsItem,
  type UpToDownDailyResponse,
  type UpToDownDailyStockItem,
  type BrokerRecommendResponse,
  type StockHistoryResponse,
  type StockHistoryEntry,
  type BrokerDailyReturn,
  type BrokerRecommendItem,
  type BrokerBacktestResponse,
  type EnrichmentResponse,
  type YtdBacktestResponse,
  type ConsecutiveStockItem,
  type EqualWeightStrategyResponse,
} from '../api/brokerRecommend';
import { stocksApi } from '../api/stocks';

/** 当前月：历史推荐月份数最多的股票行数（红色高亮） */
const HISTORY_RECOMMEND_TOP_N = 10;

const UP_TO_DOWN_STRUCK_STORAGE_KEY = 'broker_recommend_up_to_down_struck';

function loadUpToDownStruckSet(): Set<string> {
  try {
    const raw = localStorage.getItem(UP_TO_DOWN_STRUCK_STORAGE_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw) as string[];
    return new Set(Array.isArray(parsed) ? parsed : []);
  } catch {
    return new Set();
  }
}

function saveUpToDownStruckSet(keys: Set<string>): void {
  try {
    localStorage.setItem(UP_TO_DOWN_STRUCK_STORAGE_KEY, JSON.stringify([...keys]));
  } catch {
    /* ignore quota errors */
  }
}

function upToDownStruckRowKey(
  month: string,
  signalDate: string,
  tsCode: string,
  signalType: string,
): string {
  return `${month}:${signalDate}:${tsCode}:${signalType}`;
}

function formatReversalSignalLabel(row: UpToDownDailyStockItem): string {
  if (row.signal_type === 'down_to_up') {
    return `降${row.prev_nineturn_down_count ?? 0}升`;
  }
  return `升${row.prev_nineturn_up_count}转降`;
}

type ReversalSignalTableProps = {
  title: string;
  titleClassName: string;
  signalType: 'up_to_down' | 'down_to_up';
  signalDate: string;
  monthStr: string;
  stocks: UpToDownDailyStockItem[];
  struckSet: Set<string>;
  emptyText: string;
  onToggleStruck: (signalDate: string, tsCode: string, signalType: string) => void;
  onFocusStock: (tsCode: string) => void;
};

const ReversalSignalTable: React.FC<ReversalSignalTableProps> = ({
  title,
  titleClassName,
  signalType,
  signalDate,
  monthStr,
  stocks,
  struckSet,
  emptyText,
  onToggleStruck,
  onFocusStock,
}) => (
  <div className="min-w-0">
    <div className={`text-xs font-medium mb-2 ${titleClassName}`}>{title}</div>
    {stocks.length ? (
      <Table
        size="small"
        pagination={false}
        tableLayout="fixed"
        rowKey="ts_code"
        dataSource={stocks}
        onRow={(record) => {
          const struck = struckSet.has(
            upToDownStruckRowKey(monthStr, signalDate, record.ts_code, signalType),
          );
          return struck
            ? { className: '[&_td:not(:first-child)]:line-through [&_td:not(:first-child)]:opacity-50' }
            : {};
        }}
        columns={[
          {
            title: '',
            key: 'struck',
            width: 36,
            render: (_: unknown, row: UpToDownDailyStockItem) => {
              const struck = struckSet.has(
                upToDownStruckRowKey(monthStr, signalDate, row.ts_code, signalType),
              );
              return (
                <button
                  type="button"
                  aria-label={struck ? '取消划线' : '划线标记'}
                  className="flex items-center justify-center text-tertiary-text hover:text-secondary-text cursor-pointer"
                  onClick={() => onToggleStruck(signalDate, row.ts_code, signalType)}
                >
                  {struck
                    ? <CheckSquare className="h-4 w-4 text-cyan/80" />
                    : <Square className="h-4 w-4" />}
                </button>
              );
            },
          },
          {
            title: '股票',
            dataIndex: 'name',
            width: '28%',
            ellipsis: true,
            render: (name: string, row: UpToDownDailyStockItem) => (
              <button
                type="button"
                className="max-w-full truncate text-left text-xs font-medium text-cyan hover:underline cursor-pointer"
                onClick={() => onFocusStock(row.ts_code)}
              >
                {name || row.ts_code}
              </button>
            ),
          },
          {
            title: '代码',
            dataIndex: 'ts_code',
            width: '28%',
            ellipsis: true,
            render: (code: string) => (
              <span className="text-xs font-mono text-secondary-text">{code}</span>
            ),
          },
          {
            title: '信号',
            key: 'signal',
            width: '24%',
            ellipsis: true,
            render: (_: unknown, row: UpToDownDailyStockItem) => (
              <span className={titleClassName}>{formatReversalSignalLabel(row)}</span>
            ),
          },
          {
            title: '所属行业',
            dataIndex: 'sector',
            width: '20%',
            ellipsis: true,
            render: (sector: string | null | undefined) => (
              <span className="text-xs text-secondary-text">{sector || '--'}</span>
            ),
          },
        ]}
      />
    ) : (
      <div className="text-xs text-tertiary-text py-2">{emptyText}</div>
    )}
  </div>
);

const BROKER_COLORS = [
  '#34d399', '#60a5fa', '#f472b6', '#fbbf24', '#a78bfa',
  '#fb923c', '#2dd4bf', '#e879f9', '#facc15', '#38bdf8',
];

function fmtDate(s: string): string {
  if (!s || s.length < 8) return s;
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}




function HistoryStatsCell({ row }: { row: StockRow }) {
  if (!row.historyPeriodCount) {
    return <span className="text-xs text-tertiary-text">--</span>;
  }
  const winColor = row.historyWinRate != null && row.historyWinRate >= 0.5 ? 'text-red-400' : 'text-emerald-400';
  const maxRetColor = row.historyMaxReturn != null && row.historyMaxReturn >= 0 ? 'text-red-400' : 'text-emerald-400';
  const minRetColor = row.historyMaxDrawdown != null && row.historyMaxDrawdown >= 0 ? 'text-red-400' : 'text-emerald-400';
  return (
    <div className="text-[10px] leading-relaxed space-y-0.5 tabular-nums">
      <div>
        <span className="text-tertiary-text">胜率 </span>
        <span className={winColor}>
          {row.historyWinRate != null ? `${(row.historyWinRate * 100).toFixed(0)}%` : '--'}
        </span>
        <span className="text-tertiary-text ml-1">({row.historyPeriodCount}期)</span>
      </div>
      <div>
        <span className="text-tertiary-text">最高 </span>
        <span className={maxRetColor}>{fmtPct(row.historyMaxReturn)}</span>
      </div>
      <div>
        <span className="text-tertiary-text">最低 </span>
        <span className={minRetColor}>{fmtPct(row.historyMaxDrawdown)}</span>
      </div>
    </div>
  );
}

function StockTagsCell({ row }: { row: StockRow }) {
  const tags: React.ReactNode[] = [];
  if (row.isHistoryTop) {
    tags.push(
      <span key="hist" className="px-1 py-0.5 text-[10px] bg-red-500/20 text-red-400 rounded font-medium">
        历史推荐{row.historyMonthCount != null ? `×${row.historyMonthCount}` : ''}
      </span>,
    );
  }
  if (row.isConsecutive) {
    tags.push(
      <span key="cons" className="px-1 py-0.5 text-[10px] bg-amber-500/15 text-amber-400 rounded">连续</span>,
    );
  }
  if (tags.length === 0) {
    return <span className="text-xs text-tertiary-text">--</span>;
  }
  return <div className="flex flex-col gap-0.5 items-start">{tags}</div>;
}

function fmtPct(v?: number | null): string {
  if (v == null) return '--';
  return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`;
}

type StrategyMonthlyStock = {
  month_return?: number | null;
  buy_amount?: number | null;
  sell_amount?: number | null;
  buy_reason?: {
    prev_nineturn_up_count?: number;
  };
};

function legPnl(s: StrategyMonthlyStock): number | null {
  const buyAmt = s.buy_amount;
  const sellAmt = s.sell_amount;
  if (buyAmt != null && sellAmt != null) {
    return sellAmt - buyAmt;
  }
  const ret = s.month_return;
  if (ret != null && buyAmt != null && buyAmt > 0) {
    return ret * buyAmt;
  }
  return ret ?? null;
}

type BestUpToDownSignal = {
  label: string;
  returnPct: number;
};

function bestUpToDownSignal(stocks?: StrategyMonthlyStock[]): BestUpToDownSignal | null {
  if (!stocks?.length) return null;
  const bucketPnl = new Map<number, number>();
  const bucketInvested = new Map<number, number>();
  for (const s of stocks) {
    const up = s.buy_reason?.prev_nineturn_up_count;
    const pnl = legPnl(s);
    if (up == null || up < 1 || pnl == null) continue;
    bucketPnl.set(up, (bucketPnl.get(up) ?? 0) + pnl);
    const buyAmt = s.buy_amount;
    if (buyAmt != null && buyAmt > 0) {
      bucketInvested.set(up, (bucketInvested.get(up) ?? 0) + buyAmt);
    }
  }
  let bestUp: number | null = null;
  let bestPnl = -Infinity;
  for (const [up, pnl] of bucketPnl) {
    if (pnl > bestPnl) {
      bestPnl = pnl;
      bestUp = up;
    }
  }
  if (bestUp == null) return null;
  const invested = bucketInvested.get(bestUp) ?? 0;
  const returnPct = invested > 0 ? bestPnl / invested : bestPnl;
  return {
    label: `升${bestUp}转降`,
    returnPct,
  };
}

type StrategyTradeReason = {
  summary?: string;
  nineturn_up_count?: number;
  nineturn_down_count?: number;
  prev_nineturn_up_count?: number;
  trigger?: string;
};

function StrategyTradeReasonTooltip({
  label,
  reason,
}: {
  label: string;
  reason?: StrategyTradeReason | null;
}) {
  if (!reason?.summary) {
    return <span className="text-xs text-tertiary-text">{label}</span>;
  }
  const ntParts: string[] = [];
  if (reason.nineturn_up_count) ntParts.push(`上升↑${reason.nineturn_up_count}`);
  if (reason.nineturn_down_count) ntParts.push(`下降↓${reason.nineturn_down_count}`);
  const ntText = ntParts.length ? ntParts.join('、') : '无计数';
  return (
    <AntTooltip
      title={(
        <div className="text-xs space-y-1.5 max-w-xs">
          <div className="font-medium">{reason.summary ?? label}</div>
          <div>九转信号：{ntText}</div>
        </div>
      )}
    >
      <span className="text-xs text-tertiary-text cursor-pointer border-b border-dotted border-secondary-text/40">
        {label}
      </span>
    </AntTooltip>
  );
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
  isConsecutive?: boolean;
  dailyChange?: number | null;
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
    scr90?: number | null;
  } | null;
  sector?: string | null;
  isTopPick?: boolean;
  isHistoryTop?: boolean;
  historyMonthCount?: number;
  historyWinRate?: number | null;
  historyMaxReturn?: number | null;
  historyMaxDrawdown?: number | null;
  historyPeriodCount?: number;
};

function brokerStockRowStyle(record: StockRow): React.CSSProperties | undefined {
  if (record.isTopPick) {
    return { background: 'linear-gradient(90deg, rgba(251,191,36,0.08) 0%, rgba(245,158,11,0.04) 100%)' };
  }
  return undefined;
}



const stockHistoryCache = new Map<string, StockHistoryResponse>();


function sanitizeChartBars(drs: BrokerDailyReturn[]): BrokerDailyReturn[] {
  return drs
    .filter(d => Boolean(d.date) && d.price != null)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function pickCumulativeReturn(drs: BrokerDailyReturn[]): number | null | undefined {
  if (!drs.length) return null;
  const withCum = drs.filter(d => d.cumulative != null);
  if (withCum.length) return withCum[withCum.length - 1].cumulative;
  const p0 = drs[0].price;
  const p1 = drs[drs.length - 1].price;
  if (p0 != null && p1 != null && p0 > 0) return (p1 - p0) / p0;
  return null;
}


const PRE_MONTH_KLINE_COUNT = 6;

function normTradeDate(s: string): string {
  return (s || '').replace(/\D/g, '').slice(0, 8);
}

type PreKlineBar = { date: string; price: number; open: number; high: number; low: number };

function filterPreKlineBars(apiBars: PreKlineBar[], window: { start: string; end: string }): PreKlineBar[] {
  return apiBars
    .filter(d => d.date.length === 8 && d.date >= window.start && d.date <= window.end)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function supplementPreKlineTail(
  bars: PreKlineBar[],
  tail: BrokerDailyReturn[] | undefined,
  window: { start: string; end: string },
): PreKlineBar[] {
  if (!tail?.length) return bars;
  // 连续日 K 不足时不用持仓期顶替整段（否则只剩当月）
  if (bars.length < minPreKlineBars(window)) return bars;
  const map = new Map(bars.map(b => [b.date, b]));
  const lastDate = bars.length ? bars[bars.length - 1].date : '';
  for (const d of tail) {
    const date = normTradeDate(d.date);
    if (date.length !== 8 || date < window.start || date > window.end) continue;
    if (lastDate && date <= lastDate) continue;
    if (d.price == null || Number.isNaN(d.price)) continue;
    map.set(date, {
      date,
      price: d.price,
      open: d.open ?? d.price,
      high: d.high ?? d.price,
      low: d.low ?? d.price,
    });
  }
  return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date));
}

function parseBrokerMonth(month: string): Dayjs {
  const digits = (month || '').replace(/\D/g, '');
  if (digits.length >= 6) {
    return dayjs(`${digits.slice(0, 4)}-${digits.slice(4, 6)}-01`);
  }
  return dayjs().startOf('month');
}

function minPreKlineBars(window: { start: string; end: string }): number {
  const span = dayjs(window.end, 'YYYYMMDD').diff(dayjs(window.start, 'YYYYMMDD'), 'day') + 1;
  return Math.max(20, Math.floor(span * 0.28));
}

function preSixMonthWindow(highlightMonth: string): { start: string; end: string; label: string } {
  const anchor = parseBrokerMonth(highlightMonth);
  const now = dayjs();
  // 含所选月：共 6 个自然月，截止所选月最新交易日（当前月则为今天）
  const start = anchor.subtract(PRE_MONTH_KLINE_COUNT - 1, 'month').startOf('month');
  const end = anchor.isSame(now, 'month') ? now : anchor.endOf('month');
  const labelEnd = anchor.isSame(now, 'month') ? '至今' : end.format('YYYY-MM');
  return {
    start: start.format('YYYYMMDD'),
    end: end.format('YYYYMMDD'),
    label: `${start.format('YYYY-MM')} ~ ${labelEnd}`,
  };
}

function fmtMonthLabel(month: string): string {
  if (!month || month.length < 6) return month;
  return `${month.slice(0, 4)}-${month.slice(4, 6)}`;
}

function StockHistoryExpandPanel({
  tsCode,
  name,
  highlightMonth,
}: {
  tsCode: string;
  name: string;
  highlightMonth: string;
}) {
  const [data, setData] = useState<StockHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAllMonths, setShowAllMonths] = useState(false);
  const [preKlineBase, setPreKlineBase] = useState<
    Array<{ date: string; price: number; open: number; high: number; low: number }>
  >([]);
  const [preKlineLoading, setPreKlineLoading] = useState(true);
  const [preKlineError, setPreKlineError] = useState<string | null>(null);
  const preWindow = useMemo(() => preSixMonthWindow(highlightMonth), [highlightMonth]);
  const preKlineMinBars = useMemo(() => minPreKlineBars(preWindow), [preWindow]);

  const preKlineBars = useMemo(() => {
    const base = filterPreKlineBars(preKlineBase, preWindow);
    const highlightEntry = data?.entries?.find(e => e.month === highlightMonth);
    return supplementPreKlineTail(base, highlightEntry?.daily_returns, preWindow);
  }, [preKlineBase, data, highlightMonth, preWindow]);

  useEffect(() => {
    let cancelled = false;
    const cached = stockHistoryCache.get(tsCode);
    if (cached) {
      setData(cached);
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    getStockHistory(tsCode)
      .then(resp => {
        if (cancelled) return;
        stockHistoryCache.set(tsCode, resp);
        setData(resp);
      })
      .catch(() => {
        if (!cancelled) setError('加载历史推荐走势失败，请稍后重试');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [tsCode]);

  useEffect(() => {
    let cancelled = false;
    setPreKlineLoading(true);
    setPreKlineError(null);
    setPreKlineBase([]);

    const load = async (attempt: number) => {
      const calendarSpan = dayjs(preWindow.end, 'YYYYMMDD').diff(dayjs(preWindow.start, 'YYYYMMDD'), 'day') + 1;
      const days = Math.min(730, Math.max(150, Math.ceil(calendarSpan * 1.6) + 30));
      try {
        const klines = await stocksApi.getHistory(tsCode, days, {
          startDate: preWindow.start,
          endDate: preWindow.end,
        });
        if (cancelled) return;
        const bars = filterPreKlineBars(
          klines
            .map(k => ({
              date: normTradeDate(k.date),
              price: k.close,
              open: k.open,
              high: k.high,
              low: k.low,
            }))
            .filter(d => d.price != null && !Number.isNaN(d.price)),
          preWindow,
        );
        if (bars.length >= preKlineMinBars || attempt >= 1) {
          setPreKlineBase(bars);
          if (bars.length < preKlineMinBars) {
            setPreKlineError(`仅加载到 ${bars.length} 个交易日，少于近 6 个月预期`);
          }
          return;
        }
        await load(1);
      } catch {
        if (cancelled) return;
        if (attempt < 1) {
          await load(1);
          return;
        }
        setPreKlineBase([]);
        setPreKlineError('加载近 6 个月日 K 失败，请稍后重试');
      }
    };

    void load(0).finally(() => {
      if (!cancelled) setPreKlineLoading(false);
    });
    return () => { cancelled = true; };
  }, [tsCode, highlightMonth, preWindow.start, preWindow.end, preKlineMinBars]);

  useEffect(() => {
    setShowAllMonths(false);
  }, [highlightMonth, tsCode]);

  const entries = useMemo((): StockHistoryEntry[] => {
    if (!data?.entries?.length) return [];
    const normalized = data.entries
      .filter(e => e.month <= highlightMonth)
      .map(e => {
        const bars = sanitizeChartBars(e.daily_returns ?? []);
        return {
          ...e,
          daily_returns: bars,
          cumulative_return: e.cumulative_return ?? pickCumulativeReturn(bars),
          buy_date: e.buy_date || bars[0]?.date || e.buy_date,
          sell_date: e.sell_date || bars[bars.length - 1]?.date || e.sell_date,
        };
      });
    return normalized.sort((a, b) => {
      if (a.month === highlightMonth) return -1;
      if (b.month === highlightMonth) return 1;
      return b.month.localeCompare(a.month);
    });
  }, [data, highlightMonth]);

  const visibleEntries = useMemo(() => {
    if (showAllMonths) return entries;
    const current = entries.find(e => e.month === highlightMonth);
    return current ? [current] : [];
  }, [entries, showAllMonths, highlightMonth]);

  if (loading) {
    return (
      <div className="p-4 flex items-center justify-center gap-2 text-xs text-secondary-text">
        <Loader2 className="h-4 w-4 animate-spin" />
        加载历史推荐 K 线…
      </div>
    );
  }
  if (error) {
    return (
      <div className="p-3 border border-border/20 rounded-lg bg-muted/10 text-xs text-secondary-text">{error}</div>
    );
  }
  if (visibleEntries.length === 0) {
    return (
      <div className="p-3 border border-border/20 rounded-lg bg-muted/10 text-xs text-secondary-text">
        {name || tsCode} — {fmtMonthLabel(highlightMonth)} 暂无持仓期 K 线（与表格累计收益区间一致）
      </div>
    );
  }
  const displayName = data?.name || name || tsCode;
  const otherCount = entries.length - (entries.some(e => e.month === highlightMonth) ? 1 : 0);

  return (
    <div className="p-3 border border-border/20 rounded-lg bg-muted/10 space-y-3 min-w-0 max-w-full overflow-hidden">
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="font-medium text-secondary-text">
          {displayName} · {fmtMonthLabel(highlightMonth)} 持仓期 K 线
        </span>
        {entries.length > 1 && (
          <button
            type="button"
            className="text-cyan-400 hover:text-cyan-300 cursor-pointer"
            onClick={() => setShowAllMonths(v => !v)}
          >
            {showAllMonths ? '仅看当前月' : `查看全部推荐月 (${entries.length})`}
          </button>
        )}
        {!showAllMonths && otherCount > 0 && (
          <span className="text-tertiary-text">另有 {otherCount} 个历史推荐月可展开</span>
        )}
      </div>
      <div className="rounded-lg border border-border/10 p-2">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs mb-2">
          <span className="font-medium text-secondary-text">近 6 个月行情</span>
          <span className="text-tertiary-text font-mono">{preWindow.label}</span>
          <span className="text-tertiary-text">（连续日 K，非持仓片段）</span>
        </div>
        {preKlineLoading ? (
          <div className="flex items-center gap-2 py-6 text-xs text-tertiary-text">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            加载近 6 个月 K 线…
          </div>
        ) : preKlineBars.length >= preKlineMinBars ? (
          <div className="min-w-0 max-w-full overflow-hidden">
            <CandlestickMiniChart data={preKlineBars} height={160} longSeriesScroll />
            {preKlineError ? (
              <div className="text-[10px] text-amber-400/90 mt-1">{preKlineError}</div>
            ) : null}
          </div>
        ) : (
          <div className="text-xs text-tertiary-text py-2">
            {preKlineError || '该区间暂无足够连续日 K 数据'}
          </div>
        )}
      </div>
      {visibleEntries.map(entry => {
        const isHighlight = entry.month === highlightMonth;
        const cum = entry.cumulative_return;
        const cumColor = cum != null ? (cum >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-secondary-text';
        const bars = entry.daily_returns ?? [];
        const hasOHLC = bars.length >= 2 && bars.some(d => d.open != null && d.high != null && d.low != null);
        return (
          <div
            key={entry.month}
            className={
              isHighlight
                ? 'rounded-lg border border-amber-500/30 bg-amber-500/5 p-2'
                : 'rounded-lg border border-border/10 p-2'
            }
          >
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs mb-2">
              <span className="font-medium">{fmtMonthLabel(entry.month)}</span>
              {entry.broker_count > 0 && (
                <span className="text-secondary-text">{entry.broker_count} 家券商推荐</span>
              )}
              {entry.brokers?.length > 0 && (
                <span className="text-tertiary-text truncate max-w-[280px]" title={entry.brokers.join('、')}>
                  {entry.brokers.slice(0, 3).join('、')}
                  {entry.brokers.length > 3 ? ` 等${entry.brokers.length}家` : ''}
                </span>
              )}
              {(entry.buy_date || entry.sell_date) && (
                <span className="text-tertiary-text font-mono">
                  {fmtDate(entry.buy_date)} → {fmtDate(entry.sell_date)}
                </span>
              )}
              <span className={`font-medium ml-auto tabular-nums ${cumColor}`}>{fmtPct(cum)}</span>
            </div>
            {bars.length >= 2 && hasOHLC ? (
              <CandlestickMiniChart
                data={bars.map(d => ({
                  date: d.date,
                  price: d.price,
                  open: d.open,
                  high: d.high,
                  low: d.low,
                }))}
                height={140}
              />
            ) : bars.length >= 2 ? (
              <ResponsiveContainer width="100%" height={120}>
                <LineChart
                  margin={{ top: 2, right: 0, bottom: 4, left: -20 }}
                  data={bars.map(d => ({
                    date: fmtDate(d.date),
                    cumulative: d.cumulative,
                    daily_return: d.daily_return,
                    price: d.price,
                  }))}
                >
                  <XAxis dataKey="date" tick={{ fontSize: 8, fill: '#9ca3af' }} stroke="#6b7280" interval={3} />
                  <YAxis tick={{ fontSize: 8, fill: '#9ca3af' }} stroke="#6b7280" tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`} />
                  <Tooltip
                    content={({ active, payload, label }: any) => {
                      if (!active || !payload?.length) return null;
                      const row = payload[0]?.payload;
                      const dr = row?.daily_return;
                      const drColor = dr != null ? (dr >= 0 ? '#ef4444' : '#10b981') : '#9ca3af';
                      const cumVal = row?.cumulative;
                      const cumCol = cumVal != null ? (cumVal >= 0 ? '#ef4444' : '#10b981') : '#9ca3af';
                      return (
                        <div style={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', borderRadius: 6, padding: '6px 10px', fontSize: 11 }}>
                          <div style={{ marginBottom: 2, color: '#9ca3af' }}>{label}</div>
                          <div>涨跌幅: <span style={{ color: drColor }}>{dr != null ? `${(dr * 100).toFixed(2)}%` : '--'}</span></div>
                          <div>价格: <span style={{ color: '#e2e8f0' }}>{row?.price != null ? row.price.toFixed(2) : '--'}</span></div>
                          <div>累计: <span style={{ color: cumCol }}>{cumVal != null ? `${(cumVal * 100).toFixed(2)}%` : '--'}</span></div>
                        </div>
                      );
                    }}
                  />
                  <Line type="monotone" dataKey="cumulative" stroke="#f59e0b" strokeWidth={1.5} dot={false} connectNulls />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="text-xs text-tertiary-text py-2">该月持仓期不足 2 个交易日，暂无 K 线</div>
            )}
          </div>
        );
      })}
    </div>
  );
}


const BrokerRecommendPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const monthParam = searchParams.get('month');
  const selectedMonth: Dayjs = monthParam && dayjs(monthParam, 'YYYYMM').isValid() && !dayjs(monthParam, 'YYYYMM').isBefore(dayjs('2020-03-01'), 'month') ? dayjs(monthParam, 'YYYYMM') : dayjs();
  const [loadingData, setLoadingData] = useState(false);
  const [fetchTrigger, setFetchTrigger] = useState(0);
  const [recommendData, setRecommendData] = useState<BrokerRecommendResponse | null>(null);
  const [backtestData, setBacktestData] = useState<BrokerBacktestResponse | null>(null);
  const [enrichmentData, setEnrichmentData] = useState<EnrichmentResponse | null>(null);
  const [loadingEnrichment, setLoadingEnrichment] = useState(false);
  const [expandedBrokers, setExpandedBrokers] = useState<Set<string>>(new Set());
  const viewParam = searchParams.get('view');
  const viewMode: 'broker' | 'stock' = viewParam === 'stock' ? 'stock' : 'broker';
  const setViewMode = useCallback((mode: 'broker' | 'stock') => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      if (mode === 'broker') next.delete('view');
      else next.set('view', mode);
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  const monthStr = selectedMonth.format('YYYYMM');
  const isCurrentMonth = monthStr === dayjs().format('YYYYMM');

  useEffect(() => {
    stockHistoryCache.clear();
  }, [monthStr]);

  const prevMonthRef = useRef(monthStr);
  const [visibleChartBrokers, setVisibleChartBrokers] = useState<Set<string>>(new Set());
  const [expandedKey, setExpandedKey] = useState<string>('');
  const [tableKey, setTableKey] = useState(0);
  const expandedKeyRef = useRef<string>('');
  const pendingScrollToStockRef = useRef<string | null>(null);
  const [activeTab, setActiveTab] = useState<string>('monthly');
  const [ytdData, setYtdData] = useState<YtdBacktestResponse | null>(null);
  const [topBrokers, setTopBrokers] = useState<string[]>([]);
  const [ytdLoading, setYtdLoading] = useState(false);
  const [strategyData, setStrategyData] = useState<EqualWeightStrategyResponse | null>(null);
  const [strategyLoading, setStrategyLoading] = useState(false);
  const [strategyStartMonth, setStrategyStartMonth] = useState<Dayjs>(() => dayjs().startOf('year'));
  const [strategyEndMonth, setStrategyEndMonth] = useState<Dayjs>(() => dayjs());
  const strategyLoadedKeyRef = useRef<string | null>(null);
  const [strategyFetchTrigger, setStrategyFetchTrigger] = useState(0);
  const strategyStartStr = strategyStartMonth.format('YYYYMM');
  const strategyEndStr = strategyEndMonth.format('YYYYMM');
  const [consecutiveData, setConsecutiveData] = useState<ConsecutiveStockItem[]>([]);
  const consecutiveSet = useMemo(() => new Set(consecutiveData.map(c => c.ts_code)), [consecutiveData]);
  const [historicalStats, setHistoricalStats] = useState<Record<string, HistoricalRecommendStatsItem>>({});
  const [upToDownDaily, setUpToDownDaily] = useState<UpToDownDailyResponse | null>(null);
  const [loadingUpToDownDaily, setLoadingUpToDownDaily] = useState(false);
  const [upToDownAsOfDate, setUpToDownAsOfDate] = useState<Dayjs | null>(null);
  const [upToDownStruck, setUpToDownStruck] = useState<Set<string>>(loadUpToDownStruckSet);
  const [showScrollTop, setShowScrollTop] = useState(false);
  const sectorStatsRef = useRef<HTMLDivElement>(null);
  const nineturnCardRef = useRef<HTMLDivElement>(null);
  const stockNavSourceRef = useRef<'sector' | 'nineturn'>('nineturn');

  useEffect(() => {
    const onScroll = () => setShowScrollTop(window.scrollY > 600);
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  const scrollToTop = useCallback(() => {
    const ref = stockNavSourceRef.current === 'sector' ? sectorStatsRef : nineturnCardRef;
    ref.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, []);

  const focusStockInDetailList = useCallback((tsCode: string) => {
    pendingScrollToStockRef.current = tsCode;
    setViewMode('stock');
    setExpandedKey(tsCode);
    expandedKeyRef.current = tsCode;
    setTableKey((k) => k + 1);
  }, [setViewMode]);

  const toggleUpToDownStruck = useCallback((
    signalDate: string,
    tsCode: string,
    signalType: string,
  ) => {
    const key = upToDownStruckRowKey(monthStr, signalDate, tsCode, signalType);
    setUpToDownStruck((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      saveUpToDownStruckSet(next);
      return next;
    });
  }, [monthStr]);

  /** 仅当 API 返回的 month 与当前选择一致时才用于表格，避免切换月份时旧请求覆盖。 */
  const activeBacktest = useMemo(
    () => (backtestData?.month === monthStr ? backtestData : null),
    [backtestData, monthStr],
  );
  const activeRecommend = useMemo(
    () => (recommendData?.month === monthStr ? recommendData : null),
    [recommendData, monthStr],
  );
  const activeEnrichment = useMemo(
    () => (enrichmentData?.month === monthStr ? enrichmentData : null),
    [enrichmentData, monthStr],
  );
  const activeUpToDownDaily = useMemo(
    () => (upToDownDaily?.month === monthStr ? upToDownDaily : null),
    [upToDownDaily, monthStr],
  );
  const upToDownDateBounds = useMemo(() => {
    const buy = activeUpToDownDaily?.buy_date || activeBacktest?.buy_date;
    const sell = activeUpToDownDaily?.sell_date || activeBacktest?.sell_date;
    if (!buy || !sell) return null;
    let max = dayjs(sell, 'YYYYMMDD');
    if (isCurrentMonth && max.isAfter(dayjs(), 'day')) {
      max = dayjs();
    }
    return { min: dayjs(buy, 'YYYYMMDD'), max };
  }, [activeUpToDownDaily, activeBacktest, isCurrentMonth]);

  const filteredUpToDown = useMemo(() => {
    const days = activeUpToDownDaily?.days;
    if (!days?.length || !upToDownAsOfDate) return null;
    const selected = upToDownAsOfDate.format('YYYYMMDD');
    const hit = days.find((d) => d.date === selected);
    if (!hit) return null;
    return { date: hit.date, stocks: hit.stocks ?? [] };
  }, [activeUpToDownDaily, upToDownAsOfDate]);

  const reversalSplitStocks = useMemo(() => {
    if (!filteredUpToDown) {
      return { date: '', upToDown: [] as UpToDownDailyStockItem[], downToUp: [] as UpToDownDailyStockItem[] };
    }
    const stocks = filteredUpToDown.stocks;
    const enrichSector = (s: UpToDownDailyStockItem) => ({
      ...s,
      sector: activeEnrichment?.data?.[s.ts_code]?.sector ?? null,
    });
    return {
      date: filteredUpToDown.date,
      upToDown: stocks
        .filter((s) => (s.signal_type ?? 'up_to_down') === 'up_to_down')
        .map(enrichSector),
      downToUp: stocks
        .filter((s) => s.signal_type === 'down_to_up')
        .map(enrichSector),
    };
  }, [filteredUpToDown, activeEnrichment]);

  useEffect(() => {
    setUpToDownAsOfDate(null);
  }, [monthStr]);

  useEffect(() => {
    const tsCode = pendingScrollToStockRef.current;
    if (!tsCode || expandedKey !== tsCode || viewMode !== 'stock') return;
    pendingScrollToStockRef.current = null;
    const timer = window.setTimeout(() => {
      document.getElementById(`broker-stock-row-${tsCode}`)?.scrollIntoView({
        behavior: 'smooth',
        block: 'center',
      });
    }, 150);
    return () => window.clearTimeout(timer);
  }, [expandedKey, tableKey, viewMode]);

  useEffect(() => {
    if (!upToDownDateBounds || upToDownAsOfDate) return;
    const latest = activeUpToDownDaily?.days?.[0]?.date;
    if (latest) {
      setUpToDownAsOfDate(dayjs(latest, 'YYYYMMDD'));
    } else {
      setUpToDownAsOfDate(upToDownDateBounds.max);
    }
  }, [activeUpToDownDaily, upToDownDateBounds, monthStr, upToDownAsOfDate]);
  const enrichAsOfLabel = useMemo(() => {
    const qd = activeEnrichment?.query_date;
    if (!qd || qd.length < 8) return '';
    return `(${qd.slice(4, 6)}-${qd.slice(6, 8)})`;
  }, [activeEnrichment]);
  const holdPeriodLabel = useMemo(() => {
    if (!activeBacktest?.buy_date || !activeBacktest?.sell_date) return '';
    return `(${fmtDate(activeBacktest.buy_date).slice(5)}~${fmtDate(activeBacktest.sell_date).slice(5)})`;
  }, [activeBacktest]);


  useEffect(() => {
    if (!activeRecommend?.items?.length) {
      setHistoricalStats({});
      return;
    }
    const codes = [...new Set(activeRecommend.items.map((i) => i.ts_code))];
    let cancelled = false;
    getHistoricalRecommendStats(codes, monthStr)
      .then((items) => {
        if (cancelled) return;
        const map: Record<string, HistoricalRecommendStatsItem> = {};
        for (const it of items) map[it.ts_code] = it;
        setHistoricalStats(map);
      })
      .catch(() => {
        if (!cancelled) setHistoricalStats({});
      });
    return () => { cancelled = true; };
  }, [activeRecommend]);
  const historyTopCodes = useMemo(() => {
    if (!isCurrentMonth || !activeRecommend?.items?.length) return new Set<string>();
    if (!Object.keys(historicalStats).length) return new Set<string>();
    const codes = dedupStocks(activeRecommend.items).map((i) => i.ts_code);
    const ranked = codes
      .map((tc) => ({ ts_code: tc, count: historicalStats[tc]?.month_count ?? 0 }))
      .filter((x) => x.count > 0)
      .sort((a, b) => b.count - a.count)
      .slice(0, HISTORY_RECOMMEND_TOP_N);
    return new Set(ranked.map((x) => x.ts_code));
  }, [isCurrentMonth, activeRecommend, historicalStats]);


  // Controlled sort state to preserve across data refreshes
  const [tableSort, setTableSort] = useState<{ columnKey?: string; order?: 'ascend' | 'descend' }>({
    columnKey: 'cumRet', order: 'descend',
  });

  // Auto-load recommendations when selectedMonth changes or refresh triggered
  useEffect(() => {
    if (!monthStr) return;
    const isMonthChange = prevMonthRef.current !== monthStr;
    prevMonthRef.current = monthStr;
    const loadMonth = monthStr;
    let cancelled = false;

    async function load() {
      setLoadingData(true);
      setLoadingEnrichment(true);
      if (isMonthChange) {
        setRecommendData(null);
        setBacktestData(null);
        setEnrichmentData(null);
        setUpToDownDaily(null);
        setExpandedKey('');
        setTableKey(k => k + 1);
      }
      setLoadingUpToDownDaily(true);
      try {
        const [data, bt, enrich, cons, top, upDaily] = await Promise.all([
          getMonthlyRecommendations(loadMonth),
          getBacktest(loadMonth),
          getMonthlyEnrichment(loadMonth),
          getConsecutiveStocks(loadMonth),
          getTopBrokers(5).catch(() => [] as string[]),
          getMonthlyUpToDownDaily(loadMonth).catch(() => null),
        ]);
        if (cancelled || loadMonth !== monthStr) return;
        setRecommendData(data.month === loadMonth ? data : null);
        setBacktestData(bt.month === loadMonth ? bt : null);
        setEnrichmentData(enrich.month === loadMonth ? enrich : null);
        setUpToDownDaily(upDaily?.month === loadMonth ? upDaily : null);
        setConsecutiveData(cons);
        setTopBrokers(top);
      } catch (e) {
        if (!cancelled) console.error('Failed to load:', e);
      } finally {
        if (!cancelled) {
          setLoadingData(false);
          setLoadingEnrichment(false);
          setLoadingUpToDownDaily(false);
        }
      }
    }
    load();
    return () => { cancelled = true; };
  }, [monthStr, fetchTrigger]);

  // 盘中轮询：当前月交易日 09:30-15:00 每 30 秒拉最新回测数据（含最新价）
  useEffect(() => {
    if (!isCurrentMonth || activeTab !== 'monthly') return;
    const isTradingHour = (h: number) => h >= 9 && (h < 15 || (h === 15 && new Date().getMinutes() < 0));
    if (!isTradingHour(new Date().getHours())) return;

    const interval = setInterval(async () => {
      const hour = new Date().getHours();
      if (!isTradingHour(hour)) { clearInterval(interval); return; }
      try {
        const bt = await getBacktest(monthStr);
        if (bt?.month === monthStr) setBacktestData(bt);
      } catch {
        // 静默失败，下次重试
      }
    }, 30_000);
    return () => clearInterval(interval);
  }, [isCurrentMonth, activeTab, monthStr]);

  const handleFetch = useCallback(async () => {
    if (!monthStr) return;
    setLoadingData(true);
    try {
      await fetchMonth(monthStr);
      // 当前月抓取后触发数据刷新（价格、筹码胜率、累计收益）
      setFetchTrigger(t => t + 1);
    } catch (e) {
      console.error('Failed to fetch:', e);
    } finally {
      setLoadingData(false);
    }
  }, [monthStr]);

  // Init chart to top 5 brokers by cumulative return
  useEffect(() => {
    if (activeBacktest?.brokers?.length) {
      const top5 = [...activeBacktest.brokers]
        .sort((a, b) => b.cumulative_return - a.cumulative_return)
        .slice(0, 5)
        .map(b => b.broker);
      setVisibleChartBrokers(new Set(top5));
    }
  }, [activeBacktest]);

  // Load YTD data when switching to YTD tab
  useEffect(() => {
    if (activeTab !== 'ytd' || ytdData) return;
    setYtdLoading(true);
    getYtdBacktest(5)
      .then(setYtdData)
      .catch((e) => console.error('Failed to load YTD:', e))
      .finally(() => setYtdLoading(false));
  }, [activeTab, ytdData]);

  const strategyPeriodLabel = useMemo(() => {
    if (!strategyData?.period_start_month) return null;
    const start = fmtMonthLabel(strategyData.period_start_month);
    const end = strategyData.period_end_month
      ? fmtMonthLabel(strategyData.period_end_month)
      : start;
    return start === end ? start : `${start} – ${end}`;
  }, [strategyData]);

  const reloadStrategyData = useCallback(() => {
    strategyLoadedKeyRef.current = null;
    setStrategyData(null);
    setStrategyFetchTrigger((t) => t + 1);
  }, []);

  // Load equal-weight strategy when tab active or range changes (with polling)
  useEffect(() => {
    if (activeTab !== 'strategy') return;
    const fetchKey = `${strategyStartStr}|${strategyEndStr}`;
    if (strategyLoadedKeyRef.current === fetchKey) return;

    setStrategyLoading(true);
    setStrategyData(null);
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const poll = () => {
      if (cancelled) return;
      getEqualWeightStrategy(4, strategyStartStr, strategyEndStr)
        .then((data) => {
          if (cancelled) return;
          if (data.status === 'computing') {
            timer = setTimeout(poll, 2000);
          } else {
            strategyLoadedKeyRef.current = fetchKey;
            setStrategyData(data);
            setStrategyLoading(false);
          }
        })
        .catch((e) => {
          if (!cancelled) {
            console.error('Failed to load strategy:', e);
            setStrategyLoading(false);
          }
        });
    };
    poll();

    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [activeTab, strategyStartStr, strategyEndStr, strategyFetchTrigger]);

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
    if (!activeBacktest) return [];
    const dateSet = new Set<string>();
    activeBacktest.brokers.forEach(b => {
      b.daily_returns.forEach(d => dateSet.add(d.date));
    });
    const dates = Array.from(dateSet).sort();
    return dates.map(date => {
      const entry: Record<string, string | number | undefined> = { date: fmtDate(date) };
      activeBacktest.brokers.forEach((b) => {
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

  // 九转选股等权策略图表数据
  const strategyChartData = useMemo(() => {
    if (!strategyData?.daily_returns?.length) return [];
    return strategyData.daily_returns.map(dr => ({
      date: fmtDate(dr.date),
      cumulative: dr.cumulative,
      daily_return: dr.daily_return,
      stock_count: dr.stock_count,
    }));
  }, [strategyData]);

  // rank2 单独 + rank2&4 等权对比曲线
  const multiCurveColors: Record<string, string> = { rank2: '#60a5fa', rank24: '#34d399' };
  const multiCurveLabels: Record<string, string> = { rank2: '第2顺位单独', rank24: '第2+4顺位等权' };
  const multiCurveData = useMemo(() => {
    if (!strategyData?.multi_curves) return [];
    const keys = Object.keys(strategyData.multi_curves).sort();
    if (!keys.length) return [];
    const dateSet = new Set<string>();
    keys.forEach(k => {
      (strategyData.multi_curves![k] || []).forEach(d => dateSet.add(d.date));
    });
    const dates = Array.from(dateSet).sort();
    return dates.map(date => {
      const entry: Record<string, string | number | undefined> = { date: fmtDate(date) };
      keys.forEach(k => {
        const dr = (strategyData.multi_curves![k] || []).find(d => d.date === date);
        entry[k] = dr?.cumulative ?? undefined;
      });
      return entry;
    });
  }, [strategyData]);

  const strategyMonthlyWinRate = useMemo(() => {
    if (!strategyData?.monthly_returns?.length) return null;
    const wins = strategyData.monthly_returns.filter(m => m.month_return > 0).length;
    return wins / strategyData.monthly_returns.length;
  }, [strategyData]);

  const strategyAvgMonthlyReturn = useMemo(() => {
    if (!strategyData?.monthly_returns?.length) return null;
    return strategyData.monthly_returns.reduce((s, m) => s + m.month_return, 0) / strategyData.monthly_returns.length;
  }, [strategyData]);

  // Build deduped stock rows with enrichment
  const stockRows = useMemo((): StockRow[] => {
    if (!activeRecommend?.items) return [];
    const rows: StockRow[] = dedupStocks(activeRecommend.items).map(item => {
      const stockRet = activeBacktest?.stock_returns?.find(
        s => s.ts_code === item.ts_code
      );
      const cumRet = stockRet?.daily_returns?.length
        ? stockRet.daily_returns[stockRet.daily_returns.length - 1].cumulative
        : undefined;
      return {
        ts_code: item.ts_code,
        name: item.name,
        broker_count: item.broker_count,
        isConsecutive: consecutiveSet.has(item.ts_code),
        dailyChange: stockRet?.daily_change,
        endPrice: stockRet?.end_price,
        endDate: stockRet?.end_date,
        cumRet,
        nineturn: activeEnrichment?.data[item.ts_code]?.nineturn ?? null,
        forecast: activeEnrichment?.data[item.ts_code]?.forecast ?? null,
        cyq_perf: activeEnrichment?.data[item.ts_code]?.cyq_perf ?? null,
        sector: activeEnrichment?.data[item.ts_code]?.sector ?? null,
      };
    });

    for (const r of rows) {
      const hist = historicalStats[r.ts_code];
      if (hist) {
        r.historyMonthCount = hist.month_count;
        r.historyPeriodCount = hist.period_count;
        r.historyWinRate = hist.win_rate ?? null;
        r.historyMaxReturn = hist.max_return ?? null;
        r.historyMaxDrawdown = hist.max_drawdown ?? null;
      }
      if (historyTopCodes.has(r.ts_code)) {
        r.isHistoryTop = true;
      }
    }

    // 当前月：筹码集中度 + 胜率综合 Top3（金色行背景，与历史 Top 可并存）
    if (isCurrentMonth) {
      const scored = rows
        .filter(r => {
          const conc = r.cyq_perf?.concentration ?? r.cyq_perf?.scr90;
          return conc != null && r.cyq_perf?.winner_rate != null;
        })
        .map(r => {
          const rawConc = (r.cyq_perf!.scr90 ?? r.cyq_perf!.concentration)!;
          const normConc = rawConc / 100;
          const score = 0.5 * (1 - normConc) + 0.5 * r.cyq_perf!.winner_rate!;
          return { ts_code: r.ts_code, score };
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 3);

      const topSet = new Set(scored.map(s => s.ts_code));
      for (const r of rows) {
        if (topSet.has(r.ts_code)) r.isTopPick = true;
      }
    }

    return rows;
  }, [activeRecommend, activeBacktest, activeEnrichment, isCurrentMonth, consecutiveSet, historicalStats, historyTopCodes]);

  // 行业统计（按累计收益均值排序，Top3 高亮）
  const sectorStats = useMemo(() => {
    if (!stockRows.length) return [];
    type SectorStat = { sector: string; count: number; brokerCount: number; avgCumRet: number; stocks: StockRow[] };
    const map = new Map<string, SectorStat>();
    for (const row of stockRows) {
      const sector = row.sector || '未分类';
      const existing = map.get(sector);
      if (existing) {
        existing.count += 1;
        existing.brokerCount += row.broker_count;
        if (row.cumRet != null) {
          existing.avgCumRet = ((existing.avgCumRet * (existing.count - 1)) + row.cumRet) / existing.count;
        }
        existing.stocks.push(row);
      } else {
        map.set(sector, {
          sector, count: 1, brokerCount: row.broker_count,
          avgCumRet: row.cumRet ?? 0, stocks: [row],
        });
      }
    }
    const sorted = Array.from(map.values()).sort((a, b) => b.avgCumRet - a.avgCumRet);
    const topSet = new Set(sorted.slice(0, 3).map((s) => s.sector));
    return sorted.map((s) => ({ ...s, isTop: topSet.has(s.sector) }));
  }, [stockRows]);

  // --- Table column definitions ---
  const stockColumns: ColumnsType<StockRow> = useMemo(() => [
    {
      title: '股票', key: 'stock',
      sorter: (a, b) => {
        const pri = (r: StockRow) => (r.isConsecutive ? 0 : 1);
        const d = pri(a) - pri(b);
        if (d !== 0) return d;
        return (a.name || '').localeCompare(b.name || '', 'zh-CN');
      },
      sortOrder: tableSort.columnKey === 'stock' ? tableSort.order : undefined,
      render: (_: unknown, row: StockRow) => (
        <div className="leading-tight min-w-[4.5rem]">
          <div className="text-xs text-secondary-text">{row.name}</div>
          <div className="font-mono text-[10px] text-tertiary-text">{row.ts_code}</div>
        </div>
      ),
    },
    {
      title: '标签', key: 'tags', width: 92,
      render: (_: unknown, row: StockRow) => <StockTagsCell row={row} />,
    },
    {
      title: '历史统计', key: 'historyStats', width: 108,
      sorter: (a: StockRow, b: StockRow) => (a.historyWinRate ?? -1) - (b.historyWinRate ?? -1),
      sortOrder: tableSort.columnKey === 'historyStats' ? tableSort.order : undefined,
      render: (_: unknown, row: StockRow) => <HistoryStatsCell row={row} />,
    },

    ...(isCurrentMonth ? [{
      title: '当天涨幅', dataIndex: 'dailyChange', key: 'dailyChange',
      sorter: (a: StockRow, b: StockRow) => (a.dailyChange ?? -Infinity) - (b.dailyChange ?? -Infinity),
      sortOrder: tableSort.columnKey === 'dailyChange' ? tableSort.order : undefined,
      render: (_: any, row: StockRow) => (
        <span className={`text-xs font-medium ${row.dailyChange != null ? (row.dailyChange >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-tertiary-text'}`}>
          {row.dailyChange != null ? `${row.dailyChange >= 0 ? '+' : ''}${(row.dailyChange * 100).toFixed(2)}%` : '--'}
        </span>
      ),
    }] : []),
    {
      title: isCurrentMonth ? '最新价' : '月末价', dataIndex: 'endPrice', key: 'endPrice',
      render: (_: any, row: StockRow) => (
        <span className="text-xs text-secondary-text whitespace-nowrap">
          {row.endPrice != null ? row.endPrice.toFixed(2) : '--'}
          {row.endDate ? <span className="text-tertiary-text ml-1">({fmtDate(row.endDate).slice(5)})</span> : null}
        </span>
      ),
    },
    ...(isCurrentMonth ? [{
      title: <span>集中度{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</span>,
      key: 'concentration',
      sorter: (a: StockRow, b: StockRow) => {
        const valA = a.cyq_perf?.scr90 ?? a.cyq_perf?.concentration;
        const valB = b.cyq_perf?.scr90 ?? b.cyq_perf?.concentration;
        if (valA == null && valB == null) return 0;
        if (valA == null) return 1;
        if (valB == null) return -1;
        return valA - valB;
      },
      sortOrder: tableSort.columnKey === 'concentration' ? tableSort.order : undefined,
      render: (_: any, row: StockRow) => {
        const val = row.cyq_perf?.scr90 ?? row.cyq_perf?.concentration;
        if (val == null) return <span className="text-xs text-tertiary-text">--</span>;
        return <span className="text-xs text-secondary-text">{val.toFixed(2)}%</span>;
      },
    }] : []),
    {
      title: <span>九转信号{enrichAsOfLabel ? <span className="text-tertiary-text font-normal ml-0.5">{enrichAsOfLabel}</span> : null}{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</span>,
      key: 'nineturn',
      render: (_, row) => {
        const nt = row.nineturn;
        if (!nt) return <span className="text-xs text-tertiary-text">--</span>;
        const showNineUp = nt.nine_up_turn && (!nt.up_count || nt.up_count <= 9);
        const showNineDown = nt.nine_down_turn && (!nt.down_count || nt.down_count <= 9);
        if (showNineUp) return <span className="text-xs text-emerald-400 font-medium">上涨9转</span>;
        if (showNineDown) return <span className="text-xs text-red-400 font-medium">下跌9转</span>;
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
      title: <span>盈利预测{enrichAsOfLabel ? <span className="text-tertiary-text font-normal ml-0.5">{enrichAsOfLabel}</span> : null}{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</span>,
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
      title: <span>筹码胜率{loadingEnrichment ? <Loader2 className="h-3 w-3 animate-spin inline ml-1" /> : null}</span>,
      key: 'cyq_perf',
      sorter: (a, b) => (a.cyq_perf?.winner_rate ?? -Infinity) - (b.cyq_perf?.winner_rate ?? -Infinity),
      sortOrder: tableSort.columnKey === 'cyq_perf' ? tableSort.order : undefined,
      render: (_, row) => {
        const cyq = row.cyq_perf;
        if (!cyq) return <span className="text-xs text-tertiary-text">--</span>;
        return (
          <div className="text-xs">
            {cyq.winner_rate != null && (
              <div className={cyq.winner_rate >= 0.5 ? 'text-red-400' : 'text-emerald-400'}>
                {(cyq.winner_rate * 100).toFixed(1)}%
                {activeEnrichment?.query_date && (
                  <span className="text-tertiary-text ml-1">
                    ({activeEnrichment.query_date.slice(4, 6)}-{activeEnrichment.query_date.slice(6)})
                  </span>
                )}
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
      sortOrder: tableSort.columnKey === 'cumRet' ? (tableSort.order ?? 'descend') : undefined,
      render: (_, row) => (
        <div className="text-xs leading-snug tabular-nums">
          <span className={`font-medium ${row.cumRet != null ? (row.cumRet >= 0 ? 'text-red-400' : 'text-emerald-400') : 'text-tertiary-text'}`}>
            {fmtPct(row.cumRet)}
          </span>
          {holdPeriodLabel ? (
            <div className="text-[10px] text-tertiary-text font-normal mt-0.5">{holdPeriodLabel}</div>
          ) : null}
        </div>
      ),
    },
    {
      title: '所属行业', key: 'sector',
      sorter: (a, b) => (a.sector || '').localeCompare(b.sector || ''),
      sortOrder: tableSort.columnKey === 'sector' ? tableSort.order : undefined,
      render: (_: any, row: StockRow) => (
        <span className="text-xs text-secondary-text whitespace-nowrap">{row.sector || '--'}</span>
      ),
    },
    {
      title: <span style={{ whiteSpace: 'nowrap' }}>推荐数</span>, dataIndex: 'broker_count', key: 'broker_count',
      sorter: (a, b) => a.broker_count - b.broker_count,
      sortOrder: tableSort.columnKey === 'broker_count' ? tableSort.order : undefined,
      render: (v: number) => <span className="text-xs text-tertiary-text whitespace-nowrap">{v}</span>,
    },
  ], [loadingEnrichment, monthStr, tableSort, isCurrentMonth, enrichAsOfLabel, holdPeriodLabel, activeEnrichment]);

  // Broker groups
  const brokerGroups = useMemo((): Map<string, BrokerRecommendItem[]> => {
    if (!activeRecommend?.items?.length) return new Map();
    const map = new Map<string, BrokerRecommendItem[]>();
    for (const item of activeRecommend.items) {
      const existing = map.get(item.broker) || [];
      existing.push(item);
      map.set(item.broker, existing);
    }
    return map;
  }, [activeRecommend]);

  return (
    <AppPage className="max-w-none px-2 md:px-3">
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
                disabledDate={(d) => d.isAfter(dayjs(), 'month') || d.isBefore(dayjs('2020-03-01'), 'month')}
                className="h-9"
              />
            </div>

            {isCurrentMonth && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleFetch}
              disabled={loadingData}
            >
              {loadingData ? <Loader2 className="h-4 w-4 animate-spin mr-1" /> : <RefreshCw className="h-4 w-4 mr-1" />}
              刷新数据
            </Button>
            )}

            <span className="text-xs text-tertiary-text ml-auto">
              {activeBacktest
                ? `回测区间: ${fmtDate(activeBacktest.buy_date)} → ${fmtDate(activeBacktest.sell_date)}`
                : activeRecommend
                ? `${monthStr} 月券商金股`
                : '--'}
            </span>
          </div>
        </Card>

        {/* Loading - only show full skeleton when no cached data */}
        {loadingData && !activeRecommend && (
          <Card className="p-4 text-center text-sm text-tertiary-text">
            <Loader2 className="h-4 w-4 animate-spin inline mr-2" />
            加载中...
          </Card>
        )}

        {/* Subtle refresh indicator when loading with existing data */}
        {loadingData && activeRecommend && (
          <div className="text-xs text-tertiary-text flex items-center gap-1 mb-1">
            <Loader2 className="h-3 w-3 animate-spin" />
            更新中...
          </div>
        )}

        {/* Overview */}
        {activeRecommend && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <Card className="p-3 text-center">
              <div className="text-lg font-bold">{activeRecommend.total_recommendations}</div>
              <div className="text-xs text-secondary-text">推荐总数</div>
            </Card>
            <button
              className={`rounded-2xl terminal-card p-3 text-center cursor-pointer transition-colors w-full ${viewMode === 'stock' ? 'ring-2 ring-cyan/50 bg-cyan/[0.05]' : 'hover:bg-muted/50'}`}
              onClick={() => setViewMode('stock')}
            >
              <div className="text-lg font-bold">{activeRecommend.unique_stocks}</div>
              <div className="text-xs text-secondary-text">涉及股票</div>
            </button>
            <button
              className={`rounded-2xl terminal-card p-3 text-center cursor-pointer transition-colors w-full ${viewMode === 'broker' ? 'ring-2 ring-cyan/50 bg-cyan/[0.05]' : 'hover:bg-muted/50'}`}
              onClick={() => setViewMode('broker')}
            >
              <div className="text-lg font-bold">{activeRecommend.unique_brokers}</div>
              <div className="text-xs text-secondary-text">券商数量</div>
            </button>
            <Card className="p-3 text-center">
              <div className={`text-lg font-bold ${(activeBacktest?.brokers[0]?.cumulative_return || 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                {fmtPct(activeBacktest?.brokers[0]?.cumulative_return ?? 0)}
              </div>
              <div className="text-xs text-secondary-text">最优券商收益</div>
            </Card>
          </div>
        )}

        {/* Chart */}
        {viewMode === 'broker' && activeBacktest && chartData.length > 0 && activeBacktest.brokers.length > 0 && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-2">券商组合收益曲线</div>
            {/* Legend: click to toggle, greyed out when hidden */}
            <div className="flex flex-wrap gap-x-3 gap-y-1 mb-1">
              {activeBacktest.brokers.map((b, i) => {
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
                {activeBacktest.brokers.map((b, i) => (
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

        {/* Tables - keep visible during refresh to preserve sort state */}
        {viewMode === 'stock' && sectorStats.length > 0 && (
          <div ref={sectorStatsRef}>
          <Card className="p-2.5">
            <div className="text-[11px] font-medium mb-1 text-secondary-text">当月金股行业统计（按累计收益均值，Top3 高亮）</div>
            <div className="grid grid-cols-4 gap-1.5">
              {sectorStats.map((stat) => (
                <details key={stat.sector} className={`group rounded py-0.5 px-1.5 cursor-pointer open:border-cyan/50 ${stat.isTop ? 'border border-amber-500/40 bg-amber-500/5 open:border-amber-500/70' : 'border border-border/10 hover:border-border/30'}`}>
                  <summary className="flex items-center gap-1 text-[11px] marker:text-tertiary-text marker:text-[10px]">
                    {stat.isTop && <span className="text-[9px] text-amber-400 shrink-0">★</span>}
                    <span className="truncate" title={stat.sector}>{stat.sector}</span>
                    <span className={`text-[10px] tabular-nums shrink-0 ml-auto ${stat.avgCumRet >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(stat.avgCumRet)}</span>
                  </summary>
                  <div className="mt-0.5 border-t border-border/10 pt-0.5 grid grid-cols-2 gap-x-1">
                    {stat.stocks.map((s) => (
                      <button
                        key={s.ts_code}
                        type="button"
                        className="flex items-center gap-1 w-full text-left hover:bg-foreground/[0.03] rounded px-0.5"
                        onClick={() => { stockNavSourceRef.current = 'sector'; focusStockInDetailList(s.ts_code); }}
                      >
                        <span className="text-[10px] text-secondary-text truncate">{s.name}</span>
                        <span className="font-mono text-[9px] text-tertiary-text">{s.ts_code}</span>
                      </button>
                    ))}
                  </div>
                </details>
              ))}
            </div>
          </Card>
          </div>
        )}

        {activeRecommend && brokerGroups.size > 0 && viewMode === 'stock' && activeBacktest && (
          <div ref={nineturnCardRef}>
            <Card className="p-4">
            <div className="flex flex-wrap items-center justify-between gap-2 mb-1">
              <div className="text-sm font-medium flex items-center gap-2">
                九转反转个股
                {loadingUpToDownDaily && <Loader2 className="h-3.5 w-3.5 animate-spin text-tertiary-text" />}
              </div>
              {upToDownDateBounds && (
                <DatePicker
                  size="small"
                  locale={zhCN.DatePicker}
                  value={upToDownAsOfDate}
                  onChange={(d) => setUpToDownAsOfDate(d)}
                  allowClear={false}
                  disabledDate={(d) => (
                    d.isBefore(upToDownDateBounds.min, 'day')
                    || d.isAfter(upToDownDateBounds.max, 'day')
                  )}
                />
              )}
            </div>
            <div className="text-xs text-tertiary-text mb-3">
              {upToDownAsOfDate
                ? filteredUpToDown
                  ? `信号日 ${fmtDate(filteredUpToDown.date)}；升 1..8 转降 / 降 1..8 升；点击方框划线标记`
                  : `${upToDownAsOfDate.format('YYYY-MM-DD')} 当日无升转降或降转升信号`
                : '当月金股池收盘升 1..8 转降、降 1..8 升；末交易日忽略'}
            </div>
            {upToDownAsOfDate && !loadingUpToDownDaily ? (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <ReversalSignalTable
                  title="升转降"
                  titleClassName="text-cyan"
                  signalType="up_to_down"
                  signalDate={reversalSplitStocks.date || upToDownAsOfDate.format('YYYYMMDD')}
                  monthStr={monthStr}
                  stocks={reversalSplitStocks.upToDown}
                  struckSet={upToDownStruck}
                  emptyText={
                    filteredUpToDown
                      ? '当日无升转降信号'
                      : `${upToDownAsOfDate.format('YYYY-MM-DD')} 当日无升转降信号`
                  }
                  onToggleStruck={toggleUpToDownStruck}
                  onFocusStock={(tc) => { stockNavSourceRef.current = 'nineturn'; focusStockInDetailList(tc); }}
                />
                <ReversalSignalTable
                  title="降转升"
                  titleClassName="text-amber-400"
                  signalType="down_to_up"
                  signalDate={reversalSplitStocks.date || upToDownAsOfDate.format('YYYYMMDD')}
                  monthStr={monthStr}
                  stocks={reversalSplitStocks.downToUp}
                  struckSet={upToDownStruck}
                  emptyText={
                    filteredUpToDown
                      ? '当日无降转升信号'
                      : `${upToDownAsOfDate.format('YYYY-MM-DD')} 当日无降转升信号`
                  }
                  onToggleStruck={toggleUpToDownStruck}
                  onFocusStock={(tc) => { stockNavSourceRef.current = 'nineturn'; focusStockInDetailList(tc); }}
                />
              </div>
            ) : (
              !loadingUpToDownDaily && (
                <div className="text-xs text-tertiary-text py-2">本月暂无升转降或降转升信号</div>
              )
            )}
          </Card>
          </div>
        )}

        {activeRecommend && brokerGroups.size > 0 && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">
              {viewMode === 'broker' ? '券商金股明细' : '全部金股明细'}
            </div>

            {/* Stock view: flat table with inline expandable rows */}
            {viewMode === 'stock' && (
              <Table
                key={tableKey}
                columns={stockColumns}
                dataSource={stockRows}
                rowKey="ts_code"
                size="small"
                pagination={false}
                scroll={{ x: 700 }}
                onRow={(record) => {
                  const style = brokerStockRowStyle(record);
                  return {
                    id: `broker-stock-row-${record.ts_code}`,
                    ...(style ? { style } : {}),
                  };
                }}
                onChange={(_pagination, _filters, sorter) => {
                  if (!Array.isArray(sorter) && sorter.columnKey) {
                    setTableSort({ columnKey: sorter.columnKey as string, order: sorter.order as 'ascend' | 'descend' });
                  }
                }}
                expandable={{
                  defaultExpandedRowKeys: expandedKey ? [expandedKey] : [],
                  onExpand: (expanded: boolean, record: any) => {
                    const code = String(record.ts_code);
                    if (expanded) {
                      if (expandedKeyRef.current && expandedKeyRef.current !== code) {
                        setExpandedKey(code);
                        setTableKey(k => k + 1);
                      } else {
                        setExpandedKey(code);
                      }
                      expandedKeyRef.current = code;
                    } else {
                      setExpandedKey('');
                      expandedKeyRef.current = '';
                      setTableKey(k => k + 1);
                    }
                  },
                  expandedRowRender: (record) => (
                    <div className="min-w-0 max-w-full overflow-hidden">
                      <StockHistoryExpandPanel
                        tsCode={record.ts_code}
                        name={record.name}
                        highlightMonth={monthStr}
                      />
                    </div>
                  ),
                  rowExpandable: () => true,
                }}
              />
            )}

            {/* Broker view: grouped by broker */}
            {viewMode === 'broker' && (
              <div className="space-y-2">
                {Array.from(brokerGroups.entries())
                  .sort(([, aItems], [, bItems]) => {
                    const aBt = activeBacktest?.brokers.find(b => b.broker === aItems[0]?.broker);
                    const bBt = activeBacktest?.brokers.find(b => b.broker === bItems[0]?.broker);
                    return (bBt?.cumulative_return ?? -Infinity) - (aBt?.cumulative_return ?? -Infinity);
                  })
                  .map(([broker, items], idx) => {
                  const brokerBt = activeBacktest?.brokers.find(b => b.broker === broker);
                  const brokerRows: StockRow[] = items.map(item => {
                    const stockRet = activeBacktest?.stock_returns?.find(
                      s => s.ts_code === item.ts_code
                    );
                    const cumRet = stockRet?.daily_returns?.length
                      ? stockRet.daily_returns[stockRet.daily_returns.length - 1].cumulative
                      : undefined;
                    const row: StockRow = {
                      ts_code: item.ts_code,
                      name: item.name,
                      broker_count: item.broker_count,
                      isConsecutive: consecutiveSet.has(item.ts_code),
                      dailyChange: stockRet?.daily_change,
                      endPrice: stockRet?.end_price,
                      endDate: stockRet?.end_date,
                      cumRet,
                      nineturn: activeEnrichment?.data[item.ts_code]?.nineturn ?? null,
                      forecast: activeEnrichment?.data[item.ts_code]?.forecast ?? null,
                      cyq_perf: activeEnrichment?.data[item.ts_code]?.cyq_perf ?? null,
                      sector: activeEnrichment?.data[item.ts_code]?.sector ?? null,
                    };
                    const hist = historicalStats[item.ts_code];
                    if (hist) {
                      row.historyMonthCount = hist.month_count;
                      row.historyPeriodCount = hist.period_count;
                      row.historyWinRate = hist.win_rate ?? null;
                      row.historyMaxReturn = hist.max_return ?? null;
                      row.historyMaxDrawdown = hist.max_drawdown ?? null;
                    }
                    if (historyTopCodes.has(item.ts_code)) {
                      row.isHistoryTop = true;
                    }
                    return row;
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
                        <span className="text-sm font-medium flex-1 text-left">
                          {broker}
                          {topBrokers.includes(broker) && (
                            <span className="ml-1.5 px-1 py-0.5 text-[10px] bg-yellow-500/20 text-yellow-400 rounded font-bold">历史 Top5</span>
                          )}
                        </span>
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
                          {activeBacktest ? (
                            <Table
                              key={tableKey}
                              columns={stockColumns}
                              dataSource={brokerRows}
                              rowKey="ts_code"
                              size="small"
                              pagination={false}
                              scroll={{ x: 700 }}
                              onRow={(record) => { const style = brokerStockRowStyle(record); return style ? { style } : {}; }}
                              onChange={(_pagination, _filters, sorter) => {
                                if (!Array.isArray(sorter) && sorter.columnKey) {
                                  setTableSort({ columnKey: sorter.columnKey as string, order: sorter.order as 'ascend' | 'descend' });
                                }
                              }}
                              expandable={{
                                defaultExpandedRowKeys: expandedKey ? [expandedKey] : [],
                                onExpand: (expanded: boolean, record: any) => {
                                  const code = String(record.ts_code);
                                  if (expanded) {
                                    if (expandedKeyRef.current && expandedKeyRef.current !== code) {
                                      setExpandedKey(code);
                                      setTableKey(k => k + 1);
                                    } else {
                                      setExpandedKey(code);
                                    }
                                    expandedKeyRef.current = code;
                                  } else {
                                    setExpandedKey('');
                                    expandedKeyRef.current = '';
                                    setTableKey(k => k + 1);
                                  }
                                },
                                expandedRowRender: (record) => (
                                  <div className="min-w-0 max-w-full overflow-hidden">
                                    <StockHistoryExpandPanel
                                      tsCode={record.ts_code}
                                      name={record.name}
                                      highlightMonth={monthStr}
                                    />
                                  </div>
                                ),
                                rowExpandable: () => true,
                              }}
                            />
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
                          {activeBacktest && brokerBt && brokerBt.daily_returns.length > 0 && (() => {
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
            label: '有记录以来',
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
              <div className="text-xs text-secondary-text">最优累计收益</div>
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
            <div className="text-sm font-medium mb-2">有记录以来 Top 5 券商累计收益</div>
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
          {
            key: 'strategy',
            label: '策略回测',
            children: (
              <div className="space-y-4 pt-2">
        <Card className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2">
              <label className="text-sm text-secondary-text">回测区间</label>
              <RangePicker
                picker="month"
                locale={zhCN.DatePicker}
                value={[strategyStartMonth, strategyEndMonth]}
                onChange={(vals) => {
                  if (!vals?.[0] || !vals[1]) return;
                  const [start, end] = vals;
                  const nextEnd = end.isBefore(start, 'month') ? start : end;
                  strategyLoadedKeyRef.current = null;
                  setStrategyData(null);
                  setStrategyStartMonth(start);
                  setStrategyEndMonth(nextEnd);
                  setStrategyFetchTrigger((t) => t + 1);
                }}
                allowClear={false}
                disabledDate={(d) => (
                  d.isAfter(dayjs(), 'month') || d.isBefore(dayjs('2020-03-01'), 'month')
                )}
                className="h-9"
              />
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={reloadStrategyData}
              disabled={strategyLoading}
            >
              {strategyLoading ? (
                <Loader2 className="h-4 w-4 animate-spin mr-1" />
              ) : (
                <RefreshCw className="h-4 w-4 mr-1" />
              )}
              重新计算
            </Button>
            {strategyPeriodLabel ? (
              <span className="text-xs text-tertiary-text">当前：{strategyPeriodLabel}</span>
            ) : null}
          </div>
        </Card>

        {/* Strategy Loading */}
        {strategyLoading && (
          <Card className="p-4 text-center text-sm text-tertiary-text">
            <Loader2 className="h-4 w-4 animate-spin inline mr-2" />
            策略计算中...
          </Card>
        )}

        {/* Strategy Overview */}
        {strategyData && !strategyLoading && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <Card className="p-3 text-center">
              <div className={`text-lg font-bold ${(strategyData.cumulative_return ?? 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                {fmtPct(strategyData.cumulative_return)}
              </div>
              <div className="text-xs text-secondary-text">策略累计收益</div>
            </Card>
            <Card className="p-3 text-center">
              <div className="text-lg font-bold">{strategyData.total_months}</div>
              <div className="text-xs text-secondary-text">活跃月份数</div>
            </Card>
            <Card className="p-3 text-center">
              <div className={`text-lg font-bold ${(strategyAvgMonthlyReturn ?? 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                {strategyAvgMonthlyReturn != null ? `${strategyAvgMonthlyReturn >= 0 ? '+' : ''}${(strategyAvgMonthlyReturn * 100).toFixed(2)}%` : '--'}
              </div>
              <div className="text-xs text-secondary-text">月均收益</div>
            </Card>
            <Card className="p-3 text-center">
              <div className={`text-lg font-bold ${(strategyMonthlyWinRate ?? 0) >= 0.5 ? 'text-red-400' : 'text-emerald-400'}`}>
                {strategyMonthlyWinRate != null ? `${(strategyMonthlyWinRate * 100).toFixed(0)}%` : '--'}
              </div>
              <div className="text-xs text-secondary-text">月度胜率</div>
            </Card>
          </div>
        )}

        {/* Strategy Chart */}
        {strategyData && strategyChartData.length > 0 && !strategyLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-2">
              九转选股等权策略累计收益
              {strategyPeriodLabel ? (
                <span className="text-tertiary-text font-normal ml-2">{strategyPeriodLabel}</span>
              ) : null}
            </div>
            <div className="text-xs text-tertiary-text mb-1">
              总资金固定：当日收盘升 1..8 转降 N 股 T+1 开盘均摊买入（均须落在当月）；T+1 买入后 T+2 开盘亏损则 T+2 开盘卖、盈利则自 T+3 起每日收盘评估：T+3 收盘超买入日收盘价则继续持有，直至某日收盘低于买入开盘价再收盘卖出；月末最后交易日收盘强制清仓（无行情则顺延开盘清仓，可跨月）；升 9+ 转降忽略；末交易日升转降忽略；当日无有效升转降则 T+1 开盘清仓后暂停；总收益按结算资产相对固定总资金计算
            </div>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={strategyChartData} margin={{ top: 4, right: 0, bottom: 6, left: -20 }}>
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#9ca3af' }} stroke="#6b7280" />
                <YAxis
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  stroke="#6b7280"
                  tickFormatter={v => `${(v * 100).toFixed(0)}%`}
                />
                <Tooltip content={<CustomTooltip />} />
                <ReferenceLine y={0} stroke="#4b5563" strokeWidth={1} strokeDasharray="4 4" />
                <Line
                  type="monotone"
                  dataKey="cumulative"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  dot={false}
                  connectNulls
                />
              </LineChart>
            </ResponsiveContainer>
          </Card>
        )}

        {/* Multi-Curve Comparison: rank2 vs rank24 */}
        {strategyData && multiCurveData.length > 0 && !strategyLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-2">第2顺位单独 / 第2+4顺位等权 收益对比</div>
            <div className="text-xs text-tertiary-text mb-1">
              同一评分体系下，每月单独买入第2顺位 vs 等权买入第2+4顺位，按月复合
            </div>
            <div className="flex flex-wrap gap-x-3 gap-y-1 mb-1">
              {Object.keys(multiCurveColors).map(k => (
                <span key={k} className="inline-flex items-center gap-1 text-xs text-secondary-text">
                  <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: multiCurveColors[k] }} />
                  {multiCurveLabels[k] || k}
                </span>
              ))}
            </div>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={multiCurveData} margin={{ top: 4, right: 0, bottom: 6, left: -20 }}>
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#9ca3af' }} stroke="#6b7280" />
                <YAxis
                  tick={{ fontSize: 10, fill: '#9ca3af' }}
                  stroke="#6b7280"
                  tickFormatter={v => `${(v * 100).toFixed(0)}%`}
                />
                <Tooltip content={<CustomTooltip />} />
                <ReferenceLine y={0} stroke="#4b5563" strokeWidth={1} strokeDasharray="4 4" />
                {Object.keys(multiCurveColors).map(k => (
                  <Line
                    key={k}
                    type="monotone"
                    dataKey={k}
                    stroke={multiCurveColors[k]}
                    strokeWidth={1.5}
                    dot={false}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </Card>
        )}

        {/* Up-to-down trade stats by prev up_count */}
        {strategyData && (strategyData.up_to_down_stats?.length ?? 0) > 0 && !strategyLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">升转降分档统计</div>
            <div className="text-xs text-tertiary-text mb-2">
              按信号日前日上升计数（升 1..8 转降）汇总每笔 T+1 买 / T+2 卖交易的平均收益与胜率
            </div>
            <Table
              dataSource={(strategyData.up_to_down_stats ?? []).map((r) => ({
                key: r.up_count,
                up_count: r.up_count,
                trade_count: r.trade_count,
                avg_return: r.avg_return,
                win_rate: r.win_rate,
              }))}
              columns={[
                { title: '信号', dataIndex: 'up_count', key: 'up_count', width: 90, render: (v: number) => (
                  <span className="text-xs font-medium">升{v}转降</span>
                )},
                { title: '交易次数', dataIndex: 'trade_count', key: 'trade_count', render: (v: number) => (
                  <span className="text-xs text-tertiary-text">{v}</span>
                )},
                { title: '平均收益', dataIndex: 'avg_return', key: 'avg_return', render: (v: number, row: { trade_count: number }) => (
                  <span className={`text-xs font-medium ${row.trade_count === 0 ? 'text-tertiary-text' : v >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                    {row.trade_count === 0 ? '--' : fmtPct(v)}
                  </span>
                )},
                { title: '胜率', dataIndex: 'win_rate', key: 'win_rate', render: (v: number, row: { trade_count: number }) => (
                  <span className={`text-xs font-medium ${row.trade_count === 0 ? 'text-tertiary-text' : (v ?? 0) >= 0.5 ? 'text-red-400' : 'text-emerald-400'}`}>
                    {row.trade_count === 0 ? '--' : `${(v * 100).toFixed(0)}%`}
                  </span>
                )},
              ]}
              size="small"
              pagination={false}
            />
          </Card>
        )}

        {/* Strategy Rank Stats */}
        {strategyData && (strategyData.rank_stats?.length ?? 0) > 0 && !strategyLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">各顺位历史收益贡献</div>
            <div className="text-xs text-tertiary-text mb-2">
              每月按评分选股 Top {strategyData.top_n ?? 4}，统计每个顺位在所有月份的平均收益与活跃月份数
            </div>
            <Table
              dataSource={(strategyData.rank_stats ?? []).map(r => ({
                key: r.rank,
                rank: r.rank,
                avg_return: r.avg_return,
                month_count: r.month_count,
                win_rate: r.win_rate,
              }))}
              columns={[
                { title: '顺位', dataIndex: 'rank', key: 'rank', width: 60, render: (v: number) => <span className="text-xs font-medium">#{v}</span> },
                { title: '平均收益', dataIndex: 'avg_return', key: 'avg_return', render: (v: number) => (
                  <span className={`text-xs font-medium ${v >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(v)}</span>
                )},
                { title: '胜率', dataIndex: 'win_rate', key: 'win_rate', render: (v: number) => (
                  <span className={`text-xs font-medium ${(v ?? 0) >= 0.5 ? 'text-red-400' : 'text-emerald-400'}`}>{v != null ? `${(v * 100).toFixed(0)}%` : '--'}</span>
                )},
                { title: '活跃月份数', dataIndex: 'month_count', key: 'month_count', render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span> },
              ]}
              size="small"
              pagination={false}
            />
          </Card>
        )}

        {/* Strategy Monthly Table */}
        {strategyData && !strategyLoading && (
          <Card className="p-4">
            <div className="text-sm font-medium mb-3">策略月度表现</div>
            <Table
              dataSource={[...(strategyData.monthly_returns ?? [])]
                .reverse()
                .map((m, i) => ({
                  key: m.month,
                  rank: i + 1,
                  month: m.month,
                  month_return: m.month_return,
                  cumulative_return: m.cumulative_return,
                  stock_count: m.stock_count,
                  best_up_to_down: bestUpToDownSignal(m.stocks),
                  stocks: m.stocks,
                }))}
              columns={[
                { title: '#', dataIndex: 'rank', key: 'rank', width: 40, render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span> },
                { title: '月份', dataIndex: 'month', key: 'month', render: (v: string) => <span className="text-xs">{fmtDate(v).slice(0, 7)}</span> },
                { title: '月收益', dataIndex: 'month_return', key: 'month_return', render: (v: number) => (
                  <span className={`text-xs font-medium ${v >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(v)}</span>
                )},
                { title: '月最佳信号', dataIndex: 'best_up_to_down', key: 'best_up_to_down', render: (v: BestUpToDownSignal | null) => (
                  v ? (
                    <div className="flex flex-col gap-0.5">
                      <span className="text-xs font-medium text-secondary-text">{v.label}</span>
                      <span className={`text-xs font-medium ${v.returnPct >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                        {fmtPct(v.returnPct)}
                      </span>
                    </div>
                  ) : (
                    <span className="text-xs text-tertiary-text">--</span>
                  )
                )},
                { title: '累计收益', dataIndex: 'cumulative_return', key: 'cumulative_return', render: (v: number) => (
                  <span className={`text-xs font-medium ${v >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{fmtPct(v)}</span>
                )},
                { title: '选股数', dataIndex: 'stock_count', key: 'stock_count', render: (v: number) => <span className="text-xs text-tertiary-text">{v}</span> },
              ]}
              size="small"
              pagination={false}
              expandable={{
                rowExpandable: (r: any) => (r.stocks?.length ?? 0) > 0,
                expandedRowRender: (record: any) => {
                  const stocks = [...(record.stocks || [])].sort(
                    (a, b) => String(a.buy_date ?? '').localeCompare(String(b.buy_date ?? '')),
                  );
                  if (!stocks.length) return null;
                  return (
                    <Table
                      dataSource={stocks.map((s: any) => ({
                        key: `${s.ts_code}-${s.buy_date ?? ''}`,
                        ts_code: s.ts_code,
                        name: s.name,
                        month_return: s.month_return,
                        buy_date: s.buy_date,
                        sell_date: s.sell_date,
                        buy_price: s.buy_price,
                        buy_amount: s.buy_amount,
                        sell_price: s.sell_price,
                        sell_amount: s.sell_amount,
                        buy_reason: s.buy_reason,
                        sell_reason: s.sell_reason,
                      }))}
                      columns={[
                        { title: '代码', dataIndex: 'ts_code', key: 'ts_code', width: 100, render: (v: string) => <span className="text-xs font-mono text-secondary-text">{v}</span> },
                        { title: '名称', dataIndex: 'name', key: 'name', render: (v: string) => <span className="text-xs">{v}</span> },
                        { title: '买入日', dataIndex: 'buy_date', key: 'buy_date', render: (v: string | null, row: any) => (
                          v
                            ? <StrategyTradeReasonTooltip label={fmtDate(v)} reason={row.buy_reason} />
                            : <span className="text-xs text-tertiary-text">--</span>
                        )},
                        { title: '买入价', dataIndex: 'buy_price', key: 'buy_price', render: (v: number | null) => (
                          <span className="text-xs font-mono text-secondary-text">{v != null ? v.toFixed(2) : '--'}</span>
                        )},
                        { title: '买入额', dataIndex: 'buy_amount', key: 'buy_amount', render: (v: number | null) => (
                          <span className="text-xs font-mono text-secondary-text">{v != null ? v.toFixed(2) : '--'}</span>
                        )},
                        { title: '卖出日', dataIndex: 'sell_date', key: 'sell_date', render: (v: string | null, row: any) => (
                          v
                            ? <StrategyTradeReasonTooltip label={fmtDate(v)} reason={row.sell_reason} />
                            : <span className="text-xs text-tertiary-text">--</span>
                        )},
                        { title: '卖出价', dataIndex: 'sell_price', key: 'sell_price', render: (v: number | null) => (
                          <span className="text-xs font-mono text-secondary-text">{v != null ? v.toFixed(2) : '--'}</span>
                        )},
                        { title: '卖出额', dataIndex: 'sell_amount', key: 'sell_amount', render: (v: number | null) => (
                          <span className="text-xs font-mono text-secondary-text">{v != null ? v.toFixed(2) : '--'}</span>
                        )},
                        { title: '持仓收益', dataIndex: 'month_return', key: 'month_return', render: (v: number | null) => (
                          <span className={`text-xs font-medium ${(v ?? 0) >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>{v != null ? fmtPct(v) : '--'}</span>
                        )},
                      ]}
                      size="small"
                      pagination={false}
                    />
                  );
                },
              }}
            />
          </Card>
        )}

        {/* Strategy Empty */}
        {!strategyData && !strategyLoading && (
          <Card className="p-4 text-center text-sm text-tertiary-text">
            暂无策略回测数据
          </Card>
        )}
              </div>
            ),
          },
        ]}
      />
      {showScrollTop && (
        <button
          type="button"
          onClick={scrollToTop}
          className="fixed bottom-6 right-6 z-50 flex h-10 w-10 items-center justify-center rounded-full border border-border/30 bg-card/90 shadow-lg backdrop-blur-sm transition-all hover:bg-card hover:border-border/50 translate-x-[calc(50%_+_3px)] cursor-pointer"
          aria-label="返回顶部"
        >
          <ChevronUp className="h-5 w-5 text-secondary-text" />
        </button>
      )}
    </AppPage>
  );
};

export default BrokerRecommendPage;
