import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Compass, RefreshCw, TrendingUp, TrendingDown,
  Loader2, ArrowUp, ArrowDown, Sparkles,
  ChevronDown, Target, Shield, Zap, Gauge, Download,
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { AutoComplete, DatePicker, Table, Segmented } from 'antd';
import dayjs from 'dayjs';
import { AppPage, Button, EmptyState } from '../components/common';
import { discoveryApi, type DiscoveryItem, type BacktestResponse, type TradeRecordItem, type ScanModeResponse, type StockScoreResponse, type FactorTopsResponse } from '../api/discovery';
import { stocksApi, type KLineItem } from '../api/stocks';
import { useStockIndex } from '../hooks/useStockIndex';
import { searchStocks } from '../utils/searchStocks';

type TabKey = 'intraday' | 'postmarket';

const MIN_INTRADAY_FETCH_GAP_MS = 60_000;
const BACKTEST_REFRESH_MS = 300_000;
const DISCOVERY_RANK_TOP_N = 4;  // 寻股回测固定展示 Top1~Top4 顺位统计

const getDefaultTabByCnMarketTime = (): TabKey => {
  const now = new Date();
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    hour12: false,
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
  }).formatToParts(now);

  const partMap: Record<string, string> = {};
  parts.forEach((p) => {
    if (p.type !== 'literal') partMap[p.type] = p.value;
  });

  const weekday = partMap.weekday;
  const hour = Number(partMap.hour ?? '0');
  const minute = Number(partMap.minute ?? '0');
  const minuteOfDay = hour * 60 + minute;

  // A-share session (CN): Mon-Fri, 09:15-15:00.
  const isWeekday = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday);
  const isIntraday = minuteOfDay >= (9 * 60 + 15) && minuteOfDay < (15 * 60);

  return isWeekday && isIntraday ? 'intraday' : 'postmarket';
};

/* ──────────────────────────────────────────────
   1. Score Ring — the 120% detail
   ────────────────────────────────────────────── */

const ScoreRing: React.FC<{ score: number }> = ({ score }) => {
  const size = 52;
  const stroke = 5;
  const r = (size - stroke) / 2;
  const circ = 2 * Math.PI * r;
  const [a, setA] = useState(0);

  useEffect(() => {
    const t = setTimeout(() => setA(score), 150);
    return () => clearTimeout(t);
  }, [score]);

  const progress = (a / 100) * circ;
  const hue = score >= 50 ? '193 100% 43%' : score >= 35 ? '37 92% 50%' : '224 12% 42%';
  const color = `hsl(${hue})`;

  return (
    <div className="relative shrink-0 select-none" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="-rotate-90">
        {/* track */}
        <circle cx={size / 2} cy={size / 2} r={r} fill="none"
          stroke="hsl(var(--border) / 0.35)" strokeWidth={stroke} />
        {/* glow */}
        <circle cx={size / 2} cy={size / 2} r={r} fill="none"
          stroke={color} strokeWidth={stroke + 4} strokeLinecap="round" opacity={0.18}
          strokeDasharray={`${circ} ${circ}`}
          strokeDashoffset={circ - progress}
          style={{ filter: `blur(3px)`, transition: 'stroke-dashoffset 0.9s cubic-bezier(0.4, 0, 0.2, 1)' }}
        />
        {/* arc */}
        <circle cx={size / 2} cy={size / 2} r={r} fill="none"
          stroke={color} strokeWidth={stroke} strokeLinecap="round"
          strokeDasharray={`${circ} ${circ}`}
          strokeDashoffset={circ - progress}
          style={{ transition: 'stroke-dashoffset 0.9s cubic-bezier(0.4, 0, 0.2, 1)' }}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-sm font-bold text-foreground leading-none tabular-nums">{score.toFixed(0)}</span>
        <span className="text-[9px] text-tertiary-text mt-0.5 tracking-wider">SCORE</span>
      </div>
    </div>
  );
};

/* ──────────────────────────────────────────────
   2. Factor Bar
   ────────────────────────────────────────────── */

const FACTOR_LABELS: Record<string, string> = {
  money_flow: '资金流向',
  margin: '融资融券',
  chip: '筹码分布',
  technical: '技术形态',
  limit: '涨跌停',
  fundamental: '基本面',
  northbound: '北向资金',
  institution_hold: '机构持股',
  profit_forecast: '盈利预测',
  buyback: '回购',
  insider_buy: '高管增持',
  broker_recommend: '券商推荐',
  popularity: '人气',
  hot_money: '游资',
  performance: '业绩',
  momentum: '动量',
  rebound: '反弹',
  sector: '板块',
  ma_entry: '均线',
  ranking_momentum: '排名动量',
  concept_heat: '概念热度',
  alpha042: '均值回归Alpha042',
  vwap_deviation: 'VWAP偏离',
  gap_reversal: '跳空反转',
  liquid_oversold: '流动性超卖',
  vwap_reversal: 'VWAP动量反转',
  gtja114: 'GTJA114',
  alpha60: 'Alpha60收盘位置',
  money_flow_osc: '资金流振荡',
};

const factorLabel = (key: string) => FACTOR_LABELS[key] || key;

const colorizeArrows = (text: string): React.ReactNode => {
  const parts = text.split(/([↑↓])/g);
  return parts.map((part, i) => {
    if (part === '↑') return <span key={i} style={{color: '#ef4444', fontWeight: 700}}>↑</span>;
    if (part === '↓') return <span key={i} style={{color: '#22c55e', fontWeight: 700}}>↓</span>;
    return part;
  });
};

const FactorBar: React.FC<{ label: string; value: number; pctShare: number }> = ({ label, value, pctShare }) => {
  const pct = Math.min(100, Math.max(0, value));
  const hue = pct >= 70 ? '193 100% 43%' : pct >= 40 ? '37 92% 50%' : '224 12% 42%';

  return (
    <div className="flex items-center gap-2.5 text-[11px]">
      <span className="w-28 shrink-0 text-tertiary-text text-right truncate" title={`${label} (${pctShare}%)`}>
        {label}<span className="text-tertiary-text/60 ml-0.5">({pctShare}%)</span>
      </span>
      <div className="flex-1 h-1 rounded-full bg-border/30 overflow-hidden">
        <motion.div
          className="h-full rounded-full"
          style={{ backgroundColor: `hsl(${hue})` }}
          initial={{ width: 0 }}
          animate={{ width: `${pct}%` }}
          transition={{ duration: 0.5, ease: 'easeOut' }}
        />
      </div>
      <span className="w-7 text-right font-semibold text-foreground/60 tabular-nums">{value.toFixed(0)}</span>
    </div>
  );
};

/* ──────────────────────────────────────────────
   3. Helpers
   ────────────────────────────────────────────── */

const fmtPx = (v: number | null | undefined) => v != null ? v.toFixed(2) : '--';
const fmtPct = (v: number | null) => (v != null ? `${v.toFixed(2)}%` : '--');

const calcBuyRef = (low: number | null | undefined, high: number | null | undefined): number | null => {
  if (low == null && high == null) return null;
  if (low != null && high != null) return (low + high) / 2;
  return low ?? high ?? null;
};

const calcPctFromBase = (base: number | null, target: number | null | undefined): number | null => {
  if (base == null || target == null || base <= 0) return null;
  return ((target - base) / base) * 100;
};

const calcPnLRatio = (profitPct: number | null, lossPct: number | null): number | null => {
  if (profitPct == null || lossPct == null || lossPct <= 0) return null;
  return profitPct / lossPct;
};

const getRefPrice = (item: DiscoveryItem): number | null => {
  if (item.price_at_discovery != null && item.price_at_discovery > 0) return item.price_at_discovery;
  return calcBuyRef(item.buy_price_low, item.buy_price_high);
};

const chCfg = (c?: string) => {
  switch (c) {
    case 'new': return { icon: <Sparkles className="h-3 w-3" />, label: '新进', cls: 'text-cyan bg-cyan/8 border-cyan/15' };
    case 'up': return { icon: <ArrowUp className="h-3 w-3" />, label: '上升', cls: 'text-red-400 bg-red-400/8 border-red-400/15' };
    case 'down': return { icon: <ArrowDown className="h-3 w-3" />, label: '下降', cls: 'text-emerald-400 bg-emerald-400/8 border-emerald-400/15' };
    default: return null;
  }
};

/* ──────────────────────────────────────────────
   4. Shared StockCard
   ────────────────────────────────────────────── */

const StockCard: React.FC<{
  item: DiscoveryItem;
  open: boolean;
  onToggle: () => void;
}> = ({ item, open, onToggle }) => {
  const ch = chCfg(item.change);
  const px = item.buy_price_low != null || item.stop_loss != null;
  const buyRange = item.buy_price_low != null
    ? `${fmtPx(item.buy_price_low)}${item.buy_price_high != null && item.buy_price_high !== item.buy_price_low ? ` - ${fmtPx(item.buy_price_high)}` : ''}`
    : '--';
  const keyReasons = item.reasons?.slice(0, 6) ?? [];
  const refPrice = getRefPrice(item);
  const profitPct = calcPctFromBase(refPrice, item.take_profit_1);
  const lossPctRaw = calcPctFromBase(refPrice, item.stop_loss);
  const lossPct = lossPctRaw != null ? Math.abs(lossPctRaw) : null;
  const pnlRatio = calcPnLRatio(profitPct, lossPct);

  return (
    <motion.div
      layout
      transition={{ type: 'spring', stiffness: 420, damping: 36 }}
      onClick={onToggle}
      className="group cursor-pointer overflow-hidden rounded-2xl border border-border/30 bg-card/70 transition-all duration-200 hover:border-cyan/30 hover:bg-card"
    >
      {/* ── Collapsed ── */}
      <div className="space-y-3 px-4 py-4 md:px-5">
        <div className="flex items-center gap-3.5">
          {/* Rank */}
          <div className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-xl text-sm font-bold
            ${item.rank <= 3
              ? 'bg-gradient-to-br from-cyan/15 to-cyan/3 text-cyan ring-1 ring-cyan/15'
              : 'bg-muted/30 text-secondary-text'
            }`}>
            {item.rank}
          </div>

          {/* Name */}
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-[15px] font-semibold tracking-tight text-foreground">{item.stock_code}</span>
              <span className="text-[13px] font-semibold">{item.stock_name}</span>
              {item.sector && (
                <span className="rounded-md border border-border/40 bg-muted/30 px-1.5 py-0.5 text-[10px] text-tertiary-text">{item.sector}</span>
              )}
              {ch && (
                <span className={`inline-flex items-center gap-1 rounded-lg border px-1.5 py-0.5 text-[11px] font-medium ${ch.cls}`}>
                  {ch.icon}{ch.label}
                </span>
              )}
              {item.recent_count != null && item.recent_count > 0 && (
                <span className="inline-flex items-center gap-1 rounded-md border border-cyan/25 bg-cyan/8 px-1.5 py-0.5 text-[10px] text-cyan font-medium">
                  近5日{item.recent_count}次
                </span>
              )}
              {item.discovered_at && (
                <span className="text-[15px] font-medium text-foreground">{item.discovered_at} 发现</span>
              )}
              {item.price_at_discovery != null && (
                <span className="text-[15px] font-medium text-foreground">· ¥{item.price_at_discovery.toFixed(2)}</span>
              )}
              {item.pct_chg != null && (
                <span className={`text-[13px] font-medium tabular-nums ml-0.5 ${item.pct_chg >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                  {item.pct_chg >= 0 ? '+' : ''}{item.pct_chg.toFixed(2)}%
                </span>
              )}
              {item.live_price != null && item.price_at_discovery != null && (
                <span className={`text-[15px] font-medium tabular-nums ${item.live_price >= item.price_at_discovery ? 'text-red-400' : 'text-emerald-400'}`}>
                  → ¥{item.live_price.toFixed(2)}
                </span>
              )}
              {item.live_price != null && item.price_at_discovery == null && (
                <span className="text-[15px] font-medium text-foreground">→ ¥{item.live_price.toFixed(2)}</span>
              )}
            </div>
          </div>

          {/* Score — composite_score (factor×0.3 + tech×0.7) when available, else fallback */}
          <ScoreRing score={item.composite_score && item.composite_score > 0 ? item.composite_score : (item.tech_score && item.tech_score > 0 ? item.tech_score : item.score)} />

          {/* Chevron */}
          <div className={`shrink-0 text-tertiary-text/50 transition-transform duration-200 ${open ? 'rotate-180' : ''}`}>
            <ChevronDown className="h-4 w-4" />
          </div>
        </div>

        {/* Prices: compact inline bar — clean minimal look */}
        {px && (
          <div className="flex flex-wrap items-center gap-x-5 gap-y-1.5 text-xs border-t border-border/10 pt-2.5">
            <span className="inline-flex items-center gap-1.5">
              <Target className="h-3 w-3 text-cyan/60" />
              <span className="text-tertiary-text">买入</span>
              <span className="font-semibold tabular-nums text-foreground">{buyRange}</span>
            </span>
            <span className="inline-flex items-center gap-1.5">
              <Zap className="h-3 w-3 text-red-400/60" />
              <span className="text-tertiary-text">止盈</span>
              <span className="font-semibold tabular-nums text-red-400">{fmtPx(item.take_profit_1)}</span>
            </span>
            <span className="inline-flex items-center gap-1.5">
              <Shield className="h-3 w-3 text-emerald-400/60" />
              <span className="text-tertiary-text">止损</span>
              <span className="font-semibold tabular-nums text-emerald-400">{fmtPx(item.stop_loss)}</span>
            </span>
            {pnlRatio != null && (
              <span className="text-tertiary-text">
                盈亏比 <span className="font-semibold text-foreground">{pnlRatio.toFixed(2)}:1</span>
              </span>
            )}
            {profitPct != null && (
              <span className="text-tertiary-text">
                预期盈利 <span className="font-semibold text-red-400">{fmtPct(profitPct)}</span>
              </span>
            )}
          </div>
        )}
      </div>

      {/* ── Expanded ── */}
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            key="detail"
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25, ease: 'easeInOut' }}
            className="overflow-hidden"
          >
            <div className="border-t border-border/20 bg-muted/10 px-4 pb-5 md:px-5">
              {/* Reasons */}
              {keyReasons.length > 0 && (
                <div className="mt-4 rounded-xl border border-border/30 bg-card/60 p-3.5">
                  <div className="mb-2.5 flex items-center gap-1.5 text-[11px] font-medium tracking-wide text-tertiary-text">
                    <Gauge className="h-3 w-3" /> 推荐理由
                  </div>
                  <div className="grid gap-2 sm:grid-cols-2">
                    {keyReasons.map((r, i) => (
                      <div key={i} className="rounded-lg border border-border/20 bg-foreground/[0.02] px-2.5 py-2 text-xs leading-5 text-secondary-text">
                        <span className="mr-1.5 text-cyan/80">#{i + 1}</span>
                        {colorizeArrows(r)}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Candlestick chart — 4-month daily K-line */}
              <div className="mt-4">
                <StockKLineChart
                  stockCode={item.stock_code}
                  height={200}
                  minHeight={200}
                />
              </div>

              {/* Grid: prices + factors */}
              <div className="mt-4 grid gap-4 sm:grid-cols-2">
                {item.factor_scores && Object.keys(item.factor_scores).length > 0 && (
                  <div className="space-y-2.5">
                    <div className="text-[11px] font-medium text-tertiary-text tracking-wide">因子得分</div>
                    {(() => {
                      const entries = Object.entries(item.factor_scores)
                        .filter(([, v]) => v > 0)
                        .sort(([a], [b]) => (item.factor_weights?.[b] ?? 0) - (item.factor_weights?.[a] ?? 0));
                      return entries.map(([k, v]) => (
                        <FactorBar key={k} label={factorLabel(k)} value={v} pctShare={item.factor_weights?.[k] ?? 0} />
                      ));
                    })()}
                  </div>
                )}

                {item.tech_score && item.tech_score > 0 && (
                  <div className="space-y-2.5">
                    <div className="text-[11px] font-medium text-tertiary-text tracking-wide">技术评分</div>
                    {(() => {
                      const techItems = [
                        ['赔率', item.rr_score ?? 0, 'rr_score'] as const,
                        ['大盘环境', item.market_score ?? 0, 'market_score'] as const,
                        ['板块强弱', item.sector_score ?? 0, 'sector_score'] as const,
                        ['量能质量', item.volume_score ?? 0, 'volume_score'] as const,
                        ['相对位置', item.position_score ?? 0, 'position_score'] as const,
                        ['形态确认', item.formation_score ?? 0, 'formation_score'] as const,
                      ];
                      techItems.sort((a, b) => (item.tech_score_weights?.[b[2]] ?? 0) - (item.tech_score_weights?.[a[2]] ?? 0));
                      return techItems.map(([label, val, weightKey]) => (
                        <FactorBar key={label} label={label} value={val} pctShare={item.tech_score_weights?.[weightKey] ?? 0} />
                      ));
                    })()}
                  </div>
                )}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
};

/* ──────────────────────────────────────────────
   6b. StockKLineChart — SVG candlestick chart with MA / BOLL overlay
   ────────────────────────────────────────────── */

const fmtDateShort = (s: string) => {
  const clean = s.replace(/-/g, '');
  if (clean.length >= 8) return `${clean.slice(0, 4)}-${clean.slice(4, 6)}-${clean.slice(6, 8)}`;
  return s;
};

const computeMA = (closes: number[], period: number): (number | null)[] =>
  closes.map((_, i) => {
    if (i < period - 1) return null;
    const slice = closes.slice(i - period + 1, i + 1);
    return slice.reduce((a, b) => a + b, 0) / period;
  });

const computeStd = (closes: number[], period: number): (number | null)[] =>
  closes.map((_, i) => {
    if (i < period - 1) return null;
    const slice = closes.slice(i - period + 1, i + 1);
    const mean = slice.reduce((a, b) => a + b, 0) / period;
    const variance = slice.reduce((a, b) => a + (b - mean) ** 2, 0) / period;
    return Math.sqrt(variance);
  });

type ActiveOverlay = 'ma5' | 'ma10' | 'ma20' | 'boll';

const StockKLineChart: React.FC<{ stockCode: string; height?: number; minHeight?: number }> = ({
  stockCode,
  height = 200,
  minHeight,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [width, setWidth] = useState(400);
  const [klines, setKlines] = useState<KLineItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeOverlays, setActiveOverlays] = useState<Set<ActiveOverlay>>(new Set(['boll']));
  const [period, setPeriod] = useState<'day' | 'week'>('day');

  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(entries => {
      for (const e of entries) setWidth(e.contentRect.width);
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    if (!stockCode) return;
    setLoading(true);
    stocksApi
      .getHistory(stockCode, 500)
      .then(data => { setKlines(data); setLoading(false); })
      .catch(() => setLoading(false));
  }, [stockCode]);

  const dailyRaw = klines.filter(d => d.open != null && d.high != null && d.low != null && d.close != null);

  const aggregateWeekly = (daily: typeof dailyRaw): typeof dailyRaw => {
    const weeks: Map<string, typeof daily> = new Map();
    daily.forEach(d => {
      const date = new Date(fmtDateShort(d.date));
      const jan1 = new Date(date.getFullYear(), 0, 1);
      const weekNum = Math.ceil(((date.getTime() - jan1.getTime()) / 86400000 + jan1.getDay() + 1) / 7);
      const key = `${date.getFullYear()}-W${String(weekNum).padStart(2, '0')}`;
      if (!weeks.has(key)) weeks.set(key, []);
      weeks.get(key)!.push(d);
    });
    return Array.from(weeks.values())
      .map(days => {
        const sorted = days.sort((a, b) => a.date.localeCompare(b.date));
        return {
          date: sorted[0].date,
          open: sorted[0].open!,
          close: sorted[sorted.length - 1].close!,
          high: Math.max(...sorted.map(d => d.high!)),
          low: Math.min(...sorted.map(d => d.low!)),
          volume: sorted.reduce((s, d) => s + (d.volume || 0), 0),
        };
      })
      .sort((a, b) => a.date.localeCompare(b.date));
  };

  const raw = period === 'week'
    ? aggregateWeekly(dailyRaw)
    : dailyRaw.slice(-100);

  // scroll to latest candle after data loads
  useEffect(() => {
    if (raw.length < 2 || !containerRef.current) return;
    requestAnimationFrame(() => {
      if (containerRef.current) {
        containerRef.current.scrollLeft = containerRef.current.scrollWidth;
      }
    });
  }, [raw]);
  if (raw.length < 2) {
    return (
      <div className="flex h-32 w-full items-center justify-center text-xs text-tertiary-text">
        {loading ? '加载中...' : '暂无数据'}
      </div>
    );
  }

  const pads = { t: 8, r: 8, b: 22, l: 48 };
  const count = raw.length;
  const candleStep = Math.max(24, Math.min(36, (width - pads.l - pads.r) / count));
  const candleW = Math.max(5, Math.min(16, candleStep * 0.55));
  const chartW = pads.l + count * candleStep + pads.r;
  const chartH = height - pads.t - pads.b;
  const dayToX = (i: number) => pads.l + i * candleStep + candleStep / 2;

  const closes = raw.map(d => d.close);
  const allPrices: number[] = [...closes];
  raw.forEach(d => { allPrices.push(d.high, d.low); });
  const priceMin = Math.min(...allPrices);
  const priceMax = Math.max(...allPrices);
  const margin = (priceMax - priceMin) * 0.08 || 1;
  const yMin = priceMin - margin;
  const yMax = priceMax + margin;
  const yRange = yMax - yMin || 1;
  const scaleY = (p: number) => pads.t + chartH * (1 - (p - yMin) / yRange);

  const gridLines = 4;
  const yTicks: number[] = [];
  for (let i = 1; i < gridLines; i++) yTicks.push(yMax - (yRange * i) / gridLines);

  const xLabelInterval = Math.max(Math.ceil(count / 8), 1);
  const xLabels: Array<{ x: number; label: string }> = [];
  for (let i = 0; i < count; i += xLabelInterval) {
    xLabels.push({ x: dayToX(i), label: fmtDate(raw[i].date).slice(5) });
  }
  const lastIdx = count - 1;
  if (xLabels.length === 0 || fmtDateShort(raw[lastIdx].date) !== xLabels[xLabels.length - 1].label) {
    xLabels.push({ x: dayToX(lastIdx), label: fmtDateShort(raw[lastIdx].date).slice(5) });
  }

  // MA / BOLL
  const ma5 = computeMA(closes, 5);
  const ma10 = computeMA(closes, 10);
  const ma20 = computeMA(closes, 20);
  const std20 = computeStd(closes, 20);
  const bollUpper = ma20.map((v, i) => (v != null && std20[i] != null ? v + 2 * std20[i] : null));
  const bollLower = ma20.map((v, i) => (v != null && std20[i] != null ? v - 2 * std20[i] : null));

  const makePolyline = (
    vals: (number | null)[],
    color: string,
    dashed?: boolean,
  ) => {
    const pts = vals
      .map((v, i) => (v != null ? `${dayToX(i)},${scaleY(v)}` : null))
      .filter(Boolean)
      .join(' ');
    return pts ? (
      <polyline points={pts} fill="none" stroke={color} strokeWidth={1.5}
        strokeDasharray={dashed ? '5 3' : undefined}
        strokeLinejoin="round" strokeLinecap="round" />
    ) : null;
  };

  const overlayBtns: Array<{ key: 'clear' | ActiveOverlay; label: string; color?: string }> = [
    { key: 'clear', label: '纯K' },
    { key: 'ma5', label: 'MA5', color: 'var(--amber-400)' },
    { key: 'ma10', label: 'MA10', color: 'var(--blue-400)' },
    { key: 'ma20', label: 'MA20', color: 'var(--purple-400)' },
    { key: 'boll', label: 'BOLL', color: 'var(--pink-400)' },
  ];

  return (
    <div className="mt-4">
      <div className="mb-1.5 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1.5 text-[11px] font-medium text-tertiary-text tracking-wide">
            <TrendingUp className="h-3 w-3" /> {period === 'day' ? '日线' : '周线'}
          </div>
          <div className="flex rounded border border-border/40 overflow-hidden text-[10px]">
            <button
              onClick={(e) => { e.stopPropagation(); setPeriod('day'); }}
              className={`px-1.5 py-0.5 font-medium transition-colors ${period === 'day' ? 'bg-primary/20 text-primary' : 'text-tertiary-text hover:text-primary'}`}
            >日</button>
            <button
              onClick={(e) => { e.stopPropagation(); setPeriod('week'); }}
              className={`px-1.5 py-0.5 font-medium transition-colors ${period === 'week' ? 'bg-primary/20 text-primary' : 'text-tertiary-text hover:text-primary'}`}
            >周</button>
          </div>
        </div>
        <div className="flex gap-1">
          {overlayBtns.map(({ key, label, color }) => {
            const isActive = key === 'clear' ? activeOverlays.size === 0 : activeOverlays.has(key as ActiveOverlay);
            return (
              <button
                key={key}
                onClick={(e) => {
                  e.stopPropagation();
                  if (key === 'clear') {
                    setActiveOverlays(new Set());
                  } else {
                    setActiveOverlays(prev => {
                      const next = new Set(prev);
                      next.has(key as ActiveOverlay) ? next.delete(key as ActiveOverlay) : next.add(key as ActiveOverlay);
                      return next;
                    });
                  }
                }}
                className={`rounded px-1.5 py-0.5 text-[10px] font-medium transition-colors ${
                  isActive
                    ? 'bg-primary/20 text-primary ring-1 ring-primary/40'
                    : 'text-tertiary-text hover:text-primary'
                }`}
                style={isActive && color ? { color } : undefined}
              >
                {label}
              </button>
            );
          })}
        </div>
      </div>
      <div ref={containerRef} className="relative w-full overflow-x-auto rounded-lg border border-border/20 bg-card"
        style={minHeight ? { minHeight } : undefined}>
        <svg
          ref={svgRef}
          width={chartW} height={height}
          style={{ display: 'block' }}
          onMouseMove={(e) => {
            const rect = svgRef.current?.getBoundingClientRect();
            if (!rect) return;
            const mx = e.clientX - rect.left;
            const idx = Math.round((mx - pads.l - candleStep / 2) / candleStep);
            if (idx >= 0 && idx < count) setHoverIdx(idx);
            else setHoverIdx(null);
          }}
          onMouseLeave={() => setHoverIdx(null)}
        >
          {/* Grid */}
          {yTicks.map((y, i) => (
            <line key={i} x1={pads.l} y1={scaleY(y)} x2={chartW - pads.r} y2={scaleY(y)}
              stroke="var(--border)" strokeWidth={0.5} strokeDasharray="3 3" opacity={0.6} />
          ))}

          {/* MA / BOLL overlays */}
          {activeOverlays.has('ma5') && makePolyline(ma5, '#f59e0b', true)}
          {activeOverlays.has('ma10') && makePolyline(ma10, '#60a5fa', true)}
          {activeOverlays.has('ma20') && makePolyline(ma20, '#a78bfa', true)}
          {activeOverlays.has('boll') && makePolyline(ma20, '#a78bfa', true)}
          {activeOverlays.has('boll') && makePolyline(bollUpper, '#ec4899', true)}
          {activeOverlays.has('boll') && makePolyline(bollLower, '#ec4899', true)}

          {/* Candles */}
          {raw.map((d, i) => {
            const x = dayToX(i);
            const isUp = d.close >= d.open;
            const color = isUp ? '#ef4444' : '#10b981';
            const bodyTop = scaleY(Math.max(d.open, d.close));
            const bodyBot = scaleY(Math.min(d.open, d.close));
            const bodyH = Math.max(1, bodyBot - bodyTop);
            const wickTop = scaleY(d.high);
            const wickBot = scaleY(d.low);
            const isHovered = hoverIdx === i;
            return (
              <g key={i}>
                {/* Wick */}
                <line x1={x} y1={wickTop} x2={x} y2={wickBot} stroke={color} strokeWidth={1} />
                {/* Body */}
                <rect
                  x={x - candleW / 2} y={bodyTop}
                  width={candleW} height={bodyH}
                  fill={isHovered ? (isUp ? '#ff6b6b' : '#34d399') : color}
                  rx={0.5}
                  style={{ cursor: 'crosshair' }}
                />
              </g>
            );
          })}

          {/* Y-axis labels */}
          {yTicks.map((y, i) => (
            <text key={i} x={pads.l - 4} y={scaleY(y) + 4}
              textAnchor="end" fill="var(--text-muted-text)" fontSize={10}>
              {y.toFixed(2)}
            </text>
          ))}

          {/* X-axis labels */}
          {xLabels.map((l, i) => (
            <text key={i} x={l.x} y={height - 6}
              textAnchor="middle" fill="var(--text-muted-text)" fontSize={10}>
              {l.label}
            </text>
          ))}

          {/* Crosshair */}
          {hoverIdx !== null && (
            <line x1={dayToX(hoverIdx)} y1={pads.t} x2={dayToX(hoverIdx)} y2={pads.t + chartH}
              stroke="var(--text-muted-text)" strokeWidth={1} strokeDasharray="2 3" opacity={0.45} />
          )}

          {/* Hover tooltip */}
          {hoverIdx !== null && (() => {
            const d = raw[hoverIdx];
            const x = dayToX(hoverIdx);
            const changePct = d.change_percent != null
              ? d.change_percent
              : ((d.close - d.open) / d.open * 100);
            const chgSign = changePct >= 0 ? '+' : '';
            const chgColor = changePct >= 0 ? '#ef4444' : '#10b981';
            const tw = 150, th = 82, rowH = 13;
            const tx = Math.max(pads.l + 2, Math.min(chartW - pads.r - tw - 2, x - tw / 2));
            const ty = pads.t + 4;
            const lx = tx + 10;
            const rx = tx + tw - 10;
            const row = (i: number) => ty + 17 + i * rowH;
            const rows: Array<[string, string, string | undefined]> = [
              ['开盘', d.open.toFixed(2), d.open >= (d.close ?? d.open) ? '#10b981' : '#ef4444'],
              ['最高', d.high.toFixed(2), '#ef4444'],
              ['最低', d.low.toFixed(2), '#10b981'],
              ['收盘', d.close.toFixed(2), (d.close ?? 0) >= (d.open ?? 0) ? '#ef4444' : '#10b981'],
              ['涨跌幅', `${chgSign}${changePct.toFixed(2)}%`, chgColor],
            ];
            return (
              <g>
                <rect x={tx} y={ty} width={tw} height={th} rx={5}
                  fill="var(--surface-2)" stroke="var(--border-default)" strokeWidth={0.5} />
                <text x={tx + tw / 2} y={ty + 14}
                  textAnchor="middle" fill="var(--text-primary)" fontSize={10.5} fontWeight={600}>
                  {fmtDateShort(d.date)}
                </text>
                {rows.map(([label, value, color], ri) => (
                  <g key={label}>
                    <text x={lx} y={row(ri)}
                      fill="var(--text-secondary-text)" fontSize={10}>{label}</text>
                    <text x={rx} y={row(ri)}
                      textAnchor="end" fill={color ?? 'var(--text-secondary-text)'} fontSize={10}
                      fontFamily="monospace">{value}</text>
                  </g>
                ))}
              </g>
            );
          })()}
        </svg>
      </div>
    </div>
  );
};


/* ──────────────────────────────────────────────
   7. Backtest Card
   ────────────────────────────────────────────── */


function tradesWithEffectivePickRank(trades: TradeRecordItem[], topN: number) {
  const groups = new Map<string, TradeRecordItem[]>();
  for (const t of trades) {
    const key = String(t.buy_date).slice(0, 8);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)!.push(t);
  }
  const out: Array<(typeof trades)[0] & { effective_pick_rank: number }> = [];
  for (const group of groups.values()) {
    let order = 0;
    for (const t of group) {
      order += 1;
      let pr = 0;
      if (t.pick_rank && t.pick_rank >= 1 && t.pick_rank <= topN) {
        pr = t.pick_rank;
      } else if (!t.pick_rank) {
        pr = Math.min(order, topN);
      }
      if (pr >= 1 && pr <= topN) {
        out.push({ ...t, effective_pick_rank: pr });
      }
    }
  }
  return out;
}

const fmtWan = (v: number) => `${(v / 10000).toFixed(1)}万`;
const fmtDate = (s: string) => {
  const clean = s.replace(/-/g, '');
  const d = `${clean.slice(0, 4)}-${clean.slice(4, 6)}-${clean.slice(6, 8)}`;
  return clean.length > 8 ? d + clean.slice(8) : d;
};

/** Portfolio candlestick chart — SVG-based, Y-axis shows returns %. */
const PortfolioCandleChart: React.FC<{
  data: Array<{ date: string; capital: number; open?: number; high?: number; low?: number; close?: number }>;
  initCapital: number;
  height?: number;
}> = ({ data, initCapital, height = 200 }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [width, setWidth] = useState(400);

  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(entries => {
      for (const e of entries) setWidth(e.contentRect.width);
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  const raw = data.filter(d => d.open != null && d.high != null && d.low != null && d.close != null);
  if (raw.length < 1) return null;

  // Convert to returns % relative to first day's open
  const base = raw[0].open!;
  const toPct = (v: number) => (v - base) / base * 100;
  const ohlcData = raw.map(d => ({
    date: d.date,
    capital: d.capital,
    open: toPct(d.open!),
    high: toPct(d.high!),
    low: toPct(d.low!),
    close: toPct(d.close!),
  }));

  const pads = { t: 10, r: 8, b: 22, l: 48 };
  const count = ohlcData.length;
  const candleStep = 28;
  const candleW = 8;
  // Chart grows with data; scrolls horizontally when wider than container
  const chartW = Math.max(width, pads.l + count * candleStep + pads.r);
  const chartH = height - pads.t - pads.b;
  const dayToX = (i: number) => pads.l + i * candleStep;

  const allPct: number[] = [];
  ohlcData.forEach(d => { allPct.push(d.high, d.low, d.open, d.close); });
  allPct.push(0); // baseline
  const pctMin = Math.min(...allPct);
  const pctMax = Math.max(...allPct);
  const margin = (pctMax - pctMin) * 0.12 || 0.1;
  const yMin = pctMin - margin;
  const yMax = pctMax + margin;
  const yRange = yMax - yMin || 1;
  const scaleY = (p: number) => pads.t + chartH * (1 - (p - yMin) / yRange);

  const fmtPct = (v: number) => {
    if (Math.abs(v) < 0.005) return '0%';
    return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
  };

  const gridLines = 4;
  const yTicks: number[] = [];
  for (let i = 1; i < gridLines; i++) yTicks.push(yMax - (yRange * i) / gridLines);
  // X-axis labels: at candle positions, every Nth candle to avoid overlap
  const xLabelInterval = Math.max(Math.ceil(50 / candleStep), 1);
  const xLabels: Array<{ x: number; label: string }> = [];
  const fmtXLabel = (d: string) => {
    const datePart = d.split(' ')[0]; // "20260508" or "2026-05-08"
    if (datePart.includes('-')) return datePart.slice(5).replace('-', '/');
    return datePart.slice(4, 6) + '/' + datePart.slice(6, 8);
  };
  for (let i = 0; i < count; i += xLabelInterval) {
    xLabels.push({ x: dayToX(i), label: fmtXLabel(ohlcData[i].date) });
  }
  // Always include last
  const lastIdx = count - 1;
  if (xLabels.length === 0 || fmtXLabel(ohlcData[lastIdx].date) !== xLabels[xLabels.length - 1].label) {
    xLabels.push({ x: dayToX(lastIdx), label: fmtXLabel(ohlcData[lastIdx].date) });
  }

  return (
    <div ref={containerRef} style={{ position: 'relative', width: '100%', overflowX: 'auto' }}>
      <svg width={chartW} height={height} style={{ display: 'block' }}>
        {/* Grid */}
        {yTicks.map((v, i) => {
          const y = scaleY(v);
          return (
            <g key={`g-${i}`}>
              <line x1={pads.l} x2={chartW - pads.r} y1={y} y2={y} stroke="hsl(var(--border))" strokeWidth={0.5} opacity={0.4} />
              <text x={pads.l - 14} y={y + 3} textAnchor="end" fill="hsl(var(--muted-foreground))" fontSize={9} fontFamily="monospace">
                {fmtPct(v)}
              </text>
            </g>
          );
        })}

        {/* 0% baseline */}
        <line x1={pads.l} x2={chartW - pads.r} y1={scaleY(0)} y2={scaleY(0)}
          stroke="hsl(var(--border))" strokeWidth={0.8} strokeDasharray="4 4" opacity={0.5} />
        <text x={chartW - pads.r - 2} y={scaleY(0) - 3} textAnchor="end"
          fill="hsl(var(--muted-foreground))" fontSize={8} opacity={0.6}>0%</text>

        {/* Candles */}
        {ohlcData.map((d, i) => {
          const x = dayToX(i);
          const isUp = d.close >= d.open;
          const color = isUp ? '#ef4444' : '#10b981';
          const bodyTop = scaleY(Math.max(d.open, d.close));
          const bodyBot = scaleY(Math.min(d.open, d.close));
          const bodyH = Math.max(bodyBot - bodyTop, 1);
          return (
            <g key={i} onMouseEnter={() => setHoverIdx(i)} onMouseLeave={() => setHoverIdx(null)}
              style={{ cursor: 'crosshair' }}>
              <line x1={x} x2={x} y1={scaleY(d.high)} y2={scaleY(d.low)} stroke={color} strokeWidth={1} />
              <rect x={x - candleW / 2} y={bodyTop} width={candleW} height={bodyH} rx={0.8}
                fill={isUp ? color : '#0f1723'} stroke={color} strokeWidth={1} />
              {hoverIdx === i && (
                <line x1={x} x2={x} y1={pads.t} y2={pads.t + chartH}
                  stroke="hsl(var(--border))" strokeWidth={0.8} strokeDasharray="2 3" opacity={0.6} />
              )}
            </g>
          );
        })}

        {/* X-axis labels */}
        {xLabels.map((lbl, i) => (
          <text key={`xl-${i}`} x={lbl.x} y={height - 4} textAnchor="middle"
            fill="hsl(var(--muted-foreground))" fontSize={9} fontFamily="monospace">{lbl.label}</text>
        ))}
      </svg>

      {/* Tooltip */}
      {hoverIdx != null && ohlcData[hoverIdx] && (() => {
        const d = ohlcData[hoverIdx];
        const r = raw[hoverIdx];
        const isUp = d.close >= d.open;
        const chgColor = isUp ? '#ef4444' : '#10b981';
        const dayChg = r.open != null && r.open !== 0 ? ((r.close! - r.open!) / Math.abs(r.open!) * 100) : null;
        const cumChg = initCapital > 0 ? ((r.capital - initCapital) / initCapital) * 100 : 0;
        const xPx = dayToX(hoverIdx);
        const tooltipW = 170;
        const margin = 8;
        const leftPx = Math.max(margin, Math.min(chartW - tooltipW - margin, xPx - tooltipW / 2));
        return (
          <div style={{
            position: 'absolute',
            top: 6,
            left: leftPx,
            background: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 8,
            padding: '8px 12px',
            fontSize: 11,
            zIndex: 50,
            width: tooltipW,
            pointerEvents: 'none',
            boxShadow: '0 4px 20px rgba(0,0,0,0.4), 0 0 0 1px rgba(255,255,255,0.05)',
          }}>
            <div style={{ color: '#9ca3af', marginBottom: 4, fontSize: 10, fontWeight: 500, letterSpacing: '0.02em' }}>
              {d.date}
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2px 8px', fontFamily: 'monospace', fontSize: 11 }}>
              <span style={{ color: '#9ca3af' }}>开盘</span>
              <span style={{ textAlign: 'right', color: d.open >= 0 ? '#ef4444' : '#10b981', fontWeight: 500 }}>{fmtPct(d.open)}</span>
              <span style={{ color: '#9ca3af' }}>最高</span>
              <span style={{ textAlign: 'right', color: '#ef4444', fontWeight: 500 }}>{fmtPct(d.high)}</span>
              <span style={{ color: '#9ca3af' }}>最低</span>
              <span style={{ textAlign: 'right', color: '#10b981', fontWeight: 500 }}>{fmtPct(d.low)}</span>
              <span style={{ color: '#9ca3af' }}>收盘</span>
              <span style={{ textAlign: 'right', color: chgColor, fontWeight: 600 }}>{fmtPct(d.close)}</span>
            </div>
            {dayChg != null && (
              <div style={{
                color: chgColor, fontFamily: 'monospace', marginTop: 4,
                fontSize: 11, fontWeight: 600, textAlign: 'right',
              }}>
                涨跌 {dayChg >= 0 ? '+' : ''}{dayChg.toFixed(2)}%
              </div>
            )}
            <div style={{
              marginTop: 4, borderTop: '1px solid hsl(var(--border))', paddingTop: 4,
              fontSize: 10, color: '#9ca3af', display: 'flex', justifyContent: 'space-between',
            }}>
              <span>累计 <span style={{ color: cumChg >= 0 ? '#ef4444' : '#10b981', fontWeight: 500 }}>{cumChg >= 0 ? '+' : ''}{cumChg.toFixed(2)}%</span></span>
              <span style={{ color: '#d1d5db' }}>{fmtWan(r.capital)}</span>
            </div>
          </div>
        );
      })()}
    </div>
  );
};

const FactorTopsCard: React.FC<{
  data: FactorTopsResponse | null;
  loading: boolean;
}> = ({ data, loading }) => {
  if (loading) {
    return (
      <div className="flex items-center gap-2 rounded-xl border border-border/20 bg-card/40 px-4 py-6 text-[12px] text-tertiary-text">
        <Loader2 className="h-3 w-3 animate-spin" />加载因子数据...
      </div>
    );
  }
  if (!data || !data.factors.length) {
    return (
      <div className="rounded-xl border border-border/20 bg-card/40 px-4 py-6 text-[12px] text-tertiary-text">
        暂无因子评分数据
      </div>
    );
  }

  const rankCls = [
    'bg-amber-400/15 text-amber-400',
    'bg-slate-300/20 text-slate-400',
    'bg-orange-400/15 text-orange-400',
    'bg-emerald-400/15 text-emerald-400',
    'bg-violet-400/15 text-violet-400',
  ];

  return (
    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
      {data.factors.map((f) => (
        <div
          key={f.factor_name}
          className="rounded-xl border border-border/20 bg-card/40 overflow-hidden"
        >
          {/* header */}
          <div className="flex items-center gap-2 border-b border-border/10 px-3 py-1.5 bg-muted/15">
            <span className="text-sm font-medium text-foreground">{f.factor_label}</span>
          </div>
          {/* stocks */}
          <div className="divide-y divide-border/5">
            {f.stocks.map((s, i) => (
              <div
                key={s.stock_code}
                className="flex items-center gap-2.5 px-3 py-1.5 text-[13px] hover:bg-muted/10 transition-colors"
              >
                <span className={`flex h-5 w-5 shrink-0 items-center justify-center rounded text-[11px] font-bold ${rankCls[i]}`}>
                  {i + 1}
                </span>
                <span className="font-mono font-semibold text-foreground shrink-0">{s.stock_code}</span>
                <span className="text-secondary-text shrink-0">{s.stock_name}</span>
                {s.sector && (
                  <span className="shrink-0 text-[11px] text-tertiary-text/70 border border-border/20 rounded px-1 py-px">
                    {s.sector}
                  </span>
                )}
                <span className="ml-auto font-mono tabular-nums text-cyan font-semibold text-sm">
                  {s.factor_score.toFixed(1)}
                </span>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
};

const BacktestCard: React.FC<{
  data: BacktestResponse | null;
  loading: boolean;
  error?: string | null;
  startDate: string;
  endDate: string;
  onStartDate: (v: string) => void;
  onEndDate: (v: string) => void;
  onRefresh: () => void;
}> = ({ data, loading, error, startDate, endDate, onStartDate, onEndDate, onRefresh }) => {
  const [section, setSection] = useState<'chart' | 'trades'>('chart');
  const [collapsed, setCollapsed] = useState(false);
  const tradeRows = data?.trade_records ?? [];

  const rankSlotContribution = useMemo(() => {
    if (tradeRows.length === 0) return [] as Array<{
      slot: number; label: string; trade_count: number; total_pnl: number;
      contribution_pct: number; win_rate: number; avg_return_pct: number;
    }>;
    const ranked = tradesWithEffectivePickRank(tradeRows, DISCOVERY_RANK_TOP_N);
    if (ranked.length === 0) return [];
    const totalPnl = ranked.reduce((s, t) => s + Number(t.pnl), 0);
    return Array.from({ length: DISCOVERY_RANK_TOP_N }, (_, i) => {
      const slot = i + 1;
      const slotTrades = ranked.filter((t) => t.effective_pick_rank === slot);
      const trade_count = slotTrades.length;
      const win_count = slotTrades.filter((t) => Number(t.pnl) > 0).length;
      const total_pnl = slotTrades.reduce((s, t) => s + Number(t.pnl), 0);
      const total_return_pct = slotTrades.reduce((s, t) => s + Number(t.return_pct), 0);
      return {
        slot,
        label: `Top${slot}`,
        trade_count,
        total_pnl,
        contribution_pct: totalPnl !== 0 ? total_pnl / totalPnl : 0,
        win_rate: trade_count > 0 ? win_count / trade_count : 0,
        avg_return_pct: trade_count > 0 ? total_return_pct / trade_count : 0,
      };
    });
  }, [tradeRows]);

  const hasOpenTrades = tradeRows.some(t => t.is_open);

  useEffect(() => {
    if (!hasOpenTrades || collapsed || section !== 'trades') return;
    const id = setInterval(() => onRefresh(), 30_000);
    return () => clearInterval(id);
  }, [hasOpenTrades, collapsed, section, onRefresh]);

  const exportTrades = useCallback(() => {
    if (!data?.trade_records?.length) return;
    const header = '股票代码,股票名称,买入日,买入价,股数,买入金额,卖出日,卖出价,卖出金额,收益%,盈亏,状态';
    const rows = data.trade_records.map(t => {
      const buyAmount = t.shares ? (t.shares * t.buy_price).toFixed(2) : '--';
      const sellAmount = t.shares ? (t.shares * t.sell_price).toFixed(2) : '--';
      return [
        t.stock_code,
        t.stock_name,
        fmtDate(t.buy_date),
        t.buy_price.toFixed(2),
        t.shares ? t.shares.toString() : '--',
        buyAmount,
        t.is_open ? '持仓中' : fmtDate(t.sell_date),
        t.sell_price.toFixed(2),
        sellAmount,
        `${(t.return_pct >= 0 ? '+' : '')}${(t.return_pct * 100).toFixed(2)}%`,
        `${(t.pnl >= 0 ? '+' : '')}${t.pnl.toFixed(0)}`,
        t.is_open ? '持仓中' : '已平仓',
      ].join(',');
    }).join('\n');
    const bom = '﻿';
    const blob = new Blob([bom + header + '\n' + rows], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `backtest_trades_${data.mode}_${fmtDate(startDate || '')}_${fmtDate(endDate || '')}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [data, startDate, endDate]);

  // Only show full loading skeleton when there's no data yet — prevent height collapse on refresh
  if (!data && loading) {
    return (
      <div className="rounded-xl border border-border/20 bg-card/40 px-4 py-3 text-[12px] text-tertiary-text min-h-[48px]">
        <Loader2 className="inline h-3 w-3 animate-spin mr-1.5" />加载回测数据...
      </div>
    );
  }

  if (!data) {
    return (
      <div className="rounded-xl border border-border/20 bg-card/40 px-4 py-3 text-[12px] text-tertiary-text min-h-[48px]">
        {loading ? (
          <><Loader2 className="inline h-3 w-3 animate-spin mr-1.5" />加载回测数据...</>
        ) : (
          <>{error || '暂无回测数据'}</>
        )}
      </div>
    );
  }

  const isPositive = data.cumulative_return >= 0;
  const pct = data.total_days > 0 ? (data.cumulative_return * 100).toFixed(2) : '--';
  const wrPct = data.total_days > 0 ? (data.win_rate * 100).toFixed(0) : '--';
  const maxDdPct = data.total_days > 0 ? (data.max_drawdown * 100).toFixed(1) : '--';
  const pnlSign = data.total_pnl >= 0 ? '+' : '';
  const initCapital = data.initial_capital || 5_000_000;
  const initialLine = initCapital;
  const chartData = data.capital_curve.length > 0
    ? data.capital_curve.map(p => ({
        date: fmtDate(p.date),
        capital: p.capital,
        ...(p.open != null && { open: p.open, high: p.high, low: p.low, close: p.close }),
      }))
    : [{ date: fmtDate(new Date().toISOString().slice(0, 10).replace(/-/g, '')), capital: initCapital }];

  return (
    <div className="rounded-xl border border-border/20 bg-card/40">
      {/* ── Summary bar ── */}
      <button
        onClick={() => setCollapsed(v => !v)}
        className="w-full px-4 py-3 flex flex-wrap items-center gap-x-5 gap-y-1.5 text-[12px] border-b border-border/15 hover:bg-foreground/[0.02] transition-colors"
      >
        <span className="text-tertiary-text text-[11px] font-medium tracking-wide">回测</span>
        {loading && <Loader2 className="h-3 w-3 animate-spin text-cyan/60" />}
        {error && !loading && (
          <span className="text-amber-400/90 text-[11px]">{error}</span>
        )}

        <div className="flex items-center gap-3">
          <span className={`font-bold text-sm tabular-nums ${isPositive ? 'text-red-400' : 'text-emerald-400'}`}>
            {isPositive ? '+' : ''}{pct}%
          </span>
          <span className="text-tertiary-text">
            胜率 <span className="text-foreground font-medium">{wrPct}%</span>
          </span>
          <span className="text-tertiary-text">
            最大回撤 <span className="text-foreground font-medium">{maxDdPct}%</span>
          </span>
          <span className="text-tertiary-text">
            {data.total_days}天 · {data.total_trades}笔
          </span>
        </div>

        <div className="flex items-center gap-2 text-[11px] text-tertiary-text">
          <span>初始 {fmtWan(initCapital)}</span>
          <span className="text-foreground/60">→</span>
          <span className={`font-medium tabular-nums ${isPositive ? 'text-red-400' : 'text-emerald-400'}`}>
            最终 {fmtWan(data.final_capital)}
          </span>
          {data.total_pnl !== 0 && (
            <span className={`tabular-nums ${isPositive ? 'text-red-400' : 'text-emerald-400'}`}>
              ({pnlSign}{fmtWan(data.total_pnl)})
            </span>
          )}
        </div>

        {/* Date filter — stop propagation to prevent collapse toggle */}
        <div className="ml-auto flex items-center gap-1.5" onClick={e => e.stopPropagation()}>
          <DatePicker
            value={startDate ? dayjs(fmtDate(startDate)) : null}
            onChange={d => onStartDate(d ? d.format('YYYYMMDD') : '')}
            minDate={dayjs('2026-05-01')}
            maxDate={dayjs()}
            placeholder="开始日期"
            style={{ width: 120 }}
            size="small"
            allowClear
            showToday={false}
          />
          <span className="text-tertiary-text text-[11px]">-</span>
          <DatePicker
            value={endDate ? dayjs(fmtDate(endDate)) : null}
            onChange={d => onEndDate(d ? d.format('YYYYMMDD') : '')}
            minDate={dayjs('2026-05-01')}
            maxDate={dayjs()}
            placeholder="结束日期"
            style={{ width: 120 }}
            size="small"
            allowClear
            showToday={false}
          />
          <button
            onClick={e => { e.stopPropagation(); onRefresh(); }}
            className="h-7 px-2 rounded-lg border border-border/30 bg-muted/30 text-[11px] text-cyan hover:bg-cyan/10 transition-colors"
          >
            查询
          </button>
        </div>

        <ChevronDown className={`h-4 w-4 text-tertiary-text transition-transform duration-200 ${collapsed ? '' : 'rotate-180'}`} />
      </button>

      {!collapsed && (
        <>
          {/* ── Tab switcher ── */}
          <div className="flex items-center border-b border-border/10">
            <button
              onClick={() => setSection('chart')}
              className={`px-4 py-1.5 text-[11px] font-medium transition-colors ${section === 'chart' ? 'text-cyan border-b border-cyan' : 'text-tertiary-text hover:text-secondary-text'}`}
            >
              收益曲线
            </button>
            <button
              onClick={() => { setCollapsed(false); setSection('trades'); }}
              className={`px-4 py-1.5 text-[11px] font-medium transition-colors ${section === 'trades' ? 'text-cyan border-b border-cyan' : 'text-tertiary-text hover:text-secondary-text'}`}
            >
              交易记录{tradeRows.length > 0 ? ` (${tradeRows.length})` : ''}
            </button>
            {section === 'trades' && tradeRows.length > 0 && (
              <button
                onClick={e => { e.stopPropagation(); exportTrades(); }}
                className="ml-auto mr-2 flex items-center gap-1 px-2 py-1 rounded text-[11px] text-tertiary-text hover:text-cyan hover:bg-cyan/5 transition-colors"
                title="导出 CSV"
              >
                <Download className="h-3 w-3" />
                导出
              </button>
            )}
          </div>

          {/* ── Chart ── */}
          {section === 'chart' && (
            <div className="px-2 py-3">
              {chartData.filter(d => d.open != null && d.high != null && d.low != null && d.close != null).length >= 1 ? (
                <PortfolioCandleChart data={chartData} initCapital={initCapital} height={200} />
              ) : (
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData} margin={{ left: 12 }}>
                    <XAxis dataKey="date" tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }} stroke="hsl(var(--border))" tickFormatter={v => String(v).slice(5).replace('-', '/')} />
                    <YAxis
                      tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
                      stroke="hsl(var(--border))"
                      tickFormatter={v => `${(v / 10000).toFixed(0)}w`}
                      domain={['auto', 'auto']}
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
                        return isNaN(n) ? ['-', '资金'] : [`¥${n.toLocaleString()}`, '资金'];
                      }}
                    />
                    <ReferenceLine y={initialLine} stroke="hsl(var(--border))" strokeDasharray="4 4" />
                    <Line
                      type="monotone"
                      dataKey="capital"
                      stroke={isPositive ? '#f87171' : '#34d399'}
                      strokeWidth={2}
                      dot={false}
                      activeDot={{ r: 4 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          )}

          {/* ── Trade records ── */}
          {section === 'trades' && (
            <div className="max-h-64 overflow-y-auto">
              {tradeRows.length === 0 ? (
                <div className="px-4 py-6 text-center text-[12px] text-tertiary-text">
                  暂无交易记录{data.total_trades > 0 ? '（数据加载不完整，请点「查询」重试）' : ''}
                </div>
              ) : (
              <>
              {rankSlotContribution.length > 0 && (
                <div className="border-b border-border/10 px-3 py-3">
                  <h4 className="mb-1 text-[12px] font-semibold text-foreground">
                    选股顺位收益贡献（Top1 ~ Top4）
                  </h4>
                  <p className="mb-2 text-[10px] text-tertiary-text">
                    按发现日综合分顺位汇总交易盈亏；含已平仓与持仓中记录
                  </p>
                  <table className="w-full text-[11px]">
                    <thead className="text-tertiary-text">
                      <tr>
                        <th className="py-1.5 text-left font-medium">顺位</th>
                        <th className="py-1.5 text-right font-medium">交易次数</th>
                        <th className="py-1.5 text-right font-medium">累计盈亏</th>
                        <th className="py-1.5 text-right font-medium">贡献占比</th>
                        <th className="py-1.5 text-right font-medium">胜率</th>
                        <th className="py-1.5 text-right font-medium">均收益</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rankSlotContribution.map((r) => (
                        <tr key={r.slot} className="border-t border-border/10">
                          <td className="py-1.5 text-foreground">{r.label}</td>
                          <td className="py-1.5 text-right tabular-nums">{r.trade_count > 0 ? r.trade_count : '--'}</td>
                          <td className={`py-1.5 text-right tabular-nums font-medium ${r.total_pnl >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                            {r.trade_count > 0 ? `${r.total_pnl >= 0 ? '+' : ''}${r.total_pnl.toFixed(0)}` : '--'}
                          </td>
                          <td className="py-1.5 text-right tabular-nums">
                            {r.trade_count > 0 ? `${(r.contribution_pct * 100).toFixed(1)}%` : '--'}
                          </td>
                          <td className="py-1.5 text-right tabular-nums">
                            {r.trade_count > 0 ? `${(r.win_rate * 100).toFixed(0)}%` : '--'}
                          </td>
                          <td className={`py-1.5 text-right tabular-nums ${r.avg_return_pct >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                            {r.trade_count > 0 ? `${r.avg_return_pct >= 0 ? '+' : ''}${(r.avg_return_pct * 100).toFixed(2)}%` : '--'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {hasOpenTrades && (
                <p className="px-3 py-2 text-[10px] text-tertiary-text border-b border-border/10">
                  持仓中个股的收益%与盈亏按实时价估算；卖出价列为现价，每 30 秒自动刷新
                </p>
              )}
              <table className="w-full text-[11px]">
                <thead className="sticky top-0 bg-card/90 text-tertiary-text">
                  <tr>
                    <th className="px-3 py-2 text-left font-medium">股票</th>
                    <th className="px-2 py-2 text-right font-medium">买入日</th>
                    <th className="px-2 py-2 text-right font-medium">买入价</th>
                    <th className="px-2 py-2 text-right font-medium">股数</th>
                    <th className="px-2 py-2 text-right font-medium">买入金额</th>
                    <th className="px-2 py-2 text-right font-medium">卖出日</th>
                    <th className="px-2 py-2 text-right font-medium">卖出价</th>
                    <th className="px-2 py-2 text-right font-medium">卖出金额</th>
                    <th className="px-2 py-2 text-right font-medium">收益%</th>
                    <th className="px-2 py-2 text-right font-medium">盈亏</th>
                    <th className="px-2 py-2 text-right font-medium">状态</th>
                  </tr>
                </thead>
                <tbody>
                  {[...tradeRows].reverse().map((t, i) => {
                    const retPct = Number(t.return_pct);
                    const pnl = Number(t.pnl);
                    const buyPx = Number(t.buy_price);
                    const sellPx = Number(t.sell_price);
                    return (
                    <tr key={`${t.stock_code}-${t.buy_date}-${i}`} className="border-t border-border/10 hover:bg-foreground/[0.02]">
                      <td className="px-3 py-1.5">
                        <span className="font-medium text-foreground">{t.stock_code}</span>
                        <span className="text-tertiary-text ml-1">{t.stock_name}</span>
                      </td>
                      <td className="px-2 py-1.5 text-right text-tertiary-text">{fmtDate(t.buy_date)}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{Number.isFinite(buyPx) ? buyPx.toFixed(2) : '--'}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{t.shares ? t.shares.toLocaleString() : '--'}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{t.shares && Number.isFinite(buyPx) ? `${(t.shares * buyPx / 10000).toFixed(2)}万` : '--'}</td>
                      <td className="px-2 py-1.5 text-right text-tertiary-text">{t.is_open ? '持仓中' : fmtDate(t.sell_date)}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{Number.isFinite(sellPx) ? sellPx.toFixed(2) : '--'}</td>
                      <td className="px-2 py-1.5 text-right tabular-nums">{t.shares && Number.isFinite(sellPx) ? `${(t.shares * sellPx / 10000).toFixed(2)}万` : '--'}</td>
                      <td className={`px-2 py-1.5 text-right font-medium tabular-nums ${retPct >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                        {Number.isFinite(retPct) ? `${retPct >= 0 ? '+' : ''}${(retPct * 100).toFixed(2)}%` : '--'}
                      </td>
                      <td className={`px-2 py-1.5 text-right font-medium tabular-nums ${pnl >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                        {Number.isFinite(pnl) ? `${pnl >= 0 ? '+' : ''}${pnl.toFixed(0)}` : '--'}
                      </td>
                      <td className="px-2 py-1.5 text-right text-tertiary-text">{t.is_open ? '持仓中' : '已平仓'}</td>
                    </tr>
                    );
                  })}
                </tbody>
              </table>
              </>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
};

/* ──────────────────────────────────────────────
   8. Page
   ────────────────────────────────────────────── */

const DiscoveryPage: React.FC = () => {
  const [tab, setTab] = useState<TabKey>(() => getDefaultTabByCnMarketTime());
  const [intraday, setIntraday] = useState<{
    updated?: string; round: number; top_n: DiscoveryItem[]; dropped: DiscoveryItem[];
  } | null>(null);
  const [postTopN, setPostTopN] = useState<DiscoveryItem[]>([]);
  const [reportDate, setReportDate] = useState<string | null>(null);
  const [liveRescored, setLiveRescored] = useState(false);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [backtestByTab, setBacktestByTab] = useState<Record<string, BacktestResponse | null>>({});
  const [backtestError, setBacktestError] = useState<string | null>(null);
  const [btStartDate, setBtStartDate] = useState<string>('');
  const [btEndDate, setBtEndDate] = useState<string>('');
  const intradayFetchInFlightRef = useRef(false);
  const intradayLastFetchAtRef = useRef(0);
  const postTopNRef = useRef(postTopN);
  postTopNRef.current = postTopN;
  const [intradayScanMode, setIntradayScanMode] = useState<ScanModeResponse>({ scan_universe: 'full_market', has_whitelist: false });
  const [postmarketScanMode, setPostmarketScanMode] = useState<ScanModeResponse>({ scan_universe: 'full_market', has_whitelist: false });
  const [resultSubTab, setResultSubTab] = useState<string>('composite');
  const [factorTops, setFactorTops] = useState<FactorTopsResponse | null>(null);
  const [factorTopsLoading, setFactorTopsLoading] = useState(false);
  const [lookupInput, setLookupInput] = useState('');
  const [lookupResult, setLookupResult] = useState<StockScoreResponse | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [lookupExpanded, setLookupExpanded] = useState<Set<string>>(new Set());
  const { index: stockIndex } = useStockIndex();

  const lookupOptions = useMemo(() => {
    const segments = lookupInput.split(/[,，]/);
    const last = segments[segments.length - 1]?.trim() || '';
    if (last.length < 1) return [];
    const results = searchStocks(last, stockIndex, { limit: 8 });
    return results.map(s => ({
      value: s.displayCode,
      label: `${s.displayCode}  ${s.nameZh}`,
    }));
  }, [lookupInput, stockIndex]);

  const handleLookup = useCallback(async () => {
    const trimmed = lookupInput.trim();
    if (!trimmed) return;
    setLookupLoading(true);
    setLookupError(null);
    setLookupResult(null);
    try {
      // Convert name segments to bare stock codes
      const segments = trimmed.split(/[,，]/);
      const codes = segments.map(seg => {
        const s = seg.trim();
        if (!s) return '';
        // Already a bare stock code (6 digits)
        if (/^\d{6}$/.test(s)) return s;
        // Search stock index by name/code
        const results = searchStocks(s, stockIndex, { limit: 1 });
        return results.length > 0 ? results[0].displayCode : s;
      });
      const query = codes.join(',');
      const data = await discoveryApi.getStockScore(query, tab);
      setLookupResult(data);
    } catch (e: unknown) {
      setLookupError(e instanceof Error ? e.message : '查询失败');
    } finally {
      setLookupLoading(false);
    }
  }, [lookupInput, tab, stockIndex]);

  const fetchIntraday = useCallback(async (force = false) => {
    const now = Date.now();
    if (intradayFetchInFlightRef.current) return;
    if (!force && now - intradayLastFetchAtRef.current < MIN_INTRADAY_FETCH_GAP_MS) return;

    intradayFetchInFlightRef.current = true;
    intradayLastFetchAtRef.current = now;
    try {
      const data = await discoveryApi.getIntradayTop10({ force });
      setIntraday(data);
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'err');
    } finally {
      intradayFetchInFlightRef.current = false;
    }
  }, []);

  const fetchReport = useCallback(async (date?: string) => {
    try {
      if (postTopNRef.current.length === 0) setLoading(true);
      const d = await discoveryApi.getPostmarketReport(date);
      setPostTopN(d.top_n ?? []);
      setReportDate(d.date ?? null);
      setLiveRescored(false);
      setError(null);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : 'err'); }
    finally { setLoading(false); }
  }, []);

  const fetchLiveRescore = useCallback(async () => {
    try {
      if (postTopNRef.current.length === 0) setLoading(true);
      const d = await discoveryApi.getPostmarketFollowup();
      if (d.top_n && d.top_n.length > 0) {
        setPostTopN(d.top_n);
        setLiveRescored(!!d.live_rescored);
        setReportDate(d.date ?? null);
        setError(null);
      } else {
        // followup 无数据（非交易时段或无盘中数据），fallback 到今天的静态报告
        const r = await discoveryApi.getPostmarketReport();
        setPostTopN(r.top_n ?? []);
        setReportDate(r.date ?? null);
        setLiveRescored(false);
      }
    } catch {
      // fallback: 加载今天的静态数据
      try {
        const r = await discoveryApi.getPostmarketReport();
        setPostTopN(r.top_n ?? []);
        setReportDate(r.date ?? null);
        setLiveRescored(false);
      } catch { /* silent */ }
    } finally {
      if (postTopNRef.current.length === 0) setLoading(false);
    }
  }, []);

  const runDiscovery = useCallback(async () => {
    setRunning(true);
    setError(null);
    try {
      const { task_id } = await discoveryApi.runPostmarketDiscovery();
      for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const s = await discoveryApi.getPostmarketRunStatus(task_id);
        if (s.status === 'completed') {
          await fetchReport();
          return;
        }
        if (s.status === 'failed') {
          setError(s.error || '盘后发现执行失败');
          return;
        }
      }
      setError('盘后发现超时（超过 4 分钟）');
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'err');
    } finally {
      setRunning(false);
    }
  }, [fetchReport]);

  const fetchBacktest = useCallback(async (mode: 'intraday' | 'postmarket') => {
    setBacktestLoading(true);
    setBacktestError(null);
    try {
      const opts: { days?: number; start_date?: string; end_date?: string } = {};
      if (btStartDate) opts.start_date = btStartDate;
      if (btEndDate) opts.end_date = btEndDate;
      if (!btStartDate && !btEndDate) opts.days = 60;
      const d = await discoveryApi.getBacktest(mode, opts);
      setBacktestByTab(prev => ({ ...prev, [mode]: d }));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : '回测加载失败';
      setBacktestError(msg.includes('timeout') || msg.includes('Timeout') ? '回测计算超时，请稍后重试' : msg);
    } finally { setBacktestLoading(false); }
  }, [btStartDate, btEndDate]);

  const fetchScanMode = useCallback((mode: 'intraday' | 'postmarket') => {
    discoveryApi.getScanMode(mode).then((data: any) => {
      if (data.scan_universe === undefined && data.use_whitelist !== undefined) {
        data = { scan_universe: data.use_whitelist ? 'whitelist' : 'full_market', has_whitelist: data.has_whitelist ?? false };
      }
      if (mode === 'intraday') setIntradayScanMode(data as ScanModeResponse);
      else setPostmarketScanMode(data as ScanModeResponse);
    }).catch(() => {});
  }, []);

  const fetchFactorTops = useCallback(async (mode: 'intraday' | 'postmarket') => {
    setFactorTopsLoading(true);
    try {
      const data = await discoveryApi.getFactorTops(mode);
      setFactorTops(data);
    } catch { /* silent */ }
    finally { setFactorTopsLoading(false); }
  }, []);

  // 切换顶层 Tab 时重置子 Tab
  useEffect(() => {
    setResultSubTab('composite');
  }, [tab]);

  // 盘中 → 盘后自动切换：15:00 后仅切一次，之后尊重用户手动选择
  const autoSwitchedRef = useRef(false);
  useEffect(() => {
    const id = setInterval(() => {
      if (autoSwitchedRef.current) return;
      const next = getDefaultTabByCnMarketTime();
      if (next === 'postmarket') {
        autoSwitchedRef.current = true;
        setTab(prev => prev === 'intraday' ? 'postmarket' : prev);
      }
    }, 60_000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    setBacktestLoading(true);
    if (tab === 'intraday') { fetchIntraday(); fetchBacktest('intraday'); fetchScanMode('intraday'); }
    else {
      // 优先用内存缓存（含盘中最后一次重评），没有再加载静态报告
      fetchLiveRescore();
      fetchBacktest('postmarket');
      fetchScanMode('postmarket');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  useEffect(() => {
    if (resultSubTab === 'factor-tops') fetchFactorTops(tab);
  }, [resultSubTab, tab, fetchFactorTops]);
  useEffect(() => {
    if (lookupInput.trim()) void handleLookup();
  }, [tab]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (tab !== 'intraday') return;

    const streamUrl = `${import.meta.env.VITE_API_BASE_URL ?? ''}/api/v1/discovery/intraday/stream`;
    const es = new EventSource(streamUrl);

    es.addEventListener('update', () => {
      fetchIntraday(true);
      fetchBacktest('intraday');
    });

    es.onerror = () => {
      es.close();
    };

    return () => es.close();
  }, [tab, fetchIntraday, fetchBacktest]);

  useEffect(() => {
    const id = setInterval(() => fetchBacktest(tab), BACKTEST_REFRESH_MS);
    return () => clearInterval(id);
  }, [tab, fetchBacktest]);

  useEffect(() => {
    if (tab !== 'postmarket') return;

    const streamUrl = `${import.meta.env.VITE_API_BASE_URL ?? ''}/api/v1/discovery/postmarket/stream`;
    const es = new EventSource(streamUrl);

    es.addEventListener('update', () => {
      // 优先用内存缓存（含盘中最后一次重评），没有再加载静态报告
      fetchLiveRescore();
    });

    es.addEventListener('rescore', () => {
      fetchLiveRescore();
    });

    es.addEventListener('heartbeat', () => {
      // 非交易时段仅维持心跳，不主动刷新
    });

    es.onerror = () => {
      es.close();
    };

    return () => es.close();
  }, [tab, fetchReport, fetchLiveRescore]);

  useEffect(() => { document.title = '寻股 - DSA'; }, []);

  const toggle = (code: string) => setExpanded(prev => {
    const n = new Set(prev);
    if (n.has(code)) n.delete(code); else n.add(code);
    return n;
  });

  const cardList = useMemo(
    () => (tab === 'intraday' ? intraday?.top_n ?? [] : postTopN),
    [tab, intraday?.top_n, postTopN]
  );
  const hasCards = cardList.length > 0;

  /* ── Card grid ── */
  const cardGrid = (
    <div className="grid gap-2">
      <AnimatePresence>
        {cardList.map((item) => (
          <StockCard
            key={item.stock_code}
            item={item}
            open={expanded.has(item.stock_code)}
            onToggle={() => toggle(item.stock_code)}
          />
        ))}
      </AnimatePresence>
    </div>
  );

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      {/* ── Header ── */}
      <div className="mb-7 flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-foreground flex items-center gap-2.5 tracking-tight">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-gradient-to-br from-cyan to-blue shadow-md shadow-cyan/15">
              <Compass className="h-[18px] w-[18px] text-white" />
            </div>
            寻股
          </h1>
          <p className="mt-1.5 text-[13px] text-tertiary-text">多因子智能选股 · 盘中实时 + 盘后深度</p>
        </div>

        {/* Score lookup */}
        <div className="flex items-center gap-2">
          <AutoComplete
            value={lookupInput}
            onChange={(v) => setLookupInput(v)}
            options={lookupOptions}
            onSelect={(code: string) => {
              const segments = lookupInput.split(/[,，]/);
              segments[segments.length - 1] = ` ${code}`;
              setLookupInput(segments.join(',').replace(/^,/, '').trim());
            }}
            className="w-64"
          >
            <input
              type="text"
              onKeyDown={(e) => { if (e.key === 'Enter') void handleLookup(); }}
              placeholder="代码/名称, 逗号分隔"
              className="w-full rounded-lg border border-border/40 bg-card px-3 py-1.5 text-[13px] text-foreground placeholder:text-tertiary-text/50 focus:outline-none focus:ring-1 focus:ring-cyan/30 transition-colors"
            />
          </AutoComplete>
          <button
            type="button"
            onClick={() => void handleLookup()}
            disabled={lookupLoading}
            className="inline-flex items-center gap-1 rounded-lg bg-cyan/15 px-3 py-1.5 text-[13px] font-medium text-cyan hover:bg-cyan/20 transition-colors disabled:opacity-50"
          >
            {lookupLoading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Zap className="h-3.5 w-3.5" />}
            查分
          </button>
        </div>

        {/* Tab switcher */}
        <div className="flex rounded-xl bg-card/80 p-0.5 ring-1 ring-border/30">
          {(['intraday', 'postmarket'] as TabKey[]).map(t => (
            <button key={t} type="button" onClick={() => setTab(t)}
              className={`relative px-4 py-1.5 rounded-lg text-[13px] font-medium transition-colors ${
                tab === t ? 'text-cyan' : 'text-tertiary-text hover:text-secondary-text'
              }`}
            >
              {tab === t && (
                <motion.div
                  layoutId="disco-tab"
                  className="absolute inset-0 rounded-lg bg-cyan/10 ring-1 ring-cyan/15"
                  transition={{ type: 'spring', stiffness: 380, damping: 30 }}
                />
              )}
              <span className="relative z-10 flex items-center gap-1.5">
                {t === 'intraday' ? <TrendingUp className="h-4 w-4" /> : <TrendingDown className="h-4 w-4" />}
                {t === 'intraday' ? '盘中' : '盘后'}
              </span>
            </button>
          ))}
        </div>
      </div>

      {error ? (
        <div className="mb-5 rounded-xl border border-red/25 bg-red/5 px-4 py-3 text-[13px] text-red" role="alert">{error}</div>
      ) : null}

      {/* ── Lookup results ── */}
      {lookupLoading && (
        <div className="mb-5 flex items-center gap-2 rounded-xl border border-border/30 bg-card/60 px-4 py-6 text-[13px] text-tertiary-text">
          <Loader2 className="h-4 w-4 animate-spin" />正在计算技术评分，请稍候...
        </div>
      )}
      {lookupError && (
        <div className="mb-5 rounded-xl border border-red/25 bg-red/5 px-4 py-3 text-[13px] text-red" role="alert">{lookupError}</div>
      )}
      {lookupResult && lookupResult.items.length > 0 && (
        <div className="mb-5 rounded-xl border border-border/30 bg-card/60 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-[13px] font-medium text-secondary-text">
              评分查询结果（{lookupResult.items.length} 只）
            </h3>
            <button
              type="button"
              onClick={() => { setLookupResult(null); setLookupInput(''); }}
              className="text-[11px] text-tertiary-text hover:text-secondary-text transition-colors"
            >
              清除
            </button>
          </div>
          <Table
            size="small"
            pagination={false}
            dataSource={lookupResult.items}
            rowKey="stock_code"
            expandable={{
              expandedRowKeys: Array.from(lookupExpanded),
              onExpandedRowsChange: (keys) => setLookupExpanded(new Set(keys as string[])),
              expandedRowRender: (entry) => {
                const scoreItem = entry.intraday || entry.postmarket;
                if (!scoreItem) return null;
                const factorEntries = Object.entries(scoreItem.factor_scores)
                  .filter(([, score]) => score > 0)
                  .sort(([a], [b]) => (scoreItem.factor_weights?.[b] ?? 0) - (scoreItem.factor_weights?.[a] ?? 0));
                const techDims = [
                  { label: '赔率', key: 'rr_score' },
                  { label: '大盘环境', key: 'market_score' },
                  { label: '板块强弱', key: 'sector_score' },
                  { label: '量能质量', key: 'volume_score' },
                  { label: '相对位置', key: 'position_score' },
                  { label: '形态确认', key: 'formation_score' },
                ];
                return (
                  <div className="space-y-4 py-2" style={{ width: 0, minWidth: '100%' }}>
                    <StockKLineChart stockCode={entry.stock_code} height={280} />
                    <div className="grid grid-cols-2 gap-6">
                    {/* 左：因子得分 */}
                    <div className="space-y-2">
                      <div className="text-[11px] font-medium text-tertiary-text tracking-wide">因子得分</div>
                      {factorEntries.map(([name, score]) => {
                        const pct = scoreItem.factor_weights?.[name] ?? 0;
                        return (
                          <div key={name} className="flex items-center gap-2">
                            <span className="text-[11px] text-secondary-text w-28 shrink-0 truncate text-right" title={`${FACTOR_LABELS[name] || name} (${pct}%)`}>
                              {FACTOR_LABELS[name] || name}
                              <span className="text-tertiary-text/60 ml-0.5">({pct}%)</span>
                            </span>
                            <div className="flex-1 h-2 rounded-full bg-border/15 overflow-hidden">
                              <div
                                className="h-full rounded-full bg-gradient-to-r from-cyan/60 to-blue/60"
                                style={{ width: `${Math.min(100, score)}%` }}
                              />
                            </div>
                            <span className="text-[11px] font-mono text-tertiary-text w-10 text-right">{score.toFixed(0)}</span>
                          </div>
                        );
                      })}
                    </div>
                    {/* 右：技术得分 */}
                    <div className="space-y-2">
                      <div className="text-[11px] font-medium text-tertiary-text tracking-wide">技术评分</div>
                      {(() => {
                        const entries = (scoreItem.tech_score_breakdown ? Object.entries(scoreItem.tech_score_breakdown) : [])
                          .map(([key, val]) => ({ key, val, dim: techDims.find(d => d.key === key) }))
                          .filter(e => e.dim && e.val)
                          .sort((a, b) => (scoreItem.tech_score_weights?.[b.key] ?? 0) - (scoreItem.tech_score_weights?.[a.key] ?? 0));
                        return entries.map(({ key, val, dim }) => {
                          const pct = Math.round((scoreItem.tech_score_weights?.[key] ?? 0) * 100);
                          return (
                          <div key={key} className="flex items-center gap-2">
                            <span className="text-[11px] text-secondary-text w-28 shrink-0 truncate text-right">{dim!.label}<span className="text-tertiary-text/60 ml-0.5">({pct}%)</span></span>
                            <div className="flex-1 h-1.5 rounded-full bg-border/15 overflow-hidden">
                              <div
                                className="h-full rounded-full bg-gradient-to-r from-cyan/40 to-blue/40"
                                style={{ width: `${Math.min(100, val)}%` }}
                              />
                            </div>
                            <span className="text-[11px] font-mono text-tertiary-text w-10 text-right">{Number(val).toFixed(0)}</span>
                          </div>
                        );
                      })})()}
                    </div>
                  </div>
                  </div>
                );
              },
            }}
            columns={[
              { title: '代码', dataIndex: 'stock_code', width: 100, render: (v: string) => <span className="font-mono">{v}</span> },
              { title: '名称', dataIndex: 'stock_name', width: 100 },
              { title: '当前价', width: 80, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                return s?.current_price ? <span className="font-mono text-foreground">{s.current_price.toFixed(2)}</span> : '-';
              }},
              { title: '买入区间', width: 140, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                if (s?.buy_price_low == null) return '-';
                const lo = s.buy_price_low.toFixed(2);
                const hi = s.buy_price_high != null && s.buy_price_high !== s.buy_price_low ? s.buy_price_high.toFixed(2) : null;
                return <span className="font-mono text-cyan">{lo}{hi ? ` ~ ${hi}` : ''}</span>;
              }},
              { title: '止损', width: 80, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                return s?.stop_loss ? <span className="font-mono text-emerald-400">{s.stop_loss.toFixed(2)}</span> : '-';
              }},
              { title: '止盈', width: 80, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                return s?.take_profit_1 ? <span className="font-mono text-red-400">{s.take_profit_1.toFixed(2)}</span> : '-';
              }},
              { title: '综合分', width: 70, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                return s ? <span className="font-mono text-cyan">{s.composite_score > 0 ? s.composite_score.toFixed(1) : '-'}</span> : '-';
              }},
              { title: '因子分', width: 70, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                return s ? <span className="font-mono text-secondary-text">{s.total_score.toFixed(1)}</span> : '-';
              }},
              { title: '技术分', width: 70, render: (_: any, r) => {
                const s = r.intraday || r.postmarket;
                return s ? <span className="font-mono text-tertiary-text">{s.tech_score > 0 ? s.tech_score.toFixed(1) : '-'}</span> : '-';
              }},
              { title: '板块', width: 100, render: (_: any, r) => (r.intraday || r.postmarket)?.sector || '-' },
            ]}
          />
        </div>
      )}

      {/* ═══════════════════════════════
          INTRA DAY
          ═══════════════════════════════ */}
      {tab === 'intraday' && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-3 text-[11px] text-tertiary-text">
            {intraday?.updated && (
              <span className="inline-flex items-center gap-1.5">
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                更新 {new Date(intraday.updated).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
              </span>
            )}
            {intraday?.round ? <span>· 第 {intraday.round} 轮</span> : null}
            <button onClick={() => void fetchIntraday(true)} className="inline-flex items-center gap-1 text-cyan hover:underline transition-colors">
              <RefreshCw className="h-3 w-3" /> 刷新
            </button>
            <span className="text-tertiary-text/40">· 60s 自动</span>

            {/* 全市场 / 白名单 / 金股 切换 */}
            <div className="ml-auto inline-flex rounded-md border border-border/30 overflow-hidden shrink-0">
              <button
                onClick={() => { setIntradayScanMode(prev => ({ ...prev, scan_universe: 'full_market' })); discoveryApi.setScanMode('full_market', 'intraday').then(setIntradayScanMode).then(() => fetchIntraday(true)).catch(() => {}); }}
                className={`px-2.5 py-1 text-[11px] transition-colors ${intradayScanMode.scan_universe === 'full_market' ? 'bg-cyan/20 text-cyan font-medium' : 'text-tertiary-text hover:text-secondary-text'}`}
              >
                全市场
              </button>
              <button
                onClick={() => { if (!intradayScanMode.has_whitelist) return; setIntradayScanMode(prev => ({ ...prev, scan_universe: 'whitelist' })); discoveryApi.setScanMode('whitelist', 'intraday').then(setIntradayScanMode).then(() => fetchIntraday(true)).catch(() => {}); }}
                className={`px-2.5 py-1 text-[11px] border-l border-border/30 transition-colors ${
                  !intradayScanMode.has_whitelist
                    ? 'text-tertiary-text/40 cursor-not-allowed'
                    : intradayScanMode.scan_universe === 'whitelist'
                    ? 'bg-cyan/20 text-cyan font-medium'
                    : 'text-tertiary-text hover:text-secondary-text'
                }`}
                title={!intradayScanMode.has_whitelist ? '未配置 DISCOVERY_STOCK_WHITELIST' : undefined}
              >
                白名单
              </button>
              <button
                onClick={() => { setIntradayScanMode(prev => ({ ...prev, scan_universe: 'broker_gold' })); discoveryApi.setScanMode('broker_gold', 'intraday').then(setIntradayScanMode).then(() => fetchIntraday(true)).catch(() => {}); }}
                className={`px-2.5 py-1 text-[11px] border-l border-border/30 transition-colors ${intradayScanMode.scan_universe === 'broker_gold' ? 'bg-cyan/20 text-cyan font-medium' : 'text-tertiary-text hover:text-secondary-text'}`}
              >
                金股
              </button>
            </div>
          </div>

          <BacktestCard
            data={backtestByTab['intraday'] ?? null}
            loading={backtestLoading}
            error={tab === 'intraday' ? backtestError : null}
            startDate={btStartDate}
            endDate={btEndDate}
            onStartDate={setBtStartDate}
            onEndDate={setBtEndDate}
            onRefresh={() => fetchBacktest('intraday')}
          />

          <div className="mb-5">
            <Segmented
              value={resultSubTab}
              onChange={(v) => setResultSubTab(v as string)}
              options={[
                { label: '综合排名', value: 'composite' },
                { label: '因子Top4', value: 'factor-tops' },
              ]}
              block
            />
          </div>

          {resultSubTab === 'composite' ? (
            !hasCards ? (
              <EmptyState
                title="暂无盘中扫描结果"
                description={intraday === null ? '加载中...' : '扫描器未运行或非盘中交易时段（9:30-15:00）'}
                icon={<TrendingUp className="h-8 w-8 text-tertiary-text" />}
              />
            ) : cardGrid
          ) : (
            <FactorTopsCard data={factorTops} loading={factorTopsLoading} />
          )}

        </div>
      )}

      {/* ═══════════════════════════════
          POST MARKET
          ═══════════════════════════════ */}
      {tab === 'postmarket' && (
        <div className="space-y-5">
          <div className="flex flex-wrap items-center gap-2.5 rounded-2xl border border-border/30 bg-card/55 px-3 py-2.5">
            <Button
              variant="primary"
              onClick={runDiscovery}
              disabled={running}
              className="inline-flex h-9 items-center gap-2 rounded-xl border-0 bg-gradient-to-r from-cyan to-blue px-4 text-sm font-medium text-white shadow-md shadow-cyan/15 transition-all hover:shadow-lg hover:shadow-cyan/20"
            >
              {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <Compass className="h-4 w-4" />}
              {running ? '正在发现...' : '立即运行盘后发现'}
            </Button>
            {reportDate && (
              <span className="inline-flex h-9 items-center rounded-xl border border-border/40 bg-muted/25 px-3 text-xs font-medium tracking-wide text-secondary-text">
                报告日期 {reportDate}
              </span>
            )}
            {liveRescored && (
              <span className="inline-flex h-9 items-center gap-1.5 rounded-xl border border-emerald-400/30 bg-emerald-400/10 px-3 text-xs font-medium text-emerald-400">
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                盘中实时评分
              </span>
            )}

            {/* 全市场 / 白名单 / 金股 切换 */}
            <div className="ml-auto inline-flex rounded-md border border-border/30 overflow-hidden shrink-0">
              <button
                onClick={() => { setPostmarketScanMode(prev => ({ ...prev, scan_universe: 'full_market' })); discoveryApi.setScanMode('full_market', 'postmarket').then(setPostmarketScanMode).catch(() => {}); }}
                className={`px-2.5 py-1 text-[11px] transition-colors ${postmarketScanMode.scan_universe === 'full_market' ? 'bg-cyan/20 text-cyan font-medium' : 'text-tertiary-text hover:text-secondary-text'}`}
              >
                全市场
              </button>
              <button
                onClick={() => { if (!postmarketScanMode.has_whitelist) return; setPostmarketScanMode(prev => ({ ...prev, scan_universe: 'whitelist' })); discoveryApi.setScanMode('whitelist', 'postmarket').then(setPostmarketScanMode).catch(() => {}); }}
                className={`px-2.5 py-1 text-[11px] border-l border-border/30 transition-colors ${
                  !postmarketScanMode.has_whitelist
                    ? 'text-tertiary-text/40 cursor-not-allowed'
                    : postmarketScanMode.scan_universe === 'whitelist'
                    ? 'bg-cyan/20 text-cyan font-medium'
                    : 'text-tertiary-text hover:text-secondary-text'
                }`}
                title={!postmarketScanMode.has_whitelist ? '未配置 DISCOVERY_STOCK_WHITELIST' : undefined}
              >
                白名单
              </button>
              <button
                onClick={() => { setPostmarketScanMode(prev => ({ ...prev, scan_universe: 'broker_gold' })); discoveryApi.setScanMode('broker_gold', 'postmarket').then(setPostmarketScanMode).catch(() => {}); }}
                className={`px-2.5 py-1 text-[11px] border-l border-border/30 transition-colors ${postmarketScanMode.scan_universe === 'broker_gold' ? 'bg-cyan/20 text-cyan font-medium' : 'text-tertiary-text hover:text-secondary-text'}`}
              >
                金股
              </button>
            </div>
          </div>

          <BacktestCard
            data={backtestByTab['postmarket'] ?? null}
            loading={backtestLoading}
            error={tab === 'postmarket' ? backtestError : null}
            startDate={btStartDate}
            endDate={btEndDate}
            onStartDate={setBtStartDate}
            onEndDate={setBtEndDate}
            onRefresh={() => fetchBacktest('postmarket')}
          />

          <div className="mb-5">
            <Segmented
              value={resultSubTab}
              onChange={(v) => setResultSubTab(v as string)}
              options={[
                { label: '综合排名', value: 'composite' },
                { label: '因子Top4', value: 'factor-tops' },
              ]}
              block
            />
          </div>

          {resultSubTab === 'composite' ? (
            loading ? (
              <div className="flex items-center gap-2 py-16 text-secondary-text justify-center">
                <Loader2 className="h-4 w-4 animate-spin" /> 加载中...
              </div>
            ) : !hasCards ? (
              <EmptyState
                title="暂无盘后发现报告"
                description="点击上方按钮运行多因子深度发现，自动生成 Top 10 推荐及买卖点位"
                icon={<Compass className="h-8 w-8 text-tertiary-text" />}
              />
            ) : (
              cardGrid
            )
          ) : (
            <FactorTopsCard data={factorTops} loading={factorTopsLoading} />
          )}
        </div>
      )}
    </AppPage>
  );
};

export default DiscoveryPage;
