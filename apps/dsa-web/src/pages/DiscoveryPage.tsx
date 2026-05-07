import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Compass, RefreshCw, TrendingUp, TrendingDown,
  Loader2, ArrowUp, ArrowDown, Sparkles,
  ChevronDown, Target, Shield, Zap, Gauge,
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { AppPage, Button, EmptyState } from '../components/common';
import { discoveryApi, type DiscoveryItem, type BacktestResponse } from '../api/discovery';

type TabKey = 'intraday' | 'postmarket';
const AUTO_REFRESH_MS = 60_000;
const MIN_INTRADAY_FETCH_GAP_MS = 60_000;

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

  // A-share regular session (CN): Mon-Fri, 09:30-15:00.
  const isWeekday = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday);
  const isIntraday = minuteOfDay >= (9 * 60 + 30) && minuteOfDay < (15 * 60);

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
  momentum: '动量',
  rebound: '反弹',
  sector: '板块',
  ma_entry: '均线',
};

const factorLabel = (key: string) => {
  const zh = FACTOR_LABELS[key];
  return zh ? `${key}（${zh}）` : key;
};

const FactorBar: React.FC<{ label: string; value: number }> = ({ label, value }) => {
  const pct = Math.min(100, Math.max(0, value));
  const hue = pct >= 70 ? '193 100% 43%' : pct >= 40 ? '37 92% 50%' : '224 12% 42%';

  return (
    <div className="flex items-center gap-2.5 text-[11px]">
      <span className="w-28 shrink-0 text-tertiary-text text-right truncate" title={label}>{label}</span>
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

const calcItemPnLRatio = (item: DiscoveryItem): number | null => {
  const refPrice = getRefPrice(item);
  const profitPct = calcPctFromBase(refPrice, item.take_profit_1);
  const lossPctRaw = calcPctFromBase(refPrice, item.stop_loss);
  const lossPct = lossPctRaw != null ? Math.abs(lossPctRaw) : null;
  return calcPnLRatio(profitPct, lossPct);
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
              <span className="text-[13px] text-secondary-text">{item.stock_name}</span>
              {item.sector && (
                <span className="rounded-md border border-border/40 bg-muted/30 px-1.5 py-0.5 text-[10px] text-tertiary-text">{item.sector}</span>
              )}
              {ch && (
                <span className={`inline-flex items-center gap-1 rounded-lg border px-1.5 py-0.5 text-[11px] font-medium ${ch.cls}`}>
                  {ch.icon}{ch.label}
                </span>
              )}
              {item.discovered_at && (
                <span className="text-[15px] font-semibold text-foreground">{item.discovered_at} 发现</span>
              )}
              {item.price_at_discovery != null && (
                <span className="text-[15px] font-semibold text-foreground">· ¥{item.price_at_discovery.toFixed(2)}</span>
              )}
              {item.live_price != null && item.price_at_discovery != null && (
                <span className={`text-[15px] font-semibold tabular-nums ${item.live_price >= item.price_at_discovery ? 'text-red-400' : 'text-emerald-400'}`}>
                  → ¥{item.live_price.toFixed(2)}
                </span>
              )}
              {item.live_price != null && item.price_at_discovery == null && (
                <span className="text-[15px] font-semibold text-foreground">→ ¥{item.live_price.toFixed(2)}</span>
              )}
            </div>
          </div>

          {/* Score */}
          <ScoreRing score={item.score} />

          {/* Chevron */}
          <div className={`shrink-0 text-tertiary-text/50 transition-transform duration-200 ${open ? 'rotate-180' : ''}`}>
            <ChevronDown className="h-4 w-4" />
          </div>
        </div>

        {/* Prices: keep key buy/sell points visible when collapsed */}
        {px && (
          <div className="space-y-2">
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
              <div className="rounded-xl border border-cyan/15 bg-cyan/[0.06] px-3 py-2">
                <div className="mb-1 flex items-center gap-1 text-[10px] text-cyan/80">
                  <Target className="h-3 w-3" />
                  买入区间
                </div>
                <div className="text-sm font-semibold tabular-nums text-cyan">{buyRange}</div>
              </div>
              <div className="rounded-xl border border-red-400/15 bg-red-400/[0.06] px-3 py-2">
                <div className="mb-1 flex items-center gap-1 text-[10px] text-red-400/80">
                  <Zap className="h-3 w-3" />
                  止盈 1
                </div>
                <div className="text-sm font-semibold tabular-nums text-red-400">{fmtPx(item.take_profit_1)}</div>
              </div>
              <div className="rounded-xl border border-emerald-400/15 bg-emerald-400/[0.06] px-3 py-2">
                <div className="mb-1 flex items-center gap-1 text-[10px] text-emerald-400/80">
                  <Shield className="h-3 w-3" />
                  止损
                </div>
                <div className="text-sm font-semibold tabular-nums text-emerald-400">{fmtPx(item.stop_loss)}</div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2 text-xs">
              <span className="rounded-lg border border-cyan/20 bg-cyan/[0.1] px-2.5 py-1 font-semibold text-cyan">
                盈亏比 {pnlRatio != null ? `${pnlRatio.toFixed(2)} : 1` : '--'}
              </span>
              <span className="rounded-lg border border-red-400/20 bg-red-400/[0.08] px-2.5 py-1 font-medium text-red-300">
                预期盈利 {fmtPct(profitPct)}
              </span>
              <span className="rounded-lg border border-emerald-400/20 bg-emerald-400/[0.08] px-2.5 py-1 font-medium text-emerald-300">
                预期亏损 {fmtPct(lossPct)}
              </span>
              {refPrice != null && (
                <span className="text-[15px] font-semibold text-foreground">
                  {item.price_at_discovery != null && item.price_at_discovery > 0
                    ? <>发现价 ¥{item.price_at_discovery.toFixed(2)}{item.live_price != null && <span className={item.live_price >= item.price_at_discovery ? 'text-red-400' : 'text-emerald-400'}> → ¥{item.live_price.toFixed(2)}</span>}</>
                    : `基准买入价 ${refPrice.toFixed(2)}（区间中位）`}
                </span>
              )}
            </div>
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
                        {r}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Grid: prices + factors */}
              <div className="mt-4 grid gap-4 sm:grid-cols-2">
                {px && (
                  <div className="grid grid-cols-2 gap-2">
                    {([
                      { label: '买入区间', v: item.buy_price_low != null
                        ? `${fmtPx(item.buy_price_low)}${item.buy_price_high != null && item.buy_price_high !== item.buy_price_low ? ` — ${fmtPx(item.buy_price_high)}` : ''}`
                        : '--', c: 'border-cyan/10 bg-cyan/[0.03] text-cyan', ic: Target },
                      { label: '止盈 1', v: fmtPx(item.take_profit_1), c: 'border-red-400/10 bg-red-400/[0.03] text-red-400', ic: Zap },
                      { label: '止盈 2', v: fmtPx(item.take_profit_2), c: 'border-red-400/10 bg-red-400/[0.03] text-red-400', ic: Zap },
                      { label: '止损', v: fmtPx(item.stop_loss), c: 'border-emerald-400/10 bg-emerald-400/[0.03] text-emerald-400', ic: Shield },
                    ] as const).map(({ label, v, c, ic: Ic }) => (
                      <div key={label} className={`rounded-xl border p-3 text-center ${c}`}>
                        <div className="text-[10px] text-current/60 mb-1 flex items-center justify-center gap-1">
                          <Ic className="h-2.5 w-2.5" />{label}
                        </div>
                        <div className="text-sm font-bold">{v}</div>
                      </div>
                    ))}
                  </div>
                )}

                {item.factor_scores && Object.keys(item.factor_scores).length > 0 && (
                  <div className="space-y-2.5">
                    <div className="text-[11px] font-medium text-tertiary-text tracking-wide">因子得分</div>
                    {Object.entries(item.factor_scores).map(([k, v]) => (
                      <FactorBar key={k} label={factorLabel(k)} value={v} />
                    ))}
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
   7. Backtest Card
   ────────────────────────────────────────────── */

const fmtWan = (v: number) => `${(v / 10000).toFixed(1)}万`;
const fmtDate = (s: string) => `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;

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
  if (raw.length < 2) return null;

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

  const pads = { t: 10, r: 6, b: 22, l: 48 };
  const chartW = width;
  const chartH = height - pads.t - pads.b;
  const count = ohlcData.length;
  const xStep = (chartW - pads.l - pads.r) / Math.max(count - 1, 1);
  const candleW = Math.max(Math.min(xStep * 0.6, 12), 3);

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
  const xTickInterval = Math.max(Math.ceil(count / 6), 1);

  return (
    <div ref={containerRef} style={{ position: 'relative', width: '100%' }}>
      <svg width={chartW} height={height} style={{ display: 'block' }}>
        {/* Grid */}
        {yTicks.map((v, i) => {
          const y = scaleY(v);
          return (
            <g key={`g-${i}`}>
              <line x1={pads.l} x2={chartW - pads.r} y1={y} y2={y} stroke="hsl(var(--border))" strokeWidth={0.5} opacity={0.4} />
              <text x={pads.l - 4} y={y + 3} textAnchor="end" fill="hsl(var(--muted-foreground))" fontSize={9} fontFamily="monospace">
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
          const x = pads.l + i * xStep;
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
        {ohlcData.map((d, i) => {
          if (i % xTickInterval !== 0 && i !== count - 1) return null;
          const x = pads.l + i * xStep;
          return (
            <text key={`xl-${i}`} x={x} y={height - 4} textAnchor="middle"
              fill="hsl(var(--muted-foreground))" fontSize={9} fontFamily="monospace">{d.date}</text>
          );
        })}
      </svg>

      {/* Tooltip */}
      {hoverIdx != null && ohlcData[hoverIdx] && (() => {
        const d = ohlcData[hoverIdx];
        const r = raw[hoverIdx]; // absolute values
        const isUp = d.close >= d.open;
        const chgColor = isUp ? '#ef4444' : '#10b981';
        const dayChg = d.open !== 0 ? ((d.close - d.open) / Math.abs(d.open) * 100) : null;
        const cumChg = d.close;
        const xPx = pads.l + hoverIdx * xStep;
        const isRight = xPx > chartW * 0.65;
        return (
          <div style={{
            position: 'absolute', top: 4,
            [isRight ? 'right' : 'left']: isRight ? 8 : `${(xPx / chartW) * 100}%`,
            transform: isRight ? 'none' : 'translateX(-50%)',
            background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))',
            borderRadius: 8, padding: '6px 10px', fontSize: 11, zIndex: 10,
            whiteSpace: 'nowrap', pointerEvents: 'none',
            boxShadow: '0 2px 12px rgba(0,0,0,0.3)',
          }}>
            <div style={{ color: '#9ca3af', marginBottom: 3, fontSize: 10 }}>{d.date}</div>
            <div style={{ fontFamily: 'monospace' }}>
              <span style={{ color: '#9ca3af' }}>O </span>{fmtPct(d.open)}
              <span style={{ color: '#9ca3af', marginLeft: 6 }}>H </span>{fmtPct(d.high)}
            </div>
            <div style={{ fontFamily: 'monospace' }}>
              <span style={{ color: '#9ca3af' }}>L </span>{fmtPct(d.low)}
              <span style={{ color: '#9ca3af', marginLeft: 6 }}>C </span>
              <span style={{ color: chgColor }}>{fmtPct(d.close)}</span>
            </div>
            {dayChg != null && (
              <div style={{ color: chgColor, fontFamily: 'monospace', marginTop: 2 }}>
                当日 {dayChg >= 0 ? '+' : ''}{dayChg.toFixed(2)}%
              </div>
            )}
            <div style={{ marginTop: 2, borderTop: '1px solid hsl(var(--border))', paddingTop: 3, fontSize: 10, color: '#9ca3af' }}>
              <span>累计 {cumChg >= 0 ? '+' : ''}{cumChg.toFixed(2)}%</span>
              <span style={{ marginLeft: 8 }}>资金 ¥{r.capital.toLocaleString()}</span>
            </div>
          </div>
        );
      })()}
    </div>
  );
};

const BacktestCard: React.FC<{
  data: BacktestResponse;
  loading: boolean;
  startDate: string;
  endDate: string;
  onStartDate: (v: string) => void;
  onEndDate: (v: string) => void;
  onRefresh: () => void;
}> = ({ data, loading, startDate, endDate, onStartDate, onEndDate, onRefresh }) => {
  const [section, setSection] = useState<'chart' | 'trades'>('chart');

  if (loading) {
    return (
      <div className="rounded-xl border border-border/20 bg-card/40 px-4 py-3 text-[12px] text-tertiary-text">
        <Loader2 className="inline h-3 w-3 animate-spin mr-1.5" />加载回测数据...
      </div>
    );
  }

  if (!data) {
    return (
      <div className="rounded-xl border border-border/20 bg-card/40 px-4 py-3 text-[12px] text-tertiary-text">
        <Loader2 className="inline h-3 w-3 animate-spin mr-1.5" />加载回测数据...
      </div>
    );
  }

  const isPositive = data.cumulative_return >= 0;
  const pct = data.total_days > 0 ? (data.cumulative_return * 100).toFixed(2) : '--';
  const wrPct = data.total_days > 0 ? (data.win_rate * 100).toFixed(0) : '--';
  const pnlSign = data.total_pnl >= 0 ? '+' : '';
  const initCapital = data.initial_capital || 5_000_000;
  const initialLine = initCapital;
  const isPostmarket = data.mode === 'postmarket';
  const chartData = data.capital_curve.length > 0
    ? data.capital_curve.map(p => ({
        date: fmtDate(p.date),
        capital: p.capital,
        ...(p.open != null && { open: p.open, high: p.high, low: p.low, close: p.close }),
      }))
    : [{ date: fmtDate(new Date().toISOString().slice(0, 10).replace(/-/g, '')), capital: initCapital }];

  return (
    <div className="rounded-xl border border-border/20 bg-card/40 overflow-hidden">
      {/* ── Summary bar ── */}
      <div className="px-4 py-3 flex flex-wrap items-center gap-x-5 gap-y-1.5 text-[12px] border-b border-border/15">
        <span className="text-tertiary-text text-[11px] font-medium tracking-wide">回测</span>

        <div className="flex items-center gap-3">
          <span className={`font-bold text-sm tabular-nums ${isPositive ? 'text-red-400' : 'text-emerald-400'}`}>
            {isPositive ? '+' : ''}{pct}%
          </span>
          <span className="text-tertiary-text">
            胜率 <span className="text-foreground font-medium">{wrPct}%</span>
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

        {/* Date filter */}
        <div className="ml-auto flex items-center gap-1.5">
          <input
            type="date"
            value={startDate ? fmtDate(startDate) : ''}
            min="2026-05-01"
            max={new Date().toISOString().slice(0, 10)}
            onChange={e => onStartDate(e.target.value.replace(/-/g, ''))}
            onClick={e => e.stopPropagation()}
            className="h-7 w-28 rounded-lg border border-border/30 bg-muted/30 px-2 text-[11px] text-foreground"
          />
          <span className="text-tertiary-text text-[11px]">-</span>
          <input
            type="date"
            value={endDate ? fmtDate(endDate) : ''}
            min="2026-05-01"
            max={new Date().toISOString().slice(0, 10)}
            onChange={e => onEndDate(e.target.value.replace(/-/g, ''))}
            onClick={e => e.stopPropagation()}
            className="h-7 w-28 rounded-lg border border-border/30 bg-muted/30 px-2 text-[11px] text-foreground"
          />
          <button
            onClick={e => { e.stopPropagation(); onRefresh(); }}
            className="h-7 px-2 rounded-lg border border-border/30 bg-muted/30 text-[11px] text-cyan hover:bg-cyan/10 transition-colors"
          >
            查询
          </button>
        </div>
      </div>

      {/* ── Tab switcher ── */}
      <div className="flex border-b border-border/10">
        <button
          onClick={() => setSection('chart')}
          className={`px-4 py-1.5 text-[11px] font-medium transition-colors ${section === 'chart' ? 'text-cyan border-b border-cyan' : 'text-tertiary-text hover:text-secondary-text'}`}
        >
          收益曲线
        </button>
        <button
          onClick={() => setSection('trades')}
          className={`px-4 py-1.5 text-[11px] font-medium transition-colors ${section === 'trades' ? 'text-cyan border-b border-cyan' : 'text-tertiary-text hover:text-secondary-text'}`}
        >
          交易记录
        </button>
      </div>

      {/* ── Chart ── */}
      {section === 'chart' && (
        <div className="px-2 py-3">
          {isPostmarket && chartData.some(d => d.open != null) ? (
            <PortfolioCandleChart data={chartData} initCapital={initCapital} height={200} />
          ) : (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={chartData}>
                <XAxis dataKey="date" tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }} stroke="hsl(var(--border))" />
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
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-card/90 text-tertiary-text">
              <tr>
                <th className="px-3 py-2 text-left font-medium">股票</th>
                <th className="px-2 py-2 text-right font-medium">买入日</th>
                <th className="px-2 py-2 text-right font-medium">买入价</th>
                <th className="px-2 py-2 text-right font-medium">卖出日</th>
                <th className="px-2 py-2 text-right font-medium">卖出价</th>
                <th className="px-2 py-2 text-right font-medium">收益%</th>
                <th className="px-2 py-2 text-right font-medium">盈亏</th>
              </tr>
            </thead>
            <tbody>
              {[...data.trade_records].reverse().map((t, i) => (
                <tr key={`${t.stock_code}-${t.buy_date}-${i}`} className="border-t border-border/10 hover:bg-foreground/[0.02]">
                  <td className="px-3 py-1.5">
                    <span className="font-medium text-foreground">{t.stock_code}</span>
                    <span className="text-tertiary-text ml-1">{t.stock_name}</span>
                  </td>
                  <td className="px-2 py-1.5 text-right text-tertiary-text">{fmtDate(t.buy_date)}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{t.buy_price.toFixed(2)}</td>
                  <td className="px-2 py-1.5 text-right text-tertiary-text">{fmtDate(t.sell_date)}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{t.sell_price.toFixed(2)}</td>
                  <td className={`px-2 py-1.5 text-right font-medium tabular-nums ${t.return_pct >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                    {t.return_pct >= 0 ? '+' : ''}{(t.return_pct * 100).toFixed(2)}%
                  </td>
                  <td className={`px-2 py-1.5 text-right font-medium tabular-nums ${t.pnl >= 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                    {t.pnl >= 0 ? '+' : ''}{t.pnl.toFixed(0)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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
  const [report, setReport] = useState<string | null>(null);
  const [postTopN, setPostTopN] = useState<DiscoveryItem[]>([]);
  const [reportDate, setReportDate] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [backtest, setBacktest] = useState<BacktestResponse | null>(null);
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [btStartDate, setBtStartDate] = useState<string>('');
  const [btEndDate, setBtEndDate] = useState<string>('');
  const intradayFetchInFlightRef = useRef(false);
  const intradayLastFetchAtRef = useRef(0);

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

  const fetchReport = useCallback(async () => {
    try {
      setLoading(true);
      const d = await discoveryApi.getPostmarketReport();
      setReport(d.exists ? d.report : null);
      setPostTopN(d.top_n ?? []);
      setReportDate(d.date ?? null);
      setError(null);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : 'err'); }
    finally { setLoading(false); }
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
    try {
      const opts: { days?: number; start_date?: string; end_date?: string } = {};
      if (btStartDate) opts.start_date = btStartDate;
      if (btEndDate) opts.end_date = btEndDate;
      if (!btStartDate && !btEndDate) opts.days = 60;
      const d = await discoveryApi.getBacktest(mode, opts);
      setBacktest(d);
    } catch { /* silent */ }
    finally { setBacktestLoading(false); }
  }, [btStartDate, btEndDate]);

  useEffect(() => {
    if (tab === 'intraday') { fetchIntraday(); fetchBacktest('intraday'); }
    else { fetchReport(); fetchBacktest('postmarket'); }
  }, [tab, fetchIntraday, fetchReport, fetchBacktest]);
  useEffect(() => {
    if (tab !== 'intraday') return;
    const id = setInterval(fetchIntraday, AUTO_REFRESH_MS);
    return () => clearInterval(id);
  }, [tab, fetchIntraday]);
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
  const sortedCardList = useMemo(() => {
    return [...cardList].sort((a, b) => {
      const ratioA = calcItemPnLRatio(a);
      const ratioB = calcItemPnLRatio(b);
      if (ratioA == null && ratioB == null) return a.rank - b.rank;
      if (ratioA == null) return 1;
      if (ratioB == null) return -1;
      return ratioB - ratioA;
    });
  }, [cardList]);
  const hasCards = sortedCardList.length > 0;

  /* ── Card grid ── */
  const cardGrid = (
    <div className="grid gap-2">
      <AnimatePresence>
        {sortedCardList.map((item) => (
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
    <AppPage>
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

      {/* ═══════════════════════════════
          INTRA DAY
          ═══════════════════════════════ */}
      {tab === 'intraday' && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-3 text-[11px] text-tertiary-text">
            {intraday?.updated && (
              <span className="inline-flex items-center gap-1.5">
                <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                更新 {new Date(intraday.updated).toLocaleTimeString('zh-CN')}
              </span>
            )}
            {intraday?.round ? <span>· 第 {intraday.round} 轮</span> : null}
            <button onClick={() => void fetchIntraday(true)} className="inline-flex items-center gap-1 text-cyan hover:underline transition-colors">
              <RefreshCw className="h-3 w-3" /> 刷新
            </button>
            <span className="text-tertiary-text/40">· 60s 自动</span>
          </div>

          <BacktestCard
            data={backtest!}
            loading={backtestLoading}
            startDate={btStartDate}
            endDate={btEndDate}
            onStartDate={setBtStartDate}
            onEndDate={setBtEndDate}
            onRefresh={() => fetchBacktest('intraday')}
          />

          {!hasCards ? (
            <EmptyState
              title="暂无盘中扫描结果"
              description={intraday === null ? '加载中...' : '扫描器未运行或非盘中交易时段（9:30-15:00）'}
              icon={<TrendingUp className="h-8 w-8 text-tertiary-text" />}
            />
          ) : cardGrid}

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
          </div>

          <BacktestCard
            data={backtest!}
            loading={backtestLoading}
            startDate={btStartDate}
            endDate={btEndDate}
            onStartDate={setBtStartDate}
            onEndDate={setBtEndDate}
            onRefresh={() => fetchBacktest('postmarket')}
          />

          {loading ? (
            <div className="flex items-center gap-2 py-16 text-secondary-text justify-center">
              <Loader2 className="h-4 w-4 animate-spin" /> 加载中...
            </div>
          ) : !report ? (
            <EmptyState
              title="暂无盘后发现报告"
              description="点击上方按钮运行多因子深度发现，自动生成 Top 10 推荐及买卖点位"
              icon={<Compass className="h-8 w-8 text-tertiary-text" />}
            />
          ) : (
            <>
              {hasCards && cardGrid}
            </>
          )}
        </div>
      )}
    </AppPage>
  );
};

export default DiscoveryPage;
