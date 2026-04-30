import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Compass, RefreshCw, TrendingUp, TrendingDown,
  Loader2, ArrowUp, ArrowDown, Minus, Sparkles,
  ChevronDown, Target, Shield, Zap, Gauge,
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { AppPage, Button, EmptyState } from '../components/common';
import { discoveryApi, type DiscoveryItem } from '../api/discovery';

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

const FactorBar: React.FC<{ label: string; value: number }> = ({ label, value }) => {
  const pct = Math.min(100, Math.max(0, value));
  const hue = pct >= 70 ? '193 100% 43%' : pct >= 40 ? '37 92% 50%' : '224 12% 42%';

  return (
    <div className="flex items-center gap-2.5 text-[11px]">
      <span className="w-14 shrink-0 text-tertiary-text text-right">{label}</span>
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

const calcItemPnLRatio = (item: DiscoveryItem): number | null => {
  const buyRef = calcBuyRef(item.buy_price_low, item.buy_price_high);
  const profitPct = calcPctFromBase(buyRef, item.take_profit_1);
  const lossPctRaw = calcPctFromBase(buyRef, item.stop_loss);
  const lossPct = lossPctRaw != null ? Math.abs(lossPctRaw) : null;
  return calcPnLRatio(profitPct, lossPct);
};

const parseNumber = (v: string): number | null => {
  const n = Number(v.trim());
  return Number.isFinite(n) ? n : null;
};

const parsePriceRange = (value: string): { low: number | null; high: number | null } => {
  const nums = value.match(/\d+(?:\.\d+)?/g) ?? [];
  if (nums.length === 0) return { low: null, high: null };
  if (nums.length === 1) {
    const n = parseNumber(nums[0]);
    return { low: n, high: n };
  }
  return {
    low: parseNumber(nums[0]!),
    high: parseNumber(nums[1]!),
  };
};

const parseReportTopN = (md: string): DiscoveryItem[] => {
  const items: DiscoveryItem[] = [];
  const titleRegex = /^###\s+#(\d+)\s+([0-9A-Za-z.]+)\s+(.+?)\s+—\s+综合评分\s+([0-9.]+)\s*$/gm;
  const matches = Array.from(md.matchAll(titleRegex));

  for (let i = 0; i < matches.length; i++) {
    const match = matches[i];
    const start = match.index ?? 0;
    const end = i + 1 < matches.length ? (matches[i + 1].index ?? md.length) : md.length;
    const block = md.slice(start, end);

    const rank = Number(match[1]);
    const stock_code = match[2];
    const stock_name = match[3].trim();
    const score = Number(match[4]);
    if (!Number.isFinite(rank) || !Number.isFinite(score)) continue;

    const reasons = Array.from(block.matchAll(/^- (.+)$/gm)).map((m) => m[1].trim());
    const tableRow = block.match(/^\|\s*([^|\n]+)\s*\|\s*([^|\n]+)\s*\|\s*([^|\n]+)\s*\|\s*([^|\n]+)\s*\|$/gm);
    const dataRow = tableRow && tableRow.length > 0 ? tableRow[tableRow.length - 1] : null;

    let buy_price_low: number | null = null;
    let buy_price_high: number | null = null;
    let take_profit_1: number | null = null;
    let take_profit_2: number | null = null;
    let stop_loss: number | null = null;

    if (dataRow) {
      const cells = dataRow.split('|').map((c) => c.trim()).filter(Boolean);
      if (cells.length >= 4) {
        const range = parsePriceRange(cells[0]);
        buy_price_low = range.low;
        buy_price_high = range.high;
        take_profit_1 = parseNumber(cells[1]);
        take_profit_2 = parseNumber(cells[2]);
        stop_loss = parseNumber(cells[3]);
      }
    }

    const factor_scores: Record<string, number> = {};
    const factorMatch = block.match(/\*因子得分：([^\n*]+)\*/);
    if (factorMatch) {
      factorMatch[1]
        .split('|')
        .map((part) => part.trim())
        .forEach((pair) => {
          const [rawKey, rawValue] = pair.split(':').map((v) => v.trim());
          if (!rawKey || !rawValue) return;
          const value = Number(rawValue);
          if (Number.isFinite(value)) factor_scores[rawKey] = value;
        });
    }

    items.push({
      rank,
      stock_code,
      stock_name,
      score,
      reasons,
      buy_price_low,
      buy_price_high,
      take_profit_1,
      take_profit_2,
      stop_loss,
      factor_scores,
    });
  }

  return items;
};

const chCfg = (c?: string) => {
  switch (c) {
    case 'new': return { icon: <Sparkles className="h-3 w-3" />, label: '新进', cls: 'text-cyan bg-cyan/8 border-cyan/15' };
    case 'up': return { icon: <ArrowUp className="h-3 w-3" />, label: '上升', cls: 'text-emerald-400 bg-emerald-400/8 border-emerald-400/15' };
    case 'down': return { icon: <ArrowDown className="h-3 w-3" />, label: '下降', cls: 'text-amber-400 bg-amber-400/8 border-amber-400/15' };
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
  const buyRef = calcBuyRef(item.buy_price_low, item.buy_price_high);
  const profitPct = calcPctFromBase(buyRef, item.take_profit_1);
  const lossPctRaw = calcPctFromBase(buyRef, item.stop_loss);
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
        <div className="flex items-start gap-3.5">
          {/* Rank */}
          <div className={`mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-xl text-sm font-bold
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
              {ch && (
                <span className={`inline-flex items-center gap-1 rounded-lg border px-1.5 py-0.5 text-[11px] font-medium ${ch.cls}`}>
                  {ch.icon}{ch.label}
                </span>
              )}
            </div>
            <div className="mt-1 text-[11px] text-tertiary-text">
              点击展开查看推荐理由与因子评分
            </div>
          </div>

          {/* Score */}
          <ScoreRing score={item.score} />

          {/* Chevron */}
          <div className={`mt-4 shrink-0 text-tertiary-text/50 transition-transform duration-200 ${open ? 'rotate-180' : ''}`}>
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
              <div className="rounded-xl border border-emerald-400/15 bg-emerald-400/[0.06] px-3 py-2">
                <div className="mb-1 flex items-center gap-1 text-[10px] text-emerald-400/80">
                  <Zap className="h-3 w-3" />
                  止盈 1
                </div>
                <div className="text-sm font-semibold tabular-nums text-emerald-400">{fmtPx(item.take_profit_1)}</div>
              </div>
              <div className="rounded-xl border border-red-400/15 bg-red-400/[0.06] px-3 py-2">
                <div className="mb-1 flex items-center gap-1 text-[10px] text-red-400/80">
                  <Shield className="h-3 w-3" />
                  止损
                </div>
                <div className="text-sm font-semibold tabular-nums text-red-400">{fmtPx(item.stop_loss)}</div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2 text-xs">
              <span className="rounded-lg border border-cyan/20 bg-cyan/[0.1] px-2.5 py-1 font-semibold text-cyan">
                盈亏比 {pnlRatio != null ? `${pnlRatio.toFixed(2)} : 1` : '--'}
              </span>
              <span className="rounded-lg border border-emerald-400/20 bg-emerald-400/[0.08] px-2.5 py-1 font-medium text-emerald-300">
                预期盈利 {fmtPct(profitPct)}
              </span>
              <span className="rounded-lg border border-red-400/20 bg-red-400/[0.08] px-2.5 py-1 font-medium text-red-300">
                预期亏损 {fmtPct(lossPct)}
              </span>
              {buyRef != null && (
                <span className="text-[11px] text-tertiary-text">
                  基准买入价 {buyRef.toFixed(2)}（区间中位）
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
                      { label: '止盈 1', v: fmtPx(item.take_profit_1), c: 'border-emerald-400/10 bg-emerald-400/[0.03] text-emerald-400', ic: Zap },
                      { label: '止盈 2', v: fmtPx(item.take_profit_2), c: 'border-emerald-400/10 bg-emerald-400/[0.03] text-emerald-400', ic: Zap },
                      { label: '止损', v: fmtPx(item.stop_loss), c: 'border-red-400/10 bg-red-400/[0.03] text-red-400', ic: Shield },
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
                      <FactorBar key={k} label={k} value={v} />
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
   7. Page
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
    try { setRunning(true); setError(null); await discoveryApi.runPostmarketDiscovery(); await fetchReport(); }
    catch (e: unknown) { setError(e instanceof Error ? e.message : 'err'); }
    finally { setRunning(false); }
  }, [fetchReport]);

  useEffect(() => { if (tab === 'intraday') fetchIntraday(); else fetchReport(); }, [tab, fetchIntraday, fetchReport]);
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

  const parsedReportTopN = useMemo(() => {
    if (tab !== 'postmarket' || !report) return [];
    return parseReportTopN(report);
  }, [tab, report]);
  const postmarketCards = postTopN.length > 0 ? postTopN : parsedReportTopN;
  const cardList = useMemo(
    () => (tab === 'intraday' ? intraday?.top_n ?? [] : postmarketCards),
    [tab, intraday?.top_n, postmarketCards]
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

          {!hasCards ? (
            <EmptyState
              title="暂无盘中扫描结果"
              description={intraday === null ? '加载中...' : '扫描器未运行或非盘中交易时段（9:30-15:00）'}
              icon={<TrendingUp className="h-8 w-8 text-tertiary-text" />}
            />
          ) : cardGrid}

          {/* Dropped */}
          {intraday?.dropped && intraday.dropped.length > 0 && (
            <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className="mt-4">
              <p className="text-[11px] font-medium text-tertiary-text mb-2 tracking-wide">退出榜单</p>
              <div className="flex flex-wrap gap-2">
                {intraday.dropped.map(item => (
                  <span key={item.stock_code} className="inline-flex items-center gap-1 rounded-lg bg-muted/30 ring-1 ring-border/20 px-2.5 py-1 text-[11px] text-tertiary-text">
                    <Minus className="h-3 w-3" />{item.stock_code} {item.stock_name}
                  </span>
                ))}
              </div>
            </motion.div>
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
          </div>

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
