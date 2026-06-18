import type React from 'react';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import { ArrowUpRight, Loader2, RefreshCw } from 'lucide-react';
import { AppPage, Button, EmptyState } from '../components/common';
import { CandlestickMiniChart } from '../components/charts/CandlestickMiniChart';
import { getMonthlyRecommendations } from '../api/brokerRecommend';
import { marketApi, type HfqBollPickItem, type HfqNewHighItem } from '../api/market';

const normStockCode = (code: string) => code.split('.')[0].replace(/\D/g, '').slice(-6).padStart(6, '0');

type GoldHighlight = {
  current: Set<string>;
  prevOnly: Set<string>;
};

const EMPTY_GOLD: GoldHighlight = { current: new Set(), prevOnly: new Set() };

const goldRowClass = (record: HfqNewHighItem, gold: GoldHighlight): string => {
  const bare = normStockCode(record.ts_code || record.stock_code);
  if (gold.current.has(bare)) return 'bg-cyan-500/10';
  if (gold.prevOnly.has(bare)) return 'bg-amber-500/10';
  return '';
};

const getGoldFlags = (code: string, gold: GoldHighlight) => {
  const bare = normStockCode(code);
  const isCurrentGold = gold.current.has(bare);
  const isPrevGold = !isCurrentGold && gold.prevOnly.has(bare);
  return { isCurrentGold, isPrevGold };
};

const goldCardClass = (code: string, gold: GoldHighlight): string => {
  const { isCurrentGold, isPrevGold } = getGoldFlags(code, gold);
  if (isCurrentGold) return 'bg-cyan-500/10 border-cyan-400/25';
  if (isPrevGold) return 'bg-amber-500/10 border-amber-400/25';
  return '';
};

const fmtDate = (d: string) => {
  if (!d || d.length < 8) return d;
  return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
};

const fmtPct = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return '--';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}%`;
};

const pctColor = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return 'text-secondary-text';
  return v >= 0 ? 'text-red-400' : 'text-emerald-400';
};

const BollPickCard: React.FC<{
  item: HfqBollPickItem;
  active: boolean;
  distLabel: string;
  distValue: number;
  goldHighlight: GoldHighlight;
  onSelect: (tsCode: string) => void;
}> = ({ item, active, distLabel, distValue, goldHighlight, onSelect }) => {
  const { isCurrentGold, isPrevGold } = getGoldFlags(item.ts_code || item.stock_code, goldHighlight);
  const goldClass = goldCardClass(item.ts_code || item.stock_code, goldHighlight);

  return (
  <button
    type="button"
    onClick={() => onSelect(item.ts_code)}
    className={`w-full rounded-lg border px-2 py-1.5 text-left transition-colors ${
      active
        ? `border-primary/40 bg-primary/10 ring-1 ring-primary/30 ${isCurrentGold || isPrevGold ? goldClass : ''}`
        : goldClass || 'border-border/15 bg-muted/20 hover:border-border/30 hover:bg-muted/35'
    }`}
  >
    <div className="flex items-center gap-1 truncate text-xs font-medium text-foreground">
      <span className="truncate">{item.stock_name}</span>
      {isCurrentGold ? (
        <span className="shrink-0 rounded px-1 py-0.5 text-[9px] font-medium text-cyan-400 ring-1 ring-cyan-400/30">当月</span>
      ) : isPrevGold ? (
        <span className="shrink-0 rounded px-1 py-0.5 text-[9px] font-medium text-amber-400 ring-1 ring-amber-400/30">上月</span>
      ) : null}
    </div>
    <div className="font-mono text-[10px] text-tertiary-text">{item.stock_code}</div>
    <div className="mt-1 flex items-center justify-between gap-2 text-[10px]">
      <span className="text-tertiary-text">距新高</span>
      <span className={`font-mono tabular-nums ${pctColor(item.drawdown_from_high_pct)}`}>
        {fmtPct(item.drawdown_from_high_pct)}
      </span>
    </div>
    <div className="mt-0.5 flex items-center justify-between gap-2 text-[10px]">
      <span className="text-tertiary-text">{distLabel}</span>
      <span className={`font-mono tabular-nums ${pctColor(distValue)}`}>{fmtPct(distValue)}</span>
    </div>
    <div className="mt-0.5 text-[10px] text-tertiary-text">新高 {fmtDate(item.latest_new_high_date)}</div>
  </button>
  );
};

const BollPickColumn: React.FC<{
  title: string;
  count: number;
  loading: boolean;
  emptyText: string;
  items: HfqBollPickItem[];
  distKey: 'dist_mid_pct' | 'dist_lower_pct';
  distLabel: string;
  titleClass: string;
  activeTsCode: string;
  goldHighlight: GoldHighlight;
  onSelect: (tsCode: string) => void;
}> = ({
  title,
  count,
  loading,
  emptyText,
  items,
  distKey,
  distLabel,
  titleClass,
  activeTsCode,
  goldHighlight,
  onSelect,
}) => (
  <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-lg border border-border/15 bg-muted/10">
    <div className={`shrink-0 border-b border-border/15 px-2 py-1.5 text-[11px] font-medium ${titleClass}`}>
      {title}
      <span className="ml-1 font-normal text-tertiary-text">({count})</span>
    </div>
    <div className="min-h-0 flex-1 overflow-y-auto p-1.5">
      {loading ? (
        <div className="flex items-center justify-center gap-1.5 py-6 text-[10px] text-tertiary-text">
          <Loader2 className="h-3 w-3 animate-spin" />
          加载中…
        </div>
      ) : !items.length ? (
        <div className="py-6 text-center text-[10px] text-tertiary-text">{emptyText}</div>
      ) : (
        <div className="space-y-1">
          {items.map(item => (
            <BollPickCard
              key={item.ts_code}
              item={item}
              active={!!activeTsCode && matchTsCode(activeTsCode, item.ts_code)}
              distLabel={distLabel}
              distValue={item[distKey]}
              goldHighlight={goldHighlight}
              onSelect={onSelect}
            />
          ))}
        </div>
      )}
    </div>
  </div>
);

const BollPickPanel: React.FC<{
  loading: boolean;
  picks: HfqBollPickItem[];
  nearPct: number;
  lookbackDays: number;
  maxDrawdownFromHighPct: number;
  goldHighlight: GoldHighlight;
  onSelect: (tsCode: string) => void;
  activeTsCode: string;
  className?: string;
}> = ({ loading, picks, nearPct, lookbackDays, maxDrawdownFromHighPct, goldHighlight, onSelect, activeTsCode, className = '' }) => {
  const sortByNearHigh = (a: HfqBollPickItem, b: HfqBollPickItem) =>
    (b.drawdown_from_high_pct ?? -9999) - (a.drawdown_from_high_pct ?? -9999);

  const midPicks = useMemo(
    () => picks
      .filter(p => p.band_zone === 'mid' || p.band_zone === 'both')
      .sort(sortByNearHigh),
    [picks],
  );
  const lowerPicks = useMemo(
    () => picks
      .filter(p => p.band_zone === 'lower' || p.band_zone === 'both')
      .sort(sortByNearHigh),
    [picks],
  );

  return (
    <div className={`flex h-full max-h-full min-h-0 flex-col overflow-hidden rounded-xl border border-border/20 bg-card/40 ${className}`}>
      <div className="shrink-0 border-b border-border/20 px-3 py-2">
        <div className="text-sm font-medium text-foreground">BOLL 推荐</div>
        <div className="mt-0.5 text-[11px] text-tertiary-text">
          近 {lookbackDays} 日创新高 · 距轨道 ≤ {nearPct}% · 距新高 ≤ {maxDrawdownFromHighPct}% · 后复权 BOLL(20,2)
        </div>
      </div>
      <div className="grid h-full min-h-0 flex-1 grid-cols-2 grid-rows-1 gap-2 overflow-hidden p-2">
        <BollPickColumn
          title="中轨附近"
          count={midPicks.length}
          loading={loading}
          emptyText="暂无"
          items={midPicks}
          distKey="dist_mid_pct"
          distLabel="距中轨"
          titleClass="text-sky-400"
          activeTsCode={activeTsCode}
          goldHighlight={goldHighlight}
          onSelect={onSelect}
        />
        <BollPickColumn
          title="下轨附近"
          count={lowerPicks.length}
          loading={loading}
          emptyText="暂无"
          items={lowerPicks}
          distKey="dist_lower_pct"
          distLabel="距下轨"
          titleClass="text-pink-400"
          activeTsCode={activeTsCode}
          goldHighlight={goldHighlight}
          onSelect={onSelect}
        />
      </div>
    </div>
  );
};

type KlineState = {
  loading: boolean;
  error: string | null;
  bars: Array<{ date: string; price: number; open: number; high: number; low: number }>;
};

const ExpandPanel: React.FC<{ record: HfqNewHighItem; startDate: string }> = ({ record, startDate }) => {
  const [kline, setKline] = useState<KlineState>({ loading: true, error: null, bars: [] });

  useEffect(() => {
    let cancelled = false;
    setKline({ loading: true, error: null, bars: [] });
    marketApi
      .getHfqKlines(record.stock_code, startDate)
      .then(resp => {
        if (cancelled) return;
        const bars = resp.data
          .map(k => ({
            date: k.date,
            price: k.close,
            open: k.open ?? k.close,
            high: k.high ?? k.close,
            low: k.low ?? k.close,
          }))
          .filter(d => d.price != null && !Number.isNaN(d.price));
        setKline({ loading: false, error: null, bars });
      })
      .catch(() => {
        if (!cancelled) setKline({ loading: false, error: '加载 K 线失败', bars: [] });
      });
    return () => { cancelled = true; };
  }, [record.stock_code, startDate]);

  return (
    <div id={`hfq-expand-${record.ts_code}`} className="scroll-mt-16 space-y-2 p-2">
      <div>
        <div className="mb-1 text-xs font-medium text-secondary-text">
          2026 至今创新高记录（倒序）
        </div>
        <div className="rounded-lg border border-border/20 px-2 py-1">
          <div className="grid grid-cols-5 gap-x-1.5 gap-y-0">
            {record.new_high_dates.map((row, idx) => (
              <div
                key={row.date}
                className="flex items-center gap-0.5 whitespace-nowrap border-b border-border/10 py-0.5 text-[11px] last:border-0"
              >
                <span className="shrink-0 tabular-nums text-primary/85 font-medium">{idx + 1}</span>
                <span className="shrink-0 font-mono leading-none">{fmtDate(row.date)}</span>
                <span className="ml-2 shrink-0 font-mono tabular-nums leading-none text-secondary-text">
                  {row.hfq_close.toFixed(2)}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div>
        <div className="mb-1 flex items-center gap-2 text-xs">
          <span className="font-medium text-secondary-text">2026 至今后复权日 K</span>
          <span className="rounded px-1.5 py-0.5 text-[10px] font-medium text-pink-400/90 ring-1 ring-pink-400/30">BOLL</span>
        </div>
        {kline.loading ? (
          <div className="flex items-center gap-2 py-4 text-xs text-tertiary-text">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
            加载 K 线…
          </div>
        ) : kline.error ? (
          <div className="text-xs text-tertiary-text py-2">{kline.error}</div>
        ) : kline.bars.length >= 2 ? (
          <CandlestickMiniChart data={kline.bars} height={180} longSeriesScroll overlay="boll" />
        ) : (
          <div className="text-xs text-tertiary-text py-2">暂无足够 K 线数据</div>
        )}
      </div>
    </div>
  );
};

const measureTableListHeight = (root: HTMLElement): number => {
  const wrapper = root.querySelector('.ant-table-wrapper');
  const pagination = root.querySelector('.ant-pagination');
  if (!wrapper) return root.offsetHeight;

  const thead = wrapper.querySelector('.ant-table-thead') as HTMLElement | null;
  let tbodyHeight = 0;
  wrapper.querySelectorAll('.ant-table-tbody > tr.ant-table-row').forEach(row => {
    tbodyHeight += (row as HTMLElement).offsetHeight;
  });

  const theadHeight = thead?.offsetHeight ?? 0;
  const paginationHeight = pagination
    ? (pagination as HTMLElement).offsetHeight + 12
    : 0;
  return theadHeight + tbodyHeight + paginationHeight;
};

const sortNewHighItemsDefault = (items: HfqNewHighItem[]) =>
  [...items].sort((a, b) => {
    const byDate = b.latest_new_high_date.localeCompare(a.latest_new_high_date);
    if (byDate !== 0) return byDate;
    return b.new_high_count - a.new_high_count;
  });

const matchTsCode = (left: string, right: string) => {
  if (left === right) return true;
  return normStockCode(left) === normStockCode(right);
};

const findNewHighItem = (items: HfqNewHighItem[], tsCode: string) =>
  items.find(item => matchTsCode(item.ts_code || item.stock_code, tsCode));

const findTableRowByTsCode = (root: HTMLElement | null, tsCode: string): HTMLElement | null => {
  if (!root) return null;
  for (const row of root.querySelectorAll('tr.ant-table-row')) {
    const key = row.getAttribute('data-row-key');
    if (key && matchTsCode(key, tsCode)) return row as HTMLElement;
  }
  return null;
};

const findExpandedRowForTsCode = (root: HTMLElement | null, tsCode: string): HTMLElement | null => {
  const row = findTableRowByTsCode(root, tsCode);
  if (!row) return null;
  const next = row.nextElementSibling;
  if (next instanceof HTMLElement && next.classList.contains('ant-table-expanded-row')) {
    return next;
  }
  return null;
};

const findStockDetailElement = (root: HTMLElement | null, tsCode: string): HTMLElement | null => {
  const byId = document.getElementById(`hfq-expand-${tsCode}`);
  if (byId) return byId;
  return findExpandedRowForTsCode(root, tsCode);
};

const scrollToStockDetail = (
  root: HTMLElement | null,
  tsCode: string,
  attempt: number,
  maxAttempts: number,
): boolean => {
  const detail = findStockDetailElement(root, tsCode);
  if (detail) {
    detail.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return true;
  }
  const row = findTableRowByTsCode(root, tsCode);
  if (row && attempt >= maxAttempts - 1) {
    row.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return true;
  }
  return false;
};

const LOCATE_SCROLL_MAX_ATTEMPTS = 24;

const HfqNewHighPage: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<Awaited<ReturnType<typeof marketApi.getHfqNewHighs>> | null>(null);
  const [expandedKey, setExpandedKey] = useState<string>('');
  const [goldHighlight, setGoldHighlight] = useState<GoldHighlight>(EMPTY_GOLD);
  const [bollPicks, setBollPicks] = useState<HfqBollPickItem[]>([]);
  const [bollMeta, setBollMeta] = useState({ nearPct: 2, lookbackDays: 30, maxDrawdownFromHighPct: 20 });
  const [bollLoading, setBollLoading] = useState(false);
  const [tablePage, setTablePage] = useState(1);
  const [tablePageSize, setTablePageSize] = useState(50);
  const [focusTsCode, setFocusTsCode] = useState('');
  const tableWrapRef = useRef<HTMLDivElement>(null);
  const pendingLocateRef = useRef('');
  const [panelHeight, setPanelHeight] = useState<number | undefined>(undefined);

  const startDate = data?.start_date ?? '20260101';

  useLayoutEffect(() => {
    const el = tableWrapRef.current;
    if (!el) return;
    const apply = () => {
      const h = measureTableListHeight(el);
      if (h > 0) setPanelHeight(h);
    };
    apply();
    const ro = new ResizeObserver(() => apply());
    ro.observe(el);
    return () => ro.disconnect();
  }, [data, tablePage, tablePageSize, loading, bollLoading]);

  const fetchGoldHighlight = useCallback(async () => {
    const currentMonth = dayjs().format('YYYYMM');
    const prevMonth = dayjs().subtract(1, 'month').format('YYYYMM');
    try {
      const [cur, prev] = await Promise.all([
        getMonthlyRecommendations(currentMonth),
        getMonthlyRecommendations(prevMonth),
      ]);
      const current = new Set((cur.items ?? []).map(i => normStockCode(i.ts_code)));
      const prevOnly = new Set<string>();
      for (const item of prev.items ?? []) {
        const bare = normStockCode(item.ts_code);
        if (!current.has(bare)) prevOnly.add(bare);
      }
      setGoldHighlight({ current, prevOnly });
    } catch {
      setGoldHighlight(EMPTY_GOLD);
    }
  }, []);

  const fetchData = useCallback(async (refresh = false) => {
    if (refresh) setRefreshing(true);
    else setLoading(true);
    setError(null);
    setBollLoading(true);
    try {
      const resp = await marketApi.getHfqNewHighs({ refresh });
      setData(resp);
      setLoading(false);
      setRefreshing(false);

      const bollResp = await marketApi.getHfqBollPicks({ refresh });
      setBollPicks(bollResp.items ?? []);
      setBollMeta({
        nearPct: bollResp.near_pct,
        lookbackDays: bollResp.lookback_days,
        maxDrawdownFromHighPct: bollResp.max_drawdown_from_high_pct,
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : '加载失败';
      setError(msg);
      setBollPicks([]);
    } finally {
      setLoading(false);
      setRefreshing(false);
      setBollLoading(false);
    }
  }, []);

  const locateStock = useCallback((tsCode: string) => {
    if (!data?.items?.length) return;
    const match = findNewHighItem(data.items, tsCode);
    if (!match) return;

    const canonicalKey = match.ts_code;
    const sorted = sortNewHighItemsDefault(data.items);
    const idx = sorted.findIndex(item => item.ts_code === canonicalKey);
    if (idx < 0) return;

    const page = Math.floor(idx / tablePageSize) + 1;
    pendingLocateRef.current = canonicalKey;
    setTablePage(page);
    setExpandedKey(canonicalKey);
    setFocusTsCode(canonicalKey);
    window.setTimeout(() => setFocusTsCode(''), 2400);
  }, [data?.items, tablePageSize]);

  useLayoutEffect(() => {
    const tsCode = pendingLocateRef.current;
    if (!tsCode || expandedKey !== tsCode) return;

    let attempts = 0;
    let cancelled = false;

    const tryScroll = () => {
      if (cancelled) return;
      attempts += 1;
      const scrolled = scrollToStockDetail(tableWrapRef.current, tsCode, attempts, LOCATE_SCROLL_MAX_ATTEMPTS);
      if (scrolled) {
        pendingLocateRef.current = '';
        return;
      }
      if (attempts < LOCATE_SCROLL_MAX_ATTEMPTS) {
        window.requestAnimationFrame(tryScroll);
      } else {
        pendingLocateRef.current = '';
      }
    };

    tryScroll();
    return () => {
      cancelled = true;
    };
  }, [tablePage, expandedKey, data?.items, tablePageSize, loading]);

  useEffect(() => {
    void fetchData(false);
  }, [fetchData]);

  useEffect(() => {
    void fetchGoldHighlight();
  }, [fetchGoldHighlight]);

  const columns: ColumnsType<HfqNewHighItem> = useMemo(() => [
    {
      title: '名称 / 代码',
      key: 'name',
      render: (_, r) => {
        const { isCurrentGold, isPrevGold } = getGoldFlags(r.ts_code || r.stock_code, goldHighlight);
        return (
          <div>
            <div className="flex items-center gap-1.5">
              <span className="font-medium text-foreground">{r.stock_name}</span>
              {isCurrentGold ? (
                <span className="rounded px-1 py-0.5 text-[10px] font-medium text-cyan-400 ring-1 ring-cyan-400/30">当月金股</span>
              ) : isPrevGold ? (
                <span className="rounded px-1 py-0.5 text-[10px] font-medium text-amber-400 ring-1 ring-amber-400/30">上月金股</span>
              ) : null}
            </div>
            <div className="text-[11px] font-mono text-tertiary-text">{r.stock_code}</div>
          </div>
        );
      },
    },
    {
      title: '最近新高日',
      dataIndex: 'latest_new_high_date',
      key: 'latest_new_high_date',
      defaultSortOrder: 'descend',
      sorter: (a, b) => {
        const byDate = b.latest_new_high_date.localeCompare(a.latest_new_high_date);
        if (byDate !== 0) return byDate;
        return b.new_high_count - a.new_high_count;
      },
      render: (v: string) => <span className="font-mono text-xs">{fmtDate(v)}</span>,
    },
    {
      title: '新高价',
      dataIndex: 'latest_new_high_close',
      key: 'latest_new_high_close',
      align: 'right',
      render: (v: number) => <span className="font-mono tabular-nums">{v?.toFixed(2)}</span>,
    },
    {
      title: '次数',
      dataIndex: 'new_high_count',
      key: 'new_high_count',
      align: 'right',
      width: 72,
    },
    {
      title: '现价',
      dataIndex: 'current_hfq_close',
      key: 'current_hfq_close',
      align: 'right',
      render: (v?: number | null) => (
        <span className="font-mono tabular-nums">{v != null ? v.toFixed(2) : '--'}</span>
      ),
    },
    {
      title: '2026 涨幅',
      dataIndex: 'ytd_hfq_return_pct',
      key: 'ytd_hfq_return_pct',
      align: 'right',
      sorter: (a, b) => (a.ytd_hfq_return_pct ?? -Infinity) - (b.ytd_hfq_return_pct ?? -Infinity),
      render: (v?: number | null) => (
        <span className={`font-mono tabular-nums ${pctColor(v)}`}>{fmtPct(v)}</span>
      ),
    },
    {
      title: '距新高',
      dataIndex: 'drawdown_from_high_pct',
      key: 'drawdown_from_high_pct',
      align: 'right',
      render: (v?: number | null) => (
        <span className={`font-mono tabular-nums ${pctColor(v)}`}>{fmtPct(v)}</span>
      ),
    },
  ], [goldHighlight]);

  const goldLegend = goldHighlight.current.size > 0 || goldHighlight.prevOnly.size > 0;

  const rowClassName = useCallback((record: HfqNewHighItem) => {
    const classes = [goldRowClass(record, goldHighlight)];
    if (focusTsCode && matchTsCode(record.ts_code, focusTsCode)) {
      classes.push('ring-2 ring-inset ring-primary/40');
    }
    return classes.filter(Boolean).join(' ');
  }, [goldHighlight, focusTsCode]);

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="space-y-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <ArrowUpRight className="h-5 w-5 text-tertiary-text" />
              <h1 className="text-lg font-semibold">后复权新高</h1>
            </div>
            <p className="mt-1 text-xs text-tertiary-text">2026 年至今全 A 股后复权收盘价创新高，按最近创新高日排序</p>
          </div>
          <Button
            variant="secondary"
            size="sm"
            disabled={loading || refreshing}
            onClick={() => {
              void fetchData(true);
              void fetchGoldHighlight();
            }}
          >
            <RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${refreshing ? 'animate-spin' : ''}`} />
            刷新
          </Button>
        </div>

      {loading && !data ? (
        <div className="flex items-center justify-center gap-2 py-16 text-sm text-tertiary-text">
          <Loader2 className="h-4 w-4 animate-spin" />
          扫描全市场新高数据…
        </div>
      ) : error ? (
        <EmptyState title="加载失败" description={error} />
      ) : !data?.items?.length ? (
        <EmptyState title="暂无新高记录" description={`截止 ${fmtDate(data?.as_of_date ?? '')} 无符合条件个股`} />
      ) : (
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-tertiary-text">
            <span>截止 {fmtDate(data.as_of_date)} · 共 {data.total} 只 · 后复权收盘口径</span>
            <span>推荐 {bollPicks.length} 只</span>
            {goldLegend ? (
              <>
                <span className="inline-flex items-center gap-1">
                  <span className="h-2 w-3 rounded-sm bg-cyan-500/25 ring-1 ring-cyan-400/30" />
                  当月金股
                </span>
                <span className="inline-flex items-center gap-1">
                  <span className="h-2 w-3 rounded-sm bg-amber-500/25 ring-1 ring-amber-400/30" />
                  上月金股
                </span>
              </>
            ) : null}
          </div>
          <div className="flex items-start gap-3">
            <div ref={tableWrapRef} className="min-w-0 flex-1">
              <Table<HfqNewHighItem>
                rowKey="ts_code"
                size="small"
                columns={columns}
                dataSource={data.items}
                rowClassName={rowClassName}
                pagination={{
                  current: tablePage,
                  pageSize: tablePageSize,
                  showSizeChanger: true,
                  pageSizeOptions: ['20', '50', '100'],
                  onChange: (page, size) => {
                    setTablePage(page);
                    if (size !== tablePageSize) setTablePageSize(size);
                  },
                }}
                expandable={{
                  expandedRowKeys: expandedKey ? [expandedKey] : [],
                  onExpand: (expanded, record) => setExpandedKey(expanded ? record.ts_code : ''),
                  expandedRowRender: record => <ExpandPanel record={record} startDate={startDate} />,
                }}
              />
            </div>
            <div
              className="sticky top-2 hidden min-h-0 w-[520px] shrink-0 overflow-hidden lg:block xl:w-[560px]"
              style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : undefined}
            >
              <BollPickPanel
                loading={bollLoading}
                picks={bollPicks}
                nearPct={bollMeta.nearPct}
                lookbackDays={bollMeta.lookbackDays}
                maxDrawdownFromHighPct={bollMeta.maxDrawdownFromHighPct}
                goldHighlight={goldHighlight}
                onSelect={locateStock}
                activeTsCode={focusTsCode || expandedKey}
                className="h-full max-h-full"
              />
            </div>
          </div>
          <div
            className="min-h-0 overflow-hidden lg:hidden"
            style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : { height: 360, maxHeight: 360 }}
          >
            <BollPickPanel
              className="h-full max-h-full"
              loading={bollLoading}
              picks={bollPicks}
              nearPct={bollMeta.nearPct}
              lookbackDays={bollMeta.lookbackDays}
              maxDrawdownFromHighPct={bollMeta.maxDrawdownFromHighPct}
              goldHighlight={goldHighlight}
              onSelect={locateStock}
              activeTsCode={focusTsCode || expandedKey}
            />
          </div>
        </div>
      )}
      </div>
    </AppPage>
  );
};

export default HfqNewHighPage;
