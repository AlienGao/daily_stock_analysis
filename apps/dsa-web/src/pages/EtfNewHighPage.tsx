import type React from 'react';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Table, Tabs } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ArrowUpRight, Loader2, PieChart, RefreshCw, X } from 'lucide-react';
import { AppPage, Button, EmptyState } from '../components/common';
import { CandlestickMiniChart } from '../components/charts/CandlestickMiniChart';
import { marketApi, type EtfBollPickItem, type EtfNewHighItem, type GlobalIndexNewHighItem } from '../api/market';

const normStockCode = (code: string) => code.split('.')[0].replace(/\D/g, '').slice(-6).padStart(6, '0');

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

// ── Shared types ──

type BollPickItemLike = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close?: number | null;
  drawdown_from_high_pct?: number | null;
  dist_mid_pct: number;
  dist_lower_pct: number;
  dist_upper_pct?: number;
  band_zone: string;
};

type NewHighItemLike = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close: number;
  new_high_count: number;
  current_close?: number | null;
  drawdown_from_high_pct?: number | null;
  ytd_return_pct?: number | null;
  new_high_dates: Array<{ date: string; close: number }>;
};

// ── BOLL Card (generic over boll pick type) ──

const BollPickCard: React.FC<{
  item: BollPickItemLike;
  active: boolean;
  distLabel: string;
  distValue: number | undefined;
  onSelect: (tsCode: string) => void;
}> = ({ item, active, distLabel, distValue, onSelect }) => {
  const closeVal = 'current_close' in item ? (item as any).current_close || (item as any).current_hfq_close : undefined;
  return (
  <button
    type="button"
    onClick={() => onSelect(item.ts_code)}
    className={`w-full rounded-lg border px-2 py-1.5 text-left transition-colors ${
      active
        ? 'border-primary/40 bg-primary/10 ring-1 ring-primary/30'
        : 'border-border/15 bg-muted/20 hover:border-border/30 hover:bg-muted/35'
    }`}
  >
    <div className="truncate text-xs font-medium text-foreground">
      <span className="truncate">{item.stock_name}</span>
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
  items: BollPickItemLike[];
  distKey: 'dist_mid_pct' | 'dist_lower_pct' | 'dist_upper_pct';
  distLabel: string;
  titleClass: string;
  activeTsCode: string;
  onSelect: (tsCode: string) => void;
}> = ({ title, count, loading, emptyText, items, distKey, distLabel, titleClass, activeTsCode, onSelect }) => (
  <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-lg border border-border/15 bg-muted/10">
    <div className={`shrink-0 border-b border-border/15 px-2 py-1.5 text-[11px] font-medium ${titleClass}`}>
      {title}
      <span className="ml-1 font-normal text-tertiary-text">({count})</span>
    </div>
    <div className="min-h-0 flex-1 overflow-y-auto p-1.5">
      {loading ? (
        <div className="flex items-center justify-center gap-1.5 py-6 text-[10px] text-tertiary-text">
          <Loader2 className="h-3 w-3 animate-spin" />加载中…
        </div>
      ) : !items.length ? (
        <div className="py-6 text-center text-[10px] text-tertiary-text">{emptyText}</div>
      ) : (
        <div className="space-y-1">
          {items.map(item => (
            <BollPickCard key={item.ts_code} item={item}
              active={!!activeTsCode && matchTsCode(activeTsCode, item.ts_code)}
              distLabel={distLabel} distValue={(item as any)[distKey]} onSelect={onSelect} />
          ))}
        </div>
      )}
    </div>
  </div>
);

const BollPickPanel: React.FC<{
  loading: boolean;
  picks: BollPickItemLike[];
  nearPct: number;
  lookbackDays: number;
  maxDrawdownFromHighPct: number;
  onSelect: (tsCode: string) => void;
  activeTsCode: string;
  className?: string;
}> = ({ loading, picks, nearPct, lookbackDays, maxDrawdownFromHighPct, onSelect, activeTsCode, className = '' }) => {
  const sortByNearHigh = (a: BollPickItemLike, b: BollPickItemLike) =>
    (b.drawdown_from_high_pct ?? -9999) - (a.drawdown_from_high_pct ?? -9999);

  const midPicks = useMemo(() => picks.filter(p => p.band_zone.includes('mid')).sort(sortByNearHigh), [picks]);
  const lowerPicks = useMemo(() => picks.filter(p => p.band_zone.includes('lower')).sort(sortByNearHigh), [picks]);
  const upperPicks = useMemo(() => picks.filter(p => p.band_zone.includes('upper')).sort(sortByNearHigh), [picks]);

  return (
    <div className={`flex h-full max-h-full min-h-0 flex-col overflow-hidden rounded-xl border border-border/20 bg-card/40 ${className}`}>
      <div className="shrink-0 border-b border-border/20 px-3 py-2">
        <div className="text-sm font-medium text-foreground">BOLL 推荐</div>
        <div className="mt-0.5 text-[11px] text-tertiary-text">
          近 {lookbackDays} 日创新高 · 距轨道 ≤ {nearPct}% · 距新高 ≤ {maxDrawdownFromHighPct}% · BOLL(20,2)
        </div>
      </div>
      <div className="grid h-full min-h-0 flex-1 grid-cols-3 grid-rows-1 gap-2 overflow-hidden p-2">
        <BollPickColumn title="上轨附近" count={upperPicks.length} loading={loading} emptyText="暂无"
          items={upperPicks} distKey="dist_upper_pct" distLabel="距上轨" titleClass="text-amber-400"
          activeTsCode={activeTsCode} onSelect={onSelect} />
        <BollPickColumn title="中轨附近" count={midPicks.length} loading={loading} emptyText="暂无"
          items={midPicks} distKey="dist_mid_pct" distLabel="距中轨" titleClass="text-sky-400"
          activeTsCode={activeTsCode} onSelect={onSelect} />
        <BollPickColumn title="下轨附近" count={lowerPicks.length} loading={loading} emptyText="暂无"
          items={lowerPicks} distKey="dist_lower_pct" distLabel="距下轨" titleClass="text-pink-400"
          activeTsCode={activeTsCode} onSelect={onSelect} />
      </div>
    </div>
  );
};

// ── Shared expand panel ──

type KlineState = {
  loading: boolean;
  error: string | null;
  bars: Array<{ date: string; price: number; open: number; high: number; low: number }>;
};

const ExpandPanel: React.FC<{
  record: NewHighItemLike;
  startDate: string;
  loadKlines: (code: string, startDate: string) => Promise<{ data: Array<{ date: string; close: number; open?: number | null; high?: number | null; low?: number | null }> }>;
}> = ({ record, startDate, loadKlines }) => {
  const [kline, setKline] = useState<KlineState>({ loading: true, error: null, bars: [] });

  useEffect(() => {
    let cancelled = false;
    setKline({ loading: true, error: null, bars: [] });
    loadKlines(record.stock_code, startDate)
      .then(resp => {
        if (cancelled) return;
        const bars = resp.data
          .map(k => ({ date: k.date, price: k.close, open: k.open ?? k.close, high: k.high ?? k.close, low: k.low ?? k.close }))
          .filter(d => d.price != null && !Number.isNaN(d.price));
        setKline({ loading: false, error: null, bars });
      })
      .catch(() => { if (!cancelled) setKline({ loading: false, error: '加载 K 线失败', bars: [] }); });
    return () => { cancelled = true; };
  }, [record.stock_code, startDate, loadKlines]);

  return (
    <div id={`nh-expand-${record.ts_code}`} className="scroll-mt-16 space-y-2 p-2">
      <div>
        <div className="mb-1 text-xs font-medium text-secondary-text">2026 至今创新高记录（倒序）</div>
        <div className="rounded-lg border border-border/20 px-2 py-1">
          <div className="grid grid-cols-5 gap-x-1.5 gap-y-0">
            {record.new_high_dates.map((row, idx) => (
              <div key={row.date}
                className="flex items-center gap-0.5 whitespace-nowrap border-b border-border/10 py-0.5 text-[11px] last:border-0"
              >
                <span className="shrink-0 tabular-nums text-primary/85 font-medium">{idx + 1}</span>
                <span className="shrink-0 font-mono leading-none">{fmtDate(row.date)}</span>
                <span className="ml-2 shrink-0 font-mono tabular-nums leading-none text-secondary-text">{row.close.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div>
        <div className="mb-1 flex items-center gap-2 text-xs">
          <span className="font-medium text-secondary-text">2026 至今日 K</span>
          <span className="rounded px-1.5 py-0.5 text-[10px] font-medium text-pink-400/90 ring-1 ring-pink-400/30">BOLL</span>
        </div>
        {kline.loading ? (
          <div className="flex items-center gap-2 py-4 text-xs text-tertiary-text">
            <Loader2 className="h-3.5 w-3.5 animate-spin" />加载 K 线…
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

// ── Helpers ──

const measureTableListHeight = (root: HTMLElement): number => {
  const wrapper = root.querySelector('.ant-table-wrapper');
  if (!wrapper) return root.offsetHeight;
  return Math.ceil((wrapper as HTMLElement).getBoundingClientRect().height);
};

const sortItemsDefault = (items: NewHighItemLike[]) =>
  [...items].sort((a, b) => {
    const byDate = b.latest_new_high_date.localeCompare(a.latest_new_high_date);
    if (byDate !== 0) return byDate;
    return b.new_high_count - a.new_high_count;
  });

const matchTsCode = (left: string, right: string) => {
  if (left === right) return true;
  // 纯数字股票代码才做规范化匹配
  const leftBare = left.split('.')[0];
  const rightBare = right.split('.')[0];
  if (/^\d+$/.test(leftBare) && /^\d+$/.test(rightBare)) {
    return leftBare.replace(/\D/g, '').slice(-6).padStart(6, '0') === rightBare.replace(/\D/g, '').slice(-6).padStart(6, '0');
  }
  return false;
};

const findItem = (items: NewHighItemLike[], tsCode: string) =>
  items.find(item => matchTsCode(item.ts_code || item.stock_code, tsCode));

const scrollToDetail = (root: HTMLElement | null, tsCode: string, attempt: number, maxAttempts: number): boolean => {
  const byId = document.getElementById(`nh-expand-${tsCode}`);
  if (byId) { byId.scrollIntoView({ behavior: 'smooth', block: 'start' }); return true; }
  const rows = root?.querySelectorAll('tr.ant-table-row') ?? [];
  for (const row of rows) {
    const key = row.getAttribute('data-row-key');
    if (key && matchTsCode(key, tsCode)) {
      if (attempt >= maxAttempts - 1) { (row as HTMLElement).scrollIntoView({ behavior: 'smooth', block: 'start' }); return true; }
      const next = row.nextElementSibling;
      if (next instanceof HTMLElement && next.classList.contains('ant-table-expanded-row')) {
        next.scrollIntoView({ behavior: 'smooth', block: 'start' }); return true;
      }
    }
  }
  return false;
};

const LOCATE_SCROLL_MAX_ATTEMPTS = 24;

// ── Table columns factory ──

const makeColumns = <T extends NewHighItemLike>({
  currentCloseKey,
  ytdReturnKey,
  onNameClick,
}: { currentCloseKey: keyof T; ytdReturnKey: keyof T; onNameClick?: (tsCode: string) => void }): ColumnsType<T> => [
  {
    title: '名称 / 代码',
    key: 'name',
    render: (_, r) => (
      <div>
        {onNameClick ? (
          <button type="button" onClick={() => onNameClick(r.ts_code)}
            className="font-medium text-foreground hover:text-primary transition-colors text-left underline decoration-dotted underline-offset-2">
            {r.stock_name}
          </button>
        ) : (
          <span className="font-medium text-foreground">{r.stock_name}</span>
        )}
        <div className="text-[11px] font-mono text-tertiary-text">{r.stock_code}</div>
      </div>
    ),
  },
  {
    title: '最近新高日', dataIndex: 'latest_new_high_date', key: 'latest_new_high_date',
    defaultSortOrder: 'descend',
    sorter: (a, b) => {
      const byDate = b.latest_new_high_date.localeCompare(a.latest_new_high_date);
      if (byDate !== 0) return byDate;
      return b.new_high_count - a.new_high_count;
    },
    render: (v: string) => <span className="font-mono text-xs">{fmtDate(v)}</span>,
  },
  {
    title: '新高价', dataIndex: 'latest_new_high_close', key: 'latest_new_high_close',
    align: 'right',
    render: (v: number) => <span className="font-mono tabular-nums">{v?.toFixed(2)}</span>,
  },
  { title: '次数', dataIndex: 'new_high_count', key: 'new_high_count', align: 'right', width: 72 },
  {
    title: '现价', key: 'current_close', align: 'right',
    render: (_, r) => {
      const v = r[currentCloseKey] as number | undefined | null;
      return <span className="font-mono tabular-nums">{v != null ? v.toFixed(2) : '--'}</span>;
    },
  },
  {
    title: '2026 涨幅', key: 'ytd_return', align: 'right',
    sorter: (a, b) => ((a[ytdReturnKey] as number | undefined | null) ?? -Infinity) - ((b[ytdReturnKey] as number | undefined | null) ?? -Infinity),
    render: (_, r) => {
      const v = r[ytdReturnKey] as number | undefined | null;
      return <span className={`font-mono tabular-nums ${pctColor(v)}`}>{fmtPct(v)}</span>;
    },
  },
  {
    title: '距新高', dataIndex: 'drawdown_from_high_pct', key: 'drawdown_from_high_pct', align: 'right',
    sorter: (a, b) => (a.drawdown_from_high_pct ?? -Infinity) - (b.drawdown_from_high_pct ?? -Infinity),
    render: (v?: number | null) => (
      <span className={`font-mono tabular-nums ${pctColor(v)}`}>{fmtPct(v)}</span>
    ),
  },
];

// ── NewHighTablePanel (shared table + boll layout) ──

const NewHighTablePanel: React.FC<{
  loading: boolean;
  refreshing: boolean;
  data: { start_date: string; as_of_date: string; total: number; items: NewHighItemLike[] } | null;
  error: string | null;
  bollPicks: BollPickItemLike[];
  bollLoading: boolean;
  bollMeta: { nearPct: number; lookbackDays: number; maxDrawdownFromHighPct: number };
  onRefresh: (refresh: boolean) => void;
  currentCloseKey: string;
  ytdReturnKey: string;
  loadKlines: (code: string, startDate: string) => Promise<{ data: Array<{ date: string; close: number; open?: number | null; high?: number | null; low?: number | null }> }>;
  onNameClick?: (tsCode: string) => void;
  rightOverlay?: React.ReactNode;
}> = ({ loading, refreshing, data, error, bollPicks, bollLoading, bollMeta, onRefresh, currentCloseKey, ytdReturnKey, loadKlines, onNameClick, rightOverlay }) => {
  const [expandedKey, setExpandedKey] = useState('');
  const [tablePage, setTablePage] = useState(1);
  const [tablePageSize, setTablePageSize] = useState(50);
  const [focusTsCode, setFocusTsCode] = useState('');
  const tableWrapRef = useRef<HTMLDivElement>(null);
  const pendingLocateRef = useRef('');
  const [panelHeight, setPanelHeight] = useState<number | undefined>(undefined);

  const startDate = data?.start_date ?? '20260101';

  const locateStock = useCallback((tsCode: string) => {
    if (!data?.items?.length) return;
    const match = findItem(data.items, tsCode);
    if (!match) return;
    const canonicalKey = match.ts_code;
    const sorted = sortItemsDefault(data.items);
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
      if (scrollToDetail(tableWrapRef.current, tsCode, attempts, LOCATE_SCROLL_MAX_ATTEMPTS)) { pendingLocateRef.current = ''; return; }
      if (attempts < LOCATE_SCROLL_MAX_ATTEMPTS) { window.requestAnimationFrame(tryScroll); } else { pendingLocateRef.current = ''; }
    };
    tryScroll();
    return () => { cancelled = true; };
  }, [tablePage, expandedKey, data?.items, tablePageSize, loading]);

  useLayoutEffect(() => {
    const el = tableWrapRef.current;
    if (!el) return;
    const apply = () => { const h = measureTableListHeight(el); if (h > 0) setPanelHeight(h); };
    apply();
    const ro = new ResizeObserver(() => apply());
    ro.observe(el);
    const wrapper = el.querySelector('.ant-table-wrapper');
    if (wrapper) ro.observe(wrapper);
    return () => ro.disconnect();
  }, [data, tablePage, tablePageSize, loading, bollLoading]);

  const columns = useMemo(
    () => makeColumns({ currentCloseKey: currentCloseKey as any, ytdReturnKey: ytdReturnKey as any, onNameClick }),
    [currentCloseKey, ytdReturnKey, onNameClick],
  );

  const rowClassName = useCallback((record: NewHighItemLike) => {
    if (focusTsCode && matchTsCode(record.ts_code, focusTsCode)) return 'ring-2 ring-inset ring-primary/40';
    return '';
  }, [focusTsCode]);

  if (loading && !data) {
    return <div className="flex items-center justify-center gap-2 py-16 text-sm text-tertiary-text">
      <Loader2 className="h-4 w-4 animate-spin" />扫描新高数据…
    </div>;
  }

  if (error) return <EmptyState title="加载失败" description={error} />;
  if (!data?.items?.length) return <EmptyState title="暂无新高记录" description={`截止 ${fmtDate(data?.as_of_date ?? '')} 无符合条件`} />;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-tertiary-text">
        <span>截止 {fmtDate(data.as_of_date)} · 共 {data.total} 只</span>
        {bollPicks.length > 0 && <span>推荐 {bollPicks.length} 只</span>}
      </div>
      <div className="flex items-start gap-3">
        <div ref={tableWrapRef} className="min-w-0 flex-1">
          <Table<any>
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
              onChange: (page, size) => { setTablePage(page); if (size !== tablePageSize) setTablePageSize(size); },
            }}
            expandable={{
              expandedRowKeys: expandedKey ? [expandedKey] : [],
              onExpand: (expanded, record) => setExpandedKey(expanded ? record.ts_code : ''),
              expandedRowRender: record => <ExpandPanel record={record} startDate={startDate} loadKlines={loadKlines} />,
            }}
          />
        </div>
        <div
          className="sticky top-2 hidden min-h-0 w-[520px] shrink-0 overflow-hidden lg:block xl:w-[560px]"
          style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : undefined}
        >
          <div className="relative h-full w-full">
            <BollPickPanel
              loading={bollLoading} picks={bollPicks}
              nearPct={bollMeta.nearPct} lookbackDays={bollMeta.lookbackDays} maxDrawdownFromHighPct={bollMeta.maxDrawdownFromHighPct}
              onSelect={locateStock} activeTsCode={focusTsCode || expandedKey} className="h-full max-h-full" />
            {rightOverlay && (
              <div className="absolute inset-0 z-10">
                {rightOverlay}
              </div>
            )}
          </div>
        </div>
      </div>
      <div className="min-h-0 overflow-hidden lg:hidden"
        style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : { height: 360, maxHeight: 360 }}>
        <BollPickPanel
          className="h-full max-h-full" loading={bollLoading} picks={bollPicks}
          nearPct={bollMeta.nearPct} lookbackDays={bollMeta.lookbackDays} maxDrawdownFromHighPct={bollMeta.maxDrawdownFromHighPct}
          onSelect={locateStock} activeTsCode={focusTsCode || expandedKey} />
      </div>
    </div>
  );
};

// ════════════════════════════════════════════════════════════════════
// Main Page
// ════════════════════════════════════════════════════════════════════

const EtfNewHighPage: React.FC = () => {
  const [etfLoading, setEtfLoading] = useState(true);
  const [etfRefreshing, setEtfRefreshing] = useState(false);
  const [etfError, setEtfError] = useState<string | null>(null);
  const [etfData, setEtfData] = useState<Awaited<ReturnType<typeof marketApi.getEtfNewHighs>> | null>(null);
  const [etfBollPicks, setEtfBollPicks] = useState<BollPickItemLike[]>([]);
  const [etfBollMeta, setEtfBollMeta] = useState({ nearPct: 2, lookbackDays: 30, maxDrawdownFromHighPct: 30 });
  const [etfBollLoading, setEtfBollLoading] = useState(false);

  const [globalLoading, setGlobalLoading] = useState(false);
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [globalData, setGlobalData] = useState<Awaited<ReturnType<typeof marketApi.getGlobalIndexNewHighs>> | null>(null);
  const [globalBollPicks, setGlobalBollPicks] = useState<BollPickItemLike[]>([]);
  const [globalBollLoading, setGlobalBollLoading] = useState(false);

  const [aIndexLoading, setAIndexLoading] = useState(false);
  const [aIndexError, setAIndexError] = useState<string | null>(null);
  const [aIndexData, setAIndexData] = useState<Awaited<ReturnType<typeof marketApi.getAIndexNewHighs>> | null>(null);
  const [aIndexBollPicks, setAIndexBollPicks] = useState<BollPickItemLike[]>([]);
  const [aIndexBollLoading, setAIndexBollLoading] = useState(false);
  const [aIndexFreq, setAIndexFreq] = useState('daily');
  const [constituentIndex, setConstituentIndex] = useState<string | null>(null);
  const [constituentData, setConstituentData] = useState<Array<{ con_code: string; con_name: string | null; weight: number | null }>>([]);
  const [constituentLoading, setConstituentLoading] = useState(false);

  const [activeTab, setActiveTab] = useState('etf');

  const fetchEtf = useCallback(async (refresh = false) => {
    if (refresh) setEtfRefreshing(true);
    else setEtfLoading(true);
    setEtfError(null);
    try {
      const [resp, bollResp] = await Promise.all([
        marketApi.getEtfNewHighs({ refresh }),
        marketApi.getEtfBollPicks({ refresh }),
      ]);
      setEtfData(resp);
      setEtfLoading(false);
      setEtfRefreshing(false);
      setEtfBollPicks(bollResp.items ?? []);
      setEtfBollMeta({ nearPct: bollResp.near_pct, lookbackDays: bollResp.lookback_days, maxDrawdownFromHighPct: bollResp.max_drawdown_from_high_pct });
    } catch (err: unknown) {
      setEtfError(err instanceof Error ? err.message : '加载失败');
      setEtfBollPicks([]);
    } finally {
      setEtfLoading(false);
      setEtfRefreshing(false);
      setEtfBollLoading(false);
    }
  }, []);

  const fetchGlobal = useCallback(async (refresh = false) => {
    setGlobalLoading(true);
    setGlobalError(null);
    setGlobalBollLoading(true);
    try {
      const [resp, bollResp] = await Promise.all([
        marketApi.getGlobalIndexNewHighs({ refresh }),
        marketApi.getGlobalIndexBollPicks({ refresh }),
      ]);
      setGlobalData(resp);
      setGlobalBollPicks(bollResp.items ?? []);
    } catch (err: unknown) {
      setGlobalError(err instanceof Error ? err.message : '加载失败');
      setGlobalBollPicks([]);
    } finally {
      setGlobalLoading(false);
      setGlobalBollLoading(false);
    }
  }, []);

  const fetchAIndex = useCallback(async (refresh = false, freq = 'daily') => {
    setAIndexLoading(true);
    setAIndexError(null);
    setAIndexBollLoading(true);
    try {
      const [resp, bollResp] = await Promise.all([
        marketApi.getAIndexNewHighs({ refresh, freq }),
        marketApi.getAIndexBollPicks({ refresh, freq }),
      ]);
      setAIndexData(resp);
      setAIndexBollPicks(bollResp.items ?? []);
    } catch (err: unknown) {
      setAIndexError(err instanceof Error ? err.message : '加载失败');
      setAIndexBollPicks([]);
    } finally {
      setAIndexLoading(false);
      setAIndexBollLoading(false);
    }
  }, []);

  useEffect(() => { void fetchEtf(false); }, [fetchEtf]);

  useEffect(() => {
    if (activeTab === 'global' && !globalData && !globalLoading) {
      void fetchGlobal(false);
    }
    if (activeTab === 'aindex' && !aIndexData && !aIndexLoading) {
      void fetchAIndex(false, aIndexFreq);
    }
  }, [activeTab, globalData, globalLoading, fetchGlobal, aIndexData, aIndexLoading, fetchAIndex, aIndexFreq]);

  const handleRefresh = useCallback(() => {
    if (activeTab === 'etf') void fetchEtf(true);
    else if (activeTab === 'global') void fetchGlobal(true);
    else if (activeTab === 'aindex') void fetchAIndex(true, aIndexFreq);
  }, [activeTab, fetchEtf, fetchGlobal, fetchAIndex, aIndexFreq]);

  const isLoading = activeTab === 'etf' ? etfLoading : (activeTab === 'global' ? globalLoading : aIndexLoading);
  const isRefreshing = activeTab === 'etf' ? etfRefreshing : false;

  const handleFreqChange = useCallback((newFreq: string) => {
    setAIndexFreq(newFreq);
    setAIndexData(null);
    void fetchAIndex(false, newFreq);
  }, [fetchAIndex]);

  const handleConstituentClick = useCallback(async (tsCode: string) => {
    setConstituentIndex(tsCode);
    setConstituentLoading(true);
    setConstituentData([]);
    try {
      const resp = await marketApi.getAIndexConstituents(tsCode);
      setConstituentData(resp.items ?? []);
    } catch {
      setConstituentData([]);
    } finally {
      setConstituentLoading(false);
    }
  }, []);

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="space-y-4">
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <PieChart className="h-5 w-5 text-tertiary-text" />
            <h1 className="text-lg font-semibold">指数新高</h1>
          </div>
          <Button variant="secondary" size="sm" disabled={isLoading || isRefreshing} onClick={handleRefresh}>
            <RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />刷新
          </Button>
        </div>

        <Tabs activeKey={activeTab} onChange={setActiveTab}
          items={[
            {
              key: 'etf',
              label: <span className="text-sm font-medium">ETF 专题</span>,
              children: (
                <NewHighTablePanel
                  loading={etfLoading} refreshing={etfRefreshing}
                  data={etfData} error={etfError}
                  bollPicks={etfBollPicks} bollLoading={etfBollLoading} bollMeta={etfBollMeta}
                  onRefresh={fetchEtf}
                  currentCloseKey="current_close" ytdReturnKey="ytd_return_pct"
                  loadKlines={(code, sd) => marketApi.getEtfKlines(code, sd)} />
              ),
            },
            {
              key: 'global',
              label: <span className="text-sm font-medium">国际主要指数</span>,
              children: (
                <NewHighTablePanel
                  loading={globalLoading} refreshing={false}
                  data={globalData} error={globalError}
                  bollPicks={globalBollPicks} bollLoading={globalBollLoading}
                  bollMeta={{ nearPct: 2, lookbackDays: 30, maxDrawdownFromHighPct: 30 }}
                  onRefresh={fetchGlobal}
                  currentCloseKey="current_close" ytdReturnKey="ytd_return_pct"
                  loadKlines={(code, sd) => marketApi.getGlobalIndexKlines(code, sd)} />
              ),
            },
            {
              key: 'aindex',
              label: <span className="text-sm font-medium">A 股指数</span>,
              children: (
                <div className="space-y-3">
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-tertiary-text">BOLL 频率：</span>
                    <div className="flex gap-1">
                      <button type="button"
                        className={`rounded px-2.5 py-1 text-xs font-medium transition-colors ${
                          aIndexFreq === 'daily'
                            ? 'bg-primary/15 text-primary ring-1 ring-primary/30'
                            : 'bg-muted/20 text-secondary-text hover:bg-muted/40'
                        }`}
                        onClick={() => handleFreqChange('daily')}>日线</button>
                      <button type="button"
                        className={`rounded px-2.5 py-1 text-xs font-medium transition-colors ${
                          aIndexFreq === 'weekly'
                            ? 'bg-primary/15 text-primary ring-1 ring-primary/30'
                            : 'bg-muted/20 text-secondary-text hover:bg-muted/40'
                        }`}
                        onClick={() => handleFreqChange('weekly')}>周线</button>
                    </div>
                  </div>
                  <NewHighTablePanel
                    loading={aIndexLoading} refreshing={false}
                    data={aIndexData} error={aIndexError}
                    bollPicks={aIndexBollPicks} bollLoading={aIndexBollLoading}
                    bollMeta={{ nearPct: 2, lookbackDays: 30, maxDrawdownFromHighPct: 30 }}
                    onRefresh={(r) => fetchAIndex(r, aIndexFreq)}
                    currentCloseKey="current_close" ytdReturnKey="ytd_return_pct"
                    loadKlines={(code, sd) => marketApi.getAIndexKlines(code, sd, undefined, aIndexFreq)}
                    onNameClick={handleConstituentClick}
                    rightOverlay={constituentIndex && constituentData ? (
                      <div className="relative h-full w-full">
                        <BollPickPanel
                          loading={false} picks={[]}
                          nearPct={2} lookbackDays={30} maxDrawdownFromHighPct={30}
                          onSelect={() => {}} activeTsCode=""
                          className="h-full max-h-full opacity-20 pointer-events-none" />
                        <div className="absolute inset-0 z-10">
                          <div className="relative h-full w-full rounded-xl border border-border/20 bg-card shadow-xl flex flex-col">
                            <div className="flex items-center justify-between border-b border-border/20 px-3 py-2 shrink-0">
                              <div className="text-sm font-medium text-foreground">{constituentIndex} 成分股</div>
                              <button type="button" onClick={() => setConstituentIndex(null)}
                                className="rounded p-0.5 text-tertiary-text hover:text-foreground hover:bg-muted/30 transition-colors">
                                <X className="h-4 w-4" />
                              </button>
                            </div>
                            <div className="flex-1 overflow-y-auto p-2 min-h-0">
                              {constituentLoading ? (
                                <div className="flex items-center justify-center gap-1.5 py-6 text-[10px] text-tertiary-text">
                                  <Loader2 className="h-3 w-3 animate-spin" />加载中…
                                </div>
                              ) : constituentData.length === 0 ? (
                                <div className="py-6 text-center text-[10px] text-tertiary-text">暂无成分股数据</div>
                              ) : (
                                <div className="grid grid-cols-2 gap-1.5">
                                  {constituentData.map(c => (
                                    <div key={c.con_code}
                                      className="flex items-center justify-between rounded-lg border border-border/10 bg-muted/15 px-2 py-1 text-xs">
                                      <div className="truncate min-w-0">
                                        <div className="font-medium text-foreground truncate">{c.con_name || c.con_code}</div>
                                        <div className="font-mono text-[10px] text-tertiary-text">{c.con_code}</div>
                                      </div>
                                      {c.weight != null && (
                                        <span className="shrink-0 ml-2 font-mono tabular-nums text-secondary-text">{c.weight.toFixed(2)}%</span>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    ) : undefined} />
                </div>
              ),
            },
          ]}
        />
      </div>
    </AppPage>
  );
};

export default EtfNewHighPage;
