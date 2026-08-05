import type React from 'react';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Table } from 'antd';
import type { ColumnsType, SorterResult, SortOrder } from 'antd/es/table/interface';
import { Loader2, RefreshCw, Search } from 'lucide-react';
import { AppPage, Button, EmptyState } from '../components/common';
import { CandlestickMiniChart } from '../components/charts/CandlestickMiniChart';
import { hkStockApi, type HkBollPickItem, type HkStockKLineItem, type HkStockListItem } from '../api/hkMonitor';
import { calcBollBandWidthPct, compareBollBandWidth } from '../utils/hkBollBandwidth';
import { sortHkItemsByPctChangeDesc } from '../utils/hkMonitorSort';

const fmtPct = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return '--';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}%`;
};

const measureTableListHeight = (root: HTMLElement): number => {
  const wrapper = root.querySelector('.ant-table-wrapper');
  if (!wrapper) return root.offsetHeight;
  return Math.ceil((wrapper as HTMLElement).getBoundingClientRect().height);
};

const pctColor = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return 'text-secondary-text';
  return v >= 0 ? 'text-red-400' : 'text-emerald-400';
};

const fmtPrice = (v?: number | null) => (v == null || Number.isNaN(v) ? '--' : v.toFixed(3));

const calcDistPct = (price?: number | null, band?: number | null) => {
  if (price == null || band == null || !Number.isFinite(price) || !Number.isFinite(band) || band <= 0) return null;
  return Number((((price - band) / band) * 100).toFixed(2));
};

const DEFAULT_TABLE_SORT = { columnKey: 'pct_change', order: 'descend' as SortOrder };

const calcLatestConsecutiveDrawdown = (
  bars: Array<{ date: string; close: number }>,
): { pct: number; days: number; startDate: string; endDate: string } | null => {
  let endIdx = bars.length - 1;
  while (endIdx > 0) {
    while (endIdx > 0 && bars[endIdx].close >= bars[endIdx - 1].close) endIdx -= 1;
    if (endIdx <= 0) return null;

    let startIdx = endIdx;
    while (startIdx > 0 && bars[startIdx].close < bars[startIdx - 1].close) startIdx -= 1;
    const days = endIdx - startIdx;
    if (days >= 2) {
      return {
        pct: Number((((bars[endIdx].close - bars[startIdx].close) / bars[startIdx].close) * 100).toFixed(2)),
        days,
        startDate: bars[startIdx].date,
        endDate: bars[endIdx].date,
      };
    }
    endIdx = startIdx - 1;
  }
  return null;
};

const patchItemFromKlines = (item: HkStockListItem, klines: HkStockKLineItem[]): HkStockListItem => {
  const bars = klines.filter(bar => bar.close != null && Number.isFinite(bar.close));
  const latest = bars.at(-1);
  if (!latest) return item;

  const prev = [...bars].reverse().find(bar => bar.date < latest.date && bar.close > 0);
  const pctChange = prev ? Number((((latest.close - prev.close) / prev.close) * 100).toFixed(2)) : item.pct_change;

  const loadedHighs = klines
    .map(bar => bar.high)
    .filter((high): high is number => high != null && Number.isFinite(high) && high > 0);
  const highNPrice = item.high_n_price != null
    ? Math.max(item.high_n_price, latest.close, ...loadedHighs)
    : null;
  const drawdownPct = highNPrice != null && highNPrice > 0 && latest.close > 0
    ? Number((((latest.close - highNPrice) / highNPrice) * 100).toFixed(2))
    : item.drawdown_pct ?? null;
  const latestConsecutiveDrawdown = calcLatestConsecutiveDrawdown(bars);

  return {
    ...item,
    latest_price: latest.close,
    pct_change: pctChange,
    boll_mid: latest.boll_mid ?? item.boll_mid ?? null,
    boll_upper: latest.boll_upper ?? item.boll_upper ?? null,
    boll_lower: latest.boll_lower ?? item.boll_lower ?? null,
    boll_mid_dist_pct: calcDistPct(latest.close, latest.boll_mid) ?? item.boll_mid_dist_pct ?? null,
    boll_upper_dist_pct: calcDistPct(latest.close, latest.boll_upper) ?? item.boll_upper_dist_pct ?? null,
    boll_lower_dist_pct: calcDistPct(latest.close, latest.boll_lower) ?? item.boll_lower_dist_pct ?? null,
    high_n_price: highNPrice,
    drawdown_pct: drawdownPct,
    latest_consecutive_drawdown_pct: latestConsecutiveDrawdown?.pct ?? item.latest_consecutive_drawdown_pct ?? null,
    latest_consecutive_drawdown_days: latestConsecutiveDrawdown?.days ?? item.latest_consecutive_drawdown_days ?? null,
    latest_consecutive_drawdown_start_date: latestConsecutiveDrawdown?.startDate
      ?? item.latest_consecutive_drawdown_start_date ?? null,
    latest_consecutive_drawdown_end_date: latestConsecutiveDrawdown?.endDate
      ?? item.latest_consecutive_drawdown_end_date ?? null,
  };
};

const BollDistanceCell: React.FC<{ distance?: number | null; bandPrice?: number | null }> = ({ distance, bandPrice }) => (
  <span
    className={`font-mono text-xs tabular-nums ${pctColor(distance)}`}
    title={bandPrice == null ? undefined : `轨道价 ${fmtPrice(bandPrice)}`}
  >
    {fmtPct(distance)}
  </span>
);

// ── BOLL 推荐卡片 ─────────────────────────────────────────

const mapBandClass: Record<string, string> = {
  upper: 'text-orange-400',
  mid: 'text-sky-400',
  lower: 'text-pink-400',
};

const mapBandLabel: Record<string, string> = {
  upper: '上轨',
  mid: '中轨',
  lower: '下轨',
};

// 左侧列表行背景色，与右侧 BOLL 推荐（mapBandClass）字体颜色一一对应：
// 上轨橙 / 中轨天蓝 / 下轨粉，便于左右联动定位。
const mapBandBg: Record<string, string> = {
  upper: 'bg-orange-500/10',
  mid: 'bg-sky-500/10',
  lower: 'bg-pink-500/10',
};

const BollPickCard: React.FC<{
  item: HkBollPickItem;
  active: boolean;
  onSelect: (hkCode: string) => void;
}> = ({ item, active, onSelect }) => (
  <button
    type="button"
    onClick={() => onSelect(item.hk_code)}
    className={`w-full rounded-lg border px-2 py-1.5 text-left transition-colors ${
      active
        ? 'border-primary/40 bg-primary/10 ring-1 ring-primary/30'
        : 'border-border/15 bg-muted/20 hover:border-border/30 hover:bg-muted/35'
    }`}
  >
    <div className="truncate text-xs font-medium text-foreground">{item.name}</div>
    <div className="font-mono text-[10px] text-tertiary-text">{item.hk_code}</div>
    <div className="mt-1 flex items-center justify-between gap-2 text-[10px]">
      <span className="text-tertiary-text">现价</span>
      <span className="font-mono tabular-nums">{item.close.toFixed(3)}</span>
    </div>
    <div className="mt-0.5 flex items-center justify-between gap-2 text-[10px]">
      <span className="text-tertiary-text">距轨道</span>
      <span className={`font-mono tabular-nums ${item.dist_pct != null ? pctColor(item.dist_pct) : ''}`}>
        {fmtPct(item.dist_pct)}
      </span>
    </div>
    <div className="mt-0.5 text-[10px] text-tertiary-text">
      BOLL <span className={mapBandClass[item.band] || ''}>{mapBandLabel[item.band] || item.band}</span>
    </div>
  </button>
);

const BollPickColumn: React.FC<{
  title: string;
  titleClass: string;
  items: HkBollPickItem[];
  loading: boolean;
  emptyText: string;
  activeHkCode: string;
  onSelect: (hkCode: string) => void;
}> = ({ title, titleClass, items, loading, emptyText, activeHkCode, onSelect }) => (
  <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-lg border border-border/15 bg-muted/10">
    <div className={`shrink-0 border-b border-border/15 px-2 py-1.5 text-[11px] font-medium ${titleClass}`}>
      {title} <span className="ml-1 font-normal text-tertiary-text">({items.length})</span>
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
            <BollPickCard key={item.hk_code} item={item} active={activeHkCode === item.hk_code} onSelect={onSelect} />
          ))}
        </div>
      )}
    </div>
  </div>
);

const BollPickPanel: React.FC<{
  loading: boolean;
  picks: HkBollPickItem[];
  drawdownItems: HkStockListItem[];
  activeHkCode: string;
  onSelect: (hkCode: string) => void;
  className?: string;
}> = ({ loading, picks, drawdownItems, activeHkCode, onSelect, className = '' }) => {
  const upperPicks = useMemo(() => picks.filter(p => p.band === 'upper'), [picks]);
  const midPicks = useMemo(() => picks.filter(p => p.band === 'mid'), [picks]);
  const lowerPicks = useMemo(() => picks.filter(p => p.band === 'lower'), [picks]);
  const recentDrawdowns = useMemo(() => drawdownItems
    .filter(item => (item.latest_consecutive_drawdown_days ?? 0) >= 2 && item.latest_consecutive_drawdown_pct != null)
    .sort((a, b) => (a.latest_consecutive_drawdown_pct ?? 0) - (b.latest_consecutive_drawdown_pct ?? 0))
    .slice(0, 8), [drawdownItems]);

  return (
    <div className={`flex h-full max-h-full min-h-0 flex-col overflow-hidden rounded-xl border border-border/20 bg-card/40 ${className}`}>
      <div className="shrink-0 border-b border-border/20 px-3 py-2">
        <div className="mb-2 rounded-lg border border-border/15 bg-muted/10 p-2">
          <div className="text-xs font-medium text-foreground">最近结束的最高回撤</div>
          <div className="mt-1 space-y-1">
            {recentDrawdowns.length ? recentDrawdowns.map(item => (
              <button type="button" key={item.hk_code} onClick={() => onSelect(item.hk_code)} className="grid w-full grid-cols-3 items-center gap-2 rounded px-1 py-0.5 text-left text-[10px] hover:bg-muted/30">
                <span className="min-w-0 truncate text-foreground">{item.name || item.hk_code}</span>
                <span className="truncate whitespace-nowrap text-center font-mono text-[9px] text-tertiary-text">
                  {item.latest_consecutive_drawdown_start_date || '--'} ~ {item.latest_consecutive_drawdown_end_date || '--'}
                </span>
                <span className="text-right font-mono text-emerald-400">{fmtPct(item.latest_consecutive_drawdown_pct)}</span>
              </button>
            )) : <div className="py-1 text-[10px] text-tertiary-text">暂无最近连续回撤</div>}
          </div>
        </div>
        <div className="text-sm font-medium text-foreground">BOLL 推荐</div>
        <div className="mt-0.5 text-[11px] text-tertiary-text">收盘价距 BOLL(20,2) 轨道 ±1.5%</div>
      </div>
      <div className="grid h-full min-h-0 flex-1 grid-cols-3 grid-rows-1 gap-2 overflow-hidden p-2">
        <BollPickColumn title="上轨附近" titleClass="text-orange-400" items={upperPicks} loading={loading} emptyText="暂无" activeHkCode={activeHkCode} onSelect={onSelect} />
        <BollPickColumn title="中轨附近" titleClass="text-sky-400" items={midPicks} loading={loading} emptyText="暂无" activeHkCode={activeHkCode} onSelect={onSelect} />
        <BollPickColumn title="下轨附近" titleClass="text-pink-400" items={lowerPicks} loading={loading} emptyText="暂无" activeHkCode={activeHkCode} onSelect={onSelect} />
      </div>
    </div>
  );
};

// ── K 线图 ────────────────────────────────────────────────

const KLineChart: React.FC<{ data: HkStockKLineItem[]; loading: boolean }> = ({ data, loading }) => {
  const chartData = useMemo(() => data
    .filter(d => d.close > 0)
    .map(d => ({
      date: d.date,
      price: d.close,
      open: d.open ?? d.close,
      high: d.high ?? d.close,
      low: d.low ?? d.close,
    })), [data]);

  if (loading) return <div className="flex h-48 items-center justify-center text-secondary-text"><Loader2 className="mr-2 h-4 w-4 animate-spin" />加载 K 线…</div>;
  if (!chartData.length) return <div className="py-6 text-center text-sm text-secondary-text">暂无日 K 线数据</div>;

  return (
    <div className="w-full min-w-0">
      <div className="mb-1 text-xs text-tertiary-text">共 {chartData.length} 个交易日</div>
      <div className="overflow-x-auto">
        <CandlestickMiniChart data={chartData} height={200} barPitch={6} overlay="boll" />
      </div>
    </div>
  );
};

const ExpandPanel: React.FC<{
  record: HkStockListItem;
  onKlinesLoaded: (hkCode: string, klines: HkStockKLineItem[]) => void;
}> = ({ record, onKlinesLoaded }) => {
  const [klines, setKlines] = useState<HkStockKLineItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void hkStockApi.getKlines(record.hk_code)
      .then(resp => {
        if (!cancelled) {
          setKlines(resp.data);
          onKlinesLoaded(record.hk_code, resp.data);
          setLoading(false);
        }
      })
      .catch(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [onKlinesLoaded, record.hk_code]);

  return <KLineChart data={klines} loading={loading} />;
};

// ── 主页面 ────────────────────────────────────────────────

const HkMonitorPage: React.FC = () => {
  const [items, setItems] = useState<HkStockListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedKey, setExpandedKey] = useState<string>('');
  const [searchText, setSearchText] = useState<string>('');
  const [bollPicks, setBollPicks] = useState<HkBollPickItem[]>([]);
  const [bollLoading] = useState(false);
  const [tableSort, setTableSort] = useState(DEFAULT_TABLE_SORT);
  const [panelHeight, setPanelHeight] = useState<number | undefined>(undefined);
  const tableWrapRef = useRef<HTMLDivElement>(null);

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
    const wrapper = el.querySelector('.ant-table-wrapper');
    if (wrapper) ro.observe(wrapper);
    return () => ro.disconnect();
  }, [items, expandedKey, loading, searchText]);

  const load = useCallback(async (opts?: { refresh?: boolean }) => {
    setLoading(true);
    setError(null);
    try {
      const shouldRefresh = opts?.refresh ?? false;
      const [listResp, bollResp] = shouldRefresh
        ? [await hkStockApi.list({ refresh: true }), await hkStockApi.getBollPicks()]
        : await Promise.all([
          hkStockApi.list(),
          hkStockApi.getBollPicks(),
        ]);
      setItems(sortHkItemsByPctChangeDesc(listResp.items ?? []));
      if (shouldRefresh) setTableSort({ ...DEFAULT_TABLE_SORT });
      setBollPicks([...(bollResp.upper ?? []), ...(bollResp.mid ?? []), ...(bollResp.lower ?? [])]);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : '加载失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const filteredItems = useMemo(() => {
    if (!searchText.trim()) return items;
    const q = searchText.trim().toLowerCase();
    return items.filter(item =>
      (item.name && item.name.toLowerCase().includes(q)) ||
      item.hk_code.includes(q)
    );
  }, [items, searchText]);

  const handleKlinesLoaded = useCallback((hkCode: string, klines: HkStockKLineItem[]) => {
    setItems(prevItems => prevItems.map(item => (
      item.hk_code === hkCode ? patchItemFromKlines(item, klines) : item
    )));
  }, []);

  const columns: ColumnsType<HkStockListItem> = useMemo(() => [
    {
      title: <span className="whitespace-nowrap">名称 / 代码</span>,
      dataIndex: 'name',
      key: 'name',
      width: 112,
      render: (v: string | null, record) => (
        <span className="block min-w-0">
          <span className="block max-w-full truncate whitespace-nowrap text-sm font-medium text-foreground" title={v || undefined}>{v || '--'}</span>
          <span className="mt-0.5 block font-mono text-[11px] text-tertiary-text">{record.hk_code}</span>
        </span>
      ),
    },
    {
      title: <span className="whitespace-nowrap">最新价</span>,
      dataIndex: 'latest_price',
      align: 'right',
      render: (v: number | null) => (
        <span className="font-mono tabular-nums">{fmtPrice(v)}</span>
      ),
    },
    {
      title: <span className="whitespace-nowrap">距上轨</span>,
      dataIndex: 'boll_upper_dist_pct',
      key: 'boll_upper_dist_pct',
      align: 'right',
      sorter: (a, b) => (a.boll_upper_dist_pct ?? Number.POSITIVE_INFINITY) - (b.boll_upper_dist_pct ?? Number.POSITIVE_INFINITY),
      sortOrder: tableSort.columnKey === 'boll_upper_dist_pct' ? tableSort.order : null,
      render: (_v: number | null, record) => (
        <BollDistanceCell distance={record.boll_upper_dist_pct} bandPrice={record.boll_upper} />
      ),
    },
    {
      title: <span className="whitespace-nowrap">距中轨</span>,
      dataIndex: 'boll_mid_dist_pct',
      key: 'boll_mid_dist_pct',
      align: 'right',
      sorter: (a, b) => (a.boll_mid_dist_pct ?? Number.POSITIVE_INFINITY) - (b.boll_mid_dist_pct ?? Number.POSITIVE_INFINITY),
      sortOrder: tableSort.columnKey === 'boll_mid_dist_pct' ? tableSort.order : null,
      render: (_v: number | null, record) => (
        <BollDistanceCell distance={record.boll_mid_dist_pct} bandPrice={record.boll_mid} />
      ),
    },
    {
      title: <span className="whitespace-nowrap">距下轨</span>,
      dataIndex: 'boll_lower_dist_pct',
      key: 'boll_lower_dist_pct',
      align: 'right',
      sorter: (a, b) => (a.boll_lower_dist_pct ?? Number.POSITIVE_INFINITY) - (b.boll_lower_dist_pct ?? Number.POSITIVE_INFINITY),
      sortOrder: tableSort.columnKey === 'boll_lower_dist_pct' ? tableSort.order : null,
      render: (_v: number | null, record) => (
        <BollDistanceCell distance={record.boll_lower_dist_pct} bandPrice={record.boll_lower} />
      ),
    },
    {
      title: <span className="whitespace-nowrap">轨道宽度</span>,
      key: 'boll_band_width_pct',
      align: 'right',
      sorter: compareBollBandWidth,
      sortOrder: tableSort.columnKey === 'boll_band_width_pct' ? tableSort.order : null,
      render: (_v, record) => {
        const widthPct = calcBollBandWidthPct(record);
        return (
          <span className="font-mono text-xs tabular-nums text-emerald-400">
            {widthPct == null ? '--' : `${widthPct.toFixed(2)}%`}
          </span>
        );
      },
    },
    {
      title: <span className="whitespace-nowrap">最高回撤</span>,
      dataIndex: 'drawdown_pct',
      key: 'drawdown_pct',
      align: 'right',
      width: 112,
      sorter: (a, b) => (a.drawdown_pct ?? Number.NEGATIVE_INFINITY) - (b.drawdown_pct ?? Number.NEGATIVE_INFINITY),
      sortOrder: tableSort.columnKey === 'drawdown_pct' ? tableSort.order : null,
      render: (v: number | null | undefined, record) => (
        <span
          className={`font-mono text-xs tabular-nums ${v != null ? 'text-amber-400' : 'text-tertiary-text'}`}
          title={record.high_n_price != null ? `数据库历史最高价 ${fmtPrice(record.high_n_price)}` : undefined}
        >
          {v != null ? `${v.toFixed(2)}%` : '--'}
        </span>
      ),
    },
    {
      title: <span className="whitespace-nowrap">连续回撤</span>,
      dataIndex: 'latest_consecutive_drawdown_pct',
      key: 'latest_consecutive_drawdown_pct',
      align: 'right',
      width: 112,
      sorter: (a, b) => (a.latest_consecutive_drawdown_pct ?? Number.NEGATIVE_INFINITY)
        - (b.latest_consecutive_drawdown_pct ?? Number.NEGATIVE_INFINITY),
      sortOrder: tableSort.columnKey === 'latest_consecutive_drawdown_pct' ? tableSort.order : null,
      render: (v: number | null | undefined, record) => {
        const startDate = record.latest_consecutive_drawdown_start_date;
        const endDate = record.latest_consecutive_drawdown_end_date;
        const shortDate = (date?: string | null) => date
          ? `${date.replaceAll('-', '').slice(4, 6)}-${date.replaceAll('-', '').slice(6, 8)}`
          : '--';
        const fullDate = (date?: string | null) => date
          ? date.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')
          : '--';
        return (
          <span
            className="block font-mono tabular-nums"
            title={record.latest_consecutive_drawdown_days != null
              ? `最近一次连续下跌 ${record.latest_consecutive_drawdown_days} 个交易日：${fullDate(startDate)} 至 ${fullDate(endDate)}`
              : undefined}
          >
            <span className={`block text-xs ${v != null ? 'text-orange-400' : 'text-tertiary-text'}`}>
              {v != null ? `${v.toFixed(2)}%` : '--'}
            </span>
            {startDate && endDate && (
              <span className="mt-0.5 block whitespace-nowrap text-[10px] leading-none text-tertiary-text">
                {shortDate(startDate)} → {shortDate(endDate)}
              </span>
            )}
          </span>
        );
      },
    },
    {
      title: <span className="whitespace-nowrap">涨跌幅</span>,
      dataIndex: 'pct_change',
      key: 'pct_change',
      align: 'right',
      sorter: (a, b) => (a.pct_change ?? Number.NEGATIVE_INFINITY) - (b.pct_change ?? Number.NEGATIVE_INFINITY),
      sortOrder: tableSort.columnKey === 'pct_change' ? tableSort.order : null,
      render: (v: number | null) => (
        <span className={`font-mono tabular-nums ${pctColor(v)}`}>{fmtPct(v)}</span>
      ),
    },
  ], [tableSort]);

  const handleTableChange = useCallback((
    _pagination: unknown,
    _filters: unknown,
    sorter: SorterResult<HkStockListItem> | SorterResult<HkStockListItem>[],
  ) => {
    const activeSorter = Array.isArray(sorter) ? sorter[0] : sorter;
    const columnKey = activeSorter?.columnKey;
    setTableSort({
      columnKey: columnKey == null ? '' : String(columnKey),
      order: activeSorter?.order ?? null,
    });
  }, []);

  const locateStock = useCallback((hkCode: string) => {
    if (expandedKey === hkCode) { setExpandedKey(''); setSearchText(''); return; }
    setSearchText(hkCode);
    setExpandedKey(hkCode);
  }, [expandedKey]);

  const bandByCode = useMemo(() => {
    const m = new Map<string, string>();
    for (const p of bollPicks) m.set(p.hk_code, p.band);
    return m;
  }, [bollPicks]);

  const rowClassName = useCallback((record: HkStockListItem) => {
    const band = bandByCode.get(record.hk_code);
    return band ? (mapBandBg[band] ?? '') : '';
  }, [bandByCode]);

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="space-y-4">
        <div className="flex items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-lg font-semibold">港股通监控</h1>
            </div>
            <p className="mt-1 text-xs text-tertiary-text">成份快照 + 展开日 K 线（BOLL 叠加）· BOLL 推荐三列</p>
          </div>
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-tertiary-text" />
              <input
                type="text"
                placeholder="搜索名称 / 代码…"
                value={searchText}
                onChange={e => setSearchText(e.target.value)}
                className="h-7 w-48 rounded-md border border-border/20 bg-muted/20 pl-7 pr-2 text-xs text-foreground outline-none placeholder:text-tertiary-text focus:border-primary/40 focus:ring-1 focus:ring-primary/30"
              />
            </div>
            <Button variant="secondary" size="sm" disabled={loading} onClick={() => { void load({ refresh: true }); }}>
              <RefreshCw className={`mr-1.5 h-3.5 w-3.5 ${loading ? 'animate-spin' : ''}`} />
              刷新
            </Button>
          </div>
        </div>

        {error ? (
          <EmptyState title="加载失败" description={error} />
        ) : loading && !items.length ? (
          <div className="flex items-center justify-center gap-2 py-16 text-sm text-tertiary-text">
            <Loader2 className="h-4 w-4 animate-spin" />
            加载港股通成份…
          </div>
        ) : !items.length ? (
          <EmptyState title="暂无成份数据" description="港股通成份列表自动加载中" />
        ) : (
          <div className="flex items-start gap-3">
            <div ref={tableWrapRef} className="min-w-0 flex-1">
              <div className="mb-2 text-xs text-tertiary-text">
                共 {items.length} 只 · BOLL 推荐 {bollPicks.length} 只
              </div>
              <Table<HkStockListItem>
                rowKey="hk_code"
                size="small"
                tableLayout="fixed"
                pagination={{ pageSize: 50, showSizeChanger: true }}
                columns={columns}
                dataSource={filteredItems}
                onChange={handleTableChange}
                rowClassName={rowClassName}
                expandable={{
                  expandedRowKeys: expandedKey ? [expandedKey] : [],
                  onExpand: (expanded, record) => setExpandedKey(expanded ? record.hk_code : ''),
                  expandedRowRender: record => <ExpandPanel record={record} onKlinesLoaded={handleKlinesLoaded} />,
                }}
              />
            </div>
            <div className="sticky top-2 hidden min-h-[calc(100vh-8rem)] w-[440px] shrink-0 overflow-hidden lg:block xl:w-[480px]"
              style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : { minHeight: "calc(100vh - 8rem)" }}>
              <BollPickPanel
                loading={bollLoading}
                picks={bollPicks}
                drawdownItems={items}
                activeHkCode={expandedKey}
                onSelect={locateStock}
              />
            </div>
          </div>
        )}
        {items.length > 0 && (
          <div className="min-h-0 overflow-hidden lg:hidden" style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : { height: 360, maxHeight: 360 }}>
            <BollPickPanel
              className="h-full max-h-full"
              loading={bollLoading}
              picks={bollPicks}
              drawdownItems={items}
              activeHkCode={expandedKey}
              onSelect={locateStock}
            />
          </div>
        )}
      </div>
    </AppPage>
  );
};

export default HkMonitorPage;
