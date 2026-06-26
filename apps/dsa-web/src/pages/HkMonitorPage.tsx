import type React from 'react';
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { Loader2, RefreshCw, Search } from 'lucide-react';
import { AppPage, Button, EmptyState } from '../components/common';
import { CandlestickMiniChart } from '../components/charts/CandlestickMiniChart';
import { hkStockApi, type HkBollPickItem, type HkStockKLineItem, type HkStockListItem } from '../api/hkMonitor';

const fmtPct = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return '--';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}%`;
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

const pctColor = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return 'text-secondary-text';
  return v >= 0 ? 'text-red-400' : 'text-emerald-400';
};

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
  activeHkCode: string;
  onSelect: (hkCode: string) => void;
  className?: string;
}> = ({ loading, picks, activeHkCode, onSelect, className = '' }) => {
  const upperPicks = useMemo(() => picks.filter(p => p.band === 'upper'), [picks]);
  const midPicks = useMemo(() => picks.filter(p => p.band === 'mid'), [picks]);
  const lowerPicks = useMemo(() => picks.filter(p => p.band === 'lower'), [picks]);

  return (
    <div className={`flex h-full max-h-full min-h-0 flex-col overflow-hidden rounded-xl border border-border/20 bg-card/40 ${className}`}>
      <div className="shrink-0 border-b border-border/20 px-3 py-2">
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

const ExpandPanel: React.FC<{ record: HkStockListItem }> = ({ record }) => {
  const [klines, setKlines] = useState<HkStockKLineItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void hkStockApi.getKlines(record.hk_code)
      .then(resp => { if (!cancelled) { setKlines(resp.data); setLoading(false); } })
      .catch(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [record.hk_code]);

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
    return () => ro.disconnect();
  }, [items]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [listResp, bollResp] = await Promise.all([
        hkStockApi.list(),
        hkStockApi.getBollPicks(),
      ]);
      setItems(listResp.items ?? []);
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

  const columns: ColumnsType<HkStockListItem> = useMemo(() => [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      render: (v: string | null) => (
        <span className="text-sm font-medium text-foreground">{v || '--'}</span>
      ),
    },
    {
      title: '代码',
      dataIndex: 'hk_code',
      key: 'hk_code',
      render: (v: string) => (
        <span className="font-mono text-xs text-tertiary-text">{v}</span>
      ),
    },
    {
      title: '最新价',
      dataIndex: 'latest_price',
      align: 'right',
      render: (v: number | null) => (
        <span className="font-mono tabular-nums">{v == null ? '--' : v.toFixed(3)}</span>
      ),
    },
    {
      title: '涨跌幅',
      dataIndex: 'pct_change',
      align: 'right',
      sorter: (a, b) => (a.pct_change ?? 0) - (b.pct_change ?? 0),
      defaultSortOrder: 'descend',
      render: (v: number | null) => (
        <span className={`font-mono tabular-nums ${pctColor(v)}`}>{fmtPct(v)}</span>
      ),
    },
  ], []);

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
            <Button variant="secondary" size="sm" disabled={loading} onClick={load}>
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
                rowClassName={rowClassName}
                expandable={{
                  expandedRowKeys: expandedKey ? [expandedKey] : [],
                  onExpand: (expanded, record) => setExpandedKey(expanded ? record.hk_code : ''),
                  expandedRowRender: record => <ExpandPanel record={record} />,
                }}
              />
            </div>
            <div className="sticky top-2 hidden min-h-[calc(100vh-8rem)] w-[520px] shrink-0 overflow-hidden lg:block xl:w-[560px]"
              style={panelHeight ? { height: panelHeight, maxHeight: panelHeight } : { minHeight: "calc(100vh - 8rem)" }}>
              <BollPickPanel
                loading={bollLoading}
                picks={bollPicks}
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
