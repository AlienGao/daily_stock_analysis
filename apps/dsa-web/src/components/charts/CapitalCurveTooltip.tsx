export function fmtMoney(v: number): string {
  if (Math.abs(v) >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(0)}万`;
  return v.toFixed(0);
}

export function fmtSignedPct(v: number): string {
  return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`;
}

function returnColor(v: number): string {
  return v >= 0 ? '#f87171' : '#34d399';
}

function formatCurveSeriesLabel(dataKey: string, name?: string): string {
  if (name) return name;
  if (dataKey === 'benchmark' || dataKey === 'h_benchmark') return '上证指数';
  if (dataKey === 'capital') return '资金';
  if (dataKey === 'lgb') return 'LGB';
  const holdMatch = dataKey.match(/^h(?:d)?(\d+)(?:_(fixed|dynamic))?$/);
  if (holdMatch) {
    let label = `${holdMatch[1]}日`;
    if (holdMatch[2] === 'fixed') label += ' (固定)';
    if (holdMatch[2] === 'dynamic') label += ' (动态)';
    return label;
  }
  return `${dataKey.replace(/^h(?:d)?/, '')}日`;
}

export function buildCapitalCurveChartMeta(
  chartData: Array<Record<string, unknown>>,
): { latestDate: string; latestByKey: Record<string, number>; baseByKey: Record<string, number> } {
  if (chartData.length === 0) {
    return { latestDate: '', latestByKey: {}, baseByKey: {} };
  }
  const latestByKey: Record<string, number> = {};
  const baseByKey: Record<string, number> = {};
  for (const row of chartData) {
    for (const [key, value] of Object.entries(row)) {
      if (key === 'date' || typeof value !== 'number') continue;
      if (baseByKey[key] == null) baseByKey[key] = value;
      latestByKey[key] = value;
    }
  }
  const last = chartData[chartData.length - 1];
  return { latestDate: String(last.date ?? ''), latestByKey, baseByKey };
}

export interface CapitalCurveTooltipProps {
  active?: boolean;
  payload?: Array<{ dataKey?: string | number; value?: number; color?: string; name?: string }>;
  label?: string;
  latestByKey: Record<string, number>;
  latestDate: string;
  baseByKey?: Record<string, number>;
}

export function CapitalCurveTooltip({
  active,
  payload,
  label,
  latestByKey,
  latestDate,
  baseByKey = {},
}: CapitalCurveTooltipProps) {
  if (!active || !payload?.length) return null;
  const hoverDate = String(label ?? '');
  const showTailReturn = hoverDate && latestDate && hoverDate !== latestDate;

  return (
    <div
      style={{
        backgroundColor: '#000',
        border: '1px solid #333',
        borderRadius: 6,
        padding: '8px 12px',
        fontSize: 12,
        minWidth: 180,
      }}
    >
      <div style={{ color: '#fff', marginBottom: 6 }}>{hoverDate}</div>
      {payload
        .filter((p) => p.value != null && p.dataKey != null)
        .map((p) => {
          const key = String(p.dataKey);
          const value = Number(p.value);
          const base = baseByKey[key] ?? value;
          const cumReturn = base > 0 ? (value - base) / base : 0;
          const latest = latestByKey[key];
          const tailReturn = showTailReturn && latest != null && value > 0
            ? (latest - value) / value
            : null;
          return (
            <div key={key} style={{ marginBottom: 4 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#fff' }}>
                <span
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    backgroundColor: p.color || '#888',
                    flexShrink: 0,
                  }}
                />
                <span style={{ color: '#aaa' }}>{formatCurveSeriesLabel(key, p.name)}</span>
                <span style={{ marginLeft: 'auto', color: returnColor(cumReturn) }}>
                  {fmtSignedPct(cumReturn)}
                </span>
              </div>
              {tailReturn != null && (
                <div
                  style={{
                    marginLeft: 16,
                    marginTop: 2,
                    color: returnColor(tailReturn),
                    fontSize: 11,
                  }}
                >
                  至最新({latestDate.slice(5)}) {fmtSignedPct(tailReturn)}
                </div>
              )}
            </div>
          );
        })}
    </div>
  );
}
