import type React from 'react';
import { useLayoutEffect, useRef, useState } from 'react';

type OhlcPoint = {
  date: string;
  price?: number | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
};

/** 分钟 K 用 YYYYMMDDHHmm；日 K 用 YYYYMMDD。兼容旧格式 MM-DD HH:mm。 */
const formatChartDateLabel = (date: string): string => {
  const digits = date.replace(/\D/g, '');
  if (digits.length >= 12) {
    return `${digits.slice(8, 10)}:${digits.slice(10, 12)}`;
  }
  if (digits.length >= 8) {
    return `${Number(digits.slice(4, 6))}/${Number(digits.slice(6, 8))}`;
  }
  return date.length > 11 ? date.slice(6, 11) : date;
};

const formatChartDateTooltip = (date: string): string => {
  const digits = date.replace(/\D/g, '');
  if (digits.length >= 12) {
    return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)} ${digits.slice(8, 10)}:${digits.slice(10, 12)}`;
  }
  if (digits.length >= 8) {
    return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`;
  }
  return date;
};

const hasValidOhlc = (d: OhlcPoint): boolean =>
  d.open != null && d.open > 0 && d.high != null && d.low != null && d.high >= d.low && d.high > 0;

/** SVG candlestick chart for monthly stock trend.
 *  Shows OHLC candles with MA5 or BOLL overlay. */
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

const CandlestickMiniChart: React.FC<{
  data: Array<{ date: string; price?: number | null; open?: number | null; high?: number | null; low?: number | null }>;
  height?: number;
  /** 长区间（如近 6 个月）：K 线横向撑满容器宽度 */
  longSeriesScroll?: boolean;
  barPitch?: number;
  /** K 线叠加指标：默认 MA5；近 6 个月行情使用 BOLL */
  overlay?: 'ma5' | 'boll';
  /** 可视窗口根数：图内横向滚动，默认滚动到最右（展示最近 N 根） */
  windowBars?: number;
}> = ({ data, height = 160, longSeriesScroll = false, barPitch, overlay = 'ma5', windowBars }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [containerW, setContainerW] = useState(0);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const validData = data.filter(d => d.price != null);
  const pads = { t: 14, r: 6, b: 20, l: 42 };
  const count = validData.length;
  const measureWidth = longSeriesScroll || windowBars != null;
  const plotW = Math.max(containerW - pads.l - pads.r, 0);
  const autoPitch = windowBars != null && plotW > 0 ? plotW / Math.max(windowBars - 1, 1) : undefined;
  const pitch = autoPitch ?? barPitch ?? 14;
  const fallbackW = count > 0
    ? Math.max(count * pitch + pads.l + pads.r, pads.l + pads.r + 40)
    : pads.l + pads.r + 40;

  useLayoutEffect(() => {
    if (!measureWidth) return;
    const el = windowBars != null ? scrollRef.current : containerRef.current;
    if (!el) return;
    const apply = () => {
      const w = Math.floor(el.clientWidth);
      if (w > 0) setContainerW(w);
    };
    apply();
    const ro = new ResizeObserver(() => apply());
    ro.observe(el);
    return () => ro.disconnect();
  }, [measureWidth, windowBars, count]);

  useLayoutEffect(() => {
    if (windowBars == null) return;
    const el = scrollRef.current;
    if (el) el.scrollLeft = el.scrollWidth;
  }, [windowBars, count, containerW]);

  if (count < 1) return null;

  const chartW = longSeriesScroll && containerW > 0
    ? containerW
    : windowBars != null && containerW > 0
      ? Math.max(fallbackW, containerW)
      : fallbackW;

  const chartH = height - pads.t - pads.b;
  const xStep = (chartW - pads.l - pads.r) / Math.max(count - 1, 1);

  const closes = validData.map(d => d.price!);
  const ma20Preview = overlay === 'boll' ? computeMA(closes, 20) : [];
  const std20Preview = overlay === 'boll' ? computeStd(closes, 20) : [];
  const bollUpperPreview = overlay === 'boll'
    ? ma20Preview.map((v, i) => (v != null && std20Preview[i] != null ? v + 2 * std20Preview[i]! : null))
    : [];
  const bollLowerPreview = overlay === 'boll'
    ? ma20Preview.map((v, i) => (v != null && std20Preview[i] != null ? v - 2 * std20Preview[i]! : null))
    : [];

  const allPrices: number[] = [];
  validData.forEach(d => {
    if (hasValidOhlc(d)) {
      allPrices.push(d.high!, d.low!, d.open!, d.price!);
    } else {
      allPrices.push(d.price!);
    }
  });
  if (overlay === 'boll') {
    for (const v of [...bollUpperPreview, ...bollLowerPreview, ...ma20Preview]) {
      if (v != null) allPrices.push(v);
    }
  }
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

  const makeOverlayPolyline = (vals: (number | null)[], color: string, dashed?: boolean) => {
    const pts = vals
      .map((v, i) => (v != null ? `${pads.l + i * xStep},${scaleY(v)}` : null))
      .filter(Boolean)
      .join(' ');
    if (!pts) return null;
    return (
      <polyline
        points={pts}
        fill="none"
        stroke={color}
        strokeWidth={1.2}
        strokeDasharray={dashed ? '3 2' : undefined}
        opacity={0.85}
      />
    );
  };

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

        {/* MA5 / BOLL overlay */}
        {overlay === 'boll' ? (
          <>
            {makeOverlayPolyline(ma20Preview, '#a78bfa', true)}
            {makeOverlayPolyline(bollUpperPreview, '#ec4899', true)}
            {makeOverlayPolyline(bollLowerPreview, '#ec4899', true)}
          </>
        ) : sma5.length > 1 ? (
          <polyline
            points={sma5.map(p => `${p.x},${p.y}`).join(' ')}
            fill="none" stroke="#f59e0b" strokeWidth={1.2}
            strokeDasharray="3 2" opacity={0.8}
          />
        ) : null}

        {/* Candles */}
        {validData.map((d, i) => {
          const x = pads.l + i * xStep;
          const closeP = d.price!;
          const hasOhlc = hasValidOhlc(d);
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
          const label = formatChartDateLabel(d.date);
          return (
            <text key={`xl-${i}`} x={x} y={height - 3} textAnchor="middle" fill="#6b7280"
              fontSize={9} fontFamily="monospace">{label}</text>
          );
        })}
        {/* Tooltip — SVG-native, perfectly aligned */}
        {hoverIdx != null && validData[hoverIdx] && (() => {
          const d = validData[hoverIdx];
          const hasOhlc = hasValidOhlc(d);
          const isUp = hasOhlc ? d.price! >= d.open! : true;
          const chgColor = isUp ? '#ef4444' : '#10b981';
          const chg = hasOhlc && d.open! > 0 ? ((d.price! - d.open!) / d.open! * 100) : null;
          const cx = pads.l + hoverIdx * xStep;
          const tipW = 105, tipH = hasOhlc ? 52 : 30;
          const tipX = cx + tipW + 6 > chartW - pads.r ? cx - tipW - 6 : cx + 6;
          const tipY = pads.t;
          const dateStr = formatChartDateTooltip(d.date);
          return (
            <g pointerEvents="none">
              <rect x={tipX} y={tipY} width={tipW} height={tipH} rx={4}
                fill="var(--chart-tooltip-bg, #1e293b)" stroke="var(--chart-tooltip-border, #334155)" strokeWidth={0.8}
                filter="url(#tipShadow)" />
              <text x={tipX + 4} y={tipY + 12} fill="var(--chart-tooltip-label, #94a3b8)" fontSize={9} fontFamily="monospace">{dateStr}</text>
              {hasOhlc ? (
                <>
                  <text x={tipX + 4} y={tipY + 25} fill="var(--chart-tooltip-label, #94a3b8)" fontSize={9} fontFamily="monospace">
                    O {d.open!.toFixed(2)}  </text>
                  <text x={tipX + 60} y={tipY + 25} fill={chgColor} fontSize={9} fontFamily="monospace">
                    C {d.price!.toFixed(2)}</text>
                  <text x={tipX + 4} y={tipY + 38} fill="var(--chart-tooltip-label, #94a3b8)" fontSize={9} fontFamily="monospace">
                    H {d.high!.toFixed(2)}  </text>
                  <text x={tipX + 60} y={tipY + 38} fill="var(--chart-tooltip-label, #94a3b8)" fontSize={9} fontFamily="monospace">
                    L {d.low!.toFixed(2)}</text>
                  {chg != null && (
                    <text x={tipX + 4} y={tipY + 50} fill={chgColor} fontSize={9} fontFamily="monospace"
                      fontWeight="bold">
                      {chg >= 0 ? '+' : ''}{chg.toFixed(2)}%
                    </text>
                  )}
                </>
              ) : (
                <text x={tipX + 4} y={tipY + 22} fill="var(--chart-tooltip-value, #e2e8f0)" fontSize={9} fontFamily="monospace">
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

  if (windowBars != null) {
    return (
      <div ref={scrollRef} className="w-full max-w-full min-w-0 overflow-x-auto">
        {svgEl}
      </div>
    );
  }

  return (
    <div className="w-full overflow-x-auto">
      {svgEl}
    </div>
  );
};

export { CandlestickMiniChart, computeMA, computeStd };
