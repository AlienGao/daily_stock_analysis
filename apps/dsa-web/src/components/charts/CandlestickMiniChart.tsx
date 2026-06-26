import type { EChartsOption } from 'echarts';
import type React from 'react';
import { useEffect, useLayoutEffect, useMemo, useRef } from 'react';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { CandlestickChart, LineChart } from 'echarts/charts';
import {
  GridComponent,
  DataZoomComponent,
  TooltipComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

// Register ECharts modules (tree-shakeable)
echarts.use([
  CanvasRenderer,
  CandlestickChart,
  LineChart,
  GridComponent,
  DataZoomComponent,
  TooltipComponent,
]);

type OhlcPoint = {
  date: string;
  price?: number | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
};

/** Normalize date string to YYYY-MM-DD or YYYY-MM-DD HH:mm for ECharts axis. */
const normalizeDate = (date: string): string => {
  const digits = date.replace(/\D/g, '');
  if (digits.length >= 12) {
    return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)} ${digits.slice(8, 10)}:${digits.slice(10, 12)}`;
  }
  if (digits.length >= 8) {
    return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`;
  }
  return date;
};

const UP_COLOR = '#ef4444';
const DOWN_COLOR = '#10b981';

const hasValidOhlc = (d: OhlcPoint): boolean =>
  d.open != null && d.open > 0 && d.high != null && d.low != null && d.high >= d.low && d.high > 0;

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
  longSeriesScroll?: boolean;
  barPitch?: number; // kept for API compat, ignored in echarts mode
  overlay?: 'ma5' | 'boll';
  windowBars?: number;
}> = ({ data, height = 160, overlay = 'ma5', windowBars = 0 }) => {
  const chartDomRef = useRef<HTMLDivElement>(null);
  const validData = data.filter(d => d.price != null);
  const count = validData.length;

  if (count < 1) return null;

  const dates = validData.map(d => normalizeDate(d.date));

  // OHLC data: [open, close, low, high]
  const ohlcData: number[][] = validData.map(d => [
    hasValidOhlc(d) ? d.open! : d.price!,
    d.price!,
    hasValidOhlc(d) ? d.low! : d.price!,
    hasValidOhlc(d) ? d.high! : d.price!,
  ]);

  const closes = validData.map(d => d.price!);

  const getOverlaySeries = (): EChartsOption['series'] => {
    if (overlay === 'boll') {
      const ma20: (number | null)[] = [];
      const upper: (number | null)[] = [];
      const lower: (number | null)[] = [];
      for (let i = 0; i < closes.length; i++) {
        if (i < 19) {
          ma20.push(null);
          upper.push(null);
          lower.push(null);
        } else {
          const slice = closes.slice(i - 19, i + 1);
          const mean = slice.reduce((a, b) => a + b, 0) / 20;
          const variance = slice.reduce((a, b) => a + (b - mean) ** 2, 0) / 20;
          const std = Math.sqrt(variance);
          ma20.push(mean);
          upper.push(mean + 2 * std);
          lower.push(mean - 2 * std);
        }
      }
      return [
        {
          name: 'BOLL Upper',
          type: 'line',
          data: upper,
          symbol: 'none',
          lineStyle: { width: 1, type: 'dashed', color: '#94a3b8' },
          z: 1,
        },
        {
          name: 'BOLL Mid',
          type: 'line',
          data: ma20,
          symbol: 'none',
          lineStyle: { width: 1, color: '#64748b' },
          z: 1,
        },
        {
          name: 'BOLL Lower',
          type: 'line',
          data: lower,
          symbol: 'none',
          lineStyle: { width: 1, type: 'dashed', color: '#94a3b8' },
          z: 1,
        },
      ];
    }

    // MA5
    const sma5: (number | null)[] = [];
    for (let i = 0; i < closes.length; i++) {
      if (i < 4) {
        sma5.push(null);
      } else {
        const slice = closes.slice(i - 4, i + 1);
        sma5.push(slice.reduce((a, b) => a + b, 0) / 5);
      }
    }
    return [
      {
        name: 'MA5',
        type: 'line',
        data: sma5,
        symbol: 'none',
        lineStyle: { width: 1.2, color: '#f59e0b' },
        z: 1,
      },
    ];
  };

  const showSlider = count >= 30 && windowBars === 0;
  const start = windowBars > 0 ? Math.max(0, 100 - (windowBars / count) * 100) : 60;
  const end = 100;

  const option: EChartsOption = useMemo(() => ({
    animation: false,
    backgroundColor: 'transparent',
    grid: { top: 14, right: 6, bottom: 20, left: 42 },
    xAxis: {
      type: 'category',
      data: dates,
      axisLine: { lineStyle: { color: '#1f2937' } },
      axisLabel: { color: '#6b7280', fontSize: 9, fontFamily: 'monospace' },
      splitLine: { show: false },
    },
    yAxis: {
      scale: true,
      splitLine: { lineStyle: { color: '#1f2937', width: 0.8 } },
      axisLabel: { color: '#6b7280', fontSize: 9, fontFamily: 'monospace' },
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: 'var(--chart-tooltip-bg, #1e293b)',
      borderColor: 'var(--chart-tooltip-border, #334155)',
      textStyle: { color: '#e2e8f0', fontSize: 11 },
      formatter: (params: any) => {
        if (!params || params.length === 0) return '';
        const candle = params.find((p: any) => p.seriesType === 'candlestick');
        if (!candle) return '';
        // ECharts candlestick data is [open, close, lowest, highest] - access by index
        const val = Array.isArray(candle.data) ? candle.data : (candle.data?.value ?? []);
        const open = val[1], close = val[2], low = val[3], high = val[4];
        if (open == null || close == null) return '';
        const isUp = close >= open;
        const chg = open > 0 ? ((close - open) / open * 100) : 0;
        const chgStr = `${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%`;
        return [
          `<div style="font-family:monospace;font-size:9px;color:#94a3b8">${candle.axisValue}&nbsp;&nbsp;<span style="font-weight:bold;color:${isUp ? '#ef4444' : '#10b981'};font-size:10px;margin-top:2px">${chgStr}</span></div>`,
          '<table style="width:100%;font-family:monospace;font-size:10px"><tr>',
          `<td style="padding-right:8px">开 <b>${Number(open).toFixed(2)}</b></td>`,
          `<td>收 <b style="color:${isUp ? '#ef4444' : '#10b981'}">${Number(close).toFixed(2)}</b></td>`,
          '</tr><tr>',
          `<td>高 <b>${Number(high).toFixed(2)}</b></td>`,
          `<td>低 <b>${Number(low).toFixed(2)}</b></td>`,
          '</tr></table>',
        ].join('');
      },
    },
    dataZoom: showSlider
      ? [
          {
            type: 'inside',
            start,
            end,
          },
          {
            type: 'slider',
            start,
            end,
            height: 12,
            bottom: 0,
            borderColor: '#334155',
            backgroundColor: '#1e293b',
            fillerColor: 'rgba(59,130,246,0.15)',
            handleStyle: { color: '#3b82f6' },
            textStyle: { color: '#94a3b8', fontSize: 9 },
          },
        ]
      : undefined,
    series: [
      {
        type: 'candlestick',
        data: ohlcData,
        itemStyle: {
          color: UP_COLOR,
          color0: DOWN_COLOR,
          borderColor: UP_COLOR,
          borderColor0: DOWN_COLOR,
        },

      },
      ...getOverlaySeries(),
    ],
  }), [dates, ohlcData, overlay, showSlider, start, end]);

  useLayoutEffect(() => {
    const dom = chartDomRef.current;
    if (!dom) return;
    const instance = echarts.getInstanceByDom(dom) || echarts.init(dom, undefined, { renderer: 'canvas' });
    instance.setOption(option, { notMerge: true, lazyUpdate: true });
    return () => { instance.dispose(); };
  }, [option]);

  useLayoutEffect(() => {
    const dom = chartDomRef.current;
    if (!dom) return;
    const instance = echarts.getInstanceByDom(dom);
    if (!instance) return;
    const ro = new ResizeObserver(() => { instance.resize(); });
    ro.observe(dom);
    return () => ro.disconnect();
  }, []);

  return (
    <div className="w-full min-w-0">
      <div ref={chartDomRef} style={{ width: '100%', height: `${height}px` }} />
    </div>
  );
};

export { CandlestickMiniChart, computeMA, computeStd };
