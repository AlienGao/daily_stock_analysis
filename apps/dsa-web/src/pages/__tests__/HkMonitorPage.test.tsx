import { describe, expect, it } from 'vitest';
import type { HkStockListItem } from '../../api/hkMonitor';
import { calcBollBandWidthPct, compareBollBandWidth } from '../../utils/hkBollBandwidth';
import { sortHkItemsByPctChangeDesc } from '../../utils/hkMonitorSort';

const stock = (
  hkCode: string,
  upper: number | null,
  mid: number | null,
  lower: number | null,
): HkStockListItem => ({
  hk_code: hkCode,
  boll_upper: upper,
  boll_mid: mid,
  boll_lower: lower,
});

describe('港股通 BOLL 轨道宽度列', () => {
  it('按相对中轨的上下轨宽度计算百分比', () => {
    expect(calcBollBandWidthPct(stock('00700', 12, 10, 8))).toBe(40);
  });

  it('排序时将缺失轨道数据固定放在末尾', () => {
    const narrow = stock('00001', 11, 10, 9);
    const wide = stock('00002', 14, 10, 6);
    const missing = stock('00003', null, 10, 8);

    expect(compareBollBandWidth(narrow, wide, 'ascend')).toBeLessThan(0);
    expect(compareBollBandWidth(missing, narrow, 'ascend')).toBeGreaterThan(0);
    expect(compareBollBandWidth(missing, narrow, 'descend')).toBeLessThan(0);
  });
});

describe('港股通刷新排序', () => {
  it('按最新涨跌幅降序排列，并将缺失数据放到末尾', () => {
    const refreshed = [
      { hk_code: '00700', pct_change: -0.57 },
      { hk_code: '00941', pct_change: null },
      { hk_code: '03690', pct_change: 3.91 },
      { hk_code: '01810', pct_change: 0.25 },
    ] satisfies HkStockListItem[];

    expect(sortHkItemsByPctChangeDesc(refreshed).map(item => item.hk_code)).toEqual([
      '03690',
      '01810',
      '00700',
      '00941',
    ]);
  });
});
