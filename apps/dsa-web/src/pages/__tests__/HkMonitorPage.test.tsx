import { describe, expect, it } from 'vitest';
import type { HkStockListItem } from '../../api/hkMonitor';
import { calcBollBandWidthPct, compareBollBandWidth } from '../../utils/hkBollBandwidth';

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
