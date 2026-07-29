import type { HkStockListItem } from '../api/hkMonitor';

export const calcBollBandWidthPct = (item: HkStockListItem): number | null => {
  const { boll_upper: upper, boll_mid: mid, boll_lower: lower } = item;
  if (
    upper == null || mid == null || lower == null ||
    !Number.isFinite(upper) || !Number.isFinite(mid) || !Number.isFinite(lower) ||
    mid <= 0 || upper < lower
  ) return null;
  return Number((((upper - lower) / mid) * 100).toFixed(2));
};

export const compareBollBandWidth = (
  a: HkStockListItem,
  b: HkStockListItem,
  sortOrder?: 'ascend' | 'descend' | null,
) => {
  const aWidth = calcBollBandWidthPct(a);
  const bWidth = calcBollBandWidthPct(b);
  if (aWidth == null) return bWidth == null ? 0 : sortOrder === 'descend' ? -1 : 1;
  if (bWidth == null) return sortOrder === 'descend' ? 1 : -1;
  return aWidth - bWidth;
};
