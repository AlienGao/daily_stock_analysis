import type { HkStockListItem } from '../api/hkMonitor';

export const sortHkItemsByPctChangeDesc = (items: HkStockListItem[]): HkStockListItem[] => (
  [...items].sort((a, b) => {
    const aPct = a.pct_change;
    const bPct = b.pct_change;
    if (aPct == null || Number.isNaN(aPct)) return bPct == null || Number.isNaN(bPct) ? 0 : 1;
    if (bPct == null || Number.isNaN(bPct)) return -1;
    return bPct - aPct;
  })
);
