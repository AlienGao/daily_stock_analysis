import type { HkBollPickItem, HkStockListItem, HkStockRealtimeItem } from '../api/hkMonitor';

const distancePct = (price?: number | null, band?: number | null) => {
  if (price == null || band == null || !Number.isFinite(price) || !Number.isFinite(band) || band <= 0) return null;
  return Number((((price - band) / band) * 100).toFixed(2));
};

export const mergeHkRealtimeItems = (
  items: readonly HkStockListItem[],
  realtimeItems: readonly HkStockRealtimeItem[],
): HkStockListItem[] => {
  const realtimeByCode = new Map(realtimeItems.map(item => [item.hk_code, item]));
  return items.map(item => {
    const realtime = realtimeByCode.get(item.hk_code);
    const latestPrice = realtime?.latest_price;
    if (latestPrice == null || !Number.isFinite(latestPrice)) return item;
    const highPrice = item.high_n_price != null ? Math.max(item.high_n_price, latestPrice) : null;
    return {
      ...item,
      latest_price: latestPrice,
      pct_change: realtime?.pct_change ?? item.pct_change ?? null,
      boll_mid_dist_pct: distancePct(latestPrice, item.boll_mid) ?? item.boll_mid_dist_pct ?? null,
      boll_upper_dist_pct: distancePct(latestPrice, item.boll_upper) ?? item.boll_upper_dist_pct ?? null,
      boll_lower_dist_pct: distancePct(latestPrice, item.boll_lower) ?? item.boll_lower_dist_pct ?? null,
      high_n_price: highPrice,
      drawdown_pct: highPrice != null && highPrice > 0
        ? Number((((latestPrice - highPrice) / highPrice) * 100).toFixed(2))
        : item.drawdown_pct ?? null,
    };
  });
};

export const mergeHkRealtimeBollPicks = (
  picks: readonly HkBollPickItem[],
  realtimeItems: readonly HkStockRealtimeItem[],
): HkBollPickItem[] => {
  const realtimeByCode = new Map(realtimeItems.map(item => [item.hk_code, item]));
  return picks.map(pick => {
    const latestPrice = realtimeByCode.get(pick.hk_code)?.latest_price;
    if (latestPrice == null || !Number.isFinite(latestPrice)) return pick;
    const bandPrice = pick.band === 'upper'
      ? pick.boll_upper
      : pick.band === 'lower'
        ? pick.boll_lower
        : pick.boll_mid;
    return {
      ...pick,
      close: latestPrice,
      dist_pct: distancePct(latestPrice, bandPrice),
    };
  });
};
