import type { HkStockListItem } from '../api/hkMonitor';

const normalizeTradeDate = (value?: string | null): string | null => {
  const normalized = value?.replaceAll('-', '').slice(0, 8) ?? '';
  return /^\d{8}$/.test(normalized) ? normalized : null;
};

export const filterRecentDrawdowns = (
  drawdownItems: HkStockListItem[],
  recentTradeDates: readonly string[] = [],
): HkStockListItem[] => {
  const recentDateSet = new Set(
    recentTradeDates
      .map(normalizeTradeDate)
      .filter((value): value is string => value != null),
  );
  const candidates = drawdownItems.filter(item => (
    (item.latest_consecutive_drawdown_days ?? 0) >= 2
    && item.latest_consecutive_drawdown_pct != null
  ));
  const recentCandidates = recentDateSet.size
    ? candidates.filter(item => recentDateSet.has(normalizeTradeDate(item.latest_consecutive_drawdown_end_date) ?? ''))
    : candidates;
  return [...recentCandidates]
    .sort((a, b) => (a.latest_consecutive_drawdown_pct ?? 0) - (b.latest_consecutive_drawdown_pct ?? 0))
    .slice(0, 8);
};

export const filterRecentGains = (
  gainItems: HkStockListItem[],
  recentTradeDates: readonly string[] = [],
): HkStockListItem[] => {
  const recentDateSet = new Set(
    recentTradeDates
      .map(normalizeTradeDate)
      .filter((value): value is string => value != null),
  );
  const candidates = gainItems.filter(item => (
    (item.latest_consecutive_gain_days ?? 0) >= 2
    && item.latest_consecutive_gain_pct != null
  ));
  const recentCandidates = recentDateSet.size
    ? candidates.filter(item => recentDateSet.has(normalizeTradeDate(item.latest_consecutive_gain_end_date) ?? ''))
    : candidates;
  return [...recentCandidates]
    .sort((a, b) => (b.latest_consecutive_gain_pct ?? 0) - (a.latest_consecutive_gain_pct ?? 0))
    .slice(0, 8);
};
