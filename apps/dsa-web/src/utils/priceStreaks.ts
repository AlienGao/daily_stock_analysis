/** 连续涨跌段字段：港股列表与后复权新高列表共用同一后端口径。 */
export type ConsecutiveDrawdownFields = {
  latest_consecutive_drawdown_pct?: number | null;
  latest_consecutive_drawdown_days?: number | null;
  latest_consecutive_drawdown_end_date?: string | null;
};

export type ConsecutiveGainFields = {
  latest_consecutive_gain_pct?: number | null;
  latest_consecutive_gain_days?: number | null;
  latest_consecutive_gain_end_date?: string | null;
};

const normalizeTradeDate = (value?: string | null): string | null => {
  const normalized = value?.replaceAll('-', '').slice(0, 8) ?? '';
  return /^\d{8}$/.test(normalized) ? normalized : null;
};

const buildRecentDateSet = (recentTradeDates: readonly string[] = []) => new Set(
  recentTradeDates
    .map(normalizeTradeDate)
    .filter((value): value is string => value != null),
);

export const filterRecentDrawdowns = <T extends ConsecutiveDrawdownFields>(
  drawdownItems: T[],
  recentTradeDates: readonly string[] = [],
  limit = 15,
): T[] => {
  const recentDateSet = buildRecentDateSet(recentTradeDates);
  const candidates = drawdownItems.filter(item => (
    (item.latest_consecutive_drawdown_days ?? 0) >= 2
    && item.latest_consecutive_drawdown_pct != null
  ));
  const recentCandidates = recentDateSet.size
    ? candidates.filter(item => recentDateSet.has(normalizeTradeDate(item.latest_consecutive_drawdown_end_date) ?? ''))
    : candidates;
  return [...recentCandidates]
    .sort((a, b) => (a.latest_consecutive_drawdown_pct ?? 0) - (b.latest_consecutive_drawdown_pct ?? 0))
    .slice(0, limit);
};

export const filterRecentGains = <T extends ConsecutiveGainFields>(
  gainItems: T[],
  recentTradeDates: readonly string[] = [],
  limit = 12,
): T[] => {
  const recentDateSet = buildRecentDateSet(recentTradeDates);
  const candidates = gainItems.filter(item => (
    (item.latest_consecutive_gain_days ?? 0) >= 2
    && item.latest_consecutive_gain_pct != null
  ));
  const recentCandidates = recentDateSet.size
    ? candidates.filter(item => recentDateSet.has(normalizeTradeDate(item.latest_consecutive_gain_end_date) ?? ''))
    : candidates;
  return [...recentCandidates]
    .sort((a, b) => (b.latest_consecutive_gain_pct ?? 0) - (a.latest_consecutive_gain_pct ?? 0))
    .slice(0, limit);
};
