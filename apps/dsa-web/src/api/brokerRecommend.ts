import apiClient from './index';

export type BrokerRecommendItem = {
  ts_code: string;
  name: string;
  broker: string;
  broker_count: number;
};

export type StockEnrichment = {
  nineturn?: {
    up_count?: number | null;
    down_count?: number | null;
    nine_up_turn?: number | null;
    nine_down_turn?: number | null;
  } | null;
  forecast?: {
    eps?: number | null;
    pe?: number | null;
    roe?: number | null;
    np?: number | null;
    rating?: string | null;
    min_price?: number | null;
    max_price?: number | null;
    imp_dg?: string | null;
  } | null;
  cyq_perf?: {
    cost_avg?: number | null;
    winner_rate?: number | null;
    concentration?: number | null;
    scr90?: number | null;
  } | null;
  sector?: string | null;
};

export type EnrichmentResponse = {
  month: string;
  query_date: string;
  data: Record<string, StockEnrichment>;
};

export type BrokerRecommendResponse = {
  month: string;
  total_recommendations: number;
  unique_stocks: number;
  unique_brokers: number;
  items: BrokerRecommendItem[];
};

export type BrokerFetchResponse = {
  month: string;
  saved_count: number;
};

export type BrokerDailyReturn = {
  date: string;
  price?: number;
  daily_return?: number;
  cumulative?: number;
  open?: number | null;
  high?: number | null;
  low?: number | null;
};

export type BrokerBacktestItem = {
  broker: string;
  stock_count: number;
  cumulative_return: number;
  win_rate: number;
  avg_return: number;
  daily_returns: BrokerDailyReturn[];
  stocks: Array<{ ts_code: string; name: string }>;
};

export type StockReturnItem = {
  ts_code: string;
  name: string;
  broker_count: number;
  broker: string;
  end_price?: number;
  end_date?: string;
  daily_change?: number | null;
  daily_returns: BrokerDailyReturn[];
  nineturn?: {
    up_count?: number | null;
    down_count?: number | null;
    nine_up_turn?: number | null;
    nine_down_turn?: number | null;
  } | null;
  forecast?: {
    eps?: number | null;
    pe?: number | null;
    roe?: number | null;
    np?: number | null;
    rating?: string | null;
    min_price?: number | null;
    max_price?: number | null;
    imp_dg?: string | null;
  } | null;
  cyq_perf?: {
    cost_avg?: number | null;
    winner_rate?: number | null;
    concentration?: number | null;
    scr90?: number | null;
  } | null;
};

export type BrokerBacktestResponse = {
  month: string;
  next_month: string;
  buy_date: string;
  sell_date: string;
  total_recommendations: number;
  unique_stocks: number;
  unique_brokers: number;
  brokers: BrokerBacktestItem[];
  stock_returns: StockReturnItem[];
};

export type YtdMonthlyReturn = {
  month: string;
  cumulative_return: number;
  stock_count: number;
  win_rate: number;
};

export type YtdBrokerItem = {
  broker: string;
  cumulative_return: number;
  active_months: number;
  daily_returns: BrokerDailyReturn[];
  monthly_returns: YtdMonthlyReturn[];
};

export type YtdBacktestResponse = {
  year: string;
  start_date: string;
  end_date: string;
  total_brokers: number;
  brokers: YtdBrokerItem[];
};

export type ConsecutiveStockItem = {
  ts_code: string;
  name: string;
  broker_count_current: number;
  broker_count_prev: number;
  brokers_current: string[];
  brokers_prev: string[];
};

export async function getAvailableMonths(): Promise<string[]> {
  const resp = await apiClient.get<string[]>('/api/v1/broker-recommend/months');
  return resp.data;
}

export async function getMonthlyRecommendations(
  month: string
): Promise<BrokerRecommendResponse> {
  const resp = await apiClient.get<BrokerRecommendResponse>(
    `/api/v1/broker-recommend/${month}`
  );
  return resp.data;
}

export async function fetchMonth(
  month: string
): Promise<BrokerFetchResponse> {
  const resp = await apiClient.post<BrokerFetchResponse>(
    `/api/v1/broker-recommend/${month}/fetch`
  );
  return resp.data;
}

export async function getBacktest(
  month: string,
  topN: number = 10
): Promise<BrokerBacktestResponse> {
  const resp = await apiClient.get<BrokerBacktestResponse>(
    `/api/v1/broker-recommend/${month}/backtest`,
    { params: { top_n: topN } }
  );
  return resp.data;
}

export async function getMonthlyEnrichment(
  month: string
): Promise<EnrichmentResponse> {
  const resp = await apiClient.get<EnrichmentResponse>(
    `/api/v1/broker-recommend/${month}/enrichment`
  );
  return resp.data;
}

export async function getYtdBacktest(
  topN: number = 5
): Promise<YtdBacktestResponse> {
  const resp = await apiClient.get<YtdBacktestResponse>(
    '/api/v1/broker-recommend/ytd',
    { params: { top_n: topN } }
  );
  return resp.data;
}

export async function getConsecutiveStocks(
  month: string
): Promise<ConsecutiveStockItem[]> {
  const resp = await apiClient.get<ConsecutiveStockItem[]>(
    `/api/v1/broker-recommend/${month}/consecutive`
  );
  return resp.data;
}

// ---- 机构调研 Top 10 ----

export type SurveyDetail = {
  surv_date: string;
  rece_org: string;
  org_type: string;
  rece_mode: string;
  weight: number;
  fund_visitors: string;
  rece_place: string;
  comp_rece: string;
};

export type InstitutionSurveyItem = {
  ts_code: string;
  name: string;
  weighted_score: number;
  visit_count: number;
  last_surv_date: string;
  top_orgs: string[];
  details: SurveyDetail[];
};

export type InstitutionSurveyResponse = {
  date: string;
  start_date: string;
  end_date: string;
  total_stocks: number;
  items: InstitutionSurveyItem[];
};

export async function getTopBrokers(topN: number = 5): Promise<string[]> {
  const resp = await apiClient.get<string[]>(
    '/api/v1/broker-recommend/top-brokers',
    { params: { top_n: topN } }
  );
  return resp.data;
}

export async function getInstitutionSurvey(params?: {
  start_date?: string;
  end_date?: string;
}): Promise<InstitutionSurveyResponse> {
  const resp = await apiClient.get<InstitutionSurveyResponse>(
    '/api/v1/broker-recommend/institution-survey',
    { params }
  );
  return resp.data;
}

export async function getInstitutionSurveyDates(): Promise<string[]> {
  const resp = await apiClient.get<string[]>(
    '/api/v1/broker-recommend/institution-survey/dates'
  );
  return resp.data;
}

export type StockHistoryEntry = {
  month: string;
  brokers: string[];
  broker_count: number;
  buy_date: string;
  sell_date: string;
  cumulative_return?: number | null;
  daily_returns: BrokerDailyReturn[];
};

export type StockHistoryResponse = {
  ts_code: string;
  name: string;
  entries: StockHistoryEntry[];
};

export async function getStockHistory(tsCode: string): Promise<StockHistoryResponse> {
  const resp = await apiClient.get<StockHistoryResponse>(
    `/api/v1/broker-recommend/stock/${encodeURIComponent(tsCode)}/history`
  );
  return resp.data;
}

export type HistoricalMonthCountItem = {
  ts_code: string;
  month_count: number;
};

export async function getHistoricalMonthCounts(
  codes: string[],
): Promise<HistoricalMonthCountItem[]> {
  if (!codes.length) return [];
  const resp = await apiClient.get<HistoricalMonthCountItem[]>(
    '/api/v1/broker-recommend/historical-month-counts',
    { params: { codes: codes.join(',') } },
  );
  return resp.data;
}

export type HistoricalRecommendStatsItem = {
  ts_code: string;
  month_count: number;
  period_count: number;
  win_rate?: number | null;
  max_return?: number | null;
  max_drawdown?: number | null;
};

export async function getHistoricalRecommendStats(
  codes: string[],
): Promise<HistoricalRecommendStatsItem[]> {
  if (!codes.length) return [];
  const resp = await apiClient.get<HistoricalRecommendStatsItem[]>(
    '/api/v1/broker-recommend/historical-recommend-stats',
    { params: { codes: codes.join(',') }, timeout: 120_000 },
  );
  return resp.data;
}

