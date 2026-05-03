import apiClient from './index';

export type BrokerRecommendItem = {
  ts_code: string;
  name: string;
  broker: string;
  broker_count: number;
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
  daily_return?: number;
  cumulative?: number;
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
  daily_returns: BrokerDailyReturn[];
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
