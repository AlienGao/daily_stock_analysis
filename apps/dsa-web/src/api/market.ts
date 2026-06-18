import apiClient from './index';

export type NewHighDateItem = {
  date: string;
  hfq_close: number;
};

export type HfqNewHighItem = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close: number;
  new_high_count: number;
  current_hfq_close?: number | null;
  drawdown_from_high_pct?: number | null;
  ytd_hfq_return_pct?: number | null;
  new_high_dates: NewHighDateItem[];
};

export type HfqNewHighListResponse = {
  start_date: string;
  as_of_date: string;
  total: number;
  items: HfqNewHighItem[];
};

export type HfqBollPickItem = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close?: number | null;
  current_hfq_close: number;
  drawdown_from_high_pct?: number | null;
  boll_mid: number;
  boll_lower: number;
  dist_mid_pct: number;
  dist_lower_pct: number;
  band_zone: 'mid' | 'lower' | 'both';
};

export type HfqBollPickListResponse = {
  start_date: string;
  as_of_date: string;
  lookback_days: number;
  near_pct: number;
  max_drawdown_from_high_pct: number;
  cutoff_date: string;
  total: number;
  items: HfqBollPickItem[];
};

export type HfqKLineItem = {
  date: string;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close: number;
  volume?: number | null;
};

export type HfqKLineResponse = {
  stock_code: string;
  start_date: string;
  end_date: string;
  data: HfqKLineItem[];
};

export const marketApi = {
  async getHfqNewHighs(opts?: { startDate?: string; asOfDate?: string; refresh?: boolean }): Promise<HfqNewHighListResponse> {
    const response = await apiClient.get<HfqNewHighListResponse>('/api/v1/market/hfq-new-highs', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getHfqBollPicks(opts?: {
    startDate?: string;
    asOfDate?: string;
    refresh?: boolean;
    nearPct?: number;
    lookbackDays?: number;
    maxDrawdownFromHighPct?: number;
  }): Promise<HfqBollPickListResponse> {
    const response = await apiClient.get<HfqBollPickListResponse>('/api/v1/market/hfq-new-highs/boll-picks', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
        near_pct: opts?.nearPct ?? 2,
        lookback_days: opts?.lookbackDays ?? 30,
        max_drawdown_from_high_pct: opts?.maxDrawdownFromHighPct ?? 20,
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getHfqKlines(stockCode: string, startDate = '20260101', endDate?: string): Promise<HfqKLineResponse> {
    const response = await apiClient.get<HfqKLineResponse>(`/api/v1/market/hfq-new-highs/${encodeURIComponent(stockCode)}/klines`, {
      params: {
        start_date: startDate,
        end_date: endDate,
      },
      timeout: 30000,
    });
    return response.data;
  },
};
