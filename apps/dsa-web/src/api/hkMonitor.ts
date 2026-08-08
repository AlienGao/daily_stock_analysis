import apiClient from './index';

export type HkGgtComponentItem = {
  trade_date: string;
  hk_code: string;
  name?: string | null;
  pinyin_full?: string | null;
  pinyin_abbr?: string | null;
  latest_price?: number | null;
  pct_change?: number | null;
  change_amount?: number | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  prev_close?: number | null;
  volume?: number | null;
  amount?: number | null;
};

export type HkGgtComponentListResponse = {
  trade_date: string;
  total: number;
  items: HkGgtComponentItem[];
  available_dates: string[];
};

export type HkGgtMinuteBarItem = {
  hk_code: string;
  trade_date: string;
  bar_time: string;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close?: number | null;
  volume?: number | null;
  amount?: number | null;
  avg_price?: number | null;
  period: string;
  source: string;
};

export type HkGgtMinuteBarListResponse = {
  hk_code: string;
  trade_date: string;
  total: number;
  items: HkGgtMinuteBarItem[];
};

export type HkStockListItem = {
  hk_code: string;
  name?: string | null;
  latest_price?: number | null;
  pct_change?: number | null;
  boll_mid?: number | null;
  boll_upper?: number | null;
  boll_lower?: number | null;
  boll_mid_dist_pct?: number | null;
  boll_upper_dist_pct?: number | null;
  boll_lower_dist_pct?: number | null;
  high_n_price?: number | null;
  drawdown_pct?: number | null;
  latest_consecutive_drawdown_pct?: number | null;
  latest_consecutive_drawdown_days?: number | null;
  latest_consecutive_drawdown_start_date?: string | null;
  latest_consecutive_drawdown_end_date?: string | null;
};

export type HkStockListResponse = {
  trade_date: string;
  total: number;
  items: HkStockListItem[];
};

export type HkStockKLineItem = {
  date: string;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close: number;
  volume?: number | null;
  boll_mid?: number | null;
  boll_upper?: number | null;
  boll_lower?: number | null;
};

export type HkStockKLineResponse = {
  hk_code: string;
  start_date: string;
  end_date: string;
  data: HkStockKLineItem[];
};

export type HkBollPickItem = {
  hk_code: string;
  name: string;
  close: number;
  band: string;
  boll_mid: number;
  boll_upper: number;
  boll_lower: number;
  dist_pct?: number | null;
};

export type HkBollPickListResponse = {
  near_pct: number;
  upper: HkBollPickItem[];
  mid: HkBollPickItem[];
  lower: HkBollPickItem[];
};

export const hkStockApi = {
  async list(opts?: { refresh?: boolean }): Promise<HkStockListResponse> {
    const resp = await apiClient.get<HkStockListResponse>('/api/v1/market/hk-stocks', {
      params: { refresh: opts?.refresh ?? false },
      timeout: 120_000,
    });
    return resp.data;
  },

  async getBollPicks(nearPct: number = 1.5): Promise<HkBollPickListResponse> {
    const resp = await apiClient.get<HkBollPickListResponse>('/api/v1/market/hk-stocks/boll-picks', {
      params: { near_pct: nearPct },
      timeout: 300_000,
    });
    return resp.data;
  },

  async getKlines(hkCode: string, opts?: { startDate?: string; endDate?: string }): Promise<HkStockKLineResponse> {
    const resp = await apiClient.get<HkStockKLineResponse>(`/api/v1/market/hk-stocks/${hkCode}/klines`, {
      params: { start_date: opts?.startDate, end_date: opts?.endDate },
      timeout: 120_000,
    });
    return resp.data;
  },
};

export const hkMonitorApi = {
  async getComponents(opts?: { tradeDate?: string; refresh?: boolean }): Promise<HkGgtComponentListResponse> {
    const response = await apiClient.get<HkGgtComponentListResponse>('/api/v1/market/hk-ggt/components', {
      params: {
        trade_date: opts?.tradeDate,
        refresh: opts?.refresh ?? false,
      },
      timeout: 120_000,
    });
    return response.data;
  },

  async getMinutes(hkCode: string, tradeDate?: string): Promise<HkGgtMinuteBarListResponse> {
    const response = await apiClient.get<HkGgtMinuteBarListResponse>(`/api/v1/market/hk-ggt/${hkCode}/minutes`, {
      params: { trade_date: tradeDate },
      timeout: 120_000,
    });
    return response.data;
  },
};
