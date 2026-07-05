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
  boll_upper?: number;
  dist_mid_pct: number;
  dist_lower_pct: number;
  dist_upper_pct?: number;
  band_zone: string;
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

// ── ETF 新高 ──

export type EtfNewHighDateItem = {
  date: string;
  close: number;
};

export type EtfNewHighItem = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close: number;
  new_high_count: number;
  current_close?: number | null;
  drawdown_from_high_pct?: number | null;
  ytd_return_pct?: number | null;
  new_high_dates: EtfNewHighDateItem[];
};

export type EtfNewHighListResponse = {
  start_date: string;
  as_of_date: string;
  total: number;
  items: EtfNewHighItem[];
};

export type EtfBollPickItem = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close?: number | null;
  current_close: number;
  drawdown_from_high_pct?: number | null;
  boll_mid: number;
  boll_lower: number;
  boll_upper?: number;
  dist_mid_pct: number;
  dist_lower_pct: number;
  dist_upper_pct?: number;
  band_zone: string;
  mid_slope?: number | null;
};

export type EtfBollPickListResponse = {
  start_date: string;
  as_of_date: string;
  lookback_days: number;
  near_pct: number;
  max_drawdown_from_high_pct: number;
  cutoff_date: string;
  total: number;
  items: EtfBollPickItem[];
};

export type EtfKLineResponse = {
  stock_code: string;
  start_date: string;
  end_date: string;
  data: HfqKLineItem[];
};

// ── 全球主要指数 ──

export type GlobalIndexNewHighDateItem = {
  date: string;
  close: number;
};

export type GlobalIndexNewHighItem = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close: number;
  new_high_count: number;
  current_close?: number | null;
  drawdown_from_high_pct?: number | null;
  ytd_return_pct?: number | null;
  new_high_dates: GlobalIndexNewHighDateItem[];
};

export type GlobalIndexNewHighListResponse = {
  start_date: string;
  as_of_date: string;
  total: number;
  items: GlobalIndexNewHighItem[];
};

export type GlobalIndexKLineResponse = {
  ts_code: string;
  start_date: string;
  end_date: string;
  data: HfqKLineItem[];
};

export type GlobalIndexBollPickListResponse = {
  start_date: string;
  as_of_date: string;
  lookback_days: number;
  near_pct: number;
  max_drawdown_from_high_pct: number;
  cutoff_date: string;
  total: number;
  items: Array<{
    ts_code: string;
    stock_code: string;
    stock_name: string;
    latest_new_high_date: string;
    latest_new_high_close: number | null;
    current_close: number;
    drawdown_from_high_pct: number | null;
    boll_mid: number;
    boll_lower: number;
    boll_upper: number | null;
    dist_mid_pct: number;
    dist_lower_pct: number;
    dist_upper_pct: number | null;
    band_zone: string;
  }>;
};

// ── A 股指数 ──

export type AIndexNewHighDateItem = {
  date: string;
  close: number;
};

export type AIndexNewHighItem = {
  ts_code: string;
  stock_code: string;
  stock_name: string;
  latest_new_high_date: string;
  latest_new_high_close: number;
  new_high_count: number;
  current_close?: number | null;
  drawdown_from_high_pct?: number | null;
  ytd_return_pct?: number | null;
  new_high_dates: AIndexNewHighDateItem[];
};

export type AIndexNewHighListResponse = {
  start_date: string;
  as_of_date: string;
  total: number;
  items: AIndexNewHighItem[];
};

export type AIndexKLineResponse = {
  ts_code: string;
  start_date: string;
  end_date: string;
  freq: string;
  data: HfqKLineItem[];
};

export type AIndexBollPickListResponse = {
  start_date: string;
  as_of_date: string;
  lookback_days: number;
  near_pct: number;
  max_drawdown_from_high_pct: number;
  cutoff_date: string;
  total: number;
  items: Array<{
    ts_code: string;
    stock_code: string;
    stock_name: string;
    latest_new_high_date: string;
    latest_new_high_close: number | null;
    current_close: number;
    drawdown_from_high_pct: number | null;
    boll_mid: number;
    boll_lower: number;
    boll_upper: number | null;
    dist_mid_pct: number;
    dist_lower_pct: number;
    dist_upper_pct: number | null;
    band_zone: string;
  }>;
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

  // ── ETF 新高 ──

  async getEtfNewHighs(opts?: { startDate?: string; asOfDate?: string; refresh?: boolean }): Promise<EtfNewHighListResponse> {
    const response = await apiClient.get<EtfNewHighListResponse>('/api/v1/market/etf-new-highs', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getEtfBollPicks(opts?: {
    startDate?: string;
    asOfDate?: string;
    refresh?: boolean;
    nearPct?: number;
    lookbackDays?: number;
    maxDrawdownFromHighPct?: number;
  }): Promise<EtfBollPickListResponse> {
    const response = await apiClient.get<EtfBollPickListResponse>('/api/v1/market/etf-new-highs/boll-picks', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
        near_pct: opts?.nearPct ?? 2,
        lookback_days: opts?.lookbackDays ?? 30,
        max_drawdown_from_high_pct: opts?.maxDrawdownFromHighPct ?? 30,
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getEtfKlines(etfCode: string, startDate = '20260101', endDate?: string): Promise<EtfKLineResponse> {
    const response = await apiClient.get<EtfKLineResponse>(`/api/v1/market/etf-new-highs/${encodeURIComponent(etfCode)}/klines`, {
      params: {
        start_date: startDate,
        end_date: endDate,
      },
      timeout: 30000,
    });
    return response.data;
  },

  // ── 全球主要指数 ──

  async getGlobalIndexNewHighs(opts?: { startDate?: string; asOfDate?: string; refresh?: boolean }): Promise<GlobalIndexNewHighListResponse> {
    const response = await apiClient.get<GlobalIndexNewHighListResponse>('/api/v1/market/global-index-new-highs', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getGlobalIndexBollPicks(opts?: { startDate?: string; asOfDate?: string; refresh?: boolean }): Promise<GlobalIndexBollPickListResponse> {
    const response = await apiClient.get<GlobalIndexBollPickListResponse>('/api/v1/market/global-index-new-highs/boll-picks', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getGlobalIndexKlines(tsCode: string, startDate = '20260101', endDate?: string): Promise<GlobalIndexKLineResponse> {
    const response = await apiClient.get<GlobalIndexKLineResponse>(`/api/v1/market/global-index-new-highs/${encodeURIComponent(tsCode)}/klines`, {
      params: {
        start_date: startDate,
        end_date: endDate,
      },
      timeout: 30000,
    });
    return response.data;
  },

  // ── A 股指数（日线/周线）──

  async getAIndexList(): Promise<{ total: number; items: Array<{ ts_code: string; name: string }> }> {
    const response = await apiClient.get('/api/v1/market/a-index-list', { timeout: 30000 });
    return response.data;
  },

  async getAIndexNewHighs(opts?: { startDate?: string; asOfDate?: string; refresh?: boolean; freq?: string }): Promise<AIndexNewHighListResponse> {
    const response = await apiClient.get<AIndexNewHighListResponse>('/api/v1/market/a-index-new-highs', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
        freq: opts?.freq ?? 'daily',
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getAIndexBollPicks(opts?: { startDate?: string; asOfDate?: string; refresh?: boolean; nearPct?: number; lookbackDays?: number; maxDrawdownFromHighPct?: number; freq?: string }): Promise<AIndexBollPickListResponse> {
    const response = await apiClient.get<AIndexBollPickListResponse>('/api/v1/market/a-index-new-highs/boll-picks', {
      params: {
        start_date: opts?.startDate ?? '20260101',
        as_of_date: opts?.asOfDate,
        refresh: opts?.refresh ?? false,
        near_pct: opts?.nearPct ?? 2,
        lookback_days: opts?.lookbackDays ?? 30,
        max_drawdown_from_high_pct: opts?.maxDrawdownFromHighPct ?? 30,
        freq: opts?.freq ?? 'daily',
      },
      timeout: 300000,
    });
    return response.data;
  },

  async getAIndexKlines(tsCode: string, startDate = '20260101', endDate?: string, freq = 'daily'): Promise<AIndexKLineResponse> {
    const response = await apiClient.get<AIndexKLineResponse>(`/api/v1/market/a-index-new-highs/${encodeURIComponent(tsCode)}/klines`, {
      params: { start_date: startDate, end_date: endDate, freq },
      timeout: 30000,
    });
    return response.data;
  },

  async getAIndexConstituents(indexCode: string): Promise<{ index_code: string; total: number; items: Array<{ con_code: string; con_name: string | null; weight: number | null }> }> {
    const response = await apiClient.get(`/api/v1/market/a-index-constituents/${encodeURIComponent(indexCode)}`, {
      timeout: 30000,
    });
    return response.data;
  },
};
