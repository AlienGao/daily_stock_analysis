import apiClient from './index';

export type DiscoveryItem = {
  rank: number;
  ts_code?: string;
  stock_code: string;
  stock_name: string;
  score: number;
  sector?: string;
  factor_scores?: Record<string, number>;
  reasons?: string[];
  buy_price_low?: number | null;
  buy_price_high?: number | null;
  stop_loss?: number | null;
  take_profit_1?: number | null;
  take_profit_2?: number | null;
  change?: string;
};

export type IntradayTopResponse = {
  updated?: string;
  round: number;
  mode: 'intraday';
  top_n: DiscoveryItem[];
  dropped: DiscoveryItem[];
};

export type PostmarketReportResponse = {
  date: string;
  report: string;
  exists: boolean;
  top_n?: DiscoveryItem[];
};

export type PostmarketRunResponse = {
  mode: 'postmarket';
  top_n?: DiscoveryItem[];
  report_preview?: string;
  message?: string;
};

const INTRADAY_MIN_REQUEST_GAP_MS = 60_000;
let intradayInFlight: Promise<IntradayTopResponse> | null = null;
let intradayLastFetchedAt = 0;
let intradayLastData: IntradayTopResponse | null = null;

export const discoveryApi = {
  async getIntradayTop10(options?: { force?: boolean }): Promise<IntradayTopResponse> {
    const force = options?.force === true;
    const now = Date.now();

    if (!force) {
      if (intradayInFlight) return intradayInFlight;
      if (intradayLastData && now - intradayLastFetchedAt < INTRADAY_MIN_REQUEST_GAP_MS) {
        return intradayLastData;
      }
    }

    intradayInFlight = (async () => {
      const resp = await apiClient.get('/api/v1/discovery/intraday/top10');
      const data = resp.data as IntradayTopResponse;
      intradayLastData = data;
      intradayLastFetchedAt = Date.now();
      return data;
    })();

    try {
      return await intradayInFlight;
    } finally {
      intradayInFlight = null;
    }
  },

  async getPostmarketReport(date?: string): Promise<PostmarketReportResponse> {
    const params = date ? { report_date: date } : {};
    const resp = await apiClient.get('/api/v1/discovery/postmarket/report', { params });
    return resp.data as PostmarketReportResponse;
  },

  async runPostmarketDiscovery(): Promise<PostmarketRunResponse> {
    const resp = await apiClient.post('/api/v1/discovery/postmarket/run');
    return resp.data as PostmarketRunResponse;
  },
};
