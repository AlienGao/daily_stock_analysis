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
  discovered_at?: string;
  price_at_discovery?: number | null;
  live_price?: number | null;
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

export type RunTaskResponse = {
  task_id: string;
  status: string;
};

export type RunStatusResponse = {
  task_id: string;
  status: string;  // "running" | "completed" | "failed"
  error: string;
  top_n_count: number;
};

export type TradeRecordItem = {
  stock_code: string;
  stock_name: string;
  buy_date: string;
  buy_price: number;
  sell_date: string;
  sell_price: number;
  return_pct: number;
  pnl: number;
  allocated_capital: number;
};

export type BacktestDailyItem = {
  trade_date: string;
  avg_return: number;
  cumulative_return: number;
  capital: number;
  win_count: number;
  total_count: number;
};

export type CapitalCurvePoint = {
  date: string;
  capital: number;
};

export type BacktestResponse = {
  mode: string;
  initial_capital: number;
  final_capital: number;
  cumulative_return: number;
  total_pnl: number;
  win_rate: number;
  total_days: number;
  total_trades: number;
  daily_results: BacktestDailyItem[];
  trade_records: TradeRecordItem[];
  capital_curve: CapitalCurvePoint[];
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

  async runPostmarketDiscovery(): Promise<RunTaskResponse> {
    const resp = await apiClient.post('/api/v1/discovery/postmarket/run');
    return resp.data as RunTaskResponse;
  },

  async getPostmarketRunStatus(taskId: string): Promise<RunStatusResponse> {
    const resp = await apiClient.get('/api/v1/discovery/postmarket/run/status', {
      params: { task_id: taskId },
    });
    return resp.data as RunStatusResponse;
  },

  async getBacktest(
    mode: 'intraday' | 'postmarket',
    options?: { days?: number; start_date?: string; end_date?: string },
  ): Promise<BacktestResponse> {
    const resp = await apiClient.get('/api/v1/discovery/backtest', {
      params: { mode, days: options?.days ?? 60, ...options },
    });
    return resp.data as BacktestResponse;
  },
};
