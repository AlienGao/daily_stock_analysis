import apiClient from './index';

/* ── Types ── */

export type LGBTrainRequest = {
  mode: 'intraday' | 'postmarket';
  forward_days: number;
  start_date?: string | null;
  end_date?: string | null;
  n_estimators: number;
  num_leaves: number;
  learning_rate: number;
  cv_folds: number;
};

export type LGBTaskStatusResponse = {
  task_id: string;
  status: string;
  status_message?: string;
  error?: string;
  result?: {
    model_path: string;
    feature_importance: { gain: Record<string, number>; split: Record<string, number> };
    predictions: LGBPredictionItem[];
    training_metrics: Record<string, number>;
  };
};

export type LGBFeatureImportanceResponse = {
  gain: Record<string, number>;
  split: Record<string, number>;
};

export type LGBPredictionItem = {
  rank: number;
  ts_code: string;
  stock_code: string;
  lgb_score: number;
  raw_score: number;
};

export type LGBPredictionsResponse = {
  model_date: string;
  forward_days: number;
  mode: string;
  predictions: LGBPredictionItem[];
};

export type LGBBacktestCompareResponse = {
  lgb_metrics: Record<string, number>;
  factor_metrics: Record<string, number>;
  capital_curve: Array<{
    date: string;
    lgb: number;
    benchmark: number;
  }>;
  comparison: Record<string, number>;
};

export type LGBModelInfo = {
  name: string;
  path: string;
  size_kb: number;
  saved_at: string;
};

export type LGBModelListResponse = {
  models: LGBModelInfo[];
};

export type LGBDateRangeResponse = {
  intraday: { min: string; max: string } | null;
  postmarket: { min: string; max: string } | null;
};

export type LGBStockLookupItem = {
  stock_code: string;
  ts_code: string;
  rank: number;
  lgb_score: number;
  raw_score: number;
  total_stocks: number;
};

export type LGBStockLookupResponse = {
  found: boolean;
  item: LGBStockLookupItem | null;
  message: string;
};

/* ── API ── */

export const researchApi = {
  async train(params: LGBTrainRequest): Promise<{ task_id: string; status: string }> {
    const resp = await apiClient.post('/api/v1/research/lgb/train', params);
    return resp.data;
  },

  async getStatus(taskId: string): Promise<LGBTaskStatusResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/status', {
      params: { task_id: taskId },
    });
    return resp.data;
  },

  async getFeatureImportance(modelPath?: string): Promise<LGBFeatureImportanceResponse> {
    const params = modelPath ? { model_path: modelPath } : {};
    const resp = await apiClient.get('/api/v1/research/lgb/feature-importance', { params });
    return resp.data;
  },

  async getPredictions(modelPath?: string): Promise<LGBPredictionsResponse> {
    const params = modelPath ? { model_path: modelPath } : {};
    const resp = await apiClient.get('/api/v1/research/lgb/predictions', { params });
    return resp.data;
  },

  async getBacktestCompare(params: {
    mode?: string;
    top_n?: number;
    forward_days?: number;
    start_date?: string;
    end_date?: string;
    model_path?: string;
  } = {}): Promise<LGBBacktestCompareResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/backtest-compare', { params });
    return resp.data;
  },

  async listModels(): Promise<LGBModelListResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/models');
    return resp.data;
  },

  async getDateRange(): Promise<LGBDateRangeResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/date-range');
    return resp.data;
  },

  async lookupStock(stockCode: string, modelPath?: string): Promise<LGBStockLookupResponse> {
    const params: Record<string, string> = { stock_code: stockCode };
    if (modelPath) params.model_path = modelPath;
    const resp = await apiClient.get('/api/v1/research/lgb/stock-lookup', { params });
    return resp.data;
  },
};
