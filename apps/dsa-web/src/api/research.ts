import apiClient from './index';

/* ── Types ── */

export type LGBTrainRequest = {
  mode: 'intraday' | 'postmarket';
  forward_days: number;
  exec_mode?: string;
  label_mode?: 'fixed' | 'peak_speed';
  window_days?: number;
  peak_min_return?: number;
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
    model_date?: string;
    feature_importance: { gain: Record<string, number>; split: Record<string, number> };
    predictions: LGBPredictionItem[];
    training_metrics: LGBTrainingMetrics;
    tree_diagnostics?: LGBTreeDiagnostics;
    prediction_stats?: LGBPredictionStats | null;
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
  stock_name: string;
  lgb_score: number;
  raw_score: number;
  predicted_days?: number | null;
  win_rate: number | null;
  avg_return: number | null;
  max_return: number | null;
  max_loss: number | null;
  profit_loss_ratio: number | null;
  hit_count: number | null;
  score_percentile: number | null;
  finbert_label?: string | null;
  finbert_score?: number | null;
  finbert_summary?: string | null;
  news_items?: Array<{
    title: string;
    snippet: string;
    source: string;
    url: string;
    date: string;
    sentiment_label?: string;
    sentiment_score?: number;
  }>;
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
  stock_name?: string;
  rank: number;
  lgb_score: number;
  raw_score: number;
  total_stocks: number;
  finbert_sentiment?: {
    overall_score?: number;
    overall_label?: string;
    positive_count?: number;
    negative_count?: number;
    neutral_count?: number;
    summary?: string;
    news_items?: Array<{
      title: string;
      snippet: string;
      source: string;
      url: string;
      date: string;
      sentiment_label?: string;
      sentiment_score?: number;
    }>;
  } | null;
};

export type LGBStockLookupResponse = {
  found: boolean;
  item: LGBStockLookupItem | null;
  message: string;
};

export type LGBBacktestTradeItem = {
  pred_date: string;
  stock_code: string;
  ts_code: string;
  stock_name: string;
  rank: number;
  buy_date: string;
  buy_price: number;
  sell_date: string;
  sell_price: number;
  return_pct: number;
  skipped: boolean;
  expected_sell_date?: string;
  target_return?: number;
  shares: number;
  actual_cost: number;
};

export type LGBBacktestSimMetrics = {
  cumulative_return: number;
  win_rate: number;
  max_drawdown: number;
  total_trades: number;
  skipped_trades: number;
  holding_trades: number;
};

export type LGBBacktestSimResponse = {
  forward_days: number;
  top_n: number;
  exec_mode: string;
  metrics: LGBBacktestSimMetrics;
  capital_curve: Array<{ date: string; capital: number; daily_return?: number }>;
  trades: LGBBacktestTradeItem[];
};

export type LGBBacktestSimAvailableResponse = {
  open: number[];
  close: number[];
  has_peak: boolean;
};

export type LGBBruteForceItem = {
  exec_mode: string;
  forward_days: number;
  top_n: number;
  stop_strategy: string;
  label_mode?: string;
  window_days?: number;
  cumulative_return: number;
  sharpe_ratio: number;
  win_rate: number;
  max_drawdown: number;
  total_trades: number;
  skipped_trades: number;
  error: string;
};

export type LGBBruteForceResult = {
  best_by_return: LGBBruteForceItem | null;
  best_by_sharpe: LGBBruteForceItem | null;
  top5_by_return: LGBBruteForceItem[];
  top5_by_sharpe: LGBBruteForceItem[];
  all_results: LGBBruteForceItem[];
  report_path: string;
};

export type LGBBruteForceTaskStatus = {
  task_id: string;
  status: string;
  progress_current: number;
  progress_total: number;
  status_message: string;
  result: LGBBruteForceResult | null;
  error: string;
};

export type LGBTrainingMetrics = {
  cv_rmse_mean: number;
  cv_rmse_std: number;
  n_samples: number;
  n_features: number;
  cv_scores: number[];
  rank_ic_mean: number | null;
  rank_ic_std: number | null;
  icir: number | null;
  oof_corr: number | null;
};

export type LGBTreeDiagnostics = {
  n_trees: number;
  avg_depth: number;
  avg_n_leaves: number;
  total_n_leaves: number;
};

export type LGBPredictionStats = {
  mean: number;
  std: number;
  skew: number;
  kurtosis: number;
  min: number;
  max: number;
  median: number;
};

export type LGBDiagnosticsResponse = {
  training_metrics: LGBTrainingMetrics;
  tree_diagnostics: LGBTreeDiagnostics;
  prediction_stats: LGBPredictionStats | null;
};

export type LGBCrossModelOverlapStock = {
  stock_code: string;
  ts_code: string;
  stock_name: string;
  count: number;
  model_names: string[];
};

export type LGBCrossModelOverlapResponse = {
  exec_mode: string;
  total_models: number;
  stocks: LGBCrossModelOverlapStock[];
};

export type LGBFactorSubsetResult = {
  all_factors: string[];
  final_subset: string[];
  excluded_factors: string[];
  baseline_ic: number;
  final_ic: number;
  final_icir: number;
  final_rmse: number;
  delta_ic: number;
  elapsed_seconds: number;
  report_path: string;
};

export type LGBFactorSubsetTaskStatus = {
  task_id: string;
  status: string;
  status_message: string;
  result: LGBFactorSubsetResult | null;
  error: string;
};

/* ── API ── */

export const researchApi = {
  async train(params: LGBTrainRequest): Promise<{ task_id: string; status: string }> {
    const resp = await apiClient.post('/api/v1/research/lgb/train', params, { timeout: 120000 });
    return resp.data;
  },

  async getStatus(taskId: string): Promise<LGBTaskStatusResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/status', {
      params: { task_id: taskId },
      timeout: 60000,
    });
    return resp.data;
  },

  async getFeatureImportance(modelPath?: string, signal?: AbortSignal): Promise<LGBFeatureImportanceResponse> {
    const params = modelPath ? { model_path: modelPath } : {};
    const resp = await apiClient.get('/api/v1/research/lgb/feature-importance', { params, timeout: 120000, signal });
    return resp.data;
  },

  async getPredictions(modelPath?: string, signal?: AbortSignal): Promise<LGBPredictionsResponse> {
    const params = modelPath ? { model_path: modelPath } : {};
    const resp = await apiClient.get('/api/v1/research/lgb/predictions', { params, timeout: 120000, signal });
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
    const resp = await apiClient.get('/api/v1/research/lgb/backtest-compare', { params, timeout: 300000 });
    return resp.data;
  },

  async listModels(labelMode?: string): Promise<LGBModelListResponse> {
    const params = labelMode ? { label_mode: labelMode } : {};
    const resp = await apiClient.get('/api/v1/research/lgb/models', { params });
    return resp.data;
  },

  async getDateRange(): Promise<LGBDateRangeResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/date-range');
    return resp.data;
  },

  async getFinbertForStock(stockCode: string, stockName?: string): Promise<{
    stock_code: string;
    finbert_label: string | null;
    finbert_score: number | null;
    finbert_summary: string | null;
    news_items: Array<{
      title: string;
      snippet: string;
      source: string;
      url: string;
      date: string;
      sentiment_label?: string;
      sentiment_score?: number;
    }> | null;
  }> {
    const params: Record<string, string> = { stock_code: stockCode };
    if (stockName) params.stock_name = stockName;
    const resp = await apiClient.get('/api/v1/research/lgb/finbert', { params, timeout: 120000 });
    return resp.data;
  },

  async lookupStock(stockCode: string, modelPath?: string): Promise<LGBStockLookupResponse> {
    const params: Record<string, string> = { stock_code: stockCode };
    if (modelPath) params.model_path = modelPath;
    const resp = await apiClient.get('/api/v1/research/lgb/stock-lookup', { params, timeout: 120000 });
    return resp.data;
  },

  async getBacktestSim(params: {
    forward_days: number;
    top_n?: number;
    exec_mode?: string;
    stop_strategy?: string;
  }, signal?: AbortSignal): Promise<LGBBacktestSimResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/backtest-sim', { params, timeout: 300000, signal });
    return resp.data;
  },

  async getBacktestSimAvailable(): Promise<LGBBacktestSimAvailableResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/backtest-sim/available');
    return resp.data;
  },

  async getBacktestSimPeak(params: {
    top_n?: number;
    exec_mode?: string;
    stop_loss?: number;
  }, signal?: AbortSignal): Promise<LGBBacktestSimResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/backtest-sim/peak', { params, timeout: 300000, signal });
    return resp.data;
  },

  async startBruteForce(): Promise<{ task_id: string; status: string }> {
    const resp = await apiClient.post('/api/v1/research/lgb/brute-force-search', {}, { timeout: 120000 });
    return resp.data;
  },

  async getLatestBruteForceReport(): Promise<LGBBruteForceResult> {
    const resp = await apiClient.get('/api/v1/research/lgb/brute-force-reports/latest', { timeout: 30000 });
    return resp.data;
  },

  async getBruteForceStatus(taskId: string): Promise<LGBBruteForceTaskStatus> {
    const resp = await apiClient.get('/api/v1/research/lgb/brute-force-search/status', {
      params: { task_id: taskId },
      timeout: 60000,
    });
    return resp.data;
  },

  async getDiagnostics(modelPath?: string): Promise<LGBDiagnosticsResponse> {
    const params = modelPath ? { model_path: modelPath } : {};
    const resp = await apiClient.get('/api/v1/research/lgb/diagnostics', { params, timeout: 120000 });
    return resp.data;
  },

  async getCrossModelOverlap(execMode: string, topN?: number): Promise<LGBCrossModelOverlapResponse> {
    const resp = await apiClient.get('/api/v1/research/lgb/cross-model-overlap', {
      params: { exec_mode: execMode, top_n: topN ?? 5 },
      timeout: 300000,
    });
    return resp.data;
  },

  async startCatchUp(): Promise<{ task_id: string; status: string }> {
    const resp = await apiClient.post('/api/v1/research/lgb/catch-up', {}, { timeout: 120000 });
    return resp.data;
  },

  async getCatchUpStatus(taskId: string): Promise<CatchUpTaskStatus> {
    const resp = await apiClient.get('/api/v1/research/lgb/catch-up/status', {
      params: { task_id: taskId },
      timeout: 60000,
    });
    return resp.data;
  },

  async startFactorSubsetSearch(params: {
    label_mode?: string;
    forward_days?: number;
    window_days?: number;
    exec_mode?: string;
    mode?: string;
    tpe_trials?: number;
  } = {}): Promise<{ task_id: string; status: string }> {
    const resp = await apiClient.post('/api/v1/research/lgb/factor-subset-search', null, {
      params,
      timeout: 120000,
    });
    return resp.data;
  },

  async getFactorSubsetSearchStatus(taskId: string): Promise<LGBFactorSubsetTaskStatus> {
    const resp = await apiClient.get('/api/v1/research/lgb/factor-subset-search/status', {
      params: { task_id: taskId },
      timeout: 60000,
    });
    return resp.data;
  },

  async applyFactorSubsetResult(): Promise<{ applied: boolean; existing_excluded?: string[]; new_excluded?: string[]; env_value?: string; message?: string }> {
    const resp = await apiClient.post('/api/v1/research/lgb/factor-subset-apply', null, { timeout: 30000 });
    return resp.data;
  },
};

export type CatchUpResultItem = {
  exec_mode: string;
  forward_days: number;
  label_mode?: string;
  window_days?: number;
  status: string;
  train_window?: string;
  pred_range?: string;
  ok?: number;
  fail?: number;
  latest_pred?: string;
  used_existing_model?: boolean;
  error?: string;
};

export type CatchUpTaskStatus = {
  task_id: string;
  status: string;
  progress_current: number;
  progress_total: number;
  status_message: string;
  result?: {
    combos: CatchUpResultItem[];
    latest_trading_day: string;
  };
  error: string;
};
