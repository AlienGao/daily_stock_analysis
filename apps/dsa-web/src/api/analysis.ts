import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  AnalysisRequest,
  AnalysisResult,
  AnalyzeResponse,
  AnalyzeAsyncResponse,
  AnalysisReport,
  TaskStatus,
  TaskListResponse,
} from '../types/analysis';

// ============ API Interfaces ============

export const analysisApi = {
  /**
   * Trigger stock analysis.
   * @param data Analysis request payload
   * @returns Sync mode returns AnalysisResult; async mode returns accepted task payloads
   */
  analyze: async (data: AnalysisRequest): Promise<AnalyzeResponse> => {
    const requestData = {
      stock_code: data.stockCode,
      stock_codes: data.stockCodes,
      report_type: data.reportType || 'detailed',
      force_refresh: data.forceRefresh || false,
      async_mode: data.asyncMode || false,
      stock_name: data.stockName,
      original_query: data.originalQuery,
      selection_source: data.selectionSource,
      ...(data.notify !== undefined && { notify: data.notify }),
    };

    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/analysis/analyze',
      requestData
    );

    const result = toCamelCase<AnalyzeResponse>(response.data);

    // Ensure the sync analysis report payload is converted recursively.
    if ('report' in result && result.report) {
      result.report = toCamelCase<AnalysisReport>(result.report);
    }

    return result;
  },

  /**
   * Trigger analysis in async mode.
   * @param data Analysis request payload
   * @returns Accepted task payloads; throws DuplicateTaskError on 409
   */
  analyzeAsync: async (data: AnalysisRequest): Promise<AnalyzeAsyncResponse> => {
    const requestData = {
      stock_code: data.stockCode,
      stock_codes: data.stockCodes,
      report_type: data.reportType || 'detailed',
      force_refresh: data.forceRefresh || false,
      async_mode: true,
      stock_name: data.stockName,
      original_query: data.originalQuery,
      selection_source: data.selectionSource,
      ...(data.notify !== undefined && { notify: data.notify }),
    };

    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/analysis/analyze',
      requestData,
      {
        // Allow 202 accepted responses in addition to standard success codes.
        validateStatus: (status) => status === 200 || status === 202 || status === 409,
      }
    );

    // Handle duplicate submission compatibility.
    if (response.status === 409) {
      const errorData = toCamelCase<{
        error: string;
        message: string;
        stockCode: string;
        existingTaskId: string;
      }>(response.data);
      throw new DuplicateTaskError(errorData.stockCode, errorData.existingTaskId, errorData.message);
    }

    return toCamelCase<AnalyzeAsyncResponse>(response.data);
  },

  /**
   * Get async task status.
   * @param taskId Task ID
   */
  getStatus: async (taskId: string): Promise<TaskStatus> => {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/analysis/status/${taskId}`
    );

    const data = toCamelCase<TaskStatus>(response.data);

    // Ensure nested result payloads are converted recursively.
    if (data.result) {
      data.result = toCamelCase<AnalysisResult>(data.result);
      if (data.result.report) {
        data.result.report = toCamelCase<AnalysisReport>(data.result.report);
      }
    }

    return data;
  },

  /**
   * Get task list.
   * @param params Filter parameters
   */
  getTasks: async (params?: {
    status?: string;
    limit?: number;
  }): Promise<TaskListResponse> => {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/analysis/tasks',
      { params }
    );

    const data = toCamelCase<TaskListResponse>(response.data);

    return data;
  },

  /**
   * Get the SSE stream URL.
   */
  getTaskStreamUrl: (): string => {
    // Read API base URL from the shared client.
    const baseUrl = apiClient.defaults.baseURL || '';
    return `${baseUrl}/api/v1/analysis/tasks/stream`;
  },

  /**
   * Manually trigger the full STOCK_LIST analysis (equivalent to the
   * scheduled task). Returns the initial job status (HTTP 202).
   * 409 is surfaced as FullAnalysisBusyError.
   */
  runFullAnalysis: async (
    options?: RunFullAnalysisOptions,
  ): Promise<RunFullAnalysisStatus> => {
    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/analysis/run-full',
      {
        no_notify: options?.noNotify ?? false,
        no_market_review: options?.noMarketReview ?? false,
        force_run: options?.forceRun ?? false,
      },
      {
        validateStatus: (status) => status === 200 || status === 202 || status === 409,
      },
    );

    if (response.status === 409) {
      const errorData = toCamelCase<{ error?: string; message?: string; startedAt?: string }>(
        (response.data as { detail?: Record<string, unknown> })?.detail ?? response.data,
      );
      throw new FullAnalysisBusyError(errorData.message || '已有全量分析任务在运行', errorData.startedAt);
    }

    return toCamelCase<RunFullAnalysisStatus>(response.data);
  },

  /**
   * Poll the current status of the manual full-analysis job.
   */
  getFullAnalysisStatus: async (): Promise<RunFullAnalysisStatus> => {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/analysis/run-full/status',
    );
    return toCamelCase<RunFullAnalysisStatus>(response.data);
  },
};

// ============ Full-analysis types ============

export interface RunFullAnalysisOptions {
  /** 不发送通知（仅生成本地报告），默认 false。 */
  noNotify?: boolean;
  /** 跳过大盘复盘，默认 false。 */
  noMarketReview?: boolean;
  /** 忽略交易日检查，默认 false。 */
  forceRun?: boolean;
}

export interface RunFullAnalysisStatus {
  status: 'idle' | 'running' | 'completed' | 'failed';
  startedAt?: string | null;
  completedAt?: string | null;
  stockCount: number;
  message?: string | null;
  error?: string | null;
}

// ============ Custom Error Classes ============

/**
 * Duplicate task error.
 */
export class DuplicateTaskError extends Error {
  stockCode: string;
  existingTaskId: string;

  constructor(stockCode: string, existingTaskId: string, message?: string) {
    super(message || `股票 ${stockCode} 正在分析中`);
    this.name = 'DuplicateTaskError';
    this.stockCode = stockCode;
    this.existingTaskId = existingTaskId;
  }
}

/**
 * Raised when a full-analysis job is already running.
 */
export class FullAnalysisBusyError extends Error {
  startedAt?: string;

  constructor(message: string, startedAt?: string) {
    super(message);
    this.name = 'FullAnalysisBusyError';
    this.startedAt = startedAt;
  }
}
