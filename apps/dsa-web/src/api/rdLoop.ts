import apiClient from './index';

export type PendingFactor = {
  name: string;
  path: string;
  hypothesis: string;
  score: number;
  cum_return: number;
  sharpe: number;
  win_rate: number;
  code: string;
};

export type PendingListResponse = {
  total: number;
  items: PendingFactor[];
};

export type ActionResponse = {
  ok: boolean;
  message: string;
};

export const rdLoopApi = {
  async listPending(): Promise<PendingListResponse> {
    const resp = await apiClient.get('/api/v1/rd-loop/pending');
    return resp.data as PendingListResponse;
  },

  async approve(factorName: string): Promise<ActionResponse> {
    const resp = await apiClient.post('/api/v1/rd-loop/approve', { factor_name: factorName });
    return resp.data as ActionResponse;
  },

  async reject(factorName: string): Promise<ActionResponse> {
    const resp = await apiClient.post('/api/v1/rd-loop/reject', { factor_name: factorName });
    return resp.data as ActionResponse;
  },
};
