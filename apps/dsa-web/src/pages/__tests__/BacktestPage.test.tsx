import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import BacktestPage from '../BacktestPage';

const {
  mockGetResults,
  mockGetOverallPerformance,
  mockGetStockPerformance,
  mockRun,
} = vi.hoisted(() => ({
  mockGetResults: vi.fn(),
  mockGetOverallPerformance: vi.fn(),
  mockGetStockPerformance: vi.fn(),
  mockRun: vi.fn(),
}));

vi.mock('../../api/backtest', () => ({
  backtestApi: {
    getResults: mockGetResults,
    getOverallPerformance: mockGetOverallPerformance,
    getStockPerformance: mockGetStockPerformance,
    run: mockRun,
  },
}));

const basePerformance = {
  scope: 'overall',
  evalWindowDays: 10,
  engineVersion: 'test-engine',
  totalEvaluations: 3,
  completedCount: 2,
  insufficientCount: 1,
  longCount: 2,
  cashCount: 1,
  winCount: 1,
  lossCount: 1,
  neutralCount: 0,
  directionAccuracyPct: 66.7,
  winRatePct: 50,
  neutralRatePct: 0,
  avgStockReturnPct: 2.4,
  avgSimulatedReturnPct: 1.2,
  stopLossTriggerRate: 10,
  takeProfitTriggerRate: 20,
  ambiguousRate: 0,
  avgDaysToFirstHit: 3.5,
  adviceBreakdown: {},
  diagnostics: {},
};

beforeEach(() => {
  vi.clearAllMocks();
  mockGetOverallPerformance.mockResolvedValue(basePerformance);
  mockGetStockPerformance.mockResolvedValue(null);
  mockGetResults.mockResolvedValue({
    total: 1,
    page: 1,
    limit: 20,
    items: [
      {
        analysisHistoryId: 101,
        code: '600519',
        stockName: '贵州茅台',
        analysisDate: '2026-03-20',
        evalWindowDays: 10,
        engineVersion: 'test-engine',
        evalStatus: 'completed',
        operationAdvice: '继续持有',
        trendPrediction: '震荡偏多',
        sentimentScore: 68,
        actualMovement: 'up',
        actualReturnPct: 3.8,
        directionExpected: 'long',
        directionCorrect: true,
        outcome: 'win',
        simulatedReturnPct: 3.8,
      },
    ],
  });
  mockRun.mockResolvedValue({
    processed: 1,
    saved: 1,
    completed: 1,
    insufficient: 0,
    errors: 0,
  });
});

describe('BacktestPage', () => {
  it('默认渲染分数列与排序按钮', async () => {
    render(<BacktestPage />);

    await screen.findByPlaceholderText('按股票代码筛选（留空=全部）');
    expect(screen.getByRole('button', { name: '按分数排序' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '按分析日期排序' })).toBeInTheDocument();
    expect(screen.getByText('分数')).toBeInTheDocument();
  });

  it('默认查询带上后端排序参数', async () => {
    render(<BacktestPage />);

    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith({
        code: undefined,
        triggerSource: undefined,
        evalWindowDays: 10,
        analysisDateFrom: undefined,
        analysisDateTo: undefined,
        sortBy: 'analysis_date',
        sortOrder: 'desc',
        page: 1,
        limit: 20,
      });
    });
  });

  it('点击分数排序按钮会切换后端全量排序参数', async () => {
    render(<BacktestPage />);

    await screen.findByText('600519');
    const sortBtn = screen.getByRole('button', { name: '按分数排序' });
    fireEvent.click(sortBtn);
    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith(expect.objectContaining({
        sortBy: 'sentiment_score',
        sortOrder: 'desc',
      }));
    });

    const sortBtnAfterFirstToggle = await screen.findByRole('button', { name: '按分数排序' });
    fireEvent.click(sortBtnAfterFirstToggle);
    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith(expect.objectContaining({
        sortBy: 'sentiment_score',
        sortOrder: 'asc',
      }));
    });
  });

  it('点击分析日期排序按钮会切换后端全量排序参数', async () => {
    render(<BacktestPage />);

    const dateSortBtn = await screen.findByRole('button', { name: '按分析日期排序' });

    fireEvent.click(dateSortBtn);
    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith(expect.objectContaining({
        sortBy: 'analysis_date',
        sortOrder: 'asc',
      }));
    });
  });

  it('点击实际表现列头会切换后端排序参数', async () => {
    render(<BacktestPage />);

    await screen.findByPlaceholderText('按股票代码筛选（留空=全部）');
    const actualSortBtn = screen.getByRole('button', { name: '按实际表现排序' });

    fireEvent.click(actualSortBtn); // actual_desc
    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith(expect.objectContaining({
        sortBy: 'actual_return_pct',
        sortOrder: 'desc',
      }));
    });
    const actualSortBtnAfterFirstToggle = await screen.findByRole('button', { name: '按实际表现排序' });

    fireEvent.click(actualSortBtnAfterFirstToggle); // actual_asc
    await waitFor(() => {
      expect(mockGetResults).toHaveBeenLastCalledWith(expect.objectContaining({
        sortBy: 'actual_return_pct',
        sortOrder: 'asc',
      }));
    });
  });
});
