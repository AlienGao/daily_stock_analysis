import type React from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend,
  BarChart, Bar, CartesianGrid,
} from 'recharts';
import { DatePicker, Segmented, Table, InputNumber, Button, Select, Input } from 'antd';
import { Brain, Play, Loader2, Search } from 'lucide-react';
import { AppPage, Card, StatCard, EmptyState, ApiErrorAlert } from '../components/common';
import { researchApi, type LGBTaskStatusResponse, type LGBPredictionItem, type LGBBacktestCompareResponse, type LGBModelInfo, type LGBDateRangeResponse, type LGBStockLookupItem, type LGBBacktestSimResponse } from '../api/research';
import type { ParsedApiError } from '../api/error';
import { getParsedApiError } from '../api/error';
import dayjs from 'dayjs';

function pctNum(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

const LightGBMPage: React.FC = () => {
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const [forwardDays, setForwardDays] = useState(3);
  const [trainExecMode, setTrainExecMode] = useState('close');
  const [nEstimators, setNEstimators] = useState(200);
  const [numLeaves, setNumLeaves] = useState(31);
  const [learningRate, setLearningRate] = useState(0.05);
  const [cvFolds, setCvFolds] = useState(5);
  const [startDate, setStartDate] = useState<string | null>(null);
  const [endDate, setEndDate] = useState<string | null>(null);

  const [training, setTraining] = useState(false);
  const [statusMsg, setStatusMsg] = useState('');
  const [error, setError] = useState<ParsedApiError | null>(null);

  const [featureImportance, setFeatureImportance] = useState<{ name: string; gain: number; split: number }[]>([]);
  const [predictions, setPredictions] = useState<LGBPredictionItem[]>([]);
  const [backtest, setBacktest] = useState<LGBBacktestCompareResponse | null>(null);
  const [models, setModels] = useState<LGBModelInfo[]>([]);
  const [selectedModel, setSelectedModel] = useState<string | undefined>(undefined);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [dateRange, setDateRange] = useState<LGBDateRangeResponse | null>(null);
  const [stockCode, setStockCode] = useState('');
  const [stockLookup, setStockLookup] = useState<LGBStockLookupItem | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState('');
  const [backtestSim, setBacktestSim] = useState<LGBBacktestSimResponse | null>(null);
  const [backtestSimLoading, setBacktestSimLoading] = useState(false);
  const [backtestFwd, setBacktestFwd] = useState(1);
  const [backtestTopN, setBacktestTopN] = useState(1);

  /* ── Derived per-mode bounds ── */
  const dateBounds = dateRange?.postmarket;
  const dateMin = dateBounds ? dayjs(dateBounds.min) : null;
  const dateMax = dateBounds ? dayjs(dateBounds.max) : null;
  const disableDate = useCallback(
    (d: dayjs.Dayjs) => dateMin !== null && dateMax !== null && (d.isBefore(dateMin) || d.isAfter(dateMax)),
    [dateMin, dateMax],
  );

  /* ── Load date range ── */
  const fetchDateRange = useCallback(async () => {
    try {
      const dr = await researchApi.getDateRange();
      setDateRange(dr);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { fetchDateRange(); }, [fetchDateRange]);

  /* ── Load model list ── */
  const loadModels = useCallback(async () => {
    setModelsLoading(true);
    try {
      const data = await researchApi.listModels();
      setModels(data.models);
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setModelsLoading(false);
    }
  }, []);

  useEffect(() => { loadModels(); }, [loadModels]);

  const [predictLoading, setPredictLoading] = useState(false);
  const [predictionDate, setPredictionDate] = useState('');

  /* ── Predict Top 5 with selected model ── */
  const handlePredictTop5 = useCallback(async () => {
    if (!selectedModel) return;
    setError(null);
    setPredictLoading(true);
    try {
      const pred = await researchApi.getPredictions(selectedModel);
      setPredictions(pred.predictions);
      setPredictionDate(pred.model_date);
    } catch (e) {
      setError(getParsedApiError(e));
    } finally {
      setPredictLoading(false);
    }
  }, [selectedModel]);
  const loadModelResults = useCallback(async (modelPath: string) => {
    setError(null);
    setBacktest(null);
    // Fetch FI & predictions independently from backtest (backtest may fail on loaded model)
    try {
      const [fi, pred] = await Promise.all([
        researchApi.getFeatureImportance(modelPath),
        researchApi.getPredictions(modelPath),
      ]);
      const fiList = Object.keys(fi.gain).map((name) => ({
        name,
        gain: fi.gain[name],
        split: fi.split[name] ?? 0,
      })).sort((a, b) => b.gain - a.gain);
      setFeatureImportance(fiList);
      setPredictions(pred.predictions);
      setPredictionDate(pred.model_date);
    } catch (e) {
      setError(getParsedApiError(e));
    }
  }, []);

  /* ── Train ── */
  const handleTrain = useCallback(async () => {
    setError(null);
    setTraining(true);
    setStatusMsg('');
    setFeatureImportance([]);
    setPredictions([]);
    setBacktest(null);

    try {
      const { task_id } = await researchApi.train({
        mode: 'postmarket',
        forward_days: forwardDays,
        exec_mode: trainExecMode,
        start_date: startDate,
        end_date: endDate,
        n_estimators: nEstimators,
        num_leaves: numLeaves,
        learning_rate: learningRate,
        cv_folds: cvFolds,
      });
      const finalResult = await new Promise<LGBTaskStatusResponse>((resolve, reject) => {
        pollRef.current = setInterval(async () => {
          try {
            const status = await researchApi.getStatus(task_id);
            if (status.status_message) setStatusMsg(status.status_message);
            if (status.status === 'completed') {
              setStatusMsg('');
              clearInterval(pollRef.current!);
              pollRef.current = null;
              resolve(status);
            } else if (status.status === 'failed') {
              clearInterval(pollRef.current!);
              pollRef.current = null;
              reject(new Error(status.error || '训练失败'));
            }
          } catch (e) {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            reject(e);
          }
        }, 1000);
      });

      if (finalResult?.result) {
        const fi = finalResult.result.feature_importance;
        const fiList = Object.keys(fi.gain).map((name) => ({
          name,
          gain: fi.gain[name],
          split: fi.split[name] ?? 0,
        })).sort((a, b) => b.gain - a.gain);
        setFeatureImportance(fiList);
        setPredictions(finalResult.result.predictions);
        if (finalResult.result.model_date) setPredictionDate(finalResult.result.model_date);
        setTraining(false);
        setStatusMsg('');

        loadModels();
        researchApi.getBacktestCompare({
          mode: 'postmarket',
          top_n: 10,
          forward_days: forwardDays,
        }).then((bt) => setBacktest(bt)).catch(() => {});
      }
    } catch (e) {
      setError(getParsedApiError(e));
      setTraining(false);
      setStatusMsg('');
    }
  }, [forwardDays, trainExecMode, nEstimators, numLeaves, learningRate, cvFolds, startDate, endDate, loadModels]);

  /* ── Stock lookup ── */
  const handleLookup = useCallback(async () => {
    const code = stockCode.trim();
    if (!code) return;
    setLookupLoading(true);
    setLookupError('');
    setStockLookup(null);
    try {
      const resp = await researchApi.lookupStock(code, selectedModel);
      if (resp.found && resp.item) {
        setStockLookup(resp.item);
      } else {
        setLookupError(resp.message || '未找到该股票');
      }
    } catch (e) {
      setLookupError('查询失败，请确认模型已训练完成');
    } finally {
      setLookupLoading(false);
    }
  }, [stockCode, selectedModel]);

  /* ── Backtest Sim ── */
  const fetchBacktestSim = useCallback(async (fwd: number, exec: string, topN: number) => {
    setBacktestSimLoading(true);
    setBacktestSim(null);
    try {
      const data = await researchApi.getBacktestSim({ forward_days: fwd, top_n: topN, exec_mode: exec });
      setBacktestSim(data);
    } catch {
      // ignore
    } finally {
      setBacktestSimLoading(false);
    }
  }, []);

  useEffect(() => { fetchBacktestSim(backtestFwd, trainExecMode, backtestTopN); }, [fetchBacktestSim, trainExecMode, backtestTopN]);

  /* ── Cleanup polling on unmount ── */
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  /* ── Capital curve chart data ── */
  const capitalData = backtest?.capital_curve?.map((p) => ({
    date: p.date,
    lgb: p.lgb,
    benchmark: p.benchmark,
  })) ?? [];

  const predColumns = [
    { title: '排名', dataIndex: 'rank', key: 'rank', width: 60 },
    { title: '代码', dataIndex: 'ts_code', key: 'ts_code', width: 100 },
    { title: '名称', dataIndex: 'stock_name', key: 'stock_name', width: 100 },
    { title: 'LGB 评分', dataIndex: 'lgb_score', key: 'lgb_score', width: 100, render: (_: unknown, r: LGBPredictionItem) => r.lgb_score.toFixed(4) },
    { title: '原始分', dataIndex: 'raw_score', key: 'raw_score', width: 100, render: (_: unknown, r: LGBPredictionItem) => r.raw_score.toFixed(4) },
  ];

  return (
    <AppPage className="max-w-none px-2 md:px-3">
      <div className="flex flex-col lg:flex-row gap-5">
        {/* ──── Left Panel ──── */}
        <div className="lg:w-[260px] shrink-0 space-y-4">
          <Card>
            <div className="space-y-3">
              <div className="font-medium text-sm text-secondary-text">训练标签模式</div>
              <Segmented
                block
                value={trainExecMode}
                onChange={(v) => setTrainExecMode(v as string)}
                options={[
                  { label: '收盘→收盘', value: 'close' },
                  { label: '开盘→开盘', value: 'open' },
                ]}
              />
              <div className="text-[10px] text-tertiary-text">
                训练标签与回测执行模式对应，训练/回测一致才可对比
              </div>
            </div>
          </Card>

          <Card>
            <div className="space-y-4">
              <div className="font-medium text-sm text-secondary-text">训练参数</div>

              {/* 预测天数 */}
              <div className="space-y-1.5">
                <div className="text-xs text-secondary-text">预测天数 <span className="text-tertiary-text">（未来N日后涨跌幅，推荐3）</span></div>
                <InputNumber size="small" min={1} max={60} value={forwardDays} onChange={(v) => setForwardDays(v ?? 5)} className="w-full" />
              </div>

              {/* 树结构参数：一行两列 */}
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">树数量 <span className="text-tertiary-text">n_estimators，迭代轮数，推荐200</span></div>
                  <InputNumber size="small" min={10} max={1000} value={nEstimators} onChange={(v) => setNEstimators(v ?? 200)} className="w-full" />
                </div>
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">叶子数 <span className="text-tertiary-text">num_leaves，单树复杂度，推荐31</span></div>
                  <InputNumber size="small" min={2} max={255} value={numLeaves} onChange={(v) => setNumLeaves(v ?? 31)} className="w-full" />
                </div>
              </div>

              {/* 学习率 + CV：一行两列 */}
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">学习率 <span className="text-tertiary-text">lr，每轮步长，推荐0.05</span></div>
                  <InputNumber size="small" min={0.001} max={1} step={0.01} value={learningRate} onChange={(v) => setLearningRate(v ?? 0.05)} className="w-full" />
                </div>
                <div className="space-y-1.5">
                  <div className="text-xs text-secondary-text">交叉验证 <span className="text-tertiary-text">cv，时序切分评估泛化，推荐5</span></div>
                  <InputNumber size="small" min={2} max={20} value={cvFolds} onChange={(v) => setCvFolds(v ?? 5)} className="w-full" />
                </div>
              </div>

              {/* 日期范围 */}
              <div className="space-y-1.5">
                <div className="text-xs text-secondary-text">日期范围 <span className="text-tertiary-text">（可选）</span></div>
                {dateBounds && (
                  <>
                    <div className="text-[10px] text-tertiary-text/70">{dateBounds.min} ~ {dateBounds.max}</div>
                    <div className="flex flex-wrap gap-1">
                      {[{ label: '30日', days: 42 }, { label: '60日', days: 85 }, { label: '120日', days: 170 }, { label: '240日', days: 340 }].map(({ label, days }) => (
                        <Button
                          key={label}
                          size="small"
                          type="default"
                          className="text-[10px] h-5 px-1.5"
                          onClick={() => {
                            const end = dateMax ?? dayjs();
                            setStartDate(end.subtract(days, 'day').format('YYYYMMDD'));
                            setEndDate(end.format('YYYYMMDD'));
                          }}
                        >
                          近{label}
                        </Button>
                      ))}
                    </div>
                  </>
                )}
                <DatePicker
                  size="small"
                  placeholder="起始日"
                  disabledDate={disableDate}
                  value={startDate ? dayjs(startDate) : null}
                  onChange={(_, ds) => setStartDate(typeof ds === 'string' ? ds : null)}
                  className="w-full"
                />
                <div className="mt-2">
                  <DatePicker
                    size="small"
                    placeholder="结束日"
                    disabledDate={disableDate}
                    value={endDate ? dayjs(endDate) : null}
                    onChange={(_, ds) => setEndDate(typeof ds === 'string' ? ds : null)}
                    className="w-full"
                  />
                </div>
              </div>

              <Button
                block
                type="primary"
                icon={training ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                onClick={handleTrain}
                disabled={training}
                loading={training}
              >
                {training ? (statusMsg || '训练中...') : '开始训练'}
              </Button>
            </div>
          </Card>

          {models.length > 0 && (
            <Card>
              <div className="space-y-3">
                <div className="font-medium text-sm text-secondary-text">已有模型</div>
                <Select
                  size="small"
                  className="w-full"
                  placeholder="选择已保存的模型"
                  allowClear
                  loading={modelsLoading}
                  value={selectedModel}
                  onChange={(v) => {
                    setSelectedModel(v);
                    if (v) loadModelResults(v);
                  }}
                  options={models.map((m) => ({
                    label: `${m.name} (${new Date(m.saved_at).toLocaleDateString()})`,
                    value: m.path,
                  }))}
                />
                {selectedModel && (
                  <>
                    <div className="border-t border-white/5" />
                    <Button
                      block
                      size="small"
                      type="primary"
                      icon={predictLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Brain className="h-4 w-4" />}
                      onClick={handlePredictTop5}
                      loading={predictLoading}
                    >
                      预测当前 Top 5
                    </Button>
                  </>
                )}
              </div>
            </Card>
          )}
        </div>

        {/* ──── Right Panel ──── */}
        <div className="flex-1 min-w-0 space-y-4">
          {error && <ApiErrorAlert error={error} />}

          {!featureImportance.length && !predictions.length && !backtest && !training && (
            <EmptyState
              icon={<Brain className="h-12 w-12 text-tertiary-text" />}
              title="LightGBM 因子研究"
              description="配置左侧参数后点击「开始训练」，系统将使用因子快照数据训练 LightGBM 模型并展示分析结果。"
            />
          )}

          {/* Feature Importance */}
          {featureImportance.length > 0 && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">特征重要性 (Gain)</div>
              <ResponsiveContainer width="100%" height={Math.max(240, featureImportance.length * 28)}>
                <BarChart data={featureImportance} layout="vertical" margin={{ left: 80, right: 20, top: 5, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                  <XAxis type="number" tick={{ fontSize: 11 }} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 11 }} width={75} />
                  <Tooltip
                    contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }}
                    formatter={(value) => [Number(value).toFixed(4), 'Gain']}
                  />
                  <Bar dataKey="gain" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          )}

          {/* Stock Lookup */}
          {(predictions.length > 0 || selectedModel) && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">个股查询</div>
              <div className="flex gap-2">
                <Input
                  size="small"
                  placeholder="输入股票代码，如 600519"
                  value={stockCode}
                  onChange={(e) => setStockCode(e.target.value)}
                  onPressEnter={handleLookup}
                  style={{ maxWidth: 200 }}
                />
                <Button
                  size="small"
                  type="primary"
                  icon={lookupLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Search className="h-3 w-3" />}
                  onClick={handleLookup}
                  loading={lookupLoading}
                >
                  查询
                </Button>
              </div>
              {lookupError && <div className="text-red-400 text-xs mt-2">{lookupError}</div>}
              {stockLookup && (
                <div className="mt-3 p-3 rounded bg-blue-500/10 border border-blue-500/20">
                  <div className="grid grid-cols-3 md:grid-cols-6 gap-2 text-xs">
                    <div>
                      <span className="text-tertiary-text">代码</span>
                      <div className="font-medium text-sm">{stockLookup.ts_code}</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">LGB 评分</span>
                      <div className="font-medium text-sm text-blue-400">{stockLookup.lgb_score.toFixed(2)}</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">全市场排名</span>
                      <div className="font-medium text-sm">{stockLookup.rank} / {stockLookup.total_stocks}</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">百分位</span>
                      <div className="font-medium text-sm">Top {(stockLookup.rank / stockLookup.total_stocks * 100).toFixed(1)}%</div>
                    </div>
                    <div>
                      <span className="text-tertiary-text">原始分</span>
                      <div className="font-medium text-sm">{stockLookup.raw_score.toFixed(4)}</div>
                    </div>
                  </div>
                </div>
              )}
            </Card>
          )}

          {/* Predictions */}
          {predictions.length > 0 && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">预测结果 Top 5{predictionDate ? <span className="text-tertiary-text ml-2 text-xs">数据日期: {predictionDate}</span> : ''}</div>
              <Table
                size="small"
                dataSource={predictions}
                rowKey="ts_code"
                pagination={{ pageSize: 20, size: 'small' }}
                columns={predColumns}
                scroll={{ x: 400 }}
              />
            </Card>
          )}

          {/* Backtest Simulation (from prediction files) */}
          <Card>
            <div className="flex items-center justify-between mb-3">
              <div className="font-medium text-sm text-secondary-text">回测模拟（预测文件）</div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-tertiary-text">Top</span>
                <InputNumber
                  size="small"
                  min={1}
                  max={5}
                  value={backtestTopN}
                  onChange={(v) => {
                    const n = v ?? 1;
                    setBacktestTopN(n);
                    fetchBacktestSim(backtestFwd, trainExecMode, n);
                  }}
                  style={{ width: 52 }}
                />
                <Segmented
                  size="small"
                  value={backtestFwd.toString()}
                  onChange={(v) => {
                    const fwd = Number(v);
                    setBacktestFwd(fwd);
                    fetchBacktestSim(fwd, trainExecMode, backtestTopN);
                  }}
                  options={[
                    { label: '1 日', value: '1' },
                    { label: '3 日', value: '3' },
                  ]}
                />
              </div>
            </div>

            {backtestSimLoading && (
              <div className="flex items-center gap-2 text-sm text-tertiary-text py-4">
                <Loader2 className="h-4 w-4 animate-spin" />
                计算中...
              </div>
            )}

            {backtestSim && !backtestSimLoading && (
              <>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4">
                  <StatCard
                    label="总收益率"
                    value={pctNum(backtestSim.metrics.cumulative_return)}
                  />
                  <StatCard
                    label="胜率"
                    value={pctNum(backtestSim.metrics.win_rate)}
                  />
                  <StatCard
                    label="最大回撤"
                    value={pctNum(backtestSim.metrics.max_drawdown)}
                  />
                  <StatCard
                    label="交易笔数"
                    value={String(backtestSim.metrics.total_trades)}
                  />
                  <StatCard
                    label="跳过（涨停）"
                    value={String(backtestSim.metrics.skipped_trades)}
                  />
                </div>

                {backtestSim.capital_curve.length > 1 && (
                  <ResponsiveContainer width="100%" height={280} className="mb-4">
                    <LineChart data={backtestSim.capital_curve}>
                      <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                      <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
                      <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                      <Tooltip
                        contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }}
                        formatter={(value) => [Number(value).toFixed(4), '资金']}
                      />
                      <Line
                        type="monotone"
                        dataKey="capital"
                        name="资金曲线"
                        stroke="#22c55e"
                        strokeWidth={2}
                        dot={false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                )}

                {backtestSim.trades.length > 0 && (
                  <details open>
                    <summary className="cursor-pointer text-xs text-tertiary-text mb-2 select-none">
                      交易明细（{backtestSim.trades.filter((t) => !t.skipped).length} 笔，跳过 {backtestSim.metrics.skipped_trades} 笔涨停）
                    </summary>
                    <Table
                      size="small"
                      dataSource={backtestSim.trades.filter((t) => !t.skipped)}
                      rowKey={(r) => `${r.pred_date}_${r.stock_code}`}
                      pagination={{ pageSize: 50, size: 'small', showSizeChanger: true, pageSizeOptions: ['20', '50', '100'], showTotal: (total) => `共 ${total} 笔` }}
                      scroll={{ x: 780, y: 400 }}
                      columns={[
                        { title: '预测日', dataIndex: 'pred_date', key: 'pred_date', width: 85, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.pred_date },
                        { title: '代码', dataIndex: 'stock_code', key: 'stock_code', width: 75 },
                        { title: '名称', dataIndex: 'stock_name', key: 'stock_name', width: 70 },
                        { title: '买入日', dataIndex: 'buy_date', key: 'buy_date', width: 85 },
                        { title: '买入价', dataIndex: 'buy_price', key: 'buy_price', width: 70, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.buy_price.toFixed(2) },
                        { title: '卖出日', dataIndex: 'sell_date', key: 'sell_date', width: 85 },
                        { title: '卖出价', dataIndex: 'sell_price', key: 'sell_price', width: 70, render: (_: unknown, r: typeof backtestSim.trades[0]) => r.sell_price.toFixed(2) },
                        {
                          title: '收益', dataIndex: 'return_pct', key: 'return_pct', width: 70,
                          render: (_: unknown, r: typeof backtestSim.trades[0]) => (
                            <span className={r.return_pct >= 0 ? 'text-red-400' : 'text-green-400'}>
                              {pctNum(r.return_pct)}
                            </span>
                          ),
                        },
                      ]}
                    />
                  </details>
                )}
              </>
            )}

            {!backtestSim && !backtestSimLoading && (
              <div className="text-xs text-tertiary-text">
                选择天数点击查询，系统将读取预测文件模拟实盘交易。
              </div>
            )}
          </Card>

          {/* Backtest Compare */}
          {backtest && (
            <Card>
              <div className="font-medium text-sm text-secondary-text mb-3">回测对比</div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                {backtest.lgb_metrics && Object.entries(backtest.lgb_metrics).slice(0, 4).map(([k, v]) => (
                  <StatCard key={`lgb_${k}`} label={`LGB ${k}`} value={typeof v === 'number' ? v.toFixed(4) : String(v)} />
                ))}
                {backtest.comparison && Object.entries(backtest.comparison).slice(0, 4).map(([k, v]) => (
                  <StatCard key={`cmp_${k}`} label={k} value={typeof v === 'number' ? pctNum(v as number) : String(v)} />
                ))}
              </div>

              {capitalData.length > 0 && (
                <ResponsiveContainer width="100%" height={320}>
                  <LineChart data={capitalData}>
                    <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                    <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
                    <Tooltip contentStyle={{ background: '#000', border: '1px solid #333', borderRadius: 6, color: '#fff', fontSize: 12 }} />
                    <Legend />
                    <Line type="monotone" dataKey="lgb" name="LightGBM" stroke="#3b82f6" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="benchmark" name="基准" stroke="#fbbf24" strokeWidth={1.5} strokeDasharray="3 3" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </Card>
          )}
        </div>
      </div>
    </AppPage>
  );
};

export default LightGBMPage;
