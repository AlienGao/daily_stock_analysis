import type React from 'react';
import { useState, useEffect, useCallback } from 'react';
import { ClipboardCheck, CheckCircle2, XCircle, ChevronDown, ChevronRight, Loader2, RefreshCw, FileCode } from 'lucide-react';
import { AppPage, Button, Card, EmptyState } from '../components/common';
import { rdLoopApi, type PendingFactor } from '../api/rdLoop';

const RdLoopReviewPage: React.FC = () => {
  const [factors, setFactors] = useState<PendingFactor[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [actionLoading, setActionLoading] = useState<Set<string>>(new Set());
  const [actionMsg, setActionMsg] = useState<{ text: string; ok: boolean } | null>(null);

  const fetchPending = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await rdLoopApi.listPending();
      setFactors(data.items);
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || '加载待审核因子失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPending();
  }, [fetchPending]);

  const toggleExpand = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const doAction = async (name: string, action: 'approve' | 'reject') => {
    setActionLoading((prev) => new Set(prev).add(name));
    setActionMsg(null);
    try {
      const resp = action === 'approve'
        ? await rdLoopApi.approve(name)
        : await rdLoopApi.reject(name);
      setActionMsg({ text: resp.message, ok: resp.ok });
      if (resp.ok) {
        setFactors((prev) => prev.filter((f) => f.name !== name));
        setExpanded((prev) => {
          const next = new Set(prev);
          next.delete(name);
          return next;
        });
      }
    } catch (e: any) {
      setActionMsg({ text: e?.response?.data?.detail || e?.message || '操作失败', ok: false });
    } finally {
      setActionLoading((prev) => {
        const next = new Set(prev);
        next.delete(name);
        return next;
      });
    }
  };

  return (
    <AppPage>
      {/* Header */}
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-foreground">因子审核</h1>
          <p className="mt-1 text-sm text-secondary-text">
            审核 R&D 闭环自动发现的因子，批准后生效
          </p>
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={fetchPending}
          disabled={loading}
        >
          <RefreshCw className={`h-4 w-4 mr-1 ${loading ? 'animate-spin' : ''}`} />
          刷新
        </Button>
      </div>
      {actionMsg ? (
        <div
          className={`mb-4 rounded-xl border px-4 py-3 text-sm ${
            actionMsg.ok
              ? 'border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300'
              : 'border-red-200 bg-red-50 text-red-800 dark:border-red-800 dark:bg-red-950 dark:text-red-300'
          }`}
        >
          {actionMsg.text}
          <button
            type="button"
            className="ml-2 underline"
            onClick={() => setActionMsg(null)}
          >
            关闭
          </button>
        </div>
      ) : null}

      {loading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="h-6 w-6 animate-spin text-cyan" />
        </div>
      ) : error ? (
        <EmptyState
          icon={<ClipboardCheck className="h-12 w-12 text-secondary-text" />}
          title="加载失败"
          description={error}
          action={
            <Button variant="primary" onClick={fetchPending}>
              重试
            </Button>
          }
        />
      ) : factors.length === 0 ? (
        <EmptyState
          icon={<ClipboardCheck className="h-12 w-12 text-secondary-text" />}
          title="暂无待审核因子"
          description="R&D 闭环运行后，发现的因子会出现在这里等待审核"
        />
      ) : (
        <div className="space-y-3">
          {factors.map((f) => (
            <Card key={f.name} className="overflow-hidden">
              <div className="flex items-start justify-between gap-3 p-4">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <ClipboardCheck className="h-4 w-4 shrink-0 text-cyan" />
                    <h3 className="truncate text-sm font-semibold text-foreground">
                      {f.name}
                    </h3>
                  </div>
                  <p className="text-xs text-secondary-text mb-2">
                    {f.hypothesis}
                  </p>
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
                    <span className="text-amber-600 dark:text-amber-400">
                      综合评分 {f.score.toFixed(0)}
                    </span>
                    <span className="text-emerald-600 dark:text-emerald-400">
                      累计收益 {f.cum_return.toFixed(1)}%
                    </span>
                    <span className="text-blue-600 dark:text-blue-400">
                      夏普 {f.sharpe.toFixed(2)}
                    </span>
                    <span className="text-purple-600 dark:text-purple-400">
                      胜率 {f.win_rate.toFixed(0)}%
                    </span>
                  </div>
                </div>

                <div className="flex shrink-0 items-center gap-1.5">
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={actionLoading.has(f.name)}
                    onClick={() => doAction(f.name, 'approve')}
                  >
                    {actionLoading.has(f.name) ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <CheckCircle2 className="h-3.5 w-3.5" />
                    )}
                    批准
                  </Button>
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={actionLoading.has(f.name)}
                    onClick={() => doAction(f.name, 'reject')}
                    className="text-red-600 hover:bg-red-50 dark:hover:bg-red-950"
                  >
                    <XCircle className="h-3.5 w-3.5" />
                    拒绝
                  </Button>
                </div>
              </div>

              <button
                type="button"
                onClick={() => toggleExpand(f.name)}
                className="flex w-full items-center gap-1.5 border-t border-border/50 px-4 py-2 text-xs text-secondary-text hover:bg-hover transition-colors"
              >
                {expanded.has(f.name) ? (
                  <ChevronDown className="h-3.5 w-3.5" />
                ) : (
                  <ChevronRight className="h-3.5 w-3.5" />
                )}
                <FileCode className="h-3.5 w-3.5" />
                查看代码
              </button>

              {expanded.has(f.name) ? (
                <div className="border-t border-border/50 bg-muted/30 px-4 py-3">
                  <pre className="overflow-x-auto text-xs leading-relaxed text-secondary-text whitespace-pre-wrap font-mono max-h-96 overflow-y-auto">
                    {f.code}
                  </pre>
                </div>
              ) : null}
            </Card>
          ))}
        </div>
      )}
    </AppPage>
  );
};

export default RdLoopReviewPage;
