import type React from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { Button, InlineAlert, Tooltip } from '../common';
import {
  analysisApi,
  FullAnalysisBusyError,
  type RunFullAnalysisStatus,
} from '../../api/analysis';
import { getParsedApiError, type ParsedApiError } from '../../api/error';

interface RunFullAnalysisButtonProps {
  /**
   * Number of stocks currently configured in STOCK_LIST (best-effort, used for
   * the confirm prompt).
   */
  stockCount?: number;
  disabled?: boolean;
}

// Polling cadence while the job is running. Short enough to feel live,
// long enough not to hammer the backend for long analysis runs (>10 min).
const POLL_INTERVAL_MS = 3000;
// When we detect prolonged silence from the backend we auto-resync on the
// next tick — covers browser throttling of backgrounded tabs.
const MAX_SILENT_GAP_MS = 15_000;

function formatDuration(startIso?: string | null): string {
  if (!startIso) {
    return '';
  }
  const started = new Date(startIso).getTime();
  if (Number.isNaN(started)) {
    return '';
  }
  const diffSec = Math.max(0, Math.floor((Date.now() - started) / 1000));
  const m = Math.floor(diffSec / 60);
  const s = diffSec % 60;
  return m > 0 ? `${m}分${s}秒` : `${s}秒`;
}

/**
 * Button rendered next to the STOCK_LIST field that triggers the same full
 * analysis pipeline as the scheduled task, generates a local report, and
 * optionally pushes notifications. Runs in the background; polls status.
 *
 * Rendering model:
 *   - A single `setInterval` drives polling (3s cadence).
 *   - The elapsed label is derived from `state.startedAt` inside a **separate**
 *     1s ticker that only runs while `isRunning` is true. This avoids the
 *     earlier bug where two timers fought over `elapsedLabel` and one of them
 *     fed a human-formatted string back into `new Date(...)`.
 *   - `visibilitychange` forces an immediate resync when the tab comes back
 *     from background, so we don't rely on browser-throttled timers to catch
 *     completion.
 */
export const RunFullAnalysisButton: React.FC<RunFullAnalysisButtonProps> = ({
  stockCount,
  disabled = false,
}) => {
  const [state, setState] = useState<RunFullAnalysisStatus | null>(null);
  const [startError, setStartError] = useState<ParsedApiError | null>(null);
  const [isStarting, setIsStarting] = useState(false);
  const [elapsedLabel, setElapsedLabel] = useState('');
  const pollTimer = useRef<number | null>(null);
  const lastPollAt = useRef<number>(0);

  const isRunning = state?.status === 'running';

  const stopPolling = useCallback(() => {
    if (pollTimer.current !== null) {
      window.clearInterval(pollTimer.current);
      pollTimer.current = null;
    }
  }, []);

  // Fetch latest server status. Returns whether the job is still running,
  // so the caller can decide whether to keep polling.
  const pollOnce = useCallback(async (): Promise<boolean> => {
    try {
      const next = await analysisApi.getFullAnalysisStatus();
      lastPollAt.current = Date.now();
      setState(next);
      return next.status === 'running';
    } catch (err) {
      if (typeof console !== 'undefined') {
        // Surface polling errors in dev tools without breaking the UI loop.
        console.warn('[run-full] status poll failed', err);
      }
      return true;
    }
  }, []);

  const ensurePolling = useCallback(() => {
    if (pollTimer.current !== null) {
      return;
    }
    pollTimer.current = window.setInterval(() => {
      void (async () => {
        const stillRunning = await pollOnce();
        if (!stillRunning) {
          stopPolling();
        }
      })();
    }, POLL_INTERVAL_MS);
  }, [pollOnce, stopPolling]);

  // Sync with backend once on mount — covers "job started in another tab" and
  // also lets the button reflect a completed/failed run from a previous visit.
  useEffect(() => {
    void (async () => {
      const stillRunning = await pollOnce();
      if (stillRunning) {
        ensurePolling();
      }
    })();
    return () => {
      stopPolling();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Resync aggressively when the tab becomes visible again — browsers throttle
  // setInterval heavily for background tabs, so we cannot rely on the 3s loop
  // alone to catch transitions that happen off-screen.
  useEffect(() => {
    const onVisibility = () => {
      if (document.visibilityState !== 'visible') {
        return;
      }
      // Always re-poll when the tab gains focus; restart the loop if the job
      // is still running and we had stopped polling for any reason.
      void (async () => {
        const stillRunning = await pollOnce();
        if (stillRunning) {
          ensurePolling();
        } else {
          stopPolling();
        }
      })();
    };
    const onFocus = () => onVisibility();
    document.addEventListener('visibilitychange', onVisibility);
    window.addEventListener('focus', onFocus);
    return () => {
      document.removeEventListener('visibilitychange', onVisibility);
      window.removeEventListener('focus', onFocus);
    };
  }, [ensurePolling, pollOnce, stopPolling]);

  // Watchdog: if no successful poll has happened for a long time while we
  // *think* the job is running, trigger an immediate resync. Protects against
  // weird edge cases where the interval stalls (laptop sleep, etc).
  useEffect(() => {
    if (!isRunning) {
      return;
    }
    const id = window.setInterval(() => {
      const silentMs = Date.now() - (lastPollAt.current || 0);
      if (silentMs > MAX_SILENT_GAP_MS) {
        void pollOnce().then((stillRunning) => {
          if (!stillRunning) {
            stopPolling();
          } else {
            ensurePolling();
          }
        });
      }
    }, 5000);
    return () => window.clearInterval(id);
  }, [isRunning, pollOnce, ensurePolling, stopPolling]);

  // Elapsed label ticker. Runs only while the job is active.
  useEffect(() => {
    if (!isRunning) {
      setElapsedLabel('');
      return;
    }
    const update = () => setElapsedLabel(formatDuration(state?.startedAt));
    update();
    const id = window.setInterval(update, 1000);
    return () => window.clearInterval(id);
  }, [isRunning, state?.startedAt]);

  const handleClick = useCallback(async () => {
    if (isStarting || isRunning) {
      return;
    }

    const confirmMsg =
      stockCount && stockCount > 0
        ? `将对 ${stockCount} 只自选股执行完整分析并生成本地报告，过程可能持续 10 分钟以上。是否继续？`
        : '将对当前自选股执行完整分析并生成本地报告，过程可能持续较久。是否继续？';
    if (!window.confirm(confirmMsg)) {
      return;
    }

    setStartError(null);
    setIsStarting(true);
    try {
      const next = await analysisApi.runFullAnalysis();
      lastPollAt.current = Date.now();
      setState(next);
      ensurePolling();
    } catch (err) {
      if (err instanceof FullAnalysisBusyError) {
        // Job already running — resync and resume polling.
        const stillRunning = await pollOnce();
        if (stillRunning) {
          ensurePolling();
        }
      } else {
        setStartError(getParsedApiError(err));
      }
    } finally {
      setIsStarting(false);
    }
  }, [ensurePolling, isRunning, isStarting, pollOnce, stockCount]);

  const buttonLabel = isRunning
    ? elapsedLabel
      ? `分析中 · ${elapsedLabel}`
      : '分析中…'
    : '立即分析';

  const tooltip =
    '等价于定时任务：遍历自选股逐个分析并生成 reports/report_YYYYMMDD.md。过程在后台运行，可关闭本页面。';

  return (
    <div className="flex flex-col items-end gap-1">
      <div className="flex items-center gap-2">
        <Tooltip content={tooltip}>
          <Button
            type="button"
            variant="settings-primary"
            size="sm"
            onClick={() => void handleClick()}
            disabled={disabled || isStarting || isRunning}
            isLoading={isStarting || isRunning}
            loadingText={buttonLabel}
          >
            {buttonLabel}
          </Button>
        </Tooltip>
      </div>
      {state?.status === 'completed' && !isRunning ? (
        <p className="text-[11px] leading-5 text-emerald-500">
          {state.message || '全量分析完成，报告已生成。'}
        </p>
      ) : null}
      {state?.status === 'failed' && !isRunning ? (
        <p className="text-[11px] leading-5 text-danger">
          分析失败：{state.error || state.message || '未知错误'}
        </p>
      ) : null}
      {startError ? (
        <div className="w-[320px] max-w-full">
          <InlineAlert
            variant="danger"
            title="启动分析失败"
            message={startError.message || startError.title || '请查看后端日志。'}
          />
        </div>
      ) : null}
    </div>
  );
};
