import type React from 'react';
import { useCallback, useRef, useState } from 'react';
import { getParsedApiError } from '../../api/error';
import { stocksApi, type ExtractItem } from '../../api/stocks';
import { systemConfigApi, SystemConfigConflictError } from '../../api/systemConfig';
import { Badge, Button, InlineAlert, MultiSelect } from '../common';

const IMG_EXT = ['.jpg', '.jpeg', '.png', '.webp', '.gif'];
const IMG_MAX = 5 * 1024 * 1024; // 5MB
const FILE_MAX = 2 * 1024 * 1024; // 2MB
const TEXT_MAX = 100 * 1024; // 100KB

interface IntelligentImportProps {
  stockListValue: string;
  configVersion: string;
  maskToken: string;
  onMerged: (newValue: string) => void | Promise<void>;
  disabled?: boolean;
}

type ItemWithChecked = ExtractItem & { id: string; checked: boolean };

function getConfidenceMeta(confidence: 'high' | 'medium' | 'low') {
  if (confidence === 'high') {
    return { label: '高', badge: 'success' as const };
  }
  if (confidence === 'low') {
    return { label: '低', badge: 'warning' as const };
  }
  return { label: '中', badge: 'default' as const };
}

function normalizeConfidence(confidence?: string | null): 'high' | 'medium' | 'low' {
  if (confidence === 'high' || confidence === 'low' || confidence === 'medium') {
    return confidence;
  }
  return 'medium';
}

function mergeItems(
  prev: ItemWithChecked[],
  newItems: ExtractItem[]
): ItemWithChecked[] {
  const byCode = new Map<string, ItemWithChecked>();
  const confOrder: Record<'high' | 'medium' | 'low', number> = {
    high: 3,
    medium: 2,
    low: 1,
  };
  const failed: ItemWithChecked[] = [];
  for (const p of prev) {
    if (p.code) {
      byCode.set(p.code, p);
    } else {
      failed.push(p);
    }
  }
  for (const it of newItems) {
    const normalizedConfidence = normalizeConfidence(it.confidence);
    if (it.code) {
      const existing = byCode.get(it.code);
      if (!existing) {
        byCode.set(it.code, {
          ...it,
          confidence: normalizedConfidence,
          id: `${it.code}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
          checked: normalizedConfidence === 'high',
        });
      } else {
        const existingConfidence = normalizeConfidence(existing.confidence);
        const shouldUpgradeConfidence = confOrder[normalizedConfidence] > confOrder[existingConfidence];
        const shouldFillName = !existing.name && !!it.name;

        if (shouldUpgradeConfidence || shouldFillName) {
          byCode.set(it.code, {
            ...existing,
            name: it.name || existing.name,
            confidence: shouldUpgradeConfidence ? normalizedConfidence : existingConfidence,
            checked: shouldUpgradeConfidence
              ? (normalizedConfidence === 'high' ? true : existing.checked)
              : existing.checked,
          });
        }
      }
    } else {
      failed.push({
        ...it,
        confidence: normalizedConfidence,
        id: `fail-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        checked: false,
      });
    }
  }
  return [...byCode.values(), ...failed];
}

export const IntelligentImport: React.FC<IntelligentImportProps> = ({
  configVersion,
  maskToken,
  onMerged,
  disabled,
}) => {
  const [items, setItems] = useState<ItemWithChecked[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isMerging, setIsMerging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [pasteText, setPasteText] = useState('');
  const [selectedCategories, setSelectedCategories] = useState<string[]>(['ALL']);
  const imageInputRef = useRef<HTMLInputElement | null>(null);
  const dataFileInputRef = useRef<HTMLInputElement | null>(null);



  const getCategoryStocks = useCallback(() => {
    const categoryStocks: Record<string, string[]> = {
      ALL: ["000063","002415","002583","002709","002920","002968","300073","300347","300357","300709","300917","600739","603328","688039","688046","688117","000909","002484","600748","603186","300249","002007","002475","603496","002407","300560","300019","600064","002371","300603","600110","603087","002245","300725","000988","002074","002091","002229","002315","002446","002460","002532","002600","002812","002885","300017","300166","300245","300252","300373","300383","300627","300759","300833","301292","301630","600583","600884","601512","603019","688153","688228","688653","688259","000977","301176","688088","000158","000402","002314","002929","300892","301152","600512","603236","688131","002354","002913","300613","300693","300837","600094","600845","603006","300686","300811","301238","600126","600460","600657","603659","603881","605088","688158","688270","688316","688443","002273","601633","000676","000718","000736","001339","002156","002757","300077","300456","300496","300619","301150","600050","600100","600266","601588","688183","688687","002177","688235","688702","002594","600736","688020","688322","000029","000066","000636","000668","000725","000823","002185","002340","002373","002465","002466","002617","002837","300047","300183","300250","300454","300465","300671","600206","600510","600622","600663","600797","603026","603887","605358","688076","688261","688396","688469","688573","688737","688795","920808","000036","300442","300846","600183","300459","000514","001914","300700","600067","920060","001389","002463","300223","300408","600186","605398","300475","301171","688005","000938","600895","002916","002938","603663","688041","688256","688519","002125","002636","000815","001283","300134","300386","300698","300738","300890","300895","301217","600198","600340","600589","600699","603912","688017","688234","688345","688521","688981","920491","300570","300663","600278","002436","002552","688116","688515","600322","002023","002845","300353","600584","601155","688661","300490","300638","600638","600649","600325","000560","001979","300025","300123","300340","300476","300614","300676","300708","688567","688220","600048","600745","600539","002428","688048","688478","688559","688486","002188","002608","002653","300058","300308","300438","300857","301041","301085","603228","603322","688195","000031","002123","002975","300296","300394","301511","600515","600602","600665","000656","000926","002281","300870","600162","600246","688112","688313","688802","000002","000011","000608","000797","000965","002174","002285","300295","300317","300548","300620","600173","600376","600383","600606","603042","603200","603290","603920","688047","688063","300473","600552","603220","300068","000006","002384","002771","300153","301205","600641","600773","300502","000517","000620","000614","002133","002261","002587","002882","600208","600563","600675","600743","833881","002208","002400","300376","301611","600703","688662","002146","002654","002778","300474","600604","600708","600782","920961","002197","688498","600791","600683","688778","000042","603259","600692","000553","000863","600620","600684","600848","600716","000981","000631","000838"],
      BUY: ["000063","002415","002583","002709","002920","002968","300073","300347","300357","300709","300917","600739","603328","688039","688046","688117","000909","002484","600748","603186","300249","002007","002475","603496","002407","300560","300019","600064","002371","300603","600110","603087","002245","300725","000988","002074","002091","002229","002315","002446","002460","002532","002600","002812","002885","300017","300166","300245","300252","300373","300383","300627","300759","300833","301292","301630","600583","600884","601512","603019","688153","688228","688653","688259","000977","301176","688088","000158","000402","002314","002929","300892","301152","600512","603236","688131","002354","002913","300613","300693","300837","600094","600845","603006","300686","300811","301238","600126","600460","600657","603659","603881","605088","688158","688270","688316","688443","002273","601633","000676","000718","000736","001339","002156","002757","300077","300456","300496","300619","301150","600050","600100","600266","601588","688183","688687","002177","688235","688702","002594","600736","688020","688322","000029","000066","000636","000668","000725","000823","002185","002340","002373","002465","002466","002617","002837","300047","300183","300250","300454","300465","300671","600206","600510","600622","600663","600797","603026","603887","605358","688076","688261","688396","688469","688573","688737","688795","920808","000036","300442","300846","600183","300459","000514","001914","300700","600067","920060"],
      HOLD: ["001389","002463","300223","300408","600186","605398","300475","301171","688005","000938","600895","002916","002938","603663","688041","688256","688519","002125","002636","000815","001283","300134","300386","300698","300738","300890","300895","301217","600198","600340","600589","600699","603912","688017","688234","688345","688521","688981","920491","300570","300663","600278","002436","002552","688116","688515","600322","002023","002845","300353","600584","601155","688661","300490","300638","600638","600649","600325","000560","001979","300025","300123","300340","300476","300614","300676","300708","688567","688220","600048","600745"],
      LOOK: ["600539","002428","688048","688478","688559","688486","002188","002608","002653","300058","300308","300438","300857","301041","301085","603228","603322","688195","000031","002123","002975","300296","300394","301511","600515","600602","600665","000656","000926","002281","300870","600162","600246","688112","688313","688802","000002","000011","000608","000797","000965","002174","002285","300295","300317","300548","300620","600173","600376","600383","600606","603042","603200","603290","603920","688047","688063","300473","600552","603220","300068","000006","002384","002771","300153","301205","600641","600773","300502","000517","000620","000614","002133","002261","002587","002882","600208","600563","600675","600743","833881","002208","002400","300376","301611","600703","688662","002146","002654","002778","300474","600604","600708","600782","920961","002197","688498","600791","600683"],
      SELL: ["688778","000042","603259","600692","000553","000863","600620","600684","600848","600716","000981","000631","000838"]
    };
    
    if (selectedCategories.includes('ALL')) {
      return categoryStocks['ALL'] || [];
    }
    
    return selectedCategories.flatMap(category => categoryStocks[category] || []);
  }, [selectedCategories]);

  const addItems = useCallback((newItems: ExtractItem[]) => {
    setItems((prev) => mergeItems(prev, newItems));
  }, []);

  const handleImageFile = useCallback(
    async (file: File) => {
      const ext = '.' + (file.name.split('.').pop() ?? '').toLowerCase();
      if (!IMG_EXT.includes(ext)) {
        setError('图片仅支持 JPG、PNG、WebP、GIF');
        return;
      }
      if (file.size > IMG_MAX) {
        setError('图片不超过 5MB');
        return;
      }
      setError(null);
      setIsLoading(true);
      try {
        const res = await stocksApi.extractFromImage(file);
        addItems(res.items ?? res.codes.map((c) => ({ code: c, name: null, confidence: 'medium' })));
      } catch (e) {
        const parsed = getParsedApiError(e);
        const err = e && typeof e === 'object' ? (e as { response?: { status?: number }; code?: string }) : null;
        let fallback = '识别失败，请重试';
        if (err?.response?.status === 429) fallback = '请求过于频繁，请稍后再试';
        else if (err?.code === 'ECONNABORTED') fallback = '请求超时，请检查网络后重试';
        setError(parsed.message || fallback);
      } finally {
        setIsLoading(false);
      }
    },
    [addItems],
  );

  const handleDataFile = useCallback(
    async (file: File) => {
      if (file.size > FILE_MAX) {
        setError('文件不超过 2MB');
        return;
      }
      setError(null);
      setIsLoading(true);
      try {
        const res = await stocksApi.parseImport(file);
        addItems(res.items ?? res.codes.map((c) => ({ code: c, name: null, confidence: 'medium' })));
      } catch (e) {
        const parsed = getParsedApiError(e);
        setError(parsed.message || '解析失败');
      } finally {
        setIsLoading(false);
      }
    },
    [addItems],
  );

  const handlePasteParse = useCallback(() => {
    const t = pasteText.trim();
    if (!t) return;
    if (new Blob([t]).size > TEXT_MAX) {
      setError('粘贴文本不超过 100KB');
      return;
    }
    setError(null);
    setIsLoading(true);
    stocksApi
      .parseImport(undefined, t)
      .then((res) => {
        addItems(res.items ?? res.codes.map((c) => ({ code: c, name: null, confidence: 'medium' })));
        setPasteText('');
      })
      .catch((e) => {
        const parsed = getParsedApiError(e);
        setError(parsed.message || '解析失败');
      })
      .finally(() => setIsLoading(false));
  }, [pasteText, addItems]);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      if (disabled || isLoading) return;
      const f = e.dataTransfer?.files?.[0];
      if (!f) return;
      const ext = '.' + (f.name.split('.').pop() ?? '').toLowerCase();
      if (IMG_EXT.includes(ext)) void handleImageFile(f);
      else void handleDataFile(f);
    },
    [disabled, isLoading, handleImageFile, handleDataFile],
  );

  const onImageInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const f = e.target.files?.[0];
      if (f) void handleImageFile(f);
      e.target.value = '';
    },
    [handleImageFile],
  );

  const onDataFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const f = e.target.files?.[0];
      if (f) void handleDataFile(f);
      e.target.value = '';
    },
    [handleDataFile],
  );

  const openFilePicker = useCallback((inputRef: React.RefObject<HTMLInputElement | null>) => {
    if (disabled || isLoading) {
      return;
    }
    inputRef.current?.click();
  }, [disabled, isLoading]);

  const toggleChecked = useCallback((id: string) => {
    setItems((prev) => prev.map((p) => (p.id === id && p.code ? { ...p, checked: !p.checked } : p)));
  }, []);

  const toggleAll = useCallback((checked: boolean) => {
    setItems((prev) => prev.map((p) => (p.code ? { ...p, checked } : p)));
  }, []);

  const removeItem = useCallback((id: string) => {
    setItems((prev) => prev.filter((p) => p.id !== id));
  }, []);

  const clearAll = useCallback(() => {
    setItems([]);
    setPasteText('');
    setError(null);
  }, []);

  const mergeToWatchlist = useCallback(async () => {
    const toMerge = items.filter((i) => i.checked && i.code).map((i) => i.code!);
    if (toMerge.length === 0 && selectedCategories.length === 0) return;
    if (!configVersion) {
      setError('请先加载配置后再合并');
      return;
    }
    
    const categoryStocks = getCategoryStocks();
    const merged = [...new Set([...toMerge, ...categoryStocks])];
    const value = merged.join(',');

    setIsMerging(true);
    setError(null);
    try {
      await systemConfigApi.update({
        configVersion,
        maskToken,
        reloadNow: true,
        items: [{ key: 'STOCK_LIST', value }],
      });
      setItems([]);
      setPasteText('');
      await onMerged(value);
    } catch (e) {
      if (e instanceof SystemConfigConflictError) {
        await onMerged(value);
        setError('配置已更新，请再次点击「合并到自选股」');
      } else {
        setError(e instanceof Error ? e.message : '合并保存失败');
      }
    } finally {
      setIsMerging(false);
    }
  }, [items, configVersion, maskToken, onMerged, getCategoryStocks, selectedCategories]);

  const validCount = items.filter((i) => i.code).length;
  const checkedCount = items.filter((i) => i.checked && i.code).length;

  return (
    <div className="space-y-4">
      <div className="settings-surface-panel settings-border-strong rounded-xl border p-4 shadow-soft-card">
        <p className="text-sm font-medium text-foreground">支持图片、CSV/Excel 文件与剪贴板文本</p>
        <p className="mt-1 text-xs leading-5 text-secondary-text">
          图片识别需预先配置 Vision 模型。建议先人工核对解析结果，再合并到自选股。
        </p>
      </div>



      <div
        onDrop={onDrop}
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={(e) => { e.preventDefault(); setIsDragging(false); }}
        className={`flex min-h-[96px] flex-col gap-4 rounded-xl border border-dashed  p-4 transition-colors ${
          isDragging ? 'settings-drag-active' : 'settings-border-strong settings-surface-overlay-soft'
        } ${disabled || isLoading ? 'cursor-not-allowed opacity-60' : ''}`}
      >
        <div className="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            variant="settings-secondary"
            disabled={disabled || isLoading}
            onClick={() => openFilePicker(imageInputRef)}
          >
            选择图片
          </Button>
          <input
            ref={imageInputRef}
            type="file"
            accept=".jpg,.jpeg,.png,.webp,.gif"
            className="hidden"
            onChange={onImageInput}
            disabled={disabled || isLoading}
          />
          <Button
            type="button"
            variant="settings-secondary"
            disabled={disabled || isLoading}
            onClick={() => openFilePicker(dataFileInputRef)}
          >
            选择文件
          </Button>
          <input
            ref={dataFileInputRef}
            type="file"
            accept=".csv,.xlsx,.txt"
            className="hidden"
            onChange={onDataFileInput}
            disabled={disabled || isLoading}
          />
        </div>
        <div className="flex flex-col gap-2 sm:flex-row">
          <textarea
            placeholder="或粘贴 CSV/Excel 复制的文本..."
            className="input-surface settings-surface-strong settings-border-strong min-h-[72px] w-full rounded-xl border px-3 py-2 text-sm text-foreground shadow-none transition-colors placeholder:text-muted-text focus:outline-none"
            value={pasteText}
            onChange={(e) => setPasteText(e.target.value)}
            disabled={disabled || isLoading}
          />
          <Button
            type="button"
            variant="settings-secondary"
            className="shrink-0 sm:self-start"
            onClick={handlePasteParse}
            disabled={disabled || isLoading || !pasteText.trim()}
          >
            解析
          </Button>
        </div>
      </div>

      {isLoading && <p className="text-sm text-secondary-text">处理中...</p>}
      {error && (
        <InlineAlert
          variant="danger"
          message={error}
          className="rounded-xl px-3 py-2 text-sm shadow-none"
        />
      )}

      {items.length > 0 && (
        <div className="space-y-2">
          <InlineAlert
            variant="warning"
            message="建议人工逐条核对后再合并。高置信度默认勾选，中/低置信度需手动确认。"
            className="rounded-xl px-3 py-2 text-xs shadow-none"
          />
          <div className="flex items-center justify-between">
            <span className="text-xs text-secondary-text">
              共 {validCount} 条可合并，已勾选 {checkedCount} 条
            </span>
            <div className="flex gap-2">
              <button type="button" className="text-xs text-secondary-text transition-colors hover:text-foreground" onClick={() => toggleAll(true)}>
                全选
              </button>
              <button type="button" className="text-xs text-secondary-text transition-colors hover:text-foreground" onClick={() => toggleAll(false)}>
                取消
              </button>
              <button type="button" className="text-xs text-secondary-text transition-colors hover:text-foreground" onClick={clearAll}>
                清空
              </button>
            </div>
          </div>
          <div className="max-h-[220px] space-y-1 overflow-y-auto rounded-xl border settings-border-strong settings-surface-overlay-soft p-2">
            {items.map((it) => {
              const confidence = normalizeConfidence(it.confidence);
              const confidenceMeta = getConfidenceMeta(confidence);

              return (
                <div
                  key={it.id}
                  className={`flex items-center gap-2 rounded-xl border px-3 py-2 text-sm ${
                    it.code ? 'settings-border bg-[var(--settings-surface-strong)]' : 'border-danger/25 bg-danger/10'
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={it.checked}
                    onChange={() => toggleChecked(it.id)}
                    disabled={!it.code || disabled}
                    className="settings-input-checkbox h-4 w-4 rounded border-border/70 bg-base"
                  />
                  <span className={it.code ? 'font-medium text-foreground' : 'font-medium text-danger'}>
                    {it.code || '解析失败'}
                  </span>
                  {it.name && <span className="text-secondary-text">({it.name})</span>}
                  <div className="ml-auto flex items-center gap-2">
                    <Badge variant={confidenceMeta.badge} size="sm">
                      {confidenceMeta.label}
                    </Badge>
                    <button
                      type="button"
                      className="text-secondary-text transition-colors hover:text-foreground"
                      onClick={() => removeItem(it.id)}
                      disabled={disabled}
                    >
                      ×
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="mt-2">
            <p className="text-xs text-secondary-text mb-1">分类选择</p>
            <MultiSelect
              value={selectedCategories}
              onChange={(value) => {
                if (value.includes('ALL')) {
                  setSelectedCategories(['ALL']);
                } else {
                  setSelectedCategories(value);
                }
              }}
              options={[
                { value: 'ALL', label: '全部 (ALL)' },
                { value: 'BUY', label: '买入 (BUY)' },
                { value: 'HOLD', label: '持有 (HOLD)' },
                { value: 'LOOK', label: '观望 (LOOK)' },
                { value: 'SELL', label: '卖出 (SELL)' }
              ]}
              disabled={disabled || isLoading}
            />
          </div>
          <Button
            type="button"
            variant="primary"
            className="mt-2"
            onClick={() => void mergeToWatchlist()}
            disabled={disabled || isMerging || checkedCount === 0}
          >
            {isMerging ? '保存中...' : '合并到自选股'}
          </Button>
        </div>
      )}
    </div>
  );
};
