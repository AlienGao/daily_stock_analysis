import { useState } from 'react';
import type React from 'react';
import { Badge, Button, Select, Input, Tooltip } from '../common';
import type { ConfigValidationIssue, SystemConfigFieldSchema, SystemConfigItem } from '../../types/systemConfig';
import { getFieldDescriptionZh, getFieldTitleZh } from '../../utils/systemConfigI18n';
import { cn } from '../../utils/cn';

function normalizeSelectOptions(options: SystemConfigFieldSchema['options'] = []) {
  return options.map((option) => {
    if (typeof option === 'string') {
      return { value: option, label: option };
    }

    return option;
  });
}

function isMultiValueField(item: SystemConfigItem): boolean {
  const validation = (item.schema?.validation ?? {}) as Record<string, unknown>;
  return Boolean(validation.multiValue ?? validation.multi_value);
}

function parseMultiValues(value: string): string[] {
  if (!value) {
    return [''];
  }

  const values = value.split(',').map((entry) => entry.trim());
  return values.length ? values : [''];
}

function serializeMultiValues(values: string[]): string {
  return values.map((entry) => entry.trim()).join(',');
}

function inferPasswordIconType(key: string): 'password' | 'key' {
  return key.toUpperCase().includes('PASSWORD') ? 'password' : 'key';
}

interface SettingsFieldProps {
  item: SystemConfigItem;
  value: string;
  disabled?: boolean;
  onChange: (key: string, value: string) => void;
  issues?: ConfigValidationIssue[];
  /**
   * Optional extra element rendered to the right of the field title.
   * Currently used to inject the "立即分析" button next to 自选股列表.
   */
  extraHeaderAction?: React.ReactNode;
}

function renderFieldControl(
  item: SystemConfigItem,
  value: string,
  disabled: boolean,
  onChange: (nextValue: string) => void,
  isPasswordEditable: boolean,
  onPasswordFocus: () => void,
  controlId: string,
  stockCategoryTemp?: string[],
  setStockCategoryTemp?: (value: string[] | ((prev: string[]) => string[])) => void,
  handleStockCategoryConfirm?: () => void,
) {
  const schema = item.schema;
  const commonClass = 'input-surface input-focus-glow h-11 w-full rounded-xl border bg-transparent px-4 text-sm transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60';
  const controlType = schema?.uiControl ?? 'text';
  const isMultiValue = isMultiValueField(item);

  if (controlType === 'textarea') {
    return (
      <textarea
        id={controlId}
        className={`${commonClass} min-h-[92px] resize-y py-3`}
        value={value}
        disabled={disabled || !schema?.isEditable}
        onChange={(event) => onChange(event.target.value)}
      />
    );
  }

  if (controlType === 'select' && schema?.options?.length) {
    const isStockCategory = item.key === 'STOCK_CATEGORY';
    
    if (isStockCategory && stockCategoryTemp && setStockCategoryTemp && handleStockCategoryConfirm) {
      return (
        <div className="space-y-4">
          <div className="flex flex-wrap gap-2">
            {normalizeSelectOptions(schema.options).map((option) => (
              <label key={option.value} className="inline-flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={stockCategoryTemp.includes(option.value)}
                  onChange={(e) => {
                    if (e.target.checked) {
                      if (option.value === 'ALL') {
                        setStockCategoryTemp(['ALL']);
                      } else {
                        setStockCategoryTemp(prev => {
                          if (prev.includes('ALL')) {
                            return [option.value];
                          }
                          return [...prev, option.value];
                        });
                      }
                    } else {
                      setStockCategoryTemp(prev => prev.filter(item => item !== option.value));
                    }
                  }}
                  disabled={disabled || !schema.isEditable}
                  className="rounded border-gray-300 text-primary focus:ring-primary"
                />
                <span className="text-sm">{option.label}</span>
              </label>
            ))}
          </div>
          <Button
            type="button"
            variant="primary"
            className="mt-2"
            onClick={handleStockCategoryConfirm}
            disabled={disabled || !schema.isEditable || stockCategoryTemp.length === 0}
          >
            确认
          </Button>
        </div>
      );
    }
    
    return (
        <Select
          id={controlId}
          value={value}
          onChange={onChange}
          options={normalizeSelectOptions(schema.options)}
          disabled={disabled || !schema.isEditable}
          placeholder="请选择"
        />
      );
  }

  if (controlType === 'switch') {
    const checked = value.trim().toLowerCase() === 'true';
    return (
      <label className="inline-flex cursor-pointer items-center gap-3">
        <input
          id={controlId}
          type="checkbox"
          checked={checked}
          disabled={disabled || !schema?.isEditable}
          onChange={(event) => onChange(event.target.checked ? 'true' : 'false')}
        />
        <span className="text-sm text-secondary-text">{checked ? '已启用' : '未启用'}</span>
      </label>
    );
  }

  if (controlType === 'password') {
    const iconType = inferPasswordIconType(item.key);

    if (isMultiValue) {
      const values = parseMultiValues(value);

      return (
        <div className="space-y-2">
          {values.map((entry, index) => (
            <div className="flex items-center gap-2" key={`${item.key}-${index}`}>
              <div className="flex-1">
                <Input
                  type="password"
                  allowTogglePassword
                  iconType={iconType}
                  id={index === 0 ? controlId : `${controlId}-${index}`}
                  readOnly={!isPasswordEditable}
                  onFocus={onPasswordFocus}
                  value={entry}
                  disabled={disabled || !schema?.isEditable}
                  onChange={(event) => {
                    const nextValues = [...values];
                    nextValues[index] = event.target.value;
                    onChange(serializeMultiValues(nextValues));
                  }}
                />
              </div>
              <Button
                type="button"
                variant="settings-secondary"
                size="lg"
                className="px-3 text-xs text-muted-text shadow-none hover:text-danger"
                disabled={disabled || !schema?.isEditable || values.length <= 1}
                onClick={() => {
                  const nextValues = values.filter((_, rowIndex) => rowIndex !== index);
                  onChange(serializeMultiValues(nextValues.length ? nextValues : ['']));
                }}
              >
                删除
              </Button>
            </div>
          ))}

          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="settings-secondary"
              size="sm"
              className="text-xs shadow-none"
              disabled={disabled || !schema?.isEditable}
              onClick={() => onChange(serializeMultiValues([...values, '']))}
            >
              添加 Key
            </Button>
          </div>
        </div>
      );
    }

    return (
      <Input
        type="password"
        allowTogglePassword
        iconType={iconType}
        id={controlId}
        readOnly={!isPasswordEditable}
        onFocus={onPasswordFocus}
        value={value}
        disabled={disabled || !schema?.isEditable}
        onChange={(event) => onChange(event.target.value)}
      />
    );
  }

  const inputType = controlType === 'number' ? 'number' : controlType === 'time' ? 'time' : 'text';

  return (
    <input
      id={controlId}
      type={inputType}
      className={commonClass}
      value={value}
      disabled={disabled || !schema?.isEditable}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

// Stock category to stock list mapping (frontend copy of backend logic)
const categoryStocks: Record<string, string> = {
  "ALL": "000063,002415,002583,002709,002920,002968,300073,300347,300357,300709,300917,600739,603328,688039,688046,688117,000909,002484,600748,603186,300249,002007,002475,603496,002407,300560,300019,600064,002371,300603,600110,603087,002245,300725,000988,002074,002091,002229,002315,002446,002460,002532,002600,002812,002885,300017,300166,300245,300252,300373,300383,300627,300759,300833,301292,301630,600583,600884,601512,603019,688153,688228,688653,688259,000977,301176,688088,000158,000402,002314,002929,300892,301152,600512,603236,688131,002354,002913,300613,300693,300837,600094,600845,603006,300686,300811,301238,600126,600460,600657,603659,603881,605088,688158,688270,688316,688443,002273,601633,000676,000718,000736,001339,002156,002757,300077,300456,300496,300619,301150,600050,600100,600266,601588,688183,688687,002177,688235,688702,002594,600736,688020,688322,000029,000066,000636,000668,000725,000823,002185,002340,002373,002465,002466,002617,002837,300047,300183,300250,300454,300465,300671,600206,600510,600622,600663,600797,603026,603887,605358,688076,688261,688396,688469,688573,688737,688795,920808,000036,300442,300846,600183,300459,000514,001914,300700,600067,920060,001389,002463,300223,300408,600186,605398,300475,301171,688005,000938,600895,002916,002938,603663,688041,688256,688519,002125,002636,000815,001283,300134,300386,300698,300738,300890,300895,301217,600198,600340,600589,600699,603912,688017,688234,688345,688521,688981,920491,300570,300663,600278,002436,002552,688116,688515,600322,002023,002845,300353,600584,601155,688661,300490,300638,600638,600649,600325,000560,001979,300025,300123,300340,300476,300614,300676,300708,688567,688220,600048,600745,600539,002428,688048,688478,688559,688486,002188,002608,002653,300058,300308,300438,300857,301041,301085,603228,603322,688195,000031,002123,002975,300296,300394,301511,600515,600602,600665,000656,000926,002281,300870,600162,600246,688112,688313,688802,000002,000011,000608,000797,000965,002174,002285,300295,300317,300548,300620,600173,600376,600383,600606,603042,603200,603290,603920,688047,688063,300473,600552,603220,300068,000006,002384,002771,300153,301205,600641,600773,300502,000517,000620,000614,002133,002261,002587,002882,600208,600563,600675,600743,833881,002208,002400,300376,301611,600703,688662,002146,002654,002778,300474,600604,600708,600782,920961,002197,688498,600791,600683,688778,000042,603259,600692,000553,000863,600620,600684,600848,600716,000981,000631,000838",
  "BUY": "000063,002415,002583,002709,002920,002968,300073,300347,300357,300709,300917,600739,603328,688039,688046,688117,000909,002484,600748,603186,300249,002007,002475,603496,002407,300560,300019,600064,002371,300603,600110,603087,002245,300725,000988,002074,002091,002229,002315,002446,002460,002532,002600,002812,002885,300017,300166,300245,300252,300373,300383,300627,300759,300833,301292,301630,600583,600884,601512,603019,688153,688228,688653,688259,000977,301176,688088,000158,000402,002314,002929,300892,301152,600512,603236,688131,002354,002913,300613,300693,300837,600094,600845,603006,300686,300811,301238,600126,600460,600657,603659,603881,605088,688158,688270,688316,688443,002273,601633,000676,000718,000736,001339,002156,002757,300077,300456,300496,300619,301150,600050,600100,600266,601588,688183,688687,002177,688235,688702,002594,600736,688020,688322,000029,000066,000636,000668,000725,000823,002185,002340,002373,002465,002466,002617,002837,300047,300183,300250,300454,300465,300671,600206,600510,600622,600663,600797,603026,603887,605358,688076,688261,688396,688469,688573,688737,688795,920808,000036,300442,300846,600183,300459,000514,001914,300700,600067,920060",
  "HOLD": "001389,002463,300223,300408,600186,605398,300475,301171,688005,000938,600895,002916,002938,603663,688041,688256,688519,002125,002636,000815,001283,300134,300386,300698,300738,300890,300895,301217,600198,600340,600589,600699,603912,688017,688234,688345,688521,688981,920491,300570,300663,600278,002436,002552,688116,688515,600322,002023,002845,300353,600584,601155,688661,300490,300638,600638,600649,600325,000560,001979,300025,300123,300340,300476,300614,300676,300708,688567,688220,600048,600745",
  "LOOK": "600539,002428,688048,688478,688559,688486,002188,002608,002653,300058,300308,300438,300857,301041,301085,603228,603322,688195,000031,002123,002975,300296,300394,301511,600515,600602,600665,000656,000926,002281,300870,600162,600246,688112,688313,688802,000002,000011,000608,000797,000965,002174,002285,300295,300317,300548,300620,600173,600376,600383,600606,603042,603200,603290,603920,688047,688063,300473,600552,603220,300068,000006,002384,002771,300153,301205,600641,600773,300502,000517,000620,000614,002133,002261,002587,002882,600208,600563,600675,600743,833881,002208,002400,300376,301611,600703,688662,002146,002654,002778,300474,600604,600708,600782,920961,002197,688498,600791,600683",
  "SELL": "688778,000042,603259,600692,000553,000863,600620,600684,600848,600716,000981,000631,000838"
};

// Get merged stock list from multiple categories
function getStocksByCategories(categories: string[]): string {
  const allStocks = new Set<string>();
  for (const cat of categories) {
    if (categoryStocks[cat]) {
      const stocks = categoryStocks[cat].split(',');
      stocks.forEach(stock => allStocks.add(stock));
    }
  }
  return Array.from(allStocks).sort().join(',');
}

export const SettingsField: React.FC<SettingsFieldProps> = ({
  item,
  value,
  disabled = false,
  onChange,
  issues = [],
  extraHeaderAction,
}) => {
  const schema = item.schema;
  const isMultiValue = isMultiValueField(item);
  const title = getFieldTitleZh(item.key, item.key);
  const description = getFieldDescriptionZh(item.key, schema?.description);
  const hasError = issues.some((issue) => issue.severity === 'error');
  const [isPasswordEditable, setIsPasswordEditable] = useState(false);
  const [stockCategoryTemp, setStockCategoryTemp] = useState<string[]>(
    item.key === 'STOCK_CATEGORY' && value ? value.split(',').map(v => v.trim()).filter(Boolean) : []
  );
  const controlId = `setting-${item.key}`;

  const handleStockCategoryConfirm = () => {
    if (item.key === 'STOCK_CATEGORY') {
      // Update STOCK_CATEGORY
      onChange(item.key, stockCategoryTemp.join(','));
      // Update STOCK_LIST immediately
      const stockList = getStocksByCategories(stockCategoryTemp);
      onChange('STOCK_LIST', stockList);
    }
  };

  return (
    <div
      className={cn(
        'rounded-[1.15rem] border bg-[var(--settings-surface)] p-4 shadow-soft-card transition-[background-color,border-color,box-shadow] duration-200',
        hasError ? 'border-danger/40 hover:border-danger/55' : 'border-[var(--settings-border)] hover:border-[var(--settings-border-strong)]',
        'hover:bg-[var(--settings-surface-hover)]',
      )}
    >
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <label className="text-sm font-semibold text-foreground" htmlFor={controlId}>
          {title}
        </label>
        {schema?.isSensitive ? (
          <Badge variant="history" size="sm">
            敏感
          </Badge>
        ) : null}
        {!schema?.isEditable ? (
          <Badge variant="default" size="sm">
            只读
          </Badge>
        ) : null}
        {extraHeaderAction ? (
          <div className="ml-auto flex items-center gap-2">
            {extraHeaderAction}
          </div>
        ) : null}
      </div>

      {description ? (
        <Tooltip content={description}>
          <p className="mb-3 inline-flex max-w-full text-xs leading-5 text-muted-text">
            {description}
          </p>
        </Tooltip>
      ) : null}

      <div>
        {renderFieldControl(
          item,
          value,
          disabled,
          (nextValue) => onChange(item.key, nextValue),
          isPasswordEditable,
          () => setIsPasswordEditable(true),
          controlId,
          stockCategoryTemp,
          setStockCategoryTemp,
          handleStockCategoryConfirm,
        )}
      </div>

      {schema?.isSensitive ? (
        <p className="mt-3 text-[11px] leading-5 text-secondary-text">
          敏感内容默认隐藏，可点击眼睛图标查看明文。
          {isMultiValue ? ' 支持添加多个输入框进行增删。' : ''}
        </p>
      ) : null}

      {issues.length ? (
        <div className="mt-2 space-y-1">
          {issues.map((issue, index) => (
            <p
              key={`${issue.code}-${issue.key}-${index}`}
              className={issue.severity === 'error' ? 'text-xs text-danger' : 'text-xs text-warning'}
            >
              {issue.message}
            </p>
          ))}
        </div>
      ) : null}
    </div>
  );
};
