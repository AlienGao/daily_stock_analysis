interface ValidationResult {
  valid: boolean;
  message?: string;
  normalized: string;
}

const SUPPORTED_QUERY_CHARACTERS = /^[A-Z0-9.\u3400-\u9FFF\s]+$/;

const STOCK_CODE_PATTERNS = [
  /^\d{6}$/, // A-share 6-digit code
  /^(SH|SZ|BJ)\d{6}$/, // A-share code with exchange prefix
  /^\d{6}\.(SH|SZ|SS|BJ)$/, // A-share code with exchange suffix
  /^\d{5}$/, // HK code without prefix
  /^HK\d{1,5}$/, // HK-prefixed code, for example HK00700
  /^\d{1,5}\.HK$/, // HK suffix format, for example 00700.HK
  /^[A-Z]{1,5}(?:\.(?:US|[A-Z]))?$/, // Common US ticker format
];

/**
 * Check whether the input looks like a stock code.
 */
export const looksLikeStockCode = (value: string): boolean => {
  const normalized = value.trim().toUpperCase();
  return STOCK_CODE_PATTERNS.some((regex) => regex.test(normalized));
};

/**
 * Validate common A-share, HK, and US stock code formats.
 */
export const validateStockCode = (value: string): ValidationResult => {
  const normalized = value.trim().toUpperCase();

  if (!normalized) {
    return { valid: false, message: '请输入股票代码', normalized };
  }

  const valid = looksLikeStockCode(normalized);

  return {
    valid,
    message: valid ? undefined : '股票代码格式不正确',
    normalized,
  };
};

/**
 * Reject obviously invalid free-text queries before they reach the backend.
 */
export const isObviouslyInvalidStockQuery = (value: string): boolean => {
  const normalized = value.trim().toUpperCase();

  if (!normalized) {
    return false;
  }

  // 处理批量输入（逗号分隔）
  if (normalized.includes(',')) {
    const stockCodes = normalized.split(',').map(code => code.trim()).filter(code => code);
    if (stockCodes.length === 0) {
      return true;
    }
    // 检查每个股票代码是否有效
    for (const code of stockCodes) {
      if (!looksLikeStockCode(code)) {
        return true;
      }
    }
    return false;
  }

  if (looksLikeStockCode(normalized)) {
    return false;
  }

  // 对于非股票代码的输入（如股票名称），允许字母和数字的组合
  // 只拒绝明显无效的输入，如包含特殊字符的输入
  if (!SUPPORTED_QUERY_CHARACTERS.test(normalized)) {
    return true;
  }

  return false;
};
