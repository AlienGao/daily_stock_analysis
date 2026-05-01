# -*- coding: utf-8 -*-
"""因子代码生成器 —— 使用 LLM 将因子假设转化为可执行的 Python 代码。

借鉴 RD-Agent 的 CoSTEER 机制：LLM 根据假设描述和现有因子示例，
生成完整的 BaseFactor 子类代码，包含 fetch_data()、score()、describe()。
"""

import logging
import re
import textwrap
from typing import Dict, List, Optional

from src.discovery.factor_evaluator import _validate_code_safety

logger = logging.getLogger(__name__)

# 示例因子代码（MomentumFactor 简化版），展示 fetch_data / score / describe 三件套
_FACTOR_EXAMPLE = textwrap.dedent("""
import pandas as pd
from src.discovery.factors.base import BaseFactor

class MomentumFactor(BaseFactor):
    name = "momentum"
    available_intraday = True
    available_postmarket = False
    weight = 25.0

    def fetch_data(self, trade_date, **kwargs):
        tushare_fetcher = kwargs.get("tushare_fetcher")
        if tushare_fetcher is None:
            return None
        mf = tushare_fetcher.get_bulk_money_flow(trade_date)
        db = tushare_fetcher.get_daily_basic_all(trade_date)
        if mf is None:
            return None
        result = mf.copy()
        if db is not None and not db.empty:
            for col in ["turnover_rate", "volume_ratio"]:
                if col in db.columns:
                    result[col] = db[col]
        return result

    def score(self, df, **context):
        scores = pd.Series(0.0, index=df.index)
        if df.empty:
            return scores
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        turnover_rate = df.get("turnover_rate", pd.Series(0, index=df.index))
        buy_elg = df.get("buy_elg_amount", pd.Series(0, index=df.index))
        sell_elg = df.get("sell_elg_amount", pd.Series(0, index=df.index))
        major_net = buy_elg - sell_elg
        scores.loc[major_net > 0] += 20
        scores.loc[volume_ratio > 2] += 15
        scores.loc[(turnover_rate >= 3) & (turnover_rate <= 15)] += 10
        scores.loc[major_net < 0] = 0
        return scores.clip(0, 100)

    def describe(self, df, scores, **context):
        reasons = {}
        volume_ratio = df.get("volume_ratio", pd.Series(1.0, index=df.index))
        for ts_code in scores.index:
            if scores[ts_code] <= 0:
                continue
            r = []
            _vr = volume_ratio.get(ts_code, 1)
            if _vr > 2:
                r.append(f"放量启动(量比{_vr:.1f})")
            if r:
                reasons[ts_code] = r
        return reasons
""").strip()


# 可用的 Tushare 数据源描述
_AVAILABLE_DATA_SOURCES = """
## 可用的 Tushare 数据获取方法（通过 tushare_fetcher 调用）

### 全市场批量数据（返回 index=ts_code 的 DataFrame）：
- `tushare_fetcher.get_bulk_money_flow(trade_date)` → 资金流向
  列: buy_elg_amount, sell_elg_amount, buy_lg_amount, sell_lg_amount,
       buy_md_amount, sell_md_amount, net_mf_amount
- `tushare_fetcher.get_daily_basic_all(trade_date)` → 每日指标
  列: turnover_rate(换手率), volume_ratio(量比), pe, pe_ttm, pb, total_mv, circ_mv
- `tushare_fetcher.get_margin_detail_all(trade_date)` → 融资融券
  列: rzye(融资余额), rqye(融券余额), rzche(融资买入额), rzrqjyzl(融资融券余额)
- `tushare_fetcher.get_technical_factors_all(trade_date)` → 技术指标
  列: macd_dif, macd_dea, macd, kdj_k, kdj_d, kdj_j,
       rsi_6, rsi_12, rsi_24, boll_up, boll_mid, boll_lower
- `tushare_fetcher.get_limit_list(trade_date)` → 涨跌停记录
  列: pct_chg(涨跌幅), limit_times(连板数), up_stat(涨停统计)
- `tushare_fetcher.get_cyq_winner_all(trade_date)` → CYQ 筹码
  列: winner_ratio(获利比例), avg_cost(平均成本),
       concentration_90, concentration_70

### 单股票查询：
- `tushare_fetcher.get_money_flow(ts_code, trade_date)` → 单股资金流向
- `tushare_fetcher.get_daily_basic(ts_code, trade_date)` → 单股每日指标
- `tushare_fetcher.get_daily(ts_code)` → 日线行情 (open/high/low/close/volume)

### 注意事项：
- 所有全市场批量方法传入 trade_date (YYYYMMDD) 字符串
- DataFrame index 为 ts_code (如 000001.SZ, 600519.SH)
- 部分列可能为空，务必使用 df.get() 带默认值的模式
- score() 的输入 df 即是 fetch_data() 的返回值，index 为 ts_code
- 返回 pd.Series(index=ts_code, values=0-100)，越高越好
- 可用 scores.loc[条件] += 分值 累加打分
- describe() 为可选，接收 df 和 scores，返回 {ts_code: [理由列表]}
""".strip()


_SYSTEM_PROMPT = """你是一个量化因子工程师。你的任务是生成中国 A 股选股因子的 Python 代码。

## 规则
1. 只输出 Python 代码，不要有任何解释或 markdown 标记
2. 代码必须是一个完整的 BaseFactor 子类，包含 name、available_intraday、available_postmarket、weight 四个类属性
3. 必须实现 fetch_data(trade_date, **kwargs)、score(df, **context) 两个方法
4. describe(df, scores, **context) 方法可选但推荐实现
5. 从 tushare_fetcher 获取数据，从 kwargs/context 中取出 tushare_fetcher
6. score() 返回 pd.Series(index=ts_code, values=0-100)，通过累加打分模式
7. 正确的导入: import pandas as pd; from src.discovery.factors.base import BaseFactor
8. 使用 df.get() 安全取值，避免 KeyError
9. scores.clip(0, 100) 确保分数在合理范围
10. 类名用 PascalCase，name 用 snake_case 英文

## 可用数据源（详见上文）

## 参考示例（MomentumFactor）
只参考结构，逻辑需要根据你的假设重新设计。""".strip()


class FactorCoder:
    """LLM 驱动的因子代码生成器。

    使用现有 LLM 基础设施（LLMToolAdapter）将自然语言假设转化为
    可执行的 BaseFactor 子类代码。
    """

    def __init__(self, llm_adapter=None):
        """初始化。

        Args:
            llm_adapter: LLMToolAdapter 实例。None 则自动创建。
        """
        if llm_adapter is None:
            from src.agent.llm_adapter import LLMToolAdapter
            llm_adapter = LLMToolAdapter()
        self._adapter = llm_adapter

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, hypothesis: str, max_retries: int = 2) -> str:
        """根据假设生成因子代码。

        Args:
            hypothesis: 自然语言因子假设，如 "检测缩量回调后放量起涨的股票"
            max_retries: 代码不合格时的最大重试次数

        Returns:
            完整的 Python 代码字符串；失败返回空字符串
        """
        for attempt in range(max_retries + 1):
            code = self._generate_once(hypothesis)
            if not code:
                continue

            # 验证
            ok, err = _validate_code_safety(code)
            if not ok:
                logger.warning("[FactorCoder] 安全检查未通过: %s", err)
                if attempt < max_retries:
                    continue
                return ""

            # 检查是否包含 BaseFactor 子类
            if "class " not in code or "BaseFactor" not in code:
                logger.warning("[FactorCoder] 未检测到 BaseFactor 子类")
                if attempt < max_retries:
                    continue
                return ""

            logger.info("[FactorCoder] 成功生成因子代码 (%d 字符)", len(code))
            return code

        return ""

    def generate_batch(
        self, hypotheses: List[str], max_retries: int = 2
    ) -> Dict[str, str]:
        """批量生成因子代码。

        Args:
            hypotheses: 因子假设列表
            max_retries: 每个因子的最大重试次数

        Returns:
            {hypothesis: code_string}，失败的值为空字符串
        """
        results: Dict[str, str] = {}
        for h in hypotheses:
            code = self.generate(h, max_retries=max_retries)
            results[h] = code
        return results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _generate_once(self, hypothesis: str) -> str:
        """单次 LLM 调用生成代码。"""
        messages = [
            {"role": "system", "content": self._build_system_prompt()},
            {"role": "user", "content": self._build_user_prompt(hypothesis)},
        ]

        try:
            response = self._adapter.call_text(messages, temperature=0.3)
        except Exception as e:
            logger.warning("[FactorCoder] LLM 调用失败: %s", e)
            return ""

        if not response.content:
            return ""

        return self._extract_code(response.content)

    def _build_system_prompt(self) -> str:
        return _SYSTEM_PROMPT + "\n\n" + _AVAILABLE_DATA_SOURCES

    def _build_user_prompt(self, hypothesis: str) -> str:
        return textwrap.dedent(f"""
        请根据以下假设生成一个完整的因子类代码:

        ## 因子假设
        {hypothesis}

        ## 参考示例
        ```python
        {_FACTOR_EXAMPLE}
        ```

        请直接输出完整的 Python 代码，包含 import 和类定义。
        不要有任何 markdown 标记、解释或注释说明。
        类名请使用有意义的 PascalCase 名称。
        """).strip()

    @staticmethod
    def _extract_code(text: str) -> str:
        """从 LLM 回复中提取纯 Python 代码。"""
        # 去掉 markdown 代码块
        text = text.strip()
        code_pattern = re.compile(r"```(?:python)?\s*\n(.*?)```", re.DOTALL)
        matches = code_pattern.findall(text)
        if matches:
            return matches[0].strip()

        # 尝试找到 import 和 class 之间的内容
        lines = text.split("\n")
        start_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith("import ") or line.strip().startswith("from "):
                start_idx = i
                break
        if start_idx is not None:
            return "\n".join(lines[start_idx:]).strip()

        return text
