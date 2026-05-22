# LGB Peak Speed Label Mode Design

## Overview

新增 LightGBM 训练标签模式 `peak_speed`，与现有固定持有期模式 (`fixed`) 互斥并列。模型不再预测"第 N 天涨多少"，而是预测"窗口内最多能涨多少"以及"几天能到达峰值"。

## 动机

固定持有期标签的局限：
- 股票第 2 天涨 8% 但第 3 天回落到 2%，模型只学到 2%（信息浪费）
- 不同股票的最佳持有期不同，固定天数无法适配
- 无法回答"什么时候卖"的问题

## 设计

### 双模型架构

| 模型 | 标签 | 类型 | 用途 |
|------|------|------|------|
| 主模型 | peak_return | 回归 | 选股排序（预期涨幅） |
| 辅助模型 | days_to_peak | 回归 | 预测卖出时间 |

两个独立 LGB 回归模型，共享相同特征矩阵（因子分数），分别训练。

### 标签构造

```
对每个样本 (trade_date T, stock):
  1. 取 T+1 到 T+W（W=window_days，默认20）共 W 个交易日的后复权价格
  2. peak_return = max(price[T+1:T+W]) / price[T] - 1
  3. days_to_peak = argmax(price[T+1:T+W]) + 1
  4. 如果 peak_return < peak_min_return（默认 1%），则:
     - peak_return 标签 = 0
     - days_to_peak 标签 = W（视为窗口到期无有效上涨）
  5. 对 peak_return 做 Winsorize（1% / 99% 分位数截断）
```

exec_mode 逻辑：
- `exec_mode="close"`: price 取收盘价
- `exec_mode="open"`: price 取开盘价（T+1 为买入日开盘）

### 参数设计

LGBTrainer 新增参数：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `label_mode` | str | `"fixed"` | `"fixed"` 或 `"peak_speed"` |
| `window_days` | int | 20 | 峰值搜索窗口（仅 peak_speed 模式有效） |
| `peak_min_return` | float | 0.01 | 最小门槛，低于此值视为无效上涨 |
| `winsorize_quantile` | float | 0.99 | Winsorize 截断分位数 |

当 `label_mode="fixed"` 时，行为与现有完全一致（向后兼容）。

### 模型保存

- 主模型文件：`lgb_{mode}_peak{W}d_{start}_{end}_{exec_suffix}.joblib`
- 辅助模型文件：`lgb_{mode}_peak{W}d_{start}_{end}_{exec_suffix}_days.joblib`
- meta 中记录 `label_mode`, `window_days`, `peak_min_return`, `winsorize_quantile`

### 模式互斥（前端）

训练面板：
- Radio 选择：「固定持有期」 vs 「峰值速度」
- 选固定持有期 → 显示 `forward_days` 参数
- 选峰值速度 → 显示 `window_days`、`peak_min_return` 参数，隐藏 `forward_days`

预测页面：
- 加载模型时根据 meta.label_mode 自动识别模式
- 固定模式：显示"预测涨幅"
- 峰值模式：显示"预测涨幅" + "预计见顶天数"

### 推理输出

固定模式（现有）：
```json
{"rank": 1, "stock_code": "600519", "lgb_score": 85.2, "raw_score": 0.032}
```

峰值速度模式（新增）：
```json
{"rank": 1, "stock_code": "600519", "lgb_score": 85.2, "raw_score": 0.082, "predicted_days": 7}
```

### 回测适配

训练时两种模式独立回测，结果可并行对比：
- 固定模式回测：买入后持有 N 天卖出（现有逻辑）
- 峰值模式回测：买入后按辅助模型预测天数卖出（±2天容差观察窗口），叠加止盈止损兜底：
  - 到达预测天数前若已达到预测涨幅 80%，提前止盈
  - 持有期间跌破 -5% 止损
  - 超过 window_days 强制退出

### API 变更

`LGBTrainRequest` 新增字段：
```python
label_mode: str = Field("fixed", description="标签模式: fixed | peak_speed")
window_days: int = Field(20, ge=5, le=60, description="峰值搜索窗口天数（peak_speed 模式）")
peak_min_return: float = Field(0.01, ge=0.0, le=0.1, description="最小峰值门槛")
```

预测响应 `LGBPredictionItem` 新增可选字段：
```python
predicted_days: Optional[int] = None  # 峰值模式下的预测见顶天数
```

## 实现范围

1. `src/discovery/ml/lgb_trainer.py` — 标签计算、双模型训练/保存/加载/预测
2. `api/v1/schemas/research.py` — 请求/响应 schema 扩展
3. `api/v1/endpoints/research.py` — 训练/预测 endpoint 适配
4. `apps/dsa-web/src/pages/LightGBMPage.tsx` — 前端训练面板模式切换 + 预测展示

## 不做的事

- 不修改现有 `label_mode="fixed"` 的任何行为
- 不引入多任务学习（multi-task）——两个模型完全独立
- 不修改因子体系或特征工程
- 不修改现有回测 API 的返回格式（新增字段但不删改）
