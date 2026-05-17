# Walk-Forward TPE 动态权重回测 — 设计文档

## 概述

在因子回测页面增加一个开关，打开后模拟真实环境中每 5 个交易日使用 TPE 重新调优因子权重的场景。回测结果展示固定权重 vs 动态调优两条资金曲线叠加对比。

## 核心流程

```
回测起点                                          回测终点
  |←60日→|                               ←固定权重跑全程→
  |←60日→|←5日→|                          TPE节点1→权重1 用于这5日交易
  |←60日→|←5日→|←5日→|                     TPE节点2→权重2 用于这5日交易
  |←60日→|←5日→|←5日→|←5日→|               TPE节点3→权重3 ...
```

每个 TPE 节点的 60 日窗口严格截止到该节点之前，独立 study（纯内存），无信息泄露。

## 关键隔离约束

| 约束 | 说明 |
|---|---|
| **无未来信息** | 每个 TPE 节点只看该节点之前的历史数据 |
| **纯内存 study** | `storage=None`，不写 SQLite，回测结束即销毁 |
| **独立节点** | 每个节点创建独立 study（`study_name` 含节点日期） |
| **不污染生产** | 调优页面 TPE（`factor_opt.db`）完全不受影响 |
| **预加载复用** | 全量快照+得分+价格数据预加载一次，节点间共享 |

## 详细设计

### 1. 后端 engine：`compute_walk_forward()`

`src/discovery/factor_backtest_engine.py` 新增方法：

```
compute_walk_forward(mode, start_date, end_date, hold_days,
                     reoptimize_interval, factor_weights, ...)
```

流程：
1. 预加载全量 snap_dates、trading_days、因子得分、价格（一次 DB 查询）
2. 固定权重评估全程（基线）
3. 动态调优评估：遍历每个快照节点
   - 取节点前 60 日窗口
   - 独立 TPE 搜索（内存 study）
   - 用优化权重评估该节点选股 → 持有 5 日
   - 节点按 interval 推进
4. 返回双线资金曲线 + 交易记录

### 2. 后端 optimizer：共享数据注入 + 内存模式

`src/discovery/factor_optimizer.py` 改动：

- `optimize()` 新增 `preloaded` 参数（Dict，含 scores/snap_dates/price_cache/trading_days）
- 当 `preloaded` 传入时，跳过内部预加载步骤
- 新增 `use_persistent_storage` 参数（默认 True），walk-forward 传 False → `storage=None`

窗口采样：`n_pick = min(5, len(window_pool))`，早期节点窗口少时少抽。

### 3. 后端 API

`POST /api/v1/discovery/factor-backtest` 请求体加字段：

```python
reoptimize_interval: int | None = None  # None=固定权重, 5=每5日调优
```

响应 `capital_curves` 键名：

- 固定模式（现有，`reoptimize_interval=None`）：`{"1": [...], "5": [...]}`
- 动态模式：`{"1_fixed": [...], "1_dynamic": [...], "5_fixed": [...], "5_dynamic": [...]}`

`summary` 新增 `dynamic` 子对象放动态版本统计指标（年化收益、最大回撤、夏普、胜率）。

### 4. 前端

`apps/dsa-web/src/pages/FactorBacktestPage.tsx`：

- 参数区加 Switch 开关：「动态调优 (Walk-Forward TPE)」
- 开关打开时传 `reoptimize_interval=5`
- 图表层：固定虚线 + 动态实线叠加，不同颜色区分
- summary 两列对比：固定 | 动态

### 5. 性能预估

- 预加载（1 次 DB I/O）：~30-60 秒
- 每个 TPE 节点（纯内存搜索，无 DB）：~3-10 秒
- 150 节点：~7-25 分钟（取决于 trial 数）
- 异步执行 + 前端轮询进度

### 6. 测试验证

- 回测起点在最早因子数据 3 个月之后（确保至少 1 个窗口）
- 动态曲线各节点不重叠、无跳空
- 早期节点（窗口少）和后期节点（窗口多）都能正常产出权重
- 固定权重曲线与现有回测结果一致（无回归）
- 调优页面 factor_opt.db 无新增 trial

## 不变部分

- 现有 `compute()` 固定权重回测逻辑完全不动
- TPE objective（随机窗口超额收益）不变
- 前端其他展示（IC 表、交易记录、分位数收益）保持现有逻辑
- 调优页面（FactorTuningPage）零改动
