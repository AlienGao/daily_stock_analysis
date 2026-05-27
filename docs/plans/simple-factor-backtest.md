# Plan: 独立因子回测页面

## Context

用户需要一个全新的独立因子回测页面，与现有 FactorBacktestPage 隔离。需求：
- 前端可单独勾选任意因子进行回测，也可自由组合多因子
- 不分 intraday/postmarket 模式，统一使用开盘价买入、开盘价卖出
- 新增 MarketCapFactor（小市值因子）

## 方案

### 1. 新建 `src/discovery/factors/market_cap_factor.py`

```python
class MarketCapFactor(BaseFactor):
    name = "market_cap"
    available_intraday = False
    available_postmarket = True
    weight = 1.0

    def fetch_data(trade_date, **kwargs):
        # 从 daily_basic 取 total_mv（YYYYMMDD 格式）
        # 返回 DataFrame indexed by ts_code

    def score(df, **context):
        # 市值越小分数越高，归一化 0-100

    def describe(df, scores, **context):
        # 返回市值描述
```

### 2. 注册因子 — `src/discovery/factors/__init__.py`

添加 import 和 `__all__` 条目。

### 3. 新建后端 API — `api/v1/endpoints/factor_backtest.py`

独立于现有 discovery.py 的回测端点：
- `POST /api/v1/factor-backtest/run` — 提交回测任务
- `GET /api/v1/factor-backtest/status` — 查询任务状态
- `GET /api/v1/factor-backtest/factors` — 获取可用因子列表

核心逻辑：复用 `FactorBacktestEngine`，但强制 `mode=postmarket`（开盘价交易）。

### 4. 新建前端页面 — `apps/dsa-web/src/pages/SimpleFactorBacktestPage.tsx`

- 左侧：因子列表（checkbox + 权重输入），无模式切换
- 右侧：参数配置（日期、top_n、hold_days、资金）、回测结果、资金曲线图
- 路由：`/simple-factor-backtest`

### 5. 注册路由 — `apps/dsa-web/src/App.tsx`

添加新路由和导航菜单项。

## 关键文件

- 新建：`src/discovery/factors/market_cap_factor.py`
- 新建：`api/v1/endpoints/factor_backtest.py`
- 新建：`apps/dsa-web/src/pages/SimpleFactorBacktestPage.tsx`
- 修改：`src/discovery/factors/__init__.py`
- 修改：`apps/dsa-web/src/App.tsx`（路由）
- 参考：`src/discovery/factor_backtest_engine.py`
- 参考：`api/v1/endpoints/discovery.py`（现有回测端点）
- 参考：`apps/dsa-web/src/pages/FactorBacktestPage.tsx`（现有页面）

## 验证

```bash
python -m py_compile src/discovery/factors/market_cap_factor.py
python -m py_compile api/v1/endpoints/factor_backtest.py
cd apps/dsa-web && npm run build
```
