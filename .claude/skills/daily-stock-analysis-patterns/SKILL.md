---
name: daily-stock-analysis-patterns
description: "daily_stock_analysis 编码模式，含全市场扫描专题页（新高/BOLL）工作流及金股页交互模式"
source_sessions:
  - 385184602_385184602's Organization_default_b962989c-d74c-4fdf-8571-5d0fcac50f01
  - 385184602_385184602's Organization_default_442b9834-07aa-41da-a5f3-2a99ed7a332d
  - 385184602_385184602's Organization_default_7e87d629-ba29-40c2-9589-7ae9340c310b
  - 385184602_385184602's Organization_default_55057f2e-18c6-44ef-8a1f-c35377165bc5
  - 385184602_385184602's Organization_default_cd5e978b-5273-4cde-8cf1-f659b489ff85
contributors:
  - 385184602
version: 3
created_by_agent: cursor
created_at: 2026-06-24T00:52:47.961Z
updated_at: 2026-06-24T07:44:50.438Z
---

# daily_stock_analysis 编码模式

## Commit 约定

优先 conventional commits（`feat:`/`fix:`/`chore:` 等）；本地分支可用中文描述性 commit。PR 合并用 merge commit；不添加 `Co-Authored-By`。

## 代码架构

分层：API → Service → Repository → DataProvider。数据源与策略用 Strategy Pattern；Multi-Agent 经 Orchestrator；配置变更同步 `src/config.py`、`src/core/config_registry.py`、`.env.example`。

## 工作流

### 添加新功能

1. 在 `src/` 对应子目录创建模块
2. 同步配置与 `.env.example`
3. `tests/test_<module>.py` + `./scripts/ci_gate.sh`
4. `docs/CHANGELOG.md` `[Unreleased]` 扁平条目

### 全市场扫描 / 行情专题页（新高、BOLL 推荐）

**When to use**：全 A 扫描 + 主表 + 展开详情 + 懒加载 K 线 + 侧栏推荐 + 跨面板联动。

**Workflow**

1. **Service** `src/services/<feature>_service.py`：扫描逻辑、5 分钟内存缓存、可选 `reports_market/` 落盘
2. **并发去重**：`scan_*` 维护 in-flight 锁/ Future，同参数并发只扫一次，其余等待缓存
3. **名称映射**：先 `stocks.index.json`，缺失条目用 `realtime_spot` 补全（北交所新股等）
4. **API** `api/v1/endpoints/market.py` + `api/v1/schemas/market.py`；
   - 全市场扫描设 120–300s timeout
   - 衍生 API（如 BOLL picks）应在主表 API 后加载，复用已 warm 缓存
5. **前端页面** `apps/dsa-web/src/pages/*Page.tsx`：
   - 主表排序与后端一致（含 tie-break，如同日按次数降序）
   - 展开行：上方列表（如创新高日期倒序网格 5-6 列），下方懒加载 K 线
   - 展开区锚点 `id="hfq-expand-{ts_code}"` + `scroll-mt-16`
6. **图表**：复用 `apps/dsa-web/src/components/charts/CandlestickMiniChart.tsx`，`overlay="boll"`（MA20±2σ）；金股页惯例：近 6 个月 BOLL，持仓期 MA5
7. **侧栏联动（BOLL 推荐）**：
   - 右侧面板固定高度与左侧表头+数据行+分页高度一致（`measureTableListHeight` 排除 `.ant-table-expanded-row`）
   - 中轨/下轨分两列，列内 `overflow-y-auto` 滚动
   - 点击推荐项 → 翻页 → 设 `expandedKey` → `requestAnimationFrame` 等 DOM 就绪 → `scrollIntoView` 锚点
8. **加载顺序**：先主表 API 再衍生 API（如 BOLL picks），避免并行触发双倍全市场扫描
9. **Dev 超时**：`apps/dsa-web` axios 与 `vite.config` `/api` 代理均设 `timeout`/`proxyTimeout: 300000`
10. **测试**：`tests/test_<feature>_service.py`；`pytest` + `npm run build`

**Anti-patterns**
- 勿并行请求两个都会冷启动全市场扫描的接口
- 勿仅用 index.json 解析名称
- merge 后 `.py` 残留 `<<<<<<<` 会导致 `uvicorn` SyntaxError；用 `git diff --diff-filter=U` 与 ripgrep 确认清零后再启动

### 金股页展开模式

- 展开个股：所选推荐月之前展示最近 6 个自然月 K 线（叠加 BOLL），再展示该月持仓期 K 线（MA5）
- 「全部金股明细」列头排序三态循环（默认→升序→降序）
- 当前月标题旁展示上月推荐×当月收益 Top5（点击名称定位，当月亦有推荐高亮）

### 修复 Bug / 添加数据源 / Agent / Factor

见既有流程：复现测试 → 最小修复 → `pytest -m "not network"`；新 Fetcher 接入 fallback chain；新 Factor 继承 `BaseFactor`。

## 测试模式

`tests/` + `pytest`；`@pytest.mark.network`；CI：`./scripts/ci_gate.sh`。

## 关键约定

不写死密钥；新增配置必同步 `.env.example`；稳定性优先，克制无关重构。

## 缓存与历史记录去重

- 全市场扫描缓存：5 分钟内存 + `reports_market/` 落盘
- 快测缓存历史：**组合去重**以（因子权重 + top_n + 持仓天数 + 初始资金 + 无风险利率）为唯一签名，排除 `start_date/end_date`；同签名以最新一条覆盖旧数据
