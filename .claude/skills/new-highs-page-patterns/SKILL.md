---
name: new-highs-page-patterns
description: "新高页（全市场新高扫描 + BOLL 推荐侧栏联动）完整工作流：全 A 后复权扫描 → 主表/展开/BOLL 推荐 → 跨面板联动"
trigger: "涉及 hfq_new_high_service、HfqNewHighPage、BOLL 推荐、右侧侧栏与左侧主表联动定位"
author: 385184602
source_sessions:
  - 385184602_385184602's Organization_default_7e87d629-ba29-40c2-9589-7ae9340c310b
contributors:
  - 385184602
version: 1
created_by_agent: codex
created_at: 2026-06-24T07:48:23.032Z
updated_at: 2026-06-24T07:48:23.032Z
---

# 新高页（全市场新高 + BOLL 推荐）模式

**When to use**：全 A 后复权创新高扫描 + 主表 + 展开详情 + 懒加载 K 线 + 右侧 BOLL 推荐面板 + 跨面板联动定位。

## Workflow

1. **Service** `src/services/hfq_new_high_service.py`：扫描全 A 股 2026 至今 `close × adj_factor` 创新高；倒序输出 `new_high_dates`；5 分钟内存缓存 + `reports_market/` 落盘
2. **并发去重**：`scan_new_highs` 维护 in-flight Future，同参数并发只扫一次，其余等待同一缓存
3. **名称映射**：先 `stocks.index.json`，缺失条目用 `realtime_spot` 补全（覆盖北交所新股等）
4. **API** `api/v1/endpoints/market.py` + `api/v1/schemas/market.py`：
   - `GET /api/v1/market/hfq-new-highs` 全市场新高列表，设 120–300s timeout
   - `GET /api/v1/market/hfq-new-highs/{code}/klines` 个股后复权 K 线
   - `GET /api/v1/market/hfq-new-highs/boll-picks` BOLL 推荐（复用已 warm 的新高缓存，避免并行触发双倍全量扫描）
5. **前端页面** `apps/dsa-web/src/pages/HfqNewHighPage.tsx`：
   - 主表按 `(最近新高日 ↓, 次数 ↓)` 排序，与后端定序一致
   - 展开行：上方为「2026 至今创新高记录（倒序）」5 列网格（序号用 `text-primary/85` 不换行），下方懒加载 BOLL K 线（`overlay="boll"`）
   - 展开区锚点 `id="hfq-expand-{ts_code}"` + `scroll-mt-16`
6. **BOLL 推荐侧栏联动**：
   - 右侧面板固定高度与左侧表头+数据行+分页高度一致（`measureTableListHeight` 排除 `.ant-table-expanded-row`）
   - 中轨/下轨分两列，列内 `overflow-y-auto` 滚动
   - 筛选：距中轨/下轨 ≤ 2%、距新高 ≤ 20%，按月新高日期降序排列
   - 金股高亮：当月青色+标签、上月琥珀色+标签
   - 点击推荐项 → 翻页 → 设 `expandedKey` → `requestAnimationFrame` 等 DOM 就绪 → `scrollIntoView` 锚点
7. **加载顺序**：先主表 API 再 BOLL 推荐 API，避免并行触发双倍全市场扫描
8. **Dev 超时**：`apps/dsa-web` axios 与 `vite.config` `/api` 代理均设 `timeout`/`proxyTimeout: 300000`

## Anti-patterns
- 勿并行请求两个都会冷启动全市场扫描的接口（主表 + BOLL 推荐先后加载）
- 勿仅用 index.json 解析名称，新股/北交所条目用 `realtime_spot` 兜底
- merge 后 `.py` 残留 `<<<<<<<` 会导致 `uvicorn` SyntaxError；用 `git diff --diff-filter=U` 与 ripgrep 确认清零后再启动
