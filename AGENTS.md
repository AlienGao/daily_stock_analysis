# AGENTS.md

本文件是本仓库 AI 协作规则的唯一真源，目标是减少返工并保持改动与项目契约一致。如本文件与可执行代码/脚本/工作流不一致，以实际可执行内容为准，并在相关改动中顺手修正文档。

## 1. 硬规则

- 遵循目录边界：后端 `src/`、`data_provider/`、`api/`、`bot/`；Web `apps/dsa-web/`；桌面端 `apps/dsa-desktop/`；部署与流水线 `scripts/`、`.github/workflows/`、`docker/`。
- 未经明确确认，不执行 `git commit`、`git tag`、`git push`；commit message 使用英文，不添加 `Co-Authored-By`。
- 不写死密钥、账号、路径、模型名、端口或环境差异逻辑；优先复用现有模块、配置入口、脚本和测试，不新增平行实现。
- 默认稳定性优先，克制与当前任务无关的重构；禁止在仓库工作树内创建 `git worktree`（临时 worktree 放仓库外目录）。
- 新增或变更配置项/环境变量时，必须同步 `.env.example` 与相关文档，并评估本地、Docker、GitHub Actions、API、Web、Desktop 影响。
- 涉及用户可见能力、CLI/API 行为、部署方式、通知方式、报告结构变化时，必须同步更新相关 `docs/*.md` 与 `docs/CHANGELOG.md`：
  - `docs/CHANGELOG.md` 的 `[Unreleased]` 段使用**扁平格式**：每条一行 `- [类型] 描述`（类型取值：`新功能`/`改进`/`修复`/`文档`/`测试`/`chore`），**禁止新增 `### 类目标题`**。
  - `README.md` 仅承载首页级信息（定位、核心能力总览、快速开始、主要入口、赞助/合作），细则写入 `docs/*.md`。
  - 修改中英双语文档之一时，评估另一份是否同步；未同步须在交付说明中写明原因。
- 修改报告格式、报告渲染或 Web UI 界面时，PR 描述须附受影响报告/页面截图（涉及前后差异优先附前后对比），无法截图时写明原因与可复现替代证据；一次性验收截图不得作为仓库文件合入。
- 注释、docstring、日志文案以清晰准确为准，与文件语境保持一致。

### 1.1 PR 标题规范（非阻断建议）

- 推荐 `<类型>: <修改内容>`（`fix`/`feat`/`refactor`/`docs`/`chore`/`test`/`ci`），不添加 `[codex]`、`codex`、`autocode`、`copilot` 等工具前缀；仅作协作提示，不作为合入阻断项。

### 1.2 贡献质量底线与 Review 反馈收敛

- 贡献质量以解决明确问题、最小化影响面、保持契约一致、覆盖真实风险路径为准，不接受堆叠补丁、broad fallback/静默降级、或 mock 掉真实风险层的测试。
- 收到 review 反馈后，禁止只在评论行打局部 patch：须逐条列出原问题与根因，排查同一业务语义涉及的全部入口（runtime、API/Web、CLI、docs、workflow、tests），修复完整契约并补回归测试，同时保持 PR 描述与实际 diff 一致。CI 通过不能替代人工语义收敛。

## 2. AI 协作资产治理

- 规则真源：根目录 `AGENTS.md`。
- Claude 兼容入口：根目录 `CLAUDE.md`（指向 `AGENTS.md` 的软链接）与 `.claude/skills`（包含与 `.agents/skills/` 同步的兼容副本；支持软链接的平台也可使用指向 `../.agents/skills` 的软链接）。Windows 使用 `core.symlinks=false` 时直接读取 `.claude/skills/` 副本，不依赖创建软链接。
- 仓库协作 skill 真源：`.agents/skills/`（含 `analyze-pr`、`analyze-issue`、`fix-issue`，须带 YAML frontmatter 并引用 `AGENTS.md`）。
- Copilot 入口：`.github/copilot-instructions.md` 与 `.github/instructions/*.instructions.md`；如与本文件冲突，以 `AGENTS.md` 为准。
- 本地审查/分析产物：**默认直接输出在对话中，不落盘**；仅当用户明确要求存档时写入 `.reviews/`（已加入 `.gitignore`）。
- 根目录 `SKILL.md` 与 `docs/openclaw-skill-integration.md` 属于产品外部集成说明，不是仓库协作治理真源。
- 修改 AI 协作治理资产后，必须执行：`python scripts/check_ai_assets.py`。

## 3. 仓库速览与常用命令

- 定位：覆盖 A 股、港股、美股、日股、韩股、台股的股票智能分析系统（抓取数据 -> 技术/新闻分析 -> LLM 分析 -> 报告 -> 推送）。
- 入口：`main.py`（分析/CLI）、`server.py`（FastAPI）、`apps/dsa-web/`（Web）、`apps/dsa-desktop/`（Electron）。

```bash
# 运行
python main.py [--debug | --dry-run | --stocks 600519,hk00700,AAPL | --market-review | --schedule | --serve | --serve-only | --doctor]
uvicorn server:app --reload --host 0.0.0.0 --port 8000

# 验证（Windows 运行 ci_gate.sh 需 Git Bash / WSL）
./scripts/ci_gate.sh
python -m pytest -m "not network"
python -m py_compile <changed_python_files>
cd apps/dsa-web && npm ci && npm run lint && npm run build
cd apps/dsa-desktop && npm install && npm run build
python scripts/check_ai_assets.py
```

## 4. 默认工作流

1. 判断任务类型与改动边界（后端 / API / Web / Desktop / Workflow / Docs / AI 治理），识别高风险区域（配置语义、API/Schema、数据源 fallback、报告结构、认证、调度、发布、桌面启动链路）。
2. 先读现有实现、配置、测试、工作流与文档，再做最小改动；文档与实际代码冲突时优先信可执行代码。
3. 按 §5 验证矩阵检查，并按 §8 交付结构输出结果。

## 5. 验证矩阵

| CI 作业名 | 来源 | 说明 | 阻断 |
| --- | --- | --- | --- |
| `🔎 Change Detection` | `.github/workflows/ci.yml` | 路径变更检测 | 是 |
| `ai-governance` | `.github/workflows/ci.yml` | `python scripts/check_ai_assets.py` | 是 |
| `backend-tests (1/3)`..`(3/3)` | `.github/workflows/ci.yml` | 语法、flake8、确定性检查与 3 分片离线 pytest | 是（触发时） |
| `backend-gate` | `.github/workflows/ci.yml` | 汇总前置与后端测试门禁 | 是 |
| `docker-build` | `.github/workflows/ci.yml` | Docker 构建与关键模块导入 smoke | 是（触发时） |
| `web-gate` | `.github/workflows/ci.yml` | `npm run lint` + `npm run build` + Playwright smoke | 是（触发时） |
| `desktop-futu-package-windows` / `desktop-futu-package-macos` | `.github/workflows/ci.yml` | 桌面端 Futu 打包验证 | 是（触发时） |
| `network-smoke` | `.github/workflows/network-smoke.yml` | `pytest -m network` + `scripts/test.sh quick` | 否 |
| `pr-review` | `.github/workflows/pr-review.yml` | 手动触发的只读静态检查 + AI 审查 | 否 |

按改动面执行本地检查：
- Python 后端：优先 `./scripts/ci_gate.sh`，最低 `python -m py_compile` + 最近确定性测试。
- Web / Desktop：Web 执行 `npm run lint && npm run build`；Desktop 先构建 Web 再构建 Electron，受平台限制未验证项须明确写出。
- API / Schema / 认证：后端验证 + 受影响客户端构建，并明确写出字段/会话兼容性影响。
- AI 治理与文档：治理资产改动须跑 `python scripts/check_ai_assets.py`；纯文档改动需核对命令与文件名与仓库一致。
- 网络或三方依赖：先跑离线/确定性检查，确认 timeout、retry、fallback 与降级路径，未做在线验证须说明原因。

## 6. 稳定性护栏

- 配置：新配置优先做到“不配置也可运行，配置后增强能力”，避免叠加互斥开关。
- 数据源与通知：单一数据源或单一通知渠道失败不应拖垮整个分析主流程（除非明确要求 fail-fast）。
- 客户端兼容：改 API / Schema / 认证 / 报告载荷时优先追加字段、保留旧字段或提供兼容层，同步评估 Web 与 Desktop。
- `EXTRACT_PROMPT`：修改 `src/services/image_stock_extractor.py` 中 `EXTRACT_PROMPT` 时，PR 描述须附完整最新 prompt。
- 发布：自动 tag 默认 opt-in（仅 commit title 含 `#patch` / `#minor` / `#major` 触发）；手动 tag 须用 annotated tag。

## 7. Issue / PR / Skill 工作流

- 仓库内置 skill：`.agents/skills/analyze-pr/SKILL.md`、`.agents/skills/analyze-issue/SKILL.md`、`.agents/skills/fix-issue/SKILL.md`；`.claude/skills/` 保留同步副本，兼容 Claude Code。
- 每次 PR 创建/更新、PR 审查或 Issue 分析前，必须先刷新基线：检查 `git status --short` 并执行 `git fetch --all --prune`；**仅当**工作区干净且当前分支可 fast-forward 时执行 `git pull --ff-only`。否则不得强行切分支、stash、reset 或覆盖本地状态，改用已 fetch 的远端 refs/PR head 分析并记录本地 HEAD 与远端基线。
- 除上述安全 fast-forward 同步外，不得未经确认执行 `git pull`、`git push`、`git tag`、`gh pr create/merge/close`。
- PR 审查遵循 `.agents/skills/analyze-pr/SKILL.md`（文件覆盖账本 + 14 维影响面清单 + 因果链 + 描述与 diff 核对）。
- **合入阻断条件**（仅限以下情形）：
  - 正确性或安全性问题
  - 阻断型 CI 未通过
  - PR 描述与实际 diff 存在实质性矛盾
  - 存在不可逆变更（持久化/配置自动改写/外部状态写入/发布策略）但未说明恢复路径
  - 反复出现未收敛的契约漂移、补丁堆叠或验证证据失真

## 8. 默认交付结构

交付结果直接聚焦事实与影响，禁止空泛套话：
- `改了什么`（新增文件 vs 修改的既有运行时文件，带 `path:line`）
- `行为变化`（改前 vs 改后，谁会感知）
- `影响面`（是否涉及配置/环境变量、API/CLI、UI/Web/Desktop、报告/通知、CI/Docker）
- `验证情况与未验证项`
- `不可逆变更`（仅在存在持久化/配置迁移/外部状态写入时展开恢复路径；否则写“无”）
- `docs` 任务可直接写 `Docs only, tests not run`，但仍需说明是否核对了命令与文件名。
- 用户可见变更优先通过 PR 合入，并补齐 label 与验证说明。

## Learned User Preferences

- 与用户沟通默认使用中文。
- 资金曲线 tooltip 展示收益率（%）而非金额，并展示从 hover/高亮日到最新交易日的区间收益。
- 前端展示不复权价格；盈亏、回测与金股页持仓期/历史统计收益需使用 Tushare 后复权价格（`adj_factor`）。
- 快测回测：胜率与交易次数基于已平仓记录；累计收益与夏普基于含未平仓的最新持仓。
- 大规模批量回测或因子搜索前，先小范围验证逻辑再全量跑。
- 因子权重优化默认优先提收益：回撤 slack +3pp，目标函数用 `min(两区间 ret5)`。
- 定时任务（含「立即分析」）默认关闭 LLM 深度思考；手动单股分析开启深度思考。
- 多因子搜索完成后，前端快捷组合列表应即时更新，无需手动刷新页面。
- 多持仓回测展示「选股顺位收益贡献」表（递补不计入）：快测/LGB/因子回测 `top_n>1` 时按 Top1~TopN；寻股固定 Top1~Top4 且表在交易记录上方。快测页选中历史记录时因子卡片默认回填对应组合。
- 寻股页回测交易记录：持仓中展示实时收益%与盈亏（卖出价列为现价），有持仓时每 30 秒自动刷新。
- 金股页表格：名称与代码同列（名称在上），标签独立列换行展示，无标签显示 `--`；不以红色背景高亮历史高频推荐；历史统计「最低」正收益也用红色（≥0 红，<0 绿）。展开个股：所选推荐月之前展示最近 6 个自然月 K 线（叠加 BOLL），再展示该月持仓期 K 线（MA5）；月末价/九转/盈利预测/累计收益与所选月份一致；九转反转按所选信号日展示（无信号空表），升转降/降转升分左右两列表。全部金股明细不展示「推荐数」列；表格列头排序三态循环（默认→升序→降序）；查看历史月份时在累计收益后展示当前自然月累计收益列（当前月不显示）；当前月标题旁展示上月推荐×当月收益 Top5（点击名称定位，当月亦有推荐高亮）。
- Web 导航隐藏 portfolio/backtest/alerts 页面；首页「大盘复盘历史」无记录时不展示。

## Learned Workspace Facts

- 多因子组合 YAML 在 `combos/`（六基准+业绩因子，3/4/5 因子搜索与权重微调）；搜索报告 `reports_simple_backtest/`，权重优化 `reports_discovery/factor_optimization/`。
- 新高页 `/new-highs`：`hfq_new_high_service` 扫描全 A 股 2026 至今后复权收盘价创新高（`close×adj_factor`）；API `/api/v1/market/hfq-new-highs` 与 `/{code}/klines`；主表按最近新高日降序、同日按次数降序；展开含创新高日期倒序列表 + 2026 至今后复权日 K（BOLL，懒加载）；日级缓存 `reports_market/`。
- 快测历史记录按累计收益降序排列；交叉验证每个组合保留 Top 3。
- 首页批量/临时分析报告目录为 `reports_temp/`（`HOME_ANALYSIS_REPORTS_DIR`）；多 Agent 复核在定时批跑结束后触发，结果写入 `reports_multi_agent/`。
- 港股通监控页 `/hk-monitor`（导航「港股」）：`hk_ggt_monitor_service` + API `/api/v1/market/hk-ggt/*`；成份 `hk_ggt_component`、分钟 `hk_ggt_minute_bar`（AkShare 回填 + Tushare `rt_hk_k` 轮询，默认 60s）；前端成份表 + 展开 recharts 曲线，30 秒只读库刷新；盘中为轮询快照非交易所原生 1 分钟 K。
- 新闻检索 Provider 优先级：Bocha > MiniMax > Anspire > Tavily > Brave > SerpAPI > SearXNG。
- `AGENT_SKILLS` 采用 4–6 策略精简方案，非全开所有策略。
- 券商金股 API 前缀 `/api/v1/broker-recommend/`；月度表 `broker_recommend_monthly`；当前月 enrichment 全 0 九转视为缓存未命中；当月 `/{month}/backtest` 实时计算约 17–27s 且无缓存，历史月有缓存；前端 `getBacktest` 超时 120s，页面加载用 `Promise.allSettled` 避免回测失败整页空白。九转反转列表 `/{month}/up-to-down-daily`：升 1..8 转降/降 1..8 转升，月初第 1 日与上月最后交易日对比九转，历史推荐月信号扫描延至今日（推荐月末交易日仍忽略）；前端左右分列表、信号日可选推荐月首日至今日。策略回测 `nineturn_up_to_down_open`（缓存 v49）：总资金固定；升 1..8 转降有效（升 9+ 忽略），当日 N 股均摊、T+1 开盘买；T+2 开盘亏损卖/盈利则 T+3 起收盘跟踪（T+3 收盘超买入日收盘价继续持有，直至收盘低于买入开盘价卖出），月末最后交易日收盘强制清仓（无行情顺延开盘清仓，最多 20 日、可跨月），末交易日信号忽略；无信号日 T+1 开盘清仓后暂停（盈利跟踪持仓除外）；策略与反转判定均跨月连续；交易记录仅限当月；总收益=结算资产/固定总资金-1；交易记录含买卖额；策略 Tab 含 `up_to_down_stats` 升转降分档统计与策略月度表「月最佳信号」；展开行不复权成交价；可选月份范围并「重新计算」；买卖日 tooltip 展示交易理由；前端 timeout 120s。
- 筹码因子 `chip`：`winner_rate` 以 85% 为峰向两侧递减；`deep` 满分 9 分且需 `dist_low`/成本确认。
- 因子快照回填脚本默认每 50 个交易日执行 `wal_checkpoint(TRUNCATE)`，避免 WAL 占满磁盘。
- 快测/寻股回测交易记录含 `pick_rank`（1..top_n，递补买入为 0），用于顺位收益贡献统计。
- 金股页历史统计接口 `/api/v1/broker-recommend/historical-recommend-stats` 基于持仓期 `daily_returns` + 批量 `adj_factor` 快速聚合，前端请求超时 120s；历史月份「全部金股明细」另批量拉取 `/api/v1/broker-recommend/current-month-returns` 展示当前自然月累计收益（后复权，无当日精确 `adj_factor` 时截止上一交易日）；当前月另拉取 `/api/v1/broker-recommend/prev-month-current-top?top_n=5` 展示上月推荐×当月收益 Top5。



<claude-mem-context>
# Memory Context

# [daily_stock_analysis] recent context, 2026-05-01 4:36pm GMT+8

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision 🚨security_alert 🔐security_note
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 50 obs (16,438t read) | 833,869t work | 98% savings

### May 1, 2026
S119 Discovery page backtesting implementation complete. User now planning Phase 2 enhancements: richer backtest data (capital curve, trade records, date filtering) and frontend chart visualization (profit curve with recharts). Three new tasks created. (May 1 at 9:01 AM)
S120 Add backtesting UI to discovery page — Phase 1 complete (scanner JSON archiving, backtest engine, API endpoint, frontend component), Phase 2 planned (richer data + chart visualization). (May 1 at 9:01 AM)
S122 Add backtesting (回测) to the discovery page with intraday and postmarket modes, including capital curves, trade records, date filtering, and chart visualization (May 1 at 9:02 AM)
S123 Add backtesting (回测) to the discovery page with intraday and postmarket modes — capital curves, trade records, date filtering, and chart visualization (May 1 at 9:07 AM)
S124 Add backtesting (回测) to the discovery page with intraday and postmarket modes — capital curves, trade records, date filtering, and chart visualization (May 1 at 9:20 AM)
S125 Add backtesting (回测) to the discovery page with intraday and postmarket modes — capital curves, trade records, date filtering, and chart visualization (May 1 at 9:21 AM)
S126 Add intraday and postmarket backtesting functionality to the discovery page with Phase 2 enhancements including capital curve, trade records, date filtering, and frontend chart visualization. (May 1 at 9:21 AM)
S127 Add intraday and postmarket backtesting to the discovery page with capital curve, trade records, date filtering, and recharts chart visualization. Phase 2 enhancements for richer backtest data display. (May 1 at 9:23 AM)
S136 Evaluate feasibility of integrating Microsoft RD-Agent into the daily_stock_analysis project (May 1 at 9:25 AM)
289 3:59p 🔵 Discovered RD-Agent-Quant NeurIPS 2025 paper with factor-model co-optimization results
290 4:00p 🔵 RD-Agent deeply integrated with Qlib — incompatible Python version with existing project
291 " 🔵 Existing project has custom multi-agent architecture with 5 specialized agents
292 " 🔵 StockDiscoveryEngine provides extensible factor-based discovery with 9 registered factors
293 " 🔵 Data provider uses strategy pattern with 8 fetchers and automatic fallback chain
294 " 🔵 StockAnalysisPipeline coordinates complete analysis workflow with 3-letter service integration
295 4:01p 🔵 Deep-dive revealed sophisticated LLM output parsing with negation-aware trend inference
296 " 🔵 Tool registry supports multi-provider schema generation for OpenAI/Anthropic compatibility
297 " 🔵 Pipeline implements comprehensive graceful degradation across all data sources
298 " 🔵 YAML-based natural language trading strategy system with 12 portable strategy files
299 " 🔵 StockTrendAnalyzer implements 7-level trend classification with MACD/RSI/Bollinger analysis
300 4:02p 🔵 Microsoft RD-Agent supports Dockerless execution via CondaConf and LocalEnv
301 " 🔵 daily_stock_analysis core architecture revealed for RD-Agent integration assessment
302 " 🔵 RD-Agent LLM backend has known DeepSeek/Ollama provider bug in v0.6.x
303 " 🔵 RD-Agent supports custom data integration via DataScienceScen and pluggable pipeline components
304 4:03p 🔵 RD-Agent RDLoop architecture revealed from source: propose→code→run→feedback cycle
305 " 🔵 RD-Agent configuration uses env_prefix-isolated Pydantic BaseSettings per scenario
306 " 🔵 RD-Agent RDLoop subclasses override minimal methods while inheriting propose→code→run→feedback cycle
307 " 🔵 RD-Agent Scenario is a prompt-constructing object, not just data source abstraction
308 " 🔵 Stock analysis project architecture mapped for RD-Agent evaluation
309 " 🔵 RD-Agent FactorCoSTEERSettings architecture examined
314 " 🔵 RD-Agent internal architecture: five-unit framework with bandit scheduling
315 " 🔵 RD-Agent Qlib coupling depth mapped for decoupling assessment
316 " 🔵 DeepSeek workaround and Docker fallback paths identified
317 " ⚖️ RD-Agent integration plan: recommend architecture-borrowing over full integration
318 4:05p ⚖️ Evaluating Microsoft RD-Agent for project integration
310 " 🔵 RD-Agent known bug with non-OpenAI LLM providers in 0.6.x
311 " 🔵 RD-Agent(Q) quant finance scenario architecture documented
312 " 🔵 RD-Agent uses Docker/Conda env isolation and CoSTEER code generation
313 " ⚖️ Integration gaps identified between existing project and RD-Agent
319 4:07p ⚖️ RD-Agent integration feasibility assessed for daily_stock_analysis
320 4:17p 🟣 R&D loop CLI integration task created
321 " 🟣 R&D loop implementation decomposed into 4 sub-tasks
322 " ⚖️ R&D loop task dependency graph established
323 " 🔵 Existing project infrastructure reviewed for R&D loop integration
324 4:18p 🔵 BaseFactor and factor implementation patterns examined
325 " 🔵 LLMToolAdapter call_text usage mapped across the codebase
326 " 🟣 Factor evaluator implementation started
327 " 🔵 StockRepo price query methods found for factor evaluator
328 4:19p 🟣 FactorEvaluator created — backtest-driven feedback for R&D loop
329 4:22p 🟣 Task 2 completed — factor_evaluator.py verified and done
330 " 🟣 FactorCoder created — LLM-driven factor code generation
331 4:24p 🟣 Tasks 2 and 3 completed — R&D loop leaf components done
332 " 🟣 Task 4 started — RDLoop orchestrator now in progress
333 4:26p 🟣 RDLoop orchestrator created — completes the R&D loop core
334 " 🟣 Task 4 completed — RDLoop orchestrator verified via py_compile
335 " 🟣 R&D Factor Discovery Loop CLI wired and compiled
336 4:28p 🔵 R&D loop modules pass regression — 1 pre-existing test failure confirmed
337 4:31p ✅ CHANGELOG updated with R&D loop entry
338 4:34p 🔵 .env.example lacks R&D loop configuration entries
S137 Implement RD-Agent-inspired R&D factor discovery loop into daily_stock_analysis project — CLI flag `--rd-loop` with automated hypothesis generation, factor code generation, historical backtest evaluation, SOTA tracking, and iterative refinement. (May 1 at 4:34 PM)
**Investigated**: Examined existing main.py CLI pattern (argparse, mode dispatch with early return), existing TushareFetcher at data_provider/tushare_fetcher.py, LLMToolAdapter at src/agent/llm_adapter.py (LiteLLM Router with is_available property), base factor classes, and the CHANGELOG format. Confirmed FactorEvaluator AST sandbox blocks dangerous imports, FactorCoder generates BaseFactor subclasses, and RDLoop orchestrator manages the full iteration loop with state persistence and report generation.

**Learned**: The project's CLI uses a mode-based dispatch pattern with `getattr(args, 'flag', False)` for hypenated flags and early `return 0` for exclusive modes. Lazy imports are the convention for mode-specific dependencies. The existing CHANGELOG keeps [Unreleased] entries as flat lines under a single comment block. pytest requires `pytest-timeout` plugin separately — the project doesn't bundle it. The test suite has 1115+ non-network tests with one pre-existing failure in test_portfolio_api.

**Completed**: All 4 implementation tasks completed and verified: (1) CLI wiring in main.py with --rd-loop, --rd-loop-iterations, --rd-loop-hypotheses, (2) FactorEvaluator with AST sandbox, Rank IC scoring, and SOTA tracking, (3) FactorCoder with LLM-based code generation and safety validation, (4) RDLoop orchestrator with iteration loop and markdown leaderboard output. All files pass py_compile. Import smoke tests pass. 1115 existing tests pass with zero regressions. CHANGELOG updated with feature entry.

**Next Steps**: Session appears complete — no further work is actively planned. The user can run `python main.py --rd-loop` to execute the loop with a configured LLM and Tushare token. Potential follow-ups include adding .env.example entries for discoverability, writing dedicated unit tests for the new modules, or iterating on the factor scoring formula based on real-world results.


Access 834k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **daily_stock_analysis** (82096 symbols, 113066 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/daily_stock_analysis/context` | Codebase overview, check index freshness |
| `gitnexus://repo/daily_stock_analysis/clusters` | All functional areas |
| `gitnexus://repo/daily_stock_analysis/processes` | All execution flows |
| `gitnexus://repo/daily_stock_analysis/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
