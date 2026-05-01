# AGENTS.md

本文件用于约束本仓库的默认开发流程，目标是减少重复沟通、减少返工，并让改动和当前项目结构保持一致。

如果本文件与仓库中的脚本、工作流、代码现状不一致，以实际可执行内容为准，并在相关改动中顺手修正文档，避免规则继续漂移。

## 1. 硬规则

- 遵循现有目录边界：
  - 后端逻辑优先放在 `src/`、`data_provider/`、`api/`、`bot/`
  - Web 前端改动在 `apps/dsa-web/`
  - 桌面端改动在 `apps/dsa-desktop/`
  - 部署与流水线改动在 `scripts/`、`.github/workflows/`、`docker/`
- 未经明确确认，不执行 `git commit`、`git tag`、`git push`。
- commit message 使用英文，不添加 `Co-Authored-By`。
- 不写死密钥、账号、路径、模型名、端口或环境差异逻辑。
- 优先复用现有模块、配置入口、脚本和测试，不新增平行实现。
- 默认稳定性优先于“顺手优化”；非当前任务直接需要的重构、抽象和基础设施迁移一律克制。
- 新增配置项时，必须同步更新 `.env.example` 和相关文档。
- 涉及用户可见能力、CLI/API 行为、部署方式、通知方式、报告结构变化时，必须同步更新相关文档与 `docs/CHANGELOG.md`。
- `docs/CHANGELOG.md` 的 `[Unreleased]` 段使用**扁平格式**：每条独立一行，格式为 `- [类型] 描述`，类型取值：`新功能`/`改进`/`修复`/`文档`/`测试`/`chore`；**禁止在 `[Unreleased]` 内新增 `### 类目标题`**，以减少并发 PR 的 merge 冲突。发版时由 maintainer 汇总整理成带标题的正式格式。
- `README.md` 只用于项目定位、核心能力总览、快速开始、主要入口、赞助/合作等首页级信息；非必要不更新 README，避免持续膨胀。
- 更细的模块行为、页面交互、专题配置、排障说明、字段契约、实现语义和边界条件，优先更新对应 `docs/*.md` 或专题文档，不写入 README。
- 变更中英双语文档之一时，需评估另一份是否需要同步；若未同步，交付说明里要写明原因。
- 注释、docstring、日志文案以清晰准确为准，不强制要求英文，但应与文件语境保持一致。

## 2. AI 协作资产治理

- `AGENTS.md` 是仓库内 AI 协作规则的唯一真源。
- `CLAUDE.md` 必须是指向 `AGENTS.md` 的软链接，用于兼容 Claude 生态。
- `.github/copilot-instructions.md` 与 `.github/instructions/*.instructions.md` 是 GitHub Copilot / Coding Agent 的镜像或分层补充；若与本文件冲突，以 `AGENTS.md` 为准。
- 仓库协作 skill 存放在 `.claude/skills/`，分析产物存放在 `.claude/reviews/`；前者可以入库，后者默认视为本地产物。
- 根目录 `SKILL.md` 与 `docs/openclaw-skill-integration.md` 属于产品或外部集成说明，不是仓库协作规则真源。
- 若未来新增 `.agents/skills/` 或其他 agent 专用目录，必须先明确单一真源，再通过脚本或镜像同步；禁止手工长期维护多份同义内容。
- 修改 AI 协作治理资产时，执行：

```bash
python scripts/check_ai_assets.py
```

## 3. 仓库速览

- 项目定位：股票智能分析系统，覆盖 A 股、港股、美股。
- 主流程：抓取数据 -> 技术分析/新闻检索 -> LLM 分析 -> 生成报告 -> 通知推送。
- 关键入口：
  - `main.py`：分析任务主入口
  - `server.py`：FastAPI 服务入口
  - `apps/dsa-web/`：Web 前端
  - `apps/dsa-desktop/`：Electron 桌面端
  - `.github/workflows/`：CI、发布、每日任务
- 核心职责：
  - `src/core/`：主流程编排
  - `src/services/`：业务服务层
  - `src/repositories/`：数据访问层
  - `src/reports/`：报告生成
  - `src/schemas/`：Schema / 数据结构
  - `data_provider/`：多数据源适配与 fallback
  - `api/`：FastAPI API
  - `bot/`：机器人接入
  - `scripts/`：本地脚本
  - `.github/scripts/`：GitHub 自动化脚本
  - `tests/`：pytest 测试
  - `docs/`：文档与说明

## 4. 常用命令

### 运行应用

```bash
python main.py
python main.py --debug
python main.py --dry-run
python main.py --stocks 600519,hk00700,AAPL
python main.py --market-review
python main.py --schedule
python main.py --serve
python main.py --serve-only
uvicorn server:app --reload --host 0.0.0.0 --port 8000
```

### 后端验证

```bash
pip install -r requirements.txt
pip install flake8 pytest
./scripts/ci_gate.sh
python -m pytest -m "not network"
python -m py_compile <changed_python_files>
```

### Web / Desktop

```bash
cd apps/dsa-web
npm ci
npm run lint
npm run build

cd ../dsa-desktop
npm install
npm run build
```

### PR / CI 证据

```bash
gh pr view <pr_number>
gh pr checks <pr_number>
gh run view <run_id> --log-failed
```

## 5. 默认工作流

1. 先判断任务类型：`fix / feat / refactor / docs / chore / test / review`
2. 先读现有实现、配置、测试、脚本、工作流和文档，再动手修改。
3. 识别改动边界：后端 / API / Web / Desktop / Workflow / Docs / AI 协作资产。
4. 先判断是否命中高风险区域：配置语义、API / Schema、数据源 fallback、报告结构、认证、调度、发布流程、桌面端启动链路。
5. 只做和当前任务直接相关的最小改动，不顺手夹带无关重构。
6. 如果发现文档、脚本、工作流描述不一致，优先信任实际代码与工作流，再决定是否顺手修正文档。
7. 改完后按下面的验证矩阵执行检查。
8. 最终交付默认要说明：
   - 改了什么
   - 为什么这么改
   - 验证情况
   - 未验证项
   - 风险点
   - 回滚方式

## 6. 验证矩阵

### CI 覆盖原则

当前仓库 CI 主要包含：

| 检查项 | 来源 | 说明 | 是否阻断 |
| --- | --- | --- | --- |
| `ai-governance` | `.github/workflows/ci.yml` | 校验 `AGENTS.md` / `CLAUDE.md` / `.github` 指令 / `.claude/skills` 关系 | 是 |
| `backend-gate` | `.github/workflows/ci.yml` | 执行 `./scripts/ci_gate.sh` | 是 |
| `docker-build` | `.github/workflows/ci.yml` | Docker 构建与关键模块导入 smoke | 是 |
| `web-gate` | `.github/workflows/ci.yml` | 前端改动时执行 `npm run lint` + `npm run build` | 是（触发时） |
| `network-smoke` | `.github/workflows/network-smoke.yml` | `pytest -m network` + `test.sh quick` | 否，观测项 |
| `pr-review` | `.github/workflows/pr-review.yml` | PR 静态检查 + AI 审查 + 自动标签 | 否，辅助项 |

若 PR 上已有对应 CI 结果，可直接引用 CI 结论；若 CI 未覆盖改动面，或本地与 CI 环境差异较大，需要补充说明本地验证与缺口。

### 按改动面执行

- Python 后端改动：
  - 适用范围：`main.py`、`src/`、`data_provider/`、`api/`、`bot/`、`tests/`
  - 优先执行：`./scripts/ci_gate.sh`
  - 最低要求：`python -m py_compile <changed_python_files>`
  - 若影响 API、任务编排、报告生成、通知发送、数据源 fallback、认证、调度，交付说明中要写明是否覆盖了对应路径。

- Web 前端改动：
  - 适用范围：`apps/dsa-web/`
  - 默认执行：`cd apps/dsa-web && npm ci && npm run lint && npm run build`
  - 若涉及 API 联调、路由、状态管理、Markdown/图表渲染或认证状态，交付说明中要明确说明联动面和未覆盖风险。

- 桌面端改动：
  - 适用范围：`apps/dsa-desktop/`、`scripts/run-desktop.ps1`、`scripts/build-desktop*.ps1`、`scripts/build-*.sh`、`docs/desktop-package.md`
  - 默认执行：先构建 Web，再构建桌面端
  - 如受平台限制未能完整验证，需要明确说明是否验证了 Web 构建产物、Electron 构建以及 Release 工作流影响。

- API / Schema / 认证联动改动：
  - 适用范围：`api/**`、`src/schemas/**`、`src/services/**`、`apps/dsa-web/**`、`apps/dsa-desktop/**`
  - 至少覆盖对应后端验证 + 受影响客户端构建验证。
  - 若涉及登录、Cookie、会话、轮询状态、字段增删或枚举变化，必须明确写出兼容性影响。

- 文档与治理文件改动：
  - 适用范围：`README.md`、`docs/**`、`AGENTS.md`、`.github/copilot-instructions.md`、`.github/instructions/**`、`.claude/skills/**`
  - 不强制代码测试。
  - 需确认命令、配置项、文件名、工作流名称与实际仓库一致。
  - 改动 AI 协作治理资产时，执行 `python scripts/check_ai_assets.py`。

- 工作流 / 脚本 / Docker 改动：
  - 适用范围：`.github/**`、`scripts/**`、`docker/**`
  - 运行最接近改动面的本地验证。
  - 交付时说明影响了哪条流水线、发布路径或部署路径。
  - 若未执行 Docker / GitHub Actions 相关验证，明确说明原因与潜在风险。

- 网络或三方依赖相关改动：
  - 先跑离线或确定性检查。
  - 优先确认 timeout、retry、fallback、异常文案、降级路径是否仍然成立。
  - 若未执行在线验证，必须明确写出原因。

## 7. 稳定性护栏

- 配置与运行入口：
  - 修改 `.env` 语义、默认值、CLI 参数、服务启动方式、调度语义时，要同时评估本地运行、Docker、GitHub Actions、API、Web、Desktop 的影响。
  - 新配置优先做到“不配置也可运行，配置后增强能力”，避免叠加开关和互斥模式。

- 数据源与 fallback：
  - 修改 `data_provider/` 时，要关注数据源优先级、失败降级、字段标准化、缓存与超时策略。
  - 单一数据源失败不应拖垮整个分析流程，除非需求明确要求 fail-fast。

- API / Web / Desktop 兼容：
  - 改 API / Schema / 认证 / 报告载荷时，要同时检查后端、Web、Desktop 的兼容性。
  - 默认优先追加字段、保留旧字段或提供兼容层，避免无提示破坏现有客户端。

- 报告 / Prompt / 通知：
  - 修改报告结构、Prompt、提取器、通知模板、机器人链路时，要检查上游输入与下游消费方是否仍兼容。
  - 单一通知渠道失败不应拖垮整个分析主流程，除非需求明确要求 fail-fast。
  - 修改 `src/services/image_stock_extractor.py` 中 `EXTRACT_PROMPT` 时，要在 PR 描述中附完整最新 prompt。

- 工作流 / 发布 / 打包：
  - 修改自动 tag、Release、Docker 发布、日常分析或桌面端打包流程时，要评估触发条件、产物路径、权限边界和回滚方式。
  - 自动 tag 默认保持 opt-in：只有 commit title 含 `#patch`、`#minor`、`#major` 才触发版本号更新，除非需求明确要求改变发布策略。

## 8. Issue / PR / Skill 工作流

- 仓库内已有以下 skill，可优先复用：
  - `.claude/skills/analyze-issue/SKILL.md`
  - `.claude/skills/analyze-pr/SKILL.md`
  - `.claude/skills/fix-issue/SKILL.md`
- 如果任务明确是 issue 分析、PR 审查、issue 修复，优先按对应 skill 执行，并将产物保存到 `.claude/reviews/`。
- skill 中的命令、模板、验证顺序和交付结构必须与 `AGENTS.md` 保持一致。
- skill 默认优先读取 CI / 工作流证据，再决定是否补本地验证。
- skill 不得默认执行 `git pull`、`git push`、`git tag`、`gh pr create` 等会改变远端或当前分支状态的操作；这些操作必须要求用户确认。
- PR 审查默认顺序：
  1. 必要性
  2. 关联性
  3. 描述完整性（对照 `.github/PULL_REQUEST_TEMPLATE.md`）
  4. 验证证据
  5. 实现正确性
  6. 合入判定
- 对 `fix` 类 PR，必须说明：原问题、根因、修复点、回归风险。
- 合入阻断条件：
  - 正确性或安全性问题
  - 阻断型 CI 未通过
  - PR 描述与实际改动内容实质性矛盾
  - 缺少回滚方案

## 9. 交付与发布

- 默认交付结构：
  - `改了什么`
  - `为什么这么改`
  - `验证情况`
  - `未验证项`
  - `风险点`
  - `回滚方式`
- 如果是 `docs` 任务，可直接写：`Docs only, tests not run`，但仍需说明是否核对了命令和文件名。
- 自动 tag 默认不触发，只有 commit title 包含 `#patch`、`#minor`、`#major` 才会触发版本号更新。
- 手动打 tag 必须使用 annotated tag。
- 用户可见变更优先通过 PR 合入，并补齐 label 与验证说明。


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