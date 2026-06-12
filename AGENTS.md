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
- 修改报告格式、报告渲染效果或 Web UI 界面时，PR 描述必须附受影响报告 / 页面截图；涉及前后差异时优先附前后对比，无法截图时说明原因与替代可视证据。
- Issue / PR 过程截图、审查截图、一次性验收截图和临时可视证据不得作为仓库文件合入；应放在 PR 描述、PR 评论、GitHub 附件、Actions artifact 或外部可访问证据链接中。产品长期文档确需保留的示意图除外，但文件名和文档语义必须脱离具体 issue / PR 编号。
- `docs/CHANGELOG.md` 的 `[Unreleased]` 段使用**扁平格式**：每条独立一行，格式为 `- [类型] 描述`，类型取值：`新功能`/`改进`/`修复`/`文档`/`测试`/`chore`；**禁止在 `[Unreleased]` 内新增 `### 类目标题`**，以减少并发 PR 的 merge 冲突。发版时由 maintainer 汇总整理成带标题的正式格式。
- `README.md` 只用于项目定位、核心能力总览、快速开始、主要入口、赞助/合作等首页级信息；非必要不更新 README，避免持续膨胀。
- 更细的模块行为、页面交互、专题配置、排障说明、字段契约、实现语义和边界条件，优先更新对应 `docs/*.md` 或专题文档，不写入 README。
- 变更中英双语文档之一时，需评估另一份是否需要同步；若未同步，交付说明里要写明原因。
- 注释、docstring、日志文案以清晰准确为准，不强制要求英文，但应与文件语境保持一致。

## 1.1 PR 标题规范（非阻断建议）

- 推荐使用 `<类型>: <修改内容>` 作为 PR 标题，例如 `fix: 修复大盘分析历史记录丢失`，优先类型为 `fix`/`feat`/`refactor`/`docs`/`chore`/`test`/`ci`。
- 标题应描述实际变更内容，建议不添加 `[codex]`、`codex`、`autocode`、`copilot` 或其他工具/agent 来源前缀。
- 该规范仅用于协作可读性与一致性提示，不应单独作为 review process blocker。

## 1.2 贡献质量底线

- 本仓库不接受以堆叠代码量、扩大 diff 面、补丁式响应 review 来替代真实设计收敛的 PR。
- 贡献质量以是否解决明确问题、是否最小化影响面、是否保持现有契约一致、是否覆盖真实风险路径为准；不以新增行数、文件数量、功能宣传或“看起来完整”为准。
- 请不要把本仓库当作低成本试验场、简历展示场或 contribution farming 场所。任何 PR 都必须证明作者理解当前系统契约，并完成基本自审、集成和验证。
- 使用 AI 辅助开发本身不是问题；问题是提交 AI 生成后未经人工语义审查、未验证、未收敛的代码。此类 PR 会按低质量提交处理。
- review 反馈后，不接受只在被指出的位置追加局部 patch。作者必须重新检查同一业务语义涉及的所有入口、配置、测试、文档、workflow 和用户可见路径。
- 如果一个 PR 在多轮 review 后仍持续出现同类契约漂移、重复 fallback、测试绕过真实风险层、PR body 与实际 diff 不一致等问题，维护者可以要求关闭重做，而不是继续逐点 review。

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
| `network-smoke` | `.github/workflows/network-smoke.yml` | `pytest -m network` + `scripts/test.sh quick` | 否，观测项 |
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
  3. 标题建议（`<类型>: <修改内容>`，且不含工具/agent 前缀；不作为硬性阻断项）
  4. 描述完整性（对照 `.github/PULL_REQUEST_TEMPLATE.md`）
  5. 验证证据
  6. 实现正确性
  7. 合入判定
- 对 `fix` 类 PR，必须说明：原问题、根因、修复点、回归风险。
- 合入阻断条件：
  - 正确性或安全性问题
  - 阻断型 CI 未通过
  - PR 描述与实际改动内容实质性矛盾
  - 缺少回滚方案
  - 反复出现未收敛的契约漂移、补丁堆叠或验证证据失真

## 8.1 Review 反馈处理与补丁堆叠禁止

当你处理 review 反馈时，禁止只在 reviewer 点名的位置追加局部 patch 后声称“已全部修复”。你必须先重新理解 reviewer 指出的业务契约，再检查同一语义涉及的所有入口、配置、测试、文档、workflow 和用户可见路径。

收到 review 反馈后，必须按以下顺序处理：

1. 逐条列出 reviewer 指出的原问题。
2. 说明根因，不能只描述“改了哪几行”。
3. 找出同一语义影响的所有相关路径，例如 runtime、API/Web、CLI、diagnostics、workflow、docs、tests。
4. 修复完整契约，而不是只修复当前失败测试或当前评论行。
5. 补充能覆盖 reviewer 反例的回归测试、最终入口验证，或明确说明无法验证的原因。
6. 同步更新 PR body，保证 scope、验证结果、兼容性、风险和回滚方案与当前 head 一致。

如果你无法完成上述收敛，不要继续堆叠补丁，不要声称 ready for merge。应主动说明当前 PR 需要拆分、关闭重做，或请求维护者确认新的最小范围。

以下行为会被视为低质量 PR：

- 用 broad fallback、静默降级、`return False/None/[]` 掩盖不清晰的契约。
- 测试 mock 掉真实风险层，只证明局部实现通过。
- CI 通过后声称问题已关闭，但没有覆盖 reviewer 指出的反例。
- PR body 与实际 diff、验证结果或兼容风险不一致。
- review 后继续追加零散 patch，而不是重新收敛完整语义。
- 同一业务语义在 runtime、Web/API、docs、workflow、tests 中表现不一致。

CI 通过只能说明自动检查通过，不能替代人工语义收敛，也不能单独证明 reviewer 指出的反例已经关闭。

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
- 金股页表格：名称与代码同列（名称在上），标签独立列换行展示，无标签显示 `--`；不以红色背景高亮历史高频推荐；历史统计「最低」正收益也用红色（≥0 红，<0 绿）。展开个股：所选推荐月之前展示最近 6 个自然月 K 线，再展示该月持仓期 K 线；月末价/九转/盈利预测/累计收益与所选月份一致；九转反转按所选信号日展示（无信号空表），升转降/降转升分左右两列表。全部金股明细不展示「推荐数」列；查看历史月份时在累计收益后展示当前自然月累计收益列（当前月不显示）。
- Web 导航隐藏 portfolio/backtest/alerts 页面；首页「大盘复盘历史」无记录时不展示。

## Learned Workspace Facts

- 多因子组合 YAML 存放在 `combos/`；六基准因子加业绩因子，支持 3/4/5 因子组合搜索与权重微调优化。
- 因子组合搜索与权重优化报告分别输出到 `reports_simple_backtest/` 与 `reports_discovery/factor_optimization/`。
- 快测历史记录按累计收益降序排列；交叉验证每个组合保留 Top 3。
- 首页批量/临时分析报告目录为 `reports_temp/`（`HOME_ANALYSIS_REPORTS_DIR`）。
- 多 Agent 复核在定时批跑结束后触发，结果写入 `reports_multi_agent/`。
- 新闻检索 Provider 优先级：Bocha > MiniMax > Anspire > Tavily > Brave > SerpAPI > SearXNG。
- `AGENT_SKILLS` 采用 4–6 策略精简方案，非全开所有策略。
- 券商金股 API 前缀 `/api/v1/broker-recommend/`；月度表 `broker_recommend_monthly`；当前月 enrichment 全 0 九转视为缓存未命中。九转反转列表 `/{month}/up-to-down-daily`：升 1..8 转降/降 1..8 转升，月初第 1 日与上月最后交易日对比九转，历史推荐月信号扫描延至今日（推荐月末交易日仍忽略）；前端左右分列表、信号日可选推荐月首日至今日。策略回测 `nineturn_up_to_down_open`（缓存 v49）：总资金固定；升 1..8 转降有效（升 9+ 忽略），当日 N 股均摊、T+1 开盘买；T+2 开盘亏损卖/盈利则 T+3 起收盘跟踪（T+3 收盘超买入日收盘价继续持有，直至收盘低于买入开盘价卖出），月末最后交易日收盘强制清仓（无行情顺延开盘清仓，最多 20 日、可跨月），末交易日信号忽略；无信号日 T+1 开盘清仓后暂停（盈利跟踪持仓除外）；策略与反转判定均跨月连续；交易记录仅限当月；总收益=结算资产/固定总资金-1；交易记录含买卖额；策略 Tab 含 `up_to_down_stats` 升转降分档统计与策略月度表「月最佳信号」；展开行不复权成交价；可选月份范围并「重新计算」；买卖日 tooltip 展示交易理由；前端 timeout 120s。
- 筹码因子 `chip`：`winner_rate` 以 85% 为峰向两侧递减；`deep` 满分 9 分且需 `dist_low`/成本确认。
- 因子快照回填脚本默认每 50 个交易日执行 `wal_checkpoint(TRUNCATE)`，避免 WAL 占满磁盘。
- 快测/寻股回测交易记录含 `pick_rank`（1..top_n，递补买入为 0），用于顺位收益贡献统计。
- 金股页历史统计接口 `/api/v1/broker-recommend/historical-recommend-stats` 基于持仓期 `daily_returns` + 批量 `adj_factor` 快速聚合，前端请求超时 120s；历史月份「全部金股明细」另批量拉取 `/api/v1/broker-recommend/current-month-returns` 展示当前自然月累计收益（后复权，月初首日至有效截止日）。



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

This project is indexed by GitNexus as **daily_stock_analysis** (23793 symbols, 37568 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

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
