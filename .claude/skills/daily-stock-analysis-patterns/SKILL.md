---
name: daily-stock-analysis-patterns
description: Coding patterns extracted from daily_stock_analysis repository
version: 1.0.0
source: local-git-analysis
analyzed_commits: 200
generated: 2026-05-01
---

# daily_stock_analysis 编码模式

## Commit 约定

本项目使用**混合型 commit 约定**（来自 200 个 commit 的分析）：

### Conventional Commits（117/200，58.5%）

- `feat:` — 新功能（如 `feat: Add Hong Kong market support (#1068)`）
- `fix:` — Bug 修复（如 `fix: sanitize conflicting trend signals before analysis prompt`）
- `chore:` — 维护任务（如 `chore(release): consolidate docker publish workflows`）
- `docs:` — 文档更新（如 `docs: streamline README documentation`）
- `refactor:` — 重构（如 `refactor: 优化飞书配置和文档`）
- `test:` — 测试变更（如 `test: align market recap prompt assertion`）
- `release:` — 版本发布（如 `release: prepare v3.14.0`）
- `ci:` — CI/CD 变更（如 `ci: ignore vendored node modules in lint config`）

### 中文描述性 Commit（27/200，13.5%）

- `添加*` — 添加功能/配置（如 `添加类RD-Agent因子自动挖掘 技术指标切换为tushare`）
- `修复*` — 修复问题（如 `修复session不隔离情况`）
- `修改*` — 修改行为（如 `修改report匹配逻辑 更新日报`）
- `更新*` — 更新内容（如 `日报更新`）
- `集成*` — 集成功能

**规则**：
- 优先使用 conventional commit 格式用于跨团队协作
- 本地开发/个人分支可使用中文描述性 commit
- Commit message 使用英文（per CLAUDE.md）
- 不添加 `Co-Authored-By`（per CLAUDE.md）
- PR 合并使用 merge commit（非 squash）
- 自动 tag 仅当 commit title 包含 `#patch`、`#minor`、`#major` 时触发

## 代码架构

```
.
├── api/                    # FastAPI API 层
│   ├── middlewares/        # 中间件（CORS、认证等）
│   └── v1/
│       ├── endpoints/      # API 路由处理器
│       └── schemas/        # Pydantic 请求/响应模型
├── apps/                   # 客户端应用
│   ├── dsa-web/            # Web 前端（React/TypeScript）
│   └── dsa-desktop/        # Electron 桌面端
├── bot/                    # 机器人集成
│   ├── commands/           # 命令定义
│   └── platforms/          # 平台适配器（飞书、Discord、Telegram、Slack）
├── data_provider/          # 多数据源适配层（Strategy Pattern）
├── scripts/                # CI/CD 和工具脚本
├── src/
│   ├── agent/              # 多 Agent 架构
│   │   ├── agents/         # 5 个专业 Agent
│   │   ├── skills/         # Agent 技能
│   │   ├── strategies/     # 策略系统
│   │   └── tools/          # Agent 工具
│   ├── core/               # 核心分析流水线编排
│   ├── data/               # 数据层
│   ├── discovery/          # 股票发现引擎
│   │   └── factors/        # 9+ 注册因子
│   ├── repositories/       # 数据访问层（Repository Pattern）
│   ├── schemas/            # 核心 Schema / 数据结构
│   ├── services/           # 业务服务层
│   └── utils/              # 工具函数
├── strategies/             # YAML 交易策略文件（12+ 策略）
├── templates/              # 模板
└── tests/                  # 所有 pytest 测试
```

### 架构原则

1. **分层架构**：API → Service → Repository → DataProvider，严格单向依赖
2. **Strategy Pattern**：数据源（`data_provider/`）和交易策略（`strategies/`）均使用策略模式
3. **Multi-Agent**：5 个专业 Agent（Pipeline、Market、Skill、Review、Chat），通过 Orchestrator 编排
4. **Graceful Degradation**：所有数据源失败不拖垮主流程，逐级降级
5. **Repository Pattern**：数据访问通过 Repository 抽象，支持 SQLite/内存/文件多种后端
6. **YAML-based 策略**：交易策略通过 YAML 配置文件定义，支持自然语言描述

## 频繁共同变更的文件

当修改以下文件时，通常需要同步修改相关联的文件：

| 主文件 | 经常同步变更的文件 |
|--------|-------------------|
| `src/config.py` | `src/core/config_registry.py`、`.env.example` |
| `src/core/pipeline.py` | `src/core/config_registry.py` |
| 新增配置项 | `.env.example` + `src/config.py` + `docs/CHANGELOG.md` |
| API/Schema 变更 | `api/v1/schemas/` + `api/v1/endpoints/` + 受影响客户端 |

## 工作流

### 添加新功能

1. 在 `src/` 对应子目录创建模块
2. 如需配置项，同步更新 `src/config.py` + `src/core/config_registry.py` + `.env.example`
3. 在 `tests/` 添加 `test_<module>.py`
4. 运行 `./scripts/ci_gate.sh` 验证
5. 更新 `docs/CHANGELOG.md` 的 `[Unreleased]` 段（扁平格式：`- [类型] 描述`）
6. 如涉及用户可见变更，同步更新相关文档

### 修复 Bug

1. 在 `tests/` 中添加复现测试
2. 在对应模块修复
3. 运行 `python -m py_compile <changed_files>` + `pytest -m "not network"`
4. 更新 `docs/CHANGELOG.md`

### 添加新的数据源

1. 在 `data_provider/` 创建新 Fetcher，遵守现有接口
2. 在 `src/core/` 或调用处注册到 fallback chain
3. 确保 graceful degradation：timeout、retry、fallback 完整

### 添加新的 Agent

1. 在 `src/agent/agents/` 创建 Agent 类
2. 在 `src/agent/orchestrator.py` 注册
3. 如需工具，在 `src/agent/tools/` 添加
4. 添加 `tests/test_agent_*.py`

### 添加新 Factor（股票发现）

1. 在 `src/discovery/factors/` 创建因子类，继承 `BaseFactor`
2. 在发现引擎中注册
3. 添加对应测试

## 测试模式

- **框架**：pytest
- **测试目录**：`tests/`（所有测试集中在此目录）
- **命名规范**：`test_<module_name>.py`
- **测试类**：`unittest.TestCase` 或 pytest 风格
- **Mock**：`unittest.mock.MagicMock`、`patch`、`PropertyMock`
- **编码头**：`# -*- coding: utf-8 -*-`
- **标记**：
  - `@pytest.mark.network` — 需要网络的测试
  - `@pytest.mark.benchmark` — 性能测试
- **运行方式**：
  - 离线测试：`pytest -m "not network"`
  - 全量测试：`pytest`
  - CI 门禁：`./scripts/ci_gate.sh`
- **测试 stub**：`tests/litellm_stub.py` 提供 LiteLLM mock

## 关键约定

1. **不写死密钥/路径/模型名**：使用环境变量和配置
2. **新增配置项必须同步 `.env.example`**
3. **CHANGELOG 扁平格式**：`[Unreleased]` 下每条独立一行 `- [新功能/改进/修复/文档/测试/chore] 描述`
4. **编码**：Python 文件 UTF-8，中文注释允许但不强制
5. **入口文件**：`main.py`（CLI）、`server.py`（FastAPI）
6. **Lazy import**：模式相关依赖使用延迟导入减少启动开销
7. **稳定性优先**：不妨碍当前任务的额外重构、抽象和基础设施迁移一律克制
