# 通知能力基线

本文档记录通知能力 P0-P2 基线：渠道、配置 key、GitHub Actions 映射、Web 设置元数据、CLI 诊断口径、Web 一键测试和自定义 Webhook Body 模板语义。P0 只做基线与只读诊断；P1 增加 Web 单渠道真实测试；P2 只产品化现有 Body 模板，不包含渠道路由、降噪、per-URL 模板或新增一等渠道。

## 渠道基线

| 渠道 | 类型 | Minimal key | Advanced key | 说明 |
| --- | --- | --- | --- | --- |
| 企业微信 | 静态配置 | `WECHAT_WEBHOOK_URL` | `WECHAT_MSG_TYPE` | 配置后参与批量通知发送 |
| 飞书 Webhook | 静态配置 | `FEISHU_WEBHOOK_URL` | `FEISHU_WEBHOOK_SECRET`, `FEISHU_WEBHOOK_KEYWORD` | `FEISHU_APP_ID` / `FEISHU_APP_SECRET` 不会单独开启群 Webhook 推送 |
| Telegram | 静态配置 | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | `TELEGRAM_MESSAGE_THREAD_ID` | token 与 chat id 必须同时存在 |
| 邮件 | 静态配置 | `EMAIL_SENDER`, `EMAIL_PASSWORD` | `EMAIL_RECEIVERS`, `EMAIL_SENDER_NAME` | `EMAIL_RECEIVERS` 留空时发给自己 |
| Pushover | 静态配置 | `PUSHOVER_USER_KEY`, `PUSHOVER_API_TOKEN` | - | 两个 key 必须同时存在 |
| PushPlus | 静态配置 | `PUSHPLUS_TOKEN` | `PUSHPLUS_TOPIC` | `PUSHPLUS_TOPIC` 仅在 token 存在时生效 |
| Server酱3 | 静态配置 | `SERVERCHAN3_SENDKEY` | - | 手机 App 推送 |
| 自定义 Webhook | 静态配置 | `CUSTOM_WEBHOOK_URLS` | `CUSTOM_WEBHOOK_BEARER_TOKEN`, `CUSTOM_WEBHOOK_BODY_TEMPLATE`, `WEBHOOK_VERIFY_SSL` | 支持多个 URL，逗号分隔 |
| Discord | 静态配置 | `DISCORD_WEBHOOK_URL` 或 `DISCORD_BOT_TOKEN` + `DISCORD_MAIN_CHANNEL_ID` | `DISCORD_INTERACTIONS_PUBLIC_KEY` | Webhook 与 Bot 均可启用发送 |
| Slack | 静态配置 | `SLACK_WEBHOOK_URL` 或 `SLACK_BOT_TOKEN` + `SLACK_CHANNEL_ID` | - | Bot 优先用于文本与图片同频道发送 |
| AstrBot | 静态配置 | `ASTRBOT_URL` | `ASTRBOT_TOKEN`, `WEBHOOK_VERIFY_SSL` | `ASTRBOT_TOKEN` 可选 |
| `UNKNOWN` | 兜底枚举 | - | - | 仅为未知渠道兜底，不由静态环境变量启用 |
| 钉钉会话 | 运行时上下文 | - | - | 从来源消息上下文提取，无法仅由 `.env` 静态判断 |
| 飞书会话 | 运行时上下文 | - | - | 从来源消息上下文提取，无法仅由 `.env` 静态判断 |

## Minimal / Advanced 分层

- Minimal key：足以启用一个通知渠道的最小配置。
- Advanced key：只影响认证、安全、格式、线程、群组、证书校验或展示行为，不能单独启用渠道。
- P0-P2 不新增路由、降噪或发送策略语义；相关配置如未来引入，应先更新本文档、`.env.example`、Web 元数据与回归测试。

## GitHub Actions 映射

仓库自带 `.github/workflows/daily_analysis.yml` 只显式导入固定变量名。P0 补齐以下已存在发送链路所需的映射：

- `CUSTOM_WEBHOOK_BODY_TEMPLATE`
- `WEBHOOK_VERIFY_SSL`
- `FEISHU_WEBHOOK_SECRET`
- `FEISHU_WEBHOOK_KEYWORD`
- `PUSHPLUS_TOPIC`

P0 不映射 `MARKDOWN_TO_IMAGE_CHANNELS` 与 `MERGE_EMAIL_NOTIFICATION`。它们是发送形态或聚合行为开关，不是渠道凭证；在 Actions 中自动开始读取同名 Secret/Variable 会引入行为变化，留到后续专门阶段处理。

## CLI 诊断

```bash
python main.py --check-notify
```

该命令只读配置，不发送通知，不写入 `.env`。它会在配置加载和日志初始化后立即执行，完成后直接退出，不再进入 Web、调度、大盘复盘或默认分析流程。

- 返回码 `0`：没有 error 级诊断。
- 返回码 `1`：存在 error，例如 0 个静态通知渠道已配置，或成对 key 只配置了一半。

## Web 一键测试

Web 设置页的“通知渠道”分类提供单渠道测试入口。测试会使用当前页面草稿值合成临时配置，发送一条真实测试通知，但不会保存 `.env`，也不会修改运行时全局配置。

- 测试范围：11 个静态通知渠道，不包含 `UNKNOWN` 和运行时上下文渠道。
- 普通渠道：返回单次发送结果、耗时和通用错误码。
- 自定义 Webhook：按 URL 顺序返回 attempts，展示每个 URL 的成功/失败、HTTP 状态、耗时和错误码。
- 返回结果会脱敏 token、secret、password、Bearer、完整 webhook query 和疑似 path token。
- 配置缺失或发送失败返回 `success=false`，不会影响已保存配置和默认分析流程。

## 自定义 Webhook Body 模板

`CUSTOM_WEBHOOK_BODY_TEMPLATE` 是自定义 Webhook 的全局 JSON body 模板。配置后，它会先于 URL 自动识别生效，因此会覆盖 Bark、Slack、Discord、钉钉等自动 payload。未配置时仍使用原有 URL 自动识别；渲染后不是合法 JSON object 时会记录错误并回退默认 payload，不中断主通知流程。

可用占位符：

- `$content_json`：JSON 转义后的通知正文，推荐默认使用。
- `$title_json`：JSON 转义后的通知标题，推荐默认使用。
- `$content` / `$title`：原始字符串，不做 JSON 转义。正文含双引号、反斜杠或换行时可能导致 JSON 无效并触发 fallback。

通用 webhook 示例：

```env
CUSTOM_WEBHOOK_BODY_TEMPLATE={"title":$title_json,"content":$content_json}
```

Bark 通过 custom webhook 使用时，默认会按 `api.day.app` 自动生成 `title` / `body` / `group`。如果配置全局模板，需要自己写出 Bark body：

```env
CUSTOM_WEBHOOK_BODY_TEMPLATE={"title":$title_json,"body":$content_json,"group":"stock"}
```

AstrBot 已是一等通知渠道，优先使用 `ASTRBOT_URL` 和可选的 `ASTRBOT_TOKEN`。只有需要把 AstrBot 兼容端点放入 `CUSTOM_WEBHOOK_URLS` 时，才使用 custom webhook 模板，例如：

```env
CUSTOM_WEBHOOK_BODY_TEMPLATE={"content":$content_json}
```

NapCat / OneBot HTTP API 需要按实际 endpoint 和目标类型调整。下面只是常见 body 形态示例，`user_id`、`group_id`、URL 路径和鉴权方式都应以你的 NapCat 配置为准：

```env
# 私聊：CUSTOM_WEBHOOK_URLS=http://127.0.0.1:3000/send_private_msg
CUSTOM_WEBHOOK_BODY_TEMPLATE={"user_id":123456,"message":$content_json}
```

```env
# 群聊：CUSTOM_WEBHOOK_URLS=http://127.0.0.1:3000/send_group_msg
CUSTOM_WEBHOOK_BODY_TEMPLATE={"group_id":123456789,"message":$content_json}
```

## 场景占位

- Local：优先使用 `.env`，可用 `python main.py --check-notify` 做本地诊断。
- Docker：配置来源与本地一致，需确保容器环境变量已注入。
- GitHub Actions：只会读取 workflow `env:` 中显式映射的 Secret/Variable。
- Desktop：桌面端内嵌 Web 设置页可复用同一通知测试入口；测试仍只使用临时配置，不写入 `.env`。
