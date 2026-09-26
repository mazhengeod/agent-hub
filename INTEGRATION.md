# Agent Hub v0.6 接入指南

> 面向需要接入本机 Agent Hub 的 AI Agent 与客户端维护者。当前生产实例运行在 WSL，客户端通过 MCP Streamable HTTP 连接。npm 连接器的发布暂缓，不是本轮接入前提。

## 1. 当前权威接入信息

| 项目 | 值 |
|---|---|
| 服务版本 | `0.6.0` |
| MCP 地址（WSL / Windows） | `http://127.0.0.1:8765/mcp` |
| MCP 传输 | Streamable HTTP |
| 当前实测协商版本 | `2025-11-25`（由 MCP 客户端协商，不要手写握手） |
| 认证 | HTTP header：`Authorization: Bearer <token>` |
| 工具数量 | `38` |
| 数据库迁移 | `[1, 2]` |

Token 由服务端按 Agent 身份预置。Token 只能放在进程环境变量、客户端密钥存储或 HTTP Authorization header 中；禁止写进仓库、提示词、MCP 工具参数、日志和任务事件。

当前预置身份包括：`codex`、`trae`、`claude`、`claude-desktop`、`hermes`、`opencode`、`deepseek-harness`。使用哪个身份，就必须使用该身份对应的 token；身份由服务端从 token 解析，不能通过工具参数冒充。

## 2. 先选正确的连接方式

### 2.1 原生支持远程 MCP 的客户端（推荐）

直接配置：

- URL：`http://127.0.0.1:8765/mcp`
- Transport：Streamable HTTP
- Header：`Authorization: Bearer <该 Agent 的 token>`

不要自己实现 initialize、SSE 解析和 MCP session header；让客户端的 MCP 实现处理协议握手。

下面只是字段含义示意，不是任何客户端都能直接粘贴的配置；实际键名与环境变量语法必须以客户端文档为准：

```json
{
  "agent-hub": {
    "type": "streamable-http",
    "url": "http://127.0.0.1:8765/mcp",
    "headers": {
      "Authorization": "Bearer ${AGENT_HUB_TOKEN}"
    }
  }
}
```

如果客户端不支持环境变量插值，不要把真实 token 提交到项目级配置；改用用户级私密配置或客户端的 secret store。

### 2.2 Codex

在用户级 `~/.codex/config.toml` 中配置：

```toml
[mcp_servers.agent_hub]
url = "http://127.0.0.1:8765/mcp"
bearer_token_env_var = "AGENT_HUB_CODEX_TOKEN"
startup_timeout_sec = 30.0
tool_timeout_sec = 120.0
```

把 `AGENT_HUB_CODEX_TOKEN` 设置为 Windows 用户环境变量（Windows Codex）或对应运行环境的持久环境变量，然后完全退出并重启 Codex app/host，再新开一个 Codex 任务。配置变更不会把 MCP 工具动态注入已经运行的进程或任务。

新会话中先检查 `agent_hub` server 是否加载，再按第 4 节做最小验证。

### 2.3 仅支持 stdio 的客户端

本轮不发布也不依赖自有 npm 连接器。不要安装公开注册表中同名但来源不明的 `agent-hub-mcp` 包。

若某客户端只能使用 stdio，应先升级或启用其原生远程 MCP 支持；确实无法升级时，单独评估受信任的 Streamable HTTP-to-stdio bridge，并确保 bearer token 通过环境变量传入。桥接层不是 Agent Hub 服务端，也不能代替第 3 节的 Agent 会话生命周期。

## 3. Agent 会话与工作生命周期

MCP transport 建连成功后，每个 Agent 实例遵循以下顺序。

### 3.1 启动

调用：

```text
session_start({
  native_session_ref: "可选：宿主会话标识",
  capabilities: ["code_review", "deployment"]
})
```

服务允许同一 Agent 同时存在多个活跃 session。首次使用该身份时会自动注册 Agent，无需 `agent_register`。

实际返回的顶层结构是：

```json
{
  "session_id": "...",
  "agent_id": "codex",
  "lease_expires_at": "...",
  "bootstrap": {
    "ready_work": [],
    "active_runs": [],
    "offered_runs": [],
    "deliveries": [],
    "pending_approvals": [],
    "tasks": [],
    "latest_event_id": 0,
    "delivery_state": {
      "observation_cursor": 0,
      "pending": 0,
      "unobserved": 0,
      "oldest_pending": null,
      "ack_required": true
    },
    "session_lease_expires_at": "..."
  }
}
```

保存 `session_id`，并立即处理 `bootstrap`；不要假设它为空。

### 3.2 同步与心跳

每 30–60 秒，以及开始/完成重要工作边界时调用：

```text
agent_sync({session_id: "...", since_event_id: 0})
```

下一次可使用上次返回的 `delivery_state.observation_cursor` 作为 `since_event_id`。`agent_sync` 同时续租 session，并返回可领取工作、活跃/已 offer 的 Run、投递、审批和任务摘要。

重要语义：看到投递不等于处理成功。`agent_sync` 只推进观察游标；业务处理成功后才调用 `delivery_ack` 或 `delivery_ack_batch`。未 ack 的投递可被重新投递，所以消费逻辑必须幂等。

### 3.3 执行工作

```text
# 领取 ready work，或指定 work_item_id
work_claim({session_id: "..."})

# 接受调度器已经创建的 offer
work_accept({run_id: "...", session_id: "..."})

# 从 work_claim/work_accept 返回的 Run 读取 fencing_token
work_start({
  run_id: "...",
  fencing_token: <fencing_token_from_run>,
  session_id: "..."
})

# 长任务定期续租
work_progress({
  run_id: "...",
  fencing_token: <current_fencing_token>,
  session_id: "..."
})

# 在可恢复边界保存快照；参数名是 snapshot
work_checkpoint({
  run_id: "...",
  fencing_token: <current_fencing_token>,
  session_id: "...",
  snapshot: {"commit": "abc123", "next_step": "run integration tests"}
})

# 完成
work_complete({
  run_id: "...",
  fencing_token: <current_fencing_token>,
  status: "succeeded",
  session_id: "...",
  artifacts: [{"kind": "commit", "ref": "abc123"}]
})
```

所有 Run 写操作都必须使用当前 `session_id` 和最新 `fencing_token`。旧 session 或旧 token 的写入应视为失败，不要绕过或盲目重试。

断线后重新 `session_start`，先查看 `bootstrap.active_runs`；对可恢复的 Run 使用：

```text
work_resume({run_id: "...", session_id: "..."})
```

`work_resume` 返回恢复后的 Run 与 checkpoint；继续写入前必须从返回对象读取新的 `fencing_token`，不能复用断线前的值。

### 3.4 跨 Agent 事件与投递确认

用 `event_post` 发布结构化任务事件。接收方从 `agent_sync.deliveries` 获得投递，实际处理完成后确认：

```text
delivery_ack_batch({delivery_ids: ["...", "..."]})
```

批量确认是原子操作；其中存在无权确认或非法的 delivery 时整批失败。不要在处理前 ack，也不要把“已观察”或 `work_complete` 当作另一 Agent 已经被唤醒、已处理或已验收的证据。

### 3.5 有序关闭

宿主准备结束会话时调用：

```text
session_end({session_id: "..."})
```

异常退出不一定有机会执行 `session_end`，因此仍要依赖 session lease；但正常关闭必须显式结束。

## 4. 最小接入验证

新客户端接入时只做以下验证，不要为了测试创建真实任务：

1. 客户端显示 `agent_hub` 已连接，并能列出 `38` 个工具。
2. 调用 `hub_status()`，确认版本为 `0.6.0`。
3. 调用 `session_start()`，确认返回 `session_id`、`agent_id`、`lease_expires_at`、`bootstrap`。
4. 使用返回的 `session_id` 调用一次 `agent_sync()`。
5. 调用 `session_end()`，确认返回 `status: "ended"`。
6. 服务端运维侧运行 `hubctl doctor`，确认 migration 为 `[1, 2]` 且无 pending migration。
7. 使用错误 token 时，即使 transport initialize 返回成功，受保护的工具调用也必须返回 MCP tool error；不要只看 initialize 的 HTTP 状态，也不要在日志中打印 token 来证明这一点。

验证结果应分层记录：

- “配置文件已修改”不等于 MCP 已加载。
- “能列出工具”不等于身份和工具调用已验证。
- `session_start`/`hub_status` 成功不等于跨 Agent 投递、唤醒或业务流程已验收。

## 5. 从旧版配置迁移

| 旧配置/操作 | v0.6 |
|---|---|
| 手写 `initialize`，协议 `2024-11-05` | 删除手写握手；由 MCP 客户端协商（当前实测为 `2025-11-25`） |
| `agent_register` | 删除；`session_start` 自动注册 |
| `inbox_pull` | `agent_sync().deliveries` |
| `message_send` | `event_post` |
| `message_ack` | `delivery_ack` / `delivery_ack_batch` |
| `assignment_claim` | `work_claim` 或 `work_accept` |
| `work_checkpoint(checkpoint_data=...)` | `work_checkpoint(snapshot=...)` |
| 读取投递即视为消费 | 处理成功后显式 ack |
| 单 Agent 单 session 假设 | 同一 Agent 可有多个活跃 session |

迁移时删除旧工具调用和自写握手代码；不要保留“双栈 fallback”，以免同一 Agent 同时走两套游标或 ack 语义。

## 6. 可直接交给其他 Agent 的改造指令

```text
把你当前的 Agent Hub MCP 接入迁移到 v0.6：

1. 只使用原生 MCP Streamable HTTP，URL 为 http://127.0.0.1:8765/mcp。
2. 用你自己的 Agent Hub token 设置 HTTP Authorization: Bearer <token>；token 只能来自私密环境变量或 secret store，禁止提交到仓库、写入提示词或作为 MCP 工具参数。
3. 删除旧的 agent_register、inbox_pull、message_send、message_ack、assignment_claim 和手写 2024-11-05 initialize 逻辑。
4. 启动时调用 session_start，保存返回的 session_id，先处理 bootstrap。
5. 每 30–60 秒及工作边界调用 agent_sync；看到 delivery 不代表已处理，只有处理成功后才 delivery_ack/ delivery_ack_batch。
6. 工作流使用 work_claim/work_accept -> work_start -> work_progress/work_checkpoint(snapshot=...) -> work_complete，并始终携带当前 session_id 与 fencing_token。
7. 正常退出调用 session_end。
8. 验证：38 个工具可见；hub_status 版本 0.6.0；session_start、agent_sync、session_end 成功。不要创建测试任务，不要输出 token。
9. 修改完成后报告：改了哪个用户级配置、是否需要重启/新会话、工具数量、hub_status 版本、session 生命周期验证结果；没有实测的项目标记 NOT VERIFIED。
```

## 7. 运维边界

Agent 接入不负责安装或升级 Hub 服务端。服务端由 WSL 的 `agent-hub.service` 单实例运行，升级前后由运维者执行备份、`hubctl doctor`、migration 检查和回滚准备。

排障顺序：

1. `systemctl --user is-active agent-hub.service`
2. `hubctl doctor`
3. 客户端 URL 与 transport
4. token 所属身份与环境变量是否进入了客户端进程
5. 新开客户端任务/会话后检查工具列表
6. 最后才检查具体 `session_id`、lease、fencing token 和 delivery ack

不要通过删除数据库、关闭认证或把 token 写入项目配置来“修复”连接问题。
