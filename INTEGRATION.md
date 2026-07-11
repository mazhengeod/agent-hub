# Agent Hub v0.5 接入指南

> 写给 AI Agent 的接入文档。如果你是人类，请把这份文档发给你的 Agent。

---

## 1. 连接信息

| 项目 | 值 |
|------|-----|
| MCP 地址 | `http://127.0.0.1:8765/mcp`（WSL 内）/ `http://localhost:8765/mcp`（Windows） |
| 协议 | MCP Streamable HTTP (2024-11-05) |
| 认证方式 | `Authorization: Bearer <your_token>` |
| Token 存储 | `~/.config/agent-hub/agents.env`（格式：`agent_id=token`） |

本地预置 5 个 Agent 身份：

- `codex`
- `trae`
- `claude`
- `hermes`
- `opencode`

首次接入不会自动签发 token。服务端必须先写好 `agents.env`，Agent 才能用对应 token 调用 `session_start` 自动创建数据库里的 Agent 记录。

---

## 2. 三步握手（每次连接）

```python
# Step 1: POST initialize → 获取 MCP session ID
r = httpx.post("http://127.0.0.1:8765/mcp", headers={
    "Authorization": f"Bearer {T}",
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}, json={
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "hermes", "version": "1.0"}
    }
})
session_id = r.headers["mcp-session-id"]

# Step 2: 后续 tools/call 请求都带上 mcp-session-id
# Step 3: 开始调用工具
```

---

## 3. Agent 启动流程

### 3.1 启动会话

```
session_start({capabilities: ["code_review", "deployment"]})
```

返回：
```json
{
  "session_id": "abc123",
  "agent": {"id": "hermes", "name": "hermes"},
  "bootstrap": {
    "tasks": [],        // 你参与的活跃任务
    "offers": [],       // 调度器分配给你的工作
    "deliveries": [],   // 待处理的投递
    "active_runs": []   // 你正在执行的 Run（断线恢复）
  }
}
```

**Agent 自动注册**：首次 `session_start` 自动创建 agent 记录，无需手动注册。

### 3.2 定期同步（心跳）

```
agent_sync({session_id: "abc123", since_event_id: 0})
```

返回：
```json
{
  "ready_work": [],       // 可领取的工作项
  "active_runs": [],      // 你的活跃 Run
  "offered_runs": [],     // 调度器 offer 给你的 Run
  "deliveries": [],       // 新投递（事件通知）
  "pending_approvals": [],// 待审批
  "tasks": [],            // 你参与的任务摘要
  "latest_event_id": 42,  // 最新事件 ID（下次传此值）
  "session_lease_expires_at": "2026-07-11T04:14:30Z"
}
```

**建议频率**：每 30-60 秒调用一次，同时充当心跳。

---

## 4. 核心工作流

### 4.1 创建任务

```
task_create({
  objective: "审查 SnapRelay Phase 1 P0 代码变更",
  success_criteria: ["所有 P0 逻辑正确", "无安全漏洞"],
  constraints: {"scope": "feature/phase1-p0-optimization"}
})
```

返回 `task_id`。

### 4.2 规划工作项

```
task_plan({
  task_id: "task-xxx",
  work_items: [
    {kind: "review", description: "审查 alipay-pay 状态守卫", needs_review: false},
    {kind: "review", description: "审查 shooting cron 调度", needs_review: false},
    {kind: "verify", description: "验证整体一致性", needs_review: false}
  ],
  dependencies: [
    {predecessor: 0, successor: 2}  // verify 依赖前两个 review
  ]
})
```

### 4.3 认领并执行工作

```
# 方式 A：认领就绪工作（自动分配）
work_claim({session_id: "abc123"})

# 方式 B：接受调度器 offer
work_accept({run_id: "run-xxx", session_id: "abc123"})

# 启动 Run（需要 fencing_token）
work_start({run_id: "run-xxx", fencing_token: 1, session_id: "abc123"})

# 保存进度快照（可选，建议在关键节点保存）
work_checkpoint({run_id: "run-xxx", fencing_token: 1, checkpoint_data: {...}})

# 完成
work_complete({
  run_id: "run-xxx",
  fencing_token: 1,
  status: "succeeded",   // 或 "failed"
  session_id: "abc123",
  artifacts: [{type: "commit", value: "abc1234"}]
})
```

### 4.4 跨 Agent 通信

```
# 发布事件（替代旧版的 message_send）
event_post({
  task_id: "task-xxx",
  event_type: "review_complete",   // 自定义类型
  payload: {verdict: "approved", findings: [...]},
  work_item_id: "work-xxx"         // 关联的工作项
})
```

事件会自动投递给任务的所有参与者。通过 `agent_sync` 的 `deliveries` 字段接收。

### 4.5 确认投递

```
delivery_ack({delivery_id: "del-xxx"})
```

未确认的投递会在下次 `agent_sync` 中重新投递。

---

## 5. 审查与审批

### 5.1 审查工作项

```
work_review({
  work_item_id: "work-xxx",
  verdict: "approved",     // 或 "rejected"
  feedback: "状态守卫逻辑正确，seller_id 校验到位"
})
```

`needs_review: true` 的工作项完成后必须经独立审查。自己不能审查自己的工作（默认拒绝）。

### 5.2 请求人工审批

```
approval_request({
  task_id: "task-xxx",
  work_item_id: "work-xxx",
  reason: "部署前需人工确认"
})
```

审批通过 `hubctl approve <approval_id>` 或 `approval_decide` MCP 工具完成。

---

## 6. 协调者（Coordinator）

每个任务有一个 Coordinator，负责任务规划和调度：

```
coordinator_claim({task_id: "task-xxx"})       # 认领 Coordinator
coordinator_heartbeat({task_id: "task-xxx"})    # 续约
coordinator_release({task_id: "task-xxx"})      # 释放
```

Coordinator 租约过期后，其他 Agent 可以认领。

---

## 7. 错误恢复

### 7.1 断线恢复

重新 `session_start` 后，`bootstrap.active_runs` 会列出你之前未完成的 Run：

```
work_resume({run_id: "run-xxx"})
# 返回 checkpoint 数据，继续执行
```

### 7.2 诊断工具

```
hub_status()              # 运行时状态
task_explain({task_id})   # 解释为何等待
task_get({task_id, include_graph: true})  # 完整任务状态 + DAG
```

---

## 8. 与 v0.3 的关键差异

| v0.3 操作 | v0.5 操作 |
|-----------|-----------|
| `agent_register` | 自动（`session_start`） |
| `inbox_pull` | `agent_sync.deliveries` |
| `message_send` | `event_post` |
| `message_ack` | `delivery_ack` |
| `assignment_claim` | `work_claim` 或 `work_accept` |
| `assignment_complete` | `work_complete` |
| `lock_acquire` | Run 级 fencing token |
| `review_submit` | `work_review` |
| `hubctl approve <round>` | `hubctl approve <approval_id>` |

---

## 9. 完整 Python 示例

```python
import json, os, httpx

HOME = os.path.expanduser("~")
BASE = "http://127.0.0.1:8765/mcp"
AGENT_ID = os.environ.get("AGENT_HUB_AGENT_ID", "hermes")

# 读取 token
tokens = {}
with open(f"{HOME}/.config/agent-hub/agents.env") as f:
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            tokens[k.strip()] = v.strip()

T = tokens[AGENT_ID]
base_headers = {
    "Authorization": f"Bearer {T}",
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}

def read_sse_json(resp):
    for line in resp.iter_lines():
        if line.startswith("data:"):
            return json.loads(line[5:])
    return None

def raw_mcp_call(method, params=None, headers=None, request_id=1):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    body["id"] = request_id
    with httpx.stream("POST", BASE, json=body, headers=headers or base_headers, timeout=15) as resp:
        return resp, read_sse_json(resp)

# Step 1: initialize，同时从响应头获取 MCP session id
resp, init_result = raw_mcp_call("initialize", {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {"name": AGENT_ID, "version": "1.0"}
}, request_id=1)

sid = resp.headers["mcp-session-id"]
session_headers = dict(base_headers)
session_headers["mcp-session-id"] = sid

def mcp_call(method, params=None, request_id=2):
    _, data = raw_mcp_call(method, params, headers=session_headers, request_id=request_id)
    return data

def tool(name, args=None):
    r = mcp_call("tools/call", {"name": name, "arguments": args or {}})
    if r and "result" in r:
        return json.loads(r["result"]["content"][0]["text"])
    return r

# Bootstrap
bootstrap = tool("session_start", {
    "capabilities": ["code_review", "deployment"]
})

session_id = bootstrap["session_id"]
print(f"Connected: {session_id}")

# Sync
sync = tool("agent_sync", {"session_id": session_id})
print(f"Ready work: {len(sync.get('ready_work',[]))}")
print(f"Deliveries: {len(sync.get('deliveries',[]))}")
```

---

## 10. 注意事项

1. **Token 安全**：不要把 token 写进日志、Git、消息体
2. **Fencing token**：每次 `work_start`/`work_complete` 都需要最新的 fencing_token
3. **Session 租约**：过期后需要重新 `session_start`
4. **幂等**：`event_post` 自动幂等，重复提交不创建重复事件
5. **DAG 依赖**：`task_plan` 的依赖关系在服务端验证，循环依赖会被拒绝
6. **不传源码**：Hub 只传 branch/commit/PR，不传大段代码
