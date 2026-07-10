# Agent Hub 任务驱动的跨 Session、多 Agent 主动协作架构

> 状态：架构审计后的重构方案（替代原“会话级通信优化方案”）  
> 日期：2026-07-10  
> 范围：本机 Agent Hub（FastMCP + SQLite + systemd）及各 agent 接入规则/runner  
> 核心目标：以任务结果为中心，允许任务跨 session 持续，多 agent 可并行、接力、复核，并在无人充当“邮差”时继续推进可自动推进的工作。

---

## 0. 结论与架构裁决

原方案不建议按原样实施，应从底层模型重构。

原方案正确识别了两个问题：

1. 消息只按 `to_agent` 路由，同一 agent 的多个 session 会互相抢消息。
2. session 短命，任务状态不能只存在于聊天上下文中。

但原方案用 `conversation` 作为新的顶层实体，并把 `current_session`、消息 `seq`、60 分钟 TTL 和 agent 自建定时任务组合成第二套状态机。这会与已经存在的 `root_tasks / rounds / assignments / handoffs / locks` 竞争“谁才是任务真相”，同时仍不能真正唤醒已退出的 agent。

本方案做出以下裁决：

- **Task 是唯一顶层业务身份。** Conversation 只作为任务内的沟通视图，不拥有任务生命周期。
- **Work Item 是可调度工作单元。** 用依赖图表达并行、阻塞、复核和动态拆分，不再把固定 Round 当作主流程。
- **Run 是一次执行尝试。** session 只通过 Run 临时参与任务；session 消失不会使 Task 或 Work Item 消失。
- **Event 是事实记录，Delivery 是投递状态。** 删除全局 `is_read` 语义，避免一个 session ack 后其他 session 永久丢失消息。
- **Scheduler 负责机械推进，Coordinator agent 负责语义决策。** Hub 自身不假装具备推理能力。
- **主动唤醒依赖可执行 adapter/runner。** MCP、SSE、长轮询只能通知仍在线的客户端，不能唤醒已关闭的桌面 session。
- **跨 session 恢复依赖 checkpoint + lease + fencing token。** 不依赖旧 session 主动交接。
- **SQLite 保留。** 当前规模无需换 PostgreSQL，但必须采用单写者、短事务、迁移版本和 outbox，先解决现有事务与锁问题。

目标架构可以概括为：

```text
用户目标
  -> Task（目标、成功标准、约束、授权边界）
      -> Work Item DAG（可执行、可并行、可复核的工作）
          -> Run / Attempt（某 agent 的某个 session 执行一次）
              -> Checkpoint / Artifact / Result

Hub Runtime
  -> Scheduler（依赖、租约、重试、派发、超时）
  -> Event Log + Delivery（可靠通信）
  -> Outbox（可靠触发 adapter）

Agent Adapter / Runner
  -> 唤醒或启动可被程序调用的 agent
  -> 为不能被程序唤醒的 agent 创建待办并等待 session_start
```

---

## 1. 需求定义

### 1.1 核心需求

系统应满足：

1. 用户只需定义任务目标、成功标准、约束和授权边界。
2. 一个任务可以持续数小时或数天，不依赖单个聊天窗口存活。
3. 多个 agent 可以针对同一任务并行工作，且不会串任务、抢消息或覆盖结果。
4. agent/session 中断后，其他 session 可以基于 Hub 中的事实状态恢复，而不是依赖人工复制聊天记录。
5. 工作完成、失败、超时、被阻塞或需要复核后，系统可以主动选择下一步。
6. 自动推进不能突破用户授权；高风险动作仍需 approval gate。
7. 所有派发、认领、进度、交接、审批和重试均可审计。

### 1.2 “主动协调”的准确含义

主动协调分为三个层级，必须区分：

| 层级 | 能力 | 是否需要 agent 在线 |
|---|---|---|
| L1 状态主动 | Hub 自动处理依赖、租约、重试、超时、状态转换 | 否 |
| L2 投递主动 | Hub 自动把可运行工作投递给 runner/adapter | 否，但 adapter 必须常驻 |
| L3 执行主动 | adapter 启动或唤醒 agent，agent 自动领取并执行 | 取决于该 agent 是否提供可调用入口 |

如果某个桌面 agent 没有可调用 API、CLI、automation 或常驻 runner，Hub 最多做到 L2：可靠记录待办，并在它的下一个 session 启动时立即恢复。任何方案都不能仅靠 MCP 消息把已关闭的客户端“叫醒”。

### 1.3 非目标

- 不把 Agent Hub 做成另一个通用聊天产品。
- 不依赖隐藏的文件 watcher 或不可观察的触发技巧。
- 不要求所有 agent 使用同一种 session/automation 实现。
- 不让 Hub 通过字符串规则替代 coordinator agent 的任务规划和判断。
- 第一阶段不追求跨机器、高吞吐或多租户。

---

## 2. 对原方案的系统性审计

### 2.1 应保留的判断

- 任务身份必须比 session 长命。
- 同一 agent 的多个 session 必须隔离。
- 必须有 lease、heartbeat、超时和幂等。
- Hub 应承担持久状态和超时回收。
- 长轮询/SSE 只能作为在线体验优化，不能作为可靠性根基。

### 2.2 必须推翻或改写的部分

#### A. `conversation` 不应成为顶层任务

问题：现有系统已经有 Task、Assignment、Handoff。再引入具备状态、TTL、持有 session 的 Conversation，会形成两个聚合根：Task 说任务在运行，Conversation 可能已经 expired；或 Conversation active，但 Assignment 已完成。

替代：所有沟通事件带 `task_id`，必要时再带 `work_item_id` / `run_id`。Conversation 可作为 UI 查询投影，例如“某 Work Item 的问答串”，不拥有状态机。

#### B. 单一 `current_session` 无法表达多 agent 协作

问题：同一任务可同时有 planner、implementer、reviewer；一个 agent 也可能有两个合法并行 run。`current_session` 既无法表达多人协作，也会把正常并行误判为冲突。

替代：每个 Work Item 可产生多个 Run；每个 Run 绑定一个 Hub 生成的 `session_id` 和 `agent_id`。并行边界由 Work Item 和资源锁表达。

#### C. `seq` 不能由客户端递增

问题：多个 agent 并发发消息时无法安全协商下一个序号；重试也可能造成重复或空洞。

替代：事件顺序由服务端分配全局单调 `event_id`，或由 SQLite 整数主键排序。客户端只提供 `idempotency_key`。

#### D. 全局 `is_read` / `message_ack` 是破坏性消费

问题：一个 session ack 会让同 agent 的其他 session 永远看不到消息；广播消息也无法被每个订阅者分别确认。

替代：事件不可变；每个接收 endpoint/run 有独立 Delivery，或维护独立 cursor。ack 的对象是 `delivery_id`，不是 message 本身。

#### E. 60 分钟 Conversation TTL 混淆了四种时间

任务、session、run、消息的时间语义不同：

- Task 可持续数天，通常由 deadline 或用户关闭。
- Session lease 通常为数分钟。
- Run lease 取决于工作类型，并由 heartbeat 续租。
- Delivery 有重试/过期时间，但不决定 Task 是否结束。

替代：分别建模，不使用一个 TTL 控制全部生命周期。

#### F. “看到 init 后自己创建定时任务”不是可靠触发协议

问题：不同 agent 的定时能力不一致；有的 session 无权创建 automation；session 退出后定时任务可能一起消失；而接收方必须先看到 init，形成循环依赖。

替代：Hub 内持久 Scheduler + 每类 agent 的 adapter/runner。agent 规则只负责标准化接入，不承担可靠调度。

#### G. 现有编排层不应直接删除

当前 `root_tasks / assignments / handoffs / locks / audit_events` 使用率低，原因是没有 scheduler、发现协议和好用的 agent bootstrap，不代表这些概念错误。应重构为 Task / Work Item / Run，而不是退化成消息频道。

#### H. `GET /mcp` 返回 400 不能单独证明“没有推送能力”

Streamable HTTP 的 GET/SSE 是可选在线通道；即使启用，它也只对当前连接有效。系统设计应依据“断线客户端无法被传输层唤醒”这一稳定约束，而不是依赖某一次 GET 的返回码。

---

## 3. 目标领域模型

### 3.1 Task：唯一目标聚合根

Task 必须保存：

- `objective`：用户最终想得到什么。
- `success_criteria`：可验证的完成条件。
- `constraints`：不可触碰的边界、平台、分支、环境等。
- `authorization_policy`：哪些动作可自动做，哪些必须审批。
- `context_refs`：仓库、文档、issue、已有 handoff 等引用，不塞入超长正文。
- `priority / deadline / budget`：调度约束。
- `coordinator_policy`：指定 coordinator、自动选举或人工模式。
- `plan_version`：当前工作图版本。

建议状态：

```text
draft -> planned -> ready -> running -> verifying -> completed
                         \-> blocked
                         \-> failed
任意非终态 -> cancelled
completed/failed/cancelled -> archived（仅保留层级，不删除事实）
```

`blocked` 必须带结构化原因：`waiting_dependency | waiting_agent | waiting_user | waiting_external | policy_gate | retry_exhausted`。

### 3.2 Work Item：任务内可调度工作

Work Item 替代“固定 round 中的一条 assignment”，包含：

- 明确目标和验收条件。
- 所需 capability、建议 agent、资源范围。
- 依赖关系 `depends_on`。
- 工作类型：`plan | research | implement | review | verify | operate | summarize`。
- 失败/重试策略。
- 是否需要独立 reviewer。
- 可否动态拆分子 Work Item。

状态：

```text
pending -> ready -> offered -> running -> reviewing -> succeeded
                         |          |          \-> changes_requested -> ready
                         |          \-> blocked
                         \-> failed
任意非终态 -> cancelled
```

依赖图必须为 DAG。动态新增 Work Item 时进行环检测，并限制拆分深度和总数量。

### 3.3 Run：一次可恢复的执行尝试

Run 是 agent 对一个 Work Item 的一次尝试：

- `agent_id`
- `session_id`
- `attempt_no`
- `lease_expires_at`
- `heartbeat_at`
- `fencing_token`
- `checkpoint_id`
- `status`
- `started_at / ended_at / failure_code`

状态：

```text
offered -> claimed -> running -> succeeded
                            \-> failed
                            \-> lost（lease 过期）
                            \-> cancelled
```

同一个 Work Item 可在失败或 lost 后创建新 Run。旧 Run 不能被“复活”覆盖新 Run；所有写操作必须携带 fencing token。

### 3.4 Session：临时执行端点

Session 是 Hub 生成的短命实体，不由 agent 自报一个容易冲突的字符串：

- `session_id`：Hub 生成。
- `agent_id`：来自 Bearer 身份。
- `native_session_ref`：可选、不透明，仅用于定位原生 session。
- `capabilities_snapshot`
- `started_at / last_seen_at / lease_expires_at / ended_at`
- `adapter_id`

同一 `agent_id` 可以同时存在多个 Session。权限校验同时检查 agent 身份、session 和 run fencing token。

### 3.5 Checkpoint：跨 session 恢复包

Checkpoint 至少包含：

- 当前 Work Item 目标和验收条件。
- 已完成步骤与关键结论。
- 决策及原因。
- 当前仓库/分支/HEAD/工作区状态（适用时）。
- 变更文件、测试结果、风险。
- 外部依赖和未决问题。
- 精确下一步。
- 关联 Artifact 引用。
- 来源 Run、版本和时间。

Checkpoint 是结构化快照，不是整段聊天历史。新 session 可用 Task snapshot + Work Item + 最新 Checkpoint + 未消费事件恢复。

### 3.6 Event、Delivery 和 Artifact

Event 是追加写事实，例如：

- `task.created`
- `work.ready`
- `run.claimed`
- `run.progressed`
- `run.lost`
- `checkpoint.saved`
- `question.asked`
- `answer.posted`
- `review.requested`
- `approval.required`
- `task.completed`

Event 不修改“已读”。需要通知谁由 Delivery 表表达：

```text
event 1 -> delivery A -> codex run 12 -> acked
        -> delivery B -> hermes endpoint -> pending
```

Artifact 保存交付物引用及校验信息，例如文件路径、commit SHA、测试报告、文档、日志摘要。大正文放文件或对象存储，数据库保存元数据和 hash。

---

## 4. 控制平面与主动协调

### 4.1 Scheduler：只做确定性机械决策

Scheduler 是 systemd 常驻 Hub Runtime 的一部分，循环执行：

1. 回收过期 Session 和 Run lease。
2. 把依赖已满足的 Work Item 从 `pending` 变为 `ready`。
3. 对 `ready` 工作按 capability、亲和性、容量、优先级选择候选 agent/adapter。
4. 创建 Run offer 和 outbox 事件。
5. 对投递失败做指数退避；超过阈值进入 dead letter 并阻塞 Work Item。
6. 根据完成/复核事件推进 Task 状态。
7. 在 coordinator 丢失时触发 coordinator failover。

Scheduler 不阅读自然语言猜测下一步，不擅自扩大任务范围。

### 4.2 Coordinator agent：负责语义协调

每个复杂 Task 可有一个 coordinator lease。Coordinator 的职责：

- 把目标拆成 Work Item DAG。
- 根据结果动态新增、取消或重排 Work Item。
- 处理 agent 提问和冲突。
- 判断何时需要 reviewer 或用户决策。
- 汇总最终验收证据。

Coordinator 不是永久绑定任务创建者。其 lease 过期后，可由同一 agent 新 session 或其他具备 `coordinate` capability 的 agent 接管。

### 4.3 Agent Adapter / Runner

每类 agent 必须登记真实唤醒能力：

| adapter 模式 | 行为 | 自动化能力 |
|---|---|---|
| `resident_runner` | 常驻进程轮询 offer 并调用 agent runtime | L3 |
| `cli_spawn` | 受控启动 CLI agent，注入 bootstrap packet | L3 |
| `automation` | 使用产品原生 automation/thread wakeup | L3 或受限 L3 |
| `online_session` | 仅向当前在线 session 长轮询/SSE 投递 | L2，断线后停止 |
| `manual_resume` | 记录待办；下次 session_start 自动呈现 | L2 |

不能把所有 agent 宣称为 L3。上线前必须逐个做真实的“断线 -> 派发 -> 唤醒 -> claim -> 回报”验收。

### 4.4 Outbox：状态变更与投递原子化

Scheduler 不应在数据库事务中直接调用外部 agent。事务内同时写业务状态和 outbox；事务提交后由 dispatcher 发送：

```text
BEGIN
  Work Item: ready -> offered
  INSERT Run offer
  INSERT outbox(adapter.dispatch, payload)
  INSERT audit event
COMMIT

dispatcher 发送成功 -> outbox delivered
发送失败 -> retry_at + attempts
```

adapter 回调也携带 `idempotency_key`，保证重复投递不会产生两个有效 claim。

---

## 5. 跨 Session 的标准恢复协议

### 5.1 新 session 启动

每个 agent 的接入规则只需固化一个统一入口，而不是自己创建 cron：

```text
session_start(native_session_ref?, capabilities?)
  -> 返回 session_id、session lease
  -> 返回该 agent 的：
       1. 可恢复 Run
       2. 待 claim offer
       3. 未 ack Delivery
       4. 相关 Task/Work Item 摘要
```

如果有可恢复 Run，agent 调用：

```text
run_resume(run_id, session_id, expected_fencing_token)
  -> Hub 创建新的 attempt 或安全转移执行权
  -> 返回新的 fencing_token
  -> 返回 task_snapshot + work_item + latest_checkpoint + events_after_cursor
```

默认不让两个 session 同时写同一个 Run。

### 5.2 正常执行

agent 通过一个合并同步接口降低工具调用和 SQLite 写争用：

```text
agent_sync(
  session_id,
  heartbeats=[...],
  progress=[...],
  delivery_acks=[...],
  cursor=...
)
  -> lease 续期
  -> 批量写进度/ack
  -> 返回新事件、offer、控制指令
```

关键阶段调用 `checkpoint_save`，不是每条聊天都写 checkpoint。

### 5.3 session 非正常退出

1. Session lease 到期。
2. 对应 Run 进入 `lost`，旧 fencing token 失效。
3. Scheduler 根据 retry policy 创建新 attempt。
4. 新 session/agent 从最新 Checkpoint 恢复。
5. 旧 session 如果迟到，只能提交被拒绝的 stale write，不能覆盖新结果。

### 5.4 主动交接

正常交接调用：

```text
run_handoff_prepare(run_id, checkpoint, preferred_agent?, reason)
```

Hub 原子完成：保存 checkpoint、结束当前 Run、创建后继 offer、记录 handoff event。Handoff 不再只是完成后的 git 摘要，也可用于中途接力。

---

## 6. 多 Agent 协作协议

### 6.1 并行与依赖

示例：

```text
Task: 完成一个功能并交付

W1 代码/现状调查 ----\
                     -> W3 实现 -> W4 独立复核 -> W5 集成验收
W2 方案/风险调查 ----/
```

W1/W2 可由不同 agent 并行；W3 只有在依赖成功后 ready；W4 必须由不同 reviewer capability 的 Run 承担。

### 6.2 动态协作

agent 可在授权范围内：

- `work_spawn_child`：拆分子工作。
- `work_block`：声明结构化 blocker。
- `question_post`：向 coordinator、指定 agent 或用户提问。
- `review_request`：请求独立复核。
- `artifact_publish`：发布可复用产物。

为避免 agent 无限自增工作：

- Task 设置 `max_work_items`、`max_depth`、`max_attempts`。
- 超预算或扩大范围必须触发 approval。
- 子 Work Item 继承父 Task 的 authorization policy，不能自行放宽。

### 6.3 冲突控制

- Work Item 只解决“谁负责什么”。
- Lock 解决共享资源冲突，如 branch、文件组、服务器变更窗口。
- Lock lease 与 Run lease 联动；Run lost 后自动释放或转入 grace period。
- 对 git 工作，Artifact/Checkpoint 必须记录 repo、branch、base SHA、HEAD 和 dirty 状态。
- reviewer 默认不能是产生被审结果的同一个 Run；是否允许同 agent 不同 session 由 policy 决定。

### 6.4 用户审批

不再要求“每一 Round 都审批”。审批由 policy gate 触发，例如：

- 生产环境写操作。
- 发外部消息、创建 PR、部署、付费调用。
- 超出文件/仓库/主机范围。
- 扩大预算或任务目标。
- coordinator 无法在既有约束内解决的冲突。

approval 也必须是持久对象；用户决定后 Scheduler 自动继续相应 Work Item。

---

## 7. 建议数据模型（v2 轮廓）

不建议直接在现有 `messages` 上不断加 nullable 列。新增 v2 表，再做兼容投影：

```sql
CREATE TABLE schema_migrations (...);

CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  objective TEXT NOT NULL,
  success_criteria_json TEXT NOT NULL,
  constraints_json TEXT NOT NULL,
  authorization_policy_json TEXT NOT NULL,
  context_refs_json TEXT NOT NULL,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  deadline_at TEXT,
  budget_json TEXT NOT NULL,
  plan_version INTEGER NOT NULL DEFAULT 1,
  coordinator_run_id TEXT,
  created_by_agent_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE work_items (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  parent_id TEXT,
  kind TEXT NOT NULL,
  objective TEXT NOT NULL,
  acceptance_json TEXT NOT NULL,
  required_capabilities_json TEXT NOT NULL,
  preferred_agent_id TEXT,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  retry_policy_json TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE work_dependencies (
  work_item_id TEXT NOT NULL,
  depends_on_id TEXT NOT NULL,
  condition TEXT NOT NULL DEFAULT 'succeeded',
  PRIMARY KEY (work_item_id, depends_on_id)
);

CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  native_session_ref TEXT,
  adapter_id TEXT,
  capabilities_json TEXT NOT NULL,
  status TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  lease_expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  ended_at TEXT
);

CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  work_item_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  session_id TEXT,
  status TEXT NOT NULL,
  fencing_token INTEGER NOT NULL,
  lease_expires_at TEXT,
  heartbeat_at TEXT,
  checkpoint_id TEXT,
  failure_code TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT,
  UNIQUE(work_item_id, attempt_no)
);

CREATE TABLE checkpoints (...);
CREATE TABLE artifacts (...);

CREATE TABLE events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  work_item_id TEXT,
  run_id TEXT,
  event_type TEXT NOT NULL,
  actor_agent_id TEXT,
  payload_json TEXT NOT NULL,
  idempotency_key TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(actor_agent_id, idempotency_key)
);

CREATE TABLE deliveries (
  id TEXT PRIMARY KEY,
  event_id INTEGER NOT NULL,
  recipient_kind TEXT NOT NULL,
  recipient_id TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  available_at TEXT NOT NULL,
  lease_expires_at TEXT,
  acked_at TEXT,
  UNIQUE(event_id, recipient_kind, recipient_id)
);

CREATE TABLE outbox (...);
CREATE TABLE approvals (...);
CREATE TABLE resource_locks (...);
```

约束应通过 SQL `CHECK`、外键、唯一索引和 service 状态转换共同保证。时间统一存 UTC ISO 8601；事件排序使用服务端 `event_id`。

### 7.1 现有表的处理

| 现有表 | v2 处理 |
|---|---|
| `root_tasks` | 迁移到 `tasks`，旧 ID 保留 |
| `rounds` | 降级为 `plan_versions` / milestone 投影，不再是强制生命周期 |
| `assignments` | 迁移为 `work_items`；认领历史映射为首个 Run |
| `messages` | 保留只读兼容；新通信写 `events + deliveries` |
| `handoffs` | 内容迁移为 Checkpoint + Artifact + event |
| `locks` | 升级为带 fencing 和 owner Run 的 resource lock |
| `operator_approvals` | 升级为可关联 Task/Work Item/action 的 approvals |
| `audit_events` | 迁移/合并到统一 Event Log，保留原始记录 |

---

## 8. MCP / Agent API 设计

优先提供少量高价值工具，而不是让 agent 手工拼完整状态机。

### 8.1 启动与同步

- `session_start(native_session_ref?, capabilities_json?)`
- `agent_sync(session_id, cursor?, updates_json?)`
- `session_end(session_id, reason?)`

### 8.2 任务与工作

- `task_create(spec_json)`
- `task_get(task_id, include_graph=false)`
- `task_list(status?, role?)`
- `work_claim(work_item_id, session_id, idempotency_key)`
- `work_progress(run_id, fencing_token, progress_json)`
- `checkpoint_save(run_id, fencing_token, checkpoint_json)`
- `work_complete(run_id, fencing_token, result_json)`
- `work_fail(run_id, fencing_token, failure_json)`
- `work_block(run_id, fencing_token, blocker_json)`
- `work_spawn_child(run_id, fencing_token, spec_json)`

### 8.3 协作与控制

- `event_post(task_id, event_type, payload_json, recipient?)`
- `delivery_ack(delivery_ids)`（通常由 `agent_sync` 批量完成）
- `review_submit(run_id, verdict_json)`
- `approval_resolve(approval_id, decision, note)`（operator-only）
- `artifact_publish(run_id, artifact_json)`

### 8.4 快照而非聊天回放

`task_get` / `work_claim` 返回面向恢复的精简快照：

```json
{
  "task": {"objective": "...", "success_criteria": [], "constraints": []},
  "work_item": {"objective": "...", "acceptance": [], "dependencies": []},
  "latest_checkpoint": {},
  "artifacts": [],
  "recent_relevant_events": [],
  "run": {"id": "...", "fencing_token": 4, "lease_expires_at": "..."}
}
```

---

## 9. SQLite 与运行时可靠性

### 9.1 先修现有事务边界

当前代码中 storage 函数有的自行 `commit()`，service 又试图包 `BEGIN IMMEDIATE`；嵌套调用会提前提交，所谓原子事务并不可靠。`create_operator_approval` 和 assignment 完成路径都存在这种风险。

重构规则：

- 只有 Unit of Work 管理 begin/commit/rollback。
- repository/storage 函数永不隐式 commit。
- audit/event 与业务状态在同一事务写入。
- 重试包围整个事务，不包围事务中的单条语句。
- 任何状态转换使用条件 UPDATE 或 version 做 compare-and-swap。

### 9.2 单写者

本机规模继续使用 SQLite，但所有写请求进入一个 Hub write executor/queue：

- MCP tool、Scheduler、adapter 回调都走同一个写入口。
- 写事务短小，不在事务中调用网络/agent。
- 读请求使用独立只读连接。
- `hubctl` 改为调用 Hub operator API，不再直接写 DB。
- WAL、foreign_keys、busy_timeout 在每个连接设置。

这样既保留 SQLite 的低运维成本，也避免并发 `to_thread` 和外部 CLI 形成多写者争锁。

### 9.3 Scheduler 生命周期

Scheduler 通过 FastMCP/ASGI lifespan 启动，并由 systemd 管理整个 Hub 进程。状态全部持久化，进程重启后执行 reconcile：

- 恢复未完成 outbox。
- 回收过期 lease。
- 重算 ready Work Item。
- 不重复创建已存在的有效 Run。

如果未来吞吐量或跨机器需求上升，再把存储替换为 PostgreSQL、把 outbox worker 独立部署；领域模型和 API 不变。

---

## 10. 安全、授权与防失控

- Bearer token 只代表 agent，不代表具体 session；session/run 权限必须额外校验。
- adapter 启动 agent 前应用 Task authorization policy。
- secrets 只保存引用，不进入 event body、checkpoint 或 audit。
- 所有外部副作用使用 idempotency key。
- Run 使用 fencing token 防止僵尸 session 迟到写入。
- 自动拆分、重试、消息、运行时间和成本均有预算。
- 超过重试上限进入可观察的 dead letter / blocked，不做无限循环。
- Task cancel 传播到未开始 Work Item；运行中副作用是否中止由 compensation policy 决定。
- Artifact 带来源、hash 和可见范围；不同任务默认不互相读取私有上下文。

---

## 11. 可观测性

`hub_status` 应从静态版本字符串升级为实际运行状态：

- scheduler 是否存活、最后 tick 时间。
- write queue 深度和最老等待时间。
- ready/offered/running/blocked Work Item 数。
- active/stale Session 和 Run 数。
- outbox pending/retry/dead-letter 数。
- 最近 SQLite busy/lock 次数和事务延迟。
- 每个 adapter 的最后成功投递、唤醒能力等级和健康状态。

还应提供：

- `task_timeline(task_id)`：从 Event Log 生成可读时间线。
- `task_explain(task_id)`：解释为什么未继续推进。
- `run_diagnose(run_id)`：lease、checkpoint、adapter、最后错误。
- operator dashboard/CLI：审批、取消、重试、接管和归档。

系统必须能回答：“现在是谁在做什么、为什么没继续、下一步由谁触发、是否需要我决定”。

---

## 12. 分阶段落地计划

### Phase 0：正确性基线（必须先做）

1. 加测试框架和临时 SQLite 集成测试。
2. 引入 `schema_migrations`，禁止只靠“表是否存在”判断版本。
3. 重构事务边界，修复嵌套 commit/rollback。
4. 所有写操作统一 retry 和 idempotency。
5. `hub_status` 加 DB/scheduler 基础诊断。

验收：并发发送、assignment complete、approval、lock 冲突测试稳定；故障注入后不存在半提交状态。

### Phase 1：Task / Work Item / Run 核心

1. 新建 v2 表和领域 service。
2. 实现 Task spec、Work Item DAG、Run lease/fencing。
3. 实现 Session、Checkpoint、Artifact。
4. 提供 `session_start / task_get / work_claim / work_progress / checkpoint_save / work_complete`。

验收：同 agent 两个 session 不串工作；旧 session 的 stale write 被拒绝；新 session 可在不读取旧聊天的情况下恢复。

### Phase 2：可靠通信与调度

1. Event + Delivery 替代新消息的全局 is_read。
2. Outbox + Scheduler reconcile。
3. `agent_sync` 批量 heartbeat、ack、进度和新事件。
4. 依赖满足、lease 过期、重试、blocked 状态自动推进。

验收：服务在任意关键事务后重启，不丢 offer、不重复有效 claim；无人手工催 inbox 时，在线 runner 能收到并处理下一 Work Item。

### Phase 3：Coordinator 与多 Agent 工作图

1. coordinator lease/failover。
2. 动态子工作、独立 review、approval gate。
3. 资源锁与 Run 联动。
4. 任务预算和循环保护。

验收：一个包含并行研究、实现、独立复核、返修、最终验收的任务可自动走完整状态图。

### Phase 4：逐 agent adapter

按 agent 逐一实现并记录能力等级：Hermes、Claude Code、Codex、Claude Desktop、OpenCode、Trae。

每个 adapter 必须通过：

1. agent 离线时创建任务。
2. Hub 派发。
3. adapter 唤醒或明确进入 manual_resume。
4. claim + heartbeat。
5. 中途 kill session。
6. 新 session/agent 从 checkpoint 恢复。
7. 完成并触发下一依赖。

### Phase 5：迁移与下线旧路径

1. 旧 P2P `message_send/inbox_pull` 进入兼容模式。
2. 历史消息只读保留；新任务默认 v2。
3. 将有效 root task/assignment/handoff 转换为 v2。
4. 观察期后下线 Round 强制流程和全局 `is_read`。

---

## 13. 端到端验收场景

### 场景 A：跨 session 接力

- Session A claim Work Item，写 checkpoint 后被强制结束。
- lease 到期，Run lost。
- Session B 启动时自动获得恢复候选。
- B 获得新 fencing token，从 checkpoint 继续。
- A 迟到提交被拒绝。

### 场景 B：同 agent 多 session 隔离

- Codex 两个 session 分别执行不同 Task/Work Item。
- 两者只能看到与自身 Run/Delivery 相关的工作。
- ack、heartbeat、complete 不互相影响。

### 场景 C：多 agent 并行 + reviewer

- 两个研究 Work Item 并行。
- 依赖完成后实现 Work Item 自动 ready。
- 实现完成自动创建/激活 reviewer Work Item。
- reviewer changes_requested 后生成返修 attempt，最终通过。

### 场景 D：Hub 重启恢复

- 在 outbox 写入后、adapter 发送前重启。
- 重启后 reconcile 继续投递且不产生两个有效 Run。

### 场景 E：无法主动唤醒的桌面 agent

- Hub 正确显示 adapter=L2/manual_resume。
- Task 进入 waiting_agent，而非伪装成已执行。
- 用户打开新 session 后，`session_start` 立即展示恢复包并继续。

### 场景 F：授权边界

- agent 在实现中发现需要部署生产。
- Work Item 进入 `policy_gate` 并创建 approval。
- 未批准前 Scheduler 不派发生产操作。
- 批准后从原 checkpoint 继续。

---

## 14. 实施前最终决策（本方案已给默认值）

| 决策 | 默认选择 |
|---|---|
| 顶层身份 | Task，不新增顶层 Conversation |
| 工作编排 | Work Item DAG，Round 降级为计划版本/里程碑 |
| session 冲突 | 每个 Work Item 多 Run；同一有效 Run 单写者 |
| 接力 | lease 过期自动新 attempt；主动 handoff 可立即转移 |
| stale 写入 | fencing token 严格拒绝 |
| 通信 | immutable Event + per-recipient Delivery |
| 排序 | 服务端 event_id，不接受客户端 seq |
| 定时机制 | Hub Scheduler 持久运行；agent cron 非可靠性依赖 |
| 推送 | adapter/runner 为主，SSE/long-poll 仅优化在线延迟 |
| SQLite | 保留，单写者 + Unit of Work + outbox |
| 人工审批 | policy gate 触发，不按每个 Round 强制 |
| 超时 | Task deadline、Session lease、Run lease、Delivery retry 分离 |
| 旧 API | 兼容读取、渐进迁移，不原地堆叠 nullable 字段 |

---

## 15. 第一批具体开发任务

下一步不应先实现 `conversation_resume`，而应按以下顺序开工：

1. 为现有事务、锁、消息 ack 写回归测试，锁定当前行为。
2. 修复 storage/service 的事务所有权，建立单写者入口。
3. 写 `002_task_runtime_v2.sql`：Task、Work Item、Session、Run、Checkpoint、Event、Delivery、Outbox。
4. 实现 `session_start` 和 `agent_sync`，先解决同 agent 多 session 隔离。
5. 实现 Run lease、checkpoint、fencing，完成跨 session 恢复闭环。
6. 实现 Scheduler/outbox，完成在线 runner 的自动续接。
7. 选择一个真正可常驻的 agent（建议 Hermes）做首个 L3 adapter 试点。
8. 再接 Codex/Claude 等 adapter，并诚实标注无法完全自动唤醒的入口。
9. 通过第 13 节六个端到端场景后，再迁移现有 P2P 工作流。

这一路径先建立任务连续性和可靠状态，再增加自动唤醒范围；不会把系统可用性押在某个 agent 的临时 session、提示词服从度或定时任务能力上。
