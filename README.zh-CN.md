<div align="center">

# Agent Hub

### 任务驱动的多 Agent 协调控制平面

[![version](https://img.shields.io/badge/version-0.6.0-4B3FE3)](pyproject.toml)
[![python](https://img.shields.io/badge/python-3.11+-3776AB)](pyproject.toml)
[![tests](https://img.shields.io/badge/tests-pytest-1DC981)](#验证)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

[English](README.md) | **中文**

</div>

---

<div align="center">

**[概览](#概览)** ·
**[核心概念](#核心概念)** ·
**[快速开始](#快速开始)** ·
**[MCP 工具](#mcp-工具)** ·
**[Adapter 机制](#adapter-机制)** ·
**[运维手册](#运维手册)** ·
**[架构设计](#架构设计)** ·
**[验证](#验证)**

</div>

---

## 概览

Agent Hub 是一个面向多 Agent 协作的任务驱动控制平面。它不是消息队列，也不是简单的 RPC 注册中心--它是一个完整的**任务编排系统**：

- **Task** 是唯一的顶层业务身份，拥有目标、策略和预算
- **WorkItem** 是 Task 内可调度的最小工作单元，通过 DAG 表达依赖关系
- **Run** 是一次具体的执行尝试，绑定租约和 fencing token，可跨 session 恢复
- **Session** 是 Agent 的短生命周期执行端点，同一 Agent 可同时拥有多个活跃 Session

v0.6 可通过带校验和的 migration 原地升级 v0.5 数据库；更早的 round/message/assignment schema 仍不受支持。设计目标：**让 Agent 可以在任何时候中断、恢复、切换，而任务状态永不丢失**。

### 已实现能力

| 能力 | 说明 |
|---|---|
| 多 Session 并行 | 同一 Agent 可同时拥有多个活跃 Session，各自独立租约 |
| Coordinator 选举 | 租约 + fencing token，支持心跳、释放和故障接管 |
| Work DAG 编排 | 依赖校验、环检测、自动推进、动态子工作拆分 |
| 调度器主动派活 | 基于可用性、能力和容量自动创建 Offer |
| 跨 Agent 恢复 | 从持久化 Checkpoint 接管，不锁定原 Agent |
| 阻塞 / 解阻塞 | 工作可被阻塞并恢复，不丢失上下文 |
| 重试预算 | 每个任务有 Run 上限，防止无限重试爆炸 |
| 截止时间取消 | 超过 deadline 的任务自动取消剩余工作 |
| 独立 Review | 默认不允许自审，需不同 Agent 审核 |
| 审批门控 | 审批请求真正阻塞工作流，决定后恢复或取消 |
| 不可变事件流 | 事件 + 每接收方独立 Delivery，幂等投递 |
| 可靠 Outbox | Adapter 租约、ack、重试、dead-letter 完整闭环 |
| Fencing 资源锁 | Run 终止时自动释放其持有的全部锁 |
| 运行诊断 | 任务快照、时间线、原因解释、健康检查 |

---

## 核心概念

### 领域模型关系

```
Task（任务 - 唯一聚合根）
├── objective          目标
├── budget             预算（max_runs, max_work_items...）
├── auth_policy        授权策略
├── coordinator        Coordinator Run（选举产生）
│
├── WorkItem（工作项）
│   ├── kind           plan | research | implement | review | verify | operate
│   ├── dependencies   DAG 边（succeeded | failed | completed 条件）
│   ├── retry_policy   重试策略
│   └── needs_review   是否需要独立审核
│
└── Run（执行尝试）
    ├── agent_id       执行者
    ├── session_id     绑定的 Session
    ├── fencing_token  防过期写入栅栏
    ├── lease          租约（过期自动标记 lost）
    └── checkpoint     恢复快照
```

### 可靠性三层模型

| 层级 | 职责 | 实现状态 |
|---|---|---|
| **L1 状态主动** | Hub 自动管理依赖、租约、重试、超时--不需要 Agent 在线 | ✅ 已实现 |
| **L2 投递主动** | Hub 把工作投递到 Outbox，Adapter 常驻即可拉取 | ✅ 已实现 |
| **L3 执行主动** | Adapter 真正唤醒 Agent 运行时 | ✅ 已实现（resident_runner） |

> **桌面 Agent 限制**：Claude Desktop、Codex Desktop 等没有可调用 API 的客户端，Hub 最多做到 L2（可靠记录待办，等下次开 Session 时恢复）。任何方案都不能靠 MCP 消息把已关闭的客户端叫醒。

---

## 快速开始

### 1. 安装（WSL 环境）

```bash
python3 -m venv .venv
.venv/bin/pip install agent-hub-mcp
```

### 2. 配置

```bash
mkdir -p ~/.config/agent-hub
cp config.example.yaml ~/.config/agent-hub/config.yaml
```

创建 `~/.config/agent-hub/agents.env`，每个 Agent 一行：

```env
codex=your-codex-token
trae=your-trae-token
claude=your-claude-token
hermes=your-hermes-token
opencode=your-opencode-token
```

> **安全须知**：Token 仅从 HTTP `Authorization: Bearer ...` header 读取，绝不作为 MCP 工具参数暴露。

### 3. 备份并启动

升级时不要删除数据库。先用 SQLite 在线备份并验证，再重启服务：

```bash
hubctl doctor
hubctl backup
systemctl --user daemon-reload
systemctl --user enable --now agent-hub.service
hubctl doctor
```

只有一个服务进程。Scheduler 运行在 FastMCP lifespan 内部，所有写入共享同一个进程内单写者门控。

### 4. Agent 启动流程

1. 调用 `session_start`，新数据库下自动注册 Agent
2. 读取返回的 `bootstrap` 对象，获取任务、Offer、投递和活跃 Run
3. 定期调用 `agent_sync` 续约 Session 并接收持久化工作
4. 对调度器 Offer 调用 `work_accept`，然后 `work_start`
5. 在有意义的边界保存 Checkpoint
6. 完成、阻塞或请求审批--不要让 Run 静默挂起

> `agent_sync` 只标记 Delivery 已观察并推进观察游标，不会确认处理成功。未 ack 的 Delivery 会一直重投，直到 `delivery_ack` 或 `delivery_ack_batch` 成功。

---

## MCP 工具

共 38 个 MCP 工具，按功能分组：

### Session 管理

| 工具 | 说明 |
|---|---|
| `session_start` | 启动新 Session，自动注册 Agent，返回 bootstrap 信息 |
| `session_heartbeat` | 续约 Session 租约 |
| `session_end` | 结束 Session |

### Task 管理

| 工具 | 说明 |
|---|---|
| `task_create` | 创建任务（目标、成功标准、约束、预算、授权策略） |
| `task_get` | 获取任务详情，支持 `include_graph` 返回完整 DAG 和恢复快照 |
| `task_list` | 列出任务，可按状态过滤 |
| `task_plan` | 创建 WorkItem 和依赖（支持 ref 引用、DAG 校验） |
| `task_start` | 启动任务（需 Coordinator 权限） |
| `task_cancel` | 取消任务，传播到所有未完成的 WorkItem |
| `task_timeline` | 获取任务事件时间线 |
| `task_explain` | 解释任务为什么在等待、下一步该做什么 |
| `task_participant_add` | 添加任务参与者（需 Coordinator 权限） |

### Coordinator

| 工具 | 说明 |
|---|---|
| `coordinator_claim` | 竞选任务 Coordinator（租约 + fencing token） |
| `coordinator_heartbeat` | 续约 Coordinator 租约 |
| `coordinator_release` | 释放 Coordinator 职责 |

### Work / Run 执行

| 工具 | 说明 |
|---|---|
| `work_claim` | 认领工作项（自动或指定） |
| `work_accept` | 接受调度器 Offer |
| `work_start` | 启动 Run（offered -> running） |
| `work_progress` | 心跳续约 |
| `work_checkpoint` | 保存恢复快照 |
| `work_complete` | 完成 Run（succeeded / failed），自动释放锁 |
| `work_resume` | 恢复丢失的 Run（跨 Agent，返回 Checkpoint） |
| `work_spawn_child` | 运行时动态创建子工作项 |
| `work_block` | 阻塞工作（保存 Checkpoint，等待 unblock） |
| `work_unblock` | 解除阻塞（需 Coordinator 权限） |

### Review / 审批

| 工具 | 说明 |
|---|---|
| `work_review` | 审核工作项（approved / rejected，默认禁止自审） |
| `approval_request` | 请求操作员审批 |
| `approval_decide` | 决定审批（阻塞中的工作恢复或取消） |

### 通信与同步

| 工具 | 说明 |
|---|---|
| `agent_sync` | 批量拉取：心跳 + 待办工作 + 游标投递 + 活跃 Run |
| `delivery_ack` | 确认投递（非破坏性，每接收方独立） |
| `delivery_ack_batch` | 原子确认有上限的一批投递 |
| `event_post` | 幂等发布事件 |

### Adapter

| 工具 | 说明 |
|---|---|
| `adapter_register` | 注册 Adapter（mode / wake_level） |
| `adapter_poll` | 拉取并租约 Outbox 条目 |
| `adapter_ack` | 确认 Outbox 条目投递结果 |

### 诊断

| 工具 | 说明 |
|---|---|
| `hub_status` | 运行诊断：版本、计数、租约健康、migration |

---

## Adapter 机制

Agent 可注册以下模式之一：

| 模式 | 说明 | 唤醒级别 |
|---|---|---|
| `manual_resume` | 工作持久化等待，下次 Session 恢复 | L2 |
| `online_session` | 在线 Session 通过 `agent_sync` 接收 | L2 |
| `resident_runner` | 常驻 daemon 调用 `adapter_poll`，启动 Agent 运行时，再 `adapter_ack` | L3 |
| `webhook` | 预留，Hub 本身不从数据库配置执行任意 URL 或 shell 命令 | L3 |

> **安全设计**：Agent Hub 协调权限，但不会把数据库字符串变成无沙箱的命令执行。

### Resident Runner 用法

实现一个受信任的 Python callable，接收一个 outbox 条目字典：

```python
# my_agent_adapter.py
def dispatch(entry: dict):
    """处理 Hub 派发的工作"""
    work_item_id = entry.get("work_item_id")
    objective = entry.get("objective")
    # 启动 Agent 运行时...
    return True  # None / True = 成功, False = 失败
```

然后运行：

```bash
export AGENT_HUB_TOKEN='the raw token from agents.env'
hubrunner --adapter-id hermes-runner --handler my_agent_adapter:dispatch
```

Handler 可以是同步或异步，返回值规则：
- `None` / `True` -> 成功
- `False` -> 失败（触发重试）
- `{"success": false, "error": "..."}` -> 失败带错误信息

只有操作员在命令行选择的 handler 会被导入；Outbox 载荷不能选择模块或 shell 命令。

---

## 运维手册

### hubctl 命令

```bash
hubctl status              # 查看运行诊断
hubctl status --json       # 机器可读诊断
hubctl doctor              # 严格只读的数据库、配置和权限检查
hubctl backup              # SQLite 在线备份并生成 SHA256 旁车文件
hubctl tasks               # 列出任务
hubctl tasks running       # 按状态过滤
hubctl task <task-id>      # 查看任务详情（含 WorkItem 和 Run）
hubctl agents              # 列出已注册 Agent
hubctl approvals           # 列出待审批
hubctl approve <id>        # 批准审批
hubctl reject <id>         # 拒绝审批
hubctl reconcile           # 手动触发调度器 reconcile
```

### 配置项

配置文件位于 `~/.config/agent-hub/config.yaml`，详见 [config.example.yaml](config.example.yaml)：

```yaml
session_lease_seconds: 300          # Session 租约
run_lease_seconds: 600              # Run 租约
offer_lease_seconds: 900            # Offer 租约
coordinator_lease_seconds: 600      # Coordinator 租约
reconcile_interval_seconds: 10      # 调度器周期

max_run_attempts: 3                 # 单 WorkItem 最大重试
max_runs_per_task: 100              # 单 Task 最大 Run 数（预算保护）
max_work_items_per_task: 100        # 单 Task 最大 WorkItem 数
max_work_depth: 6                   # DAG 最大深度
max_simultaneous_runs_per_agent: 4  # 单 Agent 最大并发 Run

operator_agent_ids:                 # 可决定审批的 Agent
  - codex
```

### 诊断技巧

- `task_explain` 回答"任务为什么在等"
- `task_get(include_graph=true)` 获取完整跨 session 恢复快照
- `hub_status` 查看租约健康（stale_runs / stale_sessions / pending_outbox）

---

## 架构设计

### 事务边界

- 只有 `UnitOfWork` 管理 `BEGIN` / `COMMIT` / `ROLLBACK`
- `storage` 层函数**永不隐式 commit**
- 事件 + 业务状态在同一事务内写入
- `retry` 包装整个事务，不是事务内的单条语句

### 单写者

所有写入通过 `WriteExecutor` 序列化：
- 进程内：`threading.Lock` 保证序列化
- 跨进程：SQLite `BEGIN IMMEDIATE` 提供互斥
- Scheduler 合并进 Hub lifespan，保证单进程

### Migration

- 使用 `schema_migrations` 表追踪版本，不靠"表是否存在"判断
- 每个 migration 在单个 `BEGIN IMMEDIATE / COMMIT` 事务内执行
- UTF-8 编码读取，SHA256 checksum 校验
- 自定义 SQL splitter 避免 `executescript` 的隐式 COMMIT

### 状态机不变量

- 只有 `running` 状态的 Task 的 `ready` WorkItem 才能被 claim
- `preferred_agent_id`、`required_capabilities`、`authorization_policy` 强制校验
- Run 操作验证 session 归属 + session 活跃 + run 归属
- Fencing token 是并发栅栏，不替代鉴权
- `complete_run` 的 status 有白名单

### Fencing Token

- 单调递增，每次新 Run / 锁获取 / 锁续期都分配新 token
- 旧 session 的过期写入因 token 不匹配被拒绝
- Run 终止时自动释放其持有的全部资源锁

### 投递闭环

```
状态变化
  -> Event（不可变，幂等）
  -> Delivery（每接收方独立，非破坏性 ack）
  -> Outbox（Adapter 租约、重试、dead-letter）
  -> dispatcher 投递
  -> ack / retry / dead-letter
  -> agent_sync cursor（since_event_id）
```

---

## 验证

```bash
# 运行测试套件
.venv/bin/python -m pytest -q

# 编译检查
.venv/bin/python -m compileall -q src tests

# 构建 wheel 和 sdist
python -m build
```

测试覆盖事务、migration、输入校验、权限、DAG、恢复、review、投递与
outbox 语义、运维诊断、审批和 adapter。CI 还会在干净环境安装构建出的
wheel，并检查 migration 是否随包发布。

生产切换前请阅读[升级与回滚](docs/upgrade-and-rollback.md)。

历史测试文件覆盖如下：

| 测试文件 | 用例数 | 覆盖场景 |
|---|---|---|
| test_transactions.py | 5 | 事务边界、UoW 提交/回滚、retry on busy |
| test_migrations.py | 9 | UTF-8、checksum、幂等、SQL splitter、fencing 序列 |
| test_security.py | 13 | 多 Session、越权拒绝、preferred_agent、capability、stale token |
| test_lifecycle.py | 7 | 完整生命周期、失败重试、跨 Agent 恢复、review 返修 |
| test_dag.py | 4 | 环检测、自依赖、线性链、条件依赖 |
| test_delivery.py | 14 | 投递创建、cursor、ack、outbox 创建/处理/重启存活、锁全场景 |
| test_adapters.py | 2 | Resident adapter 接收调度、失败重试后 dead-letter |
| test_orchestration.py | 10 | Coordinator 选举、动态子工作、预算、block/unblock、审批门控、自审防护、取消传播、deadline |
| test_runner.py | 4 | Handler 加载、参数校验、结果解释、异常捕获 |

---

<div align="center">

**[回到顶部](#agent-hub)**

</div>
