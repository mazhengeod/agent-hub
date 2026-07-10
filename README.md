<div align="center">

# Agent Hub

### Task-driven control plane for multi-agent work

[![version](https://img.shields.io/badge/version-0.5.0-4B3FE3)](pyproject.toml)
[![python](https://img.shields.io/badge/python-3.11+-3776AB)](pyproject.toml)
[![tests](https://img.shields.io/badge/tests-67%20passed-1DC981)](#verification)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

**English** | [中文](README.zh-CN.md)

</div>

---

<div align="center">

**[Overview](#overview)** ·
**[Concepts](#concepts)** ·
**[Quick Start](#quick-start)** ·
**[MCP Tools](#mcp-tools)** ·
**[Adapters](#adapter-contract)** ·
**[Operator Guide](#operator-workflow)** ·
**[Architecture](#architecture)** ·
**[Verification](#verification)**

</div>

---

## Overview

Agent Hub is a task-driven control plane for multi-agent work. A durable `Task`
owns the goal and policy, `WorkItem` nodes form a DAG, and every execution is a
leased, fenced `Run` bound to a short-lived agent `Session`.

The v0.5 model is intentionally clean-slate. It does not read or migrate the old
round/message/assignment schema.

### What is implemented

- Multiple concurrent sessions per agent.
- Task participants and task-scoped visibility.
- Coordinator lease, heartbeat, release and failover fencing.
- Work DAG validation, dependency advancement and dynamic child work.
- Scheduler-created offers based on availability, capabilities and capacity.
- Cross-agent recovery from durable checkpoints.
- Work blocking/unblocking, retry budgets, deadlines and cancellation propagation.
- Independent review by default.
- Approval gates that actually block and resume/cancel work.
- Immutable events, per-agent deliveries and idempotent event posting.
- Durable outbox with adapter leasing, ack, retry and dead-letter states.
- Fenced resource locks released when Runs terminate.
- Task snapshot, timeline, explanation and runtime diagnostics.

---

## Concepts

```
Task (sole aggregate root)
├── objective          Goal
├── budget             Limits (max_runs, max_work_items...)
├── auth_policy        Authorization policy
├── coordinator        Coordinator Run (elected)
│
├── WorkItem
│   ├── kind           plan | research | implement | review | verify | operate
│   ├── dependencies   DAG edges (succeeded | failed | completed)
│   ├── retry_policy   Retry strategy
│   └── needs_review   Requires independent review
│
└── Run (one execution attempt)
    ├── agent_id       Executor
    ├── session_id     Bound Session
    ├── fencing_token  Stale-write guard
    ├── lease          Expires -> lost
    └── checkpoint     Recovery snapshot
```

### Reliability tiers

| Tier | Responsibility | Status |
|---|---|---|
| **L1 State-active** | Hub auto-manages deps, leases, retries, timeouts—no agent needed online | ✅ |
| **L2 Delivery-active** | Hub pushes work to Outbox; adapter polls when resident | ✅ |
| **L3 Execution-active** | Adapter wakes the agent runtime | ✅ (resident_runner) |

> **Desktop agent limit**: Claude Desktop, Codex Desktop, etc. have no callable
> API. Hub can at most reach L2 (durable backlog, resume on next session).

---

## Quick Start

### 1. Install (WSL)

```bash
cd ~/agent-hub
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
mkdir -p ~/.config/agent-hub
cp config.example.yaml ~/.config/agent-hub/config.yaml
```

Create `~/.config/agent-hub/agents.env` with one `agent_id=token` entry per
agent. Tokens are read only from the HTTP `Authorization: Bearer ...` header;
they are never MCP tool arguments.

### 2. First start (clean old data)

v0.5 has no legacy compatibility. Remove any pre-v0.5 database before first start:

```bash
systemctl --user stop agent-hub.service
rm -f ~/.local/share/agent-hub/hub.db*
systemctl --user daemon-reload
systemctl --user enable --now agent-hub.service
```

There is only one service. The scheduler runs inside the FastMCP lifespan so all
writers share the same process-local write gate.

### 3. Agent bootstrap

1. Call `session_start`; registration is automatic on a fresh database.
2. Read the returned `bootstrap` object for tasks, offers, deliveries and active Runs.
3. Call `agent_sync` periodically to renew the session and receive durable work.
4. Use `work_accept` for scheduler offers, then `work_start`.
5. Save checkpoints at meaningful boundaries.
6. Complete, block, or request approval; do not leave a Run silently hanging.

`since_event_id` is only an ordering hint. Unacknowledged deliveries are always
redelivered until `delivery_ack`, preventing cursor advancement from losing work.

---

## MCP Tools

37 MCP tools, grouped by function:

### Session

| Tool | Description |
|---|---|
| `session_start` | Start a new session, auto-register agent, return bootstrap |
| `session_heartbeat` | Renew session lease |
| `session_end` | End a session |

### Task

| Tool | Description |
|---|---|
| `task_create` | Create task (objective, criteria, constraints, budget, policy) |
| `task_get` | Get task detail, `include_graph` for full DAG + recovery snapshot |
| `task_list` | List tasks, filter by status |
| `task_plan` | Create work items and dependencies (ref support, DAG validation) |
| `task_start` | Start task (requires Coordinator) |
| `task_cancel` | Cancel task, propagates to all unfinished work items |
| `task_timeline` | Get event timeline |
| `task_explain` | Explain why a task is waiting and what should happen next |
| `task_participant_add` | Add participant (requires Coordinator) |

### Coordinator

| Tool | Description |
|---|---|
| `coordinator_claim` | Claim task Coordinator (lease + fencing token) |
| `coordinator_heartbeat` | Renew Coordinator lease |
| `coordinator_release` | Release Coordinator role |

### Work / Run

| Tool | Description |
|---|---|
| `work_claim` | Claim a work item (auto or specified) |
| `work_accept` | Accept a scheduler offer |
| `work_start` | Start a Run (offered -> running) |
| `work_progress` | Heartbeat to renew lease |
| `work_checkpoint` | Save recovery snapshot |
| `work_complete` | Complete Run (succeeded / failed), auto-releases locks |
| `work_resume` | Resume a lost Run (cross-agent, returns checkpoint) |
| `work_spawn_child` | Dynamically create child work item at runtime |
| `work_block` | Block work (saves checkpoint, waits for unblock) |
| `work_unblock` | Unblock work (requires Coordinator) |

### Review / Approval

| Tool | Description |
|---|---|
| `work_review` | Review work item (approved / rejected, self-review denied by default) |
| `approval_request` | Request operator approval |
| `approval_decide` | Decide approval (blocked work resumes or cancels) |

### Communication

| Tool | Description |
|---|---|
| `agent_sync` | Batch pull: heartbeat + ready work + cursor deliveries + active runs |
| `delivery_ack` | Ack delivery (non-destructive, per-recipient) |
| `event_post` | Post event (idempotent) |

### Adapter

| Tool | Description |
|---|---|
| `adapter_register` | Register adapter (mode / wake_level) |
| `adapter_poll` | Poll and lease outbox entries |
| `adapter_ack` | Ack outbox entry delivery result |

### Diagnostics

| Tool | Description |
|---|---|
| `hub_status` | Runtime diagnostics: version, counts, lease health, migrations |

---

## Adapter Contract

An agent may register one of these modes:

- `manual_resume`: work waits durably for the next session.
- `online_session`: online sessions receive deliveries through `agent_sync`.
- `resident_runner`: a daemon calls `adapter_poll`, starts the agent runtime, then
  calls `adapter_ack`.
- `webhook`: reserved for a runner that owns HTTP delivery; the Hub itself does
  not execute arbitrary URLs or shell commands from database configuration.

The last rule is deliberate: Agent Hub coordinates authority but does not turn
database strings into unsandboxed command execution.

For a resident integration, implement a trusted Python callable accepting one
outbox-entry dictionary, then run:

```bash
export AGENT_HUB_TOKEN='the raw token from agents.env'
hubrunner --adapter-id hermes-runner --handler my_agent_adapter:dispatch
```

The handler may be synchronous or async and returns `None`/`True` for success,
`False` for failure, or `{"success": false, "error": "..."}`. Only the command
line chosen by the operator is imported; payloads cannot select a module or
shell command.

---

## Operator Workflow

```bash
hubctl status              # show hub diagnostics
hubctl tasks               # list tasks
hubctl tasks running       # filter by status
hubctl task <task-id>      # show task detail with work items and runs
hubctl agents              # list registered agents
hubctl approvals           # list pending approvals
hubctl approve <id>        # approve an approval
hubctl reject <id>         # reject an approval
hubctl reconcile           # run scheduler reconcile manually
```

Use `task_explain` to answer why a task is waiting, and `task_get` with
`include_graph=true` for a complete cross-session recovery snapshot.

---

## Architecture

### Transaction boundary

- Only `UnitOfWork` manages `BEGIN` / `COMMIT` / `ROLLBACK`
- `storage` functions **never implicitly commit**
- Event + business state written in the same transaction
- `retry` wraps the whole transaction, not single statements inside it

### Single writer

All writes go through `WriteExecutor`:
- In-process: `threading.Lock` for serialization
- Cross-process: SQLite `BEGIN IMMEDIATE` for mutual exclusion
- Scheduler merged into Hub lifespan → single process

### Migration

- `schema_migrations` table tracks versions (no "does table exist" heuristic)
- Each migration runs in one `BEGIN IMMEDIATE / COMMIT` transaction
- UTF-8 encoding, SHA256 checksum verification
- Custom SQL splitter avoids `executescript` implicit COMMIT

### State machine invariants

- Only `ready` WorkItems of `running` Tasks can be claimed
- `preferred_agent_id`, `required_capabilities`, `authorization_policy` enforced
- Run operations verify session ownership + session active + run ownership
- Fencing token is a concurrency guard, not a substitute for auth
- `complete_run` status whitelist enforced

### Fencing token

- Monotonically increasing; new token per Run / lock acquire / lock renew
- Stale session writes rejected by token mismatch
- Run termination auto-releases all held resource locks

### Delivery pipeline

```
State change
  -> Event (immutable, idempotent)
  -> Delivery (per-recipient, non-destructive ack)
  -> Outbox (adapter lease, retry, dead-letter)
  -> dispatcher delivery
  -> ack / retry / dead-letter
  -> agent_sync cursor (since_event_id)
```

---

## Verification

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src tests
```

The test suite (67 tests, all passing) covers transactions, migrations, security
ownership, genuine multi-connection claims, DAG transitions, cross-agent
recovery, review/rework, delivery/outbox recovery, coordinator leases, dynamic
work, approvals, adapters, blocking and cancellation.

---

<div align="center">

**[Back to top](#agent-hub)**

</div>
