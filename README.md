# Agent Hub v0.5

Agent Hub is a task-driven control plane for multi-agent work. A durable `Task`
owns the goal and policy, `WorkItem` nodes form a DAG, and every execution is a
leased, fenced `Run` bound to a short-lived agent `Session`.

The v0.5 model is intentionally clean-slate. It does not read or migrate the old
round/message/assignment schema.

## What is implemented

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

## Install in WSL

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

Because v0.5 has no legacy compatibility, an existing pre-v0.5 database must be
removed before first start. Do this only when the service is stopped and only
when old data is intentionally disposable:

```bash
systemctl --user stop agent-hub.service
rm -f ~/.local/share/agent-hub/hub.db*
systemctl --user daemon-reload
systemctl --user enable --now agent-hub.service
```

There is only one service. The scheduler runs inside the FastMCP lifespan so all
writers share the same process-local write gate.

## Agent bootstrap

1. Call `session_start`; registration is automatic on a fresh database.
2. Read the returned `bootstrap` object for tasks, offers, deliveries and active Runs.
3. Call `agent_sync` periodically to renew the session and receive durable work.
4. Use `work_accept` for scheduler offers, then `work_start`.
5. Save checkpoints at meaningful boundaries.
6. Complete, block, or request approval; do not leave a Run silently hanging.

`since_event_id` is only an ordering hint. Unacknowledged deliveries are always
redelivered until `delivery_ack`, preventing cursor advancement from losing work.

## Adapter contract

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

## Operator workflow

```bash
hubctl status
hubctl tasks
hubctl task <task-id>
hubctl approvals
hubctl approve <approval-id>
hubctl reject <approval-id>
hubctl reconcile
```

Use `task_explain` to answer why a task is waiting, and `task_get` with
`include_graph=true` for a complete cross-session recovery snapshot.

## Verification

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q src tests
```

The test suite covers transactions, migrations, security ownership, genuine
multi-connection claims, DAG transitions, cross-agent recovery, review/rework,
delivery/outbox recovery, coordinator leases, dynamic work, approvals, adapters,
blocking and cancellation.
