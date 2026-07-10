-- 001_init_v2.sql
-- Agent Hub v2 domain model. No legacy compatibility.
-- Tables: agents, adapters, sessions, tasks, work_items, work_dependencies,
--         runs, checkpoints, artifacts, events, deliveries, outbox,
--         approvals, resource_locks.
-- PRAGMAs are set in get_db(), not here (cannot run inside transaction).

-- Agents
CREATE TABLE IF NOT EXISTS agents (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    capabilities    TEXT NOT NULL DEFAULT '[]',
    token_hash      TEXT NOT NULL DEFAULT '',
    is_active       INTEGER NOT NULL DEFAULT 1,
    last_heartbeat  TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ── Adapter registry: per-agent wake capability ──────────────────
CREATE TABLE IF NOT EXISTS adapters (
    id              TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    mode            TEXT NOT NULL,
    config_json     TEXT NOT NULL DEFAULT '{}',
    wake_level      TEXT NOT NULL DEFAULT 'L2',
    is_healthy      INTEGER NOT NULL DEFAULT 1,
    last_success_at TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (agent_id) REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS idx_adapters_agent ON adapters(agent_id);

-- ── Sessions: short-lived execution endpoints (multiple per agent) ─
CREATE TABLE IF NOT EXISTS sessions (
    id                  TEXT PRIMARY KEY,
    agent_id            TEXT NOT NULL,
    native_session_ref  TEXT,
    adapter_id          TEXT,
    capabilities_json   TEXT NOT NULL DEFAULT '[]',
    status              TEXT NOT NULL DEFAULT 'active',
    last_seen_at        TEXT NOT NULL,
    lease_expires_at    TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    ended_at            TEXT,
    FOREIGN KEY (agent_id) REFERENCES agents(id),
    FOREIGN KEY (adapter_id) REFERENCES adapters(id)
);
CREATE INDEX IF NOT EXISTS idx_sessions_agent_active ON sessions(agent_id, status);
CREATE INDEX IF NOT EXISTS idx_sessions_lease ON sessions(lease_expires_at) WHERE status='active';

-- ── Tasks: the sole top-level business identity ──────────────────
CREATE TABLE IF NOT EXISTS tasks (
    id                          TEXT PRIMARY KEY,
    objective                   TEXT NOT NULL,
    success_criteria_json       TEXT NOT NULL DEFAULT '[]',
    constraints_json            TEXT NOT NULL DEFAULT '{}',
    authorization_policy_json   TEXT NOT NULL DEFAULT '{}',
    context_refs_json           TEXT NOT NULL DEFAULT '[]',
    status                      TEXT NOT NULL DEFAULT 'draft',
    priority                    INTEGER NOT NULL DEFAULT 0,
    deadline_at                 TEXT,
    budget_json                 TEXT NOT NULL DEFAULT '{}',
    plan_version                INTEGER NOT NULL DEFAULT 1,
    coordinator_run_id          TEXT,
    created_by_agent_id         TEXT NOT NULL,
    created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at                  TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at                TEXT,
    FOREIGN KEY (created_by_agent_id) REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, priority DESC);

-- ── Work Items: schedulable work units within a task ─────────────
CREATE TABLE IF NOT EXISTS work_items (
    id                          TEXT PRIMARY KEY,
    task_id                     TEXT NOT NULL,
    parent_id                   TEXT,
    kind                        TEXT NOT NULL,
    objective                   TEXT NOT NULL,
    acceptance_json             TEXT NOT NULL DEFAULT '[]',
    required_capabilities_json  TEXT NOT NULL DEFAULT '[]',
    preferred_agent_id          TEXT,
    status                      TEXT NOT NULL DEFAULT 'pending',
    priority                    INTEGER NOT NULL DEFAULT 0,
    retry_policy_json           TEXT NOT NULL DEFAULT '{"max_attempts":3}',
    needs_review                INTEGER NOT NULL DEFAULT 0,
    version                     INTEGER NOT NULL DEFAULT 1,
    created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at                  TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (task_id) REFERENCES tasks(id),
    FOREIGN KEY (parent_id) REFERENCES work_items(id),
    FOREIGN KEY (preferred_agent_id) REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS idx_work_items_task_status ON work_items(task_id, status);
CREATE INDEX IF NOT EXISTS idx_work_items_status ON work_items(status, priority DESC);

-- ── Work dependencies: DAG edges ─────────────────────────────────
CREATE TABLE IF NOT EXISTS work_dependencies (
    work_item_id    TEXT NOT NULL,
    depends_on_id   TEXT NOT NULL,
    condition       TEXT NOT NULL DEFAULT 'succeeded',
    PRIMARY KEY (work_item_id, depends_on_id),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id),
    FOREIGN KEY (depends_on_id) REFERENCES work_items(id)
);

-- ── Runs: one execution attempt of a work item ───────────────────
CREATE TABLE IF NOT EXISTS runs (
    id                  TEXT PRIMARY KEY,
    work_item_id        TEXT NOT NULL,
    attempt_no          INTEGER NOT NULL,
    agent_id            TEXT NOT NULL,
    session_id          TEXT,
    status              TEXT NOT NULL DEFAULT 'offered',
    fencing_token       INTEGER NOT NULL,
    lease_expires_at    TEXT,
    heartbeat_at        TEXT,
    checkpoint_id       TEXT,
    failure_code        TEXT,
    failure_json        TEXT NOT NULL DEFAULT '{}',
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    started_at          TEXT,
    ended_at            TEXT,
    UNIQUE(work_item_id, attempt_no),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id),
    FOREIGN KEY (agent_id) REFERENCES agents(id),
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);
CREATE INDEX IF NOT EXISTS idx_runs_work_status ON runs(work_item_id, status);
CREATE INDEX IF NOT EXISTS idx_runs_agent_status ON runs(agent_id, status);
CREATE INDEX IF NOT EXISTS idx_runs_lease ON runs(lease_expires_at) WHERE status IN ('offered','claimed','running');

-- ── Checkpoints: cross-session recovery snapshots ────────────────
CREATE TABLE IF NOT EXISTS checkpoints (
    id              TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    snapshot_json   TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_run ON checkpoints(run_id, version);

-- ── Artifacts ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS artifacts (
    id              TEXT PRIMARY KEY,
    run_id          TEXT,
    work_item_id    TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    kind            TEXT NOT NULL,
    ref             TEXT NOT NULL,
    hash            TEXT,
    metadata_json   TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(id),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);
CREATE INDEX IF NOT EXISTS idx_artifacts_task ON artifacts(task_id);

-- ── Events: immutable append-only fact log ───────────────────────
-- No FK constraints: events may reference tasks/work_items during creation
-- before the full transaction commits. Integrity is enforced by the service layer.
CREATE TABLE IF NOT EXISTS events (
    event_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT NOT NULL,
    work_item_id        TEXT,
    run_id              TEXT,
    event_type          TEXT NOT NULL,
    actor_agent_id      TEXT,
    payload_json        TEXT NOT NULL DEFAULT '{}',
    idempotency_key     TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(actor_agent_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, event_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, created_at);

-- ── Deliveries: per-recipient delivery state ─────────────────────
CREATE TABLE IF NOT EXISTS deliveries (
    id              TEXT PRIMARY KEY,
    event_id        INTEGER NOT NULL,
    recipient_kind  TEXT NOT NULL,
    recipient_id    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    available_at    TEXT NOT NULL DEFAULT (datetime('now')),
    lease_expires_at TEXT,
    acked_at        TEXT,
    UNIQUE(event_id, recipient_kind, recipient_id),
    FOREIGN KEY (event_id) REFERENCES events(event_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_recipient ON deliveries(recipient_kind, recipient_id, status);
CREATE INDEX IF NOT EXISTS idx_deliveries_pending ON deliveries(status, available_at) WHERE status='pending';

-- ── Outbox: reliable adapter dispatch ────────────────────────────
CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    retry_at        TEXT,
    delivered_at    TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, retry_at) WHERE status='pending';

-- ── Approvals ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS approvals (
    id              TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL,
    work_item_id    TEXT,
    run_id          TEXT,
    action          TEXT NOT NULL,
    reason          TEXT NOT NULL DEFAULT '',
    decision        TEXT,
    decided_by      TEXT,
    decided_at      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (task_id) REFERENCES tasks(id),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id),
    FOREIGN KEY (run_id) REFERENCES runs(id)
);
CREATE INDEX IF NOT EXISTS idx_approvals_pending ON approvals(task_id) WHERE decision IS NULL;

-- ── Resource locks: fencing-aware (no FK to runs, holder may be coordinator) ─
CREATE TABLE IF NOT EXISTS resource_locks (
    id              TEXT PRIMARY KEY,
    lock_key        TEXT NOT NULL UNIQUE,
    holder_run_id   TEXT NOT NULL,
    resource_type   TEXT NOT NULL DEFAULT 'general',
    resource_id     TEXT NOT NULL DEFAULT '',
    fencing_token   INTEGER NOT NULL,
    expires_at      TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_locks_key ON resource_locks(lock_key);
CREATE INDEX IF NOT EXISTS idx_locks_expires ON resource_locks(expires_at);
