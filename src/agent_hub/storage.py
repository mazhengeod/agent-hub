"""Repository layer: pure data access functions.

CRITICAL RULE: No function in this module ever calls commit() or rollback().
The caller (UnitOfWork / WriteExecutor) owns the transaction boundary.
This fixes the v1 nested-commit bug where storage.commit() inside a
service-level BEGIN IMMEDIATE prematurely flushed partial state.

All write functions take a conn that is already inside a transaction.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from .db import now_iso, new_id, next_fencing_token
from .models import (
    Agent, Adapter, Session, Task, WorkItem, WorkDependency, Run,
    Checkpoint, Artifact, Event, Delivery, Approval, ResourceLock,
)


def _row_dict(row: sqlite3.Row) -> dict:
    return dict(row) if row else {}


# ════════════════════════════════════════════════════════════════════
#  Agents
# ════════════════════════════════════════════════════════════════════

def upsert_agent(conn: sqlite3.Connection, agent_id: str, name: str,
                 capabilities: str = "[]", token_hash: str = "") -> Agent:
    conn.execute(
        """INSERT INTO agents (id, name, capabilities, token_hash, is_active, updated_at)
           VALUES (?, ?, ?, ?, 1, ?)
           ON CONFLICT(id) DO UPDATE SET
             name=excluded.name,
             capabilities=excluded.capabilities,
             token_hash=excluded.token_hash,
             updated_at=excluded.updated_at""",
        (agent_id, name, capabilities, token_hash, now_iso()),
    )
    return get_agent(conn, agent_id)  # type: ignore[return-value]


def get_agent(conn: sqlite3.Connection, agent_id: str) -> Optional[Agent]:
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row:
        return None
    d = _row_dict(row)
    d["is_active"] = bool(d["is_active"])
    return Agent(**d)


def update_heartbeat(conn: sqlite3.Connection, agent_id: str):
    conn.execute(
        "UPDATE agents SET last_heartbeat=?, updated_at=? WHERE id=?",
        (now_iso(), now_iso(), agent_id),
    )


def list_agents(conn: sqlite3.Connection) -> list[Agent]:
    rows = conn.execute("SELECT * FROM agents ORDER BY created_at").fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["is_active"] = bool(d["is_active"])
        result.append(Agent(**d))
    return result


# ════════════════════════════════════════════════════════════════════
#  Adapters
# ════════════════════════════════════════════════════════════════════

def upsert_adapter(conn: sqlite3.Connection, agent_id: str, mode: str,
                   config_json: str = "{}", wake_level: str = "L2",
                   adapter_id: Optional[str] = None) -> Adapter:
    aid = adapter_id or f"{agent_id}-adapter"
    conn.execute(
        """INSERT INTO adapters (id, agent_id, mode, config_json, wake_level, is_healthy)
           VALUES (?, ?, ?, ?, ?, 1)
           ON CONFLICT(id) DO UPDATE SET
             mode=excluded.mode, config_json=excluded.config_json,
             wake_level=excluded.wake_level""",
        (aid, agent_id, mode, config_json, wake_level),
    )
    row = conn.execute("SELECT * FROM adapters WHERE id=?", (aid,)).fetchone()
    d = _row_dict(row)
    d["is_healthy"] = bool(d["is_healthy"])
    return Adapter(**d)


def get_adapters_for_agent(conn: sqlite3.Connection, agent_id: str) -> list[Adapter]:
    rows = conn.execute("SELECT * FROM adapters WHERE agent_id=?", (agent_id,)).fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["is_healthy"] = bool(d["is_healthy"])
        result.append(Adapter(**d))
    return result


# ════════════════════════════════════════════════════════════════════
#  Sessions
# ════════════════════════════════════════════════════════════════════

def create_session(conn: sqlite3.Connection, session_id: str, agent_id: str,
                   native_session_ref: Optional[str], adapter_id: Optional[str],
                   capabilities_json: str, lease_expires_at: str) -> Session:
    conn.execute(
        """INSERT INTO sessions (id, agent_id, native_session_ref, adapter_id,
           capabilities_json, status, last_seen_at, lease_expires_at)
           VALUES (?, ?, ?, ?, ?, 'active', ?, ?)""",
        (session_id, agent_id, native_session_ref, adapter_id,
         capabilities_json, now_iso(), lease_expires_at),
    )
    return get_session(conn, session_id)  # type: ignore[return-value]


def get_session(conn: sqlite3.Connection, session_id: str) -> Optional[Session]:
    row = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if not row:
        return None
    return Session(**_row_dict(row))


def get_active_session_for_agent(conn: sqlite3.Connection, agent_id: str) -> Optional[Session]:
    row = conn.execute(
        "SELECT * FROM sessions WHERE agent_id=? AND status='active' ORDER BY created_at DESC LIMIT 1",
        (agent_id,),
    ).fetchone()
    if not row:
        return None
    return Session(**_row_dict(row))


def touch_session(conn: sqlite3.Connection, session_id: str, lease_expires_at: str):
    conn.execute(
        "UPDATE sessions SET last_seen_at=?, lease_expires_at=? WHERE id=? AND status='active'",
        (now_iso(), lease_expires_at, session_id),
    )


def end_session(conn: sqlite3.Connection, session_id: str):
    conn.execute(
        "UPDATE sessions SET status='ended', ended_at=? WHERE id=?",
        (now_iso(), session_id),
    )


def expire_stale_sessions(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "UPDATE sessions SET status='lost' WHERE status='active' AND lease_expires_at < ?",
        (now_iso(),),
    )
    return cur.rowcount


# ════════════════════════════════════════════════════════════════════
#  Tasks
# ════════════════════════════════════════════════════════════════════

def create_task(conn: sqlite3.Connection, task_id: str, objective: str,
                created_by_agent_id: str, **fields) -> Task:
    success_criteria = fields.get("success_criteria_json", "[]")
    constraints = fields.get("constraints_json", "{}")
    auth_policy = fields.get("authorization_policy_json", "{}")
    context_refs = fields.get("context_refs_json", "[]")
    priority = fields.get("priority", 0)
    deadline = fields.get("deadline_at")
    budget = fields.get("budget_json", "{}")
    conn.execute(
        """INSERT INTO tasks (id, objective, success_criteria_json, constraints_json,
           authorization_policy_json, context_refs_json, status, priority,
           deadline_at, budget_json, created_by_agent_id)
           VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?)""",
        (task_id, objective, success_criteria, constraints, auth_policy,
         context_refs, priority, deadline, budget, created_by_agent_id),
    )
    return get_task(conn, task_id)  # type: ignore[return-value]


def get_task(conn: sqlite3.Connection, task_id: str) -> Optional[Task]:
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        return None
    return Task(**_row_dict(row))


def update_task_status(conn: sqlite3.Connection, task_id: str, status: str,
                       **extra) -> bool:
    sets = ["status=?", "updated_at=?"]
    vals = [status, now_iso()]
    for k in ("completed_at", "coordinator_run_id", "plan_version"):
        if k in extra:
            sets.append(f"{k}=?")
            vals.append(extra[k])
    vals.append(task_id)
    cur = conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", vals)
    return cur.rowcount > 0


def list_tasks(conn: sqlite3.Connection, status: Optional[str] = None,
               limit: int = 50) -> list[Task]:
    if status:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status=? ORDER BY priority DESC, created_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [Task(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Work Items + Dependencies
# ════════════════════════════════════════════════════════════════════

def create_work_item(conn: sqlite3.Connection, task_id: str, kind: str,
                     objective: str, **fields) -> WorkItem:
    wi_id = new_id()
    conn.execute(
        """INSERT INTO work_items (id, task_id, parent_id, kind, objective,
           acceptance_json, required_capabilities_json, preferred_agent_id,
           status, priority, retry_policy_json, needs_review)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
        (wi_id, task_id, fields.get("parent_id"), kind, objective,
         fields.get("acceptance_json", "[]"),
         fields.get("required_capabilities_json", "[]"),
         fields.get("preferred_agent_id"),
         fields.get("priority", 0),
         fields.get("retry_policy_json", '{"max_attempts":3}'),
         1 if fields.get("needs_review") else 0),
    )
    return get_work_item(conn, wi_id)  # type: ignore[return-value]


def get_work_item(conn: sqlite3.Connection, wi_id: str) -> Optional[WorkItem]:
    row = conn.execute("SELECT * FROM work_items WHERE id=?", (wi_id,)).fetchone()
    if not row:
        return None
    d = _row_dict(row)
    d["needs_review"] = bool(d["needs_review"])
    return WorkItem(**d)


def add_dependency(conn: sqlite3.Connection, work_item_id: str,
                   depends_on_id: str, condition: str = "succeeded"):
    conn.execute(
        """INSERT OR IGNORE INTO work_dependencies (work_item_id, depends_on_id, condition)
           VALUES (?, ?, ?)""",
        (work_item_id, depends_on_id, condition),
    )


def get_dependencies(conn: sqlite3.Connection, work_item_id: str) -> list[WorkDependency]:
    rows = conn.execute(
        "SELECT * FROM work_dependencies WHERE work_item_id=?",
        (work_item_id,),
    ).fetchall()
    return [WorkDependency(**_row_dict(r)) for r in rows]


def get_dependents(conn: sqlite3.Connection, work_item_id: str) -> list[WorkDependency]:
    rows = conn.execute(
        "SELECT * FROM work_dependencies WHERE depends_on_id=?",
        (work_item_id,),
    ).fetchall()
    return [WorkDependency(**_row_dict(r)) for r in rows]


def update_work_item_status(conn: sqlite3.Connection, wi_id: str, status: str,
                            version: Optional[int] = None) -> bool:
    """Conditional UPDATE with optimistic version check (compare-and-swap)."""
    if version is not None:
        cur = conn.execute(
            """UPDATE work_items SET status=?, version=version+1, updated_at=?
               WHERE id=? AND version=?""",
            (status, now_iso(), wi_id, version),
        )
    else:
        cur = conn.execute(
            "UPDATE work_items SET status=?, version=version+1, updated_at=? WHERE id=?",
            (status, now_iso(), wi_id),
        )
    return cur.rowcount > 0


def list_work_items(conn: sqlite3.Connection, task_id: str,
                    status: Optional[str] = None) -> list[WorkItem]:
    if status:
        rows = conn.execute(
            "SELECT * FROM work_items WHERE task_id=? AND status=? ORDER BY priority DESC, created_at",
            (task_id, status),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM work_items WHERE task_id=? ORDER BY priority DESC, created_at",
            (task_id,),
        ).fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["needs_review"] = bool(d["needs_review"])
        result.append(WorkItem(**d))
    return result


def list_ready_work_items(conn: sqlite3.Connection, agent_id: Optional[str] = None,
                          limit: int = 20) -> list[WorkItem]:
    """Work items whose dependencies are all satisfied and are ready to run."""
    rows = conn.execute(
        """SELECT wi.* FROM work_items wi
           WHERE wi.status = 'ready'
           AND (wi.preferred_agent_id IS NULL OR wi.preferred_agent_id = ?)
           ORDER BY wi.priority DESC, wi.created_at
           LIMIT ?""",
        (agent_id, limit),
    ).fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["needs_review"] = bool(d["needs_review"])
        result.append(WorkItem(**d))
    return result


# ════════════════════════════════════════════════════════════════════
#  Runs
# ════════════════════════════════════════════════════════════════════

def create_run(conn: sqlite3.Connection, work_item_id: str, agent_id: str,
               session_id: Optional[str], lease_expires_at: Optional[str]) -> Run:
    run_id = new_id()
    token = next_fencing_token(conn)
    attempt_row = conn.execute(
        "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS next FROM runs WHERE work_item_id=?",
        (work_item_id,),
    ).fetchone()
    attempt_no = attempt_row["next"] if attempt_row else 1
    conn.execute(
        """INSERT INTO runs (id, work_item_id, attempt_no, agent_id, session_id,
           status, fencing_token, lease_expires_at)
           VALUES (?, ?, ?, ?, ?, 'offered', ?, ?)""",
        (run_id, work_item_id, attempt_no, agent_id, session_id, token, lease_expires_at),
    )
    return get_run(conn, run_id)  # type: ignore[return-value]


def get_run(conn: sqlite3.Connection, run_id: str) -> Optional[Run]:
    row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        return None
    return Run(**_row_dict(row))


def get_active_run_for_work_item(conn: sqlite3.Connection, work_item_id: str) -> Optional[Run]:
    row = conn.execute(
        "SELECT * FROM runs WHERE work_item_id=? AND status IN ('offered','claimed','running') ORDER BY attempt_no DESC LIMIT 1",
        (work_item_id,),
    ).fetchone()
    if not row:
        return None
    return Run(**_row_dict(row))


def claim_run(conn: sqlite3.Connection, run_id: str, fencing_token: int,
              session_id: str, lease_expires_at: str) -> bool:
    """Transition offered -> claimed. Requires correct fencing token."""
    cur = conn.execute(
        """UPDATE runs SET status='claimed', session_id=?, lease_expires_at=?,
           started_at=?, heartbeat_at=?
           WHERE id=? AND fencing_token=? AND status='offered'""",
        (session_id, lease_expires_at, now_iso(), now_iso(), run_id, fencing_token),
    )
    return cur.rowcount > 0


def start_run(conn: sqlite3.Connection, run_id: str, fencing_token: int,
              session_id: str, lease_expires_at: str) -> bool:
    """Transition offered/claimed -> running."""
    cur = conn.execute(
        """UPDATE runs SET status='running', session_id=?, lease_expires_at=?, heartbeat_at=?
           WHERE id=? AND fencing_token=? AND status IN ('offered','claimed','running')""",
        (session_id, lease_expires_at, now_iso(), run_id, fencing_token),
    )
    return cur.rowcount > 0


def heartbeat_run(conn: sqlite3.Connection, run_id: str, fencing_token: int,
                  lease_expires_at: str) -> bool:
    cur = conn.execute(
        """UPDATE runs SET heartbeat_at=?, lease_expires_at=?
           WHERE id=? AND fencing_token=? AND status='running'""",
        (now_iso(), lease_expires_at, run_id, fencing_token),
    )
    return cur.rowcount > 0


def complete_run(conn: sqlite3.Connection, run_id: str, fencing_token: int,
                 status: str, failure_code: Optional[str] = None,
                 failure_json: str = "{}") -> bool:
    """Transition running -> succeeded/failed. Requires correct fencing token."""
    cur = conn.execute(
        """UPDATE runs SET status=?, failure_code=?, failure_json=?, ended_at=?
           WHERE id=? AND fencing_token=? AND status='running'""",
        (status, failure_code, failure_json, now_iso(), run_id, fencing_token),
    )
    return cur.rowcount > 0


def cancel_run(conn: sqlite3.Connection, run_id: str, fencing_token: int) -> bool:
    cur = conn.execute(
        "UPDATE runs SET status='cancelled', ended_at=? WHERE id=? AND fencing_token=? AND status IN ('offered','claimed','running')",
        (now_iso(), run_id, fencing_token),
    )
    return cur.rowcount > 0


def expire_stale_runs(conn: sqlite3.Connection) -> list[str]:
    """Mark runs with expired leases as 'lost'. Returns list of lost run ids."""
    rows = conn.execute(
        "SELECT id FROM runs WHERE status IN ('offered','claimed','running') AND lease_expires_at < ?",
        (now_iso(),),
    ).fetchall()
    lost_ids = [r["id"] for r in rows]
    if lost_ids:
        conn.execute(
            "UPDATE runs SET status='lost', ended_at=? WHERE id IN ({})".format(
                ",".join("?" * len(lost_ids))
            ),
            [now_iso()] + lost_ids,
        )
    return lost_ids


def list_runs_for_agent(conn: sqlite3.Connection, agent_id: str,
                        status: Optional[str] = None) -> list[Run]:
    if status:
        rows = conn.execute(
            "SELECT * FROM runs WHERE agent_id=? AND status=? ORDER BY created_at DESC",
            (agent_id, status),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM runs WHERE agent_id=? ORDER BY created_at DESC",
            (agent_id,),
        ).fetchall()
    return [Run(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Checkpoints
# ════════════════════════════════════════════════════════════════════

def save_checkpoint(conn: sqlite3.Connection, run_id: str,
                    snapshot_json: str) -> Checkpoint:
    cp_id = new_id()
    version_row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS next FROM checkpoints WHERE run_id=?",
        (run_id,),
    ).fetchone()
    version = version_row["next"] if version_row else 1
    conn.execute(
        "INSERT INTO checkpoints (id, run_id, version, snapshot_json) VALUES (?, ?, ?, ?)",
        (cp_id, run_id, version, snapshot_json),
    )
    conn.execute("UPDATE runs SET checkpoint_id=? WHERE id=?", (cp_id, run_id))
    row = conn.execute("SELECT * FROM checkpoints WHERE id=?", (cp_id,)).fetchone()
    return Checkpoint(**_row_dict(row))


def get_latest_checkpoint(conn: sqlite3.Connection, run_id: str) -> Optional[Checkpoint]:
    row = conn.execute(
        "SELECT * FROM checkpoints WHERE run_id=? ORDER BY version DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    if not row:
        return None
    return Checkpoint(**_row_dict(row))


# ════════════════════════════════════════════════════════════════════
#  Artifacts
# ════════════════════════════════════════════════════════════════════

def create_artifact(conn: sqlite3.Connection, work_item_id: str, task_id: str,
                    kind: str, ref: str, run_id: Optional[str] = None,
                    hash_val: Optional[str] = None,
                    metadata_json: str = "{}") -> Artifact:
    art_id = new_id()
    conn.execute(
        """INSERT INTO artifacts (id, run_id, work_item_id, task_id, kind, ref, hash, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (art_id, run_id, work_item_id, task_id, kind, ref, hash_val, metadata_json),
    )
    row = conn.execute("SELECT * FROM artifacts WHERE id=?", (art_id,)).fetchone()
    return Artifact(**_row_dict(row))


def list_artifacts(conn: sqlite3.Connection, task_id: Optional[str] = None,
                   work_item_id: Optional[str] = None) -> list[Artifact]:
    if task_id:
        rows = conn.execute("SELECT * FROM artifacts WHERE task_id=? ORDER BY created_at", (task_id,)).fetchall()
    elif work_item_id:
        rows = conn.execute("SELECT * FROM artifacts WHERE work_item_id=? ORDER BY created_at", (work_item_id,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT 100").fetchall()
    return [Artifact(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Events + Deliveries
# ════════════════════════════════════════════════════════════════════

def append_event(conn: sqlite3.Connection, task_id: str, event_type: str,
                 actor_agent_id: Optional[str] = None, work_item_id: Optional[str] = None,
                 run_id: Optional[str] = None, payload_json: str = "{}",
                 idempotency_key: Optional[str] = None) -> Optional[Event]:
    """Append an immutable event. Returns None if duplicate (idempotency)."""
    if idempotency_key and actor_agent_id:
        existing = conn.execute(
            "SELECT * FROM events WHERE actor_agent_id=? AND idempotency_key=?",
            (actor_agent_id, idempotency_key),
        ).fetchone()
        if existing:
            return None
    cur = conn.execute(
        """INSERT INTO events (task_id, work_item_id, run_id, event_type,
           actor_agent_id, payload_json, idempotency_key)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (task_id, work_item_id, run_id, event_type, actor_agent_id, payload_json, idempotency_key),
    )
    event_id = cur.lastrowid
    row = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
    return Event(**_row_dict(row))


def create_delivery(conn: sqlite3.Connection, event_id: int,
                    recipient_kind: str, recipient_id: str,
                    available_at: Optional[str] = None) -> Optional[Delivery]:
    deliv_id = new_id()
    try:
        conn.execute(
            """INSERT INTO deliveries (id, event_id, recipient_kind, recipient_id, status, available_at)
               VALUES (?, ?, ?, ?, 'pending', ?)""",
            (deliv_id, event_id, recipient_kind, recipient_id, available_at or now_iso()),
        )
    except sqlite3.IntegrityError:
        return None
    row = conn.execute("SELECT * FROM deliveries WHERE id=?", (deliv_id,)).fetchone()
    return Delivery(**_row_dict(row))


def list_pending_deliveries(conn: sqlite3.Connection, recipient_kind: str,
                            recipient_id: str, limit: int = 50) -> list[dict]:
    """Returns joined event+delivery rows for pending deliveries."""
    rows = conn.execute(
        """SELECT d.id AS delivery_id, d.event_id, d.status, d.available_at,
                  e.event_type, e.payload_json, e.task_id, e.work_item_id, e.run_id,
                  e.actor_agent_id, e.created_at AS event_created_at
           FROM deliveries d
           JOIN events e ON d.event_id = e.event_id
           WHERE d.recipient_kind=? AND d.recipient_id=? AND d.status='pending'
           AND d.available_at <= ?
           ORDER BY e.event_id ASC
           LIMIT ?""",
        (recipient_kind, recipient_id, now_iso(), limit),
    ).fetchall()
    return [_row_dict(r) for r in rows]


def ack_delivery(conn: sqlite3.Connection, delivery_id: str,
                 recipient_id: str) -> bool:
    """Ack a delivery for a specific recipient. Non-destructive."""
    cur = conn.execute(
        """UPDATE deliveries SET status='acked', acked_at=?
           WHERE id=? AND recipient_id=? AND status='pending'""",
        (now_iso(), delivery_id, recipient_id),
    )
    return cur.rowcount > 0


def expire_stale_deliveries(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "UPDATE deliveries SET status='expired' WHERE status='pending' AND lease_expires_at < ?",
        (now_iso(),),
    )
    return cur.rowcount


# ════════════════════════════════════════════════════════════════════
#  Outbox
# ════════════════════════════════════════════════════════════════════

def enqueue_outbox(conn: sqlite3.Connection, event_type: str,
                   payload_json: str, max_attempts: int = 5) -> str:
    ob_id = new_id()
    conn.execute(
        """INSERT INTO outbox (id, event_type, payload_json, status, max_attempts)
           VALUES (?, ?, ?, 'pending', ?)""",
        (ob_id, event_type, payload_json, max_attempts),
    )
    return ob_id


def list_pending_outbox(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        """SELECT * FROM outbox WHERE status='pending'
           AND (retry_at IS NULL OR retry_at <= ?)
           ORDER BY created_at LIMIT ?""",
        (now_iso(), limit),
    ).fetchall()
    return [_row_dict(r) for r in rows]


def mark_outbox_delivered(conn: sqlite3.Connection, outbox_id: str):
    conn.execute(
        "UPDATE outbox SET status='delivered', delivered_at=? WHERE id=?",
        (now_iso(), outbox_id),
    )


def retry_outbox(conn: sqlite3.Connection, outbox_id: str):
    conn.execute(
        "UPDATE outbox SET attempts=attempts+1, retry_at=? WHERE id=?",
        (now_iso(), outbox_id),
    )


def dead_letter_outbox(conn: sqlite3.Connection, outbox_id: str):
    conn.execute("UPDATE outbox SET status='dead_letter' WHERE id=?", (outbox_id,))


# ════════════════════════════════════════════════════════════════════
#  Approvals
# ════════════════════════════════════════════════════════════════════

def create_approval(conn: sqlite3.Connection, task_id: str, action: str,
                    reason: str = "", work_item_id: Optional[str] = None,
                    run_id: Optional[str] = None) -> Approval:
    ap_id = new_id()
    conn.execute(
        """INSERT INTO approvals (id, task_id, work_item_id, run_id, action, reason)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (ap_id, task_id, work_item_id, run_id, action, reason),
    )
    row = conn.execute("SELECT * FROM approvals WHERE id=?", (ap_id,)).fetchone()
    return Approval(**_row_dict(row))


def decide_approval(conn: sqlite3.Connection, approval_id: str,
                    decision: str, decided_by: str) -> bool:
    cur = conn.execute(
        """UPDATE approvals SET decision=?, decided_by=?, decided_at=?
           WHERE id=? AND decision IS NULL""",
        (decision, decided_by, now_iso(), approval_id),
    )
    return cur.rowcount > 0


def list_pending_approvals(conn: sqlite3.Connection, task_id: Optional[str] = None) -> list[Approval]:
    if task_id:
        rows = conn.execute(
            "SELECT * FROM approvals WHERE task_id=? AND decision IS NULL ORDER BY created_at",
            (task_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM approvals WHERE decision IS NULL ORDER BY created_at",
        ).fetchall()
    return [Approval(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Resource Locks
# ════════════════════════════════════════════════════════════════════

def acquire_lock(conn: sqlite3.Connection, lock_key: str, holder_run_id: str,
                 resource_type: str = "general", resource_id: str = "",
                 ttl_seconds: int = 600) -> tuple[bool, int]:
    """Try to acquire a resource lock. Returns (success, fencing_token).

    If an expired lock exists, it's stolen. If an active lock exists
    held by a different run, acquisition fails.
    """
    existing = conn.execute(
        "SELECT * FROM resource_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()

    token = next_fencing_token(conn)
    now = now_iso()

    if existing is None:
        conn.execute(
            """INSERT INTO resource_locks (id, lock_key, holder_run_id, resource_type,
               resource_id, fencing_token, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (new_id(), lock_key, holder_run_id, resource_type, resource_id,
             token, _iso_plus_seconds(ttl_seconds)),
        )
        return True, token

    existing = _row_dict(existing)
    if existing["holder_run_id"] == holder_run_id:
        conn.execute(
            "UPDATE resource_locks SET fencing_token=?, expires_at=? WHERE lock_key=?",
            (token, _iso_plus_seconds(ttl_seconds), lock_key),
        )
        return True, token

    conn.execute(
        "UPDATE resource_locks SET expires_at=? WHERE lock_key=? AND expires_at < ?",
        ("1970-01-01T00:00:00+00:00", lock_key, now),
    )
    deleted = conn.execute(
        "DELETE FROM resource_locks WHERE lock_key=? AND expires_at < ?",
        (lock_key, now),
    ).rowcount

    if deleted > 0:
        conn.execute(
            """INSERT INTO resource_locks (id, lock_key, holder_run_id, resource_type,
               resource_id, fencing_token, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (new_id(), lock_key, holder_run_id, resource_type, resource_id,
             token, _iso_plus_seconds(ttl_seconds)),
        )
        return True, token

    return False, existing["fencing_token"]


def release_lock(conn: sqlite3.Connection, lock_key: str,
                 holder_run_id: str, fencing_token: int) -> bool:
    cur = conn.execute(
        """DELETE FROM resource_locks
           WHERE lock_key=? AND holder_run_id=? AND fencing_token=?""",
        (lock_key, holder_run_id, fencing_token),
    )
    return cur.rowcount > 0


def get_lock(conn: sqlite3.Connection, lock_key: str) -> Optional[ResourceLock]:
    row = conn.execute("SELECT * FROM resource_locks WHERE lock_key=?", (lock_key,)).fetchone()
    if not row:
        return None
    return ResourceLock(**_row_dict(row))


def expire_stale_locks(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "DELETE FROM resource_locks WHERE expires_at < ?",
        (now_iso(),),
    )
    return cur.rowcount


# ── Helpers ────────────────────────────────────────────────────────

def _iso_plus_seconds(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
