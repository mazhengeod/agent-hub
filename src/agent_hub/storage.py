"""Repository layer: pure data access. NEVER commits."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

from .db import now_iso, new_id, next_fencing_token, iso_plus_seconds
from .models import (
    Agent, Adapter, Session, Task, WorkItem, WorkDependency, Run,
    Checkpoint, Artifact, Event, Delivery, Approval, ResourceLock,
)


def _row_dict(row: sqlite3.Row) -> dict:
    return dict(row) if row else {}


# ════════════════════════════════════════════════════════════════════
#  Agents
# ════════════════════════════════════════════════════════════════════

def upsert_agent(conn, agent_id, name, capabilities="[]", token_hash=""):
    conn.execute(
        """INSERT INTO agents (id, name, capabilities, token_hash, is_active, updated_at)
           VALUES (?, ?, ?, ?, 1, ?)
           ON CONFLICT(id) DO UPDATE SET
             name=excluded.name, capabilities=excluded.capabilities,
             token_hash=excluded.token_hash, updated_at=excluded.updated_at""",
        (agent_id, name, capabilities, token_hash, now_iso()),
    )
    return get_agent(conn, agent_id)


def get_agent(conn, agent_id):
    row = conn.execute("SELECT * FROM agents WHERE id=?", (agent_id,)).fetchone()
    if not row:
        return None
    d = _row_dict(row)
    d["is_active"] = bool(d["is_active"])
    return Agent(**d)


def update_heartbeat(conn, agent_id):
    conn.execute(
        "UPDATE agents SET last_heartbeat=?, updated_at=? WHERE id=?",
        (now_iso(), now_iso(), agent_id),
    )


def list_agents(conn):
    rows = conn.execute("SELECT * FROM agents ORDER BY created_at").fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["is_active"] = bool(d["is_active"])
        result.append(Agent(**d))
    return result


# ════════════════════════════════════════════════════════════════════
#  Sessions
# ════════════════════════════════════════════════════════════════════

def create_session(conn, session_id, agent_id, native_session_ref,
                   adapter_id, capabilities_json, lease_expires_at):
    conn.execute(
        """INSERT INTO sessions (id, agent_id, native_session_ref, adapter_id,
           capabilities_json, status, last_seen_at, lease_expires_at)
           VALUES (?, ?, ?, ?, ?, 'active', ?, ?)""",
        (session_id, agent_id, native_session_ref, adapter_id,
         capabilities_json, now_iso(), lease_expires_at),
    )
    return get_session(conn, session_id)


def get_session(conn, session_id):
    row = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if not row:
        return None
    return Session(**_row_dict(row))


def list_active_sessions_for_agent(conn, agent_id):
    rows = conn.execute(
        "SELECT * FROM sessions WHERE agent_id=? AND status='active' ORDER BY created_at DESC",
        (agent_id,),
    ).fetchall()
    return [Session(**_row_dict(r)) for r in rows]


def touch_session(conn, session_id, lease_expires_at):
    conn.execute(
        "UPDATE sessions SET last_seen_at=?, lease_expires_at=? WHERE id=? AND status='active'",
        (now_iso(), lease_expires_at, session_id),
    )


def end_session(conn, session_id):
    conn.execute(
        "UPDATE sessions SET status='ended', ended_at=? WHERE id=?",
        (now_iso(), session_id),
    )


def expire_stale_sessions(conn):
    cur = conn.execute(
        "UPDATE sessions SET status='lost' WHERE status='active' AND lease_expires_at < ?",
        (now_iso(),),
    )
    return cur.rowcount


# ════════════════════════════════════════════════════════════════════
#  Tasks
# ════════════════════════════════════════════════════════════════════

def create_task(conn, task_id, objective, created_by_agent_id, **fields):
    conn.execute(
        """INSERT INTO tasks (id, objective, success_criteria_json, constraints_json,
           authorization_policy_json, context_refs_json, status, priority,
           deadline_at, budget_json, created_by_agent_id)
           VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?)""",
        (task_id, objective,
         fields.get("success_criteria_json", "[]"),
         fields.get("constraints_json", "{}"),
         fields.get("authorization_policy_json", "{}"),
         fields.get("context_refs_json", "[]"),
         fields.get("priority", 0),
         fields.get("deadline_at"),
         fields.get("budget_json", "{}"),
         created_by_agent_id),
    )
    return get_task(conn, task_id)


def get_task(conn, task_id):
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        return None
    return Task(**_row_dict(row))


def update_task_status(conn, task_id, status, **extra):
    sets = ["status=?", "updated_at=?"]
    vals = [status, now_iso()]
    for k in ("completed_at", "coordinator_run_id", "plan_version"):
        if k in extra:
            sets.append(f"{k}=?")
            vals.append(extra[k])
    vals.append(task_id)
    cur = conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", vals)
    return cur.rowcount > 0


def list_tasks(conn, status=None, limit=50):
    if status:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status=? ORDER BY priority DESC, created_at DESC LIMIT ?",
            (status, limit)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY priority DESC, created_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [Task(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Work Items + Dependencies
# ════════════════════════════════════════════════════════════════════

def create_work_item(conn, task_id, kind, objective, **fields):
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
    return get_work_item(conn, wi_id)


def get_work_item(conn, wi_id):
    row = conn.execute("SELECT * FROM work_items WHERE id=?", (wi_id,)).fetchone()
    if not row:
        return None
    d = _row_dict(row)
    d["needs_review"] = bool(d["needs_review"])
    return WorkItem(**d)


def add_dependency(conn, work_item_id, depends_on_id, condition="succeeded"):
    conn.execute(
        """INSERT OR IGNORE INTO work_dependencies (work_item_id, depends_on_id, condition)
           VALUES (?, ?, ?)""",
        (work_item_id, depends_on_id, condition),
    )


def get_dependencies(conn, work_item_id):
    rows = conn.execute(
        "SELECT * FROM work_dependencies WHERE work_item_id=?", (work_item_id,)).fetchall()
    return [WorkDependency(**_row_dict(r)) for r in rows]


def get_all_dependencies_for_task(conn, task_id):
    rows = conn.execute(
        """SELECT wd.* FROM work_dependencies wd
           JOIN work_items wi ON wd.work_item_id = wi.id
           WHERE wi.task_id=?""",
        (task_id,)).fetchall()
    return [WorkDependency(**_row_dict(r)) for r in rows]


def get_dependents(conn, work_item_id):
    rows = conn.execute(
        "SELECT * FROM work_dependencies WHERE depends_on_id=?", (work_item_id,)).fetchall()
    return [WorkDependency(**_row_dict(r)) for r in rows]


def update_work_item_status(conn, wi_id, status, version=None):
    if version is not None:
        cur = conn.execute(
            """UPDATE work_items SET status=?, version=version+1, updated_at=?
               WHERE id=? AND version=?""",
            (status, now_iso(), wi_id, version))
    else:
        cur = conn.execute(
            "UPDATE work_items SET status=?, version=version+1, updated_at=? WHERE id=?",
            (status, now_iso(), wi_id))
    return cur.rowcount > 0


def list_work_items(conn, task_id, status=None):
    if status:
        rows = conn.execute(
            "SELECT * FROM work_items WHERE task_id=? AND status=? ORDER BY priority DESC, created_at",
            (task_id, status)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM work_items WHERE task_id=? ORDER BY priority DESC, created_at",
            (task_id,)).fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["needs_review"] = bool(d["needs_review"])
        result.append(WorkItem(**d))
    return result


def list_ready_work_items(conn, agent_id=None, limit=20):
    rows = conn.execute(
        """SELECT wi.* FROM work_items wi
           JOIN tasks t ON wi.task_id = t.id
           WHERE wi.status = 'ready' AND t.status = 'running'
           AND (wi.preferred_agent_id IS NULL OR wi.preferred_agent_id = ?)
           ORDER BY wi.priority DESC, wi.created_at
           LIMIT ?""",
        (agent_id, limit)).fetchall()
    result = []
    for r in rows:
        d = _row_dict(r)
        d["needs_review"] = bool(d["needs_review"])
        result.append(WorkItem(**d))
    return result


# ════════════════════════════════════════════════════════════════════
#  Runs
# ════════════════════════════════════════════════════════════════════

def create_run(conn, work_item_id, agent_id, session_id, lease_expires_at):
    run_id = new_id()
    token = next_fencing_token(conn)
    attempt_row = conn.execute(
        "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS next FROM runs WHERE work_item_id=?",
        (work_item_id,)).fetchone()
    attempt_no = attempt_row["next"] if attempt_row else 1
    conn.execute(
        """INSERT INTO runs (id, work_item_id, attempt_no, agent_id, session_id,
           status, fencing_token, lease_expires_at)
           VALUES (?, ?, ?, ?, ?, 'offered', ?, ?)""",
        (run_id, work_item_id, attempt_no, agent_id, session_id, token, lease_expires_at))
    return get_run(conn, run_id)


def get_run(conn, run_id):
    row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        return None
    return Run(**_row_dict(row))


def get_active_run_for_work_item(conn, work_item_id):
    row = conn.execute(
        "SELECT * FROM runs WHERE work_item_id=? AND status IN ('offered','claimed','running') ORDER BY attempt_no DESC LIMIT 1",
        (work_item_id,)).fetchone()
    if not row:
        return None
    return Run(**_row_dict(row))


def claim_run(conn, run_id, fencing_token, session_id, lease_expires_at):
    cur = conn.execute(
        """UPDATE runs SET status='claimed', session_id=?, lease_expires_at=?,
           started_at=?, heartbeat_at=?
           WHERE id=? AND fencing_token=? AND status='offered'""",
        (session_id, lease_expires_at, now_iso(), now_iso(), run_id, fencing_token))
    return cur.rowcount > 0


def start_run(conn, run_id, fencing_token, session_id, lease_expires_at):
    cur = conn.execute(
        """UPDATE runs SET status='running', session_id=?, lease_expires_at=?, heartbeat_at=?
           WHERE id=? AND fencing_token=? AND status IN ('offered','claimed','running')""",
        (session_id, lease_expires_at, now_iso(), run_id, fencing_token))
    return cur.rowcount > 0


def heartbeat_run(conn, run_id, fencing_token, lease_expires_at):
    cur = conn.execute(
        """UPDATE runs SET heartbeat_at=?, lease_expires_at=?
           WHERE id=? AND fencing_token=? AND status='running'""",
        (now_iso(), lease_expires_at, run_id, fencing_token))
    return cur.rowcount > 0


def complete_run(conn, run_id, fencing_token, status, failure_code=None, failure_json="{}"):
    cur = conn.execute(
        """UPDATE runs SET status=?, failure_code=?, failure_json=?, ended_at=?
           WHERE id=? AND fencing_token=? AND status='running'""",
        (status, failure_code, failure_json, now_iso(), run_id, fencing_token))
    return cur.rowcount > 0


def cancel_run(conn, run_id, fencing_token):
    cur = conn.execute(
        "UPDATE runs SET status='cancelled', ended_at=? WHERE id=? AND fencing_token=? AND status IN ('offered','claimed','running')",
        (now_iso(), run_id, fencing_token))
    return cur.rowcount > 0


def expire_stale_runs(conn):
    rows = conn.execute(
        "SELECT id FROM runs WHERE status IN ('offered','claimed','running') AND lease_expires_at < ?",
        (now_iso(),)).fetchall()
    lost_ids = [r["id"] for r in rows]
    if lost_ids:
        placeholders = ",".join("?" * len(lost_ids))
        conn.execute(
            f"UPDATE runs SET status='lost', ended_at=? WHERE id IN ({placeholders})",
            [now_iso()] + lost_ids)
    return lost_ids


def list_runs_for_agent(conn, agent_id, status=None):
    if status:
        rows = conn.execute(
            "SELECT * FROM runs WHERE agent_id=? AND status=? ORDER BY created_at DESC",
            (agent_id, status)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM runs WHERE agent_id=? ORDER BY created_at DESC",
            (agent_id,)).fetchall()
    return [Run(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Checkpoints
# ════════════════════════════════════════════════════════════════════

def save_checkpoint(conn, run_id, snapshot_json):
    cp_id = new_id()
    version_row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS next FROM checkpoints WHERE run_id=?",
        (run_id,)).fetchone()
    version = version_row["next"] if version_row else 1
    conn.execute(
        "INSERT INTO checkpoints (id, run_id, version, snapshot_json) VALUES (?, ?, ?, ?)",
        (cp_id, run_id, version, snapshot_json))
    conn.execute("UPDATE runs SET checkpoint_id=? WHERE id=?", (cp_id, run_id))
    row = conn.execute("SELECT * FROM checkpoints WHERE id=?", (cp_id,)).fetchone()
    return Checkpoint(**_row_dict(row))


def get_latest_checkpoint(conn, run_id):
    row = conn.execute(
        "SELECT * FROM checkpoints WHERE run_id=? ORDER BY version DESC LIMIT 1",
        (run_id,)).fetchone()
    if not row:
        return None
    return Checkpoint(**_row_dict(row))


def get_latest_checkpoint_for_work_item(conn, work_item_id):
    row = conn.execute(
        """SELECT c.* FROM checkpoints c
           JOIN runs r ON c.run_id = r.id
           WHERE r.work_item_id=?
           ORDER BY c.version DESC LIMIT 1""",
        (work_item_id,)).fetchone()
    if not row:
        return None
    return Checkpoint(**_row_dict(row))


# ════════════════════════════════════════════════════════════════════
#  Artifacts
# ════════════════════════════════════════════════════════════════════

def create_artifact(conn, work_item_id, task_id, kind, ref,
                    run_id=None, hash_val=None, metadata_json="{}"):
    art_id = new_id()
    conn.execute(
        """INSERT INTO artifacts (id, run_id, work_item_id, task_id, kind, ref, hash, metadata_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (art_id, run_id, work_item_id, task_id, kind, ref, hash_val, metadata_json))
    row = conn.execute("SELECT * FROM artifacts WHERE id=?", (art_id,)).fetchone()
    return Artifact(**_row_dict(row))


def list_artifacts(conn, task_id=None, work_item_id=None):
    if task_id:
        rows = conn.execute("SELECT * FROM artifacts WHERE task_id=? ORDER BY created_at", (task_id,)).fetchall()
    elif work_item_id:
        rows = conn.execute("SELECT * FROM artifacts WHERE work_item_id=? ORDER BY created_at", (work_item_id,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT 100").fetchall()
    return [Artifact(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Events + Deliveries + Outbox  (the reliability pipeline)
# ════════════════════════════════════════════════════════════════════

def append_event(conn, task_id, event_type, actor_agent_id=None,
                 work_item_id=None, run_id=None, payload_json="{}",
                 idempotency_key=None):
    if idempotency_key and actor_agent_id:
        existing = conn.execute(
            "SELECT * FROM events WHERE actor_agent_id=? AND idempotency_key=?",
            (actor_agent_id, idempotency_key)).fetchone()
        if existing:
            return None
    cur = conn.execute(
        """INSERT INTO events (task_id, work_item_id, run_id, event_type,
           actor_agent_id, payload_json, idempotency_key)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (task_id, work_item_id, run_id, event_type, actor_agent_id, payload_json, idempotency_key))
    event_id = cur.lastrowid
    row = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
    return Event(**_row_dict(row))


def create_delivery(conn, event_id, recipient_kind, recipient_id, available_at=None):
    deliv_id = new_id()
    try:
        conn.execute(
            """INSERT INTO deliveries (id, event_id, recipient_kind, recipient_id, status, available_at)
               VALUES (?, ?, ?, ?, 'pending', ?)""",
            (deliv_id, event_id, recipient_kind, recipient_id, available_at or now_iso()))
    except sqlite3.IntegrityError:
        return None
    row = conn.execute("SELECT * FROM deliveries WHERE id=?", (deliv_id,)).fetchone()
    return Delivery(**_row_dict(row))


def list_deliveries_since(conn, recipient_kind, recipient_id,
                          since_event_id=0, limit=50):
    """Cursor-based delivery pull: unacked deliveries for events > since_event_id."""
    rows = conn.execute(
        """SELECT d.id AS delivery_id, d.event_id, d.status, d.available_at,
                  e.event_type, e.payload_json, e.task_id, e.work_item_id, e.run_id,
                  e.actor_agent_id, e.created_at AS event_created_at
           FROM deliveries d
           JOIN events e ON d.event_id = e.event_id
           WHERE d.recipient_kind=? AND d.recipient_id=?
           AND d.status='pending'
           AND e.event_id > ?
           AND d.available_at <= ?
           ORDER BY e.event_id ASC
           LIMIT ?""",
        (recipient_kind, recipient_id, since_event_id, now_iso(), limit)).fetchall()
    return [_row_dict(r) for r in rows]


def ack_delivery(conn, delivery_id, recipient_id):
    cur = conn.execute(
        """UPDATE deliveries SET status='acked', acked_at=?
           WHERE id=? AND recipient_id=? AND status='pending'""",
        (now_iso(), delivery_id, recipient_id))
    return cur.rowcount > 0


def expire_stale_deliveries(conn):
    cur = conn.execute(
        "UPDATE deliveries SET status='expired' WHERE status='pending' AND lease_expires_at < ?",
        (now_iso(),))
    return cur.rowcount


def enqueue_outbox(conn, event_type, payload_json, max_attempts=5):
    ob_id = new_id()
    conn.execute(
        """INSERT INTO outbox (id, event_type, payload_json, status, max_attempts)
           VALUES (?, ?, ?, 'pending', ?)""",
        (ob_id, event_type, payload_json, max_attempts))
    return ob_id


def list_pending_outbox(conn, limit=20):
    rows = conn.execute(
        """SELECT * FROM outbox WHERE status='pending'
           AND (retry_at IS NULL OR retry_at <= ?)
           ORDER BY created_at LIMIT ?""",
        (now_iso(), limit)).fetchall()
    return [_row_dict(r) for r in rows]


def mark_outbox_delivered(conn, outbox_id):
    conn.execute(
        "UPDATE outbox SET status='delivered', delivered_at=? WHERE id=?",
        (now_iso(), outbox_id))


def retry_outbox(conn, outbox_id):
    conn.execute(
        "UPDATE outbox SET attempts=attempts+1, retry_at=? WHERE id=?",
        (now_iso(), outbox_id))


def dead_letter_outbox(conn, outbox_id):
    conn.execute("UPDATE outbox SET status='dead_letter' WHERE id=?", (outbox_id,))


# ════════════════════════════════════════════════════════════════════
#  Approvals
# ════════════════════════════════════════════════════════════════════

def create_approval(conn, task_id, action, reason="", work_item_id=None, run_id=None):
    ap_id = new_id()
    conn.execute(
        "INSERT INTO approvals (id, task_id, work_item_id, run_id, action, reason) VALUES (?, ?, ?, ?, ?, ?)",
        (ap_id, task_id, work_item_id, run_id, action, reason))
    row = conn.execute("SELECT * FROM approvals WHERE id=?", (ap_id,)).fetchone()
    return Approval(**_row_dict(row))


def decide_approval(conn, approval_id, decision, decided_by):
    cur = conn.execute(
        "UPDATE approvals SET decision=?, decided_by=?, decided_at=? WHERE id=? AND decision IS NULL",
        (decision, decided_by, now_iso(), approval_id))
    return cur.rowcount > 0


def list_pending_approvals(conn, task_id=None):
    if task_id:
        rows = conn.execute(
            "SELECT * FROM approvals WHERE task_id=? AND decision IS NULL ORDER BY created_at",
            (task_id,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM approvals WHERE decision IS NULL ORDER BY created_at").fetchall()
    return [Approval(**_row_dict(r)) for r in rows]


# ════════════════════════════════════════════════════════════════════
#  Resource Locks
# ════════════════════════════════════════════════════════════════════

def acquire_lock(conn, lock_key, holder_run_id, resource_type="general",
                 resource_id="", ttl_seconds=600):
    existing = conn.execute(
        "SELECT * FROM resource_locks WHERE lock_key=?", (lock_key,)).fetchone()
    token = next_fencing_token(conn)
    now = now_iso()

    if existing is None:
        conn.execute(
            """INSERT INTO resource_locks (id, lock_key, holder_run_id, resource_type,
               resource_id, fencing_token, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (new_id(), lock_key, holder_run_id, resource_type, resource_id,
             token, iso_plus_seconds(ttl_seconds)))
        return True, token

    existing = _row_dict(existing)
    if existing["holder_run_id"] == holder_run_id:
        conn.execute(
            "UPDATE resource_locks SET fencing_token=?, expires_at=? WHERE lock_key=?",
            (token, iso_plus_seconds(ttl_seconds), lock_key))
        return True, token

    conn.execute(
        "DELETE FROM resource_locks WHERE lock_key=? AND expires_at < ?",
        (lock_key, now))
    deleted = conn.execute(
        "SELECT changes() AS n").fetchone()["n"]

    if deleted > 0:
        conn.execute(
            """INSERT INTO resource_locks (id, lock_key, holder_run_id, resource_type,
               resource_id, fencing_token, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (new_id(), lock_key, holder_run_id, resource_type, resource_id,
             token, iso_plus_seconds(ttl_seconds)))
        return True, token

    return False, existing["fencing_token"]


def release_lock(conn, lock_key, holder_run_id, fencing_token):
    cur = conn.execute(
        "DELETE FROM resource_locks WHERE lock_key=? AND holder_run_id=? AND fencing_token=?",
        (lock_key, holder_run_id, fencing_token))
    return cur.rowcount > 0


def get_lock(conn, lock_key):
    row = conn.execute("SELECT * FROM resource_locks WHERE lock_key=?", (lock_key,)).fetchone()
    if not row:
        return None
    return ResourceLock(**_row_dict(row))


def expire_stale_locks(conn):
    cur = conn.execute("DELETE FROM resource_locks WHERE expires_at < ?", (now_iso(),))
    return cur.rowcount
