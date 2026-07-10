"""Domain service layer: state machine, Run lease, checkpoint recovery,
dependency advancement, and the single-writer write entry point.

Per architecture audit:
- All writes go through WriteExecutor (single-writer gate).
- State transitions are conditional UPDATEs (compare-and-swap).
- Fencing tokens prevent stale writes from old sessions.
- Checkpoints enable cross-session recovery.
- Dependencies are auto-advanced by the scheduler.
"""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Optional

from . import storage
from .db import now_iso, new_id, write_executor, init_db, get_db
from .models import (
    Task, WorkItem, Run, Session, Event, HubError,
)
from .config import get_config

# ── Lease durations (configurable) ────────────────────────────────

DEFAULT_SESSION_LEASE = int(get_config("session_lease_seconds", 300))       # 5 min
DEFAULT_RUN_LEASE = int(get_config("run_lease_seconds", 600))               # 10 min
MAX_RUN_ATTEMPTS = int(get_config("max_run_attempts", 3))
CHECKPOINT_INTERVAL = int(get_config("checkpoint_interval_seconds", 60))


def _iso_plus(seconds: int) -> str:
    from datetime import datetime, timezone
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


# ════════════════════════════════════════════════════════════════════
#  Session lifecycle
# ════════════════════════════════════════════════════════════════════

def session_start(agent_id: str, native_session_ref: Optional[str] = None,
                  capabilities: Optional[list] = None,
                  adapter_id: Optional[str] = None,
                  lease_seconds: Optional[int] = None) -> dict:
    """Start a new session for an agent. Ends any previous active session."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_SESSION_LEASE
    session_id = new_id()
    caps_json = json.dumps(capabilities or [])

    def _write(c):
        prev = storage.get_active_session_for_agent(c, agent_id)
        if prev:
            storage.end_session(c, prev.id)
        return storage.create_session(
            c, session_id, agent_id, native_session_ref, adapter_id,
            caps_json, _iso_plus(lease),
        )

    sess = write_executor.execute_write(conn, _write)
    return {"session_id": sess.id, "agent_id": agent_id,
            "lease_expires_at": sess.lease_expires_at}


def session_heartbeat(session_id: str, lease_seconds: Optional[int] = None) -> dict:
    """Renew session lease."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_SESSION_LEASE

    def _write(c):
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_found", f"Session {session_id} not active")
        storage.touch_session(c, session_id, _iso_plus(lease))
        return sess

    sess = write_executor.execute_write(conn, _write)
    return {"session_id": session_id, "lease_expires_at": _iso_plus(lease)}


def session_end(session_id: str) -> dict:
    conn = get_db()

    def _write(c):
        storage.end_session(c, session_id)
        return None

    write_executor.execute_write(conn, _write)
    return {"session_id": session_id, "status": "ended"}


# ════════════════════════════════════════════════════════════════════
#  Task lifecycle
# ════════════════════════════════════════════════════════════════════

def create_task(objective: str, created_by_agent_id: str,
                success_criteria: Optional[list] = None,
                constraints: Optional[dict] = None,
                authorization_policy: Optional[dict] = None,
                context_refs: Optional[list] = None,
                priority: int = 0, deadline_at: Optional[str] = None,
                budget: Optional[dict] = None) -> dict:
    conn = get_db()
    task_id = new_id()

    def _write(c):
        task = storage.create_task(c, task_id, objective, created_by_agent_id,
            success_criteria_json=json.dumps(success_criteria or []),
            constraints_json=json.dumps(constraints or {}),
            authorization_policy_json=json.dumps(authorization_policy or {}),
            context_refs_json=json.dumps(context_refs or []),
            priority=priority, deadline_at=deadline_at,
            budget_json=json.dumps(budget or {}),
        )
        storage.append_event(c, task_id, "task.created",
            actor_agent_id=created_by_agent_id,
            payload_json=json.dumps({"objective": objective}))
        return task

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


def get_task(task_id: str) -> Optional[dict]:
    conn = get_db()
    task = storage.get_task(conn, task_id)
    return _task_dict(task) if task else None


def list_tasks(status: Optional[str] = None, limit: int = 50) -> list[dict]:
    conn = get_db()
    tasks = storage.list_tasks(conn, status=status, limit=limit)
    return [_task_dict(t) for t in tasks]


def plan_task(task_id: str, work_items: list[dict],
              dependencies: Optional[list[dict]] = None,
              actor_agent_id: Optional[str] = None) -> dict:
    """Create work items and dependencies for a task, transitioning it to 'planned'."""
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        if task.status not in ("draft", "planned"):
            raise HubError("invalid_state",
                f"Cannot plan task in status '{task.status}'")

        created_ids = {}
        for wi_spec in work_items:
            wi = storage.create_work_item(c, task_id, wi_spec["kind"],
                wi_spec["objective"],
                parent_id=wi_spec.get("parent_id"),
                acceptance_json=json.dumps(wi_spec.get("acceptance", [])),
                required_capabilities_json=json.dumps(wi_spec.get("required_capabilities", [])),
                preferred_agent_id=wi_spec.get("preferred_agent_id"),
                priority=wi_spec.get("priority", 0),
                retry_policy_json=wi_spec.get("retry_policy_json", '{"max_attempts":3}'),
                needs_review=wi_spec.get("needs_review", False),
            )
            created_ids[wi_spec.get("ref", wi.id)] = wi.id

        if dependencies:
            for dep in dependencies:
                wi_id = created_ids.get(dep["work_item"], dep["work_item"])
                dep_id = created_ids.get(dep["depends_on"], dep["depends_on"])
                storage.add_dependency(c, wi_id, dep_id,
                    dep.get("condition", "succeeded"))

        _advance_ready_work_items(c, task_id)
        storage.update_task_status(c, task_id, "planned",
            plan_version=task.plan_version + 1)
        storage.append_event(c, task_id, "task.planned",
            actor_agent_id=actor_agent_id,
            payload_json=json.dumps({"work_item_count": len(work_items)}))
        return storage.get_task(c, task_id)

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


def start_task(task_id: str, actor_agent_id: str) -> dict:
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        if task.status not in ("planned", "ready"):
            raise HubError("invalid_state",
                f"Cannot start task in status '{task.status}'")
        storage.update_task_status(c, task_id, "running")
        storage.append_event(c, task_id, "task.started",
            actor_agent_id=actor_agent_id)
        return storage.get_task(c, task_id)

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


# ════════════════════════════════════════════════════════════════════
#  Work Item + Run lifecycle
# ════════════════════════════════════════════════════════════════════

def claim_work(agent_id: str, session_id: str,
               work_item_id: Optional[str] = None,
               lease_seconds: Optional[int] = None) -> Optional[dict]:
    """Claim a work item for execution. Creates a new Run.

    If work_item_id is None, picks the highest-priority ready work item
    that the agent can execute.
    """
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        if sess.agent_id != agent_id:
            raise HubError("session_agent_mismatch",
                f"Session belongs to {sess.agent_id}, not {agent_id}")

        if work_item_id:
            wi = storage.get_work_item(c, work_item_id)
            if not wi:
                raise HubError("work_not_found", f"Work item {work_item_id} not found")
            if wi.status not in ("ready", "offered"):
                raise HubError("work_not_ready",
                    f"Work item {work_item_id} in status '{wi.status}'")
        else:
            candidates = storage.list_ready_work_items(c, agent_id=agent_id, limit=1)
            if not candidates:
                return None
            wi = candidates[0]

        existing_run = storage.get_active_run_for_work_item(c, wi.id)
        if existing_run:
            raise HubError("work_already_running",
                f"Work item {wi.id} already has active run {existing_run.id}")

        run = storage.create_run(c, wi.id, agent_id, session_id, _iso_plus(lease))
        storage.update_work_item_status(c, wi.id, "offered")
        storage.append_event(c, wi.task_id, "work.offered",
            actor_agent_id=agent_id, work_item_id=wi.id, run_id=run.id,
            payload_json=json.dumps({"attempt": run.attempt_no}))
        return {"run": run, "work_item": wi}

    result = write_executor.execute_write(conn, _write)
    if not result:
        return None
    run = result["run"]
    wi = result["work_item"]
    return {
        "run_id": run.id,
        "fencing_token": run.fencing_token,
        "work_item_id": wi.id,
        "task_id": wi.task_id,
        "attempt_no": run.attempt_no,
        "lease_expires_at": run.lease_expires_at,
        "objective": wi.objective,
        "kind": wi.kind,
    }


def start_run(run_id: str, fencing_token: int, session_id: str,
              lease_seconds: Optional[int] = None) -> dict:
    """Transition a run from offered/claimed to running."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token",
                f"Fencing token mismatch: expected {run.fencing_token}, got {fencing_token}")
        ok = storage.start_run(c, run_id, fencing_token, session_id, _iso_plus(lease))
        if not ok:
            raise HubError("invalid_state",
                f"Run {run_id} not in claimable state (status={run.status})")
        wi = storage.get_work_item(c, run.work_item_id)
        if wi:
            storage.update_work_item_status(c, wi.id, "running", version=wi.version)
        storage.append_event(c, wi.task_id if wi else "", "run.started",
            actor_agent_id=run.agent_id, work_item_id=run.work_item_id, run_id=run_id)
        return storage.get_run(c, run_id)

    run = write_executor.execute_write(conn, _write)
    return _run_dict(run)


def heartbeat_run(run_id: str, fencing_token: int,
                  lease_seconds: Optional[int] = None) -> dict:
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        ok = storage.heartbeat_run(c, run_id, fencing_token, _iso_plus(lease))
        if not ok:
            run = storage.get_run(c, run_id)
            raise HubError("stale_token",
                f"Cannot heartbeat run {run_id}: token mismatch or not running"
                + (f" (status={run.status})" if run else ""))
        return None

    write_executor.execute_write(conn, _write)
    return {"run_id": run_id, "lease_expires_at": _iso_plus(lease)}


def save_checkpoint(run_id: str, fencing_token: int, snapshot: dict) -> dict:
    """Save a recovery checkpoint for a run. Validates fencing token."""
    conn = get_db()

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token", "Fencing token mismatch for checkpoint")
        if run.status != "running":
            raise HubError("invalid_state",
                f"Cannot checkpoint run in status '{run.status}'")
        cp = storage.save_checkpoint(c, run_id, json.dumps(snapshot))
        return cp

    cp = write_executor.execute_write(conn, _write)
    return {"checkpoint_id": cp.id, "version": cp.version, "run_id": run_id}


def resume_run(run_id: str, session_id: str, agent_id: str,
               lease_seconds: Optional[int] = None) -> Optional[dict]:
    """Resume a run that was lost/interrupted. Creates a new attempt.

    Returns the checkpoint for recovery, or None if no checkpoint exists.
    """
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        old_run = storage.get_run(c, run_id)
        if not old_run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if old_run.status != "lost":
            raise HubError("invalid_state",
                f"Can only resume lost runs, not '{old_run.status}'")
        if old_run.agent_id != agent_id:
            raise HubError("agent_mismatch",
                f"Run belongs to {old_run.agent_id}, not {agent_id}")

        wi = storage.get_work_item(c, old_run.work_item_id)
        if not wi:
            raise HubError("work_not_found", "Work item not found")

        new_run = storage.create_run(c, wi.id, agent_id, session_id, _iso_plus(lease))
        storage.update_work_item_status(c, wi.id, "running", version=wi.version)
        storage.append_event(c, wi.task_id, "run.resumed",
            actor_agent_id=agent_id, work_item_id=wi.id, run_id=new_run.id,
            payload_json=json.dumps({"previous_run": run_id, "attempt": new_run.attempt_no}))

        checkpoint = storage.get_latest_checkpoint(c, run_id)
        return {"new_run": new_run, "checkpoint": checkpoint, "work_item": wi}

    result = write_executor.execute_write(conn, _write)
    new_run = result["new_run"]
    cp = result["checkpoint"]
    return {
        "run_id": new_run.id,
        "fencing_token": new_run.fencing_token,
        "attempt_no": new_run.attempt_no,
        "checkpoint": json.loads(cp.snapshot_json) if cp else None,
        "checkpoint_version": cp.version if cp else 0,
        "work_item_id": result["work_item"].id,
        "lease_expires_at": new_run.lease_expires_at,
    }


def complete_run(run_id: str, fencing_token: int, status: str,
                 artifacts: Optional[list] = None,
                 failure_code: Optional[str] = None,
                 failure_detail: Optional[dict] = None,
                 actor_agent_id: Optional[str] = None) -> dict:
    """Complete a run. Transitions work item to succeeded/failed/reviewing."""
    conn = get_db()

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token", "Fencing token mismatch on complete")
        if run.status != "running":
            raise HubError("invalid_state",
                f"Cannot complete run in status '{run.status}'")

        ok = storage.complete_run(c, run_id, fencing_token, status,
            failure_code=failure_code,
            failure_json=json.dumps(failure_detail or {}))
        if not ok:
            raise HubError("complete_failed", "Conditional update failed")

        wi = storage.get_work_item(c, run.work_item_id)
        if not wi:
            raise HubError("work_not_found", "Work item not found")

        if artifacts:
            for art in artifacts:
                storage.create_artifact(c, wi.id, wi.task_id,
                    art.get("kind", "file"), art["ref"],
                    run_id=run_id, hash_val=art.get("hash"),
                    metadata_json=json.dumps(art.get("metadata", {})))

        if status == "succeeded":
            new_wi_status = "reviewing" if wi.needs_review else "succeeded"
            storage.update_work_item_status(c, wi.id, new_wi_status, version=wi.version)
            storage.append_event(c, wi.task_id, "run.succeeded",
                actor_agent_id=actor_agent_id or run.agent_id,
                work_item_id=wi.id, run_id=run_id)
        elif status == "failed":
            retry_policy = json.loads(wi.retry_policy_json)
            max_attempts = retry_policy.get("max_attempts", MAX_RUN_ATTEMPTS)
            if run.attempt_no < max_attempts:
                storage.update_work_item_status(c, wi.id, "ready", version=wi.version)
                storage.append_event(c, wi.task_id, "run.failed_retryable",
                    actor_agent_id=actor_agent_id or run.agent_id,
                    work_item_id=wi.id, run_id=run_id,
                    payload_json=json.dumps({"attempt": run.attempt_no,
                                             "failure_code": failure_code}))
            else:
                storage.update_work_item_status(c, wi.id, "failed", version=wi.version)
                storage.append_event(c, wi.task_id, "work.failed",
                    actor_agent_id=actor_agent_id or run.agent_id,
                    work_item_id=wi.id,
                    payload_json=json.dumps({"attempts": run.attempt_no}))
                _check_task_completion(c, wi.task_id)

        _advance_ready_work_items(c, wi.task_id)
        return storage.get_run(c, run_id)

    run = write_executor.execute_write(conn, _write)
    return _run_dict(run)


# ════════════════════════════════════════════════════════════════════
#  Review / Approval
# ════════════════════════════════════════════════════════════════════

def approve_work(work_item_id: str, reviewer_agent_id: str,
                 decision: str, comment: str = "") -> dict:
    """Approve or reject a work item in 'reviewing' state."""
    conn = get_db()
    if decision not in ("approved", "rejected"):
        raise HubError("invalid_decision", "Decision must be 'approved' or 'rejected'")

    def _write(c):
        wi = storage.get_work_item(c, work_item_id)
        if not wi:
            raise HubError("work_not_found", f"Work item {work_item_id} not found")
        if wi.status != "reviewing":
            raise HubError("invalid_state",
                f"Work item not in review (status='{wi.status}')")

        if decision == "approved":
            storage.update_work_item_status(c, work_item_id, "succeeded", version=wi.version)
            storage.append_event(c, wi.task_id, "work.approved",
                actor_agent_id=reviewer_agent_id, work_item_id=work_item_id,
                payload_json=json.dumps({"comment": comment}))
        else:
            storage.update_work_item_status(c, work_item_id, "changes_requested", version=wi.version)
            storage.append_event(c, wi.task_id, "work.rejected",
                actor_agent_id=reviewer_agent_id, work_item_id=work_item_id,
                payload_json=json.dumps({"comment": comment}))

        _advance_ready_work_items(c, wi.task_id)
        _check_task_completion(c, wi.task_id)
        return storage.get_work_item(c, work_item_id)

    wi = write_executor.execute_write(conn, _write)
    return {"work_item_id": work_item_id, "status": wi.status}


def request_approval(task_id: str, action: str, reason: str = "",
                     work_item_id: Optional[str] = None,
                     run_id: Optional[str] = None,
                     requested_by: Optional[str] = None) -> dict:
    conn = get_db()

    def _write(c):
        ap = storage.create_approval(c, task_id, action, reason, work_item_id, run_id)
        storage.append_event(c, task_id, "approval.requested",
            actor_agent_id=requested_by, work_item_id=work_item_id, run_id=run_id,
            payload_json=json.dumps({"action": action, "approval_id": ap.id}))
        return ap

    ap = write_executor.execute_write(conn, _write)
    return {"approval_id": ap.id, "task_id": task_id, "action": action,
            "status": "pending"}


def decide_approval(approval_id: str, decision: str, decided_by: str) -> dict:
    conn = get_db()

    def _write(c):
        ok = storage.decide_approval(c, approval_id, decision, decided_by)
        if not ok:
            raise HubError("approval_not_found",
                f"Approval {approval_id} not found or already decided")
        return None

    write_executor.execute_write(conn, _write)
    return {"approval_id": approval_id, "decision": decision}


# ════════════════════════════════════════════════════════════════════
#  Resource locks
# ════════════════════════════════════════════════════════════════════

def acquire_lock(lock_key: str, holder_run_id: str,
                 resource_type: str = "general", resource_id: str = "",
                 ttl_seconds: int = 600) -> dict:
    conn = get_db()

    def _write(c):
        return storage.acquire_lock(c, lock_key, holder_run_id,
            resource_type, resource_id, ttl_seconds)

    success, token = write_executor.execute_write(conn, _write)
    if not success:
        raise HubError("lock_busy",
            f"Lock '{lock_key}' held by another run (token={token})")
    return {"lock_key": lock_key, "fencing_token": token, "holder_run_id": holder_run_id}


def release_lock(lock_key: str, holder_run_id: str, fencing_token: int) -> dict:
    conn = get_db()

    def _write(c):
        ok = storage.release_lock(c, lock_key, holder_run_id, fencing_token)
        if not ok:
            raise HubError("lock_not_held",
                f"Lock '{lock_key}' not held by run {holder_run_id} with token {fencing_token}")
        return None

    write_executor.execute_write(conn, _write)
    return {"lock_key": lock_key, "status": "released"}


# ════════════════════════════════════════════════════════════════════
#  Agent sync (batch pull)
# ════════════════════════════════════════════════════════════════════

def agent_sync(agent_id: str, session_id: str,
               since_event_id: int = 0) -> dict:
    """Batch pull: pending work, deliveries, active runs, heartbeat."""
    conn = get_db()

    def _write(c):
        storage.update_heartbeat(c, agent_id)
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        storage.touch_session(c, session_id, _iso_plus(DEFAULT_SESSION_LEASE))

        ready_work = storage.list_ready_work_items(c, agent_id=agent_id, limit=10)
        active_runs = storage.list_runs_for_agent(c, agent_id, status="running")
        offered_runs = storage.list_runs_for_agent(c, agent_id, status="offered")
        pending_deliveries = storage.list_pending_deliveries(c, "agent", agent_id, limit=50)
        pending_approvals = storage.list_pending_approvals(c)

        latest_event = conn.execute(
            "SELECT MAX(event_id) AS max_id FROM events"
        ).fetchone()
        latest_event_id = latest_event["max_id"] if latest_event and latest_event["max_id"] else 0

        return {
            "ready_work": [_work_item_dict(wi) for wi in ready_work],
            "active_runs": [_run_dict(r) for r in active_runs],
            "offered_runs": [_run_dict(r) for r in offered_runs],
            "deliveries": pending_deliveries,
            "pending_approvals": [{"id": a.id, "task_id": a.task_id,
                                    "action": a.action, "reason": a.reason}
                                   for a in pending_approvals],
            "latest_event_id": latest_event_id,
            "session_lease_expires_at": _iso_plus(DEFAULT_SESSION_LEASE),
        }

    return write_executor.execute_write(conn, _write, immediate=False)


# ════════════════════════════════════════════════════════════════════
#  Scheduler: dependency advancement + lease expiry + retry
# ════════════════════════════════════════════════════════════════════

def reconcile() -> dict:
    """Run by the scheduler: expire stale sessions/runs/locks, advance deps."""
    conn = get_db()

    def _write(c):
        expired_sessions = storage.expire_stale_sessions(c)
        lost_runs = storage.expire_stale_runs(c)
        expired_locks = storage.expire_stale_locks(c)

        for run_id in lost_runs:
            run = storage.get_run(c, run_id)
            if run:
                wi = storage.get_work_item(c, run.work_item_id)
                if wi:
                    storage.update_work_item_status(c, wi.id, "ready")
                    storage.append_event(c, wi.task_id, "run.lost",
                        work_item_id=wi.id, run_id=run_id,
                        payload_json=json.dumps({"reason": "lease_expired"}))

        tasks = storage.list_tasks(c, status="running")
        for task in tasks:
            _advance_ready_work_items(c, task.id)
            _check_task_completion(c, task.id)

        return {
            "expired_sessions": expired_sessions,
            "lost_runs": len(lost_runs),
            "expired_locks": expired_locks,
        }

    return write_executor.execute_write(conn, _write)


# ════════════════════════════════════════════════════════════════════
#  Hub diagnostics
# ════════════════════════════════════════════════════════════════════

def hub_status() -> dict:
    """Runtime diagnostics: version, db path, counts, lease health."""
    from . import __version__
    from .db import DEFAULT_DB_PATH
    conn = get_db()

    counts = {}
    for table in ("agents", "tasks", "work_items", "runs", "sessions",
                  "events", "deliveries", "outbox", "approvals", "resource_locks"):
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        counts[table] = row["n"] if row else 0

    stale_runs = conn.execute(
        "SELECT COUNT(*) AS n FROM runs WHERE status IN ('offered','claimed','running') AND lease_expires_at < ?",
        (now_iso(),),
    ).fetchone()
    stale_sessions = conn.execute(
        "SELECT COUNT(*) AS n FROM sessions WHERE status='active' AND lease_expires_at < ?",
        (now_iso(),),
    ).fetchone()
    pending_outbox = conn.execute(
        "SELECT COUNT(*) AS n FROM outbox WHERE status='pending'"
    ).fetchone()

    from .db import get_applied_migrations
    migrations = get_applied_migrations(conn)

    return {
        "version": __version__,
        "db_path": str(DEFAULT_DB_PATH),
        "counts": counts,
        "health": {
            "stale_runs": stale_runs["n"] if stale_runs else 0,
            "stale_sessions": stale_sessions["n"] if stale_sessions else 0,
            "pending_outbox": pending_outbox["n"] if pending_outbox else 0,
        },
        "migrations": [m["version"] for m in migrations],
    }


# ════════════════════════════════════════════════════════════════════
#  Internal helpers
# ════════════════════════════════════════════════════════════════════

def _advance_ready_work_items(conn, task_id: str):
    """Move pending work items to 'ready' if all dependencies are satisfied."""
    pending_items = storage.list_work_items(conn, task_id, status="pending")
    for wi in pending_items:
        deps = storage.get_dependencies(conn, wi.id)
        if not deps:
            storage.update_work_item_status(conn, wi.id, "ready")
            continue
        all_satisfied = True
        for dep in deps:
            dep_wi = storage.get_work_item(conn, dep.depends_on_id)
            if not dep_wi:
                all_satisfied = False
                break
            if dep.condition == "succeeded" and dep_wi.status != "succeeded":
                all_satisfied = False
                break
            elif dep.condition == "failed" and dep_wi.status != "failed":
                all_satisfied = False
                break
            elif dep.condition == "completed" and dep_wi.status not in ("succeeded", "failed", "cancelled"):
                all_satisfied = False
                break
        if all_satisfied:
            storage.update_work_item_status(conn, wi.id, "ready")
            storage.append_event(conn, task_id, "work.ready", work_item_id=wi.id)


def _check_task_completion(conn, task_id: str):
    """If all work items are terminal, mark the task complete."""
    items = storage.list_work_items(conn, task_id)
    if not items:
        return
    terminal = ("succeeded", "failed", "cancelled")
    all_terminal = all(wi.status in terminal for wi in items)
    if not all_terminal:
        return
    any_failed = any(wi.status == "failed" for wi in items)
    task = storage.get_task(conn, task_id)
    if task and task.status in ("running", "verifying"):
        final_status = "failed" if any_failed else "completed"
        storage.update_task_status(conn, task_id, final_status, completed_at=now_iso())
        storage.append_event(conn, task_id, f"task.{final_status}")


def _task_dict(task: Task) -> dict:
    return {
        "id": task.id, "objective": task.objective, "status": task.status,
        "priority": task.priority, "plan_version": task.plan_version,
        "deadline_at": task.deadline_at, "created_by_agent_id": task.created_by_agent_id,
        "created_at": task.created_at, "updated_at": task.updated_at,
        "completed_at": task.completed_at,
    }


def _work_item_dict(wi: WorkItem) -> dict:
    return {
        "id": wi.id, "task_id": wi.task_id, "kind": wi.kind,
        "objective": wi.objective, "status": wi.status,
        "priority": wi.priority, "needs_review": wi.needs_review,
        "version": wi.version, "preferred_agent_id": wi.preferred_agent_id,
    }


def _run_dict(run: Run) -> dict:
    return {
        "id": run.id, "work_item_id": run.work_item_id, "attempt_no": run.attempt_no,
        "agent_id": run.agent_id, "session_id": run.session_id,
        "status": run.status, "fencing_token": run.fencing_token,
        "lease_expires_at": run.lease_expires_at, "heartbeat_at": run.heartbeat_at,
        "checkpoint_id": run.checkpoint_id, "failure_code": run.failure_code,
    }
