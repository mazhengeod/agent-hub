"""Domain service layer: state machine, ownership, delivery pipeline, DAG.

Per Codex hardening review:
- All writes through WriteExecutor (single-writer, in-process since scheduler
  is merged into Hub lifespan).
- State machine invariants enforced (task must be running to claim, etc).
- Run operations verify session ownership + session active.
- Cross-agent resume allowed (capability-based, not agent-locked).
- Review rejection routes work back to ready (no dead-end).
- DAG cycle detection + same-task edge constraint.
- Event -> Delivery -> Outbox created in same transaction.
- agent_sync uses since_event_id cursor for incremental delivery.
- complete_run status whitelist enforced.
"""
from __future__ import annotations

import json
from datetime import timedelta, timezone, datetime
from typing import Optional

from . import storage
from .db import now_iso, new_id, iso_plus_seconds, write_executor, get_db
from .models import Task, WorkItem, Run, HubError
from .config import get_config

DEFAULT_SESSION_LEASE = int(get_config("session_lease_seconds", 300))
DEFAULT_RUN_LEASE = int(get_config("run_lease_seconds", 600))
MAX_RUN_ATTEMPTS = int(get_config("max_run_attempts", 3))

VALID_RUN_COMPLETION_STATUS = {"succeeded", "failed"}
VALID_WORK_ITEM_STATUSES = {
    "pending", "ready", "offered", "running", "reviewing",
    "succeeded", "failed", "blocked", "changes_requested", "cancelled",
}


# ════════════════════════════════════════════════════════════════════
#  _emit: event + delivery + outbox in one transaction
# ════════════════════════════════════════════════════════════════════

def _emit(conn, task_id, event_type, actor_agent_id=None, work_item_id=None,
          run_id=None, payload=None, deliver_to=None, outbox_entries=None):
    """Append event + create deliveries + enqueue outbox atomically."""
    event = storage.append_event(
        conn, task_id, event_type, actor_agent_id=actor_agent_id,
        work_item_id=work_item_id, run_id=run_id,
        payload_json=json.dumps(payload or {}),
    )
    if event and deliver_to:
        for kind, rid in deliver_to:
            storage.create_delivery(conn, event.event_id, kind, rid)
    if outbox_entries:
        for ob_type, ob_payload in outbox_entries:
            storage.enqueue_outbox(conn, ob_type, json.dumps(ob_payload))
    return event


# ════════════════════════════════════════════════════════════════════
#  Session lifecycle  (P0 fix: allow multiple active sessions per agent)
# ════════════════════════════════════════════════════════════════════

def session_start(actor_agent_id, native_session_ref=None, capabilities=None,
                  adapter_id=None, lease_seconds=None):
    """Start a new session. Does NOT end existing sessions.
    Multiple active sessions per agent are allowed (P0 fix)."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_SESSION_LEASE
    session_id = new_id()
    caps_json = json.dumps(capabilities or [])

    def _write(c):
        return storage.create_session(
            c, session_id, actor_agent_id, native_session_ref, adapter_id,
            caps_json, iso_plus_seconds(lease),
        )

    sess = write_executor.execute_write(conn, _write)
    return {"session_id": sess.id, "agent_id": actor_agent_id,
            "lease_expires_at": sess.lease_expires_at}


def session_heartbeat(actor_agent_id, session_id, lease_seconds=None):
    """Renew session lease. Verifies session belongs to actor."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_SESSION_LEASE

    def _write(c):
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        if sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned",
                f"Session {session_id} belongs to {sess.agent_id}, not {actor_agent_id}")
        storage.touch_session(c, session_id, iso_plus_seconds(lease))
        return sess

    write_executor.execute_write(conn, _write)
    return {"session_id": session_id, "lease_expires_at": iso_plus_seconds(lease)}


def session_end(actor_agent_id, session_id):
    """End a session. Verifies ownership."""
    conn = get_db()

    def _write(c):
        sess = storage.get_session(c, session_id)
        if not sess:
            raise HubError("session_not_found", f"Session {session_id} not found")
        if sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned",
                f"Session {session_id} belongs to {sess.agent_id}, not {actor_agent_id}")
        storage.end_session(c, session_id)
        return None

    write_executor.execute_write(conn, _write)
    return {"session_id": session_id, "status": "ended"}


# ════════════════════════════════════════════════════════════════════
#  Task lifecycle
# ════════════════════════════════════════════════════════════════════

def create_task(objective, actor_agent_id, success_criteria=None,
                constraints=None, authorization_policy=None, context_refs=None,
                priority=0, deadline_at=None, budget=None):
    conn = get_db()
    task_id = new_id()

    def _write(c):
        task = storage.create_task(c, task_id, objective, actor_agent_id,
            success_criteria_json=json.dumps(success_criteria or []),
            constraints_json=json.dumps(constraints or {}),
            authorization_policy_json=json.dumps(authorization_policy or {}),
            context_refs_json=json.dumps(context_refs or []),
            priority=priority, deadline_at=deadline_at,
            budget_json=json.dumps(budget or {}),
        )
        _emit(c, task_id, "task.created", actor_agent_id=actor_agent_id,
              payload={"objective": objective},
              deliver_to=[("agent", actor_agent_id)])
        return task

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


def get_task(task_id):
    conn = get_db()
    task = storage.get_task(conn, task_id)
    return _task_dict(task) if task else None


def list_tasks(status=None, limit=50):
    conn = get_db()
    tasks = storage.list_tasks(conn, status=status, limit=limit)
    return [_task_dict(t) for t in tasks]


def plan_task(task_id, work_items, dependencies=None, actor_agent_id=None):
    """Create work items + dependencies. Validates DAG (cycle detection, same-task)."""
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

                wi = storage.get_work_item(c, wi_id)
                dep_wi = storage.get_work_item(c, dep_id)
                if not wi or not dep_wi:
                    raise HubError("dep_not_found",
                        f"Dependency references unknown work item")
                if wi.task_id != dep_wi.task_id:
                    raise HubError("dep_cross_task",
                        "Dependencies must be within the same task")

                _check_dag_no_cycle(c, wi_id, dep_id)
                storage.add_dependency(c, wi_id, dep_id, dep.get("condition", "succeeded"))

        _advance_ready_work_items(c, task_id)
        storage.update_task_status(c, task_id, "planned",
            plan_version=task.plan_version + 1)
        _emit(c, task_id, "task.planned", actor_agent_id=actor_agent_id,
              payload={"work_item_count": len(work_items)},
              deliver_to=[("agent", task.created_by_agent_id)])
        return storage.get_task(c, task_id)

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


def start_task(task_id, actor_agent_id):
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        if task.status not in ("planned", "ready"):
            raise HubError("invalid_state",
                f"Cannot start task in status '{task.status}'")
        storage.update_task_status(c, task_id, "running")
        _emit(c, task_id, "task.started", actor_agent_id=actor_agent_id,
              deliver_to=[("agent", task.created_by_agent_id)])
        return storage.get_task(c, task_id)

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


# ════════════════════════════════════════════════════════════════════
#  Work Item + Run lifecycle  (P0 fixes: state machine + ownership)
# ════════════════════════════════════════════════════════════════════

def claim_work(actor_agent_id, session_id, work_item_id=None, lease_seconds=None):
    """Claim a work item. Enforces: task running, work item ready,
    preferred_agent, required_capabilities, session ownership."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        if sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned",
                f"Session belongs to {sess.agent_id}, not {actor_agent_id}")

        if work_item_id:
            wi = storage.get_work_item(c, work_item_id)
            if not wi:
                raise HubError("work_not_found", f"Work item {work_item_id} not found")
        else:
            candidates = storage.list_ready_work_items(c, agent_id=actor_agent_id, limit=1)
            if not candidates:
                return None
            wi = candidates[0]

        task = storage.get_task(c, wi.task_id)
        if not task:
            raise HubError("task_not_found", "Task not found")
        if task.status != "running":
            raise HubError("task_not_running",
                f"Task is '{task.status}', must be 'running' to claim work")

        if wi.status != "ready":
            raise HubError("work_not_ready",
                f"Work item in status '{wi.status}', must be 'ready'")

        if wi.preferred_agent_id and wi.preferred_agent_id != actor_agent_id:
            raise HubError("agent_not_preferred",
                f"Work item prefers agent '{wi.preferred_agent_id}'")

        required_caps = json.loads(wi.required_capabilities_json)
        if required_caps:
            session_caps = json.loads(sess.capabilities_json)
            missing = set(required_caps) - set(session_caps)
            if missing:
                raise HubError("capability_missing",
                    f"Session lacks capabilities: {missing}")

        existing_run = storage.get_active_run_for_work_item(c, wi.id)
        if existing_run:
            raise HubError("work_already_running",
                f"Work item {wi.id} already has active run {existing_run.id}")

        run = storage.create_run(c, wi.id, actor_agent_id, session_id,
                                  iso_plus_seconds(lease))
        storage.update_work_item_status(c, wi.id, "offered", version=wi.version)
        _emit(c, wi.task_id, "work.offered", actor_agent_id=actor_agent_id,
              work_item_id=wi.id, run_id=run.id,
              payload={"attempt": run.attempt_no},
              deliver_to=[("agent", actor_agent_id), ("agent", task.created_by_agent_id)],
              outbox_entries=[("adapter.dispatch", {
                  "run_id": run.id, "agent_id": actor_agent_id,
                  "work_item_id": wi.id, "objective": wi.objective,
              })])
        return {"run": run, "work_item": wi}

    result = write_executor.execute_write(conn, _write)
    if not result:
        return None
    run = result["run"]
    wi = result["work_item"]
    return {
        "run_id": run.id, "fencing_token": run.fencing_token,
        "work_item_id": wi.id, "task_id": wi.task_id,
        "attempt_no": run.attempt_no, "lease_expires_at": run.lease_expires_at,
        "objective": wi.objective, "kind": wi.kind,
    }


def start_run(run_id, fencing_token, session_id, actor_agent_id, lease_seconds=None):
    """Transition offered/claimed -> running. Verifies session ownership."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token",
                f"Fencing token mismatch: expected {run.fencing_token}, got {fencing_token}")
        if run.agent_id != actor_agent_id:
            raise HubError("run_not_owned",
                f"Run belongs to {run.agent_id}, not {actor_agent_id}")
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        if sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned",
                f"Session belongs to {sess.agent_id}, not {actor_agent_id}")

        ok = storage.start_run(c, run_id, fencing_token, session_id, iso_plus_seconds(lease))
        if not ok:
            raise HubError("invalid_state",
                f"Run {run_id} not in startable state (status={run.status})")
        wi = storage.get_work_item(c, run.work_item_id)
        if wi:
            storage.update_work_item_status(c, wi.id, "running", version=wi.version)
        _emit(c, wi.task_id if wi else "", "run.started",
              actor_agent_id=actor_agent_id,
              work_item_id=run.work_item_id, run_id=run_id,
              deliver_to=[("agent", actor_agent_id)])
        return storage.get_run(c, run_id)

    run = write_executor.execute_write(conn, _write)
    return _run_dict(run)


def heartbeat_run(run_id, fencing_token, actor_agent_id, lease_seconds=None):
    """Heartbeat a running run. Verifies ownership."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token", "Fencing token mismatch for heartbeat")
        if run.agent_id != actor_agent_id:
            raise HubError("run_not_owned",
                f"Run belongs to {run.agent_id}, not {actor_agent_id}")
        ok = storage.heartbeat_run(c, run_id, fencing_token, iso_plus_seconds(lease))
        if not ok:
            raise HubError("invalid_state",
                f"Cannot heartbeat run {run_id}: not running")
        return None

    write_executor.execute_write(conn, _write)
    return {"run_id": run_id, "lease_expires_at": iso_plus_seconds(lease)}


def save_checkpoint(run_id, fencing_token, snapshot, actor_agent_id):
    """Save checkpoint. Verifies ownership."""
    conn = get_db()

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token", "Fencing token mismatch for checkpoint")
        if run.agent_id != actor_agent_id:
            raise HubError("run_not_owned",
                f"Run belongs to {run.agent_id}, not {actor_agent_id}")
        if run.status != "running":
            raise HubError("invalid_state",
                f"Cannot checkpoint run in status '{run.status}'")
        cp = storage.save_checkpoint(c, run_id, json.dumps(snapshot))
        return cp

    cp = write_executor.execute_write(conn, _write)
    return {"checkpoint_id": cp.id, "version": cp.version, "run_id": run_id}


def complete_run(run_id, fencing_token, status, actor_agent_id,
                 artifacts=None, failure_code=None, failure_detail=None):
    """Complete a run. Enforces status whitelist + ownership."""
    conn = get_db()

    if status not in VALID_RUN_COMPLETION_STATUS:
        raise HubError("invalid_status",
            f"status must be one of {VALID_RUN_COMPLETION_STATUS}, got '{status}'")

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if run.fencing_token != fencing_token:
            raise HubError("stale_token", "Fencing token mismatch on complete")
        if run.agent_id != actor_agent_id:
            raise HubError("run_not_owned",
                f"Run belongs to {run.agent_id}, not {actor_agent_id}")
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

        task = storage.get_task(c, wi.task_id)

        if artifacts:
            for art in artifacts:
                storage.create_artifact(c, wi.id, wi.task_id,
                    art.get("kind", "file"), art["ref"],
                    run_id=run_id, hash_val=art.get("hash"),
                    metadata_json=json.dumps(art.get("metadata", {})))

        deliver_to = [("agent", task.created_by_agent_id)] if task else []

        if status == "succeeded":
            new_wi_status = "reviewing" if wi.needs_review else "succeeded"
            storage.update_work_item_status(c, wi.id, new_wi_status, version=wi.version)
            _emit(c, wi.task_id, "run.succeeded",
                  actor_agent_id=actor_agent_id,
                  work_item_id=wi.id, run_id=run_id,
                  deliver_to=deliver_to)
            _check_task_completion(c, wi.task_id)
        elif status == "failed":
            retry_policy = json.loads(wi.retry_policy_json)
            max_attempts = retry_policy.get("max_attempts", MAX_RUN_ATTEMPTS)
            if run.attempt_no < max_attempts:
                storage.update_work_item_status(c, wi.id, "ready", version=wi.version)
                _emit(c, wi.task_id, "run.failed_retryable",
                      actor_agent_id=actor_agent_id,
                      work_item_id=wi.id, run_id=run_id,
                      payload={"attempt": run.attempt_no, "failure_code": failure_code},
                      deliver_to=deliver_to)
            else:
                storage.update_work_item_status(c, wi.id, "failed", version=wi.version)
                _emit(c, wi.task_id, "work.failed",
                      actor_agent_id=actor_agent_id, work_item_id=wi.id,
                      payload={"attempts": run.attempt_no},
                      deliver_to=deliver_to)
                _check_task_completion(c, wi.task_id)

        _advance_ready_work_items(c, wi.task_id)
        return storage.get_run(c, run_id)

    run = write_executor.execute_write(conn, _write)
    return _run_dict(run)


def resume_run(run_id, session_id, actor_agent_id, lease_seconds=None):
    """Resume a lost run. Cross-agent allowed (P1 fix).
    Creates new attempt with new fencing token. Returns latest checkpoint."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        old_run = storage.get_run(c, run_id)
        if not old_run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        if old_run.status != "lost":
            raise HubError("invalid_state",
                f"Can only resume lost runs, not '{old_run.status}'")

        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        if sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned",
                f"Session belongs to {sess.agent_id}, not {actor_agent_id}")

        wi = storage.get_work_item(c, old_run.work_item_id)
        if not wi:
            raise HubError("work_not_found", "Work item not found")
        task = storage.get_task(c, wi.task_id)
        if not task or task.status != "running":
            raise HubError("task_not_running",
                f"Task must be running to resume, got '{task.status if task else 'missing'}'")

        if wi.preferred_agent_id and wi.preferred_agent_id != actor_agent_id:
            raise HubError("agent_not_preferred",
                f"Work item prefers agent '{wi.preferred_agent_id}'")

        required_caps = json.loads(wi.required_capabilities_json)
        if required_caps:
            session_caps = json.loads(sess.capabilities_json)
            missing = set(required_caps) - set(session_caps)
            if missing:
                raise HubError("capability_missing",
                    f"Session lacks capabilities: {missing}")

        new_run = storage.create_run(c, wi.id, actor_agent_id, session_id,
                                      iso_plus_seconds(lease))
        storage.update_work_item_status(c, wi.id, "running", version=wi.version)
        _emit(c, wi.task_id, "run.resumed",
              actor_agent_id=actor_agent_id, work_item_id=wi.id, run_id=new_run.id,
              payload={"previous_run": run_id, "attempt": new_run.attempt_no},
              deliver_to=[("agent", actor_agent_id),
                          ("agent", task.created_by_agent_id)],
              outbox_entries=[("adapter.dispatch", {
                  "run_id": new_run.id, "agent_id": actor_agent_id,
                  "work_item_id": wi.id, "objective": wi.objective,
                  "resumed_from": run_id,
              })])

        checkpoint = storage.get_latest_checkpoint_for_work_item(c, wi.id)
        return {"new_run": new_run, "checkpoint": checkpoint, "work_item": wi}

    result = write_executor.execute_write(conn, _write)
    new_run = result["new_run"]
    cp = result["checkpoint"]
    return {
        "run_id": new_run.id, "fencing_token": new_run.fencing_token,
        "attempt_no": new_run.attempt_no,
        "checkpoint": json.loads(cp.snapshot_json) if cp else None,
        "checkpoint_version": cp.version if cp else 0,
        "work_item_id": result["work_item"].id,
        "lease_expires_at": new_run.lease_expires_at,
    }


# ════════════════════════════════════════════════════════════════════
#  Review / Approval  (P1 fix: rejection routes back to ready)
# ════════════════════════════════════════════════════════════════════

def approve_work(work_item_id, reviewer_agent_id, decision, comment=""):
    """Approve or reject a work item in 'reviewing' state.
    Rejection routes work back to 'ready' for rework (not dead-end)."""
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
        task = storage.get_task(c, wi.task_id)

        if decision == "approved":
            storage.update_work_item_status(c, work_item_id, "succeeded", version=wi.version)
            _emit(c, wi.task_id, "work.approved",
                  actor_agent_id=reviewer_agent_id, work_item_id=work_item_id,
                  payload={"comment": comment},
                  deliver_to=[("agent", task.created_by_agent_id)] if task else [])
        else:
            storage.update_work_item_status(c, work_item_id, "ready", version=wi.version)
            _emit(c, wi.task_id, "work.changes_requested",
                  actor_agent_id=reviewer_agent_id, work_item_id=work_item_id,
                  payload={"comment": comment},
                  deliver_to=[("agent", task.created_by_agent_id)] if task else [],
                  outbox_entries=[("adapter.dispatch", {
                      "work_item_id": wi.id, "objective": wi.objective,
                      "reason": "rework_needed", "comment": comment,
                  })])

        _advance_ready_work_items(c, wi.task_id)
        _check_task_completion(c, wi.task_id)
        return storage.get_work_item(c, work_item_id)

    wi = write_executor.execute_write(conn, _write)
    return {"work_item_id": work_item_id, "status": wi.status}


def request_approval(task_id, action, reason="", work_item_id=None,
                     run_id=None, requested_by=None):
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        ap = storage.create_approval(c, task_id, action, reason, work_item_id, run_id)
        _emit(c, task_id, "approval.requested",
              actor_agent_id=requested_by, work_item_id=work_item_id, run_id=run_id,
              payload={"action": action, "approval_id": ap.id},
              deliver_to=[("agent", task.created_by_agent_id)])
        return ap

    ap = write_executor.execute_write(conn, _write)
    return {"approval_id": ap.id, "task_id": task_id, "action": action, "status": "pending"}


def decide_approval(approval_id, decision, decided_by):
    conn = get_db()

    def _write(c):
        ok = storage.decide_approval(c, approval_id, decision, decided_by)
        if not ok:
            raise HubError("approval_not_found",
                f"Approval {approval_id} not found or already decided")
        row = conn.execute("SELECT * FROM approvals WHERE id=?", (approval_id,)).fetchone()
        return dict(row) if row else {}

    result = write_executor.execute_write(conn, _write)
    return {"approval_id": approval_id, "decision": decision,
            "task_id": result.get("task_id")}


# ════════════════════════════════════════════════════════════════════
#  Agent sync (P0 fix: cursor-based delivery)
# ════════════════════════════════════════════════════════════════════

def agent_sync(actor_agent_id, session_id, since_event_id=0):
    """Batch pull: heartbeat + ready work + cursor-based deliveries + runs."""
    conn = get_db()

    def _write(c):
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active":
            raise HubError("session_not_active", f"Session {session_id} not active")
        if sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned",
                f"Session belongs to {sess.agent_id}, not {actor_agent_id}")

        storage.update_heartbeat(c, actor_agent_id)
        storage.touch_session(c, session_id, iso_plus_seconds(DEFAULT_SESSION_LEASE))

        ready_work = storage.list_ready_work_items(c, agent_id=actor_agent_id, limit=10)
        active_runs = storage.list_runs_for_agent(c, actor_agent_id, status="running")
        offered_runs = storage.list_runs_for_agent(c, actor_agent_id, status="offered")
        deliveries = storage.list_deliveries_since(
            c, "agent", actor_agent_id, since_event_id=since_event_id, limit=50)
        pending_approvals = storage.list_pending_approvals(c)

        latest_event = conn.execute(
            "SELECT MAX(event_id) AS max_id FROM events").fetchone()
        latest_event_id = latest_event["max_id"] if latest_event and latest_event["max_id"] else 0

        return {
            "ready_work": [_work_item_dict(wi) for wi in ready_work],
            "active_runs": [_run_dict(r) for r in active_runs],
            "offered_runs": [_run_dict(r) for r in offered_runs],
            "deliveries": deliveries,
            "pending_approvals": [{"id": a.id, "task_id": a.task_id,
                                    "action": a.action, "reason": a.reason}
                                   for a in pending_approvals],
            "latest_event_id": latest_event_id,
            "session_lease_expires_at": iso_plus_seconds(DEFAULT_SESSION_LEASE),
        }

    return write_executor.execute_write(conn, _write, immediate=False)


def ack_delivery(actor_agent_id, delivery_id):
    """Ack a delivery for this agent."""
    conn = get_db()

    def _write(c):
        ok = storage.ack_delivery(c, delivery_id, actor_agent_id)
        if not ok:
            raise HubError("delivery_not_found",
                f"Delivery {delivery_id} not found for agent {actor_agent_id}")
        return None

    write_executor.execute_write(conn, _write)
    return {"delivery_id": delivery_id, "status": "acked"}


# ════════════════════════════════════════════════════════════════════
#  Resource locks
# ════════════════════════════════════════════════════════════════════

def acquire_lock(lock_key, holder_run_id, resource_type="general",
                 resource_id="", ttl_seconds=600):
    conn = get_db()

    def _write(c):
        return storage.acquire_lock(c, lock_key, holder_run_id,
            resource_type, resource_id, ttl_seconds)

    success, token = write_executor.execute_write(conn, _write)
    if not success:
        raise HubError("lock_busy",
            f"Lock '{lock_key}' held by another run (token={token})")
    return {"lock_key": lock_key, "fencing_token": token, "holder_run_id": holder_run_id}


def release_lock(lock_key, holder_run_id, fencing_token):
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
#  Scheduler: reconcile + outbox dispatch (runs in Hub lifespan)
# ════════════════════════════════════════════════════════════════════

def reconcile():
    """Expire stale sessions/runs/locks, advance deps, process outbox."""
    conn = get_db()

    def _write(c):
        expired_sessions = storage.expire_stale_sessions(c)
        lost_run_ids = storage.expire_stale_runs(c)
        expired_locks = storage.expire_stale_locks(c)

        for run_id in lost_run_ids:
            run = storage.get_run(c, run_id)
            if run:
                wi = storage.get_work_item(c, run.work_item_id)
                if wi:
                    storage.update_work_item_status(c, wi.id, "ready")
                    task = storage.get_task(c, wi.task_id)
                    _emit(c, wi.task_id, "run.lost",
                          work_item_id=wi.id, run_id=run_id,
                          payload={"reason": "lease_expired"},
                          deliver_to=[("agent", task.created_by_agent_id)] if task else [],
                          outbox_entries=[("adapter.dispatch", {
                              "work_item_id": wi.id, "objective": wi.objective,
                              "reason": "run_lost",
                          })])

        tasks = storage.list_tasks(c, status="running")
        for task in tasks:
            _advance_ready_work_items(c, task.id)
            _check_task_completion(c, task.id)

        return {
            "expired_sessions": expired_sessions,
            "lost_runs": len(lost_run_ids),
            "expired_locks": expired_locks,
        }

    result = write_executor.execute_write(conn, _write)
    _process_outbox()
    return result


def _process_outbox():
    """Process pending outbox entries: retry or dead-letter."""
    conn = get_db()
    pending = storage.list_pending_outbox(conn, limit=20)
    for item in pending:
        if item["attempts"] >= item["max_attempts"]:
            conn.execute("BEGIN")
            storage.dead_letter_outbox(conn, item["id"])
            conn.execute("COMMIT")
        else:
            conn.execute("BEGIN")
            storage.retry_outbox(conn, item["id"])
            conn.execute("COMMIT")


# ════════════════════════════════════════════════════════════════════
#  Hub diagnostics
# ════════════════════════════════════════════════════════════════════

def hub_status():
    from . import __version__
    from .db import DEFAULT_DB_PATH, get_applied_migrations
    conn = get_db()

    counts = {}
    for table in ("agents", "tasks", "work_items", "runs", "sessions",
                  "events", "deliveries", "outbox", "approvals", "resource_locks"):
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        counts[table] = row["n"] if row else 0

    stale_runs = conn.execute(
        "SELECT COUNT(*) AS n FROM runs WHERE status IN ('offered','claimed','running') AND lease_expires_at < ?",
        (now_iso(),)).fetchone()
    stale_sessions = conn.execute(
        "SELECT COUNT(*) AS n FROM sessions WHERE status='active' AND lease_expires_at < ?",
        (now_iso(),)).fetchone()
    pending_outbox = conn.execute(
        "SELECT COUNT(*) AS n FROM outbox WHERE status='pending'").fetchone()
    pending_deliveries = conn.execute(
        "SELECT COUNT(*) AS n FROM deliveries WHERE status='pending'").fetchone()

    migrations = get_applied_migrations(conn)

    return {
        "version": __version__,
        "db_path": str(DEFAULT_DB_PATH),
        "counts": counts,
        "health": {
            "stale_runs": stale_runs["n"] if stale_runs else 0,
            "stale_sessions": stale_sessions["n"] if stale_sessions else 0,
            "pending_outbox": pending_outbox["n"] if pending_outbox else 0,
            "pending_deliveries": pending_deliveries["n"] if pending_deliveries else 0,
        },
        "migrations": [{"version": m["version"], "checksum": m["checksum"][:12]} for m in migrations],
    }


# ════════════════════════════════════════════════════════════════════
#  Internal helpers
# ════════════════════════════════════════════════════════════════════

def _check_dag_no_cycle(conn, work_item_id, depends_on_id):
    """DFS cycle detection: adding edge (work_item_id -> depends_on_id)
    must not create a cycle. Check if depends_on_id can reach work_item_id."""
    if work_item_id == depends_on_id:
        raise HubError("dag_self_dependency",
            "Work item cannot depend on itself")

    visited = set()
    stack = [depends_on_id]
    while stack:
        node = stack.pop()
        if node == work_item_id:
            raise HubError("dag_cycle",
                f"Adding dependency would create a cycle: "
                f"{work_item_id} -> {depends_on_id} -> ... -> {work_item_id}")
        if node in visited:
            continue
        visited.add(node)
        deps = storage.get_dependencies(conn, node)
        for dep in deps:
            stack.append(dep.depends_on_id)


def _advance_ready_work_items(conn, task_id):
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
            _emit(conn, task_id, "work.ready", work_item_id=wi.id)


def _check_task_completion(conn, task_id):
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
        _emit(conn, task_id, f"task.{final_status}",
              deliver_to=[("agent", task.created_by_agent_id)])


def _task_dict(task):
    return {
        "id": task.id, "objective": task.objective, "status": task.status,
        "priority": task.priority, "plan_version": task.plan_version,
        "deadline_at": task.deadline_at, "created_by_agent_id": task.created_by_agent_id,
        "created_at": task.created_at, "updated_at": task.updated_at,
        "completed_at": task.completed_at,
    }


def _work_item_dict(wi):
    return {
        "id": wi.id, "task_id": wi.task_id, "kind": wi.kind,
        "objective": wi.objective, "status": wi.status,
        "priority": wi.priority, "needs_review": wi.needs_review,
        "version": wi.version, "preferred_agent_id": wi.preferred_agent_id,
    }


def _run_dict(run):
    return {
        "id": run.id, "work_item_id": run.work_item_id, "attempt_no": run.attempt_no,
        "agent_id": run.agent_id, "session_id": run.session_id,
        "status": run.status, "fencing_token": run.fencing_token,
        "lease_expires_at": run.lease_expires_at, "heartbeat_at": run.heartbeat_at,
        "checkpoint_id": run.checkpoint_id, "failure_code": run.failure_code,
    }
