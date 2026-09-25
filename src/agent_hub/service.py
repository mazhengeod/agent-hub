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
import re
from datetime import timedelta, timezone, datetime
from typing import Optional

from pydantic import ValidationError

from . import storage
from .db import now_iso, new_id, iso_plus_seconds, write_executor, get_db
from .models import (
    ArtifactSpec,
    DependencySpec,
    HubError,
    Run,
    Task,
    WorkItem,
    WorkItemSpec,
)
from .config import get_config

DEFAULT_SESSION_LEASE = int(get_config("session_lease_seconds", 300))
DEFAULT_RUN_LEASE = int(get_config("run_lease_seconds", 600))
MAX_RUN_ATTEMPTS = int(get_config("max_run_attempts", 3))
MAX_RUNS_PER_TASK = int(get_config("max_runs_per_task", 100))
MAX_WORK_ITEMS_PER_TASK = int(get_config("max_work_items_per_task", 100))
MAX_WORK_DEPTH = int(get_config("max_work_depth", 6))
DEFAULT_COORDINATOR_LEASE = int(get_config("coordinator_lease_seconds", 600))
DEFAULT_OUTBOX_LEASE = int(get_config("outbox_lease_seconds", 60))
DEFAULT_OFFER_LEASE = int(get_config("offer_lease_seconds", 900))
DEFAULT_APPROVAL_TTL = int(get_config("approval_ttl_seconds", 604800))
DELIVERY_TERMINAL_RETENTION_DAYS = int(
    get_config("delivery_terminal_retention_days", 30)
)
TASK_ATTENTION_HOURS = int(get_config("task_attention_hours", 24))
MAX_EVENT_PAYLOAD_BYTES = int(get_config("max_event_payload_bytes", 65536))
MAX_DELIVERY_ACK_BATCH = int(get_config("max_delivery_ack_batch", 200))

VALID_RUN_COMPLETION_STATUS = {"succeeded", "failed"}
VALID_WORK_ITEM_STATUSES = {
    "pending", "ready", "offered", "running", "reviewing",
    "succeeded", "failed", "blocked", "cancelled",
}
VALID_ADAPTER_MODES = {"resident_runner", "webhook", "online_session", "manual_resume"}
VALID_APPROVAL_DECISIONS = {"approved", "rejected"}
VALID_PARTICIPANT_ROLES = {"owner", "coordinator", "worker", "reviewer", "observer"}
VALID_WORK_KINDS = {"plan", "research", "implement", "review", "verify", "operate", "summarize"}
_RUNTIME_STATE = {"last_reconcile_at": None, "last_reconcile_result": {}}
EVENT_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")


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
        for entry in outbox_entries:
            ob_type, ob_payload = entry[0], entry[1]
            adapter_id = entry[2] if len(entry) > 2 else None
            storage.enqueue_outbox(
                conn, ob_type, json.dumps(ob_payload), adapter_id=adapter_id)
    return event


def _json_obj(raw, default):
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError) as exc:
        raise HubError("invalid_json", f"Invalid stored JSON: {exc}") from exc


def _validation_error(code, message, exc):
    detail = exc.errors(include_url=False, include_context=False)
    raise HubError(code, f"{message}: {detail}") from None


def _normalize_work_item_specs(work_items):
    if not isinstance(work_items, list) or not work_items:
        raise HubError("invalid_work_items", "work_items must be a non-empty list")
    try:
        return [
            WorkItemSpec.model_validate(item).model_dump(exclude_none=True)
            for item in work_items
        ]
    except ValidationError as exc:
        _validation_error("invalid_work_item", "Invalid Work Item specification", exc)


def _normalize_dependency_specs(dependencies):
    if dependencies is None:
        return []
    if not isinstance(dependencies, list):
        raise HubError("invalid_dependencies", "dependencies must be a list")
    try:
        return [
            DependencySpec.model_validate(item).model_dump(exclude_none=True)
            for item in dependencies
        ]
    except ValidationError as exc:
        _validation_error("invalid_dependency", "Invalid dependency specification", exc)


def _normalize_artifact_specs(artifacts):
    if artifacts is None:
        return []
    if not isinstance(artifacts, list):
        raise HubError("invalid_artifacts", "artifacts must be a list")
    try:
        return [
            ArtifactSpec.model_validate(item).model_dump(exclude_none=True)
            for item in artifacts
        ]
    except ValidationError as exc:
        _validation_error("invalid_artifact", "Invalid artifact specification", exc)


def _validate_event_input(event_type, payload):
    if not isinstance(event_type, str) or not EVENT_TYPE_PATTERN.fullmatch(event_type):
        raise HubError(
            "invalid_event_type",
            "event_type must match ^[a-z][a-z0-9_.-]{0,127}$",
        )
    try:
        encoded = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise HubError("invalid_event_payload", f"payload must be JSON serializable: {exc}") from None
    if len(encoded) > MAX_EVENT_PAYLOAD_BYTES:
        raise HubError(
            "event_payload_too_large",
            f"event payload exceeds {MAX_EVENT_PAYLOAD_BYTES} bytes",
        )


def _assert_task_control(conn, task, actor_agent_id, coordinator_token=None,
                         coordinator_session_id=None):
    if task.created_by_agent_id == actor_agent_id:
        return
    if (task.coordinator_agent_id == actor_agent_id
            and coordinator_token is not None
            and coordinator_session_id is not None
            and task.coordinator_fencing_token == coordinator_token
            and task.coordinator_session_id == coordinator_session_id
            and task.coordinator_lease_expires_at
            and task.coordinator_lease_expires_at >= now_iso()):
        return
    raise HubError("task_control_denied", "Only the task owner or active coordinator may do this")


def _ensure_agent(conn, agent_id, capabilities=None):
    agent = storage.get_agent(conn, agent_id)
    if not agent:
        return storage.upsert_agent(
            conn, agent_id, agent_id, json.dumps(capabilities or []), "")
    storage.update_heartbeat(conn, agent_id)
    return agent


def _assert_run_execution(run, actor_agent_id, fencing_token, session_id=None):
    if run.fencing_token != fencing_token:
        raise HubError("stale_token", "Run fencing token is stale")
    if run.agent_id != actor_agent_id:
        raise HubError("run_not_owned", f"Run belongs to {run.agent_id}, not {actor_agent_id}")
    if session_id is not None and run.session_id != session_id:
        raise HubError("run_session_mismatch", "Run is bound to another session")


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
        _ensure_agent(c, actor_agent_id, capabilities)
        return storage.create_session(
            c, session_id, actor_agent_id, native_session_ref, adapter_id,
            caps_json, iso_plus_seconds(lease),
        )

    sess = write_executor.execute_write(conn, _write)
    result = {"session_id": sess.id, "agent_id": actor_agent_id,
              "lease_expires_at": sess.lease_expires_at}
    result["bootstrap"] = agent_sync(actor_agent_id, sess.id, since_event_id=0)
    return result


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
    if not isinstance(objective, str) or not objective.strip():
        raise HubError("invalid_objective", "objective must be a non-empty string")
    if len(objective) > 20_000:
        raise HubError("invalid_objective", "objective exceeds 20000 characters")
    conn = get_db()
    task_id = new_id()

    def _write(c):
        _ensure_agent(c, actor_agent_id)
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


def get_task(task_id, actor_agent_id=None):
    conn = get_db()
    task = storage.get_task(conn, task_id)
    if task and actor_agent_id and not storage.is_task_participant(conn, task_id, actor_agent_id):
        raise HubError("task_access_denied", f"Agent {actor_agent_id} is not a task participant")
    return _task_dict(task) if task else None


def list_tasks(status=None, limit=50, actor_agent_id=None):
    conn = get_db()
    tasks = (storage.list_tasks_for_agent(conn, actor_agent_id, status=status, limit=limit)
             if actor_agent_id else storage.list_tasks(conn, status=status, limit=limit))
    return [_task_dict(t) for t in tasks]


def plan_task(task_id, work_items, dependencies=None, actor_agent_id=None,
              coordinator_token=None, coordinator_session_id=None):
    """Create work items + dependencies. Validates DAG (cycle detection, same-task)."""
    work_items = _normalize_work_item_specs(work_items)
    dependencies = _normalize_dependency_specs(dependencies)
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        if task.status not in ("draft", "planned"):
            raise HubError("invalid_state",
                f"Cannot plan task in status '{task.status}'")
        _assert_task_control(c, task, actor_agent_id, coordinator_token,
                             coordinator_session_id)
        if storage.count_work_items(c, task_id) + len(work_items) > MAX_WORK_ITEMS_PER_TASK:
            raise HubError("work_budget_exceeded", "Task work-item budget exceeded")

        created_ids = {}
        for wi_spec in work_items:
            if wi_spec.get("kind") not in VALID_WORK_KINDS:
                raise HubError("invalid_work_kind", f"Unsupported Work Item kind: {wi_spec.get('kind')}")
            wi = storage.create_work_item(c, task_id, wi_spec["kind"],
                wi_spec["objective"],
                parent_id=wi_spec.get("parent_id"),
                acceptance_json=json.dumps(wi_spec.get("acceptance", [])),
                required_capabilities_json=json.dumps(wi_spec.get("required_capabilities", [])),
                preferred_agent_id=wi_spec.get("preferred_agent_id"),
                priority=wi_spec.get("priority", 0),
                retry_policy_json=wi_spec.get("retry_policy_json", '{"max_attempts":3}'),
                needs_review=wi_spec.get("needs_review", False),
                depth=wi_spec.get("depth", 0),
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


def start_task(task_id, actor_agent_id, coordinator_token=None,
               coordinator_session_id=None):
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        if task.status not in ("planned", "ready"):
            raise HubError("invalid_state",
                f"Cannot start task in status '{task.status}'")
        _assert_task_control(c, task, actor_agent_id, coordinator_token,
                             coordinator_session_id)
        storage.update_task_status(c, task_id, "running")
        _emit(c, task_id, "task.started", actor_agent_id=actor_agent_id,
              deliver_to=[("agent", task.created_by_agent_id)])
        return storage.get_task(c, task_id)

    task = write_executor.execute_write(conn, _write)
    return _task_dict(task)


def task_snapshot(task_id, actor_agent_id=None, after_event_id=0):
    """Return the durable recovery package for a task."""
    conn = get_db()
    task = storage.get_task(conn, task_id)
    if not task:
        raise HubError("task_not_found", f"Task {task_id} not found")
    if actor_agent_id and not storage.is_task_participant(conn, task_id, actor_agent_id):
        raise HubError("task_access_denied", f"Agent {actor_agent_id} is not a task participant")
    items = storage.list_work_items(conn, task_id)
    return {
        "task": _task_dict(task, include_spec=True),
        "participants": storage.list_task_participants(conn, task_id),
        "work_items": [_work_item_dict(wi, include_spec=True) for wi in items],
        "dependencies": [d.model_dump() for d in storage.get_all_dependencies_for_task(conn, task_id)],
        "runs": [_run_dict(run) for wi in items for run in storage.list_runs_for_work_item(conn, wi.id)],
        "artifacts": [a.model_dump() for a in storage.list_artifacts(conn, task_id=task_id)],
        "events": [e.model_dump() for e in storage.list_events(conn, task_id, after_event_id)],
    }


def task_timeline(task_id, actor_agent_id, after_event_id=0, limit=200):
    conn = get_db()
    task = storage.get_task(conn, task_id)
    if not task:
        raise HubError("task_not_found", f"Task {task_id} not found")
    if not storage.is_task_participant(conn, task_id, actor_agent_id):
        raise HubError("task_access_denied", "Only task participants may read the timeline")
    return [e.model_dump() for e in storage.list_events(conn, task_id, after_event_id, limit)]


def task_explain(task_id, actor_agent_id):
    snapshot = task_snapshot(task_id, actor_agent_id)
    task = snapshot["task"]
    active = [w for w in snapshot["work_items"] if w["status"] in
              ("ready", "offered", "running", "reviewing", "blocked")]
    blockers = [w for w in active if w["status"] == "blocked"]
    pending = [w for w in snapshot["work_items"] if w["status"] == "pending"]
    return {
        "task_id": task_id,
        "status": task["status"],
        "coordinator": task.get("coordinator_agent_id"),
        "active_work": active,
        "blocked_work": blockers,
        "pending_dependency_count": len(pending),
        "next_action": (
            "operator_decision" if blockers else
            "agent_execution" if any(w["status"] in ("ready", "offered", "running") for w in active) else
            "review" if any(w["status"] == "reviewing" for w in active) else
            "reconcile_or_replan"
        ),
    }


def add_participant(task_id, target_agent_id, role, actor_agent_id,
                    coordinator_token=None, coordinator_session_id=None):
    if role not in VALID_PARTICIPANT_ROLES:
        raise HubError("invalid_participant_role", f"Unsupported participant role: {role}")
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        _assert_task_control(c, task, actor_agent_id, coordinator_token,
                             coordinator_session_id)
        if not storage.get_agent(c, target_agent_id):
            raise HubError("agent_not_found", f"Agent {target_agent_id} is not registered")
        storage.add_task_participant(c, task_id, target_agent_id, role)
        _emit(c, task_id, "participant.added", actor_agent_id=actor_agent_id,
              payload={"agent_id": target_agent_id, "role": role},
              deliver_to=[("agent", target_agent_id)])

    write_executor.execute_write(conn, _write)
    return {"task_id": task_id, "agent_id": target_agent_id, "role": role}


def cancel_task(task_id, actor_agent_id, reason="", coordinator_token=None,
                coordinator_session_id=None):
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        _assert_task_control(c, task, actor_agent_id, coordinator_token,
                             coordinator_session_id)
        if task.status in ("completed", "failed", "cancelled", "archived"):
            raise HubError("invalid_state", f"Task is already terminal: {task.status}")
        cancelled_runs = storage.cancel_active_runs_for_task(c, task_id)
        for run_id in cancelled_runs:
            storage.release_locks_by_run(c, run_id)
        cancelled_work = storage.cancel_work_items_for_task(c, task_id)
        storage.update_task_status(c, task_id, "cancelled", completed_at=now_iso(),
                                   blocked_reason_json=json.dumps({"reason": reason}))
        _emit(c, task_id, "task.cancelled", actor_agent_id=actor_agent_id,
              payload={"reason": reason, "cancelled_runs": len(cancelled_runs),
                       "cancelled_work_items": cancelled_work},
              deliver_to=[("agent", p["agent_id"])
                          for p in storage.list_task_participants(c, task_id)])
        return storage.get_task(c, task_id)

    return _task_dict(write_executor.execute_write(conn, _write))


# ════════════════════════════════════════════════════════════════════
#  Coordinator lease
# ════════════════════════════════════════════════════════════════════

def coordinator_claim(task_id, actor_agent_id, session_id, lease_seconds=None):
    conn = get_db()
    lease = lease_seconds or DEFAULT_COORDINATOR_LEASE

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active" or sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned", "An active owned session is required")
        caps = set(_json_obj(sess.capabilities_json, []))
        if actor_agent_id != task.created_by_agent_id and "coordinate" not in caps:
            raise HubError("capability_missing", "Coordinator requires the 'coordinate' capability")
        token = storage.claim_coordinator(
            c, task_id, actor_agent_id, session_id, iso_plus_seconds(lease))
        if token is None:
            raise HubError("coordinator_busy", "Task already has an active coordinator")
        _emit(c, task_id, "coordinator.claimed", actor_agent_id=actor_agent_id,
              payload={"session_id": session_id, "fencing_token": token},
              deliver_to=[("agent", task.created_by_agent_id)])
        return token

    token = write_executor.execute_write(conn, _write)
    return {"task_id": task_id, "agent_id": actor_agent_id,
            "fencing_token": token, "lease_expires_at": iso_plus_seconds(lease)}


def coordinator_heartbeat(task_id, actor_agent_id, fencing_token, lease_seconds=None):
    conn = get_db()
    lease = lease_seconds or DEFAULT_COORDINATOR_LEASE

    def _write(c):
        if not storage.heartbeat_coordinator(
                c, task_id, actor_agent_id, fencing_token, iso_plus_seconds(lease)):
            raise HubError("stale_coordinator", "Coordinator lease or fencing token is stale")

    write_executor.execute_write(conn, _write)
    return {"task_id": task_id, "lease_expires_at": iso_plus_seconds(lease)}


def coordinator_release(task_id, actor_agent_id, fencing_token):
    conn = get_db()

    def _write(c):
        if not storage.release_coordinator(c, task_id, actor_agent_id, fencing_token):
            raise HubError("stale_coordinator", "Coordinator lease or fencing token is stale")
        task = storage.get_task(c, task_id)
        _emit(c, task_id, "coordinator.released", actor_agent_id=actor_agent_id,
              deliver_to=[("agent", task.created_by_agent_id)] if task else [])

    write_executor.execute_write(conn, _write)
    return {"task_id": task_id, "status": "released"}


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

        budget = json.loads(task.budget_json) if task.budget_json else {}
        max_runs = budget.get("max_runs", MAX_RUNS_PER_TASK)
        current_runs = storage.count_runs_for_task(c, wi.task_id)
        if current_runs >= max_runs:
            raise HubError("budget_exceeded",
                f"Task has {current_runs} runs, budget max is {max_runs}")

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
        storage.claim_run(c, run.id, run.fencing_token, session_id, iso_plus_seconds(lease))
        run = storage.get_run(c, run.id)
        storage.add_task_participant(c, wi.task_id, actor_agent_id, "worker")
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


def accept_offer(run_id, session_id, actor_agent_id, lease_seconds=None):
    """Claim a scheduler-created offer assigned to this agent."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run or run.status != "offered":
            raise HubError("offer_not_found", f"Run {run_id} is not an active offer")
        if run.agent_id != actor_agent_id:
            raise HubError("run_not_owned", "Offer belongs to another agent")
        sess = storage.get_session(c, session_id)
        if not sess or sess.status != "active" or sess.agent_id != actor_agent_id:
            raise HubError("session_not_owned", "An active owned session is required")
        if not storage.claim_run(c, run_id, run.fencing_token, session_id, iso_plus_seconds(lease)):
            raise HubError("offer_raced", "Offer was already claimed or expired")
        wi = storage.get_work_item(c, run.work_item_id)
        storage.add_task_participant(c, wi.task_id, actor_agent_id, "worker")
        _emit(c, wi.task_id, "work.claimed", actor_agent_id=actor_agent_id,
              work_item_id=wi.id, run_id=run_id)
        return storage.get_run(c, run_id), wi

    run, wi = write_executor.execute_write(conn, _write)
    return {"run_id": run.id, "fencing_token": run.fencing_token,
            "work_item_id": wi.id, "task_id": wi.task_id,
            "attempt_no": run.attempt_no, "lease_expires_at": run.lease_expires_at,
            "objective": wi.objective, "kind": wi.kind}


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
        if run.session_id is not None and run.session_id != session_id:
            raise HubError("run_session_mismatch", "Run is already bound to another session")

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


def heartbeat_run(run_id, fencing_token, actor_agent_id, lease_seconds=None,
                  session_id=None):
    """Heartbeat a running run. Verifies ownership."""
    conn = get_db()
    lease = lease_seconds or DEFAULT_RUN_LEASE

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        _assert_run_execution(run, actor_agent_id, fencing_token, session_id)
        ok = storage.heartbeat_run(c, run_id, fencing_token, iso_plus_seconds(lease))
        if not ok:
            raise HubError("invalid_state",
                f"Cannot heartbeat run {run_id}: not running")
        return None

    write_executor.execute_write(conn, _write)
    return {"run_id": run_id, "lease_expires_at": iso_plus_seconds(lease)}


def save_checkpoint(run_id, fencing_token, snapshot, actor_agent_id,
                    session_id=None):
    """Save checkpoint. Verifies ownership."""
    conn = get_db()

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        _assert_run_execution(run, actor_agent_id, fencing_token, session_id)
        if run.status != "running":
            raise HubError("invalid_state",
                f"Cannot checkpoint run in status '{run.status}'")
        cp = storage.save_checkpoint(c, run_id, json.dumps(snapshot))
        return cp

    cp = write_executor.execute_write(conn, _write)
    return {"checkpoint_id": cp.id, "version": cp.version, "run_id": run_id}


def complete_run(run_id, fencing_token, status, actor_agent_id,
                 artifacts=None, failure_code=None, failure_detail=None,
                 session_id=None):
    """Complete a run. Enforces status whitelist + ownership."""
    artifacts = _normalize_artifact_specs(artifacts)
    conn = get_db()

    if status not in VALID_RUN_COMPLETION_STATUS:
        raise HubError("invalid_status",
            f"status must be one of {VALID_RUN_COMPLETION_STATUS}, got '{status}'")

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run:
            raise HubError("run_not_found", f"Run {run_id} not found")
        _assert_run_execution(run, actor_agent_id, fencing_token, session_id)
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
        released_locks = storage.release_locks_by_run(c, run_id)
        if released_locks > 0:
            _emit(c, wi.task_id, "run.locks_released",
                  work_item_id=wi.id, run_id=run_id,
                  payload={"count": released_locks})
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

        budget = _json_obj(task.budget_json, {})
        max_runs = int(budget.get("max_runs", MAX_RUNS_PER_TASK))
        if storage.count_runs_for_task(c, wi.task_id) >= max_runs:
            raise HubError("budget_exceeded", "Task Run budget is exhausted")

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


def spawn_child_work(run_id, fencing_token, actor_agent_id, work_items,
                     dependencies=None, session_id=None):
    """Dynamically extend a running task from an active Run."""
    work_items = _normalize_work_item_specs(work_items)
    dependencies = _normalize_dependency_specs(dependencies)
    conn = get_db()

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run or run.status != "running":
            raise HubError("run_not_active", "An active run is required")
        _assert_run_execution(run, actor_agent_id, fencing_token, session_id)
        parent = storage.get_work_item(c, run.work_item_id)
        task = storage.get_task(c, parent.task_id)
        budget = _json_obj(task.budget_json, {})
        max_items = int(budget.get("max_work_items", MAX_WORK_ITEMS_PER_TASK))
        max_depth = int(budget.get("max_depth", MAX_WORK_DEPTH))
        if storage.count_work_items(c, task.id) + len(work_items) > max_items:
            raise HubError("work_budget_exceeded", "Task work-item budget exceeded")
        child_depth = parent.depth + 1
        if child_depth > max_depth:
            raise HubError("work_depth_exceeded", f"Child depth {child_depth} exceeds {max_depth}")

        created = {}
        for spec in work_items:
            if spec.get("kind") not in VALID_WORK_KINDS:
                raise HubError("invalid_work_kind", f"Unsupported Work Item kind: {spec.get('kind')}")
            wi = storage.create_work_item(
                c, task.id, spec["kind"], spec["objective"],
                parent_id=parent.id, depth=child_depth,
                acceptance_json=json.dumps(spec.get("acceptance", [])),
                required_capabilities_json=json.dumps(spec.get("required_capabilities", [])),
                preferred_agent_id=spec.get("preferred_agent_id"),
                priority=spec.get("priority", parent.priority),
                retry_policy_json=spec.get(
                    "retry_policy_json",
                    json.dumps({"max_attempts": MAX_RUN_ATTEMPTS}),
                ),
                needs_review=spec.get("needs_review", False))
            created[spec.get("ref", wi.id)] = wi.id
        for dep in dependencies or []:
            wi_id = created.get(dep["work_item"], dep["work_item"])
            dep_id = created.get(dep["depends_on"], dep["depends_on"])
            wi, dep_wi = storage.get_work_item(c, wi_id), storage.get_work_item(c, dep_id)
            if not wi or not dep_wi or wi.task_id != task.id or dep_wi.task_id != task.id:
                raise HubError("dep_cross_task", "Dynamic dependencies must stay within the task")
            _check_dag_no_cycle(c, wi_id, dep_id)
            storage.add_dependency(c, wi_id, dep_id, dep.get("condition", "succeeded"))
        _advance_ready_work_items(c, task.id)
        _emit(c, task.id, "work.children_spawned", actor_agent_id=actor_agent_id,
              work_item_id=parent.id, run_id=run_id,
              payload={"work_item_ids": list(created.values())},
              deliver_to=[("agent", task.created_by_agent_id)])
        return list(created.values())

    ids = write_executor.execute_write(conn, _write)
    return {"run_id": run_id, "work_item_ids": ids}


def block_work(run_id, fencing_token, actor_agent_id, blocker, checkpoint=None,
               session_id=None):
    conn = get_db()

    def _write(c):
        run = storage.get_run(c, run_id)
        if not run or run.status != "running":
            raise HubError("run_not_active", "An active running Run is required")
        _assert_run_execution(run, actor_agent_id, fencing_token, session_id)
        wi = storage.get_work_item(c, run.work_item_id)
        if checkpoint is not None:
            storage.save_checkpoint(c, run_id, json.dumps(checkpoint))
        reason_json = json.dumps(blocker or {})
        storage.block_run(c, run_id, fencing_token, failure_json=reason_json)
        storage.update_work_item_status(c, wi.id, "blocked", version=wi.version,
                                        blocked_reason_json=reason_json)
        storage.release_locks_by_run(c, run_id)
        task = storage.get_task(c, wi.task_id)
        _emit(c, wi.task_id, "work.blocked", actor_agent_id=actor_agent_id,
              work_item_id=wi.id, run_id=run_id, payload=blocker,
              deliver_to=[("agent", task.created_by_agent_id)] if task else [])
        return storage.get_work_item(c, wi.id)

    wi = write_executor.execute_write(conn, _write)
    return {"work_item_id": wi.id, "status": wi.status}


def unblock_work(work_item_id, actor_agent_id, coordinator_token=None, note="",
                 coordinator_session_id=None):
    conn = get_db()

    def _write(c):
        wi = storage.get_work_item(c, work_item_id)
        if not wi or wi.status != "blocked":
            raise HubError("work_not_blocked", "Work Item is not blocked")
        task = storage.get_task(c, wi.task_id)
        _assert_task_control(c, task, actor_agent_id, coordinator_token,
                             coordinator_session_id)
        storage.update_work_item_status(c, wi.id, "ready", version=wi.version,
                                        blocked_reason_json="{}")
        _emit(c, wi.task_id, "work.unblocked", actor_agent_id=actor_agent_id,
              work_item_id=wi.id, payload={"note": note})
        return storage.get_work_item(c, wi.id)

    wi = write_executor.execute_write(conn, _write)
    return {"work_item_id": wi.id, "status": wi.status}


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
        if not storage.is_task_participant(c, wi.task_id, reviewer_agent_id):
            raise HubError("task_access_denied", "Reviewer must be an assigned task participant")
        policy = _json_obj(task.authorization_policy_json, {}) if task else {}
        last_run = storage.get_latest_successful_run(c, work_item_id)
        if (last_run and last_run.agent_id == reviewer_agent_id
                and not policy.get("allow_self_review", False)):
            raise HubError("self_review_denied", "Reviewer must be independent from the producing agent")
        storage.add_task_participant(c, wi.task_id, reviewer_agent_id, "reviewer")

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
        wi = storage.get_work_item(c, work_item_id) if work_item_id else None
        run = storage.get_run(c, run_id) if run_id else None
        if wi and wi.task_id != task_id:
            raise HubError("approval_scope_mismatch", "Work Item belongs to another task")
        if run and (not wi or run.work_item_id != wi.id):
            raise HubError("approval_scope_mismatch", "Run does not belong to the approval Work Item")
        if run and requested_by and run.agent_id != requested_by:
            raise HubError("run_not_owned", "Only the running agent may request this approval")

        previous_task_status = task.status
        previous_work_status = wi.status if wi else None
        if run and run.status == "running":
            storage.block_run(c, run.id, run.fencing_token,
                              failure_code="approval_required",
                              failure_json=json.dumps({"action": action, "reason": reason}))
            storage.release_locks_by_run(c, run.id)
        if wi and wi.status not in ("succeeded", "failed", "cancelled"):
            storage.update_work_item_status(
                c, wi.id, "blocked", version=wi.version,
                blocked_reason_json=json.dumps({"type": "policy_gate", "action": action,
                                                "reason": reason}))
        elif not wi and task.status not in ("completed", "failed", "cancelled", "archived"):
            storage.update_task_status(
                c, task_id, "blocked",
                blocked_reason_json=json.dumps({"type": "policy_gate", "action": action,
                                                "reason": reason}))

        ap = storage.create_approval(
            c, task_id, action, reason, work_item_id, run_id,
            previous_task_status=previous_task_status,
            previous_work_status=previous_work_status,
            expires_at=iso_plus_seconds(DEFAULT_APPROVAL_TTL),
        )
        _emit(c, task_id, "approval.requested",
              actor_agent_id=requested_by, work_item_id=work_item_id, run_id=run_id,
              payload={"action": action, "approval_id": ap.id},
              deliver_to=[("agent", task.created_by_agent_id)])
        return ap

    ap = write_executor.execute_write(conn, _write)
    return {
        "approval_id": ap.id,
        "task_id": task_id,
        "action": action,
        "status": "pending",
        "expires_at": ap.expires_at,
    }


def decide_approval(approval_id, decision, decided_by, is_operator=False):
    conn = get_db()
    if decision not in VALID_APPROVAL_DECISIONS:
        raise HubError("invalid_decision", f"Decision must be one of {VALID_APPROVAL_DECISIONS}")
    if not is_operator:
        operator_agents = set(get_config("operator_agent_ids", []))
        if decided_by not in operator_agents:
            raise HubError("operator_required", "Approval decisions require an operator identity")

    def _write(c):
        approval = storage.get_approval(c, approval_id)
        if not approval or approval.decision is not None:
            raise HubError("approval_not_found",
                f"Approval {approval_id} not found or already decided")
        ok = storage.decide_approval(c, approval_id, decision, decided_by)
        if not ok:
            raise HubError("approval_not_found",
                f"Approval {approval_id} not found or already decided")
        task = storage.get_task(c, approval.task_id)
        wi = storage.get_work_item(c, approval.work_item_id) if approval.work_item_id else None
        if decision == "approved":
            if wi and wi.status == "blocked":
                storage.update_work_item_status(c, wi.id, "ready", version=wi.version,
                                                blocked_reason_json="{}")
            elif task and task.status == "blocked":
                resume_status = approval.previous_task_status or "running"
                storage.update_task_status(c, task.id, resume_status, blocked_reason_json="{}")
        else:
            if wi and wi.status == "blocked":
                storage.update_work_item_status(c, wi.id, "failed", version=wi.version,
                                                blocked_reason_json=json.dumps({
                                                    "type": "approval_rejected",
                                                    "action": approval.action}))
                _check_task_completion(c, wi.task_id)
            elif task and task.status == "blocked":
                storage.cancel_active_runs_for_task(c, task.id)
                storage.cancel_work_items_for_task(c, task.id)
                storage.update_task_status(c, task.id, "cancelled", completed_at=now_iso())
        _emit(c, approval.task_id, f"approval.{decision}",
              actor_agent_id=(decided_by if storage.get_agent(c, decided_by) else None),
              work_item_id=approval.work_item_id,
              run_id=approval.run_id,
              payload={"approval_id": approval_id, "action": approval.action,
                       "decided_by": decided_by},
              deliver_to=[("agent", p["agent_id"])
                          for p in storage.list_task_participants(c, approval.task_id)])
        return storage.get_approval(c, approval_id)

    result = write_executor.execute_write(conn, _write)
    return {"approval_id": approval_id, "decision": decision,
            "task_id": result.task_id}


# ════════════════════════════════════════════════════════════════════
#  Agent adapters and task-scoped events
# ════════════════════════════════════════════════════════════════════

def register_adapter(actor_agent_id, mode, config=None, wake_level="L2", adapter_id=None):
    if mode not in VALID_ADAPTER_MODES:
        raise HubError("invalid_adapter_mode", f"Unsupported adapter mode: {mode}")
    if wake_level not in ("L2", "L3"):
        raise HubError("invalid_wake_level", "wake_level must be L2 or L3")
    conn = get_db()
    adapter_id = adapter_id or f"{actor_agent_id}-{mode}"

    def _write(c):
        _ensure_agent(c, actor_agent_id)
        return storage.upsert_adapter(
            c, adapter_id, actor_agent_id, mode, json.dumps(config or {}), wake_level)

    adapter = write_executor.execute_write(conn, _write)
    return adapter.model_dump()


def adapter_poll(actor_agent_id, adapter_id, limit=20, lease_seconds=None):
    conn = get_db()
    lease = lease_seconds or DEFAULT_OUTBOX_LEASE

    def _write(c):
        adapter = storage.get_adapter(c, adapter_id)
        if not adapter or adapter.agent_id != actor_agent_id:
            raise HubError("adapter_not_owned", f"Adapter {adapter_id} is not owned by this agent")
        return storage.lease_outbox_for_adapter(c, adapter_id, lease, limit)

    entries = write_executor.execute_write(conn, _write)
    return {"adapter_id": adapter_id, "entries": entries,
            "lease_seconds": lease}


def adapter_ack(actor_agent_id, adapter_id, outbox_id, success=True, error=""):
    conn = get_db()

    def _write(c):
        adapter = storage.get_adapter(c, adapter_id)
        item = storage.get_outbox(c, outbox_id)
        if not adapter or adapter.agent_id != actor_agent_id:
            raise HubError("adapter_not_owned", f"Adapter {adapter_id} is not owned by this agent")
        if not item or item.get("adapter_id") != adapter_id or item["status"] != "leased":
            raise HubError("outbox_not_leased", "Outbox entry is not leased to this adapter")
        if success:
            storage.mark_outbox_delivered(c, outbox_id, resolution="adapter_ack")
            storage.update_adapter_health(c, adapter_id, True)
        else:
            attempts = int(item["attempts"]) + 1
            if attempts >= int(item["max_attempts"]):
                storage.dead_letter_outbox(c, outbox_id, error or "adapter_failed")
                storage.update_adapter_health(c, adapter_id, False, error or "adapter_failed")
            else:
                delay = min(300, 2 ** attempts)
                storage.retry_outbox(c, outbox_id, error or "adapter_failed", delay)
        return storage.get_outbox(c, outbox_id)

    item = write_executor.execute_write(conn, _write)
    return {"outbox_id": outbox_id, "status": item["status"]}


def post_event(task_id, actor_agent_id, event_type, payload=None,
               work_item_id=None, run_id=None, recipients=None,
               idempotency_key=None):
    """Post a typed task-scoped coordination event."""
    _validate_event_input(event_type, payload)
    if recipients is not None and not isinstance(recipients, list):
        raise HubError("invalid_recipients", "recipients must be a list")
    conn = get_db()

    def _write(c):
        task = storage.get_task(c, task_id)
        if not task:
            raise HubError("task_not_found", f"Task {task_id} not found")
        if not storage.is_task_participant(c, task_id, actor_agent_id):
            raise HubError("task_access_denied", "Only task participants may post events")
        if work_item_id:
            wi = storage.get_work_item(c, work_item_id)
            if not wi or wi.task_id != task_id:
                raise HubError("event_scope_mismatch", "Work Item belongs to another task")
        deliver_to = []
        for agent_id in recipients or []:
            if not storage.is_task_participant(c, task_id, agent_id):
                raise HubError("recipient_not_participant", f"{agent_id} is not a task participant")
            deliver_to.append(("agent", agent_id))
        event = storage.append_event(
            c, task_id, event_type, actor_agent_id=actor_agent_id,
            work_item_id=work_item_id, run_id=run_id,
            payload_json=json.dumps(payload or {}), idempotency_key=idempotency_key)
        if event:
            for kind, recipient_id in deliver_to:
                storage.create_delivery(c, event.event_id, kind, recipient_id)
        return event

    event = write_executor.execute_write(conn, _write)
    return {"event_id": event.event_id if event else None,
            "duplicate": event is None}


# ════════════════════════════════════════════════════════════════════
#  Agent sync (P0 fix: cursor-based delivery)
# ════════════════════════════════════════════════════════════════════

def agent_sync(actor_agent_id, session_id, since_event_id=0):
    """Batch pull: heartbeat + ready work + cursor-based deliveries + runs."""
    if not isinstance(since_event_id, int) or isinstance(since_event_id, bool) or since_event_id < 0:
        raise HubError("invalid_cursor", "since_event_id must be a non-negative integer")
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
        stored_cursor = storage.get_consumer_cursor(c, "agent", actor_agent_id)
        effective_cursor = max(since_event_id, stored_cursor)
        deliveries = storage.list_deliveries_since(
            c, "agent", actor_agent_id, since_event_id=effective_cursor, limit=50)
        storage.mark_deliveries_observed(
            c, [delivery["delivery_id"] for delivery in deliveries]
        )
        observed_event_id = max(
            [effective_cursor] + [int(delivery["event_id"]) for delivery in deliveries]
        )
        storage.advance_consumer_cursor(
            c, "agent", actor_agent_id, observed_event_id
        )
        backlog = c.execute(
            """SELECT COUNT(*) AS pending,
                      SUM(CASE WHEN observed_at IS NULL THEN 1 ELSE 0 END) AS unobserved,
                      MIN(available_at) AS oldest_pending
               FROM deliveries
               WHERE recipient_kind='agent' AND recipient_id=? AND status='pending'""",
            (actor_agent_id,),
        ).fetchone()
        pending_approvals = storage.list_pending_approvals_for_agent(c, actor_agent_id)
        task_summaries = storage.list_tasks_for_agent(c, actor_agent_id, limit=20)

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
            "tasks": [_task_dict(task) for task in task_summaries],
            "latest_event_id": latest_event_id,
            "delivery_state": {
                "observation_cursor": observed_event_id,
                "pending": backlog["pending"] if backlog else 0,
                "unobserved": (backlog["unobserved"] or 0) if backlog else 0,
                "oldest_pending": backlog["oldest_pending"] if backlog else None,
                "ack_required": True,
            },
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


def ack_deliveries(actor_agent_id, delivery_ids):
    """Idempotently acknowledge a bounded delivery batch for one agent."""
    if not isinstance(delivery_ids, list) or not delivery_ids:
        raise HubError("invalid_delivery_ids", "delivery_ids must be a non-empty list")
    if len(delivery_ids) > MAX_DELIVERY_ACK_BATCH:
        raise HubError(
            "delivery_batch_too_large",
            f"at most {MAX_DELIVERY_ACK_BATCH} deliveries may be acknowledged at once",
        )
    if any(not isinstance(item, str) or not item for item in delivery_ids):
        raise HubError("invalid_delivery_ids", "every delivery_id must be a non-empty string")
    conn = get_db()

    def _write(c):
        result = storage.ack_deliveries(c, delivery_ids, actor_agent_id)
        if result is None:
            raise HubError(
                "delivery_not_found",
                "one or more deliveries do not exist or belong to another agent",
            )
        return result

    acked, already_final = write_executor.execute_write(conn, _write)
    return {
        "requested": len(set(delivery_ids)),
        "acked": acked,
        "already_final": already_final,
        "status": "acked",
    }


# ════════════════════════════════════════════════════════════════════
#  Resource locks
# ════════════════════════════════════════════════════════════════════

def acquire_lock(lock_key, holder_run_id, resource_type="general",
                 resource_id="", ttl_seconds=600, actor_agent_id=None,
                 run_fencing_token=None):
    conn = get_db()

    def _write(c):
        if actor_agent_id:
            run = storage.get_run(c, holder_run_id)
            if (not run or run.agent_id != actor_agent_id or run.status != "running"
                    or run.fencing_token != run_fencing_token):
                raise HubError("run_not_owned", "An active owned Run is required for locking")
        return storage.acquire_lock(c, lock_key, holder_run_id,
            resource_type, resource_id, ttl_seconds)

    success, token = write_executor.execute_write(conn, _write)
    if not success:
        raise HubError("lock_busy",
            f"Lock '{lock_key}' held by another run (token={token})")
    return {"lock_key": lock_key, "fencing_token": token, "holder_run_id": holder_run_id}


def release_lock(lock_key, holder_run_id, fencing_token, actor_agent_id=None):
    conn = get_db()

    def _write(c):
        if actor_agent_id:
            run = storage.get_run(c, holder_run_id)
            if not run or run.agent_id != actor_agent_id:
                raise HubError("run_not_owned", "Run belongs to another agent")
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
    """Expire leases, enforce deadlines, dispatch ready work and process outbox."""
    conn = get_db()

    def _write(c):
        expired_sessions = storage.expire_stale_sessions(c)
        lost_run_ids = storage.expire_stale_runs(c)
        expired_locks = storage.expire_stale_locks(c)
        expired_coordinators = storage.expire_stale_coordinators(c)
        requeued_outbox = storage.requeue_expired_outbox_leases(c)
        delivery_cutoff = (
            datetime.now(timezone.utc)
            - timedelta(days=DELIVERY_TERMINAL_RETENTION_DAYS)
        ).isoformat()
        archived_deliveries = storage.archive_terminal_task_deliveries(
            c, delivery_cutoff
        )

        for run_id in lost_run_ids:
            run = storage.get_run(c, run_id)
            if run:
                wi = storage.get_work_item(c, run.work_item_id)
                if wi:
                    storage.release_locks_by_run(c, run_id)
                    task = storage.get_task(c, wi.task_id)
                    retry_policy = _json_obj(wi.retry_policy_json, {})
                    max_attempts = int(retry_policy.get("max_attempts", MAX_RUN_ATTEMPTS))
                    next_status = "ready" if run.attempt_no < max_attempts else "failed"
                    storage.update_work_item_status(c, wi.id, next_status)
                    _emit(c, wi.task_id,
                          "run.lost" if next_status == "ready" else "work.failed",
                          work_item_id=wi.id, run_id=run_id,
                          payload={"reason": "lease_expired", "attempt": run.attempt_no},
                          deliver_to=[("agent", task.created_by_agent_id)] if task else [],
                          outbox_entries=([("adapter.dispatch", {
                              "work_item_id": wi.id, "objective": wi.objective,
                              "reason": "run_lost",
                          })] if next_status == "ready" else None))
                    if next_status == "failed":
                        _check_task_completion(c, wi.task_id)

        deadline_tasks = c.execute(
            """SELECT id FROM tasks WHERE status IN ('planned','ready','running','blocked')
               AND deadline_at IS NOT NULL AND deadline_at < ?""", (now_iso(),)).fetchall()
        for row in deadline_tasks:
            task = storage.get_task(c, row["id"])
            cancelled_runs = storage.cancel_active_runs_for_task(c, task.id)
            storage.cancel_work_items_for_task(c, task.id)
            storage.update_task_status(c, task.id, "failed", completed_at=now_iso(),
                                       blocked_reason_json=json.dumps({"reason": "deadline_exceeded"}))
            _emit(c, task.id, "task.deadline_exceeded",
                  deliver_to=[("agent", p["agent_id"])
                              for p in storage.list_task_participants(c, task.id)],
                  payload={"cancelled_runs": len(cancelled_runs)})

        tasks = storage.list_tasks(c, status="running")
        for task in tasks:
            _advance_ready_work_items(c, task.id)
            _check_task_completion(c, task.id)

        dispatched = _dispatch_ready_work(c)

        for expired in expired_coordinators:
            task = storage.get_task(c, expired["id"])
            if task:
                _emit(c, task.id, "coordinator.lost",
                      payload={"agent_id": expired["coordinator_agent_id"]},
                      deliver_to=[("agent", task.created_by_agent_id)])

        expired_approvals = storage.list_expired_pending_approvals(c)
        for approval in expired_approvals:
            task = storage.get_task(c, approval.task_id)
            if task:
                _emit(
                    c,
                    task.id,
                    "approval.expired_attention",
                    work_item_id=approval.work_item_id,
                    run_id=approval.run_id,
                    payload={
                        "approval_id": approval.id,
                        "action": approval.action,
                        "expires_at": approval.expires_at,
                    },
                    deliver_to=[("agent", task.created_by_agent_id)],
                )
            storage.mark_approval_reminded(c, approval.id)

        return {
            "expired_sessions": expired_sessions,
            "lost_runs": len(lost_run_ids),
            "expired_locks": expired_locks,
            "expired_coordinators": len(expired_coordinators),
            "requeued_outbox": requeued_outbox,
            "archived_terminal_deliveries": archived_deliveries,
            "expired_approval_attention": len(expired_approvals),
            "dispatched": dispatched,
            "deadline_tasks": len(deadline_tasks),
        }

    result = write_executor.execute_write(conn, _write)
    result.update(_process_outbox())
    _RUNTIME_STATE["last_reconcile_at"] = now_iso()
    _RUNTIME_STATE["last_reconcile_result"] = result.copy()
    return result


def _process_outbox():
    """Resolve local/manual deliveries; runner-backed adapters lease via adapter_poll."""
    conn = get_db()

    def _write(c):
        delivered = dead = 0
        for item in storage.list_pending_outbox(c, limit=100):
            if item["attempts"] >= item["max_attempts"]:
                storage.dead_letter_outbox(c, item["id"], item.get("last_error") or "max_attempts_exceeded")
                dead += 1
                continue
            adapter_id = item.get("adapter_id")
            if not adapter_id:
                storage.mark_outbox_delivered(
                    c, item["id"], resolution="no_adapter"
                )
                delivered += 1
                continue
            adapter = storage.get_adapter(c, adapter_id)
            if not adapter:
                if item["attempts"] + 1 >= item["max_attempts"]:
                    storage.dead_letter_outbox(c, item["id"], "adapter_not_found")
                    dead += 1
                else:
                    storage.retry_outbox(c, item["id"], "adapter_not_found", 30)
                continue
            if adapter.mode in ("manual_resume", "online_session"):
                storage.mark_outbox_delivered(
                    c, item["id"], resolution="durable_backlog"
                )
                storage.update_adapter_health(c, adapter.id, True)
                delivered += 1
            # resident_runner/webhook stay pending until adapter_poll leases them.
        return {"outbox_delivered": delivered, "outbox_dead_lettered": dead}

    return write_executor.execute_write(conn, _write)


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
    delivery_health = conn.execute(
        """SELECT
               SUM(CASE WHEN status='pending' AND observed_at IS NULL THEN 1 ELSE 0 END)
                   AS pending_unobserved,
               SUM(CASE WHEN status='pending' AND observed_at IS NOT NULL THEN 1 ELSE 0 END)
                   AS pending_observed,
               MIN(CASE WHEN status='pending' THEN available_at END) AS oldest_pending
           FROM deliveries"""
    ).fetchone()
    delivery_backlog_by_recipient = [
        dict(row) for row in conn.execute(
            """SELECT recipient_kind, recipient_id, COUNT(*) AS pending,
                      MIN(available_at) AS oldest_pending
               FROM deliveries WHERE status='pending'
               GROUP BY recipient_kind, recipient_id
               ORDER BY pending DESC LIMIT 10"""
        ).fetchall()
    ]
    attention_cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=TASK_ATTENTION_HOURS)
    ).isoformat()
    task_attention = conn.execute(
        """SELECT
               SUM(CASE WHEN status='running' AND coordinator_agent_id IS NULL THEN 1 ELSE 0 END)
                   AS running_without_coordinator,
               SUM(CASE WHEN status IN ('planned','ready','running','verifying','blocked')
                              AND updated_at < ? THEN 1 ELSE 0 END)
                   AS stale_active_tasks
           FROM tasks""",
        (attention_cutoff,),
    ).fetchone()
    stale_reviews = conn.execute(
        """SELECT COUNT(*) AS n FROM work_items
           WHERE status='reviewing' AND updated_at < ?""",
        (attention_cutoff,),
    ).fetchone()
    expired_approvals = conn.execute(
        """SELECT COUNT(*) AS n FROM approvals
           WHERE decision IS NULL AND expires_at IS NOT NULL AND expires_at < ?""",
        (now_iso(),),
    ).fetchone()
    legacy_pending_approvals = conn.execute(
        """SELECT COUNT(*) AS n FROM approvals
           WHERE decision IS NULL AND expires_at IS NULL"""
    ).fetchone()
    outbox_by_resolution = {
        (row["resolution"] or "unresolved"): row["n"]
        for row in conn.execute(
            """SELECT resolution, COUNT(*) AS n FROM outbox
               GROUP BY resolution"""
        ).fetchall()
    }

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
            "pending_deliveries_unobserved": (
                delivery_health["pending_unobserved"] or 0
                if delivery_health else 0
            ),
            "pending_deliveries_observed_unacked": (
                delivery_health["pending_observed"] or 0
                if delivery_health else 0
            ),
            "oldest_pending_delivery": (
                delivery_health["oldest_pending"] if delivery_health else None
            ),
            "delivery_backlog_by_recipient": delivery_backlog_by_recipient,
            "running_tasks_without_coordinator": (
                task_attention["running_without_coordinator"] or 0
                if task_attention else 0
            ),
            "stale_active_tasks": (
                task_attention["stale_active_tasks"] or 0
                if task_attention else 0
            ),
            "stale_reviews": stale_reviews["n"] if stale_reviews else 0,
            "expired_pending_approvals": (
                expired_approvals["n"] if expired_approvals else 0
            ),
            "legacy_pending_approvals": (
                legacy_pending_approvals["n"] if legacy_pending_approvals else 0
            ),
            "outbox_by_status": storage.count_outbox_by_status(conn),
            "outbox_by_resolution": outbox_by_resolution,
            "attention_threshold_hours": TASK_ATTENTION_HOURS,
            "last_reconcile_at": _RUNTIME_STATE["last_reconcile_at"],
            "last_reconcile_result": _RUNTIME_STATE["last_reconcile_result"],
        },
        "migrations": [{"version": m["version"], "checksum": m["checksum"][:12]} for m in migrations],
    }


# ════════════════════════════════════════════════════════════════════
#  Internal helpers
# ════════════════════════════════════════════════════════════════════

def _select_agent_for_work(conn, wi):
    required = set(_json_obj(wi.required_capabilities_json, []))
    max_active = int(get_config("max_simultaneous_runs_per_agent", 4))
    candidates = []
    for agent in storage.list_agents(conn):
        if not agent.is_active:
            continue
        if wi.preferred_agent_id and wi.preferred_agent_id != agent.id:
            continue
        sessions = storage.list_active_sessions_for_agent(conn, agent.id)
        adapters = storage.list_adapters(conn, agent.id, healthy_only=True)
        if not sessions and not adapters:
            continue
        caps = set(_json_obj(agent.capabilities, []))
        for session in sessions:
            caps.update(_json_obj(session.capabilities_json, []))
        if not required.issubset(caps):
            continue
        active = storage.count_active_runs_for_agent(conn, agent.id)
        if active >= max_active:
            continue
        adapter = adapters[0] if adapters else None
        candidates.append((active, agent.id, adapter.id if adapter else None))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1], candidates[0][2]


def _dispatch_ready_work(conn, limit=20):
    """Create durable offers for ready work without waiting for a human postal step."""
    dispatched = 0
    for wi in storage.list_ready_work_items(conn, agent_id=None, limit=limit):
        if storage.get_active_run_for_work_item(conn, wi.id):
            continue
        task = storage.get_task(conn, wi.task_id)
        budget = _json_obj(task.budget_json, {}) if task else {}
        max_runs = int(budget.get("max_runs", MAX_RUNS_PER_TASK))
        if storage.count_runs_for_task(conn, wi.task_id) >= max_runs:
            storage.update_work_item_status(
                conn, wi.id, "blocked", version=wi.version,
                blocked_reason_json=json.dumps({"type": "budget_exceeded",
                                                "budget": "max_runs"}))
            _emit(conn, wi.task_id, "work.budget_exceeded", work_item_id=wi.id,
                  deliver_to=[("agent", task.created_by_agent_id)] if task else [])
            continue
        selection = _select_agent_for_work(conn, wi)
        if not selection:
            continue
        agent_id, adapter_id = selection
        run = storage.create_run(conn, wi.id, agent_id, None,
                                 iso_plus_seconds(DEFAULT_OFFER_LEASE))
        if not storage.update_work_item_status(conn, wi.id, "offered", version=wi.version):
            continue
        storage.add_task_participant(conn, wi.task_id, agent_id, "worker")
        payload = {"run_id": run.id, "agent_id": agent_id,
                   "work_item_id": wi.id, "objective": wi.objective,
                   "attempt": run.attempt_no}
        _emit(conn, wi.task_id, "work.offered", work_item_id=wi.id, run_id=run.id,
              payload=payload, deliver_to=[("agent", agent_id)],
              outbox_entries=[("adapter.dispatch", payload, adapter_id)])
        dispatched += 1
    return dispatched

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


def _task_dict(task, include_spec=False):
    result = {
        "id": task.id, "objective": task.objective, "status": task.status,
        "priority": task.priority, "plan_version": task.plan_version,
        "deadline_at": task.deadline_at, "created_by_agent_id": task.created_by_agent_id,
        "coordinator_agent_id": task.coordinator_agent_id,
        "coordinator_session_id": task.coordinator_session_id,
        "coordinator_lease_expires_at": task.coordinator_lease_expires_at,
        "created_at": task.created_at, "updated_at": task.updated_at,
        "completed_at": task.completed_at,
    }
    if include_spec:
        result.update({
            "success_criteria": _json_obj(task.success_criteria_json, []),
            "constraints": _json_obj(task.constraints_json, {}),
            "authorization_policy": _json_obj(task.authorization_policy_json, {}),
            "context_refs": _json_obj(task.context_refs_json, []),
            "budget": _json_obj(task.budget_json, {}),
            "blocked_reason": _json_obj(task.blocked_reason_json, {}),
        })
    return result


def _work_item_dict(wi, include_spec=False):
    result = {
        "id": wi.id, "task_id": wi.task_id, "kind": wi.kind,
        "objective": wi.objective, "status": wi.status,
        "priority": wi.priority, "needs_review": wi.needs_review,
        "parent_id": wi.parent_id, "depth": wi.depth,
        "version": wi.version, "preferred_agent_id": wi.preferred_agent_id,
    }
    if include_spec:
        result.update({
            "acceptance": _json_obj(wi.acceptance_json, []),
            "required_capabilities": _json_obj(wi.required_capabilities_json, []),
            "retry_policy": _json_obj(wi.retry_policy_json, {}),
            "blocked_reason": _json_obj(wi.blocked_reason_json, {}),
        })
    return result


def _run_dict(run):
    return {
        "id": run.id, "work_item_id": run.work_item_id, "attempt_no": run.attempt_no,
        "agent_id": run.agent_id, "session_id": run.session_id,
        "status": run.status, "fencing_token": run.fencing_token,
        "lease_expires_at": run.lease_expires_at, "heartbeat_at": run.heartbeat_at,
        "checkpoint_id": run.checkpoint_id, "failure_code": run.failure_code,
    }
