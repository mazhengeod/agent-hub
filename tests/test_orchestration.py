"""End-to-end orchestration behavior introduced in v0.5."""
from __future__ import annotations

import pytest

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def _task_with_work(owner="agent-a", *, needs_review=False, budget=None):
    task = service.create_task("Orchestrate work", owner, budget=budget)
    service.plan_task(task["id"], [{
        "kind": "implement", "objective": "build", "ref": "root",
        "needs_review": needs_review,
    }], actor_agent_id=owner)
    service.start_task(task["id"], owner)
    return task["id"]


def test_scheduler_creates_offer_and_agent_accepts(db_path, make_session):
    session = make_session("agent-a")
    task_id = _task_with_work()

    result = service.reconcile()
    assert result["dispatched"] == 1

    sync = service.agent_sync("agent-a", session["session_id"])
    assert len(sync["offered_runs"]) == 1
    offered = sync["offered_runs"][0]

    accepted = service.accept_offer(offered["id"], session["session_id"], "agent-a")
    assert accepted["run_id"] == offered["id"]
    started = service.start_run(accepted["run_id"], accepted["fencing_token"],
                                session["session_id"], "agent-a")
    assert started["status"] == "running"


def test_coordinator_lease_and_takeover(db_path, make_session):
    task = service.create_task("Coordinate", "agent-a")
    session_a = make_session("agent-a")
    session_b = make_session("agent-b", capabilities=["coordinate"])

    first = service.coordinator_claim(task["id"], "agent-a", session_a["session_id"], 60)
    with pytest.raises(HubError) as exc:
        service.coordinator_claim(task["id"], "agent-b", session_b["session_id"], 60)
    assert exc.value.code == "coordinator_busy"

    service.coordinator_release(task["id"], "agent-a", first["fencing_token"])
    second = service.coordinator_claim(task["id"], "agent-b", session_b["session_id"], 60)
    assert second["agent_id"] == "agent-b"
    assert second["fencing_token"] > first["fencing_token"]


def test_dynamic_child_work_and_budget(db_path, make_session):
    session = make_session("agent-a")
    task_id = _task_with_work(budget={"max_work_items": 2, "max_depth": 2})
    conn = get_db()
    root = storage.list_work_items(conn, task_id)[0]
    claimed = service.claim_work("agent-a", session["session_id"], root.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"],
                      session["session_id"], "agent-a")

    result = service.spawn_child_work(
        claimed["run_id"], claimed["fencing_token"], "agent-a",
        [{"kind": "verify", "objective": "verify", "ref": "child"}])
    assert len(result["work_item_ids"]) == 1
    child = storage.get_work_item(conn, result["work_item_ids"][0])
    assert child.parent_id == root.id
    assert child.depth == 1

    with pytest.raises(HubError) as exc:
        service.spawn_child_work(
            claimed["run_id"], claimed["fencing_token"], "agent-a",
            [{"kind": "verify", "objective": "too many"}])
    assert exc.value.code == "work_budget_exceeded"


def test_block_unblock_creates_new_attempt(db_path, make_session):
    session = make_session("agent-a")
    task_id = _task_with_work()
    conn = get_db()
    wi = storage.list_work_items(conn, task_id)[0]
    first = service.claim_work("agent-a", session["session_id"], wi.id)
    service.start_run(first["run_id"], first["fencing_token"],
                      session["session_id"], "agent-a")
    blocked = service.block_work(
        first["run_id"], first["fencing_token"], "agent-a",
        {"type": "waiting_external", "detail": "dependency"},
        checkpoint={"next_steps": ["retry later"]})
    assert blocked["status"] == "blocked"

    service.unblock_work(wi.id, "agent-a", note="dependency ready")
    second = service.claim_work("agent-a", session["session_id"], wi.id)
    assert second["attempt_no"] == 2


def test_approval_gate_blocks_and_resumes(db_path, make_session):
    session = make_session("agent-a")
    task_id = _task_with_work()
    conn = get_db()
    wi = storage.list_work_items(conn, task_id)[0]
    claimed = service.claim_work("agent-a", session["session_id"], wi.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"],
                      session["session_id"], "agent-a")

    approval = service.request_approval(
        task_id, "deploy", "production change", wi.id, claimed["run_id"], "agent-a")
    assert storage.get_work_item(conn, wi.id).status == "blocked"
    assert storage.get_run(conn, claimed["run_id"]).status == "blocked"

    decided = service.decide_approval(
        approval["approval_id"], "approved", "hubctl", is_operator=True)
    assert decided["decision"] == "approved"
    assert storage.get_work_item(conn, wi.id).status == "ready"


def test_self_review_denied_by_default(db_path, make_session):
    session = make_session("agent-a")
    task_id = _task_with_work(needs_review=True)
    conn = get_db()
    wi = storage.list_work_items(conn, task_id)[0]
    claimed = service.claim_work("agent-a", session["session_id"], wi.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"],
                      session["session_id"], "agent-a")
    service.complete_run(claimed["run_id"], claimed["fencing_token"],
                         "succeeded", "agent-a")
    with pytest.raises(HubError) as exc:
        service.approve_work(wi.id, "agent-a", "approved")
    assert exc.value.code == "self_review_denied"


def test_task_cancel_propagates(db_path, make_session):
    session = make_session("agent-a")
    task_id = _task_with_work()
    conn = get_db()
    wi = storage.list_work_items(conn, task_id)[0]
    claimed = service.claim_work("agent-a", session["session_id"], wi.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"],
                      session["session_id"], "agent-a")

    result = service.cancel_task(task_id, "agent-a", "user stopped")
    assert result["status"] == "cancelled"
    assert storage.get_work_item(conn, wi.id).status == "cancelled"
    assert storage.get_run(conn, claimed["run_id"]).status == "cancelled"


def test_task_access_and_idempotent_event(db_path):
    task = service.create_task("Private task", "agent-a")
    with pytest.raises(HubError) as exc:
        service.task_snapshot(task["id"], "agent-b")
    assert exc.value.code == "task_access_denied"

    first = service.post_event(task["id"], "agent-a", "question.asked",
                               {"text": "hello"}, idempotency_key="same-request")
    second = service.post_event(task["id"], "agent-a", "question.asked",
                                {"text": "hello"}, idempotency_key="same-request")
    assert first["event_id"] is not None
    assert second["duplicate"] is True


def test_fresh_authenticated_agent_auto_registers(db_path):
    conn = get_db()
    conn.execute("DELETE FROM agents WHERE id='agent-b'")
    session = service.session_start("agent-b", capabilities=["coordinate"])
    assert session["agent_id"] == "agent-b"
    assert storage.get_agent(conn, "agent-b") is not None


def test_deadline_cancels_remaining_work(db_path, make_session):
    task = service.create_task(
        "Expired", "agent-a", deadline_at="2000-01-01T00:00:00+00:00")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "late work"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")
    result = service.reconcile()
    assert result["deadline_tasks"] == 1
    assert service.get_task(task["id"])["status"] == "failed"
