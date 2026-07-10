"""Test lifecycle: task -> plan -> claim -> run -> complete, checkpoint,
cross-agent resume, retry, review rework."""
from __future__ import annotations

import pytest

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def test_full_task_lifecycle(db_path, make_session):
    sess = make_session("agent-a")
    task = service.create_task("Build feature X", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Write code", "ref": "wi1"},
        {"kind": "verify", "objective": "Test code", "ref": "wi2"},
    ], dependencies=[
        {"work_item": "wi2", "depends_on": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    items = storage.list_work_items(conn, task["id"])
    wi1 = next(wi for wi in items if wi.objective == "Write code")
    wi2 = next(wi for wi in items if wi.objective == "Test code")
    assert wi1.status == "ready"
    assert wi2.status == "pending"

    claimed = service.claim_work("agent-a", sess["session_id"], work_item_id=wi1.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"],
                      sess["session_id"], "agent-a")
    service.complete_run(claimed["run_id"], claimed["fencing_token"],
                         "succeeded", "agent-a",
                         artifacts=[{"kind": "file", "ref": "src/feature.py"}])

    conn = get_db()
    wi1 = storage.get_work_item(conn, wi1.id)
    wi2 = storage.get_work_item(conn, wi2.id)
    assert wi1.status == "succeeded"
    assert wi2.status == "ready"

    claimed2 = service.claim_work("agent-a", sess["session_id"], work_item_id=wi2.id)
    service.start_run(claimed2["run_id"], claimed2["fencing_token"],
                      sess["session_id"], "agent-a")
    service.complete_run(claimed2["run_id"], claimed2["fencing_token"],
                         "succeeded", "agent-a")
    task = service.get_task(task["id"])
    assert task["status"] == "completed"


def test_run_retry_on_failure(db_path, make_session):
    sess = make_session("agent-a")
    task = service.create_task("Task Z", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1",
         "retry_policy_json": '{"max_attempts": 2}'},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    c1 = service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
    service.start_run(c1["run_id"], c1["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c1["run_id"], c1["fencing_token"], "failed", "agent-a",
                         failure_code="err")
    wi = storage.get_work_item(conn, wi.id)
    assert wi.status == "ready"

    c2 = service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
    assert c2["attempt_no"] == 2
    service.start_run(c2["run_id"], c2["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c2["run_id"], c2["fencing_token"], "failed", "agent-a",
                         failure_code="err")
    wi = storage.get_work_item(conn, wi.id)
    assert wi.status == "failed"
    assert service.get_task(task["id"])["status"] == "failed"


def test_checkpoint_and_cross_agent_resume(db_path, make_session):
    """P1 fix: agent B can resume agent A's lost run from checkpoint."""
    sess_a = make_session("agent-a")
    task = service.create_task("Task CP", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    c1 = service.claim_work("agent-a", sess_a["session_id"], work_item_id=wi.id)
    run_id = c1["run_id"]
    token = c1["fencing_token"]
    service.start_run(run_id, token, sess_a["session_id"], "agent-a")

    snapshot = {"completed_steps": ["step1"], "decisions": ["use_redis"]}
    cp = service.save_checkpoint(run_id, token, snapshot, "agent-a")
    assert cp["version"] == 1

    conn = get_db()
    conn.execute("BEGIN")
    conn.execute("UPDATE runs SET lease_expires_at=? WHERE id=?",
                 ("2000-01-01T00:00:00+00:00", run_id))
    conn.execute("COMMIT")
    storage.expire_stale_runs(conn)
    conn.execute("BEGIN")
    conn.execute("UPDATE runs SET status='lost' WHERE id=?", (run_id,))
    conn.execute("COMMIT")
    assert storage.get_run(conn, run_id).status == "lost"

    sess_b = make_session("agent-b")
    resumed = service.resume_run(run_id, sess_b["session_id"], "agent-b")
    assert resumed["attempt_no"] == 2
    assert resumed["checkpoint"] is not None
    assert resumed["checkpoint"]["completed_steps"] == ["step1"]


def test_review_rework_cycle(db_path, make_session):
    """P1 fix: rejection routes work back to ready for rework."""
    sess = make_session("agent-a")
    task = service.create_task("Task Review", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1",
         "needs_review": True},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    c1 = service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
    service.start_run(c1["run_id"], c1["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c1["run_id"], c1["fencing_token"], "succeeded", "agent-a")

    wi = storage.get_work_item(conn, wi.id)
    assert wi.status == "reviewing"

    result = service.approve_work(wi.id, "reviewer", "rejected", "fix bugs")
    assert result["status"] == "ready"

    c2 = service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
    assert c2 is not None
    assert c2["attempt_no"] == 2
    service.start_run(c2["run_id"], c2["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c2["run_id"], c2["fencing_token"], "succeeded", "agent-a")

    result = service.approve_work(wi.id, "reviewer", "approved", "good")
    assert result["status"] == "succeeded"
    assert service.get_task(task["id"])["status"] == "completed"


def test_review_approved_completes_task(db_path, make_session):
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1",
         "needs_review": True},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    c1 = service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
    service.start_run(c1["run_id"], c1["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c1["run_id"], c1["fencing_token"], "succeeded", "agent-a")

    service.approve_work(wi.id, "reviewer", "approved", "ok")
    assert service.get_task(task["id"])["status"] == "completed"


def test_concurrent_claim_no_duplicate(db_path, make_session):
    import threading
    sess = make_session("agent-a")
    task = service.create_task("Concurrent", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    results = []
    errors = []

    def try_claim():
        try:
            r = service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
            results.append(r)
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=try_claim)
    t2 = threading.Thread(target=try_claim)
    t1.start(); t2.start()
    t1.join(); t2.join()

    successful = [r for r in results if r is not None]
    assert len(successful) <= 1
