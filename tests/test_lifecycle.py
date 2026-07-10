"""Test core lifecycle: task -> plan -> claim -> run -> complete."""
from __future__ import annotations

import pytest

from agent_hub import service
from agent_hub.models import HubError


def test_full_task_lifecycle(make_session):
    """Task: create -> plan -> start -> claim -> start_run -> complete."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Build feature X", "test-agent")
    assert task["status"] == "draft"
    task_id = task["id"]

    planned = service.plan_task(task_id, [
        {"kind": "implement", "objective": "Write the code", "ref": "wi1"},
        {"kind": "verify", "objective": "Test the code", "ref": "wi2",
         "needs_review": True},
    ], dependencies=[
        {"work_item": "wi2", "depends_on": "wi1", "condition": "succeeded"},
    ], actor_agent_id="test-agent")
    assert planned["status"] == "planned"
    assert planned["plan_version"] == 2

    started = service.start_task(task_id, "test-agent")
    assert started["status"] == "running"

    # wi1 should be ready (no deps), wi2 should be pending (depends on wi1)
    from agent_hub import storage
    from agent_hub.db import get_db
    conn = get_db()
    items = storage.list_work_items(conn, task_id)
    wi1 = next(wi for wi in items if wi.objective == "Write the code")
    wi2 = next(wi for wi in items if wi.objective == "Test the code")
    assert wi1.status == "ready"
    assert wi2.status == "pending"

    # Claim wi1
    claimed = service.claim_work("test-agent", session_id, work_item_id=wi1.id)
    assert claimed is not None
    assert claimed["work_item_id"] == wi1.id
    run_id = claimed["run_id"]
    fencing_token = claimed["fencing_token"]

    # Start the run
    started_run = service.start_run(run_id, fencing_token, session_id)
    assert started_run["status"] == "running"

    # Complete successfully
    completed = service.complete_run(run_id, fencing_token, "succeeded",
        artifacts=[{"kind": "file", "ref": "src/feature.py"}],
        actor_agent_id="test-agent")
    assert completed["status"] == "succeeded"

    # wi1 should be succeeded, wi2 should now be ready (dep satisfied)
    conn = get_db()
    items = storage.list_work_items(conn, task_id)
    wi1 = next(wi for wi in items if wi.objective == "Write the code")
    wi2 = next(wi for wi in items if wi.objective == "Test the code")
    assert wi1.status == "succeeded"
    assert wi2.status == "ready"


def test_stale_fencing_token_rejected(make_session):
    """A stale fencing token must be rejected."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Task Y", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Do work", "ref": "wi1"},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    from agent_hub import storage
    from agent_hub.db import get_db
    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    claimed = service.claim_work("test-agent", session_id, work_item_id=wi.id)
    run_id = claimed["run_id"]
    token = claimed["fencing_token"]

    service.start_run(run_id, token, session_id)

    # Try to complete with WRONG token
    with pytest.raises(HubError) as exc:
        service.complete_run(run_id, token + 999, "succeeded")
    assert exc.value.code == "stale_token"


def test_run_retry_on_failure(make_session):
    """Failed runs should be retried up to max_attempts, then marked failed."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Task Z", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Do work", "ref": "wi1",
         "retry_policy_json": '{"max_attempts": 2}'},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    from agent_hub import storage
    from agent_hub.db import get_db
    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    # First attempt fails
    claimed = service.claim_work("test-agent", session_id, work_item_id=wi.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"], session_id)
    service.complete_run(claimed["run_id"], claimed["fencing_token"], "failed",
                         failure_code="test_error")

    wi = storage.get_work_item(conn, wi.id)
    assert wi.status == "ready", "Should be ready for retry after first failure"

    # Second attempt fails - should exhaust retries
    claimed2 = service.claim_work("test-agent", session_id, work_item_id=wi.id)
    assert claimed2["attempt_no"] == 2
    service.start_run(claimed2["run_id"], claimed2["fencing_token"], session_id)
    service.complete_run(claimed2["run_id"], claimed2["fencing_token"], "failed",
                         failure_code="test_error")

    wi = storage.get_work_item(conn, wi.id)
    assert wi.status == "failed", "Should be failed after exhausting retries"

    task = service.get_task(task["id"])
    assert task["status"] == "failed"


def test_checkpoint_and_resume(make_session):
    """Checkpoint saves state; resume creates new attempt with checkpoint."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Task CP", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Do work", "ref": "wi1"},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    from agent_hub import storage
    from agent_hub.db import get_db
    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    claimed = service.claim_work("test-agent", session_id, work_item_id=wi.id)
    run_id = claimed["run_id"]
    token = claimed["fencing_token"]
    service.start_run(run_id, token, session_id)

    # Save checkpoint
    snapshot = {"completed_steps": ["step1", "step2"], "decisions": ["use_redis"]}
    cp = service.save_checkpoint(run_id, token, snapshot)
    assert cp["version"] == 1

    # Simulate lease expiry -> run becomes lost
    conn = get_db()
    conn.execute("BEGIN")
    conn.execute(
        "UPDATE runs SET lease_expires_at=? WHERE id=?",
        ("2000-01-01T00:00:00+00:00", run_id),
    )
    conn.commit()
    storage.expire_stale_runs(conn)
    conn.commit()
    run = storage.get_run(conn, run_id)
    assert run.status == "lost"

    # Resume
    resumed = service.resume_run(run_id, session_id, "test-agent")
    assert resumed["attempt_no"] == 2
    assert resumed["checkpoint"] is not None
    assert resumed["checkpoint"]["completed_steps"] == ["step1", "step2"]
    assert resumed["checkpoint_version"] == 1


def test_review_gate(make_session):
    """Work items with needs_review go to 'reviewing' after success."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Task Review", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Do work", "ref": "wi1",
         "needs_review": True},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    from agent_hub import storage
    from agent_hub.db import get_db
    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    claimed = service.claim_work("test-agent", session_id, work_item_id=wi.id)
    service.start_run(claimed["run_id"], claimed["fencing_token"], session_id)
    service.complete_run(claimed["run_id"], claimed["fencing_token"], "succeeded")

    wi = storage.get_work_item(conn, wi.id)
    assert wi.status == "reviewing"

    # Approve
    result = service.approve_work(wi.id, "reviewer-agent", "approved", "looks good")
    assert result["status"] == "succeeded"

    task = service.get_task(task["id"])
    assert task["status"] == "completed"
