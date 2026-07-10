"""Test concurrency: lock conflicts, stale writes, agent_sync."""
from __future__ import annotations

import pytest
import threading
import time

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def test_lock_acquire_and_release(make_session):
    """Basic lock acquire/release cycle."""
    sess = make_session()
    result = service.acquire_lock("test-lock", "run-1", ttl_seconds=60)
    assert result["fencing_token"] > 0

    lock = storage.get_lock(get_db(), "test-lock")
    assert lock is not None
    assert lock.holder_run_id == "run-1"

    service.release_lock("test-lock", "run-1", result["fencing_token"])
    lock = storage.get_lock(get_db(), "test-lock")
    assert lock is None


def test_lock_conflict(make_session):
    """Second run cannot acquire a lock held by another run."""
    sess = make_session()

    r1 = service.acquire_lock("conflict-lock", "run-1", ttl_seconds=600)
    assert r1["fencing_token"] > 0

    with pytest.raises(HubError) as exc:
        service.acquire_lock("conflict-lock", "run-2", ttl_seconds=600)
    assert exc.value.code == "lock_busy"

    lock = storage.get_lock(get_db(), "conflict-lock")
    assert lock.holder_run_id == "run-1"


def test_lock_reissue_to_same_holder(make_session):
    """Same holder can re-acquire (renew) its own lock."""
    sess = make_session()

    r1 = service.acquire_lock("reissue-lock", "run-1", ttl_seconds=60)
    r2 = service.acquire_lock("reissue-lock", "run-1", ttl_seconds=60)
    assert r2["fencing_token"] > r1["fencing_token"], "Renewal should get higher token"


def test_lock_steal_after_expiry(make_session):
    """An expired lock can be stolen by another run."""
    sess = make_session()

    r1 = service.acquire_lock("expire-lock", "run-1", ttl_seconds=1)
    time.sleep(1.1)

    r2 = service.acquire_lock("expire-lock", "run-2", ttl_seconds=60)
    assert r2["fencing_token"] > r1["fencing_token"]

    lock = storage.get_lock(get_db(), "expire-lock")
    assert lock.holder_run_id == "run-2"


def test_release_with_wrong_token_fails(make_session):
    """Release with wrong fencing token should fail."""
    sess = make_session()

    r1 = service.acquire_lock("wrong-token-lock", "run-1", ttl_seconds=60)
    with pytest.raises(HubError) as exc:
        service.release_lock("wrong-token-lock", "run-1", r1["fencing_token"] + 999)
    assert exc.value.code == "lock_not_held"

    lock = storage.get_lock(get_db(), "wrong-token-lock")
    assert lock is not None, "Lock should still be held"


def test_agent_sync_returns_state(make_session):
    """agent_sync returns ready work, active runs, deliveries."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Sync task", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Work 1", "ref": "wi1"},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    result = service.agent_sync("test-agent", session_id)
    assert "ready_work" in result
    assert "active_runs" in result
    assert "offered_runs" in result
    assert "deliveries" in result
    assert "latest_event_id" in result
    assert len(result["ready_work"]) >= 1
    assert result["latest_event_id"] > 0


def test_reconcile_expires_stale(make_session):
    """reconcile() should expire stale sessions and runs."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Reconcile task", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Work 1", "ref": "wi1"},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    claimed = service.claim_work("test-agent", session_id, work_item_id=None)
    assert claimed is not None
    service.start_run(claimed["run_id"], claimed["fencing_token"], session_id,
                      lease_seconds=1)

    time.sleep(1.1)
    result = service.reconcile()
    assert result["lost_runs"] >= 1


def test_concurrent_claim_does_not_duplicate(make_session):
    """Two threads claiming the same work item: only one should succeed."""
    sess = make_session()
    session_id = sess["session_id"]

    task = service.create_task("Concurrent task", "test-agent")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "Work 1", "ref": "wi1"},
    ], actor_agent_id="test-agent")
    service.start_task(task["id"], "test-agent")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    results = []
    errors = []

    def try_claim():
        try:
            r = service.claim_work("test-agent", session_id, work_item_id=wi.id)
            results.append(r)
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=try_claim)
    t2 = threading.Thread(target=try_claim)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    successful = [r for r in results if r is not None]
    assert len(successful) <= 1, "Both threads claimed the same work item!"
