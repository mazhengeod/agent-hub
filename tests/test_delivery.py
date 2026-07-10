"""Test delivery pipeline: Event -> Delivery -> Outbox -> cursor sync -> ack."""
from __future__ import annotations

import json

import pytest

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def test_events_create_deliveries(db_path, make_session):
    """State changes create deliveries to relevant agents."""
    task = service.create_task("Task", "agent-a")

    conn = get_db()
    deliveries = conn.execute("SELECT COUNT(*) AS n FROM deliveries").fetchone()
    assert deliveries["n"] > 0, "task.created should create deliveries"

    events = conn.execute("SELECT * FROM events WHERE task_id=?", (task["id"],)).fetchall()
    assert len(events) >= 1
    assert any(e["event_type"] == "task.created" for e in events)


def test_agent_sync_cursor(db_path, make_session):
    """agent_sync returns deliveries via since_event_id cursor."""
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    result = service.agent_sync("agent-a", sess["session_id"], since_event_id=0)
    assert len(result["deliveries"]) > 0
    assert result["latest_event_id"] > 0
    first_cursor = result["latest_event_id"]

    c = service.claim_work("agent-a", sess["session_id"])
    assert c is not None
    service.start_run(c["run_id"], c["fencing_token"], sess["session_id"], "agent-a")

    result2 = service.agent_sync("agent-a", sess["session_id"], since_event_id=first_cursor)
    assert len(result2["deliveries"]) > 0
    assert result2["latest_event_id"] > first_cursor


def test_delivery_ack(db_path, make_session):
    """Ack marks delivery as acked (non-destructive)."""
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")

    result = service.agent_sync("agent-a", sess["session_id"], since_event_id=0)
    assert len(result["deliveries"]) > 0

    delivery_id = result["deliveries"][0]["delivery_id"]
    ack_result = service.ack_delivery("agent-a", delivery_id)
    assert ack_result["status"] == "acked"

    result2 = service.agent_sync("agent-a", sess["session_id"], since_event_id=0)
    acked_ids = [d["delivery_id"] for d in result2["deliveries"]]
    assert delivery_id not in acked_ids


def test_delivery_ack_wrong_agent_rejected(db_path, make_session):
    """Agent B cannot ack agent A's delivery."""
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")

    result = service.agent_sync("agent-a", sess["session_id"], since_event_id=0)
    delivery_id = result["deliveries"][0]["delivery_id"]

    with pytest.raises(HubError) as exc:
        service.ack_delivery("agent-b", delivery_id)
    assert exc.value.code == "delivery_not_found"


def test_outbox_created_on_claim(db_path, make_session):
    """Claiming work enqueues outbox for adapter dispatch."""
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    before = conn.execute("SELECT COUNT(*) AS n FROM outbox").fetchone()["n"]

    c = service.claim_work("agent-a", sess["session_id"])
    assert c is not None

    after = conn.execute("SELECT COUNT(*) AS n FROM outbox").fetchone()["n"]
    assert after > before, "Claim should enqueue outbox entry"

    entries = conn.execute("SELECT * FROM outbox WHERE status='pending'").fetchall()
    assert any(e["event_type"] == "adapter.dispatch" for e in entries)


def test_outbox_processed_by_reconcile(db_path, make_session):
    """Reconcile processes outbox entries (retry/dead-letter)."""
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    c = service.claim_work("agent-a", sess["session_id"])
    assert c is not None

    conn = get_db()
    pending_before = conn.execute(
        "SELECT COUNT(*) AS n FROM outbox WHERE status='pending'").fetchone()["n"]
    assert pending_before > 0

    service.reconcile()

    delivered = conn.execute(
        "SELECT COUNT(*) AS n FROM outbox WHERE status='delivered'"
    ).fetchone()["n"]
    assert delivered > 0


def test_outbox_survives_restart(db_path, make_session):
    """Outbox entries persist across 'restart' (re-query)."""
    sess = make_session("agent-a")
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    c = service.claim_work("agent-a", sess["session_id"])
    assert c is not None

    conn = get_db()
    entries = storage.list_pending_outbox(conn)
    assert len(entries) > 0

    pending_ids = [e["id"] for e in entries]

    entries_after = storage.list_pending_outbox(conn)
    assert len(entries_after) > 0
    assert any(e["id"] in pending_ids for e in entries_after)


def test_lock_acquire_and_release(db_path):
    result = service.acquire_lock("test-lock", "run-1", ttl_seconds=60)
    assert result["fencing_token"] > 0

    lock = storage.get_lock(get_db(), "test-lock")
    assert lock is not None
    assert lock.holder_run_id == "run-1"

    service.release_lock("test-lock", "run-1", result["fencing_token"])
    assert storage.get_lock(get_db(), "test-lock") is None


def test_lock_conflict(db_path):
    service.acquire_lock("conflict-lock", "run-1", ttl_seconds=600)
    with pytest.raises(HubError) as exc:
        service.acquire_lock("conflict-lock", "run-2", ttl_seconds=600)
    assert exc.value.code == "lock_busy"


def test_lock_steal_after_expiry(db_path):
    import time
    r1 = service.acquire_lock("expire-lock", "run-1", ttl_seconds=1)
    time.sleep(1.1)
    r2 = service.acquire_lock("expire-lock", "run-2", ttl_seconds=60)
    assert r2["fencing_token"] > r1["fencing_token"]
    lock = storage.get_lock(get_db(), "expire-lock")
    assert lock.holder_run_id == "run-2"


def test_lock_reissue_to_same_holder(db_path):
    r1 = service.acquire_lock("reissue-lock", "run-1", ttl_seconds=60)
    r2 = service.acquire_lock("reissue-lock", "run-1", ttl_seconds=60)
    assert r2["fencing_token"] > r1["fencing_token"]


def test_release_with_wrong_token_fails(db_path):
    r1 = service.acquire_lock("wrong-token-lock", "run-1", ttl_seconds=60)
    with pytest.raises(HubError) as exc:
        service.release_lock("wrong-token-lock", "run-1", r1["fencing_token"] + 999)
    assert exc.value.code == "lock_not_held"


def test_reconcile_expires_stale(db_path, make_session):
    import time
    sess = make_session("agent-a")
    task = service.create_task("Reconcile task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    c = service.claim_work("agent-a", sess["session_id"])
    service.start_run(c["run_id"], c["fencing_token"], sess["session_id"], "agent-a",
                      lease_seconds=1)
    time.sleep(1.1)

    result = service.reconcile()
    assert result["lost_runs"] >= 1


def test_hub_status_diagnostics(db_path, make_session):
    task = service.create_task("Status task", "agent-a")
    status = service.hub_status()
    assert "version" in status
    assert "counts" in status
    assert "health" in status
    assert "migrations" in status
    assert status["counts"]["tasks"] >= 1
    assert len(status["migrations"]) >= 1
