"""Adapter outbox leasing and acknowledgement."""
from __future__ import annotations

from agent_hub import service, storage
from agent_hub.db import get_db


def test_resident_adapter_receives_scheduler_dispatch(db_path):
    adapter = service.register_adapter(
        "agent-a", "resident_runner", {"poll_interval": 2}, "L3", "runner-a")
    task = service.create_task("Adapter task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    result = service.reconcile()
    assert result["dispatched"] == 1

    poll = service.adapter_poll("agent-a", adapter["id"])
    assert len(poll["entries"]) == 1
    entry = poll["entries"][0]
    assert entry["event_type"] == "adapter.dispatch"

    ack = service.adapter_ack("agent-a", adapter["id"], entry["id"], True)
    assert ack["status"] == "delivered"


def test_adapter_failure_retries_then_dead_letters(db_path):
    adapter = service.register_adapter(
        "agent-a", "resident_runner", {}, "L3", "runner-a")
    conn = get_db()
    with_conn = service.write_executor.execute_write

    def enqueue(c):
        return storage.enqueue_outbox(
            c, "adapter.dispatch", "{}", max_attempts=2, adapter_id=adapter["id"])

    outbox_id = with_conn(conn, enqueue)
    first = service.adapter_poll("agent-a", adapter["id"])["entries"][0]
    service.adapter_ack("agent-a", adapter["id"], first["id"], False, "boom")

    # Make retry immediately available.
    conn.execute("UPDATE outbox SET available_at=datetime('now','-1 second') WHERE id=?",
                 (outbox_id,))
    second = service.adapter_poll("agent-a", adapter["id"])["entries"][0]
    result = service.adapter_ack("agent-a", adapter["id"], second["id"], False, "boom again")
    assert result["status"] == "dead_letter"

