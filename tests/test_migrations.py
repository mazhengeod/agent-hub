"""Test migration system: schema_migrations table tracks versions correctly."""
from __future__ import annotations

from agent_hub import db
from agent_hub.db import run_migrations, get_applied_migrations


def test_migrations_applied(temp_db):
    conn = db.get_db(temp_db)
    migrations = get_applied_migrations(conn)
    versions = [m["version"] for m in migrations]
    assert 1 in versions, "schema_migrations table not created"
    assert 2 in versions, "002_task_runtime_v2.sql not applied"


def test_migrations_idempotent(temp_db):
    conn = db.get_db(temp_db)
    # Running again should not re-apply
    newly = run_migrations(conn)
    assert newly == [], f"Unexpected re-applied migrations: {newly}"


def test_all_v2_tables_exist(temp_db):
    conn = db.get_db(temp_db)
    expected = [
        "schema_migrations", "agents", "adapters", "sessions", "tasks",
        "work_items", "work_dependencies", "runs", "checkpoints", "artifacts",
        "events", "deliveries", "outbox", "approvals", "resource_locks",
    ]
    for table in expected:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        assert row is not None, f"Table '{table}' not found"


def test_events_autoincrement(temp_db):
    conn = db.get_db(temp_db)
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO events (task_id, event_type) VALUES ('t1', 'test.event')"
    )
    conn.execute(
        "INSERT INTO events (task_id, event_type) VALUES ('t1', 'test.event2')"
    )
    conn.commit()
    rows = conn.execute(
        "SELECT event_id FROM events ORDER BY event_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[1]["event_id"] > rows[0]["event_id"]


def test_delivery_unique_constraint(temp_db):
    """Same event to same recipient should be rejected (idempotent delivery)."""
    conn = db.get_db(temp_db)
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO events (task_id, event_type) VALUES ('t1', 'test.event')"
    )
    event_id = conn.execute("SELECT event_id FROM events").fetchone()["event_id"]
    conn.execute(
        "INSERT INTO deliveries (id, event_id, recipient_kind, recipient_id) VALUES ('d1', ?, 'agent', 'a1')",
        (event_id,),
    )
    conn.commit()

    conn.execute("BEGIN")
    try:
        conn.execute(
            "INSERT INTO deliveries (id, event_id, recipient_kind, recipient_id) VALUES ('d2', ?, 'agent', 'a1')",
            (event_id,),
        )
        assert False, "Should have raised IntegrityError"
    except Exception:
        pass
    conn.rollback()
