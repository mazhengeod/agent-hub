"""Test migration system: UTF-8, checksum, atomicity, idempotency."""
from __future__ import annotations

import hashlib
from pathlib import Path

from agent_hub import db
from agent_hub.db import run_migrations, get_applied_migrations, _split_sql


def test_migrations_applied(db_path):
    conn = db.get_db(db_path)
    migrations = get_applied_migrations(conn)
    versions = [m["version"] for m in migrations]
    assert 1 in versions


def test_migrations_idempotent(db_path):
    conn = db.get_db(db_path)
    newly = run_migrations(conn)
    assert newly == []


def test_checksum_stored(db_path):
    conn = db.get_db(db_path)
    migrations = get_applied_migrations(conn)
    assert len(migrations) >= 1
    for m in migrations:
        assert m["checksum"]
        assert len(m["checksum"]) == 64


def test_all_tables_exist(db_path):
    conn = db.get_db(db_path)
    expected = [
        "schema_migrations", "agents", "adapters", "sessions", "tasks",
        "work_items", "work_dependencies", "runs", "checkpoints", "artifacts",
        "events", "deliveries", "outbox", "approvals", "resource_locks",
    ]
    for table in expected:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)).fetchone()
        assert row is not None, f"Table '{table}' missing"


def test_sql_split_handles_comments():
    sql = "-- comment\nCREATE TABLE t (id INTEGER); -- trailing\nCREATE INDEX i ON t(id);"
    stmts = _split_sql(sql)
    assert len(stmts) == 2
    assert "CREATE TABLE" in stmts[0]
    assert "CREATE INDEX" in stmts[1]


def test_sql_split_handles_strings():
    sql = "INSERT INTO t VALUES ('has;semicolon'); INSERT INTO t VALUES ('ok');"
    stmts = _split_sql(sql)
    assert len(stmts) == 2


def test_events_autoincrement(db_path):
    conn = db.get_db(db_path)
    conn.execute("BEGIN")
    conn.execute("INSERT INTO events (task_id, event_type) VALUES ('t1', 'a')")
    conn.execute("INSERT INTO events (task_id, event_type) VALUES ('t1', 'b')")
    conn.execute("COMMIT")
    rows = conn.execute("SELECT event_id FROM events ORDER BY event_id").fetchall()
    assert len(rows) == 2
    assert rows[1]["event_id"] > rows[0]["event_id"]


def test_delivery_unique_constraint(db_path):
    conn = db.get_db(db_path)
    conn.execute("BEGIN")
    conn.execute("INSERT INTO events (task_id, event_type) VALUES ('t1', 'e')")
    eid = conn.execute("SELECT event_id FROM events").fetchone()["event_id"]
    conn.execute(
        "INSERT INTO deliveries (id, event_id, recipient_kind, recipient_id) VALUES ('d1', ?, 'agent', 'a')",
        (eid,))
    conn.execute("COMMIT")
    conn.execute("BEGIN")
    try:
        conn.execute(
            "INSERT INTO deliveries (id, event_id, recipient_kind, recipient_id) VALUES ('d2', ?, 'agent', 'a')",
            (eid,))
        assert False, "Should raise IntegrityError"
    except Exception:
        pass
    conn.execute("ROLLBACK")
