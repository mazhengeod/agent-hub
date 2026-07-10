"""Test fixtures: per-test isolated SQLite database.

Each test gets a unique temp-file database with migrations applied.
Connections are closed in teardown. No shared state between tests.
"""
from __future__ import annotations

import uuid
from pathlib import Path

import pytest


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """Create a unique temp database for each test."""
    from agent_hub import db, service

    path = str(tmp_path / f"hub_{uuid.uuid4().hex}.db")

    # Create real connection and run migrations
    real_get_db = db.get_db
    conn = real_get_db(path)
    db.run_migrations(conn)

    # Insert test agents so FK constraints pass
    for agent_id, name in [
        ("agent-a", "Agent A"),
        ("agent-b", "Agent B"),
        ("reviewer", "Reviewer"),
    ]:
        conn.execute(
            "INSERT INTO agents (id, name, capabilities, token_hash, is_active) "
            "VALUES (?, ?, '[]', '', 1)",
            (agent_id, name),
        )
    conn.commit()

    # Cache the connection - all get_db calls return the same conn
    _cache = {"conn": conn}

    def _cached_get_db(*_a, **_kw):
        return _cache["conn"]

    monkeypatch.setattr(db, "get_db", _cached_get_db)
    monkeypatch.setattr(service, "get_db", _cached_get_db)
    monkeypatch.setattr(db, "DEFAULT_DB_PATH", Path(path))

    yield path

    conn.close()


@pytest.fixture
def make_session():
    """Helper to create a session for a specific agent."""
    from agent_hub import service

    def _make(agent_id="agent-a", **kwargs):
        return service.session_start(agent_id, **kwargs)

    return _make


@pytest.fixture
def make_task():
    """Helper to create + plan + start a task with work items."""
    from agent_hub import service

    def _make(agent_id="agent-a", work_items=None, dependencies=None, start=True):
        task = service.create_task("Test task", agent_id)
        if work_items is None:
            work_items = [{"kind": "implement", "objective": "Do work", "ref": "wi1"}]
        service.plan_task(task["id"], work_items, dependencies, agent_id)
        if start:
            service.start_task(task["id"], agent_id)
        return task["id"]

    return _make


@pytest.fixture
def claim_and_start(make_task):
    """Helper to create task, claim work, and start the run."""
    from agent_hub import service

    def _do(agent_id="agent-a", session_id=None, work_item_id=None):
        if session_id is None:
            sess = service.session_start(agent_id)
            session_id = sess["session_id"]
        if work_item_id is None:
            task_id = make_task(agent_id=agent_id)
            from agent_hub import storage
            from agent_hub.db import get_db
            conn = get_db()
            wi = storage.list_work_items(conn, task_id)
            if wi:
                work_item_id = wi[0].id
        claimed = service.claim_work(agent_id, session_id, work_item_id)
        if not claimed:
            raise RuntimeError("No work to claim")
        run = service.start_run(
            claimed["run_id"], claimed["fencing_token"], session_id, agent_id)
        return {"run": run, "session_id": session_id, "claimed": claimed}

    return _do
