"""Test transaction boundary: storage never commits, UoW owns transactions."""
from __future__ import annotations

import sqlite3
import pytest

from agent_hub import db, storage
from agent_hub.db import UnitOfWork, WriteExecutor
from agent_hub.models import HubError


def test_storage_never_commits(db_path):
    conn = db.get_db(db_path)
    conn.execute("BEGIN")
    storage.create_task(conn, "t1", "obj", "agent-a")
    conn.execute("ROLLBACK")
    assert storage.get_task(conn, "t1") is None


def test_uow_commits_on_success(db_path):
    conn = db.get_db(db_path)
    with UnitOfWork(conn):
        storage.create_task(conn, "t1", "obj", "agent-a")
    assert storage.get_task(conn, "t1") is not None


def test_uow_rolls_back_on_exception(db_path):
    conn = db.get_db(db_path)
    with pytest.raises(RuntimeError):
        with UnitOfWork(conn):
            storage.create_task(conn, "t1", "obj", "agent-a")
            raise RuntimeError("fail")
    assert storage.get_task(conn, "t1") is None


def test_event_and_state_same_transaction(db_path):
    conn = db.get_db(db_path)
    executor = WriteExecutor()

    def _write(c):
        storage.create_task(c, "t1", "obj", "agent-a")
        storage.append_event(c, "t1", "task.created", actor_agent_id="agent-a")
        return None

    executor.execute_write(conn, _write)
    assert storage.get_task(conn, "t1") is not None
    events = conn.execute("SELECT * FROM events WHERE task_id='t1'").fetchall()
    assert len(events) == 1


def test_write_executor_retries_on_busy(db_path):
    conn = db.get_db(db_path)
    calls = {"n": 0}

    def _write(c):
        calls["n"] += 1
        if calls["n"] < 2:
            raise sqlite3.OperationalError("database is locked")
        storage.create_task(c, "t1", "obj", "agent-a")
        return None

    executor = WriteExecutor()
    executor.execute_write(conn, _write, max_retries=3)
    assert calls["n"] == 2
    assert storage.get_task(conn, "t1") is not None
