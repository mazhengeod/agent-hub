"""Test transaction boundary: storage functions never commit,
UnitOfWork owns begin/commit/rollback."""
from __future__ import annotations

import sqlite3
import pytest

from agent_hub import db, storage, service
from agent_hub.db import UnitOfWork, WriteExecutor, write_executor
from agent_hub.models import HubError


def test_storage_never_commits(temp_db):
    """If a storage write is not wrapped in UoW, it should not persist.
    With isolation_level=None, we must manually BEGIN to test this."""
    conn = db.get_db(temp_db)
    conn.execute("BEGIN")
    storage.create_task(conn, "test-task-1", "test objective", "test-agent")
    conn.rollback()
    task = storage.get_task(conn, "test-task-1")
    assert task is None, "storage.create_task should not have persisted after rollback"


def test_uow_commits_on_success(temp_db):
    conn = db.get_db(temp_db)
    with UnitOfWork(conn):
        storage.create_task(conn, "test-task-2", "test objective", "test-agent")
    task = storage.get_task(conn, "test-task-2")
    assert task is not None
    assert task.objective == "test objective"


def test_uow_rolls_back_on_exception(temp_db):
    conn = db.get_db(temp_db)
    with pytest.raises(RuntimeError):
        with UnitOfWork(conn):
            storage.create_task(conn, "test-task-3", "test objective", "test-agent")
            raise RuntimeError("simulated failure")
    task = storage.get_task(conn, "test-task-3")
    assert task is None, "Task should not persist after exception in UoW"


def test_event_and_state_same_transaction(temp_db):
    """Event and business state must be in the same transaction."""
    conn = db.get_db(temp_db)
    executor = WriteExecutor()

    def _write(c):
        storage.create_task(c, "test-task-4", "test objective", "test-agent")
        storage.append_event(c, "test-task-4", "task.created",
                             actor_agent_id="test-agent")
        return None

    executor.execute_write(conn, _write)
    task = storage.get_task(conn, "test-task-4")
    events = conn.execute("SELECT * FROM events WHERE task_id=?", ("test-task-4",)).fetchall()
    assert task is not None
    assert len(events) == 1


def test_write_executor_retries_on_busy(temp_db):
    """WriteExecutor should retry on SQLITE_BUSY."""
    conn = db.get_db(temp_db)
    call_count = {"n": 0}

    def _write(c):
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise sqlite3.OperationalError("database is locked")
        storage.create_task(c, "test-task-5", "test objective", "test-agent")
        return None

    executor = WriteExecutor()
    result = executor.execute_write(conn, _write, max_retries=3)
    assert result is None
    assert call_count["n"] == 2, "Should have retried once"
    task = storage.get_task(conn, "test-task-5")
    assert task is not None
