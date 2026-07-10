"""Test fixtures: temporary SQLite database for integration tests.

Each test gets a fresh temp-file database with migrations applied.
We monkeypatch db.get_db to return a cached connection to the temp DB.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

_TEMP_DIR = tempfile.mkdtemp(prefix="agent_hub_test_")


@pytest.fixture(autouse=True)
def temp_db(monkeypatch):
    """Provide a fresh temp database for each test."""
    from agent_hub import db

    db_path = os.path.join(_TEMP_DIR, f"test_{os.getpid()}_{id(object())}.db")
    monkeypatch.setattr(db, "DEFAULT_DB_PATH", Path(db_path))

    # Create real connection and run migrations BEFORE monkeypatching get_db
    real_get_db = db.get_db
    conn = real_get_db(db_path)
    db.run_migrations(conn)

    # Insert a test agent so FK constraints pass
    conn.execute(
        """INSERT OR IGNORE INTO agents (id, name, capabilities, token_hash, is_active)
           VALUES ('test-agent', 'Test Agent', '[]', '', 1)"""
    )
    conn.execute(
        """INSERT OR IGNORE INTO agents (id, name, capabilities, token_hash, is_active)
           VALUES ('reviewer-agent', 'Reviewer Agent', '[]', '', 1)"""
    )
    conn.commit()

    # Cache the connection and return it for all get_db calls
    _conn_cache = {"conn": conn}

    def _cached_get_db(*_args, **_kwargs):
        return _conn_cache["conn"]

    monkeypatch.setattr(db, "get_db", _cached_get_db)

    # Also patch service.get_db since service imports get_db at module level
    from agent_hub import service
    monkeypatch.setattr(service, "get_db", _cached_get_db)

    yield db_path

    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture(autouse=True)
def mock_auth(monkeypatch):
    """Bypass token verification in tests."""
    from agent_hub import auth
    monkeypatch.setattr(auth, "verify_token", lambda token: "test-agent" if token else None)


@pytest.fixture
def make_session():
    """Helper to create a session for testing."""
    from agent_hub import service

    def _make(agent_id: str = "test-agent", **kwargs):
        return service.session_start(agent_id, **kwargs)

    return _make
