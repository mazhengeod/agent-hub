"""Read-only doctor and online backup behavior."""
from __future__ import annotations

import sqlite3

from agent_hub.ops import backup_database, doctor_database


def test_doctor_is_read_only_and_reports_current_migrations(db_path):
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    before = connection.execute(
        "SELECT COUNT(*) FROM schema_migrations"
    ).fetchone()[0]
    connection.close()
    result = doctor_database(db_path)
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    after = connection.execute(
        "SELECT COUNT(*) FROM schema_migrations"
    ).fetchone()[0]
    connection.close()
    assert result["checks"]["sqlite_quick_check"]["ok"] is True
    assert result["checks"]["migrations"]["ok"] is True
    assert before == after


def test_online_backup_is_consistent(db_path, tmp_path):
    destination = tmp_path / "backup" / "hub.db"
    result = backup_database(db_path, str(destination))
    assert result["quick_check"] == "ok"
    assert destination.exists()
    assert destination.with_suffix(".db.sha256").exists()
    connection = sqlite3.connect(f"file:{destination}?mode=ro", uri=True)
    assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    connection.close()
