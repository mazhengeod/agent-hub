"""Read-only doctor and online backup behavior."""
from __future__ import annotations

import sqlite3

from agent_hub import cli
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


def test_doctor_can_allow_pending_migrations_for_preflight(db_path):
    connection = sqlite3.connect(db_path)
    connection.execute("DELETE FROM schema_migrations WHERE version=2")
    connection.commit()
    connection.close()

    strict = doctor_database(db_path)
    assert strict["checks"]["migrations"]["ok"] is False
    preflight = doctor_database(db_path, allow_pending_migrations=True)
    assert preflight["checks"]["migrations"]["ok"] is True
    assert preflight["checks"]["migrations"]["value"]["pending"] == [2]


def test_doctor_cli_passes_allow_pending_flag(monkeypatch, capsys):
    called = {}

    def fake_doctor_database(*, allow_pending_migrations=False):
        called["allow_pending_migrations"] = allow_pending_migrations
        return {"ok": True, "db_path": "/tmp/hub.db", "checks": {}}

    monkeypatch.setattr(cli, "doctor_database", fake_doctor_database)
    assert cli.main(["doctor", "--allow-pending-migrations", "--json"]) == 0
    assert called["allow_pending_migrations"] is True
    assert '"ok": true' in capsys.readouterr().out


def test_online_backup_is_consistent(db_path, tmp_path):
    destination = tmp_path / "backup" / "hub.db"
    result = backup_database(db_path, str(destination))
    assert result["quick_check"] == "ok"
    assert destination.exists()
    assert destination.with_suffix(".db.sha256").exists()
    connection = sqlite3.connect(f"file:{destination}?mode=ro", uri=True)
    assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    connection.close()
