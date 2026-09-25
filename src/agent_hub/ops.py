"""Operator-safe diagnostics and backup helpers."""
from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import DEFAULT_DB_PATH, MIGRATIONS_DIR


def _mode(path: Path) -> Optional[str]:
    if not path.exists() or os.name != "posix":
        return None
    return f"{stat.S_IMODE(path.stat().st_mode):04o}"


def doctor_database(db_path: Optional[str] = None) -> dict:
    """Inspect an existing database without creating files or running migrations."""
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    checks: dict[str, dict] = {}

    def record(name: str, ok: bool, value=None, detail: Optional[str] = None) -> None:
        checks[name] = {"ok": bool(ok), "value": value}
        if detail:
            checks[name]["detail"] = detail

    if not path.exists():
        record("database_exists", False, str(path))
        return {"ok": False, "db_path": str(path), "checks": checks}

    record("database_exists", True, str(path))
    mode = _mode(path)
    record(
        "database_permissions",
        mode in (None, "0600"),
        mode,
        "expected 0600 on POSIX" if mode not in (None, "0600") else None,
    )
    parent_mode = _mode(path.parent)
    record(
        "data_directory_permissions",
        parent_mode in (None, "0700"),
        parent_mode,
        "expected 0700 on POSIX" if parent_mode not in (None, "0700") else None,
    )

    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
        record("sqlite_quick_check", quick_check == "ok", quick_check)
        foreign_key_errors = connection.execute("PRAGMA foreign_key_check").fetchall()
        record("foreign_key_check", not foreign_key_errors, len(foreign_key_errors))

        migration_table = connection.execute(
            """SELECT name FROM sqlite_master
               WHERE type='table' AND name='schema_migrations'"""
        ).fetchone()
        if migration_table:
            applied = {
                row["version"]: row["checksum"]
                for row in connection.execute(
                    "SELECT version, checksum FROM schema_migrations ORDER BY version"
                ).fetchall()
            }
        else:
            applied = {}
        expected = {}
        if MIGRATIONS_DIR.exists():
            for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
                prefix = migration.stem.split("_", 1)[0]
                if prefix.isdigit():
                    expected[int(prefix)] = hashlib.sha256(
                        migration.read_bytes()
                    ).hexdigest()
        missing = sorted(set(expected) - set(applied))
        mismatched = sorted(
            version for version in set(expected) & set(applied)
            if expected[version] != applied[version]
        )
        record(
            "migrations",
            not missing and not mismatched and bool(expected),
            {"applied": sorted(applied), "expected": sorted(expected)},
            f"missing={missing}, checksum_mismatch={mismatched}"
            if missing or mismatched or not expected else None,
        )
    finally:
        connection.close()

    disk = shutil.disk_usage(path.parent)
    record(
        "disk_free",
        disk.free >= 100 * 1024 * 1024,
        {"free_bytes": disk.free, "total_bytes": disk.total},
        "less than 100 MiB free" if disk.free < 100 * 1024 * 1024 else None,
    )

    backup_root = path.parent / "backups"
    backups = sorted(
        backup_root.glob("*/hub.db"), key=lambda item: item.stat().st_mtime,
        reverse=True,
    ) if backup_root.exists() else []
    latest_backup = backups[0] if backups else None
    record(
        "backup_present",
        latest_backup is not None,
        str(latest_backup) if latest_backup else None,
    )
    return {
        "ok": all(check["ok"] for check in checks.values()),
        "db_path": str(path),
        "checks": checks,
    }


def backup_database(db_path: Optional[str] = None,
                    destination: Optional[str] = None) -> dict:
    """Create and verify a transactionally consistent SQLite online backup."""
    source_path = Path(db_path) if db_path else DEFAULT_DB_PATH
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    if destination:
        destination_path = Path(destination)
        if destination_path.exists():
            raise FileExistsError(destination_path)
        destination_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination_path = source_path.parent / "backups" / stamp / "hub.db"
        destination_path.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
    if os.name == "posix" and not destination:
        destination_path.parent.chmod(0o700)

    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    target = sqlite3.connect(destination_path)
    try:
        source.backup(target)
        quick_check = target.execute("PRAGMA quick_check").fetchone()[0]
        if quick_check != "ok":
            raise RuntimeError(f"backup quick_check failed: {quick_check}")
    finally:
        target.close()
        source.close()

    if os.name == "posix":
        destination_path.chmod(0o600)
    digest = hashlib.sha256(destination_path.read_bytes()).hexdigest()
    checksum_path = destination_path.with_suffix(".db.sha256")
    checksum_path.write_text(f"{digest}  {destination_path.name}\n", encoding="utf-8")
    if os.name == "posix":
        checksum_path.chmod(0o600)
    return {
        "path": str(destination_path),
        "sha256": digest,
        "quick_check": "ok",
    }
