"""Database layer: connection, migration runner, Unit of Work, retry.

Design rules (per architecture audit):
- Only UnitOfWork manages begin/commit/rollback.
- repository/storage functions never implicitly commit.
- audit/event + business state written in the same transaction.
- retry wraps the whole transaction, not single statements inside it.
- migration runner: UTF-8, per-migration atomic transaction, checksum.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
import time
import functools
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Callable, Any

DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "agent-hub" / "hub.db"
MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"


# ── Time helpers ───────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_plus_seconds(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


# ── ID generation ─────────────────────────────────────────────────

def new_id() -> str:
    return uuid.uuid4().hex[:12]


def next_fencing_token(conn: sqlite3.Connection) -> int:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _seq_fencing (n INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute("INSERT INTO _seq_fencing DEFAULT VALUES")
    conn.execute("UPDATE _seq_fencing SET n = n + 1")
    row = conn.execute("SELECT n FROM _seq_fencing").fetchone()
    return row[0] if row else 1


# ── Connection management ──────────────────────────────────────────

def get_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Get a WAL-mode connection. isolation_level=None so only UoW controls transactions."""
    path = db_path or str(DEFAULT_DB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = get_db(db_path)
    run_migrations(conn)
    return conn


# ── SQL splitter (avoids executescript's implicit COMMIT) ─────────

def _split_sql(sql: str) -> list[str]:
    """Split SQL into individual statements, respecting comments and strings."""
    statements = []
    current = []
    i = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ''

        if in_line_comment:
            current.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            current.append(ch)
            if ch == '*' and nxt == '/':
                current.append(nxt)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue
        if in_single:
            current.append(ch)
            if ch == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            current.append(ch)
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == '-' and nxt == '-':
            in_line_comment = True
            current.append(ch)
            current.append(nxt)
            i += 2
            continue
        if ch == '/' and nxt == '*':
            in_block_comment = True
            current.append(ch)
            i += 2
            continue
        if ch == "'":
            in_single = True
            current.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            current.append(ch)
            i += 1
            continue
        if ch == ';':
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = ''.join(current).strip()
    if tail:
        statements.append(tail)
    return statements


# ── Migration runner ───────────────────────────────────────────────

def run_migrations(conn: sqlite3.Connection) -> list[int]:
    """Apply pending migrations. Each migration is atomic with checksum."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            filename    TEXT NOT NULL,
            checksum    TEXT NOT NULL,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )"""
    )

    applied_rows = conn.execute(
        "SELECT version, checksum FROM schema_migrations ORDER BY version"
    ).fetchall()
    applied = {r["version"]: r["checksum"] for r in applied_rows}

    if not MIGRATIONS_DIR.exists():
        return []

    migration_files = sorted(
        f for f in MIGRATIONS_DIR.glob("*.sql")
        if f.stem.split("_")[0].isdigit()
    )

    newly_applied = []
    for mf in migration_files:
        version = int(mf.stem.split("_")[0])
        sql = mf.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()

        if version in applied:
            if applied[version] != checksum:
                raise RuntimeError(
                    f"Migration {mf.name} checksum mismatch: "
                    f"db={applied[version]} file={checksum}"
                )
            continue

        statements = _split_sql(sql)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in statements:
                lines = stmt.split('\n')
                code_lines = [l for l in lines if not l.strip().startswith('--')]
                code = '\n'.join(code_lines).strip()
                if code:
                    conn.execute(code)
            conn.execute(
                "INSERT INTO schema_migrations (version, filename, checksum) VALUES (?, ?, ?)",
                (version, mf.name, checksum),
            )
            conn.execute("COMMIT")
            newly_applied.append(version)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return newly_applied


def get_applied_migrations(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT version, filename, checksum, applied_at FROM schema_migrations ORDER BY version"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Retry decorator ────────────────────────────────────────────────

def retry_on_busy(max_retries: int = 3, base_delay: float = 0.05):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() and attempt < max_retries - 1:
                        last_exc = e
                        time.sleep(base_delay * (2 ** attempt))
                    else:
                        raise
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ── Unit of Work ───────────────────────────────────────────────────

class UnitOfWork:
    """Transaction boundary owner. Only UoW calls BEGIN/COMMIT/ROLLBACK."""

    def __init__(self, conn: sqlite3.Connection, immediate: bool = True):
        self.conn = conn
        self.immediate = immediate
        self._active = False

    def __enter__(self) -> "UnitOfWork":
        if self.immediate:
            self.conn.execute("BEGIN IMMEDIATE")
        else:
            self.conn.execute("BEGIN")
        self._active = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if not self._active:
            return False
        self._active = False
        if exc_type is not None:
            self.conn.execute("ROLLBACK")
            return False
        self.conn.execute("COMMIT")
        return False

    def rollback(self):
        if self._active:
            self.conn.execute("ROLLBACK")
            self._active = False


# ── Write executor ────────────────────────────────────────────────

class WriteExecutor:
    """Single-writer gate: all writes go through one serialized entry point.

    Uses threading.Lock for in-process serialization. SQLite BEGIN IMMEDIATE
    provides cross-process safety. The scheduler runs in the same process
    (Hub lifespan), so this lock covers all writers.
    """

    def __init__(self):
        import threading
        self._lock = threading.Lock()

    def execute_write(self, conn: sqlite3.Connection,
                      fn: Callable[[sqlite3.Connection], Any],
                      immediate: bool = True, max_retries: int = 3) -> Any:
        @retry_on_busy(max_retries=max_retries)
        def _do():
            with UnitOfWork(conn, immediate=immediate):
                return fn(conn)
        with self._lock:
            return _do()


write_executor = WriteExecutor()
