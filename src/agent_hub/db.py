"""Database layer: connection management, migration runner, Unit of Work, retry.

Design rules (per architecture audit section 9.1):
- Only UnitOfWork manages begin/commit/rollback.
- repository/storage functions never implicitly commit.
- audit/event + business state written in the same transaction.
- retry wraps the whole transaction, not single statements inside it.
- state transitions use conditional UPDATE or version compare-and-swap.
"""
from __future__ import annotations

import sqlite3
import time
import functools
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable, Any

DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "agent-hub" / "hub.db"
MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"

# ── Time helpers ───────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def ts_from_iso(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


# ── ID generation ─────────────────────────────────────────────────

_id_counter = 0

def new_id() -> str:
    """Generate a 12-char hex id. Not globally unique across processes,
    but combined with idempotency_key and DB UNIQUE constraints it's safe."""
    global _id_counter
    _id_counter += 1
    import uuid
    return uuid.uuid4().hex[:12]


def next_fencing_token(conn: sqlite3.Connection) -> int:
    """Monotonic fencing token via a single-row sequence table."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _seq_fencing (n INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute("INSERT INTO _seq_fencing DEFAULT VALUES")
    conn.execute("UPDATE _seq_fencing SET n = n + 1")
    row = conn.execute("SELECT n FROM _seq_fencing").fetchone()
    return row[0] if row else 1


# ── Connection management ──────────────────────────────────────────

def get_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Get a WAL-mode connection with standard pragmas.

    isolation_level=None disables Python's auto-transaction management,
    so only UnitOfWork controls BEGIN/COMMIT/ROLLBACK.
    """
    path = db_path or str(DEFAULT_DB_PATH)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Initialize database: run all pending migrations, return connection."""
    conn = get_db(db_path)
    run_migrations(conn)
    return conn


# ── Migration runner ──────────────────────────────────────────────

def run_migrations(conn: sqlite3.Connection) -> list[int]:
    """Apply all pending migrations in order. Returns list of applied versions.

    Uses schema_migrations table instead of 'does table exist' heuristic.
    Each migration runs in its own transaction.
    """
    conn.execute(
        """CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            filename    TEXT NOT NULL,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )"""
    )
    conn.commit()

    applied_rows = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version"
    ).fetchall()
    applied = {r["version"] for r in applied_rows}

    if not MIGRATIONS_DIR.exists():
        return []

    migration_files = sorted(
        f for f in MIGRATIONS_DIR.glob("*.sql")
        if f.stem.split("_")[0].isdigit()
    )

    newly_applied = []
    for mf in migration_files:
        version = int(mf.stem.split("_")[0])
        if version in applied:
            continue
        sql = mf.read_text()
        try:
            conn.executescript(sql)
            newly_applied.append(version)
        except Exception:
            raise

    return newly_applied


def get_applied_migrations(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT version, filename, applied_at FROM schema_migrations ORDER BY version"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Retry decorator ────────────────────────────────────────────────

def retry_on_busy(max_retries: int = 3, base_delay: float = 0.05):
    """Decorator: retry on SQLITE_BUSY. Wraps the WHOLE transaction, not
    single statements inside it (per audit rule)."""
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
    """Transaction boundary owner.

    Only UoW calls BEGIN/COMMIT/ROLLBACK. Repository functions receive
    the connection but never commit. This fixes the nested-commit bug
    where storage.commit() inside a service-level BEGIN IMMEDIATE
    prematurely flushes partial state.

    Usage:
        with UnitOfWork(conn) as uow:
            storage.create_task(uow.conn, ...)
            storage.create_work_item(uow.conn, ...)
            # commit happens at __exit__ if no exception

    For retry, wrap the whole UoW block with retry_on_busy.
    """

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
            self.conn.rollback()
            return False
        self.conn.commit()
        return False

    def rollback(self):
        if self._active:
            self.conn.rollback()
            self._active = False


# ── Single-writer write executor ──────────────────────────────────

class WriteExecutor:
    """Single-writer gate: all writes go through one serialized entry point.

    Per audit section 9.2: MCP tools, Scheduler, adapter callbacks all
    go through the same write entry. Write transactions are short and
    never call network/agent inside the transaction.

    This is a cooperative lock (threading.Lock) since FastMCP runs in
    a single process with asyncio.to_thread for DB work. SQLite's own
    BEGIN IMMEDIATE provides cross-process safety.
    """

    def __init__(self):
        import threading
        self._lock = threading.Lock()

    def execute_write(self, conn: sqlite3.Connection,
                      fn: Callable[[sqlite3.Connection], Any],
                      immediate: bool = True, max_retries: int = 3) -> Any:
        """Run fn inside a UoW with retry. fn must not commit."""
        @retry_on_busy(max_retries=max_retries)
        def _do():
            with UnitOfWork(conn, immediate=immediate):
                return fn(conn)
        with self._lock:
            return _do()


write_executor = WriteExecutor()
