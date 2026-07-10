-- 001_schema_migrations.sql
-- Migration framework: tracks which migrations have been applied.
-- Replaces the old "does table exist" heuristic.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    filename    TEXT NOT NULL,
    applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO schema_migrations (version, filename) VALUES (1, '001_schema_migrations.sql');
