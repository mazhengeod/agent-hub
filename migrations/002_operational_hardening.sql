-- Agent Hub v0.6 operational hardening.
-- Additive only: existing v0.5 rows and APIs remain valid.

ALTER TABLE deliveries ADD COLUMN observed_at TEXT;
ALTER TABLE deliveries ADD COLUMN archived_at TEXT;

ALTER TABLE outbox ADD COLUMN resolution TEXT;
UPDATE outbox
SET resolution = 'legacy_unknown'
WHERE status = 'delivered' AND resolution IS NULL;

ALTER TABLE approvals ADD COLUMN expires_at TEXT;
ALTER TABLE approvals ADD COLUMN reminder_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE approvals ADD COLUMN last_reminded_at TEXT;

CREATE TABLE IF NOT EXISTS consumer_cursors (
    consumer_kind       TEXT NOT NULL,
    consumer_id         TEXT NOT NULL,
    stream              TEXT NOT NULL DEFAULT 'events',
    high_watermark      INTEGER NOT NULL DEFAULT 0,
    updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (consumer_kind, consumer_id, stream)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_recipient_pending_event
ON deliveries(recipient_kind, recipient_id, status, event_id);

CREATE INDEX IF NOT EXISTS idx_deliveries_archived
ON deliveries(archived_at)
WHERE archived_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tasks_attention
ON tasks(status, updated_at);

CREATE INDEX IF NOT EXISTS idx_work_items_attention
ON work_items(status, updated_at);

CREATE INDEX IF NOT EXISTS idx_approvals_expiry
ON approvals(expires_at)
WHERE decision IS NULL;
