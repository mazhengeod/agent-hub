"""Resident scheduler: runs reconcile() on a timer.

Per architecture audit:
- L1 state-active: Hub automatically manages dependencies, leases,
  retries, timeouts WITHOUT requiring agents to be online.
- L2 delivery-active: Hub pushes work to adapter outbox.
- L3 execution-active: adapter wakes the agent (depends on agent capability).

This scheduler handles L1 + L2. L3 is handled by per-agent adapters.
"""
from __future__ import annotations

import time
import signal
import logging

from . import service
from .db import init_db
from .config import get_config

log = logging.getLogger("agent_hub.scheduler")

RECONCILE_INTERVAL = int(get_config("reconcile_interval_seconds", 15))
OUTBOX_INTERVAL = int(get_config("outbox_interval_seconds", 10))

_running = True


def _handle_stop(signum, frame):
    global _running
    _running = False
    log.info("Received signal %s, stopping scheduler...", signum)


def run_scheduler():
    """Main scheduler loop. Runs until SIGINT/SIGTERM."""
    global _running
    _running = True
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    init_db()
    log.info("Scheduler started (reconcile=%ds, outbox=%ds)",
             RECONCILE_INTERVAL, OUTBOX_INTERVAL)

    last_reconcile = 0.0
    last_outbox = 0.0

    while _running:
        now = time.time()

        if now - last_reconcile >= RECONCILE_INTERVAL:
            try:
                result = service.reconcile()
                if any(result.values()):
                    log.info("Reconcile: %s", result)
            except Exception:
                log.exception("Reconcile failed")
            last_reconcile = now

        if now - last_outbox >= OUTBOX_INTERVAL:
            try:
                _process_outbox()
            except Exception:
                log.exception("Outbox processing failed")
            last_outbox = now

        time.sleep(1)

    log.info("Scheduler stopped.")


def _process_outbox():
    """Process pending outbox entries. For now, just marks them as
    delivered since adapter dispatch is Phase 4."""
    from . import storage
    from .db import get_db
    conn = get_db()

    pending = storage.list_pending_outbox(conn, limit=20)
    for item in pending:
        event_type = item["event_type"]
        payload = item["payload_json"]

        if event_type.startswith("adapter."):
            log.debug("Outbox %s: %s (adapter dispatch TBD)", item["id"], event_type)
        else:
            storage.mark_outbox_delivered(conn, item["id"])


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    run_scheduler()


if __name__ == "__main__":
    main()
