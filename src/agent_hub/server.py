"""FastMCP server: exposes Agent Hub tools to MCP clients.

Auth: Bearer token extracted from HTTP Authorization header via
get_http_request(). Never passed as a tool parameter.
Scheduler: runs as background asyncio task in Hub lifespan.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastmcp import FastMCP

from . import service
from .auth import verify_token
from .config import get_config
from .db import init_db
from .models import HubError

log = logging.getLogger("agent_hub.server")
RECONCILE_INTERVAL = int(get_config("reconcile_interval_seconds", 15))


def _agent_from_request() -> str:
    """Extract agent_id from HTTP Authorization header."""
    from fastmcp.server.dependencies import get_http_request
    request = get_http_request()
    if request is None:
        raise HubError("no_request", "No HTTP request context available")
    auth_header = request.headers.get("authorization", "")
    agent_id = verify_token(auth_header)
    if not agent_id:
        raise HubError("unauthorized", "Invalid or missing Authorization header")
    return agent_id


# ── Lifespan: init DB + start scheduler ───────────────────────────

@asynccontextmanager
async def hub_lifespan(app):
    init_db()
    log.info("Agent Hub starting (scheduler in-process, interval=%ds)", RECONCILE_INTERVAL)
    scheduler_task = asyncio.create_task(_scheduler_loop())
    try:
        yield
    finally:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        log.info("Agent Hub stopped.")


async def _scheduler_loop():
    """Background reconcile loop. Runs in Hub lifespan (single process)."""
    while True:
        await asyncio.sleep(RECONCILE_INTERVAL)
        try:
            await asyncio.to_thread(service.reconcile)
        except Exception:
            log.exception("Scheduler reconcile failed")


mcp = FastMCP("agent-hub", lifespan=hub_lifespan)


# ════════════════════════════════════════════════════════════════════
#  Session tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def session_start(native_session_ref: Optional[str] = None,
                  capabilities: Optional[list] = None,
                  adapter_id: Optional[str] = None,
                  lease_seconds: Optional[int] = None) -> dict:
    """Start a new agent session. Multiple sessions per agent allowed."""
    agent_id = _agent_from_request()
    return service.session_start(agent_id, native_session_ref, capabilities,
                                 adapter_id, lease_seconds)


@mcp.tool()
def session_heartbeat(session_id: str,
                      lease_seconds: Optional[int] = None) -> dict:
    """Renew session lease."""
    agent_id = _agent_from_request()
    return service.session_heartbeat(agent_id, session_id, lease_seconds)


@mcp.tool()
def session_end(session_id: str) -> dict:
    """End a session."""
    agent_id = _agent_from_request()
    return service.session_end(agent_id, session_id)


# ════════════════════════════════════════════════════════════════════
#  Task tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def task_create(objective: str,
                success_criteria: Optional[list] = None,
                constraints: Optional[dict] = None,
                authorization_policy: Optional[dict] = None,
                context_refs: Optional[list] = None,
                priority: int = 0,
                deadline_at: Optional[str] = None,
                budget: Optional[dict] = None) -> dict:
    """Create a new task."""
    agent_id = _agent_from_request()
    return service.create_task(objective, agent_id, success_criteria,
                               constraints, authorization_policy, context_refs,
                               priority, deadline_at, budget)


@mcp.tool()
def task_get(task_id: str) -> dict:
    """Get task details."""
    _agent_from_request()
    result = service.get_task(task_id)
    if not result:
        raise HubError("task_not_found", f"Task {task_id} not found")
    return result


@mcp.tool()
def task_list(status: Optional[str] = None, limit: int = 50) -> list[dict]:
    """List tasks."""
    _agent_from_request()
    return service.list_tasks(status=status, limit=limit)


@mcp.tool()
def task_plan(task_id: str, work_items: list[dict],
              dependencies: Optional[list[dict]] = None) -> dict:
    """Create work items and dependencies for a task.

    work_items: [{"kind":"implement", "objective":"...", "ref":"wi1",
                   "needs_review":true, "priority":1}]
    dependencies: [{"work_item":"wi1", "depends_on":"wi2", "condition":"succeeded"}]
    """
    agent_id = _agent_from_request()
    return service.plan_task(task_id, work_items, dependencies, agent_id)


@mcp.tool()
def task_start(task_id: str) -> dict:
    """Transition a planned task to running."""
    agent_id = _agent_from_request()
    return service.start_task(task_id, agent_id)


# ════════════════════════════════════════════════════════════════════
#  Work + Run tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def work_claim(session_id: str,
               work_item_id: Optional[str] = None,
               lease_seconds: Optional[int] = None) -> Optional[dict]:
    """Claim a work item for execution. Returns a Run or None."""
    agent_id = _agent_from_request()
    return service.claim_work(agent_id, session_id, work_item_id, lease_seconds)


@mcp.tool()
def work_start(run_id: str, fencing_token: int, session_id: str,
               lease_seconds: Optional[int] = None) -> dict:
    """Transition a claimed run to running."""
    agent_id = _agent_from_request()
    return service.start_run(run_id, fencing_token, session_id, agent_id, lease_seconds)


@mcp.tool()
def work_progress(run_id: str, fencing_token: int,
                  lease_seconds: Optional[int] = None) -> dict:
    """Heartbeat a running run to renew its lease."""
    agent_id = _agent_from_request()
    return service.heartbeat_run(run_id, fencing_token, agent_id, lease_seconds)


@mcp.tool()
def work_checkpoint(run_id: str, fencing_token: int, snapshot: dict) -> dict:
    """Save a recovery checkpoint for a run."""
    agent_id = _agent_from_request()
    return service.save_checkpoint(run_id, fencing_token, snapshot, agent_id)


@mcp.tool()
def work_complete(run_id: str, fencing_token: int, status: str,
                  artifacts: Optional[list] = None,
                  failure_code: Optional[str] = None,
                  failure_detail: Optional[dict] = None) -> dict:
    """Complete a run. status: 'succeeded' or 'failed'."""
    agent_id = _agent_from_request()
    return service.complete_run(run_id, fencing_token, status, agent_id,
                                artifacts, failure_code, failure_detail)


@mcp.tool()
def work_resume(run_id: str, session_id: str,
                lease_seconds: Optional[int] = None) -> dict:
    """Resume a lost run. Cross-agent allowed. Returns checkpoint for recovery."""
    agent_id = _agent_from_request()
    return service.resume_run(run_id, session_id, agent_id, lease_seconds)


# ════════════════════════════════════════════════════════════════════
#  Review + Approval tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def work_review(work_item_id: str, decision: str, comment: str = "") -> dict:
    """Approve or reject a work item in 'reviewing' state."""
    agent_id = _agent_from_request()
    return service.approve_work(work_item_id, agent_id, decision, comment)


@mcp.tool()
def approval_request(task_id: str, action: str, reason: str = "",
                     work_item_id: Optional[str] = None,
                     run_id: Optional[str] = None) -> dict:
    """Request operator approval for an action."""
    agent_id = _agent_from_request()
    return service.request_approval(task_id, action, reason,
                                    work_item_id, run_id, agent_id)


@mcp.tool()
def approval_decide(approval_id: str, decision: str) -> dict:
    """Decide a pending approval."""
    agent_id = _agent_from_request()
    return service.decide_approval(approval_id, decision, agent_id)


# ════════════════════════════════════════════════════════════════════
#  Sync + Status tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def agent_sync(session_id: str, since_event_id: int = 0) -> dict:
    """Batch pull: heartbeat + ready work + cursor-based deliveries + runs."""
    agent_id = _agent_from_request()
    return service.agent_sync(agent_id, session_id, since_event_id)


@mcp.tool()
def delivery_ack(delivery_id: str) -> dict:
    """Acknowledge a delivery."""
    agent_id = _agent_from_request()
    return service.ack_delivery(agent_id, delivery_id)


@mcp.tool()
def hub_status() -> dict:
    """Runtime diagnostics."""
    _agent_from_request()
    return service.hub_status()


# ════════════════════════════════════════════════════════════════════
#  Lock tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def lock_acquire(lock_key: str, holder_run_id: str,
                 resource_type: str = "general", resource_id: str = "",
                 ttl_seconds: int = 600) -> dict:
    """Acquire a resource lock."""
    _agent_from_request()
    return service.acquire_lock(lock_key, holder_run_id,
                                resource_type, resource_id, ttl_seconds)


@mcp.tool()
def lock_release(lock_key: str, holder_run_id: str, fencing_token: int) -> dict:
    """Release a resource lock."""
    _agent_from_request()
    return service.release_lock(lock_key, holder_run_id, fencing_token)


# ════════════════════════════════════════════════════════════════════
#  Entry point
# ════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    mcp.run(transport="http", host="127.0.0.1", port=8765, path="/mcp")


if __name__ == "__main__":
    main()
