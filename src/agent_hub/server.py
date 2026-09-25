"""FastMCP server: exposes Agent Hub tools to MCP clients.

Auth: Bearer token extracted from HTTP Authorization header via
get_http_request(). Never passed as a tool parameter.
Scheduler: runs as background asyncio task in Hub lifespan.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware

from . import service
from .auth import verify_token
from .config import get_config
from .db import init_db
from .models import ArtifactSpec, DependencySpec, HubError, WorkItemSpec

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
        try:
            await asyncio.to_thread(service.reconcile)
        except Exception:
            log.exception("Scheduler reconcile failed")
        await asyncio.sleep(RECONCILE_INTERVAL)


mcp = FastMCP("agent-hub", lifespan=hub_lifespan)


class HubErrorMiddleware(Middleware):
    """Expose stable domain errors without treating expected conflicts as crashes."""

    async def on_call_tool(self, context, call_next):
        try:
            return await call_next(context)
        except HubError as exc:
            log.info("Hub request rejected code=%s message=%s", exc.code, exc.message)
            raise ToolError(
                f"{exc.code}: {exc.message}", log_level=logging.INFO
            ) from None


mcp.add_middleware(HubErrorMiddleware())


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
def task_get(task_id: str, include_graph: bool = False,
             after_event_id: int = 0) -> dict:
    """Get task details."""
    agent_id = _agent_from_request()
    result = (service.task_snapshot(task_id, agent_id, after_event_id)
              if include_graph else service.get_task(task_id, agent_id))
    if not result:
        raise HubError("task_not_found", f"Task {task_id} not found")
    return result


@mcp.tool()
def task_list(status: Optional[str] = None, limit: int = 50) -> list[dict]:
    """List tasks."""
    agent_id = _agent_from_request()
    return service.list_tasks(status=status, limit=limit, actor_agent_id=agent_id)


@mcp.tool()
def task_plan(task_id: str, work_items: list[WorkItemSpec],
              dependencies: Optional[list[DependencySpec]] = None,
              coordinator_token: Optional[int] = None,
              coordinator_session_id: Optional[str] = None) -> dict:
    """Create work items and dependencies for a task.

    work_items: [{"kind":"implement", "objective":"...", "ref":"wi1",
                   "needs_review":true, "priority":1}]
    dependencies: [{"work_item":"wi1", "depends_on":"wi2", "condition":"succeeded"}]
    """
    agent_id = _agent_from_request()
    return service.plan_task(task_id, work_items, dependencies, agent_id,
                             coordinator_token, coordinator_session_id)


@mcp.tool()
def task_start(task_id: str, coordinator_token: Optional[int] = None,
               coordinator_session_id: Optional[str] = None) -> dict:
    """Transition a planned task to running."""
    agent_id = _agent_from_request()
    return service.start_task(task_id, agent_id, coordinator_token,
                              coordinator_session_id)


@mcp.tool()
def task_cancel(task_id: str, reason: str = "",
                coordinator_token: Optional[int] = None,
                coordinator_session_id: Optional[str] = None) -> dict:
    """Cancel a task and propagate cancellation to active work and runs."""
    return service.cancel_task(task_id, _agent_from_request(), reason,
                               coordinator_token, coordinator_session_id)


@mcp.tool()
def task_timeline(task_id: str, after_event_id: int = 0,
                  limit: int = 200) -> list[dict]:
    """Return the immutable task event timeline."""
    return service.task_timeline(task_id, _agent_from_request(), after_event_id, limit)


@mcp.tool()
def task_explain(task_id: str) -> dict:
    """Explain why a task is waiting and what should happen next."""
    return service.task_explain(task_id, _agent_from_request())


@mcp.tool()
def task_participant_add(task_id: str, agent_id: str, role: str,
                         coordinator_token: Optional[int] = None,
                         coordinator_session_id: Optional[str] = None) -> dict:
    """Add a task participant with an explicit role."""
    return service.add_participant(task_id, agent_id, role, _agent_from_request(),
                                   coordinator_token, coordinator_session_id)


@mcp.tool()
def coordinator_claim(task_id: str, session_id: str,
                      lease_seconds: Optional[int] = None) -> dict:
    return service.coordinator_claim(task_id, _agent_from_request(), session_id,
                                     lease_seconds)


@mcp.tool()
def coordinator_heartbeat(task_id: str, fencing_token: int,
                          lease_seconds: Optional[int] = None) -> dict:
    return service.coordinator_heartbeat(task_id, _agent_from_request(), fencing_token,
                                         lease_seconds)


@mcp.tool()
def coordinator_release(task_id: str, fencing_token: int) -> dict:
    return service.coordinator_release(task_id, _agent_from_request(), fencing_token)


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
def work_accept(run_id: str, session_id: str,
                lease_seconds: Optional[int] = None) -> dict:
    """Accept a scheduler-created offer."""
    return service.accept_offer(run_id, session_id, _agent_from_request(), lease_seconds)


@mcp.tool()
def work_start(run_id: str, fencing_token: int, session_id: str,
               lease_seconds: Optional[int] = None) -> dict:
    """Transition a claimed run to running."""
    agent_id = _agent_from_request()
    return service.start_run(run_id, fencing_token, session_id, agent_id, lease_seconds)


@mcp.tool()
def work_progress(run_id: str, fencing_token: int, session_id: str,
                  lease_seconds: Optional[int] = None) -> dict:
    """Heartbeat a running run to renew its lease."""
    agent_id = _agent_from_request()
    return service.heartbeat_run(run_id, fencing_token, agent_id, lease_seconds,
                                 session_id)


@mcp.tool()
def work_checkpoint(run_id: str, fencing_token: int, session_id: str,
                    snapshot: dict) -> dict:
    """Save a recovery checkpoint for a run."""
    agent_id = _agent_from_request()
    return service.save_checkpoint(run_id, fencing_token, snapshot, agent_id,
                                   session_id)


@mcp.tool()
def work_complete(run_id: str, fencing_token: int, status: str,
                  session_id: str,
                  artifacts: Optional[list[ArtifactSpec]] = None,
                  failure_code: Optional[str] = None,
                  failure_detail: Optional[dict] = None) -> dict:
    """Complete a run. status: 'succeeded' or 'failed'."""
    agent_id = _agent_from_request()
    return service.complete_run(run_id, fencing_token, status, agent_id,
                                artifacts, failure_code, failure_detail, session_id)


@mcp.tool()
def work_resume(run_id: str, session_id: str,
                lease_seconds: Optional[int] = None) -> dict:
    """Resume a lost run. Cross-agent allowed. Returns checkpoint for recovery."""
    agent_id = _agent_from_request()
    return service.resume_run(run_id, session_id, agent_id, lease_seconds)


@mcp.tool()
def work_spawn_child(run_id: str, fencing_token: int,
                     session_id: str,
                     work_items: list[WorkItemSpec],
                     dependencies: Optional[list[DependencySpec]] = None) -> dict:
    """Dynamically add child work within the current task budget."""
    return service.spawn_child_work(run_id, fencing_token, _agent_from_request(),
                                    work_items, dependencies, session_id)


@mcp.tool()
def work_block(run_id: str, fencing_token: int, session_id: str, blocker: dict,
               checkpoint: Optional[dict] = None) -> dict:
    """Persist a blocker and release the active Run."""
    return service.block_work(run_id, fencing_token, _agent_from_request(),
                              blocker, checkpoint, session_id)


@mcp.tool()
def work_unblock(work_item_id: str, note: str = "",
                 coordinator_token: Optional[int] = None,
                 coordinator_session_id: Optional[str] = None) -> dict:
    return service.unblock_work(work_item_id, _agent_from_request(),
                                coordinator_token, note, coordinator_session_id)


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
    operators = set(get_config("operator_agent_ids", []))
    return service.decide_approval(approval_id, decision, agent_id,
                                   is_operator=agent_id in operators)


@mcp.tool()
def event_post(task_id: str, event_type: str, payload: Optional[dict] = None,
               work_item_id: Optional[str] = None, run_id: Optional[str] = None,
               recipients: Optional[list[str]] = None,
               idempotency_key: Optional[str] = None) -> dict:
    return service.post_event(task_id, _agent_from_request(), event_type, payload,
                              work_item_id, run_id, recipients, idempotency_key)


@mcp.tool()
def adapter_register(mode: str, config: Optional[dict] = None,
                     wake_level: str = "L2", adapter_id: Optional[str] = None) -> dict:
    return service.register_adapter(_agent_from_request(), mode, config,
                                    wake_level, adapter_id)


@mcp.tool()
def adapter_poll(adapter_id: str, limit: int = 20,
                 lease_seconds: Optional[int] = None) -> dict:
    return service.adapter_poll(_agent_from_request(), adapter_id, limit, lease_seconds)


@mcp.tool()
def adapter_ack(adapter_id: str, outbox_id: str, success: bool = True,
                error: str = "") -> dict:
    return service.adapter_ack(_agent_from_request(), adapter_id, outbox_id,
                               success, error)


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
def delivery_ack_batch(delivery_ids: list[str]) -> dict:
    """Idempotently acknowledge a bounded batch of deliveries."""
    return service.ack_deliveries(_agent_from_request(), delivery_ids)


@mcp.tool()
def hub_status() -> dict:
    """Runtime diagnostics."""
    _agent_from_request()
    return service.hub_status()


# ════════════════════════════════════════════════════════════════════
#  Lock tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def lock_acquire(lock_key: str, holder_run_id: str, run_fencing_token: int,
                 resource_type: str = "general", resource_id: str = "",
                 ttl_seconds: int = 600) -> dict:
    """Acquire a resource lock."""
    agent_id = _agent_from_request()
    return service.acquire_lock(lock_key, holder_run_id,
                                resource_type, resource_id, ttl_seconds, agent_id,
                                run_fencing_token)


@mcp.tool()
def lock_release(lock_key: str, holder_run_id: str, fencing_token: int) -> dict:
    """Release a resource lock."""
    agent_id = _agent_from_request()
    return service.release_lock(lock_key, holder_run_id, fencing_token, agent_id)


# ════════════════════════════════════════════════════════════════════
#  Entry point
# ════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    host = os.environ.get("AGENT_HUB_HOST", str(get_config("host", "127.0.0.1")))
    port = int(os.environ.get("AGENT_HUB_PORT", get_config("port", 8765)))
    mcp.run(transport="http", host=host, port=port, path="/mcp")


if __name__ == "__main__":
    main()
