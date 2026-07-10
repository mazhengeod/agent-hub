"""FastMCP server: exposes Agent Hub tools to MCP clients.

Tool categories:
- session:  session_start, session_heartbeat, session_end
- task:     task_create, task_get, task_list, task_plan, task_start
- work:     work_claim, work_start, work_progress, work_checkpoint, work_complete, work_resume
- review:   work_review, approval_request, approval_decide
- sync:     agent_sync, hub_status
- lock:     lock_acquire, lock_release
"""
from __future__ import annotations

import json
from typing import Optional

from fastmcp import FastMCP

from . import service
from .auth import verify_token
from .models import HubError

mcp = FastMCP("agent-hub")


# ── Auth context ───────────────────────────────────────────────────

def _agent_from_token(bearer_token: str) -> str:
    agent_id = verify_token(bearer_token)
    if not agent_id:
        raise HubError("unauthorized", "Invalid or missing Bearer token")
    return agent_id


# ════════════════════════════════════════════════════════════════════
#  Session tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def session_start(bearer_token: str, native_session_ref: Optional[str] = None,
                  capabilities: Optional[list] = None,
                  adapter_id: Optional[str] = None,
                  lease_seconds: Optional[int] = None) -> dict:
    """Start a new agent session. Ends any previous active session for this agent."""
    agent_id = _agent_from_token(bearer_token)
    return service.session_start(agent_id, native_session_ref, capabilities,
                                 adapter_id, lease_seconds)


@mcp.tool()
def session_heartbeat(bearer_token: str, session_id: str,
                      lease_seconds: Optional[int] = None) -> dict:
    """Renew session lease to prevent expiry."""
    _agent_from_token(bearer_token)
    return service.session_heartbeat(session_id, lease_seconds)


@mcp.tool()
def session_end(bearer_token: str, session_id: str) -> dict:
    """End a session."""
    _agent_from_token(bearer_token)
    return service.session_end(session_id)


# ════════════════════════════════════════════════════════════════════
#  Task tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def task_create(bearer_token: str, objective: str,
                success_criteria: Optional[list] = None,
                constraints: Optional[dict] = None,
                authorization_policy: Optional[dict] = None,
                context_refs: Optional[list] = None,
                priority: int = 0,
                deadline_at: Optional[str] = None,
                budget: Optional[dict] = None) -> dict:
    """Create a new task. Returns task with id."""
    agent_id = _agent_from_token(bearer_token)
    return service.create_task(objective, agent_id, success_criteria,
                               constraints, authorization_policy, context_refs,
                               priority, deadline_at, budget)


@mcp.tool()
def task_get(bearer_token: str, task_id: str) -> dict:
    """Get task details."""
    _agent_from_token(bearer_token)
    result = service.get_task(task_id)
    if not result:
        raise HubError("task_not_found", f"Task {task_id} not found")
    return result


@mcp.tool()
def task_list(bearer_token: str, status: Optional[str] = None,
              limit: int = 50) -> list[dict]:
    """List tasks, optionally filtered by status."""
    _agent_from_token(bearer_token)
    return service.list_tasks(status=status, limit=limit)


@mcp.tool()
def task_plan(bearer_token: str, task_id: str,
              work_items: list[dict],
              dependencies: Optional[list[dict]] = None) -> dict:
    """Create work items and dependencies for a task.

    work_items: [{"kind":"implement", "objective":"...", "ref":"wi1",
                   "needs_review":true, "priority":1}]
    dependencies: [{"work_item":"wi1", "depends_on":"wi2", "condition":"succeeded"}]
    Use 'ref' to reference work items in dependencies before they have IDs.
    """
    agent_id = _agent_from_token(bearer_token)
    return service.plan_task(task_id, work_items, dependencies, agent_id)


@mcp.tool()
def task_start(bearer_token: str, task_id: str) -> dict:
    """Transition a planned task to running."""
    agent_id = _agent_from_token(bearer_token)
    return service.start_task(task_id, agent_id)


# ════════════════════════════════════════════════════════════════════
#  Work + Run tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def work_claim(bearer_token: str, session_id: str,
               work_item_id: Optional[str] = None,
               lease_seconds: Optional[int] = None) -> Optional[dict]:
    """Claim a work item for execution. If work_item_id is omitted,
    picks the highest-priority ready work item.
    Returns a Run with fencing_token, or None if no work available."""
    agent_id = _agent_from_token(bearer_token)
    return service.claim_work(agent_id, session_id, work_item_id, lease_seconds)


@mcp.tool()
def work_start(bearer_token: str, run_id: str, fencing_token: int,
               session_id: str, lease_seconds: Optional[int] = None) -> dict:
    """Transition a claimed run to running."""
    _agent_from_token(bearer_token)
    return service.start_run(run_id, fencing_token, session_id, lease_seconds)


@mcp.tool()
def work_progress(bearer_token: str, run_id: str, fencing_token: int,
                  lease_seconds: Optional[int] = None) -> dict:
    """Heartbeat a running run to renew its lease."""
    _agent_from_token(bearer_token)
    return service.heartbeat_run(run_id, fencing_token, lease_seconds)


@mcp.tool()
def work_checkpoint(bearer_token: str, run_id: str, fencing_token: int,
                    snapshot: dict) -> dict:
    """Save a recovery checkpoint for a run.

    snapshot: {"completed_steps":[], "decisions":[], "changed_files":[],
               "tests":[], "risks":[], "next_steps":[], "artifacts":[]}
    """
    _agent_from_token(bearer_token)
    return service.save_checkpoint(run_id, fencing_token, snapshot)


@mcp.tool()
def work_complete(bearer_token: str, run_id: str, fencing_token: int,
                  status: str,
                  artifacts: Optional[list] = None,
                  failure_code: Optional[str] = None,
                  failure_detail: Optional[dict] = None) -> dict:
    """Complete a run. status must be 'succeeded' or 'failed'.

    artifacts: [{"kind":"file", "ref":"path/to/file", "hash":"sha256"}]
    """
    agent_id = _agent_from_token(bearer_token)
    return service.complete_run(run_id, fencing_token, status, artifacts,
                                failure_code, failure_detail, agent_id)


@mcp.tool()
def work_resume(bearer_token: str, run_id: str, session_id: str,
                lease_seconds: Optional[int] = None) -> dict:
    """Resume a lost run. Creates a new attempt and returns the
    latest checkpoint for recovery."""
    agent_id = _agent_from_token(bearer_token)
    return service.resume_run(run_id, session_id, agent_id, lease_seconds)


# ════════════════════════════════════════════════════════════════════
#  Review + Approval tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def work_review(bearer_token: str, work_item_id: str,
                decision: str, comment: str = "") -> dict:
    """Approve or reject a work item in 'reviewing' state.

    decision: 'approved' or 'rejected'
    """
    agent_id = _agent_from_token(bearer_token)
    return service.approve_work(work_item_id, agent_id, decision, comment)


@mcp.tool()
def approval_request(bearer_token: str, task_id: str, action: str,
                     reason: str = "",
                     work_item_id: Optional[str] = None,
                     run_id: Optional[str] = None) -> dict:
    """Request operator approval for an action.

    action: deploy|external_msg|pr|budget_increase|scope_change|conflict
    """
    agent_id = _agent_from_token(bearer_token)
    return service.request_approval(task_id, action, reason,
                                    work_item_id, run_id, agent_id)


@mcp.tool()
def approval_decide(bearer_token: str, approval_id: str,
                    decision: str) -> dict:
    """Decide a pending approval. decision: 'approved' or 'rejected'."""
    agent_id = _agent_from_token(bearer_token)
    return service.decide_approval(approval_id, decision, agent_id)


# ════════════════════════════════════════════════════════════════════
#  Sync + Status tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def agent_sync(bearer_token: str, session_id: str,
               since_event_id: int = 0) -> dict:
    """Batch pull: heartbeat + pending work + deliveries + active runs.

    Call this every poll cycle (recommended 5-10s) to stay alive and
    discover new work. Returns:
    - ready_work: work items available to claim
    - active_runs: runs this agent is executing
    - offered_runs: runs offered to this agent
    - deliveries: pending event deliveries
    - pending_approvals: approvals awaiting decision
    - latest_event_id: watermark for incremental sync
    """
    agent_id = _agent_from_token(bearer_token)
    return service.agent_sync(agent_id, session_id, since_event_id)


@mcp.tool()
def hub_status(bearer_token: str) -> dict:
    """Runtime diagnostics: version, counts, lease health, migrations."""
    _agent_from_token(bearer_token)
    return service.hub_status()


# ════════════════════════════════════════════════════════════════════
#  Lock tools
# ════════════════════════════════════════════════════════════════════

@mcp.tool()
def lock_acquire(bearer_token: str, lock_key: str, holder_run_id: str,
                 resource_type: str = "general", resource_id: str = "",
                 ttl_seconds: int = 600) -> dict:
    """Acquire a resource lock. Raises lock_busy if held by another run."""
    _agent_from_token(bearer_token)
    return service.acquire_lock(lock_key, holder_run_id,
                                resource_type, resource_id, ttl_seconds)


@mcp.tool()
def lock_release(bearer_token: str, lock_key: str, holder_run_id: str,
                 fencing_token: int) -> dict:
    """Release a resource lock. Requires correct fencing token."""
    _agent_from_token(bearer_token)
    return service.release_lock(lock_key, holder_run_id, fencing_token)


# ════════════════════════════════════════════════════════════════════
#  Entry point
# ════════════════════════════════════════════════════════════════════

def main():
    from .db import init_db
    init_db()
    mcp.run(transport="http", host="127.0.0.1", port=8765, path="/mcp")


if __name__ == "__main__":
    main()
