"""Domain models for Agent Hub v2.

Task is the sole top-level business identity.
Work Item is a schedulable work unit within a task.
Run is one execution attempt of a work item by an agent session.
Session is a short-lived execution endpoint.
Checkpoint is a cross-session recovery snapshot.
Event is an immutable fact; Delivery is per-recipient state.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field


class Agent(BaseModel):
    id: str
    name: str
    capabilities: str = "[]"
    token_hash: str = ""
    is_active: bool = True
    last_heartbeat: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class Adapter(BaseModel):
    id: str
    agent_id: str
    mode: str = "manual_resume"  # resident_runner|cli_spawn|automation|online_session|manual_resume
    config_json: str = "{}"
    wake_level: str = "L2"  # L3|L2
    is_healthy: bool = True
    last_success_at: Optional[str] = None


class Session(BaseModel):
    id: str
    agent_id: str
    native_session_ref: Optional[str] = None
    adapter_id: Optional[str] = None
    capabilities_json: str = "[]"
    status: str = "active"  # active|ended|lost
    last_seen_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    lease_expires_at: str = ""
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ended_at: Optional[str] = None


class Task(BaseModel):
    id: str
    objective: str
    success_criteria_json: str = "[]"
    constraints_json: str = "{}"
    authorization_policy_json: str = "{}"
    context_refs_json: str = "[]"
    status: str = "draft"
    # draft|planned|ready|running|verifying|completed|failed|blocked|cancelled|archived
    priority: int = 0
    deadline_at: Optional[str] = None
    budget_json: str = "{}"
    plan_version: int = 1
    coordinator_run_id: Optional[str] = None
    created_by_agent_id: str = ""
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: Optional[str] = None


class WorkItem(BaseModel):
    id: str
    task_id: str
    parent_id: Optional[str] = None
    kind: str = "implement"  # plan|research|implement|review|verify|operate|summarize
    objective: str
    acceptance_json: str = "[]"
    required_capabilities_json: str = "[]"
    preferred_agent_id: Optional[str] = None
    status: str = "pending"
    # pending|ready|offered|running|reviewing|succeeded|failed|blocked|changes_requested|cancelled
    priority: int = 0
    retry_policy_json: str = '{"max_attempts":3}'
    needs_review: bool = False
    version: int = 1
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class WorkDependency(BaseModel):
    work_item_id: str
    depends_on_id: str
    condition: str = "succeeded"  # succeeded|failed|completed


class Run(BaseModel):
    id: str
    work_item_id: str
    attempt_no: int
    agent_id: str
    session_id: Optional[str] = None
    status: str = "offered"  # offered|claimed|running|succeeded|failed|lost|cancelled
    fencing_token: int
    lease_expires_at: Optional[str] = None
    heartbeat_at: Optional[str] = None
    checkpoint_id: Optional[str] = None
    failure_code: Optional[str] = None
    failure_json: str = "{}"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    started_at: Optional[str] = None
    ended_at: Optional[str] = None


class Checkpoint(BaseModel):
    id: str
    run_id: str
    version: int = 1
    snapshot_json: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class Artifact(BaseModel):
    id: str
    run_id: Optional[str] = None
    work_item_id: str
    task_id: str
    kind: str = "file"  # file|commit|report|doc|log
    ref: str
    hash: Optional[str] = None
    metadata_json: str = "{}"
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class Event(BaseModel):
    event_id: Optional[int] = None
    task_id: str
    work_item_id: Optional[str] = None
    run_id: Optional[str] = None
    event_type: str
    actor_agent_id: Optional[str] = None
    payload_json: str = "{}"
    idempotency_key: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class Delivery(BaseModel):
    id: str
    event_id: int
    recipient_kind: str  # session|run|agent|adapter
    recipient_id: str
    status: str = "pending"  # pending|acked|expired
    attempt_count: int = 0
    available_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    lease_expires_at: Optional[str] = None
    acked_at: Optional[str] = None


class Approval(BaseModel):
    id: str
    task_id: str
    work_item_id: Optional[str] = None
    run_id: Optional[str] = None
    action: str  # deploy|external_msg|pr|budget_increase|scope_change|conflict
    reason: str = ""
    decision: Optional[str] = None  # None=pending|approved|rejected
    decided_by: Optional[str] = None
    decided_at: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ResourceLock(BaseModel):
    id: str
    lock_key: str
    holder_run_id: str
    resource_type: str = "general"  # branch|file_group|server|general
    resource_id: str = ""
    fencing_token: int
    expires_at: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class HubError(Exception):
    """Domain error with a machine-readable code."""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)
