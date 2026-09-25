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
import json
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class WorkItemSpec(BaseModel):
    """Validated public input for task planning and child work creation."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["plan", "research", "implement", "review", "verify", "operate", "summarize"]
    objective: str = Field(min_length=1, max_length=20_000)
    ref: Optional[str] = Field(default=None, min_length=1, max_length=256)
    parent_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    acceptance: list[Any] = Field(default_factory=list, max_length=200)
    required_capabilities: list[str] = Field(default_factory=list, max_length=100)
    preferred_agent_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    priority: int = Field(default=0, ge=-1000, le=1000)
    # Public object form is normalized to the storage-facing JSON string.
    retry_policy: Optional[dict[str, Any]] = None
    retry_policy_json: Optional[str] = None
    needs_review: bool = False
    depth: int = Field(default=0, ge=0, le=100)

    @model_validator(mode="after")
    def normalize_retry_policy(self):
        if self.retry_policy is not None and self.retry_policy_json is not None:
            raise ValueError("provide retry_policy or retry_policy_json, not both")
        if self.retry_policy is not None:
            policy = self.retry_policy
        elif self.retry_policy_json is not None:
            try:
                policy = json.loads(self.retry_policy_json)
            except (TypeError, json.JSONDecodeError) as exc:
                raise ValueError("retry_policy_json must be valid JSON") from exc
        else:
            policy = {"max_attempts": 3}
        if not isinstance(policy, dict):
            raise ValueError("retry policy must be an object")
        attempts = policy.get("max_attempts", 3)
        if not isinstance(attempts, int) or isinstance(attempts, bool) or not 1 <= attempts <= 100:
            raise ValueError("max_attempts must be an integer between 1 and 100")
        try:
            self.retry_policy_json = json.dumps(
                policy, separators=(",", ":"), allow_nan=False
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("retry policy must be JSON serializable") from exc
        self.retry_policy = None
        return self


class DependencySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    work_item: str = Field(min_length=1, max_length=256)
    depends_on: str = Field(min_length=1, max_length=256)
    condition: Literal["succeeded", "failed", "completed"] = "succeeded"


class ArtifactSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ref: str = Field(min_length=1, max_length=4_096)
    kind: str = Field(default="file", min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    hash: Optional[str] = Field(default=None, max_length=256)
    metadata: dict[str, Any] = Field(default_factory=dict)


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
    last_error: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


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
    coordinator_agent_id: Optional[str] = None
    coordinator_session_id: Optional[str] = None
    coordinator_fencing_token: Optional[int] = None
    coordinator_lease_expires_at: Optional[str] = None
    blocked_reason_json: str = "{}"
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
    # pending|ready|offered|running|reviewing|succeeded|failed|blocked|cancelled
    priority: int = 0
    retry_policy_json: str = '{"max_attempts":3}'
    needs_review: bool = False
    depth: int = 0
    blocked_reason_json: str = "{}"
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
    status: str = "offered"  # offered|claimed|running|succeeded|failed|blocked|lost|cancelled
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
    observed_at: Optional[str] = None
    archived_at: Optional[str] = None


class Approval(BaseModel):
    id: str
    task_id: str
    work_item_id: Optional[str] = None
    run_id: Optional[str] = None
    action: str  # deploy|external_msg|pr|budget_increase|scope_change|conflict
    reason: str = ""
    decision: Optional[str] = None  # None=pending|approved|rejected
    previous_task_status: Optional[str] = None
    previous_work_status: Optional[str] = None
    decided_by: Optional[str] = None
    decided_at: Optional[str] = None
    expires_at: Optional[str] = None
    reminder_count: int = 0
    last_reminded_at: Optional[str] = None
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
