"""Boundary validation prevents malformed MCP payloads from reaching transactions."""
from __future__ import annotations

import json

import pytest

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def test_task_plan_rejects_non_object_work_item(db_path):
    task = service.create_task("Task", "agent-a")
    with pytest.raises(HubError) as exc:
        service.plan_task(task["id"], ["not-an-object"], actor_agent_id="agent-a")
    assert exc.value.code == "invalid_work_item"
    assert storage.count_work_items(get_db(), task["id"]) == 0


def test_retry_policy_object_remains_backward_compatible(db_path):
    task = service.create_task("Task", "agent-a")
    service.plan_task(
        task["id"],
        [{
            "kind": "implement",
            "objective": "work",
            "retry_policy": {"max_attempts": 5},
        }],
        actor_agent_id="agent-a",
    )
    work_item = get_db().execute(
        "SELECT retry_policy_json FROM work_items WHERE task_id=?",
        (task["id"],),
    ).fetchone()
    assert json.loads(work_item["retry_policy_json"])["max_attempts"] == 5


@pytest.mark.parametrize(
    "retry_fields",
    [
        {"retry_policy": {"max_attempts": 0}},
        {"retry_policy": {"max_attempts": 3, "jitter": float("nan")}},
        {
            "retry_policy": {"max_attempts": 3},
            "retry_policy_json": '{"max_attempts":3}',
        },
    ],
)
def test_invalid_retry_policies_are_rejected_without_mutation(
        db_path, retry_fields):
    task = service.create_task("Task", "agent-a")
    with pytest.raises(HubError) as exc:
        service.plan_task(
            task["id"],
            [{"kind": "implement", "objective": "work", **retry_fields}],
            actor_agent_id="agent-a",
        )
    assert exc.value.code == "invalid_work_item"
    assert storage.count_work_items(get_db(), task["id"]) == 0


def test_work_complete_rejects_artifact_without_ref_before_mutation(
        db_path, claim_and_start):
    claimed = claim_and_start()
    run = claimed["run"]
    run_id = run["id"]
    with pytest.raises(HubError) as exc:
        service.complete_run(
            run_id,
            run["fencing_token"],
            "succeeded",
            "agent-a",
            artifacts=[{"kind": "file"}],
            session_id=claimed["session_id"],
        )
    assert exc.value.code == "invalid_artifact"
    stored = storage.get_run(get_db(), run_id)
    assert stored.status == "running"


def test_event_type_and_payload_are_bounded(db_path):
    task = service.create_task("Task", "agent-a")
    with pytest.raises(HubError) as exc:
        service.post_event(task["id"], "agent-a", "Invalid Event", {})
    assert exc.value.code == "invalid_event_type"

    with pytest.raises(HubError) as exc:
        service.post_event(
            task["id"], "agent-a", "custom.large", {"value": "x" * 70_000}
        )
    assert exc.value.code == "event_payload_too_large"


def test_approval_has_expiry(db_path):
    task = service.create_task("Approval task", "agent-a")
    result = service.request_approval(
        task["id"], "external_change", requested_by="agent-a"
    )
    assert result["expires_at"]
