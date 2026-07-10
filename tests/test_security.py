"""Test security: session ownership, cross-agent auth, multi-session."""
from __future__ import annotations

import pytest

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def test_multiple_active_sessions_for_same_agent(db_path):
    """P0 fix: same agent can have multiple active sessions."""
    s1 = service.session_start("agent-a")
    s2 = service.session_start("agent-a")
    assert s1["session_id"] != s2["session_id"]

    conn = get_db()
    sessions = storage.list_active_sessions_for_agent(conn, "agent-a")
    assert len(sessions) == 2
    assert all(s.status == "active" for s in sessions)


def test_session_heartbeat_wrong_agent_rejected(db_path):
    """Agent B cannot heartbeat agent A's session."""
    sess = service.session_start("agent-a")
    with pytest.raises(HubError) as exc:
        service.session_heartbeat("agent-b", sess["session_id"])
    assert exc.value.code == "session_not_owned"


def test_session_end_wrong_agent_rejected(db_path):
    sess = service.session_start("agent-a")
    with pytest.raises(HubError) as exc:
        service.session_end("agent-b", sess["session_id"])
    assert exc.value.code == "session_not_owned"


def test_claim_work_task_not_running(db_path):
    """P0: cannot claim work when task is not running."""
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1"},
    ], actor_agent_id="agent-a")
    # Task is 'planned', NOT 'running'

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    sess = service.session_start("agent-a")
    with pytest.raises(HubError) as exc:
        service.claim_work("agent-a", sess["session_id"], work_item_id=wi.id)
    assert exc.value.code == "task_not_running"


def test_claim_work_wrong_agent_when_preferred(db_path):
    """preferred_agent_id enforcement."""
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1",
         "preferred_agent_id": "agent-a"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    wi = storage.list_work_items(conn, task["id"])[0]

    sess_b = service.session_start("agent-b")
    with pytest.raises(HubError) as exc:
        service.claim_work("agent-b", sess_b["session_id"], work_item_id=wi.id)
    assert exc.value.code == "agent_not_preferred"


def test_claim_work_capability_check(db_path):
    """required_capabilities enforced against session capabilities."""
    task = service.create_task("Task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "work", "ref": "wi1",
         "required_capabilities": ["python"]},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    sess = service.session_start("agent-a")  # no capabilities
    with pytest.raises(HubError) as exc:
        service.claim_work("agent-a", sess["session_id"])
    assert exc.value.code == "capability_missing"

    sess2 = service.session_start("agent-a", capabilities=["python"])
    claimed = service.claim_work("agent-a", sess2["session_id"])
    assert claimed is not None


def test_start_run_wrong_agent_rejected(db_path, make_task, make_session):
    """Agent B cannot start agent A's run."""
    task_id = make_task()
    sess = make_session("agent-a")
    claimed = service.claim_work("agent-a", sess["session_id"])
    assert claimed is not None

    sess_b = make_session("agent-b")
    with pytest.raises(HubError) as exc:
        service.start_run(claimed["run_id"], claimed["fencing_token"],
                          sess_b["session_id"], "agent-b")
    assert exc.value.code == "run_not_owned"


def test_heartbeat_wrong_agent_rejected(db_path, claim_and_start):
    result = claim_and_start("agent-a")
    run_id = result["run"]["id"]
    token = result["run"]["fencing_token"]

    with pytest.raises(HubError) as exc:
        service.heartbeat_run(run_id, token, "agent-b")
    assert exc.value.code == "run_not_owned"


def test_complete_run_wrong_agent_rejected(db_path, claim_and_start):
    result = claim_and_start("agent-a")
    run_id = result["run"]["id"]
    token = result["run"]["fencing_token"]

    with pytest.raises(HubError) as exc:
        service.complete_run(run_id, token, "succeeded", "agent-b")
    assert exc.value.code == "run_not_owned"


def test_checkpoint_wrong_agent_rejected(db_path, claim_and_start):
    result = claim_and_start("agent-a")
    run_id = result["run"]["id"]
    token = result["run"]["fencing_token"]

    with pytest.raises(HubError) as exc:
        service.save_checkpoint(run_id, token, {"x": 1}, "agent-b")
    assert exc.value.code == "run_not_owned"


def test_stale_fencing_token_rejected(db_path, claim_and_start):
    result = claim_and_start("agent-a")
    run_id = result["run"]["id"]
    token = result["run"]["fencing_token"]

    with pytest.raises(HubError) as exc:
        service.complete_run(run_id, token + 999, "succeeded", "agent-a")
    assert exc.value.code == "stale_token"


def test_complete_run_invalid_status(db_path, claim_and_start):
    """Status whitelist enforcement."""
    result = claim_and_start("agent-a")
    run_id = result["run"]["id"]
    token = result["run"]["fencing_token"]

    with pytest.raises(HubError) as exc:
        service.complete_run(run_id, token, "cancelled", "agent-a")
    assert exc.value.code == "invalid_status"
