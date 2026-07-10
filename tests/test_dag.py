"""Test DAG constraints: cycle detection, same-task, self-dependency."""
from __future__ import annotations

import pytest

from agent_hub import service, storage
from agent_hub.db import get_db
from agent_hub.models import HubError


def test_dag_cycle_rejected(db_path):
    """A -> B -> C -> A must be rejected."""
    task = service.create_task("Cycle task", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "A", "ref": "a"},
        {"kind": "implement", "objective": "B", "ref": "b"},
        {"kind": "implement", "objective": "C", "ref": "c"},
    ], dependencies=[
        {"work_item": "b", "depends_on": "a"},
        {"work_item": "c", "depends_on": "b"},
    ], actor_agent_id="agent-a")

    conn = get_db()
    items = {wi.objective: wi for wi in storage.list_work_items(conn, task["id"])}
    a_id = items["A"].id
    b_id = items["B"].id
    c_id = items["C"].id

    with pytest.raises(HubError) as exc:
        service.plan_task(task["id"], [
            {"kind": "implement", "objective": "A", "ref": "a"},
            {"kind": "implement", "objective": "B", "ref": "b"},
            {"kind": "implement", "objective": "C", "ref": "c"},
        ], dependencies=[
            {"work_item": "b", "depends_on": "a"},
            {"work_item": "c", "depends_on": "b"},
            {"work_item": "a", "depends_on": "c"},
        ], actor_agent_id="agent-a")
    assert exc.value.code == "dag_cycle"


def test_dag_self_dependency_rejected(db_path):
    task = service.create_task("Self dep", "agent-a")
    with pytest.raises(HubError) as exc:
        service.plan_task(task["id"], [
            {"kind": "implement", "objective": "A", "ref": "a"},
        ], dependencies=[
            {"work_item": "a", "depends_on": "a"},
        ], actor_agent_id="agent-a")
    assert exc.value.code == "dag_self_dependency"


def test_dag_linear_chain_advances(db_path, make_session):
    """A -> B -> C: B ready after A succeeds, C ready after B succeeds."""
    sess = make_session("agent-a")
    task = service.create_task("Chain", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "A", "ref": "a"},
        {"kind": "implement", "objective": "B", "ref": "b"},
        {"kind": "implement", "objective": "C", "ref": "c"},
    ], dependencies=[
        {"work_item": "b", "depends_on": "a"},
        {"work_item": "c", "depends_on": "b"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    items = {wi.objective: wi for wi in storage.list_work_items(conn, task["id"])}
    assert items["A"].status == "ready"
    assert items["B"].status == "pending"
    assert items["C"].status == "pending"

    c = service.claim_work("agent-a", sess["session_id"], work_item_id=items["A"].id)
    service.start_run(c["run_id"], c["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c["run_id"], c["fencing_token"], "succeeded", "agent-a")

    items = {wi.objective: wi for wi in storage.list_work_items(conn, task["id"])}
    assert items["A"].status == "succeeded"
    assert items["B"].status == "ready"
    assert items["C"].status == "pending"


def test_dependency_condition_failed(db_path, make_session):
    """Dep with condition='failed' advances when dep fails."""
    sess = make_session("agent-a")
    task = service.create_task("Cond", "agent-a")
    service.plan_task(task["id"], [
        {"kind": "implement", "objective": "A", "ref": "a",
         "retry_policy_json": '{"max_attempts": 1}'},
        {"kind": "implement", "objective": "Fallback", "ref": "fb"},
    ], dependencies=[
        {"work_item": "fb", "depends_on": "a", "condition": "failed"},
    ], actor_agent_id="agent-a")
    service.start_task(task["id"], "agent-a")

    conn = get_db()
    items = {wi.objective: wi for wi in storage.list_work_items(conn, task["id"])}
    assert items["A"].status == "ready"
    assert items["Fallback"].status == "pending"

    c = service.claim_work("agent-a", sess["session_id"], work_item_id=items["A"].id)
    service.start_run(c["run_id"], c["fencing_token"], sess["session_id"], "agent-a")
    service.complete_run(c["run_id"], c["fencing_token"], "failed", "agent-a",
                         failure_code="err")

    items = {wi.objective: wi for wi in storage.list_work_items(conn, task["id"])}
    assert items["A"].status == "failed"
    assert items["Fallback"].status == "ready"
