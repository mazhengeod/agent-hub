"""hubctl: operator CLI for Agent Hub.

Commands:
  hubctl status           - show hub diagnostics
  hubctl tasks            - list tasks
  hubctl task <id>        - show task detail with work items
  hubctl reconcile        - run scheduler reconcile manually
  hubctl agents           - list registered agents
  hubctl approvals        - list pending approvals
  hubctl approve <id>     - approve a pending approval
  hubctl reject <id>      - reject a pending approval
"""
from __future__ import annotations

import sys
import json
from typing import Optional

from . import service
from .db import init_db, get_db
from . import storage


def main(argv: Optional[list[str]] = None):
    argv = argv or sys.argv[1:]
    if not argv:
        print(__doc__)
        return 0

    cmd = argv[0]
    init_db()

    if cmd == "status":
        status = service.hub_status()
        print(f"Agent Hub v{status['version']}")
        print(f"DB: {status['db_path']}")
        print(f"Migrations: {status['migrations']}")
        print(f"Counts: {json.dumps(status['counts'], indent=2)}")
        print(f"Health: {json.dumps(status['health'], indent=2)}")

    elif cmd == "tasks":
        status_filter = argv[1] if len(argv) > 1 else None
        tasks = service.list_tasks(status=status_filter)
        if not tasks:
            print("No tasks found.")
        for t in tasks:
            print(f"  {t['id']}  [{t['status']}]  pri={t['priority']}  {t['objective'][:60]}")

    elif cmd == "task":
        if len(argv) < 2:
            print("Usage: hubctl task <id>")
            return 1
        task = service.get_task(argv[1])
        if not task:
            print(f"Task {argv[1]} not found")
            return 1
        print(json.dumps(task, indent=2))
        conn = get_db()
        items = storage.list_work_items(conn, argv[1])
        print(f"\nWork items ({len(items)}):")
        for wi in items:
            print(f"  {wi.id}  [{wi.status}]  v{wi.version}  {wi.kind}: {wi.objective[:50]}")
        runs = conn.execute(
            "SELECT * FROM runs WHERE work_item_id IN "
            "(SELECT id FROM work_items WHERE task_id=?) ORDER BY attempt_no",
            (argv[1],),
        ).fetchall()
        if runs:
            print(f"\nRuns ({len(runs)}):")
            for r in runs:
                print(f"  {r['id']}  [{r['status']}]  attempt={r['attempt_no']}  "
                      f"agent={r['agent_id']}  token={r['fencing_token']}")

    elif cmd == "reconcile":
        result = service.reconcile()
        print(f"Reconciled: {json.dumps(result, indent=2)}")

    elif cmd == "agents":
        conn = get_db()
        agents = storage.list_agents(conn)
        for a in agents:
            print(f"  {a.id}  active={a.is_active}  hb={a.last_heartbeat}  {a.name}")

    elif cmd == "approvals":
        conn = get_db()
        approvals = storage.list_pending_approvals(conn)
        if not approvals:
            print("No pending approvals.")
        for a in approvals:
            print(f"  {a.id}  task={a.task_id}  action={a.action}  reason={a.reason}")

    elif cmd in ("approve", "reject"):
        if len(argv) < 2:
            print(f"Usage: hubctl {cmd} <approval_id>")
            return 1
        decision = "approved" if cmd == "approve" else "rejected"
        result = service.decide_approval(argv[1], decision, "hubctl", is_operator=True)
        print(json.dumps(result, indent=2))

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
