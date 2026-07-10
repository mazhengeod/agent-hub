"""Trusted adapter runner for L3 agent integrations.

The runner never executes database-provided shell commands. It imports one
operator-configured Python callable and gives it leased outbox entries. The
callable owns the product-specific wake/start logic for Codex, Hermes, Trae,
or another runtime.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import logging
import os
from collections.abc import Callable
from typing import Any

from fastmcp import Client

log = logging.getLogger("agent_hub.runner")


def load_handler(spec: str) -> Callable[[dict], Any]:
    """Load a trusted `module:function` adapter handler."""
    if ":" not in spec:
        raise ValueError("handler must use module:function syntax")
    module_name, attr = spec.split(":", 1)
    if not module_name or not attr:
        raise ValueError("handler must use module:function syntax")
    handler = getattr(importlib.import_module(module_name), attr)
    if not callable(handler):
        raise TypeError(f"{spec} is not callable")
    return handler


def interpret_handler_result(value: Any) -> tuple[bool, str]:
    if value is None or value is True:
        return True, ""
    if value is False:
        return False, "handler returned false"
    if isinstance(value, dict):
        return bool(value.get("success", False)), str(value.get("error", ""))
    raise TypeError("handler must return None, bool, or {'success': bool, 'error': str}")


async def invoke_handler(handler: Callable[[dict], Any], entry: dict) -> tuple[bool, str]:
    try:
        result = handler(entry)
        if inspect.isawaitable(result):
            result = await result
        return interpret_handler_result(result)
    except Exception as exc:  # adapter failures are reported to the durable outbox
        log.exception("Adapter handler failed for outbox %s", entry.get("id"))
        return False, f"{type(exc).__name__}: {exc}"


async def process_once(client: Client, adapter_id: str,
                       handler: Callable[[dict], Any], limit: int = 20,
                       lease_seconds: int = 60) -> int:
    response = await client.call_tool(
        "adapter_poll",
        {"adapter_id": adapter_id, "limit": limit,
         "lease_seconds": lease_seconds},
    )
    if not isinstance(response, dict):
        raise RuntimeError("adapter_poll returned an unexpected payload")
    entries = response.get("entries", [])
    for entry in entries:
        success, error = await invoke_handler(handler, entry)
        await client.call_tool(
            "adapter_ack",
            {"adapter_id": adapter_id, "outbox_id": entry["id"],
             "success": success, "error": error},
        )
    return len(entries)


async def run_loop(url: str, token: str, adapter_id: str,
                   handler: Callable[[dict], Any], interval: float = 2.0,
                   limit: int = 20, lease_seconds: int = 60) -> None:
    async with Client(url, auth=token) as client:
        await client.call_tool(
            "adapter_register",
            {"mode": "resident_runner", "wake_level": "L3",
             "adapter_id": adapter_id,
             "config": {"runner": "agent_hub.runner"}},
        )
        while True:
            processed = await process_once(
                client, adapter_id, handler, limit, lease_seconds)
            if processed == 0:
                await asyncio.sleep(interval)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Agent Hub trusted adapter runner")
    parser.add_argument("--url", default="http://127.0.0.1:8765/mcp")
    parser.add_argument("--adapter-id", required=True)
    parser.add_argument("--handler", required=True, help="trusted module:function")
    parser.add_argument("--token-env", default="AGENT_HUB_TOKEN")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--lease-seconds", type=int, default=60)
    args = parser.parse_args(argv)

    token = os.environ.get(args.token_env, "")
    if not token:
        parser.error(f"environment variable {args.token_env} is empty")
    handler = load_handler(args.handler)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    try:
        asyncio.run(run_loop(
            args.url, token, args.adapter_id, handler, args.interval,
            args.limit, args.lease_seconds))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

