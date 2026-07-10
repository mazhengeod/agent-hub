"""Trusted runner helpers."""
from __future__ import annotations

import asyncio

import pytest

from agent_hub.runner import interpret_handler_result, invoke_handler, load_handler


def sample_handler(entry):
    return {"success": entry["ok"], "error": "no" if not entry["ok"] else ""}


def test_load_handler():
    handler = load_handler("test_runner:sample_handler")
    assert handler({"ok": True})["success"] is True


def test_load_handler_rejects_bad_spec():
    with pytest.raises(ValueError):
        load_handler("missing_separator")


def test_interpret_handler_result():
    assert interpret_handler_result(None) == (True, "")
    assert interpret_handler_result(False)[0] is False
    assert interpret_handler_result({"success": False, "error": "boom"}) == (False, "boom")


def test_invoke_handler_captures_exception():
    def broken(_entry):
        raise RuntimeError("boom")

    success, error = asyncio.run(invoke_handler(broken, {"id": "x"}))
    assert success is False
    assert "RuntimeError" in error
