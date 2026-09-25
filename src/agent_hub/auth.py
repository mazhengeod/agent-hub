"""Authentication: Bearer token verification and agent identity.

Tokens are cached in memory; reloaded when the token file changes.
Bearer token identifies the agent, NOT the session. Session and run
authorization is enforced separately in the service layer.
"""
from __future__ import annotations

import hashlib
import hmac
import os
from pathlib import Path
from typing import Optional

CONFIG_DIR = Path.home() / ".config" / "agent-hub"
TOKEN_FILE = CONFIG_DIR / "agents.env"

_tokens_cache: Optional[dict[str, str]] = None
_tokens_signature: tuple[int, int] = (0, 0)


def load_tokens() -> dict[str, str]:
    """Load agent tokens from agents.env, cached with mtime check."""
    global _tokens_cache, _tokens_signature
    try:
        stat_result = TOKEN_FILE.stat() if TOKEN_FILE.exists() else None
        signature = (
            (stat_result.st_mtime_ns, stat_result.st_size)
            if stat_result else (0, 0)
        )
    except OSError:
        signature = (0, 0)

    if _tokens_cache is not None and signature == _tokens_signature:
        return _tokens_cache

    tokens = {}
    if TOKEN_FILE.exists():
        if os.name == "posix" and TOKEN_FILE.stat().st_mode & 0o077:
            raise PermissionError(
                f"Token file must not be group/world accessible: {TOKEN_FILE}"
            )
        with open(TOKEN_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    agent_id, token = line.split("=", 1)
                    tokens[agent_id.strip()] = token.strip().strip('"').strip("'")

    _tokens_cache = tokens
    _tokens_signature = signature
    return tokens


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:32]


def verify_token(bearer_token: str) -> Optional[str]:
    """Verify a Bearer token and return the agent_id, or None."""
    if not bearer_token or not bearer_token.startswith("Bearer "):
        return None
    raw_token = bearer_token[7:]
    tokens = load_tokens()
    for agent_id, expected in tokens.items():
        if hmac.compare_digest(raw_token, expected):
            return agent_id
    return None
