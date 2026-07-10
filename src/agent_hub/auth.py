"""Authentication: Bearer token verification and agent identity.

Tokens are cached in memory; reloaded when the token file changes.
Bearer token identifies the agent, NOT the session. Session and run
authorization is enforced separately in the service layer.
"""
from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Optional

CONFIG_DIR = Path.home() / ".config" / "agent-hub"
TOKEN_FILE = CONFIG_DIR / "agents.env"

_tokens_cache: Optional[dict[str, str]] = None
_tokens_mtime: float = 0.0


def load_tokens() -> dict[str, str]:
    """Load agent tokens from agents.env, cached with mtime check."""
    global _tokens_cache, _tokens_mtime
    try:
        mtime = TOKEN_FILE.stat().st_mtime if TOKEN_FILE.exists() else 0.0
    except OSError:
        mtime = 0.0

    if _tokens_cache is not None and mtime == _tokens_mtime:
        return _tokens_cache

    tokens = {}
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    agent_id, token = line.split("=", 1)
                    tokens[agent_id.strip()] = token.strip().strip('"').strip("'")

    _tokens_cache = tokens
    _tokens_mtime = mtime
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
        if raw_token == expected:
            return agent_id
    return None
