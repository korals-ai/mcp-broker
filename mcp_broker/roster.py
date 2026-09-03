"""Parse the operator-stamped ``WORKSPACE_MCP_SERVERS`` roster.

Lightweight on purpose — json + os only, NO ``claude_agent_sdk`` import — so the
pod startup path can build the broker without dragging in the SDK's heavy import
graph (which is deferred to first chat attach to keep cold ``/healthz`` fast).
:mod:`src.mcp_config` reuses this for the SDK-facing map.
"""

from __future__ import annotations

import json
import logging
import os

WORKSPACE_MCP_SERVERS_ENV = "WORKSPACE_MCP_SERVERS"

log = logging.getLogger("workspace.tool_broker")


def parse_roster(
    raw: str | None = None, *, env_var: str = WORKSPACE_MCP_SERVERS_ENV
) -> list[tuple[str, str]]:
    """Return the upstream roster as ``[(name, real_url), ...]``.

    ``raw`` defaults to the value of ``env_var``; pass either explicitly. The
    default env name is the one this package's original deployment stamps —
    override it rather than adopting it, or skip this helper entirely and build
    the ``[(name, url), ...]`` list yourself, which is all the broker wants.

    Empty/absent/malformed => ``[]`` (no upstreams): a bad roster must degrade
    to "tools absent", never raise.
    """
    if raw is None:
        raw = os.environ.get(env_var, "")
    raw = raw.strip()
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("ignoring malformed %s (not JSON)", WORKSPACE_MCP_SERVERS_ENV)
        return []

    if not isinstance(parsed, list):
        log.warning(
            "ignoring %s: expected a JSON array, got %s",
            WORKSPACE_MCP_SERVERS_ENV,
            type(parsed).__name__,
        )
        return []

    roster: list[tuple[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            log.warning("skipping non-object entry in %s: %r", WORKSPACE_MCP_SERVERS_ENV, item)
            continue
        name = item.get("name")
        url = item.get("url")
        if not name or not url:
            log.warning("skipping %s entry missing name/url: %r", WORKSPACE_MCP_SERVERS_ENV, item)
            continue
        roster.append((str(name), str(url)))
    return roster
