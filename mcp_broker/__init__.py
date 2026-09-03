"""In-pod MCP tool broker.

A single, lazy, retriable interface between the agent (the claude-code CLI) and
the workspace-tool sidecars. The CLI connects to the always-up broker instead of
to each sidecar, which lets the sidecars start in parallel with the main
container instead of gating its boot. See :mod:`mcp_broker.broker`.
"""

from __future__ import annotations

from mcp_broker.broker import BROKER_PATH_PREFIX, ToolBroker, broker_route
from mcp_broker.dialer import Dialer, UpstreamConn, default_dialer
from mcp_broker.metrics import NULL_METRICS, BrokerMetrics, NullMetrics
from mcp_broker.roster import parse_roster
from mcp_broker.upstream import Upstream

__all__ = [
    "BROKER_PATH_PREFIX",
    "NULL_METRICS",
    "BrokerMetrics",
    "Dialer",
    "NullMetrics",
    "ToolBroker",
    "Upstream",
    "UpstreamConn",
    "broker_route",
    "default_dialer",
    "parse_roster",
]
