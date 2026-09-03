"""How the broker reaches an upstream sidecar over MCP.

A :data:`Dialer` opens a short-lived MCP client connection to a sidecar's
``/mcp`` endpoint and yields an :class:`UpstreamConn` for one operation
(list its tools, or call one). The connection is opened per operation and
torn down after — streamable-HTTP is request-shaped, and a fresh session per
call is what makes the broker naturally resilient to a sidecar restarting
between calls (no stale connection to detect and discard).

The default dialer uses the in-tree ``mcp`` client. Tests inject a fake dialer
to drive :class:`~src.tool_broker.upstream.Upstream` without a real sidecar.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable

import mcp.types as types
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


@runtime_checkable
class UpstreamConn(Protocol):
    """One live MCP connection to a sidecar, scoped to a single operation."""

    async def list_tools(self) -> list[types.Tool]: ...

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult: ...


class Dialer(Protocol):
    """Opens an :class:`UpstreamConn` to ``url`` as an async context manager.

    ``headers`` rides along unconditionally (empty dict when the caller has
    none) so a cross-service correlation id — or anything else a future
    caller wants on the wire — has one place to attach, without every
    dialer implementation needing its own opinion about when headers apply.
    """

    def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> contextlib.AbstractAsyncContextManager[UpstreamConn]: ...


class _ClientConn:
    """Adapts an initialized :class:`ClientSession` to :class:`UpstreamConn`."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session

    async def list_tools(self) -> list[types.Tool]:
        return list((await self._session.list_tools()).tools)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        return await self._session.call_tool(name, arguments)


def default_dialer(connect_timeout: float) -> Dialer:
    """A :data:`Dialer` over streamable HTTP with a per-attempt connect timeout.

    ``connect_timeout`` bounds how long a single dial waits for the sidecar to
    answer — that is the knob that turns "sidecar is down" into a fast failure
    the caller can retry, rather than a hang.
    """

    @contextlib.asynccontextmanager
    async def _dial(
        url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[UpstreamConn]:
        async with (
            streamablehttp_client(url, timeout=connect_timeout, headers=headers) as (
                read,
                write,
                _get_session_id,
            ),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            yield _ClientConn(session)

    return _dial
