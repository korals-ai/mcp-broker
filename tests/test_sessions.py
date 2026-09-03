"""Per-session broker routing: a session-scoped connector (odoo) is routed to the
right per-user child by the session token on the request, while static sidecars
are untouched.

Drives the routing with a URL-recording fake dialer so we can assert which child
a call actually reached — the isolation property (session A's calls hit A's
child, never B's).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import mcp.types as types

from mcp_broker.broker import (
    _CURRENT_TOKEN,
    ToolBroker,
    _query_from_scope,
    _token_from_scope,
)

_ODOO = "workspace-tool-odoo"
_CONTROL_URL = "http://127.0.0.1:8093/mcp"


def _tool(name: str) -> types.Tool:
    return types.Tool(name=name, description="", inputSchema={"type": "object"})


class _RecordingConn:
    def __init__(self, url: str, tools: list[types.Tool]) -> None:
        self._url = url
        self._tools = tools

    async def list_tools(self) -> list[types.Tool]:
        return self._tools

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=f"{name}@{self._url}")], isError=False
        )


class _RecordingDialer:
    def __init__(self, tools: list[types.Tool]) -> None:
        self._tools = tools
        self.urls: list[str] = []

    @contextlib.asynccontextmanager
    async def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[_RecordingConn]:
        self.urls.append(url)
        yield _RecordingConn(url, self._tools)


def _broker(dialer: _RecordingDialer) -> ToolBroker:
    return ToolBroker(
        [(_ODOO, _CONTROL_URL)],
        dialer=dialer,
        session_scoped={_ODOO},
    )


# ---- token extraction ------------------------------------------------------


def test_token_from_scope() -> None:
    assert _token_from_scope({"query_string": b"session=tok-123"}) == "tok-123"
    assert _token_from_scope({"query_string": b"foo=bar&session=t2&x=y"}) == "t2"
    assert _token_from_scope({"query_string": b""}) is None
    assert _token_from_scope({}) is None


def test_chat_id_from_scope() -> None:
    # The capture end: the browser tool's ?chat_id= is pulled off the ASGI scope so
    # the broker can forward it to the sidecar dial.
    assert _query_from_scope({"query_string": b"chat_id=chat-9"}, "chat_id") == "chat-9"
    assert _query_from_scope({"query_string": b"session=t&chat_id=chat-9"}, "chat_id") == "chat-9"
    assert _query_from_scope({"query_string": b"session=t"}, "chat_id") is None
    assert _query_from_scope({}, "chat_id") is None


# ---- routing ---------------------------------------------------------------


def test_session_scoped_unroutable_without_token_or_registration() -> None:
    broker = _broker(_RecordingDialer([_tool("odoo_search")]))
    reset = _CURRENT_TOKEN.set(None)
    try:
        assert broker._upstream_for(_ODOO) is None  # no token
    finally:
        _CURRENT_TOKEN.reset(reset)
    reset = _CURRENT_TOKEN.set("tok-unregistered")
    try:
        assert broker._upstream_for(_ODOO) is None  # token, but not registered
    finally:
        _CURRENT_TOKEN.reset(reset)


async def test_register_then_route_to_that_users_child() -> None:
    dialer = _RecordingDialer([_tool("odoo_search")])
    broker = _broker(dialer)
    ready = await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    assert ready  # probe reached the child

    reset = _CURRENT_TOKEN.set("tok-alice")
    try:
        up = broker._upstream_for(_ODOO)
        assert up is not None
        assert [t.name for t in up.tools()] == ["odoo_search"]
        result = await up.call("odoo_search", {})
    finally:
        _CURRENT_TOKEN.reset(reset)
    # The call reached ALICE's child, not the control URL.
    assert "8100/mcp" in result.content[0].text
    assert _CONTROL_URL not in dialer.urls  # control URL is never dialed for MCP


async def test_two_sessions_route_to_distinct_children() -> None:
    dialer = _RecordingDialer([_tool("odoo_search")])
    broker = _broker(dialer)
    await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    await broker.register_session(_ODOO, "tok-bob", "http://127.0.0.1:8101/mcp")

    async def _call_as(token: str) -> str:
        reset = _CURRENT_TOKEN.set(token)
        try:
            up = broker._upstream_for(_ODOO)
            assert up is not None
            return (await up.call("odoo_search", {})).content[0].text
        finally:
            _CURRENT_TOKEN.reset(reset)

    assert "8100/mcp" in await _call_as("tok-alice")  # Alice → Alice's child
    assert "8101/mcp" in await _call_as("tok-bob")  # Bob → Bob's child


async def test_clear_session_drops_routing() -> None:
    broker = _broker(_RecordingDialer([_tool("odoo_search")]))
    await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    broker.clear_session("tok-alice")
    reset = _CURRENT_TOKEN.set("tok-alice")
    try:
        assert broker._upstream_for(_ODOO) is None  # gone after clear
    finally:
        _CURRENT_TOKEN.reset(reset)


async def test_clear_one_session_leaves_the_other() -> None:
    broker = _broker(_RecordingDialer([_tool("odoo_search")]))
    await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    await broker.register_session(_ODOO, "tok-bob", "http://127.0.0.1:8101/mcp")
    broker.clear_session("tok-alice")
    for token, present in (("tok-alice", False), ("tok-bob", True)):
        reset = _CURRENT_TOKEN.set(token)
        try:
            assert (broker._upstream_for(_ODOO) is not None) is present
        finally:
            _CURRENT_TOKEN.reset(reset)


class _FlakyDialer(_RecordingDialer):
    """Fails the first ``fail_first`` dials (child spawned but not listening
    yet — the sidecar control POST returns at Popen time), then behaves like
    :class:`_RecordingDialer`."""

    def __init__(self, tools: list[types.Tool], *, fail_first: int) -> None:
        super().__init__(tools)
        self._remaining_failures = fail_first

    @contextlib.asynccontextmanager
    async def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[_RecordingConn]:
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise ConnectionRefusedError("child not listening yet")
        self.urls.append(url)
        yield _RecordingConn(url, self._tools)


async def test_register_session_retries_probe_until_child_listens() -> None:
    """The child-startup race: register_session must probe-poll within its
    budget, or a child that takes a beat to listen leaves the session's
    tools/list empty for the session's whole life (no dial loop heals
    per-user upstreams)."""
    dialer = _FlakyDialer([_tool("odoo_search")], fail_first=2)
    broker = ToolBroker(
        [(_ODOO, _CONTROL_URL)],
        dialer=dialer,
        session_scoped={_ODOO},
        session_probe_budget_s=5.0,
    )
    ready = await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    assert ready is True


async def test_list_tools_lazily_reprobes_child_that_outlasted_the_budget() -> None:
    """A child still not listening when register_session's budget runs out is
    registered not-ready; the next tools/list re-probes it so the tools
    appear on the CLI's next refresh instead of never."""
    dialer = _FlakyDialer([_tool("odoo_search")], fail_first=1)
    broker = ToolBroker(
        [(_ODOO, _CONTROL_URL)],
        dialer=dialer,
        session_scoped={_ODOO},
        session_probe_budget_s=0.0,  # lose the race deterministically
        probe_timeout_s=0.01,  # shrink the lazy re-probe rate-limit window
    )
    ready = await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    assert ready is False
    await asyncio.sleep(0.05)  # step past the rate-limit window (2x probe timeout)

    reset = _CURRENT_TOKEN.set("tok-alice")
    try:
        # First list after the child came up: the lazy re-probe heals it.
        assert [t.name for t in await broker._list_tools_for(_ODOO)] == ["odoo_search"]
    finally:
        _CURRENT_TOKEN.reset(reset)


class _WedgedDialer(_RecordingDialer):
    """Connects fine, then never answers list_tools — the crashed-transport
    shape: mcp-server-odoo with an unreachable Odoo host accepts the
    initialize POST, its session crashes server-side, and the client's next
    read waits forever (reference: the May 2026 wedged-sidecar call hang;
    same class, probe path)."""

    def __init__(self) -> None:
        super().__init__([])
        self.dials = 0

    @contextlib.asynccontextmanager
    async def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[Any]:
        self.dials += 1

        class _NeverAnswers:
            async def list_tools(self) -> list[types.Tool]:
                await asyncio.Event().wait()  # never set
                raise AssertionError("unreachable")

        yield _NeverAnswers()


async def test_register_session_survives_wedged_child() -> None:
    """A child whose transport session crashed mid-initialize must cost at
    most the probe budget — NOT hang register_session forever. Unbounded,
    this hang propagated through attach() into the 60s spawn watchdog and
    killed the whole session spawn for any user with an unreachable Odoo
    (Jul 2026 stg finding)."""
    dialer = _WedgedDialer()
    broker = ToolBroker(
        [(_ODOO, _CONTROL_URL)],
        dialer=dialer,
        session_scoped={_ODOO},
        session_probe_budget_s=0.0,  # one probe attempt
        probe_timeout_s=0.2,
    )
    ready = await asyncio.wait_for(
        broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp"),
        timeout=5.0,  # generous outer guard — the register itself must be quick
    )
    assert ready is False
    assert dialer.dials >= 1


async def test_lazy_reprobe_is_rate_limited() -> None:
    """Back-to-back tools/list against a not-ready child must not pay one
    probe (and its timeout) per list — the second list inside the window
    skips the probe."""
    dialer = _FlakyDialer([_tool("odoo_search")], fail_first=100)  # never succeeds
    broker = ToolBroker(
        [(_ODOO, _CONTROL_URL)],
        dialer=dialer,
        session_scoped={_ODOO},
        session_probe_budget_s=0.0,
        probe_timeout_s=5.0,  # rate-limit window = 2x this
    )
    await broker.register_session(_ODOO, "tok-alice", "http://127.0.0.1:8100/mcp")
    dials_after_register = dialer.urls  # _FlakyDialer only records successes
    failures_after_register = dialer._remaining_failures

    reset = _CURRENT_TOKEN.set("tok-alice")
    try:
        assert await broker._list_tools_for(_ODOO) == []  # within window: no new probe
        assert await broker._list_tools_for(_ODOO) == []
    finally:
        _CURRENT_TOKEN.reset(reset)
    assert dialer._remaining_failures == failures_after_register  # no dials happened
    assert dialer.urls == dials_after_register


def test_static_sidecar_unaffected_by_token() -> None:
    # A non-session-scoped sidecar always routes to its one static upstream,
    # token or not.
    dialer = _RecordingDialer([_tool("convert")])
    broker = ToolBroker([("workspace-tool-office", "http://127.0.0.1:8090/mcp")], dialer=dialer)
    reset = _CURRENT_TOKEN.set("whatever")
    try:
        assert broker._upstream_for("workspace-tool-office") is not None
    finally:
        _CURRENT_TOKEN.reset(reset)


# ---- _notify: a wedged CLI session must not stall the fan-out --------------


class _FakeCliSession:
    """Stand-in for an MCP ServerSession the broker pushes list_changed to.

    ``hang=True`` models a session whose transport is wedged (a dead/slow
    reader) — its send never completes.
    """

    def __init__(self, *, hang: bool = False) -> None:
        self.notified = 0
        self._hang = hang

    async def send_tool_list_changed(self) -> None:
        if self._hang:
            await asyncio.Event().wait()  # never returns
        self.notified += 1


async def test_notify_bounds_a_wedged_session_and_still_reaches_the_healthy_one() -> None:
    dialer = _RecordingDialer([_tool("x")])
    broker = ToolBroker(
        [(_ODOO, _CONTROL_URL)],
        dialer=dialer,
        session_scoped={_ODOO},
        notify_timeout_s=0.05,
    )
    healthy = _FakeCliSession()
    wedged = _FakeCliSession(hang=True)
    broker._sessions[_ODOO] = {healthy, wedged}  # type: ignore[set-item]

    # A wedged session must not hang _notify forever (it runs on the bind
    # path + the dial-heal loop). With no per-send bound this call never
    # returns and the outer wait_for trips.
    await asyncio.wait_for(broker._notify(_ODOO), timeout=2.0)

    assert healthy.notified == 1  # not starved by head-of-line blocking
    assert wedged not in broker._sessions[_ODOO]  # the wedged session is pruned
    assert healthy in broker._sessions[_ODOO]  # the healthy one is retained
