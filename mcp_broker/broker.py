"""The in-pod tool broker: one always-up MCP endpoint per sidecar.

The agent's MCP client (the claude-code CLI) connects to the broker — which is
in-process in the main workspace container and therefore up the instant the
session starts — instead of to the sidecars directly. This decouples "the agent
can use tool X" from "sidecar X was already listening at session start", which
is what lets the sidecars come off the pod's boot critical path (see
``docs/arch.md`` §"Sidecar lifecycle").

Per sidecar the broker runs a low-level MCP :class:`Server` that:

* advertises ``tools.listChanged=true`` (forced — see :class:`_ListChangedServer`),
* serves ``tools/list`` from the upstream's **confirmed-lazy** cache (empty until
  the sidecar has been reached once),
* proxies ``tools/call`` to the sidecar with a bounded retry budget, and
* is notified by a background dial loop when its sidecar first comes up, so the
  new tools appear **mid-session** (the CLI re-lists on the notification).

A sidecar that never comes up: its tools stay absent (the agent never sees a
phantom), the dial loop backs off to a cap and then stops + alerts. See
:class:`Upstream`.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import os
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import parse_qs

import mcp.types as types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.session import ServerSession
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.types import ASGIApp

from mcp_broker.dialer import Dialer, default_dialer
from mcp_broker.metrics import NULL_METRICS, BrokerMetrics
from mcp_broker.upstream import Upstream

log = logging.getLogger("workspace.tool_broker")

# URL path the agent's MCP client reaches a fronted sidecar at, e.g.
# http://localhost:8080/_broker/workspace-tool-office/mcp. The "_broker" prefix
# keeps these localhost-only endpoints clear of the tenant-facing REST routes.
BROKER_PATH_PREFIX = "/_broker"

# The current request's session token, extracted from the ``?session=<token>``
# query param the agent's per-session MCP URL carries (set by the ASGI wrapper,
# read by the tool handlers). None for static sidecars / tokenless requests. A
# ContextVar so concurrent requests to the shared broker don't cross wires.
_CURRENT_TOKEN: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "broker_session_token", default=None
)

# The current request's chat id, from the ``?chat_id=<id>`` query param the
# agent's browser-tool MCP URL carries (set by the ASGI wrapper, read when
# forwarding the call to the sidecar so each chat gets its own browser session).
# None for tools that aren't per-chat. A ContextVar so concurrent requests to the
# shared broker don't cross wires.
_CURRENT_CHAT_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "broker_chat_id", default=None
)


def broker_route(name: str) -> str:
    """The mount path for one sidecar's broker endpoint (no trailing /mcp host)."""
    return f"{BROKER_PATH_PREFIX}/{name}/mcp"


def _query_from_scope(scope: dict[str, Any], key: str) -> str | None:
    """The value of query param ``key`` from an ASGI scope, or None."""
    qs = scope.get("query_string") or b""
    vals = parse_qs(qs.decode("latin-1")).get(key)
    return vals[0] if vals else None


def _token_from_scope(scope: dict[str, Any]) -> str | None:
    """The ``session`` query param from an ASGI scope, or None."""
    return _query_from_scope(scope, "session")


class _ListChangedServer(Server[Any, Any]):
    """A low-level MCP server that ALWAYS advertises ``tools.listChanged=true``.

    ``StreamableHTTPSessionManager`` builds the server's ``InitializationOptions``
    by calling ``create_initialization_options()`` with NO arguments, which
    defaults to ``NotificationOptions(tools_changed=False)`` — so the
    ``listChanged`` capability would NOT be advertised and the claude-code CLI
    would never register its ``tools/list_changed`` refresh handler. Without that
    handler the broker's whole mid-session recovery silently breaks. We force
    ``tools_changed=True`` here. (Verified against claude-code 2.1.122's MCP
    client, which keys its refresh handler off this capability.)
    """

    def create_initialization_options(
        self,
        notification_options: NotificationOptions | None = None,
        experimental_capabilities: dict[str, dict[str, Any]] | None = None,
    ) -> Any:
        return super().create_initialization_options(
            notification_options or NotificationOptions(tools_changed=True),
            experimental_capabilities,
        )


class ToolBroker:
    """Fronts a fixed roster of sidecars with one always-up MCP endpoint each.

    The roster is ``[(name, real_url), ...]`` — the operator-stamped sidecar
    list (a pod's roster is fixed for its life). ``dialer`` is injectable for
    tests; the default dials real sidecars over streamable HTTP.
    """

    def __init__(
        self,
        roster: list[tuple[str, str]],
        *,
        dialer: Dialer | None = None,
        session_scoped: set[str] | None = None,
        connect_timeout_s: float = 5.0,
        poll_min_s: float = 0.5,
        poll_max_s: float = 8.0,
        giveup_after_s: float = 60.0,
        call_timeout_s: float = 120.0,
        session_probe_budget_s: float = 8.0,
        probe_timeout_s: float = 5.0,
        notify_timeout_s: float = 2.0,
        metrics: BrokerMetrics = NULL_METRICS,
    ) -> None:
        dial = dialer or default_dialer(connect_timeout_s)
        self._dial = dial
        self._metrics = metrics
        self._call_timeout_s = call_timeout_s
        self._poll_min_s = poll_min_s
        self._poll_max_s = poll_max_s
        self._giveup_after_s = giveup_after_s
        self._session_probe_budget_s = session_probe_budget_s
        self._probe_timeout_s = probe_timeout_s
        self._notify_timeout_s = notify_timeout_s
        # Session-scoped connectors (e.g. odoo): routed per-session to a per-user
        # child, NOT fronted by a single static upstream. Their roster URL is the
        # sidecar's CONTROL endpoint, not an MCP server, so they get no static
        # dial loop and no static tools — a session's creds must be registered
        # first (see register_session).
        self._session_scoped = session_scoped or set()
        self._upstreams: dict[str, Upstream] = {
            name: Upstream(
                name,
                url,
                dialer=dial,
                call_timeout_s=call_timeout_s,
                probe_timeout_s=probe_timeout_s,
                metrics=metrics,
            )
            for name, url in roster
        }
        # (name, session token) -> that session's per-user child upstream.
        self._session_upstreams: dict[tuple[str, str], Upstream] = {}
        # Live CLI sessions per sidecar, captured in the list_tools handler, so
        # the dial loop can push tools/list_changed to each open connection.
        self._sessions: dict[str, set[ServerSession]] = {name: set() for name in self._upstreams}
        self._managers: dict[str, StreamableHTTPSessionManager] = {
            name: StreamableHTTPSessionManager(app=self._build_server(name), stateless=False)
            for name in self._upstreams
        }

    @classmethod
    def from_env(
        cls,
        roster: list[tuple[str, str]],
        *,
        dialer: Dialer | None = None,
        session_scoped: set[str] | None = None,
        metrics: BrokerMetrics = NULL_METRICS,
    ) -> ToolBroker:
        """Construct from ``MCP_BROKER_*`` env knobs (the defaults below if unset).

        Lets prod tune the dial cadence / give-up cap without an image rebuild,
        and lets tests collapse the give-up window so a missing sidecar doesn't
        hold the dial loop open for the full ~60s.
        """

        def _f(name: str, default: float) -> float:
            try:
                return float(os.environ.get(name) or default)
            except ValueError:
                return default

        return cls(
            roster,
            dialer=dialer,
            session_scoped=session_scoped,
            metrics=metrics,
            connect_timeout_s=_f("MCP_BROKER_CONNECT_TIMEOUT_S", 5.0),
            poll_min_s=_f("MCP_BROKER_POLL_MIN_S", 0.5),
            poll_max_s=_f("MCP_BROKER_POLL_MAX_S", 8.0),
            giveup_after_s=_f("MCP_BROKER_GIVEUP_S", 60.0),
            call_timeout_s=_f("MCP_BROKER_CALL_TIMEOUT_S", 120.0),
        )

    @property
    def names(self) -> list[str]:
        return list(self._upstreams)

    def asgi_app(self, name: str) -> ASGIApp:
        """The ASGI app to mount at :func:`broker_route` for ``name``."""
        manager = self._managers[name]

        async def app(scope: Any, receive: Any, send: Any) -> None:
            # Stash the request's session token (route a session-scoped connector
            # to the right per-user child) and chat id (route a browser call to
            # that chat's own session) for the tool handlers to read.
            is_http = scope.get("type") == "http"
            token = _token_from_scope(scope) if is_http else None
            chat_id = _query_from_scope(scope, "chat_id") if is_http else None
            reset_token = _CURRENT_TOKEN.set(token)
            reset_chat = _CURRENT_CHAT_ID.set(chat_id)
            try:
                await manager.handle_request(scope, receive, send)
            finally:
                _CURRENT_CHAT_ID.reset(reset_chat)
                _CURRENT_TOKEN.reset(reset_token)

        return app

    def _upstream_for(self, name: str) -> Upstream | None:
        """The upstream that should serve the CURRENT request for ``name``.

        Static sidecar → its one upstream. Session-scoped connector → the
        per-user child upstream registered for this request's session token, or
        None when no token / not yet connected for this session (the handlers
        degrade to empty tools / an unavailable result)."""
        if name not in self._session_scoped:
            return self._upstreams[name]
        token = _CURRENT_TOKEN.get()
        if token is None:
            return None
        return self._session_upstreams.get((name, token))

    async def register_session(self, name: str, token: str, url: str) -> bool:
        """Attach a session's per-user child (already spawned by the sidecar) at
        ``url`` for a session-scoped connector, probe it so its tools are live,
        and notify open CLI sessions so the tools appear mid-session. Returns
        whether the child answered the probe."""
        up = Upstream(
            name,
            url,
            dialer=self._dial,
            call_timeout_s=self._call_timeout_s,
            probe_timeout_s=self._probe_timeout_s,
        )
        # The sidecar's control POST returns as soon as the child process is
        # SPAWNED — it may not be listening yet. A single probe loses that
        # race, and session upstreams have no dial loop to heal them (only
        # static sidecars do), so tools/list would stay empty for the
        # session's life. Poll within a short budget; past it, register
        # anyway — calls dial fresh, and _list_tools re-probes lazily.
        deadline = asyncio.get_running_loop().time() + self._session_probe_budget_s
        await up.probe()
        while not up.ready and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.25)
            await up.probe()
        self._session_upstreams[(name, token)] = up
        await self._notify(name)
        log.info("registered session upstream name=%s url=%s ready=%s", name, url, up.ready)
        return up.ready

    def clear_session(self, token: str) -> None:
        """Drop every per-session upstream for ``token`` (session ended)."""
        for key in [k for k in self._session_upstreams if k[1] == token]:
            del self._session_upstreams[key]

    async def _list_tools_for(self, name: str) -> list[types.Tool]:
        """The ``tools/list`` body for one sidecar: the routed upstream's
        cached roster. A not-ready per-user child is re-probed lazily here —
        it lost the startup race (registered before it listened and
        register_session's budget ran out) and has no dial loop to heal it,
        so the CLI's next list is its only recovery point. No-op once ready,
        and rate-limited so a permanently wedged child (crashed transport,
        e.g. unreachable Odoo host) costs at most one probe timeout per
        window instead of one per list."""
        up = self._upstream_for(name)
        if up is None:
            return []
        if (
            not up.ready
            and name in self._session_scoped
            and not up.probed_recently(self._probe_timeout_s * 2)
        ):
            await up.probe()
        return up.tools()

    def _build_server(self, name: str) -> Server[Any, Any]:
        server: Server[Any, Any] = _ListChangedServer(f"broker-{name}")

        @server.list_tools()
        async def _list_tools() -> list[types.Tool]:
            # request_context is only valid inside a handler — this is the one
            # safe point to capture the live session for later notification.
            with contextlib.suppress(LookupError):
                self._sessions[name].add(server.request_context.session)
            return await self._list_tools_for(name)

        @server.call_tool(validate_input=False)
        async def _call_tool(tool_name: str, arguments: dict[str, Any]) -> types.CallToolResult:
            up = self._upstream_for(name)
            if up is None:
                return types.CallToolResult(
                    content=[
                        types.TextContent(
                            type="text",
                            text=(
                                f"'{name}' isn't connected for this session yet. Connect it "
                                "first, then try again in a moment."
                            ),
                        )
                    ],
                    isError=True,
                )
            return await up.call(tool_name, arguments, chat_id=_CURRENT_CHAT_ID.get())

        return server

    @contextlib.asynccontextmanager
    async def run(self) -> AsyncIterator[None]:
        """Drive the session managers and per-sidecar dial loops for the app's life.

        Enter this once in the app lifespan. Each ``StreamableHTTPSessionManager``
        needs its ``run()`` active to serve requests; each sidecar gets one dial
        loop that probes until ready (then notifies) or gives up.
        """
        async with contextlib.AsyncExitStack() as stack:
            for manager in self._managers.values():
                await stack.enter_async_context(manager.run())
            loop = asyncio.get_running_loop()
            # Session-scoped connectors have no static MCP upstream to probe (their
            # roster URL is the control endpoint), so they get no dial loop — a
            # per-session child is probed on register_session instead.
            tasks = [
                loop.create_task(self._dial_loop(name))
                for name in self._upstreams
                if name not in self._session_scoped
            ]
            log.info(
                "tool broker started for %d sidecar(s): %s",
                len(self._upstreams),
                ", ".join(self.names),
            )
            try:
                yield
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _dial_loop(self, name: str) -> None:
        """Probe one sidecar with exponential backoff until it is ready (then
        notify open sessions) or the give-up cap is reached (then alert)."""
        upstream = self._upstreams[name]
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._giveup_after_s
        delay = self._poll_min_s
        while True:
            transitioned = await upstream.probe()
            if upstream.ready:
                if transitioned:
                    log.info(
                        "broker upstream %s ready; notifying %d session(s)",
                        name,
                        len(self._sessions[name]),
                    )
                    await self._notify(name)
                return
            if loop.time() >= deadline:
                self._metrics.inc_upstream_giveup(name)
                log.warning(
                    "broker upstream %s never became ready within %.0fs; giving up (sidecar wedged?)",
                    name,
                    self._giveup_after_s,
                )
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._poll_max_s)

    async def _notify(self, name: str) -> None:
        """Push ``tools/list_changed`` to every open CLI session for ``name``,
        pruning any that have since closed OR gone unresponsive.

        Each send is bounded: a CLI session whose transport is wedged (a
        slow/dead reader applying backpressure) must not block the fan-out
        to the OTHER sessions — nor stall the caller, since _notify runs on
        the register_session -> connector attach -> bind path and inside
        the dial-heal loop. A timed-out session is pruned like a dead one;
        it re-registers on its next tools/list."""
        dead: set[ServerSession] = set()
        for session in list(self._sessions[name]):
            try:
                await asyncio.wait_for(
                    session.send_tool_list_changed(), timeout=self._notify_timeout_s
                )
            except Exception as exc:  # closed OR wedged -> prune; re-adds on next list
                log.debug("broker notify dropping dead/wedged session for %s: %s", name, exc)
                dead.add(session)
        self._sessions[name] -= dead
