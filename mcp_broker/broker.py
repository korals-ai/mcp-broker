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
        # What each live CLI session is listing UNDER: (session token, chat id).
        # The set above cannot answer that, which is why a notify can be
        # absorbed by the wrong session and why clear_session cannot prune.
        # Recorded here so both become measurable before either is changed.
        self._session_ctx: dict[ServerSession, tuple[str | None, str | None]] = {}
        # Session token -> the chat that registered it. Broker events carry no
        # chat id of their own (only the browser tool's URL has one), so
        # without this every tools/list line in Loki reads `chat_id=-` and
        # cannot be tied to the conversation the user is complaining about.
        self._token_chat: dict[str, str] = {}
        # (name, token) registered and notified, but not yet observed serving
        # tools to anyone. This is the HEAL, as opposed to the notify: "we told
        # the CLI" and "the agent ended up with the tools" are different
        # events, and only the second one is what the user experiences.
        self._pending_heal: dict[tuple[str, str], float] = {}
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

    async def register_session(
        self, name: str, token: str, url: str, *, chat_id: str | None = None
    ) -> bool:
        """Attach a session's per-user child (already spawned by the sidecar) at
        ``url`` for a session-scoped connector, probe it so its tools are live,
        and notify open CLI sessions so the tools appear mid-session. Returns
        whether the child answered the probe.

        ``chat_id`` is recorded for attribution only — it never routes
        anything. Without it a broker log line cannot be tied to the chat whose
        tools went missing, which is the first question anyone asks."""
        if chat_id:
            self._token_chat[token] = chat_id
        # metrics threaded deliberately: session children have no dial loop, so
        # the probe counter (child_probe seam) is the ONLY health signal a
        # broken per-user child emits — without it a down child is metric-dark
        # (found 2026-09-05 auditing why a broken connector raised no alert).
        up = Upstream(
            name,
            url,
            dialer=self._dial,
            call_timeout_s=self._call_timeout_s,
            probe_timeout_s=self._probe_timeout_s,
            metrics=self._metrics,
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
        if (name, token) in self._session_upstreams:
            # Overwriting a live upstream: the old one is dropped without being
            # closed. Harmless once, a churn signal in bulk — and previously
            # indistinguishable from a first registration.
            self._metrics.inc_session_upstream_replaced(name)
            log.info("replacing existing session upstream name=%s (re-attach)", name)
        self._session_upstreams[(name, token)] = up
        # How many CLI sessions this notify actually reached is the difference
        # between "the tools will appear" and "the tools are invisible for the
        # life of this session". _sessions[name] is only populated when a CLI
        # has already issued a tools/list for this server (see _build_server),
        # so registering BEFORE the CLI's first list reaches nobody — and
        # nothing re-lists afterwards. Report it rather than leaving the two
        # cases indistinguishable in the log.
        notified, matched = await self._notify(name, token=token)
        # Three outcomes, not two. "delivered" hid the case that actually
        # bites: the notify reached CLI sessions, but none of them was the one
        # holding THIS token — so the session that needs the tools was never
        # told, while the counter said delivered.
        if not notified:
            outcome = "no_sessions"
        elif matched:
            outcome = "delivered"
        else:
            outcome = "delivered_foreign_only"
        self._metrics.inc_session_notify(name, outcome)
        # Arm the heal watch: cleared when a list for this exact (name, token)
        # actually serves tools. Anything still armed is a session that was
        # told and never came back.
        self._pending_heal[(name, token)] = asyncio.get_running_loop().time()
        self._metrics.set_pending_heals(name, self._pending_heal_count(name))
        log.info(
            "registered session upstream name=%s chat=%s url=%s ready=%s "
            "notified_sessions=%d notified_this_token=%d",
            name,
            self._token_chat.get(token, "-"),
            url,
            up.ready,
            notified,
            matched,
        )
        return up.ready

    def _pending_heal_count(self, name: str) -> int:
        return sum(1 for key in self._pending_heal if key[0] == name)

    def clear_session(self, token: str) -> None:
        """Drop every per-session upstream for ``token`` (session ended).

        Does NOT touch ``_sessions``: those are CLI ServerSession objects keyed
        by server name, not by token, so this method cannot tell which of them
        belonged to the ending session. They are pruned lazily instead — a dead
        session fails its next notify and is removed there. That laziness is
        worth knowing about, because until the next notify each stale entry
        costs one notify timeout on the attach path, so the count is published
        (set_cli_sessions) rather than left to be guessed at.
        """
        for key in [k for k in self._session_upstreams if k[1] == token]:
            self._metrics.inc_session_cleared(key[0])
            del self._session_upstreams[key]
        # A session ending with its heal still armed is the verdict: it was
        # told the tools existed and never saw them for its whole life. Counted
        # HERE because this is the last moment the fact is knowable — after
        # this the evidence is gone.
        for key in [k for k in self._pending_heal if k[1] == token]:
            self._metrics.inc_heal(key[0], "never_served")
            log.warning(
                "session ended with tools never served name=%s chat=%s — the agent "
                "was notified the connector attached and never listed it successfully",
                key[0],
                self._token_chat.get(token, "-"),
            )
            del self._pending_heal[key]
            self._metrics.set_pending_heals(key[0], self._pending_heal_count(key[0]))
        self._token_chat.pop(token, None)

    async def _list_tools_for(self, name: str) -> list[types.Tool]:
        """The ``tools/list`` body for one sidecar: the routed upstream's
        cached roster. A not-ready per-user child is re-probed lazily here —
        it lost the startup race (registered before it listened and
        register_session's budget ran out) and has no dial loop to heal it,
        so the CLI's next list is its only recovery point. No-op once ready,
        and rate-limited so a permanently wedged child (crashed transport,
        e.g. unreachable Odoo host) costs at most one probe timeout per
        window instead of one per list."""
        token = _CURRENT_TOKEN.get()
        up = self._upstream_for(name)
        if up is None:
            # Two very different failures used to share one label, and they need
            # opposite fixes:
            #   no_token      - this CLI's MCP URL never carried a session
            #                   token. It cannot be healed by waiting: the URL
            #                   was fixed when the agent spawned, so no later
            #                   attach can ever route to it. A BUG.
            #   unregistered  - the token is fine, the child just is not
            #                   attached yet. Ordinary startup, heals on the
            #                   listChanged re-list. A RACE.
            outcome = "empty_unregistered" if token else "empty_no_token"
            self._metrics.inc_tools_list(name, outcome)
            self._metrics.set_tools_advertised(name, 0)
            log.warning(
                "broker tools/list %s -> EMPTY (%s) chat=%s session_scoped=%s "
                "— the agent will report it as not connected",
                name,
                outcome,
                self._token_chat.get(token or "", "-"),
                name in self._session_scoped,
            )
            return []
        if (
            not up.ready
            and name in self._session_scoped
            and not up.probed_recently(self._probe_timeout_s * 2)
        ):
            await up.probe()
        tools = up.tools()
        if tools:
            outcome = "served"
        elif up.ready:
            # Ready and advertising nothing: a misconfigured child, not a
            # missing one. The two look identical to the agent and need
            # opposite fixes, so they must not share an outcome.
            outcome = "empty_ready"
        else:
            outcome = "empty_not_ready"
        self._metrics.inc_tools_list(name, outcome)
        self._metrics.set_tools_advertised(name, len(tools))
        if tools and token is not None:
            self._settle_heal(name, token)
        if not tools:
            log.warning(
                "broker tools/list %s -> EMPTY (%s, ready=%s) chat=%s "
                "— the agent cannot see this server's tools",
                name,
                outcome,
                up.ready,
                self._token_chat.get(token or "", "-"),
            )
        return tools

    def _settle_heal(self, name: str, token: str) -> None:
        """Record that the tools actually REACHED the agent for this session.

        The notify counter says we told the CLI; this says the CLI came back
        and got a non-empty roster. Only the second one is the thing the user
        experiences, and the two can disagree.
        """
        started = self._pending_heal.pop((name, token), None)
        if started is None:
            return
        self._metrics.inc_heal(name, "served")
        self._metrics.set_pending_heals(name, self._pending_heal_count(name))
        log.info(
            "tools reached the agent name=%s chat=%s after_ms=%d",
            name,
            self._token_chat.get(token, "-"),
            int((asyncio.get_running_loop().time() - started) * 1000),
        )

    def _build_server(self, name: str) -> Server[Any, Any]:
        server: Server[Any, Any] = _ListChangedServer(f"broker-{name}")

        @server.list_tools()
        async def _list_tools() -> list[types.Tool]:
            # request_context is only valid inside a handler — this is the one
            # safe point to capture the live session for later notification.
            with contextlib.suppress(LookupError):
                session = server.request_context.session
                self._sessions[name].add(session)
                token = _CURRENT_TOKEN.get()
                self._session_ctx[session] = (token, self._token_chat.get(token or ""))
                if name in self._session_scoped and token is None:
                    # A CLI session on a per-user connector with no token can
                    # never be routed, no matter what attaches later. Counting
                    # it separates "not connected yet" from "unreachable for
                    # the life of this session".
                    self._metrics.inc_tools_list(name, "cli_session_tokenless")
            # The denominator for a "no_sessions" notify: without it, "nobody
            # was listening" and "everybody was unreachable" look the same.
            self._metrics.set_cli_sessions(name, len(self._sessions[name]))
            return await self._list_tools_for(name)

        @server.call_tool(validate_input=False)
        async def _call_tool(tool_name: str, arguments: dict[str, Any]) -> types.CallToolResult:
            up = self._upstream_for(name)
            if up is None:
                self._metrics.inc_call_no_upstream(name)
                log.warning(
                    "broker call %s.%s -> NO UPSTREAM for this session; returning "
                    "'not connected' to the agent (token_present=%s)",
                    name,
                    tool_name,
                    _CURRENT_TOKEN.get() is not None,
                )
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

    async def _notify(self, name: str, *, token: str | None = None) -> tuple[int, int]:
        """Push ``tools/list_changed`` to every open CLI session for ``name``,
        returning (successfully told, of which were listening under ``token``).

        The second number is the one that matters. A notify can land on several
        CLI sessions and still miss the only one that needed it — the counters
        said "delivered" while the agent that was waiting heard nothing.
        pruning any that have since closed OR gone unresponsive.

        Each send is bounded: a CLI session whose transport is wedged (a
        slow/dead reader applying backpressure) must not block the fan-out
        to the OTHER sessions — nor stall the caller, since _notify runs on
        the register_session -> connector attach -> bind path and inside
        the dial-heal loop. A timed-out session is pruned like a dead one;
        it re-registers on its next tools/list."""
        dead: set[ServerSession] = set()
        delivered = 0
        matched = 0
        for session in list(self._sessions[name]):
            try:
                await asyncio.wait_for(
                    session.send_tool_list_changed(), timeout=self._notify_timeout_s
                )
                delivered += 1
                if token is not None and self._session_ctx.get(session, (None, None))[0] == token:
                    matched += 1
            except Exception as exc:  # closed OR wedged -> prune; re-adds on next list
                log.debug("broker notify dropping dead/wedged session for %s: %s", name, exc)
                dead.add(session)
        self._sessions[name] -= dead
        for session in dead:
            self._session_ctx.pop(session, None)
        return delivered, matched
