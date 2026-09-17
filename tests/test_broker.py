"""Unit tests for the in-pod tool broker.

The broker is the lazy/retriable interface that lets sidecars come off the boot
critical path. These tests drive its core decisions with a FAKE dialer (no real
sidecar), pinning the contract that matters for correctness:

  * Confirmed-lazy advertising: a sidecar's tools are absent until a probe has
    reached it once; they appear after.
  * probe() reports the not-ready -> ready EDGE (so the broker notifies once).
  * call() proxies on success and DEGRADES (isError, never raises) when the
    sidecar is down, so the chat survives.
  * The dial loop notifies open sessions when a slow sidecar finally comes up
    (the mid-session recovery path), and gives up + alerts on one that never does.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import logging
import socket
from collections.abc import AsyncIterator
from typing import Any

import mcp.types as types
import pytest

from mcp_broker.broker import _CURRENT_CHAT_ID, _CURRENT_TOKEN, ToolBroker
from mcp_broker.upstream import _UNREACHABLE_WARN_AFTER, Upstream, classify_probe_result

_OFFICE = "workspace-tool-office"
_URL = "http://localhost:8090/mcp"


def _tool(name: str) -> types.Tool:
    return types.Tool(name=name, description="", inputSchema={"type": "object"})


class _FakeConn:
    def __init__(self, owner: _FakeDialer) -> None:
        self._owner = owner

    async def list_tools(self) -> list[types.Tool]:
        self._owner.list_calls += 1
        return list(self._owner.tools)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        self._owner.calls.append((name, arguments))
        return types.CallToolResult(content=[types.TextContent(type="text", text=f"ran {name}")])


class _FakeDialer:
    """A dialer that fails the first ``fail_first`` dials, then succeeds.

    Models a sidecar that is slow to start (or down): every dial within the
    failure window raises, exactly as a real connect to a not-yet-listening
    ``/mcp`` would.
    """

    def __init__(self, tools: list[types.Tool] | None = None, *, fail_first: int = 0) -> None:
        # `tools or [default]` cannot express "this upstream advertises NOTHING"
        # — an empty list is falsy and silently becomes the default. That state
        # is exactly what the empty_ready outcome exists to detect, so the
        # fixture has to be able to produce it.
        self.tools = [_tool("convert")] if tools is None else tools
        self.fail_first = fail_first
        self.dials = 0
        self.list_calls = 0
        self.calls: list[tuple[str, dict[str, Any]]] = []

    @contextlib.asynccontextmanager
    async def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[_FakeConn]:
        self.dials += 1
        if self.dials <= self.fail_first:
            raise ConnectionError("sidecar not up yet")
        yield _FakeConn(self)


class _FakeSession:
    def __init__(self, *, fail: bool = False) -> None:
        self.notified = 0
        self._fail = fail

    async def send_tool_list_changed(self) -> None:
        if self._fail:
            raise RuntimeError("session closed")
        self.notified += 1


# --- Upstream ----------------------------------------------------------------


class _RecordingDialer:
    """Captures the exact URL + headers each dial used, so a test can assert
    the broker threaded the chat id onto the sidecar dial, or the request id
    onto the outgoing headers."""

    def __init__(self) -> None:
        self.urls: list[str] = []
        self.headers: list[dict[str, str] | None] = []

    @contextlib.asynccontextmanager
    async def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[_FakeConn]:
        self.urls.append(url)
        self.headers.append(headers)
        yield _FakeConn(_FakeDialer())


async def test_call_appends_chat_id_to_the_dial_url() -> None:
    # The per-chat browser tool: chat id rides onto the sidecar dial as a query
    # param, so the sidecar routes the call to that chat's own session.
    dialer = _RecordingDialer()
    up = Upstream("workspace-tool-browser", _URL, dialer=dialer, metrics=METRICS)
    await up.call("browser_open", {"url": "https://x"}, chat_id="chat-9")
    assert dialer.urls == [f"{_URL}?chat_id=chat-9"]


async def test_call_without_chat_id_dials_the_bare_url() -> None:
    dialer = _RecordingDialer()
    up = Upstream(_OFFICE, _URL, dialer=dialer, metrics=METRICS)
    await up.call("convert", {}, chat_id=None)
    assert dialer.urls == [_URL]


async def test_dial_url_uses_ampersand_when_url_already_has_a_query() -> None:
    up = Upstream("workspace-tool-browser", f"{_URL}?a=1", dialer=_FakeDialer(), metrics=METRICS)
    assert up._dial_url("chat-9") == f"{_URL}?a=1&chat_id=chat-9"


async def test_call_sends_a_fresh_request_id_header_every_time() -> None:
    """Correlating one call across the caller's own logs and an upstream
    provider's used to need manual wall-clock timestamp matching across two
    systems with no shared identifier — see Upstream.call's docstring."""
    dialer = _RecordingDialer()
    up = Upstream(_OFFICE, _URL, dialer=dialer, metrics=METRICS)
    await up.call("convert", {})
    await up.call("convert", {})
    assert len(dialer.headers) == 2
    first, second = dialer.headers
    assert first is not None and second is not None
    assert first.keys() == {"X-Platform-Request-Id"}
    assert first["X-Platform-Request-Id"] != second["X-Platform-Request-Id"]


async def test_upstream_confirmed_lazy_then_ready() -> None:
    dialer = _FakeDialer(tools=[_tool("convert")])
    up = Upstream(_OFFICE, _URL, dialer=dialer, metrics=METRICS)

    # Confirmed-lazy: nothing advertised before the first successful probe.
    assert up.ready is False
    assert up.tools() == []

    transitioned = await up.probe()
    assert transitioned is True  # not-ready -> ready edge
    assert up.ready is True
    assert [t.name for t in up.tools()] == ["convert"]

    # A second probe stays ready but is NOT a fresh transition (no re-notify).
    assert await up.probe() is False


async def test_upstream_probe_failure_stays_invisible() -> None:
    up = Upstream(_OFFICE, _URL, dialer=_FakeDialer(fail_first=99), metrics=METRICS)
    assert await up.probe() is False
    assert up.ready is False
    assert up.tools() == []


async def test_upstream_persistent_unreachable_escalates_to_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A sidecar the broker can never reach (e.g. a NetworkPolicy dropping the SYN,
    # so the dial times out) must NAME ITSELF in the logs — the url + error — once
    # it's clearly past a cold start, not just leave a silent missing tool.
    up = Upstream(_OFFICE, _URL, dialer=_FakeDialer(fail_first=99), metrics=METRICS)
    caplog.set_level(logging.DEBUG, logger="workspace.tool_broker")

    for _ in range(_UNREACHABLE_WARN_AFTER - 1):
        await up.probe()
    assert not [r for r in caplog.records if r.levelno == logging.WARNING], (
        "must not warn during the normal cold-start window"
    )

    await up.probe()  # crosses the threshold
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1
    msg = warns[0].getMessage()
    assert _URL in msg and "STILL unreachable" in msg  # the dial target is in the log


async def test_upstream_recovery_after_escalation_logs_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Fail exactly the threshold count, then succeed — the recovery must log so the
    # outage is seen closing, not just opening.
    up = Upstream(
        _OFFICE, _URL, dialer=_FakeDialer(fail_first=_UNREACHABLE_WARN_AFTER), metrics=METRICS
    )
    caplog.set_level(logging.INFO, logger="workspace.tool_broker")

    for _ in range(_UNREACHABLE_WARN_AFTER):
        assert await up.probe() is False
    assert await up.probe() is True  # recovers

    infos = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "reachable again" in r.getMessage()
    ]
    assert len(infos) == 1 and _URL in infos[0].getMessage()


async def test_upstream_call_proxies_on_success() -> None:
    dialer = _FakeDialer()
    up = Upstream(_OFFICE, _URL, dialer=dialer, metrics=METRICS)
    result = await up.call("convert", {"path": "/x.docx"})
    assert result.isError in (False, None)
    assert dialer.calls == [("convert", {"path": "/x.docx"})]


async def test_upstream_call_degrades_when_down() -> None:
    # fail_first huge => every dial raises. call() must degrade, not raise.
    up = Upstream(
        _OFFICE,
        _URL,
        dialer=_FakeDialer(fail_first=99),
        call_retries=1,
        call_backoff_s=0.0,
        metrics=METRICS,
    )
    result = await up.call("convert", {})
    assert result.isError is True
    assert "temporarily unavailable" in result.content[0].text  # type: ignore[union-attr]


# --- R17: broker_upstream_ready honesty + sidecar_exit edge-trigger ----------


class _FlakyDialer:
    """Succeeds until ``.down`` is set, then every dial raises — models a
    sidecar that comes up (probe ok) then OOMs (calls fail)."""

    def __init__(self, tools: list[types.Tool] | None = None) -> None:
        self.tools = tools or [_tool("convert")]
        self.down = False
        self.dials = 0
        self.list_calls = 0
        self.calls: list[tuple[str, dict[str, Any]]] = []

    @contextlib.asynccontextmanager
    async def __call__(
        self, url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[_FakeConn]:
        self.dials += 1
        if self.down:
            raise ConnectionError("sidecar OOMed")
        yield _FakeConn(self)


class RecordingMetrics:
    """A BrokerMetrics that just remembers, so the tests assert on what the
    broker REPORTS rather than on some host application's registry internals.

    The library states reachability through this seam and nowhere else, so these
    counters are the whole observable surface."""

    def __init__(self) -> None:
        self.ready: dict[str, float] = {}
        self.exits: dict[tuple[str, str], float] = {}
        self.giveups: dict[str, float] = {}
        self.probes: dict[tuple[str, str], float] = {}
        self.session_notifies: dict[tuple[str, str], float] = {}
        self.tools_lists: dict[tuple[str, str], float] = {}
        self.tools_advertised: dict[str, int] = {}
        self.call_no_upstream: dict[str, float] = {}
        self.replaced: dict[str, float] = {}
        self.cleared: dict[str, float] = {}
        self.cli_sessions: dict[str, int] = {}
        self.heals: dict[tuple[str, str], float] = {}
        self.pending_heals: dict[str, int] = {}
        self.roster_refreshes: dict[tuple[str, str], float] = {}

    def set_upstream_ready(self, name: str, ready: bool) -> None:
        self.ready[name] = 1.0 if ready else 0.0

    def inc_upstream_exit(self, name: str, reason: str) -> None:
        self.exits[(name, reason)] = self.exits.get((name, reason), 0.0) + 1.0

    def inc_upstream_giveup(self, name: str) -> None:
        self.giveups[name] = self.giveups.get(name, 0.0) + 1.0

    def inc_child_probe(self, name: str, result: str) -> None:
        self.probes[(name, result)] = self.probes.get((name, result), 0.0) + 1.0

    def inc_session_notify(self, name: str, outcome: str) -> None:
        key = (name, outcome)
        self.session_notifies[key] = self.session_notifies.get(key, 0.0) + 1.0

    def inc_tools_list(self, name: str, outcome: str) -> None:
        key = (name, outcome)
        self.tools_lists[key] = self.tools_lists.get(key, 0.0) + 1.0

    def set_tools_advertised(self, name: str, count: int) -> None:
        self.tools_advertised[name] = count

    def inc_call_no_upstream(self, name: str) -> None:
        self.call_no_upstream[name] = self.call_no_upstream.get(name, 0.0) + 1.0

    def inc_session_upstream_replaced(self, name: str) -> None:
        self.replaced[name] = self.replaced.get(name, 0.0) + 1.0

    def inc_session_cleared(self, name: str) -> None:
        self.cleared[name] = self.cleared.get(name, 0.0) + 1.0

    def set_cli_sessions(self, name: str, count: int) -> None:
        self.cli_sessions[name] = count

    def inc_heal(self, name: str, outcome: str) -> None:
        key = (name, outcome)
        self.heals[key] = self.heals.get(key, 0.0) + 1.0

    def set_pending_heals(self, name: str, count: int) -> None:
        self.pending_heals[name] = count

    def inc_roster_refresh(self, name: str, outcome: str) -> None:
        key = (name, outcome)
        self.roster_refreshes[key] = self.roster_refreshes.get(key, 0.0) + 1.0


# One recorder per module run; each test uses its own upstream name, matching how
# the previous global-registry helpers were keyed.
METRICS = RecordingMetrics()


def _ready_gauge(server: str) -> float:
    return METRICS.ready.get(server, 0.0)


def _exit_count(sidecar: str, reason: str) -> float:
    return METRICS.exits.get((sidecar, reason), 0.0)


async def test_upstream_probe_failure_flips_gauge_off() -> None:
    """R17 (the latch bug): a probe that FAILS after the upstream was ready
    flips broker_upstream_ready 1→0. The old code set it True on a probe
    success and NEVER False, so a sidecar that OOMed read as 'ready' forever."""
    name = "workspace-tool-r17probeflip"
    dialer = _FlakyDialer()
    up = Upstream(name, _URL, dialer=dialer, metrics=METRICS)

    await up.probe()
    assert _ready_gauge(name) == 1.0  # came up

    dialer.down = True
    assert await up.probe() is False
    assert _ready_gauge(name) == 0.0  # bites: old code left it latched at 1


async def test_upstream_never_ready_probe_is_not_an_exit() -> None:
    """A sidecar that never came up is not an 'exit' — its failed probe sets the
    gauge 0 but must NOT count a sidecar exit (nothing came up to exit)."""
    name = "workspace-tool-r17neverup"
    up = Upstream(name, _URL, dialer=_FakeDialer(fail_first=99), metrics=METRICS)
    exits_before = _exit_count(name, "probe_unreachable")

    assert await up.probe() is False
    assert _ready_gauge(name) == 0.0
    assert _exit_count(name, "probe_unreachable") == exits_before  # no exit


async def test_upstream_ready_then_down_counts_exit_once() -> None:
    """R17: a sidecar that comes up (probe ok → gauge 1) then OOMs (calls fail)
    flips the gauge to 0 AND counts exactly ONE sidecar exit on the up→down
    edge — not one per failed call (which would make the alert meaningless)."""
    name = "workspace-tool-r17flap"
    dialer = _FlakyDialer()
    up = Upstream(name, _URL, dialer=dialer, call_retries=0, call_backoff_s=0.0, metrics=METRICS)

    assert await up.probe() is True
    assert _ready_gauge(name) == 1.0

    dialer.down = True
    exits_before = _exit_count(name, "call_unreachable")
    r1 = await up.call("convert", {})
    r2 = await up.call("convert", {})

    assert r1.isError is True and r2.isError is True
    assert _ready_gauge(name) == 0.0
    assert _exit_count(name, "call_unreachable") == exits_before + 1  # edge, counted once


async def test_upstream_recovers_flips_gauge_back_ready() -> None:
    """R17: after a down blip, a successful call flips the gauge back to 1 — the
    call path is the fastest reachability signal there is (a re-probe waits for
    the dial loop or the next rate-limited tools/list)."""
    name = "workspace-tool-r17recover"
    dialer = _FlakyDialer()
    up = Upstream(name, _URL, dialer=dialer, call_retries=0, call_backoff_s=0.0, metrics=METRICS)

    await up.probe()
    dialer.down = True
    await up.call("convert", {})
    assert _ready_gauge(name) == 0.0

    dialer.down = False
    await up.call("convert", {})
    assert _ready_gauge(name) == 1.0


async def test_upstream_call_times_out_when_wedged() -> None:
    """A sidecar that accepts the connection but NEVER answers the call must not
    hang the chat forever. ``wait_for`` caps the attempt at ``call_timeout_s`` and
    ``call()`` degrades to isError fast — this is the "stuck chat" guard. The
    outer wait_for fails the test (instead of hanging the suite) if it regresses.
    """

    class _WedgedConn:
        async def list_tools(self) -> list[types.Tool]:
            return []

        async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
            await asyncio.Event().wait()  # never resolves — the wedged upstream
            raise AssertionError("unreachable")

    @contextlib.asynccontextmanager
    async def _wedged_dialer(
        url: str, *, headers: dict[str, str] | None = None
    ) -> AsyncIterator[_WedgedConn]:
        yield _WedgedConn()

    up = Upstream(
        _OFFICE,
        _URL,
        dialer=_wedged_dialer,
        call_retries=2,
        call_backoff_s=0.0,
        call_timeout_s=0.05,
        metrics=METRICS,
    )
    result = await asyncio.wait_for(up.call("convert", {}), timeout=2.0)
    assert result.isError is True
    assert "did not respond" in result.content[0].text  # type: ignore[union-attr]


# --- ToolBroker dial loop ----------------------------------------------------


async def test_dial_loop_notifies_when_slow_sidecar_comes_up() -> None:
    # Fails 2 dials, then succeeds — the slow-start case mid-session recovery
    # is built for.
    dialer = _FakeDialer(tools=[_tool("convert")], fail_first=2)
    broker = ToolBroker(
        [(_OFFICE, _URL)],
        dialer=dialer,
        poll_min_s=0.0,
        poll_max_s=0.0,
        giveup_after_s=5.0,
        metrics=METRICS,
    )
    session = _FakeSession()
    broker._sessions[_OFFICE].add(session)  # type: ignore[arg-type]

    assert await broker._dial_until_ready(_OFFICE) is True

    assert broker._upstreams[_OFFICE].ready is True
    assert session.notified == 1  # tools/list_changed pushed exactly once


async def test_dial_loop_gives_up_and_alerts_on_dead_sidecar() -> None:
    before = _giveup_count(_OFFICE)
    dialer = _FakeDialer(fail_first=99)  # never comes up
    broker = ToolBroker(
        [(_OFFICE, _URL)],
        dialer=dialer,
        poll_min_s=0.0,
        poll_max_s=0.0,
        giveup_after_s=0.0,  # give up after the first failed probe
        metrics=METRICS,
    )
    session = _FakeSession()
    broker._sessions[_OFFICE].add(session)  # type: ignore[arg-type]

    # The whole loop returns on give-up: nothing will ever wake it (no
    # down-edge can come from an upstream that never came up).
    await asyncio.wait_for(broker._dial_loop(_OFFICE), timeout=2.0)

    assert broker._upstreams[_OFFICE].ready is False
    assert session.notified == 0  # never advertised, so never notified
    assert _giveup_count(_OFFICE) == before + 1  # alert metric fired


async def test_notify_prunes_dead_sessions() -> None:
    broker = ToolBroker([(_OFFICE, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    live = _FakeSession()
    closed = _FakeSession(fail=True)
    broker._sessions[_OFFICE].update({live, closed})  # type: ignore[arg-type]

    await broker._notify(_OFFICE)

    assert live.notified == 1
    assert closed not in broker._sessions[_OFFICE]  # pruned
    assert live in broker._sessions[_OFFICE]


@contextlib.contextmanager
def _capture(logger_name: str):
    """Collect records emitted on ``logger_name`` for the block's duration."""
    import logging

    records: list[logging.LogRecord] = []

    class _H(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(logger_name)
    handler = _H()
    prev_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)


def _giveup_count(server: str) -> float:
    return METRICS.giveups.get(server, 0.0)


# --- classified cross-pod probe result (incident 2026-09-04) -----------------
#
# The broker used to fold every probe failure into a bare ready=False, so a
# cutover child that was NEVER reachable (loopback bind / no Service route) read
# the same as one still booting. classify_probe_result + inc_child_probe on every
# probe give the distinguishing axis.


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (None, "reachable"),
        (TimeoutError("slow"), "timeout"),
        (ConnectionRefusedError(errno.ECONNREFUSED, "refused"), "conn_refused"),
        (OSError(errno.ECONNREFUSED, "Connection refused"), "conn_refused"),
        (socket.gaierror(-2, "Name or service not known"), "dns_fail"),
        (RuntimeError("Server returned status 503"), "http_error"),
        # httpx-shaped: no OSError in the chain, classify by message text.
        (RuntimeError("connect operation timed out"), "timeout"),
        (RuntimeError("[Errno 111] Connection refused"), "conn_refused"),
    ],
)
def test_classify_probe_result(exc: BaseException | None, expected: str) -> None:
    assert classify_probe_result(exc) == expected


def test_classify_probe_result_walks_the_cause_chain() -> None:
    # httpx wraps the OSError, so the top exception is a generic transport error
    # and the errno lives on __cause__ — the classifier must find it.
    root = ConnectionRefusedError(errno.ECONNREFUSED, "refused")
    wrapped = RuntimeError("All connection attempts failed")
    wrapped.__cause__ = root
    assert classify_probe_result(wrapped) == "conn_refused"


async def test_probe_reports_reachable_result_on_success() -> None:
    m = RecordingMetrics()
    up = Upstream("workspace-tool-telegram", _URL, dialer=_FakeDialer(), metrics=m)
    await up.probe()
    assert m.probes.get(("workspace-tool-telegram", "reachable")) == 1.0


async def test_probe_reports_conn_refused_on_a_refusing_child() -> None:
    # A child that binds loopback in another pod: the pod answers but nothing is
    # listening on the child port -> ECONNREFUSED, the bind-gap fingerprint.
    class _RefusingDialer(_FakeDialer):
        @contextlib.asynccontextmanager
        async def __call__(self, url: str, *, headers: dict[str, str] | None = None):  # type: ignore[override]
            self.dials += 1
            raise ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
            yield  # pragma: no cover

    m = RecordingMetrics()
    up = Upstream("workspace-tool-telegram", _URL, dialer=_RefusingDialer(), metrics=m)
    assert await up.probe() is False
    assert m.probes.get(("workspace-tool-telegram", "conn_refused")) == 1.0
    # Every probe reports — a never-reachable child is NOT silent on this axis.
    assert await up.probe() is False
    assert m.probes.get(("workspace-tool-telegram", "conn_refused")) == 2.0


def _session_notify(server: str, outcome: str) -> float:
    return METRICS.session_notifies.get((server, outcome), 0.0)


async def test_register_session_reports_no_sessions_when_nobody_is_listening() -> None:
    """THE instrument for the invisible-connector race.

    A per-user child registered BEFORE the CLI has ever issued a tools/list for
    that server reaches nobody: _sessions is populated only from inside the
    list handler. The upstream is healthy, the log says ready=True, and the
    tools are invisible for the life of the session. Without this counter the
    two outcomes are indistinguishable.
    """
    name = "sess-notify-nobody"
    before = _session_notify(name, "no_sessions")
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)

    await broker.register_session(name, "tok-1", _URL)

    assert _session_notify(name, "no_sessions") == before + 1
    assert _session_notify(name, "delivered") == 0.0


async def test_register_session_reports_delivered_when_a_cli_is_listening() -> None:
    """The positive control: with a live CLI session the notify lands, the
    tools appear mid-session, and the counter says so. Without this half, a
    permanently-zero 'delivered' would look like healthy silence."""
    name = "sess-notify-live"
    before = _session_notify(name, "delivered")
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    session = _FakeSession()
    broker._sessions[name].add(session)  # type: ignore[arg-type]
    # The CLI records what it is listening under on its first tools/list; a
    # session with no recorded token is a DIFFERENT outcome (below), so the
    # positive control has to look like a real listener.
    broker._session_ctx[session] = ("tok-2", "chat-live")  # type: ignore[index]

    await broker.register_session(name, "tok-2", _URL, chat_id="chat-live")

    assert _session_notify(name, "delivered") == before + 1
    assert session.notified == 1


async def test_a_wedged_cli_session_counts_as_nobody_reached() -> None:
    """A session that cannot be told is not a session that was told — pruning
    it must not be recorded as a successful delivery."""
    name = "sess-notify-wedged"
    before = _session_notify(name, "no_sessions")
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    broker._sessions[name].add(_FakeSession(fail=True))  # type: ignore[arg-type]

    await broker.register_session(name, "tok-3", _URL)

    assert _session_notify(name, "no_sessions") == before + 1


def _tools_list(server: str, outcome: str) -> float:
    return METRICS.tools_lists.get((server, outcome), 0.0)


async def test_tools_list_reports_empty_when_no_upstream_is_registered() -> None:
    """The single most important signal: the agent asked what tools exist and
    was told NONE. Previously this produced no record at all, which is why a
    connected-but-unroutable connector was invisible for a whole day."""
    name = "listsig-no-upstream"
    before = _tools_list(name, "empty_no_token")
    broker = ToolBroker(
        [(name, _URL)], dialer=_FakeDialer(), session_scoped={name}, metrics=METRICS
    )

    assert await broker._list_tools_for(name) == []

    # No token in context, so this is the unhealable half of the split: the
    # CLI's MCP URL never carried one and no later attach can change that.
    assert _tools_list(name, "empty_no_token") == before + 1
    assert METRICS.tools_advertised[name] == 0


async def test_tools_list_distinguishes_ready_but_empty_from_absent() -> None:
    """A child that is READY and advertises nothing is misconfigured; one that
    is absent is unrouted. They look identical to the agent and need opposite
    fixes, so they must not share an outcome."""
    name = "listsig-ready-empty"
    before = _tools_list(name, "empty_ready")
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(tools=[]), metrics=METRICS)
    await broker._upstreams[name].probe()

    assert await broker._list_tools_for(name) == []

    assert _tools_list(name, "empty_ready") == before + 1
    assert _tools_list(name, "empty_not_ready") == 0.0


async def test_tools_list_reports_served_and_the_count() -> None:
    """The positive control — a permanently-zero 'served' would otherwise look
    like healthy silence."""
    name = "listsig-served"
    before = _tools_list(name, "served")
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    await broker._upstreams[name].probe()

    tools = await broker._list_tools_for(name)

    assert tools
    assert _tools_list(name, "served") == before + 1
    assert METRICS.tools_advertised[name] == len(tools)


async def test_a_call_with_no_upstream_is_counted() -> None:
    """The agent gets a polite 'not connected yet', which reads to the user as
    the assistant being unable rather than the platform failing to route."""
    name = "callsig-no-upstream"
    broker = ToolBroker(
        [(name, _URL)], dialer=_FakeDialer(), session_scoped={name}, metrics=METRICS
    )
    server = broker._build_server(name)
    handler = server.request_handlers[types.CallToolRequest]

    await handler(
        types.CallToolRequest(
            method="tools/call",
            params=types.CallToolRequestParams(name="whatever", arguments={}),
        )
    )

    assert METRICS.call_no_upstream.get(name, 0.0) == 1.0


async def test_re_registering_a_session_upstream_is_recorded() -> None:
    """The old upstream is dropped without being closed. Harmless once, a churn
    signal in bulk — and previously indistinguishable from a first attach."""
    name = "replace-sig"
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)

    await broker.register_session(name, "tok", _URL)
    await broker.register_session(name, "tok", _URL)

    assert METRICS.replaced.get(name, 0.0) == 1.0


async def test_clearing_a_session_counts_what_it_dropped() -> None:
    name = "clear-sig"
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    await broker.register_session(name, "tok-clear", _URL)

    broker.clear_session("tok-clear")

    assert METRICS.cleared.get(name, 0.0) == 1.0
    assert (name, "tok-clear") not in broker._session_upstreams


async def test_clear_session_does_not_prune_cli_sessions() -> None:
    """Documents a real limitation rather than pretending it away: _sessions is
    keyed by server name, not by token, so clear_session CANNOT know which
    entries belonged to the ending session. They are pruned lazily on the next
    failed notify, and until then each stale entry costs one notify timeout on
    the attach path. If this ever becomes token-keyed, this test should flip."""
    name = "clear-cli-sessions"
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    broker._sessions[name].add(_FakeSession())  # type: ignore[arg-type]
    await broker.register_session(name, "tok-x", _URL)

    broker.clear_session("tok-x")

    assert len(broker._sessions[name]) == 1, "behaviour changed — update the docstring above"


async def test_a_notify_that_reaches_only_OTHER_sessions_is_not_delivered() -> None:
    """The failure the old counter could not express, seen live on stg
    2026-09-06: the notify landed on two CLI sessions and neither was the one
    holding this token. "delivered" was true and the agent that was waiting
    heard nothing, so the tools stayed invisible while the metric looked
    healthy. A foreign delivery must not read as a delivery."""
    name = "sess-notify-foreign"
    before = _session_notify(name, "delivered_foreign_only")
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), metrics=METRICS)
    stranger = _FakeSession()
    broker._sessions[name].add(stranger)  # type: ignore[arg-type]
    broker._session_ctx[stranger] = ("someone-elses-token", "other-chat")  # type: ignore[index]

    await broker.register_session(name, "tok-mine", _URL)

    assert _session_notify(name, "delivered_foreign_only") == before + 1
    assert _session_notify(name, "delivered") == 0.0
    assert stranger.notified == 1  # it WAS told; it just was not the one waiting


def _heal(server: str, outcome: str) -> float:
    return METRICS.heals.get((server, outcome), 0.0)


async def test_the_heal_is_recorded_when_tools_actually_reach_the_agent() -> None:
    """The notify is a message we send; the heal is the outcome the user gets.
    Only a later list that actually SERVES tools proves the second."""
    name = "heal-served"
    broker = ToolBroker(
        [(name, _URL)], dialer=_FakeDialer(), session_scoped={name}, metrics=METRICS
    )
    token = _CURRENT_TOKEN.set("tok-heal")
    try:
        await broker.register_session(name, "tok-heal", _URL, chat_id="chat-heal")
        assert METRICS.pending_heals[name] == 1

        assert await broker._list_tools_for(name)
    finally:
        _CURRENT_TOKEN.reset(token)

    assert _heal(name, "served") == 1.0
    assert METRICS.pending_heals[name] == 0


async def test_a_session_that_ends_still_waiting_is_recorded_as_never_served() -> None:
    """The verdict has to be taken at teardown: after the session is gone there
    is no longer any way to know it never saw the tools it was promised."""
    name = "heal-never"
    broker = ToolBroker(
        [(name, _URL)], dialer=_FakeDialer(), session_scoped={name}, metrics=METRICS
    )
    await broker.register_session(name, "tok-never", _URL, chat_id="chat-never")

    broker.clear_session("tok-never")

    assert _heal(name, "never_served") == 1.0
    assert METRICS.pending_heals[name] == 0


async def test_an_empty_list_separates_a_missing_token_from_an_unattached_child() -> None:
    """These look identical to the agent and need opposite fixes. A missing
    token can NEVER heal — the CLI's MCP URL was fixed when the agent spawned —
    while an unattached child heals on the next list. Sharing one label is what
    made a bug and ordinary startup indistinguishable for a day."""
    name = "empty-split"
    broker = ToolBroker(
        [(name, _URL)], dialer=_FakeDialer(), session_scoped={name}, metrics=METRICS
    )

    assert await broker._list_tools_for(name) == []
    no_token = _tools_list(name, "empty_no_token")

    reset = _CURRENT_TOKEN.set("tok-present-but-unattached")
    try:
        assert await broker._list_tools_for(name) == []
    finally:
        _CURRENT_TOKEN.reset(reset)

    assert no_token == 1.0
    assert _tools_list(name, "empty_unregistered") == 1.0


# --- The result-filter seam ---------------------------------------------------


class _RecordingFilter:
    """A ResultFilter that records every call and either rewrites, passes
    through, or raises — the three behaviours the seam's contract covers."""

    def __init__(
        self, *, replace_with: types.CallToolResult | None = None, raise_: bool = False
    ) -> None:
        self.replace_with = replace_with
        self.raise_ = raise_
        self.seen: list[tuple[str, str, dict[str, Any], types.CallToolResult, str | None]] = []

    async def filter_result(
        self,
        name: str,
        tool_name: str,
        arguments: dict[str, Any],
        result: types.CallToolResult,
        *,
        chat_id: str | None,
    ) -> types.CallToolResult:
        self.seen.append((name, tool_name, arguments, result, chat_id))
        if self.raise_:
            raise RuntimeError("filter bug")
        return self.replace_with or result


async def _proxy_call(broker: ToolBroker, name: str, tool: str = "convert") -> types.CallToolResult:
    handler = broker._build_server(name).request_handlers[types.CallToolRequest]
    res = await handler(
        types.CallToolRequest(
            method="tools/call",
            params=types.CallToolRequestParams(name=tool, arguments={"a": 1}),
        )
    )
    return res.root  # type: ignore[return-value]  # ServerResult wraps the CallToolResult


async def test_result_filter_sees_the_call_and_its_answer_is_what_the_agent_gets() -> None:
    """The seam's whole point: the filter receives the upstream's real answer
    (plus who called what, under which chat) and whatever it returns is what
    reaches the agent — the raw result never bypasses it."""
    name = "filter-rewrite"
    rewritten = types.CallToolResult(content=[types.TextContent(type="text", text="rewritten")])
    flt = _RecordingFilter(replace_with=rewritten)
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), result_filter=flt)

    reset = _CURRENT_CHAT_ID.set("chat-42")
    try:
        got = await _proxy_call(broker, name)
    finally:
        _CURRENT_CHAT_ID.reset(reset)

    assert got.content[0].text == "rewritten"  # type: ignore[union-attr]
    ((seen_name, seen_tool, seen_args, seen_result, seen_chat),) = flt.seen
    assert (seen_name, seen_tool, seen_args, seen_chat) == (name, "convert", {"a": 1}, "chat-42")
    assert seen_result.content[0].text == "ran convert"  # type: ignore[union-attr]


async def test_default_filter_passes_the_upstream_answer_through() -> None:
    name = "filter-default"
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer())

    got = await _proxy_call(broker, name)

    assert got.isError is False
    assert got.content[0].text == "ran convert"  # type: ignore[union-attr]


async def test_a_raising_filter_withholds_the_result_instead_of_leaking_it() -> None:
    """A host installs a filter to ENFORCE something (visibility, redaction).
    A bug in it must fail closed: the agent gets an error result, and the
    upstream's unfiltered answer never reaches it."""
    name = "filter-raises"
    flt = _RecordingFilter(raise_=True)
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), result_filter=flt)

    got = await _proxy_call(broker, name)

    assert got.isError is True
    assert "ran convert" not in got.content[0].text  # type: ignore[union-attr]
    assert len(flt.seen) == 1  # it was consulted, it just could not decide


async def test_filter_gets_none_chat_id_when_the_url_carried_none() -> None:
    name = "filter-no-chat"
    flt = _RecordingFilter()
    broker = ToolBroker([(name, _URL)], dialer=_FakeDialer(), result_filter=flt)

    await _proxy_call(broker, name)

    assert flt.seen[0][4] is None
