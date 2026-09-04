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

from mcp_broker.broker import ToolBroker
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
        self.tools = tools or [_tool("convert")]
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

    def set_upstream_ready(self, name: str, ready: bool) -> None:
        self.ready[name] = 1.0 if ready else 0.0

    def inc_upstream_exit(self, name: str, reason: str) -> None:
        self.exits[(name, reason)] = self.exits.get((name, reason), 0.0) + 1.0

    def inc_upstream_giveup(self, name: str) -> None:
        self.giveups[name] = self.giveups.get(name, 0.0) + 1.0

    def inc_child_probe(self, name: str, result: str) -> None:
        self.probes[(name, result)] = self.probes.get((name, result), 0.0) + 1.0


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
    broker never re-probes a ready upstream, so the call path is the only signal
    that can recover it."""
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

    await broker._dial_loop(_OFFICE)

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

    await broker._dial_loop(_OFFICE)

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
