"""The advertised roster follows the PROCESS behind an upstream, not its address.

Found 2026-09-14: two replicas of one tenant's workspace advertised two
different ``browser_login`` signatures for the same browser tool pod. The
broker probed each sidecar once, cached the roster, and never looked again —
so the replica that started BEFORE the tool image rolled kept serving the
predecessor's schema for its whole life. Two triggers close it, and these
tests pin both plus the edges around them:

  1. A ``tools/list`` re-confirms a READY upstream's roster when it has not
     been probed recently; a changed roster fans out ``list_changed`` to every
     open session, an unchanged one is quiet.
  2. The up→down edge DROPS the cached roster and wakes the dial loop, which
     re-dials and re-announces whatever answers next.

The fixtures are deliberately the same fakes ``test_broker`` uses: a dialer
whose tool list can be swapped mid-test is the rolled image; ``.down`` is the
restart window.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Any

import mcp.types as types
import pytest
from test_broker import METRICS, _FakeConn, _FakeSession

from mcp_broker.broker import _CURRENT_TOKEN, ToolBroker
from mcp_broker.upstream import ROSTER_REFRESH_OUTCOMES, Upstream, roster_diff, roster_shape

_URL = "http://localhost:8090/mcp"


def _tool(name: str, *params: str) -> types.Tool:
    schema = {"type": "object", "properties": {p: {"type": "string"} for p in params}}
    return types.Tool(name=name, description="", inputSchema=schema)


# The shape of the real incident: same tool, one more parameter.
_V1 = [_tool("browser_login", "portal_id")]
_V2 = [_tool("browser_login", "portal_id", "ref")]


class _RollingDialer:
    """A dialer whose advertised roster and reachability can be changed mid-test:
    swapping ``.tools`` is a rolled image, ``.down`` is the restart window."""

    def __init__(self, tools: list[types.Tool]) -> None:
        self.tools = tools
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
            raise ConnectionError("sidecar restarting")
        yield _FakeConn(self)  # type: ignore[arg-type]


def _refreshes(name: str, outcome: str) -> float:
    return METRICS.roster_refreshes.get((name, outcome), 0.0)


def _exits(name: str, reason: str) -> float:
    return METRICS.exits.get((name, reason), 0.0)


def _upstream(name: str, dialer: _RollingDialer) -> Upstream:
    return Upstream(name, _URL, dialer=dialer, call_retries=0, call_backoff_s=0.0, metrics=METRICS)


def _broker(name: str, dialer: _RollingDialer, **kw: Any) -> ToolBroker:
    return ToolBroker(
        [(name, _URL)], dialer=dialer, poll_min_s=0.0, poll_max_s=0.0, metrics=METRICS, **kw
    )


def _force_stale(broker: ToolBroker, name: str) -> None:
    """Age the upstream past the re-probe rate-limit window without sleeping."""
    broker._upstreams[name]._last_probe_t = None


# --- roster shape ---------------------------------------------------------------


def test_roster_shape_is_keyed_by_name_and_ignores_order() -> None:
    a, b = _tool("x", "p"), _tool("y")
    assert roster_shape([a, b]) == roster_shape([b, a])


def test_roster_shape_sees_a_schema_change_on_a_same_named_tool() -> None:
    assert roster_shape(_V1) != roster_shape(_V2)
    assert roster_diff(roster_shape(_V1), roster_shape(_V2)) == (
        "added=[] removed=[] reshaped=['browser_login']"
    )


def test_roster_diff_names_added_and_removed_tools() -> None:
    before = roster_shape([_tool("keep"), _tool("gone")])
    after = roster_shape([_tool("keep"), _tool("new")])
    assert roster_diff(before, after) == "added=['new'] removed=['gone'] reshaped=[]"


def test_roster_refresh_outcomes_is_the_closed_set_the_code_emits() -> None:
    """A consumer pre-seeding metric labels pins itself to this constant, so a
    new outcome here without a seed there fails loudly rather than reading as
    'no data'."""
    assert {"dropped", "changed", "unchanged"} == ROSTER_REFRESH_OUTCOMES


# --- Upstream: re-probe verdicts -----------------------------------------------


async def test_reprobe_with_same_roster_is_quiet() -> None:
    name = "workspace-tool-roster-same"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    assert await up.probe() is True  # came ready: the existing edge
    dialer.tools = list(reversed(_V1))  # a reorder is not a change
    assert await up.probe() is False
    assert _refreshes(name, "unchanged") == 1.0
    assert _refreshes(name, "changed") == 0.0


async def test_reprobe_with_changed_roster_asks_for_a_renotify(
    caplog: pytest.LogCaptureFixture,
) -> None:
    name = "workspace-tool-roster-changed"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    caplog.set_level(logging.WARNING, logger="workspace.tool_broker")

    dialer.tools = list(_V2)  # the image rolled: same tool, one more parameter
    assert await up.probe() is True  # bites: the old code returned `not was_ready` == False

    assert [t.inputSchema["properties"].keys() for t in up.tools()] == [{"portal_id", "ref"}]
    assert _refreshes(name, "changed") == 1.0
    msgs = [r.getMessage() for r in caplog.records]
    assert any("roster changed" in m and "reshaped=['browser_login']" in m for m in msgs), msgs


async def test_first_roster_is_neither_changed_nor_unchanged() -> None:
    """There is nothing to compare the first probe to — counting it as
    'unchanged' would inflate the denominator with every pod boot."""
    name = "workspace-tool-roster-first"
    up = _upstream(name, _RollingDialer(list(_V1)))
    await up.probe()
    assert _refreshes(name, "unchanged") == 0.0
    assert _refreshes(name, "changed") == 0.0


# --- Upstream: the down edge drops the roster ----------------------------------


async def test_down_edge_drops_the_roster_and_wakes_the_redialer() -> None:
    name = "workspace-tool-roster-drop"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    assert up.ready is True

    dialer.down = True
    result = await up.call("browser_login", {})
    assert result.isError is True  # the existing degrade path is untouched

    assert up.ready is False
    assert up.tools() == []  # nothing is behind that menu any more
    assert _refreshes(name, "dropped") == 1.0
    await asyncio.wait_for(up.wait_lost(), timeout=1.0)  # the dial loop's wake-up


async def test_a_second_failure_in_the_same_outage_drops_nothing_more() -> None:
    """The drop is edge-triggered like the exit counter: N failed calls during
    one restart window are one drop, and the loop is woken once."""
    name = "workspace-tool-roster-drop-once"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    dialer.down = True
    await asyncio.gather(up.call("a", {}), up.call("b", {}), up.call("c", {}))
    assert _refreshes(name, "dropped") == 1.0
    assert _exits(name, "call_unreachable") == 1.0


async def test_failure_on_a_never_ready_upstream_has_nothing_to_drop() -> None:
    name = "workspace-tool-roster-never"
    dialer = _RollingDialer(list(_V1))
    dialer.down = True
    up = _upstream(name, dialer)
    assert await up.probe() is False
    await up.call("x", {})
    assert _refreshes(name, "dropped") == 0.0
    assert not up._lost.is_set()


async def test_failed_reprobe_on_a_ready_upstream_is_also_the_edge() -> None:
    """Trigger 1 finding the sidecar mid-restart is the same edge as a failed
    call: drop, count the exit, wake the loop."""
    name = "workspace-tool-roster-probe-edge"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    dialer.down = True
    assert await up.probe() is False
    assert up.ready is False
    assert _refreshes(name, "dropped") == 1.0
    assert _exits(name, "probe_unreachable") == 1.0


async def test_recovery_after_a_roll_compares_with_the_pre_roll_roster() -> None:
    """The roster that comes back is judged against the one the OLD process
    advertised — that is the question 'did the deploy change the menu?'."""
    name = "workspace-tool-roster-roll-changed"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    dialer.down = True
    await up.call("x", {})  # edge: dropped
    dialer.down = False
    dialer.tools = list(_V2)  # what replaced it advertises more

    assert await up.probe() is True  # ready again AND a new menu -> notify
    assert _refreshes(name, "changed") == 1.0
    assert _refreshes(name, "dropped") == 1.0


async def test_recovery_after_a_roll_with_the_same_image_still_renotifies() -> None:
    """Same roster after a restart is 'unchanged' for the metric but the
    upstream DID go not-ready -> ready, and sessions that listed during the
    outage saw an empty menu — they must be told it is back."""
    name = "workspace-tool-roster-roll-same"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    dialer.down = True
    await up.call("x", {})
    dialer.down = False

    assert await up.probe() is True
    assert _refreshes(name, "unchanged") == 1.0


async def test_wait_lost_rearms_for_the_next_edge() -> None:
    name = "workspace-tool-roster-rearm"
    dialer = _RollingDialer(list(_V1))
    up = _upstream(name, dialer)
    await up.probe()
    dialer.down = True
    await up.call("x", {})
    await asyncio.wait_for(up.wait_lost(), timeout=1.0)
    assert not up._lost.is_set()  # cleared: a second wait would block
    dialer.down = False
    await up.probe()
    dialer.down = True
    await up.call("x", {})
    await asyncio.wait_for(up.wait_lost(), timeout=1.0)  # second edge wakes again
    assert _refreshes(name, "dropped") == 2.0


# --- Broker: trigger 2, the dial loop re-dials on the edge ---------------------


async def _until(pred: Any, *, budget_s: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + budget_s
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition never held")
        await asyncio.sleep(0.005)


async def test_dial_loop_redials_after_the_edge_and_renotifies_open_sessions() -> None:
    """The live-chat half of the fix: a session open across a sidecar roll is
    told to re-list once the replacement answers, without a pod restart."""
    name = "workspace-tool-roster-loop"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer, giveup_after_s=5.0)
    session = _FakeSession()
    broker._sessions[name].add(session)  # type: ignore[arg-type]

    task = asyncio.create_task(broker._dial_loop(name))
    try:
        await _until(lambda: session.notified == 1)
        up = broker._upstreams[name]

        dialer.down = True
        await up.call("browser_login", {})  # the failed call IS the edge
        assert up.ready is False
        dialer.down = False
        dialer.tools = list(_V2)

        await _until(lambda: session.notified == 2)  # bites: old loop had returned
        assert up.ready is True
        assert [t.inputSchema["properties"].keys() for t in up.tools()] == [{"portal_id", "ref"}]
        assert not task.done()  # still owning the upstream for the next roll
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_dial_loop_survives_two_rolls() -> None:
    name = "workspace-tool-roster-loop-twice"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer, giveup_after_s=5.0)
    session = _FakeSession()
    broker._sessions[name].add(session)  # type: ignore[arg-type]
    task = asyncio.create_task(broker._dial_loop(name))
    try:
        up = broker._upstreams[name]
        for expected in (2, 3):
            await _until(lambda n=expected: session.notified == n - 1)
            dialer.down = True
            await up.call("x", {})
            dialer.down = False
            await _until(lambda n=expected: session.notified == n)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_dial_loop_gives_up_per_phase_after_a_roll_that_never_returns() -> None:
    """A sidecar that rolls and never comes back is alerted like one that never
    came up: the re-dial phase has its own give-up cap, and the loop ends."""
    name = "workspace-tool-roster-loop-giveup"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer, giveup_after_s=0.0)
    before = METRICS.giveups.get(name, 0.0)
    task = asyncio.create_task(broker._dial_loop(name))
    try:
        up = broker._upstreams[name]
        await _until(lambda: up.ready)
        dialer.down = True
        await up.call("x", {})
        await asyncio.wait_for(task, timeout=2.0)  # returned: gave up on the re-dial
        assert METRICS.giveups.get(name, 0.0) == before + 1
    finally:
        if not task.done():
            task.cancel()


# --- Broker: trigger 1, tools/list re-confirms a ready upstream ----------------


async def test_tools_list_reprobes_a_stale_ready_upstream_and_serves_the_new_roster() -> None:
    """The new-chat half of the fix: a fresh session's first tools/list gets
    the roster the sidecar has NOW, not the one cached at pod boot."""
    name = "workspace-tool-roster-list"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer)
    up = broker._upstreams[name]
    await up.probe()
    dialer.tools = list(_V2)  # rolled while nobody was calling it
    _force_stale(broker, name)

    tools = await broker._list_tools_for(name)

    assert [t.inputSchema["properties"].keys() for t in tools] == [{"portal_id", "ref"}]
    assert METRICS.tools_lists[(name, "served")] >= 1.0


async def test_tools_list_reprobe_with_a_changed_roster_notifies_the_other_sessions() -> None:
    """The session that listed already has the new roster; the OTHERS on this
    pod do not, so a change found here fans out to all of them."""
    name = "workspace-tool-roster-list-notify"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer)
    other = _FakeSession()
    broker._sessions[name].add(other)  # type: ignore[arg-type]
    await broker._upstreams[name].probe()
    dialer.tools = list(_V2)
    _force_stale(broker, name)

    await broker._list_tools_for(name)

    assert other.notified == 1


async def test_tools_list_reprobe_with_an_unchanged_roster_stays_silent() -> None:
    """Routine re-confirmation must not storm every session with list_changed."""
    name = "workspace-tool-roster-list-quiet"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer)
    other = _FakeSession()
    broker._sessions[name].add(other)  # type: ignore[arg-type]
    await broker._upstreams[name].probe()
    _force_stale(broker, name)

    await broker._list_tools_for(name)

    assert other.notified == 0
    assert _refreshes(name, "unchanged") == 1.0


async def test_tools_list_reprobe_is_rate_limited_for_ready_upstreams() -> None:
    """Back-to-back lists inside the window serve the cache — a ready upstream
    is not dialed once per list."""
    name = "workspace-tool-roster-list-ratelimit"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer, probe_timeout_s=5.0)  # window = 10s
    await broker._upstreams[name].probe()
    dials = dialer.dials

    await broker._list_tools_for(name)
    await broker._list_tools_for(name)

    assert dialer.dials == dials  # inside the window: served from cache


async def test_tools_list_on_a_ready_upstream_mid_restart_reports_empty_not_ready() -> None:
    """Honest, not stale: if the re-probe finds the sidecar gone, the list says
    so (and the drop/exit edge fires) instead of serving a menu nobody is behind.
    The dial loop then re-announces it."""
    name = "workspace-tool-roster-list-down"
    dialer = _RollingDialer(list(_V1))
    broker = _broker(name, dialer)
    await broker._upstreams[name].probe()
    dialer.down = True
    _force_stale(broker, name)

    assert await broker._list_tools_for(name) == []

    assert METRICS.tools_lists[(name, "empty_not_ready")] >= 1.0
    assert METRICS.tools_advertised[name] == 0
    assert _refreshes(name, "dropped") == 1.0
    assert _exits(name, "probe_unreachable") == 1.0


async def test_tools_list_does_not_reprobe_a_never_ready_static_sidecar() -> None:
    """Static sidecars that never came up belong to the dial loop; probing them
    on tools/list would cost every new session a probe timeout on its first
    list once the loop has given up."""
    name = "workspace-tool-roster-list-static-down"
    dialer = _RollingDialer(list(_V1))
    dialer.down = True
    broker = _broker(name, dialer)
    _force_stale(broker, name)

    assert await broker._list_tools_for(name) == []

    assert dialer.dials == 0
    assert METRICS.tools_lists[(name, "empty_not_ready")] >= 1.0


async def test_tools_list_reprobes_a_ready_session_scoped_child_too() -> None:
    """Per-user children have no dial loop, so trigger 1 is their only roster
    refresh; a re-registered child with a new tool set is served as such."""
    name = "workspace-tool-roster-list-session"
    dialer = _RollingDialer(list(_V1))
    broker = ToolBroker(
        [(name, _URL)],
        dialer=dialer,
        session_scoped={name},
        session_probe_budget_s=0.0,
        metrics=METRICS,
    )
    assert await broker.register_session(name, "tok-a", _URL) is True
    dialer.tools = list(_V2)
    broker._session_upstreams[(name, "tok-a")]._last_probe_t = None

    reset = _CURRENT_TOKEN.set("tok-a")
    try:
        tools = await broker._list_tools_for(name)
    finally:
        _CURRENT_TOKEN.reset(reset)
    assert [t.inputSchema["properties"].keys() for t in tools] == [{"portal_id", "ref"}]
