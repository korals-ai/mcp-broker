"""State and connection logic for ONE sidecar the broker fronts.

An :class:`Upstream` holds a sidecar's real ``/mcp`` URL and its last-known tool
list, and knows how to (1) *probe* the sidecar to confirm it is up and cache its
tools, and (2) *proxy* a tool call to it with a bounded retry budget. It knows
nothing about the broker's MCP server face (see :mod:`mcp_broker.broker`);
the broker drives it.

Advertising is **confirmed-lazy**: a sidecar's tools are exposed only after a
probe has reached it at least once. So a sidecar that never comes up stays
invisible to the agent (no phantom tool to hang on), and one that comes up late
appears mid-session once the broker notifies (see the broker's dial loop).

And it stays confirmed only while the sidecar stays reachable. The cached roster
describes the PROCESS that answered the probe; once that process is observed
gone (a failed call or a failed re-probe — the up→down edge) the roster is
dropped, and whatever replaces it is probed afresh. Without that, a sidecar
rolled to a new image behind a stable Service kept advertising its predecessor's
tool schemas for the workspace pod's whole life (2026-09-14: two replicas of one
tenant advertised two different ``browser_login`` signatures for the same
browser pod).
"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import logging
import re
import socket
import uuid
from typing import Any
from urllib.parse import quote

import mcp.types as types

from mcp_broker.dialer import Dialer
from mcp_broker.metrics import NULL_METRICS, BrokerMetrics

log = logging.getLogger("workspace.tool_broker")

# The complete, closed vocabulary classify_probe_result can return. Exported so a
# consumer that turns these into metric labels can pre-seed EXACTLY this set (an
# unseeded label is absent-until-first-increment, so a result that never happens
# to fire reads as "no data" instead of 0). A join test pins the consumer's label
# set to this constant, so adding a result here without seeding it fails loudly.
PROBE_RESULTS = frozenset(
    {"reachable", "timeout", "conn_refused", "dns_fail", "session_unknown", "http_error"}
)

# Up→down reasons that are NOT a sidecar exit: the sidecar answered, it just no
# longer knows this session's token (a router in front of per-session children
# 404s a detached token). Dropping the roster is still right — calls under that
# token cannot succeed — but counting it as an exit raised a sidecar-exit alert
# for a sidecar that never restarted: a warm agent on a retired token re-listed
# after ANOTHER session's attach notify. In one measured week that shape was
# 32 of 35 exit edges on session-scoped connectors.
_NOT_AN_EXIT = frozenset({"probe_session_unknown", "call_session_unknown"})


def _exception_tree(exc: BaseException) -> list[BaseException]:
    """``exc`` plus everything reachable through ``__cause__`` / ``__context__``
    AND ``ExceptionGroup`` members — the mcp client surfaces a failed request as
    a TaskGroup ``ExceptionGroup`` whose real error sits inside ``.exceptions``,
    where a cause/context walk alone never looks."""
    seen: set[int] = set()
    out: list[BaseException] = []
    stack: list[BaseException] = [exc]
    while stack:
        cur = stack.pop(0)
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        out.append(cur)
        if isinstance(cur, BaseExceptionGroup):
            stack.extend(cur.exceptions)
        stack.extend(nxt for nxt in (cur.__cause__, cur.__context__) if nxt is not None)
    return out


# How the ``mcp`` streamable-HTTP client reports an HTTP 404 on a request: not
# as an HTTP error but as a JSON-RPC error it synthesises itself —
# ``ErrorData(code=32600, message="Session terminated")`` (positive 32600, the
# client's own constant). The broker dials a FRESH session per operation, so a
# 404 on it can only mean the URL itself is unknown: the data router's reply to
# a token it no longer holds.
_MCP_CLIENT_404_CODE = 32600


def _is_not_found(exc: BaseException) -> bool:
    """Whether ``exc`` is the upstream answering 404, duck-typed so the broker
    stays independent of the transport library: the ``mcp`` client's
    synthesised session-terminated error, or an HTTP error carrying
    ``.response.status_code == 404`` (a plain-HTTP dialer)."""
    if getattr(getattr(exc, "error", None), "code", None) == _MCP_CLIENT_404_CODE:
        return True
    return getattr(getattr(exc, "response", None), "status_code", None) == 404


def classify_probe_result(exc: BaseException | None) -> str:
    """Map a probe outcome to the low-cardinality ``inc_child_probe`` axis:
    ``reachable`` (exc is None) | ``timeout`` | ``conn_refused`` | ``dns_fail`` |
    ``session_unknown`` | ``http_error``. Transport-library-agnostic on purpose — the broker mustn't
    depend on httpx's exception classes — so it walks the ``__cause__`` /
    ``__context__`` chain for an ``OSError`` errno (the ground truth httpx wraps)
    and falls back to type-name / message heuristics. The distinction is the
    whole point: ``conn_refused`` = the pod answered but nothing is listening (a
    child *bind* gap), ``timeout`` = the dial blackholed (a *routing*/Service
    gap), ``dns_fail`` = the roster host doesn't resolve, ``session_unknown`` =
    the sidecar ANSWERED 404 (it is up; it no longer knows this session's
    token); a bare bool folds all of these into one indistinguishable "not
    ready"."""
    if exc is None:
        return "reachable"
    for cur in _exception_tree(exc):
        if isinstance(cur, TimeoutError | asyncio.TimeoutError):
            return "timeout"
        if isinstance(cur, socket.gaierror):
            return "dns_fail"
        if isinstance(cur, OSError) and cur.errno == errno.ECONNREFUSED:
            return "conn_refused"
        if _is_not_found(cur):
            return "session_unknown"
    text = f"{type(exc).__name__}: {exc}".lower()
    _dns = ("name or service not known", "nodename nor servname", "temporary failure in name")
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "refused" in text:
        return "conn_refused"
    if any(marker in text for marker in _dns):
        return "dns_fail"
    return "http_error"


# After this many consecutive failed probes, a sidecar that never comes ready is
# no longer "still booting" — escalate from DEBUG to WARNING (with the dial URL +
# error) so a persistent blackhole names itself in the logs instead of silently
# leaving the tool missing from the agent. Sized past a normal cold start (a few
# probe cycles). See docs/incidents/2026-07-27-workspace-egress-fence-*.
_UNREACHABLE_WARN_AFTER = 3

# A per-session upstream URL may carry a bearer as a path segment (the embedder
# appends a signed, expiring ticket to route the session's calls), and the
# dial exceptions the client library raises render the full URL in their
# message. Anything logged about a URL goes through here: a long base64url
# segment is replaced, never printed. Purely lexical — the broker does not
# know what the segment means, only that a 32+ char token-shaped segment in a
# log line is a bearer until proven otherwise.
_BEARER_SEGMENT = re.compile(r"/[A-Za-z0-9_-]{32,}(?=[/?#\s'\"]|$)")


def redact_url(text: str) -> str:
    """``text`` (a URL, or an error message quoting one) with every
    token-shaped path segment replaced by ``<redacted>``."""
    return _BEARER_SEGMENT.sub("/<redacted>", text)


# Sent on every tool call so a slow/failing turn can be grepped for by the SAME
# id in both the platform's own logs and an external provider's (see the docstring
# on Upstream.call for why this exists and why it isn't OTel trace context).
_REQUEST_ID_HEADER = "X-Platform-Request-Id"

# The closed vocabulary of inc_roster_refresh outcomes (see BrokerMetrics).
ROSTER_REFRESH_OUTCOMES = frozenset({"dropped", "changed", "unchanged"})


def roster_shape(tools: list[types.Tool]) -> dict[str, str]:
    """Tool name -> a short digest of WHAT that tool advertises (description,
    input schema, annotations), so two probes can be compared for "did the menu
    change" — and the log can name WHICH tools moved — without diffing pydantic
    objects. Keyed by name, so a server listing its tools in a different order
    is not a change."""
    shape: dict[str, str] = {}
    for t in tools:
        canonical = json.dumps(t.model_dump(mode="json", exclude_none=True), sort_keys=True)
        shape[t.name] = hashlib.sha256(canonical.encode()).hexdigest()[:12]
    return shape


def roster_diff(before: dict[str, str], after: dict[str, str]) -> str:
    """One line naming what changed between two roster shapes."""
    added = sorted(after.keys() - before.keys())
    removed = sorted(before.keys() - after.keys())
    reshaped = sorted(n for n in after.keys() & before.keys() if after[n] != before[n])
    return f"added={added} removed={removed} reshaped={reshaped}"


class Upstream:
    """One workspace-tool sidecar, fronted by the broker."""

    def __init__(
        self,
        name: str,
        url: str,
        *,
        dialer: Dialer,
        call_retries: int = 2,
        call_backoff_s: float = 0.5,
        call_timeout_s: float = 120.0,
        probe_timeout_s: float = 5.0,
        metrics: BrokerMetrics = NULL_METRICS,
    ) -> None:
        self._name = name
        self._url = url
        self._dial = dialer
        self._metrics = metrics
        self._call_retries = call_retries
        self._call_backoff_s = call_backoff_s
        self._call_timeout_s = call_timeout_s
        self._probe_timeout_s = probe_timeout_s
        # None => not confirmed ready; the agent must not see these tools yet.
        # Cleared again on the up→down edge (see _set_reachable): a roster is
        # only ever a claim about the process that answered the last probe.
        self._tools: list[types.Tool] | None = None
        # Shape of the last roster a probe returned. Deliberately NOT cleared
        # with _tools, so the probe that recovers a rolled sidecar can say
        # whether the new process advertises something different from the old
        # one — the question a "did the deploy change the tool schema?" reader
        # is actually asking.
        self._shape: dict[str, str] | None = None
        # Set on every up→down edge, cleared by whoever re-dials (the broker's
        # dial loop waits on it, so a lost upstream is re-dialed and re-announced
        # without the loop having to poll a ready upstream forever).
        self._lost = asyncio.Event()
        # Monotonic time of the last probe ATTEMPT (not success) — lets the
        # broker's lazy re-probe rate-limit itself instead of paying the
        # probe timeout on every tools/list against a permanently wedged child.
        self._last_probe_t: float | None = None
        # Last-observed reachability (from probe AND call). Drives the
        # `workspace_broker_upstream_ready` gauge honestly — it used to latch at
        # 1 forever (set True on a probe success, never False), so a sidecar
        # that OOMed after coming up read as "ready" (R17). It also edge-triggers
        # `workspace_sidecar_exit_total` on the up→down transition (that counter
        # had zero callers, so its WorkspaceSidecarExit alert was blind).
        self._reachable: bool = False
        # Consecutive failed probes since the last success. Drives the escalation
        # from a DEBUG "not up yet" (normal during boot) to a WARNING that names
        # the unreachable URL + error, so a persistent blackhole is diagnosable
        # from the logs alone (a tool that NEVER comes ready otherwise leaves only
        # DEBUG traces and an absent tool).
        self._consecutive_probe_failures: int = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def ready(self) -> bool:
        return self._tools is not None

    def tools(self) -> list[types.Tool]:
        """The cached roster the broker advertises for this sidecar (``[]`` until ready)."""
        return list(self._tools) if self._tools is not None else []

    def probed_recently(self, within_s: float) -> bool:
        """Whether a probe was ATTEMPTED within the last ``within_s`` seconds."""
        if self._last_probe_t is None:
            return False
        return (asyncio.get_running_loop().time() - self._last_probe_t) < within_s

    def _set_reachable(self, value: bool, *, reason: str) -> None:
        """Record the sidecar's current reachability (from a probe or a call).

        Keeps the ``workspace_broker_upstream_ready`` gauge honest — set it 1
        on any success, 0 on any failure (idempotent, so repeated failures don't
        skew it). Emits ``workspace_sidecar_exit_total`` ONLY on the up→down
        EDGE (was reachable, now not), so a sidecar that OOMs counts one exit,
        not one per failed call. A concurrent-failure double-count is possible
        and harmless — the alert keys on ``rate() > 0``, not an exact tally.
        A ``_NOT_AN_EXIT`` reason drops the roster without counting an exit."""
        self._metrics.set_upstream_ready(self._name, value)
        if not value and self._reachable:
            if reason not in _NOT_AN_EXIT:
                self._metrics.inc_upstream_exit(self._name, reason)
            self._drop_roster(reason)
        self._reachable = value

    def _drop_roster(self, reason: str) -> None:
        """The up→down edge: forget the cached tools and wake the re-dialer.

        The roster came from a process that is now gone. Keeping it would let
        the agent plan around tool schemas the replacement may not have — and
        because ``ready`` reads ``_tools``, dropping it also makes the next
        ``tools/list`` report ``empty_not_ready`` honestly instead of serving a
        menu nobody is behind."""
        if self._tools is None:
            return
        log.warning(
            "broker upstream %s lost (%s); dropping its %d cached tool(s) until it answers again",
            self._name,
            reason,
            len(self._tools),
        )
        self._tools = None
        self._metrics.inc_roster_refresh(self._name, "dropped")
        self._lost.set()

    async def wait_lost(self) -> None:
        """Block until the next up→down edge, then arm for the one after."""
        await self._lost.wait()
        self._lost.clear()

    async def probe(self) -> bool:
        """Dial the sidecar and cache its tools. Return ``True`` iff open
        sessions should be told the roster changed: the upstream just became
        ready, OR it was already ready and the sidecar now advertises a different
        roster (a rolled image behind the same Service). A same-roster re-probe
        returns ``False`` so a routine refresh never fans out a ``list_changed``.
        A failed probe is swallowed — the dialer retries.

        Bounded by ``probe_timeout_s`` end to end (dial + initialize + list):
        a server whose transport session crashed mid-initialize accepts the
        POST and then never answers the list — mcp-server-odoo does exactly
        this when its Odoo host is unreachable (its connection self-test
        raises inside session setup). An unbounded probe then hangs its
        caller forever; on the attach path that used to take the whole
        session spawn down with the 60s watchdog (Jul 2026 stg finding —
        same wedged-conn class as the ``call()`` timeout above)."""
        was_ready = self._tools is not None
        self._last_probe_t = asyncio.get_running_loop().time()
        try:
            tools = await asyncio.wait_for(self._probe_once(), timeout=self._probe_timeout_s)
        except TimeoutError as exc:
            self._note_probe_failure(
                f"no answer within {self._probe_timeout_s:.0f}s (wedged or booting)"
            )
            self._metrics.inc_child_probe(self._name, classify_probe_result(exc))
            self._set_reachable(False, reason="probe_timeout")
            return False
        except Exception as exc:  # any dial/list failure means "not up yet"
            self._note_probe_failure(f"{type(exc).__name__}: {exc}")
            result = classify_probe_result(exc)
            self._metrics.inc_child_probe(self._name, result)
            self._set_reachable(
                False,
                reason="probe_session_unknown"
                if result == "session_unknown"
                else "probe_unreachable",
            )
            return False
        self._note_probe_success()
        changed = self._note_roster(tools)
        self._tools = tools
        self._metrics.inc_child_probe(self._name, "reachable")
        self._set_reachable(True, reason="probe_ok")
        return changed or not was_ready

    def _note_roster(self, tools: list[types.Tool]) -> bool:
        """Compare a freshly probed roster with the last one seen and record the
        verdict. ``True`` iff it differs from a previously seen roster. The first
        roster ever seen is neither changed nor unchanged — there is nothing to
        compare it to — and is reported by ``inc_child_probe`` alone."""
        shape = roster_shape(tools)
        prev = self._shape
        self._shape = shape
        if prev is None:
            return False
        if shape == prev:
            self._metrics.inc_roster_refresh(self._name, "unchanged")
            return False
        self._metrics.inc_roster_refresh(self._name, "changed")
        log.warning(
            "broker upstream %s roster changed (%d -> %d tools; %s); "
            "open sessions will be told to re-list",
            self._name,
            len(prev),
            len(shape),
            roster_diff(prev, shape),
        )
        return True

    def _note_probe_failure(self, detail: str) -> None:
        """Count a failed probe and escalate to WARNING once it's clearly not a
        transient cold start. The WARNING carries the dial URL + error — the two
        facts that make a blackhole (e.g. a NetworkPolicy dropping the SYN, so the
        connect times out) diagnosable at a glance instead of a silent missing
        tool. Warns on the threshold crossing and then every 10th probe, so a
        long outage stays visible without flooding."""
        self._consecutive_probe_failures += 1
        n = self._consecutive_probe_failures
        if n == _UNREACHABLE_WARN_AFTER or (n > _UNREACHABLE_WARN_AFTER and n % 10 == 0):
            log.warning(
                "broker upstream %s STILL unreachable after %d probes (url=%s): %s",
                self._name,
                n,
                redact_url(self._url),
                redact_url(detail),
            )
        else:
            log.debug(
                "broker upstream %s not reachable yet (url=%s): %s",
                self._name,
                redact_url(self._url),
                redact_url(detail),
            )

    def _note_probe_success(self) -> None:
        """Reset the failure counter; log recovery at INFO if we had escalated so
        the log shows the outage closing, not just opening."""
        if self._consecutive_probe_failures >= _UNREACHABLE_WARN_AFTER:
            log.info(
                "broker upstream %s reachable again after %d failed probes (url=%s)",
                self._name,
                self._consecutive_probe_failures,
                redact_url(self._url),
            )
        self._consecutive_probe_failures = 0

    async def _probe_once(self) -> list[types.Tool]:
        async with self._dial(self._url) as conn:
            return await conn.list_tools()

    def _dial_url(self, chat_id: str | None) -> str:
        """The sidecar URL to dial for this call. ``chat_id`` (set for the per-chat
        browser tool) is appended as a query param so the sidecar routes the call to
        that chat's own session; other tools dial the bare URL unchanged."""
        if not chat_id:
            return self._url
        sep = "&" if "?" in self._url else "?"
        return f"{self._url}{sep}chat_id={quote(chat_id, safe='')}"

    async def call(
        self, tool_name: str, arguments: dict[str, Any], *, chat_id: str | None = None
    ) -> types.CallToolResult:
        """Proxy one tool call to the sidecar with a bounded retry budget.

        Never raises AND never hangs: a sidecar that is down/restarting yields an
        ``isError`` result the agent can relay and retry, so the chat survives
        (the graceful degradation invariant).

        Two failure modes, two responses:
          * the dial *raises* (connection refused — sidecar down/restarting):
            retry with backoff, since it may be transiently starting;
          * the dial connects but the call *never answers* (a wedged upstream —
            the streamable-HTTP session hangs): ``asyncio.wait_for`` caps it at
            ``call_timeout_s`` and we fail fast — retrying only stacks more
            full-length timeouts and leaves the chat spinning forever, which is
            exactly the "stuck chat" this guards against.

        ``request_id`` is minted here, not passed in: correlating one call
        across this side's own logs and a remote upstream's (one run by a
        different team, on different infrastructure) used to require
        manually matching wall-clock
        timestamps across two systems with no shared identifier — a real
        investigation (2026-08-05) cost significant time on exactly this. Real
        OTel trace continuity does NOT reach this call site today: the agent
        CLI subprocess starts a disconnected root trace on every broker call
        (confirmed live via Tempo — every ``/_broker/*/mcp`` trace has
        ``rootServiceName=workspace`` with no parent span), so a fresh id per
        call, logged on both sides, is the simple thing that actually works
        rather than a fake trace-shaped id riding on propagation that isn't
        happening. Logged here (not just sent on the wire) so the platform's own
        Loki has the same id to grep for.
        """
        request_id = uuid.uuid4().hex
        log.info("broker upstream %s call %s request_id=%s", self._name, tool_name, request_id)
        last_exc: Exception | None = None
        for attempt in range(self._call_retries + 1):
            try:
                result = await asyncio.wait_for(
                    self._call_once(tool_name, arguments, chat_id, request_id),
                    timeout=self._call_timeout_s,
                )
                # A successful call is the freshest reachability signal — the
                # broker never re-probes a ready upstream, so without this the
                # gauge could stay 0 after a transient blip recovered.
                self._set_reachable(True, reason="call_ok")
                # isError is the sidecar reaching the tool and the TOOL saying no
                # (bad argument, downstream 4xx) — not a broker/transport failure,
                # so it gets its own outcome rather than folding into "ok".
                self._metrics.inc_call(
                    self._name,
                    outcome="tool_error" if result.isError else "ok",
                    cause="none",
                )
                return result
            except TimeoutError:
                log.warning(
                    "broker upstream %s call %s did not respond within %.0fs; degrading",
                    self._name,
                    tool_name,
                    self._call_timeout_s,
                )
                # A wedged upstream (dialed but never answered) is down NOW — the
                # gauge/counter path a ready→OOMed sidecar surfaces through
                # (probe() never re-runs for a ready upstream).
                self._set_reachable(False, reason="call_timeout")
                self._metrics.inc_call(self._name, outcome="timeout", cause="timeout")
                return self._unavailable(
                    f"did not respond within {self._call_timeout_s:.0f}s (it may be wedged)"
                )
            except Exception as exc:  # degrade, don't crash the chat
                last_exc = exc
                # A 404 is the sidecar's final answer for this token, not a
                # restart in progress; retrying only delays the agent.
                if classify_probe_result(exc) == "session_unknown":
                    break
                if attempt < self._call_retries:
                    await asyncio.sleep(self._call_backoff_s * (attempt + 1))
        log.warning(
            "broker upstream %s call %s failed after retries: %s",
            self._name,
            tool_name,
            redact_url(str(last_exc)),
        )
        unknown = last_exc is not None and classify_probe_result(last_exc) == "session_unknown"
        self._set_reachable(False, reason="call_session_unknown" if unknown else "call_unreachable")
        self._metrics.inc_call(
            self._name,
            outcome="unavailable",
            cause=classify_probe_result(last_exc) if last_exc is not None else "unknown",
        )
        return self._unavailable(
            f"not reachable after {self._call_retries + 1} attempts; it may be starting or restarting"
        )

    async def _call_once(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        chat_id: str | None = None,
        request_id: str | None = None,
    ) -> types.CallToolResult:
        """One dial+call attempt. Wrapped in ``wait_for`` by :meth:`call` so a
        hung dial, initialize, OR call is bounded — not just a raised connect."""
        headers = {_REQUEST_ID_HEADER: request_id} if request_id else None
        async with self._dial(self._dial_url(chat_id), headers=headers) as conn:
            return await conn.call_tool(tool_name, arguments)

    def _unavailable(self, reason: str) -> types.CallToolResult:
        """A degraded ``isError`` result the agent can relay to the user."""
        return types.CallToolResult(
            content=[
                types.TextContent(
                    type="text",
                    text=(
                        f"Tool sidecar '{self._name}' is temporarily unavailable "
                        f"({reason}). Try again shortly."
                    ),
                )
            ],
            isError=True,
        )
