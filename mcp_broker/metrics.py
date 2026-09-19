"""The observability seam.

The broker reports four things worth graphing, and refuses to decide how. It
calls a :class:`BrokerMetrics` you hand it; the default does nothing, so the
library has no opinion about Prometheus, OpenTelemetry, statsd, or logs.

Pass your own to :meth:`ToolBroker.from_env` (or construct :class:`Upstream`
with it) and adapt each call onto whatever you already run. The counters that
matter operationally:

* ``upstream_ready`` — is this upstream currently reachable. The single most
  useful series: a tool silently missing from the agent is this going 0.
* ``upstream_giveup`` — the dial loop hit its backoff cap and stopped. An
  upstream that will now never appear without a restart.
* ``upstream_exit`` — a live upstream dropped, with a reason.
* ``call`` — the per-tool-call outcome (ok / tool_error / unavailable /
  timeout, plus a ``cause`` on the failure outcomes). The other three answer
  "is this upstream up"; this one answers "what happened to THIS call",
  which is what tells a sidecar-side tool error apart from a broker-side
  routing failure.
"""

from __future__ import annotations

from typing import Protocol


class BrokerMetrics(Protocol):
    """What the broker reports. Implement the ones you care about."""

    def set_upstream_ready(self, name: str, ready: bool) -> None:
        """An upstream's reachability changed."""

    def inc_upstream_giveup(self, name: str) -> None:
        """The dial loop stopped retrying this upstream."""

    def inc_upstream_exit(self, name: str, reason: str) -> None:
        """A previously-live upstream went away."""

    def inc_child_probe(self, name: str, result: str) -> None:
        """The outcome of a SINGLE probe dial, reported on EVERY probe (not only
        the ready/not-ready edge ``set_upstream_ready`` fires on). ``result`` is
        one of ``reachable`` | ``timeout`` | ``conn_refused`` | ``dns_fail`` |
        ``http_error`` — the axis that distinguishes a child that is booting
        (comes ready shortly) from one that is on the network but refusing (a
        bind gap) from one that is blackholed (a routing/Service gap). A bare
        ready/not-ready bool cannot tell those apart, which is exactly how a
        cross-pod child that was never reachable read the same as a
        slow-to-start one."""

    def inc_session_notify(self, name: str, outcome: str) -> None:
        """A ``tools/list_changed`` fan-out for a session-scoped upstream that
        arrived AFTER the CLI had already connected.

        ``outcome`` is ``delivered`` (at least one live CLI session was told,
        so the tools can appear mid-session) or ``no_sessions`` (the notify
        reached NOBODY). ``no_sessions`` is the silent-failure case and the
        whole reason this seam exists: the upstream is registered and healthy,
        every log line reads success, and the agent still never sees the tools
        because nothing will make it re-list. A bare "registered ready=True"
        log cannot distinguish the two."""

    def inc_tools_list(self, name: str, outcome: str) -> None:
        """A ``tools/list`` was answered for ``name``.

        ``outcome`` is the WHOLE point: an empty answer is the failure mode that
        matters (the agent sees a server with no tools and concludes the system
        is not connected), and there are four distinct reasons for it that were
        previously indistinguishable —

        * ``served``            — a non-empty roster went to the CLI.
        * ``empty_no_upstream`` — session-scoped and nothing is registered for
          this request's token: either the connector is not connected for this
          user, or the token did not propagate. Nothing else reports this.
        * ``empty_not_ready``   — an upstream exists but has never answered a
          probe, so its tools are unknown.
        * ``empty_ready``       — the upstream IS ready and genuinely advertises
          zero tools. A child in this state is misconfigured, not absent, and
          the two demand opposite fixes.
        * ``empty_retired``     — the request's session token was registered
          and later cleared, yet a CLI still lists under it: the agent outlived
          its routing key. Never heals on its own.
        """

    def set_tools_advertised(self, name: str, count: int) -> None:
        """How many tools the CLI was just told ``name`` has. A drop to zero on
        a server that previously served tools is a regression the ready gauge
        cannot show — ``ready`` is about reachability, this is about content."""

    def inc_call_no_upstream(self, name: str) -> None:
        """A tool CALL arrived for a server with no upstream for this session.
        The caller gets a polite "isn't connected yet" result, which reads to
        the user like the assistant being unable rather than the platform
        failing to route — so it must be counted or it is invisible."""

    def inc_session_upstream_replaced(self, name: str) -> None:
        """A session upstream was re-registered over a live one for the same
        (server, token). The previous upstream is dropped without being closed;
        a rising count means connector re-attaches are churning."""

    def inc_session_cleared(self, name: str) -> None:
        """A session upstream was dropped because its session ended."""

    def set_cli_sessions(self, name: str, count: int) -> None:
        """How many CLI sessions the broker currently believes are attached to
        ``name`` — the denominator that makes a ``no_sessions`` notify
        interpretable (nobody listening vs. everybody unreachable)."""

    def inc_heal(self, name: str, outcome: str) -> None:
        """Did the tools actually REACH the agent after a connector attached?

        ``served`` — a later tools/list for that same session returned a
        non-empty roster. ``never_served`` — the session ended still waiting.
        This is deliberately NOT the notify counter: "we told the CLI" and "the
        agent ended up with the tools" are different events, they can disagree,
        and only the second is what the user experiences."""

    def set_pending_heals(self, name: str, count: int) -> None:
        """Sessions told a connector attached and not yet observed receiving
        its tools. A number that does not return to zero is the shape of a
        connector that is connected and invisible."""

    def inc_roster_refresh(self, name: str, outcome: str) -> None:
        """The cached tool roster for ``name`` was re-derived from the live
        upstream, or discarded. ``outcome``:

        * ``dropped``   — the upstream was observed gone (a failed call or a
          failed re-probe after it had been ready) and its roster was
          forgotten; the dial loop re-announces it once it answers again.
        * ``changed``   — a re-probe returned a roster that differs from the
          last one seen (a rolled sidecar image, typically). Open sessions are
          told to re-list.
        * ``unchanged`` — a re-probe returned the same roster. The routine
          case; it is what makes ``changed`` a rate worth alerting on.

        Before this existed a sidecar that rolled behind a stable Service kept
        its predecessor's schemas advertised for the workspace pod's whole
        life, and nothing counted it (2026-09-14)."""

    def inc_call(self, name: str, outcome: str, cause: str) -> None:
        """One ``Upstream.call`` to ``name`` finished — every exit path, exactly
        once. A host that only counts ok/error at its own bridge (a tool_result
        ``is_error`` observed several layers up) cannot tell "the sidecar
        answered with an error" from "the sidecar was unreachable" from "the
        MCP session was split across replicas" (a multi-replica sidecar
        answering 404 for a session a DIFFERENT replica created) — three failure
        classes with three different fixes that collapsed into one bool.

        ``outcome``:

        * ``ok``          — the call reached the sidecar; the result was not
          ``isError``.
        * ``tool_error``  — the call reached the sidecar; the TOOL itself
          answered ``isError`` (a bad argument, a downstream 4xx, ...). The
          broker/transport is not at fault.
        * ``unavailable`` — the retry budget was exhausted, or the loop broke
          out early on a definitive answer (a 404 session-unknown). The
          sidecar could not be reached this call, or no longer holds this
          session.
        * ``timeout``      — the dial connected but the call never answered
          within ``call_timeout_s`` (a wedged upstream).

        ``cause`` narrows the failure outcomes: for ``unavailable`` it is
        :func:`classify_probe_result` run on the last exception seen
        (``session_unknown`` is the split-session/multi-replica signature this
        axis exists to catch); for ``timeout`` it is the literal string
        ``"timeout"``; for ``ok`` / ``tool_error`` it is ``"none"`` — nothing
        failed, so nothing to classify."""


class NullMetrics:
    """The default: report nothing.

    Chosen over "guess a metrics library" deliberately — a library that reaches
    for a global registry is a library that fights the host application.
    """

    def set_upstream_ready(self, name: str, ready: bool) -> None:
        return None

    def inc_upstream_giveup(self, name: str) -> None:
        return None

    def inc_upstream_exit(self, name: str, reason: str) -> None:
        return None

    def inc_child_probe(self, name: str, result: str) -> None:
        return None

    def inc_session_notify(self, name: str, outcome: str) -> None:
        return None

    def inc_tools_list(self, name: str, outcome: str) -> None:
        return None

    def set_tools_advertised(self, name: str, count: int) -> None:
        return None

    def inc_call_no_upstream(self, name: str) -> None:
        return None

    def inc_session_upstream_replaced(self, name: str) -> None:
        return None

    def inc_session_cleared(self, name: str) -> None:
        return None

    def set_cli_sessions(self, name: str, count: int) -> None:
        return None

    def inc_heal(self, name: str, outcome: str) -> None:
        return None

    def set_pending_heals(self, name: str, count: int) -> None:
        return None

    def inc_roster_refresh(self, name: str, outcome: str) -> None:
        return None

    def inc_call(self, name: str, outcome: str, cause: str) -> None:
        return None


NULL_METRICS: BrokerMetrics = NullMetrics()
