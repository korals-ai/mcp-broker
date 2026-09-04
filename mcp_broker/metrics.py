"""The observability seam.

The broker reports four things worth graphing, and refuses to decide how. It
calls a :class:`BrokerMetrics` you hand it; the default does nothing, so the
library has no opinion about Prometheus, OpenTelemetry, statsd, or logs.

Pass your own to :meth:`ToolBroker.from_env` (or construct :class:`Upstream`
with it) and adapt each call onto whatever you already run. The three counters
that matter operationally:

* ``upstream_ready`` — is this upstream currently reachable. The single most
  useful series: a tool silently missing from the agent is this going 0.
* ``upstream_giveup`` — the dial loop hit its backoff cap and stopped. An
  upstream that will now never appear without a restart.
* ``upstream_exit`` — a live upstream dropped, with a reason.
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


NULL_METRICS: BrokerMetrics = NullMetrics()
