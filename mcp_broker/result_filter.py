"""The result-gating seam.

Every tool result the broker proxies back to the agent passes through ONE
:class:`ResultFilter` the host hands it. The default passes results through
untouched, so the library has no opinion about what a host might want to
withhold or rewrite — a per-user visibility rule, a redaction pass, a size cap.

The contract that makes it a GATE rather than a hook: if the filter raises,
the agent gets an error result, never the unfiltered one. A host that reaches
for this seam is enforcing something; a bug in its filter must fail closed.
"""

from __future__ import annotations

from typing import Any, Protocol

import mcp.types as types


class ResultFilter(Protocol):
    """Post-process one proxied tool result before the agent sees it."""

    async def filter_result(
        self,
        name: str,
        tool_name: str,
        arguments: dict[str, Any],
        result: types.CallToolResult,
        *,
        chat_id: str | None,
    ) -> types.CallToolResult:
        """Return the result the agent should receive.

        ``name`` is the upstream's roster name, ``tool_name`` the tool called on
        it, ``arguments`` what the agent passed, ``result`` what the upstream
        answered (error results included — the filter sees everything).
        ``chat_id`` is the ``?chat_id=`` the agent's per-session URL carried,
        or None when it carried none.
        """


class NullResultFilter:
    """The default: every result passes through as the upstream answered it."""

    async def filter_result(
        self,
        name: str,
        tool_name: str,
        arguments: dict[str, Any],
        result: types.CallToolResult,
        *,
        chat_id: str | None,
    ) -> types.CallToolResult:
        return result


NULL_RESULT_FILTER: ResultFilter = NullResultFilter()


def withheld_result(reason: str) -> types.CallToolResult:
    """The error result the agent gets when a filter could not decide.

    ``reason`` is agent-facing prose, so it says what the agent can DO (retry,
    ask the user) — not what broke."""
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=reason)],
        isError=True,
    )
