"""A runnable broker in front of two tool servers — one up, one not yet.

    pip install -e '.[dev]'
    python examples/serve_broker.py

Then point an agent at it. For Claude Code:

    claude mcp add --transport http office  http://127.0.0.1:9000/_broker/office/mcp
    claude mcp add --transport http slowly  http://127.0.0.1:9000/_broker/slowly/mcp

Both register instantly, even though `slowly` is not listening. Ask the agent
what tools it has: it sees office's tools and nothing from slowly — no phantom
entry that would hang on first call. Now start something on port 9102:

    python examples/fake_upstream.py 9102

…and WITHOUT restarting the agent or the broker, ask again. slowly's tools are
there. That is the whole point of this library: a tool server that is slow to
boot, or starts later, does not have to be up before the agent session does.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import uvicorn
from starlette.applications import Starlette

from mcp_broker import ToolBroker, broker_route

# (name, url) — the roster. `slowly` is deliberately not running.
ROSTER = [
    ("office", "http://127.0.0.1:9101/mcp"),
    ("slowly", "http://127.0.0.1:9102/mcp"),
]


def build() -> Starlette:
    broker = ToolBroker.from_env(ROSTER)

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        # run() owns the dial loop that notices an upstream coming up and pushes
        # tools/list_changed to every attached agent session. Without it the
        # broker still serves, but nothing ever appears mid-session.
        async with broker.run():
            yield

    app = Starlette(lifespan=lifespan)
    for name in broker.names:
        # /_broker/<name>/mcp — one MCP endpoint per upstream, so the agent sees
        # them as separate servers and can be granted them separately.
        app.mount(broker_route(name), broker.asgi_app(name))
    return app


if __name__ == "__main__":
    print("broker on http://127.0.0.1:9000")
    for n, u in ROSTER:
        print(f"  /_broker/{n}/mcp  ->  {u}")
    uvicorn.run(build(), host="127.0.0.1", port=9000, log_level="info")
