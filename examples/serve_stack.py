"""The broker in front of the whole tool fleet from docker-compose.yml.

    docker compose up -d                  # cad, render, ffmpeg
    docker compose --profile heavy up -d  # adds office, ocr, browser
    python examples/serve_stack.py

Then give an agent one endpoint per tool. For Claude Code:

    for t in cad render ffmpeg office ocr browser; do
      claude mcp add --transport http "$t" "http://127.0.0.1:9000/_broker/$t/mcp"
    done

Add them all, whether or not you started them all. The ones that are running
bring their tools; the ones that are not stay silent instead of offering a tool
that fails when called. Bring one up later and its tools appear in the session
you already have open.

The roster below is the source of truth for both this script and
docker-compose.yml — tests/test_stack_roster.py fails if they disagree, because
a wrong port here does not raise, it just silently produces a tool that never
shows up.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import uvicorn
from starlette.applications import Starlette

from mcp_broker import ToolBroker, broker_route

# name -> published port, matching docker-compose.yml.
STACK = {
    "cad": 8092,
    "render": 8095,
    "ffmpeg": 8094,
    "office": 8090,
    "ocr": 8091,
    "browser": 8096,
}

ROSTER = [(name, f"http://127.0.0.1:{port}/mcp") for name, port in STACK.items()]


def build() -> Starlette:
    broker = ToolBroker(
        ROSTER,
        # The default give-up window is 60s, sized for upstreams that start
        # alongside the application. Here a service may still be BUILDING when the
        # broker starts, so the dial loop has to outlive that or it stops before the
        # image finishes and the tools never appear — the failure is silent, because
        # "gave up" and "not started yet" look identical from the agent's side.
        giveup_after_s=3600,
    )

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        async with broker.run():
            yield

    app = Starlette(lifespan=lifespan)
    for name in broker.names:
        app.mount(broker_route(name), broker.asgi_app(name))
    return app


if __name__ == "__main__":
    print("broker on http://127.0.0.1:9000")
    for name, port in STACK.items():
        print(f"  /_broker/{name}/mcp  ->  127.0.0.1:{port}")
    uvicorn.run(build(), host="127.0.0.1", port=9000, log_level="info")
