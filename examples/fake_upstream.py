"""A trivial MCP tool server, so the broker example has something to front.

    python examples/fake_upstream.py 9101        # the one that is up
    python examples/fake_upstream.py 9102        # start this one LATE

Start the second one after the agent is already connected to the broker to see
its tool appear mid-session.
"""

from __future__ import annotations

import sys

from mcp.server.fastmcp import FastMCP

port = int(sys.argv[1]) if len(sys.argv) > 1 else 9101
mcp = FastMCP(f"upstream-{port}", host="127.0.0.1", port=port)


@mcp.tool()
def shout(text: str) -> str:
    """Return the text in upper case. Exists only to prove the call arrived."""
    return f"[{port}] {text.upper()}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
