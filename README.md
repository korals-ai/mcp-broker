# mcp-broker

An MCP gateway for tool servers that **aren't up yet**.

Point your agent at the broker instead of at each tool server. A server that is
still booting — or that you haven't started — doesn't block the session and
doesn't appear as a phantom tool that hangs on first call. When it comes up, its
tools appear **mid-session**, without restarting the agent or the broker.

It's a library, not a service: you mount it in your own ASGI app.

## The problem

Agent runtimes read their MCP server list once, at session start. So every tool
server has to be listening before the agent starts, and your startup time is the
slowest one. Anything heavy — a browser, a CAD toolchain, a model server — is
either on the critical path or missing for the whole session.

Point the agent at a broker instead and that ordering constraint goes away.

## Install

```bash
pip install mcp-broker
```

## Use

```python
from mcp_broker import ToolBroker, broker_route

broker = ToolBroker.from_env([
    ("office", "http://127.0.0.1:9101/mcp"),
    ("slowly", "http://127.0.0.1:9102/mcp"),   # not running yet — fine
])

app = Starlette(lifespan=...)          # see examples/serve_broker.py
for name in broker.names:
    app.mount(broker_route(name), broker.asgi_app(name))
```

`broker.run()` — an async context manager, entered from your lifespan — owns the
dial loop that notices an upstream arriving and pushes `tools/list_changed` to
every attached session. Without it the broker still proxies, but nothing ever
appears mid-session.

Each upstream gets its **own** endpoint, `/_broker/<name>/mcp`, so agents see
them as separate servers and can be granted them separately.

## Using it from Claude Code

```bash
claude mcp add --transport http office http://127.0.0.1:9000/_broker/office/mcp
claude mcp add --transport http slowly http://127.0.0.1:9000/_broker/slowly/mcp
```

Both register instantly, whether or not the upstream behind them is running.

Any MCP client works the same way — the broker speaks ordinary Streamable HTTP
and holds no client-specific behaviour. In a config file:

```json
{
  "mcpServers": {
    "office": { "type": "http", "url": "http://127.0.0.1:9000/_broker/office/mcp" }
  }
}
```

## See it work

```bash
pip install -e '.[dev]'

python examples/fake_upstream.py 9101     # one upstream, running
python examples/serve_broker.py           # the broker, fronting two
```

Ask your agent what tools it has: `office` has `shout`, `slowly` has nothing.
Now start the second upstream — leaving the agent and the broker running:

```bash
python examples/fake_upstream.py 9102
```

Ask again. `slowly`'s tools are there. That is the whole library.

## A whole toolchain, in two commands

`docker-compose.yml` here brings up six real MCP tool servers — CAD, HTML→PDF,
ffmpeg, Office documents, OCR, and a browser — each built **straight from its own
public repository**, so there is nothing to clone:

```bash
docker compose up -d                    # cad, render, ffmpeg — quick to build
docker compose --profile heavy up -d    # adds office, ocr, browser — slow, large
python examples/serve_stack.py          # the broker, in your process
```

Then give an agent one endpoint per tool:

```bash
for t in cad render ffmpeg office ocr browser; do
  claude mcp add --transport http "$t" "http://127.0.0.1:9000/_broker/$t/mcp"
done
```

Register all six whether or not you started all six. The running ones bring
their tools; the rest stay silent instead of offering tools that fail when
called. Bring one up later and it joins the session you already have open.

The heavy three sit behind a profile because they install LibreOffice, Tesseract
and Chrome — hundreds of megabytes and several minutes on a first build. Start
with the default set.

## What it does, precisely

- **Confirmed-lazy `tools/list`.** An upstream's tools are advertised only after
  a probe has actually reached it. A server that never starts stays invisible,
  rather than offering a tool that fails when called.
- **`tools/listChanged` on arrival.** The dial loop notifies open sessions when
  an upstream first answers, so clients re-list and the tools appear.
- **Bounded retries, then give up loudly.** The dial loop backs off to a cap and
  then stops, reporting through the metrics seam rather than retrying forever.
  **This window bounds how late an upstream may arrive** — `giveup_after_s`
  defaults to 60 seconds, which suits upstreams that start alongside your
  application and is far too short if you might start one by hand minutes later.
  Raise it (`giveup_after_s=` or `MCP_BROKER_GIVEUP_S`) when upstreams can arrive
  late; a broker that has given up looks exactly like one whose upstream has not
  started, so this is worth setting deliberately.
- **Reachability is honest in both directions.** A probe or call that fails after
  an upstream was ready marks it not-ready again; a live upstream that drops
  reports one exit, on the edge, not one per failed call.
- **Per-session routing.** An upstream can be marked session-scoped and resolved
  per session token instead of to one static address — for tools that need
  per-user credentials.

## Observability

The broker reports through a seam and has no opinion about your metrics stack:

```python
class MyMetrics:
    def set_upstream_ready(self, name: str, ready: bool) -> None: ...
    def inc_upstream_giveup(self, name: str) -> None: ...
    def inc_upstream_exit(self, name: str, reason: str) -> None: ...

ToolBroker.from_env(roster, metrics=MyMetrics())
```

The default reports nothing. `set_upstream_ready` is the series worth graphing
first: a tool silently missing from your agent is that going 0.

## Tuning

`ToolBroker.from_env` reads `MCP_BROKER_CONNECT_TIMEOUT_S`,
`_POLL_MIN_S`, `_POLL_MAX_S`, `_GIVEUP_S` and `_CALL_TIMEOUT_S`, falling back to
the defaults. Use the `ToolBroker(...)` constructor directly to pass them
explicitly instead.

## Contributing

Issues and PRs welcome. This repository is a one-way mirror of a directory in a
private monorepo, which stays canonical — contributions are applied there and
reappear here on the next sync, so your change keeps your authorship upstream
but arrives here inside a sync commit.

## License

Apache-2.0 — see [LICENSE](LICENSE).
