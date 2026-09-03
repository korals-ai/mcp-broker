"""The example stack and docker-compose.yml must name the same ports.

This is a match-list that spans two files and six other repositories, and it
fails in the worst way: a wrong port raises nothing, it just yields an upstream
that never becomes ready — which is indistinguishable from the lazy behaviour
working correctly. So it gets a test rather than care.
"""

from __future__ import annotations

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "docker-compose.yml"


def _compose_ports() -> dict[str, int]:
    """Service name -> published host port, parsed without a YAML dependency.

    Deliberately regex over the file rather than yaml.safe_load: the package's
    runtime deps are mcp + starlette, and a test-only parser dependency would be
    a strange thing to make a contributor install.
    """
    ports: dict[str, int] = {}
    service: str | None = None
    for line in COMPOSE.read_text().splitlines():
        m = re.match(r"^  ([a-z0-9_-]+):\s*$", line)
        if m:
            service = m.group(1)
            continue
        # Full-line anchor: a service growing a SECOND mapping must fail here,
        # not pass with the extra mapping unvalidated.
        m = re.match(r'^\s+ports:\s*\["(\d+):(\d+)"\]\s*$', line)
        if m is None and re.match(r"^\s+ports:", line):
            raise AssertionError(f"unparseable ports line (second mapping?): {line!r}")
        if m and service:
            assert m.group(1) == m.group(2), f"{service}: host and container ports differ"
            ports[service] = int(m.group(1))
    return ports


def test_the_compose_file_was_actually_parsed() -> None:
    # Without this the two assertions below pass vacuously on an empty dict if
    # the compose format ever drifts from what the regex expects.
    assert COMPOSE.exists()
    assert len(_compose_ports()) == 6


def _stack() -> dict[str, int]:
    """Load STACK from the example without importing it as a package.

    examples/ is not part of the distribution, so it is not importable; reading
    the module by path keeps the test honest about what the file actually says
    rather than duplicating the mapping here.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "serve_stack", ROOT / "examples" / "serve_stack.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return dict(module.STACK)


def test_every_stack_entry_matches_its_compose_service() -> None:
    STACK = _stack()
    compose = _compose_ports()
    assert set(STACK) == set(compose), "a service is in one file and not the other"
    for name, port in STACK.items():
        assert compose[name] == port, f"{name}: example says {port}, compose says {compose[name]}"


@pytest.mark.parametrize("name", ["cad", "render", "ffmpeg", "office", "ocr", "browser"])
def test_each_service_builds_from_its_own_public_repo(name: str) -> None:
    # The build context is the whole reason nobody has to clone seven repos.
    body = COMPOSE.read_text()
    assert f"https://github.com/korals-ai/mcp-{name}.git" in body
