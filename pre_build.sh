#!/usr/bin/env bash
# Pre-build gate for the mcp-broker library: ruff format + ruff + mypy +
# pip-audit + pytest. Self-contained (own venv, no repo-root references) so it
# runs identically here and in the public mirror.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { echo "$(date '+%Y-%m-%d %H:%M:%S') [mcp-broker] $*"; }
fail() { log "FAIL: $*"; exit 1; }

log "Running pre-build checks..."

cd "$SCRIPT_DIR"

VENV="$SCRIPT_DIR/.venv"

if [ ! -x "$VENV/bin/ruff" ]; then
  log "Bootstrapping .venv..."
  uv venv "$VENV" --python python3.12 >&2
  uv pip install --python "$VENV/bin/python" --index-url https://pypi.org/simple/ -e '.[dev]' >&2
fi

pick() {
  if [ -x "$VENV/bin/$1" ]; then
    echo "$VENV/bin/$1"
  elif command -v "$1" >/dev/null 2>&1; then
    command -v "$1"
  fi
}

RUFF="$(pick ruff)"
MYPY="$(pick mypy)"
PYTEST="$(pick pytest)"
PIP_AUDIT="$(pick pip-audit)"

HINT="  Install: uv pip install --python $VENV/bin/python -e '.[dev]'"
[ -n "$RUFF" ]      || fail "ruff not found. $HINT"
[ -n "$MYPY" ]      || fail "mypy not found. $HINT"
[ -n "$PYTEST" ]    || fail "pytest not found. $HINT"
[ -n "$PIP_AUDIT" ] || fail "pip-audit not found. $HINT"

log "1/5 Format check (ruff format)..."
"$RUFF" format --check mcp_broker tests examples || fail "ruff format (run: ruff format mcp_broker tests examples)"
log "  ✓ ruff format passed"

log "2/5 Linting (ruff)..."
"$RUFF" check mcp_broker tests examples || fail "ruff"
log "  ✓ ruff passed"

log "3/5 Static typing (mypy)..."
"$MYPY" mcp_broker || fail "mypy"
log "  ✓ mypy passed"

log "4/5 Dependency CVE scan (pip-audit)..."
PIP_INDEX_URL=https://pypi.org/simple/ "$PIP_AUDIT" --no-deps -r requirements.txt || fail "pip-audit"
log "  ✓ pip-audit passed"

log "5/5 Running unit tests..."
"$PYTEST" -q || fail "pytest"
log "  ✓ pytest passed"

log "Pre-build checks complete ✓"
