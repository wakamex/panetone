#!/usr/bin/env bash
# Build, check and restart the production Panetone daemon from a clean HEAD.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT=panetone.service
BIN="$REPO_ROOT/target/release/panetone"
RECORD="${XDG_STATE_HOME:-$HOME/.local/state}/panetone-rust/deployed"

usage() {
    echo "Usage: ./deploy.sh [--skip-checks]"
    echo ""
    echo "  (no flags)     Build, test, lint, restart $UNIT and record the deployed commit"
    echo "  --skip-checks  Build and restart without running tests and clippy"
    echo ""
    echo "The daemon runs $BIN directly, so the build itself replaces the client"
    echo "that agents call. The running daemon keeps its old code until the restart."
}

CHECKS=true
while [ "$#" -gt 0 ]; do
    case "$1" in
        --skip-checks) CHECKS=false; shift ;;
        --help|-h) usage; exit 0 ;;
        *) echo "Unknown arg: $1"; usage; exit 1 ;;
    esac
done

cd "$REPO_ROOT"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    echo "Refusing to deploy: the working tree has uncommitted changes."
    git status --short --untracked-files=no
    exit 1
fi
HEAD_COMMIT="$(git rev-parse --short=8 HEAD)"

echo "=== Deployed now ==="
cat "$RECORD" 2>/dev/null || echo "  no record"
echo ""

if $CHECKS; then
    echo "=== Checks ==="
    if ! TEST_OUTPUT="$(cargo test --locked 2>&1)"; then
        grep -E 'FAILED|panicked' <<<"$TEST_OUTPUT"
        echo "Tests failed; not deploying."
        exit 1
    fi
    echo "  tests pass"
    cargo clippy --locked --all-targets --all-features -- -D warnings 2>&1 | tail -1
    echo ""
fi

echo "=== Build $HEAD_COMMIT ==="
cargo build --locked --release 2>&1 | tail -1
echo ""

echo "=== Restart $UNIT ==="
systemctl --user restart "$UNIT"
sleep 2
if ! systemctl --user is-active --quiet "$UNIT"; then
    echo "  $UNIT is not active:"
    journalctl --user -u "$UNIT" -n 20 --no-pager
    exit 1
fi
"$BIN" status >/dev/null || { echo "  daemon did not answer status"; exit 1; }
echo "  active and answering"
echo ""

mkdir -p "$(dirname "$RECORD")"
{
    echo "commit $HEAD_COMMIT $(git log -1 --format=%s)"
    echo "sha256 $(sha256sum "$BIN" | cut -d' ' -f1)"
    echo "deployed $(date -Iseconds)"
} >"$RECORD"
echo "=== Deployed ==="
cat "$RECORD"
