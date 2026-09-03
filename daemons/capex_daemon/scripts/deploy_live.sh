#!/bin/bash
# CD-BRIEF1 — deploy the capex daemon to the pinned service root.
#
# **A gate, not a wish.** The previous deploy was a sequence of hopeful commands
# with `git pull --ff-only origin main >/dev/null 2>&1` at the top. The pull
# failed — the live tree was on another workstream's branch and could not
# fast-forward — and the redirect swallowed it. Everything after it ran happily
# against the wrong code: tests passed, a scan ran, a snapshot was rebuilt, and
# the summary line reported success. That is the pull-through-tail class that
# has bitten Smart Money three times: a failure upstream of a pipe or a redirect
# never reaches the exit status, so the script cannot tell that it has already
# lost.
#
# So every step here is an assertion with a nonzero exit behind it:
#
#   * refuses unless the live tree is on `main`
#   * never redirects pull output
#   * asserts HEAD == origin/main AFTER the pull, not before
#   * prints the commit hash it deployed
#   * `set -euo pipefail` and an explicit `|| exit` on each check
#
# It deploys ONLY the pinned service root. It never touches a development
# checkout, never switches a branch, and never merges anything.
set -euo pipefail

LIVE="${ABELARD_LIVE:-$HOME/Code/abelard-live}"

# --- 0. run from a COPY, because step 2 rewrites this file -------------------
#
# This script lives inside the tree it deploys, and the pull below updates it.
# bash reads a script incrementally by byte offset, so a file that changes
# underneath a running shell resumes at the wrong place and executes whatever
# now sits at that offset. It bit this gate twice: the second run reported a
# health-check timeout of 7795ms when the code being edited had a 3s cap — the
# shell was executing the previous revision's bytes.
#
# So: copy to a stable location outside the tree and hand over immediately. The
# copy is immune to the pull, and re-exec happens before anything else is read.
if [ -z "${ABELARD_DEPLOY_DETACHED:-}" ]; then
  _self="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
  _copy="$(mktemp -t abelard-deploy)" || exit 1
  cat "$_self" >"$_copy"
  chmod +x "$_copy"
  ABELARD_DEPLOY_DETACHED=1 exec bash "$_copy"
fi
DAEMON="$LIVE/daemons/capex_daemon"
EXPECT_BRANCH=main

fail() { echo "deploy: $*" >&2; exit 1; }

# --- 1. the service root must exist and be the pinned one -------------------
[ -d "$LIVE/.git" ] || [ -f "$LIVE/.git" ] || fail "no git tree at $LIVE"
cd "$LIVE"

branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$branch" = "$EXPECT_BRANCH" ] || fail \
  "live tree is on '$branch', not '$EXPECT_BRANCH'. A service root is a pinned
   ref; it is not a place to check out a feature branch. Refusing."

# A dirty service root means someone edited production in place.
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  git status --short
  fail "live tree has local modifications. Refusing to deploy over them."
fi

# --- 2. pull, LOUDLY ---------------------------------------------------------
echo "== fetching =="
git fetch origin "$EXPECT_BRANCH"
echo "== pulling =="
git merge --ff-only "origin/$EXPECT_BRANCH"

# --- 3. assert we are exactly where we meant to be ---------------------------
head_sha="$(git rev-parse HEAD)"
origin_sha="$(git rev-parse "origin/$EXPECT_BRANCH")"
[ "$head_sha" = "$origin_sha" ] || fail \
  "after pull, HEAD ($head_sha) != origin/$EXPECT_BRANCH ($origin_sha)"

echo
echo "== deploying =="
echo "   root:   $LIVE"
echo "   branch: $branch"
echo "   commit: $head_sha"
echo "   subject: $(git log -1 --format=%s)"

# --- 4. tests must pass IN THE TREE BEING DEPLOYED ---------------------------
cd "$DAEMON"
[ -x .venv/bin/python ] || fail "no venv at $DAEMON/.venv"
echo
echo "== tests =="
.venv/bin/python -m pytest tests/ -q

# --- 5. scan, then republish so code-derived fields reach the published view --
echo
echo "== scan =="
.venv/bin/python -m capex_daemon scan
echo "== rebuild (recompute and republish; does not re-ingest) =="
.venv/bin/python -m capex_daemon scan --rebuild

# --- 6. artifacts ------------------------------------------------------------
echo
echo "== render =="
.venv/bin/python -m capex_daemon report --brief
.venv/bin/python -m capex_daemon report

# --- 7. restart the dashboard and prove it answers ---------------------------
echo
echo "== dashboard =="
PLIST="$HOME/Library/LaunchAgents/com.abelard.capex-dash.plist"
launchctl unload "$PLIST" 2>/dev/null || true
sleep 1
launchctl load "$PLIST"
launchctl list | grep capex-dash || fail "capex-dash did not load"

TS_BIN="$(command -v tailscale || true)"
[ -x "$TS_BIN" ] || TS_BIN="/Applications/Tailscale.app/Contents/MacOS/Tailscale"
HOST="$("$TS_BIN" ip -4 2>/dev/null | head -1)"
[ -n "$HOST" ] || fail "no Tailscale address; dashboard cannot be verified"

# POLL for readiness; never sleep-and-hope. A fixed sleep failed this gate on
# its first real run — the server was binding and answered a second later — and
# "wait a guessed interval, then assert" is the same shape as the redirect this
# script exists to eliminate: it converts a timing question into a false verdict.
ready=""
for _ in $(seq 1 30); do
  if curl -fsS --max-time 3 "http://$HOST:8788/health" >/tmp/capex_health.json 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
[ -n "$ready" ] || fail "dashboard did not answer /health within 30s"
cat /tmp/capex_health.json
echo

echo
echo "deployed $head_sha to $LIVE"
