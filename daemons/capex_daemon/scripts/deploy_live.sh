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
sleep 4
launchctl list | grep capex-dash || fail "capex-dash did not load"

TS_BIN="$(command -v tailscale || true)"
[ -x "$TS_BIN" ] || TS_BIN="/Applications/Tailscale.app/Contents/MacOS/Tailscale"
HOST="$("$TS_BIN" ip -4 2>/dev/null | head -1)"
[ -n "$HOST" ] || fail "no Tailscale address; dashboard cannot be verified"
curl -fsS --max-time 25 "http://$HOST:8788/health" || fail "health check failed"
echo

echo
echo "deployed $head_sha to $LIVE"
