#!/usr/bin/env bash
# ChatterDaemon — one-command run: scan (persist + raw history) -> render PDF -> summary.
#
# Repeatable entrypoint. ALL config lives in .env (auto-loaded by the daemon): the Finnhub /
# Anthropic keys, the Twitter session cookies + absolute CLI path, and the Order-21 Twitter
# priority list + top-N cap. So this script needs NO env exports and NO `source .env`, and the
# Twitter CLI is invoked by absolute path so $PATH does not matter. Runs on WSL today and macOS
# later (bash on both). Override the interpreter with $PYTHON if the venv lives elsewhere.
#
#   bash scripts/run.sh
#
set -uo pipefail

cd "$(dirname "$0")/.." || { echo "run.sh: cannot cd to the daemon root" >&2; exit 2; }

# Resolve the venv python: $PYTHON wins, else the WSL-native venv, else a generic .venv.
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for cand in .venv-linux/bin/python .venv/bin/python; do
    [ -x "$cand" ] && PY="$cand" && break
  done
fi
[ -n "$PY" ] && [ -x "$PY" ] || { echo "run.sh: no venv python found (set \$PYTHON)" >&2; exit 2; }

# Self-heal the Twitter-CLI patch (idempotent). A pipx reinstall silently reverts it and the
# Twitter lane goes dark with no obvious cause; re-applying here keeps runs repeatable. Best-effort.
"$PY" scripts/patch_twitter_cli.py 2>&1 | sed 's/^/run.sh: twitter-patch: /' || true

# Optional watchlist arg: `run.sh mando_watch_list` scans just that list; no arg = every list.
WL="${1:-}"
if [ -n "$WL" ]; then SCAN=(scan --watchlist "$WL"); else SCAN=(scan --all); fi

TMP="${TMPDIR:-/tmp}"
echo "=== chatter scan [${WL:-all}] $(date -u +%FT%TZ) ==="
start=$(date +%s)
"$PY" -m chatter_daemon "${SCAN[@]}" > "$TMP/chatter_scan.json" 2> "$TMP/chatter_scan.err"
rc=$?
echo "scan exit: $rc  wall: $(( $(date +%s) - start ))s"

arch=$(ls -t archive/*/*.json 2>/dev/null | head -1)
hist=$(ls -t history/*.txt 2>/dev/null | head -1)
[ -n "$arch" ] && echo "archive: $arch"
[ -n "$hist" ] && echo "history: $hist ($(wc -l < "$hist") lines)"

# Render the client-facing PDF from the just-persisted scan.
if [ -n "$arch" ]; then
  if "$PY" -m chatter_daemon report "$arch" > "$TMP/chatter_report.out" 2> "$TMP/chatter_report.err"; then
    echo "report:  $(ls -t chatter-report_*.pdf 2>/dev/null | head -1)"
  else
    echo "report:  FAILED (see $TMP/chatter_report.err)" >&2
  fi
fi

echo "--- scan warnings / degraded (tail) ---"
tail -8 "$TMP/chatter_scan.err" 2>/dev/null || true
exit "$rc"
