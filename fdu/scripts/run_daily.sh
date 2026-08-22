#!/bin/zsh
# FDU daily cadence: pull the bulk snapshot, then enrich what moved.
#
# Deliberately uses the ABSOLUTE interpreter path. A non-interactive shell on
# Basilic resolves `python3` to /usr/bin/python3 (3.9.6), which is below FDU's
# floor; the homebrew 3.14 is only on a login shell's PATH. A scheduled job that
# said `python3` would fail every night with a syntax error.
set -u

FDU_HOME="${FDU_HOME:-$HOME/Code/Abelard-fdu/fdu}"
PY="$FDU_HOME/.venv/bin/python"
STATE="${FDU_STATE_HOME:-$HOME/.openclaw/fdu}"
LOG="$STATE/logs/daily.log"

mkdir -p "$STATE/logs"
cd "$FDU_HOME" || exit 1

stamp() { date -u +%FT%TZ; }
say() { echo "$(stamp) $*" >>"$LOG"; }

say "=== daily start ==="

if [ -f "$STATE/HALT" ]; then
  say "HALT present; doing nothing"
  exit 0
fi

if [ ! -x "$PY" ]; then
  say "FATAL: interpreter missing at $PY"
  exit 1
fi

# -- scan is always safe: one 7 MB GET, no per-firm traffic ---------------
"$PY" -m fdu_daemon.cli scan >>"$LOG" 2>&1
rc=$?
say "scan rc=$rc"
[ $rc -ne 0 ] && { say "scan failed; skipping enrich"; exit $rc; }

# -- do not compete with a running backfill ------------------------------
# Both would hit the same surface at once and double the request rate for no
# gain: the backfill will reach these firms anyway. Skipping is the polite and
# the correct behaviour, and it is logged rather than silent.
if pgrep -f "scripts_backfill.sh" >/dev/null 2>&1; then
  say "backfill in progress; skipping enrich this cycle"
  exit 0
fi

"$PY" -m fdu_daemon.cli enrich --limit 300 >>"$LOG" 2>&1
rc=$?
say "enrich rc=$rc"

"$PY" -m fdu_daemon.cli status >>"$LOG" 2>&1
say "=== daily end ==="
exit 0
