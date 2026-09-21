#!/usr/bin/env bash
#
# Abelard morning briefs — the weekday 8am ET cycle (Mando 2026-07-15).
#
# Runs the two report daemons back-to-back and emails the PDFs:
#   1. news_watch  `run --pdf`  -> ~/.openclaw/news_watch/brief-latest.pdf
#      (Mondays scan a 72h window so the weekend gap is covered; the
#      daemon's alert path still enqueues to abelard_queue as always)
#   2. chatter     `scan --all` -> archive JSON -> `report` ->
#      ~/.openclaw/chatter/report-latest.pdf
#   3. scripts/email_briefs.py  -> one message, every FRESH pdf attached
#      (creds sourced from abelard_queue/.env — Abelard's outbound file;
#      nothing secret lives in launchd)
#
# Fail loud, degrade gracefully: one daemon failing never blocks the other's
# brief (the emailer skips stale PDFs), but ANY failure -> non-zero exit so
# launchd logs show it. Daemons self-load their own .env; this script never
# reads keys itself.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NW_DIR="$REPO_ROOT/daemons/news_watch_daemon"
CH_DIR="$REPO_ROOT/daemons/chatter_daemon"
NW_PDF="$HOME/.openclaw/news_watch/brief-latest.pdf"
CH_PDF="$HOME/.openclaw/chatter/report-latest.pdf"
FAILURES=""

mkdir -p "$HOME/.openclaw/chatter"

# --- 1. news watch full brief ------------------------------------------------
# Monday (%u = 1) widens the window to cover Sat+Sun since Friday's run.
WINDOW_HOURS=24
[ "$(date +%u)" = "1" ] && WINDOW_HOURS=72
echo ">> news_watch run (window ${WINDOW_HOURS}h)"
NW_OUT="$(mktemp)"
# Capture stdout (the "Artifact (JSON):" line) while still logging it; INFO logs
# ride stderr straight to the log, untouched by the pipe.
( cd "$NW_DIR" && .venv/bin/news-watch-daemon run \
    --pdf "$NW_PDF" --window-hours "$WINDOW_HOURS" --quiet ) | tee "$NW_OUT"
rc=${PIPESTATUS[0]}
# exit 2 = ran with attention-state (e.g. queue items pending) — brief exists.
if [ $rc -ne 0 ] && [ $rc -ne 2 ]; then
  echo "!! news_watch run failed (exit $rc)" >&2
  FAILURES="$FAILURES news_watch($rc)"
fi
NW_JSON="$(grep -E '^Artifact \(JSON\):' "$NW_OUT" | tail -1 | sed -E 's/^Artifact \(JSON\): *//')"
rm -f "$NW_OUT"
NW_SUMMARY=""
if [ -n "$NW_JSON" ] && [ -f "$NW_JSON" ]; then
  NW_SUMMARY="$("$NW_DIR/.venv/bin/python" "$NW_DIR/tools/brief_summary.py" "$NW_JSON")"
fi

# --- 2. chatter scan -> newest archive json -> pdf report --------------------
echo ">> chatter scan --all"
SCAN_STAMP="$(mktemp)"
( cd "$CH_DIR" && .venv/bin/python -m chatter_daemon scan --all )
rc=$?
[ $rc -ne 0 ] && { echo "!! chatter scan failed (exit $rc)" >&2; FAILURES="$FAILURES chatter-scan($rc)"; }
# Render whatever fresh scan JSON exists (a partial-failure scan may still
# have persisted a result worth reading).
NEWEST_JSON="$(find "$CH_DIR/archive" -name '*.json' -newer "$SCAN_STAMP" -print 2>/dev/null | sort | tail -1)"
rm -f "$SCAN_STAMP"
CH_SUMMARY=""
if [ -n "$NEWEST_JSON" ]; then
  echo ">> chatter report: $NEWEST_JSON"
  if ! ( cd "$CH_DIR" && .venv/bin/python -m chatter_daemon report "$NEWEST_JSON" --out "$CH_PDF" ); then
    echo "!! chatter report render failed" >&2
    FAILURES="$FAILURES chatter-report"
  fi
  CH_SUMMARY="$("$CH_DIR/.venv/bin/python" "$CH_DIR/tools/scan_summary.py" "$NEWEST_JSON")"
else
  echo "!! chatter: no fresh scan JSON — skipping report" >&2
fi

# --- 3. email every fresh pdf (one message) -----------------------------------
echo ">> email briefs"
set -a
# shellcheck disable=SC1091 — Abelard's outbound-channel creds (SMTP_*, BRIEF_EMAIL_TO)
. "$REPO_ROOT/abelard_queue/.env"
set +a
EMAIL_ARGS=(--pdf "$NW_PDF" --pdf "$CH_PDF")
# Order matters — news line first, then chatter — to read top-to-bottom in the body.
[ -n "$NW_SUMMARY" ] && EMAIL_ARGS+=(--summary "$NW_SUMMARY")
[ -n "$CH_SUMMARY" ] && EMAIL_ARGS+=(--summary "$CH_SUMMARY")
if ! "$NW_DIR/.venv/bin/python" "$SCRIPT_DIR/email_briefs.py" "${EMAIL_ARGS[@]}"; then
  echo "!! email dispatch failed" >&2
  FAILURES="$FAILURES email"
fi

if [ -n "$FAILURES" ]; then
  echo ">> FAILED:$FAILURES" >&2
  exit 1
fi
echo ">> morning briefs complete"
