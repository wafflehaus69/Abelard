#!/bin/bash
# Capex read-only dashboard launcher (Basilic). Mirrors the Smart Money pattern
# exactly. Mando-authorized deploy 2026-08-22 (CD-DASH1 P1).
#
# Binds to the Tailscale IP ONLY, never 0.0.0.0 and never a public interface.
# If there is no Tailscale IPv4 address this REFUSES TO START rather than
# falling back to a broader bind — a dashboard that silently becomes reachable
# from somewhere it should not be is worse than one that is down, and launchd's
# KeepAlive will surface the failure as a restart loop in the log.
set -euo pipefail
cd "$(dirname "$0")/.."
# The tailscale CLI is not on the launchd PATH on macOS; it lives in the app
# bundle. Resolve it robustly.
TS_BIN="$(command -v tailscale || true)"
[ -x "$TS_BIN" ] || TS_BIN="/Applications/Tailscale.app/Contents/MacOS/Tailscale"
HOST="$("$TS_BIN" ip -4 2>/dev/null | head -1)"
if [ -z "$HOST" ]; then
  echo "run_dash: no Tailscale IPv4 address - refusing to start" >&2
  exit 1
fi
exec .venv/bin/python -m capex_daemon dashboard --host "$HOST" --port 8788
