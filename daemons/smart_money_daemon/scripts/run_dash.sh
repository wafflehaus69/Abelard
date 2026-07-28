#!/bin/bash
# SM-R1 dashboard launcher (Basilic). Binds to the Tailscale IP ONLY, never
# 0.0.0.0/public. Launched by launchd com.abelard.smart-money-dash with
# KeepAlive (restart-on-crash). Logs go to the state-home logs dir.
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
exec .venv/bin/python -m smart_money.dashboard --host "$HOST" --port 8787
