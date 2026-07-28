#!/bin/bash
# SM-R1 dashboard launcher (Basilic). Binds to the Tailscale IP ONLY, never
# 0.0.0.0/public. Launched by launchd com.abelard.smart-money-dash with
# KeepAlive (restart-on-crash). Logs go to the state-home logs dir.
set -euo pipefail
cd "$(dirname "$0")/.."
HOST="$(tailscale ip -4 2>/dev/null | head -1)"
if [ -z "$HOST" ]; then
  echo "run_dash: no Tailscale IPv4 address - refusing to start" >&2
  exit 1
fi
exec .venv/bin/python -m smart_money.dashboard --host "$HOST" --port 8787
