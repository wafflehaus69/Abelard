#!/bin/bash
# SM-R1 scheduled brief (Basilic). Renders the full brief to state home shortly
# after the nightly scan completes. Launched by launchd
# com.abelard.smart-money-brief. Referenced, never emailed (alerting is out of
# scope). Logs go to the state-home logs dir.
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python -m smart_money.brief --scheduled \
  --db "$HOME/.openclaw/smart_money/smart_money_v0.db"
