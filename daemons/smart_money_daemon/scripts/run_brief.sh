#!/bin/bash
# SM-R1 scheduled brief (Basilic). Renders the full brief to state home shortly
# after the nightly scan completes. Launched by launchd
# com.abelard.smart-money-brief. Referenced, never emailed (alerting is out of
# scope). Logs go to the state-home logs dir.
set -euo pipefail
cd "$(dirname "$0")/.."
# SM-C3 Phase W rider: one randomized-time eFD probe per brief run. Read-only, a single
# request, and never fatal to the brief (|| true) — a failed probe is a datapoint, not an
# outage. NOTE: this job fires in the evening, so the rider ALONE samples one hour; the
# hour-of-day window map needs the spread-sample job in ops/com.abelard.efd-probe.plist.
.venv/bin/python -m smart_money.waf_probe --rider \
  --db "$HOME/.openclaw/smart_money/smart_money_v0.db" || true

exec .venv/bin/python -m smart_money.brief --scheduled \
  --db "$HOME/.openclaw/smart_money/smart_money_v0.db"
