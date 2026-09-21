#!/bin/zsh
# Steady-state delta-scan runner for launchd (com.abelard.smart-money).
# Mando-authorized deploy 2026-07-22. Runs one scan, appends a timestamped line
# to the state-home log. Exit code is the scan's own (all-sources-failed => 1).
# Relative to THIS file, never to a named checkout (E33: a launcher must stay
# inside the tree it was started from). This used to cd into ~/Code/Abelard,
# the development checkout, so a plist pointed at the pinned live root would
# still have run whatever branch was checked out there.
cd "$(dirname "$0")/.." || exit 2
LOG=~/.openclaw/smart_money/logs/scan.log
echo ">>> scan run $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
.venv/bin/python -m smart_money.scan >> $LOG 2>&1
rc=$?
echo "<<< exit $rc $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
exit $rc
