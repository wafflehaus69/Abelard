#!/bin/zsh
# Nightly scan runner for launchd (com.abelard.capex).
# Mando-authorized deploy 2026-08-21. Matches the Smart Money runner: one run,
# a timestamped line either side of it in the state-home log, and the scan's own
# exit code passed through so the slot can alert on status alone without parsing
# output.
#
# Exit codes: 0 on a clean run AND on a clean no-op (most nights are no-ops and
# that is the design point, not a fault). Non-zero only when something broke.
# 2 is this wrapper failing to find the daemon directory at all.
#
# A code change needs only `git pull` — this file is referenced by absolute path
# from the plist and does not itself need reloading.
#
# **Resolve the daemon directory RELATIVE to this script, never absolutely.**
# This used to `cd ~/Code/Abelard/daemons/capex_daemon`, which meant the service
# ran whatever branch the shared development checkout happened to be on. It was
# found on `ps-1-price-substrate` — another workstream's branch — serving code
# no one had chosen to deploy. Worse, a plist repointed at a pinned service root
# would still have been dragged back here by this line.
#
# A service root must be a pinned ref, and a runner must stay inside the tree it
# was launched from. `run_dash.sh` already did this; this one did not.
cd "$(dirname "$0")/.." || exit 2
LOG=~/.openclaw/capex_daemon/logs/scan.log
mkdir -p ~/.openclaw/capex_daemon/logs
echo ">>> capex scan $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
.venv/bin/python -m capex_daemon scan >> $LOG 2>&1
rc=$?
echo "<<< exit $rc $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
exit $rc
