#!/bin/zsh
# Nightly price-substrate runner for launchd (com.abelard.prices).
# PS-1B Phase D. Mirrors the Capex and Smart Money runners: one run, a
# timestamped line either side of it in the state-home log, and the command's
# own exit code passed through so the slot alerts on status alone without
# parsing output.
#
# Exit codes, from prices/cli.py:
#   0  clean
#   1  something lagged, a fact changed, the vendor was degraded, or a
#      verification sweep disagreed. All are "look at it", not "it is broken".
#   2  the run could not start (no store, no contact, no token)
#   3  this wrapper could not find the package directory at all.
#
# A code change needs only `git pull` -- this file is referenced by absolute
# path from the plist and does not itself need reloading.
#
# The .env holds TIINGO_API_TOKEN and is read HERE, not by the library: the
# package takes explicit paths and never reads the environment itself
# (alert_queue's discipline, Mando's ruling 3 of 2026-09-02).

cd ~/Code/Abelard/daemons/common || exit 3

STATE=~/.openclaw/prices
LOG=$STATE/logs/prices.log
mkdir -p $STATE/logs

# Absolute, because launchd does not expand ~ and a relative store path would
# silently create a second, empty database wherever the job happened to start.
export ABELARD_PRICES_DB_PATH=$STATE/prices.db

if [ -f $STATE/.env ]; then
  set -a; . $STATE/.env; set +a
fi

# EDGAR requires a declared contact and returns 403 without one, but only
# `universe-sync` talks to EDGAR and that verb is not in this nightly. So this
# is a fallback, not a guard: pick the contact up from Smart Money's .env if the
# prices one does not carry it, and let the verb that needs it fail loud on its
# own if neither does. Failing the whole night here would cost the append over a
# variable the append never reads.
if [ -z "$EDGAR_CONTACT" ] && [ -f ~/Code/Abelard/daemons/smart_money_daemon/.env ]; then
  set -a; . ~/Code/Abelard/daemons/smart_money_daemon/.env; set +a
fi

PY=.venv/bin/python
[ -x $PY ] || { echo "run_prices: no venv at daemons/common/.venv" >&2; exit 3; }

run() {
  echo ">>> prices $1 $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
  $PY -m abelard_common.prices.cli "$@" >> $LOG 2>&1
  local rc=$?
  echo "<<< $1 exit $rc $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
  return $rc
}

# Order matters and is not arbitrary.
#   1. nightly append first, so the store is current before anything reads it.
#   2. reference series next: the reconciliation benchmark comes from here.
#   3. reconcile, which needs both of the above for the session just appended.
#   4. verify last -- it is the metered call and the only one that can be
#      refused by a quota; putting it last means a refusal never costs the
#      store its nightly append.
# Each leg's status is kept and the worst is returned, so one failure is
# visible in the exit code without hiding the others in the log.
worst=0
run nightly;              rc=$?; [ $rc -gt $worst ] && worst=$rc
run reference --no-fred;  rc=$?; [ $rc -gt $worst ] && worst=$rc
run reconcile;            rc=$?; [ $rc -gt $worst ] && worst=$rc
run verify -n 18;         rc=$?; [ $rc -gt $worst ] && worst=$rc

echo "=== prices night done, worst exit $worst $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $LOG
exit $worst
