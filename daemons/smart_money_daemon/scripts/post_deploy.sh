#!/bin/bash
# After scripts/deploy_live.sh advances the live root: migrate the smart_money schema,
# read the 13F arrival ledger, then restart the long-lived dashboard. Run it from the
# live tree, every deploy.
#
# The deploy gate only moves code, and that is not enough here for two reasons.
# The brief and the dashboard open the database READ-ONLY (queries.connect_ro) and so
# can never migrate it: a column the new code reads does not exist until something
# calls db.connect(), and until then every page that reads it answers 500. And the
# dashboard is ONE long-lived process that keeps its modules loaded and imports the
# brief lazily, so without a restart it can serve a new brief.py against an old
# queries.py.
#
# The migration is additive (new nullable columns) and old code reads by column name,
# so running it is safe whichever code is live. It fails loud if the nightly scan holds
# the write lock -- run it outside 22:30-23:20.
set -euo pipefail
cd "$(dirname "$0")/.."
.venv/bin/python - <<'PY'
from smart_money import db
con = db.connect(db.DB_PATH_DEFAULT)
con.close()
print("post_deploy: migrated", db.DB_PATH_DEFAULT)
PY
# The arrival ledger's read (SM-H1). On a database that has never been read this is the
# backfill: every 13F-family filing in each filer's feed and every cover since 2023,
# 618 requests and about four minutes. Left to the nightly leg it would run inside the
# 22:30 scan, which already ends minutes before the clock-gated 23:15 brief. On every
# later deploy it is 28 requests and writes nothing new. The dashboard is restarted
# whatever the read did, because the pages say what was not read. But the two ways it can
# go wrong are told apart: some reads failing is the nightly leg's to retry, and NO read
# happening leaves the whole backfill for tonight's scan, which this script exists to
# prevent, so that ends the script non-zero.
set +e
.venv/bin/python -m smart_money.arrivals_ingest
ledger=$?
set -e
launchctl kickstart -k "gui/$(id -u)/com.abelard.smart-money-dash"
echo "post_deploy: restarted com.abelard.smart-money-dash"
if [ "$ledger" -eq 1 ]; then
  echo "post_deploy: the ledger read reported failures (above); the nightly leg retries them"
elif [ "$ledger" -ne 0 ]; then
  echo "post_deploy: THE LEDGER READ DID NOT RUN (exit $ledger). Run" >&2
  echo "  .venv/bin/python -m smart_money.arrivals_ingest" >&2
  echo "before 22:30, or tonight's scan carries the 618-request first read." >&2
  exit 1
fi
