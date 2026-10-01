#!/bin/bash
# After scripts/deploy_live.sh advances the live root: migrate the smart_money schema,
# then restart the long-lived dashboard. Run it from the live tree, every deploy.
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
launchctl kickstart -k "gui/$(id -u)/com.abelard.smart-money-dash"
echo "post_deploy: restarted com.abelard.smart-money-dash"
