#!/bin/zsh
# Basilic staging collector, House Clerk backfill. Mando-authorized 2026-07-17.
# Resume-safe by DocID. Fail-loud stops leave the exact error in the log.
cd ~/staging/smart_money_daemon || exit 2
exec .venv/bin/python -u -m smart_money.house_ingest
