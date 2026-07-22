#!/bin/zsh
# Basilic Senate collector. Mando-authorized 2026-07-17, reworked 2026-07-20.
# eFD deployed a WAF that 503s the DataTables index endpoint (recon/EFD_WAF_
# FINDING.md). Detail pages are NOT WAF-blocked, so this ingests the full
# backfill from a browser-harvested index via plain requests. Resume-safe by
# filing uuid. Fail-loud stops leave the exact error in the log.
cd ~/staging/smart_money_daemon || exit 2
exec .venv/bin/python -u -m smart_money.efd_ingest \
  --index-file data/raw/efd/senate_ptr_index_20260720.json
