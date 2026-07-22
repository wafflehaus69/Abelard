#!/bin/bash
# One-way mirror of Basilic staging collection data down to Orban local disk.
# Run from Orban WSL. Basilic DB is canonical while it is the active collector,
# so the DB is checkpointed remotely then overwritten locally. Raw artifacts
# are immutable and unioned — never deleted on Orban (no --delete on purpose).
set -euo pipefail
LOCAL=/mnt/c/Users/mdiba/Code/Abelard/daemons/smart_money_daemon
REMOTE=wafflehaus@basilic:staging/smart_money_daemon

# Consistent snapshot even while the collector is mid-write: VACUUM INTO gives a
# transactionally-clean copy (a live-file rsync produces a torn "database disk
# image is malformed"). We rsync the snapshot, not the hot DB.
ssh wafflehaus@basilic \
  "cd ~/staging/smart_money_daemon && rm -f data/cache/_sync_snapshot.db && \
   .venv/bin/python -c \
  \"import sqlite3; sqlite3.connect('data/cache/smart_money_v0.db').execute(\\\"VACUUM INTO 'data/cache/_sync_snapshot.db'\\\")\""

rsync -az --inplace --stats \
  --exclude='smart_money_v0.db' --exclude='smart_money_v0.db-wal' \
  --exclude='smart_money_v0.db-shm' \
  "$REMOTE/data/" "$LOCAL/data/"
# Drop any local WAL/SHM first: a stale WAL replayed over the fresh snapshot
# corrupts it (the snapshot itself is WAL-free).
rm -f "$LOCAL/data/cache/smart_money_v0.db-wal" \
      "$LOCAL/data/cache/smart_money_v0.db-shm"
rsync -az --inplace "$REMOTE/data/cache/_sync_snapshot.db" \
  "$LOCAL/data/cache/smart_money_v0.db"
rsync -az --inplace --stats "$REMOTE/logs/" "$LOCAL/logs_basilic/"
echo "sync complete $(date -u +%Y-%m-%dT%H:%M:%SZ)"
