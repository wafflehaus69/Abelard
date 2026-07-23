#!/bin/bash
# One-way mirror of Basilic's CANONICAL state home -> Orban. Run from Orban WSL.
# Basilic (~/.openclaw/smart_money/) is the live collector and source of truth.
# VACUUM INTO gives a transactionally-clean snapshot even mid-write (a live-file
# rsync produces a torn "database disk image is malformed"). We pull the
# snapshot, not the hot DB. Fixed 2026-07-23: was pointed at the pre-SM-4
# ~/staging + data/cache path and silently mirrored a frozen DB.
set -euo pipefail
LOCAL_HOME="$HOME/.openclaw/smart_money"
RVENV="/Users/wafflehaus/Code/Abelard/daemons/smart_money_daemon/.venv/bin/python"
RDB="/Users/wafflehaus/.openclaw/smart_money/smart_money_v0.db"
RSNAP="/Users/wafflehaus/.openclaw/smart_money/_sync_snapshot.db"
mkdir -p "$LOCAL_HOME/scans"

ssh wafflehaus@basilic "rm -f '$RSNAP'; $RVENV -c \"import sqlite3; sqlite3.connect('$RDB').execute(\\\"VACUUM INTO '$RSNAP'\\\")\""

# Stale local WAL/SHM replayed over the fresh snapshot corrupts it — drop first.
rm -f "$LOCAL_HOME/smart_money_v0.db-wal" "$LOCAL_HOME/smart_money_v0.db-shm"
rsync -az --inplace --stats \
  "wafflehaus@basilic:.openclaw/smart_money/_sync_snapshot.db" \
  "$LOCAL_HOME/smart_money_v0.db"
rsync -az --inplace "wafflehaus@basilic:.openclaw/smart_money/scans/" \
  "$LOCAL_HOME/scans/" 2>/dev/null || true
echo "sync complete $(date -u +%Y-%m-%dT%H:%M:%SZ)"
