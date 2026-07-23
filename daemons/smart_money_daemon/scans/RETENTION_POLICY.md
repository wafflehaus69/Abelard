# RETENTION_POLICY — smart_money_daemon SM-D1 Phase D

Design proposal for Mando's ratification. No code here. Rules are per data
class, keyed to whether the data is regenerable from committed code + network.
The extract-to-Orban pipeline is explicitly out of scope (its own order).

## Principle
The DB is the source of truth for parsed facts. Raw captures exist to (a) parse
into the DB and (b) allow re-parse/debug. Once parsed, raw is regenerable and
its retention is a convenience/debug window, not a durability requirement.

## Per-class rules

| Data class | Location | Regenerable | Proposed retention |
|---|---|---|---|
| House PTR zips + extracted PDFs | data/raw/house (676M) | YES (House Clerk + committed parser) | Purge PDFs 14 days after the DocID is successfully parsed into ingested_filings. Keep the yearly index zips (tiny) as the re-fetch manifest. |
| Unparsed-layout House PDFs | data/raw/house_unparsed (212M) | YES | KEEP until a second-generation House parser is built or declined (they are the raw material for that order). Then purge. Meanwhile cap at most-recent 2 filing years on disk; older DocIDs are re-fetchable. |
| eFD raw HTML/index | data/raw/efd (38M) | YES (browser harvest + requests) | Keep the harvested PTR index JSON (small, the Senate work-list). Purge cached detail HTML 14 days after parse. |
| EDGAR Form 4 raw XML | (transient, not persisted) | YES | Do not persist raw XML at all; parse-and-drop. The corpus table is the durable home. Backfill re-fetches on demand. |
| price rows | DB prices table (part of 251M) | YES but EXPENSIVE and history-critical | **DO NOT PRUNE.** The scorecard needs full history — 21/63/126d forward horizons, recency decay out to 60 months, and excess-vs-SPY on trades back to 2012. Pruning old prices breaks deterministic re-scores. Confirmed against the data: spans run 2012->2026 and are all reachable by the scorecard. Keep-forever. |
| congress_trades / persons / form4_transactions / 13F baseline | DB | NO (Senate needs browser re-harvest; corpus is accumulated) | KEEP-FOREVER. These are the accumulated corpus, not regenerable cheaply. |
| scan envelopes | ~/.openclaw/smart_money/scans (32K) | partially (DB has the events) | KEEP-FOREVER — they are tiny and are the audit trail of what fired when. Revisit only if the dir exceeds ~100MB (years away). |
| logs (scan.log, launchd.out/err) | ~/.openclaw/smart_money/logs | NO but low-value | Rotate: cap scan.log at ~10MB with 3 rotations, or purge entries older than 90 days. |
| deploy/sync snapshots (_deploy_snapshot.db, _sync_snapshot.db) | state home + data/cache | YES (VACUUM copies) | EPHEMERAL — always delete immediately after the transfer that created them. The sync script already rm's its snapshot; the deploy path missed one (see VESTIGIAL_INVENTORY). Rule: no snapshot survives its own script. |

## Enforcement note (for the future implementation order)
- A `purge` subcommand keyed to "successfully parsed" is the clean mechanism:
  it reads ingested_filings for parsed DocIDs and removes their raw PDFs past
  the horizon. Fail-loud if a DocID marked parsed has no DB rows (would signal
  a bad parse, not a purge candidate).
- Price pruning is deliberately absent — the one class where retention is a
  correctness requirement, not a convenience.
