-- Schema v2 — composite index for the scrape orchestrator's dedup lookup.
--
-- The scrape layer's dedup check is:
--   SELECT 1 FROM headlines
--   WHERE dedupe_hash = ? AND fetched_at_unix >= ?
--   LIMIT 1;
--
-- With only the single-column idx_headlines_dedupe from v1, SQLite would
-- use that index for the equality and apply the time filter on the
-- matching rowset. That's fine for low dedup-hit volumes but degrades as
-- the table grows. The composite covers the full predicate in one seek.
--
-- The original idx_headlines_dedupe is intentionally retained — storage
-- cost is negligible and other future queries may key off dedupe_hash
-- alone (e.g. cross-window dedup analytics).
--
-- IF NOT EXISTS guards make this migration safe to re-run.

CREATE INDEX IF NOT EXISTS idx_headlines_dedupe_fetched
    ON headlines(dedupe_hash, fetched_at_unix);
