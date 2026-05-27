-- Schema v3 — language column on headlines for Pass F translation gating.
--
-- Task 2 (2026-05-27). Adds a TEXT column populated by an orchestrator-
-- level classifier at headline ingest. Pass F's translation pass uses
-- this column to find candidates for translation:
--
--   SELECT ... FROM headlines WHERE language != 'en' AND ...
--
-- The classifier emits "ru" / "en" / "mixed". "other" is reserved for
-- future non-Cyrillic / non-Latin scripts (CJK, Arabic, etc.); the
-- current classifier does not assign it. When that becomes operationally
-- relevant the classifier expands and the column accepts the new value
-- without further migration.
--
-- ALTER TABLE ADD COLUMN is a metadata-only operation in SQLite — no
-- table rewrite, no locking issues on large tables. Existing rows get
-- NULL until the operator runs:
--
--   news-watch-daemon db backfill-language
--
-- The backfill subcommand is idempotent — re-running it after all rows
-- are classified is a zero-cost no-op. New rows inserted after this
-- migration land with non-null language directly (orchestrator runs the
-- classifier inside _insert_headline_and_tags).
--
-- Index motivation: Pass F's translation-candidate query is
--   WHERE language != 'en' AND fetched_at_unix >= ?
-- which the language index makes seek-fast. Storage cost is trivial
-- (column is a 2-3 char string with extremely low cardinality).
--
-- IF NOT EXISTS guards the index so this migration is safe to re-run.

ALTER TABLE headlines ADD COLUMN language TEXT;

CREATE INDEX IF NOT EXISTS idx_headlines_language ON headlines(language);
