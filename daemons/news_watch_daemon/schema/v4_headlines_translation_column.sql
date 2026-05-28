-- Schema v4 — headline_en column on headlines for Pass F translation gating.
--
-- Pass F (2026-05-27). Adds a nullable TEXT column populated by the
-- Telegram-native translation pass (`messages.translateText` MTProto).
-- The Pass F translation gate finds candidates via:
--
--   SELECT ... FROM headlines
--   WHERE language != 'en' AND headline_en IS NULL
--   AND <other-filters>
--
-- Downstream consumers (theme tagger, Pass E ATTENTION counter) read
-- headline_en with COALESCE-style fallback to headline:
--
--   SELECT COALESCE(headline_en, headline) AS text_for_tagging ...
--
-- So English-content rows (where headline_en stays NULL by design)
-- naturally fall back to the original headline column, and translated
-- rows surface their English text to the tagger / counter / synthesis
-- prompt. Russian content currently invisible to those layers becomes
-- visible without any per-theme keyword expansion.
--
-- ALTER TABLE ADD COLUMN is metadata-only in SQLite — no table rewrite,
-- no locking issues on large tables. Existing rows get NULL; the
-- backfill subcommand (Pass F Commit 2) populates them.
--
-- Index motivation: the gate query above filters on (language,
-- headline_en) — a composite index covers both predicates in one seek.
-- Storage cost is trivial (sparse — most rows will have non-null
-- headline_en or be 'en' language). IF NOT EXISTS guards the index so
-- this migration is safe to re-run.

ALTER TABLE headlines ADD COLUMN headline_en TEXT;

CREATE INDEX IF NOT EXISTS idx_headlines_translation_pending
    ON headlines(language, headline_en);
