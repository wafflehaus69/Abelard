-- News Watch Daemon — initial schema (v1).
--
-- Applied by `news-watch-daemon db init`. Tracked in the schema_version
-- table so future migrations can apply incrementally without re-running
-- this file. Foreign keys are enforced via `PRAGMA foreign_keys=ON` set
-- on every connection (SQLite default is OFF); WAL mode is set via
-- `PRAGMA journal_mode=WAL` for concurrent reads during scrape writes.
--
-- Conventions (carried over from Research Daemon):
--   - All `_at_unix` + `_at` timestamps are paired (Unix seconds + ISO-8601 UTC).
--   - JSON columns are validated on write, parsed on read; helpers live in db.py.
--   - CHECK constraints make invalid states unrepresentable.

-- ---- migration tracking ------------------------------------------------

CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_unix INTEGER NOT NULL,
    applied_at TEXT NOT NULL,
    description TEXT NOT NULL
);

-- ---- themes ------------------------------------------------------------

-- Themes are loaded from YAML; this table is a registry/cache for runtime.
CREATE TABLE themes (
    theme_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'archived')),
    config_hash TEXT NOT NULL,        -- SHA256 of canonical Pydantic dump
    loaded_at_unix INTEGER NOT NULL,
    loaded_at TEXT NOT NULL
);

-- ---- headlines ---------------------------------------------------------

CREATE TABLE headlines (
    headline_id TEXT PRIMARY KEY,     -- hash of (normalized_headline + source)
    source TEXT NOT NULL,             -- e.g. 'finnhub' | 'telegram:<channel>' | 'rss:<feed_id>'
    raw_source TEXT,                  -- original publisher if syndicated (e.g. 'Reuters')
    headline TEXT NOT NULL,
    url TEXT,
    published_at_unix INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    fetched_at_unix INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    dedupe_hash TEXT NOT NULL,        -- first 80 chars normalized
    tickers_json TEXT,                -- JSON array of ticker symbols pre-tagged by source
    entities_json TEXT                -- JSON object: {companies: [], countries: [], commodities: [], people: []}
);

CREATE INDEX idx_headlines_dedupe ON headlines(dedupe_hash);
CREATE INDEX idx_headlines_published ON headlines(published_at_unix);
CREATE INDEX idx_headlines_fetched ON headlines(fetched_at_unix);

-- ---- many-to-many: headline <-> theme tags -----------------------------

CREATE TABLE headline_theme_tags (
    headline_id TEXT NOT NULL,
    theme_id TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK (confidence IN ('primary', 'secondary')),
    tagged_at_unix INTEGER NOT NULL,
    PRIMARY KEY (headline_id, theme_id),
    FOREIGN KEY (headline_id) REFERENCES headlines(headline_id),
    FOREIGN KEY (theme_id) REFERENCES themes(theme_id)
);

CREATE INDEX idx_tags_theme ON headline_theme_tags(theme_id, tagged_at_unix);

-- ---- narratives (versioned synthesis output) ---------------------------

CREATE TABLE narratives (
    narrative_id INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_id TEXT NOT NULL,
    version INTEGER NOT NULL,         -- monotonic per theme_id, starts at 1
    synthesized_at_unix INTEGER NOT NULL,
    synthesized_at TEXT NOT NULL,
    headlines_considered_count INTEGER NOT NULL,
    headlines_window_start_unix INTEGER NOT NULL,
    headlines_window_end_unix INTEGER NOT NULL,

    -- Synthesis output (LLM-produced)
    thesis TEXT NOT NULL,
    evidence_json TEXT NOT NULL,      -- JSON array of {headline_id, why_relevant}
    velocity TEXT NOT NULL CHECK (velocity IN ('accelerating', 'plateauing', 'fading', 'unclear')),
    counter_evidence_json TEXT,       -- JSON array (may be empty / null)
    notable_entities_json TEXT,       -- JSON object of which entities were most active

    -- Continuity from prior version
    prior_version INTEGER,            -- nullable for v1
    shift_from_prior TEXT,            -- prose summary of what changed; null for v1

    -- Synthesis metadata
    model_used TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,

    FOREIGN KEY (theme_id) REFERENCES themes(theme_id),
    UNIQUE (theme_id, version)
);

CREATE INDEX idx_narratives_theme_version ON narratives(theme_id, version DESC);
CREATE INDEX idx_narratives_synthesized ON narratives(synthesized_at_unix);

-- ---- alerts ------------------------------------------------------------

CREATE TABLE alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_id TEXT NOT NULL,
    alert_type TEXT NOT NULL CHECK (alert_type IN (
        'narrative_shift', 'velocity_spike', 'counter_evidence', 'new_theme_suggested'
    )),
    triggered_at_unix INTEGER NOT NULL,
    triggered_at TEXT NOT NULL,
    narrative_id INTEGER,             -- nullable; links to narrative that triggered if applicable
    message TEXT NOT NULL,
    handoff_hint_json TEXT,           -- JSON: suggested Research Daemon commands for Abelard
    sent_channels_json TEXT NOT NULL, -- JSON array of channels successfully sent to
    FOREIGN KEY (theme_id) REFERENCES themes(theme_id),
    FOREIGN KEY (narrative_id) REFERENCES narratives(narrative_id)
);

CREATE INDEX idx_alerts_theme_time ON alerts(theme_id, triggered_at_unix DESC);

-- ---- source-level health tracking --------------------------------------

CREATE TABLE source_health (
    source TEXT PRIMARY KEY,
    last_successful_fetch_unix INTEGER,
    last_successful_fetch TEXT,
    last_attempt_unix INTEGER NOT NULL,
    last_attempt TEXT NOT NULL,
    last_status TEXT NOT NULL CHECK (last_status IN ('ok', 'rate_limited', 'error', 'partial')),
    last_error_detail TEXT,
    consecutive_failure_count INTEGER NOT NULL DEFAULT 0
);

-- ---- daemon-level heartbeat -------------------------------------------

CREATE TABLE daemon_heartbeat (
    component TEXT PRIMARY KEY,       -- 'scrape' | 'synthesize' | 'alert_check'
    last_run_unix INTEGER NOT NULL,
    last_run TEXT NOT NULL,
    last_status TEXT NOT NULL,
    last_duration_ms INTEGER,
    last_error_detail TEXT
);
