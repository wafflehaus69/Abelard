"""SQLite state for the Capex Daemon. Schema v0.

Every stored fact and series row carries its own provenance: which leg produced
it, which concept was resolved, what unit basis was established, and which
accession it came from. A row that cannot state those is not publishable
(E1, E7 provenance rule).
"""
import os
import sqlite3

from . import config

SCHEMA = """
-- Entity registry. CIK is the durable key; display name is a snapshot for
-- across-scan comparison, never an identity (E10).
CREATE TABLE IF NOT EXISTS entities(
    cik TEXT PRIMARY KEY,
    ticker_display TEXT,
    name_current TEXT,
    bucket TEXT,
    sic TEXT,
    fiscal_year_end TEXT,
    entity_type TEXT,
    last_resolved_unix INTEGER
);

-- Rename / identity discontinuity markers. Written, never applied to history.
CREATE TABLE IF NOT EXISTS identity_events(
    cik TEXT NOT NULL,
    observed_unix INTEGER NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    PRIMARY KEY (cik, observed_unix, field)
);

-- Raw XBRL facts. One row per (concept, period, unit, dimension-set, source).
-- Dimensioned facts are first-class: dim_key is the canonical serialization of
-- the axis->member set, empty string for the undimensioned/consolidated fact.
-- The 2025-12-31 Meta case must store BOTH the dimensioned 45.95B and the
-- undimensioned 5.58B as distinct rows (E6).
CREATE TABLE IF NOT EXISTS facts(
    cik TEXT NOT NULL,
    taxonomy TEXT NOT NULL,
    concept TEXT NOT NULL,
    period_start TEXT,
    period_end TEXT NOT NULL,
    duration_days INTEGER,
    unit TEXT NOT NULL,
    value REAL NOT NULL,
    scale INTEGER,
    scale_basis TEXT NOT NULL,
    dim_key TEXT NOT NULL DEFAULT '',
    dims_json TEXT,
    source_leg TEXT NOT NULL,
    accession TEXT,
    filed TEXT,
    form TEXT,
    context_ref TEXT,
    PRIMARY KEY (cik, taxonomy, concept, period_start, period_end, unit, dim_key, source_leg)
);
CREATE INDEX IF NOT EXISTS idx_facts_lookup ON facts(cik, concept, period_end);

-- Resolved tag map. One row per (cik, series_kind, era). Recency-resolved (E7).
CREATE TABLE IF NOT EXISTS tag_map(
    cik TEXT NOT NULL,
    series_kind TEXT NOT NULL,
    concept TEXT NOT NULL,
    era_start TEXT,
    era_end TEXT,
    latest_observation TEXT,
    fact_count INTEGER,
    resolved_unix INTEGER,
    PRIMARY KEY (cik, series_kind, era_start)
);

-- Normalized discrete-quarter series. Provenance is mandatory on every row.
CREATE TABLE IF NOT EXISTS series(
    cik TEXT NOT NULL,
    series_kind TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    calendar_quarter TEXT,
    calendar_offset_days INTEGER,
    value REAL NOT NULL,
    unit TEXT NOT NULL,
    scale_basis TEXT NOT NULL,
    resolved_concept TEXT NOT NULL,
    derivation TEXT NOT NULL,
    source_leg TEXT NOT NULL,
    accession TEXT,
    PRIMARY KEY (cik, series_kind, period_start, period_end)
);

-- Coverage: tier, consecutive-quarter count, and per-series coverage status.
CREATE TABLE IF NOT EXISTS coverage(
    cik TEXT NOT NULL,
    series_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    consecutive_quarters INTEGER,
    tier TEXT,
    tier_reason TEXT,
    detail TEXT,
    computed_unix INTEGER,
    PRIMARY KEY (cik, series_kind)
);

-- CD-G3 anchor reconciliation outcomes. Reported, never corrected.
CREATE TABLE IF NOT EXISTS anchor_checks(
    cik TEXT NOT NULL,
    window_end TEXT NOT NULL,
    deployed REAL,
    delta_anchor REAL,
    ratio REAL,
    verdict TEXT NOT NULL,
    anchor_concept TEXT,
    detail TEXT,
    PRIMARY KEY (cik, window_end)
);

-- Watermarks advance only on success-with-items (E12).
CREATE TABLE IF NOT EXISTS watermarks(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_unix INTEGER
);

CREATE TABLE IF NOT EXISTS meta_kv(
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def connect(path=None):
    path = os.path.expanduser(path or config.DB_PATH_DEFAULT)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(SCHEMA)
    con.commit()
    return con
