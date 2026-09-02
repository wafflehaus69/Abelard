"""PS-1 Phase 1 — the price substrate schema, as one versioned migration.

Everything the store enforces is enforced HERE, in DDL, not in the writer:

  * ``prices_raw`` is insert-only. UPDATE and DELETE are blocked by trigger, so
    a caller cannot quietly restate a fact — not even via ``INSERT OR REPLACE``,
    which SQLite implements as DELETE + INSERT and which the delete trigger
    therefore also blocks. A colliding insert raises ``IntegrityError`` on the
    UNIQUE constraint; the writer's job is to compare and fail loud, and the
    schema's job is to make sure it cannot skip that step.
  * ``index_membership``, ``classification`` and ``corporate_actions`` are
    as-of append-only, by the same mechanism. Membership history is a fact:
    a name leaving an index is a new row with ``present=0``, never a deletion.
  * ``adjustment_factors`` versions are immutable once written. A re-version
    writes a new ``version``; the old one stays so any statistic computed under
    it stays reproducible.

Derived tables (``adjusted_view``) are deliberately NOT protected — they are
rebuildable by definition, and rebuilding one for a name whose factor version
changed is the normal path.

Schema version is stamped in ``price_meta`` at creation and checked by
``migrate()`` on every open. There is one migration and it is version 1; when a
version 2 arrives it appends here rather than editing version 1 in place.

Ruling 3 (Mando, 2026-09-02) and the ``alert_queue`` precedent: ``connect()``
takes an explicit path. No env resolution in this library.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ..errors import DaemonError

SCHEMA_VERSION = 2

# A price row's honesty flag. 'ok' is a fact; the other two are records of the
# vendor failing, kept as rows so a gap is visible rather than inferred from
# absence (A5 — a silently dropped null makes freshness lie).
PRICE_STATUSES = ("ok", "vendor_null", "quarantined")

# Vendor-DECLARED corporate actions, from the chart endpoint's events block.
CA_KINDS = ("split", "dividend")

# INFERRED events — a ratio move the declared feed does not explain. This is the
# alarm channel, not the primary path (Amendment A1/A2 item 3).
INFERRED_KINDS = ("split_inferred", "dividend_inferred", "vendor_corruption", "unknown")

INDEX_CODES = ("SPX", "NDX", "RUT")


class PriceStoreError(DaemonError):
    """Storage-contract violation. Rooted at DaemonError so an orchestrator can
    fold ``to_error()`` into its errors array without fabricating data."""

    def __init__(self, message: str, *, stage: str = "price_store") -> None:
        super().__init__(message, stage=stage)


_SCHEMA = """
-- ---------------------------------------------------------------- identity --
-- instrument_id is <cik10>.<class>, never CIK alone: dual-class securities
-- share one CIK (GOOG/GOOGL, FOX/FOXA, NWS/NWSA, BRK-A/BRK-B), and keying on
-- CIK would collide two distinct price series into one UNIQUE(instrument_id,
-- date) slot -- raising a fact-change event every single night.
-- class_source records HOW the discriminator was obtained, so the ordinal
-- fallback is never mistaken for a vendor-supplied class.
CREATE TABLE IF NOT EXISTS instruments (
    instrument_id  TEXT PRIMARY KEY,
    cik            TEXT,
    class_code     TEXT NOT NULL DEFAULT '0',
    class_source   TEXT NOT NULL DEFAULT 'single'
        CHECK (class_source IN ('wikipedia','sec','ordinal','single')),
    name           TEXT,
    primary_ticker TEXT,
    source         TEXT NOT NULL,
    provisional    INTEGER NOT NULL DEFAULT 0 CHECK (provisional IN (0,1)),
    first_seen     TEXT NOT NULL,
    last_seen      TEXT NOT NULL
);

-- Four ticker notations are in play (PS-1-P0 P0.4(v)): concatenated (BRKB),
-- dotted (BRK.B), dashed (BRK-B) and the vendor's. Resolution is by lookup,
-- never by string surgery -- CMCSA and GOOGL are genuine 5-letter tickers.
-- valid_to NULL means current.
CREATE TABLE IF NOT EXISTS ticker_aliases (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    ticker        TEXT NOT NULL,
    notation      TEXT NOT NULL
        CHECK (notation IN ('concat','dot','dash','vendor')),
    valid_from    TEXT NOT NULL,
    valid_to      TEXT,
    source        TEXT NOT NULL,
    PRIMARY KEY (instrument_id, ticker, notation, valid_from)
);

-- ------------------------------------------------------------- membership --
-- As-of, never overwritten. A name leaving an index is a new row with
-- present=0, so "who was in the S&P on date D" stays answerable forever.
CREATE TABLE IF NOT EXISTS index_membership (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    index_code    TEXT NOT NULL CHECK (index_code IN ('SPX','NDX','RUT')),
    as_of         TEXT NOT NULL,
    present       INTEGER NOT NULL CHECK (present IN (0,1)),
    source        TEXT NOT NULL,
    PRIMARY KEY (instrument_id, index_code, as_of, source)
);

-- Append-only. taxonomy is GICS; NDX-only names carry NO row rather than a
-- hand-mapped ICB guess (A6). Two sources may disagree (Wikipedia vs IVV) --
-- both rows are kept and the disagreement is logged, not resolved here.
CREATE TABLE IF NOT EXISTS classification (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    taxonomy      TEXT NOT NULL DEFAULT 'GICS',
    sector        TEXT,
    sub_industry  TEXT,
    as_of         TEXT NOT NULL,
    source        TEXT NOT NULL,
    PRIMARY KEY (instrument_id, taxonomy, source, as_of)
);

-- ------------------------------------------------------------------ facts --
-- Reconstructed TRUE traded prices:
--     raw(d) = close(d) * PROD{ split_ratio(e) : e effective after d }
-- with O/H/L on the same factor and volume on its inverse (verified against
-- AAPL's clean 4:1 of 2020-08-31: Yahoo does split-adjust volume).
-- INSERT ONLY -- see the triggers below.
-- run_asof is one timestamp for a whole nightly run (window-alignment doctrine).
CREATE TABLE IF NOT EXISTS prices_raw (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    date          TEXT NOT NULL,
    open          REAL,
    high          REAL,
    low           REAL,
    close         REAL,
    volume        INTEGER,
    status        TEXT NOT NULL DEFAULT 'ok'
        CHECK (status IN ('ok','vendor_null','quarantined')),
    source        TEXT NOT NULL,
    fetched_at    INTEGER NOT NULL,
    run_asof      INTEGER NOT NULL,
    UNIQUE (instrument_id, date)
);

-- The vendor's DECLARED corporate actions. Primary detector: the chart
-- endpoint returns these in the same request as prices, at one-day granularity,
-- so a split is caught on its effective date for free.
-- ratio is set for kind='split' (2.0 for a 2:1); amount for kind='dividend'.
CREATE TABLE IF NOT EXISTS corporate_actions (
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    effective_date TEXT NOT NULL,
    kind           TEXT NOT NULL CHECK (kind IN ('split','dividend')),
    ratio          REAL,
    amount         REAL,
    declared_at    INTEGER NOT NULL,
    source         TEXT NOT NULL,
    run_asof       INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, effective_date, kind, source),
    CHECK ((kind = 'split'    AND ratio  IS NOT NULL AND ratio  > 0)
        OR (kind = 'dividend' AND amount IS NOT NULL))
);

-- INFERRED events -- the alarm channel. A ratio move the declared feed does not
-- explain, or a session whose price is internally inconsistent with the
-- declared split (the MNST signature: 6 of 21 pre-split sessions halved, 15
-- not). evidence is JSON carrying both series' values at the boundary.
CREATE TABLE IF NOT EXISTS adjustment_events (
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    effective_date TEXT NOT NULL,
    implied_ratio  REAL,
    kind           TEXT NOT NULL
        CHECK (kind IN ('split_inferred','dividend_inferred',
                        'vendor_corruption','unknown')),
    detected_at    INTEGER NOT NULL,
    evidence       TEXT,
    version        INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, effective_date, kind, version)
);

-- The cumulative factor series, versioned. A new declaration bumps the version
-- for that name; old versions are NEVER deleted, so a statistic published under
-- version N stays reproducible after version N+1 lands.
CREATE TABLE IF NOT EXISTS adjustment_factors (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    date          TEXT NOT NULL,
    factor        REAL NOT NULL,
    version       INTEGER NOT NULL,
    computed_at   INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, date, version)
);

-- Materialised current view. DERIVED and rebuildable -- deliberately NOT
-- protected by triggers, because rebuilding a name whose factor version changed
-- is the normal path. This is the only table analytics reads.
CREATE TABLE IF NOT EXISTS adjusted_view (
    instrument_id  TEXT NOT NULL REFERENCES instruments(instrument_id),
    date           TEXT NOT NULL,
    adj_close      REAL,
    factor_version INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, date)
);

-- The vendor's own adjusted close, for COMPARISON ONLY, never analytics.
-- A separate table rather than a prices_raw column so it cannot be joined into
-- a return calculation by accident.
CREATE TABLE IF NOT EXISTS vendor_adjusted (
    instrument_id   TEXT NOT NULL REFERENCES instruments(instrument_id),
    date            TEXT NOT NULL,
    vendor_adjclose REAL,
    source          TEXT NOT NULL,
    fetched_at      INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, date)
);

-- ------------------------------------------------------------- operations --
-- The G1 / staleness ledger. last_date_held is set from the max date in the
-- vendor's RETURNED rows, never from the requested span, and advances only on
-- a non-null close (A5). This is the table SM will read as a precondition in
-- Phase 4 instead of fetching for itself.
CREATE TABLE IF NOT EXISTS freshness (
    instrument_id        TEXT PRIMARY KEY REFERENCES instruments(instrument_id),
    last_date_held       TEXT,
    last_fetch_at        INTEGER,
    last_fetch_status    TEXT,
    last_full_refetch_at INTEGER
);

-- VIXCLS / DCOILWTICO / SPY / CL=F / ^VIX ...
-- contract and roll_flag carry the WTI front-month identity from the chart
-- meta's shortName ("Crude Oil Oct 26"). Nullable, so this schema serves either
-- reference-series ruling without a migration.
CREATE TABLE IF NOT EXISTS reference_series (
    series_id  TEXT NOT NULL,
    date       TEXT NOT NULL,
    value      REAL,
    contract   TEXT,
    roll_flag  INTEGER NOT NULL DEFAULT 0 CHECK (roll_flag IN (0,1)),
    status     TEXT NOT NULL DEFAULT 'ok'
        CHECK (status IN ('ok','vendor_null','quarantined')),
    source     TEXT NOT NULL,
    fetched_at INTEGER NOT NULL,
    PRIMARY KEY (series_id, date, source)
);

CREATE TABLE IF NOT EXISTS price_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per nightly run. Written BEFORE the run's data commits, per the
-- cost/telemetry-before-persistence rule.
CREATE TABLE IF NOT EXISTS run_telemetry (
    run_asof          INTEGER PRIMARY KEY,
    started_at        INTEGER NOT NULL,
    finished_at       INTEGER,
    requests_made     INTEGER NOT NULL DEFAULT 0,
    rows_returned     INTEGER NOT NULL DEFAULT 0,
    rows_inserted     INTEGER NOT NULL DEFAULT 0,
    names_refetched   INTEGER NOT NULL DEFAULT 0,
    actions_detected  INTEGER NOT NULL DEFAULT 0,
    quarantined       INTEGER NOT NULL DEFAULT 0,
    http_429          INTEGER NOT NULL DEFAULT 0,
    status            TEXT
);

-- ------------------------------------------------------- v2: corrections --
-- Insert-only plus a vendor that legitimately corrects a bad print equals a
-- perpetual fail-loud with no way out. This is the exit, and it is a HUMAN one:
-- rows are Mando-authored, carry a reason, and never arrive from a fetch.
--
-- The fact stays. prices_raw is untouched forever; a correction is an OVERLAY
-- that the adjusted view honours and the fact-change comparison consults. So
-- "what did the vendor originally tell us" and "what do we believe" are both
-- answerable, which is the whole point of keeping them apart.
CREATE TABLE IF NOT EXISTS corrections (
    instrument_id   TEXT NOT NULL REFERENCES instruments(instrument_id),
    date            TEXT NOT NULL,
    corrected_close REAL,
    supersedes      REAL,
    reason          TEXT NOT NULL,
    authored_by     TEXT NOT NULL,
    authored_at     INTEGER NOT NULL,
    evidence        TEXT,
    PRIMARY KEY (instrument_id, date, authored_at)
);

-- ---------------------------------------------------- v2: index weights --
-- From the iShares holdings file, which carries Weight (%) already. Feeds the
-- index-level reconciliation: rebuild the cap-weighted index return from the
-- constituents and compare against the ETF's own. That is the only check that
-- sees a SYSTEMIC vendor failure -- many names stale or mangled at once --
-- which every per-name detector is blind to by construction.
CREATE TABLE IF NOT EXISTS index_weights (
    instrument_id TEXT NOT NULL REFERENCES instruments(instrument_id),
    index_code    TEXT NOT NULL,
    as_of         TEXT NOT NULL,
    weight        REAL NOT NULL,
    source        TEXT NOT NULL,
    PRIMARY KEY (instrument_id, index_code, as_of, source)
);

-- --------------------------------------------- v2: reconciliation results --
CREATE TABLE IF NOT EXISTS reconciliation (
    as_of          TEXT NOT NULL,
    index_code     TEXT NOT NULL,
    benchmark      TEXT NOT NULL,
    rebuilt_return REAL,
    actual_return  REAL,
    diff_bp        REAL,
    members_used   INTEGER,
    members_missing INTEGER,
    tolerance_bp   REAL,
    passed         INTEGER NOT NULL CHECK (passed IN (0,1)),
    detail         TEXT,
    run_asof       INTEGER NOT NULL,
    PRIMARY KEY (as_of, index_code, benchmark, run_asof)
);

-- ----------------------------------------------------------------- indexes --
CREATE INDEX IF NOT EXISTS idx_prices_raw_date        ON prices_raw (date);
CREATE INDEX IF NOT EXISTS idx_adjusted_view_date     ON adjusted_view (date);
CREATE INDEX IF NOT EXISTS idx_aliases_ticker         ON ticker_aliases (ticker);
CREATE INDEX IF NOT EXISTS idx_membership_index_asof  ON index_membership (index_code, as_of);
CREATE INDEX IF NOT EXISTS idx_freshness_lastdate     ON freshness (last_date_held);
CREATE INDEX IF NOT EXISTS idx_ca_effective           ON corporate_actions (effective_date);
CREATE INDEX IF NOT EXISTS idx_corrections_instrument ON corrections (instrument_id, date);
CREATE INDEX IF NOT EXISTS idx_weights_index_asof     ON index_weights (index_code, as_of);

-- ---------------------------------------------------------------- triggers --
-- Insert-only enforcement. DELETE is blocked as well as UPDATE because SQLite
-- implements INSERT OR REPLACE as DELETE + INSERT: without the delete trigger,
-- an upsert would walk straight through the update trigger.
-- Blocks INSERT OR REPLACE regardless of how the connection was opened.
-- The delete trigger below is NOT enough on its own: SQLite fires delete
-- triggers during REPLACE conflict resolution only when recursive_triggers is
-- ON, which is a per-connection pragma. A caller opening the file with a bare
-- sqlite3.connect() would otherwise walk straight through and restate a fact --
-- which is precisely the upsert path this substrate exists to remove.
-- A BEFORE INSERT trigger fires on the insert itself, so the guarantee holds
-- for every connection.
CREATE TRIGGER IF NOT EXISTS trg_prices_raw_no_replace
BEFORE INSERT ON prices_raw
WHEN EXISTS (SELECT 1 FROM prices_raw
             WHERE instrument_id = NEW.instrument_id AND date = NEW.date)
BEGIN
    SELECT RAISE(ABORT, 'prices_raw is insert-only: a row already exists for this (instrument_id, date); a vendor disagreement is a fail-loud fact-change event, never an update');
END;

CREATE TRIGGER IF NOT EXISTS trg_prices_raw_no_update
BEFORE UPDATE ON prices_raw BEGIN
    SELECT RAISE(ABORT, 'prices_raw is insert-only: a recorded close is a fact; a vendor disagreement is a fail-loud fact-change event, never an update');
END;

CREATE TRIGGER IF NOT EXISTS trg_prices_raw_no_delete
BEFORE DELETE ON prices_raw BEGIN
    SELECT RAISE(ABORT, 'prices_raw is insert-only: rows are never deleted; this also blocks INSERT OR REPLACE, which SQLite implements as DELETE + INSERT');
END;

CREATE TRIGGER IF NOT EXISTS trg_membership_no_update
BEFORE UPDATE ON index_membership BEGIN
    SELECT RAISE(ABORT, 'index_membership is as-of append-only: a name leaving an index is a new row with present=0, never an edit');
END;

CREATE TRIGGER IF NOT EXISTS trg_membership_no_delete
BEFORE DELETE ON index_membership BEGIN
    SELECT RAISE(ABORT, 'index_membership is as-of append-only: no deletes.');
END;

CREATE TRIGGER IF NOT EXISTS trg_classification_no_update
BEFORE UPDATE ON classification BEGIN
    SELECT RAISE(ABORT, 'classification is as-of append-only: a reclassification is a new as-of row, never an edit');
END;

CREATE TRIGGER IF NOT EXISTS trg_classification_no_delete
BEFORE DELETE ON classification BEGIN
    SELECT RAISE(ABORT, 'classification is as-of append-only: no deletes.');
END;

CREATE TRIGGER IF NOT EXISTS trg_corporate_actions_no_update
BEFORE UPDATE ON corporate_actions BEGIN
    SELECT RAISE(ABORT, 'corporate_actions is append-only: a revised declaration is a new row with its own declared_at');
END;

CREATE TRIGGER IF NOT EXISTS trg_corporate_actions_no_delete
BEFORE DELETE ON corporate_actions BEGIN
    SELECT RAISE(ABORT, 'corporate_actions is append-only: no deletes.');
END;

CREATE TRIGGER IF NOT EXISTS trg_factors_no_update
BEFORE UPDATE ON adjustment_factors BEGIN
    SELECT RAISE(ABORT, 'adjustment_factors versions are immutable: write a new version, never edit an old one; past statistics must stay reproducible');
END;

CREATE TRIGGER IF NOT EXISTS trg_factors_no_delete
BEFORE DELETE ON adjustment_factors BEGIN
    SELECT RAISE(ABORT, 'adjustment_factors versions are never deleted; past statistics must stay reproducible');
END;

CREATE TRIGGER IF NOT EXISTS trg_corrections_no_update
BEFORE UPDATE ON corrections BEGIN
    SELECT RAISE(ABORT, 'corrections is append-only: a revised correction is a new row with its own authored_at');
END;

CREATE TRIGGER IF NOT EXISTS trg_corrections_no_delete
BEFORE DELETE ON corrections BEGIN
    SELECT RAISE(ABORT, 'corrections is append-only: no deletes, so the correction history stays auditable');
END;

CREATE TRIGGER IF NOT EXISTS trg_adjustment_events_no_update
BEFORE UPDATE ON adjustment_events BEGIN
    SELECT RAISE(ABORT, 'adjustment_events is append-only.');
END;

CREATE TRIGGER IF NOT EXISTS trg_adjustment_events_no_delete
BEFORE DELETE ON adjustment_events BEGIN
    SELECT RAISE(ABORT, 'adjustment_events is append-only.');
END;
"""


def connect(db_path: Path | str) -> sqlite3.Connection:
    """Open the price store at ``db_path`` and bring it to ``SCHEMA_VERSION``.

    Explicit path, no env resolution -- ``alert_queue``'s discipline and Mando's
    ruling 3. The Phase 2 CLI reads ``ABELARD_PRICES_DB_PATH``; this library
    never does.

    WAL so a reader (a dashboard, SM's freshness precondition) can read while
    the nightly writer runs. ``foreign_keys=ON`` so an orphan price row for an
    unknown instrument fails at insert rather than surfacing as a silent gap.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=10.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA foreign_keys=ON")
    # Defence in depth for INSERT OR REPLACE: with recursive triggers ON the
    # delete trigger also fires during REPLACE conflict resolution. The
    # trg_prices_raw_no_replace BEFORE INSERT trigger already covers this for
    # any connection; this pragma covers the same ground for callers that reach
    # the other protected tables by that route.
    con.execute("PRAGMA recursive_triggers=ON")
    migrate(con)
    return con


def migrate(con: sqlite3.Connection) -> int:
    """Create or verify the schema. Returns the version now on disk.

    Idempotent: every statement is IF NOT EXISTS, so re-opening an existing
    store is a no-op. A store stamped NEWER than this code raises rather than
    running against a shape it does not understand -- an old binary quietly
    writing into a new schema is how a corpus gets corrupted.
    """
    found = schema_version(con)
    if found is not None and found > SCHEMA_VERSION:
        raise PriceStoreError(
            "price store is at schema version {} but this code only knows {}; "
            "refusing to run against a newer shape".format(found, SCHEMA_VERSION),
            stage="migrate",
        )
    with con:
        con.executescript(_SCHEMA)
        con.execute(
            "INSERT OR IGNORE INTO price_meta (key, value) VALUES "
            "('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
    return SCHEMA_VERSION


def schema_version(con: sqlite3.Connection) -> int | None:
    """The version stamped on disk, or None for a store not yet created."""
    try:
        row = con.execute(
            "SELECT value FROM price_meta WHERE key='schema_version'"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return int(row[0]) if row else None
