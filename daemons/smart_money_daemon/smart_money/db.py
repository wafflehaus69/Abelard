"""SQLite cache layer, schema v0 amended per ORDER SM-1.

Deviation from original SM-0/1 congress_trades schema, flagged in the order
report: owner, asset_name, asset_type, comment, filing_id columns added —
required by the SM-1 clause that non-stock assets are ingested but
asset_type-tagged and by filing-ID resume safety.
"""
import os
import pathlib
import sqlite3

# SM-4 state home. Canonical DB lives under ~/.openclaw/smart_money/. Path from
# SMART_MONEY_DB_PATH (env, or the daemon .env), with the new home as default.
# One canonical home — no dual-read fallback.
STATE_HOME = os.path.expanduser("~/.openclaw/smart_money")
_DEFAULT = os.path.join(STATE_HOME, "smart_money_v0.db")


def _load_env_var(key):
    v = os.environ.get(key)
    if v:
        return v
    for envp in (".env", os.path.join(os.path.dirname(__file__), "..", ".env")):
        p = pathlib.Path(envp)
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip()
    return None


def resolve_db_path():
    return os.path.expanduser(_load_env_var("SMART_MONEY_DB_PATH") or _DEFAULT)


DB_PATH_DEFAULT = resolve_db_path()
SCANS_DIR = os.path.join(STATE_HOME, "scans")
LOGS_DIR = os.path.join(STATE_HOME, "logs")

SCHEMA = """
CREATE TABLE IF NOT EXISTS persons(
  person_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL,
  cik_or_chamber TEXT,
  meta TEXT
);
CREATE TABLE IF NOT EXISTS congress_trades(
  trade_id INTEGER PRIMARY KEY,
  person_id INTEGER NOT NULL REFERENCES persons(person_id),
  ticker TEXT,
  side TEXT NOT NULL,
  amt_low INTEGER NOT NULL,
  amt_high INTEGER,
  tx_date TEXT NOT NULL,
  disclosure_date TEXT NOT NULL,
  lag_days INTEGER NOT NULL,
  chamber TEXT NOT NULL,
  source TEXT NOT NULL,
  raw_ref TEXT NOT NULL,
  owner TEXT,
  asset_name TEXT,
  asset_type TEXT,
  comment TEXT,
  filing_id TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0,
  UNIQUE(filing_id, raw_ref)
);
CREATE TABLE IF NOT EXISTS ingested_filings(
  filing_id TEXT PRIMARY KEY,
  chamber TEXT NOT NULL,
  status TEXT NOT NULL,
  person_name TEXT,
  report_label TEXT,
  filed_date TEXT,
  n_rows INTEGER,
  ingested_at_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS prices(
  ticker TEXT NOT NULL,
  date TEXT NOT NULL,
  close REAL,
  adj_close REAL,
  price_type TEXT NOT NULL,
  asof_unix INTEGER NOT NULL,
  fetched_at_unix INTEGER NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY(ticker, date, price_type)
);
CREATE TABLE IF NOT EXISTS price_spans(
  ticker TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  fetched_at_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta_kv(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ticker_status(
  ticker TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  last_trade_date TEXT,
  probed_at_unix INTEGER NOT NULL,
  heuristic TEXT
);
CREATE TABLE IF NOT EXISTS watermarks(
  source TEXT PRIMARY KEY,
  watermark_ts TEXT,
  updated_at_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS thirteenf_baseline(
  cik TEXT PRIMARY KEY,
  accession TEXT NOT NULL,
  period TEXT,
  filed_date TEXT,
  holdings_json TEXT NOT NULL,
  ingested_at_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS scan_events(
  event_id TEXT PRIMARY KEY,
  scan_id TEXT NOT NULL,
  leg TEXT NOT NULL,
  ticker TEXT,
  side TEXT,
  tx_date TEXT,
  disclosure_date TEXT,
  emitted_at_unix INTEGER NOT NULL
);
-- Persistent Form 4 transaction corpus (SM-A1 / SM-F4). The g1/g2 commonality
-- counters read this; the scan Leg B persists here (SM-F4 Step 1) and the
-- historical backfill (Step 2) fills it. Idempotent by (accession, tx_index).
CREATE INDEX IF NOT EXISTS idx_trades_person ON congress_trades(person_id);
CREATE INDEX IF NOT EXISTS idx_trades_ticker ON congress_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_spans_ticker ON price_spans(ticker);
"""

# Full Step-1 shape. Idempotent by (accession, tx_index). Kept separate so the
# migration can recreate it after dropping the empty SM-4b-shape table.
FORM4_DDL = """
CREATE TABLE IF NOT EXISTS form4_transactions(
  accession TEXT NOT NULL,
  tx_index INTEGER NOT NULL,
  reporting_person TEXT,
  reporting_cik TEXT,
  issuer TEXT,
  ticker TEXT,
  code TEXT,
  plan_flag INTEGER,
  shares REAL,
  price REAL,
  value REAL,
  ownership_after REAL,
  tx_date TEXT,
  filed_date TEXT,
  role TEXT,
  PRIMARY KEY(accession, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_f4_ticker ON form4_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_f4_cik ON form4_transactions(reporting_cik);
CREATE TABLE IF NOT EXISTS form4_backfill_seen(
  accession TEXT PRIMARY KEY,
  seen_at_unix INTEGER NOT NULL
);
"""


def connect(db_path: str = DB_PATH_DEFAULT) -> sqlite3.Connection:
    p = pathlib.Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(p))
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    _migrate_form4(con)
    con.executescript(FORM4_DDL)
    _migrate(con)
    return con


def _migrate_form4(con):
    """Recreate form4_transactions if the empty SM-4b-shape table (no tx_index)
    is present. Refuses to drop a non-empty table — a populated old-shape corpus
    means a real migration is needed, not a silent drop."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(form4_transactions)")}
    if cols and "tx_index" not in cols:
        n = con.execute("SELECT COUNT(*) FROM form4_transactions").fetchone()[0]
        if n:
            raise RuntimeError(
                "form4_transactions has old shape with {} rows; "
                "manual migration required".format(n))
        con.execute("DROP TABLE form4_transactions")
        con.commit()


def _migrate(con):
    """Idempotent column adds for DBs created before a schema bump. CREATE TABLE
    IF NOT EXISTS never alters an existing table, so new columns land here."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(congress_trades)")}
    if "superseded" not in cols:
        con.execute(
            "ALTER TABLE congress_trades ADD COLUMN superseded INTEGER NOT NULL DEFAULT 0"
        )
        con.commit()
