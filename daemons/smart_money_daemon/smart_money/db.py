"""SQLite cache layer, schema v0 amended per ORDER SM-1.

Deviation from original SM-0/1 congress_trades schema, flagged in the order
report: owner, asset_name, asset_type, comment, filing_id columns added —
required by the SM-1 clause that non-stock assets are ingested but
asset_type-tagged and by filing-ID resume safety.
"""
import pathlib
import sqlite3

DB_PATH_DEFAULT = "data/cache/smart_money_v0.db"

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
CREATE INDEX IF NOT EXISTS idx_trades_person ON congress_trades(person_id);
CREATE INDEX IF NOT EXISTS idx_trades_ticker ON congress_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_spans_ticker ON price_spans(ticker);
"""


def connect(db_path: str = DB_PATH_DEFAULT) -> sqlite3.Connection:
    p = pathlib.Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(p))
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    _migrate(con)
    return con


def _migrate(con):
    """Idempotent column adds for DBs created before a schema bump. CREATE TABLE
    IF NOT EXISTS never alters an existing table, so new columns land here."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(congress_trades)")}
    if "superseded" not in cols:
        con.execute(
            "ALTER TABLE congress_trades ADD COLUMN superseded INTEGER NOT NULL DEFAULT 0"
        )
        con.commit()
