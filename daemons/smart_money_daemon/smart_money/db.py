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


def artifact_path(name, sub="analysis"):
    """Where a run WRITES an artifact — the state home, never the repo tree."""
    p = os.path.join(STATE_HOME, sub)
    os.makedirs(p, exist_ok=True)
    return os.path.join(p, name)


def find_artifact(name, sub="analysis"):
    """Where to READ an artifact — state home first, else the repo-relative copy
    (committed deliverables live in the repo)."""
    p = os.path.join(STATE_HOME, sub, name)
    return p if os.path.exists(p) else os.path.join(sub, name)


DB_PATH_DEFAULT = resolve_db_path()
SCANS_DIR = os.path.join(STATE_HOME, "scans")
LOGS_DIR = os.path.join(STATE_HOME, "logs")
# SM-A1-fix: report artifacts write to the state home, not the repo tree, so
# scheduled Basilic runs never dirty the working tree (which was blocking pulls).
# Committing a deliverable snapshot is then a deliberate copy from here.
ANALYSIS_DIR = os.path.join(STATE_HOME, "analysis")

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
  filing_status TEXT,
  clerk_line_id TEXT,
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
  issuer_cik TEXT,
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
  ingest_regime TEXT NOT NULL DEFAULT 'watchlist',
  value_flag TEXT,
  -- The filer's OWN words for what was bought ("Common Stock", "5.95% Preferred Stock",
  -- "Depositary Shares for Series B Preferred"). form4.py has always PARSED this for
  -- Table I and then dropped it at the INSERT, while writing it faithfully for
  -- derivatives. Without it a preferred purchase and a unit error are indistinguishable:
  -- GAM at $24.66 against a $64.05 common close is a 5.95% preferred, and BGDE at
  -- $1,000 against ~$7 is a Series D Convertible Preferred - both CORRECT and both
  -- previously flagged as suspect. One filing can even carry two securities under one
  -- ticker: TSM acc 0001046179-26-000461 holds ADS at $390 and local common at $67.97.
  security_title TEXT,
  PRIMARY KEY(accession, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_f4_ticker ON form4_transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_f4_cik ON form4_transactions(reporting_cik);
CREATE TABLE IF NOT EXISTS form4_backfill_seen(
  accession TEXT PRIMARY KEY,
  seen_at_unix INTEGER NOT NULL
);
-- SM-O1 P1: separate ledger for the bounded Table II backfill. The Table I
-- backfill already filled form4_backfill_seen for the scoped issuers, so the
-- derivative backfill needs its own seen-set to re-fetch those filings once (and
-- get two-run idempotence of its own).
CREATE TABLE IF NOT EXISTS form4_deriv_backfill_seen(
  accession TEXT PRIMARY KEY,
  seen_at_unix INTEGER NOT NULL
);
-- Per-day watermark for the SM-U1 universal daily-index walk. A day is marked
-- complete only after every Form 4 in its index is persisted; resume skips
-- completed days. Parse failures counted per day, never guessed.
CREATE TABLE IF NOT EXISTS form4_universal_days(
  day TEXT PRIMARY KEY,
  form4_count INTEGER,
  persisted INTEGER,
  parse_fail INTEGER,
  completed_at_unix INTEGER NOT NULL
);
-- Durable per-holding 13F table (SM-A1 Phase 2). thirteenf_baseline keeps only
-- the latest-quarter JSON blob per CIK for the scan's diff; this is the queryable
-- multi-quarter home for cross-manager analysis. One row per (filer, filing,
-- cusip, put/call bucket).
CREATE TABLE IF NOT EXISTS thirteenf_holdings(
  cik TEXT NOT NULL,
  accession TEXT NOT NULL,
  period TEXT,
  filed_date TEXT,
  cusip TEXT NOT NULL,
  ticker TEXT,
  issuer TEXT,
  put_call TEXT NOT NULL DEFAULT 'long',
  value INTEGER,
  shares INTEGER,
  ingested_at_unix INTEGER NOT NULL,
  -- Form 13F states no unit for VALUE. Since the 2023 amendments whole dollars are
  -- mandated, but filers still report in thousands and the filing says so nowhere:
  -- not on the cover page, not in the info table. Duquesne reports Natera as
  -- value=864923 against 3,186,306 shares, an implied $0.27 on a ~$271 stock.
  -- The scale is therefore RESOLVED at ingest and stored, so no reader has to
  -- re-derive it. Ten read sites existed and three remembered to; that divergence
  -- is the whole reason this column exists.
  -- PER FILING, never per filer: Duquesne filed thousands at 2022-09-30, whole
  -- dollars at 2022-12-31 (its first post-amendment filing), then reverted. A
  -- per-filer scale would silently mis-scale a whole quarter on backfill.
  value_scale INTEGER,
  -- <sshPrnamtType>: SH or PRN. PRN means `shares` holds DOLLARS OF PAR, not a
  -- share count -- true of 19 convertible-note rows whose par sums to ~1.42bn and
  -- would otherwise be added to real share counts.
  shares_type TEXT,
  -- <titleOfClass>: the filer's own class label, e.g. COM, CAP STK CL A, NOTE
  -- 1.500%, *W EXP, PFD. The only stated instrument discriminator in the filing.
  title_of_class TEXT,
  -- Correct-by-construction dollars. Readers select this and cannot forget to
  -- scale; `value` stays raw for audit. VIRTUAL so it costs no storage and needs
  -- no backfill of its own.
  value_usd INTEGER GENERATED ALWAYS AS (value * COALESCE(value_scale, 1)) VIRTUAL,
  -- What the line actually IS: common / option_call / option_put /
  -- convertible_note / convertible_preferred / warrant / unit / unresolved.
  -- The table previously had no instrument vocabulary at all beyond put_call, so
  -- 264 convertible rows worth $17.2bn counted as equity conviction, and a filer
  -- holding $899k of CORZ common beside $42.0m of CORZW warrants read as one
  -- equity stake. Derived by smart_money.instrument from stored evidence only —
  -- never from a ticker suffix, which is wrong in both directions.
  instrument_class TEXT,
  -- CUSIP characters 1-6 are the ISSUER; 7-8 the issue. So GOOGL 02079K305, GOOG
  -- 02079K107 and both GOOGL convertible series share 02079K, and Alphabet's real
  -- $4.49bn exposure across 4 ticker strings finally has a join key. Generated, so
  -- it is always correct and needs no backfill.
  issuer_id TEXT GENERATED ALWAYS AS (substr(cusip, 1, 6)) VIRTUAL,
  PRIMARY KEY(cik, accession, cusip, put_call)
);
-- NB: the index on issuer_id is created in _migrate, NOT here. This DDL runs via
-- executescript on EVERY connect, including against a pre-existing table where
-- CREATE TABLE IF NOT EXISTS is a no-op and the column does not exist yet — so an
-- index declared here fails with "no such column" before the migration can add it.
CREATE INDEX IF NOT EXISTS idx_13fh_cik ON thirteenf_holdings(cik, period);
CREATE INDEX IF NOT EXISTS idx_13fh_ticker ON thirteenf_holdings(ticker);
CREATE INDEX IF NOT EXISTS idx_13fh_cusip ON thirteenf_holdings(cusip);
-- One row per 13F filing: the cover page's own declared totals, plus the resolved
-- value scale and how it was decided. tableValueTotal is a filer-declared control
-- total for the whole info table -- an independent check that our parse is complete,
-- which nothing verified before.
CREATE TABLE IF NOT EXISTS thirteenf_filing_meta(
  cik TEXT NOT NULL,
  accession TEXT NOT NULL,
  period TEXT,
  filed_date TEXT,
  entry_total INTEGER,          -- cover page <tableEntryTotal>
  value_total INTEGER,          -- cover page <tableValueTotal>, raw units
  parsed_rows INTEGER,          -- what we actually ingested
  parsed_value INTEGER,         -- sum of parsed raw values
  value_scale INTEGER,          -- 1 or 1000
  scale_basis TEXT,             -- price_anchored | control_total | undetermined
  resolved_at_unix INTEGER,
  PRIMARY KEY(cik, accession)
);
-- CUSIP -> ticker cache (OpenFIGI). Unmapped stays NULL ticker; never dropped.
-- OpenFIGI returns EVERY listing of an instrument worldwide, unordered. Taking
-- data[0] blindly is why 457669307 (Insmed) stored as IM8N, a Frankfurt line, and
-- 88023U101 (Somnigroup) as TPD: both had a non-US record first. 13F covers
-- section 13(f) securities, which are US-exchange-traded, so a non-US pick is
-- definitionally a resolver error and must be visible as one.
CREATE TABLE IF NOT EXISTS cusip_ticker(
  cusip TEXT PRIMARY KEY,
  ticker TEXT,
  name TEXT,
  -- how the row was decided, and therefore whether it is worth re-resolving:
  --   openfigi_us       picked a US composite listing        (trusted)
  --   openfigi_foreign  no US listing offered, took data[0]  (suspect, retry)
  --   openfigi_miss     the API answered with no data        (real miss)
  --   openfigi_error    the call failed                      (NOT a miss, retry)
  -- The old code wrote a bare 'openfigi' with a NULL ticker for BOTH of the last
  -- two, so a transient network failure was durably indistinguishable from
  -- "no such instrument" and was never retried.
  mapped_via TEXT,
  mapped_at_unix INTEGER NOT NULL,
  exch_code TEXT,        -- the chosen record's exchCode; US = composite
  market_sector TEXT,    -- Equity / Corp / Muni ... Corp is how bond descriptors won
  security_type TEXT,    -- Common Stock / Warrant / Preference / ...
  ticker_raw TEXT        -- what data[0] would have given, kept for audit
);
CREATE TABLE IF NOT EXISTS thirteenf_filings_seen(
  cik TEXT NOT NULL,
  accession TEXT NOT NULL,
  seen_at_unix INTEGER NOT NULL,
  PRIMARY KEY(cik, accession)
);
-- SM-P1b congressional ANNUAL FD holdings (Schedule A: Assets). Band-valued holdings
-- snapshot (annual cadence), the level PTRs do not give. Unmapped assets kept with
-- ticker NULL, never dropped. value_hi NULL = open top band. One row per asset line.
CREATE TABLE IF NOT EXISTS congress_holdings(
  doc_id TEXT NOT NULL,
  chamber TEXT NOT NULL,
  filing_year INTEGER,
  -- SM-C3 Phase H. `filing_year` meant DIFFERENT things per chamber: the House FD
  -- cycle/zip year (coverage = year-1) but the Senate eFD title year (already the
  -- coverage year, "Annual Report for CY 2025" filed 2026). Fusion against PTR flows
  -- depends on knowing WHICH YEAR A POSITION DESCRIBES, so both are now explicit:
  --   coverage_year = the calendar year the disclosure COVERS
  --   filing_date   = ISO date the report was filed (annuals run up to ~18mo stale)
  -- `filing_year` and `period` are retained unchanged so nothing downstream breaks.
  coverage_year INTEGER,
  filing_date TEXT,
  period TEXT,
  member_last TEXT,
  member_first TEXT,
  state_dist TEXT,
  person_id INTEGER,
  row_idx INTEGER NOT NULL,
  asset_name TEXT,
  ticker TEXT,
  asset_type TEXT,
  owner TEXT,
  value_lo INTEGER,
  value_hi INTEGER,
  income_type TEXT,
  ingested_at_unix INTEGER NOT NULL,
  PRIMARY KEY(doc_id, row_idx)
);
CREATE INDEX IF NOT EXISTS idx_ch_ticker ON congress_holdings(ticker);
CREATE INDEX IF NOT EXISTS idx_ch_year ON congress_holdings(filing_year);
CREATE INDEX IF NOT EXISTS idx_ch_person ON congress_holdings(person_id);
CREATE INDEX IF NOT EXISTS idx_ch_doc ON congress_holdings(doc_id);
CREATE TABLE IF NOT EXISTS congress_fd_seen(
  doc_id TEXT PRIMARY KEY,
  chamber TEXT NOT NULL,
  status TEXT NOT NULL,
  seen_at_unix INTEGER NOT NULL
);
-- OGE Form 278e executive-branch disclosure holdings. UNLIKE every other source here,
-- this one carries a STATUTORY use restriction (Ethics in Government Act, 5 U.S.C. app.
-- Sec 105(c) — no commercial use, $11k civil penalty). `use_restriction` is NOT NULL so a
-- row cannot physically exist without its restriction tag attached, and the tag travels
-- into every view and export. Deliberately its OWN table that the scan/alert/enqueue path
-- does not read, so restricted data cannot leak into a signal product.
CREATE TABLE IF NOT EXISTS oge_holdings(
  doc_id TEXT NOT NULL,
  filer TEXT NOT NULL,
  report_type TEXT,
  filed_date TEXT,
  line_no TEXT NOT NULL,
  description TEXT,
  ticker TEXT,
  eif TEXT,
  value_lo INTEGER,
  value_hi INTEGER,
  income_type TEXT,
  income_lo INTEGER,
  income_hi INTEGER,
  use_restriction TEXT NOT NULL,
  source_url TEXT,
  ingested_at_unix INTEGER NOT NULL,
  PRIMARY KEY(doc_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_oge_filer ON oge_holdings(filer);
CREATE INDEX IF NOT EXISTS idx_oge_ticker ON oge_holdings(ticker);
-- SM-C2 P3: party/state for each distinct FD filer identity, resolved from the keyless
-- congress-legislators roster by smart_money/roster.py. party is NULL when the filer did
-- not resolve DETERMINISTICALLY (match_kind 'unmatched') -- never guessed. That bucket is
-- dominated by candidates who filed an FD but never served.
-- SM-C3 Phase W: eFD availability probe log. The Senate legs were degraded on the
-- BELIEF that eFD blocks scripted access; this turns that belief into data. One row per
-- probe, including failures (a failed probe is the datapoint). hour_local is stored at
-- write time so the window map buckets by the operator's clock, not UTC.
CREATE TABLE IF NOT EXISTS efd_probe_log(
  probe_id INTEGER PRIMARY KEY,
  probed_at_unix INTEGER NOT NULL,
  probed_at_iso TEXT NOT NULL,
  hour_local INTEGER NOT NULL,
  kind TEXT NOT NULL,
  ok INTEGER NOT NULL,
  status TEXT,
  latency_ms INTEGER,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_probe_time ON efd_probe_log(probed_at_unix);
CREATE TABLE IF NOT EXISTS congress_member_roster(
  chamber TEXT NOT NULL,
  member_last TEXT,
  member_first TEXT,
  state_dist TEXT,
  party TEXT,
  state TEXT,
  match_kind TEXT NOT NULL,
  synced_at_unix INTEGER NOT NULL,
  -- SM-C3 Phase R. bioguide is the id committee membership is organised by, so without
  -- it a committee join would have to go back through (chamber, surname, state,
  -- district) and re-run the whole matching argument a second time. fetch_roster has
  -- always carried it per entry; it simply was never written down.
  bioguide TEXT,
  PRIMARY KEY(chamber, member_last, member_first, state_dist)
);
-- SM-C3 Phase R. Committee + subcommittee membership from unitedstates/congress-legislators,
-- keyed by bioguide. One row per (bioguide, committee). Current Congress only -- the
-- dataset publishes no historical membership file, so this CANNOT be used to ask what a
-- member sat on in a prior year, and any view built on it must say so.
-- SM-O1 P2: nightly options-chain snapshots. One row per contract per snapshot day.
-- OI SEMANTICS: open_interest is OCC-settled T+1, so a chain pulled on snapshot_date
-- carries the OI settled on oi_asof (the prior trading day) while `volume` is
-- snapshot_date's. The two dates are stored SEPARATELY and on purpose: collapsing them
-- would silently produce an off-by-one-day vol/OI ratio that still looks plausible.
-- Idempotent on (contract, snapshot_date) so a re-run overwrites rather than doubles.
CREATE TABLE IF NOT EXISTS options_chain_snapshots(
  ticker TEXT NOT NULL,
  snapshot_date TEXT NOT NULL,       -- when WE pulled it (provenance)
  session_date TEXT,                 -- the trading session the data describes

  expiry TEXT NOT NULL,
  strike REAL NOT NULL,
  option_type TEXT NOT NULL,          -- 'C' | 'P'
  contract_symbol TEXT,
  volume INTEGER,                     -- snapshot_date's trading
  open_interest INTEGER,              -- settled as of oi_asof, NOT snapshot_date
  oi_asof TEXT NOT NULL,
  implied_vol REAL,
  last_price REAL,
  bid REAL,
  ask REAL,
  underlying_close REAL,
  ingested_at_unix INTEGER NOT NULL,
  PRIMARY KEY(ticker, snapshot_date, expiry, strike, option_type)
);
CREATE INDEX IF NOT EXISTS idx_opt_snap ON options_chain_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_opt_tk ON options_chain_snapshots(ticker, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_opt_contract
  ON options_chain_snapshots(contract_symbol, snapshot_date);
-- Pass ledger. A missed night must be VISIBLE, not inferred from absent rows — absence
-- reads identically to "nothing traded", and options data cannot be rebuilt afterwards.
CREATE TABLE IF NOT EXISTS options_snapshot_passes(
  snapshot_date TEXT PRIMARY KEY,
  session_date TEXT,
  tickers INTEGER NOT NULL,
  ok INTEGER NOT NULL,
  gaps INTEGER NOT NULL,
  no_chain INTEGER NOT NULL,
  contracts INTEGER NOT NULL,
  dropped INTEGER NOT NULL,
  oi_asof TEXT,
  ran_at_unix INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS congress_committees(
  bioguide TEXT NOT NULL,
  committee_id TEXT NOT NULL,
  parent_id TEXT,
  committee_name TEXT,
  chamber TEXT,
  title TEXT,
  rank INTEGER,
  side TEXT,
  synced_at_unix INTEGER NOT NULL,
  PRIMARY KEY(bioguide, committee_id)
);
CREATE INDEX IF NOT EXISTS idx_comm_id ON congress_committees(committee_id);
-- Market-cap + SMID band cache (SM-A1-fix SMID scan). shares from SEC
-- companyconcept (dei then us-gaap fallback), cap = shares x price. Both as-of
-- dates recorded; a stale cap on a volatile small cap is a labeled error source.
CREATE TABLE IF NOT EXISTS market_cap(
  ticker TEXT PRIMARY KEY,
  cik TEXT,
  shares INTEGER,
  shares_asof TEXT,
  concept TEXT,
  price REAL,
  price_asof_unix INTEGER,
  cap REAL,
  band TEXT NOT NULL,
  computed_at_unix INTEGER NOT NULL
);
-- SM-O1 P1: Form 4 Table II (derivative) transactions — the option/warrant legs
-- the non-derivative table omits. Same idempotence key (accession, tx_index) and
-- regime tag as Table I; tx_index enumerates derivativeTransaction rows and is
-- independent of Table I's tx_index (separate table).
CREATE TABLE IF NOT EXISTS form4_derivatives(
  accession TEXT NOT NULL,
  tx_index INTEGER NOT NULL,
  reporting_person TEXT,
  reporting_cik TEXT,
  issuer TEXT,
  issuer_cik TEXT,
  ticker TEXT,
  security_title TEXT,
  code TEXT,
  plan_flag INTEGER,
  shares REAL,
  price REAL,
  exercise_price REAL,
  tx_date TEXT,
  exercise_date TEXT,
  expiration_date TEXT,
  underlying_title TEXT,
  underlying_shares REAL,
  filed_date TEXT,
  role TEXT,
  ingest_regime TEXT NOT NULL DEFAULT 'watchlist',
  PRIMARY KEY(accession, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_f4d_ticker ON form4_derivatives(ticker);
CREATE INDEX IF NOT EXISTS idx_f4d_cik ON form4_derivatives(reporting_cik);
CREATE INDEX IF NOT EXISTS idx_f4d_issuer ON form4_derivatives(issuer_cik);
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



def _migrate_coverage(con):
    """SM-C3 Phase H: add coverage_year / filing_date and backfill each chamber by
    its OWN existing convention. Idempotent — a no-op once the columns exist."""
    chcols = {r[1] for r in con.execute("PRAGMA table_info(congress_holdings)")}
    if not chcols:
        return
    if "coverage_year" not in chcols:
        con.execute("ALTER TABLE congress_holdings ADD COLUMN coverage_year INTEGER")
        con.execute("ALTER TABLE congress_holdings ADD COLUMN filing_date TEXT")
        con.commit()
    # The ALTER is one-shot, but the BACKFILL must run for ANY row still missing the
    # derived values — otherwise rows written before the columns existed never heal.
    # Guarded by a cheap existence probe so the normal case is a single indexed lookup.
    todo = con.execute("SELECT 1 FROM congress_holdings WHERE coverage_year IS NULL "
                       "OR filing_date IS NULL LIMIT 1").fetchone()
    if todo:
        # Backfill the two chambers by their DIFFERENT existing conventions, verified
        # against the corpus: the Senate eFD title year is already the coverage year
        # ("Annual Report for CY 2025", filed 2026), while the House filing_year is the
        # FD cycle/zip year so its coverage is year-1. Getting this backwards mis-ages
        # every House anchor by a year.
        con.execute("UPDATE congress_holdings SET coverage_year=filing_year "
                    "WHERE coverage_year IS NULL AND chamber='senate' "
                    "AND filing_year IS NOT NULL")
        # House: {N}FD.zip holds annuals COVERING calendar year N (filed mostly N+1),
        # so coverage_year == filing_year. An earlier `-1` here put every House coverage
        # year one year early and moved the Phase F flow cutoff back a full year.
        con.execute("UPDATE congress_holdings SET coverage_year=filing_year "
                    "WHERE coverage_year IS NULL AND chamber='house' "
                    "AND filing_year IS NOT NULL")
        # `period` holds the raw filed date as the source wrote it (M/D/YYYY or
        # MM/DD/YYYY). Normalise to ISO; anything unparseable stays NULL, never guessed.
        con.execute(
            "UPDATE congress_holdings SET filing_date = "
            "substr(period, -4) || '-' || "
            "substr('0' || substr(period, 1, instr(period,'/')-1), -2) || '-' || "
            "substr('0' || substr(substr(period, instr(period,'/')+1), 1, "
            "  instr(substr(period, instr(period,'/')+1), '/')-1), -2) "
            "WHERE filing_date IS NULL AND period LIKE '%/%/%'")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ch_cov ON "
                    "congress_holdings(coverage_year)")
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
    if "filing_status" not in cols:
        # The Clerk's per-line New/Amended/Deleted marker. Parsed since the
        # first House ingest but discarded before insert, so existing rows are
        # NULL until reparse_status backfills them. NULL means "not yet
        # extracted", never "no amendment" — readers must not treat the two
        # as equivalent.
        con.execute("ALTER TABLE congress_trades ADD COLUMN filing_status TEXT")
        con.execute("ALTER TABLE congress_trades ADD COLUMN clerk_line_id TEXT")
        con.execute("CREATE INDEX IF NOT EXISTS idx_trades_fstatus ON "
                    "congress_trades(filing_status)")
        con.commit()
    hcols = {r[1] for r in con.execute("PRAGMA table_info(thirteenf_holdings)")}
    if hcols and "value_scale" not in hcols:
        # See the schema comment: the filing states no unit for VALUE, so the scale
        # is resolved once at ingest instead of re-derived by every reader.
        # NULL means "not yet resolved", never "scale 1" — value_usd COALESCEs to 1
        # so an unresolved row reads as raw, which is the pre-existing behaviour and
        # is visibly wrong for a thousands filer rather than silently plausible.
        con.execute("ALTER TABLE thirteenf_holdings ADD COLUMN value_scale INTEGER")
        con.execute("ALTER TABLE thirteenf_holdings ADD COLUMN shares_type TEXT")
        con.execute("ALTER TABLE thirteenf_holdings ADD COLUMN title_of_class TEXT")
        con.execute(
            "ALTER TABLE thirteenf_holdings ADD COLUMN value_usd INTEGER "
            "GENERATED ALWAYS AS (value * COALESCE(value_scale, 1)) VIRTUAL")
        con.commit()
    if hcols and "instrument_class" not in hcols:
        # NULL means "not yet classified", never "common" — an unclassified row
        # must not read as an ordinary equity position.
        con.execute("ALTER TABLE thirteenf_holdings ADD COLUMN instrument_class TEXT")
        con.execute("ALTER TABLE thirteenf_holdings ADD COLUMN issuer_id TEXT "
                    "GENERATED ALWAYS AS (substr(cusip, 1, 6)) VIRTUAL")
        con.commit()
    # Generated columns are invisible to PRAGMA table_info, so ask table_xinfo.
    # Created here rather than in the DDL because the DDL runs before this on a
    # pre-existing table, where the column does not exist yet.
    xcols = {r[1] for r in con.execute("PRAGMA table_xinfo(thirteenf_holdings)")}
    if "issuer_id" in xcols:
        con.execute("CREATE INDEX IF NOT EXISTS idx_13fh_issuer ON "
                    "thirteenf_holdings(issuer_id)")
        con.commit()
    ccols = {r[1] for r in con.execute("PRAGMA table_info(cusip_ticker)")}
    if ccols and "exch_code" not in ccols:
        for c in ("exch_code", "market_sector", "security_type", "ticker_raw"):
            con.execute("ALTER TABLE cusip_ticker ADD COLUMN {} TEXT".format(c))
        con.commit()
    _migrate_coverage(con)
    ocols = {r[1] for r in con.execute("PRAGMA table_info(options_chain_snapshots)")}
    if ocols and "session_date" not in ocols:
        # SM-O1 P2: the leg rides a nightly scan, so it fires on weekends, and a weekend
        # pull returns the PREVIOUS session verbatim (measured: Sat vs Sun matched on
        # 9,499/9,500 contracts by volume). Backfilled by the weekday rule so existing
        # rows become groupable immediately.
        con.execute("ALTER TABLE options_chain_snapshots ADD COLUMN session_date TEXT")
        con.execute(
            "UPDATE options_chain_snapshots SET session_date = CASE "
            "WHEN CAST(strftime('%w', snapshot_date) AS INTEGER)=6 "
            "  THEN date(snapshot_date,'-1 day') "
            "WHEN CAST(strftime('%w', snapshot_date) AS INTEGER)=0 "
            "  THEN date(snapshot_date,'-2 day') "
            "ELSE snapshot_date END WHERE session_date IS NULL")
        con.commit()
    # Repair rows written before oi_asof was derived from the SESSION rather than the
    # pull date. A weekend pull got oi_asof == session_date, i.e. the chain claiming to
    # carry its own session's settled OI - impossible under T+1. Measured before
    # repairing: the Friday pull and the Saturday pull agreed on open interest for
    # 10,568 of 10,568 contracts (100.0%), so the weekend rows do carry the Friday
    # chain's Thursday-settled figure and the prior-trading-day target is right.
    # Self-healing and idempotent: only rows that are still wrong are touched.
    if ocols and "session_date" in {r[1] for r in con.execute(
            "PRAGMA table_info(options_chain_snapshots)")}:
        con.execute(
            "UPDATE options_chain_snapshots SET oi_asof = CASE "
            "WHEN CAST(strftime('%w', session_date) AS INTEGER)=1 "
            "  THEN date(session_date,'-3 day') "
            "ELSE date(session_date,'-1 day') END "
            "WHERE session_date IS NOT NULL AND oi_asof >= session_date")
        con.commit()
    # Index created HERE, never in the eager schema block: CREATE TABLE IF NOT EXISTS is
    # a no-op on an existing DB, so the column only exists after the ALTER above. Indexing
    # it earlier raised "no such column: session_date" and took the whole connect() down.
    if ocols:
        con.execute("CREATE INDEX IF NOT EXISTS idx_opt_sess "
                    "ON options_chain_snapshots(session_date)")
        con.commit()
    pcols = {r[1] for r in con.execute("PRAGMA table_info(options_snapshot_passes)")}
    if pcols and "session_date" not in pcols:
        con.execute("ALTER TABLE options_snapshot_passes ADD COLUMN session_date TEXT")
        con.commit()
    # Backfill the LEDGER too, on the same weekday rule as the rows. Missed initially,
    # which left older passes reading session=None — and the ledger is precisely what
    # makes a missed night visible and what the P5 miss-rate is computed from, so a null
    # there is not cosmetic. Both the session and the oi_asof are healed.
    if pcols:
        con.execute(
            "UPDATE options_snapshot_passes SET session_date = CASE "
            "WHEN CAST(strftime('%w', snapshot_date) AS INTEGER)=6 "
            "  THEN date(snapshot_date,'-1 day') "
            "WHEN CAST(strftime('%w', snapshot_date) AS INTEGER)=0 "
            "  THEN date(snapshot_date,'-2 day') "
            "ELSE snapshot_date END WHERE session_date IS NULL")
        con.execute(
            "UPDATE options_snapshot_passes SET oi_asof = CASE "
            "WHEN CAST(strftime('%w', session_date) AS INTEGER)=1 "
            "  THEN date(session_date,'-3 day') "
            "ELSE date(session_date,'-1 day') END "
            "WHERE session_date IS NOT NULL AND oi_asof >= session_date")
        con.commit()
    rcols = {r[1] for r in con.execute("PRAGMA table_info(congress_member_roster)")}
    if rcols and "bioguide" not in rcols:
        # SM-C3 Phase R. Left NULL until the next roster sync repopulates it — the sync
        # is a full rebuild, so no backfill is needed here and inventing one from names
        # would re-run the matching argument the bioguide exists to end.
        con.execute("ALTER TABLE congress_member_roster ADD COLUMN bioguide TEXT")
        con.commit()
    f4cols = {r[1] for r in con.execute("PRAGMA table_info(form4_transactions)")}
    if f4cols and "issuer_cik" not in f4cols:
        con.execute("ALTER TABLE form4_transactions ADD COLUMN issuer_cik TEXT")
        con.commit()
    if f4cols and "ingest_regime" not in f4cols:
        # Existing rows are the issuer-scoped backfill = 'watchlist'; universal
        # discovery rows tag 'universal'. One corpus, distinguishable by tag.
        con.execute("ALTER TABLE form4_transactions ADD COLUMN ingest_regime TEXT "
                    "NOT NULL DEFAULT 'watchlist'")
        con.commit()
    if f4cols and "security_title" not in f4cols:
        # Forward-only. Existing rows stay NULL rather than being back-filled from a
        # re-parse of 153,182 accessions; NULL honestly means "we did not record it",
        # and a guess would be worse than the gap.
        con.execute("ALTER TABLE form4_transactions ADD COLUMN security_title TEXT")
        con.commit()
    if f4cols and "value_flag" not in f4cols:
        # Reason a row's derived dollar value was quarantined by the parse-time
        # sanity guard (form4.value_sanity_flag); NULL means the value is trusted.
        # Existing rows stay NULL until re-parsed (scripts/reparse_corrupt_form4).
        con.execute("ALTER TABLE form4_transactions ADD COLUMN value_flag TEXT")
        con.commit()
