"""Dossier Store — M10-D §3.2, the spine of the intelligence product.

Persistent, append-mostly SQLite store of every footprint a scan detects above a low
notional floor. CAPTURE WIDE, RENDER NARROW: this layer captures broadly and tags
richly; narrowing happens at query/render time, never here. Nothing is deleted.

Grain: one row per footprint = (condition_id, token_id, wallet). ``dossier_id`` is a
deterministic hash of that triple, so a re-scan of the same footprint updates the same
row (idempotent) rather than duplicating it. Cluster/actor facts, funding, and CEX class
are recorded as fields on the footprint (recorded, not scored — the mesh-collapse count
is the n=1 INVERSION made visible; "cluster size" = ``actor_count_post_collapse``).

Query-time tags (category / price-band / notional-bucket / freshness / cluster-solo /
mesh-collapsed / resolved) are DERIVED from the columns at query time — there are no
capture-time filter columns, by design.

Rule 1: every row carries ``provenance`` (refs to the raw cached records it came from);
missing data is stored as NULL and declared, never imputed. Tiers LATCH at their
high-water mark (v1.3 §3.3): ``tier`` is the latest, ``tier_peak``/``tier_peak_ts`` the
crossing — decay shows as trajectory, never retraction. Resolution is stamped later by
the backfill job (§6), which is what turns the store into a labeled dataset over time.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any, Callable, Iterable

SCHEMA_VERSION = 1

TIER_RANK = {"NONE": 0, "WATCH": 1, "ELEVATED": 2, "CRITICAL": 3}

SCHEMA = """
CREATE TABLE IF NOT EXISTS dossiers (
  dossier_id       TEXT PRIMARY KEY,
  -- capture bookkeeping (idempotency)
  first_scan_ts    INTEGER NOT NULL,
  last_scan_ts     INTEGER NOT NULL,
  n_scans          INTEGER NOT NULL DEFAULT 1,
  -- identity
  wallet           TEXT NOT NULL,
  condition_id     TEXT NOT NULL,
  token_id         TEXT NOT NULL,
  side             TEXT,
  market_question  TEXT,
  market_category  TEXT,
  event_slug       TEXT,
  -- footprint facts
  first_seen_ts    INTEGER,
  first_seen_source TEXT,          -- activity | etherscan | activity_capped | unavailable (Rule-1)
  detection_ts     INTEGER,
  entry_vwap       REAL,
  price_at_detection REAL,
  contested_notional REAL,         -- the informed/contested slice
  headline_notional  REAL,         -- total position (carry-trade confound; kept SEPARATE)
  -- factors (latency nullable; A/P excluded upstream, declared not imputed)
  f_factor         REAL,
  s_factor         REAL,
  d_factor         REAL,
  c_factor         REAL,
  latency_factor   REAL,
  composite        REAL,
  -- tier, LATCHED high-water mark
  tier             TEXT,
  tier_peak        TEXT,
  tier_peak_ts     INTEGER,
  -- composite high-water mark. The alert bar reads THIS, not the frozen/decaying
  -- composite: a footprint that peaked above the bar must not escape alerting just
  -- because a later scan re-scored it lower (constraint 6, decay is trajectory).
  composite_peak   REAL,
  -- cluster / actor (recorded, not scored) -- the INVERSION
  cluster_id       TEXT,
  cluster_wallets  TEXT,           -- JSON raw list (may be many)
  actor_count_post_collapse INTEGER,  -- n after mesh-collapse; "cluster size"
  cross_market_cluster TEXT,       -- JSON
  -- funding / CEX (confidence kept; render applies the low-conf -> unclassified honesty)
  funding_summary  TEXT,           -- JSON
  cex_class        TEXT,
  cex_confidence   REAL,
  -- resolution (stamped by the backfill job, NULL until the market resolves)
  resolved         INTEGER NOT NULL DEFAULT 0,
  winning_token    TEXT,
  resolution_ts    INTEGER,
  outcome_for_side INTEGER,        -- 1 flagged side won, 0 lost, NULL unresolved
  -- provenance + label
  provenance       TEXT,           -- JSON refs to raw cached records (Rule 1)
  label            TEXT,           -- unset until reviewed
  -- alerting (M10-D §3.5): stamped when this dossier has been alerted on, so a
  -- footprint that persists across daily scans pages the owner ONCE, not daily.
  alerted_ts       INTEGER,
  alerted_reason   TEXT,
  -- the composite the page was consumed AT. A dossier that later escalates materially
  -- above this must be able to page again; without it, one early marginal crossing
  -- permanently silences a footprint that goes on to become far stronger.
  alerted_composite REAL
);
CREATE INDEX IF NOT EXISTS idx_dossier_market ON dossiers(condition_id);
CREATE INDEX IF NOT EXISTS idx_dossier_wallet ON dossiers(wallet);
CREATE INDEX IF NOT EXISTS idx_dossier_tierpeak ON dossiers(tier_peak);
CREATE INDEX IF NOT EXISTS idx_dossier_resolved ON dossiers(resolved);
CREATE INDEX IF NOT EXISTS idx_dossier_category ON dossiers(market_category);
CREATE TABLE IF NOT EXISTS dossier_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
"""

# columns the caller supplies on a footprint (everything except bookkeeping + resolution)
_CAPTURE_COLS = [
    "wallet", "condition_id", "token_id", "side", "market_question", "market_category",
    "event_slug", "first_seen_ts", "first_seen_source", "detection_ts", "entry_vwap",
    "price_at_detection", "contested_notional", "headline_notional", "f_factor", "s_factor",
    "d_factor", "c_factor", "latency_factor", "composite", "tier", "cluster_id",
    "cluster_wallets", "actor_count_post_collapse", "cross_market_cluster", "funding_summary",
    "cex_class", "cex_confidence", "provenance",
]
_JSON_COLS = {"cluster_wallets", "cross_market_cluster", "funding_summary", "provenance"}
_REQUIRED = {"wallet", "condition_id", "token_id"}
# §6 FROZEN AS-SCORED: the detection-time snapshot a future powered edge test must be
# able to trust. Written on first capture, never rewritten — otherwise a re-scan would
# overwrite the factor vector that produced the original tier, and the accumulating
# labeled dataset would silently describe today's scoring rather than the scoring that
# actually fired. (Current-state movement lives in tier/tier_peak/composite_peak.)
_FROZEN_COLS = {
    "detection_ts", "entry_vwap", "price_at_detection",
    "f_factor", "s_factor", "d_factor", "c_factor", "latency_factor", "composite",
}


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    # Idempotent column adds for stores created before a schema bump. CREATE TABLE
    # IF NOT EXISTS never alters an existing table, so new columns land here. NOTE:
    # migrations run BEFORE any index that references a new column (the ordering bug
    # that makes an old l2_tape unopenable — see tape.py; not repeated here).
    have = {r[1] for r in con.execute("PRAGMA table_info(dossiers)")}
    for col, decl in (("alerted_ts", "INTEGER"), ("alerted_reason", "TEXT"),
                      ("alerted_composite", "REAL"), ("composite_peak", "REAL")):
        if col not in have:
            con.execute(f"ALTER TABLE dossiers ADD COLUMN {col} {decl}")
    con.execute("INSERT OR IGNORE INTO dossier_meta(k, v) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),))
    con.commit()
    return con


def make_dossier_id(condition_id: str, token_id: str, wallet: str) -> str:
    h = hashlib.sha1(f"{condition_id}|{token_id}|{wallet}".encode()).hexdigest()
    return h[:20]


def _enc(rec: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for c in _CAPTURE_COLS:
        v = rec.get(c)
        out[c] = json.dumps(v) if (c in _JSON_COLS and v is not None) else v
    return out


def upsert(con: sqlite3.Connection, rec: dict[str, Any], *, scan_ts: int | None = None) -> str:
    """Insert or idempotently update a footprint. Returns 'inserted' or 'updated'.

    On update: ``first_scan_ts`` and a set ``label`` and the resolution fields are
    PRESERVED; ``n_scans`` increments; current fields take the new scan's values; and
    ``tier_peak`` LATCHES at the high-water mark (never retracts)."""
    missing = _REQUIRED - {k for k, v in rec.items() if v is not None}
    if missing:
        raise ValueError(f"dossier record missing required fields: {sorted(missing)}")
    scan_ts = scan_ts or int(time.time())
    did = make_dossier_id(rec["condition_id"], rec["token_id"], rec["wallet"])
    enc = _enc(rec)
    cur = con.execute("SELECT tier_peak, tier_peak_ts FROM dossiers WHERE dossier_id=?", (did,))
    row = cur.fetchone()
    new_tier = rec.get("tier")
    if row is None:
        cols = ["dossier_id", "first_scan_ts", "last_scan_ts", "n_scans",
                "tier_peak", "tier_peak_ts"] + _CAPTURE_COLS
        vals = [did, scan_ts, scan_ts, 1, new_tier, (scan_ts if new_tier else None)] + \
               [enc[c] for c in _CAPTURE_COLS]
        con.execute(f"INSERT INTO dossiers({','.join(cols)}) VALUES({','.join('?' * len(cols))})", vals)
        con.commit()
        return "inserted"
    # latch tier_peak at high-water mark
    peak, peak_ts = row
    if new_tier and TIER_RANK.get(new_tier, 0) > TIER_RANK.get(peak or "NONE", 0):
        peak, peak_ts = new_tier, scan_ts
    # NEVER-WIPE semantics: a later scan fills gaps but cannot erase what an earlier
    # scan captured. A re-scan in which a wallet is not gated for enrichment supplies
    # first_seen/F/funding as NULL; a plain assignment would wipe the enrichment the
    # gated scan paid for, silently shrinking the store (breaks CAPTURE WIDE and the
    # §6 labeled-dataset promise). COALESCE(new, old) keeps the earlier value.
    # Frozen-as-scored columns are exempt from update entirely (see _FROZEN_COLS).
    sets = ["last_scan_ts=?", "n_scans=n_scans+1", "tier_peak=?", "tier_peak_ts=?",
            "composite_peak=MAX(COALESCE(composite_peak,-1e9), COALESCE(?,-1e9))"]
    vals: list[Any] = [scan_ts, peak, peak_ts, rec.get("composite")]
    for c in _CAPTURE_COLS:
        if c in _FROZEN_COLS:
            continue          # detection-time snapshot: written once, never rewritten
        sets.append(f"{c}=COALESCE(?,{c})")
        vals.append(enc[c])
    vals.append(did)
    con.execute(f"UPDATE dossiers SET {','.join(sets)} WHERE dossier_id=?", vals)
    con.commit()
    return "updated"


def write_scan(con: sqlite3.Connection, records: Iterable[dict[str, Any]], *,
               scan_ts: int | None = None) -> dict[str, int]:
    scan_ts = scan_ts or int(time.time())
    counts = {"inserted": 0, "updated": 0}
    for rec in records:
        counts[upsert(con, rec, scan_ts=scan_ts)] += 1
    return counts


def tape_resolver(tape_path: str) -> Callable[[str], tuple[str, int] | None]:
    """Build a ``resolve_fn`` for :func:`backfill_resolutions` from the L2 tape.

    The collector stamps ``l2_markets.resolution`` with the RAW gamma outcome JSON
    (``outcomes`` + ``outcomePrices``); the winning TOKEN is recovered by mapping the
    winning outcome NAME to the token id via the market's own fills. Rule 1: raw values
    parsed at read time, nothing imputed — an unresolved or unparseable market simply
    returns None and stays unresolved rather than being guessed.
    """
    def resolve(condition_id: str) -> tuple[str, int] | None:
        con = sqlite3.connect(f"file:{tape_path}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT resolution, close_seen_ts FROM l2_markets WHERE condition_id=?",
                (condition_id,)).fetchone()
            if not row or not row[0]:
                return None
            try:
                blob = json.loads(row[0])
                names = blob.get("outcomes")
                prices = blob.get("outcomePrices")
                if isinstance(names, str):
                    names = json.loads(names)
                if isinstance(prices, str):
                    prices = json.loads(prices)
                if not names or not prices or len(names) != len(prices):
                    return None
                idx = max(range(len(prices)), key=lambda i: float(prices[i]))
                if float(prices[idx]) < 0.99:      # not a decisive settle -> leave it
                    return None
                winner_name = str(names[idx])
            except (ValueError, TypeError, KeyError):
                return None
            tok = con.execute(
                "SELECT asset FROM l2_trades WHERE condition_id=? AND outcome=? "
                "AND asset IS NOT NULL LIMIT 1", (condition_id, winner_name)).fetchone()
            if not tok:
                return None
            res_ts = int(blob.get("swept_ts") or row[1] or 0) or None
            return (tok[0], res_ts) if res_ts else None
        finally:
            con.close()
    return resolve


def set_label(con: sqlite3.Connection, dossier_id: str, label: str | None) -> None:
    """Review-time labeling. Scans never touch ``label`` (not a capture column), so a
    label survives every re-scan; this is the only writer."""
    con.execute("UPDATE dossiers SET label=? WHERE dossier_id=?", (label, dossier_id))
    con.commit()


def backfill_resolutions(
    con: sqlite3.Connection,
    resolve_fn: Callable[[str], tuple[str, int] | None],
) -> dict[str, int]:
    """Stamp outcomes on unresolved dossiers. ``resolve_fn(condition_id)`` returns
    ``(winning_token, resolution_ts)`` once the market has resolved, else None. This is
    what turns the store into a labeled dataset (§6). ``outcome_for_side`` = did the
    footprint's held token win."""
    stats = {"checked": 0, "stamped": 0}
    rows = con.execute(
        "SELECT DISTINCT condition_id FROM dossiers WHERE resolved=0").fetchall()
    for (cid,) in rows:
        stats["checked"] += 1
        res = resolve_fn(cid)
        if not res:
            continue
        win, rts = res
        con.execute(
            "UPDATE dossiers SET resolved=1, winning_token=?, resolution_ts=?, "
            "outcome_for_side=(token_id=?) WHERE condition_id=? AND resolved=0",
            (win, rts, win, cid))
        stats["stamped"] += 1
    con.commit()
    return stats


# --- query-time tag derivation (NOT stored; computed on read) ---------------------

def price_band(vwap: float | None) -> str:
    if vwap is None:
        return "unknown"
    if vwap < 0.10 or vwap > 0.90:
        return "favorite"        # outside the contested gate
    return "contested"


def notional_bucket(n: float | None) -> str:
    if not n:
        return "none"
    for edge, name in ((1e4, "<10k"), (5e4, "10-50k"), (2.5e5, "50-250k"), (1e6, "250k-1M")):
        if n < edge:
            return name
    return ">1M"


def freshness_tag(first_seen_ts: int | None, ref_ts: int | None) -> str:
    if first_seen_ts is None or ref_ts is None:
        return "unknown"
    age = (ref_ts - first_seen_ts) / 86400
    return "fresh<=7d" if age <= 7 else ("recent<=30d" if age <= 30 else "established")


def is_mesh_collapsed(row: dict[str, Any]) -> bool | None:
    """True/False when the post-collapse actor count is known; None when it is NOT.

    A NULL actor count means the funding mesh was never computed — it does NOT mean
    "no collapse occurred". Returning False there would fail OPEN, presenting an
    unchecked cluster as verified-independent (the §4.2 inversion in reverse)."""
    actors = row.get("actor_count_post_collapse")
    if actors is None:
        return None
    cw = row.get("cluster_wallets")
    try:
        n_raw = len(json.loads(cw)) if cw else 1
    except Exception:
        n_raw = 1
    return actors < n_raw


def query(con: sqlite3.Connection, *, category: str | None = None,
          min_tier_peak: str | None = None, resolved: bool | None = None,
          contested_only: bool = False, since_ts: int | None = None,
          limit: int = 500) -> list[dict[str, Any]]:
    """Narrow the store at read time. This is the 'render narrow' side."""
    where, args = [], []
    if category:
        where.append("market_category=?"); args.append(category)
    if min_tier_peak:
        ranks = [t for t, r in TIER_RANK.items() if r >= TIER_RANK.get(min_tier_peak, 0)]
        where.append(f"tier_peak IN ({','.join('?' * len(ranks))})"); args += ranks
    if resolved is not None:
        where.append("resolved=?"); args.append(1 if resolved else 0)
    if contested_only:
        where.append("entry_vwap>=0.10 AND entry_vwap<=0.90")
    if since_ts:
        where.append("detection_ts>=?"); args.append(since_ts)
    sql = "SELECT * FROM dossiers"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY composite DESC LIMIT ?"; args.append(limit)
    cur = con.execute(sql, args)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]
