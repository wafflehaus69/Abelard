"""SM-4 scan-layer unit tests: watermark discipline, cluster flag threshold,
overlay matching, 13F diff. No network."""
import os
import tempfile

from smart_money import db as dbmod
from smart_money import watermarks
from smart_money.events import cluster_flag
from smart_money.overlay import Overlay
from smart_money.thirteenf import diff


def _fresh():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return p


# ---- watermark advance/preserve matrix ----

def test_watermark_advance_and_preserve():
    p = _fresh()
    try:
        con = dbmod.connect(p)
        assert watermarks.get(con, "src") is None            # initial
        watermarks.advance(con, "src", "2026-07-01")          # ok-with-items
        assert watermarks.get(con, "src") == "2026-07-01"
        watermarks.advance(con, "src", "2026-07-10")          # newer item
        assert watermarks.get(con, "src") == "2026-07-10"
        # non-monotonic (older) never moves backward
        watermarks.advance(con, "src", "2026-07-05")
        assert watermarks.get(con, "src") == "2026-07-10"
        # 0-item-ok / non-ok: caller does not call advance -> preserved
        assert watermarks.get(con, "src") == "2026-07-10"
    finally:
        os.remove(p)


# ---- cluster flag threshold ----

def _trade(con, pid, ticker, side, tx):
    con.execute(
        "INSERT OR IGNORE INTO persons(person_id,name,type,cik_or_chamber) "
        "VALUES (?,?,'congress','house')", (pid, "P{}".format(pid)))
    con.execute(
        "INSERT INTO congress_trades(person_id,ticker,side,amt_low,amt_high,"
        "tx_date,disclosure_date,lag_days,chamber,source,raw_ref,asset_type,"
        "filing_id,superseded) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
        (pid, ticker, side, 1001, 15000, tx, tx, 0, "house", "t",
         "{}-{}".format(pid, tx), "Stock", "f{}-{}".format(pid, tx)))


def test_cluster_flag_threshold():
    p = _fresh()
    try:
        con = dbmod.connect(p)
        # 3 distinct persons buy AAA within 30d -> flag at default 3/30
        _trade(con, 1, "AAA", "purchase", "2026-07-01")
        _trade(con, 2, "AAA", "purchase", "2026-07-10")
        _trade(con, 3, "AAA", "purchase", "2026-07-20")
        con.commit()
        f = cluster_flag(con, "AAA", "buy", "2026-07-20", 3, 30)
        assert f and f["n_persons"] == 3, f
        # only 2 within window (first one falls outside 30d back from 08-15)
        assert cluster_flag(con, "AAA", "buy", "2026-08-15", 3, 30) is None
        # opposite direction has no sellers
        assert cluster_flag(con, "AAA", "sell", "2026-07-20", 3, 30) is None
    finally:
        os.remove(p)


# ---- overlay matching (exact only) ----

def test_overlay_exact_match():
    ov = Overlay(["MSTR", "BITB"], ["GLD", "MOG.A"], 3, 30)
    assert ov.match("MSTR") == (True, False)
    assert ov.match("gld") == (False, True)
    assert ov.match("MOG.A") == (False, True)
    assert ov.match("AAPL") == (False, False)   # unknown does not flag
    assert ov.match(None) == (False, False)


# ---- 13F diff ----

def test_13f_diff():
    base = {
        "AAA": {"issuer": "Aaa", "value": 100, "shares": 10, "net_opt": 50},
        "BBB": {"issuer": "Bbb", "value": 200, "shares": 20, "net_opt": 0},
        "CCC": {"issuer": "Ccc", "value": 100, "shares": 5, "net_opt": -10},
    }
    new = {
        "AAA": {"issuer": "Aaa", "value": 250, "shares": 25, "net_opt": -30},  # 2x up + flip
        "BBB": {"issuer": "Bbb", "value": 200, "shares": 20, "net_opt": 0},    # unchanged
        "DDD": {"issuer": "Ddd", "value": 300, "shares": 30, "net_opt": 0},    # new
    }
    kinds = {(c, k) for c, _, k, _ in diff(base, new)}
    assert ("DDD", "new_position") in kinds
    assert ("CCC", "exit") in kinds
    assert ("AAA", "value_2x_up") in kinds
    assert ("AAA", "directionality_flip") in kinds


QUEUE_SCHEMA = """
CREATE TABLE queue_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_unix INTEGER NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL,
  topic_key TEXT NOT NULL, dedupe_key TEXT NOT NULL UNIQUE,
  payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending');
"""


def test_enqueue_notable_only_and_idempotent():
    import sqlite3
    from smart_money import scan
    fd, qpath = tempfile.mkstemp(suffix=".db"); os.close(fd)
    sqlite3.connect(qpath).executescript(QUEUE_SCHEMA)
    os.environ["ABELARD_QUEUE_DB_PATH"] = qpath
    try:
        events = [
            {"event_id": "e1", "ticker": "MSTR",
             "flags": {"conviction_overlay": True, "watchlist_overlay": False,
                       "cluster": None, "sentinel": None}},
            {"event_id": "e2", "ticker": "BAC",
             "flags": {"conviction_overlay": False, "watchlist_overlay": False,
                       "cluster": None, "sentinel": None}},
            {"event_id": "e3", "ticker": "BITB",
             "flags": {"conviction_overlay": False, "watchlist_overlay": False,
                       "cluster": None, "sentinel": {"role": "btc_flow_sentinel"}}},
        ]
        r1 = scan._enqueue({}, events)
        assert r1["enqueued"] == 2, r1  # e1 overlay + e3 sentinel, not e2 quiet
        n = sqlite3.connect(qpath).execute(
            "SELECT COUNT(*) FROM queue_items").fetchone()[0]
        assert n == 2
        r2 = scan._enqueue({}, events)  # re-run
        assert r2["enqueued"] == 0, r2  # idempotent by event_id
        n2 = sqlite3.connect(qpath).execute(
            "SELECT COUNT(*) FROM queue_items").fetchone()[0]
        assert n2 == 2
    finally:
        del os.environ["ABELARD_QUEUE_DB_PATH"]
        os.remove(qpath)


# ---- SM-P2 G1: the nightly price universe must cover the unit anchor ----

_H13 = ("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
        "ticker, issuer, put_call, value, shares, ingested_at_unix) "
        "VALUES(?,?,?,?,?,?,'Co',?,1,1,0)")
_F4 = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
       "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
       "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
       "VALUES(?,0,'P','1','Co','9',?,'P',0,1,1.0,1,NULL,?,?,NULL,'watchlist')")


def test_price_universe_covers_13f_holdings_not_just_insider_trades():
    """G1 residue: a filer whose only position never appears in a Form 4 (Affinity/QXO)
    had no close to anchor dollars-vs-thousands against, so its unit read UNDETERMINED."""
    from smart_money.scan import _price_universe
    p = _fresh()
    try:
        con = dbmod.connect(p)
        con.execute(_F4, ("a", "AAA", "2026-08-01", "2026-08-02"))
        con.execute(_H13, ("1", "acc", "2026-03-31", "2026-05-15", "c1", "QXO", "long"))
        con.commit()
        u = _price_universe(con, [], "2026-06-01")
        assert "QXO" in u and "AAA" in u, u
        con.close()
    finally:
        os.unlink(p)


def test_price_universe_takes_the_newest_period_per_filer_only():
    """Bounded by design: a filer's stale periods are the backfill's job, not the
    nightly leg's, so an exited name does not accrue a nightly fetch forever."""
    from smart_money.scan import _price_universe
    p = _fresh()
    try:
        con = dbmod.connect(p)
        con.execute(_H13, ("1", "old", "2025-12-31", "2026-02-14", "c1", "GONE", "long"))
        con.execute(_H13, ("1", "new", "2026-03-31", "2026-05-15", "c2", "HELD", "long"))
        # a second filer's newest period is older - it still contributes ITS newest
        con.execute(_H13, ("2", "b", "2025-12-31", "2026-02-14", "c3", "OTHER", "long"))
        con.commit()
        u = _price_universe(con, [], "2026-06-01")
        assert "HELD" in u and "OTHER" in u, u
        assert "GONE" not in u, "a superseded period must not be refreshed nightly"
        con.close()
    finally:
        os.unlink(p)


def test_price_universe_drops_symbols_yahoo_cannot_fetch():
    from smart_money.scan import _price_universe
    p = _fresh()
    try:
        con = dbmod.connect(p)
        con.execute(_H13, ("1", "a", "2026-03-31", "2026-05-15", "c1", "UONE/K", "long"))
        con.execute(_H13, ("1", "a", "2026-03-31", "2026-05-15", "c2", "BAD SYM", "long"))
        con.execute(_H13, ("1", "a", "2026-03-31", "2026-05-15", "c3", "OK", "long"))
        con.commit()
        u = _price_universe(con, [], "2026-06-01")
        assert u == ["OK"], u
        con.close()
    finally:
        os.unlink(p)


def test_price_universe_ignores_option_legs():
    """put/call rows carry notional, not shares, so they cannot anchor a unit."""
    from smart_money.scan import _price_universe
    p = _fresh()
    try:
        con = dbmod.connect(p)
        con.execute(_H13, ("1", "a", "2026-03-31", "2026-05-15", "c1", "PUTONLY", "put"))
        con.execute(_H13, ("1", "a", "2026-03-31", "2026-05-15", "c2", "LONGONE", "long"))
        con.commit()
        assert _price_universe(con, [], "2026-06-01") == ["LONGONE"]
        con.close()
    finally:
        os.unlink(p)
