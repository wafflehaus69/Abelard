"""SM-R1 P2 query-layer tests: flow-based ownership pressure (with the Form 4
amendment-supersede case), distribution-first sell-anomaly, the mixed-padding
CIK regression guard, and the structural read-only guarantee."""
import os
import sqlite3
import tempfile

from smart_money import db as dbmod
from smart_money import queries as q

_F4_INSERT = (
    "INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
    "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
    "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
    "VALUES(:accession,0,:reporting_person,:reporting_cik,:issuer,:issuer_cik,"
    ":ticker,:code,:plan_flag,:shares,NULL,NULL,NULL,:tx_date,:filed_date,NULL,"
    ":regime)")


def _row(accession, reporting_cik, code, shares, tx_date, filed_date,
         regime="watchlist", issuer_cik="999", ticker="XYZ", plan_flag=0,
         reporting_person=None):
    return {"accession": accession, "reporting_cik": reporting_cik,
            "reporting_person": reporting_person or ("P" + str(reporting_cik)),
            "issuer": "Xyz Corp", "issuer_cik": issuer_cik, "ticker": ticker,
            "code": code, "plan_flag": plan_flag, "shares": shares,
            "tx_date": tx_date, "filed_date": filed_date, "regime": regime}


def _fresh_db(rows):
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)                 # setup only: creates schema (writes)
    con.executemany(_F4_INSERT, rows)
    con.commit()
    con.close()
    return path


def test_ownership_pressure_flow_and_amendment_supersede():
    rows = [
        # Original 4 and its 4/A: same economic key, different accession — MUST
        # collapse to the later filing, counted once (not 200 shares).
        _row("ACC-1", "111", "P", 100, "2026-02-01", "2026-02-02"),
        _row("ACC-2", "111", "P", 100, "2026-02-01", "2026-02-05"),
        # Same insider, DIFFERENT shares same day — a genuine second trade, must
        # NOT collapse (guards over-dedup).
        _row("ACC-3", "111", "P", 50, "2026-02-01", "2026-02-03"),
        # A distinct seller.
        _row("ACC-4", "222", "S", 30, "2026-02-10", "2026-02-11"),
        # A universal-regime buy — passes through, never deduped.
        _row("ACC-5", "333", "P", 10, "2026-02-15", "2026-02-16", regime="universal"),
    ]
    path = _fresh_db(rows)
    con = q.connect_ro(path)
    res = q.q_ownership_pressure(con, target="all", window=90, anchor="2026-03-31")
    assert len(res["rows"]) == 1, res["rows"]
    r = res["rows"][0]
    # 100 (ACC-1/ACC-2 collapsed) + 50 (ACC-3) + 10 (ACC-5) = 160, NOT 260.
    assert r["buy_shares"] == 160.0, r
    assert r["n_buys"] == 3, r            # 4 buy rows in, 3 after dedup
    assert r["distinct_buyers"] == 2, r   # ciks 111, 333
    assert r["distinct_sellers"] == 1, r
    assert r["sell_shares"] == 30.0, r
    assert r["net_shares"] == 130.0, r
    assert r["direction"] == "accumulating", r
    os.unlink(path)


def test_sell_anomaly_distribution_first_no_verdict():
    rows = [
        # issuer AAA: 2 distinct sellers in the 90d window, 3 distinct over 12mo.
        _row("S1", "111", "S", 10, "2026-02-01", "2026-02-02", issuer_cik="1", ticker="AAA"),
        _row("S2", "222", "S", 10, "2026-02-05", "2026-02-06", issuer_cik="1", ticker="AAA"),
        _row("S3", "111", "S", 10, "2025-06-01", "2025-06-02", issuer_cik="1", ticker="AAA"),
        _row("S4", "333", "S", 10, "2025-08-01", "2025-08-02", issuer_cik="1", ticker="AAA"),
    ]
    path = _fresh_db(rows)
    con = q.connect_ro(path)
    res = q.q_sell_anomaly(con, window=90, anchor="2026-03-31")
    assert len(res["rows"]) == 1, res["rows"]
    r = res["rows"][0]
    assert r["distinct_sellers_window"] == 2, r
    assert r["distinct_sellers_12mo"] == 3, r          # n_yr on every row
    # expected = 3 * 90/365 = 0.7397; ratio = 2/0.7397 = 2.703
    assert abs(r["rate_ratio"] - 2.703) < 0.01, r
    assert r["baseline_sufficient"] is True, r          # n_yr=3 meets the >=3 floor
    assert r["elevated"] is False, r                    # 2.703 < 3.0 tint threshold
    assert "window_sell_value" in r, r                  # dollar volume alongside
    # RANKED CONTEXT FEED: no anomaly VERDICT key (elevated is a tint, allowed).
    for banned in ("anomalous", "highlight", "flag", "alert"):
        assert banned not in r, "sell feed must carry no anomaly verdict: " + banned
    d = q._distribution_report(res)
    assert d["issuers_scored"] == 1 and d["histogram_all"]["2.0-3.0"] == 1, d
    assert d["baseline_breakdown"]["n_yr=3-4"] == 1, d   # this issuer has 3 12mo sellers
    os.unlink(path)


def test_cik_int_mixed_padding_join_guard():
    # The silent zero-row-join class: registry stores zero-padded, holdings/form4
    # store zero-stripped. cik_int must collapse them so a join never misses.
    assert q.cik_int("0001536411") == 1536411
    assert q.cik_int("1536411") == 1536411
    assert q.cik_int(1536411) == 1536411
    assert q.cik_int("  1536411 ") == 1536411
    assert q.cik_int("0001536411") == q.cik_int("1536411")   # the guarantee
    assert q.cik_int(None) is None
    assert q.cik_int("not-a-cik") is None


def test_connect_ro_blocks_writes():
    path = tempfile.mktemp(suffix=".db")
    dbmod.connect(path).close()            # create schema
    con = q.connect_ro(path)
    # The structural guarantee: a write raises at the SQLite layer.
    raised = False
    try:
        con.execute("CREATE TABLE _should_fail(a)")
    except sqlite3.OperationalError as exc:
        raised = True
        assert "readonly" in str(exc).lower() or "read-only" in str(exc).lower(), exc
    assert raised, "connect_ro must reject writes"
    os.unlink(path)
