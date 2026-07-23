"""SM-A1 commonality counter tests: g3 threshold/window/registry filter, and
g1/g2 empty-corpus handling. No network."""
import datetime as dt
import os
import tempfile

from smart_money import db as dbmod
from smart_money import commonality as cm
from smart_money.overlay import Overlay

ANCHOR = "2026-06-30"
OV = Overlay(conviction=["META"], watchlist=[], min_persons=3, window_days=30)


def _mk(con, pid, name):
    con.execute(
        "INSERT OR IGNORE INTO persons(person_id, name, type, cik_or_chamber) "
        "VALUES (?,?,'congress','house')", (pid, name))


def _buy(con, pid, ticker, tx_date, lo=1001, hi=15000, ref=None):
    con.execute(
        "INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
        "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, asset_type, "
        "filing_id) VALUES (?,?,'purchase',?,?,?,?,0,'house','t',?,'Stock',?)",
        (pid, ticker, lo, hi, tx_date, tx_date, ref or (ticker + str(pid) + tx_date),
         "F" + str(pid)))


def _fresh():
    fd, p = tempfile.mkstemp(suffix=".db"); os.close(fd); return p


def test_g3_threshold_and_window():
    p = _fresh()
    try:
        con = dbmod.connect(p)
        for i in range(1, 5):
            _mk(con, i, "Rep{}".format(i))
        # AAA: 3 distinct buyers inside 90d -> qualifies
        _buy(con, 1, "AAA", "2026-06-01")
        _buy(con, 2, "AAA", "2026-06-02")
        _buy(con, 3, "AAA", "2026-06-03")
        # BBB: only 2 distinct buyers -> below threshold
        _buy(con, 1, "BBB", "2026-06-01")
        _buy(con, 2, "BBB", "2026-06-02")
        # CCC: 3 buyers but one is outside the 90d window
        _buy(con, 1, "CCC", "2026-06-01")
        _buy(con, 2, "CCC", "2026-06-02")
        _buy(con, 3, "CCC", "2026-01-01")  # >90d before anchor
        con.commit()
        res = cm.g3_congress_coholding(con, ANCHOR, OV)
        t90 = {r["ticker"]: r["member_count"] for r in res[90]}
        assert t90.get("AAA") == 3, t90
        assert "BBB" not in t90
        assert "CCC" not in t90            # third buyer out of window
        t365 = {r["ticker"]: r["member_count"] for r in res[365]}
        assert t365.get("CCC") == 3        # all three inside 365d
    finally:
        os.remove(p)


def test_g3_registry_filter():
    p = _fresh()
    try:
        con = dbmod.connect(p)
        for i in range(1, 5):
            _mk(con, i, "Rep{}".format(i))
        for i in (1, 2, 3, 4):
            _buy(con, i, "DDD", "2026-06-1{}".format(i % 10))
        con.commit()
        # universe: 4 buyers qualifies
        allr = cm.g3_congress_coholding(con, ANCHOR, OV)
        assert {r["ticker"] for r in allr[365]} == {"DDD"}
        # registry = only persons 1,2 -> below the 3 threshold
        regr = cm.g3_congress_coholding(con, ANCHOR, OV, person_filter={1, 2})
        assert regr[365] == []
    finally:
        os.remove(p)


def test_g1_g2_empty_corpus():
    p = _fresh()
    try:
        con = dbmod.connect(p)
        g1 = cm.g1_insider_convergence(con, ANCHOR, OV)
        g2 = cm.g2_cross_issuer_persons(con)
        assert g1["source_rows"] == 0
        assert all(g1["windows"][w] == [] for w in cm.G1_WINDOWS)
        assert g2["source_rows"] == 0 and g2["people"] == []
    finally:
        os.remove(p)


def test_g1_counts_discretionary_open_market_only():
    p = _fresh()
    try:
        con = dbmod.connect(p)
        # 3 distinct CIKs buy EEE discretionary open-market -> qualifies at >=2
        for cik in ("C1", "C2", "C3"):
            con.execute(
                "INSERT INTO form4_transactions(filing_ref, reporting_cik, ticker, "
                "code, plan_flag, value, tx_date, shares) VALUES (?,?,?,?,?,?,?,?)",
                (cik + "f", cik, "EEE", "P", 0, 10000, "2026-06-10", 100))
        # a plan buy and a sale must NOT count
        con.execute(
            "INSERT INTO form4_transactions(filing_ref, reporting_cik, ticker, code, "
            "plan_flag, value, tx_date, shares) VALUES ('x','C4','EEE','P',1,1,'2026-06-10',1)")
        con.execute(
            "INSERT INTO form4_transactions(filing_ref, reporting_cik, ticker, code, "
            "plan_flag, value, tx_date, shares) VALUES ('y','C5','EEE','S',0,1,'2026-06-10',1)")
        con.commit()
        g1 = cm.g1_insider_convergence(con, ANCHOR, OV)
        w90 = {r["ticker"]: r["distinct_buyers"] for r in g1["windows"][90]}
        assert w90.get("EEE") == 3, w90  # only the 3 discretionary P buyers
    finally:
        os.remove(p)
