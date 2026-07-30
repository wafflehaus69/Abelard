"""SM-R1 P2 query-layer tests: flow-based ownership pressure (with the Form 4
amendment-supersede case), distribution-first sell-anomaly, the mixed-padding
CIK regression guard, and the structural read-only guarantee."""
import json
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


def test_insider_trades_dedup_dates_plan_smid_price():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    # Same economic buy under two accessions in the UNIVERSAL regime — the
    # regime-agnostic dedup must collapse them to the latest filing.
    con.execute(_F4_INSERT, _row("A1", "111", "P", 100, "2026-06-01", "2026-06-02",
                                 regime="universal", ticker="ZZZ", issuer_cik="9"))
    con.execute(_F4_INSERT, _row("A2", "111", "P", 100, "2026-06-01", "2026-06-03",
                                 regime="universal", ticker="ZZZ", issuer_cik="9"))
    # A 10b5-1 planned sell.
    con.execute(_F4_INSERT, _row("A3", "222", "S", 50, "2026-06-05", "2026-06-06",
                                 plan_flag=1, ticker="ZZZ", issuer_cik="9"))
    con.executemany(
        "INSERT INTO prices(ticker, date, close, adj_close, price_type, asof_unix, "
        "fetched_at_unix, source) VALUES(?,?,?,?,?,?,?,?)",
        [("ZZZ", "2026-06-01", 10.0, 10.0, "eod", 0, 0, "y"),
         ("ZZZ", "2026-06-26", 12.0, 12.0, "eod", 0, 0, "y")])
    con.execute("INSERT INTO market_cap(ticker, cik, shares, band, computed_at_unix) "
                "VALUES('ZZZ','9',100,'small',0)")
    con.commit()
    con.close()
    con = q.connect_ro(path)
    # ZZZ is not a scoped ticker, so the enrichment assertions use scope='all'.
    buys = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                              plan="all", scope="all")
    assert len(buys["rows"]) == 1, buys["rows"]          # A1/A2 collapsed
    t = buys["rows"][0]
    assert t["trade_date"] == "2026-06-01", t
    assert t["reported_date"] == "2026-06-03", t           # latest filing kept
    assert t["lag_days"] == 2, t
    assert t["entry_close"] == 10.0 and t["latest_close"] == 12.0, t
    assert abs(t["pct_since_trade"] - 0.2) < 1e-9, t        # +20%
    assert t["smid_band"] == "small" and t["plan_10b5_1"] is False, t
    assert t["provenance"] is None, "ZZZ is in no overlay set -> no provenance tag"
    planned = q.q_insider_trades(con, side="sell", window=120, anchor="2026-06-30",
                                 plan="planned", scope="all")
    assert len(planned["rows"]) == 1 and planned["rows"][0]["plan_10b5_1"] is True, planned
    disc = q.q_insider_trades(con, side="sell", window=120, anchor="2026-06-30",
                              plan="discretionary", scope="all")
    assert len(disc["rows"]) == 0, "the only sell is planned"
    smid = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                              plan="all", smid_only=True, scope="all")
    assert len(smid["rows"]) == 1, "ZZZ is small-cap"
    # scope='scoped' (default) filters ZZZ out — it is not an overlay/tn ticker.
    scoped = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                                plan="all", scope="scoped")
    assert len(scoped["rows"]) == 0, "ZZZ is out of the watchlist scope"
    os.unlink(path)


def test_insider_trades_provenance_and_scope():
    # DJT is in trump_network, PLTR in thiel_network, ZZZ in no overlay set. The
    # provenance tag and the scoped filter both come from overlay.yaml.
    path = _fresh_db([
        _row("D1", "10", "P", 5, "2026-06-01", "2026-06-02", ticker="DJT", issuer_cik="1"),
        _row("T1", "20", "P", 5, "2026-06-01", "2026-06-02", ticker="PLTR", issuer_cik="2"),
        _row("Z1", "30", "P", 5, "2026-06-01", "2026-06-02", ticker="ZZZ", issuer_cik="3"),
    ])
    con = q.connect_ro(path)
    prov = {r["ticker"]: r["provenance"]
            for r in q.q_insider_trades(con, side="buy", window=120,
                                        anchor="2026-06-30", scope="all")["rows"]}
    assert prov.get("DJT") == "trump", prov
    assert prov.get("PLTR") == "thiel", prov
    assert prov.get("ZZZ") is None, prov
    # scoped view keeps the network tickers, drops the unscoped one.
    scoped = {r["ticker"] for r in q.q_insider_trades(
        con, side="buy", window=120, anchor="2026-06-30", scope="scoped")["rows"]}
    assert "DJT" in scoped and "PLTR" in scoped and "ZZZ" not in scoped, scoped
    con.close()
    os.unlink(path)


def test_insider_trades_pagination_and_full():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    for i in range(5):  # 5 distinct buys (distinct shares -> no dedup), dated 10..14
        con.execute(_F4_INSERT, _row("P{}".format(i), str(100 + i), "P", 10 + i,
                                     "2026-06-{:02d}".format(10 + i),
                                     "2026-06-{:02d}".format(11 + i),
                                     regime="universal", ticker="ZZZ", issuer_cik="9"))
    con.commit()
    con.close()
    con = q.connect_ro(path)
    r1 = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                            plan="all", scope="all", per_page=2, page=1)
    assert r1["total_matching"] == 5 and r1["per_page"] == 2 and r1["pages"] == 3, r1
    assert len(r1["rows"]) == 2, r1["rows"]
    r3 = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                            plan="all", scope="all", per_page=2, page=3)
    assert len(r3["rows"]) == 1, "last page holds the remainder"
    assert r1["rows"][0]["trade_date"] > r3["rows"][0]["trade_date"], "newest first"
    full = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                              plan="all", scope="all", per_page=2, full=True)
    assert len(full["rows"]) == 5, "full returns every row regardless of per_page"
    # An over-range page clamps to the last page (matches dashboard _page_slice) —
    # never an empty "page 999 of 3" table.
    over = q.q_insider_trades(con, side="buy", window=120, anchor="2026-06-30",
                              plan="all", scope="all", per_page=2, page=999)
    assert over["page"] == over["pages"] == 3 and len(over["rows"]) == 1, over
    os.unlink(path)


def test_insider_trades_sort_by_column():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    FI = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
          "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
          "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
          "VALUES(?,0,?,?,'Co','9','ZZZ','P',0,?,1.0,?,NULL,?,?,NULL,'watchlist')")
    for acc, cik, val, tx in [("s1", "p1", 100, "2026-06-10"),
                              ("s2", "p2", 900, "2026-06-11"),
                              ("s3", "p3", 400, "2026-06-12")]:
        con.execute(FI, (acc, cik, cik, val, val, tx, tx))
    con.commit()
    con.close()
    con = q.connect_ro(path)
    kw = dict(side="buy", window=120, anchor="2026-06-30", scope="all")
    desc = q.q_insider_trades(con, sort="value", direction="desc", **kw)
    assert [r["value"] for r in desc["rows"]] == [900, 400, 100], desc["rows"]
    asc = q.q_insider_trades(con, sort="value", direction="asc", **kw)
    assert [r["value"] for r in asc["rows"]] == [100, 400, 900]
    # an unknown sort key falls back to the default (newest trade first)
    dflt = q.q_insider_trades(con, sort="bogus", **kw)
    assert [r["trade_date"] for r in dflt["rows"]] == ["2026-06-12", "2026-06-11", "2026-06-10"]
    con.close()
    os.unlink(path)


def test_net_flows_nested_windows_and_metrics():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    FI = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
          "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, "
          "price, value, ownership_after, tx_date, filed_date, role, ingest_regime) "
          "VALUES(?,0,?,?,'Co',?,?,?,0,?,?,?,NULL,?,?,NULL,'watchlist')")

    def ins(acc, cik, code, sh, val, tk, tx):
        con.execute(FI, (acc, "P" + cik, cik, cik, tk, code, sh,
                         (val / sh if sh else 0.0), val, tx, tx))
    ins("a1", "111", "P", 100, 1000, "AAA", "2026-06-20")   # 10d before anchor: buy
    ins("a2", "222", "S", 40, 400, "AAA", "2026-06-25")     # 5d: sell, other person
    ins("a3", "333", "P", 50, 500, "AAA", "2026-01-15")     # ~166d: buy -> 180/365/all
    ins("a4", "444", "P", 10, 100, "AAA", "2024-06-30")     # ~730d: buy -> all-time only
    ins("b1", "555", "P", 5, 50, "BBB", "2026-06-20")       # small, tests sort order
    ins("z1", "666", "P", 1, 10, "NONE", "2026-06-20")      # non-security -> excluded
    con.commit()
    con.close()
    con = q.connect_ro(path)
    res = q.q_net_flows(con, anchor="2026-06-30", scope="all")
    rows = {r["ticker"]: r for r in res["rows"]}
    assert "NONE" not in rows and "-" not in rows, "non-securities excluded"
    a = rows["AAA"]
    assert a["value_30"] == 600 and a["value_90"] == 600, a               # 1000 - 400
    assert a["value_180"] == 1100 and a["value_365"] == 1100, a           # + Jan buy
    assert a["value_all"] == 1200, a                                       # + 2yr-old buy
    assert a["shares_30"] == 60 and a["shares_180"] == 110 and a["shares_all"] == 120, a
    assert a["persons_30"] == 0, a          # one buyer, one seller -> net 0
    assert a["persons_180"] == 1 and a["persons_all"] == 2, a
    # 7-day window: only the 2026-06-25 sell falls inside (buy is 10 days out)
    assert a["value_7"] == -400 and a["shares_7"] == -40 and a["persons_7"] == -1, a
    assert res["rows"][0]["ticker"] == "AAA", "default sort by all-time net insiders desc"
    sc = q.q_net_flows(con, anchor="2026-06-30", scope="scoped")
    assert all(r["ticker"] not in ("AAA", "BBB") for r in sc["rows"]), "overlay scope drops them"
    con.close()
    os.unlink(path)


def test_net_flows_price_sanity_guard():
    # A corrupt Form 4 row (per-share price in the millions -> value = shares*price is
    # garbage) is dropped from BOTH net $ and net shares; only net insiders keeps it,
    # since the filer's identity is not corrupt.
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    FI = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
          "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, "
          "price, value, ownership_after, tx_date, filed_date, role, ingest_regime) "
          "VALUES(?,0,?,?,'Co',?,?,?,0,?,?,?,NULL,?,?,NULL,'watchlist')")

    def ins(acc, cik, code, sh, price, val, tk, tx):
        con.execute(FI, (acc, "P" + cik, cik, cik, tk, code, sh, price, val, tx, tx))
    ins("c1", "111", "P", 100, 10.0, 1000, "CCC", "2026-06-20")          # clean
    ins("c2", "222", "P", 40000000, 40000000.0, 1.6e15, "CCC", "2026-06-21")  # corrupt price
    # corrupt shares with a plausible price -> value backstop AND share cap catch it
    ins("c3", "333", "P", 20000000000, 8.0, 1.6e11, "CCC", "2026-06-22")
    con.commit()
    con.close()
    con = q.connect_ro(path)
    res = q.q_net_flows(con, anchor="2026-06-30", scope="all")
    assert res["rows_excluded"] == 2, res["rows_excluded"]
    r = {x["ticker"]: x for x in res["rows"]}["CCC"]
    assert r["value_all"] == 1000, r["value_all"]            # both corrupt $ dropped
    assert r["shares_all"] == 100, r["shares_all"]           # corrupt shares dropped too
    assert r["persons_all"] == 3, r["persons_all"]           # all three buyers counted
    con.close()
    os.unlink(path)


def test_portfolio_holdings_and_qoq_deltas():
    # Duquesne (0001536411) is a manager_13f filer in the committed registry.
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    HI = ("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
          "ticker, issuer, put_call, value, shares, ingested_at_unix) "
          "VALUES('1536411',?,?,?,?,?,?,?,?,?,0)")

    def ins(acc, period, cusip, ticker, pc, val, sh):
        con.execute(HI, (acc, period, period, cusip, ticker, ticker or "Iss", pc, val, sh))
    ins("p1", "2025-12-31", "CU_A", "AAA", "long", 1000, 100)
    ins("p1", "2025-12-31", "CU_B", "BBB", "long", 500, 50)
    ins("p1", "2025-12-31", "CU_D", "DDD", "long", 100, 10)     # will exit
    ins("c1", "2026-03-31", "CU_A", "AAA", "long", 1500, 150)   # added (shares up)
    ins("c1", "2026-03-31", "CU_B", "BBB", "long", 400, 40)     # trimmed (shares down)
    ins("c1", "2026-03-31", "CU_C", "CCC", "long", 200, 20)     # new
    ins("c1", "2026-03-31", "CU_E", "EEE", "put", 300, 0)       # new put (put-heavy)
    ins("c1", "2026-03-31", "CU_F", None, "long", 250, 25)      # unmapped CUSIP
    con.commit()
    con.close()
    con = q.connect_ro(path)
    res = q.q_portfolio(con, filer_cik="0001536411", period="2026-03-31")
    assert q.cik_int(res["filer_cik"]) == 1536411 and res["period"] == "2026-03-31"
    assert res["prior_period"] == "2025-12-31" and res["has_deltas"] is True
    assert res["book_value"] == 2650, res["book_value"]
    assert res["long_value"] == 2350 and res["put_notional"] == 300 and res["call_notional"] == 0
    assert res["unmapped_count"] == 1 and res["unmapped_value"] == 250
    byk = {(r["cusip"], r["instrument"]): r for r in res["rows"]}
    assert byk[("CU_A", "SH")]["badge"] == "added"
    assert byk[("CU_B", "SH")]["badge"] == "trimmed"
    assert byk[("CU_C", "SH")]["badge"] == "new"
    assert byk[("CU_E", "PUT")]["badge"] == "new", "put-heavy new position"
    assert byk[("CU_D", "SH")]["badge"] == "exited" and byk[("CU_D", "SH")]["value"] == 0
    assert byk[("CU_F", "SH")]["unmapped"] is True and byk[("CU_F", "SH")]["ticker"] is None
    assert abs(byk[("CU_A", "SH")]["pct_of_book"] - round(100 * 1500 / 2650, 2)) < 0.01
    # single filing (prior=None) -> no deltas
    solo = q.q_portfolio(con, filer_cik="0001536411", period="2025-12-31")
    assert solo["prior_period"] is None and solo["has_deltas"] is False
    assert all(r["badge"] is None for r in solo["rows"]), "no badges without a prior period"
    con.close()
    os.unlink(path)


def test_portfolio_value_unit_normalization():
    # 13F value units differ BY FILER but are consistent WITHIN a filer. The scale is
    # detected once per filer from the newest price-covered period and applied to ALL
    # periods, so an older period with no price coverage is not misread as dollars.
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    HI = ("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
          "ticker, issuer, put_call, value, shares, ingested_at_unix) "
          "VALUES(?,?,?,?,?,?,?,'long',?,?,0)")
    PX = "INSERT INTO prices VALUES(?,?,?,?,'eod',0,0,'y')"
    # thousands filer 111: latest 2026-03-31 is priced; older 2024-09-30 has NO price
    con.execute(HI, ("111", "x1", "2026-03-31", "2026-03-31", "CU_A", "AAA", "Aco", 150, 1000))
    con.execute(HI, ("111", "x0", "2024-09-30", "2024-09-30", "CU_A", "AAA", "Aco", 120, 900))
    con.execute(PX, ("AAA", "2026-03-31", 150, 150))          # only the latest period priced
    # dollars filer 222
    con.execute(HI, ("222", "dy", "2026-03-31", "2026-03-31", "CU_B", "BBB", "Bco", 150000, 1000))
    con.execute(PX, ("BBB", "2026-03-31", 150, 150))
    con.commit()
    con.close()
    con = q.connect_ro(path)
    # scale detected from the newest covered period, applied filer-wide
    assert q._filer_unit_scale(con, "111", ["2026-03-31", "2024-09-30"]) == 1000, "thousands"
    assert q._filer_unit_scale(con, "222", ["2026-03-31"]) == 1, "dollars"
    # the OLD uncovered period is still scaled x1000 (filer-level scale, not per-period)
    assert q._scaled_holdings(con, "111", "2024-09-30", 1000)[("CU_A", "long")]["value"] == 120000
    assert q._scaled_holdings(con, "111", "2026-03-31", 1000)[("CU_A", "long")]["value"] == 150000
    con.close()
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


def test_positioning_events_from_envelope_flagged_first():
    d = tempfile.mkdtemp()
    env = {"scan_id": "scan_100", "events": [
        {"event_id": "a", "leg": "form4", "ticker": "AAA", "tx_date": "2026-02-01",
         "disclosure_date": "2026-02-02", "flags": {"conviction_overlay": False,
          "watchlist_overlay": False, "cluster": None, "sentinel": None}},
        {"event_id": "b", "leg": "congress", "ticker": "BBB", "tx_date": "2026-01-01",
         "disclosure_date": "2026-01-15", "flags": {"conviction_overlay": True,
          "watchlist_overlay": False, "cluster": None, "sentinel": None}},
    ]}
    with open(os.path.join(d, "scan_100.json"), "w", encoding="utf-8") as fh:
        json.dump(env, fh)
    res = q.q_positioning_events(scans_dir=d)
    assert res["count"] == 2, res
    assert res["rows"][0]["event_id"] == "b", res["rows"]   # flagged sorts first
    # since filter drops the older event
    res2 = q.q_positioning_events(since="2026-02-01", scans_dir=d)
    assert res2["count"] == 1 and res2["rows"][0]["event_id"] == "a", res2


def test_sentinel_log_cik_cast_match_and_orphan_failloud():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    # 13F row stores the CIK zero-STRIPPED; registry seed is zero-PADDED.
    con.execute("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
                "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
                "VALUES('1536411','ACC','2026-03-31','2026-05-10','C','NVDA','Nvidia',"
                "'long',100,10,0)")
    con.commit(); con.close()
    con = q.connect_ro(path)
    entries = [{"person_id": None, "name": "Duquesne", "cik": "0001536411",
                "role": "manager_13f"}]
    res = q.q_sentinel_log(con, window=3650, anchor="2026-07-01", entries=entries)
    assert res["count"] == 1 and res["rows"][0]["ticker"] == "NVDA", res  # CAST matched
    # An orphaned Shape-A person_id must fail loud, never silently return nothing.
    raised = False
    try:
        q.q_sentinel_log(con, entries=[{"person_id": 99999, "name": "Ghost",
                                        "role": "qualitative_watch"}])
    except q.QueryError:
        raised = True
    assert raised, "orphaned registry person_id must raise QueryError"
    os.unlink(path)


def test_principal_convergence_qoq_cross_manager_pairing():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    ins = ("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
           "ticker, issuer, put_call, value, shares, ingested_at_unix) VALUES"
           "(?,?,?,?,?,?,?,?,?,?,0)")
    # filer 1 adds X into 2025-06-30 (held Y before); filer 2 exits X (holds Y after).
    con.executemany(ins, [
        ("1", "A1", "2025-03-31", "2025-05-01", "cy", "Y", "Yco", "long", 50, 5),
        ("1", "A2", "2025-06-30", "2025-08-01", "cx", "X", "Xco", "long", 100, 10),
        ("2", "B1", "2025-03-31", "2025-05-01", "cx", "X", "Xco", "long", 80, 8),
        ("2", "B2", "2025-06-30", "2025-08-01", "cy", "Y", "Yco", "long", 60, 6),
    ])
    con.commit(); con.close()
    con = q.connect_ro(path)
    res = q.q_principal_convergence(con)
    qoq = res["qoq_accumulate_distribute_disagreements"]
    hit = [r for r in qoq if r["ticker"] == "X" and r["period"] == "2025-06-30"]
    assert hit, qoq
    assert hit[0]["accumulating_ciks"] == ["1"] and hit[0]["distributing_ciks"] == ["2"], hit
    # the 'short' label must never leak — it is renamed put-heavy
    for c in res["convergences"]:
        assert c["converge_dir"] != "short" and "short_filers" not in c, c
    os.unlink(path)


def test_cluster_context_capitulation_flag():
    rows = [
        # 3 distinct buyers on ZZZ all within one month -> cluster, capitulation.
        _row("C1", "1", "P", 10, "2026-02-02", "2026-02-03", issuer_cik="9", ticker="ZZZ"),
        _row("C2", "2", "P", 10, "2026-02-10", "2026-02-11", issuer_cik="9", ticker="ZZZ"),
        _row("C3", "3", "P", 10, "2026-02-20", "2026-02-21", issuer_cik="9", ticker="ZZZ"),
        # only 2 distinct buyers on WWW -> below floor 3, no cluster.
        _row("D1", "4", "P", 10, "2026-02-02", "2026-02-03", issuer_cik="8", ticker="WWW"),
        _row("D2", "5", "P", 10, "2026-02-10", "2026-02-11", issuer_cik="8", ticker="WWW"),
    ]
    path = _fresh_db(rows)
    con = q.connect_ro(path)
    res = q.q_cluster_context(con, window=180, floor=3, anchor="2026-03-31")
    assert res["count"] == 1, res["rows"]
    c = res["rows"][0]
    assert c["ticker"] == "ZZZ" and c["n_buyers"] == 3, c
    assert c["capitulation"] is True and c["calendar_months"] == ["2026-02"], c
    os.unlink(path)


def test_ticker_panel_assembles_surfaces():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    con.execute(_F4_INSERT, _row("T1", "1", "P", 100, "2026-06-01", "2026-06-02",
                                 issuer_cik="5", ticker="TTT"))
    con.execute("INSERT INTO persons(person_id, name, type, cik_or_chamber) "
                "VALUES(1,'Doe, John','congress','house')")
    con.execute("INSERT INTO congress_trades(person_id, ticker, side, amt_low, "
                "amt_high, tx_date, disclosure_date, lag_days, chamber, source, "
                "raw_ref, filing_id, asset_type, superseded) VALUES(1,'TTT',"
                "'purchase',1000,15000,'2026-06-05','2026-06-20',15,'house','efd',"
                "'r1',1,'Stock',0)")
    con.execute("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
                "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
                "VALUES('5','Z','2026-03-31','2026-05-10','c','TTT','Tco','long',500,50,0)")
    con.executemany("INSERT INTO prices(ticker, date, close, adj_close, price_type, "
                    "asof_unix, fetched_at_unix, source) VALUES(?,?,?,?,?,?,?,?)", [
        ("TTT", "2026-06-01", 10.0, 10.0, "eod", 0, 0, "y"),
        ("TTT", "2026-06-02", 11.0, 11.0, "eod", 0, 0, "y")])
    con.commit(); con.close()
    con = q.connect_ro(path)
    p = q.q_ticker_panel(con, "ttt", anchor="2026-07-01")
    assert p["ticker"] == "TTT", p
    assert p["insider_by_code"] and p["insider_by_code"][0]["code"] == "P", p
    assert len(p["congress"]) == 1 and p["congress"][0]["side"] == "purchase", p
    assert len(p["thirteenf_net"]) == 1 and p["thirteenf_net"][0]["net_value"] == 500, p
    assert len(p["price_sparkline"]) == 2, p
    assert "conviction" in p["overlay"], p
    os.unlink(path)
