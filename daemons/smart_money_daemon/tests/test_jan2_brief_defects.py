"""ORDER SM-JAN2 — J2, J3, J6. Each pinned to the falsehood it removes.

J6  Eight of the fifteen routes had no branch in the brief-spec dispatch, so their
    Print button rendered the FRONT brief and downloaded it as brief_<view>.pdf.
J2  'NONE' is not a ticker. The loader excludes NULL and lets the string through:
    1,724 'NONE' rows plus 'N/A' 479, 'NA' 199, '-' 28, 'None' 5, spread over 153
    distinct issuers that all rendered as a company called NONE.
J3  Ownership pressure sorted on raw share count, which ranks by how cheap a stock
    is -- MGTI's 1.75-billion-share sub-penny line led the board.
"""
import os
import tempfile

import pytest

from smart_money import brief
from smart_money import dashboard as dash
from smart_money import db as dbmod
from smart_money import queries as q

FRONT_TITLE = "Smart Money Brief"


def _db(rows=(), market_cap=()):
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    con.execute(
        "INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
        "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
        "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
        "VALUES('A',0,'Insider','1','Co','9','ZZZ','P',0,100,1.0,100,NULL,"
        "'2026-06-01','2026-06-02',NULL,'watchlist')")
    for i, (person, cik, issuer_cik, ticker, code, shares, price) in enumerate(rows, 2):
        con.execute(
            "INSERT INTO form4_transactions(accession, tx_index, reporting_person,"
            " reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares,"
            " price, value, ownership_after, tx_date, filed_date, role, ingest_regime)"
            " VALUES(?,0,?,?,'Co',?,?,?,0,?,?,?,NULL,'2026-06-01','2026-06-02',NULL,"
            "'watchlist')",
            ("ACC{}".format(i), person, cik, issuer_cik, ticker, code, shares, price,
             shares * price))
    for ticker, cik, shares in market_cap:
        con.execute("INSERT INTO market_cap(ticker, cik, shares, band,"
                    " computed_at_unix) VALUES(?,?,?,'large',0)",
                    (ticker, cik, shares))
    entries, _ = q._load_registry()
    for e in entries:
        if e.get("person_id") is not None:
            con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, "
                        "cik_or_chamber) VALUES(?,?,?,?)",
                        (e["person_id"], e["name"], "congress",
                         e.get("chamber") or "house"))
    con.commit()
    con.close()
    return path


def _pdf_text(path):
    import pdfplumber
    with pdfplumber.open(path) as doc:
        return "\n".join((pg.extract_text() or "") for pg in doc.pages)


# ------------------------------------------------------------------ J6 --

def test_every_route_either_prints_itself_or_is_not_printable():
    """THE gap. A route with no spec silently printed the front brief, so the set
    of printable views must be a decision, not an accident."""
    assert dash.PRINTABLE_VIEWS | {"/"} == set(dash.ROUTES), (
        "routes with no printing decision: %r" %
        (set(dash.ROUTES) - dash.PRINTABLE_VIEWS - {"/"},))


@pytest.mark.parametrize("path", sorted(dash.PRINTABLE_VIEWS))
def test_each_views_pdf_is_its_own_page_and_never_the_front_brief(path, tmp_path):
    db = _db()
    con = q.connect_ro(db)
    try:
        p = dash._params({"symbol": ["ZZZ"], "member": ["x"], "cmte": [""]})
        p["_path"] = path
        spec = dash._page_brief_spec(con, p, path)
        assert spec, "%s has no printable spec" % path
        out = str(tmp_path / "v.pdf")
        brief.render_page_brief(out, title=spec["title"], subtitle=spec["subtitle"],
                                tables=spec["tables"], notes=spec["notes"])
        text = _pdf_text(out)
        assert spec["title"].split(" - ")[0][:18] in text, (
            "%s: its own title is missing from its PDF" % path)
        assert FRONT_TITLE not in text, (
            "%s printed the FRONT brief under its own name" % path)
    finally:
        con.close()
        os.unlink(db)


def test_a_route_with_no_printable_surface_yields_no_spec():
    db = _db()
    con = q.connect_ro(db)
    try:
        assert dash._page_brief_spec(con, dash._params({}), "/nope") is None
        assert dash._page_brief_spec(con, dash._params({}), "/") is None
    finally:
        con.close()
        os.unlink(db)


def test_the_print_button_is_absent_where_there_is_nothing_of_its_own():
    body = dash._page("T", "<p>x</p>", dict(dash._params({}), _path="/nope"))
    assert "/brief.pdf" not in body
    shown = dash._page("T", "<p>x</p>", dict(dash._params({}), _path="/trades"))
    assert "view=trades" in shown


def test_a_multi_table_view_prints_every_table_it_draws(tmp_path):
    """Clusters draws a buy board and a sell board; a PDF with only one would be
    as misleading as one showing another page."""
    out = str(tmp_path / "m.pdf")
    brief.render_page_brief(
        out, title="Two boards", subtitle="s",
        tables=[("Buy clusters", ["ticker"], [{"ticker": "AAA"}]),
                ("Sell anomalies", ["ticker"], [{"ticker": "BBB"}])])
    text = _pdf_text(out)
    assert "Buy clusters" in text and "Sell anomalies" in text
    assert "AAA" in text and "BBB" in text


# ------------------------------------------------------------------ J2 --

@pytest.mark.parametrize("raw", ["NONE", "None", "n/a", "NA", "-", "", "  ", "?",
                                 "UNKNOWN", "null"])
def test_a_placeholder_is_not_a_symbol(raw):
    assert q.norm_ticker(raw) is None


@pytest.mark.parametrize("raw,want", [("aapl", "AAPL"), (" msft ", "MSFT"),
                                      ("BRK-B", "BRK-B")])
def test_a_real_symbol_survives(raw, want):
    assert q.norm_ticker(raw) == want


def test_unmapped_issuers_are_counted_not_named():
    db = _db(rows=[("P{}".format(i), str(100 + i), "555", "NONE", "P", 10, 5.0)
                   for i in range(4)])
    con = q.connect_ro(db)
    try:
        res = q.q_cluster_context(con, floor=3, anchor="2026-06-30", lookback=365)
        rows = [r for r in res["rows"] if r["issuer_cik"] == "555"]
        assert rows, "the cluster is real - these rows carry an issuer CIK"
        assert rows[0]["unmapped"] is True
        assert rows[0]["ticker"] == "unmapped (CIK 555)", rows[0]["ticker"]
        assert "NONE" not in rows[0]["ticker"]
        assert res["unmapped_clusters"] >= 1
    finally:
        con.close()
        os.unlink(db)


def test_two_unmapped_issuers_stay_two_clusters():
    """They share a placeholder, not an identity."""
    rows = [("P{}".format(i), str(200 + i), "601", "NONE", "P", 10, 5.0)
            for i in range(3)]
    rows += [("Q{}".format(i), str(300 + i), "602", "N/A", "P", 10, 5.0)
             for i in range(3)]
    db = _db(rows=rows)
    con = q.connect_ro(db)
    try:
        res = q.q_cluster_context(con, floor=3, anchor="2026-06-30", lookback=365)
        got = {r["issuer_cik"] for r in res["rows"] if r["unmapped"]}
        assert got == {"601", "602"}, got
    finally:
        con.close()
        os.unlink(db)


# ------------------------------------------------------------------ J3 --

def test_pressure_ranks_on_dollars_not_raw_shares():
    """THE regression. A 1.75-billion-share sub-penny line outranked a real
    purchase because the sort was on share count."""
    rows = [("Penny", "11", "900", "MGTI", "P", 1_750_000_000, 0.0001),   # $175k
            ("Real", "12", "901", "BIGCO", "P", 100_000, 400.0)]          # $40m
    db = _db(rows=rows)
    con = q.connect_ro(db)
    try:
        res = q.q_ownership_pressure(con, window=3650, anchor="2026-06-30")
        order = [r["ticker"] for r in res["rows"]]
        assert order.index("BIGCO") < order.index("MGTI"), order
        mg = [r for r in res["rows"] if r["ticker"] == "MGTI"][0]
        assert mg["net_shares"] == 1_750_000_000, "share count stays a COLUMN"
        assert mg["net_value"] == pytest.approx(175_000.0)
    finally:
        con.close()
        os.unlink(db)


def test_pressure_reports_share_of_float_where_known_and_none_otherwise():
    db = _db(rows=[("A", "21", "910", "AAA", "P", 1_000, 10.0),
                   ("B", "22", "911", "BBB", "P", 1_000, 10.0)],
             market_cap=[("AAA", "910", 100_000)])
    con = q.connect_ro(db)
    try:
        by = {r["ticker"]: r for r in
              q.q_ownership_pressure(con, window=3650, anchor="2026-06-30")["rows"]}
        assert by["AAA"]["pct_shares_outstanding"] == pytest.approx(1.0)
        assert by["BBB"]["pct_shares_outstanding"] is None, "never a guessed float"
    finally:
        con.close()
        os.unlink(db)


def test_direction_falls_back_to_shares_when_nothing_was_priced():
    """Some regimes carry shares and no price. Calling a real accumulation 'flat'
    because the filing priced nothing would swap one falsehood for another."""
    db = _db(rows=[("A", "41", "930", "DDD", "P", 500, 0.0)])
    con = q.connect_ro(db)
    try:
        row = [r for r in q.q_ownership_pressure(
            con, window=3650, anchor="2026-06-30")["rows"]
            if r["issuer_cik"] == "930"][0]
        assert row["net_value"] == 0.0 and row["net_shares"] == 500
        assert row["direction"] == "accumulating"
        assert row["direction_basis"] == "shares"
    finally:
        con.close()
        os.unlink(db)


def test_pressure_direction_follows_dollars():
    """Buying 10 shares at $1,000 and selling 1,000 at $1 is accumulation in
    dollars and distribution in share count. Dollars decide."""
    db = _db(rows=[("A", "31", "920", "CCC", "P", 10, 1000.0),
                   ("B", "32", "920", "CCC", "S", 1000, 1.0)])
    con = q.connect_ro(db)
    try:
        row = [r for r in q.q_ownership_pressure(
            con, window=3650, anchor="2026-06-30")["rows"]
            if r["issuer_cik"] == "920"][0]
        assert row["net_value"] == pytest.approx(9_000.0)
        assert row["net_shares"] == -990
        assert row["direction"] == "accumulating"
    finally:
        con.close()
        os.unlink(db)
