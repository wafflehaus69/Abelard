"""SM-C3 Phase Y: YoY breadth deltas.

The load-bearing property is that a filing-calendar artifact must not read as a trade.
Members file annuals on extensions, so a year is partially filed for months; differencing
raw holder counts turns that into a fake mass exit. These fixtures pin the split.
"""
import os
import tempfile

from smart_money import db as dbmod, queries as q

_H = ("INSERT INTO congress_holdings(doc_id, chamber, coverage_year, filing_date, "
      "member_last, member_first, state_dist, row_idx, asset_name, ticker, asset_type, "
      "owner, value_lo, value_hi, ingested_at_unix) "
      "VALUES(?,?,?,?,?,'A',?,?,'Asset',?,?,'Self',?,?,0)")
_R = ("INSERT OR REPLACE INTO congress_member_roster(chamber, member_last, member_first, "
      "state_dist, party, state, match_kind, synced_at_unix) VALUES(?,?,'A',?,?,'NC',"
      "'unique',0)")


def _db():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return p


def _seed(con, rows, roster=True):
    """rows: (chamber, year, last, state_dist, ticker[, atype, lo, hi])"""
    seen = set()
    for i, r in enumerate(rows):
        cham, yr, last, sd, tk = r[:5]
        atype = r[5] if len(r) > 5 else "ST"
        lo = r[6] if len(r) > 6 else 1001
        hi = r[7] if len(r) > 7 else 15000
        con.execute(_H, ("d%d" % i, cham, yr, "%d-05-15" % (yr + 1), last, sd, i,
                         tk, atype, lo, hi))
        if roster and (cham, last, sd) not in seen:
            seen.add((cham, last, sd))
            con.execute(_R, (cham, last, sd, "Republican"))
    con.commit()


def _run(rows, **kw):
    p = _db()
    con = dbmod.connect(p)
    _seed(con, rows)
    con.close()
    ro = q.connect_ro(p)
    try:
        return q.q_congress_breadth_yoy(ro, **kw)
    finally:
        ro.close()
        os.unlink(p)


def _row(res, ticker):
    for r in res["rows"]:
        if r["ticker"] == ticker:
            return r
    raise AssertionError("no %s in %s" % (ticker, [r["ticker"] for r in res["rows"]]))


def test_a_member_who_did_not_file_this_year_is_not_an_exit():
    """THE artifact. Bravo filed CY2024 and has not yet filed CY2025 (extensions run
    months past the May due date). Their AAPL position must not read as a sale."""
    res = _run([("house", 2024, "Alpha", "NC01", "AAPL"),
                ("house", 2024, "Bravo", "NC02", "AAPL"),
                ("house", 2025, "Alpha", "NC01", "AAPL")], year=2025, prior=2024)
    r = _row(res, "AAPL")
    assert r["delta_total"] == -1, "the raw difference does show the drop"
    assert r["delta_comparable"] == 0, "but no member who filed both years sold"
    assert r["exited_members"] == 0
    assert res["population"]["left"] == 1 and res["population"]["both"] == 1


def test_a_newly_filing_member_is_not_a_new_position():
    """Mirror case: a member's first annual makes everything they hold look bought."""
    res = _run([("house", 2024, "Alpha", "NC01", "AAPL"),
                ("house", 2025, "Alpha", "NC01", "AAPL"),
                ("house", 2025, "Charlie", "NC03", "AAPL")], year=2025, prior=2024)
    r = _row(res, "AAPL")
    assert (r["delta_total"], r["delta_comparable"]) == (1, 0)
    assert r["new_members"] == 0
    assert res["population"]["entered"] == 1


def test_a_real_buy_by_a_both_years_filer_counts():
    res = _run([("house", 2024, "Alpha", "NC01", "AAPL"),
                ("house", 2025, "Alpha", "NC01", "AAPL"),
                ("house", 2024, "Bravo", "NC02", "MSFT"),
                ("house", 2025, "Bravo", "NC02", "MSFT"),
                ("house", 2025, "Bravo", "NC02", "AAPL")], year=2025, prior=2024)
    r = _row(res, "AAPL")
    assert r["delta_comparable"] == 1 and r["new_members"] == 1
    assert r["exited_members"] == 0


def test_a_real_sale_by_a_both_years_filer_counts():
    res = _run([("house", 2024, "Alpha", "NC01", "AAPL"),
                ("house", 2024, "Alpha", "NC01", "MSFT"),
                ("house", 2025, "Alpha", "NC01", "MSFT")], year=2025, prior=2024)
    r = _row(res, "AAPL")
    assert r["delta_comparable"] == -1 and r["exited_members"] == 1


def test_a_new_badge_from_a_sub_bar_member_is_low_confidence():
    """Mando's binding condition. Alpha's CY2024 filing has 3 unreadable equity rows, so
    their CY2025 'new' AAPL may be a position we simply could not read last year."""
    rows = [("house", 2024, "Alpha", "NC01", "MSFT"),
            ("house", 2025, "Alpha", "NC01", "MSFT"),
            ("house", 2025, "Alpha", "NC01", "AAPL")]
    rows += [("house", 2024, "Alpha", "NC01", None) for _ in range(3)]
    res = _run(rows, year=2025, prior=2024)
    r = _row(res, "AAPL")
    assert r["delta_comparable"] == 1, "the delta is still reported"
    assert r["confidence"] == "low" and r["new_low_conf"] == 1
    assert any("2024" in w for w in r["confidence_why"]), r["confidence_why"]


def test_confidence_is_per_member_not_per_chamber():
    """A chamber-level flag fires on every row in the corpus and discriminates nothing.
    Bravo filed CY2024 cleanly, so Bravo's new position is a clean claim even though a
    chamber-mate (Alpha) dragged the CY2024 house cell under the bar."""
    rows = [("house", 2024, "Alpha", "NC01", "MSFT"),
            ("house", 2025, "Alpha", "NC01", "MSFT"),
            ("house", 2025, "Alpha", "NC01", "AAPL"),      # soft
            ("house", 2024, "Bravo", "NC02", "MSFT"),
            ("house", 2025, "Bravo", "NC02", "MSFT"),
            ("house", 2025, "Bravo", "NC02", "TSLA")]      # clean
    rows += [("house", 2024, "Alpha", "NC01", None) for _ in range(6)]
    res = _run(rows, year=2025, prior=2024)
    assert res["sub_bar_cells"], "the chamber cell IS under the bar"
    assert _row(res, "AAPL")["confidence"] == "low"
    assert _row(res, "TSLA")["confidence"] == "ok", "a clean filer keeps a clean badge"
    assert res["low_confidence_rows"] == 1


def test_capture_at_or_above_the_bar_is_ok_confidence():
    res = _run([("house", 2024, "Alpha", "NC01", "MSFT"),
                ("house", 2025, "Alpha", "NC01", "MSFT"),
                ("house", 2025, "Alpha", "NC01", "AAPL")], year=2025, prior=2024)
    assert _row(res, "AAPL")["confidence"] == "ok"
    assert res["low_confidence_rows"] == 0 and res["sub_bar_cells"] == []


def test_asset_types_are_reported_verbatim_not_classified():
    """The filer's own EF code travels with the row so the head of the distribution can
    be read for what it is. The query does NOT decide which asset classes matter."""
    res = _run([("house", 2024, "Alpha", "NC01", "IVV", "EF"),
                ("house", 2025, "Alpha", "NC01", "IVV", "EF"),
                ("house", 2025, "Bravo", "NC02", "IVV", "EF"),
                ("house", 2024, "Bravo", "NC02", "MSFT", "ST"),
                ("house", 2025, "Bravo", "NC02", "MSFT", "ST")], year=2025, prior=2024)
    assert _row(res, "IVV")["asset_types"] == {"EF": 2}
    assert _row(res, "MSFT")["asset_types"] == {"ST": 1}


def test_distribution_is_returned_and_carries_no_threshold():
    """Distribution-first: Phase Y ships the shape, NOT a cut. A watch-cut must not
    exist until the distribution has been seen and a bar set on it."""
    rows = []
    for i in range(6):                                  # six both-years members
        rows += [("house", 2024, "M%d" % i, "NC%02d" % i, "MSFT"),
                 ("house", 2025, "M%d" % i, "NC%02d" % i, "MSFT")]
    for i in range(4):                                  # four of them add AAPL
        rows.append(("house", 2025, "M%d" % i, "NC%02d" % i, "AAPL"))
    res = _run(rows, year=2025, prior=2024)
    assert res["distribution"]["delta_comparable"]["+3..+4"] == 1
    assert res["distribution"]["delta_comparable"]["0"] == 1     # MSFT unchanged
    assert sum(res["distribution"]["delta_comparable"].values()) == res["count"]
    assert "threshold" not in res and "watch" not in res
    for r in res["rows"]:
        assert "watch" not in r and "flag" not in r


def test_house_and_senate_members_never_collapse():
    """Senate state_dist is NULL, so chamber must be part of the identity."""
    res = _run([("house", 2024, "Smith", "NC01", "AAPL"),
                ("senate", 2024, "Smith", None, "AAPL"),
                ("house", 2025, "Smith", "NC01", "AAPL"),
                ("senate", 2025, "Smith", None, "AAPL")], year=2025, prior=2024)
    r = _row(res, "AAPL")
    assert r["holders_both_year"] == 2 and r["delta_comparable"] == 0
    assert res["population"]["house"]["both"] == 1
    assert res["population"]["senate"]["both"] == 1


def test_options_stay_distinct_from_stock():
    res = _run([("house", 2024, "Alpha", "NC01", "GOOGL", "ST"),
                ("house", 2025, "Alpha", "NC01", "GOOGL", "ST"),
                ("house", 2025, "Alpha", "NC01", "GOOGL", "OP")], year=2025, prior=2024)
    inst = sorted(r["instrument"] for r in res["rows"] if r["ticker"] == "GOOGL")
    assert inst == ["OP", "SH"], inst


def test_unbounded_top_band_contributes_its_floor():
    """'over $50,000,000' has no midpoint. Inventing a ceiling would put a number in the
    filer's mouth."""
    res = _run([("house", 2024, "Alpha", "NC01", "AAPL", "ST", 50000001, None),
                ("house", 2025, "Alpha", "NC01", "AAPL", "ST", 50000001, None)],
               year=2025, prior=2024)
    assert _row(res, "AAPL")["floor_exposure"] == 50000001


def test_default_years_skip_a_trickle_year():
    """CY2026 annuals are not due until May 2027. A handful of early/amended rows must
    not be mistaken for a filed cycle and differenced against a full year."""
    rows = []
    for i in range(10):
        rows += [("house", 2024, "M%d" % i, "NC%02d" % i, "AAPL"),
                 ("house", 2025, "M%d" % i, "NC%02d" % i, "AAPL")]
    rows.append(("house", 2026, "M0", "NC00", "AAPL"))      # one early filer
    res = _run(rows)
    assert (res["year"], res["prior_year"]) == (2025, 2024), (res["year"],
                                                              res["prior_year"])


def test_no_usable_years_returns_empty_not_a_crash():
    p = _db()
    con = dbmod.connect(p)
    con.close()
    ro = q.connect_ro(p)
    try:
        res = q.q_congress_breadth_yoy(ro)
        assert res["rows"] == [] and res["year"] is None
    finally:
        ro.close()
        os.unlink(p)
