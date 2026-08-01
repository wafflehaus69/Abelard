"""SM-C3 Phase H capture-report tests.

The denominators are the whole point — a sloppy one manufactures either a fake PASS or a
fake FAIL. These pin them.
"""
from smart_money import capture_report as cr, db as dbmod


def _row(con, chamber="house", cov=2024, atype="ST", ticker="AAPL", lo=1001, hi=15000,
         doc="d1", idx=[0]):
    idx[0] += 1
    con.execute(
        "INSERT INTO congress_holdings(doc_id, chamber, coverage_year, row_idx, "
        "asset_name, ticker, asset_type, value_lo, value_hi, ingested_at_unix) "
        "VALUES(?,?,?,?,?,?,?,?,?,0)",
        (doc, chamber, cov, idx[0], "Asset", ticker, atype, lo, hi))


def test_non_equity_rows_excluded_from_ticker_denominator(tmp_path):
    """Real property and bank accounts have no ticker BY NATURE. Counting them as misses
    would manufacture a failure."""
    con = dbmod.connect(str(tmp_path / "a.db"))
    for _ in range(10):
        _row(con, atype="ST", ticker="AAPL")
    for _ in range(90):                       # no type, no ticker: real property etc.
        _row(con, atype=None, ticker=None)
    con.commit()
    c = cr.measure(con)[("house", 2024)]
    assert c["rows"] == 100
    assert c["tick_den"] == 10 and c["tick_hit"] == 10
    assert cr._pct(c["tick_hit"], c["tick_den"]) == 100.0, "non-equity must not dilute"
    con.close()


def test_equity_row_missing_ticker_is_a_real_miss(tmp_path):
    con = dbmod.connect(str(tmp_path / "b.db"))
    for _ in range(19):
        _row(con, atype="ST", ticker="AAPL")
    _row(con, atype="ST", ticker=None)        # a genuine parse miss
    con.commit()
    c = cr.measure(con)[("house", 2024)]
    assert c["tick_den"] == 20 and c["tick_hit"] == 19
    assert cr._pct(c["tick_hit"], c["tick_den"]) == 95.0
    con.close()


def test_band_gate_uses_equity_denominator_and_floor_is_separate(tmp_path):
    """The gate counts equity rows; the corpus-wide rate is reported as a FLOOR and must
    NOT be what the gate reads, since value-less non-equity rows drag it down."""
    con = dbmod.connect(str(tmp_path / "c.db"))
    for _ in range(10):                        # equity, banded
        _row(con, atype="ST", ticker="AAPL", lo=1001, hi=15000)
    for _ in range(10):                        # non-equity, genuinely value-less
        _row(con, atype=None, ticker=None, lo=None, hi=None)
    con.commit()
    c = cr.measure(con)[("house", 2024)]
    assert (c["band_den"], c["band_hit"]) == (10, 10), "gate denominator = equity rows"
    assert (c["all_den"], c["all_hit"]) == (20, 10), "floor counts everything"
    txt = cr.render(cr.measure(con))
    assert "GATE band   >= 90.0%: PASS" in txt
    assert "50.0% - reported as a" in txt, "floor shown, and shown as a floor"
    con.close()


def test_floor_below_gate_does_not_flip_the_verdict(tmp_path):
    con = dbmod.connect(str(tmp_path / "d.db"))
    for _ in range(5):
        _row(con, atype="ST", ticker="AAPL")
    for _ in range(200):
        _row(con, atype=None, ticker=None, lo=None, hi=None)
    con.commit()
    txt = cr.render(cr.measure(con))
    assert "GATE ticker >= 95.0%: PASS" in txt and "GATE band   >= 90.0%: PASS" in txt
    assert "FLOOR, NOT the gate" in txt
    con.close()


def test_per_year_cells_are_reported_before_the_verdict(tmp_path):
    """Distribution-first: one bad year must be visible, not averaged away."""
    con = dbmod.connect(str(tmp_path / "e.db"))
    for _ in range(100):
        _row(con, cov=2024, atype="ST", ticker="AAPL")
    for _ in range(60):                        # a bad year
        _row(con, cov=2022, atype="ST", ticker=None)
    con.commit()
    cells = cr.measure(con)
    assert cr._pct(*[cells[("house", 2022)][k] for k in ("tick_hit", "tick_den")]) == 0.0
    txt = cr.render(cells)
    assert "2022" in txt and "2024" in txt
    assert "Weakest cells" in txt and "house 2022 0.0%" in txt
    con.close()
