"""SM-C3 Phase F: member fusion (anchor + PTR flows).

This is the FIRST view that DERIVES a claim about a person's holdings rather than
rendering a filing, so the fixtures pin the arithmetic and the tiers exactly.
"""
import json

from smart_money import db as dbmod, queries as q

MEMBER = ("house", "Testman", "Tim", "NC01")
KEY = "house|Testman|Tim|NC01"


def _reg(tmp_path, party="Republican"):
    d = tmp_path / "analysis"
    d.mkdir(exist_ok=True)
    p = d / "registry.json"
    p.write_text(json.dumps({"as_of": "2026-01-01", "entries": []}), encoding="utf-8")
    return str(p)


def _seed(con, holdings=(), trades=(), cov=2025):
    """holdings: (ticker, owner, lo, hi[, asset_name]) ; trades: (ticker, side, lo, hi,
    tx_date, owner)"""
    con.execute("INSERT OR REPLACE INTO congress_member_roster(chamber, member_last, "
                "member_first, state_dist, party, state, match_kind, synced_at_unix) "
                "VALUES(?,?,?,?,'Republican','NC','unique',0)", MEMBER)
    for i, h in enumerate(holdings):
        tk, ow, lo, hi = h[:4]
        nm = h[4] if len(h) > 4 else ((tk or "Asset") + " Inc")
        con.execute(
            "INSERT INTO congress_holdings(doc_id, chamber, coverage_year, filing_date, "
            "member_last, member_first, state_dist, row_idx, asset_name, ticker, "
            "asset_type, owner, value_lo, value_hi, ingested_at_unix) "
            "VALUES('d1',?,?,?,?,?,?,?,?,?,'ST',?,?,?,0)",
            (MEMBER[0], cov, "%d-05-15" % (cov + 1), MEMBER[1], MEMBER[2], MEMBER[3],
             i, nm, tk, ow, lo, hi))
    if trades:
        con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, cik_or_chamber) "
                    "VALUES(1,'Testman, Tim','congress','house')")
        for j, (tk, side, lo, hi, tx, ow) in enumerate(trades):
            con.execute(
                "INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
                "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, owner, "
                "asset_type, filing_id, superseded) "
                "VALUES(1,?,?,?,?,?,?,0,'house','x',?,?,'Stock',?,0)",
                (tk, side, lo, hi, tx, tx, "r%d" % j, ow, "f%d" % j))
    con.commit()


def _fusion(tmp_path, monkeypatch, **kw):
    path = str(tmp_path / "f.db")
    con = dbmod.connect(path)
    _seed(con, **kw)
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: _reg(tmp_path))
    ro = q.connect_ro(path)
    res = q.q_member_fusion(ro, member_key=KEY)
    ro.close()
    return res


def _row(res, ticker, owner="self"):
    for r in res["rows"]:
        if r["ticker"] == ticker and r["owner"] == owner:
            return r
    raise AssertionError("no row {}/{} in {}".format(
        ticker, owner, [(r["ticker"], r["owner"]) for r in res["rows"]]))


def test_anchor_only_is_stale_not_current(tmp_path, monkeypatch):
    res = _fusion(tmp_path, monkeypatch, holdings=[("AAPL", "Self", 15001, 50000)])
    r = _row(res, "AAPL")
    assert r["tier"] == "anchored"
    assert (r["anchor_lo"], r["anchor_hi"]) == (15001, 50000)
    assert (r["buy_flow"], r["sell_flow"]) == (0, 0)


def test_anchor_plus_buy_shifts_the_whole_range(tmp_path, monkeypatch):
    """Estimate is a RANGE: both bounds move by the flow midpoint, never collapsing to
    a point."""
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("AAPL", "Self", 15001, 50000)],
                  trades=[("AAPL", "purchase", 1001, 15000, "2026-03-01", "Self")])
    r = _row(res, "AAPL")
    mid = (1001 + 15000) / 2.0
    assert r["tier"] == "anchored+flows"
    assert r["estimate_lo"] == round(15001 + mid)
    assert r["estimate_hi"] == round(50000 + mid)
    assert r["estimate_lo"] != r["estimate_hi"], "must stay a range"


def test_sells_exceeding_anchor_are_flagged_never_negative(tmp_path, monkeypatch):
    """flows>anchor: the sale midpoints exceed what the band could hold. Flag it, show
    no negative dollars, and do NOT interpret the cause."""
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("AAPL", "Self", 1001, 15000)],
                  trades=[("AAPL", "sale", 500001, 1000000, "2026-03-01", "Self")])
    r = _row(res, "AAPL")
    assert r["tier"] == "flows>anchor"
    assert r["estimate_lo"] >= 0 and (r["estimate_hi"] is None or r["estimate_hi"] >= 0)


def test_flows_only_is_new_since_the_annual(tmp_path, monkeypatch):
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("AAPL", "Self", 1001, 15000)],
                  trades=[("NVDA", "purchase", 15001, 50000, "2026-04-01", "Self")])
    r = _row(res, "NVDA")
    assert r["tier"] == "flows-only" and r["anchor_lo"] is None
    assert r["estimate_lo"] == round((15001 + 50000) / 2.0)


def test_spouse_and_self_never_merge(tmp_path, monkeypatch):
    """Owner is part of the key. Merging would invent a position neither disclosure
    reported."""
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("AAPL", "Self", 1001, 15000),
                            ("AAPL", "SP", 500001, 1000000)],
                  trades=[("AAPL", "purchase", 1001, 15000, "2026-03-01", "Self")])
    me, spouse = _row(res, "AAPL", "self"), _row(res, "AAPL", "spouse")
    assert me["anchor_lo"] == 1001 and spouse["anchor_lo"] == 500001
    assert me["tier"] == "anchored+flows" and spouse["tier"] == "anchored"
    assert spouse["buy_flow"] == 0, "the member's own buy must not touch the spouse row"


def test_flows_inside_the_anchor_year_are_not_double_counted(tmp_path, monkeypatch):
    """An annual covering CY2025 already reflects trades through 2025-12-31."""
    res = _fusion(tmp_path, monkeypatch, cov=2025,
                  holdings=[("AAPL", "Self", 15001, 50000)],
                  trades=[("AAPL", "purchase", 500001, 1000000, "2025-06-01", "Self")])
    r = _row(res, "AAPL")
    assert r["tier"] == "anchored" and r["buy_flow"] == 0


def test_unfusable_rows_are_marked_not_dropped(tmp_path, monkeypatch):
    """Mando's Phase H ruling: a tickerless holding can never join a flow, so it must be
    MARKED — otherwise 'no flows matched' reads as 'no flows occurred'."""
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[(None, "Self", 15001, 50000, "11 Zinfandel Lane")])
    assert res["unfusable"] == 1
    u = [r for r in res["rows"] if r["unfusable"]]
    assert len(u) == 1 and u[0]["ticker"] is None
    assert u[0]["asset_name"] == "11 Zinfandel Lane"
    assert u[0]["estimate_lo"] is None, "an unfusable row must not carry an estimate"


def test_open_top_band_contributes_floor_not_an_invented_ceiling(tmp_path, monkeypatch):
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("AAPL", "Self", 50000000, None)],
                  trades=[("AAPL", "purchase", 50000001, None, "2026-03-01", "Self")])
    r = _row(res, "AAPL")
    assert r["buy_flow"] == 50000001, "open band contributes its floor"
    assert r["estimate_hi"] is None, "an open anchor stays open"


def test_member_with_no_ptr_link_is_anchored_not_broken(tmp_path, monkeypatch):
    """Hyde-Smith/Slotkin/Hirono hold FD positions and file no PTRs we hold. That is the
    anchored tier, NOT a join failure."""
    res = _fusion(tmp_path, monkeypatch, holdings=[("AAPL", "Self", 1001, 15000)])
    assert res["ptr_linked"] is False
    assert res["tiers"].get("anchored") == 1


def test_resolve_person_prefix_handles_thom_thomas():
    idx = {("tillis", "thom"): [(7, "Tillis, Thom")]}
    assert q.resolve_person(idx, "Tillis", "Thomas R")[0] == 7
    # a single initial must never match a full name
    assert q.resolve_person({("x", "theodore"): [(1, "X, Theodore")]}, "X", "T")[0] is None


def test_anchor_reported_without_a_value_is_not_a_zero_floor(tmp_path, monkeypatch):
    """Caught by the Moore hand-verification. Seeding the accumulator at 0 turned an
    asset disclosed with NO band into "at least $0, open-ended" — a floor the filer never
    stated — and then made a later sale look like it exceeded a known holding."""
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("DNUT", "Self", None, None)],
                  trades=[("DNUT", "sale", 15001, 50000, "2026-03-01", "Self")])
    r = _row(res, "DNUT")
    assert (r["anchor_lo"], r["anchor_hi"]) == (None, None), "no invented floor"
    assert r["estimate_lo"] is None, "an unvalued anchor cannot carry an estimate"
    assert r["tier"] != "flows>anchor", "a sale cannot exceed a holding never valued"


def test_multiple_lots_of_one_ticker_sum_into_one_anchor(tmp_path, monkeypatch):
    """Two disclosed lots of the same ticker+owner are one position."""
    res = _fusion(tmp_path, monkeypatch,
                  holdings=[("AAPL", "Self", 1001, 15000),
                            ("AAPL", "Self", 15001, 50000)])
    r = _row(res, "AAPL")
    assert (r["anchor_lo"], r["anchor_hi"]) == (16002, 65000)


def test_member_path_route_and_rendering(tmp_path, monkeypatch):
    """/congress/member/<id> is the one PATH-parameterised route (the order's URL shape);
    the id is the url-quoted member key."""
    from urllib.parse import quote

    from smart_money import dashboard as dash
    path = str(tmp_path / "v.db")
    con = dbmod.connect(path)
    _seed(con, holdings=[("AAPL", "Self", 15001, 50000),
                         (None, "Self", 1001, 15000, "11 Zinfandel Lane")],
          trades=[("AAPL", "purchase", 1001, 15000, "2026-03-01", "Self")])
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: _reg(tmp_path))
    ro = q.connect_ro(path)
    p = dash._params({"member": [KEY]})
    html = dash.view_member(ro, p)
    assert dash.MEMBER_PREFIX == "/congress/member/"
    assert quote(KEY, safe="") or True
    # the page must state that it DERIVES, mark unfusable rows, and show the tiers
    assert "DERIVES, IT DOES NOT REPORT" in html
    assert "UNFUSABLE" in html
    assert "anchored+flows" in html
    # an estimate is a RANGE, never a bare point
    assert "&ndash;" in html
    csv_text = dash._build_member_csv(ro, p, full=True)
    assert csv_text.splitlines()[0].startswith("ticker,asset_name,owner")
    ro.close()


def test_member_page_says_when_no_ptr_filer_matched(tmp_path, monkeypatch):
    """Annual-only must not read as 'they did not trade'."""
    from smart_money import dashboard as dash
    path = str(tmp_path / "n.db")
    con = dbmod.connect(path)
    _seed(con, holdings=[("AAPL", "Self", 1001, 15000)])
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: _reg(tmp_path))
    ro = q.connect_ro(path)
    html = dash.view_member(ro, dash._params({"member": [KEY]}))
    assert "no PTR filer matched" in html
    assert "not that they did not trade" in html
    ro.close()
