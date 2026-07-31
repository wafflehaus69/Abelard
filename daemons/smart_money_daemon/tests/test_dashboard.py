"""SM-R1 dashboard view + safety tests. No live socket: view functions are called
directly against a read-only fixture DB. Covers HTML rendering, filter-param
clamping/escaping, and the public-bind refusal."""
import os
import tempfile

from smart_money import dashboard as dash
from smart_money import db as dbmod
from smart_money import queries as q


def _fixture_db():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    con.execute(
        "INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
        "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
        "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
        "VALUES('A',0,'Insider','1','Co','9','ZZZ','P',0,100,1.0,100,NULL,"
        "'2026-06-01','2026-06-02',NULL,'watchlist')")
    # Insert every registry Shape-A seed person so q_sentinel_log's fail-loud
    # orphan check does not false-fire against an empty persons table.
    entries, _ = q._load_registry()
    for e in entries:
        if e.get("person_id") is not None:
            con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, "
                        "cik_or_chamber) VALUES(?,?,?,?)",
                        (e["person_id"], e["name"], "congress", e.get("chamber") or "house"))
    con.commit()
    con.close()
    return path


def test_all_views_render_html():
    path = _fixture_db()
    con = q.connect_ro(path)
    p = dash._params({})
    for view in (dash.view_front, dash.view_clusters, dash.view_sentinels):
        h = view(con, p)
        assert h.startswith("<!doctype html>"), view.__name__
        assert "Smart Money" in h and "/brief.pdf" in h, view.__name__
    # ticker view with no symbol prompts; with a symbol renders the panel.
    assert "symbol" in dash.view_ticker(con, dash._params({}))
    h = dash.view_ticker(con, dash._params({"symbol": ["zzz"]}))
    assert "ZZZ" in h and "Insider" in h, "ticker panel"
    con.close()
    os.unlink(path)


def test_params_clamped_and_normalized():
    p = dash._params({"window": ["99999"], "floor": ["x"],
                      "anchor": ["2026-01-01"], "symbol": ["aapl"]})
    assert p["window"] == 3650   # clamped to the max
    assert p["floor"] == 3       # unparseable -> default
    assert p["symbol"] == "AAPL"  # upper-cased


def test_html_escaping_blocks_injection():
    # A hostile symbol must be escaped in the rendered page, never reflected raw.
    p = dash._params({"symbol": ["<script>x</script>"]})
    assert p["symbol"] == "<SCRIPT>X</SCRIPT>"
    page = dash._page("t", "body", p)
    assert "<script>" not in page and "&lt;" in page


def test_trades_view_and_expand_params():
    path = _fixture_db()
    con = q.connect_ro(path)
    h = dash.view_trades(con, dash._params({"side": ["buy"], "scope": ["all"]}))
    assert h.startswith("<!doctype html>"), "trades page"
    assert "Insider trades" in h and "trade date" in h and "reported" in h, h[:400]
    assert "per page:" in h and "page 1 of" in h, "pager present"
    assert "/trades.csv" in h and "whole dataset CSV" in h, "csv export links present"
    assert "scope:" in h and "overlay" in h, "scope toggle present"
    assert "clear filters" in h and "/trades?side=all&plan=all&scope=all" in h, \
        "clear-filters must reset every filter and overlay"
    # pagination + filter param sanitizing
    assert dash._params({"per_page": ["250"]})["per_page"] == 250
    assert dash._params({"per_page": ["999"]})["per_page"] == 100   # bad -> default 100
    assert dash._params({"page": ["3"]})["page"] == 3
    assert dash._params({"page": ["0"]})["page"] == 1               # <1 -> 1
    assert dash._params({"page": ["x"]})["page"] == 1               # non-numeric -> 1
    assert dash._params({"side": ["hack"]})["side"] == "buy"        # sanitized
    assert dash._params({"plan": ["planned"]})["plan"] == "planned"
    assert dash._params({"smid": ["1"]})["smid"] is True
    assert dash._params({"scope": ["all"]})["scope"] == "all"
    assert dash._params({"scope": ["hack"]})["scope"] == "scoped"   # default
    con.close()
    os.unlink(path)


def test_trades_csv_export():
    path = _fixture_db()
    con = q.connect_ro(path)
    p = dash._params({"side": ["buy"], "scope": ["all"]})
    data = dash._build_trades_csv(con, p, full=False)
    assert data.startswith("person,ticker,side,trade_date"), data[:60]
    assert data.splitlines()[0].endswith(",provenance"), "provenance is the last CSV column"
    lines = data.strip().splitlines()
    assert len(lines) >= 2, "header plus at least one row"
    assert "ZZZ" in data, "the fixture buy is in the CSV"
    # full export returns at least as many rows as the page
    full = dash._build_trades_csv(con, p, full=True)
    assert len(full.strip().splitlines()) >= len(lines)
    con.close()
    os.unlink(path)


def test_clusters_view_pagination_and_csv():
    path = _fixture_db()
    con = q.connect_ro(path)
    h = dash.view_clusters(con, dash._params({}))
    assert h.startswith("<!doctype html>"), "clusters page"
    assert "per page:" in h and "page 1 of" in h, "pager present on clusters"
    assert "/clusters.csv" in h and "whole dataset CSV" in h, "clusters csv links"
    assert "which=buy" in h and "which=sell" in h, "both cluster tables export"
    assert "capitulation only" in h, "cluster filter present"
    assert "clear filters" in h and "/clusters'" in h.replace('"', "'"), \
        "clusters clear-filters reset"
    p = dash._params({})
    assert dash._build_clusters_csv(con, p, full=False, which="buy").startswith(
        "ticker,issuer_cik,n_buyers"), "buy CSV header"
    assert dash._build_clusters_csv(con, p, full=True, which="sell").startswith(
        "ticker,issuer_cik,distinct_sellers_window"), "sell CSV header"
    assert dash._params({"spage": ["4"]})["spage"] == 4
    assert dash._params({"spage": ["0"]})["spage"] == 1          # <1 -> 1
    assert dash._params({"capit": ["1"]})["capit"] is True
    con.close()
    os.unlink(path)


def test_sentinels_view_pagination_and_csv():
    path = _fixture_db()
    con = q.connect_ro(path)
    h = dash.view_sentinels(con, dash._params({}))
    assert h.startswith("<!doctype html>"), "sentinels page"
    assert "per page:" in h and "page 1 of" in h, "pager present on sentinels"
    assert "/sentinels.csv" in h and "whole dataset CSV" in h, "sentinels csv links"
    assert "source" in h and "clear filters" in h, "sentinel source filter + clear"
    assert "/sentinels'" in h.replace('"', "'"), "sentinels clear-filters reset"
    data = dash._build_sentinels_csv(con, dash._params({}), full=True)
    assert data.startswith("event_date,src,seed,role,ticker,action,value"), data[:40]
    assert dash._params({"src": ["congress"]})["src"] == "congress"
    assert dash._params({"src": ["13f"]})["src"] == "13f"
    assert dash._params({"src": ["hack"]})["src"] == "all"       # bad -> default
    con.close()
    os.unlink(path)


def test_flows_view_and_csv():
    path = _fixture_db()
    con = q.connect_ro(path)
    h = dash.view_flows(con, dash._params({}))
    assert h.startswith("<!doctype html>"), "flows page"
    assert "per page:" in h and "page 1 of" in h, "pager on flows"
    assert "/flows.csv" in h and "whole dataset CSV" in h, "flows csv links"
    assert "insiders" in h and "net $" in h, "insiders + $ described"
    assert "all scraped" in h and "clear filters" in h, "scope toggle + clear present"
    # 7d column added; insiders + secondary ($) shown paired per timeframe
    assert "7d ins" in h and "all-time ins" in h and "7d $" in h and "365d $" in h, h[:900]
    # headers are click-to-sort; the default active column carries a sort link
    assert "sort=persons_all" in h, "sortable headers with default sort"
    # the fixture's ZZZ buy is a scraped security -> shows under all-scope
    hall = dash.view_flows(con, dash._params({"scope": ["all"]}))
    assert "ZZZ" in hall, "scraped ticker appears under all-scope"
    # secondary-column sanitizing (metric is now value|shares, default value)
    assert dash._params({"metric": ["shares"]})["metric"] == "shares"
    assert dash._params({"metric": ["hack"]})["metric"] == "value"      # bad -> default
    # CSV carries all three metrics per timeframe including the new 7d
    data = dash._build_flows_csv(con, dash._params({"scope": ["all"]}), full=True)
    assert data.startswith("ticker,ins_7,val_7,sh_7,ins_30"), data[:60]
    assert "ZZZ" in data, "full export includes the scraped ticker"
    con.close()
    os.unlink(path)


def test_portfolios_view_and_front_strip():
    path = _fixture_db()                    # provides registry persons (front page needs them)
    conw = dbmod.connect(path)
    HI = ("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
          "ticker, issuer, put_call, value, shares, ingested_at_unix) "
          "VALUES('1536411',?,?,?,?,?,?,?,?,?,0)")
    conw.execute(HI, ("c1", "2026-03-31", "2026-03-31", "CU_A", "AAA", "Aco", "long", 1500, 150))
    conw.execute(HI, ("c1", "2026-03-31", "2026-03-31", "CU_F", None, "Unk", "long", 250, 25))
    conw.commit()
    conw.close()
    con = q.connect_ro(path)
    h = dash.view_portfolios(con, dash._params({"filer": ["1536411"]}))
    assert h.startswith("<!doctype html>") and "Reported portfolio" in h, "title"
    assert "Reported book only" in h and "45 days stale" in h, "caveats block"
    assert "reported book" in h and "put-notional" in h, "direction-netted header"
    assert "unmapped" in h and "% book" in h, "unmapped row + pct column"
    assert "sort=value" in h, "sortable headers"
    assert "/portfolios.csv" in h and "per page:" in h, "csv + pager"
    # front-page tracked-books strip links into /portfolios
    hf = dash.view_front(con, dash._params({}))
    assert "Tracked books" in hf and "/portfolios?filer=" in hf, "front strip"
    data = dash._build_portfolio_csv(con, dash._params({"filer": ["1536411"]}), full=True)
    assert data.startswith("cusip,ticker,issuer,instrument,value,shares,pct_of_book,badge"), data[:60]
    # param sanitizing
    assert dash._params({"filer": ["00015 36411"]})["filer"] == "0001536411"  # digits only
    assert dash._params({"period": ["2026-03-31"]})["period"] == "2026-03-31"
    assert dash._params({"period": ["garbage"]})["period"] == ""            # bad -> empty
    con.close()
    os.unlink(path)


def test_congress_view_and_csv():
    path = tempfile.mktemp(suffix=".db")
    con = dbmod.connect(path)
    CH = ("INSERT INTO congress_holdings(doc_id, chamber, filing_year, period, "
          "member_last, member_first, state_dist, person_id, row_idx, asset_name, "
          "ticker, asset_type, owner, value_lo, value_hi, income_type, ingested_at_unix) "
          "VALUES(?,'house',2026,'2026-05-15',?,?,?,NULL,?,?,?,?,?,?,?,NULL,0)")
    con.execute(CH, ("d1", "Aaa", "A", "A1", 0, "Apple", "AAPL", "ST", "Self", 15001, 50000))
    con.execute(CH, ("d2", "Bbb", "B", "B1", 0, "Apple", "AAPL", "ST", "SP", 50001, 100000))
    con.commit()
    con.close()
    con = q.connect_ro(path)
    h = dash.view_congress(con, dash._params({}))
    assert h.startswith("<!doctype html>") and "Congress breadth" in h, "breadth page"
    assert "Distribution-first" in h and "Annual snapshot" in h, "distribution note + caveat"
    assert "sort=holder_count" in h, "sortable"
    assert "cticker=AAPL" in h, "ticker drill-down link"
    hd = dash.view_congress(con, dash._params({"cticker": ["AAPL"], "cinstr": ["SH"]}))
    assert "Congress holders" in hd and "breadth board" in hd, "drill-down"
    data = dash._build_congress_csv(con, dash._params({}), full=True)
    assert data.startswith("ticker,instrument,holder_count"), data[:40]
    assert dash._params({"cowner": ["spouse"]})["cowner"] == "spouse"
    assert dash._params({"cowner": ["x"]})["cowner"] == "all"      # bad -> default
    assert dash._params({"cinstr": ["OP"]})["cinstr"] == "OP"
    con.close()
    os.unlink(path)


def test_sort_helpers_and_param_sanitizing():
    rows = [{"x": 3}, {"x": 1}, {"x": None}, {"x": 2}]
    assert [r["x"] for r in dash._sorted(rows, "x", "asc")] == [1, 2, 3, None], "None last"
    assert [r["x"] for r in dash._sorted(rows, "x", "desc")] == [3, 2, 1, None], "None still last"
    p = dash._params({})
    qs = lambda **kw: dash._qs(p, **kw)
    hh = dash._sort_headers(p, [("x", "X"), (None, "Y")], qs, "x", "desc")
    assert "sort=x" in hh and "&#9660;" in hh, "active desc column links + arrow"
    assert "<th>Y</th>" in hh, "None-key column is a plain header"
    assert dash._params({"sort": ["value_30"]})["sort"] == "value_30"
    assert dash._params({"sort": ["a; DROP"]})["sort"] == ""           # sanitized to empty
    assert dash._params({"dir": ["asc"]})["dir"] == "asc"
    assert dash._params({"dir": ["x"]})["dir"] == "desc"
    assert dash._params({"ssort": ["rate_ratio"], "sdir": ["asc"]})["sdir"] == "asc"


def test_nav_links_reset_paging_cursor():
    # A page/spage cursor must NOT bleed across views via the top nav — switching
    # views should open on page 1, not land deep in an unrelated table. (The theme
    # toggle and print links legitimately keep the cursor; only cross-view nav resets.)
    p = dash._params({"page": ["3"], "spage": ["2"], "per_page": ["250"],
                      "sort": ["value_180"], "dir": ["asc"]})
    navqs = dash._qs(p, page=None, spage=None, sort=None, dir=None, ssort=None, sdir=None)
    fullqs = dash._qs(p)                           # carries page=3, spage=2, sort=value_180
    assert "page=3" not in navqs and "spage" not in navqs, navqs
    assert "sort=" not in navqs and "dir=" not in navqs, "cross-view nav drops sort state"
    assert "page=3" in fullqs and "per_page=250" in navqs, "size carries, cursor does not"
    page = dash._page("t", "body", p)
    for href in ("/trades", "/clusters", "/sentinels", "/ticker"):
        assert 'href="{}{}"'.format(href, navqs) in page, "reset qs for " + href
        assert 'href="{}{}"'.format(href, fullqs) not in page, "no cursor/sort bleed for " + href


def test_page_slice_math():
    rows = list(range(250))
    page1, m1 = dash._page_slice(rows, 100, 1)
    assert len(page1) == 100 and m1["pages"] == 3 and m1["total"] == 250
    page3, m3 = dash._page_slice(rows, 100, 3)
    assert page3 == list(range(200, 250)) and m3["page"] == 3
    _, m9 = dash._page_slice(rows, 100, 9)          # out of range -> last page
    assert m9["page"] == 3
    _, m0 = dash._page_slice([], 100, 1)            # empty still reports one page
    assert m0["pages"] == 1 and m0["total"] == 0


def test_dark_mode_theme():
    # theme is sanitized to dark|light only.
    assert dash._params({"theme": ["dark"]})["theme"] == "dark"
    assert dash._params({"theme": ["light"]})["theme"] == "light"
    assert dash._params({"theme": ["hackerman"]})["theme"] == ""
    # forced dark stamps data-theme on <html> and carries the dark palette + toggle.
    page = dash._page("t", "body", dash._params({"theme": ["dark"]}))
    assert 'data-theme="dark"' in page, "forced dark must set the html attribute"
    assert "prefers-color-scheme:dark" in page, "auto dark media query must exist"
    assert "theme=light" in page, "toggle must offer switching back to light"
    # default (auto) leaves the <html> tag bare — it follows the system preference.
    auto = dash._page("t", "body", dash._params({}))
    assert "<html>" in auto and "theme=dark" in auto, "auto default + toggle to dark"


def test_serve_refuses_public_bind():
    for bad in ("0.0.0.0", "::", ""):
        raised = False
        try:
            dash.serve("/nonexistent.db", bad, 8787)
        except SystemExit:
            raised = True
        assert raised, "serve must refuse to bind {!r}".format(bad)
