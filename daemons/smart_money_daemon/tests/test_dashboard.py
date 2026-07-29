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
    lines = data.strip().splitlines()
    assert len(lines) >= 2, "header plus at least one row"
    assert "ZZZ" in data, "the fixture buy is in the CSV"
    # full export returns at least as many rows as the page
    full = dash._build_trades_csv(con, p, full=True)
    assert len(full.strip().splitlines()) >= len(lines)
    con.close()
    os.unlink(path)


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
