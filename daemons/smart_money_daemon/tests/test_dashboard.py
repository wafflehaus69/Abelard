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


def test_serve_refuses_public_bind():
    for bad in ("0.0.0.0", "::", ""):
        raised = False
        try:
            dash.serve("/nonexistent.db", bad, 8787)
        except SystemExit:
            raised = True
        assert raised, "serve must refuse to bind {!r}".format(bad)
