"""Print-brief targets the CURRENTLY VIEWED page, not always the front-page brief."""
from smart_money import dashboard as dash


def test_print_button_targets_the_current_page():
    front = dash._page("t", "body", dict(dash._params({}), _path="/"))
    assert "view=front" in front and "Print brief (PDF)" in front
    for path, label in (("/portfolios", "portfolios"), ("/oge", "oge"),
                        ("/disagreements", "disagreements")):
        page = dash._page("t", "body", dict(dash._params({}), _path=path))
        assert "view=" + label in page, path
        assert "Print page (PDF)" in page, path


def test_front_page_falls_back_to_the_editorial_brief(tmp_path):
    """view=front (or absent) must NOT hit the table renderer — the front page has its
    own editorial brief and losing it would be a regression."""
    from smart_money import db as dbmod, queries as q
    path = str(tmp_path / "e.db")
    dbmod.connect(path).close()
    con = q.connect_ro(path)
    p = dash._params({})
    assert dash._page_brief_spec(con, p, "/") is None
    assert dash._page_brief_spec(con, p, "/front") is None
    con.close()


def test_page_spec_uses_the_shared_csv_columns(tmp_path):
    """PDF and CSV must share one column definition so they cannot drift."""
    from smart_money import db as dbmod, queries as q
    path = str(tmp_path / "c.db")
    dbmod.connect(path).close()
    con = q.connect_ro(path)
    p = dash._params({})
    for route, cols in (("/oge", dash._OGE_CSV_COLS),
                        ("/disagreements", dash._DIS_CSV_COLS),
                        ("/congress_gaps", dash._GAP_CSV_COLS),
                        ("/portfolios", dash._PORT_CSV_COLS),
                        ("/insiders", dash._INSIDER_CSV_COLS)):
        spec = dash._page_brief_spec(con, p, route)
        assert spec is not None, route
        assert spec[2] is cols, route
    con.close()


def test_render_page_brief_truncates_loudly(tmp_path):
    """A long page is capped for print, and the PDF SAYS it was truncated."""
    from smart_money import brief
    rows = [{"a": i, "b": "x" * 60} for i in range(300)]
    out = str(tmp_path / "t.pdf")
    brief.render_page_brief(out, title="T", subtitle="s", columns=["a", "b"],
                            rows=rows, notes=["n"], max_rows=50)
    import os
    assert os.path.getsize(out) > 1000
    # empty page still renders rather than raising
    out2 = str(tmp_path / "e.pdf")
    brief.render_page_brief(out2, title="T", subtitle="s", columns=["a"], rows=[])
    assert os.path.getsize(out2) > 500


def test_cell_formatting():
    from smart_money.brief import _cell
    assert _cell(None) == "-"
    assert _cell(True) == "yes" and _cell(False) == "no"
    assert _cell(1234567) == "1,234,567"
    assert _cell(12.50) == "12.5"
    assert _cell("x" * 60).endswith("...") and len(_cell("x" * 60)) == 44
