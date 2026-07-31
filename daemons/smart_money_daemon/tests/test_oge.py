"""OGE 278e restricted-source tests.

The load-bearing property is that the statutory use restriction is attached to EVERY row
and cannot be dropped — in the schema, in the query layer, on the page, and in the export.
"""
import pytest

from smart_money import db as dbmod, dashboard as dash, oge_ingest as oge, queries as q

RESTRICTION = "NOT TO BE USED FOR COMMERCIAL PURPOSES"


def _seed(path):
    con = dbmod.connect(path)
    rows = [
        {"line_no": "15.1", "description": "Coupang Inc. (Class A Common Stock (CPNG))",
         "ticker": "CPNG", "eif": "N/A", "value_lo": 1000001, "value_hi": 5000000,
         "income_type": None, "income_lo": None, "income_hi": None},
        {"line_no": "16.94", "description": "SPOUSE - SPDR S&P 500 ETF (SPY)",
         "ticker": "SPY", "eif": "Yes", "value_lo": 100001, "value_hi": 1000000,
         "income_type": "Dividends", "income_lo": None, "income_hi": None},
    ]
    oge.ingest(con, "DOC1", "Warsh, Kevin", "Nominee 278 (04/10/2026)", "04/10/2026",
               rows, "https://example.invalid/report.pdf")
    return con


def test_every_row_carries_the_restriction(tmp_path):
    path = str(tmp_path / "o.db")
    con = _seed(path)
    got = con.execute("SELECT use_restriction FROM oge_holdings").fetchall()
    assert got and all(r[0] == RESTRICTION for r in got), got
    con.close()


def test_restriction_column_is_not_nullable(tmp_path):
    """Structural guarantee: a row cannot physically exist without its restriction."""
    con = dbmod.connect(str(tmp_path / "n.db"))
    with pytest.raises(Exception):
        con.execute(
            "INSERT INTO oge_holdings(doc_id, filer, line_no, use_restriction, "
            "ingested_at_unix) VALUES('D','F','1',NULL,0)")
    con.close()


def test_query_and_csv_carry_the_tag_per_row(tmp_path):
    path = str(tmp_path / "v.db")
    con = _seed(path)
    con.commit()
    con.close()
    ro = q.connect_ro(path)
    res = q.q_oge_holdings(ro)
    assert res["count"] == 2 and res["banded"] == 2
    assert all(r["use_restriction"] == RESTRICTION for r in res["rows"])
    # the report's own FILER/SPOUSE marking is surfaced as owner
    assert {r["owner"] for r in res["rows"]} == {"-", "spouse"}
    p = dash._params({})
    page = dash.view_oge(ro, p)
    assert "restrict-banner" in page and RESTRICTION in page
    assert page.count("class='restrict'") == 2, "one visible tag per row"
    csv_text = dash._build_oge_csv(ro, p, full=True)
    lines = csv_text.splitlines()
    assert lines[0].endswith("use_restriction")
    assert all(RESTRICTION in ln for ln in lines[1:]), "tag must leave with the data"
    ro.close()


def test_oge_table_is_not_read_by_the_signal_path():
    """The restricted source must not be reachable from scan/alert/enqueue code."""
    import inspect

    from smart_money import events, scan
    for mod in (scan, events):
        assert "oge_holdings" not in inspect.getsource(mod), mod.__name__


def test_parse_band_reused_for_oge_bands():
    from smart_money.house_fd_ingest import _parse_band
    assert _parse_band("$1,000,001 - $5,000,000") == (1000001, 5000000)
    assert _parse_band("None (or less than $201)") == (0, 201)
