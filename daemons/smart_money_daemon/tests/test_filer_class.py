"""Corporate filers must be distinguishable from managers, and never pooled.

Alphabet, Amazon and NVIDIA file 13Fs marking legacy venture and strategic stakes
that became reportable when the underlying listed. They are not expressing a market
view. Measured on production: they are the two largest cards on the front page,
about half the tracked book, and 122 of 788 convergences (15%) exist ONLY because
one of them was counted as a principal — "two principals converged on ARM" is one
manager plus Google's balance sheet.

The classification already existed as registry `thesis` and was read by exactly two
cosmetic consumers. Doctrine is mark-never-drop, so the fix is propagation and
reporting BOTH counts, not exclusion.
"""
import json
import os
import tempfile

from smart_money import db as dbmod, queries as q


def _reg(tmp_path, filers):
    d = tmp_path / "analysis"
    d.mkdir(exist_ok=True)
    p = d / "registry.json"
    p.write_text(json.dumps({"as_of": "2026-01-01", "entries": [
        {"name": n, "cik": c, "role": "manager_13f", "thesis": t}
        for c, n, t in filers]}), encoding="utf-8")
    return str(p)


def _hold(con, cik, period, cusip, ticker, value, shares=100, pc="long"):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_holdings(cik, accession, period, "
        "filed_date, cusip, ticker, issuer, put_call, value, shares, "
        "ingested_at_unix, value_scale) VALUES (?,?,?,?,?,?,?,?,?,?,0,1)",
        (cik, "acc" + cik + period, period, period, cusip, ticker,
         ticker, pc, value, shares))


FILERS = [("111", "Alpha Capital", "ai_tmt"),
          ("222", "Beta Partners", "macro"),
          ("333", "Gamma Corp", "corporate_strategic")]


def test_is_discretionary_separates_the_balance_sheets():
    assert q.is_discretionary("ai_tmt") is True
    assert q.is_discretionary("macro") is True
    assert q.is_discretionary("contrarian") is True
    assert q.is_discretionary("corporate_strategic") is False
    # an unclassified filer is not silently demoted
    assert q.is_discretionary(None) is True


def test_tracked_books_carry_thesis_and_report_both_totals(tmp_path, monkeypatch):
    path = str(tmp_path / "tb.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2026-06-30", "cA", "AAA", 1_000_000_000)
    _hold(con, "222", "2026-06-30", "cA", "AAA", 2_000_000_000)
    _hold(con, "333", "2026-06-30", "cA", "AAA", 90_000_000_000)
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    res = q.q_tracked_books(ro)
    by = {f["name"]: f for f in res["filers"]}
    assert by["Gamma Corp"]["discretionary"] is False
    assert by["Alpha Capital"]["discretionary"] is True
    assert res["n_filers"] == 3 and res["n_discretionary"] == 2
    assert res["book_total"] == 93_000_000_000
    assert res["book_total_discretionary"] == 3_000_000_000
    # nothing is filtered away — the corporate filer still has a card
    assert "Gamma Corp" in by
    ro.close()


def test_a_convergence_that_needs_a_corporate_filer_is_marked(tmp_path, monkeypatch):
    """One manager plus a balance sheet is not two principals converging."""
    path = str(tmp_path / "cv.db")
    con = dbmod.connect(path)
    # BBB: one manager + the corporate filer -> does NOT survive
    _hold(con, "111", "2026-06-30", "cB", "BBB", 1_000_000)
    _hold(con, "333", "2026-06-30", "cB", "BBB", 5_000_000)
    # CCC: two managers -> survives
    _hold(con, "111", "2026-06-30", "cC", "CCC", 1_000_000)
    _hold(con, "222", "2026-06-30", "cC", "CCC", 2_000_000)
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    res = q.q_principal_convergence(ro)
    by = {c["ticker"]: c for c in res["convergences"]}
    assert by["BBB"]["survives_discretionary_only"] is False
    assert by["BBB"]["discretionary_long_filers"] == 1
    assert by["CCC"]["survives_discretionary_only"] is True
    assert by["CCC"]["discretionary_long_filers"] == 2
    # both counts reported, neither silently chosen
    assert res["n_convergences"] == 2
    assert res["n_convergences_discretionary"] == 1
    # and the row itself is still present — marked, not dropped
    assert "BBB" in by
    ro.close()


def test_the_universe_note_is_not_stale(tmp_path, monkeypatch):
    """The payload asserted '13F universe = 6 confirmed CIKs' while the live shelf
    was 19 — a false fact about its own scope, shipped into the PDF brief."""
    path = str(tmp_path / "note.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2026-06-30", "cA", "AAA", 1_000_000)
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    note = q.q_principal_convergence(ro)["note"]
    assert "6 confirmed CIKs" not in note
    assert "3 registry CIKs" in note, note
    ro.close()


def test_ticker_panel_names_the_holder(tmp_path, monkeypatch):
    """cik alone said nothing. On /ticker?symbol=NOK the single $2.21B principal is
    Nokia's own strategic holder."""
    path = str(tmp_path / "tp.db")
    con = dbmod.connect(path)
    _hold(con, "333", "2026-06-30", "cN", "NOK", 2_209_650_581)
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    rows = q.q_ticker_panel(ro, "NOK")["thirteenf_net"]
    assert rows and rows[0]["filer"] == "Gamma Corp"
    assert rows[0]["thesis"] == "corporate_strategic"
    assert rows[0]["discretionary"] is False
    ro.close()


def test_tracked_books_reports_no_book_as_none_not_zero(tmp_path, monkeypatch):
    """A filer with nothing ingested is not a filer with a worthless book — the
    Burry shape. book_value 0 was a literal the doctrine forbids."""
    path = str(tmp_path / "empty.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2026-06-30", "cA", "AAA", 1_000_000)
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    by = {f["name"]: f for f in q.q_tracked_books(ro)["filers"]}
    assert by["Beta Partners"]["book_value"] is None
    assert by["Beta Partners"]["magnitude_warning"]
    ro.close()
