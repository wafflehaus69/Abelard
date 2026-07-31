"""ORDER SM-P2 flagship tests: cross-manager disagreements (opposed pairs)."""
import json
import os
import tempfile

from smart_money import db as dbmod, queries as q


def _reg(tmp_path, filers):
    """Minimal registry so _tracked_filers/_filer_thesis resolve in-test."""
    d = tmp_path / "analysis"
    d.mkdir(exist_ok=True)
    p = d / "registry.json"
    p.write_text(json.dumps({"as_of": "2026-01-01", "entries": [
        {"name": n, "cik": c, "role": "manager_13f", "thesis": t}
        for c, n, t in filers]}), encoding="utf-8")
    return str(p)


def _hold(con, cik, period, cusip, ticker, shares, value, pc="long"):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_holdings(cik, accession, period, filed_date, "
        "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,0)",
        (cik, "acc-" + cik + period + cusip + pc, period, period, cusip, ticker,
         ticker + " Inc", pc, value, shares))


def test_opposed_pairs_finds_disagreement(tmp_path, monkeypatch):
    path = str(tmp_path / "op.db")
    con = dbmod.connect(path)
    # A accumulates AAA (shares up) and exits BBB; B trims AAA and opens BBB.
    _hold(con, "111", "2025-12-31", "CA", "AAA", 100, 1000)
    _hold(con, "111", "2026-03-31", "CA", "AAA", 200, 2000)      # added
    _hold(con, "111", "2025-12-31", "CB", "BBB", 50, 500)        # exits (absent later)
    _hold(con, "222", "2025-12-31", "CA", "AAA", 900, 9000)
    _hold(con, "222", "2026-03-31", "CA", "AAA", 400, 4000)      # trimmed
    _hold(con, "222", "2026-03-31", "CB", "BBB", 70, 700)        # new
    con.commit()
    con.close()

    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt"),
                           ("222", "Beta Partners", "macro")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    res = q.q_opposed_pairs(ro)
    by = {(r["ticker"], r["instrument"]): r for r in res["rows"]}
    assert ("AAA", "SH") in by, res["rows"]
    a = by[("AAA", "SH")]
    assert a["n_accumulating"] == 1 and a["n_distributing"] == 1
    assert "Alpha Capital" in a["acc_names"] and "Beta Partners" in a["dis_names"]
    # opposite sides come from different thesis groups
    assert a["cross_thesis"] is True
    # BBB: Alpha exited while Beta opened -> also a disagreement
    b = by[("BBB", "SH")]
    assert b["n_accumulating"] == 1 and b["n_distributing"] == 1
    assert "Beta Partners" in b["acc_names"] and "Alpha Capital" in b["dis_names"]
    ro.close()


def test_agreement_is_not_reported_as_disagreement(tmp_path, monkeypatch):
    """Both managers accumulating the same name must NOT appear."""
    path = str(tmp_path / "ag.db")
    con = dbmod.connect(path)
    for cik in ("111", "222"):
        _hold(con, cik, "2025-12-31", "CA", "AAA", 100, 1000)
        _hold(con, cik, "2026-03-31", "CA", "AAA", 300, 3000)    # both added
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt"),
                           ("222", "Beta Partners", "macro")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    assert q.q_opposed_pairs(ro)["count"] == 0
    ro.close()


def test_single_period_filer_expresses_no_direction(tmp_path, monkeypatch):
    """A filer with one filing cannot have a QoQ direction and must be excluded,
    not guessed at as 'new = accumulating'."""
    path = str(tmp_path / "sp.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2025-12-31", "CA", "AAA", 100, 1000)
    _hold(con, "111", "2026-03-31", "CA", "AAA", 50, 500)        # trimmed
    _hold(con, "222", "2026-03-31", "CA", "AAA", 100, 1000)      # ONE period only
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt"),
                           ("222", "Solo Fund", "macro")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    res = q.q_opposed_pairs(ro)
    assert res["filers_compared"] == 1, "the single-period filer contributes no direction"
    assert res["count"] == 0, "one-sided flow is not a disagreement"
    ro.close()


def test_options_are_a_distinct_instrument(tmp_path, monkeypatch):
    """A put position and a long position on the same ticker are different rows, so a
    disagreement in one must not be conflated with the other."""
    path = str(tmp_path / "op2.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2025-12-31", "CA", "AAA", 0, 1000, pc="put")
    _hold(con, "111", "2026-03-31", "CA", "AAA", 0, 5000, pc="put")   # added (value)
    _hold(con, "222", "2025-12-31", "CA", "AAA", 0, 9000, pc="put")
    _hold(con, "222", "2026-03-31", "CA", "AAA", 0, 1000, pc="put")   # trimmed
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "contrarian"),
                           ("222", "Beta Partners", "contrarian")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    rows = q.q_opposed_pairs(ro)["rows"]
    assert [(r["ticker"], r["instrument"]) for r in rows] == [("AAA", "OP")]
    # same thesis on both sides -> not cross-thesis
    assert rows[0]["cross_thesis"] is False
    ro.close()
