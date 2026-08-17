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
         (ticker or cusip) + " Inc", pc, value, shares))


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
    assert [(r["ticker"], r["instrument"]) for r in rows] == [("AAA", "PUT")]
    # same thesis on both sides -> not cross-thesis
    assert rows[0]["cross_thesis"] is False
    ro.close()


def test_scan_and_queries_read_the_same_registry():
    """SM-P2 event path: scan.REGISTRY_PATH once hardcoded the REPO copy while queries.py
    resolved the STATE HOME copy. That fork let the dashboard show 19 tracked filers while
    leg_13f still iterated a stale 6 — so newly added filers had baselines seeded yet
    would emit NO events on their next filing. Both must resolve identically."""
    import os

    from smart_money import db as dbmod, scan
    assert os.path.abspath(scan.REGISTRY_PATH) == os.path.abspath(
        dbmod.find_artifact("registry.json", "analysis")), (
            "scan and queries must read ONE registry; a fork silently halves the "
            "event path")


def test_flat_hold_is_not_reported_as_an_exit(tmp_path, monkeypatch):
    """THE regression. _manager_flow's exit branch tested a (TICKER, instrument) key
    against a dict keyed (cusip, put_call) -- tuple shapes that can never be equal, so
    the test was always True and any position whose shares did not move was reported as
    a full exit at its prior value. Live, that manufactured 119 of 328 exits, including
    'NVIDIA exited INTC $7.93B' against an unchanged 214,776,632 shares."""
    path = str(tmp_path / "flat.db")
    con = dbmod.connect(path)
    # Alpha holds AAA perfectly flat across both periods.
    _hold(con, "111", "2025-12-31", "CA", "AAA", 1000, 5000)
    _hold(con, "111", "2026-03-31", "CA", "AAA", 1000, 5000)
    # Beta genuinely trims it, so the ticker is live on the board either way.
    _hold(con, "222", "2025-12-31", "CA", "AAA", 900, 9000)
    _hold(con, "222", "2026-03-31", "CA", "AAA", 400, 4000)
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt"),
                           ("222", "Beta Partners", "macro")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    res = q.q_opposed_pairs(ro)
    names = [x["filer"] for r in res["rows"] for x in r["distributing"]]
    assert "Alpha Capital" not in names, (
        "a flat hold is not a distribution: {}".format(res["rows"]))
    actions = [x["action"] for r in res["rows"] for x in r["distributing"]]
    assert "exited" not in actions, actions
    # a flat hold expresses no direction at all, so there is no disagreement here
    assert res["count"] == 0, res["rows"]
    ro.close()


def test_genuine_exit_is_still_reported(tmp_path, monkeypatch):
    """Guard the other side of the fix: tightening the exit test must not suppress
    real exits."""
    path = str(tmp_path / "exit.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2025-12-31", "CB", "BBB", 50, 500)     # gone next period
    _hold(con, "111", "2025-12-31", "CA", "AAA", 10, 100)
    _hold(con, "111", "2026-03-31", "CA", "AAA", 10, 100)
    _hold(con, "222", "2025-12-31", "CA", "AAA", 10, 100)
    _hold(con, "222", "2026-03-31", "CB", "BBB", 70, 700)     # opens it
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt"),
                           ("222", "Beta Partners", "macro")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    by = {(r["ticker"], r["instrument"]): r
          for r in q.q_opposed_pairs(ro)["rows"]}
    b = by[("BBB", "SH")]
    assert [x["action"] for x in b["distributing"]] == ["exited"]
    assert "Alpha Capital" in b["dis_names"]
    ro.close()


def test_call_to_put_roll_is_not_recorded_as_accumulation(tmp_path, monkeypatch):
    """Puts and calls shared one 'OP' bucket, so the dict assignment silently dropped
    one leg. A filer rolling a call into a put had the call's disappearance suppressed
    and the new put logged as 'new -> accumulating' -- the bearish flip at the centre of
    the chip-put thesis read as a bullish one."""
    path = str(tmp_path / "roll.db")
    con = dbmod.connect(path)
    # Alpha rolls AAA call -> AAA put between the two periods.
    _hold(con, "111", "2025-12-31", "CA", "AAA", 0, 746_760_060, pc="call")
    _hold(con, "111", "2026-03-31", "CA", "AAA", 0, 159_106_302, pc="put")
    # Beta holds a shrinking put so the PUT key has two sides.
    _hold(con, "222", "2025-12-31", "CA", "AAA", 0, 900, pc="put")
    _hold(con, "222", "2026-03-31", "CA", "AAA", 0, 400, pc="put")
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt"),
                           ("222", "Beta Partners", "macro")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    _p, flows, _e = q._manager_flow(ro, "111", "ai_tmt", "Alpha Capital")
    assert ("AAA", "CALL") in flows, "the abandoned call leg must be visible: %r" % (flows,)
    assert flows[("AAA", "CALL")][0] == q._DIR_DIS, flows[("AAA", "CALL")]
    assert flows[("AAA", "CALL")][2] == "exited", flows[("AAA", "CALL")]
    assert ("AAA", "PUT") in flows and flows[("AAA", "PUT")][2] == "new"
    ro.close()


def test_unmapped_tickers_are_counted_not_silently_dropped(tmp_path, monkeypatch):
    """Rows with no mapped ticker cannot be compared across filers, but dropping them
    in silence removed real reported value from the board -- live, 521 rows and $58.4B,
    including a $494M ASML put that belonged to the chip-put complex."""
    path = str(tmp_path / "unmapped.db")
    con = dbmod.connect(path)
    _hold(con, "111", "2025-12-31", "CA", "AAA", 100, 1000)
    _hold(con, "111", "2026-03-31", "CA", "AAA", 200, 2000)
    _hold(con, "111", "2025-12-31", "CZ", None, 0, 494_122_503, pc="put")
    _hold(con, "111", "2026-03-31", "CZ", None, 0, 494_122_503, pc="put")
    con.commit()
    con.close()
    regp = _reg(tmp_path, [("111", "Alpha Capital", "ai_tmt")])
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: regp)
    ro = q.connect_ro(path)
    res = q.q_opposed_pairs(ro)
    assert res["excluded_rows"] == 2, res
    assert res["excluded_value"] == 2 * 494_122_503, res
    ro.close()
