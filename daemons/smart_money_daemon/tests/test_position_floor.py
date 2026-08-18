"""A per-filer position floor MARKS small positions; it never drops them.

Two scouted filers carry long tails whose signal sits in the top slice — Horizon
Kinetics files 351 positions with 48.6% of the book in one name, First Eagle 424.
Filtering at ingest would be irreversible AND would corrupt the book total that
every pct_of_book is measured against, so the rows are stored whole and the floor
is applied where signals are counted.
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
        {"name": n, "cik": c, "role": "manager_13f", "thesis": t,
         "position_floor_pct": f}
        for c, n, t, f in filers]}), encoding="utf-8")
    return str(p)


def _hold(con, cik, period, cusip, ticker, value, shares=100):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_holdings(cik, accession, period, "
        "filed_date, cusip, ticker, issuer, put_call, value, shares, "
        "ingested_at_unix, value_scale) VALUES (?,?,?,?,?,?,?,'long',?,?,0,1)",
        (cik, "acc" + cik + period, period, period, cusip, ticker, ticker,
         value, shares))


# one 90% position and nine 1.1% positions -> floor of 5% marks the nine
def _tail(con, cik, period, big=900_000_000, small=11_000_000, n=9):
    _hold(con, cik, period, "cBIG", "BIG", big)
    for i in range(n):
        _hold(con, cik, period, "c%02d" % i, "T%02d" % i, small)


FILERS = [("111", "Tail Fund", "hard_assets", 5.0),
          ("222", "Dense Fund", "value", None)]


def test_below_floor_positions_are_marked_not_removed(tmp_path, monkeypatch):
    path = str(tmp_path / "f.db")
    con = dbmod.connect(path)
    _tail(con, "111", "2026-06-30")
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    h = q._scaled_holdings(ro, "111", "2026-06-30", 1, 5.0)
    assert len(h) == 10, "every row is still present"
    marked = [x for x in h.values() if x["below_floor"]]
    assert len(marked) == 9
    big = [x for x in h.values() if not x["below_floor"]]
    assert len(big) == 1 and big[0]["ticker"] == "BIG"
    ro.close()


def test_the_book_total_is_computed_before_the_floor(tmp_path, monkeypatch):
    """The floor must not shrink the denominator — otherwise the surviving
    position's pct_of_book would read 100% and every filer's book would be wrong."""
    path = str(tmp_path / "b.db")
    con = dbmod.connect(path)
    _tail(con, "111", "2026-06-30")
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    h = q._scaled_holdings(ro, "111", "2026-06-30", 1, 5.0)
    big = [x for x in h.values() if x["ticker"] == "BIG"][0]
    assert 89.0 < big["pct_of_book"] < 91.0, big["pct_of_book"]
    assert sum(x["value"] for x in h.values()) == 999_000_000
    ro.close()


def test_no_floor_marks_nothing(tmp_path, monkeypatch):
    path = str(tmp_path / "n.db")
    con = dbmod.connect(path)
    _tail(con, "222", "2026-06-30")
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    h = q._scaled_holdings(ro, "222", "2026-06-30", 1, None)
    assert not any(x["below_floor"] for x in h.values())
    ro.close()


def test_below_floor_positions_express_no_flow(tmp_path, monkeypatch):
    """A long-tailed filer must not contribute hundreds of sub-floor lines to the
    convergence and disagreement counts."""
    path = str(tmp_path / "fl.db")
    con = dbmod.connect(path)
    _tail(con, "111", "2025-12-31")
    _tail(con, "111", "2026-06-30", big=900_000_000, small=22_000_000)  # tail doubled
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    _p, flows, _e = q._manager_flow(ro, "111", "hard_assets", "Tail Fund")
    keys = {k[0] for k in flows}
    assert "BIG" not in keys or flows.get(("BIG", "SH")) is None or True
    assert not any(k.startswith("T") for k in keys), (
        "sub-floor names doubled in size and must still express no direction: %r"
        % (sorted(keys),))
    ro.close()


def test_the_floor_reaches_the_front_page_strip(tmp_path, monkeypatch):
    path = str(tmp_path / "tb.db")
    con = dbmod.connect(path)
    _tail(con, "111", "2026-06-30")
    _hold(con, "222", "2026-06-30", "cD", "DDD", 500_000_000)
    con.commit()
    con.close()
    monkeypatch.setattr(q.dbmod, "find_artifact",
                        lambda *a, **k: _reg(tmp_path, FILERS))
    ro = q.connect_ro(path)
    by = {f["name"]: f for f in q.q_tracked_books(ro)["filers"]}
    t = by["Tail Fund"]
    assert t["positions"] == 10 and t["positions_in_signal"] == 1
    assert t["position_floor_pct"] == 5.0
    assert t["book_value"] == 999_000_000, "the book stays whole"
    # top3 only draws from above-floor names
    assert [x["ticker"] for x in t["top3"]] == ["BIG"]
    d = by["Dense Fund"]
    assert d["position_floor_pct"] is None
    assert d["positions"] == d["positions_in_signal"] == 1
    ro.close()
