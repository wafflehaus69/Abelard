"""The runner's threshold tripwire (MR-14 ruling 5).
Regression test: the burned-week query, which carried the 2x and 3x cuts into Dune, is refused."""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "recon"))
import verdict_constants as vc  # noqa: E402

SQL = ROOT / "recon" / "sql"


def test_the_burned_week_query_is_refused():
    names = {n for n, _, _ in vc.hits((SQL / "pricepath_burned_week.sql").read_text(encoding="utf-8"))}
    assert any("3x" in n for n in names) and any("7 days judged" in n for n in names)


@pytest.mark.parametrize("kind", ["events", "b1a", "b1b", "b2", "fan"])
def test_every_scheduled_chunk_query_passes(kind):
    files = sorted((SQL / "chunks").glob(f"*_{kind}.sql"))
    assert len(files) == 19
    for f in files:
        assert vc.hits(f.read_text(encoding="utf-8")) == [], f.name


@pytest.mark.parametrize("sql", [
    "WHERE role = 'F' OR n_same_funder >= 5", "HAVING approx_distinct(x) >= 400", "WHERE fan_out < 32",
    "WHERE top10 / supply > 0.4", "WHERE top10_pct >= 40", "WHERE linked > 0.15", "approx_percentile(v, 0.95)",
    "WHERE ratio < 0.2", "count_if(peak >= 3 * px)", "avg(CASE WHEN peak >= 2 * px THEN 1 END)",
    "HAVING count(*) >= 3", "count_if(s.px_7d >= s.px_g240)", "WHERE s.px_g15 < s.px_7d", "WHERE share > .3",
    "WHERE 5 <= n_same_funder", "WHERE 400<=fan_out", "WHERE n BETWEEN 5 AND 99", "WHERE pct BETWEEN 0 AND 40",
    "WHERE x>=5", "where fan_out >= 8192", "WHERE top10 > 0.40", "WHERE 0.15 < linked",
    # found by the review of 2026-10-01: every one of these passed the first version
    "count_if(s.peak_24h_g15 >= s.px_g15 * 3)", "WHERE top10_240 > supply240 * 0.4", "WHERE qres_g240 - vqr < (qres_grad - vqr) * 0.2",
    "HAVING count(*) >= 5e0", "WHERE c > 0.4e0", "WHERE f >= 400e0", "WHERE p >= 3.0e0 * q",
    "WHERE s.px_7d / s.px_g15 > 1", "WHERE s.px_7d - s.px_g15 > 0", "WHERE px_7d > s1.px_g240", "WHERE s.px_7d > (s.px_g15)",
    "HAVING count(*) >=" + chr(10) + "       5", "count_if(s.px_7d >=" + chr(10) + " s.px_g15)", "/* funder's cut */ WHERE n >= 5 AND role = 'B'",
    "WHERE n >= (5)", "WHERE n >= CAST(5 AS bigint)", "WHERE n >= +5", "WHERE p BETWEEN (0) AND 40", "WHERE n BETWEEN lo + 1 AND 5",
    "WHERE n_same_funder > 4", "WHERE fan_out > 399", "WHERE pctl >= 95"])
def test_registered_constants_trip(sql):
    assert vc.hits(sql), sql


@pytest.mark.parametrize("sql", [
    "AND p.rn = 1", "WHERE x.d > 0", "AND CAST(s.amount AS double) >= 1e6", "AND r15 <= 10",
    "WHERE g = 15 AND win = '7d'", "t < grad_time + INTERVAL '30' MINUTE", "INTERVAL '5' DAY",
    "approx_percentile(1e4 * (gross - net) / gross, 0.5)", "-- flagged when >= 5 share a funder",
    "ARRAY[0.1, 0.25, 0.5, 0.75, 0.9, 0.99]", "THEN 17584505500e0 ELSE 0e0 END", "evt_block_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-31'",
    "max(1 - px_post / runmax)", "WHERE s.block_time >= TIMESTAMP '2025-06-05 00:00:00'", "gw.g * INTERVAL '1' MINUTE",
    "evt_block_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'", "t < grad_time + INTERVAL '5' DAY",
    "WHERE pool IN ('5', '400')", "ORDER BY slot_last DESC, sender DESC) AS rn", "WHERE rn = 1",
    "ORDER BY bl.b15 DESC NULLS LAST) AS r15", "sum(m.f2) AS f2, sum(m.f3) AS f3", "AS b240, sum(m.b15) AS b15",
    "avg(s.px_7d / s.px_g15)", "approx_percentile(s.px_7d / s.px_g240, 0.5)", "SELECT s.px_7d, s.px_g15, s.px_g60",
    "ev.b * ev.net / ev.base_amt - ev.q - ev.net", "AND ev.net >= 1e6", "1e4 * cre / gross", "AND s.block_slot <= m.slot_act"])
def test_structural_definitions_do_not(sql):
    assert vc.hits(sql) == [], sql


def test_superseded_queries_with_the_bundle_cut_are_refused():
    assert vc.hits((SQL / "a3_seta_day.sql").read_text(encoding="utf-8"))


def test_the_runner_refuses_before_it_touches_the_network(monkeypatch):
    """Order 3: the grep is in the runner, and the burned-week query is the regression test."""
    import dune_run_sql as r

    def boom(*a, **k):
        raise AssertionError("reached the network or the owner list")
    monkeypatch.setattr(r.rt, "dune", boom)
    monkeypatch.setattr(r.dune_usage, "usage", boom)
    monkeypatch.setattr(r.owner_wallets, "sql_not_owner", boom)
    monkeypatch.setattr(sys, "argv", ["dune_run_sql.py", str(SQL / "pricepath_burned_week.sql"), "--expect", "1"])
    with pytest.raises(SystemExit) as e:
        r.main()
    assert "REFUSED" in str(e.value)


@pytest.mark.parametrize("path,key", [("/sql/execute", "sql"), ("/query", "query_sql")])
def test_the_one_request_function_refuses_too(monkeypatch, path, key):
    """Saved queries and view definitions do not go through the runner; they do go through this."""
    import urllib.request
    import dune_roundtrip as rt
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(AssertionError("reached the network")))
    with pytest.raises(SystemExit):
        rt.dune("POST", path, "no-key", {key: "SELECT mint FROM t WHERE n_same_funder >= 5"})
    with pytest.raises(SystemExit):
        rt.dune("POST", path, "no-key", {key: "SELECT * FROM pumpdotfun_solana.pump_evt_createevent WHERE evt_block_date = DATE '2026-09-01' LIMIT 1"})


def test_b3_is_refused_and_the_manifest_says_so():
    import json
    man = json.loads((ROOT / "recon" / "chunks_manifest.json").read_text(encoding="utf-8"))
    for row in man:
        assert row["queries"]["b3"]["review"] != "ok" and "NOT SCHEDULED" in row["queries"]["b3"]["review"][0]
        assert all(row["queries"][k]["review"] == "ok" for k in ("events", "b1a", "b1b", "b2", "fan"))
    assert vc.hits((SQL / "chunks" / "2026-08_b3.sql").read_text(encoding="utf-8"))


def test_the_fan_out_query_needs_its_funder_list(monkeypatch):
    import dune_run_sql as r
    monkeypatch.setattr(r.rt, "dune", lambda *a, **k: (_ for _ in ()).throw(AssertionError("reached the network")))
    monkeypatch.setattr(sys, "argv", ["dune_run_sql.py", str(SQL / "chunks" / "2026-08_fan.sql"), "--expect", "1"])
    with pytest.raises(SystemExit) as e:
        r.main()
    assert "--funders-from" in str(e.value)
