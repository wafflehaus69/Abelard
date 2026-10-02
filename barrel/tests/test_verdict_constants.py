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


@pytest.mark.parametrize("kind", ["events", "b1a", "b1b", "b2"])
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
    "WHERE x>=5", "where fan_out >= 8192", "WHERE top10 > 0.40", "WHERE 0.15 < linked"])
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
    "ORDER BY bl.b15 DESC NULLS LAST) AS r15", "sum(m.f2) AS f2, sum(m.f3) AS f3", "AS b240, sum(m.b15) AS b15"])
def test_structural_definitions_do_not(sql):
    assert vc.hits(sql) == [], sql


def test_superseded_queries_with_the_bundle_cut_are_refused():
    assert vc.hits((SQL / "a3_seta_day.sql").read_text(encoding="utf-8"))
