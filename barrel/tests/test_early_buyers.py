"""Early-buyer groups (kind b3, cluster_*, H2): the query text, its reduction, and the local reader.
Run: python -m pytest barrel/tests/test_early_buyers.py

Three kinds of test, none of which touches Dune:
  1. the reduction: on random full group tables, the largest linking group at ANY line read from the
     rows the reduction keeps equals the same read from all rows. The design rests on this.
  2. the text: no registered constant at any scope, literal bounds, the calendar-month fan window,
     each large table read as often as designed, one text for every scope.
  3. the text, executed: SQLite runs the generated query on made-up tables through a small dialect
     shim, and the rows are compared with a plain Python model of the definitions. SQLite is not
     Trino: this checks the logic of the text (joins, windows, filters, order), not Trino's types
     or its syntax.
Every address here is made up (the hash of a counter)."""
import calendar
import datetime as dt
import hashlib
import json
import pathlib
import random
import re
import sqlite3
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "recon"))
import early_buyers as eb  # noqa: E402
import gen_fanout  # noqa: E402
import gen_heavy_b2_b3 as b23  # noqa: E402
import verdict_constants as vc  # noqa: E402

SQL = ROOT / "recon" / "sql"
PROVING = {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15"), "pbday": ("2026-09-01", "2026-09-01")}
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def key(i) -> str:
    """A made-up 32-byte key in base58."""
    n, s = int.from_bytes(hashlib.sha256(f"barrel-test-{i}".encode()).digest(), "big"), ""
    while n:
        n, r = divmod(n, 58)
        s = _B58[r] + s
    return s


def chunk_bounds() -> list[tuple[str, str]]:
    """The 19 calendar chunks of the window, written out here so this file does not depend on gen_chunks."""
    out, d, end = [], dt.date(2025, 3, 20), dt.date(2026, 9, 20)
    while d <= end:
        last = min(dt.date(d.year, d.month, calendar.monthrange(d.year, d.month)[1]), end)
        out.append((d.isoformat(), last.isoformat()))
        d = last + dt.timedelta(days=1)
    return out


SCOPES = list(PROVING.values()) + chunk_bounds()


# ---------------------------------------------------------------- 1. the reduction

def _random_table(rng: random.Random, mint: str) -> list[dict]:
    """A full group table for one token: sizes, recipients and labels drawn with many ties."""
    n = rng.randint(1, 45)
    recs = [0, 0, 1, 2, 5, 31, 32, 33, 399, 400, 401, 8191, 8192, 9000]
    return [{"mint": mint, "funder": key((mint, i)), "window_days": 30,
             "labelled": rng.random() < 0.15,
             "recipients": rng.choice(recs) if rng.random() < 0.7 else rng.randint(0, 20000),
             "n_buyers": rng.choice((1, 1, 1, 2, 2, 3, 4, 5, 5, 6, 9)) if rng.random() < 0.8 else rng.randint(1, 900)}
            for i in range(n)]


def _largest_linking(rows: list[dict], line: float) -> int:
    """Straight from the definition, with none of the reader's code: the largest group whose funder is
    unlabelled and below the line. 0 when there is none."""
    return max((r["n_buyers"] for r in rows if not r["labelled"] and r["recipients"] / r["window_days"] < line), default=0)


def _lines(rows: list[dict]) -> list[float]:
    """Every line that can change the answer on this table: each funder's own rate, just under and just
    over it, and the two ends."""
    rates = sorted({r["recipients"] / r["window_days"] for r in rows})
    return [0.0, 1e12] + [x for r in rates for x in (r, r - 1e-9, r + 1e-9)]


def test_largest_linking_group_at_any_line_survives_the_reduction():
    rng = random.Random(20261008)
    kept_total = full_total = 0
    for t in range(400):
        full = _random_table(rng, f"M{t}")
        kept = eb.reduce_reference(full)
        kept_total, full_total = kept_total + len(kept), full_total + len(full)
        assert {r["funder"] for r in kept} <= {r["funder"] for r in full}
        for line in _lines(full):
            want = _largest_linking(full, line)
            assert eb.cluster_actors(kept, line=line) == want, (t, line)
            a, b = eb.the_cluster(full, line=line), eb.the_cluster(kept, line=line)
            assert (a is None) == (b is None) == (want == 0)
            if a:   # the same funder too, not only the same size: the tie-break of the reader is the order of the query
                assert (a["funder"], a["n_buyers"]) == (b["funder"], b["n_buyers"]) and a["n_buyers"] == want
    assert kept_total < full_total / 2      # and it does reduce


def test_the_any_funder_maximum_and_its_funder_survive_too():
    rng = random.Random(7)
    for t in range(300):
        full = _random_table(rng, f"M{t}")
        kept = eb.reduce_reference(full)
        biggest = max(r["n_buyers"] for r in full)
        twin = eb.raw_twin(kept)
        assert twin["cluster_n"] == biggest and all(r["n_max_any"] == biggest for r in kept)
        assert twin["cluster_src"] in {r["funder"] for r in full if r["n_buyers"] == biggest}
        assert all(r["n_groups"] == len(full) and r["n_pairs"] == sum(x["n_buyers"] for x in full) for r in kept)


def test_what_the_reduction_keeps():
    """Unlabelled: a staircase, strictly larger at each step up in recipients. Labelled: its largest group only."""
    rng = random.Random(11)
    for t in range(300):
        full = _random_table(rng, f"M{t}")
        kept = eb.reduce_reference(full)
        un = sorted((r for r in kept if not r["labelled"]), key=lambda r: r["recipients"])
        assert all(a["n_buyers"] < b["n_buyers"] and a["recipients"] < b["recipients"] for a, b in zip(un, un[1:]))
        lab = [r for r in kept if r["labelled"]]
        assert len(lab) == (1 if any(r["labelled"] for r in full) else 0)
        if lab:
            assert lab[0]["n_buyers"] == max(r["n_buyers"] for r in full if r["labelled"])
        # by definition, with no sort: a group is kept exactly when no group before it in the order is as large
        for r in (x for x in full if not x["labelled"]):
            before = [x for x in full if not x["labelled"] and x is not r
                      and (x["recipients"], -x["n_buyers"], x["funder"]) < (r["recipients"], -r["n_buyers"], r["funder"])]
            assert (r["funder"] in {k["funder"] for k in un}) == all(x["n_buyers"] < r["n_buyers"] for x in before)


def test_a_label_the_query_did_not_see_is_a_blind_spot_and_is_counted():
    """Why the label is read in the query: a funder that only a local list rules out hides a smaller group."""
    full = [dict(mint="M", funder="EXCH", labelled=False, recipients=3, n_buyers=50, window_days=30),
            dict(mint="M", funder="REAL", labelled=False, recipients=9, n_buyers=7, window_days=30)]
    kept = eb.reduce_reference(full)
    assert [r["funder"] for r in kept] == ["EXCH"]                                   # REAL is gone
    assert eb.cluster_actors(full, line=1.0, labelled_cex={"EXCH"}) == 7
    assert eb.cluster_actors(kept, line=1.0, labelled_cex={"EXCH"}) == 0             # a lower bound, not the answer
    assert eb.blind_spots(kept, labelled_cex={"EXCH"}) == {"tokens": 1, "tokens_lower_bound_only": 1, "query_only_labels": 0}
    seen = [dict(r, labelled=r["funder"] == "EXCH") for r in full]                   # the same fact, read in the query
    assert eb.cluster_actors(eb.reduce_reference(seen), line=1.0) == 7
    assert eb.blind_spots(eb.reduce_reference(seen))["query_only_labels"] == 1


# ---------------------------------------------------------------- the reader

def krow(**kw) -> dict:
    """One exported row, as the query returns it."""
    r = dict(mint="MINT", grad_time="2025-06-09 10:00:00.000 UTC", funder="F1", labelled=False, recipients=60, window_days=30,
             n_buyers=6, buy_s=[3, 10, 10, 50, 700, 900], n_early=800, n_funded=500, n_groups=900, n_pairs=2500, n_max_any=140,
             vqr=0.0, ev_no_slot=0, ev_no_txi=0, t_first_s=40, q_first=85e9, b_first=2e14,
             t1_s=[950, 4000, None], q_t1=[90e9, 60e9, None], b_t1=[1.9e14, 2.5e14, None],
             px_t1=[90e9 / 1.9e14, 60e9 / 2.5e14, None], net_base=[3e12, 1e12, -2e11], n_sold_pre=[1, 4, 6])
    r.update(kw)
    return r


def test_rate_is_recipients_per_day_of_the_month():
    assert eb.rate(krow(recipients=60, window_days=30)) == 2.0
    assert eb.rate(krow(recipients=0)) == 0.0                          # sent nothing in the month: measured, and it links
    assert eb.rate(krow(funder=None, recipients=None)) is None


def test_funder_kind_is_label_first_and_the_line_is_a_rate():
    k = lambda r, **kw: eb.funder_kind(r, line=2.0, **kw)  # noqa: E731
    assert k(krow(recipients=59)) == "dedicated" and k(krow(recipients=60)) == "hub"
    assert k(krow(recipients=1, labelled=True)) == "cex"               # the query's label, whatever the fan-out
    assert k(krow(recipients=1), labelled_cex={"F1"}) == "cex"         # the local list
    assert k(krow(recipients=9000), factories={"F1"}) == "factory" and k(krow(recipients=1), nonpersonal={"F1"}) == "nonpersonal"
    assert k(krow(funder=None, recipients=None)) is None
    with pytest.raises(TypeError):
        eb.funder_kind(krow())                                         # no default line


def test_detection_is_the_size_th_earliest_member_buy():
    assert eb.SYNDICATE_MIN == 5
    assert eb.detection_s(krow()) == 700 and eb.detection_s(krow(), size=3) == 10 and eb.detection_s(krow(), size=6) == 900
    assert eb.detection_s(krow(), size=7) is None                      # six members: it never reaches seven
    big = krow(n_buyers=40, buy_s=[1, 2, 3, 4, 5, 6, 7, 8])
    assert eb.detection_s(big, size=8) == 8
    with pytest.raises(ValueError):
        eb.detection_s(big, size=9)                                    # not answerable from eight offsets: say so


def test_at_lag_reads_one_element_per_lag():
    a15, a60, a240 = (eb.at_lag(krow(), g) for g in eb.LAGS)
    assert a15["detected_s"] == 700 and a15["detectable"] and a60["detectable"]
    assert not eb.at_lag(krow(buy_s=[3, 10, 10, 50, 900, 901]), 15)["detectable"]       # reached five AT the entry, not before
    assert (a15["holding"], a60["holding"], a240["holding"]) == (True, True, False)
    assert (a15["n_sold_pre"], a240["n_sold_pre"]) == (1, 6)
    assert a15["t1"] == dt.datetime(2025, 6, 9, 10, 15, 50, tzinfo=dt.timezone.utc) and a15["px_t1"] == 90e9 / 1.9e14
    assert a240["t1"] is None and a240["px_t1"] is None                                  # no member sold in that window
    assert eb.at_lag(krow(net_base=[None, 1.0, 1.0]), 15)["holding"] is False            # no pool trade before the lag


def test_exit_value_is_the_ruled_fill():
    """MR-14 order 5: curve proceeds with q + v, never more than the real reserve q."""
    v = eb.exit_value(1e12, 80e9, 2e14, 0.0, sell_cost_bps=0.0)
    assert v["spot"] == pytest.approx(4e8) and v["curve"] == pytest.approx(80e9 * 1e12 / 2.01e14)
    assert v["exit_adjusted"] == v["exit_gross"] == v["curve"] and not v["capped_by_reserve"]
    assert v["ratio"] == pytest.approx(2e14 / 2.01e14)
    # a pool nearly drained of real SOL, priced by its virtual reserve: the exit pays what is really there
    c = eb.exit_value(5e13, 1e9, 2e14, 17.58e9, sell_cost_bps=0.0)
    assert c["curve"] == pytest.approx(18.58e9 * 5e13 / 2.5e14) and c["capped_by_reserve"] and c["exit_gross"] == 1e9
    assert c["ratio"] == pytest.approx(1e9 / (5e13 * 18.58e9 / 2e14))
    n = eb.exit_value(1e12, 80e9, 2e14, 0.0, sell_cost_bps=125.0)
    assert n["exit_adjusted"] == pytest.approx(n["exit_gross"] * 0.9875)
    assert eb.exit_value(1e12, 80e9, 2e14, None, sell_cost_bps=0.0) is None          # no derived reserve: no value, never an assumed one
    assert eb.exit_value(1e12, None, None, 0.0, sell_cost_bps=0.0) is None
    with pytest.raises(TypeError):
        eb.exit_value(1e12, 80e9, 2e14, 0.0)                                           # the sell cost is never silently zero
    r = krow(vqr=17.58e9)
    assert eb.exit_at_lag(r, 60, 1e12, sell_cost_bps=0.0)["curve"] == pytest.approx((60e9 + 17.58e9) * 1e12 / 2.51e14)
    assert eb.exit_at_lag(r, 240, 1e12, sell_cost_bps=0.0) is None and eb.exit_at_lag(krow(vqr=None), 15, 1e12, sell_cost_bps=0.0) is None


def test_cluster_columns_raw_and_collapsed_side_by_side():
    rows = [krow(funder="HUB", recipients=90000, n_buyers=140),                         # the any-funder maximum
            krow(funder="EX", labelled=True, recipients=4, n_buyers=90),
            krow(funder="F1", recipients=60, n_buyers=6),
            krow(funder="F0", recipients=3, n_buyers=2, buy_s=[5, 9])]
    c = eb.cluster_columns(rows, line=10.0)
    assert (c["cluster_n"], c["cluster_src_raw"]) == (140, "HUB")                       # raw: the schema's cluster_n as written
    assert (c["cluster_actors"], c["cluster_src"], c["is_syndicate"]) == (6, "F1", True)
    assert c["cluster_px_t1"][15] == 90e9 / 1.9e14 and c["cluster_t1"][240] is None and set(c["lags"]) == set(eb.LAGS)
    low = eb.cluster_columns(rows, line=1.0)                                            # F1 is a hub at this line
    assert (low["cluster_actors"], low["cluster_src"], low["is_syndicate"]) == (2, "F0", False)
    none = eb.cluster_columns(rows, line=0.05)
    assert (none["cluster_actors"], none["cluster_src"], none["is_syndicate"], none["lags"]) == (0, None, False, {})
    assert eb.cluster_columns(rows, line=10.0, size=7)["is_syndicate"] is False


def test_a_token_with_no_group_and_a_token_with_no_row_are_different_things():
    empty = krow(funder=None, labelled=None, recipients=None, n_buyers=None, buy_s=None, n_early=None, n_max_any=None, vqr=None)
    c = eb.cluster_columns([empty], line=10.0)
    assert (c["cluster_n"], c["cluster_actors"], c["is_syndicate"], c["cluster_src"]) == (0, 0, False, None)   # measured: nothing
    m = eb.cluster_columns([], line=10.0)
    assert (m["cluster_n"], m["cluster_actors"], m["is_syndicate"]) == (None, None, None)                    # not measured
    assert eb.groups([empty]) == [] and eb.raw_twin([]) is None


def test_virtual_reserve_cross_check_against_the_event_export():
    rows = [krow(mint="A", vqr=17584505500.0), krow(mint="A", funder="F2", vqr=17584505500.0), krow(mint="B", vqr=0.0),
            krow(mint="C", vqr=None), krow(mint="D", vqr=None), krow(mint="E", vqr=5.0), krow(mint="G", vqr=1.0),
            krow(mint="H", vqr=1.0), krow(mint="H", funder="F2", vqr=2.0), krow(mint="I", funder=None, vqr=None)]
    ev = [dict(mint="A", vqr=17584505540.0), dict(mint="B", vqr=17584505500.0), dict(mint="C", vqr=None),
          dict(mint="D", vqr=0.0), dict(mint="E", vqr=None), dict(mint="H", vqr=1.0), dict(mint="I", vqr=0.0)]
    x = eb.vqr_cross_check(rows, ev)
    assert (x["tokens"], x["agree"], x["both_null"]) == (8, 1, 1) and x["max_abs_diff"] == 17584505500.0
    assert (x["differ"], x["null_here_only"], x["null_in_events_only"], x["not_in_events"], x["not_one_value"]) == \
           (["B"], ["D"], ["E"], ["G"], ["H"])


def test_fill_counts_look_inside_the_arrays():
    rows = [krow(), krow(funder="F2", px_t1=[1.0, 60e9 / 2.5e14, None]), krow(mint="B", vqr=None, px_t1=[None, None, None], ev_no_txi=4),
            krow(mint="C", funder=None)]
    f = eb.fill_counts(rows)
    assert (f["rows"], f["tokens"], f["tokens_no_group"], f["tokens_vqr_null"], f["tokens_ev_no_txi"], f["tokens_ev_no_slot"]) == (4, 3, 1, 1, 1, 0)
    assert f["null"]["t1_s"] == [0, 0, 3] and f["null"]["px_t1"] == [1, 1, 3] and f["px_not_from_reserves"] == 1


def test_parse_ts_reads_the_format_dune_returns():
    assert eb.parse_ts("2025-06-09 23:59:58.000 UTC") == dt.datetime(2025, 6, 9, 23, 59, 58, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------- 2. the text

LARGE = {"tokens_solana.sol_transfers": 2, "pumpdotfun_solana.pump_amm_evt_buyevent": 1, "pumpdotfun_solana.pump_amm_evt_sellevent": 1}
BOUND = r"(block_date|evt_block_date|call_block_date)\s+(BETWEEN|=)\s+DATE '|block_time >= TIMESTAMP '"     # gen_chunks.review's own
HEADER = "-- Rows name wallets: run with --private-rows. No threshold, no classification, no verdict here."
COLUMNS = ["mint", "grad_time", "funder", "labelled", "recipients", "window_days", "n_buyers", "buy_s", "n_early", "n_funded",
           "n_groups", "n_pairs", "n_max_any", "vqr", "ev_no_slot", "ev_no_txi", "t_first_s", "q_first", "b_first",
           "t1_s", "q_t1", "b_t1", "px_t1", "net_base", "n_sold_pre"]


def test_no_registered_constant_at_any_scope():
    assert len(SCOPES) == 22
    for a, b in SCOPES:
        assert vc.hits(b23.cluster(a, b)) == [], (a, b)


def test_the_day_count_of_the_month_is_injected_once_as_window_days():
    """30 is a registered constant (and 20, the days of the last chunk, which this text never carries): the
    month's day count may stand in one place only, as a returned column."""
    seen = set()
    for a, b in SCOPES:
        sql = b23.cluster(a, b)
        t0, t1, days = gen_fanout.month_window(dt.date.fromisoformat(a))
        seen.add(days)
        code = vc.strip(sql)        # comments and quoted literals out: INTERVAL '30' MINUTE is not a 30
        assert [m.group(0) for m in re.finditer(rf"(?<![\w.]){days}(?![\w.])( AS window_days)?", code)] == [f"{days} AS window_days"]
        assert f"TIMESTAMP '{t0} 00:00:00'" in sql and f"TIMESTAMP '{t1} 00:00:00'" in sql      # the calendar month (MR-15 B3)
        n = (dt.date.fromisoformat(b) - dt.date.fromisoformat(a)).days + 1
        if n in (12, 20):           # the two partial chunks: their own length is nowhere in the text
            assert not re.search(rf"(?<![\w.]){n}(?![\w.])", code)
    assert seen == {28, 30, 31}
    last = b23.cluster("2026-09-01", "2026-09-20")      # a partial chunk still takes its whole month
    assert "TIMESTAMP '2026-09-01 00:00:00'" in last and "TIMESTAMP '2026-10-01 00:00:00'" in last and "30 AS window_days" in last


def test_every_large_table_is_read_as_designed_under_a_literal_bound():
    for a, b in SCOPES:
        sql = b23.cluster(a, b)
        for table, n in LARGE.items():
            refs = list(re.finditer(re.escape(table) + r"\b", sql))
            assert len(refs) == n, (table, a, b)
            for m in refs:
                assert re.search(BOUND, sql[m.end():m.end() + 900]), (table, a, b)
        assert f"DATE '{a}'" in sql and f"DATE '{b}'" in sql                                     # the chunk's own dates
        d0, d1 = dt.date.fromisoformat(a), dt.date.fromisoformat(b)
        # events: to the third day after the last graduation day (a pool a day late, 240 minutes, 24 hours)
        assert sql.count(f"evt_block_date BETWEEN DATE '{a}' AND DATE '{d1 + dt.timedelta(days=3)}' AND pool IN (SELECT pool FROM u)") == 2
        # funding: 7 days before the first graduation day, to the end of the day after the last
        assert (f"s.block_time >= TIMESTAMP '{d0 - dt.timedelta(days=7)} 00:00:00' AND s.block_time < "
                f"TIMESTAMP '{d1 + dt.timedelta(days=2)} 00:00:00'") in sql
        assert sql.count("AND CAST(s.amount AS double) >= 1e6") == 2 and "AND s.to_owner <> s.from_owner" in sql


def test_header_owner_filter_and_nothing_but_a_select():
    for a, b in SCOPES:
        sql = b23.cluster(a, b)
        assert sql.split("\n")[1] == HEADER
        assert "__NOT_OWNER(w)__" in sql and "__NOT_OWNER(funder)__" in sql
        assert re.findall(r"__\w+\(([^)]*)\)__", sql) == ["w", "funder"]                          # arguments the runner can substitute
        assert not re.search(r"\b(CREATE|INSERT|DELETE|UPDATE)\b", sql) and "/*" not in sql
        assert not re.search(r"block_date[^\n]*\bOR\b|\bOR\b[^\n]*block_date", sql)
        assert "{" not in sql and "}" not in sql and "HAVING" not in sql
        code = vc.strip(sql)
        assert code.count("(") == code.count(")") and code.count("[") == code.count("]")
        assert "'" not in "".join(re.findall(r"--.*$", sql, re.M))       # an apostrophe in a comment can hide code from the tripwire


def test_every_block_is_referenced_once():
    """A WITH block is executed again at every reference (85 credits against 14.65 for one output)."""
    sql = b23.cluster("2026-08-01", "2026-08-31")
    names = re.findall(r"^(?:WITH )?(\w+) AS \(", sql, re.M)
    assert names == ["comp", "pc", "u", "ev", "evh", "wal", "early", "fund0", "fund", "grp", "fan", "lab", "gl", "gf", "ranked", "kept"]
    refs = {n: len(re.findall(rf"\b(?:FROM|JOIN)\s+{n}\b", vc.strip(sql))) for n in names}
    assert refs.pop("u") == 4           # two small event tables: both sides of the event pass, the horizon, the final row per token
    assert set(refs.values()) == {1}, refs


def test_one_text_for_every_scope():
    """Every definition is per token: between a one-day proof and a chunk nothing changes but the literal dates
    and the month's day count."""
    def shape(a, b):
        return re.sub(r"\d+ AS window_days", "N AS window_days", re.sub(r"\d{4}-\d{2}-\d{2}", "D", b23.cluster(a, b)))
    assert len({shape(a, b) for a, b in SCOPES}) == 1


def test_a_scope_across_two_months_is_refused():
    with pytest.raises(ValueError):
        b23.cluster("2025-06-25", "2025-07-05")


def test_generator_and_reader_agree_on_lags_and_offsets():
    sql = b23.cluster(*PROVING["day"])
    assert f", 1, {eb.BUY_S_KEPT}) AS buy_s" in sql
    for name, col in (("t1_s", "c.t1_{}"), ("q_t1", "c.q1_{}"), ("b_t1", "c.b1_{}"), ("net_base", "c.hold_{}"), ("n_sold_pre", "c.nsp_{}")):
        assert "ARRAY[" + ", ".join(col.format(g) for g in eb.LAGS) + f"] AS {name}" in sql
    assert set(re.findall(r"INTERVAL '(\d+)' MINUTE", sql)) == {str(g) for g in eb.LAGS} | {"30"}
    assert "INTERVAL '7' DAY" in sql and set(eb.PER_LAG) < set(COLUMNS)


def test_proving_files_on_disk_are_what_the_generator_returns():
    for tag, (a, b) in PROVING.items():
        assert (SQL / f"heavy_b3_cluster_{tag}.sql").read_text(encoding="utf-8") == b23.cluster(a, b), tag
        assert vc.hits((SQL / f"heavy_b3_cluster_{tag}.sql").read_text(encoding="utf-8")) == []
        assert (SQL / f"heavy_b2_auth_{tag}.sql").read_text(encoding="utf-8") == b23.auth(a, b), tag


def test_the_authority_query_was_not_touched():
    """b2 is a scheduled family generated from the same module as this one: the chunk file on disk, written
    before the early-buyer rewrite, is still what the generator returns."""
    assert (SQL / "chunks" / "2026-08_b2.sql").read_text(encoding="utf-8") == b23.auth("2026-08-01", "2026-08-31")
    assert "sol_transfers" not in b23.auth("2026-08-01", "2026-08-31") and "__NOT_OWNER" not in b23.auth("2026-08-01", "2026-08-31")


# ---------------------------------------------------------------- 3. the text, executed

MIN, HOUR, DAY = 60, 3600, 86400
UNITS = {"MINUTE": MIN, "HOUR": HOUR, "DAY": DAY}
OWNER = key("owner")


def epoch(d) -> int:
    d = dt.date.fromisoformat(d) if isinstance(d, str) else d
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())


def day(t: int) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).date().isoformat()


def to_sqlite(sql: str) -> str:
    """The generated text with timestamps as epoch seconds, dates as ISO strings, intervals as seconds and
    arrays as JSON, and the owner placeholder filled as the runner fills it. Nothing else is rewritten."""
    s = re.sub(r"__NOT_OWNER\((\w+)\)__", lambda m: f"{m.group(1)} NOT IN ('{OWNER}')", sql)
    s = re.sub(r"TIMESTAMP '(\d{4}-\d{2}-\d{2}) 00:00:00'", lambda m: str(epoch(m.group(1))), s)
    s = re.sub(r"DATE '(\d{4}-\d{2}-\d{2})'", r"'\1'", s)
    s = re.sub(r"INTERVAL '(\d+)' (MINUTE|HOUR|DAY)", lambda m: str(int(m.group(1)) * UNITS[m.group(2)]), s)
    return s.replace("ARRAY[", "json_array(").replace("]", ")")


class _By:          # min_by / max_by; a NULL key is never the minimum, as in Trino
    better = staticmethod(lambda a, b: a < b)

    def __init__(self):
        self.k = self.v = None

    def step(self, v, k):
        if k is not None and (self.k is None or self.better(k, self.k)):
            self.k, self.v = k, v

    def inverse(self, v, k):
        pass

    def value(self):
        return self.v

    finalize = value


class _MaxBy(_By):
    better = staticmethod(lambda a, b: a > b)


class _CountIf:
    def __init__(self):
        self.n = 0

    def step(self, x):
        self.n += bool(x)

    def inverse(self, x):
        self.n -= bool(x)

    def value(self):
        return self.n

    finalize = value


class _Collect:     # arbitrary, bool_or, array_agg, approx_distinct, by what `fold` does with the values
    fold = staticmethod(lambda xs: None)

    def __init__(self):
        self.xs = []

    def step(self, x):
        self.xs.append(x)

    def finalize(self):
        return self.fold(self.xs)


def _agg(fold):
    return type("Agg", (_Collect,), {"fold": staticmethod(fold)})


def connect(data: dict) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    for schema in ("pumpdotfun_solana", "tokens_solana", "cex_solana"):
        con.execute(f"ATTACH ':memory:' AS {schema}")
    con.create_window_function("max_by", 2, _MaxBy)
    con.create_window_function("min_by", 2, _By)
    con.create_window_function("count_if", 1, _CountIf)
    con.create_aggregate("arbitrary", 1, _agg(lambda xs: next((x for x in xs if x is not None), None)))
    con.create_aggregate("bool_or", 1, _agg(lambda xs: int(any(xs))))
    con.create_aggregate("array_agg", 1, _agg(lambda xs: json.dumps(xs)))
    con.create_aggregate("approx_distinct", 1, _agg(lambda xs: len({x for x in xs if x is not None})))
    con.create_function("least", 2, lambda a, b: None if a is None or b is None else min(a, b))
    con.create_function("date_diff", 3, lambda unit, a, b: None if a is None or b is None else b - a)
    con.create_function("array_sort", 1, lambda js: json.dumps(sorted(json.loads(js))))
    con.create_function("slice", 3, lambda js, start, n: json.dumps(json.loads(js)[start - 1:start - 1 + n]))
    ev_cols = ("pool", '"user"', "evt_block_time", "evt_block_date", "evt_block_slot", "evt_tx_index",
               "pool_quote_token_reserves", "pool_base_token_reserves")
    tables = {
        "pumpdotfun_solana.pump_evt_completeevent": (("mint", "evt_block_time", "evt_block_date"),
                                                     [(m, ct, day(ct)) for m, ct in data["completions"]]),
        "pumpdotfun_solana.pump_amm_evt_createpoolevent": (("base_mint", "quote_mint", "pool", "evt_block_time", "evt_block_date"),
                                                           [(m, qm, p, pt, day(pt)) for m, qm, p, pt in data["pools"]]),
        "pumpdotfun_solana.pump_amm_evt_buyevent": (ev_cols + ("quote_amount_in", "user_quote_amount_in", "base_amount_out"),
                                                    [(e["pool"], e["w"], e["t"], day(e["t"]), e["slot"], e["txi"], e["q"], e["b"], e["qa"], e["qb"], e["base"])
                                                     for e in data["events"] if e["side"] == "buy"]),
        "pumpdotfun_solana.pump_amm_evt_sellevent": (ev_cols + ("base_amount_in",),
                                                     [(e["pool"], e["w"], e["t"], day(e["t"]), e["slot"], e["txi"], e["q"], e["b"], e["base"])
                                                      for e in data["events"] if e["side"] == "sell"]),
        "tokens_solana.sol_transfers": (("from_owner", "to_owner", "amount", "block_time"), data["transfers"]),
        "cex_solana.addresses": (("address",), [(a,) for a in data["labels"]]),
    }
    for name, (cols, rows) in tables.items():
        con.execute(f"CREATE TABLE {name} ({', '.join(cols)})")
        con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(cols))})", rows)
    return con


def run(data: dict, gd0: str, gd1: str) -> tuple[list[str], dict]:
    cur = connect(data).execute(to_sqlite(b23.cluster(gd0, gd1)))
    cols = [c[0] for c in cur.description]
    out = {}
    for rec in cur.fetchall():
        r = dict(zip(cols, rec))
        for c in ("buy_s", *eb.PER_LAG):
            r[c] = None if r[c] is None else json.loads(r[c])
        r["labelled"] = None if r["labelled"] is None else bool(r["labelled"])
        r["grad_time"] = dt.datetime.fromtimestamp(r["grad_time"], dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000 UTC")
        assert (r["mint"], r["funder"]) not in out
        out[r["mint"], r["funder"]] = r
    return cols, out


def synthetic(seed: int = 3) -> dict:
    """Made-up tokens, pool events and SOL transfers around 2025-06-03 and 2025-06-04. Built to hold every case
    the definitions distinguish: buys on both sides of 30 minutes, sells on both sides of each lag and of each
    24-hour end, events past the horizon, two events in one second in different slots, an event with no
    transaction index and one with no slot, funding on both sides of the 7 days, of graduation and of the
    0.001 SOL floor, self-transfers, a funder active in the month before only, labelled funders, the owner
    wallet as buyer and as funder, a pool with only dust buys, a pool that stops trading before the last lag,
    a token with no funded buyer, pools that are not admitted (wrong quote mint, created more than a day after
    completion, a second pool on a mint), and two hand-made tokens at the end of the function."""
    rng = random.Random(seed)
    wsol = b23.WSOL
    wallets = [key(("w", i)) for i in range(26)] + [OWNER]
    funders = [key(("f", i)) for i in range(9)] + [OWNER]
    d3, d4 = epoch("2025-06-03"), epoch("2025-06-04")
    tokens = [("T0", d3 + 2 * HOUR, 40), ("T1", d3 + 9 * HOUR, 700), ("T2", d3 + 23 * HOUR + 50 * MIN, 30 * MIN),   # pool on the next day
              ("T3", d4 + 5 * HOUR, 5), ("T4", d4 + 22 * HOUR, 3 * HOUR), ("DUST", d3 + 6 * HOUR, 60), ("LONE", d4 + 1 * HOUR, 90),
              ("CALM", d3 + 12 * HOUR, 20)]
    comp = [(m, ct) for m, ct, _ in tokens] + [("USDC", d3 + HOUR), ("LATE", d3 + 3 * HOUR)]
    pools = [(m, wsol, "P" + m, ct + lag) for m, ct, lag in tokens] + [("USDC", "EPjFWdd5", "PUSDC", d3 + HOUR + 9),
                                                                      ("LATE", wsol, "PLATE", d3 + 3 * HOUR + DAY + 1),
                                                                      ("T0", wsol, "PT0b", d3 + 5 * HOUR)]       # a second pool: not the graduation pool
    events, seq = [], 0
    for mint, _qm, pool, grad in pools:
        q, b = 85e9, 2.06e14
        marks = [0, 1, 29 * MIN, 30 * MIN, 30 * MIN + 1] + [g * MIN + x for g in eb.LAGS for x in (-1, 0, 1, DAY - 1, DAY, DAY + 1)]
        times = sorted(marks + [rng.randrange(0, 31 * HOUR) for _ in range(150)] + [rng.randrange(0, 40 * MIN) for _ in range(90)])
        if mint == "LONE":
            times = times[:6]
        if mint == "CALM":
            times = [t for t in times if t < 3 * HOUR]      # nothing trades at the last lag: no sell to find there
        edge = {30 * MIN: key(("edge", mint, "in")), 30 * MIN + 1: key(("edge", mint, "out"))}     # one buy each, on the line and past it
        lag_marks = {g * MIN + x for g in eb.LAGS for x in (-1, 0, 1, DAY - 1, DAY, DAY + 1)}
        for i, off in enumerate(times):
            seq += 1
            side = "buy" if (off <= 30 * MIN and rng.random() < 0.8) or rng.random() < 0.35 else "sell"
            w = wallets[rng.randrange(len(wallets))] if mint != "LONE" else key(("lone", i))
            if off in edge and edge[off] not in {e["w"] for e in events}:
                side, w = "buy", edge[off]
            elif off in lag_marks and mint != "LONE":
                side = "sell"
            # no two buys of one size: the largest buy of a pool is then one event, in SQL and in the model alike
            net = float(rng.randrange(2_000, 900_000) * 10_000 + seq) if mint != "DUST" else float(rng.randrange(1000, 999_999))
            fee = float(rng.randrange(1000, 90_000))
            base = float(int(b * net / (q + 17.58e9 * (mint in ("T3", "T4")) + net))) if side == "buy" else float(rng.randrange(10**9, 10**12))
            qa, qb = (net, net + fee) if rng.random() < 0.5 else (net + fee, net)       # the two quote fields swap by variant
            t = grad + off
            events.append(dict(pool=pool, side=side, w=w, t=t, slot=2 * t + (seq % 2), txi=seq % 997, q=q, b=b, qa=qa, qb=qb, base=base))
            q, b = (q + net, b - base) if side == "buy" else (max(q - 1e6, 1e9), b + base)
        mine = [e for e in events if e["pool"] == pool]
        mine[len(mine) // 3]["txi"] = None                # ordered by slot alone
        if mint == "T1":
            next(e for e in mine if e["side"] == "sell" and e["t"] > grad + HOUR)["slot"] = None     # can never be a first sell
    transfers = []
    for mint, _qm, pool, grad in pools:
        buyers = sorted({e["w"] for e in events if e["pool"] == pool})
        if mint == "LONE":
            continue
        for w in buyers:
            for _ in range(rng.randrange(0, 4)):
                f = rng.choice(funders) if rng.random() < 0.35 else funders[rng.randrange(4)]
                t = grad - rng.choice([1, 0, -5, 7 * DAY, 7 * DAY + 1, 7 * DAY - 1, rng.randrange(1, 9 * DAY)])
                transfers.append((f, w, rng.choice([999_999, 1_000_000, 5_000_000_000]), t))
            if rng.random() < 0.3:
                transfers.append((w, w, 3_000_000, grad - HOUR))                         # to itself: not a funder
        for w in wallets[:6] + [key(("edge", mint, "in")), key(("edge", mint, "out"))]:
            transfers.append((key("may"), w, 2_000_000, epoch("2025-05-30") + 7))        # a funder that sent nothing in June
        if mint == "T3":
            for w in wallets[:20]:
                transfers.append((OWNER, w, 2_000_000, epoch("2025-05-30") + 9))         # the owner as a funder, of a large group
    for i, f in enumerate(funders[:-1]):                    # recipients over June: 0 for the last (active in May only), ties among others
        for j in range((0, 3, 3, 40, 7, 1, 12, 2, 0)[i]):
            transfers.append((f, key(("r", i, j)), 2_000_000, epoch("2025-06-01") + rng.randrange(30 * DAY)))
        transfers.append((f, key(("r", i, "may")), 2_000_000, epoch("2025-05-31") + 100))  # the month before: not counted
        transfers.append((f, key(("r", i, "dust")), 999_999, epoch("2025-06-10")))          # under the floor: not counted
        transfers.append((f, f, 9_000_000, epoch("2025-06-11")))                           # to itself: not counted

    # Hand-made groups, one for each line of the definitions that random trading never lands on.
    def trade(pool, side, w, t, slot=None, txi=1, q=85e9, net=3e6):
        b = 2.06e14
        events.append(dict(pool=pool, side=side, w=w, t=t, slot=2 * t if slot is None else slot, txi=txi, q=q, b=b,
                           qa=net, qb=net + 5000.0, base=float(int(b * net / (q + net)))))

    g = d3 + 15 * HOUR + 30
    comp.append(("EDGE", g - 30))
    pools.append(("EDGE", wsol, "PEDGE", g))
    for i, (x, n) in enumerate((("x1", 1), ("x2", 2), ("x3", 3), ("x4", 4))):     # larger with more recipients: all four are kept
        for j in range(n):
            trade("PEDGE", "buy", key((x, j)), g + 5 + 10 * i + j, net=9e8 - 1e6 * (10 * i + j))
            transfers.append((key(x), key((x, j)), 2_000_000, g - HOUR))
    trade("PEDGE", "sell", key(("x1", 0)), g + 28 * HOUR)             # at the end of the horizon: not in it
    trade("PEDGE", "sell", key(("x2", 0)), g + 15 * MIN + DAY)        # at the end of the first lag window: not in it
    trade("PEDGE", "sell", key(("x3", 0)), g + 60 * MIN + DAY)        # and of the second
    t = g + 2 * HOUR                                                   # one second, two slots: the lower slot is first
    trade("PEDGE", "sell", key(("x4", 1)), t, slot=2 * t + 1, txi=3, q=70e9)
    trade("PEDGE", "sell", key(("x4", 2)), t, slot=2 * t, txi=500, q=71e9)
    t = g + 5 * HOUR                                                   # one slot, two transactions: the lower index is first
    trade("PEDGE", "sell", key(("x4", 2)), t, txi=7, q=60e9)
    trade("PEDGE", "sell", key(("x4", 3)), t, txi=2, q=61e9)
    # completed in the last minute of 06-04 with its pool late on 06-05: the latest graduation a scope ending 06-04 can
    # hold. Its funding is a second before, and its last lag window reaches 06-07: the far end of every literal bound.
    g2 = epoch("2025-06-05") + 23 * HOUR + 50 * MIN
    comp.append(("EDGE2", d4 + 23 * HOUR + 59 * MIN))
    pools.append(("EDGE2", wsol, "PEDGE2", g2))
    trade("PEDGE2", "buy", key("z"), g2 + 3)
    trade("PEDGE2", "sell", key("z"), g2 + 27 * HOUR)
    transfers.append((key("x5"), key("z"), 2_000_000, g2 - 1))
    return dict(completions=comp, pools=pools, events=events, transfers=transfers, labels=[funders[2], funders[2], funders[5], key("unused")])


def model(data: dict, gd0: str, gd1: str) -> tuple[dict, list[dict]]:
    """The definitions, written out plainly from the module docstring of the generator: no SQL, no sorting trick.
    The literal scan bounds of the text are deliberately not repeated here: agreement with the text then also shows
    that those bounds cut off nothing a token needs. Returns the rows the query should return, and the full group
    table they were reduced from."""
    d0, d1 = dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)
    m0, m1, mdays = gen_fanout.month_window(d0)
    labels = set(data["labels"])
    out, full = {}, []
    for mint, ct in data["completions"]:
        if not gd0 <= day(ct) <= gd1:
            continue
        cands = sorted((pt, qm, p) for m, qm, p, pt in data["pools"] if m == mint and gd0 <= day(pt) <= (d1 + dt.timedelta(days=1)).isoformat())
        if not cands or not (ct <= cands[0][0] < ct + DAY) or cands[0][1] != b23.WSOL:
            continue
        grad, _qm, pool = cands[0]
        evs = [e for e in data["events"] if e["pool"] == pool and grad <= e["t"] < grad + 28 * HOUR]
        key_ = lambda e: None if e["slot"] is None else e["slot"] * 100000 + (e["txi"] or 0)  # noqa: E731
        big = [e for e in evs if e["side"] == "buy" and e["base"] > 0 and min(e["qa"], e["qb"]) >= 1e6]
        v = None
        if big:
            x = max(big, key=lambda e: min(e["qa"], e["qb"]))
            net = min(x["qa"], x["qb"])
            v = x["b"] * net / x["base"] - x["q"] - net
        first_buy = {}
        for e in evs:
            if e["side"] == "buy" and e["t"] <= grad + 30 * MIN and e["w"] != OWNER:
                first_buy[e["w"]] = min(first_buy.get(e["w"], e["t"]), e["t"])
        members: dict[str, set] = {}
        for f, w, amount, t in data["transfers"]:
            if w in first_buy and amount >= 1e6 and f != w and f != OWNER and grad - 7 * DAY <= t < grad:
                members.setdefault(f, set()).add(w)
        table = []
        for f, ws in members.items():
            sells = [e for e in evs if e["side"] == "sell" and e["w"] in ws and key_(e) is not None]

            def first(lo, hi):
                s = min((e for e in sells if lo <= e["t"] < hi), key=key_, default=None)
                return (None, None, None) if s is None else (s["t"] - grad, s["q"], s["b"])

            t1, q1, b1, hold, nsp = [], [], [], [], []
            for g in eb.LAGS:
                a, b, c = first(grad + g * MIN, grad + g * MIN + DAY)
                t1.append(a), q1.append(b), b1.append(c)
                before = [e for e in evs if e["w"] in ws and e["t"] < grad + g * MIN]
                hold.append(sum(e["base"] if e["side"] == "buy" else -e["base"] for e in before) if before else None)
                nsp.append(len({e["w"] for e in before if e["side"] == "sell"}))
            tf, qf, bf = first(grad, grad + 28 * HOUR)
            table.append(dict(
                mint=mint, funder=f, labelled=f in labels, window_days=mdays, n_buyers=len(ws),
                recipients=len({to for fr, to, amount, t in data["transfers"] if fr == f and to != fr and amount >= 1e6 and epoch(m0) <= t < epoch(m1)}),
                buy_s=sorted(first_buy[w] - grad for w in ws)[:eb.BUY_S_KEPT], n_early=len(first_buy), vqr=v,
                ev_no_slot=sum(e["slot"] is None for e in evs), ev_no_txi=sum(e["txi"] is None for e in evs),
                t_first_s=tf, q_first=qf, b_first=bf, t1_s=t1, q_t1=q1, b_t1=b1,
                px_t1=[None if q is None or v is None else (q + v) / b for q, b in zip(q1, b1)], net_base=hold, n_sold_pre=nsp))
        for r in table:
            r.update(n_groups=len(table), n_pairs=sum(x["n_buyers"] for x in table), n_max_any=max(x["n_buyers"] for x in table),
                     n_funded=len(set().union(*members.values())))
        stamp = dt.datetime.fromtimestamp(grad, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000 UTC")
        o = lambda x: (0 if x["labelled"] else x["recipients"], -x["n_buyers"], x["funder"])  # noqa: E731
        for r in table:     # kept: no group of the same label part that comes before it in the order is as large
            if all(x["n_buyers"] < r["n_buyers"] for x in table if x["labelled"] == r["labelled"] and o(x) < o(r)):
                out[mint, r["funder"]] = dict(r, grad_time=stamp)
        if not table:
            out[mint, None] = dict(mint=mint, grad_time=stamp, funder=None)
        full += table
    return out, full


def _same(a, b) -> bool:
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and a == pytest.approx(b, rel=1e-12, abs=1e-6)
    return a == b


def test_the_text_executed_returns_what_the_definitions_say():
    data = synthetic()
    cols, got = run(data, "2025-06-03", "2025-06-04")
    want, full = model(data, "2025-06-03", "2025-06-04")
    assert cols == COLUMNS
    assert set(got) == set(want)
    # the Python reference of the reduction keeps exactly the groups the executed text keeps, out of all of them
    assert {(r["mint"], r["funder"]) for r in eb.reduce_reference(full)} == {k for k in got if k[1]} and len(full) > 2 * len(got)
    for k, w in want.items():
        g = got[k]
        if k[1] is None:        # a token with no group: one row, the token and its graduation, everything else NULL
            assert g["grad_time"] == w["grad_time"] and g["window_days"] == 30
            assert all(g[c] is None or g[c] == [None] * 3 for c in COLUMNS if c not in ("mint", "grad_time", "window_days")), k
            continue
        for c in COLUMNS:
            assert _same(g[c], w[c]), (k[0], c, g[c], w[c])
    # the made-up data did exercise the cases: several tokens, a staircase, a label, a token with no group, a pool
    # with no derived reserve, sells missing at a lag, an order key that could not be built
    mints = {m for m, _f in got}
    assert mints == {"T0", "T1", "T2", "T3", "T4", "DUST", "LONE", "CALM", "EDGE", "EDGE2"}
    # the hand-made groups, against values worked out by hand and not by the model
    x = {f: got["EDGE", key(f)] for f in ("x1", "x2", "x3", "x4")}
    assert [x[f]["n_buyers"] for f in x] == [1, 2, 3, 4] == [x[f]["recipients"] for f in x]
    assert x["x1"]["t1_s"] == [None, None, None] and x["x1"]["t_first_s"] is None                # its one sell is past the horizon
    assert x["x2"]["t1_s"] == [None, 15 * MIN + DAY, 15 * MIN + DAY] and x["x3"]["t1_s"] == [None, None, 60 * MIN + DAY]
    assert x["x4"]["t1_s"] == [2 * HOUR, 2 * HOUR, 5 * HOUR] and x["x4"]["q_t1"] == [71e9, 71e9, 61e9]   # slot, then index
    assert x["x4"]["buy_s"] == [35, 36, 37, 38] and x["x4"]["n_sold_pre"] == [0, 0, 2] and x["x4"]["n_early"] == 10
    assert got["EDGE2", key("x5")]["t1_s"] == [None, None, 27 * HOUR]
    rows = list(got.values())
    assert max(sum(1 for m, f in got if m == x and f) for x in mints) >= 3 and any(r["labelled"] for r in rows)
    assert got["LONE", None]["funder"] is None and all(r["vqr"] is None for (m, f), r in got.items() if m == "DUST" and f)
    assert {round(r["vqr"] / 1e9, 2) for (m, f), r in got.items() if f and m != "DUST"} == {0.0, 17.58}
    assert all(r["t1_s"][2] is None and r["t1_s"][0] is not None for (m, f), r in got.items() if m == "CALM" and f)
    assert all(None not in r["t1_s"] for (m, f), r in got.items() if m == "T0" and f)
    assert all(r["ev_no_txi"] == (0 if m.startswith("EDGE") else 1) and r["ev_no_slot"] == (m == "T1") for (m, f), r in got.items() if f)
    assert any(r["recipients"] == 0 for r in rows if r["funder"]) and all(r["funder"] != OWNER for r in rows)
    assert eb.fill_counts(rows)["px_not_from_reserves"] == 0
    for m in mints:             # and the reader reads them
        c = eb.cluster_columns([r for (x, _f), r in got.items() if x == m], line=0.2)
        assert c["cluster_n"] is not None and (c["cluster_actors"] or 0) <= c["cluster_n"]


def test_a_token_gets_the_same_rows_wherever_it_falls_in_the_scope():
    """The defect found in the aligned-set query: a definition that varied with the position in the chunk."""
    data = synthetic()
    _c, one = run(data, "2025-06-03", "2025-06-03")
    _c, two = run(data, "2025-06-03", "2025-06-04")
    _c, last = run(data, "2025-06-04", "2025-06-04")
    assert {m for m, _f in one} == {"T0", "T1", "T2", "DUST", "CALM", "EDGE"} and {m for m, _f in last} == {"T3", "T4", "LONE", "EDGE2"}
    assert {**one, **last} == two


def test_the_owner_placeholder_is_what_keeps_the_owner_out():
    data = synthetic()
    sql = to_sqlite(b23.cluster("2025-06-03", "2025-06-04")).replace(f"NOT IN ('{OWNER}')", "NOT IN ('nobody')")
    funders = {r[2] for r in connect(data).execute(sql).fetchall()}
    assert OWNER in funders     # the made-up owner funds and buys: without the filter it comes back
