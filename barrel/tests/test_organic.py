"""Organic-v2: the generated text (recon/gen_org2.py) and the local reducer (recon/organic.py).
Nothing here talks to Dune and no exported row is read: every key below is made up.
Run: python -m pytest barrel/tests/test_organic.py"""
import ast
import datetime as dt
import itertools
import pathlib
import random
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "recon"))
import actors  # noqa: E402
import gen_org2 as g  # noqa: E402
import organic  # noqa: E402
import verdict_constants as vc  # noqa: E402

PROOFS = {"day": ("2025-06-09", "2025-06-09"), "week": ("2025-06-09", "2025-06-15"), "pbday": ("2026-09-01", "2026-09-01")}
# the chunks whose day counts are 12, 30, 31, 31 and 20: 30 and 20 are registered constants
CHUNKS = [("2025-03-20", "2025-03-31"), ("2025-06-01", "2025-06-30"), ("2026-07-01", "2026-07-31"),
          ("2026-08-01", "2026-08-31"), ("2026-09-01", "2026-09-20")]
BOUNDS = list(PROOFS.values()) + CHUNKS
ONE_EACH = {g.BUY: 1, g.SELL: 1, g.TRADE: 1, g.SOL: 1}


# ---------------------------------------------------------------- the text

@pytest.mark.parametrize("gd0,gd1", BOUNDS)
def test_no_registered_constant_in_any_text(gd0, gd1):
    assert vc.hits(g.build(gd0, gd1)) == []
    assert vc.hits(g.bundle_takers(gd0, gd1)) == []


@pytest.mark.parametrize("gd0,gd1", BOUNDS)
def test_the_text_passes_its_own_reading(gd0, gd1):
    assert g.self_check(g.build(gd0, gd1), gd0, gd1) == []
    assert g.self_check(g.bundle_takers(gd0, gd1), gd0, gd1, ONE_EACH) == []


@pytest.mark.parametrize("gd0,gd1", BOUNDS)
def test_designed_references_and_placeholders(gd0, gd1):
    sql = g.build(gd0, gd1)
    count = lambda table: len(re.findall(re.escape(table) + r"\b", sql))  # noqa: E731
    assert (count(g.BUY), count(g.SELL), count(g.TRADE), count(g.SOL)) == (2, 2, 1, 1)
    assert count("tokens_solana.transfers") == 0                 # the token ledger is not read here
    assert "WHERE w IS NOT NULL AND __NOT_OWNER(w)__" in sql      # the owner filter, before any count
    assert "(VALUES __FS_CODES__) AS t(mint, w, code)" in sql and "WHERE mint IS NOT NULL" in sql
    assert "__FS_SUPPLIED__ AS fs_supplied" in sql
    assert f"DATE '{gd0}'" in sql and f"DATE '{gd1}'" in sql
    assert sql.split("\n")[1] == g.NO_WALLET and "--private-rows" not in sql
    assert g.bundle_takers(gd0, gd1).split("\n")[1] == g.WALLETS


def test_every_large_table_is_followed_by_its_literal_bound():
    """The spellings the static review asks for, with the dates one post-BOOST day must give."""
    sql = g.build(*PROOFS["pbday"])
    after = lambda table: [sql[m.end():m.end() + 900] for m in re.finditer(re.escape(table) + r"\b", sql)]  # noqa: E731
    takers, history = "evt_block_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-09' AND pool IN (SELECT pool FROM u)", \
        "evt_block_date BETWEEN DATE '2026-07-03' AND DATE '2026-09-09'"
    for table in (g.BUY, g.SELL):
        first, second = after(table)
        assert takers in first[:200] and history in second[:200]          # 7 x 24 h of takers; 60 days of history
    assert "evt_block_date BETWEEN DATE '2026-08-29' AND DATE '2026-09-01'" in after(g.TRADE)[0][:200]   # 3 days of creations
    assert ("s.block_time >= TIMESTAMP '2026-08-28 00:00:00' AND s.block_time < TIMESTAMP '2026-09-02 00:00:00'"
            in after(g.SOL)[0][:200])                                      # the 24 h before a creation in that scope
    assert "evt_block_date BETWEEN DATE '2026-06-23' AND DATE '2026-09-01' AND mint IN" in sql           # the event query's 70 days
    assert sql.count("CASE WHEN evt_block_date >= DATE '2026-08-25' THEN pool END") == 2                 # pool grain: the bot week on
    assert "WHERE d >= DATE '2026-09-01')," in sql                                                       # only days a swap can fall on leave wf


def test_the_bounds_of_the_first_chunk_reach_back_before_the_venue_existed():
    """2025-03-20 .. 03-31, dates worked by hand. The history scan starts 60 days earlier, where there is nothing
    to read: the first weeks of the window cannot show an aged wallet, whatever the lookback."""
    sql = g.build(*CHUNKS[0])
    assert sql.count("evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-04-08' AND pool IN (SELECT pool FROM u)") == 2
    assert sql.count("WHERE evt_block_date BETWEEN DATE '2025-01-19' AND DATE '2025-04-08')") == 1       # sell half of the history
    assert sql.count("WHERE evt_block_date BETWEEN DATE '2025-01-19' AND DATE '2025-04-08'\n") == 1      # buy half
    assert sql.count("CASE WHEN evt_block_date >= DATE '2025-03-13' THEN pool END") == 2
    assert "evt_block_date BETWEEN DATE '2025-03-20' AND DATE '2025-04-01')," in sql                     # pools: a day after the last completion
    assert "evt_block_date BETWEEN DATE '2025-01-09' AND DATE '2025-03-31' AND mint IN" in sql
    assert "t.evt_block_date BETWEEN DATE '2025-03-17' AND DATE '2025-03-31' AND k.s8_scope)" in sql
    assert "s.block_time >= TIMESTAMP '2025-03-16 00:00:00' AND s.block_time < TIMESTAMP '2025-04-01 00:00:00'" in sql
    assert "WHERE d >= DATE '2025-03-20')," in sql


def test_the_per_token_flags_are_measured_from_the_tokens_own_graduation_day():
    sql = g.build(*CHUNKS[3])
    assert "coalesce(cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '3' DAY, false) AS s8_scope" in sql
    assert "coalesce(cr.t0 >= date_trunc('day', u.grad_time) - INTERVAL '70' DAY, false) AS cr_own" in sql
    assert "k.cr_own AS has_creator" in sql and "AND k.s8_scope)" in sql
    assert "e.t >= k.grad_time AND e.t < k.grad_time + INTERVAL '7' DAY" in sql
    assert "s.block_time BETWEEN k.t0 - INTERVAL '24' HOUR AND k.t0" in sql


def test_the_text_differs_between_scopes_only_in_its_dates():
    """Every definition is per token: nothing but the literal bounds knows how long the chunk is."""
    masked = {re.sub(r"\d{4}-\d\d-\d\d", "DATE", g.build(a, b)) for a, b in BOUNDS}
    assert len(masked) == 1


def test_no_window_reads_the_swap_day_or_after():
    sql = g.build(*PROOFS["pbday"])
    assert sql.count("RANGE BETWEEN") == sql.count("AND INTERVAL '1' DAY PRECEDING)") == 6
    assert "FOLLOWING" not in sql and "CURRENT ROW" not in sql and "UNBOUNDED" not in sql
    assert "h.t_seen <= e.t - INTERVAL '24' HOUR" in sql          # aged is judged against the swap's own time
    assert "h.d = e.d" in sql                                     # and the bot week is the one before the swap's own day


def test_the_ruled_numbers_are_in_the_text_and_the_bundle_cut_is_not():
    sql = g.build(*PROOFS["pbday"])
    assert (g.AGE_HOURS, g.HOLD_SECONDS, g.POOLS_MAX, g.BOT_DAYS) == (24, 60, 200, 7)      # MR-7
    assert sql.count("> 200") == 4 and "fs < fb + INTERVAL '60' SECOND" in sql
    assert "h.fast7 + h.fast7 >= h.rt7" in sql                    # "at least half", with no constant to multiply by
    assert "max(nsf) AS bn" in sql and not re.search(r"\bbn\s*[<>]", sql)   # the count is returned, never cut


def _blocks(sql: str) -> dict[str, str]:
    parts = re.split(r"^(\w+) AS \(", sql.replace("WITH ", "", 1), flags=re.M)
    return dict(zip(parts[1::2], parts[2::2]))


def test_the_head_without_history_is_the_same_taker_set():
    """A later query that collapses organic counts by funder must collapse exactly this set."""
    full, short = _blocks(g.head(*PROOFS["pbday"])[0]), _blocks(g.head(*PROOFS["pbday"], history=False)[0])
    assert set(full) - set(short) == {"wpd", "wd", "wf"}
    for name in ("comp", "pc", "u", "cr", "tok", "slot0", "hitb", "cand", "fsc", "tk", "pw", "pk"):
        assert full[name] == short[name], name
    assert "min(slot) AS slot1" in full["pw"] and "p.slot1" in full["pk"]      # the slot of first action travels with the set


def test_the_static_review_passes_with_the_designed_counts(monkeypatch):
    """gen_chunks.review is the lead's check. org2 is not registered there by this file; the counts it must be
    registered with are set here, and every text must then review clean."""
    import gen_chunks
    for table, n in g.DESIGNED.items():
        monkeypatch.setitem(gen_chunks.LARGE[table], "org2", n)
    for gd0, gd1 in BOUNDS:
        assert gen_chunks.review("org2", g.build(gd0, gd1), dt.date.fromisoformat(gd0), dt.date.fromisoformat(gd1)) == []


@pytest.mark.parametrize("tag", list(PROOFS))
def test_the_proving_files_on_disk_are_what_the_generator_writes(tag):
    assert (ROOT / "recon" / "sql" / f"org2_{tag}.sql").read_text(encoding="utf-8") == g.build(*PROOFS[tag])


def test_self_check_sees_a_second_consumer_and_a_missing_bound():
    sql = g.build(*PROOFS["day"])
    assert any("tk: 2 consumers" in p for p in g.self_check(sql + "\n-- x\nSELECT 1 FROM tk", *PROOFS["day"]))
    assert any("no literal bound" in p for p in g.self_check(sql.replace("s.block_time < TIMESTAMP", "s.block_time <= TIMESTAMP"), *PROOFS["day"]))
    # a scan that lost its own bound is not excused by the bound of the table written right after it
    # (within 900 characters, which is all the static review asks)
    lost = sql.replace("\n    WHERE evt_block_date BETWEEN DATE '2025-04-10' AND DATE '2025-06-17'\n    UNION ALL", "\n    UNION ALL")
    assert lost != sql and any(g.BUY in p and "no literal bound" in p for p in g.self_check(lost, *PROOFS["day"]))
    assert any("verdict constant" in p for p in g.self_check(sql.replace("max(nsf) AS bn", "max(nsf) AS bn, max(nsf) >= 5 AS cut"), *PROOFS["day"]))


# ---------------------------------------------------------------- the reducer

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
HOUR, WEEK = 3600, 7 * 86400


def key(rng: random.Random) -> str:
    """A made-up 32-byte key in base58."""
    n, s = int.from_bytes(rng.randbytes(32), "big"), ""
    while n:
        n, r = divmod(n, 58)
        s = _B58[r] + s
    return s


def takers(rng: random.Random, n: int) -> list[dict]:
    """One token's takers as the query's pk block holds them: one row per wallet, every key fixed for it."""
    out = []
    for i in range(n):
        t1 = rng.randrange(WEEK)
        t2 = rng.choice([None, t1, rng.randrange(t1, WEEK)])
        out.append({"w": key(rng), "cls": "C" if i == 0 else "M" if i == 1 else "O",
                    "fsx": rng.choice([None, None, None, "E", "L", "P"]),
                    "bn": rng.choice([None, None, None, 1, 2, 3, 4, 5, 6, 9, 40]),
                    "gs": rng.random() < 0.2, "t1": t1, "t2": t2,
                    "any_aged": t2 is not None or rng.random() < 0.3, "any_notbot": t2 is not None or rng.random() < 0.3,
                    "any_many": rng.random() < 0.2, "any_quick": rng.random() < 0.3})
    return out


def cells_from(mint: str, ws: list[dict], *, s8_scope=True, has_creator=True, fs_supplied=True) -> list[dict]:
    """The query's `cell` block and final SELECT, in Python: GROUP BY (cls, fsx, bn, gs), and one row with no
    cell for a token with no taker."""
    tail = {"s8_scope": s8_scope, "has_creator": has_creator, "lb_days": 60, "fs_supplied": fs_supplied}
    zero = {"n1": 0, "n2": 0, "n1_1h": 0, "n2_1h": 0, "t1_s": None, "t2_s": None,
            "n_aged": 0, "n_notbot": 0, "n_many": 0, "n_quick": 0, "n_nohist": 0}
    groups: dict[tuple, list[dict]] = {}
    for w in ws:
        groups.setdefault((w["cls"], w["fsx"], w["bn"], w["gs"]), []).append(w)
    out = []
    for (cls, fsx, bn, gs), m in groups.items():
        t2 = [w["t2"] for w in m if w["t2"] is not None]
        out.append({"mint": mint, "cls": cls, "fsx": fsx, "bn": bn, "gs": gs,
                    "n1": len(m), "n2": len(t2), "n1_1h": sum(w["t1"] < HOUR for w in m), "n2_1h": sum(t < HOUR for t in t2),
                    "t1_s": min(w["t1"] for w in m), "t2_s": min(t2) if t2 else None,
                    "n_aged": sum(w["any_aged"] for w in m), "n_notbot": sum(w["any_notbot"] for w in m),
                    "n_many": sum(w["any_many"] for w in m), "n_quick": sum(w["any_quick"] for w in m), "n_nohist": 0, **tail})
    return out or [{"mint": mint, "cls": None, "fsx": None, "bn": None, "gs": None, **zero, **tail}]


CODE_SETS = [set(c) for k in range(4) for c in itertools.combinations(organic.FS_CODES, k)]


@pytest.mark.parametrize("has_creator", [True, False])
def test_cells_add_up_to_a_direct_count_over_wallets_for_every_cut_and_every_code_set(has_creator):
    """The property the whole design rests on: each wallet is in exactly one cell, so the reducer's sums and
    minima equal a count over the wallets themselves, whatever the bundle cut and whichever codes exclude.
    Where the creation is not within the token's own 70 days, the creator is counted like anyone else."""
    rng = random.Random(20261008)
    out = ("C", "M") if has_creator else ("M",)
    for _ in range(40):
        ws = takers(rng, rng.randrange(0, 80))
        rows = cells_from(key(rng), ws, has_creator=has_creator)
        assert organic.problems(rows) == []
        for cut in range(1, 12):
            for codes in CODE_SETS:
                org = [w for w in ws if w["cls"] not in out and w["fsx"] not in codes and (w["bn"] is None or w["bn"] < cut)]
                v2 = [w["t2"] for w in org if w["t2"] is not None]
                rec = organic.token_record(rows, fs_exclude=codes, bundle_min=cut)
                assert rec["org1_n_7d"] == len(org) and rec["org2_n_7d"] == len(v2)
                assert rec["org1_t_s"] == (min(w["t1"] for w in org) if org else None)
                assert rec["org2_t_s"] == (min(v2) if v2 else None)
                assert rec["org1_n_1h"] == sum(w["t1"] < HOUR for w in org) and rec["org2_n_1h"] == sum(t < HOUR for t in v2)
                assert rec["n_takers"] == len(ws) == rec["org1_n_7d"] + sum(rec["excluded"].values())
                assert rec["gap"]["n_aged"] == sum(w["any_aged"] for w in org)


def cell(mint="m", cls="O", fsx=None, bn=None, gs=False, n1=1, n2=0, n1_1h=0, n2_1h=0, t1_s=0, t2_s=None, **kw):
    aged = kw.pop("n_aged", n2)
    return {"mint": mint, "cls": cls, "fsx": fsx, "bn": bn, "gs": gs, "n1": n1, "n2": n2, "n1_1h": n1_1h, "n2_1h": n2_1h,
            "t1_s": t1_s, "t2_s": t2_s, "n_aged": aged, "n_notbot": kw.pop("n_notbot", n2), "n_many": 0, "n_quick": 0,
            "n_nohist": 0, "s8_scope": True, "has_creator": True, "lb_days": 60, "fs_supplied": True, **kw}


ROWS = [cell(n1=100, n2=40, n1_1h=30, n2_1h=4, t1_s=2, t2_s=95),
        cell(gs=True, n1=7, n2=1, n1_1h=7, n2_1h=1, t1_s=0, t2_s=0),            # graduation-slot traders
        cell(cls="C", fsx="E", bn=1, n1=1, t1_s=30),                              # the creator, also a fee-share recipient
        cell(cls="M", n1=1, t1_s=0),
        cell(fsx="L", n1=2, n2=2, n1_1h=1, n2_1h=1, t1_s=50, t2_s=60),
        cell(bn=4, n1=4, n2=1, n1_1h=4, t1_s=1, t2_s=9000),                      # four creation-slot traders behind one funder
        cell(bn=5, n1=5, n2=5, n1_1h=5, n2_1h=5, t1_s=0, t2_s=0)]                # five: a bundle by the ruled cut


def test_the_record_of_one_token():
    rec = organic.token_record(ROWS, fs_exclude=organic.FS_THROUGH_DAY_7)
    assert (rec["org1_n_7d"], rec["org2_n_7d"]) == (111, 42)       # 100 + 7 + 4; the bundle of five and code L are out
    assert (rec["org1_t_s"], rec["org2_t_s"]) == (0, 0)            # a graduation-slot trader is organic in the ruled v1
    assert (rec["org1_n_1h"], rec["org2_n_1h"]) == (41, 5)         # org2_n_1h is H5's marker
    assert rec["n_takers"] == 120 and rec["excluded"] == {"creator": 1, "migrator": 1, "fee_share": 2, "bundle": 5}
    assert rec["org1_actors_7d"] is None and rec["org2_actors_7d"] is None      # no query collapses them yet
    assert rec["s8_state"] == "measured" and rec["fs_state"] == "applied" and rec["u_codes"] == [] and rec["lb_days"] == 60


def test_which_codes_exclude_is_the_callers_choice_and_has_no_default():
    with pytest.raises(TypeError):
        organic.token_record(ROWS)
    with pytest.raises(ValueError):
        organic.token_record(ROWS, fs_exclude={"X"})
    assert organic.token_record(ROWS, fs_exclude=())["org1_n_7d"] == 113
    assert organic.token_record(ROWS, fs_exclude=organic.FS_AT_ENTRY)["org1_n_7d"] == 113     # the only E is the creator
    assert organic.token_record(ROWS, fs_exclude=organic.FS_EVER)["org1_n_7d"] == 111


def test_the_bundle_cut_is_the_one_in_actors(monkeypatch):
    assert organic.token_record(ROWS, fs_exclude=())["excluded"]["bundle"] == 5
    monkeypatch.setattr(actors, "BUNDLE_MIN", 4)
    assert organic.token_record(ROWS, fs_exclude=())["excluded"]["bundle"] == 9
    numbers = {n.value for n in ast.walk(ast.parse(pathlib.Path(organic.__file__).read_text(encoding="utf-8")))
               if isinstance(n, ast.Constant) and type(n.value) in (int, float)}
    assert actors.BUNDLE_MIN not in numbers                        # the reducer has no number of its own for the cut


def test_what_was_not_measured_is_said_and_never_hidden_in_the_number():
    out = [dict(c, s8_scope=False, bn=None) for c in ROWS]
    rec = organic.token_record(out, fs_exclude=())
    assert rec["s8_state"] == "unknown" and rec["u_codes"] == [organic.U_S8] and rec["excluded"]["bundle"] == 0
    assert organic.token_record(ROWS, fs_exclude=(), n_slot0=0)["s8_state"] == "unknown"       # none decoded (b1a's n_slot0)
    assert organic.token_record(ROWS, fs_exclude=(), n_slot0=3)["s8_state"] == "measured"
    rec = organic.token_record([dict(c, fs_supplied=False, fsx=None) for c in ROWS], fs_exclude=organic.FS_EVER)
    assert rec["fs_state"] == "not_supplied" and rec["u_codes"] == [organic.U_S7B] and rec["excluded"]["fee_share"] == 0
    # created more than 70 days before its own graduation day: the creator's cell counts as anyone else's,
    # whether or not the chunk's longer lookback happened to find the creator
    rec = organic.token_record([dict(c, has_creator=False) for c in ROWS], fs_exclude=())
    assert rec["u_codes"] == [organic.U_CREATOR] and rec["org1_n_7d"] == 114 and rec["excluded"]["creator"] == 0
    assert organic.token_record([dict(c, has_creator=False) for c in ROWS], fs_exclude=organic.FS_AT_ENTRY)["excluded"]["fee_share"] == 1


def test_no_taker_is_a_measured_zero_and_no_row_is_not_measured():
    rows = cells_from("m", [])
    rec = organic.token_record(rows, fs_exclude=())
    assert (rec["org1_n_7d"], rec["org2_n_7d"], rec["org1_t_s"], rec["org2_n_1h"]) == (0, 0, None, 0) and rec["u_codes"] == []
    assert organic.problems(rows) == [] and organic.proxy_v1(rows) == {"org1_n_7d": 0, "org1_t_s": None}
    rec = organic.token_record([], fs_exclude=())
    assert rec["org1_n_7d"] is None and rec["org2_n_7d"] is None and rec["u_codes"] == [organic.U_NOT_MEASURED]


def test_the_proxy_is_what_the_event_query_calls_org1():
    """Class O, graduation-slot flag false, summed over every other key: fee-share and bundle wallets included."""
    assert organic.proxy_v1(ROWS) == {"org1_n_7d": 111, "org1_t_s": 0}
    assert organic.reconcile_proxy(ROWS, 111, 0) == "equal"
    assert organic.reconcile_proxy(cells_from("m", []), None, None) == "equal"       # the event query's NULL is a zero
    assert organic.reconcile_proxy(ROWS, 112, 0) == "differs"                         # one more, and no owner wallet to explain it
    assert organic.reconcile_proxy(ROWS, 112, 0, n_owner=2) == "owner"                # expected: the event query keeps owner wallets
    assert organic.reconcile_proxy(ROWS, 114, 0, n_owner=2) == "differs"              # more than the owner has wallets
    assert organic.reconcile_proxy(ROWS, 110, 0, n_owner=2) == "differs"              # fewer can never be the owner filter
    assert organic.reconcile_proxy(ROWS, 111, 1, n_owner=2) == "differs"              # same count, other first swap
    assert organic.reconcile_proxy(ROWS, 112, 5, n_owner=2) == "differs"              # an extra taker cannot make the first swap later


def test_rows_of_two_runs_or_two_tokens_are_refused():
    with pytest.raises(ValueError):
        organic.token_record(ROWS + [cell(mint="other")], fs_exclude=())
    with pytest.raises(ValueError):
        organic.token_record(ROWS + [cell(bn=2, fs_supplied=False)], fs_exclude=())
    with pytest.raises(ValueError):
        organic.token_record(ROWS + [cell(fsx="Z", gs=True)], fs_exclude=())


def test_problems_names_what_is_wrong_with_a_tokens_rows():
    assert organic.problems(ROWS) == []
    bad = lambda **kw: organic.problems(ROWS + [cell(bn=2, **kw)])  # noqa: E731
    assert any("twice" in p for p in organic.problems(ROWS + [ROWS[0]]))                   # the token is in the universe twice
    assert any("out of order" in p for p in bad(n1=1, n2=2, t2_s=5))
    assert any("no wallet-history row" in p for p in bad(n_nohist=1))
    assert any("without its time" in p for p in bad(n1=3, n2=1))
    assert any("differs between rows" in p for p in bad(lb_days=30))
    assert any("no cell beside" in p for p in organic.problems(ROWS + cells_from("m", [])))
    assert any("more than one wallet" in p for p in organic.problems(ROWS + [cell(cls="C", n1=2)]))


def test_same_funder_counts_against_member_rows():
    rng = random.Random(7)
    mint, a, b, c = key(rng), key(rng), key(rng), key(rng)
    assert organic.bundle_hist(ROWS) == {1: 1, 4: 4, 5: 5}
    mine = [{"mint": mint, "w": a, "bn": 6}, {"mint": mint, "w": b, "bn": 2}, {"mint": mint, "w": c, "bn": 5}]
    members = [{"mint": mint, "w": a, "bundle_n": 6}, {"mint": mint, "w": b, "bundle_n": 3}, {"mint": mint, "w": key(rng), "bundle_n": None}]
    assert organic.bundle_takers_check(mine, members) == {"takers": 3, "same": 1, "different": 1, "not_in_members": 1}
    # member rows from before MR-14 carry the cut already applied: only its side can be compared
    older = [{"mint": mint, "w": a, "is_bundle": True}, {"mint": mint, "w": b, "is_bundle": False}, {"mint": mint, "w": c, "is_bundle": False}]
    assert organic.bundle_takers_check(mine, older) == {"takers": 3, "same": 2, "different": 1, "not_in_members": 0}


def test_the_lookback_reaches_past_the_venues_birth_in_the_first_weeks():
    assert organic.lookback_truncated(dt.date(2025, 3, 20), 60) and organic.lookback_truncated(dt.date(2025, 5, 18), 60)
    assert not organic.lookback_truncated(dt.date(2025, 5, 19), 60) and not organic.lookback_truncated(dt.date(2025, 6, 9), 60)
