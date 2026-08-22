"""SC-R2 interim ranking, including the pre-registered pass/fail criteria."""

from __future__ import annotations

import json

import pytest

from scout_daemon import ledger, rank, state, verdicts
from scout_daemon.rank import (
    SEGMENT_AFFILIATE,
    SEGMENT_GREEN,
    SEGMENT_GREEN_PROMOTED,
    SEGMENT_HUMAN_ONLY,
    build_ranking,
    classify_segment,
    expected_usd,
)


class FakeEff:
    def __init__(self, vetoed=False, scans_seen=3, flip_count=0):
        self.vetoed = vetoed
        self.scans_seen = scans_seen
        self.flip_count = flip_count
        self.state = "VETOED" if vetoed else "GREEN"


def row(oid, **kw):
    base = dict(
        opportunity_id=oid, source="opire", title=f"t-{oid}",
        legitimacy_class="GREEN", mechanical_class="GREEN",
        payout_usd_low=100.0, payout_usd_high=200.0, award_rate=None,
        award_rate_observed_unix=None, contention=None,
    )
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# Invariant 1 -- segments never merge
# ---------------------------------------------------------------------------

def test_green_and_green_promoted_are_separate_lists() -> None:
    rows = [row("a", payout_usd_low=10.0),
            row("b", payout_usd_low=999.0, legitimacy_class="GREEN_PROMOTED")]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["a"]
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN_PROMOTED]] == ["b"]


def test_a_higher_paying_promoted_row_never_outranks_inside_green() -> None:
    """The promoted row has the bigger payout; it must not appear in GREEN."""
    rows = [row("a", payout_usd_low=10.0),
            row("b", payout_usd_low=10_000.0, legitimacy_class="GREEN_PROMOTED")]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    assert all(r.opportunity_id != "b" for r in res.ranked[SEGMENT_GREEN])


def test_affiliate_lane_is_parked_with_the_recorded_reason() -> None:
    rows = [row("x", source="affiliate_watch"), row("y", source="affpaying")]
    res = build_ranking(rows, {"x": FakeEff(), "y": FakeEff()})
    assert SEGMENT_AFFILIATE not in res.ranked
    reasons = {r.unranked_reason for r in res.unranked}
    assert reasons == {rank.AFFILIATE_PARK_REASON}
    assert "commission_rate" in rank.AFFILIATE_PARK_REASON


# ---------------------------------------------------------------------------
# Invariant 2 -- absent P(award) stays absent; unrankable is reported
# ---------------------------------------------------------------------------

def test_missing_payout_is_unranked_with_reason_not_sorted_to_the_bottom() -> None:
    rows = [row("a", payout_usd_low=5.0), row("b", payout_usd_low=None)]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["a"]
    assert [r.opportunity_id for r in res.unranked] == ["b"]
    assert res.unranked[0].unranked_reason == rank.UNRANKED_NO_PAYOUT
    assert res.unranked[0].position is None


def test_award_rate_is_never_inferred() -> None:
    assert expected_usd(100.0, None) is None
    assert expected_usd(None, 0.5) is None
    assert expected_usd(100.0, 0.25) == 25.0


# ---------------------------------------------------------------------------
# Invariant 3 -- expected_usd needs both operands; staleness is carried
# ---------------------------------------------------------------------------

def test_expected_usd_computed_only_when_both_operands_present() -> None:
    rows = [row("a", payout_usd_low=100.0, award_rate=0.5,
                award_rate_observed_unix=1700),
            row("b", payout_usd_low=100.0, award_rate=None)]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    got = {r.opportunity_id: r.expected_usd for r in res.ranked[SEGMENT_GREEN]}
    assert got["a"] == 50.0 and got["b"] is None


def test_result_carries_input_observation_window() -> None:
    rows = [row("a", award_rate=0.5, award_rate_observed_unix=100),
            row("b", award_rate=0.5, award_rate_observed_unix=900)]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    assert res.oldest_input_unix == 100 and res.newest_input_unix == 900


def test_expected_usd_is_not_the_sort_key() -> None:
    """Low payout with a high award rate must NOT outrank a bigger payout."""
    rows = [row("small", payout_usd_low=10.0, award_rate=1.0),
            row("big", payout_usd_low=1000.0, award_rate=0.001)]
    res = build_ranking(rows, {"small": FakeEff(), "big": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["big", "small"]


# ---------------------------------------------------------------------------
# E22 -- rank consumes the EFFECTIVE verdict, never the raw one
# ---------------------------------------------------------------------------

def test_effective_vetoed_row_does_not_rank() -> None:
    rows = [row("a"), row("b")]
    res = build_ranking(rows, {"a": FakeEff(vetoed=True), "b": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["b"]


def test_churn_is_exposed_not_excluded() -> None:
    """An edge-breather still ranks; its flip history rides along."""
    rows = [row("a")]
    res = build_ranking(rows, {"a": FakeEff(flip_count=3, scans_seen=4)})
    entry = res.ranked[SEGMENT_GREEN][0]
    assert entry.flip_count == 3 and entry.verdicts_seen == 4
    assert entry.effective_verdict == "GREEN"


def test_red_never_ranks() -> None:
    assert classify_segment(source="opire", legitimacy_class="RED",
                            mechanical_class="GREEN", effective_vetoed=False,
                            affiliate_sources=frozenset()) is None


# ---------------------------------------------------------------------------
# PRE-REGISTERED PASS/FAIL
# ---------------------------------------------------------------------------

def test_two_consecutive_runs_are_byte_identical() -> None:
    """Pre-registered: rank must be reproducible and stable on unchanged data."""
    rows = [row(f"id{i:03d}", payout_usd_low=float(i % 7)) for i in range(60)]
    eff = {r["opportunity_id"]: FakeEff() for r in rows}
    a = build_ranking(rows, eff)
    b = build_ranking(list(reversed(rows)), eff)  # input order must not matter
    dump = lambda res: json.dumps(
        {k: [r.as_dict() for r in v] for k, v in sorted(res.ranked.items())}
        | {"unranked": [r.as_dict() for r in res.unranked]}, sort_keys=True)
    assert dump(a) == dump(b)


def test_every_green_family_row_appears_exactly_once() -> None:
    """Pre-registered count reconciliation: ranked + unranked, no loss, no dupes."""
    rows = (
        [row(f"g{i}", payout_usd_low=float(i)) for i in range(10)]
        + [row(f"n{i}", payout_usd_low=None) for i in range(5)]
        + [row(f"p{i}", legitimacy_class="GREEN_PROMOTED") for i in range(4)]
        + [row(f"a{i}", source="affiliate_watch") for i in range(3)]
    )
    eff = {r["opportunity_id"]: FakeEff() for r in rows}
    res = build_ranking(rows, eff)
    seen = [r.opportunity_id for v in res.ranked.values() for r in v]
    seen += [r.opportunity_id for r in res.unranked]
    assert len(seen) == len(set(seen)), "a row appears twice"
    assert set(seen) == {r["opportunity_id"] for r in rows}
    assert res.total_rows == len(rows)


def test_positions_are_dense_and_start_at_one() -> None:
    rows = [row(f"id{i}", payout_usd_low=float(i)) for i in range(5)]
    res = build_ranking(rows, {r["opportunity_id"]: FakeEff() for r in rows})
    assert [r.position for r in res.ranked[SEGMENT_GREEN]] == [1, 2, 3, 4, 5]


def test_equal_payouts_break_ties_deterministically() -> None:
    rows = [row("zzz", payout_usd_low=5.0), row("aaa", payout_usd_low=5.0)]
    res = build_ranking(rows, {"zzz": FakeEff(), "aaa": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["aaa", "zzz"]


# ---------------------------------------------------------------------------
# Invariants 4 and 5 -- never admits, never calls an LLM
# ---------------------------------------------------------------------------

def _code_strings(module) -> list[str]:
    """Every string literal in the module EXCEPT docstrings.

    Checking raw source is not good enough: this module's own docstring says
    it never writes `admitted`, which made a substring check fail against the
    very sentence promising the guarantee. Only executable literals can carry a
    value into the database, so only those are inspected.
    """
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(module))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            doc = ast.get_docstring(node, clean=False)
            if doc is not None:
                docstrings.add(doc)
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and n.value not in docstrings]


def test_rank_has_no_admission_path() -> None:
    """Invariant 4, test-asserted: ranking orders, it never admits."""
    literals = _code_strings(rank)
    assert not [s for s in literals if "admitted" in s]
    # Nothing in the rank path may write the lifecycle column at all.
    assert not [s for s in literals if "status" in s.lower() and "UPDATE" in s]


def test_rank_makes_no_llm_call() -> None:
    import inspect

    src = inspect.getsource(rank)
    for forbidden in ("anthropic", "Anthropic", "messages.create", "classify_batch"):
        assert forbidden not in src, f"{forbidden} must not appear in the rank path"


# ---------------------------------------------------------------------------
# Persistence round-trip
# ---------------------------------------------------------------------------

@pytest.fixture()
def conn(tmp_path):
    c = state.connect(tmp_path / "r.sqlite3")
    ledger.apply_schema(c)
    return c


def test_migration_adds_rank_columns_to_an_existing_ledger(conn) -> None:
    have = {r[1] for r in conn.execute("PRAGMA table_info(opportunities)")}
    for col in ("rank_segment", "rank_position", "rank_sort_key",
                "rank_unranked_reason", "verdicts_seen", "flip_count",
                "effective_verdict"):
        assert col in have


def test_migration_is_idempotent(conn) -> None:
    before = {r[1] for r in conn.execute("PRAGMA table_info(opportunities)")}
    ledger.apply_schema(conn)
    ledger.apply_schema(conn)
    after = {r[1] for r in conn.execute("PRAGMA table_info(opportunities)")}
    assert before == after


# ---------------------------------------------------------------------------
# The human-only gate -- agent_permitted must GATE, not merely be stored
# ---------------------------------------------------------------------------

def test_human_only_row_never_lands_in_the_agent_queue() -> None:
    """The defect this closes: a mechanically-GREEN human-only row ranked in
    SEGMENT_GREEN indistinguishable from work an agent may take."""
    rows = [row("ok"), row("nope", agent_permitted="no")]
    res = build_ranking(rows, {"ok": FakeEff(), "nope": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["ok"]
    assert [r.opportunity_id for r in res.ranked[SEGMENT_HUMAN_ONLY]] == ["nope"]


def test_a_huge_human_only_payout_cannot_reach_green() -> None:
    """Sorting is by payout, so the gate has to beat the sort key."""
    rows = [row("small", payout_usd_low=10.0),
            row("whale", payout_usd_low=5_000_000.0, agent_permitted="no")]
    res = build_ranking(rows, {"small": FakeEff(), "whale": FakeEff()})
    assert all(r.opportunity_id != "whale" for r in res.ranked[SEGMENT_GREEN])
    assert res.ranked[SEGMENT_HUMAN_ONLY][0].opportunity_id == "whale"


def test_human_only_promoted_row_diverts_out_of_promoted_too() -> None:
    rows = [row("p", legitimacy_class="GREEN_PROMOTED", agent_permitted="no")]
    res = build_ranking(rows, {"p": FakeEff()})
    assert res.ranked[SEGMENT_GREEN_PROMOTED] == []
    assert [r.opportunity_id for r in res.ranked[SEGMENT_HUMAN_ONLY]] == ["p"]


def test_human_only_rows_are_ranked_not_dropped() -> None:
    """Invariant 1: gated is not dropped. They keep a position and a sort key."""
    rows = [row("a", payout_usd_low=10.0, agent_permitted="no"),
            row("b", payout_usd_low=900.0, agent_permitted="no")]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    seg = res.ranked[SEGMENT_HUMAN_ONLY]
    assert [r.opportunity_id for r in seg] == ["b", "a"]
    assert [r.position for r in seg] == [1, 2]
    assert res.total_rows == 2


def test_unstated_is_absence_not_prohibition() -> None:
    """Ruled by Mando 2026-08-21: a silent source does not forbid agents, and a
    GREEN row is the agent-executable class. Only an explicit `no` gates.
    `unstated` is 560 of 669 rows; treating it as a bar would gate 84% of the
    ledger on a fact no source stated, and call that safety."""
    rows = [row("u", agent_permitted="unstated"), row("n", agent_permitted=None)]
    res = build_ranking(rows, {"u": FakeEff(), "n": FakeEff()})
    assert {r.opportunity_id for r in res.ranked[SEGMENT_GREEN]} == {"u", "n"}
    assert res.ranked[SEGMENT_HUMAN_ONLY] == []


def test_gate_cannot_widen_the_ranked_set() -> None:
    """A YELLOW human-only row is unrankable and must STAY unrankable -- the
    diversion runs after the base segment, never before it."""
    assert classify_segment(
        source="opire", legitimacy_class="YELLOW", mechanical_class="YELLOW",
        effective_vetoed=False, affiliate_sources=frozenset(),
        agent_permitted="no",
    ) is None


def test_red_human_only_stays_red_not_human_only() -> None:
    assert classify_segment(
        source="opire", legitimacy_class="RED", mechanical_class="GREEN",
        effective_vetoed=False, affiliate_sources=frozenset(),
        agent_permitted="no",
    ) is None


def test_vetoed_human_only_stays_unrankable() -> None:
    rows = [row("v", agent_permitted="no")]
    res = build_ranking(rows, {"v": FakeEff(vetoed=True)})
    assert res.ranked[SEGMENT_HUMAN_ONLY] == []
    assert res.total_rows == 0


# ---------------------------------------------------------------------------
# A pool is not a payout (SC-Q1 follow-on, 2026-08-21)
# ---------------------------------------------------------------------------

def test_a_program_pool_never_enters_the_per_task_queue() -> None:
    rows = [row("task", payout_usd_low=500.0, payout_basis="per_task"),
            row("pool", payout_usd_low=1_000_000.0, payout_basis="program_pool")]
    res = build_ranking(rows, {"task": FakeEff(), "pool": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["task"]
    assert [r.opportunity_id for r in res.ranked[rank.SEGMENT_POOL]] == ["pool"]


def test_payout_kind_pool_also_diverts() -> None:
    """Either marker is sufficient; sources set one or the other."""
    rows = [row("p", payout_usd_low=9_000.0, payout_basis="per_task",
                payout_kind="pool")]
    res = build_ranking(rows, {"p": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[rank.SEGMENT_POOL]] == ["p"]


def test_a_million_dollar_pool_cannot_outrank_a_bounty_it_never_shares_a_list_with() -> None:
    rows = [row("small", payout_usd_low=50.0, payout_basis="per_task"),
            row("huge", payout_usd_low=1_000_000.0, payout_basis="program_pool")]
    res = build_ranking(rows, {"small": FakeEff(), "huge": FakeEff()})
    green = res.ranked[SEGMENT_GREEN]
    assert len(green) == 1 and green[0].position == 1


def test_pool_rows_are_marked_as_ceilings() -> None:
    rows = [row("pool", payout_usd_low=500.0, payout_basis="program_pool"),
            row("task", payout_usd_low=500.0, payout_basis="per_task")]
    res = build_ranking(rows, {"pool": FakeEff(), "task": FakeEff()})
    assert res.ranked[rank.SEGMENT_POOL][0].payout_is_ceiling is True
    assert res.ranked[SEGMENT_GREEN][0].payout_is_ceiling is False


def test_per_recipient_is_never_claimed_as_verified() -> None:
    """No current source publishes a split. False is the honest default and
    must not drift to True without a source that actually says so."""
    rows = [row("a", payout_basis="per_task"), row("b", payout_basis="program_pool")]
    res = build_ranking(rows, {"a": FakeEff(), "b": FakeEff()})
    every = [r for v in res.ranked.values() for r in v] + res.unranked
    assert all(r.payout_per_recipient_verified is False for r in every)


def test_human_only_beats_pool_when_a_row_is_both() -> None:
    """Eligibility is the stronger fact: an agent cannot execute it at all."""
    rows = [row("x", payout_basis="program_pool", agent_permitted="no")]
    res = build_ranking(rows, {"x": FakeEff()})
    assert [r.opportunity_id for r in res.ranked[rank.SEGMENT_HUMAN_ONLY]] == ["x"]


def test_pool_rows_still_reconcile_exactly_once() -> None:
    rows = ([row(f"t{i}", payout_basis="per_task") for i in range(4)]
            + [row(f"p{i}", payout_basis="program_pool") for i in range(6)])
    eff = {r["opportunity_id"]: FakeEff() for r in rows}
    res = build_ranking(rows, eff)
    seen = [r.opportunity_id for v in res.ranked.values() for r in v]
    seen += [r.opportunity_id for r in res.unranked]
    assert len(seen) == len(set(seen)) == len(rows)
