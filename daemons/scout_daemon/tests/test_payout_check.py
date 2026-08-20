"""Payout integrity cross-check: flag, never correct."""

from __future__ import annotations

import json

from scout_daemon import payout_check as PC
from scout_daemon import rank as R
from scout_daemon.rank import SEGMENT_GREEN


class FakeEff:
    vetoed = False
    scans_seen = 3
    flip_count = 0
    state = "GREEN"


def row(oid, **kw):
    base = dict(
        opportunity_id=oid, source="opire", title=f"t-{oid}",
        legitimacy_class="GREEN", mechanical_class="GREEN",
        payout_usd_low=100.0, payout_usd_high=None, award_rate=None,
        award_rate_observed_unix=None, contention=None,
        scope_text=None, effort_note=None, raw_json=None,
    )
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# FLAG, NEVER CORRECT
# ---------------------------------------------------------------------------

def test_the_module_never_writes_a_scraped_field() -> None:
    """Same structural guard the admissions file carries.

    A cross-check that silently corrected a payout would replace the
    counterparty's claim with our inference, leaving no way to tell which is in
    the ledger.
    """
    import inspect

    src = inspect.getsource(PC)
    for forbidden in ("UPDATE ", "INSERT ", "conn.execute", "commit()"):
        assert forbidden not in src, f"{forbidden} must not appear in the check"


def test_flagging_does_not_change_the_stored_payout() -> None:
    rows = [row("a" * 64, payout_usd_low=100_100.0, title="[BOUNTY] pays $50")]
    before = rows[0]["payout_usd_low"]
    res = R.build_ranking(rows, {"a" * 64: FakeEff()})
    assert rows[0]["payout_usd_low"] == before, "input row was mutated"
    assert res.unranked[0].payout_usd_low == before, "reported value was rewritten"


# ---------------------------------------------------------------------------
# The two-part rule
# ---------------------------------------------------------------------------

def test_big_disagreement_with_no_payload_explanation_flags() -> None:
    """The Kickama shape: field $100,100, title says $50, nothing explains it."""
    assert PC.is_flagged(text="[BOUNTY] Kickama pays $50", payout_low=100_100.0) is True


def test_big_disagreement_the_source_can_explain_does_NOT_flag() -> None:
    """The questbook shape: committed pool vs cumulative disbursed.

    Both figures are correct and mean different things. The ratio alone would
    have demoted four real programs.
    """
    raw = json.dumps({"reward": {"committed": 25_000},
                      "totalGrantFundingDisbursedUSD": 709_300})
    assert PC.is_flagged(text="disbursed $709,300 to date",
                         payout_low=25_000.0, raw_json=raw) is False


def test_ratio_below_the_cut_does_not_flag() -> None:
    assert PC.is_flagged(text="pays $250", payout_low=100.0) is False   # 2.5x


def test_ratio_at_the_cut_flags() -> None:
    assert PC.is_flagged(text="pays $500", payout_low=100.0) is True    # 5.0x


def test_explanation_must_be_within_one_percent() -> None:
    raw = json.dumps({"other_field": 500_000})
    # 709,300 vs 500,000 is far outside 1% -- not an explanation.
    assert PC.is_flagged(text="$709,300", payout_low=25_000.0, raw_json=raw) is True


def test_booleans_in_the_payload_are_not_treated_as_numbers() -> None:
    """bool is an int subclass; True must not 'explain' a $1 figure."""
    raw = json.dumps({"acceptingApplications": True})
    assert PC.explained_by_payload(1.0, raw) is None


# ---------------------------------------------------------------------------
# Charitable matching and measurability
# ---------------------------------------------------------------------------

def test_closest_mention_wins() -> None:
    """A title naming an unrelated sum must not flag on the unrelated one."""
    assert PC.is_flagged(text="up to $100 for the $9,999,999 grand prize",
                         payout_low=100.0) is False


def test_a_figure_inside_the_parsed_range_agrees() -> None:
    d = PC.disagreement(text="pays $750", payout_low=500.0, payout_high=1000.0)
    assert d is not None and d[0] == 1.0


def test_zero_dollar_mentions_are_ignored() -> None:
    """'$0 disbursed' is a status, not a payout claim."""
    assert PC.monetary_mentions("$0 disbursed so far") == []
    assert PC.is_flagged(text="$0 disbursed", payout_low=30_000.0) is False


def test_unmeasurable_rows_are_not_flagged_and_not_passed() -> None:
    assert PC.disagreement(text="no figures here", payout_low=100.0) is None
    assert PC.disagreement(text="$50", payout_low=None) is None
    assert PC.is_flagged(text="no figures here", payout_low=100.0) is False


def test_mentions_parse_both_spellings() -> None:
    assert PC.monetary_mentions("$1,500 and 2000$") == [1500.0, 2000.0]


# ---------------------------------------------------------------------------
# Integration with the ranking
# ---------------------------------------------------------------------------

def test_flagged_row_leaves_the_order_with_its_reason() -> None:
    rows = [row("a" * 64, payout_usd_low=100_100.0, title="pays $50"),
            row("b" * 64, payout_usd_low=500.0, title="pays $500")]
    eff = {r["opportunity_id"]: FakeEff() for r in rows}
    res = R.build_ranking(rows, eff)
    assert [r.opportunity_id for r in res.ranked[SEGMENT_GREEN]] == ["b" * 64]
    flagged = [r for r in res.unranked
               if r.unranked_reason == PC.UNRANKED_PAYOUT_UNVERIFIED]
    assert [r.opportunity_id for r in flagged] == ["a" * 64]


def test_flagged_rows_still_reconcile_exactly_once() -> None:
    """The count-reconciliation invariant survives the new exit path."""
    rows = [row(f"{i:064d}", payout_usd_low=100_100.0, title="pays $50")
            for i in range(4)] + [row("f" * 64, payout_usd_low=500.0)]
    eff = {r["opportunity_id"]: FakeEff() for r in rows}
    res = R.build_ranking(rows, eff)
    seen = [r.opportunity_id for v in res.ranked.values() for r in v]
    seen += [r.opportunity_id for r in res.unranked]
    assert len(seen) == len(set(seen)) == len(rows)
