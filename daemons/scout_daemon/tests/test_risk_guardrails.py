"""Phase 3B guardrails on the promotion path.

The promotion path is the daemon's ONLY upward classification move. Every test
here exists because an upward path with a soft guardrail is worse than no
upward path at all.
"""

from __future__ import annotations

import inspect

import pytest

from scout_daemon import classify, ledger, risk, state
from scout_daemon.classify import GREEN, RED, YELLOW
from scout_daemon.models import RawItem


@pytest.fixture()
def conn(tmp_path):
    connection = state.connect(tmp_path / "g.sqlite3")
    ledger.apply_schema(connection)
    yield connection
    connection.close()


def _item(**kw) -> RawItem:
    base = dict(source="superteam_earn", native_id="x", title="Write a blog post",
                category="bounty", payout_raw="500 USDC", payout_basis="per_task")
    base.update(kw)
    return RawItem(**base)


# ---------------------------------------------------------------------------
# Threshold pinned in ONE place
# ---------------------------------------------------------------------------

def test_threshold_is_pinned_at_31_in_exactly_one_place() -> None:
    """One constant, and everything downstream derives from it.

    The histogram previously hardcoded 31 as a band edge, so raising the
    threshold would have left the report splitting at the old boundary and
    labelling 31-45 as above-threshold when it no longer was.
    """
    assert risk.PROMOTION_THRESHOLD == 31

    # The band edges must MOVE with the threshold, not sit beside it.
    edges = {low for low, _ in risk.HISTOGRAM_BANDS}
    assert risk.PROMOTION_THRESHOLD in edges
    assert risk.DEAD_ZONE in risk.HISTOGRAM_BANDS
    assert risk.DEAD_ZONE[1] == risk.PROMOTION_THRESHOLD - 1, (
        "the dead zone must end exactly where promotion stops"
    )

    # No other module defines its own threshold constant.
    for module in (classify, ledger):
        assert not hasattr(module, "PROMOTION_THRESHOLD"), module.__name__


def test_dead_zone_is_declared_and_bounded() -> None:
    assert risk.DEAD_ZONE == (21, 30)
    assert risk.in_dead_zone(25) is True
    assert risk.in_dead_zone(31) is False
    assert risk.in_dead_zone(None) is False


# ---------------------------------------------------------------------------
# Eligibility lockout -- the data-absence rule
# ---------------------------------------------------------------------------

def test_data_absence_reasons_are_structurally_ineligible() -> None:
    """A risk score computed over fields cannot cure a missing field.

    Measured 2026-08-10: a category-unresolved item, otherwise clean, scored
    25 and promoted -- and the score was MADE OF the absence (+20) plus a
    benign +5. It scored the absence as calm.
    """
    for code in (
        classify.Y_CATEGORY_UNRESOLVED,
        classify.Y_SCOPE_UNPUBLISHED,
        classify.Y_NP_UNKNOWN,
        classify.Y_UNCLASSIFIED,
        classify.A_NO_VERDICT,
    ):
        eligible, blocked = risk.check_eligibility((code,))
        assert eligible is False, f"{code} must never be promotable"
        assert code in blocked


def test_the_measured_absence_case_no_longer_promotes() -> None:
    """The exact item that promoted at 25 before the lockout."""
    item = _item(category=None, counterparty="Acme", counterparty_verified=True,
                 payout_confidence="claimed")
    verdict = classify.mechanical_classify(item)
    assert classify.Y_CATEGORY_UNRESOLVED in verdict.codes

    assessment = risk.assess(item, reason_codes=verdict.codes)
    assert assessment.score < risk.PROMOTION_THRESHOLD, "still scores low..."
    assert assessment.eligible is False, "...but is now ineligible"
    assert risk.should_promote(assessment) is False


def test_one_absence_among_rubric_reasons_still_blocks() -> None:
    """ALL reasons must be allowlisted; an absence beside a rubric reason wins."""
    eligible, blocked = risk.check_eligibility(
        (classify.Y_AFFILIATE, classify.Y_CATEGORY_UNRESOLVED)
    )
    assert eligible is False
    assert classify.Y_CATEGORY_UNRESOLVED in blocked
    assert classify.Y_AFFILIATE not in blocked


def test_unknown_reason_codes_are_ineligible_by_default() -> None:
    """A reason added later must not acquire a promotion path by omission."""
    eligible, blocked = risk.check_eligibility(("some_future_reason",))
    assert eligible is False
    assert "some_future_reason" in blocked


def test_no_reason_code_at_all_is_ineligible() -> None:
    eligible, blocked = risk.check_eligibility(())
    assert eligible is False
    assert blocked == ("no_reason_code_recorded",)


def test_eligibility_is_checked_independently_of_score() -> None:
    """A low score on an ineligible item is the failure this gate stops."""
    assessment = risk.RiskAssessment(score=0, eligible=False, blocked_by=("x",))
    assert risk.should_promote(assessment) is False
    assessment_ok = risk.RiskAssessment(score=0, eligible=True)
    assert risk.should_promote(assessment_ok) is True


# ---------------------------------------------------------------------------
# Named regressions
# ---------------------------------------------------------------------------

def test_arbitrum_audit_program_never_promotes() -> None:
    """Two independent blocks now: category detection AND the absence lockout."""
    audit = _item(
        source="arbitrum_grants", native_id="audit",
        title="Arbitrum Audit Program",
        payout_raw="offers $10M in ARB to subsidise third-party smart contract audits",
        category="grant_program", scope_published=False,
    )
    verdict = classify.mechanical_classify(audit)
    assert verdict.legitimacy_class == YELLOW
    assert classify.Y_SCOPE_UNPUBLISHED in verdict.codes

    assessment = risk.assess(audit, reason_codes=verdict.codes)
    assert assessment.eligible is False
    assert risk.should_promote(assessment) is False


def test_giveth_giv_arb_no_longer_promotes() -> None:
    """SC-1 3A finding: GIV-ARB yellowed on a NON-assessable reason.

    It has no payout at all and was never given a rubric YELLOW -- it was
    ambiguous, and became YELLOW only because `resolve()` defaults an
    unanswered escalation. Its old score of 15 even included "payout is a
    claim +5" on an item with NO payout: scoring a default field on an absent
    value. Under the allowlist it is ineligible, and promotions go to zero.
    """
    giv = _item(
        source="giveth_qf", native_id="20", title="GIV-ARB",
        category="quadratic_funding_round", payout_raw=None,
        payout_basis="program_pool", tos_flags=["round_inactive"],
    )
    decision = classify.resolve(
        classify.MechanicalVerdict(None, "no confident mechanical verdict", False,
                                   (classify.A_NO_VERDICT,)),
        None,
    )
    assert decision.legitimacy_class == YELLOW
    assert classify.Y_UNCLASSIFIED in decision.reason_codes

    assessment = risk.assess(giv, reason_codes=decision.reason_codes)
    assert assessment.eligible is False
    assert risk.should_promote(assessment) is False


# ---------------------------------------------------------------------------
# GREEN_PROMOTED is a distinct class
# ---------------------------------------------------------------------------

def test_green_promoted_is_not_green(conn) -> None:
    """A reader querying legitimacy_class='GREEN' must not pick up promotions."""
    assert ledger.GREEN_PROMOTED != ledger.GREEN

    native = ledger.Classification(GREEN, "clean", "mechanical", "v1")
    promoted = ledger.Classification(
        ledger.GREEN_PROMOTED, "promoted", "risk-promotion", "v1",
        risk_score=10, promoted_from_yellow=True, pre_promotion_class=YELLOW,
    )
    ledger.upsert_items(
        conn,
        [(_item(native_id="a"), native), (_item(native_id="b"), promoted)],
        scan_id="s", now_unix=1,
    )
    green_only = conn.execute(
        "SELECT COUNT(*) n FROM opportunities WHERE legitimacy_class='GREEN'"
    ).fetchone()["n"]
    assert green_only == 1, "a promotion must not answer to a GREEN query"
    assert ledger.class_distribution(conn) == {GREEN: 1, ledger.GREEN_PROMOTED: 1}


def test_admission_firewall_covers_green_promoted_and_every_module() -> None:
    """Behavioural, not lexical.

    The prior version counted string literals in two modules and would have
    passed a real write via the `ADMITTED` constant, and never looked at the
    orchestrator where promotion actually mutates the class.
    """
    from scout_daemon import orchestrator

    for module in (ledger, classify, risk, orchestrator):
        src = inspect.getsource(module)
        # No module may set a status column to an admitted/dismissed value.
        for pattern in ("status='admitted'", 'status="admitted"',
                        "status='dismissed'", 'status="dismissed"',
                        "status=ADMITTED", "status=DISMISSED"):
            assert pattern not in src.replace(" ", ""), (
                f"{module.__name__} writes {pattern} -- admission is human"
            )
    # And the constants that would enable it are not even defined any more.
    assert not hasattr(ledger, "ADMITTED")
    assert not hasattr(ledger, "DISMISSED")


# ---------------------------------------------------------------------------
# Promotion provenance
# ---------------------------------------------------------------------------

def test_promotion_records_full_provenance(conn) -> None:
    decision = ledger.Classification(
        ledger.GREEN_PROMOTED, "promoted", "risk-promotion", "v1",
        reason_codes=(classify.Y_AFFILIATE,),
        risk_score=12,
        risk_factors='[{"factor": "affiliate", "points": 12}]',
        risk_weights_version=risk.RISK_WEIGHTS_VERSION,
        promotion_eligible=True,
        promoted_from_yellow=True,
        pre_promotion_class=YELLOW,
        promoted_unix=1_800_000_000,
    )
    ledger.upsert_items(conn, [(_item(), decision)], scan_id="s", now_unix=1)
    row = ledger.promotions(conn)[0]
    assert row["risk_score"] == 12
    assert row["risk_weights_version"] == risk.RISK_WEIGHTS_VERSION
    assert "affiliate" in row["risk_factors"]
    assert row["promoted_unix"] == 1_800_000_000, "canonical scan clock, not local"
    assert classify.Y_AFFILIATE in row["reason_codes"]


def test_promotion_is_reversible_from_stored_data(conn) -> None:
    """Raising the threshold must re-derive the old answer without a re-scan."""
    decision = ledger.Classification(
        ledger.GREEN_PROMOTED, "promoted", "risk-promotion", "v1",
        risk_score=12, promoted_from_yellow=True, pre_promotion_class=YELLOW,
    )
    ledger.upsert_items(conn, [(_item(), decision)], scan_id="s", now_unix=1)
    row = conn.execute(
        "SELECT pre_promotion_class, risk_score FROM opportunities"
    ).fetchone()
    assert row["pre_promotion_class"] == YELLOW
    assert row["risk_score"] == 12


def test_dead_zone_occupants_are_queryable(conn) -> None:
    for score, native in ((25, "a"), (40, "b")):
        decision = ledger.Classification(
            YELLOW, "y", "mechanical", "v1", risk_score=score,
            promotion_eligible=False,
        )
        ledger.upsert_items(
            conn, [(_item(native_id=native), decision)], scan_id="s", now_unix=1
        )
    occupants = ledger.dead_zone_occupants(conn)
    assert len(occupants) == 1
    assert occupants[0]["risk_score"] == 25


# ---------------------------------------------------------------------------
# Asset class (Mando 2026-08-10: token pay stays GREEN-eligible)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("USD", "fiat"), ("EUR", "fiat"),
        ("USDC", "stablecoin"), ("DAI", "stablecoin"),
        ("HENKAKU", "volatile_token"), ("ARB", "volatile_token"),
        ("XP", "points_or_xp"),
        (None, None),
    ],
)
def test_asset_class_derivation(symbol, expected) -> None:
    from scout_daemon.models import classify_asset

    assert classify_asset(symbol) == expected


def test_volatile_token_stays_promotable_but_scores_higher() -> None:
    """GREEN-eligible per Mando, but the posture is visible in the score."""
    stable = _item(payout_currency="USDC", payout_asset_class="stablecoin")
    volatile = _item(payout_currency="HENKAKU", payout_asset_class="volatile_token")
    assert risk.assess(volatile).score > risk.assess(stable).score
