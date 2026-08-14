"""Risk scoring over the YELLOW set, with gated promotion below a threshold.

NAMING IS DELIBERATE AND THE RENAME WAS ORDERED. This is a RISK SCORE. It is
not authentication and not a threat assessment: nothing is authenticated, no
counterparty is verified, no adversary is modelled. It reads fields the scout
already captured and adds up weights. Calling it "authentication" would put a
claim in the record that the mechanism cannot support.

Mando's ruling, 2026-08-10: score every YELLOW item 0-100 and reclassify below
31 as GREEN_PROMOTED. Ratified as a doctrine amendment conditional on the
guardrails below.

THIS IS THE ONLY UPWARD CLASSIFICATION PATH IN THE DAEMON. Everything else
resolves downward -- uncertainty lands YELLOW and never GREEN. That rule still
holds inside `classify.resolve()`; this module is a separately-gated exception
that runs AFTER resolution, and every promotion records score, per-weight
breakdown, weights version, eligibility, and the canonical timestamp. A
promotion is never silent and never irreversible: `pre_promotion_class` means
raising the threshold re-derives the old answer from stored data.

WHAT PROMOTION DOES NOT MEAN. GREEN_PROMOTED is a LEGITIMACY class -- "no
rubric objection, at this risk score" -- not an admission. `status` stays
`discovered`. Invariant 2 is untouched.

MECHANICAL, NOT LLM -- deliberate. Every input is a structured field captured
in Phase 1. Scoring in code keeps the one-LLM-call-per-scan doctrine, makes
every promotion deterministic and unit-testable, and costs nothing. A
non-deterministic gate on an upward path would be a worse default than a
legible one.

THE WEIGHTS ENCODE MANDO'S RUBRIC, NOT A SEPARATE OPINION. Categories he named
as requiring his judgment carry weights that hold them above the threshold by
construction. That is why there is no exception list: the rubric does the
excluding, so the scorer cannot quietly promote something already reserved for
a human look.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import classify, config
from .models import RawItem

# Mando's ruling: 31. Pinned in ONE place; a test asserts it.
PROMOTION_THRESHOLD = 31

# The band between the highest observed promotion and the threshold. Empty on
# the corpus that set the threshold -- an observation about that corpus, NOT a
# property of the weights (a category-unresolved item scores exactly 25).
# Occupancy is reported loudly every run.
DEAD_ZONE = (21, 30)

RISK_WEIGHTS_VERSION = "sc1-p3b-risk-2026-08-11"

# Histogram bands, DERIVED from the threshold rather than written beside it.
# The band edge at 31 is the threshold; hardcoding it in the ledger meant that
# raising the threshold to 46 would have left the histogram still splitting at
# 31 and still labelling 31-45 as above-threshold. One constant, one boundary.
HISTOGRAM_BANDS: tuple[tuple[int, int], ...] = (
    (0, DEAD_ZONE[0] - 1),                 # 0-20   promotable range
    DEAD_ZONE,                             # 21-30  the monitored dead zone
    (PROMOTION_THRESHOLD, 45),             # 31-45  first band above threshold
    (46, 60),
    (61, 80),
    (81, 100),
)

# ---------------------------------------------------------------------------
# Promotion eligibility -- an ALLOWLIST, intentionally incomplete
# ---------------------------------------------------------------------------
# A reason must be on this list for an item to be promotable. Anything not
# listed is INELIGIBLE by default, which is the safe direction: a new yellowing
# reason added later cannot silently acquire a promotion path by omission.
#
# THE DIVIDING LINE IS RUBRIC-JUDGMENT vs DATA-ABSENCE.
#
# A risk score is computed over FIELDS. When the reason an item yellowed is
# that a field is MISSING, the score is computed over the remaining fields and
# comes back low -- it scores the absence as calm. Measured 2026-08-10: a
# category-unresolved item, otherwise clean, scored 25 and promoted, and its
# score was literally MADE OF the absence (+20) plus a benign +5. Scoring
# cannot cure a missing field; only fetching the field can.
_ELIGIBLE_REASONS = frozenset({
    classify.Y_WHITEHAT_PER_PROGRAM,   # scope IS published; a judgment call
    classify.Y_AFFILIATE,              # rubric lane, all fields present
    classify.Y_AFFILIATE_PAID,         # rubric lane + a known capital posture
    classify.Y_AGENT_NATIVE,           # novel category, but the fields are there
    classify.Y_AIRDROP,                # receiving-not-issuing; a posture call
    classify.Y_PERSONA_PRESUMED,       # the task states what it wants; a call
    classify.Y_REGULATED_VERTICAL,     # compliance posture, fields present
    classify.Y_PLATFORM_ACCOUNT_TOS,   # platform terms, fields present
})

# Named for the report, so a blocked promotion says WHICH absence blocked it.
_ABSENCE_REASONS = frozenset({
    classify.Y_CATEGORY_UNRESOLVED,
    classify.Y_SCOPE_UNPUBLISHED,
    classify.Y_NP_UNKNOWN,
    classify.Y_UNCLASSIFIED,
    classify.A_NO_VERDICT,
    classify.A_NFT_TOKEN,
    classify.A_ANALYSIS_CONTEXT,
    # Added 2026-08-11. Same class as a missing category: the score would be
    # computed over the fields that survived and come back reassuringly low.
    classify.Y_PAYOUT_CURRENCY_UNRESOLVED,
    classify.Y_COUNTERPARTY_UNRESOLVED,
    # Persona vetoes are promotion-ineligible by ruling. The LLM judged that
    # the task presumes a human; a risk score computed over listing fields has
    # no way to overturn that, and letting it try would be the score
    # second-guessing the only gate that can see the question.
    classify.Y_PERSONA_LLM_VETO,
})


@dataclass
class RiskAssessment:
    score: int
    factors: list[tuple[str, int]] = field(default_factory=list)
    eligible: bool = False
    blocked_by: tuple[str, ...] = ()
    evaluated_codes: tuple[str, ...] = ()

    @property
    def rationale(self) -> str:
        if not self.factors:
            return "no risk factors detected"
        return "; ".join(f"{name} +{points}" for name, points in self.factors)

    @property
    def breakdown(self) -> list[dict]:
        """Per-weight contribution, for the promotion provenance record."""
        return [{"factor": name, "points": points} for name, points in self.factors]


def check_eligibility(reason_codes: tuple[str, ...]) -> tuple[bool, tuple[str, ...]]:
    """(eligible, blocking_codes).

    EVERY reason must be allowlisted. An item carrying both a rubric reason and
    an absence reason is ineligible -- the absence does not stop mattering
    because something assessable sits beside it.
    """
    if not reason_codes:
        # No recorded reason is itself a non-answer. Ineligible.
        return False, ("no_reason_code_recorded",)
    blocking = tuple(c for c in reason_codes if c not in _ELIGIBLE_REASONS)
    return (not blocking), blocking


def assess(
    item: RawItem,
    *,
    reason_codes: tuple[str, ...] = (),
    mechanical_reason: str = "",
) -> RiskAssessment:
    """Score one item 0-100 and decide promotion eligibility. Deterministic."""
    factors: list[tuple[str, int]] = []

    def add(name: str, points: int) -> None:
        if points:
            factors.append((name, points))

    source_cfg = config.SOURCES_BY_NAME.get(item.source)
    lane = source_cfg.lane if source_cfg else None

    # Detected by CATEGORY, not by source lane. Keying off the lane promoted
    # the Arbitrum Audit Program -- a security item from a `grant`-lane source
    # -- past a gate Mando set explicitly.
    is_security = classify.is_security_research(item)
    is_agent_native = classify.is_agent_native(item)

    # --- Eligibility risk: can the tribe truthfully participate at all? ----
    if item.natural_person_required is None and is_security:
        add("natural-person status unknown", 35)
    if item.agent_permitted == "no":
        add("item is human-only", 45)

    # --- Categories Mando reserved for his judgment -----------------------
    if is_security:
        add("security research: per-program admission required", 45)
        if not item.scope_published:
            add("security scope unpublished", 30)
    if lane == "affiliate" or item.payout_basis == "per_sale_commission":
        add("affiliate: disclosure + net-cashflow gate", 35)
        if item.paid_acquisition:
            add("paid acquisition: needs hard loss cap", 25)
    if is_agent_native:
        add("newly discovered category", 35)

    # Mando's 2026-08-11 rulings, weighted to hold above the threshold on their
    # own -- same discipline as the other reserved categories: the rubric does
    # the excluding, so there is no exception list to drift.
    if classify.Y_PERSONA_PRESUMED in reason_codes:
        add("task presumes a human participant", 40)
    if classify.Y_REGULATED_VERTICAL in reason_codes:
        add("regulated vertical: compliance exposure", 35)
    if classify.Y_PLATFORM_ACCOUNT_TOS in reason_codes:
        add("platform terms may bar non-human accounts", 25)

    # --- Asset posture ----------------------------------------------------
    # Token-denominated pay stays GREEN-eligible per Mando 2026-08-10, but a
    # volatile token is a different posture than a stablecoin and the score
    # should say so rather than treating "500 HENKAKU" as "500 dollars".
    if item.payout_asset_class == "volatile_token":
        add("paid in a volatile token", 15)
    elif item.payout_asset_class == "points_or_xp":
        add("paid in points/XP, not a transferable asset", 25)

    # --- Counterparty and payout reliability ------------------------------
    if item.payout_confidence == "unverified":
        add("payout unverified", 15)
    elif item.payout_confidence == "claimed":
        add("payout is a claim, not escrow", 5)
    if item.escrow_verified is False:
        add("explicitly not escrowed", 15)
    if not item.counterparty:
        add("counterparty unknown", 15)
    elif item.counterparty_verified is False:
        add("counterparty unverified by source", 10)
    if item.capital_required_usd:
        add("capital required to participate", 25)

    # --- Implausible payout: the Opire phantom shape ----------------------
    if (
        item.payout_basis == "per_task"
        and (item.payout_usd_high or 0) > 100_000
        and item.escrow_verified is not True
    ):
        add("implausible unescrowed payout", 30)

    # --- Weaker signals ---------------------------------------------------
    if not item.category:
        add("category unresolved", 20)
    if item.tos_flags:
        add(
            f"tos flags: {','.join(item.tos_flags)[:60]}",
            min(20, 10 * len(item.tos_flags)),
        )
    if "ambiguous" in (mechanical_reason or "").lower():
        add("mechanically ambiguous", 15)

    score = min(100, sum(points for _, points in factors))
    eligible, blocking = check_eligibility(reason_codes)
    return RiskAssessment(
        score=score,
        factors=factors,
        eligible=eligible,
        blocked_by=blocking,
        evaluated_codes=reason_codes,
    )


def should_promote(assessment: RiskAssessment) -> bool:
    """Promotion needs BOTH eligibility and a sub-threshold score.

    Eligibility is checked first and independently: a low score on an
    ineligible item is exactly the failure mode this gate exists to stop.
    """
    return assessment.eligible and assessment.score < PROMOTION_THRESHOLD


def in_dead_zone(score: int | None) -> bool:
    return score is not None and DEAD_ZONE[0] <= score <= DEAD_ZONE[1]


__all__ = [
    "PROMOTION_THRESHOLD",
    "DEAD_ZONE",
    "RISK_WEIGHTS_VERSION",
    "RiskAssessment",
    "assess",
    "check_eligibility",
    "should_promote",
    "in_dead_zone",
]
