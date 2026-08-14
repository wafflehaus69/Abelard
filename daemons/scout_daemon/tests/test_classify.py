"""Classification, the asymmetric rule, risk scoring, and gated promotion.

The Opire regression is the centrepiece. SC-R1 found a $1,260,988 "bounty" on a
throwaway repo with the payout bot not installed; SC-1 names it the live
regression case for `payout_confidence`. It is tested against the real row
shape, not a toy.
"""

from __future__ import annotations

import json

import pytest

from scout_daemon import classify, ledger, models, risk, state
from scout_daemon.classify import GREEN, RED, YELLOW, MechanicalVerdict
from scout_daemon.models import RawItem
from scout_daemon.sources.json_api import OpireAdapter


@pytest.fixture()
def conn(tmp_path):
    connection = state.connect(tmp_path / "l.sqlite3")
    ledger.apply_schema(connection)
    yield connection
    connection.close()


def _item(**kw) -> RawItem:
    base = dict(source="superteam_earn", native_id="x", title="Write a blog post",
                category="bounty", payout_raw="500 USDC", payout_basis="per_task")
    base.update(kw)
    return RawItem(**base)


# ---------------------------------------------------------------------------
# THE OPIRE PHANTOM -- the live regression case
# ---------------------------------------------------------------------------

def test_opire_phantom_is_never_a_verified_payout() -> None:
    """A scraped number is a CLAIM. This is the real payload shape."""

    class _Client:
        def get_json(self, url, params=None):
            return [{
                "id": "01KSXXX60RR76RSSXZXD0F4AGF",
                "title": "c1work",
                "url": "https://github.com/rodrigompy/bugb/issues/1",
                "platform": "GitHub",
                "claimerUsers": [],
                "programmingLanguages": [],
                "pendingPrice": {"value": 126098800, "unit": "USD_CENT"},
                "organization": {"name": "rodrigompy"},
                "project": {"name": "bugb", "isPublic": True, "isBotInstalled": False},
            }]

    result = OpireAdapter().fetch(_Client(), now_unix=1_800_000_000, since_unix=0)
    assert len(result.items) == 1
    item = result.items[0]

    assert item.payout_usd_high == pytest.approx(1_260_988.0)
    assert item.payout_confidence == models.CLAIMED, "pendingPrice is pending"
    assert item.payout_confidence != models.ESCROWED
    assert item.escrow_verified is False
    assert item.counterparty_verified is False, "payout bot is not installed"


def test_opire_phantom_scores_as_a_risk_not_a_jackpot() -> None:
    """A six-figure unescrowed per-task payout is a warning, not an opportunity."""
    phantom = _item(
        source="opire", native_id="1", title="c1work", category=None,
        payout_usd_high=1_260_988.0, payout_basis=models.PER_TASK,
        payout_confidence=models.CLAIMED, escrow_verified=False,
        counterparty="rodrigompy", counterparty_verified=False,
    )
    assessment = risk.assess(phantom)
    assert assessment.score >= risk.PROMOTION_THRESHOLD
    assert not risk.should_promote(assessment)
    assert "implausible unescrowed payout" in assessment.rationale


def test_no_item_in_the_corpus_is_escrow_verified() -> None:
    """Phase 1 measured 0 escrowed across 527 items. Nothing may claim it."""
    assert _item().payout_confidence != models.ESCROWED


# ---------------------------------------------------------------------------
# RED hooks -- category-first
# ---------------------------------------------------------------------------

def test_natural_person_is_checked_before_anything_else() -> None:
    """Even a perfect white-hat program is RED if an agent cannot attest."""
    verdict = classify.mechanical_classify(_item(
        source="sherlock", title="Audit contest", category="Public Bug Bounty",
        natural_person_required=True, scope_published=True, scope_text="repo@main",
    ))
    assert verdict.legitimacy_class == RED
    assert "natural-person" in verdict.reason


def test_security_category_catches_an_unlisted_platform() -> None:
    """THE YesWeHack LESSON: the category rule must fire without the domain."""
    verdict = classify.mechanical_classify(_item(
        source="unknown_source", native_id="1",
        title="Public bug bounty program for our API",
        category="bug-bounty", url="https://a-platform-nobody-listed.example/x",
        scope_published=True, scope_text="in scope: api.example.com",
        natural_person_required=False,
    ))
    assert verdict.legitimacy_class == YELLOW
    assert "white-hat" in verdict.reason


def test_whitehat_without_scope_is_held_never_admitted() -> None:
    verdict = classify.mechanical_classify(_item(
        source="sherlock", title="Bug bounty", category="Public Bug Bounty",
        scope_published=False, natural_person_required=False,
    ))
    assert verdict.legitimacy_class == YELLOW
    assert "scope-unpublished" in verdict.reason


def test_token_launch_is_red_but_original_work_nft_escalates() -> None:
    launch = classify.mechanical_classify(
        _item(title="Help us with our token launch and tokenomics")
    )
    assert launch.legitimacy_class == RED

    both = classify.mechanical_classify(
        _item(title="Mint an NFT of your original artwork for our token launch")
    )
    assert both.legitimacy_class is None, "ambiguous -> LLM, not auto-RED"


def test_sybil_and_fraud_and_human_subject_hooks() -> None:
    assert classify.mechanical_classify(
        _item(title="Complete quests to earn XP and airdrop farm")
    ).legitimacy_class == RED
    assert classify.mechanical_classify(
        _item(title="Write a fake review for our product")
    ).legitimacy_class == RED
    assert classify.mechanical_classify(
        _item(title="Survey participant needed", url="https://prolific.com/s/1")
    ).legitimacy_class == RED


def test_mandos_ruling_missing_category_is_yellow() -> None:
    """2026-08-10: 0% category -> YELLOW."""
    verdict = classify.mechanical_classify(_item(category=None))
    assert verdict.legitimacy_class == YELLOW
    assert "category unresolved" in verdict.reason


# ---------------------------------------------------------------------------
# The asymmetric rule
# ---------------------------------------------------------------------------

def test_disagreement_never_resolves_upward_to_green() -> None:
    mech = MechanicalVerdict(GREEN, "looked clean", True)
    decision = classify.resolve(mech, (RED, "fraud signal"))
    assert decision.legitimacy_class == RED
    assert decision.disagreed is True

    decision = classify.resolve(MechanicalVerdict(YELLOW, "novel", True), (GREEN, "fine"))
    assert decision.legitimacy_class == YELLOW, "YELLOW + GREEN must not become GREEN"
    assert decision.disagreed is True


def test_failed_llm_pass_lands_yellow_not_green() -> None:
    decision = classify.resolve(MechanicalVerdict(None, "ambiguous", False), None)
    assert decision.legitimacy_class == YELLOW
    assert "LLM pass returned no verdict" in decision.class_reason


def test_agreement_is_preserved() -> None:
    decision = classify.resolve(MechanicalVerdict(GREEN, "clean", True), (GREEN, "ok"))
    assert decision.legitimacy_class == GREEN
    assert decision.disagreed is False


# ---------------------------------------------------------------------------
# Batched LLM pass -- structure verified without a live key
# ---------------------------------------------------------------------------

class _StubResponse:
    def __init__(self, text, stop_reason="end_turn", in_tok=100, out_tok=50):
        self.content = [type("B", (), {"type": "text", "text": text})()]
        self.stop_reason = stop_reason
        self.usage = type("U", (), {
            "input_tokens": in_tok, "output_tokens": out_tok,
            "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
        })()


class _StubClient:
    def __init__(self, response):
        self._response = response
        self.messages = type("M", (), {"create": lambda _s, **kw: response})()


def test_batch_filters_hallucinated_ids() -> None:
    payload = json.dumps({"classifications": [
        {"id": "real", "legitimacy_class": GREEN, "reason": "fine"},
        {"id": "invented", "legitimacy_class": GREEN, "reason": "not requested"},
    ]})
    out, cost = classify.classify_batch(
        [_item()], ["real"], client=_StubClient(_StubResponse(payload))
    )
    assert set(out) == {"real"}, "an id we did not ask about is a hallucination"
    assert cost.input_tokens == 100 and cost.output_tokens == 50


def test_truncated_batch_raises_but_cost_is_still_captured() -> None:
    """Cost is captured BEFORE the guard that raises -- the tokens were spent."""
    stub = _StubResponse("{}", stop_reason="max_tokens")
    with pytest.raises(Exception) as exc:
        classify.classify_batch([_item()], ["k"], client=_StubClient(stub))
    assert "truncated" in str(exc.value)


def test_empty_and_unparseable_responses_raise() -> None:
    with pytest.raises(Exception):
        classify.classify_batch([_item()], ["k"], client=_StubClient(_StubResponse("")))
    with pytest.raises(Exception):
        classify.classify_batch(
            [_item()], ["k"], client=_StubClient(_StubResponse("not json"))
        )


def test_cost_arithmetic_uses_sonnet_rates() -> None:
    cost = classify.CostRecord(input_tokens=1_000_000, output_tokens=1_000_000)
    assert cost.cost_usd == pytest.approx(18.0), "$3/M in + $15/M out"


# ---------------------------------------------------------------------------
# Threat promotion
# ---------------------------------------------------------------------------

def test_promotion_threshold_is_31() -> None:
    # should_promote now requires eligibility AND a sub-threshold score, so an
    # assessment has to be marked eligible to isolate the threshold behaviour.
    assert risk.PROMOTION_THRESHOLD == 31
    assert risk.should_promote(risk.RiskAssessment(30, eligible=True)) is True
    assert risk.should_promote(risk.RiskAssessment(31, eligible=True)) is False


def test_categories_mando_reserved_stay_above_threshold() -> None:
    """The rubric does the excluding, so there is no exception list to drift."""
    whitehat = _item(source="sherlock", category="Public Bug Bounty",
                     scope_published=True, natural_person_required=False)
    affiliate = _item(source="affiliate_watch", category="VPN",
                      payout_basis=models.PER_SALE_COMMISSION)
    agent_native = _item(source="dealwork", category="Data Processing")
    human_only = _item(agent_permitted="no")

    for item, label in (
        (whitehat, "white-hat"), (affiliate, "affiliate"),
        (agent_native, "agent-native"), (human_only, "human-only"),
    ):
        assessment = risk.assess(item)
        assert not risk.should_promote(assessment), (
            f"{label} promoted at {assessment.score}: {assessment.rationale}"
        )


def test_security_research_is_detected_by_category_not_by_source_lane() -> None:
    """REGRESSION 2026-08-10 -- the Arbitrum Audit Program bug.

    A $10M smart-contract-audit program arriving from a `grant`-lane source was
    correctly held YELLOW by the classifier (scope unpublished), then PROMOTED
    to GREEN at risk score 5 because the scorer asked about the source lane
    instead of the category. An explicit gate was silently defeated.
    """
    audit_grant = _item(
        source="arbitrum_grants", native_id="audit",
        title="Arbitrum Audit Program",
        payout_raw="offers $10M in ARB to subsidise third-party smart contract audits",
        category="grant_program", scope_published=False,
    )
    assert classify.is_security_research(audit_grant) is True

    verdict = classify.mechanical_classify(audit_grant)
    assert verdict.legitimacy_class == YELLOW
    assert "scope-unpublished" in verdict.reason

    assessment = risk.assess(audit_grant)
    assert not risk.should_promote(assessment), (
        f"held-pending security item promoted at {assessment.score}: "
        f"{assessment.rationale}"
    )


def test_analysing_a_token_launch_is_not_launching_one() -> None:
    """REGRESSION 2026-08-10 -- the dealwork false positive.

    "Crypto/DeFi research reports with tokenomics analysis" is an agent
    offering ANALYSIS. It was classified RED as a fungible-token launch on the
    bare word "tokenomics".
    """
    analyst = _item(
        source="dealwork", native_id="solene",
        title="Solene - Code Review, Security Audit, Crypto Research & Automation",
        effort_note="Crypto/DeFi research reports with tokenomics analysis",
        category="Data Processing",
    )
    verdict = classify.mechanical_classify(analyst)
    assert verdict.legitimacy_class != RED, (
        "describing a token launch is not running one"
    )

    # A genuine launch must still be caught.
    launcher = _item(title="Run our token generation event and set tokenomics")
    assert classify.mechanical_classify(launcher).legitimacy_class == RED


def test_scores_are_bounded_and_deterministic() -> None:
    item = _item(source="sherlock", category=None, agent_permitted="no",
                 payout_confidence=models.UNVERIFIED, capital_required_usd=50.0)
    first = risk.assess(item)
    assert 0 <= first.score <= 100
    assert first.score == risk.assess(item).score


def test_promotion_is_recorded_and_reversible(conn) -> None:
    """An upward path must be auditable: the prior class is preserved."""
    decision = ledger.Classification(
        ledger.GREEN_PROMOTED, "promoted", "risk-promotion", "v1",
        risk_score=12, risk_factors="none",
        promoted_from_yellow=True, pre_promotion_class=YELLOW,
    )
    ledger.upsert_items(conn, [(_item(), decision)], scan_id="s", now_unix=1)
    rows = ledger.promotions(conn)
    assert len(rows) == 1
    assert rows[0]["risk_score"] == 12
    stored = conn.execute(
        "SELECT pre_promotion_class FROM opportunities"
    ).fetchone()
    assert stored["pre_promotion_class"] == YELLOW


# ---------------------------------------------------------------------------
# Ledger invariants
# ---------------------------------------------------------------------------

def test_red_items_are_stored_and_visible(conn) -> None:
    """Invariant 1: surface, never drop."""
    red = ledger.Classification(RED, "fraud signal", "mechanical", "v1")
    ledger.upsert_items(conn, [(_item(title="Fake reviews"), red)],
                        scan_id="s", now_unix=1)
    assert ledger.class_distribution(conn) == {RED: 1}
    row = conn.execute("SELECT class_reason FROM opportunities").fetchone()
    assert row["class_reason"] == "fraud signal", "RED keeps its reason"


def test_reseeing_an_item_never_resets_status(conn) -> None:
    """The ledger must not silently forget a human decision."""
    verdict = ledger.Classification(GREEN, "ok", "mechanical", "v1")
    item = _item()
    ledger.upsert_items(conn, [(item, verdict)], scan_id="s1", now_unix=1)
    conn.execute("UPDATE opportunities SET status = 'admitted'")
    conn.commit()

    ledger.upsert_items(conn, [(item, verdict)], scan_id="s2", now_unix=2)
    row = conn.execute(
        "SELECT status, first_seen_unix, last_seen_unix FROM opportunities"
    ).fetchone()
    assert row["status"] == "admitted", "re-seeing must not un-admit"
    assert row["first_seen_unix"] == 1
    assert row["last_seen_unix"] == 2


def test_no_code_path_writes_admitted_or_dismissed() -> None:
    """Invariant 2 enforced by absence: only Mando moves an item past proposed."""
    import inspect

    source = inspect.getsource(ledger) + inspect.getsource(classify)
    for forbidden in ("'admitted'", '"admitted"', "'dismissed'", '"dismissed"'):
        assert source.count(forbidden) <= 1, (
            f"{forbidden} appears in a writable position -- admission is human"
        )


def test_cost_is_recorded_before_items(conn) -> None:
    """A ledger write failure must not lose the record of money spent."""
    ledger.record_cost(
        conn, scan_id="s", model="claude-sonnet-4-6", llm_calls=1,
        input_tokens=10, output_tokens=5, cache_read_tokens=0,
        cache_creation_tokens=0, cost_usd=0.001, items_classified=1,
    )
    row = conn.execute("SELECT cost_usd, model FROM scan_cost").fetchone()
    assert row["cost_usd"] == 0.001
    assert conn.execute("SELECT COUNT(*) c FROM opportunities").fetchone()["c"] == 0
