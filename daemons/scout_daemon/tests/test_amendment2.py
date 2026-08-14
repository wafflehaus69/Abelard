"""Amendment 2: CAPTCHA RED, incentivised testnet, data-completeness gate.

Every rule here was surfaced by a live LLM veto pass catching something the
mechanical rubric missed. Each is pinned so it cannot regress silently.
"""

from __future__ import annotations

import importlib
import json

import pytest

from scout_daemon import classify, risk
from scout_daemon.classify import RED, YELLOW
from scout_daemon.models import RawItem
from scout_daemon.sources.graphql_sources import QuestbookAdapter


def _item(**kw) -> RawItem:
    base = dict(source="superteam_earn", native_id="x", title="Write a blog post",
                category="bounty", payout_raw="500 USDC", payout_currency="USDC",
                counterparty="Acme", payout_basis="per_task")
    base.update(kw)
    return RawItem(**base)


# ---------------------------------------------------------------------------
# The CLI smoke test -- the coverage hole
# ---------------------------------------------------------------------------

def test_cli_imports() -> None:
    """A syntax break in cli.py was invisible to the whole suite.

    No test imported the module, so `pytest` stayed green while
    `scout-daemon scan` was unrunnable. One line closes it.
    """
    module = importlib.import_module("scout_daemon.cli")
    assert callable(module.main)


def test_every_module_imports() -> None:
    """Same hole, generalised: an unimported module cannot fail a test run."""
    for name in ("cli", "config", "classify", "identity", "errors", "fetch",
                 "ledger", "models", "orchestrator", "risk", "state",
                 "sources", "sources.base", "sources.json_api",
                 "sources.graphql_sources", "sources.html_sources",
                 "sources.whitehat"):
        importlib.import_module(f"scout_daemon.{name}")


# ---------------------------------------------------------------------------
# CAPTCHA / bot-detection bypass -> RED
# ---------------------------------------------------------------------------

def test_captcha_bypass_is_red() -> None:
    """The live case: an Opire bounty titled 'Auto Solve hcaptcha'."""
    verdict = classify.mechanical_classify(
        _item(source="opire", title="500 Server Error - Auto Solve hcaptcha")
    )
    assert verdict.legitimacy_class == RED
    assert classify.R_CAPTCHA_BYPASS in verdict.codes


@pytest.mark.parametrize("title", [
    "Solve reCAPTCHA at scale",
    "Bypass bot detection on the checkout flow",
    "Cloudflare bypass for our scraper",
    "anti-bot evasion research",
])
def test_bot_detection_family_is_red(title) -> None:
    assert classify.mechanical_classify(_item(title=title)).legitimacy_class == RED


def test_ordinary_security_work_is_not_caught_by_the_captcha_rule() -> None:
    """The rule must not swallow legitimate white-hat scope."""
    verdict = classify.mechanical_classify(_item(
        source="sherlock", title="Smart contract audit contest",
        category="Public Bug Bounty", scope_published=True,
        scope_text="repo@main", natural_person_required=False,
    ))
    assert verdict.legitimacy_class == YELLOW
    assert classify.R_CAPTCHA_BYPASS not in verdict.codes


# ---------------------------------------------------------------------------
# Incentivised testnet -> YELLOW via the airdrop rule
# ---------------------------------------------------------------------------

def test_incentivised_testnet_is_yellow_airdrop() -> None:
    """The live cases: 'Aptos Incentivized Testnet', 'Phoenix Testnets'."""
    verdict = classify.mechanical_classify(
        _item(title="Aptos Incentivized Testnet", category="grant")
    )
    assert verdict.legitimacy_class == YELLOW
    assert classify.Y_AIRDROP in verdict.codes


def test_plain_testnet_work_is_not_an_airdrop() -> None:
    """'Testnet' alone is ordinary engineering; the incentive makes it one."""
    verdict = classify.mechanical_classify(
        _item(title="Deploy our contracts to the Sepolia testnet", category="dev")
    )
    assert classify.Y_AIRDROP not in verdict.codes


def test_testnet_plus_sybil_is_red_not_yellow() -> None:
    """Sybil short-circuits at GATE 2, before the airdrop rule is reached."""
    verdict = classify.mechanical_classify(_item(
        title="Farm the incentivized testnet across multiple wallets"
    ))
    assert verdict.legitimacy_class == RED
    assert classify.R_SYBIL in verdict.codes


# ---------------------------------------------------------------------------
# Data-completeness gate
# ---------------------------------------------------------------------------

def test_missing_currency_yellows_and_cannot_promote() -> None:
    item = _item(payout_raw="10000 committed", payout_currency=None)
    verdict = classify.mechanical_classify(item)
    assert verdict.legitimacy_class == YELLOW
    assert classify.Y_PAYOUT_CURRENCY_UNRESOLVED in verdict.codes

    assessment = risk.assess(item, reason_codes=verdict.codes)
    assert assessment.eligible is False
    assert risk.should_promote(assessment) is False


def test_missing_counterparty_yellows_and_cannot_promote() -> None:
    item = _item(counterparty=None)
    verdict = classify.mechanical_classify(item)
    assert verdict.legitimacy_class == YELLOW
    assert classify.Y_COUNTERPARTY_UNRESOLVED in verdict.codes

    assessment = risk.assess(item, reason_codes=verdict.codes)
    assert assessment.eligible is False


def test_a_complete_item_still_reaches_green() -> None:
    """The gate must not swallow everything -- completeness is achievable."""
    verdict = classify.mechanical_classify(_item())
    assert verdict.legitimacy_class == classify.GREEN


def test_no_payout_does_not_trigger_the_currency_gate() -> None:
    """The gate is 'stated without a currency', not 'has no payout'."""
    verdict = classify.mechanical_classify(_item(payout_raw=None, payout_currency=None))
    assert classify.Y_PAYOUT_CURRENCY_UNRESOLVED not in verdict.codes


# ---------------------------------------------------------------------------
# payout_raw rendering
# ---------------------------------------------------------------------------

def test_null_token_label_never_renders_none_or_exponent() -> None:
    """Questbook returns "token": null on 22/80 rows.

    The old f-string produced "10000 None committed (pool)" and, on six rows,
    "1e+21 None". The live veto pass flagged those strings as uninterpretable.
    """

    class _Client:
        def post_json(self, url, json_body=None):
            return {"data": {"grants": [
                {"_id": "a", "title": "MAPO Omnichain Builder Grants",
                 "acceptingApplications": True, "numberOfApplications": 12,
                 "reward": {"committed": 10000, "token": None},
                 "workspace": {"title": "MAPO"}},
                {"_id": "b", "title": "Phoenix Testnets",
                 "acceptingApplications": True, "numberOfApplications": 30,
                 "reward": {"committed": 1e21, "token": None},
                 "workspace": {"title": "Phoenix"}},
            ]}}

    result = QuestbookAdapter().fetch(_Client(), now_unix=1_800_000_000, since_unix=0)
    assert result.items, "adapter returned nothing"
    for item in result.items:
        raw = item.payout_raw or ""
        assert "None" not in raw, f"null label leaked into classifier input: {raw!r}"
        assert "e+" not in raw.lower(), f"exponent notation in payout_raw: {raw!r}"
        assert "token unspecified" in raw


def test_questbook_award_rate_carries_its_source() -> None:
    """Provenance discipline: a rate without its source field is unauditable."""

    class _Client:
        def post_json(self, url, json_body=None):
            return {"data": {"grants": [{
                "_id": "ton", "title": "TON Grants", "acceptingApplications": True,
                "numberOfApplications": 2132, "numberOfApplicationsSelected": 103,
                "reward": {"committed": 150000, "token": {"label": "USD"}},
                "workspace": {"title": "TON"},
            }]}}

    item = QuestbookAdapter().fetch(
        _Client(), now_unix=1_800_000_000, since_unix=0
    ).items[0]
    assert item.award_rate == pytest.approx(103 / 2132, rel=1e-3)
    assert "numberOfApplicationsSelected" in (item.award_rate_source or "")
    assert item.contention == 2132
