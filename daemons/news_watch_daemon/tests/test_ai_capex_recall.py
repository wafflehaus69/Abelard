"""ai_capex recall-widen tests (2026-06-30).

Verifies the AI capital-cycle / AI-trade / AI-automation phrase widen against
the live ai_capex_cycle theme config: capital-cycle phrasing tags, the
labor/society bucket stays out, and the financing wall still overrides
inclusions. Loads the real theme YAML (same discipline as test_theme_config's
seed-theme tests) so it guards the actual matcher behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from news_watch_daemon.scrape.orchestrator import _compile_theme_regexes, _tag_for_theme
from news_watch_daemon.theme_config import load_all_themes

_REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def ai_capex():
    themes = load_all_themes(_REPO / "themes")
    return {x.theme_id: x for x in _compile_theme_regexes(themes)}["ai_capex_cycle"]


@pytest.mark.parametrize("headline", [
    "Tokyo and Seoul Bet the House on AI Buildouts",
    "Lee's $880 Billion AI Bet Ties Legacy to South Korea Chip Boom",
    "China Loses Out on AI Boom as Stocks Trail by Most Since 2001",
    "Dual Perspectives on AI Investment Costs",
    "Salesforce is on an AI buying spree",
    "AI Leverage Is More Worrying Than Valuations, IMF Says",
    "The AI trade is back on as megacaps rally",
    "AI bubble fears grip markets",
    "Honeywell touts AI automation push",
    "Nvidia data center buildout accelerates AI capacity",
])
def test_capital_cycle_phrases_tag(ai_capex, headline):
    assert _tag_for_theme(headline, ai_capex) is not None, headline


@pytest.mark.parametrize("headline", [
    "Former Governors Team Up to Address AI Job Losses",
    "AI Notetakers in Meetings Raise Mounting Privacy Concerns",
    "Weak Hiring Is Hurting Young Workers More than AI, Study Says",
    "House passed a new version of the Kids Online Safety Act",
    "Jon and Mindy Gray Bet on AI Science Research to Prevent Cancer",
    "Investment in AI safety research expands at Anthropic",
])
def test_society_bucket_stays_untagged(ai_capex, headline):
    assert _tag_for_theme(headline, ai_capex) is None, headline


@pytest.mark.parametrize("headline", [
    # inclusion phrase present, but a financing term overrides it (wall holds)
    "AI buildout IPO prices above range",
    "AI capacity secondary offering announced",
])
def test_financing_wall_overrides_inclusion(ai_capex, headline):
    assert _tag_for_theme(headline, ai_capex) is None, headline


@pytest.mark.parametrize("headline", [
    "Micron Soars After AI-Fueled Sales Forecast",
    "Nvidia data center revenue beats",
    "SK Hynix ramps HBM output on AI demand",
])
def test_existing_capex_still_tags(ai_capex, headline):
    assert _tag_for_theme(headline, ai_capex) is not None, headline
