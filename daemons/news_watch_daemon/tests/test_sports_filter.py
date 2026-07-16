"""Sports-noise filter tests — drop pure sports, keep anything with a real
markets/geopolitics token (the guard)."""

from __future__ import annotations

import pytest

from news_watch_daemon.scrape.sports_filter import classify_sports


# ---------- pure sports → dropped ----------


@pytest.mark.parametrize("headline", [
    "Defending champion Argentina reaches World Cup final by beating England 2-1",
    "Spain shuts down France and Kylian Mbappé, advances to the World Cup final",
    "Warriors beat Lakers 110-98 in NBA clash",
    "Edmonton Oilers win in overtime NHL",
    "NBA trade deadline sees blockbuster deal",
    "IOC asked to investigate FIFA president's role in Balogun reversal",
    "England Is On the Cusp of World Cup History",
    "Wimbledon semifinal set as top seed advances",
    "NFL Players Targeted in E-Commerce Scheme Using Fake Shopify Stores",
])
def test_pure_sports_is_dropped(headline):
    assert classify_sports(headline) is not None


# ---------- geopolitics/markets mentioning sport → KEPT (guard) ----------


@pytest.mark.parametrize("headline", [
    "Argentina Stokes Falklands Tensions Ahead of World Cup Clash With England",
    "World Cup security boosted as Iran tensions raise terror threat level",
    "Qatar spent $200bn on World Cup stadiums funded by oil exports",
    "SpaceX IPO priced above range as markets rally",  # no sports term anyway
])
def test_guarded_headlines_are_kept(headline):
    assert classify_sports(headline) is None


# ---------- non-sports → None (not flagged at all) ----------


@pytest.mark.parametrize("headline", [
    "Fed holds rates steady as inflation cools",
    "Reddit files for IPO with Goldman Sachs",
    "General Dynamics wins submarine production contract",
    "Iran downs drone as Ukraine war grinds on",
    "Russian troops mass near Ukraine border",
])
def test_non_sports_is_none(headline):
    assert classify_sports(headline) is None


# ---------- guard inflection-matching (the Falklands-plural bug) ----------


def test_guard_matches_plurals_and_inflections():
    # "Falklands"/"tensions" (plural) must trip the guard even though the terms
    # are stored singular ("Falkland"/"tension").
    assert classify_sports("Falklands dispute overshadows the World Cup") is None
    assert classify_sports("Rising tensions cloud the World Cup opener") is None
    # "sanctioned" (past) via "sanction"
    assert classify_sports("Sanctioned oligarch's club exits the Champions League") is None


def test_guard_does_not_over_match_team_names():
    # "war" must NOT match "Warriors"; "oil" must NOT match "Oilers"; so these
    # stay droppable via their sports token.
    assert classify_sports("Warriors clinch NBA title in Game 7") is not None
    assert classify_sports("Oilers advance in NHL playoffs") is not None


# ---------- edge cases ----------


def test_empty_and_none():
    assert classify_sports(None) is None
    assert classify_sports("") is None


def test_returns_the_matched_term():
    term = classify_sports("Lionel Messi and Argentina reach the World Cup final")
    assert term is not None and term.lower() in ("world cup", "lamine yamal", "kylian")
