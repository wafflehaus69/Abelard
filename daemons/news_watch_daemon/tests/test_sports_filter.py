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


# ---------- prediction-markets guard (2026-07-19) ----------


def test_prediction_market_stories_survive_a_sports_token():
    # The regression that motivated the guard: dropped live on 2026-07-19.
    assert classify_sports(
        "Prediction Markets Swell to 27% of Sports Bets During World Cup"
    ) is None
    # Platform-named business/regulatory stories pinned to a sports event.
    assert classify_sports(
        "Kalshi Picks Up World Cup Sponsorship Deal at a Deep Discount"
    ) is None
    assert classify_sports(
        "Massachusetts AG files amended lawsuit against Kalshi over sports betting"
    ) is None
    assert classify_sports(
        "Polymarket volumes hit records on Super Bowl and World Cup markets"
    ) is None
    assert classify_sports(
        "CFTC weighs sports event contracts as Super Bowl volume climbs"
    ) is None


def test_hyphenated_prediction_market_compound_survives():
    # Regression, live 2026-07-20: the compound adjective is hyphenated, and
    # word-boundary matching treats "-" as a break, so the space spelling alone
    # let this drop. This was the day's best volume-signal headline.
    assert classify_sports(
        "Argentina-Spain World Cup Final Drives $5.69B in Prediction-Market Volume"
    ) is None
    assert classify_sports(
        "Prediction-Markets Volume Sets a Record During the World Cup"
    ) is None


def test_plain_sports_gambling_still_drops():
    # The guard is PM-specific: ordinary sports-betting coverage carries no
    # platform/instrument token and must still be dropped as noise.
    assert classify_sports("Sportsbooks post record World Cup handle") is not None
    assert classify_sports("Best betting odds for the NBA finals tonight") is not None
    assert classify_sports("Spain beat France to reach the World Cup final") is not None


# ---------- affiliate/promo spam drops outright (rides in via the PM guard) ----------


def test_affiliate_promo_drops_even_with_a_prediction_market_token():
    # Real examples pulled from the live Google News PM query (2026-07-19).
    # Each carries a PM platform name, so the guard would otherwise KEEP them.
    assert classify_sports(
        "Polymarket Promo Code SBWIRE Get $50 Bonus for Sports, Politics & More"
    ) is not None
    assert classify_sports("Kalshi World Cup 2026: how to bet on the final") is not None
    assert classify_sports("Kalshi bonus code unlocks a risk-free bet this week") is not None
    assert classify_sports("Polymarket parlay picks for Sunday's slate") is not None


def test_promo_rule_does_not_eat_idiomatic_markets_copy():
    # "best bet" / bare "odds" are ordinary finance idiom — must NOT drop.
    assert classify_sports("Gold is analysts' best bet for the second half") is None
    assert classify_sports("Traders raise the odds of a September rate cut") is None
    # And a genuine PM markets story stays kept.
    assert classify_sports(
        "Kalshi and Polymarket saw $5.7B wagered on the World Cup final"
    ) is None
