"""Sports-noise filter — drops pure-sports headlines before ingest.

Order (Mando, 2026-07-15): eliminate sports news. Sports headlines (World
Cup, NBA, transfer gossip) carry no markets/geopolitics signal but flow in
through the broad wire feeds (Google News geopolitics/Reuters/AP, Al Jazeera,
CIG Telegram) and the general Finnhub feed. Left in the corpus they:
  - burn Pass E attention calls (the World Cup drove `world cup` / `argentina`
    / `england` crossings on the NW-SRC-4 brief),
  - pad the frequency-diagnostic near-miss tables,
  - dilute the per-theme sample headlines.

Design — DROP ONLY UNAMBIGUOUS SPORTS. The matcher is:

    is sports-noise  iff  (a strong sports token matches)
                     AND  (NO markets/geopolitics guard token matches)

The guard is the safety net: a real geopolitics/markets story that merely
mentions a match survives. "Argentina Stokes Falklands Tensions Ahead of World
Cup Clash With England" carries `Falkland`/`tension` -> KEPT. A pure
"Spain beat France to reach the World Cup final" carries no guard token ->
DROPPED. The bias is deliberately toward KEEPING: sports-adjacent business
("World Cup drives host-city travel boom") with no hard signal is dropped, but
anything touching a real market/geo token is kept.

The guard intentionally EXCLUDES sports-ambiguous tokens ("market" as in
transfer market, "trade" as in NBA trade, "economy") so that pure sports-
transfer stories still drop; it uses only tokens that signal a genuine
markets/geopolitics story.

Forward-only, like the publisher-suffix strip: headlines already in the DB are
not retroactively removed (they age out of the brief windows). Every drop is
logged to the noise-filter audit trail (filtered.jsonl) so "did the filter eat
post X?" stays answerable.

Keyword lists are module constants (editable inline, like adjacency.py's token
sets). Case-insensitive throughout.
"""

from __future__ import annotations

import re


# Strong sports signals — competition/league names, sport-specific roles and
# actions, and governing bodies. Chosen to be unambiguous: bare-word collisions
# are avoided (e.g. "Formula 1"/"Grand Prix" not bare "F1" which is also a visa;
# "golf tournament" not bare "golf"; "Masters Tournament" not bare "Masters").
_SPORTS_TERMS = [
    # Football / soccer
    "World Cup", "FIFA", "UEFA", "Premier League", "La Liga", "Champions League",
    "Europa League", "Serie A", "Bundesliga", "Ligue 1", "Copa America",
    "Ballon d'Or", "transfer window", "penalty shootout", "hat-trick", "hat trick",
    # US leagues
    "NBA", "NFL", "NHL", "MLB", "MLS", "NCAA", "Super Bowl", "touchdown", "home run",
    # Olympics / governing
    "Olympics", "Olympic", "IOC",
    # Tennis / golf / motorsport / cricket / rugby
    "Wimbledon", "Grand Slam", "Ryder Cup", "Masters Tournament", "Grand Prix",
    "Formula 1", "Formula One", "Test match", "Six Nations", "cricket match",
    "golf tournament", "rugby",
    # Generic competition-stage / role vocabulary
    "semifinal", "semi-final", "quarterfinal", "quarter-final", "knockout stage",
    "group stage", "striker", "midfielder", "goalkeeper", "quarterback", "kickoff",
    # Athletes prominent in current coverage (unambiguous surnames)
    "Lamine Yamal", "Mbappe", "Mbappé", "Kylian",
]

# Markets / geopolitics guard — if ANY of these match, the headline is NOT
# dropped even when a sports term also matched. Deliberately excludes sports-
# ambiguous tokens (market/trade/economy). Strong signals only. Stored SINGULAR:
# the matcher appends an optional inflection group so "Falkland" catches
# "Falklands", "tension" catches "tensions", "sanction" catches "sanctioned",
# "Iran" catches "Iranian(s)" — WITHOUT over-matching ("war" does NOT match
# "Warriors"/"warm"/"warning"; "oil" does NOT match "Oilers"). The guard errs
# generous on purpose: a false-keep leaves a sports headline in, a false-drop
# would delete a real story.
_GUARD_TERMS = [
    "sanction", "tariff", "missile", "airstrike", "Iran", "Russia", "Ukraine",
    "China", "Israel", "Gaza", "Taiwan", "Federal Reserve", "inflation",
    "oil", "crude", "nuclear", "war", "troop", "military", "IPO", "merger",
    "acquisition", "buyout", "stock", "earnings", "election", "Trump", "Falkland",
    "tension", "treaty", "GDP", "rate cut", "rate hike", "central bank", "defense",
    "defence", "budget", "dollar", "bond", "yield", "stablecoin", "tokeniz",
    "semiconductor", "Nvidia", "chip", "datacenter", "data center", "diplomat",
    "parliament", "sovereign", "peso", "currency", "OPEC", "recession",
    # Prediction markets (2026-07-19). PM stories are pinned to sports events
    # far more often than any other markets thread — World Cup volume records,
    # Kalshi's sports-betting litigation, Kalshi's World Cup sponsorship. Without
    # these the sports token silently drops the most CONSENSUS-relevant content
    # in the feed ("Prediction Markets Swell to 27% of Sports Bets During World
    # Cup" was dropped on 2026-07-19). Platform names and the instrument term are
    # unambiguous — a pure match report never carries them — so this does not
    # reopen the sports floodgates. NOT included: "betting"/"odds"/"sportsbook",
    # which would keep ordinary sports-gambling coverage.
    # HYPHENATED FORM IS LOAD-BEARING: headlines routinely write the compound
    # adjective ("$5.69B in Prediction-Market Volume", 2026-07-20). Word-boundary
    # matching treats the hyphen as a break, so "prediction market" does NOT
    # match "Prediction-Market" — both spellings must be listed.
    "prediction market", "prediction-market",
    "Polymarket", "Kalshi", "PredictIt", "event contract",
]

# Affiliate / betting-promo spam — never signal, and (as of the prediction-
# markets guard above) able to EVADE the sports drop by name-dropping a PM
# platform: "Polymarket Promo Code SBWIRE Get $50 Bonus" (Golfweek), "Kalshi
# World Cup 2026: how to bet" (CBS Sports). These tells are unambiguous
# marketing artifacts, so they drop OUTRIGHT — guard or no guard.
#
# Deliberately EXCLUDES idiomatic finance phrasing that reads as promo but
# isn't: "best bet" ("gold is the best bet for H2"), bare "odds", bare "props".
# The bias stays toward keeping — only unmistakable affiliate copy drops.
_PROMO_TERMS = [
    "promo code", "bonus code", "sign-up bonus", "signup bonus",
    "deposit bonus", "free bet", "risk-free bet", "parlay", "player prop",
    "betting guide", "how to bet", "picks and prediction", "betting promo",
]

# Optional inflection suffix: plural / past / gerund / -ian(s). Tight enough
# that "war"+group matches "war"/"wars" but the trailing \b still fails on
# "warm"/"warning"/"warrior" (their next char is a word char with no valid
# suffix consumed). Applied to both lists for robustness.
_INFLECT = r"(?:s|es|ed|ing|ians?)?"


def _compile(terms: list[str]) -> re.Pattern[str]:
    # Word-boundary alternation with an optional inflection suffix,
    # case-insensitive. re.escape handles apostrophes / hyphens / internal
    # spaces (the suffix binds to the final word of a multi-word term).
    return re.compile(
        "|".join(rf"\b{re.escape(t)}{_INFLECT}\b" for t in terms),
        re.IGNORECASE,
    )


_SPORTS_RE = _compile(_SPORTS_TERMS)
_GUARD_RE = _compile(_GUARD_TERMS)
_PROMO_RE = _compile(_PROMO_TERMS)


def classify_sports(headline: str | None) -> str | None:
    """Return the matched noise term if `headline` is drop-worthy, else None.

    Two drop rules:
      1. affiliate/betting-promo copy matches -> drop OUTRIGHT (these evade the
         sports rule below by name-dropping a prediction-markets platform), or
      2. a strong sports token matches AND no markets/geopolitics guard token
         matches (pure sports noise).

    The returned term is the specific keyword that triggered the drop, for the
    audit trail's `matched_pattern`.
    """
    if not headline:
        return None
    promo = _PROMO_RE.search(headline)
    if promo is not None:
        return promo.group(0)  # affiliate spam is never signal
    m = _SPORTS_RE.search(headline)
    if m is None:
        return None
    if _GUARD_RE.search(headline):
        return None  # a real markets/geo story that mentions sport — keep it
    return m.group(0)


__all__ = ["classify_sports"]
