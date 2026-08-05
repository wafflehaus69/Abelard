"""The single chokepoint for UNRESOLVED handling.

Why this module exists. "We could not determine this" is a distinct third state from
any measured value, and every consumer that re-derived it got a vote on whether to fail
open. Four separate fail-open defects shipped from exactly that: the score path
renormalised over missing factors, and then `or n_raw` / `or 1` / an unconditional
high-water update each independently converted an unresolved value into a confident one
— in the renderer, in the alerting payload, and in the store. Each was fixed in
isolation and the next consumer reintroduced it.

So: nothing outside this module may decide what unresolved means. Consumers ask the
questions below and get an answer that already fails CLOSED. The rule everywhere is the
same and it is deliberately asymmetric — an unresolved value must never be presented,
scored, or alerted on as though it were measured, because every error in that direction
overstates evidence, and overstating evidence is the one failure this product cannot
afford.
"""

from __future__ import annotations

from typing import Any

#: Tiers that represent a COMPLETE score. INSUFFICIENT_DATA is deliberately absent:
#: m0f assigns it when a factor could not be resolved, and the resulting composite is
#: renormalised over the surviving factors and therefore inflated.
REAL_TIERS = frozenset({"NONE", "WATCH", "ELEVATED", "CRITICAL"})

#: What a human reads where a number would otherwise sit.
UNRESOLVED = "UNRESOLVED"


def is_complete_score(tier: str | None) -> bool:
    """True only if this tier came from a score with every factor resolved. A score the
    detector refused to tier must not set a high-water mark, rank a dossier, or page."""
    return (tier or "NONE") in REAL_TIERS


def row_is_complete(row: dict[str, Any]) -> bool:
    """A stored dossier is trustworthy only if BOTH its current and its peak tier came
    from complete scores — the peak is what the alert path reads."""
    return is_complete_score(row.get("tier")) and is_complete_score(row.get("tier_peak"))


def actor_count(row: dict[str, Any]) -> int | None:
    """THE reader for post-collapse actor count. None means the funding mesh could not
    be computed. Callers must not substitute 1 (asserts a collapse was checked) or the
    raw wallet count (asserts every wallet is an independent actor — the Mojtaba
    overstatement)."""
    v = row.get("actor_count_post_collapse")
    return int(v) if v is not None else None


def raw_wallet_count(wallets: Any) -> int:
    """Raw roster size. Always knowable — it is a count of what was observed, never
    inferred — so unlike the actor count it has no unresolved state."""
    try:
        return len(wallets) if wallets else 1
    except TypeError:
        return 1


def collapse_state(row: dict[str, Any], wallets: Any) -> str:
    """One of ``collapsed`` / ``independent`` / ``solo`` / ``unresolved``.

    ``unresolved`` is returned whenever the actor count is unknown, INCLUDING for a
    single-wallet row, because "one wallet whose funding we could not check" is not the
    same claim as "one actor"."""
    actors = actor_count(row)
    n_raw = raw_wallet_count(wallets)
    if actors is None:
        return "unresolved"
    if actors < n_raw:
        return "collapsed"
    return "solo" if n_raw == 1 else "independent"


def fmt(value: Any, *, unresolved: str = UNRESOLVED) -> str:
    """Render a possibly-unresolved value. Never emits a bare ``None``, and never
    silently substitutes a plausible number."""
    return unresolved if value is None else str(value)
