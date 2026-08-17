"""Interim within-segment ranking (SC-R2 Phase 1).

RANKING ORDERS. IT NEVER ADMITS. There is no code path from this module to
`status='admitted'` and a test asserts the string does not appear here. Rank is
a reading order for Mando's queue, not a decision about it.

WHY PAYOUT AND NOT EXPECTED VALUE
---------------------------------
The intended sort key was `expected_usd = payout_usd * P(award)`. Measured
2026-08-14: P(award) is derivable on 23 of 99 GREEN-family rows (23.2%), below
the 40% line pre-registered for shipping it, and the derivable ones are mostly
worthless -- 34 of 63 questbook award rates are `selected 0 / applied 1`, which
is an empty new listing rather than a 0% award rate. So the interim key is
`payout_usd_low` descending.

`payout_usd_low` is the CONSERVATIVE bound of the parsed range. A listing
advertising "$500-$5,000" sorts at 500, not 5,000, so the order under-promises.

STATED BIAS: prize-size ordering over-weights contested items. The biggest
prize is usually the one most people are chasing, and with P(award) unavailable
this ranking cannot discount for that. It is acceptable HERE only because
admission stays behind Mando's gate -- the ranking decides what he reads first,
never what the tribe does. If anything downstream ever acts on rank without a
human in between, this bias stops being acceptable and the key must change.

`expected_usd` is still COMPUTED and carried where both operands exist, so the
minority of rows that can be value-ranked are visible as such. It is not the
sort key and must not silently become one while coverage is this thin.

SEGMENTS NEVER MERGE (invariant 1). GREEN, GREEN_PROMOTED and the affiliate
lane are ranked and returned separately. GREEN_PROMOTED is displayed AFTER
GREEN and never interleaved: it reached GREEN through the risk-score gate
rather than on the rubric, and merging the lists would erase that distinction
in exactly the surface where it matters.

CHURN IS EXPOSED, NOT EXCLUDED (Mando 2026-08-15). Classification flips at a
measured ~2.5% per scan, so a row can sit near the GREEN boundary and breathe
across it. Every current effective-GREEN row ranks; the flip history rides
along as columns (`verdicts_seen`, `flip_count`, `effective_verdict`) so an
edge-breather is visible rather than quietly dropped.

NO LLM CALL. NO LIVE FETCH. Everything here is a pure function of stored
columns, which is what makes two consecutive runs byte-identical.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

from . import config

SEGMENT_GREEN = "GREEN"
SEGMENT_GREEN_PROMOTED = "GREEN_PROMOTED"
SEGMENT_AFFILIATE = "AFFILIATE_PARKED"

# Recorded verbatim as the park reason, so the ledger says WHY rather than
# leaving a lane mysteriously empty. Measured 2026-08-14 across all 130
# affiliate rows: `cookie_days` is present in raw_json, commission rate is
# absent as a column, absent from raw_json, and absent from every source.
AFFILIATE_PARK_REASON = "commission_rate: not published in any current source"

UNRANKED_NO_PAYOUT = "payout_usd absent -- no sort key"
UNRANKED_AFFILIATE = AFFILIATE_PARK_REASON

RANK_ALGORITHM_VERSION = "sc-r2-p1-payout-desc-2026-08-15"


@dataclass(frozen=True)
class RankedRow:
    opportunity_id: str
    source: str
    title: str
    segment: str
    position: int | None
    sort_key: float | None
    payout_usd_low: float | None
    payout_usd_high: float | None
    expected_usd: float | None
    award_rate: float | None
    award_rate_observed_unix: int | None
    contention: int | None
    verdicts_seen: int
    flip_count: int
    effective_verdict: str
    unranked_reason: str | None

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class RankResult:
    ranked: dict[str, list[RankedRow]] = field(default_factory=dict)
    unranked: list[RankedRow] = field(default_factory=list)
    oldest_input_unix: int | None = None
    newest_input_unix: int | None = None

    @property
    def total_rows(self) -> int:
        return sum(len(v) for v in self.ranked.values()) + len(self.unranked)


def _affiliate_sources() -> frozenset[str]:
    return frozenset(s.name for s in config.WIRE_SOURCES if s.lane == "affiliate")


def expected_usd(payout: float | None, award_rate: float | None) -> float | None:
    """Invariant 3: computes ONLY when both operands are verified-present.

    A missing award rate is never imputed as 1/entrants -- an inferred
    probability is an estimate wearing a measurement's clothes, and it would
    then be indistinguishable from a real one in the same column.
    """
    if payout is None or award_rate is None:
        return None
    return payout * award_rate


def classify_segment(
    *,
    source: str,
    legitimacy_class: str,
    mechanical_class: str | None,
    effective_vetoed: bool,
    affiliate_sources: frozenset[str],
) -> str | None:
    """Which ranked segment a row belongs to, or None if it is not rankable.

    Consumes the EFFECTIVE verdict (doctrine E22), never the raw one.
    """
    if source in affiliate_sources:
        return SEGMENT_AFFILIATE
    if legitimacy_class == "RED":
        return None
    if legitimacy_class == "GREEN_PROMOTED":
        return SEGMENT_GREEN_PROMOTED
    if mechanical_class == "GREEN" and not effective_vetoed:
        return SEGMENT_GREEN
    return None


def _sort_key(row: RankedRow) -> tuple:
    """Descending payout, then opportunity_id ASC as a total-order tiebreak.

    The tiebreak is not cosmetic: without it, equal payouts order by whatever
    the query happened to return, and the pre-registered byte-identical
    requirement across two runs would hold only by luck.
    """
    return (-(row.sort_key or 0.0), row.opportunity_id)


def build_ranking(rows: list[dict], effective: dict) -> RankResult:
    """Pure function: (stored rows, effective verdicts) -> ranking.

    `rows` are dict-likes of stored columns. `effective` maps opportunity_id to
    an object exposing .vetoed / .scans_seen / .flip_count / .state.
    """
    affiliates = _affiliate_sources()
    buckets: dict[str, list[RankedRow]] = {
        SEGMENT_GREEN: [], SEGMENT_GREEN_PROMOTED: [], SEGMENT_AFFILIATE: []
    }
    unranked: list[RankedRow] = []
    observed: list[int] = []

    for r in rows:
        oid = r["opportunity_id"]
        eff = effective.get(oid)
        vetoed = bool(eff.vetoed) if eff is not None else False
        segment = classify_segment(
            source=r["source"],
            legitimacy_class=r["legitimacy_class"],
            mechanical_class=r["mechanical_class"],
            effective_vetoed=vetoed,
            affiliate_sources=affiliates,
        )
        if segment is None:
            continue

        payout = r["payout_usd_low"]
        award = r["award_rate"]
        if r["award_rate_observed_unix"] is not None:
            observed.append(r["award_rate_observed_unix"])

        entry = RankedRow(
            opportunity_id=oid,
            source=r["source"],
            title=r["title"] or "",
            segment=segment,
            position=None,
            sort_key=payout,
            payout_usd_low=payout,
            payout_usd_high=r["payout_usd_high"],
            expected_usd=expected_usd(payout, award),
            award_rate=award,
            award_rate_observed_unix=r["award_rate_observed_unix"],
            contention=r["contention"],
            verdicts_seen=eff.scans_seen if eff is not None else 0,
            flip_count=eff.flip_count if eff is not None else 0,
            effective_verdict=eff.state if eff is not None else "GREEN",
            unranked_reason=None,
        )

        # The affiliate lane is PARKED, not ranked -- its own key
        # (commission x cookie window) has an operand that does not exist.
        # Parked rows are reported with the reason, never dropped (invariant 2).
        if segment == SEGMENT_AFFILIATE:
            unranked.append(_with(entry, unranked_reason=UNRANKED_AFFILIATE))
            continue
        if payout is None:
            # Invariant 2: no payout means unrankable, reported WITH the reason.
            # Never defaulted to the bottom, which would read as "measured and
            # worth least" rather than "not measured".
            unranked.append(_with(entry, unranked_reason=UNRANKED_NO_PAYOUT))
            continue
        buckets[segment].append(entry)

    ranked: dict[str, list[RankedRow]] = {}
    for segment, entries in buckets.items():
        if segment == SEGMENT_AFFILIATE:
            continue
        entries.sort(key=_sort_key)
        ranked[segment] = [_with(e, position=i + 1) for i, e in enumerate(entries)]

    return RankResult(
        ranked=ranked,
        unranked=sorted(unranked, key=lambda e: (e.source, e.opportunity_id)),
        oldest_input_unix=min(observed) if observed else None,
        newest_input_unix=max(observed) if observed else None,
    )


def _with(row: RankedRow, **changes) -> RankedRow:
    data = asdict(row)
    data.update(changes)
    return RankedRow(**data)


# ---------------------------------------------------------------------------
# The only impure functions in this module. Everything above is a pure
# function of its arguments, which is what the byte-identical-rerun test rests
# on -- keep the boundary here.
# ---------------------------------------------------------------------------

_LOAD_SQL = """
SELECT opportunity_id, source, title, legitimacy_class, mechanical_class,
       payout_usd_low, payout_usd_high, award_rate, award_rate_observed_unix,
       contention
FROM opportunities
ORDER BY opportunity_id
"""


def load_rows(conn) -> list[dict]:
    """Stored columns only. No fetch, no network, no LLM -- ORDER BY makes the
    input sequence deterministic before the sort ever runs."""
    return [dict(r) for r in conn.execute(_LOAD_SQL)]


def write_ranking(conn, result: RankResult, *, now_unix: int) -> int:
    """Project the ranking onto `opportunities`. Derived state, rewritten whole.

    Every rank column is cleared first, so a row that drops out of the ranked
    set does not keep a stale position -- a stale rank is worse than no rank
    because it still looks authoritative.
    """
    conn.execute(
        "UPDATE opportunities SET rank_segment=NULL, rank_position=NULL,"
        " rank_sort_key=NULL, rank_expected_usd=NULL, rank_unranked_reason=NULL,"
        " rank_algorithm_version=NULL, rank_computed_unix=NULL"
    )
    written = 0
    everything = [r for rows in result.ranked.values() for r in rows] + result.unranked
    for row in everything:
        # An UNRANKED row records its reason but must NOT claim a segment.
        # Writing `rank_segment='GREEN'` on a row with `rank_position=NULL`
        # made the obvious consumer query --
        #   WHERE rank_segment='GREEN' ORDER BY rank_position
        # -- return the unranked rows FIRST, because SQLite sorts NULL low.
        # The ranked list is `rank_position IS NOT NULL`; the segment column
        # answers "where does this sit in the order", and an unranked row has
        # no place in any order.
        segment = row.segment if row.position is not None else None
        conn.execute(
            "UPDATE opportunities SET rank_segment=?, rank_position=?,"
            " rank_sort_key=?, rank_expected_usd=?, rank_unranked_reason=?,"
            " rank_algorithm_version=?, rank_computed_unix=?,"
            " verdicts_seen=?, flip_count=?, effective_verdict=?"
            " WHERE opportunity_id=?",
            (segment, row.position, row.sort_key, row.expected_usd,
             row.unranked_reason, RANK_ALGORITHM_VERSION, now_unix,
             row.verdicts_seen, row.flip_count, row.effective_verdict,
             row.opportunity_id),
        )
        written += 1
    conn.commit()
    return written


def rank_ledger(conn, *, now_unix: int) -> RankResult:
    """Load, rank, persist. The verb the CLI calls."""
    from . import verdicts as _verdicts

    result = build_ranking(load_rows(conn), _verdicts.effective_all(conn))
    write_ranking(conn, result, now_unix=now_unix)
    return result


__all__ = [
    "SEGMENT_GREEN",
    "SEGMENT_GREEN_PROMOTED",
    "SEGMENT_AFFILIATE",
    "AFFILIATE_PARK_REASON",
    "UNRANKED_NO_PAYOUT",
    "RANK_ALGORITHM_VERSION",
    "RankedRow",
    "RankResult",
    "expected_usd",
    "classify_segment",
    "build_ranking",
]
