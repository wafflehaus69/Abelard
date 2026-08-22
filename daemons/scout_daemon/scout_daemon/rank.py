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

SEGMENTS NEVER MERGE (invariant 1). GREEN, GREEN_PROMOTED, POOL, HUMAN_ONLY and
the affiliate lane are ranked and returned separately. GREEN_PROMOTED is displayed
AFTER GREEN and never interleaved: it reached GREEN through the risk-score gate
rather than on the rubric, and merging the lists would erase that distinction
in exactly the surface where it matters.

HUMAN_ONLY IS A GATE, NOT A LABEL (2026-08-21). A source that publishes
`agentAccess: HUMAN_ONLY` has told us the tribe's agents may not execute the
item. Until now `agent_permitted` was consumed in exactly one place --
`risk.py` scores an explicit "no" at 45 points, which blocks PROMOTION -- and
nowhere else. That left the case it does not cover wide open: a row already
mechanically GREEN, never needing promotion, ranked in SEGMENT_GREEN sorted by
payout with nothing distinguishing it from work an agent may actually do.
Measured at the handoff: 23 of 100 ranked rows are flagged human-only by their
own source. Storing an eligibility fact and ranking as though it were absent is
the failure the field exists to prevent.

So an item that WOULD have entered GREEN or GREEN_PROMOTED and carries
`agent_permitted == "no"` is diverted to HUMAN_ONLY. It is still ranked, by the
same key, because the payout is real and Mando may well do it himself -- the
ledger records the distinction rather than resolving it. What it is not is
mixed into the queue an agent reads.

The diversion happens AFTER the base segment is computed, never before. Checking
first would sweep YELLOW human-only rows -- currently unrankable -- into a
ranked segment, widening the ranked set under cover of tightening it.

ONLY AN EXPLICIT "no" GATES -- RULED by Mando 2026-08-21: **a silent source does
not forbid agents, and a GREEN row is the agent-executable class.** `unstated` is
the value on 560 of 669 rows because only Superteam publishes the field at all,
so treating absence as a prohibition would empty GREEN and call it safety. That
is the `_ABSENCE_REASONS` distinction `risk.py` already draws -- a rubric
judgement is not a data absence, and a risk score computed over fields scores a
MISSING field as calm rather than as damning.

The question is closed; do not reopen it from the asymmetric-error rule alone.
That rule ("uncertainty resolves YELLOW, never GREEN") governs CLASSIFICATION --
whether an opportunity is legitimate at all -- and not this field, which asks the
different question of who may execute an item already judged legitimate. Applying
it here would gate 84% of the ledger on a fact no source stated.

A POOL IS NOT A PAYOUT (Mando 2026-08-21). A `program_pool` figure is the size
of a FUND; a `per_task` figure is what one recipient is paid. Sorting them on one
key ranks by who advertises the largest fund. SC-R1 predicted this before a line
was written -- "payout_basis must say so or ranking is meaningless" -- and the
label shipped while the ranker ignored it. Measured 2026-08-21: 30 of 80 ranked
rows were pools averaging $108,054 against 50 per-task rows averaging $1,315,
and pools held 20 of the top 20.

Pools now rank in their own lane, `payout_is_ceiling` is set on them, and the
CLI prints the figure as `<=$X`. `payout_per_recipient_verified` is False
everywhere and stays there until a source publishes a split -- the admitted ZNS
row is the reference case: Superteam's API reports `compensationType: fixed`,
`rewardAmount: 500` for a listing whose page splits 500 across ten places
(100/100/100/50/50/20x5, max realizable $100). No field in the payload carries
that. The honest state is "unknown, bounded above", which is what the flag says.

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

from . import config, payout_check

SEGMENT_GREEN = "GREEN"
SEGMENT_GREEN_PROMOTED = "GREEN_PROMOTED"
SEGMENT_AFFILIATE = "AFFILIATE_PARKED"
SEGMENT_HUMAN_ONLY = "HUMAN_ONLY"
SEGMENT_POOL = "POOL"

# The values the ledger already uses to mark a fund rather than a payment.
PROGRAM_POOL = "program_pool"
POOL_KIND = "pool"

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
    payout_basis: str | None
    payout_is_ceiling: bool
    payout_per_recipient_verified: bool
    expected_usd: float | None
    award_rate: float | None
    award_rate_observed_unix: int | None
    contention: int | None
    verdicts_seen: int
    flip_count: int
    effective_verdict: str
    agent_permitted: str | None
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


def _is_pool(payout_basis: str | None, payout_kind: str | None) -> bool:
    """Does this row's figure describe a FUND rather than a payment?"""
    return payout_basis == PROGRAM_POOL or payout_kind == POOL_KIND


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
    agent_permitted: str | None = None,
    payout_basis: str | None = None,
    payout_kind: str | None = None,
) -> str | None:
    """Which ranked segment a row belongs to, or None if it is not rankable.

    Consumes the EFFECTIVE verdict (doctrine E22), never the raw one.
    """
    if source in affiliate_sources:
        return SEGMENT_AFFILIATE
    if legitimacy_class == "RED":
        return None
    if legitimacy_class == "GREEN_PROMOTED":
        base = SEGMENT_GREEN_PROMOTED
    elif mechanical_class == "GREEN" and not effective_vetoed:
        base = SEGMENT_GREEN
    else:
        return None

    # The gate. Diverted AFTER the base segment is known, so this can only move
    # a row OUT of the agent-executable queue and never widen the ranked set.
    # Only an explicit "no" gates; `unstated` is absence of data, not a bar.
    if agent_permitted == "no":
        return SEGMENT_HUMAN_ONLY

    # A POOL IS NOT A PAYOUT, AND THE TWO MUST NOT SHARE A SORT KEY.
    #
    # SC-R1 predicted this before a line was written: "reward.committed is a
    # PROGRAM POOL, not a per-task payout. payout_basis must say so or ranking
    # is meaningless" (config.py:165). The label shipped -- `program_pool` on
    # 194 rows, `payout_kind='pool'` on 186 -- and then the ranker ignored it.
    #
    # Measured 2026-08-21: 30 of 80 ranked rows were pools averaging $108,054,
    # sorted against 50 per-task rows averaging $1,315. Pools held 20 of the top
    # 20. The queue was ordered by which listings advertise the largest fund,
    # not by what anyone could be paid, which is the recon's warning arriving
    # exactly as written. Third instance of this failure class, and the first to
    # reach the admission surface.
    #
    # Diverted after HUMAN_ONLY, so an ineligible pool is reported as ineligible
    # first -- eligibility is the stronger fact about a row than its payout
    # shape.
    if payout_basis == PROGRAM_POOL or payout_kind == POOL_KIND:
        return SEGMENT_POOL
    return base


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
        SEGMENT_GREEN: [], SEGMENT_GREEN_PROMOTED: [],
        SEGMENT_HUMAN_ONLY: [], SEGMENT_POOL: [], SEGMENT_AFFILIATE: [],
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
            agent_permitted=r.get("agent_permitted"),
            payout_basis=r.get("payout_basis"),
            payout_kind=r.get("payout_kind"),
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
            payout_basis=r.get("payout_basis"),
            # A pool figure is a CEILING on what anyone receives, not a payment.
            payout_is_ceiling=_is_pool(r.get("payout_basis"), r.get("payout_kind")),
            # Nothing in any current source publishes a per-recipient breakdown,
            # so this is False everywhere until one does. Default-false is the
            # point: it says "unknown", never "verified as the full amount".
            payout_per_recipient_verified=False,
            expected_usd=expected_usd(payout, award),
            award_rate=award,
            award_rate_observed_unix=r["award_rate_observed_unix"],
            contention=r["contention"],
            verdicts_seen=eff.scans_seen if eff is not None else 0,
            flip_count=eff.flip_count if eff is not None else 0,
            effective_verdict=eff.state if eff is not None else "GREEN",
            agent_permitted=r.get("agent_permitted"),
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

        # Payout integrity. A sort key the listing's own text contradicts is not
        # a sort key. Flagged rows leave the ORDER, not the ledger -- the
        # scraped field is untouched and the row stays visible with its reason.
        if payout_check.is_flagged(
            text=" ".join(str(r.get(k) or "") for k in ("title", "scope_text", "effort_note")),
            payout_low=payout,
            payout_high=r["payout_usd_high"],
            raw_json=r.get("raw_json"),
        ):
            unranked.append(
                _with(entry, unranked_reason=payout_check.UNRANKED_PAYOUT_UNVERIFIED)
            )
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
       contention, scope_text, effort_note, raw_json, agent_permitted,
       payout_basis, payout_kind
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
    "SEGMENT_HUMAN_ONLY",
    "SEGMENT_POOL",
    "PROGRAM_POOL",
    "POOL_KIND",
]
