"""Payout integrity cross-check: compare the listing's own text to the parsed field.

FLAG, NEVER CORRECT. Scraped fields are CLAIMS. This module reads them and
writes nothing back -- there is no code path here that updates `payout_usd_low`,
`payout_raw`, or any other ingested column, and a test asserts it the same way
the admissions file is asserted un-writable. A cross-check that silently
"fixed" a payout would replace a counterparty's claim with our inference and
leave no way to tell which is in the ledger.

WHY THE CHECK EXISTS. Ranking sorts by `payout_usd_low` descending, which makes
a unit-scale error an ERROR AMPLIFIER: the biggest numbers rise, and mis-parsed
numbers are among the biggest. Measured 2026-08-19, one row sat at rank #6 with
`payout_usd_low` = $100,100 while its own title read "$50" -- a 2002x
disagreement at the top of the queue Mando reads first.

THE RATIO ALONE IS NECESSARY AND INSUFFICIENT. This is the finding that shaped
the rule. Four questbook rows disagree by 11x-28x and NONE of them is an error:

    Compound dapps and protocol ideas   field $25,000   text $709,300   28x
    Dapps and Ideas Domain              field $25,000   text $499,370   20x
    Arbitrum Gaming 3.0                 field $50,000   text $850,049   17x
    Arbitrum Education & Community      field $50,000   text $540,363   11x

`payout_usd_low` is `reward.committed` -- the pool committed to THIS round.
The figure in the description is `totalGrantFundingDisbursedUSD` -- cumulative
historical disbursement across all rounds. Two different, both-correct
quantities. A bare ratio cut at 5x would have flagged all four as suspect and
demoted real programs out of the queue.

So the rule has two parts, and the second is what makes it safe:

    flag when   text_figure / parsed_field  >=  5x
    AND         no other numeric value in the SOURCE'S OWN payload matches the
                text figure within 1%

The second clause asks: does the source itself already explain this number? If
the figure appears somewhere in the row's `raw_json`, then the text is quoting a
different published field rather than contradicting the parsed one, and there is
nothing to flag. Only a figure the source cannot account for is evidence of a
parse failure.

CHARITABLE MATCHING. A listing may mention several sums. The check compares
against the CLOSEST one, so a title that happens to name an unrelated figure
alongside the real payout is not flagged on the unrelated one.
"""

from __future__ import annotations

import json
import re

# Both constants are measured, not chosen. 5x sits above the entire benign
# questbook cluster (max 28x is excluded by the payload test, not by the ratio)
# and below the one genuine suspect at 2002x. 1% is a float-representation
# tolerance, not a judgment band.
MISMATCH_RATIO = 5.0
EXPLAIN_TOLERANCE = 0.01

UNRANKED_PAYOUT_UNVERIFIED = "payout_unverified: title/field mismatch"

_MONEY = re.compile(
    r"(?:\$\s*(\d[\d,]*(?:\.\d+)?))"      # $1,500
    r"|(?:(\d[\d,]*(?:\.\d+)?)\s*\$)"     # 1500$
)


def monetary_mentions(text: str | None) -> list[float]:
    """Positive dollar figures in free text.

    ZERO IS EXCLUDED DELIBERATELY. "$0 disbursed" is a status, not a payout
    claim, and dividing by it produced ratios in the trillions during Phase 0 --
    noise that would have buried the one real finding.
    """
    out: list[float] = []
    for m in _MONEY.finditer(text or ""):
        raw = m.group(1) or m.group(2)
        try:
            v = float(raw.replace(",", ""))
        except ValueError:
            continue
        if v > 0:
            out.append(v)
    return out


def _payload_numbers(obj, acc: list[float] | None = None) -> list[float]:
    """Every positive number anywhere in the source's own payload."""
    acc = [] if acc is None else acc
    if isinstance(obj, dict):
        for v in obj.values():
            _payload_numbers(v, acc)
    elif isinstance(obj, list):
        for v in obj:
            _payload_numbers(v, acc)
    elif isinstance(obj, bool):
        pass                      # bool is an int subclass; never a payout
    elif isinstance(obj, (int, float)) and obj > 0:
        acc.append(float(obj))
    return acc


def explained_by_payload(value: float, raw_json: str | None) -> float | None:
    """The source's own field that accounts for `value`, if any."""
    if not raw_json:
        return None
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError):
        return None
    for candidate in _payload_numbers(payload):
        if abs(candidate - value) <= EXPLAIN_TOLERANCE * value:
            return candidate
    return None


def disagreement(
    *,
    text: str | None,
    payout_low: float | None,
    payout_high: float | None = None,
    raw_json: str | None = None,
) -> tuple[float, float, float | None] | None:
    """(ratio, text_figure, explaining_field) for the closest mention, or None.

    Returns None when the check cannot run at all -- no mention, or no parsed
    payout. An unmeasurable row is NOT a passing row, and callers must not treat
    it as one.
    """
    if not payout_low or payout_low <= 0:
        return None
    mentions = monetary_mentions(text)
    if not mentions:
        return None

    def ratio(m: float) -> float:
        if payout_high and payout_low <= m <= payout_high:
            return 1.0
        target = payout_low
        if payout_high and abs(m - payout_high) < abs(m - payout_low):
            target = payout_high
        return max(m, target) / min(m, target)

    closest = min(mentions, key=ratio)
    return ratio(closest), closest, explained_by_payload(closest, raw_json)


def is_flagged(
    *,
    text: str | None,
    payout_low: float | None,
    payout_high: float | None = None,
    raw_json: str | None = None,
) -> bool:
    """True when BOTH clauses hold: big disagreement AND source cannot explain it."""
    result = disagreement(
        text=text, payout_low=payout_low, payout_high=payout_high, raw_json=raw_json
    )
    if result is None:
        return False
    ratio, _figure, explained = result
    return ratio >= MISMATCH_RATIO and explained is None


__all__ = [
    "MISMATCH_RATIO",
    "EXPLAIN_TOLERANCE",
    "UNRANKED_PAYOUT_UNVERIFIED",
    "monetary_mentions",
    "explained_by_payload",
    "disagreement",
    "is_flagged",
]
