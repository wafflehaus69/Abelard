"""Adapter protocol and shared parsing helpers.

Helpers here are deliberately conservative: each returns `None` rather than a
guess when the input does not clearly say what it means. A null payout is a
fact the ledger can carry; a fabricated one is a number Mando might act on.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from ..fetch import FetchResult
from ..models import RawItem


@dataclass
class AdapterResult:
    """One source's contribution to a scan."""

    source: str
    status: str                      # 'ok' | 'empty' | 'error'
    items: list[RawItem] = field(default_factory=list)
    detail: str = ""
    resolved_via: str = ""

    def high_watermark_unix(self) -> int | None:
        """Newest usable timestamp among ingested items, or None.

        `posted_unix` only -- never `deadline_unix`. A deadline is a future
        date; using it would push the watermark ahead of real time and skip
        everything published in between. This is the same footgun as advancing
        to `now`, wearing a different hat.
        """
        stamps = [i.posted_unix for i in self.items if i.posted_unix]
        return max(stamps) if stamps else None


class Adapter(Protocol):
    """A read-only reader for one surface."""

    source_name: str

    def fetch(self, client: Any, *, now_unix: int, since_unix: int) -> AdapterResult:
        ...


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_CURRENCY_SYMBOLS = {"$": "USD", "€": "EUR", "£": "GBP", "₹": "INR"}
_CURRENCY_CODES = (
    "USD", "USDC", "USDG", "USDT", "EUR", "CHF", "GBP", "INR", "ARB",
    "DAI", "SOL", "ETH", "OP", "DOT", "AXL",
)

# Matches "1000", "25 000", "25,000", "1.5", "2 000.50" -- Zindi writes
# "$25 000 USD" with a non-breaking-space-ish separator, hence the space class.
_NUMBER_RE = re.compile(r"\d[\d\s,]*(?:\.\d+)?")


def parse_amount(text: str | None) -> float | None:
    """First number in a string, separators tolerated. None when absent."""
    if not text:
        return None
    match = _NUMBER_RE.search(text.replace(" ", " "))
    if not match:
        return None
    cleaned = match.group(0).replace(",", "").replace(" ", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


# Monetary figure in PROSE: a currency anchor, a number, an optional magnitude
# suffix. The anchor is what separates this from `parse_amount` above.
_MONETARY_RE = re.compile(
    r"(?:\$\s*|USD\s*)(?P<n>\d[\d,]*(?:\.\d+)?)\s*(?P<suf>[KkMmBb])?"
    r"|(?P<n2>\d[\d,]*(?:\.\d+)?)\s*(?P<suf2>[KkMmBb])?\s*(?:\$|\s*USD\b)"
)
_MAGNITUDE = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}


def parse_monetary(text: str | None) -> float | None:
    """First MONETARY figure in prose, or None. For description-shaped text only.

    WHY THIS EXISTS SEPARATELY FROM `parse_amount`. `parse_amount` returns the
    first number of any kind, which is correct for the callers that hand it a
    clean amount string ("25,000 USD committed"). It is wrong for prose, and two
    adapters hand it prose. Measured 2026-08-19 against live arbitrum_grants
    rows:

        "offers $10M in ARB over 12 months"   parse_amount -> 10.0
        "up to $250K per grant"               parse_amount -> 250.0
        "Trailblazer 2"                       parse_amount -> 2.0   (from the title)

    The first two are magnitude errors of 10^6 and 10^3. The third is worse: a
    payout invented out of a digit in a program name, with no currency anywhere
    in the text. Ranking sorts by payout descending, so a 10^6 underestimate
    hides a listing and a spurious $2 fabricates one.

    `parse_amount` IS DELIBERATELY LEFT ALONE. Its other call sites pass
    strings where the first number is the payout and no currency symbol is
    present; requiring an anchor there would return None and break them. The
    defect is not in the helper, it is in handing prose to a helper that never
    claimed to read prose.

    Rules:
      * A currency anchor is REQUIRED. No `$` and no `USD` means no figure --
        "2M ARB" returns None, because a token quantity is not a USD amount and
        the caller stores this in `payout_usd_low`.
      * K/M/B multiply. "$10M" is 10,000,000, not 10.
      * FIRST match wins, matching `parse_amount`'s contract. In observed
        listings the headline figure leads.
    """
    if not text:
        return None
    match = _MONETARY_RE.search(text.replace(" ", " "))
    if not match:
        return None
    raw = match.group("n") or match.group("n2")
    suffix = (match.group("suf") or match.group("suf2") or "").lower()
    try:
        value = float(raw.replace(",", ""))
    except (AttributeError, ValueError):
        return None
    return value * _MAGNITUDE.get(suffix, 1)


def parse_currency(text: str | None, default: str | None = None) -> str | None:
    """Currency code from an explicit code, else a leading symbol, else default."""
    if not text:
        return default
    upper = text.upper()
    for code in _CURRENCY_CODES:
        if re.search(rf"\b{code}\b", upper):
            return code
    for symbol, code in _CURRENCY_SYMBOLS.items():
        if symbol in text:
            return code
    return default


def iso_to_unix(value: str | None) -> int | None:
    """ISO-8601 (with Z or offset) to epoch seconds. None on anything unclear."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def strip_html(text: str | None, limit: int = 4000) -> str | None:
    """Tags out, entities for the few that matter, whitespace collapsed."""
    if not text:
        return None
    without_tags = re.sub(r"<[^>]+>", " ", text)
    for entity, char in (
        ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
        ("&quot;", '"'), ("&#39;", "'"), ("&nbsp;", " "),
    ):
        without_tags = without_tags.replace(entity, char)
    collapsed = re.sub(r"\s+", " ", without_tags).strip()
    return collapsed[:limit] or None


def error_result(source: str, result: FetchResult) -> AdapterResult:
    """Uniform mapping from a failed fetch to a failed adapter result."""
    return AdapterResult(
        source=source,
        status="error",
        detail=result.detail,
        resolved_via=result.resolved_url,
    )


__all__ = [
    "Adapter",
    "AdapterResult",
    "parse_amount",
    "parse_monetary",
    "parse_currency",
    "iso_to_unix",
    "strip_html",
    "error_result",
]
