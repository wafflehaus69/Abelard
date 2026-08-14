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
    "parse_currency",
    "iso_to_unix",
    "strip_html",
    "error_result",
]
