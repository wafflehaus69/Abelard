"""Stated-magnitude extractor — pure script, no LLM, no I/O.

Pass C magnitude-awareness (2026-07-07). News Watch tags on words, not
numbers, so a "$2B chips lost at sea" headline reaches the synthesis LLM
only as raw text it may or may not weigh — a $2B and a $2M story can score
identically. This leaf pulls the stated magnitudes OUT of the headline as a
structured, explicit signal the Pass C prompt can surface. It does NOT
score: the LLM still judges size against the calibration anchors; this only
makes the number visible and consistent.

Core principle (the noise guard): a number becomes a Magnitude ONLY if it
carries a qualifier — a currency marker, a scale word, a recognized unit,
or a percent/bps marker. A bare integer with none of these is not
extracted. That single rule drops "3 senators", "two ships",
counts-in-passing, standalone years, versions, and phone-like runs by
construction (none carry a qualifier adjacent in a matching position).

`extract_magnitudes` is total over `str`: it returns () when nothing
qualifies and never raises on ordinary text (a regex over a string has no
data-source soft-fail surface). It raises only on non-`str` input, which is
a programming error.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Magnitude:
    """One stated magnitude extracted from headline text.

    `raw_span` is the verbatim source text ("$2B", "40M barrels", "300bps")
    — the prompt renders THIS, not the normalized float, so the model reads
    the real article language. `value` is scale-normalized numeric.
    """

    value: float
    unit: str    # "USD"|"EUR"|"GBP"|"JPY"|"percent"|"bps"->percent|"barrels"|"tonnes"|"MW"|"GW"|"TWh"...
    kind: str    # "currency" | "percent" | "volume"
    raw_span: str


# Scale words → multiplier. Longest-first alternation in the regex so
# "million" wins over "m"; a trailing \b forces whole-word scale matches so
# "$2 to $3" does not read "to" as a "t"(trillion) scale.
_SCALE = {
    "k": 1e3,
    "m": 1e6, "mn": 1e6, "million": 1e6,
    "b": 1e9, "bn": 1e9, "billion": 1e9,
    "t": 1e12, "tn": 1e12, "trillion": 1e12,
}
_SCALE_ALT = "million|mn|billion|bn|trillion|tn|m|b|t|k"

_CCY_SYMBOL = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"}

_NUM = r"\d[\d,]*(?:\.\d+)?"

# Physical-commodity units that take a numeric scale word ("40M barrels").
_COMMODITY_ALT = "barrels|barrel|tonnes|tonne|tons|ton"
_COMMODITY_CANON = {
    "barrel": "barrels", "barrels": "barrels",
    "ton": "tonnes", "tons": "tonnes", "tonne": "tonnes", "tonnes": "tonnes",
}
# Power/energy units where the prefix is intrinsic to the unit ("2.5GW") —
# NO scale word, and never cross-normalized to each other.
_POWER_ALT = "TWh|GWh|MWh|kWh|GW|MW|kW"
_POWER_CANON = {
    "twh": "TWh", "gwh": "GWh", "mwh": "MWh", "kwh": "kWh",
    "gw": "GW", "mw": "MW", "kw": "kW",
}

_CURRENCY_SYMBOL_RE = re.compile(
    rf"(?P<sym>[$€£¥])\s?(?P<num>{_NUM})\s?(?P<scale>{_SCALE_ALT})?\b",
    re.IGNORECASE,
)
_CURRENCY_CODE_RE = re.compile(
    rf"\b(?P<code>USD|EUR|GBP|JPY)\s?(?P<num>{_NUM})\s?(?P<scale>{_SCALE_ALT})?\b",
)
# `%` self-delimits (a trailing \b after it fails at end-of-string); the
# word alternatives carry their own \b so "20 percentage" doesn't match.
_PERCENT_RE = re.compile(
    rf"(?P<num>{_NUM})\s?(?P<pct>%|(?:percent|bps|basis\s?points)\b)",
    re.IGNORECASE,
)
_COMMODITY_RE = re.compile(
    rf"(?P<num>{_NUM})\s?(?P<scale>{_SCALE_ALT})?\s?(?P<unit>{_COMMODITY_ALT})\b",
    re.IGNORECASE,
)
_POWER_RE = re.compile(
    rf"(?P<num>{_NUM})\s?(?P<unit>{_POWER_ALT})\b",
)


def _num(s: str) -> float:
    return float(s.replace(",", ""))


def _scale_mult(scale: str | None) -> float:
    if not scale:
        return 1.0
    return _SCALE[scale.lower()]


def _currency(m: re.Match[str], unit: str) -> Magnitude:
    value = _num(m.group("num")) * _scale_mult(m.group("scale"))
    return Magnitude(value=value, unit=unit, kind="currency", raw_span=m.group(0))


def extract_magnitudes(text: str) -> tuple[Magnitude, ...]:
    """Extract stated magnitudes from `text`. Total over str; returns ()
    when nothing qualifies. Overlapping matches are resolved left-to-right,
    longest-first, so each stretch of text yields at most one Magnitude."""
    if not isinstance(text, str):
        raise TypeError(f"extract_magnitudes expects str, got {type(text).__name__}")
    if not text:
        return ()

    candidates: list[tuple[int, int, Magnitude]] = []

    for m in _CURRENCY_SYMBOL_RE.finditer(text):
        candidates.append((m.start(), m.end(), _currency(m, _CCY_SYMBOL[m.group("sym")])))
    for m in _CURRENCY_CODE_RE.finditer(text):
        candidates.append((m.start(), m.end(), _currency(m, m.group("code").upper())))

    for m in _PERCENT_RE.finditer(text):
        pct = m.group("pct").lower().replace(" ", "")
        raw_num = _num(m.group("num"))
        # bps / basis points → percent (300bps = 3.0%), raw_span preserved.
        value = raw_num / 100.0 if pct in ("bps", "basispoints") else raw_num
        candidates.append((m.start(), m.end(), Magnitude(
            value=value, unit="percent", kind="percent", raw_span=m.group(0),
        )))

    for m in _COMMODITY_RE.finditer(text):
        value = _num(m.group("num")) * _scale_mult(m.group("scale"))
        unit = _COMMODITY_CANON[m.group("unit").lower()]
        candidates.append((m.start(), m.end(), Magnitude(
            value=value, unit=unit, kind="volume", raw_span=m.group(0),
        )))
    for m in _POWER_RE.finditer(text):
        value = _num(m.group("num"))
        unit = _POWER_CANON[m.group("unit").lower()]
        candidates.append((m.start(), m.end(), Magnitude(
            value=value, unit=unit, kind="volume", raw_span=m.group(0),
        )))

    # Resolve overlaps: earliest start first, then longest span; greedily
    # accept a candidate only if it does not overlap an already-accepted one.
    candidates.sort(key=lambda c: (c[0], -(c[1] - c[0])))
    accepted: list[tuple[int, int, Magnitude]] = []
    occupied: list[tuple[int, int]] = []
    for start, end, mag in candidates:
        if any(start < oe and end > os for os, oe in occupied):
            continue
        accepted.append((start, end, mag))
        occupied.append((start, end))

    accepted.sort(key=lambda c: c[0])
    return tuple(mag for _, _, mag in accepted)


__all__ = ["Magnitude", "extract_magnitudes"]
