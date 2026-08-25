"""Field lineage: the archive's twenty years of format drift, as DATA.

The archive spans 2006-06 to 2026-08 in three format families and at least four
column widths (26 / 88 / 215 / 262 / 448), across pipe-delimited ``.txt``, XLSX
and CSV. The obvious implementation is a parser per era. This is not that: it is
ONE resolver plus a table, so adding an era is a table entry rather than code.

Every canonical field lists the column-name patterns it has been observed under.
Resolution is per-snapshot, against the header the file actually has, and a
field that does not resolve is recorded as **absent for that era** rather than
defaulted -- a missing column and a reported zero are different facts, and the
whole point of the exercise is that they stay different [PA-1.0 absence rule].

Patterns are ordered most-specific first and matched case-insensitively against
the stripped header cell. They are anchored where the real name is exact
(``^5A$``), because ``5A`` unanchored also matches ``5A(1)`` in wider eras.

Observed per-era availability, measured 2026-08-24 on eight sampled snapshots:

    field            2006   2012   2016   2018-2023   2026
    crd               yes    yes    yes      yes       yes
    legal_name        yes    yes    yes      yes       yes
    sec_status         -     yes    yes      yes       yes
    filing_date        -     yes    yes      yes       yes
    employees          -      -     yes      yes       yes
    aum_total          -     yes    yes      yes       yes
    acquired_firm      -      -      -        -        yes
    owners             -      -      -        -         -      (never, any era)
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: canonical field -> ordered list of observed column-name patterns.
FIELD_PATTERNS: dict[str, tuple[str, ...]] = {
    "crd": (r"^organization crd\s*#?$", r"^crd\s*#?$"),
    "legal_name": (r"^legal name$",),
    "primary_name": (r"^primary business name$",),
    "sec_number": (r"^sec\s*#$",),
    "main_city": (r"^main office city$", r"^main office city, state, postal code$"),
    "main_state": (r"^main office state$",),
    "filing_date": (r"^latest adv filing date$", r"^date submitted$"),
    "sec_status": (r"^sec current status$", r"^status$"),
    "total_employees": (r"^5a$",),
    "aum_total": (r"^5f\(2\)\(c\)$",),
    "accounts_total": (r"^5f\(2\)\(f\)$",),
    "acquired_name": (r"^acquired firm$",),
    "acquired_crd": (r"^acquired firm crd\s*#?$",),
}

#: Fields whose movement between snapshots is a transition event worth recording.
#: Deliberately narrow, and deliberately NOT including address churn -- see the
#: live pipeline's enrich-trigger set for the same reasoning.
TRACKED_FIELDS: tuple[str, ...] = (
    "legal_name",
    "sec_status",
    "total_employees",
    "aum_total",
)

#: The join key. Stable across every era measured; a whitespace difference in the
#: header is the only drift.
JOIN_KEY = "crd"


@dataclass(frozen=True)
class Resolution:
    """Which canonical fields this snapshot's header actually supplies."""

    index: dict[str, int]
    absent: tuple[str, ...]
    n_columns: int

    def get(self, row: list, field: str) -> str | None:
        i = self.index.get(field)
        if i is None or i >= len(row):
            return None
        v = row[i]
        if v is None:
            return None
        v = str(v).strip()
        return v or None


def resolve(header: list[str]) -> Resolution:
    """Map canonical fields onto this header's column positions.

    Unresolved fields are returned in ``absent`` so the caller can record era
    coverage instead of silently reading None as a measured value.
    """
    cleaned = [(h or "").strip().strip('"').strip() for h in header]
    index: dict[str, int] = {}
    for field, pats in FIELD_PATTERNS.items():
        for pat in pats:
            hit = next((i for i, h in enumerate(cleaned) if re.match(pat, h, re.I)), None)
            if hit is not None:
                index[field] = hit
                break
    absent = tuple(f for f in FIELD_PATTERNS if f not in index)
    return Resolution(index=index, absent=absent, n_columns=len(cleaned))


def era_label(resolution: Resolution) -> str:
    """A coarse era name derived from what the header CARRIES, not its filename.

    Filenames drift independently of content (six naming conventions observed),
    so content decides. [I-9: presentation is never provenance -- and a filename
    is presentation.]
    """
    has = resolution.index.__contains__
    if has("acquired_name"):
        return "2026-wide"
    if has("total_employees"):
        return "part1a-full"
    if has("aum_total"):
        return "part1a-partial"
    return "roster-only"


def normalize_int(value: str | None) -> int | None:
    """Parse a numeric cell. Returns None for absent/unparseable -- never 0.

    XLSX cells arrive as ``'8'`` or ``'8.0'`` or ``8``; CSV brings commas and
    currency noise. A value we cannot read is not a zero.
    """
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None
