"""Order-invariant change detection.

This module exists because of one measurement, and it is the most expensive
thing in the build to get wrong.

Diffing the 2026-08-14 and 2026-08-21 IAPD snapshots (23,731 firms present in
both):

    raw byte diff          6,150 firms   25.92%
    order-normalised diff    462 firms    1.95%
    false positives        5,688 firms   92.5% of raw hits

The publisher emits ``<States>`` notice-filing children in UNSTABLE ORDER --
same set, shuffled between snapshots. One sampled record differed only by an
identical line appearing on both sides of the diff.

A change key built on raw bytes therefore fires ~5,688 spurious triggers per
week. At the measured mean document size that is roughly 11 GB/week of pointless
retrieval, and worse, it buries the 462 real movements in noise until the
pipeline looks broken and gets abandoned.

This is [E12] with a new face. "Dedup keys are content-derived" is necessary and
INSUFFICIENT: the key must also be ORDER-INVARIANT over every repeated child.
The rule generalises past this feed -- any publisher emitting a set as a
sequence will do this, and nothing in the payload announces it.
"""

from __future__ import annotations

import hashlib
from dataclasses import fields as dataclass_fields

from .feed import FirmRecord

#: Fields that carry meaning for change detection. Everything else in the
#: record is either identity (crd) or provenance (source_feed) and would make
#: the key fire on bookkeeping.
#:
#: NECESSARILY INCOMPLETE, and deliberately a list of things we have MEASURED
#: moving rather than everything the form offers. A field added here starts
#: firing deltas immediately, so additions are a decision, not a tidy-up.
CHANGE_FIELDS: tuple[str, ...] = (
    "legal_name",
    "business_name",
    "sec_number",
    "umbrella",
    "rgstn_type",
    "rgstn_status",
    "rgstn_date",
    "filing_date",
    "form_version",
    "city",
    "state",
    "country",
    "postal_code",
    "total_employees",
    "advisory_employees",
    "aum_total",
    "aum_discretionary",
    "aum_non_discretionary",
    "accounts_total",
    "clients_hnw",
    "disciplinary_flag",
    "notice_states",
)

#: Fields whose movement is worth a per-firm document pull. Deliberately NARROW.
#: `filing_date` moving alone means the firm amended something -- which is
#: exactly when Item 4 and Schedule A may have changed -- so it is the primary
#: trigger. Address and name churn is not.
ENRICH_TRIGGER_FIELDS: frozenset[str] = frozenset(
    {
        "filing_date",
        "rgstn_status",
        "rgstn_type",
        "umbrella",
        "legal_name",
        "aum_total",
        "total_employees",
        "disciplinary_flag",
    }
)


def _canonical(value: object) -> str:
    """Render one value order-invariantly and type-stably."""
    if value is None:
        return "\x00"
    if isinstance(value, (tuple, list, set, frozenset)):
        # Sort AND dedupe: the publisher shuffles these, and has been observed
        # emitting the same child twice across snapshots.
        return "[" + ",".join(sorted({_canonical(v) for v in value})) + "]"
    return str(value)


def canonical_form(rec: FirmRecord) -> str:
    """A stable, order-invariant string for the meaningful content of a record."""
    known = {f.name for f in dataclass_fields(rec)}
    missing = [name for name in CHANGE_FIELDS if name not in known]
    if missing:
        # Fail loud: a renamed field would otherwise silently stop being watched.
        raise KeyError(f"CHANGE_FIELDS names fields absent from FirmRecord: {missing}")
    return "|".join(f"{name}={_canonical(getattr(rec, name))}" for name in CHANGE_FIELDS)


def change_key(rec: FirmRecord) -> str:
    """Content-derived, order-invariant change key. 32 hex chars."""
    return hashlib.sha256(canonical_form(rec).encode("utf-8")).hexdigest()[:32]


def diff_fields(old: FirmRecord, new: FirmRecord) -> dict[str, tuple[object, object]]:
    """Which watched fields actually moved, order-invariantly.

    Returns ``{field: (old_value, new_value)}``. Empty dict means the record is
    unchanged in every way we care about, however much its bytes shuffled.
    """
    moved: dict[str, tuple[object, object]] = {}
    for name in CHANGE_FIELDS:
        a, b = getattr(old, name), getattr(new, name)
        if _canonical(a) != _canonical(b):
            moved[name] = (a, b)
    return moved


def should_enrich(moved: dict[str, tuple[object, object]]) -> bool:
    """Does this movement justify pulling the firm's full ADV document?

    Narrow by design. The steady-state answer is ~66 firms/day; a wider trigger
    set would multiply that without adding succession signal.
    """
    return bool(ENRICH_TRIGGER_FIELDS & moved.keys())
