"""B2: the CRD-keyed diff engine and its event vocabulary.

One vocabulary for archive-derived and live-observed events, with a provenance
column separating them, so the historical record and the live change log are one
queryable surface.

**Language discipline [I-13].** Every name here describes what was OBSERVED in a
filing, never what it means commercially. A firm that leaves the roster is a
``disappearance``, not an acquisition. A firm whose legal name changes is a
``rename``. The unit currently has **zero verified genuine acquisitions**, and no
event type in this module asserts one. The word is earned per-event by label
evidence, which the archive does not contain — see the Phase A report §6.

The gap handling matters as much as the diff. The archive is missing 49 of 243
months, clustered in 2006-07 and 2023-25. Diffing across a hole silently turns
an N-month interval into a one-step transition and inflates every rate computed
from it, so every event carries its actual interval in months and a flag when it
spans a gap.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import lineage

#: The closed event vocabulary. Anything not in here is not an event we know how
#: to interpret, and a test asserts nothing outside it is ever written.
EVENT_TYPES: frozenset[str] = frozenset(
    {
        "appearance",       # CRD present in this snapshot, absent in the prior one
        "disappearance",    # CRD absent here, present in the prior one
        "rename",           # legal_name changed while the CRD persisted
        "status_change",    # sec_status changed
        "aum_delta",        # aum_total changed
        "headcount_delta",  # total_employees changed
    }
)

#: Provenance values. Archive-derived events are reconstructed from published
#: monthly snapshots; live events come from the daily feed's change log.
PROVENANCE_ARCHIVE = "archive"
PROVENANCE_LIVE = "live"

_FIELD_EVENT = {
    "legal_name": "rename",
    "sec_status": "status_change",
    "aum_total": "aum_delta",
    "total_employees": "headcount_delta",
}


@dataclass(frozen=True)
class TransitionEvent:
    crd: str
    event_type: str
    field: str | None
    old_value: str | None
    new_value: str | None
    snapshot_from: str
    snapshot_to: str
    interval_months: int
    spans_gap: bool
    source_file: str
    provenance: str = PROVENANCE_ARCHIVE

    def as_row(self) -> dict:
        return {
            "crd": self.crd,
            "event_type": self.event_type,
            "field": self.field,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "snapshot_from": self.snapshot_from,
            "snapshot_to": self.snapshot_to,
            "interval_months": self.interval_months,
            "spans_gap": int(self.spans_gap),
            "source_file": self.source_file,
            "provenance": self.provenance,
        }


def _months_between(a: str, b: str) -> int:
    ya, ma = int(a[:4]), int(a[5:7])
    yb, mb = int(b[:4]), int(b[5:7])
    return (yb - ya) * 12 + (mb - ma)


def _norm(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def diff_snapshots(prev, curr, *, expected_interval: int = 1) -> list[TransitionEvent]:
    """Diff two parsed snapshots into transition events.

    ``expected_interval`` is the cadence the archive is supposed to have; an
    actual interval larger than it means the pair spans a coverage hole, and
    every event from that pair is flagged rather than quietly counted as a
    one-month move.
    """
    interval = _months_between(prev.snapshot_date, curr.snapshot_date)
    spans_gap = interval > expected_interval

    def ev(crd, etype, field=None, old=None, new=None) -> TransitionEvent:
        return TransitionEvent(
            crd=crd, event_type=etype, field=field,
            old_value=_norm(old), new_value=_norm(new),
            snapshot_from=prev.snapshot_date, snapshot_to=curr.snapshot_date,
            interval_months=interval, spans_gap=spans_gap,
            source_file=curr.source_file,
        )

    events: list[TransitionEvent] = []
    prev_ids, curr_ids = set(prev.rows), set(curr.rows)

    for crd in sorted(prev_ids - curr_ids):
        events.append(ev(crd, "disappearance", "crd", crd, None))
    for crd in sorted(curr_ids - prev_ids):
        events.append(ev(crd, "appearance", "crd", None, crd))

    # Only compare fields BOTH eras actually carry. A field absent from one side
    # would otherwise read as a change from a value to nothing, manufacturing a
    # transition out of a schema difference.
    comparable = [
        f for f in lineage.TRACKED_FIELDS
        if f not in prev.absent_fields and f not in curr.absent_fields
    ]
    for crd in sorted(prev_ids & curr_ids):
        a, b = prev.rows[crd], curr.rows[crd]
        for field in comparable:
            va, vb = _norm(a.get(field)), _norm(b.get(field))
            if va == vb or va is None or vb is None:
                continue
            events.append(ev(crd, _FIELD_EVENT[field], field, va, vb))
    return events


def summarize(events: list[TransitionEvent]) -> dict:
    """Counts by event type, and how many are gap-spanning."""
    out: dict[str, int] = {}
    for e in events:
        out[e.event_type] = out.get(e.event_type, 0) + 1
    return {
        "total": len(events),
        "by_type": dict(sorted(out.items())),
        "gap_spanning": sum(1 for e in events if e.spans_gap),
    }
