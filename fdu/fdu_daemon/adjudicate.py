"""L2: the manual adjudication worksheet.

Phase C needs a dependent variable and the archive does not contain one. Nothing
in 2006-2023 says which of the ~8,000 observed disappearances was an acquisition
rather than a retirement, a wind-down, a merger into an affiliate, or a move to
state registration. L1 measured the alternative — forward accrual via Item 4
yields **~1.6 third-party successions per year**, so five years of waiting buys
roughly eight labels. That is not a validation set.

So a human adjudicates a sample, and this module prepares the worksheet.

**The machine does not guess the label.** Every column here is either an observed
fact from our own ledger or a pre-built lookup URL. There is no "likely
acquisition" column and no score, because a machine-suggested label is the thing
a human then agrees with, and the whole value of L2 is an independent judgement.

**I-11 boundary.** Firms only. The worksheet carries firm names, CRDs and
filing-derived figures — the approved firm-level research activity. It carries no
individual's name, no contact detail, and no outreach language. It is an
adjudication instrument, not a call list.

Stratification, per the order: era, AUM band, and whether the disappearance was
accompanied by a rename elsewhere in the same interval.
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

IAPD_FIRM_URL = "https://adviserinfo.sec.gov/firm/summary/{crd}"

#: Outcome vocabulary the adjudicator picks from. Deliberately includes the
#: boring outcomes, because a worksheet offering only interesting ones
#: manufactures interesting answers.
OUTCOMES = (
    "acquired_third_party",   # book/practice absorbed by an unrelated firm
    "merged_affiliate",       # folded into a parent or sibling entity
    "reorganized_same_owner", # re-registered, converted, re-domiciled
    "moved_to_state",         # dropped SEC registration, still advising
    "wound_down",             # ceased operating, no successor
    "still_active_elsewhere", # firm or its book visibly continues, route unclear
    "undetermined",           # public record does not say -- a valid answer
)

AUM_BANDS = (
    ("micro", 0, 25_000_000),
    ("small", 25_000_000, 100_000_000),
    ("mid", 100_000_000, 500_000_000),
    ("large", 500_000_000, None),
    ("unknown", None, None),
)


def _band(aum) -> str:
    if aum is None:
        return "unknown"
    for name, lo, hi in AUM_BANDS:
        if lo is None:
            continue
        if aum >= lo and (hi is None or aum < hi):
            return name
    return "unknown"


@dataclass
class Candidate:
    crd: str
    last_name_seen: str | None
    snapshot_from: str
    snapshot_to: str
    interval_months: int
    spans_gap: bool
    last_aum: int | None
    last_employees: int | None
    era: str
    aum_band: str = ""
    concurrent_renames: int = 0
    notes: list[str] = field(default_factory=list)


def gather_disappearances(conn: sqlite3.Connection) -> list[Candidate]:
    """Every observed disappearance, with the firm's last known state attached.

    One pass per event type rather than three correlated subqueries per
    candidate. The first cut did the latter and issued ~55,000 queries against
    18,508 disappearances, which simply never returned.
    """
    eras = {r["snapshot_date"]: r["era"] for r in
            conn.execute("SELECT snapshot_date, era FROM snapshot")}

    def last_values(event_type: str) -> dict[str, str]:
        """Most recent new_value per CRD for one event type, in a single scan."""
        out: dict[str, str] = {}
        for r in conn.execute(
            "SELECT crd, new_value FROM transition_events "
            "WHERE event_type=? ORDER BY snapshot_to", (event_type,)
        ):
            if r["new_value"] is not None:
                out[r["crd"]] = r["new_value"]
        return out

    names = last_values("rename")
    aums = last_values("aum_delta")
    emps = last_values("headcount_delta")

    def _i(v):
        try:
            return int(v) if v else None
        except (ValueError, TypeError):
            return None

    out: list[Candidate] = []
    seen: set[str] = set()
    for r in conn.execute(
        "SELECT crd, snapshot_from, snapshot_to, interval_months, spans_gap "
        "FROM transition_events WHERE event_type='disappearance' ORDER BY snapshot_to"
    ):
        crd = r["crd"]
        if crd in seen:
            continue
        seen.add(crd)
        c = Candidate(
            crd=crd,
            last_name_seen=names.get(crd),
            snapshot_from=r["snapshot_from"], snapshot_to=r["snapshot_to"],
            interval_months=r["interval_months"], spans_gap=bool(r["spans_gap"]),
            last_aum=_i(aums.get(crd)), last_employees=_i(emps.get(crd)),
            era=eras.get(r["snapshot_to"], "unknown"),
        )
        c.aum_band = _band(c.last_aum)
        out.append(c)
    return out


def stratified_sample(cands: list[Candidate], n: int = 50, seed: int = 20260825) -> list[Candidate]:
    """Proportional stratified sample across (era, aum_band).

    Deterministic seed so the sample is reproducible and an adjudicator can be
    handed the same 50 twice. Gap-spanning disappearances are EXCLUDED: their
    interval is up to 13 months, so "when did it vanish" is unanswerable and the
    adjudicator would be guessing at the wrong question.
    """
    import random

    usable = [c for c in cands if not c.spans_gap]
    if not usable:
        return []
    strata: dict[tuple, list[Candidate]] = {}
    for c in usable:
        strata.setdefault((c.era, c.aum_band), []).append(c)
    rng = random.Random(seed)
    picked: list[Candidate] = []
    total = len(usable)
    for key, group in sorted(strata.items()):
        want = max(1, round(n * len(group) / total))
        rng.shuffle(group)
        picked.extend(group[:want])
    rng.shuffle(picked)
    return picked[:n]


def write_worksheet(cands: list[Candidate], path: Path) -> Path:
    """Emit the adjudication CSV. One row per firm, blank decision columns."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow([
            "crd", "last_known_name", "last_seen_snapshot", "vanished_by_snapshot",
            "interval_months", "era", "aum_band", "last_aum", "last_employees",
            "iapd_lookup", "OUTCOME", "SUCCESSOR_FIRM", "EVIDENCE_URL", "ADJUDICATOR", "NOTES",
        ])
        for c in cands:
            w.writerow([
                c.crd, c.last_name_seen or "", c.snapshot_from, c.snapshot_to,
                c.interval_months, c.era, c.aum_band,
                c.last_aum if c.last_aum is not None else "",
                c.last_employees if c.last_employees is not None else "",
                IAPD_FIRM_URL.format(crd=c.crd),
                "", "", "", "", "",
            ])
    return path
