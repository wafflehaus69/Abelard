"""PS-1 — the human correction path.

A correction is the only way a value in this store changes, and it is by
construction a **human act**: authored by a person, carrying a reason and
citable evidence. Nothing in a fetch path may write here.

Two kinds, and the distinction is not cosmetic:

* ``corrected`` — the held value is wrong and a named source says what is right.
* ``confirmed`` — the held value is RIGHT, and an independent source has been
  checked. ``corrected_close`` equals what is already held.

Confirmations matter because quarantine is a statement of *ignorance*, not of
error: the detector could not adjudicate the window, so it refused to call any
of it fact. Most sessions inside a quarantined span are usually fine. A
confirmation is how they are released, and recording it means the next reader
can see the window was adjudicated rather than merely aged out.

**Dry run first.** ``plan()`` reports exactly what would change and why;
``apply()`` writes. The two are separate calls so the diff can be read before
anything lands (E9, dry-run-and-diff).
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from .schema import PriceStoreError

VALID_KINDS = ("corrected", "confirmed")


class CorrectionError(PriceStoreError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="corrections")


@dataclass
class PlannedRow:
    instrument_id: str
    ticker: str
    date: str
    held: float | None
    held_status: str | None
    corrected_close: float
    kind: str
    reason: str
    evidence: dict
    note: str = ""

    @property
    def changes_value(self) -> bool:
        """Does this row actually move the number?

        Deliberately looser than ``reconstruct.FACT_EPS`` (1e-9), and the reason
        matters. FACT_EPS compares a value against ITSELF across two fetches of
        the same vendor, where equality is exact. This compares ACROSS vendors:
        Yahoo serves float32-precision closes widened to float64
        (``94.16000366210938``) while Tiingo quotes to the cent (``94.16``). At
        1e-9 every genuine confirmation reads as a change — caught by this
        module's own dry run before anything was written.

        1e-6 relative is a hundredth of a cent on a $100 share: far tighter than
        any real price difference, far looser than float noise.
        """
        if self.held is None:
            return True
        if self.held == 0:
            return self.corrected_close != 0
        return abs(self.corrected_close / self.held - 1.0) > 1e-6


@dataclass
class Plan:
    authored_by: str
    source: str
    rows: list[PlannedRow] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    def render(self) -> str:
        out = ["correction plan — authored_by={} source={}".format(
            self.authored_by, self.source)]
        out.append("  {:<7} {:<12} {:>10} {:>10}  {:<9} {}".format(
            "ticker", "date", "held", "becomes", "kind", "held status"))
        for r in self.rows:
            held = "(null)" if r.held is None else "{:.2f}".format(r.held)
            out.append("  {:<7} {:<12} {:>10} {:>10}  {:<9} {}{}".format(
                r.ticker, r.date, held, "{:.2f}".format(r.corrected_close),
                r.kind, r.held_status or "-",
                "" if r.changes_value else "   (value unchanged)"))
        changed = sum(1 for r in self.rows if r.changes_value)
        out.append("  {} rows: {} change a value, {} confirm one".format(
            len(self.rows), changed, len(self.rows) - changed))
        for p in self.problems:
            out.append("  PROBLEM {}".format(p))
        return "\n".join(out)

    @property
    def ok(self) -> bool:
        return not self.problems and bool(self.rows)


def load(path: str | Path) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    for key in ("authored_by", "source", "rows"):
        if key not in payload:
            raise CorrectionError("staging file missing {!r}".format(key))
    if not str(payload["authored_by"]).strip():
        raise CorrectionError(
            "authored_by is empty — a correction is a human act and must name "
            "the human making it")
    return payload


def plan(con: sqlite3.Connection, payload: dict) -> Plan:
    """Resolve a staging payload against the store. Writes nothing."""
    p = Plan(authored_by=str(payload["authored_by"]), source=str(payload["source"]))
    for raw in payload["rows"]:
        ticker = raw.get("ticker")
        iid = raw.get("instrument_id")
        if not iid and ticker:
            row = con.execute(
                "SELECT instrument_id FROM instruments WHERE primary_ticker=?",
                (ticker,)).fetchone()
            iid = row[0] if row else None
        if not iid:
            p.problems.append("cannot resolve instrument for {!r}".format(raw))
            continue
        kind = raw.get("kind", "corrected")
        if kind not in VALID_KINDS:
            p.problems.append("{} {}: kind must be one of {}".format(
                ticker, raw.get("date"), VALID_KINDS))
            continue
        if not str(raw.get("reason", "")).strip():
            p.problems.append("{} {}: a correction must carry a reason".format(
                ticker, raw.get("date")))
            continue
        held = con.execute(
            "SELECT close, status FROM prices_raw WHERE instrument_id=? AND date=?",
            (iid, raw["date"])).fetchone()
        p.rows.append(PlannedRow(
            instrument_id=iid, ticker=ticker or iid, date=raw["date"],
            held=held["close"] if held else None,
            held_status=held["status"] if held else None,
            corrected_close=float(raw["corrected_close"]),
            kind=kind, reason=raw["reason"], evidence=raw.get("evidence") or {},
        ))
    # A 'confirmed' row that actually moves the number is a mislabelled
    # correction, and the label is what a later reader will trust.
    for r in p.rows:
        if r.kind == "confirmed" and r.changes_value:
            p.problems.append(
                "{} {}: labelled 'confirmed' but changes {} -> {}".format(
                    r.ticker, r.date, r.held, r.corrected_close))
    return p


def apply(con: sqlite3.Connection, p: Plan, authored_at: int | None = None) -> int:
    """Write the plan. Append-only: a later revision is a new row, never an edit."""
    if not p.ok:
        raise CorrectionError(
            "refusing to apply a plan with problems:\n  " + "\n  ".join(p.problems))
    ts = authored_at or int(time.time())
    n = 0
    for r in p.rows:
        con.execute(
            "INSERT INTO corrections (instrument_id, date, corrected_close,"
            " supersedes, reason, authored_by, authored_at, evidence)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (r.instrument_id, r.date, r.corrected_close, r.held,
             "[{}] {}".format(r.kind, r.reason), p.authored_by, ts + n,
             json.dumps({**r.evidence, "kind": r.kind, "source": p.source})),
        )
        n += 1
    con.commit()
    return n


def affected_instruments(p: Plan) -> list[str]:
    return sorted({r.instrument_id for r in p.rows})
