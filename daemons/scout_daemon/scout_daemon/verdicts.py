"""Per-scan verdict history and the effective-verdict derivation (doctrine E22).

WHY THIS TABLE EXISTS. `opportunities` holds ONE row per opportunity and the
write path is `UPDATE ... WHERE opportunity_id=?`, so every re-scan overwrites
the previous verdict; only `opportunity_id`, `first_seen_unix` and `status`
survive. A debounce is derived state over verdict HISTORY, and that history did
not exist. `opportunity_verdicts` is append-only: one row per
(opportunity_id, scan_id), never updated, never deleted. `opportunities` keeps
only the DERIVED effective verdict, so the raw record and the judgment made
from it are separately inspectable.

THE DEBOUNCE, AND THE ARITHMETIC THAT JUSTIFIES IT
--------------------------------------------------
Measured 2026-08-15: re-running classification over a 21-minute gap -- during
which listings cannot meaningfully change -- flipped 4 of 157 mechanical-GREEN
rows, every one with byte-identical stored inputs. The judge is stochastic at
P = 2.55% per row per scan (95% Wilson [1.00%, 6.37%]).

Applying that floor to 20,000 rows x 400 scans, the two error rates are NOT
symmetric across debounce designs:

    design                  standing false-VETO    standing false-GREEN
    no debounce  (1,1)            2.551%                 2.5499%
    THIS ONE     (1,2)            5.017%                 0.0658%
    symmetric    (2,2)            0.128%                 0.1316%
    inverted     (2,1)            0.065%                 5.0118%

This module implements (1,2): a veto takes effect on ONE observation, recovery
to effective-GREEN needs the TWO most recent scans both clean.

Read the table before assuming that is a mistake. Against no debounce, (1,2)
makes false vetoes 1.97x MORE common and false GREENs 39x LESS common. That is
the intended trade and it follows scout's cost asymmetry directly: an
over-classification costs Mando a review, an under-classification costs the
tribe its record. The expensive error is the false GREEN, so the design spends
false vetoes to buy them down.

The 0.06% figure that motivated this rule is real but belongs to the
false-GREEN column. Attaching it to false-VETO -- where the true value is
5.017%, worse than no debounce at all -- inverts the argument. If the false-VETO
rate is ever the thing to minimise, the design that delivers 0.065% there is
(2,1), which reverses the asymmetry and makes false GREENs 39x more likely.

PERSONA VETOES ARE EXEMPT. They are permanent and downward-only by Mando's
2026-08-11 ruling: once the LLM judges that a task presumes a human, no number
of subsequent clean scans clears it. Debouncing a permanent gate would mean two
lucky scans could unlock something ruled unlockable, so the exemption is a
correctness requirement rather than a convenience.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .classify import is_persona_veto

# Consecutive clean scans required to return an ordinary veto to effective-GREEN.
# Raising this lowers false-GREEN and raises false-VETO, along the table above.
RECOVERY_SCANS = 2

# Observations required for a veto to take effect. Deliberately 1: downward
# stays fast. Pinned as a named constant so the asymmetry is legible rather
# than implied by an `if`.
VETO_SCANS = 1

VERDICT_SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunity_verdicts (
    opportunity_id    TEXT NOT NULL,
    scan_id           TEXT NOT NULL,
    observed_unix     INTEGER NOT NULL,
    mechanical_class  TEXT NOT NULL,
    legitimacy_class  TEXT,
    classes_disagreed INTEGER NOT NULL DEFAULT 0,
    class_reason      TEXT,
    is_persona_veto   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (opportunity_id, scan_id)
);
CREATE INDEX IF NOT EXISTS idx_verdicts_opportunity
    ON opportunity_verdicts(opportunity_id, observed_unix);
"""


@dataclass(frozen=True)
class EffectiveVerdict:
    """Derived state. `vetoed` is what rank and surfacing must consume."""

    vetoed: bool
    persona_locked: bool
    scans_seen: int
    flip_count: int
    clean_run: int
    raw_latest_vetoed: bool

    @property
    def state(self) -> str:
        return "VETOED" if self.vetoed else "GREEN"

    @property
    def debounce_held(self) -> bool:
        """True when the derived state DIFFERS from the latest raw verdict.

        This is the flag that makes the debounce auditable: it marks every row
        where the effective answer is not simply the newest one.
        """
        return self.vetoed != self.raw_latest_vetoed


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(VERDICT_SCHEMA)
    conn.commit()


def record_verdict(
    conn: sqlite3.Connection,
    *,
    opportunity_id: str,
    scan_id: str,
    observed_unix: int,
    mechanical_class: str,
    legitimacy_class: str | None,
    classes_disagreed: bool,
    class_reason: str | None,
) -> None:
    """Append one observation. INSERT OR IGNORE, never UPDATE.

    Re-running a scan_id is idempotent and cannot rewrite what was observed --
    the table is the raw record, and a record that can be edited is not one.
    """
    conn.execute(
        "INSERT OR IGNORE INTO opportunity_verdicts"
        "(opportunity_id, scan_id, observed_unix, mechanical_class,"
        " legitimacy_class, classes_disagreed, class_reason, is_persona_veto)"
        " VALUES(?,?,?,?,?,?,?,?)",
        (
            opportunity_id,
            scan_id,
            observed_unix,
            mechanical_class,
            legitimacy_class,
            1 if classes_disagreed else 0,
            class_reason,
            1 if (classes_disagreed and is_persona_veto(class_reason)) else 0,
        ),
    )


def derive(observations: list[tuple[int, bool, bool]]) -> EffectiveVerdict:
    """Fold verdict history into the effective verdict.

    `observations` is (observed_unix, classes_disagreed, is_persona_veto),
    OLDEST FIRST. Pure function of the history -- no clock, no config -- so the
    same history always derives the same answer and the fold is testable
    without a database.
    """
    if not observations:
        return EffectiveVerdict(False, False, 0, 0, 0, False)

    vetoed: bool | None = None
    persona_locked = False
    clean_run = 0
    flips = 0
    latest_raw = False

    for _, disagreed, persona in observations:
        latest_raw = disagreed
        if disagreed and persona:
            persona_locked = True

        if disagreed:
            clean_run = 0
            new = True
        else:
            clean_run += 1
            if vetoed is None:
                # First observation is clean: the row enters at GREEN rather
                # than serving a recovery period it never earned.
                new = False
            elif vetoed and clean_run >= RECOVERY_SCANS:
                new = False
            else:
                new = vetoed

        # Persona is applied last and only ever downward, so no clean run can
        # unlock it.
        if persona_locked:
            new = True

        if vetoed is not None and new != vetoed:
            flips += 1
        vetoed = new

    return EffectiveVerdict(
        vetoed=bool(vetoed),
        persona_locked=persona_locked,
        scans_seen=len(observations),
        flip_count=flips,
        clean_run=clean_run,
        raw_latest_vetoed=latest_raw,
    )


def judgeless_scans(conn: sqlite3.Connection) -> frozenset[str]:
    """Scans that queued items to the judge and never reached it.

    A transport failure degrades the batch to YELLOW-with-the-reason, which is
    the safe direction -- but it still writes one verdict row per item carrying
    `classes_disagreed = 0`. That zero means "no veto came back", NOT "the judge
    looked and cleared it". Replayed as history those rows are SPURIOUS CLEAN
    VOTES, and because recovery needs two consecutive clean scans they push rows
    toward GREEN, which is the expensive direction under scout's cost asymmetry.

    Measured 2026-08-21 before this fix: three such scans had written 1,609 of
    6,782 observations (23.7%), and 16 of 669 rows carried a different effective
    verdict because of them.

    Detected from COST TELEMETRY rather than a flag on the verdict row, because
    telemetry is persisted before any opportunity row and is therefore
    trustworthy even when the rest of the scan went wrong. `llm_calls = 0` with
    `items_classified > 0` reads exactly as "there was work for the judge and no
    call was made".
    """
    try:
        rows = conn.execute(
            "SELECT scan_id FROM scan_cost"
            " WHERE llm_calls = 0 AND items_classified > 0"
        ).fetchall()
    except sqlite3.OperationalError:
        # No cost telemetry available. Nothing is excludable, and saying so is
        # honest -- guessing which scans were judgeless would be worse.
        return frozenset()
    return frozenset(r[0] for r in rows)


def _observations(
    conn: sqlite3.Connection,
    *,
    opportunity_id: str | None = None,
    include_judgeless: bool = False,
) -> dict[str, list[tuple[int, bool, bool]]]:
    """Verdict history with judgeless scans dropped by default.

    `include_judgeless=True` exists only for forensics -- reproducing what a
    derived state looked like before this fix. It must not be used for a live
    read.
    """
    excluded = frozenset() if include_judgeless else judgeless_scans(conn)
    sql = (
        "SELECT opportunity_id, observed_unix, classes_disagreed,"
        " is_persona_veto, scan_id FROM opportunity_verdicts"
    )
    params: tuple = ()
    if opportunity_id is not None:
        sql += " WHERE opportunity_id = ?"
        params = (opportunity_id,)
    sql += " ORDER BY observed_unix, scan_id"

    grouped: dict[str, list[tuple[int, bool, bool]]] = {}
    for row in conn.execute(sql, params):
        if row[4] in excluded:
            continue
        grouped.setdefault(row[0], []).append((row[1], bool(row[2]), bool(row[3])))
    return grouped


def effective_for(
    conn: sqlite3.Connection,
    opportunity_id: str,
    *,
    include_judgeless: bool = False,
) -> EffectiveVerdict:
    grouped = _observations(
        conn, opportunity_id=opportunity_id, include_judgeless=include_judgeless
    )
    return derive(grouped.get(opportunity_id, []))


def effective_all(
    conn: sqlite3.Connection, *, include_judgeless: bool = False
) -> dict[str, EffectiveVerdict]:
    """Every opportunity's effective verdict, one pass over the history."""
    grouped = _observations(conn, include_judgeless=include_judgeless)
    return {oid: derive(obs) for oid, obs in grouped.items()}


__all__ = [
    "RECOVERY_SCANS",
    "VETO_SCANS",
    "VERDICT_SCHEMA",
    "EffectiveVerdict",
    "apply_schema",
    "record_verdict",
    "derive",
    "judgeless_scans",
    "effective_for",
    "effective_all",
]
