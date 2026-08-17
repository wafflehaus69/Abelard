"""Admission: the human gate (invariant 2).

THIS MODULE IS THE ONLY PATH TO `status='admitted'`, AND IT CANNOT WRITE THE
FILE IT READS. `config/admissions.yaml` is MANDO-OWNED. The daemon reads it and
never writes it -- byte-for-byte the `overlay.yaml` discipline
(`smart_money/overlay.py:1-2`).

That asymmetry is the whole mechanism. Scout can discover, classify, score,
rank and surface; it can move a row as far as `proposed`. Only a human editing
a file the daemon has no writer for can move it to `admitted`. Invariant 2 is
therefore enforced by the shape of the code, not by remembering a rule: there is
no function here that opens the YAML for writing, and a test asserts it.

WHY A FILE AND NOT A CLI VERB. A `scout-daemon admit <key>` verb would put the
admission decision inside the daemon's own process, one argument away from being
called by the daemon itself. A file Mando edits keeps the decision physically
outside the thing being decided about.

STATUS LADDER
    discovered  -- ingested, classified, ranked. The default.
    proposed    -- surfaced to Abelard's queue. The daemon MAY set this.
    admitted    -- cleared to act on. ONLY this module, ONLY from the file.
    dismissed   -- judged not worth pursuing. Also only from the file.

UNKNOWN KEYS ARE REPORTED, NEVER SWALLOWED (E1). A key in the file matching no
ledger row is a typo, a delisted item, or a re-scanned identity -- all things
Mando needs told. Silently ignoring it would let an admission he believes he
made simply not exist.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from . import config

# Mando-owned. Never opened for writing anywhere in this package.
DEFAULT_ADMISSIONS_PATH = Path(__file__).resolve().parent.parent / "config" / "admissions.yaml"

STATUS_DISCOVERED = "discovered"
STATUS_PROPOSED = "proposed"
STATUS_ADMITTED = "admitted"
STATUS_DISMISSED = "dismissed"

# The daemon may move a row INTO these on its own. `admitted` is deliberately
# absent, and `resolve()` refuses to write anything outside this set plus the
# two file-driven terminals.
DAEMON_SETTABLE = frozenset({STATUS_DISCOVERED, STATUS_PROPOSED})


@dataclass
class Admissions:
    """Parsed, validated contents of the Mando-owned file."""

    admitted: frozenset[str] = frozenset()
    dismissed: frozenset[str] = frozenset()
    category_rules: dict[str, str] = field(default_factory=dict)
    path: Path | None = None
    present: bool = False

    @property
    def is_empty(self) -> bool:
        return not (self.admitted or self.dismissed or self.category_rules)


@dataclass
class AdmissionOutcome:
    admitted: int = 0
    dismissed: int = 0
    by_category: int = 0
    unknown_keys: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)


def load(path: Path | None = None) -> Admissions:
    """Read the admissions file. Absent file is a valid state, not an error.

    A missing file means nothing has been admitted yet -- which is exactly true
    on a fresh install and must not read as a failure.
    """
    p = Path(path) if path else DEFAULT_ADMISSIONS_PATH
    if not p.exists():
        return Admissions(path=p, present=False)
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"admissions file must be a mapping, got {type(raw).__name__}")

    def _keyset(name: str) -> frozenset[str]:
        val = raw.get(name) or []
        if not isinstance(val, list):
            raise ValueError(f"admissions.{name} must be a list, got {type(val).__name__}")
        return frozenset(str(v).strip() for v in val if str(v).strip())

    rules = raw.get("category_rules") or {}
    if not isinstance(rules, dict):
        raise ValueError("admissions.category_rules must be a mapping")

    return Admissions(
        admitted=_keyset("admitted"),
        dismissed=_keyset("dismissed"),
        category_rules={str(k): str(v) for k, v in rules.items()},
        path=p,
        present=True,
    )


# Length of the pasteable short key. Verified unique across all 585 ledger
# rows at 8 chars; 12 is used for headroom, and `apply()` refuses an ambiguous
# prefix rather than picking one.
SHORT_ID_LEN = 12


def short_id(opportunity_id: str) -> str:
    return opportunity_id[:SHORT_ID_LEN]


def identity_keys(source: str, native_id: str, opportunity_id: str) -> set[str]:
    """Every string that may legitimately name this row in the file.

    Three spellings, because none alone is both stable and usable:

      * `opportunity_id` -- exact, 64 hex chars, unusable by hand.
      * `source:native_id` -- readable for most sources, but NOT for all:
        measured 2026-08-16, zindi native ids run to 124 characters and
        yeswehack to 90, because those adapters use the listing title as the
        native id. A key with spaces, commas and colons cannot be pasted into
        a YAML list without quoting games.
      * `short_id` -- first 12 hex chars. This is the one to paste, and the
        one `scout-daemon proposals` prints first.
    """
    keys = {opportunity_id, short_id(opportunity_id)}
    if native_id and len(f"{source}:{native_id}") <= 60 and "\n" not in native_id:
        keys.add(f"{source}:{native_id}")
    return keys


def apply(conn, admissions: Admissions, *, now_unix: int) -> AdmissionOutcome:
    """Apply the file to the ledger. The ONLY writer of `admitted`/`dismissed`.

    Returns what changed and what could not be matched. Never raises on an
    unknown key -- it reports it, because a half-applied admission set that
    aborts mid-way is worse than a complete one with a named gap.
    """
    outcome = AdmissionOutcome()
    if not admissions.present or admissions.is_empty:
        return outcome

    rows = conn.execute(
        "SELECT opportunity_id, source, source_native_id, category, status"
        " FROM opportunities"
    ).fetchall()

    # key -> opportunity_id, for both accepted spellings
    index: dict[str, str] = {}
    for r in rows:
        for k in identity_keys(r["source"], r["source_native_id"], r["opportunity_id"]):
            index[k] = r["opportunity_id"]

    both = admissions.admitted & admissions.dismissed
    for key in sorted(both):
        # Refuse rather than pick. A key in both lists is a contradiction only
        # Mando can resolve, and guessing would silently enact one of his two
        # stated intentions.
        outcome.conflicts.append(key)

    def _set(oid: str, status: str) -> None:
        conn.execute(
            "UPDATE opportunities SET status=?, admission_applied_unix=?"
            " WHERE opportunity_id=?",
            (status, now_unix, oid),
        )

    def _resolve(key: str) -> str | None:
        """Exact match, else a UNIQUE opportunity_id prefix. Ambiguity refuses."""
        if key in index:
            return index[key]
        hits = {oid for k, oid in index.items() if k.startswith(key) and len(key) >= 8}
        if len(hits) == 1:
            return next(iter(hits))
        if len(hits) > 1:
            outcome.conflicts.append(f"{key!r} is ambiguous -- matches {len(hits)} rows")
        return None

    for key in sorted(admissions.admitted - both):
        oid = _resolve(key)
        if oid is None:
            if not any(key in c for c in outcome.conflicts):
                outcome.unknown_keys.append(key)
            continue
        _set(oid, STATUS_ADMITTED)
        outcome.admitted += 1

    for key in sorted(admissions.dismissed - both):
        oid = _resolve(key)
        if oid is None:
            if not any(key in c for c in outcome.conflicts):
                outcome.unknown_keys.append(key)
            continue
        _set(oid, STATUS_DISMISSED)
        outcome.dismissed += 1

    # Standing category rulings. Applied AFTER explicit keys so that a per-item
    # decision always beats a blanket one -- the specific overrides the general,
    # never the reverse.
    for category, verdict in sorted(admissions.category_rules.items()):
        status = {"admit": STATUS_ADMITTED, "dismiss": STATUS_DISMISSED}.get(str(verdict).lower())
        if status is None:
            outcome.conflicts.append(f"category_rules[{category}]={verdict!r} (expected admit|dismiss)")
            continue
        explicit = {index.get(k) for k in (admissions.admitted | admissions.dismissed)}
        for r in rows:
            if r["category"] != category or r["opportunity_id"] in explicit:
                continue
            _set(r["opportunity_id"], status)
            outcome.by_category += 1

    conn.commit()
    return outcome


def propose(conn, opportunity_ids: list[str], *, now_unix: int) -> int:
    """Mark rows as surfaced. The furthest the daemon may move a row itself.

    Deliberately refuses to touch a row that is already `admitted` or
    `dismissed`: a human decision is terminal, and re-surfacing must not quietly
    reopen something Mando has closed.
    """
    moved = 0
    for oid in opportunity_ids:
        cur = conn.execute(
            "UPDATE opportunities SET status=?, proposed_unix=?"
            " WHERE opportunity_id=? AND status IN (?,?)",
            (STATUS_PROPOSED, now_unix, oid, STATUS_DISCOVERED, STATUS_PROPOSED),
        )
        moved += cur.rowcount
    conn.commit()
    return moved


TEMPLATE = """\
# Admissions -- MANDO-OWNED. The daemon reads this file and never writes it.
#
# Keys may be either the opportunity_id hash or the human-writable form
# `source:native_id` (e.g. `opire:12345`). Run `scout-daemon proposals` to see
# both spellings for anything awaiting a decision.
#
# A key listed in BOTH admitted and dismissed is refused, not guessed.
# An explicit key always beats a category_rule.

admitted: []      # cleared to act on
dismissed: []     # judged not worth pursuing
category_rules: {}  # standing rulings, e.g.  bug_bounty: dismiss
"""


__all__ = [
    "DEFAULT_ADMISSIONS_PATH",
    "STATUS_DISCOVERED", "STATUS_PROPOSED", "STATUS_ADMITTED", "STATUS_DISMISSED",
    "DAEMON_SETTABLE", "TEMPLATE",
    "Admissions", "AdmissionOutcome",
    "load", "apply", "propose", "identity_keys",
]
