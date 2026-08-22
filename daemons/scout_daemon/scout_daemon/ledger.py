"""The opportunities ledger.

DISTRIBUTION-FIRST. Every column is populatable from data observed across 527
live items. The SC-R1 T4 proposal is the base; predicted-but-unfillable columns
were dropped, and columns Phase 1 discovered were added.

INVARIANT 1 IS ENFORCED IN SQL. There is no DELETE path in this module and no
filter that discards a row. A RED item is stored with its reason and stays
visible. The classifier assigns; it never drops.

ADMISSION IS HUMAN. `status` starts at 'discovered'. Nothing in this daemon
writes 'admitted' or 'dismissed'; the absence of a code path is the
enforcement, and a behavioural test asserts it.

GREEN vs GREEN_PROMOTED ARE DISTINCT CLASSES, NOT A FLAG ON ONE CLASS.
A promoted item never carries `legitimacy_class = 'GREEN'`. Before this split,
`WHERE legitimacy_class='GREEN'` returned 236 rows with one promotion
invisibly among them -- any downstream reader had to know to join a second
column, and the ones that didn't know silently treated a risk-scored promotion
as a native clearance. Two values means a reader cannot collapse them by
accident; collapsing them now takes an explicit `IN (...)`.

Two columns are kept DESPITE being near-constant, each for a stated reason:
  `capital_required_usd` -- 0.0 on every item, because the one known nonzero
  case (Cantina's submission fee) is a HELD source. The rubric requires the
  gate and a constant today is not a constant after the next roster ruling.
  `safe_harbor_text` -- populated on 4 of 527. Its ABSENCE is the legally
  meaningful state for a white-hat item; a dropped column cannot say
  "no safe-harbour language offered".
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from abelard_common.dedupe import compute_dedupe_hash

from .identity import compute_opportunity_id
from .models import RawItem

LEDGER_SCHEMA_VERSION = 2

# status -- only the first two are reachable from code.
DISCOVERED = "discovered"
PROPOSED = "proposed"

# legitimacy classes. GREEN_PROMOTED is a peer of GREEN, never a decoration.
GREEN = "GREEN"
GREEN_PROMOTED = "GREEN_PROMOTED"
YELLOW = "YELLOW"
RED = "RED"

# tos_class -- doctrine A.1.1 carried as DATA, not as a wiring decision.
# ToS-hostility is a separate, higher-priority gate than legitimacy: a surface
# can be perfectly legitimate work and still be un-wireable because READING it
# violates terms. Immunefi is the reference case.
TOS_WIRE = "WIRE"
TOS_REJECT = "REJECT-on-ToS"

# Column -> DDL. Kept as a mapping so the insert/update statements are
# generated from one list; the previous positional tuple had 40+ members and
# adding a column meant editing three places in matching order.
_COLUMNS: dict[str, str] = {
    "opportunity_id": "TEXT PRIMARY KEY",
    "source": "TEXT NOT NULL",
    "source_native_id": "TEXT NOT NULL",
    "title_hash": "TEXT NOT NULL",
    "url": "TEXT",
    "title": "TEXT NOT NULL",
    "category": "TEXT",
    "category_source": "TEXT",
    "counterparty": "TEXT",
    "counterparty_verified": "INTEGER",
    # --- payout ---------------------------------------------------------
    "payout_raw": "TEXT",
    "payout_usd_low": "REAL",
    "payout_usd_high": "REAL",
    "payout_currency": "TEXT",
    "payout_kind": "TEXT",
    "payout_basis": "TEXT NOT NULL",
    "payout_confidence": "TEXT NOT NULL",
    "escrow_verified": "INTEGER",
    # Is `payout_usd_low` what ONE recipient receives, or the size of a fund?
    # Default 0 = unknown, never "verified as the full amount". No current
    # source publishes a per-recipient breakdown: Superteam's API reports
    # `compensationType: fixed` and `rewardAmount: 500` for a listing whose page
    # splits that 500 across ten places, and there is no field carrying the
    # split. Until a source publishes one, this stays 0 everywhere -- which is
    # the honest state, not a gap.
    "payout_per_recipient_verified": "INTEGER NOT NULL DEFAULT 0",
    # --- asset class and indicative basis (Mando 2026-08-10) -------------
    # The scout sees LISTINGS, never RECEIPTS -- it does not execute work, so
    # it never witnesses a payment. These columns therefore carry an
    # INDICATIVE price at discovery with its provenance, not a cost basis.
    # Actual basis is set when tokens land and belongs to whatever accepts
    # payment. Naming the price source is the point: it keeps an illiquid
    # token's quoted price from being laundered into a dollar figure.
    "payout_asset_class": "TEXT",
    "payout_token_symbol": "TEXT",
    "payout_token_quantity": "REAL",
    "indicative_usd_at_discovery": "REAL",
    "price_source": "TEXT",
    "price_observed_unix": "INTEGER",
    # Contention / expected-gain inputs. `award_rate_source` names the field it
    # came from and `award_rate_observed_unix` when -- same provenance rule as
    # the token prices, because a counter scraped today is not a fact forever.
    "contention": "INTEGER",
    "award_rate": "REAL",
    "award_rate_source": "TEXT",
    "award_rate_observed_unix": "INTEGER",
    # --- gates ------------------------------------------------------------
    "identity_gate": "TEXT",
    "agent_permitted": "TEXT",
    "natural_person_required": "INTEGER",
    "scope_published": "INTEGER",
    "scope_text": "TEXT",
    "safe_harbor_text": "TEXT",
    "capital_required_usd": "REAL",
    "paid_acquisition": "INTEGER",
    "deadline_unix": "INTEGER",
    "effort_note": "TEXT",
    # --- classification ---------------------------------------------------
    "legitimacy_class": "TEXT NOT NULL",
    "class_reason": "TEXT NOT NULL",
    "reason_codes": "TEXT",
    "classified_by": "TEXT NOT NULL",
    "classifier_version": "TEXT NOT NULL",
    "mechanical_class": "TEXT",
    "llm_class": "TEXT",
    "classes_disagreed": "INTEGER NOT NULL DEFAULT 0",
    # --- risk score + promotion provenance --------------------------------
    "risk_score": "INTEGER",
    "risk_factors": "TEXT",
    "risk_weights_version": "TEXT",
    "promotion_eligible": "INTEGER",
    "promotion_blocked_by": "TEXT",
    "promoted_from_yellow": "INTEGER NOT NULL DEFAULT 0",
    "pre_promotion_class": "TEXT",
    "promoted_unix": "INTEGER",
    # --- lifecycle / provenance -------------------------------------------
    "status": "TEXT NOT NULL DEFAULT 'discovered'",
    "tos_class": "TEXT NOT NULL DEFAULT 'WIRE'",
    "first_seen_unix": "INTEGER NOT NULL",
    "last_seen_unix": "INTEGER NOT NULL",
    "scan_id": "TEXT NOT NULL",
    "resolved_via": "TEXT NOT NULL",
    "tos_flags": "TEXT",
    "raw_json": "TEXT",
    # --- SC-R2 interim ranking (derived; rewritten every rank run) ----------
    # Nullable with no default on purpose: a row that has never been ranked
    # reads as NULL rather than as position 0 or "unranked", which would be a
    # claim the ledger has not earned.
    "rank_segment": "TEXT",
    "rank_position": "INTEGER",
    "rank_sort_key": "REAL",
    "rank_expected_usd": "REAL",
    "rank_unranked_reason": "TEXT",
    "rank_algorithm_version": "TEXT",
    "rank_computed_unix": "INTEGER",
    # --- E22 flip history, denormalised onto the row for the CLI cut --------
    # Source of truth stays `opportunity_verdicts`; these are a projection so
    # churn is visible beside the rank rather than requiring a join.
    "verdicts_seen": "INTEGER",
    "flip_count": "INTEGER",
    "effective_verdict": "TEXT",
    # --- admission provenance (invariant 2) --------------------------------
    # `admission_applied_unix` is stamped ONLY by admissions.apply(), i.e. only
    # when the Mando-owned file moved this row. Its presence is the audit trail
    # that a human decision, not the daemon, set the status.
    "proposed_unix": "INTEGER",
    "admission_applied_unix": "INTEGER",
}

# Columns that must NOT be overwritten when an item is re-seen. Resetting
# `status` would silently un-admit something Mando had already ruled on --
# the worst failure this table can have.
_IMMUTABLE_ON_UPDATE = frozenset({"opportunity_id", "first_seen_unix", "status"})

_AUX_SCHEMA = """
CREATE TABLE IF NOT EXISTS scan_cost (
    scan_id               TEXT NOT NULL,
    recorded_unix         INTEGER NOT NULL,
    model                 TEXT,
    llm_calls             INTEGER NOT NULL DEFAULT 0,
    input_tokens          INTEGER NOT NULL DEFAULT 0,
    output_tokens         INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens     INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cost_usd              REAL NOT NULL DEFAULT 0.0,
    items_classified      INTEGER NOT NULL DEFAULT 0,
    transport_retries     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (scan_id, recorded_unix)
);

CREATE TABLE IF NOT EXISTS title_collisions (
    title_hash      TEXT NOT NULL,
    scan_id         TEXT NOT NULL,
    opportunity_ids TEXT NOT NULL,
    noted_unix      INTEGER NOT NULL,
    PRIMARY KEY (title_hash, scan_id)
);

CREATE INDEX IF NOT EXISTS idx_opp_class    ON opportunities(legitimacy_class, status);
CREATE INDEX IF NOT EXISTS idx_opp_source   ON opportunities(source, last_seen_unix);
CREATE INDEX IF NOT EXISTS idx_opp_hash     ON opportunities(title_hash);
CREATE INDEX IF NOT EXISTS idx_opp_disagree ON opportunities(classes_disagreed);
CREATE INDEX IF NOT EXISTS idx_opp_promoted ON opportunities(promoted_from_yellow);
"""


@dataclass
class Classification:
    """The verdict attached to one item."""

    legitimacy_class: str
    class_reason: str
    classified_by: str               # 'mechanical' | 'llm' | 'risk-promotion'
    classifier_version: str
    mechanical_class: str | None = None
    llm_class: str | None = None
    disagreed: bool = False
    reason_codes: tuple[str, ...] = ()
    risk_score: int | None = None
    risk_factors: str | None = None
    risk_weights_version: str | None = None
    promotion_eligible: bool | None = None
    promotion_blocked_by: str | None = None
    promoted_from_yellow: bool = False
    pre_promotion_class: str | None = None
    promoted_unix: int | None = None


def _add_missing_columns(conn: sqlite3.Connection) -> None:
    """Additive migration for ledgers created before a column existed.

    `CREATE TABLE IF NOT EXISTS` is a no-op on an existing table, so adding a
    name to `_COLUMNS` alone would leave live databases without it and fail at
    the next insert. ADD COLUMN only -- nothing here drops, renames, or
    rewrites, so an older ledger gains columns and loses nothing.
    """
    have = {r[1] for r in conn.execute("PRAGMA table_info(opportunities)")}
    if not have:
        return  # table not created yet; the CREATE above will carry every column
    for name, ddl in _COLUMNS.items():
        if name in have:
            continue
        # NOT NULL cannot be added to a populated table without a default;
        # every column added this way is deliberately nullable.
        safe = ddl.replace(" NOT NULL", "")
        conn.execute(f"ALTER TABLE opportunities ADD COLUMN {name} {safe}")
    conn.commit()


def apply_schema(conn: sqlite3.Connection) -> None:
    columns = ",\n    ".join(f"{name} {ddl}" for name, ddl in _COLUMNS.items())
    conn.executescript(
        f"CREATE TABLE IF NOT EXISTS opportunities (\n    {columns}\n);"
    )
    conn.executescript(_AUX_SCHEMA)
    # Append-only verdict history (doctrine E22). Lives in its own module
    # because it is the ONE table here that must never be updated in place.
    from . import verdicts as _verdicts

    conn.executescript(_verdicts.VERDICT_SCHEMA)
    _add_missing_columns(conn)
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES('ledger_schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(LEDGER_SCHEMA_VERSION),),
    )
    conn.commit()


def record_cost(
    conn: sqlite3.Connection,
    *,
    scan_id: str,
    model: str,
    llm_calls: int,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    cache_creation_tokens: int,
    cost_usd: float,
    items_classified: int,
    transport_retries: int = 0,
) -> None:
    """Persist cost telemetry.

    CALLED BEFORE ANY OPPORTUNITY ROW IS WRITTEN. A disk-write failure on the
    ledger must not lose the record of money already spent at the API.
    """
    conn.execute(
        "INSERT OR REPLACE INTO scan_cost(scan_id, recorded_unix, model, llm_calls,"
        " input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,"
        " cost_usd, items_classified, transport_retries)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            scan_id, int(time.time()), model, llm_calls, input_tokens,
            output_tokens, cache_read_tokens, cache_creation_tokens,
            cost_usd, items_classified, transport_retries,
        ),
    )
    conn.commit()


def _row_values(
    item: RawItem, verdict: Classification, *, scan_id: str, now_unix: int,
    tos_class: str,
) -> dict[str, Any]:
    return {
        "opportunity_id": compute_opportunity_id(item.source, item.native_id),
        "source": item.source,
        "source_native_id": item.native_id,
        "title_hash": compute_dedupe_hash(item.title),
        "url": item.url,
        "title": item.title,
        "category": item.category,
        "category_source": item.category_source,
        "counterparty": item.counterparty,
        "counterparty_verified": _as_int(item.counterparty_verified),
        "payout_raw": item.payout_raw,
        "payout_usd_low": item.payout_usd_low,
        "payout_usd_high": item.payout_usd_high,
        "payout_currency": item.payout_currency,
        "payout_kind": item.payout_kind,
        "payout_basis": item.payout_basis,
        "payout_confidence": item.payout_confidence,
        "escrow_verified": _as_int(item.escrow_verified),
        "payout_asset_class": item.payout_asset_class,
        "payout_token_symbol": item.payout_token_symbol,
        "payout_token_quantity": item.payout_token_quantity,
        "indicative_usd_at_discovery": item.indicative_usd_at_discovery,
        "price_source": item.price_source,
        "price_observed_unix": now_unix if item.price_source else None,
        "contention": item.contention,
        "award_rate": item.award_rate,
        "award_rate_source": item.award_rate_source,
        "award_rate_observed_unix": now_unix if item.award_rate_source else None,
        "identity_gate": item.identity_gate,
        "agent_permitted": item.agent_permitted,
        "natural_person_required": _as_int(item.natural_person_required),
        "scope_published": _as_int(item.scope_published),
        "scope_text": item.scope_text,
        "safe_harbor_text": item.safe_harbor_text,
        "capital_required_usd": item.capital_required_usd,
        "paid_acquisition": _as_int(item.paid_acquisition),
        "deadline_unix": item.deadline_unix,
        "effort_note": item.effort_note,
        "legitimacy_class": verdict.legitimacy_class,
        "class_reason": verdict.class_reason,
        "reason_codes": json.dumps(list(verdict.reason_codes)),
        "classified_by": verdict.classified_by,
        "classifier_version": verdict.classifier_version,
        "mechanical_class": verdict.mechanical_class,
        "llm_class": verdict.llm_class,
        "classes_disagreed": int(verdict.disagreed),
        "risk_score": verdict.risk_score,
        "risk_factors": verdict.risk_factors,
        "risk_weights_version": verdict.risk_weights_version,
        "promotion_eligible": _as_int(verdict.promotion_eligible),
        "promotion_blocked_by": verdict.promotion_blocked_by,
        "promoted_from_yellow": int(verdict.promoted_from_yellow),
        "pre_promotion_class": verdict.pre_promotion_class,
        "promoted_unix": verdict.promoted_unix,
        "status": DISCOVERED,
        "tos_class": tos_class,
        "first_seen_unix": now_unix,
        "last_seen_unix": now_unix,
        "scan_id": scan_id,
        "resolved_via": item.resolved_via,
        "tos_flags": json.dumps(item.tos_flags),
        "raw_json": json.dumps(item.raw, default=str)[:20000],
        # Derived columns, written by `rank.write_ranking`. Ingest sets them
        # back to None ON PURPOSE: a scan changes the data the ranking was
        # derived from, so the previous rank is stale the moment it lands, and
        # a stale rank is worse than no rank because it still reads as
        # authoritative. `scan` clears; `rank` recomputes.
        #
        # Listed EXPLICITLY rather than defaulted via `.get()` at insert time,
        # so forgetting a genuinely new column still raises KeyError instead of
        # silently writing NULL (E1: fail loud).
        "rank_segment": None,
        "rank_position": None,
        "rank_sort_key": None,
        "rank_expected_usd": None,
        "rank_unranked_reason": None,
        "rank_algorithm_version": None,
        "rank_computed_unix": None,
        "verdicts_seen": None,
        "flip_count": None,
        "effective_verdict": None,
        # Admission provenance is never set by ingest -- a scan must not be able
        # to stamp a row as though a human had ruled on it.
        "payout_per_recipient_verified": 0,
        "proposed_unix": None,
        "admission_applied_unix": None,
    }


def upsert_items(
    conn: sqlite3.Connection,
    items: Iterable[tuple[RawItem, Classification]],
    *,
    scan_id: str,
    now_unix: int,
    tos_class: str = TOS_WIRE,
) -> tuple[int, int]:
    """Insert or refresh rows. Returns (inserted, updated)."""
    # Local import: verdicts -> classify -> ledger is circular at module level.
    from . import verdicts as _verdicts

    inserted = updated = 0
    names = list(_COLUMNS)
    placeholders = ",".join("?" for _ in names)
    updatable = [n for n in names if n not in _IMMUTABLE_ON_UPDATE]
    set_clause = ",".join(f"{n}=?" for n in updatable)

    for item, verdict in items:
        values = _row_values(
            item, verdict, scan_id=scan_id, now_unix=now_unix, tos_class=tos_class
        )
        opportunity_id = values["opportunity_id"]
        exists = conn.execute(
            "SELECT 1 FROM opportunities WHERE opportunity_id = ?", (opportunity_id,)
        ).fetchone()

        if exists is None:
            conn.execute(
                f"INSERT INTO opportunities({','.join(names)}) VALUES({placeholders})",
                [values[n] for n in names],
            )
            inserted += 1
        else:
            conn.execute(
                f"UPDATE opportunities SET {set_clause} WHERE opportunity_id=?",
                [values[n] for n in updatable] + [opportunity_id],
            )
            updated += 1

        # E21: the UPDATE above overwrites this row's previous verdict. Append
        # the observation FIRST-CLASS before that loss becomes permanent --
        # this table is the only place the history survives.
        _verdicts.record_verdict(
            conn,
            opportunity_id=opportunity_id,
            scan_id=scan_id,
            observed_unix=now_unix,
            mechanical_class=values.get("mechanical_class") or "",
            legitimacy_class=values.get("legitimacy_class"),
            classes_disagreed=bool(values.get("classes_disagreed")),
            class_reason=values.get("class_reason"),
        )
    conn.commit()
    return inserted, updated


def record_collisions(
    conn: sqlite3.Connection,
    collisions: dict[str, list[str]],
    *,
    scan_id: str,
    now_unix: int,
) -> None:
    """Log title collisions. Recorded and linked -- never used to drop a row."""
    for title_hash, ids in collisions.items():
        conn.execute(
            "INSERT OR REPLACE INTO title_collisions"
            "(title_hash, scan_id, opportunity_ids, noted_unix) VALUES(?,?,?,?)",
            (title_hash, scan_id, json.dumps(sorted(ids)), now_unix),
        )
    conn.commit()


def class_distribution(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT legitimacy_class, COUNT(*) AS n FROM opportunities "
        "GROUP BY legitimacy_class"
    ).fetchall()
    return {row["legitimacy_class"]: row["n"] for row in rows}


def disagreements(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """The mechanical-vs-LLM disagreement set -- the calibration evidence."""
    return conn.execute(
        "SELECT source, title, mechanical_class, llm_class, legitimacy_class,"
        " class_reason FROM opportunities WHERE classes_disagreed = 1"
        " ORDER BY source, title"
    ).fetchall()


def promotions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Every YELLOW->GREEN_PROMOTED move, with score, factors, and weights."""
    return conn.execute(
        "SELECT source, title, risk_score, risk_factors, risk_weights_version,"
        " reason_codes, promoted_unix, class_reason FROM opportunities"
        " WHERE promoted_from_yellow = 1 ORDER BY risk_score DESC, source"
    ).fetchall()


def risk_histogram(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    """Score distribution in bands, for calibrating the threshold on evidence.

    Band edges come from `risk.HISTOGRAM_BANDS`, which derives them from the
    threshold. Writing them here independently meant a threshold change would
    silently leave the histogram splitting at the old boundary.
    """
    from .risk import HISTOGRAM_BANDS

    rows = conn.execute(
        "SELECT risk_score FROM opportunities WHERE risk_score IS NOT NULL"
    ).fetchall()
    return [
        (f"{low}-{high}", sum(1 for r in rows if low <= (r["risk_score"] or 0) <= high))
        for low, high in HISTOGRAM_BANDS
    ]


def veto_rates(conn: sqlite3.Connection) -> dict[str, object]:
    """Raw and corrected LLM veto rates, so the correction is auditable.

    RAW is row-level: vetoed mechanical-GREEN rows / all mechanical-GREEN rows.
    It over-counts, because Dework serves near-identical rows and one judgment
    applied to nine duplicates reads as nine disagreements.

    CORRECTED counts JUDGMENT UNITS -- distinct `title_hash` -- so a repeated
    listing contributes one judgment however many times it appears. The LEDGER
    still keeps every row (invariant 1); only the RATE collapses them.

    Both are reported. A correction nobody can inspect is just a smaller number.
    """
    def scalar(sql: str) -> int:
        return conn.execute(sql).fetchone()[0] or 0

    raw_green = scalar(
        "SELECT COUNT(*) FROM opportunities WHERE mechanical_class='GREEN'"
    )
    raw_vetoed = scalar(
        "SELECT COUNT(*) FROM opportunities WHERE mechanical_class='GREEN'"
        " AND classes_disagreed=1"
    )
    unit_green = scalar(
        "SELECT COUNT(DISTINCT title_hash) FROM opportunities"
        " WHERE mechanical_class='GREEN'"
    )
    unit_vetoed = scalar(
        "SELECT COUNT(DISTINCT title_hash) FROM opportunities"
        " WHERE mechanical_class='GREEN' AND classes_disagreed=1"
    )
    # THIRD RATE: persona vetoes removed as CORRECT CATCHES (Mando 2026-08-11).
    #
    # The persona gate is a ruled-in permanent mechanism, not a rubric defect,
    # so counting its firings as "disagreement" measures the wrong thing. The
    # question the gate is meant to answer is: setting persona aside, how often
    # does the LLM overturn the mechanical rubric?
    #
    # Detection reads the STORED reason text rather than the code, so this is
    # computable over rows classified before the code existed -- no re-scan.
    from .classify import is_persona_veto

    rows = conn.execute(
        "SELECT class_reason, MIN(rowid) FROM opportunities"
        " WHERE mechanical_class='GREEN' AND classes_disagreed=1"
        " GROUP BY title_hash"
    ).fetchall()
    persona_units = sum(1 for r in rows if is_persona_veto(r["class_reason"]))
    non_persona = unit_vetoed - persona_units

    # PER SOURCE -- the only cut of this number that answers a question
    # (doctrine E19, Mando 2026-08-13). The aggregate is a weighted average
    # dominated by whichever source is thinnest, so it reports corpus quality
    # while looking like it reports rubric quality. Per source it is a monitor:
    # a source that MOVES has degraded or drifted, which is diagnosable.
    per_source_rows = conn.execute(
        "SELECT source, class_reason, classes_disagreed, MIN(rowid)"
        " FROM opportunities WHERE mechanical_class='GREEN' GROUP BY title_hash"
    ).fetchall()
    tally: dict[str, list[int]] = {}
    for row in per_source_rows:
        entry = tally.setdefault(row["source"], [0, 0])
        entry[0] += 1
        if row["classes_disagreed"] and not is_persona_veto(row["class_reason"]):
            entry[1] += 1
    per_source = sorted(
        (
            {
                "source": src,
                "green": green,
                "vetoed": vetoed,
                "rate": (100.0 * vetoed / green) if green else 0.0,
            }
            for src, (green, vetoed) in tally.items()
        ),
        key=lambda d: -d["rate"],
    )

    return {
        "per_source": per_source,
        "raw_green": raw_green,
        "raw_vetoed": raw_vetoed,
        "raw_rate": (100.0 * raw_vetoed / raw_green) if raw_green else 0.0,
        "unit_green": unit_green,
        "unit_vetoed": unit_vetoed,
        "unit_rate": (100.0 * unit_vetoed / unit_green) if unit_green else 0.0,
        "duplicates_collapsed": raw_vetoed - unit_vetoed,
        "persona_units": persona_units,
        "non_persona_vetoed": non_persona,
        "non_persona_rate": (100.0 * non_persona / unit_green) if unit_green else 0.0,
    }


def residual_vetoes(
    conn: sqlite3.Connection, *, exclude_persona: bool = True
) -> list[sqlite3.Row]:
    """Surviving judgment units -- the rubric-review material.

    Persona vetoes are excluded by default: they are a ruled-in permanent gate,
    so listing them as "unresolved rubric questions" would keep re-presenting a
    decision Mando has already made.
    """
    from .classify import is_persona_veto

    rows = conn.execute(
        "SELECT source, title, class_reason, MIN(rowid) FROM opportunities"
        " WHERE mechanical_class='GREEN' AND classes_disagreed=1"
        " GROUP BY title_hash ORDER BY source, title"
    ).fetchall()
    if not exclude_persona:
        return rows
    return [r for r in rows if not is_persona_veto(r["class_reason"])]


def dead_zone_occupants(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Items scoring 21-30.

    Empty on the corpus that set the threshold, which is an OBSERVATION about
    that corpus and not a property of the weights -- a category-unresolved item
    scores exactly 25. Occupancy is information for Mando, so it is surfaced
    loudly rather than assumed away.
    """
    return conn.execute(
        "SELECT source, title, risk_score, risk_factors, promotion_eligible"
        " FROM opportunities WHERE risk_score BETWEEN 21 AND 30"
        " ORDER BY risk_score DESC"
    ).fetchall()


def _as_int(value: bool | None) -> int | None:
    return None if value is None else int(value)


__all__ = [
    "LEDGER_SCHEMA_VERSION",
    "DISCOVERED", "PROPOSED",
    "GREEN", "GREEN_PROMOTED", "YELLOW", "RED",
    "TOS_WIRE", "TOS_REJECT",
    "Classification",
    "apply_schema", "record_cost", "upsert_items", "record_collisions",
    "class_distribution", "disagreements", "promotions", "risk_histogram",
    "veto_rates", "residual_vetoes",
    "dead_zone_occupants",
]
