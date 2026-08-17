"""Surfacing: the upward path, scout -> Abelard's alert queue (recon §6.1-6.2).

THE DAEMON ENQUEUES. IT NEVER DISPATCHES. `abelard_queue` is the single
component allowed to send anything outward; scout writes a durable row and
stops. That boundary is the dumb-daemon invariant, and it is why nothing here
imports a network client.

MECHANICAL DETECT, ABELARD JUDGES. The routing rules below are deterministic
and dumb on purpose. Whether a surfaced item deserves Mando's attention is
Abelard's call at his layer; scout's job is to make sure the item cannot pass
unnoticed. Same split as the classifier's mechanical/LLM boundary.

ROUTING (recon §6.2)
    GREEN, category already seen      -> accrues in the ledger, no queue
    GREEN, category NEVER SEEN BEFORE -> QUEUE. A novel surface is the one
                                         thing always worth a look, because it
                                         is the case the rubric was not written
                                         against.
    YELLOW, routine                   -> no queue
    YELLOW, high payout               -> NOT IMPLEMENTED, see below
    RED                               -> NEVER queued as work. Visible in the
                                         ledger and exports only.

WHY THE YELLOW HIGH-PAYOUT CUT IS ABSENT. Recon §6.3 deferred the threshold and
it has still not been measured. Shipping a number here would be exactly the
measure-before-mandate failure E8 exists to prevent, so the rule is left
unimplemented and named rather than guessed at. `high_payout_cut_pending()`
reports the population it would act on, which is the measurement Mando needs to
set it.

NOVELTY IS A LEDGER FACT, NOT A JUDGMENT. A category is novel the first time it
appears, recorded in `seen_categories` (append-only). Recording is separate from
surfacing so a category cannot be marked seen by a run that failed to queue it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from . import admissions as admissions_mod

QUEUE_SOURCE = "scout_daemon"
KIND_NOVEL_CATEGORY = "scout.novel_category"

# `~/.openclaw/abelard_queue/queue.db` -- the queue owns its own state home;
# scout does not create or migrate it beyond AlertQueue's own schema call.
DEFAULT_QUEUE_PATH = Path.home() / ".openclaw" / "abelard_queue" / "queue.db"

SEEN_CATEGORIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_categories (
    category              TEXT PRIMARY KEY,
    first_seen_unix       INTEGER NOT NULL,
    first_opportunity_id  TEXT NOT NULL,
    first_scan_id         TEXT
);
"""


@dataclass
class SurfaceOutcome:
    novel_categories: list[str] = field(default_factory=list)
    enqueued: int = 0
    already_queued: int = 0
    proposed: int = 0
    skipped_red: int = 0
    errors: list[str] = field(default_factory=list)


def apply_schema(conn) -> None:
    conn.executescript(SEEN_CATEGORIES_SCHEMA)
    conn.commit()


def novel_categories(conn) -> list[tuple[str, str]]:
    """[(category, opportunity_id)] for categories never recorded as seen.

    Reads GREEN-family rows only: a category first observed on a RED item has
    not been legitimately encountered, and marking it seen there would suppress
    the alert when it later shows up on real work.
    """
    # DERIVED CATEGORIES ARE EXCLUDED, and this is the difference between a
    # novelty detector and a new-listing detector.
    #
    # Measured 2026-08-16: `category` holds 130 distinct values over 527 rows
    # with 85 singletons -- not a taxonomy. The cause is `category_source`:
    # where it is 'derived', the adapter synthesised the category from the
    # listing's own title (questbook alone: 67 distinct over 80 rows, one per
    # grant program), so "a category never seen before" fires on essentially
    # every new listing forever. Sample derived values: "test_grants", "wdsa",
    # "QA Testing & Bug Reporting for dApps" -- those are titles, not classes.
    #
    # Restricted to 'structured' and 'source_constant', the population behaves
    # like a taxonomy: 41 novel categories drops to 17, singletons 30 -> 7.
    # A category that is only a restatement of one listing cannot be evidence
    # that a NEW KIND of work has appeared.
    rows = conn.execute(
        "SELECT category, opportunity_id FROM opportunities"
        " WHERE category IS NOT NULL AND category <> ''"
        "   AND COALESCE(category_source,'') <> 'derived'"
        "   AND legitimacy_class IN ('GREEN','GREEN_PROMOTED')"
        " ORDER BY category, opportunity_id"
    ).fetchall()
    seen = {r[0] for r in conn.execute("SELECT category FROM seen_categories")}
    out: list[tuple[str, str]] = []
    claimed: set[str] = set()
    for r in rows:
        cat = r["category"]
        if cat in seen or cat in claimed:
            continue
        claimed.add(cat)
        out.append((cat, r["opportunity_id"]))
    return out


def record_seen(conn, category: str, opportunity_id: str, *, now_unix: int,
                scan_id: str | None = None) -> None:
    """INSERT OR IGNORE -- a category is marked seen exactly once, ever."""
    conn.execute(
        "INSERT OR IGNORE INTO seen_categories"
        "(category, first_seen_unix, first_opportunity_id, first_scan_id)"
        " VALUES(?,?,?,?)",
        (category, now_unix, opportunity_id, scan_id),
    )


def high_payout_cut_pending(conn) -> dict:
    """The measurement the deferred YELLOW rule needs -- NOT the rule itself.

    Returns the payout distribution over YELLOW rows so a cut can be set from
    observed quantiles rather than invented. Until Mando rules a number, no
    YELLOW row is queued on payout grounds.
    """
    vals = [r[0] for r in conn.execute(
        "SELECT payout_usd_low FROM opportunities"
        " WHERE legitimacy_class='YELLOW' AND payout_usd_low IS NOT NULL"
        " ORDER BY payout_usd_low")]
    if not vals:
        return {"n": 0, "note": "no YELLOW row carries a payout; cut unmeasurable"}
    q = lambda k: vals[min(len(vals) - 1, int(k * len(vals)))]
    return {"n": len(vals), "min": vals[0], "p50": q(0.50), "p75": q(0.75),
            "p90": q(0.90), "p99": q(0.99), "max": vals[-1],
            "note": "threshold deferred per recon 6.3 -- no YELLOW row is queued"}


def run(conn, *, now_unix: int, queue_path: Path | None = None,
        dry_run: bool = False) -> SurfaceOutcome:
    """Detect novel categories, enqueue them, mark the rows proposed.

    Ordering is deliberate: enqueue FIRST, then record the category as seen,
    then mark the row proposed. If the enqueue raises, the category stays unseen
    and the next run retries it. Recording first would lose the alert silently,
    which is the failure E1 forbids.
    """
    outcome = SurfaceOutcome()
    apply_schema(conn)

    outcome.skipped_red = conn.execute(
        "SELECT COUNT(*) FROM opportunities WHERE legitimacy_class='RED'"
    ).fetchone()[0]

    candidates = novel_categories(conn)
    if not candidates:
        return outcome

    if dry_run:
        outcome.novel_categories = [c for c, _ in candidates]
        return outcome

    from abelard_common.alert_queue import AlertQueue, QueueError

    proposed_ids: list[str] = []
    queue = AlertQueue(queue_path or DEFAULT_QUEUE_PATH)
    try:
        for category, oid in candidates:
            row = conn.execute(
                "SELECT source, title, url, payout_usd_low, legitimacy_class,"
                " rank_position FROM opportunities WHERE opportunity_id=?",
                (oid,)).fetchone()
            try:
                _, created = queue.enqueue(
                    source=QUEUE_SOURCE,
                    kind=KIND_NOVEL_CATEGORY,
                    topic_key=f"category:{category}",
                    # Category-scoped, so a novel category alerts ONCE however
                    # many listings arrive under it.
                    dedupe_key=f"{QUEUE_SOURCE}:novel_category:{category}",
                    payload={
                        "category": category,
                        "example_opportunity_id": oid,
                        "source": row["source"],
                        "title": row["title"],
                        "url": row["url"],
                        "payout_usd_low": row["payout_usd_low"],
                        "legitimacy_class": row["legitimacy_class"],
                        "rank_position": row["rank_position"],
                        "why": "category never seen before in the scout ledger",
                        "action_required": "none -- surfaced for judgment; "
                                           "admission is a human edit to config/admissions.yaml",
                    },
                )
            except QueueError as exc:
                outcome.errors.append(f"{category}: {exc}")
                continue
            outcome.enqueued += int(created)
            outcome.already_queued += int(not created)
            outcome.novel_categories.append(category)
            record_seen(conn, category, oid, now_unix=now_unix)
            proposed_ids.append(oid)
        conn.commit()
    finally:
        queue.close()

    outcome.proposed = admissions_mod.propose(conn, proposed_ids, now_unix=now_unix)
    return outcome


__all__ = [
    "QUEUE_SOURCE", "KIND_NOVEL_CATEGORY", "DEFAULT_QUEUE_PATH",
    "SurfaceOutcome", "apply_schema", "novel_categories", "record_seen",
    "high_payout_cut_pending", "run",
]
