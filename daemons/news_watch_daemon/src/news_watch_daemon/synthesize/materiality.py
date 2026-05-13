"""Materiality gate — final dispatch decision.

Pure script. No LLM, no I/O beyond the Brief archive scan. The gate
consumes a Brief (synthesized but not yet dispatched), compares its
events against a threshold and against recent briefs' events, and
returns a decision the orchestrator acts on.

The two suppression paths:

  1. `below_materiality_threshold`: zero events with materiality_score
     ≥ threshold. Sonnet wrote a brief but nothing in it crosses the
     bar; suppress the dispatch but archive the brief anyway.

  2. `dedup_recent`: events above threshold exist, but every single
     one of them has a fingerprint that matches an event in a brief
     archived within the last `dedup_window_hours`. Sonnet is
     re-reporting events Mando has already seen; suppress.

If at least one event above threshold has a fingerprint NOT in the
recent set, the gate dispatches.

Dedup key (Mando's Step 8 directive):

    fingerprint(event) = compute_dedupe_hash(event.headline_summary)

Reuses Pass A's scrape/dedup.py normalization (lowercase, drop
non-alphanum, collapse whitespace, truncate to 80 chars, SHA256[:32]).
This is the same "same logical event, regardless of wording" key the
scrape layer uses to dedup near-duplicate Reuters wire variants. The
choice is explicit: we dedup on the **summary text**, not on the
underlying source_headlines.headline_id set (which would require
loading + comparing every recent brief's headlines), and not on
whole-Brief equality (trivial, useless).

If two events across briefs share the same normalized summary text,
they are treated as the same event for dedup purposes. False
positives here suppress legitimate updates; false negatives let
duplicates through. The §14 calibration review will tune.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..scrape.dedup import compute_dedupe_hash
from .archive import ArchiveError, list_brief_ids, read_brief
from .brief import Brief


_LOG = logging.getLogger("news_watch_daemon.synthesize.materiality")


@dataclass(frozen=True)
class MaterialityDecision:
    """Output of evaluate_materiality(). Pure data; consumed by the
    orchestrator to set Brief.dispatch fields and decide whether to
    invoke the alert sink."""

    dispatch: bool
    reason: str   # "above_threshold" | "below_materiality_threshold" |
                  # "dedup_recent" | "no_events"
    above_threshold_count: int = 0
    new_events_count: int = 0
    deduped_against_brief_ids: tuple[str, ...] = field(default_factory=tuple)


def fingerprint_event(headline_summary: str) -> str:
    """Compute the dedup fingerprint for a Brief event.

    Single source of truth — exported so tests can pin behavior and
    future modules can reuse without re-deriving the normalization.
    """
    return compute_dedupe_hash(headline_summary)


def _iso_to_unix(iso: str) -> int:
    """Parse an ISO-8601 timestamp string into Unix seconds.

    Handles both `2026-05-13T14:32:08Z` and `2026-05-13T14:32:08+00:00`.
    """
    # datetime.fromisoformat in 3.11+ handles "Z" natively, but to be
    # robust across older interpreters: normalize "Z" → "+00:00".
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _collect_recent_fingerprints(
    archive_root: Path,
    since_unix: int,
) -> tuple[set[str], list[str]]:
    """Walk the archive newest-first, gathering event fingerprints from
    briefs generated since `since_unix`. Returns (fingerprints, brief_ids).

    Stops scanning as soon as a brief's generated_at falls below the
    cutoff — saves I/O on large archives.
    """
    fingerprints: set[str] = set()
    brief_ids: list[str] = []
    if not archive_root.is_dir():
        return fingerprints, brief_ids

    for bid in list_brief_ids(archive_root):
        try:
            brief = read_brief(archive_root, bid)
        except ArchiveError as exc:
            _LOG.warning("skipping unreadable brief %s during dedup scan: %s", bid, exc)
            continue
        try:
            gen_unix = _iso_to_unix(brief.generated_at)
        except (ValueError, TypeError):
            _LOG.warning("skipping brief %s with unparseable generated_at: %r",
                         bid, brief.generated_at)
            continue
        if gen_unix < since_unix:
            break  # newest-first; everything after is older
        for event in brief.events:
            fingerprints.add(fingerprint_event(event.headline_summary))
        brief_ids.append(bid)
    return fingerprints, brief_ids


def evaluate_materiality(
    brief: Brief,
    *,
    threshold: float,
    dedup_window_hours: int,
    archive_root: Path,
    now_unix: int | None = None,
) -> MaterialityDecision:
    """Decide whether to dispatch `brief` based on threshold + dedup.

    Pure-ish: reads the archive but doesn't mutate it. The orchestrator
    is responsible for writing the brief and calling the sink based on
    this decision.

    Algorithm:
      1. Filter events to those with materiality_score >= threshold.
      2. If none, return suppress(below_materiality_threshold).
      3. Scan archive for briefs in the dedup window; collect event
         fingerprints.
      4. For each surviving event, fingerprint it; if NOT in the recent
         set, count as a new event.
      5. If at least one new event survives, dispatch. Otherwise
         suppress(dedup_recent).
    """
    if not brief.events:
        return MaterialityDecision(dispatch=False, reason="no_events")

    above = [e for e in brief.events if e.materiality_score >= threshold]
    if not above:
        return MaterialityDecision(
            dispatch=False, reason="below_materiality_threshold",
            above_threshold_count=0,
        )

    now = int(time.time()) if now_unix is None else now_unix
    since = now - dedup_window_hours * 3600
    recent_fingerprints, recent_brief_ids = _collect_recent_fingerprints(archive_root, since)

    new_count = 0
    deduped_against: set[str] = set()
    for event in above:
        fp = fingerprint_event(event.headline_summary)
        if fp not in recent_fingerprints:
            new_count += 1
        else:
            # Record which recent briefs we dedup against (best-effort).
            # We don't track per-fingerprint origin in the scan above,
            # so report all recent briefs as candidates.
            deduped_against.update(recent_brief_ids)

    if new_count > 0:
        return MaterialityDecision(
            dispatch=True, reason="above_threshold",
            above_threshold_count=len(above),
            new_events_count=new_count,
            deduped_against_brief_ids=tuple(sorted(deduped_against)),
        )
    return MaterialityDecision(
        dispatch=False, reason="dedup_recent",
        above_threshold_count=len(above),
        new_events_count=0,
        deduped_against_brief_ids=tuple(sorted(deduped_against)),
    )


__all__ = [
    "MaterialityDecision",
    "evaluate_materiality",
    "fingerprint_event",
]
