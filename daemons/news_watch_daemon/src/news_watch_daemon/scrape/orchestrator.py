"""Scrape orchestration — sequential, dedup-aware, theme-tagging.

Sequential by design (carried over from Pass A flag #10): sources are
independent network calls but the 15-minute cycle has plenty of latency
budget, and thread pools introduce coordination failure modes for no
gain. If a single slow source ever becomes a real problem, parallelism
becomes a follow-up flag.

Responsibilities:

  1. For each source: check per-source cadence (Pass B Artifact 3). If
     `cadence_minutes` is set and last_attempt_unix is more recent than
     `cadence_minutes` minutes ago, the source is skipped — no fetch,
     no source_health update, a status="skipped" PerSourceResult.
  2. For each non-skipped source: compute since_unix from
     source_health, call fetch().
  3. For each returned FetchedItem:
     - compute dedupe_hash; skip if seen in last 72h.
     - apply theme keyword/exclusion regexes; insert headline +
       headline_theme_tags rows.
  4. Update source_health (status, attempt/success timestamps, failure
     counter). Skipped sources DO NOT receive an update.
  5. Write daemon_heartbeat for component='scrape'.

The orchestrator never raises. CLI-level errors (DB unreachable, no
active themes, schema not applied) surface from the CLI handler before
this function is called.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

from ..db import record_heartbeat, to_json_column, transaction
from ..sources.base import FetchedItem, FetchResult, SourcePlugin
from ..theme_config import ThemeConfig
from .dedup import compute_dedupe_hash
from .ticker_extract import TrackedTickers


_LOG = logging.getLogger("news_watch_daemon.scrape")

DEFAULT_SINCE_LOOKBACK_S = 7 * 86400        # 7 days
DEDUP_WINDOW_S = 72 * 3600                  # 72 hours
_HEADLINE_ID_LEN = 64                       # full SHA256 hex


# ---------- result shapes ----------


@dataclass(frozen=True)
class PerSourceResult:
    name: str
    # FetchStatus values plus "skipped" (cadence-throttled by orchestrator).
    status: str
    items_fetched: int
    items_after_dedup: int
    items_inserted: int
    error_detail: str | None


@dataclass(frozen=True)
class ScrapeResult:
    started_at_unix: int
    started_at: str
    duration_ms: int
    sources_attempted: int       # sources we actually called fetch() on
    sources_succeeded: int       # status="ok"
    sources_failed: int          # status not in ("ok", "skipped")
    sources_skipped: int         # cadence-throttled
    per_source: list[PerSourceResult]
    headlines_inserted_total: int
    theme_tags_inserted_total: int
    themes_active: list[str]


# ---------- compiled theme regexes ----------


@dataclass(frozen=True)
class _ThemeRegexes:
    theme_id: str
    primary: re.Pattern[str]
    secondary: re.Pattern[str] | None
    exclusion: re.Pattern[str] | None


def _compile_theme_regexes(themes: Iterable[ThemeConfig]) -> list[_ThemeRegexes]:
    out: list[_ThemeRegexes] = []
    for theme in themes:
        primary = re.compile("|".join(theme.keywords.primary), re.IGNORECASE)
        secondary = (
            re.compile("|".join(theme.keywords.secondary), re.IGNORECASE)
            if theme.keywords.secondary
            else None
        )
        exclusion = (
            re.compile("|".join(theme.keywords.exclusions), re.IGNORECASE)
            if theme.keywords.exclusions
            else None
        )
        out.append(_ThemeRegexes(
            theme_id=theme.theme_id,
            primary=primary,
            secondary=secondary,
            exclusion=exclusion,
        ))
    return out


def _tag_for_theme(headline: str, regs: _ThemeRegexes) -> str | None:
    """Return 'primary' / 'secondary' / None for one theme + headline."""
    if regs.exclusion and regs.exclusion.search(headline):
        return None
    if regs.primary.search(headline):
        return "primary"
    if regs.secondary and regs.secondary.search(headline):
        return "secondary"
    return None


# ---------- helpers ----------


def _now_pair(now_unix: int | None = None) -> tuple[int, str]:
    if now_unix is not None:
        dt = datetime.fromtimestamp(now_unix, tz=timezone.utc)
        return now_unix, dt.isoformat(timespec="seconds").replace("+00:00", "Z")
    dt = datetime.now(timezone.utc)
    return int(dt.timestamp()), dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def _iso_from_unix(unix: int) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _headline_id(headline: str, source: str) -> str:
    basis = f"{headline}|{source}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:_HEADLINE_ID_LEN]


def _since_unix_for_source(conn: sqlite3.Connection, source_name: str, now_unix: int) -> int:
    row = conn.execute(
        "SELECT last_successful_fetch_unix FROM source_health WHERE source = ?",
        (source_name,),
    ).fetchone()
    if row is None or row["last_successful_fetch_unix"] is None:
        return now_unix - DEFAULT_SINCE_LOOKBACK_S
    return int(row["last_successful_fetch_unix"])


def _cadence_skip(
    conn: sqlite3.Connection,
    source_name: str,
    cadence_minutes: int | None,
    now_unix: int,
) -> int | None:
    """Return next_eligible_at_unix if the source should be skipped, else None.

    Brand-new sources (no source_health row, or NULL last_attempt_unix)
    are never throttled — they always run on their first appearance.
    """
    if cadence_minutes is None:
        return None
    row = conn.execute(
        "SELECT last_attempt_unix FROM source_health WHERE source = ?",
        (source_name,),
    ).fetchone()
    if row is None or row["last_attempt_unix"] is None:
        return None
    last_attempt = int(row["last_attempt_unix"])
    elapsed = now_unix - last_attempt
    cadence_s = cadence_minutes * 60
    if elapsed >= cadence_s:
        return None
    return last_attempt + cadence_s


def _dedupe_hash_exists(
    conn: sqlite3.Connection, dedupe_hash: str, window_start_unix: int
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM headlines "
        "WHERE dedupe_hash = ? AND fetched_at_unix >= ? "
        "LIMIT 1",
        (dedupe_hash, window_start_unix),
    ).fetchone()
    return row is not None


def _update_source_health(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    status: str,
    error_detail: str | None,
    now_unix: int,
    now_iso: str,
) -> None:
    """Upsert source_health row according to the failure-counter rules.

    Counter policy (Pass A flag #5):
      - ok:           update last_successful + last_attempt; counter=0
      - partial:      update last_attempt only; counter=0
      - error/rate_limited: update last_attempt only; counter++
    """
    is_success = status == "ok"
    resets_counter = status in ("ok", "partial")
    existing = conn.execute(
        "SELECT consecutive_failure_count FROM source_health WHERE source = ?",
        (source_name,),
    ).fetchone()
    prior_count = existing["consecutive_failure_count"] if existing else 0
    new_count = 0 if resets_counter else (prior_count + 1)

    # Compose the success columns: only updated on status=ok.
    if is_success:
        last_success_unix: int | None = now_unix
        last_success_iso: str | None = now_iso
    else:
        # Preserve prior values if any
        prior = conn.execute(
            "SELECT last_successful_fetch_unix, last_successful_fetch "
            "FROM source_health WHERE source = ?",
            (source_name,),
        ).fetchone()
        if prior:
            last_success_unix = prior["last_successful_fetch_unix"]
            last_success_iso = prior["last_successful_fetch"]
        else:
            last_success_unix = None
            last_success_iso = None

    conn.execute(
        "INSERT INTO source_health "
        "(source, last_successful_fetch_unix, last_successful_fetch, "
        " last_attempt_unix, last_attempt, last_status, last_error_detail, "
        " consecutive_failure_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(source) DO UPDATE SET "
        " last_successful_fetch_unix=excluded.last_successful_fetch_unix, "
        " last_successful_fetch=excluded.last_successful_fetch, "
        " last_attempt_unix=excluded.last_attempt_unix, "
        " last_attempt=excluded.last_attempt, "
        " last_status=excluded.last_status, "
        " last_error_detail=excluded.last_error_detail, "
        " consecutive_failure_count=excluded.consecutive_failure_count",
        (
            source_name,
            last_success_unix,
            last_success_iso,
            now_unix,
            now_iso,
            status,
            error_detail,
            new_count,
        ),
    )


def _insert_headline_and_tags(
    conn: sqlite3.Connection,
    *,
    headline_id: str,
    source_name: str,
    item: FetchedItem,
    tickers: list[str],
    dedupe_hash: str,
    fetched_at_unix: int,
    fetched_at_iso: str,
    tag_rows: list[tuple[str, str]],  # (theme_id, confidence)
) -> int:
    """Insert one headline + N theme tags atomically. Returns tag count inserted.

    `tickers` is the orchestrator-computed merge of (source-provided
    tickers from FetchedItem.tickers) ∪ (extracted tickers from the
    headline text via TrackedTickers). Lands in the existing
    `headlines.tickers_json` column.
    """
    with transaction(conn):
        conn.execute(
            "INSERT INTO headlines "
            "(headline_id, source, raw_source, headline, url, "
            " published_at_unix, published_at, fetched_at_unix, fetched_at, "
            " dedupe_hash, tickers_json, entities_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                headline_id,
                source_name,
                item.raw_source,
                item.headline,
                item.url,
                item.published_at_unix,
                _iso_from_unix(item.published_at_unix),
                fetched_at_unix,
                fetched_at_iso,
                dedupe_hash,
                to_json_column(tickers),
                None,
            ),
        )
        for theme_id, confidence in tag_rows:
            conn.execute(
                "INSERT INTO headline_theme_tags "
                "(headline_id, theme_id, confidence, tagged_at_unix) "
                "VALUES (?, ?, ?, ?)",
                (headline_id, theme_id, confidence, fetched_at_unix),
            )
    return len(tag_rows)


# ---------- main entry point ----------


def run_scrape(
    conn: sqlite3.Connection,
    sources: list[SourcePlugin],
    themes: list[ThemeConfig],
    *,
    now_unix: int | None = None,
    tracked_tickers: TrackedTickers | None = None,
) -> ScrapeResult:
    """Execute one scrape sweep. Caller owns conn lifecycle.

    The caller (the CLI handler) must have already validated that:
      - the DB schema is applied
      - at least one active theme exists
    This function assumes those invariants hold.

    `tracked_tickers`, when provided, runs ticker extraction over each
    headline's text. Extracted tickers union with any tickers the source
    plugin pre-tagged on the `FetchedItem`. When None, no extraction —
    only source-provided tickers are persisted (Pass A/B behavior).
    """
    start_unix, start_iso = _now_pair(now_unix)
    start_perf = time.perf_counter()

    theme_regexes = _compile_theme_regexes(themes)
    active_theme_ids = sorted(t.theme_id for t in themes)
    dedup_window_start = start_unix - DEDUP_WINDOW_S

    per_source: list[PerSourceResult] = []
    headlines_inserted_total = 0
    theme_tags_inserted_total = 0
    seen_dedup_hashes: set[str] = set()  # in-memory dedup within sweep

    for source in sources:
        # Cadence check: skip if not yet due.
        next_eligible = _cadence_skip(conn, source.name, source.cadence_minutes, start_unix)
        if next_eligible is not None:
            per_source.append(PerSourceResult(
                name=source.name,
                status="skipped",
                items_fetched=0,
                items_after_dedup=0,
                items_inserted=0,
                error_detail=f"cadence_throttled: next_eligible_at_unix={next_eligible}",
            ))
            continue

        since = _since_unix_for_source(conn, source.name, start_unix)
        fetch_result: FetchResult = source.fetch(since)

        items_fetched = len(fetch_result.items)
        items_after_dedup = 0
        items_inserted = 0

        if fetch_result.status in ("ok", "partial"):
            for item in fetch_result.items:
                dedupe_hash = compute_dedupe_hash(item.headline)
                if dedupe_hash in seen_dedup_hashes:
                    continue
                if _dedupe_hash_exists(conn, dedupe_hash, dedup_window_start):
                    seen_dedup_hashes.add(dedupe_hash)
                    continue
                items_after_dedup += 1
                seen_dedup_hashes.add(dedupe_hash)

                tag_rows: list[tuple[str, str]] = []
                for regs in theme_regexes:
                    confidence = _tag_for_theme(item.headline, regs)
                    if confidence is not None:
                        tag_rows.append((regs.theme_id, confidence))

                hid = _headline_id(item.headline, source.name)
                # Merge source-provided tickers with extracted ones (Step 0).
                if tracked_tickers is not None:
                    extracted = tracked_tickers.extract(item.headline)
                    merged_tickers = sorted(set(item.tickers) | set(extracted))
                else:
                    merged_tickers = list(item.tickers)
                try:
                    tags_inserted = _insert_headline_and_tags(
                        conn,
                        headline_id=hid,
                        source_name=source.name,
                        item=item,
                        tickers=merged_tickers,
                        dedupe_hash=dedupe_hash,
                        fetched_at_unix=start_unix,
                        fetched_at_iso=start_iso,
                        tag_rows=tag_rows,
                    )
                except sqlite3.IntegrityError as exc:
                    # PK collision (same source emits identical headline)
                    # is handled silently — dedupe_hash should have caught
                    # it; if not, log and skip.
                    _LOG.warning(
                        "skipping duplicate headline insert (source=%s): %s",
                        source.name, exc,
                    )
                    continue
                items_inserted += 1
                theme_tags_inserted_total += tags_inserted
                headlines_inserted_total += 1

        _update_source_health(
            conn,
            source_name=source.name,
            status=fetch_result.status,
            error_detail=fetch_result.error_detail,
            now_unix=start_unix,
            now_iso=start_iso,
        )

        per_source.append(PerSourceResult(
            name=source.name,
            status=fetch_result.status,
            items_fetched=items_fetched,
            items_after_dedup=items_after_dedup,
            items_inserted=items_inserted,
            error_detail=fetch_result.error_detail,
        ))

    duration_ms = max(0, int((time.perf_counter() - start_perf) * 1000))
    sources_succeeded = sum(1 for s in per_source if s.status == "ok")
    sources_skipped = sum(1 for s in per_source if s.status == "skipped")
    sources_failed = sum(1 for s in per_source if s.status not in ("ok", "skipped"))
    # sources_attempted = sources we actually called fetch() on
    sources_attempted = len(per_source) - sources_skipped

    return ScrapeResult(
        started_at_unix=start_unix,
        started_at=start_iso,
        duration_ms=duration_ms,
        sources_attempted=sources_attempted,
        sources_succeeded=sources_succeeded,
        sources_failed=sources_failed,
        sources_skipped=sources_skipped,
        per_source=per_source,
        headlines_inserted_total=headlines_inserted_total,
        theme_tags_inserted_total=theme_tags_inserted_total,
        themes_active=active_theme_ids,
    )


def write_heartbeat(
    conn: sqlite3.Connection,
    *,
    status: str,
    duration_ms: int,
    error_detail: str | None = None,
) -> None:
    """Convenience wrapper used by both the orchestrator and the CLI handler."""
    record_heartbeat(
        conn,
        component="scrape",
        status=status,
        duration_ms=duration_ms,
        error_detail=error_detail,
    )


__all__ = [
    "DEDUP_WINDOW_S",
    "DEFAULT_SINCE_LOOKBACK_S",
    "PerSourceResult",
    "ScrapeResult",
    "run_scrape",
    "write_heartbeat",
]
