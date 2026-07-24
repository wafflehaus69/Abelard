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
from typing import Any, Callable, Iterable, Literal

from pathlib import Path

from ..db import record_heartbeat, to_json_column, transaction
from ..lang import classify_language
from ..sources.base import FetchedItem, FetchResult, SourcePlugin
from ..sources.noise_filter_log import write_filter_entry
from ..sources.telegram import PLUGIN_PREFIX as TELEGRAM_PLUGIN_PREFIX
from ..theme_config import ThemeConfig
from ..translation import run_translation_pass
from .content_blocklist import classify_blocklist
from .cross_source_log import write_observation as write_cross_source_observation
from .dedup import compute_dedupe_hash
from .sports_filter import classify_sports
from .ticker_extract import TrackedTickers, log_tracked_ticker_match


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
    # Counts of items dropped by the ingest content filters before dedup
    # (defaults 0 keep every existing keyword construction valid).
    items_sports_filtered: int = 0
    items_blocklist_filtered: int = 0


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
    # Totals dropped by the ingest content filters this sweep (defaults 0 keep
    # existing keyword constructions valid).
    sports_filtered_total: int = 0
    blocklist_filtered_total: int = 0


# ---------- compiled theme regexes ----------


@dataclass(frozen=True)
class _ThemeRegexes:
    theme_id: str
    primary: re.Pattern[str]
    secondary: re.Pattern[str] | None
    exclusion: re.Pattern[str] | None


_CONSECUTIVE_UPPERCASE_RE = re.compile(r"[A-Z]{2}")


def _has_consecutive_uppercase(keyword: str) -> bool:
    """True iff `keyword` contains 2+ consecutive uppercase letters.

    Drives case-sensitivity classification in `_join_keywords_wb`. The
    heuristic catches the class of acronyms-that-collide-with-English:
    SWIFT vs swift, MiCA vs mica, USDC, NATO, FOMC, IRGC, etc.
    Common-word keywords ("stablecoin", "rate cut", "Federal Reserve",
    "Circle Internet") have no 2-consecutive-cap run and stay
    case-insensitive.
    """
    return bool(_CONSECUTIVE_UPPERCASE_RE.search(keyword))


def _join_keywords_wb(keywords: list[str]) -> str:
    """Build a word-boundary-wrapped alternation regex with per-keyword case rules.

    Pass C Step 1 added `\\b...\\b` boundaries so short acronyms (MiCA, QT, BIS,
    RWA) don't substring-match inside unrelated words (chemical, antique,
    business, drawer). Pass D follow-on (this change) adds per-keyword
    case-sensitivity: any keyword containing 2+ consecutive uppercase letters
    gets wrapped in `(?-i:...)` to opt out of the IGNORECASE flag the outer
    compile applies. This solves the "SWIFT matches swift" class of false
    positives without a YAML schema change.

    Multi-word keywords ("rate cut", "Federal Reserve") are similarly bounded
    at the phrase start/end, not within. `re.escape` handles special chars in
    keywords (hyphens, dots, dollar signs).

    Apostrophe edge case verified: `\\bIran\\b` still matches `Iran` in
    `Iran's` because `'` is non-word, providing the right boundary.
    """
    parts: list[str] = []
    for k in keywords:
        escaped = re.escape(k)
        if _has_consecutive_uppercase(k):
            # Opt out of the IGNORECASE flag the outer compile applies.
            parts.append(rf"(?-i:\b{escaped}\b)")
        else:
            parts.append(rf"\b{escaped}\b")
    return "|".join(parts)


def _compile_theme_regexes(themes: Iterable[ThemeConfig]) -> list[_ThemeRegexes]:
    out: list[_ThemeRegexes] = []
    for theme in themes:
        primary = re.compile(_join_keywords_wb(theme.keywords.primary), re.IGNORECASE)
        secondary = (
            re.compile(_join_keywords_wb(theme.keywords.secondary), re.IGNORECASE)
            if theme.keywords.secondary
            else None
        )
        exclusion = (
            re.compile(_join_keywords_wb(theme.keywords.exclusions), re.IGNORECASE)
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


def _log_cross_source(
    log_path: Path,
    *,
    dedupe_hash: str,
    first_source: str,
    first_observed_at_unix: int,
    second_source: str,
    second_observed_at_unix: int,
    headline: str,
) -> None:
    """Wrap cross_source_log.write_observation in a try/log-on-failure.

    Cross-source observation logging is best-effort instrumentation —
    a disk-full or permission error on the log file must NOT abort the
    scrape. Failures surface as WARN, the rest of the sweep proceeds.
    """
    try:
        write_cross_source_observation(
            log_path,
            dedupe_hash=dedupe_hash,
            first_source=first_source,
            first_observed_at_unix=first_observed_at_unix,
            second_source=second_source,
            second_observed_at_unix=second_observed_at_unix,
            headline=headline,
        )
    except OSError as exc:
        _LOG.warning(
            "cross_source_log append failed (%s -> %s, dedupe_hash=%s): %s",
            first_source, second_source, dedupe_hash, exc,
        )


def _dedupe_hash_first_observation(
    conn: sqlite3.Connection, dedupe_hash: str, window_start_unix: int,
) -> tuple[str, int] | None:
    """Look up the FIRST observation of `dedupe_hash` within the dedup window.

    Pass D foundation (cross-source log): when a window-dup is detected
    the orchestrator needs to know who observed the headline first so
    it can emit a cross_source_log entry if the current source differs.
    Returns (source, fetched_at_unix) or None if not found.
    """
    row = conn.execute(
        "SELECT source, fetched_at_unix FROM headlines "
        "WHERE dedupe_hash = ? AND fetched_at_unix >= ? "
        "ORDER BY fetched_at_unix ASC "
        "LIMIT 1",
        (dedupe_hash, window_start_unix),
    ).fetchone()
    if row is None:
        return None
    return (row["source"], int(row["fetched_at_unix"]))


def _update_source_health(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    status: str,
    error_detail: str | None,
    now_unix: int,
    now_iso: str,
    ingested_high_watermark_unix: int | None = None,
) -> None:
    """Upsert source_health row according to the failure-counter rules.

    Counter policy (Pass A flag #5):
      - ok WITH ingested items: advance last_successful_fetch to the NEWEST
        ingested item's published_at (`ingested_high_watermark_unix`), not to
        `now`; update last_attempt; counter=0
      - ok with 0 items:  last_attempt only, watermark PRESERVED; counter=0
      - partial:          last_attempt only, watermark preserved; counter=0
      - error/rate_limited: last_attempt only, watermark preserved; counter++

    Watermark discipline (2026-07-07 footgun #1): the fetch watermark
    (`last_successful_fetch_unix`, read back by `_since_unix_for_source`)
    advances ONLY on an ok fetch that actually ingested content, and only to
    the newest ingested item's timestamp. Advancing on an empty ok fetch
    (a non-bozo empty/malformed body still returns status="ok" with 0 items)
    would push the watermark past a window that had real content -> permanent
    silent skip. Advancing to `now` on a non-empty fetch could likewise skip
    content published after the newest item but not yet in the feed. A quiet
    source keeps its last real-item timestamp (not stuck: the 72h dedup layer
    stops re-ingest of already-seen items, and the next real item advances it).
    """
    is_success = status == "ok"
    resets_counter = status in ("ok", "partial")
    advance_watermark = is_success and ingested_high_watermark_unix is not None
    existing = conn.execute(
        "SELECT consecutive_failure_count FROM source_health WHERE source = ?",
        (source_name,),
    ).fetchone()
    prior_count = existing["consecutive_failure_count"] if existing else 0
    new_count = 0 if resets_counter else (prior_count + 1)

    # Advance the watermark only on ok-with-ingestion, to the newest item's
    # published_at. Otherwise (ok-but-empty, or any non-ok) preserve prior.
    if advance_watermark:
        last_success_unix: int | None = ingested_high_watermark_unix
        last_success_iso: str | None = _iso_from_unix(ingested_high_watermark_unix)
    else:
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
    language: str | None = None,
    headline_en: str | None = None,
) -> int:
    """Insert one headline + N theme tags atomically. Returns tag count inserted.

    `tickers` is the orchestrator-computed merge of (source-provided
    tickers from FetchedItem.tickers) ∪ (extracted tickers from the
    headline text via TrackedTickers). Lands in the existing
    `headlines.tickers_json` column.

    `language`: classified language label. If None, computed in-line via
    classify_language(). Pass F orchestrator passes pre-computed value
    to avoid double-classification.

    `headline_en`: translated English text (Pass F). None when row needs
    no translation (`language == 'en'`) OR when translation failed and
    row sits in pending queue for next-cycle retry. Downstream consumers
    read COALESCE(headline_en, headline).
    """
    # Task 2 (2026-05-27): per-row language classification. Single call
    # site by default — every headline regardless of source goes through
    # this insert path, so all rows land with non-null `language`. Pass F
    # (2026-05-28) added the `language` kwarg so the orchestrator can
    # pre-classify and pass through to avoid double-computation; if
    # caller doesn't supply, we still classify here.
    if language is None:
        language = classify_language(item.headline)
    with transaction(conn):
        conn.execute(
            "INSERT INTO headlines "
            "(headline_id, source, raw_source, headline, url, "
            " published_at_unix, published_at, fetched_at_unix, fetched_at, "
            " dedupe_hash, tickers_json, entities_json, language, headline_en) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                language,
                headline_en,
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


# ---------- Pass F translation helpers ----------


def _parse_telegram_msg_id(url: str | None) -> int | None:
    """Parse the integer msg_id from a Telegram URL like
    `https://t.me/<channel>/<msg_id>`.

    Returns None if the URL doesn't match the expected shape — translation
    deferral falls back to in-line insert with headline_en=NULL.
    """
    if not url:
        return None
    if "t.me/" not in url:
        return None
    try:
        tail = url.rstrip("/").rsplit("/", 1)[-1]
        return int(tail)
    except (ValueError, IndexError):
        return None


def _apply_pending_translations(
    conn: sqlite3.Connection,
    *,
    source: SourcePlugin,
    channel_username: str,
    pending: list[dict[str, Any]],
    translation_credentials: tuple[int, str, str],
    translation_source: str,
    translation_batch_size: int,
    theme_regexes: Iterable[Any],
    start_unix: int,
    start_iso: str,
) -> tuple[int, int]:
    """Translate this source's pending ru/mixed rows, then tag+insert.

    Returns (items_inserted_count, theme_tags_inserted_count) — both
    accumulated into the per-source loop's accounting.

    Failure isolation: per-row translation failures (rate_limited,
    network_error, message_deleted, premium_required, translation_error)
    insert with headline_en=NULL; the row sits in the pending queue
    (WHERE language != 'en' AND headline_en IS NULL) for next-cycle
    retry. See translation/runner.py docstring for the re-queue contract.

    Translation outage at the channel level (entire batch fails with
    network_error) inserts ALL pending rows with headline_en=NULL.
    The daemon does NOT abort the cycle; downstream consumers fall back
    to original (Russian) headline via COALESCE(headline_en, headline)
    and English theme keywords produce zero tags on that path —
    acceptable degradation until next-cycle translation succeeds.
    """
    api_id, api_hash, session_string = translation_credentials
    msg_ids = [p["msg_id"] for p in pending]
    originals = {p["msg_id"]: p["item"].headline for p in pending}

    try:
        translations = run_translation_pass(
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_string,
            pending_by_channel={channel_username: list(zip(msg_ids, [originals[m] for m in msg_ids]))},
            batch_size=translation_batch_size,
            translation_source=translation_source,
        )
    except NotImplementedError as exc:
        # DeepL stub raised — translation_source was flipped to 'deepl'
        # in config but DeepL implementation isn't ready. Insert all
        # pending rows with headline_en=NULL.
        _LOG.critical(
            "translation_source='deepl' but stub raised NotImplementedError "
            "(channel=%s pending=%d): %s. Inserting with headline_en=NULL; "
            "rows sit in pending queue.",
            channel_username, len(pending), exc,
        )
        translations = {}
    except Exception as exc:  # noqa: BLE001 — translation must never abort scrape
        _LOG.warning(
            "translation pass raised unexpected exception (channel=%s pending=%d): "
            "%s: %s. Inserting with headline_en=NULL.",
            channel_username, len(pending), type(exc).__name__, exc,
        )
        translations = {}

    items_inserted = 0
    theme_tags_inserted = 0
    for p in pending:
        msg_id = p["msg_id"]
        result = translations.get((channel_username, msg_id))
        # Determine the text used for theme tagging.
        # Per Mando's α/γ contract: tag against COALESCE(headline_en,
        # headline) so translated rows tag correctly; failed-translation
        # rows fall back to the original (Russian) text and tag zero
        # English keywords (acceptable degradation).
        translated_text: str | None = None
        if result is not None and result.status == "ok" and result.translated_text:
            translated_text = result.translated_text
        tagging_text = translated_text or p["item"].headline

        tag_rows: list[tuple[str, str]] = []
        for regs in theme_regexes:
            confidence = _tag_for_theme(tagging_text, regs)
            if confidence is not None:
                tag_rows.append((regs.theme_id, confidence))

        try:
            tags_inserted = _insert_headline_and_tags(
                conn,
                headline_id=p["hid"],
                source_name=source.name,
                item=p["item"],
                tickers=p["merged_tickers"],
                dedupe_hash=p["dedupe_hash"],
                fetched_at_unix=start_unix,
                fetched_at_iso=start_iso,
                tag_rows=tag_rows,
                language=p["language"],
                headline_en=translated_text,
            )
        except sqlite3.IntegrityError as exc:
            _LOG.warning(
                "skipping duplicate headline insert post-translation (source=%s): %s",
                source.name, exc,
            )
            continue
        items_inserted += 1
        theme_tags_inserted += tags_inserted
    return items_inserted, theme_tags_inserted


# ---------- main entry point ----------


def run_scrape(
    conn: sqlite3.Connection,
    sources: list[SourcePlugin],
    themes: list[ThemeConfig],
    *,
    now_unix: int | None = None,
    tracked_tickers: TrackedTickers | None = None,
    cross_source_log_path: Path | None = None,
    ingest_filter_log_path: Path | None = None,
    translation_credentials: tuple[int, str, str] | None = None,
    translation_source: str = "telegram_native",
    translation_batch_size: int = 10,
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

    `cross_source_log_path`, when provided, enables per-sweep emission
    of cross-source duplicate observations (Pass D foundation, Pass C
    Step 11+1 2026-05-17). When a dedupe_hash arrives from a SECOND
    source within the dedup window — either earlier in this sweep or
    in the DB from a prior sweep — an entry is appended to the log
    capturing both observations and the latency. Same-source dupes are
    NOT logged. When None, behavior matches Pass A/B (silent dedup).
    """
    start_unix, start_iso = _now_pair(now_unix)
    start_perf = time.perf_counter()

    theme_regexes = _compile_theme_regexes(themes)
    active_theme_ids = sorted(t.theme_id for t in themes)
    dedup_window_start = start_unix - DEDUP_WINDOW_S

    per_source: list[PerSourceResult] = []
    headlines_inserted_total = 0
    theme_tags_inserted_total = 0
    sports_filtered_total = 0
    blocklist_filtered_total = 0
    # In-memory dedup within sweep. Promoted from set[str] to
    # dict[str, (source, fetched_at_unix)] so cross-source dups can be
    # logged with the first observer's identity.
    seen_dedup_observations: dict[str, tuple[str, int]] = {}

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
        items_sports_filtered = 0
        items_blocklist_filtered = 0

        # Pass F (2026-05-28): defer ru/mixed Telegram rows so translation
        # can run as a single batched call per source. en rows still
        # insert in-line (no translation needed). Each deferred entry
        # carries everything _insert_headline_and_tags needs PLUS the
        # original-text + language for the post-translation tag step.
        #
        # See translation/runner.py's run_translation_pass() docstring
        # for the rate-limited re-queue semantics: translation failures
        # leave headline_en=NULL; the row inserts and sits in the
        # pending queue (WHERE language != 'en' AND headline_en IS NULL)
        # for the NEXT scrape cycle or db backfill-translation CLI run.
        # Failures DO NOT block insertion of other rows.
        is_telegram_source = source.name.startswith(TELEGRAM_PLUGIN_PREFIX)
        pending_translation: list[dict[str, Any]] = []

        if fetch_result.status in ("ok", "partial"):
            for item in fetch_result.items:
                # Ingest content filters (Mando 2026-07-15): drop headlines
                # BEFORE dedup/tag/insert so they never reach the corpus, the
                # attention counter, or the frequency diagnostic. Two filters:
                #   - editorial blocklist: UNCONDITIONAL (forbidden conspiracy/
                #     ideological terms that must never appear in a report),
                #   - sports: pure-sports with an internal markets/geo guard.
                # Blocklist takes precedence (checked first). Every drop is
                # recorded in the noise-filter audit trail with a kind: prefix.
                block_term = classify_blocklist(item.headline)
                sports_term = None if block_term else classify_sports(item.headline)
                if block_term is not None or sports_term is not None:
                    if block_term is not None:
                        kind, term = "blocklist", block_term
                        items_blocklist_filtered += 1
                        blocklist_filtered_total += 1
                    else:
                        kind, term = "sports", sports_term
                        items_sports_filtered += 1
                        sports_filtered_total += 1
                    _LOG.info(
                        "%s-filtered (%s): matched %r | %s",
                        kind, source.name, term, item.headline[:120],
                    )
                    if ingest_filter_log_path is not None:
                        try:
                            write_filter_entry(
                                ingest_filter_log_path,
                                channel=source.name,
                                msg_id=item.source_item_id,
                                matched_pattern=f"{kind}:{term}",
                                full_text=item.headline,
                                now_unix=start_unix,
                            )
                        except Exception as exc:  # noqa: BLE001 — audit must never abort scrape
                            _LOG.warning(
                                "%s-filter audit write failed (%s): %s: %s",
                                kind, source.name, type(exc).__name__, exc,
                            )
                    continue

                dedupe_hash = compute_dedupe_hash(item.headline)

                # In-sweep dedup. If the hash was seen earlier in this
                # sweep, drop the second observation — but log the
                # cross-source delta if the original observer was a
                # different source.
                first_in_sweep = seen_dedup_observations.get(dedupe_hash)
                if first_in_sweep is not None:
                    first_source, first_observed_unix = first_in_sweep
                    if (
                        cross_source_log_path is not None
                        and first_source != source.name
                    ):
                        _log_cross_source(
                            cross_source_log_path,
                            dedupe_hash=dedupe_hash,
                            first_source=first_source,
                            first_observed_at_unix=first_observed_unix,
                            second_source=source.name,
                            second_observed_at_unix=start_unix,
                            headline=item.headline,
                        )
                    continue

                # Window dedup. If the hash exists in the DB within
                # the dedup window from a prior sweep, also drop —
                # and log if the prior observer was a different source.
                if cross_source_log_path is not None:
                    prior = _dedupe_hash_first_observation(
                        conn, dedupe_hash, dedup_window_start,
                    )
                    if prior is not None:
                        prior_source, prior_fetched_unix = prior
                        if prior_source != source.name:
                            _log_cross_source(
                                cross_source_log_path,
                                dedupe_hash=dedupe_hash,
                                first_source=prior_source,
                                first_observed_at_unix=prior_fetched_unix,
                                second_source=source.name,
                                second_observed_at_unix=start_unix,
                                headline=item.headline,
                            )
                        seen_dedup_observations[dedupe_hash] = (
                            prior_source, prior_fetched_unix,
                        )
                        continue
                else:
                    if _dedupe_hash_exists(conn, dedupe_hash, dedup_window_start):
                        seen_dedup_observations[dedupe_hash] = (source.name, start_unix)
                        continue

                items_after_dedup += 1
                seen_dedup_observations[dedupe_hash] = (source.name, start_unix)

                hid = _headline_id(item.headline, source.name)
                # Merge source-provided tickers with extracted ones (Step 0).
                if tracked_tickers is not None:
                    extracted = tracked_tickers.extract(item.headline)
                    merged_tickers = sorted(set(item.tickers) | set(extracted))
                    # Calibration instrumentation: log each tracked-list match
                    # (excluding cashtags) at DEBUG level for per-channel
                    # false-positive measurement. See log_tracked_ticker_match
                    # docstring for the deferred-Option-E rationale.
                    for ticker, pos in tracked_tickers.find_tracked_matches(item.headline):
                        log_tracked_ticker_match(
                            source_channel=source.name,
                            headline_id=hid,
                            ticker=ticker,
                            headline=item.headline,
                            match_position=pos,
                        )
                else:
                    merged_tickers = list(item.tickers)

                # Classify language up-front so we can branch on it for
                # translation deferral. Was previously computed inside
                # _insert_headline_and_tags (Task 2 wire-up); the call
                # is still cheap (microseconds) and avoiding double-
                # classification keeps things tidy.
                language = classify_language(item.headline)

                should_defer = (
                    is_telegram_source
                    and language != "en"
                    and translation_credentials is not None
                    and _parse_telegram_msg_id(item.url) is not None
                )
                if should_defer:
                    pending_translation.append({
                        "item": item,
                        "dedupe_hash": dedupe_hash,
                        "hid": hid,
                        "merged_tickers": merged_tickers,
                        "language": language,
                        "msg_id": _parse_telegram_msg_id(item.url),
                    })
                    continue

                # In-line tag + insert (en path, OR ru/mixed when no
                # translation credentials available → degrades to
                # inserting with headline_en=NULL + Russian-content
                # tagging which produces zero tags against English
                # keywords; row sits in pending queue, db backfill-
                # translation can rescue it later).
                tag_rows: list[tuple[str, str]] = []
                for regs in theme_regexes:
                    confidence = _tag_for_theme(item.headline, regs)
                    if confidence is not None:
                        tag_rows.append((regs.theme_id, confidence))

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
                        language=language,
                        headline_en=None,
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

        # ---- Pass F per-source batched translation ----
        # All ru/mixed Telegram items from this source were deferred
        # above. Run ONE batched translation call against this source's
        # channel, then tag+insert each deferred row with its translated
        # text (or NULL if translation failed — row sits in pending
        # queue for next-cycle retry; see translation/runner.py
        # docstring for the re-queue contract).
        if pending_translation and translation_credentials is not None:
            channel = source.name[len(TELEGRAM_PLUGIN_PREFIX):]
            tx_inserts, tx_tags = _apply_pending_translations(
                conn,
                source=source,
                channel_username=channel,
                pending=pending_translation,
                translation_credentials=translation_credentials,
                translation_source=translation_source,
                translation_batch_size=translation_batch_size,
                theme_regexes=theme_regexes,
                start_unix=start_unix,
                start_iso=start_iso,
            )
            items_inserted += tx_inserts
            theme_tags_inserted_total += tx_tags
            headlines_inserted_total += tx_inserts

        # Newest published_at among the items this fetch returned (already
        # since-filtered by the source). None when 0 items -> watermark holds.
        ingested_high_watermark_unix = max(
            (it.published_at_unix for it in fetch_result.items),
            default=None,
        )
        _update_source_health(
            conn,
            source_name=source.name,
            status=fetch_result.status,
            error_detail=fetch_result.error_detail,
            now_unix=start_unix,
            now_iso=start_iso,
            ingested_high_watermark_unix=ingested_high_watermark_unix,
        )

        per_source.append(PerSourceResult(
            name=source.name,
            status=fetch_result.status,
            items_fetched=items_fetched,
            items_after_dedup=items_after_dedup,
            items_inserted=items_inserted,
            error_detail=fetch_result.error_detail,
            items_sports_filtered=items_sports_filtered,
            items_blocklist_filtered=items_blocklist_filtered,
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
        sports_filtered_total=sports_filtered_total,
        blocklist_filtered_total=blocklist_filtered_total,
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


# ---------------------------------------------------------------------------
# Full Brief Stage 2a-i sub-step B (2026-05-29): scrape_cycle()
#
# Pure-callable cycle wrapper mirroring SynthesizeResult discipline. The
# CLI handler (`_handle_scrape`) and the Full Brief orchestrator at
# Stage 2a-ii are both callers; both want a structured result rather
# than a CLI envelope dict.
#
# The existing `run_scrape` stays as the lower-level "execute one sweep"
# primitive. `scrape_cycle` composes it with heartbeat write + optional
# auto-attention callback into a structured result.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScrapeCycleResult:
    """Structured return from `scrape_cycle` — mirrors SynthesizeResult shape.

    Status discriminator:
      - "ok": run_scrape completed (per-source partial failures don't
        propagate to status; they live in `scrape_result.per_source` and
        the CLI envelope's data_completeness field).
      - "scrape_failed": run_scrape raised an orchestration-level error
        (e.g., DB connection broken mid-sweep). Heartbeat record written
        with error_detail.

    `attention_outcome` is populated when status="ok" AND an
    attention_callback was provided. The callback's exceptions are
    captured into an error-shaped dict here rather than propagating,
    matching the existing _handle_scrape attention-never-kills-scrape
    discipline.
    """

    status: Literal["ok", "scrape_failed"]
    started_at_unix: int

    # Populated when status="ok":
    scrape_result: ScrapeResult | None = None
    attention_outcome: dict[str, Any] | None = None

    # Populated when status="scrape_failed":
    reason: str | None = None


def scrape_cycle(
    *,
    conn: sqlite3.Connection,
    sources: list[SourcePlugin],
    themes: list[ThemeConfig],
    tracked_tickers: Any | None = None,
    cross_source_log_path: Path | None = None,
    ingest_filter_log_path: Path | None = None,
    translation_credentials: tuple[int, str, str] | None = None,
    translation_source: str = "telegram_native",
    translation_batch_size: int = 10,
    attention_callback: Callable[[], dict[str, Any]] | None = None,
) -> ScrapeCycleResult:
    """Run one scrape sweep + heartbeat + optional auto-attention follow-on.

    Pure callable hoisted from cli._handle_scrape composition logic. The
    CLI handler at cli.py becomes a thin argparse wrapper that:

      1. Resolves config + themes + DB + sources + tracked_tickers +
         translation config from args + cfg
      2. Calls scrape_cycle with explicit kwargs
      3. Formats the CLI envelope from the structured result

    The Full Brief orchestrator at Stage 2a-ii is the second caller — it
    composes scrape_cycle output with synthesize_window output without
    going through the CLI argparse path.

    Args:
        conn: SQLite connection. Caller owns lifecycle.
        sources: pre-built list of SourcePlugin instances.
        themes: pre-loaded ThemeConfig list (already filtered to active).
        tracked_tickers: optional TrackedTickers for ticker extraction.
        cross_source_log_path: optional path for cross-source dedup log.
        translation_credentials: Telegram API tuple for translation, or
            None to disable.
        translation_source: translation backend identifier.
        translation_batch_size: batch size for per-source translation.
        attention_callback: zero-arg callable returning the attention
            outcome dict. None disables the auto-attention follow-on.
            Exceptions from the callback are captured into an
            error-shaped attention_outcome dict; they do NOT propagate.

    Returns:
        ScrapeCycleResult with status discriminator. Never raises for
        normal scrape failures — orchestration errors during run_scrape
        surface as status="scrape_failed" with reason populated.
    """
    started_at_unix = int(time.time())

    try:
        result = run_scrape(
            conn, sources, themes,
            tracked_tickers=tracked_tickers,
            cross_source_log_path=cross_source_log_path,
            ingest_filter_log_path=ingest_filter_log_path,
            translation_credentials=translation_credentials,
            translation_source=translation_source,
            translation_batch_size=translation_batch_size,
        )
    except Exception as exc:  # noqa: BLE001 — capture all orchestration errors
        try:
            write_heartbeat(conn, status="error", duration_ms=0, error_detail=str(exc))
        except Exception as hb_exc:  # noqa: BLE001 — heartbeat itself can fail on DB issue
            logging.getLogger("news_watch_daemon.scrape.orchestrator").warning(
                "failed to write error heartbeat after scrape failure: %s: %s",
                type(hb_exc).__name__, hb_exc,
            )
        return ScrapeCycleResult(
            status="scrape_failed",
            started_at_unix=started_at_unix,
            reason=f"{type(exc).__name__}: {exc}",
        )

    # Heartbeat: scrape completed (independent of per-source outcomes).
    try:
        write_heartbeat(
            conn, status="ok",
            duration_ms=result.duration_ms, error_detail=None,
        )
    except Exception as hb_exc:  # noqa: BLE001 — heartbeat failure is non-fatal
        logging.getLogger("news_watch_daemon.scrape.orchestrator").warning(
            "failed to write ok heartbeat after scrape: %s: %s",
            type(hb_exc).__name__, hb_exc,
        )

    # Optional auto-attention follow-on (Pass E chain inside scrape).
    attention_outcome: dict[str, Any] | None = None
    if attention_callback is not None:
        try:
            attention_outcome = attention_callback()
        except Exception as exc:  # noqa: BLE001 — never let attention kill scrape
            _log = logging.getLogger("news_watch_daemon.scrape.orchestrator")
            _log.warning(
                "attention follow-on raised: %s: %s", type(exc).__name__, exc,
            )
            attention_outcome = {
                "status": "error",
                "reason": f"unhandled_exception: {type(exc).__name__}: {exc}",
            }

    return ScrapeCycleResult(
        status="ok",
        started_at_unix=started_at_unix,
        scrape_result=result,
        attention_outcome=attention_outcome,
    )


__all__ = [
    "DEDUP_WINDOW_S",
    "DEFAULT_SINCE_LOOKBACK_S",
    "PerSourceResult",
    "ScrapeCycleResult",
    "ScrapeResult",
    "run_scrape",
    "scrape_cycle",
    "write_heartbeat",
]
