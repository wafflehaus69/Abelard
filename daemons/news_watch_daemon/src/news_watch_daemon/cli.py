"""CLI entry point for the News Watch Daemon.

Invocation contract (what Abelard can rely on):

  - Exactly one JSON envelope is written to stdout per invocation.
  - Logs, warnings, and tracebacks go to stderr, never stdout.
  - Exit 0 iff `envelope.status == "ok"`. Stub commands return
    `status="error"` + a `not_implemented` warning and exit 1.

Foundation pass implements db / themes / status leaves; scrape /
synthesize / alert / query leaves are stubs that surface a structured
`not_implemented` warning so Abelard can plan around them.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import Config, ConfigError, configure_logging, load_env_file
from .db import (
    connect,
    init_db,
    list_themes,
    read_heartbeats,
    read_source_health,
    schema_version,
    transaction,
    upsert_themes,
)
from .envelope import build_error, build_ok, make_warning
from .lang import classify_language
from .http_client import HttpClient
from .scrape.factory import build_sources
from .scrape.orchestrator import (
    PerSourceResult,
    ScrapeResult,
    _parse_telegram_msg_id,
    scrape_cycle,
)
from .scrape.ticker_extract import TickerExtractError, load_tracked_tickers
from .alert.factory import AlertSinkFactoryError, build_alert_sink
from .attention.brief_schema import AttentionBrief
from .translation import (
    TranslationConfigError,
    load_translation_config,
    run_translation_pass,
)
from .synthesize.archive import ArchiveError, brief_path, list_brief_ids, read_brief
from .synthesize.brief import (
    Brief,
    Dispatch,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)
from .synthesize.config import (
    SynthesisConfigError,
    SynthesisDaemonConfig,
    load_synthesis_config,
)
from .synthesize.proposals_store import (
    ProposalsStoreError,
    append_resolved,
    find_proposal,
    read_pending,
    remove_proposal,
)
from .synthesize.theme_mutator import ThemeMutationError, apply_proposal_to_theme
from .synthesize.synthesize import (
    SynthesisError,
    SynthesizeResult,
    build_anthropic_client,
    synthesize_window,
)
# Full Brief Stage 2b-ii: orchestrator + render for the full-brief subcommand.
from .fullbrief.brief import FullBriefEnvelope
from .fullbrief.loader import FullBriefLoadError, load_full_brief_from_path
from .fullbrief.orchestrator import assemble_full_brief
from .fullbrief.pdf import PdfRenderError, render_full_brief_pdf
from .fullbrief.render import render_full_brief
from .synthesize.trigger_log import read_last_n as read_trigger_log_last_n
from .theme_config import ThemeLoadError, load_all_themes
from .attention.orchestrator import run_attention_cycle


# ---------- parser ------------------------------------------------------


def _add_brief_output_args(p: argparse.ArgumentParser) -> None:
    """Shared assembly + output args for `full-brief` and `run`.

    Kept in one place so the two commands never drift: both assemble the same
    envelope and offer the same output surface (window, scrape toggle, PDF/JSON
    side-writes, and the mutually-exclusive stdout mode). `run` layers the
    ensure-schema + ensure-themes steps on top; the assembly/output is identical.
    """
    p.add_argument(
        "--window-hours", type=int, default=24,
        help=(
            "Hours back to scan for Pass C + Pass E (1..168). Default 24. "
            "At non-24 values the FREQUENCY DIAGNOSTIC section surfaces a "
            "threshold-tuning warning per Adjustment 2."
        ),
    )
    p.add_argument(
        "--no-scrape", action="store_true",
        help=(
            "Skip the scrape step; run Pass C + Pass E against existing DB "
            "state. Default: scrape first. Useful for testing + re-running "
            "analysis on the same window."
        ),
    )
    p.add_argument(
        "--pdf",
        metavar="OUT.pdf",
        help=(
            "Also render the assembled brief to a PDF at this path (ReportLab), "
            "in the SAME pass — no separate read-brief call, no hunting for the "
            "artifact. Orthogonal to the stdout mode below. Fails loud (exit 1) "
            "on render error; never emits a zero-byte PDF."
        ),
    )
    p.add_argument(
        "--out",
        metavar="OUT.json",
        help=(
            "Also write the JSON envelope to this path, in addition to the "
            "archive. Convenience for landing the artifact somewhere "
            "predictable. Fails loud (exit 1) on write error."
        ),
    )
    # --quiet and --json-only are mutually exclusive per Q7 resolution.
    output_group = p.add_mutually_exclusive_group()
    output_group.add_argument(
        "--quiet", action="store_true",
        help=(
            "Suppress human-readable stdout rendering; only write the JSON "
            "artifact to disk. Mutually exclusive with --json-only."
        ),
    )
    output_group.add_argument(
        "--json-only", action="store_true",
        help=(
            "Print the JSON artifact to stdout instead of human-readable "
            "rendering. For downstream tooling consumption. Mutually "
            "exclusive with --quiet."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="news-watch-daemon",
        description=(
            "Narrative-state engine. Emits a JSON envelope on stdout; "
            "logs on stderr."
        ),
    )
    top = parser.add_subparsers(dest="command", required=True)

    # ---- leaf commands ----

    top.add_parser("scrape", help="Sweep all enabled sources for new headlines.")

    p_synth = top.add_parser("synthesize", help="Run synthesis for one or all themes.")
    p_synth.add_argument(
        "--theme",
        help=(
            "Force a pull-trigger synthesis for a single theme_id "
            "(bypasses the trigger gate). Default: event-trigger over all "
            "active themes."
        ),
    )
    p_synth.add_argument(
        "--window-hours", type=int, default=_DEFAULT_SYNTHESIS_WINDOW_HOURS,
        help=(
            "Hours back to scan headlines + recent briefs (1..168). "
            f"Default {_DEFAULT_SYNTHESIS_WINDOW_HOURS}."
        ),
    )
    p_synth.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Run the pipeline up to (but not including) the Sonnet call. "
            "Reports trigger decision + cluster count. No archive write, "
            "no dispatch, no LLM call."
        ),
    )

    # ---- attention (Pass E ATTENTION-driven synthesis) ----

    p_attn = top.add_parser(
        "attention",
        help=(
            "Run one ATTENTION cycle: word-frequency counter -> threshold gate "
            "-> per-term LLM call. Theme-blind by design; surfaces unknown-unknowns."
        ),
    )
    p_attn.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Run counter + threshold but skip the LLM call. Reports crossings "
            "(or top-5 candidates if none) without producing briefs."
        ),
    )
    p_attn.add_argument(
        "--top-candidates-limit", type=int, default=5,
        help=(
            "When zero terms cross threshold, surface this many near-miss "
            "candidates with their counts. Default 5."
        ),
    )

    # ---- full-brief (Stage 2b, Full Brief composition) ----

    p_fb = top.add_parser(
        "full-brief",
        help=(
            "Assemble a Full Brief composing Pass C theme-event synthesis + "
            "Pass E ATTENTION sweep + convergence + frequency diagnostic + "
            "Pass F footprint + cost telemetry. Writes artifact to disk; "
            "renders human-readable to stdout by default."
        ),
    )
    _add_brief_output_args(p_fb)

    # ---- run (one-pass operating cycle: ensure + full-brief, 2026-07-10) ----

    p_run = top.add_parser(
        "run",
        help=(
            "One-pass operating cycle: ensure schema + themes (both idempotent), "
            "then assemble a full brief (scrape + attention + synthesis) and "
            "optionally render a PDF — the single command a cron or operator runs "
            "cold-start. Accepts the same output flags as full-brief."
        ),
    )
    _add_brief_output_args(p_run)

    # ---- read-brief (reload + render a persisted Full Brief artifact) ----

    p_read_brief = top.add_parser(
        "read-brief",
        help=(
            "Reload a persisted Full Brief v1 JSON artifact from disk and render "
            "it to stdout — identical human-readable output to `full-brief` at "
            "generation time. No scrape, no LLM calls, no disk writes."
        ),
    )
    p_read_brief.add_argument(
        "path",
        help=(
            "Filesystem path to a persisted Full Brief artifact "
            "(nwd-fullbrief-*.json). Validated against the FullBriefEnvelope "
            "schema before rendering; malformed / wrong-type / missing files "
            "fail loudly with a nonzero exit."
        ),
    )
    p_read_brief.add_argument(
        "--pdf",
        metavar="OUT.pdf",
        help=(
            "Render the brief to a PDF at this path (ReportLab) instead of text "
            "to stdout. Same structured FullBriefEnvelope, Unicode-safe fonts. "
            "Fails loud on render error; never emits a zero-byte PDF."
        ),
    )

    # ---- alert-sink (sink verification) ----

    p_alert_sink = top.add_parser(
        "alert-sink", help="Verify the configured AlertSink transport.",
    )
    alert_sink_sub = p_alert_sink.add_subparsers(dest="alert_sink_action", required=True)
    p_as_test = alert_sink_sub.add_parser(
        "test",
        help="Send a synthetic test brief through the configured sink.",
    )
    p_as_test.add_argument(
        "--message",
        default=None,
        help="Custom narrative for the test brief (default: timestamped self-test).",
    )

    # ---- trigger-log (recent fires + suppressions) ----

    p_trigger_log = top.add_parser(
        "trigger-log", help="Inspect the append-only trigger-gate log.",
    )
    trigger_log_sub = p_trigger_log.add_subparsers(dest="trigger_log_action", required=True)
    p_tl_tail = trigger_log_sub.add_parser(
        "tail", help="Show the last N trigger-gate decisions (oldest-first).",
    )
    p_tl_tail.add_argument(
        "--limit", type=int, default=20,
        help="Number of entries to return (1..500). Default 20.",
    )

    top.add_parser("status", help="Report daemon component heartbeats and schema version.")

    top.add_parser(
        "doctor",
        help=(
            "Preflight-check env, paths, external deps, and DB state before a "
            "run. Read-only; surfaces every problem at once. Exit 1 if anything "
            "BLOCKS a run (no schema, no active themes, missing config, "
            "unwritable output dir); exit 0 if only non-blocking warnings."
        ),
    )

    # ---- themes (registry) ----

    p_themes = top.add_parser("themes", help="Theme registry management.")
    themes_sub = p_themes.add_subparsers(dest="themes_action", required=True)
    themes_sub.add_parser("list", help="List themes currently in the registry.")
    themes_sub.add_parser(
        "load",
        help="Parse YAML theme files and upsert them into the registry.",
    )

    # ---- headlines ----

    p_headlines = top.add_parser("headlines", help="Inspect ingested headlines.")
    headlines_sub = p_headlines.add_subparsers(dest="headlines_action", required=True)
    p_h_recent = headlines_sub.add_parser("recent", help="Recent headlines, optionally by theme.")
    p_h_recent.add_argument("--theme", help="Filter to one theme_id.")
    p_h_recent.add_argument(
        "--ticker",
        help=(
            "Filter to headlines whose tickers_json contains this symbol "
            "(matches both tracked-list and cashtag extractions)."
        ),
    )
    p_h_recent.add_argument(
        "--hours", type=int, default=24,
        help="Hours back to look (1..168). Default 24.",
    )
    p_h_recent.add_argument(
        "--limit", type=int, default=50,
        help="Maximum headlines to return (1..500). Default 50.",
    )

    # ---- briefs (Abelard read-access) ----

    p_briefs = top.add_parser("briefs", help="Inspect archived Briefs.")
    briefs_sub = p_briefs.add_subparsers(dest="briefs_action", required=True)
    p_b_list = briefs_sub.add_parser("list", help="List recent briefs (summary view).")
    p_b_list.add_argument(
        "--limit", type=int, default=30,
        help="Maximum briefs to return (1..500). Default 30.",
    )
    p_b_list.add_argument(
        "--theme",
        help="Filter to briefs whose themes_covered includes this theme_id.",
    )
    p_b_show = briefs_sub.add_parser("show", help="Show one brief's full payload.")
    p_b_show.add_argument("brief_id")

    # ---- proposals (drift watcher review) ----

    p_proposals = top.add_parser(
        "proposals", help="Review drift-watcher keyword proposals.",
    )
    proposals_sub = p_proposals.add_subparsers(dest="proposals_action", required=True)
    proposals_sub.add_parser("list", help="List all pending drift proposals.")
    p_pshow = proposals_sub.add_parser("show", help="Show one proposal in detail.")
    p_pshow.add_argument("proposal_id")
    p_papprove = proposals_sub.add_parser(
        "approve", help="Approve a proposal and append its keyword to the theme YAML.",
    )
    p_papprove.add_argument("proposal_id")
    p_papprove.add_argument(
        "--dry-run", action="store_true",
        help="Show what WOULD change without modifying disk.",
    )
    p_preject = proposals_sub.add_parser(
        "reject", help="Reject a proposal (remove from pending; log to resolved.jsonl).",
    )
    p_preject.add_argument("proposal_id")
    p_preject.add_argument(
        "--reason",
        help="Optional free-text rationale; recorded in resolved.jsonl.",
    )

    # ---- db (admin) ----

    p_db = top.add_parser("db", help="Database administration.")
    db_sub = p_db.add_subparsers(dest="db_action", required=True)
    db_sub.add_parser("init", help="Apply the initial schema.")
    db_sub.add_parser("migrate", help="Apply any pending migrations.")
    db_sub.add_parser(
        "backfill-language",
        help="Classify the language of headlines with NULL language (Task 2 / Pass F).",
    )
    p_backfill_tx = db_sub.add_parser(
        "backfill-translation",
        help=(
            "Translate headlines with non-English language and NULL headline_en "
            "(Pass F). Re-tags rows against translated text. Idempotent: re-runs "
            "naturally process zero pending rows."
        ),
    )
    p_backfill_tx.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Preview the queue without making translation API calls. Returns the "
            "by-source breakdown of pending rows so the operator can validate the "
            "queue before committing to the cost/rate-limit envelope."
        ),
    )
    p_retag = db_sub.add_parser(
        "retag",
        help=(
            "Re-evaluate existing headlines against CURRENT active theme configs "
            "and add any newly-matching theme tags. Purely additive (never deletes "
            "or alters existing tags or headlines). Idempotent via the "
            "(headline_id, theme_id) primary key + INSERT OR IGNORE. Re-run after "
            "any matcher/keyword change (e.g. the semis widen) to backfill history."
        ),
    )
    p_retag.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Report how many currently-untagged (headline, theme) pairs WOULD newly "
            "tag under current config, broken down by theme with samples, without "
            "writing anything. The gate: review the count before authorizing the write."
        ),
    )

    # ---- translate (Pass F manual one-shot) ----

    p_translate = top.add_parser(
        "translate",
        help=(
            "Manual translation pass against existing pending rows (Pass F). "
            "Equivalent to `db backfill-translation` but with optional source "
            "filter + limit for narrow re-runs after rate-limit recovery."
        ),
    )
    p_translate.add_argument(
        "--source",
        help=(
            "Restrict to a single source name (e.g. telegram:Ateobreaking). "
            "Default: all sources with pending rows."
        ),
    )
    p_translate.add_argument(
        "--limit", type=int, default=200,
        help="Maximum pending rows to process this invocation. Default 200.",
    )
    p_translate.add_argument(
        "--dry-run", action="store_true",
        help="Preview without API calls; same shape as `db backfill-translation --dry-run`.",
    )

    return parser


# ---------- leaf-path helpers ------------------------------------------

_NESTED_DEST = {
    "themes": "themes_action",
    "headlines": "headlines_action",
    "proposals": "proposals_action",
    "briefs": "briefs_action",
    "alert-sink": "alert_sink_action",
    "trigger-log": "trigger_log_action",
    "db": "db_action",
}


def command_path(args: argparse.Namespace) -> str:
    """Return the leaf path, e.g. 'db init', 'scrape', 'themes load'."""
    top = args.command
    nested = _NESTED_DEST.get(top)
    if nested is None:
        return top
    return f"{top} {getattr(args, nested)}"


# ---------- handlers: real --------------------------------------------


def _schema_not_ready(conn: sqlite3.Connection) -> dict[str, Any] | None:
    """Shared schema guard: the canonical 'no schema applied' error envelope if
    the DB is unmigrated, else None. Single source of truth for the message that
    seven handlers previously copy-pasted verbatim."""
    if schema_version(conn) == 0:
        return build_error(
            status="error",
            source="internal",
            detail="database has no schema applied. Run `news-watch-daemon db init` first.",
        )
    return None


def _handle_db_init(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    conn = connect(cfg.db_path)
    try:
        version = init_db(conn)
    finally:
        conn.close()
    return build_ok(
        {
            "db_path": str(cfg.db_path),
            "schema_version": version,
        },
        source="internal",
    )


def _handle_db_migrate(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    # Same code path as init — init_db applies any pending migrations
    # and is idempotent when nothing is pending.
    return _handle_db_init(args, cfg)


def _handle_db_backfill_language(
    args: argparse.Namespace, cfg: Config
) -> dict[str, Any]:
    """Classify the language of headlines whose `language` column is NULL.

    Idempotent: re-running after all rows are classified is a no-op
    (rows_examined == rows_classified == 0). Pre-migration rows land
    with NULL language; this subcommand fills them in-place. New rows
    inserted after the v3 migration land with non-null language
    directly via the orchestrator path; only pre-migration rows need
    backfill.

    Output: by_language + by_source_language breakdown so Pass F design
    can see where Russian content actually lives in the corpus.
    """
    conn = connect(cfg.db_path)
    try:
        # Confirm schema is at v3+ — backfill against a pre-v3 schema
        # would crash on the UPDATE because the language column doesn't
        # exist yet. Surface a clear error instead.
        version = schema_version(conn)
        if version < 3:
            return build_error(
                status="error",
                source="internal",
                detail=(
                    f"schema_version={version} predates the language column (v3). "
                    "Run `news-watch-daemon db migrate` first."
                ),
            )
        rows = conn.execute(
            "SELECT headline_id, source, headline FROM headlines "
            "WHERE language IS NULL"
        ).fetchall()
        rows_examined = len(rows)
        by_language: dict[str, int] = {}
        by_source_language: dict[str, dict[str, int]] = {}
        updates: list[tuple[str, str]] = []
        for r in rows:
            lang = classify_language(r["headline"])
            updates.append((lang, r["headline_id"]))
            by_language[lang] = by_language.get(lang, 0) + 1
            per_source = by_source_language.setdefault(r["source"], {})
            per_source[lang] = per_source.get(lang, 0) + 1
        if updates:
            with transaction(conn):
                conn.executemany(
                    "UPDATE headlines SET language = ? WHERE headline_id = ?",
                    updates,
                )
    finally:
        conn.close()
    return build_ok(
        {
            "db_path": str(cfg.db_path),
            "rows_examined": rows_examined,
            "rows_classified": len(updates),
            "by_language": by_language,
            "by_source_language": by_source_language,
        },
        source="internal",
    )


# ---------- db retag (retroactive tag backfill) ----------


def _handle_db_retag(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Re-evaluate existing headlines against CURRENT active theme configs and
    add any newly-matching theme tags. Purely additive + idempotent.

    Why: matcher/keyword changes (e.g. the semis-demand widen) tag forward-only
    — headlines already in the corpus that arrived before the change stay
    untagged because dedup drops re-fetches, so nothing re-evaluates them. This
    pass closes that gap WITHOUT a DB wipe: it is a tagging backfill, not a
    capture-logic change.

    Mechanism reuse: the scrape orchestrator's exact tag path
    (`_compile_theme_regexes` / `_tag_for_theme`), tagging against
    `COALESCE(headline_en, headline)` — the same canonical text the
    orchestrator and the Pass F translation re-tag use. Never deletes or alters
    existing tags/headlines; new tag rows only.

    Idempotent by construction: candidates exclude already-tagged
    (headline_id, theme_id) pairs, AND inserts are `INSERT OR IGNORE` against
    the `headline_theme_tags` PRIMARY KEY (headline_id, theme_id). A second run
    finds zero candidates and writes nothing.

    `--dry-run` is the gate: reports the by-theme would-add count + samples
    with no write, so the operator validates before authorizing the mutation.
    """
    from .scrape.orchestrator import _compile_theme_regexes, _tag_for_theme

    dry_run = bool(getattr(args, "dry_run", False))
    conn = connect(cfg.db_path)
    try:
        if schema_version(conn) == 0:
            return build_error(
                status="error", source="internal",
                detail="database has no schema applied. Run `news-watch-daemon db init` first.",
            )
        registered = list_themes(conn)
        active_ids = {e.theme_id for e in registered if e.status == "active"}
        if not active_ids:
            return build_error(
                status="error", source="internal",
                detail="no active themes in registry. Run `news-watch-daemon themes load` first.",
            )
        try:
            all_themes = load_all_themes(cfg.themes_dir)
        except ThemeLoadError as exc:
            return build_error(
                status="error", source="internal",
                detail=f"theme load failed: {exc}",
            )
        themes = [t for t in all_themes if t.theme_id in active_ids and t.status == "active"]
        if not themes:
            return build_error(
                status="error", source="internal",
                detail="registry lists active themes but none parse from themes_dir.",
            )
        theme_regexes = _compile_theme_regexes(themes)

        # Existing tags — so the backfill only ADDS, never re-touches.
        existing: set[tuple[str, str]] = {
            (r["headline_id"], r["theme_id"])
            for r in conn.execute(
                "SELECT headline_id, theme_id FROM headline_theme_tags"
            )
        }

        rows = conn.execute(
            "SELECT headline_id, COALESCE(headline_en, headline) AS txt FROM headlines"
        ).fetchall()
        headlines_examined = len(rows)

        candidates: list[tuple[str, str, str]] = []
        by_theme: dict[str, int] = {}
        samples: dict[str, list[str]] = {}
        for r in rows:
            hid = r["headline_id"]
            txt = r["txt"] or ""
            for regs in theme_regexes:
                if (hid, regs.theme_id) in existing:
                    continue
                confidence = _tag_for_theme(txt, regs)
                if confidence is not None:
                    candidates.append((hid, regs.theme_id, confidence))
                    by_theme[regs.theme_id] = by_theme.get(regs.theme_id, 0) + 1
                    bucket = samples.setdefault(regs.theme_id, [])
                    if len(bucket) < 5:
                        bucket.append(txt[:120])

        by_theme_sorted = dict(sorted(by_theme.items(), key=lambda kv: -kv[1]))

        if dry_run:
            return build_ok(
                {
                    "db_path": str(cfg.db_path),
                    "dry_run": True,
                    "headlines_examined": headlines_examined,
                    "tags_would_add": len(candidates),
                    "by_theme": by_theme_sorted,
                    "samples_by_theme": samples,
                },
                source="internal",
            )

        now_unix = int(time.time())
        added = 0
        with transaction(conn):
            for hid, theme_id, confidence in candidates:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO headline_theme_tags "
                    "(headline_id, theme_id, confidence, tagged_at_unix) "
                    "VALUES (?, ?, ?, ?)",
                    (hid, theme_id, confidence, now_unix),
                )
                added += cur.rowcount
        return build_ok(
            {
                "db_path": str(cfg.db_path),
                "dry_run": False,
                "headlines_examined": headlines_examined,
                "tags_added": added,
                "by_theme": by_theme_sorted,
            },
            source="internal",
        )
    finally:
        conn.close()


# ---------- Pass F translation handlers (Commit 2) ----------


def _run_translation_subcommand(
    *,
    cfg: Config,
    source_filter: str | None,
    limit: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    """Shared core for `translate` and `db backfill-translation` subcommands.

    See translation/runner.py docstring for the load-bearing re-queue
    semantics: rate-limited / failed translations leave rows with
    headline_en=NULL; future invocations of this subcommand naturally
    retry them (the `WHERE headline_en IS NULL` filter is the queue).

    Idempotency: re-running on a corpus with zero pending rows returns
    rows_examined=0; no API calls made, no DB writes. The `headline_en
    IS NULL` filter makes successfully-translated rows untouchable by
    subsequent runs.

    Re-tag idempotency: after translation succeeds, the row's existing
    theme tags are DELETEd before fresh tag_rows are INSERTed against
    the translated text. So if a row had zero tags (Russian content +
    English keywords = no matches) and translation gives English text
    that DOES match a keyword, the new tag lands. Re-running on the
    same row (now headline_en NOT NULL) finds zero pending rows.
    """
    conn = connect(cfg.db_path)
    try:
        version = schema_version(conn)
        if version < 4:
            return build_error(
                status="error",
                source="internal",
                detail=(
                    f"schema_version={version} predates the headline_en column (v4). "
                    "Run `news-watch-daemon db migrate` first."
                ),
            )

        # Load translation config
        try:
            tx_cfg = load_translation_config(cfg.translation_config_path)
        except TranslationConfigError as exc:
            return build_error(
                status="error",
                source="internal",
                detail=f"translation config load failed: {exc}",
            )

        # Verify Telegram credentials (Telegram-native path requires them).
        if tx_cfg.translation_source == "telegram_native":
            if not cfg.telegram_creds_complete:
                return build_error(
                    status="error",
                    source="internal",
                    detail=(
                        "translation_source=telegram_native but Telegram credentials are not "
                        "configured. Set TELEGRAM_API_ID, TELEGRAM_API_HASH, and "
                        "TELEGRAM_SESSION_STRING."
                    ),
                )

        # Query pending rows.
        sql = (
            "SELECT headline_id, source, headline, url, fetched_at_unix, language "
            "FROM headlines "
            "WHERE language != 'en' AND headline_en IS NULL"
        )
        params: list[Any] = []
        if source_filter:
            sql += " AND source = ?"
            params.append(source_filter)
        sql += " ORDER BY fetched_at_unix DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(sql, params).fetchall()

        if not rows:
            return build_ok(
                {
                    "db_path": str(cfg.db_path),
                    "translation_source": tx_cfg.translation_source,
                    "dry_run": dry_run,
                    "rows_examined": 0,
                    "rows_translated": 0,
                    "by_status": {},
                    "by_source": {},
                },
                source="internal",
            )

        # Group by channel (only Telegram rows can be translated via the
        # telegram_native path; other-source rows skip silently).
        by_source: dict[str, list[Any]] = {}
        for r in rows:
            by_source.setdefault(r["source"], []).append(r)

        if dry_run:
            # Preview-only — no API calls, no DB writes.
            preview = {
                src: {"count": len(srcrows)} for src, srcrows in by_source.items()
            }
            return build_ok(
                {
                    "db_path": str(cfg.db_path),
                    "translation_source": tx_cfg.translation_source,
                    "dry_run": True,
                    "rows_examined": len(rows),
                    "rows_would_translate": len(rows),
                    "by_source_preview": preview,
                },
                source="internal",
            )

        # Build per-channel pending lists for translation
        pending_by_channel: dict[str, list[tuple[int, str]]] = {}
        row_index: dict[tuple[str, int], Any] = {}  # (channel, msg_id) -> row
        for src, srcrows in by_source.items():
            if not src.startswith("telegram:"):
                # Non-Telegram sources can't use telegram_native path
                continue
            channel = src[len("telegram:"):]
            entries: list[tuple[int, str]] = []
            for r in srcrows:
                msg_id = _parse_telegram_msg_id(r["url"])
                if msg_id is None:
                    continue
                entries.append((msg_id, r["headline"]))
                row_index[(channel, msg_id)] = r
            if entries:
                pending_by_channel[channel] = entries

        if not pending_by_channel:
            return build_ok(
                {
                    "db_path": str(cfg.db_path),
                    "translation_source": tx_cfg.translation_source,
                    "dry_run": False,
                    "rows_examined": len(rows),
                    "rows_translated": 0,
                    "by_status": {},
                    "by_source": {},
                    "note": "no eligible Telegram-source rows in pending queue",
                },
                source="internal",
            )

        # Run the translation pass
        try:
            translations = run_translation_pass(
                api_id=cfg.telegram_api_id,
                api_hash=cfg.telegram_api_hash,
                session_string=cfg.telegram_session_string,
                pending_by_channel=pending_by_channel,
                batch_size=tx_cfg.telegram_native_batch_size,
                translation_source=tx_cfg.translation_source,
            )
        except NotImplementedError as exc:
            return build_error(
                status="error",
                source="internal",
                detail=f"translation_source stub raised NotImplementedError: {exc}",
            )

        # Apply translations: UPDATE headline_en + re-tag.
        by_status: dict[str, int] = {}
        rows_translated = 0
        # Load themes for re-tagging
        try:
            all_themes = load_all_themes(cfg.themes_dir)
        except ThemeLoadError as exc:
            return build_error(
                status="error",
                source="internal",
                detail=f"theme load failed during re-tag: {exc}",
            )
        registered = list_themes(conn)
        active_ids = {e.theme_id for e in registered if e.status == "active"}
        active_themes = [t for t in all_themes if t.theme_id in active_ids and t.status == "active"]
        from .scrape.orchestrator import _compile_theme_regexes, _tag_for_theme
        theme_regexes = _compile_theme_regexes(active_themes)

        now_unix = int(time.time())
        with transaction(conn):
            for (channel, msg_id), result in translations.items():
                by_status[result.status] = by_status.get(result.status, 0) + 1
                row = row_index.get((channel, msg_id))
                if row is None:
                    continue
                if result.status != "ok" or not result.translated_text:
                    # Failed or empty translation — leave row with
                    # headline_en=NULL, sit in queue for next retry.
                    continue
                # Successful translation: update headline_en
                conn.execute(
                    "UPDATE headlines SET headline_en = ? WHERE headline_id = ?",
                    (result.translated_text, row["headline_id"]),
                )
                # Re-tag against translated text (DELETE existing then
                # INSERT fresh — idempotent).
                conn.execute(
                    "DELETE FROM headline_theme_tags WHERE headline_id = ?",
                    (row["headline_id"],),
                )
                tagging_text = result.translated_text
                for regs in theme_regexes:
                    confidence = _tag_for_theme(tagging_text, regs)
                    if confidence is not None:
                        conn.execute(
                            "INSERT INTO headline_theme_tags "
                            "(headline_id, theme_id, confidence, tagged_at_unix) "
                            "VALUES (?, ?, ?, ?)",
                            (row["headline_id"], regs.theme_id, confidence, now_unix),
                        )
                rows_translated += 1

        # Per-source rollup
        per_source_rollup: dict[str, dict[str, int]] = {}
        for (channel, msg_id), result in translations.items():
            src = f"telegram:{channel}"
            per_source_rollup.setdefault(src, {})
            per_source_rollup[src][result.status] = (
                per_source_rollup[src].get(result.status, 0) + 1
            )

        return build_ok(
            {
                "db_path": str(cfg.db_path),
                "translation_source": tx_cfg.translation_source,
                "dry_run": False,
                "rows_examined": len(rows),
                "rows_translated": rows_translated,
                "by_status": by_status,
                "by_source": per_source_rollup,
            },
            source="internal",
        )
    finally:
        conn.close()


def _handle_db_backfill_translation(
    args: argparse.Namespace, cfg: Config
) -> dict[str, Any]:
    """Backfill subcommand: translate all pending rows + re-tag.

    Idempotent via the `WHERE headline_en IS NULL` filter — re-runs find
    zero pending rows once all are translated. See
    _run_translation_subcommand docstring for the load-bearing
    re-queue / idempotency semantics.
    """
    return _run_translation_subcommand(
        cfg=cfg,
        source_filter=None,
        limit=None,
        dry_run=args.dry_run,
    )


def _handle_translate(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Translate subcommand: same as backfill-translation with optional
    --source filter + --limit. Use for narrow re-runs after rate-limit
    recovery on a specific channel."""
    return _run_translation_subcommand(
        cfg=cfg,
        source_filter=args.source,
        limit=args.limit,
        dry_run=args.dry_run,
    )


def _handle_themes_load(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    try:
        themes = load_all_themes(cfg.themes_dir)
    except ThemeLoadError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"theme load failed: {exc}",
        )
    conn = connect(cfg.db_path)
    try:
        err = _schema_not_ready(conn)
        if err is not None:
            return err
        counts = upsert_themes(conn, themes)
    finally:
        conn.close()
    return build_ok(
        {
            "themes_dir": str(cfg.themes_dir),
            "loaded_count": len(themes),
            "loaded_theme_ids": [t.theme_id for t in themes],
            **counts,
        },
        source="internal",
    )


def _handle_themes_list(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    conn = connect(cfg.db_path)
    try:
        err = _schema_not_ready(conn)
        if err is not None:
            return err
        entries = list_themes(conn)
    finally:
        conn.close()
    return build_ok(
        {
            "count": len(entries),
            "themes": [asdict(e) for e in entries],
        },
        source="internal",
    )


def _handle_scrape(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Run one scrape sweep.

    Orchestration errors (DB unreachable, no schema, no active themes)
    return `status="error"` and exit 1. Per-source upstream failures
    degrade `data_completeness` to `partial` but the orchestration
    itself succeeds with exit 0.
    """
    conn = connect(cfg.db_path)
    try:
        err = _schema_not_ready(conn)
        if err is not None:
            return err

        registered = list_themes(conn)
        active_ids = {e.theme_id for e in registered if e.status == "active"}
        if not active_ids:
            return build_error(
                status="error",
                source="internal",
                detail=(
                    "no active themes in registry. Run `news-watch-daemon themes load` "
                    "and ensure at least one theme has status=active."
                ),
            )

        try:
            all_themes = load_all_themes(cfg.themes_dir)
        except ThemeLoadError as exc:
            return build_error(
                status="error",
                source="internal",
                detail=f"theme load failed: {exc}",
            )
        themes = [t for t in all_themes if t.theme_id in active_ids and t.status == "active"]
        if not themes:
            return build_error(
                status="error",
                source="internal",
                detail="registry lists active themes but none parse from themes_dir.",
            )

        http = HttpClient(
            user_agent=cfg.http_user_agent,
            default_timeout_s=cfg.http_default_timeout_s,
        )
        sources = build_sources(cfg, themes, http)

        try:
            tracked_tickers = load_tracked_tickers(cfg.tracked_tickers_path)
        except TickerExtractError as exc:
            return build_error(
                status="error",
                source="internal",
                detail=f"tracked_tickers load failed: {exc}",
            )

        # Pass F (2026-05-28): load translation config; thread Telegram
        # credentials into run_scrape so the orchestrator's per-source
        # batched translation pass has what it needs.
        translation_credentials: tuple[int, str, str] | None = None
        translation_source = "telegram_native"
        translation_batch_size = 10
        try:
            tx_cfg = load_translation_config(cfg.translation_config_path)
            translation_source = tx_cfg.translation_source
            translation_batch_size = tx_cfg.telegram_native_batch_size
        except TranslationConfigError as exc:
            _log = logging.getLogger("news_watch_daemon.cli")
            _log.warning(
                "translation config load failed: %s. Scrape continues with translation disabled "
                "(non-en rows will insert with headline_en=NULL).", exc,
            )
        if cfg.telegram_creds_complete and translation_source == "telegram_native":
            translation_credentials = (
                cfg.telegram_api_id,
                cfg.telegram_api_hash,
                cfg.telegram_session_string,
            )

        # Auto-attention callback: closes over cfg + conn so scrape_cycle
        # doesn't need to know about Config. Per Stage 2a-i sub-step B
        # forward-guidance: pure callable receives a callback, doesn't
        # import CLI types.
        def _attention_followon() -> dict[str, Any]:
            return run_attention_cycle(cfg=cfg, conn=conn, dry_run=False)

        cycle_result = scrape_cycle(
            conn=conn,
            sources=sources,
            themes=themes,
            tracked_tickers=tracked_tickers,
            cross_source_log_path=cfg.cross_source_log_path,
            ingest_filter_log_path=cfg.filtered_log_path,
            translation_credentials=translation_credentials,
            translation_source=translation_source,
            translation_batch_size=translation_batch_size,
            attention_callback=_attention_followon,
        )

        if cycle_result.status == "scrape_failed":
            # Surface as an internal error envelope; heartbeat already
            # written inside scrape_cycle's exception path.
            return build_error(
                status="error",
                source="internal",
                detail=f"scrape orchestration failed: {cycle_result.reason}",
            )

        # status == "ok"
        assert cycle_result.scrape_result is not None
        envelope = _scrape_result_to_envelope(cycle_result.scrape_result)
        # Pass E (2026-05-26): attention_outcome nests under data per the
        # single-envelope contract. scrape_cycle handles the exception
        # capture internally.
        envelope["data"]["attention_outcome"] = cycle_result.attention_outcome
        return envelope
    finally:
        conn.close()


def _scrape_result_to_envelope(result: ScrapeResult) -> dict[str, Any]:
    """Render a `ScrapeResult` as the Pass A/B envelope shape.

    Completeness rule (Pass B flag #8): skipped sources are neither
    successes nor failures; completeness reflects only sources we
    actually called fetch() on. An all-skipped sweep is `complete`.
    """
    real_failure = any(
        s.status not in ("ok", "skipped") for s in result.per_source
    )
    completeness = "partial" if real_failure else "complete"
    warnings: list[dict[str, Any]] = []
    if real_failure:
        warnings.append(
            make_warning(
                field="per_source",
                reason="upstream_error",
                source="internal",
                detail=(
                    f"{result.sources_failed} of {result.sources_attempted} sources "
                    f"did not return status=ok; see per_source for details"
                ),
            )
        )
    data = {
        "started_at_unix": result.started_at_unix,
        "started_at": result.started_at,
        "duration_ms": result.duration_ms,
        "sources_attempted": result.sources_attempted,
        "sources_succeeded": result.sources_succeeded,
        "sources_failed": result.sources_failed,
        "sources_skipped": result.sources_skipped,
        "per_source": [_per_source_to_dict(p) for p in result.per_source],
        "headlines_inserted_total": result.headlines_inserted_total,
        "theme_tags_inserted_total": result.theme_tags_inserted_total,
        "themes_active": result.themes_active,
    }
    return build_ok(
        data,
        source="internal",
        data_completeness=completeness,
        warnings=warnings,
    )


def _per_source_to_dict(p: PerSourceResult) -> dict[str, Any]:
    return {
        "name": p.name,
        "status": p.status,
        "items_fetched": p.items_fetched,
        "items_after_dedup": p.items_after_dedup,
        "items_inserted": p.items_inserted,
        "error_detail": p.error_detail,
    }


# ---------- handlers: attention (Pass E 2026-05-26) ----------

# Attention cycle helpers (DEFAULT_ATTENTION_MAX_TOKENS, attention_outcome_to_dict,
# run_attention_cycle) hoisted to attention/orchestrator.py per Full Brief
# Stage 2a-ii-A composition-glue refactor (2026-05-29). Imported above; both
# this CLI and the new Full Brief orchestrator at fullbrief/orchestrator.py
# call into the same canonical implementation.
def _handle_attention(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Standalone `news-watch-daemon attention` subcommand.

    For manual / testing invocation. Production cron runs scrape which
    chains attention as a follow-on (per Pass E Q4 decision, 2026-05-26).
    """
    conn = connect(cfg.db_path)
    try:
        err = _schema_not_ready(conn)
        if err is not None:
            return err
        outcome = run_attention_cycle(
            cfg=cfg,
            conn=conn,
            dry_run=bool(getattr(args, "dry_run", False)),
            top_candidates_limit=int(getattr(args, "top_candidates_limit", 5)),
        )
    finally:
        conn.close()

    # Map cycle outcome onto envelope status.
    cycle_status = outcome.get("status", "ok")
    if cycle_status == "skipped":
        # Skipped is success-shape but data_completeness=partial — the
        # scrape envelope (when chained) preserves overall ok shape, but
        # the standalone invocation surfaces the skip clearly.
        return build_ok(
            outcome,
            source="internal",
            data_completeness="partial",
            warnings=[
                make_warning(
                    field="attention",
                    reason="attention_skipped",
                    source="internal",
                    detail=outcome.get("reason", "unspecified"),
                ),
            ],
        )
    return build_ok(outcome, source="internal")


# ---------- handlers: synthesize (Pass C Step 13) ----------


_DEFAULT_SYNTHESIS_WINDOW_HOURS = 4
# NOTE: max_tokens is no longer a CLI-side constant — it's read from
# synthesis_config.yaml's synthesis.default_max_tokens (default 8192).
# The first live smoke (2026-05-14) hit a budget-exhaustion bug at
# 2048 where adaptive thinking consumed the entire output budget.
# Routing through config makes per-deploy tuning the standard knob.


# Synthesize helpers (_iso_from_unix, _query_window_headlines,
# _filter_clusters_to_scope) hoisted to synthesize/synthesize.py per
# Full Brief Commit C Stage 2a-i sub-step B (2026-05-29). They are
# module-private to synthesize/synthesize.py — only synthesize_window
# calls them. Tests that previously imported them from cli should
# update to:
#   from news_watch_daemon.synthesize.synthesize import _query_window_headlines


def _handle_synthesize(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Thin argparse wrapper around synthesize_window (Stage 2a-i sub-step B).

    Resolves config + DB + client dependencies from args + cfg, delegates
    to the pure callable in synthesize/synthesize.py, formats the CLI
    envelope from the structured SynthesizeResult.

    Per Stage 2a-i forward-guidance: exit codes are a CLI concern, not a
    synthesis concern — the structured result's `status` field
    discriminates outcomes; this wrapper maps them to build_ok / build_error.
    """
    # CLI pre-flight: API key required for non-dry-run.
    if not args.dry_run and not cfg.anthropic_api_key:
        return build_error(
            status="error",
            source="internal",
            detail=(
                "ANTHROPIC_API_KEY is not set; synthesis cannot run. "
                "Export the key or use --dry-run to exercise the pipeline "
                "without an LLM call."
            ),
        )

    synth_cfg, err = _load_synthesis_config_for_cli(cfg)
    if err is not None:
        return err
    assert synth_cfg is not None

    try:
        all_themes = load_all_themes(cfg.themes_dir)
    except ThemeLoadError as exc:
        return build_error(
            status="error", source="internal",
            detail=f"theme load failed: {exc}",
        )
    active_themes = [t for t in all_themes if t.status == "active"]
    active_ids = {t.theme_id for t in active_themes}

    pull_mode = args.theme is not None
    if pull_mode and args.theme not in active_ids:
        return build_error(
            status="error", source="internal",
            detail=(
                f"theme {args.theme!r} is not in the active themes set; "
                f"active themes: {sorted(active_ids)}"
            ),
        )

    window_hours = max(1, min(168, args.window_hours))

    conn = connect(cfg.db_path)
    try:
        if schema_version(conn) == 0:
            return build_error(
                status="error", source="internal",
                detail="database has no schema applied. Run `news-watch-daemon db init` first.",
            )

        # Build anthropic client (skip in dry-run).
        anthropic_client: Any = None
        if not args.dry_run:
            try:
                anthropic_client = build_anthropic_client(cfg.anthropic_api_key)
            except SynthesisError as exc:
                return build_error(
                    status="error", source="internal",
                    detail=f"Anthropic client construction failed: {exc}",
                )

        # Sink factory (lazy — only invoked when materiality says dispatch).
        sink_factory = (
            (lambda: build_alert_sink(synth_cfg.alert_sink))
            if not args.dry_run else None
        )

        result = synthesize_window(
            conn=conn,
            active_themes=active_themes,
            brief_archive_path=cfg.brief_archive_path,
            trigger_log_path=cfg.trigger_log_path,
            theses_path=cfg.theses_path,
            synth_cfg=synth_cfg,
            anthropic_client=anthropic_client,
            sink_factory=sink_factory,
            window_hours=window_hours,
            pull_theme=args.theme,
            dry_run=args.dry_run,
        )
    finally:
        conn.close()

    return _synthesize_result_to_envelope(result)


def _synthesize_result_to_envelope(result: "SynthesizeResult") -> dict[str, Any]:
    """Convert SynthesizeResult to the existing CLI envelope contract.

    Preserves backwards-compatible envelope keys (synthesis_run, brief_id,
    archive_path, themes_covered, trigger, materiality_decision,
    dispatch_result, telemetry, events_count, trigger_decision) — tests
    and downstream tooling rely on these.
    """
    if result.status == "synthesis_failed":
        return build_error(
            status="error", source="internal",
            detail=result.reason or "synthesis failed",
        )

    if result.status == "archive_failed":
        return build_error(
            status="error", source="internal",
            detail=result.reason or "archive write failed",
        )

    if result.status == "no_trigger":
        return build_ok(
            {
                "trigger_decision": {
                    "fire": False,
                    "reason": result.trigger_decision_reason,
                    "window_since_unix": result.window_since_unix,
                    "window_until_unix": result.window_until_unix,
                },
                "synthesis_run": False,
            },
            source="internal",
        )

    if result.status == "dry_run":
        return build_ok(
            {
                "dry_run": True,
                "themes_in_scope": result.themes_in_scope,
                "trigger_type": (
                    result.trigger_obj.type if result.trigger_obj else None
                ),
                "trigger_reason": (
                    result.trigger_obj.reason if result.trigger_obj else None
                ),
                "headlines_in_window": result.headlines_in_window,
                "tagged_headlines_in_window": result.tagged_headlines_in_window,
                "cluster_count": result.cluster_count,
                "synthesis_run": False,
            },
            source="internal",
        )

    # status == "synthesized"
    assert result.brief is not None
    assert result.metadata is not None
    assert result.brief_path is not None

    md = result.metadata
    payload: dict[str, Any] = {
        "synthesis_run": True,
        "brief_id": result.brief.brief_id,
        "archive_path": str(result.brief_path),
        "themes_covered": list(result.brief.themes_covered),
        "trigger": {
            "type": result.brief.trigger.type,
            "reason": result.brief.trigger.reason,
        },
        "materiality_decision": result.materiality_decision_payload,
        "dispatch_result": result.dispatch_result_payload,
        "telemetry": {
            "model_used": md.model_used,
            "input_tokens": md.input_tokens,
            "output_tokens": md.output_tokens,
            "cache_creation_input_tokens": md.cache_creation_input_tokens,
            "cache_read_input_tokens": md.cache_read_input_tokens,
        },
        "events_count": len(result.brief.events),
    }
    if result.trigger_decision_fire is not None:
        payload["trigger_decision"] = {
            "fire": result.trigger_decision_fire,
            "reason": result.trigger_decision_reason,
        }
    return build_ok(payload, source="internal")


# ---------- handlers: alert-sink (Pass C Step 13) ----------


def _load_synthesis_config_for_cli(
    cfg: Config,
) -> tuple[SynthesisDaemonConfig | None, dict[str, Any] | None]:
    """Load synthesis_config.yaml. On failure, return (None, error_envelope).

    Helper used by `synthesize` and `alert-sink test` — both need the same
    config load + error-envelope discipline.
    """
    try:
        synth_cfg = load_synthesis_config(cfg.synthesis_config_path)
    except SynthesisConfigError as exc:
        return None, build_error(
            status="error",
            source="internal",
            detail=f"synthesis_config load failed: {exc}",
        )
    return synth_cfg, None


def _handle_alert_sink_test(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Verify the configured AlertSink transport with a synthetic brief.

    Builds the sink from synthesis_config.yaml, constructs a minimal
    Brief (narrative is either --message or a timestamped self-test
    line), calls dispatch(), surfaces DispatchResult.
    """
    synth_cfg, err = _load_synthesis_config_for_cli(cfg)
    if err is not None:
        return err
    assert synth_cfg is not None

    try:
        sink = build_alert_sink(synth_cfg.alert_sink)
    except AlertSinkFactoryError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"alert sink construction failed: {exc}",
        )

    now = datetime.now(timezone.utc)
    iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    narrative = (
        args.message
        if args.message
        else f"News Watch Daemon alert-sink self-test at {iso}."
    )

    # Construct a minimal valid Brief. The sink only reads narrative +
    # brief_id + themes_covered; the rest is required by the schema
    # but is not transport-visible.
    test_brief = Brief(
        brief_id=Brief.new_brief_id(now),
        generated_at=iso,
        trigger=Trigger(
            type="pull", reason="alert-sink self-test",
            window=TriggerWindow(since=iso, until=iso),
        ),
        themes_covered=["alert_sink_self_test"],
        events=[],
        narrative=narrative,
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="(no model — self-test)",
            theses_doc_available=False,
        ),
    )

    result = sink.dispatch(test_brief)
    payload = {
        "sink_type": synth_cfg.alert_sink.type,
        "channel_name": sink.channel_name,
        "test_brief_id": test_brief.brief_id,
        "narrative": narrative,
        "success": result.success,
        "channel": result.channel,
        "error": result.error,
        "dispatched_at_unix": result.dispatched_at_unix,
    }
    if result.success:
        return build_ok(payload, source="internal")
    return build_error(
        status="error",
        source="internal",
        detail=f"alert-sink dispatch failed: {result.error}",
        data=payload,
    )


# ---------- handlers: trigger-log (Pass C Step 13) ----------


def _handle_trigger_log_tail(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Show the last N trigger-gate decisions (oldest-first within window)."""
    limit = max(1, min(500, args.limit))
    log_path = cfg.trigger_log_path
    entries = read_trigger_log_last_n(log_path, limit)
    return build_ok(
        {
            "trigger_log_path": str(log_path),
            "limit": limit,
            "count": len(entries),
            "entries": entries,
        },
        source="internal",
    )


# ---------- handlers: briefs (Pass C Step 12) ----------


def _brief_summary(brief: Any) -> dict[str, Any]:
    """Project a Brief or AttentionBrief into the compact summary shape
    used by `briefs list`.

    Full Brief JSONs can run 1-10 KB; `briefs list` returns up to 500
    summaries so the envelope stays inspectable. Discriminates on the
    `brief_type` field so both Pass C theme-event briefs and Pass E
    attention briefs project correctly — Pass C-only fields are
    populated only for Pass C briefs; attention-only fields populated
    only for attention briefs. Common fields (brief_id, generated_at,
    brief_type, dispatch.*) always present.

    Before this discrimination was added (2026-05-27 Follow-up #5),
    walking an archive containing both brief types would crash on the
    first AttentionBrief encountered (no `.events` / `.themes_covered`
    attributes). Same defect class as materiality.py's archive walk.
    """
    common = {
        "brief_id": brief.brief_id,
        "brief_type": brief.brief_type,
        "generated_at": brief.generated_at,
        "alerted": brief.dispatch.alerted,
        "channel": brief.dispatch.channel,
        "suppressed_reason": brief.dispatch.suppressed_reason,
    }
    if isinstance(brief, AttentionBrief):
        return {
            **common,
            "triggering_term": brief.triggering_term,
            "term_frequency_window": brief.term_frequency_window,
            "term_frequency_prior": brief.term_frequency_prior,
            "cluster_size": brief.cluster_size,
            "attention_shape": brief.attention_shape,
        }
    # Pass C Brief
    max_mat = max((e.materiality_score for e in brief.events), default=0.0)
    return {
        **common,
        "themes_covered": list(brief.themes_covered),
        "events_count": len(brief.events),
        "max_materiality_score": round(max_mat, 3),
    }


def _handle_briefs_list(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    limit = max(1, min(500, args.limit))
    archive_root = cfg.brief_archive_path
    warnings: list[dict[str, Any]] = []

    # Walk newest-first; load each Brief; project to the summary view.
    # If a brief fails to load (corrupt JSON or schema drift), surface
    # one warning per failure but keep going — Abelard should still get
    # the readable subset.
    summaries: list[dict[str, Any]] = []
    seen = 0
    for brief_id in list_brief_ids(archive_root):
        try:
            brief = read_brief(archive_root, brief_id)
        except ArchiveError as exc:
            warnings.append(make_warning(
                field="briefs",
                reason="parse_error",
                source="internal",
                detail=f"brief {brief_id!r} unreadable: {exc}",
            ))
            continue
        # Theme filter applies to Pass C theme-event briefs only.
        # AttentionBriefs don't have themes_covered and would crash
        # on `in` — skip them when --theme is set (they're not in the
        # theme-event namespace anyway).
        if args.theme:
            if isinstance(brief, AttentionBrief):
                continue
            if args.theme not in brief.themes_covered:
                continue
        summaries.append(_brief_summary(brief))
        seen += 1
        if seen >= limit:
            break

    completeness = "partial" if warnings else "complete"
    return build_ok(
        {
            "archive_path": str(archive_root),
            "count": len(summaries),
            "limit": limit,
            "filter_theme": args.theme,
            "briefs": summaries,
        },
        source="internal",
        data_completeness=completeness,
        warnings=warnings,
    )


def _handle_briefs_show(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    try:
        brief = read_brief(cfg.brief_archive_path, args.brief_id)
    except ArchiveError as exc:
        return build_error(
            status="not_found" if "not found" in str(exc) else "error",
            source="internal",
            detail=str(exc),
        )
    return build_ok(
        {"brief": brief.model_dump(mode="json")},
        source="internal",
    )


# ---------- handlers: headlines (Pass C Step 12 — `recent` made real) ----


def _handle_headlines_recent(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Recent headlines with optional theme + ticker + hours filters.

    --ticker filter uses JSON-quoted substring match against tickers_json,
    so it matches BOTH tracked-list extractions (Pass C Step 0) and
    cashtag extractions (same column). Pattern is `"<SYM>"` quoted to
    avoid `AAPL` matching inside `BAAPL`.
    """
    hours = max(1, min(168, args.hours))
    limit = max(1, min(500, args.limit))
    since_unix = int(time.time()) - hours * 3600

    sql = (
        "SELECT h.headline_id, h.source, h.raw_source, h.headline, h.url, "
        "       h.published_at, h.published_at_unix, h.tickers_json, h.entities_json "
        "FROM headlines h "
    )
    params: list[Any] = []
    where_clauses: list[str] = ["h.published_at_unix >= ?"]
    params.append(since_unix)

    if args.theme:
        sql += (
            "INNER JOIN headline_theme_tags t "
            "  ON t.headline_id = h.headline_id "
        )
        where_clauses.append("t.theme_id = ?")
        params.append(args.theme)

    if args.ticker:
        # JSON-quoted substring match — robust against substring collisions
        # (e.g. "AAPL" matching inside "BAAPL"). Both tracked-list and
        # cashtag extractions write to tickers_json with the symbol quoted.
        where_clauses.append('h.tickers_json LIKE ?')
        params.append(f'%"{args.ticker}"%')

    sql += "WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY h.published_at_unix DESC LIMIT ?"
    params.append(limit)

    conn = connect(cfg.db_path)
    try:
        err = _schema_not_ready(conn)
        if err is not None:
            return err
        rows = conn.execute(sql, params).fetchall()
        # Pull theme tags per headline in one extra round trip; cheaper
        # than N+1 selects, simple enough not to need a separate index.
        ids = [r["headline_id"] for r in rows]
        tags_by_id: dict[str, list[str]] = {hid: [] for hid in ids}
        if ids:
            placeholders = ",".join("?" * len(ids))
            tag_rows = conn.execute(
                f"SELECT headline_id, theme_id "
                f"FROM headline_theme_tags WHERE headline_id IN ({placeholders}) "
                f"ORDER BY headline_id, theme_id",
                ids,
            ).fetchall()
            for tr in tag_rows:
                tags_by_id[tr["headline_id"]].append(tr["theme_id"])
    finally:
        conn.close()

    headlines = []
    for r in rows:
        try:
            tickers = json.loads(r["tickers_json"]) if r["tickers_json"] else []
        except json.JSONDecodeError:
            tickers = []
        try:
            entities = json.loads(r["entities_json"]) if r["entities_json"] else {}
        except json.JSONDecodeError:
            entities = {}
        headlines.append({
            "headline_id": r["headline_id"],
            "source": r["source"],
            "publisher": r["raw_source"],
            "headline": r["headline"],
            "url": r["url"],
            "published_at": r["published_at"],
            "themes": tags_by_id.get(r["headline_id"], []),
            "tickers": tickers,
            "entities": entities,
        })

    return build_ok(
        {
            "since_unix": since_unix,
            "since_hours": hours,
            "filter_theme": args.theme,
            "filter_ticker": args.ticker,
            "count": len(headlines),
            "limit": limit,
            "headlines": headlines,
        },
        source="internal",
    )


# ---------- handlers: proposals (Pass C Step 11) ----------


def _handle_proposals_list(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    try:
        proposals = read_pending(cfg.proposals_path)
    except ProposalsStoreError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposals store unreadable: {exc}",
        )
    return build_ok(
        {
            "proposals_path": str(cfg.proposals_path),
            "count": len(proposals),
            "proposals": [
                {
                    "proposal_id": p.proposal_id,
                    "theme_id": p.theme_id,
                    "proposed_keyword": p.proposed_keyword,
                    "suggested_tier": p.suggested_tier,
                    "evidence_count": p.evidence_count,
                    "generated_at": p.generated_at,
                }
                for p in proposals
            ],
        },
        source="internal",
    )


def _handle_proposals_show(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    try:
        proposal = find_proposal(cfg.proposals_path, args.proposal_id)
    except ProposalsStoreError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposals store unreadable: {exc}",
        )
    if proposal is None:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposal_id {args.proposal_id!r} not found in pending",
        )
    return build_ok(
        {"proposal": proposal.model_dump(mode="json")},
        source="internal",
    )


def _handle_proposals_approve(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Approve a proposal: mutate the theme YAML, remove from pending,
    record in resolved.jsonl.

    On dry-run, only report what WOULD happen — no disk writes.
    """
    try:
        proposal = find_proposal(cfg.proposals_path, args.proposal_id)
    except ProposalsStoreError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposals store unreadable: {exc}",
        )
    if proposal is None:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposal_id {args.proposal_id!r} not found in pending",
        )

    if args.dry_run:
        return build_ok(
            {
                "action": "approve",
                "proposal_id": proposal.proposal_id,
                "theme_id": proposal.theme_id,
                "proposed_keyword": proposal.proposed_keyword,
                "suggested_tier": proposal.suggested_tier,
                "would_mutate_file": str(cfg.themes_dir / f"{proposal.theme_id}.yaml"),
                "dry_run": True,
                "applied": False,
            },
            source="internal",
        )

    # Mutate the theme YAML first — if that fails, we leave the
    # proposal in pending so Mando can retry or reject.
    try:
        mutated_path = apply_proposal_to_theme(
            cfg.themes_dir,
            theme_id=proposal.theme_id,
            proposed_keyword=proposal.proposed_keyword,
            suggested_tier=proposal.suggested_tier,
        )
    except ThemeMutationError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"theme mutation failed: {exc}",
        )

    # Remove from pending + audit.
    try:
        remove_proposal(cfg.proposals_path, proposal.proposal_id)
        append_resolved(
            cfg.proposals_path,
            proposal=proposal,
            action="approve",
            applied_to_yaml=True,
        )
    except ProposalsStoreError as exc:
        # YAML was already mutated — surface the store error but the
        # mutation is real. Mando can manually clean up pending.json.
        return build_error(
            status="error",
            source="internal",
            detail=(
                f"theme YAML at {mutated_path} was mutated successfully, "
                f"but proposals store update failed: {exc}. "
                f"Manually remove proposal_id {proposal.proposal_id!r} from "
                f"{cfg.proposals_path / 'pending.json'}."
            ),
        )

    return build_ok(
        {
            "action": "approve",
            "proposal_id": proposal.proposal_id,
            "theme_id": proposal.theme_id,
            "proposed_keyword": proposal.proposed_keyword,
            "suggested_tier": proposal.suggested_tier,
            "mutated_file": str(mutated_path),
            "applied": True,
        },
        source="internal",
    )


def _handle_proposals_reject(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Reject a proposal: remove from pending, record in resolved.jsonl.

    Does NOT mutate any theme YAML. The proposal's keyword stays out
    of the theme; future drift cycles may propose it again unless
    Mando explicitly added it to keywords.exclusions.
    """
    try:
        proposal = find_proposal(cfg.proposals_path, args.proposal_id)
    except ProposalsStoreError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposals store unreadable: {exc}",
        )
    if proposal is None:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposal_id {args.proposal_id!r} not found in pending",
        )
    try:
        remove_proposal(cfg.proposals_path, proposal.proposal_id)
        append_resolved(
            cfg.proposals_path,
            proposal=proposal,
            action="reject",
            applied_to_yaml=False,
            reason=args.reason,
        )
    except ProposalsStoreError as exc:
        return build_error(
            status="error",
            source="internal",
            detail=f"proposals store update failed: {exc}",
        )
    return build_ok(
        {
            "action": "reject",
            "proposal_id": proposal.proposal_id,
            "theme_id": proposal.theme_id,
            "proposed_keyword": proposal.proposed_keyword,
            "reason": args.reason,
            "applied": False,
        },
        source="internal",
    )


def _check_writable_dir(path: Path) -> tuple[bool, str]:
    """Best-effort: ensure `path` exists (create if needed) and is writable."""
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return False, f"{path} — cannot create ({exc})"
    if not os.access(path, os.W_OK):
        return False, f"{path} — not writable"
    return True, str(path)


def _handle_doctor(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Preflight: validate env, paths, external deps, and DB state before a run.

    Read-only. Config is already known-valid (main() built it before dispatch);
    this surfaces the problems Config does NOT catch — missing bundled config
    files, unwritable output dirs, absent secrets/binaries, and an
    uninitialized DB. Exit 1 iff a check BLOCKS a run; warnings alone are exit 0.
    """
    checks: list[dict[str, str]] = []

    def add(name: str, status: str, detail: str) -> None:
        checks.append({"name": name, "status": status, "detail": detail})

    # --- secrets / credentials (non-blocking; each degrades one capability) ---
    add("anthropic_api_key", "ok" if cfg.anthropic_api_key else "warn",
        "set" if cfg.anthropic_api_key
        else "unset — synthesize hard-fails; attention/full-brief skip the LLM step")
    add("finnhub_api_key", "ok" if cfg.finnhub_api_key else "warn",
        "set" if cfg.finnhub_api_key
        else "unset — the Finnhub source errors cleanly and is skipped")
    add("telegram_credentials", "ok" if cfg.telegram_creds_complete else "warn",
        "complete" if cfg.telegram_creds_complete
        else "incomplete — Telegram sources are skipped (RSS + Finnhub only)")
    add("theses_doc", "ok" if cfg.theses_path else "warn",
        str(cfg.theses_path) if cfg.theses_path
        else "unset — synthesis runs the no-theses prompt variant")

    # --- bundled config files must exist (blocking) ---
    for label, p in (
        ("themes_dir", cfg.themes_dir),
        ("synthesis_config", cfg.synthesis_config_path),
        ("stopwords", cfg.stopwords_path),
        ("translation_config", cfg.translation_config_path),
        ("tracked_tickers", cfg.tracked_tickers_path),
    ):
        exists = p.exists()
        add(f"path:{label}", "ok" if exists else "error",
            str(p) if exists else f"{p} — MISSING")
    if cfg.theses_path is not None and not cfg.theses_path.exists():
        add("path:theses", "warn",
            f"{cfg.theses_path} — set but missing; no-theses variant will run")

    # --- output dirs must be writable (blocking) ---
    for label, p in (
        ("brief_archive", cfg.brief_archive_path),
        ("trigger_log_dir", cfg.trigger_log_path.parent),
        ("cross_source_log_dir", cfg.cross_source_log_path.parent),
    ):
        ok, detail = _check_writable_dir(p)
        add(f"writable:{label}", "ok" if ok else "error", detail)

    # --- alert sink prerequisites (sink-aware; GATE 2 default is the
    # abelard_queue enqueue-only sink, which needs no external binary) ---
    synth_cfg, synth_err = _load_synthesis_config_for_cli(cfg)
    sink_type = synth_cfg.alert_sink.type if synth_cfg else "unknown"
    if synth_err:
        err_detail = (synth_err.get("error_detail")
                      if isinstance(synth_err, dict) else str(synth_err))
        add("alert_sink", "warn",
            f"synthesis config unreadable ({err_detail}); sink checks skipped")
    else:
        add("alert_sink", "ok", f"type={sink_type}")
    if sink_type == "abelard_queue" and synth_cfg is not None:
        aq = synth_cfg.alert_sink.abelard_queue
        raw = os.environ.get(aq.db_path_env, "").strip() or aq.db_path_default
        queue_dir = Path(raw).expanduser().parent
        ok, detail = _check_writable_dir(queue_dir)
        add("writable:abelard_queue_dir", "ok" if ok else "error", detail)
    if sink_type == "signal":
        sig = shutil.which("signal-cli")
        add("signal-cli", "ok" if sig else "warn",
            sig or "not on PATH — alert dispatch fails (briefs still archived)")
        jav = shutil.which("java")
        add("java", "ok" if jav else "warn",
            jav or "not on PATH — signal-cli needs a Java runtime")

    # --- DB schema + active themes (blocking) ---
    conn = connect(cfg.db_path)
    try:
        ver = schema_version(conn)
        if ver == 0:
            add("database", "error",
                f"{cfg.db_path} — no schema applied; run `news-watch-daemon db init`")
        else:
            add("database", "ok", f"{cfg.db_path} — schema v{ver}")
            active = [e for e in list_themes(conn) if e.status == "active"]
            add("active_themes", "ok" if active else "error",
                f"{len(active)} active" if active
                else "none — run `news-watch-daemon themes load`")
    finally:
        conn.close()

    summary = {
        "ok": sum(1 for c in checks if c["status"] == "ok"),
        "warn": sum(1 for c in checks if c["status"] == "warn"),
        "error": sum(1 for c in checks if c["status"] == "error"),
    }
    payload = {"db_path": str(cfg.db_path), "summary": summary, "checks": checks}
    if summary["error"]:
        return build_error(
            status="error", source="internal",
            detail=f"doctor found {summary['error']} blocking problem(s); see data.checks",
            data=payload,
        )
    completeness = "partial" if summary["warn"] else "complete"
    return build_ok(payload, source="internal", data_completeness=completeness)


def _handle_status(args: argparse.Namespace, cfg: Config) -> dict[str, Any]:
    """Show schema version, daemon heartbeats, and per-source health.

    Pass B adds `source_health` to the envelope so the operator can see
    each registered source's last attempt/success and failure counter
    via one CLI call.
    """
    warnings: list[dict[str, Any]] = []
    conn = connect(cfg.db_path)
    try:
        version = schema_version(conn)
        heartbeats: list[dict[str, Any]] = []
        source_health: list[dict[str, Any]] = []
        if version == 0:
            warnings.append(
                make_warning(
                    field="schema_version",
                    reason="config_drift",
                    source="internal",
                    detail="schema not applied; run `news-watch-daemon db init`",
                )
            )
        else:
            heartbeats = read_heartbeats(conn)
            source_health = read_source_health(conn)
    finally:
        conn.close()
    completeness = "partial" if warnings else "complete"
    return build_ok(
        {
            "db_path": str(cfg.db_path),
            "schema_version": version,
            "heartbeats": heartbeats,
            "source_health": source_health,
        },
        source="internal",
        data_completeness=completeness,
        warnings=warnings,
    )


# ---------- dispatch ---------------------------------------------------


Handler = Callable[[argparse.Namespace, Config], dict[str, Any]]


HANDLERS: dict[str, Handler] = {
    "db init": _handle_db_init,
    "db migrate": _handle_db_migrate,
    "db backfill-language": _handle_db_backfill_language,
    "db backfill-translation": _handle_db_backfill_translation,
    "db retag": _handle_db_retag,
    "translate": _handle_translate,
    "themes load": _handle_themes_load,
    "themes list": _handle_themes_list,
    "status": _handle_status,
    "doctor": _handle_doctor,
    "scrape": _handle_scrape,
    "proposals list": _handle_proposals_list,
    "proposals show": _handle_proposals_show,
    "proposals approve": _handle_proposals_approve,
    "proposals reject": _handle_proposals_reject,
    "briefs list": _handle_briefs_list,
    "briefs show": _handle_briefs_show,
    "headlines recent": _handle_headlines_recent,
    "synthesize": _handle_synthesize,
    "alert-sink test": _handle_alert_sink_test,
    "trigger-log tail": _handle_trigger_log_tail,
    "attention": _handle_attention,
}


# ---------- full-brief subcommand (Stage 2b-ii, 2026-05-29) -----------
#
# full-brief has different output semantics than other handlers:
#   - Default: render human-readable text to stdout
#   - --json-only: print JSON envelope to stdout
#   - --quiet: no stdout output (artifact write is the only side effect)
# Plus a third exit code (2) for primary-path failures per spec Section 3.
#
# Because of these unique semantics, full-brief bypasses the standard
# dispatch -> _emit_envelope -> 0/1-exit pipeline. main() special-cases it
# below.


def _compute_full_brief_exit_code(envelope: "FullBriefEnvelope") -> int:
    """Map FullBriefEnvelope state to exit code per spec Section 3.

    Per Mando's Stage 2b-ii forward-guidance:
      0 - Full Brief assembled successfully. pass_failures may be non-empty
          but only for SECONDARY metric failures (pass_f_footprint,
          frequency_diagnostic). The brief is healthy enough for normal
          consumption.
      2 - Brief assembled but a PRIMARY analytical path failed
          (scrape, Pass C, or Pass E). Scripted consumers should notice;
          downstream automation may want to skip the brief or alert.

    Exit 1 is reserved for infrastructure errors (config invalid, DB
    unreachable, can't construct envelope at all) and handled separately
    in the caller — never reached from this function.

    Convergence + frequency_diagnostic step failures don't trigger exit 2:
    convergence is total-over-valid-inputs per Stage 1 doctrine, and
    frequency_diagnostic failure means "Pass F footprint metric
    unavailable" which is a secondary concern — operator sees the
    pass_failures footnote.
    """
    health = envelope.envelope_health
    if health.scrape.status == "failed":
        return 2
    if health.pass_c.status == "failed":
        return 2
    if health.pass_e.status == "failed":
        return 2
    return 0


def _handle_full_brief(args: argparse.Namespace, cfg: Config) -> int:
    """Handle the `full-brief` subcommand. Returns exit code directly.

    Writes output to stdout per flag combinations:
      Default (no flags): render_full_brief(envelope) -> stdout
      --json-only: envelope.model_dump_json(indent=2) -> stdout
      --quiet: no stdout output

    Returns:
      0 — Full Brief assembled successfully (per _compute_full_brief_exit_code)
      1 — Unrecoverable error: assemble_full_brief raised, or the artifact
          write failed BEYOND the orchestrator's internal handling
      2 — Brief assembled with a primary-path failure (per spec Section 3)
    """
    # Stage 2b-ii live smoke discovery (2026-06-01): Windows stdout defaults
    # to cp1252 which doesn't support the Unicode characters the render
    # layer uses (→ U+2192, Δ U+0394, ✓ etc.). Force UTF-8 for consistent
    # cross-platform rendering — no-op on POSIX systems where stdout is
    # already UTF-8. errors='replace' converts any unencodable char to '?'
    # rather than crashing if a future render addition introduces a char
    # outside the BMP.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    raw_window_hours = args.window_hours
    window_hours = max(1, min(168, raw_window_hours))
    if window_hours != raw_window_hours:
        sys.stderr.write(
            f"window_hours clamped to {window_hours} (was {raw_window_hours}, "
            "bounds [1, 168])\n"
        )
        sys.stderr.flush()
    try:
        envelope = assemble_full_brief(
            cfg=cfg,
            window_hours=window_hours,
            no_scrape=args.no_scrape,
            sink_factory=None,   # spec § 13: Full Brief doesn't dispatch in v1
        )
    except Exception as exc:  # noqa: BLE001 — CLI boundary, last-resort catch
        _log = logging.getLogger("news_watch_daemon.cli")
        _log.exception("full-brief assembly raised an unrecoverable error")
        sys.stderr.write(f"ERROR: Full Brief assembly failed: {exc}\n")
        sys.stderr.flush()
        return 1

    # Output per flag combinations.
    if args.json_only:
        json.dump(
            envelope.model_dump(mode="json"),
            sys.stdout, indent=2, ensure_ascii=False, default=str,
        )
        sys.stdout.write("\n")
        sys.stdout.flush()
    elif args.quiet:
        # No stdout output. Artifact write inside assemble_full_brief is
        # the only side effect.
        pass
    else:
        # Default: human-readable rendering to stdout.
        sys.stdout.write(render_full_brief(envelope))
        sys.stdout.write("\n")
        sys.stdout.flush()

    # --- one-pass extras (2026-07-10): full artifact path + optional JSON
    # copy + in-process PDF. These write to STDERR (and their own files) so
    # they never pollute the stdout contract of --json-only. Collapses the
    # old two-command dance: full-brief then locate-JSON then read-brief --pdf.
    try:
        artifact_path = brief_path(cfg.brief_archive_path, envelope.brief_id)
        sys.stderr.write(f"Artifact (JSON): {artifact_path}\n")
        sys.stderr.flush()
    except ArchiveError:
        # A malformed brief_id shouldn't sink an otherwise-good run; the
        # render footer still names the file.
        pass

    if args.out:
        try:
            out_path = Path(args.out)
            if out_path.parent:
                out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(
                    envelope.model_dump(mode="json"),
                    indent=2, ensure_ascii=False, default=str,
                ),
                encoding="utf-8",
            )
        except OSError as exc:
            sys.stderr.write(f"ERROR: full-brief --out write failed: {exc}\n")
            sys.stderr.flush()
            return 1
        sys.stderr.write(f"Wrote JSON: {out_path}\n")
        sys.stderr.flush()

    if args.pdf:
        try:
            written = render_full_brief_pdf(envelope, args.pdf)
        except PdfRenderError as exc:
            sys.stderr.write(f"ERROR: full-brief --pdf failed: {exc}\n")
            sys.stderr.flush()
            return 1
        sys.stderr.write(
            f"Wrote PDF: {written} ({written.stat().st_size} bytes)\n"
        )
        sys.stderr.flush()

    return _compute_full_brief_exit_code(envelope)


# ---------- run subcommand (one-pass operating cycle, 2026-07-10) -----------
#
# `run` is the single cold-start-safe command: ensure schema + themes, then
# assemble a full brief and optionally render a PDF. It shares full-brief's
# stdout-render semantics + exit codes, so main() special-cases it too.


def _handle_run(args: argparse.Namespace, cfg: Config) -> int:
    """One-pass operating cycle. Returns an exit code directly (like full-brief).

    Steps, each idempotent and cheap to repeat:
      1. ensure schema (db init == migrate) — cold-start needs no prior setup
      2. ensure themes loaded (idempotent upsert from themes_dir)
      3. assemble the full brief + emit output (delegates to _handle_full_brief,
         honoring --window-hours/--no-scrape/--pdf/--out/--quiet/--json-only)

    A failure in step 1 or 2 aborts with a clear stderr message and exit 1
    BEFORE any scrape/LLM spend. Progress is logged to stderr so stdout stays
    the brief render (or JSON) byte-identical to what `full-brief` produces.
    """
    log = logging.getLogger("news_watch_daemon.cli")

    init_env = _handle_db_init(args, cfg)
    if init_env["status"] != "ok":
        sys.stderr.write(
            f"ERROR: run aborted at ensure-schema: {init_env['error_detail']}\n"
        )
        sys.stderr.flush()
        return 1
    log.info("run: schema ready (v%s)", init_env["data"].get("schema_version"))

    themes_env = _handle_themes_load(args, cfg)
    if themes_env["status"] != "ok":
        sys.stderr.write(
            f"ERROR: run aborted at ensure-themes: {themes_env['error_detail']}\n"
        )
        sys.stderr.flush()
        return 1
    log.info(
        "run: %s theme(s) loaded from %s",
        themes_env["data"].get("loaded_count"), cfg.themes_dir,
    )

    return _handle_full_brief(args, cfg)


# ---------- read-brief subcommand (reload + render persisted artifact) -------
#
# read-brief shares full-brief's stdout-render output semantics (human-readable
# text, NOT a JSON envelope), so it bypasses the standard dispatch ->
# _emit_envelope -> 0/1 pipeline and is special-cased in main() alongside
# full-brief.
#
# It reuses the SAME renderer (render_full_brief) and the SAME schema
# (FullBriefEnvelope) as generation — no second renderer, no forked validation.
# Generation already renders via the reusable render_full_brief(envelope), so
# no extraction/refactor was needed; read-brief just loads-then-renders.
#
# Exit codes (fail-loud doctrine — the subcommand owns failure cases):
#   0 — artifact loaded, validated, and rendered successfully
#   1 — load/validate failure (missing file, malformed JSON, wrong brief_type,
#       schema mismatch). Explicit error to stderr naming the path; no stdout.
# Note: read-brief does NOT recompute full-brief's exit-code-2 from the
# persisted envelope's health — exit 2 is a generation-time primary-path
# signal. A re-read of a degraded brief still renders fine (the ENVELOPE HEALTH
# / PASS FAILURES sections surface the degradation in the output itself), so a
# successful read is exit 0 regardless of the persisted brief's internal health.


def _handle_read_brief(args: argparse.Namespace, cfg: Config) -> int:
    """Handle the `read-brief <path>` subcommand. Returns exit code directly.

    Loads the persisted Full Brief JSON at `args.path`, validates it against
    the FullBriefEnvelope schema, and renders it to stdout via the shared
    render_full_brief — byte-identical to `full-brief`'s default rendering.

    Returns:
      0 — loaded + rendered successfully
      1 — path missing, malformed JSON, wrong brief_type, or schema mismatch
    """
    # Match full-brief's Windows-stdout discipline: the render layer emits
    # Unicode (→ U+2192, Δ U+0394) that cp1252 can't encode. Force UTF-8 so
    # the reloaded render is identical cross-platform; no-op on POSIX.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    path = Path(args.path)
    try:
        envelope = load_full_brief_from_path(path)
    except FullBriefLoadError as exc:
        sys.stderr.write(f"ERROR: read-brief failed: {exc}\n")
        sys.stderr.flush()
        return 1

    # --pdf: render to a PDF file (fail-loud) instead of text to stdout.
    pdf_out = getattr(args, "pdf", None)
    if pdf_out:
        try:
            written = render_full_brief_pdf(envelope, pdf_out)
        except PdfRenderError as exc:
            sys.stderr.write(f"ERROR: read-brief --pdf failed: {exc}\n")
            sys.stderr.flush()
            return 1
        sys.stdout.write(f"Wrote PDF: {written} ({written.stat().st_size} bytes)\n")
        sys.stdout.flush()
        return 0

    sys.stdout.write(render_full_brief(envelope))
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


def dispatch(args: argparse.Namespace, *, cfg: Config) -> dict[str, Any]:
    leaf = command_path(args)
    handler = HANDLERS.get(leaf)
    if handler is not None:
        return handler(args, cfg)
    # Every parser-advertised leaf is mapped in HANDLERS (or special-cased in
    # main() for run/full-brief/read-brief). A miss here is a wiring bug, not a
    # user error — surface it loudly rather than as a silent "not implemented".
    return build_error(
        status="error",
        source="internal",
        detail=f"internal error: no handler mapped for leaf {leaf!r}",
    )


# ---------- envelope emission -----------------------------------------


def _emit_envelope(envelope: dict[str, Any]) -> None:
    json.dump(envelope, sys.stdout, indent=2, ensure_ascii=False, default=str)
    sys.stdout.write("\n")
    sys.stdout.flush()


# ---------- main ------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Minimal stderr logging before config loads. Real config replaces this.
    logging.basicConfig(
        level="WARNING",
        stream=sys.stderr,
        format="%(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("news_watch_daemon.cli")

    # Fold `.env` into the process env before building Config, so a bare
    # `news-watch-daemon <cmd>` works without a shell wrapper. Real env vars
    # win; disabled under tests via NEWS_WATCH_NO_ENV_FILE. Key NAMES only are
    # logged (at DEBUG) — never values, which may be secrets.
    loaded_env_keys = load_env_file()

    try:
        cfg = Config.from_env()
        configure_logging(cfg)
        if loaded_env_keys:
            log.debug(
                ".env loaded %d key(s): %s",
                len(loaded_env_keys), ", ".join(sorted(loaded_env_keys)),
            )
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        envelope = build_error(
            status="error",
            source="internal",
            detail=f"configuration error: {exc}",
        )
        _emit_envelope(envelope)
        return 1

    # run subcommand: one-pass ensure-schema + ensure-themes + full-brief.
    # Shares full-brief's stdout-render semantics + exit codes, so it bypasses
    # the standard dispatch -> _emit_envelope -> 0/1 flow just like full-brief.
    if command_path(args) == "run":
        try:
            return _handle_run(args, cfg)
        except Exception as exc:  # noqa: BLE001 — CLI boundary
            log.exception("unhandled error in run")
            sys.stderr.write(f"ERROR: unhandled exception in run: {exc}\n")
            sys.stderr.flush()
            return 1

    # full-brief subcommand has unique output semantics (rendered text vs
    # JSON vs silent) and a third exit code (2) per spec Section 3. Bypass
    # the standard dispatch -> _emit_envelope -> 0/1 flow.
    if command_path(args) == "full-brief":
        try:
            return _handle_full_brief(args, cfg)
        except Exception as exc:  # noqa: BLE001 — CLI boundary
            log.exception("unhandled error in full-brief")
            sys.stderr.write(f"ERROR: unhandled exception in full-brief: {exc}\n")
            sys.stderr.flush()
            return 1

    # read-brief shares full-brief's stdout-render output semantics (rendered
    # text, not a JSON envelope), so it also bypasses the standard
    # dispatch -> _emit_envelope -> 0/1 flow.
    if command_path(args) == "read-brief":
        try:
            return _handle_read_brief(args, cfg)
        except Exception as exc:  # noqa: BLE001 — CLI boundary
            log.exception("unhandled error in read-brief")
            sys.stderr.write(f"ERROR: unhandled exception in read-brief: {exc}\n")
            sys.stderr.flush()
            return 1

    try:
        envelope = dispatch(args, cfg=cfg)
    except Exception as exc:  # noqa: BLE001 — CLI boundary, last-resort catch
        log.exception("unhandled error in %s", command_path(args))
        envelope = build_error(
            status="error",
            source="internal",
            detail=f"unhandled exception: {exc}",
        )

    _emit_envelope(envelope)
    return 0 if envelope["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
