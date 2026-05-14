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
import sqlite3
import sys
import time
from dataclasses import asdict
from typing import Any, Callable

from .config import Config, ConfigError, configure_logging
from .db import (
    connect,
    init_db,
    list_themes,
    read_heartbeats,
    read_source_health,
    schema_version,
    upsert_themes,
)
from .envelope import Source, build_error, build_ok, make_warning
from .http_client import HttpClient
from .scrape.factory import build_sources
from .scrape.orchestrator import PerSourceResult, ScrapeResult, run_scrape, write_heartbeat
from .scrape.ticker_extract import TickerExtractError, load_tracked_tickers
from .synthesize.archive import ArchiveError, list_brief_ids, read_brief
from .synthesize.proposals_store import (
    ProposalsStoreError,
    append_resolved,
    find_proposal,
    read_pending,
    remove_proposal,
)
from .synthesize.theme_mutator import ThemeMutationError, apply_proposal_to_theme
from .theme_config import ThemeLoadError, load_all_themes


# ---------- parser ------------------------------------------------------


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
        help="Restrict synthesis to a single theme_id (default: all due themes).",
    )

    top.add_parser("alert-check", help="Re-evaluate alert conditions across themes.")

    top.add_parser("status", help="Report daemon component heartbeats and schema version.")

    # ---- themes (registry) ----

    p_themes = top.add_parser("themes", help="Theme registry management.")
    themes_sub = p_themes.add_subparsers(dest="themes_action", required=True)
    themes_sub.add_parser("list", help="List themes currently in the registry.")
    themes_sub.add_parser(
        "load",
        help="Parse YAML theme files and upsert them into the registry.",
    )

    # ---- theme (singular: one-theme inspection) ----

    p_theme = top.add_parser("theme", help="Inspect a single theme.")
    theme_sub = p_theme.add_subparsers(dest="theme_action", required=True)
    p_show = theme_sub.add_parser("show", help="Show the latest narrative for a theme.")
    p_show.add_argument("theme_id")
    p_history = theme_sub.add_parser("history", help="Show narrative history for a theme.")
    p_history.add_argument("theme_id")
    p_history.add_argument(
        "--days", type=int, default=30,
        help="Days of narrative history to return (1..365). Default 30.",
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

    # ---- alerts ----

    p_alerts = top.add_parser("alerts", help="Inspect alert history.")
    alerts_sub = p_alerts.add_subparsers(dest="alerts_action", required=True)
    p_a_recent = alerts_sub.add_parser("recent", help="Recent alerts across all themes.")
    p_a_recent.add_argument(
        "--days", type=int, default=7,
        help="Days back to look (1..90). Default 7.",
    )

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

    return parser


# ---------- leaf-path helpers ------------------------------------------

_NESTED_DEST = {
    "themes": "themes_action",
    "theme": "theme_action",
    "headlines": "headlines_action",
    "alerts": "alerts_action",
    "proposals": "proposals_action",
    "briefs": "briefs_action",
    "db": "db_action",
}


def command_path(args: argparse.Namespace) -> str:
    """Return the leaf path, e.g. 'db init', 'scrape', 'themes load'."""
    top = args.command
    nested = _NESTED_DEST.get(top)
    if nested is None:
        return top
    return f"{top} {getattr(args, nested)}"


# ---------- handlers: stubs -------------------------------------------


def _stub_envelope(leaf: str, detail: str) -> dict[str, Any]:
    return build_error(
        status="error",
        source="internal",
        detail=f"{leaf}: not implemented in foundation pass",
        warnings=[
            make_warning(
                field=leaf,
                reason="not_implemented",
                source="internal",
                detail=detail,
            )
        ],
    )


_STUB_DETAILS: dict[str, str] = {
    "synthesize": "implemented in synthesis brief",
    "alert-check": "implemented in alert brief",
    "theme show": "implemented in synthesis brief (depends on narrative storage)",
    "theme history": "implemented in synthesis brief",
    "alerts recent": "implemented in alert brief",
}


# ---------- handlers: real --------------------------------------------


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
        if schema_version(conn) == 0:
            return build_error(
                status="error",
                source="internal",
                detail="database has no schema applied. Run `news-watch-daemon db init` first.",
            )
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
        if schema_version(conn) == 0:
            return build_error(
                status="error",
                source="internal",
                detail="database has no schema applied. Run `news-watch-daemon db init` first.",
            )
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
        if schema_version(conn) == 0:
            return build_error(
                status="error",
                source="internal",
                detail="database has no schema applied. Run `news-watch-daemon db init` first.",
            )

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

        try:
            result = run_scrape(conn, sources, themes, tracked_tickers=tracked_tickers)
            write_heartbeat(
                conn,
                status="ok",
                duration_ms=result.duration_ms,
                error_detail=None,
            )
            return _scrape_result_to_envelope(result)
        except Exception as exc:  # noqa: BLE001 — never let a scrape exception kill the CLI
            try:
                write_heartbeat(conn, status="error", duration_ms=0, error_detail=str(exc))
            except Exception:  # noqa: BLE001 — heartbeat itself can fail on DB unreachable
                pass
            raise
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


# ---------- handlers: briefs (Pass C Step 12) ----------


def _brief_summary(brief: Any) -> dict[str, Any]:
    """Project a Brief into the compact summary shape used by `briefs list`.

    Full Brief JSONs can run 1-10 KB; `briefs list` returns up to 500
    summaries so the envelope stays inspectable.
    """
    max_mat = max((e.materiality_score for e in brief.events), default=0.0)
    return {
        "brief_id": brief.brief_id,
        "generated_at": brief.generated_at,
        "themes_covered": list(brief.themes_covered),
        "events_count": len(brief.events),
        "max_materiality_score": round(max_mat, 3),
        "alerted": brief.dispatch.alerted,
        "channel": brief.dispatch.channel,
        "suppressed_reason": brief.dispatch.suppressed_reason,
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
        if args.theme and args.theme not in brief.themes_covered:
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
        if schema_version(conn) == 0:
            return build_error(
                status="error",
                source="internal",
                detail="database has no schema applied. Run `news-watch-daemon db init` first.",
            )
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
    "themes load": _handle_themes_load,
    "themes list": _handle_themes_list,
    "status": _handle_status,
    "scrape": _handle_scrape,
    "proposals list": _handle_proposals_list,
    "proposals show": _handle_proposals_show,
    "proposals approve": _handle_proposals_approve,
    "proposals reject": _handle_proposals_reject,
    "briefs list": _handle_briefs_list,
    "briefs show": _handle_briefs_show,
    "headlines recent": _handle_headlines_recent,
}


def dispatch(args: argparse.Namespace, *, cfg: Config) -> dict[str, Any]:
    leaf = command_path(args)
    handler = HANDLERS.get(leaf)
    if handler is not None:
        return handler(args, cfg)
    detail = _STUB_DETAILS.get(leaf, f"unmapped leaf: {leaf}")
    return _stub_envelope(leaf, detail)


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

    try:
        cfg = Config.from_env()
        configure_logging(cfg)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        envelope = build_error(
            status="error",
            source="internal",
            detail=f"configuration error: {exc}",
        )
        _emit_envelope(envelope)
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
