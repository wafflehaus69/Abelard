"""CLI: `scan` (aggregate + persist + emit) and `read-chatter` (render a persisted run).

Invocation:
  chatter-daemon scan --watchlist NAME | --all
      Run the spine, aggregate per-ticker against the baseline store (compute the
      anomaly read, THEN append this scan's counts), persist the result to the
      archive, and emit the AggregatedScanResult as one JSON object on stdout.
  chatter-daemon read-chatter PATH
      Render a persisted {scan_id}.json as a human-readable per-ticker view.

Contract (mirrors BizDaemon): exactly one JSON object to stdout per scan; logs to
stderr, never stdout; exit 0 iff the scan ran without a total source failure. Cost
telemetry is folded into the result BEFORE persistence — so an archive-write failure
can't lose it: the result (with cost) is still emitted on stdout and the write
failure is loud (exit 1).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from . import __version__, baseline, ticker_universe
from .aggregate import build_aggregate
from .config import Config, ConfigError, configure_logging
from .discovery import format_distribution, run_dry_run
from .errors import ChatterDaemonError
from .matching import Matcher
from .orchestrator import run_scan
from .persist import ArchiveError, load_result, make_scan_id, write_result
from .render import render_chatter
from .sources.reddit import PrawClient, RedditAuthError
from .sources.registry import build_sources
from .watchlist import load_all_watchlists, load_watchlist


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chatter-daemon",
        description=(
            "Multi-source retail-chatter sensor. The daemon extracts and classifies; "
            "Abelard judges."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"chatter-daemon {__version__}"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    scan = sub.add_parser(
        "scan", help="run a scan: aggregate against the baseline, persist, emit JSON"
    )
    target = scan.add_mutually_exclusive_group(required=True)
    target.add_argument("--watchlist", metavar="NAME", help="load one watchlist by name")
    target.add_argument(
        "--all", action="store_true", help="load every watchlist in watchlists/"
    )

    read = sub.add_parser("read-chatter", help="render a persisted {scan_id}.json")
    read.add_argument("path", help="path to a persisted scan result")

    attn = sub.add_parser(
        "attention",
        help="off-watchlist discovery (Phase 1: --dry-run calibration pull, no store)",
    )
    attn.add_argument(
        "--dry-run",
        action="store_true",
        help="calibration pull: print the per-source mention distribution, no store",
    )
    return parser


def _emit(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


def _error_envelope(message: str) -> dict[str, Any]:
    return {
        "schema_version": "1",
        "scan_mode": "watchlist",
        "canonical_ts": None,
        "windows": [],
        "watchlists": [],
        "sources": [],
        "tickers": [],
        "degraded": False,
        "cost": {},
        "errors": [message],
    }


def _cmd_scan(args: argparse.Namespace, cfg: Config, log: logging.Logger) -> int:
    try:
        if args.all:
            watchlists = load_all_watchlists(cfg.watchlists_dir)
        else:
            watchlists = [load_watchlist(args.watchlist, watchlists_dir=cfg.watchlists_dir)]
    except ChatterDaemonError as exc:
        log.error("watchlist load failed: %s", exc)
        _emit(_error_envelope(exc.to_error()))
        return 1
    except Exception as exc:  # never crash without structured output
        log.exception("unhandled error during watchlist load")
        _emit(_error_envelope(f"unhandled: {exc}"))
        return 1

    now = int(time.time())  # the daemon's single clock read, threaded everywhere
    envelope = run_scan(watchlists, sources=build_sources(cfg), now=now)

    try:
        conn = baseline.connect(cfg.baseline_db_path)
        baseline.init_db(conn)
        scan_id = make_scan_id(envelope.canonical_ts, [w.name for w in watchlists])
        result = build_aggregate(
            envelope,
            conn=conn,
            scan_id=scan_id,
            source_floors=cfg.source_floors,
            baseline_window=cfg.baseline_window,
            baseline_min_obs=cfg.baseline_min_obs,
            spike_z_threshold=cfg.spike_z_threshold,
            trend_spike_ratio=cfg.trend_spike_ratio,
            now=now,
        )
        conn.close()
    except baseline.BaselineError as exc:
        # Baseline store down: can't aggregate. Loud — but the cost is logged so it
        # isn't lost even though no artifact could be built.
        log.error("baseline store failed: %s", exc)
        log.error("scan cost (unpersisted): %s", envelope.cost.model_dump())
        _emit(_error_envelope(f"baseline: {exc}"))
        return 1

    # Persist. Cost is already on `result`, so a write failure can't lose it — we
    # still emit the result (with cost) on stdout and make the failure loud.
    rc = 0
    try:
        write_result(cfg.archive_root, result)
    except ArchiveError as exc:
        log.error("archive write failed (cost preserved in stdout): %s", exc)
        result.errors.append(f"archive: {exc}")
        rc = 1

    _emit(result.model_dump(mode="json"))

    # Total-source-failure rule: sources attempted, every one errored, zero records.
    if envelope.sources and not envelope.records and all(not s.ok for s in envelope.sources):
        return 1
    return rc


def _cmd_read_chatter(args: argparse.Namespace, log: logging.Logger) -> int:
    try:
        result = load_result(Path(args.path))
    except ArchiveError as exc:
        log.error("read-chatter: %s", exc)
        sys.stderr.write(f"read-chatter error: {exc}\n")
        return 1
    sys.stdout.write(render_chatter(result) + "\n")
    sys.stdout.flush()
    return 0


def _cmd_attention(args: argparse.Namespace, cfg: Config, log: logging.Logger) -> int:
    if not args.dry_run:
        sys.stderr.write("attention: use --dry-run (Phase 1; the live store is Phase 2)\n")
        return 2

    from abelard_common import fourchan_fetch, ticker_noise
    from abelard_common.http_client import HttpClient

    chatter_log = logging.getLogger("chatter_daemon")
    now = int(time.time())
    conn = baseline.connect(cfg.baseline_db_path)
    ticker_universe.init_universe_table(conn)

    # Validation universe: cache -> live Finnhub -> optional static fallback. Without
    # it there's nothing to validate against, so a hard failure is loud (exit 1).
    try:
        universe = ticker_universe.load_universe(
            conn,
            client=HttpClient(user_agent=cfg.user_agent, logger=chatter_log),
            api_key=cfg.finnhub_api_key or "",
            ttl_s=cfg.universe_cache_ttl_s,
            now=now,
            fallback_path=cfg.symbol_fallback_path,
        )
    except ChatterDaemonError as exc:
        log.error("attention: universe unavailable: %s", exc)
        sys.stderr.write(f"attention: universe unavailable: {exc}\n")
        return 1

    matcher = Matcher.for_universe(
        universe.symbols,
        blacklist=ticker_noise.load_blacklist(cfg.slang_blacklist_path),
        common_words=ticker_noise.load_common_words(cfg.common_words_path),
        allowlist=cfg.word_ticker_allowlist,
    )

    # Reddit surface: skip cleanly if creds are absent — the pull runs on /smg/ alone.
    reddit_client = None
    try:
        reddit_client = PrawClient(
            client_id=cfg.reddit_client_id,
            client_secret=cfg.reddit_client_secret,
            user_agent=cfg.reddit_user_agent,
        )
    except RedditAuthError as exc:
        log.warning("attention: reddit surface unavailable: %s", exc)

    fetcher = fourchan_fetch.Fetcher(user_agent=cfg.user_agent, logger=chatter_log)

    results = run_dry_run(
        matcher=matcher,
        universe=universe.symbols,
        now=now,
        reddit_client=reddit_client,
        subreddits=cfg.attention_subreddits,
        reddit_limit=cfg.attention_post_limit,
        fetcher=fetcher,
        stocktwits_client=None,  # walled; joins when the residential curl frees it
    )
    out = format_distribution(results)
    if universe.warning:
        out = f"[universe: {universe.warning}]\n" + out
    sys.stdout.write(out + "\n")
    sys.stdout.flush()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    log = logging.getLogger("chatter_daemon.cli")

    # Keep stdout from crashing on a legacy code page when emitting non-ASCII.
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    if args.command == "read-chatter":
        return _cmd_read_chatter(args, log)

    try:
        cfg = Config.from_env()
        configure_logging(cfg)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        if args.command == "attention":
            sys.stderr.write(f"attention: config error: {exc}\n")
            return 1
        _emit(_error_envelope(f"config: {exc}"))
        return 1

    if args.command == "attention":
        return _cmd_attention(args, cfg, log)
    return _cmd_scan(args, cfg, log)


if __name__ == "__main__":
    sys.exit(main())
