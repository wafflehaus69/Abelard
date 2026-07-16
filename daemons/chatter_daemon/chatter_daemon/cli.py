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

from . import __version__, attention_store, baseline, ticker_universe
from .aggregate import build_aggregate
from .attention import prune_cold, run_attention_scan
from .config import Config, ConfigError, configure_logging
from .discovery import format_distribution, run_dry_run
from .errors import ChatterDaemonError
from .matching import Matcher
from .orchestrator import run_scan
from .persist import (
    ArchiveError,
    load_attention_result,
    load_result,
    make_scan_id,
    peek_scan_mode,
    write_attention_result,
    write_result,
)
from .render import render_attention, render_chatter
from .sources.registry import build_news_summarizer, build_sources
from .watchlist import load_all_watchlists, load_watchlist
from .windows import iso_z


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

    rep = sub.add_parser("report", help="render a persisted scan as a client-facing PDF")
    rep.add_argument("path", help="path to a persisted scan result")
    rep.add_argument("--out", metavar="FILE", help="output PDF path (default: {scan_id}.pdf)")
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

    # CH-SRC-2: one per-ticker news summary over the Finnhub + Yahoo (+ AV) headlines TOGETHER,
    # after the fan-out so both feeds are in hand. Cost accumulates into envelope.cost; a failure
    # warns and yields no summaries but never sinks the scan (no key -> auto-off, empty result).
    news_summaries: dict[tuple[str, str], str] = {}
    try:
        news_summaries, sum_warnings = build_news_summarizer(cfg).summarize(
            envelope.records, watchlists, cost=envelope.cost
        )
        envelope.errors.extend(sum_warnings)
    except Exception as exc:  # never crash the scan over the summary step
        log.warning("news summary step failed: %s", exc)
        envelope.errors.append(f"news_summary: {exc}")

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
            now=now,
            news_summaries=news_summaries,
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

    # Order 19: raw-scrape history dump (headlines / StockTwits / Twitter) beside the
    # archive. Best-effort — a failure warns but never fails the scan.
    if envelope.raw_items:
        try:
            from .history import write_history

            hpath = write_history(
                cfg.history_root,
                envelope.raw_items,
                scan_id=scan_id,
                canonical_ts=envelope.canonical_ts,
            )
            log.info("raw history: %s", hpath)
        except Exception as exc:  # never fail the scan over the side dump
            log.warning("history dump failed: %s", exc)
            result.errors.append(f"history: {exc}")

    _emit(result.model_dump(mode="json"))

    # Total-source-failure rule: sources attempted, every one errored, zero records.
    if envelope.sources and not envelope.records and all(not s.ok for s in envelope.sources):
        return 1
    return rc


def _cmd_read_chatter(args: argparse.Namespace, log: logging.Logger) -> int:
    path = Path(args.path)
    try:
        # Dispatch on the persisted scan_mode: watchlist scans and attention scans
        # render through their own views.
        if peek_scan_mode(path) == "attention":
            text = render_attention(load_attention_result(path))
        else:
            text = render_chatter(load_result(path))
    except ArchiveError as exc:
        log.error("read-chatter: %s", exc)
        sys.stderr.write(f"read-chatter error: {exc}\n")
        return 1
    sys.stdout.write(text + "\n")
    sys.stdout.flush()
    return 0


def _report_aliases(cfg: Config, log: logging.Logger) -> dict[str, list[str]] | None:
    """`{SYMBOL: [name words]}` from the watchlists + shared company-name map, for the
    report's ticker-relevance headline filter. Best-effort: an unreadable watchlists dir
    or name map degrades to symbol-only matching (None), never fails the report."""
    try:
        from abelard_common.company_aliases import load_name_map

        from .matching import build_name_map

        shared = load_name_map(cfg.company_names_path)
        aliases: dict[str, set[str]] = {}
        for wl in load_all_watchlists(cfg.watchlists_dir):
            for name, sym in build_name_map(wl, shared).items():
                aliases.setdefault(sym, set()).add(name)
        return {k: sorted(v) for k, v in aliases.items()} or None
    except Exception as exc:
        log.warning("report: name aliases unavailable (symbol-only headlines): %s", exc)
        return None


def _cmd_report(args: argparse.Namespace, log: logging.Logger) -> int:
    path = Path(args.path)
    try:
        result = (
            load_attention_result(path)
            if peek_scan_mode(path) == "attention"
            else load_result(path)
        )
    except ArchiveError as exc:
        log.error("report: %s", exc)
        sys.stderr.write(f"report error: {exc}\n")
        return 1
    # Ticker-relevance aliases for headline sampling (watchlist mode). Built best-effort
    # so a missing watchlists dir degrades to symbol-only matching, never fails the report.
    try:
        cfg = Config.from_env()
    except ConfigError:
        cfg = Config()  # defaults — the report only needs paths, not keys
    name_aliases = _report_aliases(cfg, log)
    from .report import render_report, report_default_filename

    out = Path(args.out) if args.out else Path(report_default_filename(result.canonical_ts))
    try:
        render_report(result, out, name_aliases=name_aliases)
    except Exception as exc:  # surface a render failure loudly, never a half file
        log.error("report: PDF render failed: %s", exc)
        sys.stderr.write(f"report error: PDF render failed: {exc}\n")
        return 1
    sys.stdout.write(f"wrote {out}\n")
    sys.stdout.flush()
    return 0


def _cmd_attention(args: argparse.Namespace, cfg: Config, log: logging.Logger) -> int:
    from abelard_common import fourchan_fetch, ticker_noise
    from abelard_common.http_client import HttpClient

    from .sources.stocktwits import StockTwitsClient

    chatter_log = logging.getLogger("chatter_daemon")
    now = int(time.time())
    conn = baseline.connect(cfg.baseline_db_path)
    ticker_universe.init_universe_table(conn)
    attention_store.init_attention_table(conn)

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

    fetcher = fourchan_fetch.Fetcher(user_agent=cfg.user_agent, logger=chatter_log)
    # StockTwits trending: public endpoint, browser UA, no key. Degrade-clean — a CF
    # wall raises inside the pull and becomes a soft surface warning, never a crash.
    stocktwits_client = StockTwitsClient(
        logger=logging.getLogger("chatter_daemon.stocktwits")
    )
    surfaces = run_dry_run(
        matcher=matcher,
        universe=universe.symbols,
        now=now,
        fetcher=fetcher,
        stocktwits_client=stocktwits_client,
    )

    # --dry-run: calibration only — print the distribution; no store, gate, or persist.
    if args.dry_run:
        out = format_distribution(surfaces)
        if universe.warning:
            out = f"[universe: {universe.warning}]\n" + out
        sys.stdout.write(out + "\n")
        sys.stdout.flush()
        return 0

    # Real scan: gate -> store -> salience/velocity -> amplified -> prune -> persist.
    canonical_ts = iso_z(now)
    try:
        watchlist_symbols = {
            w.name: {s.symbol for s in w.active_tickers}
            for w in load_all_watchlists(cfg.watchlists_dir)
        }
    except ChatterDaemonError as exc:
        log.warning("attention: watchlists unavailable for amplified flag: %s", exc)
        watchlist_symbols = {}

    result = run_attention_scan(
        conn=conn,
        surfaces=surfaces,
        watchlist_symbols=watchlist_symbols,
        floors=cfg.attention_floors,
        scan_id=make_scan_id(canonical_ts, ["attention"]),
        canonical_ts=canonical_ts,
        now=now,
        baseline_window=cfg.baseline_window,
        baseline_min_obs=cfg.baseline_min_obs,
        spike_z_threshold=cfg.spike_z_threshold,
    )
    if universe.warning:
        result.errors.append(f"universe: {universe.warning}")

    rc = 0
    try:
        result.pruned = prune_cold(
            conn, now=now, archive_root=cfg.archive_root, generated_ts=now
        )
    except ArchiveError as exc:
        log.error("attention: prune-to-cold failed: %s", exc)
        result.errors.append(f"prune: {exc}")
        rc = 1
    try:
        write_attention_result(cfg.archive_root, result)
    except ArchiveError as exc:
        log.error("attention: archive write failed (output preserved on stdout): %s", exc)
        result.errors.append(f"archive: {exc}")
        rc = 1

    _emit(result.model_dump(mode="json"))
    # Total discovery failure: surfaces attempted, all failed, nothing admitted.
    if result.surfaces and not result.tickers and all(not s.ok for s in result.surfaces):
        return 1
    return rc


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
    if args.command == "report":
        return _cmd_report(args, log)

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
