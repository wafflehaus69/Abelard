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

from . import __version__, baseline
from .aggregate import build_aggregate
from .config import Config, ConfigError, configure_logging
from .errors import ChatterDaemonError
from .orchestrator import run_scan
from .persist import ArchiveError, load_result, make_scan_id, write_result
from .render import render_chatter
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

    # scan
    try:
        cfg = Config.from_env()
        configure_logging(cfg)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        _emit(_error_envelope(f"config: {exc}"))
        return 1
    return _cmd_scan(args, cfg, log)


if __name__ == "__main__":
    sys.exit(main())
