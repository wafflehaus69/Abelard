"""CLI: load watchlist(s), run the spine, emit the ScanEnvelope as JSON.

Invocation contract (mirrors BizDaemon, the daemon Chatter extends):
  - Exactly one JSON object (the scan envelope) is written to stdout per
    invocation.
  - Logs, warnings, and tracebacks go to stderr, never stdout.
  - Exit 0 iff the scan produced no errors (`errors == []`); exit 1 otherwise. A
    JSON object is still emitted on failure so the caller always has structured
    output.

Order 1: no source plugins, so the run fans out over zero sources — the envelope
carries the canonical timestamp, the derived windows, and the validated watchlist
summaries with an empty record list.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from . import __version__
from .config import Config, ConfigError, configure_logging
from .errors import ChatterDaemonError
from .orchestrator import run_scan
from .watchlist import load_all_watchlists, load_watchlist


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chatter-daemon",
        description=(
            "Multi-source retail-chatter sensor. Loads named watchlists and emits "
            "the structured scan envelope. The daemon extracts and classifies; "
            "Abelard judges."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"chatter-daemon {__version__}"
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--watchlist", metavar="NAME", help="load one watchlist by name")
    target.add_argument(
        "--all",
        action="store_true",
        help="load every watchlist in the watchlists/ directory",
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
        "records": [],
        "errors": [message],
    }


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

    try:
        cfg = Config.from_env()
        configure_logging(cfg)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        _emit(_error_envelope(f"config: {exc}"))
        return 1

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

    envelope = run_scan(watchlists)
    _emit(envelope.model_dump(mode="json"))
    return 0 if not envelope.errors else 1


if __name__ == "__main__":
    sys.exit(main())
