"""CLI entry point for the Biz Daemon.

Invocation contract (what Abelard can rely on):
  - Exactly one JSON object (the §8 output contract) is written to stdout per
    invocation.
  - Logs, warnings, and tracebacks go to stderr, never stdout.
  - Exit 0 iff the scrape produced no errors (`errors == []`).
  - Exit 1 if `errors` is non-empty or an uncaught exception occurs. A JSON
    object is still emitted so the caller always has structured output.

On-demand only: one scrape per invocation. There is no scheduler and no loop.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from . import __version__, blacklist
from .config import Config, ConfigError, configure_logging, resolve_blacklist_path
from .orchestrator import run_scrape
from .tableview import render_table


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="biz-daemon",
        description=(
            "Retail-sentiment sensor over 4chan /biz/ /smg/. With no "
            "subcommand, runs one scrape and prints the structured JSON "
            "contract. The daemon extracts and classifies; Abelard judges."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"biz-daemon {__version__}"
    )

    # Output format for the scrape path. JSON (the data contract) is the
    # default; --table renders the SAME object as a human-readable table.
    out_group = parser.add_mutually_exclusive_group()
    out_group.add_argument(
        "--json", action="store_const", dest="output", const="json",
        help="emit the JSON output contract (default)",
    )
    out_group.add_argument(
        "--table", action="store_const", dest="output", const="table",
        help="render the same output object as a human-readable table",
    )
    parser.set_defaults(output="json")

    sub = parser.add_subparsers(dest="command")
    bl = sub.add_parser(
        "blacklist",
        help="denylist file maintenance (pure file edit, no LLM, no scrape)",
    )
    bl_sub = bl.add_subparsers(dest="bl_action", required=True)

    bl_add = bl_sub.add_parser("add", help="append one or more tokens")
    bl_add.add_argument("tokens", nargs="+", metavar="TOKEN")

    bl_remove = bl_sub.add_parser("remove", help="remove one or more tokens")
    bl_remove.add_argument("tokens", nargs="+", metavar="TOKEN")

    bl_sub.add_parser("list", help="print the current denylist")

    return parser


def _handle_blacklist(args: argparse.Namespace) -> int:
    """Pure file maintenance on the slang denylist. Takes effect next scrape."""
    path = resolve_blacklist_path()
    current = lambda: sorted(blacklist.load_blacklist(path)) if path.exists() else []

    if args.bl_action == "add":
        added, skipped = blacklist.add_tokens(path, args.tokens)
        _emit(
            {
                "command": "blacklist add",
                "added": added,
                "skipped": skipped,
                "denylist_size": len(current()),
            }
        )
        return 0

    if args.bl_action == "remove":
        removed = blacklist.remove_tokens(path, args.tokens)
        _emit(
            {
                "command": "blacklist remove",
                "removed": removed,
                "denylist_size": len(current()),
            }
        )
        return 0

    # list
    denylist = current()
    _emit({"command": "blacklist list", "denylist": denylist, "denylist_size": len(denylist)})
    return 0


def _emit(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


def _output(payload: dict[str, Any], mode: str) -> None:
    """Render the scrape payload as JSON (the contract) or a text table."""
    if mode == "table":
        sys.stdout.write(render_table(payload))
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        _emit(payload)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    log = logging.getLogger("biz_daemon.cli")

    # The table view uses a couple of non-ASCII glyphs; keep stdout from
    # crashing on a legacy code page (e.g. Windows cp1252).
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # Denylist maintenance is pure file editing — no config, no key, no scrape.
    if args.command == "blacklist":
        try:
            return _handle_blacklist(args)
        except Exception as exc:
            log.exception("blacklist maintenance failed")
            _emit({"command": "blacklist", "errors": [f"blacklist: {exc}"]})
            return 1

    try:
        cfg = Config.from_env()
        configure_logging(cfg)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        _output(
            {
                "scrape_ts": None,
                "threads": [],
                "tickers": [],
                "cost": {"haiku_calls": 0, "input_tokens": 0, "output_tokens": 0},
                "errors": [f"config: {exc}"],
            },
            args.output,
        )
        return 1

    try:
        payload = run_scrape(cfg)
    except Exception as exc:  # never crash without structured output
        log.exception("unhandled error during scrape")
        _output(
            {
                "scrape_ts": None,
                "threads": [],
                "tickers": [],
                "cost": {"haiku_calls": 0, "input_tokens": 0, "output_tokens": 0},
                "errors": [f"unhandled: {exc}"],
            },
            args.output,
        )
        return 1

    _output(payload, args.output)
    return 0 if not payload.get("errors") else 1


if __name__ == "__main__":
    sys.exit(main())
