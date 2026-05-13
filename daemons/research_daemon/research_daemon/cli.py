"""CLI entry point for the Research Daemon.

Invocation contract (what Abelard can rely on):

  - Exactly one JSON envelope is written to stdout per invocation.
  - Logs, warnings, and tracebacks go to stderr, never stdout.
  - Exit 0 iff `envelope.status == "ok"` (partial completeness still counts
    as success — check `data_completeness` and `warnings` on the envelope).
  - Exit 1 for any non-ok status or uncaught exception. An envelope is
    still emitted in those cases so the caller always has structured
    output to parse.

Subcommands map 1:1 to capability functions in this package. See
`research-daemon --help` and `research-daemon <subcommand> --help` for
parameter details.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from .config import Config, ConfigError, configure_logging
from .detect_insider_activity import detect_insider_activity
from .detect_institutional_changes import detect_institutional_changes
from .envelope import build_error
from .fetch_insider_transactions import fetch_insider_transactions
from .fetch_institutional_holdings import fetch_institutional_holdings
from .fetch_news import fetch_news
from .fetch_quote import fetch_quote
from .fetch_sec_filing import fetch_sec_filing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="research-daemon",
        description=(
            "Read-only market-data + SEC research daemon. Emits a JSON envelope "
            "on stdout; logs on stderr."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- deep-read capabilities ----

    p = sub.add_parser("fetch-quote", help="Current price, day range, 52-week range.")
    p.add_argument("ticker")

    p = sub.add_parser("fetch-news", help="Recent company news within a day window.")
    p.add_argument("ticker")
    p.add_argument("--days", type=int, default=7, help="Days back to look (1..365). Default 7.")

    p = sub.add_parser("fetch-insider-transactions",
                       help="Recent Form 4 insider trades.")
    p.add_argument("ticker")
    p.add_argument("--days", type=int, default=30,
                   help="Days back to look (1..365). Default 30.")

    p = sub.add_parser("fetch-institutional-holdings",
                       help="Top-N 13F holders; optionally multi-quarter.")
    p.add_argument("ticker")
    p.add_argument("--top-n", type=int, default=10,
                   help="How many top holders to return per quarter (1..100). Default 10.")
    p.add_argument("--num-quarters", type=int, default=1,
                   help="How many recent quarters to return (1..8). Default 1 "
                        "(flat shape). With >=2 returns a quarters list.")

    p = sub.add_parser("fetch-sec-filing",
                       help="Recent SEC filings of a given type (metadata or body).")
    p.add_argument("ticker")
    p.add_argument("filing_type", help="E.g. 10-K, 10-Q, 8-K, DEF 14A.")
    p.add_argument("--limit", type=int, default=3, help="Max filings to return (1..40). Default 3.")
    p.add_argument("--include-body", action="store_true",
                   help="Fetch each filing's primary document and include extracted text.")
    p.add_argument("--max-body-chars", type=int, default=50_000,
                   help="Max characters of body to return per filing. Default 50000.")
    p.add_argument("--offset-chars", type=int, default=0,
                   help="Byte offset to start body slice at (for pagination). Default 0.")

    # ---- monitoring capabilities ----

    p = sub.add_parser("detect-institutional-changes",
                       help="QoQ 13F position changes across a ticker list.")
    p.add_argument("tickers", nargs="+", help="One or more tickers.")
    p.add_argument("--min-change-pct", type=int, default=10,
                   help="Minimum absolute change %% to report (1..1000). Default 10.")

    p = sub.add_parser("detect-insider-activity",
                       help="Material insider buys, cluster detection, first-time filers.")
    p.add_argument("tickers", nargs="+", help="One or more tickers.")
    p.add_argument("--lookback-days", type=int, default=30,
                   help="Recent-window days (1..365). Default 30.")
    p.add_argument("--min-value-usd", type=int, default=100_000,
                   help="Min transaction value USD to count as 'large buy'. Default 100000.")
    p.add_argument(
        "--first-time-detection",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable first-time-filer detection. Use --no-first-time-detection for "
             "cheap-sweep mode (skips the baseline call).",
    )
    p.add_argument("--first-time-lookback-days", type=int, default=365,
                   help="Baseline window in days (must exceed --lookback-days). Default 365.")

    return parser


def dispatch(args: argparse.Namespace, *, cfg: Config) -> dict[str, Any]:
    cmd = args.command
    if cmd == "fetch-quote":
        return fetch_quote(args.ticker, config=cfg)
    if cmd == "fetch-news":
        return fetch_news(args.ticker, days=args.days, config=cfg)
    if cmd == "fetch-insider-transactions":
        return fetch_insider_transactions(args.ticker, days=args.days, config=cfg)
    if cmd == "fetch-institutional-holdings":
        return fetch_institutional_holdings(
            args.ticker, top_n=args.top_n, num_quarters=args.num_quarters, config=cfg,
        )
    if cmd == "fetch-sec-filing":
        return fetch_sec_filing(
            args.ticker, args.filing_type,
            limit=args.limit,
            include_body=args.include_body,
            max_body_chars=args.max_body_chars,
            offset_chars=args.offset_chars,
            config=cfg,
        )
    if cmd == "detect-institutional-changes":
        return detect_institutional_changes(
            args.tickers, min_change_pct=args.min_change_pct, config=cfg,
        )
    if cmd == "detect-insider-activity":
        return detect_insider_activity(
            args.tickers,
            lookback_days=args.lookback_days,
            min_value_usd=args.min_value_usd,
            include_first_time_detection=args.first_time_detection,
            first_time_lookback_days=args.first_time_lookback_days,
            config=cfg,
        )
    # argparse's `required=True` on subparsers makes this unreachable, but keep
    # for defence in depth so an unknown command still produces an envelope.
    raise ValueError(f"unknown command: {cmd}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Minimal stderr logging before config loads. Real config replaces this.
    logging.basicConfig(
        level="WARNING",
        stream=sys.stderr,
        format="%(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("research_daemon.cli")

    source = _source_for_command(args.command)

    try:
        cfg = Config.from_env()
        configure_logging(cfg)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        envelope = build_error(
            status="error",
            source=source,
            detail=f"configuration error: {exc}",
        )
        _emit_envelope(envelope)
        return 1

    try:
        envelope = dispatch(args, cfg=cfg)
    except Exception as exc:  # noqa: BLE001 — CLI boundary, last-resort catch
        log.exception("unhandled error in %s", args.command)
        envelope = build_error(
            status="error",
            source=source,
            detail=f"unhandled exception: {exc}",
        )

    _emit_envelope(envelope)
    return 0 if envelope["status"] == "ok" else 1


def _source_for_command(cmd: str) -> str:
    # Every subcommand currently resolves to exactly one upstream. Update
    # this mapping if a subcommand gains a multi-source shape.
    return "edgar" if cmd == "fetch-sec-filing" else "finnhub"


def _emit_envelope(envelope: dict[str, Any]) -> None:
    json.dump(envelope, sys.stdout, indent=2, ensure_ascii=False, default=str)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    sys.exit(main())
