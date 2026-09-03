"""Command line.

THE VERBS THIS DAEMON HAS: `queue`, `rehearse`, `doctor`.

THE VERB IT DOES NOT HAVE: anything that executes work, and anything that
submits. Phase 1 ships no execution path -- not a disabled one, not a
flag-guarded one. `rehearse` runs the gates and emits a packet; that is the
whole of what this daemon can currently do, and it is deliberate.
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import config, fetch, intake, runner
from .errors import BuilderError
from .outcomes import DECLINED, ESCALATED
from .packet import emit


def _logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return logging.getLogger("builder_daemon")


def cmd_queue(args) -> int:
    """Show the Builder's entire input: admitted, code-PR-shaped rows."""
    conn = intake.connect_scout(args.scout_db)
    items = intake.select_work(conn)
    if not items:
        print("no admitted code-PR work items")
        print("  (this is the expected state until Mando admits a code row)")
        return 0
    for it in items:
        payout = f"${it.payout_usd_low:,.0f}" if it.payout_usd_low else "  -"
        print(f"  {it.short_id}  {payout:>10}  {it.repo_slug}#{it.issue_number}  {it.title[:44]}")
    return 0


def cmd_rehearse(args) -> int:
    """Run both gates against one named row and emit a packet. No patch, ever.

    Accepts a non-admitted row on purpose: rehearsal exists to exercise the
    plumbing against real repositories before any admitted work exists. It
    cannot search for a row -- the id is required, which means a human chose it.
    """
    log = _logger()
    if config.halted():
        log.error("halt file or %s set; refusing to run", config.HALT_ENV_VAR)
        return 2

    conn = intake.connect_scout(args.scout_db)
    item = intake.load_one(conn, args.id)

    if not item.is_admitted:
        log.warning(
            "%s is status=%s, not admitted -- REHEARSAL ONLY, no patch will be produced",
            item.short_id, item.status,
        )

    client = fetch.build_client(logger=log)
    packet = runner.run_gates(client, item, rehearsal=True)

    out_dir = args.out or config.PACKET_DIR
    written = emit(packet, out_dir)

    print()
    print(f"  item     {item.short_id}  {item.repo_slug}#{item.issue_number}")
    print(f"  status   {item.status}")
    print(f"  outcome  {packet.outcome.value}")
    for name, verdict in (("policy  ", packet.policy), ("liveness", packet.liveness)):
        if verdict is None:
            print(f"  {name} not run")
            continue
        print(f"  {name} {verdict.result.value}"
              + (f" -- {verdict.reason[:110]}" if verdict.reason else ""))
    if packet.obligations:
        print("  obligations:")
        for o in packet.obligations:
            print(f"    - {o[:100]}")
    print(f"  sources  {len(packet.sources_read)} read")
    for kind, path in written.items():
        print(f"  {kind:<12} {path}")
    print()

    if packet.outcome in DECLINED:
        return 0      # a decline is a completed run, not a failure
    if packet.outcome in ESCALATED:
        return 0      # so is an escalation; both are first-class outcomes
    return 0


def cmd_doctor(args) -> int:
    """Report what the daemon can see. Reads nothing it does not own."""
    print(f"  state home     {config.STATE_HOME}")
    print(f"  packets        {config.PACKET_DIR}")
    print(f"  scout ledger   {config.SCOUT_DB_PATH}"
          f"  ({'present' if config.SCOUT_DB_PATH.exists() else 'MISSING'})")
    print(f"  halted         {config.halted()}")
    try:
        conn = intake.connect_scout(args.scout_db)
        total = conn.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        admitted = len(intake.select_work(conn))
        print(f"  ledger rows    {total}")
        print(f"  admitted code  {admitted}")
    except BuilderError as exc:
        print(f"  ledger         UNREADABLE: {exc}")
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="builder-daemon",
        description="Code-PR drafter. Drafts against admitted work; never submits.",
    )
    p.add_argument("--scout-db", dest="scout_db", default=None,
                   help="path to scout's ledger (read-only)")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("queue", help="show admitted code-PR work items").set_defaults(
        func=cmd_queue)

    r = sub.add_parser("rehearse", help="run both gates against one row; emit a packet")
    r.add_argument("--id", required=True, help="opportunity_id or 12-char short id")
    r.add_argument("--out", default=None, help="packet output directory")
    r.set_defaults(func=cmd_rehearse)

    sub.add_parser("doctor", help="report visible state").set_defaults(func=cmd_doctor)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except BuilderError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
