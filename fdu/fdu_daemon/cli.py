"""``fdu-daemon`` command line. The CLI is the contract.

Read commands only. There is no ``contact``, no ``export-contacts``, no verb
that reaches a counterparty -- not by policy but because no such code exists.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import config, ledger, leads as leads_mod, orchestrator
from .errors import FduError, HaltRequested
from .fetch import Fetcher


def _fetcher() -> Fetcher:
    return Fetcher()


def cmd_scan(args) -> int:
    conn = ledger.connect()
    kinds = tuple(args.feeds.split(",")) if args.feeds else ("FIRM_SEC",)
    result = orchestrator.scan(conn, _fetcher(), kinds=kinds, keep_feed=args.keep_feed)
    print(result.render())
    return 0


def cmd_enrich(args) -> int:
    conn = ledger.connect()
    out = orchestrator.enrich(conn, _fetcher(), limit=args.limit,
                              backfill=args.backfill, delay=args.delay)
    print(f"enrich {out['run_id']}")
    print(f"  targets    {out['targets']:,}")
    print(f"  extracted  {out['extracted']:,}")
    print(f"  failed     {out['failed']:,}")
    print(f"  fetched    {out['fetch_calls']} calls, {out['fetch_bytes']:,} bytes")
    print("  llm calls        0   cost $0.00")
    for err in out["errors"][:10]:
        print(f"  ! {err}")
    return 0 if not out["failed"] else 1


def cmd_leads(args) -> int:
    conn = ledger.connect()
    print(leads_mod.render(leads_mod.collect(conn, limit=args.limit)))
    return 0


def cmd_status(args) -> int:
    conn = ledger.connect()
    row = conn.execute(
        "SELECT COUNT(*) n, SUM(aum_total) aum, MAX(snapshot_date) snap FROM firm"
    ).fetchone()
    changes = conn.execute("SELECT COUNT(*) n FROM firm_change").fetchone()["n"]
    details = conn.execute("SELECT COUNT(*) n FROM adv_detail").fetchone()["n"]
    succ = conn.execute("SELECT COUNT(*) n FROM adv_detail WHERE section4_filed=1").fetchone()["n"]
    print(f"db            {config.db_path()}")
    print(f"snapshot      {row['snap']}")
    print(f"firms         {row['n']:,}")
    print(f"change rows   {changes:,}   (append-only)")
    print(f"adv extracts  {details:,}   of which successions filed: {succ:,}")
    if row["n"]:
        print(f"coverage      {100.0 * details / row['n']:.1f}% enriched")
    print(f"halt          {'ENGAGED' if config.halt_requested() else 'clear'}")
    return 0


def cmd_runs(args) -> int:
    conn = ledger.connect()
    rows = conn.execute(
        "SELECT run_id, kind, started_unix, firms_seen, firms_changed, docs_fetched, "
        "fetch_calls, fetch_bytes, llm_calls, llm_cost_usd, status FROM run "
        "ORDER BY started_unix DESC LIMIT ?", (args.limit,)
    ).fetchall()
    if not rows:
        print("no runs recorded")
        return 0
    print(f"{'run':<13}{'kind':<10}{'seen':>8}{'changed':>9}{'docs':>7}{'calls':>7}{'bytes':>13}{'llm$':>7}  status")
    for r in rows:
        print(f"{r['run_id']:<13}{r['kind']:<10}{(r['firms_seen'] or 0):>8,}{(r['firms_changed'] or 0):>9,}"
              f"{(r['docs_fetched'] or 0):>7,}{(r['fetch_calls'] or 0):>7,}{(r['fetch_bytes'] or 0):>13,}"
              f"{(r['llm_cost_usd'] or 0):>7.2f}  {r['status']}")
    return 0


def cmd_show(args) -> int:
    conn = ledger.connect()
    firm = conn.execute("SELECT * FROM firm WHERE crd = ?", (args.crd,)).fetchone()
    if firm is None:
        print(f"no firm with CRD {args.crd} in the ledger", file=sys.stderr)
        return 1
    print(json.dumps({k: firm[k] for k in firm.keys()}, indent=2, default=str))
    detail = conn.execute("SELECT * FROM adv_detail WHERE crd = ?", (args.crd,)).fetchone()
    print("\nadv_detail:")
    print(json.dumps({k: detail[k] for k in detail.keys()}, indent=2, default=str)
          if detail else "  (not enriched)")
    hist = conn.execute(
        "SELECT observed_unix, field, old_value, new_value FROM firm_change "
        "WHERE crd = ? ORDER BY observed_unix DESC LIMIT 25", (args.crd,)
    ).fetchall()
    print(f"\nchange history ({len(hist)} most recent):")
    for h in hist:
        print(f"  {h['field']:<22} {h['old_value']} -> {h['new_value']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fdu-daemon", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", help="pull the bulk snapshot and record what moved")
    s.add_argument("--feeds", default="FIRM_SEC", help="comma list: FIRM_SEC,FIRM_STATE")
    s.add_argument("--keep-feed", action="store_true", help="retain the downloaded feed file")
    s.set_defaults(func=cmd_scan)

    s = sub.add_parser("enrich", help="pull ADV documents for firms that moved")
    s.add_argument("--limit", type=int, default=None)
    s.add_argument("--backfill", action="store_true", help="enrich firms never enriched, largest first")
    s.add_argument("--delay", type=float, default=None, help="seconds between fetches")
    s.set_defaults(func=cmd_enrich)

    s = sub.add_parser("leads", help="firms with succession-shaped movement (unranked)")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(func=cmd_leads)

    s = sub.add_parser("status", help="ledger summary")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("runs", help="run history and cost telemetry")
    s.add_argument("--limit", type=int, default=15)
    s.set_defaults(func=cmd_runs)

    s = sub.add_parser("show", help="everything held on one firm")
    s.add_argument("crd")
    s.set_defaults(func=cmd_show)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except HaltRequested as exc:
        print(f"HALT: {exc}", file=sys.stderr)
        return 2
    except FduError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
