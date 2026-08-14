"""Capex Daemon CLI. Zero LLM calls anywhere in this daemon (E2).

Subcommands land as their build phases do. Present: `roster` (B1) and
`parse` (B2). `parse` is the operator-facing view of the filing-level leg and
is what CD-1-VERIFY.md drives for hand-verification against real filings.
"""
import argparse
import json
import sys

from . import ixbrl, storage, universe


def cmd_roster(args):
    roster = universe.load()
    print("{:<8}{:<12}{:<14}{}".format("TICKER", "CIK", "BUCKET", "NOTES"))
    for e in sorted(roster.values(), key=lambda e: (e.bucket, e.ticker_display)):
        print("{:<8}{:<12}{:<14}{}".format(
            e.ticker_display, e.cik, e.bucket, e.notes[:72]))
    print("\n{} entities. Tier is computed from measured coverage, never stored here."
          .format(len(roster)))
    return 0


def cmd_parse(args):
    parse = ixbrl.parse_ixbrl if args.inline else ixbrl.parse_instance
    facts = parse(args.path)
    if args.concept:
        facts = ixbrl.select(facts, concept=args.concept)
    if args.period_end:
        facts = ixbrl.select(facts, period_end=args.period_end)
    if args.dimensioned_only:
        facts = ixbrl.select(facts, dimensioned=True)

    if args.json:
        out = [{"taxonomy": f.taxonomy, "concept": f.concept, "value": f.value,
                "unit": f.unit, "scale": f.scale, "scale_basis": f.scale_basis,
                "period_start": f.period_start, "period_end": f.period_end,
                "dim_key": f.dim_key, "context_ref": f.context_ref,
                "collapsed_context_refs": f.collapsed_context_refs}
               for f in facts]
        json.dump(out, sys.stdout, indent=2)
        print()
        return 0

    for f in sorted(facts, key=lambda f: (f.concept, f.period_end or "", -abs(f.value))):
        collapsed = (" collapsed={}".format(len(f.collapsed_context_refs))
                     if f.collapsed_context_refs else "")
        print("{:<62} {}..{}  {:>20,.0f} {:<6} basis={}{} dim={}".format(
            f.concept[:62], f.period_start or "", f.period_end or "", f.value,
            f.unit, f.scale_basis, collapsed, f.dim_key or "-"))
    print("\n{} facts.".format(len(facts)))
    return 0


def cmd_initdb(args):
    con = storage.connect(args.db)
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    print("schema ready at {}".format(args.db or "default state home"))
    print("tables: {}".format(", ".join(tables)))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="capex-daemon", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("roster", help="list the universe roster")
    p.set_defaults(func=cmd_roster)

    p = sub.add_parser("parse", help="parse a filing instance or inline-XBRL document")
    p.add_argument("path")
    p.add_argument("--inline", action="store_true",
                   help="source is an inline-XBRL .htm rather than an extracted *_htm.xml")
    p.add_argument("--concept")
    p.add_argument("--period-end")
    p.add_argument("--dimensioned-only", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_parse)

    p = sub.add_parser("initdb", help="create the state schema")
    p.add_argument("--db", default=None)
    p.set_defaults(func=cmd_initdb)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
