"""Command line: `scout-daemon scan`.

Phase 1 verbs only -- `scan` (ingest + measure) and `health` (read state). No
verb here can classify, admit, submit, or contact anything, which is the CLI
surface of the containment boundary rather than an accident of scope.
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import config, state
from .orchestrator import run_scan


def _cmd_scan(args: argparse.Namespace) -> int:
    log = config.configure_logging(logging.DEBUG if args.verbose else logging.INFO)
    report = run_scan(
        only=args.only,
        logger=log,
        classify_items=args.classify or args.mechanical_only,
        use_llm=not args.mechanical_only,
    )

    if report.scan_id == "halted":
        print("HALTED -- kill switch engaged; no fetching performed.")
        return 0

    totals = report.totals
    print(f"\nscan {report.scan_id}  now_unix={report.now_unix}")
    print(
        f"sources ok={totals['ok']} empty={totals['empty']} "
        f"error={totals['error']}  items={totals['items']}\n"
    )
    header = f"{'source':<18}{'status':<8}{'items':>6}{'fit%':>8}{'recon%':>8}{'delta':>8}  detail"
    print(header)
    print("-" * len(header))
    for entry in sorted(report.sources, key=lambda s: -s.item_count):
        delta = f"{entry.divergence:+.1f}" if entry.item_count else "--"
        print(
            f"{entry.source:<18}{entry.status:<8}{entry.item_count:>6}"
            f"{entry.observed_field_fit:>8.1f}{entry.recon_field_fit:>8.1f}"
            f"{delta:>8}  {entry.detail[:60]}"
        )

    if report.title_collisions:
        print(
            f"\nduplicate-title collisions: "
            f"{len(report.title_collisions)} (recorded and linked, none dropped)"
        )

    if report.classes:
        total = sum(report.classes.values())
        print(f"\nlegitimacy distribution over {total} ledger rows:")
        # GREEN_PROMOTED is listed as its own line, never folded into GREEN.
        # Collapsing them here would undo in the report exactly what the
        # separate class exists to prevent in the schema.
        for name in ("GREEN", "GREEN_PROMOTED", "YELLOW", "RED"):
            count = report.classes.get(name, 0)
            share = (100.0 * count / total) if total else 0.0
            print(f"  {name:<15}{count:>6}  {share:5.1f}%")
        print(f"  ledger: {report.inserted} inserted, {report.updated} updated")

    if report.risk_bands:
        from .risk import DEAD_ZONE, PROMOTION_THRESHOLD

        print(f"\nrisk scores over the YELLOW set (promote below {PROMOTION_THRESHOLD}):")
        for band, count in report.risk_bands:
            if count:
                low = int(band.split("-")[0])
                marker = " <- promotion band" if low < PROMOTION_THRESHOLD else ""
                print(f"  {band:>7}  {count:>4}{marker}")
        print(f"  promotions this scan: {report.promotions}")

        # Dead-zone monitor. Warning, not failure: occupancy of 21-30 is
        # information for Mando, and the band being empty on one corpus is an
        # observation rather than a guarantee.
        if report.dead_zone:
            print(
                f"\n  !! WARNING: {len(report.dead_zone)} item(s) in the "
                f"{DEAD_ZONE[0]}-{DEAD_ZONE[1]} dead zone -- the band assumed "
                f"empty when the threshold was pinned:"
            )
            for row in report.dead_zone[:10]:
                gate = "ELIGIBLE" if row["eligible"] else "ineligible"
                print(
                    f"       score={row['risk_score']:<4}{gate:<11}"
                    f"{row['source']:<16}{row['title'][:40]}"
                )

    if report.cost:
        cost = report.cost
        print(
            f"\ncost: {cost.llm_calls} call(s)  in={cost.input_tokens}  "
            f"out={cost.output_tokens}  cache_read={cost.cache_read_tokens}  "
            f"${cost.cost_usd:.4f}  ({cost.items_classified} items to LLM)"
        )

    if report.veto_rates:
        v = report.veto_rates
        # BOTH rates, always. A correction nobody can inspect is just a
        # smaller number -- reporting raw beside corrected is what makes the
        # duplicate-collapsing auditable rather than something to take on faith.
        print("\nLLM veto rate over mechanical-GREEN (MONITOR, not a gate):")
        print(
            f"  RAW        {v['raw_vetoed']:>4}/{v['raw_green']:<5}"
            f"{v['raw_rate']:6.1f}%   row-level; duplicates counted N times"
        )
        print(
            f"  CORRECTED  {v['unit_vetoed']:>4}/{v['unit_green']:<5}"
            f"{v['unit_rate']:6.1f}%   judgment units; "
            f"{v['duplicates_collapsed']} duplicate rows collapsed"
        )
        print(
            f"  NON-PERSONA{v['non_persona_vetoed']:>4}/{v['unit_green']:<5}"
            f"{v['non_persona_rate']:6.1f}%   {v['persona_units']} persona vetoes "
            f"excluded as correct catches"
        )
        # RETIRED AS A HALT CONDITION -- Mando 2026-08-13, doctrine E19.
        #
        # This rate halted four times running (45.8 -> 39.8 -> 26.2 -> 23.1) and
        # every halt traced to corpus thinness, not rubric miscalibration. The
        # mechanical rubric passes on absent triggers; the LLM suspects on thin
        # data. Where listings are thin they disagree BECAUSE each is working,
        # so the aggregate measures the corpus and not the rubric. No verdict is
        # printed here on purpose: there is no threshold to pass.
        #
        # What remains diagnosable is the per-source cut, and what to watch
        # there is MOVEMENT, not level -- a source that jumps has degraded or a
        # prompt has drifted.
        per_source = v.get("per_source") or []
        if per_source:
            print("\n  per source (watch for MOVEMENT between scans, not level):")
            print(f"    {'source':<18}{'green':>6}{'vetoed':>8}{'rate':>8}")
            for entry in per_source:
                print(
                    f"    {entry['source']:<18}{entry['green']:>6}"
                    f"{entry['vetoed']:>8}{entry['rate']:>7.1f}%"
                )

    if report.residual_vetoes:
        print(
            f"\nresidual disagreements "
            f"({len(report.residual_vetoes)} judgment units) -- rubric material:"
        )
        for row in report.residual_vetoes[:25]:
            print(f"  [{row['source']}] {(row['title'] or '')[:46]}")

    if report.disagreements:
        print(f"\nmechanical/LLM disagreements: {len(report.disagreements)}")
        for row in report.disagreements[:15]:
            print(
                f"  [{row['mechanical']}->{row['llm']} = {row['resolved']}] "
                f"{row['source']}: {row['title'][:52]}"
            )
    return 0


def _cmd_ledger(args: argparse.Namespace) -> int:
    """Read the ledger, including the RED set. Seeing the excluded space is an
    invariant, so the default view does NOT filter it out."""
    from . import ledger as ledger_mod

    conn = state.connect()
    ledger_mod.apply_schema(conn)
    where, params = "", []
    if args.klass:
        where, params = "WHERE legitimacy_class = ?", [args.klass.upper()]
    rows = conn.execute(
        f"SELECT legitimacy_class, status, source, title, payout_raw, "
        f"payout_confidence, class_reason FROM opportunities {where} "
        f"ORDER BY legitimacy_class, source, title LIMIT ?",
        (*params, args.limit),
    ).fetchall()
    if not rows:
        print("ledger empty -- run `scout-daemon scan --classify` first")
        return 0
    for row in rows:
        print(
            f"[{row['legitimacy_class']:<6}] {row['status']:<11}"
            f"{row['source']:<17}{(row['title'] or '')[:44]:46}"
            f"{(row['payout_raw'] or '-')[:22]:24}{row['payout_confidence']}"
        )
        print(f"           reason: {(row['class_reason'] or '')[:150]}")
    conn.close()
    return 0


def _cmd_rank(args: argparse.Namespace) -> int:
    """Order the queue. Reads stored columns; makes no LLM call and no fetch."""
    import time

    from . import ledger as ledger_mod
    from . import rank as rank_mod

    conn = state.connect()
    ledger_mod.apply_schema(conn)
    result = rank_mod.rank_ledger(conn, now_unix=int(time.time()))

    print(f"\nranking  algorithm={rank_mod.RANK_ALGORITHM_VERSION}")
    print("key: payout_usd_low DESC (conservative bound of the parsed range)")
    # Invariant 3: the output states the staleness of its own inputs rather
    # than presenting an order as though it were computed just now.
    if result.newest_input_unix:
        age_h = (int(time.time()) - result.oldest_input_unix) / 3600.0
        print(f"contention/award inputs observed {age_h:.1f}h ago at the oldest")
    else:
        print("no award-rate inputs present -- expected_usd uncomputable on every row")

    for segment in (rank_mod.SEGMENT_GREEN, rank_mod.SEGMENT_GREEN_PROMOTED):
        rows = result.ranked.get(segment, [])
        # Printed as separate blocks, never one list. GREEN_PROMOTED reached
        # GREEN through the risk gate, not the rubric; interleaving would erase
        # that in the one place a reader would act on it.
        print(f"\n=== {segment} -- {len(rows)} ranked ===")
        if not rows:
            print("  (none)")
        for row in rows[: args.limit]:
            exp = f"{row.expected_usd:>10,.0f}" if row.expected_usd is not None else "         -"
            print(
                f"  {row.position:>3}. ${row.payout_usd_low:>11,.0f}{exp}  "
                f"{row.source:<16}{row.title[:40]:<42}"
                f"seen={row.verdicts_seen} flips={row.flip_count}"
            )
        if len(rows) > args.limit:
            print(f"  ... {len(rows) - args.limit} more")

    print(f"\n=== UNRANKED -- {len(result.unranked)} rows, each with a reason ===")
    by_reason: dict[str, int] = {}
    for row in result.unranked:
        by_reason[row.unranked_reason or "?"] = by_reason.get(row.unranked_reason or "?", 0) + 1
    for reason, n in sorted(by_reason.items(), key=lambda kv: -kv[1]):
        print(f"  {n:>4}  {reason}")
    print("\n  (unranked rows are REPORTED, never dropped and never sorted to the")
    print("   bottom as if their value had been measured and found lowest)")
    conn.close()
    return 0


def _cmd_health(args: argparse.Namespace) -> int:
    conn = state.connect()
    rows = conn.execute(
        "SELECT * FROM source_health ORDER BY consecutive_failures DESC, source"
    ).fetchall()
    if not rows:
        print("no source health recorded yet -- run `scout-daemon scan` first")
        return 0
    print(f"{'source':<18}{'status':<8}{'items':>6}{'fails':>7}  watermark")
    for row in rows:
        print(
            f"{row['source']:<18}{str(row['last_status']):<8}"
            f"{row['last_item_count']:>6}{row['consecutive_failures']:>7}  "
            f"{row['last_successful_fetch_unix']}"
        )
    conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="scout-daemon",
        description="Income-discovery sensor. Read-only; proposes, never executes.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    scan = sub.add_parser("scan", help="fetch and measure the roster")
    scan.add_argument("--only", nargs="*", help="restrict to these source names")
    scan.add_argument("--classify", action="store_true",
                      help="classify and write the ledger (makes one LLM call per batch)")
    scan.add_argument("--mechanical-only", action="store_true",
                      help="classify with the mechanical filter only; no LLM, no cost")
    scan.add_argument("-v", "--verbose", action="store_true")
    scan.set_defaults(func=_cmd_scan)

    health = sub.add_parser("health", help="show per-source health and watermarks")
    health.set_defaults(func=_cmd_health)

    rank = sub.add_parser("rank", help="order the queue within segments (no fetch, no LLM)")
    rank.add_argument("--limit", type=int, default=20)
    rank.set_defaults(func=_cmd_rank)

    view = sub.add_parser("ledger", help="read the ledger (RED set included)")
    view.add_argument("--class", dest="klass", help="GREEN | YELLOW | RED")
    view.add_argument("--limit", type=int, default=40)
    view.set_defaults(func=_cmd_ledger)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
