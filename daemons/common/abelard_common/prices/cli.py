"""PS-1 Phase 2 — the ``abelard-prices`` CLI.

    abelard-prices universe-sync   pull the constituent sources, write as-of rows
    abelard-prices backfill        full history, one time
    abelard-prices nightly         append what each name is missing
    abelard-prices refetch         the rotation verification sweep
    abelard-prices status          freshness ledger; non-zero exit if anything lags

**This is where the environment is read, and the only place.** The library takes
explicit paths (``alert_queue``'s discipline, Mando's ruling 3); the CLI resolves
``ABELARD_PRICES_DB_PATH`` and hands a path down. Set it ABSOLUTE in the launchd
job — a ``~`` will not expand there.

**Exit codes are the alerting contract**, so the job needs no log scraping:

    0  clean
    1  something lagged, or a fact changed, or the vendor was degraded
    2  the run could not start (bad config, unreadable store)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from ..http_client import HttpClient
from . import corrections, reconcile, reference, schema, universe, writer
from .schema import PriceStoreError
from .vendor import USER_AGENT, YahooVendor

ENV_DB_PATH = "ABELARD_PRICES_DB_PATH"
DEFAULT_DB_PATH = "~/.openclaw/prices/prices.db"


def resolve_db_path(explicit: str | None = None) -> Path:
    """CLI-layer env resolution. Deliberately not in the library."""
    raw = explicit or os.environ.get(ENV_DB_PATH) or DEFAULT_DB_PATH
    return Path(os.path.expanduser(raw))


def _client(contact: str | None) -> HttpClient:
    ua = USER_AGENT + (" ({})".format(contact) if contact else "")
    return HttpClient(user_agent=ua)


def _echo(line: str) -> None:
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="abelard-prices", description=__doc__)
    ap.add_argument("--db", help="store path (else ${})".format(ENV_DB_PATH))
    ap.add_argument("--contact", default=os.environ.get("EDGAR_CONTACT"),
                    help="contact string appended to the User-Agent")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_u = sub.add_parser("universe-sync", help="constituents, classification, identity")
    p_u.add_argument("--as-of", default=None, help="ISO date (default: today)")
    p_u.add_argument("--funds", default=",".join(universe.DEFAULT_ISHARES),
                     help="iShares funds, comma separated (IWM enables the Russell 2000)")
    p_u.add_argument("--no-ndx", action="store_true")
    p_u.add_argument("--history-since", default=None,
                     help="also backfill as-of membership for names that have LEFT "
                          "the index since this date (survivorship fix)")

    p_b = sub.add_parser("backfill", help="full history, one time")
    p_b.add_argument("--since", default=writer.DEFAULT_BACKFILL_START)
    p_b.add_argument("--limit", type=int, help="cap the name count for a chunked run")

    sub.add_parser("nightly", help="append missing sessions")

    p_r = sub.add_parser("refetch", help="rotation verification sweep")
    p_r.add_argument("-n", type=int, default=18,
                     help="names per night (516/30 = 18 at v1 scope)")
    p_r.add_argument("--since", default=writer.DEFAULT_BACKFILL_START)

    p_ref = sub.add_parser("reference", help="VIX / WTI / benchmark series")
    p_ref.add_argument("--since", default=writer.DEFAULT_BACKFILL_START)
    p_ref.add_argument("--no-fred", action="store_true")

    p_rc = sub.add_parser("reconcile", help="index-level systemic-failure check")
    p_rc.add_argument("--date", default=None, help="session to check (default: latest held)")
    p_rc.add_argument("--tolerance-bp", type=float, default=reconcile.DEFAULT_TOLERANCE_BP)

    p_c = sub.add_parser("correct", help="apply a staged correction file (human-authored)")
    p_c.add_argument("file", help="staging JSON")
    p_c.add_argument("--apply", action="store_true",
                     help="write it; without this the plan is printed and nothing changes")

    sub.add_parser("status", help="freshness ledger")

    args = ap.parse_args(argv)
    db_path = resolve_db_path(args.db)

    try:
        con = schema.connect(db_path)
    except (PriceStoreError, OSError) as exc:
        _echo("[prices] cannot open store at {}: {}".format(db_path, exc))
        return 2

    started = time.time()
    try:
        if args.cmd == "universe-sync" and not (args.contact or "").strip():
            _echo("[prices] universe-sync needs a contact for SEC: set "
                  "EDGAR_CONTACT or pass --contact <email>")
            return 2
        if args.cmd == "universe-sync":
            as_of = args.as_of or time.strftime("%Y-%m-%d")
            funds = tuple(f.strip().upper() for f in args.funds.split(",") if f.strip())
            rep = universe.sync(con, _client(args.contact), as_of=as_of,
                                contact=args.contact or "", funds=funds,
                                include_ndx=not args.no_ndx,
                                history_since=args.history_since)
            _echo(rep.render())
            _echo("[prices] universe-sync done in {}s".format(int(time.time() - started)))
            return 0

        if args.cmd == "reference":
            vendor = YahooVendor(client=_client(args.contact))
            end = time.strftime("%Y-%m-%d")
            rep = reference.sync_yahoo(con, vendor, args.since, end)
            _echo(rep.render())
            if not args.no_fred:
                frep = reference.sync_fred(con, _client(args.contact))
                _echo(frep.render())
            vrep = reference.reconcile_validators(con)
            _echo(vrep.render())
            return 1 if (vrep.divergences or rep.errors) else 0

        if args.cmd == "reconcile":
            from .calendar import previous_session
            date = args.date or (con.execute(
                "SELECT MAX(last_date_held) FROM freshness").fetchone() or [None])[0]
            if not date:
                _echo("[prices] nothing held yet; nothing to reconcile")
                return 2
            recs = reconcile.run(con, date, previous_session(date),
                                 tolerance_bp=args.tolerance_bp)
            for r in recs:
                _echo(r.render())
            return 0 if all(r.passed for r in recs) else 1

        if args.cmd == "correct":
            payload = corrections.load(args.file)
            plan = corrections.plan(con, payload)
            _echo(plan.render())
            if not plan.ok:
                return 1
            if not args.apply:
                _echo("[prices] DRY RUN — nothing written. Re-run with --apply.")
                return 0
            n = corrections.apply(con, plan)
            _echo("[prices] wrote {} correction rows".format(n))
            # The view honours corrections, so it has to be rebuilt for every
            # name touched. prices_raw is not modified.
            for iid in corrections.affected_instruments(plan):
                splits, divs = writer.declared_actions(con, iid)
                writer._rebuild_view(con, iid, splits, divs,
                                     writer.current_factor_version(con, iid) or 1,
                                     int(time.time()))
            con.commit()
            _echo("[prices] rebuilt adjusted_view for {} instruments".format(
                len(corrections.affected_instruments(plan))))
            return 0

        if args.cmd == "status":
            rep = writer.status(con)
            _echo(rep.render())
            return 0 if rep.ok else 1

        vendor = YahooVendor(client=_client(args.contact))
        run_asof = int(time.time())
        if args.cmd == "backfill":
            run = writer.backfill(con, vendor, since=args.since, limit=args.limit,
                                  run_asof=run_asof, progress=_echo)
        elif args.cmd == "nightly":
            run = writer.nightly(con, vendor, run_asof=run_asof, progress=_echo)
        else:
            run = writer.refetch(con, vendor, n=args.n, since=args.since,
                                 run_asof=run_asof, progress=_echo)

        counts = run.counts()
        _echo("[prices] {} run_asof={} names={} requests={} {} in {}s".format(
            args.cmd, run_asof, len(run.names), run.requests_made,
            " ".join("{}={}".format(k, v) for k, v in sorted(counts.items())),
            int(time.time() - started)))

        # Fail loud, in the exit code, so launchd surfaces it without log parsing.
        for nm in run.fact_changes:
            _echo("[prices] FACT CHANGE {} {}: {}".format(
                nm.instrument_id, nm.symbol, nm.detail))
        rep = writer.status(con)
        if rep.lagging:
            _echo("[prices] {} names lag the latest held session".format(len(rep.lagging)))
        bad = bool(run.fact_changes) or bool(rep.lagging) or counts.get("vendor_error")
        return 1 if bad else 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
