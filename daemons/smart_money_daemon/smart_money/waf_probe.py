"""ORDER SM-C3 Phase W — Senate eFD availability probe log and window map.

The Senate legs have been degraded for a long time on the belief that eFD blocks scripted
access. That belief is currently ASSERTION, not data. This logs every probe (timestamp,
result, latency) so that after ~2 weeks we can answer the question with a table instead of
a shrug: is there an hour-of-day window where the Senate endpoints answer reliably?

Two outcomes, both legitimate and both reportable:
  * a window exists  -> move the Senate legs into it
  * no window exists -> "no window", with the data behind it

DELIBERATELY CHEAP: one probe per invocation, paced, read-only, no ingest. It must be safe
to ride along on the morning brief without competing for the request budget that the real
Senate ingest needs.
"""
import argparse
import collections
import datetime as dt
import random
import sys
import time

from . import db as dbmod

# What "reachable" means, concretely: the search endpoint returns a well-formed DataTables
# payload. That is the exact call the Senate delta leg depends on, so a green probe is
# evidence about the thing we actually care about, not a proxy for it.
PROBE_KIND_SEARCH = "search"


# A probe that cannot tell "eFD refused us" from "this host had no internet" produces a
# FRAUDULENT availability map. On 2026-08-04 Basilic lost outbound DNS/HTTPS entirely
# (every domain resolving to one IP pair, all connects failing) and ten consecutive probes
# recorded as eFD failures — which read as the weekday throttle we were trying to measure.
# Failures are therefore classified against a CONTROL host before being counted.
CONTROL_URL = "https://www.google.com/generate_204"


def _have_network(timeout=6):
    """True if this host can reach the open internet at all. Used only to CLASSIFY a
    probe failure, never to decide the probe's own result."""
    import requests
    try:
        requests.get(CONTROL_URL, timeout=timeout)
        return True
    except Exception:  # noqa: BLE001 - any failure means treat the host as offline
        return False


def probe_once(kind=PROBE_KIND_SEARCH, contact="smartmoney@example.com"):
    """One probe. Returns (ok, status, latency_ms, detail). Never raises — a probe that
    blows up is itself a datapoint and must be recorded, not lost."""
    from .efd_session import bootstrap, post_data
    t0 = time.monotonic()
    try:
        sess = bootstrap("AbelardSmartMoney/0.1 (+{})".format(contact), probe=False)
        since = (dt.date.today() - dt.timedelta(days=30)).strftime("%m/%d/%Y")
        body = post_data(sess, {
            "draw": "1", "start": "0", "length": "1",
            "report_types": "[11]", "filer_types": "[]",
            "first_name": "", "last_name": "",
            "submitted_start_date": "{} 00:00:00".format(since),
            "submitted_end_date": "", "candidate_state": "", "senator_state": "",
            "office_id": "", "order[0][column]": "4", "order[0][dir]": "desc"})
        ms = int((time.monotonic() - t0) * 1000)
        if not isinstance(body, dict) or "recordsTotal" not in body:
            return False, "malformed", ms, "200 but no recordsTotal"
        return True, "ok", ms, "recordsTotal={}".format(body.get("recordsTotal"))
    except Exception as exc:  # noqa: BLE001 - a failed probe is data, never an abort
        ms = int((time.monotonic() - t0) * 1000)
        # Distinguish "eFD blocked us" from "we had no network". Only the former is
        # evidence about eFD availability; the latter says nothing and must not be
        # counted as a refusal.
        if not _have_network():
            return False, "no_network", ms, "host offline: " + str(exc)[:120]
        return False, "error", ms, str(exc)[:160]


def record(con, kind, ok, status, latency_ms, detail):
    now = dt.datetime.now()
    con.execute(
        "INSERT INTO efd_probe_log(probed_at_unix, probed_at_iso, hour_local, kind, "
        "ok, status, latency_ms, detail) VALUES(?,?,?,?,?,?,?,?)",
        (int(time.time()), now.isoformat(timespec="seconds"), now.hour, kind,
         1 if ok else 0, status, latency_ms, detail))
    con.commit()


def _is_weekend(iso):
    try:
        return dt.datetime.fromisoformat(iso).weekday() >= 5
    except (TypeError, ValueError):
        return None


def window_map(con, days=14, weekday_only=False):
    """hour-of-day -> (attempts, successes, rate, median latency).

    DAY OF WEEK IS A CONFOUND, not a detail. If eFD's throttling is load-shaped, weekend
    traffic is a fraction of weekday traffic and probes come back green at EVERY hour —
    which would make an hours-only map read "no window" for the wrong reason. The verdict
    is therefore computed from WEEKDAY samples (when the daemon actually has to run) and
    weekend samples are reported separately as context."""
    since = int(time.time()) - days * 86400
    buckets = collections.defaultdict(lambda: {"n": 0, "ok": 0, "lat": []})
    for hour, ok, ms, iso in con.execute(
            "SELECT hour_local, ok, latency_ms, probed_at_iso FROM efd_probe_log "
            "WHERE probed_at_unix >= ? AND status != 'no_network'", (since,)):
        if weekday_only and _is_weekend(iso) is not False:
            continue
        b = buckets[hour]
        b["n"] += 1
        b["ok"] += ok or 0
        if ms is not None:
            b["lat"].append(ms)
    out = []
    for hour in sorted(buckets):
        b = buckets[hour]
        lat = sorted(b["lat"])
        out.append({"hour": hour, "attempts": b["n"], "successes": b["ok"],
                    "rate": round(100.0 * b["ok"] / b["n"], 1) if b["n"] else 0.0,
                    "median_ms": lat[len(lat) // 2] if lat else None})
    return out


def dow_split(con, days=14):
    """(weekday, weekend) attempt/success counts — the confound, made visible."""
    since = int(time.time()) - days * 86400
    wd = {"n": 0, "ok": 0}
    we = {"n": 0, "ok": 0}
    for ok, iso in con.execute(
            "SELECT ok, probed_at_iso FROM efd_probe_log WHERE probed_at_unix >= ? "
            "AND status != 'no_network'", (since,)):
        wknd = _is_weekend(iso)
        if wknd is None:
            continue
        b = we if wknd else wd
        b["n"] += 1
        b["ok"] += ok or 0
    return wd, we


def _render_map(rows, days, wd=None, we=None, weekday_rows=None, excluded=0):
    total = sum(r["attempts"] for r in rows)
    lines = ["eFD SENATE AVAILABILITY WINDOW MAP  (trailing {}d, n={})".format(days, total),
             "=" * 62]
    if not total:
        lines.append("NO PROBES LOGGED YET - the map is empty, which is not the same as")
        lines.append("'no window'. Let the rider run before reading anything into this.")
        return "\n".join(lines)
    lines.append("%-6s %9s %10s %8s %s" % ("hour", "attempts", "successes", "rate", "med ms"))
    for r in rows:
        lines.append("%02d:00  %9d %10d %7.1f%% %s" % (
            r["hour"], r["attempts"], r["successes"], r["rate"],
            r["median_ms"] if r["median_ms"] is not None else "-"))
    lines.append("-" * 62)
    # DAY-OF-WEEK CONFOUND. Weekend load on eFD is a fraction of weekday load, so if the
    # throttle is load-shaped, weekend probes go green at EVERY hour and an hours-only map
    # reads "no window" for the wrong reason. The verdict is therefore computed from
    # WEEKDAY samples only — the days the daemon actually has to run.
    if excluded:
        lines.append("EXCLUDED as host-offline (no_network): {} probe(s) - these say "
                     "NOTHING about eFD and are kept out of every rate above.".format(
                         excluded))
        lines.append("-" * 62)
    if wd is not None and we is not None:
        lines.append("DAY-OF-WEEK SPLIT (the confound):")
        for lbl, b in (("weekday", wd), ("weekend", we)):
            lines.append("  %-8s attempts %-4d successes %-4d %s" % (
                lbl, b["n"], b["ok"],
                ("%.1f%%" % (100.0 * b["ok"] / b["n"])) if b["n"] else "-"))
        lines.append("-" * 62)
    FLOOR = 20
    wd_n = wd["n"] if wd is not None else total
    if wd_n < FLOOR:
        lines.append("INSUFFICIENT WEEKDAY DATA (weekday n={} < {}). No window verdict "
                     "offered.".format(wd_n, FLOOR))
        lines.append("Weekend probes are NOT a substitute: if the throttle is load-shaped "
                     "they succeed at every hour regardless, which would read as "
                     "'no window' for entirely the wrong reason.")
        return "\n".join(lines)
    basis = weekday_rows or rows
    if not basis:
        lines.append("INSUFFICIENT WEEKDAY DATA. No window verdict offered.")
        return "\n".join(lines)
    best = max(basis, key=lambda r: (r["rate"], r["attempts"]))
    worst = min(basis, key=lambda r: (r["rate"], -r["attempts"]))
    if best["rate"] >= 60 and best["rate"] - worst["rate"] >= 30:
        lines.append("WINDOW CANDIDATE (weekday only): {:02d}:00 at {}% ({}/{}) vs worst "
                     "{:02d}:00 at {}%.".format(
                         best["hour"], best["rate"], best["successes"],
                         best["attempts"], worst["hour"], worst["rate"]))
    else:
        lines.append("NO WINDOW (weekday only): success rate does not vary materially by "
                     "hour (best {}% vs worst {}%).".format(best["rate"], worst["rate"]))
    return "\n".join(lines)


def main(argv=None):
    from .efd_ingest import load_env
    ap = argparse.ArgumentParser(description="SM-C3 Phase W eFD probe / window map")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--map", action="store_true", help="emit the window map and exit")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--rider", action="store_true",
                    help="randomized delay before probing, for the morning-brief rider "
                         "so samples do not all land on the same minute")
    ap.add_argument("--max-jitter", type=int, default=1800)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    try:
        if args.map:
            wd, we = dow_split(con, args.days)
            since = int(time.time()) - args.days * 86400
            excl = con.execute(
                "SELECT count(*) FROM efd_probe_log WHERE probed_at_unix >= ? "
                "AND status = 'no_network'", (since,)).fetchone()[0]
            print(_render_map(window_map(con, args.days), args.days, wd, we,
                              window_map(con, args.days, weekday_only=True), excl))
            return 0
        if args.rider:
            # Randomized so repeated riders sample different minutes of the hour; the
            # HOUR is what the map buckets on, so jitter must stay well under an hour.
            time.sleep(random.randint(0, max(0, min(args.max_jitter, 3000))))
        contact = load_env().get("EDGAR_CONTACT") or "smartmoney@example.com"
        ok, status, ms, detail = probe_once(contact=contact)
        record(con, PROBE_KIND_SEARCH, ok, status, ms, detail)
        print("[waf_probe] {} status={} {}ms {}".format(
            "OK" if ok else "FAIL", status, ms, detail))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
