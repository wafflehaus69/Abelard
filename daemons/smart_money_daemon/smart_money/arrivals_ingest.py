"""SM-H1 arrival ledger, write path: the unfiltered EDGAR read.

smart_money.arrivals holds everything that judges and reads the ledger and carries no
write path, so the read-only query layer can import it. This module is the only writer
of thirteenf_arrivals and thirteenf_feed_reads, and the only place the ledger touches
the network.
"""
import argparse
import datetime as dt
import json
import sys
import time

from . import arrivals
from .arrivals import COVER_FLOOR, PAPER

SUBMISSIONS_PAGE = "https://data.sec.gov/submissions/{name}"


# ------------------------------------------------------------------ EDGAR --
def _get(url, contact):
    """The one network seam. Paced like every other EDGAR reader in the daemon."""
    import requests

    from . import thirteenf
    time.sleep(thirteenf.PACE)
    return requests.get(url, headers=thirteenf._ua(contact), timeout=30)


def _ok(resp, what):
    """A non-200 is a failure, never a body to parse: an error page that happens to be
    well-formed XML would otherwise be recorded as a filing that states nothing."""
    if resp.status_code != 200:
        raise RuntimeError("{} HTTP {}".format(what, resp.status_code))
    return resp


def _why(exc):
    """'KeyError: filings' rather than a bare 'filings'."""
    return "{}: {}".format(type(exc).__name__, exc)[:200]


# EDGAR gives a filing the day's date if it is accepted by 17:30 Eastern. A read made
# after that has seen everything dated that day; a read made before it has not.
CUTOFF = dt.timedelta(hours=17, minutes=30)


def read_day(unix):
    """The last US Eastern day whose filings a read at `unix` can have seen.

    The 22:30 nightly on day D answers for D (its UTC date is already D+1). A read at
    09:00 on a deadline day answers for the day BEFORE: of 150 deadline-day originals
    with a known stamp, 137 were accepted after 11:00 and 103 in the 16:00 hour. The
    deploy script reads at whatever hour the deploy happens, and that read must not be
    able to stand in for the night's."""
    return (_eastern(unix) - CUTOFF).date().isoformat()


def calendar_day(unix):
    """The US Eastern calendar day a read happened on. Not the day it answers for:
    that is read_day, and before 17:30 the two differ."""
    return _eastern(unix).date().isoformat()


def _eastern(unix):
    moment = dt.datetime.fromtimestamp(unix, dt.timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return moment.astimezone(ZoneInfo("America/New_York")).replace(tzinfo=None)
    except Exception:  # noqa: BLE001 - no tz database on this host
        return (moment - dt.timedelta(hours=5)).replace(tzinfo=None)


def fetch_feed(cik, contact, deep=False):
    """(13F-family rows, older_pages) from a filer's submissions feed.

    filings.recent holds about the newest 1,000 filings of ANY form, which reaches
    back only to mid-2023 for the heaviest filers; deep=True follows filings.files for
    the full history. older_pages is how many such pages the feed lists, read or not."""
    from . import thirteenf
    cik10 = str(int(cik)).zfill(10)
    d = _ok(_get(thirteenf.SUBMISSIONS.format(cik10=cik10), contact), "submissions").json()
    blocks = [d["filings"]["recent"]]
    older = d["filings"].get("files") or []
    if deep:
        for page in older:
            blocks.append(_ok(_get(SUBMISSIONS_PAGE.format(name=page["name"]), contact),
                              "submissions page").json())
    rows = []
    for b in blocks:
        rows.extend(arrivals.family_rows(b))
    return rows, len(older)


def fetch_cover(cik, accession, contact, primary_document=None):
    """(parsed cover, None) or (None, 'no_xml_cover').

    A text-era filing has no primary_doc.xml and a 404 there is a fact about the
    filing. But the feed names each filing's primary document, and when it names
    primary_doc.xml a 404 is a failed fetch: recording it as 'no cover' would leave a
    modern filing permanently unread on one bad response."""
    from . import thirteenf
    url = thirteenf.ARCH.format(cik=int(cik), acc_nodash=accession.replace("-", ""),
                                doc="primary_doc.xml")
    r = _get(url, contact)
    if r.status_code == 404 and not (primary_document or "").endswith("primary_doc.xml"):
        return None, "no_xml_cover"
    _ok(r, "cover")
    return arrivals.parse_cover(r.content), None


def fetch_accepted(cik, accession, contact):
    from . import thirteenf
    url = thirteenf.ARCH.format(cik=int(cik), acc_nodash=accession.replace("-", ""),
                                doc=accession + "-index.htm")
    stamp = arrivals.parse_accepted(_ok(_get(url, contact), "index page").text)
    if not stamp:
        raise RuntimeError("index page states no Accepted stamp")
    return stamp


# ------------------------------------------------------------------ writing --
def record_feed(con, cik, rows, now):
    """Insert the feed's filings the ledger has not recorded. Returns the new ones.

    first_seen_unix is written only for a filing this read could not have seen
    before: the feed had already been read successfully, AND the filing is dated on or
    after the newest filing that read saw. Everything else is history -- the whole
    first read, and every old filing a later --deep read turns up -- and stamping the
    read time on it would manufacture an arrival time for filings that are years old.
    It stays NULL, which reads 'not recorded'.

    A filing already recorded is never rewritten, with one exception: a period or
    filing date the feed did not state the first time is filled in when it does."""
    cik = str(int(cik))
    known = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT accession, period, filed_date FROM thirteenf_arrivals WHERE cik=?",
        (cik,))}
    prior = con.execute("SELECT read_unix, newest_filed FROM thirteenf_feed_reads "
                        "WHERE cik=?", (cik,)).fetchone()
    # '' when the earlier read saw no 13F at all: then any dated filing is new.
    seen_through = (prior[1] or "") if (prior and prior[0]) else None
    new = []
    for r in rows:
        if not r["accession"]:
            continue
        if r["accession"] in known:
            period, filed = known[r["accession"]]
            if (period is None and r["period"]) or (filed is None and r["filed_date"]):
                con.execute(
                    "UPDATE thirteenf_arrivals SET period=COALESCE(period, ?), "
                    "filed_date=COALESCE(filed_date, ?) WHERE cik=? AND accession=?",
                    (r["period"], r["filed_date"], cik, r["accession"]))
            continue
        known[r["accession"]] = (r["period"], r["filed_date"])
        observed = (now if seen_through is not None and r["filed_date"]
                    and r["filed_date"] >= seen_through else None)
        con.execute(
            "INSERT INTO thirteenf_arrivals(cik, accession, form, period, filed_date, "
            "feed_accepted, primary_document, cover_status, first_seen_unix, "
            "recorded_unix) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (cik, r["accession"], r["form"], r["period"], r["filed_date"],
             r["feed_accepted"], r["primary_document"],
             "paper" if r["form"] == PAPER else "pending", observed, now))
        new.append(r)
    return new


def _read_cover(con, cik, accession, form, period, primary_document, contact):
    cover, why = fetch_cover(cik, accession, contact, primary_document)
    if cover is None:
        con.execute("UPDATE thirteenf_arrivals SET cover_status=?, read_error=NULL "
                    "WHERE cik=? AND accession=?", (why, cik, accession))
        return
    if cover["submission_type"] != form or (period and cover["period"] != period):
        # The cover of some OTHER filing must not be recorded as this one's.
        raise RuntimeError("cover states {} for {}, feed says {} for {}".format(
            cover["submission_type"], cover["period"], form, period))
    if not period and not cover["period"]:
        raise RuntimeError("neither the feed nor the cover states a period")
    con.execute(
        "UPDATE thirteenf_arrivals SET cover_status='read', read_error=NULL, "
        "period=COALESCE(period, ?), report_type=?, is_amendment=?, amendment_no=?, "
        "amendment_type=?, entry_total=?, value_total=?, confidential_omitted=?, "
        "other_managers=? WHERE cik=? AND accession=?",
        (cover["period"], cover["report_type"], cover["is_amendment"],
         cover["amendment_no"], cover["amendment_type"], cover["entry_total"],
         cover["value_total"], cover["confidential_omitted"],
         json.dumps(cover["other_managers"]), cik, accession))


def read_details(con, cik, contact, floor=COVER_FLOOR):
    """Read the cover and the acceptance stamp of every filing still owed either.

    The two are owed and stored separately: a cover that parsed is kept even when the
    index page cannot be read, so the ledger's states never depend on an HTML page
    they do not need. Each is its own transaction, committed before the next request,
    so no write lock is held across the network and one bad filing cannot discard the
    others. A failure is recorded on the row with its reason and retried on the next
    run; it is never a state of the filing.

    A filing whose period the feed did not state is read whatever the floor: its
    cover states the period. Returns ({"covers": n, "stamps": n} stored, failures)."""
    cik = str(int(cik))
    todo = con.execute(
        "SELECT accession, form, period, primary_document, cover_status, accepted_et "
        "FROM thirteenf_arrivals WHERE cik=? AND form != ? AND "
        "(period IS NULL OR period >= ?) AND "
        "(cover_status IN ('pending','failed') OR accepted_et IS NULL) "
        "ORDER BY filed_date", (cik, PAPER, floor)).fetchall()
    read, failures = {"covers": 0, "stamps": 0}, []
    for accession, form, period, primary_document, status, accepted in todo:
        for what, owed in (("cover", status in ("pending", "failed")),
                           ("stamp", accepted is None)):
            if not owed:
                continue
            try:
                if what == "cover":
                    _read_cover(con, cik, accession, form, period, primary_document,
                                contact)
                else:
                    con.execute(
                        "UPDATE thirteenf_arrivals SET accepted_et=?, read_error=CASE "
                        "WHEN read_error LIKE 'stamp:%' THEN NULL ELSE read_error END "
                        "WHERE cik=? AND accession=?",
                        (fetch_accepted(cik, accession, contact), cik, accession))
                con.commit()
                read[what + "s"] += 1
            except Exception as exc:  # noqa: BLE001 - per filing, recorded and retried
                con.rollback()
                why = "{}: {}".format(what, _why(exc))
                if what == "cover":
                    con.execute("UPDATE thirteenf_arrivals SET cover_status='failed', "
                                "read_error=? WHERE cik=? AND accession=?",
                                (why, cik, accession))
                else:
                    # A cover's reason outlives a stamp failure on the same filing:
                    # the cover is what the states wait on.
                    con.execute("UPDATE thirteenf_arrivals SET read_error=CASE WHEN "
                                "read_error LIKE 'cover:%' THEN read_error ELSE ? END "
                                "WHERE cik=? AND accession=?", (why, cik, accession))
                con.commit()
                failures.append({"accession": accession, "what": what, "error": why})
    return read, failures


def read_filer(con, cik, contact, now=None, deep=False, floor=COVER_FLOOR):
    """One unfiltered read of a filer's feed into the ledger.

    The read time is recorded only after the feed has been fetched and its rows
    stored. A failed fetch records the attempt and its error and leaves the last
    successful read time alone, so 'awaited as of <date>' can never be dated by a read
    that did not happen."""
    cik = str(int(cik))
    now = int(time.time()) if now is None else now
    try:
        rows, older_pages = fetch_feed(cik, contact, deep=deep)
    except Exception as exc:  # noqa: BLE001 - recorded, then raised to the caller
        con.rollback()
        con.execute(
            "INSERT INTO thirteenf_feed_reads(cik, attempted_unix, error) VALUES (?,?,?) "
            "ON CONFLICT(cik) DO UPDATE SET attempted_unix=excluded.attempted_unix, "
            "error=excluded.error", (cik, now, _why(exc)))
        con.commit()
        raise
    try:
        new = _store_feed(con, cik, rows, older_pages, deep, now)
    except Exception as exc:  # noqa: BLE001 - recorded best effort, then raised
        # A lock held by another writer, say. Without this the scan line reported a
        # failed read and the ledger row was byte-identical to before: no attempt, no
        # error, nothing for a page to say.
        con.rollback()
        try:
            con.execute(
                "INSERT INTO thirteenf_feed_reads(cik, attempted_unix, error) VALUES "
                "(?,?,?) ON CONFLICT(cik) DO UPDATE SET attempted_unix="
                "excluded.attempted_unix, error=excluded.error",
                (cik, now, "store: " + _why(exc)))
            con.commit()
        except Exception:  # noqa: BLE001 - the original failure is the one to raise
            con.rollback()
        raise
    read, failures = read_details(con, cik, contact, floor)
    return {"family": len(rows), "new": new, "details_read": read["covers"],
            "stamps_read": read["stamps"], "details_failed": failures,
            "older_pages": older_pages}


def _store_feed(con, cik, rows, older_pages, deep, now):
    """The feed's rows and the record of the read, in one transaction."""
    new = record_feed(con, cik, rows, now)
    # `deep` is sticky: once the older pages are in the ledger they stay there, and an
    # ordinary nightly read afterwards must not make the ledger forget it holds them.
    con.execute(
        "INSERT INTO thirteenf_feed_reads(cik, read_unix, read_day, newest_filed, "
        "older_pages, deep, attempted_unix, error) VALUES (?,?,?,?,?,?,?,NULL) "
        "ON CONFLICT(cik) DO UPDATE SET read_unix=excluded.read_unix, "
        "read_day=excluded.read_day, newest_filed=excluded.newest_filed, "
        "older_pages=excluded.older_pages, "
        "deep=MAX(COALESCE(thirteenf_feed_reads.deep, 0), excluded.deep), "
        "attempted_unix=excluded.attempted_unix, error=NULL",
        (cik, now, read_day(now),
         max((r["filed_date"] for r in rows if r["filed_date"]), default=None),
         older_pages, 1 if deep else 0, now))
    con.commit()
    return new


def read_roster(con, contact, roster, now=None, deep=False, floor=COVER_FLOOR):
    """Read every filer in `roster` ({cik: name}). One filer failing never stops the
    rest, and is never reported as anything but a failure."""
    report = {"filers": len(roster), "read": 0, "family": 0, "new": [],
              "details_read": 0, "stamps_read": 0, "failed_reads": [],
              "failed_details": []}
    for cik, name in roster.items():
        try:
            r = read_filer(con, cik, contact, now=now, deep=deep, floor=floor)
        except Exception as exc:  # noqa: BLE001 - per filer, into the report
            con.rollback()
            report["failed_reads"].append(
                {"cik": str(int(cik)), "name": name, "error": _why(exc)})
            continue
        report["read"] += 1
        report["family"] += r["family"]
        report["details_read"] += r["details_read"]
        report["stamps_read"] += r["stamps_read"]
        for n in r["new"]:
            report["new"].append({"cik": str(int(cik)), "name": name, "form": n["form"],
                                  "period": n["period"], "filed_date": n["filed_date"],
                                  "accession": n["accession"]})
        for f in r["details_failed"]:
            report["failed_details"].append(dict(f, cik=str(int(cik)), name=name))
    return report


# ------------------------------------------------------------------ CLI --
def main(argv=None):
    """Read the roster's feeds into the ledger now, outside the nightly scan.

    The first run is the backfill: it records every 13F-family filing in each feed's
    recent block (or its whole history with --deep) and reads the covers from
    COVER_FLOOR on. scripts/post_deploy.sh runs it, so the nightly leg never has to.
    It writes only the two ledger tables, which nothing else writes, so there is no
    in-place repair here and no backup to take.

    Exit 0 = every feed and every owed cover read. 1 = some failed and are retried by
    the nightly leg. 3 = NO feed was read: the ledger is where it was, and a caller
    must not describe that as a read with failures. 2 = no EDGAR contact."""
    from . import db as dbmod
    from . import thirteenf_ingest
    from .efd_ingest import load_env

    ap = argparse.ArgumentParser(description="Read EDGAR 13F feeds into the arrival ledger")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--deep", action="store_true",
                    help="follow filings.files for each filer's full history")
    ap.add_argument("--floor", default=COVER_FLOOR,
                    help="read covers for periods on or after this quarter-end")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    report = read_roster(con, contact, thirteenf_ingest.CONFIRMED, deep=args.deep,
                         floor=args.floor)
    print("[arrivals] filers={filers} read={read} family_rows={family} "
          "covers_read={details_read} stamps_read={stamps_read}".format(**report))
    print("[arrivals] new to the ledger: {}".format(len(report["new"])))
    for f in report["failed_reads"]:
        print("[arrivals] FEED READ FAILED {} {}: {}".format(f["cik"], f["name"], f["error"]))
    for f in report["failed_details"]:
        print("[arrivals] DETAIL READ FAILED {} {} {}: {}".format(
            f["cik"], f["name"], f["accession"], f["error"]))
    if report["filers"] and not report["read"]:
        print("[arrivals] NO FEED WAS READ", file=sys.stderr)
        return 3
    return 1 if (report["failed_reads"] or report["failed_details"]) else 0


if __name__ == "__main__":
    sys.exit(main())
