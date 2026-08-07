"""SM-C3 Phase R: committee membership, joined to FD filers through bioguide.

Same keyless public source as the roster (unitedstates/congress-legislators). Two files:
  * committees-current.json          — 49 parent committees, each with subcommittees.
  * committee-membership-current.json — {thomas_id: [{bioguide, title, rank, party}]},
                                        230 keys once subcommittees are counted.

TWO STANDING LIMITS, both of which any view built on this MUST state rather than absorb:

1. CURRENT CONGRESS ONLY. The dataset publishes no historical membership file. So this
   answers "what does this member sit on NOW", never "what did they sit on in CY2024".
   Holdings are dated; committee membership here is not. Pairing a CY2023 holding with a
   2026 committee seat is a real hazard and the join must be read as "this member, who
   today sits on X, disclosed Y in CY2023" — not as a claim about what they sat on then.

2. THE JOIN IS PARTIAL, AND NOT UNIFORMLY SO. A committee can only attach to a filer
   identity the roster resolved to ONE person. Measured on the live corpus: 68.1% of
   anchor holdings rows would carry a committee (house 72.6%, senate 61.0%); on
   ticker-bearing anchor rows it is house 68.5% / senate 52.7%. A committee cut that
   does not print that number reads as complete when a third of the corpus is invisible.

WHY THE GAP IS NOT A MATCHER BUG. 167 of 172 unmatched House identities have no original
annual ('O') filing at all — they are candidates, admitted because house_fd_ingest
ANNUAL_TYPES accepts amendments and the House index does not distinguish an amended
candidate report from an amended member annual. Restricted to House identities that filed
a member annual, the roster join is 98.9%. That narrowed figure is a DIAGNOSTIC, never
the coverage number to quote — quoting it would be the gate-shopping the Phase H ruling
rejected.
"""
import argparse
import json
import sys
import time
import urllib.request

from . import db as dbmod
from .roster import BASE

COMMITTEES = "committees-current.json"
MEMBERSHIP = "committee-membership-current.json"


def fetch(timeout=45, base=BASE):
    """(committees, membership) as parsed JSON. Fails loud — a partial committee set
    would silently shrink every downstream cut."""
    out = []
    for fn in (COMMITTEES, MEMBERSHIP):
        req = urllib.request.Request(base + fn, headers={"User-Agent": "smart-money"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out.append(json.loads(r.read().decode()))
    return out[0], out[1]


def flatten(committees, membership):
    """[(bioguide, committee_id, parent_id, name, chamber, title, rank, side)].

    Subcommittee ids in the membership file are the PARENT id concatenated with the
    subcommittee's own id (HSAG + 15 -> HSAG15), so the parent is recoverable by prefix
    and a member's subcommittee seat never loses its committee."""
    meta = {}
    for c in committees:
        cid = c.get("thomas_id")
        if not cid:
            continue
        meta[cid] = (None, c.get("name"), c.get("type"))
        for sub in c.get("subcommittees") or []:
            sid = sub.get("thomas_id")
            if sid:
                meta[cid + sid] = (cid, "{} - {}".format(c.get("name"), sub.get("name")),
                                   c.get("type"))
    rows = []
    for cid, members in (membership or {}).items():
        parent, name, chamber = meta.get(cid, (None, None, None))
        for m in members or []:
            bg = m.get("bioguide")
            if not bg:
                continue
            rows.append((bg, cid, parent, name, chamber, m.get("title"),
                         m.get("rank"), m.get("party")))
    return rows


def sync(con, committees=None, membership=None):
    """Rebuild congress_committees. Idempotent; returns {committees, memberships,
    unknown_ids}. A full rebuild because membership is a CURRENT snapshot — keeping stale
    rows would quietly assert seats a member no longer holds."""
    if committees is None or membership is None:
        committees, membership = fetch()
    rows = flatten(committees, membership)
    now = int(time.time())
    con.execute("DELETE FROM congress_committees")
    for r in rows:
        con.execute(
            "INSERT OR REPLACE INTO congress_committees(bioguide, committee_id, "
            "parent_id, committee_name, chamber, title, rank, side, synced_at_unix) "
            "VALUES(?,?,?,?,?,?,?,?,?)", r + (now,))
    con.commit()
    return {"committees": len({r[1] for r in rows}),
            "memberships": len(rows),
            "members": len({r[0] for r in rows}),
            # A membership id absent from committees-current is reported, never dropped
            # silently — it means the two files drifted and names are missing.
            "unknown_ids": sorted({r[1] for r in rows if r[3] is None})}


def coverage(con):
    """How much of the ANCHOR holdings corpus a committee cut can actually see.

    Reported per chamber because the chambers differ by more than ten points and one
    aggregate number hides that. `rows` is anchor rows; `with_committee` is anchor rows
    whose filer resolved to a bioguide that carries at least one committee seat."""
    seats = {r[0] for r in con.execute("SELECT DISTINCT bioguide FROM congress_committees")}
    bio = {}
    for c, l, f, s, b in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, bioguide "
            "FROM congress_member_roster"):
        bio[(c, l, f, s)] = b
    latest = {}
    for c, l, f, s, y in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, MAX(coverage_year) "
            "FROM congress_holdings WHERE coverage_year IS NOT NULL "
            "GROUP BY chamber, member_last, member_first, state_dist"):
        latest[(c, l, f, s)] = y
    out = {}
    for c, l, f, s, y, tk in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "ticker FROM congress_holdings WHERE coverage_year IS NOT NULL"):
        k = (c, l, f, s)
        if latest.get(k) != y:                       # anchor rows only
            continue
        cell = out.setdefault(c, {"rows": 0, "with_committee": 0, "tick_rows": 0,
                                  "tick_with_committee": 0, "members": set(),
                                  "members_with_committee": set(),
                                  "no_bioguide": 0, "left_congress": 0})
        b = bio.get(k)
        seen = b is not None and b in seats
        cell["rows"] += 1
        cell["members"].add(k)
        if seen:
            cell["with_committee"] += 1
            cell["members_with_committee"].add(k)
        elif b is None:
            # The filer identity never resolved to one person — candidate, or a genuine
            # matcher miss. This is the bucket the "69% roster join" refers to.
            cell["no_bioguide"] += 1
        else:
            # Resolved to a real person who holds NO seat in the current Congress. Not a
            # data defect at all: they left office, and our anchors run back years. This
            # bucket is invisible to the roster-join number and has to be named
            # separately or the committee gap looks like a matcher failure it is not.
            cell["left_congress"] += 1
        if tk:
            cell["tick_rows"] += 1
            if seen:
                cell["tick_with_committee"] += 1
    for cell in out.values():
        cell["members"] = len(cell["members"])
        cell["members_with_committee"] = len(cell["members_with_committee"])
    return out


def _pct(a, b):
    return round(100.0 * a / b, 1) if b else None


def render(cov, stats=None):
    lines = ["SM-C3 PHASE R - COMMITTEE JOIN COVERAGE (anchor rows)", "=" * 68]
    if stats:
        lines.append("committees {} | memberships {} | members with a seat {}".format(
            stats["committees"], stats["memberships"], stats["members"]))
        if stats.get("unknown_ids"):
            lines.append("UNNAMED committee ids (files drifted): {}".format(
                ", ".join(stats["unknown_ids"])))
        lines.append("")
    lines.append("%-8s %9s %9s %8s %9s %9s %8s" % (
        "chamber", "rows", "w/ cmte", "cov%", "tickrows", "w/ cmte", "cov%"))
    tot = [0, 0, 0, 0, 0, 0]
    for cham in sorted(cov):
        c = cov[cham]
        tot = [tot[0] + c["rows"], tot[1] + c["with_committee"],
               tot[2] + c["tick_rows"], tot[3] + c["tick_with_committee"],
               tot[4] + c["no_bioguide"], tot[5] + c["left_congress"]]
        lines.append("%-8s %9d %9d %7s%% %9d %9d %7s%%" % (
            cham, c["rows"], c["with_committee"], _pct(c["with_committee"], c["rows"]),
            c["tick_rows"], c["tick_with_committee"],
            _pct(c["tick_with_committee"], c["tick_rows"])))
    lines.append("%-8s %9d %9d %7s%% %9d %9d %7s%%" % (
        "ALL", tot[0], tot[1], _pct(tot[1], tot[0]), tot[2], tot[3],
        _pct(tot[3], tot[2])))
    lines += [
        "",
        "WHY THE UNCOVERED ROWS ARE UNCOVERED - two causes, not one:",
        "%-8s %9s %9s   %s" % ("chamber", "no bioguide", "left Cong.", "of uncovered"),
    ]
    for cham in sorted(cov):
        c = cov[cham]
        unc = c["no_bioguide"] + c["left_congress"]
        lines.append("%-8s %9d %9d   %s%% / %s%%" % (
            cham, c["no_bioguide"], c["left_congress"],
            _pct(c["no_bioguide"], unc), _pct(c["left_congress"], unc)))
    lines += [
        "%-8s %9d %9d   %s%% / %s%%" % (
            "ALL", tot[4], tot[5], _pct(tot[4], tot[4] + tot[5]),
            _pct(tot[5], tot[4] + tot[5])),
        "",
        "'no bioguide' is the roster-join gap. 'left Cong.' is a member who resolved",
        "cleanly but holds no seat in the CURRENT Congress - our anchors run back years,",
        "and membership is a present-tense snapshot. The second bucket is invisible to",
        "the roster-join rate, so committee coverage is strictly WORSE than that rate and",
        "must never be quoted from it.",
    ]
    lines += [
        "",
        "A committee cut is BLIND to the remainder. The dominant cause is not a matcher",
        "failure: unmatched House identities are overwhelmingly candidates who filed but",
        "never served, admitted because the House index does not distinguish an amended",
        "candidate report from an amended member annual.",
        "The Senate cause cannot be attributed from the data we hold - senate_fd_ingest",
        "discards the eFD office column, so no served/candidate flag exists there.",
        "",
        "Membership is CURRENT-CONGRESS ONLY. There is no historical file, so this says",
        "what a member sits on today, never what they sat on in a past coverage year.",
    ]
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Sync committee membership (SM-C3 Phase R)")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--coverage-only", action="store_true",
                    help="report join coverage without re-fetching")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    try:
        stats = None if args.coverage_only else sync(con)
        print(render(coverage(con), stats))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
