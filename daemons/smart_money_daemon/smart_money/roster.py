"""SM-C2 Phase 3 congressional roster resolution: party + state for FD filers.

Keyless public source (unitedstates/congress-legislators, the source the order named).
Scripts-only, no LLM.

WHY DETERMINISTIC KEYS ONLY. Filers file under their LEGAL name while the roster carries
their COMMON name — Tuberville files "Thomas H" (roster: Tommy), Budd "THEODORE P" (Ted),
Cruz "Rafael E" (Ted), Ossoff "Thomas J" (Jon), Ricketts "John P" (Pete). Fuzzy
name-matching to close that gap measured 59% and mislabels people, so it is NOT used to
assign party. Instead:
  * House filings carry state_dist ('CA11') -> key on (surname, state). Measured 97.4%
    precision on key-hits, 1.9% ambiguous.
  * Senate filings carry no state, so the roster is narrowed to senators serving recently
    (a ~134-surname space) and keyed on surname. Measured 100% precision, 0 ambiguous.
  * A given name is used ONLY to break a tie between same-key entries, never to create a
    match.
Anything that does not resolve deterministically is left party=NULL with match_kind
'unmatched' — NEVER guessed. That bucket is dominated by CANDIDATES who filed an FD but
never served (~37% of filers), which is a real property of the eFD/House corpus, not a
parser gap; the /congress gap list surfaces it so breadth counts read as floors.
"""
import argparse
import collections
import json
import re
import sys
import time
import urllib.request

from . import db as dbmod

BASE = "https://unitedstates.github.io/congress-legislators/"
FILES = ("legislators-current.json", "legislators-historical.json")
# A senator whose term ended before this is not a plausible current-corpus filer; the cut
# keeps the surname-only Senate space small enough to stay collision-free.
SENATE_RECENT_FLOOR = "2021-01-01"
_SUFFIX = re.compile(r"[\s,]+\b(jr|sr|ii|iii|iv|v)\b\.?\s*$", re.I)


def norm(s):
    """Comparison key: lowercase, drop punctuation and a trailing name suffix
    ('Fleming, Jr' -> 'fleming', 'McConnell, Jr.' -> 'mcconnell')."""
    s = _SUFFIX.sub("", (s or "").lower().strip())
    return re.sub(r"\s+", " ", re.sub(r"[^a-z\s\-]", "", s)).strip()


def _given(s):
    """Informative given-name tokens — never a bare initial ('David H' -> ['david'])."""
    return [t for t in norm(s).split() if len(t) > 1]


def first_agrees(filer_first, entry):
    """Tie-break only: does the filer's given name plausibly match this roster entry?"""
    ftoks = _given(filer_first)
    ctoks = set(_given(entry.get("first")) + _given(entry.get("nick"))
                + _given(entry.get("official")))
    if not ftoks or not ctoks:
        return False
    if set(ftoks) & ctoks:
        return True
    return any(a.startswith(b) or b.startswith(a)             # Joe/Joseph, Mitch/Mitchell
               for a in ftoks for b in ctoks if min(len(a), len(b)) >= 3)


def fetch_roster(timeout=60):
    """All roster entries flattened to one dict per (person, term)."""
    out = []
    for fn in FILES:
        with urllib.request.urlopen(BASE + fn, timeout=timeout) as r:
            for m in json.loads(r.read().decode()):
                nm = m.get("name") or {}
                for t in m.get("terms") or []:
                    out.append({"first": nm.get("first"), "last": nm.get("last"),
                                "nick": nm.get("nickname"), "official": nm.get("official_full"),
                                "bioguide": (m.get("id") or {}).get("bioguide"),
                                "type": t.get("type"), "state": t.get("state"),
                                "district": t.get("district"), "party": t.get("party"),
                                "end": t.get("end")})
    return out


def build_index(entries):
    """(house_by_last_state, senate_recent_by_last) — the two deterministic key spaces."""
    house = collections.defaultdict(list)
    senate = collections.defaultdict(list)
    for e in entries:
        if e["state"]:
            house[(norm(e["last"]), e["state"].upper())].append(e)
        if e["type"] == "sen" and (e["end"] or "") >= SENATE_RECENT_FLOOR:
            senate[norm(e["last"])].append(e)
    return house, senate


def resolve(index, chamber, last, first, state_dist):
    """{party, state, match_kind} for one filer identity. match_kind is 'unique' (the key
    resolved to one party outright), 'byname' (same key, tie broken by given name), or
    'unmatched' (party None — no key hit, or a tie the given name could not break)."""
    house_idx, senate_idx = index
    if chamber == "senate":
        cands = senate_idx.get(norm(last), [])
    else:
        st = (state_dist or "")[:2].upper()
        cands = house_idx.get((norm(last), st), []) if st else []
    if not cands:
        return {"party": None, "state": None, "match_kind": "unmatched"}
    parties = {c["party"] for c in cands}
    states = {c["state"] for c in cands}
    if len(parties) == 1 and len(states) == 1:
        return {"party": parties.pop(), "state": states.pop(), "match_kind": "unique"}
    narrowed = [c for c in cands if first_agrees(first, c)]
    np_ = {c["party"] for c in narrowed}
    ns = {c["state"] for c in narrowed}
    if len(np_) == 1 and len(ns) == 1:
        return {"party": np_.pop(), "state": ns.pop(), "match_kind": "byname"}
    return {"party": None, "state": None, "match_kind": "unmatched"}


def sync(con, entries=None):
    """Resolve every distinct filer identity in congress_holdings into
    congress_member_roster. Idempotent; returns the match-kind tally."""
    index = build_index(entries if entries is not None else fetch_roster())
    tally = collections.Counter()
    now = int(time.time())
    for chamber, last, first, sd in con.execute(
            "SELECT DISTINCT chamber, member_last, member_first, state_dist "
            "FROM congress_holdings"):
        r = resolve(index, chamber, last, first, sd)
        tally[r["match_kind"]] += 1
        con.execute(
            "INSERT OR REPLACE INTO congress_member_roster(chamber, member_last, "
            "member_first, state_dist, party, state, match_kind, synced_at_unix) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (chamber, last, first, sd, r["party"], r["state"], r["match_kind"], now))
    con.commit()
    return tally


def main(argv=None):
    ap = argparse.ArgumentParser(description="Sync congressional roster (SM-C2 P3)")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    try:
        tally = sync(con)
    finally:
        con.close()
    total = sum(tally.values())
    print("[roster] identities {} {}".format(total, dict(tally)))
    if total:
        matched = tally["unique"] + tally["byname"]
        print("[roster] party assigned to {}/{} ({:.1f}%); {} unmatched are dominated by "
              "CANDIDATES who filed but never served".format(
                  matched, total, 100.0 * matched / total, tally["unmatched"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
