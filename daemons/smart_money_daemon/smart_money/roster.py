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
import unicodedata
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
    """Comparison key: fold accents, lowercase, drop punctuation and a trailing name
    suffix ('Fleming, Jr' -> 'fleming', 'McConnell, Jr.' -> 'mcconnell').

    ACCENT FOLDING IS LOAD-BEARING: stripping non-ASCII instead of folding it turns
    'Barragan' with its acute accent into 'barragn', which matches nothing — that
    silently dropped a sitting House member from the roster join."""
    s = unicodedata.normalize("NFKD", (s or "")).encode("ascii", "ignore").decode()
    s = _SUFFIX.sub("", s.lower().strip())
    return re.sub(r"\s+", " ", re.sub(r"[^a-z\s\-]", "", s)).strip()


def surname_keys(last):
    """Candidate surname keys, most specific first. Filers sometimes write a compound
    surname the roster records under one part ('Paulina Luna' vs roster 'Luna'), so the
    trailing token is tried as a fallback — never the leading one, which would make
    'Van Duyne' match every 'Van'."""
    n = norm(last)
    keys = [n]
    toks = n.split()
    if len(toks) > 1 and len(toks[-1]) > 2:
        keys.append(toks[-1])
    return keys


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


def _dist(d):
    """District as a zero-padded 2-char string, matching House state_dist ('OH04')."""
    try:
        return "{:02d}".format(int(d))
    except (TypeError, ValueError):
        return None


def build_index(entries):
    """(house_by_last_state_district, house_by_last_state, senate_recent_by_last).

    The DISTRICT-qualified key is the precise one: House filings carry 'OH04', and a
    surname+state key alone collides with every historical namesake from that state
    (there are 11 roster rows for (jordan, OH), spanning parties, which is what made
    Jim Jordan unresolvable). State-only remains as a fallback for at-large/odd districts.
    """
    house_d = collections.defaultdict(list)
    house_s = collections.defaultdict(list)
    senate = collections.defaultdict(list)
    for e in entries:
        if e["state"]:
            st = e["state"].upper()
            house_s[(norm(e["last"]), st)].append(e)
            d = _dist(e.get("district"))
            if d:
                house_d[(norm(e["last"]), st, d)].append(e)
        if e["type"] == "sen" and (e["end"] or "") >= SENATE_RECENT_FLOOR:
            senate[norm(e["last"])].append(e)
    return house_d, house_s, senate


def _verdict(cands, kind):
    parties = {c["party"] for c in cands}
    states = {c["state"] for c in cands}
    if len(parties) == 1 and len(states) == 1:
        # SM-C3 Phase R: carry the bioguide out only when the candidates agree on ONE
        # person. Same key, same party, two different people is a party match but NOT an
        # identity match, and a committee attached on that basis would be a false claim.
        bg = {c.get("bioguide") for c in cands if c.get("bioguide")}
        return {"party": parties.pop(), "state": states.pop(), "match_kind": kind,
                "bioguide": (bg.pop() if len(bg) == 1 else None)}
    return None


def resolve(index, chamber, last, first, state_dist):
    """{party, state, match_kind} for one filer identity. match_kind:
      'unique'   — the key resolved to a single party outright
      'byname'   — same key, tie broken by the given name
      'incumbent'— same key, tie broken by taking the SEAT'S most recent holder
      'unmatched'— party None, never guessed
    Also returns `bioguide`, but ONLY when the surviving candidates are one person — a
    key that agrees on party across two people is not an identity match.
    """
    house_d, house_s, senate_idx = index
    st = (state_dist or "")[:2].upper()
    keys = []
    for sn in surname_keys(last):
        if chamber == "senate":
            keys.append(senate_idx.get(sn, []))
        else:
            d = (state_dist or "")[2:].strip() or None
            if st and d:
                keys.append(house_d.get((sn, st, d.zfill(2)), []))
            if st:
                keys.append(house_s.get((sn, st), []))
    for cands in keys:
        if not cands:
            continue
        v = _verdict(cands, "unique")
        if v:
            return v
        narrowed = [c for c in cands if first_agrees(first, c)]
        if narrowed:
            v = _verdict(narrowed, "byname")
            if v:
                return v
        # Same seat, several holders across history with differing parties (11 rows for
        # (jordan, OH) alone). Our corpus is 2024+ filings, so the filer is the seat's
        # CURRENT holder — take the most recent term. Only applied to a keyed hit, and
        # only when that newest term is itself recent, so this can never reach back and
        # label a filer with some 19th-century namesake's party.
        newest = max(cands, key=lambda c: c["end"] or "")
        if (newest["end"] or "") >= SENATE_RECENT_FLOOR:
            top = [c for c in cands if c["end"] == newest["end"]]
            v = _verdict(top, "incumbent")
            if v:
                return v
    return {"party": None, "state": None, "match_kind": "unmatched",
            "bioguide": None}


def sync(con, entries=None):
    """Resolve every distinct filer identity in congress_holdings into
    congress_member_roster. Idempotent; returns the match-kind tally."""
    index = build_index(entries if entries is not None else fetch_roster())
    tally = collections.Counter()
    now = int(time.time())
    # Full rebuild of derived data. REQUIRED, not tidiness: state_dist is NULL for every
    # Senate identity and SQLite treats NULL != NULL in a PRIMARY KEY, so INSERT OR
    # REPLACE never collides on those rows and each sync would append a fresh duplicate
    # set (484 identities had grown to 698 rows).
    con.execute("DELETE FROM congress_member_roster")
    for chamber, last, first, sd in con.execute(
            "SELECT DISTINCT chamber, member_last, member_first, state_dist "
            "FROM congress_holdings"):
        r = resolve(index, chamber, last, first, sd)
        tally[r["match_kind"]] += 1
        con.execute(
            "INSERT OR REPLACE INTO congress_member_roster(chamber, member_last, "
            "member_first, state_dist, party, state, match_kind, synced_at_unix, "
            "bioguide) VALUES(?,?,?,?,?,?,?,?,?)",
            (chamber, last, first, sd, r["party"], r["state"], r["match_kind"], now,
             r.get("bioguide")))
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
