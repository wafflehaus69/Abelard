"""ORDER SM-P2 Phase 1 — resolve 13F manager CIKs via EDGAR and report filing history.

Read-only EDGAR. NO ingest, NO registry writes. Identity = EDGAR CIK. Where a name is
ambiguous, EVERY candidate with 13F history is surfaced for Mando to pick — the resolver
never silently chooses. Mirrors network_recon.py (SM-A1 Phase 1) discipline and reuses its
HTTP helpers.

Reported per candidate: conformed name, 13F form-type breakdown (13F-HR holdings reports,
13F-NT notice-of-no-holdings, amendments), distinct report periods, and date range. A
13F-NT period is a LEGITIMATE result (the filer reported no Section 13(f) holdings) and is
rendered as such, never as an empty book — this is the expected shape for Scion.

DELIBERATE EXCLUSIONS (per order, not oversight): Renaissance, Citadel, Millennium, Two
Sigma, DE Shaw, Balyasny. Quant/multistrat 13Fs are structurally illegible — RenTech files
RIEF/RIDA/RIDGE, not Medallion, and runs ~3,213 positions.
"""
import argparse
import collections
import datetime as dt
import re
import sys

from .network_recon import _ua, getcompany_candidates, submissions

# (search name, tier, thesis tag, note)
TARGETS = [
    ("Coatue Management", "tier1", "ai_tmt", "Laffont, TMT/AI"),
    ("Whale Rock Capital Management", "tier1", "ai_tmt", "Sacerdote, TMT"),
    ("Light Street Capital Management", "tier1", "ai_tmt", "Kacher, tech"),
    ("Lone Pine Capital", "tier1", "ai_tmt", "Tiger cub"),
    ("Appaloosa", "tier1", "value", "Tepper"),
    ("Baker Bros Advisors", "tier1", "biotech", "adjacent to GUTS/ABCL surfacing"),
    ("Pershing Square Capital Management", "tier1", "activist", "Ackman, ~10 positions"),
    ("Soros Fund Management", "tier2", "macro", ""),
    ("Third Point", "tier2", "activist", ""),
    ("Greenlight Capital", "tier2", "value", ""),
    ("Scion Asset Management", "wildcard", "contrarian",
     "expect small, put-heavy, possible 13F-NT and confidential-treatment gaps"),
]

EXCLUDED = [
    ("Renaissance Technologies", "files RIEF/RIDA/RIDGE, not Medallion; ~3,213 positions"),
    ("Citadel", "quant/multistrat, structurally illegible"),
    ("Millennium", "quant/multistrat, structurally illegible"),
    ("Two Sigma", "quant/multistrat, structurally illegible"),
    ("DE Shaw", "quant/multistrat, structurally illegible"),
    ("Balyasny", "quant/multistrat, structurally illegible"),
]

_PERIOD_HINT = re.compile(r"13F-(HR|NT)(/A)?", re.I)


def profile(contact, cik10):
    """13F filing profile for one CIK, or None when EDGAR has no submissions."""
    sub = submissions(contact, cik10)
    if not sub:
        return None
    kinds = collections.Counter()
    dates = []
    for _acc, form, fdate in sub["forms"]:
        if form.upper().startswith("13F"):
            kinds[form.upper()] += 1
            dates.append(fdate)
    return {"name": sub.get("name"), "kinds": dict(kinds), "n": sum(kinds.values()),
            "first": min(dates) if dates else None,
            "last": max(dates) if dates else None,
            # a 13F is filed 45d after quarter end; distinct filing dates approximate
            # distinct reported periods well enough for a resolution report
            "periods": len(set(dates))}


def resolve_one(contact, name, form_type="13F-HR"):
    """[candidate] for a manager name, each with its 13F profile. Candidates WITHOUT any
    13F history are reported too (as zero) so an ambiguous name shows its full surface."""
    out = []
    seen = set()
    for c in getcompany_candidates(contact, name, form_type=form_type):
        if c["cik"] in seen:
            continue
        seen.add(c["cik"])
        pr = profile(contact, c["cik"])
        out.append({"cik": c["cik"], "edgar_name": c["name"],
                    "profile": pr or {"kinds": {}, "n": 0, "periods": 0,
                                      "first": None, "last": None, "name": None}})
    out.sort(key=lambda c: -c["profile"]["n"])
    return out


def render(results, path):
    L = []
    L.append("# ORDER SM-P2 Phase 1 — 13F manager CIK resolution\n")
    L.append("Read-only EDGAR resolution. Identity = EDGAR CIK. No ingest, no registry "
             "writes. Where a name resolves to more than one CIK with 13F history, EVERY "
             "candidate is listed — Mando picks, the resolver never chooses silently.\n")
    L.append("`13F-HR` = holdings report. `13F-NT` = notice of NO Section 13(f) holdings "
             "— a legitimate reported state, rendered as such, never as an empty book. "
             "`/A` = amendment.\n")
    for name, tier, tag, note in TARGETS:
        cands = results.get(name, [])
        L.append("\n## {} ({}, thesis `{}`)".format(name, tier, tag))
        if note:
            L.append("_{}_\n".format(note))
        withf = [c for c in cands if c["profile"]["n"] > 0]
        if not cands:
            L.append("- **NO CANDIDATE RESOLVED** — no EDGAR company match.")
            continue
        if not withf:
            L.append("- Candidates found but NONE has 13F history:")
        elif len(withf) > 1:
            L.append("- **AMBIGUOUS — {} candidates with 13F history. MANDO PICKS.**"
                     .format(len(withf)))
        for c in cands:
            p = c["profile"]
            kinds = ", ".join("{} x{}".format(k, v) for k, v in sorted(p["kinds"].items()))
            L.append("  - CIK {} `{}` — 13F filings: {}{} | periods {} | {} .. {}".format(
                c["cik"], c["edgar_name"], p["n"],
                (" [" + kinds + "]") if kinds else "", p["periods"],
                p["first"] or "-", p["last"] or "-"))
    # ---- rulings needed: computed from the data, never hand-listed
    lasts = [c["profile"]["last"] for cs in results.values() for c in cs
             if c["profile"]["last"]]
    newest = max(lasts) if lasts else None
    stale_cut = None
    if newest:
        stale_cut = (dt.date.fromisoformat(newest) - dt.timedelta(days=200)).isoformat()
    amb, stale, none_found = [], [], []
    for name, _t, _g, _n in TARGETS:
        withf = [c for c in results.get(name, []) if c["profile"]["n"] > 0]
        if not withf:
            none_found.append(name)
            continue
        if len(withf) > 1:
            amb.append((name, withf))
        top = max(withf, key=lambda c: c["profile"]["last"] or "")
        if stale_cut and (top["profile"]["last"] or "") < stale_cut:
            stale.append((name, top))
    L.append("\n## RULINGS NEEDED before ingest\n")
    if not (amb or stale or none_found):
        L.append("- None. Every target resolved to exactly one current 13F filer.")
    for name, withf in amb:
        L.append("- **{} — AMBIGUOUS.** Candidates:".format(name))
        for c in withf:
            L.append("  - CIK {} — {} filings, {} .. {}{}".format(
                c["cik"], c["profile"]["n"], c["profile"]["first"], c["profile"]["last"],
                "  <- most recent" if c["profile"]["last"] == max(
                    x["profile"]["last"] or "" for x in withf) else ""))
    for name, top in stale:
        L.append("- **{} — STALE.** Newest 13F is {} while the target set runs to {}. "
                 "An 8-quarter window from today would be largely EMPTY; this needs a "
                 "keep/drop ruling rather than a silently thin book.".format(
                     name, top["profile"]["last"], newest))
    for name in none_found:
        L.append("- **{} — NO 13F HISTORY** on any resolved candidate.".format(name))
    L.append("\n## Deliberate exclusions (per order, NOT oversight)\n")
    for nm, why in EXCLUDED:
        L.append("- **{}** — {}".format(nm, why))
    L.append("\n## STOP\n")
    L.append("Resolution only. No CIK ingested, no registry entry written. Awaiting "
             "Mando's picks before ORDER SM-P2 ingest (8 quarters each, gates G1-G4).")
    text = "\n".join(L) + "\n"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return text


def main(argv=None):
    from .efd_ingest import load_env
    ap = argparse.ArgumentParser(description="SM-P2 Phase 1 manager CIK resolution")
    ap.add_argument("--out", default="scans/SM_P2_FILER_RESOLUTION.md")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT") or "smartmoney@example.com"
    results = {}
    for name, _tier, _tag, _note in TARGETS:
        cands = resolve_one(contact, name)
        results[name] = cands
        top = cands[0] if cands else None
        print("[resolve] {:<36} {} candidate(s){}".format(
            name, len(cands),
            "" if not top else "  best: CIK {} n13F={}".format(
                top["cik"], top["profile"]["n"])), flush=True)
    render(results, args.out)
    print("[resolve] wrote {}".format(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
