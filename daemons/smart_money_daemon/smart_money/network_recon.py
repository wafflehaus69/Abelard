"""SM-A1 Phase 1: EDGAR filer resolution + person-first issuer enumeration.

Read-only against EDGAR. Produces a report under scans/; NO ingest, NO registry
writes. Identity stays inside EDGAR (persistent CIKs); outside-EDGAR naming is
fuzzy and out of scope. STOP at report.

Two resolvers, both live-verified:
  - efts.sec.gov/LATEST/search-index full-text search (entity display_names
    carry the CIK) for 13F managers.
  - browse-edgar getcompany atom (type=4) for ownership reporting persons.
Person-first enumeration: for each resolved reporting-person CIK, walk their
ownership filings (submissions API) and read each filing's issuer from the XML,
aggregating per issuer (ticker, first/last date, count, roles).
"""
import argparse
import datetime as dt
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict

import requests

from . import db as dbmod
from . import form4
from .efd_ingest import load_env

EFTS = "https://efts.sec.gov/LATEST/search-index"
BROWSE = "https://www.sec.gov/cgi-bin/browse-edgar"
SUB = "https://data.sec.gov/submissions/CIK{cik10}.json"
PACE = 0.12
OWNERSHIP_FORMS = {"3", "4", "5", "3/A", "4/A", "5/A"}

THIRTEEN_F_TARGETS = [
    {"name": "Duquesne Family Office LLC", "who": "Druckenmiller", "conf": "high"},
    {"name": "Thiel Macro", "who": "Thiel", "conf": "low"},
    {"name": "Founders Fund", "who": "Thiel", "conf": "low"},
    {"name": "Affinity Partners", "who": "Kushner", "conf": "hypothesis"},
]
# EDGAR stores reporting persons LAST FIRST; queries are in that order.
OWNERSHIP_TARGETS = [
    {"who": "Donald J. Trump and/or Revocable Trust",
     "queries": ["Trump Donald J", "Donald J. Trump Revocable Trust"]},
    {"who": "Donald Trump Jr.", "queries": ["Trump Donald J. JR", "Trump Donald John"]},
    {"who": "Eric Trump", "queries": ["Trump Eric"]},
    {"who": "Peter Thiel", "queries": ["Thiel Peter"]},
]
CONFIRM_NEGATIVE = [
    {"who": "Druckenmiller", "queries": ["Druckenmiller Stanley", "Druckenmiller"],
     "expect": "absent from ownership surface"},
]


def _ua(contact):
    return {"User-Agent": form4.UA_TMPL.format(contact)}


def efts_entities(contact, query, forms=None):
    params = {"q": '"{}"'.format(query)}
    if forms:
        params["forms"] = forms
    time.sleep(PACE)
    r = requests.get(EFTS, params=params, headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        return []
    hits = r.json().get("hits", {}).get("hits", [])
    seen = {}
    for h in hits:
        for dn in h["_source"].get("display_names", []):
            m = re.search(r"\(CIK (\d+)\)", dn)
            if m:
                seen[m.group(1).zfill(10)] = re.sub(r"\s*\(CIK \d+\)", "", dn).strip()
    return [{"cik": c, "name": n} for c, n in seen.items()]


def getcompany_candidates(contact, company, form_type="4"):
    params = {"action": "getcompany", "company": company, "type": form_type,
              "dateb": "", "owner": "include", "count": "40", "output": "atom"}
    time.sleep(PACE)
    r = requests.get(BROWSE, params=params, headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        return []
    out = []
    # multi-match: <cik> + <conformed-name>; single-match feed: header cik/name
    for m in re.finditer(
            r"<cik>(\d+)</cik>\s*<conformed-name>([^<]+)</conformed-name>", r.text):
        out.append({"cik": m.group(1).zfill(10), "name": m.group(2).strip()})
    if not out:
        ciks = re.findall(r"<cik>(\d+)</cik>", r.text)
        name = re.search(r"<conformed-name>([^<]+)</conformed-name>", r.text)
        for c in dict.fromkeys(ciks):
            out.append({"cik": c.zfill(10), "name": name.group(1).strip() if name else "?"})
    return out


def submissions(contact, cik10):
    time.sleep(PACE)
    r = requests.get(SUB.format(cik10=cik10), headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        return None
    d = r.json()
    blocks = [d["filings"]["recent"]]
    for f in d["filings"].get("files", []):
        time.sleep(PACE)
        rr = requests.get("https://data.sec.gov/submissions/" + f["name"],
                          headers=_ua(contact), timeout=30)
        if rr.status_code == 200:
            blocks.append(rr.json())
    forms = []
    for b in blocks:
        forms += list(zip(b["accessionNumber"], b["form"], b["filingDate"]))
    return {"name": d.get("name"), "forms": forms}


def is_13f_filer(sub):
    hits = [(a, f, dte) for a, f, dte in sub["forms"] if f.startswith("13F")]
    return hits


def _issuer_from_filing(contact, cik10, accession):
    acc_nodash = accession.replace("-", "")
    idx = form4.ARCH.format(cik=cik10.lstrip("0"), acc_nodash=acc_nodash, doc="index.json")
    time.sleep(PACE)
    r = requests.get(idx, headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        return None
    doc = None
    for it in r.json()["directory"]["item"]:
        if it["name"].lower().endswith(".xml"):
            doc = it["name"]
    if not doc:
        return None
    url = form4.ARCH.format(cik=cik10.lstrip("0"), acc_nodash=acc_nodash, doc=doc)
    time.sleep(PACE)
    rr = requests.get(url, headers=_ua(contact), timeout=30)
    if rr.status_code != 200:
        return None
    try:
        p = form4.parse_ownership(rr.text)
    except ET.ParseError:
        return None
    return {"issuer": p.get("issuer"), "symbol": p.get("symbol"),
            "role": p.get("role"), "owner_cik": (p.get("owner_cik") or "").zfill(10)}


def person_issuers(contact, cik10, sub, max_filings=400, probe=4):
    """Aggregate issuers from filings the candidate filed AS the reporting owner.
    Early-exits as an ISSUER (not a person) if the first `probe` filings never
    show owner_cik == the candidate — issuer CIKs also carry Form 4s about them,
    and we must not mislabel an issuer as a reporting person."""
    own = [(a, f, dte) for a, f, dte in sub["forms"] if f in OWNERSHIP_FORMS]
    agg = defaultdict(lambda: {"ticker": None, "issuer": None, "first": None,
                               "last": None, "count": 0, "roles": set()})
    parse_fail = 0
    owner_match = 0
    probed = 0
    for acc, form, fdate in own[:max_filings]:
        info = _issuer_from_filing(contact, cik10, acc)
        probed += 1
        if not info or not info.get("issuer"):
            parse_fail += 1
        elif info.get("owner_cik") == cik10:
            owner_match += 1
            key = info.get("symbol") or info["issuer"]
            a = agg[key]
            a["ticker"] = info.get("symbol") or a["ticker"]
            a["issuer"] = info["issuer"]
            a["first"] = min(a["first"] or fdate, fdate)
            a["last"] = max(a["last"] or fdate, fdate)
            a["count"] += 1
            if info.get("role"):
                a["roles"].add(info["role"])
        # Early-exit: probed enough with zero owner matches => this is an issuer.
        if probed >= probe and owner_match == 0:
            return {"is_person": False, "n_ownership_filings": len(own),
                    "parse_fail": parse_fail, "issuers": {}}
    return {"is_person": owner_match > 0, "n_ownership_filings": len(own),
            "parse_fail": parse_fail,
            "issuers": {k: {**v, "roles": sorted(v["roles"])} for k, v in agg.items()}}


def resolve_and_report(contact, out_path):
    report = {"as_of": None, "thirteen_f": [], "ownership": [],
              "confirm_negative": [], "trump_network_issuers": set()}
    # 13F targets
    for t in THIRTEEN_F_TARGETS:
        cands = efts_entities(contact, t["name"], forms="13F-HR") or \
            efts_entities(contact, t["name"])
        entry = {"target": t, "candidates": []}
        for c in cands[:8]:
            sub = submissions(contact, c["cik"])
            f13 = is_13f_filer(sub) if sub else []
            entry["candidates"].append({
                "cik": c["cik"], "name": c["name"],
                "files_13f": bool(f13), "n_13f": len(f13),
                "latest_13f": (max(x[2] for x in f13) if f13 else None)})
        report["thirteen_f"].append(entry)
    # ownership targets: full-text resolve (Last-First), dedupe, keep only CIKs
    # that actually have ownership (3/4/5) filings = real reporting persons.
    for t in OWNERSHIP_TARGETS:
        cands = {}
        for q in t["queries"]:
            for e in efts_entities(contact, q, forms="4"):
                cands.setdefault(e["cik"], e["name"])
        entry = {"target": t, "persons": [], "non_person_matches": []}
        for cik, name in list(cands.items())[:10]:
            sub = submissions(contact, cik)
            if not sub:
                continue
            n_own = sum(1 for a, f, d in sub["forms"] if f in OWNERSHIP_FORMS)
            if n_own == 0:
                entry["non_person_matches"].append(
                    {"cik": cik, "name": name, "why": "no ownership filings"})
                continue
            pi = person_issuers(contact, cik, sub)
            if not pi.get("is_person"):
                entry["non_person_matches"].append(
                    {"cik": cik, "name": sub.get("name") or name,
                     "why": "issuer entity, not a reporting person"})
                continue
            for iss in pi["issuers"].values():
                if iss.get("ticker"):
                    report["trump_network_issuers"].add(iss["ticker"].upper())
            entry["persons"].append({"cik": cik, "name": sub.get("name") or name, **pi})
        report["ownership"].append(entry)
    # confirm-negative: a real ownership-surface presence requires a candidate
    # whose parsed filings have reporting-owner CIK == the candidate (is_person).
    # Ownership filings alone are insufficient — issuer CIKs also carry Form 4s.
    for t in CONFIRM_NEGATIVE:
        cands = {}
        for q in t["queries"]:
            for e in efts_entities(contact, q, forms="4"):
                cands.setdefault(e["cik"], e["name"])
        checked = []
        resolved_as_person = False
        for cik, name in list(cands.items())[:6]:
            sub = submissions(contact, cik)
            n_own = sum(1 for a, f, d in sub["forms"] if f in OWNERSHIP_FORMS) if sub else 0
            is_person = False
            if n_own and sub:
                is_person = person_issuers(contact, cik, sub, probe=4).get("is_person")
                if is_person:
                    resolved_as_person = True
            checked.append({"cik": cik, "name": name, "ownership_filings": n_own,
                            "is_reporting_person": is_person})
        report["confirm_negative"].append(
            {"target": t, "checked": checked, "resolved_as_person": resolved_as_person})
    report["trump_network_issuers"] = sorted(report["trump_network_issuers"])
    _render(report, out_path)
    # Machine-readable issuer set handed to SM-F4 Step 2 backfill scope.
    import pathlib
    setp = pathlib.Path(out_path).parent / "trump_network_issuers.json"
    setp.write_text(json.dumps({"as_of_run": True,
                                "tickers": report["trump_network_issuers"]}))
    return report


def _render(r, path):
    m = ["# TRUMP_NETWORK_RESOLUTION — smart_money_daemon SM-A1 Phase 1", "",
         "Read-only EDGAR resolution. Identity = EDGAR CIK. No ingest, no registry "
         "writes. Proposed seeds below are for Mando ratification only.", ""]
    m.append("## 13F surface")
    m.append("")
    for e in r["thirteen_f"]:
        t = e["target"]
        m.append("### {} ({}, confidence {})".format(t["name"], t["who"], t["conf"]))
        if not e["candidates"]:
            m.append("- NO EDGAR entity match found.")
        for c in e["candidates"]:
            m.append("- CIK {} {} — files 13F: {} (n={}, latest {})".format(
                c["cik"], c["name"], c["files_13f"], c["n_13f"], c["latest_13f"]))
        m.append("")
    m.append("## Ownership surface (Form 3/4/5 reporting persons)")
    m.append("")
    for e in r["ownership"]:
        t = e["target"]
        m.append("### {}".format(t["who"]))
        if not e["persons"]:
            m.append("- No reporting-person CIK resolved.")
        for p in e["persons"]:
            m.append("- **CIK {} {}** — {} ownership filings, {} parse-fail".format(
                p["cik"], p["name"], p["n_ownership_filings"], p["parse_fail"]))
            for tk, iss in sorted(p["issuers"].items(),
                                  key=lambda kv: -kv[1]["count"]):
                m.append("  - {} ({}) filings={} {}..{} roles={}".format(
                    iss.get("ticker") or "?", iss["issuer"], iss["count"],
                    iss["first"], iss["last"], ",".join(iss["roles"]) or "-"))
        if e.get("non_person_matches"):
            m.append("  - _resolver also matched (not reporting persons): {}_".format(
                "; ".join("CIK {} {} [{}]".format(c["cik"], c["name"], c["why"])
                          for c in e["non_person_matches"])))
        m.append("")
    m.append("## Confirm-negative")
    m.append("")
    for e in r["confirm_negative"]:
        t = e["target"]
        checked = e["checked"]
        persons = [c for c in checked if c.get("is_reporting_person")]
        if e.get("resolved_as_person"):
            m.append("- {} ({}): **PRESENT** — reporting-person match: {}".format(
                t["who"], t["expect"],
                ", ".join("CIK {} {}".format(c["cik"], c["name"]) for c in persons)))
        else:
            noise = [c for c in checked if c["ownership_filings"] > 0]
            m.append("- {} ({}): **UNRESOLVED / ABSENT as expected.** {} name-match "
                     "CIK(s) carry ownership filings but NONE is a reporting person "
                     "(owner CIK != candidate — they are issuer entities or false "
                     "name matches): {}".format(
                         t["who"], t["expect"], len(noise),
                         ", ".join("CIK " + c["cik"] for c in noise) or "none"))
    m.append("")
    m.append("## trump_network issuer set (union of discovered tickers) -> SM-F4 Step 2 scope")
    m.append("")
    m.append(" ".join(r["trump_network_issuers"]) or "(none discovered)")
    m.append("")
    m.append("## PROPOSED registry seeds — NOT applied, for Mando ratification")
    m.append("")
    m.append("- Ownership persons above with a resolved CIK: role `trump_network`.")
    m.append("- 13F candidates that file 13F: role `manager_13f`.")
    m.append("- HYPOTHESIS checks (verify against results, not scope): Trump Jr ~ "
             "UMAC/PSQH/DOMH/PEW; Eric ~ ABTC/mining; DJT trust ~ DJT. Divergence "
             "is a finding.")
    open(path, "w").write("\n".join(m) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-A1 Phase 1 EDGAR network recon")
    ap.add_argument("--out", default=dbmod.artifact_path(
        "TRUMP_NETWORK_RESOLUTION.md", "scans"))
    args = ap.parse_args(argv)
    env = load_env()
    contact = env.get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    import pathlib
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    rep = resolve_and_report(contact, args.out)
    print("[network-recon] 13f_targets={} ownership_targets={} "
          "trump_network_issuers={} -> {}".format(
              len(rep["thirteen_f"]), len(rep["ownership"]),
              len(rep["trump_network_issuers"]), args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
