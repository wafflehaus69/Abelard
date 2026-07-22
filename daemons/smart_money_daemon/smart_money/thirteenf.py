"""13F leg (SM-4 STEP 3 Leg C). Fetch a manager's latest 13F-HR, parse the
information table, diff against the stored baseline, emit positioning events for
new positions, exits, >2x value changes, and put/call directionality flips.
Reuses the certified EDGAR path and the net-directionality machinery.
"""
import json
import re
import time
import xml.etree.ElementTree as ET

import requests

UA_TMPL = "Abelard-SmartMoney mdiba personal research {}"
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik10}.json"
ARCH = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{doc}"
PACE = 0.15


def _ua(contact):
    return {"User-Agent": UA_TMPL.format(contact)}


def latest_13f(cik, contact):
    """Return dict {accession, period, filed, info_doc} for the newest 13F-HR,
    or None. cik is the numeric string."""
    cik10 = str(int(cik)).zfill(10)
    time.sleep(PACE)
    r = requests.get(SUBMISSIONS.format(cik10=cik10), headers=_ua(contact), timeout=30)
    if r.status_code != 200:
        raise RuntimeError("submissions HTTP {}".format(r.status_code))
    d = r.json()["filings"]["recent"]
    for form, acc, date, prim, rdate in zip(
        d["form"], d["accessionNumber"], d["filingDate"],
        d["primaryDocument"], d.get("reportDate", d["filingDate"]),
    ):
        if form == "13F-HR":
            return {"accession": acc, "period": rdate, "filed": date}
    return None


def fetch_info_table(cik, accession, contact):
    acc_nodash = accession.replace("-", "")
    idx_url = ARCH.format(cik=int(cik), acc_nodash=acc_nodash, doc="index.json")
    time.sleep(PACE)
    idx = requests.get(idx_url, headers=_ua(contact), timeout=30).json()
    info_doc = None
    for it in idx["directory"]["item"]:
        n = it["name"].lower()
        if n.endswith(".xml") and "primary_doc" not in n:
            info_doc = it["name"]
    if not info_doc:
        raise RuntimeError("no info table xml in {}".format(accession))
    url = ARCH.format(cik=int(cik), acc_nodash=acc_nodash, doc=info_doc)
    time.sleep(PACE)
    raw = requests.get(url, headers=_ua(contact), timeout=30).text
    return parse_holdings(raw)


def parse_holdings(raw_xml):
    """cusip -> {issuer, value, shares, putCall}. Aggregates rows per cusip
    keeping separate put/call/long buckets folded into net fields."""
    raw = re.sub(r'xmlns="[^"]+"', "", raw_xml, count=1)
    root = ET.fromstring(raw)
    holdings = {}
    for it in root.iter():
        if it.tag.split("}")[-1] != "infoTable":
            continue

        def g(tag):
            for e in it.iter():
                if e.tag.split("}")[-1] == tag:
                    return (e.text or "").strip()
            return ""

        cusip = g("cusip")
        pc = g("putCall")
        val = int(g("value") or 0)
        sh = int(g("sshPrnamt") or 0)
        h = holdings.setdefault(
            cusip,
            {"issuer": g("nameOfIssuer"), "value": 0, "shares": 0,
             "call_val": 0, "put_val": 0},
        )
        if pc == "Put":
            h["put_val"] += val
        elif pc == "Call":
            h["call_val"] += val
        else:
            h["value"] += val
            h["shares"] += sh
    for h in holdings.values():
        h["net_opt"] = h["call_val"] - h["put_val"]
    return holdings


def diff(baseline, new):
    """Yield (cusip, issuer, kind, detail) diff lines."""
    bk, nk = set(baseline), set(new)
    for c in sorted(nk - bk):
        yield c, new[c]["issuer"], "new_position", {"value": new[c]["value"]}
    for c in sorted(bk - nk):
        yield c, baseline[c]["issuer"], "exit", {"was_value": baseline[c]["value"]}
    for c in sorted(bk & nk):
        b, n = baseline[c], new[c]
        if b["value"] > 0 and n["value"] >= 2 * b["value"]:
            yield c, n["issuer"], "value_2x_up", {"from": b["value"], "to": n["value"]}
        elif n["value"] > 0 and b["value"] >= 2 * n["value"]:
            yield c, n["issuer"], "value_2x_down", {"from": b["value"], "to": n["value"]}
        if (b["net_opt"] >= 0) != (n["net_opt"] >= 0) and (b["net_opt"] or n["net_opt"]):
            yield c, n["issuer"], "directionality_flip", {
                "from_net_opt": b["net_opt"], "to_net_opt": n["net_opt"]}


def store_baseline(con, cik, meta, holdings):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_baseline VALUES (?,?,?,?,?,?)",
        (str(int(cik)), meta["accession"], meta["period"], meta["filed"],
         json.dumps(holdings), int(time.time())),
    )
    con.commit()


def get_baseline(con, cik):
    r = con.execute(
        "SELECT accession, period, filed_date, holdings_json FROM "
        "thirteenf_baseline WHERE cik=?", (str(int(cik)),)
    ).fetchone()
    if not r:
        return None
    return {"accession": r[0], "period": r[1], "filed": r[2],
            "holdings": json.loads(r[3])}


def main(argv=None):
    """Seed a manager's latest 13F-HR as the standing baseline."""
    import argparse
    import sys

    from . import db as dbmod
    from .efd_ingest import load_env

    ap = argparse.ArgumentParser(description="Seed 13F baseline for a CIK")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--cik", required=True)
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    latest = latest_13f(args.cik, contact)
    if not latest:
        print("no 13F-HR found for CIK {}".format(args.cik), file=sys.stderr)
        return 1
    holdings = fetch_info_table(args.cik, latest["accession"], contact)
    store_baseline(con, args.cik, latest, holdings)
    print("[13f-seed] CIK {} baseline={} period={} holdings={}".format(
        args.cik, latest["accession"], latest["period"], len(holdings)))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
