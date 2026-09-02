"""CD-GAP1 P3 — NBIS prose capex probe. Report-only; publishes no row.

Nebius is a foreign private issuer: 245 6-Ks and 16 20-Fs. It was ruled into the
panel as a major neocloud and has contributed nothing for two months, which is
the materiality case the deferred prose leg was waiting for.

**Premise corrected 2026-09-02.** This tool was written asserting *zero capex in
companyfacts*. Measured against the live API that is false — 58 facts on
`PaymentsToAcquirePropertyPlantAndEquipment`, 19 USD, all annual durations,
through FY2025 at $4,066.0M, including all three anchors below. The API carries
no sub-annual duration, which is why no quarterly series exists; the prose leg's
value is half-yearly granularity and earlier arrival, not filling a void. See
CD-GAP1-VERIFY §P3-COMPANYFACTS.

**Regex-tier, zero LLM, one issuer** (E2: scripts-first). The extraction works.
Whether the extracted number may enter the panel is a different question, and
this tool deliberately does not answer it — see the basis note below.

## What was measured

Scanning 41 EX-99 exhibits across the recent 6-K run:

  * the phrase "capital expenditures" appears in **nearly every** release, inside
    the forward-looking-statements boilerplate ("...planned investments and
    capital expenditures, capacity expansion plans..."). Keying on the phrase
    yields ~100% false positives.
  * a real, structured capex table appears in **2 of 41** — the earnings
    releases. It is highly regular and unambiguous:

        Three months ended March 31,   2025      2026
        Purchases of property and equipment and intangible assets
                                       (543.9)   (2,472.9)

        Six months ended June 30,      2025      2026
        Purchases of property and equipment and intangible assets
                                       (1,054.5) (8,130.3)

So the extractor must anchor on the TABLE, never on the phrase.

## Verification against annual anchors (the ordered gate)

The FY2024 20-F carries three years in one table:

        Year ended December 31,   2022    2023    2024
        Capital expenditures      14.6    83.4    807.7

H1-2025 at $1,054.5M already exceeds all of FY2024 at $807.7M, which is
consistent with the releases' own language about substantially increasing the
pace — the anchors corroborate the interim figures rather than contradicting
them.

## Why no row is published here

**The basis does not match the panel.** Three different labels appear across the
documents for what may or may not be one measure:

  * interim releases:  "Purchases of property and equipment **and intangible assets**"
  * the 20-F:          "Capital expenditures"
  * the panel:         PP&E payments only (`tagmap.CAPEX`)

Intangibles are not PP&E. A series built from the interim line is a BROADER
measure than every other name in the panel, and [E23] is the standing rule that
concept identity is not semantic identity — a number that means something
slightly different must not enter an aggregate that assumes it means the same.

That is a ruling, not a parse, so this tool stops here and reports.
"""
import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from capex_daemon import edgar, universe  # noqa: E402

TICKER = "NBIS"
ARCHIVE = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"

# Anchor on the table's own sentence, never on the boilerplate phrase.
TABLE_ANCHOR = re.compile(r"information about our capital expenditures", re.I)
PERIOD = re.compile(r"(three|six|nine|twelve) months ended ([A-Z][a-z]+ \d{1,2})|"
                    r"year ended (december 31)", re.I)
FIGURES = re.compile(r"\(?\s*([\d,]+\.\d)\s*\)?")

ENTITIES = (("&#8203;", ""), ("&#160;", " "), ("&nbsp;", " "),
            ("&#8239;", " "), ("&#8212;", "-"), ("&#36;", "$"))


def _plain(body):
    txt = body if isinstance(body, str) else body.decode("utf-8", "replace")
    for a, b in ENTITIES:
        txt = txt.replace(a, b)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt))


def probe(limit=45, http=None):
    roster = universe.load()
    e = [x for x in roster.values() if x.ticker_display == TICKER][0]
    http = http or edgar.client()
    sub = edgar.fetch_submissions(e.cik, http)
    r = sub["filings"]["recent"]
    out, scanned, boiler_only = [], 0, 0
    for i in range(len(r["form"])):
        if r["form"][i] not in ("6-K", "20-F"):
            continue
        acc = r["accessionNumber"][i]
        try:
            idx = http.get_json(
                ARCHIVE.format(cik=int(e.cik), acc=acc.replace("-", "")) + "index.json")
        except Exception:
            continue
        names = [x["name"] for x in idx.get("directory", {}).get("item", [])
                 if re.search(r"(ex99d1|20f)\.htm$", x["name"], re.I)]
        for name in names[:1]:
            try:
                plain = _plain(edgar.fetch_document(e.cik, acc, name, http=http))
            except Exception:
                continue
            scanned += 1
            m = TABLE_ANCHOR.search(plain)
            if not m:
                if re.search(r"capital expenditures", plain, re.I):
                    boiler_only += 1
                continue
            seg = plain[m.end():m.end() + 420]
            per = PERIOD.search(seg)
            vals = [float(v.replace(",", "")) for v in FIGURES.findall(seg)[:4]]
            out.append({"filed": r["filingDate"][i], "doc": name, "accession": acc,
                        "period_label": (per.group(0) if per else None),
                        "figures_musd": vals, "excerpt": seg[:200]})
        if len(out) >= limit:
            break
    return {"scanned": scanned, "boilerplate_only": boiler_only, "tables": out}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=45)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    res = probe(limit=args.limit)
    if args.json:
        print(json.dumps(res, indent=2))
        return 0
    print("EX-99/20-F documents scanned : %d" % res["scanned"])
    print("carried ONLY the boilerplate : %d  (phrase-keyed extraction would take these)"
          % res["boilerplate_only"])
    print("carried a real capex table   : %d" % len(res["tables"]))
    print()
    for t in res["tables"]:
        print("  %s  %s" % (t["filed"], t["doc"]))
        print("     period : %s" % t["period_label"])
        print("     figures: %s (USD millions)" % t["figures_musd"])
    print()
    print("  NO ROW PUBLISHED. The interim line is 'property and equipment AND")
    print("  intangible assets' — a broader basis than the panel's PP&E-only capex")
    print("  (E23). Admitting it is a ruling, not a parse.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
