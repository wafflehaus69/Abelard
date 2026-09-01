import re, sys
sys.path.insert(0, ".")
from capex_daemon import edgar, prose, universe
roster = universe.load(); http = edgar.client()
e = [x for x in roster.values() if x.ticker_display == "NBIS"][0]
spec = prose.source_for(e.cik)
print("source:", spec["ticker"], "basis:", spec["basis"])
sub = edgar.fetch_submissions(e.cik, http); r = sub["filings"]["recent"]
base = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/"
rows, prov = [], []
for i in range(len(r["form"])):
    if r["form"][i] not in spec["forms"]:
        continue
    acc = r["accessionNumber"][i]
    try:
        idx = http.get_json(base.format(cik=int(e.cik), acc=acc.replace("-", "")) + "index.json")
    except Exception:
        continue
    names = [x["name"] for x in idx.get("directory", {}).get("item", [])
             if re.search(spec["exhibit_re"], x["name"], re.I)]
    for name in names[:1]:
        try:
            plain = prose.plain_text(edgar.fetch_document(e.cik, acc, name, http=http))
        except Exception:
            continue
        got = prose.parse_table(plain)
        if got:
            rows += got
            prov.append((r["filingDate"][i], name, got))
    if len(prov) >= 6:
        break
print()
for fd, name, got in prov:
    print("  %s %-34s %s" % (fd, name[:34], got))
q = prose.discrete_from_cumulative(rows)
print()
print("discrete calendar quarters derived:")
for k in sorted(q):
    print("   %s  $%,.1fM" .replace(",", "") % (k, q[k] / 1e6))
print()
print("ANCHOR CHECK vs the FY2024 20-F:")
for y, v in sorted(spec["anchors"].items()):
    print("   FY%s reported %.1fM" % (y, v))
