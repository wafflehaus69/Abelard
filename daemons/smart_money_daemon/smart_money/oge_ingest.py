"""OGE Form 278e (executive-branch public financial disclosure) holdings ingest.

USE RESTRICTION — READ recon/OGE_278E_SOURCE_VERDICT.md BEFORE TOUCHING THIS.
Unlike every other source in this daemon (EDGAR Form 4/13F, STOCK Act congressional
filings — all unrestricted), OGE 278e reports carry a STATUTORY use restriction under the
Ethics in Government Act, 5 U.S.C. app. Sec 105(c): it is unlawful to obtain or use a
report for any unlawful purpose, for any COMMERCIAL purpose (other than by news media for
dissemination to the general public), to establish a credit rating, or to solicit money.
The Attorney General may bring a civil action with a penalty up to $11,000.

Every row this module writes therefore carries `use_restriction` NOT NULL, so a row
physically cannot exist in the DB without its restriction attached, and the tag travels
into every view and export. The tag is PROVENANCE, not a legal cure — what actually keeps
this lawful is the use remaining non-commercial. This data is deliberately kept in its own
table that the scan/alert/enqueue path does not read, so it cannot leak into a signal
product.

Reports are fetched from the OGE Presidential Nominee and Appointee index. Level 1/2
Executive Schedule reports are served directly as PDFs (no OGE Form 201 request); items
annotated "(OGE Form 201)" are NOT fetched here — those require an attributable signed
request that only a human filer of that request may make.
"""
import argparse
import re
import sys
import time
import urllib.parse

import requests

from . import db as dbmod
from .house_fd_ingest import _parse_band

# The view is a COLLAPSED category list: filer rows carry no document links until that
# row is expanded. ExpandView (expand-all) is server-capped around 425KB and silently
# truncates mid-alphabet regardless of Count, so it is NOT usable — instead read the
# collapsed index, find the filer's own Expand=N ordinal, and expand just that row.
# The view name must be "PAS+Index"; the %20 spelling 301s away to a landing page.
INDEX = ("https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index"
         "?OpenView&Start=1&Count=2000")
ROW = INDEX + "&Expand={n}"
HOST = "https://extapps2.oge.gov"
RESTRICTION = "NOT TO BE USED FOR COMMERCIAL PURPOSES"
PACE = 0.5

# Direct-download report links are single-quoted in the Domino HTML and end in $FILE/<pdf>.
_DOC = re.compile(r"<a href='(/201/Presiden\.nsf/[^']*?/\$FILE/[^']+\.pdf)'[^>]*>(.*?)</a>",
                  re.S | re.I)
_ROW_START = re.compile(r"^(\d+(?:\.\d+)*)\s+(.*)$")
_TICKER = re.compile(r"\(([A-Z]{1,5})\)")
_EIF = re.compile(r"\s(Yes|No|N/A)\s")
# Income-type words that terminate a value band, so a wrapped band is not glued to them.
_INCOME_HINT = re.compile(
    r"\b(None \(or less than|Dividends|Capital Gains|Interest|Rent|Salary|Honorarium|"
    r"Director Fees|Consulting fees|Partnership|Excepted|Royalt)", re.I)


def _ua(contact):
    return {"User-Agent": "AbelardSmartMoney/0.1 (+{})".format(contact)}


def fetch_index(contact, timeout=60):
    r = requests.get(INDEX, headers=_ua(contact), timeout=timeout)
    r.raise_for_status()
    return r.text


def find_expand_ordinal(index_html, filer_substr):
    """The filer row's own Expand=N ordinal, read from its 'Show details for <filer>'
    anchor. Positional in the view, so it is resolved fresh every run, never cached."""
    m = re.search(r"Expand=(\d+)[^>]*>\s*<img[^>]*alt=\"Show details for "
                  + re.escape(filer_substr), index_html)
    return int(m.group(1)) if m else None


def find_reports(row_html, filer_substr):
    """[(url, label)] of DIRECTLY downloadable PDFs for a filer, from that filer's
    EXPANDED row. Items requiring an OGE Form 201 request carry no $FILE pdf link and are
    therefore never returned — those need an attributable signed request by a human."""
    i = row_html.find('alt="Show details for ' + filer_substr)
    if i < 0:
        i = row_html.find(filer_substr)
    if i < 0:
        return []
    # the block runs to the next filer category row
    j = row_html.find('alt="Show details for ', i + 30)
    seg = row_html[i:j if j > i else i + 8000]
    out = []
    for m in _DOC.finditer(seg):
        label = re.sub(r"\s+", " ", re.sub("<[^>]+>", "", m.group(2))).strip()
        out.append((HOST + urllib.parse.quote(m.group(1), safe="/:$"), label))
    return out


def fetch_pdf(url, contact, timeout=90):
    time.sleep(PACE)
    r = requests.get(url, headers=_ua(contact), timeout=timeout)
    r.raise_for_status()
    return r.content


def _split_value_income(tail):
    """A row's tail is '<value band> <income type> <income band>' with the bands possibly
    wrapped. Return (value_text, income_type, income_text) on a best-effort split; None
    parts are reported as None, never guessed."""
    m = _INCOME_HINT.search(tail)
    if not m:
        return tail.strip() or None, None, None
    value = tail[:m.start()].strip()
    rest = tail[m.start():].strip()
    # income amount is a trailing band or "None (or less than $201)"
    im = re.search(r"(\$[\d,]+\s*-\s*\$[\d,]+|None \(or less than \$[\d,]+\)|"
                   r"Over \$[\d,]+)\s*$", rest)
    if im and im.start() > 0:
        return value or None, rest[:im.start()].strip() or None, im.group(1)
    return value or None, rest or None, None


def parse_278e(pdf_bytes):
    """[{line_no, description, ticker, eif, value_lo/hi, income_type, income_lo/hi}] from
    an OGE 278e. Rows accumulate across wrapped lines; a row starts at a leading line
    number. Endnote/instruction pages are skipped (they have no numbered asset rows)."""
    import io

    import pdfplumber
    recs = []
    cur = None
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if "DESCRIPTION" not in text and "EIF" not in text:
                continue
            for raw in text.split("\n"):
                line = raw.rstrip()
                if not line or line.startswith("#") or "Page " in line[-12:]:
                    continue
                m = _ROW_START.match(line)
                if m:
                    if cur:
                        recs.append(cur)
                    cur = {"line_no": m.group(1), "parts": [m.group(2)]}
                elif cur:
                    cur["parts"].append(line.strip())
    if cur:
        recs.append(cur)
    out = []
    for r in recs:
        blob = " ".join(r["parts"])
        blob = re.sub(r"\s+", " ", blob).strip()
        eifm = _EIF.search(" " + blob + " ")
        eif = eifm.group(1) if eifm else None
        if eifm:
            desc = blob[:eifm.start()].strip()
            tail = blob[eifm.end() - 1:].strip()
        else:
            desc, tail = blob, ""
        vtxt, itype, itxt = _split_value_income(tail)
        vlo, vhi = _parse_band(vtxt or "")
        ilo, ihi = _parse_band(itxt or "")
        tk = _TICKER.findall(desc)
        out.append({"line_no": r["line_no"], "description": desc[:300],
                    "ticker": tk[-1] if tk else None, "eif": eif,
                    "value_lo": vlo, "value_hi": vhi,
                    "income_type": (itype or None) and itype[:60],
                    "income_lo": ilo, "income_hi": ihi})
    return out


def ingest(con, doc_id, filer, report_type, filed_date, rows, source_url):
    """Land rows. `use_restriction` is NOT NULL in the schema, so the tag is attached to
    EVERY row by construction — it cannot be omitted."""
    n = 0
    for r in rows:
        con.execute(
            "INSERT OR REPLACE INTO oge_holdings(doc_id, filer, report_type, filed_date, "
            "line_no, description, ticker, eif, value_lo, value_hi, income_type, "
            "income_lo, income_hi, use_restriction, source_url, ingested_at_unix) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (doc_id, filer, report_type, filed_date, r["line_no"], r["description"],
             r["ticker"], r["eif"], r["value_lo"], r["value_hi"], r["income_type"],
             r["income_lo"], r["income_hi"], RESTRICTION, source_url, int(time.time())))
        n += 1
    con.commit()
    return n


def main(argv=None):
    from .efd_ingest import load_env
    ap = argparse.ArgumentParser(description="OGE 278e holdings ingest")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--filer", required=True, help="surname prefix as listed, e.g. 'Warsh, Kevin'")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT") or "smartmoney@example.com"
    print("[oge] {}".format(RESTRICTION))
    index_html = fetch_index(contact)
    n = find_expand_ordinal(index_html, args.filer)
    if n is None:
        print("[oge] filer {!r} not listed in the PAS index".format(args.filer))
        return 1
    time.sleep(PACE)
    row_html = requests.get(ROW.format(n=n), headers=_ua(contact), timeout=90).text
    reports = find_reports(row_html, args.filer)
    if not reports:
        print("[oge] no directly downloadable report for {!r} (items needing an OGE Form "
              "201 are never auto-fetched)".format(args.filer))
        return 1
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    total = 0
    try:
        for url, label in reports:
            if "278" not in label:
                print("[oge] skip non-278 document: {}".format(label))
                continue
            fm = re.search(r"(\d{2}/\d{2}/\d{4})", label)
            pdf = fetch_pdf(url, contact)
            rows = parse_278e(pdf)
            doc = url.rsplit("/", 2)[-2] if "/" in url else url
            n = ingest(con, doc, args.filer, label, fm.group(1) if fm else None,
                       rows, url)
            total += n
            print("[oge] {} -> {} rows ({} with a value band, {} with a ticker)".format(
                label, n, sum(1 for r in rows if r["value_lo"] is not None),
                sum(1 for r in rows if r["ticker"])))
    finally:
        con.close()
    print("[oge] DONE {} rows, every row tagged {!r}".format(total, RESTRICTION))
    return 0


if __name__ == "__main__":
    sys.exit(main())
