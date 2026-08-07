"""SM-P1b House annual Financial Disclosure (FD) holdings ingest.

The annual FD report's Schedule A: Assets is the HOLDINGS snapshot (band-valued) that
periodic transaction reports (PTRs) do not give. This lands per-asset rows into
congress_holdings for the reported-portfolios / who-holds-what views. Scripts-only, no
LLM. Positional column parsing (per-page header anchors -> column bounds -> record
accumulation across wrapped band lines) mirrors house_ingest's PTR parser. Fail-loud:
a text PDF with no recognizable Schedule A header is status unparsed_layout, sampled to
disk, counted, never guessed. Scanned (no text layer) = paper. Resume-safe by DocID via
congress_fd_seen; a rerun refetches nothing (PDFs disk-cached) and converges.

Tickers come from the filing itself (parens after the asset name) — no CUSIP mapping
needed; assets with no ticker (real property, LPs, funds) are kept with ticker NULL.
"""
import argparse
import io
import pathlib
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile

import requests

from . import db as dbmod
from .efd_ingest import load_env

# pdfplumber is imported lazily inside the two functions that open PDFs, so the pure
# parsing helpers (_parse_band / _finalize / _find_assets_header ...) import and unit-test
# without the heavy dependency present.

RAW_DIR_DEFAULT = "data/raw/house_fd"
UNPARSED_DIR = "data/raw/house_fd_unparsed"
ZIP_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
PDF_URLS = (
    "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}/{doc}.pdf",
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc}.pdf",
)
UA_TMPL = "AbelardSmartMoney/0.1 (+{})"
PACE_SECONDS = 0.5
# 'O' = original annual report (sitting members), 'A' = amendment, 'C' = candidate.
ANNUAL_TYPES = ("O", "A")

# Retirement-plan codes, stripped from an asset name BEFORE symbol extraction.
# A parenthetical belonging to a plan code is not a security:
# Retirement-plan codes, stripped from an asset name BEFORE symbol extraction.
# A parenthetical belonging to a plan code is not a security:
# "401(k)" is a retirement PLAN, not Kellogg. This extractor takes the LAST symbol-shaped
# parenthetical, so an account suffix was overwriting the real symbol -- "Vanguard Mid-Cap
# Index Fund Admiral Shares (VIMAX) ... Vanguard - 401(K)" resolved to K. Measured: 884 of
# 900 "K" rows were 401(k) labels and only 16 were genuinely Kellogg/Kellanova, and the
# phantom carried 45-49 holders a year into breadth. Also covers 403(b) and 457(b).
_TICKER_RE = re.compile(r"\(([A-Za-z0-9.\-]{1,10})\)")
# House Schedule A asset-type codes, from the FD instruction booklet. This is a CLOSED
# vocabulary and validating against it matters: `_TYPE_RE` used to accept ANY 2-3 letter
# bracketed token, so ETF tickers written in brackets ("[QQQ]", "[GLD]", "[VOO]") were
# stored as asset TYPES. That both invented 27 bogus type codes and pushed those rows out
# of the ST/OP/EF denominator, so they vanished from capture measurement entirely.
_FD_TYPES = frozenset((
    "4K", "5C", "5F", "5P", "AB", "BA", "BO", "CO", "CS", "CT", "DB", "DC", "DO", "DON",
    "DS", "EF", "EQ", "ET", "FA", "FN", "GS", "HE", "IH", "IP", "IS", "MA", "MF", "MO",
    "OI", "OL", "OP", "OT", "PE", "PM", "PS", "RE", "RF", "RP", "RS", "SA", "ST", "TR",
    "VA", "VI", "WU", "DES", "DFP"))
# Alphanumeric because real codes include 4K/5C/5F/5P; safe to widen ONLY because
# every match is now validated against _FD_TYPES below.
_TYPE_RE = re.compile(r"\[([A-Za-z0-9]{2,3})\]")
# Filers also write tickers in SQUARE brackets ("ARK INNOVATION [ARKK]"), which the
# parens-only ticker rule missed. Anything bracketed that is not a known type code and not
# a footnote number is treated as a symbol candidate.
# Retirement-plan codes: 401(k), 403(b), 457(b), 401(a), and the spaced variants
# filers also type. Stripped from an asset name BEFORE symbol extraction, because
# this extractor takes the LAST symbol-shaped parenthetical and a trailing plan code
# was beating the real symbol. Measured: 884 of 900 "K" rows were 401(k) labels, only
# 16 genuinely Kellogg, and the phantom carried 45-49 holders a year into breadth.
# The rule is DELIBERATELY narrow. A blunter version - reject any parenthetical
# preceded by a digit - looks equivalent and is not: it also strips "Vanguard Target
# Retirement 2040 (VFORX)", and in a dry run it cleared SPY, QQQ, VFORX, RERGX and
# ~1,800 other correct rows. Only the actual plan codes come out.
PLAN_CODE = re.compile(r"\b(40[1239]|457)\s*\(\s*[kab]\s*\)", re.I)
_BRACKET_TICKER_RE = re.compile(r"\[([A-Za-z]{1,5})\]")
# Parenthesised tokens that look like symbols but are exchanges, structure labels, or the
# ACCOUNT an asset sits in. One denylist for both chambers - senate_fd_ingest imports it
# from here, the direction the dependency already runs.
NOT_TICKER = frozenset((
    "NYSE", "AMEX", "OTC", "ARCA", "BATS", "LSE", "TSX", "NASD",
    "ADR", "ADS", "ORD", "LLC", "LP", "REIT", "ETF",
    "INC", "CORP", "CO", "USD", "EUR", "GBP", "JPY", "COM",
    # A filer writing "(IRA)" or "(ROLLOVER)" is labelling the account, not naming a
    # security. Measured live: IRA 132 rows, SPOUSE 36, ROLLOVER 35, SEP 20 - every one
    # an account label, none a ticker.
    "IRA", "SEP", "SIMPLE", "ROTH", "ROLLOVER", "SPOUSE", "JOINT", "TRUST",
    "ESTATE", "EST", "CUSTODIAL", "UTMA", "UGMA", "HSA", "PENSION", "ANNUITY",
    "BROKERAGE", "SAVINGS", "CHECKING", "MONEY", "VESTED", "DEFERRED"))
# Words that are BOTH a plausible account/structure label AND a real US symbol. Checked
# against our own corpora: ETN is Eaton Corp plc (97 Form 4 rows), CASH is Pathward
# Financial, FUND is Sprott Focus Trust, NA is VineBrook Homes Trust, ASX is ASE
# Technology. Denying these outright cost 34 legitimate Eaton rows in the first cut of
# the repair. They are NOT denied at extraction - the asset-type gate already keeps a
# bank-deposit row from minting a symbol - and the repair reports them as a residue for
# a human rather than deciding on their behalf.
AMBIGUOUS_LABELS = frozenset(("ASX", "CASH", "ETN", "FUND", "NA"))
_NOT_TICKER = NOT_TICKER          # internal alias, kept so call sites read the same
_BAND_RE = re.compile(r"\$([\d,]+)\s*-\s*\$([\d,]+)")
_OVER_RE = re.compile(r"[Oo]ver \$([\d,]+)")
# Standard FD floor band, e.g. "None (or less than $1,001)" -> ($0, $1,001).
_LESS_RE = re.compile(r"less than \$([\d,]+)", re.I)
# Core Schedule A header anchors. "Tx." is OPTIONAL — one template family omits the
# "Tx. > $1,000?" column entirely (header ends "... Income Type(s) Income"); requiring
# it dropped ~19% of filings to unparsed_layout. When absent, Income is the last column.
_ASSETS_CORE = ("Asset", "Owner", "Value", "Income")
_ASSETS_HEADER = _ASSETS_CORE + ("Tx.",)

_last_call = 0.0


class FDIngestError(RuntimeError):
    pass


def _pace():
    global _last_call
    wait = PACE_SECONDS - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def _get(url, ua):
    _pace()
    return requests.get(url, headers={"User-Agent": ua}, timeout=120)


def fetch_year_index(year, raw_dir, ua, max_age_days=None):
    """Annual-report (O/A) index entries for a year, or None if the zip is absent.

    `max_age_days` forces a re-download when the cached zip is older than that. REQUIRED
    for any scheduled use: the cache-if-exists rule is right for a closed historical year
    but makes the CURRENT year permanently blind — new annual filings land in that same
    zip all cycle, and a nightly leg reading a month-old cache would never see them."""
    zpath = raw_dir / "{}FD.zip".format(year)
    stale = False
    if max_age_days is not None and zpath.exists():
        age = time.time() - zpath.stat().st_mtime
        stale = age > max_age_days * 86400
    if stale:
        try:
            zpath.unlink()
        except OSError:
            stale = False          # keep the cached copy rather than lose the index
    if not zpath.exists():
        r = _get(ZIP_URL.format(year=year), ua)
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise FDIngestError("{}FD.zip HTTP {}".format(year, r.status_code))
        zpath.write_bytes(r.content)
    with zipfile.ZipFile(zpath) as z:
        xmls = [n for n in z.namelist() if n.lower().endswith(".xml")]
        if not xmls:
            raise FDIngestError("{}FD.zip has no XML index".format(year))
        root = ET.fromstring(z.read(xmls[0]))
    out = []
    for m in root:
        f = {e.tag: (e.text or "").strip() for e in m}
        if f.get("FilingType") in ANNUAL_TYPES:
            out.append(f)
    return out


def fetch_pdf(year, doc_id, raw_dir, ua):
    pdir = raw_dir / "pdfs" / str(year)
    pdir.mkdir(parents=True, exist_ok=True)
    path = pdir / "{}.pdf".format(doc_id)
    if path.exists():
        return path
    for tmpl in PDF_URLS:
        r = _get(tmpl.format(year=year, doc=doc_id), ua)
        if r.status_code == 200 and r.content[:5] == b"%PDF-":
            path.write_bytes(r.content)
            return path
    return None


def _lines(page):
    grouped = {}
    for w in page.extract_words():
        grouped.setdefault(round(w["top"] / 4), []).append(
            (w["x0"], w["text"].replace("\x00", "")))
    return [sorted(grouped[k]) for k in sorted(grouped)]


def _find_assets_header(line):
    texts = [t for _, t in line]
    if not all(h in texts for h in _ASSETS_CORE):   # Tx. optional
        return None
    anchors = {}
    for x, t in line:
        if t in _ASSETS_HEADER and t not in anchors:
            anchors[t] = x
    return anchors


def _col_bounds(anchors):
    cols = sorted((x, name) for name, x in anchors.items())
    return [(name, x, cols[i + 1][0] - 2 if i + 1 < len(cols) else 10000)
            for i, (x, name) in enumerate(cols)]


def _bucket(line, bounds):
    b = {name: [] for name, _, _ in bounds}
    for x, t in line:
        for name, lo, hi in bounds:
            if lo - 4 <= x < hi:
                b[name].append(t)
                break
    return b


def _num(s):
    return int(s.replace(",", ""))


def _cov_year(filing, zip_year):
    """Calendar year a House annual COVERS. The index carries it explicitly as `Year`;
    the zip year is only a fallback (they agree in every observed zip)."""
    y = (filing or {}).get("Year")
    try:
        return int(str(y).strip())
    except (TypeError, ValueError):
        return zip_year


def _iso_mdy(s):
    """'1/14/2026' -> '2026-01-14'. Unparseable stays None, never guessed."""
    m = re.match(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})", s or "")
    if not m:
        return None
    return "{}-{:02d}-{:02d}".format(m.group(3), int(m.group(1)), int(m.group(2)))


_BAND_OPEN_RE = re.compile(r"\$[\d,]+\s*-\s*$")


def _band_complete(tokens):
    """False when a value cell ends mid-band ('$500,001 -'), meaning the second amount
    wrapped onto the following line. Anything else — a full band, 'None',
    'Over $X' — is complete."""
    j = " ".join(tokens).strip()
    return not bool(_BAND_OPEN_RE.search(j))


def _parse_band(text):
    m = _BAND_RE.search(text)
    if m:
        return _num(m.group(1)), _num(m.group(2))
    m = _OVER_RE.search(text)
    if m:
        return _num(m.group(1)), None
    m = _LESS_RE.search(text)
    if m:
        return 0, _num(m.group(1))
    return None, None


def _finalize(rec):
    name = " ".join(rec["asset"]).strip()
    # Strip plan codes FIRST. This extractor takes the LAST symbol-shaped parenthetical,
    # so "401(K)" was beating the real symbol. A blunter rule -- reject any parenthetical
    # preceded by a digit -- looked equivalent and was not: it also killed
    # "Vanguard Target Retirement 2040 (VFORX)", clearing SPY, QQQ and 1,800 other good
    # rows in a dry run. Only the actual plan codes are removed.
    ticker = None
    name_for_ticker = PLAN_CODE.sub(" ", name)
    for cand in _TICKER_RE.findall(name_for_ticker):   # last symbol-shaped parens wins
        if (re.fullmatch(r"[A-Za-z]{1,5}[A-Za-z.\-]{0,3}", cand)
                and cand.upper() not in _NOT_TICKER):
            ticker = cand.upper()
    if not ticker:                               # "ARK INNOVATION [ARKK]"
        for cand in _BRACKET_TICKER_RE.findall(name_for_ticker):
            if (cand.upper() not in _FD_TYPES     # never mistake a type code for a symbol
                    and cand.upper() not in _NOT_TICKER):
                ticker = cand.upper()
    # Only a KNOWN code counts as the asset type; a bracketed ETF symbol is not a type.
    tm = None
    for m in _TYPE_RE.finditer(name):
        if m.group(1).upper() in _FD_TYPES:
            tm = m
    lo, hi = _parse_band(" ".join(rec["value"]))
    clean = _TYPE_RE.sub("", name).strip(" -")
    return {"asset_name": clean[:200] or name[:200], "ticker": ticker,
            "asset_type": tm.group(1).upper() if tm else None,
            "owner": (rec["owner"].strip() or "Self")[:8],
            "value_lo": lo, "value_hi": hi,
            "income_type": (" ".join(rec["income"]).strip() or None)}


def fd_header_signature(path):
    """Layout fingerprint for clustering: the rounded x-positions of the Schedule A
    header anchors, or None when no Schedule A header is found (a distinct no-header
    variant). Filings sharing a signature share a column geometry, so failures cluster
    by signature rather than by member."""
    import pdfplumber
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for line in _lines(page):
                anchors = _find_assets_header(line)
                if anchors:
                    return tuple(sorted((name, int(round(x / 10) * 10))
                                        for name, x in anchors.items()))
    return None


def parse_fd_assets(path):
    """(rows, status). status ok|paper|unparsed_layout. rows = per-asset dicts."""
    import pdfplumber
    with pdfplumber.open(path) as pdf:
        if sum(len(p.extract_words()) for p in pdf.pages) == 0:
            return [], "paper"
        records, current, header_seen, awaiting = [], None, False, False
        pending = {"asset": [], "owner": [], "income": []}
        for page in pdf.pages:
            bounds = None
            for line in _lines(page):
                if bounds is None:
                    anchors = _find_assets_header(line)
                    if anchors:
                        header_seen, bounds, current = True, _col_bounds(anchors), None
                        awaiting = False
                        pending = {"asset": [], "owner": [], "income": []}
                    continue
                joined = " ".join(t for _, t in line)
                if joined.startswith("* For the complete list") or \
                        re.match(r"^(Schedule [B-Z]\b|S [B-Z]:)", joined):
                    bounds, current, awaiting = None, None, False
                    pending = {"asset": [], "owner": [], "income": []}
                    continue
                bkt = _bucket(line, bounds)
                asset, val = bkt["Asset"], bkt["Value"]
                first = asset[0] if asset else ""
                if first in ("L:", "D:", "L", "D"):
                    continue
                # THE VALUE LINE CLOSES A RECORD; the asset name accumulates BEFORE it.
                #
                # The previous rule required name and value on the SAME line, which is
                # wrong for the dominant House template: the asset name (and its ticker
                # and [TYPE] code) sit on their own lines, and the value lands on the
                # following account line:
                #     A=UnitedHealth Group Incorporated Common Stock | V=
                #     A=(UNH) [ST]                                   | V=
                #     A=Schwab One =>                                | V=$15,001 - $50,000
                # Under the old rule the name was appended to the PREVIOUS record and the
                # real record opened on "Schwab One =>" — which put UNH's ticker on a bank
                # balance and gave it that row's $1,000,001-$5,000,000 band instead of its
                # own $15,001-$50,000. A 100x error on a holding claim.
                #
                # A value cell of the literal word "None" still closes a record: the filer
                # reported the asset with no value, which is a real disclosure state.
                # A WRAPPED band splits across two lines, and its second half lands on the
                # line that also carries the NEXT asset's name:
                #     A=Bitwise Solana Staking ETF (BSOL) [EF] | V=
                #     A=Schwab One =>                          | V=$500,001 -
                #     A=Cash account [BA]                      | V=$1,000,000
                # BSOL is $500,001-$1,000,000 and "Cash account" is a separate asset. So a
                # line can be the TAIL of one record's value and the HEAD of the next
                # record's name at the same time — split it, never assign it wholesale.
                if awaiting and val:
                    current["value"].extend(val)
                    awaiting = not _band_complete(current["value"])
                    pending["asset"].extend(asset)
                    pending["owner"].extend(bkt["Owner"])
                    continue
                if val:
                    current = {"asset": pending["asset"] + asset,
                               "owner": " ".join(pending["owner"] + bkt["Owner"]),
                               "value": val[:],
                               "income": pending["income"] + bkt["Income"]}
                    records.append(current)
                    awaiting = not _band_complete(val)
                    pending = {"asset": [], "owner": [], "income": []}
                else:
                    pending["asset"].extend(asset)
                    pending["owner"].extend(bkt["Owner"])
                    pending["income"].extend(bkt["Income"])
        if not records and not header_seen:
            return [], "unparsed_layout"
        rows = [_finalize(r) for r in records]
        # Drop pure-structural rows: a held-through account header ('Savings Plus ⇒')
        # carries no ticker, no value, and no asset-type code — it is not a holding and
        # only inflates the value-band miss count. Rows with any of the three are kept.
        rows = [r for r in rows if r["ticker"] or r["value_lo"] is not None or r["asset_type"]]
        return rows, "ok"


def _mark(con, doc, chamber, status):
    con.execute("INSERT OR REPLACE INTO congress_fd_seen VALUES (?,?,?,?)",
                (doc, chamber, status, int(time.time())))


def ingest_filing(con, year, filing, raw_dir, ua, unparsed_dir):
    doc = filing.get("DocID")
    if not doc:
        return {"status": "no_docid", "rows": 0}
    if con.execute("SELECT 1 FROM congress_fd_seen WHERE doc_id=?", (doc,)).fetchone():
        return {"status": "seen", "rows": 0}
    path = fetch_pdf(year, doc, raw_dir, ua)
    if not path:
        _mark(con, doc, "house", "no_pdf")
        con.commit()
        return {"status": "no_pdf", "rows": 0}
    try:
        rows, status = parse_fd_assets(path)
    except Exception as exc:  # noqa: BLE001 - fail loud into status, never abort the run
        _mark(con, doc, "house", "parse_error")
        con.commit()
        return {"status": "parse_error:" + str(exc)[:80], "rows": 0}
    if status == "unparsed_layout":
        unparsed_dir.mkdir(parents=True, exist_ok=True)
        (unparsed_dir / "{}.pdf".format(doc)).write_bytes(path.read_bytes())
    for i, r in enumerate(rows):
        con.execute(
            "INSERT OR REPLACE INTO congress_holdings(doc_id, chamber, filing_year, "
            "coverage_year, filing_date, period, member_last, member_first, state_dist, "
            "person_id, row_idx, asset_name, ticker, asset_type, owner, value_lo, "
            "value_hi, income_type, ingested_at_unix) "
            "VALUES(?,'house',?,?,?,?,?,?,?,NULL,?,?,?,?,?,?,?,?,?)",
            # COVERAGE YEAR comes from the index's own `Year` field, never inferred.
            # {N}FD.zip holds annuals COVERING calendar year N, filed mostly in N+1
            # (verified: every entry in the 2022/2023/2024/2025 zips carries Year=N while
            # FilingDate is predominantly N+1). An earlier `year - 1` inference put every
            # House coverage year one year early, which also moved the Phase F flow cutoff
            # back a year and counted an extra year of PTRs as post-anchor.
            (doc, year, _cov_year(filing, year),
             _iso_mdy(filing.get("FilingDate")), filing.get("FilingDate"),
             filing.get("Last"), filing.get("First"),
             filing.get("StateDst"), i, r["asset_name"], r["ticker"], r["asset_type"],
             r["owner"], r["value_lo"], r["value_hi"], r["income_type"], int(time.time())))
    _mark(con, doc, "house", status)
    con.commit()
    return {"status": status, "rows": len(rows), "name": filing.get("Last")}


def main(argv=None):
    ap = argparse.ArgumentParser(description="House annual FD holdings ingest (SM-P1b)")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--year", type=int, default=None, help="filing year (default: current + prior)")
    ap.add_argument("--limit", type=int, help="cap filings (for a sample run)")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT") or "smartmoney@example.com"
    ua = UA_TMPL.format(contact)
    raw_dir = pathlib.Path(RAW_DIR_DEFAULT)
    raw_dir.mkdir(parents=True, exist_ok=True)
    unparsed = pathlib.Path(UNPARSED_DIR)
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    years = [args.year] if args.year else [time.gmtime().tm_year, time.gmtime().tm_year - 1]
    tallies = {"filings": 0, "ok": 0, "rows": 0, "paper": 0, "unparsed": 0,
               "no_pdf": 0, "seen": 0, "error": 0}
    try:
        for year in years:
            idx = fetch_year_index(year, raw_dir, ua)
            if not idx:
                continue
            if args.limit:
                idx = idx[:args.limit]
            for i, filing in enumerate(idx, 1):
                res = ingest_filing(con, year, filing, raw_dir, ua, unparsed)
                st = res["status"]
                tallies["filings"] += 1
                tallies["rows"] += res["rows"]
                key = ("ok" if st == "ok" else "paper" if st == "paper" else
                       "unparsed" if st == "unparsed_layout" else "seen" if st == "seen"
                       else "no_pdf" if st == "no_pdf" else "error")
                tallies[key] += 1
                if i % 25 == 0:
                    print("[house_fd] {} {}/{} {}".format(year, i, len(idx), tallies),
                          flush=True)
    finally:
        con.close()
    print("[house_fd] DONE {}".format(tallies))
    return 0


if __name__ == "__main__":
    sys.exit(main())
