"""SM-C2 Phase 1 Senate annual Financial Disclosure holdings ingest.

The Senate eFD annual report is HTML (a `<table id="grid_items">` assets grid), distinct
from the House PDF path. Enumerate annual reports via the eFD search (report_type 7, now
reachable via post_data — see recon/EFD_WAF_FINDING.md), fetch each detail page, parse the
assets grid, and land per-asset rows into congress_holdings with chamber='senate'.
Scripts-only, no LLM.

FAIL-LOUD, never silently lose a filing (the source is WAF-fronted and rate-limits under
load — recon/EFD_WAF_FINDING.md):
  * requests are PACED (0.5s) like the sibling efd_ingest / house_fd_ingest.
  * the detail GET checks HTTP status; a non-200 is a transient error, NOT terminal.
  * a 200 body is classified: an assets grid -> ok; a real eFD report skeleton (Part 3 /
    Part 4 headers) with no grid -> no_grid (genuinely empty, terminal); anything else is
    a WAF / interstitial soft_block (transient).
  * only TERMINAL outcomes (ok / no_grid / paper) are written to the congress_fd_seen
    resume ledger. Transient outcomes (http_*, fetch_error, soft_block) are left UNMARKED
    so a later run retries them, and main() reports the retriable count loudly.
  * the search endpoint fails loud (IngestError) on a malformed / WAF response.

Ticker comes from the yahoo-finance link the filing carries, else a leading 'TICKER-'
prefix / bare symbol / trailing '(TICKER)', only for equity-like asset types so a bank
deposit can't mint a phantom ticker. Owner is normalized to House-style codes. filing_year
is eFD's own "Annual Report for YYYY" title label (authoritative), falling back to the
filed-date year. state_dist / person_id are NULL for the Senate (the search row carries no
state; a roster supplies party/state in a later phase).
"""
import argparse
import datetime as dt
import html as _html
import re
import sys
import time

from . import db as dbmod
from .efd_session import bootstrap, post_data
from .efd_ingest import load_env, IngestError
from .house_fd_ingest import _parse_band   # shared FD value-band vocabulary

VIEW = "https://efdsearch.senate.gov/search/view/{kind}/{uuid}/"
ANN_LINK = re.compile(r"/search/view/(annual|paper)/([0-9a-f-]{8,})/")
_TABLE = re.compile(r'<table id="grid_items".*?</table>', re.S)
_TR = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_TD = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_STRONG = re.compile(r"<strong[^>]*>(.*?)</strong>", re.S)
_TITLE_YEAR = re.compile(r"Annual Report for (\d{4})", re.I)
# eFD annual report section skeleton — always rendered on a real report page (even one with
# no assets), absent on a WAF / rate-limit / interstitial body. Used to tell a genuinely
# empty report (terminal no_grid) apart from a soft-block that must be retried.
_CHROME = ("Part 3", "Part 4")
# eFD bounces some report ids back to the search form instead of serving a detail page.
# Observed consistently (across sessions and hours) for CANDIDATE filings — people who
# filed a disclosure but never served; eFD indexes them under the /annual/ path yet will
# not serve them there. That is terminal, not transient: retrying never recovers them.
_SEARCH_PAGE = "eFD: Find Reports"
_CANDIDATE = re.compile(r"candidate", re.I)
_TERMINAL = frozenset(("ok", "no_grid", "paper", "candidate_not_served"))
_YH_TICK = re.compile(r"finance\.yahoo\.com/q\?s=([A-Za-z0-9.\-]{1,10})")
# "MFC-Manulife..." / "MUB - iShares..." — bare-symbol prefix, no yahoo link. The tail
# is any letter (company names may start lowercase, e.g. "iShares"); the leading all-caps
# run is the discriminator, so a prose name like "Bristol-Meyers" (one leading cap) won't
# match while a real symbol prefix (all-caps 1-5) will.
_LEAD_TICK = re.compile(r"^([A-Z]{1,5}(?:\.[A-Z]{1,2})?)\s*-\s*[A-Za-z]")
_BARE_TICK = re.compile(r"^[A-Z]{1,5}$")           # whole name is a bare symbol ("VTV")
# Trailing "(IBM)" when the filer typed a company name with the symbol in parens.
_PAREN_TICK = re.compile(r"\(([A-Z]{1,5})\)")
# Parenthesized tokens that look like tickers but are exchanges / structure labels.
_NOT_TICKER = frozenset((
    "NYSE", "AMEX", "OTC", "ARCA", "BATS", "LSE", "TSX", "ASX", "NASD",
    "ADR", "ADS", "ORD", "LLC", "LP", "REIT", "ETF", "ETN", "FUND",
    "INC", "CORP", "CO", "NA", "USD", "EUR", "GBP", "JPY", "COM"))
# Asset types where a leading/parenthetical symbol convention is expected. The heuristic
# extractors run only for these, so a bank-deposit "USAA - Federal Savings Bank" or a
# muni-bond row can't mint a phantom ticker. The yahoo link is trusted for any type.
_EQUITYLIKE = frozenset(("ST", "OP", "EF", "MF"))

PACE_SECONDS = 0.5
_last_call = 0.0


def _pace():
    """Throttle to the sibling eFD modules' rate (the host tarpits under load)."""
    global _last_call
    wait = PACE_SECONDS - (time.monotonic() - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def _extract_ticker(asset_cell, strong_txt, atype):
    ym = _YH_TICK.search(asset_cell)
    if ym:
        return ym.group(1).upper()
    if atype not in _EQUITYLIKE:
        return None
    lm = _LEAD_TICK.match(strong_txt)
    if lm:
        return lm.group(1).upper()
    if _BARE_TICK.match(strong_txt):
        return strong_txt.upper()
    hit = None
    for cand in _PAREN_TICK.findall(strong_txt):   # last symbol-shaped parens wins
        if cand.upper() not in _NOT_TICKER:
            hit = cand.upper()
    return hit


def _text(s):
    t = _html.unescape(re.sub(r"<[^>]+>", " ", s or ""))   # strip tags, then decode entities
    return re.sub(r"\s+", " ", t).strip()


def _norm_owner(t):
    t = t.lower()
    if "spouse" in t:
        return "SP"
    if "joint" in t:
        return "JT"
    if "depend" in t or "child" in t:
        return "DC"
    return "Self"


def _asset_type(raw):
    """Equity-like types -> short codes used by the breadth query; everything else -> None
    (matches the House writer, which stores None for unrecognized types rather than a
    truncated free-text phrase that would pollute the shared column)."""
    low = raw.lower()
    if "option" in low:
        return "OP"
    if "stock" in low:
        return "ST"
    if "exchange traded" in low or "etf" in low:
        return "EF"
    if "mutual fund" in low:
        return "MF"
    if "corporate" in low and "bond" in low:
        return "CS"
    return None


def parse_annual_assets(html):
    """(rows, status). status ok | no_grid. Pure parser over the assets grid; callers that
    need to tell an empty report from a soft-block use _classify_body on the raw page."""
    tm = _TABLE.search(html)
    if not tm:
        return [], "no_grid"
    out = []
    for tr in _TR.findall(tm.group(0)):
        tds = _TD.findall(tr)
        if len(tds) < 7:
            continue
        asset_cell = tds[1]
        sm = _STRONG.search(asset_cell)
        strong_txt = _text(sm.group(1) if sm else asset_cell)
        atype = _asset_type(_text(tds[2]))
        ticker = _extract_ticker(asset_cell, strong_txt, atype)
        lo, hi = _parse_band(_text(tds[4]))
        out.append({"asset_name": strong_txt[:200], "ticker": ticker,
                    "asset_type": atype,
                    "owner": _norm_owner(_text(tds[3])),
                    "value_lo": lo, "value_hi": hi,
                    "income_type": _text(tds[5])[:60] or None})
    return out, "ok"


def _classify_body(html):
    """ok = assets grid present; no_grid = a real eFD report page (Part 3/Part 4 skeleton)
    with no assets grid, i.e. genuinely empty (TERMINAL); not_served = eFD bounced us to
    the search form rather than serving this report; soft_block = not an eFD page we
    recognise at all (WAF / interstitial / rate-limit body). The last two are RETRIABLE by
    default — the guard that stops a rate-limited fetch being logged as a healthy empty
    filing and lost forever. `not_served` is only retired when the caller can also see the
    filing is a candidate report (see ingest_report)."""
    if _TABLE.search(html):
        return "ok"
    if all(c in html for c in _CHROME):
        return "no_grid"
    if _SEARCH_PAGE in html:
        return "not_served"
    return "soft_block"


def search_annual(sess, since_mdy, length=100):
    """Enumerate annual (report_type 7) filings submitted on/after since_mdy (MM/DD/YYYY),
    newest first. Yields index rows [first, last, office, link_html, filed]. Fails loud
    (IngestError) on a malformed / WAF search response rather than silently yielding
    nothing (mirrors efd_ingest.search_year)."""
    start = 0
    while True:
        _pace()
        body = post_data(sess, {
            "draw": "1", "start": str(start), "length": str(length),
            "report_types": "[7]", "filer_types": "[]", "first_name": "", "last_name": "",
            "submitted_start_date": "{} 00:00:00".format(since_mdy), "submitted_end_date": "",
            "candidate_state": "", "senator_state": "", "office_id": "",
            "order[0][column]": "4", "order[0][dir]": "desc"})
        data = body.get("data")
        if data is None or "recordsTotal" not in body:
            raise IngestError("malformed senate annual search response at start={}".format(start))
        for row in data:
            yield row
        start += length
        if start >= body["recordsTotal"] or not data:
            break


def _mark(con, uuid, status):
    con.execute("INSERT OR REPLACE INTO congress_fd_seen VALUES (?,?,?,?)",
                (uuid, "senate", status, int(time.time())))


def _iso_mdy(s):
    """'05/15/2024' or '5/15/2024' -> '2024-05-15'. Unparseable stays None, never
    guessed — filing_date feeds the fusion anchor and a wrong date mis-ages a book."""
    m = re.match(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})", s or "")
    if not m:
        return None
    return "{}-{:02d}-{:02d}".format(m.group(3), int(m.group(1)), int(m.group(2)))


def _year_of(html, filed):
    """eFD's authoritative 'Annual Report for YYYY' label, else the filed-date year."""
    ty = _TITLE_YEAR.search(html or "")
    if ty:
        return int(ty.group(1))
    fm = re.search(r"(\d{4})", filed or "")
    return int(fm.group(1)) if fm else None


def ingest_report(con, sess, row):
    m = ANN_LINK.search(row[3])
    if not m:
        return {"status": "no_link", "rows": 0}
    kind, uuid = m.groups()
    if con.execute("SELECT 1 FROM congress_fd_seen WHERE doc_id=?", (uuid,)).fetchone():
        return {"status": "seen", "rows": 0}
    if kind == "paper":
        _mark(con, uuid, "paper")
        con.commit()
        return {"status": "paper", "rows": 0}
    try:
        _pace()
        resp = sess.get(VIEW.format(kind=kind, uuid=uuid), timeout=60)
    except Exception as exc:  # noqa: BLE001 - transient network error, retried (not marked)
        return {"status": "fetch_error:" + str(exc)[:50], "rows": 0}
    if resp.status_code != 200:
        return {"status": "http_{}".format(resp.status_code), "rows": 0}   # transient
    html = resp.text
    cls = _classify_body(html)
    if cls == "not_served":
        # eFD bounced to the search form. If the index row itself says this is a candidate
        # filing (office 'Candidate (Candidate)' / link text 'Candidate Report'), the
        # report is not obtainable at this path and never will be -> retire it with a
        # status that says exactly that, so the retriable count keeps meaning "member data
        # we still owe". For a non-candidate, stay retriable: that would be a real block.
        if _CANDIDATE.search(row[2] or "") or _CANDIDATE.search(row[3] or ""):
            _mark(con, uuid, "candidate_not_served")
            con.commit()
            return {"status": "candidate_not_served", "rows": 0}
        return {"status": "not_served", "rows": 0}
    if cls == "soft_block":
        return {"status": "soft_block", "rows": 0}     # WAF/interstitial, retried (not marked)
    year = _year_of(html, row[4])
    rows = parse_annual_assets(html)[0] if cls == "ok" else []
    for i, r in enumerate(rows):
        con.execute(
            "INSERT OR REPLACE INTO congress_holdings(doc_id, chamber, filing_year, "
            "coverage_year, filing_date, period, member_last, member_first, state_dist, "
            "person_id, row_idx, asset_name, ticker, asset_type, owner, value_lo, "
            "value_hi, income_type, ingested_at_unix) "
            "VALUES(?,'senate',?,?,?,?,?,?,NULL,NULL,?,?,?,?,?,?,?,?,?)",
            # eFD's title year IS the coverage year ("Annual Report for CY 2025" filed
            # 2026), so coverage_year == year here; filing_date is when it was filed.
            (uuid, year, year, _iso_mdy(row[4]), row[4], row[1], row[0], i,
             r["asset_name"], r["ticker"], r["asset_type"], r["owner"],
             r["value_lo"], r["value_hi"], r["income_type"], int(time.time())))
    _mark(con, uuid, cls)          # ok or no_grid — terminal
    con.commit()
    return {"status": cls, "rows": len(rows), "name": row[1]}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Senate annual FD holdings ingest (SM-C2 P1)")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--since", default="01/01/2024", help="MM/DD/YYYY submitted floor")
    ap.add_argument("--limit", type=int, help="cap filings (sample run)")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT") or "smartmoney@example.com"
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    sess = bootstrap("AbelardSmartMoney/0.1 (+{})".format(contact), probe=False)
    tally = {"filings": 0, "ok": 0, "rows": 0, "no_grid": 0, "paper": 0,
             "seen": 0, "candidate_not_served": 0, "retriable": 0}
    try:
        for i, row in enumerate(search_annual(sess, args.since), 1):
            if args.limit and tally["filings"] >= args.limit:
                break
            res = ingest_report(con, sess, row)
            st = res["status"]
            tally["filings"] += 1
            tally["rows"] += res["rows"]
            tally[st if st in ("ok", "no_grid", "paper", "seen",
                               "candidate_not_served") else "retriable"] += 1
            if i % 25 == 0:
                print("[senate_fd] {} {}".format(i, tally), flush=True)
    finally:
        con.close()
    print("[senate_fd] DONE {}".format(tally))
    if tally["candidate_not_served"]:
        print("[senate_fd] NOTE {} candidate reports are indexed but not served by eFD at "
              "the annual path (filers who never served) - retired, not retriable".format(
                  tally["candidate_not_served"]))
    if tally["retriable"]:
        print("[senate_fd] WARNING {} filings hit transient errors (http/fetch/soft_block) "
              "and were left UNMARKED for retry on the next run".format(tally["retriable"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
