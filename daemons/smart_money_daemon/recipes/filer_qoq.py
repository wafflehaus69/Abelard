#!/usr/bin/env python3
"""filer_qoq.py - one 13F filer, every period, quarter-on-quarter deltas.

ANSWERS
  What did this filer report holding at each quarter end, and what changed against
  its OWN previous filing: new / added / trimmed / unchanged / exited.

RUN
  python3 filer_qoq.py --db PATH --cik 1536411        newest period, 60 largest rows
  python3 filer_qoq.py --cik 0001536411 --period all --limit 0
  python3 filer_qoq.py --cik 1135730 --period 2024-12-31 --status exited,new
  python3 filer_qoq.py --cik 1135730 --csv out.csv
  python3 filer_qoq.py --list                         the filers in the database
  --cik takes any spelling, padded or bare. With no --cik it shows Duquesne.
  --csv writes EVERY row of EVERY period, whatever --period/--status/--limit print.
  From Python: filer_qoq(connect_ro(db), cik)["rows"] is a list of dicts.
  filer_qoq_df(db, cik) returns a DataFrame if pandas is installed (not needed).

SOURCE
  thirteenf_holdings, opened read-only. Counts below are the mirror of 2026-10-01:
  13,697 rows, 28 filers, 225 filings, 11 periods 2023-12-31 .. 2026-06-30.
  Run for all 28 filers the recipe returns 15,816 rows: those 13,697 plus 2,119
  exit rows. prices is read only for the split hint.

ONE ROW = one filer, one period, one position.
  A position is (UPPER(cusip), put_call). put_call is long / call / put, so the
  stock, the calls and the puts on one issuer are three positions.

COLUMNS
  cik             the filer, as a bare integer
  period          quarter end the holdings are stated for
  filed_date      day the filing reached EDGAR: 30-66 days after period, mean 44.
                  This is when the market learned. On an exit row it is the filing
                  in which the position is ABSENT.
  accession       that filing
  prev_period     this filer's previous period in the table; blank on its first
  gap_q           quarters from prev_period to period. 1 is normal. See TRAPS.
  cusip, issuer, ticker, put_call, instrument_class   as stored. Blank ticker = unmapped.
  shares          share count as filed. 0 on an exit row. See TRAPS: PRN, options.
  shares_type     SH, PRN, or blank (not recorded)
  value_usd       whole US dollars = value * value_scale. 0 on an exit row.
  prev_shares, prev_value_usd   the same position one filing earlier; 0 if new
  d_shares, d_value_usd         this minus previous
                  All four are blank when status is first.
  status          first / new / added / trimmed / unchanged / exited
  basis           what decided added/trimmed/unchanged: shares or value
  note            par, par?, unit_change, split?.., cusip_change?.., scale_unresolved

STATUS
  first      the filer's earliest period in the table. Not a purchase: it is where
             the ingest starts. No deltas. 1,610 rows.
  new        absent in prev_period, present now. Delta = the whole position. 2,211.
  exited     present in prev_period, absent now. Its own row: shares 0,
             value_usd 0, delta = minus the whole position. 2,119.
  added / trimmed / unchanged   present in both: 3,466 / 3,880 / 2,530. Decided on
             SHARES, because value also moves with the price. Value decides only
             if a share count is 0 or SH meets PRN (1 row on the mirror).
  Check: within a period, sum(d_value_usd) = this book minus the previous book.
  Holds on all 197 transitions.

EXCLUDED
  thirteenf_holdings has no quarantine flag (no date_flag, no value_flag), so
  nothing is excluded by flag. Two structural exclusions, both 0 rows on the mirror:
    period IS NULL            cannot be placed on the period axis
    superseded filing         two filings for one filer and period: the later
                              filed_date wins, the other is dropped and counted
  Not excluded: unmapped tickers, small positions, PRN rows, options, value 0.

TRAPS (counts are all 28 filers on the mirror)
  Read value_usd, never value. Raw value is in thousands on 17 filings (808
    rows: Duquesne and Baupost) and in dollars on the other 208. value_scale is
    never NULL here; if it were, value_usd would be the raw number and the row
    is noted `scale_unresolved`.
  d_value_usd is price AND trading. Only d_shares is the trade.
  Splits are NOT adjusted. A split moves shares with no trade. `split?xN(adj:S)`
    marks a continuing row where the filing's implied price (value_usd / shares)
    moved against prices.close by a clean ratio N between the two periods;
    prices.close is restated for later splits, the filing is not. S is what status
    would be with prev_shares * N. It is a hint and is not applied. 19 rows
    flagged; in 13 the status changes (11 added -> trimmed, 2 added -> unchanged).
    The test needs a ticker and a close at both period ends: it ran on 6,511 of
    9,876 continuing rows. The other 3,365 are untested.
  A CUSIP change looks like an exit plus a new. Reverse splits, re-domiciles and
    renames re-issue the CUSIP. The two rows are never merged.
    `cusip_change?->X` on the exit and `cusip_change?<-X` on the new mark a pair
    when exactly one exit and one new in the same transition share put_call,
    instrument_class and issuer name (or ticker). 23 pairs flagged; all 23 read as
    one issuer by eye. At least 8 real ones are NOT flagged (Lucid at Coatue
    2025-09-30, Formula One at Berkshire 2025-12-31). The old CUSIP often maps to
    a foreign-listing ticker (LRCXEUR, HONGBP): do not key history on ticker.
  PRN rows are principal, not shares. 46 rows have shares_type PRN (note `par`):
    43 convertible notes, and 3 Cheniere common rows at Horizon Kinetics that the
    filer mis-typed (value/shares is $180-231, a share price). 6,063 rows have
    shares_type blank, all ingested before 2026-08-18; 238 of those are
    convertible_note. 234 of the 238 have value_usd/shares under 10, so
    principal (note `par?`). The other 4 (Situational Awareness 2045724,
    2025-09-30) get no note: CIFR, LITE and WDC are valued exactly at the
    common's close, so they are share counts filed under a note CUSIP; BE
    (14.40 against a close of 84.57) fits neither. d_shares on a par row is a
    change in principal. An exit row keeps the note of the position that left,
    so the output carries `par` on 61 rows and `par?` on 290.
  Option rows carry UNDERLYING shares, all 450 typed SH, none 0. value_usd is
    whatever the filer wrote. Of 365 option rows with a close, 303 are within 10%
    of shares * close and 10 more at a clean split multiple (underlying notional).
    All 44 priced Kopernik (1599814) option rows are far below it (premium-like).
    The other 8 fit neither (VIXY, SPYX, WOLF, GDX). 85 have no close to test.
    Do not add option value_usd across filers.
  gap_q > 1. 2 of 197 transitions skip quarters: Thiel Macro (1562087)
    2025-09-30 -> 2026-06-30 and Founders Fund Growth II (2106825) 2025-12-31 ->
    2026-06-30. Their deltas span the gap. A 13F-HR was fetched in the gap:
    thirteenf_filings_seen holds 5 accessions that landed no holdings rows
    (Thiel Macro 0001315863-26-000169, -26-000423, -20-000698, -20-000924;
    Growth II 0002106825-26-000003). Their periods are not stored; the 2026
    ones sit between the stored filings by accession order. Whether those
    tables were empty (nothing held) or failed to parse: not established. If
    empty, every position was exited and re-opened inside the gap, and the exit
    was knowable earlier than the filed_date shown.
  No filing is not an exit. 3 filers stop before 2026-06-30 (Pershing 1336528,
    Founders Fund VII 1846021, Scion 1649339). Their last positions get no exit row.
  Unmapped tickers are kept. 1,136 rows, $216.4bn of $4,590.4bn, have no ticker
    (the output adds 194 exit rows of unmapped positions: 1,330).
    8 of them are KKR at Akre (1112520), stored as lower-case CUSIP 48251w104.
  13F itself: long section-13(f) securities only, no shorts or cash, so an exit
    can be a position that left reporting rather than a sale. Only form 13F-HR is
    ingested; amendments (13F-HR/A) are not. How many quarters were later
    restated: not established.

AGAINST THE HOUSE (smart_money/queries.py, commit c3b7dbb)
  q_portfolio uses the same key and the same shares-first rule. Checked on all
  15,816 rows: status, shares, value and prior value agree (its badge None is
  `first` or `unchanged` here). Deliberate differences:
    1. Scale. The house multiplies raw value by ONE factor per filer
       (_filer_unit_scale). This reads value_usd, scaled per filing. Same numbers
       today: no filer mixes scales on the mirror. db.py records Duquesne
       switching units in 2022; if those periods are backfilled, value_usd is the
       one that stays right.
    2. Exit dates. The house stamps an exit with the PRIOR filing's period and
       filed_date. Here the exit sits in the period where it is absent, with that
       filing's filed_date: the day the exit became knowable.
    3. New rows have prev 0 and a delta; the house leaves prior_value None.
  _manager_flow is a different product: newest transition only, drops rows with
  no ticker, drops rows under a 0.25% book floor for two filers (Horizon
  Kinetics 1056823, First Eagle 1325447), hides an exit when the ticker is
  still held under another CUSIP, and writes nothing for a flat hold. For the 28
  newest transitions it yields 949 flows (163 unmapped rows skipped and counted)
  against 1,640 moving rows here. Call it with the CIK as the registry spells
  it, zero-padded: the floor is looked up by raw string, so a bare CIK silently
  turns the floor off and gives 1,511 flows and 257 skipped.
  Stale comments: the q_portfolio and _manager_flow docstrings say options are
  judged on notional value. The code (_flow_measure) uses shares when both
  periods have them, and all 450 option rows do. _flow_measure also says
  un-backfilled option rows still carry 0 shares; none do.
"""
import argparse
import csv
import os
import re
import sqlite3
import sys

try:                                    # optional; the recipe never requires it
    import pandas as pd
except ImportError:
    pd = None

DEFAULT_DB = os.path.expanduser("~/.openclaw/smart_money/smart_money_v0.db")
DEFAULT_CIK = "1536411"                 # Duquesne; used only when --cik is omitted

# Display labels only. Copied from smart_money/thirteenf_ingest.py CONFIRMED
# (commit c3b7dbb). The database stores no filer name.
FILER_LABELS = {
    1536411: "Duquesne Family Office", 1562087: "Thiel Macro",
    1846021: "Founders Fund VII", 2106825: "Founders Fund Growth II",
    2059583: "Affinity Partners (Kushner)", 2045724: "Situational Awareness LP",
    1135730: "Coatue Management", 1387322: "Whale Rock Capital Management",
    1569049: "Light Street Capital Management", 1061165: "Lone Pine Capital",
    1656456: "Appaloosa LP", 1263508: "Baker Bros Advisors",
    1336528: "Pershing Square Capital Management", 1029160: "Soros Fund Management",
    1040273: "Third Point LLC", 1649339: "Scion Asset Management",
    1652044: "Alphabet Inc", 1018724: "Amazon com Inc", 1045810: "NVIDIA Corp",
    1647251: "TCI Fund Management", 1709323: "Himalaya Capital Management",
    1067983: "Berkshire Hathaway Inc", 1061768: "Baupost Group",
    1112520: "Akre Capital Management", 1599814: "Kopernik Global Investors",
    1056823: "Horizon Kinetics Asset Management (Stahl)",
    1325447: "First Eagle Investment Management",
    1791786: "Elliott Investment Management (Singer)",
}

# ------------------------------------------------------------------ SQL
# Every holding row of one filer. value_usd, never raw value. CUSIP upper-cased
# so a case change cannot split one position into an exit plus a new.
SQL_HOLDINGS = """
SELECT accession,
       period,
       filed_date,
       UPPER(cusip)      AS cusip,
       issuer,
       ticker,
       put_call,
       instrument_class,
       shares,
       shares_type,
       value_usd,
       value_scale
FROM   thirteenf_holdings
WHERE  CAST(cik AS INTEGER) = :cik
ORDER  BY period, filed_date, accession, cusip, put_call
"""

# Filers present, for --list.
SQL_FILERS = """
SELECT CAST(cik AS INTEGER)        AS cik,
       COUNT(DISTINCT period)      AS n_periods,
       MIN(period)                 AS first_period,
       MAX(period)                 AS last_period,
       COUNT(*)                    AS n_rows
FROM   thirteenf_holdings
GROUP  BY CAST(cik AS INTEGER)
ORDER  BY 1
"""

# Last close on or before a quarter end (quarter ends fall on weekends).
# prices.close is split-adjusted to today; that is what makes the split hint work.
SQL_CLOSE = """
SELECT close
FROM   prices
WHERE  ticker = :ticker
  AND  price_type = 'eod'
  AND  date <= :day
  AND  date >= date(:day, '-7 day')
ORDER  BY date DESC
LIMIT  1
"""

COLUMNS = [
    "cik", "period", "filed_date", "accession", "prev_period", "gap_q",
    "cusip", "issuer", "ticker", "put_call", "instrument_class",
    "shares", "shares_type", "value_usd",
    "prev_shares", "prev_value_usd", "d_shares", "d_value_usd",
    "status", "basis", "note",
]

STATUSES = ("first", "new", "added", "trimmed", "unchanged", "exited")

# Split ratios the hint will name. Anything else is left unnamed.
_CLEAN_SPLITS = (1.5, 2, 3, 4, 5, 6, 7, 8, 10, 15, 20, 25, 50)
_SPLIT_TOL = 0.02

# value_usd / shares at or above this is read as a share price, not a price of par.
_PAR_MAX_RATIO = 10


# ------------------------------------------------------------------ helpers
def connect_ro(db_path=DEFAULT_DB):
    """Read-only connection. mode=ro makes any write fail inside SQLite."""
    return sqlite3.connect("file:{}?mode=ro".format(db_path), uri=True)


def cik_int(cik):
    """Any CIK spelling ('0001536411', '1536411', 1536411) -> int."""
    return int(str(cik).strip())


def _quarter_index(period):
    return int(period[:4]) * 4 + (int(period[5:7]) - 1) // 3


def _norm_name(name):
    return re.sub(r"[^A-Z0-9]+", " ", (name or "").upper()).strip()


def _is_par(row):
    """True when `shares` is dollars of principal, 'maybe' when it probably is.

    'maybe' = type not recorded, class convertible_note, and value_usd per unit
    under _PAR_MAX_RATIO. Typed PRN notes run 0.22-2.78 on the mirror; the 4
    blank-type rows at 12.59 and above are share prices, not prices of par.
    """
    if row["shares_type"] == "PRN":
        return True
    if row["shares_type"] is None and row["instrument_class"] == "convertible_note":
        sh, val = row["shares"], row["value_usd"]
        if sh and val is not None and val / sh >= _PAR_MAX_RATIO:
            return False
        return "maybe"
    return False


def _tickers_compatible(a, b):
    """False only when both are mapped and neither is a prefix of the other."""
    if not a or not b:
        return True
    a, b = a.upper(), b.upper()
    return a.startswith(b) or b.startswith(a)


class _Closes:
    """Cached quarter-end closes. Silent no-op if the prices table is absent."""

    def __init__(self, con):
        self.con, self.cache, self.ok = con, {}, True
        try:
            con.execute("SELECT close FROM prices LIMIT 1")
        except sqlite3.Error:
            self.ok = False

    def get(self, ticker, day):
        if not self.ok or not ticker:
            return None
        key = (ticker.upper(), day)
        if key not in self.cache:
            r = self.con.execute(SQL_CLOSE, {"ticker": key[0], "day": day}).fetchone()
            self.cache[key] = r[0] if r and r[0] and r[0] > 0 else None
        return self.cache[key]


def _split_hint(closes, prev, cur, prev_period, period):
    """(ratio or None, checked) for one continuing position. A HINT, never applied.

    The filing's implied price (value_usd / shares) is the price as quoted THEN.
    prices.close is restated for every later split. Their ratio is therefore the
    cumulative split factor since that date, and the change in that ratio between
    two periods is the split that happened in between - whatever the filer traded.
    `checked` is False when the test could not be run (par row, no ticker, no price).
    """
    if _is_par(prev) or _is_par(cur):
        return None, False
    if not (prev["shares"] and cur["shares"] and prev["value_usd"] and cur["value_usd"]):
        return None, False
    c0 = closes.get(cur["ticker"], prev_period)
    c1 = closes.get(cur["ticker"], period)
    if not c0 or not c1:
        return None, False
    f = ((prev["value_usd"] / prev["shares"]) / c0) / ((cur["value_usd"] / cur["shares"]) / c1)
    for n in _CLEAN_SPLITS:
        for ratio in (n, 1.0 / n):
            if abs(f / ratio - 1.0) <= _SPLIT_TOL:
                moved_with_it = (cur["shares"] > prev["shares"]) == (ratio > 1)
                if cur["shares"] != prev["shares"] and moved_with_it:
                    return ratio, True
                return None, True
    return None, True


def _status(a, b, tol=0):
    return "added" if a > b + tol else "trimmed" if a < b - tol else "unchanged"


def _pair_cusip_changes(gone, new, prev_book, cur_book):
    """{key: other_cusip} for an exit and a new that look like one position
    under a re-issued CUSIP. A HINT: the rows are never merged.

    Paired when, in the same transition, exactly one exit and exactly one new
    share put_call + instrument_class and either the same issuer name with
    tickers that do not contradict, or the same mapped ticker.
    """
    out = {}

    def group(keys, book, keyfn):
        g = {}
        for k in keys:
            gk = keyfn(k, book[k])
            if gk is not None:
                g.setdefault(gk, []).append(k)
        return g

    def by_name(k, r):
        n = _norm_name(r["issuer"])
        return (k[1], r["instrument_class"], n) if n else None

    def by_ticker(k, r):
        return (k[1], r["instrument_class"], r["ticker"].upper()) if r["ticker"] else None

    for keyfn, need_compat in ((by_name, True), (by_ticker, False)):
        g_gone = group([k for k in gone if k not in out], prev_book, keyfn)
        g_new = group([k for k in new if k not in out], cur_book, keyfn)
        for gk, gs in g_gone.items():
            ns = g_new.get(gk, [])
            if len(gs) != 1 or len(ns) != 1:
                continue
            g, n = gs[0], ns[0]
            if need_compat and not _tickers_compatible(prev_book[g]["ticker"],
                                                      cur_book[n]["ticker"]):
                continue
            out[g] = n[0]
            out[n] = g[0]
    return out


# ------------------------------------------------------------------ core
def filer_qoq(con, cik):
    """All periods of one filer with QoQ deltas.

    Returns a dict: cik, label, filings (one per period, oldest first),
    rows (list of dicts keyed by COLUMNS), excluded, flagged.
    """
    cik = cik_int(cik)
    cursor = con.cursor()
    cursor.row_factory = sqlite3.Row
    raw = cursor.execute(SQL_HOLDINGS, {"cik": cik}).fetchall()
    excluded = {"period_null": 0, "superseded_filing": 0}
    flagged = {"par": 0, "par?": 0, "unit_change": 0, "scale_unresolved": 0,
               "split?": 0, "split_unchecked": 0, "cusip_change?": 0,
               "cusip_case_merged": 0}

    # 1. one filing per period: the latest-filed accession wins
    winner = {}
    for r in raw:
        if r["period"] is None:
            excluded["period_null"] += 1
            continue
        rank = (r["filed_date"] or "", r["accession"])
        if r["period"] not in winner or rank > winner[r["period"]]:
            winner[r["period"]] = rank

    # 2. books: period -> {(cusip, put_call): row}
    books, filings = {}, {}
    for r in raw:
        if r["period"] is None:
            continue
        if (r["filed_date"] or "", r["accession"]) != winner[r["period"]]:
            excluded["superseded_filing"] += 1
            continue
        d = dict(r)
        d["put_call"] = d["put_call"] or "long"
        book = books.setdefault(r["period"], {})
        key = (d["cusip"], d["put_call"])
        if key in book:                       # same CUSIP in two cases, one filing
            flagged["cusip_case_merged"] += 1
            book[key]["shares"] = (book[key]["shares"] or 0) + (d["shares"] or 0)
            book[key]["value_usd"] = (book[key]["value_usd"] or 0) + (d["value_usd"] or 0)
        else:
            book[key] = d
        filings[r["period"]] = {"period": r["period"], "filed_date": r["filed_date"],
                                "accession": r["accession"]}

    periods = sorted(books)
    closes = _Closes(con)
    rows = []
    for i, period in enumerate(periods):
        cur = books[period]
        prev_period = periods[i - 1] if i else None
        prev = books[prev_period] if i else {}
        gap_q = (_quarter_index(period) - _quarter_index(prev_period)) if i else None
        f = filings[period]
        f.update({"prev_period": prev_period, "gap_q": gap_q, "n_positions": len(cur),
                  "value_usd": sum(h["value_usd"] or 0 for h in cur.values())})
        gone = [k for k in prev if k not in cur]
        new = [k for k in cur if k not in prev] if i else []
        pairs = _pair_cusip_changes(gone, new, prev, cur) if i else {}

        def emit(h, p, status, basis, notes):
            par = _is_par(h)
            if par is True:
                notes.append("par")
                flagged["par"] += 1
            elif par == "maybe":
                notes.append("par?")
                flagged["par?"] += 1
            if h.get("value_scale") is None:
                notes.append("scale_unresolved")
                flagged["scale_unresolved"] += 1
            exited = status == "exited"
            sh = 0 if exited else (h["shares"] or 0)
            val = 0 if exited else (h["value_usd"] or 0)
            if status == "first":             # no earlier filing: nothing to diff
                psh = pval = None
            elif p is None:                   # new: it was absent, i.e. zero
                psh = pval = 0
            else:
                psh, pval = p["shares"] or 0, p["value_usd"] or 0
            rows.append({
                "cik": cik, "period": period, "filed_date": f["filed_date"],
                "accession": f["accession"], "prev_period": prev_period, "gap_q": gap_q,
                "cusip": h["cusip"], "issuer": h["issuer"], "ticker": h["ticker"],
                "put_call": h["put_call"], "instrument_class": h["instrument_class"],
                "shares": sh, "shares_type": h["shares_type"], "value_usd": val,
                "prev_shares": psh, "prev_value_usd": pval,
                "d_shares": None if psh is None else sh - psh,
                "d_value_usd": None if pval is None else val - pval,
                "status": status, "basis": basis, "note": " ".join(notes)})

        for key, h in cur.items():
            p = prev.get(key)
            notes = []
            if not i:
                emit(h, None, "first", "", notes)
                continue
            if p is None:
                if key in pairs:
                    notes.append("cusip_change?<-" + pairs[key])
                    flagged["cusip_change?"] += 1
                emit(h, None, "new", "", notes)
                continue
            # Same rule as queries._flow_measure: shares when both periods report
            # them, else value. Plus one guard: SH against PRN is not comparable.
            types = {h["shares_type"], p["shares_type"]} - {None}
            if len(types) > 1:
                basis = "value"
                notes.append("unit_change")
                flagged["unit_change"] += 1
            elif h["shares"] and p["shares"]:
                basis = "shares"
            else:
                basis = "value"
            a, b = ((h["shares"], p["shares"]) if basis == "shares"
                    else (h["value_usd"] or 0, p["value_usd"] or 0))
            status = _status(a, b)
            ratio, checked = _split_hint(closes, p, h, prev_period, period)
            if ratio:
                # what the status WOULD be with prev shares restated; not applied
                adj = _status(h["shares"], p["shares"] * ratio, tol=1)
                notes.append("split?x{:g}(adj:{})".format(round(ratio, 4), adj))
                flagged["split?"] += 1
            elif not checked:
                flagged["split_unchecked"] += 1
            emit(h, p, status, basis, notes)

        for key in gone:                      # held last period, absent now
            p = prev[key]
            notes = []
            if key in pairs:
                notes.append("cusip_change?->" + pairs[key])
                flagged["cusip_change?"] += 1
            emit(p, p, "exited", "", notes)

    # within a period: largest first, an exit ranked by what it was worth before
    rows.sort(key=lambda r: (r["period"],
                             -max(r["value_usd"], r["prev_value_usd"] or 0),
                             r["cusip"], r["put_call"]))
    return {"cik": cik, "label": FILER_LABELS.get(cik, ""),
            "filings": [filings[p] for p in periods], "rows": rows,
            "excluded": excluded, "flagged": flagged,
            "split_hint_available": closes.ok}


def filer_qoq_df(db_path=DEFAULT_DB, cik=DEFAULT_CIK):
    """Same table as a pandas DataFrame. Needs pandas; nothing else here does."""
    if pd is None:
        raise RuntimeError("pandas is not installed; use filer_qoq() or --csv")
    con = connect_ro(db_path)
    try:
        return pd.DataFrame(filer_qoq(con, cik)["rows"], columns=COLUMNS)
    finally:
        con.close()


# ------------------------------------------------------------------ output
def _n(x, signed=False):
    if x is None:
        return ""
    return "{:+,}".format(x) if signed and x else "{:,}".format(x)


_CLASS_SHORT = {"common": "common", "option_call": "opt_call", "option_put": "opt_put",
                "convertible_note": "conv_note", "convertible_preferred": "conv_pref",
                "warrant": "warrant", "unit": "unit", "unresolved": "unresolvd"}


def _print_table(header, lines, right):
    widths = [max(len(str(x[i])) for x in [header] + lines) for i in range(len(header))]

    def fmt(vals):
        return "  ".join(str(v).rjust(w) if i in right else str(v).ljust(w)
                         for i, (v, w) in enumerate(zip(vals, widths))).rstrip()
    print(fmt(header))
    print("  ".join("-" * w for w in widths))
    for ln in lines:
        print(fmt(ln))


def print_report(res, period="latest", statuses=None, limit=60):
    rows, filings = res["rows"], res["filings"]
    print("13F filer {}  {}".format(res["cik"], res["label"]))
    if not filings:
        print("no rows in thirteenf_holdings for this CIK (--list shows the filers)")
        return
    print("{} periods {} .. {}   value_usd = whole US dollars".format(
        len(filings), filings[0]["period"], filings[-1]["period"]))

    print("\nPER PERIOD (positions and book are the filing as reported; the status "
          "counts are against prev_period)")
    counts = {}
    for r in rows:
        counts.setdefault(r["period"], dict.fromkeys(STATUSES, 0))[r["status"]] += 1
    lines = []
    for f in filings:
        c = counts.get(f["period"], dict.fromkeys(STATUSES, 0))
        lines.append([f["period"], f["filed_date"], f["prev_period"] or "-",
                      "" if f["gap_q"] is None else f["gap_q"],
                      _n(f["n_positions"]), _n(f["value_usd"])]
                     + [c[s] for s in STATUSES])
    _print_table(["period", "filed_date", "prev_period", "gap_q", "positions",
                  "book_value_usd"] + list(STATUSES), lines,
                 right=set(range(3, 6 + len(STATUSES))))

    if period == "latest":
        wanted = [filings[-1]["period"]]
    elif period == "all":
        wanted = [f["period"] for f in filings]
    elif period in counts:
        wanted = [period]
    else:
        wanted = []
        print("\nno such period for this filer: {} (see the table above)".format(period))
    for per in wanted:
        f = next(x for x in filings if x["period"] == per)
        sel = [r for r in rows if r["period"] == per
               and (not statuses or r["status"] in statuses)]
        print("\nPERIOD {}  filed {}  accession {}  vs {}{}".format(
            per, f["filed_date"], f["accession"], f["prev_period"] or "nothing (first)",
            "  ** GAP: {} quarters **".format(f["gap_q"]) if (f["gap_q"] or 1) > 1 else ""))
        shown = sel if not limit else sel[:limit]
        lines = [[r["status"], (r["ticker"] or "")[:16], (r["issuer"] or "")[:24],
                  r["cusip"], r["put_call"],
                  _CLASS_SHORT.get(r["instrument_class"], r["instrument_class"] or ""),
                  r["shares_type"] or "", _n(r["shares"]), _n(r["d_shares"], True),
                  _n(r["value_usd"]), _n(r["d_value_usd"], True), r["note"]]
                 for r in shown]
        if lines:
            _print_table(["status", "ticker", "issuer", "cusip", "p/c", "class", "typ",
                          "shares", "d_shares", "value_usd", "d_value_usd", "note"],
                         lines, right={7, 8, 9, 10})
        if len(shown) < len(sel):
            print("... {} of {} rows shown, ranked by the larger of value_usd and the "
                  "previous value_usd (--limit 0 for all, --csv for every period)"
                  .format(len(shown), len(sel)))

    print("\nEXCLUDED (rows left out of the table)")
    print("  period IS NULL ................ {}".format(res["excluded"]["period_null"]))
    print("  superseded filing, same period  {}".format(res["excluded"]["superseded_filing"]))
    print("  unmapped ticker / below a floor / PRN / options / value 0: none excluded")
    print("KEPT BUT FLAGGED in note (rows, all periods)")
    for k in ("par", "par?", "unit_change", "split?", "cusip_change?",
              "scale_unresolved", "cusip_case_merged"):
        print("  {:<18} {}".format(k, res["flagged"][k]))
    print("KEPT, NOT FLAGGED, worth knowing (rows, all periods)")
    print("  {:<18} {}  (exit rows included)".format(
        "ticker unmapped", sum(1 for r in rows if not r["ticker"])))
    print("  {:<18} {}  (continuing rows where a split could not be tested)".format(
        "split_unchecked", res["flagged"]["split_unchecked"]))
    if not res["split_hint_available"]:
        print("  (no prices table: no split? hint was computed at all)")


def write_csv(res, path):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(res["rows"])


def print_filers(con):
    lines = [[c, FILER_LABELS.get(c, ""), n, a, b, _n(rows)]
             for c, n, a, b, rows in con.execute(SQL_FILERS)]
    _print_table(["cik", "label", "periods", "first", "last", "rows"], lines, right={2, 5})


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="One 13F filer across all periods with quarter-on-quarter deltas.")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite path (opened read-only)")
    ap.add_argument("--cik", default=None,
                    help="filer CIK, padded or bare (default {})".format(DEFAULT_CIK))
    ap.add_argument("--period", default="latest",
                    help="period to print: latest (default), all, or YYYY-MM-DD")
    ap.add_argument("--status", default=None,
                    help="comma list to print, e.g. exited,new (default: all)")
    ap.add_argument("--limit", type=int, default=60,
                    help="max rows printed per period, largest first; 0 = all")
    ap.add_argument("--csv", default=None, metavar="OUT",
                    help="write EVERY row of EVERY period to this CSV")
    ap.add_argument("--list", action="store_true", help="list the filers and exit")
    args = ap.parse_args(argv)

    try:
        con = connect_ro(args.db)
        con.execute("SELECT 1 FROM thirteenf_holdings LIMIT 1")
    except sqlite3.Error as exc:
        ap.error("cannot read thirteenf_holdings in {}: {}".format(args.db, exc))
    if args.list:
        print_filers(con)
        return 0
    try:
        cik = cik_int(args.cik if args.cik is not None else DEFAULT_CIK)
    except ValueError:
        ap.error("--cik must be a number, padded or bare: {!r}".format(args.cik))
    if args.cik is None:
        print("(no --cik given: showing the default filer; --list shows all)")
    statuses = None
    if args.status:
        statuses = {s.strip() for s in args.status.split(",") if s.strip()}
        bad = statuses - set(STATUSES)
        if bad:
            ap.error("unknown status {}; choose from {}".format(sorted(bad), STATUSES))
    res = filer_qoq(con, cik)
    print_report(res, args.period, statuses, args.limit)
    if args.csv:
        write_csv(res, args.csv)
        print("\nwrote {} rows to {}".format(len(res["rows"]), args.csv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
