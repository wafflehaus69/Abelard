#!/usr/bin/env python3
"""opposed_pairs.py - recipe (c): the 13F opposed-pairs table at a chosen quarter end.

WHAT IT ANSWERS
  For one quarter end: which tracked 13F filers ACCUMULATED each issuer over the
  quarter, which DISTRIBUTED it, the count and the names on each side, and every
  filer-vs-filer pair standing on opposite sides of one issuer. It ranks by
  breadth of disagreement. It says nothing about who is right.

RUN
  wsl -e python3 opposed_pairs.py --db /home/wafflehouse/.openclaw/smart_money/smart_money_v0.db
    --period 2026-03-31   a quarter end; default = newest COMPLETE period (below)
    --union-twins         merge share-class twins; list rotators separately
    --floor-fix           judge floor crossings on shares (below)
    --align quarter|own|house    which two filings are compared (below)
    --min-side N          filers needed on EACH side to count as opposed (default 1,
                          minimum 1)
    --instrument SH|CALL|PUT     show one instrument only
    --discretionary-only  drop corporate_strategic filers (Alphabet, Amazon, NVIDIA)
    --csv OUT             OUT = issuer table; OUT.pairs.csv, OUT.rotating.csv beside it
    --top N               rows printed per section (default 15, 0 = all)
    --registry PATH       registry.json if it is not at <db dir>/analysis/
    --as-of YYYY-MM-DD    override the clock that picks the default period
  Standard library only. The database is opened mode=ro. If pandas is installed,
  opposed_pairs_df(db, table="issuers"|"pairs"|"rotating", **options) returns a
  DataFrame; build(con, registry_path, **options) returns plain dicts without it.
  All numbers below were measured on the mirror (snapshot 2026-10-01).

DEFINITIONS - the house rule (queries._manager_flow, queries.q_opposed_pairs)
  tracked filer  A registry.json entry with role "manager_13f" and a cik. 28 of
                 them. The registry is a JSON FILE beside the database
                 (<db dir>/analysis/registry.json, as_of 2026-07-31), not a table.
                 All 28 CIKs in thirteenf_holdings are tracked; none is untracked.
  flow           One filer, one (ticker, instrument). Instrument is SH (put_call
                 'long'), CALL or PUT. Lines are matched between the filer's two
                 filings on (cusip, put_call):
                   new / added     -> accumulating
                   trimmed / exited -> distributing
                   same size       -> no flow, on neither side
  size           SHARES when both filings give a non-zero share count, else
                 dollars. Every line on the mirror has shares, so every direction,
                 options included, is judged on shares: the dollar fallback fired
                 0 times in any period. The _manager_flow docstring line that
                 options are judged "on notional value" is stale; _flow_measure
                 states the current rule.
  floor          registry position_floor_pct. Two filers have one, both 0.25% of
                 the filing's total value_usd: Horizon Kinetics and First Eagle.
                 Lines under it are dropped before matching.
  options        Puts and calls are separate instruments. They are never netted
                 against shares or each other.
  opposed        At least --min-side filers on each side of one (ticker,
                 instrument).
  value          value_usd = value * value_scale, US dollars, scale resolved per
                 filing. Shown for the current period; for an exit, the prior one.

WHICH PERIOD COLUMN, AND WHY filed_date IS THE MARKET'S CLOCK
  The table is built on `period`, the quarter end the positions are reported AS
  OF. A flow is the difference between two quarter-end snapshots, so two filers
  are comparable only when both are measured over the same quarter.
  Nobody outside the filer knew any of it on `period`. The market learned on
  `filed_date`. The 2026-06-30 filings reached EDGAR 2026-08-05 to 2026-08-14,
  36 to 45 days after the quarter end; 19 of the 25 arrived on the last day.
  For "what did the market know and when" (an event study, a backtest entry
  date, a join to prices or to Form 4) date the row by filed_date. Dating it by
  period hands the test 36-45 days of hindsight.
  Each issuer row carries opposed_known_on, the first day both sides were
  public. Each pair carries known_on, the later of its two filing dates. 67 of
  the 73 opposed rows at 2026-06-30 became knowable only on 2026-08-14.

COMPLETE PERIOD (the default --period)
  A period is COMPLETE when its filing deadline (quarter end + 45 days, rolled
  past a weekend) is on or before the corpus 13F clock. The clock is the later
  of MAX(filed_date) and the last 13F ingest day in thirteenf_holdings:
  2026-08-18 on the mirror. Newest period 2026-06-30, deadline 2026-08-14, so
  it is complete and is the default.
  Complete is a statement about the calendar, not about coverage: 25 of the 28
  tracked filers have a 2026-06-30 filing. Filings also arrive after day 45:
  16 of the 26 filings for 2025-12-31 came on 2026-02-17 or 02-18 (day 45 was a
  Saturday, the Monday a holiday), and one 2024-12-31 filing came on day 66.

ALIGNMENT (--align)
  quarter (default)  the filer needs a filing at --period AND at the quarter end
                     before it. 23 of 28 filers at 2026-06-30.
  own                prior = the filer's previous filing, whatever the gap.
                     25 filers: adds Thiel Macro (vs 2025-09-30) and Founders
                     Fund Growth II (vs 2025-12-31).
  house              what q_opposed_pairs does: each filer's own newest period
                     against its own previous one. Ignores --period.
  Filers compared / opposed rows / pair rows, quarter alignment, house rule:
    2026-06-30  23 /  73 / 205      2025-06-30  27 /  71 / 192
    2026-03-31  24 /  90 / 303      2025-03-31  27 /  82 / 234
    2025-12-31  25 /  90 / 231      2024-12-31  24 /  77 / 186
    2025-09-30  27 /  71 / 229      2024-09-30  15 /  46 / 105
  Before 2024-09-30 at most 2 filers can be compared. Do not use those periods.

MATCH WITH THE HOUSE FUNCTION
  --align house reproduces queries.q_opposed_pairs exactly on the mirror: 80
  rows in the same order, 251 side entries each with the same filer, action,
  value and period, all 949 filer flows identical, and the same no-ticker tally
  (163 lines, $59.37B).
  The house reports filers_compared 27, this recipe 28 under --align house: the
  house counts a filer only if it has a flow, and Founders Fund VII's two lines
  (CMPS, RUM) are both flat.
  --period 2026-06-30 gives 73 rows, identical to the house flows restricted to
  the same 23 filers. The gap from 80 to 73 is entirely off-phase filers. The
  house table mixes 6 period pairs: 23 filers on 2026-06-30 vs 2026-03-31, and
  Thiel Macro (2026-06-30 vs 2025-09-30), Founders Fund Growth II (2026-06-30
  vs 2025-12-31), Pershing Square (2026-03-31 vs 2025-12-31), Founders Fund VII
  (2025-12-31 vs 2025-09-30), Scion (2025-09-30 vs 2025-06-30). Those supply 19
  of its 251 side entries and touch 17 of its 80 rows; 7 rows exist only
  because of them (AAPL, CMS, DTE, LULU, MOH, TSLA shares; META calls).

TWO THINGS THE HOUSE FUNCTION DOES NOT DO
  --union-twins  Merges the reviewed TWIN_GROUPS list (10 groups) into one
      issuer, one vote per filer. GOOGL+GOOG, BRK/A+BRK/B and FOXA+FOX were
      named in the order; the other 7 were found in the corpus. Tickers are
      spelled as the corpus spells them: BRK/A, BRK/B, LEN/B, with a slash.
      Three old spellings of a class already listed are folded into its group
      (FWONKUSD = FWONK; LLYVK*, LLYVA* = LLYVK, LLYVA before 2025-12-31); the
      evidence is in the comment at TWIN_GROUPS.
      Alphabet at 2026-06-30:
        without   GOOGL  7 accumulating / 2 distributing
                  GOOG   4 accumulating / 2 distributing
                  Berkshire and First Eagle are counted in both rows; Coatue is
                  accumulating in one row and distributing in the other.
        with      GOOGL+GOOG  9 accumulating / 2 distributing / 1 rotating
      It is NOT a join on issuer_id (the CUSIP prefix). 100 prefixes carry more
      than one ticker; 34 do even among long common lines, and only 10 of those
      34 hold a share-class twin. The other 24: 18 fund trusts (iShares Trust
      464287 alone is 8 unrelated funds), 5 replaced CUSIPs, 1 rights line.
      Prefix 531229 would also fuse two different Liberty tracking stocks.
  rotating       A filer that added one class and cut the other is on NEITHER
      side. It is listed separately with both legs and the net change in
      equivalent shares (BRK/A = 1,500 BRK/B; the other pairs 1:1). The net is
      information, not a verdict. Rotators per period: 1 at 2026-06-30
      (Coatue), 3 at 2026-03-31, 3, 1, 1, 3, 1, 1 going back to 2024-09-30; 11
      of the 14 are Alphabet, 1 is Soros between FWONA and FWONK. Himalaya's two
      (2025-12-31 and 2026-03-31) net to exactly 0 shares: the two class counts
      trade places and trade back. The other 2 are Berkshire at 2025-12-31 and
      are NOT trades: the same shares under a new CUSIP (trap 5), net 0. The
      flag keeps them off the accumulating side; without it they are "new".
  --floor-fix    See the floor trap below. Not a house behaviour either.

EXCLUDED, AND WHY (the run prints the tally)
  13F holdings carry no date_flag or value_flag; nothing here is excluded by a
  flag. What is left out, at 2026-06-30:
    5 tracked filers        no filing at one of the two quarter ends
    81 + 81 lines           no ticker (current $32.8B, prior $26.5B): cannot be
                            keyed across filers
    671 + 674 lines         under a filer's floor ($5.4B, $5.0B)
    225 positions           held flat: no flow
    598 issuer rows         a flow on one side only (in the CSV, opposed=False)
    0 filings               superseded: no filer has two filings for one period

TRAPS (2026-06-30 unless a period is given)
  1. Accumulating a PUT is a bearish act. It sits on the "accumulating" side of
     its own PUT row. 3 of the 73 opposed rows are PUT rows, 0 are CALL rows. 25
     filer-tickers have flows in more than one instrument at once; Soros is
     "distributing" NVDA shares and "accumulating" NVDA puts.
  2. Floor crossings read as trades. A line that moves across a filer's 0.25%
     floor is "new" or "exited" although it was held throughout. 10 flows here,
     54 across the 8 usable periods. 2 of the 10 have the wrong DIRECTION:
     Horizon Kinetics PAG and First Eagle CCU are "new, accumulating" on a
     falling share count. --floor-fix judges these on shares: 2 flip, 8 are
     relabelled, the opposed count stays 73.
  3. One line moves First Eagle's floor. Its 2026-03-31 filing carries
     "MSTR 0 12/01/29" at $16.59B (par 20,000,000,000); the 2026-06-30 filing
     carries the same note at $17.2M (par 20,000,000). That one line is 22% of
     the 2026-03-31 book and lifts the floor from about $147M to $189M, pushing
     4 lines ($667M) under it. At 2026-06-30 the note itself reads as a $16.59B
     "exited" line. Whether the filer or the ingest is wrong: not established.
  4. Lines with no ticker are invisible. 130 lines, $33.3B, 4.3% of all
     2026-06-30 value. 119 of them have a letter-prefixed (foreign-domicile)
     CUSIP: Chubb $11.7B at Berkshire, Eaton, Ferrovial, Nebius (two filers),
     ASML, Medtronic, Linde, Spotify. No opposed row can form on these names.
  5. A replaced CUSIP reads as an exit plus a new position when its ticker
     string also changes: Soros "exited HONGBP" and is "new" in HON at
     2026-06-30 (358,052 -> 174,025 shares). 13 candidates in 8 periods, printed
     under CAVEATS and marked *. Three are Berkshire at 2025-12-31 with share
     counts unchanged (FWONKUSD -> FWONK, LLYVK* -> LLYVK, LLYVA* -> LLYVA):
     $1.6B of "new" that is not buying, and the FWONK one sits on the
     accumulating side of an opposed row. At least one candidate (Soros IVV ->
     IYR, 2024-12-31) is two different funds. Nothing is corrected, and a
     position whose CUSIP prefix AND ticker root both changed is not caught.
     When the CUSIP changes and the ticker string does not, the line is "new"
     whatever the share count did. 2 cases, both Barrick ("B") at 2025-06-30:
     First Eagle is "new, accumulating" on 45,617,874 -> 39,483,961 shares, so
     B shows no opposition against Kopernik (5,044,236 -> 5,128,287).
  6. Everything long is SH. Convertible notes, preferreds, units and warrants
     are SH lines under their own ticker string ("MSTR 0 12/01/29"). 34 of the
     906 flows; none sits in an opposed row.
  7. Three tracked filers are companies marking balance-sheet stakes, not
     managers: Alphabet, Amazon, NVIDIA. 3 of the 223 side entries.
     --discretionary-only drops them: 70 rows.
  8. An exit may already have reversed, and a trim is still a holding. A 13F
     shows long positions and listed options only, 36-45 days old on arrival.
  9. 74 ticker strings (233 lines) end in *, USD, GBP or EUR: the CUSIP lookup
     found no US listing and kept another venue's symbol (HONGBP, DFSEUR,
     TMHC*). When every filer reports the same CUSIP the row is only
     mislabelled. When one filer reports a different CUSIP for the same share
     it is keyed apart: Berkshire's Formula One C shares are FWONKUSD through
     2025-09-30 while Horizon Kinetics holds FWONK. 10 flows at 2026-06-30 are
     on such strings; none sits in an opposed row.

OUTPUT
  stdout: the issuer table, the filer-vs-filer pairs, the rotators, the
  excluded tally. --csv writes every issuer x instrument with a flow (671 rows
  at 2026-06-30; filter opposed=True for the 73), one row per (issuer,
  accumulator, distributor) in .pairs.csv (205), and .rotating.csv.
"""
import argparse
import csv
import datetime as dt
import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict

try:                                    # optional; the recipe never requires it
    import pandas as _pd
except Exception:                       # noqa: BLE001 - any import failure means "no pandas"
    _pd = None

DEFAULT_DB = os.path.expanduser("~/.openclaw/smart_money/smart_money_v0.db")

ACC = "accumulating"
DIS = "distributing"
INSTRUMENT = {"long": "SH", "call": "CALL", "put": "PUT"}
CORPORATE_THESES = frozenset(("corporate_strategic",))   # same constant as queries.py

# --------------------------------------------------------------------------- twins
# Share classes of ONE issuer, merged only under --union-twins.
# (label, {ticker as spelled in thirteenf_holdings: economic shares per share}, source)
# The weight converts each class to a common unit so a rotation can be netted:
# one BRK/A is 1,500 BRK/B by charter; every other pair here is 1:1 economically
# and differs only in votes.
# source "order"  = named in ORDER SM-DATA1.
# source "corpus" = found by scanning the mirror for two tickers that share a
#                   6-character CUSIP issuer prefix, are both instrument_class
#                   'common', and are known share classes of one company.
# The list is deliberately explicit. Do NOT replace it with a join on issuer_id
# (CUSIP prefix): on this corpus that fuses unrelated funds of one trust, common
# with its convertibles, and tracking stocks.
# Three members are OLD SPELLINGS of a class already in its group, not a third
# class. The corpus keys them apart because the ticker string follows the CUSIP:
#   FWONKUSD  CUSIP 531229854, a second Formula One Series C CUSIP that only
#             Berkshire reported, through 2025-09-30 (other filers: FWONK,
#             531229755). At 2025-12-31 Berkshire switches to 531229755 with the
#             same 3,018,555 shares.
#   LLYVK*    CUSIP 531229722 / 531229748, the Liberty Live tracking stock of
#   LLYVA*    Liberty Media through 2025-09-30; from 2025-12-31 the same holders
#             report LLYVK / LLYVA of Liberty Live Holdings (530909). Berkshire's
#             10,917,661 and 4,986,588 shares carry over unchanged.
# Without them --union-twins would miss the LLYVK*/LLYVA* twin entirely and would
# read Berkshire's 2025-12-31 relabel as $1.6B of new buying.
TWIN_GROUPS = (
    ("GOOGL+GOOG", {"GOOGL": 1, "GOOG": 1}, "order"),
    ("BRK/A+BRK/B", {"BRK/A": 1500, "BRK/B": 1}, "order"),
    ("FOXA+FOX", {"FOXA": 1, "FOX": 1}, "order"),
    ("NWSA+NWS", {"NWSA": 1, "NWS": 1}, "corpus"),
    ("Z+ZG", {"Z": 1, "ZG": 1}, "corpus"),
    ("LEN+LEN/B", {"LEN": 1, "LEN/B": 1}, "corpus"),
    ("LBRDK+LBRDA", {"LBRDK": 1, "LBRDA": 1}, "corpus"),
    ("BATRK+BATRA", {"BATRK": 1, "BATRA": 1}, "corpus"),
    ("FWONK+FWONA", {"FWONK": 1, "FWONA": 1, "FWONKUSD": 1}, "corpus"),
    ("LLYVK+LLYVA", {"LLYVK": 1, "LLYVA": 1, "LLYVK*": 1, "LLYVA*": 1}, "corpus"),
)
TWIN_OF = {t: label for label, members, _src in TWIN_GROUPS for t in members}
TWIN_WEIGHT = {t: w for _label, members, _src in TWIN_GROUPS for t, w in members.items()}

# --------------------------------------------------------------------------- SQL
# The 13F clock of the corpus: the newest filing date held, and the last day the
# 13F ingest wrote a row. Used only to decide which period is COMPLETE.
SQL_CLOCK = """
SELECT MAX(filed_date)                                   AS newest_filed,
       date(MAX(ingested_at_unix), 'unixepoch')          AS last_ingest_day
FROM thirteenf_holdings
"""

# One line per filing. CIKs are bare in this table and zero-padded in the
# registry, so every CIK is cast to INTEGER before it is compared.
SQL_FILINGS = """
SELECT CAST(cik AS INTEGER) AS cik_i,
       period,
       accession,
       filed_date,
       COUNT(*)             AS n_rows
FROM thirteenf_holdings
WHERE period IS NOT NULL
GROUP BY cik_i, period, accession, filed_date
ORDER BY cik_i, period DESC, filed_date DESC, accession DESC
"""

# Every holding line of one filing. value_usd = value * value_scale (US dollars,
# scale resolved per filing at ingest). No row is filtered here: the book total
# that the floor is measured against must be the whole book.
SQL_HOLDINGS = """
SELECT cusip,
       UPPER(COALESCE(ticker, ''))  AS ticker,
       issuer,
       put_call,
       COALESCE(shares, 0)          AS shares,
       COALESCE(value_usd, 0)       AS value_usd,
       instrument_class
FROM thirteenf_holdings
WHERE CAST(cik AS INTEGER) = ?
  AND period = ?
  AND accession = ?
ORDER BY rowid
"""

SQL_HOLDING_CIKS = "SELECT DISTINCT CAST(cik AS INTEGER) FROM thirteenf_holdings"

# First and last period each CUSIP appears in, across every filer. Used only to
# flag a CUSIP succession (old CUSIP stops, new one starts, same quarter).
SQL_CUSIP_SPAN = """
SELECT cusip, MIN(period) AS first_period, MAX(period) AS last_period
FROM thirteenf_holdings
GROUP BY cusip
"""


# --------------------------------------------------------------------------- helpers
def connect_ro(db_path):
    """Read-only connection. mode=ro makes any write fail inside SQLite."""
    return sqlite3.connect("file:{}?mode=ro".format(db_path), uri=True)


def is_quarter_end(p):
    try:
        d = dt.date.fromisoformat(p)
    except (TypeError, ValueError):
        return False
    return (d.month, d.day) in ((3, 31), (6, 30), (9, 30), (12, 31))


def prev_quarter_end(p):
    d = dt.date.fromisoformat(p)
    y, m = d.year, d.month - 3
    if m <= 0:
        y, m = y - 1, m + 12
    first_of_next = dt.date(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1)
    return (first_of_next - dt.timedelta(days=1)).isoformat()


def filing_deadline(p):
    """Quarter end + 45 calendar days, rolled past a weekend. Federal holidays
    are not modelled (2025-12-31 was really due 2026-02-17, not 02-16)."""
    d = dt.date.fromisoformat(p) + dt.timedelta(days=45)
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d.isoformat()


def default_registry_path(db_path):
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), "analysis",
                        "registry.json")


def load_registry(path):
    """Tracked filers = registry entries with role 'manager_13f' and a CIK, in
    registry order. Same rule as queries._tracked_filers. Returns (filers, as_of)."""
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    out = []
    for e in data.get("entries", []):
        if e.get("role") == "manager_13f" and e.get("cik"):
            out.append({"cik": int(str(e["cik"]).strip()), "name": e.get("name"),
                        "thesis": e.get("thesis"),
                        "floor": e.get("position_floor_pct") or None})
    return out, data.get("as_of")


_NAME_STOP = {"LLC", "LP", "L.P.", "INC", "CORP", "LTD", "MANAGEMENT", "CAPITAL",
              "ASSET", "INVESTMENT", "INVESTORS", "ADVISORS", "PARTNERS", "FAMILY",
              "COM", "GP", "GROUP"}


def short_names(filers):
    """Readable short labels for the printed table; falls back to the full name
    if two filers would collide."""
    out = {}
    for f in filers:
        words = []
        for w in (f["name"] or "").split("(")[0].replace(",", " ").split():
            if w.upper() in _NAME_STOP:
                break
            words.append(w)
        out[f["cik"]] = " ".join(words) or f["name"]
    if len(set(out.values())) < len(out):
        return {f["cik"]: f["name"] for f in filers}
    return out


def money(v):
    v = float(v or 0)
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return "${:.1f}{}".format(v / div, suf)
    return "${:.0f}".format(v)


# --------------------------------------------------------------------------- periods
def filing_index(con):
    """{cik_int: {period: (accession, filed_date, n_rows)}} keeping, for each
    filer-period, the filing with the newest filed_date. Also returns how many
    older filings for the same filer-period were set aside."""
    idx = defaultdict(dict)
    superseded = 0
    for cik_i, period, accession, filed, n_rows in con.execute(SQL_FILINGS):
        if period in idx[cik_i]:
            superseded += 1
            continue
        idx[cik_i][period] = (accession, filed, n_rows)
    return idx, superseded


def corpus_clock(con):
    newest_filed, last_ingest = con.execute(SQL_CLOCK).fetchone()
    return max(x for x in (newest_filed, last_ingest) if x)


def newest_complete_period(idx, clock):
    """COMPLETE = the filing deadline for the period (quarter end + 45 days,
    rolled past a weekend) is on or before the corpus 13F clock. It does NOT mean
    every tracked filer filed; the header prints how many did."""
    periods = sorted({p for per in idx.values() for p in per}, reverse=True)
    for p in periods:
        if is_quarter_end(p) and filing_deadline(p) <= clock:
            return p
    return None


# --------------------------------------------------------------------------- flows
def _load(con, cik_i, period, accession, floor):
    """All lines of one filing, each stamped with pct of book and below-floor.
    The book is the whole filing (options notional and unmapped lines included),
    which is what the house measures the floor against."""
    rows = []
    for cusip, ticker, issuer, pc, shares, value, iclass in con.execute(
            SQL_HOLDINGS, (cik_i, period, accession)):
        rows.append({"cusip": cusip, "ticker": ticker, "issuer": issuer,
                     "put_call": pc or "long", "shares": shares, "value": value,
                     "iclass": iclass})
    book = sum(r["value"] for r in rows)
    for r in rows:
        pct = (100.0 * r["value"] / book) if book else None
        r["below"] = bool(floor and pct is not None and pct < floor)
    return rows


def _fkey(r):
    return (r["ticker"], INSTRUMENT.get(r["put_call"], r["put_call"].upper()))


def filer_flows(cur_rows, prior_rows, floor_fix=False):
    """The house rule (queries._manager_flow), one filer, one pair of periods.

    Returns (flows, tally). flows = {(ticker, instrument): flow dict}.
      new      in the current filing, not in the prior one      -> accumulating
      added    in both, current size > prior size               -> accumulating
      trimmed  in both, current size < prior size               -> distributing
      exited   in the prior filing, ticker+instrument gone now  -> distributing
      flat     in both, same size                               -> no flow
    Lines are matched across periods on (cusip, put_call). Size is SHARES when
    both periods report a non-zero share count, else dollars. Below-floor lines
    are removed from EACH period separately before matching (so a line that
    crosses the floor reads as new or exited). Lines with no ticker cannot be
    keyed across filers and are counted, not used.

    floor_fix=True keeps a below-floor line when the same line clears the floor
    in the other period, so a floor crossing is judged on its real share change.
    """
    t = Counter()
    cur_all = {(r["cusip"], r["put_call"]): r for r in cur_rows}
    prior_all = {(r["cusip"], r["put_call"]): r for r in prior_rows}
    below_cur = {k for k, r in cur_all.items() if r["below"]}
    below_pri = {k for k, r in prior_all.items() if r["below"]}
    if floor_fix:
        keep_cur = {k for k in below_cur if k in prior_all and k not in below_pri}
        keep_pri = {k for k in below_pri if k in cur_all and k not in below_cur}
        below_cur -= keep_cur
        below_pri -= keep_pri
        t["floor_rescued"] += len(keep_cur) + len(keep_pri)
    for tag, allrows, below in (("cur", cur_all, below_cur),
                                ("prior", prior_all, below_pri)):
        for k in below:
            t["below_floor_rows_" + tag] += 1
            t["below_floor_usd_" + tag] += allrows[k]["value"]
    cur = {k: r for k, r in cur_all.items() if k not in below_cur}
    prior = {k: r for k, r in prior_all.items() if k not in below_pri}
    cur_keys = {_fkey(r) for r in cur.values() if r["ticker"]}
    prior_by_key = defaultdict(list)
    for r in prior_all.values():
        if r["ticker"]:
            prior_by_key[_fkey(r)].append(r)
    flows = {}
    for k, r in cur.items():
        key = _fkey(r)
        if not key[0]:
            t["no_ticker_rows_cur"] += 1
            t["no_ticker_usd_cur"] += r["value"]
            continue
        if key in flows:
            t["key_collisions"] += 1          # two CUSIPs, one ticker+instrument
        pv = prior.get(k)
        if pv is None:
            note, prior_sh = "", 0
            held = prior_all.get(k)
            if held is not None:              # held last quarter, under the floor
                note, prior_sh = "floor_cross", held["shares"]
                t["floor_cross_new"] += 1
            elif prior_by_key.get(key):       # same ticker, different CUSIP
                note = "cusip_change"
                prior_sh = sum(x["shares"] for x in prior_by_key[key])
                t["cusip_change_new"] += 1
            flows[key] = {"direction": ACC, "action": "new", "value": r["value"],
                          "shares_cur": r["shares"], "shares_prior": prior_sh,
                          "cusip": r["cusip"], "issuer": r["issuer"], "note": note,
                          "iclass": r["iclass"]}
            continue
        if r["shares"] and pv["shares"]:
            cm, pm = r["shares"], pv["shares"]
        else:
            cm, pm = r["value"], pv["value"]
            if cm != pm:
                t["judged_on_value"] += 1
        if cm == pm:
            t["flat"] += 1
            continue
        flows[key] = {"direction": ACC if cm > pm else DIS,
                      "action": "added" if cm > pm else "trimmed",
                      "value": r["value"], "shares_cur": r["shares"],
                      "shares_prior": pv["shares"], "cusip": r["cusip"],
                      "issuer": r["issuer"], "note": "", "iclass": r["iclass"]}
    for k, r in prior.items():
        key = _fkey(r)
        if not key[0]:
            t["no_ticker_rows_prior"] += 1
            t["no_ticker_usd_prior"] += r["value"]
            continue
        if key in cur_keys or key in flows:
            continue
        note, cur_sh = "", 0
        still = cur_all.get(k)
        if still is not None:                 # still held, now under the floor
            note, cur_sh = "floor_cross", still["shares"]
            t["floor_cross_exit"] += 1
        flows[key] = {"direction": DIS, "action": "exited", "value": r["value"],
                      "shares_cur": cur_sh, "shares_prior": r["shares"],
                      "cusip": r["cusip"], "issuer": r["issuer"], "note": note,
                      "iclass": r["iclass"]}
    return flows, t


_RESOLVER_SUFFIX = re.compile(r"^([A-Z/]+?)\d?(\*|USD|GBP|EUR)$")


def _ticker_root(t):
    """The ticker without a resolver suffix. A CUSIP that no longer trades comes
    back from the CUSIP->ticker resolver with '*', 'USD', 'GBP' or 'EUR' tacked
    on, sometimes after a digit: HONGBP, LRCXEUR, CCL1EUR, FWONKUSD, LLYVK*.
    74 such strings on the mirror (233 lines)."""
    m = _RESOLVER_SUFFIX.match(t)
    return m.group(1) if m else t


def _successions(flows, cur_period, span):
    """(exited ticker, new ticker, instrument) inside one filer that MAY be one
    position under a replaced CUSIP (a split, a reorganisation, or a filer
    catching up with a CUSIP change). The flow rule reads these as an exit plus
    a new buy. Both legs common shares, and EITHER
      a. the two tickers are the same string once a resolver suffix is removed
         (OREUR -> OR, FWONKUSD -> FWONK, LLYVK* -> LLYVK), OR
      b. same 6-character CUSIP issuer prefix, and across the whole corpus the
         old CUSIP is never seen from this period on while the new one is never
         seen before it (CCIVGBP -> LCID).
    A candidate list for review, not a finding: under (b) two funds of one trust
    can pass. Positions that changed BOTH prefix and ticker root are not caught.
    Marks both flows; changes no direction."""
    out = []
    exits = [(k, f) for k, f in flows.items() if f["action"] == "exited"]
    news = [(k, f) for k, f in flows.items() if f["action"] == "new"]
    for (kt, ki), fe in exits:
        for (nt, ni), fn in news:
            if ki != "SH" or ni != "SH" or kt == nt:
                continue
            if fe["iclass"] != "common" or fn["iclass"] != "common":
                continue
            if _ticker_root(kt) != _ticker_root(nt):       # not (a): test (b)
                if fe["cusip"][:6] != fn["cusip"][:6]:
                    continue
                if TWIN_OF.get(kt) and TWIN_OF.get(kt) == TWIN_OF.get(nt):
                    continue
                old_last = span.get(fe["cusip"], (None, None))[1]
                new_first = span.get(fn["cusip"], (None, None))[0]
                if not (old_last and new_first and old_last < cur_period <= new_first):
                    continue
            for f in (fe, fn):
                f["note"] = ",".join(x for x in (f["note"], "succession") if x)
            out.append((kt, nt, ki))
    return out


# --------------------------------------------------------------------------- build
def build(con, registry_path, period=None, align="quarter", union_twins=False,
          min_side=1, discretionary_only=False, as_of=None, instrument="ALL",
          floor_fix=False):
    """Compute everything. Returns a dict with keys: meta, rows (one per issuer x
    instrument that has any flow; row['opposed'] says whether both sides reach
    min_side), pairs (filer-vs-filer), rotating, excluded, caveats, filers."""
    if align not in ("quarter", "own", "house"):
        raise ValueError("align must be quarter, own or house")
    if min_side < 1:
        raise ValueError("--min-side must be 1 or more (got {})".format(min_side))
    if as_of is not None:
        try:
            as_of = dt.date.fromisoformat(as_of).isoformat()
        except (TypeError, ValueError):
            raise ValueError("--as-of must be a date YYYY-MM-DD (got {!r})".format(as_of))
    filers, reg_as_of = load_registry(registry_path)
    if not filers:
        raise ValueError("{} has no entry with role 'manager_13f' and a cik: no "
                         "tracked filers, nothing to compare".format(registry_path))
    short = short_names(filers)
    idx, superseded = filing_index(con)
    span = {c: (a, b) for c, a, b in con.execute(SQL_CUSIP_SPAN)}
    clock = as_of or corpus_clock(con)
    if period is None:
        period = newest_complete_period(idx, clock)
        if period is None and align != "house":
            raise ValueError("no period in the corpus is complete as of {} (its filing "
                             "deadline would have to be on or before that day). Pass "
                             "--period.".format(clock))
    all_periods = sorted({p for per in idx.values() for p in per}, reverse=True)
    if align != "house":
        if period not in all_periods:
            raise ValueError("period {} is not in the corpus; available: {}".format(
                period, ", ".join(all_periods)))
        if not is_quarter_end(period):
            raise ValueError("period {} is not a quarter end".format(period))
    prior_q = prev_quarter_end(period) if period else None

    excluded = Counter()
    not_compared = []                       # (short name, reason)
    caveats = Counter()
    successions = []
    compared = []
    tracked_ciks = {f["cik"] for f in filers}
    excluded["holding_ciks_not_tracked"] = sum(
        1 for (c,) in con.execute(SQL_HOLDING_CIKS) if c not in tracked_ciks)
    excluded["superseded_filings"] = superseded

    for f in filers:
        if discretionary_only and f["thesis"] in CORPORATE_THESES:
            not_compared.append((short[f["cik"]], "corporate_strategic (dropped by "
                                                  "--discretionary-only)"))
            continue
        per = idx.get(f["cik"], {})
        own = sorted(per, reverse=True)
        if align == "house":
            cur_p = own[0] if own else None
            prior_p = own[1] if len(own) > 1 else None
            if cur_p is None or prior_p is None:
                not_compared.append((short[f["cik"]], "fewer than two filings"))
                continue
        else:
            if period not in per:
                last = own[0] if own else "none"
                not_compared.append((short[f["cik"]],
                                     "no filing for {} (newest held: {})".format(
                                         period, last)))
                continue
            cur_p = period
            older = [p for p in own if p < period]
            if align == "own":
                prior_p = older[0] if older else None
                if prior_p is None:
                    not_compared.append((short[f["cik"]], "no earlier filing"))
                    continue
            else:
                prior_p = prior_q if prior_q in per else None
                if prior_p is None:
                    not_compared.append((short[f["cik"]],
                                         "no filing for {} (previous held: {})".format(
                                             prior_q, older[0] if older else "none")))
                    continue
        cur_acc, cur_filed, _n = per[cur_p]
        prior_acc, _prior_filed, _n2 = per[prior_p]
        cur_rows = _load(con, f["cik"], cur_p, cur_acc, f["floor"])
        prior_rows = _load(con, f["cik"], prior_p, prior_acc, f["floor"])
        flows, t = filer_flows(cur_rows, prior_rows, floor_fix=floor_fix)
        for k, v in t.items():
            if k in ("judged_on_value", "floor_cross_new", "floor_cross_exit",
                     "cusip_change_new", "key_collisions", "floor_rescued"):
                caveats[k] += v
            else:
                excluded[k] += v
        for a, b, inst in _successions(flows, cur_p, span):
            successions.append((short[f["cik"]], a, b, inst))
        compared.append({"cik": f["cik"], "name": f["name"], "short": short[f["cik"]],
                         "thesis": f["thesis"], "period": cur_p, "prior": prior_p,
                         "filed_date": cur_filed, "flows": flows})

    # ---- one vote per filer per issuer x instrument
    agg = defaultdict(lambda: {"acc": [], "dis": [], "rot": []})
    rotating = []
    for fr in compared:
        groups = defaultdict(list)
        for (ticker, inst), fl in fr["flows"].items():
            label = TWIN_OF.get(ticker, ticker) if union_twins else ticker
            groups[(label, inst)].append((ticker, fl))
        for (label, inst), legs in groups.items():
            base = {"filer": fr["name"], "short": fr["short"], "cik": fr["cik"],
                    "thesis": fr["thesis"], "period": fr["period"],
                    "prior_period": fr["prior"], "filed_date": fr["filed_date"]}
            dirs = {fl["direction"] for _t, fl in legs}
            if len(dirs) == 2:                # added one class, cut the other
                net = sum(TWIN_WEIGHT.get(tk, 1) * (fl["shares_cur"] - fl["shares_prior"])
                          for tk, fl in legs)
                gross = sum(TWIN_WEIGHT.get(tk, 1) * abs(fl["shares_cur"] - fl["shares_prior"])
                            for tk, fl in legs)
                rec = dict(base, issuer=label, instrument=inst,
                           legs="; ".join("{} {} {:,}->{:,}".format(
                               tk, fl["action"], fl["shares_prior"], fl["shares_cur"])
                               for tk, fl in sorted(legs)),
                           net_equiv_shares=net, gross_equiv_shares=gross,
                           value=sum(fl["value"] for _t, fl in legs),
                           # "succession" here = the legs are an exit plus a new
                           # line that may be ONE position under a replaced
                           # CUSIP: a relabel, not a rotation.
                           note=",".join(sorted({n for _t, fl in legs
                                                 for n in fl["note"].split(",") if n})))
                rotating.append(rec)
                agg[(label, inst)]["rot"].append(rec)
                continue
            legs.sort()
            entry = dict(base,
                         value=sum(fl["value"] for _t, fl in legs),
                         action="+".join(sorted({fl["action"] for _t, fl in legs})),
                         tickers=",".join(tk for tk, _fl in legs),
                         note=",".join(sorted({fl["note"] for _t, fl in legs if fl["note"]})))
            side = "acc" if dirs == {ACC} else "dis"
            agg[(label, inst)][side].append(entry)

    rows, pairs = [], []
    for (label, inst), a in agg.items():
        if instrument != "ALL" and inst != instrument:
            continue
        if not a["acc"] and not a["dis"]:
            continue                         # held only by rotators
        for side in ("acc", "dis"):
            a[side].sort(key=lambda x: -(x["value"] or 0))
        opposed = len(a["acc"]) >= min_side and len(a["dis"]) >= min_side
        known = None
        if opposed:                          # day the min_side-th filer on the
            n = max(min_side, 1) - 1         # slower side reached EDGAR
            known = max(sorted(x["filed_date"] for x in a["acc"])[n],
                        sorted(x["filed_date"] for x in a["dis"])[n])
        row = {"issuer": label, "instrument": inst,
               "n_accumulating": len(a["acc"]), "n_distributing": len(a["dis"]),
               "n_managers": len(a["acc"]) + len(a["dis"]),
               "n_rotating": len(a["rot"]),
               "acc_value_usd": sum(x["value"] or 0 for x in a["acc"]),
               "dis_value_usd": sum(x["value"] or 0 for x in a["dis"]),
               "acc_names": "; ".join(x["filer"] for x in a["acc"]),
               "dis_names": "; ".join(x["filer"] for x in a["dis"]),
               "rot_names": "; ".join(x["filer"] for x in a["rot"]),
               "cross_thesis": bool(a["acc"] and a["dis"]) and not (
                   {x["thesis"] for x in a["acc"]} & {x["thesis"] for x in a["dis"]}),
               "opposed": opposed, "opposed_known_on": known,
               "last_filed": max(x["filed_date"] for x in a["acc"] + a["dis"]),
               "accumulating": a["acc"], "distributing": a["dis"],
               "rotating": a["rot"]}
        rows.append(row)
        if opposed:
            for x in a["acc"]:
                for y in a["dis"]:
                    pairs.append({
                        "issuer": label, "instrument": inst,
                        "acc_filer": x["filer"], "acc_cik": x["cik"],
                        "acc_action": x["action"], "acc_value_usd": x["value"],
                        "acc_period": x["period"], "acc_filed_date": x["filed_date"],
                        "dis_filer": y["filer"], "dis_cik": y["cik"],
                        "dis_action": y["action"], "dis_value_usd": y["value"],
                        "dis_period": y["period"], "dis_filed_date": y["filed_date"],
                        "known_on": max(x["filed_date"], y["filed_date"]),
                        "_a": x["short"], "_d": y["short"]})
    rows.sort(key=lambda r: (not r["opposed"],
                             -min(r["n_accumulating"], r["n_distributing"]),
                             -r["n_managers"],
                             -(r["acc_value_usd"] + r["dis_value_usd"]),
                             r["issuer"], r["instrument"]))
    excluded["one_sided_issuers"] = sum(1 for r in rows if not r["opposed"])
    excluded["rotating_filer_issuers"] = len(rotating)
    caveats["successions"] = len(successions)

    filed = [fr["filed_date"] for fr in compared if fr["period"] == period]
    n_filed_period = sum(1 for f in filers if period in idx.get(f["cik"], {}))
    meta = {"period": period, "prior_period": prior_q, "align": align,
            "union_twins": union_twins, "min_side": min_side, "floor_fix": floor_fix,
            "instrument": instrument, "clock": clock,
            "deadline": filing_deadline(period) if period else None,
            "complete": bool(period) and filing_deadline(period) <= clock,
            "tracked": len(filers), "filed_for_period": n_filed_period,
            "compared": len(compared), "registry_as_of": reg_as_of,
            "registry_path": registry_path,
            "filed_min": min(filed) if filed else None,
            "filed_max": max(filed) if filed else None,
            "periods_used": dict(Counter(fr["period"] + " vs " + fr["prior"]
                                         for fr in compared))}
    return {"meta": meta, "rows": rows, "pairs": pairs, "rotating": rotating,
            "excluded": excluded, "caveats": caveats, "not_compared": not_compared,
            "successions": successions, "filers": compared}


# --------------------------------------------------------------------------- output
ROW_COLS = ("issuer", "instrument", "n_accumulating", "n_distributing", "n_managers",
            "n_rotating", "opposed", "opposed_known_on", "last_filed", "cross_thesis",
            "acc_value_usd", "dis_value_usd", "acc_names", "dis_names", "rot_names")
PAIR_COLS = ("issuer", "instrument", "acc_filer", "acc_cik", "acc_action",
             "acc_value_usd", "acc_period", "acc_filed_date", "dis_filer", "dis_cik",
             "dis_action", "dis_value_usd", "dis_period", "dis_filed_date", "known_on")
ROT_COLS = ("issuer", "instrument", "filer", "cik", "period", "prior_period",
            "filed_date", "legs", "net_equiv_shares", "gross_equiv_shares", "value",
            "note")


def _side(entries, show_period):
    out = []
    for x in entries:
        tag = x["action"] + ("*" if x["note"] else "")
        if show_period:
            tag += " " + x["period"]
        out.append("{} ({})".format(x["short"], tag))
    return ", ".join(out) or "-"


def print_report(res, top=15, out=sys.stdout):
    m = res["meta"]
    w = out.write
    w("OPPOSED PAIRS - 13F tracked filers\n")
    if m["align"] == "house":
        w("  alignment : house - each filer's own newest period vs its own previous\n")
        for k, v in sorted(m["periods_used"].items(), reverse=True):
            w("              {} : {} filers\n".format(k, v))
    else:
        w("  period    : {} vs {}  (align={})\n".format(
            m["period"], m["prior_period"] if m["align"] == "quarter"
            else "each filer's previous filing", m["align"]))
        w("  complete  : {} - deadline {} vs corpus 13F clock {}; {} of {} tracked "
          "filers filed\n".format("yes" if m["complete"] else "NO", m["deadline"],
                                  m["clock"], m["filed_for_period"], m["tracked"]))
        w("  known     : these filings reached EDGAR {} to {}. Use filed_date, not "
          "period, as the date the market knew.\n".format(m["filed_min"], m["filed_max"]))
    w("  compared  : {} filers   union_twins={}   floor_fix={}   min_side={}   "
      "instrument={}\n".format(
          m["compared"], "on" if m["union_twins"] else "off",
          "on" if m["floor_fix"] else "off", m["min_side"], m["instrument"]))
    w("  registry  : {} (as_of {})\n".format(m["registry_path"], m["registry_as_of"]))
    w("  value     : US dollars (value_usd). Current-period value; an exit shows "
      "the prior-period value.\n\n")

    opp = [r for r in res["rows"] if r["opposed"]]
    show_period = m["align"] == "house"
    w("ISSUERS WITH BOTH SIDES: {}   (showing {})\n".format(
        len(opp), len(opp) if not top else min(top, len(opp))))
    w("  {:<14}{:<5}{:>4}{:>4}  {:<10}  {:>8} {:>8}\n".format(
        "issuer", "inst", "acc", "dis", "known_on", "acc_usd", "dis_usd"))
    for r in (opp[:top] if top else opp):
        w("  {:<14}{:<5}{:>4}{:>4}  {:<10}  {:>8} {:>8}{}\n".format(
            r["issuer"][:13], r["instrument"], r["n_accumulating"], r["n_distributing"],
            r["opposed_known_on"], money(r["acc_value_usd"]), money(r["dis_value_usd"]),
            "  x-thesis" if r["cross_thesis"] else ""))
        w("      ACC: {}\n".format(_side(r["accumulating"], show_period)))
        w("      DIS: {}\n".format(_side(r["distributing"], show_period)))
        if r["rotating"]:
            w("      ROT: {}\n".format(", ".join(x["short"] for x in r["rotating"])))
    w("  (* after an action = flagged under CAVEATS: floor crossing or CUSIP change)\n\n")

    fp = defaultdict(lambda: defaultdict(list))
    for p in res["pairs"]:
        key = tuple(sorted((p["_a"], p["_d"])))
        fp[key][p["_a"]].append(p["issuer"] + ("" if p["instrument"] == "SH"
                                                else " " + p["instrument"]))
    ranked = sorted(fp.items(), key=lambda kv: (-sum(len(v) for v in kv[1].values()),
                                                kv[0]))
    w("FILER-VS-FILER PAIRS: {} (issuer, accumulator, distributor) rows across {} "
      "filer pairs   (showing {})\n".format(
          len(res["pairs"]), len(ranked),
          len(ranked) if not top else min(top, len(ranked))))
    for (a, b), d in (ranked[:top] if top else ranked):
        w("  {} vs {}: {} issuers\n".format(a, b, sum(len(v) for v in d.values())))
        for buyer, seller in ((a, b), (b, a)):
            if d.get(buyer):
                w("      {} accumulating / {} distributing: {}\n".format(
                    buyer, seller, ", ".join(sorted(d[buyer]))))
    w("\n")

    w("ROTATING BETWEEN SHARE CLASSES (on neither side): {}\n".format(
        len(res["rotating"])))
    if not m["union_twins"]:
        w("  not computed - run with --union-twins\n")
    for x in res["rotating"]:
        w("  {:<12}{:<5}{:<22} {}   net {:+,} equiv shares (gross {:,}){}\n".format(
            x["issuer"], x["instrument"], x["short"][:21], x["legs"],
            x["net_equiv_shares"], x["gross_equiv_shares"],
            "   * may be one position under a replaced CUSIP - see CAVEATS"
            if "succession" in x["note"] else ""))
    w("\n")

    e, c = res["excluded"], res["caveats"]
    w("EXCLUDED\n")
    w("  tracked filers not compared            : {}\n".format(len(res["not_compared"])))
    for name, why in res["not_compared"]:
        w("      {} - {}\n".format(name, why))
    w("  lines with no ticker, current period   : {:>5}  {}\n".format(
        e["no_ticker_rows_cur"], money(e["no_ticker_usd_cur"])))
    w("  lines with no ticker, prior period     : {:>5}  {}\n".format(
        e["no_ticker_rows_prior"], money(e["no_ticker_usd_prior"])))
    w("  lines under a filer's floor, current   : {:>5}  {}\n".format(
        e["below_floor_rows_cur"], money(e["below_floor_usd_cur"])))
    w("  lines under a filer's floor, prior     : {:>5}  {}\n".format(
        e["below_floor_rows_prior"], money(e["below_floor_usd_prior"])))
    w("  positions held flat (no flow)          : {:>5}\n".format(e["flat"]))
    w("  issuers with one side only             : {:>5}\n".format(e["one_sided_issuers"]))
    w("  filer-issuers set aside as rotating    : {:>5}\n".format(
        e["rotating_filer_issuers"]))
    w("  older filings superseded for a period  : {:>5}\n".format(e["superseded_filings"]))
    w("  CIKs in thirteenf_holdings not tracked : {:>5}\n".format(
        e["holding_ciks_not_tracked"]))
    w("CAVEATS (kept in the table, marked *)\n")
    w("  'new' that was held under the floor    : {:>5}{}\n".format(
        c["floor_cross_new"], "" if m["floor_fix"] or not c["floor_cross_new"]
        else "   <- --floor-fix judges these on shares"))
    w("  'exited' that is still held under floor: {:>5}\n".format(c["floor_cross_exit"]))
    w("  floor crossings judged on shares       : {:>5}{}\n".format(
        c["floor_rescued"], "" if m["floor_fix"] else "   (--floor-fix is off)"))
    w("  'new' that is a CUSIP change, same tkr : {:>5}\n".format(c["cusip_change_new"]))
    w("  exit+new that may be one replaced CUSIP: {:>5}\n".format(c["successions"]))
    for name, a, b, inst in res["successions"]:
        w("      {} - {} exited, {} new ({})\n".format(name, a, b, inst))
    w("  directions judged on dollars not shares: {:>5}\n".format(c["judged_on_value"]))
    w("  ticker+instrument with two CUSIPs      : {:>5}\n".format(c["key_collisions"]))


def write_csvs(res, out_path):
    """OUT = issuer table (every issuer x instrument with a flow; see `opposed`).
    OUT with .pairs.csv / .rotating.csv appended = the other two tables."""
    stem = out_path[:-4] if out_path.lower().endswith(".csv") else out_path
    written = []
    for path, cols, data in ((out_path, ROW_COLS, res["rows"]),
                             (stem + ".pairs.csv", PAIR_COLS, res["pairs"]),
                             (stem + ".rotating.csv", ROT_COLS, res["rotating"])):
        with open(path, "w", newline="", encoding="utf-8") as fh:
            wr = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            wr.writeheader()
            wr.writerows(data)
        written.append((path, len(data)))
    return written


# --------------------------------------------------------------------------- pandas
def opposed_pairs_df(db=DEFAULT_DB, table="issuers", registry=None, **kwargs):
    """DataFrame of one table: 'issuers', 'pairs' or 'rotating'. kwargs go to
    build() (period, align, union_twins, floor_fix, min_side, discretionary_only,
    as_of, instrument). Needs pandas; the command line does not."""
    if _pd is None:
        raise ImportError("pandas is not installed; use build() or the command line")
    con = connect_ro(db)
    try:
        res = build(con, registry or default_registry_path(db), **kwargs)
    finally:
        con.close()
    cols, data = {"issuers": (ROW_COLS, res["rows"]),
                  "pairs": (PAIR_COLS, res["pairs"]),
                  "rotating": (ROT_COLS, res["rotating"])}[table]
    return _pd.DataFrame([{c: r.get(c) for c in cols} for r in data], columns=list(cols))


# --------------------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(
        description="13F opposed-pairs table at a chosen quarter end.")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite path (opened read-only)")
    ap.add_argument("--period", help="quarter end YYYY-MM-DD; default = newest "
                                     "complete period")
    ap.add_argument("--csv", help="write the issuer table here, plus .pairs.csv and "
                                  ".rotating.csv beside it")
    ap.add_argument("--union-twins", action="store_true",
                    help="merge the reviewed share-class twins into one issuer and "
                         "report rotators separately")
    ap.add_argument("--align", choices=("quarter", "own", "house"), default="quarter",
                    help="quarter: prior = the quarter end before --period (default). "
                         "own: prior = the filer's previous filing, any gap. "
                         "house: ignore --period, each filer's newest vs its previous "
                         "(what queries.q_opposed_pairs does)")
    ap.add_argument("--registry", help="registry.json; default <db dir>/analysis/"
                                       "registry.json")
    ap.add_argument("--min-side", type=int, default=1,
                    help="filers needed on EACH side to call it opposed (default 1)")
    ap.add_argument("--instrument", choices=("ALL", "SH", "CALL", "PUT"), default="ALL")
    ap.add_argument("--floor-fix", action="store_true",
                    help="judge a line that crosses a filer's floor on its real share "
                         "change instead of calling it new or exited")
    ap.add_argument("--discretionary-only", action="store_true",
                    help="drop corporate_strategic filers (Alphabet, Amazon, NVIDIA)")
    ap.add_argument("--as-of", help="override the corpus 13F clock (YYYY-MM-DD) used "
                                    "to pick the default period")
    ap.add_argument("--top", type=int, default=15,
                    help="rows to print per section; 0 = all. CSV always has all.")
    a = ap.parse_args(argv)

    if not os.path.exists(a.db):
        sys.exit("database not found at {} - pass --db.".format(a.db))
    registry = a.registry or default_registry_path(a.db)
    if not os.path.exists(registry):
        sys.exit("registry.json not found at {} - it defines which filers are "
                 "tracked and their floors. Pass --registry.".format(registry))
    con = connect_ro(a.db)
    try:
        res = build(con, registry, period=a.period, align=a.align,
                    union_twins=a.union_twins, min_side=a.min_side,
                    discretionary_only=a.discretionary_only, as_of=a.as_of,
                    instrument=a.instrument, floor_fix=a.floor_fix)
    except ValueError as exc:
        sys.exit(str(exc))
    finally:
        con.close()
    print_report(res, top=a.top)
    if a.csv:
        try:
            written = write_csvs(res, a.csv)
        except OSError as exc:
            sys.exit("could not write the CSV: {}".format(exc))
        for path, n in written:
            print("wrote {} rows -> {}".format(n, path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
