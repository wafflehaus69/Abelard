#!/usr/bin/env python3
"""insider_buys.py - discretionary insider buys: entry close vs latest close, by role
and by SMID band.

WHAT IT ANSWERS
  One row per Form 4 purchase (code P) that the database does not mark as a 10b5-1 plan
  trade (plan_flag = 0; weaker than it sounds for about a third of the rows, see ROLE):
  who, role, issuer, ticker, trade date, filing date, shares, the price on the form,
  dollar value, the market close on the trade date, the latest close and its date, and
  the close-to-close return between the two. Then count, median and mean return by role
  and by SMID band.

RUN
  wsl -e python3 insider_buys.py --db /home/wafflehouse/.openclaw/smart_money/smart_money_v0.db
     --since / --until YYYY-MM-DD   trade-date (tx_date) window, inclusive. Default: open.
     --regime watchlist|universal|all            default all
     --csv OUT                                   every kept row, every column
     --limit N                                   rows printed (default 25, 0 = all)
     --readable-boxes-only  --operating-only  --collapse-cofilers   optional filters
     --include-jump-returns  --total-return                         summary switches
  Python: run(db, since=, until=, regime=, ...) -> dict (rows, excluded, marks, by_role,
  by_band). dataframe(...) -> pandas DataFrame when pandas is installed; never required.
  The database is opened mode=ro.

UNITS
  shares: as filed. exec_price, entry_close, latest_close: USD per share. value: USD.
  ret, ret_adj: fraction (0.10 = +10%), trade date to latest close, NOT annualised.
  lag_days (trade to filing) and days_held (entry_date to latest_date): calendar days.
  reporting_cik is zero-padded to 10 digits and issuer_cik is bare, as stored (all 24,308
  code-P rows). Join either to another table on int(cik), never on the text.

Every count below was measured on the mirror of 2026-10-01, no window, regime all.

EXCLUDED - printed as a tally, in this order, each row counted once
  24,308  code-P rows to start from.
     581  plan_flag = 1. Not discretionary.
       0  date_flag IS NOT NULL. The row is off the TRADE clock (a trade dated after its
          own filing, before 1990, unparseable). This is a trade-date analysis, so it
          goes. It is still a valid disclosure on the filing clock. None among code-P
          rows today; the step is there for when one appears.
       0  tx_date unparseable or outside 1990-01-01..tomorrow (the house clamp).
   1,063  placeholder ticker: NONE 774, N/A 282, NA 5, N.A. 2 (149 issuers; 72 of them,
          592 rows, are funds by the house name test; the rest include trusts, credit
          vehicles and companies such as Davey Tree Expert Co). A real
          purchase with no symbol. It cannot be priced or banded, so it is not listed.
          List = house PLACEHOLDER_TICKERS. NA is also Nano Labs' real symbol; none of
          the 5 rows dropped here is Nano Labs (its 142 rows are all sales).
   1,098  the same trade already counted. House rule queries._dedup_amendments, applied
          unchanged: key (reporting_cik, issuer_cik, tx_date, code, shares); the latest
          (filed_date, accession) survives. 89 were under another accession (amendment or
          re-report). 1,009 were inside the survivor's own filing, and 495 of those carry
          a DIFFERENT price: likely separate fills of the same size on one day, collapsed
          by the house rule. The key has no price in it. dup_collapsed counts them.
      20  shares NULL or <= 0. Nothing was bought.
  21,546  kept (universal 21,374, watchlist 172).

KEPT BUT MARKED - printed as a second tally
  value_flag IS NOT NULL (171). The parse-time guard judged shares x price not to be a
     dollar figure (price_vs_close 155, price_over_max 9, value_denominated 7) and set
     value to NULL. Shares and price are as filed. The row is kept and priced; it is left
     out of every dollar total. value_note = 'value_flag:<reason>'.
  No usable exec price (263): price NULL or 0. Same treatment. value_note = no_exec_price.
  House read-time value markers (281): value above $1bn (8), price 10x off the ticker's
     other filers (25), and every other row of a filer who tripped one on that ticker
     (248, house contaminated_filers; a value_flag row can trip it too, 24 do). value_flag
     does NOT catch these. They are $35.5bn of the $57.3bn that value_flag alone would
     let through (62%); 8 rows above $1bn (SVRE 7, MRUS 1) are $31.0bn of it. Left out
     of dollar totals, value still shown in the CSV. What is left, $21,815,837,406 on
     20,831 rows, equals house clean_subset(drop_funds=False, collapse_cofiling=False)
     on the same rows.
  date_subclass = non_trading_day (84). Kept. A mark, not a quarantine. entry_close is
     the last session ON OR BEFORE tx_date (TSM dated Labor Day 2026-09-07 -> the
     2026-09-04 close); entry_date says which. 75 of the 84 have a price.
  no_price_series (623): the ticker has no rows in prices. No close, no return. Never
     imputed.
  no_entry_close (775): a series exists but has no close within 7 days on or before
     tx_date. In 757 the series begins after the trade (112 within a week of it, 99
     more than a year later; new listing or short price history, not separated). In
     the other 18 the last close before the trade is more than 7 days old. No return.
     latest_close is still shown where the series has one.
  no_later_close (1): the entry close is the series' last close, so there is nothing
     later to compare with (IVYIX: trade 2025-12-22, series ends 2025-12-16).
  role NULL -> not_recorded (6,616). See ROLE.
  unbanded (14,007). See BAND.
  lag_days > 365 (304): late filing or a mistyped year. date_flag catches a trade dated
     AFTER its filing or before 1990, not one dated too early. LEE dated 2006-06-12 on
     a 2026-06-01 filing is one.
  fund issuer (2,077) and co-filed block (745): house labels, kept. See the filters.

THE RETURN, AND WHY IT IS CLOSE TO CLOSE
  ret = latest_close / entry_close - 1, both from prices.close.
  prices.close is split-adjusted (NVDA 2024-06-06 is 121.00, not 1,210). exec_price is
  the nominal price on the form. Across a split the two are on different share bases,
  so exec-price-vs-latest-close is wrong. Measured: exec_price and entry_close differ by
  more than 1.5x on 1,328 of 19,986 priced rows and by more than 10x on 351; the
  exec-price return differs from ret by more than 20 points on 1,787 rows. Not all of
  that is splits: code P also covers private placements, IPO allocations, tender offers
  and non-common securities bought away from the market price.
  ret_adj is the same thing on prices.adj_close (dividends too). It is the house's
  market_return_since_trade. ret and ret_adj differ by more than 5 points on 1,439 of
  19,589 rows; medians -0.3% (ret) vs +1.0% (ret_adj).

  TRAP - the price series is not on one share basis. A price row keeps the basis of the
  night it was fetched (fetches run 2026-07-21..2026-10-01) and is not re-adjusted when
  a split comes later. BYND: 3.24 on 2025-07-30 and 91.20 on 2025-07-31, adjacent rows
  from two fetches. 62 tickers in prices have such a break of 2x or more.
     jump_cross_fetch: a one-session close move >= 2x or <= 0.5x between two rows from
       different fetches lies between entry and latest. 17 rows, 5 tickers, their median
       'return' +403%. Withheld from the summary.
     jump_same_fetch: the same size of move inside one fetch. 541 rows, 106 tickers,
       median -33%, mean +31%. One fetch can hold a real crash (MLTX 61.99 -> 6.24 on
       2025-09-29 is in the table, inside one fetch) or a break in what the symbol
       prices (ALP: 0.099 on 2026-09-04, next row 3.71 on 2026-09-10, while three
       insiders paid about $5.1 on 2026-09-09; those three rows show +4,779%). They
       cannot be told apart from prices alone, so they are withheld too. This can
       remove real crashes.
     --include-jump-returns puts both back: ALL median -0.3% -> -0.4%, mean +4.5% ->
       +5.6%. Read the median. A break smaller than 2x (a 3-for-2 split) is not caught.
  TRAP - 'latest' is not one date. 3,083 of 5,270 priced tickers end on 2026-09-30;
  1,423 end in 2026-07. 4,710 of the 19,589 summary returns stop before 2026-09-23.
  latest_date is on every row whose ticker has a price series.
  TRAP - horizons differ row to row (median 196 days) and nothing is benchmark-adjusted.

SMID BAND
  band = market_cap.band: micro < $300M, small < $2B, mid < $10B, large. market_cap
  holds 853 tickers, 678 banded. 7,539 of 21,546 rows (35%) get a band. The rest are
  'unbanded': 11,659 have no market_cap row and 2,348 have a row that says 'unbandable'
  (of the 175 such tickers: 112 no share count, 62 no CIK match, 1 no price). Unbanded
  is NOT small.
  TRAP - the band is the cap on the day the row was computed (2026-07-24..2026-10-01;
  marketcap.compute skips a ticker that already has a row), not the size at the trade.
  A stock that fell 80% after the buy sits in a lower band, so band returns carry
  look-ahead.
  TRAP - cap = SEC share count x the quoted price, with no check that the two are on
  the same share basis. TSM is stored at $10.8 trillion (ordinary shares x the ADS
  price). How many bands this moves is not established.

ROLE
  role is written as 'director', 'officer:<free-text title>', '10pct', comma-joined in
  that order. The title is free text (659 distinct titles in the kept rows) and can
  contain commas or the word Director, so role_flags() strips the fixed pieces from both
  ends and never searches the string.
  Bucket, one per row, priority officer > director > 10% owner:
     officer        6,503  (officer 2,434, director+officer 2,057, all three 1,098,
                            officer+10pct 914)
     director       5,355  (director 4,699, director+10pct 656)
     ten_pct_owner  3,072
     not_recorded   6,616  role IS NULL
  TRAP - NULL role is not one thing, and it is not a bucket called 'other'. Two causes,
  which the database cannot tell apart:
   (a) The parser (form4.parse_ownership) reads a box as ticked only when the XML says
       "1". A filing that spells it "true" comes out with no role. Frank Holding,
       Chairman and CEO of FCNCA, is NULL: accession 0001193125-26-262386 says
       isDirector, isOfficer, isTenPercentOwner = true (read on sec.gov).
   (b) The parser never reads the fourth box, isOther. A filer who ticks only 'Other'
       has no role even when the filing is spelled 1/0. Read on sec.gov: Bridgford
       (BRID, 0001493152-26-043437, 'Consultant'), McDougall (BDCO,
       0001437749-26-030276), Rossa (AMFN, 0001079973-26-001308). All three say
       isOther = 1 and aff10b5One = 0, so their plan_flag = 0 WAS read.
  117,782 of 351,519 Form 4 rows have role NULL; all 42,286 rows from filing-agent
  prefix 0001193125 do. Of the 6,616 kept NULL rows, 5,599 come from an accession prefix
  that has no readable role anywhere in the corpus (points to a) and 1,017 from a prefix
  that does (points to b, or to a filer who mixes spellings). That split is a count,
  not a classification.
  TRAP - the same "1" test reads the 10b5-1 box, so under cause (a) plan_flag is 0
  whatever was ticked. Agent 0001193125 shows 0 plan sales in 15,450 sale rows; rows
  with a readable role show 62%. Angeliki Frangou's NMM purchases (101 rows, $8.3M)
  are in this output as discretionary; accessions 0001193125-26-347503 and -340786 say
  aff10b5One = true and footnote a Rule 10b5-1 plan. Six filings were read in all; the
  rest is inferred from the counts. Proxy used here: boxes_read = 0 when role is NULL.
  It is a superset: it also marks cause (b), where the plan box was read correctly.
  --readable-boxes-only drops all 6,616 rows.
  TRAP - rows are not people. STAHL MURRAY is 1,004 rows (RENN Fund 894, HKHC 104,
  MIAX 6; daily lots) and 998 of the 6,503 officer rows; the top 10 filers are 12% of
  rows. The summary prints persons and tickers beside rows.

OPTIONAL FILTERS (off by default; each adds a line to the excluded tally)
  --readable-boxes-only  drop role-NULL rows (6,616).
  --operating-only       drop fund issuers by house _issuer_class (2,077).
  --collapse-cofilers    one row per co-filed block, lowest reporting CIK kept, house
                         clean_subset rule (440 dropped).
  --total-return         summarise ret_adj instead of ret.

REGIME
  universal = every Form 4 filed from 2025-07-23. A trade dated earlier is in it only if
  it was filed late (614 rows), so a window before 2025-07-23 is not that period's
  buying. watchlist = issuer-scoped backfill; 172 kept rows on 20 tickers, trades from
  2021-08-20.

WHERE THIS DIFFERS FROM THE HOUSE (queries.q_insider_trades), deliberately
  Checked against it with scope='all': house 22,591 rows = these 21,546 + 1,025
  placeholder-ticker rows + 20 zero-share rows. ret_adj equals the house return on
  20,147 of 20,147 rows where both exist. Band agrees on all 21,546.
  1. ret is on close; the house return is on adj_close (= ret_adj here).
  2. Entry close at most 7 days old. The house has no limit and reaches further back on
     18 rows. On 14 of them the series ended before the trade, so the house uses one
     close as entry AND latest and reports 0.0%. Here all 18 are no_entry_close.
  3. Entry close is the series' last close (trade dated up to 7 days after the series
     ends): house 0.0%, here no return (1 row).
  4. Placeholder-ticker and zero-share rows are dropped; the house lists them.
  5. No overlay scope and no 90-day default window.
  6. The house refuses to rank any insider return above 100%. No cap here; jump marks
     instead.
  7. Dollar totals follow house clean_subset for value quality only (same total to the
     dollar). Funds and co-filers stay in unless the flags are given. The house FEED
     marks price_vs_ticker_peers on 2 more rows than clean_subset does, because the
     feed takes a peer median from fewer than 3 prices; this follows clean_subset.
  8. A tie inside one filing keeps the lowest tx_index (the house keeps table order).

NOT ESTABLISHED
  How many role-NULL rows are really plan trades, and which of them are unread boxes
  (a) rather than an 'Other' filer (b). Whether a given jump_same_fetch move is real.
  Share-basis breaks under 2x. Whether a stale latest close is a delisting or a stalled
  fetch. Whether the 495 same-filing, different-price rows the house dedup collapses
  are separate fills. How many bands a share-basis mismatch in market_cap moves.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import os
import re
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict

try:                                    # optional; never required
    import pandas as pd
except Exception:                       # noqa: BLE001 - any import failure means "no pandas"
    pd = None

DEFAULT_DB = os.path.expanduser("~/.openclaw/smart_money/smart_money_v0.db")

# ---------------------------------------------------------------- constants
FLOOR = "1990-01-01"                    # house dates.FLOOR
MAX_ENTRY_GAP_DAYS = 7                  # entry close must be within this many calendar days
JUMP_UP, JUMP_DOWN = 2.0, 0.5           # one-session close ratio that withholds a return
STALE_DAYS = 7                          # latest close older than this vs the last SPY session
CALENDAR_TICKER = "SPY"                 # house dates.CALENDAR_TICKER

# house queries.PLACEHOLDER_TICKERS, verbatim
PLACEHOLDER_TICKERS = frozenset(
    {"", "-", "--", ".", "N/A", "NA", "N.A.", "NONE", "NULL", "UNKNOWN", "?"})

# house queries._issuer_class
_FUND_TICKER = re.compile(r"^[A-Z]{4}X$")
_FUND_NAME = re.compile(r"\b(FUNDS?|SERIES|ETF|PORTFOLIOS?)\b", re.I)

# house queries._value_quality / _peer_medians
_NOT_COMMON = re.compile(
    r"\b(PREFERRED|DEPOSITARY|DEPOSITORY|ADS|ADR|SERIES\s+[A-Z0-9]|UNIT|WARRANT|"
    r"RIGHT|DEBENTURE|CLASS\s+[B-Z])\b", re.I)
_PEER_RATIO_MAX = 10.0
_VALUE_REVIEW_MAX = 1e9
_PEER_MIN = 3

BANDS = ("micro", "small", "mid", "large")            # market_cap.band, banded values
BAND_ORDER = BANDS + ("unbanded",)
ROLE_ORDER = ("officer", "director", "ten_pct_owner", "not_recorded")

_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})(?:Z|[+-]\d{2}:\d{2})?")

# ---------------------------------------------------------------- SQL
SQL_BUYS = """
SELECT accession, tx_index, reporting_person, reporting_cik, role, issuer, issuer_cik,
       ticker, security_title, ingest_regime, tx_date, filed_date,
       shares, price, value, ownership_after,
       plan_flag, value_flag, date_flag, date_subclass
FROM   form4_transactions
WHERE  code = 'P'
  AND  (:regime = 'all' OR ingest_regime = :regime)
ORDER  BY accession, tx_index
"""

# One ticker's daily closes from just before its first trade in the result set.
# close = split-adjusted; adj_close = split- and dividend-adjusted (Yahoo v8).
# fetched_at_unix identifies the fetch that wrote the row: one fetch = one share basis.
SQL_SERIES = """
SELECT date, close, adj_close, fetched_at_unix
FROM   prices
WHERE  ticker = ? AND price_type = 'eod' AND date >= ?
ORDER  BY date
"""

SQL_LAST_ROW = """
SELECT date, close, adj_close, fetched_at_unix
FROM   prices
WHERE  ticker = ? AND price_type = 'eod'
ORDER  BY date DESC LIMIT 1
"""

SQL_BANDS = """
SELECT UPPER(ticker), band FROM market_cap
"""

SQL_LAST_SESSION = """
SELECT MAX(date) FROM prices WHERE ticker = ? AND price_type = 'eod'
"""

COLUMNS = (
    "accession", "tx_index", "reporting_person", "reporting_cik", "role", "role_bucket",
    "boxes_read", "issuer", "issuer_cik", "issuer_class", "ticker", "security_title",
    "ingest_regime", "tx_date", "date_subclass", "filed_date", "lag_days",
    "shares", "exec_price", "value", "value_note",
    "entry_date", "entry_close", "latest_date", "latest_close", "days_held",
    "ret", "ret_adj", "ret_note", "band", "cofilers", "dup_collapsed")


# ---------------------------------------------------------------- small helpers
def connect_ro(db_path):
    """Read-only by construction: a write raises at the SQLite layer."""
    return sqlite3.connect("file:{}?mode=ro".format(db_path), uri=True)


def iso10(value):
    """YYYY-MM-DD if the value is a calendar date (EDGAR TZ suffix tolerated), else None.
    Same contract as house dates.iso10."""
    if not isinstance(value, str):
        return None
    m = _ISO.fullmatch(value.strip())
    if not m:
        return None
    try:
        dt.date.fromisoformat(m.group(1))
    except ValueError:
        return None
    return m.group(1)


def role_flags(role):
    """(is_director, is_officer, is_ten_pct, officer_title) from form4_transactions.role.

    The writer builds the string as ",".join of, in this order: 'director',
    'officer:<free-text title>', '10pct'. The title is free text and can itself contain
    commas or the word Director ('officer:EVP/Director of Trust Services'), so the string
    is taken apart from both ENDS, never searched."""
    if not role:
        return False, False, False, None
    s = role
    is_dir = s == "director" or s.startswith("director,")
    if is_dir:
        s = s[len("director"):].lstrip(",")
    is_ten = s == "10pct" or s.endswith(",10pct")
    if is_ten:
        s = s[:-len("10pct")].rstrip(",")
    is_off = s.startswith("officer:")
    return is_dir, is_off, is_ten, (s[len("officer:"):] if is_off else None)


def role_bucket(role):
    """One bucket per row. Priority officer > director > 10% owner. A NULL role is
    'not_recorded': either the boxes were spelled true/false and not read, or only
    'Other' was ticked, which the parser never reads - see the docstring."""
    d, o, t, _ = role_flags(role)
    if o:
        return "officer"
    if d:
        return "director"
    if t:
        return "ten_pct_owner"
    return "not_recorded"


def role_combo(role):
    d, o, t, _ = role_flags(role)
    parts = [n for n, f in (("director", d), ("officer", o), ("10pct", t)) if f]
    return "+".join(parts) if parts else "(role NULL)"


def issuer_class(ticker, issuer):
    """house queries._issuer_class: fund_certain | fund_named | operating. A label."""
    if _FUND_TICKER.match(ticker or ""):
        return "fund_certain"
    if issuer and _FUND_NAME.search(issuer):
        return "fund_named"
    return "operating"


def dedup_amendments(rows):
    """house queries._dedup_amendments, same key and same rank.

    Key (reporting_cik or person, issuer_cik or ticker, tx_date, code, shares); the row
    with the greatest (filed_date, accession) survives; on a tie the first seen survives.
    Returns (survivors, dropped) where each dropped item is (dropped_row, survivor_row).
    Input order is (accession, tx_index), so a tie inside one filing keeps the lowest
    tx_index."""
    best, order, members = {}, [], defaultdict(list)
    for r in rows:
        key = (r["reporting_cik"] or r["reporting_person"],
               r["issuer_cik"] or r["ticker_raw"], r["tx_date"], "P", r["shares"])
        members[key].append(r)
        cur = best.get(key)
        if cur is None:
            best[key] = r
            order.append(key)
        elif ((r["filed_date"] or "", r["accession"] or "")
              > (cur["filed_date"] or "", cur["accession"] or "")):
            best[key] = r
    dropped = []
    for key in order:
        keep = best[key]
        keep["dup_collapsed"] = len(members[key]) - 1
        dropped.extend((m, keep) for m in members[key] if m is not keep)
    return [best[k] for k in order], dropped


def cofiling_key(r):
    """house queries._cofiling_key: one economic block reported by several affiliated
    filers has the same ticker, date, shares, price AND post-transaction holding. A NULL
    holding never matches another NULL."""
    if r["ownership_after"] is None:
        return ("no-holding-reported", r["accession"], r["tx_date"], r["shares"],
                r["exec_price"])
    return (r["ticker"], r["tx_date"], r["shares"], r["exec_price"], r["ownership_after"])


def _security_is_non_common(title):
    return bool(title and _NOT_COMMON.search(title))


def value_quality(shares, price, value, peer_median, title):
    """house queries._value_quality: a structural reason the dollar value needs review."""
    if (shares and price is not None and shares > 1 and price == shares
            and peer_median and peer_median > 0
            and price / peer_median > _PEER_RATIO_MAX):
        return "price_equals_share_count"
    if value is not None and abs(value) > _VALUE_REVIEW_MAX:
        return "value_above_1b_review"
    if (peer_median and price and price > 0 and peer_median > 0
            and (price / peer_median > _PEER_RATIO_MAX
                 or peer_median / price > _PEER_RATIO_MAX)
            and not _security_is_non_common(title)):
        return "price_vs_ticker_peers"
    return None


def mark_values(rows):
    """Set value_note on every row. None means the value may be summed.

    Precedence of the note: the stored value_flag; then no usable execution price; then
    the house read-time review markers; then 'filer_contaminated' (house
    contaminated_filers: the same filer on the same ticker fired a marker on another row).

    The marker test runs on EVERY row, value_flag rows included, exactly as the house
    contaminated_filers does: a value_flag row keeps its price, and a price 10x off the
    ticker's other filers contaminates that filer's other rows on the ticker."""
    px = defaultdict(list)
    for r in rows:
        if r["exec_price"] and r["exec_price"] > 0:
            px[r["ticker"]].append(r["exec_price"])
    peer = {}
    for t, v in px.items():
        if len(v) >= _PEER_MIN:
            v.sort()
            peer[t] = v[len(v) // 2]
    dirty = set()
    for r in rows:
        marker = value_quality(r["shares"], r["exec_price"], r["value"],
                               peer.get(r["ticker"]), r["security_title"])
        if marker:
            dirty.add((r["ticker"], r["reporting_cik"]))
        if r["value_flag"]:
            note = "value_flag:" + r["value_flag"]
        elif r["exec_price"] is None or r["exec_price"] <= 0 or r["value"] is None:
            note = "no_exec_price"
        else:
            note = marker
        r["value_note"] = note
    for r in rows:
        if r["value_note"] is None and (r["ticker"], r["reporting_cik"]) in dirty:
            r["value_note"] = "filer_contaminated"


class Series:
    """One ticker's eod closes, plus where its one-session jumps are."""

    def __init__(self, recs):
        self.dates = [r[0] for r in recs]
        self.close = [r[1] for r in recs]
        self.adj = [r[2] for r in recs]
        # index i where close[i] / close[i-1] is a jump, split by whether the two rows
        # came from the same fetch (same share basis) or from two different fetches
        self.jumps_cross, self.jumps_same = [], []
        for i in range(1, len(recs)):
            a, b = self.close[i - 1], self.close[i]
            if a and b and a > 0 and (b / a >= JUMP_UP or b / a <= JUMP_DOWN):
                (self.jumps_same if recs[i - 1][3] == recs[i][3]
                 else self.jumps_cross).append(i)

    def on_or_before(self, day):
        """Index of the last session on or before `day`, or None."""
        i = bisect.bisect_right(self.dates, day) - 1
        return i if i >= 0 else None

    @staticmethod
    def _any(jumps, i, j):
        k = bisect.bisect_right(jumps, i)
        return k < len(jumps) and jumps[k] <= j

    def jump_between(self, i, j):
        """'jump_cross_fetch', 'jump_same_fetch' or None for the sessions (i, j].
        Cross-fetch wins when both are present."""
        if self._any(self.jumps_cross, i, j):
            return "jump_cross_fetch"
        if self._any(self.jumps_same, i, j):
            return "jump_same_fetch"
        return None


def attach_prices(con, rows):
    """entry close (last session on or before tx_date, at most MAX_ENTRY_GAP_DAYS back),
    latest close, and the two returns. Nothing is imputed: a row that cannot be priced
    keeps None and says why in ret_note."""
    first = {}
    for r in rows:
        t = r["ticker"]
        if t not in first or r["tx_date"] < first[t]:
            first[t] = r["tx_date"]
    series = {}
    for t, d0 in first.items():
        start = (dt.date.fromisoformat(d0)
                 - dt.timedelta(days=MAX_ENTRY_GAP_DAYS)).isoformat()
        recs = con.execute(SQL_SERIES, (t, start)).fetchall()
        if recs:
            series[t] = Series(recs)
        else:
            # Either no price rows at all, or a series that ended more than
            # MAX_ENTRY_GAP_DAYS before the first trade. In the second case its last row
            # is still the latest close; it is too old to be an entry close.
            last = con.execute(SQL_LAST_ROW, (t,)).fetchall()
            series[t] = Series(last) if last else None
    for r in rows:
        s = series[r["ticker"]]
        if s is None:
            r["ret_note"] = "no_price_series"
            continue
        if s.dates:
            r["latest_date"], r["latest_close"] = s.dates[-1], s.close[-1]
        i = s.on_or_before(r["tx_date"]) if s.dates else None
        if i is not None:
            gap = (dt.date.fromisoformat(r["tx_date"])
                   - dt.date.fromisoformat(s.dates[i])).days
            if gap > MAX_ENTRY_GAP_DAYS:
                i = None
        if i is None:
            r["ret_note"] = "no_entry_close"
            continue
        j = len(s.dates) - 1
        r["entry_date"], r["entry_close"] = s.dates[i], s.close[i]
        r["days_held"] = (dt.date.fromisoformat(s.dates[j])
                          - dt.date.fromisoformat(s.dates[i])).days
        if j == i:
            r["ret_note"] = "no_later_close"
            continue
        if s.close[i] and s.close[i] > 0:
            r["ret"] = s.close[j] / s.close[i] - 1.0
        if s.adj[i] and s.adj[i] > 0 and s.adj[j] is not None:
            r["ret_adj"] = s.adj[j] / s.adj[i] - 1.0
        r["ret_note"] = s.jump_between(i, j)


# ---------------------------------------------------------------- the recipe
def run(db=DEFAULT_DB, since=None, until=None, regime="all", operating_only=False,
        readable_boxes_only=False, collapse_cofilers=False, include_jump_returns=False,
        total_return=False, today=None):
    """Build the row list, the excluded tally, the marks and the summaries.
    Returns a dict; rows are dicts keyed by COLUMNS, newest trade first."""
    ret_key = "ret_adj" if total_return else "ret"
    if regime not in ("watchlist", "universal", "all"):
        raise ValueError("regime must be watchlist, universal or all")
    for name, v in (("since", since), ("until", until)):
        if v is not None and iso10(v) != v:
            raise ValueError("--{} must be YYYY-MM-DD, got {!r}".format(name, v))
    if since is not None and until is not None and since > until:
        raise ValueError("--since {} is after --until {}".format(since, until))
    ceiling = ((dt.date.fromisoformat(today) if today else dt.date.today())
               + dt.timedelta(days=1)).isoformat()      # house dates.ceiling()
    con = connect_ro(db)
    try:
        cur = con.execute(SQL_BUYS, {"regime": regime})
        names = [c[0] for c in cur.description]
        raw = [dict(zip(names, rec)) for rec in cur.fetchall()]
        bands = dict(con.execute(SQL_BANDS).fetchall())
        last_session = con.execute(SQL_LAST_SESSION, (CALENDAR_TICKER,)).fetchone()[0]

        excluded = []                   # (label, count, detail) in pipeline order

        def step(label, keep, rows, detail=""):
            kept = [r for r in rows if keep(r)]
            excluded.append((label, len(rows) - len(kept), detail))
            return kept

        universe = len(raw)
        rows = step("plan_flag = 1 (10b5-1 plan box read as ticked)",
                    lambda r: r["plan_flag"] == 0, raw)
        rows = step("date_flag set (off the trade clock; any tx_date)",
                    lambda r: r["date_flag"] is None, rows)
        for r in rows:
            r["tx_raw"] = r["tx_date"]
            r["tx_date"] = iso10(r["tx_date"])
            r["filed_date"] = iso10(r["filed_date"]) or r["tx_date"]   # house fallback
        rows = step("tx_date unparseable or outside {}..{}".format(FLOOR, ceiling),
                    lambda r: r["tx_date"] is not None and FLOOR <= r["tx_date"] <= ceiling,
                    rows)
        rows = step("outside the trade-date window (scope, not a defect)",
                    lambda r: (since is None or r["tx_date"] >= since)
                    and (until is None or r["tx_date"] <= until), rows)
        for r in rows:
            r["ticker_raw"] = r["ticker"]
            r["ticker"] = (r["ticker"] or "").strip().upper()
        ph = Counter(r["ticker"] or "(empty)" for r in rows
                     if r["ticker"] in PLACEHOLDER_TICKERS)
        rows = step("placeholder ticker (no symbol; cannot be priced or banded)",
                    lambda r: r["ticker"] not in PLACEHOLDER_TICKERS, rows,
                    ", ".join("{} {}".format(k, v) for k, v in ph.most_common()))
        before = len(rows)
        rows, dropped = dedup_amendments(rows)
        same = [d for d, k in dropped if d["accession"] == k["accession"]]
        same_px = sum(1 for d, k in dropped
                      if d["accession"] == k["accession"] and d["price"] != k["price"])
        excluded.append((
            "same trade already counted (house _dedup_amendments)", before - len(rows),
            "{} other accession; {} same filing ({} at a different price)".format(
                len(dropped) - len(same), len(same), same_px)))
        rows = step("shares NULL or <= 0 (nothing was bought)",
                    lambda r: r["shares"] is not None and r["shares"] > 0, rows)

        for r in rows:
            r["exec_price"] = r.pop("price")
            r["role_bucket"] = role_bucket(r["role"])
            r["boxes_read"] = 0 if r["role"] is None else 1
            r["issuer_class"] = issuer_class(r["ticker"], r["issuer"])

        # co-filing groups are counted BEFORE the optional filters, as the house does
        grp = defaultdict(set)
        for r in rows:
            grp[cofiling_key(r)].add(r["reporting_cik"])
        for r in rows:
            r["cofilers"] = len(grp[cofiling_key(r)])

        if readable_boxes_only:
            rows = step("--readable-boxes-only: role NULL (10b5-1 box may be unread)",
                        lambda r: r["boxes_read"] == 1, rows)
        if operating_only:
            rows = step("--operating-only: fund issuer (house _issuer_class)",
                        lambda r: r["issuer_class"] == "operating", rows)
        if collapse_cofilers:
            seen, keep_ids = set(), set()
            for r in sorted(rows, key=lambda x: (str(x["reporting_cik"] or ""),
                                                 str(x["accession"] or ""), x["tx_index"])):
                k = cofiling_key(r)
                if k not in seen:
                    seen.add(k)
                    keep_ids.add((r["accession"], r["tx_index"]))
            rows = step("--collapse-cofilers: co-filed copy of a block already kept",
                        lambda r: (r["accession"], r["tx_index"]) in keep_ids, rows)

        for r in rows:
            b = bands.get(r["ticker"])
            r["band_source"] = ("no_row" if b is None
                                else "unbandable" if b not in BANDS else "banded")
            r["band"] = b if b in BANDS else "unbanded"
            try:
                r["lag_days"] = (dt.date.fromisoformat(r["filed_date"])
                                 - dt.date.fromisoformat(r["tx_date"])).days
            except (TypeError, ValueError):
                r["lag_days"] = None
            for k in ("entry_date", "entry_close", "latest_date", "latest_close",
                      "days_held", "ret", "ret_adj", "ret_note"):
                r[k] = None
        mark_values(rows)
        attach_prices(con, rows)
    finally:
        con.close()

    rows.sort(key=lambda r: (r["tx_date"], r["filed_date"], r["accession"], r["tx_index"]),
              reverse=True)

    def is_jump(r):
        return r[ret_key] is not None and (r["ret_note"] or "").startswith("jump")

    def in_summary(r):
        return r[ret_key] is not None and (r["ret_note"] is None
                                           or (include_jump_returns and is_jump(r)))

    def stats(note):
        x = [r[ret_key] for r in rows if r[ret_key] is not None and r["ret_note"] == note]
        if not x:
            return ""
        return "their own median {:+.1f}%, mean {:+.1f}%".format(
            100 * statistics.median(x), 100 * statistics.fmean(x))

    jump_fate = ("INCLUDED (--include-jump-returns)" if include_jump_returns
                 else "withheld from the summary")
    n = len(rows)
    stale_cut = None
    if last_session:
        stale_cut = (dt.date.fromisoformat(last_session)
                     - dt.timedelta(days=STALE_DAYS)).isoformat()
    vnotes = Counter(r["value_note"].split(":")[0] if r["value_note"] else None for r in rows)
    vflags = Counter(r["value_flag"] for r in rows if r["value_flag"])
    marks = [
        ("value_flag set: kept, value is NULL, out of $ totals",
         sum(vflags.values()), ", ".join("{} {}".format(k, v) for k, v in vflags.most_common())),
        ("no usable exec price (NULL or 0): kept, out of $ totals",
         vnotes["no_exec_price"], ""),
        ("house value-review marker: kept, out of $ totals",
         sum(v for k, v in vnotes.items()
             if k in ("price_equals_share_count", "value_above_1b_review",
                      "price_vs_ticker_peers", "filer_contaminated")),
         ", ".join("{} {}".format(k, vnotes[k]) for k in
                   ("value_above_1b_review", "price_vs_ticker_peers",
                    "price_equals_share_count", "filer_contaminated") if vnotes[k])),
        ("date_subclass non_trading_day: kept",
         sum(1 for r in rows if r["date_subclass"] == "non_trading_day"),
         "entry close = last session on or before; {} of them priced".format(
             sum(1 for r in rows if r["date_subclass"] == "non_trading_day"
                 and r["entry_close"]))),
        ("entry close is from a session before tx_date (any reason)",
         sum(1 for r in rows if r["entry_date"] and r["entry_date"] < r["tx_date"]), ""),
        ("no_price_series: ticker has no price rows; no close, no return",
         sum(1 for r in rows if r["ret_note"] == "no_price_series"), "never imputed"),
        ("no_entry_close: no close within {} days on or before tx_date; no return"
         .format(MAX_ENTRY_GAP_DAYS),
         sum(1 for r in rows if r["ret_note"] == "no_entry_close"), "never imputed"),
        ("no_later_close: entry close is the series' last close; no return",
         sum(1 for r in rows if r["ret_note"] == "no_later_close"), ""),
        ("jump_cross_fetch: {}".format(jump_fate),
         sum(1 for r in rows if r["ret_note"] == "jump_cross_fetch"),
         "one-session move >= {:g}x or <= {:g}x across two fetches; {}".format(
             JUMP_UP, JUMP_DOWN, stats("jump_cross_fetch")).rstrip("; ")),
        ("jump_same_fetch: {}".format(jump_fate),
         sum(1 for r in rows if r["ret_note"] == "jump_same_fetch"),
         "the same size of move inside one fetch; {}".format(
             stats("jump_same_fetch")).rstrip("; ")),
        ("returns in the summary ({})".format(ret_key),
         sum(1 for r in rows if in_summary(r)), ""),
        ("latest close is before {} (stale series): return kept, shorter horizon"
         .format(stale_cut),
         sum(1 for r in rows if in_summary(r) and stale_cut
             and r["latest_date"] < stale_cut),
         "last {} session {}".format(CALENDAR_TICKER, last_session)),
        ("filed more than 365 days after the trade: kept",
         sum(1 for r in rows if r["lag_days"] is not None and r["lag_days"] > 365),
         "late filing or mistyped year; date_flag cannot see it"),
        ("unbanded (NOT small)",
         sum(1 for r in rows if r["band"] == "unbanded"),
         "no market_cap row {}, row says unbandable {}".format(
             sum(1 for r in rows if r["band_source"] == "no_row"),
             sum(1 for r in rows if r["band_source"] == "unbandable"))),
        ("role NULL -> not_recorded: kept, plan_flag = 0 may be unread",
         sum(1 for r in rows if r["boxes_read"] == 0),
         "boxes spelled true/false were not read, OR only 'Other' was ticked (see ROLE)"),
        ("fund issuer (house _issuer_class): kept",
         sum(1 for r in rows if r["issuer_class"] != "operating"),
         "fund_named {}, fund_certain {}".format(
             sum(1 for r in rows if r["issuer_class"] == "fund_named"),
             sum(1 for r in rows if r["issuer_class"] == "fund_certain"))),
        ("co-filed block (cofilers > 1): every copy kept",
         sum(1 for r in rows if r["cofilers"] > 1),
         "one lot reported by several affiliated filers"),
    ]

    def summarize(key, order):
        out = []
        for b in tuple(order) + ("ALL",):
            sub = rows if b == "ALL" else [r for r in rows if r[key] == b]
            rets = [r[ret_key] for r in sub if in_summary(r)]
            vals = [r["value"] for r in sub if r["value_note"] is None]
            out.append({
                key: b, "rows": len(sub),
                "persons": len({r["reporting_cik"] for r in sub}),
                "tickers": len({r["ticker"] for r in sub}),
                "n_ret": len(rets),
                "n_jump": sum(1 for r in sub if is_jump(r)),
                "median_ret": statistics.median(rets) if rets else None,
                "mean_ret": statistics.fmean(rets) if rets else None,
                "share_up": (sum(1 for x in rets if x > 0) / len(rets)) if rets else None,
                "n_value": len(vals), "value_usd": sum(vals) if vals else None})
        return out

    combos = Counter((role_combo(r["role"]), r["role_bucket"]) for r in rows)
    return {
        "meta": {"db": db, "since": since, "until": until, "regime": regime,
                 "universe": universe, "kept": n, "last_session": last_session,
                 "ret_key": ret_key, "include_jump_returns": include_jump_returns,
                 "options": [name for name, on in (
                     ("--readable-boxes-only", readable_boxes_only),
                     ("--operating-only", operating_only),
                     ("--collapse-cofilers", collapse_cofilers),
                     ("--include-jump-returns", include_jump_returns),
                     ("--total-return", total_return)) if on],
                 "tx_min": min((r["tx_date"] for r in rows), default=None),
                 "tx_max": max((r["tx_date"] for r in rows), default=None)},
        "rows": [{c: r.get(c) for c in COLUMNS} for r in rows],
        "excluded": excluded, "marks": marks,
        "role_combos": sorted(combos.items(), key=lambda kv: -kv[1]),
        "by_role": summarize("role_bucket", ROLE_ORDER),
        "by_band": summarize("band", BAND_ORDER),
    }


def dataframe(db=DEFAULT_DB, **kwargs):
    """The row list as a pandas DataFrame (columns = COLUMNS). Needs pandas; the script
    itself does not. Same keyword arguments as run()."""
    if pd is None:
        raise RuntimeError("pandas is not installed; use run() or --csv instead")
    return pd.DataFrame(run(db, **kwargs)["rows"], columns=list(COLUMNS))


# ---------------------------------------------------------------- printing
def _num(v, spec):
    return "" if v is None else format(v, spec)


def _pct(v):
    return "" if v is None else "{:+.1f}%".format(100.0 * v)


def _table(headers, lines, right=()):
    widths = [max(len(str(h)), *(len(str(l[i])) for l in lines)) if lines else len(str(h))
              for i, h in enumerate(headers)]
    def fmt(cells):
        return "  ".join(str(c).rjust(w) if i in right else str(c).ljust(w)
                         for i, (c, w) in enumerate(zip(cells, widths))).rstrip()
    out = [fmt(headers), fmt(["-" * w for w in widths])]
    out.extend(fmt(l) for l in lines)
    return "\n".join(out)


def print_report(res, limit=25, out=sys.stdout):
    m = res["meta"]
    w = out.write
    w("INSIDER BUYS - discretionary purchases (Form 4 code P, plan_flag = 0)\n")
    w("db {}\n".format(m["db"]))
    w("trade-date window {} .. {}   regime {}   last {} session in prices {}\n".format(
        m["since"] or "(open)", m["until"] or "(open)", m["regime"], CALENDAR_TICKER,
        m["last_session"]))
    w("options {}\n".format(" ".join(m["options"]) or "(none)"))
    w("code-P rows in regime scope {:,}   kept {:,}   kept tx_date {} .. {}\n\n".format(
        m["universe"], m["kept"], m["tx_min"], m["tx_max"]))

    shown = res["rows"] if not limit else res["rows"][:limit]
    lines = []
    for r in shown:
        entry = _num(r["entry_close"], ",.2f")
        if r["entry_date"] and r["entry_date"] < r["tx_date"]:
            entry += "*"
        lines.append([
            r["tx_date"], r["filed_date"], r["ticker"][:8],
            (r["reporting_person"] or "")[:22], r["role_bucket"],
            _num(r["shares"], ",.0f"), _num(r["exec_price"], ",.2f"),
            "" if r["value_note"] else _num(r["value"], ",.0f"),
            entry, _num(r["latest_close"], ",.2f"), r["latest_date"] or "",
            _pct(r["ret"]), r["band"],
            ",".join(x for x in (r["value_note"], r["ret_note"],
                                 "ntd" if r["date_subclass"] == "non_trading_day" else None)
                     if x)])
    w("ROWS ({} of {:,}, newest trade first; --csv writes all, --limit 0 prints all)\n".format(
        len(shown), m["kept"]))
    w(_table(["tx_date", "filed", "ticker", "person", "role", "shares", "exec_px",
              "value_usd", "entry_cl", "latest_cl", "latest_dt", "ret", "band", "notes"],
             lines, right=(5, 6, 7, 8, 9, 11)))
    w("\n  exec_px = nominal price on the form. entry_cl / latest_cl = split-adjusted closes.\n"
      "  ret = latest_cl / entry_cl - 1.  * = entry close is from an earlier session.\n"
      "  value_usd blank = not trusted for $ totals (see notes).\n\n")

    w("EXCLUDED (in order; each row is counted once, at the first step that removes it)\n")
    w(_table(["rows", "step", "detail"],
             [["{:,}".format(c), label, detail] for label, c, detail in res["excluded"]],
             right=(0,)))
    w("\n  kept: {:,}\n\n".format(m["kept"]))

    w("KEPT BUT MARKED (not exclusive; a row can carry several)\n")
    w(_table(["rows", "mark", "detail"],
             [["{:,}".format(c), label, detail] for label, c, detail in res["marks"]],
             right=(0,)))
    w("\n\n")

    w("ROLE BUCKETING (form4_transactions.role -> bucket; officer > director > 10% owner)\n")
    w(_table(["rows", "boxes read as ticked", "bucket"],
             [["{:,}".format(c), combo, bucket] for (combo, bucket), c in res["role_combos"]],
             right=(0,)))
    w("\n\n")

    def summary(title, key, data):
        w(title + "\n")
        w(_table([key, "rows", "persons", "tickers", "n_ret", "n_jump", "median_ret",
                  "mean_ret", "share_up", "n_value", "value_usd"],
                 [[d[key], "{:,}".format(d["rows"]), "{:,}".format(d["persons"]),
                   "{:,}".format(d["tickers"]), "{:,}".format(d["n_ret"]),
                   "{:,}".format(d["n_jump"]),
                   _pct(d["median_ret"]), _pct(d["mean_ret"]),
                   "" if d["share_up"] is None else "{:.0f}%".format(100 * d["share_up"]),
                   "{:,}".format(d["n_value"]), _num(d["value_usd"], ",.0f")] for d in data],
                 right=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
        w("\n\n")

    what = ("ret_adj: split- and dividend-adjusted close to close" if m["ret_key"] == "ret_adj"
            else "ret: split-adjusted close to close, dividends not included")
    summary("SUMMARY BY ROLE ({})".format(what), "role_bucket", res["by_role"])
    summary("SUMMARY BY SMID BAND (band = market_cap.band as cached in 2026, not the size "
            "at the trade)", "band", res["by_band"])
    w("n_ret = returns in the median/mean. n_jump = returns computed but {} (see KEPT BUT\n"
      "MARKED). Each return runs from its own trade date to its own latest close, so\n"
      "horizons differ row to row. Not annualised, not benchmark-adjusted. Rows are not\n"
      "independent: one person can be hundreds of rows. Read the median; the mean moves\n"
      "with a handful of rows.\n".format(
          "INCLUDED" if m["include_jump_returns"] else "withheld"))


def write_csv(res, path):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(COLUMNS)
        for r in res["rows"]:
            wr.writerow(["" if r[c] is None else r[c] for c in COLUMNS])


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Discretionary insider buys with entry-vs-latest close, by role and "
                    "SMID band. Read the module docstring before trusting a number.")
    ap.add_argument("--db", default=DEFAULT_DB, help="SQLite path (opened mode=ro)")
    ap.add_argument("--since", help="first trade date, YYYY-MM-DD (inclusive)")
    ap.add_argument("--until", help="last trade date, YYYY-MM-DD (inclusive)")
    ap.add_argument("--regime", choices=("watchlist", "universal", "all"), default="all")
    ap.add_argument("--csv", metavar="OUT", help="write every kept row to this CSV")
    ap.add_argument("--limit", type=int, default=25,
                    help="rows printed to stdout (0 = all; default 25)")
    ap.add_argument("--readable-boxes-only", action="store_true",
                    help="drop rows whose role is NULL (10b5-1 box may be unread)")
    ap.add_argument("--operating-only", action="store_true",
                    help="drop fund issuers (house _issuer_class)")
    ap.add_argument("--collapse-cofilers", action="store_true",
                    help="keep one row per co-filed block (lowest reporting CIK)")
    ap.add_argument("--include-jump-returns", action="store_true",
                    help="put jump-marked returns into the summary")
    ap.add_argument("--total-return", action="store_true",
                    help="summarise ret_adj (dividends included; the house's number) "
                         "instead of ret")
    a = ap.parse_args(argv)
    try:
        res = run(a.db, a.since, a.until, a.regime, a.operating_only,
                  a.readable_boxes_only, a.collapse_cofilers, a.include_jump_returns,
                  a.total_return)
    except (ValueError, sqlite3.Error) as exc:
        print("insider_buys: {}".format(exc), file=sys.stderr)
        return 2
    print_report(res, a.limit)
    if a.csv:
        write_csv(res, a.csv)
        print("\ncsv: {:,} rows -> {}".format(len(res["rows"]), a.csv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
