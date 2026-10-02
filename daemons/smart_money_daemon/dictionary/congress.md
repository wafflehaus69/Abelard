# Congress

Tables: `congress_trades` (periodic transaction reports, PTRs), `congress_holdings` (annual reports), `persons`, `ingested_filings`, `congress_member_roster`, `congress_committees`. All counts measured on the mirror (snapshot 2026-10-01). Dollar columns are band bounds in USD, not amounts. "House" below always means the chamber; the package's own code is called `smart_money`.

## congress_trades

**One row = one transaction line of one PTR filing.** 47,168 rows: House 32,868 (`source='house_clerk'`), Senate 14,300 (`source='efd'`).

| | disclosure_date | tx_date (unflagged) | PTR filings parsed / indexed |
|---|---|---|---|
| House | 2018-04-27 .. 2026-09-27 | 2015-05-08 .. 2026-09-22 | 2,488 / 8,389 (29.7%) |
| Senate | 2014-01-29 .. 2026-09-28 | 2012-06-14 .. 2026-09-11 | 1,603 / 1,875 (85.5%) |

| column | meaning | unit / values |
|---|---|---|
| `trade_id` | row key | |
| `person_id` | the filing member (not the owner) -> `persons` | |
| `chamber` | which chamber's filing this is. Use this, not `persons.cik_or_chamber` | house, senate |
| `filing_id` | House DocID or Senate eFD uuid -> `ingested_filings` | |
| `ticker` | as filed, uppercased, not validated | NULL on 7,512 rows |
| `asset_name` | filer's text. House text carries the `[ST]` code and sometimes bled description | |
| `asset_type` | see below | |
| `side` | see below | |
| `amt_low`, `amt_high` | **band** bounds | USD integers |
| `owner` | whose account | see below |
| `tx_date` | trade clock | ISO date |
| `disclosure_date` | filing clock; equals `ingested_filings.filed_date` on every row | ISO date |
| `lag_days` | `disclosure_date - tx_date`, NOT NULL | days |
| `superseded` | 1 = replaced or retracted by an amendment | 0/1 |
| `filing_status` | House Clerk per-line marker | New, Amended, Deleted, NULL |
| `date_flag`, `date_subclass`, `tx_date_suggested` | trade-date quarantine, see Flags | |
| `comment` | free text | 4,202 rows |

**Values differ by chamber.**

- `side`: House purchase 16,285 / sale 10,977 / sale_partial 5,438 / exchange 168. Senate purchase 7,460 / sale_full 3,925 / sale_partial 2,799 / exchange 116. All sales = `side IN ('sale','sale_full','sale_partial')`.
- `owner`: House Self 16,392 / JT 8,796 / SP 6,468 / DC 1,042 / sP 170 (same as SP). Senate Joint 5,675 / Spouse 5,450 / Self 2,837 / Child 338. A blank House owner is stored as Self.
- `asset_type` (rows; rows with a ticker): **Stock 37,194; 35,807 - this is the stock corpus `smart_money` reads.** Then NULL 2,065; 1,494 (House, 2,064 of them disclosed 2018-2022) - `''` 781; 684 (Senate 2014-2015) - Government Security 1,673; 46 - Corporate Bond 1,166; 63 - Municipal Security 1,120; 0 - Stock Option 1,001; 997 - Other 785; 357 (Senate, includes ETFs and funds) - OT 640; 152 (House, includes ETFs) - HN 160 - Commodities/Futures Contract 130 - Non-Public Stock 129 - PS 87 - Cryptocurrency 65 - OI 63 - VA 34 - OL 32 - AB 28 - ET 11 - RS 3 - SA 1. There is no ETF or mutual-fund type: ETFs sit inside Stock (at least 917 rows by name - ETF, iShares, SPDR, Index Fund - House 483, Senate 434; SPY alone 44), OT and Other.
- **Amounts are bands.** 1,001-15,000: 33,105 rows; 15,001-50,000: 7,829; 50,001-100,000: 2,324; 100,001-250,000: 1,882; 250,001-500,000: 898; 500,001-1,000,000: 583; 1,000,001-5,000,000: 212; 5,000,001-25,000,000: 49; 25,000,001-50,000,000: 3. **Open top band, `amt_high` NULL: 79 rows** (76 with floor 1,000,001, 3 with floor 50,000,001). Exact amounts (`amt_low = amt_high`): 204 House rows, 194 of them under $1,001.

### Flags and what excluding them means

| filter | drops | when |
|---|---|---|
| `superseded = 0` | 392 rows (House 238, Senate 154; 322 Stock) | **always.** Leaves 46,776. The kept row carries the amendment's dates (trap 2) |
| `date_flag IS NULL` | 27 rows, all `after_filing` (House 19, Senate 8) | anything anchored on `tx_date` or `lag_days`. **Not** on the filing clock: these rows are valid disclosures |
| `asset_type = 'Stock'` | 9,904 live rows | equity work. Leaves 36,872 |
| `ticker IS NOT NULL` | 1,380 live Stock rows (House 103, Senate 1,277) | joins to prices. All four filters: 35,474 |

- `date_subclass`: non_trading_day 107 (100 with `date_flag` NULL - a mark, keep them; only Stock rows are tested), future_dated_other 14, year_slip_pattern 6. `tx_date_suggested` is set on those 6 only and is never applied.
- `lag_days` negative on 30 rows: 27 flagged (down to -368,891, a trade dated 3031) and 3 at -1 with no flag (one-day tolerance, by design). Over 45 days (the statutory deadline) on 17.3% of live unflagged House rows and 24.5% of Senate; amended rows are in those shares (trap 2), without them 16.3% and 23.5%.
- `filing_status` NULL = **not extracted, never "no amendment"**: 21,117 rows. Senate: all 14,300, every year (never filled). House: 6,817 - disclosure year 2025: 4,239 of 7,667; 2026: 2,578 of 3,365; 2018-2024: 0. Where filled: New 25,579, Amended 437, Deleted 35 (all 35 superseded).

### Traps

1. **Senate amendments are mostly unresolved.** The only Senate amendment evidence is the filing label, and no Senate filing loaded before 2026-08-06 (1,840 of 1,875) carries one: the bulk load of 2026-07-21/22 (1,831 filings) wrote its own label ("Periodic Transaction Report for <filed date>" or "Paper PTR"), so an amendment could not be marked. The 154 superseded Senate rows all come from 13 labelled amendments loaded since. 1,589 live Senate rows (1,197 Stock) repeat an earlier-filed live row with the same member, trade date, ticker, side and band; House 87 (60 Stock). 1,087 of the 1,676 are inside the four-filter set of 35,474. Example: one senator's filings of 2016-08-13 and 2016-09-01, 127 rows each, 126 identical. The filter in example 2 removes them. That these are amendments is inferred, not stated by the source.
2. **An amended line carries the amendment's dates.** `superseded=0` keeps the latest filing, so `disclosure_date` and `lag_days` on that row are the amendment's, not the first disclosure's. House: 429 live `Amended` rows, median lag 445 days. Senate: 196 live rows in the 13 labelled amendments, all disclosed 2026-08-05 to 2026-08-24 for trades back to 2024-03-08. 299 live rows (House 162, Senate 137) have a superseded original filed earlier, on average 306 and 672 days earlier; that superseded row is the first disclosure. For an event date on the filing clock take `MIN(disclosure_date)` over all rows, superseded included, with the same member, trade date, ticker, side and band.
3. **House coverage is thin before 2022.** Filings parsed: 2014-2017 0 of 2,986; 2018-2021 517 of 2,941 (18%); 2022 385 of 621 (62%); 2023-2026 1,586 of 1,841 (86%). The rest are paper or an unparsed layout and have no rows. Do not compare House counts across 2022.
4. `asset_type='Stock'` misses 1,411 House stock rows whose code extracted as `[sT]` (2018: 334, 2019: 586, 2020: 491) and the untyped Senate rows of 2014-2015. To add the House ones: `OR (asset_type IS NULL AND asset_name GLOB '*[[][sS][tT][]]*')`.
5. House rows with NULL `filing_status` (550 filings, 6,817 rows) may hide Amended or Deleted lines. Where status is filled, 472 of 26,051 lines (1.8%) are one or the other.
6. Identical lines inside one filing are kept on purpose (live rows identical on date, ticker, asset name, side, band and owner: House 751 extra rows, Senate 728); the code comments say they were checked against the source PDFs. Do not `DISTINCT` them away.
7. Tickers are as filed: FB 152 rows and META 150; BRK.B 169, BRK-B 24, BRKB 3; `'--'` on 19 Senate rows. 2,145 of 3,129 distinct live Stock tickers have any row in `prices` (31,694 of 35,492 live Stock rows with a ticker).
8. Summing `amt_low` gives a floor. A midpoint needs a rule for the 79 open bands (`queries._mid` uses the floor).
9. 4 Senate rows were lost at ingest (`n_rows` 14,304 vs 14,300 stored).

## persons (congress rows) and ingested_filings

`persons`: one row per distinct filer name. 444 rows with `type='congress'` (the other 51,625 are Form 4 insiders); 331 have trades, 113 have none. `name` is 'Last, First'. `meta` is JSON: House `$.state_dst` (348 rows), Senate `$.office`.

**Identity is the exact name string, nothing else.** No bioguide, no state key.

- `cik_or_chamber` is the chamber where the name was first seen. 706 Senate trade rows sit on 4 persons marked 'house'.
- 13 name pairs are one person under two `person_id`s (e.g. 'Suozzi, Thomas' 708 trades and 'Suozzi, Thomas R.' 9); 9 pairs have trades on both ids; 3 are House-to-Senate moves.
- 24 Senate persons have a leaked JSON blob as `name` (167 trades in 24 filings), and one is named 'Former Senator' (18 trades in 3 filings). The member cannot be read from any column.

`ingested_filings`: one row per PTR filing seen, 10,264 (House 8,389, Senate 1,875). `status`: electronic 4,091 (has rows), paper 2,723, unparsed_layout 3,449, fetch_failed 1. `filed_date`, `n_rows`, `report_label`, `person_name`. `ingested_at_unix` is load time (bulk loads 2026-07-17 to 07-22), not arrival time. Annual reports are not in this table.

## congress_holdings

**One row = one asset line of one annual financial-disclosure document.** 146,093 rows: House 83,833 (1,911 documents, 687 filer identities), Senate 62,260 (847 documents, 229 identities). **A LEVEL reported once a year, not a flow:** no trade dates, and a year-over-year difference is not a trade.

| | coverage_year present (documents) | filing_date |
|---|---|---|
| House | 2022: 467, 2023: 495, 2024: 419, 2025: 496, 2026: 34 | 2022-04-13 .. 2026-09-28 |
| Senate | 2016-2020: 31 in total, 2021: 124, 2022: 130, 2023: 140, 2024: 121, 2025: 211, 2026: 90 | 2022-01-20 .. 2026-09-27 |

| column | meaning | unit / values |
|---|---|---|
| `doc_id`, `row_idx` | document and line (primary key) | |
| `chamber`, `member_last`, `member_first`, `state_dist` | the filer as written. Together these are the member key | `state_dist` like CA11; **NULL on every Senate row** |
| `person_id` | **NULL on all 146,093 rows** | |
| `coverage_year` | calendar year the report covers | year |
| `filing_year` | equal to `coverage_year` on every row | year |
| `filing_date` | date filed | ISO date |
| `period` | the filed date as the source wrote it. Not a period | M/D/YYYY text |
| `ticker` | parsed from the asset text | NULL: House 39,151, Senate 28,725 |
| `asset_type` | House Schedule A codes: ST 22,553, MF 18,759, EF 11,889, BA 6,821, NULL 6,475, 30 more. Senate reduced to ST 16,245, MF 13,444, EF 8,416, CS 972, OP 667, else NULL 22,516 | **'ST' here, 'Stock' in trades** |
| `owner` | Self, SP, JT, DC | 1,546 House rows hold junk text |
| `value_lo`, `value_hi` | **band** bounds | USD integers |
| `income_type` | free text | |

- Bands: `value_hi` NULL with `value_lo` set = open top band, 791 rows (floor 1,000,000 or 50,000,000 on all but 2, not the +1 values used in trades). Both NULL = no value captured, 16,619 rows (House 10,355, Senate 6,264). Senate (0, 1001) = "None (or less than $1,001)", 7,311 rows.
- **`coverage_year` vs `filing_year`.** The `db.py` schema comment says House `filing_year` is a cycle year with coverage one year earlier. That comment is stale: the migration and ingest code call the -1 a bug, and the data agree (359 of 467 House documents with `coverage_year` 2022 were filed in 2023). The chambers differ in source: House takes the Clerk index `Year`; Senate takes the eFD title "Annual Report for YYYY" and **falls back to the year filed** when the title has none.
- Documents whose `coverage_year` equals the year they were filed: Senate 206 (15,895 rows), House 158 (4,180 rows), including every 2026 document. 152 and 135 of them belong to filers the roster could not match. What these reports are is not established (likely candidate or new-filer reports); do not read them as a year-end level.

### Traps

1. **More than one document per member-year** (amendments restate the schedule): House 171 of 1,713 member-years, Senate 130 of 686. 19,861 rows (House 7,965, Senate 11,896) sit in a document that a later one replaces. Pick one document, as example 3 does. The `queries.py` read functions do not: their holder counts are safe, their dollar sums are not.
2. No key to trades. `smart_money` links by name (`queries._person_index` + `resolve_person`): 223 of 331 trade persons link, covering 75.1% of live trade rows.
3. One person can be several identities (spelling, chamber move): 27 bioguides map to 2 or 3 identities. Counting identities overcounts people.
4. Non-members are in the table: 282 of 916 identities did not match the roster (per the code, mostly candidates who never served).
5. Documents not parsed have no rows. `congress_fd_seen` is the ledger: House ok 1,914 / paper 167 / unparsed_layout 91; Senate ok 847 / candidate_not_served 162 / paper 62 / no_grid 20.

## congress_member_roster and congress_committees

`congress_member_roster`: one row per holdings identity, 916 (House 687, Senate 229). Key is (`chamber`, `member_last`, `member_first`, `state_dist`) - join with `IS`, not `=`. `party` (Republican 317, Democrat 312, Independent 5, NULL 282), `state`, `match_kind` (unique 625, byname 5, incumbent 4, unmatched 282), `bioguide` (626 rows, 596 distinct).

- **Senate matching is by surname only.** 6 Senate identities carry the party and bioguide of a senator with a different given name (three Senate filers named Brown share one bioguide): 420 holdings rows. 1 House identity does the same (1 row).

`congress_committees`: one row per (`bioguide`, `committee_id`), 3,895 rows, 531 members, 228 ids (49 committees, the rest subcommittees with `parent_id`). `chamber` house / senate / joint, `title`, `rank`, `side`. **Current Congress only** (synced 2026-10-01); it cannot say what a member sat on in a past year. A seat attaches to 56.3% of latest-year holdings rows (House 60.0%, Senate 49.4%; code comments quoting 68.1% and 55.7% are stale) and, through the name link, to 60.6% of live trade rows.

## Example queries (all run on the mirror)

Trade clock: live stock purchases dated in 2026. Returns House 1,034 trades / 32 members / floor $7,268,034; Senate 425 / 10 / $2,152,425.

```sql
SELECT t.chamber,
       COUNT(*)                    AS trades,
       COUNT(DISTINCT t.person_id) AS members,
       SUM(t.amt_low)              AS floor_usd   -- sum of band floors, not dollars traded
FROM congress_trades t
WHERE t.superseded = 0                -- amendments resolved
  AND t.date_flag IS NULL             -- trade date usable
  AND t.asset_type = 'Stock' AND t.ticker IS NOT NULL
  AND t.side = 'purchase'
  AND t.tx_date BETWEEN '2026-01-01' AND '2026-09-30'
GROUP BY t.chamber;
```

Filing clock: stock sales disclosed in September 2026, repeats of an earlier filing removed. Returns 265 rows. The repeat filter removes none here; over the whole table it removes 1,676 rows (1,257 Stock). One of the 265 is a House `Amended` line (trade 2025-06-03, lag 466 days): an amendment's date, not a first disclosure (trap 2).

```sql
SELECT t.disclosure_date, p.name, t.chamber, t.ticker, t.side,
       t.amt_low, t.amt_high, t.tx_date, t.date_flag
FROM congress_trades t JOIN persons p USING (person_id)
WHERE t.superseded = 0                -- no date_flag filter on this clock
  AND t.asset_type = 'Stock' AND t.ticker IS NOT NULL
  AND t.side IN ('sale', 'sale_full', 'sale_partial')
  AND t.disclosure_date BETWEEN '2026-09-01' AND '2026-09-30'
  AND NOT EXISTS (                    -- drop a repeat of an earlier filing (trap 1)
      SELECT 1 FROM congress_trades e
      WHERE e.superseded = 0 AND e.person_id = t.person_id
        AND e.filing_id <> t.filing_id AND e.disclosure_date < t.disclosure_date
        AND e.tx_date = t.tx_date AND e.ticker IS t.ticker AND e.side = t.side
        AND e.amt_low = t.amt_low AND e.amt_high IS t.amt_high)
ORDER BY t.disclosure_date, p.name;
```

Holdings level: stock holders by party, one document per roster-matched person. Top row: AAPL, 87 holders (36 Democrat, 50 Republican), floor $15,187,126. `queries.q_congress_breadth` gives 96 for AAPL because it counts identities and every asset type. The 7 surname mismatches above are not corrected here.

```sql
WITH docs AS (
  SELECT DISTINCT h.doc_id, h.coverage_year, h.filing_date, r.bioguide, r.party
  FROM congress_holdings h
  JOIN congress_member_roster r
    ON r.chamber = h.chamber AND r.member_last IS h.member_last
   AND r.member_first IS h.member_first
   AND r.state_dist IS h.state_dist          -- IS, not =: Senate state_dist is NULL
  WHERE r.bioguide IS NOT NULL               -- roster-matched members only
), pick AS (                                 -- one document per person: latest year, latest filed
  SELECT doc_id, bioguide, party,
         ROW_NUMBER() OVER (PARTITION BY bioguide
                            ORDER BY coverage_year DESC, filing_date DESC, doc_id DESC) AS rn
  FROM docs
)
SELECT h.ticker,
       COUNT(DISTINCT k.bioguide)                                          AS holders,
       COUNT(DISTINCT CASE WHEN k.party = 'Democrat'   THEN k.bioguide END) AS dem,
       COUNT(DISTINCT CASE WHEN k.party = 'Republican' THEN k.bioguide END) AS rep,
       SUM(h.value_lo)                                                     AS floor_usd
FROM congress_holdings h
JOIN pick k ON k.doc_id = h.doc_id AND k.rn = 1
WHERE h.asset_type = 'ST' AND h.ticker IS NOT NULL
GROUP BY h.ticker
ORDER BY holders DESC
LIMIT 5;
```

Not established: why 550 House filings were never status-backfilled; which Senate documents used the filed-year fallback; whether Senate paper PTRs existed in 2014-2015 (none are recorded); how many roster mismatches exist beyond the 7 visible ones; whether each amended annual is a full restatement.
