# DATA DICTIONARY - the smart_money corpus

Read this page first; the per-table detail is in `dictionary/`. Every number here was measured on the mirror (snapshot 2026-10-01 19:09 ET, code c3b7dbb) and re-measured by an independent fact-check.

**Where the data is.** Analysis runs on the mirror, never on Basilic's production database. The mirror is in WSL on Orban at `~/.openclaw/smart_money/smart_money_v0.db`; refresh it with `scripts/sync_from_basilic.sh`. Open it read-only:

```python
import sqlite3
con = sqlite3.connect("file:/home/wafflehouse/.openclaw/smart_money/smart_money_v0.db?mode=ro", uri=True)
```

Neither Python on Orban has pandas. The recipes in `recipes/` run on the standard library and return a DataFrame only if pandas is installed.

## Known defects in the data, as of this snapshot

These are not traps in how to read the data. They are things the data gets wrong today.

1. **`plan_flag` and `role` are wrong on filings that write their checkboxes as `true`.** The parser accepts only `1` (`form4.py:116-123`) and never reads `isOther`. Confirmed against the raw EDGAR XML on 2026-10-01: Sundar Pichai's GOOGL filing `0001193125-26-114850` says `aff10b5One=true`, `isDirector=true`, `isOfficer=true`, title Chief Executive Officer, with a 10b5-1 footnote; the database holds `plan_flag = 0`, `role` NULL. Size: 106,366 rows in 51,063 filings carry `plan_flag = 0` with `role` NULL, and that is the ceiling on what is affected. Among sales, the plan share is 61.7% where the role was read and 20.0% where it is NULL.
   **Rule until it is re-parsed:** `plan_flag = 0 AND role IS NULL` means "plan status not read", not discretionary. `role IS NULL` means "not read", not "no role".
2. **`value_flag = 'price_vs_close'` (495 rows) is partly a false positive.** It compares the filing's nominal price with `prices.close`, which is restated for later splits. 238 of the flagged rows are option exercises at the strike, and among purchases and sales 195 of 208 sit in the direction a later split produces. Their `value` is NULL although the trade is real. Detail: `dictionary/form4.md`.
3. **39 rows exceed the guard's own ceilings and carry no flag.** `SUM(value)` over every code is $1.137 quadrillion, almost all of it one code-J row. `value_flag IS NULL` does not mean checked. Sum dollars for P and S only.
4. **Unmarked congressional repeats.** 3,444 live rows repeat member, date, ticker, side and band in another filing; 3,226 are Senate, where `filing_status` is never filled. Amendment or separate trade: not established.
5. **Two 13F filers have filings that stored no rows.** Thiel Macro and Founders Fund Growth II each filed a 13F-HR for 2026-03-31 and the ingest stored nothing from it; Thiel Macro's 2025-12-31 looks the same. Empty filing or parse miss: not established.

## Part 1 - Identity keys

| Who / what | Key | Tables | Stored as |
|---|---|---|---|
| Form 4 insider | `reporting_cik` | form4_transactions, form4_derivatives | text, zero-padded to 10 |
| Form 4 issuer | `issuer_cik` | same | text, bare |
| 13F manager | `cik` | thirteenf_* | text, bare. 28 filers. No name column: names are in `~/.openclaw/smart_money/analysis/registry.json`, entries with role `manager_13f` (CIK padded) |
| 13F security, issuer | `cusip`, `issuer_id` | thirteenf_holdings | 9 chars; its first 6 (generated) |
| Congress member, trades | `person_id` | congress_trades -> persons | integer |
| Congress member, annual holdings | `chamber, member_last, member_first, state_dist` | congress_holdings, congress_member_roster | text. `congress_holdings.person_id` is NULL on all 146,093 rows |
| A security across sources | `ticker` | all | a label, not a key |

### CIK format, every column that holds one

| Column | Rows | Format | Distinct |
|---|---|---|---|
| form4_transactions.reporting_cik | 351,519 | padded-10, 100% | 46,905 |
| form4_transactions.issuer_cik | 351,519 | bare, 100% | 5,689 |
| form4_derivatives.reporting_cik | 17,997 | padded-10, 100% | 6,055 |
| form4_derivatives.issuer_cik | 17,997 | bare, 100% | 1,702 |
| thirteenf_holdings.cik | 13,697 | bare, 100% | 28 |
| thirteenf_filing_meta.cik | 225 | bare, 100% | 28 |
| thirteenf_filings_seen.cik | 230 | bare, 100% | 28 |
| thirteenf_baseline.cik | 28 | bare, 100% | 28 |
| market_cap.cik | 853 | padded-10 on 791, NULL on 62 | 788 |
| persons.cik_or_chamber, type='insider' | 51,625 | padded-10, 100% | 51,555 |
| persons.cik_or_chamber, type='congress' | 444 | the word `house` or `senate` | 2 |
| watermarks.source (`form4_backfill:<cik>`) | 27 | padded-10 inside the key | 27 |

No column mixes formats. The two CIK columns of one Form 4 row differ from each other.

One filing has one `reporting_cik`: the parser reads only the first reporting owner (`form4.parse_ownership`), and 0 of 169,671 accessions carry two. How many filings were joint: not established.

**Rule: join CIKs through an integer cast, never raw.** Python `queries.cik_int(x)`; SQL `CAST(x AS INTEGER)` on both sides. Filter `type='insider'` first: `CAST('house' AS INTEGER)` is 0.

```sql
-- raw: 0 rows
SELECT COUNT(*) FROM form4_transactions f
WHERE EXISTS (SELECT 1 FROM market_cap m WHERE m.cik = f.issuer_cik);
-- integer: 80,266 rows
SELECT COUNT(*) FROM form4_transactions f
WHERE EXISTS (SELECT 1 FROM market_cap m
              WHERE CAST(m.cik AS INTEGER) = CAST(f.issuer_cik AS INTEGER));
```

### Ticker: each source keeps its own notation

| Table | Share class | Not a symbol |
|---|---|---|
| form4_transactions (issuer's own field, verbatim; 879 rows not upper-case) | dot, or both classes in one string: `BRK.A` 25, `BRK.B` 5, `LEN, LEN.B`, `MOGA/MOGB`. Alphabet is `GOOGL` only | 2,493 placeholder rows over 217 issuers: `NONE` 1,747, `N/A` 481, `NA` 199, `none` 31, `-` 28, `None` 5, `N.A.` 2 |
| thirteenf_holdings (OpenFIGI lookup of the CUSIP) | slash: `BRK/A`, `BRK/B`, `LEN/B` | NULL on 1,136 rows ($216bn, 4.7% of value); 325 bond descriptors (`GOOGL 6.25 05/15/29 A`) |
| congress_trades (upper-case) | `BRK.B` 169, `BRK-B` 24, `BRKB` 3, `BRK` 1 | NULL on 7,512 rows, 1,387 of them Stock; `--` on 19 |
| congress_holdings (upper-case) | `BRK.B` 297, `BRK-B` 51, `BRKB` 6, `BRK` 8 | NULL on 67,876 rows (46%) |
| prices (what Yahoo was asked) | dash: `BRK-B`; no BRK.A series | - |

Berkshire B has a different spelling in Form 4, 13F and prices, and four in congress. No table stores `BRK B` with a space.

- Form 4 issuer = `CAST(issuer_cik AS INTEGER)`. 232 issuers appear under 2+ ticker strings (10,778 rows); 26 real tickers map to 2 issuer CIKs each.
- `queries.norm_ticker(t)` returns an upper-case symbol, or None for a placeholder. It misses the bracketed forms `[NONE]`, `[none]`, `[NONE`, `NONE]` (7 rows).
- To prices: `prices.ticker = UPPER(f.ticker) AND price_type = 'eod'`. 4,465 of 4,720 Form 4 P/S tickers have a series. Berkshire B needs `.` turned to `-` first.

### CUSIP and issuer_id

- CUSIP exists only on the 13F side. Form 4 and congress reach 13F through ticker.
- `issuer_id` joins the classes of one issuer: `02079K` = GOOG + GOOGL, `084670` = BRK/A + BRK/B.
- It also joins every fund of one trust: `464287` is eight iShares ETFs (EEM, IWM, IVV, TLT ...). 34 issuer_ids hold 2+ common tickers; the 10 with 3+ are fund trusts or tracking-stock groups. Roll up companies on `issuer_id`, funds on `cusip`.

### persons, and the other identity tables

| Table | One row is | Rows |
|---|---|---|
| persons | one distinct name (`name` is UNIQUE) | 52,069: 51,625 insider, 444 congress |
| cusip_ticker | one CUSIP and its resolved ticker | 2,141; ticker NULL on 213 |
| congress_member_roster | one annual-report filer identity | 916; party NULL on 282, bioguide NULL on 290 |

| persons column | Meaning |
|---|---|
| person_id | integer key; only congress_trades points at it |
| name | insider: as EDGAR prints it. Congress: `Last, First` |
| type | `insider` or `congress` |
| cik_or_chamber | insider: CIK padded to 10. Congress: `house` (348) or `senate` (96) |
| meta | insider: role text, NULL on 17,014. Congress: JSON - House `state_dst, prefix, suffix`; Senate `first, last, office` |

- **Insiders: key on `reporting_cik`, never persons.** 69 CIKs have 2+ persons rows (13 differ only in case; the rest are name changes, initials and renamed entities); 11 names shared by 2 CIKs collapse into one row.
- **Congress trades: `person_id` resolves on every row.** 24 Senate persons rows hold a leaked eFD JSON blob as the name and carry 167 trades.
- **Chamber: read `congress_trades.chamber`.** persons disagrees on 706 trades (4 members who moved to the Senate).
- **Annual holdings: no person_id.** `queries.resolve_person` maps 251 of the 916 identities to a trades person_id (77,270 rows, 53%).

## Flags, and what excluding them means

| Flag | Set on | Excluding the flagged rows |
|---|---|---|
| `date_flag` (Form 4, derivatives, congress) | 14 / 4 / 27 rows | Right on the trade clock. Wrong on the filing clock: the row is a valid disclosure |
| `date_subclass` | `non_trading_day` with date_flag NULL: 271 Form 4 P/S rows, 100 congress | Do not exclude. A mark |
| `tx_date_suggested` | 6 congress rows, 0 Form 4 | A hypothesis. Never the date |
| `value_flag` (Form 4) | 553 rows | Drops dollars only; `value` is already NULL there. Keep the row for share and person counts. NULL does not mean clean (trap 3) |
| `superseded` (congress_trades) | 392 rows (238 House, 154 Senate) | `= 0` is right for every analysis. It is set from `filing_status` or an "amend" report label (`amendments.py`); an amendment carrying neither stays live (see Unmarked congress repeats) |
| `filing_status` (congress_trades) | New 25,579, Amended 437, Deleted 35 | Do not filter on it. NULL on 21,117 rows means not extracted: all 14,300 Senate rows, and 6,817 House rows disclosed 2025-01 to 2026-07 |
| `value_scale` (13F) | 1000 on 808 rows / 17 filings; never NULL | Never exclude. Read `value_usd` |
| `instrument_class` (13F) | non-common on 962 rows, $137.4bn (3.0%); never NULL | `= 'common'` is the equity book. Option rows carry the value of the underlying shares, not premium; a put is not a short (the house says put-heavy) |
| `ingest_regime` (Form 4) | watchlist 23,049 rows / 29 issuers; universal 328,470 / 5,672 | `= 'universal'` is not the full cross-section: it drops 8,913 rows of 23 watchlist issuers filed since the universal start (CRWV, META, COIN). Use `filed_date >= '2025-07-23'` |
| `plan_flag` (Form 4) | 1 on 57,490 of 126,065 S rows, 581 of 24,308 P rows | `= 1` is a 10b5-1 plan trade. **`= 0` is NOT reliably discretionary** - see Known defect 1. Set per filing, not per row |

## Part 2 - The five traps

Chosen on measured size, and on whether the wrong answer still looks plausible.

**1. A raw CIK join returns nothing and looks like absent data.**
- Size: Form 4 issuer to market_cap, 0 rows raw, 80,266 by integer. Registry CIK `0001536411` against 13F, 0 raw, 630. The comment on `queries._cik_key` records the incident: every ownership percentage silently read None.
- Rule: cast both sides to integer, every time.

**2. Two clocks. The trade date is not when the market knew.**
- Size, looking ahead: all 225 13F filings were filed 30 to 66 days after `period` (median 45). Congress stock trades (live, unflagged): median 28 days to disclosure, 20% over 45 (7,387 of 36,853). Form 4 P/S: 96.9% within 5 days.
- Size, bad dates: with `date_flag` rows left in, Form 4 `MIN(tx_date)` is `0025-07-25`, congress `MAX` is `3031-04-30`, and congress mean `lag_days` is 73.4 against 84.2. The nightly brief printed those endpoints. Dropping them on the filing clock loses 45 valid disclosures.
- Not flagged: 170 Form 4 rows dated more than 5 days before the first filing collected, 2021-08-24 (earliest 2002-02-24). Late reports, amendments or date errors; no range check tells them apart.
- Rule: pick the clock first. What was knowable: `filed_date` / `disclosure_date`, no date filter. When it happened: `tx_date` with `date_flag IS NULL`.

**3. Form 4 dollars. Two tickers are half the total.**
- Size, flag set: the 186 flagged P rows rebuild from `shares * price` to $7.05 quadrillion, 110,854 times the unflagged P total of $63.57bn.
- Size, flag NULL: 8 P rows above $1bn are $31.05bn, 48.8% of P dollars. SVRE is $27.88bn from one filer (per the code comments, a per-ADS price on an ordinary-share count). MRUS is $7.59bn, 11 rows, all Genmab A/S at $97.00: a corporate acquirer, not an insider buy (that it is Genmab's takeover is inferred from the single filer and price, not checked against the filings). Placeholder ticker `NONE` ranks third at $3.98bn.
- `value` is also NULL on 25,887 unflagged rows that report no price.
- Rule: sum `value`, never `shares * price`. Exclude a bad filer whole, not row by row: SVRE's 11 rows under $1bn are still $4.19bn. For totals and rankings use `q.clean_subset(q._fetch_f4(con, ("P",), "0001-01-01", "2026-10-02", plan="all"))`, which leaves $22.3bn in 19,612 rows of $63.2bn in 23,159.

**4. 13F `value` has no unit. `value_usd` does.**
- Size: 17 of 225 filings (808 rows) are in thousands - every filing of Duquesne (1536411) and Baupost (1061768). At 2026-06-30 raw `value` ranks them 24th and 25th of 25 books, at $5.4m and $5.2m; `value_usd` ranks them 15th and 16th, at $5.42bn and $5.21bn. The corpus total moves 1.5%, so a grand total hides it. The schema comment records ten read sites, three of which applied the scale.
- No filer changes unit on the mirror. The schema comment records Duquesne switching unit between quarters in 2022, before the mirror's first period, which is why the scale is per filing. `thirteenf_filing_meta.value_total` (the cover-page check) is filled on 8 of 225 filings.
- Rule: read `value_usd`. Never `value`.

**5. Rows are not independent events.**
- Size: 22,664 discretionary P rows with a real ticker (`code='P' AND plan_flag=0`, placeholders dropped) are 13,638 filings, 6,556 issuer-months by trade month and 2,748 issuers - 3.5 rows per issuer-month. With placeholders kept: 23,727 rows, 14,537 filings, 7,025 issuer-months, 2,890 issuers.
- The house test (`scans/U1_DISCOVERY_20260726.md`): 737 cluster events gave naive t = 4.17 / 1.70 / 2.21 at 21 / 63 / 126 days. With one observation per calendar month: 0.61 / -0.19 / -1.29, over 16 / 14 / 11 months. On the mirror `discovery.clusters` finds 1,026 clusters in 19 months, 31% of them in three months.
- Also: 313 identical blocks (ticker, date, shares, price, post-trade holding) are reported by 2+ filers - 2.5% of the dollars in those 22,664 rows.
- Rule: one event per issuer per 30-day window, dated on the filing clock, standard errors over calendar months. N is months. `queries.q_cluster_context(con, lookback="all")` builds such events for issuers with 3+ distinct buyers (887 on the mirror; `event_filed` is the filing-clock date); its default looks back only 180 days.

### Runners-up
- **Split basis.** `prices.close` and `adj_close` are split-adjusted (`adj_close` for dividends too); Form 4 `price` and `shares` are as filed. 9,825 of 143,492 unflagged P/S rows with a same-day close (6.8%) are over 1.5x from it, all causes: MSTR 2,652 rows at ~10x, CVNA 2,020 at 5x. `value` is right; per-share comparisons and share sums across a split are not. No split table exists.
- **Coverage break.** Form 4 filed before 2025-07-23 is 26 chosen issuers (29 watchlist issuers in all): 16 issuers in 2025-06, 2,472 in 2025-08. Table II is universal only from 2026-07-28.
- **Share-class twins.** At 2026-06-30 GOOGL has 12 holders and $33.5bn, GOOG 8 and $18.0bn; the issuer has 14 and $51.5bn. 719 common rows ($208.9bn) are in issuer_ids with 2+ tickers.
- **Congress amounts are bands.** 70% of rows are $1,001 to $15,000. Live stock rows sum to $0.59bn low, $1.98bn high. `amt_high` is NULL on 79 open-top rows.
- **Unmarked congress repeats.** 3,444 live rows repeat member, date, ticker, side and band in another filing; 3,226 are Senate, where `filing_status` is never filled. Amendment or separate trade: not established.
- **Placeholder tickers.** `GROUP BY ticker` folds 217 issuers into seven fake companies; `NONE` alone is 153 issuers.
- **Form 4 codes.** Only P (24,308) and S (126,065) are market trades; 57.2% of rows are grants, withholdings, exercises, gifts.
- **House dedup has no price in its key.** `queries._dedup_amendments` removes 1,149 P and 7,596 S rows (2.7% of S dollars); at least 470 and 4,381 of them are equal-size fills at a different price in the same filing as the row kept, not amendments.
- **prices.** Filter `price_type = 'eod'`: 1,047 ticker-days also have a `quote` row. 1,831 of 5,270 tickers have no close after 2026-08-31.
- **Annual holdings.** 301 of 2,399 member-years have 2+ documents (37,072 rows). Why: not established.
- **13F par rows.** `shares_type = 'PRN'` (46 rows, 43 of them convertible notes): `shares` is dollars of par.

### Comment against data (the data wins)
- `db.py`: a NULL value_flag "means the value is trusted". See trap 3.
- `queries.py` header: reporting_cik is "verbatim from EDGAR". True, but it reads as "format unknown": what EDGAR writes is padded to 10, on every row here.
- `discovery._p_buys`: watchlist rows the universal walk never fetched "stay OUT". Both backfills write one ledger, so 23,044 of 23,049 come in.
- Counts quoted in comments are stale: SVRE "46.84% of $59.52bn" is now 43.9% of $63.57bn.

`smart_money.scorecard` and `.clustering` need pandas and do not import on this machine. `queries`, `discovery`, `dates` and `instrument` do.

## Date ranges present

| Corpus | Filing clock | Trade or period clock |
|---|---|---|
| Form 4 Table I | filed 2021-08-24 to 2026-09-30; every filer from 2025-07-23 | tx_date unflagged 2002-02-24 to 2026-09-29 |
| Form 4 Table II | filed 2021-08-10 to 2026-09-29; every filer from 2026-07-28 | tx_date unflagged 2009-09-09 to 2026-09-29 |
| 13F | filed 2024-02-14 to 2026-08-14 | period 2023-12-31 to 2026-06-30; 15+ filers from 2024-06-30 |
| Congress trades | Senate from 2014-01-29, House from 2018-04-27, to 2026-09-28 | tx_date unflagged 2012-06-14 to 2026-09-22 |
| Congress holdings | filing_date 2022-01-20 to 2026-09-28 | coverage_year Senate 2016-2026, House 2022-2026 |
| prices (eod) | - | 2006-06-12 to 2026-09-30, 5,270 tickers |

## Example queries, correct for the flags

All three run on the mirror, opened with `q.connect_ro(path)`.

**Insider buying by month, filing clock.** The date bound, not the regime tag, selects full coverage. No `date_flag` filter: a flagged row is still a disclosure. Rows with a `value_flag` have NULL `value`: they count as rows (178 here) and add no dollars. `bad` takes the dollars of every (ticker, filer) with any P row above the $1bn house review bar - SVRE/VisionWave and MRUS/Genmab, 29 rows - out of the sum; the rows still count. A row-by-row `value < 1e9` leaves $4.43bn of theirs in (2026-04 reads $3,086m).

```sql
WITH bad AS (
  SELECT DISTINCT UPPER(ticker) AS tk, reporting_cik
  FROM form4_transactions
  WHERE code = 'P' AND value > 1e9)
SELECT substr(f.filed_date,1,7)                      AS filed_month,
       COUNT(*)                                      AS p_rows,
       COUNT(DISTINCT f.accession)                   AS filings,
       COUNT(DISTINCT CAST(f.issuer_cik AS INTEGER)) AS issuers,
       ROUND(SUM(CASE WHEN b.tk IS NULL THEN f.value END)/1e6) AS usd_m
FROM form4_transactions f
LEFT JOIN bad b ON b.tk = UPPER(f.ticker) AND b.reporting_cik = f.reporting_cik
WHERE f.code = 'P' AND f.plan_flag = 0
  AND f.filed_date >= '2025-07-23'
GROUP BY 1 ORDER BY 1;
-- 2025-08: 2,131 rows, 1,291 filings, 593 issuers, $1,719m
-- 2026-04: 1,139 rows,   705 filings, 313 issuers, $1,008m
-- 2026-09: 1,687 rows, 1,038 filings, 474 issuers, $2,350m
```

**The 13F book the market could see on a date.** `filed_date`, `value_usd`, common only, rolled up on `issuer_id`. On 2026-06-30 the newest known book was 2026-03-31 for 24 filers and older for 4.

```sql
WITH known AS (
  SELECT cik, MAX(period) AS period
  FROM thirteenf_holdings
  WHERE filed_date <= '2026-06-30'
  GROUP BY cik)
SELECT h.issuer_id, MIN(h.issuer) AS name,
       GROUP_CONCAT(DISTINCT h.ticker) AS tickers,
       COUNT(DISTINCT h.cik) AS holders,
       ROUND(SUM(h.value_usd)/1e9, 2) AS usd_bn
FROM thirteenf_holdings h
JOIN known k ON k.cik = h.cik AND k.period = h.period
WHERE h.instrument_class = 'common'
GROUP BY h.issuer_id
ORDER BY SUM(h.value_usd) DESC LIMIT 5;
-- 037833 APPLE INC            AAPL         6 holders  58.04
-- 025816 AMERICAN EXPRESS CO  AXP          3          46.36
-- 191216 COCA COLA CO         KO           2          30.43
-- 02079K ALPHABET INC         GOOGL,GOOG  14          27.32
-- 060505 BANK AMERICA CORP    BAC          3          25.19
```

**Congress stock purchases, disclosure clock, as a range.** `superseded = 0`, both band bounds, no `date_flag` filter. For the trade clock group on `tx_date` and add `AND date_flag IS NULL`.

```sql
SELECT substr(disclosure_date,1,4)       AS disclosed,
       COUNT(*)                          AS live_rows,
       COUNT(DISTINCT person_id)         AS members,
       ROUND(SUM(amt_low)/1e6, 1)                     AS usd_m_low,
       ROUND(SUM(COALESCE(amt_high, amt_low))/1e6, 1) AS usd_m_high
FROM congress_trades
WHERE superseded = 0 AND asset_type = 'Stock' AND side = 'purchase'
  AND disclosure_date >= '2023-01-01'
GROUP BY 1 ORDER BY 1;
-- 2025: 3,595 rows, 66 members, $31.2m to $122.6m
-- 2026: 2,215 rows, 53 members, $18.6m to $72.8m
```

## Where to go next

| For | Read |
|---|---|
| Form 4: codes, dollars, amendments, derivatives | `dictionary/form4.md` |
| Congress: trades, annual holdings, amendments, member identity | `dictionary/congress.md` |
| 13F: units, instrument classes, share-class twins, the registry | `dictionary/thirteenf.md` |
| Prices, market cap, survivorship, option chains | `dictionary/market.md` |
| Every CSV endpoint and its parameters | `EXPORTS.md` |

Three recipes, each of which states the flags it excludes and why. Run them under WSL:

```
python3 recipes/filer_qoq.py --cik 1536411           # one filer, every period, QoQ deltas, exits as rows
python3 recipes/insider_buys.py --since 2026-07-01   # discretionary buys, entry vs latest close, by role and band
python3 recipes/opposed_pairs.py --union-twins       # opposed pairs at a period, share classes merged
```

Each takes `--db PATH` (default the mirror), `--csv OUT` and `--help`.
