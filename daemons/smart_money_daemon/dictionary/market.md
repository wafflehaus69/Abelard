# Prices and market reference

Tables: `prices`, `price_spans`, `market_cap`, `ticker_status`, `options_chain_snapshots`, `options_snapshot_passes`. All Yahoo data, except the share counts in `market_cap` (SEC XBRL). Every count below was measured on the mirror.

## Join keys

- **Ticker** is the key to the trade tables. Write `prices.ticker = UPPER(f.ticker)`. Never `UPPER(prices.ticker)`: three lowercase symbols (`emyb`, `fmbm`, `zivo`, 400 rows) duplicate upper-case twins.
- **CIK**: `market_cap.cik` is zero-padded to 10 characters (`0000320193`); `form4_transactions.issuer_cik` is bare (`320193`). A raw join matches 0 rows. `CAST(.. AS INTEGER)` on both sides, or `queries.cik_int`, matches 779 `market_cap` rows.
- Share classes: Yahoo uses a dash (`BRK-B`, `MOG-A`); Form 4 writes `BRK.B`, `MOGA/MOGB`. None of the 16 dotted Form 4 P/S tickers has a price series as written.

## prices

One row = one ticker, one date, one `price_type`. 3,598,351 rows.

| column | meaning | unit |
|---|---|---|
| ticker | Yahoo symbol | |
| date | session date (`eod`); UTC date of the quote (`quote`) | ISO date |
| close | closing price, adjusted for splits only | listing currency per share |
| adj_close | close adjusted for splits and cash dividends | same |
| price_type | `eod` daily bar, or `quote` (a last price saved when a job asked for one) | |
| fetched_at_unix | when the row was last pulled; identical for all rows of one pull | unix seconds |
| asof_unix | `eod`: the bar's timestamp, which is the session open, not the close; `quote`: Yahoo's time of that price | unix seconds |
| source | always `yahoo_v8` | |

**Coverage.** `eod`: 3,597,271 rows, 5,270 tickers, 2006-06-12 to 2026-09-30. `quote`: 1,080 rows, 1,007 tickers. No NULL `close` or `adj_close`. Thin before 2025: 99 tickers have rows in 2015, 1,024 in 2020, 1,637 in 2024, 4,758 in 2025. A series starts where a job first asked for it, not at listing: about 400 days before a nightly pull (2,340 tickers), about 10 days before the ticker's first congressional stock purchase (1,667), at its first Form 4 P/S trade (738) or first 13F period (57); 468 fit none of these. 3,181 series start before the ticker's first Form 4 P/S trade or 13F period. SPY (3,538 rows from 2012-09-04) is the house trading calendar.

**Reach into the trade tables.** Form 4 P/S: 4,465 of 4,720 tickers have a series; 144,172 of 150,371 rows with `date_flag IS NULL` have a close on the exact `tx_date`. Congress: 2,159 of 3,421 tickers; 31,585 of 39,636 tickered rows. 13F: 1,632 of 1,926 tickers.

**What each price carries, shown on the data.**
- `close` is split-adjusted. NFLX insiders sold at $1,164.15 on 2025-10-01 (Form 4); `close` that day is 117.09. The Form 4 price is 9.8 to 10.2 times `close` on all six NFLX sale dates from 2025-09-01 to 2025-11-14, before its 10-for-1 split.
- `close` is not dividend-adjusted; `adj_close` is. KO insider sale on 2026-08-06 at $86.67: `close` 86.85, `adj_close` 86.33. KO's `adj_close / close` steps up at each of 46 ex-dates (by $0.53 at the latest) and is exactly 1.0 from the latest (2026-09-15) on. It also steps down once, on 2025-08-11, where the pull changes: that is the dividend seam below. For NFLX and TSLA, which pay none, `adj_close = close` on every row.
- Both adjustments are as of `fetched_at_unix`, not as of today. Whether spin-offs are in `adj_close`: not established.

**A missing series is never imputed.** No row means no price. Against the SPY calendar, 54 tickers lack 1,235 ticker-sessions inside their own range. No weekend rows; 109 rows on 9 tickers fall on dates SPY did not trade.

### Flags and what excluding them means
No quality flags. `price_type` is the only switch: always filter `price_type = 'eod'`. That drops 1,080 quote rows and 32 tickers that exist only as a quote. Leaving it out double-counts 1,047 (ticker, date) pairs, 25 of which disagree by more than 0.5%.

### Traps
- **Split seam. The big one.** The nightly job re-pulls only the last 400 days, so a later split leaves the series on two bases. APH: `close` 106.70 on 2025-07-28 and 52.65 on 2025-07-29, pulled on 2026-09-02 and 2026-09-03, either side of a 2-for-1 split. A false 51% drop. 151 one-day moves beyond 1.6x sit exactly where `fetched_at_unix` changes, on 98 tickers. The rate of such moves elsewhere predicts about 58 real ones; the two cannot be told apart row by row. Confirmed: APH and IESC (Form 4 prices run 2x `close`), MNST (quote row 2x eod). The comment "historical closes are immutable" in `price_backfill.py` is wrong; the data wins.
- **Dividend seam.** Same cause on `adj_close`: its ratio to `close` falls going forward, impossible on one basis, on 1,153 tickers (1,308 steps, every one where `fetched_at_unix` changes). Median step 0.58%, 90th percentile 1.5%, largest 43% (VISN).
- **Safe zone.** 3,115 tickers were re-pulled on the last night; for 2,943 of them that one pull covers 2025-08-26 onward (40 leftover rows on 5 tickers aside). For any window, require both endpoints to share `fetched_at_unix` (query 1).
- **Form 4 `price` is nominal; `close` is split-adjusted.** Of 143,492 P/S rows with a same-day close (`value_flag` and `date_flag` NULL, price > 0), 7,712 show `price` above 2x `close` and 1,658 below half. Splits are one cause (NFLX, APH); how many are splits is not established.
- **Stale tails.** 2,187 tickers do not reach 2026-09-30; 1,508 end before 2026-08-01. `queries._latest_close` returns the last row whatever its age. `queries._close_on` returns `adj_close`, not `close`.
- **No pre-trade history.** 3,624 tickers start in 2025 or later. 12,107 of the 144,172 priced P/S rows have fewer than 60 sessions in the 120 days before the trade.
- **Small ones.** DAIUF `adj_close` is negative on 440 rows. 172 tickers contain a run of 10 or more identical closes (Yahoo's fill, not ours). Currency is not stored: on 2026-07-23 `IBM.MX` closes at 3,570 against `IBM` at 206.65; the foreign-suffix symbols are `0QZI.IL`, `IBM.MX`, `MSTY.PA`.

## price_spans

One row = one date span the price cache believes it holds for a ticker. 5,204 rows, 5,202 tickers, 2006-06-12 to 2026-09-30. Columns: `ticker`, `start_date` (the start that was requested), `end_date` (last date received), `fetched_at_unix`. Cache bookkeeping only. Do not read coverage from it: `start_date` is earlier than the first price row for 1,351 tickers, `end_date` is later than the last price row for 478 (85 by more than 5 days), and 68 priced tickers have no span row. Use `MIN(date)` and `MAX(date)` from `prices`.

## market_cap

One row = one ticker, computed once and never refreshed. 853 rows.

| column | meaning | unit |
|---|---|---|
| ticker | upper-case symbol (primary key) | |
| cik | issuer CIK, zero-padded to 10 | |
| shares | shares outstanding from SEC XBRL, latest period end | shares |
| shares_asof | period end of that fact | ISO date |
| concept | XBRL tag used: `EntityCommonStockSharesOutstanding` (dei, 643 rows), then the us-gaap fallbacks `CommonStockSharesOutstanding` (32) and `CommonStockSharesIssued` (4) | |
| price | Yahoo quote when the row was computed | USD per share |
| price_asof_unix | Yahoo's time of that quote | unix seconds |
| cap | `shares * price` | USD |
| band | `micro` < $300M, `small` $300M to < $2B, `mid` $2B to < $10B, `large` >= $10B, `unbandable` = no cap | |
| computed_at_unix | when the row was made: 2026-07-24 to 2026-10-01, 630 rows on two July days | unix seconds |

**Coverage.** 791 rows have a CIK. 679 have a share count; 666 have one above 1,000. 678 are banded: micro 252, small 150, mid 119, large 157. By CIK, 767 of 4,779 Form 4 P/S issuers have a row, 659 are banded, 652 have `shares > 0`. By ticker, 26,957 of 150,373 P/S rows land on a banded row (watchlist 4,136 of 17,987; universal 22,821 of 132,386). 13F: 277 of 1,926 tickers banded. Congress: 252 of 3,421.

### Flags and what excluding them means
`band = 'unbandable'` (175 rows): 62 no CIK, 112 no share count (META and MSTR among them), 1 no price. `price` and `cap` are NULL on all 175, `shares` on 174. Excluding them loses nothing usable. `band` alone does not exclude the corrupt rows below.

### Traps
- **Corrupt share counts on real companies.** `shares = 0`: CHYM, GLOO, HLNE, NTSK, SE, SHAK, SPG, VIA. `shares = 100`: CLBK, GLXY, PLNT. `shares = 1,000`: MAIR, SUJA. All 13 are banded `micro`. The us-gaap fallback concepts are where it concentrates: 11 of 36 fallback rows against 2 of 643 dei rows (SPG, CLBK). Filter `shares > 1000`. `queries._shares_outstanding` filters only `shares > 0` and returns 100 for PLNT.
- **More suspects.** One insider's `ownership_after` exceeds `shares` on 20 tickers: the 13 above plus BRCC (92,659 shares, as of 2021), ZDGE, ADTX, GPUS, BRLT, ASPS, XBP. For those seven, corruption is not established; a reverse split gives the same signature.
- **Stale share dates.** 39 rows carry `shares_asof` before 2025 (AMRC 2009, DKS 2011, VALE 2012, NWS 2013), banded as if current. AAL reads 2027-07-17.
- **Frozen in time.** `price` and `band` are as of `computed_at_unix`, not the trade date. At the latest close, 41 of the 678 banded rows would change band.
- **Implausible cap on a foreign filer.** TSM is the largest row at $10.78 trillion (25.9bn shares x $415.58), above NVDA at $5.05 trillion. That fits ordinary shares multiplied by an ADR price; the cause, and how many foreign filers share it, are not established.
- **CIK and ticker are both imperfect keys.** NWS and NWSA share one CIK and one share count, as do TAGS, CORN and WEAT, so a CIK join fans out. For 8 tickers (CLBK, NVRI, NWN, STRR, UBCP, NNOX, VRXA, PNFP) some Form 4 rows under the same ticker carry a different CIK.
- Stale comment: `queries.py` says 13 corrupt of 662 populated in 830 rows. The mirror has 853 rows, 679 populated.

## ticker_status

One row = one ticker that appeared in a congressional stock purchase and had no price series when probed. 449 rows. Columns: `ticker`, `verdict`, `last_trade_date` (latest congressional trade in that ticker, not a market date), `probed_at_unix`, `heuristic` (the rule text, one value).

`verdict` is `delisted_presumed` (346: Yahoo returned nothing and no congressional trade in 24 months) or `data_gap` (103: anything else). It is a heuristic, probed once: 448 rows on 2026-07-22. 26 `data_gap` tickers now have a price series. 14 `delisted_presumed` tickers have Form 4 P/S rows dated 2025-07 or later (KAR, PCH, CMA, MPW among them). Not a delisting record.

## options_chain_snapshots

One row = one option contract on one nightly pull. 596,310 rows. Key: (ticker, snapshot_date, expiry, strike, option_type).

| column | meaning | unit |
|---|---|---|
| snapshot_date | day the pull ran, at about 23:00 US Eastern | ISO date |
| session_date | trading session the chain describes; weekend pulls point back to Friday | ISO date |
| oi_asof | session whose settled open interest the row carries: the weekday before `session_date` | ISO date |
| expiry, strike, option_type | contract terms; `C` or `P` | date, USD |
| contract_symbol | OCC symbol; never shared between tickers | |
| volume | contracts traded in the session; see the stale-volume trap | contracts |
| open_interest | settled after the PRIOR session, not this one | contracts |
| implied_vol | Yahoo's figure; 0.75 means 75% | fraction |
| last_price, bid, ask | premium | USD per share |
| underlying_close | underlying's last price at pull time | USD |
| ingested_at_unix | when the row was written | unix seconds |

**The three dates.** A Tuesday-night pull has `snapshot_date` = `session_date` = Tuesday, `volume` = Tuesday's, `open_interest` = the figure settled after Monday, `oi_asof` = Monday. Tuesday's own closing open interest is the `open_interest` on Wednesday's pull. Checked on 37 consecutive session pairs: of 204,973 contracts whose open interest changed, the change fits inside the prior session's volume for 98.6% and inside the same session's for 78.8%.

**Coverage.** 55 pulls, every calendar day 2026-08-07 to 2026-09-30, none missed; 38 real sessions. 27 tickers every night: ABCL, BITB, BKSY, COIN, CORN, CRCL, CRWV, DJT, ETHA, GFS, GLD, IBIT, LHX, LIN, META, MOG-A, MSTR, NEU, NOW, NTR, ONDS, OSCR, PEW, PLTR, PSQH, TSLA, WEAT. 1 to 6 expiries per ticker per night (mean 4.4; 7 on the two double-run dates below), all within 75 days; about 119 ticker-expiry chains and 10,800 contracts a night. NULLs: `bid` 1,587 rows, `ask` 249, nothing else.

### Flags and what excluding them means
No flags. Keep `snapshot_date = session_date`, drop the holiday and drop `expiry = snapshot_date` (query 3). That leaves 416,167 rows, one pull per real session, and loses no session.

### Traps
- **Expiry day is missing.** No late-night pull has a row with `expiry = snapshot_date`, which fits Yahoo no longer listing that day's expiry by then (the code would keep it if listed). A contract's last captured session is the day before it expires, and summed volume leaves out same-day-expiry trading. Two exceptions: an extra earlier run left expiry-day rows that the 23:00 run did not overwrite, on 2026-08-07 (2,134 rows, 17 tickers, written 19:53 ET) and 2026-09-21 (550 rows on GLD, IBIT, META, TSLA, written 14:20 ET, mid-session). On 2026-08-07 those rows are 56% of the day's summed volume. Exclude `expiry = snapshot_date` for a consistent series.
- **Weekend re-pulls.** 16 of the 55 pulls are Saturday or Sunday copies of Friday (open interest identical on at least 99.86% of shared contracts, every weekend). Grouping by `snapshot_date` counts Friday three times.
- **Holiday.** `session_date = 2026-09-07` (Labor Day) is Friday's chain again: identical to the Sunday 2026-09-06 re-pull on 11,037 of 11,037 contracts. Its `oi_asof` is wrong, and so is the next pull's (2026-09-08 says 2026-09-07; the true prior session is 2026-09-04).
- **Stale volume.** Of 372,421 contract pairs across consecutive sessions, 121,701 (33%) repeat the same non-zero volume and the same last price. One ABCL put reads volume 82, last 0.04, ten sessions running. That fits Yahoo carrying the last traded day's volume on a contract that did not trade, instead of 0. The last trade date is not stored, so repetition is the only test. For these rows the schema comment "`volume` = snapshot_date's trading" does not hold.
- **Zero can mean missing.** `volume` and `open_interest` are never NULL: a missing value is written as 0 (38,248 and 36,417 zero rows; how many were missing is not recoverable).
- **Placeholder IV.** `implied_vol` is 0.00001 or 0 on 17,044 rows. Exclude `implied_vol < 0.0001`.
- **MOG-A joins to nothing.** No price series and no Form 4 rows under that symbol.
- `underlying_close` is within 0.1% of `prices.close` on 986 of 988 comparable ticker-nights.

## options_snapshot_passes

One row = one nightly pull. 55 rows. Columns: `snapshot_date`, `session_date`, `oi_asof`, `tickers` (universe, 32 every night), `ok` (27), `no_chain` (5; symbols for which Yahoo returned no chain, not named), `gaps` (failed tickers, 0 every night), `contracts`, `dropped` (0), `ran_at_unix`. A missed night would show here; there is none. `contracts` counts the day's last run only. It is below the table's row count on two dates (2026-08-07: 10,568 vs 12,702; 2026-09-21: 9,768 vs 10,318): the difference is the expiry-day rows an earlier run left behind (see the expiry-day trap). `queries.py` has no read function for either options table; `options_chain.confirm_t1` compares the last two sessions only.

## Example queries (each run on the mirror)

1. 20-session forward return after an insider purchase, on one price basis. 18,804 rows.

```sql
WITH px AS (
  SELECT ticker, date, close, adj_close, fetched_at_unix,
         LEAD(adj_close, 20)       OVER w AS adj_close_20,
         LEAD(fetched_at_unix, 20) OVER w AS fetched_20
  FROM prices
  WHERE price_type = 'eod'
  WINDOW w AS (PARTITION BY ticker ORDER BY date)
)
SELECT f.accession, f.tx_index, px.ticker, f.tx_date, f.price AS exec_price,
       px.close, px.adj_close_20 / px.adj_close - 1 AS ret_20_sessions
FROM form4_transactions f
JOIN px ON px.ticker = UPPER(f.ticker) AND px.date = f.tx_date
WHERE f.code = 'P'
  AND f.date_flag IS NULL                    -- tx_date is used, so stay on the trade clock
  AND px.adj_close > 0
  AND px.fetched_20 = px.fetched_at_unix;    -- both ends from one pull: no seam
```

The last line is conservative: it also drops windows that cross two pulls with no split between them. It does not remove Yahoo's own errors: 96 rows still show a move beyond 100% (OPAD 0.74 to 6.17 overnight inside one pull; real or not is not established). `value_flag` is not needed because no dollar value is used.

2. Purchases by cap band, joined on CIK, corrupt share counts out.

```sql
WITH mc AS (                -- one row per CIK (SQLite keeps the MAX(cap) row's band)
  SELECT CAST(cik AS INTEGER) AS cik_i, band, shares, MAX(cap) AS cap
  FROM market_cap
  WHERE band <> 'unbandable' AND shares > 1000
  GROUP BY CAST(cik AS INTEGER)
)
SELECT COALESCE(mc.band, 'no usable market_cap row') AS band,
       COUNT(DISTINCT f.issuer_cik) AS issuers, COUNT(*) AS buy_rows
FROM form4_transactions f
LEFT JOIN mc ON mc.cik_i = CAST(f.issuer_cik AS INTEGER)
WHERE f.code = 'P' AND f.date_flag IS NULL
GROUP BY 1 ORDER BY 3 DESC;
```

Result: no usable row 2,356 issuers / 16,433 rows; micro 235 / 3,930; small 141 / 2,159; large 85 / 1,043; mid 94 / 743.

3. Daily option volume against the right open interest. 1,026 rows (27 tickers x 38 sessions).

```sql
WITH s AS (                 -- one pull per real session
  SELECT ticker, session_date, oi_asof,
         SUM(volume) AS volume, SUM(open_interest) AS oi_prior_session
  FROM options_chain_snapshots
  WHERE snapshot_date = session_date
    AND expiry > snapshot_date               -- expiry-day rows exist on two dates only
    AND session_date IN (SELECT date FROM prices
                         WHERE ticker = 'SPY' AND price_type = 'eod')
  GROUP BY ticker, session_date
)
SELECT ticker, session_date, volume, oi_prior_session,
       LEAD(oi_prior_session) OVER (PARTITION BY ticker ORDER BY session_date)
         AS oi_end_of_session,
       ROUND(1.0 * volume / NULLIF(oi_prior_session, 0), 3) AS vol_to_prior_oi
FROM s
ORDER BY ticker, session_date;
```

`volume` here still includes the stale carried figures, so the sum overstates quiet contracts. `oi_end_of_session` is summed over the next pull's contracts, and the expiry set changes on 245 of 999 ticker-session steps (one expiry drops out, another enters); for a like-for-like change in open interest, join per `contract_symbol`.
