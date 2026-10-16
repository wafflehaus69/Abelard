# 13F: manager holdings

Tables: `thirteenf_holdings`, `cusip_ticker`, `thirteenf_filings_seen`, `thirteenf_baseline`, the arrival ledger (`thirteenf_arrivals`, `thirteenf_feed_reads`), plus the filer registry (`registry.json`). Every number below is measured on the mirror (snapshot 2026-10-01). These tables were last written 2026-08-18/19. Q3 2026 filings are due around 2026-11-16.

**This is not the 13F universe.** It is 28 hand-picked filers, about 8 quarters each. "N filers hold X" means N of 28.

## thirteenf_holdings

**One row** = one filer (`cik`), one filing (`accession`), one `cusip`, one `put_call` bucket. **13,697 rows**, 28 filers, 225 filings. Every (cik, period) has exactly one filing, so (cik, period, cusip, put_call) is unique too (0 duplicates). Several lines for one cusip and bucket in a filing are summed at ingest.

**Dates present:** `period` 2023-12-31 to 2026-06-30 (11 quarter-ends). `filed_date` 2024-02-14 to 2026-08-14. **No 2026-09-30 rows (0).**

| column | meaning | unit |
|---|---|---|
| `cik` | filer CIK, bare digits (trap 8) | text |
| `accession` | SEC filing id | text |
| `period` | quarter-end the positions are as of | ISO date |
| `filed_date` | day the filing reached EDGAR: when the market knew | ISO date |
| `cusip` | 9-char CUSIP as the filer typed it. The position key | text |
| `issuer_id` | CUSIP characters 1-6, generated (trap 7) | text |
| `ticker` | copied from `cusip_ticker` at ingest. NULL on 1,136 rows (trap 6) | text |
| `issuer` | issuer name as filed. 388 CUSIPs have more than one spelling | text |
| `put_call` | `long` / `call` / `put` | text |
| `value` | VALUE as filed, raw. The form states no unit | filer's unit |
| `value_scale` | 1 or 1000, decided per filing. Never NULL in the mirror | multiplier |
| `value_usd` | `value * value_scale`, generated. **Use this** | US dollars |
| `shares` | share count if SH; par dollars if PRN; underlying shares on option rows. As filed, never split-adjusted | traps 3, 4, 13 |
| `shares_type` | `SH` / `PRN`. NULL on 6,063 rows (trap 3) | text |
| `title_of_class` | filer's class label (COM, CL A, NOTE ...). NULL on 6,327 rows | text |
| `instrument_class` | what the line is. Never NULL in the mirror (see flags) | text |

Left out: `ingested_at_unix`.

### Periods

| period | rows | filers | filed, first to last | value_usd, $bn |
|---|---|---|---|---|
| 2023-12-31 | 25 | 1 | 2024-02-14 | 0.09 |
| 2024-03-31 | 18 | 2 | 2024-05-15 | 0.12 |
| 2024-06-30 | 658 | 15 | 2024-07-30 to 08-14 | 92.10 |
| 2024-09-30 | 1,600 | 24 | 2024-11-07 to 11-14 | 503.09 |
| 2024-12-31 | 1,644 | 27 | 2025-02-07 to 03-07 | 501.56 |
| 2025-03-31 | 1,597 | 27 | 2025-05-02 to 05-15 | 483.82 |
| 2025-06-30 | 1,606 | 27 | 2025-08-01 to 08-15 | 523.44 |
| 2025-09-30 | 1,599 | 27 | 2025-11-03 to 11-14 | 559.65 |
| 2025-12-31 | 1,606 | 26 | 2026-02-04 to 02-18 | 581.19 |
| 2026-03-31 | 1,663 | 24 | 2026-05-05 to 05-18 | 569.92 |
| 2026-06-30 | 1,681 | 25 | 2026-08-05 to 08-14 | 775.44 |

The thin early periods are the ingest window (the newest 8 or 9 filings per filer), not small books. Do not trend totals before 2024-09-30. Totals include option notional (trap 4); 2026-03-31 includes a $16.59bn bad row (trap 2); 2026-06-30 includes $122.32bn of SpaceX, which first appears that quarter (7 filers; $115.15bn of it Alphabet and NVIDIA).

No rows where you might expect them:
- 2026-06-30: Pershing Square (last period 2026-03-31), Scion (last 2025-09-30), Founders Fund VII (last 2025-12-31). Not an ingest miss. Read from EDGAR on 2026-10-02: Pershing Square filed a **13F-NT** for 2026-06-30 on 2026-08-14, a notice that its holdings are reported by PERSHING SQUARE INC. (CIK 2026053), which is not a tracked filer; that filer's 13F-HR for the quarter has 15 rows worth $19.47bn. Scion has filed nothing of any form since 2025-11-03 and Founders Fund VII nothing since 2026-02-17. Why those two stopped: not established.
- Gaps: Thiel Macro has nothing for 2025-12-31 and 2026-03-31; Founders Fund Growth II nothing for 2026-03-31. All three are **filed-empty**, not a parse miss: each is a 13F-HR whose cover states a table value of 0 and whose table is one placeholder row (CUSIP `000000000`, value 0). Read from the covers on 2026-10-02.

A missing filer-period is not evidence the filer sold everything.

### period vs filed_date

**Use `filed_date` for when the market knew, never `period`.** Lag in days, one observation per filing (n = 225):

| min | p10 | p25 | median | p75 | p90 | max | mean |
|---|---|---|---|---|---|---|---|
| 30 | 41 | 44 | 45 | 45 | 45 | 66 | 44.2 |

145 filings landed on day 45 exactly, 61 earlier, 19 later. The late ones: 16 of the 26 filings for 2025-12-31 came on day 48 or 49 (day 45 was a Saturday before a Monday holiday); Affinity 66 days (2024-12-31); Situational Awareness 48 (2026-03-31); Duquesne 46 (2025-06-30). Earliest: Amazon 30, Alphabet 32-33.

### Value units

- `value` is raw as filed. `value_scale` is resolved once per filing. `value_usd` is dollars.
- 208 filings (12,889 rows, 26 filers) are in dollars. **17 filings (808 rows) are in thousands, from two filers:**
  - **Baupost Group**: all 8 of its periods, 2024-09-30 to 2026-06-30.
  - **Duquesne Family Office**: all 9 of its periods, 2024-06-30 to 2026-06-30.
- **No filer changes scale between periods in the mirror.** `db.py` records Duquesne filing whole dollars once, at 2022-12-31. That quarter is outside the mirror, so it cannot be checked here.
- `value_scale` NULL: **0 rows.** If it is ever NULL, `value_usd` silently equals raw `value`. Check for NULLs on new filings.
- My own check: per filing, the median of (`value_usd` / `shares`) / quarter-end close over its 20 largest common long lines. 223 of 225 filings fall between 0.95 and 1.05, including all 17 thousands filings. The other two are both Founders Fund VII: 2024-09-30 reads 1.99 on a single position (not a 1000x error), and 2024-03-31 has no priced line.

### Flags and what excluding them means

There is no quarantine flag on this table and nothing is dropped at ingest. Two columns classify a row.

| `put_call` | rows | value_usd, $bn | filers |
|---|---|---|---|
| long | 13,247 | 4,498.37 | 28 |
| put | 257 | 77.67 | 10 |
| call | 193 | 14.40 | 11 |

Excluding put and call leaves positions at market value. Including them adds $92.07bn of underlying notional, which is not money invested.

| `instrument_class` | rows | value_usd, $bn | note |
|---|---|---|---|
| common | 12,735 | 4,453.01 | default bucket, not only common stock (trap 5) |
| convertible_note | 289 | 33.75 | `shares` is par dollars on nearly all (trap 3) |
| option_put | 257 | 77.67 | the same rows as `put_call = 'put'` |
| option_call | 193 | 14.40 | the same rows as `put_call = 'call'` |
| unit | 112 | 9.82 | MLP, trust and LP units. Equity, not SPAC units |
| warrant | 64 | 0.28 | |
| convertible_preferred | 47 | 1.51 | |

Excluding everything but `common` and `unit` leaves equity. `instrument_class` NULL: 0 rows. **NULL means "not classified yet".** Ingest does not write this column; a separate manual pass does (`classify_holdings --apply`). New filings arrive NULL, and `instrument_class = 'common'` then drops them without a word. Count NULLs once Q3 lands.

### Traps

1. **Summing `value`.** `SUM(value)` is $4,521.46bn, `SUM(value_usd)` is $4,590.44bn. The $68.98bn gap is Baupost and Duquesne read 1000x too small.
2. **One row is 1000x too big.** First Eagle, 2026-03-31, Strategy 0% 2029 note (cusip `594972AS0`): `shares` 20,000,000,000 PRN, `value_usd` $16.59bn. Next quarter the same line is 20,000,000 par and $17.2m. It inflates First Eagle's book that quarter ($75.57bn against about $59bn). An equity-only filter removes it. The fault is the filer's, and it was corrected: the original `0001325447-26-000009` states 16,590,000,000, and a restatement filed two days later (13F-HR/A `0001325447-26-000018`, 2026-05-14) states 16,590,000. The restatement is recorded in `thirteenf_arrivals` and is **not applied** to this table (trap 9).
3. **`shares` is not always shares.** 46 rows are tagged PRN (Elliott 35, Baupost 6, Horizon Kinetics 3, First Eagle 2); `shares` there is dollars of par. But `shares_type` is NULL on 6,063 rows: every long row of 19 filers (the 153 filings ingested before 2026-08-18 UTC). Only the 9 filers added that day carry it (Berkshire, TCI, Himalaya, Baupost, Akre, Kopernik, Horizon Kinetics, First Eagle, Elliott). The 238 debt-CUSIP rows of the other 19 are unmarked. Filter on `instrument_class <> 'convertible_note'` instead. The 281 debt-CUSIP note rows hold 36.63bn "shares", **40% of `SUM(shares)` over long rows** (90.71bn); 20.00bn of that is the single trap-2 row. Nearly all are par dollars (`value_usd / shares` has median 0.98). Misfits: 3 Situational Awareness rows at 2025-09-30 (Lumentum, Cipher, Western Digital) sit under a note CUSIP but hold share counts valued at the stock's close; 3 Horizon Kinetics Cheniere rows are tagged PRN but are real shares; 8 Horizon Kinetics BSV rows (a bond ETF, $4.3m) are classed convertible_note.
4. **Option rows** (450 rows, 13 filers).
   - `shares` is the UNDERLYING share count (all 450 are multiples of 100, none zero). There is no strike and no expiry anywhere.
   - `value_usd` is underlying shares x quarter-end price, not premium: 303 of the 365 rows I could price are within 10% of that. Two filers differ: Kopernik (47 rows, $4.9m) reports about 5% of notional; Horizon Kinetics (21 rows, $10.5m) reports round per-share figures. 12 rows of other filers also miss.
   - An option row carries the underlying's cusip, ticker and `issuer_id`, so it sums into the stock unless you filter `put_call`. 138 filer-period-cusips are held both long and through options.
   - Elliott's puts alone are $52.65bn, 57% of all option value. The house "book" (`q_tracked_books`, `q_portfolio`) adds long + call + put.
5. **`common` is a catch-all.** By OpenFIGI type it holds 454 ETF rows ($25.68bn), 689 ADR rows, 420 closed-end fund rows, 250 REIT rows, 53 MLP rows, 46 tracking-stock rows. `unit` is $8.45bn of KKR (Akre, filed as "COM UNITS") plus royalty trusts, MLPs and 11 SPY rows. A row is `unit` only when `title_of_class` says UNIT, and that column is NULL for the 19 filers of trap 3. So 3 CUSIPs (SPY, WES, PHYS; 33 rows) are `common` on some rows and `unit` on others, and `unit` is not all the MLPs.
6. **Ticker.** Key positions on `cusip`; use `ticker` for display and price joins only.
   - NULL on **1,136 rows (8.3%), $216.35bn (4.7% of value), 213 CUSIPs.** All 161 foreign-numbered CUSIPs (first character a letter) are unmapped: 968 rows, $204.83bn. Chubb alone is $73.65bn; also Eaton, Willis Towers Watson, Nu, Aon, Ferrovial, ASML, Medtronic, Spotify. Largest other miss: KKR, filed with a lower-case cusip `48251w104` (8 rows, $8.45bn).
   - A non-NULL ticker is not always a US symbol. 817 rows (237 CUSIPs, $67.19bn) carry a symbol from a non-US or bond venue: 325 bond descriptors such as `PCG 4.25 12/01/27` (normal for convertibles), 57 ending in `*`, 435 others. Exxon Mobil is `EXMOC` on all 15 of its rows ($7.27bn). Trust test: `cusip_ticker.exch_code = 'US'` (11,708 rows, $4,306.47bn). It is conservative: `EA` (Electronic Arts, exch `MM`) fails it and matches the `prices` close exactly.
   - Price join: of 12,735 common rows, 10,924 ($4,206.77bn) have a ticker with an eod row in `prices`. 1,811 rows ($246.24bn) do not: 1,101 with no ticker, 710 with a ticker `prices` does not know. **A ticker in `prices` is not a price at the quarter-end:** only 8,906 rows ($4,076.69bn) have a close on or within 7 days before `period`. The other 2,018 ($130.07bn) have none, 2,015 of them because the ticker's price history starts after the period (1,984 sit at periods 2024-06-30 to 2025-06-30). Share classes use a slash here (`BRK/B`, `BRK/A`, `HEI/A`, `LEN/B`) and a dash in `prices` (`BRK-B`, the only one of the four present).
   - Ticker `B` is two CUSIPs: Barrick before and after its 2025 rename.
7. **`issuer_id` (CUSIP 1-6).** It joins an issuer's share classes, convertibles, warrants and options. It is not a company id, and these tables hold no issuer CIK.
   - **Share-class twins: union them.** Live at 2026-06-30, Alphabet `02079K`: GOOGL 12 filers $33.51bn + GOOG 8 filers $18.03bn = **14 distinct filers, $51.54bn**. `ticker = 'GOOGL'` alone misses $18.03bn and 2 filers. 6 of the 14 hold both classes that quarter (over all periods: 8 filers, 50 filer-periods). Berkshire `084670`: BRK/A 2 filers $0.42bn, BRK/B 5 filers $0.68bn. Fox `35137L`: FOXA $115.0m, FOX $29.6m. Sum `value_usd` across twins, never `shares` (1 BRK/A = 1,500 BRK/B).
   - It over-merges fund trusts: `464287` (iShares Trust) is IVV, IWM, TLT, EEM, LQD and 3 more. 20 of the 52 issuer_ids with more than one common-class CUSIP are ETF or closed-end trusts.
   - It over-merges tracking stocks: `531229` (Liberty Media) is Formula One and Liberty Live.
   - It does not survive a CUSIP change: Barrick is `067901` through 2025-03-31 and `06849F` from 2025-06-30.
   - It is case-sensitive: KKR is `48251W` (24 rows, 4 filers, $6.35bn) and `48251w` (Akre's 8 rows, $8.45bn). Compare on `upper(issuer_id)`.
8. **CIK format.** Bare in all six 13F tables, zero-padded to 10 in `registry.json`. `cik = '0001536411'` returns 0 rows; the cast returns 630. Join with `queries.cik_int` or `CAST(cik AS INTEGER)`.
9. **Coverage.** Only form `13F-HR` is ingested into this table. Amendments (`13F-HR/A`) and notices (`13F-NT`) are recorded in `thirteenf_arrivals` with what their covers state, and **no amendment is applied to a book here**. On 2026-10-05 EDGAR held 10 amendments for periods this table carries: 6 state RESTATEMENT (Scion 2023-12-31, Founders Fund VII 2024-09-30, Elliott 2024-12-31, Coatue 2025-12-31, Himalaya 2025-12-31, First Eagle 2026-03-31) and 4 state NEW HOLDINGS (Pershing 2024-12-31, Berkshire 2025-03-31, Elliott 2025-06-30, First Eagle 2026-06-30). The stated type does not say what the table carries: the first three NEW HOLDINGS ones add 1 to 4 lines to the original, and First Eagle's repeats its whole book plus 2 lines. 13F shows US-listed longs and options only: no shorts, no cash.
10. **Corporate filers.** Alphabet, Amazon and NVIDIA are $166.95bn of the $775.44bn at 2026-06-30 (21.5%). Alphabet's SpaceX line (`SPCX`) alone is $94.18bn. The `queries.py` comment saying 52.8% is stale: that is the share with the nine filers added 2026-08-18 left out.
11. **House functions read raw `value`.** `q_portfolio` and `q_tracked_books` re-derive one scale per FILER. On the mirror that equals `SUM(value_usd)` for every filer's latest period (28 of 28; `q_portfolio` on all 225 filer-periods). It would be wrong for a filer that switched units.
12. 9 rows have `value = 0` with shares (Opthea, Cue Health, Invitae), as stored.
13. **`shares` is not split-adjusted; `prices` is.** Netflix split 10-for-1 between 2025-09-30 and 2025-12-31: Coatue's row goes from 618,735 to 10,861,355 shares, and `value_usd / shares` is $1,198.92 at 2025-09-30 against a `prices` close of $119.89. A quarter-on-quarter share change across a split is false, and so is `shares` x close at an older period. The house badge compares shares and gets it wrong: `q_portfolio` marks Horizon Kinetics' Netflix `added` at 2025-12-31 (311 to 2,410 shares; split-adjusted, a trim from 3,110). 124 common rows on 20 tickers ($51.15bn) sit at a clean multiple (1.5x to 25x) of the `prices` quarter-end close: BN, CVNA, NFLX, BKNG, CRWD, KLAC and others. Compare `value_usd`, or rescale shares first.

## cusip_ticker

**One row** = one CUSIP seen in a 13F. **2,141 rows**, exactly the CUSIPs in `thirteenf_holdings`. `thirteenf_holdings.ticker` equals `cusip_ticker.ticker` on every row, so join only for the extra columns.

| column | meaning |
|---|---|
| `ticker` | chosen symbol. NULL for 213 CUSIPs |
| `name` | OpenFIGI's name. NULL for 212 |
| `exch_code` | venue of the chosen record. `US` 1,687; other 238; NULL 216 |
| `security_type` | Common Stock 1,478; ETP 110; ADR 90; US DOMESTIC 78 (bonds); Closed-End Fund 67; REIT 53; others 49; NULL 216 |
| `market_sector` | Equity 1,830; Corp 81; Pfd 14; NULL 216 |
| `mapped_via` | `openfigi_checked_no_us` 1,248; `openfigi_us` 764; `openfigi_miss` 76; `openfigi_foreign` 50; `manual` 3 |
| `ticker_raw` | what OpenFIGI's first record said. Audit only |

**Trap:** `openfigi_checked_no_us` does not mean "no US listing". 923 of those 1,248 have `exch_code = 'US'`. Use `exch_code`.

## thirteenf_filings_seen

**One row** = one (cik, accession) the ingest has processed. **230 rows**, 28 filers, seen 2026-07-23 to 2026-08-18. Columns: `cik`, `accession`, `seen_at_unix`. No period, no filed date.

225 match holdings. **5 have no holdings rows:** Thiel Macro `0001315863-20-000698`, `-20-000924`, `-26-000169`, `-26-000423`; Founders Fund Growth II `0002106825-26-000003`. All five are filed-empty reports (cover table value 0, one placeholder row), read from EDGAR on 2026-10-02: Thiel Macro for 2020-06-30, 2020-09-30, 2025-12-31 and 2026-03-31, Founders Fund Growth II for 2026-03-31. This table stores no period, form or date; `thirteenf_arrivals` does. No analytic use.

## thirteenf_baseline

**One row** = one filer's newest 13F-HR as the nightly diff saw it. **28 rows** (25 at 2026-06-30; Pershing 2026-03-31, Founders Fund VII 2025-12-31, Scion 2025-09-30). Columns: `cik`, `accession`, `period`, `filed_date`, `holdings_json`, `ingested_at_unix`.

**Trap:** `value` inside `holdings_json` is raw and unscaled (Duquesne sums to 4,354,220; the real figure is $4.354bn), and there is no ticker. Do not analyse from it.

## thirteenf_filing_meta

Not in the brief; one line because it explains `value_scale`. 225 rows, one per filing. `scale_basis`: `price_anchored` 202, `price_anchored_weak` 22 (under 3 priced lines), `inherited` 1. The cover-page control totals (`entry_total`, `value_total`) are filled for 8 filings only, all Elliott; NULL for the other 217.

## thirteenf_arrivals and thirteenf_feed_reads

The arrival ledger (order SM-H1). Written only by `smart_money.arrivals_ingest`; judged and read by `smart_money.arrivals` and `queries.q_arrivals`. **Both tables exist on production from the SM-H1 deploy and on the mirror from the first sync after it.** Counts below are from a trial read of real EDGAR on 2026-10-08 into a throwaway database, repeated on 2026-10-15 with the same counts.

**`thirteenf_arrivals`: one row** = one 13F-family filing in the feed pages read for a tracked filer, whatever its form. **1,537 rows**, 28 filers: 1,404 `13F-HR`, 125 `13F-HR/A`, 6 `13FCONP` (paper, 1999-2012), 2 `13F-NT`. The pages read are each filer's `filings.recent` block. For 22 filers that is their whole history. For 6 (Alphabet, Amazon, NVIDIA, Berkshire, Baker Bros, Horizon Kinetics) it starts in mid-2023 and 374 older filings sit on pages a `--deep` read would follow; none has been run.

| column | meaning |
|---|---|
| `cik` | filer CIK, bare digits (trap 8) |
| `accession`, `form` | as EDGAR lists them. `form` is verbatim |
| `period` | the feed's reportDate; if the feed states none, the cover's. Never guessed |
| `filed_date` | EDGAR's official filing date. A filing accepted after 17:30 ET carries the NEXT business day |
| `accepted_et` | acceptance time, US Eastern, from the filing's own index page. NULL = not read yet. **Use this for "when the market knew"** |
| `feed_accepted` | the feed's acceptanceDateTime, verbatim. See the trap below |
| `report_type` | cover: `13F HOLDINGS REPORT`, `13F NOTICE`, `13F COMBINATION REPORT` |
| `is_amendment`, `amendment_no`, `amendment_type` | cover. `amendment_type` is `RESTATEMENT` or `NEW HOLDINGS` as stated |
| `entry_total`, `value_total` | cover control totals, raw units (the form states no unit) |
| `confidential_omitted` | cover: holdings omitted under a confidential treatment request |
| `other_managers` | JSON list from the COVER: the managers who report holdings FOR this filer |
| `cover_status` | `read` 295, `pending` 1,236 (period before 2023-12-31, never requested), `paper` 6, `no_xml_cover`, `failed` (ours; retried nightly) |
| `read_error` | why the last cover or index-page read failed (`cover: ...` / `stamp: ...`); a cover's reason is kept over a stamp's, since the states wait on the cover. NULL on all 1,537 |
| `first_seen_unix` | when a read first saw the filing, set only where that read could not have seen it before. **NULL on all 1,537 backfilled rows**: a backfill time is not an arrival time |

**`thirteenf_feed_reads`: one row** = one filer. `read_unix` and `read_day` are the last SUCCESSFUL read. `read_day` is the last US Eastern day whose filings that read can have seen: the read's own Eastern date if it ran at or after 17:30 ET, EDGAR's same-day cut-off, else the day before. The 22:30 nightly answers for its own day; a deploy-time read at 09:00 answers for yesterday, and so cannot stand in for deadline night; `attempted_unix` and `error` the last attempt; `newest_filed` the newest filing date that read saw; `older_pages` and `deep` say whether feed pages beyond `filings.recent` exist and whether a read has followed them (6 filers have older pages, none read).

**The four arrival states** of a (filer, period), derived by `arrivals.period_state` from that period's filings:

- `filed`: any filing for the period whose cover states holdings.
- `nt_notice`: a notice and no book.
- `filed_empty`: a holdings report whose cover states a table value of 0.
- `awaited`: nothing filed, as of a feed read that answers for the day it is judged against.

Three more labels are not states of the filer. `not_read`: no read can answer for the period (the feed was never read, or the period's deadline has passed and no read answers for deadline day, or the period lies behind feed pages that were not read). `filed_unread`, printed `filed (contents not read)`: a 13F is on EDGAR for the period and what it states has not been read; it outranks `nt_notice` and `filed_empty`, because the unread filing may be the book. It is also what a period reads when holdings for it are held and the ledger has no filing for it (the ledger's read is behind the holdings ingest), and what the quarter just ended reads when a filing's stated period is not a quarter end; such a filing blocks 'awaited' for that quarter only. `not_due`: the period is before the filer's first 13F.

Among the 295 covers read: 274 holdings reports, 20 combination reports, 1 notice; 12 amendments (6 RESTATEMENT, 6 NEW HOLDINGS); 3 with `value_total = 0`.

**What `q_arrivals` adds per filer.** `closed_*` is the newest quarter whose deadline has passed and `open_*` the quarter whose window is open. `book_period` is the newest period held in `thirteenf_holdings` as of the anchor, and `book_check` compares that book's raw value sum with its own cover's `value_total`: on the trial read 28 of 28 newest books match their cover (the two agree to within 4 on every held filing). The allowance is half a unit per line held, never under 5: a filer rounds each line and the total separately. A wider gap prints `differs from cover` and is a partial parse or a wrong filing; so does a period held from two filings, because every card sums the period. `amendments_not_applied` counts amendments on EDGAR for periods from the earliest one ever held (10). `filed_not_held` lists periods with a 13F-HR on EDGAR and no holdings held (none today), an ingest that has not run or has failed, and periods whose only book is an amendment (a placeholder 13F-HR restated into a book: the holdings ingest reads 13F-HR only and will never hold it). `needs_look` is true for a closed state other than `filed` or `not_due`, an open-quarter filing that is not a book, a filing not held, a book that differs from its cover, or a last read attempt that failed. `missed_periods` lists quarters since the later of the filer's first 13F and 2023-12-31 whose deadline passed with nothing filed: Scion 3, Founders Fund VII 2, Thiel Macro 4 (2023-12-31 to 2024-09-30, the end of its 16 silent quarters).

**Traps**

- **`feed_accepted` is labelled Z and is not reliably UTC.** Measured against each filing's index page: Horizon Kinetics' stamps are 4 to 5 hours late, and Scion's and Founders Fund VII's are raw Eastern time. It orders two filings of ONE filer and nothing else. `accepted_et` is the stamp to use.
- **`filed_date` is not the acceptance date** for 19 of the 295 filings read. Situational Awareness's Q1 2026 13F is dated Monday 2026-05-18 and was accepted Friday 2026-05-15 18:33:35 ET. Such a filing is not in the public feed until its `filed_date`, which is why the ledger counts filings by that date.
- **An empty book is `value_total = 0`, never `entry_total`.** The entry total is 0 on two real books (Third Point 2019-09-30, Scion 2023-12-31) and 1 on fifteen real one-position books.
- **A `13F-NT` is not a notice of no holdings.** It says the holdings are in another manager's report; `other_managers` names it. That manager's CIK is optional on the form (86 of 124 cover entries on this roster give only a file number), though every notice read so far gives one.
- **`13F COMBINATION REPORT` means the book is partial.** Kopernik in every held quarter and First Eagle from 2024-12-31: part of the book is reported by the manager in `other_managers`.
- **Nothing here is applied to `thirteenf_holdings`.** "Filed" in the ledger does not mean the holdings were ingested, and an amendment in the ledger is not in any book.
- **The older 13F legs are unchanged.** The nightly diff (`thirteenf.latest_13f`) and the holdings ingest (`thirteenf_ingest`) still read form `13F-HR` only and each still fetches the feed itself, so every filer's feed is read three times a night. The diff leg will keep logging "no new filing since 2026-03-31" for Pershing beside a ledger that holds its notice. The boards and the QoQ badges still compare each filer's newest held period with its previous held one; the ledger is shown beside them.
- **The deadline is rolled.** 45 days after quarter end, moved to the next day that is neither a weekend nor a federal holiday: 2026-11-16 for Q3 2026, 2027-02-16 for Q4 2026.

## Registry

`/home/wafflehouse/.openclaw/smart_money/analysis/registry.json`, shape `{as_of, entries[]}`. **43 entries; 28 have `role = 'manager_13f'`.** The other 15 are 12 members of Congress and 3 Form 4 persons. The 28 registry CIKs, the 28 CIKs in holdings and the ingest list (`thirteenf_ingest.CONFIRMED`) are the same set.

Fields on a 13F entry: `name`, `cik` (10-digit zero-padded string), `role`, `thesis`, `position_floor_pct`, `status` (all `active`), `as_of` (all 2026-08-18; the top-level `as_of` still says 2026-07-31). Also present and carrying nothing: `type` (same as `role`), `person_id`, `chamber`, `scores` (all null).

- **`thesis`** is a grouping label, not a performance claim:
  - ai_tmt (7): Coatue, Whale Rock, Light Street, Lone Pine, Situational Awareness, Founders Fund VII, Founders Fund Growth II
  - value (5): Berkshire, Appaloosa, Himalaya, Akre, First Eagle
  - macro (4): Duquesne, Thiel Macro, Affinity, Soros
  - activist (4): Pershing Square, Third Point, TCI, Elliott
  - corporate_strategic (3): Alphabet, Amazon, NVIDIA
  - contrarian (2): Scion, Baupost
  - hard_assets (2): Kopernik, Horizon Kinetics
  - biotech (1): Baker Bros
- **Discretionary** = `queries.is_discretionary(thesis)` = thesis is not `corporate_strategic`. 25 filers are discretionary. The other 3 are operating companies marking strategic stakes, not expressing a view. A filer with no thesis would count as discretionary.
- **`position_floor_pct`** is 0.25 for Horizon Kinetics and First Eagle (about 350 and 420 lines per filing), null for the other 26. House signal counts skip lines under that % of book. All rows are still stored.

## Example queries (all run on the mirror)

**A. Equity book per filer for one quarter, in dollars.**

```sql
SELECT cik, MIN(filed_date) AS market_knew, COUNT(*) AS lines, SUM(value_usd) AS usd
FROM thirteenf_holdings
WHERE period = '2026-06-30'
  AND put_call = 'long'                        -- no option notional
  AND instrument_class IN ('common', 'unit')   -- no convertibles or warrants
GROUP BY cik ORDER BY usd DESC;
```

Returns 25 filers, $763.12bn in total (all rows for the quarter: $775.44bn). Top: Berkshire $299.25bn, Alphabet $99.08bn, NVIDIA $63.44bn.

**B. Who held Alphabet as the market knew it on a given day.** Filing clock, both share classes.

```sql
WITH known AS (            -- each filer's newest filing the market had on :asof
  SELECT cik, MAX(period) AS period FROM thirteenf_holdings
  WHERE filed_date <= :asof GROUP BY cik)
SELECT h.cik, h.period, h.filed_date, GROUP_CONCAT(h.ticker) AS classes,
       SUM(h.value_usd) AS usd
FROM thirteenf_holdings h JOIN known k ON k.cik = h.cik AND k.period = h.period
WHERE h.issuer_id = '02079K'                   -- Alphabet: GOOG + GOOGL
  AND h.instrument_class = 'common'
  AND h.period >= date(:asof, '-200 days')     -- drop filers whose last filing is stale
GROUP BY h.cik ORDER BY usd DESC;
```

With `:asof = '2026-08-10'`: 14 filers, $27.63bn, and 13 of the 14 are still on their 2026-03-31 filing. With `:asof = '2026-08-14'`: 15 filers, $51.64bn. Four days apart; that is the filing clock.

**C. Most widely held issuers among discretionary filers.** Registry joined through `cik_int`; fund trusts and tracking stocks kept out of the `issuer_id` rollup.

```python
import json, sys
sys.path.insert(0, "/mnt/c/Users/mdiba/Code/Abelard/daemons/smart_money_daemon")  # a checkout at or after c3b7dbb
from smart_money import queries as q

con = q.connect_ro("/home/wafflehouse/.openclaw/smart_money/smart_money_v0.db")
reg = json.load(open("/home/wafflehouse/.openclaw/smart_money/analysis/registry.json"))
who = {q.cik_int(e["cik"]): e for e in reg["entries"] if e["role"] == "manager_13f"}
disc = [c for c, e in who.items() if q.is_discretionary(e.get("thesis"))]
sql = """
SELECT h.issuer_id, MAX(h.issuer) AS issuer, COUNT(DISTINCT h.cik) AS filers,
       SUM(h.value_usd) AS usd
FROM thirteenf_holdings h JOIN cusip_ticker c ON c.cusip = h.cusip
WHERE h.period = '2026-06-30' AND h.put_call = 'long' AND h.instrument_class = 'common'
  AND COALESCE(c.security_type, '') NOT IN ('ETP', 'Closed-End Fund', 'Tracking Stk')
  AND CAST(h.cik AS INTEGER) IN ({})
GROUP BY h.issuer_id ORDER BY filers DESC, usd DESC LIMIT 5
""".format(",".join("?" * len(disc)))
for r in con.execute(sql, disc):
    print(tuple(r))
```

Returns Alphabet 14 filers $51.54bn, Taiwan Semiconductor 10 filers $9.29bn, Amazon 10 filers $6.45bn, Meta 6 filers $3.89bn, Broadcom 6 filers $2.41bn. That is out of the 22 discretionary filers with a 2026-06-30 filing.
