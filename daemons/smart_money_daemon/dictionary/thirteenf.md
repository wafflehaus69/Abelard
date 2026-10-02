# 13F: manager holdings

Tables: `thirteenf_holdings`, `cusip_ticker`, `thirteenf_filings_seen`, `thirteenf_baseline`, plus the filer registry (`registry.json`). Every number below is measured on the mirror (snapshot 2026-10-01). These tables were last written 2026-08-18/19. Q3 2026 filings are due around 2026-11-16.

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
- 2026-06-30: Pershing Square (last period 2026-03-31), Scion (last 2025-09-30), Founders Fund VII (last 2025-12-31). Not an ingest miss: the scan of 2026-10-01 02:30 UTC (`scans/scan_1790821801.json` in the state home) found no newer 13F-HR on EDGAR for any of the three. Why they have none: not established.
- Gaps: Thiel Macro has nothing for 2025-12-31 and 2026-03-31; Founders Fund Growth II nothing for 2026-03-31. For 2026-03-31 both filed a 13F-HR (the scan of 2026-07-24 saw it as their newest) and the ingest stored 0 rows from it. Thiel Macro's 2025-12-31 looks the same (see `thirteenf_filings_seen`). Whether those filings were truly empty: not established.

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
2. **One row is 1000x too big.** First Eagle, 2026-03-31, Strategy 0% 2029 note (cusip `594972AS0`): `shares` 20,000,000,000 PRN, `value_usd` $16.59bn. Next quarter the same line is 20,000,000 par and $17.2m. It inflates First Eagle's book that quarter ($75.57bn against about $59bn). An equity-only filter removes it. Whether the filer or the parse is at fault: not established.
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
8. **CIK format.** Bare in all four 13F tables, zero-padded to 10 in `registry.json`. `cik = '0001536411'` returns 0 rows; the cast returns 630. Join with `queries.cik_int` or `CAST(cik AS INTEGER)`.
9. **Coverage.** Only form `13F-HR` is ingested; amendments (`13F-HR/A`) are never read. 13F shows US-listed longs and options only: no shorts, no cash.
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

225 match holdings. **5 have no holdings rows:** Thiel Macro `0001315863-20-000698`, `-20-000924`, `-26-000169`, `-26-000423`; Founders Fund Growth II `0002106825-26-000003`. The three `-26-` accessions fall where the three gaps listed above are; the two `-20-` ones are 2020 filings. This table stores no period, so the match is by accession order only. No analytic use.

## thirteenf_baseline

**One row** = one filer's newest 13F-HR as the nightly diff saw it. **28 rows** (25 at 2026-06-30; Pershing 2026-03-31, Founders Fund VII 2025-12-31, Scion 2025-09-30). Columns: `cik`, `accession`, `period`, `filed_date`, `holdings_json`, `ingested_at_unix`.

**Trap:** `value` inside `holdings_json` is raw and unscaled (Duquesne sums to 4,354,220; the real figure is $4.354bn), and there is no ticker. Do not analyse from it.

## thirteenf_filing_meta

Not in the brief; one line because it explains `value_scale`. 225 rows, one per filing. `scale_basis`: `price_anchored` 202, `price_anchored_weak` 22 (under 3 priced lines), `inherited` 1. The cover-page control totals (`entry_total`, `value_total`) are filled for 8 filings only, all Elliott; NULL for the other 217.

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
