# Form 4: `form4_transactions`, `form4_derivatives`

All counts measured on the mirror (snapshot 2026-10-01). Each figure was re-measured by an independent fact-check.

## form4_transactions

**One row** = one non-derivative transaction line (Table I) of one Form 4 filing, keyed `(accession, tx_index)`. **351,519 rows**, 169,671 filings, 5,689 issuers, 46,905 reporting persons.

**Dates present.** `filed_date` 2021-08-24 to 2026-09-30. `tx_date` (unflagged) 2002-02-24 to 2026-09-29; 295 rows dated before 2023.

| ingest_regime | rows | filings | filed_date |
|---|---|---|---|
| universal (every Form 4) | 328,470 | 165,898 | 2025-07-23 to 2026-09-29 |
| watchlist (29 issuers) | 23,049 | 3,773 | 2021-08-24 to 2026-09-30 |

The tag says which job wrote the filing first, not which population it is in: 8,913 watchlist-tagged rows were filed on or after 2025-07-23. For the whole market use `filed_date BETWEEN '2025-07-23' AND '2026-09-29'` and ignore the tag. 2026-09-30 holds 5 rows from one watchlist filing; the universal job had not reached that day. Inside the window 4 market sessions have no rows (2025-10-13, 2025-11-11, 2025-12-24, 2025-12-26; probably days EDGAR was closed, not verified) and `form4_universal_days` records 65 filings that failed to parse. Before 2025-07-23 the corpus is 26 issuers (14,136 rows); only 29 rows were filed before 2023-07.

| column | meaning | unit / format |
|---|---|---|
| reporting_person, reporting_cik | the first reporting owner on the filing | CIK 10 digits, zero-padded, every row |
| issuer, issuer_cik | the company | CIK bare, no leading zeros, every row |
| ticker | symbol as the filer typed it | free text (trap 6) |
| code | SEC transaction code | letter |
| plan_flag | 1 = filing ticks the 10b5-1 box; one value per filing | 0/1 (trap 1) |
| shares | size of the line; never negative. Direction follows from `code` for P, S, A, F; for J, G, C and the rest it is not stored (trap 11) | shares; zero 208 |
| price | per-share price as filed, nominal on the day | USD/share; NULL 25,887, zero 80,193 |
| value | `round(shares*price, 2)` | USD; NULL 26,440 |
| ownership_after | holding after this line, for that security and ownership form; not the person's total | shares; NULL 57 |
| tx_date, filed_date | trade clock, filing clock | YYYY-MM-DD, every row |
| role | `director`, `officer:<title>`, `10pct`, comma-joined | NULL 117,782 (trap 1) |
| security_title | the filer's words for the security | NULL 315,643 (trap 8) |

**Codes.** Open market: `P` buy 24,308, `S` sale 126,065. No cash at a market price (house `NO_CASH_CODES`): `A` grant 67,368, `F` tax withholding 56,424, `M` option exercise 49,227, `D` 7,663, `J` 7,246, `G` gift 7,117, `C` conversion 4,529, `W` 92, `Z` 17, `E` 2. Other: `L` 577, `X` 382, `U` 360, `I` 126, `O` 6, blank 10. `value` is above zero on 94,278 no-cash rows (F at the vesting-day price, M at the strike); it is not money traded.

### Flags and what excluding them means

**value_flag** (553 rows: P 186, S 75, M 238, other 54): `price_vs_close` 495, `price_over_max` 48, `value_denominated` 9, `value_over_max` 1. On these rows `value` is NULL; `shares` and `price` are kept.

- Summing dollars: use `SUM(value)`. It skips the 553 flagged rows and the 25,887 with no price. Never rebuild dollars as `shares*price`: for P that turns $63.6bn into $7.05 quadrillion.
- Counting trades, filings or shares: do not filter on `value` or `value_flag`. You would drop 553 real disclosures, or 26,440 with `value IS NOT NULL`.
- NULL does not mean checked. 39 unflagged rows (codes D, A, J, F; filed 2025-09-17 to 2026-07-24) exceed the guard's own ceilings. `SUM(value)` over all codes is $1.137 quadrillion, $1.105 quadrillion of it one DNP `J` row. Sum dollars for P and S only.

**date_flag** (14 rows: `after_filing` 13, `before_floor` 1). Off the trade clock only.

- Anchored on `tx_date` (returns, lags, windows): add `date_flag IS NULL`.
- Anchored on `filed_date`: keep them.
- `date_subclass`: `non_trading_day` 276, of which 271 have `date_flag` NULL (P 124, S 147). That is a mark; keep the rows. `future_dated_other` 8, `century_slip` 1.
- `tx_date_suggested`: 0 rows in either Form 4 table.
- Lag: 96.1% of rows were filed within 5 days of the trade; 13,799 later; 1,037 over a year later.

### Traps

1. **`plan_flag = 0` and `role` NULL are often parse misses.** The parser accepts the 10b5-1 box and the role boxes only when written `1`; filings that write `true` are stored as plan_flag 0, role NULL. Checked on EDGAR: `0001193125-26-114850` (Pichai, GOOGL) says `aff10b5One = true` with a 10b5-1 footnote; the mirror has 0 and NULL. Role is NULL on 117,782 rows (33.5%). Filer-agent prefix `0001193125` alone: 20,532 filings, 42,286 rows, all role NULL and plan_flag 0. Among S rows plan_flag is 1 on 61.7% where role is filled, 20.0% where it is NULL. `plan_flag = 1` is reliable. Treat `plan_flag = 0 AND role IS NULL` (106,366 rows) as unknown. How many are really plan trades: not established. The house default `plan="discretionary"` is `plan_flag = 0` and so includes them.
2. **Close vs price.** `prices.close` is restated for later splits; `form4.price` is what was paid that day. YYAI: bought at $0.3205 on 2025-10-08, close that day 7,720 (24,087x). NFLX: 163 sales at $1,079 to $1,215 against a close of $109 to $121 on the same dates. Of 21,531 P rows with a price and a same-day close, 1,151 are more than 2x away and 373 more than 10x; S: 8,472 and 2,393 of 122,218. Take a return's entry price from `prices` on `tx_date`, never from `form4.price`, and do not multiply `shares` by `close`. The `price_vs_close` flag is partly this artefact: of its 208 P and S rows, 195 have price below today's stored close/10 (the direction a later reverse split produces), nine of them YYAI purchases. Its 238 M rows are all exercise strikes below close/10; a strike is not a market price, so the flag on M is not evidence of an error.
3. **Same trade under more than one accession.** Later filings re-report earlier trades; nothing marks them. (By the code, the universal job reads form type `4` only, so `4/A` amendments exist only for watchlist issuers.) `queries._dedup_amendments` keys on (reporting_cik, issuer_cik, tx_date, code, shares), keeps the latest filing, and run over the whole table removes 13,323 rows (P 1,149, S 7,596). Its key has no price, so it also merges separate lots: 531 of those P rows ($39.2m) and 4,286 S rows ($932m) come from groups inside one filing at more than one price. The docstring calls these an artifact; the data disagrees. With price in the key and collapsing only across filings: 61 P rows ($144m), 637 S rows ($5.57bn). Counting filings needs no dedup. Counting trades or summing dollars does: the house function to match the dashboard, the stricter rule otherwise.
4. **Co-filings.** Affiliates report one block in separate filings (same issuer, date, shares, price, `ownership_after`; different reporting_cik): P 545 extra rows, $1.96bn counted twice; S 1,812 rows, $28.0bn. House fix: `queries.clean_subset`, which by default also drops fund issuers and every row of a filer with a suspect value. Example 2 covers traps 3 and 4.
5. **One filer dominates P dollars.** SVRE: 18 rows, $27.88bn of the $63.57bn P total (43.9%), none flagged.
6. **Ticker is free text.** Placeholders, not symbols: `NONE` 1,747, `N/A` 481, `NA` 199, `none` 31, `-` 28, `None` 5, `N.A.` 2; empty 0, NULL 0. Total 2,493 rows, 217 issuers, 1,063 of them P rows. `ticker IS NOT NULL` removes none; use `queries.norm_ticker` (it returns None for all 2,493 but passes 7 rows written with brackets, such as `[NONE]`). Another 2,276 rows are not a plain symbol (`Z AND ZG`, `NYSE: KRC`, `(SIRI)`), and 212 issuers appear under more than one string after upper-casing (232 on the raw string). Group by `issuer_cik`.
7. **CIK format.** `issuer_cik = market_cap.cik` raw matches nothing; cast both to INTEGER (`queries.cik_int` in Python) and 776 Form 4 issuers match 779 `market_cap` rows (a CIK can carry more than one ticker there).
8. **`security_title` is forward-only.** NULL on every row filed up to 2026-08-07 except 1,117; filled on all 34,759 rows filed from 2026-08-10.
9. **P rows without a price.** Of 24,308, price is NULL on 214 and zero on 94: 308 rows (1.3%).
10. **P is not always an open-market buy.** 3,054 P rows (12.6%) are fund issuers by `queries._issuer_class`.
11. **Not stored:** form type (4 vs 4/A), acquired/disposed, direct/indirect, and any reporting owner after the first.

## form4_derivatives

**One row** = one derivative transaction line (Table II), keyed `(accession, tx_index)`, numbered separately from Table I. **17,997 rows**, 9,982 filings (3,337 with no Table I row), 1,702 issuers.

**Dates present.** `filed_date` 2021-08-10 to 2026-09-29. Universal: 12,837 rows filed **2026-07-28 to 2026-09-29 only**. Watchlist: 5,160 rows, 20 issuers. Before 2026-07-28 this table is 20 issuers.

| column | meaning | unit / NULLs |
|---|---|---|
| security_title | the derivative, e.g. "Restricted Stock Units" | never NULL |
| shares | number of derivative securities | NULL 76 |
| price | price of the derivative in this transaction | USD; zero 13,574, NULL 2,352 |
| exercise_price | strike or conversion price | USD/share; NULL 10,153, zero 1,779 |
| tx_date | when the transaction happened | every row |
| exercise_date | date first exercisable, not the date exercised | NULL 15,321 (85%) |
| expiration_date | expiry | NULL 11,355 (63%); 2016-07-28 to 2050-12-16 |
| underlying_title, underlying_shares | what it converts into, how many | shares NULL 190 |

`code`, `plan_flag`, `role`, `ticker`, CIKs and the date flags behave as in Table I, with the same traps (role NULL 5,190; placeholder tickers 26). No `value`, `value_flag` or `ownership_after`.

**The three dates.** Only `tx_date` places the row in time and only it is judged (`date_flag` set on 4 rows). The other two are forward-looking and never flagged. Where filled, `exercise_date` is before `tx_date` on 1,171 rows, the same day on 597, after on 908. `expiration_date` is before `tx_date` on 19 rows (16 with `date_flag` NULL).

**Traps.** An exercise sits in both tables: 7,741 of the 8,259 derivative `M` rows have a Table I `M` row in the same filing; do not add them. By title (substring match): "option" 5,508; "restricted stock unit" or "RSU" 6,923; exactly `Class B Common Stock` (convertible) 1,504. `P` (258) and `S` (536) here are mostly option, warrant, swap, preferred and depositary-share trades.

## Example queries (all run on the mirror)

**1. Purchases per month, filing clock.** A count: no value, date_flag or regime filter.

```sql
SELECT substr(filed_date,1,7) AS month,
       COUNT(*)                   AS p_rows,
       COUNT(DISTINCT accession)  AS filings,
       COUNT(DISTINCT issuer_cik) AS issuers
FROM form4_transactions
WHERE code = 'P'
  AND filed_date >= '2025-08-01'
GROUP BY 1 ORDER BY 1;
```
14 months. 2025-08: 2,194 rows, 1,331 filings, 598 issuers. 2026-09: 1,696 / 1,047 / 476. `p_rows` still contains re-reports and co-filings (traps 3, 4).

**2. Dollars bought per issuer, trade clock, each block once.** Keeps the latest filing for each block, so re-reports and co-filings drop and separate lots inside a filing stay. My rule, not the house's.

```sql
WITH base AS (
  SELECT f.*,
         MAX(filed_date || accession) OVER (
           PARTITION BY issuer_cik, tx_date, code, shares, price,
                        COALESCE(ownership_after, -rowid)
         ) AS keep_filing
  FROM form4_transactions f
  WHERE code = 'P'
    AND date_flag IS NULL
    AND tx_date BETWEEN '2026-07-01' AND '2026-09-30'
    AND value IS NOT NULL
)
SELECT issuer_cik, MAX(issuer) AS issuer,
       COUNT(*) AS lots, COUNT(DISTINCT reporting_cik) AS buyers,
       ROUND(SUM(value)) AS usd
FROM base
WHERE filed_date || accession = keep_filing
  AND value < 1e9
GROUP BY issuer_cik
ORDER BY usd DESC
LIMIT 10;
```
Top row: Republic Services, 86 lots, 1 buyer, $1,380,322,194. Over all P rows the rule keeps 23,707 of 24,308 ($61.47bn of $63.57bn); over all S rows 123,705 of 126,065 ($236.5bn of $270.1bn). `value < 1e9` is the house review bar. Over all dates it leaves 11 SVRE rows ($4.19bn, trades 2026-03 to 2026-06; none fall in this example's window), so in a window that reaches them exclude that filer by name.

**3. Check a ticker's Form 4 price against the stored close.**

```sql
SELECT f.tx_date, f.code, f.price AS form4_price, p.close AS close_that_day,
       ROUND(p.close / f.price, 1) AS close_over_price, f.value_flag
FROM form4_transactions f
LEFT JOIN prices p
       ON p.ticker = UPPER(TRIM(f.ticker)) AND p.date = f.tx_date AND p.price_type = 'eod'
WHERE UPPER(TRIM(f.ticker)) = 'YYAI' AND f.code IN ('P','S') AND f.price > 0
ORDER BY f.tx_date;
```
14 rows; `close_over_price` runs from 20.0 to 40,425.5. A ratio near 1 means the same share basis.
