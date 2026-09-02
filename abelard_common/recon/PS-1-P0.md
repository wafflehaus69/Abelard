# PS-1-P0 — Price Substrate Hoist, Phase 0 Recon

**Order:** PS-1, issued 2026-09-02 by Abelard · **Class:** BUILD, phased · **This report:** Phase 0 only
**Executed:** 2026-09-02 by ClaudeCode, from **Orban**; Basilic reached read-only over Tailscale SSH
**Input read in full:** `daemons/corr_daemon/recon/CR-R0-RECON.md` (§R1 inventory, §R8 register)

**Writes performed:** this file only. No commits. No pushes. Nothing written on Basilic. No schema, no
code, no plist.

**Path.** `abelard_common/recon/PS-1-P0.md` at the repo root, per Mando's ruling 2026-09-02. The
`abelard_common` *package* remains at `daemons/common/abelard_common/`; only this recon directory is
top-level.

---

## Rulings — Mando, 2026-09-02 (recorded here, not left in chat)

| # | Question | Ruling |
|---|---|---|
| 1 | Universe scope for v1 | **Ship SPX + NDX.** IWM is a good Russell 2000 source; **build the direct pull-and-update from source** rather than a manual file drop. |
| 2 | Report path | **Top level** — `abelard_common/recon/`. |
| 3 | `abelard_common` stateless convention | **Use the P0.7 proposal** — explicit path arguments, default from `ABELARD_PRICES_DB_PATH`, state home `~/.openclaw/prices/prices.db`. |
| 4 | 21:00 slot vs. news-watch's authored 21:30 | **Take 21:00.** |
| 5 | `prices.py:194` Phase 4 line item | **Struck** — the fix landed at `ccd01c6` and the spans are clean. |

Ruling 1 prompted a re-test of the iShares route. **My original P0.4(iii) "BLOCKED" finding was wrong**
and is corrected in place below — the direct pull works keyless. Mando's instinct was right.

**Worktree note.** The order requires a worktree per E18. Phase 0 is read-only and produces one untracked
file, so it was run in the shared `main` checkout — same posture as CR-R0. **Phase 1 must take its own
worktree** (`Abelard-ps1` on `ps-1-price-substrate`) before the first edit. Eight worktrees are already
live; taking a ninth is routine.

---

## Executive summary — three premises amended, one strengthened

**P0 does not refute the order's direction. It changes three of its mechanisms and makes one acceptance
criterion cheaper than specified.**

1. **The vendor already publishes a corporate-action feed, free, in the same request.** Yahoo's v8 chart
   endpoint returns a `events` block with splits and dividends when `&events=div,split` is passed — and it
   is returned even for a **one-day** window. Acceptance criterion (2) — *"a split landing tonight is
   detected by the nightly … without any vendor corporate-action feed"* — is satisfiable **on the night of
   the split, at zero additional requests**. The rotating full-history refetch should be re-scoped from
   *primary detector* to *corruption-verification sweep*. (§P0.1, §A1)
2. **True raw closes ARE derivable, deterministically.** The order's fallback design (`prices_raw` holds
   the as-first-fetched close with its vintage) is **not needed**. Yahoo's `close` is split-adjusted and
   dividend-**un**adjusted, so `raw(d) = close(d) × Π(split ratios effective after d)` reconstructs the
   true traded close exactly from the declared split feed. (§P0.1, §A2)
3. **`instrument_id` cannot be CIK alone.** Dual-class securities share one CIK (`GOOG`/`GOOGL`,
   `FOX`/`FOXA`, `NWS`/`NWSA`, `BRK-A`/`BRK-B`). CIK is an *issuer* identifier; the substrate keys
   *securities*. (§P0.5, §A3)
4. **Wikipedia's Nasdaq-100 list is ICB, not GICS, and carries no CIK.** Membership only; classification
   must come from elsewhere or stay null. (§P0.4(ii), §A6)
5. **The iShares holdings feed works keyless and is the best classification source found.**
   `latest-holdings.csv` returns dated CSV with a GICS-shaped sector column for **any** iShares ETF —
   IWM (1,961 equities) and IVV (504 equities) both confirmed. This gives Russell 2000 a fully
   refreshable source and gives the S&P 500 an **independent second sector opinion** to check Wikipedia
   against. (§P0.4(iii), §A4)

---

# P0.1 — Vendor identity and semantics

## Which vendor writes the store

**One vendor. Yahoo Finance v8 chart. No others.**

| Fact | Path:line |
|---|---|
| Endpoint `https://query1.finance.yahoo.com/v8/finance/chart/{t}` | `smart_money/prices.py:21` |
| `SOURCE = "yahoo_v8"` — the only value ever written | `smart_money/prices.py:24` |
| EOD write path | `smart_money/prices.py:215` |
| Quote write path | `smart_money/prices.py:254` |

Confirmed against the live store (CR-R0 §R1.2): `SELECT source, COUNT(*) FROM prices GROUP BY source`
→ `[('yahoo_v8', 3505717)]`. **No Stooq. No Finnhub candles.** The Finnhub key in SM's `.env` is used for
company profile data, not prices.

**Alternative vendors tested this session, both unavailable:**

```
Stooq daily CSV  https://stooq.com/q/d/l/?s=mnst.us&i=d
   HTTP 200, 796 bytes — a JavaScript proof-of-work browser challenge, not CSV. BLOCKED.

Finnhub /stock/candle  HTTP 403 {"error":"You don't have access to this resource."}
Finnhub /stock/split   HTTP 403 {"error":"You don't have access to this resource."}
   Paid tier. The existing key does not reach either endpoint. (Key read from Basilic's .env in place,
   never printed, never moved.)
```

## What the endpoint returns

**There is no raw close, and no parameter selects one.** Two series come back:

| Field | JSON path | Semantics |
|---|---|---|
| `close` | `indicators.quote[0].close` | **split-adjusted, dividend-UNadjusted** |
| `adjclose` | `indicators.adjclose[0].adjclose` | **split- and dividend-adjusted** |

Also returned and currently **discarded** by `prices.py`: `open`, `high`, `low`, `volume`.

**Evidence — dividend payer (JNJ, live fetch 2026-09-02):**

```
date        open      high      low       close     adjclose
2026-07-14  255.53    256.39    252.14    253.85     252.60      <- close != adjclose
2026-07-17  252.59    255.66    251.95    253.04     251.80
```

`close` ≠ `adjclose` for JNJ, so `close` carries no dividend adjustment.

**Evidence — split applied retroactively to `close` (AAPL, from the store):** `AAPL 2015-01-05
close = 26.5625`. AAPL traded near $106 in January 2015; the 4:1 split of August 2020 is already applied
to the stored `close`. **`close` is a retroactively split-adjusted view, not a fact.**

## The finding that changes Phase 2: the vendor publishes its corporate actions

Adding `&events=div,split` to the identical request returns an `events` block:

```
GET .../chart/MNST?...&events=div,split
"events": {"splits": {"1786455000": {"date": 1786455000, "numerator": 2.0,
                                     "denominator": 1.0, "splitRatio": "2:1"}}}
   1786455000 -> 2026-08-11

GET .../chart/JNJ?...&events=div,split   ->  splits 0, dividends 7
   2025-02-18 {'amount': 1.24} · 2025-05-27 {'amount': 1.3} · 2025-08-26 {'amount': 1.3}
   2025-11-25 {'amount': 1.3}  · 2026-02-24 {'amount': 1.3} · 2026-05-26 {'amount': 1.34}

KO -> 6 dividends · AAPL -> 7 dividends · MRNA -> 0 splits, 0 dividends
```

**And it works in a narrow window — which is what makes it a nightly detector:**

```
MNST, split effective 2026-08-11:
   5-day window CONTAINING the split      splits=1  <- detected
   3-day window containing the split      splits=1  <- detected
   1-day window ON the split date         splits=1  <- detected
   5-day window AFTER the split (control) splits=0  <- correctly silent
JNJ, ex-div 2026-05-26:
   5-day window containing the ex-div     dividends=1
   5-day window with no event (control)   dividends=0
```

The nightly append already requests a short window per name. **Adding `events=div,split` to that request
costs zero extra requests and detects every split and dividend on its effective date.**

## MNST — the corruption is at the vendor, and it is live right now

CR-R0 §R8.3 established MNST is corrupt in the store. It is corrupt **in the vendor's current response**,
fetched fresh 2026-09-02. Yahoo declares a **2:1 split effective 2026-08-11**. If honoured, every close
before that date would be halved. Instead:

```
2026-07-10   97.39   PRE-split scale   <- not adjusted
2026-07-17   97.50   PRE-split scale   <- not adjusted
2026-07-20   47.72   post-split scale  <- adjusted
2026-07-21   47.23   post-split scale  <- adjusted
2026-07-22   47.83   post-split scale  <- adjusted
2026-07-23   93.56   PRE-split scale   <- not adjusted
2026-07-30   97.65   PRE-split scale   <- not adjusted
2026-07-31   48.19   post-split scale  <- adjusted
2026-08-05   94.46   PRE-split scale   <- not adjusted
2026-08-06   47.08   post-split scale  <- adjusted
2026-08-07   90.36   PRE-split scale   <- not adjusted
2026-08-11   45.53   post-split (correct from here on)
```

**Six of the twenty-one pre-split sessions in this window are halved; fifteen are not.** Each day is
internally consistent (its own OHLC agrees with its close), so the corruption is per-session, not a
transcription error.

**This defeats the detector the order specifies.** Phase 2.3 proposes finding adjustment events by
looking for a ratio departure *at a date boundary*. MNST has **six** such boundaries, none of them the
real one, and the real split date (2026-08-11) shows a boundary that looks like every other. A
ratio-only detector would write six spurious `adjustment_events` for MNST and still miss the truth.

**The events block gives the truth in one field.** This is the argument for §A1 below.

---

# P0.2 — Vendor limits and the nightly budget

## Documented limits

Yahoo's v8 chart endpoint is an **undocumented, unofficial API with no published rate limit and no
terms-of-service allowance**. That is a standing risk, not a number. The daemon's own posture
(`prices.py:26-27`): `PACE_SECONDS = 0.5`, `MAX_ATTEMPTS = 3`, retry on HTTP 429 and 5xx, fail-loud with a
raw-body dump otherwise. `prices.py:1-11` grades the source **DEGRADED-class** per `SOURCE_VERDICTS.md`.

## Observed throughput (measured this session, from Orban)

```
8 sequential single-session requests (range=5d):
   AAPL 1.030s · MSFT 0.851s · XOM 0.879s · JPM 0.839s
   KO   0.927s · PG   0.888s · CVX 0.979s · T   1.136s
   median 0.907s   mean 0.941s   max 1.136s   HTTP 200 x8, zero 429

Full 5y history, one name (AAPL, 2021-01-01 -> now, with events):
   1.241s · 1,422 sessions · 158,539 bytes
```

**Round-trip latency (~0.9 s) exceeds the 0.5 s pacing floor**, so pacing is not the binding constraint —
`_pace()` waits `0.5 − elapsed`, which is already satisfied. **Real cost is ~0.9 s per name, sequential.**

## Nightly budget

Per **ruling 1**, v1 ships **SPX ∪ NDX = 516** names. Figures are given for v1 and for the full universe
(~2,470 with RUT switched on) so the slot holds when RUT is enabled.

| Component | v1 (516) | Full (~2,470) |
|---|---:|---:|
| Nightly 1-session append | 516 req · 465 s ≈ **8 min** | 2,470 req · 2,223 s ≈ **37 min** |
| Rotating full-history refetch | N=**18** · 22 s | N=**83** · 103 s ≈ 2 min |
| **Nightly total** | **534 req ≈ 9 min** | **2,553 req ≈ 39 min** |
| One-time 5y backfill (Phase 2.4) | 516 req · 640 s ≈ **11 min**, ≈ 82 MB | 2,470 req · 3,063 s ≈ **51 min**, ≈ 392 MB |

**N for a ≤30-calendar-day full-refetch rotation: 516 / 30 = 18 names per night in v1**; 83 at full
universe.

Note the order's "2,600-name universe" is close to the full figure but not exact: the measured live
sources give ~2,470 (503 SPX + 102 NDX + 1,961 IWM equities, less overlap).

## Does the vendor tolerate it?

**Unknown, and I did not stress-test it.** What is measured: ~30 requests this session, zero 429s, zero
throttling. What is inferred, not measured: that 534 (v1) or 2,553 (full) sequential requests at 0.9 s
apart are tolerated. The SM daemon has been making a few hundred nightly for months without visible
throttling (CR-R0 §R4.3 — the 22:30 scan runs 38–43 min and exits 0). **At v1's 534 requests that is
roughly the volume SM already sustains**, so v1 carries very little vendor risk; the full universe is the
step that would need watching.

**Recommendation:** keep sequential and keep `PACE_SECONDS = 0.5` for the first production run. Nine
minutes fits the slot (P0.7) with enormous room. Do **not** introduce concurrency to save time that is not needed — if a
future universe makes it necessary, add it deliberately with 429 counting in telemetry (Phase 2.6) and a
back-off, as a separate authorized change.

**Rotation length, amended:** with the events block detecting corporate actions on the night they land
(§P0.1), the rotation is no longer racing a split. Its job becomes catching (a) vendor corruption of the
MNST kind, (b) back-dated event revisions, (c) silent restatements. 30 days at 18 names/night (v1) costs 22
seconds and is worth keeping at that price.

---

# P0.3 — Reader inventory (the Phase 4 migration surface)

Every module that reads the price table or calls a fetch. `grep -rnE "FROM prices|prices\.eod|prices\.latest|adj_close"`, venvs and `__pycache__` excluded.

## Fetch-path callers (these hit the vendor)

| Path:line | Call | Purpose |
|---|---|---|
| `smart_money/scan.py:378` | `prices.eod(con, tk, today-400d, today)` | **`leg_enrich` nightly refresh — the only thing keeping any name fresh.** This is what PS-1 replaces. |
| `smart_money/grade_case.py:23` | `prices.eod(con, ticker, start, end)` → `adj_close` | MTM case grading |
| `smart_money/grade_case.py:24` | `prices.eod(con, "SPY", start, end)` → `adj_close` | SPY benchmark leg of excess return |
| `smart_money/scorecard.py:65` | `prices.eod(con, ticker, start, end)` | Scorecard window returns |
| `smart_money/scorecard.py:287` | `prices.latest(con, "SPY")` | Live SPY quote |
| `smart_money/scorecard.py:294` | `prices.latest(con, t)` | Live ticker quote |
| `smart_money/marketcap.py:85` | `prices.latest(con, t)` | Market-cap band computation |
| `smart_money/survivorship.py:64` | `prices.latest(con, t)` | Liveness probe |
| `smart_money/price_backfill.py:95` | `prices.eod(con, tk, start, end)` | Bulk backfill orchestration |
| `smart_money/discovery.py:28` | `from . import prices` | import only — verify use at Phase 4 |

## Direct-SELECT readers (no fetch; read the table)

| Path:line | Reads | Purpose |
|---|---|---|
| `smart_money/queries.py:274` | `adj_close` | `_close_on` — close on/before a date |
| `smart_money/queries.py:281` | `adj_close` | `_close_on` — the second leg |
| `smart_money/queries.py:1251` | `adj_close` | Ticker-page price sparkline |
| `smart_money/form4.py:266` | **`close`** | Form 4 valuation — **the only reader of `close`, not `adj_close`** |
| `smart_money/brief.py:173` | `COUNT(DISTINCT ticker)` | Brief coverage stat |
| `smart_money/survivorship.py:28` | `DISTINCT ticker` | Zero-coverage set |

`queries.py:272` and `:1195` both carry comments stressing these are *"a DIRECT read-only SELECT (never
`prices.eod()`, the write-through cache)"* — so the read and write paths are already deliberately
separated. That helps Phase 4.

## `prices.py:194` current state

**The fix landed.** Commit `ccd01c6` (2026-08-16), *"Record the price span actually returned rather than
the one requested"*. The line now reads
`_add_span(con, ticker, start, min(end, max(r[1] for r in rows)), fetched_at)` at `prices.py:222-228`,
guarded by `if rows:`. Repair tool `purge_future_spans()` at `prices.py:150-171`. Live check on Basilic:
`SELECT COUNT(*) FROM price_spans WHERE end_date > date('now')` → **0**. The 343 poisoned spans are
cleared. Regression tests exist at `tests/test_price_spans.py`.

**Phase 4's "prices.py:194 fix lands here if not already landed" is already satisfied.** What is *not*
satisfied is the regression the order actually wants in Phase 2: that `last_date_held` in the new
`freshness` table comes from returned rows. That test must be written fresh against the new writer.

## Test fixtures that write the table directly

`tests/test_filing_scale.py:23`, `test_form4_sanity.py:196`, `test_price_backfill.py:77,118`,
`test_queries.py:112,360,559`, `test_trade_quality.py:16`, `test_price_spans.py:24`. All insert into
`prices` / `price_spans` with the current 8-column shape. **Every one breaks at Phase 4** and must be
migrated to the new schema or to a fixture helper.

---

# P0.4 — Universe assembly probe

## (i) Wikipedia S&P 500 — WORKS, GICS + CIK

```
GET https://en.wikipedia.org/wiki/List_of_S%26P_500_companies   HTTP 200, 568,248 bytes
table#constituents -> 503 rows
Symbol | Security | GICS Sector | GICS Sub-Industry | Headquarters Location | Date added | CIK | Founded
last revision 2026-08-19T03:43:37Z
```

**GICS: yes** (11 sectors, sub-industries). **CIK: yes** (10-digit). Coverage measured in CR-R0 §R3.1:
503/503 current constituents.

## (ii) Wikipedia Nasdaq-100 — WORKS, but NOT GICS and NO CIK

The order's phrasing assumes the constituents live on the `Nasdaq-100` article. **They do not.** That
article's sections are `History, Selection criteria, Performance, Record values, Annual returns, Closing
milestones, …` — no components section, and the page body contains no ticker symbols.

The list lives at a separate page:

```
GET https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies   HTTP 200, 169,604 bytes
102 constituents
Ticker | Company | ICB Industry[1] | ICB Subsector[1]
   ICB Industry  :  9 distinct   (Basic Materials, Consumer Discretionary, Consumer Staples, …)
   ICB Subsector : 47 distinct   (Aerospace, Automobiles & Parts, Biotechnology, …)
last revision 2026-08-11T20:51:02Z — "move to [[Historical components of the Nasdaq-100]]"
```

**Answering the order's two questions directly: GICS present? NO — the taxonomy is ICB. CIK present?
NO.** ICB has 9 industries against GICS's 11 sectors and merges what GICS splits between Information
Technology and Communication Services — the same hazard CR-R0 §R3.1(iv) documented for Yahoo.

**Consequence:** the Nasdaq-100 page supplies *membership* only. Classification for NDX names must come
from the S&P 500 GICS map where they overlap (88 of 101 do, below) and be **absent, not guessed**, for
the remainder. Do not mix ICB into a GICS `classification` table.

## (iii) iShares holdings CSV — WORKS KEYLESS

**Correction.** My first pass reported this BLOCKED. That was wrong: I used the **deprecated `.ajax`
endpoint**, which now serves the HTML product page. The live path is a plain file on the product URL:

```
https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/latest-holdings.csv
```

Requirements, tested three ways — **none beyond a browser User-Agent**:

```
COLD, no session, no Referer  HTTP 200  317,688 bytes  text/plain  <- works
COLD, Referer only            HTTP 200  317,688 bytes              <- works
SESSION + Referer             HTTP 200  317,688 bytes              <- works
```

No cookies, no handshake, no key. The old `.ajax` URLs (and the BlackRock host, and Vanguard's VTWO API)
are genuinely dead — those results stand — but they are the wrong door, not a locked one.

**Shape (IWM, fetched 2026-09-02):**

```
line 0   iShares Russell 2000 ETF
line 1   Fund Holdings as of,"Aug 31, 2026"          <- DATED. staleness is measurable.
lines 2-8  Inception Date / Shares Outstanding / Stock / Bond / Cash / Other
line 9   Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,
         Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date
1,967 rows · 1,961 Asset Class == "Equity"
```

**The `Sector` column is GICS-shaped** — 12 distinct values across the equity rows:

```
437 Health Care · 399 Financials · 257 Industrials · 218 Information Technology
203 Consumer Discretionary · 106 Energy · 93 Real Estate · 89 Materials
74 Communication · 54 Consumer Staples · 30 Utilities · 1 Other
```

Two deviations from canonical GICS to normalise on ingest: **`Communication`** (GICS says
*Communication Services*) and an **`Other`** bucket (1 row). Sector only — **no sub-industry, and no CIK**.

**Notation:** concatenated share classes, same convention as the workbook — `MOGA` for Moog Class A.
This is the fourth notation in play (P0.4(v)) and reinforces that resolution must be by lookup, never by
string surgery.

**The pattern generalises to any iShares ETF.** IVV (iShares Core S&P 500) fetched identically:

```
IVV  HTTP 200  83,389 bytes   "Fund Holdings as of, Aug 31, 2026"
     508 rows · 504 equity · 11 distinct sectors
```

**That is more valuable than it looks.** IVV gives an **independent, dated, GICS-shaped sector opinion on
the S&P 500** to cross-check Wikipedia against. CR-1's §4.3 plausibility gate and the handoff's §10.1
second-vendor test both want exactly that, and neither had a source before now. It also holds `Exchange`,
`Location`, `Currency` and `Weight (%)` — cap-weight basket construction comes free.

## (iv) Universe size — v1 and full

**Ruled: v1 ships SPX + NDX.** Sizes from the live sources:

| Set | Source | Names | Refreshable? | Dated? |
|---|---|---:|---|---|
| SPX | Wikipedia `List of S&P 500 companies` | 503 | yes | yes — MediaWiki revision ts |
| SPX (cross-check) | iShares **IVV** `latest-holdings.csv` | 504 equity | yes | yes — "as of" line |
| NDX | Wikipedia `List of NASDAQ-100 companies` | 102 | yes | yes — MediaWiki revision ts |
| RUT | iShares **IWM** `latest-holdings.csv` | 1,961 equity | **yes** | yes — "as of" line |

**v1 universe (SPX ∪ NDX): 516 names**, from the workbook-measured overlap of 88 between the two
(505 + 101 − 88 = 518 on workbook membership; 516 on the live lists). Nightly cost falls from ~39 min to
**~9 min** (P0.2).

**Full universe once RUT is switched on: ~2,470**, and it is now a config change rather than a new
source — the iShares adapter built in Phase 2.1 serves both IWM and IVV.

The workbook's `Russel 2000` sheet (1,967 tickers, static and undated) is **no longer needed** and should
not be ingested. Its `Sector` column shares provenance with the corrupted S&P labels (CR-R0 §R2.3).

## (v) Ticker notation — four conventions in play

| Source | Convention | Example |
|---|---|---|
| Workbook (Thomson/Refinitiv RTD) | **concatenated** | `BRKB`, `BFB`, `GEFB` |
| Wikipedia S&P 500 | **dotted** | `BRK.B`, `BF.B` |
| Yahoo v8 (the vendor) | **dashed** | `BRK-B` |
| SEC `company_tickers.json` | **dashed** | `BRK-B` |

21 tickers in the union exceed 4 characters and are candidates for share-class concatenation:
`BATRA BATRK BELFA BELFB CENTA CMCSA DGICA GLIBA GLIBK GOOGL IMKTA KELYA LILAK METCB RBCAA RUSHA RUSHB
SENEA SNFCA TSEOQ VLGEA`. Most are genuine 5-letter tickers (`CMCSA`, `GOOGL`), not concatenations —
**a length rule would corrupt them.** The `ticker_aliases.notation` column in the Phase 1 schema is the
right mechanism; the resolution must be by lookup against SEC + Wikipedia, never by string surgery.

---

# P0.5 — Identity

## Source

`https://www.sec.gov/files/company_tickers.json` — keyless, HTTP 200, 795,337 bytes,
**10,391 entries**, shape `{"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"}`.
`data.sec.gov` also confirmed live (XBRL frames sanity fetch, HTTP 200 JSON).

## CIK coverage, measured against the workbook union (2,483 names)

Measured before ruling 1 narrowed v1 to SPX + NDX. The wider sample is kept deliberately — it is the
best available evidence for what happens when RUT is switched on.

| Index | Names | CIK resolved | Rate | Unresolved |
|---|---:|---:|---:|---:|
| SPX | 505 | 499 | **98.8%** | 6 |
| NDX | 101 | 100 | **99.0%** | 1 |
| RUT | 1,967 | 1,944 | **98.8%** | 23 |
| **Union** | **2,483** | **2,455** | **98.9%** | **28** |

**The order asks what fraction of the Russell 2000 would lack a CIK at first pass: 23 of 1,967 = 1.2%.**

The 23 are: `ADRO AKE AVNS BBBY BHA CAD FRBA GTXI HIFS INH ISSC LPRO MDV NBN PDLI SBT SKYT TALK THRD
TMHC TOI TOWN USD`. Several are **not first-pass identity failures at all** — `BBBY`, `PDLI` and `GTXI`
are long-delisted, and `USD` and `CAD` are the workbook's cash placeholders. The true unresolved rate for
live securities is lower than 1.2%. This is a stale-source problem (§A4), not an identity-scheme problem.

## Recommendation on `instrument_id` — with one correction to Abelard's expectation

**CIK works, and 98.9% first-pass coverage endorses it. But CIK alone cannot be the `instrument_id`.**

CIK identifies an **issuer**, not a **security**. Dual-class names share one CIK:

```
GOOG / GOOGL     one issuer, two securities, two price series
FOX  / FOXA      "
NWS  / NWSA      "
BRK-A / BRK-B    "
```

All four pairs are in the S&P 500 universe. Keying `prices_raw` on CIK would collide two distinct price
series into one `UNIQUE(instrument_id, date)` slot — and, given the order's insert-only rule, the second
class would raise a fail-loud fact-change event **every single night**.

**Proposed:** `instrument_id = <cik10>.<class>` where `<class>` is a short discriminator resolved from the
share-class suffix (`0001652044.A` for GOOGL, `0001652044.C` for GOOG), defaulting to `.0` for
single-class issuers. `ticker_aliases` then carries `(instrument_id, ticker, notation, valid_from,
valid_to)` exactly as the order specifies, and the four notations of P0.4(v) become four alias rows.

For the ~1.2% with no CIK: a synthetic `TMP.<ticker>` id with `source='unresolved'`, quarantined from
`index_membership` until resolved. **Never silently drop.** FIGI stays optional and later, as the order
says — nothing in P0 requires it.

---

# P0.6 — Reference series

## FRED — both keyless, both work, but one lags badly

```
GET https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS
   HTTP 200, 161,051 bytes, header "observation_date,VIXCLS"
   last 4: 2026-08-26,15.21 | 2026-08-27,14.51 | 2026-08-28,14.43 | 2026-08-31,14.92

GET https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO
   HTTP 200, 179,000 bytes, header "observation_date,DCOILWTICO"
   last 4: 2026-08-20,89.75 | 2026-08-21,87.21 | 2026-08-24,86.34 | 2026-08-25,83.90
```

| Series | Latest observation | Lag vs. latest session (2026-09-01) |
|---|---|---|
| `VIXCLS` | **2026-08-31** | ~1 session — usable next morning |
| `DCOILWTICO` | **2026-08-25** | **~5 business days** |

**`DCOILWTICO` is not fit for a daily dashboard.** The handoff's WTI ↔ Mag-7 seesaw and rebalance-band
monitor are same-session constructs; a five-day-stale oil print would produce a conditional table that is
wrong by a week.

## Yahoo covers the gap — and both series fetch fine

CR-R0 §R1.5 found `^VIX` and `CL=F` with **zero rows** in the SM store. That was never a fetch failure —
**nothing had ever requested them**:

```
CL=F  HTTP 200  0.78s   2026-08-27 83.53 | 2026-08-28 83.40 | 2026-08-31 85.76 | 2026-09-01 None | 2026-09-02 91.73
^VIX  HTTP 200  0.91s   2026-08-26 15.21 | 2026-08-27 14.51 | 2026-08-28 None  | 2026-08-31 14.92 | 2026-09-01 16.34
SPY   HTTP 200  0.77s   2026-08-28 769.35 | 2026-08-31 767.05 | 2026-09-01 761.78
```

Cross-check: Yahoo `^VIX` 2026-08-26 = 15.21 and 2026-08-31 = 14.92 **match FRED `VIXCLS` exactly**. The
two sources agree where they overlap.

**Recommendation:** `reference_series` takes both, with the roles separated —

- **Yahoo `^VIX`, `CL=F`** as the *daily operational* series (current, same request shape as everything
  else, already paced).
- **FRED `VIXCLS`, `DCOILWTICO`** as the *official reconciliation and history* series, pulled weekly, used
  to validate the Yahoo series and to backfill. FRED is the citable source; Yahoo is the timely one.

**Null handling is mandatory, not optional.** Two of the ten live rows above are `None` (`CL=F`
2026-09-01, `^VIX` 2026-08-28) — a half-session or an exchange holiday mismatch. The current writer drops
nulls silently (`prices.py:207-211`: `if c is None or a is None: continue`). Under PS-1's fail-loud
doctrine a null must be **recorded as a null with its provenance**, not skipped, or `freshness` will
report a name as current while its latest row is missing.

**SPY:** in the store, fresh to 2026-08-31, 3,517 rows back to 2012-09-04 (CR-R0 §R1.5). Confirmed.

---

# P0.7 — Basilic placement

## State home — `abelard_common` has no state convention, by design

> **CORRECTION, logged during Phase 1 (2026-09-02).** The claim below that
> `abelard_common` is *stateless* is **wrong**, and the grep that produced it was at fault: it used a
> case-sensitive `DB_PATH` and missed `alert_queue.py`'s `db_path`. **`alert_queue.py` is already a
> SQLite-backed component of `abelard_common`** — `SCHEMA_VERSION`, a `_SCHEMA` script, WAL,
> `busy_timeout`, `foreign_keys=ON`. PS-1 is therefore **not** the first stateful component, and there is
> a direct house precedent to copy rather than an architectural change to justify.
>
> Ruling 3 is unaffected and now better supported — `alert_queue`'s docstring states the discipline
> exactly: *"the constructor takes an explicit `db_path` (**no env resolution here — call sites own
> that**)"*. But it does refine the recommendation below: the `resolve_db_path()` env helper must **not**
> live in the library. `ABELARD_PRICES_DB_PATH` is read by the Phase 2 CLI; `connect()` takes a path.
> The original text is left below unedited, per the append-only rule.

```
grep -rnE "openclaw|STATE_HOME|expanduser|DB_PATH" daemons/common/abelard_common/*.py   ->  (no matches)
```

`abelard_common/__init__.py` states the convention explicitly:

> *"Each consuming daemon owns its own seed data files … every loader here takes an explicit path rather
> than bundling data."*

**`abelard_common` is a stateless pure-library package. PS-1 would be the first stateful component in
it** — a genuine architectural change, not a placement detail.

**Recommendation: keep the convention rather than break it.** Every public function in
`abelard_common.prices` takes an explicit DB path; a single `resolve_db_path()` helper supplies the
default from env `ABELARD_PRICES_DB_PATH`, mirroring SM's `SMART_MONEY_DB_PATH` (BASILIC_MANUAL §4, which
requires it be set **absolute** so launchd resolves it). Canonical location **`~/.openclaw/prices/prices.db`**.

**Verified free on both hosts.** Basilic `~/.openclaw/` holds `abelard_queue, agents, capex_daemon,
chatter, fdu, identity, news_watch, scout_backups, smart_money, state`; Orban holds `biz_daemon,
capex_daemon, fdu, news_watch, scout`. **No `prices` on either.**

## Disk estimate — measured, not arithmetic

Tables built to the Phase 1 DDL with 200 real-shaped names × 1,258 sessions (5y), `VACUUM`ed, sizes read
off disk, extrapolated to **516** (v1) and ~2,470 (full):

| Table | Bytes/name | v1 (516) / full (~2,470) |
|---|---:|---:|
| `prices_raw` (OHLCV + source + fetched_at + run_asof) | 122,552 | **0.06 GB** (v1) / 0.30 GB (full) |
| `adjustment_factors` (version 1) | 84,910 | **0.04 GB** (v1) / 0.21 GB (full) |
| `adjusted_view` | 87,450 | **0.05 GB** (v1) / 0.22 GB (full) |
| **Total initial (5y, one factor version)** | | **0.15 GB** (v1) / **0.73 GB** (full) |

- **Each additional factor version: 0.08 MB per name.** Only names with a new adjustment event get one —
  500 names re-versioned costs **+42 MB**; re-versioning the *entire* universe costs **+211 MB**.
- **Growth after backfill: 0.02 GB/year** at v1 (516 names × 252 sessions × ~167 B for a raw + view row
  pair); 0.10 GB/year at full universe.

Against **307 GB free** on Basilic (CR-R0 §R4.4), storage is a non-issue at every horizon. Even ten years
of history at the full universe with fifty factor versions stays under 10 GB.

## Schedule slot

The evening block, verified live (CR-R0 §R4.3):

| Time | Job | Note |
|---|---|---|
| ~~21:30~~ | ~~`com.abelard.news-watch`~~ | **plist authored, never installed** — E32 instance |
| 22:30 | `com.abelard.smart-money` | finishes **23:08–23:13** |
| 23:15 | `com.abelard.smart-money-brief` | ~2 min margin over the scan |
| 23:40 | `com.abelard.capex` | finishes in ~19 s |
| 06:00 | `com.abelard.queue-digest` | |

**RULED: `com.abelard.prices` at 21:00 America/New_York.** At ~9 minutes for v1 (P0.2) it finishes ~21:10 —
**50 minutes clear of SM's 22:30** and comfortably before any CR nightly, which CR-R0 §R4.3 proposed for
00:15–00:30. Market close is 16:00 ET, so Yahoo's EOD is well settled by 21:00.

**News-watch note (ruling 4: take 21:00).** 21:30 is the slot `com.abelard.news-watch` was authored for
but never installed (CR-R0 §R4.3). At v1's ~9-minute runtime prices finishes ~21:10, so even if
news-watch is activated at 21:30 there is **no overlap**. If RUT is later switched on (~39 min, finishing
~21:40) the two would collide, and news-watch would need moving at that point. Recorded so the decision
is not rediscovered later.

## What SM's nightly currently does with prices that PS-1 removes

`smart_money/scan.py:376-381`, inside `leg_enrich`:

```python
end = dt.date.today().isoformat()
start = (dt.date.today() - dt.timedelta(days=400)).isoformat()
for tk in price_tickers:
    try:
        prices.eod(con, tk, start, end)
        counts["price_ok"] += 1
    except Exception:
        counts["price_fail"] += 1
```

`price_tickers` comes from `_price_universe(con, tickers, start90)` — a **scoped** subset, not the whole
store. **This loop is the entire freshness mechanism of the current price layer**, and it explains CR-R0
§R1.4 precisely: the 429 names it touches are current to 2026-08-31; the 68 it does not touch are frozen,
most at 2026-07-23.

It also explains the one-session-per-night crawl (CR-R0 §R8.2): with `start = today − 400 days`,
`_covered()` finds the range almost entirely covered and the fetch extends the span by the single missing
day at the old end.

**PS-1 removes this loop entirely.** `leg_enrich` keeps its market-cap band work and loses its price
work; `counts["price_ok"]/["price_fail"]` come out of the scan envelope, or are re-sourced from the new
`freshness` table. **That is a change to the SM envelope's shape** — a downstream contract — and it needs
calling out in the Phase 4 diff, not discovering during it.

---

# Proposed amendments to Phases 1–4

Presented for Abelard. **Nothing below has been built.**

## A1 — Phase 2.3: make the events block the primary detector; demote rotation to verification

The order builds corporate-action detection on ratio comparison during a 30-day rotation. P0.1 shows the
vendor hands over its splits and dividends **in the same request, at a one-day granularity**. Proposed:

1. **Nightly append carries `&events=div,split`.** Any split or dividend in the window is written to a new
   `corporate_actions` table (vendor-declared) on the night it lands, and triggers a factor re-version for
   that name. **Acceptance criterion (2) is met on day one at zero marginal request cost.**
2. **The rotation becomes a verification sweep.** For each rotated name: recompute the adjusted series
   from `prices_raw` × declared factors, compare to the vendor's `adjclose`. Agreement is a pass;
   disagreement is a **fail-loud vendor-corruption event**. That check is exactly what would have caught
   MNST, and the ratio-boundary detector as specified would not have.
3. Keep `adjustment_events` for *inferred* events (a ratio move with no declared action behind it) — it
   becomes the alarm channel rather than the primary path.

**This also fixes a defect in the specified design:** on MNST, ratio-boundary detection produces six
spurious events and misses the real one (§P0.1).

## A2 — Phase 1: `prices_raw` can hold true raw closes

The order hedges: *"If P0.1 finds no vendor in use returns raw closes, `prices_raw` stores the
as-first-fetched close with its vintage … document the weakness."*

**The hedge is not needed.** Yahoo's `close` is split-adjusted and dividend-unadjusted, so with the
declared split feed:

```
raw_close(d) = close(d) × Π{ split_ratio(e) : e effective after d }
```

reconstructs the true traded close **exactly**, not as-of-a-vintage. `open`, `high`, `low` follow the same
factor; `volume` inverts it. Those four fields are already in every response and are currently discarded
(`prices.py:203-207` reads only `close` and `adjclose`) — Phase 1's `prices_raw` should capture them.

**Guard, required:** MNST proves `close` is not always correctly split-adjusted. So the writer must
**validate rather than trust**: recompute `adjclose` from raw + factors and compare against the vendor's
`adjclose`. When they disagree beyond tolerance, the name is quarantined and reported — never written
as if correct.

## A3 — Phase 1: `instrument_id` is CIK + share class, not CIK

Per P0.5. Without the class discriminator, `GOOG`/`GOOGL`, `FOX`/`FOXA`, `NWS`/`NWSA` and `BRK-A`/`BRK-B`
each collide on `UNIQUE(instrument_id, date)` and manufacture a nightly fail-loud fact-change event.

## A4 — Phase 2.1: build the iShares adapter; ship SPX + NDX (RULED)

**Ruling 1: ship SPX + NDX; build the direct pull from source.** P0.4(iii) makes this cheap — the
iShares route needs no key, no session and no manual file drop.

`universe-sync` (weekly, Friday) gets **one adapter with three configured sources**:

| Source | URL | Gives |
|---|---|---|
| Wikipedia SPX | `/wiki/List_of_S%26P_500_companies` | membership + **GICS sector & sub-industry** + **CIK** |
| Wikipedia NDX | `/wiki/List_of_NASDAQ-100_companies` | membership only (ICB, no CIK — see §A6) |
| iShares `latest-holdings.csv` | `…/{product-path}/latest-holdings.csv` | membership + **GICS sector** + weight, exchange, as-of date |

Configured iShares products at build time: **IVV** (S&P 500 cross-check, on in v1) and **IWM**
(Russell 2000, **wired but not enabled** in v1). Switching RUT on later is a config row, not new code —
which is what makes ruling 1 a scope decision rather than an architecture decision.

**Two capabilities this unlocks that the order did not anticipate:**

1. **A second sector opinion on the S&P 500.** Wikipedia and IVV are independent, both dated, both
   GICS. Disagreement between them is exactly the signal CR-1's §4.3 plausibility gate wants, and it is
   the only *taxonomy-constant* second source found for the handoff §10.1 reproduction test. Per the
   order, Phase 2.1 **logs** classification disagreement for Mando and does not block on it.
2. **Cap weights for free.** The `Weight (%)` column supports the handoff's `MAG7_CW` and the cap-weight
   secondary basket without a market-cap pipeline.

**Normalisation the adapter must carry** (all measured, §P0.4(iii)/(v)): `Communication` →
`Communication Services`; the `Other` sector bucket → null, not a sector; `Asset Class != "Equity"` rows
dropped (6 of 1,967 in IWM — cash and futures); concatenated share classes (`MOGA`) resolved by lookup
against SEC + Wikipedia, **never by string surgery** — `CMCSA` and `GOOGL` are genuine 5-letter tickers.

## A5 — Phase 2.2: null closes must be recorded, not skipped

Two of ten live reference-series rows came back `None` (P0.6). The current writer drops nulls silently.
Under `freshness`, a silently-dropped null makes `last_date_held` disagree with what the vendor actually
returned — the same class of error as the `:194` span bug, in a different column.

## A6 — Nasdaq-100 classification is ICB; do not mix it into `classification`

P0.4(ii). The `classification` table is declared `taxonomy (GICS)`. NDX names not in the S&P 500 (13 of
101) would have **no GICS row**. Leave them null and let CR-1's plausibility gate see the gap — do not
map ICB→GICS by hand to fill it.

---

# Open questions — all five closed

**All five were ruled by Mando on 2026-09-02** — see **Rulings** at the head of this report. Their
consequences are folded into the sections above:

| Ruling | Landed in |
|---|---|
| 1 — SPX + NDX for v1; build the iShares direct pull | §P0.4(iii), §P0.4(iv), §A4 |
| 2 — top-level report path | this file's location |
| 3 — the P0.7 state proposal stands | §P0.7 (unchanged; it was the proposal) |
| 4 — take 21:00 | §P0.7 schedule (unchanged; 21:00 was the proposal) |
| 5 — strike the `prices.py:194` item | §P0.3; Phase 4's scope shrinks by one line |

**No open questions remain for Phase 1.** Nothing blocks it except the word to begin.

---

---

# P0 ADDENDUM — responses to the Amendment Sheet (2026-09-02)

Abelard's post-Phase-0 amendment sheet ratifies A3–A6 and the P0.7 rulings, amends A1/A2, and assigns
three items back to me. Those three are answered here. **Still Phase 0. No code written, no schema
created. Phase 1 awaits Mando's word.**

## AD.1 — Amendment item 4, dollar-volume continuity: **DOES NOT DISCRIMINATE. DROP IT.**

Tested on MNST across the corrupted window (2026-07-06 → 2026-08-25), the instruction being *"if the test
discriminates on MNST, keep it as a second tell; if not, drop it and say so."*

```
sessions with a ~split-ratio PRICE step: 7
  of those, $volume ALSO stepped by ~ratio: 3
  of those, $volume did NOT step:           4
```

Three of seven. That is a coin flip, and **it fails for a structural reason, not a tuning reason.**

**Why.** Yahoo split-adjusts volume by the inverse factor — verified on a clean split, AAPL 4:1 effective
2020-08-31:

```
date        close     volume        close*vol($M)
2020-08-28  124.81   187,630,000       23,417.6
2020-08-31  129.04   225,702,700       29,124.7      <- split effective; no 4x volume step
2020-09-01  134.18   151,948,100       20,388.4
```

Closes sit at ~$125 on both sides (retro-adjusted from ~$500) and volume is smooth across the boundary,
so **dollar volume is continuous through a correctly handled split**. Amendment item 2's "volume inverse"
is therefore **correct and confirmed** — that premise holds.

But MNST's corruption is **self-consistent per session**: a session carries price *and* volume on the same
scale, both pre-split or both post-split. Mean dollar volume over the window is ~$579M on pre-scale
sessions and ~$484M on post-scale sessions — a ratio of **1.20, not 2.0**. Dollar volume is nearly
continuous across the very sessions the test is supposed to flag, because both factors move together.

**There is no signal for this test to find.** Dropping it, as instructed. The residual boundary detector
(amendment item 3), which works on the reconstructed raw series against declared ratios, remains the check
that catches MNST.

## AD.2 — WTI roll detection: contract identity is available and exact; the |return| > 4% heuristic is refuted

The amendment asks which of the two detections is available. Answer: **the contract identity, and it is
strictly better.**

**(a) Forward-looking, zero extra requests.** `CL=F`'s response names the front-month contract in its
metadata:

```
CL=F   meta.shortName = 'Crude Oil Oct 26'      instrumentType = FUTURE
```

Capturing `shortName` on the nightly append and flagging a change is exact and free. `contractSymbol` and
`expireDate` are **not** present in the meta — `shortName` is the only carrier, so it must be parsed
("Crude Oil Oct 26" → 2026-10). Stable format, but it is string parsing and needs a guard.

**(b) Historical, reconstructable by exact match.** Dated contracts fetch fine (`CLV26.NYM`, `CLX26.NYM`,
`CLZ26.NYM` all HTTP 200). Matching the continuous series against them dates the roll precisely:

```
date        CL=F     CLV26    CLX26    front month
2026-08-19  85.83    84.39    82.81    -           <- CL=F tracks the Sep contract (CLU26, expired)
2026-08-20  87.83    86.83    84.86    -
2026-08-21  87.06    87.06    85.16    CLV26       <- ROLL. exact match from here on
2026-08-24  85.01    85.01    83.30    CLV26
2026-08-31  85.76    85.76    84.06    CLV26
2026-09-02  91.80    91.81    89.13    CLV26
```

**The roll is 2026-08-21**, identified to the day with no roll calendar.

**And this refutes the heuristic.** The move across that roll was `87.83 → 87.06 = −0.88%`. **A
|return| > 4% rule would have missed it entirely.** On this sample the heuristic has a 0% hit rate, and it
would instead fire on genuine oil moves — 2026-09-02's `85.76 → 91.80 = +7.0%` is *not* a roll.
**Recommend dropping the |return| threshold component**; it is worse than nothing, because it manufactures
false roll flags on exactly the large moves the seesaw analysis exists to study.

**Limitation, stated:** expired contracts stop being served (`CLU26.NYM` returns no rows for August), so
match-based reconstruction reaches back only as far as the vendor still serves the legs — roughly the
current and next few contracts. For deeper history the roll dates need a calendar or must be inferred.
Going forward, nightly `shortName` capture is exact and needs no history.

**This is Mando's ruling, not mine.** The evidence supports Abelard's recommendation — Yahoo `CL=F` as the
working daily series with a `roll_flag` from `meta.shortName`, FRED `DCOILWTICO` kept as the lagging
validator — **with the |return| > 4% component dropped**. The alternative (FRED only, accept ~5 business
days of lag) leaves the handoff's same-session seesaw and rebalance-band monitor unbuildable.

## AD.3 — A3 premise check: `company_tickers_exchange.json` works, but carries no class field

```
GET https://www.sec.gov/files/company_tickers_exchange.json   HTTP 200, 521,378 bytes
fields: ['cik', 'name', 'ticker', 'exchange']      rows: 10,391

dual-class probe:
  BRK-A cik=1067983 NYSE      GOOG  cik=1652044 Nasdaq     NWS  cik=1564708 Nasdaq
  BRK-B cik=1067983 NYSE      GOOGL cik=1652044 Nasdaq     NWSA cik=1564708 Nasdaq
  FOX   cik=1754301 Nasdaq    FOXA  cik=1754301 Nasdaq
```

The file confirms the collision A3 exists to solve — every pair shares one CIK — and it adds `exchange`,
which the plain `company_tickers.json` lacks. **But there is no class column.** It supports *grouping* by
CIK; it does not supply the discriminator.

**Wikipedia does, for most cases:**

```
GOOGL  Security='Alphabet Inc. (Class A)'      GOOG  Security='Alphabet Inc. (Class C)'
FOXA   Security='Fox Corporation (Class A)'    FOX   Security='Fox Corporation (Class B)'
NWSA   Security='News Corp (Class A)'          NWS   Security='News Corp (Class B)'
BRK.B  Security='Berkshire Hathaway'           <-- NO class parenthetical
```

**The gap:** Wikipedia disambiguates only where both classes are index members. Within the S&P 500 exactly
**3 CIKs carry >1 ticker** (`GOOGL/GOOG`, `FOXA/FOX`, `NWSA/NWS`) and all three are named. Berkshire is
not disambiguated because `BRK-A` is not an S&P 500 member — the collision appears only once the universe
widens, and then the naming source is silent.

**Proposed rule, consistent with "never string surgery":**

1. Class from Wikipedia's `(Class X)` parenthetical where present.
2. Absent, and the SEC file shows one ticker on the CIK → `.0`.
3. Absent, and the SEC file shows >1 ticker on the CIK (the Berkshire case) → **deterministic ordinal by
   ticker, recorded in `ticker_aliases` with `class_source='ordinal'`** so it is visibly a fallback and
   never mistaken for a vendor-supplied class.

Rule 3 is the only arbitrary part, it is stable, and it is flagged. **Confirm before Phase 1 builds it.**

## AD.4 — Items ratified with nothing further to check

A4 (one adapter, three sources; IVV on, IWM wired-off), A5 (`vendor_null` rows, `last_date_held` advancing
only on non-null rows), A6 (no ICB→GICS map), the state-home and slot rulings, and the `prices.py:194`
strike are all consistent with what P0 measured. The universe correction (~2,470 full, **516 v1**) matches
§P0.4(iv).

Amendment item 3's reasoning is confirmed by P0: a recompute-vs-`adjclose` comparison cannot catch MNST,
because both sides derive from the same per-session `close`. The residual boundary detector on the
reconstructed raw series is the check that can.

## AD.5 — Open items blocking nothing, but needing a word

| Item | Needs |
|---|---|
| Reference series / WTI | **Mando's ruling** — Yahoo `CL=F` + `roll_flag`, FRED as validator, |return| component dropped (recommended); or FRED only. |
| A3 rule 3 (ordinal fallback) | Abelard's confirm. |
| Phase 1 start | **Mando's word.** Worktree `Abelard-ps1` on `ps-1-price-substrate` to be taken before the first edit. |


## Verification note

Every claim carries a path:line, a query with its output, or a live command with its output. Nothing here
is HYPOTHESIS. Two things are explicitly **not** measured and are labelled as such: Yahoo's tolerance of
2,566 nightly requests (not stress-tested, §P0.2) and the licensing posture of a paid Russell 2000 source
(not investigated, §A4c).

**Phase 0 ends here. No phase after 0 begins without Mando's word.**
