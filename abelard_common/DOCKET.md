# PS-1 — Build Docket

Append-only. One entry per phase, written as the phase closes. Newest at the
bottom. Nothing here is edited after it is written; a correction is a new entry
that says what it corrects.

Order: **PS-1 — Price Substrate Hoist into `abelard_common`** (Abelard, 2026-09-02)
Amendment sheet: post-Phase-0, 2026-09-02.
Worktree: `Abelard-ps1` on branch `ps-1-price-substrate` (E18, one writer per tree).

---

## Phase 0 — Recon · CLOSED 2026-09-02 · accepted

Report: `abelard_common/recon/PS-1-P0.md` (untracked in the `main` checkout).

Established: one vendor (`yahoo_v8`, `smart_money/prices.py:21`); no raw close
from any available vendor (Stooq behind a JS proof-of-work challenge, Finnhub
candle/split both HTTP 403); **the vendor publishes its own corporate actions**
via `&events=div,split` at one-day granularity; universe is 516 for v1 and
~2,470 full; CIK resolves 98.9%; FRED `DCOILWTICO` lags ~5 business days.

Five rulings taken (Mando, 2026-09-02): ship SPX+NDX; top-level report path;
explicit-path state convention; 21:00 slot; `prices.py:194` line item struck.

**Corrections logged against my own P0 findings:**

1. **iShares was reported BLOCKED. It is not.** I tested the deprecated
   `.ajax?fileType=csv` endpoint. The live path is
   `…/{product}/latest-holdings.csv` and it works cold — no key, no cookies, no
   Referer. Corrected in place in P0.4(iii). Mando caught this.
2. **`abelard_common` was reported stateless. It is not.** `alert_queue.py` is
   SQLite-backed with `SCHEMA_VERSION`, a `_SCHEMA` script and WAL. My grep used
   a case-sensitive `DB_PATH` and missed its `db_path`. Corrected in P0.7 — see
   the Phase 1 entry for what changed as a result.

---

## Phase 1 — Schema · CLOSED 2026-09-02 · awaiting Mando disk-review

**Delivered**

| Artifact | Path |
|---|---|
| Package | `daemons/common/abelard_common/prices/__init__.py` |
| Migration (all DDL, version-stamped) | `daemons/common/abelard_common/prices/schema.py` |
| Contract tests | `daemons/common/tests/test_prices_schema.py` |
| This docket | `abelard_common/DOCKET.md` |

`SCHEMA_VERSION = 1`, stamped into `price_meta` at creation. One migration file,
every statement `IF NOT EXISTS`, so re-opening is a no-op. `migrate()` refuses to
run against a store stamped newer than the code.

**Tables** — the order's ten, plus four the amendment sheet and P0 require:

- `instruments`, `ticker_aliases`, `index_membership`, `classification`
- `prices_raw`, `adjustment_events`, `adjustment_factors`, `adjusted_view`
- `freshness`, `reference_series`
- **`corporate_actions`** (new; amendment A1 — vendor-declared, distinct from the
  inferred `adjustment_events`)
- **`vendor_adjusted`** (new; the dividend-factor section requires `vendor_adjclose`
  be kept for comparison only — a separate table, not a `prices_raw` column, so
  it cannot be joined into analytics by accident)
- **`run_telemetry`** (new; cost/telemetry-before-persistence)
- `price_meta`

**Enforcement lives in DDL, not in the writer.** Thirteen triggers: `prices_raw`
insert-only; `index_membership`, `classification`, `corporate_actions` and
`adjustment_events` as-of append-only; `adjustment_factors` versions immutable.
`adjusted_view` is deliberately left writable — it is derived and rebuilding it
on a re-version is the normal path.

**Build finding — a real hole, found and closed.**

The first test run was 32 pass / 1 fail, and the failure was the one that
mattered: `INSERT OR REPLACE` on `prices_raw` succeeded despite the delete
trigger. SQLite fires delete triggers during REPLACE conflict resolution **only
when `recursive_triggers` is ON**, which is a per-connection pragma. So a caller
opening the file with a bare `sqlite3.connect()` would have walked straight
through the insert-only guarantee and silently restated a fact — which is
exactly the upsert path this substrate exists to remove.

Closed two ways: `PRAGMA recursive_triggers=ON` in `connect()`, and a
`BEFORE INSERT` trigger (`trg_prices_raw_no_replace`) that fires on the insert
itself and therefore holds **for every connection, however opened**. Pinned by
`test_replace_is_blocked_on_a_bare_connection_too`, which opens the file with a
bare `sqlite3.connect()` and asserts the fact survives.

**Consequence of the P0.7 correction.** `alert_queue`'s docstring states the
house discipline exactly: *"the constructor takes an explicit `db_path` (no env
resolution here — call sites own that)"*. Mando's ruling 3 matches it, so the
ruling stands — but my P0.7 wording put a `resolve_db_path()` env helper inside
the library. That would have broken the precedent. **`ABELARD_PRICES_DB_PATH` is
read by the Phase 2 CLI, never by this package.** `connect()` takes a path.

**Tests: 34 new, 113 total in `abelard_common`, all passing.** Not shape tests —
each pins a rule against a defect CR-R0 measured in the layer being replaced:
insert-only vs. the 92%-of-names vintage fragmentation; REPLACE-blocked vs. the
old upsert; dual-class non-collision vs. A3's nightly fact-change; `vendor_null`
rows recorded vs. silently dropped nulls; two sources disagreeing and both kept
vs. a resolved-away classification conflict.

**Not built, by design.** No writer, no fetch, no analytics, no CLI, no plist —
Phases 2–4. Nothing was run against Basilic. No commit.

**Open, carried into Phase 2**

- **Reference-series ruling still outstanding** (Yahoo `CL=F` + `roll_flag` with
  FRED as validator, vs FRED only). The schema serves either: `reference_series`
  carries nullable `contract` and `roll_flag`, so no migration is needed once the
  ruling lands. P0 addendum AD.2 recommends dropping the `|return| > 4%`
  component — it missed the one real roll in the sample (−0.88%) and would fire
  on genuine moves.
- **A3 rule 3** (ordinal class fallback for the Berkshire case) is built as
  specified — `class_source='ordinal'` — but still wants Abelard's confirm.
- The Phase 0 report is untracked in the `main` checkout while this branch holds
  the build. Whether it joins the branch at commit time is Mando's call.

---

## Phase 2 — Writer · CLOSED 2026-09-02 · awaiting Mando disk-review

**Delivered**

| Module | Lines | What |
|---|---:|---|
| `prices/reconstruct.py` | ~380 | Pure functions: raw reconstruction, factor series, corruption detector |
| `prices/vendor.py` | ~200 | Yahoo v8 adapter; always `events=div,split`; fail-loud on schema drift |
| `prices/writer.py` | ~430 | `ingest_series` + nightly / backfill / refetch / status; telemetry |
| `prices/universe.py` | ~450 | Three-source adapter, identity resolution, as-of membership |
| `prices/cli.py` | ~140 | `abelard-prices`, five subcommands, exit-code alerting contract |
| tests ×4 | ~1,100 | 159 passing (79 pre-existing + 80 new) |

`HttpClient` is **reused, not reimplemented** — the house has logged the
duplicate-HttpClient debt three times and a fourth copy is not the contribution
to make here. `universe.py` parses HTML with stdlib `html.parser` rather than
adding `lxml`, because `abelard_common` declares only `requests`.

**Verified end to end on live data.** `universe-sync` → 519 instruments in 1 s;
`backfill --limit 6 --since 2021-01-04` → 6 names × 1,423 sessions in 2 s;
`status` → clean, exit 0. Then 8,538 rows checked for phantom returns: worst
single-session move across the six names is **+17.43%**, all genuine; dividend
drift lands in the right order (ABBV 1.244 > ABT 1.115 > AAPL 1.030, matching
their yields); 5 vendor-null sessions **recorded, not dropped**; 0 quarantined.

### Three build findings, two of them real bugs I shipped and caught

**1. The detector was pointed at the wrong series.** First draft ran the
step-detector on the *reconstructed raw* series, on the reasoning that
reconstruction should remove a split's step. That is backwards: raw is the true
traded price, so a split is a genuine 4-for-1 step there — the vendor's
*adjusted* close is the series that must be smooth. The AAPL control caught it:
a perfectly clean 2020 4:1 was flagged as corruption and eight good sessions
quarantined. Moved to the vendor series; AAPL now flags nothing and MNST still
flags seven.

**2. The adjusted view was missing its split component.** `adjustment_factors`
initially carried only the CRSP dividend factor, so `adjusted_view` inherited
raw's split step — a −75% phantom return on every split date, which is the exact
defect this substrate exists to remove. The factor is
`dividend_cumulative(d) / split_factor(d)`. Pinned by a test asserting our
adjusted closes differ from the vendor's `adjclose` by a **constant** (0 spread
across AAPL's window), i.e. identical returns.

**3. Ticker notation bit for real, as P0.4(v) predicted.** iShares serves
`BRKB`, Wikipedia serves `BRK.B`; keyed on the raw string they became two
instruments and the second had no CIK. Fixed with a reverse index over the SEC
file's own tickers — `BRKB` resolves because SEC lists `BRK-B`, whose concat
form is `BRKB`. That is lookup, not string surgery: nothing is transformed
unless SEC vouches for the result, so `CMCSA` and `GOOGL` stay themselves.
Instrument count fell 521 → 519 and the provisional list fell to one name
(`HOLX`, genuinely delisted, correctly flagged rather than dropped).

A fourth, smaller: a first draft compared only *non-quarantined* offered closes
against held facts, so a vendor restating history while also tripping the
detector would have slipped past the fact gate. The comparison is now asymmetric
on purpose — held side facts only, offered side everything.

### Amendment items

* **A1/A2 implemented as amended.** Declared layer primary (`corporate_actions`
  from the events block, on the nightly, zero extra requests); raw
  reconstruction; residual boundary detector re-aimed; rotation demoted to a
  verification sweep. Item 4 (dollar-volume continuity) **dropped per AD.1**.
* **Detector tuning corrected.** A surviving step is not a clean 2.0 — MNST's is
  0.4895, because the stock also moved −2.1% that session. Matching now divides
  the declared ratio out and asks whether the *residual* is a plausible session
  move. A tight tolerance on the ratio itself mislabelled every real case.
* **Quarantine is span-based, and extends when the split date itself is
  discontinuous.** A uniformly mis-scaled stretch contains no step at all and
  would otherwise sail through as fact — the MNST shape exactly. On the real
  fixture: 27 sessions quarantined, only the clean post-split window kept.

### MNST / MRNA re-derived (order 2.4), hand-verified

Vendor responses captured as committed fixtures so these stay reproducible after
Yahoo repairs or further mangles its own series.

| | declared | anomalies | quarantined | facts kept |
|---|---|---:|---:|---|
| **AAPL** (control, clean 4:1) | 1 split 2020-08-31 | **0** | 0 | all 14 |
| **MNST** | 1 split 2026-08-11 | 7, all `vendor_corruption` | 27 | 10 (2026-08-12 →) |
| **MRNA** | none | 1, `unknown` | 2 | 12 |

MRNA's +177% session is flagged `unknown` and quarantined — **not** relabelled a
split. Inventing a corporate action the vendor never declared would be the same
class of fabrication as caching a view as a fact. 5-row hand checks for all
three are in `tests/test_prices_reconstruct.py`.

### NOT delivered, and why

* **`reference_series` has no writer.** FRED and Yahoo reference adapters are
  not built, because the reference-series ruling is still open. The table takes
  either answer (nullable `contract`, `roll_flag`), so this is a writer to add,
  not a migration. **This is the one Phase 2 sub-item outstanding.**
* Nothing deployed, nothing run on Basilic, no plist, no commit. The stop point
  is before the first `refetch` rotation runs there.

### Open

* **Reference-series ruling** (§AD.2). Blocks only the reference writer.
* **"Moore rule"** — the order's 2.4 says "hand-verified (5-row check, Moore
  rule)". `grep -rniE moore` across the monorepo returns only a politician's
  surname in SM scorecards; there is no such rule on disk. The 5-row checks are
  done; whatever "Moore rule" adds, I could not apply what I could not find.
* **A3 rule 3** (ordinal fallback) is built and exercised, still wants Abelard's
  confirm.
* `pyproject.toml` is unchanged — the `abelard-prices` console entry point is
  not declared yet. Deliberate: packaging belongs with the Phase 4 deploy.

---

## Phase 2H — Pre-deploy hardening · CLOSED 2026-09-02 · awaiting Mando disk-review

Abelard's seven items, 2026-09-02. Items 1-4 (before deploy) and 7 (the WTI
ruling) are built; 5 and 6 are scoped below with what verification found.

**New modules:** `prices/calendar.py`, `prices/reconcile.py`, `prices/reference.py`.
**Schema -> v2:** `corrections`, `index_weights`, `reconciliation`.
**Tests: 32 new, 188 total, all passing.**

### 1. Index-level reconciliation — BUILT

`reconcile.py` rebuilds the cap-weighted index return from IVV's own published
weights × our adjusted closes and compares it to the ETF's actual return, ~10 bp
tolerance. Weights come free — they were already in the holdings file we parse
for membership, and are now captured into `index_weights` (504 rows, summing to
99.88%).

Two design points worth recording. The rebuilt return is **renormalised over the
members we actually hold**, with coverage reported separately, so "did our names
move as the index did" and "do we hold the whole index" stay separate questions.
And below 80% weight coverage the check reports **`insufficient`, never `pass`** —
an empty panel must not read as a clean bill of health (E1).

**Live result, 2026-09-01 vs 2026-08-31: PASS at −2.6 bp against IVV and −3.8 bp
against SPY**, 502 of 504 members, 99.7% of index weight.

Getting there took two corrections, both found by running it rather than by
reading it:

* **Weights were stamped with the sync date, not the fund's.** The holdings file
  carries its own "Fund Holdings as of"; weights describe the fund on *that*
  date, and a reconciliation of a past session needs the weights in force then.
  Stamping them "today" made every historical reconciliation report
  `INSUFFICIENT`. Compounding it, the file writes `"Aug 31, 2026"` — abbreviated
  — and the date parser only knew full month names, so it failed silently and
  fell back to the sync date. Two bugs chained into one wrong answer that looked
  like a coverage problem.
* **The comparison was not like-for-like.** The rebuild ran on `adjusted_view`
  (a TOTAL return) against an ETF PRICE return. On 2026-09-01 **17 S&P names went
  ex-dividend** and the wedge was **12.8 bp — larger than the whole 10 bp
  tolerance**. The rebuild now runs on `prices_raw` with any split inside the
  window divided back out. Same session, like-for-like: **−2.6 bp**.

A third thing the check caught on its own, which is the best evidence it works:
**2026-08-28 came back `vendor_null` for 464 of 517 names**. The reconciliation
refused to produce a number and reported `INSUFFICIENT` rather than passing on a
hollow panel — exactly the systemic-failure shape it exists for.

### 2. Exchange calendar — BUILT, and the DST concern was right

`calendar.py`: NYSE holidays computed from observed rules (fixed horizon, fails
loud past it), sessions-not-days arithmetic, and exchange-timezone dating.

**The DST hazard is real and I had it wrong.** Measured against the live
endpoint: equity bars are stamped 13:30/14:30 UTC, but **`CL=F` is stamped
04:00/05:00 UTC — midnight exchange-local**. Dating in UTC (as the first
implementation did) lands on the right date only because New York is behind UTC;
any venue ahead of it would be off by one on every bar, silently. Now converted
in `meta.exchangeTimezoneName`.

A second trap found while testing: **`meta.gmtoffset` is unusable for this.** It
reports the offset in force *now*, not per bar — fetching November 2021 in
September returns `gmtoffset=-14400` (EDT) for bars that traded in EST. The
timezone name plus `zoneinfo` handles the transition; the offset does not. Both
are pinned.

Freshness now counts sessions: over Thanksgiving a current name reads 2 sessions
behind rather than 5 days stale.

### 3. Human correction path — BUILT

`corrections`: append-only, Mando-authored, carries a reason, references the row.
`held_raw_closes` consults it, so an authorised correction **releases the fact
gate**; `prices_raw` is never touched, so what the vendor originally said stays
on the record. A correction can also rescue a quarantined session — adjudicating
what the detector could not is exactly what the human path is for.

### 4. Survivorship — BUILT, and the source is not where the order said

The S&P page's changes table has **moved to its own article**,
`Historical components of the S&P 500` (the same restructuring that split the NDX
pages). The main list page has exactly two tables — constituents and a navbox.

Parsed: 407 changes back to 1976, and it corroborates CR-R0 independently
(2026-08-18 RDDT in / AVB out; 2026-08-05 FERG in / EA out — the exact names the
workbook carried as stale).

**The gap quantified: 104 distinct names left the S&P 500 since 2021-01-04.**
Backfilling current members only would have run every 2021-2025 handoff test on
503 survivors while missing roughly a sixth of the period's universe. Departed
names now get an instrument row, aliases, `present=1` as of the floor and
`present=0` at their removal date — 204 as-of rows over 103 names. They drop out
of the nightly automatically (`_targets` reads the latest as-of row) while their
history stays queryable.

### 7. WTI — BUILT to the ruling

`reference.py`: Yahoo `CL=F` daily as the working series with a roll flag from
`meta.shortName`; `^VIX`, SPY, IVV, RSP, XLE alongside; FRED `VIXCLS` /
`DCOILWTICO` as validators. Weekly reconciliation compares them where sessions
overlap, **exempting roll days** (the front month legitimately steps; spot does
not) and attributing any other divergence to the Yahoo series — FRED is slow, not
wrong. The `|return| > 4%` component is **absent by design**, with a test
asserting no return-based roll threshold exists. First live run: 138 Yahoo rows,
19,495 FRED rows, **0 divergences**.

### A third real bug, found by running it

**An in-progress session is not a fact.** A run interrupted mid-universe stored
2026-09-02 intraday prices; the re-run the same afternoon fired `fact_change` on
**~240 names**. The prices were not wrong — the session had not finished
happening. Insert-only and a live session are simply incompatible.

`is_final_session()` now gates every bar: a session is committed only once it has
closed and settled (17:00 exchange-local). The 21:00 nightly still commits the
day it runs; any earlier run stops at yesterday. Re-run clean: **0 fact changes.**

This is the kind of defect that only appears when the thing is actually run, and
it would have produced a nightly alert storm on day one at Basilic.

### 5. Second vendor (Tiingo) — VERIFICATION BLOCKED, not built

`api.tiingo.com` is reachable (HTTP 200 on the unauthenticated test endpoint) and
the docs page returns 200, **but the limits are rendered client-side and are not
scrapeable** — I could not verify the current free-tier ceiling, which is what
the item asks for. Verifying it requires an account key, and **creating accounts
is outside what I do**. Mando registers; I then verify limits and wire the
adapter. The rotation sweep is designed to take a second opinion when it exists —
`reconcile`'s structure and `vendor_adjusted` already anticipate it.

### 6. SEC split corroboration — SCOPED, not built

Deliberately deferred as "soon after". The shape is settled: for each declared
split, read `dei:EntityCommonStockSharesOutstanding` from the issuer's next 10-Q
and check for a jump of the declared ratio, lagged a quarter. The Capex daemon's
iXBRL parser is the tool and the CIK is already the instrument key, so this is a
join rather than new infrastructure. It is the only **non-vendor** evidence
available for a split, which makes it worth more than its cost.

### Open

* Reference `--since` on a cold start detects **0 rolls** — correct, since a roll
  is a *change* and there is no prior contract on the first run. It arms itself
  after one nightly. Worth knowing before someone reads the first log as a bug.
* Observed sustained throughput on the full 519-name backfill is materially
  slower than the isolated-request timing in P0.2 (~0.9 s). Real number to be
  recorded from the completed run before the Basilic slot is finalised.
* `pyproject.toml` still declares no console entry point; packaging stays with
  the Phase 4 deploy.

### Measured throughput (supersedes the P0.2 estimate)

Full 519-name backfill over a 21-session window: **518 requests in 263 s ≈ 0.51 s
per name**, faster than P0.2's isolated-request timing of ~0.9 s. Outcome:
515 ok, 2 quarantined, 1 `vendor_error` (`HOLX`, 404 — delisted), 1 no-rows,
**0 fact changes**. A 5-year window costs more per request; the 21-session figure
is the nightly-append shape, and it puts the v1 nightly comfortably inside its
21:00 slot.

One more fix this run forced: a `404` from the vendor raised `NotFound`, which
was outside the caught set and **aborted the entire 519-name run** on the first
delisted ticker. Now `VendorUnknownSymbol`, counted per name and surfaced in
`status` — the same lesson the old `price_backfill` had already learned and that
I had not carried across.

---

## Item 5 — second vendor (Tiingo) · VERIFIED 2026-09-02 · adapter not yet built

Mando registered and wrote the token to `/home/wafflehouse/.openclaw/prices/.env`
on **WSL Ubuntu (Orban)**, mode 600, single key `TIINGO_API_TOKEN`. The value was
never printed and never left that machine; every probe below ran inside WSL.

**Auth, verified two ways.** Both `?token=` and `Authorization: Token <t>` are
accepted (a bogus token returns `"Invalid token."` on each, a missing one
`"Please supply a token"`). Use the header: a token in a query string reaches
logs, and while `http_client.redact_url` scrubs it, a header never gets there.

**Limits are NOT machine-readable.** `/account/usage`, `/api/usage`,
`/tiingo/utilities/usage`, `/account/limits` are all 301/404, and a normal call
returns **no rate-limit headers at all**. So the free-tier ceiling can only be
read from the account web page — or measured by deliberately exhausting it, which
is not a thing to do on day one. **Mando to read it off the account page.** The
number that matters is unique symbols per month: the v1 rotation touches ~519
distinct names over a 30-day cycle, which sits right on the historical free-tier
ceiling. If it does not fit, the rotation takes a subset and the sweep lengthens.

### MNST: the corruption is Yahoo's, proven against an independent vendor

This is what a second vendor is for, and it paid immediately.

```
date        tiingo    yahoo    ratio   verdict
2026-07-17    97.50    97.50  1.0000   match
2026-07-20    95.45    47.72  2.0002   YAHOO HALVED
2026-07-21    94.46    47.23  2.0000   YAHOO HALVED
2026-07-22    95.67    47.83  2.0002   YAHOO HALVED
2026-07-23    93.56    93.56  1.0000   match
2026-07-31    96.38    48.19  2.0000   YAHOO HALVED
2026-08-06    94.16    47.08  2.0000   YAHOO HALVED
2026-08-11    45.53    45.53  1.0000   match  (split effective)
```

Five disagreements in the compared window, plus 2026-08-10 (Tiingo 91.43 vs
Yahoo 45.72) from the wider fixture — **six, and every one an exact factor of
2.0000**. Tiingo's series is smooth in both raw and adjusted terms and carries
`splitFactor = 2.0` on 2026-08-11, the same effective date Yahoo declares.

So: Yahoo declared its 2:1 correctly and then applied it to six pre-split
sessions and not the other fifteen. The detector was right, the quarantine was
right, and **the decision not to repair was right** — there was no way to know
from inside one vendor which side of each flip was true. Now there is.

### P0.1 is superseded on one point

P0.1 concluded "there is no raw close, and no parameter selects one", and A2 was
built on deriving raw by un-splitting Yahoo's adjusted close. **Tiingo returns
raw `close` directly**, alongside `adjClose`, `adjOpen/High/Low`, `adjVolume`,
and **per-row `splitFactor` and `divCash`** — i.e. prices and the corporate-action
feed in one response, with no reconstruction step to get wrong.

That was true of Yahoo only after a derivation; it is true of Tiingo natively.
The finding stands for the vendors P0 could reach at the time, and is corrected
here rather than edited in place.

**This raises a question above my pay grade and I am not deciding it:** Tiingo
now looks like a candidate for *primary*, not merely verifier. Against that —
Yahoo needs no key, has no quota, and the whole reconstruction path is built and
tested against it; Tiingo's ceiling is unknown and a personal license is a
single point of failure. My read is Yahoo stays primary and Tiingo verifies,
because a free unmetered source is the right thing to depend on nightly and the
metered one is the right thing to check it with. **Abelard's call.**

### Immediate consequence: MNST becomes repairable

Item 3 (human corrections) and item 5 (second vendor) compose. The six sessions
now have an independently sourced true value, so they can be entered as
`corrections` rows — Mando-authored, reason recorded, evidence citable — and
MNST comes out of quarantine without `prices_raw` being touched. That is exactly
the exit the correction path was built for. **Not done: it needs Mando's
authorship, since a correction is by construction a human act.**

---

## Ruling — Mando, 2026-09-02

**Yahoo stays primary; Tiingo verifies.** Recorded here rather than left in chat.
The reasoning that led to it: a free, unmetered source is the right thing to
depend on nightly, and the metered one is the right thing to check it with.
Tiingo's raw-close shape is better, but a personal license is a single point of
failure and its ceiling is still unread.

## MNST / MRNA adjudicated · corrections applied 2026-09-02

New module `prices/corrections.py`, CLI verb `abelard-prices correct <file>`
(dry-run by default, `--apply` to write), staged artifact at
`abelard_common/corrections/2026-09-02_mnst_mrna.json`. **196 tests passing.**

Two kinds of row, and the distinction is load-bearing:

* **`corrected`** — the held value is wrong; a named source says what is right.
* **`confirmed`** — the held value is RIGHT and an independent source was
  checked. Quarantine is a statement of *ignorance*, not of error: the detector
  could not adjudicate the window, so it refused to call any of it fact. Most
  sessions inside a quarantined span are fine, and a confirmation is how they
  are released — with the record showing they were adjudicated rather than
  merely aged out.

```
ticker  date               held    becomes    kind
MNST    2026-08-03       187.10      93.55    corrected   Yahoo left it unadjusted; reconstruction doubled it
MNST    2026-08-04       188.36      94.18    corrected
MNST    2026-08-05       188.92      94.46    corrected
MNST    2026-08-06        94.16      94.16    confirmed   Yahoo HAD applied the split here
MNST    2026-08-07       180.72      90.36    corrected
MNST    2026-08-10       (null)      91.43    corrected   Yahoo returned no price at all; Tiingo has it
MNST    2026-08-11        45.53      45.53    confirmed   the 2:1 effective date
MRNA    2026-08-18        62.96      62.96    confirmed
MRNA    2026-08-19       174.38     174.38    confirmed   REAL EVENT, not corruption
```

**MNST**: `adjusted_view` 14 -> 21 sessions, worst single-day move now
**-4.04%**. The oscillation is gone. `prices_raw` is byte-for-byte unchanged —
every original value and status still on the record.

**MRNA**: the detector flagged the +177% session `unknown` and refused to invent
a split to explain it. Tiingo settles it: same close to the cent, `splitFactor
1.0`, and **189,338,177 shares against 4,304,996 the prior session — a 44x
volume spike**. A real market event. Two vendors and the tape agree, so the
adjudication is *confirm*, not correct. **Note for CR-1: MRNA's adjusted series
legitimately contains a +177% session**, and it will dominate any vol or
correlation statistic for that name. That is data, not a defect.

### A bug the dry run caught before anything was written

The first plan flagged all four confirmations as value changes. Cause: I
compared cross-vendor values at `reconstruct.FACT_EPS` (1e-9). That constant
compares a value against **itself** across two fetches of the same vendor, where
equality is exact. Across vendors it is wrong: Yahoo serves float32-precision
closes widened to float64 (`94.16000366210938`) while Tiingo quotes to the cent.
Now 1e-6 relative — a hundredth of a cent on a $100 share — with the reason in
the docstring so the two tolerances are not later "unified".

This is exactly what dry-run-and-diff (E9) is for: the error surfaced in a plan
nobody had to undo.

### Standing

The correction path is now proven end to end: a human authors a file, the plan
is read, `--apply` writes an append-only overlay, and the view is rebuilt while
the facts stay put. `authored_by` is `mando`; the authorisation is quoted in the
artifact.

---

## Phase 3 — analytics.py · CLOSED 2026-09-02 · awaiting Mando disk-review

`prices/analytics.py` + `tests/test_prices_analytics.py`. **31 new tests, 227
total, all passing.** Pure functions; the single I/O boundary is `load_panel`,
kept at the top and marked, so every statistic can be reproduced from a literal
dict in a test rather than from a store that has to be built first.

**Stdlib only.** `abelard_common` declares one runtime dependency (`requests`),
and a shared library should not grow numpy so one consumer can compute a
five-point regression. Everything here is small by construction. CR-1 can reach
for numpy on the 500x500 matrices; these are the primitives underneath.

### The MA ladder, verified before it was written

Mando's formula was checked against both pins BEFORE a line of it was
implemented, so the module was written against a confirmed target rather than
retro-fitted to make a test pass:

```
FBRX  0, 20.2%, 69.2%, 126.6%, 136.9%  ->  slope x 10 = 15.208  (pinned 15.2)
MRNA  0, 22.8%, 49.7%, 62.3%, 190.4%   ->  slope x 10 = 16.812  (pinned 16.8)
```

Both tests assert from the **ladder values, not from prices**, per the order.
That keeps the scoring formula pinned independently of the moving-average
construction: if a later change to how MAs are computed breaks a ladder, the
ladder test fails and the score test does not, and the failure says which half
moved. A third test asserts the two pins are not accidentally equal — a formula
returning a constant would satisfy either one alone.

`momentum_ma_ladder` returns the ladder **and** the score, deliberately. The
score alone hides shape: a steady climb and a single terminal spike can produce
the same slope, and MRNA's real ladder — flat through MA30, then +190% at the
last rung — is exactly the latter. There is a test pinning that two utterly
different shapes score within 1.0 of each other, so nobody later "simplifies"
the return value to a scalar.

### Scale calibration, from real 5y series

Run against the six names backfilled to 2021:

```
name    MA200   MA100    MA50    MA30    Last   score   63skip5
ABT     +0.0%   -8.9%   -2.3%   +3.6%   +3.9%    0.81   +34.95%
AAPL    +0.0%   +6.6%  +10.8%  +11.6%  +14.9%    1.40    +0.92%
ACGL    +0.0%   +0.8%   +4.0%   +4.1%   +2.8%    0.36    +5.44%
A       +0.0%   -0.0%   +7.6%  +11.9%  +14.6%    1.64   +34.75%
ABBV    +0.0%   +2.8%  +12.2%  +13.4%  +16.5%    1.75   +25.60%
ABNB    +0.0%   +7.3%  +15.8%  +22.8%  +31.0%    3.10   +43.58%
```

**Ordinary large caps score 0.3 to 3.1**; the pinned FBRX and MRNA sit at 15.2
and 16.8. So a score in the teens is not a strong name, it is an extreme one —
worth knowing before anyone reads 1.75 as weak. ABT is the useful case: its
ladder is non-monotonic (down at MA100, up thereafter) and still scores
positive, which is the shape-versus-score point above, in live data.

### Constraints encoded, not just noted

* **Momentum is descriptive.** Handoff §2-E and §3.3: a basket selected on
  momentum is just a momentum portfolio, and the prototype's name-level version
  lost three straight months when the leverage cycle turned. Nothing in this
  module filters, ranks-and-cuts or selects on a momentum value, and the module
  docstring says so as a constraint on callers, not as a caveat.
* **Leave-one-out is the primary basket.** A name inside its own benchmark
  manufactures agreement in proportion to its own weight — with 20 members, a
  floor of about 1/20 of its own variance, which looks like signal and is
  arithmetic. `ew_basket_returns(leave_out=...)` and `loo_basket_for_each`.
* **Composition is disclosed** (E14). `basket_composition` reports how many
  members actually contributed to each session, because an equal-weight average
  over a varying membership is not comparable across time unless you can see
  the membership.
* **`aligned_returns` returns the dates alongside the series.** Intersecting is
  the honest default for a correlation input and also how a panel silently
  dates itself to its stalest member — CR-R0 §R1.5, where intersecting 497
  names truncated the window to 2026-07-23 with nothing on screen saying so.
  There is a test for the pathological case where two names do not overlap at
  all and the caller gets an empty date list rather than a plausible number.
* **Missing data returns None, never a number.** 199 sessions is not a 200-day
  average. A partial mean labelled MA200 is the exact species of quiet
  wrongness this substrate exists against.

### Open

* MRNA's adjusted series legitimately contains the +177% session (two vendors,
  volume 44x). Any Phase 3 statistic touching MRNA will be dominated by it.
  Real data, flagged so a correct number is not mistaken for a defect.
* `analytics.py` is Phase 3 and postdates the four-commit plan; it wants its own
  commit rather than being folded into one of them.

---

# ORDER PS-1B — received 2026-09-02

**Not started.** The order's prerequisite is the five PS-1 commits reviewed and
authorized; G4 is marked "needed by: all". HEAD is `eb4eac1`, index empty,
nothing committed. No PS-1B phase has begun, including 3.1.

## Gate register

| Gate | Needed by | Status 2026-09-02 |
|---|---|---|
| G1 Tiingo unique-symbols/month ceiling | 2V | **PENDING (Mando).** Not machine-readable: `/account/usage`, `/api/usage`, `/tiingo/utilities/usage`, `/account/limits` all 301/404, and a normal call returns no rate-limit headers. Account page only. |
| G2 Tiingo token on Basilic, mode 600, by Mando's hand | Deploy | **PENDING (Mando).** Token currently exists only at `/home/wafflehouse/.openclaw/prices/.env` on WSL Ubuntu (Orban). |
| G3 Hole-fill policy | 2V | **PENDING (Mando confirm/override).** Abelard default: a `vendor_null` is a first-write and may be filled from a sourced vendor automatically with attribution; a HELD value changes only by human correction; the two paths never share code. |
| G4 Five PS-1 commits authorized | all | **PENDING (Mando).** Five messages prepared and PowerShell-verified; 24 files; nothing staged. |

## Phase 3.1's premise, verified before the gate (read-only; no build)

Abelard reports a defect in `analytics.py`, which is code committed to this
branch's plan and about to be authorized. It is real, and it is not marginal.

`dated_log_returns` computes `log(c[d_i] / c[d_{i-1}])` over adjacent **rows**.
Where a session is missing — `vendor_null`, quarantined, or excluded from
`adjusted_view` — the two adjacent rows are not adjacent sessions, and a
multi-session return is emitted keyed to a single date. `ew_basket_returns` then
averages it against genuine single-session returns; `basket_composition` counts
it as one member-session; `moving_average` takes the last N **rows**, so a
window with k holes spans N+k sessions and is still labelled MA200.

Measured against the live store:

```
names with >=1 hole inside their own span : 464 of 517
MNST 2026-08-27 -> 2026-08-31 spans 2 SESSIONS,
     emitted as one return of -1.67%
basket composition on 2026-08-31 claims 3 members,
     one of which is a 2-session return
```

**464 of 517 names, right now.** Every one is the 2026-08-28 mass vendor-null,
so the defect and 2V.5's remediation target are the same event seen from two
ends: refilling 08-28 removes ~463 of the 464 holes. 3.1 is still required —
holes recur, and quarantine creates them by design — but the present count is
dominated by one remediable outage, which matters for sequencing: doing 2V.5
first would make 3.1's test data thin, and doing 3.1 first gives 2V.5 a correct
yardstick.

**Consequence for the review that gates this order:** commit 5 (`analytics.py`)
carries a known defect. That is a normal thing for history to record and 3.1
fixes it — but Mando should decide knowingly rather than discover it after.
Either order is defensible; it is his call, not mine to assume.

---

## PS-1B gates — resolved 2026-09-02

**G4 AUTHORIZED.** Five commits landed on `ps-1-price-substrate`:
`563ad24` schema, `594fcf6` writer, `075fa99` hardening, `7802e67` corrections,
`2af8a97` analytics. Working tree clean. Not pushed.

**G3 CONFIRMED** as Abelard's default. A `vendor_null` session is a FIRST WRITE
and may be filled from a sourced vendor automatically, with `source` attribution
and evidence. A HELD value changes only by human correction. The two paths never
share code.

**G2 DONE.** Token placed on Basilic at `~/.openclaw/prices/.env`, mode 600,
`~/.openclaw/prices/logs/` created. Transferred WSL -> Basilic through a pipe so
the value never entered an argv, a log, or shell history (verified: 0 matches in
`~/.bash_history`). Confirmed authenticating from Basilic itself, not merely
present: `api/test` 200 and a real MNST fetch 200.

**G1 READ — and the ceiling is NOT what the order assumed.**

There is no unique-symbols-per-month cap. The free tier meters three things:

```
Hourly Requests      50 requests/hour      <- the binding constraint
Daily Requests    1,000 requests/day
Bandwidth             2.00 GB/month
```

So the rotation sizing changes shape. N = 519/30 = **18 names/night** sits far
inside all three: 18 of 50 hourly, 18 of 1,000 daily, and at roughly a quarter
megabyte per full-history response about 135 MB/month against a 2 GB allowance.

The constraint the code must enforce is therefore **requests per hour**, not
unique symbols. 2V.1 says "refuse to start a rotation that would exceed the
ceiling" — that check should count requests in a rolling hour and a rolling day,
and track month-to-date bytes, because those are the meters that exist. A
unique-symbol counter would guard a limit Tiingo does not impose while leaving
the one it does impose unguarded.

Headroom is large enough that the full 519-name universe could be swept in a
single night across ~11 hours of pacing if that were ever wanted. It is not
wanted — a 30-day rotation is the design — but it means the ceiling is not a
constraint on this build.

### Security event — token exposed in transcript, rotation recommended

Reading the usage page required loading `/account/api/token` first, and **that
page renders the token in plaintext**, so it entered the session transcript. It
had until then been handled without ever being printed: read in place on WSL,
piped to Basilic, verified only by length and prefix.

The exposure is not from the transfer; it is from the account page itself. The
right response is rotation, which is a one-click action on that page and which
**only Mando should perform** — it is a security action on his account.

Rotation invalidates the copy now on Basilic, so the sequence is: rotate, then
re-place. Re-placing is mechanical and can be done the same piped way.

---

## PS-1B item 1 — token re-placed 2026-09-02

New token piped WSL -> Basilic, `~/.openclaw/prices/.env`, mode 600. Verified:
new token `api/test` **200 from Basilic**; the value differs from the one it
replaced; the old prefix returns **0 matches** in either host's `.env`.

**Two findings that go beyond the check requested.**

1. **Rotation did not revoke.** Tiingo's control states it will "create a new
   token and immediately invalidate the current one". Tested from a clean host
   minutes after Mando rotated: the new token returns 200 **and so does the
   old** one. The exposed credential is still live. This is E33's corollary in
   the first instance that produced it — rotation is not proof of revocation,
   and the only proof is a 401/403 from the endpoint. **Mando to re-check, and
   escalate to Tiingo if it stays live.**
2. **The old value is in WSL's `~/.bash_history`.** Basilic's history was clean
   (checked at placement); Orban's WSL history was not checked then and holds
   it. Given (1), that file currently contains a working credential. Not
   scrubbed — a shell history is personal and deleting from it is Mando's call.
   The line is removable with
   `grep -v '<prefix>' ~/.bash_history > /tmp/h && mv /tmp/h ~/.bash_history`.

## PS-1B item 2 — 2V.1 amended (recorded, not yet built)

Mando's amendment, verbatim in effect: replace the unique-symbol counter with
three rolling counters in telemetry — **requests/hour (50), requests/day
(1,000), bytes/month (2 GB)** — and refuse a rotation that would breach any.
**Hard floor: pace at >= 72 s between requests inside the sweep**, so a
pathological retry loop cannot reach 50/hour. N stays 18/night; the headroom is
real but the 30-day rotation is the design and does not change.

72 s x 18 names = 21.6 minutes per sweep, which sits inside the 21:00 slot with
the nightly append ahead of it. Built in 2V, not here.

## Phase 3.1 — analytics session-awareness · CLOSED 2026-09-02

**227 tests passing.** `analytics.py` 336 -> 461 lines; every function that
spans time now takes `sessions` from `calendar.py`.

* `dated_log_returns(closes, sessions)` returns **`(returns, gaps)`**. A return
  is emitted only between consecutive sessions; a bridged span is appended to
  `gaps` and never produced. Callers wanting only returns write `[0]`, and the
  explicitness is the point.
* `moving_average(closes, window, sessions, as_of)` and
  `ma_ladder(closes, sessions, as_of)` refuse unless every one of the last
  `window` **sessions** is held. A 200-row mean over a holed series spans 200+k
  sessions and is a longer average wearing a shorter label.
* `ladder_status()` reports which rungs failed and why.
* `ew_basket_returns` / `basket_composition` / `loo_basket_for_each` count only
  consecutive-session returns.
* `momentum_return_63_skip_5` was not in the order's list but carries the same
  defect and is fixed with it: both endpoints must be held sessions, because
  sliding to the nearest held row silently changes the window being measured.

**Measured against the live store, before and after:**

```
before : 464 of 517 names silently bridged a hole (all the 2026-08-28 outage)
after  : 464 gaps COUNTED and returned; 0 bridged
MNST   : gap ('2026-08-27','2026-08-31') reported; 19 returns emitted, was 20
basket : composition on 2026-08-31 is now empty, not "3 members" with one bridged
```

### A design flaw the tests caught

First `ladder_status` returned at the first failing rung. But the windows nest —
MA30's sessions are a subset of MA200's — so a hole near the right edge fails
every rung, and an early return always reported `MA200` and never revealed how
recent the hole was. That is the diagnostic that matters: it distinguishes "wait
for history" from "fill a hole". Now every rung is tested and the **shortest**
failing window is reported.

### The order's MRNA requirement could not be met as written, and why

The order requires MRNA's real series to "reproduce 16.8 via the
ladder-from-prices path". It does not, and the reason is worth more than the
test would have been.

Computed from real adjusted closes, MRNA's ladder as of **2026-08-31** is
`[0.0, 22.8%, 49.7%, 62.3%, 168.2%]`. The first four rungs match Mando's pinned
ladder **exactly** — three independent values to a tenth of a percent, which is
not coincidence and which dates the pin to 2026-08-31. The fifth does not:
pinned 190.4% against computed 168.2%.

That gap resolves cleanly. His Last rung implies a price of **151.97**; the
2026-08-31 close is **140.34**, the 2026-09-01 close is **154.27** and 09-02 is
**150.81**. So the pinned ladder pairs **moving averages computed to the prior
close with a live intraday Last** — exactly what reading the number off a screen
produces, and not reproducible from closes.

The score is also violently as-of sensitive because the last rung dominates:
across three weeks of real MRNA data it ranges **2.65 to 21.96**. Pinning
16.8-from-prices would have produced a test that failed on most days and passed
by luck on a few.

So the pin is split, which is stronger than the order asked for:

* **16.8 stays pinned on the LADDER** — date-independent, formula-only.
* **The prices->ladder path is pinned against a frozen fixture**
  (`fixtures/mrna_ladder_20260831.json`, 314 real sessions) asserting the three
  MA rungs reproduce exactly and the Last rung is the close. Deterministic, and
  it exercises the full real path.

**Decision embedded, worth Mando's eye:** `ma_ladder` uses the **close** at
`as_of`. A stored, versioned system must, or the same as-of yields a different
number on every recomputation. The cost is that a live dashboard reading will
differ from this intraday — here by 7.7% on the last rung, worth about 1.8
points of score. If Mando wants the screen number reproduced, that is a
`Last = live quote` variant and a separate, explicitly non-reproducible output.

## Doctrine — E33 added to ENGINEERING.md

"A page that renders a secret is a Mando-only surface." Full entry in
`doctrine/ENGINEERING.md`, with corollaries on rotation-is-not-revocation and on
gates naming their reader.

**Still unresolved: the "Moore rule".** Order PS-1 §2.4 requires hand
verification "(5-row check, Moore rule)" and Mando has now asked that E33 be
added "with the Moore-rule entry". `grep -rniE moore` across the monorepo
returns only a politician's surname in SM scorecards. There is no such rule on
disk and it has not been stated in session. The 5-row checks are done; the Moore
rule cannot be written by someone who has never been told it.
