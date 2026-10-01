# Burn-down item 3 — the materialization unit, measured (2026-09-30)

**Orders:** `TRIAL_BURNDOWN_ORDERS.md` §3, as amended by `RULINGS_2026-09-30.md`.
**Spent:** 3a 97.1 credits (budget 250) · 3b 16.0 credits (budget 100) · **item 3 total 113.1 of 350.**
**Evidence:** `recon/out/burn_3a.json`, `recon/out/burn_3b.json`, `recon/out/pt_features_w_2025-06-09_tierA.json` (the week's 1,573 rows).
**State after:** both views deleted (GET returns 404), both saved queries archived, stored bytes 0.

---

## 3a — per-token table, one week of P

Week: graduations **2025-06-09 → 06-15** (inside the calibration slice, pre-BOOST, not DEGRADED). **1,573 tokens.**

| Measurement | Result |
|---|---|
| Validation run, one graduation day (241 tokens), summary output | 27.4 credits |
| **(i) Build**, the week | **32.4 credits** |
| **(ii) Stored bytes** | **578,658 bytes** = 368 bytes per row |
| **(iii) One full-table read** (`SELECT *`, all 85 columns, 1,573 rows) | **0.028 credits** |
| **(iv) Export**, all 1,573 rows × 85 columns (3.25 MB of JSON) | **3.0 credits** |
| **One refresh** (ordered) | **33.3 credits** — a refresh is a full rebuild |

### What was materialized, stated exactly
The 85-column table was created with every column present, but **60 columns populated and 25 left as typed NULLs.** The 60 are everything computable from swap events and the create / complete / pool events: identity, prices, reserves, peaks, drawdowns, cost legs, markup, organic-v1, and the c09/c10 features. The 25 are the heavy groups: the gate reconstructions (c01–c08, c11, `u_codes`), the `seta_*` and `cluster_*` sets, and organic-v2. They were **not** built here. Building them for the week would have needed the funding and ledger scans, which did not fit inside 250 alongside the ordered refresh. Their cost is priced below from the readiness runs, and labelled as such.

### Three findings from the measurements
1. **Cost is driven by the date range scanned, not by the number of tokens.** One day of graduations (241 tokens, 9 days of events scanned) cost 27.4. A whole week (1,573 tokens, 15 days scanned) cost 32.4. Six and a half times the tokens for 18% more credits.
2. **A refresh costs the same as a build.** M0 is historical: the table is built once and never scheduled. Every view gets an expiry before its first cron firing, as these did.
3. **Export is about 1 credit per megabyte of JSON returned**, rounded per request, on this trial. Five fetches fit that: 0.21 MB → 0 credits; 1.24 MB → 1; 2.06 MB + 1.18 MB → 3; 3.74 MB → 4. **It is not 1 credit per 1,000 datapoints**, the figure the export contract in `DERIVED_TABLE_SCHEMA.md` was budgeted on. That overstated export by roughly 40×. The trial runs on Plus features, so this is the Plus rate. Dune's docs put Analyst at 5× Plus (10 vs 2 credits per MB, or 1,000 vs 5,000 datapoints per credit); **the Analyst rate itself is unmeasured.**

### Extrapolation to the full P window (~180,500 tokens, ~610 days)

| Quantity | Projection | Basis and confidence |
|---|---|---|
| Stored size, 60 columns populated | **66 MB** | 368 bytes/row × 180,543. Linear in rows; solid. |
| Stored size, all 85 populated | **~100–130 MB** | Assumes the 25 heavy columns add at most as much again. Three are arrays. Under 1 GB either way. |
| One full read | under 5 credits | Linear from 0.028 per 1,573 rows; solid. |
| Full export, Plus rate | **~375 credits** | 3.25 MB per 1,573 rows → 373 MB. |
| Full export, Analyst rate | **~375–1,900 credits** | Low end if Analyst bills like the trial; high end at the documented 5×. **Unmeasured.** Exporting only the columns the local analysis needs cuts this proportionally. |
| **Build, 60 event-derived columns, one pass** | **~400–550 credits** | Two-point fit: cost ≈ 20 + 0.83 × days of events scanned, scaled by the window's average volume (14.3M events/day vs 20M in this week). **Thin: two points.** A single 610-day pass may also exceed the medium engine's limits. |
| Same build, in monthly chunks | **~1,000 credits** | 19 months × (20 + 0.83 × ~38 days). The safer plan. |
| Same build, in weekly chunks | ~2,800 credits | 87 weeks × 32. The expensive way; shown so nobody picks it by accident. |

### The 25 heavy columns, priced from readiness runs (not re-measured here)
These are sample-based. Each was a bounded scan measured in §1 readiness, and each is driven by the days scanned.

| Column group | Readiness measurement | Window projection |
|---|---|---|
| c07, `seta_*`, c08, `cluster_*` (wallet funding, `sol_transfers`) | 23.4 credits per 7 days scanned | **~2,000 credits** for 610 days, one scan serving all four |
| c06 and the holder ledger (`tokens_solana.transfers`) | 7.6 credits per 27 days, filtered to mints | ~170–400 credits |
| c01–c04 (authority and extension history) | 0.4–1.4 credits per sample | under 100 credits |
| org2 (wallet age, 60-day lookback) | 43.6 credits per week of graduates | largely absorbed by the event scan in a single pass; **unverified** |
| c07b (fee-share recipients, raw fee-program calls) | 11.6 credits per day scanned | **~7,000 credits.** By far the largest line. |

**Heavy groups without c07b: ~2,300–2,500 credits. With c07b: ~9,500.** c07b is the one column whose cost exceeds everything else combined.

---

## 3b — per-swap slice, one day of P

Day: **2025-06-10. 4,272,532 swap rows, 10,634 tokens, 285,353 wallets.** Cost identity holds on every row. No rows were written to the repo; the 5,000-row export sample was checked against the owner-wallet list (no match), counted and discarded.

| Measurement | Result |
|---|---|
| Validation run (aggregate count) | 4.0 credits |
| **(i) Build** | **7.5 credits** |
| **(ii) Stored bytes** | **479.8 MB** = 112.3 bytes per row |
| **(iii) One full-table read** (aggregate over all 23 columns) | **0.41 credits** |
| **(iv) Export**, 5,000 rows (3.74 MB) | **4.0 credits** → the whole day would be ~3,200 credits |

### What it settles
* **Analyst (1 GB) holds about two such days.** Confirmed by measurement, not just by the docs.
* **Plus (15 GB) holds about 33 such days**, at June-2025 volume. Post-BOOST days run ~3× the rows, so about 11 days.
* **The cost-trial's 60-day per-swap window does not fit on Plus either:** 60 days at 4–12M rows/day is 27–81 GB. H3 as specified needs per-wallet histories over rolling 60-day windows. **So M0b cannot store per-swap rows on any self-serve plan.** It needs a design that turns bounded queries directly into per-wallet aggregates. Plus does not buy a way around that.
* Building per-swap rows is cheap (7.5 credits/day, ~4,600 for the window). Storing and exporting them is what is impossible.

---

## Method notes and deviations, for the record
* **Item 4 will not read the 3a view.** Order 2 said to delete the view on completion, and item 4 says to run against it; the builder followed the delete, because a view left standing carries a cron. Item 4 rebuilds the identical token set from the same admission logic (1,573 tokens, checked by count).
* The week's rows are in the repo. They contain token mints and deployer addresses, which are public chain data, and no trader wallets.
* The meter misread the balance for one run on 2026-09-30: Dune began returning a placeholder billing period for after the trial, and `dune_usage.py` took the last period in the list. It failed safe (refused to run). Fixed to select the period containing today.

---

## Defect found after the measurements (2026-09-30) — `creator` is empty in the exported week

pump.fun's create events carry the deployer in `creator` only in the newer layout. Probe (`recon/sql/createevent_creator_probe.sql`, 1.1 credits): `creator` is NULL on **all** 28,102 creations of 2025-06-09 and all 13,836 of 2025-10-15, and populated on 2026-09-01; `user` is populated in every era. In 2026 the two differ on 211 of 36,202 creations.

**Effect on item 3:** none on the cost, size or export figures, which measure scans and bytes. **Effect on the exported week** (`pt_features_w_2025-06-09_tierA.json`): the `creator` column is NULL on all 1,573 rows, and `c10` and `org1_*` were computed excluding only the migrator, not the deployer. Those three columns in that file are not to be used. The validation run counted creation *times*, which were present, and never counted creators; that is the check that should have caught it.

**Fix:** both generators now take `COALESCE(creator, "user")`. Rule for the paid build: every column drawn from a decoded event is null-counted per era in the validation run, because decoded tables keep a column that older layouts never filled.
