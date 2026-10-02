# BARREL M0 — Paid-Month Runbook v1 (operational)

**Author:** ClaudeCode · **Date:** 2026-10-01, revised twice the same day · **Returns:** the Architect's launch order (`RUNBOOK_ARCHITECT_2026-10-01.md`, MR-13) with the operational detail filled in.
**Status: NOT READY.** Revised after MR-14 (`RULINGS_2026-10-01_C.md`) and after an adversarial review of that revision. §1 lists what stands between this document and a purchase.
**Plan assumed:** Analyst, month-to-month, 4,000 credits a month, 3,400 spendable after the 15% reserve, two months authorized. Under the Plus rule the same phases run uncut in one month.

---

## 1. Status before purchase

### The six orders of MR-14

| Order | Result |
|---|---|
| 1. Grouped `b1a`, one day, ≤ 60 | **Run, 46.2 credits.** Member and grouped forms agree on 236 of 236 tokens for set size, creator, creator-funded members, holdings at all three lags, net flow and supply at all three lags; on 235 for funders. The one difference is two senders in the same second. **The query has been changed since that run** (below), so the run proves the grouping, not the text now on disk. `B1A_GROUPED_VALIDATION.md` |
| 2. Virtual reserve over RPC | **HALT.** Not one constant: 10 of 31 post-BOOST pools carry 0 (all 10 are mayhem-mode pools), 21 carry about 17.58 SOL. Fixed for a pool's life. A derivation from each pool's own buys matched the accounts on 35 of 35. `VQR_CHECK.md` |
| 3. Threshold grep in the runner | **Done, then hardened.** The first version had holes the review found (constant on the far side of a product, `5e0`, line breaks, ratios against 1, and the saved-query path did not pass through it at all). Now enforced in the one function every request to Dune goes through. Burned-week query refused; 76 scheduled chunk files pass; 19 `b3` files are refused and marked so. |
| 4. Null-count item for the first paid chunk | **Recorded.** `recon/sql/buy_field_swap_by_day.sql`, day 0 step 5. |
| 5. Depth floor against withdrawable reserve | **Done.** `M0_TASKING.md` MR-14; the dry run is struck. |
| 6. "Ready", and the day | **Not ready.** See the three blockers. If the rulings and the authorization below arrive on 2026-10-02, the two proving runs take about an hour and day 0 can be **2026-10-03**. The trial credits that pay for them lapse on 2026-10-06. |

### Blockers (each needs a run, a ruling, or both)

| # | What | Needs |
|---|---|---|
| **B1** | **Post-BOOST prices are halted.** The event query now derives the virtual reserve per pool from its own buys (largest buy of at least 0.001 SOL). Not run. | The Architect's acceptance of the derivation, and one post-BOOST day of the regenerated event query. **≤ 50 trial credits, hard cap. Requested.** |
| **B2** | **`b1a` was changed after its proving run.** The review found that three of its definitions depended on where a token falls in the chunk, which a one-day run cannot show: how far back a funder is looked for (4 days for a first-day graduate, up to 34 for a last-day one), which creations are found, and how long a candidate has to acquire the token. All three are now per token (4 days before the token's graduation day; creation within 3 days; acquisition within 9). Also changed: funder ties broken by slot; funding counted up to and including the slot of the first action instead of strictly before its second; per-member latencies returned so the median is taken across members as ruled; creation-slot trader count returned so S8 can say UNKNOWN. | One post-BOOST day of the regenerated grouped query, compared against the member rows on disk for 2026-09-01. **≤ 70 trial credits, hard cap. Requested.** |
| **B3** | **Fan-out is measured over whatever the query scans.** Six days in the one-day run, thirteen in the week, twelve in the calibration histogram, up to thirty-six in a monthly chunk. The provisional line of 400 therefore means something different at each scope, and a funder can move between `dedicated` and `hub` because of chunk length alone. Every fan-out figure reported so far carries its own window. | **A ruling.** Options: (a) a fixed window per token (the 6 days around its graduation day), built from daily sketches and merged, which adds work to the SOL-transfer aggregation and has not been costed; (b) keep the chunk-wide count and express the line as recipients per day; (c) a separate fan-out query per chunk on a fixed window, costed like the calibration histogram (about 1 credit per day scanned). Not implemented: the choice changes what the classifier means. |

165 trial credits are spendable above the reserve. B1 and B2 together ask for up to 120.

### Rulings needed, no run

| # | Point |
|---|---|
| R1 | **Collapse does not merge a funder with the wallets it funds when that funder is itself in the set.** The aligned set's defining shape, a creator and the wallets it funded, is therefore always at least two actors. Proposed: a member whose address is another member's linking funder is the same actor. Not implemented; it changes ruled collapse semantics. |
| R2 | **`blk` puts one wallet in two blocks** when it both creates tokens (`C:`) and funds creators (`F:`), which overstates block n. Proposed: key the block by address alone. Not implemented. |
| R3 | **Organic-v2 is costed in the Analyst path of `PURCHASE_DECISION.md` (900–1,800) and has no chunk query here.** It is also on H5's marker list. Schedule it (one proving day, then per chunk) or rule it cut on Analyst. |
| R4 | **"No thresholds in Dune" and "analyse in the warehouse" pull against each other.** Thresholds are set and verdicts applied locally, so the columns they read have to leave Dune. About 25 numeric columns for 180,543 tokens is 50–100 MB [estimate]: 150 to 1,000 credits depending on the rate measured on day 0. Phase D's 200 / 300 does not cover that at the documented rate. Proposed: one column-pruned export of the analysis columns, budgeted from the measured rate; the full 91 columns stay in the warehouse. |
| R5 | **Nothing in the repo creates a view from a chunk file.** The runner submits ad hoc only; the view code that exists is the trial's, hard-wired to one file. If day 0 selects in-warehouse, a view mode has to be added to the runner (same cap, reserve, ledger and threshold check; refuses `b1a`) before phase B. Half a day of work; not started. |
| R6 | **Two things in `M0_TASKING.md` MR-14 were mine, not the ruling's:** "deepest pool" read as deepest by real reserve, and the post-BOOST depth-floor distribution taken from the burned week because the calibration slice has no post-BOOST pools. Now marked as proposals there. |
| R7 | Ruling 6 says "one addition for Mando below". **None was in the text relayed.** |
| R8 | **Pushed history contains wallet addresses that should not be in the repo:** the holders of one test token in three S6 files, and one buyer in a column probe. Truncated in the working tree today. Removing them from history means rewriting a pushed branch, which is Mando's call. |
| R9 | The stratum-N queries read a LaunchLab column that may be the token's name/symbol/uri struct rather than its mint. Not confirmed. Stratum N is outside the paid build. Those three queries are not to be run until a `LIMIT 0` column probe settles it; "LaunchLab removals: 0" is struck as unmeasured. |

### Rulings applied (MR-14)

| # | Ruling | Where |
|---|---|---|
| 1, 8 | 19 chunks; window end 2026-09-20 signed | §4 |
| 2 | Phase A 400 / 500 from C; runs the whole 2026-08 chunk including `b1a` | §5 |
| 3 | In-warehouse expected on Analyst; day 0 measures the rate; `b1a` output always exported, never materialized | §5, §7 C3 |
| 4 | Sampled window as proposed | §7 C1 |
| 5 | Definitions may live in a query, verdicts may not; the bundle's five is applied locally; no request with a registered constant reaches Dune | §2 |
| 6 | Rows to gitignored `barrel/data/`; manifest committed the same day | §2 (`--export`) |
| 7 | Grouped `b1a` one-day run | done, above |

---

## 2. Standing rules, and what enforces each

| Rule | Enforced by |
|---|---|
| Expected cost is the hard cap for an unproven pattern | `recon/dune_run_sql.py`: `--expect` is the cap unless `--proven`; above 25 needs `--confirm` |
| New patterns at one-day scope first | every generator writes a one-day file; §4's table is the record of what has run |
| Null-count every input column per era | `recon/sql/*_probe.sql`; day 0 step 5 |
| Cost runs do not fetch rows | `--no-rows` |
| Rows that name wallets stay out of the tracked tree | a query whose header says its rows name wallets is forced to `--private-rows` whatever flags were given; owner wallets are dropped from any fetched row |
| Reserve: 15% of the month's allowance | the runner computes it on the allowance the usage API reports (600 of 4,000) and refuses a run that would cross it. The trial keeps its ruled 180. |
| Daily ledger | `docs/credits.md`, one block per day (§6) |
| No verdict threshold in any query, saved or not | `recon/verdict_constants.py`, called from `dune_roundtrip.dune`, the one function every request passes through (ad hoc, saved query, view definition), and from the runner and the chunk review. No override. A newly ruled threshold is registered in the commit that records the ruling. It is a tripwire: it cannot see a verdict nobody registered, so queries are still read. |
| No bare `SELECT *` | same function: a column probe uses `LIMIT 0` and reads the column names, so no row of metadata or wallets is fetched |
| Exports | `--export`: rows to gitignored `barrel/data/`; SHA-256, row count and per-column null count appended to `recon/export_manifest.json`, which is committed. `b1a` groups are always exported and never materialized. |
| Owner wallets never in a saved query | `__NOT_OWNER(col)__` is substituted at run time from `barrel/private/`; the chunk review fails any wallet-bearing query without it |
| In-flight counter is not a meter | It jumps by up to 56 credits and has read above the final bill. Cancellation bounds slow overruns only; the one-day rule is the protection. |

## 3. Pre-purchase checklist

| Item | State |
|---|---|
| Post-BOOST price-path week, burned, by seed, recorded first | **Run (44.05 of 50); its price results are STRUCK** (`VQR_CHECK.md`). Re-counted from per-token rows after B1. Week 2026-08-10 → 08-16 stays burned. |
| `PURCHASE_DECISION.md` with both paths | Done (third version, amended). Its Analyst path and this plan disagree on organic-v2 (R3). |
| Chunk list, queries generated and reviewed, zero credits | 19 chunks; **76 scheduled files, review problems 0**; 19 `b3` files generated, not scheduled, refused by the runner as written. `recon/chunks_manifest.json`. |
| `RUNBOOK_v1.md` | this document |
| Defects found on the way | `ix_name` NULL on every buy event; virtual reserve not constant; Dune's mayhem flag empty before late August 2026; `b1a` definitions that varied with chunk length; holes in the first threshold check. |

Trial balance: 2,154.9 used; **165.1 spendable** above the 180 reserve, until 2026-10-06.

---

## 4. Chunk list

One calendar month of stratum-P graduations per chunk. Files: `recon/sql/chunks/<chunk>_<query>.sql`.

| Query | Feeds | What has actually run |
|---|---|---|
| `events` | the event-derived columns | An earlier text: one day and two weeks pre-BOOST. **The text on disk (per-pool derived reserve, gross and net by size) has not run.** First run: B1. |
| `b1a` | aligned set grouped by funder: c07, c08, `seta_*`, `seta_lat_s`, `blk`, registry | Member form: day, week, post-BOOST day. Grouped form: one pre-BOOST day (46.2). **The text on disk (per-token windows, slot tie-break, slot cutoff, latency arrays) has not run.** First run: B2. |
| `b1b` | c06, supply at entry | day, week, post-BOOST day. Text changed only by the per-token creation bound and the acquisition horizon it shares with `b1a`. |
| `b2` | c01, c02, `token_program` | day, week, post-BOOST day |
| `b3` | `cluster_*` (H2) | An earlier text ran at day, week and post-BOOST day. **Not scheduled, and not runnable as written:** it keeps a group-size cut that the threshold rule refuses, and it returns raw reserves instead of a price. |

Projected credits per chunk (measured units: a fixed part plus a rate per day of large table scanned, times the post-BOOST ratio for post-BOOST days). The `b1a` and `events` figures were measured on the earlier texts.

| Chunk | Graduations | Days | Era | events | b1a | b1b | Core total | b3 | Note |
|---|---|---|---|---|---|---|---|---|---|
| 2025-03 | 03-20 → 03-31 | 12 | pre | 37 | 95 | 11 | 144 | 81 | calibration |
| 2025-04 | month | 30 | pre | 52 | 178 | 19 | 250 | 151 | calibration |
| 2025-05 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | calibration |
| 2025-06 | month | 30 | pre | 52 | 178 | 19 | 250 | 151 | calibration |
| 2025-07 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | calibration to 07-10 |
| 2025-08 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | DEGRADED 08-05 → 08-11 |
| 2025-09 | month | 30 | pre | 52 | 178 | 19 | 250 | 151 | |
| 2025-10 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | |
| 2025-11 | month | 30 | pre | 52 | 178 | 19 | 250 | 151 | |
| 2025-12 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | |
| 2026-01 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | fee era changes 01-10 |
| 2026-02 | month | 28 | pre | 51 | 169 | 18 | 238 | 143 | |
| 2026-03 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | |
| 2026-04 | month | 30 | pre | 52 | 178 | 19 | 250 | 151 | |
| 2026-05 | month | 31 | pre | 53 | 183 | 20 | 256 | 154 | admission source changes mid-month |
| 2026-06 | month | 30 | pre | 52 | 178 | 19 | 250 | 151 | |
| 2026-07 | month | 31 | both | 59 | 205 | 21 | 285 | 182 | BOOST 07-21 |
| 2026-08 | month | 31 | post | 69 | 245 | 24 | 338 | 233 | burned week 08-10 |
| 2026-09 | 09-01 → 09-20 | 20 | post | 57 | 177 | 18 | 252 | 169 | |
| **Total** | | 550 | | **1,014** | **3,417** | **367** | **4,808** | 2,948 | b2 adds about 10 in all |

**4,808 credits for the core build**, against 6,800 spendable over two Analyst months. If every chunk costs the post-BOOST rate, about 6,200. Neither figure includes getting data out (R4).

What the static review checks on every scheduled file, and found nothing wrong with: each large table carries literal partition bounds; no large table is referenced more often than designed (SOL transfers three times in `b1a`; token ledger once; bonding-curve trades twice in `b1a`; swap events once); wallet-bearing queries carry the owner filter; the chunk's dates are in the text; no `OR` on a partition column; `SELECT` only; no registered verdict constant. It cannot show a query is cheap, and it did not catch the chunk-length dependence that a reviewer reading the definitions did.

---

## 5. Phases

Order of chunks in every phase: **2026-08 first** (phase A), then post-BOOST and mixed (2026-09, 2026-07), then backwards from 2026-06 to 2025-03. The newest eras are the least measured, so their cost is learned first.

### Day 0 — at purchase, before any build query
1. Read at checkout: monthly price, overage policy, export rate. Record in `credits.md`.
2. If overage bills automatically, the runner already stops at the reserve computed on the plan's allowance; confirm the allowance the usage API reports.
3. **Export rate, measured:** one fetch of about 1 MB from a kept trial result. Decides how R4's export is budgeted and whether C3 applies.
4. **Virtual reserve:** repeat the free RPC read on 30 fresh post-BOOST pools and compare with the reserve the event query derived for the same pools in phase A. Any mismatch is C5.
5. **Null-count probes, about 10 credits:** one hour per era for every input column of `events`, `b1a`, `b1b`, `b2`; and `recon/sql/buy_field_swap_by_day.sql` (MR-14 order 4): per day from 2026-08-08 to 09-02, how many buys have each quote field the larger, on stratum-P pools and on all pools, to explain why the burned week and the single sampled days disagree.

### Phase A — scaling check (day 1). Budget 400 / 500, taken from phase C.
| Step | File | Cap |
|---|---|---|
| A0 | `events`, one post-BOOST day (skipped if B1 ran on the trial) | 50, hard |
| A1 | `heavy_b1a_grouped_pbday.sql`, one post-BOOST day (skipped if B2 ran on the trial) | 70, hard |
| A2 | `chunks/2026-08_events.sql` | 90, hard |
| A3 | `chunks/2026-08_b1b.sql`, `2026-08_b2.sql` | 35 and 5 |
| A4 | `chunks/2026-08_b1a.sql` | 320, hard |

**Gate:** measured cost of the 2026-08 chunk ≤ 1.3 × 338 = **440**. Above that → C1. The gate is on the chunk; A0 and A1 are inside the 500 ceiling but outside the gate.
**Also checked here:** A1's grouped rows against the member rows on disk for 2026-09-01 (`recon/compare_b1a_forms.py pbday`); block count for the month; share of sets unresolved; the derived reserve against pool accounts (day 0 step 4).

### Phase B — event columns, remaining 18 chunks (days 2–8). Budget 1,000 / 1,500.
`chunks/<chunk>_events.sql`, `--proven` after A2. Projected 945. **Gate:** cumulative events ≤ 1,500 by day 8, else C1 or C4.
On landing, per chunk: row count against the monthly P counts already measured (`VALIDATION_1_2_GRADUATIONS.md`), null counts per column, and the share of pools with no derived reserve.
**In-warehouse needs R5 built first.**

### Phase C — heavy tier, remaining 18 chunks (days 9–22, and month 2). Budget 1,550 / 1,750 in month 1 (250 moved to A), up to 3,000 in month 2.
`b1b` and `b2` for all chunks first (about 350 together, cheap, and c06 is H1). Then `b1a` in the chunk order above, about 180 each, always `--export`.
**Day-10 check:** phase B complete and month-to-date ≤ 3,000, else C2.
**Month 1 stops** when the next `b1a` would cross the reserve. On the projection that is after about eight `b1a` chunks.
**Month-2 ruling** uses the measured mean cost of the `b1a` chunks run so far times the chunks remaining; bought only if that is ≤ 3,000.

### Phase D — census and reconciliation (throughout). Budget 200 / 300.
Gate 0 census: one query with `GROUP BY` week over the small tables. The 20-token hand reconciliation (`VALIDATION_1_4_CENSUS.md`, seed 20260922) on day 1: acceptance 19 of 20. The analysis-column export of R4 is not inside this budget until it is ruled and its rate measured.

### Phase E — cancel (last day)
Plan cancelled before renewal. **Before any view is deleted:** every column the local analysis needs is exported and its manifest entry committed. Then views deleted, storage read back as 0, the Read/Write key revoked if it was used.

### Budget, launch order against projection

| Phase | Plan / ceiling after MR-14 | Projection | Comment |
|---|---|---|---|
| A | 400 / 500 | 338, plus up to 120 for A0 and A1 if not run on the trial | whole 2026-08 chunk |
| B | 1,000 / 1,500 | 945 for 18 chunks | fits |
| C | 1,550 / 1,750 + ≤ 3,000 | 3,525 for 18 chunks; about 4,600 at the post-BOOST rate throughout | fits the 4,750 combined ceiling at either rate, narrowly at the high one |
| D | 200 / 300 | 30–100 for the census; `b1a` group export a few credits per chunk | R4's export is extra |
| Month 1 | 3,150 plan, 3,400 spendable | A 338–458, B 945, D 100, C about 1,900 | C runs to the reserve |
| Month 2 | ≤ 3,000 | C remainder about 1,650 | |

---

## 6. Daily ledger entry (`docs/credits.md`), posted to Mando

```
### 2026-MM-DD  (plan day N, phase X)
spent today        ___      (runs: ___, cancelled: ___, exports: ___)
month to date      ___ of 4,000     spendable left above the 600 reserve  ___
phase to date      ___ of plan ___ / ceiling ___
chunks landed      events __/19   b1b __/19   b2 __/19   b1a __/19
measured per chunk events ___   b1a ___   (projection 53 / 180)
projection to end  ___      month 2 needed: yes / no / not yet known
gates              A ___   day-8 ___   day-10 ___
defects, halts     ___
contingency in force   none / C_
```

## 7. Contingencies, made operational

| | Trigger, as measured | Action |
|---|---|---|
| **C1** | 2026-08 chunk > 440, or mean chunk cost > 1.3 × projection after any three chunks | Build only: pre-BOOST 2025-10-30 → 2026-01-27 (chunks 2025-11, 2025-12 and the two partial months, regenerated to the block's exact dates), and all of post-BOOST (2026-07-21 → 09-20). Every cell labelled SAMPLED. |
| **C2** | projected heavy remainder > remaining budget | Drop, in this order only: `b3` (already unscheduled), organic-v2 (not scheduled; R3), `seta_lat_s` (remove the latency arrays from `b1a`'s output; saves little, since the funding scan stays), `b2`. Never `b1a`'s set, funding and block columns, never `b1b`. |
| **C3** (the expected path on Analyst) | export rate > 3 per MB | `events`, `b1b` and `b2` chunk queries become view definitions: same cost, rows stay on Dune. Needs the view mode of R5, the Read/Write key, expiry before the first cron firing, and the public-table acceptance already given. **`b1a` is never a view** (MR-14 ruling 3): it is always an ad-hoc query whose rows are exported to `barrel/data/`. What leaves Dune for analysis is R4. |
| **C4** | a chunk fails on engine limits or time | Regenerate that month as two halves with `gen_chunks.py`; same budget line. |
| **C5** | an input column is empty, a count does not reconcile, the derived reserve disagrees with pool accounts | Halt the phase. Null-count across all eras. Patch the generator. Re-run only chunks whose columns are touched. Logged in `credits.md` and the validation document for that column. |
| **C6** | reserve reached with chunks outstanding | Stop. Manifest marks built chunks. H1 evaluated on those, labelled PARTIAL with raw n and block n. |
| **C7** | the runner cancels a query | No re-run of that pattern until it has passed one-day scope again. Second cancellation in the month: phase paused for review. |

## 8. After the build (local, no credits)

Inputs: the exported analysis columns (R4), the funder-group rows, the member rows already held for two days and one week. Code: `recon/actors.py` (classifier, collapse, blocks, bundle cut, latency median; grouped and member forms give the same record, tested).

* Thresholds for v1.2 are set on the calibration slice (chunks 2025-03 → 2025-07-10) only.
* The burned week 2026-08-10 → 08-16 and the calibration slice are excluded from evaluation; every cell reports raw n and block n.
* H5 and the actor registry use the funder-group rows. One limit to state now: **the registry's actor is a funder seen within four days before a token's graduation day.** Actors who fund further ahead are unlinked creators until M0b.
* `seta_lat_s` is the median across the set's members, computed locally from the per-member seconds the query returns.
* Actor counts and block counts are subject to R1, R2 and B3.

## 9. Not in this plan

`b3` early-buyer groups, fee-share recipients, collapsed organic counts: built only under the Plus path or a later ruling. Organic-v2: R3. Multi-hop funding, live tracking, chatter and H3 are as placed by the launch order's §5.

---

## Appendix — the eight points put to the Architect in the first version, as ruled in MR-14

Other documents cite these by number.

1. 19 calendar chunks, not 20. **Accepted.**
2. Phase A projects at 338 plus proving runs against 150 / 250. **400 / 500 approved, from C.**
3. Export-first of the event table does not fit phase D on Analyst. **In-warehouse expected; `b1a` always exported.** What still has to leave Dune for local analysis is now R4.
4. No 90-day post-BOOST block exists. **Approved as proposed.**
5. Structural constants in queries versus verdict thresholds. **Confirmed; the bundle's five is a verdict and is applied locally.**
6. Exported rows in gitignored `barrel/data/` with a committed manifest. **Approved**, with an addition for Mando that was not relayed (R7).
7. One-day run of the grouped `b1a`. **Authorized and run.**
8. Window end 2026-09-20. **Signed.**
