# BARREL M0 — Paid-Month Runbook v1 (operational)

**Author:** ClaudeCode · **Date:** 2026-10-01, revised twice the same day · **Returns:** the Architect's launch order (`RUNBOOK_ARCHITECT_2026-10-01.md`, MR-13) with the operational detail filled in.
**Status: NOT READY — one ruling and one run remain (§1).** Revised after MR-14, MR-15 (`RULINGS_2026-10-02.md`) and two adversarial reviews.
**Plan assumed:** Analyst, month-to-month, 4,000 credits a month, 3,400 spendable after the 15% reserve, two months authorized. Under the Plus rule the same phases run uncut in one month.

---

## 1. Status before purchase

**Not ready. One ruling and one run remain.** Everything else the Architect ordered or ruled is done, run and recorded.

### What MR-15 closed (`RULINGS_2026-10-02.md`)

| Item | Result |
|---|---|
| **B1** derived virtual reserve | **Closed.** One post-BOOST day (2026-08-10), 10.7 credits. Derived reserve matches the pool account on 35 of 35 seeded pools (largest difference 46 lamports), and on the one pool carrying a third value (19.87 SOL). Halt lifted. `VQR_CHECK.md` |
| Burned-week price paths, re-counted locally | Done for the one day run; the other six days when phase A builds 2026-08. `PRICEPATH_BURNED_WEEK.md` |
| **B3** fan-out on a fixed window | **Closed.** Fan-out left the aligned-set query. One query per chunk over the full calendar month, recipients per day; funders substituted at run time. Proven at one-day scope, 0.5 credits. `recon/gen_fanout.py`, `actors.fan_rates` |
| **R1** member-funder is one actor | Implemented in `recon/actors.py`, both forms, tested |
| **R2** block keyed by address | Implemented as a component over the whole set (`actors.assign_blocks`), which is what puts one wallet in one block; the component reading is mine |
| **R3** organic-v2 cut on Analyst | H5 re-registered without it (`M0_TASKING.md` MR-15) |
| **R4** analysis-column export | Phase D ceiling = day-0 rate x 1.3 (§5) |
| **R5** view mode | Built only if day 0 selects in-warehouse, before phase B. Not built. |
| **R6, R8, R9** | Recorded |
| **R7** back up `barrel/data/` off the machine | Mando's. The list it was said to be in was not relayed. |

### What is still open

| # | What | Needs |
|---|---|---|
| **Q1** | **Does token-account rent count as funding?** The B2 run showed the slot rule's largest effect: 295 of the 303 members that gained a funder gained the token's creator on exactly 0.00203928 SOL, the rent of the token account the creator created for them while delivering tokens. Rent is above the 0.001 SOL floor, so it also decides membership: 100 of 2,317 creator-funded members in the June week are in the set on rent alone, and 68 of them hold at entry. Options and their effects: `B1A_GROUPED_VALIDATION.md`. My recommendation: leave membership as it is; for the funder either leave it or exclude a transfer made inside the member's own first-acquisition transaction. | **A ruling.** |
| **B2** | **The aligned-set query changed again after its run.** B2 itself ran (40.6 credits) and agreed with the earlier member rows on set size, holdings, flows and supply for 1,087 of 1,087 tokens; 342 of 76,999 members have a different funder, in the direction the ratified definitions predict. A second review then found the funder scan still stopped at a chunk-level date (two days after the chunk's last day, against an acquisition horizon of nine). Fixed; the text on disk has not run. | **One post-BOOST day, after Q1 so it is run once. Up to 70 trial credits, hard cap. Requested.** 106 are spendable until 2026-10-06. |

If Q1 and the authorization arrive on 2026-10-02 or 10-03, the run takes under an hour and day 0 can be the following day. After 2026-10-06 the same run is step A1 of phase A, inside its 500 ceiling.

### Carried, not blocking

* The view mode (R5) is half a day of work if day 0 selects in-warehouse.
* Column 86 has been compared only against rows that used the earlier definition. It is validated when a member-form and a grouped-form run of one text are compared; phase A step A1 does that.
* No chunk longer than one day has run for the aligned-set query. Phase A is where the per-token windows are first exercised across a month.

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
| `events` | the event-derived columns | **The text on disk ran on one post-BOOST day (2026-08-10), 10.7 credits; reserve 35 of 35 against pool accounts.** Earlier texts: one day and two weeks pre-BOOST. |
| `b1a` | aligned set grouped by funder: c07, c08, `seta_*`, `seta_lat_s`, registry | Grouped form with per-token windows, slot cutoff and tie-break: one post-BOOST day (2026-09-01), 40.6 credits. **The text on disk differs from that run by the length of the funder scan and one ledger day; not run.** |
| `fan` | funder fan-out, recipients per day over the calendar month; feeds the classifier, collapse and `blk` | one-day scope, 0.5 credits. Month scope not run. |
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

**4,808 credits for these four, plus 579 for the fan-out query (one per chunk, its calendar month): 5,387 for the core build**, against 6,800 spendable over two Analyst months. If every chunk costs the post-BOOST rate, about 6,800. The `b1a` unit was measured while it still carried the fan-out scan and before its funder scan was lengthened; the two roughly offset. Neither figure includes the analysis-column export.

What the static review checks on every scheduled file, and found nothing wrong with: each large table carries literal partition bounds; no large table is referenced more often than designed (SOL transfers twice in `b1a` and once in `fan`; token ledger once; bonding-curve trades once in the text; swap events once); the fan-out window is the chunk's calendar month; wallet-bearing queries carry the owner filter; the chunk's dates are in the text; no `OR` on a partition column; `SELECT` only; no registered verdict constant. It cannot show a query is cheap, and it did not catch the chunk-length dependence that a reviewer reading the definitions did.

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
| A1 | `heavy_b1a_grouped_pbday.sql` and `heavy_b1a_members_pbday.sql`, one post-BOOST day each, compared (the grouped one is skipped if it ran on the trial after Q1) | 70 each, hard |
| A1b | `chunks/2026-08_fan.sql` with the chunk's funders, after A4 | 40, hard |
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
`b1b` and `b2` for all chunks first (about 350 together, cheap, and c06 is H1). Then `b1a` in the chunk order above, about 180 each, always `--export`, each followed by its `fan` query (`--funders-from` that chunk's export; about 30 each, 579 in all, inside this phase's budget as ruled).
**Day-10 check:** phase B complete and month-to-date ≤ 3,000, else C2.
**Month 1 stops** when the next `b1a` would cross the reserve. On the projection that is after about eight `b1a` chunks.
**Month-2 ruling** uses the measured mean cost of the `b1a` chunks run so far times the chunks remaining; bought only if that is ≤ 3,000.

### Phase D — census and reconciliation (throughout). Budget 200 / 300.
Gate 0 census: one query with `GROUP BY` week over the small tables. The 20-token hand reconciliation (`VALIDATION_1_4_CENSUS.md`, seed 20260922) on day 1: acceptance 19 of 20. **The analysis-column export (MR-15 R4) is in this phase: one column-pruned export, and the phase's ceiling becomes the day-0 measured rate x the export's size x 1.3.**

### Phase E — cancel (last day)
Plan cancelled before renewal. **Before any view is deleted:** every column the local analysis needs is exported and its manifest entry committed. Then views deleted, storage read back as 0, the Read/Write key revoked if it was used.

### Budget, launch order against projection

| Phase | Plan / ceiling after MR-14 | Projection | Comment |
|---|---|---|---|
| A | 400 / 500 | 338, plus up to 120 for A0 and A1 if not run on the trial | whole 2026-08 chunk |
| B | 1,000 / 1,500 | 945 for 18 chunks | fits |
| C | 1,550 / 1,750 + ≤ 3,000 | 3,525 for 18 chunks plus 579 for fan-out = 4,104; about 5,200 at the post-BOOST rate throughout | fits the 4,750 combined ceiling on the projection; **over it by about 450 at the high rate**, which the day-10 check and the month-2 ruling govern |
| D | census 200 / 300; export ceiling = measured rate x size x 1.3 (MR-15 R4) | 30–100 for the census; `b1a` and `fan` exports a few credits per chunk; analysis columns 50–100 MB | |
| Month 1 | 3,150 plan, 3,400 spendable | A 338–458, B 945, D 100, C about 1,900 | C runs to the reserve |
| Month 2 | ≤ 3,000 | C remainder about 2,200 with fan-out | |

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
| **C2** | projected heavy remainder > remaining budget | Drop, in this order only: `b3` (already unscheduled), organic-v2 (cut on Analyst by MR-15 R3), `seta_lat_s` (remove the latency arrays from `b1a`'s output; saves little, since the funding scan stays), `b2`. Never `b1a`'s set, funding and block columns, never `b1b`. |
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
* Actor counts use R1; blocks are assigned over the whole evaluation set (`actors.assign_blocks`); both read the fan-out rate table built by `actors.fan_rates` from each chunk's fan-out export. The line between hub and purpose-built is a per-day rate set in v1.2; until then no classification is final.
* **H5 on the Analyst path is registered without organic-v2** (MR-15 R3).

## 9. Not in this plan

`b3` early-buyer groups, organic-v2, fee-share recipients, collapsed organic counts: built only under the Plus path or a later ruling. Multi-hop funding, live tracking, chatter and H3 are as placed by the launch order's §5.

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
